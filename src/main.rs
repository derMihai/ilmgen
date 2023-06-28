use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    io::Write,
    io::{self, BufReader, BufWriter, Read},
    process::ExitCode,
};

use argh::FromArgs;
use evblock::{DagBlock, Event};
use log::error;
use mpev::MpEvDeser;
use serde::Serialize;
use syncev::SyncEvParser;

mod evblock;
mod mpev;
mod syncev;

struct IteratorSer<I>(RefCell<I>)
where
    I: Iterator,
    I::Item: Serialize;

impl<I> Serialize for IteratorSer<I>
where
    I: Iterator,
    I::Item: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.0.borrow_mut().by_ref())
    }
}

const DAG_BLOCK_MAX_EVENTS: usize = 4096;

fn print_dot(writer: &mut dyn Write, dag: &DagBlock) -> Result<(), std::io::Error> {
    fn write_ev_id(ev: &Event) -> String {
        format!(
            "{}{}",
            match ev.meta {
                Some(evblock::EvMeta::Acq) => "a",
                Some(evblock::EvMeta::Rel) => "r",
                Some(evblock::EvMeta::Fork) => "f",
                Some(evblock::EvMeta::Join) => "j",
                Some(evblock::EvMeta::Exit) => "e",
                _ => "",
            },
            ev.id
        )
    }

    writeln!(writer, "digraph G {{")?;

    for (idx, (thd, events)) in dag.threads.iter().enumerate() {
        writeln!(writer, "\tsubgraph cluster{} {{", idx)?;
        writeln!(writer, "\t\tlabel = \"thd {}\";", thd)?;

        let mut ev_iter = events.into_iter();

        match ev_iter.next() {
            Some(ev) => write!(writer, "\t\t{}", write_ev_id(&ev.borrow()))?,
            None => {
                write!(writer, "}}")?;
                continue;
            }
        }

        for ev in ev_iter {
            write!(writer, " -> {}", write_ev_id(&ev.borrow()))?;
        }

        write!(writer, ";\n\t}}\n")?;
    }

    for (_, events) in dag.threads.iter() {
        for ev in events {
            let ev_borrow = ev.borrow();
            if ev_borrow.id == 0 {
                continue;
            }
            for (.., next) in ev_borrow.next.iter() {
                let next_borrow = next.borrow();
                writeln!(
                    writer,
                    "\t{} -> {};",
                    write_ev_id(&ev_borrow),
                    write_ev_id(&next_borrow),
                )?;
            }
        }
    }

    write!(writer, "}}")?;

    Ok(())
}

#[derive(FromArgs)]
/// Build ILM from raw HPCMP events
struct Args {
    /// input file name, "-" for stdin
    #[argh(positional)]
    input: String,
    /// output file name, defaults to "ilm.json" ("ilm.dot" if --dot specified)
    #[argh(positional)]
    output: Option<String>,
    /// output first DAG block as .dot file (Graphviz) instead of json, then stop
    #[argh(switch)]
    dot: bool,
    /// DAG block maximal event count (default 4096)
    #[argh(option, default = "DAG_BLOCK_MAX_EVENTS")]
    block_size: usize,
    /// formatted JSON (takes up more space)
    #[argh(switch)]
    pretty_json: bool,
    /// skip transitive reduction of the event DAG
    #[argh(switch)]
    no_trans_reduce: bool,
    /// don't prune unobserved events
    #[argh(switch)]
    no_pruning: bool,
}

fn main_with_result() -> Result<(), ()> {
    env_logger::init();

    let args: Args = argh::from_env();

    let input: Box<(dyn Read + Send)> = match args.input.as_str() {
        "-" => Box::new(io::stdin()),
        input_file => {
            let file = File::open(input_file)
                .map_err(|e| error!("Cannot open input file {}: {}", input_file, e))?;
            Box::new(file)
        }
    };

    let input = BufReader::new(input);

    let output_path =
        args.output
            .as_deref()
            .unwrap_or(if args.dot { "ilm.dot" } else { "ilm.json" });

    let output = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(output_path)
        .map_err(|e| error!("Cannot open output file {}: {}", output_path, e))?;

    let mut output = BufWriter::new(output);

    let blocks = RefCell::new(evblock::DagGen::new(
        SyncEvParser::new(MpEvDeser::new(input)),
        args.block_size,
        !args.no_trans_reduce,
        !args.no_pruning,
    ));

    if args.dot {
        if let Some(block) = blocks.borrow_mut().next() {
            print_dot(&mut output, &block).map_err(|e| error!("Error when exporting .dot: {e}"))?;
        };
    } else {
        if args.pretty_json {
            serde_json::to_writer_pretty(output, &IteratorSer(blocks))
        } else {
            serde_json::to_writer(output, &IteratorSer(blocks))
        }
        .map_err(|e| error!("Error when exporting .json: {e}"))?;
    }

    Ok(())
}

fn main() -> ExitCode {
    match main_with_result() {
        Ok(_) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}
