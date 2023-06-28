use std::borrow::Borrow;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::vec;

use log::{info, warn};
use serde::Serialize;

use crate::mpev::{self, Block, BlockUsage, LifeTimeEv, MpEv, MpEvVariant, SyncEvVariant, ThId};

#[derive(Debug)]
pub struct SyncEv {
    pub id: u64,
    pub thid: ThId,
    pub block_log: Tracer<Block, BlockUsage>,
    pub sync_log: Tracer<mpev::Addr, mpev::Addr>,
    pub bk_usage: Vec<BlockUsage>,
    pub insn_cnt: u64,
    pub heap_max: u64,
    pub heap_balance: i64,
    pub variant: SyncEvVariant,
}

pub struct SyncEvParser<T> {
    ev_iter: T,
    threads: HashMap<ThId, Thread>,
}

struct Thread {
    thid: ThId,
    bk_tracer: Tracer<Block, BlockUsage>,
    sync_tracer: Tracer<mpev::Addr, mpev::Addr>,
    insn_cnt: u64,
    heap_balance: i64,
    heap_max: u64,
}

impl Thread {
    fn new(thid: ThId) -> Self {
        Self {
            thid,
            bk_tracer: Tracer::new(),
            sync_tracer: Tracer::new(),
            insn_cnt: 0,
            heap_balance: 0,
            heap_max: 0,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Tracer<T, U>
where
    T: Ord + Clone + Debug,
    U: Borrow<T> + Clone,
{
    pub new: BTreeSet<T>,
    pub del: Vec<U>,
}

impl<T, U> Tracer<T, U>
where
    T: Ord + Clone + Debug,
    U: Borrow<T> + Clone,
{
    pub fn new() -> Self {
        Self {
            new: BTreeSet::new(),
            del: vec![],
        }
    }

    fn trace_create(&mut self, obj: T) -> Result<(), T> {
        match self.new.insert(obj.clone()) {
            true => Ok(()),
            false => Err(obj),
        }
    }

    fn trace_destroy(&mut self, obj: U) -> bool {
        match self.new.remove(obj.borrow()) {
            true => {
                // object created after last sync event. We assume thus there are no external
                // observers, so we may drop it's creation and destruction events
                true
            }
            false => {
                // object was allocated prior to last sync event
                self.del.push(obj);
                false
            }
        }
    }

    pub fn get_created(&self) -> &BTreeSet<T> {
        &self.new
    }

    pub fn get_destroyed(&self) -> &Vec<U> {
        &self.del
    }

    pub fn merge_previous<C: FnMut(&U)>(&mut self, mut prev: Self, drop_create: &mut C) {
        // this will drop any allocations in 'prev' that are freed in 'self'
        for destroyed in &self.del {
            if prev.trace_destroy(destroyed.clone()) {
                drop_create(destroyed)
            }
        }

        for created in &prev.new {
            if let Err(collision) = self.trace_create(created.clone()) {
                warn!(
                    "Tracer::merge_previous: double alloc of obj {:?}",
                    collision
                );
            }
        }

        self.del = prev.del
    }
}

impl<T, U> Default for Tracer<T, U>
where
    T: Ord + Clone + Debug,
    U: Borrow<T> + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SyncEvParser<T>
where
    T: Iterator<Item = MpEv>,
{
    const MAIN_TID: ThId = ThId(1);

    pub fn new(ev_iter: T) -> SyncEvParser<T> {
        let mut threads = HashMap::new();
        threads.insert(Self::MAIN_TID, Thread::new(Self::MAIN_TID));

        SyncEvParser { ev_iter, threads }
    }

    fn parse_syncev(&mut self, thread: Thread, syncev: mpev::SyncEv, id: u64) -> SyncEv {
        match &syncev.variant {
            SyncEvVariant::Fork(child) => {
                if self.threads.insert(*child, Thread::new(*child)).is_some() {
                    warn!("Fork in {}: thread {child} already exists!", thread.thid);
                }
            }

            SyncEvVariant::Exit => {
                self.threads.remove(&thread.thid);
            }
            _ => {}
        }

        SyncEv {
            id,
            thid: thread.thid,
            block_log: thread.bk_tracer,
            sync_log: thread.sync_tracer,
            bk_usage: syncev.usage,
            insn_cnt: thread.insn_cnt,
            heap_max: thread.heap_max,
            heap_balance: thread.heap_balance,
            variant: syncev.variant,
        }
    }

    fn parse_ltev(thread: &mut Thread, ltev: mpev::LifeTimeEv) {
        match ltev {
            LifeTimeEv::Alloc(block) => {
                thread.heap_balance += block.size as i64;
                if thread.heap_balance >= 0 {
                    thread.heap_max = u64::max(thread.heap_balance as u64, thread.heap_max);
                }

                if let Err(collision) = thread.bk_tracer.trace_create(block) {
                    warn!(
                        "{:?} allocated more than once in thread {}",
                        collision, thread.thid
                    );
                }
            }
            LifeTimeEv::Free(usage) => {
                thread.heap_balance -= usage.block.size as i64;
                thread.bk_tracer.trace_destroy(usage);
            }
            LifeTimeEv::NewSync { addr, prim } => {
                if let Err(collision) = thread.sync_tracer.trace_create(addr) {
                    warn!(
                        "{prim} object {:?} allocated more than once in thread {}",
                        collision, thread.thid
                    );
                }
            }
            LifeTimeEv::DelSync { addr, .. } => {
                thread.sync_tracer.trace_destroy(addr);
            }
        }
    }

    fn parse_mpev(&mut self, mpev: MpEv) -> Option<SyncEv> {
        match mpev.variant {
            MpEvVariant::Info(msg) => {
                info!("MPEv info thd {}: {msg}", mpev.thid);
                None
            }
            MpEvVariant::LifeTime(ltev) => {
                let thread = match self.threads.get_mut(&mpev.thid) {
                    Some(thd) => thd,
                    None => {
                        warn!("LtEv for unknown thread {}", mpev.thid);
                        return None;
                    }
                };

                thread.insn_cnt += mpev.icnt;

                Self::parse_ltev(thread, ltev);
                None
            }
            MpEvVariant::SyncEv(syncev) => {
                match self.threads.insert(mpev.thid, Thread::new(mpev.thid)) {
                    Some(mut thread) => {
                        thread.insn_cnt += mpev.icnt;
                        Some(self.parse_syncev(thread, syncev, mpev.id))
                    }
                    None => {
                        warn!("SyncEv for unknown thread {}", mpev.thid);
                        None
                    }
                }
            }
        }
    }
}

impl<T> Iterator for SyncEvParser<T>
where
    T: Iterator<Item = MpEv>,
{
    type Item = SyncEv;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(mpev) = self.ev_iter.next() {
            if let Some(syncev) = self.parse_mpev(mpev) {
                return Some(syncev);
            }
        }
        None
    }
}
