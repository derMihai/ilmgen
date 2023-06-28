use log::warn;
use serde::{
    de::{DeserializeSeed, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::{
    borrow::Borrow,
    cmp::Ordering,
    fmt::Debug,
    fmt::Display,
    io::Read,
    ops::Add,
    sync::mpsc::{channel, Receiver, Sender},
    thread::{self, JoinHandle},
};

#[derive(PartialEq, PartialOrd, Eq, Ord, Clone, Copy, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ThId(pub u64);
impl From<u64> for ThId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<ThId> for u64 {
    fn from(value: ThId) -> Self {
        value.0
    }
}

impl Display for ThId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Debug for ThId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

#[derive(PartialEq, PartialOrd, Eq, Ord, Clone, Copy, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Addr(u64);
impl From<u64> for Addr {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Add for Addr {
    type Output = Addr;

    fn add(self, rhs: Self) -> Self::Output {
        (self.0 + rhs.0).into()
    }
}

impl Addr {
    fn offset(self, offset: u64) -> Self {
        (self.0 + offset).into()
    }
}

impl Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Debug for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Block {
    pub addr: Addr,
    pub size: u64,
}

impl Eq for Block {}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for Block {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Block {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.addr.offset(self.size) <= other.addr {
            return Ordering::Less;
        }
        if other.addr.offset(other.size) <= self.addr {
            return Ordering::Greater;
        }

        Ordering::Equal
    }
}

impl Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a: {:8}, s: {:8}", self.addr, self.size)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BlockUsage {
    #[serde(flatten)]
    pub block: Block,
    pub r: u64,
    pub w: u64,
}

impl Debug for BlockUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.block.fmt(f)?;
        write!(f, " r: {:8}, w: {:8}", self.r, self.w)
    }
}

impl Borrow<Block> for BlockUsage {
    fn borrow(&self) -> &Block {
        &self.block
    }
}

#[derive(Debug, Deserialize)]
pub enum LifeTimeEv {
    #[serde(rename = "alloc")]
    Alloc(Block),
    #[serde(rename = "free")]
    Free(BlockUsage),
    #[serde(rename = "newsync")]
    NewSync { addr: Addr, prim: String },
    #[serde(rename = "delsync")]
    DelSync { addr: Addr, prim: String },
}

#[derive(Debug, Deserialize)]
pub enum SyncEvVariant {
    #[serde(rename = "acq")]
    Acquire(Addr),
    #[serde(rename = "rel")]
    Release(Addr),
    #[serde(rename = "fork")]
    Fork(ThId),
    #[serde(rename = "join")]
    Join(ThId),
    #[serde(rename = "exit")]
    Exit,
}

#[derive(Debug, Deserialize)]
pub struct SyncEv {
    pub usage: Vec<BlockUsage>,
    #[serde(flatten)]
    pub variant: SyncEvVariant,
}

#[derive(Debug, Deserialize)]
pub enum MpEvVariant {
    #[serde(rename = "info")]
    Info(String),
    #[serde(rename = "life")]
    LifeTime(LifeTimeEv),
    #[serde(rename = "sync")]
    SyncEv(SyncEv),
}

#[derive(Debug, Deserialize)]
pub struct MpEv {
    pub thid: ThId,
    pub icnt: u64,
    pub id: u64,
    #[serde(flatten)]
    pub variant: MpEvVariant,
}

pub struct MpEvDeser {
    ch: Receiver<MpEv>,
    join_handle: Option<JoinHandle<Result<(), serde_json::Error>>>,
}

impl MpEvDeser {
    pub fn new<I>(input: I) -> MpEvDeser
    where
        I: Read + Send + 'static,
    {
        let (sender, receiver) = channel();

        let join_handle = thread::spawn(move || {
            let stream: MpEvStream = MpEvStream { ch: sender };
            let mut deserializer = serde_json::de::Deserializer::from_reader(input);

            stream.deserialize(&mut deserializer)
        });

        MpEvDeser {
            ch: receiver,
            join_handle: Some(join_handle),
        }
    }
}

impl Iterator for MpEvDeser {
    type Item = MpEv;

    fn next(&mut self) -> Option<Self::Item> {
        match self.ch.recv() {
            Ok(item) => Some(item),
            Err(_) => {
                if let Some(handle) = self.join_handle.take() {
                    handle
                        .join()
                        .expect("Deserializer thread panicked")
                        .expect("Deserializer thread error");
                }
                None
            }
        }
    }
}

struct MpEvStream {
    ch: Sender<MpEv>,
}

impl<'de> DeserializeSeed<'de> for MpEvStream {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EvSeqVisitor {
            ch: Sender<MpEv>,
        }

        impl<'de> Visitor<'de> for EvSeqVisitor {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("An array of events")
            }

            fn visit_seq<S>(self, mut seq: S) -> Result<(), S::Error>
            where
                S: SeqAccess<'de>,
            {
                while let Some(ev) = seq.next_element::<MpEv>()? {
                    if let Err(msg) = self.ch.send(ev) {
                        warn!("MpEv consumer thread stopped: {msg}");
                        break;
                    }
                }

                Ok(())
            }
        }

        deserializer.deserialize_seq(EvSeqVisitor { ch: self.ch })
    }
}
