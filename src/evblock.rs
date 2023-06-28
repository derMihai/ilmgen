use log::debug;
use serde::{
    ser::{SerializeSeq, SerializeStruct},
    Serialize,
};

use std::{
    cell::{Cell, RefCell, RefMut},
    fmt::Debug,
    mem::{self, take},
    rc::Rc,
    vec,
};

use crate::{
    mpev::{self, Addr, Block, BlockUsage, ThId},
    syncev::{SyncEv, Tracer},
};

type EvId = u64;

#[derive(Debug, Serialize)]
pub enum EvMeta {
    #[serde(rename = "acq")]
    Acq,
    #[serde(rename = "rel")]
    Rel,
    #[serde(rename = "fork")]
    Fork,
    #[serde(rename = "join")]
    Join,
    #[serde(rename = "exit")]
    Exit,
}

pub struct Event {
    pub id: EvId,
    pub next_in_thd: Option<Rc<RefCell<Event>>>,
    pub next: Vec<(ThId, Rc<RefCell<Event>>)>,
    pub block_log: Tracer<Block, BlockUsage>,
    pub bk_usage: Vec<BlockUsage>,
    pub insn_cnt: u64,
    pub heap_max: u64,
    pub heap_balance: i64,
    visited: Cell<Option<EvId>>,
    pub meta: Option<EvMeta>,
}

impl Event {
    pub const HELPER_NODE_ID: u64 = 0;
    fn merge_into_next(&mut self) {
        let mut next = match &self.next_in_thd {
            Some(next) => next.borrow_mut(),
            None => return,
        };

        self.bk_usage.sort_by(|a, b| a.block.cmp(&b.block));
        next.bk_usage.sort_by(|a, b| a.block.cmp(&b.block));

        next.block_log.merge_previous(
            mem::replace(&mut self.block_log, Tracer::new()),
            &mut |bku: &BlockUsage| {
                if let Ok(idx) = self
                    .bk_usage
                    .binary_search_by(|bku_self| bku_self.block.cmp(&bku.block))
                {
                    self.bk_usage.remove(idx);
                }
            },
        );

        // TODO: both usage arrays are sorted, walk them in parallel
        for bku in &self.bk_usage {
            match next
                .bk_usage
                .binary_search_by(|probe| probe.block.cmp(&bku.block))
            {
                Ok(idx) => {
                    next.bk_usage[idx].r += bku.r;
                    next.bk_usage[idx].w += bku.w;
                }
                Err(idx) => next.bk_usage.insert(idx, bku.clone()),
            }
        }

        next.heap_balance += self.heap_balance;

        next.heap_max = match next.heap_max as i64 + self.heap_balance {
            next_hm if next_hm >= 0 => u64::max(self.heap_max, next_hm as u64),
            _ => self.heap_max,
        };

        next.insn_cnt += self.insn_cnt;
    }
}

pub type EvList = (Rc<RefCell<Event>>, Rc<RefCell<Event>>);
pub struct EventQueue {
    events: Option<EvList>,
}

impl EventQueue {
    fn new() -> Self {
        EventQueue { events: None }
    }

    fn push(&mut self, event: Rc<RefCell<Event>>) {
        self.events = Some(match self.events.as_ref() {
            Some((first, last)) => {
                assert!(last.borrow().next_in_thd.is_none());
                last.borrow_mut().next_in_thd = Some(Rc::clone(&event));
                (Rc::clone(first), event)
            }
            None => (Rc::clone(&event), event),
        });
    }

    fn last_mut(&mut self) -> Option<RefMut<'_, Event>> {
        self.events.as_ref().map(|(.., last)| last.borrow_mut())
    }

    fn skip_helper_ev(&mut self) {
        for ev in self {
            let mut ev_borrow = ev.borrow_mut();
            // this is an optimization: we know we add helpers only for `fork` nodes
            if let Some(EvMeta::Fork) = ev_borrow.meta {
                for (.., next) in &mut ev_borrow.next {
                    let next_ref = next.borrow();
                    if next_ref.id != Event::HELPER_NODE_ID {
                        continue;
                    }

                    if let Some(next_next) = next_ref.next_in_thd.as_ref() {
                        let next_next = Rc::clone(next_next);
                        drop(next_ref);
                        *next = next_next;
                    }
                }
            }
        }
    }

    fn prune<F>(&mut self, to_keep_f: F)
    where
        F: Fn(&Rc<RefCell<Event>>) -> bool,
    {
        let first_ev = match &mut self.events {
            Some((first, ..)) => first,
            None => return,
        };

        let first_keep = loop {
            let next = match &first_ev.borrow().next_in_thd {
                Some(n) => Rc::clone(n),
                None => return,
            };

            if !to_keep_f(first_ev) {
                first_ev.borrow_mut().merge_into_next();
                *first_ev = next;
            } else {
                break Rc::clone(first_ev);
            }
        };

        prune_r(first_keep, to_keep_f);

        fn prune_r<F>(mut keep: Rc<RefCell<Event>>, to_keep_f: F)
        where
            F: Fn(&Rc<RefCell<Event>>) -> bool,
        {
            let mut keep_ref = keep.borrow_mut();
            let mut next_ref = &mut keep_ref.next_in_thd;
            loop {
                let next = match next_ref {
                    Some(next) => next,
                    None => return,
                };

                if !to_keep_f(next) {
                    next.borrow_mut().merge_into_next();
                    let tmp = next.borrow().next_in_thd.as_ref().map(Rc::clone);
                    *next_ref = tmp;
                    continue;
                }

                let new_keep = Rc::clone(next);

                drop(keep_ref);

                keep = new_keep;
                keep_ref = keep.borrow_mut();
                next_ref = &mut keep_ref.next_in_thd;
            }
        }
    }
}

pub struct EventQueueIter {
    curr: Option<Rc<RefCell<Event>>>,
}

pub struct EventQueueIterMut {
    curr: Option<Rc<RefCell<Event>>>,
}

impl Iterator for EventQueueIter {
    type Item = Rc<RefCell<Event>>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self
            .curr
            .as_ref()
            .and_then(|curr| curr.borrow().next_in_thd.as_ref().map(Rc::clone));

        mem::replace(&mut self.curr, next)
    }
}

impl IntoIterator for &EventQueue {
    type Item = Rc<RefCell<Event>>;
    type IntoIter = EventQueueIter;

    fn into_iter(self) -> Self::IntoIter {
        EventQueueIter {
            curr: self.events.as_ref().map(|(first, ..)| Rc::clone(first)),
        }
    }
}

impl Iterator for EventQueueIterMut {
    type Item = Rc<RefCell<Event>>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self
            .curr
            .as_ref()
            .and_then(|curr| curr.borrow().next_in_thd.as_ref().map(Rc::clone));

        mem::replace(&mut self.curr, next)
    }
}

impl IntoIterator for &mut EventQueue {
    type Item = Rc<RefCell<Event>>;
    type IntoIter = EventQueueIter;

    fn into_iter(self) -> Self::IntoIter {
        EventQueueIter {
            curr: self.events.as_ref().map(|(first, ..)| Rc::clone(first)),
        }
    }
}

impl Debug for EventQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "[")?;
        for ev in self {
            ev.fmt(f)?;
        }
        write!(f, "]")
    }
}

impl Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Event")
            .field("dbg_id", &self.id)
            .field(
                "next",
                &self
                    .next
                    .iter()
                    .map(|(.., ev)| ev.borrow().id)
                    .collect::<Vec<u64>>()
                    .as_slice(),
            )
            .field("block_log", &self.block_log)
            .field("bk_usage", &self.bk_usage)
            .field("insn_cnt", &self.insn_cnt)
            .field("heap_max", &self.heap_max)
            .field("heap_balance", &self.heap_balance)
            .field("meta", &self.meta)
            .finish()
    }
}

struct VecDict<K: Ord + Copy, V> {
    vec: Vec<(K, V)>,
}

struct VecDictIter<'a, K, V: 'a> {
    iter: std::slice::Iter<'a, (K, V)>,
}

impl<'a, K, V: 'a> Iterator for VecDictIter<'a, K, V> {
    type Item = &'a (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

struct VecDictIterMut<'a, K, V: 'a> {
    iter: std::slice::IterMut<'a, (K, V)>,
}

impl<'a, K, V: 'a> Iterator for VecDictIterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (&*k, v))
    }
}

impl<K: Ord + Copy, V> VecDict<K, V> {
    fn new() -> Self {
        Self { vec: vec![] }
    }

    fn get_mut<'a>(&'a mut self, key: &K) -> Option<&'a mut V> {
        self.vec
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
            .map(|idx| &mut self.vec[idx].1)
    }

    fn get_mut_or_insert<'a, F>(&'a mut self, key: &K, f: F) -> &'a mut V
    where
        F: Fn() -> V,
    {
        let idx = match self.vec.binary_search_by(|(k, _)| k.cmp(key)) {
            Ok(idx) => idx,
            Err(idx) => {
                self.vec.insert(idx, (*key, f()));
                idx
            }
        };

        &mut self.vec[idx].1
    }

    fn replace(&mut self, key: &K, val: V) -> Option<V> {
        match self.vec.binary_search_by(|(k, _)| k.cmp(key)) {
            Ok(idx) => Some(std::mem::replace(&mut self.vec[idx].1, val)),
            Err(idx) => {
                self.vec.insert(idx, (*key, val));
                None
            }
        }
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        self.vec
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
            .map(|idx| self.vec.remove(idx).1)
    }
}

impl<'a, K: Ord + Copy, V> IntoIterator for &'a VecDict<K, V> {
    type Item = &'a (K, V);
    type IntoIter = VecDictIter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            iter: self.vec.iter(),
        }
    }
}

impl<'a, K: Ord + Copy, V> IntoIterator for &'a mut VecDict<K, V> {
    type Item = (&'a K, &'a mut V);
    type IntoIter = VecDictIterMut<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            iter: self.vec.iter_mut(),
        }
    }
}

impl<K: Ord + Copy, V> From<VecDict<K, V>> for Vec<(K, V)> {
    fn from(value: VecDict<K, V>) -> Self {
        value.vec
    }
}

type EventXSect = VecDict<ThId, Rc<RefCell<Event>>>;

struct DagBlockInternal {
    threads: VecDict<ThId, EventQueue>,
    sync: VecDict<Addr, EventXSect>,
    ev_cnt: usize,
}

pub struct DagBlock {
    pub threads: Vec<(ThId, EventQueue)>,
}

impl DagBlock {
    const TRANS_REDUCE_REC_LIM: u32 = 64;

    fn transitive_reduce(&mut self, rec_lim: u32) {
        for (.., ev_queue) in &self.threads {
            for ev in ev_queue {
                reduce_inner(&mut ev.borrow_mut(), rec_lim)
            }
        }

        fn reduce_inner(root: &mut Event, rec_lim: u32) {
            let root_id = root.id;

            for (.., root_nxt) in &root.next {
                root_nxt.borrow().visited.replace(None);
            }

            if let Some(ref next_in_thread) = root.next_in_thd {
                reduce_rec(&next_in_thread.borrow(), root_id, rec_lim);
            }

            for (.., root_next) in &root.next {
                let head_nxt_ref = root_next.borrow();
                if let Some(ref next_in_thread) = head_nxt_ref.next_in_thd {
                    reduce_rec(&next_in_thread.borrow(), root_id, rec_lim);
                }
                for (.., root_nxt_nxt) in &root_next.borrow().next {
                    reduce_rec(&root_nxt_nxt.borrow(), root_id, rec_lim)
                }
            }

            // Drop all un-marked children
            root.next
                .retain(|(.., ev)| ev.borrow().visited.take().is_none());

            fn reduce_rec(curr: &Event, root_id: EvId, max_depth: u32) {
                if max_depth == 0 {
                    return;
                }

                // un-mark
                match curr.visited.replace(Some(root_id)) {
                    Some(traced_id) if traced_id == root_id => {
                        // we've been on this path
                        return;
                    }
                    _ => {}
                }

                if let Some(ref next_in_thread) = curr.next_in_thd {
                    reduce_rec(&next_in_thread.borrow(), root_id, max_depth - 1);
                }

                for (.., nxt) in &curr.next {
                    reduce_rec(&nxt.borrow(), root_id, max_depth - 1)
                }
            }
        }
    }

    fn prune_unobserved(&mut self) {
        let is_observed = |ev: &Rc<RefCell<Event>>| -> bool {
            match Rc::strong_count(ev) > 1 || !ev.borrow().next.is_empty() {
                true => true,
                false => {
                    debug!("Event {:?} unobserved!", ev.borrow().id);
                    false
                }
            }
        };
        for (.., ev_queue) in &mut self.threads {
            ev_queue.prune(is_observed);
        }
    }

    fn prune_helper(&mut self) {
        for (.., ev_queue) in &mut self.threads {
            ev_queue.prune(|ev| ev.borrow().id != Event::HELPER_NODE_ID);
        }
    }
}

impl Debug for DagBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (thid, ev_queue) in &self.threads {
            writeln!(f, "Thd {thid} : [")?;
            ev_queue.fmt(f)?;
            writeln!(f, "],")?;
        }
        write!(f, "")
    }
}

impl Debug for DagBlockInternal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (thid, ev_queue) in &self.threads {
            writeln!(f, "Thd {thid} : [")?;
            ev_queue.fmt(f)?;
            writeln!(f, "],")?;
        }
        write!(f, "")
    }
}

impl Default for DagBlockInternal {
    fn default() -> Self {
        Self::new()
    }
}

impl DagBlockInternal {
    fn new() -> Self {
        Self {
            threads: VecDict::new(),
            sync: VecDict::new(),
            ev_cnt: 0,
        }
    }

    fn push_event(&mut self, syncev: SyncEv) {
        let thid = syncev.thid;

        let event = match syncev.variant {
            mpev::SyncEvVariant::Acquire(addr) => {
                let rel_events = self.sync.get_mut_or_insert(&addr, VecDict::new);

                let acq_ev = Rc::new(RefCell::new(Event {
                    next_in_thd: None,
                    id: syncev.id,
                    next: vec![],
                    block_log: syncev.block_log,
                    bk_usage: syncev.bk_usage,
                    insn_cnt: syncev.insn_cnt,
                    heap_max: syncev.heap_max,
                    heap_balance: syncev.heap_balance,
                    visited: Cell::new(None),
                    meta: Some(EvMeta::Acq),
                }));

                // link last releases as previous
                for (rel_ev_thid, rel_ev) in rel_events {
                    if *rel_ev_thid != thid {
                        rel_ev.borrow_mut().next.push((thid, Rc::clone(&acq_ev)));
                    }
                }

                // remove sync primitives explicitly deleted
                for sync_del in syncev.sync_log.get_destroyed() {
                    if self.sync.remove(sync_del).is_none() {
                        debug!("thd {thid} deletes untracked sync obj {sync_del}");
                    }
                }

                acq_ev
            }
            mpev::SyncEvVariant::Release(addr) => {
                for sync_del in syncev.sync_log.get_destroyed() {
                    if self.sync.remove(sync_del).is_none() {
                        debug!("thd {thid} deletes untracked sync obj {sync_del}");
                    }
                }

                let rel_ev = Rc::new(RefCell::new(Event {
                    next_in_thd: None,
                    id: syncev.id,
                    next: vec![],
                    block_log: syncev.block_log,
                    bk_usage: syncev.bk_usage,
                    insn_cnt: syncev.insn_cnt,
                    heap_max: syncev.heap_max,
                    heap_balance: syncev.heap_balance,
                    visited: Cell::new(None),
                    meta: Some(EvMeta::Rel),
                }));

                let rel_events = self.sync.get_mut_or_insert(&addr, VecDict::new);
                let _ = rel_events.replace(&thid, Rc::clone(&rel_ev));

                rel_ev
            }
            mpev::SyncEvVariant::Fork(child_thid) => {
                // The fork event is guaranteed to happen before any child event. Thus, we can push
                // a dummy event in the child's event queue knowing is going to be the first
                let child_first_ev = Rc::new(RefCell::new(Event {
                    next_in_thd: None,
                    id: Event::HELPER_NODE_ID,
                    next: vec![],
                    block_log: Tracer::new(),
                    bk_usage: vec![],
                    insn_cnt: 0,
                    heap_max: 0,
                    heap_balance: 0,
                    visited: Cell::new(None),
                    meta: None,
                }));

                let ev_queue = self.threads.get_mut_or_insert(&child_thid, EventQueue::new);
                ev_queue.push(Rc::clone(&child_first_ev));

                Rc::new(RefCell::new(Event {
                    id: syncev.id,
                    next_in_thd: None,
                    next: vec![(child_thid, child_first_ev)],
                    block_log: syncev.block_log,
                    bk_usage: syncev.bk_usage,
                    insn_cnt: syncev.insn_cnt,
                    heap_max: syncev.heap_max,
                    heap_balance: syncev.heap_balance,
                    visited: Cell::new(None),
                    meta: Some(EvMeta::Fork),
                }))
            }
            mpev::SyncEvVariant::Join(child_thid) => {
                let join_ev = Rc::new(RefCell::new(Event {
                    id: syncev.id,
                    next_in_thd: None,
                    next: vec![],
                    block_log: syncev.block_log,
                    bk_usage: syncev.bk_usage,
                    insn_cnt: syncev.insn_cnt,
                    heap_balance: syncev.heap_balance,
                    heap_max: syncev.heap_max,
                    visited: Cell::new(None),
                    meta: Some(EvMeta::Join),
                }));
                // The join event is guaranteed to happen after the child's exit event. We can pick
                // the last event in the child's event queue as previous, knowing it should be the
                // exit event.

                // FIXME: There is a small chance that between the join operation exited and the join
                // event was recorded a new thread with the same id as the child was created.
                match self.threads.get_mut(&child_thid).and_then(|q| q.last_mut()) {
                    Some(mut child_exit_ev) => {
                        child_exit_ev.next.push((thid, Rc::clone(&join_ev)));
                    }
                    None => debug!("thd {thid} joins {child_thid} with no exit point"),
                };

                join_ev
            }
            mpev::SyncEvVariant::Exit => {
                // TODO: pin down this event somewhere s.t. the parent thread can easily find it when
                // joining. See the FIXME above in the Join match.
                Rc::new(RefCell::new(Event {
                    next_in_thd: None,
                    id: syncev.id,
                    next: vec![],
                    block_log: syncev.block_log,
                    bk_usage: syncev.bk_usage,
                    insn_cnt: syncev.insn_cnt,
                    heap_balance: syncev.heap_balance,
                    heap_max: syncev.heap_max,
                    visited: Cell::new(None),
                    meta: Some(EvMeta::Exit),
                }))
            }
        };

        let ev_queue = self.threads.get_mut_or_insert(&thid, EventQueue::new);
        ev_queue.push(event);

        self.ev_cnt += 1;
    }

    fn len(&self) -> usize {
        self.ev_cnt
    }
}

pub struct DagGen<S>
where
    S: Iterator<Item = SyncEv>,
{
    sync_ev_it: S,
    cap: usize,
    events: DagBlockInternal,
    trans_reduce: bool,
    prune_unobserved: bool,
}

impl<S> DagGen<S>
where
    S: Iterator<Item = SyncEv>,
{
    pub fn new(sync_ev_it: S, cap: usize, trans_reduce: bool, prune_unobserved: bool) -> Self {
        Self {
            sync_ev_it,
            cap,
            trans_reduce,
            prune_unobserved,
            events: DagBlockInternal::new(),
        }
    }
}

impl From<DagBlockInternal> for DagBlock {
    fn from(value: DagBlockInternal) -> Self {
        let mut dag_bk = Self {
            threads: value.threads.into(),
        };

        for (.., ev_queue) in &mut dag_bk.threads {
            ev_queue.skip_helper_ev();
        }

        dag_bk
    }
}

impl<S> Iterator for DagGen<S>
where
    S: Iterator<Item = SyncEv>,
{
    type Item = DagBlock;

    fn next(&mut self) -> Option<Self::Item> {
        for syncev in &mut self.sync_ev_it {
            self.events.push_event(syncev);

            if self.events.len() >= self.cap {
                break;
            }
        }

        if self.events.len() == 0 {
            return None;
        }

        let mut block: DagBlock = take(&mut self.events).into();
        if self.trans_reduce {
            block.transitive_reduce(DagBlock::TRANS_REDUCE_REC_LIM);
        }
        if self.prune_unobserved {
            // helper events are implicitly unobserved
            block.prune_unobserved();
        } else {
            block.prune_helper();
        }
        Some(block)
    }
}

#[derive(Serialize)]
struct NextEv {
    thid: u64,
    id: u64,
}

impl Serialize for Event {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut obj = serializer.serialize_struct("Event", 7)?;

        obj.serialize_field("id", &self.id)?;
        obj.serialize_field("icnt", &self.insn_cnt)?;
        obj.serialize_field("hbal", &self.heap_balance)?;
        obj.serialize_field("hmax", &self.heap_max)?;
        obj.serialize_field("meta", &self.meta)?;
        obj.serialize_field(
            "nxt",
            &self
                .next
                .iter()
                .map(|(thid, next)| NextEv {
                    thid: u64::from(*thid),
                    id: next.borrow().id,
                })
                .collect::<Vec<_>>(),
        )?;
        obj.serialize_field("bks", &self.block_log)?;
        obj.serialize_field("bksu", &self.bk_usage)?;

        obj.end()
    }
}

impl Serialize for EventQueue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        for ev in self {
            seq.serialize_element::<Event>(&ev.borrow())?;
        }

        seq.end()
    }
}

impl Serialize for DagBlock {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_map(self.threads.iter().map(|(thid, q)| (thid, q)))
    }
}
