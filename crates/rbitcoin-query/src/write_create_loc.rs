//! Write-thread loc pairs from Class A append (just-written parents).
//!
//! Lookup TipOnly reads `create.loc` after [`crate::Query::note_lookup_tiponly_start`].
//! At note, `keep_until` is the last height whose TipOnly had started
//! (`lookup_started_hi`, at least the noting pack). That value is never bumped.
//! Prune drops a pack when that height has finished write (`written_hi ≥
//! keep_until`) and the pack is not the one just written (`pack_height <
//! written_hi`). Fill of that write runs first. A later-wave spend takes
//! TipOnly loc on the in-flight identity when its wave read the parent. A
//! parent the wave left out, because its create was above the wave's
//! snapshot, is a hole like a same-wave create. Its loc is the in-flight
//! pin's once Class A has `set_loc`, and otherwise comes from this window:
//! a parent written after the wave's TipOnly started is noted with a
//! `keep_until` at or above that wave.
//!
//! This window is **write-thread TLS** (one writer per thread — same ownership
//! as load's [`crate::InFlight`], not a `Query` mutex). Disconnect is polled
//! via [`crate::Query::take_disconnect`] on the write thread.

use rbitcoin_primitives::Fk;
use rbitcoin_store::CreateLocPair;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

use crate::Query;

#[doc(hidden)]
pub const PAIR_BYTES: u64 = std::mem::size_of::<CreateLocPair>() as u64;

/// Last TipOnly'd height at note, floored at `pack_height`.
pub fn keep_until_at_note(pack_height: u32, started_hi: Option<u32>) -> u32 {
    started_hi.unwrap_or(pack_height).max(pack_height)
}

#[derive(Debug)]
#[doc(hidden)]
pub struct LocPack {
    pub keep_until: u32,
    pub base: u64,
    pub pairs: Vec<CreateLocPair>,
    pub approx_bytes: u64,
}

impl LocPack {
    #[doc(hidden)]
    pub fn get(&self, id: u64) -> Option<CreateLocPair> {
        let off = id.checked_sub(self.base)?;
        self.pairs.get(off as usize).copied()
    }
}

/// Write-owned loc window. Note after append; prune after that write batch
/// (and later overlapping batches) finish.
#[derive(Debug, Default)]
pub struct WriteCreateLocRam {
    pub by_height: BTreeMap<u32, LocPack>,
    pub approx_bytes: u64,
}

impl WriteCreateLocRam {
    pub fn note(&mut self, pack_height: u32, keep_until: u32, fks: &[Fk], loc: &[CreateLocPair]) {
        if fks.is_empty() || loc.len() != fks.len() {
            return;
        }
        let Some(start) = fks[0].get() else {
            return;
        };
        let bytes = (loc.len() as u64).saturating_mul(PAIR_BYTES);
        if let Some(old) = self.by_height.insert(
            pack_height,
            LocPack {
                keep_until,
                base: start,
                pairs: loc.to_vec(),
                approx_bytes: bytes,
            },
        ) {
            self.approx_bytes = self.approx_bytes.saturating_sub(old.approx_bytes);
        }
        self.approx_bytes = self.approx_bytes.saturating_add(bytes);
    }

    pub fn get(&self, fk: Fk) -> Option<CreateLocPair> {
        let id = fk.get()?;
        for pack in self.by_height.values() {
            if let Some(p) = pack.get(id) {
                return Some(p);
            }
        }
        None
    }

    /// Drop packs whose last-started-at-note height has finished write.
    ///
    /// `keep_until` is fixed at note. Equality drops once `written_hi` covers
    /// that height, except the noting pack (`pack_height < written_hi`).
    pub fn prune_written_through(&mut self, written_hi: u32) {
        let drop: Vec<u32> = self
            .by_height
            .iter()
            .filter(|(h, p)| **h < written_hi && p.keep_until <= written_hi)
            .map(|(h, _)| *h)
            .collect();
        for h in drop {
            if let Some(p) = self.by_height.remove(&h) {
                self.approx_bytes = self.approx_bytes.saturating_sub(p.approx_bytes);
            }
        }
    }

    /// Disconnect: drop packs at/above `height`. Remaining keep-until cannot
    /// wait for disconnected lookup batches.
    pub fn drop_from_height(&mut self, height: u32) {
        let drop = self.by_height.split_off(&height);
        for p in drop.into_values() {
            self.approx_bytes = self.approx_bytes.saturating_sub(p.approx_bytes);
        }
        let cap = height.saturating_sub(1);
        for p in self.by_height.values_mut() {
            if p.keep_until > cap {
                p.keep_until = cap;
            }
        }
    }

    /// Packs, pair count, approx bytes (IBD `wloc=`).
    pub fn size_snapshot(&self) -> (usize, usize, u64) {
        let pairs: usize = self.by_height.values().map(|p| p.pairs.len()).sum();
        (self.by_height.len(), pairs, self.approx_bytes)
    }
}

thread_local! {
    static RAM: RefCell<WriteCreateLocRam> = RefCell::new(WriteCreateLocRam::default());
    static DISCONNECT_SEEN: Cell<u64> = const { Cell::new(0) };
}

#[doc(hidden)]
pub fn publish(ram: &WriteCreateLocRam) {
    let (packs, pairs, bytes) = ram.size_snapshot();
    crate::process_mem_stats::note_wloc(packs, pairs, bytes);
}

/// Drop this thread's loc window (Query open / test isolation). Does not publish
/// zeros — `wloc=` atomics are process-wide like `iflight=`.
pub fn clear() {
    RAM.with(|c| {
        *c.borrow_mut() = WriteCreateLocRam::default();
    });
    DISCONNECT_SEEN.with(|c| c.set(0));
}

pub fn with_ram<R>(query: &Query, f: impl FnOnce(&mut WriteCreateLocRam) -> R) -> R {
    RAM.with(|c| {
        let mut ram = c.borrow_mut();
        poll_disconnect(query, &mut ram);
        let r = f(&mut ram);
        publish(&ram);
        r
    })
}

#[doc(hidden)]
pub fn poll_disconnect(query: &Query, ram: &mut WriteCreateLocRam) {
    let mut seen = DISCONNECT_SEEN.with(|c| c.get());
    if let Some(h) = query.take_disconnect(&mut seen) {
        ram.drop_from_height(h);
        DISCONNECT_SEEN.with(|c| c.set(seen));
    }
}
