//! Frees what the confirm write thread is done with, on the head thread.
//!
//! A written batch is mostly memory other threads allocated. Freeing it is
//! cross-thread allocator work, and the write thread sets IBD speed, so the
//! write thread hands the batch to `ibd-confirm-head` and starts the next
//! one. That thread is idle between one write's `tx.head` insert and the
//! next. Accounting: `docs/ibd-memory.md`.

use super::{confirm_thr_stats, ConfirmQueueDepths};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

/// Free `held` on the head thread, behind the jobs it already has, and add
/// the free's wall to `thr_drop_work_ns`.
pub fn free_on_head<T: Send + 'static>(
    held: HeldForDrop<T>,
    stats: Arc<rbitcoin_query::ConfirmStats>,
) {
    rbitcoin_query::confirm_run::submit_head_free(move || {
        let t = Instant::now();
        drop(held);
        confirm_thr_stats::add_drop_work(&stats, t.elapsed());
    });
}

/// Waits, when dropped, for the head thread to finish the frees queued so
/// far. The write thread holds one, so nothing is held once it has exited.
pub struct JoinHeadFrees;

impl Drop for JoinHeadFrees {
    fn drop(&mut self) {
        rbitcoin_query::confirm_run::join_head_frees();
    }
}

/// A value on its way to being freed. Field order is drop order: the value
/// goes first, then its wire bytes leave the accounted heap.
pub struct HeldForDrop<T> {
    _value: T,
    _release: DropWireRelease,
}

struct DropWireRelease {
    queues: Arc<ConfirmQueueDepths>,
    wire_bytes: usize,
}

impl Drop for DropWireRelease {
    fn drop(&mut self) {
        let _ = self
            .queues
            .drop_wire_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(self.wire_bytes))
            });
    }
}

impl ConfirmQueueDepths {
    /// Count `wire_bytes` as held until the returned value is dropped.
    pub fn hold_for_drop<T>(self: &Arc<Self>, value: T, wire_bytes: usize) -> HeldForDrop<T> {
        let _ = self
            .drop_wire_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(wire_bytes))
            });
        HeldForDrop {
            _value: value,
            _release: DropWireRelease {
                queues: Arc::clone(self),
                wire_bytes,
            },
        }
    }
}
