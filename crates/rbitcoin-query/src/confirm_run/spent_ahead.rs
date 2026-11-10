//! Process-wide `ibd-confirm-spent` thread: spent-slot read ahead of Class A.
//!
//! A write batch spends parents that earlier writes archived. Nothing writes
//! those slots between the previous batch's spend annotate and this batch's
//! own, so the write thread hands their offsets here before it appends
//! Class A and joins before structural. `--datadir-hdd` does not use this
//! thread: there the read waits for the append. One OS thread, so one
//! completion session.

use super::head_drain::{spawn_worker, DrainWorkers};
use crate::block_pipeline::{read_spent_slots, SlotRead};
use rbitcoin_store::StoreError;
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, OnceLock};

pub const SPENT_AHEAD_THREAD_NAME: &str = "ibd-confirm-spent";

fn pool() -> &'static DrainWorkers {
    static POOL: OnceLock<Arc<DrainWorkers>> = OnceLock::new();
    POOL.get_or_init(|| spawn_worker(SPENT_AHEAD_THREAD_NAME))
}

/// A slot read in flight on [`SPENT_AHEAD_THREAD_NAME`].
///
/// [`Drop`] joins, so a write that returns early leaves no read behind it.
pub struct SpentAhead {
    rx: Option<Receiver<Result<SlotRead, StoreError>>>,
}

impl SpentAhead {
    /// Nothing is read ahead. [`Self::join`] returns an empty read.
    pub fn idle() -> Self {
        Self { rx: None }
    }

    pub fn started(&self) -> bool {
        self.rx.is_some()
    }

    /// Wait for the read. A failed or panicked read is the error.
    pub fn join(mut self) -> Result<SlotRead, StoreError> {
        self.recv()
    }

    fn recv(&mut self) -> Result<SlotRead, StoreError> {
        let Some(rx) = self.rx.take() else {
            return Ok(SlotRead::default());
        };
        rx.recv().unwrap_or(Err(StoreError::Corrupt(
            "spent slot read-ahead thread gone",
        )))
    }
}

impl Drop for SpentAhead {
    fn drop(&mut self) {
        let _ = self.recv();
    }
}

/// Run `work` on [`SPENT_AHEAD_THREAD_NAME`].
pub fn submit_spent_ahead<F>(work: F) -> SpentAhead
where
    F: FnOnce() -> Result<SlotRead, StoreError> + Send + 'static,
{
    let pool = pool();
    let (tx, rx) = mpsc::sync_channel(1);
    pool.jobs
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push_back(Box::new(move || {
            let r = panic::catch_unwind(AssertUnwindSafe(work)).unwrap_or(Err(
                StoreError::Corrupt("spent slot read-ahead thread panicked"),
            ));
            let _ = tx.send(r);
        }));
    pool.cv.notify_one();
    SpentAhead { rx: Some(rx) }
}

/// Read the slots at `abs` on [`SPENT_AHEAD_THREAD_NAME`]. The job owns its
/// store handle and offsets.
pub fn read_spent_ahead(store: Arc<rbitcoin_store::Store>, abs: Vec<u64>) -> SpentAhead {
    submit_spent_ahead(move || read_spent_slots(&store, abs))
}
