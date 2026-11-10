//! Sidecar thread that frees what the confirm write thread is done with.
//!
//! A written batch is mostly memory other threads allocated. Freeing it is
//! cross-thread allocator work, and the write thread sets IBD speed, so the
//! write thread hands the batch here and starts the next one. Cap and
//! accounting: `docs/ibd-memory.md`.

use super::ConfirmQueueDepths;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub const DROP_THREAD_NAME: &str = "ibd-confirm-drop";

/// Values that may wait for the thread. One more is being dropped.
pub const DROP_QUEUE_CAP: usize = 1;

/// Where [`DropThread::retire`] freed the value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropSite {
    Sidecar,
    Inline,
}

/// Owner of the drop thread. Field order is drop order: the queue closes,
/// then the join waits for the thread to free what was queued.
pub struct DropThread<T: Send + 'static> {
    tx: SyncSender<T>,
    _join: JoinOnDrop,
}

struct JoinOnDrop(Option<JoinHandle<()>>);

impl Drop for JoinOnDrop {
    fn drop(&mut self) {
        if let Some(thread) = self.0.take() {
            let _ = thread.join();
        }
    }
}

impl<T: Send + 'static> DropThread<T> {
    /// `dropped` runs on the thread after each drop, with that drop's wall.
    pub fn spawn(cap: usize, dropped: impl Fn(Duration) + Send + 'static) -> std::io::Result<Self> {
        let (tx, rx) = sync_channel::<T>(cap);
        let thread = std::thread::Builder::new()
            .name(DROP_THREAD_NAME.into())
            .spawn(move || {
                for value in rx {
                    let t = Instant::now();
                    drop(value);
                    dropped(t.elapsed());
                }
            })?;
        Ok(Self {
            tx,
            _join: JoinOnDrop(Some(thread)),
        })
    }

    /// Free `value` on the thread, or here when its queue is full.
    pub fn retire(&self, value: T) -> DropSite {
        match self.tx.try_send(value) {
            Ok(()) => DropSite::Sidecar,
            Err(TrySendError::Full(value) | TrySendError::Disconnected(value)) => {
                drop(value);
                DropSite::Inline
            }
        }
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
