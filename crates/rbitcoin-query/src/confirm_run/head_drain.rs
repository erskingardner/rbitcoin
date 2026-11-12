//! Process-wide `ibd-confirm-head` thread: write-behind `tx.head` insert.
//!
//! Flag off overlaps this insert with structural, Class C, and annotate.
//! `--datadir-hdd` starts the insert after annotate returns. One inserter;
//! the thread stays up so each batch does not `thread::scope` spawn/teardown.
//!
//! The thread also frees what a written batch still holds
//! ([`submit_head_free`]). Jobs run in queue order, so a free submitted
//! after one write returns runs before the next write's insert, and that
//! write joins its insert: one free is queued or running at a time.

use rbitcoin_store::StoreError;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
#[cfg(debug_assertions)]
use std::thread::ThreadId;

pub const HEAD_DRAIN_THREAD_NAME: &str = "ibd-confirm-head";

#[doc(hidden)]
pub type Job = Box<dyn FnOnce() + Send>;

#[doc(hidden)]
pub struct DrainWorkers {
    pub jobs: Mutex<VecDeque<Job>>,
    pub cv: Condvar,
}

#[doc(hidden)]
pub fn pool() -> &'static DrainWorkers {
    static POOL: OnceLock<DrainWorkers> = OnceLock::new();
    static SPAWN: OnceLock<()> = OnceLock::new();
    let pool = POOL.get_or_init(|| DrainWorkers {
        jobs: Mutex::new(VecDeque::new()),
        cv: Condvar::new(),
    });
    SPAWN.get_or_init(|| {
        let jobs = &pool.jobs;
        let cv = &pool.cv;
        thread::Builder::new()
            .name(HEAD_DRAIN_THREAD_NAME.into())
            .spawn(move || loop {
                let f = recv_job(jobs, cv);
                f();
            })
            .expect("spawn ibd-confirm-head");
    });
    pool
}

#[doc(hidden)]
pub fn recv_job(jobs: &Mutex<VecDeque<Job>>, cv: &Condvar) -> Job {
    let mut g = jobs.lock().unwrap_or_else(|p| p.into_inner());
    loop {
        if let Some(job) = g.pop_front() {
            return job;
        }
        g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
    }
}

#[doc(hidden)]
pub type HeadInsert = ([u8; 32], rbitcoin_primitives::Fk);

pub struct HeadDrainHandle<'a> {
    pub rx: Option<Receiver<Result<u64, StoreError>>>,
    pub restore: Option<Arc<Mutex<Option<Vec<HeadInsert>>>>>,
    /// The insert job's raw store pointer is only sound while this borrow
    /// is live. [`Drop`] joins. Do not `mem::forget` the handle.
    pub _store: PhantomData<&'a rbitcoin_store::Store>,
    #[cfg(debug_assertions)]
    pub named: Arc<Mutex<Option<(ThreadId, String)>>>,
}

impl HeadDrainHandle<'_> {
    /// Join and, on insert failure, return the batch so the write thread can
    /// put it back on pending-head (no clone on the success path).
    pub fn join_restore(mut self) -> (Result<u64, StoreError>, Vec<HeadInsert>) {
        let r = self.recv_result();
        let queued = self
            .restore
            .take()
            .and_then(|a| a.lock().unwrap_or_else(|p| p.into_inner()).take())
            .unwrap_or_default();
        (r, queued)
    }

    #[doc(hidden)]
    pub fn recv_result(&mut self) -> Result<u64, StoreError> {
        let rx = self
            .rx
            .take()
            .ok_or(StoreError::Corrupt("tx.head drain handle joined twice"))?;
        rx.recv().unwrap_or(Err(StoreError::Corrupt(
            "tx.head write-behind drain thread gone",
        )))
    }

    #[cfg(debug_assertions)]
    pub fn join_named(mut self) -> (Result<u64, StoreError>, ThreadId, String) {
        let r = self.recv_result();
        let (id, name) = self
            .named
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .unwrap_or_else(|| (thread::current().id(), String::new()));
        (r, id, name)
    }
}

impl Drop for HeadDrainHandle<'_> {
    fn drop(&mut self) {
        if self.rx.is_some() {
            let _ = self.recv_result();
        }
    }
}

/// Run `work` on [`HEAD_DRAIN_THREAD_NAME`]. Caller must join before captured
/// store pointers go out of scope.
pub fn submit_head_drain<F>(work: F) -> HeadDrainHandle<'static>
where
    F: FnOnce() -> Result<u64, StoreError> + Send + 'static,
{
    let pool = pool();
    let (tx, rx) = mpsc::sync_channel(1);
    #[cfg(debug_assertions)]
    let named = Arc::new(Mutex::new(None));
    #[cfg(debug_assertions)]
    let named_job = Arc::clone(&named);
    {
        let mut q = pool.jobs.lock().unwrap_or_else(|p| p.into_inner());
        q.push_back(Box::new(move || {
            #[cfg(debug_assertions)]
            {
                *named_job.lock().unwrap_or_else(|p| p.into_inner()) = Some((
                    thread::current().id(),
                    thread::current().name().unwrap_or("").to_string(),
                ));
            }
            let r = panic::catch_unwind(AssertUnwindSafe(work)).unwrap_or({
                Err(StoreError::Corrupt(
                    "tx.head write-behind drain thread panicked",
                ))
            });
            let _ = tx.send(r);
        }));
    }
    pool.cv.notify_one();
    HeadDrainHandle {
        rx: Some(rx),
        restore: None,
        _store: PhantomData,
        #[cfg(debug_assertions)]
        named,
    }
}

/// Run `free` on [`HEAD_DRAIN_THREAD_NAME`] after the jobs already queued.
/// Nothing joins it. A panic in `free` is caught, so the thread keeps
/// serving inserts.
pub fn submit_head_free<F>(free: F)
where
    F: FnOnce() + Send + 'static,
{
    let pool = pool();
    {
        let mut q = pool.jobs.lock().unwrap_or_else(|p| p.into_inner());
        q.push_back(Box::new(move || {
            let _ = panic::catch_unwind(AssertUnwindSafe(free));
        }));
    }
    pool.cv.notify_one();
}

/// Wait until every job queued before this call has run.
pub fn join_head_frees() {
    let (tx, rx) = mpsc::sync_channel(1);
    submit_head_free(move || {
        let _ = tx.send(());
    });
    let _ = rx.recv();
}

#[doc(hidden)]
pub struct SendStorePtr(usize);
impl SendStorePtr {
    #[doc(hidden)]
    pub fn from_store(store: &rbitcoin_store::Store) -> Self {
        Self(store as *const rbitcoin_store::Store as usize)
    }
    #[doc(hidden)]
    pub fn insert(self, batch: &[([u8; 32], rbitcoin_primitives::Fk)]) -> Result<u64, StoreError> {
        // SAFETY: `HeadDrainHandle` borrows `Store` until join. The handle's
        // `Drop` waits for this insert. Do not `mem::forget` the handle.
        unsafe {
            (*(self.0 as *const rbitcoin_store::Store))
                .txs
                .head_insert_queued(batch)
        }
    }
}

/// Insert a taken pending-head batch on [`HEAD_DRAIN_THREAD_NAME`].
pub fn submit_head_insert<'a>(
    store: &'a rbitcoin_store::Store,
    batch: Vec<([u8; 32], rbitcoin_primitives::Fk)>,
) -> HeadDrainHandle<'a> {
    let ptr = SendStorePtr::from_store(store);
    let restore = Arc::new(Mutex::new(None));
    let restore_job = Arc::clone(&restore);
    let mut handle = submit_head_drain(move || match ptr.insert(&batch) {
        Ok(n) => Ok(n),
        Err(e) => {
            *restore_job.lock().unwrap_or_else(|p| p.into_inner()) = Some(batch);
            Err(e)
        }
    });
    handle.restore = Some(restore);
    handle
}
