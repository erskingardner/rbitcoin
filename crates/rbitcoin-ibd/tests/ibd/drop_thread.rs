//! The sidecar that frees what the confirm write thread is done with.
use bitcoin::hashes::Hash;
use rbitcoin_ibd::ibd::confirm::drop_thread::{
    DropSite, DropThread, DROP_QUEUE_CAP, DROP_THREAD_NAME,
};
use rbitcoin_ibd::ibd::confirm::ConfirmQueueDepths;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

/// Reports the thread its `Drop` ran on. With a gate, `Drop` first says it
/// started and then waits for the gate to open.
struct Probe {
    dropped_on: Sender<Option<String>>,
    gate: Option<(Sender<()>, Receiver<()>)>,
}

impl Drop for Probe {
    fn drop(&mut self) {
        if let Some((started, open)) = &self.gate {
            let _ = started.send(());
            let _ = open.recv();
        }
        let _ = self
            .dropped_on
            .send(std::thread::current().name().map(str::to_owned));
    }
}

#[test]
fn a_retired_value_is_dropped_on_the_sidecar_thread() {
    let (tx, dropped_on) = channel();
    let drops = DropThread::spawn(DROP_QUEUE_CAP, |_| {}).expect("spawn");
    assert_eq!(
        drops.retire(Probe {
            dropped_on: tx,
            gate: None,
        }),
        DropSite::Sidecar
    );
    let name = dropped_on
        .recv_timeout(Duration::from_secs(10))
        .expect("the value is dropped without the caller");
    assert_eq!(name.as_deref(), Some(DROP_THREAD_NAME));
    assert_ne!(name.as_deref(), std::thread::current().name());
}

/// One value is being dropped and the queue holds its cap. The next value is
/// dropped by the caller before `retire` returns. Dropping the handle frees
/// what was queued and joins the thread.
#[test]
fn a_full_queue_drops_on_the_caller_and_shutdown_frees_the_rest() {
    let (tx, dropped_on) = channel();
    let (started_tx, started) = channel();
    let (open, gate) = channel();
    let timed = Arc::new(AtomicUsize::new(0));
    let drops = DropThread::spawn(DROP_QUEUE_CAP, {
        let timed = Arc::clone(&timed);
        move |_| {
            timed.fetch_add(1, Ordering::Relaxed);
        }
    })
    .expect("spawn");
    let probe = || Probe {
        dropped_on: tx.clone(),
        gate: None,
    };

    assert_eq!(
        drops.retire(Probe {
            dropped_on: tx.clone(),
            gate: Some((started_tx, gate)),
        }),
        DropSite::Sidecar
    );
    started
        .recv_timeout(Duration::from_secs(10))
        .expect("the sidecar is inside the first drop");
    for _ in 0..DROP_QUEUE_CAP {
        assert_eq!(drops.retire(probe()), DropSite::Sidecar);
    }

    assert_eq!(drops.retire(probe()), DropSite::Inline);
    assert_eq!(
        dropped_on
            .try_recv()
            .expect("dropped before retire returned")
            .as_deref(),
        std::thread::current().name()
    );
    assert!(
        dropped_on.try_recv().is_err(),
        "the queued values are still held"
    );

    open.send(()).unwrap();
    drop(drops);
    let rest: Vec<Option<String>> = dropped_on.try_iter().collect();
    assert_eq!(rest.len(), 1 + DROP_QUEUE_CAP, "{rest:?}");
    assert!(
        rest.iter().all(|n| n.as_deref() == Some(DROP_THREAD_NAME)),
        "{rest:?}"
    );
    assert_eq!(
        timed.load(Ordering::Relaxed),
        1 + DROP_QUEUE_CAP,
        "one timing per sidecar drop and none for the inline one"
    );
}

/// The in-flight pin window reads this sum, so a batch waiting to be freed
/// still counts against the 1 GiB line until its memory is gone.
#[test]
fn held_bytes_stay_in_the_accounted_wire_until_the_value_is_gone() {
    struct ReadsHeld(Arc<ConfirmQueueDepths>, Sender<usize>);
    impl Drop for ReadsHeld {
        fn drop(&mut self) {
            let _ = self.1.send(self.0.content_snap().drop_wire_bytes);
        }
    }

    let queues = ConfirmQueueDepths::new();
    queues.note_load_send(1, 10);
    queues.note_script_send(1, 20, 0);
    queues.note_write_send(1, 30, 0);
    assert_eq!(queues.content_snap().wire_bytes(), 60);

    let (tx, seen) = channel();
    let held = queues.hold_for_drop(ReadsHeld(Arc::clone(&queues), tx), 2_000);
    assert_eq!(queues.content_snap().drop_wire_bytes, 2_000);
    assert_eq!(queues.content_snap().wire_bytes(), 2_060);
    drop(held);
    assert_eq!(
        seen.recv().unwrap(),
        2_000,
        "still counted while the value is being dropped"
    );
    assert_eq!(queues.content_snap().drop_wire_bytes, 0);
    assert_eq!(queues.content_snap().wire_bytes(), 60);
}

/// One block through the confirm engine. The write thread hands the written
/// batch to the sidecar, and nothing is held once the engine has joined.
#[test]
fn the_engine_frees_a_written_batch_off_the_write_thread() {
    use rbitcoin_consensus::mine_empty_regtest;
    use rbitcoin_ibd::ibd::confirm::{spawn_confirm_engine, ConfirmEvent, ConfirmFeed};
    use rbitcoin_ibd::ibd::status::LoopStats;
    use std::sync::atomic::AtomicU32;
    use std::sync::mpsc::RecvTimeoutError;
    use std::time::Instant;

    let (dir, hub) = rbitcoin_net::chain::tiny_regtest_hub_labeled("write-drop-thread");
    hub.query.enter_direct_index_mode().unwrap();
    let hub = Arc::new(hub);
    hub.ensure_genesis().unwrap();
    let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
    let b1 = mine_empty_regtest(genesis.block_hash(), genesis.header.time + 600, 1);

    let feed = Arc::new(ConfirmFeed::new());
    let (ev_tx, ev_rx) = channel();
    let (engine, queues) = spawn_confirm_engine(
        Arc::clone(&hub),
        Arc::clone(&feed),
        ev_tx,
        Arc::new(AtomicU32::new(0)),
        Arc::new(LoopStats::default()),
    );
    hub.query
        .block_queue_enqueue(
            1,
            b1.block_hash().to_byte_array(),
            1,
            &bitcoin::consensus::serialize(&b1),
        )
        .unwrap();
    feed.note(1, b1.block_hash());
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match ev_rx.recv_timeout(Duration::from_millis(5)) {
            Ok(ConfirmEvent::Accepted { hash, .. }) => {
                assert_eq!(hash, b1.block_hash());
                break;
            }
            Ok(ConfirmEvent::Reject { height, err, .. }) => {
                panic!("confirm reject @{height}: {err}")
            }
            Err(RecvTimeoutError::Timeout) => {
                assert!(Instant::now() < deadline, "block 1 did not connect")
            }
            Err(RecvTimeoutError::Disconnected) => panic!("confirm engine exited"),
        }
    }
    feed.request_stop();
    feed.notify();
    let _ = engine.join();

    assert_eq!(hub.tip_height(), Some(1));
    let window = hub.query.confirm_stats().take_window();
    assert!(
        window.thr_drop_work_ns > 0,
        "the sidecar freed the written batch"
    );
    assert_eq!(
        window.write_drop_inline, 0,
        "an empty queue takes the batch"
    );
    assert_eq!(
        queues.content_snap().drop_wire_bytes,
        0,
        "nothing is held after the engine joins"
    );
    let _ = std::fs::remove_dir_all(dir);
}
