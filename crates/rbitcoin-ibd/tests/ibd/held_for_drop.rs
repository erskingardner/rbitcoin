//! Written batches are freed on the head thread, and counted until gone.
use bitcoin::hashes::Hash;
use rbitcoin_ibd::ibd::confirm::held_for_drop::free_on_head;
use rbitcoin_ibd::ibd::confirm::ConfirmQueueDepths;
use rbitcoin_query::confirm_run::{join_head_frees, HEAD_DRAIN_THREAD_NAME};
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::time::Duration;

/// The value is dropped on the head thread, its bytes are counted until
/// then, and the free is timed.
#[test]
fn a_held_value_is_freed_and_timed_on_the_head_thread() {
    struct Probe(Sender<Option<String>>);
    impl Drop for Probe {
        fn drop(&mut self) {
            std::thread::sleep(Duration::from_millis(2));
            let _ = self
                .0
                .send(std::thread::current().name().map(str::to_owned));
        }
    }

    let queues = ConfirmQueueDepths::new();
    let stats = Arc::new(rbitcoin_query::ConfirmStats::default());
    let (tx, dropped_on) = channel();
    free_on_head(queues.hold_for_drop(Probe(tx), 500), Arc::clone(&stats));
    let name = dropped_on
        .recv_timeout(Duration::from_secs(10))
        .expect("the value is dropped without the caller");
    assert_eq!(name.as_deref(), Some(HEAD_DRAIN_THREAD_NAME));
    assert_ne!(name.as_deref(), std::thread::current().name());
    join_head_frees();
    assert_eq!(queues.content_snap().drop_wire_bytes, 0);
    assert!(stats.take_window().thr_drop_work_ns >= 2_000_000);
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
/// batch to the head thread, and nothing is held once the engine has joined.
#[test]
fn the_engine_frees_a_written_batch_off_the_write_thread() {
    use rbitcoin_consensus::mine_empty_regtest;
    use rbitcoin_ibd::ibd::confirm::{spawn_confirm_engine, ConfirmEvent, ConfirmFeed};
    use rbitcoin_ibd::ibd::status::LoopStats;
    use std::sync::atomic::AtomicU32;
    use std::sync::mpsc::RecvTimeoutError;
    use std::time::Instant;

    let (dir, hub) = rbitcoin_net::chain::tiny_regtest_hub_labeled("write-free-on-head");
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
        "the head thread freed the written batch"
    );
    assert_eq!(
        queues.content_snap().drop_wire_bytes,
        0,
        "nothing is held after the engine joins"
    );
    let _ = std::fs::remove_dir_all(dir);
}
