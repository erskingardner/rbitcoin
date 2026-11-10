//! A write batch reads the spent slots of already-archived parents once, on
//! `ibd-confirm-spent`, while the write thread appends Class A.
use super::spend_durable_tests::{mine, spend_one};
use bitcoin::hashes::Hash;
use bitcoin::{Amount, Block, BlockHash, Txid};
use rbitcoin_consensus::{ChainParams, ConsensusError, Milestone};
use rbitcoin_primitives::Height;
use rbitcoin_query::confirm_run::write::{
    spindle_trace_arm, spindle_trace_clear, spindle_trace_snapshot,
};
use rbitcoin_query::confirm_run::{
    confirm_scripts_phase, confirm_wire_load_from_plan, confirm_wire_lookup_stamp,
    confirm_write_phase, ScriptPreverified, WireBlockIn,
};
use rbitcoin_query::{accept_and_connect_block, ConfirmError, Query, ScriptOkBatch};
use std::sync::atomic::Ordering;
use std::sync::Arc;

const MS: Milestone = Milestone::NONE;

/// Connected regtest chain whose first `spendable` coinbases are mature at
/// the next height.
struct Chain {
    _dir: rbitcoin_query::testutil::TempDir,
    q: Query,
    params: ChainParams,
    tip: BlockHash,
    tip_time: u32,
    height: u32,
    /// `coinbases[i]` is the coinbase txid of height `i + 1`.
    coinbases: Vec<Txid>,
}

impl Chain {
    fn new(label: &str, spendable: u32, hdd: bool) -> Self {
        let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled(label);
        q.store().set_datadir_hdd(hdd);
        let params = ChainParams::regtest();
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, MS).unwrap();
        let mut chain = Self {
            _dir: dir,
            q,
            params,
            tip: genesis.block_hash(),
            tip_time: genesis.header.time,
            height: 0,
            coinbases: Vec::new(),
        };
        for _ in 0..chain.params.coinbase_maturity() + spendable - 1 {
            let b = chain.next(Vec::new());
            chain.coinbases.push(b.txdata[0].compute_txid());
            accept_and_connect_block(&chain.q, &chain.params, Height(chain.height), &b, MS)
                .unwrap();
        }
        chain
    }

    /// Mine the next block on the local tip. Connecting it is the caller's.
    fn next(&mut self, txs: Vec<bitcoin::Transaction>) -> Block {
        self.height += 1;
        self.tip_time += 600;
        let b = mine(self.tip, self.tip_time, self.height, txs);
        self.tip = b.block_hash();
        b
    }

    fn spend(&self, coinbase: usize, sat: u64) -> bitcoin::Transaction {
        spend_one(self.coinbases[coinbase], Amount::from_sat(sat))
    }

    /// Lookup, load, and scripts for one batch at the store tip.
    fn script_ok(&self, blocks: &[(u32, Block)]) -> ScriptOkBatch {
        let items: Vec<WireBlockIn> = blocks
            .iter()
            .map(|(h, b)| (Height(*h), Arc::new(b.clone()), None))
            .collect();
        let stamped =
            confirm_wire_lookup_stamp(&self.q, &self.params, MS, &items, None).expect("stamp");
        let mat = confirm_wire_load_from_plan(
            &self.q,
            &self.params,
            MS,
            stamped,
            None,
            &ScriptPreverified::new(),
        )
        .expect("load");
        confirm_scripts_phase(mat.batch).expect("scripts").batch
    }

    fn slot_reads(&self) -> SlotReads {
        let s = self.q.confirm_stats();
        SlotReads {
            reads: s.spend_meta_reads.load(Ordering::Relaxed),
            slots: s.spend_meta_n.load(Ordering::Relaxed),
            ahead: s.spend_meta_ahead_n.load(Ordering::Relaxed),
        }
    }
}

/// Slot-read counters. `since` is the delta one write added.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SlotReads {
    reads: u64,
    slots: u64,
    ahead: u64,
}

impl SlotReads {
    fn since(self, before: Self) -> Self {
        Self {
            reads: self.reads - before.reads,
            slots: self.slots - before.slots,
            ahead: self.ahead - before.ahead,
        }
    }
}

fn pos(trace: &[&str], ev: &str) -> Option<usize> {
    trace.iter().position(|e| *e == ev)
}

/// Three blocks, each spending a coinbase an earlier write archived: the
/// batch issues one slot read. Without `--datadir-hdd` that read starts
/// before the Class A append and is joined before `tx.head` insert and
/// structural. With it the read waits for the append to release the spindle.
#[test]
fn write_batch_reads_archived_parent_slots_once() {
    for hdd in [false, true] {
        let mut chain = Chain::new("spent-ahead-once", 3, hdd);
        let h = chain.height;
        let blocks: Vec<(u32, Block)> = (0..3)
            .map(|i| {
                let tx = chain.spend(i, 49_0000_0000);
                (h + 1 + i as u32, chain.next(vec![tx]))
            })
            .collect();
        let batch = chain.script_ok(&blocks);

        let before = chain.slot_reads();
        spindle_trace_arm();
        confirm_write_phase(&chain.q, &chain.params, MS, batch).expect("write");
        let trace = spindle_trace_snapshot();
        spindle_trace_clear();
        let got = chain.slot_reads().since(before);

        assert_eq!(got.reads, 1, "hdd={hdd}: one slot read per write batch");
        assert_eq!(got.slots, 3, "hdd={hdd}: every block's parent slot");
        let class_a = pos(&trace, "class-a").unwrap_or_else(|| panic!("{trace:?}"));
        let head = pos(&trace, "head").unwrap_or_else(|| panic!("{trace:?}"));
        if hdd {
            assert_eq!(got.ahead, 0, "the spindle case reads nothing ahead");
            assert_eq!(pos(&trace, "spent-ahead"), None, "{trace:?}");
            let release = pos(&trace, "release").unwrap_or_else(|| panic!("{trace:?}"));
            let late = pos(&trace, "spent-late").unwrap_or_else(|| panic!("{trace:?}"));
            assert!(class_a < release && release < late, "{trace:?}");
            assert!(late < pos(&trace, "ann-begin").unwrap(), "{trace:?}");
        } else {
            assert_eq!(got.ahead, 3, "archived parents are read ahead");
            let ahead = pos(&trace, "spent-ahead").unwrap_or_else(|| panic!("{trace:?}"));
            let join = pos(&trace, "spent-join").unwrap_or_else(|| panic!("{trace:?}"));
            assert!(
                ahead < class_a,
                "the read starts before the append: {trace:?}"
            );
            assert!(class_a < join, "the append overlaps the read: {trace:?}");
            assert!(join < head, "structural waits for the read: {trace:?}");
            assert_eq!(pos(&trace, "spent-late"), None, "{trace:?}");
        }
        for cb in &chain.coinbases[..3] {
            assert!(chain.q.is_outpoint_spent(cb.as_byte_array(), 0).unwrap());
        }
        assert_eq!(chain.q.tip_height(), Some(Height(h + 3)));
    }
}

/// A spend of an output an earlier batch spent is still `PrevoutSpent` when
/// its slot comes from the read ahead. The rejected batch already appended
/// Class A, so the retry of its valid block runs with no plan: that write
/// reads ahead too, and its annotate is what rejects the next respend.
#[test]
fn respend_of_an_earlier_batch_spend_is_rejected_from_the_read_ahead() {
    let mut chain = Chain::new("spent-ahead-respend", 3, false);
    let h = chain.height;
    let first = chain.spend(0, 49_0000_0000);
    let b1 = chain.next(vec![first]);
    rbitcoin_query::confirm_wire_run(&chain.q, &chain.params, MS, &[(Height(h + 1), b1)])
        .expect("the first spend connects");

    let ok_tx = chain.spend(1, 49_0000_0000);
    let b_ok = chain.next(vec![ok_tx]);
    let (ok_tip, ok_time) = (chain.tip, chain.tip_time);
    let respend = chain.spend(0, 48_0000_0000);
    let b_bad = chain.next(vec![respend]);
    let batch = chain.script_ok(&[(h + 2, b_ok.clone()), (h + 3, b_bad)]);
    let before = chain.slot_reads();
    let err = confirm_write_phase(&chain.q, &chain.params, MS, batch)
        .expect_err("an output an earlier batch spent must stay spent");
    assert!(
        matches!(err, ConfirmError::Rules(ConsensusError::PrevoutSpent)),
        "{err}"
    );
    let got = chain.slot_reads().since(before);
    assert_eq!(
        got,
        SlotReads {
            reads: 1,
            slots: 2,
            ahead: 2
        },
        "both blocks' archived parents come from one read ahead"
    );
    assert_eq!(chain.q.tip_height(), Some(Height(h + 1)));

    // Retry of the valid block: Class A is already on disk (plan = None).
    let batch = chain.script_ok(&[(h + 2, b_ok)]);
    assert!(
        batch.archive_plan.is_none(),
        "the retry is already archived"
    );
    let before = chain.slot_reads();
    confirm_write_phase(&chain.q, &chain.params, MS, batch).expect("the valid block connects");
    assert_eq!(
        chain.slot_reads().since(before),
        SlotReads {
            reads: 1,
            slots: 1,
            ahead: 1
        },
        "a write with no append still reads ahead"
    );
    assert_eq!(chain.q.tip_height(), Some(Height(h + 2)));

    let again = mine(
        ok_tip,
        ok_time + 600,
        h + 3,
        vec![chain.spend(1, 47_0000_0000)],
    );
    let err =
        rbitcoin_query::confirm_wire_run(&chain.q, &chain.params, MS, &[(Height(h + 3), again)])
            .expect_err("the retried block's spend is annotated before the next read");
    assert!(
        matches!(err, ConfirmError::Rules(ConsensusError::PrevoutSpent)),
        "{err}"
    );
}

/// IBD merges load batches into one write. Two blocks of that write spend the
/// same archived output: both read the same unspent slot, and the batch's
/// pending set rejects the second spend.
#[test]
fn second_spend_inside_one_write_batch_is_rejected() {
    use rbitcoin_query::{confirm_wire_load_phase_pipelined, InFlight, WireLoadPipeline};

    let mut chain = Chain::new("spent-ahead-pending", 2, false);
    let h = chain.height;
    let a = chain.next(vec![chain.spend(0, 49_0000_0000)]);
    let b = chain.next(vec![chain.spend(0, 48_0000_0000)]);
    let slices = [(Height(h + 1), a), (Height(h + 2), b)];

    let none = ScriptPreverified::new();
    let mut inflight = InFlight::new();
    let mut next_tx_start = chain.q.tx_body_count().saturating_add(1).max(1);
    let mut merged: Option<ScriptOkBatch> = None;
    for (i, slice) in slices.chunks(1).enumerate() {
        let pipe = WireLoadPipeline {
            path_lo: slice[0].0 .0,
            parent_hash: i
                .checked_sub(1)
                .map(|p| slices[p].1.block_hash().to_byte_array()),
            next_tx_start,
            in_flight: &inflight,
            skeleton: None,
            carried_need: Vec::new(),
            carried_header_fks: Vec::new(),
            carried_header_hashes: Vec::new(),
        };
        let mat = confirm_wire_load_phase_pipelined(
            &chain.q,
            &chain.params,
            MS,
            slice,
            &none,
            Some(&pipe),
        )
        .expect("each slice loads alone: load does not see the other spend");
        let plan = mat.batch.archive_plan.as_ref().expect("wire plan");
        inflight.note_pins(
            plan.planned_fks.iter().copied().zip(plan.batch_pin.iter()),
            Some(slice[0].0 .0),
        );
        if let Some(last) = plan.planned_fks.last().and_then(|f| f.get()) {
            next_tx_start = last + 1;
        }
        let ok = confirm_scripts_phase(mat.batch).expect("scripts").batch;
        merged = Some(match merged.take() {
            None => ok,
            Some(mut m) => {
                assert!(m.append_contiguous(ok).is_ok(), "slices merge");
                m
            }
        });
    }

    let before = chain.slot_reads();
    let err = confirm_write_phase(&chain.q, &chain.params, MS, merged.unwrap())
        .expect_err("the second spend of one output inside the batch");
    assert!(
        matches!(err, ConfirmError::Rules(ConsensusError::PrevoutSpent)),
        "{err}"
    );
    assert_eq!(
        chain.slot_reads().since(before),
        SlotReads {
            reads: 1,
            slots: 2,
            ahead: 2
        },
        "each block asks for the slot; one read serves both"
    );
    assert_eq!(chain.q.tip_height(), Some(Height(h)));
}

/// The read ahead fails after Class A appended. That is the write's error:
/// the write thread does not read those slots itself instead. The append
/// stays on disk with its header ranges, so the retry runs with no plan,
/// reads ahead again, and connects.
#[test]
fn failed_read_ahead_fails_the_write_and_the_retry_connects() {
    use rbitcoin_query::confirm_run::write::fail_next_spent_ahead;

    let mut chain = Chain::new("spent-ahead-fail", 2, false);
    let h = chain.height;
    let b1 = chain.next(vec![chain.spend(0, 49_0000_0000)]);
    let cb = chain.coinbases[0];
    let batch = chain.script_ok(&[(h + 1, b1.clone())]);
    assert!(
        batch.archive_plan.is_some(),
        "the first try appends Class A"
    );

    fail_next_spent_ahead();
    let before = chain.slot_reads();
    let err = confirm_write_phase(&chain.q, &chain.params, MS, batch)
        .expect_err("a failed read ahead is a failed write");
    assert!(
        matches!(
            err,
            ConfirmError::Store(rbitcoin_store::StoreError::Corrupt(
                "spent slot read ahead: injected failure"
            ))
        ),
        "{err}"
    );
    assert_eq!(
        chain.slot_reads().since(before).reads,
        0,
        "no slot read stands in for the failed one"
    );
    assert_eq!(chain.q.tip_height(), Some(Height(h)));
    assert!(!chain.q.is_outpoint_spent(cb.as_byte_array(), 0).unwrap());

    let batch = chain.script_ok(&[(h + 1, b1)]);
    assert!(
        batch.archive_plan.is_none(),
        "the failed write left its append and header ranges"
    );
    confirm_write_phase(&chain.q, &chain.params, MS, batch).expect("the retry connects");
    assert_eq!(chain.q.tip_height(), Some(Height(h + 1)));
    assert!(chain.q.is_outpoint_spent(cb.as_byte_array(), 0).unwrap());

    let respend = chain.next(vec![chain.spend(0, 48_0000_0000)]);
    let err =
        rbitcoin_query::confirm_wire_run(&chain.q, &chain.params, MS, &[(Height(h + 2), respend)])
            .expect_err("the retried spend stays spent");
    assert!(
        matches!(err, ConfirmError::Rules(ConsensusError::PrevoutSpent)),
        "{err}"
    );
}

/// The IBD write thread retries a batch whose blocks already connected. That
/// write only replays annotate: it has no structural pass and reads no slot.
#[test]
fn already_connected_batch_reads_no_slots() {
    let mut chain = Chain::new("spent-ahead-old", 1, false);
    let h = chain.height;
    let b1 = chain.next(vec![chain.spend(0, 49_0000_0000)]);
    let first = chain.script_ok(&[(h + 1, b1.clone())]);
    let again = chain.script_ok(&[(h + 1, b1)]);
    confirm_write_phase(&chain.q, &chain.params, MS, first).expect("write");

    let before = chain.slot_reads();
    spindle_trace_arm();
    confirm_write_phase(&chain.q, &chain.params, MS, again).expect("already connected");
    let trace = spindle_trace_snapshot();
    spindle_trace_clear();
    assert_eq!(
        chain.slot_reads().since(before),
        SlotReads {
            reads: 0,
            slots: 0,
            ahead: 0
        }
    );
    assert_eq!(pos(&trace, "spent-ahead"), None, "{trace:?}");
    assert_eq!(pos(&trace, "class-a"), None, "{trace:?}");
    assert!(pos(&trace, "ann-end").is_some(), "{trace:?}");
    assert_eq!(chain.q.tip_height(), Some(Height(h + 1)));
}

/// A write failed after its tip commit, so its spends are not annotated. The
/// next write replays that annotate before it starts the read ahead, and the
/// read sees the spend.
///
/// Turning the filter index on after live append started leaves its
/// watermark at 0, so the live seal fails after Class C.
#[test]
fn pending_annotate_replays_before_the_read_ahead() {
    let _gate = rbitcoin_consensus::script_pool::steal_test_gate();
    let mut chain = Chain::new("spent-ahead-replay", 1, false);
    let h = chain.height;
    let cb = chain.coinbases[0];
    chain.q.set_sptweaks_enabled(true, Height(0)).unwrap();
    rbitcoin_query::prepare_live_indexes(&chain.q).unwrap();
    chain.q.set_block_filter_index(true).unwrap();

    let y = chain.next(vec![chain.spend(0, 49_0000_0000)]);
    let err = accept_and_connect_block(&chain.q, &chain.params, Height(h + 1), &y, MS)
        .expect_err("the live seal fails after the tip commit");
    assert!(
        matches!(
            err,
            ConfirmError::Store(rbitcoin_store::StoreError::Corrupt(
                "invariant: live index commit"
            ))
        ),
        "{err}"
    );
    assert_eq!(
        chain.q.tip_height(),
        Some(Height(h + 1)),
        "Class C committed"
    );

    let respend = chain.next(vec![chain.spend(0, 48_0000_0000)]);
    spindle_trace_arm();
    let err = accept_and_connect_block(&chain.q, &chain.params, Height(h + 2), &respend, MS)
        .expect_err("the replayed spend must reject the respend");
    let trace = spindle_trace_snapshot();
    spindle_trace_clear();
    assert!(
        matches!(err, ConfirmError::Rules(ConsensusError::PrevoutSpent)),
        "{err}"
    );
    let replayed = pos(&trace, "ann-end").unwrap_or_else(|| panic!("{trace:?}"));
    let ahead = pos(&trace, "spent-ahead").unwrap_or_else(|| panic!("{trace:?}"));
    assert!(replayed < ahead, "replay, then read: {trace:?}");
    assert!(chain.q.is_outpoint_spent(cb.as_byte_array(), 0).unwrap());
}

mod unit {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::block_pipeline::{
        read_spent_slots, ClassAWave, SlotRead, StructuralAbsJob, StructuralScratch,
    };
    use rbitcoin_query::confirm_run::phases::{resolve_batch_slots, BatchSlots};
    use rbitcoin_query::confirm_run::{
        spend_abs_before_append, submit_spent_ahead, Prepared, SPENT_AHEAD_THREAD_NAME,
    };
    use rbitcoin_query::{BatchParents, ConfirmError};
    use rbitcoin_store::{spent_abs, InputRecord, OutputRecord, StoreError};
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    type Spend = ([u8; 32], u32, Fk, Fk, u32);

    const SLOT: u64 = OutputRecord::SPENT_SLOT_LEN as u64;

    fn rec_tx(b: u8, n_out: u32) -> rbitcoin_store::TxRecord {
        rbitcoin_store::TxRecord {
            txid: [b; 32],
            version: 1,
            locktime: 0,
            input_count: 1,
            output_count: n_out,
        }
    }

    fn prepared(height: u32, spends: Vec<Spend>) -> Prepared {
        Prepared {
            height: Height(height),
            header_fk: Fk(1),
            tx_fks: Vec::new(),
            jobs: Vec::new(),
            spends,
            fees: 0,
            check_scripts: false,
            time: 1,
            bits: bitcoin::CompactTarget::from_consensus(0x207f_ffff),
            hash: [height as u8; 32],
            txids: Vec::new(),
            prev_mtp: 0,
        }
    }

    #[test]
    fn spent_ahead_thread_is_named_and_reused() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        for _ in 0..2 {
            let seen_job = Arc::clone(&seen);
            let read = submit_spent_ahead(move || {
                let t = std::thread::current();
                seen_job
                    .lock()
                    .unwrap()
                    .push((t.id(), t.name().unwrap_or("").to_string()));
                Ok(SlotRead::default())
            })
            .join()
            .expect("read");
            assert!(read.abs.is_empty());
        }
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].1, SPENT_AHEAD_THREAD_NAME);
        assert_eq!(seen[0], seen[1], "one OS thread across batches");
        assert_ne!(seen[0].0, std::thread::current().id());
    }

    /// A write that returns early must not leave a read in flight.
    #[test]
    fn spent_ahead_drop_waits_for_the_read() {
        use std::sync::mpsc::{self, RecvTimeoutError};
        use std::time::Duration;

        let (go_tx, go_rx) = mpsc::channel();
        let handle = submit_spent_ahead(move || {
            let _ = go_rx.recv();
            Ok(SlotRead::default())
        });
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            drop(handle);
            let _ = done_tx.send(());
        });
        match done_rx.recv_timeout(Duration::from_millis(400)) {
            Err(RecvTimeoutError::Timeout) => {}
            Ok(()) => panic!("drop returned before the read finished"),
            Err(e) => panic!("drop waiter disconnected: {e}"),
        }
        go_tx.send(()).unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("drop finishes once the read does");
    }

    #[test]
    fn spent_ahead_failure_and_panic_are_hard_errors() {
        let err = submit_spent_ahead(|| Err(StoreError::Corrupt("slot read failed")))
            .join()
            .expect_err("the read error reaches the write thread");
        assert!(
            matches!(err, StoreError::Corrupt("slot read failed")),
            "{err}"
        );
        let err = submit_spent_ahead(|| panic!("slot read panic"))
            .join()
            .expect_err("a panic is an error, not an empty read");
        assert!(
            matches!(err, StoreError::Corrupt(m) if m.contains("panicked")),
            "{err}"
        );
        // The thread survives a panic.
        submit_spent_ahead(|| Ok(SlotRead::default()))
            .join()
            .expect("next read");
    }

    /// The walk before the append: parents archived by an earlier write give
    /// their slot offset in block order. This write's own creates and parents
    /// with no range yet do not, and the latter are what fill stamps.
    #[test]
    fn spend_abs_before_append_splits_archived_from_this_batch() {
        let out = |n: u32| -> Vec<(u32, OutputRecord)> {
            (0..n)
                .map(|v| (v, OutputRecord::unspent(1, vec![0x51])))
                .collect()
        };
        let mut bp = BatchParents::new();
        bp.insert_owned(Fk(1), rec_tx(1, 2), out(2), vec![0, 1], None, None, vec![]);
        bp.set_spent_range_only(Fk(1), (1000, 2 * SLOT));
        bp.insert_owned(Fk(5), rec_tx(5, 1), out(1), vec![0], None, None, vec![]);
        bp.set_spent_range_only(Fk(5), (2000, SLOT));
        // Fk 7 is the first create of this write.
        bp.insert_owned(Fk(7), rec_tx(7, 1), out(1), vec![0], None, None, vec![]);
        bp.set_spent_range_only(Fk(7), (3000, SLOT));
        // Fk 8 has no spent range until the append.
        bp.insert_owned(Fk(8), rec_tx(8, 2), out(2), vec![0, 1], None, None, vec![]);

        let blocks = [
            prepared(
                10,
                vec![
                    ([1; 32], 0, Fk(20), Fk(1), 0),
                    ([1; 32], 0, Fk(20), Fk(1), 1),
                    ([0; 32], 0, Fk(20), Fk::NULL, 2),
                    ([5; 32], 0, Fk(21), Fk(5), 0),
                    ([8; 32], 0, Fk(21), Fk(8), 1),
                    ([8; 32], 1, Fk::NULL, Fk(8), 2),
                ],
            ),
            prepared(
                11,
                vec![
                    ([1; 32], 1, Fk(22), Fk(1), 0),
                    ([5; 32], 0, Fk(22), Fk(5), 1),
                    ([7; 32], 0, Fk(23), Fk(7), 0),
                    ([8; 32], 1, Fk(23), Fk(8), 1),
                ],
            ),
        ];
        let got = spend_abs_before_append(&bp, &blocks, Some(7));
        assert_eq!(
            got.abs,
            vec![
                spent_abs(1000, 0),
                spent_abs(2000, 0),
                spent_abs(1000, 1),
                spent_abs(2000, 0),
            ],
            "block order; a repeat inside one block is one slot; each block asks again"
        );
        let mut need: Vec<(u64, Vec<u32>)> = got.need.into_iter().collect();
        need.sort();
        assert_eq!(
            need,
            vec![(8, vec![0, 1])],
            "a null spender is not a fill need"
        );

        // No plan: this write appends nothing, so fk 7 was archived earlier too.
        let got = spend_abs_before_append(&bp, &blocks, None);
        assert_eq!(got.abs.len(), 5);
        assert_eq!(got.abs[4], spent_abs(3000, 0));
    }

    struct Parents {
        _dir: rbitcoin_query::testutil::TempDir,
        q: rbitcoin_query::Query,
        fks: Vec<Fk>,
        abs: Vec<u64>,
    }

    /// Three one-output creates on disk. Parent 1's slot names spender fk 77.
    fn parents(label: &str) -> Parents {
        let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled(label);
        let rows: Vec<_> = (0..3u8)
            .map(|i| {
                (
                    rbitcoin_query::CreatePinInner::records(
                        rec_tx(0x60 + i, 1),
                        vec![OutputRecord::unspent(1, vec![0x51])],
                    ),
                    vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
                )
            })
            .collect();
        let overlay = [vec![], vec![(0u32, Fk(77), 3)], vec![]];
        let (fks, loc) = q
            .store()
            .put_tx_full_batch_from_pins(&rows, false, &overlay)
            .unwrap();
        let abs = loc.iter().map(|l| spent_abs(l.spent.0, 0)).collect();
        Parents {
            _dir: dir,
            q,
            fks,
            abs,
        }
    }

    fn job(p: &Parents, parent: usize, sfk: u64, vin: u32) -> StructuralAbsJob {
        (p.fks[parent].get().unwrap(), 0, p.abs[parent], Fk(sfk), vin)
    }

    fn spend(p: &Parents, parent: usize, sfk: u64, vin: u32) -> Spend {
        ([0x60 + parent as u8; 32], 0, Fk(sfk), p.fks[parent], vin)
    }

    fn counters(q: &rbitcoin_query::Query) -> [u64; 4] {
        let s = q.confirm_stats();
        [
            s.spend_meta_reads.load(Ordering::Relaxed),
            s.spend_meta_n.load(Ordering::Relaxed),
            s.spend_meta_ahead_n.load(Ordering::Relaxed),
            s.spend_overlay_skip_n.load(Ordering::Relaxed),
        ]
    }

    /// Slots read ahead attach to their jobs by offset, in order. A job with
    /// no slot read ahead (its create was appended by this write) is read
    /// after, in one read for the batch.
    #[test]
    fn resolve_takes_read_ahead_slots_then_reads_the_rest_once() {
        let p = parents("spent-ahead-resolve");
        let jobs = [
            vec![job(&p, 0, 50, 0), job(&p, 2, 50, 1)],
            vec![job(&p, 1, 51, 0), job(&p, 0, 51, 1), job(&p, 2, 51, 2)],
        ];
        let spends = [
            vec![spend(&p, 0, 50, 0), spend(&p, 2, 50, 1)],
            vec![
                spend(&p, 1, 51, 0),
                spend(&p, 0, 51, 1),
                spend(&p, 2, 51, 2),
            ],
        ];
        let ahead = read_spent_slots(p.q.store(), vec![p.abs[0], p.abs[1], p.abs[0]]).unwrap();
        assert_eq!(ahead.metas.len(), 3);

        let before = counters(&p.q);
        let mut out = BatchSlots::default();
        resolve_batch_slots(
            &p.q,
            spends
                .iter()
                .map(Vec::as_slice)
                .zip(jobs.iter().map(Vec::as_slice)),
            &ClassAWave::default(),
            &ahead,
            &mut StructuralScratch::default(),
            &mut out,
        )
        .expect("resolve");
        let after = counters(&p.q);
        assert_eq!(
            after[0] - before[0],
            2,
            "the read ahead, then one late read"
        );
        assert_eq!(after[1] - before[1], 5);
        assert_eq!(after[2] - before[2], 3);

        let unspent = Some((Fk::NULL, 0u8, 0u32));
        let b0 = out.block(0);
        assert_eq!(b0.jobs, &jobs[0][..]);
        assert_eq!(b0.metas, &[unspent, unspent][..]);
        let b1 = out.block(1);
        assert_eq!(b1.jobs, &jobs[1][..]);
        assert_eq!(
            b1.metas,
            &[Some((Fk(77), 0u8, 3u32)), unspent, unspent][..],
            "each job gets the slot at its own offset"
        );
    }

    /// The sole same-batch spender of a create this write appended needs no
    /// slot: Class A wrote it. Nothing is read for that job.
    #[test]
    fn resolve_skips_the_overlay_and_reads_nothing_for_it() {
        let p = parents("spent-ahead-overlay");
        let jobs = [vec![job(&p, 2, 50, 0)]];
        let spends = [vec![spend(&p, 2, 50, 0)]];
        let before = counters(&p.q);
        let mut out = BatchSlots::default();
        resolve_batch_slots(
            &p.q,
            spends
                .iter()
                .map(Vec::as_slice)
                .zip(jobs.iter().map(Vec::as_slice)),
            &ClassAWave::new(vec![p.fks[2]]),
            &SlotRead::default(),
            &mut StructuralScratch::default(),
            &mut out,
        )
        .expect("resolve");
        let after = counters(&p.q);
        assert_eq!(after[0] - before[0], 0, "no read");
        assert_eq!(after[3] - before[3], 1, "one overlay skip");
        assert!(out.block(0).jobs.is_empty());
    }

    /// Every slot read ahead must be a spend of this batch. One that no job
    /// claims means the walk before the append and the walk after disagree.
    #[test]
    fn resolve_rejects_a_read_ahead_slot_no_spend_claims() {
        let p = parents("spent-ahead-leftover");
        let jobs = [vec![job(&p, 0, 50, 0)]];
        let spends = [vec![spend(&p, 0, 50, 0)]];
        let ahead = read_spent_slots(p.q.store(), vec![p.abs[1]]).unwrap();
        let err = resolve_batch_slots(
            &p.q,
            spends
                .iter()
                .map(Vec::as_slice)
                .zip(jobs.iter().map(Vec::as_slice)),
            &ClassAWave::default(),
            &ahead,
            &mut StructuralScratch::default(),
            &mut BatchSlots::default(),
        )
        .expect_err("an unclaimed slot");
        assert!(
            matches!(
                err,
                ConfirmError::Store(StoreError::Corrupt(
                    "invariant: spent slot read ahead is not a spend of this batch"
                ))
            ),
            "{err}"
        );
    }

    /// An offset past the file is `None` from the reader. It stays a hard
    /// error: no second read, no "unspent".
    #[test]
    fn a_short_slot_read_ahead_is_a_hard_error() {
        use rbitcoin_query::block_pipeline::{structural_load_durable_spent, BlockSlots};
        let p = parents("spent-ahead-short");
        let far = p.abs[2] + (1 << 30);
        let ahead = read_spent_slots(p.q.store(), vec![far]).unwrap();
        assert_eq!(ahead.metas, vec![None]);
        let jobs = [(p.fks[0].get().unwrap(), 0u32, far, Fk(50), 0u32)];
        let err = structural_load_durable_spent(
            &p.q,
            None,
            &mut StructuralScratch::default(),
            BlockSlots {
                jobs: &jobs,
                metas: &ahead.metas,
            },
        )
        .expect_err("a short slot");
        assert!(format!("{err}").contains("short/OOB"), "{err}");
    }
}
