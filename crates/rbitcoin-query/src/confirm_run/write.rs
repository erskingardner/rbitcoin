//! Write / Class C commit stage.

use super::phases::{class_c_commit, post_commit, structural_run};
use super::*;
use std::cell::RefCell;

thread_local! {
    static WRITE_REUSE: RefCell<super::phases::StructuralReuse> =
        RefCell::new(super::phases::StructuralReuse::default());
}

#[cfg(debug_assertions)]
thread_local! {
    static SPINDLE_TRACE_ON: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static SPINDLE_TRACE: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
    static FAIL_SPENT_AHEAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Make the next read ahead this thread starts fail on `ibd-confirm-spent`,
/// the way a failed `spent.body` read does. The bulk reader's own fault arms
/// are per thread or libc-only, so they do not reach that thread's session.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn fail_next_spent_ahead() {
    FAIL_SPENT_AHEAD.with(|fail| fail.set(true));
}

#[doc(hidden)]
pub fn spindle_mark(_ev: &'static str) {
    #[cfg(debug_assertions)]
    if SPINDLE_TRACE_ON.with(|on| on.get()) {
        SPINDLE_TRACE.with(|trace| trace.borrow_mut().push(_ev));
    }
}

/// Start recording on this thread, from an empty trace. A debug node runs
/// `spindle_mark` on every batch and nothing in the node reads the trace, so
/// it records only after this call.
#[cfg(debug_assertions)]
pub fn spindle_trace_arm() {
    SPINDLE_TRACE.with(|trace| trace.borrow_mut().clear());
    SPINDLE_TRACE_ON.with(|on| on.set(true));
}

/// Stop recording on this thread and drop what was recorded.
#[cfg(debug_assertions)]
pub fn spindle_trace_clear() {
    SPINDLE_TRACE_ON.with(|on| on.set(false));
    SPINDLE_TRACE.with(|trace| trace.borrow_mut().clear());
}

#[cfg(debug_assertions)]
pub fn spindle_trace_snapshot() -> Vec<&'static str> {
    SPINDLE_TRACE.with(|trace| trace.borrow().clone())
}

#[doc(hidden)]
pub struct SpindleScope<'a> {
    pub _hold: rbitcoin_store::SpindleHold<'a>,
    pub armed: bool,
}

impl Drop for SpindleScope<'_> {
    fn drop(&mut self) {
        if self.armed {
            spindle_mark("release");
        }
    }
}

#[doc(hidden)]
pub fn hold_spindle(query: &Query) -> SpindleScope<'_> {
    let armed = query.store().datadir_hdd();
    let scope = SpindleScope {
        _hold: query.store().spindle_hold(),
        armed,
    };
    if armed {
        spindle_mark("hold");
    }
    scope
}

#[doc(hidden)]
pub fn submit_queued<'a>(
    query: &'a Query,
    queued: Vec<([u8; 32], rbitcoin_primitives::Fk)>,
) -> super::head_drain::HeadDrainHandle<'a> {
    spindle_mark("head");
    let t_head = std::time::Instant::now();
    let drain = super::head_drain::submit_head_insert(query.store(), queued);
    let head_sub_ns = t_head.elapsed().as_nanos() as u64;
    if head_sub_ns > 0 {
        crate::note_confirm(&query.confirm_stats().write_head_sub_ns, head_sub_ns);
    }
    drain
}

/// Start the batch's slot read ahead of the Class A append.
///
/// `--datadir-hdd` starts nothing: a bulk read must not overlap the append
/// on a spindle, so structural reads every slot after the append instead.
fn start_spent_ahead(query: &Query, abs: Vec<u64>) -> super::spent_ahead::SpentAhead {
    if abs.is_empty() || query.store().datadir_hdd() {
        return super::spent_ahead::SpentAhead::idle();
    }
    spindle_mark("spent-ahead");
    #[cfg(debug_assertions)]
    if FAIL_SPENT_AHEAD.with(|fail| fail.replace(false)) {
        return super::spent_ahead::submit_spent_ahead(|| {
            Err(StoreError::Corrupt(
                "spent slot read ahead: injected failure",
            ))
        });
    }
    super::spent_ahead::read_spent_ahead(Arc::clone(&query.store), abs)
}

/// Wait for the read ahead. The wait is `slots_join=` on the write thread.
fn join_spent_ahead(
    query: &Query,
    ahead: super::spent_ahead::SpentAhead,
) -> Result<crate::block_pipeline::SlotRead, ConfirmError> {
    if !ahead.started() {
        return Ok(crate::block_pipeline::SlotRead::default());
    }
    spindle_mark("spent-join");
    let t_join = Instant::now();
    let read = ahead.join().map_err(ConfirmError::from);
    let join_ns = t_join.elapsed().as_nanos() as u64;
    if join_ns > 0 {
        crate::note_confirm(&query.confirm_stats().write_slots_join_ns, join_ns);
    }
    read
}

pub fn write_height_needed(tip: Option<u32>, height: u32) -> bool {
    match tip {
        None => true,
        Some(t) => height > t,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteBatchVsTip {
    AllOld,
    AllNew,
    SpansTip,
}

pub fn write_batch_vs_tip(
    tip: Option<u32>,
    heights: impl IntoIterator<Item = u32>,
) -> WriteBatchVsTip {
    let mut any_old = false;
    let mut any_new = false;
    for h in heights {
        if write_height_needed(tip, h) {
            any_new = true;
        } else {
            any_old = true;
        }
        if any_old && any_new {
            return WriteBatchVsTip::SpansTip;
        }
    }
    if any_new {
        WriteBatchVsTip::AllNew
    } else {
        WriteBatchVsTip::AllOld
    }
}

#[doc(hidden)]
pub fn finish_already_committed_write(
    query: &Query,
    batch: &ScriptOkBatch,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConfirmError> {
    let items: Vec<(u32, [u8; 32])> = batch
        .prepared
        .iter()
        .map(|p| (p.height.0, p.hash))
        .collect();
    finish_post_commit_hashes(query, &items)?;
    if let Some(h) = items.iter().map(|(h, _)| *h).max() {
        query.prune_write_create_loc(h);
    }
    Ok(Vec::new())
}

#[doc(hidden)]
pub struct ArchivePlanNs {
    pub pins: FkMap<crate::CreatePin>,
    pub class_a_wave: crate::block_pipeline::ClassAWave,
    pub header_ranges: Vec<(rbitcoin_primitives::Fk, rbitcoin_primitives::Fk, u32)>,
    pub class_a_ns: u64,
    pub ensure_ns: u64,
    pub plan_take_ns: u64,
    pub create_map_ns: u64,
}

#[doc(hidden)]
pub fn apply_archive_plan(
    query: &Query,
    batch: &mut ScriptOkBatch,
    need: &U64Map<Vec<u32>>,
) -> Result<ArchivePlanNs, ConfirmError> {
    let mut ns = ArchivePlanNs {
        pins: FkMap::default(),
        class_a_wave: crate::block_pipeline::ClassAWave::default(),
        header_ranges: Vec::new(),
        class_a_ns: 0,
        ensure_ns: 0,
        plan_take_ns: 0,
        create_map_ns: 0,
    };
    let Some(mut plan) = batch.archive_plan.take() else {
        return Ok(ns);
    };
    if plan.is_empty() {
        return Ok(ns);
    }
    let t_take = Instant::now();
    let planned_fks = plan.planned_fks.clone();
    if plan.batch_pin.len() != plan.planned_fks.len() {
        return Err(ConfirmError::Store(StoreError::Corrupt(
            "invariant: batch_pin length",
        )));
    }
    let packed_pins: Vec<crate::CreatePin> = std::mem::take(&mut plan.batch_pin);
    ns.plan_take_ns = t_take.elapsed().as_nanos() as u64;
    let t_ca = Instant::now();
    let (committed, loc, header_ranges) = {
        let _gate = hold_spindle(query);
        spindle_mark("class-a");
        query
            .archive_commit_plan_defer_head_parents(plan)
            .map_err(ConfirmError::from)?
    };
    ns.header_ranges = header_ranges;
    ns.class_a_ns = t_ca.elapsed().as_nanos() as u64;
    if !committed {
        return Ok(ns);
    }
    let pack_hi = batch.prepared.last().map(|p| p.height.0).unwrap_or(0);
    query.note_write_create_loc(&planned_fks, &loc, pack_hi);
    if query.index_mode()?.is_tip() {
        let t_map = Instant::now();
        ns.pins.reserve(planned_fks.len());
        for (fk, pin) in planned_fks.iter().zip(packed_pins.iter()) {
            ns.pins.insert(*fk, std::sync::Arc::clone(pin));
        }
        ns.create_map_ns = t_map.elapsed().as_nanos() as u64;
    }
    let t_ens = Instant::now();
    if let Err(e) = fill_planned_create_layout_after_commit(
        query,
        &mut batch.batch_parents,
        &planned_fks,
        &loc,
        &packed_pins,
        need,
    ) {
        // Class A is already appended. `header_txs` is what a retry uses to
        // skip that append. Store it even though this phase failed.
        publish_archived_header_ranges(query, &ns.header_ranges)?;
        return Err(e);
    }
    ns.ensure_ns = t_ens.elapsed().as_nanos() as u64;
    if let Some(last) = batch.prepared.last() {
        query.set_class_a_hi(Some(last.height.0));
    }
    // The overlay is proven only for rows this commit wrote. A plan trimmed
    // at commit wrote fewer rows than `planned_fks` names.
    if loc.len() == planned_fks.len() {
        ns.class_a_wave = crate::block_pipeline::ClassAWave::new(planned_fks);
    }
    Ok(ns)
}

/// COMMIT STAGE: optional Class A plan commit → structural → class_c → spend annotate → tip GC
/// → live filter and tweak append when `index_live` assembled this batch.
///
/// When `batch.archive_plan` is set (wire lookup/load path), Class A is appended in this
/// same stage before structural/annotate — single ordered commit era.
/// Spent slots of parents archived by earlier writes are read on
/// `ibd-confirm-spent` while Class A appends, and joined before structural.
/// **Class A never leads tip** (no dual-track archive-ahead / body DONTNEED lead).
///
/// Accrues window timers on [`Query::confirm_stats`] and snapshots the last batch
/// for slow-write logs via [`crate::ConfirmStats::last_write_phases`].
pub fn confirm_write_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    batch: ScriptOkBatch,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConfirmError> {
    confirm_write_phase_or_return(query, params, milestone, batch)
        .map(|(fks, _)| fks)
        .map_err(|(e, _)| e)
}

/// [`confirm_write_phase`] that hands the batch back either way. On error a
/// caller that retries the wave can offer its bodies back. On success the
/// caller picks the thread that frees what the write left in the batch.
#[allow(clippy::result_large_err)] // the batch is the point of the error
pub fn confirm_write_phase_or_return(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    mut batch: ScriptOkBatch,
) -> Result<(Vec<rbitcoin_primitives::Fk>, ScriptOkBatch), (ConfirmError, ScriptOkBatch)> {
    match write_phase(query, params, milestone, &mut batch) {
        Ok(fks) => Ok((fks, batch)),
        Err(e) => Err((e, batch)),
    }
}

#[doc(hidden)]
pub fn write_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    batch: &mut ScriptOkBatch,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConfirmError> {
    let ids: Vec<(u32, [u8; 32])> = batch
        .prepared
        .iter()
        .map(|p| (p.height.0, p.hash))
        .collect();
    let scope = super::TrustScope::enter(query);
    let result = write_phase_inner(query, params, milestone, batch);
    // Script warns stay quiet until this batch connects. A failed write logs
    // only its own hard reason.
    if result.is_ok() && !batch.trust_script_warns.is_empty() {
        for (h, err) in std::mem::take(&mut batch.trust_script_warns) {
            let _ = rbitcoin_consensus::soften_at(Some(h), err);
        }
    }
    scope.finish(&ids, result.as_ref().err(), true);
    result
}

fn write_phase_inner(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    batch: &mut ScriptOkBatch,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConfirmError> {
    let tip = query.tip_height().map(|h| h.0);
    match write_batch_vs_tip(tip, batch.prepared.iter().map(|p| p.height.0)) {
        WriteBatchVsTip::AllOld => return finish_already_committed_write(query, batch),
        WriteBatchVsTip::SpansTip => {
            return Err(ConfirmError::Store(rbitcoin_store::StoreError::Corrupt(
                "invariant: write batch spans tip",
            )));
        }
        WriteBatchVsTip::AllNew => {}
    }
    annotate_pending_spends(query)?;

    let t_wall = Instant::now();

    // The previous batch's spend annotate has returned, or was replayed just
    // above. Nothing writes an already-archived slot again until this batch's
    // own annotate, so those slots are read while Class A appends.
    let t_ahead = Instant::now();
    let first_planned = batch
        .archive_plan
        .as_ref()
        .and_then(|plan| plan.planned_fks.iter().filter_map(|fk| fk.get()).min());
    let super::SpendAbsAhead { abs, need } =
        super::spend_abs_before_append(query, &batch.batch_parents, &batch.prepared, first_planned);
    let ahead_walk_ns = t_ahead.elapsed().as_nanos() as u64;
    let ahead = start_spent_ahead(query, abs);

    let archived = apply_archive_plan(query, batch, &need);
    // Joined on every path: a failed append leaves no read in flight.
    let ahead = join_spent_ahead(query, ahead);
    let ArchivePlanNs {
        pins: write_create_pins,
        class_a_wave,
        header_ranges,
        class_a_ns,
        mut ensure_ns,
        plan_take_ns,
        create_map_ns,
    } = archived?;
    let t_ens = Instant::now();
    let slots = super::collect_spend_abs_after_fill(&batch.batch_parents, &batch.prepared)
        .and_then(|jobs| ahead.map(|read| (jobs, read)));
    let (abs_jobs, ahead) = match slots {
        Ok(v) => v,
        Err(e) => {
            publish_archived_header_ranges(query, &header_ranges)?;
            return Err(e);
        }
    };
    ensure_ns = ensure_ns
        .saturating_add(ahead_walk_ns)
        .saturating_add(t_ens.elapsed().as_nanos() as u64);
    if class_a_ns > 0 {
        crate::note_confirm(&query.confirm_stats().class_a_ns, class_a_ns);
    }
    if ensure_ns > 0 {
        crate::note_confirm(&query.confirm_stats().ensure_layout_ns, ensure_ns);
    }
    if plan_take_ns > 0 {
        crate::note_confirm(&query.confirm_stats().write_plan_take_ns, plan_take_ns);
    }
    if create_map_ns > 0 {
        crate::note_confirm(&query.confirm_stats().write_create_map_ns, create_map_ns);
    }

    // Flag off: tx.head insert overlaps structural, Class C, and annotate.
    // Flag on: the keys stay in this Vec until annotate returns, then insert.
    let queued = query.store().txs.take_pending_queued();
    let drain_max_fk = queued.iter().filter_map(|(_, fk)| fk.get()).max();
    let mut queued = Some(queued);
    let early_drain = if query.store().datadir_hdd() {
        None
    } else {
        Some(submit_queued(query, queued.take().unwrap()))
    };

    let overlap = (|| -> Result<_, ConfirmError> {
        // Local Instant totals (not atomic deltas) — sample_and_reset races mid-batch.
        let t_struct = Instant::now();
        let (struct_ph, slots) = WRITE_REUSE.with(|reuse| {
            structural_run(
                query,
                params,
                milestone,
                &batch.prepared,
                &batch.wire_blocks,
                &batch.batch_parents,
                &abs_jobs,
                &class_a_wave,
                &ahead,
                &mut reuse.borrow_mut(),
            )
        })?;
        let structural_ns = t_struct.elapsed().as_nanos() as u64;

        let n_blocks = batch.prepared.len();
        let pending_token = batch
            .prepared
            .first()
            .map(|first| query.store().note_spend_annotate_pending(first.height.0));
        let cc0 = query.confirm_stats().class_c_ns.load(Ordering::Relaxed);
        let wait0 = query
            .confirm_stats()
            .write_tip_flush_wait_ns
            .load(Ordering::Relaxed);
        let t_cc = Instant::now();
        let out = class_c_commit(
            query,
            &mut batch.prepared,
            &write_create_pins,
            &header_ranges,
        )?;
        super::phases::remember_connected_bip34(query, params, &batch.prepared);
        let class_c_wall_ns = t_cc.elapsed().as_nanos() as u64;
        let class_c_ns = query
            .confirm_stats()
            .class_c_ns
            .load(Ordering::Relaxed)
            .saturating_sub(cc0);
        let tip_flush_wait_ns = query
            .confirm_stats()
            .write_tip_flush_wait_ns
            .load(Ordering::Relaxed)
            .saturating_sub(wait0);
        let class_c_join_ns = class_c_wall_ns
            .saturating_sub(class_c_ns)
            .saturating_sub(tip_flush_wait_ns);
        if class_c_join_ns > 0 {
            crate::note_confirm(
                &query.confirm_stats().write_class_c_join_ns,
                class_c_join_ns,
            );
        }

        let seal = std::mem::take(&mut batch.index_seal);
        let idx_put_ns = super::index::seal_live_indexes(query, seal)?;
        if idx_put_ns > 0 {
            crate::note_confirm(&query.confirm_stats().idx_put_ns, idx_put_ns);
            crate::note_confirm(&query.confirm_stats().blockfilter_ns, idx_put_ns);
        }

        let spend_ann_ns = {
            let _gate = hold_spindle(query);
            spindle_mark("ann-begin");
            let ns = post_commit(query, &slots)?;
            spindle_mark("ann-end");
            ns
        };
        if let Some(token) = pending_token {
            query.store().clear_spend_annotate_pending(token);
        }
        if let Some(tip) = query.tip_height() {
            query.store().note_spend_snapshot(tip.0);
        }
        Ok((
            out,
            n_blocks,
            structural_ns,
            struct_ph,
            class_c_ns,
            spend_ann_ns,
        ))
    })();

    let drain = if let Some(drain) = early_drain {
        drain
    } else {
        submit_queued(query, queued.take().unwrap_or_default())
    };
    spindle_mark("join");
    let t_join = Instant::now();
    let (drain_res, restore) = drain.join_restore();
    let drain_join_ns = t_join.elapsed().as_nanos() as u64;
    if drain_join_ns > 0 {
        crate::note_confirm(&query.confirm_stats().write_drain_join_ns, drain_join_ns);
    }
    if drain_res.is_err() {
        query.store().txs.head_note_pending(&restore);
    }
    let (out, n_blocks, structural_ns, struct_ph, class_c_ns, spend_ann_ns) = match overlap {
        Ok(v) => v,
        Err(e) => {
            // Success stores these inside `class_c_commit` after the same join.
            // A reject before that still has to leave `header_txs` behind, or
            // the next batch mixes a connected block with this partial append.
            publish_archived_header_ranges(query, &header_ranges)?;
            return Err(e);
        }
    };
    drain_res.map_err(ConfirmError::from)?;
    if let Some(fk) = drain_max_fk {
        query.note_head_drain_fk(fk);
    }

    // No tip GC of sparse pins (dropped with ScriptOkBatch).
    if let Some(h) = batch.prepared.iter().map(|p| p.height.0).max() {
        query.prune_write_create_loc(h);
    }
    crate::note_confirm(&query.confirm_stats().phase_blocks, n_blocks as u64);
    query
        .confirm_stats()
        .note_last_write(crate::LastWritePhases {
            n_blocks: n_blocks as u32,
            wall_ns: t_wall.elapsed().as_nanos() as u64,
            class_a_ns,
            ensure_ns,
            structural_ns,
            spent_ns: struct_ph.spent_ns,
            create_h_ns: struct_ph.create_h_ns,
            bip68_ns: struct_ph.bip68_ns,
            class_c_ns,
            spend_ann_ns,
        });
    Ok(out)
}

/// Store header ranges after the in-flight tip flush joins.
///
/// The success path does this inside `class_c_commit`. This is the same put
/// for a write that appended Class A and then rejected, so `has_body` matches
/// that append. It does not flush: the next successful commit still flushes
/// Class C once.
fn publish_archived_header_ranges(
    query: &Query,
    ranges: &[(rbitcoin_primitives::Fk, rbitcoin_primitives::Fk, u32)],
) -> Result<(), ConfirmError> {
    if ranges.is_empty() {
        return Ok(());
    }
    let _commit = query.lock_tip_commit()?;
    let write = rbitcoin_store::ClassCWrite::new();
    query
        .store
        .header_txs
        .put_ranges_batch(&write, ranges)
        .map_err(ConfirmError::from)
}

/// Replay spend annotate + `tx.head` drain after Class C already committed.
///
/// Session-fault retry must not skip this: `height_of_hash` matching is not
/// "write finished."
pub fn finish_post_commit(query: &Query, height: u32, hash: &[u8; 32]) -> Result<(), ConfirmError> {
    finish_post_commit_hashes(query, &[(height, *hash)])
}

/// IBD write-thread in-place retry: `(height, hash)` must already be connected
/// at that height.
pub fn finish_post_commit_hashes(
    query: &Query,
    items: &[(u32, [u8; 32])],
) -> Result<(), ConfirmError> {
    let queued = query.store().txs.take_pending_queued();
    if queued.is_empty() && !query.spend_index_enabled() {
        return Ok(());
    }
    let drain_max_fk = queued.iter().filter_map(|(_, fk)| fk.get()).max();
    let mut queued = Some(queued);
    let early_drain = if query.store().datadir_hdd() || queued.as_ref().unwrap().is_empty() {
        None
    } else {
        Some(submit_queued(query, queued.take().unwrap()))
    };

    let annotate_res = {
        let _gate = hold_spindle(query);
        spindle_mark("ann-begin");
        let res = (|| -> Result<(), ConfirmError> {
            if !query.spend_index_enabled() {
                return Ok(());
            }
            let mut slots = crate::block_pipeline::AnnotateSlots::default();
            for &(height, hash) in items {
                annotate_slots_from_connected_hash(query, height, &hash, &mut slots)?;
            }
            post_commit(query, &slots)?;
            Ok(())
        })();
        spindle_mark("ann-end");
        res
    };

    let drain = if let Some(drain) = early_drain {
        Some(drain)
    } else {
        let batch = queued.take().unwrap_or_default();
        if batch.is_empty() {
            None
        } else {
            Some(submit_queued(query, batch))
        }
    };

    if let Some(drain) = drain {
        spindle_mark("join");
        let (drain_res, restore) = drain.join_restore();
        if drain_res.is_err() {
            query.store().txs.head_note_pending(&restore);
        }
        annotate_res?;
        drain_res.map_err(ConfirmError::from)?;
        if let Some(fk) = drain_max_fk {
            query.note_head_drain_fk(fk);
        }
    } else {
        annotate_res?;
    }
    Ok(())
}

#[doc(hidden)]
pub const REPLAY_BATCH: usize = 8;
#[doc(hidden)]
pub const REPLAY_STATUS_MS: u64 = 10_000;

/// Rewrite spend annotations above the durable marker, then `sync_data` and advance it.
///
/// Idempotent. A missing marker has no device-flushed cursor, so every height
/// above genesis is rewritten before the marker is published at the surviving tip.
/// Returns how many heights were rewritten. Call this on process open after
/// tip-window revalidation.
pub fn replay_spend_annotations(query: &Query) -> Result<u32, ConfirmError> {
    let Some(tip) = query.tip_height().map(|h| h.0) else {
        return Ok(0);
    };
    // A marker above the tip is not a cursor for this chain. Shrink and a
    // crash between the tip flush and the clamp both leave that file behind.
    // Lower it before the `a == tip` skip, or the next confirm's spends are
    // never rewritten.
    let annotated = query
        .store()
        .spend_annotated_through()
        .map_err(ConfirmError::from)?;
    if annotated.is_some_and(|h| h > tip) {
        query
            .store()
            .clamp_spend_durable()
            .map_err(ConfirmError::from)?;
    }
    let annotated = query
        .store()
        .spend_annotated_through()
        .map_err(ConfirmError::from)?;
    let a = match annotated {
        Some(h) => h.min(tip),
        None => {
            rbitcoin_log::info!("store: no spend durable marker; replaying (0, {tip}]");
            0
        }
    };
    if a == tip {
        return Ok(0);
    }
    rewrite_spend_heights(query, a, tip)
}

#[doc(hidden)]
pub fn publish_spend_marker(query: &Query, tip: u32) -> Result<(), ConfirmError> {
    let sync_ns = query
        .store()
        .sync_spend_durable(tip)
        .map_err(ConfirmError::from)?;
    crate::note_confirm(&query.confirm_stats().spend_durable_ns, sync_ns);
    Ok(())
}

#[doc(hidden)]
pub fn rewrite_spend_heights(query: &Query, annotated: u32, tip: u32) -> Result<u32, ConfirmError> {
    rbitcoin_log::info!("store: replay spend annotations ({annotated}, {tip}]");
    let replayed = annotate_spend_heights(query, annotated + 1, tip)?;
    publish_spend_marker(query, tip)?;
    Ok(replayed)
}

/// Finish the spend annotate a failed confirm write left after its tip commit.
///
/// The next batch's spentness reads those slots, so it must not run until they
/// are written. A replay that fails returns the error and the batch does not run.
#[doc(hidden)]
pub fn annotate_pending_spends(query: &Query) -> Result<(), ConfirmError> {
    let token = query.store().spend_annotate_token();
    let Some(from) = query.store().spend_annotate_pending() else {
        return Ok(());
    };
    if let Some(tip) = query.tip_height().map(|h| h.0).filter(|&t| t >= from) {
        rbitcoin_log::warn!(
            "confirm: replay spend annotations [{from}, {tip}] after a failed write"
        );
        let t = Instant::now();
        annotate_spend_heights(query, from, tip)?;
        crate::note_confirm(
            &query.confirm_stats().spend_replay_ns,
            t.elapsed().as_nanos() as u64,
        );
    }
    query.store().clear_spend_annotate_pending(token);
    Ok(())
}

#[doc(hidden)]
pub fn annotate_spend_heights(query: &Query, start: u32, tip: u32) -> Result<u32, ConfirmError> {
    let heights: Vec<u32> = (start..=tip).collect();
    let replayed = heights.len() as u32;
    let started = std::time::Instant::now();
    let mut logged_at = started;
    let mut done = 0u32;
    for chunk in heights.chunks(REPLAY_BATCH) {
        let mut items = Vec::with_capacity(chunk.len());
        for &h in chunk {
            let Some((_, rec)) = query
                .header_at_height(rbitcoin_primitives::Height(h))
                .map_err(ConfirmError::from)?
            else {
                return Err(ConfirmError::Store(StoreError::Corrupt(
                    "invariant: spend replay missing header",
                )));
            };
            items.push((h, rec.hash));
        }
        finish_post_commit_hashes(query, &items)?;
        done = done.saturating_add(chunk.len() as u32);
        let elapsed = logged_at.elapsed().as_millis() as u64;
        if elapsed >= REPLAY_STATUS_MS {
            let h = *chunk.last().unwrap_or(&tip);
            rbitcoin_log::info!("store: replay spend annotations {done}/{replayed} height={h}");
            logged_at = std::time::Instant::now();
        }
    }
    Ok(replayed)
}

#[doc(hidden)]
pub fn annotate_slots_from_connected_hash(
    query: &Query,
    height: u32,
    hash: &[u8; 32],
    slots: &mut crate::block_pipeline::AnnotateSlots,
) -> Result<(), ConfirmError> {
    match query.height_of_hash(hash).map_err(ConfirmError::from)? {
        Some(h) if h.0 == height => {}
        _ => return Ok(()),
    }
    let Some((hfk, _)) = query.get_header_by_hash(hash).map_err(ConfirmError::from)? else {
        return Ok(());
    };
    let Some(tx_fks) = query
        .store()
        .header_txs
        .get_list(hfk)
        .map_err(ConfirmError::from)?
    else {
        return Ok(());
    };
    for (tx_i, &spend_fk) in tx_fks.iter().enumerate() {
        let (_meta, ins, _outs) = query
            .store()
            .get_tx_full(spend_fk)
            .map_err(ConfirmError::from)?;
        if tx_i == 0 {
            let coinbase = ins.len() == 1 && ins[0].is_coinbase();
            if !coinbase {
                return Err(ConfirmError::Store(StoreError::Corrupt(
                    "invariant: coinbase input edge",
                )));
            }
            continue;
        }
        for (inp_i, inp) in ins.into_iter().enumerate() {
            let rbitcoin_store::InputParent::Prevout { create_fk, vout } = inp.parent else {
                return Err(ConfirmError::Store(StoreError::Corrupt(
                    "invariant: non-coinbase input edge",
                )));
            };
            let (off, len) = query
                .store()
                .tx_spent_range(create_fk)
                .map_err(ConfirmError::from)?;
            let abs = rbitcoin_store::spent_abs(off, vout);
            if abs.saturating_add(rbitcoin_store::OutputRecord::SPENT_SLOT_LEN as u64)
                > off.saturating_add(len)
            {
                return Err(ConfirmError::Store(StoreError::Corrupt(
                    "invariant: finish_post_commit spent slot OOB",
                )));
            }
            let (multi, field, field_vin) = query
                .store()
                .txs
                .get_output_spender_meta(create_fk, vout)
                .map_err(ConfirmError::from)?;
            let flags = if multi {
                rbitcoin_store::output_flags::MULTI_SPENDER
            } else {
                0
            };
            slots.push(
                (abs, create_fk, vout, spend_fk, inp_i as u32),
                (field, flags, field_vin),
            );
        }
    }
    Ok(())
}

/// After Class A commit, stamp spend creates from append RAM loc
/// (this pack + just-written packs still in the write loc window).
/// Write never preads `create.loc`.
///
/// `need` is the creates with no slot offset yet, from the walk before the
/// append ([`super::spend_abs_before_append`]). The append does not change it.
pub fn fill_planned_create_layout_after_commit(
    query: &Query,
    batch_parents: &mut crate::BatchParents,
    planned_fks: &[rbitcoin_primitives::Fk],
    loc: &[rbitcoin_store::CreateLocPair],
    packed: &[crate::CreatePin],
    need: &U64Map<Vec<u32>>,
) -> Result<(), ConfirmError> {
    if need.is_empty() {
        return Ok(());
    }
    if !planned_fks.is_empty() {
        if loc.len() != planned_fks.len() || packed.len() != planned_fks.len() {
            return Err(ConfirmError::Store(StoreError::Corrupt(
                "invariant: append loc length",
            )));
        }
        for (fk, (pair, pin)) in planned_fks.iter().zip(loc.iter().zip(packed.iter())) {
            let Some(id) = fk.get() else { continue };
            let Some(vouts) = need.get(&id) else { continue };
            if pair.n_out != pin.n_out() as u32 {
                return Err(ConfirmError::Store(StoreError::Corrupt(
                    "invariant: append loc length",
                )));
            }
            if batch_parents.contains(*fk) {
                batch_parents.set_body_range_only(*fk, pair.txout);
                batch_parents.set_spent_range_only(*fk, pair.spent);
                continue;
            }
            let mut checked = vouts.clone();
            checked.sort_unstable();
            checked.dedup();
            let cb = if pin.tx().input_count > 1 {
                Some(false)
            } else {
                None
            };
            batch_parents.insert_create_pin(
                *fk,
                std::sync::Arc::clone(pin),
                checked,
                cb,
                Some(pair.txout),
                Vec::new(),
            );
            batch_parents.set_spent_range_only(*fk, pair.spent);
        }
    }
    for id in need.keys() {
        let fk = rbitcoin_primitives::Fk(*id);
        if !batch_parents.contains(fk) || batch_parents.has_abs_layout(fk) {
            continue;
        }
        let Some(pair) = query.write_create_loc(fk) else {
            continue;
        };
        batch_parents.set_body_range_only(fk, pair.txout);
        batch_parents.set_spent_range_only(fk, pair.spent);
    }
    Ok(())
}
