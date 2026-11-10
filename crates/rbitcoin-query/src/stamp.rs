//! External parent create_fk stamp: in-flight → skeleton → leftover TipOnly.
//!
//! IBD load passes a lookup-filled [`BatchParentIds`] and never leftover-probes.
//! plan=None / S0 (`skeleton = None`) is in-flight → leftover TipOnly.
//! One function for S0 plan (`archive_plan_batch_from_wire`) and plan=None
//! rehydrate. In-flight holds CreatePins until load drops map rows below a
//! lookup-wave drain+fence snapshot taken before TipOnly. Creates above that
//! snapshot are omitted from that skeleton. Class A `CreatePin::set_loc` after append;
//! later-wave stamp reads pin loc. IBD (`skeleton = Some`) never loc-by-fk
//! here: spent is pin loc, skeleton TipOnly loc, or unset for write fill. The
//! all-archived retry (`stamp_parent_pin_archived`) loc-fills its archived
//! parents that have no body. External IBD parents still must not.

use crate::{CreatePin, InFlight, QueryError, U64Map};
use crate::{IdMap, TxidHasher};
use rbitcoin_primitives::Fk;
use rbitcoin_store::Store;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::time::Instant;

#[doc(hidden)]
pub type TxidFkMap = HashMap<[u8; 32], Fk, BuildHasherDefault<TxidHasher>>;

/// Lookup-filled parent identity for one load chunk (fk + ranges; no outs).
#[derive(Clone, Debug, Default)]
pub struct BatchParentIds {
    /// Wave `txid → (create_fk, body_range)` (shared across chunks).
    pub ids: Arc<IdMap>,
    /// Wave `create_fk_id → spent.body` range (shared across chunks).
    pub spent: Arc<U64Map<(u64, u64)>>,
    /// Wave `create_fk_id → loc n_out`.
    pub n_out: Arc<U64Map<u32>>,
    /// Per-chunk `create_fk_id → vouts` spent in this load batch.
    pub need_vouts: U64Map<Vec<u32>>,
}

impl BatchParentIds {
    #[allow(clippy::type_complexity)] // packed (fk, range) / span row is the on-disk shape
    pub fn get(
        &self,
        txid: &[u8; 32],
    ) -> Option<(Fk, (u64, u64), Option<(u64, u64)>, Option<u32>)> {
        let &(fk, body) = self.ids.get(txid)?;
        let spent = fk.get().and_then(|id| self.spent.get(&id).copied());
        let n_out = fk.get().and_then(|id| self.n_out.get(&id).copied());
        Some((fk, body, spent, n_out))
    }
}

/// One create's lookup-stamped identity (body / spent / pin optional).
#[derive(Debug, Clone, Default)]
pub struct ParentIdent {
    pub txid: [u8; 32],
    pub body: Option<(u64, u64)>,
    pub spent: Option<(u64, u64)>,
    pub n_out: Option<u32>,
    pub pin: Option<CreatePin>,
}

impl ParentIdent {
    #[inline]
    pub fn new(txid: [u8; 32]) -> Self {
        Self {
            txid,
            body: None,
            spent: None,
            n_out: None,
            pin: None,
        }
    }

    #[inline]
    pub fn with_body(txid: [u8; 32], body: (u64, u64)) -> Self {
        Self {
            txid,
            body: Some(body),
            spent: None,
            n_out: None,
            pin: None,
        }
    }

    #[inline]
    pub fn with_loc(txid: [u8; 32], body: (u64, u64), spent: (u64, u64), n_out: u32) -> Self {
        Self {
            txid,
            body: Some(body),
            spent: Some(spent),
            n_out: Some(n_out),
            pin: None,
        }
    }
}

/// Lookup-stamped external parent identity (same-batch stays offline at pin).
///
/// `txid → create_fk` plus `create_fk → ParentIdent`. No parallel range/txid/pin maps.
#[derive(Debug, Default, Clone)]
pub struct ExternalParentStamp {
    /// prev_txid → create_fk
    pub resolved: TxidFkMap,
    /// create_fk_id → identity
    pub idents: U64Map<ParentIdent>,
    pub inflight_ns: u64,
    pub pin_txid_n: u64,
    pub pin_txid_ns: u64,
    pub recent_n: u64,
    pub recent_ns: u64,
    pub head_fk_ns: u64,
    pub head_need_n: u64,
    pub head_hit_n: u64,
}

impl ExternalParentStamp {
    #[doc(hidden)]
    pub fn bind(&mut self, id: u64, txid: [u8; 32]) -> &mut ParentIdent {
        self.idents
            .entry(id)
            .or_insert_with(|| ParentIdent::new(txid))
    }
}

#[doc(hidden)]
pub fn stamp_inflight_hits<'a>(
    stamp: &mut ExternalParentStamp,
    need: &'a [[u8; 32]],
    in_flight: &InFlight,
    skeleton: Option<&BatchParentIds>,
    still_need: &mut Vec<&'a [u8; 32]>,
) {
    for t in need {
        if *t == [0u8; 32] {
            continue;
        }
        if let Some(fk) = in_flight.get_create_fk(t) {
            stamp.resolved.insert(*t, fk);
            if let Some(id) = fk.get() {
                let e = stamp.bind(id, *t);
                if let Some(pin) = in_flight.get_out(id) {
                    e.pin = Some(std::sync::Arc::clone(pin));
                    if let Some(pair) = pin.loc() {
                        e.body = Some(pair.txout);
                        e.spent = Some(pair.spent);
                        e.n_out = Some(pair.n_out);
                    }
                }
                if let Some(skel) = skeleton {
                    if let Some((sk_fk, range, spent, n_out)) = skel.get(t) {
                        if sk_fk == fk {
                            e.body = Some(range);
                            if let Some(sr) = spent {
                                e.spent = Some(sr);
                            }
                            e.n_out = n_out;
                        }
                    }
                }
            }
        } else {
            still_need.push(t);
        }
    }
}

// Capacity is not an observable result: empty maps insert the same keys.
#[cfg_attr(feature = "mutants", mutants::skip)]
#[doc(hidden)]
pub fn external_parent_stamp(need_len: usize) -> ExternalParentStamp {
    ExternalParentStamp {
        resolved: TxidFkMap::with_capacity_and_hasher(need_len / 2, Default::default()),
        idents: U64Map::with_capacity_and_hasher(need_len, Default::default()),
        ..ExternalParentStamp::default()
    }
}

/// Bind `need` txids: in-flight → skeleton → leftover TipOnly.
///
/// `skeleton = Some` is the IBD path: miss of in-flight and skeleton is
/// `Corrupt` with no leftover `tx.head` probe. In-flight identity still takes
/// skeleton loc when TipOnly already has that create (later wave). Creates
/// above the wave's snapshot are omitted from the skeleton; those holes stay
/// for write fill.
/// IBD never loc-by-fk: pin loc unset and skeleton miss leaves spent unset
/// (write TLS / late `set_loc`).
/// `skeleton = None` is
/// plan=None / S0 leftover TipOnly. Same-batch identities are not inputs —
/// callers skip them in `need` and keep them offline at pin.
pub fn stamp_external_parents(
    store: &Store,
    need: &[[u8; 32]],
    in_flight: &InFlight,
    skeleton: Option<&BatchParentIds>,
    stats: &crate::ConfirmStats,
) -> Result<ExternalParentStamp, QueryError> {
    let mut stamp = external_parent_stamp(need.len());

    let t_inflight = Instant::now();
    let mut still_need: Vec<&[u8; 32]> = Vec::new();
    stamp_inflight_hits(&mut stamp, need, in_flight, skeleton, &mut still_need);
    stamp.inflight_ns = t_inflight.elapsed().as_nanos() as u64;

    let t_pin_txid = Instant::now();
    let mut after_skel: Vec<&[u8; 32]> = Vec::new();
    if let Some(skel) = skeleton {
        for t in still_need {
            if let Some((fk, range, spent, n_out)) = skel.get(t) {
                stamp.resolved.insert(*t, fk);
                if let Some(id) = fk.get() {
                    let e = stamp.bind(id, *t);
                    e.body = Some(range);
                    if let Some(sr) = spent {
                        e.spent = Some(sr);
                    }
                    e.n_out = n_out;
                }
                stamp.pin_txid_n = stamp.pin_txid_n.saturating_add(1);
                continue;
            }
            after_skel.push(t);
        }
        stamp.pin_txid_ns = t_pin_txid.elapsed().as_nanos() as u64;
        if !after_skel.is_empty() {
            return Err(rbitcoin_store::StoreError::Corrupt(
                "archive: parent create_fk unresolved (contiguous batch required)",
            ));
        }
        stamp.head_need_n = 0;
        stats.note_pin_txid(stamp.pin_txid_n, stamp.pin_txid_ns);
        stats.note_recent(stamp.recent_n, stamp.recent_ns);
        return Ok(stamp);
    }
    let mut need_head: Vec<[u8; 32]> = still_need.into_iter().copied().collect();
    stamp.pin_txid_ns = t_pin_txid.elapsed().as_nanos() as u64;
    stamp.head_need_n = need_head.len() as u64;

    let t_head = Instant::now();
    if !need_head.is_empty() {
        need_head.sort_by_cached_key(|txid| store.txs.head_primary_slot(txid));
        let hits = store.get_fk_by_txid_batch(&need_head)?;
        let first_fks = store.txs.head_first_fks_snapshot();
        let mut age0 = 0u64;
        let mut age3 = 0u64;
        let mut age_n = 0u64;
        for (txid, row) in hits {
            if let Some((fk, pair)) = row {
                stamp.resolved.insert(txid, fk);
                stamp.head_hit_n = stamp.head_hit_n.saturating_add(1);
                if let Some(id) = fk.get() {
                    let e = stamp.bind(id, txid);
                    e.body = Some(pair.txout);
                    e.spent = Some(pair.spent);
                    e.n_out = Some(pair.n_out);
                    if let Some(age) =
                        rbitcoin_store::head_resolve_stats::sealed_age_for_fk(&first_fks, id)
                    {
                        age_n = age_n.saturating_add(1);
                        if age == 0 {
                            age0 = age0.saturating_add(1);
                        }
                        if age <= 3 {
                            age3 = age3.saturating_add(1);
                        }
                    }
                }
            }
        }
        stats.note_leftover_mix(0, age0, age3, age_n);
    }
    {
        let mut miss_n = 0u64;
        let mut first_miss = None;
        for t in &need_head {
            if stamp.resolved.contains_key(t) {
                continue;
            }
            miss_n = miss_n.saturating_add(1);
            if first_miss.is_none() {
                first_miss = Some(*t);
            }
        }
        if let Some(tid) = first_miss {
            let pending = store.txs.queued_pending_fk(&tid).is_some();
            let (miss_on, miss_cands) = rbitcoin_store::head_resolve_stats::take_leftover_miss()
                .map(|(on, n)| (Some(on.as_str()), n))
                .unwrap_or((None, 0));
            stats.note_union_miss(tid, miss_n, pending, miss_on, miss_cands);
            store.diagnose_leftover_probe(&tid);
        } else {
            stats.note_union_miss([0u8; 32], 0, false, None, 0);
        }
    }
    stamp.head_fk_ns = t_head.elapsed().as_nanos() as u64;
    stats.note_pin_txid(stamp.pin_txid_n, stamp.pin_txid_ns);
    stats.note_recent(stamp.recent_n, stamp.recent_ns);

    fill_missing_parent_ranges(store, in_flight, &mut stamp.idents, stats)?;
    Ok(stamp)
}

/// Loc body_range and spent_range for stamped create_fks with no in-flight outs.
///
/// Body miss after identity is `Corrupt`. Spent miss after a **store** body fill
/// is `Corrupt`. RAM-only identity (in-flight outs, no spent range row) leaves
/// spent unset — write ensure still stamps those holes.
pub fn fill_missing_parent_ranges(
    store: &Store,
    in_flight: &InFlight,
    idents: &mut U64Map<ParentIdent>,
    stats: &crate::ConfirmStats,
) -> Result<(), QueryError> {
    let mut need: Vec<Fk> = Vec::new();
    for (&id, ident) in idents.iter() {
        if in_flight.get_out(id).is_some() {
            continue;
        }
        if ident.body.is_none() || ident.spent.is_none() || ident.n_out.is_none() {
            need.push(Fk(id));
        }
    }
    if need.is_empty() {
        return Ok(());
    }
    stats.note_fill_missing();
    let filled = store.tx_create_loc_range_batch(&need)?;
    for (fk, row) in need.into_iter().zip(filled) {
        let Some(id) = fk.get() else {
            continue;
        };
        let Some(pair) = row else {
            return Err(rbitcoin_store::StoreError::Corrupt(
                "archive: external parent loc missing after create_fk stamp",
            ));
        };
        if let Some(e) = idents.get_mut(&id) {
            e.body = Some(pair.txout);
            e.spent = Some(pair.spent);
            e.n_out = Some(pair.n_out);
        }
    }
    Ok(())
}
