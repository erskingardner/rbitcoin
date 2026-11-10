//! Lookup-promoted body-queue wire (decoded `Block` + [`TxPrecompute`]).

use crate::TxPrecompute;
use bitcoin::Block;
use std::sync::Arc;

/// Decoded body held after lookup drops the raw frame.
#[derive(Clone, Debug)]
pub struct ResolvedWire {
    pub block: Arc<Block>,
    pub pres: Arc<[TxPrecompute]>,
    /// Σ `tx.input.len()` (enqueue CompactSize or decode). Load-split uses this.
    pub n_inputs: u32,
    /// Class A header row from BQ enqueue. Load-split kinds via `has_body(fk)`.
    pub header_fk: u64,
    /// External `(prev_txid, vout)` from the BQ input walk (no coinbase).
    /// Prevs lookup left out of TipOnly need stay here so load can in-flight
    /// bind them; TipOnly / `need_vouts` ∩ wave ids still drop them.
    pub spend_keys: Arc<[([u8; 32], u32)]>,
    /// Peer id given at BQ enqueue ([`crate::Query::block_queue_offer_from`]).
    pub sender: Option<u64>,
}

impl ResolvedWire {
    /// Stamp `n_inputs` from the decoded block (tests / callers without enqueue meta).
    pub fn new(block: Arc<Block>, pres: Arc<[TxPrecompute]>) -> Self {
        let n_inputs = block
            .txdata
            .iter()
            .map(|tx| tx.input.len() as u32)
            .fold(0u32, u32::saturating_add);
        Self {
            block,
            pres,
            n_inputs,
            header_fk: 0,
            spend_keys: Arc::from([]),
            sender: None,
        }
    }
}

/// Lookup snapshot of unresolved heights: still-raw vs already promoted.
///
/// `raw` is **(height, n_inputs, header_fk, sender)** — no payload clone. Lookup
/// packs/holds from the stamped count; decode clones via
/// [`crate::Query::block_queue_raw_payload`] only for heights it emits.
#[derive(Clone, Debug, Default)]
pub struct BlockQueueWaveIntake {
    pub raw: Vec<(u32, u32, u64, Option<u64>)>,
    pub resolved: Vec<(u32, ResolvedWire)>,
}
