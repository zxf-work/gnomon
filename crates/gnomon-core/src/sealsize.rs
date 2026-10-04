//! How many rows one sealed segment holds.
//!
//! **The problem this module exists for.** Both writers — `staging::seal` on
//! the build path and `merge::ChunkWriter` on the rewrite path — decide when
//! to seal by accumulating a *model* of each row's size and comparing it to
//! `defaults::SEGMENT_TARGET_BYTES`. The model counts the row as the engine
//! holds it in memory; the encoder then writes columns and a
//! dictionary-compressed string heap. On the synthetic corpus the model is
//! ~58 B/row and the encoder achieves ~30.5 B/row, so a segment "targeted at
//! 128 MiB" occupies ~68 MB on disk — a measured mean/target of 0.488 at 30M
//! and 0.518 at 100M, and 0.5347 for the one segment whose header and file
//! size were compared row for row.
//! Every store therefore carries about twice the segments its configured
//! layout implies, and every per-segment term downstream — the settle byte
//! floor, the aggregate's per-selection constant, the parallel fold's unit
//! count — is scaled by that factor.
//!
//! **The correction.** Rather than re-fit the model constant to one corpus,
//! the writer measures the encoder. After every segment it seals it observes
//! `(file bytes, modelled bytes)` and keeps the running quotient; the next
//! chunk's target is the configured target divided by that quotient, so the
//! *file* lands at the configured size whatever the corpus encodes to. The
//! quotient is seeded, before a writer has sealed anything, from the sealed
//! segments already live in the lane, so a rewrite is corrected from its
//! first output rather than its second.
//!
//! Three properties make this safe to put on the build path.
//!
//! * **Content neutrality.** The quotient moves segment *boundaries* and
//!   nothing else. Which rows exist, their values and their order are
//!   untouched, so the content digest is invariant; the `store_digest`, which
//!   is a digest of the physical layout, is not and is not expected to be.
//!
//! * **Determinism.** [`SealRatio`] is exact integer arithmetic over
//!   two `u128` accumulators — no floating point, so no dependence on
//!   summation order — and its inputs are the row counts and file lengths of
//!   segments already committed or already written by this same call. A
//!   replay of the same log rebuilds the same manifests and writes the same
//!   segment bytes, so it observes the same quotients and cuts at the same
//!   rows. Recovery replays the un-applied suffix against the
//!   committed manifest, which is exactly the state the seed is derived from.
//!
//! * **Bounded memory.** The writer buffers owned rows — five `String`s each,
//!   ≈228 B resident against ≈50 B accounted — so raising
//!   the effective target raises the resident buffer in proportion. Two
//!   things bound it: the quotient is clamped to
//!   [`defaults::SEAL_RATIO_MAX_NUM`]/[`defaults::SEAL_RATIO_MAX_DEN`], and
//!   the writers carry a second, *exact* accumulator of resident bytes and
//!   seal early if it reaches [`defaults::CHUNK_RESIDENT_MAX_BYTES`]. The
//!   second is a bound the engine did not previously have at all: before this
//!   change the only thing limiting a chunk's resident set was the `disc`
//!   term in the accounting model, which is not a budget.
//!
//! **Why the accounting model lost its `disc` term.** `staging::edge_row_bytes`
//! was `EDGE_FIXED_BYTES + disc.len() + 4`; it is now `EDGE_FIXED_BYTES + 4`,
//! which is the flat 50 B that `merge::full_rows` and `settle`'s byte floor
//! already assumed. The seed has to be reconstructible from the manifest, and
//! the manifest records row counts, not string lengths — so a model with a
//! per-row string term cannot be the model the seed is measured against, and
//! a seed measured against a *different* model undershoots by exactly the
//! string term (≈14 % on this corpus). Dropping it makes one model serve the
//! writer, the tier planner and the settle floor, and costs nothing in
//! accuracy that the measured quotient does not restore: the model's job is
//! no longer to be right, only to be *stable*, because the quotient supplies
//! the corpus-specific correction. The memory property the `disc` term
//! incidentally provided is now carried explicitly by the resident cap.

use crate::defaults;
use crate::merge::LaneSlot;
use crate::row::{EdgeRow, NodeRow};

/// The writer's per-row accounting unit, by lane slot.
///
/// One definition, used by `staging`'s chunker, `merge::ChunkWriter`,
/// `merge::full_rows` and `settle`'s byte floor. The first of
/// those added `disc.len()` and the other three did not; the module docstring
/// says why they now agree.
pub fn per_row_model(slot: LaneSlot) -> u64 {
    match slot {
        LaneSlot::Node => defaults::NODE_ROW_MODEL_BYTES,
        _ => defaults::EDGE_ROW_MODEL_BYTES,
    }
}

/// Resident bytes one buffered `EdgeRow` costs, counted rather than modelled.
///
/// The struct itself plus each owned string's contents plus an allocator
/// bucket per non-empty string. Deliberately an over-estimate: it is the
/// input to a cap, and a cap that under-counts is not a cap. It does not
/// include the row vector's own geometric growth — see
/// [`ChunkWriter`](crate::merge)'s flush rule and the memory
/// section, which state that term separately.
pub fn edge_resident_bytes(r: &EdgeRow) -> u64 {
    defaults::EDGE_ROW_RESIDENT_FIXED
        + heap_of(&r.rel_type)
        + heap_of(&r.disc)
        + heap_of(&r.props)
        + heap_of(&r.source)
        + r.provenance_ref.as_deref().map_or(0, heap_of)
}

/// Resident bytes one buffered `NodeRow` costs. See [`edge_resident_bytes`].
pub fn node_resident_bytes(r: &NodeRow) -> u64 {
    defaults::NODE_ROW_RESIDENT_FIXED
        + heap_of(&r.label)
        + heap_of(&r.props)
        + heap_of(&r.source)
        + r.provenance_ref.as_deref().map_or(0, heap_of)
}

/// One owned string's heap cost: its bytes plus an allocator bucket, or zero
/// when it never allocated.
fn heap_of(s: &str) -> u64 {
    if s.is_empty() {
        0
    } else {
        s.len() as u64 + defaults::STRING_ALLOC_OVERHEAD
    }
}

/// The measured quotient between what the encoder writes and what the writer
/// accounted for it — one lane slot's, over the segments observed so far.
///
/// Exact integer arithmetic on purpose. A floating running mean would make
/// the seal boundary depend on summation order, and summation order depends
/// on which segments a particular replay happened to seal first; two `u128`
/// sums do not. `u128` because the accumulators are sums of segment sizes
/// over a whole call and a `u64` product of the two would overflow at
/// petabyte scale, not because the individual terms are large.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SealRatio {
    /// Bytes the encoder actually wrote, summed over observed segments.
    encoded: u128,
    /// Bytes the writer accounted for those same segments.
    modelled: u128,
}

impl SealRatio {
    /// No measurement yet: [`Self::effective_target`] returns the configured
    /// target unchanged, so a store that has never sealed a segment on the
    /// byte rule behaves exactly as it did previously. Every unit-scale
    /// fixture is in that case, which is why none of their segment counts
    /// move.
    pub fn unmeasured() -> Self {
        Self::default()
    }

    /// Whether anything has been observed. A ratio with a zero numerator or
    /// denominator is treated as unmeasured rather than as an infinity.
    pub fn is_measured(&self) -> bool {
        self.encoded > 0 && self.modelled > 0
    }

    /// Fold in one sealed segment: the bytes its file occupies and the bytes
    /// the writer had accumulated for its rows at the moment it flushed.
    ///
    /// Zero-sized observations are ignored rather than allowed to poison the
    /// quotient — a segment cannot legitimately be empty (`write_edge_segment`
    /// refuses), so a zero here means a `stat` failed and the right response
    /// is to keep the previous estimate.
    pub fn observe(&mut self, encoded: u64, modelled: u64) {
        if encoded == 0 || modelled == 0 {
            return;
        }
        self.encoded += encoded as u128;
        self.modelled += modelled as u128;
    }

    /// The accounted-byte target whose *file* is expected to land at `base`.
    ///
    /// `base × modelled / encoded`, clamped to
    /// `[MIN_NUM/MIN_DEN, MAX_NUM/MAX_DEN] × base`. Unmeasured returns `base`.
    ///
    /// The clamp is two-sided and both sides are failure modes, not
    /// conservatism:
    ///
    /// * **Above.** A corpus that encodes to better than
    ///   `MAX_DEN/MAX_NUM` of the model seals under target rather than
    ///   chasing the quotient into an ever larger buffer. This is the sizing
    ///   guard; the *memory* bound is
    ///   [`defaults::CHUNK_RESIDENT_MAX_BYTES`], which is exact, does not
    ///   depend on the quotient being sane, and binds before this clamp's
    ///   own worst case does.
    /// * **Below.** A corpus that encodes *larger* than the model legitimately
    ///   wants a smaller accounted target, but an estimate driven to near zero
    ///   would seal one segment per row and explode the segment count — the
    ///   exact pathology `PIN_MAX_SEGMENTS` guards and this change exists to
    ///   reduce.
    pub fn effective_target(&self, base: u64) -> u64 {
        if !self.is_measured() {
            return base;
        }
        let base128 = base as u128;
        let scaled = base128.saturating_mul(self.modelled) / self.encoded;
        let lo = base128 * defaults::SEAL_RATIO_MIN_NUM as u128
            / defaults::SEAL_RATIO_MIN_DEN as u128;
        let hi = base128 * defaults::SEAL_RATIO_MAX_NUM as u128
            / defaults::SEAL_RATIO_MAX_DEN as u128;
        scaled.clamp(lo.max(1), hi.max(1)).min(u64::MAX as u128) as u64
    }
}

/// One [`SealRatio`] per lane slot, which is the granularity the quotient has
/// to be kept at: the node lane's rows are a different shape from an edge
/// lane's and encode to a different ratio, and the event and interval edge
/// lanes hold different `vt_e` columns (a format detail elides one of them).
/// A single pooled quotient would let the 0.2 MB node lane drag the event
/// lane's target around.
///
/// Carries the [`adaptive_enabled`] switch rather than consulting it at each
/// use: a writer must not change rule halfway through a call because the
/// environment changed under it, and one resolution per call is also the only
/// one a `should_flush` on the per-row path can afford.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealRatios {
    edge_event: SealRatio,
    edge_interval: SealRatio,
    node: SealRatio,
    /// `false` under `GNOMON_SEAL_ADAPTIVE=0`: observations still accumulate,
    /// and [`Self::effective_target`] ignores them. Gating the *use* rather
    /// than the *recording* is what makes the baseline exact — it covers
    /// the within-call feedback a writer applies between its own chunks, not
    /// just the seed read off the manifest.
    adaptive: bool,
}

impl SealRatios {
    /// Nothing observed in any slot. Deliberately not `Default`: whether the
    /// correction applies is resolved here, and a `Default` would resolve it
    /// to `false` by accident.
    pub fn unmeasured() -> Self {
        Self {
            edge_event: SealRatio::unmeasured(),
            edge_interval: SealRatio::unmeasured(),
            node: SealRatio::unmeasured(),
            adaptive: adaptive_enabled(),
        }
    }

    pub fn get(&self, slot: LaneSlot) -> &SealRatio {
        match slot {
            LaneSlot::EdgeEvent => &self.edge_event,
            LaneSlot::EdgeInterval => &self.edge_interval,
            LaneSlot::Node => &self.node,
        }
    }

    pub fn get_mut(&mut self, slot: LaneSlot) -> &mut SealRatio {
        match slot {
            LaneSlot::EdgeEvent => &mut self.edge_event,
            LaneSlot::EdgeInterval => &mut self.edge_interval,
            LaneSlot::Node => &mut self.node,
        }
    }

    /// `slot`'s accounted target for the configured on-disk `base`, or
    /// `base` itself when the correction is switched off.
    pub fn effective_target(&self, slot: LaneSlot, base: u64) -> u64 {
        if !self.adaptive {
            return base;
        }
        self.get(slot).effective_target(base)
    }
}

/// The resident cap in force, from `GNOMON_CHUNK_RESIDENT_BYTES` or the
/// default. `0` — from the environment only — means no cap.
///
/// Read per writer rather than cached in a `OnceLock` so a test can set it
/// and see it; a chunk writer is constructed once per merge group, never per
/// row, so the `var` lookup is not on any hot path.
pub fn chunk_resident_cap() -> u64 {
    match std::env::var("GNOMON_CHUNK_RESIDENT_BYTES") {
        Ok(v) => v.trim().parse().unwrap_or(defaults::CHUNK_RESIDENT_MAX_BYTES),
        Err(_) => defaults::CHUNK_RESIDENT_MAX_BYTES,
    }
}

/// Whether the measured correction is enabled. `GNOMON_SEAL_ADAPTIVE=0` pins
/// the unmeasured behaviour — the configured target is used as the accounted
/// target, unmeasured — which is what lets one binary serve both sides of the
/// A/B and what an operator reaches for if a corpus ever defeats the clamp.
pub fn adaptive_enabled() -> bool {
    !matches!(
        std::env::var("GNOMON_SEAL_ADAPTIVE").as_deref(),
        Ok("0") | Ok("off") | Ok("false")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: u64 = 128 * 1024 * 1024;

    #[test]
    fn an_unmeasured_ratio_is_the_configured_target() {
        // the property every unit-scale fixture relies on: a store that has
        // never sealed on the byte rule behaves exactly as it did previously
        let r = SealRatio::unmeasured();
        assert!(!r.is_measured());
        assert_eq!(r.effective_target(BASE), BASE);
    }

    #[test]
    fn the_measured_corpus_ratio_roughly_doubles_the_accounted_target() {
        // the measured segment: 2,323,687 rows, 71,761,054 B on disk, and
        // `rows x 50` accounted (the flat per-row model)
        let mut r = SealRatio::unmeasured();
        r.observe(71_761_054, 2_323_687 * 50);
        let eff = r.effective_target(BASE);
        // 50 / 30.88 = 1.619
        assert!(
            (1.55..1.70).contains(&(eff as f64 / BASE as f64)),
            "expected ~1.62x, got {}",
            eff as f64 / BASE as f64
        );
        // and the file that target produces is the configured target, to
        // within the arithmetic: rows = eff/50, bytes = rows * 30.88
        let rows = eff / 50;
        let bytes = rows as f64 * (71_761_054.0 / 2_323_687.0);
        assert!(
            (0.98..1.02).contains(&(bytes / BASE as f64)),
            "expected the file to land on target, got {}",
            bytes / BASE as f64
        );
    }

    #[test]
    fn observations_accumulate_rather_than_replace() {
        let mut a = SealRatio::unmeasured();
        a.observe(30, 60);
        a.observe(90, 120);
        // 120/180, not the mean of 2 and 1.33
        assert_eq!(a.effective_target(180), 180 * 180 / 120);
    }

    #[test]
    fn a_zero_observation_never_poisons_the_quotient() {
        let mut r = SealRatio::unmeasured();
        r.observe(100, 200);
        let before = r.effective_target(BASE);
        r.observe(0, 5_000_000);
        r.observe(5_000_000, 0);
        assert_eq!(r.effective_target(BASE), before);
    }

    #[test]
    fn the_clamp_holds_on_both_sides() {
        // an encoder that compressed 100x must not produce a 100x buffer
        let mut wild = SealRatio::unmeasured();
        wild.observe(1, 100);
        assert_eq!(
            wild.effective_target(BASE),
            BASE * defaults::SEAL_RATIO_MAX_NUM / defaults::SEAL_RATIO_MAX_DEN
        );
        // and one that expanded 100x must not seal one segment per row
        let mut dense = SealRatio::unmeasured();
        dense.observe(100, 1);
        assert_eq!(
            dense.effective_target(BASE),
            BASE * defaults::SEAL_RATIO_MIN_NUM / defaults::SEAL_RATIO_MIN_DEN
        );
    }

    #[test]
    fn the_quotient_is_order_independent() {
        // Two replays may observe the same segments in either order
        let mut a = SealRatio::unmeasured();
        let mut b = SealRatio::unmeasured();
        for (e, m) in [(31u64, 58u64), (29, 61), (33, 57)] {
            a.observe(e, m);
        }
        for (e, m) in [(33u64, 57u64), (31, 58), (29, 61)] {
            b.observe(e, m);
        }
        assert_eq!(a, b);
        assert_eq!(a.effective_target(BASE), b.effective_target(BASE));
    }

    #[test]
    fn the_resident_model_counts_every_owned_string() {
        let mut r = crate::row::EdgeRow {
            vid: crate::derive::Id96 { hi: 1, lo: 2 },
            src_id: 1,
            dst_id: 2,
            rel_type: String::new(),
            disc: String::new(),
            vt_s: 0,
            vt_e: 1,
            tt_s: 0,
            props: String::new(),
            source: String::new(),
            provenance_ref: None,
        };
        let empty = edge_resident_bytes(&r);
        assert_eq!(empty, defaults::EDGE_ROW_RESIDENT_FIXED);
        r.disc = "x".repeat(100);
        assert_eq!(
            edge_resident_bytes(&r),
            empty + 100 + defaults::STRING_ALLOC_OVERHEAD
        );
        r.provenance_ref = Some("y".repeat(10));
        assert_eq!(
            edge_resident_bytes(&r),
            empty + 110 + 2 * defaults::STRING_ALLOC_OVERHEAD
        );
    }
}
