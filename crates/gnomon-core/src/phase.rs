//! Per-phase timers for the read path.
//!
//! One question was left unanswered with a sharp shape: on a tiered store the
//! whole-extent aggregates cost 1.13–1.35× what they cost on a settled store
//! holding *identical* content — same bytes touched, same rows selected, same
//! decode count (zero, both configurations warm). The residual is neither per byte nor
//! per row nor a per-opened-segment constant (`≤ 0.03 ms`, measured on the
//! `*.w10` pair). Something is per selection *and* scales with the rows that
//! selection carries, and no counter in the engine could say which phase.
//!
//! These timers say which phase. They are deliberately **process-global**
//! rather than per handle, for a reason learned the hard way:
//! a harness that opens ten store handles and reads a per-handle counter off
//! the wrong one reports zeros and calls them a finding. A global is immune —
//! every handle reads the same numbers, and there is no wrong handle to read.
//!
//! Cost: `Instant::now()` a handful of times per *segment*, never per row. On
//! a 29-segment whole-extent aggregate that is well under 10 µs against a
//! 120 ms query. Nothing here is ever read back by engine code; these are
//! counters, never a correctness input, and never a scheduling input.
//!
//! ## Reading them
//!
//! [`reset`] then one query then [`snapshot`] gives that query's phase split.
//! `*_max_ns` is a running maximum, so it only means "the slowest worker in
//! this query" when the reset bracketed exactly one query — which is how a
//! measurement driver reads it.
//!
//! The pair that matters is `busy` against `wall` for each parallel stage:
//! `busy / (wall * width)` is the stage's parallel efficiency, and a stage
//! whose units are unequal in size loses exactly that ratio to its makespan
//! however many workers it is given.

use std::sync::atomic::{AtomicU64, Ordering};

/// One phase's slot. Indices are stable; [`NAMES`] is the wire order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum Phase {
    /// `aggregate_edges` calls entered.
    AggCalls = 0,
    /// Building the node-label temporal index (only when a `Label` dim asks).
    AggLabels,
    /// The manifest-level zone-map prune loop — no file mapped, no column read.
    AggPrune,
    /// Opening the surviving segments (cached), summed.
    AggOpen,
    /// The sorted global rel-name table over the opened headers.
    AggRelNames,
    /// Wall time of `ScanSet::select` as the aggregate calls it.
    AggSelectWall,
    /// Wall time of `aggregate_partials` — the parallel fold plus its merge.
    AggFoldWall,
    /// Inside `aggregate_partials`: the cross-worker merge of partial maps.
    AggMerge,
    /// `finalize` — the cap check, the canonical sort, the distinct dedupe,
    /// the output columns.
    AggFinalize,

    /// Wall time of `ScanSet::select`, summed over every call — the aggregate
    /// path calls it once, the interval join four times, `diff_snapshots`
    /// twice per snapshot. `AggSelectWall` is the aggregate's single call;
    /// this one also covers the scan/materialize operators, which have no
    /// aggregate wall of their own.
    SelWall,
    /// `select_range` CPU summed over workers. Against `SelWall` this is the
    /// select stage's parallel efficiency.
    SelBusy,
    /// The single slowest `select_range` worker — the stage's makespan.
    SelMaxBusy,
    /// `select_range` invocations (workers, not segments).
    SelWorkers,
    /// Per segment: resolving column handles and the rel-allow table.
    SelCols,
    /// Per segment: `believed_ranges` + `subtract_closes`. This is the term
    /// Two earlier approaches left this confounded with segment
    /// count, and it is separated here by measurement rather than by building
    /// a third store. It used to be window-independent, which is what made
    /// it the `*.w10` residual on fully compacted stores with corrections.
    /// It is now computed for the request's rows only, unless the request
    /// covers most of the segment.
    SelCloses,
    /// Per segment: walking the verdict ranges and materialising row ids.
    SelRows,
    /// Rows a segment's ranges handed to the row loop, summed.
    SelRowsExamined,

    /// `fold_selections` CPU summed over workers.
    FoldBusy,
    /// The single slowest fold worker — the fold stage's makespan.
    FoldMaxBusy,
    /// Fold workers spawned (1 when the stage ran serial).
    FoldWorkers,
    /// Per selection: `SegCols::open` — column handles and the rel map.
    FoldCols,
    /// Per selection: the grouped row loop itself.
    FoldRows,
    /// Rows the fold's row loop walked, summed.
    FoldRowsWalked,
    /// Selections the fold consumed.
    FoldUnits,

    /// Row-range units the select stage cut — `(segment, row range)`
    /// pairs, not segments. This is the number an earlier engine could not
    /// move: it was exactly the opened-segment count, so a settled store's
    /// parallelism was a property of its layout.
    SelUnits,
    /// Segments that survived the prune and carried at least one
    /// window-clipped believed row, i.e. how many segments those units were
    /// cut from. `sel_units / sel_segments_planned` is the split factor.
    SelSegmentsPlanned,
    /// CPU of the select stage's **planning** pass, summed over its
    /// workers — the per-segment work (`sel_cols_ns` + `sel_closes_ns`) that
    /// every unit cut from a segment now shares instead of repeating.
    /// `sel_busy_ns` covers only the row loop after this split, so the select
    /// stage's total CPU is the two added.
    SelPlanBusy,

    /// Fold workers the **budget** allowed — `FOLD_MAP_BUDGET_BYTES /
    /// est_map_bytes`, saturated at the thread count and before the gate.
    /// Equal to the thread count whenever the cap did not bind, which is how
    /// a record says "this cell was unaffected" without a second run.
    FoldWidthCap,
    /// The per-worker partial group table size the width policy
    /// *predicted*, in bytes. Paired with [`Phase::FoldMapBytesMax`] it is
    /// the estimator's own error bar, measured on every query rather than
    /// argued for once.
    FoldMapBytesEst,
    /// Bytes actually held by the partial group tables, summed over
    /// workers as each finishes. The width-scaling part only — keys, state
    /// headers, stat slots and the table's own slack — because the distinct
    /// sets are bounded per selected *row* rather than per worker
    /// and so do not multiply with width.
    FoldMapBytes,
    /// The largest single worker's share of [`Phase::FoldMapBytes`].
    FoldMapBytesMax,
    /// Dictionary-coded `tt_s` codes the belief test read row by row
    /// ([`crate::vistouch`]). A count, compiled out with the timers.
    VisCodes,
    /// `tt_s` run-list entries the belief test walked.
    VisRuns,
    /// Close entries the belief test copied, merged or walked.
    VisCloses,
    /// Inner iterations of the belief diff's range-list difference.
    VisSteps,
    /// Segments skipped on manifest or header bounds, summed over every
    /// prune site; zero under `GNOMON_SEGMENT_PRUNE=0`.
    SegPruned,
    /// Rows given a per-row belief test; nonzero only under
    /// `GNOMON_RANGE_VISIBILITY=0`.
    VisRows,
    /// Endpoint-column rows scanned for adjacency; nonzero only under
    /// `GNOMON_TCSR=scan`.
    AdjScanRows,
    /// Adjacency posting rows walked by filtered scans, and
    /// the segments whose rows came from postings.
    SelAdjRows,
    SelAdjSegments,
    /// Aggregate calls answered from believed-range lengths (the
    /// count-by-bucket path); zero under `GNOMON_AGG_RANGE_COUNT=0`.
    AggRangeCounts,
    /// The reachability kernel's stages — the window's believed
    /// edges gathered from the planner's ranges, the CSR, the label-setting
    /// search, and the `(arrival, uid)` page.
    ReachEdges,
    ReachCsr,
    ReachSearch,
    ReachPage,
    /// The snapshot diff's stages — the node half, the two edge
    /// point-state plans, the row merge plus identity join, and the eids and
    /// pages of the added/removed edges.
    SnapNodes,
    SnapEdgePlan,
    SnapEdgeJoin,
    SnapEdgePages,
    /// Time spent deriving event-lane `vt_e` staircases at
    /// cached segment opens, summed.
    StairDeriveNs,
    /// Segments whose staircase was derived at open.
    StairDerived,
    /// Heap the derived staircases hold, summed over the
    /// derivations counted (bytes).
    StairDerivedBytes,
    /// `match` plans the engine executed (`matcher.rs`).
    MatchCalls,
    /// `match`: the plan's execution up to the page, summed.
    MatchExecNs,
    /// `match`: adjacency calls (`neighbors_batch`) made by its hops, summed.
    MatchHopNs,
    /// `match`: building the page's values (strings for the page only).
    MatchPageNs,
}

/// Wire names, indexed by [`Phase`]. Kept beside the enum so the two cannot
/// drift; a test asserts the lengths agree.
pub const NAMES: [&str; 56] = [
    "agg_calls",
    "agg_labels_ns",
    "agg_prune_ns",
    "agg_open_ns",
    "agg_rel_names_ns",
    "agg_select_wall_ns",
    "agg_fold_wall_ns",
    "agg_merge_ns",
    "agg_finalize_ns",
    "sel_wall_ns",
    "sel_busy_ns",
    "sel_max_busy_ns",
    "sel_workers",
    "sel_cols_ns",
    "sel_closes_ns",
    "sel_rows_ns",
    "sel_rows_examined",
    "fold_busy_ns",
    "fold_max_busy_ns",
    "fold_workers",
    "fold_cols_ns",
    "fold_rows_ns",
    "fold_rows_walked",
    "fold_units",
    "sel_units",
    "sel_segments_planned",
    "sel_plan_busy_ns",
    "fold_width_cap",
    "fold_map_bytes_est",
    "fold_map_bytes",
    "fold_map_bytes_max",
    "vis_codes",
    "vis_runs",
    "vis_closes",
    "vis_steps",
    "seg_pruned",
    "vis_rows",
    "adj_scan_rows",
    "sel_adj_rows",
    "sel_adj_segments",
    "agg_range_counts",
    "reach_edges_ns",
    "reach_csr_ns",
    "reach_search_ns",
    "reach_page_ns",
    "snap_nodes_ns",
    "snap_edge_plan_ns",
    "snap_edge_join_ns",
    "snap_edge_pages_ns",
    "stair_derive_ns",
    "stair_derived",
    "stair_derived_bytes",
    "match_calls",
    "match_exec_ns",
    "match_hop_ns",
    "match_page_ns",
];

const N: usize = NAMES.len();

static SLOTS: [AtomicU64; N] = [const { AtomicU64::new(0) }; N];

/// Add `v` to a phase. Relaxed: these are counters read after the work, never
/// synchronising anything.
#[inline]
pub(crate) fn add(p: Phase, v: u64) {
    SLOTS[p as usize].fetch_add(v, Ordering::Relaxed);
}

/// Add the elapsed nanoseconds since `t` to a phase.
#[inline]
pub(crate) fn since(p: Phase, t: std::time::Instant) {
    add(p, t.elapsed().as_nanos() as u64);
}

/// Raise a phase's running maximum to `v`.
#[inline]
pub(crate) fn max(p: Phase, v: u64) {
    SLOTS[p as usize].fetch_max(v, Ordering::Relaxed);
}

/// Record one worker's busy time in both forms at once: summed into `busy`,
/// folded into `busy_max`. The pair is what separates "this stage has too
/// little work" from "this stage has enough work and gave it to the wrong
/// worker".
#[inline]
pub(crate) fn worker(busy: Phase, busy_max: Phase, t: std::time::Instant) {
    let ns = t.elapsed().as_nanos() as u64;
    add(busy, ns);
    max(busy_max, ns);
}

/// Every slot, in [`NAMES`] order.
pub fn snapshot() -> [u64; N] {
    std::array::from_fn(|i| SLOTS[i].load(Ordering::Relaxed))
}

/// Zero every slot. A driver brackets one query between this and
/// [`snapshot`]; `*_max_ns` is only meaningful inside such a bracket.
pub fn reset() {
    for s in SLOTS.iter() {
        s.store(0, Ordering::Relaxed);
    }
}

/// `(name, value)` pairs, for a binding that wants a dict.
pub fn snapshot_named() -> Vec<(&'static str, u64)> {
    let v = snapshot();
    NAMES.iter().copied().zip(v).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_cover_every_slot_in_declaration_order() {
        // The enum's last variant must be the last name: a phase added
        // without a name would silently write into another phase's slot.
        assert_eq!(Phase::MatchPageNs as usize, N - 1);
        assert_eq!(NAMES[Phase::MatchCalls as usize], "match_calls");
        assert_eq!(NAMES[Phase::SnapEdgePages as usize], "snap_edge_pages_ns");
        assert_eq!(NAMES[Phase::StairDeriveNs as usize], "stair_derive_ns");
        assert_eq!(NAMES[Phase::ReachEdges as usize], "reach_edges_ns");
        assert_eq!(NAMES[Phase::AggRangeCounts as usize], "agg_range_counts");
        assert_eq!(NAMES[Phase::SelAdjRows as usize], "sel_adj_rows");
        assert_eq!(NAMES[Phase::AdjScanRows as usize], "adj_scan_rows");
        assert_eq!(NAMES[Phase::VisRows as usize], "vis_rows");
        assert_eq!(NAMES[Phase::SegPruned as usize], "seg_pruned");
        assert_eq!(NAMES[Phase::VisSteps as usize], "vis_steps");
        assert_eq!(NAMES[Phase::FoldMapBytesMax as usize], "fold_map_bytes_max");
        assert_eq!(NAMES[Phase::VisCodes as usize], "vis_codes");
        assert_eq!(NAMES[Phase::SelPlanBusy as usize], "sel_plan_busy_ns");
        assert_eq!(NAMES[Phase::FoldWidthCap as usize], "fold_width_cap");
        assert_eq!(NAMES[Phase::SelUnits as usize], "sel_units");
        assert_eq!(NAMES[Phase::AggCalls as usize], "agg_calls");
        assert_eq!(NAMES[Phase::SelCloses as usize], "sel_closes_ns");
        assert_eq!(NAMES[Phase::FoldRows as usize], "fold_rows_ns");
    }

    #[test]
    fn worker_records_sum_and_max_together() {
        reset();
        let t = std::time::Instant::now();
        while t.elapsed().as_micros() < 200 {
            std::hint::spin_loop();
        }
        worker(Phase::FoldBusy, Phase::FoldMaxBusy, t);
        let t2 = std::time::Instant::now();
        worker(Phase::FoldBusy, Phase::FoldMaxBusy, t2);
        let s = snapshot();
        let busy = s[Phase::FoldBusy as usize];
        let peak = s[Phase::FoldMaxBusy as usize];
        assert!(busy >= peak, "sum {busy} must cover the max {peak}");
        assert!(peak >= 100_000, "first worker's 200 us should dominate: {peak}");
        reset();
        assert_eq!(snapshot()[Phase::FoldBusy as usize], 0);
    }
}
