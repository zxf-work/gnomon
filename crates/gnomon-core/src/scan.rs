//! The scan cursor — the engine's internal read path.
//!
//! This is deliberately *not* the `StorageAdapter` interface. Kernels consume
//! selections and column views directly; materializing a sorted struct-of-
//! arrays happens once, at the public boundary, and only for callers that
//! actually need it. Putting the ABC in the middle would pay an allocation
//! and a copy between every stage.
//!
//! Filtering matches `duckdb_adapter.edges_columnar` exactly, because the two
//! backends must return identical rows:
//!
//! ```sql
//! WHERE tt_s <= as_of AND as_of < tt_e          -- belief
//!   AND vt_e > vt_min AND vt_s < vt_max         -- valid-time overlap
//!   AND rel_type IN (...)                       -- optional
//!   AND (src_id IN touching OR dst_id IN touching)   -- or AND, see below
//! ORDER BY vt_s, vid
//! ```
//!
//! Work is skipped in three escalating tiers: whole segments are dropped from
//! the manifest's zone maps without any I/O; inside a surviving segment the
//! sorted `vt_s` column turns both window bounds into binary searches; only
//! what is left is examined per row.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Parse a `GNOMON_SCAN_THREADS` override: a positive integer, clamped to 64.
/// Anything unparseable (empty, zero, garbage) is treated as unset rather
/// than as an error — a measurement knob must never make reads fail.
fn scan_threads_override(v: Option<&str>) -> Option<usize> {
    v.and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .map(|n| n.min(64))
}

/// Worker count for the parallel scan stages.
///
/// `GNOMON_SCAN_THREADS` overrides, including oversubscription past the core
/// count, hence the 64 clamp.
/// Read per call, so a harness can sweep thread counts within one process
/// against one warm store.
///
/// When unset: one worker per available core, with no fixed cap — on a
/// 32-vCPU machine, 32 workers took `series.count` from 23.6 ms (at 16) to
/// 21.1 ms — and the stages run on the persistent pool
/// ([`crate::pool`]), so the width costs a wake-up rather than a spawn.
pub(crate) fn scan_threads() -> usize {
    scan_threads_override(std::env::var("GNOMON_SCAN_THREADS").ok().as_deref())
        .unwrap_or_else(available_cores)
}

/// `std::thread::available_parallelism()`, read once per process.
///
/// On Linux it is not a cheap call: it reads the cgroup CPU quota files and
/// the affinity mask, about 0.15 ms per call on the 32-vCPU host, and
/// it was being paid on every scan and every aggregate. Neither the quota
/// nor the affinity of a running engine is something it reacts to mid-life.
pub(crate) fn available_cores() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    })
}

/// Row threshold above which a stage fans out. `GNOMON_PARALLEL_MIN_ROWS`
/// overrides it, for the same reason `GNOMON_SCAN_THREADS` exists: the gate
/// was calibrated from a sweep, and re-running that sweep at a new scale or
/// on a new stage must not need a rebuild. Unparseable values are treated as
/// unset — a measurement knob must never make reads fail.
pub(crate) fn parallel_min_rows() -> u64 {
    std::env::var("GNOMON_PARALLEL_MIN_ROWS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(crate::defaults::PARALLEL_SCAN_MIN_ROWS)
}

/// Answer the window cut from the model rather than from the decoded
/// column?
///
/// **Off by default, on measurement.** The model's two-level search is
/// slower than a `partition_point` over the already-decoded column on every
/// real segment in this corpus: 1.40x at 2.68M rows and 1.45x at 1.44M,
/// against a forecast of 0.50-0.74x. The forecast was not
/// wrong about the *mechanism* — the ratio falls monotonically with segment
/// size, 2.03 at 506k, 1.45 at 1.44M, 1.40 at 2.68M, **0.98 at 5.15M** — it
/// was wrong about the size at which it pays. The crossover is around five
/// million rows, roughly four times the segments this store writes today.
///
/// So the default is the search that is faster now, and the model path is
/// there for the segment sizes that would flip it. A micro-benchmark of the
/// two searches is kept precisely so that a later change to the seal target
/// can flip
/// this default **on evidence** rather than on the argument that the model
/// ought to win.
///
/// `GNOMON_LINFOR_LAZY=1` implies this: holding a codec-2 column back from
/// materialisation buys nothing if the planner then asks for it as a slice.
pub(crate) fn linfor_range_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("GNOMON_LINFOR_RANGE").map(|v| v == "1").unwrap_or(false)
            || crate::segment::linfor_lazy()
    })
}

/// The `vt_s` column as the planner needs it: a length and a cut.
///
/// Those two operations are all the planner asks of `vt_s` — `SegPlan`
/// carries the other columns but never this one — and both are answerable
/// from a codec-2 column's own block-first-value vector without decoding a
/// single residual. That vector is dense, `i64`, and one entry per 1024
/// rows, so the outer search is `log2(rows/1024)` probes into something that
/// fits in cache, against `log2(rows)` probes into an 8-byte-per-row array
/// that does not. The two-level search measured at 0.50–0.74x a
/// `partition_point` over the already-decoded column on
/// 1.4M- and 2.7M-row segments, and the gap widens with segment size.
///
/// Any other shape — codec 0 or 1, a codec-2 column the writer did not mark
/// sorted, or the default configuration — takes the slice, and behaves
/// exactly as before. See [`linfor_range_enabled`] for why the model path
/// is opt-in: it is measured slower than the decoded search at every
/// segment size this store currently writes, and faster only above about
/// five million rows.
enum VtS<'a> {
    Slice(&'a [i64]),
    Model(crate::linfor::LinColumn<'a>),
}

impl<'a> VtS<'a> {
    fn open<S: crate::segment::SegmentSource>(
        seg: &'a crate::segment::Segment<S>,
    ) -> crate::Result<Self> {
        match seg.lin_column("vt_s")? {
            Some(m) if m.is_sorted() && linfor_range_enabled() => Ok(VtS::Model(m)),
            _ => Ok(VtS::Slice(seg.i64_column("vt_s")?)),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            VtS::Slice(v) => v.len(),
            VtS::Model(m) => m.len(),
        }
    }

    /// How many leading rows have `vt_s < t`.
    #[inline]
    fn partition_point_lt(&self, t: i64) -> usize {
        match self {
            VtS::Slice(v) => v.partition_point(|&x| x < t),
            VtS::Model(m) => m.partition_point_lt(t),
        }
    }
}

// The `vt_e` block prune, off: `GNOMON_NO_STAIRCASE=1`, read at open
// ([`crate::designs::Designs::staircase`]). With it off the planner
// never applies the prefix cut, so the row loop runs over exactly the rows
// it would without any staircase -- which isolates "the staircase changed
// which rows are examined" from "the staircase changed the answer". The
// interval staircase test sets it for its A/B comparison.
// The *structure* is renamed because "staircase" collides with Staircase
// Join (Grust et al., 2003) and because a per-block maximum used to skip
// blocks is small materialized aggregates (Moerkotte, 1998), not something
// this engine gets to name.

/// The prefix of `open` whose rows fall below `lo`, clipped at `lo`.
///
/// Returns a borrowed prefix rather than a new vector: the ranges are
/// already sorted and disjoint, and all but at most the last of them lie
/// wholly below the cut. The caller clips that last one itself, which it has
/// to do anyway when intersecting with a belief range.
fn open_ranges_below(open: &[(u32, u32)], lo: usize) -> &[(u32, u32)] {
    let n = open.partition_point(|&(s, _)| (s as usize) < lo);
    &open[..n]
}

/// Should a scan stage fan out? `units` is the stage's chunking unit
/// (segments for select, clusters for materialize), `rows` the stage's work
/// proxy (candidate rows for select, selected rows for materialize).
///
/// Recalibrated: the old gates were segment-count-only, and
/// parallel select measured *slower* than
/// serial at every width 2–16 on a 1M-row store while paying 4.3× at 10M —
/// so the deciding term is rows, with the unit minimum kept only so there
/// is something to chunk. Widths below `PARALLEL_SCAN_MIN_THREADS` never
/// engage: t=2 lost to t=1 at both measured scales.
pub(crate) fn parallel_gate(threads: usize, units: usize, min_units: usize, rows: u64) -> bool {
    parallel_gate_at(threads, units, min_units, rows, parallel_min_rows())
}

/// [`parallel_gate`] with the row threshold given: the one form both the
/// scan gate and the aggregate's own gate ([`agg_parallel_min_rows`]) use,
/// so the width and unit terms cannot drift apart between them.
pub(crate) fn parallel_gate_at(
    threads: usize,
    units: usize,
    min_units: usize,
    rows: u64,
    min_rows: u64,
) -> bool {
    threads >= crate::defaults::PARALLEL_SCAN_MIN_THREADS
        && units >= min_units
        && rows >= min_rows
}

/// Parse a row-threshold override: a non-negative integer, or unset.
/// Unparseable values are treated as unset — a measurement knob must never
/// make reads fail.
fn min_rows_override(v: Option<&str>) -> Option<u64> {
    v.and_then(|s| s.trim().parse::<u64>().ok())
}

/// The grouped aggregate's own row threshold:
/// [`crate::defaults::AGG_PARALLEL_MIN_ROWS`], or
/// `GNOMON_AGG_PARALLEL_MIN_ROWS` for a measurement sweep. Read per call, like
/// `GNOMON_PARALLEL_MIN_ROWS`, so a sweep can run in one process.
pub(crate) fn agg_parallel_min_rows() -> u64 {
    min_rows_override(std::env::var("GNOMON_AGG_PARALLEL_MIN_ROWS").ok().as_deref())
        .unwrap_or(crate::defaults::AGG_PARALLEL_MIN_ROWS)
}

use crate::derive::Id96;
use crate::error::Result;
use crate::row::Lane;
use crate::segment::{Segment, SegmentSource, StringView};
use crate::visibility::CloseIndex;
use crate::{clamp_tt, OPEN_END};

#[derive(Clone, Debug, Default)]
pub struct ScanRequest {
    /// Belief time. `OPEN_END` (the default) means current beliefs.
    pub as_of_tt: i64,
    /// Half-open valid-time window; `None` is unbounded on that side.
    pub vt_min: Option<i64>,
    pub vt_max: Option<i64>,
    pub rel_types: Option<Vec<String>>,
    /// Incidence filter over dense ids. Must be sorted (binary-searched).
    pub touching_ids: Option<Vec<u32>>,
    /// Require *both* endpoints in `touching_ids` rather than either.
    ///
    /// Or-incidence is what most callers want ("edges at this node"). Motif
    /// matching wants the and-form, and expressing it above the scan means
    /// deriving `eid` — a sha256 per row — for rows the caller is about to
    /// discard. No effect unless `touching_ids` is set.
    pub touching_both: bool,
    /// Stop after this many rows. Applied after ordering.
    pub limit: Option<usize>,
    /// Event requests only: rows must also have `vt_s >= vt_s_min`.
    ///
    /// An aggregate over events (`aggregate_events`: `t_a <= vt_s < t_b`)
    /// asks the overlap question `vt_e > t_a` and then discards every row
    /// that starts before `t_a`. Those rows are found by the `vt_e` cut,
    /// which only bounds closed rows through the staircase and so, past the
    /// first long-lived row of a segment, examines everything from there to
    /// `t_b`: the cost of a ten-tick window grew with its distance from the
    /// segment's start. `vt_s` is sorted, so `vt_s >= vt_s_min` is a prefix
    /// cut by binary search. `None` (the default) keeps every request as it
    /// was.
    pub vt_s_min: Option<i64>,
    /// Columns to materialize. `None` means all of them.
    ///
    /// This is a real pushdown, not a filter on the way out: building a
    /// string per row for a column the caller discards was the dominant cost
    /// of a wide scan, and `edges_columnar` never asks for `disc` or `props`.
    pub columns: Option<Vec<String>>,
}

impl ScanRequest {
    pub fn current() -> Self {
        Self {
            as_of_tt: OPEN_END,
            ..Default::default()
        }
    }

    pub fn window(mut self, t_a: i64, t_b: i64) -> Self {
        self.vt_min = Some(t_a);
        self.vt_max = Some(t_b);
        self
    }

    pub fn touching(mut self, mut ids: Vec<u32>) -> Self {
        ids.sort_unstable();
        ids.dedup();
        self.touching_ids = Some(ids);
        self
    }

    /// `touching`, but both endpoints must be in the set.
    pub fn touching_both(mut self, ids: Vec<u32>) -> Self {
        self = self.touching(ids);
        self.touching_both = true;
        self
    }
}

/// The zone-map bounds a tier-1 prune reads, named independently of where
/// they were read from.
///
/// A segment's *header* and its *manifest* [`crate::manifest::SegmentEntry`]
/// carry the same three valid-time bounds and the same folded `tt_s`
/// minimum: `staging::entry_for` derives the entry from exactly the rows the
/// segment writer summarises into the header, with the same `vt_s`/`vt_e`/
/// `tt_s` accessors, and a segment file is immutable once written. So one
/// predicate answers "can this segment contribute?" both after a segment is
/// open and — in `aggregate.rs` — from the manifest alone, before any file
/// is mapped or any column decoded.
pub struct SegBounds<'a> {
    /// Folded minimum `tt_s`: nothing stored here was believed before this.
    pub tt_s_min: i64,
    /// Minimum `vt_s` over the segment's rows.
    pub vt_min: i64,
    /// Maximum `vt_e` over the segment's rows — the exclusive upper edge of
    /// everything the segment can cover. The event lane elides `vt_e` per row
    /// (`vt_e = vt_s + 1`), so there this is `vt_max + 1`; the interval lane
    /// carries real interval ends, including `OPEN_END` for still-open rows,
    /// which is why the same field serves both lanes.
    pub vt_e_max: i64,
    /// Relation names this segment holds, or empty for "not known here".
    ///
    /// Empty disables the relation test rather than pruning on it. Two
    /// callers rely on that: a segment header legitimately carries no
    /// `rel_types` (the node lanes), and a manifest entry's `rel_codes` are
    /// *segment-local* codes into that segment's own name table, which the
    /// manifest does not carry — so a manifest-level prune leaves the
    /// relation test to the header-level one instead of guessing.
    pub rel_types: &'a [String],
}

/// Tier 1: can a segment with these bounds contribute anything at all?
///
/// The single copy of the predicate. `ScanSet::prunes` calls it with header
/// bounds after opening; `NativeStore::aggregate_edges` calls it with
/// manifest bounds before opening. A second, drifting copy of this test is
/// exactly how the aggregate path came to open every segment on every call.
pub fn prunes_bounds(b: &SegBounds<'_>, req: &ScanRequest) -> bool {
    let as_of = clamp_tt(req.as_of_tt);
    // nothing in this segment was believed yet at as_of
    if b.tt_s_min > as_of {
        return true;
    }
    if let Some(vt_min) = req.vt_min {
        if b.vt_e_max <= vt_min {
            return true;
        }
    }
    if let Some(vt_max) = req.vt_max {
        if b.vt_min >= vt_max {
            return true;
        }
    }
    if let Some(rels) = &req.rel_types {
        if !b.rel_types.is_empty() && !b.rel_types.iter().any(|r| rels.contains(r)) {
            return true;
        }
    }
    false
}

/// Pruning effectiveness, surfaced into the existing trace-record counters.
/// Without these, "the zone maps are working" is an assumption rather than an
/// observation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanStats {
    pub segments_total: usize,
    pub segments_pruned: usize,
    pub rows_examined: u64,
    pub rows_selected: u64,
    /// Blocks of an interval-lane staircase cut from `lo` by the prefix
    /// search, summed across segments. Zero on every event-lane segment and
    /// on any interval-lane segment written before the staircase existed.
    pub blocks_skipped: u64,
    /// Blocks in interval-lane segments that survived `prunes` and carried a
    /// staircase, i.e. the denominator `blocks_skipped` is a fraction of.
    pub blocks_considered: u64,
    /// The same two counters for the staircase **derived at
    /// open** on event-lane segments that store `vt_e`. Kept apart so that
    /// `blocks_skipped`/`blocks_considered` keep meaning the written
    /// interval-lane structure, which the lane-routing tests read.
    pub derived_blocks_skipped: u64,
    pub derived_blocks_considered: u64,
    /// Adjacency posting rows a filtered scan walked: the slice lengths
    /// of the filter's ids in the segments whose rows came from postings
    /// (fix B). Zero when no segment's did.
    pub adj_rows_walked: u64,
    /// Segments whose rows came from adjacency postings.
    pub adj_segments: usize,
}

/// Where a filtered scan gets one segment's rows incident to
/// the request's ids: the segment's adjacency index, or with the index off
/// (`GNOMON_TCSR=scan`) one scan of its endpoint columns. The store provides
/// it ([`crate::store::NativeStore::scan_edges`]); a bare scan has none.
pub trait PostingsSource: Sync {
    /// Rows of target `idx` whose `src_id` -- and, unless `src_only`, whose
    /// `dst_id` -- is in `ids`, ascending and deduplicated. `None` when the
    /// posting rows are not fewer than `budget`, the rows the scan would
    /// examine in that segment: then the scan is no dearer, and runs.
    fn postings(
        &self,
        idx: usize,
        ids: &[u32],
        src_only: bool,
        budget: usize,
    ) -> Result<Option<Postings>>;
}

/// One segment's rows from [`PostingsSource::postings`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Postings {
    /// ascending, deduplicated row ids
    pub rows: Vec<u32>,
    /// posting rows walked to find them (before deduplication)
    pub walked: u64,
}

/// Wall time of each scan stage, in nanoseconds.
///
/// Three `Instant::now()` calls per scan, on a path that moves tens of
/// megabytes — the same argument that put the pruning counters in
/// [`ScanStats`]. Without them "the scan is the cost" is a subtraction done
/// in Python against a wall clock that also contains segment opening, `eid`
/// derivation and the NumPy boundary, and telling those apart matters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanTimings {
    pub select_ns: u64,
    pub cluster_ns: u64,
    pub materialize_ns: u64,
    /// Inside `materialize`: CPU time summed over workers, so these two do
    /// not add up to `materialize_ns` when the stage fanned out. `order_ns`
    /// is the within-cluster k-way merge that produces an order list;
    /// `copy_ns` is the run-walk that copies columns.
    pub order_ns: u64,
    pub copy_ns: u64,
    /// Cluster shape, which decides which of the two paths every row takes.
    pub clusters: u32,
    pub clusters_multi: u32,
}

/// Worker-side accumulator for the two halves of materialization. One
/// relaxed atomic add per cluster — the stage fans out, so a plain counter
/// would need the lock the stage exists to avoid.
#[derive(Default)]
struct StageProbe {
    order_ns: std::sync::atomic::AtomicU64,
    copy_ns: std::sync::atomic::AtomicU64,
}

impl StageProbe {
    fn order(&self, t: std::time::Instant) {
        self.order_ns.fetch_add(
            t.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
    fn copy(&self, t: std::time::Instant) {
        self.copy_ns.fetch_add(
            t.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

/// Row ids selected within one segment, in ascending (already sorted) order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Selection {
    pub segment: usize,
    pub rows: Vec<u32>,
}

/// One segment in a scan, with the file id close runs address it by.
pub struct ScanTarget<'a, S: SegmentSource> {
    pub segment: &'a Segment<S>,
    pub lane: Lane,
    pub id: u64,
}

/// A set of segments queried together. Segments within a lane written by one
/// batch have disjoint key ranges, but successive generations overlap, so the
/// merge below is a general k-way merge rather than a concatenation.
pub struct ScanSet<'a, S: SegmentSource> {
    targets: Vec<ScanTarget<'a, S>>,
    closes: std::sync::Arc<CloseIndex>,
    /// The designs the owning handle runs with. Every production
    /// constructor passes the handle's ([`ScanSet::with_designs`]); a bare
    /// scan runs every design.
    designs: crate::designs::Designs,
    /// Adjacency postings for a filtered scan, when the store attached
    /// them ([`ScanSet::with_postings`]).
    postings: Option<&'a dyn PostingsSource>,
    /// The row threshold of the select's row loop (stage R) when
    /// the caller has its own ([`ScanSet::with_row_loop_min_rows`]); `None`
    /// is the scan gate, [`parallel_min_rows`].
    row_loop_min_rows: Option<u64>,
}

impl<'a, S: SegmentSource> ScanSet<'a, S> {
    pub fn new(targets: Vec<ScanTarget<'a, S>>) -> Self {
        Self {
            targets,
            closes: std::sync::Arc::default(),
            designs: crate::designs::Designs::ALL,
            postings: None,
            row_loop_min_rows: None,
        }
    }

    /// The segment behind a plan's `idx` (the range consumers read
    /// their own columns from it).
    pub(crate) fn segment(&self, idx: usize) -> &'a Segment<S> {
        self.targets[idx].segment
    }

    /// How many segments this scan was built over.
    pub(crate) fn n_targets(&self) -> usize {
        self.targets.len()
    }

    /// For callers with no close runs — segment ids are positional.
    pub fn from_pairs(pairs: Vec<(&'a Segment<S>, Lane)>) -> Self {
        Self::new(
            pairs
                .into_iter()
                .enumerate()
                .map(|(i, (segment, lane))| ScanTarget {
                    segment,
                    lane,
                    id: i as u64,
                })
                .collect(),
        )
    }

    /// Attach the closes a manifest generation makes visible. Without this a
    /// scan sees every stored row as still believed.
    pub fn with_closes(mut self, closes: std::sync::Arc<CloseIndex>) -> Self {
        self.closes = closes;
        self
    }

    /// Run this scan with the owning handle's designs.
    pub fn with_designs(mut self, designs: crate::designs::Designs) -> Self {
        self.designs = designs;
        self
    }

    /// The designs this scan runs with.
    pub fn designs(&self) -> crate::designs::Designs {
        self.designs
    }

    /// Answer an incidence-filtered request from adjacency
    /// postings wherever they name fewer rows than the scan would examine.
    pub fn with_postings(mut self, postings: &'a dyn PostingsSource) -> Self {
        self.postings = Some(postings);
        self
    }

    /// Gate this scan's row loop (stage R of [`ScanSet::select_units`]) at
    /// `min_rows` examined rows instead of the scan gate. The
    /// grouped aggregate passes its own threshold here; its planner (stage
    /// P) keeps the scan gate, and so does every other scan.
    pub fn with_row_loop_min_rows(mut self, min_rows: u64) -> Self {
        self.row_loop_min_rows = Some(min_rows);
        self
    }

    pub fn len(&self) -> usize {
        self.targets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    /// Tier 1: can this segment contribute anything at all? Answered from the
    /// header alone — no column bytes are touched.
    fn prunes(&self, seg: &Segment<S>, req: &ScanRequest) -> bool {
        let h = seg.header();
        prunes_bounds(
            &SegBounds {
                // From the folded bound, not a pass over `tt_s_runs`: that
                // pass is O(runs) per segment per scan, and a compacted
                // segment carries one run per generation boundary — millions
                // of them, for an answer that never varies.
                tt_s_min: seg.tt_s_bounds().0,
                vt_min: h.vt_min,
                vt_e_max: h.vt_e_max,
                rel_types: &h.rel_types,
            },
            req,
        )
    }

    /// Tiers 2 and 3: narrow by binary search, then test what remains.
    ///
    /// Every column handle and lookup table is resolved **once per segment**.
    /// Doing any of it per row (re-resolving a column by name, walking the
    /// `tt_s` runs, scanning a `Vec` of allowed rel codes) costs more than the
    /// predicate itself and made this path slower than vectorized NumPy —
    /// measured, then fixed.
    ///
    /// Returns **one selection per segment**, which is the contract every
    /// caller but the aggregate fold wants. [`ScanSet::select_units`] is the
    /// same scan at row-range granularity; this is that call with each
    /// segment's units concatenated back together.
    pub fn select(&self, req: &ScanRequest) -> Result<(Vec<Selection>, ScanStats)> {
        let (units, stats) = self.select_units(req)?;
        Ok((coalesce_by_segment(units), stats))
    }

    /// The scan, at **row-range** granularity.
    ///
    /// Making both parallel stages self-schedule fixed the *order* units are
    /// claimed in but not their *number*: one unit per opened segment meant a
    /// store with few large segments could not fill the workers however well
    /// they were scheduled. A settled 10M store has five live edge segments
    /// against `scan_threads()` = 16, so its select stage sat below the
    /// eight-unit gate and ran **serial** — 158 ms against the tiered
    /// layout's 81 ms on identical content, and fixing the ordering left one
    /// operator 21 % slower.
    ///
    /// The unit here is `(segment, row range)`. A segment's per-scan
    /// preparation — column handles, the rel-allow table, the window's binary
    /// search bounds, and the believed ranges (computed for the window's rows)
    /// — is computed **once** per segment and shared by every unit
    /// cut from it. Only the row loop fans out.
    ///
    /// Determinism: units are generated in `(segment index, range index)`
    /// order and the output is restored to that order, whatever order the
    /// workers claimed them in. Every `ScanStats` field is a sum, and the
    /// per-segment fields are accumulated where they are computed, exactly
    /// once each.
    pub fn select_units(&self, req: &ScanRequest) -> Result<(Vec<Selection>, ScanStats)> {
        self.select_units_inner(req, None, None)
    }

    /// `select_units` with the fan-out width and the unit size both forced —
    /// the hook every exactness test uses, so that a test can pin a width or
    /// force many tiny units per segment without mutating the process
    /// environment (which races the other tests). Same reason
    /// `aggregate_partials` and `materialize_edges_at` take theirs.
    pub fn select_units_at(
        &self,
        req: &ScanRequest,
        threads: usize,
        rows_per_unit: usize,
    ) -> Result<(Vec<Selection>, ScanStats)> {
        self.select_units_inner(req, Some(threads), Some(rows_per_unit.max(1)))
    }

    /// Stage P of the scan, on its own: every segment's [`SegPlan`] — the
    /// believed, window-clipped row ranges and the column handles the row
    /// loop would read — in segment order. Gated and fanned out exactly as
    /// [`ScanSet::select_units`] fans it out; it is a function of its
    /// own so the range-count aggregate can consume the ranges
    /// without ever building a selection vector.
    fn plan_all(
        &self,
        req: &ScanRequest,
        threads: usize,
        candidate_rows: u64,
    ) -> Result<(Vec<SegPlan<'a>>, ScanStats)> {
        Ok(if self.postings.is_none()
            && parallel_gate(threads, self.targets.len(), 8, candidate_rows)
        {
                let width = threads.min(self.targets.len());
                let mut order: Vec<usize> = (0..self.targets.len()).collect();
                order.sort_by(|&a, &b| {
                    self.targets[b]
                        .segment
                        .rows()
                        .cmp(&self.targets[a].segment.rows())
                        .then(a.cmp(&b))
                });
                let next = std::sync::atomic::AtomicUsize::new(0);
                let parts: Vec<Result<(Vec<SegPlan<'a>>, ScanStats)>> =
                    crate::pool::run(width, |_| {
                                    let t_worker = std::time::Instant::now();
                                    let mut ps = Vec::new();
                                    let mut st = ScanStats::default();
                                    loop {
                                        let k = next
                                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        let Some(&i) = order.get(k) else { break };
                                        let (p, one) = self.plan_range(req, i, i + 1)?;
                                        st.segments_total += one.segments_total;
                                        st.segments_pruned += one.segments_pruned;
                                        st.blocks_skipped += one.blocks_skipped;
                                        st.blocks_considered += one.blocks_considered;
                                        st.derived_blocks_skipped += one.derived_blocks_skipped;
                                        st.derived_blocks_considered += one.derived_blocks_considered;
                                        st.adj_rows_walked += one.adj_rows_walked;
                                        st.adj_segments += one.adj_segments;
                                        ps.extend(p);
                                    }
                                    crate::phase::add(
                                        crate::phase::Phase::SelPlanBusy,
                                        t_worker.elapsed().as_nanos() as u64,
                                    );
                                    Ok((ps, st))
                    });
                let mut plans: Vec<SegPlan<'a>> = Vec::new();
                let mut stats = ScanStats::default();
                for part in parts {
                    let (p, st) = part?;
                    plans.extend(p);
                    stats.segments_total += st.segments_total;
                    stats.segments_pruned += st.segments_pruned;
                    stats.blocks_skipped += st.blocks_skipped;
                    stats.blocks_considered += st.blocks_considered;
                    stats.derived_blocks_skipped += st.derived_blocks_skipped;
                    stats.derived_blocks_considered += st.derived_blocks_considered;
                    stats.adj_rows_walked += st.adj_rows_walked;
                    stats.adj_segments += st.adj_segments;
                }
                // segment order, whatever order the workers claimed them in
                plans.sort_by_key(|p| p.idx);
                (plans, stats)
            } else {
                let t_worker = std::time::Instant::now();
                let planned = self.plan_range(req, 0, self.targets.len())?;
                crate::phase::add(
                    crate::phase::Phase::SelPlanBusy,
                    t_worker.elapsed().as_nanos() as u64,
                );
                planned
            })
    }

    /// The believed, window-clipped row ranges of every planned segment:
    /// [`ScanSet::select_units`]' stage P, without stage R. A caller that
    /// can answer from range lengths — a count — never pays for the row loop
    /// or the selection vectors. Stats are the match plan's (no rows examined).
    pub(crate) fn plan_believed(&self, req: &ScanRequest) -> Result<(Vec<SegPlan<'a>>, ScanStats)> {
        let threads = scan_threads();
        let candidate_rows: u64 = self.targets.iter().map(|t| t.segment.rows() as u64).sum();
        self.plan_all(req, threads, candidate_rows)
    }

    fn select_units_inner(
        &self,
        req: &ScanRequest,
        force_threads: Option<usize>,
        force_rows_per_unit: Option<usize>,
    ) -> Result<(Vec<Selection>, ScanStats)> {
        // The stage's wall, stamped at each exit rather than by wrapping this
        // function in another. A wrapper was tried and measured: on the
        // serial path, where it changes nothing at all, it cost 18 % of the
        // stage — the same whole-binary codegen sensitivity the fold's
        // refactor showed. Two stamps, no new frame.
        let t_stage = std::time::Instant::now();
        let threads = force_threads.unwrap_or_else(scan_threads);
        let candidate_rows: u64 = self.targets.iter().map(|t| t.segment.rows() as u64).sum();

        // ---- stage P: per-segment preparation, once per segment ----------
        //
        // Cheap — `believed_ranges` + `subtract_closes` cost 0.004–0.031 ms
        // for a *whole* 29-segment query — but not free in the general case:
        // `believed_ranges` is O(runs), and a compacted segment carries one
        // run per generation boundary. Recomputing it per sub-unit would have
        // multiplied exactly the term already spent bounding a lane, so it is
        // computed here, once, and shared.
        //
        // Gated the same way the select stage already was, because the unit
        // being fanned out over here is still the segment.
        //
        // Except a scan with adjacency postings attached (an incidence
        // filter over a few nodes). Its per-segment plan is the same
        // preparation plus a few slice lookups, so `candidate_rows` (every
        // stored row) says nothing about its work, and spawning the match plan's
        // threads cost more than the plan: 0.6 ms of a 0.65 ms scan on the
        // 10M store, against 0.05 ms planned on this thread.
        let (mut plans, mut stats) = self.plan_all(req, threads, candidate_rows)?;

        // ---- cut the units -----------------------------------------------
        let examined: usize = plans.iter().map(|p| p.rows).sum();
        let per_unit = force_rows_per_unit.unwrap_or_else(|| rows_per_unit(examined, threads));
        let units = cut_units(&mut plans, per_unit);
        crate::phase::add(crate::phase::Phase::SelUnits, units.len() as u64);
        crate::phase::add(crate::phase::Phase::SelSegmentsPlanned, plans.len() as u64);

        // ---- stage R: the row loop, one unit at a time --------------------
        //
        // The gate is now purely "is there enough work", which is what the
        // unit count and the row count jointly say: `rows_per_unit` keeps the
        // unit count at `k × threads` whenever there are rows to divide, so
        // the eight-unit minimum is no longer a statement about how many
        // files the store keeps. Rows are the *examined* rows, known exactly
        // here — a sharper proxy than the candidate rows the earlier gate
        // read, which counted a whole segment for a window that touches ten
        // of its rows.
        let touching = req.touching_ids.as_ref().map(|ids| IdSet::new(ids));
        let parallel = match force_threads {
            Some(w) => w > 1 && units.len() > 1,
            None => parallel_gate_at(
                threads,
                units.len(),
                8,
                examined as u64,
                self.row_loop_min_rows.unwrap_or_else(parallel_min_rows),
            ),
        };
        let mut out: Vec<(usize, Selection)> = Vec::with_capacity(units.len());
        if parallel {
            let width = threads.min(units.len());
            let mut order: Vec<usize> = (0..units.len()).collect();
            // Largest first, ties by unit index. Units are near-equal by
            // construction, so this mostly costs a sort of 64 integers — but
            // a store whose segments are all under the floor still produces
            // one unit each, and on *that* mix this ordering rule is the
            // whole schedule. Keep it.
            order.sort_by(|&a, &b| units[b].rows.cmp(&units[a].rows).then(a.cmp(&b)));
            let next = std::sync::atomic::AtomicUsize::new(0);
            let parts: Vec<Result<UnitPart>> =
                crate::pool::run(width, |_| {
                                let t_worker = std::time::Instant::now();
                                let mut sels = Vec::new();
                                let mut st = ScanStats::default();
                                loop {
                                    let k =
                                        next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    let Some(&u) = order.get(k) else { break };
                                    let unit = &units[u];
                                    let p = &plans[unit.plan];
                                    let (rows, examined) = self.select_rows(
                                        req,
                                        p,
                                        &p.ranges[unit.lo..unit.hi],
                                        &touching,
                                    );
                                    st.rows_examined += examined;
                                    if !rows.is_empty() {
                                        st.rows_selected += rows.len() as u64;
                                        sels.push((u, Selection { segment: p.idx, rows }));
                                    }
                                }
                                crate::phase::worker(
                                    crate::phase::Phase::SelBusy,
                                    crate::phase::Phase::SelMaxBusy,
                                    t_worker,
                                );
                                crate::phase::add(crate::phase::Phase::SelWorkers, 1);
                                Ok((sels, st))
                });
            for part in parts {
                let (sels, st) = part?;
                out.extend(sels);
                stats.rows_examined += st.rows_examined;
                stats.rows_selected += st.rows_selected;
            }
            // unit order — (segment, row range) — whatever order the workers
            // claimed them in
            out.sort_by_key(|(u, _)| *u);
        } else {
            let t_worker = std::time::Instant::now();
            for (u, unit) in units.iter().enumerate() {
                let p = &plans[unit.plan];
                let (rows, examined) =
                    self.select_rows(req, p, &p.ranges[unit.lo..unit.hi], &touching);
                stats.rows_examined += examined;
                if !rows.is_empty() {
                    stats.rows_selected += rows.len() as u64;
                    out.push((u, Selection { segment: p.idx, rows }));
                }
            }
            crate::phase::worker(
                crate::phase::Phase::SelBusy,
                crate::phase::Phase::SelMaxBusy,
                t_worker,
            );
            crate::phase::add(crate::phase::Phase::SelWorkers, 1);
        }
        crate::phase::add(crate::phase::Phase::SelRowsExamined, stats.rows_examined);
        crate::phase::since(crate::phase::Phase::SelWall, t_stage);
        Ok((out.into_iter().map(|(_, s)| s).collect(), stats))
    }

    /// Stage P over one range of targets: everything a scan resolves per
    /// segment, and nothing per row.
    ///
    /// This is the earlier `select_range_inner` with its row loop lifted out
    /// and its verdict ranges kept instead of consumed. The loop body is
    /// otherwise unchanged, in the order it was in — including that
    /// `blocks_considered` is counted before the `lo >= hi` bail and the
    /// column handles after it, because `ScanStats` is compared bit for bit
    /// against the earlier engine on every store in the record.
    fn plan_range(
        &self,
        req: &ScanRequest,
        lo_idx: usize,
        hi_idx: usize,
    ) -> Result<(Vec<SegPlan<'a>>, ScanStats)> {
        let mut stats = ScanStats {
            // this range's share; the parallel caller sums ranges
            segments_total: hi_idx - lo_idx,
            ..Default::default()
        };
        let mut out: Vec<SegPlan<'a>> = Vec::new();
        let as_of = clamp_tt(req.as_of_tt);
        let has_touching = req.touching_ids.is_some();
        // the handle's design, read at open (`GNOMON_NO_STAIRCASE=1`)
        let staircase_on = self.designs.staircase;

        for (idx, target) in self.targets.iter().enumerate().take(hi_idx).skip(lo_idx) {
            let seg: &'a Segment<S> = target.segment;
            // `GNOMON_SEGMENT_PRUNE=0` plans every segment instead
            if self.designs.segment_prune && self.prunes(seg, req) {
                stats.segments_pruned += 1;
                crate::vistouch::pruned(1);
                continue;
            }
            let t_cols = std::time::Instant::now();
            let h = seg.header();
            // The window cut is the whole of what the planner needs `vt_s`
            // for -- `SegPlan` never carries it -- so a segment whose `vt_s`
            // is codec 2 answers both bounds from the model's own sparse
            // index and never materializes the column. On a windowed sweep
            // that is 8 bytes a row the segment cache does not hold (decoded
            // columns measured as the dominant memory term at 1B rows).
            // Where the column is not codec 2, or is not sorted, this
            // is the same `partition_point` over the same slice as before.
            let vt_s = VtS::open(seg)?;

            // vt_s is sorted, so `vt_s < vt_max` is a suffix cut for free.
            let hi = match req.vt_max {
                Some(t) => vt_s.partition_point_lt(t),
                None => vt_s.len(),
            };
            // When vt_e is elided every row is instantaneous, so
            // `vt_e > vt_min` collapses to `vt_s >= vt_min` — a prefix cut,
            // valid in either lane. Otherwise, an interval-lane segment that
            // carries a per-block `vt_e` running maximum can still cut a
            // monotone prefix: the vector is non-decreasing, so the first
            // block whose entry exceeds `vt_min` bounds every row before it
            // as `vt_e <= vt_min` — those rows cannot overlap `[vt_min, ..)`.
            // Rows at or
            // after the cut still get the per-row `vt_e` test below; the
            // structure only bounds, it never decides. Absent it (event
            // lane, or an interval-lane segment written before this field
            // existed) the per-row test alone decides, exactly as before.
            //
            // The maximum is over **closed rows only**, so the cut is
            // a statement about closed rows and the open-ended rows below it
            // are added back by `open_rows` after the belief ranges are
            // known. Before that split, a running maximum being a prefix
            // aggregate meant one open-ended row anywhere ended the
            // structure's usefulness for the whole rest of the segment.
            //
            // The structure is the header's on an interval-lane
            // segment and the one derived at open on an event-lane segment
            // that stores `vt_e` -- the same running maximum and open-row
            // ranges either way ([`Segment::vt_e_staircase`]); the planner
            // was already lane-agnostic, it only lacked the structure.
            let stair = if staircase_on { seg.vt_e_staircase() } else { None };
            if let Some(st) = &stair {
                if st.derived {
                    stats.derived_blocks_considered += st.running_max.len() as u64;
                } else {
                    stats.blocks_considered += st.running_max.len() as u64;
                }
            }
            let mut lo = match (req.vt_min, &stair) {
                (Some(t), _) if h.vt_e_elided => vt_s.partition_point_lt(t),
                (Some(t), Some(st)) => {
                    let b = st.running_max.partition_point(|&m| m <= t);
                    if st.derived {
                        stats.derived_blocks_skipped += b as u64;
                    } else {
                        stats.blocks_skipped += b as u64;
                    }
                    b.saturating_mul(st.block_rows).min(vt_s.len())
                }
                _ => 0,
            };
            // Open-ended rows below the cut are not pruned by it: their
            // `vt_e` is the sentinel, so they overlap every window their
            // `vt_s` reaches, and the cut never spoke for them.
            let mut open_below: &[(u32, u32)] = if lo > 0 {
                open_ranges_below(stair.as_ref().map_or(&h.open_rows[..], |st| st.open_rows), lo)
            } else {
                &[]
            };
            // An event request starts at its first `vt_s >= vt_s_min`: below
            // it nothing is wanted, open-ended or not. Open-ended rows at or
            // after that point but below the staircase cut are kept, clipped.
            let clipped_open: Vec<(u32, u32)>;
            if let Some(t) = req.vt_s_min {
                let p = vt_s.partition_point_lt(t);
                if p >= lo {
                    lo = p;
                    open_below = &[];
                } else {
                    clipped_open = open_below
                        .iter()
                        .filter_map(|&(s, e)| {
                            let s = s.max(p as u32);
                            (s < e).then_some((s, e))
                        })
                        .collect();
                    open_below = &clipped_open;
                }
            }
            if lo >= hi && open_below.is_empty() {
                continue;
            }

            let vt_e_col = if h.vt_e_elided {
                None
            } else {
                Some(seg.i64_column("vt_e")?)
            };
            let rel_codes = seg.u16_column("rel_code").ok();
            let src = seg.u32_column("src_id").ok();
            let dst = seg.u32_column("dst_id").ok();
            // The or-form can still answer with one endpoint column missing;
            // the and-form cannot, so say so rather than silently matching
            // nothing.
            if req.touching_both && has_touching && (src.is_none() || dst.is_none()) {
                return Err(crate::EngineError::corrupt(
                    "both-endpoint incidence needs src_id and dst_id",
                ));
            }
            // direct lookup by rel_code instead of searching a list per row
            let rel_allowed: Option<Vec<bool>> = req
                .rel_types
                .as_ref()
                .map(|names| h.rel_types.iter().map(|r| names.contains(r)).collect());

            // The rows this request can use, known before any belief
            // work: the window's `[lo, hi)` plus the open-ended rows below
            // `lo` that the cut does not speak for (clipped exactly as the
            // loop below clips them). When that domain is a small part of
            // the segment the belief state is computed for it alone
            // (`window_first`); otherwise for the whole segment and clipped,
            // which is the whole-segment path, instruction for instruction.
            //
            // visibility: a segment nothing has corrected skips this work
            // entirely, which for event-stream data is nearly all of them.
            // Resolved once per segment into one sorted list, and then
            // subtracted from the verdict ranges rather than tested per row
            // — a single close used to set `needs_row_test` and so convert
            // the whole segment's scan from a range extend into a row loop.
            let n = vt_s.len();
            // An incidence filter's postings, taken before the belief
            // work rather than after it. A touching read of one vertex keeps
            // a handful of rows, but its belief ranges were computed over the
            // whole request domain first -- every close of every segment, and
            // on long-lived data the open-ended rows below the cut exceed
            // `WINDOW_FIRST_MAX_RANGES`, so over the whole segment: 3-7 ms a
            // read on FinBench SF1's 718k closes, against 0.1 ms on K10M.
            // With the postings in hand the belief domain is their rows inside
            // the request domain. Belief is decided per row, so the believed
            // rows are exactly those the old path kept after intersecting its
            // ranges with the same postings. The budget is the request
            // domain's size, at least the believed rows the later stage would
            // have offered, so a refusal here is a refusal there too.
            // Only on the shipped belief path: with `GNOMON_WINDOW_FIRST=0`,
            // `GNOMON_RANGE_VISIBILITY=0` or `GNOMON_ADJ_FILTERED_SCAN=0` (no
            // postings) the planner runs exactly its earlier code, so those
            // ablation configurations measure what they measured when signed.
            //
            // The belief work then runs once per posting range, so postings
            // first pays only where they are a few ranges
            // (`POSTINGS_FIRST_MAX_RANGES`): an incidence filter over many
            // nodes (count_temporal_motifs' node filter: ~20k posting rows a
            // call on K10M-d10) is cheaper the old way, one pass over the
            // domain. Then the postings already read are kept, and the later
            // stage applies the old decision to them at the old budget
            // (`walked < rows`), which is what `postings(.., rows)` decides:
            // the old plan, the old work and the old counters.
            let mut posted: Option<(crate::scan::Postings, Vec<(usize, usize)>)> = None;
            let mut deferred: Option<crate::scan::Postings> = None;
            let mut postings_refused = false;
            if self.designs.window_first && self.designs.range_visibility {
                if let (Some(src), Some(ids)) = (self.postings, req.touching_ids.as_deref()) {
                    let dom = request_domain(open_below, lo, hi);
                    let cand: usize = dom.iter().map(|(a, b)| b - a).sum();
                    match src.postings(idx, ids, req.touching_both, cand)? {
                        // more posting rows than a few ranges' worth: the old
                        // order, without building the posting domain to find out
                        Some(p) if p.rows.len() > POSTINGS_FIRST_MAX_ROWS => deferred = Some(p),
                        Some(p) => {
                            let posting_domain = intersect_rows(&p.rows, &dom).0;
                            if posting_domain.len() <= POSTINGS_FIRST_MAX_RANGES {
                                stats.adj_rows_walked += p.walked;
                                stats.adj_segments += 1;
                                crate::phase::add(crate::phase::Phase::SelAdjRows, p.walked);
                                crate::phase::add(crate::phase::Phase::SelAdjSegments, 1);
                                posted = Some((p, posting_domain));
                            } else {
                                deferred = Some(p);
                            }
                        }
                        None => postings_refused = true,
                    }
                }
            }
            // a whole-extent request never builds the domain at all
            // `GNOMON_WINDOW_FIRST=0` never builds the domain either: the
            // whole-segment path
            let domain: Option<Vec<(usize, usize)>> = if let Some((_, d)) = &posted {
                Some(d.clone())
            } else if !self.designs.window_first
                || (open_below.is_empty() && is_whole(lo, hi, n))
            {
                None
            } else {
                let d = request_domain(open_below, lo, hi);
                // the per-row test always works on the request's rows
                (!self.designs.range_visibility || window_first(&d, n)).then_some(d)
            };
            let view = if domain.is_some() || !self.designs.range_visibility {
                crate::visibility::CloseView::None
            } else {
                crate::visibility::close_view(seg.sidecar(), &self.closes, target.id)
            };

            let needs_vt_e_test = req.vt_min.is_some() && vt_e_col.is_some();
            let needs_row_test = needs_vt_e_test || rel_allowed.is_some() || has_touching;
            crate::phase::since(crate::phase::Phase::SelCols, t_cols);

            // The belief half of the predicate, timed on its own. This was
            // previously confounded with segment count, because `settle`
            // folds closes and merges segments in one operation. Timing it
            // separates the two without building a third store.
            //
            // Because it is per segment, it is computed here rather than
            // inside a unit, and every row-range unit cut from this segment
            // reads the same answer.
            //
            // It is *not* window-independent work. Computed over the
            // whole segment it cost every close and every straddling code
            // block the segment carries, whatever the window; restricted to
            // the request's domain it costs what the domain holds.
            let t_closes = std::time::Instant::now();
            let tt = seg.tt_s_view()?;
            let (clipped, rows) = if let (Some(domain), false) =
                (&domain, self.designs.range_visibility)
            {
                // `GNOMON_RANGE_VISIBILITY=0`: every row of the request's
                // domain tested on its own; the output is the same clipped,
                // coalesced list the range path builds
                let clipped = crate::visibility::believed_ranges_per_row(
                    &tt,
                    seg.sidecar(),
                    &self.closes,
                    target.id,
                    as_of,
                    domain,
                    n,
                    crate::visibility::RowBelief::Current,
                )?;
                crate::phase::since(crate::phase::Phase::SelCloses, t_closes);
                let rows = clipped.iter().map(|(a, b)| b - a).sum();
                (clipped, rows)
            } else if let Some(domain) = &domain {
                let mut clipped: Vec<(usize, usize)> = Vec::new();
                let mut rows = 0usize;
                for &(a, b) in domain {
                    let view = crate::visibility::close_view_in(
                        seg.sidecar(),
                        &self.closes,
                        target.id,
                        a,
                        b,
                    );
                    for (rs, re) in
                        believed_ranges_closed_in(&tt, seg.tt_s_bounds(), as_of, n, a, b, &view)
                    {
                        // ascending and disjoint across the domain's ranges;
                        // two of them can abut (an open-ended range ending at
                        // `lo`), which is where the coalescing matters
                        rows += re - rs;
                        match clipped.last_mut() {
                            Some(last) if last.1 == rs => last.1 = re,
                            _ => clipped.push((rs, re)),
                        }
                    }
                }
                crate::phase::since(crate::phase::Phase::SelCloses, t_closes);
                (clipped, rows)
            } else {
                let ranges = if self.designs.range_visibility {
                    believed_ranges_closed(&tt, seg.tt_s_bounds(), as_of, n, &view)
                } else {
                    // a whole-extent request, or `GNOMON_WINDOW_FIRST=0`:
                    // every row of the segment, then the same clip
                    crate::visibility::believed_ranges_per_row(
                        &tt,
                        seg.sidecar(),
                        &self.closes,
                        target.id,
                        as_of,
                        &[(0, n)],
                        n,
                        crate::visibility::RowBelief::Current,
                    )?
                };
                crate::phase::since(crate::phase::Phase::SelCloses, t_closes);

                // The window cut is applied here, once, so a unit's ranges
                // need no clipping and the unit planner divides *examined*
                // rows rather than stored ones.
                let mut clipped: Vec<(usize, usize)> =
                    Vec::with_capacity(ranges.len() + open_below.len());
                let mut rows = 0usize;
                let mut push = |a: usize, b: usize, clipped: &mut Vec<(usize, usize)>| {
                    if a >= b {
                        return;
                    }
                    rows += b - a;
                    match clipped.last_mut() {
                        Some(last) if last.1 == a => last.1 = b,
                        _ => clipped.push((a, b)),
                    }
                };
                // both lists are ascending and disjoint: one merge walk, the
                // open-ended runs that end at or before a range's start never
                // meet a later range (ranges ascend), and a run that starts at
                // or after the clip top meets nothing in this range
                let mut first_open = 0usize;
                for (rs, re) in ranges {
                    // the open-ended rows this range holds *below* the cut,
                    // which the cut does not speak for -- emitted first so
                    // `clipped` stays ascending and disjoint, which is the
                    // contract `select_range` consumes
                    let top = re.min(lo).min(hi);
                    while first_open < open_below.len() && open_below[first_open].1 as usize <= rs {
                        first_open += 1;
                    }
                    for &(os, oe) in &open_below[first_open..] {
                        if os as usize >= top {
                            break;
                        }
                        let (a, b) = ((os as usize).max(rs), (oe as usize).min(top));
                        push(a, b, &mut clipped);
                    }
                    let (a, b) = (rs.max(lo), re.min(hi));
                    if a >= b {
                        continue;
                    }
                    push(a, b, &mut clipped);
                }
                (clipped, rows)
            };
            if rows == 0 {
                continue;
            }
            // An incidence filter over a few nodes. The rows the
            // filter can match are the filter's adjacency postings; where
            // they are fewer than the `rows` above, the plan keeps only the
            // posting rows inside those ranges, so the row test below sees
            // them and nothing else. Every test still runs on every row it
            // sees (valid time, relation, incidence): the postings only
            // decide which rows are looked at, never which are selected.
            let (clipped, rows) = if let Some((p, _)) = &posted {
                // Taken (and counted) above; the ranges already lie
                // inside the posting rows, which the intersection keeps
                intersect_rows(&p.rows, &clipped)
            } else if postings_refused {
                // refused above at a larger budget: refused here as well
                (clipped, rows)
            } else if let Some(p) = deferred.take() {
                // read above, used as the old stage would: `postings(.., rows)`
                // refuses exactly when the walk reaches `rows`
                if (p.walked as usize) < rows {
                    stats.adj_rows_walked += p.walked;
                    stats.adj_segments += 1;
                    crate::phase::add(crate::phase::Phase::SelAdjRows, p.walked);
                    crate::phase::add(crate::phase::Phase::SelAdjSegments, 1);
                    intersect_rows(&p.rows, &clipped)
                } else {
                    (clipped, rows)
                }
            } else {
                match (self.postings, req.touching_ids.as_deref()) {
                    (Some(src), Some(ids)) => {
                        match src.postings(idx, ids, req.touching_both, rows)? {
                            Some(p) => {
                                stats.adj_rows_walked += p.walked;
                                stats.adj_segments += 1;
                                crate::phase::add(crate::phase::Phase::SelAdjRows, p.walked);
                                crate::phase::add(crate::phase::Phase::SelAdjSegments, 1);
                                intersect_rows(&p.rows, &clipped)
                            }
                            None => (clipped, rows),
                        }
                    }
                    _ => (clipped, rows),
                }
            };
            if rows == 0 {
                continue;
            }
            out.push(SegPlan {
                idx,
                vt_e: vt_e_col,
                rel_codes,
                src,
                dst,
                rel_allowed,
                needs_row_test,
                ranges: clipped,
                rows,
            });
        }
        Ok((out, stats))
    }

    /// The rows of plan `p` incident to `ids` (sorted) -- whose
    /// `src_id`, and unless `src_only` whose `dst_id`, is among them -- as
    /// ascending row ranges inside the plan's believed, window-clipped
    /// ranges, read from the adjacency postings. `None` when this set
    /// carries no postings or the segment has none. No row test is applied:
    /// the caller decides incidence and the time test per row.
    pub(crate) fn plan_incident_rows(
        &self,
        p: &SegPlan<'_>,
        ids: &[u32],
        src_only: bool,
    ) -> Result<Option<Vec<(usize, usize)>>> {
        let Some(src) = self.postings else { return Ok(None) };
        Ok(src
            .postings(p.idx, ids, src_only, usize::MAX)?
            .map(|got| intersect_rows(&got.rows, &p.ranges).0))
    }

    /// Stage R: one unit's rows. The predicate chain and the
    /// contiguous-extend fast path are exactly the earlier ones — the only
    /// change is that the ranges arrive already clipped and already cut at
    /// the unit boundary, so this loop does no `max`/`min` of its own.
    fn select_rows(
        &self,
        req: &ScanRequest,
        p: &SegPlan<'_>,
        ranges: &[(usize, usize)],
        touching: &Option<IdSet>,
    ) -> (Vec<u32>, u64) {
        let t_rows = std::time::Instant::now();
        let mut rows: Vec<u32> = Vec::new();
        let mut examined = 0u64;
        if !p.needs_row_test {
            // One reservation for the whole unit rather than one per range.
            // An uncorrected segment has a single range and the two are the
            // same call; a corrected one has a range per run of believed rows
            // between closes, and paying `reserve` per range there measured
            // as most of the residual.
            rows.reserve(ranges.iter().map(|(a, b)| b - a).sum());
        }
        for &(a, b) in ranges {
            examined += (b - a) as u64;
            if !p.needs_row_test {
                // the binary-search bounds already decided every row here
                rows.extend(a as u32..b as u32);
                continue;
            }
            for i in a..b {
                if let (Some(vt_e), Some(vt_min)) = (p.vt_e, req.vt_min) {
                    if vt_e[i] <= vt_min {
                        continue;
                    }
                }
                if let (Some(codes), Some(allowed)) = (p.rel_codes, p.rel_allowed.as_ref()) {
                    if !allowed.get(codes[i] as usize).copied().unwrap_or(false) {
                        continue;
                    }
                }
                if let Some(ids) = touching {
                    let s_hit = p.src.is_some_and(|s| ids.contains(s[i]));
                    let d_hit = p.dst.is_some_and(|d| ids.contains(d[i]));
                    let hit = if req.touching_both {
                        s_hit && d_hit
                    } else {
                        s_hit || d_hit
                    };
                    if !hit {
                        continue;
                    }
                }
                rows.push(i as u32);
            }
        }
        crate::phase::since(crate::phase::Phase::SelRows, t_rows);
        (rows, examined)
    }

    /// Merge selections into one globally `(vt_s, vid)`-ordered stream.
    ///
    /// Only callers that need global order pay for this; a kernel that works
    /// segment-at-a-time consumes `select` directly.
    /// Group selections into clusters of overlapping composite-key ranges,
    /// in key order.
    ///
    /// Everything in cluster *i* sorts strictly before everything in cluster
    /// *i+1*, so clusters concatenate; only rows *within* a cluster ever need
    /// the heap merge. The earlier version of this check was all-or-nothing
    /// disjointness — which a single correction voided, because a superseding
    /// version lands in a segment overlapping the original's range. On a
    /// corrected store (the normal store) the fast path therefore never ran.
    /// Clusters make it the common case again: correction segments are tiny,
    /// so they form small local clusters while the bulk concatenates.
    fn cluster_order(&self, selections: &[Selection]) -> Result<Vec<Vec<usize>>> {
        let mut keys = Vec::with_capacity(selections.len());
        for sel in selections {
            if sel.rows.is_empty() {
                keys.push(None);
                continue;
            }
            let seg = self.targets[sel.segment].segment;
            let vt_s = seg.i64_column("vt_s")?;
            let hi = seg.u64_column("vid64")?;
            let lo = seg.u32_column("vid_lo32")?;
            let k = |row: u32| -> (i64, Id96) {
                let r = row as usize;
                (vt_s[r], Id96 { hi: hi[r], lo: lo[r] })
            };
            let first = *sel.rows.first().expect("non-empty");
            let last = *sel.rows.last().expect("non-empty");
            keys.push(Some((k(first), k(last))));
        }
        let mut order: Vec<usize> = (0..selections.len())
            .filter(|&si| keys[si].is_some())
            .collect();
        order.sort_by_key(|&si| keys[si].expect("filtered").0);

        let mut clusters: Vec<Vec<usize>> = Vec::new();
        let mut cur_max_last: Option<(i64, Id96)> = None;
        for si in order {
            let (first, last) = keys[si].expect("filtered");
            match (&mut clusters.last_mut(), cur_max_last) {
                (Some(cluster), Some(max_last)) if first < max_last => {
                    // overlaps the running cluster: keys are unique, so
                    // first < max_last is the exact overlap condition
                    cluster.push(si);
                    cur_max_last = Some(max_last.max(last));
                }
                _ => {
                    clusters.push(vec![si]);
                    cur_max_last = Some(last);
                }
            }
        }
        Ok(clusters)
    }

    pub fn merged(
        &self,
        selections: &[Selection],
        limit: Option<usize>,
    ) -> Result<Vec<(usize, u32)>> {
        // Hoisted per selection: the vt_s slice and both vid halves. The
        // first version resolved the vt_s column *by name* and built an Id96
        // for every row popped from the heap — a known defect, recurring
        // here at 10M rows a call.
        struct Cols<'a> {
            vt_s: &'a [i64],
            hi: &'a [u64],
            lo: &'a [u32],
        }
        let mut cols = Vec::with_capacity(selections.len());
        for sel in selections {
            let seg = self.targets[sel.segment].segment;
            cols.push(Cols {
                vt_s: seg.i64_column("vt_s")?,
                hi: seg.u64_column("vid64")?,
                lo: seg.u32_column("vid_lo32")?,
            });
        }
        let key = |si: usize, row: u32| -> (i64, Id96) {
            let c = &cols[si];
            let r = row as usize;
            (c.vt_s[r], Id96 { hi: c.hi[r], lo: c.lo[r] })
        };
        let cap = limit.unwrap_or(usize::MAX);

        // Cluster-wise: concatenate across clusters, heap-merge only within.
        // A pristine store is all singleton clusters (pure concatenation); a
        // pathological one is a single cluster (the old full heap merge); a
        // corrected store is almost-all singletons plus small local clusters.
        let mut out = Vec::new();
        'clusters: for cluster in self.cluster_order(selections)? {
            if let [si] = cluster[..] {
                let sel = &selections[si];
                for &row in &sel.rows {
                    out.push((sel.segment, row));
                    if out.len() >= cap {
                        break 'clusters;
                    }
                }
                continue;
            }
            let mut heap: BinaryHeap<Reverse<(i64, Id96, usize, usize)>> = BinaryHeap::new();
            for &si in &cluster {
                let row = selections[si].rows[0];
                let (t, vid) = key(si, row);
                heap.push(Reverse((t, vid, si, 0)));
            }
            while let Some(Reverse((_, _, si, pos))) = heap.pop() {
                let sel = &selections[si];
                out.push((sel.segment, sel.rows[pos]));
                if out.len() >= cap {
                    break 'clusters;
                }
                if let Some(&next) = sel.rows.get(pos + 1) {
                    let (t, vid) = key(si, next);
                    heap.push(Reverse((t, vid, si, pos + 1)));
                }
            }
        }
        Ok(out)
    }

    /// One selection's rows, as columns. The run loop mirrors the merged
    /// path: contiguous stretches memcpy, everything else pushes.
    fn materialize_selection(&self, req: &ScanRequest, sel: &Selection) -> Result<EdgeColumns> {
        let w = Want::of(req);
        let v = SegmentView::open(self.targets[sel.segment].segment)?;
        let tid = self.targets[sel.segment].id;
        let mut cols = EdgeColumns::with_capacity(sel.rows.len(), w);
        let mut i = 0usize;
        while i < sel.rows.len() {
            let mut j = i + 1;
            while j < sel.rows.len() && sel.rows[j] == sel.rows[j - 1] + 1 {
                j += 1;
            }
            let (a, b) = (sel.rows[i] as usize, sel.rows[i] as usize + (j - i));
            copy_run(&v, a, b, &mut cols, w)?;
            if w.addr {
                cols.seg_id.extend(std::iter::repeat_n(tid, b - a));
                cols.seg_row.extend(a as u32..b as u32);
            }
            i = j;
        }
        Ok(cols)
    }

    /// One cluster's rows, as columns: the direct run-walk for a singleton,
    /// a within-cluster heap merge otherwise.
    fn materialize_cluster(
        &self,
        req: &ScanRequest,
        selections: &[Selection],
        cluster: &[usize],
        probe: &StageProbe,
    ) -> Result<EdgeColumns> {
        if let [si] = cluster[..] {
            let t = std::time::Instant::now();
            let out = self.materialize_selection(req, &selections[si]);
            probe.copy(t);
            return out;
        }
        let t = std::time::Instant::now();
        let members: Vec<Selection> = cluster
            .iter()
            .map(|&si| selections[si].clone())
            .collect();
        let order = self.merged(&members, None)?;
        probe.order(t);
        let t = std::time::Instant::now();
        let out = self.materialize_order(req, &order);
        probe.copy(t);
        out
    }

    pub fn materialize_edges(&self, req: &ScanRequest) -> Result<(EdgeColumns, ScanStats)> {
        let (cols, stats, _) = self.materialize_edges_timed(req)?;
        Ok((cols, stats))
    }

    /// `materialize_edges`, with per-stage wall times.
    pub fn materialize_edges_timed(
        &self,
        req: &ScanRequest,
    ) -> Result<(EdgeColumns, ScanStats, ScanTimings)> {
        self.materialize_at(req, None)
    }

    /// `materialize_edges_timed`, with the fan-out width forced.
    ///
    /// `Some(w)` runs `w` workers whatever the gates would have said, so a
    /// test can pin every width against the serial path without mutating the
    /// process environment (which races other tests) — the same reason
    /// `aggregate_partials` takes its width explicitly.
    pub fn materialize_edges_at(
        &self,
        req: &ScanRequest,
        threads: usize,
    ) -> Result<(EdgeColumns, ScanStats, ScanTimings)> {
        self.materialize_at(req, Some(threads))
    }

    fn materialize_at(
        &self,
        req: &ScanRequest,
        force: Option<usize>,
    ) -> Result<(EdgeColumns, ScanStats, ScanTimings)> {
        let mut timings = ScanTimings::default();
        let t_select = std::time::Instant::now();
        let (selections, stats) = self.select(req)?;
        timings.select_ns = t_select.elapsed().as_nanos() as u64;
        // Clusters materialize independently — on threads, into their own
        // columns, appended in key order. A singleton cluster (the corrected-
        // store norm: bulk segments plus tiny local correction clusters) runs
        // the direct run-walk with no order list; a multi-member cluster
        // heap-merges its own rows only. Byte-identical to the merged path
        // because cluster boundaries are exact key-order boundaries.
        // Unlimited scans only: a limit reintroduces cross-cluster coupling
        // that the merged path already handles.
        if req.limit.is_none() {
            let t_cluster = std::time::Instant::now();
            let clusters = self.cluster_order(&selections)?;
            timings.cluster_ns = t_cluster.elapsed().as_nanos() as u64;
            let t_mat = std::time::Instant::now();
            timings.clusters = clusters.len() as u32;
            timings.clusters_multi = clusters.iter().filter(|c| c.len() > 1).count() as u32;
            let probe = StageProbe::default();
            // Same recalibrated gates as select(), but on *selected* rows —
            // known exactly here, and the honest proxy for materialization
            // work.
            //
            // The fan-out used to be one thread per cluster, which on a 10M
            // store meant 371 threads on 40 cores, each allocating its own
            // column buffers: the same copy cost 2.5× more CPU parallel than
            // serial and the stage scaled 1.41×. Workers are now the
            // configured width, over contiguous cluster *ranges* balanced by
            // row count — contiguous so the parts still concatenate in key
            // order, balanced because 370 singletons plus one 89-member
            // cluster is not an even split of anything.
            let threads = force.unwrap_or_else(scan_threads);
            let parallel = match force {
                Some(w) => w > 1 && clusters.len() > 1,
                None => parallel_gate(threads, clusters.len(), 4, stats.rows_selected),
            };
            let ranges = if parallel {
                balance_clusters(&clusters, &selections, threads)
            } else {
                whole(clusters.len())
            };
            let run = |range: &std::ops::Range<usize>,
                       sels: &[Selection],
                       probe: &StageProbe| {
                let mut part = EdgeColumns::with_capacity(
                    clusters[range.clone()]
                        .iter()
                        .flat_map(|c| c.iter())
                        .map(|&si| sels[si].rows.len())
                        .sum(),
                    Want::of(req),
                );
                for cluster in &clusters[range.clone()] {
                    part.append(self.materialize_cluster(req, sels, cluster, probe)?);
                }
                Ok(part)
            };
            let parts: Vec<Result<EdgeColumns>> = if parallel {
                crate::pool::run(ranges.len(), |i| run(&ranges[i], &selections, &probe))
            } else {
                ranges.iter().map(|r| run(r, &selections, &probe)).collect()
            };
            let mut ok = Vec::with_capacity(parts.len());
            for part in parts {
                ok.push(part?);
            }
            // Concatenating the parts used to be a serial pass over every
            // column of every cluster — 145 of the 322 ms serial
            // materialization spent outside both of its halves. The parts are
            // already in key order, so the destination offsets are known and
            // each part copies into its own disjoint slice.
            let cols = concat_parts(ok, stats.rows_selected as usize, Want::of(req), parallel);
            timings.materialize_ns = t_mat.elapsed().as_nanos() as u64;
            timings.order_ns = probe.order_ns.load(std::sync::atomic::Ordering::Relaxed);
            timings.copy_ns = probe.copy_ns.load(std::sync::atomic::Ordering::Relaxed);
            return Ok((cols, stats, timings));
        }
        let t_cluster = std::time::Instant::now();
        let order = self.merged(&selections, req.limit)?;
        timings.cluster_ns = t_cluster.elapsed().as_nanos() as u64;
        let t_mat = std::time::Instant::now();
        let cols = self.materialize_order(req, &order)?;
        timings.materialize_ns = t_mat.elapsed().as_nanos() as u64;
        Ok((cols, stats, timings))
    }

    /// Materialize an explicit (segment, row) order list — the general path
    /// shared by limited scans and multi-member clusters.
    fn materialize_order(
        &self,
        req: &ScanRequest,
        order: &[(usize, u32)],
    ) -> Result<EdgeColumns> {
        let w = Want::of(req);
        let mut cols = EdgeColumns::with_capacity(order.len(), w);
        // resolve each segment's columns once, then walk its rows
        let mut views: Vec<Option<SegmentView<'_>>> =
            (0..self.targets.len()).map(|_| None).collect();

        // Copy contiguous runs rather than pushing row by row. A selection is
        // usually one ascending stretch of one segment, and for the fixed-width
        // columns that turns a bounds-checked push per row into a memcpy —
        // which was the whole gap against an Arrow-backed reader at 1M rows.
        let mut i = 0usize;
        while i < order.len() {
            let (seg_idx, first_row) = order[i];
            let mut j = i + 1;
            while j < order.len()
                && order[j].0 == seg_idx
                && order[j].1 == order[j - 1].1 + 1
            {
                j += 1;
            }
            if views[seg_idx].is_none() {
                views[seg_idx] = Some(SegmentView::open(self.targets[seg_idx].segment)?);
            }
            let v = views[seg_idx].as_ref().expect("just populated");
            let (a, b) = (first_row as usize, first_row as usize + (j - i));
            copy_run(v, a, b, &mut cols, w)?;
            if w.addr {
                let tid = self.targets[seg_idx].id;
                cols.seg_id.extend(std::iter::repeat_n(tid, b - a));
                cols.seg_row.extend(a as u32..b as u32);
            }
            i = j;
        }
        Ok(cols)
    }
}

/// One segment's per-scan preparation, shared by every row-range unit cut
/// from it.
///
/// Everything here is resolved once per segment per scan: the column handles,
/// the rel-allow table, and the verdict ranges — per segment, so recomputing
/// them per unit would have multiplied the one term two lanes spent
/// bounding. `ranges` is already cut to the window's `[lo, hi)` (it is
/// *computed* for those rows rather than cut afterwards), so a unit is
/// a contiguous slice of it and the row loop clips nothing.
pub(crate) struct SegPlan<'a> {
    /// index into `ScanSet::targets` — the `Selection::segment` this yields
    pub(crate) idx: usize,
    pub(crate) vt_e: Option<&'a [i64]>,
    pub(crate) rel_codes: Option<&'a [u16]>,
    src: Option<&'a [u32]>,
    dst: Option<&'a [u32]>,
    pub(crate) rel_allowed: Option<Vec<bool>>,
    needs_row_test: bool,
    /// ascending, disjoint, window-clipped, and (after `cut_units`) split so
    /// that no range straddles a unit boundary
    pub(crate) ranges: Vec<(usize, usize)>,
    /// rows in `ranges`
    pub(crate) rows: usize,
}

/// One scan worker's claimed units: the selections they produced, tagged
/// with the unit index that restores `(segment, row range)` order, and the
/// row counters they summed.
type UnitPart = (Vec<(usize, Selection)>, ScanStats);

/// One unit of scan work: a contiguous slice of one segment's ranges.
struct Unit {
    plan: usize,
    lo: usize,
    hi: usize,
    rows: usize,
}

/// `GNOMON_ROWS_PER_UNIT` overrides the derived unit size, for the same reason
/// `GNOMON_SCAN_THREADS` exists: the sweep that calibrates it must not need a
/// rebuild. Unparseable values are treated as unset — a measurement knob must
/// never make reads fail.
fn rows_per_unit_override() -> Option<usize> {
    std::env::var("GNOMON_ROWS_PER_UNIT")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n >= 1)
}

/// Rows per row-range unit: the scan's examined rows divided over
/// `SUBUNIT_UNITS_PER_THREAD × threads` units, floored so that a segment
/// smaller than the floor stays exactly one unit.
///
/// The oversubscription factor is what buys the self-scheduler its slack: at
/// one unit per worker a single slow unit is the whole makespan again, and at
/// very many units the per-unit constant (a `Vec` header and a claim) starts
/// to show. Four units per worker puts the makespan within ~1/k of the
/// work-conserving bound while keeping the constant under a percent of a
/// 65 k-row loop.
/// Sweep the two terms of the derivation without a rebuild.
///
/// `GNOMON_ROWS_PER_UNIT` overrides the whole formula, which makes it a test
/// hook — it says what the unit size *is*, not what the engine would choose,
/// so a sweep using it cannot calibrate the defaults it is meant to site.
/// These two override the formula's inputs instead, so
/// every point of a sweep is a size the shipped engine would actually derive
/// at that store, at that width, for that query.
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(default)
}

fn rows_per_unit(total_rows: usize, threads: usize) -> usize {
    if let Some(n) = rows_per_unit_override() {
        return n;
    }
    let k = env_usize(
        "GNOMON_UNITS_PER_THREAD",
        crate::defaults::SUBUNIT_UNITS_PER_THREAD,
    );
    let floor = env_usize(
        "GNOMON_MIN_ROWS_PER_UNIT",
        crate::defaults::MIN_ROWS_PER_UNIT,
    );
    let units = k.saturating_mul(threads).max(1);
    total_rows.div_ceil(units).max(floor)
}

/// Cut each plan's ranges at unit boundaries and enumerate the units.
///
/// A range that straddles a boundary is split in place, so afterwards a unit
/// is exactly `ranges[lo..hi]` — no head/tail special case in the row loop,
/// and no range appears in two units or in none. Units are emitted in
/// `(segment index, range index)` order, which is the order the output is
/// restored to; that is what makes the answer independent of which worker
/// claimed what.
///
/// A segment whose rows fit in one unit keeps its range list untouched and
/// costs one `Unit` — which is the earlier behaviour, and is what the floor
/// in [`rows_per_unit`] is for.
fn cut_units(plans: &mut [SegPlan<'_>], rows_per_unit: usize) -> Vec<Unit> {
    let mut units = Vec::new();
    for (pi, p) in plans.iter_mut().enumerate() {
        if p.rows == 0 {
            continue;
        }
        if p.rows <= rows_per_unit {
            units.push(Unit {
                plan: pi,
                lo: 0,
                hi: p.ranges.len(),
                rows: p.rows,
            });
            continue;
        }
        let mut cut: Vec<(usize, usize)> =
            Vec::with_capacity(p.ranges.len() + p.rows / rows_per_unit + 1);
        let mut start = 0usize;
        let mut acc = 0usize;
        for &(a0, b) in p.ranges.iter() {
            let mut a = a0;
            while a < b {
                // `acc < rows_per_unit` is the loop invariant, so `take > 0`
                // and no empty range is ever emitted
                let take = (rows_per_unit - acc).min(b - a);
                cut.push((a, a + take));
                a += take;
                acc += take;
                if acc == rows_per_unit {
                    units.push(Unit {
                        plan: pi,
                        lo: start,
                        hi: cut.len(),
                        rows: acc,
                    });
                    start = cut.len();
                    acc = 0;
                }
            }
        }
        if acc > 0 {
            units.push(Unit {
                plan: pi,
                lo: start,
                hi: cut.len(),
                rows: acc,
            });
        }
        p.ranges = cut;
    }
    units
}

/// Concatenate the units of each segment back into one selection.
///
/// `select_units` returns its selections in `(segment, row range)` order and
/// a segment's units cover disjoint ascending row ranges, so this is a
/// concatenation and the result is byte-identical to what the earlier
/// `select` produced — one ascending `rows` vector per segment, segments
/// ascending. Every caller but the aggregate fold wants that shape:
/// `cluster_order` keys a selection by its first and last row, and
/// `materialize_selection` allocates one column set per selection.
///
/// The common case — one unit per segment — moves the vectors and copies
/// nothing.
fn coalesce_by_segment(sels: Vec<Selection>) -> Vec<Selection> {
    if sels.windows(2).all(|w| w[0].segment != w[1].segment) {
        return sels;
    }
    let mut out: Vec<Selection> = Vec::with_capacity(sels.len());
    let mut it = sels.into_iter().peekable();
    while let Some(mut first) = it.next() {
        let mut extra = 0usize;
        // size the destination exactly: growing a 2.3 M-row vector a doubling
        // at a time is the whole cost of this pass
        {
            let mut ahead = Vec::new();
            while it.peek().is_some_and(|s| s.segment == first.segment) {
                let s = it.next().expect("peeked");
                extra += s.rows.len();
                ahead.push(s);
            }
            if extra > 0 {
                first.rows.reserve_exact(extra);
                for s in ahead {
                    first.rows.extend_from_slice(&s.rows);
                }
            }
        }
        out.push(first);
    }
    out
}

/// One range covering every cluster — the serial layout.
fn whole(n: usize) -> Vec<std::ops::Range<usize>> {
    std::iter::once(0..n).collect()
}

/// Split clusters into at most `parts` contiguous ranges of roughly equal
/// row count. Contiguity is what lets the parts concatenate without a merge;
/// balancing by rows rather than by cluster count is what makes the widths
/// mean anything, since one cluster can hold a hundred times another's rows.
fn balance_clusters(
    clusters: &[Vec<usize>],
    selections: &[Selection],
    parts: usize,
) -> Vec<std::ops::Range<usize>> {
    let rows = |c: &Vec<usize>| -> u64 {
        c.iter().map(|&si| selections[si].rows.len() as u64).sum()
    };
    let total: u64 = clusters.iter().map(rows).sum();
    if parts <= 1 || clusters.len() <= 1 || total == 0 {
        return whole(clusters.len());
    }
    let target = total.div_ceil(parts as u64).max(1);
    let mut out = Vec::with_capacity(parts);
    let (mut start, mut acc) = (0usize, 0u64);
    for (i, c) in clusters.iter().enumerate() {
        acc += rows(c);
        // never cut so late that the tail cannot hold the remaining parts
        let must_cut = clusters.len() - i - 1 == parts - out.len() - 1;
        if (acc >= target || must_cut) && out.len() + 1 < parts {
            out.push(start..i + 1);
            start = i + 1;
            acc = 0;
        }
    }
    if start < clusters.len() {
        out.push(start..clusters.len());
    }
    out
}

/// Cut `rest` into consecutive sub-slices of the given lengths.
fn split_by<'a, T>(mut rest: &'a mut [T], lens: &[usize]) -> Vec<&'a mut [T]> {
    let mut out = Vec::with_capacity(lens.len());
    for &n in lens {
        let n = n.min(rest.len());
        let (a, b) = std::mem::take(&mut rest).split_at_mut(n);
        out.push(a);
        rest = b;
    }
    out
}

/// Lay the parts end to end into one set of columns, each part into its own
/// disjoint slice — so the copy happens once rather than once per part into
/// a growing buffer, and can run on the same workers that produced them.
fn concat_parts(
    mut parts: Vec<EdgeColumns>,
    total: usize,
    w: Want,
    parallel: bool,
) -> EdgeColumns {
    if parts.len() == 1 {
        return parts.pop().expect("len 1");
    }
    let mut out = EdgeColumns::zeroed(total, w, &parts);
    macro_rules! cuts {
        ($field:ident) => {{
            let lens: Vec<usize> = parts.iter().map(|p| p.$field.len()).collect();
            split_by(out.$field.as_mut_slice(), &lens).into_iter()
        }};
    }
    let (mut vt_s, mut vt_e) = (cuts!(vt_s), cuts!(vt_e));
    let (mut src, mut dst) = (cuts!(src_id), cuts!(dst_id));
    let (mut vid, mut rel) = (cuts!(vid), cuts!(rel_type));
    let (mut disc, mut props) = (cuts!(disc), cuts!(props));
    let (mut sid, mut srow) = (cuts!(seg_id), cuts!(seg_row));
    let mut jobs: Vec<(EdgeColumns, Dest<'_>)> = Vec::with_capacity(parts.len());
    for part in parts {
        jobs.push((
            part,
            Dest {
                vt_s: vt_s.next().expect("one cut per part"),
                vt_e: vt_e.next().expect("one cut per part"),
                src_id: src.next().expect("one cut per part"),
                dst_id: dst.next().expect("one cut per part"),
                vid: vid.next().expect("one cut per part"),
                rel_type: rel.next().expect("one cut per part"),
                disc: disc.next().expect("one cut per part"),
                props: props.next().expect("one cut per part"),
                seg_id: sid.next().expect("one cut per part"),
                seg_row: srow.next().expect("one cut per part"),
            },
        ));
    }
    if parallel {
        // Each job is moved out exactly once, by the index that owns it.
        let jobs: Vec<std::sync::Mutex<Option<(EdgeColumns, Dest<'_>)>>> =
            jobs.into_iter().map(|j| std::sync::Mutex::new(Some(j))).collect();
        crate::pool::run(jobs.len(), |i| {
            let job = jobs[i].lock().unwrap_or_else(|e| e.into_inner()).take();
            if let Some((part, dest)) = job {
                dest.fill(part);
            }
        });
    } else {
        for (part, dest) in jobs {
            dest.fill(part);
        }
    }
    out
}

/// One part's slot in the concatenated output.
struct Dest<'a> {
    vt_s: &'a mut [i64],
    vt_e: &'a mut [i64],
    src_id: &'a mut [u32],
    dst_id: &'a mut [u32],
    vid: &'a mut [Id96],
    rel_type: &'a mut [String],
    disc: &'a mut [String],
    props: &'a mut [String],
    seg_id: &'a mut [u64],
    seg_row: &'a mut [u32],
}

impl Dest<'_> {
    fn fill(self, part: EdgeColumns) {
        self.vt_s.copy_from_slice(&part.vt_s);
        self.vt_e.copy_from_slice(&part.vt_e);
        self.src_id.copy_from_slice(&part.src_id);
        self.dst_id.copy_from_slice(&part.dst_id);
        self.vid.copy_from_slice(&part.vid);
        self.seg_id.copy_from_slice(&part.seg_id);
        self.seg_row.copy_from_slice(&part.seg_row);
        // strings move rather than copy: the part is consumed here
        for (slot, s) in [
            (self.rel_type, part.rel_type),
            (self.disc, part.disc),
            (self.props, part.props),
        ] {
            for (d, v) in slot.iter_mut().zip(s) {
                *d = v;
            }
        }
    }
}

/// Membership test for the incidence filter.
///
/// Dense ids are small integers, so a bitset answers in one shifted load —
/// versus ~12 comparisons for a binary search over the id list, which a
/// profiler showed dominating the whole scan.
struct IdSet {
    bits: Vec<u64>,
    max: u32,
}

impl IdSet {
    fn new(ids: &[u32]) -> Self {
        let max = ids.iter().copied().max().unwrap_or(0);
        let mut bits = vec![0u64; (max as usize / 64) + 1];
        for &id in ids {
            bits[id as usize / 64] |= 1u64 << (id % 64);
        }
        Self { bits, max }
    }

    #[inline]
    fn contains(&self, id: u32) -> bool {
        id <= self.max && (self.bits[id as usize / 64] >> (id % 64)) & 1 == 1
    }
}

/// Row ranges whose transaction time is believed at `as_of`.
///
/// `tt_s` is run-length encoded, so a batch collapses to one range and the
/// belief predicate costs a couple of comparisons per segment rather than one
/// per row.
///
/// Compaction breaks that assumption: it re-sorts rows from every generation
/// into global `(vt_s, vid)` order while each keeps its origin `tt_s`, so the
/// runs interleave and a measured compacted node segment held 2.1M of them.
/// Two things follow, and both are load-bearing. `tt_bounds` (the segment's
/// folded `(min, max)` tt_s) answers the all-visible and none-visible cases
/// without touching the runs at all — and current belief, the overwhelmingly
/// common query, is always the all-visible case. When the answer really is
/// mixed, adjacent runs sharing a verdict coalesce, so the output is one
/// range per verdict *change* rather than one per run: `select_range` walks
/// what comes back, and its contiguous-extend fast path only ever sees whole
/// ranges.
// Visibility is `pub(crate)` because `beliefdiff` calls this directly.
// The parameter is a `TtSView` rather than a `&SegmentHeader`, and the
// difference is load-bearing rather than cosmetic: a view built from a
// header alone has `codes: None`, and on a dictionary-coded segment the run
// list is *dropped* rather than kept alongside, so a header-built view would
// take the run-list path over an empty list and report that nothing is
// believed -- silently, and only on the segments where the dictionary fired.
// Callers must obtain the view from the segment (`seg.tt_s_view()?`).
pub(crate) fn believed_ranges(
    tt: &crate::ttime::TtSView<'_>,
    tt_bounds: (i64, i64),
    as_of: i64,
    rows: usize,
) -> Vec<(usize, usize)> {
    let (tt_min, tt_max) = tt_bounds;
    // A dictionary-coded segment answers the two extreme cases from the same
    // folded bounds, and the mixed case from its per-block code bounds --
    // only a block that straddles the `as_of` cut is walked. The ranges are
    // the same ranges: both shapes produce the maximal runs of rows whose
    // `tt_s <= as_of`, coalesced by the same rule.
    if tt.is_dictionary() {
        if tt_min > as_of {
            return Vec::new();
        }
        if tt_max <= as_of {
            return if rows > 0 { vec![(0, rows)] } else { Vec::new() };
        }
        return tt.believed_ranges_dict(as_of, rows);
    }
    // a raw column decides the extremes from the same bounds and
    // reads every row otherwise
    if tt.is_raw() {
        if tt_min > as_of {
            return Vec::new();
        }
        if tt_max <= as_of {
            return if rows > 0 { vec![(0, rows)] } else { Vec::new() };
        }
        return tt.believed_ranges_raw(as_of, rows, 0, rows);
    }
    let runs = tt.runs();
    let Some(&(first, _)) = runs.first() else {
        return Vec::new();
    };
    if tt_min > as_of {
        return Vec::new();
    }
    if tt_max <= as_of {
        let first = first as usize;
        return if first < rows {
            vec![(first, rows)]
        } else {
            Vec::new()
        };
    }
    let mut out: Vec<(usize, usize)> = Vec::new();
    for (i, (start, tt)) in runs.iter().enumerate() {
        let end = runs
            .get(i + 1)
            .map(|(s, _)| *s as usize)
            .unwrap_or(rows)
            .min(rows);
        let start = *start as usize;
        if *tt > as_of || start >= end {
            continue;
        }
        match out.last_mut() {
            Some(last) if last.1 == start => last.1 = end,
            _ => out.push((start, end)),
        }
    }
    crate::vistouch::runs(runs.len());
    out
}

/// Is `[lo, hi)` the whole of a `rows`-row segment? Whole-extent requests
/// take the whole-segment primitives unchanged, so a query
/// that needs every row runs exactly the code it ran before the restricted
/// forms existed.
#[inline]
pub(crate) fn is_whole(lo: usize, hi: usize, rows: usize) -> bool {
    lo == 0 && hi >= rows
}

/// Above this many ranges a request's row domain is not "a small part of the
/// segment" in any useful sense, and the planner computes belief for the
/// whole segment and clips, as the whole-segment path does. Only the match plan's open-ended
/// rows below the window cut can produce more than one range.
const WINDOW_FIRST_MAX_RANGES: usize = 64;

/// A touching read's postings come before the belief work when they
/// make at most this many ranges of the request domain (each range is one
/// `close_view_in` and one believed-range pass); past it, the old order.
const POSTINGS_FIRST_MAX_RANGES: usize = 256;
/// ...and a segment's postings of more rows than this are not looked at for
/// it: the posting domain would cost more to build than the old order saves.
const POSTINGS_FIRST_MAX_ROWS: usize = 4_096;

/// The rows a scan request can use in one segment: the open-ended rows below
/// the window cut (each clipped below `min(lo, hi)`, exactly as the match plan's
/// clip loop clips them) and then `[lo, hi)`. Ascending and disjoint; two
/// entries can abut.
/// The posting rows (ascending) that fall inside the planned ranges
/// (ascending, disjoint), as coalesced ranges, and their count. One merge
/// pass over both lists.
pub(crate) fn intersect_rows(
    rows: &[u32],
    ranges: &[(usize, usize)],
) -> (Vec<(usize, usize)>, usize) {
    let mut out: Vec<(usize, usize)> = Vec::new();
    let mut n = 0usize;
    let mut k = 0usize;
    for &r in rows {
        let r = r as usize;
        while k < ranges.len() && ranges[k].1 <= r {
            k += 1;
        }
        if k == ranges.len() {
            break;
        }
        if r < ranges[k].0 {
            continue;
        }
        n += 1;
        match out.last_mut() {
            Some(last) if last.1 == r => last.1 = r + 1,
            _ => out.push((r, r + 1)),
        }
    }
    (out, n)
}

fn request_domain(open_below: &[(u32, u32)], lo: usize, hi: usize) -> Vec<(usize, usize)> {
    let below = lo.min(hi);
    let mut d: Vec<(usize, usize)> = Vec::with_capacity(open_below.len() + 1);
    for &(os, oe) in open_below {
        let (a, b) = (os as usize, (oe as usize).min(below));
        if a < b {
            d.push((a, b));
        }
    }
    if lo < hi {
        d.push((lo, hi));
    }
    d
}

/// Compute belief for the request's domain alone?
///
/// No when the domain is the whole segment, when it covers more than half
/// of it, or when it is fragmented into more than
/// [`WINDOW_FIRST_MAX_RANGES`] ranges: the whole-segment path is then no
/// more expensive, and taking it keeps whole-extent queries on exactly the
/// code they ran before. Both paths give the same ranges, so this choice
/// moves cost only, never an answer.
fn window_first(domain: &[(usize, usize)], rows: usize) -> bool {
    if domain.is_empty() || domain.len() > WINDOW_FIRST_MAX_RANGES {
        return false;
    }
    if domain.len() == 1 && is_whole(domain[0].0, domain[0].1, rows) {
        return false;
    }
    let covered: usize = domain.iter().map(|(a, b)| b - a).sum();
    covered.saturating_mul(2) <= rows
}

/// [`believed_ranges`] restricted to the rows `[lo, hi)`.
///
/// Returns exactly `believed_ranges(..)` intersected with `[lo, hi)`, in
/// the same canonical form (maximal, ascending, disjoint ranges). Whether a
/// row is written by `as_of` depends on that row's own `tt_s` alone, so the
/// intersection can be taken first. What changes is what is read:
///
/// - the folded bounds still decide the all/none cases without reading
///   anything, and the all case is the range itself (clipped below by the
///   run list's first row, exactly as the whole-segment answer is);
/// - a run list is entered by one binary search (the run covering `lo`)
///   and walked only to the first run starting at or after `hi`;
/// - a dictionary-coded segment visits only the code blocks covering the
///   range ([`TtSView::believed_ranges_dict_in`](crate::ttime::TtSView)).
///
/// So the cost is `O(log runs + runs in range)` or `O(blocks in range +
/// rows of the range in straddling blocks)`, independent of the rest of
/// the segment.
pub(crate) fn believed_ranges_in(
    tt: &crate::ttime::TtSView<'_>,
    tt_bounds: (i64, i64),
    as_of: i64,
    rows: usize,
    lo: usize,
    hi: usize,
) -> Vec<(usize, usize)> {
    let hi = hi.min(rows);
    if lo >= hi {
        return Vec::new();
    }
    let (tt_min, tt_max) = tt_bounds;
    if tt.is_dictionary() {
        if tt_min > as_of {
            return Vec::new();
        }
        if tt_max <= as_of {
            return vec![(lo, hi)];
        }
        return tt.believed_ranges_dict_in(as_of, rows, lo, hi);
    }
    if tt.is_raw() {
        if tt_min > as_of {
            return Vec::new();
        }
        if tt_max <= as_of {
            return vec![(lo, hi)];
        }
        return tt.believed_ranges_raw(as_of, rows, lo, hi);
    }
    let runs = tt.runs();
    let Some(&(first, _)) = runs.first() else {
        return Vec::new();
    };
    if tt_min > as_of {
        return Vec::new();
    }
    if tt_max <= as_of {
        let a = lo.max(first as usize);
        return if a < hi { vec![(a, hi)] } else { Vec::new() };
    }
    // the run covering `lo` (or the first run, when `lo` precedes it)
    let first_i = runs
        .partition_point(|(s, _)| (*s as usize) <= lo)
        .saturating_sub(1);
    let mut i = first_i;
    let mut out: Vec<(usize, usize)> = Vec::new();
    while i < runs.len() {
        let (start, tt) = runs[i];
        let start = start as usize;
        if start >= hi {
            break;
        }
        let end = runs
            .get(i + 1)
            .map(|(s, _)| *s as usize)
            .unwrap_or(rows)
            .min(rows);
        i += 1;
        let (a, b) = (start.max(lo), end.min(hi));
        if tt > as_of || a >= b {
            continue;
        }
        match out.last_mut() {
            Some(last) if last.1 == a => last.1 = b,
            _ => out.push((a, b)),
        }
    }
    crate::vistouch::runs(i - first_i);
    out
}

/// [`believed_ranges_closed`] restricted to `[lo, hi)`. `view` may be
/// the whole segment's close view or one restricted to the same range
/// ([`crate::visibility::close_view_in`]): `subtract_closes` enters the
/// close list by binary search, so closes outside the range are never
/// walked either way, and the restricted view is what avoids *building*
/// them. A whole-segment request takes [`believed_ranges_closed`] itself.
pub(crate) fn believed_ranges_closed_in(
    tt: &crate::ttime::TtSView<'_>,
    tt_bounds: (i64, i64),
    as_of: i64,
    rows: usize,
    lo: usize,
    hi: usize,
    view: &crate::visibility::CloseView<'_>,
) -> Vec<(usize, usize)> {
    if is_whole(lo, hi, rows) {
        return believed_ranges_closed(tt, tt_bounds, as_of, rows, view);
    }
    subtract_closes(
        believed_ranges_in(tt, tt_bounds, as_of, rows, lo, hi),
        as_of,
        view.as_slice(),
    )
}

/// [`believed_ranges_any`] restricted to `[lo, hi)`.
pub(crate) fn believed_ranges_any_in(
    tt: &crate::ttime::TtSView<'_>,
    tt_bounds: (i64, i64),
    as_of: i64,
    rows: usize,
    lo: usize,
    hi: usize,
) -> Vec<(usize, usize)> {
    if is_whole(lo, hi, rows) {
        return believed_ranges_any(tt, tt_bounds, as_of, rows);
    }
    believed_ranges_in(tt, tt_bounds, as_of, rows, lo, hi)
}

/// [`believed_ranges_superseded`] restricted to `[lo, hi)`.
pub(crate) fn believed_ranges_superseded_in(
    tt: &crate::ttime::TtSView<'_>,
    tt_bounds: (i64, i64),
    as_of: i64,
    rows: usize,
    lo: usize,
    hi: usize,
    view: &crate::visibility::CloseView<'_>,
) -> Vec<(usize, usize)> {
    if is_whole(lo, hi, rows) {
        return believed_ranges_superseded(tt, tt_bounds, as_of, rows, view);
    }
    retain_closes(
        believed_ranges_in(tt, tt_bounds, as_of, rows, lo, hi),
        as_of,
        view.as_slice(),
    )
}

/// Row ranges believed at `as_of` — **both** halves of the belief predicate.
///
/// [`believed_ranges`] answers `tt_s <= as_of`. The other half, `as_of <
/// tt_e`, used to be a per-row test, and the damage was not the test itself
/// (a HashMap probe and a binary search) but that having *any* close in a
/// segment set `needs_row_test`, which forfeited `select_range`'s
/// contiguous-extend fast path for that segment's whole scan. One correction
/// anywhere in a segment therefore converted a range extend into a row loop
/// — measured at 2–7× on scan-shaped queries at 5 % correction density, and
/// 4.0–6.6× at 10M rows / 0.1 %.
///
/// Subtracting the closed positions in the range domain instead hands
/// `select_range` ranges it can extend contiguously again, and costs
/// O(closes in this segment) rather than O(rows in the believed ranges).
/// `pub(crate)` rather than private because the graph traversal in
/// `store.rs` consumes it for exactly the same reason `select_range` does —
/// belief filtering at traversal time is per segment per call, not per row.
/// Visibility only; the body is unchanged.
pub(crate) fn believed_ranges_closed(
    tt: &crate::ttime::TtSView<'_>,
    tt_bounds: (i64, i64),
    as_of: i64,
    rows: usize,
    view: &crate::visibility::CloseView<'_>,
) -> Vec<(usize, usize)> {
    subtract_closes(
        believed_ranges(tt, tt_bounds, as_of, rows),
        as_of,
        view.as_slice(),
    )
}

/// Row ranges *written* by `as_of` — the `tt_s <= as_of` half alone, with no
/// close subtracted.
///
/// This is `version_history`'s `Belief::All`: every row that existed at
/// `as_of`, whether or not a later transaction has since revised it. It is
/// the union of [`believed_ranges_closed`] and
/// [`believed_ranges_superseded`], and the domain both of those are computed
/// *within*, which is why all three are exported together rather than the
/// caller reaching for one and reconstructing the others.
///
/// `pub(crate)` for `read.rs::version_keys`; the body is [`believed_ranges`],
/// unchanged.
///
/// Takes a [`TtSView`](crate::ttime::TtSView), never a header, for the
/// reason [`believed_ranges`] does: on a dictionary-coded segment the header's
/// `tt_s_runs` is empty, so a header-built view would take the run-list path
/// over nothing and report that no row was ever written — silently, and only
/// on the segments where the dictionary fired. Obtain it with
/// `seg.tt_s_view()?`.
pub(crate) fn believed_ranges_any(
    tt: &crate::ttime::TtSView<'_>,
    tt_bounds: (i64, i64),
    as_of: i64,
    rows: usize,
) -> Vec<(usize, usize)> {
    believed_ranges(tt, tt_bounds, as_of, rows)
}

/// Row ranges written by `as_of` **and already revised** by `as_of` — the
/// complement of [`believed_ranges_closed`] within [`believed_ranges_any`].
///
/// `version_history`'s `Belief::Superseded` mode asks for exactly the rows
/// [`subtract_closes`] throws away, so this is that walk with the other side
/// kept: the same monotone cursor over the same sorted `CloseView`, emitting
/// the cut positions instead of the gaps between them. Two properties follow
/// from that construction and are asserted per segment by
/// `believed_and_superseded_partition_the_written_rows`:
///
/// - **union** — `believed_ranges_closed ∪ believed_ranges_superseded` is
///   `believed_ranges_any`, because every row of the base is either cut or
///   kept and the walk visits the base exactly once;
/// - **disjointness** — the two never share a row, because a row is cut on
///   `tt_e <= as_of` and kept on its negation.
///
/// Unlike its sibling this has no identity fast path: an uncorrected segment
/// supersedes nothing, so the empty vector *is* the answer, and returning it
/// costs no allocation at all.
///
/// Like its siblings it reads transaction time only through the
/// [`TtSView`](crate::ttime::TtSView), so the run-list and dictionary shapes
/// produce the same base and therefore the same partition;
/// `superseded_partition_holds_on_a_dictionary_coded_segment` asserts it on a
/// segment that is first checked to be dictionary-coded. `retain_closes`
/// itself never sees transaction time — it works purely on the range list
/// and the close view — so it needed no change.
pub(crate) fn believed_ranges_superseded(
    tt: &crate::ttime::TtSView<'_>,
    tt_bounds: (i64, i64),
    as_of: i64,
    rows: usize,
    view: &crate::visibility::CloseView<'_>,
) -> Vec<(usize, usize)> {
    retain_closes(
        believed_ranges(tt, tt_bounds, as_of, rows),
        as_of,
        view.as_slice(),
    )
}

/// Keep exactly the rows [`subtract_closes`] removes: the rows of `base` a
/// close has ended by `as_of`.
///
/// Same cursor, same bounds reasoning, opposite side. Adjacent closed rows
/// *do* have to be coalesced here — `subtract_closes` never needs to because
/// its output is separated by the cut rows themselves, while two closes on
/// consecutive rows produce two adjacent single-row ranges. The
/// `last.1 == r` extend below is that coalescing, and it is what keeps the
/// output minimal and strictly ascending, the same contract the range walk in
/// `version_keys` consumes.
fn retain_closes(
    base: Vec<(usize, usize)>,
    as_of: i64,
    closes: &[(u32, i64)],
) -> Vec<(usize, usize)> {
    if closes.is_empty() {
        return Vec::new();
    }
    let mut out: Vec<(usize, usize)> = Vec::new();
    let mut ci = 0usize;
    let mut walked = 0usize;
    for (rs, re) in base {
        // the first close at or after `rs`, searched from the cursor rather
        // than from zero — the same thing that keeps the sibling linear
        ci += closes[ci..].partition_point(|(r, _)| (*r as usize) < rs);
        let mut k = ci;
        while k < closes.len() && (closes[k].0 as usize) < re {
            let (r, tt_e) = closes[k];
            if tt_e <= as_of {
                let r = r as usize;
                match out.last_mut() {
                    Some(last) if last.1 == r => last.1 = r + 1,
                    _ => out.push((r, r + 1)),
                }
            }
            k += 1;
        }
        walked += k - ci;
        ci = k;
    }
    crate::vistouch::closes(walked);
    out
}

/// Cut the rows a close has ended by `as_of` out of `base`.
///
/// `base` ascends, its ranges are disjoint, and adjacent same-verdict runs
/// are already coalesced by [`believed_ranges`] — so consecutive base ranges
/// are separated by at least one unbelieved row. Within a range this only
/// ever *splits*, and every split skips at least one row, so the result is
/// still minimal and strictly ascending: exactly the contract `select_range`
/// consumes. Nothing needs re-coalescing.
///
/// `closes` is sorted by row, so one monotone cursor walks it across the
/// whole base list; rows it lists that fall outside every base range (a
/// close on a row whose `tt_s > as_of`, or a stale run naming a row id past
/// the segment's end) are stepped over in the gaps and cannot corrupt the
/// walk.
///
/// With no closes the `base` vector is returned by identity — same
/// allocation, no copy — which is what makes the `all_current` fast path
/// cost the same as before rather than merely *about* the same.
fn subtract_closes(
    base: Vec<(usize, usize)>,
    as_of: i64,
    closes: &[(u32, i64)],
) -> Vec<(usize, usize)> {
    if closes.is_empty() {
        return base;
    }
    // Every cut splits one range into at most two, so the output is bounded
    // by `base + closes` and is sized exactly here. A corrected segment is
    // the case where the output is *long* — one range per run of believed
    // rows between closes — which is precisely when growing it a doubling at
    // a time would be paid on every scan of it.
    let mut out: Vec<(usize, usize)> = Vec::with_capacity(base.len() + closes.len());
    let mut ci = 0usize;
    let mut walked = 0usize;
    for (rs, re) in base {
        // the first close at or after `rs`, searched from the cursor rather
        // than from zero — this is what keeps the whole walk linear
        ci += closes[ci..].partition_point(|(r, _)| (*r as usize) < rs);
        let mut cur = rs;
        let mut k = ci;
        while k < closes.len() && (closes[k].0 as usize) < re {
            let (r, tt_e) = closes[k];
            if tt_e <= as_of {
                // this row is no longer believed at as_of
                let r = r as usize;
                if cur < r {
                    out.push((cur, r));
                }
                cur = r + 1;
            }
            k += 1;
        }
        walked += k - ci;
        ci = k;
        if cur < re {
            out.push((cur, re));
        }
    }
    crate::vistouch::closes(walked);
    out
}

/// All of one segment's column handles, resolved once.
struct SegmentView<'a> {
    vt_s: &'a [i64],
    vt_e: Option<&'a [i64]>,
    src_id: &'a [u32],
    dst_id: &'a [u32],
    vid64: &'a [u64],
    vid_lo32: &'a [u32],
    rel_code: &'a [u16],
    disc_ref: &'a [u32],
    props_ref: &'a [u32],
    rel_types: &'a [String],
    strings: StringView<'a>,
}

impl<'a> SegmentView<'a> {
    fn open<S: SegmentSource>(seg: &'a Segment<S>) -> Result<Self> {
        Ok(Self {
            vt_s: seg.i64_column("vt_s")?,
            vt_e: if seg.header().vt_e_elided {
                None
            } else {
                Some(seg.i64_column("vt_e")?)
            },
            src_id: seg.u32_column("src_id")?,
            dst_id: seg.u32_column("dst_id")?,
            vid64: seg.u64_column("vid64")?,
            vid_lo32: seg.u32_column("vid_lo32")?,
            rel_code: seg.u16_column("rel_code")?,
            disc_ref: seg.u32_column("disc_ref")?,
            props_ref: seg.u32_column("props_ref")?,
            rel_types: &seg.header().rel_types,
            strings: seg.strings()?,
        })
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct EdgeColumns {
    pub vt_s: Vec<i64>,
    pub vt_e: Vec<i64>,
    pub src_id: Vec<u32>,
    pub dst_id: Vec<u32>,
    pub vid: Vec<Id96>,
    pub rel_type: Vec<String>,
    pub disc: Vec<String>,
    pub props: Vec<String>,
    /// Physical address of each returned row (`ScanTarget::id`, row within
    /// that segment), filled only when the projection asks for `seg_id` /
    /// `seg_row`. Addresses let a caller come back later for the expensive
    /// derived fields of a few surviving rows (`edge_idents_at`) instead of
    /// materializing them for the whole scan.
    pub seg_id: Vec<u64>,
    pub seg_row: Vec<u32>,
}

/// Which columns materialization builds. `vt_s` is unconditional: it is the
/// sort key, and `EdgeColumns::len` is defined by it.
///
/// The fixed-width columns used to be exempt from the projection — `copy_run`
/// honoured it only for `vid` and the three string columns. That is the same
/// defect one layer lower: `columns=["vt_s"]` and
/// `columns=["src_id","dst_id","vt_s","vt_e"]` measured within noise of each
/// other at 10M, because three of the four were being built either
/// way.
#[derive(Clone, Copy, Debug)]
struct Want {
    vt_e: bool,
    src: bool,
    dst: bool,
    vid: bool,
    rel: bool,
    disc: bool,
    props: bool,
    addr: bool,
}

impl Want {
    fn of(req: &ScanRequest) -> Self {
        let want = |name: &str| {
            req.columns
                .as_ref()
                .map(|c| c.iter().any(|x| x == name))
                .unwrap_or(true)
        };
        Self {
            vt_e: want("vt_e"),
            src: want("src_id"),
            dst: want("dst_id"),
            // vid is two integer columns here but a 24-char hex string at the
            // boundary, so building it unasked cost more than the scan itself
            vid: want("vid"),
            rel: want("rel_type"),
            disc: want("disc"),
            props: want("props"),
            addr: want("seg_id") || want("seg_row"),
        }
    }

}

/// Copy rows [a, b) of one segment view into the output columns — the one
/// copy routine both materialize paths share. Fixed-width columns memcpy;
/// derived and string columns pay per row only when asked for.
fn copy_run(
    v: &SegmentView<'_>,
    a: usize,
    b: usize,
    cols: &mut EdgeColumns,
    w: Want,
) -> Result<()> {
    let (w_vid, w_rel, w_disc, w_props) = (w.vid, w.rel, w.disc, w.props);
    cols.vt_s.extend_from_slice(&v.vt_s[a..b]);
    if w.src {
        cols.src_id.extend_from_slice(&v.src_id[a..b]);
    }
    if w.dst {
        cols.dst_id.extend_from_slice(&v.dst_id[a..b]);
    }
    if w.vt_e {
        match v.vt_e {
            Some(col) => cols.vt_e.extend_from_slice(&col[a..b]),
            // elided means every row is instantaneous
            None => cols.vt_e.extend(v.vt_s[a..b].iter().map(|t| t + 1)),
        }
    }
    if w_vid {
        cols.vid.extend(
            v.vid64[a..b]
                .iter()
                .zip(&v.vid_lo32[a..b])
                .map(|(&hi, &lo)| Id96 { hi, lo }),
        );
    }
    if w_rel {
        cols.rel_type.extend(
            v.rel_code[a..b]
                .iter()
                .map(|&c| v.rel_types[c as usize].clone()),
        );
    }
    if w_disc {
        for &r in &v.disc_ref[a..b] {
            cols.disc.push(v.strings.get(r)?.to_string());
        }
    }
    if w_props {
        for &r in &v.props_ref[a..b] {
            cols.props.push(v.strings.get(r)?.to_string());
        }
    }
    Ok(())
}

impl EdgeColumns {
    fn append(&mut self, mut o: EdgeColumns) {
        self.vt_s.append(&mut o.vt_s);
        self.vt_e.append(&mut o.vt_e);
        self.src_id.append(&mut o.src_id);
        self.dst_id.append(&mut o.dst_id);
        self.vid.append(&mut o.vid);
        self.rel_type.append(&mut o.rel_type);
        self.disc.append(&mut o.disc);
        self.props.append(&mut o.props);
        self.seg_id.append(&mut o.seg_id);
        self.seg_row.append(&mut o.seg_row);
    }

    /// Destination for a concatenation: every column that some part filled,
    /// sized to hold all of them. `seg_id`/`seg_row` follow the parts rather
    /// than `Want`, because they are filled together or not at all.
    fn zeroed(n: usize, w: Want, parts: &[EdgeColumns]) -> Self {
        let any = |f: fn(&EdgeColumns) -> bool| parts.iter().any(f);
        let len = |yes: bool| if yes { n } else { 0 };
        Self {
            vt_s: vec![0; n],
            vt_e: vec![0; len(w.vt_e)],
            src_id: vec![0; len(w.src)],
            dst_id: vec![0; len(w.dst)],
            vid: vec![Id96::default(); len(w.vid)],
            rel_type: vec![String::new(); len(w.rel)],
            disc: vec![String::new(); len(w.disc)],
            props: vec![String::new(); len(w.props)],
            seg_id: vec![0; len(any(|p| !p.seg_id.is_empty()))],
            seg_row: vec![0; len(any(|p| !p.seg_row.is_empty()))],
        }
    }

    /// Reserve only what will be filled. Reserving all eight columns for a
    /// 10M-row scan asks the allocator for ~1.1 GB of address space that a
    /// projected scan never touches.
    fn with_capacity(n: usize, w: Want) -> Self {
        let cap = |yes: bool| if yes { n } else { 0 };
        Self {
            vt_s: Vec::with_capacity(n),
            vt_e: Vec::with_capacity(cap(w.vt_e)),
            src_id: Vec::with_capacity(cap(w.src)),
            dst_id: Vec::with_capacity(cap(w.dst)),
            vid: Vec::with_capacity(cap(w.vid)),
            rel_type: Vec::with_capacity(cap(w.rel)),
            disc: Vec::with_capacity(cap(w.disc)),
            props: Vec::with_capacity(cap(w.props)),
            seg_id: Vec::new(),
            seg_row: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.vt_s.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vt_s.is_empty()
    }
}

#[cfg(test)]
mod tests {
    /// Every belief test here builds a header directly; the run-list
    /// shape is what such a header carries, and this is the view over it.
    fn tt_view(h: &crate::segment::SegmentHeader) -> crate::ttime::TtSView<'_> {
        crate::ttime::TtSView::from_header(h)
    }

    use super::*;
    use crate::derive::{edge_eid, version_vid};
    use crate::row::EdgeRow;
    use crate::segment::{write_edge_segment, MemorySource, Segment, SegmentSpec};
    use std::path::PathBuf;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-scan-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn edge(vt_s: i64, i: u32, tt_s: i64, rel: &str, src: u32, dst: u32) -> EdgeRow {
        let disc = format!("#{i}");
        let eid = edge_eid("a", "b", rel, &disc);
        EdgeRow {
            vid: version_vid(&eid.to_hex(), tt_s, vt_s),
            src_id: src,
            dst_id: dst,
            rel_type: rel.into(),
            disc,
            vt_s,
            vt_e: vt_s + 1,
            tt_s,
            props: "{}".into(),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    /// Reference implementation of the filter, written the obvious slow way.
    fn expected(rows: &[EdgeRow], req: &ScanRequest) -> Vec<(i64, Id96)> {
        let as_of = clamp_tt(req.as_of_tt);
        let mut v: Vec<(i64, Id96)> = rows
            .iter()
            .filter(|r| r.tt_s <= as_of)
            .filter(|r| req.vt_min.is_none_or(|t| r.vt_e > t))
            .filter(|r| req.vt_max.is_none_or(|t| r.vt_s < t))
            .filter(|r| {
                req.rel_types
                    .as_ref()
                    .is_none_or(|rs| rs.contains(&r.rel_type))
            })
            .filter(|r| {
                req.touching_ids
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&r.src_id) || ids.contains(&r.dst_id))
            })
            .map(|r| (r.vt_s, r.vid))
            .collect();
        v.sort();
        if let Some(n) = req.limit {
            v.truncate(n);
        }
        v
    }

    struct Fixture {
        rows: Vec<EdgeRow>,
        segs: Vec<Segment<MemorySource>>,
    }

    impl Fixture {
        /// Two segments with *overlapping* vt ranges, as successive
        /// generations produce — so the merge is genuinely k-way.
        fn build(name: &str) -> Self {
            let dir = tmp(name);
            let mut rows = Vec::new();
            let mut segs = Vec::new();
            for (batch, tt) in [(0u32, 10i64), (1, 20)] {
                let mut b: Vec<EdgeRow> = (0..200u32)
                    .map(|i| {
                        let rel = if i % 3 == 0 { "RATED" } else { "SENT" };
                        edge(
                            1_000 + (i as i64) * 2 + batch as i64,
                            i + batch * 1000,
                            tt,
                            rel,
                            i % 11,
                            (i + 5) % 11,
                        )
                    })
                    .collect();
                b.sort_by_key(|r| r.sort_key());
                let path = dir.join(format!("{batch}.tgs"));
                write_edge_segment(&path, &b, &SegmentSpec::default()).unwrap();
                segs.push(
                    Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap(),
                );
                rows.extend(b);
            }
            Self { rows, segs }
        }

        fn set(&self) -> ScanSet<'_, MemorySource> {
            ScanSet::from_pairs(self.segs.iter().map(|s| (s, Lane::Event)).collect())
        }
    }

    fn run(f: &Fixture, req: &ScanRequest) -> Vec<(i64, Id96)> {
        let set = f.set();
        let (sel, _) = set.select(req).unwrap();
        set.merged(&sel, req.limit)
            .unwrap()
            .into_iter()
            .map(|(si, row)| {
                let seg = &f.segs[si];
                (
                    seg.i64_column("vt_s").unwrap()[row as usize],
                    seg.vid_at(row as usize).unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn unfiltered_scan_matches_the_reference() {
        let f = Fixture::build("all");
        let req = ScanRequest::current();
        assert_eq!(run(&f, &req), expected(&f.rows, &req));
    }

    #[test]
    fn results_are_globally_ordered_across_overlapping_segments() {
        let f = Fixture::build("order");
        let got = run(&f, &ScanRequest::current());
        assert_eq!(got.len(), 400);
        assert!(
            got.windows(2).all(|w| w[0] < w[1]),
            "merged output must be strictly ascending by (vt_s, vid)"
        );
    }

    #[test]
    fn window_filter_matches_the_reference() {
        let f = Fixture::build("window");
        for (a, b) in [(1_000, 1_100), (1_150, 1_250), (0, 10), (1_390, 9_999)] {
            let req = ScanRequest::current().window(a, b);
            assert_eq!(run(&f, &req), expected(&f.rows, &req), "window [{a},{b})");
        }
    }

    /// Build one interval-lane segment: `n` rows, closed lives that jitter,
    /// and every `stride`-th row open-ended (`stride == 0` means none).
    fn interval_segment(
        dir: &std::path::Path,
        name: &str,
        n: u32,
        stride: u32,
    ) -> (Vec<EdgeRow>, Segment<MemorySource>) {
        let mut rows: Vec<EdgeRow> = (0..n)
            .map(|i| edge(1_000 + i as i64 * 3, i, 10, "SENT", i % 11, (i + 5) % 11))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        for (i, r) in rows.iter_mut().enumerate() {
            let long = i % 5 == 0;
            r.vt_e = r.vt_s + if long { 5_000 } else { 50 };
        }
        if stride > 0 {
            // planted early, where a prefix aggregate does the most damage
            for i in (0..n as usize).step_by(stride as usize) {
                rows[i].vt_e = OPEN_END;
            }
        }
        let path = dir.join(format!("{name}.tgs"));
        write_edge_segment(
            &path,
            &rows,
            &SegmentSpec {
                lane: Lane::Interval,
                vt_e_block_rows: 64,
                ..Default::default()
            },
        )
        .unwrap();
        let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        (rows, seg)
    }

    /// The design memo's cliff: **one open-ended row in ten thousand took
    /// the pruned fraction from 0.507 to 1.0000**, because a running maximum
    /// is a prefix aggregate and one far-future `vt_e` raises every later
    /// entry. The corpus's only real interval-lane segment is 194,085 rows
    /// all of which are open-ended, so it pruned nothing at all.
    ///
    /// Computing the maximum over closed rows and listing the open-ended
    /// rows separately must restore the cut at every open-ended fraction.
    /// The measurement is the same one the memo made: the mean fraction of a
    /// segment left to examine, averaged over window openings at the 10th
    /// through 90th percentiles of `vt_s`.
    #[test]
    fn the_open_ended_cliff_is_gone() {
        let dir = tmp("open-cliff");
        let n = 20_000u32;
        let scanned = |seg: &Segment<MemorySource>| -> f64 {
            let set = ScanSet::from_pairs(vec![(seg, Lane::Interval)]);
            let vt_s = seg.i64_column("vt_s").unwrap();
            let (lo_v, hi_v) = (vt_s[0], vt_s[vt_s.len() - 1]);
            let mut total = 0f64;
            let mut k = 0f64;
            for p in 1..=9 {
                let vt_min = lo_v + (hi_v - lo_v) * p / 10;
                let (_, stats) = set
                    .select(&ScanRequest { vt_min: Some(vt_min), ..ScanRequest::current() })
                    .unwrap();
                total += stats.rows_examined as f64 / n as f64;
                k += 1.0;
            }
            total / k
        };

        let (_, none) = interval_segment(&dir, "none", n, 0);
        let base = scanned(&none);
        assert!(
            base < 0.9,
            "fixture-is-real: with no open-ended rows the cut must do something, got {base:.4}"
        );

        // one row in 10^4, 10^3 and 10^2 — the memo's sweep
        for stride in [10_000u32, 1_000, 100] {
            let (_, seg) = interval_segment(&dir, &format!("s{stride}"), n, stride);
            assert!(
                !seg.header().open_rows.is_empty(),
                "stride {stride}: the fixture must really plant open-ended rows"
            );
            let f = scanned(&seg);
            assert!(
                (f - base).abs() < 0.01,
                "one open-ended row in {stride} moved the scanned fraction from \
                 {base:.4} to {f:.4}; a prefix aggregate over closed rows must not \
                 be sensitive to them"
            );
        }
    }

    /// The other half of the memo's finding: a segment that is *entirely*
    /// open-ended. The running maximum over closed rows is then empty of
    /// information, the open-row list is one range, and every answer must
    /// still be exactly right.
    #[test]
    fn an_entirely_open_ended_segment_answers_correctly() {
        let dir = tmp("all-open");
        let n = 4_000u32;
        let (rows, seg) = interval_segment(&dir, "all", n, 1);
        assert_eq!(
            seg.header().open_rows,
            vec![(0, n)],
            "every row open-ended must cost one range, not {n} row ids"
        );
        let set = ScanSet::from_pairs(vec![(&seg, Lane::Interval)]);
        let vt_s = seg.i64_column("vt_s").unwrap();
        for p in 0..=10 {
            let vt_min = vt_s[0] + (vt_s[vt_s.len() - 1] - vt_s[0]) * p / 10;
            let req = ScanRequest { vt_min: Some(vt_min), ..ScanRequest::current() };
            let (sel, _) = set.select(&req).unwrap();
            let mut got: Vec<(i64, Id96)> = sel
                .iter()
                .flat_map(|s| {
                    s.rows
                        .iter()
                        .map(|&r| (vt_s[r as usize], seg.vid_at(r as usize).unwrap()))
                })
                .collect();
            got.sort();
            assert_eq!(got, expected(&rows, &req), "vt_min={vt_min}");
        }
    }

    /// Soundness at every open-ended fraction, against the brute per-row
    /// filter that knows nothing about blocks or open-row lists. Tightness
    /// is the test above; this one is "never wrong", which is the contract.
    #[test]
    fn the_cut_never_drops_an_open_ended_row() {
        let dir = tmp("open-sound");
        let n = 5_000u32;
        for stride in [0u32, 997, 101, 7, 2] {
            let (rows, seg) = interval_segment(&dir, &format!("o{stride}"), n, stride);
            let set = ScanSet::from_pairs(vec![(&seg, Lane::Interval)]);
            let vt_s = seg.i64_column("vt_s").unwrap();
            for vt_min in [
                i64::MIN + 1, 0, 900, vt_s[0], vt_s[n as usize / 2], vt_s[n as usize - 1],
                vt_s[n as usize - 1] + 10_000, 1_000_000,
            ] {
                for vt_max in [None, Some(vt_s[n as usize / 3]), Some(i64::MAX)] {
                    let req = ScanRequest {
                        vt_min: Some(vt_min),
                        vt_max,
                        ..ScanRequest::current()
                    };
                    let (sel, _) = set.select(&req).unwrap();
                    let mut got: Vec<(i64, Id96)> = sel
                        .iter()
                        .flat_map(|s| {
                            s.rows.iter().map(|&r| {
                                (vt_s[r as usize], seg.vid_at(r as usize).unwrap())
                            })
                        })
                        .collect();
                    got.sort();
                    assert_eq!(
                        got,
                        expected(&rows, &req),
                        "stride={stride} vt_min={vt_min} vt_max={vt_max:?}"
                    );
                    // and the ranges the planner produced must still be
                    // ascending and disjoint -- the open-row add-back emits
                    // rows below the cut, which is where that could break
                    for s in &sel {
                        assert!(
                            s.rows.windows(2).all(|w| w[0] < w[1]),
                            "stride={stride}: selected rows are not strictly ascending"
                        );
                    }
                }
            }
        }
    }

    /// A header written before `open_rows` existed has no such key, and a
    /// header written before the rename has the old key names. Both must
    /// parse, and the old names must land in the new fields.
    #[test]
    fn an_old_interval_header_parses_under_the_new_names() {
        let old_json = r#"{
            "format": 1, "kind": "edge", "lane": "interval", "rows": 16,
            "block_rows": 32768, "key_lo": [0, "a"], "key_hi": [2, "b"],
            "vt_min": 0, "vt_max": 2, "vt_e_max": 3,
            "tt_s_runs": [[0, 10]], "rel_types": [], "columns": [],
            "vt_e_elided": false, "strings_offset": 0, "strings_bytes": 0,
            "strings_count": 0,
            "block_prefix_max_vt_e": [5, 9], "stair_block_rows": 8
        }"#;
        let h: crate::segment::SegmentHeader = serde_json::from_str(old_json)
            .expect("a pre-rename header must still parse");
        assert_eq!(h.block_vt_e_running_max, vec![5, 9], "the alias must carry the values");
        assert_eq!(h.vt_e_block_rows, 8);
        assert!(
            h.open_rows.is_empty(),
            "a header written before open_rows existed lists none, which is the \
             same to a reader as having none"
        );
    }

    /// An interval-lane segment carrying a staircase, with
    /// per-row `vt_e` that jitters up and down (only the *running* max is
    /// monotone). For every `vt_min` in a sweep, the row set the staircase
    /// cut lets through must equal `expected()` — the same brute per-row
    /// filter every other test in this module is checked against, which
    /// knows nothing about blocks or staircases. A regression that dropped a
    /// matching row would fail here; a regression that only failed to skip
    /// would not, because soundness (never wrong), not tightness (always
    /// minimal), is the contract.
    #[test]
    fn staircase_cut_never_drops_a_matching_row() {
        let dir = tmp("staircase-scan");
        let mut rows: Vec<EdgeRow> = (0..97u32)
            .map(|i| edge(1_000 + i as i64 * 3, i, 10, "SENT", i % 11, (i + 5) % 11))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        for (i, r) in rows.iter_mut().enumerate() {
            // a mix of short and long lives, not correlated with vt_s order,
            // so the running max genuinely climbs and plateaus rather than
            // just tracking vt_s
            let long = i % 5 == 0;
            r.vt_e = r.vt_s + if long { 5_000 } else { 50 };
        }
        let path = dir.join("0.tgs");
        write_edge_segment(
            &path,
            &rows,
            &SegmentSpec {
                lane: Lane::Interval,
                vt_e_block_rows: 8,
                ..Default::default()
            },
        )
        .unwrap();
        let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        assert!(
            !seg.header().block_vt_e_running_max.is_empty(),
            "fixture-is-real: the staircase must actually be built"
        );
        let set = ScanSet::from_pairs(vec![(&seg, Lane::Interval)]);

        // fixture-is-real, at a vt_min low enough the segment-level zone map
        // (`prunes`, which the staircase does not touch) does not short-circuit the segment
        // before the staircase is ever consulted.
        let (_, stats0) = set
            .select(&ScanRequest {
                vt_min: Some(0),
                ..ScanRequest::current()
            })
            .unwrap();
        assert!(stats0.blocks_considered > 0, "the staircase must actually be consulted");

        let mut prev_skipped = 0u64;
        for vt_min in [0i64, 900, 1_050, 1_150, 1_200, 1_260, 5_000, 1_000_000] {
            let req = ScanRequest {
                vt_min: Some(vt_min),
                ..ScanRequest::current()
            };
            let (sel, stats) = set.select(&req).unwrap();
            let got: Vec<(i64, Id96)> = sel
                .iter()
                .flat_map(|s| {
                    s.rows.iter().map(|&r| {
                        (
                            seg.i64_column("vt_s").unwrap()[r as usize],
                            seg.vid_at(r as usize).unwrap(),
                        )
                    })
                })
                .collect();
            let mut got_sorted = got.clone();
            got_sorted.sort();
            assert_eq!(got_sorted, expected(&rows, &req), "vt_min={vt_min}");
            // monotone in the cut: a stricter vt_min never skips fewer
            // blocks, as long as `prunes` (which the staircase does not touch) has not already
            // discarded the whole segment before the staircase is consulted
            // — a fully pruned segment reports zero for both, which is not a
            // regression, just a different mechanism doing the work.
            if stats.blocks_considered > 0 {
                assert!(stats.blocks_skipped >= prev_skipped, "vt_min={vt_min}");
                prev_skipped = stats.blocks_skipped;
            }
        }
    }

    /// An event request (`vt_s_min`) selects exactly the overlap rows
    /// that start at or after it, open-ended rows below the staircase cut
    /// included, and examines no row that starts before it.
    #[test]
    fn event_cut_selects_the_overlap_rows_that_start_in_the_window() {
        let dir = tmp("event-cut-scan");
        let mut rows: Vec<EdgeRow> = (0..97u32)
            .map(|i| edge(1_000 + i as i64 * 3, i, 10, "SENT", i % 11, (i + 5) % 11))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        for (i, r) in rows.iter_mut().enumerate() {
            r.vt_e = if i % 13 == 0 {
                OPEN_END
            } else {
                r.vt_s + if i % 5 == 0 { 5_000 } else { 50 }
            };
        }
        let path = dir.join("0.tgs");
        write_edge_segment(
            &path,
            &rows,
            &SegmentSpec { lane: Lane::Interval, vt_e_block_rows: 8, ..Default::default() },
        )
        .unwrap();
        let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        let set = ScanSet::from_pairs(vec![(&seg, Lane::Interval)]);
        let vt = seg.i64_column("vt_s").unwrap();
        for (t_a, t_b) in [(0i64, 2_000), (1_050, 1_080), (1_150, 1_160), (1_200, 1_400),
                           (1_288, 1_291), (5_000, 6_000)] {
            let overlap = ScanRequest { vt_min: Some(t_a), vt_max: Some(t_b), ..ScanRequest::current() };
            let event = ScanRequest { vt_s_min: Some(t_a), ..overlap.clone() };
            let (a, _) = set.select(&overlap).unwrap();
            let (b, sb) = set.select(&event).unwrap();
            let want: Vec<u32> = a.iter().flat_map(|s| s.rows.iter().copied())
                .filter(|&r| vt[r as usize] >= t_a).collect();
            let got: Vec<u32> = b.iter().flat_map(|s| s.rows.iter().copied()).collect();
            assert_eq!(got, want, "window [{t_a}, {t_b})");
            let starting = rows.iter().filter(|r| r.vt_s >= t_a && r.vt_s < t_b).count() as u64;
            assert!(sb.rows_examined <= starting, "window [{t_a}, {t_b}): examined {}", sb.rows_examined);
        }
    }

    /// An event-lane segment that stores `vt_e`, with the
    /// staircase derived at open. For a sweep of windows the derived cut
    /// lets through exactly `expected()`'s rows -- the brute per-row filter
    /// -- and the same rows as the switch-off configuration, never examines more rows
    /// than it, and does skip blocks. Open-ended rows below the cut come
    /// back through the derived open-row ranges.
    #[test]
    fn derived_event_lane_staircase_is_only_a_bound() {
        let dir = tmp("derived-staircase-scan");
        let mut rows: Vec<EdgeRow> = (0..601u32)
            .map(|i| edge(1_000 + i as i64 * 3, i, 10, "SENT", i % 11, (i + 5) % 11))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        for (i, r) in rows.iter_mut().enumerate() {
            r.vt_e = match i % 41 {
                0 => crate::OPEN_END,
                k if k % 5 == 0 => r.vt_s + 900,
                _ => r.vt_s + 7,
            };
        }
        let path = dir.join("0.tgs");
        write_edge_segment(&path, &rows, &SegmentSpec { lane: Lane::Event, ..Default::default() }).unwrap();
        let mut seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        assert!(!seg.header().vt_e_elided && seg.header().block_vt_e_running_max.is_empty());
        assert!(seg.derive_vt_e_staircase(16).unwrap(), "fixture-is-real: one is derived");
        let rows_of = |sel: &[Selection]| -> Vec<(i64, Id96)> {
            let mut v: Vec<(i64, Id96)> = sel
                .iter()
                .flat_map(|s| {
                    s.rows.iter().map(|&r| {
                        (seg.i64_column("vt_s").unwrap()[r as usize], seg.vid_at(r as usize).unwrap())
                    })
                })
                .collect();
            v.sort();
            v
        };
        let on = ScanSet::from_pairs(vec![(&seg, Lane::Event)]);
        let off = ScanSet::from_pairs(vec![(&seg, Lane::Event)]).with_designs(crate::designs::Designs {
            staircase: false,
            ..crate::designs::Designs::ALL
        });
        let mut skipped_somewhere = false;
        for (vt_min, vt_max) in [(0i64, i64::MAX), (1_500, 1_600), (2_000, 2_001), (2_500, 2_800), (2_799, 2_801), (3_000, i64::MAX)] {
            let req = ScanRequest { vt_min: Some(vt_min), vt_max: Some(vt_max), ..ScanRequest::current() };
            let (s_on, st_on) = on.select(&req).unwrap();
            let (s_off, st_off) = off.select(&req).unwrap();
            assert_eq!(rows_of(&s_on), expected(&rows, &req), "[{vt_min}, {vt_max})");
            assert_eq!(rows_of(&s_on), rows_of(&s_off), "[{vt_min}, {vt_max})");
            assert!(st_on.rows_examined <= st_off.rows_examined, "[{vt_min}, {vt_max})");
            assert_eq!(
                (st_off.derived_blocks_considered, st_off.derived_blocks_skipped),
                (0, 0),
                "off consults nothing"
            );
            assert!(st_on.derived_blocks_considered > 0, "on consults the derived staircase");
            // the written-staircase counters keep meaning the interval lane
            assert_eq!((st_on.blocks_considered, st_on.blocks_skipped), (0, 0));
            skipped_somewhere |=
                st_on.derived_blocks_skipped > 0 && st_on.rows_examined < st_off.rows_examined;
        }
        assert!(skipped_somewhere, "fixture-is-real: the derived cut must actually cut");
    }

    #[test]
    fn as_of_tt_hides_later_batches() {
        let f = Fixture::build("asof");
        let mut req = ScanRequest::current();
        req.as_of_tt = 15; // after batch 0 (tt 10), before batch 1 (tt 20)
        let got = run(&f, &req);
        assert_eq!(got, expected(&f.rows, &req));
        assert_eq!(got.len(), 200, "only the first batch was believed at tt=15");

        req.as_of_tt = 5;
        assert!(run(&f, &req).is_empty(), "nothing is believed before tt=10");
    }

    #[test]
    fn rel_type_and_incidence_filters_match_the_reference() {
        let f = Fixture::build("filters");
        let mut req = ScanRequest::current();
        req.rel_types = Some(vec!["RATED".into()]);
        assert_eq!(run(&f, &req), expected(&f.rows, &req));

        let req = ScanRequest::current().touching(vec![3, 7]);
        assert_eq!(run(&f, &req), expected(&f.rows, &req));

        let mut req = ScanRequest::current().touching(vec![3]);
        req.rel_types = Some(vec!["SENT".into()]);
        req.vt_min = Some(1_100);
        assert_eq!(run(&f, &req), expected(&f.rows, &req), "combined filters");
    }

    #[test]
    fn limit_truncates_after_ordering() {
        let f = Fixture::build("limit");
        let mut req = ScanRequest::current();
        req.limit = Some(17);
        let got = run(&f, &req);
        assert_eq!(got.len(), 17);
        assert_eq!(got, expected(&f.rows, &req));
    }

    #[test]
    fn zone_maps_prune_whole_segments_without_reading_them() {
        let f = Fixture::build("prune");
        let set = f.set();

        // a window before every row
        let (_, stats) = set.select(&ScanRequest::current().window(0, 500)).unwrap();
        assert_eq!(stats.segments_pruned, 2);
        assert_eq!(stats.rows_examined, 0, "pruned segments cost no row work");

        // an as_of before any batch was believed
        let mut req = ScanRequest::current();
        req.as_of_tt = 1;
        let (_, stats) = set.select(&req).unwrap();
        assert_eq!(stats.segments_pruned, 2);

        // a rel_type absent from the data
        let mut req = ScanRequest::current();
        req.rel_types = Some(vec!["NOPE".into()]);
        let (_, stats) = set.select(&req).unwrap();
        assert_eq!(stats.segments_pruned, 2);
    }

    #[test]
    fn binary_search_bounds_keep_row_work_proportional_to_the_window() {
        let f = Fixture::build("narrow");
        let set = f.set();
        let (_, wide) = set.select(&ScanRequest::current()).unwrap();
        let (_, narrow) = set
            .select(&ScanRequest::current().window(1_200, 1_240))
            .unwrap();
        assert_eq!(wide.rows_examined, 400);
        assert!(
            narrow.rows_examined < 60,
            "a 40-unit window examined {} rows",
            narrow.rows_examined
        );
    }

    #[test]
    fn clusters_mix_concatenation_and_local_merges() {
        // The corrected-store geometry: two overlapping segments (a bulk run
        // and its correction) plus one far-away disjoint segment. Output must
        // equal the brute-force (vt_s, vid) sort regardless of which cluster
        // path each segment took.
        let dir = tmp("clusters-mixed");
        let mut rows = Vec::new();
        let mut segs = Vec::new();
        for (name, base, n, tt) in [("a", 1_000i64, 200u32, 10i64),
                                    ("b", 1_150, 60, 20),
                                    ("c", 5_000, 80, 30)] {
            let mut b: Vec<EdgeRow> = (0..n)
                .map(|i| edge(base + i as i64, i + tt as u32 * 100, tt, "SENT",
                              i % 7, (i + 3) % 7))
                .collect();
            b.sort_by_key(|r| r.sort_key());
            let path = dir.join(format!("{name}.tgs"));
            write_edge_segment(&path, &b, &SegmentSpec::default()).unwrap();
            segs.push(Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap());
            rows.extend(b);
        }
        let set = ScanSet::from_pairs(segs.iter().map(|s| (s, Lane::Event)).collect());
        let req = ScanRequest::current();
        let (cols, _) = set.materialize_edges(&req).unwrap();
        let want = expected(&rows, &req);
        assert_eq!(cols.len(), want.len());
        for (i, (vt_s, vid)) in want.iter().enumerate() {
            assert_eq!((cols.vt_s[i], cols.vid[i]), (*vt_s, *vid), "row {i}");
        }
        // and the order list agrees with itself under a limit
        let (sel, _) = set.select(&req).unwrap();
        let full = set.merged(&sel, None).unwrap();
        let capped = set.merged(&sel, Some(37)).unwrap();
        assert_eq!(&full[..37], &capped[..]);
    }

    /// The projection reaches the fixed-width columns, not just the string
    /// ones. `columns=["vt_s"]` used to still build `vt_e`, `src_id` and
    /// `dst_id` — a pushdown that stopped three columns short, and the reason
    /// a one-column 10M scan measured the same as a four-column one.
    #[test]
    fn projection_reaches_the_fixed_width_columns() {
        let f = Fixture::build("projection");
        let req = ScanRequest {
            columns: Some(vec!["vt_s".into()]),
            ..ScanRequest::current()
        };
        let (cols, _) = f.set().materialize_edges(&req).unwrap();
        assert_eq!(cols.len(), 400);
        for (name, n) in [
            ("vt_e", cols.vt_e.len()),
            ("src_id", cols.src_id.len()),
            ("dst_id", cols.dst_id.len()),
            ("vid", cols.vid.len()),
            ("rel_type", cols.rel_type.len()),
            ("disc", cols.disc.len()),
            ("props", cols.props.len()),
        ] {
            assert_eq!(n, 0, "unprojected column '{name}' was materialized");
        }
        // …and asking for one of them brings back exactly that one
        let req = ScanRequest {
            columns: Some(vec!["vt_s".into(), "src_id".into()]),
            ..ScanRequest::current()
        };
        let (cols, _) = f.set().materialize_edges(&req).unwrap();
        assert_eq!(cols.src_id.len(), 400);
        assert!(cols.dst_id.is_empty() && cols.vt_e.is_empty());
    }

    /// Values, not just lengths: a projected scan must return the same column
    /// contents as an unprojected one, on both materialize paths.
    #[test]
    fn projected_columns_match_the_unprojected_ones() {
        let f = Fixture::build("projection-values");
        let req = ScanRequest::current().window(1_100, 1_300);
        let (full, _) = f.set().materialize_edges(&req).unwrap();
        for cols in [vec!["vt_s", "src_id"], vec!["vt_s", "vt_e", "dst_id"]] {
            let p = ScanRequest {
                columns: Some(cols.iter().map(|c| c.to_string()).collect()),
                ..req.clone()
            };
            let (got, _) = f.set().materialize_edges(&p).unwrap();
            assert_eq!(got.vt_s, full.vt_s, "vt_s under {cols:?}");
            if cols.contains(&"src_id") {
                assert_eq!(got.src_id, full.src_id);
            }
            if cols.contains(&"dst_id") {
                assert_eq!(got.dst_id, full.dst_id);
            }
            if cols.contains(&"vt_e") {
                assert_eq!(got.vt_e, full.vt_e);
            }
        }
    }

    /// Every fan-out width returns the identical columns, on a store whose
    /// clusters are a mix of singletons and a genuine multi-member merge —
    /// the two paths materialization can take. Widths are forced rather than
    /// gated, so this covers the shapes the gates would keep serial too.
    #[test]
    fn every_width_materializes_the_identical_columns() {
        let dir = tmp("width-identity");
        let mut rows = Vec::new();
        let mut segs = Vec::new();
        // eight bulk runs plus four overlapping corrections plus one far
        // segment: enough clusters to split several ways, and at least one
        // cluster that has to heap-merge
        for (name, base, n, tt) in [
            ("a", 1_000i64, 120u32, 10i64), ("b", 1_240, 120, 10),
            ("c", 1_480, 120, 10), ("d", 1_720, 120, 10),
            ("e", 1_100, 30, 20), ("f", 1_340, 30, 20),
            ("g", 1_580, 30, 20), ("h", 5_000, 60, 30),
        ] {
            let mut b: Vec<EdgeRow> = (0..n)
                .map(|i| {
                    let rel = if i % 3 == 0 { "RATED" } else { "SENT" };
                    edge(base + (i as i64) * 2, i + tt as u32 * 1000, tt, rel,
                         i % 13, (i + 4) % 13)
                })
                .collect();
            b.sort_by_key(|r| r.sort_key());
            let path = dir.join(format!("{name}.tgs"));
            write_edge_segment(&path, &b, &SegmentSpec::default()).unwrap();
            segs.push(Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap());
            rows.extend(b);
        }
        let set = ScanSet::from_pairs(segs.iter().map(|s| (s, Lane::Event)).collect());
        for req in [
            ScanRequest::current(),
            ScanRequest::current().window(1_100, 1_800),
            ScanRequest {
                columns: Some(vec!["vt_s".into(), "src_id".into()]),
                ..ScanRequest::current()
            },
        ] {
            let (serial, _, t) = set.materialize_edges_at(&req, 1).unwrap();
            assert!(t.clusters >= 3, "fixture must produce clusters to split");
            assert!(t.clusters_multi > 0, "fixture must exercise the merge path");
            // the brute-force reference, for the unprojected cases
            if req.columns.is_none() {
                let want = expected(&rows, &req);
                assert_eq!(serial.len(), want.len());
                for (i, (vt_s, vid)) in want.iter().enumerate() {
                    assert_eq!((serial.vt_s[i], serial.vid[i]), (*vt_s, *vid), "row {i}");
                }
            }
            for w in [2, 3, 5, 8, 16, 64] {
                let (got, _, _) = set.materialize_edges_at(&req, w).unwrap();
                assert_eq!(got, serial, "width {w} disagreed with serial");
            }
        }
    }

    /// Cluster ranges are contiguous, cover everything exactly once, and
    /// balance by rows rather than by cluster count.
    #[test]
    fn cluster_ranges_are_contiguous_and_row_balanced() {
        let sel = |seg: usize, n: usize| Selection {
            segment: seg,
            rows: (0..n as u32).collect(),
        };
        let selections: Vec<Selection> = (0..6).map(|i| sel(i, [900, 10, 10, 10, 10, 60][i])).collect();
        let clusters: Vec<Vec<usize>> = (0..6).map(|i| vec![i]).collect();
        for parts in [1, 2, 3, 4, 8, 100] {
            let ranges = balance_clusters(&clusters, &selections, parts);
            assert!(!ranges.is_empty());
            assert!(ranges.len() <= parts.max(1));
            assert_eq!(ranges[0].start, 0);
            assert_eq!(ranges.last().expect("non-empty").end, clusters.len());
            for w in ranges.windows(2) {
                assert_eq!(w[0].end, w[1].start, "ranges must be contiguous");
            }
        }
        // the 900-row cluster is a part on its own rather than sharing one
        let ranges = balance_clusters(&clusters, &selections, 3);
        assert_eq!(ranges[0], 0..1);
    }

    #[test]
    fn materialize_returns_every_column_in_order() {
        let f = Fixture::build("materialize");
        let req = ScanRequest::current().window(1_100, 1_200);
        let (cols, stats) = f.set().materialize_edges(&req).unwrap();
        let want = expected(&f.rows, &req);
        assert_eq!(cols.len(), want.len());
        assert_eq!(stats.rows_selected as usize, want.len());
        for (i, (vt_s, vid)) in want.iter().enumerate() {
            assert_eq!(cols.vt_s[i], *vt_s);
            assert_eq!(cols.vid[i], *vid);
            assert_eq!(cols.vt_e[i], vt_s + 1);
            assert_eq!(cols.props[i], "{}");
            assert!(cols.disc[i].starts_with('#'));
        }
    }

    /// The env override parses strictly and fails open: only a positive
    /// integer overrides, and it is clamped, never trusted for width.
    /// (Parsing is tested through the pure function — mutating the process
    /// environment inside a threaded test harness races other tests.)
    /// The recalibrated gates: rows decide,
    /// width 2–3 never engages, and the unit minimum only guards chunking.
    #[test]
    fn parallel_gate_is_row_based_and_skips_narrow_widths() {
        const ROWS: u64 = crate::defaults::PARALLEL_SCAN_MIN_ROWS;
        // a 1M-row store stays serial at every width — the measured regression
        for t in [1, 2, 4, 8, 16, 32] {
            assert!(!parallel_gate(t, 20, 8, 1_000_000), "t={t} at 1M rows");
        }
        // a 10M-row store engages from 4 workers up
        assert!(parallel_gate(4, 200, 8, 10_000_000));
        assert!(parallel_gate(16, 200, 8, 10_000_000));
        // t=2 lost to t=1 at both measured scales: never engage
        assert!(!parallel_gate(2, 200, 8, 10_000_000));
        assert!(!parallel_gate(3, 200, 8, 10_000_000));
        // nothing to chunk below the unit minimum, whatever the rows
        assert!(!parallel_gate(16, 7, 8, ROWS));
        assert!(parallel_gate(16, 8, 8, ROWS));
        // the materialize form: clusters >= 4
        assert!(parallel_gate(16, 4, 4, ROWS));
        assert!(!parallel_gate(16, 3, 4, ROWS));
    }

    /// The aggregate's own gate engages a 1M-row fold that the
    /// scan gate keeps serial, and keeps the width and unit terms exactly.
    #[test]
    fn the_aggregate_gate_engages_at_one_million_rows_and_the_scan_gate_does_not() {
        const AGG: u64 = crate::defaults::AGG_PARALLEL_MIN_ROWS;
        const SCAN: u64 = crate::defaults::PARALLEL_SCAN_MIN_ROWS;
        const { assert!(AGG < 1_000_000 && 1_000_000 < SCAN, "the gap the aggregate threshold exists for") };
        assert!(parallel_gate_at(32, 16, 2, 1_000_000, AGG));
        assert!(!parallel_gate_at(32, 16, 2, 1_000_000, SCAN));
        assert!(!parallel_gate_at(32, 16, 2, AGG - 1, AGG));
        assert!(parallel_gate_at(32, 16, 2, AGG, AGG));
        // width and units, as for the scan gate
        assert!(!parallel_gate_at(3, 16, 2, 1_000_000, AGG));
        assert!(!parallel_gate_at(32, 1, 2, 1_000_000, AGG));
        // the override parses strictly and fails open
        assert_eq!(min_rows_override(Some(" 65536 ")), Some(65_536));
        assert_eq!(min_rows_override(Some("0")), Some(0));
        for bad in ["", "-1", "1e5", "lots"] {
            assert_eq!(min_rows_override(Some(bad)), None, "{bad:?}");
        }
        assert_eq!(min_rows_override(None), None);
    }

    // ------------------------------------------------------------------ //
    // the belief predicate on a compacted layout                          //
    // ------------------------------------------------------------------ //

    /// The pre-coalescing `believed_ranges`: one output range per run. Kept
    /// as the reference the coalescing version must agree with row for row.
    fn ranges_reference(
        h: &crate::segment::SegmentHeader,
        as_of: i64,
        rows: usize,
    ) -> Vec<(usize, usize)> {
        let runs = &h.tt_s_runs;
        let mut out = Vec::new();
        for (i, (start, tt)) in runs.iter().enumerate() {
            let end = runs
                .get(i + 1)
                .map(|(s, _)| *s as usize)
                .unwrap_or(rows)
                .min(rows);
            if *tt <= as_of && (*start as usize) < end {
                out.push((*start as usize, end));
            }
        }
        out
    }

    fn flatten(ranges: &[(usize, usize)]) -> Vec<usize> {
        ranges.iter().flat_map(|&(a, b)| a..b).collect()
    }

    #[test]
    fn believed_ranges_coalesce_without_changing_the_rows() {
        let mut seed = 0x2026_0824_u64;
        let mut rng = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for case in 0..40 {
            let rows = 1 + (rng() % 300) as u32;
            // Compaction's shape: many short runs whose transaction times
            // alternate, so verdicts flip repeatedly across the segment.
            let mut runs: Vec<(u32, i64)> = Vec::new();
            let mut at = 0u32;
            while at < rows {
                runs.push((at, (rng() % 6) as i64 * 10));
                at += 1 + (rng() % 4) as u32;
            }
            let h = crate::segment::header_with_runs(runs, rows);
            let bounds = h.tt_s_bounds();
            for as_of in [-1i64, 0, 5, 10, 25, 30, 50, 99] {
                let got = believed_ranges(&tt_view(&h), bounds, as_of, rows as usize);
                assert_eq!(
                    flatten(&got),
                    flatten(&ranges_reference(&h, as_of, rows as usize)),
                    "case {case} at as_of {as_of}"
                );
                for w in got.windows(2) {
                    assert!(
                        w[0].1 < w[1].0,
                        "case {case}: adjacent ranges {:?} and {:?} were not coalesced",
                        w[0],
                        w[1]
                    );
                }
            }
        }
    }

    #[test]
    fn a_fully_believed_segment_yields_one_range_however_many_runs() {
        // The measured pathology: a compacted node segment with a run per
        // generation boundary. Current belief sees all of them, and the
        // output must be one range rather than one per run — `select_range`
        // iterates what comes back, and its contiguous-extend fast path only
        // ever fires on whole ranges.
        let rows = 5_000u32;
        let runs: Vec<(u32, i64)> = (0..rows).map(|i| (i, 100 + (i % 3) as i64)).collect();
        let h = crate::segment::header_with_runs(runs, rows);
        let bounds = h.tt_s_bounds();
        assert_eq!(bounds, (100, 102));

        let all = believed_ranges(&tt_view(&h), bounds, 102, rows as usize);
        assert_eq!(all, vec![(0, rows as usize)]);
        assert_eq!(
            believed_ranges(&tt_view(&h), bounds, i64::MAX - 1, rows as usize),
            vec![(0, rows as usize)]
        );
        // and nothing at all below the minimum
        assert!(believed_ranges(&tt_view(&h), bounds, 99, rows as usize).is_empty());
        // the mixed case still agrees with the reference, row for row
        for as_of in [100, 101] {
            assert_eq!(
                flatten(&believed_ranges(&tt_view(&h), bounds, as_of, rows as usize)),
                flatten(&ranges_reference(&h, as_of, rows as usize)),
                "as_of {as_of}"
            );
        }
    }

    #[test]
    fn believed_ranges_handle_the_degenerate_headers() {
        // no runs: nothing is believed, at any as_of
        let empty = crate::segment::header_with_runs(Vec::new(), 4);
        assert!(believed_ranges(&tt_view(&empty), empty.tt_s_bounds(), OPEN_END, 4).is_empty());
        // no rows: a run exists but covers nothing
        let none = crate::segment::header_with_runs(vec![(0, 7)], 0);
        assert!(believed_ranges(&tt_view(&none), none.tt_s_bounds(), OPEN_END, 0).is_empty());
        // runs past the row count are ignored, as the reference ignored them
        let over = crate::segment::header_with_runs(vec![(0, 7), (9, 8)], 3);
        assert_eq!(
            believed_ranges(&tt_view(&over), over.tt_s_bounds(), OPEN_END, 3),
            vec![(0, 3)]
        );
    }

    // --- close-aware verdict ranges -------------------------------------- #

    /// The per-row belief loop `select_range` ran before the closes moved
    /// into the range domain, kept verbatim as the reference the range
    /// version must agree with row for row. This is the whole evidence for
    /// "unchanged answers": if the two ever disagree on any header, any
    /// close layout or any as_of, a digest has already moved.
    fn select_rows_row_loop_reference(
        h: &crate::segment::SegmentHeader,
        tt_bounds: (i64, i64),
        as_of: i64,
        rows: usize,
        closes: &CloseIndex,
        sidecar: &crate::visibility::Sidecar<'_>,
        seg: u64,
    ) -> Vec<usize> {
        let mut out = Vec::new();
        for (a, b) in believed_ranges(&tt_view(h), tt_bounds, as_of, rows) {
            for i in a..b {
                let from_run = closes.tt_e(seg, i as u32);
                let tt_e = if from_run != OPEN_END {
                    from_run
                } else {
                    sidecar.tt_e(i as u32)
                };
                if as_of >= tt_e {
                    continue;
                }
                out.push(i);
            }
        }
        out
    }

    /// random sidecars, random close runs and a random overlay, at every
    /// as_of in the observed tt set plus the clamped OPEN_END, the range set
    /// must expand to exactly the row set the old loop selected.
    #[test]
    fn believed_ranges_closed_equals_the_row_loop() {
        let mut seed = 0x2026_0919_c105_u64;
        let mut rng = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut saw_cut = 0usize;
        for case in 0..400 {
            let rows = (rng() % 65) as u32; // 0..=64, including the empty segment
            // runs: sometimes one per row (compaction's shape), sometimes a
            // handful, sometimes none at all, sometimes starting past `rows`
            let mut runs: Vec<(u32, i64)> = Vec::new();
            match rng() % 4 {
                0 => {}
                1 => {
                    for r in 0..rows {
                        runs.push((r, (rng() % 6) as i64 * 10));
                    }
                }
                2 => {
                    let mut at = 0u32;
                    while at < rows {
                        runs.push((at, (rng() % 6) as i64 * 10));
                        at += 1 + (rng() % 5) as u32;
                    }
                }
                _ => {
                    runs.push((0, (rng() % 6) as i64 * 10));
                    runs.push((rows + 3, (rng() % 6) as i64 * 10)); // E12: past the end
                }
            }
            let h = crate::segment::header_with_runs(runs, rows);
            let bounds = h.tt_s_bounds();

            // folded sidecar: sorted, a subset of the rows
            let mut folded: Vec<(u32, i64)> = Vec::new();
            for r in 0..rows {
                if rng() % 3 == 0 {
                    folded.push((r, (rng() % 6) as i64 * 10));
                }
            }
            let sidecar = crate::visibility::Sidecar::new(&folded);

            // close runs: over rows in range, one deliberately past the end
            //, some with tt_e == OPEN_END
            let mut recs = Vec::new();
            for r in 0..rows {
                match rng() % 5 {
                    0 => recs.push(crate::visibility::CloseRecord {
                        kind: crate::row::RowKind::Edge,
                        segment: 1,
                        row: r,
                        tt_e: (rng() % 6) as i64 * 10,
                    }),
                    1 => recs.push(crate::visibility::CloseRecord {
                        kind: crate::row::RowKind::Edge,
                        segment: 1,
                        row: r,
                        tt_e: OPEN_END,
                    }),
                    _ => {}
                }
            }
            if case % 3 == 0 {
                recs.push(crate::visibility::CloseRecord {
                    kind: crate::row::RowKind::Edge,
                    segment: 1,
                    row: rows + 7, // E5: a stale or foreign run
                    tt_e: 0,
                });
            }
            let committed = std::sync::Arc::new(CloseIndex::from_records(recs));
            let closes = if case % 2 == 0 {
                let mut top = CloseIndex::layered_over(committed.clone());
                for r in 0..rows {
                    if rng() % 6 == 0 {
                        top.close_row(1, r, (rng() % 6) as i64 * 10);
                    }
                }
                top
            } else {
                CloseIndex::clone(&committed)
            };

            let view = crate::visibility::close_view(sidecar, &closes, 1);
            for as_of in [-1i64, 0, 10, 20, 30, 40, 50, crate::clamp_tt(OPEN_END)] {
                let want = select_rows_row_loop_reference(
                    &h, bounds, as_of, rows as usize, &closes, &sidecar, 1,
                );
                let got = believed_ranges_closed(&tt_view(&h), bounds, as_of, rows as usize, &view);
                assert_eq!(
                    flatten(&got),
                    want,
                    "case {case} at as_of {as_of}: rows={rows} folded={folded:?} \
                     view={:?}",
                    view.as_slice()
                );
                // nothing at all, rather than an empty (x, x) range
                for &(a, b) in &got {
                    assert!(a < b, "case {case}: empty range ({a}, {b})");
                }
                // and the output is still what select_range's contiguous
                // extend consumes: strictly ascending, disjoint, minimal
                for w in got.windows(2) {
                    assert!(
                        w[0].1 < w[1].0,
                        "case {case}: ranges {:?} and {:?} are not separated",
                        w[0],
                        w[1]
                    );
                }
                if flatten(&believed_ranges(&tt_view(&h), bounds, as_of, rows as usize)).len() > want.len() {
                    saw_cut += 1;
                }
            }
        }
        // the fixture has to actually exercise the subtraction, or every
        // assertion above is an assertion about nothing
        assert!(
            saw_cut > 200,
            "only {saw_cut} of the cases cut a row: the fixture stopped producing closes"
        );
    }

    /// The two subtractions partition the written rows.
    ///
    /// `version_history`'s `Belief::Superseded` mode is defined as the
    /// complement of `Current` within `All`, and the only reason it can be
    /// answered by a range list at all is that
    /// [`believed_ranges_superseded`] keeps exactly what
    /// [`subtract_closes`] drops. That is a property of the two cursor
    /// walks, so it is asserted on the same randomized fixture the
    /// close-aware ranges are: the same headers, the same folded sidecars,
    /// the same close runs, the same batch overlay, at every as_of in the
    /// observed tt set.
    ///
    /// Per segment, per as_of, the two claims the brief names:
    ///
    /// - `believed ∪ superseded == believed-at-any-time` — nothing the base
    ///   admits is lost by either side;
    /// - `believed ∩ superseded == ∅` — and nothing is claimed by both.
    ///
    /// Union is asserted on the flattened row sets rather than the range
    /// lists, because the two lists are each separately minimal and their
    /// union need not be: a cut row sits between two kept ranges, so
    /// merging them yields adjacent ranges the base carries as one.
    #[test]
    fn believed_and_superseded_partition_the_written_rows() {
        let mut seed = 0x2026_0924_0d18_u64;
        let mut rng = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut saw_superseded = 0usize;
        for case in 0..400 {
            let rows = (rng() % 65) as u32;
            let mut runs: Vec<(u32, i64)> = Vec::new();
            match rng() % 4 {
                0 => {}
                1 => {
                    for r in 0..rows {
                        runs.push((r, (rng() % 6) as i64 * 10));
                    }
                }
                2 => {
                    let mut at = 0u32;
                    while at < rows {
                        runs.push((at, (rng() % 6) as i64 * 10));
                        at += 1 + (rng() % 5) as u32;
                    }
                }
                _ => {
                    runs.push((0, (rng() % 6) as i64 * 10));
                    runs.push((rows + 3, (rng() % 6) as i64 * 10));
                }
            }
            let h = crate::segment::header_with_runs(runs, rows);
            let bounds = h.tt_s_bounds();

            let mut folded: Vec<(u32, i64)> = Vec::new();
            for r in 0..rows {
                if rng() % 3 == 0 {
                    folded.push((r, (rng() % 6) as i64 * 10));
                }
            }
            let sidecar = crate::visibility::Sidecar::new(&folded);

            let mut recs = Vec::new();
            for r in 0..rows {
                match rng() % 5 {
                    0 => recs.push(crate::visibility::CloseRecord {
                        kind: crate::row::RowKind::Edge,
                        segment: 1,
                        row: r,
                        tt_e: (rng() % 6) as i64 * 10,
                    }),
                    1 => recs.push(crate::visibility::CloseRecord {
                        kind: crate::row::RowKind::Edge,
                        segment: 1,
                        row: r,
                        tt_e: OPEN_END,
                    }),
                    _ => {}
                }
            }
            if case % 3 == 0 {
                // a stale or foreign run, which neither function may let corrupt
                // its walk
                recs.push(crate::visibility::CloseRecord {
                    kind: crate::row::RowKind::Edge,
                    segment: 1,
                    row: rows + 7,
                    tt_e: 0,
                });
            }
            let committed = std::sync::Arc::new(CloseIndex::from_records(recs));
            let closes = if case % 2 == 0 {
                let mut top = CloseIndex::layered_over(committed.clone());
                for r in 0..rows {
                    if rng() % 6 == 0 {
                        top.close_row(1, r, (rng() % 6) as i64 * 10);
                    }
                }
                top
            } else {
                CloseIndex::clone(&committed)
            };

            let view = crate::visibility::close_view(sidecar, &closes, 1);
            let n = rows as usize;
            for as_of in [-1i64, 0, 10, 20, 30, 40, 50, crate::clamp_tt(OPEN_END)] {
                let any = believed_ranges_any(&tt_view(&h), bounds, as_of, n);
                let believed = believed_ranges_closed(&tt_view(&h), bounds, as_of, n, &view);
                let superseded = believed_ranges_superseded(&tt_view(&h), bounds, as_of, n, &view);

                let (fa, fb, fs) = (
                    flatten(&any),
                    flatten(&believed),
                    flatten(&superseded),
                );
                let mut union = fb.clone();
                union.extend(fs.iter().copied());
                union.sort_unstable();
                assert_eq!(
                    union, fa,
                    "case {case} at as_of {as_of}: believed ∪ superseded is not \
                     the written rows; rows={rows} view={:?}",
                    view.as_slice()
                );

                let believed_set: std::collections::BTreeSet<usize> =
                    fb.iter().copied().collect();
                assert!(
                    fs.iter().all(|r| !believed_set.contains(r)),
                    "case {case} at as_of {as_of}: a row is both believed and \
                     superseded; believed={fb:?} superseded={fs:?}"
                );
                // union already forces `|believed| + |superseded| == |any|`,
                // so disjointness plus that equality is the partition — but
                // state it, because a duplicate inside one side would satisfy
                // neither on its own
                assert_eq!(fb.len() + fs.len(), fa.len());

                // the superseded side owes `version_keys` the same contract
                // its sibling owes `select_range`: no empty range, strictly
                // ascending, and separated (coalesced) rather than adjacent
                for &(a, b) in &superseded {
                    assert!(a < b, "case {case}: empty range ({a}, {b})");
                }
                for w in superseded.windows(2) {
                    assert!(
                        w[0].1 < w[1].0,
                        "case {case}: superseded ranges {:?} and {:?} are adjacent \
                         or overlapping",
                        w[0],
                        w[1]
                    );
                }
                if !fs.is_empty() {
                    saw_superseded += 1;
                }
            }
        }
        // The fixture has to actually supersede rows, or every
        // assertion above is an assertion about the empty set
        assert!(
            saw_superseded > 200,
            "only {saw_superseded} of the cases superseded a row: the fixture \
             stopped producing closes"
        );
    }

    #[test]
    fn believed_ranges_superseded_is_empty_without_closes() {
        // The sibling's counterpart to `believed_ranges_closed_is_identity_
        // without_closes`: an uncorrected segment supersedes nothing, and
        // says so without allocating.
        let out = retain_closes(vec![(0usize, 10usize), (20, 30)], 5, &[]);
        assert!(out.is_empty());
        assert_eq!(out.capacity(), 0, "the empty answer allocated");
    }

    /// The superseded/current partition holds on a
    /// **dictionary-coded** segment, and is the same partition the run-list
    /// shape of the very same rows produces.
    ///
    /// `believed_ranges_any`/`_closed`/`_superseded` read transaction time
    /// only through `TtSView`. On a dictionary-coded segment the header's
    /// `tt_s_runs` is empty, so any path that still read the header would take
    /// the run-list branch over nothing and return no rows — and the partition
    /// test on header-built fixtures could not see it, because those fixtures
    /// are never dictionary-coded. So this one writes a real segment, and
    /// FIRST asserts it took the dictionary path; a fixture that silently fell
    /// back to runs would make every assertion below one about the other shape.
    ///
    /// Three claims per (shape, as_of), each against a different oracle:
    /// - partition: `current ∪ superseded == any`, `current ∩ superseded == ∅`;
    /// - row oracle: `superseded` is exactly the rows with `tt_s <= as_of` and
    ///   `tt_e <= as_of`, read row by row through `TtSView::at` and the close;
    /// - shape oracle: the dictionary view and a run-list view built from the
    ///   same rows give byte-identical `any`, `current` and `superseded`.
    #[test]
    fn superseded_partition_holds_on_a_dictionary_coded_segment() {
        let dir = tmp("dictpart-dict-partition");
        type Shape = (&'static str, u32, fn(u32) -> i64);
        let shapes: Vec<Shape> = vec![
            ("alternating", 300, |i| 10 + (i % 3) as i64 * 10),
            ("every-row-distinct", 400, |i| 100 + i as i64),
            ("sawtooth", 600, |i| 20 + (i % 17) as i64),
            ("descending", 260, |i| 1_000 - (i / 3) as i64),
        ];
        let mut saw_superseded = 0usize;
        for (name, n, tt_of) in shapes {
            let mut rows: Vec<EdgeRow> = (0..n)
                .map(|i| edge(1_000 + i as i64, i, tt_of(i), "SENT", i % 5, (i + 2) % 5))
                .collect();
            rows.sort_by_key(|r| r.sort_key());
            let path = dir.join(format!("{name}.tgs"));
            write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
            let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
            let tt = seg.tt_s_view().unwrap();

            // the fixture must actually be what the test is about
            assert!(tt.is_dictionary(), "{name}: fixture did not take the dictionary path");
            assert!(
                seg.header().tt_s_runs.is_empty(),
                "{name}: dictionary-coded segments carry no header runs"
            );

            // the run-list shape of the same rows, for the shape oracle
            let mut runs: Vec<(u32, i64)> = Vec::new();
            for (i, r) in rows.iter().enumerate() {
                if runs.last().map(|(_, t)| *t) != Some(r.tt_s) {
                    runs.push((i as u32, r.tt_s));
                }
            }
            let oracle_h = crate::segment::header_with_runs(runs, n);
            let oracle_tt = tt_view(&oracle_h);
            let bounds = seg.tt_s_bounds();
            assert_eq!(bounds, oracle_h.tt_s_bounds(), "{name}: folded bounds differ");

            // closes on every 4th row with tt_e spread across the as_of sweep,
            // plus every 7th row closed "later than any as_of" (never superseded)
            let seg_id = 42u64;
            let mut recs = Vec::new();
            for r in 0..n {
                let tt_e = if r % 4 == 0 {
                    Some(rows[r as usize].tt_s + 1 + (r % 3) as i64 * 5)
                } else if r % 7 == 0 {
                    Some(i64::MAX / 4)
                } else {
                    None
                };
                if let Some(tt_e) = tt_e {
                    recs.push(crate::visibility::CloseRecord {
                        kind: crate::row::RowKind::Edge,
                        segment: seg_id,
                        row: r,
                        tt_e,
                    });
                }
            }
            let closes = CloseIndex::from_records(recs);
            let view = crate::visibility::close_view(seg.sidecar(), &closes, seg_id);

            let (lo, hi) = bounds;
            let mut probes: Vec<i64> = vec![lo - 1, lo, (lo + hi) / 2, hi, hi + 50];
            probes.extend((0..6).map(|k| lo + (hi - lo) * k / 5));
            for as_of in probes {
                let nu = n as usize;
                let any = believed_ranges_any(&tt, bounds, as_of, nu);
                let cur = believed_ranges_closed(&tt, bounds, as_of, nu, &view);
                let sup = believed_ranges_superseded(&tt, bounds, as_of, nu, &view);
                let (fa, fc, fs) = (flatten(&any), flatten(&cur), flatten(&sup));

                // partition
                let mut u = fc.clone();
                u.extend(fs.iter().copied());
                u.sort_unstable();
                assert_eq!(u, fa, "{name} as_of={as_of}: current ∪ superseded != any");
                let cs: std::collections::BTreeSet<usize> = fc.iter().copied().collect();
                assert!(fs.iter().all(|r| !cs.contains(r)), "{name} as_of={as_of}: overlap");

                // row oracle, through the view (never the header)
                let want_sup: Vec<usize> = (0..nu)
                    .filter(|&r| {
                        let t = tt.at(r as u32).unwrap();
                        let e = closes.tt_e(seg_id, r as u32);
                        t <= as_of && e != OPEN_END && e <= as_of
                    })
                    .collect();
                assert_eq!(fs, want_sup, "{name} as_of={as_of}: superseded != row oracle");

                // shape oracle: dictionary and run-list agree exactly
                assert_eq!(any, believed_ranges_any(&oracle_tt, bounds, as_of, nu), "{name} as_of={as_of}: any");
                assert_eq!(cur, believed_ranges_closed(&oracle_tt, bounds, as_of, nu, &view), "{name} as_of={as_of}: current");
                assert_eq!(sup, believed_ranges_superseded(&oracle_tt, bounds, as_of, nu, &view), "{name} as_of={as_of}: superseded");

                if !fs.is_empty() {
                    saw_superseded += 1;
                }
            }
        }
        // The sweep has to actually supersede rows on the dictionary path
        assert!(saw_superseded >= 8, "only {saw_superseded} probes superseded anything");
    }

    #[test]
    fn adjacent_superseded_rows_coalesce_into_one_range() {
        // The one behaviour `subtract_closes` never has to have: two closes
        // on consecutive rows are one range out, not two.
        let out = retain_closes(vec![(0usize, 8usize)], 50, &[(2, 10), (3, 10), (6, 10)]);
        assert_eq!(out, vec![(2, 4), (6, 7)]);
        // and a close whose belief has not ended by `as_of` is not
        // superseded at all
        let out = retain_closes(vec![(0usize, 8usize)], 5, &[(2, 10), (3, 10)]);
        assert!(out.is_empty());
    }

    #[test]
    fn believed_ranges_closed_is_identity_without_closes() {
        // hoped for: with no closes the base vector itself comes back, same
        // allocation, so the all_current path does no work it did not do
        // before.
        let base = vec![(0usize, 10usize), (20, 30)];
        let ptr = base.as_ptr();
        let cap = base.capacity();
        let out = subtract_closes(base, 5, &[]);
        assert!(std::ptr::eq(out.as_ptr(), ptr), "the base vector was copied");
        assert_eq!(out.capacity(), cap);
        assert_eq!(out, vec![(0, 10), (20, 30)]);

        // and end to end, through the view an uncorrected segment produces
        let h = crate::segment::header_with_runs(vec![(0, 10)], 8);
        let empty = CloseIndex::default();
        let view = crate::visibility::close_view(crate::visibility::Sidecar::new(&[]), &empty, 1);
        assert!(view.is_empty());
        assert_eq!(
            believed_ranges_closed(&tt_view(&h), h.tt_s_bounds(), OPEN_END, 8, &view),
            believed_ranges(&tt_view(&h), h.tt_s_bounds(), OPEN_END, 8),
        );
    }

    #[test]
    fn believed_ranges_closed_emits_no_empty_ranges() {
        let h = crate::segment::header_with_runs(vec![(0, 10)], 4);
        let folded: Vec<(u32, i64)> = (0..4).map(|r| (r, 20i64)).collect();
        let empty = CloseIndex::default();
        let view = crate::visibility::close_view(
            crate::visibility::Sidecar::new(&folded),
            &empty,
            1,
        );
        assert!(believed_ranges_closed(&tt_view(&h), h.tt_s_bounds(), 30, 4, &view).is_empty());
        // one row short of everything: a single range, not two empties
        let folded: Vec<(u32, i64)> = [0u32, 1, 3].iter().map(|r| (*r, 20i64)).collect();
        let view = crate::visibility::close_view(
            crate::visibility::Sidecar::new(&folded),
            &empty,
            1,
        );
        assert_eq!(
            believed_ranges_closed(&tt_view(&h), h.tt_s_bounds(), 30, 4, &view),
            vec![(2, 3)]
        );
        // and below the closes' tt_e nothing is cut at all
        assert_eq!(
            believed_ranges_closed(&tt_view(&h), h.tt_s_bounds(), 19, 4, &view),
            vec![(0, 4)]
        );
    }

    #[test]
    fn closes_outside_the_believed_ranges_are_ignored() {
        // id past the end of the segment. Neither may cut anything, panic,
        // or desynchronize the cursor for the ranges that follow.
        let h = crate::segment::header_with_runs(vec![(0, 10), (2, 99), (4, 10)], 6);
        let bounds = h.tt_s_bounds();
        // at as_of 10 the base is (0,2) and (4,6) — rows 2 and 3 are unbelieved
        assert_eq!(believed_ranges(&tt_view(&h), bounds, 10, 6), vec![(0, 2), (4, 6)]);
        let folded: Vec<(u32, i64)> = vec![(2, 5), (3, 5), (5, 5), (11, 5)];
        let empty = CloseIndex::default();
        let view = crate::visibility::close_view(
            crate::visibility::Sidecar::new(&folded),
            &empty,
            1,
        );
        // the closes in the gap and past the end are stepped over; only the
        // one inside a base range cuts, and the cursor still finds it
        assert_eq!(
            believed_ranges_closed(&tt_view(&h), bounds, 10, 6, &view),
            vec![(0, 2), (4, 5)]
        );
    }

    #[test]
    fn as_of_at_the_clamped_open_end_behaves_exactly_as_before() {
        // tt_e is OPEN_END is kept and one whose tt_e is OPEN_END - 1 is
        // cut. Pinned because it is the boundary every current-belief query
        // in the system runs at.
        let h = crate::segment::header_with_runs(vec![(0, 10)], 3);
        let folded = [(0u32, OPEN_END), (1, OPEN_END - 1), (2, OPEN_END)];
        let empty = CloseIndex::default();
        let view = crate::visibility::close_view(
            crate::visibility::Sidecar::new(&folded),
            &empty,
            1,
        );
        let as_of = crate::clamp_tt(OPEN_END);
        assert_eq!(as_of, OPEN_END - 1);
        assert_eq!(
            believed_ranges_closed(&tt_view(&h), h.tt_s_bounds(), as_of, 3, &view),
            vec![(0, 1), (2, 3)]
        );
        assert_eq!(
            flatten(&believed_ranges_closed(&tt_view(&h), h.tt_s_bounds(), as_of, 3, &view)),
            select_rows_row_loop_reference(
                &h,
                h.tt_s_bounds(),
                as_of,
                3,
                &empty,
                &crate::visibility::Sidecar::new(&folded),
                1
            ),
        );
    }

    /// The central equivalence: on a segment that codes `tt_s` by
    /// dictionary, `believed_ranges` must return the **same ranges** the run
    /// list would have returned — not merely the same rows.
    ///
    /// The ranges, not the rows, because `select_range`'s contiguous-extend
    /// fast path consumes them: an answer that is right row by row but
    /// fragmented where the run list coalesced would be a silent 2–7×
    /// regression rather than a wrong result. The oracle is the run list the
    /// same rows produce, fed through the same function, so the two sides
    /// differ in representation and in nothing else.
    ///
    /// The shapes swept are the ones that can break the block logic: times
    /// that alternate every row (every block straddles every cut), times
    /// that step in long plateaus (blocks are wholly one side), a single
    /// distinct time (the dictionary degenerates to one entry), and times
    /// that descend with the row order (nothing about the coding assumes
    /// `tt_s` ascends — compaction guarantees it does not).
    #[test]
    fn dictionary_believed_ranges_equal_the_run_derived_ranges() {
        let dir = tmp("dict-equiv");
        // (name, rows, tt_s of row i) — each must cross the 1/16 run density
        /// (name, rows, tt_s of row i)
        type Shape = (&'static str, u32, fn(u32) -> i64);
        let shapes: Vec<Shape> = vec![
            ("alternating", 300, |i| 10 + (i % 3) as i64 * 10),
            ("every-row-distinct", 400, |i| 100 + i as i64),
            ("plateaus", 500, |i| 10 + (i / 7) as i64),
            ("two-values", 200, |i| if i % 2 == 0 { 5 } else { 9 }),
            ("descending", 260, |i| 1_000 - (i / 3) as i64),
            ("sawtooth", 600, |i| 20 + (i % 17) as i64),
            ("one-long-run-then-noise", 300, |i| {
                if i < 250 { 7 } else { 7 + (i % 11) as i64 }
            }),
        ];
        for (name, n, tt_of) in shapes {
            let mut rows: Vec<EdgeRow> = (0..n)
                .map(|i| edge(1_000 + i as i64, i, tt_of(i), "SENT", i % 5, (i + 2) % 5))
                .collect();
            rows.sort_by_key(|r| r.sort_key());
            let path = dir.join(format!("{name}.tgs"));
            write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
            let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
            let tt = seg.tt_s_view().unwrap();
            assert!(
                tt.is_dictionary(),
                "{name}: the fixture must actually take the dictionary path"
            );
            assert!(
                seg.header().tt_s_runs.is_empty(),
                "{name}: the two representations must be mutually exclusive"
            );

            // the oracle: the run list these very rows would have produced,
            // read by the very same function
            let mut runs: Vec<(u32, i64)> = Vec::new();
            for (i, r) in rows.iter().enumerate() {
                if runs.last().map(|(_, tt)| *tt) != Some(r.tt_s) {
                    runs.push((i as u32, r.tt_s));
                }
            }
            assert!(
                runs.len() * 16 >= rows.len(),
                "{name}: {} runs over {} rows is below the dictionary threshold",
                runs.len(),
                rows.len()
            );
            let oracle = crate::segment::header_with_runs(runs, n);
            assert_eq!(
                seg.tt_s_bounds(),
                oracle.tt_s_bounds(),
                "{name}: folded bounds disagree"
            );
            assert_eq!(
                tt.run_count(),
                oracle.tt_s_runs.len(),
                "{name}: the run-count census must survive the representation change"
            );

            // every distinct tt_s, and the gaps either side of each
            let mut probes: Vec<i64> = rows.iter().map(|r| r.tt_s).collect();
            probes.sort_unstable();
            probes.dedup();
            let mut as_ofs: Vec<i64> = probes
                .iter()
                .flat_map(|&t| [t - 1, t, t + 1])
                .collect();
            as_ofs.extend([i64::MIN, -1, 0, OPEN_END - 1, i64::MAX]);
            for as_of in as_ofs {
                let want = believed_ranges(&tt_view(&oracle), oracle.tt_s_bounds(), as_of, n as usize);
                let got = believed_ranges(&tt, seg.tt_s_bounds(), as_of, n as usize);
                assert_eq!(got, want, "{name}: ranges differ at as_of={as_of}");
                // and the rows themselves, so a coincidence of shape cannot
                // pass for an equality of content
                for (a, b) in &got {
                    for (row, r) in rows.iter().enumerate().take(*b).skip(*a) {
                        assert!(
                            r.tt_s <= as_of,
                            "{name}: row {row} (tt_s {}) is not believed at {as_of}",
                            r.tt_s
                        );
                    }
                }
                let n_believed: usize = got.iter().map(|(a, b)| b - a).sum();
                assert_eq!(
                    n_believed,
                    rows.iter().filter(|r| r.tt_s <= as_of).count(),
                    "{name}: believed row count differs at as_of={as_of}"
                );
            }
        }
    }

    /// The dictionary path must not fragment what the run list coalesced.
    ///
    /// Stated separately from the equality above because it is the property
    /// that costs latency rather than correctness, and because the block
    /// granularity is exactly where it could be lost: a believed range that
    /// spans a block boundary has to be extended across it, not restarted.
    #[test]
    fn dictionary_ranges_are_maximal_across_block_boundaries() {
        let dir = tmp("dict-coalesce");
        // more rows than one tt_s code block (4096), with a single unbelieved
        // row planted in the middle so the answer is exactly two ranges
        let n = 12_000u32;
        let hole = 6_000u32;
        let mut rows: Vec<EdgeRow> = (0..n)
            .map(|i| {
                let tt = if i == hole { 99 } else { 10 + (i % 3) as i64 };
                edge(1_000 + i as i64, i, tt, "SENT", i % 5, (i + 2) % 5)
            })
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        let path = dir.join("coalesce.tgs");
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        let tt = seg.tt_s_view().unwrap();
        assert!(tt.is_dictionary());
        assert!(
            seg.header().tt_s_code_blocks.len() > 2,
            "the fixture must span several code blocks: {} block(s)",
            seg.header().tt_s_code_blocks.len()
        );
        let got = believed_ranges(&tt, seg.tt_s_bounds(), 50, n as usize);
        assert_eq!(
            got,
            vec![(0, hole as usize), (hole as usize + 1, n as usize)],
            "one unbelieved row must cut exactly one hole, not one per block"
        );
    }

    /// End to end on a real segment whose transaction times alternate row by
    /// row — the layout compaction produces and no batch can write. Selection
    /// must match the brute-force reference at every belief time.
    #[test]
    fn a_segment_with_interleaved_transaction_times_selects_correctly() {
        let dir = tmp("interleaved-tt");
        let mut rows: Vec<EdgeRow> = (0..300u32)
            .map(|i| edge(1_000 + i as i64, i, 10 + (i % 3) as i64 * 10, "SENT", i % 5, (i + 2) % 5))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        let path = dir.join("mixed.tgs");
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        assert!(
            seg.tt_s_run_count() > 200,
            "the fixture must actually interleave: {} runs",
            seg.tt_s_run_count()
        );
        assert_eq!(seg.tt_s_bounds(), (10, 30));

        let f = Fixture { rows, segs: vec![seg] };
        for as_of in [5i64, 10, 15, 20, 25, 30, OPEN_END] {
            for window in [None, Some((1_050i64, 1_200i64))] {
                let mut req = ScanRequest::current();
                req.as_of_tt = as_of;
                if let Some((a, b)) = window {
                    req = req.window(a, b);
                }
                assert_eq!(
                    run(&f, &req),
                    expected(&f.rows, &req),
                    "as_of {as_of}, window {window:?}"
                );
            }
        }
        // nothing is believed before the first transaction: the segment prunes
        let mut early = ScanRequest::current();
        early.as_of_tt = 9;
        let (_, stats) = f.set().select(&early).unwrap();
        assert_eq!(stats.segments_pruned, 1);
    }

    #[test]
    fn scan_threads_override_parses_strictly() {
        assert_eq!(scan_threads_override(None), None);
        assert_eq!(scan_threads_override(Some("")), None);
        assert_eq!(scan_threads_override(Some("0")), None);
        assert_eq!(scan_threads_override(Some("-4")), None);
        assert_eq!(scan_threads_override(Some("many")), None);
        assert_eq!(scan_threads_override(Some("1")), Some(1));
        assert_eq!(scan_threads_override(Some(" 8 ")), Some(8));
        assert_eq!(scan_threads_override(Some("32")), Some(32));
        assert_eq!(scan_threads_override(Some("4096")), Some(64));
    }

    /// The default width is the machine's parallelism, uncapped (an
    /// earlier version clamped it to 16), and the probe is cached.
    #[test]
    fn default_width_is_every_available_core() {
        let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        assert_eq!(available_cores(), cores);
        assert_eq!(available_cores(), available_cores());
        if std::env::var("GNOMON_SCAN_THREADS").is_err() {
            assert_eq!(scan_threads(), cores);
        }
    }
}
