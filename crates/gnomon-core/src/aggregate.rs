//! Grouped aggregation over edge events (`aggregate_events`).
//!
//! Two-phase parallel aggregation, in the style columnar engines such as
//! ClickHouse use: per-thread partial states built over the cluster-parallel
//! scan's per-segment selections, then a deterministic merge. Group keys are
//! **fixed-width codes end-to-end** — bucket index `i64`, global rel code,
//! endpoint dense id `u32`, label code — never strings; names rehydrate only
//! at output, and canonical ordering is computed on integer ranks.
//!
//! Determinism is by construction rather than by discipline: every partial
//! state is integer-valued and its merge is commutative (`count` and `sum`
//! add, `min`/`max` fold, distinct sets are id-vector *appends* that are
//! sorted and deduplicated exactly once at finalize), so the merged result
//! is byte-identical at any thread count — tested the way `scan.rs` tests
//! order, against a brute-force reference and across widths.
//!
//! The mean is the one place a float exists, and it is produced from the
//! exact integer sum by `q = s div n; q as f64 + (r as f64) / (n as f64)` —
//! the identical IEEE sequence as the Python fallback's `divmod` form, so
//! the two paths agree bit-for-bit (`ops_aggregate._mean`).

use std::collections::{BTreeSet, HashMap};

use crate::error::Result;
use crate::row::Lane;
use crate::scan::{ScanRequest, ScanSet, ScanTarget, Selection};
use crate::segment::{Segment, SegmentSource};
use crate::store::{segment_id_of, NativeStore};
use crate::{EngineError, OPEN_END};

/// One grouping dimension, already reduced to what the kernel needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dim {
    /// Bucket index of `vt_s` over `[t_a, t_b)` at `stride`.
    TimeBucket { stride: i64 },
    /// Global rel code (index into the sorted global rel-name table).
    RelType,
    /// Endpoint dense id; `dst` selects which end.
    Endpoint { dst: bool },
    /// Label code of the endpoint's believed node version valid at the
    /// event's `vt_s`; `-1` when none (sorts first, as the contract's
    /// null-first ordering).
    Label { dst: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    VtS,
    /// `vt_e - vt_s`; rows with `vt_e = OPEN_END` contribute nothing.
    Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Agg {
    Count,
    CountDistinct { dst: bool },
    Min(Source),
    Max(Source),
    Mean(Source),
}

#[derive(Clone, Debug)]
pub struct AggregateRequest {
    pub as_of_tt: i64,
    /// Events are believed edge versions with `t_a <= vt_s < t_b`.
    pub t_a: i64,
    pub t_b: i64,
    pub rel_types: Option<Vec<String>>,
    pub dims: Vec<Dim>,
    pub aggs: Vec<Agg>,
    /// Emitted-group cap; exceeding it is a `capacity` error naming both.
    pub max_groups: usize,
}

/// One aggregate's output column, aligned with the sorted group rows.
#[derive(Clone, Debug, PartialEq)]
pub enum AggValues {
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateOut {
    /// One code column per requested dimension, in canonical group order.
    pub keys: Vec<Vec<i64>>,
    /// One column per requested aggregate, same order as the request.
    pub aggs: Vec<AggValues>,
    /// Global rel code -> name (sorted, so code order is name order).
    pub rel_names: Vec<String>,
    /// Label code -> name (sorted; `-1` in a key column means null).
    pub label_names: Vec<String>,
    /// How much of the manifest this call actually had to open.
    /// Appended after the four answer fields, so nothing that constructs or
    /// reads this struct positionally is disturbed.
    pub stats: AggScanStats,
}

/// How selective the aggregate's manifest-level prune was on one call
/// (see the segment-count/latency note).
///
/// The same argument [`crate::scan::ScanStats`] carries for the scan path:
/// without a counter, "a narrow window opens few segments" is an assumption
/// rather than an observation — and it is precisely the assumption that
/// stopped holding when the open loop ran ahead of every prune.
/// `segments_pruned + segments_opened == segments_total` by construction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AggScanStats {
    /// Edge-lane segments the manifest named (both lanes).
    pub segments_total: usize,
    /// Discarded on manifest bounds alone — never mapped, never decoded.
    pub segments_pruned: usize,
    /// Opened (cached) and handed to `ScanSet::select`.
    pub segments_opened: usize,
    /// Rows the verdict ranges handed to the fold — the other half of read
    /// amplification beside `segments_opened`. Two layouts of the same store
    /// can open different numbers of segments and still walk the same rows;
    /// only reporting both tells those cases apart.
    pub rows_selected: u64,
}

/// How a group remembers which endpoints it has seen.
///
/// Endpoint ids are *dense* `u32`, so a group's distinct set is a subset of
/// `0..n_entities` and a bitset over that space answers `count_distinct`
/// with a popcount instead of a sort. A bitset per group is a capacity
/// hazard, though — the endpoint dimension can emit one group per entity —
/// so a group starts as an id vector and is promoted only once that vector
/// occupies as many bytes as the bitset would (`promote_at`). Two bounds
/// follow, and both are independent of the group cap:
///
/// * a promoted group never costs more than twice what the append-only path
///   was already paying for it at the moment of promotion;
/// * at most `rows / promote_at` groups can promote, so total distinct state
///   is at most **8 bytes per selected row per thread** — twice the 4 bytes
///   per row the id-append path costs, whatever the entity count.
///
/// Both forms answer a set cardinality, so the merged result is
/// order-independent and therefore byte-identical at any thread count; the
/// merge of two bitsets is a commutative OR.
#[derive(Clone, Debug)]
enum Distinct {
    Ids(Vec<u32>),
    Bits(Vec<u64>),
}

/// Bitset geometry for one call, derived from the dense id space.
#[derive(Clone, Copy, Debug)]
pub struct DistinctPlan {
    words: usize,
    promote_at: usize,
}

impl DistinctPlan {
    /// `n_entities` is the dictionary's dense id space; ids are `< n_entities`.
    pub fn for_space(n_entities: u32) -> Self {
        let words = (n_entities as usize).div_ceil(64).max(1);
        Self {
            words,
            // 8 bytes per word against 4 bytes per appended id
            promote_at: words * 2,
        }
    }
}

fn set_bit(w: &mut Vec<u64>, id: u32) {
    let i = (id >> 6) as usize;
    // dense ids are `< dict.len()`, so this never grows in practice; the
    // check is here because silently dropping an id would be a wrong answer
    // and a panic across the PyO3 boundary is the worst failure shape there
    if i >= w.len() {
        w.resize(i + 1, 0);
    }
    w[i] |= 1u64 << (id & 63);
}

impl Distinct {
    fn insert(&mut self, id: u32, plan: &DistinctPlan) {
        match self {
            Distinct::Ids(v) => {
                v.push(id);
                if v.len() >= plan.promote_at {
                    let mut w = vec![0u64; plan.words];
                    for &x in v.iter() {
                        set_bit(&mut w, x);
                    }
                    *self = Distinct::Bits(w);
                }
            }
            Distinct::Bits(w) => set_bit(w, id),
        }
    }

    fn merge(&mut self, other: Distinct) {
        match (&mut *self, other) {
            (Distinct::Ids(a), Distinct::Ids(mut b)) => a.append(&mut b),
            (Distinct::Bits(a), Distinct::Bits(b)) => {
                if b.len() > a.len() {
                    a.resize(b.len(), 0);
                }
                for (x, y) in a.iter_mut().zip(b.iter()) {
                    *x |= *y;
                }
            }
            (Distinct::Bits(a), Distinct::Ids(b)) => {
                for id in b {
                    set_bit(a, id);
                }
            }
            (Distinct::Ids(a), Distinct::Bits(mut w)) => {
                for &id in a.iter() {
                    set_bit(&mut w, id);
                }
                *self = Distinct::Bits(w);
            }
        }
    }

    fn cardinality(&self) -> i64 {
        match self {
            Distinct::Ids(v) => {
                let mut ids = v.clone();
                ids.sort_unstable();
                ids.dedup();
                ids.len() as i64
            }
            Distinct::Bits(w) => w.iter().map(|x| x.count_ones() as i64).sum(),
        }
    }
}

/// Per-group partial state. Shapes follow the request: `distinct[i]` and
/// `stats[j]` align with the distinct/stat aggregates in request order.
#[derive(Clone, Debug)]
pub(crate) struct GroupState {
    count: u64,
    /// Seen-endpoint sets, one per `count_distinct` aggregate.
    distinct: Vec<Distinct>,
    /// `(min, max, sum, n)` per stat aggregate.
    stats: Vec<(i64, i64, i128, u64)>,
}

impl GroupState {
    fn new(n_distinct: usize, n_stats: usize) -> Self {
        Self {
            count: 0,
            distinct: vec![Distinct::Ids(Vec::new()); n_distinct],
            stats: vec![(i64::MAX, i64::MIN, 0, 0); n_stats],
        }
    }

    fn merge(&mut self, other: GroupState) {
        self.count += other.count;
        for (a, b) in self.distinct.iter_mut().zip(other.distinct) {
            a.merge(b);
        }
        for (a, b) in self.stats.iter_mut().zip(other.stats.iter()) {
            a.0 = a.0.min(b.0);
            a.1 = a.1.max(b.1);
            a.2 += b.2;
            a.3 += b.3;
        }
    }
}

/// The aggregate list split into fixed slots, resolved once per call so the
/// row loop never re-inspects the request (hoist everything).
struct AggLayout {
    /// Distinct aggregates: `dst?` per slot.
    distinct: Vec<bool>,
    /// Stat aggregates: source per slot.
    stats: Vec<Source>,
}

impl AggLayout {
    fn of(aggs: &[Agg]) -> Self {
        let mut distinct = Vec::new();
        let mut stats = Vec::new();
        for a in aggs {
            match a {
                Agg::Count => {}
                Agg::CountDistinct { dst } => distinct.push(*dst),
                Agg::Min(s) | Agg::Max(s) | Agg::Mean(s) => stats.push(*s),
            }
        }
        Self { distinct, stats }
    }
}

/// Label lookup: per dense id, believed valid intervals sorted by `vt_s`.
/// Believed valid intervals of one identity are disjoint (the store
/// invariant), so at most one interval contains an instant.
pub struct LabelIndex {
    by_uid: HashMap<u32, Vec<(i64, i64, u32)>>,
    pub names: Vec<String>,
}

impl LabelIndex {
    fn lookup(&self, uid: u32, t: i64) -> i64 {
        let Some(ivs) = self.by_uid.get(&uid) else {
            return -1;
        };
        let i = ivs.partition_point(|&(s, _, _)| s <= t);
        if i == 0 {
            return -1;
        }
        let (s, e, code) = ivs[i - 1];
        if s <= t && t < e {
            code as i64
        } else {
            -1
        }
    }
}

/// Everything one selection's row loop reads, resolved once per segment.
struct SegCols<'a> {
    vt_s: &'a [i64],
    vt_e: Option<&'a [i64]>,
    src: &'a [u32],
    dst: &'a [u32],
    rel: Option<&'a [u16]>,
    /// Segment-local rel code -> global rel code.
    rel_map: Vec<i64>,
}

impl<'a> SegCols<'a> {
    fn open<S: SegmentSource>(
        seg: &'a Segment<S>,
        need_rel: bool,
        global_rel: &HashMap<&str, i64>,
    ) -> Result<Self> {
        let h = seg.header();
        Ok(Self {
            vt_s: seg.i64_column("vt_s")?,
            vt_e: if h.vt_e_elided {
                None
            } else {
                Some(seg.i64_column("vt_e")?)
            },
            src: seg.u32_column("src_id")?,
            dst: seg.u32_column("dst_id")?,
            rel: if need_rel {
                Some(seg.u16_column("rel_code")?)
            } else {
                None
            },
            rel_map: if need_rel {
                h.rel_types
                    .iter()
                    .map(|r| global_rel.get(r.as_str()).copied().unwrap_or(-1))
                    .collect()
            } else {
                Vec::new()
            },
        })
    }
}

type GroupMap = HashMap<[i64; 2], GroupState>;

// ---------------------------------------------------------------------------
// The fold's width is a function of its memory, not of the thread count
// ---------------------------------------------------------------------------

/// Bytes one group occupies in a partial table, **excluding** the distinct
/// sets' heap.
///
/// The exclusion is the load-bearing part and it is the bound, not a
/// convenience. A group's distinct set starts as an id vector and is promoted
/// to a bitset only once the vector costs what the bitset would, so a worker's
/// total distinct state is at most 8 bytes per row *that worker walked* —
/// summed over workers, 8 bytes per selected row however wide the fold is.
/// Distinct state therefore does not multiply with width, and a budget that
/// counted it would shrink the width to pay for memory that widening never
/// allocated. What does multiply is the group table itself: every worker that
/// sees a group materialises its own key, its own state header and its own
/// stat slots, so `width × this × groups` is the quantity measured
/// and the quantity this budget bounds.
///
/// `hashbrown` keeps one control byte per bucket and grows at a 7/8 load
/// factor, so `n` entries occupy at least `ceil(8n/7)` buckets.
fn group_fixed_bytes(layout: &AggLayout) -> u64 {
    let bucket = (std::mem::size_of::<[i64; 2]>() + std::mem::size_of::<GroupState>() + 1) as u64;
    let stats = (layout.stats.len() * std::mem::size_of::<(i64, i64, i128, u64)>()) as u64;
    let distinct_hdr = (layout.distinct.len() * std::mem::size_of::<Distinct>()) as u64;
    bucket.div_ceil(7) * 8 + stats + distinct_hdr
}

/// What one worker's partial table actually holds, in the same accounting as
/// [`group_fixed_bytes`] — so the estimate and the measurement are comparable
/// without a second convention.
fn map_fixed_bytes(map: &GroupMap, layout: &AggLayout) -> u64 {
    let bucket = (std::mem::size_of::<[i64; 2]>() + std::mem::size_of::<GroupState>() + 1) as u64;
    let stats = (layout.stats.len() * std::mem::size_of::<(i64, i64, i128, u64)>()) as u64;
    let distinct_hdr = (layout.distinct.len() * std::mem::size_of::<Distinct>()) as u64;
    // `capacity()` is entries-before-resize, i.e. 7/8 of the bucket count
    let buckets = (map.capacity() as u64).div_ceil(7) * 8;
    buckets * bucket + (map.len() as u64) * (stats + distinct_hdr)
}

/// How many distinct group keys this request can emit, from what the planner
/// already holds at the point the width is chosen.
///
/// Every dimension's code space is known before a row is folded: the rel
/// table is the sorted union of the opened headers, endpoint ids are dense
/// over the dictionary, label codes are the label index's own table, and a
/// time bucket's count is the window divided by the stride. The product is an
/// upper bound on the key space, and two things cut it down to something
/// useful: a group needs a row, so it cannot exceed the selected row count;
/// and the request's own `max_groups` is the cap past which the call fails
/// anyway, so no width policy needs to plan for more.
///
/// It is an estimate and it is allowed to be wrong in both directions — the
/// consequence of a wrong estimate is a fold that is narrower or wider than
/// ideal, never a different answer. `fold_map_bytes_est` and
/// `fold_map_bytes` are both recorded on every query so the error is a
/// measured quantity rather than an assumption.
fn est_groups(
    req: &AggregateRequest,
    rows_selected: u64,
    n_rel: usize,
    n_entities: u32,
    n_labels: usize,
) -> u64 {
    let mut space: u64 = 1;
    for d in &req.dims {
        let card: u64 = match *d {
            Dim::TimeBucket { stride } => {
                if stride <= 0 {
                    1
                } else {
                    (req.t_b.saturating_sub(req.t_a).max(0) as u64).div_ceil(stride as u64)
                }
            }
            // a rel dimension can only emit codes the opened headers name
            Dim::RelType => n_rel as u64,
            Dim::Endpoint { .. } => n_entities as u64,
            // `-1` (no believed label at the event's instant) is a code too
            Dim::Label { .. } => n_labels as u64 + 1,
        };
        space = space.saturating_mul(card.max(1));
    }
    // an ungrouped aggregate emits exactly one row
    space
        .max(1)
        .min(rows_selected.max(1))
        .min(req.max_groups.max(1) as u64)
}

/// The fold's byte budget: `GNOMON_FOLD_BYTES` if set, else
/// [`crate::defaults::FOLD_MAP_BUDGET_BYTES`]. `0` means unbounded, and
/// unparseable is treated as unset — a tuning knob must never make a read
/// fail (the rule `GNOMON_SCAN_THREADS` and `GNOMON_ROWS_PER_UNIT` already keep).
fn fold_budget_bytes() -> Option<u64> {
    match std::env::var("GNOMON_FOLD_BYTES")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
    {
        Some(0) => None,
        Some(v) => Some(v),
        None => Some(crate::defaults::FOLD_MAP_BUDGET_BYTES),
    }
}

/// Concurrent partial tables the budget allows, saturated at `threads`.
///
/// `max(1)` is not a rounding convenience: a single table is what a serial
/// fold already allocates, so the budget can narrow the fold to one worker
/// and never below. A query whose *one* table exceeds the whole budget is
/// therefore still answered, at the memory a serial fold always cost.
pub(crate) fn fold_width_cap(threads: usize, est_map_bytes: u64) -> usize {
    let Some(budget) = fold_budget_bytes() else {
        return threads;
    };
    if est_map_bytes == 0 {
        return threads;
    }
    threads.min((budget / est_map_bytes).max(1) as usize)
}

/// What the workers past the cap do.
///
/// Both keep the answer byte-identical — every partial state merges
/// commutatively and associatively, so which table a unit lands in is as free
/// as which worker folds it — and they trade differently:
///
/// * `Narrow` spends latency: the fold runs `cap` workers wide, so a cell
///   where the cap binds pays the width it gave up.
/// * `Share` spends contention: all `threads` workers run the row loop, and
///   each merges its unit's partial into one of `cap` shared tables under
///   that table's lock. The lock is held for the merge only, never for the
///   row loop. It bounds memory only where a *unit's* partial is smaller
///   than the whole group table — which is exactly the high-cardinality case
///   where the cap binds hardest, and not the small-group case at all.
///
/// Which is the default is a measurement, not a preference; `GNOMON_FOLD_OVERFLOW`
/// selects the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FoldOverflow {
    Narrow,
    Share,
}

fn fold_overflow() -> FoldOverflow {
    match std::env::var("GNOMON_FOLD_OVERFLOW")
        .ok()
        .as_deref()
        .map(str::trim)
    {
        Some("share") => FoldOverflow::Share,
        _ => FoldOverflow::Narrow,
    }
}

/// Open every segment a selection names, once.
///
/// The fold's unit used to be the selection, and a selection used to be a
/// whole segment, so `SegCols::open` per selection *was* per segment. Units
/// are now row ranges, several per segment, and the column handles and the
/// segment-local-to-global rel map are the same for all of them — so they are
/// resolved here, before the fan-out, and every worker reads the same table.
/// Segments no selection names stay `None`: a lane's scan targets include
/// segments the window pruned, and `SegCols::open` on one of those would
/// demand columns it need not have.
fn open_seg_cols<'a, S: SegmentSource>(
    segments: &[&'a Segment<S>],
    selections: &[Selection],
    req: &AggregateRequest,
    global_rel: &HashMap<&str, i64>,
) -> Result<Vec<Option<SegCols<'a>>>> {
    let need_rel = req.dims.iter().any(|d| matches!(d, Dim::RelType));
    let t_cols = std::time::Instant::now();
    let mut wanted = vec![false; segments.len()];
    for sel in selections {
        wanted[sel.segment] = true;
    }
    let mut out: Vec<Option<SegCols<'a>>> = Vec::with_capacity(segments.len());
    for (i, seg) in segments.iter().enumerate() {
        out.push(if wanted[i] {
            Some(SegCols::open(seg, need_rel, global_rel)?)
        } else {
            None
        });
    }
    crate::phase::since(crate::phase::Phase::FoldCols, t_cols);
    Ok(out)
}

/// Phase 1 over a set of selections: fold their rows into a private map.
///
/// Kept whole, and called with a one-element slice by the self-scheduling
/// worker below rather than refactored into a per-selection entry point that
/// writes into a caller's map. That refactor was written first and measured:
/// splitting the row loop into a function taking `&mut GroupMap` cost 20 % of
/// the fold's CPU *and* 27 % of the select stage's, on the settled stores
/// where the schedule is provably identical either way (5 units over 5
/// workers, 14 over 14) — a whole-binary codegen effect, since select's own
/// source was untouched in that arm. A fresh rebuild of the unmodified source
/// reproduced the original timing to 0.1 %, so the arm was the variable and
/// not the build. Leave the body here.
#[allow(clippy::too_many_arguments)]
fn fold_selections(
    cols: &[Option<SegCols<'_>>],
    selections: &[Selection],
    req: &AggregateRequest,
    layout: &AggLayout,
    labels: Option<&LabelIndex>,
    plan: &DistinctPlan,
) -> Result<GroupMap> {
    let n_distinct = layout.distinct.len();
    let n_stats = layout.stats.len();
    let mut map = GroupMap::new();
    for sel in selections {
        // Resolved once per *segment* by `open_seg_cols`, not once per
        // unit. A segment now yields several units, and re-resolving five
        // columns by name and rebuilding the rel map for each of them would
        // have put a per-unit constant into the one stage this exists to
        // widen.
        let c = cols[sel.segment]
            .as_ref()
            .ok_or_else(|| crate::EngineError::corrupt("fold unit names an unopened segment"))?;
        let t_rows = std::time::Instant::now();
        // The scan's window filter is interval overlap; an *event* is
        // containment of vt_s. Selected rows ascend and vt_s is sorted
        // within a segment, so the exact cut is a prefix binary search.
        let cut = sel
            .rows
            .partition_point(|&r| c.vt_s[r as usize] < req.t_a);
        for &r in &sel.rows[cut..] {
            let i = r as usize;
            let mut key = [0i64; 2];
            for (k, d) in req.dims.iter().enumerate() {
                key[k] = match d {
                    Dim::TimeBucket { stride } => (c.vt_s[i] - req.t_a) / stride,
                    Dim::RelType => {
                        let codes = c.rel.expect("rel column resolved");
                        c.rel_map[codes[i] as usize]
                    }
                    Dim::Endpoint { dst } => {
                        (if *dst { c.dst[i] } else { c.src[i] }) as i64
                    }
                    Dim::Label { dst } => {
                        let ep = if *dst { c.dst[i] } else { c.src[i] };
                        labels.expect("label index built").lookup(ep, c.vt_s[i])
                    }
                };
            }
            let g = map
                .entry(key)
                .or_insert_with(|| GroupState::new(n_distinct, n_stats));
            g.count += 1;
            for (slot, dst) in layout.distinct.iter().enumerate() {
                g.distinct[slot].insert(if *dst { c.dst[i] } else { c.src[i] }, plan);
            }
            if !layout.stats.is_empty() {
                let vt_e = c.vt_e.map(|col| col[i]).unwrap_or(c.vt_s[i] + 1);
                for (slot, src) in layout.stats.iter().enumerate() {
                    let v = match src {
                        Source::VtS => c.vt_s[i],
                        Source::Duration => {
                            if vt_e >= OPEN_END {
                                continue; // open-ended: contributes nothing
                            }
                            vt_e - c.vt_s[i]
                        }
                    };
                    let s = &mut g.stats[slot];
                    s.0 = s.0.min(v);
                    s.1 = s.1.max(v);
                    s.2 += v as i128;
                    s.3 += 1;
                }
            }
        }
        crate::phase::since(crate::phase::Phase::FoldRows, t_rows);
        crate::phase::add(
            crate::phase::Phase::FoldRowsWalked,
            (sel.rows.len() - cut) as u64,
        );
        crate::phase::add(crate::phase::Phase::FoldUnits, 1);
    }
    Ok(map)
}

/// Fold `src`'s groups into `dst`. Commutative and associative by
/// construction, so a caller may apply it in any order (`GroupState::merge`).
fn merge_maps(dst: &mut GroupMap, src: GroupMap) {
    for (key, state) in src {
        match dst.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => e.get_mut().merge(state),
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(state);
            }
        }
    }
}

/// Phases 1+2: parallel partials over selection chunks, merged into one map.
/// `threads` is explicit so tests can pin any width (the store entry decides
/// its width through the recalibrated scan gates); any width yields the same
/// map, because every per-group state merges commutatively.
///
/// `map_cap` bounds the number of partial tables alive at once. It is
/// the budget's verdict, resolved by the caller from an estimate of one
/// table's size, and it is a separate argument from `threads` because the two
/// answer different questions — how much parallelism the work supports, and
/// how much partial state the fold may hold. Passing `threads` for it is the
/// the previous behaviour exactly.
#[allow(clippy::too_many_arguments)]
pub(crate) fn aggregate_partials<S: SegmentSource>(
    segments: &[&Segment<S>],
    selections: &[Selection],
    req: &AggregateRequest,
    global_rel: &HashMap<&str, i64>,
    labels: Option<&LabelIndex>,
    plan: DistinctPlan,
    threads: usize,
    map_cap: usize,
) -> Result<GroupMap> {
    let layout = AggLayout::of(&req.aggs);
    let cols = open_seg_cols(segments, selections, req, global_rel)?;
    let map_cap = map_cap.max(1);
    // `Narrow` spends the width it cannot afford tables for; `Share` keeps
    // the width and shares `map_cap` tables between the workers.
    let overflow = fold_overflow();
    let threads = if overflow == FoldOverflow::Narrow {
        threads.min(map_cap)
    } else {
        threads
    };
    if threads <= 1 || selections.len() < 2 {
        let t = std::time::Instant::now();
        let map = fold_selections(&cols, selections, req, &layout, labels, &plan);
        crate::phase::worker(
            crate::phase::Phase::FoldBusy,
            crate::phase::Phase::FoldMaxBusy,
            t,
        );
        crate::phase::add(crate::phase::Phase::FoldWorkers, 1);
        if let Ok(m) = map.as_ref() {
            note_map_bytes(m, &layout);
        }
        return map;
    }
    // The units of this fold are segments, and a tiered store's
    // segments are a geometric staircase: at 30M, nine of ~2.33 M rows, then
    // 1.5 M, 1.37 M, six of 1.02 M, then a tail down to single rows. Slicing
    // them into contiguous chunks of `ceil(n/threads)` divides them equally
    // by *count*, which on that shape is not equally by work — chunk 2 puts
    // the two largest together, and the stage's makespan became 4.65 M rows
    // against an ideal of 1.88 M. A settled store's segments are near
    // uniform, so the same rule costs it nothing, and the whole measured
    // tiered/settled aggregate penalty was that difference in makespan: on
    // the 30M pair the tiered configuration did *less* total fold CPU than the settled
    // one (703 ms against 897 ms) and still took 107 ms of wall against
    // 79 ms, at parallel efficiency 0.44 against 0.81.
    //
    // Self-schedule instead: workers claim the next unclaimed selection, so
    // no worker idles while a selection is unclaimed and the makespan falls
    // to the work-conserving bound, `max(largest selection, total/width)` —
    // a quantity that depends on the store's *content*, not on how many
    // files that content is spread over. Largest-first so the longest unit
    // starts at t=0 rather than being picked up last.
    //
    // Which worker folds which selection is now nondeterministic, and that
    // changes nothing about the answer, by the same argument that already
    // licensed varying `threads`: every partial state is integer-valued and
    // merges commutatively (module header; `GroupState::merge`), and the
    // distinct sets are sorted and deduplicated exactly once, at finalize.
    // The output's order comes from `finalize`'s canonical sort on integer
    // ranks, which never sees a selection or a worker.
    let width = threads.min(selections.len());
    let mut order: Vec<usize> = (0..selections.len()).collect();
    order.sort_by(|&a, &b| {
        selections[b]
            .rows
            .len()
            .cmp(&selections[a].rows.len())
            .then(a.cmp(&b))
    });
    let next = std::sync::atomic::AtomicUsize::new(0);
    // One table per worker is the shape and stays the shape
    // whenever the budget allows `width` of them, which is every cell where
    // the estimate clears it. Where it does not, `Share` keeps the row loop
    // `width` wide and gives the workers `tables` tables to merge into: a
    // worker holds its table's lock for the merge of one unit's partial and
    // never for the row loop, and the assignment `w % tables` is fixed, so
    // the contention is `width / tables` writers per lock and nothing about
    // it is data-dependent. The answer does not notice either way — a
    // partial's destination is as free as the worker that produced it, by the
    // same commutativity that already licensed varying `threads`.
    let tables = map_cap.min(width);
    let shared: Vec<std::sync::Mutex<GroupMap>> = if tables < width {
        (0..tables).map(|_| std::sync::Mutex::new(GroupMap::new())).collect()
    } else {
        Vec::new()
    };
    let parts: Vec<Result<GroupMap>> = crate::pool::run(width, |w| -> Result<GroupMap> {
                    let (layout, plan, cols) = (&layout, &plan, &cols);
                    let t = std::time::Instant::now();
                    let mut map = GroupMap::new();
                    loop {
                        let k = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(&si) = order.get(k) else { break };
                        // A one-element slice, so `fold_selections` keeps
                        // the exact shape it had. The extra map and merge per
                        // claimed selection are what the phase timers already
                        // price at 0.01-0.28 ms for a whole query's merging.
                        let part = fold_selections(
                            cols,
                            std::slice::from_ref(&selections[si]),
                            req,
                            layout,
                            labels,
                            plan,
                        )?;
                        if let Some(lock) = shared.get(w % shared.len().max(1)) {
                            let mut dst = lock.lock().expect("fold table poisoned");
                            if dst.is_empty() {
                                *dst = part;
                            } else {
                                merge_maps(&mut dst, part);
                            }
                        } else if map.is_empty() {
                            map = part;
                        } else {
                            merge_maps(&mut map, part);
                        }
                    }
                    crate::phase::worker(
                        crate::phase::Phase::FoldBusy,
                        crate::phase::Phase::FoldMaxBusy,
                        t,
                    );
                    crate::phase::add(crate::phase::Phase::FoldWorkers, 1);
                    if shared.is_empty() {
                        note_map_bytes(&map, layout);
                    }
                    Ok(map)
    });
    let t_merge = std::time::Instant::now();
    let mut merged = GroupMap::new();
    for part in parts {
        merge_maps(&mut merged, part?);
    }
    for lock in shared {
        let m = lock.into_inner().expect("fold table poisoned");
        note_map_bytes(&m, &layout);
        merge_maps(&mut merged, m);
    }
    crate::phase::since(crate::phase::Phase::AggMerge, t_merge);
    Ok(merged)
}

/// Record one finished partial table's width-scaling bytes, summed and peak.
///
/// Counted at the moment a table stops growing rather than sampled: the
/// quantity that matters is how much partial state exists *at once*, and every
/// table that reaches here was alive alongside the others.
fn note_map_bytes(map: &GroupMap, layout: &AggLayout) {
    let b = map_fixed_bytes(map, layout);
    crate::phase::add(crate::phase::Phase::FoldMapBytes, b);
    crate::phase::max(crate::phase::Phase::FoldMapBytesMax, b);
}

/// The exact-integer mean, mirroring `ops_aggregate._mean` operation for
/// operation: sums are exact, and exactly one rounding happens per term.
fn mean(sum: i128, n: u64) -> f64 {
    let q = sum.div_euclid(n as i128);
    let r = sum.rem_euclid(n as i128);
    q as f64 + (r as f64) / (n as f64)
}

/// Finalize: cap check, canonical ordering on integer ranks, aggregate
/// columns. `endpoint_rank` turns an endpoint dense id into its rank in uid
/// code-point order — the one ordering integers cannot carry on their own.
fn finalize(
    map: GroupMap,
    req: &AggregateRequest,
    rel_names: Vec<String>,
    label_names: Vec<String>,
    endpoint_rank: impl Fn(&[i64]) -> HashMap<i64, i64>,
) -> Result<AggregateOut> {
    let mut groups: Vec<([i64; 2], GroupState)> = map.into_iter().collect();
    if groups.len() > req.max_groups {
        return Err(EngineError::capacity(format!(
            "aggregate group count {} exceeds cap {}",
            groups.len(),
            req.max_groups
        )));
    }

    // ranks per dimension: integers whose order is the canonical order
    let mut rank_of: Vec<Option<HashMap<i64, i64>>> = Vec::new();
    for (k, d) in req.dims.iter().enumerate() {
        rank_of.push(match d {
            // codes already carry the order: bucket index, sorted rel
            // names, sorted label names with -1 (null) first
            Dim::TimeBucket { .. } | Dim::RelType | Dim::Label { .. } => None,
            Dim::Endpoint { .. } => {
                let codes: Vec<i64> = groups.iter().map(|(key, _)| key[k]).collect();
                Some(endpoint_rank(&codes))
            }
        });
    }
    let rank = |key: &[i64; 2]| -> [i64; 2] {
        let mut r = [0i64; 2];
        for k in 0..req.dims.len() {
            r[k] = match &rank_of[k] {
                None => key[k],
                Some(m) => m[&key[k]],
            };
        }
        r
    };
    groups.sort_by_key(|(key, _)| rank(key));

    let n = groups.len();
    let mut keys: Vec<Vec<i64>> = vec![Vec::with_capacity(n); req.dims.len()];
    for (key, _) in &groups {
        for k in 0..req.dims.len() {
            keys[k].push(key[k]);
        }
    }

    let layout = AggLayout::of(&req.aggs);
    let mut aggs_out = Vec::with_capacity(req.aggs.len());
    let (mut di, mut si) = (0usize, 0usize);
    for a in &req.aggs {
        match a {
            Agg::Count => aggs_out.push(AggValues::Int(
                groups.iter().map(|(_, g)| Some(g.count as i64)).collect(),
            )),
            Agg::CountDistinct { .. } => {
                let slot = di;
                di += 1;
                aggs_out.push(AggValues::Int(
                    groups
                        .iter()
                        .map(|(_, g)| Some(g.distinct[slot].cardinality()))
                        .collect(),
                ));
            }
            Agg::Min(_) | Agg::Max(_) | Agg::Mean(_) => {
                let slot = si;
                si += 1;
                let col: Vec<(i64, i64, i128, u64)> =
                    groups.iter().map(|(_, g)| g.stats[slot]).collect();
                aggs_out.push(match a {
                    Agg::Min(_) => AggValues::Int(
                        col.iter()
                            .map(|s| (s.3 > 0).then_some(s.0))
                            .collect(),
                    ),
                    Agg::Max(_) => AggValues::Int(
                        col.iter()
                            .map(|s| (s.3 > 0).then_some(s.1))
                            .collect(),
                    ),
                    _ => AggValues::Float(
                        col.iter()
                            .map(|s| (s.3 > 0).then(|| mean(s.2, s.3)))
                            .collect(),
                    ),
                });
            }
        }
    }
    debug_assert_eq!(di, layout.distinct.len());
    debug_assert_eq!(si, layout.stats.len());

    Ok(AggregateOut {
        keys,
        aggs: aggs_out,
        rel_names,
        label_names,
        // filled in by the caller, which is the only place that knows how
        // much of the manifest it pruned
        stats: AggScanStats::default(),
    })
}

// ---------------------------------------------------------------------------
// Count by time bucket from believed row ranges
// ---------------------------------------------------------------------------

/// Bucket-count arrays larger than this go to the row path: the range path
/// holds one `u64` per bucket, and a window over a tiny stride is a group
/// count no fold would have to materialise either.
const RANGE_COUNT_MAX_BUCKETS: u128 = 1 << 20;

/// `Some(bucket count)` when `req` is answerable from range lengths alone:
/// one `time_bucket` dimension (or none), and only `count` aggregates. The
/// rel-type and endpoint dimensions, `count_distinct`, and every stat need a
/// value per row and keep the fold.
fn range_count_buckets(req: &AggregateRequest) -> Option<usize> {
    if req.aggs.is_empty() || !req.aggs.iter().all(|a| matches!(a, Agg::Count)) {
        return None;
    }
    let stride = match req.dims.as_slice() {
        [] => 1,
        [Dim::TimeBucket { stride }] if *stride > 0 => *stride,
        _ => return None,
    };
    // an empty window still selects (and the counter reports) the interval
    // rows that overlap it; leave that degenerate shape to the row path
    if req.t_b <= req.t_a {
        return None;
    }
    let span = req.t_b as i128 - req.t_a as i128;
    let n = (span + stride as i128 - 1) / stride as i128;
    if req.dims.is_empty() {
        return Some(1);
    }
    (n as u128 <= RANGE_COUNT_MAX_BUCKETS).then_some(n as usize)
}

/// Rows of the ascending, disjoint `ranges` below row `x`, with a forward
/// cursor: successive calls must not decrease `x`.
struct RangePrefix<'r> {
    ranges: &'r [(usize, usize)],
    j: usize,
    done: usize,
}

impl<'r> RangePrefix<'r> {
    fn new(ranges: &'r [(usize, usize)]) -> Self {
        Self {
            ranges,
            j: 0,
            done: 0,
        }
    }

    #[inline]
    fn below(&mut self, x: usize) -> usize {
        while self.j < self.ranges.len() && self.ranges[self.j].1 <= x {
            self.done += self.ranges[self.j].1 - self.ranges[self.j].0;
            self.j += 1;
        }
        match self.ranges.get(self.j) {
            Some(&(a, _)) if a < x => self.done + (x - a),
            _ => self.done,
        }
    }
}

/// Events per bucket over the planned segments, and the rows the row path
/// would have selected (the `rows_selected` counter), without a selection
/// vector or a group table.
///
/// The planner's ranges are the believed rows of the window, already clipped
/// to `vt_s < t_b` and — wherever it is exact — to `vt_e > t_a`. Rows are
/// stored in `vt_s` order, so inside a segment bucket `k` is the row interval
/// `[P(t_a + k·stride), P(t_a + (k+1)·stride))` with `P` a binary search of
/// `vt_s`; its count is the length of that interval's intersection with the
/// ranges, read off a prefix sum. Cost per segment: the buckets the
/// segment's `vt_s` span reaches, times a binary search, plus its ranges —
/// never its rows.
///
/// Exactness against the fold, which counts a selected row when `vt_s >=
/// t_a` (an event is containment of `vt_s`; the scan's window is overlap):
///
/// * `vt_s >= t_a` is the suffix from `P(t_a)`, so starting the first bucket
///   there is the fold's cut;
/// * the scan's remaining per-row test is `vt_e > t_a`, for interval-lane
///   rows. On every row the fold counts it holds already: `vt_e > vt_s >=
///   t_a`, by the store's valid-interval invariant `vt_s < vt_e`
///   (`integrity::check_rows`, finding `valid-interval-empty`). So it only
///   decides `rows_selected` for rows below `P(t_a)`, and is applied to
///   exactly those;
/// * a relation filter is decided per segment: a segment whose relations are
///   all allowed takes the range path; one with a mix is walked row by row
///   with the scan's own tests (still no selection vector, no table).
fn range_counts<S: SegmentSource>(
    segments: &[&Segment<S>],
    plans: &[crate::scan::SegPlan<'_>],
    req: &AggregateRequest,
    n_buckets: usize,
) -> Result<(Vec<u64>, u64)> {
    let stride = match req.dims.as_slice() {
        [Dim::TimeBucket { stride }] => *stride,
        _ => i64::MAX,
    };
    let (t_a, t_b) = (req.t_a, req.t_b);
    let mut counts = vec![0u64; n_buckets];
    let mut selected = 0u64;
    // bucket of an in-window vt_s
    let bucket = |v: i64| -> usize {
        if stride == i64::MAX {
            0
        } else {
            ((v as i128 - t_a as i128) / stride as i128) as usize
        }
    };
    for p in plans {
        if p.ranges.is_empty() || n_buckets == 0 {
            continue;
        }
        let vt_s = segments[p.idx].i64_column("vt_s")?;
        // the scan tests a relation only where it has both the filter and
        // the code column (`select_rows`); so does this
        let mixed_rel = match (&p.rel_allowed, p.rel_codes) {
            (Some(allowed), Some(_)) => !allowed.iter().all(|&a| a),
            _ => false,
        };
        if mixed_rel {
            // the scan's own row tests, in its own order, then the fold's cut
            let allowed = p.rel_allowed.as_ref().expect("mixed implies a filter");
            let codes = p.rel_codes.expect("mixed implies a code column");
            for &(a, b) in &p.ranges {
                for i in a..b {
                    if let Some(vt_e) = p.vt_e {
                        if vt_e[i] <= t_a {
                            continue;
                        }
                    }
                    if !allowed.get(codes[i] as usize).copied().unwrap_or(false) {
                        continue;
                    }
                    selected += 1;
                    if vt_s[i] >= t_a {
                        counts[bucket(vt_s[i])] += 1;
                    }
                }
            }
            continue;
        }
        let lo_row = p.ranges[0].0;
        let hi_row = p.ranges[p.ranges.len() - 1].1;
        let span = &vt_s[lo_row..hi_row];
        let pos = |from: usize, t: i64| -> usize {
            from + span[from - lo_row..].partition_point(|v| *v < t)
        };
        let start = pos(lo_row, t_a);
        let mut pre = RangePrefix::new(&p.ranges);
        let below_start = pre.below(start);
        // rows below `t_a` the scan selected: those whose vt_e passes
        selected += match p.vt_e {
            None => below_start as u64,
            Some(vt_e) => {
                let mut n = 0u64;
                for &(a, b) in &p.ranges {
                    if a >= start {
                        break;
                    }
                    n += vt_e[a..b.min(start)].iter().filter(|&&e| e > t_a).count() as u64;
                }
                n
            }
        };
        if start >= hi_row {
            continue;
        }
        selected += (p.rows - below_start) as u64;
        // buckets this segment's in-window rows can reach
        let k_first = bucket(vt_s[start]);
        let k_last = bucket(vt_s[hi_row - 1]).min(n_buckets - 1);
        let mut at = start;
        let mut f_at = below_start;
        for (k, slot) in counts.iter_mut().enumerate().take(k_last + 1).skip(k_first) {
            let end = if k + 1 >= n_buckets || stride == i64::MAX {
                hi_row
            } else {
                let edge = t_a as i128 + (k as i128 + 1) * stride as i128;
                if edge >= t_b as i128 {
                    hi_row
                } else {
                    pos(at, edge as i64)
                }
            };
            let f_end = pre.below(end);
            *slot += (f_end - f_at) as u64;
            at = end;
            f_at = f_end;
        }
    }
    Ok((counts, selected))
}

impl NativeStore {
    /// Believed node labels as disjoint valid intervals per dense id, with
    /// a sorted global label table — the label dimension's temporal join,
    /// built once per call (nodes are few; events are many).
    ///
    /// **Through the node scan, not through the whole node store.**
    /// This was the last place an otherwise fully range-filtered aggregate
    /// could still drag every stored node version through a row-at-a-time
    /// belief test. `all_node_versions()` materialises each one as a
    /// `NodeVersionOut` — a uid string, a 24-hex vid, a label, plus the
    /// `props`, `source` and `provenance_ref` a label index has no use for —
    /// and only *then* asks `believed_at` about it, twice, once per pass.
    /// So a `Label` dimension cost O(stored node versions) of string
    /// building even when the edge scan beneath it had pruned to a handful
    /// of segments, and nothing about that cost moved with the query.
    ///
    /// [`scan_node_versions`](crate::read::NativeStore::scan_node_versions)
    /// is the converted node path and already returns exactly the four
    /// fields wanted here, so this calls it rather than reimplementing the
    /// planning: header prune, then `believed_ranges_closed` per segment,
    /// then materialise the selected rows only. Belief is decided in the
    /// range domain and the two passes below see only believed rows.
    ///
    /// One smaller consequence, deliberate: `NodeScanRow` carries `uid_id` —
    /// the dense id — straight off the column, so the reverse
    /// `dense_id(&r.uid)` lookup per row is gone. It was total for rows that
    /// came out of the dictionary in the first place, and its `else continue`
    /// side was unreachable. It does not change the result: `names` is a
    /// `BTreeSet` and every interval list is sorted, so `LabelIndex` does not
    /// depend on the order the rows arrive in.
    ///
    /// The two passes below stay two, and saying so is worth a line because
    /// it is the obvious thing to claim and it is not true: the second pass
    /// needs `code`, which needs the label table complete and sorted, so
    /// they cannot merge. What goes away is not a pass but the `believed_at`
    /// call each of them made per row, and the whole-store materialisation
    /// both of them walked.
    fn label_index(&self, as_of_tt: i64) -> Result<LabelIndex> {
        let scanned = self.scan_node_versions(as_of_tt, None, None)?;
        let mut names: BTreeSet<String> = BTreeSet::new();
        for r in &scanned.rows {
            names.insert(r.label.clone());
        }
        let names: Vec<String> = names.into_iter().collect();
        let code: HashMap<&str, u32> = names
            .iter()
            .enumerate()
            .map(|(i, s)| (s.as_str(), i as u32))
            .collect();
        let mut by_uid: HashMap<u32, Vec<(i64, i64, u32)>> = HashMap::new();
        for r in &scanned.rows {
            by_uid
                .entry(r.uid_id)
                .or_default()
                .push((r.vt_s, r.vt_e, code[r.label.as_str()]));
        }
        for ivs in by_uid.values_mut() {
            ivs.sort_unstable();
        }
        Ok(LabelIndex { by_uid, names })
    }

    /// `aggregate_events`, engine side: scan-select in parallel, fold
    /// per-thread partials, merge, order canonically. Group keys stay
    /// fixed-width codes; the caller rehydrates names from the returned
    /// tables. Results are byte-identical at any thread count.
    pub fn aggregate_edges(&self, req: &AggregateRequest) -> Result<AggregateOut> {
        use crate::phase::{since, Phase};
        crate::phase::add(Phase::AggCalls, 1);
        self.assert_full_belief(req.as_of_tt)?;
        let t_labels = std::time::Instant::now();
        let labels = if req.dims.iter().any(|d| matches!(d, Dim::Label { .. })) {
            Some(self.label_index(req.as_of_tt)?)
        } else {
            None
        };
        since(Phase::AggLabels, t_labels);

        let scan_req = ScanRequest {
            as_of_tt: req.as_of_tt,
            vt_min: Some(req.t_a),
            vt_max: Some(req.t_b),
            rel_types: req.rel_types.clone(),
            ..Default::default()
        };

        // Prune on the manifest's own zone maps *before* opening
        // anything. `SegmentEntry` carries the same `vt_min`/`vt_e_max`
        // bounds and the same folded `tt_s` minimum the segment header does
        // (`staging::entry_for`), so `scan::prunes_bounds` — the one copy of
        // the tier-1 predicate, shared with `ScanSet::prunes` — answers
        // "cannot contribute" here without mapping a file or decoding a
        // column. The relation test is left to the header-level prune inside
        // `ScanSet::select`: a manifest entry's `rel_codes` are segment-local
        // codes with no name table beside them (see `SegBounds::rel_types`).
        //
        // This is what makes a windowed aggregate's working set the *window*
        // rather than the store, and it is the precondition for the cached
        // open below.
        let t_prune = std::time::Instant::now();
        let m = self.manifest();
        let mut files = Vec::new();
        let mut stats = AggScanStats::default();
        for (lane, entries) in [
            (Lane::Event, &m.edge_lanes.event),
            (Lane::Interval, &m.edge_lanes.interval),
        ] {
            for e in entries {
                stats.segments_total += 1;
                // `GNOMON_SEGMENT_PRUNE=0` opens every segment instead
                if self.designs().segment_prune
                    && crate::scan::prunes_bounds(
                    &crate::scan::SegBounds {
                        tt_s_min: e.tt_s_min,
                        vt_min: e.vt_min,
                        vt_e_max: e.vt_e_max,
                        rel_types: &[],
                    },
                    &scan_req,
                ) {
                    stats.segments_pruned += 1;
                    crate::vistouch::pruned(1);
                    continue;
                }
                files.push((lane, e.file.clone()));
            }
        }
        stats.segments_opened = files.len();
        since(Phase::AggPrune, t_prune);
        let t_open = std::time::Instant::now();
        let mut segs = Vec::with_capacity(files.len());
        for (lane, file) in &files {
            // Cached, deliberately.
            //
            // This opens `Uncached` along with the true whole-store
            // bulk walks, on the reading that it touched every segment the
            // manifest names. It did — but only because the prune above did
            // not exist yet. `aggregate_edges` is a per-QUERY path, not a
            // bulk walk: uncached, it re-decoded the whole store on *every*
            // call (measured at 1M on the development machine: a 10-tick window 0.85 -> 70.7 ms,
            // 83x, and cache-indifferent), which destroyed the window
            // selectivity the aggregate contract rests on.
            //
            // The RSS objection to caching here is answered by ordering rather
            // than by mode: pruning happens first, so what the session cache
            // retains is bounded by the *window*, not by the store. The bulk
            // walks that genuinely touch every segment once
            // (`all_edge_versions`/`all_node_versions`, `index_segments`,
            // compaction's merge reads) stay uncached.
            segs.push((
                self.open_segment_mode(file, crate::store::OpenMode::Cached)?,
                *lane,
                segment_id_of(file),
            ));
        }
        since(Phase::AggOpen, t_open);
        let seg_refs: Vec<&Segment<crate::segment::MmapSource>> =
            segs.iter().map(|(s, _, _)| &**s).collect();
        let targets: Vec<ScanTarget<'_, crate::segment::MmapSource>> = segs
            .iter()
            .map(|(segment, lane, id)| ScanTarget {
                segment,
                lane: *lane,
                id: *id,
            })
            .collect();
        // The row path's select and fold are gated at the
        // aggregate's own row threshold, read once per call so both stages
        // agree; the count-by-bucket path below only plans, and its planner
        // keeps the scan gate
        let agg_min_rows = crate::scan::agg_parallel_min_rows();
        let set = ScanSet::new(targets)
            .with_closes(self.close_index()?)
            .with_designs(self.designs())
            .with_row_loop_min_rows(agg_min_rows);

        // global rel codes: sorted names over the target headers, so code
        // order *is* name order and no per-row string ever exists
        let t_rel = std::time::Instant::now();
        let rel_names: Vec<String> = {
            let mut s: BTreeSet<String> = BTreeSet::new();
            for r in seg_refs.iter().flat_map(|seg| seg.header().rel_types.iter()) {
                s.insert(r.clone());
            }
            s.into_iter().collect()
        };
        let global_rel: HashMap<&str, i64> = rel_names
            .iter()
            .enumerate()
            .map(|(i, s)| (s.as_str(), i as i64))
            .collect();
        since(Phase::AggRelNames, t_rel);

        // A count by time bucket is answered from the believed
        // row ranges' lengths and never touches a row; see `range_counts`.
        // `GNOMON_AGG_RANGE_COUNT=0` keeps the row path (read at open).
        let range_buckets = if self.designs().agg_range_count {
            range_count_buckets(req)
        } else {
            None
        };
        let mut map = if let Some(n_buckets) = range_buckets {
            let t_select = std::time::Instant::now();
            let (plans, _stats) = set.plan_believed(&scan_req)?;
            since(Phase::AggSelectWall, t_select);
            let t_fold = std::time::Instant::now();
            let (counts, rows) = range_counts(&seg_refs, &plans, req, n_buckets)?;
            self.note_selected_rows(rows);
            stats.rows_selected = rows;
            let mut map = GroupMap::new();
            if req.dims.is_empty() {
                let total: u64 = counts.iter().sum();
                if total > 0 {
                    let mut g = GroupState::new(0, 0);
                    g.count = total;
                    map.insert([0, 0], g);
                }
            } else {
                for (k, &c) in counts.iter().enumerate() {
                    if c > 0 {
                        let mut g = GroupState::new(0, 0);
                        g.count = c;
                        map.insert([k as i64, 0], g);
                    }
                }
            }
            crate::phase::add(Phase::AggRangeCounts, 1);
            since(Phase::AggFoldWall, t_fold);
            map
        } else {
            let t_select = std::time::Instant::now();
            // Row-range units, not one per opened segment. The fold below is
            // per row and its partial states merge commutatively, so a segment's
            // rows may be divided across workers; what must not change is the
            // order the units arrive in, and `select_units` returns them in
            // `(segment, row range)` order whichever worker claimed which.
            let (selections, _stats) = set.select_units(&scan_req)?;
            since(Phase::AggSelectWall, t_select);

            // the aggregate's own row threshold: the scan stages'
            // 4M gate kept a 1M-row fold on
            // one worker at ~29 ns/row; the width and unit terms are the
            // scan gate's
            let rows: u64 = selections.iter().map(|s| s.rows.len() as u64).sum();
            // The same number the gate already reads, also recorded on the
            // handle, so "rows this query walked" is a counter delta around one
            // call rather than a re-derivation from the answer.
            self.note_selected_rows(rows);
            stats.rows_selected = rows;
            let width = crate::scan::scan_threads();
            let width = if crate::scan::parallel_gate_at(width, selections.len(), 2, rows, agg_min_rows) {
                width
            } else {
                1
            };
            // How many partial tables the fold may hold at once, from what
            // the planner already knows — the rel table it just built, the
            // dictionary's dense id space, the label table, and the window over
            // the stride. The estimate is recorded beside the width it produced,
            // and `fold_map_bytes` records what the tables actually cost, so a
            // record says on every query whether the estimate was worth trusting.
            let layout = AggLayout::of(&req.aggs);
            let est_map_bytes = est_groups(
                req,
                rows,
                rel_names.len(),
                self.dict().len(),
                labels.as_ref().map(|l| l.names.len()).unwrap_or(0),
            )
            .saturating_mul(group_fixed_bytes(&layout));
            let map_cap = fold_width_cap(width, est_map_bytes);
            crate::phase::add(Phase::FoldWidthCap, map_cap as u64);
            crate::phase::add(Phase::FoldMapBytesEst, est_map_bytes);
            let t_fold = std::time::Instant::now();
            let map = aggregate_partials(
                &seg_refs,
                &selections,
                req,
                &global_rel,
                labels.as_ref(),
                // endpoint ids are dense over the dictionary, which is what
                // makes a per-group bitset possible at all
                DistinctPlan::for_space(self.dict().len()),
                width,
                map_cap,
            )?;
            since(Phase::AggFoldWall, t_fold);
            map
        };
        // the contract's SQL scalar-aggregate shape: an ungrouped call emits
        // exactly one row even over an empty window (count 0, stats null)
        if req.dims.is_empty() && map.is_empty() {
            let layout = AggLayout::of(&req.aggs);
            map.insert(
                [0, 0],
                GroupState::new(layout.distinct.len(), layout.stats.len()),
            );
        }

        let label_names = labels.map(|l| l.names).unwrap_or_default();
        let t_final = std::time::Instant::now();
        let mut out = finalize(map, req, rel_names, label_names, |codes| {
            // rank endpoint dense ids by uid code-point order: one sort of
            // the distinct ids present, never a per-row string
            let mut distinct: Vec<i64> = codes.to_vec();
            distinct.sort_unstable();
            distinct.dedup();
            let mut order: Vec<usize> = (0..distinct.len()).collect();
            order.sort_by_key(|&i| self.dict().uid(distinct[i] as u32));
            let mut rank = HashMap::with_capacity(distinct.len());
            for (r, &i) in order.iter().enumerate() {
                rank.insert(distinct[i], r as i64);
            }
            rank
        })?;
        since(Phase::AggFinalize, t_final);
        out.stats = stats;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derive::{edge_eid, version_vid};
    use crate::row::EdgeRow;
    use crate::scan::ScanSet;
    use crate::segment::{write_edge_segment, MemorySource, SegmentSpec};
    use std::path::PathBuf;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-agg-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn edge(vt_s: i64, vt_e: i64, i: u32, tt_s: i64, rel: &str, src: u32, dst: u32) -> EdgeRow {
        let disc = format!("#{i}");
        let eid = edge_eid("a", "b", rel, &disc);
        EdgeRow {
            vid: version_vid(&eid.to_hex(), tt_s, vt_s),
            src_id: src,
            dst_id: dst,
            rel_type: rel.into(),
            disc,
            vt_s,
            vt_e,
            tt_s,
            props: "{}".into(),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    struct Fixture {
        rows: Vec<EdgeRow>,
        segs: Vec<Segment<MemorySource>>,
    }

    impl Fixture {
        /// Three segments with overlapping vt ranges, mixed rels, some
        /// open-ended rows — the shapes every aggregate must handle.
        fn build(name: &str) -> Self {
            let dir = tmp(name);
            let mut rows = Vec::new();
            let mut segs = Vec::new();
            for (batch, tt, n) in [(0u32, 10i64, 300u32), (1, 20, 200), (2, 30, 57)] {
                let mut b: Vec<EdgeRow> = (0..n)
                    .map(|i| {
                        let vt_s = 1_000 + ((i * 7 + batch * 13) % 400) as i64;
                        let vt_e = if i % 5 == 0 {
                            OPEN_END
                        } else {
                            vt_s + 1 + (i % 23) as i64
                        };
                        let rel = ["RATED", "SENT", "PAID"][(i % 3) as usize];
                        edge(vt_s, vt_e, i + batch * 1000, tt, rel, i % 13, (i + 5) % 13)
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
    }

    fn req(dims: Vec<Dim>, aggs: Vec<Agg>) -> AggregateRequest {
        AggregateRequest {
            as_of_tt: OPEN_END,
            t_a: 1_050,
            t_b: 1_350,
            rel_types: None,
            dims,
            aggs,
            max_groups: 100_000,
        }
    }

    type Stat = (i64, i64, i128, u64);
    type RefRow = ([i64; 2], (u64, Vec<i64>, Vec<Option<Stat>>));

    /// Brute-force reference over the raw rows, written the obvious way.
    fn reference(rows: &[EdgeRow], r: &AggregateRequest,
                 rel_names: &[String]) -> Vec<RefRow> {
        #[allow(clippy::type_complexity)]
        let mut groups: HashMap<[i64; 2], (u64, Vec<Vec<u32>>, Vec<(i64, i64, i128, u64)>)> =
            HashMap::new();
        let layout = AggLayout::of(&r.aggs);
        for row in rows {
            if !(row.vt_s >= r.t_a && row.vt_s < r.t_b) {
                continue;
            }
            let mut key = [0i64; 2];
            for (k, d) in r.dims.iter().enumerate() {
                key[k] = match d {
                    Dim::TimeBucket { stride } => (row.vt_s - r.t_a) / stride,
                    Dim::RelType => rel_names
                        .iter()
                        .position(|x| *x == row.rel_type)
                        .unwrap() as i64,
                    Dim::Endpoint { dst } => {
                        (if *dst { row.dst_id } else { row.src_id }) as i64
                    }
                    Dim::Label { .. } => unreachable!("no label dim in core tests"),
                };
            }
            let g = groups.entry(key).or_insert_with(|| {
                (0, vec![Vec::new(); layout.distinct.len()],
                 vec![(i64::MAX, i64::MIN, 0, 0); layout.stats.len()])
            });
            g.0 += 1;
            for (slot, dst) in layout.distinct.iter().enumerate() {
                g.1[slot].push(if *dst { row.dst_id } else { row.src_id });
            }
            for (slot, src) in layout.stats.iter().enumerate() {
                let v = match src {
                    Source::VtS => row.vt_s,
                    Source::Duration => {
                        if row.vt_e >= OPEN_END {
                            continue;
                        }
                        row.vt_e - row.vt_s
                    }
                };
                let s = &mut g.2[slot];
                s.0 = s.0.min(v);
                s.1 = s.1.max(v);
                s.2 += v as i128;
                s.3 += 1;
            }
        }
        let mut out: Vec<_> = groups
            .into_iter()
            .map(|(k, (c, dis, st))| {
                let dcounts = dis
                    .into_iter()
                    .map(|mut v| {
                        v.sort_unstable();
                        v.dedup();
                        v.len() as i64
                    })
                    .collect();
                let stats = st.into_iter().map(|s| (s.3 > 0).then_some(s)).collect();
                (k, (c, dcounts, stats))
            })
            .collect();
        out.sort_by_key(|(k, _)| *k);
        out
    }

    /// Dense id space of the fixture (uids `0..13`), and a plan that keeps
    /// every group in the id-vector form.
    const IDS: u32 = 13;

    fn run_partials(f: &Fixture, r: &AggregateRequest, threads: usize) -> GroupMap {
        run_partials_with(f, r, DistinctPlan::for_space(IDS), threads)
    }

    fn run_partials_with(f: &Fixture, r: &AggregateRequest, plan: DistinctPlan,
                         threads: usize) -> GroupMap {
        let set = ScanSet::from_pairs(f.segs.iter().map(|s| (s, Lane::Event)).collect());
        let scan_req = ScanRequest {
            as_of_tt: r.as_of_tt,
            vt_min: Some(r.t_a),
            vt_max: Some(r.t_b),
            rel_types: r.rel_types.clone(),
            ..Default::default()
        };
        let (selections, _) = set.select(&scan_req).unwrap();
        let seg_refs: Vec<&Segment<MemorySource>> = f.segs.iter().collect();
        let rel_names = ["PAID", "RATED", "SENT"];
        let global_rel: HashMap<&str, i64> = rel_names
            .iter()
            .enumerate()
            .map(|(i, s)| (*s, i as i64))
            .collect();
        // `map_cap = threads`: this helper is about width, and the width
        // policy has its own tests.
        aggregate_partials(
            &seg_refs, &selections, r, &global_rel, None, plan, threads, threads,
        )
        .unwrap()
    }

    #[test]
    fn partials_match_the_reference_and_every_thread_width_agrees() {
        let f = Fixture::build("widths");
        let rel_names: Vec<String> =
            ["PAID", "RATED", "SENT"].iter().map(|s| s.to_string()).collect();
        let r = req(
            vec![Dim::RelType, Dim::TimeBucket { stride: 40 }],
            vec![
                Agg::Count,
                Agg::CountDistinct { dst: true },
                Agg::Min(Source::VtS),
                Agg::Max(Source::Duration),
                Agg::Mean(Source::Duration),
            ],
        );
        let want = reference(&f.rows, &r, &rel_names);

        let base = run_partials(&f, &r, 1);
        for threads in [2, 3, 5, 8] {
            let got = run_partials(&f, &r, threads);
            // identical group sets and identical finalized values at any
            // width — the distinct vectors may differ in order, which is
            // exactly what finalize's sort+dedup erases
            assert_eq!(got.len(), base.len(), "threads={threads}");
            let a = finalize(got, &r, rel_names.clone(), vec![], |_| HashMap::new()).unwrap();
            let b = finalize(base.clone(), &r, rel_names.clone(), vec![], |_| HashMap::new())
                .unwrap();
            assert_eq!(a, b, "threads={threads}");
        }

        // and the finalized answer equals the brute-force reference
        let out = finalize(base, &r, rel_names.clone(), vec![], |_| HashMap::new()).unwrap();
        assert_eq!(out.keys[0].len(), want.len());
        for (gi, (key, (count, dcounts, stats))) in want.iter().enumerate() {
            assert_eq!([out.keys[0][gi], out.keys[1][gi]], *key);
            let AggValues::Int(counts) = &out.aggs[0] else { panic!() };
            assert_eq!(counts[gi], Some(*count as i64));
            let AggValues::Int(d) = &out.aggs[1] else { panic!() };
            assert_eq!(d[gi], Some(dcounts[0]));
            let AggValues::Int(mn) = &out.aggs[2] else { panic!() };
            assert_eq!(mn[gi], stats[0].map(|s| s.0));
            let AggValues::Int(mx) = &out.aggs[3] else { panic!() };
            assert_eq!(mx[gi], stats[1].map(|s| s.1));
            let AggValues::Float(me) = &out.aggs[4] else { panic!() };
            assert_eq!(me[gi], stats[2].map(|s| mean(s.2, s.3)));
        }
    }

    /// The non-negotiable for this distinct rewrite: the answer must not
    /// depend on which representation a group happened to take, nor on how
    /// the folding was split. `promote_at` is the only knob separating the
    /// two forms, so forcing it — rather than relying on the heuristic to
    /// pick both — is what makes this a control; a middle
    /// value guarantees partials that promoted merging with partials that
    /// did not. Same shape as scan.rs's
    /// `every_width_materializes_the_identical_columns`.
    #[test]
    fn count_distinct_is_identical_in_both_representations_at_every_width() {
        let f = Fixture::build("distinct");
        let rel_names: Vec<String> =
            ["PAID", "RATED", "SENT"].iter().map(|s| s.to_string()).collect();
        let r = req(
            vec![Dim::RelType, Dim::TimeBucket { stride: 60 }],
            vec![
                Agg::Count,
                Agg::CountDistinct { dst: true },
                Agg::CountDistinct { dst: false },
            ],
        );
        let words = (IDS as usize).div_ceil(64).max(1);
        let ids_only = DistinctPlan { words, promote_at: usize::MAX };
        let base = finalize(
            run_partials_with(&f, &r, ids_only, 1),
            &r,
            rel_names.clone(),
            vec![],
            |_| HashMap::new(),
        )
        .unwrap();
        // and the id-vector form is the one the brute-force reference
        // mirrors, so anchor the pair to it before comparing them
        let want = reference(&f.rows, &r, &rel_names);
        let AggValues::Int(d_dst) = &base.aggs[1] else { panic!() };
        let AggValues::Int(d_src) = &base.aggs[2] else { panic!() };
        assert_eq!(d_dst.len(), want.len());
        for (gi, (_, (_, dcounts, _))) in want.iter().enumerate() {
            assert_eq!(d_dst[gi], Some(dcounts[0]));
            assert_eq!(d_src[gi], Some(dcounts[1]));
        }

        for promote_at in [1usize, 4, 37, usize::MAX] {
            for threads in [1usize, 2, 3, 5, 8] {
                let plan = DistinctPlan { words, promote_at };
                let got = finalize(
                    run_partials_with(&f, &r, plan, threads),
                    &r,
                    rel_names.clone(),
                    vec![],
                    |_| HashMap::new(),
                )
                .unwrap();
                assert_eq!(got, base, "promote_at={promote_at} threads={threads}");
            }
        }
    }

    /// A bitset sized from a stale dense-id space must still be exact: an id
    /// past its end grows it rather than being dropped, because a silently
    /// missing id is a wrong count and a panic here crosses into Python.
    #[test]
    fn an_id_past_the_planned_id_space_still_counts() {
        let mut d = Distinct::Ids(Vec::new());
        let plan = DistinctPlan { words: 1, promote_at: 1 };
        for id in [3u32, 3, 70, 4096, 70] {
            d.insert(id, &plan);
        }
        assert!(matches!(d, Distinct::Bits(_)));
        assert_eq!(d.cardinality(), 3);
    }

    #[test]
    fn group_cap_is_a_loud_capacity_error() {
        let f = Fixture::build("cap");
        let mut r = req(vec![Dim::Endpoint { dst: false }], vec![Agg::Count]);
        r.max_groups = 3;
        let map = run_partials(&f, &r, 1);
        let err = finalize(map, &r, vec![], vec![], |codes| {
            codes.iter().map(|&c| (c, c)).collect()
        })
        .unwrap_err();
        assert!(err.to_string().contains("exceeds cap 3"), "{err}");
    }

    #[test]
    fn ungrouped_request_folds_to_one_global_group() {
        let f = Fixture::build("global");
        let r = req(vec![], vec![Agg::Count, Agg::CountDistinct { dst: false }]);
        let map = run_partials(&f, &r, 1);
        assert_eq!(map.len(), 1);
        let want = reference(&f.rows, &r, &[]);
        let g = &map[&[0, 0]];
        assert_eq!(g.count, want[0].1 .0);
    }

    #[test]
    fn mean_uses_the_exact_integer_form() {
        // 2^60 + 1 over n=3: float accumulation would lose the +1
        let s: i128 = (1i128 << 60) * 3 + 2;
        let q = (s.div_euclid(3)) as f64;
        let r = (s.rem_euclid(3)) as f64;
        assert_eq!(mean(s, 3), q + r / 3.0);
    }

    // -- count by time bucket from believed ranges --------------------- //

    #[test]
    fn only_counts_by_time_bucket_take_the_range_path() {
        let r = |dims: Vec<Dim>, aggs: Vec<Agg>, t_a: i64, t_b: i64| AggregateRequest {
            as_of_tt: OPEN_END,
            t_a,
            t_b,
            rel_types: None,
            dims,
            aggs,
            max_groups: 10,
        };
        let tb = |stride| vec![Dim::TimeBucket { stride }];
        assert_eq!(range_count_buckets(&r(tb(10), vec![Agg::Count], 0, 100)), Some(10));
        assert_eq!(range_count_buckets(&r(tb(10), vec![Agg::Count], 0, 101)), Some(11));
        assert_eq!(range_count_buckets(&r(tb(7), vec![Agg::Count, Agg::Count], 5, 6)), Some(1));
        assert_eq!(range_count_buckets(&r(vec![], vec![Agg::Count], 0, 100)), Some(1));
        // a value per row, or a group per relation, keeps the fold
        assert_eq!(range_count_buckets(&r(tb(10), vec![Agg::CountDistinct { dst: true }], 0, 100)), None);
        assert_eq!(range_count_buckets(&r(tb(10), vec![Agg::Count, Agg::Min(Source::VtS)], 0, 100)), None);
        assert_eq!(range_count_buckets(&r(vec![Dim::RelType], vec![Agg::Count], 0, 100)), None);
        assert_eq!(
            range_count_buckets(&r(vec![Dim::TimeBucket { stride: 10 }, Dim::RelType], vec![Agg::Count], 0, 100)),
            None
        );
        assert_eq!(range_count_buckets(&r(tb(10), vec![], 0, 100)), None);
        // degenerate shapes stay where they were
        assert_eq!(range_count_buckets(&r(tb(0), vec![Agg::Count], 0, 100)), None);
        assert_eq!(range_count_buckets(&r(tb(10), vec![Agg::Count], 100, 100)), None);
        assert_eq!(range_count_buckets(&r(tb(1), vec![Agg::Count], 0, 1 << 40)), None);
    }

    /// The range count is the fold's count, bucket for bucket, and its row
    /// counter is the scan's `rows_selected` — over interval rows straddling
    /// `t_a`, open-ended rows, three belief instants, a relation filter that
    /// splits segments, one that allows everything and one that allows
    /// nothing, strides from one tick to wider than the window, and the
    /// ungrouped form. Mutating the bucket edge, the `t_a` cut, the prefix
    /// cursor or the below-`t_a` counter each fails here.
    #[test]
    fn range_counts_equal_the_fold() {
        let f = Fixture::build("rangecount");
        let set = ScanSet::from_pairs(f.segs.iter().map(|s| (s, Lane::Event)).collect());
        let seg_refs: Vec<&Segment<MemorySource>> = f.segs.iter().collect();
        let global_rel: HashMap<&str, i64> =
            [("PAID", 0i64), ("RATED", 1), ("SENT", 2)].into_iter().collect();
        let mut nonempty = 0;
        for as_of in [15, 25, OPEN_END] {
            for (t_a, t_b) in [(1_050, 1_350), (0, 5_000), (1_100, 1_101), (1_399, 1_500), (900, 1_000), (1_200, 1_230)] {
                for rels in [
                    None,
                    Some(vec!["SENT".to_string()]),
                    Some(vec!["PAID".to_string(), "RATED".to_string(), "SENT".to_string()]),
                    Some(vec!["NONE".to_string()]),
                ] {
                    for dims in [
                        vec![Dim::TimeBucket { stride: 1 }],
                        vec![Dim::TimeBucket { stride: 7 }],
                        vec![Dim::TimeBucket { stride: 40 }],
                        vec![Dim::TimeBucket { stride: 100_000 }],
                        vec![],
                    ] {
                        let r = AggregateRequest {
                            as_of_tt: as_of,
                            t_a,
                            t_b,
                            rel_types: rels.clone(),
                            dims: dims.clone(),
                            aggs: vec![Agg::Count],
                            max_groups: 1 << 20,
                        };
                        let scan_req = ScanRequest {
                            as_of_tt: as_of,
                            vt_min: Some(t_a),
                            vt_max: Some(t_b),
                            rel_types: rels.clone(),
                            ..Default::default()
                        };
                        let (sel, _) = set.select(&scan_req).unwrap();
                        let scanned: u64 = sel.iter().map(|x| x.rows.len() as u64).sum();
                        let fold = aggregate_partials(
                            &seg_refs, &sel, &r, &global_rel, None,
                            DistinctPlan::for_space(IDS), 1, 1,
                        )
                        .unwrap();
                        let n = range_count_buckets(&r).expect("eligible");
                        let (plans, _) = set.plan_believed(&scan_req).unwrap();
                        let (counts, selected) = range_counts(&seg_refs, &plans, &r, n).unwrap();
                        let ctx = format!("as_of={as_of} [{t_a},{t_b}) rels={rels:?} dims={dims:?}");
                        assert_eq!(selected, scanned, "rows_selected: {ctx}");
                        let mut want = vec![0u64; n];
                        for (key, g) in &fold {
                            want[key[0] as usize] += g.count;
                        }
                        assert_eq!(counts, want, "{ctx}");
                        nonempty += usize::from(counts.iter().any(|&c| c > 0));
                    }
                }
            }
        }
        assert!(nonempty > 100, "the probe must not be vacuous ({nonempty})");
    }
}
