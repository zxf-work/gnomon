//! `diff_belief` — what changed *about what we believe*, between two belief
//! times, inside one valid-time window.
//!
//! # The question this answers, and why it is not `diff_snapshots`
//!
//! `gnomon/temporal/ops_snapshot.py::diff_snapshots` takes **one** `as_of_tt`
//! and **two** valid-time instants: it is a *valid-time* diff under a fixed
//! belief — "how did the world change between `t1` and `t2`, as we believe it
//! today". The dual question — "how did our *beliefs* change between `T1` and
//! `T2`, about the world in `[t_a, t_b)`" — has no single call today, and it
//! is the one a correction log makes interesting. This module is that call.
//!
//! # Semantics
//!
//! A version is *believed at* `as_of` exactly when `tt_s <= as_of < tt_e`
//! (`crate::believed_at`, and the two halves `scan::believed_ranges` and
//! `visibility::close_view` compute). For belief times `T1 < T2`,
//! `diff_belief(T1, T2, [t_a, t_b))` returns every version whose valid-time
//! interval overlaps `[t_a, t_b)` and whose belief state moved:
//!
//! * **`Added`** — `tt_s ∈ (T1, T2]`. We did not have this version at `T1`;
//!   we had written it by `T2`. Reported at `tt = tt_s`.
//! * **`Retracted`** — effective `tt_e ∈ (T1, T2]`. We still believed this
//!   version at `T1`'s side of the boundary; a correction had closed it by
//!   `T2`. Reported at `tt = tt_e`.
//!
//! The two tags are independent predicates, not a partition, and that is
//! deliberate. A version written *and* corrected inside the window — `tt_s`
//! and `tt_e` both in `(T1, T2]` — changed belief state **twice**, and emits
//! two rows. A set difference of the two believed populations cannot say
//! that: such a version is in neither `believed(T1)` nor `believed(T2)`, so
//! the difference silently drops it. The relationship is exact and is pinned
//! by [`tests::the_tags_refine_the_two_set_differences`]:
//!
//! ```text
//!   believed(T2) \ believed(T1)  ==  { Added,     still open at T2 }
//!   believed(T1) \ believed(T2)  ==  { Retracted, written by T1    }
//! ```
//!
//! so each set difference is the tag restricted to versions that were on the
//! *other* side of the window, and the two tags are their union plus the
//! versions that came and went inside it.
//!
//! # Cost
//!
//! Per segment, and **never a row scan of the store**. Both belief tests are
//! computed for the window's row range only, never for the segment:
//!
//! * `Added` is [`added_ranges_in`] — the `tt_s` runs (or, on a
//!   dictionary-coded segment, the codes of the blocks the belief times
//!   split) **inside the window's rows**, entered by binary search, with the
//!   folded `(tt_min, tt_max)` bounds answering the all/none cases without
//!   reading anything. A segment written entirely before `T1` — which is
//!   nearly all of them, for any `T1` past the bulk load — costs two
//!   comparisons.
//! * `Retracted` is a filter of
//!   [`crate::visibility::close_view_in`]'s sorted `(row, tt_e)` slice, built
//!   for the window's rows: `O(log closes + closes in the window)`. A segment
//!   nobody has corrected has `CloseView::None` and costs nothing.
//!
//! An earlier version computed both over the whole segment and clipped to the
//! window afterwards, which made a 10-tick window on a fully compacted store
//! read every one of the window segment's 6.5M `tt_s` codes.
//!
//! The valid-time window is then pushed into the *same* range domain rather
//! than being a filter over whatever the belief tests produced, and that is
//! load-bearing rather than an optimisation. Without it the `Added` candidate
//! set is every row written since `T1` — at a 10 % correction density over
//! 10M rows, a million rows — and the operator's cost would follow the
//! correction count even for a window holding ten of them. Rows are stored in
//! `(vt_s, vid)` order (`row::SortKey`, and compaction re-sorts into it), so:
//!
//! * a segment whose `[vt_min, vt_e_max)` misses the window is skipped whole,
//!   from the header, without reading a column;
//! * `vt_s >= t_b` is a *suffix* of the segment, cut by one binary search;
//! * when `vt_e` is elided — every row instantaneous, which is the whole
//!   event lane — `vt_e > t_a` is exactly `vt_s >= t_a`, so the other end is
//!   one binary search too.
//!
//! An interval-lane segment with an explicit `vt_e` keeps the per-row test on
//! its lower end, because deciding that in the range domain is what the
//! `block_prefix_max_vt_e` staircase exists for and this operator does not
//! reach into it.
//!
//! so the whole operator is `O(segments × log rows)` plus the runs (or split
//! codes) and closes **inside the window's rows**, plus the rows that
//! actually fall in the window — never a row scan of the store, and never a
//! walk of a segment's correction history outside the window. A window that
//! covers a whole segment pays that segment's runs and closes, as before.
//!
//! # Layout independence
//!
//! Every input is a *logical* fact: `tt_s` runs, the effective close set, and
//! the row's own `vt_s`/`vt_e`. None of `compact_incremental`, `settle`, the
//! lane assignment or the segment sizing changes any of them — compaction
//! re-sorts rows and folds close runs into sidecars, and `close_view` resolves
//! a folded close and a run close to the same `tt_e`. The output is sorted by
//! `(kind, key, tt, change, vid)`, a total order over logical fields with no
//! physical component, so two stores holding the same history answer
//! identically whatever their layout. `tests::settle_and_compaction_do_not_move_the_answer`
//! is the guard.

use std::collections::HashMap;

use crate::derive::{edge_eid, Id96};
use crate::error::{EngineError, Result};
use crate::row::RowKind;
use crate::store::NativeStore;
use crate::OPEN_END;

/// Which way a version's belief state moved across `(T1, T2]`.
///
/// Ordered `Added < Retracted` so a version that did both reports its arrival
/// before its retraction under the output's tie-break; the `tt` field orders
/// them the same way, because `tt_s <= tt_e` always.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BeliefChange {
    /// `tt_s ∈ (T1, T2]` — it came to be believed inside the window.
    Added,
    /// effective `tt_e ∈ (T1, T2]` — it stopped being believed inside it.
    Retracted,
}

impl BeliefChange {
    /// The wire spelling, shared with the PyO3 layer and the Python reference
    /// evaluator so the three cannot drift.
    pub fn as_str(self) -> &'static str {
        match self {
            BeliefChange::Added => "added",
            BeliefChange::Retracted => "retracted",
        }
    }
}

/// One belief-state change of one version.
///
/// `key` is the **logical identity** the change is attributed to — a node's
/// `uid`, an edge's `eid` — and `vid` is the version it happened to. Both are
/// carried because the question "which entity did our beliefs about change?"
/// and "which stored version moved?" have different answers when a correction
/// replaces one version of an identity with another.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeliefDiffRow {
    pub kind: RowKind,
    pub key: String,
    pub vid: String,
    /// `tt_s` for [`BeliefChange::Added`], effective `tt_e` for
    /// [`BeliefChange::Retracted`].
    pub tt: i64,
    pub change: BeliefChange,
    pub vt_s: i64,
    pub vt_e: i64,
}

impl BeliefDiffRow {
    /// The total order the operator's answer is defined in: `(kind, key, tt,
    /// change, vid)`. Purely logical — see the module note on layout
    /// independence. `kind` sorts nodes before edges.
    fn order_key(&self) -> (u8, &str, i64, BeliefChange, &str) {
        let k = match self.kind {
            RowKind::Node => 0u8,
            RowKind::Edge => 1u8,
        };
        (k, &self.key, self.tt, self.change, &self.vid)
    }
}

/// `diff_belief`'s answer: an ordered page, plus the exact totals behind it.
///
/// The totals are counted over the whole surviving population, not over the
/// page, for the same reason `version_history` reports `rows_total`: a paged
/// answer whose totals were also paged could not be compared against a
/// reference evaluator.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BeliefDiffOut {
    pub rows: Vec<BeliefDiffRow>,
    pub added_total: usize,
    pub retracted_total: usize,
    pub truncated: bool,
}

/// Row ranges whose `tt_s` falls in `(t1, t2]` — the `Added` candidates.
///
/// This is, by construction and by test, the set difference of the two
/// verdict range lists the belief predicate's first half produces:
///
/// ```text
///   added_ranges(tt, t1, t2)  ==  believed_ranges(tt, t2) \ believed_ranges(tt, t1)
/// ```
///
/// It is computed as one pass rather than as two passes and a subtraction
/// because a compacted segment's runs are the expensive thing to walk — the
/// global `(vt_s, vid)` re-sort shatters them, and 2.1M runs over 3.0M rows
/// has been measured on this engine. Walking them once instead of
/// twice halves the only term in the cost that is not `O(1)`.
///
/// The three short-circuits are the same ones `believed_ranges` takes, and
/// they are what makes the common case free: `tt_max <= t1` is "this segment
/// was fully written before the window opened", which is every segment of the
/// bulk load for any interesting `t1`, and it returns without reading a run.
///
/// The output is ascending, disjoint, and coalesced across adjacent runs that
/// share a verdict — the same contract `subtract_closes` consumes.
/// `a \\ b` over ascending, disjoint range lists.
///
/// Promoted out of the test module: the dictionary path of [`added_ranges`]
/// evaluates the operator's own contract, so what used to be the *oracle*
/// for the run-list path is now the *implementation* for the other shape.
/// The test that compares the two still uses it, which is sound only because
/// it is checked independently against a row-by-row walk.
///
/// One forward cursor into `b`, so the walk is `O(|a| + |b|)`. The
/// previous form restarted `b` from its first range for every range of
/// `a`, which is `|a| x |b|` comparisons: harmless while one side was the
/// single range a fully believed segment produces, and about 10^11 steps
/// on a fully compacted segment where both belief times split every code
/// block. Both inputs ascend and are disjoint, so a range of `b` that ends
/// at or before the start of the current range of `a` ends before every
/// later one too, and is never looked at again.
pub(crate) fn range_subtract(a: &[(usize, usize)], b: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let mut out: Vec<(usize, usize)> = Vec::new();
    let mut steps = 0usize;
    let mut j = 0usize;
    for &(mut s, e) in a {
        while j < b.len() && b[j].1 <= s {
            j += 1;
            steps += 1;
        }
        let mut k = j;
        while k < b.len() && b[k].0 < e {
            steps += 1;
            let (bs, be) = b[k];
            if bs > s {
                push_coalesced(&mut out, (s, bs.min(e)));
            }
            s = s.max(be);
            if s >= e {
                break;
            }
            k += 1;
        }
        if s < e {
            push_coalesced(&mut out, (s, e));
        }
    }
    crate::vistouch::steps(steps);
    out
}

fn push_coalesced(out: &mut Vec<(usize, usize)>, r: (usize, usize)) {
    if r.0 >= r.1 {
        return;
    }
    match out.last_mut() {
        Some(last) if last.1 == r.0 => last.1 = r.1,
        _ => out.push(r),
    }
}

/// **Takes the segment's `TtSView`, never a bare `&SegmentHeader`.** The segment
/// stores `tt_s` as a dictionary-coded column once the run density crosses
/// its threshold, and on such a segment the header's run list is *dropped*
/// rather than kept alongside. A header-derived implementation therefore
/// walks an empty run list and reports that nothing was added — silently,
/// and only on the segments where the dictionary fired, which is to say
/// exactly the interesting ones. That is a wrong answer rather than a
/// failure, so the shape is resolved once, here, and the two paths are
/// checked against each other in
/// `diff_belief_agrees_between_dictionary_and_run_list_segments`.
pub(crate) fn added_ranges(
    tt: &crate::ttime::TtSView<'_>,
    tt_bounds: (i64, i64),
    t1: i64,
    t2: i64,
    rows: usize,
) -> Vec<(usize, usize)> {
    let runs = tt.runs();
    // The dictionary shape has no runs to walk. Its answer is the operator's
    // own contract, evaluated through `believed_ranges`, which takes the
    // per-block code bounds and only descends into a block straddling the
    // cut — so this is not a fallback to a slow path, it is the fast path
    // for that shape.
    if tt.is_dictionary() {
        return range_subtract(
            &crate::scan::believed_ranges(tt, tt_bounds, t2, rows),
            &crate::scan::believed_ranges(tt, tt_bounds, t1, rows),
        );
    }
    let Some(&(first, _)) = runs.first() else {
        return Vec::new();
    };
    let (tt_min, tt_max) = tt_bounds;
    // nothing in this segment had been written by t2
    if tt_min > t2 {
        return Vec::new();
    }
    // everything in it had already been written by t1
    if tt_max <= t1 {
        return Vec::new();
    }
    // every row of it was written strictly inside (t1, t2]
    if tt_min > t1 && tt_max <= t2 {
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
        if *tt <= t1 || *tt > t2 || start >= end {
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

/// [`added_ranges`] restricted to the rows `[lo, hi)`.
///
/// Exactly `added_ranges(..)` intersected with `[lo, hi)`: whether a row
/// was added in `(t1, t2]` depends on its own `tt_s` alone. A whole-segment
/// request takes [`added_ranges`] itself. Otherwise:
///
/// - dictionary: the operator's own definition,
///   `believed(t2) \ believed(t1)`, evaluated over the range only, so each
///   side reads the covering code blocks and at most `hi - lo` codes;
/// - run list: the same three folded-bounds short-cuts, then the runs from
///   the one covering `lo` to the first starting at or after `hi`.
pub(crate) fn added_ranges_in(
    tt: &crate::ttime::TtSView<'_>,
    tt_bounds: (i64, i64),
    t1: i64,
    t2: i64,
    rows: usize,
    lo: usize,
    hi: usize,
) -> Vec<(usize, usize)> {
    if crate::scan::is_whole(lo, hi, rows) {
        return added_ranges(tt, tt_bounds, t1, t2, rows);
    }
    let hi = hi.min(rows);
    if lo >= hi {
        return Vec::new();
    }
    if tt.is_dictionary() {
        return range_subtract(
            &crate::scan::believed_ranges_in(tt, tt_bounds, t2, rows, lo, hi),
            &crate::scan::believed_ranges_in(tt, tt_bounds, t1, rows, lo, hi),
        );
    }
    let runs = tt.runs();
    let Some(&(first, _)) = runs.first() else {
        return Vec::new();
    };
    let (tt_min, tt_max) = tt_bounds;
    if tt_min > t2 || tt_max <= t1 {
        return Vec::new();
    }
    if tt_min > t1 && tt_max <= t2 {
        let a = lo.max(first as usize);
        return if a < hi { vec![(a, hi)] } else { Vec::new() };
    }
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
        if tt <= t1 || tt > t2 || a >= b {
            continue;
        }
        push_coalesced(&mut out, (a, b));
    }
    crate::vistouch::runs(i - first_i);
    out
}

/// Does `[vt_s, vt_e)` overlap the half-open window `[t_a, t_b)`?
///
/// Spelled once so the segment path, the staged path and the doc comment
/// cannot disagree; identical to the test `version_keys` applies.
#[inline]
fn in_window(vt_s: i64, vt_e: i64, t_a: i64, t_b: i64) -> bool {
    vt_s < t_b && vt_e > t_a
}

/// The row range of one segment that `[t_a, t_b)` can possibly touch.
///
/// Exact on one end always and on both when `vt_e` is elided; see the module
/// note. Never *narrower* than the truth, so `in_window` still runs over what
/// comes back and the answer does not depend on this being tight — only its
/// cost does. Relies on rows being stored in `vt_s` order, which is the
/// engine's physical sort key and what `scan::select_range` and the staircase
/// already assume; the `debug_assert` makes a violation a test failure rather
/// than a wrong answer discovered in production.
pub(crate) fn window_rows(vt_s: &[i64], vt_e_elided: bool, t_a: i64, t_b: i64) -> (usize, usize) {
    debug_assert!(
        vt_s.windows(2).all(|w| w[0] <= w[1]),
        "diff_belief assumes rows are stored in vt_s order"
    );
    // vt_s >= t_b is a suffix: no such row can start before the window ends.
    let hi = vt_s.partition_point(|v| *v < t_b);
    // vt_e > t_a is only a prefix-complement when vt_e is implied by vt_s.
    let lo = if vt_e_elided {
        // `vt_e = vt_s + 1`, so `vt_e > t_a` is `vt_s + 1 > t_a` is
        // `vt_s >= t_a`; spelled as the strict form so nothing adds to an
        // i64 that may already be at its maximum.
        vt_s.partition_point(|v| *v < t_a)
    } else {
        0
    };
    (lo.min(hi), hi)
}

/// One candidate row, before its strings are paid for.
///
/// Same trade as [`crate::read::VersionKey`]: the belief tests produce far
/// more candidates than the window survives, and an `eid` costs a sha256, so
/// nothing is materialized until a row has passed every filter.
#[derive(Clone, Copy)]
struct Candidate {
    row: u32,
    tt: i64,
    change: BeliefChange,
}

impl NativeStore {
    /// Every version whose belief state changed between `t1` and `t2`, inside
    /// the valid-time window `[t_a, t_b)`.
    ///
    /// See the module documentation for the semantics, the cost argument and
    /// the layout-independence argument. `limit` pages the ordered answer;
    /// `added_total` / `retracted_total` always count the whole population.
    ///
    /// `t1 == t2` is a legal, empty query rather than an error: a window of
    /// zero width contains no belief change, and a caller sweeping a sequence of
    /// belief instants should not have to special-case its own first step.
    /// `t1 > t2` is refused — it is an argument the caller got backwards, not
    /// a question with an empty answer.
    pub fn diff_belief(
        &self,
        t1: i64,
        t2: i64,
        t_a: i64,
        t_b: i64,
        limit: usize,
    ) -> Result<BeliefDiffOut> {
        if t1 > t2 {
            return Err(EngineError::invariant(format!(
                "diff_belief needs t1 <= t2, got t1={t1} t2={t2}"
            )));
        }
        if t_a > t_b {
            return Err(EngineError::invariant(format!(
                "diff_belief needs t_a <= t_b, got t_a={t_a} t_b={t_b}"
            )));
        }
        let mut rows: Vec<BeliefDiffRow> = Vec::new();
        if t1 < t2 && t_a < t_b {
            self.diff_belief_kind(RowKind::Node, t1, t2, t_a, t_b, &mut rows)?;
            self.diff_belief_kind(RowKind::Edge, t1, t2, t_a, t_b, &mut rows)?;
        }

        let added_total = rows
            .iter()
            .filter(|r| r.change == BeliefChange::Added)
            .count();
        let retracted_total = rows.len() - added_total;
        // Sorted by the logical key alone, so the physical order segments were
        // visited in is not observable. `sort_unstable_by` is safe here
        // because `order_key` ends in `vid`, which is unique per version per
        // tag: no two rows compare equal.
        rows.sort_unstable_by(|a, b| a.order_key().cmp(&b.order_key()));
        let truncated = rows.len() > limit;
        rows.truncate(limit);
        Ok(BeliefDiffOut {
            rows,
            added_total,
            retracted_total,
            truncated,
        })
    }

    /// One kind's half of [`NativeStore::diff_belief`].
    ///
    /// Kept as its own function rather than inlined twice because the node and
    /// edge branches differ only in which columns resolve the key, and the
    /// belief arithmetic above that must provably be the same for both.
    fn diff_belief_kind(
        &self,
        kind: RowKind,
        t1: i64,
        t2: i64,
        t_a: i64,
        t_b: i64,
        out: &mut Vec<BeliefDiffRow>,
    ) -> Result<()> {
        let closes = self.close_index()?;
        let files = match kind {
            RowKind::Edge => self.edge_files(),
            RowKind::Node => self.node_files(),
        };
        // Reused across segments so a large candidate set costs one allocation
        // for the whole operator rather than one per segment.
        let mut cands: Vec<Candidate> = Vec::new();

        for (file, seg_id) in files {
            let seg = self.open_segment(&file)?;
            let h = seg.header();
            // Segment-level window prune, from the header alone: no column is
            // read, and for a narrow window this is every segment but one.
            if h.vt_min >= t_b || h.vt_e_max <= t_a {
                continue;
            }
            let vt_s = seg.i64_column("vt_s")?;
            let n_rows = vt_s.len();
            if n_rows == 0 {
                continue;
            }
            let (w_lo, w_hi) = window_rows(vt_s, h.vt_e_elided, t_a, t_b);
            if w_lo >= w_hi {
                continue;
            }
            // The belief state is computed for the window's rows only.
            // Rows are stored in `vt_s` order, so `[w_lo, w_hi)` is the only
            // part of the segment the window can use, and every belief test
            // below is a per-row predicate: restricting first gives the same
            // rows as clipping afterwards. A window covering the whole
            // segment takes the whole-segment code unchanged.
            let view = crate::visibility::close_view_range(
                seg.sidecar(),
                &closes,
                seg_id,
                w_lo,
                w_hi,
                n_rows,
            );

            cands.clear();
            // --- Added: tt_s ∈ (t1, t2], O(runs), usually O(1) ------------ //
            // Intersected with the window's row range as the ranges are
            // walked, so a correction history outside the window is never
            // expanded into candidates at all.
            // Resolved once per segment, not per row: on the dictionary
            // shape this looks the code column up by name. Both the range
            // computation and the per-row `tt_s` read go through it, because
            // both are shape-dependent and `SegmentHeader::tt_s_at` is
            // documented as runs-only.
            let tt = seg.tt_s_view()?;
            for (rs, re) in added_ranges_in(&tt, tt.bounds(), t1, t2, n_rows, w_lo, w_hi) {
                for i in rs.max(w_lo)..re.min(w_hi) {
                    cands.push(Candidate {
                        row: i as u32,
                        // read from whichever shape stores it, not from a
                        // column: `tt_s` is not stored per row
                        tt: tt.at(i as u32)?,
                        change: BeliefChange::Added,
                    });
                }
            }
            // --- Retracted: effective tt_e ∈ (t1, t2], O(closes) --------- //
            // `close_view` has already resolved run-over-sidecar precedence
            // exactly as the per-row belief test does, so this slice *is* the
            // effective close set and no second lookup is needed.
            let walk: &[(u32, i64)] = if crate::scan::is_whole(w_lo, w_hi, n_rows) {
                view.as_slice()
            } else {
                crate::visibility::rows_in(view.as_slice(), w_lo, w_hi)
            };
            crate::vistouch::closes(walk.len());
            for &(row, tt_e) in walk {
                // A stale run can name a row past this segment's end; the
                // range path steps over such rows in its gaps, and this path
                // has to drop them explicitly (it only can when the window is
                // the whole segment: `close_view_in` never returns a row
                // outside `[w_lo, w_hi)`). The list is keyed by row and rows
                // are stored in `vt_s` order, so the window's rows are a
                // contiguous slice of it, found by binary search.
                let r = row as usize;
                if r >= n_rows || r < w_lo || r >= w_hi {
                    continue;
                }
                if tt_e > t1 && tt_e <= t2 && tt_e != OPEN_END {
                    cands.push(Candidate {
                        row,
                        tt: tt_e,
                        change: BeliefChange::Retracted,
                    });
                }
            }
            if cands.is_empty() {
                continue;
            }

            // --- valid-time window, then materialize --------------------- //
            let vt_e = if h.vt_e_elided {
                None
            } else {
                Some(seg.i64_column("vt_e")?)
            };
            let vid_hi = seg.u64_column("vid64")?;
            let vid_lo = seg.u32_column("vid_lo32")?;
            match kind {
                RowKind::Node => {
                    let uid_id = seg.u32_column("uid_id")?;
                    for c in &cands {
                        let i = c.row as usize;
                        // `vt_e` elided means every row is instantaneous,
                        // exactly as `version_keys` reads it
                        let ve = vt_e.map(|col| col[i]).unwrap_or(vt_s[i] + 1);
                        if !in_window(vt_s[i], ve, t_a, t_b) {
                            continue;
                        }
                        out.push(BeliefDiffRow {
                            kind,
                            key: self.uid_of(uid_id[i])?,
                            vid: Id96 {
                                hi: vid_hi[i],
                                lo: vid_lo[i],
                            }
                            .to_hex(),
                            tt: c.tt,
                            change: c.change,
                            vt_s: vt_s[i],
                            vt_e: ve,
                        });
                    }
                }
                RowKind::Edge => {
                    let strings = seg.strings()?;
                    let src = seg.u32_column("src_id")?;
                    let dst = seg.u32_column("dst_id")?;
                    let rel = seg.u16_column("rel_code")?;
                    let disc = seg.u32_column("disc_ref")?;
                    for c in &cands {
                        let i = c.row as usize;
                        let ve = vt_e.map(|col| col[i]).unwrap_or(vt_s[i] + 1);
                        if !in_window(vt_s[i], ve, t_a, t_b) {
                            continue;
                        }
                        let src_uid = self.uid_of(src[i])?;
                        let dst_uid = self.uid_of(dst[i])?;
                        let rel_type = h
                            .rel_types
                            .get(rel[i] as usize)
                            .ok_or_else(|| {
                                EngineError::corrupt(format!("rel_code {} has no entry", rel[i]))
                                    .at_row(i as u32)
                            })?
                            .clone();
                        let disc_s = strings.get(disc[i])?.to_string();
                        out.push(BeliefDiffRow {
                            kind,
                            key: edge_eid(&src_uid, &dst_uid, &rel_type, &disc_s).to_hex(),
                            vid: Id96 {
                                hi: vid_hi[i],
                                lo: vid_lo[i],
                            }
                            .to_hex(),
                            tt: c.tt,
                            change: c.change,
                            vt_s: vt_s[i],
                            vt_e: ve,
                        });
                    }
                }
            }
        }

        self.diff_belief_staged(kind, t1, t2, t_a, t_b, out)
    }

    /// The open batch's contribution, which a batch must see for the same
    /// reason `version_keys` unions it in: a batch reads its own writes, and
    /// a bi-temporal op that cannot see the corrections it has itself just
    /// recorded produces a wrong answer.
    ///
    /// Bounded by the batch, never by the store, so it does not weaken the
    /// cost claim. Every committed path reaches this with an empty batch.
    fn diff_belief_staged(
        &self,
        kind: RowKind,
        t1: i64,
        t2: i64,
        t_a: i64,
        t_b: i64,
        out: &mut Vec<BeliefDiffRow>,
    ) -> Result<()> {
        if !self.in_batch() {
            return Ok(());
        }
        let staged_closes: &HashMap<Id96, i64> = self.staged_closes();
        let push = |kind: RowKind,
                        key: String,
                        vid: Id96,
                        vt_s: i64,
                        vt_e: i64,
                        tt_s: i64,
                        out: &mut Vec<BeliefDiffRow>| {
            if !in_window(vt_s, vt_e, t_a, t_b) {
                return;
            }
            if tt_s > t1 && tt_s <= t2 {
                out.push(BeliefDiffRow {
                    kind,
                    key: key.clone(),
                    vid: vid.to_hex(),
                    tt: tt_s,
                    change: BeliefChange::Added,
                    vt_s,
                    vt_e,
                });
            }
            let tt_e = staged_closes.get(&vid).copied().unwrap_or(OPEN_END);
            if tt_e != OPEN_END && tt_e > t1 && tt_e <= t2 {
                out.push(BeliefDiffRow {
                    kind,
                    key,
                    vid: vid.to_hex(),
                    tt: tt_e,
                    change: BeliefChange::Retracted,
                    vt_s,
                    vt_e,
                });
            }
        };
        match kind {
            RowKind::Node => {
                for r in self.staged_nodes() {
                    let uid = self.uid_of(r.uid_id)?;
                    push(kind, uid, r.vid, r.vt_s, r.vt_e, r.tt_s, out);
                }
            }
            RowKind::Edge => {
                for r in self.staged_edges() {
                    let src = self.uid_of(r.src_id)?;
                    let dst = self.uid_of(r.dst_id)?;
                    let eid = edge_eid(&src, &dst, &r.rel_type, &r.disc).to_hex();
                    push(kind, eid, r.vid, r.vt_s, r.vt_e, r.tt_s, out);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derive::version_vid;
    use crate::manifest::EventLogRef;
    use crate::read::{Belief, VersionFilter};
    use crate::row::{EdgeRow, NodeRow};
    use crate::scan::believed_ranges;
    use std::path::PathBuf;

    fn tmp_root(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-beliefdiff-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    /// Everything but `tt_s`, which the caller varies.
    fn edge_row(src_id: u32, dst_id: u32, src: &str, dst: &str, i: u32, tt_s: i64) -> EdgeRow {
        let disc = format!("#{i}");
        let eid = edge_eid(src, dst, "R", &disc);
        let vt_s = i as i64;
        EdgeRow {
            vid: version_vid(&eid.to_hex(), tt_s, vt_s),
            src_id,
            dst_id,
            rel_type: "R".into(),
            disc,
            vt_s,
            vt_e: vt_s + 1,
            tt_s,
            props: format!(r#"{{"i":{i}}}"#),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    fn node_row(uid_id: u32, i: u32, tt_s: i64) -> NodeRow {
        let vt_s = i as i64;
        NodeRow {
            vid: version_vid(&format!("node-{uid_id}-{i}"), tt_s, vt_s),
            uid_id,
            label: "Node".into(),
            vt_s,
            vt_e: vt_s + 1,
            tt_s,
            props: "{}".into(),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    /// The fixture every belief test runs against.
    ///
    /// Three ingest batches at `tt = 100, 200, 300` of four edges and two node
    /// versions each, then three corrections at `tt = 400, 500, 600` against
    /// rows from different batches. A fourth batch at `tt = 700` writes a row
    /// **and** closes it at `tt = 800` — the *ephemeral* shape, a version whose
    /// whole believed life fits inside one belief window, which is the case a
    /// plain set difference of the two believed populations cannot see.
    ///
    /// `target` is small so the store keeps many under-target segments: the
    /// tiered layout is the one the layout-independence tests compare against.
    fn seeded(name: &str) -> (PathBuf, NativeStore, Vec<EdgeRow>) {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        let (partitions, _) = {
            let (p, t) = s.layout();
            (*p, t)
        };
        s.set_layout(partitions, 4096);
        let mut all: Vec<EdgeRow> = Vec::new();
        for batch in 0..3u32 {
            let tt = 100 + batch as i64 * 100;
            s.begin(tt).unwrap();
            let a = s.ensure_entity(&format!("n{batch}a"), "Node").unwrap();
            let b = s.ensure_entity(&format!("n{batch}b"), "Node").unwrap();
            for (k, uid_id) in [a, b].into_iter().enumerate() {
                s.stage_node(node_row(uid_id, batch * 2 + k as u32, tt)).unwrap();
            }
            for i in 0..4u32 {
                let r = edge_row(a, b, &format!("n{batch}a"), &format!("n{batch}b"),
                                 batch * 4 + i, tt);
                all.push(r.clone());
                s.stage_edge(r).unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        // three corrections, each in its own transaction, against rows from
        // three different ingest batches — so the close set spans segments
        for (tt, idx) in [(400i64, 1usize), (500, 5), (600, 9)] {
            s.begin(tt).unwrap();
            s.close_version(RowKind::Edge, all[idx].vid, tt).unwrap();
            s.commit(EventLogRef::default()).unwrap();
        }
        // the ephemeral row: written at 700, closed at 800
        s.begin(700).unwrap();
        let e = s.ensure_entity("ephem", "Node").unwrap();
        let r = edge_row(e, e, "ephem", "ephem", 99, 700);
        all.push(r.clone());
        s.stage_edge(r).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        s.begin(800).unwrap();
        s.close_version(RowKind::Edge, all[all.len() - 1].vid, 800).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        (root, s, all)
    }

    /// The whole valid-time extent of the fixture, so a test that is not about
    /// the window is not silently filtered by one.
    const ALL_VT: (i64, i64) = (i64::MIN / 4, i64::MAX / 4);

    fn diff(s: &NativeStore, t1: i64, t2: i64) -> BeliefDiffOut {
        s.diff_belief(t1, t2, ALL_VT.0, ALL_VT.1, usize::MAX).unwrap()
    }

    /// `(change, key, vid, tt)` for every row, in the operator's own order —
    /// the comparable shape for "identical answers" assertions.
    fn shape(out: &BeliefDiffOut) -> Vec<(&'static str, &str, &str, i64)> {
        out.rows
            .iter()
            .map(|r| (r.change.as_str(), r.key.as_str(), r.vid.as_str(), r.tt))
            .collect()
    }

    fn tagged(out: &BeliefDiffOut, c: BeliefChange) -> Vec<&str> {
        let mut v: Vec<&str> = out
            .rows
            .iter()
            .filter(|r| r.change == c)
            .map(|r| r.vid.as_str())
            .collect();
        v.sort_unstable();
        v
    }

    /// The vids believed at `as_of`, from the independent row-at-a-time path
    /// (`version_keys`) — the reference `diff_belief` is checked against
    /// inside the engine.
    fn believed_vids(s: &NativeStore, as_of: i64) -> Vec<String> {
        let f = VersionFilter {
            as_of,
            t_a: ALL_VT.0,
            t_b: ALL_VT.1,
            belief: Belief::Current,
            rel_types: None,
        };
        let mut v: Vec<String> = Vec::new();
        for kind in [RowKind::Node, RowKind::Edge] {
            for k in s.version_keys(kind, &f).unwrap() {
                v.push(
                    Id96 {
                        hi: k.vid_hi,
                        lo: k.vid_lo,
                    }
                    .to_hex(),
                );
            }
        }
        v.sort_unstable();
        v
    }

    // -- semantics -------------------------------------------------------- //

    #[test]
    fn the_added_and_retracted_sets_are_exact() {
        let (_root, s, all) = seeded("exact");
        // (400, 600] contains exactly the two corrections at 500 and 600 and
        // no ingest at all: the second and third corrected rows retract, and
        // nothing is added.
        let out = diff(&s, 400, 600);
        assert_eq!(out.added_total, 0, "no batch was written in (400, 600]");
        assert_eq!(
            tagged(&out, BeliefChange::Retracted),
            {
                let mut v = vec![all[5].vid.to_hex(), all[9].vid.to_hex()];
                v.sort();
                v
            },
            "exactly the rows corrected at 500 and 600"
        );
        assert_eq!(out.retracted_total, 2);
        assert!(!out.truncated);

        // (100, 200] contains exactly the second ingest batch: 4 edges + 2
        // node versions added, nothing retracted.
        let out = diff(&s, 100, 200);
        assert_eq!((out.added_total, out.retracted_total), (6, 0));
        assert!(out.rows.iter().all(|r| r.tt == 200));

        // and every reported tt really is the boundary it claims
        let out = diff(&s, 0, 1000);
        for r in &out.rows {
            assert!(r.tt > 0 && r.tt <= 1000, "tt {} escaped the window", r.tt);
        }
    }

    #[test]
    fn an_empty_belief_window_has_an_empty_answer() {
        let (_root, s, _) = seeded("empty");
        for t in [0i64, 100, 400, 700, 1000] {
            let out = diff(&s, t, t);
            assert_eq!(out, BeliefDiffOut::default(), "t1 == t2 == {t} must be empty");
        }
        // and t1 > t2 is a refused argument, not an empty answer
        assert!(s.diff_belief(600, 500, ALL_VT.0, ALL_VT.1, 10).is_err());
    }

    #[test]
    fn a_t1_before_any_belief_adds_everything_written_by_t2() {
        let (_root, s, _) = seeded("from-zero");
        // Nothing was written at or before tt = 0, so `added` at (0, T2] must
        // be every version written by T2 — which is the believed population at
        // T2 plus whatever was retracted on the way there.
        for t2 in [100i64, 250, 500, 800, 10_000] {
            let out = diff(&s, 0, t2);
            let mut added = tagged(&out, BeliefChange::Added);
            added.sort_unstable();
            let mut expect = believed_vids(&s, t2);
            for r in out.rows.iter().filter(|r| r.change == BeliefChange::Retracted) {
                expect.push(r.vid.clone());
            }
            expect.sort_unstable();
            expect.dedup();
            assert_eq!(
                added, expect,
                "at t2={t2}, added-from-zero must be believed(t2) + retracted-by-t2"
            );
        }
    }

    #[test]
    fn the_tags_refine_the_two_set_differences() {
        let (_root, s, all) = seeded("refine");
        // A window that straddles the ephemeral row's whole life: it was
        // written at 700 and closed at 800, so it is in neither believed(650)
        // nor believed(850) — a set difference of the two populations drops it
        // entirely, and both tags must still report it.
        let (t1, t2) = (650i64, 850i64);
        let out = diff(&s, t1, t2);
        let ephem = all[all.len() - 1].vid.to_hex();
        assert!(tagged(&out, BeliefChange::Added).contains(&ephem.as_str()));
        assert!(tagged(&out, BeliefChange::Retracted).contains(&ephem.as_str()));

        let b1 = believed_vids(&s, t1);
        let b2 = believed_vids(&s, t2);
        assert!(!b1.contains(&ephem) && !b2.contains(&ephem), "the fixture's ephemeral row must be invisible to both populations");

        // believed(T2) \ believed(T1) == { Added, still open at T2 }
        let only_2: Vec<&str> = b2.iter().filter(|v| !b1.contains(v)).map(|s| s.as_str()).collect();
        let mut added_open: Vec<&str> = out
            .rows
            .iter()
            .filter(|r| r.change == BeliefChange::Added && b2.contains(&r.vid))
            .map(|r| r.vid.as_str())
            .collect();
        added_open.sort_unstable();
        assert_eq!(added_open, only_2);

        // believed(T1) \ believed(T2) == { Retracted, written by T1 }
        let only_1: Vec<&str> = b1.iter().filter(|v| !b2.contains(v)).map(|s| s.as_str()).collect();
        let mut retracted_old: Vec<&str> = out
            .rows
            .iter()
            .filter(|r| r.change == BeliefChange::Retracted && b1.contains(&r.vid))
            .map(|r| r.vid.as_str())
            .collect();
        retracted_old.sort_unstable();
        assert_eq!(retracted_old, only_1);
    }

    #[test]
    fn the_valid_time_window_filters_and_the_limit_pages() {
        let (_root, s, _) = seeded("window");
        let wide = diff(&s, 0, 1000);
        // the fixture puts row i at vt_s = i, vt_e = i + 1, so [0, 4) holds
        // exactly the first ingest batch's four edges
        let narrow = s.diff_belief(0, 1000, 0, 4, usize::MAX).unwrap();
        assert!(narrow.rows.len() < wide.rows.len());
        assert!(narrow.rows.iter().all(|r| r.vt_s < 4 && r.vt_e > 0));
        // an empty window is an empty answer, never an error
        assert_eq!(s.diff_belief(0, 1000, 7, 7, 10).unwrap(), BeliefDiffOut::default());

        // a limit pages the ordered answer; the totals stay whole
        let paged = s.diff_belief(0, 1000, ALL_VT.0, ALL_VT.1, 3).unwrap();
        assert_eq!(paged.rows, wide.rows[..3].to_vec());
        assert!(paged.truncated);
        assert_eq!(
            (paged.added_total, paged.retracted_total),
            (wide.added_total, wide.retracted_total),
            "totals count the population, never the page"
        );
    }

    #[test]
    fn the_answer_is_sorted_by_kind_then_key_then_tt() {
        let (_root, s, _) = seeded("order");
        let out = diff(&s, 0, 1000);
        assert!(out.rows.len() > 4);
        let keys: Vec<_> = out.rows.iter().map(|r| r.order_key()).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
        // nodes before edges
        let first_edge = out.rows.iter().position(|r| r.kind == RowKind::Edge).unwrap();
        assert!(out.rows[..first_edge].iter().all(|r| r.kind == RowKind::Node));
    }

    // -- the range algebra ------------------------------------------------ //

    /// **The silent-wrong-answer guard for the dictionary shape.**
    ///
    /// `added_ranges` used to read `h.tt_s_runs` directly. The segment stores `tt_s`
    /// as a dictionary-coded column once run density crosses its threshold,
    /// and *drops* the run list when it does — so on such a segment the old
    /// code walked an empty list and reported that nothing had been added.
    /// Not an error: an empty answer, on exactly the segments where the new
    /// encoding fired. It also compiled cleanly after the merge, because it
    /// never called `believed_ranges` at all.
    ///
    /// The test therefore does three things, and the first is the one that
    /// keeps it honest: it **asserts the fixture actually is dictionary
    /// coded**, so the guard cannot quietly stop exercising the path it
    /// exists for. Then it checks the answer is non-empty, and that it equals
    /// an oracle derived independently of the range machinery.
    #[test]
    fn diff_belief_agrees_between_dictionary_and_run_list_segments() {
        // Each transaction writes one edge at its own `vt_s`, so rows sort
        // into one run per row: runs == rows crosses the `runs * 16 >= rows`
        // gate with room to spare, and 40 >= the 16-run floor.
        let dict = tmp_root("beliefdiff-dict");
        let mut sd = NativeStore::open(&dict).unwrap();
        // `set_layout`'s second argument is a byte target, not a row count:
        // the default 4096 here produced eight-row segments with eight runs,
        // which is above the density ratio but below the 16-run floor, so the
        // dictionary never fired and the guard caught its own fixture.
        let (p_d, _) = { let (p, t) = sd.layout(); (*p, t) };
        sd.set_layout(p_d, 1 << 20);
        let mut want_added: Vec<(String, i64)> = Vec::new();
        for k in 0..40u32 {
            let tt = 100 + k as i64 * 10;
            sd.begin(tt).unwrap();
            let a = sd.ensure_entity("na", "Node").unwrap();
            let b = sd.ensure_entity("nb", "Node").unwrap();
            let r = edge_row(a, b, "na", "nb", k, tt);
            want_added.push((r.vid.to_hex(), tt));
            sd.stage_edge(r).unwrap();
            sd.commit(EventLogRef::default()).unwrap();
        }
        sd.compact_incremental(None).ok();
        sd.settle(None).ok();

        // (1) the fixture must exercise the shape, or the guard is vacuous
        let mut dict_segs = 0usize;
        let mut shapes: Vec<String> = Vec::new();
        for (file, _) in sd.edge_files() {
            let seg = sd.open_segment(&file).unwrap();
            let v = seg.tt_s_view().unwrap();
            // carried into the failure message rather than printed: when this
            // guard goes vacuous it is the run count against the 16-run floor
            // that says why, and a bare "no dictionary segment" does not
            shapes.push(format!(
                "{file}: rows={} runs={} dict={}",
                seg.rows(),
                v.run_count(),
                v.is_dictionary()
            ));
            if v.is_dictionary() {
                dict_segs += 1;
            }
        }
        assert!(
            dict_segs > 0,
            "fixture no longer produces a dictionary-coded segment, so this \
             guard is not testing anything -- re-tune the run density. \
             Segments: {shapes:?}"
        );

        // (2) non-empty: the defect's signature is an empty Added set
        let got = diff(&sd, 0, 5000);
        let added = tagged(&got, BeliefChange::Added);
        assert!(
            !added.is_empty(),
            "dictionary-coded segments reported no additions at all -- this \
             is the empty-run-list defect"
        );

        // (3) equal to an oracle that never touches the range machinery: a
        //     row is added in (t1, t2] exactly when its own tt_s falls there,
        //     and the fixture takes no corrections, so nothing is retracted.
        let mut want: Vec<&str> = want_added
            .iter()
            .filter(|(_, tt)| *tt > 0 && *tt <= 5000)
            .map(|(vid, _)| vid.as_str())
            .collect();
        want.sort_unstable();
        assert_eq!(added, want, "dictionary path disagrees with the row oracle");
        assert!(tagged(&got, BeliefChange::Retracted).is_empty());

        // (4) the run-list control: same operator, same oracle, other shape.
        //     Twenty rows per transaction gives 2 runs over 40 rows, which is
        //     below the gate, so these segments keep their run list.
        let runs_root = tmp_root("beliefdiff-runs");
        let mut sr = NativeStore::open(&runs_root).unwrap();
        let (p_r, _) = { let (p, t) = sr.layout(); (*p, t) };
        sr.set_layout(p_r, 1 << 20);
        let mut want_r: Vec<(String, i64)> = Vec::new();
        for batch in 0..2u32 {
            let tt = 100 + batch as i64 * 10;
            sr.begin(tt).unwrap();
            let a = sr.ensure_entity("na", "Node").unwrap();
            let b = sr.ensure_entity("nb", "Node").unwrap();
            for j in 0..20u32 {
                let r = edge_row(a, b, "na", "nb", batch * 20 + j, tt);
                want_r.push((r.vid.to_hex(), tt));
                sr.stage_edge(r).unwrap();
            }
            sr.commit(EventLogRef::default()).unwrap();
        }
        sr.compact_incremental(None).ok();
        sr.settle(None).ok();
        for (file, _) in sr.edge_files() {
            let seg = sr.open_segment(&file).unwrap();
            assert!(
                !seg.tt_s_view().unwrap().is_dictionary(),
                "control segment took the dictionary; it is no longer a control"
            );
        }
        let got_r = diff(&sr, 0, 5000);
        let added_r = tagged(&got_r, BeliefChange::Added);
        let mut want_r2: Vec<&str> = want_r.iter().map(|(v, _)| v.as_str()).collect();
        want_r2.sort_unstable();
        assert_eq!(added_r, want_r2, "run-list path disagrees with the row oracle");
    }

    #[test]
    fn added_ranges_is_the_difference_of_the_two_verdict_range_lists() {
        let (_root, s, _) = seeded("ranges");
        // Over every segment of the fixture and a sequence of belief pairs that
        // brackets every transaction time in it, the one-pass form must equal
        // the two-pass difference the operator's contract is stated in.
        let sequence = [0i64, 99, 100, 150, 200, 300, 400, 500, 600, 700, 800, 5000];
        let mut compared = 0usize;
        for (file, _seg_id) in s.edge_files().into_iter().chain(s.node_files()) {
            let seg = s.open_segment(&file).unwrap();
            let h = seg.header();
            let tt = seg.tt_s_view().unwrap();
            let rows = seg.i64_column("vt_s").unwrap().len();
            let bounds = h.tt_s_bounds();
            for (i, &t1) in sequence.iter().enumerate() {
                for &t2 in &sequence[i..] {
                    let got = added_ranges(&tt, bounds, t1, t2, rows);
                    let want = range_subtract(
                        &believed_ranges(&tt, bounds, t2, rows),
                        &believed_ranges(&tt, bounds, t1, rows),
                    );
                    assert_eq!(got, want, "segment {file}, t1={t1} t2={t2}");
                    // the contract `subtract_closes` consumes: ascending,
                    // disjoint, non-empty, and never adjacent
                    for w in got.windows(2) {
                        assert!(w[0].1 < w[1].0, "ranges must be disjoint and coalesced");
                    }
                    assert!(got.iter().all(|(a, b)| a < b && *b <= rows));
                    compared += 1;
                }
            }
        }
        assert!(compared > 100, "the sweep must actually have run ({compared})");
    }

    /// Set difference of two ascending, disjoint range lists. Test-only: the
    /// operator computes the same set in one pass, and this exists to say so.


    // -- layout independence ---------------------------------------------- //

    #[test]
    fn settle_and_compaction_do_not_move_the_answer() {
        let pairs = [(0i64, 1000i64), (150, 650), (400, 600), (650, 850), (99, 101)];
        let (_root, mut s, _) = seeded("layout");
        let tiered: Vec<_> = pairs.iter().map(|&(a, b)| shape(&diff(&s, a, b))
            .into_iter()
            .map(|(c, k, v, t)| (c.to_string(), k.to_string(), v.to_string(), t))
            .collect::<Vec<_>>()).collect();
        assert!(tiered.iter().any(|v| !v.is_empty()), "the fixture must produce answers to compare");

        let before = s.manifest().edge_lanes.event.len()
            + s.manifest().edge_lanes.interval.len()
            + s.manifest().node_store.len();
        s.compact_incremental(None).unwrap();
        let after_compact: Vec<_> = pairs.iter().map(|&(a, b)| shape(&diff(&s, a, b))
            .into_iter()
            .map(|(c, k, v, t)| (c.to_string(), k.to_string(), v.to_string(), t))
            .collect::<Vec<_>>()).collect();
        assert_eq!(after_compact, tiered, "compact_incremental moved the answer");

        s.settle(None).unwrap();
        let after_settle: Vec<_> = pairs.iter().map(|&(a, b)| shape(&diff(&s, a, b))
            .into_iter()
            .map(|(c, k, v, t)| (c.to_string(), k.to_string(), v.to_string(), t))
            .collect::<Vec<_>>()).collect();
        assert_eq!(after_settle, tiered, "settle moved the answer");

        let now = s.manifest().edge_lanes.event.len()
            + s.manifest().edge_lanes.interval.len()
            + s.manifest().node_store.len();
        assert!(now < before, "the fixture must actually have been relaid out ({before} -> {now})");
    }

    #[test]
    fn the_answer_does_not_depend_on_the_thread_count() {
        let (_root, s, _) = seeded("threads");
        let want = shape(&diff(&s, 0, 1000))
            .into_iter()
            .map(|(c, k, v, t)| (c.to_string(), k.to_string(), v.to_string(), t))
            .collect::<Vec<_>>();
        // Eight readers against one store, and eight fresh handles against the
        // same root: neither the in-process sharing nor an independent open may
        // reorder or drop a row.
        std::thread::scope(|sc| {
            for _ in 0..8 {
                sc.spawn(|| {
                    let got = shape(&diff(&s, 0, 1000))
                        .into_iter()
                        .map(|(c, k, v, t)| (c.to_string(), k.to_string(), v.to_string(), t))
                        .collect::<Vec<_>>();
                    assert_eq!(got, want);
                });
            }
        });
        let root = s.root().to_path_buf();
        for _ in 0..4 {
            let s2 = NativeStore::open(&root).unwrap();
            let got = shape(&diff(&s2, 0, 1000))
                .into_iter()
                .map(|(c, k, v, t)| (c.to_string(), k.to_string(), v.to_string(), t))
                .collect::<Vec<_>>();
            assert_eq!(got, want, "a fresh handle must answer identically");
        }
    }

    /// A store whose edges are **long-lived**, so `vt_e` is not elided and
    /// [`window_rows`]'s lower end falls back to the per-row test.
    ///
    /// The elided case is the whole event lane and is what every other
    /// fixture here builds; without this one the `vt_e_elided == false` arm
    /// of the window cut would never run.
    fn seeded_intervals(name: &str) -> (PathBuf, NativeStore, Vec<EdgeRow>) {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        let (partitions, _) = {
            let (p, t) = s.layout();
            (*p, t)
        };
        s.set_layout(partitions, 4096);
        let mut all = Vec::new();
        for batch in 0..3u32 {
            let tt = 100 + batch as i64 * 100;
            s.begin(tt).unwrap();
            let a = s.ensure_entity(&format!("i{batch}a"), "Node").unwrap();
            let b = s.ensure_entity(&format!("i{batch}b"), "Node").unwrap();
            for i in 0..5u32 {
                let n = batch * 5 + i;
                let disc = format!("#{n}");
                let eid = edge_eid(&format!("i{batch}a"), &format!("i{batch}b"), "R", &disc);
                let vt_s = n as i64 * 3;
                // lifetimes that vary per row, so vt_e is neither elided nor
                // monotone in vt_s -- exactly the shape the lower-end cut
                // must not assume anything about
                let vt_e = vt_s + 1 + (n % 7) as i64 * 4;
                let r = EdgeRow {
                    vid: version_vid(&eid.to_hex(), tt, vt_s),
                    src_id: a,
                    dst_id: b,
                    rel_type: "R".into(),
                    disc,
                    vt_s,
                    vt_e,
                    tt_s: tt,
                    props: format!(r#"{{"i":{n}}}"#),
                    source: "ingest".into(),
                    provenance_ref: None,
                };
                all.push(r.clone());
                s.stage_edge(r).unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        for (tt, idx) in [(400i64, 2usize), (500, 8), (600, 11)] {
            s.begin(tt).unwrap();
            s.close_version(RowKind::Edge, all[idx].vid, tt).unwrap();
            s.commit(EventLogRef::default()).unwrap();
        }
        (root, s, all)
    }

    #[test]
    fn the_window_cut_never_loses_a_row() {
        // The window is pushed into the range domain -- a segment prune from
        // the header, and one or two binary searches over `vt_s`. That is a
        // cost change and must be exactly answer-preserving, so every window
        // is checked against a definition that does no cutting at all.
        for (name, seeded_fn) in [
            ("win-events", seeded as fn(&str) -> (PathBuf, NativeStore, Vec<EdgeRow>)),
            ("win-intervals", seeded_intervals),
        ] {
            let (_root, s, _) = seeded_fn(name);
            let mut nonempty = 0usize;
            for t_a in -2..40i64 {
                for len in [0i64, 1, 3, 9, 25, 1000] {
                    let t_b = t_a + len;
                    let got = s.diff_belief(0, 1000, t_a, t_b, usize::MAX).unwrap();
                    // the definition, with no range cut anywhere: every row of
                    // the unwindowed answer whose interval overlaps [t_a, t_b)
                    let all = s.diff_belief(0, 1000, ALL_VT.0, ALL_VT.1, usize::MAX).unwrap();
                    // `in_window` is the half-open overlap test, which is
                    // only meaningful for a non-empty window: at `t_a == t_b`
                    // it reports an overlap with an interval of zero width.
                    // The operator guards that case before ever reaching the
                    // test, and the reference has to guard it the same way.
                    let want: Vec<_> = if t_a < t_b {
                        all.rows
                            .iter()
                            .filter(|r| in_window(r.vt_s, r.vt_e, t_a, t_b))
                            .cloned()
                            .collect()
                    } else {
                        Vec::new()
                    };
                    assert_eq!(got.rows, want, "{name}: window [{t_a}, {t_b})");
                    assert_eq!(
                        got.added_total,
                        want.iter().filter(|r| r.change == BeliefChange::Added).count(),
                        "{name}: added_total, window [{t_a}, {t_b})"
                    );
                    assert_eq!(got.retracted_total, want.len() - got.added_total);
                    if !want.is_empty() {
                        nonempty += 1;
                    }
                }
            }
            assert!(nonempty > 20, "{name}: the sweep must hit real answers ({nonempty})");
        }
    }

    #[test]
    fn an_interval_segment_does_not_elide_vt_e() {
        // The guard on the test above: if the interval fixture's segments were
        // elided after all, that arm would be testing the event path twice.
        let (_root, s, _) = seeded_intervals("elide-check");
        let elided: Vec<bool> = s
            .edge_files()
            .into_iter()
            .map(|(f, _)| s.open_segment(&f).unwrap().header().vt_e_elided)
            .collect();
        assert!(!elided.is_empty() && elided.iter().any(|e| !e),
                "the interval fixture must produce at least one segment with an \
                 explicit vt_e, got {elided:?}");
    }

    #[test]
    fn a_batch_sees_its_own_writes_and_its_own_corrections() {
        let (_root, mut s, all) = seeded("in-batch");
        let committed = diff(&s, 800, 1000);
        assert_eq!(committed, BeliefDiffOut::default(), "nothing happened after 800");
        s.begin(900).unwrap();
        let a = s.ensure_entity("staged-a", "Node").unwrap();
        let r = edge_row(a, a, "staged-a", "staged-a", 200, 900);
        s.stage_edge(r.clone()).unwrap();
        // and a correction, in the same batch, against a committed row
        s.close_version(RowKind::Edge, all[2].vid, 900).unwrap();
        let out = diff(&s, 800, 1000);
        assert!(tagged(&out, BeliefChange::Added).contains(&r.vid.to_hex().as_str()),
                "a batch must see the row it just staged");
        assert!(tagged(&out, BeliefChange::Retracted).contains(&all[2].vid.to_hex().as_str()),
                "a batch must see the correction it just recorded");
        s.rollback().unwrap();
        assert_eq!(diff(&s, 800, 1000), committed, "rollback must undo both");
    }
}
