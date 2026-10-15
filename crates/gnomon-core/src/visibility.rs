//! Belief visibility: closing `tt_e` is recorded as data, never as a mutation
//!
//! Sealed segments are immutable, so "this version stopped being believed at
//! `tt_e`" cannot be written into the row. Instead each commit appends an
//! immutable **close run** that the manifest lists. Because a reader pins a
//! manifest generation, it sees exactly the closes that existed then — which
//! is what makes the snapshot coherent. (A store-wide mutable close set, as
//! the first blueprint had, would let a generation-N reader observe
//! generation-N+1 visibility.)
//!
//! Compaction later folds runs into a per-segment **sidecar** carried in the
//! segment header. Two representations are implemented:
//!
//! | closure density | representation | cost on a current-belief scan |
//! |---|---|---|
//! | zero | `all_current` header flag | nothing at all |
//! | sparse | sorted `(row, tt_e)` pairs | one binary search per closed row |
//!
//! The dense `tt_e`-per-row variant is deliberately *not* built: the format
//! reserves the shape, and no measured workload has needed it.
//!
//! Compaction folds closes; it never drops the rows they refer to. Historical
//! belief queries are the entire point of the system, and a closed version is
//! still the answer to "what did we believe before the correction?".

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::error::{EngineError, Result};
use crate::row::RowKind;
use crate::OPEN_END;

const MAGIC: &[u8; 4] = b"TGCR";
const FOOTER_MAGIC: &[u8; 4] = b"TGCE";
const RECORD_BYTES: usize = 1 + 8 + 4 + 8;

/// One version stopped being believed. Rows are addressed physically, by the
/// segment and row they live at — compaction that moves a row rewrites the
/// close into the new segment's sidecar rather than leaving a dangling id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloseRecord {
    pub kind: RowKind,
    pub segment: u64,
    pub row: u32,
    pub tt_e: i64,
}

/// Write one commit's closes. Called before the manifest that references it,
/// so a crash here leaves an unreferenced file rather than a dangling pointer.
pub fn write_close_run(path: &Path, records: &[CloseRecord]) -> Result<u32> {
    if records.is_empty() {
        return Err(EngineError::invariant("refusing to write an empty close run"));
    }
    let mut buf = Vec::with_capacity(12 + records.len() * RECORD_BYTES + 8);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&crate::FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for r in records {
        buf.push(match r.kind {
            RowKind::Node => 0,
            RowKind::Edge => 1,
        });
        buf.extend_from_slice(&r.segment.to_le_bytes());
        buf.extend_from_slice(&r.row.to_le_bytes());
        buf.extend_from_slice(&r.tt_e.to_le_bytes());
    }
    buf.extend_from_slice(&crc32c::crc32c(&buf[12..]).to_le_bytes());
    buf.extend_from_slice(FOOTER_MAGIC);

    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|e| EngineError::from(e).at_file(dir))?;
    }
    let tmp = path.with_extension("tgc.tmp");
    {
        let mut f = File::create(&tmp).map_err(|e| EngineError::from(e).at_file(&tmp))?;
        f.write_all(&buf)
            .map_err(|e| EngineError::from(e).at_file(&tmp))?;
        f.sync_all()
            .map_err(|e| EngineError::from(e).at_file(&tmp))?;
    }
    fs::rename(&tmp, path).map_err(|e| EngineError::from(e).at_file(path))?;
    Ok(records.len() as u32)
}

pub fn read_close_run(path: &Path) -> Result<Vec<CloseRecord>> {
    let bytes = fs::read(path).map_err(|e| EngineError::from(e).at_file(path))?;
    let at = |e: EngineError| e.at_file(path);
    if bytes.len() < 20 || &bytes[..4] != MAGIC {
        return Err(at(EngineError::corrupt("not a Gnomon close run (bad magic)")));
    }
    if &bytes[bytes.len() - 4..] != FOOTER_MAGIC {
        return Err(at(EngineError::corrupt(
            "close run has no completion marker — it was never finished being written",
        )));
    }
    let n = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes")) as usize;
    let end = 12 + n * RECORD_BYTES;
    if bytes.len() < end + 8 {
        return Err(at(EngineError::corrupt("close run is truncated")));
    }
    let expect = u32::from_le_bytes(bytes[end..end + 4].try_into().expect("4 bytes"));
    let got = crc32c::crc32c(&bytes[12..end]);
    if got != expect {
        return Err(at(EngineError::corrupt(format!(
            "close run failed its checksum (computed {got:#x}, recorded {expect:#x})"
        ))));
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let o = 12 + i * RECORD_BYTES;
        out.push(CloseRecord {
            kind: match bytes[o] {
                0 => RowKind::Node,
                1 => RowKind::Edge,
                other => {
                    return Err(at(
                        EngineError::corrupt(format!("close run has unknown row kind {other}"))
                            .at_offset(o as u64),
                    ))
                }
            },
            segment: u64::from_le_bytes(bytes[o + 1..o + 9].try_into().expect("8 bytes")),
            row: u32::from_le_bytes(bytes[o + 9..o + 13].try_into().expect("4 bytes")),
            tt_e: i64::from_le_bytes(bytes[o + 13..o + 21].try_into().expect("8 bytes")),
        });
    }
    Ok(out)
}

/// All close runs a generation makes visible, resolved for lookup.
///
/// A later close of the same row wins: corrections are applied in transaction
/// order, and replay must reach the same state as the live path.
///
/// An index may also **layer** over another one. The open batch's closes are
/// not in any run file yet, but a read inside that batch must see them — the
/// bi-temporal ops read belief to decide what to carve, so a batch that
/// cannot see its own corrections carves against a version it has already
/// stopped believing. Layering rather than merging keeps the
/// committed index shared and untouched: the overlay is the size of the open
/// batch, not of the store's correction history.
#[derive(Default, Debug, Clone)]
pub struct CloseIndex {
    by_row: HashMap<(u64, u32), i64>,
    /// The same closes, projected per segment and sorted by row — **this
    /// layer only**, exactly like `by_row`, so `segment_closes` resolves
    /// layering the same way `tt_e` does.
    ///
    /// `by_row` answers a point lookup in O(1) and is what the row-at-a-time
    /// materialization paths want. It cannot answer "the closes of segment
    /// S, in row order", which is what a *range*-domain belief test needs,
    /// and the only accessor that could — `records_for` — walks every close
    /// in the generation. Calling that per segment per scan would be
    /// quadratic in correction volume, a shape that has been removed once
    /// already.
    ///
    /// Held alongside `by_row` rather than replacing it: +16 B per
    /// close, about +3.4 MB at the measured 20 %-density 1M point against a
    /// 571 MB VmRSS, and no new cost on `tt_e`, which the current-belief
    /// point read calls per row.
    by_segment: HashMap<u64, Vec<(u32, i64)>>,
    segments: std::collections::HashSet<u64>,
    /// Committed closes this index sits on top of. `None` for a committed
    /// index, which is every index outside an open batch.
    base: Option<std::sync::Arc<CloseIndex>>,
}

impl CloseIndex {
    /// Heap this index and everything it layers over own, in bytes (for
    /// memory accounting).
    ///
    /// Charged per *close record*, twice over by design: `by_row` answers a
    /// point lookup and `by_segment` answers a range-domain one, and the
    /// doc on `by_segment` prices holding both at +16 B per close. Follows
    /// `base` so an open batch's overlay reports the whole layered chain
    /// rather than just its own layer — the chain is what is resident.
    pub fn heap_bytes(&self) -> u64 {
        let by_row = self.by_row.capacity()
            * (std::mem::size_of::<(u64, u32)>() + std::mem::size_of::<i64>());
        let by_seg_buckets = self.by_segment.capacity()
            * (std::mem::size_of::<u64>() + std::mem::size_of::<Vec<(u32, i64)>>());
        let by_seg_rows: usize = self
            .by_segment
            .values()
            .map(|v| v.capacity() * std::mem::size_of::<(u32, i64)>())
            .sum();
        let segs = self.segments.capacity() * std::mem::size_of::<u64>();
        let base = self.base.as_ref().map_or(0, |b| b.heap_bytes());
        (by_row + by_seg_buckets + by_seg_rows + segs) as u64 + base
    }

    pub fn from_records(records: impl IntoIterator<Item = CloseRecord>) -> Self {
        let mut idx = Self::default();
        for r in records {
            idx.by_row.insert((r.segment, r.row), r.tt_e);
            idx.segments.insert(r.segment);
        }
        // Projected from `by_row` rather than from `records`, so "a later
        // close of the same row wins" is decided in exactly one place and
        // the two representations cannot disagree. One sort per segment at
        // the end, not a sorted insert per record: `from_records` sees the
        // generation's whole correction history at once, and this build is
        // what the per-generation index cache amortizes.
        for (&(seg, row), &tt_e) in &idx.by_row {
            idx.by_segment.entry(seg).or_default().push((row, tt_e));
        }
        for v in idx.by_segment.values_mut() {
            v.sort_unstable_by_key(|(row, _)| *row);
        }
        idx
    }

    /// An empty index layered over `base`, to be filled by `close_row` as
    /// the batch records closes. Anything added wins over `base`: it is the
    /// later close, applied in transaction order like any other.
    pub fn layered_over(base: std::sync::Arc<CloseIndex>) -> Self {
        Self {
            base: Some(base),
            ..Self::default()
        }
    }

    /// Record one close in this layer.
    pub fn close_row(&mut self, segment: u64, row: u32, tt_e: i64) {
        self.by_row.insert((segment, row), tt_e);
        self.segments.insert(segment);
        // Inserted in place rather than appended-and-sorted-on-first-read:
        // an open batch's overlay is small by construction — that is the
        // same argument the doc comment above makes for layering rather than
        // merging — so the memmove is over a handful of entries, and paying
        // it here keeps the index free of interior mutability. A lazily
        // sorted vector would need a lock, because a committed `CloseIndex`
        // is shared across the scan's threads behind an `Arc`.
        let v = self.by_segment.entry(segment).or_default();
        match v.binary_search_by_key(&row, |(r, _)| *r) {
            Ok(i) => v[i].1 = tt_e,
            Err(i) => v.insert(i, (row, tt_e)),
        }
    }

    /// The closes this index makes visible for one segment, sorted by row,
    /// with the overlay resolved over the base **by key presence** — the
    /// rule [`CloseIndex::tt_e`] applies, so an overlay entry wins even when
    /// the base's `tt_e` is later.
    ///
    /// Borrowed whenever only one layer has anything to say about the
    /// segment, which is every committed index and every open batch that has
    /// not corrected this segment.
    pub fn segment_closes(&self, segment: u64) -> std::borrow::Cow<'_, [(u32, i64)]> {
        use std::borrow::Cow;
        let own: &[(u32, i64)] = self.by_segment.get(&segment).map_or(&[][..], |v| v.as_slice());
        let Some(base) = &self.base else {
            return Cow::Borrowed(own);
        };
        let under = base.segment_closes(segment);
        if own.is_empty() {
            return under;
        }
        if under.is_empty() {
            return Cow::Borrowed(own);
        }
        Cow::Owned(merge_layers(own, &under))
    }

    /// [`CloseIndex::segment_closes`] restricted to the rows `[lo, hi)`.
    ///
    /// Each layer's vector is sorted by row, so the restriction is two
    /// binary searches per layer, and the key-presence merge then runs over
    /// the two sub-slices only. Layering is decided per row, so restricting
    /// first and restricting afterwards give the same entries:
    /// `segment_closes_in(s, lo, hi) == segment_closes(s)` filtered to
    /// `lo <= row < hi`. Borrowed whenever one layer has nothing in the
    /// range, which is every committed index.
    pub fn segment_closes_in(
        &self,
        segment: u64,
        lo: usize,
        hi: usize,
    ) -> std::borrow::Cow<'_, [(u32, i64)]> {
        use std::borrow::Cow;
        let own_all: &[(u32, i64)] =
            self.by_segment.get(&segment).map_or(&[][..], |v| v.as_slice());
        let own = rows_in(own_all, lo, hi);
        let Some(base) = &self.base else {
            return Cow::Borrowed(own);
        };
        let under = base.segment_closes_in(segment, lo, hi);
        if own.is_empty() {
            return under;
        }
        if under.is_empty() {
            return Cow::Borrowed(own);
        }
        Cow::Owned(merge_layers(own, &under))
    }

    pub fn is_empty(&self) -> bool {
        self.by_row.is_empty() && self.base.as_ref().is_none_or(|b| b.is_empty())
    }

    /// Close entries this index and its layers hold for one segment,
    /// counting a row closed in two layers twice: an upper bound on what
    /// [`CloseIndex::segment_closes`] returns, without merging anything
    /// (the traversal's cost estimate).
    pub fn segment_close_count(&self, segment: u64) -> usize {
        self.by_segment.get(&segment).map_or(0, |v| v.len())
            + self.base.as_ref().map_or(0, |b| b.segment_close_count(segment))
    }

    /// Rows closed by this index, counting a row layered over its base once.
    pub fn len(&self) -> usize {
        match &self.base {
            None => self.by_row.len(),
            Some(b) => {
                self.by_row.len()
                    + b.rows()
                        .filter(|k| !self.by_row.contains_key(k))
                        .count()
            }
        }
    }

    fn rows(&self) -> Box<dyn Iterator<Item = (u64, u32)> + '_> {
        match &self.base {
            None => Box::new(self.by_row.keys().copied()),
            Some(b) => Box::new(
                self.by_row
                    .keys()
                    .copied()
                    .chain(b.rows().filter(|k| !self.by_row.contains_key(k))),
            ),
        }
    }

    /// Does any close in this generation touch the given segment? Lets a scan
    /// skip visibility work entirely for the segments nothing has corrected —
    /// which, for event-stream data, is nearly all of them.
    pub fn touches(&self, segment: u64) -> bool {
        self.segments.contains(&segment)
            || self.base.as_ref().is_some_and(|b| b.touches(segment))
    }

    /// Transaction-time end of one row: the close if there is one, else open.
    #[inline]
    pub fn tt_e(&self, segment: u64, row: u32) -> i64 {
        match self.by_row.get(&(segment, row)) {
            Some(tt_e) => *tt_e,
            None => match &self.base {
                Some(b) => b.tt_e(segment, row),
                None => OPEN_END,
            },
        }
    }

    /// Every `(segment, row)` this index closes, layers resolved.
    ///
    /// Incremental compaction's close-run retirement needs to know what
    /// fraction of the generation's close records name segments a rewrite
    /// has superseded. Answering that from the in-memory index costs no
    /// I/O — re-reading the run files to count would be O(all closes) of
    /// disk per compaction, to decide whether to do any work at all.
    pub fn closed_addresses(&self) -> Box<dyn Iterator<Item = (u64, u32)> + '_> {
        self.rows()
    }

    pub fn records_for(&self, segment: u64) -> Vec<(u32, i64)> {
        let mut v: Vec<(u32, i64)> = self
            .rows()
            .filter(|(s, _)| *s == segment)
            .map(|(_, r)| (r, self.tt_e(segment, r)))
            .collect();
        v.sort_unstable();
        v
    }
}

/// The entries of a row-sorted close list whose row is in `[lo, hi)`: two
/// binary searches, no copy.
#[inline]
pub(crate) fn rows_in(closes: &[(u32, i64)], lo: usize, hi: usize) -> &[(u32, i64)] {
    let a = closes.partition_point(|(r, _)| (*r as usize) < lo);
    let b = a + closes[a..].partition_point(|(r, _)| (*r as usize) < hi);
    &closes[a..b]
}

/// An overlay layer over its base, both sorted by row, resolved by key
/// presence: on a tie the overlay's entry is kept and the base's dropped.
fn merge_layers(own: &[(u32, i64)], under: &[(u32, i64)]) -> Vec<(u32, i64)> {
    crate::vistouch::closes(own.len() + under.len());
    let mut out: Vec<(u32, i64)> = Vec::with_capacity(own.len() + under.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < own.len() || j < under.len() {
        if j == under.len() || (i < own.len() && own[i].0 <= under[j].0) {
            // `<=` is where "by key presence" lives: on a tie the
            // overlay's entry is emitted and the base's is stepped over.
            if j < under.len() && under[j].0 == own[i].0 {
                j += 1;
            }
            out.push(own[i]);
            i += 1;
        } else {
            out.push(under[j]);
            j += 1;
        }
    }
    out
}

/// Which half or halves of the belief predicate a per-row test applies:
/// the per-row spelling of `believed_ranges_closed`,
/// `believed_ranges_any` and `believed_ranges_superseded`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RowBelief {
    /// `tt_s <= as_of < tt_e`
    Current,
    /// `tt_s <= as_of`
    Written,
    /// `tt_s <= as_of` and `tt_e <= as_of`
    Superseded,
}

/// The rows of `domain` that pass `mode` at `as_of`, each tested on its
/// own, as maximal ascending ranges (`GNOMON_RANGE_VISIBILITY=0`).
///
/// The ablation of range-domain visibility: every row of the domain has its
/// `tt_s` read through the view and its effective `tt_e` resolved by
/// [`crate::read::effective_tt_e`] (a close-run probe, then the sidecar), with
/// no bounds short-cut, no untouched-segment short-cut and no close view.
/// The verdicts are coalesced by the rule every range primitive uses, so
/// the output is the range primitives' answer clipped to `domain`, byte for
/// byte (`ablation_tests::the_per_row_belief_test_selects_exactly_the_range_domain_rows`).
///
/// `domain` is ascending and disjoint, and `as_of` is below the unclamped
/// `OPEN_END`, which `version_keys` answers without a walk. A row the view
/// cannot place (a code column shorter than the segment, a run list that
/// starts after row 0) is an error here where the range primitives skip it;
/// no writer produces either.
#[allow(clippy::too_many_arguments)]
pub(crate) fn believed_ranges_per_row(
    tt: &crate::ttime::TtSView<'_>,
    sidecar: Sidecar<'_>,
    closes: &CloseIndex,
    segment: u64,
    as_of: i64,
    domain: &[(usize, usize)],
    rows: usize,
    mode: RowBelief,
) -> Result<Vec<(usize, usize)>> {
    let mut out: Vec<(usize, usize)> = Vec::new();
    let mut tested = 0usize;
    for &(lo, hi) in domain {
        let hi = hi.min(rows);
        for r in lo..hi {
            tested += 1;
            let row = r as u32;
            let keep = tt.at(row)? <= as_of
                && match mode {
                    RowBelief::Written => true,
                    RowBelief::Current => {
                        as_of < crate::read::effective_tt_e(closes, &sidecar, segment, row)
                    }
                    RowBelief::Superseded => {
                        crate::read::effective_tt_e(closes, &sidecar, segment, row) <= as_of
                    }
                };
            if keep {
                match out.last_mut() {
                    Some(last) if last.1 == r => last.1 = r + 1,
                    _ => out.push((r, r + 1)),
                }
            }
        }
    }
    crate::vistouch::rows(tested);
    Ok(out)
}

/// The folded, per-segment form: sorted `(row, tt_e)` pairs living in the
/// segment header. Empty means `all_current` — the fast path.
#[derive(Clone, Copy, Debug, Default)]
pub struct Sidecar<'a> {
    closed: &'a [(u32, i64)],
}

impl<'a> Sidecar<'a> {
    pub fn new(closed: &'a [(u32, i64)]) -> Self {
        Self { closed }
    }

    pub fn all_current(&self) -> bool {
        self.closed.is_empty()
    }

    /// The folded pairs themselves, already sorted by row. Lets a range-domain
    /// reader walk them in step with a close-run list instead of binary
    /// searching once per row.
    #[inline]
    pub fn slice(&self) -> &'a [(u32, i64)] {
        self.closed
    }

    #[inline]
    pub fn tt_e(&self, row: u32) -> i64 {
        if self.closed.is_empty() {
            return OPEN_END;
        }
        match self.closed.binary_search_by_key(&row, |(r, _)| *r) {
            Ok(i) => self.closed[i].1,
            Err(_) => OPEN_END,
        }
    }
}

/// One segment's effective closes, sorted by row — the two layers the belief
/// predicate's `as_of < tt_e` half has to consult, resolved once per segment
/// per scan instead of once per row.
///
/// The three variants exist so the overwhelmingly common case allocates
/// nothing at all: a store nobody has corrected produces [`CloseView::None`]
/// for every segment, and a compacted segment whose closes are all folded
/// borrows the header's own vector.
#[derive(Clone, Debug)]
pub enum CloseView<'a> {
    /// Nothing closes any row of this segment at any `as_of`.
    None,
    /// Folded closes only — borrowed straight from the segment header.
    Sidecar(&'a [(u32, i64)]),
    /// Close runs layered over the sidecar; the merge is materialized.
    Merged(Vec<(u32, i64)>),
}

impl CloseView<'_> {
    /// The effective closes as one sorted slice; empty for [`CloseView::None`].
    #[inline]
    pub fn as_slice(&self) -> &[(u32, i64)] {
        match self {
            CloseView::None => &[],
            CloseView::Sidecar(s) => s,
            CloseView::Merged(v) => v,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }
}

/// Effective closes of one segment, sorted by row, with **exactly** the
/// precedence the per-row belief test applies.
///
/// Three rules, all of which have to survive byte-for-byte, because the whole
/// claim of the range-domain rewrite this feeds is "unchanged answers":
///
/// - a run entry outranks a sidecar entry for the same row whether its `tt_e`
///   is *earlier or later* — it is the more recent transaction;
/// - layering is resolved by key presence before the `!= OPEN_END` test, the
///   rule [`CloseIndex::tt_e`] applies;
/// - a run entry whose `tt_e` **is** `OPEN_END` means "the run has nothing to
///   say", so the sidecar answers — and where the sidecar has no entry for
///   that row, such a run entry is dropped entirely. That is a latent oddity
///   rather than a designed rule (a close run is never *written* with
///   `tt_e == OPEN_END`; `write_close_run` refuses only empty runs) and it is
///   deliberately preserved here, not fixed.
pub fn close_view<'a>(
    sidecar: Sidecar<'a>,
    closes: &'a CloseIndex,
    segment: u64,
) -> CloseView<'a> {
    view_of(sidecar.slice(), closes.segment_closes(segment))
}

/// [`close_view`] restricted to the rows `[lo, hi)`.
///
/// The three precedence rules are all decided **per row** (an entry's
/// fate depends only on the run entry and the sidecar entry for the same
/// row), so restricting both inputs first gives exactly the entries of the
/// whole-segment view whose row is in `[lo, hi)`, in the same order. The
/// merge is the same code over the two sub-slices: `O(log K)` to find them
/// plus the closes inside the range, instead of every close the segment
/// carries. A folded-only segment still borrows (a sub-slice of the
/// header's vector).
pub fn close_view_in<'a>(
    sidecar: Sidecar<'a>,
    closes: &'a CloseIndex,
    segment: u64,
    lo: usize,
    hi: usize,
) -> CloseView<'a> {
    view_of(
        rows_in(sidecar.slice(), lo, hi),
        closes.segment_closes_in(segment, lo, hi),
    )
}

/// [`close_view`] for a whole-segment request, [`close_view_in`] otherwise:
/// the dispatch every range-aware call site uses, so a query that needs
/// every row of a segment runs exactly the whole-segment code.
pub fn close_view_range<'a>(
    sidecar: Sidecar<'a>,
    closes: &'a CloseIndex,
    segment: u64,
    lo: usize,
    hi: usize,
    rows: usize,
) -> CloseView<'a> {
    if crate::scan::is_whole(lo, hi, rows) {
        close_view(sidecar, closes, segment)
    } else {
        close_view_in(sidecar, closes, segment, lo, hi)
    }
}

/// The effective-close merge over one folded slice and one close-run slice,
/// both sorted by row. Shared by [`close_view`] and [`close_view_in`] so the
/// precedence rules are written once.
fn view_of<'a>(
    folded: &'a [(u32, i64)],
    runs: std::borrow::Cow<'a, [(u32, i64)]>,
) -> CloseView<'a> {
    if runs.is_empty() {
        return if folded.is_empty() {
            CloseView::None
        } else {
            CloseView::Sidecar(folded)
        };
    }
    if folded.is_empty() {
        crate::vistouch::closes(runs.len());
        let keep: Vec<(u32, i64)> = runs
            .iter()
            .copied()
            .filter(|(_, tt_e)| *tt_e != OPEN_END)
            .collect();
        return if keep.is_empty() {
            CloseView::None
        } else {
            CloseView::Merged(keep)
        };
    }
    crate::vistouch::closes(folded.len() + runs.len());
    let mut out: Vec<(u32, i64)> = Vec::with_capacity(folded.len() + runs.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < folded.len() || j < runs.len() {
        if j == runs.len() || (i < folded.len() && folded[i].0 < runs[j].0) {
            out.push(folded[i]);
            i += 1;
        } else if i == folded.len() || runs[j].0 < folded[i].0 {
            if runs[j].1 != OPEN_END {
                out.push(runs[j]);
            }
            j += 1;
        } else {
            // same row: the run outranks the sidecar unless it is OPEN_END
            out.push((
                folded[i].0,
                if runs[j].1 != OPEN_END {
                    runs[j].1
                } else {
                    folded[i].1
                },
            ));
            i += 1;
            j += 1;
        }
    }
    CloseView::Merged(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-vis-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p.join("000001.tgc")
    }

    fn rec(seg: u64, row: u32, tt_e: i64) -> CloseRecord {
        CloseRecord {
            kind: RowKind::Edge,
            segment: seg,
            row,
            tt_e,
        }
    }

    #[test]
    fn close_runs_round_trip() {
        let path = tmp("roundtrip");
        let records = vec![
            rec(0, 0, 100),
            rec(0, 17, 200),
            CloseRecord {
                kind: RowKind::Node,
                segment: 3,
                row: 9,
                tt_e: OPEN_END - 1,
            },
        ];
        assert_eq!(write_close_run(&path, &records).unwrap(), 3);
        assert_eq!(read_close_run(&path).unwrap(), records);
    }

    #[test]
    fn truncated_close_run_is_detected() {
        let path = tmp("truncated");
        write_close_run(&path, &[rec(0, 0, 100)]).unwrap();
        let mut b = fs::read(&path).unwrap();
        b.truncate(b.len() - 6);
        fs::write(&path, b).unwrap();
        let err = match read_close_run(&path) {
            Ok(_) => panic!("a truncated close run must not load"),
            Err(e) => e,
        };
        assert_eq!(err.category, crate::error::Category::Corrupt);
    }

    #[test]
    fn flipped_byte_fails_the_checksum() {
        let path = tmp("bitflip");
        write_close_run(&path, &[rec(0, 5, 100), rec(0, 6, 101)]).unwrap();
        let mut b = fs::read(&path).unwrap();
        b[14] ^= 0xff;
        fs::write(&path, b).unwrap();
        assert!(read_close_run(&path).is_err());
    }

    #[test]
    fn empty_close_runs_are_never_written() {
        assert!(write_close_run(&tmp("empty"), &[]).is_err());
    }

    #[test]
    fn later_closes_win() {
        // the same row corrected twice: transaction order decides
        let idx = CloseIndex::from_records([rec(1, 4, 100), rec(1, 4, 250)]);
        assert_eq!(idx.tt_e(1, 4), 250);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn unclosed_rows_read_as_open() {
        let idx = CloseIndex::from_records([rec(1, 4, 100)]);
        assert_eq!(idx.tt_e(1, 5), OPEN_END);
        assert_eq!(idx.tt_e(2, 4), OPEN_END);
        assert!(idx.touches(1));
        assert!(!idx.touches(2), "untouched segments skip visibility work");
    }

    #[test]
    fn sidecar_answers_by_binary_search() {
        let closed = [(3u32, 100i64), (9, 200), (40, 300)];
        let s = Sidecar::new(&closed);
        assert!(!s.all_current());
        assert_eq!(s.tt_e(3), 100);
        assert_eq!(s.tt_e(9), 200);
        assert_eq!(s.tt_e(40), 300);
        assert_eq!(s.tt_e(0), OPEN_END);
        assert_eq!(s.tt_e(41), OPEN_END);
    }

    #[test]
    fn empty_sidecar_is_the_all_current_fast_path() {
        let s = Sidecar::default();
        assert!(s.all_current());
        assert_eq!(s.tt_e(12345), OPEN_END);
    }

    #[test]
    fn records_for_segment_come_back_sorted_for_folding() {
        let idx = CloseIndex::from_records([rec(1, 40, 3), rec(1, 4, 1), rec(2, 7, 2)]);
        assert_eq!(idx.records_for(1), vec![(4, 1), (40, 3)]);
        assert_eq!(idx.records_for(2), vec![(7, 2)]);
        assert!(idx.records_for(99).is_empty());
    }

    #[test]
    fn a_layer_answers_for_its_base_and_wins_where_they_overlap() {
        let base = std::sync::Arc::new(CloseIndex::from_records([rec(1, 4, 1), rec(2, 7, 2)]));
        let mut top = CloseIndex::layered_over(base.clone());
        top.close_row(1, 4, 9); // re-close: the open batch's tt_e is the later one
        top.close_row(3, 5, 8); // and a segment the base never touched

        assert_eq!(top.tt_e(1, 4), 9, "the layer wins");
        assert_eq!(top.tt_e(2, 7), 2, "the base still answers");
        assert_eq!(top.tt_e(3, 5), 8);
        assert_eq!(top.tt_e(9, 9), OPEN_END);

        // a scan skips visibility work per segment, so `touches` must see
        // through the layer in both directions
        for seg in [1, 2, 3] {
            assert!(top.touches(seg), "segment {seg}");
        }
        assert!(!top.touches(4));

        // three distinct rows, not four: (1, 4) is closed twice, by both
        assert_eq!(top.len(), 3);
        assert!(!top.is_empty());
        assert!(CloseIndex::layered_over(std::sync::Arc::new(CloseIndex::default())).is_empty());
        assert_eq!(top.records_for(1), vec![(4, 9)]);
    }

    // --- the per-segment sorted projection and the close view ------------ #

    #[test]
    fn segment_closes_are_sorted_and_layer_by_key_presence() {
        // deliberately out of row order, so a projection that forgot to sort
        // would be visible rather than lucky
        let base = std::sync::Arc::new(CloseIndex::from_records([
            rec(1, 9, 90),
            rec(1, 2, 20),
            rec(1, 5, 50),
            rec(2, 1, 10),
        ]));
        assert_eq!(
            base.segment_closes(1).as_ref(),
            &[(2, 20), (5, 50), (9, 90)],
            "the per-segment projection must come back in row order"
        );
        assert!(base.segment_closes(7).is_empty());
        // and it must agree with the O(all closes) accessor it exists to
        // replace, or the range path and the row path could diverge
        for seg in [1, 2, 7] {
            assert_eq!(base.segment_closes(seg).as_ref(), base.records_for(seg));
        }

        let mut top = CloseIndex::layered_over(base.clone());
        top.close_row(1, 5, 11); // overlay wins with an *earlier* tt_e
        top.close_row(1, 0, 5); // sorts before everything the base has
        top.close_row(1, 12, 7); // and after
        assert_eq!(
            top.segment_closes(1).as_ref(),
            &[(0, 5), (2, 20), (5, 11), (9, 90), (12, 7)],
            "layering resolves by key presence, in row order"
        );
        for seg in [1, 2, 7] {
            assert_eq!(top.segment_closes(seg).as_ref(), top.records_for(seg));
        }
        // re-closing a row already in this layer replaces it in place
        top.close_row(1, 5, 13);
        assert_eq!(top.segment_closes(1).as_ref()[2], (5, 13));
    }

    #[test]
    fn close_view_of_an_all_current_segment_is_none() {
        // E10: the case that must allocate nothing, asserted on the variant
        // rather than on the slice, because "returns an empty slice" is also
        // true of a Merged(vec![]) that has already paid for the merge.
        let empty = CloseIndex::default();
        let all_current = Sidecar::new(&[]);
        assert!(matches!(
            close_view(all_current, &empty, 1),
            CloseView::None
        ));
        // a close elsewhere in the generation does not drag this segment off
        // the fast path
        let other = CloseIndex::from_records([rec(2, 3, 30)]);
        assert!(matches!(close_view(all_current, &other, 1), CloseView::None));
        // folded-only borrows the header's own vector rather than copying it
        let folded = [(1u32, 10i64), (4, 40)];
        match close_view(Sidecar::new(&folded), &empty, 1) {
            CloseView::Sidecar(s) => assert!(std::ptr::eq(s, &folded[..])),
            other => panic!("expected a borrowed sidecar, got {other:?}"),
        }
    }

    #[test]
    fn close_view_merges_runs_over_the_sidecar() {
        let folded = [(1u32, 100i64), (3, 300), (5, 500), (7, 700)];
        let runs = CloseIndex::from_records([
            rec(1, 1, 50),         // E1: run overrides to an *earlier* tt_e
            rec(1, 3, 900),        // E2: run overrides to a *later* tt_e
            rec(1, 5, OPEN_END),   // E3: run says nothing; the sidecar answers
            rec(1, 6, 600),        // run-only row, interleaved between folded ones
            rec(1, 9, OPEN_END),   // E3 with no sidecar entry: dropped entirely
            rec(1, 11, 1100),      // run-only row after everything folded
            rec(2, 1, 1),          // another segment: must not leak in
        ]);
        let got = close_view(Sidecar::new(&folded), &runs, 1);
        assert_eq!(
            got.as_slice(),
            &[(1, 50), (3, 900), (5, 500), (6, 600), (7, 700), (11, 1100)],
        );
        assert!(matches!(got, CloseView::Merged(_)));

        // and the same merge over an all_current segment keeps only the runs
        // that have something to say
        let got = close_view(Sidecar::new(&[]), &runs, 1);
        assert_eq!(got.as_slice(), &[(1, 50), (3, 900), (6, 600), (11, 1100)]);

        // a generation whose every run for this segment is OPEN_END is
        // indistinguishable from no closes at all, on an all_current segment
        let hollow = CloseIndex::from_records([rec(1, 2, OPEN_END)]);
        assert!(matches!(
            close_view(Sidecar::new(&[]), &hollow, 1),
            CloseView::None
        ));
    }

    #[test]
    fn the_close_view_says_exactly_what_the_per_row_test_says() {
        // The precedence rules are the load-bearing part: a divergence here
        // changes an answer. Cross-check the view against `effective_tt_e`'s
        // own spelling of the rule, row by row, over layered and unlayered
        // indexes and every mix of folded/run/absent.
        let mut seed = 0x2026_0919_u64;
        let mut rng = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for case in 0..200 {
            let rows = 1 + (rng() % 24) as u32;
            let mut folded: Vec<(u32, i64)> = Vec::new();
            for r in 0..rows {
                if rng() % 3 == 0 {
                    folded.push((r, 1 + (rng() % 5) as i64 * 10));
                }
            }
            let mut recs = Vec::new();
            for r in 0..rows {
                match rng() % 4 {
                    0 => recs.push(rec(1, r, 1 + (rng() % 5) as i64 * 10)),
                    1 => recs.push(rec(1, r, OPEN_END)),
                    _ => {}
                }
            }
            let committed = std::sync::Arc::new(CloseIndex::from_records(recs));
            let layered = if case % 2 == 0 {
                let mut top = CloseIndex::layered_over(committed.clone());
                for r in 0..rows {
                    if rng() % 5 == 0 {
                        top.close_row(1, r, 1 + (rng() % 5) as i64 * 10);
                    }
                }
                top
            } else {
                CloseIndex::clone(&committed)
            };
            let sidecar = Sidecar::new(&folded);
            let view = close_view(sidecar, &layered, 1);
            let slice = view.as_slice();
            assert!(
                slice.windows(2).all(|w| w[0].0 < w[1].0),
                "case {case}: the view must be sorted and deduplicated: {slice:?}"
            );
            for r in 0..rows {
                let from_view = match slice.binary_search_by_key(&r, |(row, _)| *row) {
                    Ok(i) => slice[i].1,
                    Err(_) => OPEN_END,
                };
                let from_rows = crate::read::effective_tt_e(&layered, &sidecar, 1, r);
                assert_eq!(
                    from_view, from_rows,
                    "case {case} row {r}: view and per-row test disagree"
                );
            }
        }
    }

}
