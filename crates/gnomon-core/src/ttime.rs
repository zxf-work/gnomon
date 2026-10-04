//! The one reader of a segment's transaction-time axis.
//!
//! A segment stores `tt_s` in one of two shapes, and which one is a
//! measurement about the segment rather than a choice about the format:
//!
//! * **Runs** — `(first_row, tt_s)` pairs in the header. A batch writes
//!   exactly one, and while runs stay long this is the cheapest thing that
//!   could work.
//! * **Dictionary** — the distinct times, ascending, in the header, plus a
//!   per-row code column packed by the ordinary frame-of-reference codec,
//!   plus per-block code bounds.
//!
//! Compaction is what forces the second shape to exist. It re-sorts rows
//! from every generation into global `(vt_s, vid)` order while each row keeps
//! its origin `tt_s`, so transaction time arrives shredded: measured at
//! 0.6–0.9 runs per row on compacted SNAP-derived stores, where the run
//! list — `Vec<(u32, i64)>` serialised as JSON, about 28 characters an entry
//! — reached **123 to 190 bits per row**, 37–46 % of the whole segment file.
//! The distinct values behind those runs numbered 11 to 157. A dictionary
//! costs 4 to 8 bits per row for the same information, exactly.
//!
//! Neither the dictionary nor the per-block bounds is a new mechanism. The
//! segment already interns strings and dictionary-codes `rel_code`, `disc`
//! and `props`; per-block min/max metadata used to skip blocks is small
//! materialized aggregates (Moerkotte, 1998), shipped everywhere as
//! zone maps. This is those two moves applied to a column that had been left
//! in the header.
//!
//! `TtSView` exists so no caller has to know which shape it got. It borrows
//! the header and, on the dictionary shape, the decoded code column; it owns
//! nothing and allocates nothing.

use crate::error::{EngineError, Result};

/// The per-row codes, at whichever width the dictionary's size needed.
#[derive(Clone, Copy)]
pub(crate) enum TtCodes<'a> {
    U16(&'a [u16]),
    U32(&'a [u32]),
}

impl TtCodes<'_> {
    #[inline]
    pub(crate) fn get(self, row: usize) -> Option<u32> {
        match self {
            TtCodes::U16(c) => c.get(row).map(|&v| v as u32),
            TtCodes::U32(c) => c.get(row).copied(),
        }
    }

    #[inline]
    pub(crate) fn len(self) -> usize {
        match self {
            TtCodes::U16(c) => c.len(),
            TtCodes::U32(c) => c.len(),
        }
    }
}

/// A segment's transaction-time axis, in whichever shape it was written.
#[derive(Clone, Copy)]
pub struct TtSView<'a> {
    runs: &'a [(u32, i64)],
    dict: &'a [i64],
    blocks: &'a [(u32, u32)],
    block_rows: usize,
    codes: Option<TtCodes<'a>>,
    bounds: (i64, i64),
}

impl<'a> TtSView<'a> {
    /// The run-list shape. Also what a header carrying no dictionary gives,
    /// which is every segment written before this change — so an old store reads
    /// through exactly the code it always did.
    pub fn from_header(h: &'a crate::segment::SegmentHeader) -> Self {
        Self {
            runs: &h.tt_s_runs,
            dict: &[],
            blocks: &[],
            block_rows: 0,
            codes: None,
            bounds: h.tt_s_bounds(),
        }
    }

    /// The run-list shape with `(min, max)` supplied by the caller: the
    /// segment folds it once at open, where [`TtSView::from_header`]
    /// folds the whole run list again on every call.
    pub(crate) fn from_header_with_bounds(
        h: &'a crate::segment::SegmentHeader,
        bounds: (i64, i64),
    ) -> Self {
        Self {
            runs: &h.tt_s_runs,
            dict: &[],
            blocks: &[],
            block_rows: 0,
            codes: None,
            bounds,
        }
    }

    pub(crate) fn dictionary(
        h: &'a crate::segment::SegmentHeader,
        codes: TtCodes<'a>,
        bounds: (i64, i64),
    ) -> Self {
        Self {
            runs: &[],
            dict: &h.tt_s_dict,
            blocks: &h.tt_s_code_blocks,
            block_rows: h.tt_s_block_rows as usize,
            codes: Some(codes),
            bounds,
        }
    }

    /// The run list, empty on the dictionary shape.
    #[inline]
    pub(crate) fn runs(&self) -> &'a [(u32, i64)] {
        self.runs
    }

    /// Does this segment code `tt_s` by dictionary?
    #[inline]
    pub fn is_dictionary(&self) -> bool {
        self.codes.is_some()
    }

    /// `(min, max)` transaction time, folded once when the segment opened.
    #[inline]
    pub fn bounds(&self) -> (i64, i64) {
        self.bounds
    }

    /// Transaction time of one row.
    ///
    /// Called once per materialized row, which is why the dictionary shape
    /// is worth having twice over: it is one indexed load plus one indexed
    /// load, against a binary search over a list that compaction can leave
    /// nearly as long as the segment.
    #[inline]
    pub fn at(&self, row: u32) -> Result<i64> {
        match self.codes {
            Some(codes) => {
                let code = codes.get(row as usize).ok_or_else(|| {
                    EngineError::corrupt(format!(
                        "tt_s code column has {} rows, asked for row {row}",
                        codes.len()
                    ))
                })?;
                self.dict.get(code as usize).copied().ok_or_else(|| {
                    EngineError::corrupt(format!(
                        "row {row} names tt_s code {code} of {}",
                        self.dict.len()
                    ))
                })
            }
            None => self
                .runs
                .partition_point(|(start, _)| *start <= row)
                .checked_sub(1)
                .map(|i| self.runs[i].1)
                .ok_or_else(|| {
                    EngineError::corrupt(format!("no tt_s run covers row {row}"))
                }),
        }
    }

    /// How many maximal runs of equal `tt_s` this segment's rows form.
    ///
    /// Layout quality, not integrity — it is what the read path used to pay
    /// per materialized row, and `verify` reports it so a regression is
    /// visible. Reported for both shapes so the census keeps meaning the
    /// same thing after the change: on the run list it is the list's length, and
    /// on the dictionary it is counted from the codes, which is the same
    /// number the run list would have held.
    pub fn run_count(&self) -> usize {
        match self.codes {
            None => self.runs.len(),
            Some(TtCodes::U16(c)) => count_runs(c),
            Some(TtCodes::U32(c)) => count_runs(c),
        }
    }

    /// Row ranges believed at `as_of`, on the dictionary shape.
    ///
    /// The mixed case only. Both all-believed and none-believed are settled
    /// by `bounds` before this is reached, exactly as they are on the run
    /// list, and current belief — the overwhelmingly common query — is
    /// always the all-believed case.
    ///
    /// Because the dictionary ascends, `tt_s <= as_of` is `code < cut` for a
    /// single `cut` found by one binary search over 11–157 entries. Each
    /// block's stored `(min, max)` then decides the whole block: `max < cut`
    /// is wholly believed, `min >= cut` is wholly unbelieved, and only a
    /// block straddling the cut is walked row by row. Adjacent ranges
    /// sharing a verdict coalesce, so the output is one range per verdict
    /// *change* — the identical contract, and the identical ranges, that the
    /// run-derived answer produces.
    pub(crate) fn believed_ranges_dict(
        &self,
        as_of: i64,
        rows: usize,
    ) -> Vec<(usize, usize)> {
        let Some(codes) = self.codes else {
            return Vec::new();
        };
        let rows = rows.min(codes.len());
        if rows == 0 {
            return Vec::new();
        }
        // codes 0..cut are believed; cut == 0 and cut == dict.len() are the
        // none/all cases the caller's bounds test already took.
        let cut = self.dict.partition_point(|&t| t <= as_of) as u32;
        if cut == 0 {
            return Vec::new();
        }
        let block_rows = if self.block_rows > 0 {
            self.block_rows
        } else {
            rows
        };
        let mut out: Vec<(usize, usize)> = Vec::new();
        let push = |start: usize, end: usize, out: &mut Vec<(usize, usize)>| {
            if start >= end {
                return;
            }
            match out.last_mut() {
                Some(last) if last.1 == start => last.1 = end,
                _ => out.push((start, end)),
            }
        };
        let mut start = 0usize;
        let mut b = 0usize;
        let mut walked = 0usize;
        while start < rows {
            let end = (start + block_rows).min(rows);
            match self.blocks.get(b) {
                // wholly believed: no row is read
                Some(&(_, hi)) if hi < cut => push(start, end, &mut out),
                // wholly unbelieved: no row is read
                Some(&(lo, _)) if lo >= cut => {}
                // straddles the cut, or the bounds vector is short (a header
                // that lost it): walk the rows
                _ => {
                    walked += end - start;
                    let mut run: Option<usize> = None;
                    for row in start..end {
                        let believed = codes.get(row).is_some_and(|c| c < cut);
                        match (believed, run) {
                            (true, None) => run = Some(row),
                            (false, Some(s)) => {
                                push(s, row, &mut out);
                                run = None;
                            }
                            _ => {}
                        }
                    }
                    if let Some(s) = run {
                        push(s, end, &mut out);
                    }
                }
            }
            start = end;
            b += 1;
        }
        crate::vistouch::codes(walked);
        out
    }

    /// [`TtSView::believed_ranges_dict`] restricted to the rows `[lo, hi)`.
    ///
    /// Only the code blocks that cover `[lo, hi)` are visited, and a
    /// straddling block is read only for its rows inside the range. A
    /// block's verdict from its bounds is a statement about each of its
    /// rows, so clipping a decided block and clipping a walked block both
    /// give exactly the whole-segment answer's rows in `[lo, hi)`, and the
    /// same coalescing makes them the same ranges. Cost: one bounds check
    /// per covered block plus at most `hi - lo` codes, where the
    /// whole-segment form reads every row of every straddling block.
    pub(crate) fn believed_ranges_dict_in(
        &self,
        as_of: i64,
        rows: usize,
        lo: usize,
        hi: usize,
    ) -> Vec<(usize, usize)> {
        let Some(codes) = self.codes else {
            return Vec::new();
        };
        let rows = rows.min(codes.len());
        let hi = hi.min(rows);
        if lo >= hi {
            return Vec::new();
        }
        let cut = self.dict.partition_point(|&t| t <= as_of) as u32;
        if cut == 0 {
            return Vec::new();
        }
        let block_rows = if self.block_rows > 0 {
            self.block_rows
        } else {
            rows
        };
        let mut out: Vec<(usize, usize)> = Vec::new();
        let push = |start: usize, end: usize, out: &mut Vec<(usize, usize)>| {
            if start >= end {
                return;
            }
            match out.last_mut() {
                Some(last) if last.1 == start => last.1 = end,
                _ => out.push((start, end)),
            }
        };
        let mut start = lo;
        let mut walked = 0usize;
        while start < hi {
            let b = start / block_rows;
            let end = (b + 1).saturating_mul(block_rows).min(rows).min(hi);
            match self.blocks.get(b) {
                Some(&(_, bhi)) if bhi < cut => push(start, end, &mut out),
                Some(&(blo, _)) if blo >= cut => {}
                _ => {
                    walked += end - start;
                    let mut run: Option<usize> = None;
                    for row in start..end {
                        let believed = codes.get(row).is_some_and(|c| c < cut);
                        match (believed, run) {
                            (true, None) => run = Some(row),
                            (false, Some(s)) => {
                                push(s, row, &mut out);
                                run = None;
                            }
                            _ => {}
                        }
                    }
                    if let Some(s) = run {
                        push(s, end, &mut out);
                    }
                }
            }
            start = end;
        }
        crate::vistouch::codes(walked);
        out
    }
}

fn count_runs<T: PartialEq + Copy>(codes: &[T]) -> usize {
    let mut n = 0usize;
    let mut prev: Option<T> = None;
    for &c in codes {
        if prev != Some(c) {
            n += 1;
            prev = Some(c);
        }
    }
    n
}
