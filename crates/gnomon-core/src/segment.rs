//! Immutable columnar segment files — the `.tgs` format.
//!
//! A segment is written once and never modified. That single property is what
//! buys snapshot isolation, safe `mmap`, and lock-free concurrent readers: a
//! reader holding a generation can map these bytes and know nothing will
//! change underneath it.
//!
//! Layout:
//!
//! ```text
//! magic "TGSG" | format u32 | header_len u32 | header JSON | pad to 64
//! <data_start>  column extents, each padded to 64 bytes
//!               string table
//! <footer>      footer JSON | footer_len u32 | magic "TGSE"
//! ```
//!
//! Column offsets in the header are relative to `data_start`, so they do not
//! depend on how long the header serializes to.
//!
//! Every column extent and the string table carry a CRC32C, and the whole
//! pre-footer body carries a SHA. The trailing magic doubles as the
//! *complete marker*: a file without it was never finished, which is exactly
//! what a crash mid-write leaves behind.
//!
//! Hot columns are fixed-width and 64-byte aligned so a scan streams cache
//! lines and the typed views below are zero-copy. Everything variable-width
//! (props, disc, labels) lives out-of-line in the string table, referenced by
//! `u32` index — which is also why event data compresses so well later: the
//! distinct payloads in a segment are usually a handful.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::derive::{sha256_hex_bytes, Id96};
use crate::ingest_phase::{self, Timer, S};
use crate::error::{EngineError, Result};
use crate::manifest::SHA_HEX_LEN;
use crate::row::{EdgeRow, Lane, NodeRow, RowKind};
use crate::FORMAT_VERSION;

const MAGIC: &[u8; 4] = b"TGSG";
const FOOTER_MAGIC: &[u8; 4] = b"TGSE";
const ALIGN: usize = 64;
const PREAMBLE: usize = 12; // magic + format + header_len

/// Reference into the segment string table meaning SQL NULL (`None`).
pub const NULL_REF: u32 = u32::MAX;

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum DType {
    I64,
    U64,
    U32,
    U16,
}

impl DType {
    const fn size(self) -> usize {
        match self {
            DType::I64 | DType::U64 => 8,
            DType::U32 => 4,
            DType::U16 => 2,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ColumnDesc {
    pub name: String,
    pub dtype: DType,
    /// Offset from `data_start`, always a multiple of 64.
    pub offset: u64,
    pub bytes: u64,
    /// 0 = raw little-endian. Codec ids are reserved from format v0 so that
    /// adding compression later is a value change, not a format break.
    pub codec: u32,
    /// Exact scale: the stream holds `(v - base) / divisor`, and the reader
    /// recovers `v = base + divisor * k`.
    ///
    /// Lossless by construction — `divisor` is the greatest common divisor
    /// of the column's first differences, so every value is an exact
    /// multiple of it away from `base`. On SNAP-derived stores this is
    /// 1,000,000 (Unix seconds lifted into microseconds), which is twenty
    /// bits of every packed delta that were provably zero. `(0, 1)` is the
    /// identity, which `#[serde(default)]` gives every older column, so
    /// old headers read unchanged and `FORMAT_VERSION` stays 1.
    ///
    /// Applies to [`crate::codec::CODEC_FOR`] only. Raw columns are served
    /// zero-copy out of the mapping and so cannot carry a transform, and
    /// [`crate::codec::CODEC_LINMODEL`] carries its own scale in its stream
    /// where the per-block fit can use it.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub base: i64,
    #[serde(default = "one", skip_serializing_if = "is_one")]
    pub divisor: i64,
}

/// `serde` default for [`ColumnDesc::divisor`]: the identity scale.
fn one() -> i64 {
    1
}

// A header key that is written only when it carries information. The
// alternative -- always serialising the identity -- costs real bits: a store
// of many small segments pays the fixed JSON of every new key on every
// segment, measured at +0.32 bits/row on a 267-segment synthetic store where
// none of the new mechanisms fired at all. Omitting them keeps the promise
// the change was sold on: a segment that takes neither the dictionary nor
// the scale nor the model has a header byte-identical to the one it had
// before these mechanisms existed.
fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

fn is_one(v: &i64) -> bool {
    *v == 1
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    pub format: u32,
    pub kind: RowKind,
    pub lane: Lane,
    pub rows: u32,
    pub block_rows: u32,
    /// Full 96-bit composite boundary keys.
    pub key_lo: (i64, String),
    pub key_hi: (i64, String),
    pub vt_min: i64,
    pub vt_max: i64,
    /// Max `vt_e` in the segment — the cross-segment overlap bound.
    pub vt_e_max: i64,
    /// Run-length encoded transaction times: `(first_row, tt_s)`. A batch
    /// writes one entry; only compaction produces more.
    ///
    /// Written **only** when the run density is below one run per
    /// [`TT_S_DICT_DENSITY_DEN`] rows — see [`tt_s_wants_dictionary`]. At or
    /// above that density the runs are replaced by a dictionary-coded
    /// `tt_s_code` column and this vector is empty. The two representations
    /// are mutually exclusive and [`crate::ttime::TtSView`] is the single
    /// reader of either.
    pub tt_s_runs: Vec<(u32, i64)>,
    /// Distinct transaction times, **ascending**, when this segment carries
    /// a `tt_s_code` column instead of `tt_s_runs`. Empty on the run-list
    /// representation and on every segment written before this field existed, which
    /// `#[serde(default)]` supplies for free — no format bump,
    /// `FORMAT_VERSION` stays 1 and an old reader meets an unknown *column*
    /// name rather than an unparseable header.
    ///
    /// Ascending rather than first-appearance order, because that is what
    /// makes a code comparison a time comparison: `code < c` iff
    /// `tt_s < dict[c]`. That equivalence is what turns the per-block code
    /// bounds below into a belief verdict for a whole block. Sorting
    /// distinct `i64`s is a deterministic integer function of the rows in
    /// stored order, so the bytes stay reproducible.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tt_s_dict: Vec<i64>,
    /// Per-block `(min code, max code)` over the `tt_s_code` column, at
    /// [`SegmentHeader::tt_s_block_rows`] granularity.
    ///
    /// This is what keeps `believed_ranges` off a per-row scan. Because the
    /// dictionary ascends, a block whose max code is below the `as_of` cut
    /// is wholly believed and a block whose min code is at or above it is
    /// wholly unbelieved; only a block straddling the cut is walked. The run
    /// list this replaces cost 0.6–0.9 entries per row on compacted real
    /// stores; this costs one pair per [`TT_S_CODE_BLOCK_ROWS`] rows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tt_s_code_blocks: Vec<(u32, u32)>,
    /// Granularity of [`SegmentHeader::tt_s_code_blocks`], independent of
    /// `block_rows` so the two can be tuned separately without a format
    /// question. Only meaningful when that vector is non-empty; 0 on an old
    /// header, which is never read because the empty dictionary
    /// short-circuits first.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub tt_s_block_rows: u32,
    pub rel_types: Vec<String>,
    pub columns: Vec<ColumnDesc>,
    /// `vt_e` is omitted because every row is instantaneous (`vt_s + 1`).
    pub vt_e_elided: bool,
    pub strings_offset: u64,
    pub strings_bytes: u64,
    pub strings_count: u32,
    /// Folded closes: sorted `(row, tt_e)`. Empty means every row in this
    /// segment is still believed — the `all_current` fast path.
    #[serde(default)]
    pub closed_rows: Vec<(u32, i64)>,
    /// Running maximum of `vt_e` over **closed rows only**, sampled at
    /// `vt_e_block_rows` boundaries: entry `i` is the greatest `vt_e` among
    /// rows in blocks `0..=i` whose `vt_e != OPEN_END`.
    ///
    /// Written only for interval-lane segments with an explicit `vt_e`
    /// column. Empty means
    /// no structure — every reader, old or new, falls back to the per-row
    /// `vt_e` test. `#[serde(default)]` gives `Vec::new()` on an old header
    /// with no such key, which parses to exactly that fallback: no format
    /// bump, `FORMAT_VERSION` stays 1.
    ///
    /// **Renamed from `block_vt_e_running_max`, and re-scoped.** "Staircase"
    /// collided with Staircase Join (Grust et al., 2003), and the
    /// structure is not a contribution to be named: a per-block maximum used
    /// to skip blocks is small materialized aggregates (Moerkotte,
    /// 1998), shipped as zone maps in every columnar engine, and the
    /// augmented interval tree's max-endpoint (a textbook technique) is the same idea
    /// on the other axis. `#[serde(alias)]` keeps every segment written
    /// under the old key readable.
    ///
    /// The re-scoping is the substance. A running maximum is a **prefix
    /// aggregate**, so *one* far-future `vt_e` raises every later entry and
    /// every later block stops being prunable. Measured: one open-ended row
    /// in ten thousand took the pruned fraction from 0.507 to 1.0000, and
    /// the only real interval-lane segment in the design corpus is 194,085
    /// rows *every one of which* is open-ended — so it pruned nothing at
    /// all. Computing over closed rows and listing the open-ended ones
    /// separately ([`SegmentHeader::open_rows`]) restores the cut at any
    /// open-ended fraction, because an open-ended row needs no `vt_e` test:
    /// it overlaps every window that its `vt_s` reaches.
    #[serde(default, alias = "block_prefix_max_vt_e", skip_serializing_if = "Vec::is_empty")]
    pub block_vt_e_running_max: Vec<i64>,
    /// Granularity of [`SegmentHeader::block_vt_e_running_max`],
    /// independent of `block_rows` so the two can be tuned separately
    /// without a format question. Only meaningful when that vector is
    /// non-empty; `#[serde(default)]` gives 0 on an old header, which is
    /// never read because the empty vector short-circuits first.
    #[serde(default, alias = "stair_block_rows")]
    pub vt_e_block_rows: u32,
    /// Rows whose `vt_e` is [`crate::OPEN_END`], as sorted half-open
    /// **ranges** `[start, end)`.
    ///
    /// Run-length encoded rather than listed row by row because the shape
    /// that matters is the degenerate one: the corpus's only real
    /// interval-lane segment is 194,085 rows of which every single one is
    /// open-ended, and a row-id list there would cost 54 bits/row — more
    /// than the `vt_e` column it is describing, which
    /// frame-of-reference takes to 0.00 bits/row precisely because the
    /// value never changes. As ranges it is one entry. At the other end,
    /// one open-ended row in ten thousand over four million rows is 400
    /// entries.
    ///
    /// Empty on every segment written before this field existed, and on every segment
    /// with no open-ended rows, which is the same thing to a reader.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_rows: Vec<(u32, u32)>,
}

impl SegmentHeader {
    pub fn column(&self, name: &str) -> Option<&ColumnDesc> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Heap this parsed header owns, in bytes (part of memory accounting).
    ///
    /// Every field here is decoded once per segment open and held for as long
    /// as the segment is, but none of it is part of [`Segment::resident_bytes`]
    /// — so the segment cache's byte budget does not bound it. `tt_s_runs`
    /// is why that matters: a batch writes one run, but compaction re-sorts
    /// rows from every generation while each keeps its origin `tt_s`, and
    /// [`Self::tt_s_at`]'s own note records a compacted segment measured at
    /// 2.1M runs over 3.0M rows. At 16 B per run that is ~11 B per row of a
    /// heavily compacted store, held per cached segment and invisible to
    /// `segment_cache_stats`.
    ///
    /// Counts the `Vec`/`String` *capacities* the header allocated, not the
    /// `size_of` of the header struct itself (which lives inside the
    /// `Segment` allocation the caller already accounts).
    pub fn heap_bytes(&self) -> u64 {
        let runs = self.tt_s_runs.capacity() * std::mem::size_of::<(u32, i64)>();
        let closed = self.closed_rows.capacity() * std::mem::size_of::<(u32, i64)>();
        // This field was originally named `block_prefix_max_vt_e`; it was renamed to
        // `block_vt_e_running_max` and `open_rows` was added beside it, so both
        // are counted here. A heap accounting that silently omits a header
        // vector under-reports exactly the segments the new structure is for.
        let stair = self.block_vt_e_running_max.capacity() * std::mem::size_of::<i64>();
        let open = self.open_rows.capacity() * std::mem::size_of::<(u32, u32)>();
        let rel: usize = self.rel_types.capacity() * std::mem::size_of::<String>()
            + self.rel_types.iter().map(String::capacity).sum::<usize>();
        let cols: usize = self.columns.capacity() * std::mem::size_of::<ColumnDesc>()
            + self.columns.iter().map(|c| c.name.capacity()).sum::<usize>();
        let keys = self.key_lo.1.capacity() + self.key_hi.1.capacity();
        (runs + closed + stair + open + rel + cols + keys) as u64
    }

    /// Transaction time of one row, from the run-length encoding.
    ///
    /// Run starts ascend, so this is a binary search for the last run at or
    /// before `row` — the same run the reverse linear scan used to find. The
    /// distinction is not academic: a batch writes one run, but compaction
    /// re-sorts rows from every generation into global `(vt_s, vid)` order
    /// while each row keeps its origin `tt_s`, and a compacted segment has
    /// been measured at 2.1M runs over 3.0M rows. This is called once per
    /// materialized row.
    ///
    /// **Runs only.** A dictionary-coded segment answers this from its code
    /// column, which is not in the header — ask
    /// [`Segment::tt_s_view`] instead. Calling this on such a header is a
    /// programming error, and it says so rather than returning a wrong time.
    pub fn tt_s_at(&self, row: u32) -> Result<i64> {
        self.tt_s_runs
            .partition_point(|(start, _)| *start <= row)
            .checked_sub(1)
            .map(|i| self.tt_s_runs[i].1)
            .ok_or_else(|| {
                if self.tt_s_dict.is_empty() {
                    EngineError::corrupt(format!("no tt_s run covers row {row}"))
                } else {
                    EngineError::invariant(
                        "this segment codes tt_s by dictionary; read it through \
                         Segment::tt_s_view, not SegmentHeader::tt_s_at",
                    )
                }
            })
    }

    /// `(min, max)` transaction time over the segment, or
    /// `(i64::MAX, i64::MIN)` when it holds none — an empty header believes
    /// nothing at any `as_of`, which is what those sentinels give the belief
    /// predicate for free.
    ///
    /// O(1) on the dictionary representation (its ends are the extremes,
    /// because it ascends) and O(runs) on the run list; fold it once (see
    /// [`Segment::tt_s_bounds`]) rather than per scan.
    pub fn tt_s_bounds(&self) -> (i64, i64) {
        if let (Some(&lo), Some(&hi)) = (self.tt_s_dict.first(), self.tt_s_dict.last()) {
            return (lo, hi);
        }
        // A walk of every run, which is why a reader must never pay it
        // per call (the segment folds it once at open); counted so a test can
        // see where it is paid
        crate::vistouch::runs(self.tt_s_runs.len());
        self.tt_s_runs
            .iter()
            .fold((i64::MAX, i64::MIN), |(lo, hi), (_, tt)| {
                (lo.min(*tt), hi.max(*tt))
            })
    }
}

/// Granularity of [`SegmentHeader::tt_s_code_blocks`].
///
/// 4096 rows, not the segment's `block_rows` (32,768): the bounds vector is
/// the whole reason a dictionary-coded segment answers `believed_ranges`
/// without a row scan, and a *mixed* block is walked row by row, so the
/// block wants to be small enough that straddling the `as_of` cut is cheap.
/// At 4096 the vector costs one `(u32, u32)` pair per 4096 rows — under
/// 0.03 bits/row in the header's JSON, against the 123–190 bits/row the run
/// list cost on the same segments.
pub const TT_S_CODE_BLOCK_ROWS: u32 = 4096;

/// The run-density denominator: the run list is kept while there is fewer
/// than one run per this many rows.
///
/// 16 is chosen so the representation never *grows* a segment. A run is 12
/// bytes of payload (and ~28 characters of JSON), a code is at most 16 bits
/// packed plus a dictionary entry, so one run per 16 rows is comfortably
/// inside the region where the run list still wins — and it leaves every
/// batch-shaped segment (exactly one run) on the run-list path, where
/// `tt_s_at` and `believed_ranges` are untouched by this representation.
pub const TT_S_DICT_DENSITY_DEN: usize = 16;

/// Below this many runs the list cannot be a size problem, whatever the
/// density.
///
/// The density rule alone reads "one run per 16 rows", which a *one*-run
/// segment of 16 rows or fewer satisfies — and there the dictionary is a
/// little worse than the run list, because its fixed cost (the distinct
/// values, the block bounds) is paid against a run list of one entry. The
/// floor keeps every batch-shaped segment on the run list at every size,
/// which is the guarantee that matters: what compaction produces is
/// thousands of runs, not fifteen.
pub const TT_S_DICT_MIN_RUNS: usize = 16;

/// Should this segment code `tt_s` by dictionary rather than by runs?
///
/// `runs >= TT_S_DICT_MIN_RUNS` **and**
/// `runs / rows >= 1 / TT_S_DICT_DENSITY_DEN`, in integer arithmetic. A
/// batch writes one run over however many rows and stays on the run list;
/// compaction's global re-sort by `(vt_s, vid)` shreds transaction time into
/// 0.6–0.9 runs per row and crosses both tests immediately.
pub fn tt_s_wants_dictionary(runs: usize, rows: usize) -> bool {
    rows > 0
        && runs >= TT_S_DICT_MIN_RUNS
        && runs.saturating_mul(TT_S_DICT_DENSITY_DEN) >= rows
}

/// Turn a run list into `(ascending distinct times, per-row codes)`.
///
/// Built from the runs rather than from a second pass over the rows so the
/// writer never holds a per-row `i64` buffer of transaction times: the runs
/// already are that information, losslessly, and they are the only thing the
/// write loop accumulated. Deterministic by construction — sorting and
/// deduplicating `i64`s, then one binary search per run.
///
/// Returns `None` when the dictionary would not fit a `u32` code space,
/// which cannot happen for a segment (`rows` is a `u32`) but is checked
/// rather than assumed.
fn build_tt_s_dictionary(runs: &[(u32, i64)], rows: usize) -> Option<(Vec<i64>, Vec<u32>)> {
    let mut dict: Vec<i64> = runs.iter().map(|(_, tt)| *tt).collect();
    dict.sort_unstable();
    dict.dedup();
    if dict.len() > u32::MAX as usize {
        return None;
    }
    let mut codes = vec![0u32; rows];
    for (i, (start, tt)) in runs.iter().enumerate() {
        let end = runs.get(i + 1).map_or(rows, |(s, _)| *s as usize).min(rows);
        let start = (*start as usize).min(end);
        let code = dict.binary_search(tt).ok()? as u32;
        codes[start..end].fill(code);
    }
    Some((dict, codes))
}

/// `(min code, max code)` per [`TT_S_CODE_BLOCK_ROWS`] block.
fn tt_s_code_blocks(codes: &[u32]) -> Vec<(u32, u32)> {
    codes
        .chunks(TT_S_CODE_BLOCK_ROWS as usize)
        .map(|b| {
            b.iter().fold((u32::MAX, 0u32), |(lo, hi), &c| (lo.min(c), hi.max(c)))
        })
        .collect()
}

/// The per-row transaction-time code column's name.
pub const TT_S_CODE_COLUMN: &str = "tt_s_code";

/// Which of the two `tt_s` representations this segment gets, resolved once
/// and shared by both writers.
struct TtSEncoding {
    runs: Vec<(u32, i64)>,
    dict: Vec<i64>,
    blocks: Vec<(u32, u32)>,
    block_rows: u32,
    column: Option<ColumnBuilder>,
}

/// Decide the representation and build whatever it needs.
///
/// The run list is kept whenever it is still cheap ([`tt_s_wants_dictionary`])
/// — which is every batch-written segment — so `tt_s_at`, `believed_ranges`
/// and the header bytes are untouched on that shape. Above the density
/// threshold the runs become a dictionary plus a per-row code column, which
/// the existing frame-of-reference codec packs at exactly
/// `ceil(log2(distinct))` bits.
///
/// The code column's dtype is chosen by the dictionary's size, not fixed:
/// the measured maximum in the design corpus is 157 distinct transaction
/// times per segment, so `u16` is the case that matters and it halves what
/// the decoded column costs the segment cache. `u32` is the honest fallback
/// rather than a refusal, so the representation has no cliff.
fn encode_tt_s(runs: Vec<(u32, i64)>, n: usize, allowed: bool) -> TtSEncoding {
    let keep_runs = |runs| TtSEncoding {
        runs,
        dict: Vec::new(),
        blocks: Vec::new(),
        block_rows: 0,
        column: None,
    };
    if !allowed || !tt_s_wants_dictionary(runs.len(), n) {
        return keep_runs(runs);
    }
    let Some((dict, codes)) = build_tt_s_dictionary(&runs, n) else {
        return keep_runs(runs);
    };
    let blocks = tt_s_code_blocks(&codes);
    // codes 0..=65535 fit a u16, i.e. a dictionary of up to 65,536 entries
    let dtype = if dict.len() <= u16::MAX as usize + 1 {
        DType::U16
    } else {
        DType::U32
    };
    let mut column = ColumnBuilder::new(TT_S_CODE_COLUMN, dtype, n);
    for &c in &codes {
        match dtype {
            DType::U16 => column.push_u16(c as u16),
            _ => column.push_u32(c),
        }
    }
    TtSEncoding {
        runs: Vec::new(),
        dict,
        blocks,
        block_rows: TT_S_CODE_BLOCK_ROWS,
        column: Some(column),
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct SegmentFooter {
    extent_crcs: Vec<u32>,
    strings_crc: u32,
    rows: u64,
    /// SHA of every byte before the footer.
    body_sha: String,
}

// --------------------------------------------------------------------- //
// string table                                                          //
// --------------------------------------------------------------------- //

/// Deduplicating string pool. Index 0 is always `""`, so a "no value" ref is
/// cheap; genuine NULL uses [`NULL_REF`] and is distinct from empty.
#[derive(Default)]
pub struct StringTable {
    items: Vec<String>,
    lookup: HashMap<String, u32>,
}

impl StringTable {
    pub fn new() -> Self {
        let mut t = Self::default();
        t.intern("");
        t
    }

    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&i) = self.lookup.get(s) {
            return i;
        }
        let i = self.items.len() as u32;
        self.items.push(s.to_string());
        self.lookup.insert(s.to_string(), i);
        i
    }

    pub fn intern_opt(&mut self, s: Option<&str>) -> u32 {
        match s {
            None => NULL_REF,
            Some(v) => self.intern(v),
        }
    }

    pub fn len(&self) -> u32 {
        self.items.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// `count u32 | offsets u32 x (count+1) | utf-8 bytes`
    fn encode(&self) -> Vec<u8> {
        let payload: usize = self.items.iter().map(|s| s.len()).sum();
        let mut out = Vec::with_capacity(4 + 4 * (self.items.len() + 1) + payload);
        out.extend_from_slice(&(self.items.len() as u32).to_le_bytes());
        let mut cursor = 0u32;
        for s in &self.items {
            out.extend_from_slice(&cursor.to_le_bytes());
            cursor += s.len() as u32;
        }
        out.extend_from_slice(&cursor.to_le_bytes());
        for s in &self.items {
            out.extend_from_slice(s.as_bytes());
        }
        out
    }
}

/// Read-side view over an encoded string table.
pub struct StringView<'a> {
    offsets: &'a [u8],
    payload: &'a [u8],
    count: u32,
}

impl<'a> StringView<'a> {
    fn parse(bytes: &'a [u8]) -> Result<Self> {
        let count = read_u32(bytes, 0)?;
        let table_end = 4 + 4 * (count as usize + 1);
        if bytes.len() < table_end {
            return Err(EngineError::corrupt("string table offsets are truncated"));
        }
        Ok(Self {
            offsets: &bytes[4..table_end],
            payload: &bytes[table_end..],
            count,
        })
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    pub fn get(&self, idx: u32) -> Result<&'a str> {
        if idx >= self.count {
            return Err(EngineError::corrupt(format!(
                "string ref {idx} out of range (table holds {})",
                self.count
            )));
        }
        let i = idx as usize * 4;
        let start = u32::from_le_bytes(self.offsets[i..i + 4].try_into().expect("4 bytes")) as usize;
        let end =
            u32::from_le_bytes(self.offsets[i + 4..i + 8].try_into().expect("4 bytes")) as usize;
        let raw = self
            .payload
            .get(start..end)
            .ok_or_else(|| EngineError::corrupt(format!("string ref {idx} spans past the table")))?;
        std::str::from_utf8(raw)
            .map_err(|e| EngineError::corrupt(format!("string ref {idx} is not UTF-8: {e}")))
    }

    /// `None` for [`NULL_REF`], otherwise the interned string.
    pub fn get_opt(&self, idx: u32) -> Result<Option<&'a str>> {
        if idx == NULL_REF {
            Ok(None)
        } else {
            self.get(idx).map(Some)
        }
    }
}

// --------------------------------------------------------------------- //
// writing                                                               //
// --------------------------------------------------------------------- //

struct ColumnBuilder {
    name: &'static str,
    dtype: DType,
    bytes: Vec<u8>,
}

impl ColumnBuilder {
    fn new(name: &'static str, dtype: DType, rows: usize) -> Self {
        Self {
            name,
            dtype,
            bytes: Vec::with_capacity(rows * dtype.size()),
        }
    }

    fn push_i64(&mut self, v: i64) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }
    fn push_u64(&mut self, v: u64) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }
    fn push_u32(&mut self, v: u32) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }
    fn push_u16(&mut self, v: u16) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }
}

/// Everything the caller must decide before rows become a file.
pub struct SegmentSpec {
    pub lane: Lane,
    pub block_rows: u32,
    /// Closes already known at seal time — a version created and corrected
    /// inside one batch. Sorted `(row, tt_e)`.
    pub closed_rows: Vec<(u32, i64)>,
    /// Granularity of the interval lane's per-block `vt_e` running maximum.
    /// Defaults to `block_rows` so it need not be set explicitly;
    /// only consulted when the writer actually builds the structure
    /// (interval lane, explicit `vt_e`).
    pub vt_e_block_rows: u32,
    /// May this segment code `tt_s` by dictionary when its run density
    /// crosses [`tt_s_wants_dictionary`]? Default `true`.
    ///
    /// `false` writes the run list at every density, which is byte for byte
    /// what the writer always did before this field existed. It is the caller's revert lever for
    /// that change, and it is what lets a test produce a genuine old-format
    /// segment to read back. It changes only what is *written*: both shapes
    /// read through the same view, so a store written either way is read by
    /// either build.
    pub tt_s_dictionary: bool,
}

impl Default for SegmentSpec {
    fn default() -> Self {
        Self {
            lane: Lane::Event,
            block_rows: crate::defaults::BLOCK_ROWS,
            closed_rows: Vec::new(),
            vt_e_block_rows: crate::defaults::BLOCK_ROWS,
            tt_s_dictionary: true,
        }
    }
}

/// The interval lane's per-block `vt_e` running maximum, plus the
/// open-ended rows it deliberately leaves out.
///
/// Sampled at block boundaries in the write loop — one extra push per block,
/// not a second pass over the rows.
///
/// **Closed rows only.** A running maximum is a prefix aggregate, so one
/// open-ended `vt_e` raises every later entry and ends the structure's
/// usefulness for the rest of the segment: measured at one open-ended row in
/// ten thousand taking the pruned fraction from 0.507 to 1.0000. The
/// open-ended rows go into their own sorted range list instead, where they
/// need no `vt_e` test at all — an open-ended row overlaps every window its
/// `vt_s` reaches.
struct VtERunningMax {
    block_rows: usize,
    /// max over closed rows in blocks `0..=i`; `i64::MIN` while no closed
    /// row has been seen, which reads as "nothing here bounds a window" and
    /// is exactly what a prefix of purely open-ended rows should contribute
    closed_max: i64,
    per_block: Vec<i64>,
    /// half-open ranges of rows with `vt_e == OPEN_END`, coalesced as they
    /// are appended
    open_rows: Vec<(u32, u32)>,
}

impl VtERunningMax {
    /// `None` when this segment carries no such structure at all (event
    /// lane, or an elided `vt_e` column) — the empty `Vec` that means "no
    /// structure" to every reader.
    fn new(lane: Lane, vt_e_elided: bool, vt_e_block_rows: u32) -> Option<Self> {
        if lane != Lane::Interval || vt_e_elided {
            return None;
        }
        let block_rows = if vt_e_block_rows > 0 {
            vt_e_block_rows as usize
        } else {
            crate::defaults::BLOCK_ROWS as usize
        };
        Some(Self {
            block_rows,
            closed_max: i64::MIN,
            per_block: Vec::new(),
            open_rows: Vec::new(),
        })
    }

    /// Called once per row, in row order, with that row's own `vt_e`.
    fn observe(&mut self, i: usize, vt_e: i64) {
        if vt_e == crate::OPEN_END {
            match self.open_rows.last_mut() {
                Some(last) if last.1 == i as u32 => last.1 = i as u32 + 1,
                _ => self.open_rows.push((i as u32, i as u32 + 1)),
            }
        } else {
            self.closed_max = self.closed_max.max(vt_e);
        }
        if (i + 1).is_multiple_of(self.block_rows) {
            self.per_block.push(self.closed_max);
        }
    }

    /// `n` is the segment's row count; a final partial block still gets an
    /// entry so the vector's length is `ceil(n / block_rows)` exactly.
    fn finish(mut self, n: usize) -> (Vec<i64>, Vec<(u32, u32)>) {
        if !n.is_multiple_of(self.block_rows) {
            self.per_block.push(self.closed_max);
        }
        debug_assert_eq!(self.per_block.len(), n.div_ceil(self.block_rows));
        debug_assert!(
            self.per_block.windows(2).all(|w| w[0] <= w[1]),
            "a running maximum is non-decreasing by construction"
        );
        debug_assert!(
            self.open_rows.windows(2).all(|w| w[0].1 < w[1].0),
            "open-row ranges must ascend and stay disjoint"
        );
        (self.per_block, self.open_rows)
    }
}

/// Write `rows` (already sorted by `(vt_s, vid)`) as one segment file.
///
/// Returns the segment's content address — the footer's `body_sha`, the
/// value its manifest entry carries (`SegmentEntry::sha`).
pub fn write_edge_segment(path: &Path, rows: &[EdgeRow], spec: &SegmentSpec) -> Result<String> {
    if rows.is_empty() {
        return Err(EngineError::invariant("refusing to write an empty segment"));
    }
    debug_assert!(
        rows.windows(2).all(|w| w[0].sort_key() <= w[1].sort_key()),
        "rows must be sorted by (vt_s, vid) before writing"
    );
    let fill_t = Timer::start();

    let mut strings = StringTable::new();
    let mut rel_types: Vec<String> = Vec::new();
    let n = rows.len();

    let vt_e_elided = rows.iter().all(EdgeRow::is_instantaneous);
    let mut c_vt_s = ColumnBuilder::new("vt_s", DType::I64, n);
    let mut c_src = ColumnBuilder::new("src_id", DType::U32, n);
    let mut c_dst = ColumnBuilder::new("dst_id", DType::U32, n);
    let mut c_rel = ColumnBuilder::new("rel_code", DType::U16, n);
    let mut c_vid_hi = ColumnBuilder::new("vid64", DType::U64, n);
    let mut c_vid_lo = ColumnBuilder::new("vid_lo32", DType::U32, n);
    let mut c_vt_e = ColumnBuilder::new("vt_e", DType::I64, if vt_e_elided { 0 } else { n });
    let mut c_props = ColumnBuilder::new("props_ref", DType::U32, n);
    let mut c_disc = ColumnBuilder::new("disc_ref", DType::U32, n);
    let mut c_source = ColumnBuilder::new("source_ref", DType::U32, n);
    let mut c_prov = ColumnBuilder::new("prov_ref", DType::U32, n);

    let mut tt_s_runs: Vec<(u32, i64)> = Vec::new();
    let (mut vt_min, mut vt_max, mut vt_e_max) = (i64::MAX, i64::MIN, i64::MIN);
    let mut vt_e_max_blocks =
        VtERunningMax::new(spec.lane, vt_e_elided, spec.vt_e_block_rows);

    for (i, r) in rows.iter().enumerate() {
        let rel_code = match rel_types.iter().position(|t| t == &r.rel_type) {
            Some(p) => p as u16,
            None => {
                if rel_types.len() == u16::MAX as usize {
                    return Err(EngineError::capacity(
                        "a segment cannot hold more than 65535 distinct rel_types",
                    ));
                }
                rel_types.push(r.rel_type.clone());
                (rel_types.len() - 1) as u16
            }
        };
        c_vt_s.push_i64(r.vt_s);
        c_src.push_u32(r.src_id);
        c_dst.push_u32(r.dst_id);
        c_rel.push_u16(rel_code);
        c_vid_hi.push_u64(r.vid.hi);
        c_vid_lo.push_u32(r.vid.lo);
        if !vt_e_elided {
            c_vt_e.push_i64(r.vt_e);
        }
        c_props.push_u32(strings.intern(&r.props));
        c_disc.push_u32(strings.intern(&r.disc));
        c_source.push_u32(strings.intern(&r.source));
        c_prov.push_u32(strings.intern_opt(r.provenance_ref.as_deref()));

        if tt_s_runs.last().map(|(_, tt)| *tt) != Some(r.tt_s) {
            tt_s_runs.push((i as u32, r.tt_s));
        }
        vt_min = vt_min.min(r.vt_s);
        vt_max = vt_max.max(r.vt_s);
        vt_e_max = vt_e_max.max(r.vt_e);
        if let Some(s) = vt_e_max_blocks.as_mut() {
            s.observe(i, r.vt_e);
        }
    }
    let (block_vt_e_running_max, open_rows) =
        vt_e_max_blocks.map(|s| s.finish(n)).unwrap_or_default();

    let tt_s = encode_tt_s(tt_s_runs, n, spec.tt_s_dictionary);

    let mut columns = vec![c_vt_s, c_src, c_dst, c_rel, c_vid_hi, c_vid_lo];
    if !vt_e_elided {
        columns.push(c_vt_e);
    }
    columns.extend([c_props, c_disc, c_source, c_prov]);
    columns.extend(tt_s.column);
    fill_t.stop_ctx(S::ColumnFill);

    let first = &rows[0];
    let last = &rows[n - 1];
    write_segment(
        path,
        SegmentHeader {
            format: FORMAT_VERSION,
            kind: RowKind::Edge,
            lane: spec.lane,
            rows: n as u32,
            block_rows: spec.block_rows,
            key_lo: (first.vt_s, first.vid.to_hex()),
            key_hi: (last.vt_s, last.vid.to_hex()),
            vt_min,
            vt_max,
            vt_e_max,
            tt_s_runs: tt_s.runs,
            tt_s_dict: tt_s.dict,
            tt_s_code_blocks: tt_s.blocks,
            tt_s_block_rows: tt_s.block_rows,
            rel_types,
            columns: Vec::new(), // filled by write_segment
            vt_e_elided,
            strings_offset: 0,
            strings_bytes: 0,
            strings_count: strings.len(),
            closed_rows: spec.closed_rows.clone(),
            block_vt_e_running_max,
            open_rows,
            vt_e_block_rows: spec.vt_e_block_rows,
        },
        columns,
        &strings,
    )
}

/// Write node versions. Nodes are identity-clustered rather than
/// vt-partitioned, but share the segment container. Returns the
/// footer's `body_sha`, as [`write_edge_segment`] does.
pub fn write_node_segment(path: &Path, rows: &[NodeRow], spec: &SegmentSpec) -> Result<String> {
    if rows.is_empty() {
        return Err(EngineError::invariant("refusing to write an empty segment"));
    }
    let fill_t = Timer::start();
    let mut strings = StringTable::new();
    let n = rows.len();
    let vt_e_elided = rows.iter().all(NodeRow::is_instantaneous);

    let mut c_vt_s = ColumnBuilder::new("vt_s", DType::I64, n);
    let mut c_uid = ColumnBuilder::new("uid_id", DType::U32, n);
    let mut c_vid_hi = ColumnBuilder::new("vid64", DType::U64, n);
    let mut c_vid_lo = ColumnBuilder::new("vid_lo32", DType::U32, n);
    let mut c_vt_e = ColumnBuilder::new("vt_e", DType::I64, if vt_e_elided { 0 } else { n });
    let mut c_label = ColumnBuilder::new("label_ref", DType::U32, n);
    let mut c_props = ColumnBuilder::new("props_ref", DType::U32, n);
    // `name` promoted out of the props blob into its own column, so entity
    // resolution can scan it without parsing JSON per row. It is duplicated,
    // never moved: `props` is returned byte-identical because the store
    // digest is computed over that exact text.
    let mut c_name = ColumnBuilder::new("name_ref", DType::U32, n);
    let mut c_source = ColumnBuilder::new("source_ref", DType::U32, n);
    let mut c_prov = ColumnBuilder::new("prov_ref", DType::U32, n);

    let mut tt_s_runs: Vec<(u32, i64)> = Vec::new();
    let (mut vt_min, mut vt_max, mut vt_e_max) = (i64::MAX, i64::MIN, i64::MIN);
    let mut vt_e_max_blocks =
        VtERunningMax::new(spec.lane, vt_e_elided, spec.vt_e_block_rows);

    for (i, r) in rows.iter().enumerate() {
        c_vt_s.push_i64(r.vt_s);
        c_uid.push_u32(r.uid_id);
        c_vid_hi.push_u64(r.vid.hi);
        c_vid_lo.push_u32(r.vid.lo);
        if !vt_e_elided {
            c_vt_e.push_i64(r.vt_e);
        }
        c_label.push_u32(strings.intern(&r.label));
        c_props.push_u32(strings.intern(&r.props));
        c_name.push_u32(match name_of(&r.props) {
            Some(v) => strings.intern(&v),
            None => NULL_REF,
        });
        c_source.push_u32(strings.intern(&r.source));
        c_prov.push_u32(strings.intern_opt(r.provenance_ref.as_deref()));

        if tt_s_runs.last().map(|(_, tt)| *tt) != Some(r.tt_s) {
            tt_s_runs.push((i as u32, r.tt_s));
        }
        vt_min = vt_min.min(r.vt_s);
        vt_max = vt_max.max(r.vt_s);
        vt_e_max = vt_e_max.max(r.vt_e);
        if let Some(s) = vt_e_max_blocks.as_mut() {
            s.observe(i, r.vt_e);
        }
    }
    let (block_vt_e_running_max, open_rows) =
        vt_e_max_blocks.map(|s| s.finish(n)).unwrap_or_default();

    let tt_s = encode_tt_s(tt_s_runs, n, spec.tt_s_dictionary);

    let mut columns = vec![c_vt_s, c_uid, c_vid_hi, c_vid_lo];
    if !vt_e_elided {
        columns.push(c_vt_e);
    }
    columns.extend([c_label, c_props, c_name, c_source, c_prov]);
    columns.extend(tt_s.column);
    fill_t.stop_ctx(S::ColumnFill);

    let first = &rows[0];
    let last = &rows[n - 1];
    write_segment(
        path,
        SegmentHeader {
            format: FORMAT_VERSION,
            kind: RowKind::Node,
            lane: spec.lane,
            rows: n as u32,
            block_rows: spec.block_rows,
            key_lo: (first.vt_s, first.vid.to_hex()),
            key_hi: (last.vt_s, last.vid.to_hex()),
            vt_min,
            vt_max,
            vt_e_max,
            tt_s_runs: tt_s.runs,
            tt_s_dict: tt_s.dict,
            tt_s_code_blocks: tt_s.blocks,
            tt_s_block_rows: tt_s.block_rows,
            rel_types: Vec::new(),
            columns: Vec::new(),
            vt_e_elided,
            strings_offset: 0,
            strings_bytes: 0,
            strings_count: strings.len(),
            closed_rows: spec.closed_rows.clone(),
            block_vt_e_running_max,
            open_rows,
            vt_e_block_rows: spec.vt_e_block_rows,
        },
        columns,
        &strings,
    )
}

/// Reinterpret a raw little-endian extent as logical values for encoding.
fn extent_values(dtype: DType, bytes: &[u8]) -> Vec<i64> {
    let mut out = Vec::with_capacity(bytes.len() / dtype.size());
    match dtype {
        DType::I64 => {
            for c in bytes.as_chunks::<8>().0 {
                out.push(i64::from_le_bytes(*c));
            }
        }
        DType::U64 => {
            for c in bytes.as_chunks::<8>().0 {
                out.push(u64::from_le_bytes(*c) as i64);
            }
        }
        DType::U32 => {
            for c in bytes.as_chunks::<4>().0 {
                out.push(u32::from_le_bytes(*c) as i64);
            }
        }
        DType::U16 => {
            for c in bytes.as_chunks::<2>().0 {
                out.push(u16::from_le_bytes(*c) as i64);
            }
        }
    }
    out
}

/// One column's chosen representation.
struct Stored {
    bytes: Vec<u8>,
    codec: u32,
    base: i64,
    divisor: i64,
}

/// Trial-encode one column and keep the smallest representation.
///
/// Five candidates: `{frame-of-reference, linear model} x {exact divisor,
/// none}`, then raw little-endian. The two transforms are **not**
/// composable in the sense that taking the divisor out always helps the
/// model — on one 10M segment the divisor made the model worse by two orders
/// of magnitude, because dividing changes the grid the fixed-point slope has
/// to land on and a line that was exact on the original values is no longer
/// exact on the quotients. Both are exact; only the fit differs. So all four
/// are measured.
///
/// The scale is tried on every column rather than on a list of "timestamp"
/// columns: it costs one greatest-common-divisor pass that exits at the
/// second first-difference on anything without a common factor, and a rule
/// naming the timestamp columns is a rule that goes stale.
///
/// **The tie-break is fixed, in the order the candidates are listed**, so
/// the choice is a function of the rows and nothing else — two encodes of
/// one column on two hosts cannot pick differently and so cannot produce
/// different bytes. Simpler representations come first: a reader
/// meeting a tie is better off with the codec that needs no model.
fn encode_column(c: &ColumnBuilder) -> Stored {
    // The same five candidates, the same order and the same
    // strictly-smaller rule — so the same choice and the same bytes — but
    // compared by exact encoded length (`codec::encoded_len_i64`,
    // `linfor::encoded_len_prepared`) and only the winner materialised; the
    // exact scale and the divided values are computed once and shared by
    // the two arms that use them. Before, all four compressed candidates
    // were packed in full and the scale was found up to three times.
    //
    // The arm timers measure each candidate's length computation; packing
    // the winner (or copying raw) is `EncSelectNs`.
    #[derive(Clone, Copy)]
    enum Arm {
        ForScaled,
        For,
        LinScaled,
        Lin,
    }
    ingest_phase::add_ctx(S::EncColumns, 1);
    let t = Timer::start();
    let vals = extent_values(c.dtype, &c.bytes);
    t.stop_ctx(S::EncExtentNs);
    let t = Timer::start();
    let scale = crate::codec::exact_scale(&vals);
    t.stop_ctx(S::EncScaleNs);

    let mut best: Option<(usize, Arm)> = None;
    let mut offer = |len: usize, arm: Arm| {
        // strictly smaller replaces, so a tie is won by whichever candidate
        // was offered first — the listed order, preserved
        if best.is_none_or(|(b, _)| len < b) {
            best = Some((len, arm));
        }
    };

    // 1. frame-of-reference over divided values
    let t = Timer::start();
    let scaled = scale.map(|(base, divisor)| crate::codec::scale_down(&vals, base, divisor));
    if let Some(x) = &scaled {
        offer(crate::codec::encoded_len_i64(x), Arm::ForScaled);
    }
    t.stop_ctx(S::EncForScaledNs);
    // 2. frame-of-reference, undivided
    let t = Timer::start();
    offer(crate::codec::encoded_len_i64(&vals), Arm::For);
    t.stop_ctx(S::EncForNs);
    // 3. the model with the divisor taken out — on a column without one,
    // exactly the undivided model (same stream), which ties and wins as 3
    let t = Timer::start();
    let x3: &[i64] = scaled.as_deref().unwrap_or(&vals);
    if let Some(len) = crate::linfor::encoded_len_prepared(&vals, x3) {
        offer(len, Arm::LinScaled);
    }
    t.stop_ctx(S::EncLinScaledNs);
    // 4. the model, undivided
    let t = Timer::start();
    if scale.is_some() {
        if let Some(len) = crate::linfor::encoded_len_prepared(&vals, &vals) {
            offer(len, Arm::Lin);
        }
    }
    t.stop_ctx(S::EncLinNs);

    let t = Timer::start();
    let (best_len, arm) = best.expect("candidate 2 is unconditional");
    // 5. raw, which also wins every tie against a compression that did not
    // earn its 8 bytes of stream header
    let out = if best_len + 8 < c.bytes.len() {
        let (base, divisor) = scale.unwrap_or((0, 1));
        let (stored, win) = match arm {
            Arm::ForScaled => (
                Stored {
                    bytes: crate::codec::encode_i64(scaled.as_deref().expect("arm 1 has a scale")),
                    codec: crate::codec::CODEC_FOR,
                    base,
                    divisor,
                },
                S::EncWinForScaled,
            ),
            Arm::For => (
                Stored {
                    bytes: crate::codec::encode_i64(&vals),
                    codec: crate::codec::CODEC_FOR,
                    base: 0,
                    divisor: 1,
                },
                S::EncWinFor,
            ),
            Arm::LinScaled => (
                Stored {
                    bytes: crate::linfor::encode_prepared(&vals, base, divisor, x3)
                        .expect("its length was computed"),
                    codec: crate::codec::CODEC_LINMODEL,
                    base: 0,
                    divisor: 1,
                },
                S::EncWinLinScaled,
            ),
            Arm::Lin => (
                Stored {
                    bytes: crate::linfor::encode_prepared(&vals, 0, 1, &vals)
                        .expect("its length was computed"),
                    codec: crate::codec::CODEC_LINMODEL,
                    base: 0,
                    divisor: 1,
                },
                S::EncWinLin,
            ),
        };
        debug_assert_eq!(stored.bytes.len(), best_len, "size-only length disagrees");
        ingest_phase::add_ctx(win, 1);
        stored
    } else {
        ingest_phase::add_ctx(S::EncWinRaw, 1);
        Stored {
            bytes: c.bytes.clone(),
            codec: crate::codec::CODEC_RAW,
            base: 0,
            divisor: 1,
        }
    };
    t.stop_ctx(S::EncSelectNs);
    out
}

/// `encode_column` as it was originally — every candidate packed in full
/// — kept for the tests that hold the new one to it.
#[cfg(test)]
fn encode_column_reference(c: &ColumnBuilder) -> Stored {
    // Each arm below is timed into its own slot of the caller's
    // context (seal or compaction), and the winner counted. Observation
    // only: nothing here changes which candidate is kept.
    ingest_phase::add_ctx(S::EncColumns, 1);
    let t = Timer::start();
    let vals = extent_values(c.dtype, &c.bytes);
    t.stop_ctx(S::EncExtentNs);
    let t = Timer::start();
    let scale = crate::codec::exact_scale(&vals);
    t.stop_ctx(S::EncScaleNs);
    let mut candidates: Vec<(Stored, S)> = Vec::with_capacity(4);

    // 1. frame-of-reference over divided values
    if let Some((base, divisor)) = scale {
        let t = Timer::start();
        let scaled = crate::codec::scale_down(&vals, base, divisor);
        candidates.push((
            Stored {
                bytes: crate::codec::encode_i64(&scaled),
                codec: crate::codec::CODEC_FOR,
                base,
                divisor,
            },
            S::EncWinForScaled,
        ));
        t.stop_ctx(S::EncForScaledNs);
    }
    // 2. frame-of-reference, undivided
    let t = Timer::start();
    candidates.push((
        Stored {
            bytes: crate::codec::encode_i64(&vals),
            codec: crate::codec::CODEC_FOR,
            base: 0,
            divisor: 1,
        },
        S::EncWinFor,
    ));
    t.stop_ctx(S::EncForNs);
    // 3, 4. the model, which carries its own scale in its stream: a line has
    // to be fitted to whichever values it is going to predict.
    for (with_scale, slot, win) in [
        (true, S::EncLinScaledNs, S::EncWinLinScaled),
        (false, S::EncLinNs, S::EncWinLin),
    ] {
        let t = Timer::start();
        if let Some(bytes) = crate::linfor::encode(&vals, with_scale) {
            candidates.push((
                Stored {
                    bytes,
                    codec: crate::codec::CODEC_LINMODEL,
                    base: 0,
                    divisor: 1,
                },
                win,
            ));
        }
        t.stop_ctx(slot);
    }

    let t = Timer::start();
    // strictly smaller replaces, so a tie is won by whichever candidate was
    // pushed first — the listed order above, preserved
    let mut it = candidates.into_iter();
    let mut best = it.next().expect("candidate 2 is unconditional");
    for cand in it {
        if cand.0.bytes.len() < best.0.bytes.len() {
            best = cand;
        }
    }
    // 5. raw, which also wins every tie against a compression that did not
    // earn its 8 bytes of stream header
    let out = if best.0.bytes.len() + 8 < c.bytes.len() {
        ingest_phase::add_ctx(best.1, 1);
        best.0
    } else {
        ingest_phase::add_ctx(S::EncWinRaw, 1);
        Stored {
            bytes: c.bytes.clone(),
            codec: crate::codec::CODEC_RAW,
            base: 0,
            divisor: 1,
        }
    };
    t.stop_ctx(S::EncSelectNs);
    out
}

fn write_segment(
    path: &Path,
    mut header: SegmentHeader,
    columns: Vec<ColumnBuilder>,
    strings: &StringTable,
) -> Result<String> {
    // Compression is a measurement, not a policy: every column is trial-
    // encoded and the smaller representation wins. The vid halves come out
    // raw every time (sha256 prefixes have no structure to pack), the
    // timestamp and ref columns collapse — and nothing here needs to know
    // which column is which for that to hold.
    let encode_t = Timer::start();
    // Compression is a measurement, not a policy: every column is trial-
    // encoded and the smaller representation wins. The exact scale divisor
    // adds one more arm to the same trial rather than a rule about which
    // columns are timestamps — it is free to try on anything, it reports
    // `None` after two first differences on a column that has no common
    // factor, and the arm only wins where it is smaller. Nothing here needs
    // to know which column is which for that to hold.
    let stored: Vec<Stored> = columns.iter().map(encode_column).collect();

    // Offsets are relative to data_start, so they do not depend on how long
    // the header serializes to — which would otherwise be circular.
    let mut descs = Vec::with_capacity(columns.len());
    let mut cursor = 0u64;
    for (c, st) in columns.iter().zip(&stored) {
        descs.push(ColumnDesc {
            name: c.name.to_string(),
            dtype: c.dtype,
            offset: cursor,
            bytes: st.bytes.len() as u64,
            codec: st.codec,
            base: st.base,
            divisor: st.divisor,
        });
        cursor += align_up(st.bytes.len()) as u64;
    }
    encode_t.stop_ctx(S::Encode);
    let heap_t = Timer::start();
    let encoded_strings = {
        let raw = strings.encode();
        match crate::codec::pack_heap(&raw) {
            Some(packed) if packed.len() + 8 < raw.len() => packed,
            _ => raw,
        }
    };
    heap_t.stop_ctx(S::StringHeap);
    let header_t = Timer::start();
    header.columns = descs;
    header.strings_offset = cursor;
    header.strings_bytes = encoded_strings.len() as u64;

    let header_json = serde_json::to_vec(&header).expect("segment header is serializable");
    let data_start = align_up(PREAMBLE + header_json.len());

    let mut buf = Vec::with_capacity(data_start + cursor as usize + encoded_strings.len() + 512);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&(header_json.len() as u32).to_le_bytes());
    buf.extend_from_slice(&header_json);
    pad_to(&mut buf, data_start);

    let mut extent_crcs = Vec::with_capacity(stored.len());
    for st in &stored {
        extent_crcs.push(crc32c::crc32c(&st.bytes));
        buf.extend_from_slice(&st.bytes);
        let padded = align_up(buf.len());
        pad_to(&mut buf, padded);
    }
    let strings_crc = crc32c::crc32c(&encoded_strings);
    buf.extend_from_slice(&encoded_strings);

    let body_sha = sha256_hex_bytes(&buf)[..SHA_HEX_LEN].to_string();
    let footer = SegmentFooter {
        extent_crcs,
        strings_crc,
        rows: header.rows as u64,
        body_sha: body_sha.clone(),
    };
    let footer_json = serde_json::to_vec(&footer).expect("segment footer is serializable");
    buf.extend_from_slice(&footer_json);
    buf.extend_from_slice(&(footer_json.len() as u32).to_le_bytes());
    buf.extend_from_slice(FOOTER_MAGIC);
    header_t.stop_ctx(S::HeaderChecksum);

    let write_t = Timer::start();
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|e| EngineError::from(e).at_file(dir))?;
    }
    let tmp = path.with_extension("tgs.tmp");
    {
        let mut f = File::create(&tmp).map_err(|e| EngineError::from(e).at_file(&tmp))?;
        f.write_all(&buf)
            .map_err(|e| EngineError::from(e).at_file(&tmp))?;
        write_t.stop_ctx(S::FileWrite);
        let sync_t = Timer::start();
        f.sync_all()
            .map_err(|e| EngineError::from(e).at_file(&tmp))?;
        sync_t.stop_ctx(S::Fsync);
    }
    let rename_t = Timer::start();
    fs::rename(&tmp, path).map_err(|e| EngineError::from(e).at_file(path))?;
    rename_t.stop_ctx(S::Rename);
    ingest_phase::add_ctx(S::Segments, 1);
    ingest_phase::add_ctx(S::RowsWritten, header.rows as u64);
    Ok(body_sha)
}

/// The `name` property, if the row has one as a JSON string.
///
/// Parsed once at seal time rather than on every lookup. Anything that is not
/// a string (absent, null, a number) simply has no name — resolution matches
/// on text, so there is nothing to index.
pub(crate) fn name_of(props: &str) -> Option<String> {
    if !props.contains("\"name\"") {
        return None; // the overwhelmingly common case: skip the parse
    }
    serde_json::from_str::<serde_json::Value>(props)
        .ok()?
        .get("name")?
        .as_str()
        .map(str::to_string)
}

fn align_up(n: usize) -> usize {
    n.div_ceil(ALIGN) * ALIGN
}

fn pad_to(buf: &mut Vec<u8>, target: usize) {
    buf.resize(target, 0);
}

// --------------------------------------------------------------------- //
// reading                                                               //
// --------------------------------------------------------------------- //

/// How a segment's bytes are obtained. Kept swappable because `mmap` is a
/// *policy*, not a format assumption: it is usually the right
/// choice for immutable local files and can be the wrong one on network
/// storage, so both must stay available and measurable.
pub trait SegmentSource: Send + Sync {
    fn bytes(&self) -> &[u8];
}

pub struct MemorySource(Vec<u8>);

impl MemorySource {
    pub fn load(path: &Path) -> Result<Self> {
        Ok(Self(
            fs::read(path).map_err(|e| EngineError::from(e).at_file(path))?,
        ))
    }
}

impl SegmentSource for MemorySource {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Backed by `Arc<Mmap>` rather than a bare `Mmap` so one mapping can be
/// shared between the store's pinned-at-open table and every
/// `Segment` opened from it — cloning a source is then an `Arc` bump, not a
/// fresh `mmap(2)` call. `load` still does exactly one `File::open` +
/// `Mmap::map` per call; the `File` is dropped immediately after, as before.
pub struct MmapSource(std::sync::Arc<memmap2::Mmap>);

impl Clone for MmapSource {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl MmapSource {
    pub fn load(path: &Path) -> Result<Self> {
        let f = File::open(path).map_err(|e| EngineError::from(e).at_file(path))?;
        // SAFETY: segments are immutable once published — nothing in this
        // process or a well-behaved operator ever rewrites a sealed segment,
        // so the mapping cannot observe a concurrent modification.
        let map = unsafe { memmap2::Mmap::map(&f) }
            .map_err(|e| EngineError::from(e).at_file(path))?;
        Ok(Self(std::sync::Arc::new(map)))
    }
}

impl SegmentSource for MmapSource {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

pub struct Segment<S: SegmentSource> {
    source: S,
    header: SegmentHeader,
    data_start: usize,
    path: PathBuf,
    /// Compressed columns, by column index — see [`Decoded`]. Backed by
    /// `Vec<u64>` so any dtype's alignment holds; the usize is the logical
    /// byte length. Raw columns stay zero-copy out of the source.
    decoded: Vec<Decoded>,
    /// The string heap in raw layout, when the file stores it packed.
    decoded_strings: Option<Vec<u8>>,
    /// `header.tt_s_bounds()`, folded once here. Every scan asks whether this
    /// segment is believed at some `as_of`, and a compacted segment's run
    /// list is long enough (millions) that answering it by walking the runs
    /// is the scan.
    tt_s_min: i64,
    tt_s_max: i64,
}

/// How one column's bytes are served.
///
/// The segment cache accounts the *whole* footprint of an open segment, and
/// Decoded columns were measured as the dominant memory term at 1B rows. A
/// codec-2 column is the one shape that can answer a scan's window cut and a
/// range count without ever being materialised — it carries a sparse index
/// over its own blocks — so it is the one shape worth holding back.
enum Decoded {
    /// Codec 0: served zero-copy out of the mapping.
    Raw,
    /// Materialized at open, as every compressed column always has been.
    Eager((Vec<u64>, usize)),
    /// Codec 2, held back until something asks for a slice. Its structure
    /// was still validated at open — `LinColumn::parse` walks every block
    /// header and extent — so holding back the values does not hold back the
    /// bounds checking that decode doubles as.
    Lazy(std::sync::OnceLock<(Vec<u64>, usize)>),
}

impl Decoded {
    fn materialized(&self) -> Option<&(Vec<u64>, usize)> {
        match self {
            Decoded::Raw => None,
            Decoded::Eager(v) => Some(v),
            Decoded::Lazy(cell) => cell.get(),
        }
    }
}

/// Hold codec-2 columns back until a slice is asked for?
///
/// Off by default: it moves what the segment cache accounts at insert time,
/// because a column materialized after insertion is not in that snapshot. It
/// is switchable so the decoded-bytes claim can be measured as an A/B on one
/// build rather than argued between two.
pub(crate) fn linfor_lazy() -> bool {
    static LAZY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *LAZY.get_or_init(|| {
        std::env::var("GNOMON_LINFOR_LAZY").map(|v| v == "1").unwrap_or(false)
    })
}

impl<S: SegmentSource> Segment<S> {
    /// Parse and validate. `verify_checksums` walks every extent; skip it on
    /// the hot path once a store has been verified, but never skip the
    /// structural checks — a truncated file must not reach a query.
    pub fn open(path: impl Into<PathBuf>, source: S, verify_checksums: bool) -> Result<Self> {
        let path = path.into();
        let bytes = source.bytes();
        let at = |e: EngineError| e.at_file(&path);

        if bytes.len() < PREAMBLE + 8 || &bytes[..4] != MAGIC {
            return Err(at(EngineError::corrupt("not a Gnomon segment (bad magic)")));
        }
        if &bytes[bytes.len() - 4..] != FOOTER_MAGIC {
            return Err(at(EngineError::corrupt(
                "segment has no completion marker — it was never finished being written",
            )));
        }
        let format = read_u32(bytes, 4).map_err(at)?;
        if format != FORMAT_VERSION {
            return Err(at(EngineError::corrupt(format!(
                "segment format {format} is not supported (expected {FORMAT_VERSION})"
            ))));
        }
        let header_len = read_u32(bytes, 8).map_err(at)? as usize;
        let header_end = PREAMBLE + header_len;
        let header_bytes = bytes
            .get(PREAMBLE..header_end)
            .ok_or_else(|| at(EngineError::corrupt("segment header is truncated")))?;
        let header: SegmentHeader = serde_json::from_slice(header_bytes)
            .map_err(|e| at(EngineError::corrupt(format!("segment header is invalid: {e}"))))?;
        let data_start = align_up(header_end);

        let footer_len =
            read_u32(bytes, bytes.len() - 8).map_err(at)? as usize;
        let footer_end = bytes.len() - 8;
        let footer_start = footer_end
            .checked_sub(footer_len)
            .ok_or_else(|| at(EngineError::corrupt("segment footer length is impossible")))?;
        let footer: SegmentFooter = serde_json::from_slice(&bytes[footer_start..footer_end])
            .map_err(|e| at(EngineError::corrupt(format!("segment footer is invalid: {e}"))))?;

        if footer.rows != header.rows as u64 {
            return Err(at(EngineError::corrupt(format!(
                "row count disagrees: header {} vs footer {}",
                header.rows, footer.rows
            ))));
        }
        if footer.extent_crcs.len() != header.columns.len() {
            return Err(at(EngineError::corrupt(
                "footer checksum count does not match the column count",
            )));
        }

        let (tt_s_min, tt_s_max) = header.tt_s_bounds();
        let mut seg = Self {
            source,
            header,
            data_start,
            path,
            decoded: Vec::new(),
            decoded_strings: None,
            tt_s_min,
            tt_s_max,
        };
        if verify_checksums {
            seg.verify(&footer, footer_start)?;
        }
        // Materialize compressed columns exactly once. Decode is fully
        // bounds-checked, so on the fast path (checksums already verified
        // this session) it doubles as structural validation of the extent.
        let mut decoded = Vec::with_capacity(seg.header.columns.len());
        for idx in 0..seg.header.columns.len() {
            let desc = &seg.header.columns[idx];
            if desc.codec == crate::codec::CODEC_RAW {
                decoded.push(Decoded::Raw);
                continue;
            }
            if desc.codec != crate::codec::CODEC_FOR
                && desc.codec != crate::codec::CODEC_LINMODEL
            {
                return Err(EngineError::corrupt(format!(
                    "column '{}' uses codec {} which this build cannot decode",
                    desc.name, desc.codec
                ))
                .at_file(&seg.path));
            }
            if desc.codec == crate::codec::CODEC_LINMODEL && linfor_lazy() {
                // structure now, values only if asked: `parse` walks every
                // block header and every payload extent, which is the
                // validation that materializing used to provide.
                crate::linfor::LinColumn::parse(seg.raw(desc)?, seg.header.rows as usize)
                    .map_err(|e| e.at_file(&seg.path))?;
                decoded.push(Decoded::Lazy(std::sync::OnceLock::new()));
                continue;
            }
            decoded.push(Decoded::Eager(seg.materialize(desc)?));
        }
        seg.decoded = decoded;
        // Same once-per-open treatment for a packed string heap. Unpacking is
        // fully bounds-checked, so it too doubles as structural validation.
        let raw_strings = seg.raw_strings()?;
        if crate::codec::heap_is_packed(raw_strings) {
            seg.decoded_strings =
                Some(crate::codec::unpack_heap(raw_strings).map_err(|e| e.at_file(&seg.path))?);
        }
        Ok(seg)
    }

    fn verify(&self, footer: &SegmentFooter, footer_start: usize) -> Result<()> {
        let bytes = self.source.bytes();
        let at = |e: EngineError| e.at_file(&self.path);
        let body_sha = sha256_hex_bytes(&bytes[..footer_start])[..SHA_HEX_LEN].to_string();
        if body_sha != footer.body_sha {
            return Err(at(EngineError::corrupt(format!(
                "segment body checksum mismatch: computed {body_sha}, recorded {}",
                footer.body_sha
            ))));
        }
        for (desc, &expect) in self.header.columns.iter().zip(&footer.extent_crcs) {
            let got = crc32c::crc32c(self.raw(desc)?);
            if got != expect {
                return Err(at(EngineError::corrupt(format!(
                    "column '{}' failed its checksum (computed {got:#x}, recorded {expect:#x})",
                    desc.name
                ))));
            }
        }
        let got = crc32c::crc32c(self.raw_strings()?);
        if got != footer.strings_crc {
            return Err(at(EngineError::corrupt(
                "segment string table failed its checksum",
            )));
        }
        Ok(())
    }

    /// Decode one column's extent into its own little-endian layout.
    ///
    /// Decode is fully bounds-checked, so on the fast path (checksums
    /// already verified this session) it doubles as structural validation of
    /// the extent.
    fn materialize(&self, desc: &ColumnDesc) -> Result<(Vec<u64>, usize)> {
        let n = self.header.rows as usize;
        let mut vals = if desc.codec == crate::codec::CODEC_LINMODEL {
            crate::linfor::decode_i64(self.raw(desc)?, n)
        } else {
            crate::codec::decode_i64(self.raw(desc)?, n)
        }
        .map_err(|e| e.at_file(&self.path))?;
        // the exact scale, undone: `v = base + divisor * k`. `(0, 1)` is the
        // identity every column with no scale carries and returns without
        // touching a value.
        crate::codec::scale_up(&mut vals, desc.base, desc.divisor)
            .map_err(|e| e.at_file(&self.path))?;
        let size = desc.dtype.size();
        let mut backing = vec![0u64; (n * size).div_ceil(8)];
        {
            // fill through a byte view so every dtype lands in its own
            // little-endian layout, same as the raw extent would hold
            let out: &mut [u8] = unsafe {
                std::slice::from_raw_parts_mut(
                    backing.as_mut_ptr() as *mut u8,
                    backing.len() * 8,
                )
            };
            for (i, &v) in vals.iter().enumerate() {
                let b = &mut out[i * size..(i + 1) * size];
                match desc.dtype {
                    DType::I64 => b.copy_from_slice(&v.to_le_bytes()),
                    DType::U64 => b.copy_from_slice(&(v as u64).to_le_bytes()),
                    DType::U32 => b.copy_from_slice(&(v as u32).to_le_bytes()),
                    DType::U16 => b.copy_from_slice(&(v as u16).to_le_bytes()),
                }
            }
        }
        Ok((backing, n * size))
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn rows(&self) -> u32 {
        self.header.rows
    }

    /// `(min, max)` transaction time over this segment's rows, folded at open.
    /// `tt_min > as_of` means nothing here is believed yet; `tt_max <= as_of`
    /// means all of it is — the two answers a scan needs before it looks at
    /// any run.
    pub fn tt_s_bounds(&self) -> (i64, i64) {
        (self.tt_s_min, self.tt_s_max)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn raw(&self, desc: &ColumnDesc) -> Result<&[u8]> {
        let start = self.data_start + desc.offset as usize;
        let end = start + desc.bytes as usize;
        self.source.bytes().get(start..end).ok_or_else(|| {
            EngineError::corrupt(format!("column '{}' extends past the file", desc.name))
                .at_file(&self.path)
        })
    }

    fn raw_strings(&self) -> Result<&[u8]> {
        let start = self.data_start + self.header.strings_offset as usize;
        let end = start + self.header.strings_bytes as usize;
        self.source.bytes().get(start..end).ok_or_else(|| {
            EngineError::corrupt("string table extends past the file").at_file(&self.path)
        })
    }

    pub fn strings(&self) -> Result<StringView<'_>> {
        match &self.decoded_strings {
            Some(raw) => StringView::parse(raw),
            None => StringView::parse(self.raw_strings()?),
        }
    }

    fn typed<T: Copy>(&self, name: &str, dtype: DType) -> Result<&[T]> {
        let desc = self.header.column(name).ok_or_else(|| {
            EngineError::corrupt(format!("segment has no column '{name}'")).at_file(&self.path)
        })?;
        if desc.dtype != dtype {
            return Err(EngineError::corrupt(format!(
                "column '{name}' is {:?}, not {dtype:?}",
                desc.dtype
            ))
            .at_file(&self.path));
        }
        let bytes: &[u8] = if desc.codec == crate::codec::CODEC_RAW {
            self.raw(desc)?
        } else {
            // index alignment with header.columns is fixed
            let idx = self
                .header
                .columns
                .iter()
                .position(|c| std::ptr::eq(c, desc))
                .expect("desc comes from this header");
            let (backing, len) = match &self.decoded[idx] {
                Decoded::Eager(v) => v,
                Decoded::Lazy(cell) => {
                    // Two threads racing this both decode and one wins the
                    // `set`; the loser drops its copy and both read the
                    // winner's, which is the same bytes at a stable address.
                    if cell.get().is_none() {
                        let v = self.materialize(desc)?;
                        let _ = cell.set(v);
                    }
                    cell.get().expect("just set")
                }
                Decoded::Raw => unreachable!("raw columns took the branch above"),
            };
            unsafe {
                std::slice::from_raw_parts(backing.as_ptr() as *const u8, *len)
            }
        };
        let size = std::mem::size_of::<T>();
        if !bytes.len().is_multiple_of(size) {
            return Err(EngineError::corrupt(format!(
                "column '{name}' length {} is not a multiple of {size}",
                bytes.len()
            ))
            .at_file(&self.path));
        }
        if !(bytes.as_ptr() as usize).is_multiple_of(std::mem::align_of::<T>()) {
            return Err(EngineError::corrupt(format!(
                "column '{name}' is misaligned for zero-copy access"
            ))
            .at_file(&self.path));
        }
        if cfg!(target_endian = "big") {
            return Err(EngineError::corrupt(
                "segments are little-endian; this build is big-endian",
            ));
        }
        // SAFETY: T is only ever a plain integer type (the callers below);
        // length is an exact multiple of its size and the pointer alignment
        // was just checked. Extents are 64-byte aligned by construction, so
        // this holds for every column the writer emits.
        Ok(unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const T, bytes.len() / size) })
    }

    pub fn i64_column(&self, name: &str) -> Result<&[i64]> {
        self.typed(name, DType::I64)
    }
    pub fn u64_column(&self, name: &str) -> Result<&[u64]> {
        self.typed(name, DType::U64)
    }
    pub fn u32_column(&self, name: &str) -> Result<&[u32]> {
        self.typed(name, DType::U32)
    }
    pub fn u16_column(&self, name: &str) -> Result<&[u16]> {
        self.typed(name, DType::U16)
    }

    /// `vt_e` for one row, materializing the elided instantaneous case.
    pub fn vt_e_at(&self, row: usize) -> Result<i64> {
        if self.header.vt_e_elided {
            Ok(self.i64_column("vt_s")?[row] + 1)
        } else {
            Ok(self.i64_column("vt_e")?[row])
        }
    }

    /// A column's model, when it is codec 2 — random access and a range
    /// path that never materialises the column.
    ///
    /// `Ok(None)` when the column exists under another codec, which is not
    /// an error: the writer chooses per column by trial encode, and a caller
    /// that wants the model asks whether there is one.
    pub fn lin_column(&self, name: &str) -> Result<Option<crate::linfor::LinColumn<'_>>> {
        let desc = self.header.column(name).ok_or_else(|| {
            EngineError::corrupt(format!("segment has no column '{name}'")).at_file(&self.path)
        })?;
        if desc.codec != crate::codec::CODEC_LINMODEL {
            return Ok(None);
        }
        crate::linfor::LinColumn::parse(self.raw(desc)?, self.header.rows as usize)
            .map(Some)
            .map_err(|e| e.at_file(&self.path))
    }

    /// This segment's transaction-time axis, in whichever shape it was
    /// written — see [`crate::ttime::TtSView`].
    ///
    /// Resolve it **once per segment**, not per row: on the dictionary shape
    /// it looks the code column up by name, which is a walk over the column
    /// descriptors, and then every `at()` is two indexed loads.
    pub fn tt_s_view(&self) -> Result<crate::ttime::TtSView<'_>> {
        if self.header.tt_s_dict.is_empty() {
            // the bounds this segment folded at open, not a fold of the whole
            // run list per call
            return Ok(crate::ttime::TtSView::from_header_with_bounds(
                &self.header,
                (self.tt_s_min, self.tt_s_max),
            ));
        }
        let dtype = self
            .header
            .column(TT_S_CODE_COLUMN)
            .ok_or_else(|| {
                EngineError::corrupt(
                    "segment header carries a tt_s dictionary but no tt_s_code column",
                )
                .at_file(&self.path)
            })?
            .dtype;
        let codes = match dtype {
            DType::U16 => crate::ttime::TtCodes::U16(self.u16_column(TT_S_CODE_COLUMN)?),
            DType::U32 => crate::ttime::TtCodes::U32(self.u32_column(TT_S_CODE_COLUMN)?),
            other => {
                return Err(EngineError::corrupt(format!(
                    "tt_s_code is {other:?}; only u16 and u32 code widths exist"
                ))
                .at_file(&self.path))
            }
        };
        Ok(crate::ttime::TtSView::dictionary(
            &self.header,
            codes,
            (self.tt_s_min, self.tt_s_max),
        ))
    }

    /// Maximal runs of equal `tt_s` over this segment's rows, whichever
    /// shape stores them. Layout quality for `verify`, not integrity.
    pub fn tt_s_run_count(&self) -> usize {
        self.tt_s_view().map(|v| v.run_count()).unwrap_or(0)
    }

    /// Folded per-segment visibility (empty = every row still believed).
    pub fn sidecar(&self) -> crate::visibility::Sidecar<'_> {
        crate::visibility::Sidecar::new(&self.header.closed_rows)
    }

    /// Bytes this open segment keeps addressable: the source bytes (file
    /// contents or mapping) plus everything decoded at open (materialized
    /// compressed columns, the unpacked string heap). This is the unit the
    /// store's byte-budget segment cache accounts in — deliberately the
    /// *whole* footprint rather than just the anonymous heap, so the budget
    /// reads as "bytes resident because this segment is cached" without
    /// requiring the operator to know which columns compressed.
    pub fn resident_bytes(&self) -> u64 {
        self.mapped_bytes() + self.decoded_bytes()
    }

    /// The source half of [`Self::resident_bytes`]: the length of the file
    /// contents or mapping this segment reads through.
    ///
    /// Split out because memory accounting needs the two halves
    /// apart because they behave differently under eviction. Dropping a
    /// cached segment frees [`Self::decoded_bytes`] — anonymous heap — but
    /// frees these bytes only if nothing else holds the same mapping. The
    /// store's pinned-source table (`store.rs::pinned_sources`) holds
    /// exactly that for every segment of the open generation, so for a
    /// pinned store an eviction returns no source bytes at all while the
    /// budget is credited as though it had. Neither number is wrong; the
    /// budget accounts addressable bytes, and this pair is what lets a
    /// receipt say which of them an eviction could actually reclaim.
    pub fn mapped_bytes(&self) -> u64 {
        self.source.bytes().len() as u64
    }

    /// The decoded half of [`Self::resident_bytes`]: materialized compressed
    /// columns plus the unpacked string heap, all anonymous heap owned by
    /// this segment alone and freed when it is dropped.
    pub fn decoded_bytes(&self) -> u64 {
        let decoded: usize = self
            .decoded
            .iter()
            .filter_map(Decoded::materialized)
            .map(|(backing, _)| backing.len() * 8)
            .sum();
        let strings = self.decoded_strings.as_ref().map_or(0, Vec::len);
        (decoded + strings) as u64
    }

    /// Heap this segment's parsed header owns — see
    /// [`SegmentHeader::heap_bytes`]. Not part of [`Self::resident_bytes`],
    /// and therefore not bounded by the segment cache's budget; memory
    /// accounting reports it as its own component for exactly that reason.
    pub fn header_heap_bytes(&self) -> u64 {
        self.header.heap_bytes()
    }

    /// Full 96-bit version id for one row, reassembled from its two columns.
    pub fn vid_at(&self, row: usize) -> Result<Id96> {
        Ok(Id96 {
            hi: self.u64_column("vid64")?[row],
            lo: self.u32_column("vid_lo32")?[row],
        })
    }
}

/// A header carrying nothing but a run list — enough to exercise the belief
/// predicate, which reads no column bytes. Shared with `scan`'s tests, which
/// need multi-run headers that no writable batch can produce.
#[cfg(test)]
pub(crate) fn header_with_runs(runs: Vec<(u32, i64)>, rows: u32) -> SegmentHeader {
    SegmentHeader {
        format: FORMAT_VERSION,
        kind: RowKind::Edge,
        lane: Lane::Event,
        rows,
        block_rows: crate::defaults::BLOCK_ROWS,
        key_lo: (0, String::new()),
        key_hi: (0, String::new()),
        vt_min: 0,
        vt_max: 0,
        vt_e_max: 0,
        tt_s_runs: runs,
        tt_s_dict: Vec::new(),
        tt_s_code_blocks: Vec::new(),
        tt_s_block_rows: 0,
        rel_types: Vec::new(),
        columns: Vec::new(),
        vt_e_elided: true,
        strings_offset: 0,
        strings_bytes: 0,
        strings_count: 0,
        closed_rows: Vec::new(),
        block_vt_e_running_max: Vec::new(),
        open_rows: Vec::new(),
        vt_e_block_rows: 0,
    }
}

fn read_u32(bytes: &[u8], at: usize) -> Result<u32> {
    bytes
        .get(at..at + 4)
        .map(|s| u32::from_le_bytes(s.try_into().expect("4 bytes")))
        .ok_or_else(|| EngineError::corrupt("unexpected end of segment").at_offset(at as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derive::{edge_eid, version_vid};

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-seg-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p.join("000001.tgs")
    }

    fn edge(vt_s: i64, i: u32, tt_s: i64) -> EdgeRow {
        let disc = format!("#{i}");
        let eid = edge_eid("n1", "n2", "SENT_MSG_TO", &disc);
        EdgeRow {
            vid: version_vid(&eid.to_hex(), tt_s, vt_s),
            src_id: i % 7,
            dst_id: (i + 3) % 7,
            rel_type: "SENT_MSG_TO".into(),
            disc,
            vt_s,
            vt_e: vt_s + 1,
            tt_s,
            props: if i.is_multiple_of(3) { "{}" } else { r#"{"rating":-2}"# }.into(),
            source: "ingest".into(),
            provenance_ref: if i.is_multiple_of(5) { None } else { Some("p/1".into()) },
        }
    }

    fn sorted_edges(n: u32) -> Vec<EdgeRow> {
        let mut rows: Vec<EdgeRow> = (0..n).map(|i| edge(1_000 + i as i64, i, 42)).collect();
        rows.sort_by_key(|r| r.sort_key());
        rows
    }

    fn open(path: &Path) -> Segment<MemorySource> {
        Segment::open(path, MemorySource::load(path).unwrap(), true).unwrap()
    }

    /// Same thing; named for the reader, since node fixtures write node rows.
    fn open_node(path: &Path) -> Segment<MemorySource> {
        open(path)
    }

    /// The `(base, divisor)` a segment's `vt_s` column ended up recording,
    /// wherever the winning arm keeps it: in the `ColumnDesc` under codec 1,
    /// in the stream under codec 2.
    fn vt_s_scale(seg: &Segment<MemorySource>) -> (i64, i64) {
        match seg.lin_column("vt_s").unwrap() {
            Some(m) => m.scale(),
            None => {
                let d = seg.header().column("vt_s").unwrap();
                (d.base, d.divisor)
            }
        }
    }

    #[test]
    fn edge_segment_round_trips_every_field() {
        let path = tmp("roundtrip");
        let rows = sorted_edges(500);
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();

        let seg = open(&path);
        assert_eq!(seg.rows(), 500);
        assert_eq!(seg.header().kind, RowKind::Edge);
        assert!(seg.header().vt_e_elided, "instantaneous events elide vt_e");

        let strings = seg.strings().unwrap();
        let vt_s = seg.i64_column("vt_s").unwrap();
        let src = seg.u32_column("src_id").unwrap();
        let dst = seg.u32_column("dst_id").unwrap();
        let rel = seg.u16_column("rel_code").unwrap();
        let props = seg.u32_column("props_ref").unwrap();
        let disc = seg.u32_column("disc_ref").unwrap();
        let prov = seg.u32_column("prov_ref").unwrap();

        for (i, r) in rows.iter().enumerate() {
            assert_eq!(vt_s[i], r.vt_s);
            assert_eq!(src[i], r.src_id);
            assert_eq!(dst[i], r.dst_id);
            assert_eq!(seg.header().rel_types[rel[i] as usize], r.rel_type);
            assert_eq!(seg.vid_at(i).unwrap(), r.vid);
            assert_eq!(seg.vt_e_at(i).unwrap(), r.vt_e);
            assert_eq!(seg.header().tt_s_at(i as u32).unwrap(), r.tt_s);
            assert_eq!(strings.get(props[i]).unwrap(), r.props);
            assert_eq!(strings.get(disc[i]).unwrap(), r.disc);
            assert_eq!(
                strings.get_opt(prov[i]).unwrap(),
                r.provenance_ref.as_deref()
            );
        }
    }

    #[test]
    fn null_provenance_is_distinct_from_empty_string() {
        let path = tmp("null-vs-empty");
        let mut rows = sorted_edges(2);
        rows[0].provenance_ref = None;
        rows[1].provenance_ref = Some(String::new());
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();

        let seg = open(&path);
        let strings = seg.strings().unwrap();
        let prov = seg.u32_column("prov_ref").unwrap();
        assert_eq!(strings.get_opt(prov[0]).unwrap(), None);
        assert_eq!(strings.get_opt(prov[1]).unwrap(), Some(""));
    }

    #[test]
    fn long_intervals_keep_an_explicit_vt_e() {
        let path = tmp("interval");
        let mut rows = sorted_edges(4);
        rows[2].vt_e = rows[2].vt_s + 5_000_000;
        write_edge_segment(
            &path,
            &rows,
            &SegmentSpec {
                lane: Lane::Interval,
                ..Default::default()
            },
        )
        .unwrap();

        let seg = open(&path);
        assert!(!seg.header().vt_e_elided);
        assert_eq!(seg.header().lane, Lane::Interval);
        assert_eq!(seg.vt_e_at(2).unwrap(), rows[2].vt_s + 5_000_000);
        assert_eq!(seg.header().vt_e_max, rows[2].vt_s + 5_000_000);
    }

    /// For a random-ish interval-lane segment, the staircase
    /// is non-decreasing and every block's prefix maximum actually bounds
    /// the `vt_e` of every row in blocks `0..=b`.
    #[test]
    fn staircase_is_monotone_and_bounds_every_row() {
        let path = tmp("staircase-monotone");
        // 37 rows, deliberately not a multiple of the 8-row stair block, to
        // exercise the final partial block. `vt_e` is not itself monotone
        // per row (a real store's isn't either — only the running max is).
        let mut rows = sorted_edges(37);
        for (i, r) in rows.iter_mut().enumerate() {
            let jitter = ((i as i64) * 2_654_435_761) % 4_999;
            r.vt_e = r.vt_s + 1 + jitter.rem_euclid(4_999);
        }
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

        let seg = open(&path);
        let stair = &seg.header().block_vt_e_running_max;
        assert_eq!(stair.len(), (rows.len() as u32).div_ceil(8) as usize);
        assert!(
            stair.windows(2).all(|w| w[0] <= w[1]),
            "prefix maxima must be non-decreasing: {stair:?}"
        );
        for (b, &m) in stair.iter().enumerate() {
            let hi = ((b + 1) * 8).min(rows.len());
            let want = rows[..hi].iter().map(|r| r.vt_e).max().unwrap();
            assert_eq!(m, want, "block {b} prefix max");
        }
    }

    /// The field stays empty on the two shapes that must
    /// never carry a staircase — the event lane, and any elided-`vt_e`
    /// segment (every row instantaneous), interval lane or not.
    #[test]
    fn event_lane_and_elided_segments_carry_no_staircase() {
        let path_event = tmp("staircase-none-event");
        let mut varied = sorted_edges(20);
        for r in varied.iter_mut() {
            r.vt_e = r.vt_s + 1_000;
        }
        write_edge_segment(
            &path_event,
            &varied,
            &SegmentSpec {
                lane: Lane::Event,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(open(&path_event).header().block_vt_e_running_max.is_empty());

        let path_elided = tmp("staircase-none-elided");
        let instantaneous = sorted_edges(20); // vt_e == vt_s + 1 for every row
        write_edge_segment(
            &path_elided,
            &instantaneous,
            &SegmentSpec {
                lane: Lane::Interval,
                ..Default::default()
            },
        )
        .unwrap();
        let seg = open(&path_elided);
        assert!(seg.header().vt_e_elided);
        assert!(seg.header().block_vt_e_running_max.is_empty());
    }

    /// The threshold is a measurement about the segment, not a policy: a
    /// batch writes one transaction time over thousands of rows and must
    /// stay on the run list, byte for byte as before.
    #[test]
    fn a_batch_shaped_segment_keeps_its_run_list() {
        let path = tmp("ttdict-batch");
        write_edge_segment(&path, &sorted_edges(1000), &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        assert_eq!(seg.header().tt_s_runs.len(), 1);
        assert!(seg.header().tt_s_dict.is_empty(), "no dictionary below the threshold");
        assert!(seg.header().tt_s_code_blocks.is_empty());
        assert!(seg.header().column(TT_S_CODE_COLUMN).is_none(), "and no code column");
        assert!(!seg.tt_s_view().unwrap().is_dictionary());
    }

    /// The boundary itself, both sides, at exactly one run per 16 rows.
    #[test]
    fn the_run_density_threshold_is_where_it_says_it_is() {
        assert!(!tt_s_wants_dictionary(0, 1000));
        assert!(!tt_s_wants_dictionary(1, 1000));
        assert!(!tt_s_wants_dictionary(62, 1000), "62/1000 < 1/16");
        assert!(tt_s_wants_dictionary(63, 1000), "63/1000 >= 1/16");
        assert!(tt_s_wants_dictionary(1000, 1000));
        assert!(!tt_s_wants_dictionary(0, 0), "an empty segment has no representation to pick");
        // the floor: a one-run segment stays on the run list at every size,
        // including the sizes where one run per 16 rows is trivially true
        for rows in [1usize, 2, 6, 16, 17, 100] {
            assert!(!tt_s_wants_dictionary(1, rows), "one run over {rows} rows");
        }
        assert!(!tt_s_wants_dictionary(15, 15), "15 runs cannot be a size problem");
        assert!(tt_s_wants_dictionary(16, 16));
        // no overflow on absurd inputs
        assert!(tt_s_wants_dictionary(usize::MAX, 1));
    }

    /// Above the threshold: the runs leave the header, the dictionary is
    /// ascending and deduplicated, and every row still reads back its own
    /// transaction time.
    #[test]
    fn a_compaction_shaped_segment_dictionary_codes_tt_s() {
        let path = tmp("ttdict-mixed");
        let mut rows: Vec<EdgeRow> = (0..600u32)
            .map(|i| edge(1_000 + i as i64, i, 100 + (i % 7) as i64))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();

        let seg = open(&path);
        let h = seg.header();
        assert!(h.tt_s_runs.is_empty(), "the run list is dropped, not kept alongside");
        assert_eq!(h.tt_s_dict, vec![100, 101, 102, 103, 104, 105, 106]);
        assert!(
            h.tt_s_dict.windows(2).all(|w| w[0] < w[1]),
            "the dictionary must ascend strictly — code order is time order"
        );
        assert_eq!(h.tt_s_block_rows, TT_S_CODE_BLOCK_ROWS);
        assert_eq!(h.tt_s_code_blocks.len(), 1, "600 rows is one 4096-row block");
        assert_eq!(h.tt_s_code_blocks[0], (0, 6));
        let code = h.column(TT_S_CODE_COLUMN).expect("a code column");
        assert_eq!(code.dtype, DType::U16, "seven distinct times fit a u16 code");

        let tt = seg.tt_s_view().unwrap();
        assert!(tt.is_dictionary());
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(tt.at(i as u32).unwrap(), r.tt_s, "row {i}");
        }
        assert_eq!(seg.tt_s_bounds(), (100, 106));
        assert_eq!(tt.run_count(), 600, "every adjacent pair differs after the vt_s sort");
    }

    /// The point of the change, measured rather than asserted in prose: the
    /// header stops carrying one JSON tuple per run.
    ///
    /// Both segments hold the same rows and the same transaction times; only
    /// the representation differs. The run list is priced at roughly 28
    /// characters an entry, so at one run per row it is the segment. The
    /// comparison is against the same rows written with the threshold
    /// effectively disabled, which is what the code did before this change.
    #[test]
    fn the_dictionary_shrinks_the_header_by_more_than_an_order_of_magnitude() {
        let n = 5_000u32;
        let mut rows: Vec<EdgeRow> = (0..n)
            .map(|i| edge(1_000 + i as i64, i, 100 + (i % 97) as i64))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        let path = tmp("ttdict-size");
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        assert!(seg.tt_s_view().unwrap().is_dictionary());
        let with_dict = serde_json::to_vec(seg.header()).unwrap().len();

        // the same header, with the runs put back and the dictionary taken
        // out: what this segment's header would have weighed before this change
        let mut runs: Vec<(u32, i64)> = Vec::new();
        for (i, r) in rows.iter().enumerate() {
            if runs.last().map(|(_, tt)| *tt) != Some(r.tt_s) {
                runs.push((i as u32, r.tt_s));
            }
        }
        let mut old = seg.header().clone();
        old.tt_s_runs = runs;
        old.tt_s_dict = Vec::new();
        old.tt_s_code_blocks = Vec::new();
        let with_runs = serde_json::to_vec(&old).unwrap().len();

        assert!(
            with_dict * 10 < with_runs,
            "header is {with_dict} B with the dictionary against {with_runs} B with the \
             run list — expected better than 10x"
        );
    }

    /// `u32` codes are the fallback, not a refusal: a segment with more
    /// distinct transaction times than a `u16` can name still codes.
    #[test]
    fn a_dictionary_past_the_u16_code_space_widens_instead_of_failing() {
        // 70,000 distinct times over 70,000 rows
        let n = 70_000u32;
        let mut rows: Vec<EdgeRow> = (0..n)
            .map(|i| edge(1_000 + i as i64, i, 1_000_000 + i as i64))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        let path = tmp("ttdict-u32");
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        assert_eq!(seg.header().tt_s_dict.len(), n as usize);
        assert_eq!(
            seg.header().column(TT_S_CODE_COLUMN).unwrap().dtype,
            DType::U32
        );
        let tt = seg.tt_s_view().unwrap();
        for i in (0..n).step_by(311) {
            assert_eq!(tt.at(i).unwrap(), rows[i as usize].tt_s);
        }
        assert_eq!(
            seg.header().tt_s_code_blocks.len(),
            (n as usize).div_ceil(TT_S_CODE_BLOCK_ROWS as usize)
        );
    }

    /// The same rows must produce the same bytes. The dictionary's
    /// order is a sort, the codes are binary searches into it, and the block
    /// bounds are folds — every one an integer function of the rows in
    /// stored order, with no iteration order and no floating point anywhere.
    #[test]
    fn dictionary_coded_segments_are_byte_identical_across_writes() {
        let mut rows: Vec<EdgeRow> = (0..2_000u32)
            .map(|i| edge(1_000 + i as i64, i, 500 - (i % 53) as i64))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        let a = tmp("ttdict-det-a");
        let b = tmp("ttdict-det-b");
        write_edge_segment(&a, &rows, &SegmentSpec::default()).unwrap();
        write_edge_segment(&b, &rows, &SegmentSpec::default()).unwrap();
        assert!(open(&a).tt_s_view().unwrap().is_dictionary());
        assert_eq!(
            fs::read(&a).unwrap(),
            fs::read(&b).unwrap(),
            "two writes of the same rows produced different bytes"
        );
    }

    /// Node segments take the same path — the representation belongs to the
    /// container, not to the edge writer.
    #[test]
    fn node_segments_dictionary_code_tt_s_too() {
        let path = tmp("ttdict-node");
        let mut rows: Vec<NodeRow> = (0..400u32)
            .map(|i| NodeRow {
                vid: version_vid(&format!("u{i}"), 7, 1_000 + i as i64),
                uid_id: i,
                label: "Person".into(),
                vt_s: 1_000 + i as i64,
                vt_e: 1_001 + i as i64,
                tt_s: 50 + (i % 9) as i64,
                props: "{}".into(),
                source: "ingest".into(),
                provenance_ref: None,
            })
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        write_node_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = open_node(&path);
        assert!(seg.header().tt_s_runs.is_empty());
        assert_eq!(seg.header().tt_s_dict.len(), 9);
        let tt = seg.tt_s_view().unwrap();
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(tt.at(i as u32).unwrap(), r.tt_s);
        }
    }

    /// A genuine old-format segment file — many runs in the header, no
    /// dictionary, no code column — must still read, row for row.
    ///
    /// Not a forged header: the bytes are produced by the same writer with
    /// the dictionary switched off, which is byte for byte what it did
    /// before this change. That is the whole compatibility claim, and it is
    /// checked against the file rather than against a struct.
    #[test]
    fn an_old_format_segment_still_reads_row_for_row() {
        let mut rows: Vec<EdgeRow> = (0..800u32)
            .map(|i| edge(1_000 + i as i64, i, 300 + (i % 13) as i64))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        let old_path = tmp("ttdict-oldfmt");
        write_edge_segment(
            &old_path,
            &rows,
            &SegmentSpec { tt_s_dictionary: false, ..SegmentSpec::default() },
        )
        .unwrap();
        let old = open(&old_path);
        assert!(
            old.header().tt_s_runs.len() > 700,
            "the fixture must really be the run-list shape: {} runs",
            old.header().tt_s_runs.len()
        );
        assert!(old.header().tt_s_dict.is_empty());
        assert!(old.header().column(TT_S_CODE_COLUMN).is_none());

        let new_path = tmp("ttdict-newfmt");
        write_edge_segment(&new_path, &rows, &SegmentSpec::default()).unwrap();
        let new = open(&new_path);
        assert!(new.tt_s_view().unwrap().is_dictionary());

        // the two files disagree about layout and about nothing else
        let (vo, vn) = (old.tt_s_view().unwrap(), new.tt_s_view().unwrap());
        assert_eq!(old.tt_s_bounds(), new.tt_s_bounds());
        assert_eq!(vo.run_count(), vn.run_count());
        for (i, r) in rows.iter().enumerate() {
            let i = i as u32;
            assert_eq!(vo.at(i).unwrap(), r.tt_s, "old row {i}");
            assert_eq!(vn.at(i).unwrap(), r.tt_s, "new row {i}");
        }
        assert!(
            fs::metadata(&new_path).unwrap().len() < fs::metadata(&old_path).unwrap().len(),
            "the dictionary must not make the file bigger"
        );
    }

    /// The scale arm of the trial encode, end to end on a real segment: a
    /// `vt_s` column at seconds resolution must record the divisor, shrink
    /// by its width, and read back bit for bit.
    #[test]
    fn a_seconds_resolution_vt_s_column_records_its_divisor() {
        let mut t = 1_600_000_000_000_000i64;
        let mut rows: Vec<EdgeRow> = (0..20_000u32)
            .map(|i| {
                let v = t;
                t += (1 + (i as i64 * 7919) % 4001) * 1_000_000;
                edge(v, i, 42)
            })
            .collect();
        rows.sort_by_key(|r| r.sort_key());

        let path = tmp("scale-vt-s");
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        assert_eq!(
            vt_s_scale(&seg),
            (rows[0].vt_s, 1_000_000),
            "the exact scale must be recorded by whichever arm won"
        );

        // read back bit for bit -- this is the whole exactness claim
        let vt_s = seg.i64_column("vt_s").unwrap();
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(vt_s[i], r.vt_s, "row {i}");
        }

        // and the width it lands at. An undivided microsecond column of this
        // shape packs at ~43 bits/row; the divisor is worth log2(1e6) of
        // them, which the codec's own test measures exactly.
        let bits = seg.header().column("vt_s").unwrap().bytes as f64 * 8.0
            / rows.len() as f64;
        assert!(
            bits < 25.0,
            "scaled vt_s is {bits:.1} bits/row; an undivided microsecond column is ~43"
        );
    }

    /// Columns with no common factor must be untouched — the arm has to be
    /// free to try everywhere, which means it has to decline cleanly.
    #[test]
    fn columns_without_a_common_factor_keep_the_identity_scale() {
        let path = tmp("scale-identity");
        write_edge_segment(&path, &sorted_edges(5_000), &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        // sorted_edges steps vt_s by 1, and the id columns are modular
        for name in ["vt_s", "src_id", "dst_id", "vid64", "vid_lo32"] {
            let d = seg.header().column(name).unwrap();
            assert_eq!((d.base, d.divisor), (0, 1), "column {name}");
        }
    }

    /// A `ColumnDesc` written before the scale existed has neither key.
    /// `#[serde(default)]` must give it the identity, not a divisor of zero
    /// — which would be a division by zero dressed as a default.
    #[test]
    fn an_old_column_desc_defaults_to_the_identity_scale() {
        let old_json = r#"{"name":"vt_s","dtype":"i64","offset":0,"bytes":64,"codec":1}"#;
        let d: ColumnDesc = serde_json::from_str(old_json)
            .expect("a ColumnDesc with no base/divisor key must still parse");
        assert_eq!((d.base, d.divisor), (0, 1));
    }

    /// Two writes of the same rows must still be byte-identical once the
    /// scale is in play: the gcd is an integer fold over the values in
    /// stored order, so it cannot vary by host or by run.
    #[test]
    fn scaled_segments_are_byte_identical_across_writes() {
        let mut t = 1_600_000_000_000_000i64;
        let mut rows: Vec<EdgeRow> = (0..3_000u32)
            .map(|i| {
                let v = t;
                t += (1 + (i as i64 * 31) % 97) * 1_000_000;
                edge(v, i, 42)
            })
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        let a = tmp("scale-det-a");
        let b = tmp("scale-det-b");
        write_edge_segment(&a, &rows, &SegmentSpec::default()).unwrap();
        write_edge_segment(&b, &rows, &SegmentSpec::default()).unwrap();
        assert_eq!(vt_s_scale(&open(&a)).1, 1_000_000);
        assert_eq!(fs::read(&a).unwrap(), fs::read(&b).unwrap());
    }

    /// The model arm, end to end: a rising `vt_s` picks codec 2, reads back
    /// row for row through the ordinary slice accessor, and exposes its
    /// model to a caller that wants the range path.
    #[test]
    fn a_rising_vt_s_column_picks_the_model() {
        let mut t = 1_600_000_000_000_000i64;
        let mut rows: Vec<EdgeRow> = (0..30_000u32)
            .map(|i| {
                let v = t;
                t += (1 + (i as i64 * 7919) % 4001) * 1_000_000;
                edge(v, i, 42)
            })
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        let path = tmp("linfor-vt-s");
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = open(&path);

        let desc = seg.header().column("vt_s").unwrap();
        assert_eq!(desc.codec, crate::codec::CODEC_LINMODEL);
        assert_eq!(
            (desc.base, desc.divisor),
            (0, 1),
            "codec 2 carries its scale in its stream, not in the descriptor"
        );
        let m = seg.lin_column("vt_s").unwrap().expect("a model");
        assert_eq!(m.scale(), (rows[0].vt_s, 1_000_000));
        assert!(m.is_sorted(), "vt_s is cluster-ordered, so the flag must be set");
        assert_eq!(m.blocks(), rows.len().div_ceil(crate::linfor::BLOCK_ROWS));

        // the ordinary accessor is unchanged for every caller that wants a
        // slice -- this is the compatibility claim for codec 2
        let vt_s = seg.i64_column("vt_s").unwrap();
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(vt_s[i], r.vt_s, "row {i}");
            assert_eq!(m.get(i), r.vt_s, "model row {i}");
        }

        // and the cut agrees with the slice at every boundary
        for r in rows.iter().step_by(97) {
            for t in [r.vt_s - 1, r.vt_s, r.vt_s + 1] {
                assert_eq!(
                    m.partition_point_lt(t),
                    vt_s.partition_point(|&x| x < t),
                    "cut at {t}"
                );
            }
        }

        let bits = desc.bytes as f64 * 8.0 / rows.len() as f64;
        assert!(bits < 20.0, "modelled vt_s is {bits:.1} bits/row");
    }

    /// A column with no linear structure must not be forced under a model:
    /// the trial encode picks by size, and the sha-prefix columns are the
    /// case where every arm loses to raw.
    #[test]
    fn columns_with_no_structure_stay_raw() {
        let path = tmp("linfor-raw");
        write_edge_segment(&path, &sorted_edges(20_000), &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        assert_eq!(
            seg.header().column("vid64").unwrap().codec,
            crate::codec::CODEC_RAW,
            "sha256 prefixes have no structure to pack"
        );
    }

    /// The four-candidate choice must be a function of the rows: same rows,
    /// same winner, same bytes.
    #[test]
    fn the_codec_choice_is_a_function_of_the_rows() {
        let mut t = 1_600_000_000_000_000i64;
        let mut rows: Vec<EdgeRow> = (0..8_000u32)
            .map(|i| {
                let v = t;
                t += (1 + (i as i64 * 31) % 97) * 1_000_000;
                edge(v, i, 300 + (i % 23) as i64)
            })
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        let a = tmp("linfor-det-a");
        let b = tmp("linfor-det-b");
        write_edge_segment(&a, &rows, &SegmentSpec::default()).unwrap();
        write_edge_segment(&b, &rows, &SegmentSpec::default()).unwrap();
        let (sa, sb) = (open(&a), open(&b));
        for (ca, cb) in sa.header().columns.iter().zip(&sb.header().columns) {
            assert_eq!(ca, cb, "column '{}' chose differently", ca.name);
        }
        assert_eq!(fs::read(&a).unwrap(), fs::read(&b).unwrap());
    }

    /// Every dtype the writer emits must survive codec 2, because the trial
    /// encode offers it to all of them and a `u32` column that picks the
    /// model must still come back through the `u32` view.
    #[test]
    fn every_dtype_survives_the_model() {
        let path = tmp("linfor-dtypes");
        let mut rows: Vec<NodeRow> = (0..20_000u32)
            .map(|i| NodeRow {
                vid: version_vid(&format!("u{i}"), 7, 1_000 + i as i64),
                // uid_id rises steadily: a u32 column the model can fit
                uid_id: i * 3,
                label: "Person".into(),
                vt_s: 1_000_000 + i as i64 * 1_000,
                vt_e: 1_000_001 + i as i64 * 1_000,
                tt_s: 50 + (i % 9) as i64,
                props: "{}".into(),
                source: "ingest".into(),
                provenance_ref: None,
            })
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        write_node_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = open_node(&path);
        let uid = seg.u32_column("uid_id").unwrap();
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(uid[i], r.uid_id, "uid row {i}");
        }
        let vt_s = seg.i64_column("vt_s").unwrap();
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(vt_s[i], r.vt_s, "vt_s row {i}");
        }
    }

    /// A segment that takes none of these mechanisms must have a header
    /// byte-identical to the one it had before these mechanisms existed.
    ///
    /// Not a tidiness point. Every new key serialised at its identity is
    /// paid on every segment, and a store of many small segments is mostly
    /// fixed header cost: measured at +0.32 bits/row store-wide on a
    /// 267-segment synthetic build where the dictionary, the scale and the
    /// model all declined. That is a regression on exactly the corpus the
    /// change cannot otherwise show a win on.
    #[test]
    fn a_segment_using_no_w16a_mechanism_writes_no_w16a_keys() {
        let path = tmp("no-w16a-keys");
        write_edge_segment(&path, &sorted_edges(1000), &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        let json = serde_json::to_string(seg.header()).unwrap();
        for key in [
            "tt_s_dict",
            "tt_s_code_blocks",
            "tt_s_block_rows",
            "open_rows",
            "block_vt_e_running_max",
        ] {
            assert!(!json.contains(key), "header still carries {key}: {json}");
        }
        // The per-column scale is skipped at the identity too -- checked on
        // the descriptor itself rather than on this segment, because the
        // scale arm legitimately *does* fire here: the string-ref columns
        // hold a handful of distinct values (0, 1, NULL_REF) whose first
        // differences share a factor, and the trial encode takes it because
        // it is exact and smaller. That is the arm working, not leaking.
        let identity = ColumnDesc {
            name: "c".into(), dtype: DType::I64, offset: 0, bytes: 8,
            codec: crate::codec::CODEC_FOR, base: 0, divisor: 1,
        };
        let j = serde_json::to_string(&identity).unwrap();
        assert!(!j.contains("base") && !j.contains("divisor"), "identity scale written: {j}");
        let scaled = ColumnDesc { base: 5, divisor: 7, ..identity.clone() };
        let j = serde_json::to_string(&scaled).unwrap();
        assert!(j.contains("\"base\":5") && j.contains("\"divisor\":7"), "real scale dropped: {j}");
        assert_eq!(serde_json::from_str::<ColumnDesc>(&j).unwrap(), scaled);
        assert_eq!(
            serde_json::from_str::<ColumnDesc>(
                &serde_json::to_string(&identity).unwrap()
            ).unwrap(),
            identity,
            "an omitted identity must read back as the identity"
        );

        // and a header that does use them still round-trips through the
        // omission -- skipping on write must not change what is read back
        let mut rows: Vec<EdgeRow> = (0..600u32)
            .map(|i| edge(1_000 + i as i64, i, 100 + (i % 7) as i64))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        let used = tmp("w16a-keys-used");
        write_edge_segment(&used, &rows, &SegmentSpec::default()).unwrap();
        let seg = open(&used);
        let json = serde_json::to_string(seg.header()).unwrap();
        assert!(json.contains("tt_s_dict"), "a dictionary-coded header must say so");
        let back: SegmentHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(&back, seg.header(), "omission changed what reads back");
    }

    /// An old header has none of these keys at all. `#[serde(default)]`
    /// must parse it into exactly the run-list path — no format bump, and
    /// every old store reads through the code it always read through.
    #[test]
    fn an_old_header_without_the_tt_s_dictionary_keys_parses_as_runs() {
        let old_json = r#"{
            "format": 1, "kind": "edge", "lane": "event", "rows": 3,
            "block_rows": 32768, "key_lo": [0, "a"], "key_hi": [2, "b"],
            "vt_min": 0, "vt_max": 2, "vt_e_max": 3,
            "tt_s_runs": [[0, 10], [2, 20]], "rel_types": [], "columns": [],
            "vt_e_elided": true, "strings_offset": 0, "strings_bytes": 0,
            "strings_count": 0
        }"#;
        let h: SegmentHeader = serde_json::from_str(old_json)
            .expect("a header with no tt_s_dict / tt_s_code_blocks key must still parse");
        assert!(h.tt_s_dict.is_empty());
        assert!(h.tt_s_code_blocks.is_empty());
        assert_eq!(h.tt_s_block_rows, 0);
        assert_eq!(h.tt_s_bounds(), (10, 20));
        assert_eq!(h.tt_s_at(0).unwrap(), 10);
        assert_eq!(h.tt_s_at(2).unwrap(), 20);
        let v = crate::ttime::TtSView::from_header(&h);
        assert!(!v.is_dictionary());
        assert_eq!(v.at(1).unwrap(), 10);
        assert_eq!(v.run_count(), 2);
    }

    /// Asking a dictionary-coded header for a row's transaction time is a
    /// programming error, and it says so rather than answering from an empty
    /// run list — which would be a silently wrong time, not an error.
    #[test]
    fn the_header_refuses_tt_s_at_on_a_dictionary_coded_segment() {
        let path = tmp("ttdict-refuse");
        let mut rows: Vec<EdgeRow> = (0..200u32)
            .map(|i| edge(1_000 + i as i64, i, 10 + (i % 4) as i64))
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        let err = seg.header().tt_s_at(0).unwrap_err();
        assert!(
            format!("{err}").contains("tt_s_view"),
            "the error must name the right reader: {err}"
        );
    }

    /// A header JSON written before this field existed has
    /// no such key at all. `#[serde(default)]` must still parse it, into
    /// exactly the "no staircase" fallback every reader already understands.
    #[test]
    fn an_old_header_without_a_staircase_parses_with_the_fallback() {
        let old_json = r#"{
            "format": 1, "kind": "edge", "lane": "event", "rows": 3,
            "block_rows": 32768, "key_lo": [0, "a"], "key_hi": [2, "b"],
            "vt_min": 0, "vt_max": 2, "vt_e_max": 3,
            "tt_s_runs": [[0, 10]], "rel_types": [], "columns": [],
            "vt_e_elided": true, "strings_offset": 0, "strings_bytes": 0,
            "strings_count": 0
        }"#;
        let h: SegmentHeader = serde_json::from_str(old_json)
            .expect("a header with no block_vt_e_running_max / vt_e_block_rows key must still parse");
        assert!(h.block_vt_e_running_max.is_empty());
        assert_eq!(h.vt_e_block_rows, 0);
        assert!(h.closed_rows.is_empty()); // the same #[serde(default)] shape as closed_rows
    }

    /// Adding the header field is not a format bump — the
    /// format byte stays 1 and a newly written interval segment still opens
    /// under the existing strict format check.
    #[test]
    fn a_new_segment_is_readable_by_the_strict_format_check() {
        let path = tmp("staircase-format-unchanged");
        let mut rows = sorted_edges(10);
        for r in rows.iter_mut() {
            r.vt_e = r.vt_s + 500;
        }
        write_edge_segment(
            &path,
            &rows,
            &SegmentSpec {
                lane: Lane::Interval,
                ..Default::default()
            },
        )
        .unwrap();
        let seg = open(&path); // would fail to open if the format check rejected it
        assert_eq!(seg.header().format, crate::FORMAT_VERSION);
        assert_eq!(crate::FORMAT_VERSION, 1, "adding the header field must not bump the segment format");
        assert!(!seg.header().block_vt_e_running_max.is_empty());
    }

    #[test]
    fn zone_map_bounds_the_data() {
        let path = tmp("zonemap");
        let rows = sorted_edges(100);
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        let h = seg.header();
        assert_eq!(h.vt_min, rows.iter().map(|r| r.vt_s).min().unwrap());
        assert_eq!(h.vt_max, rows.iter().map(|r| r.vt_s).max().unwrap());
        assert_eq!(h.key_lo, (rows[0].vt_s, rows[0].vid.to_hex()));
        assert_eq!(h.key_hi, (rows[99].vt_s, rows[99].vid.to_hex()));
    }

    #[test]
    fn mmap_and_memory_sources_agree() {
        let path = tmp("sources");
        let rows = sorted_edges(64);
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();

        let mem = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        let mapped = Segment::open(&path, MmapSource::load(&path).unwrap(), true).unwrap();
        assert_eq!(mem.header(), mapped.header());
        assert_eq!(
            mem.i64_column("vt_s").unwrap(),
            mapped.i64_column("vt_s").unwrap()
        );
        assert_eq!(
            mem.strings().unwrap().get(3).unwrap(),
            mapped.strings().unwrap().get(3).unwrap()
        );
    }

    #[test]
    fn columns_are_64_byte_aligned_for_zero_copy() {
        let path = tmp("align");
        write_edge_segment(&path, &sorted_edges(37), &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        for c in &seg.header().columns {
            assert_eq!(
                (seg.data_start + c.offset as usize) % ALIGN,
                0,
                "column '{}' is not aligned",
                c.name
            );
        }
    }

    #[test]
    fn tt_s_runs_collapse_a_uniform_batch() {
        let path = tmp("ttruns");
        write_edge_segment(&path, &sorted_edges(1000), &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        assert_eq!(
            seg.header().tt_s_runs.len(),
            1,
            "one batch writes one transaction time"
        );
    }

    /// The reference `tt_s_at`: the reverse linear scan the binary search
    /// replaced. Kept here so the equivalence is checked rather than argued.
    fn tt_s_at_linear(h: &SegmentHeader, row: u32) -> Option<i64> {
        h.tt_s_runs
            .iter()
            .rev()
            .find(|(start, _)| *start <= row)
            .map(|(_, tt)| *tt)
    }

    #[test]
    fn tt_s_at_binary_search_matches_the_linear_scan() {
        // Compaction's shape, which no batch can write: hundreds of runs at
        // irregular starts, each carrying its origin transaction time.
        let mut seed = 0x2026_0824_u64;
        let mut rng = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for case in 0..25 {
            let rows = 1 + (rng() % 400) as u32;
            let mut runs: Vec<(u32, i64)> = Vec::new();
            let mut at = if case % 5 == 0 { 1 } else { 0 }; // sometimes leave row 0 uncovered
            while at < rows {
                runs.push((at, (rng() % 50) as i64));
                at += 1 + (rng() % 6) as u32;
            }
            let h = header_with_runs(runs, rows);
            // every row, and the boundaries explicitly: row 0, each run start,
            // the last row, and one past the end
            let mut probes: Vec<u32> = (0..rows).collect();
            probes.extend(h.tt_s_runs.iter().map(|(s, _)| *s));
            probes.extend([0, rows.saturating_sub(1), rows, rows + 7]);
            for row in probes {
                assert_eq!(
                    h.tt_s_at(row).ok(),
                    tt_s_at_linear(&h, row),
                    "case {case}, row {row}, runs {:?}",
                    h.tt_s_runs
                );
            }
        }
    }

    #[test]
    fn tt_s_bounds_fold_the_runs() {
        let h = header_with_runs(vec![(0, 30), (4, 10), (9, 20)], 12);
        assert_eq!(h.tt_s_bounds(), (10, 30));
        // no runs believes nothing at any as_of, which is what these
        // sentinels give `min > as_of` for free
        assert_eq!(header_with_runs(Vec::new(), 0).tt_s_bounds(), (i64::MAX, i64::MIN));
    }

    #[test]
    fn a_segment_caches_its_tt_s_bounds_at_open() {
        let path = tmp("ttbounds");
        write_edge_segment(&path, &sorted_edges(64), &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        assert_eq!(seg.tt_s_bounds(), (42, 42));
        assert_eq!(seg.tt_s_bounds(), seg.header().tt_s_bounds());
    }

    #[test]
    fn string_table_dedups_repeated_payloads() {
        let path = tmp("dedup");
        write_edge_segment(&path, &sorted_edges(1000), &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        // "" + 2 props + 1000 discs + rel/source/prov strings — the point is
        // that the two distinct props payloads are stored once each
        assert!(
            seg.header().strings_count < 1010,
            "expected dedup, got {} strings",
            seg.header().strings_count
        );
    }

    // --- corruption detection ------------------------------------------ //

    fn corrupt_at(path: &Path, offset: usize, byte: u8) {
        let mut b = fs::read(path).unwrap();
        b[offset] ^= byte;
        fs::write(path, b).unwrap();
    }

    #[test]
    fn truncated_file_has_no_completion_marker() {
        let path = tmp("truncated");
        write_edge_segment(&path, &sorted_edges(50), &SegmentSpec::default()).unwrap();
        let mut b = fs::read(&path).unwrap();
        b.truncate(b.len() - 16);
        fs::write(&path, &b).unwrap();

        let err = match Segment::open(&path, MemorySource(b), true) {
            Ok(_) => panic!("a truncated segment must not open"),
            Err(e) => e,
        };
        assert_eq!(err.category, crate::error::Category::Corrupt);
        assert!(err.to_string().contains("never finished"));
    }

    #[test]
    fn flipped_data_byte_fails_its_column_checksum() {
        let path = tmp("bitflip");
        let rows = sorted_edges(50);
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = open(&path);
        let vt_s_offset = seg.data_start + seg.header().column("vt_s").unwrap().offset as usize;
        drop(seg);
        corrupt_at(&path, vt_s_offset + 8, 0xff);

        let err = match Segment::open(&path, MemorySource::load(&path).unwrap(), true) {
            Ok(_) => panic!("a flipped byte must be detected"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("checksum"), "{err}");
    }

    #[test]
    fn bad_magic_is_rejected() {
        let path = tmp("magic");
        write_edge_segment(&path, &sorted_edges(4), &SegmentSpec::default()).unwrap();
        corrupt_at(&path, 1, 0xff);
        assert!(Segment::open(&path, MemorySource::load(&path).unwrap(), true).is_err());
    }

    #[test]
    fn empty_segments_are_refused() {
        let path = tmp("empty");
        assert!(write_edge_segment(&path, &[], &SegmentSpec::default()).is_err());
    }

    #[test]
    fn name_is_promoted_to_a_column_without_leaving_props() {
        let path = tmp("name-column");
        let props = [
            r#"{"name":"Alice"}"#,                 // plain
            r#"{"age":3,"name":"Bob"}"#,           // not the first key
            r#"{}"#,                               // absent
            r#"{"name":null}"#,                    // present but not a string
            r#"{"name":"\u00e9l\u00e8ve","x":1}"#, // escaped non-ascii
            r#"{"nickname":"trap"}"#,              // substring of the key only
        ];
        let mut rows: Vec<NodeRow> = props
            .iter()
            .enumerate()
            .map(|(i, p)| NodeRow {
                vid: version_vid(&format!("n{i}"), 7, i as i64),
                uid_id: i as u32,
                label: "Node".into(),
                vt_s: i as i64,
                vt_e: crate::OPEN_END,
                tt_s: 7,
                props: (*p).to_string(),
                source: "ingest".into(),
                provenance_ref: None,
            })
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        write_node_segment(&path, &rows, &SegmentSpec::default()).unwrap();

        let seg = open(&path);
        let strings = seg.strings().unwrap();
        let name = seg.u32_column("name_ref").unwrap();
        let props_col = seg.u32_column("props_ref").unwrap();
        for (i, r) in rows.iter().enumerate() {
            // props survives byte-identical: the store digest depends on it
            assert_eq!(strings.get(props_col[i]).unwrap(), r.props);
            let got = strings.get_opt(name[i]).unwrap();
            let want = match r.props.as_str() {
                p if p.contains(r#""name":"Alice""#) => Some("Alice"),
                p if p.contains(r#""name":"Bob""#) => Some("Bob"),
                p if p.contains(r#"\u00e9l"#) => Some("élève"),
                _ => None,
            };
            assert_eq!(got, want, "props {}", r.props);
        }
    }

    #[test]
    fn node_segment_round_trips() {
        let path = tmp("nodes");
        let mut rows: Vec<NodeRow> = (0..50u32)
            .map(|i| NodeRow {
                vid: version_vid(&format!("n{i}"), 7, i as i64),
                uid_id: i,
                label: "Node".into(),
                vt_s: i as i64,
                vt_e: crate::OPEN_END,
                tt_s: 7,
                props: "{}".into(),
                source: "ingest".into(),
                provenance_ref: None,
            })
            .collect();
        rows.sort_by_key(|r| r.sort_key());
        write_node_segment(&path, &rows, &SegmentSpec::default()).unwrap();

        let seg = open(&path);
        assert_eq!(seg.header().kind, RowKind::Node);
        assert!(!seg.header().vt_e_elided, "OPEN_END is not vt_s + 1");
        let uid = seg.u32_column("uid_id").unwrap();
        let strings = seg.strings().unwrap();
        let label = seg.u32_column("label_ref").unwrap();
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(uid[i], r.uid_id);
            assert_eq!(strings.get(label[i]).unwrap(), r.label);
            assert_eq!(seg.vt_e_at(i).unwrap(), crate::OPEN_END);
        }
    }
}

/// The size-only trial encode chooses exactly what the full trial
/// did, column for column — codec, scale and bytes — over a fixture on
/// which every candidate wins at least once.
#[cfg(test)]
mod w22_encode_tests {
    use super::*;

    fn rng(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn column(dtype: DType, vals: &[i64]) -> ColumnBuilder {
        let mut c = ColumnBuilder::new("c", dtype, vals.len());
        for &v in vals {
            match dtype {
                DType::I64 => c.push_i64(v),
                DType::U64 => c.push_u64(v as u64),
                DType::U32 => c.push_u32(v as u32),
                DType::U16 => c.push_u16(v as u16),
            }
        }
        c
    }

    /// Which of the five candidates a stored column is. An undivided model
    /// on a column that *has* a divisor is candidate 4 proper ("lin"); on a
    /// column without one, candidates 3 and 4 are the same stream and the
    /// listed order credits 3 ("lin-no-divisor").
    fn arm(c: &ColumnBuilder, st: &Stored) -> &'static str {
        let has_scale = crate::codec::exact_scale(&extent_values(c.dtype, &c.bytes)).is_some();
        match arm_of_stream(st) {
            "lin" if !has_scale => "lin-no-divisor",
            a => a,
        }
    }

    fn arm_of_stream(st: &Stored) -> &'static str {
        match st.codec {
            crate::codec::CODEC_RAW => "raw",
            crate::codec::CODEC_FOR if st.divisor > 1 => "for-scaled",
            crate::codec::CODEC_FOR => "for",
            _ => {
                let lin = crate::linfor::LinColumn::parse(&st.bytes, usize::MAX).ok();
                // `expect` is checked by parse; fall back to reading the
                // stored length so the scale can be read either way
                let n = u32::from_le_bytes(st.bytes[..4].try_into().unwrap()) as usize;
                let lin = lin.or_else(|| crate::linfor::LinColumn::parse(&st.bytes, n).ok()).unwrap();
                if lin.scale() != (0, 1) { "lin-scaled" } else { "lin" }
            }
        }
    }

    fn fixture() -> Vec<ColumnBuilder> {
        let mut st = 7u64;
        let mut out = Vec::new();
        for k in 0..240u64 {
            let n = [1usize, 2, 3, 17, 1024, 1025, 4096, 5000, 9000][(k % 9) as usize];
            let d = [1_000i64, 1_000_000, 200, 3][(k % 4) as usize];
            let base = 1_600_000_000_000_000i64 + (rng(&mut st) % 1_000_000) as i64 * d;
            let vals: Vec<i64> = match k % 12 {
                // unsorted values on a coarse grid: frame of reference, divided
                0 => (0..n).map(|_| base + d * (rng(&mut st) % 4096) as i64).collect(),
                // unsorted small ids, no common factor: frame of reference
                1 => (0..n).map(|_| (rng(&mut st) % 50_000) as i64).collect(),
                // rising on a grid with jitter in grid units: the divided model
                2 => (0..n as i64).map(|i| base + d * (3 * i + (rng(&mut st) % 8) as i64)).collect(),
                // rising, no common factor: the model (scale-free)
                3 => (0..n as i64).map(|i| base + 3 * i + (rng(&mut st) % 8) as i64).collect(),
                // an exact line on a grid whose quotients are not a line in
                // fixed point: the undivided model can win
                4 => (0..n as i64).map(|i| base + d * ((i * 7) / 3)).collect(),
                5 => (0..n as i64).map(|i| (i * 1_000_003) / 7 * 6).collect(),
                // hashes: raw
                6 => (0..n).map(|_| rng(&mut st) as i64).collect(),
                // constant, and nearly so
                7 => vec![base; n],
                8 => (0..n).map(|i| if i % 97 == 0 { base + d } else { base }).collect(),
                // extremes
                9 => (0..n).map(|i| if i % 2 == 0 { i64::MIN + i as i64 } else { i64::MAX - i as i64 }).collect(),
                // interval ends: mostly an open-end sentinel
                10 => (0..n as i64).map(|i| if i % 5 == 0 { i64::MAX } else { base + i * d + d }).collect(),
                // grid with a single stray value
                _ => (0..n as i64).map(|i| if i == n as i64 / 2 { base + 1 } else { base + i * d }).collect(),
            };
            out.push(column(DType::I64, &vals));
            // the narrower dtypes the writer also emits
            if k % 3 == 0 {
                let ids: Vec<i64> = vals.iter().map(|v| v.rem_euclid(1 << 32)).collect();
                out.push(column(DType::U32, &ids));
                let codes: Vec<i64> = vals.iter().map(|v| v.rem_euclid(1 << 16)).collect();
                out.push(column(DType::U16, &codes));
                out.push(column(DType::U64, &vals));
            }
        }
        out
    }

    #[test]
    fn the_size_only_trial_chooses_exactly_what_the_full_trial_chose() {
        let mut wins: std::collections::BTreeMap<&'static str, usize> = Default::default();
        for (i, c) in fixture().iter().enumerate() {
            let want = encode_column_reference(c);
            let got = encode_column(c);
            assert_eq!(
                (got.codec, got.base, got.divisor),
                (want.codec, want.base, want.divisor),
                "column {i}: a different candidate"
            );
            assert_eq!(got.bytes, want.bytes, "column {i}: different bytes");
            *wins.entry(arm(c, &want)).or_default() += 1;
        }
        eprintln!("encode fixture wins: {wins:?}");
        // Candidate 4 proper (the undivided model strictly beating the
        // divided one on a column that has a divisor) wins nowhere here, nor
        // in 200,000 structured random columns tried while writing this: an
        // exact fixed-point line whose first differences share a factor has
        // an integer slope, which the divided model fits exactly too. Its
        // length and bytes are held to the full encode arm by arm in
        // `every_arm_is_the_arm_it_replaces`, so a column on which it does
        // win chooses and writes the same either way.
        for a in ["for-scaled", "for", "lin-scaled", "lin-no-divisor", "raw"] {
            assert!(wins.get(a).copied().unwrap_or(0) > 0, "no column where `{a}` wins: {wins:?}");
        }
    }

    /// Each compressed candidate, as the new trial would materialise it,
    /// against the full encode the old trial built — bytes and lengths.
    #[test]
    fn every_arm_is_the_arm_it_replaces() {
        let mut scaled_columns = 0;
        for c in fixture() {
            let vals = extent_values(c.dtype, &c.bytes);
            let scale = crate::codec::exact_scale(&vals);
            // 1 and 2: frame of reference
            if let Some((b, d)) = scale {
                scaled_columns += 1;
                let x = crate::codec::scale_down(&vals, b, d);
                assert_eq!(crate::codec::encoded_len_i64(&x), crate::codec::encode_i64(&x).len());
            }
            assert_eq!(crate::codec::encoded_len_i64(&vals), crate::codec::encode_i64(&vals).len());
            // 3: the divided model, prepared by the caller
            let (b, d) = scale.unwrap_or((0, 1));
            let x3 = if scale.is_some() { crate::codec::scale_down(&vals, b, d) } else { vals.clone() };
            let full3 = crate::linfor::encode(&vals, true);
            assert_eq!(crate::linfor::encode_prepared(&vals, b, d, &x3), full3);
            assert_eq!(crate::linfor::encoded_len_prepared(&vals, &x3), full3.as_ref().map(Vec::len));
            // 4: the undivided model
            let full4 = crate::linfor::encode(&vals, false);
            assert_eq!(crate::linfor::encode_prepared(&vals, 0, 1, &vals), full4);
            assert_eq!(crate::linfor::encoded_len_prepared(&vals, &vals), full4.as_ref().map(Vec::len));
            if scale.is_none() {
                assert_eq!(full3, full4, "without a divisor, candidates 3 and 4 are one stream");
            }
        }
        assert!(scaled_columns > 50);
    }

    #[test]
    fn the_size_only_lengths_are_the_encoded_lengths() {
        for c in fixture() {
            let vals = extent_values(c.dtype, &c.bytes);
            assert_eq!(crate::codec::encoded_len_i64(&vals), crate::codec::encode_i64(&vals).len());
            for scale in [true, false] {
                let (b, d) = if scale {
                    crate::codec::exact_scale(&vals).unwrap_or((0, 1))
                } else {
                    (0, 1)
                };
                let x = if (b, d) == (0, 1) { vals.clone() } else { crate::codec::scale_down(&vals, b, d) };
                assert_eq!(
                    crate::linfor::encoded_len_prepared(&vals, &x),
                    crate::linfor::encode(&vals, scale).map(|v| v.len())
                );
            }
        }
    }
}
