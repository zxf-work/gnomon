//! Per-segment adaptive-sparse temporal CSR.
//!
//! The index is built over **all stored rows of one immutable segment**, not
//! over the store's current beliefs. That single choice is what the rest of
//! this module rests on:
//!
//! 1. A segment file is written once and never rewritten (implementation
//!    the spec; compaction writes *new* ids, `staging.rs`, and ids are never
//!    reused, `manifest.rs`). So the index is immutable for the life of the
//!    file, and a commit invalidates nothing — it only adds segments that
//!    have no index yet. The per-generation rebuild the NumPy TCSR pays
//!    (`gnomon/storage/tcsr.py`) disappears because the unit of the
//!    index stopped being the generation.
//! 2. Belief filtering therefore cannot live *in* the index. It happens at
//!    traversal time, against the believed row ranges of that segment at that
//!    `as_of` — see `store.rs::neighbors_of`.
//! 3. Nothing is gathered. The index holds `u32` offsets and a `u32` row
//!    permutation; `vt_s`/`vt_e`/`dst_id` are read from the segment's own
//!    already-mapped columns through the permutation. The 18 % resident tax
//!    was the gathered `int64` copies, which
//!    this design never materializes.
//!
//! **Index rule**: an index is generation-scoped,
//! rebuildable and *never authoritative*. Every load failure here — missing
//! file, bad magic, wrong format, failed CRC, truncated footer, a stamp that
//! does not match the segment — returns `None`, meaning "rebuild silently".
//! A failure to *write* one never fails a query. That is the rule
//! `tcsr.py:131–135` states and this module copies verbatim.
//!
//! ### File format — `native/idx/tcsr/<seg_id:012>.tci`
//!
//! ```text
//! magic "TGTC" | format u32
//! header_len u32 | header JSON | pad to 4
//! extents: out.active? , out.offsets, out.row_perm,
//!          in.active?  , in.offsets,  in.row_perm      (u32 LE arrays)
//! footer:  crc32c u32 per extent | footer_len u32 | magic "TGTE"
//! ```
//!
//! The extent *lengths* are all derivable from the header, so the footer only
//! has to carry checksums. The stamp is the segment's own body sha: a segment
//! file's content determines its index completely, and the manifest
//! generation is irrelevant to it.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{EngineError, Result};

const MAGIC: &[u8; 4] = b"TGTC";
const FOOTER_MAGIC: &[u8; 4] = b"TGTE";
const PREAMBLE: usize = 12; // magic + format + header_len
const ALIGN: usize = 4;

/// On-disk format of the `.tci` file. Independent of the segment format: this
/// is a rebuildable cache, so a bump here orphans index files and rebuilds
/// them rather than migrating anything.
pub const TCSR_FORMAT: u32 = 1;

/// Which endpoint column groups the rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// grouped by `src_id`
    Out,
    /// grouped by `dst_id`
    In,
}

impl Direction {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "out" => Some(Direction::Out),
            "in" => Some(Direction::In),
            _ => None,
        }
    }
}

/// One direction of one segment's CSR.
///
/// `dense` keeps an offsets entry per vertex id in the segment's id space;
/// `sparse` keeps only the vertices that actually appear, with their ids in a
/// sorted side vector. Dense is elected at or above
/// [`crate::defaults::TCSR_DENSE_THRESHOLD`] occupancy — the constant's first
/// consumer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirIndex {
    dense: bool,
    /// sorted ascending; empty when `dense`
    active: Vec<u32>,
    /// `m + 1` entries sparse, `n_entities + 1` dense
    offsets: Vec<u32>,
    /// a permutation of the segment's row ids, grouped by vertex
    row_perm: Vec<u32>,
}

impl DirIndex {
    /// Rows incident to `u` in this direction, in the segment's own row
    /// order — which is `(vt_s, vid)` (`staging.rs`, `row.rs`), the same
    /// ordering guarantee `tcsr.py`'s stable argsort provides.
    #[inline]
    pub fn slice(&self, u: u32) -> &[u32] {
        let slot = if self.dense {
            if (u as usize) + 1 >= self.offsets.len() {
                return &[];
            }
            u as usize
        } else {
            match self.active.binary_search(&u) {
                Ok(i) => i,
                Err(_) => return &[],
            }
        };
        let (a, b) = (self.offsets[slot] as usize, self.offsets[slot + 1] as usize);
        &self.row_perm[a..b]
    }

    /// [`DirIndex::slice`] for a caller that looks up ascending vertices one
    /// after another: a sparse direction searches forward from `hint`,
    /// the position the previous lookup left, by doubling steps, instead of
    /// over the whole active list. A vertex below the hint's entry falls back
    /// to the whole-list search. Either way the slot, and so the slice, is
    /// the one [`DirIndex::slice`] finds: `active` is sorted and distinct.
    #[inline]
    pub fn slice_from(&self, u: u32, hint: &mut usize) -> &[u32] {
        if self.dense {
            return self.slice(u);
        }
        let a = &self.active;
        let n = a.len();
        let mut lo = (*hint).min(n);
        if lo > 0 && a[lo - 1] >= u {
            lo = 0;
        }
        // a[lo - 1] < u (or lo == 0); find the first index i >= lo with
        // a[i] >= u by doubling, then bisecting the last step
        let mut step = 1usize;
        let mut hi = lo;
        while hi < n && a[hi] < u {
            lo = hi + 1;
            hi = lo + step;
            step <<= 1;
        }
        let hi = hi.min(n);
        let i = lo + a[lo..hi].partition_point(|&x| x < u);
        *hint = i;
        if i >= n || a[i] != u {
            return &[];
        }
        let (s, e) = (self.offsets[i] as usize, self.offsets[i + 1] as usize);
        &self.row_perm[s..e]
    }

    /// Active vertices: `m` sparse, the id-space width dense.
    pub fn m(&self) -> u32 {
        if self.dense {
            self.offsets.len().saturating_sub(1) as u32
        } else {
            self.active.len() as u32
        }
    }

    pub fn is_dense(&self) -> bool {
        self.dense
    }

    pub fn rows(&self) -> u32 {
        self.row_perm.len() as u32
    }

    /// Heap bytes held by this direction. `O(active vertices + rows)` — the
    /// shape the memory model predicts.
    pub fn resident_bytes(&self) -> u64 {
        4 * (self.active.len() + self.offsets.len() + self.row_perm.len()) as u64
    }
}

/// Both directions of one segment, plus the stamp that ties them to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentTcsr {
    seg_id: u64,
    rows: u32,
    n_entities: u32,
    body_sha: String,
    out: DirIndex,
    inn: DirIndex,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct DirHeader {
    dense: bool,
    m: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct TcsrHeader {
    seg_id: u64,
    rows: u32,
    n_entities: u32,
    body_sha: String,
    out: DirHeader,
    #[serde(rename = "in")]
    inn: DirHeader,
}

impl SegmentTcsr {
    /// Build both directions from the segment's endpoint columns.
    ///
    /// Two counting-sort passes per direction — O(n + |V|), no comparison
    /// sort — and the placement pass walks rows in ascending row order, so
    /// each vertex's slice comes out in the segment's own row order without
    /// any stability argument beyond "we appended in order".
    ///
    /// `n_entities` is **segment-local**: `max endpoint id + 1` over these
    /// rows, not the store's entity count. That is what keeps the index a
    /// pure function of the segment file — a store that grows entities must
    /// not invalidate an index whose segment never changed. A vertex
    /// id past the local bound simply has an empty slice, which is the right
    /// answer for a segment written before that vertex existed.
    pub fn build(seg_id: u64, src: &[u32], dst: &[u32], body_sha: &str) -> Result<Self> {
        if src.len() != dst.len() {
            return Err(EngineError::corrupt(
                "tcsr: src_id and dst_id columns differ in length",
            ));
        }
        if src.len() > u32::MAX as usize {
            return Err(EngineError::capacity(
                "tcsr: a segment with more than u32::MAX rows cannot be indexed",
            ));
        }
        let n_entities = src
            .iter()
            .chain(dst.iter())
            .copied()
            .max()
            .map_or(0, |v| v as u64 + 1) as u32;
        let mut aux = vec![0u32; n_entities as usize];
        let out = build_dir(src, n_entities, &mut aux);
        let inn = build_dir(dst, n_entities, &mut aux);
        Ok(Self {
            seg_id,
            rows: src.len() as u32,
            n_entities,
            body_sha: body_sha.to_string(),
            out,
            inn,
        })
    }

    pub fn dir(&self, d: Direction) -> &DirIndex {
        match d {
            Direction::Out => &self.out,
            Direction::In => &self.inn,
        }
    }

    pub fn seg_id(&self) -> u64 {
        self.seg_id
    }

    pub fn rows(&self) -> u32 {
        self.rows
    }

    pub fn n_entities(&self) -> u32 {
        self.n_entities
    }

    pub fn body_sha(&self) -> &str {
        &self.body_sha
    }

    pub fn resident_bytes(&self) -> u64 {
        self.out.resident_bytes() + self.inn.resident_bytes()
    }

    fn header(&self) -> TcsrHeader {
        TcsrHeader {
            seg_id: self.seg_id,
            rows: self.rows,
            n_entities: self.n_entities,
            body_sha: self.body_sha.clone(),
            out: DirHeader {
                dense: self.out.dense,
                m: self.out.m(),
            },
            inn: DirHeader {
                dense: self.inn.dense,
                m: self.inn.m(),
            },
        }
    }

    /// Serialize to the `.tci` byte layout documented at the top of this
    /// module.
    pub fn to_bytes(&self) -> Vec<u8> {
        let header_json =
            serde_json::to_vec(&self.header()).expect("the tcsr header is serializable");
        let mut buf = Vec::with_capacity(PREAMBLE + header_json.len() + 16 * self.rows as usize);
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&TCSR_FORMAT.to_le_bytes());
        buf.extend_from_slice(&(header_json.len() as u32).to_le_bytes());
        buf.extend_from_slice(&header_json);
        while buf.len() % ALIGN != 0 {
            buf.push(0);
        }
        let mut crcs: Vec<u32> = Vec::with_capacity(6);
        for d in [&self.out, &self.inn] {
            for extent in d.extents() {
                let start = buf.len();
                for &v in extent {
                    buf.extend_from_slice(&v.to_le_bytes());
                }
                crcs.push(crc32c::crc32c(&buf[start..]));
            }
        }
        let footer_len = (crcs.len() * 4) as u32;
        for c in &crcs {
            buf.extend_from_slice(&c.to_le_bytes());
        }
        buf.extend_from_slice(&footer_len.to_le_bytes());
        buf.extend_from_slice(FOOTER_MAGIC);
        buf
    }

    /// Write atomically (tmp + rename), so a reader sees old bytes, new
    /// bytes, or nothing. Errors are the caller's to swallow: a store that
    /// cannot take the write (read-only mount, disk full) must still answer
    /// queries, it just rebuilds next time.
    pub fn write(&self, path: &Path) -> Result<()> {
        use std::io::Write;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| EngineError::from(e).at_file(dir))?;
        }
        let tmp = path.with_extension(format!("tci.tmp{}", std::process::id()));
        let bytes = self.to_bytes();
        {
            let mut f =
                std::fs::File::create(&tmp).map_err(|e| EngineError::from(e).at_file(&tmp))?;
            f.write_all(&bytes)
                .map_err(|e| EngineError::from(e).at_file(&tmp))?;
            f.sync_all()
                .map_err(|e| EngineError::from(e).at_file(&tmp))?;
        }
        std::fs::rename(&tmp, path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            EngineError::from(e).at_file(path)
        })
    }

    /// Parse bytes written by [`Self::to_bytes`], or `None` to mean
    /// "rebuild". `None` covers every way the bytes can fail to be this
    /// segment's index: bad magic, a format this build does not write, a
    /// truncated extent, a failed checksum, a foreign `seg_id`, and — the
    /// one that matters for a store restored from backup — a `body_sha` that
    /// is not the one the segment file carries.
    pub fn from_bytes(bytes: &[u8], seg_id: u64, body_sha: &str) -> Option<Self> {
        if bytes.len() < PREAMBLE + 8 || &bytes[..4] != MAGIC {
            return None;
        }
        if &bytes[bytes.len() - 4..] != FOOTER_MAGIC {
            return None;
        }
        if u32::from_le_bytes(bytes[4..8].try_into().ok()?) != TCSR_FORMAT {
            return None;
        }
        let header_len = u32::from_le_bytes(bytes[8..12].try_into().ok()?) as usize;
        let header_end = PREAMBLE.checked_add(header_len)?;
        let header: TcsrHeader = serde_json::from_slice(bytes.get(PREAMBLE..header_end)?).ok()?;
        if header.seg_id != seg_id || header.body_sha != body_sha {
            return None;
        }

        let footer_end = bytes.len() - 8;
        let footer_len = u32::from_le_bytes(bytes[footer_end..footer_end + 4].try_into().ok()?);
        let footer_start = footer_end.checked_sub(footer_len as usize)?;
        if footer_len % 4 != 0 {
            return None;
        }
        let crcs: Vec<u32> = bytes
            .get(footer_start..footer_end)?
            .as_chunks::<4>()
            .0
            .iter()
            .map(|c| u32::from_le_bytes(*c))
            .collect();

        let mut pos = header_end.next_multiple_of(ALIGN);
        let mut crc_i = 0usize;
        let mut dirs: Vec<DirIndex> = Vec::with_capacity(2);
        for h in [&header.out, &header.inn] {
            let lens: [usize; 3] = if h.dense {
                [0, h.m as usize + 1, header.rows as usize]
            } else {
                [h.m as usize, h.m as usize + 1, header.rows as usize]
            };
            let mut parts: Vec<Vec<u32>> = Vec::with_capacity(3);
            for (i, &len) in lens.iter().enumerate() {
                if i == 0 && h.dense {
                    parts.push(Vec::new());
                    continue;
                }
                let end = pos.checked_add(len.checked_mul(4)?)?;
                let raw = bytes.get(pos..end)?;
                if raw.len() > footer_start.saturating_sub(pos) {
                    return None;
                }
                if *crcs.get(crc_i)? != crc32c::crc32c(raw) {
                    return None;
                }
                crc_i += 1;
                parts.push(
                    raw.as_chunks::<4>()
                        .0
                        .iter()
                        .map(|c| u32::from_le_bytes(*c))
                        .collect(),
                );
                pos = end;
            }
            let row_perm = parts.pop()?;
            let offsets = parts.pop()?;
            let active = parts.pop()?;
            let d = DirIndex {
                dense: h.dense,
                active,
                offsets,
                row_perm,
            };
            if !d.is_consistent(header.rows) {
                return None;
            }
            dirs.push(d);
        }
        if crc_i != crcs.len() || pos != footer_start {
            return None;
        }
        let inn = dirs.pop()?;
        let out = dirs.pop()?;
        Some(Self {
            seg_id,
            rows: header.rows,
            n_entities: header.n_entities,
            body_sha: header.body_sha,
            out,
            inn,
        })
    }

    /// Read and validate the index file for one segment, or `None` to mean
    /// "rebuild". Never an error: this is a cache.
    pub fn load(path: &Path, seg_id: u64, body_sha: &str) -> Option<Self> {
        let bytes = std::fs::read(path).ok()?;
        Self::from_bytes(&bytes, seg_id, body_sha)
    }
}

impl DirIndex {
    fn extents(&self) -> Vec<&[u32]> {
        if self.dense {
            vec![&self.offsets, &self.row_perm]
        } else {
            vec![&self.active, &self.offsets, &self.row_perm]
        }
    }

    /// Structural backstop behind the checksums: a file that passes its CRCs
    /// can still be an index of a *different* shape if the header lies, and
    /// `slice` would then index out of bounds. Checked once at load, never
    /// per query.
    fn is_consistent(&self, rows: u32) -> bool {
        if self.row_perm.len() != rows as usize {
            return false;
        }
        if !self.dense && self.offsets.len() != self.active.len() + 1 {
            return false;
        }
        if self.offsets.is_empty() {
            return false;
        }
        if *self.offsets.first().expect("non-empty") != 0
            || *self.offsets.last().expect("non-empty") != rows
        {
            return false;
        }
        if self.offsets.windows(2).any(|w| w[0] > w[1]) {
            return false;
        }
        if !self.dense && self.active.windows(2).any(|w| w[0] >= w[1]) {
            return false;
        }
        !self.row_perm.iter().any(|&r| r >= rows)
    }
}

/// One direction, by counting sort over the dense `u32` vertex id.
///
/// `aux` is a scratch vector of `n_entities` counters, reused between the two
/// directions: it is the only O(|V|) allocation in the build, it is transient,
/// and it is what makes the pass O(n + |V|) with no comparison sort. It holds
/// counts first, then is overwritten with each vertex's running write cursor,
/// which lets the placement loop be one indexed store per row for both the
/// dense and the sparse layout.
fn build_dir(key: &[u32], n_entities: u32, aux: &mut [u32]) -> DirIndex {
    aux[..n_entities as usize].fill(0);
    for &k in key {
        aux[k as usize] += 1;
    }
    let m = aux[..n_entities as usize].iter().filter(|&&c| c > 0).count();
    let dense = n_entities == 0
        || (m as f64) / (n_entities as f64) >= crate::defaults::TCSR_DENSE_THRESHOLD;

    let mut active: Vec<u32> = Vec::new();
    let mut offsets: Vec<u32> = Vec::new();
    if dense {
        offsets.reserve(n_entities as usize + 1);
        offsets.push(0);
        let mut acc = 0u32;
        for slot in aux[..n_entities as usize].iter_mut() {
            let c = *slot;
            *slot = acc; // running cursor
            acc += c;
            offsets.push(acc);
        }
    } else {
        active.reserve(m);
        offsets.reserve(m + 1);
        offsets.push(0);
        let mut acc = 0u32;
        for (v, slot) in aux[..n_entities as usize].iter_mut().enumerate() {
            let c = *slot;
            if c == 0 {
                continue;
            }
            active.push(v as u32);
            *slot = acc;
            acc += c;
            offsets.push(acc);
        }
    }

    let mut row_perm = vec![0u32; key.len()];
    for (r, &k) in key.iter().enumerate() {
        let slot = &mut aux[k as usize];
        row_perm[*slot as usize] = r as u32;
        *slot += 1;
    }
    DirIndex {
        dense,
        active,
        offsets,
        row_perm,
    }
}

/// `<root>/idx/tcsr/<seg_id:012>.tci`.
pub fn index_path(root: &Path, seg_id: u64) -> PathBuf {
    root.join("idx").join("tcsr").join(format!("{seg_id:012}.tci"))
}

/// `<root>/idx/tcsr`.
pub fn index_dir(root: &Path) -> PathBuf {
    root.join("idx").join("tcsr")
}

/// The `body_sha` a segment file records in its own footer — the index's
/// stamp.
///
/// This reads the segment trailer directly rather than through
/// [`crate::segment::Segment`], because `Segment` exposes no accessor for it
/// and `segment.rs` is out of scope here. The coupling
/// is to the trailer only — `... | footer JSON | footer_len u32 | magic` — and
/// the segment format is frozen at 1 for this cycle. Recorded as a
/// discrepancy noted previously; the clean fix is a one-line
/// `Segment::body_sha()`.
///
/// Reads a bounded tail, not the whole file: the footer is a few hundred bytes
/// even for a segment with thousands of columns' worth of CRCs.
pub fn segment_body_sha(path: &Path) -> Result<String> {
    use std::io::{Read, Seek, SeekFrom};

    const TAIL: u64 = 64 * 1024;
    let at = |e: EngineError| e.at_file(path);
    let mut f = std::fs::File::open(path).map_err(|e| at(EngineError::from(e)))?;
    let len = f
        .metadata()
        .map_err(|e| at(EngineError::from(e)))?
        .len();
    if len < 8 {
        return Err(at(EngineError::corrupt("segment file is too short")));
    }
    let take = TAIL.min(len);
    f.seek(SeekFrom::Start(len - take))
        .map_err(|e| at(EngineError::from(e)))?;
    let mut tail = vec![0u8; take as usize];
    f.read_exact(&mut tail)
        .map_err(|e| at(EngineError::from(e)))?;
    if &tail[tail.len() - 4..] != b"TGSE" {
        return Err(at(EngineError::corrupt(
            "segment has no completion marker — it was never finished being written",
        )));
    }
    let footer_end = tail.len() - 8;
    let footer_len =
        u32::from_le_bytes(tail[footer_end..footer_end + 4].try_into().expect("4 bytes")) as usize;
    let footer_start = footer_end
        .checked_sub(footer_len)
        .ok_or_else(|| at(EngineError::corrupt("segment footer does not fit its trailer")))?;
    let v: serde_json::Value = serde_json::from_slice(&tail[footer_start..footer_end])
        .map_err(|e| at(EngineError::corrupt(format!("segment footer is invalid: {e}"))))?;
    v.get("body_sha")
        .and_then(|s| s.as_str())
        .map(str::to_string)
        .ok_or_else(|| at(EngineError::corrupt("segment footer carries no body_sha")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// (src, dst) of a small graph with a deliberately uneven degree
    /// distribution and one vertex that appears only as a destination.
    fn fixture() -> (Vec<u32>, Vec<u32>) {
        let src = vec![0u32, 2, 0, 5, 2, 0, 5];
        let dst = vec![1u32, 3, 3, 1, 1, 9, 3];
        (src, dst)
    }

    fn built() -> SegmentTcsr {
        let (src, dst) = fixture();
        SegmentTcsr::build(7, &src, &dst, "abc123").expect("builds")
    }

    #[test]
    fn slices_group_rows_by_vertex_in_row_order() {
        let t = built();
        assert_eq!(t.dir(Direction::Out).slice(0), &[0, 2, 5]);
        assert_eq!(t.dir(Direction::Out).slice(2), &[1, 4]);
        assert_eq!(t.dir(Direction::Out).slice(5), &[3, 6]);
        assert_eq!(t.dir(Direction::Out).slice(1), &[] as &[u32]);
        assert_eq!(t.dir(Direction::In).slice(1), &[0, 3, 4]);
        assert_eq!(t.dir(Direction::In).slice(3), &[1, 2, 6]);
        assert_eq!(t.dir(Direction::In).slice(9), &[5]);
    }

    /// The forward search from a carried hint finds the slice the
    /// whole-list search finds, for ascending, descending, repeated and
    /// scattered lookups, on a sparse and a dense direction.
    #[test]
    fn a_lookup_from_a_hint_is_the_whole_list_lookup() {
        // sparse: 300 active ids spread over a 3M-id space, uneven degrees
        let mut src = Vec::new();
        let mut dst = Vec::new();
        for i in 0..300u32 {
            for k in 0..(i % 5 + 1) {
                src.push(i * 9_973 + 11);
                dst.push((i * 7 + k) % 40);
            }
        }
        let t = SegmentTcsr::build(3, &src, &dst, "s46").expect("builds");
        assert!(!t.dir(Direction::Out).is_dense());
        let mut x = 12_345u64;
        let mut scattered = Vec::new();
        for _ in 0..2_000 {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            let pick = (x >> 33) as u32;
            // half of them hit an active id, half fall between or outside
            scattered.push(if pick.is_multiple_of(2) { (pick % 300) * 9_973 + 11 } else { pick % 3_100_000 });
        }
        let mut ascending = scattered.clone();
        ascending.sort_unstable();
        let mut descending = ascending.clone();
        descending.reverse();
        let edges = vec![0, 10, 11, 12, 2_990_000, 3_000_000, u32::MAX, 11, 0, u32::MAX];
        for d in [Direction::Out, Direction::In] {
            let ix = t.dir(d);
            for seq in [&scattered, &ascending, &descending, &edges] {
                let mut hint = 0usize;
                for &u in seq.iter() {
                    assert_eq!(ix.slice_from(u, &mut hint), ix.slice(u), "u {u} dir {d:?}");
                }
            }
        }
    }

    #[test]
    fn a_vertex_past_the_segments_id_space_has_an_empty_slice() {
        let t = built();
        // n_entities is the segment-local bound (max endpoint + 1 == 10), so
        // a vertex the store has since created simply is not here.
        assert_eq!(t.n_entities(), 10);
        assert_eq!(t.dir(Direction::Out).slice(10), &[] as &[u32]);
        assert_eq!(t.dir(Direction::Out).slice(u32::MAX), &[] as &[u32]);
        assert_eq!(t.dir(Direction::In).slice(1000), &[] as &[u32]);
    }

    #[test]
    fn every_row_appears_exactly_once_per_direction() {
        let t = built();
        for d in [Direction::Out, Direction::In] {
            let mut seen: Vec<u32> = (0..t.n_entities())
                .flat_map(|u| t.dir(d).slice(u).to_vec())
                .collect();
            seen.sort_unstable();
            assert_eq!(seen, (0..t.rows()).collect::<Vec<_>>());
        }
    }

    #[test]
    fn density_elects_dense_or_sparse_at_the_threshold() {
        // 3 of 10 vertices are sources: 0.3 < 0.5, so `out` is sparse.
        let t = built();
        assert!(!t.dir(Direction::Out).is_dense());
        assert_eq!(t.dir(Direction::Out).m(), 3);

        // a segment whose endpoints saturate a small id space goes dense
        let src = vec![0u32, 1, 2, 3];
        let dst = vec![1u32, 2, 3, 0];
        let d = SegmentTcsr::build(1, &src, &dst, "s").expect("builds");
        assert!(d.dir(Direction::Out).is_dense());
        assert!(d.dir(Direction::In).is_dense());
        assert_eq!(d.dir(Direction::Out).slice(2), &[2]);
    }

    #[test]
    fn an_empty_segment_indexes_to_empty_slices() {
        let t = SegmentTcsr::build(3, &[], &[], "e").expect("builds");
        assert_eq!(t.rows(), 0);
        assert_eq!(t.n_entities(), 0);
        assert_eq!(t.dir(Direction::Out).slice(0), &[] as &[u32]);
        assert_eq!(t.resident_bytes(), 4 * 2); // two one-entry offset vectors
        let bytes = t.to_bytes();
        assert_eq!(
            SegmentTcsr::from_bytes(&bytes, 3, "e").expect("round-trips"),
            t
        );
    }

    #[test]
    fn bytes_round_trip_through_the_file_format() {
        let t = built();
        let bytes = t.to_bytes();
        let back = SegmentTcsr::from_bytes(&bytes, 7, "abc123").expect("round-trips");
        assert_eq!(back, t);
        assert_eq!(back.dir(Direction::In).slice(3), &[1, 2, 6]);
    }

    #[test]
    fn a_dense_direction_round_trips_without_an_active_extent() {
        let src = vec![0u32, 1, 2, 3];
        let dst = vec![1u32, 2, 3, 0];
        let t = SegmentTcsr::build(11, &src, &dst, "dd").expect("builds");
        let back = SegmentTcsr::from_bytes(&t.to_bytes(), 11, "dd").expect("round-trips");
        assert_eq!(back, t);
    }

    #[test]
    fn a_foreign_stamp_is_a_rebuild_not_a_wrong_answer() {
        let t = built();
        let bytes = t.to_bytes();
        assert!(SegmentTcsr::from_bytes(&bytes, 7, "not-the-sha").is_none());
        assert!(SegmentTcsr::from_bytes(&bytes, 8, "abc123").is_none());
    }

    #[test]
    fn damage_anywhere_is_a_rebuild_not_an_error() {
        let t = built();
        let good = t.to_bytes();
        // every single-byte flip in the body must be caught by a CRC, the
        // magic, the format, the header parse or the structural check
        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0xff;
            if bad == good {
                continue;
            }
            assert!(
                SegmentTcsr::from_bytes(&bad, 7, "abc123").is_none(),
                "a flipped byte at {i} was accepted"
            );
        }
        // and so is truncation at every length
        for cut in 0..good.len() {
            assert!(SegmentTcsr::from_bytes(&good[..cut], 7, "abc123").is_none());
        }
        assert!(SegmentTcsr::from_bytes(b"", 7, "abc123").is_none());
        assert!(SegmentTcsr::from_bytes(b"not an index file at all", 7, "abc123").is_none());
    }

    #[test]
    fn write_then_load_round_trips_and_a_missing_file_is_none() {
        let dir = std::env::temp_dir().join(format!("gnomon-tcsr-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = index_path(&dir, 7);
        assert!(SegmentTcsr::load(&path, 7, "abc123").is_none());
        let t = built();
        t.write(&path).expect("writes");
        assert_eq!(SegmentTcsr::load(&path, 7, "abc123").expect("loads"), t);
        assert!(SegmentTcsr::load(&path, 7, "other").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn index_paths_are_zero_padded_under_idx_tcsr() {
        let p = index_path(Path::new("/store/native"), 42);
        assert!(p.ends_with("idx/tcsr/000000000042.tci"), "{p:?}");
        assert_eq!(index_dir(Path::new("/store/native")), p.parent().unwrap());
    }

    #[test]
    fn mismatched_endpoint_columns_are_refused() {
        assert!(SegmentTcsr::build(1, &[0, 1], &[0], "s").is_err());
    }

    #[test]
    fn resident_bytes_are_o_active_vertices_plus_rows() {
        let t = built();
        let out = t.dir(Direction::Out);
        // sparse: active (m) + offsets (m+1) + row_perm (n)
        assert_eq!(out.resident_bytes(), 4 * (3 + 4 + 7));
        let inn = t.dir(Direction::In);
        assert_eq!(inn.resident_bytes(), 4 * (3 + 4 + 7));
        assert_eq!(t.resident_bytes(), 2 * 4 * (3 + 4 + 7));
    }
}
