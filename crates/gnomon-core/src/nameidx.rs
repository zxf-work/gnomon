//! On-disk sorted name index.
//!
//! `resolve_entities` (`read.rs:740`) answers the shipped substring query by
//! opening every node segment and visiting every row — deliberate, because a
//! sorted index cannot accelerate `contains`. This module gives `lookup_name`
//! (`read.rs`, next to `resolve_entities`) an equality/prefix path instead:
//! per node segment, a sorted array of `(casefolded name, row)` pairs, binary
//! searched.
//!
//! **Scope: a deliberate deviation from the index's baseline design.**
//! The index covers every row with a non-empty name, not only
//! currently-canonical ones. A segment is immutable and content-addressed by
//! its footer `body_sha`; an index scoped to "current" rows would be
//! invalidated by every correction and would need a generation-scoped cache
//! exactly like the sweep it replaces. Indexing all stored rows keeps the
//! index as immutable as the segment itself and lets it also answer
//! historical `as_of` probes — a query the blueprint's version could not,
//! because `lookup_name` still filters candidate rows for belief at query
//! time (`believed_at`, the same idiom `resolve_entities` uses at
//! `read.rs:785`).
//!
//! **The casefolding pin.** `str::to_lowercase()` is Rust's full
//! Unicode lowercase mapping, whose output can change if the toolchain's
//! Unicode tables change. [`CASEFOLD_VERSION`] is bumped by hand whenever
//! that mapping might have moved (`tests::casefold_vectors_are_pinned` is the
//! trip-wire); [`unicode_version_string`] records what produced the current
//! build's tables, for a human reading a corrupt store — it is not itself
//! part of the rebuild decision. A stamp mismatch on either the segment
//! `body_sha` or `CASEFOLD_VERSION` rebuilds silently; it never returns a
//! wrong answer (the same rule as `tcsr.py:131-135`: an
//! index is generation-scoped, rebuildable, and never authoritative).
//!
//! **The stamp is the manifest entry's `sha`.** The index reads it off
//! `SegmentEntry::sha`, not the segment footer. At one point no writer filled
//! that field in, so every index was stamped `""` and the `body_sha` half of
//! the check above could not fire: an index met a different segment under
//! the same id — an `idx/` copied between stores, a store recreated at the
//! same path inside one process — and answered for it. Entries written since
//! carry the footer's `body_sha`; legacy entries keep `""`, and for them
//! `lookup_name`'s re-check of every candidate against the segment's own
//! name column is what keeps a stale index from returning a wrong node.
//!
//! File format, one file per node segment at `idx/name/<seg_id:012>.tnx`
//! (the store's own `idx/` subdirectory, already reserved and created for
//! every store — `store.rs::SUBDIRS` — so this is not a new top-level
//! directory, contrary to a literal reading of the design doc's
//! `native/idx/name/...` path):
//!
//! ```text
//! magic "TGNX" | format u32
//! header_len u32 | header JSON
//! keys:    concatenated casefolded UTF-8, sorted by (key, row)
//! offsets: u32[entries+1] into `keys`
//! rows:    u32[entries]                 // segment-local row ids
//! footer:  crc32c(keys) | crc32c(offsets-as-bytes) | crc32c(rows-as-bytes)
//!          | footer_len u32 | magic "TGNE"
//! ```

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::error::{EngineError, Result};
use crate::segment::{Segment, SegmentSource};

const MAGIC: &[u8; 4] = b"TGNX";
const FOOTER_MAGIC: &[u8; 4] = b"TGNE";
const FORMAT: u32 = 1;

/// The casefolding algorithm this build uses. Recorded so a corrupt-store
/// reader can tell what produced the bytes; the *decision* to rebuild is
/// `CASEFOLD_VERSION`'s job, never a string comparison on this.
const CASEFOLD_ALGO: &str = "rust-std-lowercase";

/// Bumped by hand whenever `str::to_lowercase()`'s output might have moved —
/// a toolchain bump that touches Unicode tables, or a deliberate semantics
/// change here. `tests::casefold_vectors_are_pinned` is the trip-wire that
/// should force this bump; it does not do so automatically ("if any changes,
/// CI fails and the implementer bumps `casefold_version` deliberately").
const CASEFOLD_VERSION: u32 = 1;

fn unicode_version_string() -> String {
    let (major, minor, patch) = std::char::UNICODE_VERSION;
    format!("{major}.{minor}.{patch}")
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct NameIndexHeader {
    format: u32,
    seg_id: u64,
    rows: u32,
    body_sha: String,
    casefold: String,
    casefold_version: u32,
    unicode_version: String,
    entries: u32,
}

/// A built (or loaded) per-segment sorted name index. Immutable once
/// constructed — the same guarantee the segment it indexes carries.
pub struct NameIndex {
    header: NameIndexHeader,
    /// Concatenated casefolded keys, `keys[offsets[i]..offsets[i+1]]` is
    /// entry `i`'s key. Entries are sorted by `(key, row)`.
    keys: Vec<u8>,
    offsets: Vec<u32>,
    /// Segment-local row ids, one per entry, in the same order as `keys`.
    rows: Vec<u32>,
}

impl NameIndex {
    /// Heap this index owns, in bytes (memory accounting).
    pub fn heap_bytes(&self) -> u64 {
        (self.keys.capacity()
            + self.offsets.capacity() * std::mem::size_of::<u32>()
            + self.rows.capacity() * std::mem::size_of::<u32>()
            + self.header.body_sha.capacity()
            + self.header.casefold.capacity()
            + self.header.unicode_version.capacity()) as u64
    }

    pub fn seg_id(&self) -> u64 {
        self.header.seg_id
    }

    pub fn body_sha(&self) -> &str {
        &self.header.body_sha
    }

    pub fn is_stale(&self, body_sha: &str) -> bool {
        self.header.body_sha != body_sha || self.header.casefold_version != CASEFOLD_VERSION
    }

    pub fn is_empty(&self) -> bool {
        self.header.entries == 0
    }

    pub fn len(&self) -> usize {
        self.header.entries as usize
    }

    fn key(&self, i: usize) -> &str {
        let (a, b) = (self.offsets[i] as usize, self.offsets[i + 1] as usize);
        // SAFETY-free: built and read back as UTF-8 throughout; a corrupt
        // file is caught by the CRC check at `load`, not here.
        std::str::from_utf8(&self.keys[a..b]).expect("name index keys are valid UTF-8")
    }

    /// Smallest `i` in `lo..self.len()` for which `pred(i)` is false, given
    /// `pred` is true on a prefix of that range and false after (the
    /// `slice::partition_point` contract, hand-rolled here because the
    /// predicate indexes into `self` rather than a plain slice).
    fn partition_point_from(&self, lo: usize, pred: impl Fn(usize) -> bool) -> usize {
        let mut lo = lo;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if pred(mid) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// First index `i` with `self.key(i) >= needle` (a standard lower bound;
    /// `key` is monotone non-decreasing by construction).
    fn lower_bound(&self, needle: &str) -> usize {
        self.partition_point_from(0, |i| self.key(i) < needle)
    }

    /// Segment-local row ids whose casefolded name equals `needle` exactly.
    /// `needle` must already be casefolded by the caller (`str::to_lowercase`,
    /// pinned above) — this function does no casefolding of its own so a
    /// caller cannot accidentally casefold twice.
    pub fn exact(&self, needle: &str) -> &[u32] {
        let lo = self.lower_bound(needle);
        let hi = self.partition_point_from(lo, |i| self.key(i) == needle);
        &self.rows[lo..hi]
    }

    /// Segment-local row ids whose casefolded name starts with `needle`.
    /// Sound because a sorted array's prefix matches are contiguous starting
    /// at the lower bound of `needle` itself: any key `k` with
    /// `k.starts_with(needle)` satisfies `k >= needle`, and once a key in
    /// ascending order stops starting with `needle` no later key can start
    /// with it again.
    pub fn prefix(&self, needle: &str) -> &[u32] {
        if needle.is_empty() {
            return &self.rows[..];
        }
        let lo = self.lower_bound(needle);
        let hi = self.partition_point_from(lo, |i| self.key(i).starts_with(needle));
        &self.rows[lo..hi]
    }
}

/// `<root>/idx/name/<seg_id:012>.tnx` — `root` is the store's own root
/// (already carrying an `idx/` subdirectory reserved for every store,
/// `store.rs::SUBDIRS`), so no new top-level directory is introduced.
pub fn index_path(root: &Path, seg_id: u64) -> PathBuf {
    root.join("idx").join("name").join(format!("{seg_id:012}.tnx"))
}

/// Build a fresh index over every row of `seg` with a non-empty name,
/// exactly as `resolve_entities` reads names (`read.rs:782, :795-798`): from
/// the promoted `name_ref` column only, never `name_of(props)` (the staged
/// path). A segment written before that column was promoted has no
/// `name_ref` column at all, in which case the built index simply has zero
/// entries — correct, since there is nothing typed to match, the same
/// outcome `resolve_entities` reaches for such a segment.
pub fn build<S: SegmentSource>(seg: &Segment<S>, seg_id: u64, body_sha: &str) -> Result<NameIndex> {
    let h = seg.header();
    let rows = h.rows;
    let mut entries: Vec<(String, u32)> = Vec::new();
    if let Ok(name_ref) = seg.u32_column("name_ref") {
        let strings = seg.strings()?;
        for i in 0..rows {
            if let Some(name) = strings.get_opt(name_ref[i as usize])? {
                if !name.is_empty() {
                    entries.push((name.to_lowercase(), i));
                }
            }
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let mut keys = Vec::new();
    let mut offsets = Vec::with_capacity(entries.len() + 1);
    let mut row_ids = Vec::with_capacity(entries.len());
    offsets.push(0u32);
    for (key, row) in &entries {
        keys.extend_from_slice(key.as_bytes());
        offsets.push(keys.len() as u32);
        row_ids.push(*row);
    }

    Ok(NameIndex {
        header: NameIndexHeader {
            format: FORMAT,
            seg_id,
            rows,
            body_sha: body_sha.to_string(),
            casefold: CASEFOLD_ALGO.to_string(),
            casefold_version: CASEFOLD_VERSION,
            unicode_version: unicode_version_string(),
            entries: entries.len() as u32,
        },
        keys,
        offsets,
        rows: row_ids,
    })
}

fn u32_slice_to_bytes(v: &[u32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

fn bytes_to_u32_vec(b: &[u8]) -> Result<Vec<u32>> {
    if !b.len().is_multiple_of(4) {
        return Err(EngineError::corrupt(
            "name index extent length is not a multiple of 4",
        ));
    }
    Ok(b.as_chunks::<4>().0.iter().copied().map(u32::from_le_bytes).collect())
}

/// Write tmp + rename. Callers treat a write failure as
/// non-fatal — the index the caller already built in memory still answers
/// the query that triggered the build; only persistence for the *next* call
/// was lost.
pub fn write(path: &Path, idx: &NameIndex) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|e| EngineError::from(e).at_file(dir))?;
    }
    let header_json =
        serde_json::to_vec(&idx.header).map_err(|e| EngineError::corrupt(e.to_string()))?;
    let offsets_bytes = u32_slice_to_bytes(&idx.offsets);
    let rows_bytes = u32_slice_to_bytes(&idx.rows);

    let mut buf = Vec::new();
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&FORMAT.to_le_bytes());
    buf.extend_from_slice(&(header_json.len() as u32).to_le_bytes());
    buf.extend_from_slice(&header_json);
    buf.extend_from_slice(&idx.keys);
    buf.extend_from_slice(&offsets_bytes);
    buf.extend_from_slice(&rows_bytes);

    let mut footer = Vec::new();
    footer.extend_from_slice(&crc32c::crc32c(&idx.keys).to_le_bytes());
    footer.extend_from_slice(&crc32c::crc32c(&offsets_bytes).to_le_bytes());
    footer.extend_from_slice(&crc32c::crc32c(&rows_bytes).to_le_bytes());
    buf.extend_from_slice(&footer);
    buf.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    buf.extend_from_slice(FOOTER_MAGIC);

    let tmp = path.with_extension("tnx.tmp");
    {
        let mut f = File::create(&tmp).map_err(|e| EngineError::from(e).at_file(&tmp))?;
        f.write_all(&buf)
            .map_err(|e| EngineError::from(e).at_file(&tmp))?;
        f.sync_all()
            .map_err(|e| EngineError::from(e).at_file(&tmp))?;
    }
    fs::rename(&tmp, path).map_err(|e| EngineError::from(e).at_file(path))?;
    Ok(())
}

/// Read and validate a persisted index. Any structural problem — bad magic,
/// truncation, a failed checksum — is `Err`, which the caller treats as
/// "missing": rebuild, never surface a wrong answer.
pub fn load(path: &Path) -> Result<NameIndex> {
    let bytes = fs::read(path).map_err(|e| EngineError::from(e).at_file(path))?;
    let at = |e: EngineError| e.at_file(path);
    if bytes.len() < 20 || &bytes[..4] != MAGIC {
        return Err(at(EngineError::corrupt("not a Gnomon name index (bad magic)")));
    }
    if &bytes[bytes.len() - 4..] != FOOTER_MAGIC {
        return Err(at(EngineError::corrupt(
            "name index has no completion marker — it was never finished being written",
        )));
    }
    let format = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes"));
    if format != FORMAT {
        return Err(at(EngineError::corrupt(format!(
            "name index format {format} is not supported (expected {FORMAT})"
        ))));
    }
    let header_len = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes")) as usize;
    let header_start: usize = 12;
    let header_end = header_start
        .checked_add(header_len)
        .ok_or_else(|| at(EngineError::corrupt("name index header length is impossible")))?;
    let header_bytes = bytes
        .get(header_start..header_end)
        .ok_or_else(|| at(EngineError::corrupt("name index header is truncated")))?;
    let header: NameIndexHeader = serde_json::from_slice(header_bytes)
        .map_err(|e| at(EngineError::corrupt(format!("name index header is invalid: {e}"))))?;

    let footer_len_pos = bytes.len().checked_sub(8).ok_or_else(|| {
        at(EngineError::corrupt("name index is too short to hold a footer"))
    })?;
    let footer_len =
        u32::from_le_bytes(bytes[footer_len_pos..footer_len_pos + 4].try_into().expect("4 bytes"))
            as usize;
    let footer_end = footer_len_pos;
    let footer_start = footer_end
        .checked_sub(footer_len)
        .ok_or_else(|| at(EngineError::corrupt("name index footer length is impossible")))?;
    if footer_len != 12 {
        return Err(at(EngineError::corrupt(
            "name index footer has an unexpected shape",
        )));
    }
    let crc_keys = u32::from_le_bytes(bytes[footer_start..footer_start + 4].try_into().expect("4"));
    let crc_offsets =
        u32::from_le_bytes(bytes[footer_start + 4..footer_start + 8].try_into().expect("4"));
    let crc_rows =
        u32::from_le_bytes(bytes[footer_start + 8..footer_start + 12].try_into().expect("4"));

    let entries = header.entries as usize;
    let keys_start = header_end;
    // offsets is entries+1 u32s; rows is entries u32s; both follow keys.
    // We don't know the keys length a priori except by walking the offsets,
    // which are *after* the keys — so locate the two fixed-size trailers
    // from the footer end backwards instead of scanning forward.
    let rows_bytes_len = entries * 4;
    let offsets_bytes_len = (entries + 1) * 4;
    let rows_start = footer_start
        .checked_sub(rows_bytes_len)
        .ok_or_else(|| at(EngineError::corrupt("name index rows extent is truncated")))?;
    let offsets_start = rows_start
        .checked_sub(offsets_bytes_len)
        .ok_or_else(|| at(EngineError::corrupt("name index offsets extent is truncated")))?;
    if offsets_start < keys_start {
        return Err(at(EngineError::corrupt(
            "name index extents overlap the header — corrupt entry count",
        )));
    }
    let keys = &bytes[keys_start..offsets_start];
    let offsets_raw = &bytes[offsets_start..rows_start];
    let rows_raw = &bytes[rows_start..footer_start];

    if crc32c::crc32c(keys) != crc_keys {
        return Err(at(EngineError::corrupt("name index keys failed their checksum")));
    }
    if crc32c::crc32c(offsets_raw) != crc_offsets {
        return Err(at(EngineError::corrupt(
            "name index offsets failed their checksum",
        )));
    }
    if crc32c::crc32c(rows_raw) != crc_rows {
        return Err(at(EngineError::corrupt("name index rows failed their checksum")));
    }

    let offsets = bytes_to_u32_vec(offsets_raw)?;
    let rows = bytes_to_u32_vec(rows_raw)?;
    if offsets.len() != entries + 1 || rows.len() != entries {
        return Err(at(EngineError::corrupt(
            "name index extent lengths disagree with its own header",
        )));
    }
    if let Some(&last) = offsets.last() {
        if last as usize != keys.len() {
            return Err(at(EngineError::corrupt(
                "name index offsets do not cover the keys extent",
            )));
        }
    }

    Ok(NameIndex {
        header,
        keys: keys.to_vec(),
        offsets,
        rows,
    })
}

/// Process-global cache: `(store root, segment id, segment sha) -> built
/// index`.
///
/// The segment's content address is part of the key, not only checked
/// against the hit's stamp. The root path and the id alone do not name a
/// segment: a store deleted and recreated at the same path inside one process
/// allocates the same ids for different bytes, and early on every stamp was
/// `""`, so the stamp check could not tell the two apart either — the
/// recreated store was answered from the first one's indexes. With the sha in
/// the key such a lookup misses, and a rebuild for a `(root, id)` evicts the
/// entries it supersedes so the cache does not grow with every recreation.
///
/// A legacy entry (`sha == ""`, written by an older engine) still keys on `""`; for
/// it the protection is the one it always had — that a published id is not
/// reused — plus [`crate::store::NativeStore::lookup_name`]'s re-check of
/// every candidate row against the queried name. `core/store.rs` is
/// read-only, so this cache lives here rather than as a
/// `NativeStore` field.
type CacheKey = (PathBuf, u64, String);
static CACHE: OnceLock<Mutex<HashMap<CacheKey, Arc<NameIndex>>>> = OnceLock::new();

fn cache() -> &'static Mutex<HashMap<CacheKey, Arc<NameIndex>>> {
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Cached indexes a read-only handle built and did not write.
///
/// The cache above is process-global, so without this a reader's in-memory
/// build would be served to a later *writer* in the same process as a hit,
/// and the writer — which would have persisted an index it built itself —
/// would never write it. The first writer to hit such an entry persists it
/// and clears the mark. `UNPERSISTED_ANY` keeps the hit path free of a
/// second lock in every process that has never opened a reader.
static UNPERSISTED: OnceLock<Mutex<std::collections::HashSet<CacheKey>>> = OnceLock::new();
static UNPERSISTED_ANY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn unpersisted() -> &'static Mutex<std::collections::HashSet<CacheKey>> {
    UNPERSISTED.get_or_init(|| Mutex::new(std::collections::HashSet::new()))
}

#[cfg(test)]
pub(crate) fn clear_cache_for_tests() {
    cache().lock().expect("name index cache mutex poisoned").clear();
    unpersisted().lock().expect("name index cache mutex poisoned").clear();
}

/// `(entries, bytes)` held by the process-global index cache (memory
/// accounting).
///
/// Process-global and *unbounded*: there is no budget, no LRU, and no gc
/// hook, so an index built for a segment that compaction has since orphaned
/// stays resident for the life of the process. Reported as its own component
/// because no existing stat covers it and because it is keyed by store root,
/// so a process holding several stores sees all of them here.
pub fn cache_stats() -> (usize, u64) {
    let c = cache().lock().expect("name index cache mutex poisoned");
    let bytes = c
        .iter()
        .map(|(k, v)| k.0.as_os_str().len() as u64 + k.2.len() as u64 + v.heap_bytes())
        .sum();
    (c.len(), bytes)
}

/// Insert, dropping any entry for the same `(root, id)` under another sha —
/// it names bytes that no longer live at that path.
fn cache_insert(key: CacheKey, index: Arc<NameIndex>) {
    let mut c = cache().lock().expect("name index cache mutex poisoned");
    c.retain(|k, _| !(k.0 == key.0 && k.1 == key.1 && k.2 != key.2));
    if UNPERSISTED_ANY.load(std::sync::atomic::Ordering::Relaxed) {
        unpersisted()
            .lock()
            .expect("name index cache mutex poisoned")
            .retain(|k| !(k.0 == key.0 && k.1 == key.1 && k.2 != key.2));
    }
    c.insert(key, index);
}

/// Load-or-build-or-rebuild, per the index rule: never authoritative, always
/// reconstructible, and a failure to *persist* never fails the query
/// (`tcsr.py:131-135`).
pub fn load_or_build<S: SegmentSource>(
    root: &Path,
    seg: &Segment<S>,
    seg_id: u64,
    body_sha: &str,
) -> Result<Arc<NameIndex>> {
    load_or_build_persist(root, seg, seg_id, body_sha, true)
}

/// [`load_or_build`] with persistence chosen by the caller: `persist =
/// false` is a read-only handle's, which builds and caches the index
/// in memory and never writes it. A persisted index is still loaded either
/// way — reading is not writing.
pub fn load_or_build_persist<S: SegmentSource>(
    root: &Path,
    seg: &Segment<S>,
    seg_id: u64,
    body_sha: &str,
    persist: bool,
) -> Result<Arc<NameIndex>> {
    load_or_build_with(root, seg_id, body_sha, persist, || build(seg, seg_id, body_sha))
}

/// The caching half of [`load_or_build`], parameterized over how a fresh
/// index is built — split out so the cache/stamp logic (the part with real
/// branches to get wrong) is testable without constructing a live segment.
fn load_or_build_with(
    root: &Path,
    seg_id: u64,
    body_sha: &str,
    persist: bool,
    build_fn: impl FnOnce() -> Result<NameIndex>,
) -> Result<Arc<NameIndex>> {
    let key = (root.to_path_buf(), seg_id, body_sha.to_string());
    let hit = cache()
        .lock()
        .expect("name index cache mutex poisoned")
        .get(&key)
        .cloned();
    if let Some(hit) = hit {
        if !hit.is_stale(body_sha) {
            if persist
                && UNPERSISTED_ANY.load(std::sync::atomic::Ordering::Relaxed)
                && unpersisted()
                    .lock()
                    .expect("name index cache mutex poisoned")
                    .remove(&key)
            {
                // a reader built this in memory: persist it now, as this
                // writer would have had it built the index itself
                if let Err(e) = write(&index_path(root, seg_id), &hit) {
                    eprintln!("gnomon: failed to persist name index for segment {seg_id}: {e}");
                }
            }
            return Ok(hit);
        }
    }
    let path = index_path(root, seg_id);
    if let Ok(loaded) = load(&path) {
        if loaded.header.seg_id == seg_id && !loaded.is_stale(body_sha) {
            let arc = Arc::new(loaded);
            cache_insert(key, arc.clone());
            return Ok(arc);
        }
        // stamp mismatch (stale body_sha, another segment's id, or a bumped
        // casefold_version): fall through and rebuild, never trust the file
        // on disk.
    }
    let built = build_fn()?;
    let arc = Arc::new(built);
    // Best-effort persist. A write failure (full disk, permissions) is
    // logged and swallowed: the in-memory index this call just built still
    // answers the query; only the next process's cold start pays to rebuild
    // it again.
    if persist {
        if let Err(e) = write(&path, &arc) {
            eprintln!("gnomon: failed to persist name index for segment {seg_id}: {e}");
        }
    } else {
        UNPERSISTED_ANY.store(true, std::sync::atomic::Ordering::Relaxed);
        unpersisted()
            .lock()
            .expect("name index cache mutex poisoned")
            .insert(key.clone());
    }
    cache_insert(key, arc.clone());
    Ok(arc)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the cases that historically move under a
    /// Unicode-table change. A change here means `to_lowercase`'s output
    /// moved and `CASEFOLD_VERSION` must be bumped deliberately — this test
    /// exists to make that an explicit, reviewed act rather than a silent
    /// toolchain-bump side effect.
    #[test]
    fn casefold_vectors_are_pinned() {
        let cases: &[(&str, &str)] = &[
            // U+0130 LATIN CAPITAL LETTER I WITH DOT ABOVE: expands to two
            // code points under full Unicode lowercasing.
            ("İ", "i\u{307}"),
            // U+00DF LATIN SMALL LETTER SHARP S: already lowercase, fixed
            // point.
            ("ß", "ß"),
            // U+1E9E LATIN CAPITAL LETTER SHARP S: lowercases to U+00DF, a
            // different code point entirely (not just case-bit flipped).
            ("ẞ", "ß"),
            // Greek sigma: context-sensitive. A bare capital sigma lowercases
            // to the medial form U+03C3, but the *same letter* at the end of
            // a word lowercases to the final form U+03C2 (Unicode
            // SpecialCasing.txt's Final_Sigma condition) — pin both shapes.
            ("Σ", "σ"),
            ("ΑΣ", "ας"),
            ("ς", "ς"),
            // U+01C5 LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON
            // (titlecase): lowercases to U+01C6, not to itself.
            ("ǅ", "ǆ"),
            // U+FFFD REPLACEMENT CHARACTER: a stand-in for the
            // surrogate-adjacent case (Rust strings cannot hold an unpaired
            // surrogate at all, a UTF-8-only invariant) — pin that the
            // replacement character round-trips as a fixed point rather than
            // silently vanishing.
            ("\u{FFFD}", "\u{FFFD}"),
        ];
        for (input, expected) in cases {
            assert_eq!(
                &input.to_lowercase(),
                expected,
                "to_lowercase({input:?}) moved — bump CASEFOLD_VERSION deliberately \
                 and update this vector"
            );
        }
    }

    #[test]
    fn unicode_version_string_is_well_formed() {
        let v = unicode_version_string();
        assert_eq!(v.split('.').count(), 3, "expected MAJOR.MINOR.PATCH, got {v:?}");
    }

    /// Build a [`NameIndex`] directly from `(name, row)` pairs, bypassing a
    /// live `Segment` — the binary-search logic and the write/load/cache
    /// machinery below don't need one, only `build()` (exercised end to end
    /// by `tests/test_name_index.py`, which has a real store to build from).
    fn index_from(seg_id: u64, body_sha: &str, entries: &[(&str, u32)]) -> NameIndex {
        let mut entries: Vec<(String, u32)> =
            entries.iter().map(|(k, r)| (k.to_lowercase(), *r)).collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let mut keys = Vec::new();
        let mut offsets = vec![0u32];
        let mut rows = Vec::new();
        for (k, r) in &entries {
            keys.extend_from_slice(k.as_bytes());
            offsets.push(keys.len() as u32);
            rows.push(*r);
        }
        NameIndex {
            header: NameIndexHeader {
                format: FORMAT,
                seg_id,
                rows: entries.len() as u32,
                body_sha: body_sha.to_string(),
                casefold: CASEFOLD_ALGO.to_string(),
                casefold_version: CASEFOLD_VERSION,
                unicode_version: unicode_version_string(),
                entries: entries.len() as u32,
            },
            keys,
            offsets,
            rows,
        }
    }

    #[test]
    fn exact_finds_every_row_of_a_repeated_key_and_nothing_else() {
        let idx = index_from(
            1,
            "sha",
            &[("Alice", 0), ("bob", 1), ("ALICE", 2), ("alicia", 3), ("Carol", 4)],
        );
        // "Alice", "ALICE" both casefold to "alice"; sorted by (key, row).
        let mut got = idx.exact("alice").to_vec();
        got.sort_unstable();
        assert_eq!(got, vec![0, 2], "casefold-colliding names must both come back");
        assert_eq!(idx.exact("bob"), &[1]);
        assert_eq!(idx.exact("nobody"), &[] as &[u32]);
        assert_eq!(idx.exact(""), &[] as &[u32], "empty string is not a name any row has");
    }

    #[test]
    fn prefix_is_contiguous_and_excludes_non_matching_neighbours() {
        let idx = index_from(
            1,
            "sha",
            &[("ann", 0), ("anna", 1), ("annabel", 2), ("anne", 3), ("bob", 4), ("an", 5)],
        );
        let mut got = idx.prefix("ann").to_vec();
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2, 3], "every *ann-prefixed* row, and only those");
        assert_eq!(idx.prefix("bob"), &[4]);
        assert_eq!(idx.prefix("z"), &[] as &[u32]);
        // an empty prefix matches everything — every row's name starts_with("")
        assert_eq!(idx.prefix("").len(), 6);
    }

    #[test]
    fn empty_index_answers_both_modes_with_nothing() {
        let idx = index_from(1, "sha", &[]);
        assert!(idx.is_empty());
        assert_eq!(idx.exact("x"), &[] as &[u32]);
        assert_eq!(idx.prefix("x"), &[] as &[u32]);
    }

    #[test]
    fn write_then_load_round_trips_every_field() {
        clear_cache_for_tests();
        let dir = std::env::temp_dir().join(format!(
            "gnomon-nameidx-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("seg.tnx");
        let idx = index_from(42, "deadbeef", &[("Zed", 7), ("apple", 3), ("apple", 1)]);
        write(&path, &idx).expect("write");
        let loaded = load(&path).expect("load");
        assert_eq!(loaded.header, idx.header);
        assert_eq!(loaded.keys, idx.keys);
        assert_eq!(loaded.offsets, idx.offsets);
        assert_eq!(loaded.rows, idx.rows);
        assert_eq!(loaded.exact("apple"), &[1, 3]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_truncated_file_is_reported_corrupt_not_silently_wrong() {
        let dir = std::env::temp_dir().join(format!(
            "gnomon-nameidx-test-trunc-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("seg.tnx");
        let idx = index_from(1, "sha", &[("x", 0)]);
        write(&path, &idx).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len() - 5);
        std::fs::write(&path, &bytes).unwrap();
        assert!(load(&path).is_err(), "a truncated index file must not parse as valid");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_bit_flip_fails_its_checksum() {
        let dir = std::env::temp_dir().join(format!(
            "gnomon-nameidx-test-flip-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("seg.tnx");
        let idx = index_from(1, "sha", &[("hello", 0), ("world", 1)]);
        write(&path, &idx).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        // Flip a byte at the first key's first character: computable exactly
        // (unlike a midpoint guess, which could land in the un-checksummed
        // header JSON and pass right through) — `header_len` sits at bytes
        // [8..12], the keys extent starts immediately after the header.
        let header_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let victim = 12 + header_len;
        bytes[victim] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(load(&path).is_err(), "a corrupted body must fail its checksum, not parse silently");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The index rule end to end, without a live segment: a cache hit answers
    /// from memory; a `body_sha` mismatch (the segment moved on, or this is
    /// a different segment id's file entirely) rebuilds instead of trusting
    /// either the cache or the file on disk; a bumped `CASEFOLD_VERSION`
    /// forces the same rebuild even when `body_sha` still matches.
    #[test]
    fn load_or_build_rebuilds_on_any_stamp_mismatch_never_on_a_whim() {
        clear_cache_for_tests();
        let dir = std::env::temp_dir().join(format!(
            "gnomon-nameidx-test-lob-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let build_calls = std::cell::Cell::new(0);
        let make = |sha: &'static str| {
            build_calls.set(build_calls.get() + 1);
            Ok(index_from(1, sha, &[("only", 0)]))
        };

        let first = load_or_build_with(&dir, 1, "sha-a", true, || make("sha-a")).unwrap();
        assert_eq!(build_calls.get(), 1);
        assert_eq!(first.body_sha(), "sha-a");

        // same root, same seg_id, same sha: served from the in-process cache
        let second = load_or_build_with(&dir, 1, "sha-a", true, || make("sha-a")).unwrap();
        assert_eq!(build_calls.get(), 1, "an unchanged stamp must not rebuild");
        assert!(Arc::ptr_eq(&first, &second));

        // the segment moved on (recompaction produced a new body_sha for
        // this id — should not happen for a *live* segment id, but a stale
        // cache entry from a prior process generation must never be trusted
        // past its stamp): must rebuild, not serve the old bytes.
        let third = load_or_build_with(&dir, 1, "sha-b", true, || make("sha-b")).unwrap();
        assert_eq!(build_calls.get(), 2);
        assert_eq!(third.body_sha(), "sha-b");

        // a fresh process would also reload straight off disk without
        // rebuilding, as long as the persisted stamp still matches
        clear_cache_for_tests();
        let fourth = load_or_build_with(&dir, 1, "sha-b", true, || make("sha-b")).unwrap();
        assert_eq!(build_calls.get(), 2, "a matching on-disk file must be reused, not rebuilt");
        assert_eq!(fourth.body_sha(), "sha-b");

        std::fs::remove_dir_all(&dir).ok();
    }
}
