//! Dense-id dictionary.
//!
//! Maps uid strings to dense `u32` ids. Three properties make this a
//! first-class engine component rather than a lookup table:
//!
//! * **Append-only.** Ids are record ordinals. Nothing is ever reordered or
//!   reused, so a dense id printed in a trace is meaningful forever.
//! * **Generation-scoped.** The manifest records how many records are
//!   visible; a reader on an old generation sees a prefix and never a
//!   half-written tail.
//! * **Replay-stable.** Ids are assigned in `ensure_entities` call order,
//!   which the event log fixes — so replaying a log reproduces identical
//!   ids, which is what lets replayed stores match frozen digests.
//!
//! Storage is a string arena plus per-record spans, so a million short uids
//! cost one allocation rather than a million. Lookup is a 64-bit hash probe
//! with full-string verification (never a bare hash comparison).

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{EngineError, Result};

/// Dense ids are u32; this is the hard capacity ceiling.
pub const MAX_ENTITIES: usize = u32::MAX as usize;

#[derive(Clone, Copy)]
struct Span {
    uid_start: u32,
    uid_len: u32,
    label_start: u32,
    label_len: u32,
}

fn hash_uid(uid: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    uid.hash(&mut h);
    h.finish()
}

pub struct Dictionary {
    path: PathBuf,
    arena: String,
    spans: Vec<Span>,
    /// uid hash -> ids (ascending), in [`INDEX_SHARDS`] maps by the hash's
    /// top bits, so a large index is built on several threads. Built by the
    /// sequential load; the parallel load (`GNOMON_STORE_WARM_OPEN`,
    /// `load_fast`) leaves it to the first lookup or to
    /// [`Self::prepare_index`], which a warmed reader runs beside its segment
    /// pass. The same contents either way.
    index: std::sync::OnceLock<Vec<HashMap<u64, Vec<u32>>>>,
    /// Records durably published by the current manifest generation.
    committed_records: u32,
    /// Byte length of the file as of the current manifest generation.
    committed_bytes: u64,
}

impl Dictionary {
    /// Heap this append-only interner owns, in bytes (for memory accounting).
    ///
    /// The dictionary is never evicted or shrunk for the life of a handle, so
    /// this grows monotonically with the number of *distinct* entities a build
    /// has seen — not with rows. `index` is charged with hashbrown's own
    /// overhead (one `(u64, Vec<u32>)` bucket per distinct uid hash, at the
    /// map's current capacity) plus the per-hash `Vec<u32>` allocation, which
    /// is the part a naive `len * size_of` estimate misses.
    pub fn heap_bytes(&self) -> u64 {
        let arena = self.arena.capacity();
        let spans = self.spans.capacity() * std::mem::size_of::<Span>();
        // an index not built yet holds nothing
        let (buckets, dense) = self.index.get().map_or((0, 0), |shards| {
            shards.iter().fold((0, 0), |(b, d), index| {
                (
                    b + index.capacity() * (std::mem::size_of::<u64>() + std::mem::size_of::<Vec<u32>>()),
                    d + index.values().map(|v| v.capacity() * std::mem::size_of::<u32>()).sum::<usize>(),
                )
            })
        });
        (arena + spans + buckets + dense + self.path.as_os_str().len()) as u64
    }

    /// Open (creating if absent), exposing exactly `visible_records`.
    ///
    /// A file longer than `visible_bytes` is either a batch that appended and
    /// died before publishing its manifest, or the *live* writer's in-flight
    /// batch, between its step-3 fsync and its step-5 `CURRENT` flip. The two
    /// are byte-for-byte identical, so open cannot tell them apart — and must
    /// therefore not act on the difference. It reads the visible prefix and
    /// leaves the file alone; `commit_to_disk` reclaims the bytes by writing
    /// over them. **Opening a store never mutates it**, which is what lets a
    /// reader process open one while the writer is committing.
    pub fn open(path: impl Into<PathBuf>, visible_records: u32, visible_bytes: u64) -> Result<Self> {
        let fast = crate::warm::warm_open_on() && visible_records as usize >= fast_load_min();
        Self::open_with_load(path.into(), visible_records, visible_bytes, fast)
    }

    /// [`Self::open`] with the load chosen: `fast` is the parallel load.
    fn open_with_load(path: PathBuf, visible_records: u32, visible_bytes: u64, fast: bool) -> Result<Self> {
        let mut buf = Vec::new();
        if path.exists() {
            File::open(&path)
                .and_then(|mut f| f.read_to_end(&mut buf))
                .map_err(|e| EngineError::from(e).at_file(&path))?;
        }
        let on_disk = buf.len() as u64;
        if on_disk < visible_bytes {
            return Err(EngineError::corrupt(format!(
                "dictionary is shorter than its manifest claims \
                 ({on_disk} bytes on disk, {visible_bytes} expected)"
            ))
            .at_file(&path));
        }
        if on_disk > visible_bytes {
            // an unpublished tail: another batch's, or a live writer's
            buf.truncate(visible_bytes as usize);
        }

        let mut d = Self {
            path,
            arena: String::new(),
            spans: Vec::new(),
            index: std::sync::OnceLock::new(),
            committed_records: 0,
            committed_bytes: visible_bytes,
        };
        if fast {
            d.load_fast(&buf, visible_records)?;
        } else {
            d.load(&buf)?;
        }
        if d.spans.len() as u32 != visible_records {
            return Err(EngineError::corrupt(format!(
                "dictionary holds {} records, manifest claims {visible_records}",
                d.spans.len()
            ))
            .at_file(&d.path));
        }
        d.committed_records = visible_records;
        Ok(d)
    }

    fn load(&mut self, buf: &[u8]) -> Result<()> {
        let mut pos = 0usize;
        let read_u32 = |b: &[u8], p: usize| -> Result<u32> {
            b.get(p..p + 4)
                .map(|s| u32::from_le_bytes(s.try_into().expect("4 bytes")))
                .ok_or_else(|| {
                    EngineError::corrupt("truncated dictionary record header").at_offset(p as u64)
                })
        };
        while pos < buf.len() {
            let start = pos;
            let uid_len = read_u32(buf, pos)? as usize;
            pos += 4;
            let uid = decode(buf, pos, uid_len, start)?;
            pos += uid_len;
            let label_len = read_u32(buf, pos)? as usize;
            pos += 4;
            let label = decode(buf, pos, label_len, start)?;
            pos += label_len;
            self.push(uid, label);
        }
        Ok(())
    }

    /// The parallel load (`GNOMON_STORE_WARM_OPEN`): the record framing in
    /// one sequential pass, the arena as one copy, UTF-8 checked per field
    /// on several threads, and the uid index left to [`Self::prepare_index`]
    /// or the first lookup. The same arena, spans and index as [`Self::load`];
    /// a malformed file is handed to `load`, so the error is the one it
    /// reports.
    fn load_fast(&mut self, buf: &[u8], records: u32) -> Result<()> {
        let n = records as usize;
        let mut spans: Vec<Span> = Vec::with_capacity(n);
        let mut arena: Vec<u8> = Vec::with_capacity(buf.len().saturating_sub(8 * n));
        let mut pos = 0usize;
        let len_at = |p: usize| -> Option<usize> {
            buf.get(p..p + 4).map(|s| u32::from_le_bytes(s.try_into().expect("4 bytes")) as usize)
        };
        while pos < buf.len() {
            let Some(ul) = len_at(pos) else { return self.load(buf) };
            let Some(u) = buf.get(pos + 4..pos + 4 + ul) else { return self.load(buf) };
            let lp = pos + 4 + ul;
            let Some(ll) = len_at(lp) else { return self.load(buf) };
            let Some(l) = buf.get(lp + 4..lp + 4 + ll) else { return self.load(buf) };
            let uid_start = arena.len() as u32;
            arena.extend_from_slice(u);
            let label_start = arena.len() as u32;
            arena.extend_from_slice(l);
            spans.push(Span { uid_start, uid_len: ul as u32, label_start, label_len: ll as u32 });
            pos = lp + 4 + ll;
        }
        // every field is UTF-8 on its own (what `load` checks per field),
        // so the arena, their concatenation, is too
        let ok = std::sync::atomic::AtomicBool::new(true);
        par_chunks(spans.len(), |r| {
            for sp in &spans[r] {
                let u = &arena[sp.uid_start as usize..(sp.uid_start + sp.uid_len) as usize];
                let l = &arena[sp.label_start as usize..(sp.label_start + sp.label_len) as usize];
                if std::str::from_utf8(u).is_err() || std::str::from_utf8(l).is_err() {
                    ok.store(false, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
            }
        });
        if !ok.load(std::sync::atomic::Ordering::Relaxed) {
            return self.load(buf);
        }
        let Ok(arena) = String::from_utf8(arena) else { return self.load(buf) };
        self.arena = arena;
        self.spans = spans;
        self.index = std::sync::OnceLock::new();
        Ok(())
    }

    /// Build the uid index now if it is not built (the uids hashed on
    /// several threads, inserted in id order: the map `push` builds).
    pub fn prepare_index(&self) {
        self.index();
    }

    fn index(&self) -> &Vec<HashMap<u64, Vec<u32>>> {
        self.index.get_or_init(|| {
            let n = self.spans.len();
            if n < fast_load_min() {
                let mut shards: Vec<HashMap<u64, Vec<u32>>> = vec![HashMap::new(); INDEX_SHARDS];
                for id in 0..n as u32 {
                    let h = hash_uid(self.uid(id).expect("id is in range"));
                    shards[shard_of(h)].entry(h).or_default().push(id);
                }
                return shards;
            }
            let width = n.div_ceil(par_threads()).max(1);
            // per contiguous id range, its (hash, id) pairs by shard, ids
            // ascending; then each shard filled from the ranges in order,
            // so every hash's ids stay ascending (what `push` produces)
            let parts: Vec<Vec<Vec<(u64, u32)>>> = std::thread::scope(|sc| {
                let hs: Vec<_> = (0..n)
                    .step_by(width)
                    .map(|start| {
                        sc.spawn(move || {
                            let mut by: Vec<Vec<(u64, u32)>> = vec![Vec::new(); INDEX_SHARDS];
                            for id in start..(start + width).min(n) {
                                let h = hash_uid(self.uid(id as u32).expect("id is in range"));
                                by[shard_of(h)].push((h, id as u32));
                            }
                            by
                        })
                    })
                    .collect();
                hs.into_iter().map(|h| h.join().expect("dictionary index worker")).collect()
            });
            let parts = &parts;
            std::thread::scope(|sc| {
                let hs: Vec<_> = (0..INDEX_SHARDS)
                    .map(|k| {
                        sc.spawn(move || {
                            let len = parts.iter().map(|p| p[k].len()).sum();
                            let mut index: HashMap<u64, Vec<u32>> = HashMap::with_capacity(len);
                            for p in parts {
                                for &(h, id) in &p[k] {
                                    index.entry(h).or_default().push(id);
                                }
                            }
                            index
                        })
                    })
                    .collect();
                hs.into_iter().map(|h| h.join().expect("dictionary index worker")).collect()
            })
        })
    }

    fn index_mut(&mut self) -> &mut Vec<HashMap<u64, Vec<u32>>> {
        self.index();
        self.index.get_mut().expect("built above")
    }

    fn push(&mut self, uid: &str, label: &str) -> u32 {
        // the index of the records already here first, so this one is
        // inserted exactly once
        self.index();
        let id = self.spans.len() as u32;
        let uid_start = self.arena.len() as u32;
        self.arena.push_str(uid);
        let label_start = self.arena.len() as u32;
        self.arena.push_str(label);
        self.spans.push(Span {
            uid_start,
            uid_len: uid.len() as u32,
            label_start,
            label_len: label.len() as u32,
        });
        let h = hash_uid(uid);
        self.index.get_mut().expect("built above")[shard_of(h)].entry(h).or_default().push(id);
        id
    }

    pub fn uid(&self, id: u32) -> Option<&str> {
        let s = self.spans.get(id as usize)?;
        Some(&self.arena[s.uid_start as usize..(s.uid_start + s.uid_len) as usize])
    }

    pub fn label(&self, id: u32) -> Option<&str> {
        let s = self.spans.get(id as usize)?;
        Some(&self.arena[s.label_start as usize..(s.label_start + s.label_len) as usize])
    }

    /// Hash probe, then full-string verify — a 64-bit collision must never
    /// be able to return the wrong entity.
    pub fn dense_id(&self, uid: &str) -> Option<u32> {
        let h = hash_uid(uid);
        self.index()[shard_of(h)]
            .get(&h)?
            .iter()
            .copied()
            .find(|&id| self.uid(id) == Some(uid))
    }

    /// Register `uid` if new, returning its dense id. Staged entries are
    /// visible to reads immediately (read-your-own-writes) but are not
    /// durable until `commit_to_disk`. The label of an existing uid is kept
    /// — first registration wins, matching `ensure_entities`.
    pub fn ensure(&mut self, uid: &str, label: &str) -> Result<u32> {
        if let Some(id) = self.dense_id(uid) {
            return Ok(id);
        }
        if self.spans.len() >= MAX_ENTITIES {
            return Err(EngineError::capacity(format!(
                "dictionary is full: {MAX_ENTITIES} entities is the u32 dense-id ceiling"
            )));
        }
        Ok(self.push(uid, label))
    }

    pub fn len(&self) -> u32 {
        self.spans.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    pub fn committed_records(&self) -> u32 {
        self.committed_records
    }

    pub fn committed_bytes(&self) -> u64 {
        self.committed_bytes
    }

    pub fn has_staged(&self) -> bool {
        self.len() > self.committed_records
    }

    /// Write staged records at the committed offset and fsync. Returns the
    /// new (records, bytes) for the manifest.
    ///
    /// Positioned rather than appended, because the bytes past
    /// `committed_bytes` may be an earlier batch's orphaned tail that `open`
    /// deliberately left in place: the writer owns them, so it overwrites
    /// them and trims whatever is left over. Called *before* the manifest is
    /// written, so a crash here leaves a tail no manifest names, which the
    /// next commit reclaims the same way.
    pub fn commit_to_disk(&mut self) -> Result<(u32, u64)> {
        if !self.has_staged() {
            return Ok((self.committed_records, self.committed_bytes));
        }
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            // never truncate on open: the file already holds every committed
            // record, and this writes only from `committed_bytes` on
            .truncate(false)
            .open(&self.path)
            .map_err(|e| EngineError::from(e).at_file(&self.path))?;
        f.seek(SeekFrom::Start(self.committed_bytes))
            .map_err(|e| EngineError::from(e).at_file(&self.path))?;
        let mut w = BufWriter::new(f);
        let mut written = 0u64;
        for id in self.committed_records..self.len() {
            let uid = self.uid(id).expect("staged id is in range");
            let label = self.label(id).expect("staged id is in range");
            for part in [uid, label] {
                w.write_all(&(part.len() as u32).to_le_bytes())
                    .map_err(|e| EngineError::from(e).at_file(&self.path))?;
                w.write_all(part.as_bytes())
                    .map_err(|e| EngineError::from(e).at_file(&self.path))?;
                written += 4 + part.len() as u64;
            }
        }
        let f = w
            .into_inner()
            .map_err(|e| EngineError::from(e.into_error()).at_file(&self.path))?;
        // trim any leftover of an orphaned tail this batch did not cover, so
        // the file length always equals what the manifest is about to claim
        f.set_len(self.committed_bytes + written)
            .map_err(|e| EngineError::from(e).at_file(&self.path))?;
        f.sync_all()
            .map_err(|e| EngineError::from(e).at_file(&self.path))?;
        self.committed_records = self.len();
        self.committed_bytes += written;
        Ok((self.committed_records, self.committed_bytes))
    }

    /// Drop staged (uncommitted) registrations — the `rollback` path.
    pub fn discard_staged(&mut self) {
        let keep = self.committed_records;
        if self.len() == keep {
            return;
        }
        for id in keep..self.len() {
            let uid = self.uid(id).expect("staged id is in range").to_string();
            let h = hash_uid(&uid);
            let index = &mut self.index_mut()[shard_of(h)];
            if let Some(bucket) = index.get_mut(&h) {
                bucket.retain(|&i| i < keep);
                if bucket.is_empty() {
                    index.remove(&h);
                }
            }
        }
        let arena_len = self
            .spans
            .get(keep as usize)
            .map(|s| s.uid_start as usize)
            .unwrap_or(self.arena.len());
        self.spans.truncate(keep as usize);
        self.arena.truncate(arena_len);
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// The parallel load serves dictionaries of at least this many records
/// (`GNOMON_DICT_FAST_MIN`, default 65,536); smaller ones load sequentially,
/// where threads cost more than they save.
fn fast_load_min() -> usize {
    std::env::var("GNOMON_DICT_FAST_MIN")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(1 << 16)
}

/// The uid index's shard count, and a hash's shard (its top bits).
const INDEX_SHARDS: usize = 64;

fn shard_of(h: u64) -> usize {
    (h >> 58) as usize
}

fn par_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get()).clamp(1, 64)
}

/// `f` over `0..n` in contiguous ranges, one per thread.
fn par_chunks(n: usize, f: impl Fn(std::ops::Range<usize>) + Sync) {
    let width = n.div_ceil(par_threads()).max(1);
    std::thread::scope(|sc| {
        let f = &f;
        for start in (0..n).step_by(width) {
            sc.spawn(move || f(start..(start + width).min(n)));
        }
    });
}

fn decode(buf: &[u8], pos: usize, len: usize, rec_start: usize) -> Result<&str> {
    let bytes = buf.get(pos..pos + len).ok_or_else(|| {
        EngineError::corrupt("truncated dictionary record body").at_offset(rec_start as u64)
    })?;
    std::str::from_utf8(bytes).map_err(|e| {
        EngineError::corrupt(format!("dictionary record is not UTF-8: {e}"))
            .at_offset(rec_start as u64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-dict-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p.join("dict.log")
    }

    #[test]
    fn assigns_ordinal_ids_and_round_trips() {
        let path = tmp("roundtrip");
        let mut d = Dictionary::open(&path, 0, 0).unwrap();
        assert_eq!(d.ensure("n1", "Node").unwrap(), 0);
        assert_eq!(d.ensure("n2", "Node").unwrap(), 1);
        assert_eq!(d.ensure("n1", "Node").unwrap(), 0, "re-registration is idempotent");
        let (records, bytes) = d.commit_to_disk().unwrap();
        assert_eq!(records, 2);

        let reopened = Dictionary::open(&path, records, bytes).unwrap();
        assert_eq!(reopened.uid(0), Some("n1"));
        assert_eq!(reopened.uid(1), Some("n2"));
        assert_eq!(reopened.dense_id("n2"), Some(1));
        assert_eq!(reopened.dense_id("nope"), None);
    }

    #[test]
    fn open_never_writes_to_the_file() {
        // an unpublished tail may belong to a live writer mid-commit, so open
        // reads the visible prefix and touches nothing
        let path = tmp("no-mutation");
        let mut d = Dictionary::open(&path, 0, 0).unwrap();
        d.ensure("n1", "Node").unwrap();
        let (records, bytes) = d.commit_to_disk().unwrap();
        d.ensure("inflight", "Node").unwrap();
        let (_, longer) = d.commit_to_disk().unwrap();

        let visible = Dictionary::open(&path, records, bytes).unwrap();
        assert_eq!(visible.len(), records);
        assert_eq!(visible.dense_id("inflight"), None, "tail is not visible");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            longer,
            "open must leave every byte on disk where it found it"
        );
    }

    #[test]
    fn a_commit_overwrites_an_orphaned_tail_and_trims_it() {
        let path = tmp("overwrite-orphan");
        let mut d = Dictionary::open(&path, 0, 0).unwrap();
        d.ensure("kept", "Node").unwrap();
        let (records, bytes) = d.commit_to_disk().unwrap();
        // a long orphan from a batch that died before publishing
        d.ensure("a-very-long-orphan-uid-nobody-published", "Node").unwrap();
        let (_, orphan_len) = d.commit_to_disk().unwrap();
        assert!(orphan_len > bytes);

        // the next writer reopens at the published generation and commits a
        // *shorter* record: the leftover must go, or the file would decode
        // a record the manifest never counted
        let mut next = Dictionary::open(&path, records, bytes).unwrap();
        next.ensure("n2", "Node").unwrap();
        let (r2, b2) = next.commit_to_disk().unwrap();
        assert!(b2 < orphan_len, "the shorter record must leave a leftover to trim");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), b2);

        let re = Dictionary::open(&path, r2, b2).unwrap();
        assert_eq!(re.len(), 2);
        assert_eq!(re.dense_id("kept"), Some(0));
        assert_eq!(re.dense_id("n2"), Some(1));
    }

    /// committed; returns `(path, records, bytes)`.
    fn written(name: &str, n: u32) -> (PathBuf, u32, u64) {
        let path = tmp(name);
        let mut d = Dictionary::open_with_load(path.clone(), 0, 0, false).unwrap();
        for i in 0..n {
            let uid = match i % 5 {
                0 => format!("café-{i}"),
                1 => format!("{}-{i}", "x".repeat((i % 97) as usize)),
                _ => format!("n{i}"),
            };
            d.ensure(&uid, ["Person", "Post", "中文"][(i % 3) as usize]).unwrap();
        }
        let (r, b) = d.commit_to_disk().unwrap();
        (path, r, b)
    }

    #[test]
    fn the_parallel_load_builds_what_the_sequential_one_does() {
        // past the index's parallel threshold, so both halves run threaded
        let (path, r, b) = written("fastload", (1 << 16) + 4321);
        let seq = Dictionary::open_with_load(path.clone(), r, b, false).unwrap();
        let fast = Dictionary::open_with_load(path.clone(), r, b, true).unwrap();
        assert!(fast.index.get().is_none(), "the parallel load leaves the index to first use");
        assert_eq!(seq.len(), fast.len());
        assert_eq!(seq.arena, fast.arena);
        for id in 0..seq.len() {
            assert_eq!(seq.uid(id), fast.uid(id));
            assert_eq!(seq.label(id), fast.label(id));
        }
        fast.prepare_index();
        let (a, z) = (seq.index.get().unwrap(), fast.index.get().unwrap());
        assert_eq!(a.len(), INDEX_SHARDS);
        for k in 0..INDEX_SHARDS {
            assert_eq!(a[k], z[k], "shard {k}");
        }
        for id in (0..seq.len()).step_by(7) {
            let uid = seq.uid(id).unwrap();
            assert_eq!(fast.dense_id(uid), Some(id));
        }
        assert_eq!(fast.dense_id("absent"), None);
        assert_eq!(seq.committed_records(), fast.committed_records());
        assert_eq!(seq.committed_bytes(), fast.committed_bytes());
    }

    #[test]
    fn a_parallel_load_then_writes_and_rolls_back_as_before() {
        let (path, r, b) = written("fastwrite", 3000);
        // the index unbuilt until `ensure` needs it: an existing uid keeps
        // its id, a new one takes the next ordinal, a rollback forgets it
        let mut d = Dictionary::open_with_load(path.clone(), r, b, true).unwrap();
        assert_eq!(d.ensure("n2", "Other").unwrap(), 2);
        assert_eq!(d.ensure("brand-new", "Node").unwrap(), r);
        d.discard_staged();
        assert_eq!(d.dense_id("brand-new"), None);
        assert_eq!(d.ensure("later", "Node").unwrap(), r);
        let (r2, b2) = d.commit_to_disk().unwrap();
        let re = Dictionary::open_with_load(path, r2, b2, true).unwrap();
        assert_eq!(re.dense_id("later"), Some(r));
        assert_eq!(re.dense_id("n2"), Some(2));
    }

    #[test]
    fn a_malformed_file_fails_the_parallel_load_as_the_sequential_one() {
        let (path, r, b) = written("fastbad", 500);
        let mut bytes = std::fs::read(&path).unwrap();
        // an invalid UTF-8 byte inside the third record's uid
        let mut pos = 0usize;
        for _ in 0..2 {
            let ul = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            let ll = u32::from_le_bytes(bytes[pos + 4 + ul..pos + 8 + ul].try_into().unwrap()) as usize;
            pos += 8 + ul + ll;
        }
        bytes[pos + 4] = 0xff;
        std::fs::write(&path, &bytes).unwrap();
        let msg = |fast| match Dictionary::open_with_load(path.clone(), r, b, fast) {
            Ok(_) => panic!("a malformed dictionary must not open"),
            Err(e) => (e.category, e.to_string()),
        };
        assert_eq!(msg(false), msg(true));
        // and a truncated one
        std::fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
        let msg = |fast| match Dictionary::open_with_load(path.clone(), r, b - 3, fast) {
            Ok(_) => panic!("a truncated dictionary must not open"),
            Err(e) => (e.category, e.to_string()),
        };
        assert_eq!(msg(false), msg(true));
    }

    #[test]
    fn first_label_wins() {
        let path = tmp("label");
        let mut d = Dictionary::open(&path, 0, 0).unwrap();
        d.ensure("n1", "Node").unwrap();
        d.ensure("n1", "Other").unwrap();
        assert_eq!(d.label(0), Some("Node"));
    }

    #[test]
    fn unicode_uids_survive_the_arena() {
        let path = tmp("unicode");
        let mut d = Dictionary::open(&path, 0, 0).unwrap();
        d.ensure("café", "Node").unwrap();
        d.ensure("中文", "Node").unwrap();
        let (r, b) = d.commit_to_disk().unwrap();
        let re = Dictionary::open(&path, r, b).unwrap();
        assert_eq!(re.dense_id("café"), Some(0));
        assert_eq!(re.dense_id("中文"), Some(1));
    }

    #[test]
    fn discard_staged_restores_committed_state() {
        let path = tmp("rollback");
        let mut d = Dictionary::open(&path, 0, 0).unwrap();
        d.ensure("kept", "Node").unwrap();
        d.commit_to_disk().unwrap();
        d.ensure("dropped", "Node").unwrap();
        assert_eq!(d.len(), 2);
        d.discard_staged();
        assert_eq!(d.len(), 1);
        assert_eq!(d.dense_id("dropped"), None, "rolled-back uid must not resolve");
        assert_eq!(d.dense_id("kept"), Some(0));
        // and the id is free for a later, different entity — ordinals stay dense
        assert_eq!(d.ensure("later", "Node").unwrap(), 1);
    }

    #[test]
    fn orphaned_tail_is_invisible_on_open() {
        let path = tmp("orphan");
        let mut d = Dictionary::open(&path, 0, 0).unwrap();
        d.ensure("committed", "Node").unwrap();
        let (records, bytes) = d.commit_to_disk().unwrap();
        // simulate a batch that appended and died before publishing a manifest
        d.ensure("orphan", "Node").unwrap();
        d.commit_to_disk().unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() > bytes);

        // the manifest's byte count is the sole authority on what is visible;
        // the tail is ignored rather than removed (a live writer's tail looks
        // identical, and open must not mutate the store)
        let re = Dictionary::open(&path, records, bytes).unwrap();
        assert_eq!(re.len(), 1);
        assert_eq!(re.dense_id("orphan"), None);
        assert_eq!(re.committed_bytes(), bytes);
    }

    #[test]
    fn short_file_is_corruption_not_silent_truncation() {
        let path = tmp("short");
        let mut d = Dictionary::open(&path, 0, 0).unwrap();
        d.ensure("n1", "Node").unwrap();
        let (records, bytes) = d.commit_to_disk().unwrap();
        // no Debug on Dictionary (it owns the whole arena), so match instead
        let err = match Dictionary::open(&path, records, bytes + 999) {
            Ok(_) => panic!("a manifest claiming more bytes than exist must not open"),
            Err(e) => e,
        };
        assert_eq!(err.category, crate::error::Category::Corrupt);
    }

    #[test]
    fn ids_are_replay_stable_for_a_fixed_call_order() {
        let order = ["b", "a", "c", "a", "d"];
        let ids_of = |path: PathBuf| {
            let mut d = Dictionary::open(path, 0, 0).unwrap();
            order
                .iter()
                .map(|u| d.ensure(u, "Node").unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids_of(tmp("stable-a")), ids_of(tmp("stable-b")));
        assert_eq!(ids_of(tmp("stable-c")), vec![0, 1, 2, 1, 3]);
    }
}
