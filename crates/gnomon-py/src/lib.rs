//! PyO3 bindings for the Gnomon native storage engine.
//!
//! This crate is deliberately thin: it owns the Python boundary (type
//! conversion, buffer ownership, error mapping) and nothing else. All engine
//! logic lives in `gnomon-core`, which knows nothing about Python. If a
//! decision has to be made here, it belongs one layer down.
//!
//! **Boundary rule**: calls are coarse — one
//! crossing per batch, never per row. Every method that moves rows takes or
//! returns a *dict of columns*, not a list of records, so a 50,000-event
//! ingest chunk is one call rather than fifty thousand.
//!
//! Reads live on `NativeStore` rather
//! than on a separate `Snapshot` handle. The store's reads already union
//! rows staged in the open batch, which is what `apply_ops` needs, and a
//! single-writer store has exactly one live view. A pinned-generation
//! `Snapshot` becomes meaningful when concurrent readers arrive; adding it
//! now would be a second way to say the same thing.

use std::collections::HashMap;
use std::sync::Mutex;

use numpy::{IntoPyArray, PyArray1, PyReadonlyArray1};
use pyo3::exceptions::{PyIOError, PyKeyError, PyOverflowError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use gnomon_core::derive::Id96;
use gnomon_core::error::{Category, EngineError};
use gnomon_core::read::{
    Belief, EdgeVersionOut, NodeVersionOut, VersionFilter, VersionRows,
};
use gnomon_core::row::{EdgeRow, NodeRow, RowKind};
use gnomon_core::scan::ScanRequest;
use gnomon_core::staging::PartitionMap;
use gnomon_core::store::{
    segment_id_of, NativeStore as CoreStore, NeighborDir, NeighborRows,
};
use gnomon_core::ingest_phase::{self as ip, Timer, G};
use gnomon_core::OPEN_END;

/// Map an engine error onto a Python exception.
///
/// The category is kept as the message prefix (`not_found: ...`) so the
/// Python adapter can translate into `gnomon.core.errors` precisely rather
/// than pattern-matching prose.
fn err(e: EngineError) -> PyErr {
    let msg = e.to_string();
    match e.category {
        Category::NotFound => PyKeyError::new_err(msg),
        Category::Capacity => PyOverflowError::new_err(msg),
        Category::Io => {
            // The message stays exactly the engine's prose (other layers key
            // off it), but `.errno` is attached when the originating
            // `io::Error` carried a raw OS error code, so the
            // reopen-on-ENOENT net at the Python adapter boundary can tell
            // "a pinned reader's segment vanished under gc" apart from any
            // other I/O failure without parsing `msg`.
            let py_err = PyIOError::new_err(msg);
            if let Some(errno) = e.raw_os_error {
                Python::attach(|py| {
                    let _ = py_err.value(py).setattr("errno", errno);
                });
            }
            py_err
        }
        Category::Corrupt | Category::Invariant => PyRuntimeError::new_err(msg),
    }
}

type Res<T> = Result<T, PyErr>;

/// `(rows_total, page)` of `resolve_entities_page`.
type ResolvePage = (usize, Vec<(String, u8, String, String)>);

fn kind_of(kind: &str) -> Res<RowKind> {
    match kind {
        "node" => Ok(RowKind::Node),
        "edge" => Ok(RowKind::Edge),
        other => Err(PyRuntimeError::new_err(format!(
            "unknown row kind {other:?}; expected 'node' or 'edge'"
        ))),
    }
}

fn id96(hex: &str) -> Res<Id96> {
    Id96::from_hex(hex).map_err(err)
}

/// Columns in, columns out. Node versions as a dict of equal-length lists.
fn nodes_to_py(py: Python<'_>, rows: Vec<NodeVersionOut>) -> Res<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("vid", rows.iter().map(|r| r.vid.clone()).collect::<Vec<_>>())?;
    d.set_item("uid", rows.iter().map(|r| r.uid.clone()).collect::<Vec<_>>())?;
    d.set_item("label", rows.iter().map(|r| r.label.clone()).collect::<Vec<_>>())?;
    d.set_item("vt_s", rows.iter().map(|r| r.vt_s).collect::<Vec<_>>())?;
    d.set_item("vt_e", rows.iter().map(|r| r.vt_e).collect::<Vec<_>>())?;
    d.set_item("tt_s", rows.iter().map(|r| r.tt_s).collect::<Vec<_>>())?;
    d.set_item("tt_e", rows.iter().map(|r| r.tt_e).collect::<Vec<_>>())?;
    d.set_item("props", rows.iter().map(|r| r.props.clone()).collect::<Vec<_>>())?;
    d.set_item("source", rows.iter().map(|r| r.source.clone()).collect::<Vec<_>>())?;
    d.set_item(
        "provenance_ref",
        rows.iter().map(|r| r.provenance_ref.clone()).collect::<Vec<_>>(),
    )?;
    Ok(d.into())
}

fn edges_to_py(py: Python<'_>, rows: Vec<EdgeVersionOut>) -> Res<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("eid", rows.iter().map(|r| r.eid.clone()).collect::<Vec<_>>())?;
    d.set_item("vid", rows.iter().map(|r| r.vid.clone()).collect::<Vec<_>>())?;
    d.set_item("src", rows.iter().map(|r| r.src.clone()).collect::<Vec<_>>())?;
    d.set_item("dst", rows.iter().map(|r| r.dst.clone()).collect::<Vec<_>>())?;
    d.set_item(
        "rel_type",
        rows.iter().map(|r| r.rel_type.clone()).collect::<Vec<_>>(),
    )?;
    d.set_item("disc", rows.iter().map(|r| r.disc.clone()).collect::<Vec<_>>())?;
    d.set_item("vt_s", rows.iter().map(|r| r.vt_s).collect::<Vec<_>>())?;
    d.set_item("vt_e", rows.iter().map(|r| r.vt_e).collect::<Vec<_>>())?;
    d.set_item("tt_s", rows.iter().map(|r| r.tt_s).collect::<Vec<_>>())?;
    d.set_item("tt_e", rows.iter().map(|r| r.tt_e).collect::<Vec<_>>())?;
    d.set_item("props", rows.iter().map(|r| r.props.clone()).collect::<Vec<_>>())?;
    d.set_item("source", rows.iter().map(|r| r.source.clone()).collect::<Vec<_>>())?;
    d.set_item(
        "provenance_ref",
        rows.iter().map(|r| r.provenance_ref.clone()).collect::<Vec<_>>(),
    )?;
    Ok(d.into())
}

/// Columnar arguments for one staged batch of edge versions.
#[derive(FromPyObject)]
#[pyo3(from_item_all)]
struct EdgeCols {
    vid: Vec<String>,
    src: Vec<String>,
    dst: Vec<String>,
    rel_type: Vec<String>,
    disc: Vec<String>,
    vt_s: Vec<i64>,
    vt_e: Vec<i64>,
    tt_s: Vec<i64>,
    props: Vec<String>,
    source: Vec<String>,
    provenance_ref: Vec<Option<String>>,
}

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
struct NodeCols {
    vid: Vec<String>,
    uid: Vec<String>,
    label: Vec<String>,
    vt_s: Vec<i64>,
    vt_e: Vec<i64>,
    tt_s: Vec<i64>,
    props: Vec<String>,
    source: Vec<String>,
    provenance_ref: Vec<Option<String>>,
}

#[pyclass(module = "gnomon._engine")]
pub struct NativeStore {
    inner: CoreStore,
    /// Event-log position of the batch being committed, staged by
    /// `set_event_cursor` and consumed by the next `commit`. `None` means
    /// the caller did not say — the manifest then inherits its parent's
    /// cursor unchanged, which is exactly right for cursorless callers
    /// (legacy stores keep their empty chain, compaction keeps the real one).
    pending_cursor: Option<gnomon_core::manifest::EventLogRef>,
    /// Whole-store cache for `all_versions_page`'s edge kind (the paged
    /// digest producer). See that
    /// method's doc comment for why a page API caches the whole-store
    /// `Vec` here across calls instead of re-deriving it per page.
    edge_version_page_cache: Mutex<Option<Vec<EdgeVersionOut>>>,
    /// Node-kind counterpart of `edge_version_page_cache`.
    node_version_page_cache: Mutex<Option<Vec<NodeVersionOut>>>,
}

#[pymethods]
impl NativeStore {
    /// `read_only=True` is `NativeStore::open_read_only`: a reader
    /// that never writes under the store directory.
    #[new]
    #[pyo3(signature = (path, read_only = false))]
    fn new(path: &str, read_only: bool) -> Res<Self> {
        let inner = if read_only {
            CoreStore::open_read_only(path)
        } else {
            CoreStore::open(path)
        };
        Ok(Self {
            inner: inner.map_err(err)?,
            pending_cursor: None,
            edge_version_page_cache: Mutex::new(None),
            node_version_page_cache: Mutex::new(None),
        })
    }

    fn close(&mut self) {
        // segments are mmap'd read-only and dropped with the store; there is
        // no writer handle to flush, because a commit is already durable
    }

    fn generation(&self) -> u64 {
        self.inner.generation()
    }

    // --- batch lifecycle ------------------------------------------------ //

    fn begin(&mut self, tt: i64) -> Res<()> {
        let t = Timer::start();
        let out = self.inner.begin(tt).map_err(err);
        t.stop(G::PyBegin);
        ip::add(G::PyCalls, 1);
        out
    }

    fn commit(&mut self) -> Res<u64> {
        let t = Timer::start();
        let out = self.commit_body();
        t.stop(G::PyCommit);
        ip::add(G::PyCalls, 1);
        out
    }

    /// Process-wide ingest-phase totals (`gnomon_core::ingest_phase`)
    /// as a dict of nanosecond totals and counts: the global slots by name,
    /// the context-indexed ones as `<context>.<slot>`. Empty when the
    /// extension was built with the timers compiled out. Observation only.
    fn ingest_phase_stats<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict> {
        let d = PyDict::new(py);
        for (k, v) in ip::snapshot() {
            d.set_item(k, v).expect("fresh dict accepts u64 values");
        }
        d
    }

    /// Zero the ingest-phase totals. Instrumentation only.
    fn reset_ingest_phase_stats(&self) {
        ip::reset();
    }

    fn rollback(&mut self) -> Res<()> {
        // a cursor staged for the failed batch must not leak into the next
        // commit: the batch published nothing, so the log suffix it covered
        // is still unapplied
        self.pending_cursor = None;
        self.inner.rollback().map_err(err)
    }

    /// Stage the event-log cursor the next `commit` will record in its
    /// manifest: `offset` points immediately past the newline of the last
    /// applied record; `chain` is the rolling hash over the log prefix up
    /// to `offset` (manifest.rs `EventLogRef`). Together they are the
    /// durable replay cursor suffix recovery starts from.
    fn set_event_cursor(&mut self, offset: u64, chain: String) {
        self.pending_cursor =
            Some(gnomon_core::manifest::EventLogRef { offset, chain });
    }

    /// `(offset, chain)` the current generation recorded — the replay
    /// cursor. A store written before cursors existed reports chain `""`.
    fn event_cursor(&self) -> (u64, String) {
        let e = &self.inner.manifest().event_log;
        (e.offset, e.chain.clone())
    }

    /// `(entries, accounted_bytes, budget, evictions)` of the byte-budget
    /// segment cache — receipts for capped-memory runs.
    fn segment_cache_stats(&self) -> (usize, u64, Option<u64>, u64) {
        self.inner.segment_cache_stats()
    }

    /// How this handle's segment opens resolved, as a dict: `cached_hits`,
    /// `cached_misses`, `uncached_opens`, `decoded_bytes`, `touched_bytes`,
    /// `selected_rows`, `first_touch_verifies`, `pinned_source_hits`,
    /// `fresh_map_loads`.
    ///
    /// `segment_cache_stats` reports residency; this reports traffic. Both
    /// read zero entries when the query path never inserts *and* when
    /// something evicts everything it inserts, so residency alone cannot
    /// tell those apart — these counters can. Taken as a delta around one
    /// query, `touched_bytes` and `selected_rows` are that query's read
    /// amplification.
    fn open_path_stats<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict> {
        let (hits, misses, uncached, decoded, touched, rows, verifies, pinned, fresh) =
            self.inner.open_path_stats();
        let d = PyDict::new(py);
        for (k, v) in [
            ("cached_hits", hits),
            ("cached_misses", misses),
            ("uncached_opens", uncached),
            ("decoded_bytes", decoded),
            ("touched_bytes", touched),
            ("selected_rows", rows),
            ("first_touch_verifies", verifies),
            ("pinned_source_hits", pinned),
            ("fresh_map_loads", fresh),
        ] {
            d.set_item(k, v).expect("fresh dict accepts u64 values");
        }
        d
    }

    /// Per-phase read-path timers as a dict of nanosecond totals and counts
    /// — `agg_*` for the aggregate path's own phases, `sel_*` for the scan
    /// select stage, `fold_*` for the partial fold. See the Rust `phase`
    /// module for what each slot covers.
    ///
    /// These are **process-global**, not per handle: the counters beside them
    /// are per handle, and a harness reading a per-handle counter off a
    /// handle that never ran the query reports zeros. A global has no wrong
    /// handle to read. Bracket one query between `reset_phase_stats()` and
    /// this call to get that query's split; `*_max_ns` is a running maximum
    /// and is only meaningful inside such a bracket.
    fn phase_stats<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict> {
        let d = PyDict::new(py);
        for (k, v) in self.inner.phase_stats() {
            d.set_item(k, v).expect("fresh dict accepts u64 values");
        }
        d
    }

    /// Zero the per-phase timers. Instrumentation only.
    fn reset_phase_stats(&self) {
        self.inner.reset_phase_stats();
    }

    /// The current generation's segments, as a list of dicts: `lane`,
    /// `file`, `rows`, `key_lo`, `key_hi`, `vt_min`, `vt_max`.
    ///
    /// The keys are the full composite `(vt_s, vid)` boundaries the manifest
    /// records. With them, "what fraction of this layout's segments could a
    /// key-selective query prune" is measurable from the manifest alone,
    /// without opening anything — which is what separates a layout whose
    /// segments hold disjoint key ranges from one whose segments each span
    /// the whole range.
    fn segment_layout<'py>(&self, py: Python<'py>) -> Res<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        for (lane, file, n, key_lo, key_hi, vt_min, vt_max) in self.inner.segment_layout() {
            let d = PyDict::new(py);
            d.set_item("lane", lane)?;
            d.set_item("file", file)?;
            d.set_item("rows", n)?;
            d.set_item("key_lo", (key_lo.0, key_lo.1))?;
            d.set_item("key_hi", (key_hi.0, key_hi.1))?;
            d.set_item("vt_min", vt_min)?;
            d.set_item("vt_max", vt_max)?;
            out.append(d)?;
        }
        Ok(out)
    }

    /// `(by_identity_entries, by_vid_entries, indexed_segments)` of one row
    /// kind's identity postings — receipts that compaction cycles
    /// are not multiplying the postings without bound.
    fn postings_stats(&self, kind: &str) -> Res<(usize, usize, usize)> {
        let k = kind_of(kind)?;
        Ok(self.inner.postings_stats(k))
    }

    /// Where the last commit spent its microseconds, by phase, or `None`
    /// before this handle has committed anything. Instrumentation for the
    /// singleton-write floor: the split says whether the cost is the fsyncs
    /// or the full manifest each generation rewrites.
    fn last_commit_phases<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyDict>> {
        let p = self.inner.last_commit_phases()?;
        let d = PyDict::new(py);
        for (k, v) in [
            ("capture_us", p.capture_us),
            ("seal_us", p.seal_us),
            ("closes_us", p.closes_us),
            ("stats_us", p.stats_us),
            ("dict_us", p.dict_us),
            ("digest_us", p.digest_us),
            ("debug_verify_us", p.debug_verify_us),
            ("delta_build_us", p.delta_build_us),
            ("manifest_us", p.manifest_us),
            ("current_us", p.current_us),
            // The identity filters of the segments this commit
            // sealed. Its own phase because the <=5% write-path band is
            // scored on exactly this number.
            ("vidfilter_us", p.vidfilter_us),
            ("total_us", p.total_us),
            ("manifest_bytes", p.manifest_bytes),
            ("segments_named", p.segments_named),
            // 1 when step 4 wrote a full checkpoint rather than a delta, so a
            // harness can separate the amortized every-Kth-commit cost from
            // the per-commit one instead of averaging them together
            ("manifest_checkpoint", u64::from(p.manifest_checkpoint)),
        ] {
            d.set_item(k, v).expect("fresh dict accepts u64 values");
        }
        Some(d)
    }

    /// Where this handle's `open` call spent its microseconds, by phase.
    /// Mirrors `last_commit_phases` on the open side: open could not score
    /// "manifest-chain open <= 70 ms" because open had no
    /// internal phase timer exposed to Python. Unlike `last_commit_phases`
    /// this is never `None` — every live handle came from a successful open.
    fn open_phase_us<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict> {
        let p = self.inner.open_phases();
        let d = PyDict::new(py);
        for (k, v) in [
            ("checkpoint_read_parse_us", p.checkpoint_read_parse_us),
            ("merkle_verify_us", p.merkle_verify_us),
            ("state_build_us", p.state_build_us),
            ("delta_replay_us", p.delta_replay_us),
            ("delta_count", p.delta_count),
            ("dictionary_open_us", p.dictionary_open_us),
            ("other_us", p.other_us),
            ("total_us", p.total_us),
            // the format of the checkpoint the chain resolved to, so a
            // harness can tell a fused format-3 Merkle build from an older
            // chain's O(n) whole-document rehash without re-deriving it
            ("chain_format", u64::from(p.chain_format)),
        ] {
            d.set_item(k, v).expect("fresh dict accepts u64 values");
        }
        d
    }

    fn in_batch(&self) -> bool {
        self.inner.in_batch()
    }

    // --- dictionary ----------------------------------------------------- //

    fn ensure_entities(&mut self, uids: Vec<String>, labels: Vec<String>) -> Res<()> {
        if uids.len() != labels.len() {
            return Err(PyRuntimeError::new_err(
                "ensure_entities: uids and labels differ in length",
            ));
        }
        let t = Timer::start();
        for (u, l) in uids.iter().zip(&labels) {
            self.inner.ensure_entity(u, l).map_err(err)?;
        }
        t.stop(G::PyEnsureEntities);
        ip::add(G::PyCalls, 1);
        ip::add(G::DictEnsureCalls, uids.len() as u64);
        Ok(())
    }

    fn dense_ids<'py>(&self, py: Python<'py>, uids: Vec<String>) -> Res<Bound<'py, PyArray1<i64>>> {
        let mut out = Vec::with_capacity(uids.len());
        for u in &uids {
            match self.inner.dict().dense_id(u) {
                Some(id) => out.push(id as i64),
                None => return Err(PyKeyError::new_err(format!("unknown uid: {u}"))),
            }
        }
        Ok(out.into_pyarray(py))
    }

    fn uids_for(&self, ids: Vec<i64>) -> Res<Vec<String>> {
        ids.iter()
            .map(|&i| {
                self.inner
                    .dict()
                    .uid(i as u32)
                    .map(str::to_string)
                    .ok_or_else(|| PyKeyError::new_err(format!("unknown dense id: {i}")))
            })
            .collect()
    }

    fn num_entities(&self) -> u32 {
        self.inner.dict().len()
    }

    // --- staging (one call per insert batch) ----------------------------- //

    fn stage_edges(&mut self, cols: EdgeCols) -> Res<()> {
        let body = Timer::start();
        let n = cols.vid.len();
        // one row in STAGE_SAMPLE_EVERY is timed in two halves (dictionary
        // lookup, then row build + push), so the measured body can be split
        // without a timer on every row
        let (mut s_lookup, mut s_build, mut s_rows) = (0u64, 0u64, 0u64);
        for i in 0..n {
            let sample = ip::ENABLED && i % ip::STAGE_SAMPLE_EVERY == 0;
            let t_lookup = sample.then(Timer::start);
            let src_id = self
                .inner
                .dict()
                .dense_id(&cols.src[i])
                .ok_or_else(|| PyKeyError::new_err(format!("unknown uid: {}", cols.src[i])))?;
            let dst_id = self
                .inner
                .dict()
                .dense_id(&cols.dst[i])
                .ok_or_else(|| PyKeyError::new_err(format!("unknown uid: {}", cols.dst[i])))?;
            let t_build = t_lookup.map(|t| {
                s_lookup += t.ns();
                Timer::start()
            });
            self.inner
                .stage_edge(EdgeRow {
                    vid: id96(&cols.vid[i])?,
                    src_id,
                    dst_id,
                    rel_type: cols.rel_type[i].clone(),
                    disc: cols.disc[i].clone(),
                    vt_s: cols.vt_s[i],
                    vt_e: cols.vt_e[i],
                    tt_s: cols.tt_s[i],
                    props: cols.props[i].clone(),
                    source: cols.source[i].clone(),
                    provenance_ref: cols.provenance_ref[i].clone(),
                })
                .map_err(err)?;
            if let Some(t) = t_build {
                s_build += t.ns();
                s_rows += 1;
            }
        }
        ip::add(G::StageSampledLookupNs, s_lookup);
        ip::add(G::StageSampledBuildNs, s_build);
        ip::add(G::StageSampledRows, s_rows);
        ip::add(G::StageRows, n as u64);
        body.stop(G::PyStageEdges);
        ip::add(G::PyCalls, 1);
        Ok(())
    }

    fn stage_nodes(&mut self, cols: NodeCols) -> Res<()> {
        let body = Timer::start();
        let n = cols.vid.len();
        let (mut s_lookup, mut s_build, mut s_rows) = (0u64, 0u64, 0u64);
        for i in 0..n {
            let sample = ip::ENABLED && i % ip::STAGE_SAMPLE_EVERY == 0;
            let t_lookup = sample.then(Timer::start);
            let uid_id = self
                .inner
                .dict()
                .dense_id(&cols.uid[i])
                .ok_or_else(|| PyKeyError::new_err(format!("unknown uid: {}", cols.uid[i])))?;
            let t_build = t_lookup.map(|t| {
                s_lookup += t.ns();
                Timer::start()
            });
            self.inner
                .stage_node(NodeRow {
                    vid: id96(&cols.vid[i])?,
                    uid_id,
                    label: cols.label[i].clone(),
                    vt_s: cols.vt_s[i],
                    vt_e: cols.vt_e[i],
                    tt_s: cols.tt_s[i],
                    props: cols.props[i].clone(),
                    source: cols.source[i].clone(),
                    provenance_ref: cols.provenance_ref[i].clone(),
                })
                .map_err(err)?;
            if let Some(t) = t_build {
                s_build += t.ns();
                s_rows += 1;
            }
        }
        ip::add(G::StageSampledLookupNs, s_lookup);
        ip::add(G::StageSampledBuildNs, s_build);
        ip::add(G::StageSampledRows, s_rows);
        ip::add(G::StageRows, n as u64);
        body.stop(G::PyStageNodes);
        ip::add(G::PyCalls, 1);
        Ok(())
    }

    /// Stage `ops[op_index]` of an event-log record (an
    /// `ingest_events` op) at `tt` in the engine, exactly as the Python bulk
    /// row build would. `(edge rows, node rows)` when staged; `None` when the
    /// op is not in the accepted shape or `GNOMON_INGEST_RAW=0`, with nothing
    /// changed, and the caller then takes the Python path.
    fn stage_ingest_events_raw(
        &mut self,
        record: &[u8],
        op_index: usize,
        tt: i64,
    ) -> Res<Option<(usize, usize)>> {
        let t = Timer::start();
        let r = self
            .inner
            .stage_ingest_events_raw(record, op_index, tt)
            .map_err(err);
        t.stop(G::PyIngestRaw);
        ip::add(G::PyCalls, 1);
        r
    }

    /// Close committed (or same-batch) versions.
    ///
    /// `vt_s`, when given, is the valid-time start of each row in `vids`,
    /// positionally — the first half of the physical sort key, which lets the
    /// engine locate the row by a manifest range prune and a binary search
    /// instead of a whole-store vid postings map. It is an optimisation and
    /// never an answer: omitting it, or
    /// passing a shorter list, finds exactly the same rows by searching every
    /// segment. `gnomon/storage/base.py::_supersede_edges` holds the whole
    /// version it is superseding, so the Python correction path always has
    /// it.
    #[pyo3(signature = (kind, vids, tt_e, vt_s = None))]
    fn stage_closes(
        &mut self,
        kind: &str,
        vids: Vec<String>,
        tt_e: i64,
        vt_s: Option<Vec<i64>>,
    ) -> Res<()> {
        let t = Timer::start();
        let k = kind_of(kind)?;
        for (i, v) in vids.iter().enumerate() {
            let at = vt_s.as_ref().and_then(|t| t.get(i).copied());
            self.inner
                .close_version_at(k, id96(v)?, at, tt_e)
                .map_err(err)?;
        }
        t.stop(G::PyStageCloses);
        ip::add(G::PyCalls, 1);
        Ok(())
    }

    /// Drop versions this batch staged and has already replaced.
    fn stage_retires(&mut self, kind: &str, vids: Vec<String>) -> Res<()> {
        let t = Timer::start();
        let k = kind_of(kind)?;
        for v in &vids {
            self.inner.retire_version(k, id96(v)?).map_err(err)?;
        }
        t.stop(G::PyStageRetires);
        ip::add(G::PyCalls, 1);
        Ok(())
    }

    // --- reads ----------------------------------------------------------- //

    fn all_versions(&self, py: Python<'_>, kind: &str) -> Res<Py<PyDict>> {
        match kind_of(kind)? {
            RowKind::Edge => edges_to_py(py, self.inner.all_edge_versions().map_err(err)?),
            RowKind::Node => nodes_to_py(py, self.inner.all_node_versions().map_err(err)?),
        }
    }

    #[pyo3(signature = (kind, identity, as_of_tt = OPEN_END))]
    fn believed(
        &self,
        py: Python<'_>,
        kind: &str,
        identity: &str,
        as_of_tt: i64,
    ) -> Res<Py<PyDict>> {
        let t = Timer::start();
        let out = match kind_of(kind)? {
            RowKind::Edge => edges_to_py(
                py,
                self.inner
                    .believed_edge_versions(identity, as_of_tt)
                    .map_err(err)?,
            ),
            RowKind::Node => nodes_to_py(
                py,
                self.inner
                    .believed_node_versions(identity, as_of_tt)
                    .map_err(err)?,
            ),
        };
        t.stop(G::PyBelieved);
        ip::add(G::PyCalls, 1);
        out
    }

    #[pyo3(signature = (uids, as_of_tt = OPEN_END))]
    fn believed_any(&self, uids: Vec<String>, as_of_tt: i64) -> Res<Vec<String>> {
        let t = Timer::start();
        let set = self
            .inner
            .nodes_with_believed_versions(&uids, as_of_tt)
            .map_err(err)?;
        let mut out: Vec<String> = set.into_iter().collect();
        out.sort();
        t.stop(G::PyBelievedAny);
        ip::add(G::PyCalls, 1);
        Ok(out)
    }

    /// Entity resolution. Returns `(uid, score, label, props)` for the
    /// latest believed version of each match, already ordered by
    /// `(score, uid)`. The caller builds the rows, because the output `name`
    /// comes from `props` and may be a non-string the typed column does not
    /// index.
    #[pyo3(signature = (query, as_of_tt = OPEN_END))]
    fn resolve_entities(
        &self,
        query: &str,
        as_of_tt: i64,
    ) -> Res<Vec<(String, u8, String, String)>> {
        self.inner.resolve_entities(query, as_of_tt).map_err(err)
    }

    /// One page of entity resolution: `(rows_total, page)`, where
    /// the rows are `resolve_entities`'s restricted to canonical label
    /// `label` (when given), ordered by `(score, uid)` and sliced to
    /// `[offset, offset + limit)`. `rows_total` counts after the label
    /// filter. Only the page's strings cross the binding.
    #[pyo3(signature = (query, as_of_tt = OPEN_END, label = None, offset = 0, limit = 100))]
    fn resolve_entities_page(
        &self,
        query: &str,
        as_of_tt: i64,
        label: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Res<ResolvePage> {
        self.inner
            .resolve_entities_page(query, as_of_tt, label, offset, limit)
            .map_err(err)
    }

    fn props_for_vids(&self, kind: &str, vids: Vec<String>) -> Res<HashMap<String, String>> {
        self.inner
            .props_for_vids(kind_of(kind)?, &vids)
            .map_err(err)
    }

    /// Windowed columnar scan — the workhorse `edges_columnar` serves.
    #[pyo3(signature = (as_of_tt = OPEN_END, vt_min = None, vt_max = None,
                        rel_types = None, touching_ids = None, touching_both = false,
                        limit = None, columns = None))]
    #[allow(clippy::too_many_arguments)]
    fn scan_edges<'py>(
        &self,
        py: Python<'py>,
        as_of_tt: i64,
        vt_min: Option<i64>,
        vt_max: Option<i64>,
        rel_types: Option<Vec<String>>,
        touching_ids: Option<Vec<i64>>,
        touching_both: bool,
        limit: Option<usize>,
        columns: Option<Vec<String>>,
    ) -> Res<Bound<'py, PyDict>> {
        let req = ScanRequest {
            as_of_tt,
            vt_min,
            vt_max,
            rel_types,
            touching_ids: touching_ids.map(|v| {
                let mut ids: Vec<u32> = v.into_iter().map(|i| i as u32).collect();
                ids.sort_unstable();
                ids.dedup();
                ids
            }),
            touching_both,
            limit,
            // eid is `sha256(src_uid, dst_uid, rel_type, disc)`, so asking for
            // it implies materializing all four inputs even when the caller
            // does not want them back. The adapter drops the extras on the way
            // out. `src_id`/`dst_id` joined this list when the projection
            // started reaching the fixed-width columns — before that
            // they were built unconditionally, which is exactly the kind of
            // silent dependency a projection layer must avoid.
            columns: columns.map(|mut c| {
                if c.iter().any(|x| x == "eid") {
                    for dep in ["rel_type", "disc", "src_id", "dst_id"] {
                        if !c.iter().any(|x| x == dep) {
                            c.push(dep.to_string());
                        }
                    }
                }
                c
            }),
        };
        // the belief check, segment opens, the scan and its adjacency
        // postings all live in the engine, so its tests run this same path
        let gnomon_core::store::EdgeScan {
            cols,
            stats,
            timings,
            open_ns,
        } = self.inner.scan_edges(&req).map_err(err)?;
        let t_eid = std::time::Instant::now();

        // eid is derived, never stored. This layer owns the
        // dictionary, so it is the one place that can turn dense ids back
        // into the uids the identity hash is defined over. Done before any
        // column is moved into NumPy.
        let want_eid = req
            .columns
            .as_ref()
            .map(|c| c.iter().any(|x| x == "eid"))
            .unwrap_or(true);
        let n_ids = if want_eid { cols.len() } else { 0 };
        let mut eids = Vec::with_capacity(n_ids);
        for i in 0..n_ids {
            // never index blindly across the boundary: a projection bug
            // should surface as an error, not a panic inside Python
            let (src_id, dst_id, rel, disc) = match (
                cols.src_id.get(i),
                cols.dst_id.get(i),
                cols.rel_type.get(i),
                cols.disc.get(i),
            ) {
                (Some(&s), Some(&d), Some(r), Some(dc)) => (s, d, r, dc),
                _ => {
                    return Err(PyRuntimeError::new_err(
                        "scan_edges: eid requested without src_id/dst_id/rel_type/disc \
                         materialized",
                    ))
                }
            };
            let src = self.inner.dict().uid(src_id).ok_or_else(|| {
                PyKeyError::new_err(format!("dense id {src_id} vanished"))
            })?;
            let dst = self.inner.dict().uid(dst_id).ok_or_else(|| {
                PyKeyError::new_err(format!("dense id {dst_id} vanished"))
            })?;
            eids.push(gnomon_core::derive::edge_eid(src, dst, rel, disc).to_hex());
        }

        let eid_ns = t_eid.elapsed().as_nanos() as u64;
        let t_conv = std::time::Instant::now();
        let d = PyDict::new(py);
        if want_eid {
            d.set_item("eid", PyList::new(py, &eids)?)?;
        }
        // Every key is decided from the *request*, never from whether the
        // result happens to be empty — the two are different, and conflating
        // them once returned a dict with no `vid` key for a projected scan
        // over an empty window, which the adapter met with a KeyError.
        let want = |name: &str| {
            req.columns
                .as_ref()
                .map(|c| c.iter().any(|x| x == name))
                .unwrap_or(true)
        };
        d.set_item("vt_s", cols.vt_s.into_pyarray(py))?;
        if want("vt_e") {
            d.set_item("vt_e", cols.vt_e.into_pyarray(py))?;
        }
        // Endpoint ids are stored `u32` and the adapter's contract is `int64`
        // (both backends, or operators diverge). Someone has to widen them,
        // and it costs ~37 ms per 10M column here against ~28 ms in NumPy —
        // the arithmetic is free either way, what is paid for is writing
        // 80 MB of fresh pages. So the columns *move* across the boundary at
        // their stored width and the adapter's existing
        // `np.asarray(..., dtype=np.int64)` does the widening. The
        // dtype seen by any caller is unchanged.
        if want("src_id") {
            d.set_item("src_id", cols.src_id.into_pyarray(py))?;
        }
        if want("dst_id") {
            d.set_item("dst_id", cols.dst_id.into_pyarray(py))?;
        }
        if want("vid") {
            d.set_item("vid", PyList::new(py, cols.vid.iter().map(|v| v.to_hex()))?)?;
        }
        if want("rel_type") {
            d.set_item("rel_type", PyList::new(py, &cols.rel_type)?)?;
        }
        if want("disc") {
            d.set_item("disc", PyList::new(py, &cols.disc)?)?;
        }
        if want("props") {
            d.set_item("props", PyList::new(py, &cols.props)?)?;
        }
        // physical row addresses, for callers that will come back later for
        // derived fields of a few surviving rows (edge_idents_at)
        if want("seg_id") || want("seg_row") {
            d.set_item(
                "seg_id",
                cols.seg_id.iter().map(|&v| v as i64).collect::<Vec<_>>().into_pyarray(py),
            )?;
            d.set_item(
                "seg_row",
                cols.seg_row.iter().map(|&v| v as i64).collect::<Vec<_>>().into_pyarray(py),
            )?;
        }
        // pruning counters, so effectiveness is observable rather than assumed
        d.set_item("segments_total", stats.segments_total)?;
        d.set_item("segments_pruned", stats.segments_pruned)?;
        d.set_item("rows_examined", stats.rows_examined)?;
        // The interval-lane staircase's own effectiveness counters, in
        // the same spirit as `segments_pruned` above — zero on every
        // event-lane-only scan and on any interval-lane segment written
        // before the staircase existed. `scan_nodes` has no equivalent: it
        // never routes through `select_range` (it calls `scan_node_versions`,
        // a different sweep in `read.rs`), so there is nothing to expose
        // there — a discrepancy from "scan_edges/scan_nodes already return".
        d.set_item("blocks_skipped", stats.blocks_skipped)?;
        d.set_item("blocks_considered", stats.blocks_considered)?;
        // the staircase derived at open for event-lane segments
        // that store `vt_e`, counted apart from the written one above
        d.set_item("derived_blocks_skipped", stats.derived_blocks_skipped)?;
        d.set_item("derived_blocks_considered", stats.derived_blocks_considered)?;
        // adjacency posting rows walked, and the segments whose
        // rows came from postings; zero for a scan with no small filter
        d.set_item("adj_rows_walked", stats.adj_rows_walked)?;
        d.set_item("adj_segments", stats.adj_segments)?;
        // Stage wall times (ms). Six `Instant`s on a path that moves tens of
        // megabytes; the alternative is subtracting stages from a Python wall
        // clock that contains all of them at once.
        let ms = |ns: u64| ns as f64 / 1e6;
        d.set_item("t_open_ms", ms(open_ns))?;
        d.set_item("t_select_ms", ms(timings.select_ns))?;
        d.set_item("t_cluster_ms", ms(timings.cluster_ns))?;
        d.set_item("t_materialize_ms", ms(timings.materialize_ns))?;
        d.set_item("t_order_ms", ms(timings.order_ns))?;
        d.set_item("t_copy_ms", ms(timings.copy_ns))?;
        d.set_item("clusters", timings.clusters)?;
        d.set_item("clusters_multi", timings.clusters_multi)?;
        d.set_item("t_eid_ms", ms(eid_ns))?;
        d.set_item("t_convert_ms", ms(t_conv.elapsed().as_nanos() as u64))?;
        Ok(d)
    }

    /// `aggregate_events`, engine side (aggregate.rs): one crossing per
    /// call. `group_by` is `(dim, role)` pairs, `aggregates` is `(agg, of)`
    /// pairs — the operator's already-validated closed sets. Returns group
    /// key columns as fixed-width codes (NumPy i64) plus the rehydration
    /// tables; the operator builds only the requested page in Python.
    #[pyo3(signature = (as_of_tt, t_a, t_b, rel_types, stride, group_by,
                        aggregates, max_groups))]
    #[allow(clippy::too_many_arguments)]
    fn aggregate_edges<'py>(
        &self,
        py: Python<'py>,
        as_of_tt: i64,
        t_a: i64,
        t_b: i64,
        rel_types: Option<Vec<String>>,
        stride: Option<i64>,
        group_by: Vec<(String, Option<String>)>,
        aggregates: Vec<(String, Option<String>)>,
        max_groups: usize,
    ) -> Res<Bound<'py, PyDict>> {
        use gnomon_core::aggregate::{Agg, AggValues, AggregateRequest, Dim, Source};

        let is_dst = |role: &Option<String>, what: &str| -> Res<bool> {
            match role.as_deref() {
                Some("src") => Ok(false),
                Some("dst") => Ok(true),
                other => Err(PyRuntimeError::new_err(format!(
                    "aggregate_edges: {what} requires role src|dst, got {other:?}"
                ))),
            }
        };
        let mut dims = Vec::with_capacity(group_by.len());
        for (dim, role) in &group_by {
            dims.push(match dim.as_str() {
                "time_bucket" => Dim::TimeBucket {
                    stride: stride.ok_or_else(|| {
                        PyRuntimeError::new_err(
                            "aggregate_edges: time_bucket requires stride",
                        )
                    })?,
                },
                "rel_type" => Dim::RelType,
                "endpoint" => Dim::Endpoint { dst: is_dst(role, "endpoint")? },
                "label" => Dim::Label { dst: is_dst(role, "label")? },
                other => {
                    return Err(PyRuntimeError::new_err(format!(
                        "aggregate_edges: unknown dimension {other:?}"
                    )))
                }
            });
        }
        let source = |of: &Option<String>| -> Res<Source> {
            match of.as_deref() {
                Some("vt_s") => Ok(Source::VtS),
                Some("duration") => Ok(Source::Duration),
                other => Err(PyRuntimeError::new_err(format!(
                    "aggregate_edges: stat aggregate requires of vt_s|duration, got {other:?}"
                ))),
            }
        };
        let mut aggs = Vec::with_capacity(aggregates.len());
        for (agg, of) in &aggregates {
            aggs.push(match agg.as_str() {
                "count" => Agg::Count,
                "count_distinct" => Agg::CountDistinct {
                    dst: is_dst(of, "count_distinct")?,
                },
                "min" => Agg::Min(source(of)?),
                "max" => Agg::Max(source(of)?),
                "mean" => Agg::Mean(source(of)?),
                other => {
                    return Err(PyRuntimeError::new_err(format!(
                        "aggregate_edges: unknown aggregate {other:?}"
                    )))
                }
            });
        }
        let req = AggregateRequest {
            as_of_tt,
            t_a,
            t_b,
            rel_types,
            dims,
            aggs,
            max_groups,
        };
        let out = self.inner.aggregate_edges(&req).map_err(err)?;

        let d = PyDict::new(py);
        let n = out
            .aggs
            .first()
            .map(|c| match c {
                AggValues::Int(v) => v.len(),
                AggValues::Float(v) => v.len(),
            })
            .unwrap_or(0);
        d.set_item("rows_total", n)?;
        let keys = PyList::empty(py);
        for col in out.keys {
            keys.append(col.into_pyarray(py))?;
        }
        d.set_item("keys", keys)?;
        let aggs_py = PyList::empty(py);
        for col in out.aggs {
            match col {
                AggValues::Int(v) => aggs_py.append(v)?,
                AggValues::Float(v) => aggs_py.append(v)?,
            }
        }
        d.set_item("aggs", aggs_py)?;
        d.set_item("rel_names", PyList::new(py, &out.rel_names)?)?;
        d.set_item("label_names", PyList::new(py, &out.label_names)?)?;
        // Manifest-level prune selectivity, so "a narrow window opens
        // few segments" is checkable from Python without a stopwatch
        // (`tests/test_aggregate_window_selectivity.py`). Additive keys —
        // `ops_aggregate` reads the answer columns by name and ignores these.
        d.set_item("segments_total", out.stats.segments_total)?;
        d.set_item("segments_pruned", out.stats.segments_pruned)?;
        d.set_item("segments_opened", out.stats.segments_opened)?;
        d.set_item("rows_selected", out.stats.rows_selected)?;
        Ok(d)
    }

    /// `(eids, rel_types)` for explicit `(segment id, row)` scan addresses.
    ///
    /// Addresses are only meaningful against the generation that produced
    /// them — the engine refuses segment ids the current generation does not
    /// name, and the caller (the persisted TCSR) is stamped with the
    /// generation it was built from.
    fn edge_idents_at(&self, seg_ids: Vec<i64>, seg_rows: Vec<i64>) -> Res<(Vec<String>, Vec<String>)> {
        let ids: Vec<u64> = seg_ids.iter().map(|&v| v as u64).collect();
        let rows: Vec<u32> = seg_rows.iter().map(|&v| v as u32).collect();
        let pairs = self.inner.edge_idents_at(&ids, &rows).map_err(err)?;
        Ok(pairs.into_iter().unzip())
    }

    /// Canonical-JSON props for explicit `(segment id, row)` scan addresses
    /// of `kind` rows, in input order — `props_for_vids` for a
    /// caller that already holds the rows' addresses. Same generation rule
    /// as `edge_idents_at`.
    fn props_at(&self, kind: &str, seg_ids: Vec<i64>, seg_rows: Vec<i64>) -> Res<Vec<String>> {
        let ids: Vec<u64> = seg_ids.iter().map(|&v| v as u64).collect();
        let rows: Vec<u32> = seg_rows.iter().map(|&v| v as u32).collect();
        self.inner.props_at(kind_of(kind)?, &ids, &rows).map_err(err)
    }

    /// SHA of the current generation's manifest — with the generation
    /// number, the identity a persisted derived index must be stamped with.
    fn manifest_sha(&self) -> String {
        self.inner.manifest().manifest_sha.clone()
    }

    /// On-disk manifest format this store was opened at: 3 for a store this
    /// build wrote, 1 or 2 for one an earlier engine wrote — which opens
    /// read-only until `upgrade_manifests` converts it. Format 3 differs
    /// from 2 only in what `manifest_sha` is: a Merkle root over the ordered
    /// segment set rather than the sha of the whole document.
    fn manifest_format(&self) -> u32 {
        self.inner.manifest_format()
    }

    /// Generation of the checkpoint the manifest chain currently rests on.
    /// Equal to `generation()` right after a checkpoint; below it by the
    /// number of deltas `open` would have to replay otherwise.
    fn checkpoint_generation(&self) -> u64 {
        self.inner.checkpoint_generation()
    }

    // --- maintenance ------------------------------------------------------ //

    /// Node versions overlapping a window, sorted by `(vt_s, vid)`.
    ///
    /// Nodes are few and identity-clustered, so this sweeps the
    /// node segments directly rather than driving the segment cursor — the
    /// cursor's pruning machinery buys nothing at |V| scale.
    ///
    /// The filter runs **inside** the sweep (`scan_node_versions`). It used
    /// to run over a fully materialized `all_node_versions()` listing, which
    /// built a uid, a 24-hex vid, a label and the three columns this scan
    /// does not project for every stored version before discarding most of
    /// them — a scan that never completed in 1,400 s on SNB SF1's
    /// 2,997,352 node versions.
    #[pyo3(signature = (as_of_tt = OPEN_END, vt_min = None, vt_max = None))]
    fn scan_nodes<'py>(
        &self,
        py: Python<'py>,
        as_of_tt: i64,
        vt_min: Option<i64>,
        vt_max: Option<i64>,
    ) -> Res<Bound<'py, PyDict>> {
        self.inner.assert_full_belief(as_of_tt).map_err(err)?;
        let t_mat = std::time::Instant::now();
        // sweep + filter + sort; `scan_node_versions` reports its own counts
        let out = self
            .inner
            .scan_node_versions(as_of_tt, vt_min, vt_max)
            .map_err(err)?;
        let scan_ns = t_mat.elapsed().as_nanos() as u64;
        let rows = &out.rows;

        let t_conv = std::time::Instant::now();
        let d = PyDict::new(py);
        d.set_item(
            "uid_id",
            // straight off the segment column: the dense id is what is
            // stored, so recovering it from the materialized uid by a
            // reverse dictionary lookup was work to undo work
            rows.iter()
                .map(|r| r.uid_id as i64)
                .collect::<Vec<_>>()
                .into_pyarray(py),
        )?;
        d.set_item(
            "vt_s",
            rows.iter().map(|r| r.vt_s).collect::<Vec<_>>().into_pyarray(py),
        )?;
        d.set_item(
            "vt_e",
            rows.iter().map(|r| r.vt_e).collect::<Vec<_>>().into_pyarray(py),
        )?;
        d.set_item("uid", PyList::new(py, rows.iter().map(|r| r.uid.clone()))?)?;
        d.set_item("vid", PyList::new(py, rows.iter().map(|r| r.vid.clone()))?)?;
        d.set_item("label", PyList::new(py, rows.iter().map(|r| r.label.clone()))?)?;
        // Counters and stage times, in `scan_edges`' style. This path has no
        // pruning to report, but it does visit every stored node version, and
        // "the node scan is the cost" was until now a subtraction against a
        // Python wall clock. `rows_examined` counts what the sweep visited;
        // `rows_materialized` what it built — equal before the filter moved
        // below the materialization, and the gate in
        // `tests/test_bounded_version_history.py` is that they no longer are.
        let ms = |ns: u64| ns as f64 / 1e6;
        d.set_item("segments_total", self.inner.manifest().node_store.len())?;
        d.set_item("rows", rows.len())?;
        d.set_item("rows_examined", out.rows_examined)?;
        d.set_item("rows_materialized", out.rows_materialized)?;
        // one timer now covers the sweep, the filter and the sort: they are a
        // single pass over the segments and cannot be split without lying
        d.set_item("t_materialize_ms", ms(scan_ns))?;
        d.set_item("t_sort_ms", 0.0)?;
        d.set_item("t_convert_ms", ms(t_conv.elapsed().as_nanos() as u64))?;
        Ok(d)
    }

    /// One ordered page of `version_history`, plus the exact size of the
    /// population it was drawn from.
    ///
    /// The bounded replacement for `all_versions` on this operator's path.
    /// Filtering, ordering by `(tt_s, vid)` and counting all happen in the
    /// engine over 32-byte integer keys; the only Python objects built are
    /// the `<= limit` rows of the page, and only in the columns
    /// `VERSION_COLS` projects — no `props`, so no `json.loads` per row on
    /// the way back either.
    ///
    /// `tt_e` comes back raw. The `OPEN_END` censoring against `as_of_tt` is
    /// part of the operator's contract and stays in the operator.
    #[pyo3(signature = (kind, as_of_tt, t_a, t_b, belief, rel_types, offset, limit))]
    #[allow(clippy::too_many_arguments)]
    fn version_page<'py>(
        &self,
        py: Python<'py>,
        kind: &str,
        as_of_tt: i64,
        t_a: i64,
        t_b: i64,
        belief: &str,
        rel_types: Option<Vec<String>>,
        offset: i64,
        limit: i64,
    ) -> Res<Bound<'py, PyDict>> {
        let k = kind_of(kind)?;
        let belief = match belief {
            "current" => Belief::Current,
            "superseded" => Belief::Superseded,
            "all" => Belief::All,
            other => {
                return Err(PyRuntimeError::new_err(format!(
                    "unknown belief mode {other:?}; expected 'current', \
                     'superseded' or 'all'"
                )))
            }
        };
        if k == RowKind::Node && rel_types.is_some() {
            return Err(PyRuntimeError::new_err(
                "'rel_types' is only meaningful with kind 'edge'; node \
                 versions have a label, not a relationship type",
            ));
        }
        let f = VersionFilter {
            as_of: as_of_tt,
            t_a,
            t_b,
            belief,
            rel_types: rel_types.as_deref(),
        };
        let (rows, rows_total) = self.inner.version_page(k, &f, offset, limit).map_err(err)?;
        let d = PyDict::new(py);
        let ints = |v: Vec<i64>| v.into_pyarray(py);
        match rows {
            VersionRows::Node(rows) => {
                d.set_item("vid", PyList::new(py, rows.iter().map(|r| r.vid.clone()))?)?;
                d.set_item("uid", PyList::new(py, rows.iter().map(|r| r.uid.clone()))?)?;
                d.set_item("label", PyList::new(py, rows.iter().map(|r| r.label.clone()))?)?;
                d.set_item("vt_s", ints(rows.iter().map(|r| r.vt_s).collect()))?;
                d.set_item("vt_e", ints(rows.iter().map(|r| r.vt_e).collect()))?;
                d.set_item("tt_s", ints(rows.iter().map(|r| r.tt_s).collect()))?;
                d.set_item("tt_e", ints(rows.iter().map(|r| r.tt_e).collect()))?;
            }
            VersionRows::Edge(rows) => {
                d.set_item("vid", PyList::new(py, rows.iter().map(|r| r.vid.clone()))?)?;
                d.set_item("eid", PyList::new(py, rows.iter().map(|r| r.eid.clone()))?)?;
                d.set_item("src", PyList::new(py, rows.iter().map(|r| r.src.clone()))?)?;
                d.set_item("dst", PyList::new(py, rows.iter().map(|r| r.dst.clone()))?)?;
                d.set_item(
                    "rel_type",
                    PyList::new(py, rows.iter().map(|r| r.rel_type.clone()))?,
                )?;
                d.set_item("disc", PyList::new(py, rows.iter().map(|r| r.disc.clone()))?)?;
                d.set_item("vt_s", ints(rows.iter().map(|r| r.vt_s).collect()))?;
                d.set_item("vt_e", ints(rows.iter().map(|r| r.vt_e).collect()))?;
                d.set_item("tt_s", ints(rows.iter().map(|r| r.tt_s).collect()))?;
                d.set_item("tt_e", ints(rows.iter().map(|r| r.tt_e).collect()))?;
            }
        }
        d.set_item("rows_total", rows_total)?;
        Ok(d)
    }

    /// Statistics for the cost guardrails, computed exactly.
    ///
    /// Counts cover *all* stored rows, not just believed ones, matching what
    /// the DuckDB adapter reports — the two backends have to agree here or
    /// `estimate_cost` would diverge between them. This walks the store; the
    /// incremental maintenance that makes it O(1) rides with those
    /// indexes.
    fn stats(&self, py: Python<'_>) -> Res<Py<PyDict>> {
        // the per-generation summary: no clone of the out-degree map
        let acc = self.inner.stats_summary().map_err(err)?;
        let d = PyDict::new(py);
        d.set_item("n_entities", self.inner.dict().len())?;
        d.set_item("n_node_versions", acc.n_node_versions)?;
        d.set_item("n_edge_versions", acc.n_edge_versions)?;
        d.set_item("vt_min", acc.vt_min)?;
        d.set_item("vt_max", acc.vt_max)?;
        d.set_item("rel_type_counts", &acc.rel_type_counts)?;
        d.set_item("max_out_degree", acc.max_out_degree)?;
        d.set_item("generation", self.inner.generation())?;
        Ok(d.into())
    }

    /// Walk every referenced file, checking magic, checksums, completion
    /// markers, and cross-references. Returns a report rather than raising,
    /// so an operator sees every problem at once.
    ///
    /// `full=True` adds the checks that span files or read inside a segment:
    /// the manifest parent chain across every retained generation,
    /// dictionary-code reference validity, and the bitemporal row invariants.
    /// Both modes are strictly read-only.
    ///
    /// The report gains `findings`, a list of
    /// `{layer, kind, path, generation, detail, severity}` dicts — the
    /// machine-readable form the corruption sweep matches on. `problems`
    /// stays as it was: the prose of every *error* finding, which is what
    /// `healthy` is computed from.
    #[pyo3(signature = (full = false))]
    fn verify(&self, py: Python<'_>, full: bool) -> Res<Py<PyDict>> {
        let r = if full {
            self.inner.verify_full()
        } else {
            self.inner.verify_fast()
        }
        .map_err(err)?;
        let d = PyDict::new(py);
        d.set_item("mode", &r.mode)?;
        d.set_item("generation", r.generation)?;
        // the manifest chain: which format, which checkpoint it rests on, and
        // how many deltas were replayed to reach this generation
        d.set_item("manifest_format", r.manifest_format)?;
        d.set_item("manifest_checkpoint", r.manifest_checkpoint)?;
        d.set_item("manifest_deltas", r.manifest_deltas)?;
        d.set_item("segments_checked", r.segments_checked)?;
        d.set_item("close_runs_checked", r.close_runs_checked)?;
        d.set_item("rows", r.rows)?;
        d.set_item("closes", r.closes)?;
        d.set_item("dict_records", r.dict_records)?;
        // layout quality, not integrity: one tt_s run per segment is what
        // ingest writes, and compaction's global re-sort is what inflates it
        d.set_item("tt_s_runs", r.tt_s_runs)?;
        d.set_item("max_tt_s_runs", r.max_tt_s_runs)?;
        // full mode only; zero in fast mode, where no row is read
        d.set_item("rows_walked", r.rows_walked)?;
        d.set_item("believed_rows", r.believed_rows)?;
        d.set_item("identities_checked", r.identities_checked)?;
        d.set_item("problems", PyList::new(py, &r.problems)?)?;
        let findings = PyList::empty(py);
        for f in &r.findings {
            let fd = PyDict::new(py);
            fd.set_item("layer", f.layer)?;
            fd.set_item("kind", f.kind)?;
            fd.set_item("path", &f.path)?;
            fd.set_item("generation", f.generation)?;
            fd.set_item("detail", &f.detail)?;
            fd.set_item("severity", f.severity)?;
            findings.append(fd)?;
        }
        d.set_item("findings", findings)?;
        d.set_item("healthy", r.is_healthy())?;

        // The manifest already carries `n_closed_folded` /
        // `all_current` per segment (`manifest.rs::SegmentEntry`) — the
        // same fields the staircase's `prunes()` zone map and the
        // `all_current` fast path read — but nothing surfaced
        // them to a Python caller before now. Read straight from the pinned
        // manifest, not from `r` (`verify_fast`/`verify_full`'s own report
        // struct serves a different, read-integrity-only purpose);
        // `verify()` is the natural place because it is already the one
        // report an operator reads for "how healthy/how corrected is this
        // store", and a lane split here is exactly what tells a caller
        // whether the interval lane holds any data worth looking at without
        // a separate scan.
        let lane_summary = |segs: &[gnomon_core::manifest::SegmentEntry]| -> Res<Py<PyDict>> {
            let ld = PyDict::new(py);
            ld.set_item("segments", segs.len())?;
            // Row counts alongside the close counters: the same manifest
            // pass, and what the workload's "record ... lane row
            // counts (event vs interval)" deliverable actually needs — a
            // lane split by segment count alone does not say how much data
            // is in each lane.
            ld.set_item("rows", segs.iter().map(|s| s.rows as u64).sum::<u64>())?;
            ld.set_item(
                "segments_all_current",
                segs.iter().filter(|s| s.all_current).count(),
            )?;
            ld.set_item(
                "n_closed_folded",
                segs.iter().map(|s| s.n_closed_folded as u64).sum::<u64>(),
            )?;
            Ok(ld.into())
        };
        let m = self.inner.manifest();
        let closes_by_lane = PyDict::new(py);
        closes_by_lane.set_item("node", lane_summary(&m.node_store)?)?;
        closes_by_lane.set_item("edge_event", lane_summary(&m.edge_lanes.event)?)?;
        closes_by_lane.set_item("edge_interval", lane_summary(&m.edge_lanes.interval)?)?;
        d.set_item("closes_by_lane", closes_by_lane)?;

        Ok(d.into())
    }

    fn needs_compaction(&self) -> bool {
        let t = Timer::start();
        let out = self.inner.needs_compaction();
        t.stop(G::PyNeedsCompaction);
        ip::add(G::PyCalls, 1);
        out
    }

    /// Whole-store compaction. The return dict carries the
    /// per-compaction accumulator (`rows_rewritten`, `clusters_touched`,
    /// `wall_us`, `incremental`) that `build_synth_store.py` folds into
    /// `build-record.json`; see [`compaction_dict`].
    fn compact(&mut self, py: Python<'_>) -> Res<Py<PyDict>> {
        let t = Timer::start();
        let r = self.inner.compact().map_err(err)?;
        let out = compaction_dict(py, &r);
        t.stop(G::PyCompact);
        ip::add(G::PyCalls, 1);
        out
    }

    /// The stripped configuration: rewrite the store keeping only the
    /// currently believed rows and stamp it `CURRENT_ONLY`. Experimental —
    /// the store refuses past-belief queries and corrections afterwards.
    /// `closes_folded` reports the superseded versions *dropped*.
    fn compact_current_only(&mut self, py: Python<'_>) -> Res<Py<PyDict>> {
        let r = self.inner.compact_current_only().map_err(err)?;
        let d = PyDict::new(py);
        d.set_item("segments_before", r.segments_before)?;
        d.set_item("segments_after", r.segments_after)?;
        d.set_item("versions_dropped", r.closes_folded)?;
        d.set_item("edge_rows", r.edge_rows)?;
        d.set_item("node_rows", r.node_rows)?;
        Ok(d.into())
    }

    /// Whether this store is the stripped current-only configuration.
    fn current_only(&self) -> bool {
        self.inner.current_only()
    }

    /// The engine designs this handle runs with, `name -> in force`,
    /// fixed when the handle was opened. Receipts record this rather than
    /// the environment, which may have changed since.
    fn designs(&self, py: Python<'_>) -> Res<Py<PyDict>> {
        let d = PyDict::new(py);
        for (name, on) in self.inner.designs().named() {
            d.set_item(name, on)?;
        }
        Ok(d.into())
    }

    /// Collect superseded generations: manifests older than the retention
    /// window, then any segment or close-run file no retained manifest
    /// references (superseded by compaction, or orphaned by a crash).
    #[pyo3(signature = (keep_last = gnomon_core::defaults::GC_KEEP_GENERATIONS))]
    fn gc(&mut self, py: Python<'_>, keep_last: u64) -> Res<Py<PyDict>> {
        let t = Timer::start();
        let r = self.inner.gc(keep_last).map_err(err)?;
        t.stop(G::PyGc);
        ip::add(G::PyCalls, 1);
        let d = PyDict::new(py);
        d.set_item("manifests_removed", r.manifests_removed)?;
        d.set_item("segments_removed", r.segments_removed)?;
        d.set_item("close_runs_removed", r.close_runs_removed)?;
        d.set_item("bytes_reclaimed", r.bytes_reclaimed)?;
        d.set_item("generations_retained", r.generations_retained)?;
        d.set_item("index_files_removed", r.index_files_removed)?;
        Ok(d.into())
    }

    /// Convert a format-1 or format-2 store to format 3 by republishing the
    /// current content as a checkpoint (`gnomon store upgrade-manifests`).
    ///
    /// One manifest written, `CURRENT` flipped, nothing else touched.
    /// Idempotent: `upgraded` is False on a store already at format 3.
    /// `manifest_sha` changes — the format field is inside its preimage, and
    /// from format 3 the digest is a different function entirely — so any
    /// TCSR cache stamped against the old generation rebuilds, which is what
    /// that stamp is for.
    fn upgrade_manifests(&mut self, py: Python<'_>) -> Res<Py<PyDict>> {
        let r = self.inner.upgrade_manifests().map_err(err)?;
        let d = PyDict::new(py);
        d.set_item("upgraded", r.upgraded)?;
        d.set_item("from_format", r.from_format)?;
        d.set_item("to_format", gnomon_core::MANIFEST_FORMAT_VERSION)?;
        d.set_item("generation", r.generation)?;
        d.set_item("manifest_sha", r.manifest_sha)?;
        Ok(d.into())
    }

    // --- The per-segment TCSR neighbour primitive ------------------- //
    //
    // Appended at the end of this block, never inserted. Both entry points
    // release the GIL for the whole engine call.

    /// Neighbour rows of one vertex.
    ///
    /// Returns the same four columns `tcsr.neighbors` does
    /// (`gnomon/storage/tcsr.py`) in the same order and the same
    /// `(vt_s, vid)` order, plus the physical addresses `seg_id`/`seg_row`
    /// the native TCSR column set already carries — so `temporal_paths`'
    /// identity resolution through `edge_idents_at` is unchanged.
    ///
    /// Every column is `int64`, matching what `edges_columnar` hands the
    /// Python TCSR after the adapter's own widening, so the shim's arrays
    /// are drop-in for the ones it replaces.
    #[pyo3(signature = (u, direction, as_of_tt = OPEN_END, vt_min = None,
                        vt_max = None, rel_types = None))]
    #[allow(clippy::too_many_arguments)]
    fn neighbors_of<'py>(
        &self,
        py: Python<'py>,
        u: i64,
        direction: &str,
        as_of_tt: i64,
        vt_min: Option<i64>,
        vt_max: Option<i64>,
        rel_types: Option<Vec<String>>,
    ) -> Res<Bound<'py, PyDict>> {
        let dir = NeighborDir::parse(direction).map_err(err)?;
        let rows = py.detach(|| {
            self.inner.neighbors_of(
                u as u32,
                dir,
                as_of_tt,
                vt_min,
                vt_max,
                rel_types.as_deref(),
            )
        })
        .map_err(err)?;
        neighbor_dict(py, rows, false)
    }

    /// Neighbour rows of a whole frontier, CSR-shaped.
    ///
    /// Adds `offsets` (`len(us) + 1`) to the columns above. The per-segment
    /// setup is done once for the whole call, which is the reason this entry
    /// point exists: a frontier expansion amortizes it over every vertex
    /// instead of paying it per vertex.
    #[pyo3(signature = (us, direction, as_of_tt = OPEN_END, vt_min = None,
                        vt_max = None, rel_types = None))]
    #[allow(clippy::too_many_arguments)]
    fn neighbors_batch<'py>(
        &self,
        py: Python<'py>,
        us: PyReadonlyArray1<'_, i64>,
        direction: &str,
        as_of_tt: i64,
        vt_min: Option<i64>,
        vt_max: Option<i64>,
        rel_types: Option<Vec<String>>,
    ) -> Res<Bound<'py, PyDict>> {
        let dir = NeighborDir::parse(direction).map_err(err)?;
        // borrowed from NumPy, then narrowed once: dense ids are u32 on
        // disk and int64 at the Python boundary
        let ids: Vec<u32> = us.as_slice()?.iter().map(|&v| v as u32).collect();
        let rows = py.detach(|| {
            self.inner
                .neighbors_batch(&ids, dir, as_of_tt, vt_min, vt_max, rel_types.as_deref())
        })
        .map_err(err)?;
        neighbor_dict(py, rows, true)
    }

    /// Pre-build the TCSR index of every edge segment this generation names
    /// (`gnomon store build-indexes --tcsr`).
    ///
    /// `built` counts segments indexed from their columns, `loaded` those
    /// read from `idx/tcsr/`. A commit must never move `built` for a segment
    /// that already had an index, and that is asserted on these
    /// counters rather than on a timing.
    fn build_indexes(&self, py: Python<'_>) -> Res<Py<PyDict>> {
        let (segments, built, loaded) =
            py.detach(|| self.inner.build_tcsr_indexes()).map_err(err)?;
        let d = PyDict::new(py);
        d.set_item("segments", segments)?;
        d.set_item("built", built)?;
        d.set_item("loaded", loaded)?;
        Ok(d.into())
    }

    /// Residency and provenance of this handle's TCSR indexes: how many
    /// segments are cached, how many were built vs loaded over this handle's
    /// life, and the bytes they hold. `resident_bytes` is what the memory
    /// model is checked against.
    fn tcsr_stats(&self, py: Python<'_>) -> Res<Py<PyDict>> {
        let (cached, built, loaded, bytes, rows, active) = self.inner.tcsr_stats();
        let d = PyDict::new(py);
        d.set_item("segments_cached", cached)?;
        d.set_item("built", built)?;
        d.set_item("loaded", loaded)?;
        d.set_item("resident_bytes", bytes)?;
        // The memory model's predicted side: 4 * (active_vertices + 2*segments_cached
        // + 2*rows_indexed) bytes. Without these two, `resident_bytes` is a
        // measurement with nothing to compare against.
        d.set_item("rows_indexed", rows)?;
        d.set_item("active_vertices", active)?;
        d.set_item(
            "forecast_bytes",
            4 * (active + 2 * cached as u64 + 2 * rows),
        )?;
        let per = pyo3::types::PyList::empty(py);
        for (seg_id, seg_rows, out_m, in_m, seg_bytes, out_dense, in_dense) in
            self.inner.tcsr_segment_stats()
        {
            let e = PyDict::new(py);
            e.set_item("seg_id", seg_id)?;
            e.set_item("rows_indexed", seg_rows)?;
            e.set_item("active_vertices_out", out_m)?;
            e.set_item("active_vertices_in", in_m)?;
            e.set_item("resident_bytes", seg_bytes)?;
            e.set_item("dense_out", out_dense)?;
            e.set_item("dense_in", in_dense)?;
            per.append(e)?;
        }
        d.set_item("per_segment", per)?;
        Ok(d.into())
    }

    /// Drop this handle's in-memory TCSR indexes; the `idx/` files stay.
    /// The "nothing resident" condition of the four-condition probe.
    fn release_tcsr(&self) {
        self.inner.release_tcsr();
    }

    // --- Layout policy, appended after the TCSR neighbour primitive, each
    // appending, never inserting) ----------------------------------- //

    /// Physical layout policy for segments this store writes from now on:
    /// `partition_width`
    /// decides lane assignment (`lane_for` — how many adjacent partitions an
    /// interval crosses), `segment_target_bytes` decides segment splitting.
    /// Both are **physical only**: lane assignment is not logical identity
    /// so changing this can never
    /// move a `store_digest` — `tests/test_interval_staircase.py::
    /// test_partition_width_does_not_move_a_digest` is the gate. Already-
    /// written segments are unaffected; only future `commit()`s see it.
    fn set_layout(
        &mut self,
        partition_origin: i64,
        partition_width: i64,
        segment_target_bytes: u64,
    ) -> Res<()> {
        let partitions = PartitionMap::new(partition_origin, partition_width).map_err(err)?;
        self.inner.set_layout(partitions, segment_target_bytes);
        Ok(())
    }

    // -- On-disk sorted name index ------------------- //
    //
    // Appended at the end of this `#[pymethods]` block, never inserted —
    // the same rule earlier additions follow. `resolve_entities` above is
    // untouched: it answers substring,
    // which no sorted index accelerates, so this is a new API next
    // to it, not a replacement.

    /// Exact or prefix name/uid lookup through the sorted per-segment index,
    /// falling back transparently to building it on first use. Same output
    /// shape as `resolve_entities`: `(uid, score, label, props)`, ordered by
    /// `(score, uid)`. `mode` is `"exact"` or `"prefix"`.
    #[pyo3(signature = (query, mode, as_of_tt = OPEN_END, limit = None))]
    fn lookup_name(
        &self,
        query: &str,
        mode: &str,
        as_of_tt: i64,
        limit: Option<usize>,
    ) -> Res<Vec<(String, u8, String, String)>> {
        let mode = match mode {
            "exact" => gnomon_core::read::NameMatchMode::Exact,
            "prefix" => gnomon_core::read::NameMatchMode::Prefix,
            other => {
                return Err(PyRuntimeError::new_err(format!(
                    "unknown lookup_name mode {other:?}; expected 'exact' or 'prefix'"
                )))
            }
        };
        self.inner
            .lookup_name(query, mode, as_of_tt, limit)
            .map_err(err)
    }

    /// Force-build (and persist) the sorted name index for every node
    /// segment the current manifest names, skipping any already fresh —
    /// `gnomon store build-indexes --name`'s engine entry point. `lookup_name`
    /// above builds the same index lazily on first use; this exists for a
    /// latency-sensitive process that wants the cost paid up front.
    /// Returns the number of segments actually (re)built.
    fn build_name_indexes(&self) -> Res<usize> {
        let mut built = 0usize;
        for entry in &self.inner.manifest().node_store {
            let id = segment_id_of(&entry.file);
            let seg = self.inner.open_segment(&entry.file).map_err(err)?;
            let before = gnomon_core::nameidx::index_path(self.inner.root(), id);
            let stale = match gnomon_core::nameidx::load(&before) {
                Ok(existing) => existing.is_stale(&entry.sha),
                Err(_) => true,
            };
            gnomon_core::nameidx::load_or_build_persist(
                self.inner.root(),
                &seg,
                id,
                &entry.sha,
                !self.inner.is_read_only(),
            )
            .map_err(err)?;
            if stale {
                built += 1;
            }
        }
        Ok(built)
    }

    // -- Paged digest producer -------- //
    //
    // Appended at the end of this `#[pymethods]` block, never inserted —
    // the same rule every earlier addition followed.

    /// One page of at most `limit` rows from the whole-store version
    /// population of `kind`, every field `to_json()` needs — `props`,
    /// `source` and `provenance_ref` included. `store_digest_streaming`
    /// (`gnomon/storage/base.py`) pulls `chunk_rows`-sized pages through this
    /// instead of materializing `all_versions`'s whole-store dict once, so
    /// the Python-side row population is never bigger than one page.
    ///
    /// **Distinct from `version_page` above.** That one answers
    /// `version_history`'s narrower row shape (`VersionKey`/
    /// `version_rows_at`, `read.rs:1215`/`:1349`) and deliberately never
    /// reads `props`/`source`/`provenance_ref` — building them was most of
    /// what the old `version_history` path cost. The digest needs
    /// exactly those three fields, so `version_page`'s machinery cannot
    /// serve it; this method calls `all_edge_versions`/`all_node_versions`
    /// instead, the only public whole-row accessors `read.rs` exposes.
    ///
    /// **Why this caches the whole-store `Vec` instead of being
    /// O(chunk_rows) end to end.** `read.rs`'s `edge_files`/`node_files`,
    /// `edge_rows_of`/`node_rows_of` and `staged_edge_versions`/
    /// `staged_node_versions` — the segment-level building blocks a true
    /// paged accessor would compose — are private to that module's `impl
    /// NativeStore` block, not `pub`, and `read.rs`/`store.rs` are owned by
    /// a different part of the codebase for now; a follow-up would need to
    /// expose the exact signatures. The only public
    /// whole-row reads are `all_edge_versions()`/`all_node_versions()`
    /// themselves, each O(store) per call — so naively re-deriving the
    /// population on every page would turn an O(store) digest pass into an
    /// O(store²/chunk_rows) one, which is worse than what this replaces,
    /// not better. Instead the whole-store `Vec` is computed once per kind
    /// (on the first page, `offset == 0`) and cached on this handle,
    /// released as soon as the last page is drawn — one O(store) Rust walk
    /// per kind for the whole digest pass, same as before, while the
    /// **Python** side (where an 80 GB transient
    /// was measured — a list of `NodeVersion`/`EdgeVersion` objects with
    /// their `props` already `json.loads`-parsed, held in full alongside
    /// the Rust `Vec` that fed it) never holds more than `limit` rows.
    /// Memory target achieved: **O(chunk_rows) on the Python side**; the
    /// transient Rust-side `Vec<EdgeVersionOut>`/`Vec<NodeVersionOut>` this
    /// caches is the same one `all_versions` already built, held no longer
    /// than before (freed at the last page, same as `all_versions` freed
    /// it on return).
    ///
    /// A caller must draw pages for one kind in any order but must not
    /// interleave a fresh `offset == 0` call for that kind before the
    /// previous pass finished — `store_digest_streaming` pages one kind to
    /// exhaustion before starting the other, which satisfies this
    /// trivially. Returns `(page_dict, rows_total)`.
    #[pyo3(signature = (kind, offset, limit))]
    fn all_versions_page(
        &self,
        py: Python<'_>,
        kind: &str,
        offset: usize,
        limit: usize,
    ) -> Res<(Py<PyDict>, usize)> {
        match kind_of(kind)? {
            RowKind::Edge => {
                let mut cache = self
                    .edge_version_page_cache
                    .lock()
                    .expect("edge version page cache mutex poisoned");
                if cache.is_none() || offset == 0 {
                    *cache = Some(self.inner.all_edge_versions().map_err(err)?);
                }
                let total = cache.as_ref().expect("just populated above").len();
                let end = (offset + limit).min(total);
                let page: Vec<EdgeVersionOut> = if offset < total {
                    cache.as_ref().expect("just populated above")[offset..end].to_vec()
                } else {
                    Vec::new()
                };
                if end >= total {
                    *cache = None;
                }
                drop(cache);
                Ok((edges_to_py(py, page)?, total))
            }
            RowKind::Node => {
                let mut cache = self
                    .node_version_page_cache
                    .lock()
                    .expect("node version page cache mutex poisoned");
                if cache.is_none() || offset == 0 {
                    *cache = Some(self.inner.all_node_versions().map_err(err)?);
                }
                let total = cache.as_ref().expect("just populated above").len();
                let end = (offset + limit).min(total);
                let page: Vec<NodeVersionOut> = if offset < total {
                    cache.as_ref().expect("just populated above")[offset..end].to_vec()
                } else {
                    Vec::new()
                };
                if end >= total {
                    *cache = None;
                }
                drop(cache);
                Ok((nodes_to_py(py, page)?, total))
            }
        }
    }
    // -- Partition-local (incremental) compaction -------- //
    //
    // Appended at the end of this `#[pymethods]` block, never inserted — the
    // same rule each earlier addition followed. `compact` above is untouched
    // and stays available: it is the paired configuration for this method's
    // own gates.

    /// Rewrite only the key-range clusters whose run count or unfolded-close
    /// fraction makes them eligible, instead of the whole store.
    ///
    /// `budget_rows` caps one call's work; `None` takes every eligible
    /// cluster. Returns the same dict shape `compact()` does, with
    /// `incremental` true and `clusters_touched` set. The logical store — and
    /// therefore `store_digest` — is unchanged either way, which is what
    /// `tests/test_incremental_compaction_scaling.py` asserts against a
    /// whole-store compaction of the same log.
    #[pyo3(signature = (budget_rows = None))]
    fn compact_incremental(
        &mut self,
        py: Python<'_>,
        budget_rows: Option<u64>,
    ) -> Res<Py<PyDict>> {
        let t = Timer::start();
        let r = self
            .inner
            .compact_incremental(budget_rows)
            .map_err(err)?;
        let out = compaction_dict(py, &r);
        t.stop(G::PyCompactIncremental);
        ip::add(G::PyCalls, 1);
        out
    }

    /// The advisory hint: any unfolded close run, or more than
    /// `COMPACTION_RUNS_TRIGGER` segments. `needs_compaction` above now
    /// answers "is any cluster eligible", which is the predicate
    /// `compact_incremental` plans against; this is what whole-store
    /// compaction pairs with.
    fn needs_whole_store_compaction(&self) -> bool {
        self.inner.needs_whole_store_compaction()
    }

    /// The identity postings' byte-budget receipt, the
    /// same role `segment_cache_stats` plays for the segment cache and
    /// `tcsr_stats` for the TCSR index.
    ///
    /// `entries`/`resident_bytes` are what the budget bounds;
    /// `budget` is the resolved `GNOMON_POSTINGS_BYTES` (`None` = unbounded);
    /// `evictions`/`rebuilds` are the price of a budget that binds;
    /// `folds`/`rows_folded` count every segment ever folded and every row
    /// walked by one — the instrument that attributed the 30M build's
    /// finalisation RSS step to the first correction's point read; and
    /// `transient_folds` counts folds answered and dropped because one
    /// segment's postings alone exceeded the budget, which is the shape a
    /// very small budget produces and where the answer is still exact.
    fn postings_index_stats(&self, py: Python<'_>, kind: &str) -> Res<Py<PyDict>> {
        let k = kind_of(kind)?;
        let (entries, resident, bytes, budget, evictions, rebuilds, folds, rows, transient) =
            self.inner.postings_index_stats(k);
        let d = PyDict::new(py);
        d.set_item("entries", entries)?;
        d.set_item("resident_segments", resident)?;
        d.set_item("resident_bytes", bytes)?;
        d.set_item("budget", budget)?;
        d.set_item("evictions", evictions)?;
        d.set_item("rebuilds", rebuilds)?;
        d.set_item("folds", folds)?;
        d.set_item("rows_folded", rows)?;
        d.set_item("transient_folds", transient)?;
        // The identity filter's probe counters for this same kind, so
        // `folds / filter_probes` -- the mechanism gate, "a correction folds
        // one segment rather than the store" -- is one division inside one
        // dict rather than a join across two receipts. `filter_probes` is 0
        // on a store with no `idx/vidfilter/`, which is the earlier
        // behaviour and is exactly what `folds_per_probe: None` says.
        // `filter_empty_positives / filter_positives` measures epsilon in
        // situ: a positive whose `rows_for(key)` came back empty is a false
        // positive by definition.
        let (probes, positives, negatives, empty) = self.inner.filter_probe_stats(k);
        d.set_item("filter_probes", probes)?;
        d.set_item("filter_positives", positives)?;
        d.set_item("filter_negatives", negatives)?;
        d.set_item("filter_empty_positives", empty)?;
        d.set_item(
            "folds_per_probe",
            (probes > 0).then(|| folds as f64 / probes as f64),
        )?;
        Ok(d.into())
    }

    /// Override the identity-postings byte budget for this handle (`None` =
    /// unbounded), mirroring `set_segment_cache_budget`: a test or harness
    /// must be able to exercise eviction without mutating the process
    /// environment, which races other threads.
    #[pyo3(signature = (budget = None))]
    fn set_postings_budget(&self, budget: Option<u64>) {
        self.inner.set_postings_budget(budget);
    }

    // -- `settle`, the quiescent read-optimising merge ---- //
    //
    // Appended at the end of this `#[pymethods]` block, never inserted --
    // the same rule each earlier addition followed. `compact` and
    // `compact_incremental` above are untouched: settle is a third entry
    // point beside them, not a replacement for either, and the build path
    // keeps planning exactly as it did.

    /// Merge every live segment of every lane that is above its byte floor,
    /// once, into segments at the byte target -- a major compaction, invoked
    /// explicitly at quiescence.
    ///
    /// `budget_bytes` overrides `GNOMON_SETTLE_BYTES` for this call and bounds
    /// the input bytes one merge group may span, so peak memory is flat in the
    /// store's size. The logical store is unchanged and transaction time does
    /// not advance, so `store_digest` is invariant -- which is what lets a
    /// store built at an earlier commit be settled in place without the
    /// comparison becoming a comparison of two builds.
    ///
    /// Returns `rows_rewritten` (exactly the stored row count when `rounds`
    /// is 1), `rounds` (0 when the store was already at its floor, and a
    /// second settle immediately after the first reports 0), `fan_in`,
    /// `groups`, `budget_bytes`, `inputs_total`/`inputs_below_full_rows` (the
    /// diagnosis instrument: how many inputs carried fewer rows than the tier
    /// planner's own byte-target threshold), `reclaimable_bytes` (what a
    /// following `gc --keep 2` frees), the generations either side, and
    /// `wall_us`.
    #[pyo3(signature = (budget_bytes = None))]
    fn settle(&mut self, py: Python<'_>, budget_bytes: Option<u64>) -> Res<Py<PyDict>> {
        let r = py
            .detach(|| self.inner.settle(budget_bytes))
            .map_err(err)?;
        settle_dict(py, &r)
    }

    /// Advisory: is any lane slot above its byte floor? Opens no segment and
    /// touches no file -- row counts and key ranges from the manifest are
    /// everything the plan needs. It schedules nothing; a driver polls it at
    /// quiescence and decides for itself, exactly as `needs_compaction` is
    /// used.
    fn needs_settle(&self) -> bool {
        self.inner.needs_settle()
    }

    // -- The streaming content digest --------------------------------- //
    //
    // Appended last, never inserted -- the same seam rule every
    // wave has followed. Nothing above is touched: this is a new read-only
    // entry point beside `all_versions_page`, not a replacement for it, and
    // the paged producer `store_digest_streaming` uses stays exactly as it is.

    /// The store's `content_digest` -- `store_digest`'s fields minus
    /// `vid`/`tt_s`/`tt_e` -- computed by a byte-bounded external sort and a
    /// bounded-fan-in k-way merge, so peak memory follows the budget and the
    /// fan-in rather than the row count.
    ///
    /// Byte-identical to the posthoc Python pass
    /// (`scripts/build_synth_store.py::content_digest`) for the same store.
    /// `budget_bytes` overrides `GNOMON_DIGEST_BYTES` for this call; `0` means
    /// unbounded, which reproduces the posthoc pass's memory profile.
    ///
    /// Returns `digest`, the per-kind row counts, `rows_held_max` (the
    /// high-water mark of rows resident at once -- the memory claim, counted
    /// rather than inferred), `runs_spilled`/`spill_bytes`/`merge_rounds`/
    /// `max_fan_in`, `budget_bytes`, `segments_read`, `ambiguous_ties` (rows
    /// the reference's own sort key cannot separate; zero on every store the
    /// ingest path builds) and `wall_us`.
    #[pyo3(signature = (budget_bytes = None))]
    fn content_digest(&self, py: Python<'_>, budget_bytes: Option<u64>) -> Res<Py<PyDict>> {
        let r = py
            .detach(|| self.inner.content_digest(budget_bytes))
            .map_err(err)?;
        content_digest_dict(py, &r)
    }

    /// Per-segment `tt_s` run profile: `lane`, `file`, `rows`, `tt_s_runs`,
    /// `tt_min`, `tt_max`, and the derived `runs_per_row`.
    ///
    /// Read-only instrumentation beside `segment_layout`. `believed_ranges`
    /// and `diff_belief`'s `added_ranges` cost `O(runs)` exactly when the
    /// folded `(tt_min, tt_max)` bounds cannot decide a segment, and `O(1)`
    /// when they can -- so `runs_per_row` is what separates a layout those
    /// bounds answer for free from one they never answer for. It is a
    /// property of the layout, not of the store's size: a tiered segment is
    /// one commit and carries a single run, while a settled one is a global
    /// `(vt_s, vid)` re-sort that interleaves transaction times and shatters
    /// them.
    fn segment_tt_profile<'py>(&self, py: Python<'py>) -> Res<Bound<'py, PyList>> {
        let rows = py
            .detach(|| self.inner.segment_tt_profile())
            .map_err(err)?;
        let out = PyList::empty(py);
        for r in rows {
            let d = PyDict::new(py);
            d.set_item("lane", r.lane)?;
            d.set_item("file", &r.file)?;
            d.set_item("rows", r.rows)?;
            d.set_item("tt_s_runs", r.tt_s_runs)?;
            d.set_item("tt_min", r.tt_min)?;
            d.set_item("tt_max", r.tt_max)?;
            d.set_item(
                "runs_per_row",
                if r.rows == 0 {
                    0.0
                } else {
                    f64::from(r.tt_s_runs) / f64::from(r.rows)
                },
            )?;
            out.append(d)?;
        }
        Ok(out)
    }

    // -- The belief diff --------------------------------------------- //
    //
    // Appended last, never inserted -- the same seam rule every
    // wave has followed. A new read-only entry point beside `version_page`;
    // nothing above it changes.

    /// Every version whose *belief state* changed between transaction times
    /// `t1` and `t2`, inside the valid-time window `[t_a, t_b)`.
    ///
    /// The dual of `diff_snapshots`, which takes one `as_of_tt` and two
    /// valid-time instants: this takes two belief times and one valid-time
    /// window. A version is tagged `"added"` when its `tt_s` falls in
    /// `(t1, t2]` and `"retracted"` when its effective `tt_e` does; a version
    /// written and corrected inside the same window changed state twice and
    /// is reported twice. See `gnomon_core::beliefdiff` for the full
    /// semantics, the cost argument and the layout-independence argument.
    ///
    /// Computed per segment from the header's `tt_s` runs and the segment's
    /// effective close set, restricted to the window's rows --
    /// `O(segments x log rows)` plus the runs and closes inside the window's
    /// rows plus the answer, never a row scan -- so its cost follows neither
    /// the store's size nor the size of the segment the window lands in.
    ///
    /// Returns `rows` (ordered by `(kind, key, tt, change, vid)`, capped at
    /// `limit`), `added_total` / `retracted_total` (counted over the whole
    /// surviving population, never over the page), and `truncated`.
    #[pyo3(signature = (t1, t2, t_a, t_b, limit))]
    fn diff_belief(
        &self,
        py: Python<'_>,
        t1: i64,
        t2: i64,
        t_a: i64,
        t_b: i64,
        limit: usize,
    ) -> Res<Py<PyDict>> {
        let out = py
            .detach(|| self.inner.diff_belief(t1, t2, t_a, t_b, limit))
            .map_err(err)?;
        let d = PyDict::new(py);
        let rows = PyList::empty(py);
        for r in &out.rows {
            let row = PyDict::new(py);
            row.set_item(
                "kind",
                match r.kind {
                    RowKind::Node => "node",
                    RowKind::Edge => "edge",
                },
            )?;
            row.set_item("key", &r.key)?;
            row.set_item("vid", &r.vid)?;
            row.set_item("tt", r.tt)?;
            row.set_item("change", r.change.as_str())?;
            row.set_item("vt_s", r.vt_s)?;
            row.set_item("vt_e", r.vt_e)?;
            rows.append(row)?;
        }
        d.set_item("rows", rows)?;
        d.set_item("added_total", out.added_total)?;
        d.set_item("retracted_total", out.retracted_total)?;
        d.set_item("truncated", out.truncated)?;
        Ok(d.into())
    }

    // -- Build-time memory accounting ------------------------------- //
    //
    // Appended at the end of this `#[pymethods]` block, never inserted --
    // the same rule every earlier addition has followed.

    /// Bytes held by each long-lived in-process structure, as
    /// `{"current": {...}, "peak": {...}}`.
    ///
    /// Observation only: no behaviour on any read or write path changes
    /// because this is called, and nothing here allocates a store-sized
    /// structure. `peak` is a running maximum over the samples *this handle*
    /// has been asked for -- the same guarantee `postings_peak_bytes` already
    /// carries, and for the same reason: the accessor costs nothing between
    /// calls, so a caller samples it at its own progress cadence.
    ///
    /// See `NativeStore::memory_accounting` in the core crate for what each
    /// component covers. The two that a receipt exists to separate:
    /// `segment_cache_mapped` and `segment_cache_decoded` are the halves of
    /// the one number `segment_cache_stats` reports, and only the second is
    /// heap an eviction reclaims; `segment_cache_header` is parsed-header
    /// heap held per cached segment against no budget at all.
    ///
    /// Figures are allocated-capacity estimates, not malloc truth. Compare
    /// the sum against `RssAnon` from `/proc/self/status`, not against
    /// `VmRSS`: `segment_cache_mapped` and `pinned_mapped` are file-backed
    /// and land in `RssFile` to the extent their pages have been touched.
    fn memory_accounting(&self, py: Python<'_>) -> Res<Py<PyDict>> {
        let (current, peak) = self.inner.memory_accounting();
        let out = PyDict::new(py);
        for (name, rows) in [("current", current), ("peak", peak)] {
            let d = PyDict::new(py);
            for (k, v) in rows {
                d.set_item(k, v)?;
            }
            out.set_item(name, d)?;
        }
        Ok(out.into())
    }

    // -- The per-segment identity filter ----------------------------- //
    //
    // Appended last, never inserted -- the seam rule every
    // wave has followed. Nothing above changes
    // shape: `build_indexes` still means the TCSR index, and this is a third
    // index kind beside it and the name index, not a replacement for either.

    /// Pre-build the identity filter of every segment this generation names,
    /// both row kinds (`gnomon store build-indexes --vidfilter`, settle, and
    /// `compact(build_indexes=True)`).
    ///
    /// Eager, against the project's "indexes are built lazily at first use"
    /// default, and the departure is the point: the 1B correction ramp was
    /// measured as *one operation*, so for this index the first use
    /// **is** the entire cost and a lazily built filter would be written
    /// during a whole-store pass that has already happened. The migration
    /// path for a store that predates the change is this call; the lazy
    /// build path inside a fold stays as the correctness fallback.
    ///
    /// `built` counts segments whose filter was constructed from columns,
    /// `loaded` those already on disk.
    fn build_vid_filters(&self, py: Python<'_>) -> Res<Py<PyDict>> {
        let (segments, built, loaded) =
            py.detach(|| self.inner.build_vid_filters()).map_err(err)?;
        let d = PyDict::new(py);
        d.set_item("segments", segments)?;
        d.set_item("built", built)?;
        d.set_item("loaded", loaded)?;
        Ok(d.into())
    }

    /// Residency, provenance and probe receipt for this handle's identity
    /// filters.
    ///
    /// `mapped_bytes` belongs to **`file_mapped_total`, never to
    /// `anonymous_total`**: a filter is a file mapping, which is the
    /// property that makes it reclaimable where the anonymous postings heap
    /// it prunes is not, and the memory-class split stops being honest if it is
    /// charged to the wrong side.
    ///
    /// `lazy_built` counts the legacy-store fallback -- filters written on
    /// the way out of a fold the probe had to do anyway. It is reported so
    /// that "nothing relies on the lazy build path" is a reading rather than a
    /// claim.
    ///
    /// `epsilon` is `empty_positives / positives`: measured in situ, against
    /// a pre-registered band of 3 * 2^-16.
    fn vidfilter_stats(&self, py: Python<'_>) -> Res<Py<PyDict>> {
        let (
            segments,
            built,
            lazy_built,
            loaded,
            bytes,
            budget,
            evictions,
            transient,
            probes,
            positives,
            negatives,
            empty,
        ) = self.inner.vidfilter_stats();
        let d = PyDict::new(py);
        d.set_item("segments_resident", segments)?;
        d.set_item("built", built)?;
        d.set_item("lazy_built", lazy_built)?;
        d.set_item("loaded", loaded)?;
        d.set_item("mapped_bytes", bytes)?;
        d.set_item("budget", budget)?;
        d.set_item("evictions", evictions)?;
        d.set_item("transient", transient)?;
        d.set_item("probes", probes)?;
        d.set_item("positives", positives)?;
        d.set_item("negatives", negatives)?;
        d.set_item("empty_positives", empty)?;
        d.set_item(
            "epsilon",
            (positives > 0).then(|| empty as f64 / positives as f64),
        )?;
        Ok(d.into())
    }

    /// Override the identity-filter byte ceiling for this handle (`None` =
    /// unbounded), mirroring `set_postings_budget` and for the same reason:
    /// a sweep must be able to change it without mutating the process
    /// environment, which races other threads.
    #[pyo3(signature = (budget = None))]
    fn set_vidfilter_budget(&self, budget: Option<u64>) {
        self.inner.set_vidfilter_budget(budget);
    }

    /// Drop this handle's mapped filters; the `idx/vidfilter/` files stay.
    /// The "nothing resident" condition a residency probe needs, as
    /// `release_tcsr` is for the TCSR index.
    fn release_vid_filters(&self) {
        self.inner.release_vid_filters();    }

    // Appended at the end of this `#[pymethods]` block, never inserted.

    /// Whether this handle was opened with `read_only=True`.
    fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    // appended at the end of this `#[pymethods]` block, never inserted.

    /// `temporal_reachability`'s earliest-arrival search: the page
    /// `[offset, offset + limit)` of the reached nodes ordered by
    /// `(arrival, uid)`, as `{"uids", "arrival", "total"}`; `None` inside an
    /// open write batch (the operator keeps its NumPy path there).
    #[pyo3(signature = (src, t_a, t_b, as_of_tt, direction, offset, limit))]
    #[allow(clippy::too_many_arguments)]
    fn reach_earliest<'py>(
        &self,
        py: Python<'py>,
        src: u32,
        t_a: i64,
        t_b: i64,
        as_of_tt: i64,
        direction: &str,
        offset: usize,
        limit: usize,
    ) -> Res<Option<Bound<'py, PyDict>>> {
        let dir = gnomon_core::reach::ReachDir::parse(direction).map_err(err)?;
        let out = py
            .detach(|| self.inner.reach_earliest(src, t_a, t_b, as_of_tt, dir, offset, limit))
            .map_err(err)?;
        let Some(out) = out else { return Ok(None) };
        let d = PyDict::new(py);
        d.set_item("uids", out.uids)?;
        d.set_item("arrival", out.arrival)?;
        d.set_item("total", out.total)?;
        Ok(Some(d))
    }

    /// `diff_snapshots` without `scope`: the four identity lists' pages
    /// and totals, and the content-change candidates whose stored text
    /// differs (the operator compares those itself). `None` inside an open
    /// write batch.
    fn diff_snapshots<'py>(
        &self,
        py: Python<'py>,
        t1: i64,
        t2: i64,
        as_of_tt: i64,
        limit: usize,
    ) -> Res<Option<Bound<'py, PyDict>>> {
        let out = py
            .detach(|| self.inner.diff_snapshots(t1, t2, as_of_tt, limit))
            .map_err(err)?;
        let Some(out) = out else { return Ok(None) };
        let d = PyDict::new(py);
        d.set_item("nodes_added", out.nodes_added)?;
        d.set_item("nodes_added_total", out.nodes_added_total)?;
        d.set_item("nodes_removed", out.nodes_removed)?;
        d.set_item("nodes_removed_total", out.nodes_removed_total)?;
        for (key, descs, total) in [
            ("edges_added", &out.edges_added, out.edges_added_total),
            ("edges_removed", &out.edges_removed, out.edges_removed_total),
        ] {
            let rows = PyList::empty(py);
            for e in descs {
                let r = PyDict::new(py);
                r.set_item("eid", &e.eid)?;
                r.set_item("src", &e.src)?;
                r.set_item("dst", &e.dst)?;
                r.set_item("rel_type", &e.rel_type)?;
                rows.append(r)?;
            }
            d.set_item(key, rows)?;
            d.set_item(format!("{key}_total"), total)?;
        }
        let nc = PyList::empty(py);
        for c in &out.node_cands {
            nc.append((&c.uid, &c.vid_a, &c.vid_b, &c.label_a, &c.label_b, &c.props_a, &c.props_b))?;
        }
        d.set_item("node_cands", nc)?;
        let ec = PyList::empty(py);
        for c in &out.edge_cands {
            ec.append((&c.eid, &c.vid_a, &c.vid_b, &c.props_a, &c.props_b))?;
        }
        d.set_item("edge_cands", ec)?;
        Ok(Some(d))
    }
}

/// Plain (non-Python) helpers of the binding.
impl NativeStore {
    fn commit_body(&mut self) -> Res<u64> {
        // The event-log position is owned by the Python `Store`, which
        // appended and fsynced before the engine was called (write-ahead)
        // and staged the resulting cursor here. A commit without a staged
        // cursor inherits the parent generation's — never regresses it —
        // so cursorless callers (legacy stores, direct engine use) keep
        // whatever recovery information the store already had.
        let cursor = self
            .pending_cursor
            .take()
            .unwrap_or_else(|| self.inner.manifest().event_log.clone());
        self.inner.commit(cursor).map_err(err)
    }
}

/// One `ContentDigestReport` as the dict the Python adapter returns.
fn content_digest_dict(
    py: Python<'_>,
    r: &gnomon_core::digest::ContentDigestReport,
) -> Res<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("digest", r.digest.as_str())?;
    d.set_item("rows_edges", r.rows_edges)?;
    d.set_item("rows_nodes", r.rows_nodes)?;
    d.set_item("rows_held_max", r.rows_held_max)?;
    d.set_item("runs_spilled", r.runs_spilled)?;
    d.set_item("merge_rounds", r.merge_rounds)?;
    d.set_item("max_fan_in", r.max_fan_in)?;
    d.set_item("spill_bytes", r.spill_bytes)?;
    d.set_item("budget_bytes", r.budget_bytes)?;
    d.set_item("ambiguous_ties", r.ambiguous_ties)?;
    d.set_item("segments_read", r.segments_read)?;
    d.set_item("wall_us", r.wall_us)?;
    Ok(d.into())
}

/// One `SettleReport` as the dict the Python adapter returns.
fn settle_dict(
    py: Python<'_>,
    r: &gnomon_core::settle::SettleReport,
) -> Res<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("segments_before", r.segments_before)?;
    d.set_item("segments_after", r.segments_after)?;
    d.set_item("closes_folded", r.closes_folded)?;
    d.set_item("rows_rewritten", r.rows_rewritten)?;
    d.set_item("rounds", r.rounds)?;
    d.set_item("fan_in", r.fan_in)?;
    d.set_item("groups", r.groups)?;
    d.set_item("budget_bytes", r.budget_bytes)?;
    d.set_item("inputs_total", r.inputs_total)?;
    d.set_item("inputs_below_full_rows", r.inputs_below_full_rows)?;
    d.set_item("reclaimable_bytes", r.reclaimable_bytes)?;
    d.set_item("generation_before", r.generation_before)?;
    d.set_item("generation_after", r.generation_after)?;
    d.set_item("wall_us", r.wall_us)?;
    Ok(d.into())
}

/// One `CompactionReport` as the dict the Python adapter returns.
///
/// Shared by every compaction entry point so the accumulator
/// has one spelling: `rows_rewritten` is summed over a build and `wall_us`
/// is what that sum is divided by to get a rate, and a key that exists on
/// one path and not another would make either figure quietly path-dependent.
/// The existing keys are unchanged, so an existing reader of `compact()`'s
/// dict keeps working.
fn compaction_dict(
    py: Python<'_>,
    r: &gnomon_core::compact::CompactionReport,
) -> Res<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("segments_before", r.segments_before)?;
    d.set_item("segments_after", r.segments_after)?;
    d.set_item("closes_folded", r.closes_folded)?;
    d.set_item("edge_rows", r.edge_rows)?;
    d.set_item("node_rows", r.node_rows)?;
    d.set_item("rows_rewritten", r.rows_rewritten)?;
    d.set_item("clusters_touched", r.clusters_touched)?;
    d.set_item("wall_us", r.wall_us)?;
    d.set_item("incremental", r.incremental)?;
    Ok(d.into())
}

/// The neighbour columns as NumPy `int64`, optionally with the CSR offsets.
fn neighbor_dict(
    py: Python<'_>,
    rows: NeighborRows,
    with_offsets: bool,
) -> Res<Bound<'_, PyDict>> {
    let d = PyDict::new(py);
    d.set_item(
        "nbr",
        rows.nbr
            .iter()
            .map(|&v| v as i64)
            .collect::<Vec<_>>()
            .into_pyarray(py),
    )?;
    d.set_item("vt_s", rows.vt_s.into_pyarray(py))?;
    d.set_item("vt_e", rows.vt_e.into_pyarray(py))?;
    d.set_item(
        "seg_id",
        rows.seg_id
            .iter()
            .map(|&v| v as i64)
            .collect::<Vec<_>>()
            .into_pyarray(py),
    )?;
    d.set_item(
        "seg_row",
        rows.seg_row
            .iter()
            .map(|&v| v as i64)
            .collect::<Vec<_>>()
            .into_pyarray(py),
    )?;
    if with_offsets {
        d.set_item(
            "offsets",
            rows.offsets
                .iter()
                .map(|&v| v as i64)
                .collect::<Vec<_>>()
                .into_pyarray(py),
        )?;
    }
    Ok(d)
}

/// δ-temporal motif matching.
///
/// Takes the window's events as columns and returns `(count, instances)`,
/// where each instance is a triple of row indices into those columns — the
/// caller already holds the strings, so there is no reason to send them back
/// across the boundary. `collect=False` counts without materializing, which
/// is all `count_temporal_motifs` needs.
#[pyfunction]
#[pyo3(signature = (motif, src, dst, t, eid, delta, collect = false))]
#[allow(clippy::too_many_arguments)]
fn motif_match(
    py: Python<'_>,
    motif: &str,
    src: PyReadonlyArray1<'_, i64>,
    dst: PyReadonlyArray1<'_, i64>,
    t: PyReadonlyArray1<'_, i64>,
    eid: Vec<String>,
    delta: i64,
    collect: bool,
) -> Res<(u64, Vec<[u32; 3]>)> {
    let kind = gnomon_core::motif::Motif::parse(motif).map_err(err)?;
    // The three integer columns are borrowed from NumPy rather than copied
    // into Vecs. Building those Vecs cost 4.4 ms of an 11 ms call at 14.5k
    // events — first in Python materializing lists, then again in PyO3
    // converting them element by element. `eid` still has to be copied: it is
    // an object array of Python strings, and there is no buffer to borrow.
    let (src, dst, t) = (src.as_slice()?, dst.as_slice()?, t.as_slice()?);
    // pure Rust over borrowed data: nothing here touches Python
    py.detach(|| {
        let events = gnomon_core::motif::Events {
            src,
            dst,
            t,
            eid: &eid,
        };
        gnomon_core::motif::match_motifs(kind, &events, delta, collect).map_err(err)
    })
}

/// Interval-join pair enumeration (`co_active`).
///
/// Takes the per-row candidate ranges the caller already computed by binary
/// search and walks them. Returns `(a_row, b_row)` pairs in input order,
/// which is the order the operator contract promises.
#[pyfunction]
fn interval_pairs(
    py: Python<'_>,
    lo: Vec<u32>,
    hi: Vec<u32>,
    a_vt_e: Vec<i64>,
    b_vt_e: Vec<i64>,
    require_b_end_after_a_end: bool,
) -> Res<Vec<(u32, u32)>> {
    py.detach(|| {
        gnomon_core::interval::interval_pairs(
            &lo,
            &hi,
            &a_vt_e,
            &b_vt_e,
            require_b_end_after_a_end,
        )
        .map_err(err)
    })
}

/// Round-trip probe: proves the extension is importable,
/// the core crate is linked, and arrays cross into NumPy without a copy.
#[pyfunction]
#[pyo3(signature = (n = 8))]
fn ping(py: Python<'_>, n: usize) -> Bound<'_, PyArray1<i64>> {
    let v: Vec<i64> = (0..n as i64).collect();
    v.into_pyarray(py)
}

/// Version of the Rust core, for determinism receipts.
#[pyfunction]
fn core_version() -> &'static str {
    gnomon_core::VERSION
}

/// What this `.so` was built as, for every timing record to carry.
///
/// The 2026-09 engine-commit A/B diagnosis found 100% of the treatment's
/// decile growth sitting in the untimed residual, and a debug-assertions
/// build was the top candidate: `store::publish` runs an O(segments)
/// `debug_assert_eq!` every commit (kept in test builds "as a real net over
/// the incremental seal" — see its doc comment), which a release build never
/// pays. A number without this is not comparable to one that has it.
///
/// `profile`/`opt_level` come from `build.rs` forwarding the two env vars
/// Cargo sets only for build scripts (`PROFILE`, `OPT_LEVEL`) into
/// compile-time env vars this crate can read; `"unknown"` if a build ever
/// runs without that build script having seen them.
#[pyfunction]
fn build_info(py: Python<'_>) -> Bound<'_, PyDict> {
    let d = PyDict::new(py);
    for (k, v) in [
        ("profile", env!("GNOMON_BUILD_PROFILE")),
        ("opt_level", env!("GNOMON_BUILD_OPT_LEVEL")),
        ("engine_version", gnomon_core::VERSION),
    ] {
        d.set_item(k, v).expect("fresh dict accepts &str values");
    }
    d.set_item("debug_assertions", cfg!(debug_assertions))
        .expect("fresh dict accepts a bool value");
    // whether the per-phase ingest timers are compiled in: the baseline
    // of their overhead measurement is a build with them compiled out
    d.set_item("ingest_phase_timers", ip::ENABLED)
        .expect("fresh dict accepts a bool value");
    d.set_item(
        "manifest_format_version",
        gnomon_core::MANIFEST_FORMAT_VERSION,
    )
    .expect("fresh dict accepts a u32 value");
    d
}

/// Engine constants are exported so the Python side can *assert* agreement
/// with `gnomon.core.model` rather than trusting two copies of one number.
#[pymodule]
fn _engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeStore>()?;
    m.add_function(wrap_pyfunction!(interval_pairs, m)?)?;
    m.add_function(wrap_pyfunction!(motif_match, m)?)?;
    m.add_function(wrap_pyfunction!(ping, m)?)?;
    m.add_function(wrap_pyfunction!(core_version, m)?)?;
    m.add_function(wrap_pyfunction!(build_info, m)?)?;
    m.add("FORMAT_VERSION", gnomon_core::FORMAT_VERSION)?;
    m.add(
        "MANIFEST_FORMAT_VERSION",
        gnomon_core::MANIFEST_FORMAT_VERSION,
    )?;
    m.add("OPEN_END", gnomon_core::OPEN_END)?;
    m.add("BLOCK_ROWS", gnomon_core::defaults::BLOCK_ROWS)?;
    m.add(
        "ADJ_FILTER_MAX_IDS",
        gnomon_core::defaults::ADJ_FILTER_MAX_IDS,
    )?;
    m.add(
        "SEGMENT_TARGET_BYTES",
        gnomon_core::defaults::SEGMENT_TARGET_BYTES,
    )?;
    Ok(())
}
