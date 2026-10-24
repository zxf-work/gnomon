//! Per-phase timers for the **ingest** path: everything between a batch
//! arriving at the engine and it being durable, indexed and compacted.
//!
//! Observation only. Nothing here is read back by engine code — not for
//! correctness, not for scheduling — and with the `ingest-timers` feature off
//! every function below is an empty inline body, so the timers compile away
//! entirely (the control build the overhead measurement is scored against).
//!
//! **Process-global, like [`crate::phase`].** A build is one process, and the
//! build driver reads these once per progress line and once at the end. A
//! per-handle accumulator could be read off the wrong handle (the reader
//! reopen path replaces the native handle), which is the failure mode the
//! read-path timers were made global to avoid.
//!
//! **Granularity.** Timers sit per call, per batch, per segment or per
//! compaction — never per row. The two per-row splits the breakdown wants
//! (dictionary lookup vs row build inside staging) are *sampled*: one row in
//! [`STAGE_SAMPLE_EVERY`] is timed in two halves and the measured call total
//! is split in the sampled proportion.
//!
//! **Contexts.** The segment writer, the segment open path and the identity
//! postings fold are shared by several callers (a commit's seal, a
//! compaction's rewrite, the bulk path's endpoint-existence probe, a
//! correction's locate). Their slots are therefore indexed by a thread-local
//! [`Ctx`] the caller sets for the duration of its call, so the same code
//! path lands in the phase that caused it.

#[cfg(feature = "ingest-timers")]
use std::sync::atomic::{AtomicU64, Ordering};

/// Whether the timers are compiled in. Recorded in the build record so a
/// reader can tell the control configuration from the instrumented one.
pub const ENABLED: bool = cfg!(feature = "ingest-timers");

/// One timed row in this many inside `stage_edges`/`stage_nodes`.
pub const STAGE_SAMPLE_EVERY: usize = 64;

/// Which caller the shared code paths are working for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Ctx {
    Other = 0,
    /// `Staging::seal` inside a commit.
    Seal = 1,
    /// Either compactor (size-tiered merge or full-store rewrite).
    Compact = 2,
    /// The bulk path's "which endpoints already have a node version" probe.
    Probe = 3,
    /// A correction or retraction resolving the version it supersedes.
    Correct = 4,
    /// Building the identity filters of segments a commit or a
    /// compaction just wrote. Appended, never inserted. The filter build
    /// runs inside a commit's `Seal` guard and a compactor's `Compact`
    /// guard, both of which are function-scoped, so without its own context
    /// its segment opens were charged to those callers' slots while its
    /// time was counted in `*_vidfilter_ns` -- the same work in two phases,
    /// which drove `compaction.merge_rows` to zero on a small build.
    Vidfilter = 5,
}

pub const CTX_COUNT: usize = 6;
pub const CTX_NAMES: [&str; CTX_COUNT] =
    ["other", "seal", "compact", "probe", "correct", "vidfilter"];

/// Context-indexed slots: the shared code paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum S {
    /// `Staging::seal`: the `(vt_s, vid)` sort of the staged buffers.
    Sort = 0,
    /// `Staging::seal`: routing rows to the event / interval lane.
    SplitLanes,
    /// `Staging::seal`: choosing chunk boundaries (byte target, tie groups).
    ChunkPlan,
    /// `Staging::seal`: copying one chunk's rows into an owned buffer, and
    /// the same-batch close sidecar for it.
    RowCopy,
    /// `write_*_segment`: the per-row loop filling column builders and
    /// interning strings.
    ColumnFill,
    /// `write_segment`: trial-encoding every column (frame of reference).
    Encode,
    /// `write_segment`: string heap encode + DEFLATE pack.
    StringHeap,
    /// `write_segment`: header/footer JSON, buffer assembly, crc32c per
    /// extent and the body sha256.
    HeaderChecksum,
    /// `write_segment`: create + `write_all` of the temp file.
    FileWrite,
    /// `write_segment`: `sync_all` of the temp file.
    Fsync,
    /// `write_segment`: rename into place.
    Rename,
    /// After a segment is written: `stat` for the seal-size quotient and
    /// the manifest entry (key range, bounds).
    SegmentEntry,
    /// Count: segments written.
    Segments,
    /// Count: rows written into segments.
    RowsWritten,
    /// Segment opened through the session cache (miss path only): map,
    /// first-touch checksum verify, column decode.
    OpenCached,
    /// Segment opened for a one-pass walk (not cached): same work.
    OpenUncached,
    /// Count: segment opens (either kind).
    Opens,
    /// Identity postings fold of one segment (includes its uncached open).
    Fold,
    /// Count: rows folded into identity postings.
    FoldRows,
    /// Point read: locating an identity's candidate rows through the
    /// identity postings (folds on a miss).
    Locate,
    /// Point read: materializing the located rows (cached segment opens).
    RowsMaterialize,
    /// Point read: the staged (same-batch) half.
    Staged,
    // --- the encode step's trial, per candidate codec --------------------
    // Appended, never inserted. `encode_column` times each candidate it
    // builds; the sum of these is `Encode` minus the per-segment loop.
    /// Column bytes to `i64` values (`extent_values`).
    EncExtentNs,
    /// `codec::exact_scale` as called by `encode_column` (the linear-model
    /// arm's own internal call is charged to that arm).
    EncScaleNs,
    /// Frame of reference over divided values (`scale_down` + `encode_i64`).
    EncForScaledNs,
    /// Frame of reference, undivided (the original encode).
    EncForNs,
    /// Linear model with the divisor taken out (`linfor::encode(.., true)`).
    EncLinScaledNs,
    /// Linear model, undivided (`linfor::encode(.., false)`).
    EncLinNs,
    /// Choosing the smallest, and the raw fallback's copy.
    EncSelectNs,
    /// Count: columns encoded.
    EncColumns,
    /// Counts: which candidate won.
    EncWinForScaled,
    EncWinFor,
    EncWinLinScaled,
    EncWinLin,
    EncWinRaw,
}

pub const S_COUNT: usize = 35;
pub const S_NAMES: [&str; S_COUNT] = [
    "sort_ns",
    "split_lanes_ns",
    "chunk_plan_ns",
    "row_copy_ns",
    "column_fill_ns",
    "encode_ns",
    "string_heap_ns",
    "header_checksum_ns",
    "file_write_ns",
    "fsync_ns",
    "rename_ns",
    "segment_entry_ns",
    "segments",
    "rows_written",
    "open_cached_ns",
    "open_uncached_ns",
    "opens",
    "fold_ns",
    "fold_rows",
    "locate_ns",
    "rows_materialize_ns",
    "staged_ns",
    "enc_extent_ns",
    "enc_scale_ns",
    "enc_for_scaled_ns",
    "enc_for_ns",
    "enc_lin_scaled_ns",
    "enc_lin_ns",
    "enc_select_ns",
    "enc_columns",
    "enc_win_for_scaled",
    "enc_win_for",
    "enc_win_lin_scaled",
    "enc_win_lin",
    "enc_win_raw",
];

/// Global (context-free) slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum G {
    // --- PyO3 method bodies (set by the binding crate) ------------------
    PyBegin = 0,
    PyStageEdges,
    PyStageNodes,
    PyEnsureEntities,
    PyBelievedAny,
    PyBelieved,
    PyStageCloses,
    PyStageRetires,
    PyCommit,
    PyNeedsCompaction,
    PyCompactIncremental,
    PyCompact,
    PyGc,
    PyCalls,
    // --- staging, sampled per-row split -----------------------------------
    StageRows,
    StageSampledRows,
    StageSampledLookupNs,
    StageSampledBuildNs,
    // --- dictionary --------------------------------------------------------
    DictEnsureCalls,
    // --- endpoint existence probe (bulk path) ------------------------------
    ProbePointNs,
    ProbeScanNs,
    ProbeUids,
    ProbeCalls,
    // --- correction: close resolution --------------------------------------
    CloseLocateNs,
    CloseOverlayNs,
    ClosesCommitted,
    ClosesStaged,
    // --- commit (CommitPhases, accumulated) --------------------------------
    CommitTotalNs,
    CommitCaptureNs,
    CommitRatioSeedNs,
    CommitSealNs,
    CommitClosesNs,
    CommitStatsNs,
    CommitDictNs,
    CommitDigestNs,
    CommitDeltaBuildNs,
    CommitManifestNs,
    CommitCurrentNs,
    CommitPinNs,
    Commits,
    CommitCheckpoints,
    CommitManifestBytes,
    // --- compaction trigger ------------------------------------------------
    TriggerNs,
    TriggerCalls,
    // --- size-tiered (incremental) compaction -------------------------------
    IncTotalNs,
    IncPlanNs,
    IncCloseIndexNs,
    IncMergeNs,
    IncFlushNs,
    IncSpliceNs,
    IncManifestSealNs,
    IncPublishNs,
    IncAdoptNs,
    IncCalls,
    IncRowsRewritten,
    // --- full-store compaction ----------------------------------------------
    FullTotalNs,
    FullReadNs,
    FullRestageNs,
    FullSealNs,
    FullManifestSealNs,
    FullPublishNs,
    FullAdoptNs,
    FullCalls,
    FullRowsRewritten,
    // --- gc ----------------------------------------------------------------
    GcTotalNs,
    GcCalls,
    // --- the per-segment identity filter's build -------------------------
    // Appended, never inserted: every slot above keeps its index and name.
    // A commit builds the filters of the segments it sealed (step 6, after
    // the publication point); compaction and settle build them in
    // `install`, beside publish and adopt.
    CommitVidfilterNs,
    IncVidfilterNs,
    FullVidfilterNs,
    // --- endpoint-existence probe answered from the believed-node count ---
    // Appended for the same reason: reconciling the count against the
    // manifest, and answering one call's uids from it.
    ProbeCountReconcileNs,
    ProbeCountAnswerNs,
    // --- the identity-filter build, split ---------------------------------
    // Appended. Summed over every caller (commit, compaction, settle); the
    // per-caller totals stay `commit_vidfilter_ns` / `inc_vidfilter_ns` /
    // `full_vidfilter_ns`, and the segment open stays
    // `vidfilter.open_uncached_ns`.
    /// `read::identity_keys` for one new segment.
    VfKeysNs,
    /// `VidFilter::build`: sort, dedup, peel.
    VfConstructNs,
    /// `VidFilter::write` whole: create, write, fsync, rename.
    VfWriteNs,
    /// The `sync_all` inside `VidFilter::write` (any caller; the lazy build
    /// path does not fire on ingest). Always zero now that the filter write
    /// has no fsync; the slot stays so that names never move.
    VfFsyncNs,
    /// Mapping the written file back (`VidFilter::load`).
    VfRemapNs,
    /// Count: filters built at seal / compaction / settle.
    VfBuilt,
    /// Count: keys those filters were built over (rows, before dedup).
    VfKeys,
    // --- the raw-record `ingest_events` fast path -------------------------
    // Appended. `py_ingest_raw_ns` is the binding's body; the five `_ns`
    // slots below divide it (the probe slot is the endpoint-existence
    // probe's share, whose own `probe.*` timers run inside it).
    PyIngestRaw,
    IngestRawParseNs,
    IngestRawProbeNs,
    IngestRawEnsureNs,
    IngestRawIdentityNs,
    IngestRawStageNs,
    /// Count: edge / node rows staged by the fast path.
    IngestRawEdgeRows,
    IngestRawNodeRows,
    /// Count: calls that fell back to the Python path (shape not accepted).
    IngestRawFallbacks,
    // --- the write path's windowed point read (`pointread.rs`) -----------
    // Appended. The binding's body of `believed_window`; its own
    // `correct.*` timers run inside it, exactly as for `believed`.
    PyBelievedWindow,
    // --- A transaction applied from its record --------------
    // Appended. `py_apply_raw_ns` is the binding's body of `apply_ops_raw`;
    // the slots below divide it. Its point reads run the `correct.*` timers
    // and its closes the `close_*` timers, exactly as through
    // `believed_window` / `stage_closes`; an `ingest_events` op inside it
    // runs the `ingest_raw_*` timers.
    PyApplyRaw,
    /// Parsing the record and checking every op's shape.
    ApplyRawParseNs,
    /// `ensure_entity` calls.
    ApplyRawEnsureNs,
    /// The windowed point reads.
    ApplyRawReadNs,
    /// Retires and closes (`_supersede_*`).
    ApplyRawSupersedeNs,
    /// Staging the built rows.
    ApplyRawStageNs,
    /// `ingest_events` ops inside a record (the raw ingest path).
    ApplyRawIngestNs,
    /// Count: records applied / declined / refused.
    ApplyRawApplied,
    ApplyRawDeclined,
    ApplyRawRefused,
}

pub const G_COUNT: usize = 98;
pub const G_NAMES: [&str; G_COUNT] = [
    "py_begin_ns",
    "py_stage_edges_ns",
    "py_stage_nodes_ns",
    "py_ensure_entities_ns",
    "py_believed_any_ns",
    "py_believed_ns",
    "py_stage_closes_ns",
    "py_stage_retires_ns",
    "py_commit_ns",
    "py_needs_compaction_ns",
    "py_compact_incremental_ns",
    "py_compact_ns",
    "py_gc_ns",
    "py_calls",
    "stage_rows",
    "stage_sampled_rows",
    "stage_sampled_lookup_ns",
    "stage_sampled_build_ns",
    "dict_ensure_calls",
    "probe_point_ns",
    "probe_scan_ns",
    "probe_uids",
    "probe_calls",
    "close_locate_ns",
    "close_overlay_ns",
    "closes_committed",
    "closes_staged",
    "commit_total_ns",
    "commit_capture_ns",
    "commit_ratio_seed_ns",
    "commit_seal_ns",
    "commit_closes_ns",
    "commit_stats_ns",
    "commit_dict_ns",
    "commit_digest_ns",
    "commit_delta_build_ns",
    "commit_manifest_ns",
    "commit_current_ns",
    "commit_pin_ns",
    "commits",
    "commit_checkpoints",
    "commit_manifest_bytes",
    "trigger_ns",
    "trigger_calls",
    "inc_total_ns",
    "inc_plan_ns",
    "inc_close_index_ns",
    "inc_merge_ns",
    "inc_flush_ns",
    "inc_splice_ns",
    "inc_manifest_seal_ns",
    "inc_publish_ns",
    "inc_adopt_ns",
    "inc_calls",
    "inc_rows_rewritten",
    "full_total_ns",
    "full_read_ns",
    "full_restage_ns",
    "full_seal_ns",
    "full_manifest_seal_ns",
    "full_publish_ns",
    "full_adopt_ns",
    "full_calls",
    "full_rows_rewritten",
    "gc_total_ns",
    "gc_calls",
    "commit_vidfilter_ns",
    "inc_vidfilter_ns",
    "full_vidfilter_ns",
    "probe_count_reconcile_ns",
    "probe_count_answer_ns",
    "vf_keys_ns",
    "vf_construct_ns",
    "vf_write_ns",
    "vf_fsync_ns",
    "vf_remap_ns",
    "vf_built",
    "vf_keys",
    "py_ingest_raw_ns",
    "ingest_raw_parse_ns",
    "ingest_raw_probe_ns",
    "ingest_raw_ensure_ns",
    "ingest_raw_identity_ns",
    "ingest_raw_stage_ns",
    "ingest_raw_edge_rows",
    "ingest_raw_node_rows",
    "ingest_raw_fallbacks",
    "py_believed_window_ns",
    "py_apply_raw_ns",
    "apply_raw_parse_ns",
    "apply_raw_ensure_ns",
    "apply_raw_read_ns",
    "apply_raw_supersede_ns",
    "apply_raw_stage_ns",
    "apply_raw_ingest_ns",
    "apply_raw_applied",
    "apply_raw_declined",
    "apply_raw_refused",
];

#[cfg(feature = "ingest-timers")]
mod imp {
    use super::*;
    use std::cell::Cell;

    #[allow(clippy::declare_interior_mutable_const)]
    const ZERO: AtomicU64 = AtomicU64::new(0);
    pub(super) static GLOBAL: [AtomicU64; G_COUNT] = [ZERO; G_COUNT];
    pub(super) static BY_CTX: [[AtomicU64; S_COUNT]; CTX_COUNT] = [
        [ZERO; S_COUNT],
        [ZERO; S_COUNT],
        [ZERO; S_COUNT],
        [ZERO; S_COUNT],
        [ZERO; S_COUNT],
        [ZERO; S_COUNT],
    ];

    thread_local! {
        pub(super) static CTX: Cell<u8> = const { Cell::new(0) };
    }
}

/// A started timer. Zero-sized and inert when the feature is off.
#[must_use]
pub struct Timer {
    #[cfg(feature = "ingest-timers")]
    t: std::time::Instant,
}

impl Timer {
    #[inline]
    pub fn start() -> Self {
        Timer {
            #[cfg(feature = "ingest-timers")]
            t: std::time::Instant::now(),
        }
    }

    /// Elapsed nanoseconds so far (0 with the feature off).
    #[inline]
    pub fn ns(&self) -> u64 {
        #[cfg(feature = "ingest-timers")]
        {
            self.t.elapsed().as_nanos() as u64
        }
        #[cfg(not(feature = "ingest-timers"))]
        {
            0
        }
    }

    /// Stop into a global slot; returns the nanoseconds recorded.
    #[inline]
    pub fn stop(self, g: G) -> u64 {
        let ns = self.ns();
        add(g, ns);
        ns
    }

    /// Stop into a slot of the current context.
    #[inline]
    pub fn stop_ctx(self, s: S) -> u64 {
        let ns = self.ns();
        add_ctx(s, ns);
        ns
    }
}

/// Add to a global slot.
#[inline]
pub fn add(g: G, v: u64) {
    #[cfg(feature = "ingest-timers")]
    imp::GLOBAL[g as usize].fetch_add(v, Ordering::Relaxed);
    #[cfg(not(feature = "ingest-timers"))]
    let _ = (g, v);
}

/// Add to a slot of the calling thread's current context.
#[inline]
pub fn add_ctx(s: S, v: u64) {
    #[cfg(feature = "ingest-timers")]
    {
        let c = imp::CTX.with(|c| c.get()) as usize;
        imp::BY_CTX[c][s as usize].fetch_add(v, Ordering::Relaxed);
    }
    #[cfg(not(feature = "ingest-timers"))]
    let _ = (s, v);
}

/// The calling thread's context.
#[inline]
pub fn current() -> Ctx {
    #[cfg(feature = "ingest-timers")]
    {
        match imp::CTX.with(|c| c.get()) {
            1 => Ctx::Seal,
            2 => Ctx::Compact,
            3 => Ctx::Probe,
            4 => Ctx::Correct,
            5 => Ctx::Vidfilter,
            _ => Ctx::Other,
        }
    }
    #[cfg(not(feature = "ingest-timers"))]
    {
        Ctx::Other
    }
}

/// Restores the previous context when dropped.
#[must_use]
pub struct CtxGuard {
    #[cfg(feature = "ingest-timers")]
    prev: u8,
}

impl CtxGuard {
    /// Enter `ctx` unconditionally.
    #[inline]
    pub fn enter(ctx: Ctx) -> Self {
        #[cfg(feature = "ingest-timers")]
        {
            let prev = imp::CTX.with(|c| c.replace(ctx as u8));
            CtxGuard { prev }
        }
        #[cfg(not(feature = "ingest-timers"))]
        {
            let _ = ctx;
            CtxGuard {}
        }
    }

    /// Enter `ctx` only when no caller has claimed the work yet — a point
    /// read inside the probe stays the probe's, one issued by a correction
    /// becomes the correction's.
    #[inline]
    pub fn enter_if_unclaimed(ctx: Ctx) -> Self {
        if current() == Ctx::Other {
            Self::enter(ctx)
        } else {
            Self::enter(current())
        }
    }
}

impl Drop for CtxGuard {
    #[inline]
    fn drop(&mut self) {
        #[cfg(feature = "ingest-timers")]
        imp::CTX.with(|c| c.set(self.prev));
    }
}

/// Every slot as `(name, value)`: the globals, then `<ctx>.<slot>` for each
/// context. Empty when the feature is off.
pub fn snapshot() -> Vec<(String, u64)> {
    #[cfg(feature = "ingest-timers")]
    {
        let mut out = Vec::with_capacity(G_COUNT + CTX_COUNT * S_COUNT);
        for (i, name) in G_NAMES.iter().enumerate() {
            out.push((name.to_string(), imp::GLOBAL[i].load(Ordering::Relaxed)));
        }
        for (c, cname) in CTX_NAMES.iter().enumerate() {
            for (i, name) in S_NAMES.iter().enumerate() {
                out.push((
                    format!("{cname}.{name}"),
                    imp::BY_CTX[c][i].load(Ordering::Relaxed),
                ));
            }
        }
        out
    }
    #[cfg(not(feature = "ingest-timers"))]
    {
        Vec::new()
    }
}

/// Zero every slot. Instrumentation only.
pub fn reset() {
    #[cfg(feature = "ingest-timers")]
    {
        for a in imp::GLOBAL.iter() {
            a.store(0, Ordering::Relaxed);
        }
        for row in imp::BY_CTX.iter() {
            for a in row.iter() {
                a.store(0, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_cover_every_slot() {
        assert_eq!(G_NAMES.len(), G::ApplyRawRefused as usize + 1);
        assert_eq!(S_NAMES.len(), S::EncWinRaw as usize + 1);
        assert_eq!(CTX_NAMES.len(), Ctx::Vidfilter as usize + 1);
        let mut all: Vec<&str> = G_NAMES.iter().chain(S_NAMES.iter()).copied().collect();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), G_COUNT + S_COUNT, "slot names must be unique");
    }

    #[test]
    fn a_guard_restores_the_previous_context() {
        assert_eq!(current(), Ctx::Other);
        {
            let _a = CtxGuard::enter(Ctx::Compact);
            if ENABLED {
                assert_eq!(current(), Ctx::Compact);
            }
            {
                let _b = CtxGuard::enter_if_unclaimed(Ctx::Correct);
                if ENABLED {
                    assert_eq!(current(), Ctx::Compact, "claimed work keeps its owner");
                }
            }
            if ENABLED {
                assert_eq!(current(), Ctx::Compact);
            }
        }
        assert_eq!(current(), Ctx::Other);
    }

    #[test]
    fn the_snapshot_is_empty_exactly_when_the_timers_are_compiled_out() {
        let snap = snapshot();
        if ENABLED {
            assert_eq!(snap.len(), G_COUNT + CTX_COUNT * S_COUNT);
        } else {
            assert!(snap.is_empty());
        }
    }
}
