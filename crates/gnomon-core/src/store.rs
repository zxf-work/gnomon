//! Store layout, recovery, and the commit protocol.
//!
//! Durability objective: the store is a deterministic
//! materialization of the event log, so we do not need to make every partial
//! physical update recoverable — we need to *never expose an undetected
//! inconsistent generation*. Everything here serves that: a commit publishes
//! files first and flips a single pointer last, so any crash leaves the
//! previous generation intact and the partial work orphaned.
//!
//! Commit order (each step durable before the next begins):
//!
//! 1. the event log is appended and fsynced — by the Python `Store`, before
//!    the engine is called at all (write-ahead, `store.py::_write`);
//! 2. segment and close-run files are written and fsynced;
//! 3. the dictionary tail is appended and fsynced;
//! 4. `manifests/<G>.json` is written, fsynced, renamed, and its directory
//!    fsynced — as a delta against generation G−1, or as a full checkpoint
//!    every `K`th generation (`manifest_chain`);
//! 5. `CURRENT` is rewritten atomically — **the publication point**.
//!
//! A crash before step 5 is invisible: `open` reads the old `CURRENT`, and
//! the orphaned manifest and dictionary tail are ignored (and the tail
//! truncated). A crash during step 5 leaves either the old or the new
//! pointer, never a torn one, because the rename is atomic.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::dict::Dictionary;
use crate::error::{EngineError, Result};
use crate::derive::Id96;
use crate::manifest::{self, merkle, CloseRunRef, DictRef, EventLogRef, Manifest, Stats, Widths};
use crate::integrity::{self, Finding};
use crate::manifest_chain::{self, AppendSpan, ManifestDelta};
use crate::row::{EdgeRow, Lane, NodeRow, RowKind};
use crate::segment::MmapSource;
use crate::staging::{PartitionMap, Staging};
use crate::visibility::{read_close_run, write_close_run, CloseIndex, CloseRecord};
use crate::ingest_phase;

const CURRENT: &str = "CURRENT";

/// Durability-injection point. A no-op unless `GNOMON_CRASH_POINT`
/// names this exact point, in which case the process aborts — no unwinding,
/// no destructors — so the on-disk state is what a power cut at this line
/// would leave. The env read costs one lookup per call on the write path
/// only; the durability-testing harness is the only intended
/// setter.
pub(crate) fn crash_point(name: &str) {
    if let Ok(v) = std::env::var("GNOMON_CRASH_POINT") {
        if v == name {
            eprintln!("GNOMON_CRASH_POINT hit: {name}");
            std::process::abort();
        }
    }
}
const DICT: &str = "dict.log";
const SUBDIRS: [&str; 4] = ["manifests", "seg", "close", "idx"];
/// Marker written by `compact_current_only`. A store carrying it
/// has physically discarded its superseded versions, so it must refuse the
/// questions it can no longer answer honestly — past-belief reads and new
/// corrections — no matter which handle opens it.
const CURRENT_ONLY_MARKER: &str = "CURRENT_ONLY";

pub struct NativeStore {
    root: PathBuf,
    /// Canonicalized root — the key under which this handle pins its
    /// generation in the process-global reader table (gc.rs). Canonical so
    /// two handles opened through different spellings of one path agree.
    pin_key: PathBuf,
    dict: Dictionary,
    /// The reconstructed manifest of the generation this handle sees — and,
    /// on the write path, the *working* manifest a commit mutates in place.
    ///
    /// It used to be cloned three times per commit: once by `successor`, once
    /// by `seal`'s `body_sha`, once by `publish`'s redundant `verify`. That is
    /// 5.9 µs per live segment per commit, and 100% of the measured 1.8×
    /// last/first-decile growth. The store already carried
    /// this across commits; it simply did not reuse it.
    manifest: Manifest,
    /// The Merkle state of `manifest`, carried across commits so a commit's
    /// digest costs O(appended + log n) rather than a full re-serialization.
    /// `None` on a store this build may not write (formats 1 and 2), where
    /// there is no such digest to maintain.
    merkle: Option<manifest::merkle::ManifestMerkle>,
    /// Generation of the last full checkpoint on the manifest chain — where
    /// `open` would start replaying deltas from, and what each delta this
    /// handle writes records as its `checkpoint` hint.
    checkpoint_gen: u64,
    /// Per-handle override of the checkpoint interval (`None` = the process
    /// default, `GNOMON_MANIFEST_CHECKPOINT_EVERY` or
    /// `defaults::MANIFEST_CHECKPOINT_EVERY`). Exists for the same reason
    /// `set_segment_cache_budget` does: a sweep or a test must be able to
    /// change K without mutating the process environment, which races other
    /// threads.
    checkpoint_every: Option<u64>,
    staging: Staging,
    /// Closes landing on rows staged in this same batch — folded into the
    /// owning segment's sidecar at seal, so no run file is needed.
    staged_closes: HashMap<Id96, i64>,
    /// Closes landing on already-committed rows, written as a close run.
    pending_closes: Vec<CloseRecord>,
    partitions: PartitionMap,
    segment_target_bytes: u64,
    /// Cost-guardrail statistics, maintained incrementally (stats
    /// are served from the manifest, never by scanning). Built once on first
    /// use, then each commit folds in its own batch, so a write-then-read
    /// loop never rescans.
    stats: std::sync::Mutex<Option<StatsAccum>>,
    /// What `stats()` reports, derived from `stats` once per manifest
    /// generation: the scalar summary, `max_out_degree` included, so
    /// a per-call statistics read neither clones the per-source out-degree
    /// map nor rescans it. Keyed by the generation it was derived at and
    /// cleared wherever `stats` itself changes; never persisted.
    stats_summary: std::sync::Mutex<Option<(u64, std::sync::Arc<StatsSummary>)>>,
    /// Believed node versions per dense id, for the current-belief
    /// endpoint-existence probe (`node_belief.rs`). Derived, never persisted,
    /// built by this handle's first such probe and reconciled against the
    /// manifest by every later one; `None` until then, and reset to `None`
    /// if a reconcile fails, so a partial state is never trusted.
    node_belief: std::sync::Mutex<Option<crate::node_belief::NodeBelief>>,
    /// Identity postings, built incrementally. Segments are
    /// immutable and manifests only accumulate, so a segment is indexed once
    /// and never revisited — which keeps a store built by N single-row
    /// batches linear overall instead of quadratic.
    edge_postings: std::sync::Mutex<Postings>,
    node_postings: std::sync::Mutex<Postings>,
    /// Segments whose checksums have been walked in this session. Corruption
    /// must be caught *before* rows reach a query, but re-hashing a file on
    /// every read would be ruinous — so each segment is verified the first
    /// time this process opens it, and trusted thereafter. Segments are
    /// immutable, so once verified they stay valid for the session.
    verified: std::sync::Mutex<std::collections::HashSet<String>>,
    /// The close index the current generation makes visible, built once per
    /// generation and shared by every read (keyed by manifest generation).
    /// Sound for the same reason the segment cache is: close-run files are
    /// immutable, and the visible set of runs only changes when a commit
    /// publishes a new manifest — so within one generation the rebuilt index
    /// is always identical. Without this, every point read re-reads every
    /// run file, which made correction-heavy replay quadratic.
    close_cache: std::sync::Mutex<Option<CloseCacheEntry>>,
    /// `segment id -> filename`, per lane, built once per generation.
    ///
    /// Every point read — `locate`, `locate_open`, `locate_vid` — needs to
    /// turn a posting's segment id back into a file, and each was rebuilding
    /// this from the manifest per call: a String clone *and* a filename parse
    /// per segment, to answer a question about one identity. At 1,000
    /// segments that was 94% of a `believed_*` call. Sound for the same
    /// reason `close_cache` is: the segment set changes only when a commit
    /// publishes a new manifest.
    files_cache: std::sync::Mutex<Option<(u64, std::sync::Arc<SegmentFiles>)>>,
    /// The open batch's `pending_closes`, as a layer over the committed
    /// index, so a read inside the batch sees them. Maintained as
    /// they are recorded rather than rebuilt per read: `apply_ops` reads
    /// belief once per op, and rebuilding a K-close overlay on each of K
    /// reads is the "small lookup rebuilding the whole store" shape this
    /// engine has now met six times.
    ///
    /// `Arc::make_mut` is what keeps it cheap *and* honest: the layer is
    /// updated in place while this handle holds the only reference, and
    /// copied if a reader is still holding one — so no reader ever sees a
    /// close appear underneath it.
    pending_overlay: Option<std::sync::Arc<CloseIndex>>,
    /// Open segments, by file name. Sound because segment files are
    /// immutable — closes live in separate .tgc files and compaction writes
    /// new files — so a cached entry can never be stale. This is also what
    /// makes compressed columns viable: they are decoded once per process
    /// here, not once per operator call.
    ///
    /// Byte-budgeted: the unbounded form was measured at a ~6 GB per-process
    /// floor for a 268 MB store: least-recently-used whole segments are dropped
    /// once the accounted bytes exceed the budget, and an evicted segment
    /// simply reopens on next touch — checksum walk skipped via `verified`,
    /// which is never evicted, so the once-per-session guarantee holds.
    /// Correctness never depends on residency; the budget trades re-decode
    /// latency for memory. `GNOMON_SEGMENT_CACHE_BYTES` overrides (0 =
    /// unbounded); the default is half of detected physical RAM.
    segments: std::sync::Mutex<SegmentCache>,
    /// Bytes for every segment file this handle's generation names, mapped
    /// once at open and re-pinned on every commit, compaction, or format
    /// upgrade (`pin_generation_files`). This is the fix, not the
    /// cache above: `open_segment`/`open_segment_uncached` serve from here
    /// when present, so the protected set is the whole generation rather
    /// than the subset a query happened to touch — gc's own doc comment
    /// used to claim "segments via mmap" as the reason a collected file
    /// stays readable, when mapping was really per-first-touch. Empty when
    /// `GNOMON_PIN_MAX_SEGMENTS` disables pinning for this handle (a store
    /// whose segment count compaction does not bound), in which case
    /// `open_segment` falls back to the pre-fix lazy `MmapSource::load`.
    pinned_sources: std::sync::Mutex<HashMap<String, MmapSource>>,
    /// Set once this handle has logged the `GNOMON_PIN_MAX_SEGMENTS` fallback,
    /// so a store that hovers around the cap does not spam a line per
    /// commit.
    pin_fallback_logged: std::sync::atomic::AtomicBool,
    /// Per-segment TCSR indexes, keyed by segment id (`tcsr.rs`).
    ///
    /// Not keyed by generation, and deliberately not cleared on commit: the
    /// index is built over *all stored rows* of an immutable segment, so it
    /// is a pure function of that segment's bytes. A commit publishes new
    /// segments, which simply have no entry yet; it can never invalidate an
    /// existing one. That is the property the counters below verify.
    tcsr_cache: std::sync::Mutex<HashMap<u64, std::sync::Arc<crate::tcsr::SegmentTcsr>>>,
    /// How many segment indexes this handle built from columns, and how many
    /// it loaded from `idx/tcsr/`. Counters, not timings: the claim is "after a
    /// write batch, only the new segments are rebuilt", which is an
    /// assertion about counts.
    tcsr_built: std::sync::atomic::AtomicU64,
    tcsr_loaded: std::sync::atomic::AtomicU64,
    /// Per-segment identity filters, mapped per **handle** and swept
    /// by [`Self::evict_segments_not_in`] with the segments they belong to.
    ///
    /// Deliberately *not* process-global. `nameidx::CACHE` is: it is
    /// unbounded, it has no gc hook, nothing outside tests prunes it, and
    /// `nameidx::CACHE` is logged as a latent leak. This map takes the postings' shape
    /// instead — a byte budget, an LRU victim and a `retain_segments` sweep
    /// — so a compaction cycle cannot grow it without bound.
    ///
    /// The value is an `Option`, and the `None` is load-bearing: a store
    /// with no `idx/vidfilter/` would otherwise cost one failed `open` per
    /// segment per probe, a syscall where today there is none. Absence is
    /// remembered, and every path that *writes* a filter replaces the entry
    /// it wrote.
    vid_filters: std::sync::Mutex<Filters>,
    /// Probe counters for the identity filter, **per row kind** so they sit
    /// beside that kind's `folds` in the same receipt and `folds ÷ probes`
    /// — the mechanism gate — is one division inside one dict rather than a
    /// join across two. Indexed by [`kind_slot`].
    ///
    /// `empty_positives ÷ positives` measures ε in situ: a positive whose
    /// `rows_for(key)` came back empty is a false positive by definition.
    filter_counters: [FilterCounters; 2],
    /// Filters this handle built eagerly from columns, loaded from
    /// `idx/vidfilter/`, and built *lazily* on the way out of a fold it had
    /// to perform anyway. The third is the legacy-store fallback: it is
    /// measured and reported, and nothing relies on it, because for the case
    /// this case the first use is the entire cost.
    filter_built: std::sync::atomic::AtomicU64,
    filter_loaded: std::sync::atomic::AtomicU64,
    filter_lazy_built: std::sync::atomic::AtomicU64,
    /// Transaction time of the open batch, if any (`begin` .. `commit`).
    batch_tt: Option<i64>,
    /// True when the `CURRENT_ONLY` marker is present: the stripped
    /// current-only configuration, which has dropped historical
    /// versions and must refuse past-belief queries and corrections.
    current_only: bool,
    /// Opened by [`NativeStore::open_read_only`]: this handle never
    /// writes under the store directory. Every writer entry point is refused
    /// before it writes anything, and the three lazily built sidecars — the
    /// identity filter, the per-segment adjacency index and the sorted name
    /// index — are built in memory and never persisted.
    read_only: bool,
    /// Which engine designs this handle runs with, read from the
    /// environment once, at open. [`crate::designs::Designs::ALL`] unless an
    /// ablation switch was set.
    designs: crate::designs::Designs,
    /// `GNOMON_TCSR=python` at open. In that configuration traversal runs
    /// on the adapter's whole-store NumPy index and `idx/tcsr/` must stay
    /// untouched, so a filtered scan takes no adjacency postings
    /// and scans as before. Not a design switch: the per-segment index is
    /// simply not this configuration's adjacency source.
    tcsr_python: bool,
    /// Where the last commit spent its time (instrumentation, not contract).
    last_commit: Option<CommitPhases>,
    /// `(publish_ns, adopt_ns)` of the last `install` — read by the two
    /// compactors into their ingest-phase slots (`ingest_phase.rs`).
    /// Instrumentation only; zero with the timers compiled out.
    pub(crate) install_ns: (u64, u64, u64),
    /// Where this handle's `open` call spent its time (instrumentation, not
    /// contract) — always populated, since every live handle came from a
    /// successful `open`.
    open_phases: OpenPhases,
    /// How this handle's segment opens resolved (instrumentation, not
    /// contract) — see [`OpenPathCounters`].
    open_counters: OpenPathCounters,
    /// Running maximum of every component [`NativeStore::memory_accounting`]
    /// reports, over the samples this handle has been asked for.
    ///
    /// A maximum over *samples*, not a true high-water mark: nothing here
    /// hooks an allocator, and the point of the accessor is that it costs
    /// nothing between calls. A caller that samples at its own progress
    /// cadence — which is how `postings_peak_bytes` is already collected —
    /// gets the same guarantee that receipt does, no more.
    mem_peaks: std::sync::Mutex<std::collections::BTreeMap<&'static str, u64>>,
}

/// Per-handle counters for the segment-open path: how many opens were served
/// from the session cache, how many had to decode, how many skipped the cache
/// entirely, and how many bytes each of those cost.
///
/// The role [`NativeStore::segment_cache_stats`] plays for cache *residency*,
/// this plays for cache *traffic*. Residency alone cannot distinguish "the
/// query path never inserts" from "the query path inserts and something
/// evicts": both read `entries = 0`. These counters separate them, and they
/// are what turns "segments appear to be re-opened per query" into a reading
/// rather than an inference from latency.
///
/// Counters only, never a correctness input: nothing reads them back, and a
/// relaxed ordering is deliberate — the numbers are per-handle totals, not a
/// synchronisation point on the read hot path.
#[derive(Default, Debug)]
pub struct OpenPathCounters {
    /// `open_segment` calls answered by the session cache without decoding.
    cached_hits: std::sync::atomic::AtomicU64,
    /// `open_segment` calls that missed and decoded a segment into the cache.
    cached_misses: std::sync::atomic::AtomicU64,
    /// `open_segment_uncached` calls — decoded, used, dropped by the caller.
    uncached_opens: std::sync::atomic::AtomicU64,
    /// `resident_bytes` summed over every decode either miss path performed,
    /// so "how much decoding did this sweep actually repeat" is a number.
    decoded_bytes: std::sync::atomic::AtomicU64,
    /// `resident_bytes` summed over **every** open, hit or miss, so the delta
    /// across one query is the bytes of the segments that query reached.
    /// `decoded_bytes` prices re-decoding; this prices *reach* — the read
    /// amplification a layout imposes even when nothing is decoded twice,
    /// which is the quantity a manifest prune is supposed to cut.
    touched_bytes: std::sync::atomic::AtomicU64,
    /// Rows the scan's verdict ranges handed to a fold, summed over the read
    /// paths that compute selections — `aggregate_edges` and the edge/node
    /// scans. The delta across one query is that query's rows scanned, which
    /// is the other half of read amplification: bytes reached, rows walked.
    selected_rows: std::sync::atomic::AtomicU64,
    /// Opens that walked the checksums because `verified` had not yet named
    /// the file — the once-per-session first touch.
    first_touch_verifies: std::sync::atomic::AtomicU64,
    /// Opens whose bytes came from the pinned map, versus opens that
    /// had to `MmapSource::load` a fresh mapping (the pin-cap fallback or the
    /// open-vs-gc race).
    pinned_source_hits: std::sync::atomic::AtomicU64,
    fresh_map_loads: std::sync::atomic::AtomicU64,
}

/// `(cached_hits, cached_misses, uncached_opens, decoded_bytes,
/// touched_bytes, selected_rows, first_touch_verifies, pinned_source_hits,
/// fresh_map_loads)`.
pub type OpenPathStats = (u64, u64, u64, u64, u64, u64, u64, u64, u64);

impl OpenPathCounters {
    #[inline]
    fn bump(c: &std::sync::atomic::AtomicU64, n: u64) {
        c.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    fn snapshot(&self) -> OpenPathStats {
        let g = |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed);
        (
            g(&self.cached_hits),
            g(&self.cached_misses),
            g(&self.uncached_opens),
            g(&self.decoded_bytes),
            g(&self.touched_bytes),
            g(&self.selected_rows),
            g(&self.first_touch_verifies),
            g(&self.pinned_source_hits),
            g(&self.fresh_map_loads),
        )
    }
}

/// Wall-clock microseconds per phase of one commit, plus the two numbers
/// that decide whether the singleton-write floor is fsyncs or manifest size.
///
/// The 265× batch-vs-singleton gap is attributed to "several
/// fsyncs" by one account, and to "a fresh full
/// manifest per commit". Those are different fixes, so the split is measured
/// rather than argued.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitPhases {
    /// Step 1: `CommitBase::capture` — the O(log n) snapshot of scalars plus
    /// the Merkle state, taken before `commit_inner` touches anything, so a
    /// failed commit can roll back to it (previously untimed, folded
    /// into the residual between `commit_start` and `seal_us`).
    pub capture_us: u64,
    /// Step 2: staged rows sealed into segment files, written and fsynced.
    pub seal_us: u64,
    /// Step 2b: the close run for corrections against committed rows.
    pub closes_us: u64,
    /// Folding this batch into the running statistics.
    pub stats_us: u64,
    /// Step 3: the dictionary tail, written and fsynced.
    pub dict_us: u64,
    /// `Manifest::seal_with` — the manifest digest, O(1) from the maintained
    /// Merkle state at format >= 3, O(segments) (`legacy_body_sha`) below it
    /// (previously untimed: the gap between `dict_us` and step 4's
    /// JSON build was found to absorb the format-2
    /// configurations' decile growth).
    pub digest_us: u64,
    /// `publish`'s `debug_assert_eq!` that recomputes the manifest digest
    /// from scratch (O(segments)) to double-check `seal_with`'s incremental
    /// one — compiled out entirely in release builds, where this is always
    /// 0. In debug builds this used to run untimed ahead of `manifest_us`'s
    /// timer, so its O(segments) cost was folded into the residual and grew
    /// with generation count (`commit_phases_fully_account_for_total_us_
    /// over_sixty_singleton_commits`, CI 2026-09-15).
    pub debug_verify_us: u64,
    /// Step 4a: building the manifest record's JSON — a delta
    /// (`ManifestDelta::from_appends`, O(appended)) most generations, or a
    /// full checkpoint (`manifest_chain::checkpoint_json`, O(segments)) every
    /// `every`th one. Split from `manifest_us` because it used to run before
    /// that timer started.
    pub delta_build_us: u64,
    /// Step 4b: manifest JSON written, fsynced, renamed, dir fsynced.
    pub manifest_us: u64,
    /// Step 5: `CURRENT` rewritten atomically — the publication point.
    pub current_us: u64,
    /// Step 6: the identity filters of the segments this commit sealed.
    /// Zero when `GNOMON_VIDFILTER=0`, and on a commit that sealed
    /// nothing.
    ///
    /// It runs *after* the publication point on purpose: a filter is a
    /// rebuildable sidecar that no manifest names, so a crash between
    /// `CURRENT` and here costs a rebuild and never a generation. It is
    /// timed as its own phase because the ≤ 5 % write-path band is scored
    /// against exactly this number — folded into the residual it would be
    /// unattributable, a mistake that once took a diagnosis to
    /// undo.
    pub vidfilter_us: u64,
    pub total_us: u64,
    /// Bytes of the manifest this commit wrote, and how many segments it
    /// names: a manifest is rewritten in full every generation, so both grow
    /// with store history rather than with the batch.
    pub manifest_bytes: u64,
    pub segments_named: u64,
    /// Whether step 4 wrote a full checkpoint rather than a delta. A
    /// checkpoint every `K`th generation is the amortized cost the delta
    /// design trades for; separating the two keeps that a measurement rather
    /// than an assumption.
    pub manifest_checkpoint: bool,
}

/// Fold one successful commit's [`CommitPhases`] into the process-wide ingest
/// totals (`ingest_phase.rs`). The phases are reused as they are — the commit
/// already times them — rather than timed a second time.
fn record_commit_phases(p: &CommitPhases) {
    use ingest_phase::{add, G};
    for (g, us) in [
        (G::CommitTotalNs, p.total_us),
        (G::CommitCaptureNs, p.capture_us),
        (G::CommitSealNs, p.seal_us),
        (G::CommitClosesNs, p.closes_us),
        (G::CommitStatsNs, p.stats_us),
        (G::CommitDictNs, p.dict_us),
        (G::CommitDigestNs, p.digest_us + p.debug_verify_us),
        (G::CommitDeltaBuildNs, p.delta_build_us),
        (G::CommitManifestNs, p.manifest_us),
        (G::CommitCurrentNs, p.current_us),
        (G::CommitVidfilterNs, p.vidfilter_us),
    ] {
        add(g, us.saturating_mul(1000));
    }
    add(G::Commits, 1);
    add(G::CommitCheckpoints, u64::from(p.manifest_checkpoint));
    add(G::CommitManifestBytes, p.manifest_bytes);
}

/// Wall-clock microseconds `open` spent, by phase — the open-path sibling of
/// [`CommitPhases`], written for the same reason: an earlier evaluation
/// comparison
/// could not score "manifest-chain open ≤ 70 ms" because nothing broke the
/// open call down, and a single opaque number cannot tell a checkpoint-read
/// cost from a delta-replay one.
///
/// Instrumentation only, exactly like `CommitPhases`: no behaviour change,
/// nothing different on disk, and the only overhead beyond a normal open is
/// the handful of `Instant::now()` calls this and `manifest_chain` add.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpenPhases {
    /// Reading and JSON-deserializing the checkpoint the manifest chain
    /// resolved to.
    pub checkpoint_read_parse_us: u64,
    /// Verifying that checkpoint — building the Merkle tree at format 3 and
    /// up, or the O(n) whole-document digest recompute below it (see
    /// `chain_format`).
    pub merkle_verify_us: u64,
    /// The format of the checkpoint the manifest chain resolved to, i.e.
    /// which of the two `merkle_verify_us` shapes ran.
    pub chain_format: u32,
    /// Applying each delta above the checkpoint into the running
    /// manifest/Merkle state.
    pub state_build_us: u64,
    /// Reading and JSON-deserializing each delta above the checkpoint.
    pub delta_replay_us: u64,
    /// How many deltas were replayed to reach this generation.
    pub delta_count: u64,
    /// `Dictionary::open`: reading and validating the dictionary tail.
    pub dictionary_open_us: u64,
    /// Everything else `open` does: directory creation, the genesis publish
    /// on a fresh store, the gc pin, the current-only marker check, and
    /// assembling the `NativeStore` itself. Total minus the six phases
    /// above, so it absorbs whatever this list does not yet name rather than
    /// silently dropping it.
    pub other_us: u64,
    pub total_us: u64,
}

/// Everything a commit has to be able to put back, and everything the delta
/// it writes needs to know about the generation it came from.
///
/// The commit path advances `self.manifest` in place rather than building a
/// clone of it, so there is no parent object left to diff against or to fall
/// back to. This is that parent, in O(1) space: the scalars are a handful of
/// small values, the four lanes only ever grow within a commit so a length is
/// enough to truncate back to, and a Merkle state is O(log n) hashes.
struct CommitBase {
    generation: u64,
    parent: Option<u64>,
    created_tt: i64,
    event_log: EventLogRef,
    dict: DictRef,
    widths: Widths,
    next_segment_id: u64,
    stats: Stats,
    manifest_sha: String,
    /// Lane lengths before the commit, in lane order.
    split: [usize; 4],
    merkle: Option<merkle::ManifestMerkle>,
}

impl CommitBase {
    fn capture(m: &Manifest, merkle: &Option<merkle::ManifestMerkle>) -> Self {
        Self {
            generation: m.generation,
            parent: m.parent,
            created_tt: m.created_tt,
            event_log: m.event_log.clone(),
            dict: m.dict.clone(),
            widths: m.widths.clone(),
            next_segment_id: m.next_segment_id,
            stats: m.stats.clone(),
            manifest_sha: m.manifest_sha.clone(),
            split: [
                m.node_store.len(),
                m.edge_lanes.event.len(),
                m.edge_lanes.interval.len(),
                m.close_runs.len(),
            ],
            merkle: merkle.clone(),
        }
    }

    /// What the commit appended over, for a delta cut in O(appended).
    fn span(&self) -> AppendSpan {
        AppendSpan {
            parent: self.generation,
            parent_sha: self.manifest_sha.clone(),
            widths: self.widths.clone(),
            split: self.split,
        }
    }

    /// Put the handle's view back where it was. Only the lanes need
    /// truncating: a commit appends to them and never edits one in place, so
    /// the prefix below each split is untouched.
    fn restore(&self, m: &mut Manifest, merkle: &mut Option<merkle::ManifestMerkle>) {
        m.generation = self.generation;
        m.parent = self.parent;
        m.created_tt = self.created_tt;
        m.event_log = self.event_log.clone();
        m.dict = self.dict.clone();
        m.widths = self.widths.clone();
        m.next_segment_id = self.next_segment_id;
        m.stats = self.stats.clone();
        m.manifest_sha = self.manifest_sha.clone();
        m.node_store.truncate(self.split[0]);
        m.edge_lanes.event.truncate(self.split[1]);
        m.edge_lanes.interval.truncate(self.split[2]);
        m.close_runs.truncate(self.split[3]);
        merkle.clone_from(&self.merkle);
        debug_assert_eq!(
            m.manifest_sha,
            m.body_sha_canonical(),
            "rollback did not restore the generation the handle came in with"
        );
    }
}

impl NativeStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with(root.into(), false)
    }

    /// Open an existing store as a **reader that never writes** under its
    /// directory.
    ///
    /// `open` is a writer's open even when its caller only reads: it creates
    /// the four subdirectories, publishes a genesis generation into a
    /// directory with no `CURRENT`, and every lazily built sidecar persists
    /// itself on the way out of the read that built it. Read-only readers of
    /// the September stores gained identity-filter files under
    /// `idx/vidfilter/` that way. This handle instead:
    ///
    /// * refuses a directory with no `CURRENT` (there is nothing to read, and
    ///   creating a store is a write) and creates no directory;
    /// * refuses every writer entry point — `begin` (so every commit),
    ///   compaction, settle, gc, the manifest upgrade — *before* it writes,
    ///   with an error that says the handle is read-only;
    /// * builds the identity filter, the adjacency index and the name index
    ///   in memory only. A filter built that way is owned heap rather than a
    ///   file mapping and is charged as `vid_filter_heap`; the adjacency and
    ///   name indexes are heap in either mode, so for them the only cost is
    ///   that each new reader process rebuilds what a writer would have
    ///   loaded. Sidecars already on disk are loaded exactly as before.
    ///
    /// Answers are identical to a read-write handle's: the sidecars are
    /// derived, never authoritative.
    pub fn open_read_only(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with(root.into(), true)
    }

    fn open_with(root: PathBuf, read_only: bool) -> Result<Self> {
        let open_start = std::time::Instant::now();
        if read_only {
            if !root.join(CURRENT).is_file() {
                // a populated directory that lost `CURRENT` is corruption,
                // and says so exactly as a read-write open would
                Self::refuse_if_populated_without_current(&root)?;
                return Err(EngineError::not_found(format!(
                    "there is no published store to open read-only at {}",
                    root.display()
                ))
                .at_file(root.join(CURRENT))
                .with_remedy(
                    "a read-only handle never creates or repairs a store; open it \
                     read-write once, from the single writer process",
                ));
            }
        } else {
            for sub in SUBDIRS {
                fs::create_dir_all(root.join(sub))
                    .map_err(|e| EngineError::from(e).at_file(root.join(sub)))?;
            }
        }
        let (manifest, checkpoint_gen, merkle, chain_phases) = if root.join(CURRENT).exists() {
            Self::load_current(&root)?
        } else {
            Self::refuse_if_populated_without_current(&root)?;
            let genesis = Manifest::genesis();
            // generation 0 is always a checkpoint: a chain must start
            // somewhere, and there is no parent to diff against
            Self::publish(&root, None, &genesis, true, u64::MAX)?;
            let state = merkle::ManifestMerkle::from_manifest(&genesis);
            let phases = manifest_chain::ChainOpenPhases {
                chain_format: genesis.format,
                ..Default::default()
            };
            (genesis, 0, Some(state), phases)
        };
        let t = std::time::Instant::now();
        let dict = Dictionary::open(
            root.join(DICT),
            manifest.dict.records,
            manifest.dict.bytes,
        )?;
        let dictionary_open_us = t.elapsed().as_micros() as u64;
        let pin_key = fs::canonicalize(&root).unwrap_or_else(|_| root.clone());
        crate::gc::pin(&pin_key, manifest.generation);
        let current_only = root.join(CURRENT_ONLY_MARKER).exists();
        let total_us = open_start.elapsed().as_micros() as u64;
        let named_us = chain_phases.checkpoint_read_parse_us
            + chain_phases.merkle_verify_us
            + chain_phases.state_build_us
            + chain_phases.delta_replay_us
            + dictionary_open_us;
        let open_phases = OpenPhases {
            checkpoint_read_parse_us: chain_phases.checkpoint_read_parse_us,
            merkle_verify_us: chain_phases.merkle_verify_us,
            chain_format: chain_phases.chain_format,
            state_build_us: chain_phases.state_build_us,
            delta_replay_us: chain_phases.delta_replay_us,
            delta_count: chain_phases.delta_count,
            dictionary_open_us,
            other_us: total_us.saturating_sub(named_us),
            total_us,
        };
        let store = Self {
            root,
            pin_key,
            dict,
            manifest,
            merkle,
            checkpoint_gen,
            checkpoint_every: None,
            staging: Staging::default(),
            staged_closes: HashMap::new(),
            pending_closes: Vec::new(),
            partitions: PartitionMap::default(),
            segment_target_bytes: crate::defaults::SEGMENT_TARGET_BYTES,
            stats: std::sync::Mutex::new(None),
            stats_summary: std::sync::Mutex::new(None),
            node_belief: std::sync::Mutex::new(None),
            edge_postings: std::sync::Mutex::new(Postings::new(postings_budget(
                std::env::var("GNOMON_POSTINGS_BYTES").ok().as_deref(),
                detected_ram_bytes(),
            ))),
            node_postings: std::sync::Mutex::new(Postings::new(postings_budget(
                std::env::var("GNOMON_POSTINGS_BYTES").ok().as_deref(),
                detected_ram_bytes(),
            ))),
            verified: std::sync::Mutex::new(std::collections::HashSet::new()),
            close_cache: std::sync::Mutex::new(None),
            files_cache: std::sync::Mutex::new(None),
            pending_overlay: None,
            segments: std::sync::Mutex::new(SegmentCache::new(cache_budget(
                std::env::var("GNOMON_SEGMENT_CACHE_BYTES").ok().as_deref(),
                detected_ram_bytes(),
            ))),
            pinned_sources: std::sync::Mutex::new(HashMap::new()),
            pin_fallback_logged: std::sync::atomic::AtomicBool::new(false),
            mem_peaks: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            tcsr_cache: std::sync::Mutex::new(HashMap::new()),
            tcsr_built: std::sync::atomic::AtomicU64::new(0),
            tcsr_loaded: std::sync::atomic::AtomicU64::new(0),
            vid_filters: std::sync::Mutex::new(Filters::new(crate::vidfilter::filter_budget(
                std::env::var("GNOMON_VIDFILTER_BYTES").ok().as_deref(),
                detected_ram_bytes(),
            ))),
            filter_counters: Default::default(),
            filter_built: std::sync::atomic::AtomicU64::new(0),
            filter_loaded: std::sync::atomic::AtomicU64::new(0),
            filter_lazy_built: std::sync::atomic::AtomicU64::new(0),
            batch_tt: None,
            current_only,
            read_only,
            designs: crate::designs::Designs::from_env(),
            tcsr_python: std::env::var("GNOMON_TCSR")
                .is_ok_and(|v| v.trim().eq_ignore_ascii_case("python")),
            last_commit: None,
            install_ns: (0, 0, 0),
            open_phases,
            open_counters: OpenPathCounters::default(),
        };
        // Pin every file this generation's manifest names before
        // handing back a handle a reader might carry across a writer's
        // `gc(keep_last=…)`. Best-effort — a file missing here is either the
        // open-vs-gc race the design memo accepts as (d)'s residual, or the
        // `GNOMON_PIN_MAX_SEGMENTS` ceiling, and either way `open_segment`'s
        // lazy fallback (and the reopen-on-ENOENT net above the engine) is
        // what actually surfaces a real problem.
        store.pin_generation_files();
        Ok(store)
    }

    /// Where the last commit spent its time, if this handle has committed.
    pub fn last_commit_phases(&self) -> Option<CommitPhases> {
        self.last_commit
    }

    /// Where this handle's `open` call spent its time, by phase. Unlike
    /// [`Self::last_commit_phases`] this is never `None`: every live handle
    /// came from a successful `open`.
    pub fn open_phases(&self) -> OpenPhases {
        self.open_phases
    }

    /// Whether this store is the stripped current-only configuration:
    /// historical versions discarded, past-belief refused.
    pub fn current_only(&self) -> bool {
        self.current_only
    }

    /// Whether this handle came from [`Self::open_read_only`].
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// The engine designs this handle runs with: read from the
    /// environment when the handle was opened, so a receipt can record the
    /// configuration that actually answered.
    pub fn designs(&self) -> crate::designs::Designs {
        self.designs
    }

    /// Replace this handle's designs, for a test or a harness that wants
    /// two configurations in one process without touching the environment.
    /// Every design is a cost choice only, so this never changes an answer.
    pub fn set_designs(&mut self, designs: crate::designs::Designs) {
        self.designs = designs;
    }

    /// Refuse a write on a read-only handle. Called at the top of every
    /// writer entry point, so a refusal happens before any file is written —
    /// a compaction that wrote its new segments and only then failed at
    /// publication would leave orphans behind.
    pub(crate) fn require_writable_handle(&self) -> Result<()> {
        if !self.read_only {
            return Ok(());
        }
        Err(EngineError::invariant(format!(
            "this handle on {} is open read-only: it never writes under the store directory",
            self.root.display()
        ))
        .with_remedy(
            "reopen with NativeStore::open (Python: read_only=False), and only from \
             the single writer process",
        ))
    }

    /// Refuse a past-belief question on a current-only store. Answering it
    /// would silently use only the surviving rows and be wrong; an error is
    /// the honest response. "Current" is anything that clamps to
    /// `OPEN_END - 1`: the Python adapter passes `clamp_tt(as_of_tt)` down,
    /// so both the raw sentinel and its clamped form must count as now.
    pub fn assert_full_belief(&self, as_of_tt: i64) -> Result<()> {
        if self.current_only && crate::clamp_tt(as_of_tt) < crate::OPEN_END - 1 {
            return Err(EngineError::invariant(format!(
                "this store is current-only: \
                 historical versions were discarded, so belief at tt={as_of_tt} \
                 cannot be answered"
            ))
            .with_remedy("rebuild the store from its event log for bi-temporal queries"));
        }
        Ok(())
    }

    /// Stamp the store as current-only. Called by `compact_current_only`
    /// after the stripped generation is published; the marker outlives this
    /// handle so every later open refuses what the store can no longer
    /// answer.
    pub(crate) fn mark_current_only(&mut self) -> Result<()> {
        write_atomic(&self.root.join(CURRENT_ONLY_MARKER), "stripped by compact_current_only\n")?;
        self.current_only = true;
        Ok(())
    }

    /// Resolve `CURRENT` into `(manifest, checkpoint generation)`.
    ///
    /// The `CURRENT` half is unchanged from format 1: one line of
    /// `"<generation> <sha>"`, and the sha must equal the manifest's. What
    /// changed is that the manifest may now be assembled from a checkpoint
    /// plus up to `K−1` deltas rather than read whole — which the sha check
    /// cannot tell apart, because `manifest_sha` is the digest of the logical
    /// document either way.
    #[allow(clippy::type_complexity)]
    fn load_current(
        root: &Path,
    ) -> Result<(
        Manifest,
        u64,
        Option<merkle::ManifestMerkle>,
        manifest_chain::ChainOpenPhases,
    )> {
        let cur_path = root.join(CURRENT);
        let text = fs::read_to_string(&cur_path)
            .map_err(|e| EngineError::from(e).at_file(&cur_path))?;
        let mut parts = text.split_whitespace();
        let (gen, sha) = match (parts.next(), parts.next()) {
            (Some(g), Some(s)) => (g, s),
            _ => {
                return Err(EngineError::corrupt(format!(
                    "CURRENT must contain '<generation> <sha>', found {text:?}"
                ))
                .at_file(&cur_path))
            }
        };
        let generation: u64 = gen.parse().map_err(|_| {
            EngineError::corrupt(format!("CURRENT generation is not a number: {gen:?}"))
                .at_file(&cur_path)
        })?;

        let m_path = Self::manifest_path(root, generation);
        if !m_path.exists() {
            return Err(EngineError::corrupt(format!(
                "CURRENT points at generation {generation} but its manifest is missing"
            ))
            .at_file(&m_path));
        }
        let resolved = manifest_chain::reconstruct(root, generation)?;
        let manifest = resolved.manifest;
        if manifest.manifest_sha != sha {
            return Err(EngineError::corrupt(format!(
                "CURRENT records sha {sha} but manifest {generation} has {}",
                manifest.manifest_sha
            ))
            .at_file(&m_path));
        }
        Ok((manifest, resolved.checkpoint, resolved.merkle, resolved.phases))
    }

    fn manifest_path(root: &Path, generation: u64) -> PathBuf {
        manifest_chain::manifest_path(root, generation)
    }

    /// Refuse to treat a populated directory as a fresh store just because
    /// `CURRENT` happens to be missing (corruption-sweep finding,
    /// 2026-09-15: deleting `native/CURRENT` used to make `open` silently
    /// materialize an empty store, so every query returned empty results
    /// instead of failing).
    ///
    /// `CURRENT` legitimately does not exist yet in exactly one case: the
    /// bootstrap crash inside a *previous* call to `open`, between the
    /// genesis manifest's write (step 4 of the commit protocol, applied to
    /// generation 0) and the `CURRENT` flip that was supposed to follow it
    /// (step 5). There is no prior generation for that crash to have left
    /// intact — the commit protocol's guarantee only starts to apply once a
    /// first generation exists — so an orphaned, unpublished generation-0
    /// checkpoint with nothing else on disk is legitimately "nothing
    /// published yet" rather than corruption. `open` heals it by falling
    /// through to the normal genesis-publish path, which rewrites the
    /// (deterministic) genesis document and this time completes the flip.
    ///
    /// Anything else without `CURRENT` — any other manifest file, any
    /// segment, or a non-empty dictionary log — means a store was populated
    /// at some point and `CURRENT` is now gone: refuse instead of guessing.
    fn refuse_if_populated_without_current(root: &Path) -> Result<()> {
        let manifests_dir = root.join("manifests");
        let mut manifest_gens: Vec<u64> = Vec::new();
        if manifests_dir.is_dir() {
            for entry in fs::read_dir(&manifests_dir)
                .map_err(|e| EngineError::from(e).at_file(&manifests_dir))?
            {
                let entry = entry.map_err(|e| EngineError::from(e).at_file(&manifests_dir))?;
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue; // ignore `.tmp` leftovers from an interrupted write_atomic
                }
                if let Some(gen) = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    manifest_gens.push(gen);
                }
            }
        }
        manifest_gens.sort_unstable();

        let seg_dir = root.join("seg");
        let n_segments = if seg_dir.is_dir() {
            fs::read_dir(&seg_dir)
                .map_err(|e| EngineError::from(e).at_file(&seg_dir))?
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("tgs"))
                .count()
        } else {
            0
        };

        let dict_bytes = fs::metadata(root.join(DICT)).map(|m| m.len()).unwrap_or(0);

        if manifest_gens.is_empty() && n_segments == 0 && dict_bytes == 0 {
            return Ok(()); // a genuinely new, empty directory
        }

        // the bootstrap-crash exception: exactly the orphaned generation-0
        // checkpoint, nothing else, and its own event log starts at cursor 0
        if manifest_gens == [0] && n_segments == 0 && dict_bytes == 0 {
            if let Ok(manifest_chain::ManifestRecord::Checkpoint(m)) =
                manifest_chain::read_record(root, 0)
            {
                if m.generation == 0 && m.parent.is_none() && m.event_log.offset == 0 {
                    return Ok(());
                }
            }
        }

        Err(EngineError::corrupt(format!(
            "store has {} manifest(s) / {} segment(s) but no CURRENT — \
             refusing to treat a populated directory as empty; if this is a \
             crashed first commit, remove the orphans or restore CURRENT",
            manifest_gens.len(),
            n_segments
        ))
        .with_remedy(
            "remove the orphaned manifests/segments if this was a crashed \
             first commit, or restore CURRENT from a backup",
        ))
    }

    /// Steps 4 and 5: write this generation's manifest record, then flip
    /// `CURRENT`.
    ///
    /// `appended` is the span the commit path appended over, plus the
    /// checkpoint the chain currently rests on; `None` means "nothing to cut
    /// a delta from", which forces a checkpoint — which is what every caller
    /// but `commit` wants anyway, since each of them replaces the whole
    /// segment list or has no parent at all. `force_checkpoint` is the memo's
    /// rules 2 and 3 — compaction and gc's retention floor.
    ///
    /// **The manifest is not re-verified here.** It used to be, on its first
    /// line: a third deep clone plus a second serialize-and-sha256 of bytes
    /// the caller's own `seal` had produced microseconds earlier on the same
    /// thread, with nothing in between ("pure
    /// redundancy"). The format check it also made is
    /// kept, because that one is not redundant. A `debug_assert` keeps the
    /// digest check in test builds, where it is a real net over the
    /// incremental seal.
    ///
    /// Returns `(manifest_us, current_us, manifest_bytes, checkpoint_gen,
    /// delta_build_us, debug_verify_us)`. The timing split matters because
    /// step 4 used to rewrite the whole manifest every generation while step
    /// 5 writes forty bytes; the point of this change is that step 4 no
    /// longer grows with store history either, so the split is what shows
    /// it. `delta_build_us` is split out separately because it used
    /// to run entirely before `manifest_us`'s timer started — the
    /// delta-or-checkpoint JSON this builds is what a commit-cost diagnosis
    /// flagged as needing its own timer, isolated from
    /// `digest_us`. `debug_verify_us` times the `debug_assert_eq!` below,
    /// which is itself O(segments) and otherwise ran untimed ahead of
    /// `manifest_us`'s clock start — always 0 in release, where the assert
    /// compiles away.
    fn publish(
        root: &Path,
        appended: Option<(&AppendSpan, u64)>,
        manifest: &Manifest,
        force_checkpoint: bool,
        every: u64,
    ) -> Result<(u64, u64, u64, u64, u64, u64)> {
        #[cfg(debug_assertions)]
        let debug_verify_us = {
            let t = std::time::Instant::now();
            debug_assert_eq!(
                manifest.manifest_sha,
                manifest.body_sha_canonical(),
                "publish was handed a manifest its caller had not sealed"
            );
            t.elapsed().as_micros() as u64
        };
        #[cfg(not(debug_assertions))]
        let debug_verify_us = 0u64;
        if !manifest_chain::is_writable_format(manifest.format) {
            return Err(manifest_chain::read_only_format_error(manifest.format));
        }
        let generation = manifest.generation;
        let t = std::time::Instant::now();
        let delta = if force_checkpoint || generation.is_multiple_of(every) {
            None
        } else {
            appended.and_then(|(span, ckpt)| ManifestDelta::from_appends(manifest, span, ckpt))
        };
        let (json, checkpoint_gen) = match &delta {
            Some(d) => (d.to_json(), appended.expect("a delta needs a parent").1),
            None => (manifest_chain::checkpoint_json(manifest), generation),
        };
        let delta_build_us = t.elapsed().as_micros() as u64;

        let m_path = Self::manifest_path(root, generation);
        let t = std::time::Instant::now();
        write_atomic(&m_path, &json)?;
        let manifest_us = t.elapsed().as_micros() as u64;
        crash_point("after_manifest");
        let t = std::time::Instant::now();
        write_atomic(
            &root.join(CURRENT),
            &format!("{} {}\n", generation, manifest.manifest_sha),
        )?;
        crash_point("after_current");
        Ok((
            manifest_us,
            t.elapsed().as_micros() as u64,
            json.len() as u64,
            checkpoint_gen,
            delta_build_us,
            debug_verify_us,
        ))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn pin_key(&self) -> &Path {
        &self.pin_key
    }

    /// Adopt a freshly published manifest as this handle's view, moving its
    /// reader pin with it. Every path that advances `self.manifest` after
    /// open must come through here or gc could collect the old view early —
    /// or keep protecting a generation nobody holds.
    /// Adopt a manifest that was *built* rather than appended to — compaction
    /// and the format upgrade. Both replace the whole segment list, so the
    /// Merkle state is rebuilt wholesale; both already force a checkpoint, so
    /// the O(n) is one they were paying anyway.
    fn adopt(&mut self, next: Manifest) {
        self.repin(self.manifest.generation, next.generation);
        self.merkle = manifest_chain::is_writable_format(next.format)
            .then(|| merkle::ManifestMerkle::from_manifest(&next));
        self.manifest = next;
        // Re-pin to the adopted generation's file set — compaction
        // and the format upgrade both replace the whole segment list, so
        // this is where a pinned source for a file the new generation no
        // longer names gets dropped (mirrors `evict_segments_not_in` for
        // the decoded cache).
        self.pin_generation_files();
    }

    /// Move this handle's reader pin. Taken explicitly rather than read off
    /// `self.manifest`, because the commit path advances the manifest it
    /// already holds — by the time it repins, `self.manifest.generation` is
    /// the destination, and a `repin(new, new)` would silently strand the old
    /// pin and stop gc from ever collecting that generation.
    fn repin(&self, from: u64, to: u64) {
        crate::gc::repin(&self.pin_key, from, to);
    }

    /// Map every segment file `self.manifest` currently names into
    /// `pinned_sources`, dropping entries for files it no longer names, and
    /// warm `close_cache` for this generation.
    ///
    /// Best-effort by design, not by omission: a segment file missing here
    /// is either the open-vs-gc race the design memo's candidate (a) leaves
    /// as (d)'s job, or corruption `verify`/the next real read will catch —
    /// either way, failing the caller (open, a commit that already
    /// published, a compaction that already installed) would be the wrong
    /// failure mode. Above `GNOMON_PIN_MAX_SEGMENTS` segments, pinning is
    /// disabled for this handle entirely (logged once) and every segment
    /// falls back to the pre-fix per-touch `MmapSource::load` in
    /// `open_segment`/`open_segment_uncached`.
    fn pin_generation_files(&self) {
        let files = self.segment_files();
        let total = files.edge.len() + files.node.len();
        let cap = pin_max_segments();
        {
            let mut pinned = self
                .pinned_sources
                .lock()
                .expect("pinned-sources mutex poisoned");
            if total > cap {
                pinned.clear();
                self.log_pin_fallback(total, cap);
            } else {
                let names: std::collections::HashSet<&str> = files
                    .edge
                    .values()
                    .chain(files.node.values())
                    .map(String::as_str)
                    .collect();
                pinned.retain(|name, _| names.contains(name.as_str()));
                for name in names {
                    if pinned.contains_key(name) {
                        continue;
                    }
                    match MmapSource::load(&self.root.join(name)) {
                        Ok(src) => {
                            pinned.insert(name.to_string(), src);
                        }
                        Err(e) => {
                            // Not fatal here — see the doc comment above.
                            // `open_segment`'s lazy fallback (and the
                            // reopen-on-ENOENT net above the engine) is
                            // where a genuine problem is meant to surface.
                            eprintln!(
                                "gnomon: pin-at-open could not map {name} at generation {}: {e}",
                                self.manifest.generation
                            );
                        }
                    }
                }
            }
        }
        // Eagerly load close runs (the memo's "second lazy surface"): this
        // reads every close-run file `read_close_run` would otherwise defer
        // to the first query, while they are certainly still on disk, and
        // caches the folded result in `close_cache` keyed by this exact
        // generation — so a later `committed_close_index()` call at the same
        // generation returns from cache rather than re-reading a file gc may
        // since have unlinked. Errors are swallowed for the same reason as
        // above.
        let _ = self.committed_close_index();
    }

    /// Log the `GNOMON_PIN_MAX_SEGMENTS` fallback once per handle — a store
    /// hovering around the cap across many commits should not spam a line
    /// per commit.
    fn log_pin_fallback(&self, total: usize, cap: usize) {
        if !self
            .pin_fallback_logged
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            eprintln!(
                "gnomon: {} has {total} segments, over GNOMON_PIN_MAX_SEGMENTS={cap} — \
                 pin-at-open is disabled for this handle; falling back to \
                 lazy per-segment loading",
                self.root.display()
            );
        }
    }

    /// The bytes backing `file`: the pinned map from the last
    /// `pin_generation_files` call, or a fresh `MmapSource::load` when this
    /// file was not pinned (the cap fallback, or the open-vs-gc race).
    fn segment_source(&self, file: &str, path: &Path) -> Result<MmapSource> {
        if let Some(src) = self
            .pinned_sources
            .lock()
            .expect("pinned-sources mutex poisoned")
            .get(file)
        {
            OpenPathCounters::bump(&self.open_counters.pinned_source_hits, 1);
            return Ok(src.clone());
        }
        OpenPathCounters::bump(&self.open_counters.fresh_map_loads, 1);
        MmapSource::load(path)
    }

    /// `(cached_hits, cached_misses, uncached_opens, decoded_bytes,
    /// touched_bytes, selected_rows, first_touch_verifies,
    /// pinned_source_hits, fresh_map_loads)` for this handle's segment opens
    /// — see [`OpenPathCounters`]. Instrumentation, never a correctness
    /// input.
    pub fn open_path_stats(&self) -> OpenPathStats {
        self.open_counters.snapshot()
    }

    /// Per-phase read-path timers as `(name, value)` — see [`crate::phase`].
    ///
    /// Process-global on purpose, not per handle. The counters beside it are
    /// per handle: a harness that opens several handles and reads the idle one
    /// would see misleading counters; a global has no wrong handle to read. Bracket one
    /// query between [`Self::reset_phase_stats`] and this call and the result
    /// is that query's phase split.
    pub fn phase_stats(&self) -> Vec<(&'static str, u64)> {
        crate::phase::snapshot_named()
    }

    /// Zero the per-phase timers. Instrumentation only; changes no state a
    /// read or a write can observe.
    pub fn reset_phase_stats(&self) {
        crate::phase::reset();
    }

    /// The measured seal quotient per lane slot, read off the committed
    /// manifest (`crate::sealsize`).
    ///
    /// **Which segments count.** Only those whose row count reaches
    /// `merge::full_rows_at(base_target, slot)` — half the accounted
    /// capacity, the same discrimination `settle::LaneCensus::rows_per_seal`
    /// draws and for the same reason: a segment below it is one small commit,
    /// and its bytes-per-row is dominated by the per-file header and an
    /// unshared string heap rather than by what the encoder achieves at seal
    /// size. Averaging those in would bias the quotient towards 1 and undo
    /// the correction. A store with no such segment returns `unmeasured`, so
    /// it behaves exactly as it did previously — which is every unit-scale
    /// fixture, and every store up to its first byte-rule seal.
    ///
    /// **The threshold uses the configured target, not the effective one**,
    /// which would be circular. It is a selection rule, not a sizing rule:
    /// segments sealed before and after the correction both clear it (≈2.3M
    /// and ≈4.4M rows against ≈1.34M on the synthetic corpus), so the
    /// quotient converges within one sealed segment either way.
    ///
    /// **Determinism.** The inputs are manifest row counts and file
    /// lengths, both byte-identical across replays of the same log, and the
    /// accumulation is integer and order-independent. `GNOMON_SEAL_ADAPTIVE=0`
    /// returns `unmeasured` unconditionally, which is the A/B's control configuration.
    ///
    /// One `stat` per qualifying segment, and qualifying segments are few —
    /// 14 at 30M, 44 at 100M. Never called from `needs_settle` or any other
    /// advisory path.
    pub(crate) fn seal_ratios(&self) -> crate::sealsize::SealRatios {
        let mut ratios = crate::sealsize::SealRatios::unmeasured();
        if !crate::sealsize::adaptive_enabled() {
            return ratios;
        }
        let (_, base) = self.layout();
        let m = self.manifest();
        for (slot, entries) in [
            (crate::merge::LaneSlot::EdgeEvent, &m.edge_lanes.event),
            (crate::merge::LaneSlot::EdgeInterval, &m.edge_lanes.interval),
            (crate::merge::LaneSlot::Node, &m.node_store),
        ] {
            let at_target = crate::merge::full_rows_at(base, slot);
            let per_row = crate::sealsize::per_row_model(slot);
            for e in entries {
                let rows = e.rows as u64;
                if rows < at_target {
                    continue;
                }
                let bytes = std::fs::metadata(self.root.join(&e.file))
                    .map(|md| md.len())
                    .unwrap_or(0);
                ratios.get_mut(slot).observe(bytes, rows * per_row);
            }
        }
        ratios
    }

    /// Seeded ratios for a `Staging::seal` of `staged_rows` rows, or
    /// `unmeasured` when no chunk boundary can arise.
    ///
    /// A batch whose whole accounted size cannot reach even the narrowest
    /// effective target — `base / SEAL_RATIO_MIN_DEN` — produces exactly one
    /// chunk per lane whatever the quotient says, so seeding it would be a
    /// manifest walk and a handful of `stat`s per commit for a decision that
    /// cannot be made. The bound uses the smaller of the two per-row models,
    /// so it never skips a seed a split could have used. At `--batch 50000`
    /// that is every commit and the build path pays nothing; `compact()`'s
    /// whole-store reseal is the caller for which it does not hold.
    pub(crate) fn seal_ratios_for_batch(&self, staged_rows: usize) -> crate::sealsize::SealRatios {
        let (_, base) = self.layout();
        let floor = crate::defaults::NODE_ROW_MODEL_BYTES
            .min(crate::defaults::EDGE_ROW_MODEL_BYTES);
        let reachable = (staged_rows as u64).saturating_mul(floor)
            >= base / crate::defaults::SEAL_RATIO_MIN_DEN.max(1);
        if reachable {
            self.seal_ratios()
        } else {
            crate::sealsize::SealRatios::unmeasured()
        }
    }

    /// Per-segment layout of the current generation, straight off the
    /// manifest: `(lane, file, rows, key_lo, key_hi, vt_min, vt_max)`, where
    /// the keys are the full composite `(vt_s, vid)` boundaries
    /// (`manifest.rs`'s `SegmentEntry`).
    ///
    /// Read-only observability. It exists so "how much of the key space does
    /// a segment span, and how much do segments overlap each other" is a
    /// measurement rather than a property inferred from how a store was
    /// built — the question a tiered layout and a settled one answer
    /// differently, and the one a range-prune's effectiveness rests on.
    #[allow(clippy::type_complexity)]
    pub fn segment_layout(&self) -> Vec<(&'static str, String, u32, (i64, String), (i64, String), i64, i64)> {
        let m = self.manifest();
        let mut out = Vec::new();
        for (lane, entries) in [
            ("edge_event", &m.edge_lanes.event),
            ("edge_interval", &m.edge_lanes.interval),
            ("node", &m.node_store),
        ] {
            for e in entries {
                out.push((
                    lane,
                    e.file.clone(),
                    e.rows,
                    e.key_lo.clone(),
                    e.key_hi.clone(),
                    e.vt_min,
                    e.vt_max,
                ));
            }
        }
        out
    }

    /// Per-segment `tt_s` run profile: how shattered each segment's
    /// transaction-time runs are, and what the folded bounds say.
    ///
    /// `(lane, file, rows, tt_s_runs, tt_min, tt_max)`, one row per live
    /// segment. Opens each segment header and reads nothing else.
    ///
    /// **Why this is worth an accessor.** `believed_ranges` and
    /// `added_ranges` are `O(runs)` when the folded `(tt_min, tt_max)` bounds
    /// cannot decide a segment, and `O(1)` when they can. Which of those a
    /// store gets is not a property of its size but of its *layout*: a tiered
    /// segment is one commit's rows and carries a single run, while `settle`
    /// and `compact` re-sort globally on `(vt_s, vid)` and interleave
    /// transaction times that were contiguous per commit, shattering the runs
    /// (one measurement found 2.1M runs over 3.0M rows). Until now that could only
    /// be inferred from timings. `runs / rows` is the ratio a block-bounds
    /// `tt_s` representation would be scored against, so it has to be
    /// observable rather than deduced.
    pub fn segment_tt_profile(&self) -> Result<Vec<SegmentTtProfile>> {
        let files: Vec<(&'static str, String)> = {
            let m = self.manifest();
            [
                ("edge_event", &m.edge_lanes.event),
                ("edge_interval", &m.edge_lanes.interval),
                ("node", &m.node_store),
            ]
            .into_iter()
            .flat_map(|(lane, entries)| entries.iter().map(move |e| (lane, e.file.clone())))
            .collect()
        };
        let mut out = Vec::with_capacity(files.len());
        for (lane, file) in files {
            let seg = self.open_segment(&file)?;
            let h = seg.header();
            let (tt_min, tt_max) = h.tt_s_bounds();
            out.push(SegmentTtProfile {
                lane,
                file,
                rows: h.rows,
                // `tt_s_view().run_count()`, never `h.tt_s_runs.len()`.
                // Dictionary-coded segments drop the run list, so
                // the header length reports **0** there -- not because the
                // runs are gone but because they are stored as codes. This
                // profile is a *layout* diagnostic, so it has to mean the
                // same number across both encodings: `run_count` is the
                // list's length on the run shape and is counted from the
                // codes on the dictionary shape, which is the number the run
                // list would have held.
                tt_s_runs: seg.tt_s_view()?.run_count() as u32,
                tt_min,
                tt_max,
            });
        }
        Ok(out)
    }

    /// Record that a read path's verdict ranges selected `rows` rows, for
    /// `open_path_stats`' `selected_rows`. Called by the paths that build
    /// selections; a counter, never read back.
    pub(crate) fn note_selected_rows(&self, rows: u64) {
        OpenPathCounters::bump(&self.open_counters.selected_rows, rows);
    }

    /// Drop cached segments gc just removed from disk. Sound because ids are
    /// never reused (`Manifest::next_segment_id`), so an evicted name can
    /// never come back meaning different bytes.
    ///
    /// Also bounds the identity postings: `keep` is translated to
    /// segment ids and handed to `Postings::retain_segments`, which is the
    /// only thing standing between periodic compaction and an unbounded
    /// `by_identity`/`by_vid` — see that method's own doc for why.
    pub(crate) fn evict_segments_not_in(&self, keep: &std::collections::HashSet<String>) {
        self.segments
            .lock()
            .expect("segment-cache mutex poisoned")
            .retain_files(keep);
        self.verified
            .lock()
            .expect("verified-set mutex poisoned")
            .retain(|file| keep.contains(file));
        let keep_ids: std::collections::HashSet<u64> =
            keep.iter().map(|f| segment_id_of(f)).collect();
        self.edge_postings
            .lock()
            .expect("edge-postings mutex poisoned")
            .retain_segments(&keep_ids);
        self.node_postings
            .lock()
            .expect("node-postings mutex poisoned")
            .retain_segments(&keep_ids);
        // A TCSR index of a segment that no longer exists can never be asked
        // for again, and holding it would make `tcsr_stats`' resident bytes
        // grow across compactions for no reader's benefit.
        self.tcsr_cache
            .lock()
            .expect("tcsr-cache mutex poisoned")
            .retain(|id, _| keep_ids.contains(id));
        // …and the identity filters, for the same reason and by the same
        // rule. This is the gc hook `nameidx::CACHE` does not have.
        self.vid_filters
            .lock()
            .expect("vid-filter mutex poisoned")
            .retain_segments(&keep_ids);
    }

    /// Override the segment-cache byte budget for this handle (`None` =
    /// unbounded). Exists so tests and harnesses can exercise eviction
    /// without mutating the process environment, which races other threads.
    pub fn set_segment_cache_budget(&self, budget: Option<u64>) {
        self.segments
            .lock()
            .expect("segment-cache mutex poisoned")
            .set_budget(budget);
    }

    /// `(entries, accounted_bytes, budget, evictions)` — observability for
    /// the byte-budget cache, so "the cache stayed under budget" is a
    /// measurement rather than an assumption.
    pub fn segment_cache_stats(&self) -> (usize, u64, Option<u64>, u64) {
        self.segments
            .lock()
            .expect("segment-cache mutex poisoned")
            .stats()
    }

    /// Every long-lived in-process structure this handle holds, in bytes,
    /// with a running maximum per component.
    ///
    /// Returns `(current, peak)` as parallel ordered lists of
    /// `(component, bytes)`. Observation only: nothing here allocates a
    /// store-sized structure, changes an eviction decision, or touches a
    /// segment that was not already open. The cost is O(cached segments +
    /// pinned segments + staged rows) per call, which is why the intended
    /// cadence is a progress line, not a row.
    ///
    /// The components, and why each is its own line rather than folded into
    /// a total:
    ///
    /// * `segment_cache_mapped` / `segment_cache_decoded` — the two halves of
    ///   what `segment_cache_stats` reports as one accounted number. Only the
    ///   second is anonymous heap an eviction reclaims; the first is
    ///   file-backed and, while `pinned_sources` is populated, is not
    ///   released by eviction at all. A budget spent mostly on the first
    ///   bounds the wrong quantity.
    /// * `segment_cache_header` — parsed `SegmentHeader` heap held per cached
    ///   segment and charged against **no** budget. `tt_s_runs` dominates it
    ///   on a compacted store; see `SegmentHeader::heap_bytes`.
    /// * `pinned_mapped` / `pinned_segments` — the pin-at-open table.
    ///   Mapped length, not resident pages: the kernel decides the latter, and
    ///   a caller that needs it reads `RssFile` from the OS. This is the
    ///   ceiling those pages can reach.
    /// * `postings` — the one component that already had a budget and a
    ///   receipt; repeated here so a single sample is self-contained.
    /// * `tcsr`, `name_index_cache` — two caches with no byte budget and no
    ///   LRU. `name_index_cache` is process-global, so it is not per handle.
    /// * `vid_filter_mapped` / `vid_filter_heap` / `vid_filter_segments` —
    ///   the identity filters. Mapped, so they count toward
    ///   `file_mapped_total`; heap only for a filter whose write failed.
    /// * `close_index`, `staging`, `manifest`, `dictionary`, `stats_accum`,
    ///   `verified_set` — the remaining per-handle structures, each with a
    ///   different scaling law (per close, per staged row, per segment, per
    ///   distinct entity, per distinct source vertex, per segment ever
    ///   opened). Listing them is what lets a measurement *rule them out*.
    ///
    /// Every figure is an estimate of allocated capacity, not a malloc-level
    /// truth: it counts `Vec`/`String`/`HashMap` capacities and the bytes they
    /// point at, and it cannot see allocator slack or arena retention. The
    /// difference between the sum and `RssAnon` is exactly that slack, and is
    /// a quantity worth reading rather than hiding.
    pub fn memory_accounting(&self) -> (MemoryComponents, MemoryComponents) {
        let (entries, accounted, budget, evictions, mapped, decoded, header, cached_rows) = {
            let c = self
                .segments
                .lock()
                .expect("segment-cache mutex poisoned");
            let (entries, accounted, budget, evictions) = c.stats();
            let (mapped, decoded, header, rows) = c.residency();
            (entries, accounted, budget, evictions, mapped, decoded, header, rows)
        };
        let (pinned_segments, pinned_mapped) = {
            let p = self
                .pinned_sources
                .lock()
                .expect("pinned-sources mutex poisoned");
            (
                p.len() as u64,
                p.iter()
                    .map(|(name, src)| {
                        name.capacity() as u64 + crate::segment::SegmentSource::bytes(src).len() as u64
                    })
                    .sum::<u64>(),
            )
        };
        let postings = self.postings_bytes();
        let tcsr = self
            .tcsr_cache
            .lock()
            .expect("tcsr-cache mutex poisoned")
            .values()
            .map(|t| t.resident_bytes())
            .sum::<u64>();
        let (name_index_entries, name_index_bytes) = crate::nameidx::cache_stats();
        let (vid_filter_mapped, vid_filter_heap, vid_filter_segments) = self
            .vid_filters
            .lock()
            .expect("vid-filter mutex poisoned")
            .class_split();
        let close_index = {
            let committed = self
                .close_cache
                .lock()
                .expect("close-cache mutex poisoned")
                .as_ref()
                .map_or(0, |e| e.3.heap_bytes());
            let overlay = self.pending_overlay.as_ref().map_or(0, |o| o.heap_bytes());
            committed + overlay
        };
        let staging = self.staging.heap_bytes()
            + (self.staged_closes.capacity()
                * (std::mem::size_of::<Id96>() + std::mem::size_of::<i64>())
                + self.pending_closes.capacity() * std::mem::size_of::<CloseRecord>())
                as u64;
        let manifest = self.manifest.heap_bytes()
            + self
                .merkle
                .as_ref()
                .map_or(0, |_| self.manifest.live_segment_count() as u64 * 64);
        let dictionary = self.dict.heap_bytes();
        let node_belief = self
            .node_belief
            .lock()
            .expect("node-belief mutex poisoned")
            .as_ref()
            .map_or(0, |nb| nb.heap_bytes());
        let stats_accum = self
            .stats
            .lock()
            .expect("stats mutex poisoned")
            .as_ref()
            .map_or(0, |a| a.heap_bytes());
        let verified = {
            let v = self.verified.lock().expect("verified mutex poisoned");
            v.capacity() as u64 * std::mem::size_of::<String>() as u64
                + v.iter().map(|f| f.capacity() as u64).sum::<u64>()
        };

        // The three memory classes, summed here so every receipt reports the
        // same arithmetic rather than each caller re-deriving it.
        //
        // `anonymous` is heap the engine allocated and the only class
        // `GNOMON_SEGMENT_CACHE_BYTES` and `GNOMON_POSTINGS_BYTES` bound.
        // `file_mapped` is the store's own segment bytes, mapped in — equal
        // to the store's segment bytes once every page has been touched,
        // which a whole-store fold guarantees. Which of `RssFile` and
        // `RssShmem` it lands in is a property of the filesystem, not of the
        // engine: tmpfs puts it in the second. The caller reads both from
        // `/proc/self/status` and compares their sum to `file_mapped`.
        let anonymous: u64 = [
            decoded, header, postings, tcsr, name_index_bytes, close_index,
            staging, manifest, dictionary, stats_accum, verified, vid_filter_heap,
            node_belief,
        ]
        .iter()
        .sum();

        let current: MemoryComponents = vec![
            ("segment_cache_accounted", accounted),
            ("segment_cache_budget", budget.unwrap_or(0)),
            ("segment_cache_entries", entries as u64),
            ("segment_cache_evictions", evictions),
            ("segment_cache_rows", cached_rows),
            ("segment_cache_mapped", mapped),
            ("segment_cache_decoded", decoded),
            ("segment_cache_header", header),
            ("pinned_segments", pinned_segments),
            ("pinned_mapped", pinned_mapped),
            ("postings", postings),
            ("tcsr", tcsr),
            ("name_index_cache", name_index_bytes),
            ("name_index_entries", name_index_entries as u64),
            ("close_index", close_index),
            ("staging", staging),
            ("manifest", manifest),
            ("dictionary", dictionary),
            ("stats_accum", stats_accum),
            ("verified_set", verified),
            // The identity filters, split by class. `vid_filter_mapped`
            // is file-backed and joins `file_mapped_total`; `vid_filter_heap`
            // is non-zero only for a filter whose write failed, and joins
            // `anonymous_total`. Charging the mapped bytes to the anonymous
            // side would make the anonymous-memory bound look violated by a structure the
            // kernel can reclaim.
            ("vid_filter_mapped", vid_filter_mapped),
            ("vid_filter_heap", vid_filter_heap),
            ("vid_filter_segments", vid_filter_segments),
            ("node_belief", node_belief),
            // the three memory classes
            ("anonymous_total", anonymous),
            ("file_mapped_total", pinned_mapped + vid_filter_mapped),
        ];

        let mut peaks = self.mem_peaks.lock().expect("mem-peaks mutex poisoned");
        for (k, v) in &current {
            let slot = peaks.entry(k).or_insert(0);
            *slot = (*slot).max(*v);
        }
        let peak = current
            .iter()
            .map(|(k, _)| (*k, peaks.get(k).copied().unwrap_or(0)))
            .collect();
        (current, peak)
    }

    /// `(identity_entries, vid_entries, indexed_segments)` for one row
    /// kind's identity postings — observability for the postings bound, the same
    /// role `segment_cache_stats` plays for the segment cache. Exists so a
    /// test (or an operator) can measure "postings stayed bounded across
    /// compaction cycles" directly, rather than inferring it from process
    /// RSS, which conflates the postings leak with compaction's own
    /// legitimate O(current rows) working set.
    ///
    /// The middle element is **always 0**: there is no longer a
    /// vid postings map at all — `locate_vid` range-prunes the manifest's
    /// `key_lo`/`key_hi` for the row's `(vt_s, vid)` and binary-searches the
    /// surviving segments, which is exact and holds nothing. The slot is kept
    /// so the regression receipts keep their shape; every one
    /// of them bounds it from above. [`Self::postings_index_stats`] is the
    /// full, named form.
    pub fn postings_stats(&self, kind: RowKind) -> (usize, usize, usize) {
        let s = self.postings_index_stats(kind);
        (s.0, 0, s.1)
    }

    /// `(entries, resident_segments, resident_bytes, budget, evictions,
    /// rebuilds, folds, rows_folded, transient_folds)` for one row kind's
    /// identity postings — the byte-budget receipt, mirroring
    /// [`Self::segment_cache_stats`].
    ///
    /// `folds`/`rows_folded` are the instrument that attributed the 30M
    /// build's finalisation RSS step: they count every segment fold and every
    /// row walked by one, so "the first correction indexed the whole store"
    /// is a reading rather than an inference. `transient_folds` counts the
    /// folds answered and dropped because one segment's postings alone
    /// exceeded the budget — the shape a 1-byte budget produces, where
    /// nothing is ever resident and every answer is still exact.
    #[allow(clippy::type_complexity)]
    pub fn postings_index_stats(
        &self,
        kind: RowKind,
    ) -> (usize, usize, u64, Option<u64>, u64, u64, u64, u64, u64) {
        let p = match kind {
            RowKind::Edge => &self.edge_postings,
            RowKind::Node => &self.node_postings,
        };
        p.lock().expect("postings mutex poisoned").stats()
    }

    /// Override the identity-postings byte budget for this handle (`None` =
    /// unbounded), for the same reason [`Self::set_segment_cache_budget`]
    /// exists: a test or harness must be able to exercise eviction without
    /// mutating the process environment, which races other threads.
    pub fn set_postings_budget(&self, budget: Option<u64>) {
        for p in [&self.edge_postings, &self.node_postings] {
            p.lock()
                .expect("postings mutex poisoned")
                .set_budget(budget);
        }
    }

    /// Peak resident bytes across both kinds' postings, for a build receipt.
    pub fn postings_bytes(&self) -> u64 {
        self.postings_index_stats(RowKind::Edge).2 + self.postings_index_stats(RowKind::Node).2
    }

    pub fn generation(&self) -> u64 {
        self.manifest.generation
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn dict(&self) -> &Dictionary {
        &self.dict
    }

    pub(crate) fn stats_cell(&self) -> &std::sync::Mutex<Option<StatsAccum>> {
        &self.stats
    }

    pub(crate) fn stats_summary_cell(
        &self,
    ) -> &std::sync::Mutex<Option<(u64, std::sync::Arc<StatsSummary>)>> {
        &self.stats_summary
    }

    pub(crate) fn node_belief_cell(
        &self,
    ) -> &std::sync::Mutex<Option<crate::node_belief::NodeBelief>> {
        &self.node_belief
    }

    /// What the believed-node count has done on this handle, or `None` if no
    /// current-belief existence probe has built it yet.
    pub fn node_belief_stats(&self) -> Option<crate::node_belief::NodeBeliefStats> {
        self.node_belief
            .lock()
            .expect("node-belief mutex poisoned")
            .as_ref()
            .map(|nb| nb.stats())
    }

    /// Rows this open batch has closed, as physical addresses.
    ///
    /// The open-version index is built per committed generation and does not
    /// know about an in-flight batch, so a mid-batch read subtracts these.
    /// Bounded by the batch, not the store.
    pub(crate) fn pending_closed_rows(&self, kind: RowKind) -> std::collections::HashSet<(u64, u32)> {
        self.pending_closes
            .iter()
            .filter(|c| c.kind == kind)
            .map(|c| (c.segment, c.row))
            .collect()
    }

    pub(crate) fn edge_postings(&self) -> &std::sync::Mutex<Postings> {
        &self.edge_postings
    }

    pub(crate) fn node_postings(&self) -> &std::sync::Mutex<Postings> {
        &self.node_postings
    }

    /// Layout policy in force, for callers that re-seal existing data.
    pub(crate) fn layout(&self) -> (&PartitionMap, u64) {
        (&self.partitions, self.segment_target_bytes)
    }

    /// Publish an already-built manifest as the next generation. Used by
    /// compaction, which rewrites content without going through a batch.
    ///
    /// Always a checkpoint: the only caller
    /// replaces the entire segment list, so the delta would be no smaller
    /// than the full document — and the chain conveniently resets at exactly
    /// the point the store shrinks.
    pub(crate) fn install(&mut self, next: Manifest) -> Result<u64> {
        if self.in_batch() {
            return Err(EngineError::invariant(
                "cannot publish a generation while a batch is open",
            ));
        }
        self.require_writable_format()?;
        let publish_t = ingest_phase::Timer::start();
        let (_, _, _, ckpt, _, _) = Self::publish(
            &self.root,
            None,
            &next,
            true,
            self.checkpoint_every(),
        )?;
        self.install_ns.0 = publish_t.ns();
        self.checkpoint_gen = ckpt;
        let fresh = self.newly_named_segments(&next);
        let adopt_t = ingest_phase::Timer::start();
        self.adopt(next);
        self.install_ns.1 = adopt_t.ns();
        // compaction and settle reach the seal-time build through here, so
        // there is one rule for every path that writes a segment file: the
        // filter exists as soon as the generation naming it is live. Timed
        // as its own slot beside publish and adopt, never folded into
        // either, so the ingest breakdown attributes it rather than hiding
        // it in `install_adopt`.
        let filter_t = ingest_phase::Timer::start();
        self.build_filters_for(&fresh);
        self.install_ns.2 = filter_t.ns();
        Ok(self.manifest.generation)
    }

    /// Rewrite one generation's record as a full checkpoint, unless it is one
    /// already. Returns whether a file was written.
    ///
    /// The rewrite is content-preserving by construction — the checkpoint is
    /// serialized from the reconstruction of the very record it replaces — so
    /// `manifest_sha` is unchanged and `CURRENT` keeps pointing at the same
    /// digest whether or not this generation is the live one. That is what
    /// lets gc cut a chain below its retention floor without a publication
    /// step (`gc.rs`, pass 0).
    pub(crate) fn materialize_checkpoint(&self, generation: u64) -> Result<bool> {
        if manifest_chain::read_record(&self.root, generation)?.is_checkpoint() {
            return Ok(false);
        }
        self.require_writable_format()?;
        let resolved = manifest_chain::reconstruct(&self.root, generation)?;
        write_atomic(
            &Self::manifest_path(&self.root, generation),
            &manifest_chain::checkpoint_json(&resolved.manifest),
        )?;
        Ok(true)
    }

    /// The on-disk manifest format this store was opened at: 2 for a store
    /// this build wrote, 1 for one the previous engine wrote.
    pub fn manifest_format(&self) -> u32 {
        self.manifest.format
    }

    /// Generation of the checkpoint the current chain rests on. Equal to
    /// `generation()` right after a checkpoint; at most `K−1` below it
    /// otherwise.
    pub fn checkpoint_generation(&self) -> u64 {
        self.checkpoint_gen
    }

    /// Override the checkpoint interval for this handle (`None` = the
    /// process default). The A/B sweeps K; tests use it to reach a periodic
    /// checkpoint in a handful of commits.
    pub fn set_checkpoint_every(&mut self, every: Option<u64>) {
        self.checkpoint_every = every.filter(|k| *k > 0);
    }

    /// The checkpoint interval in force for this handle.
    pub fn checkpoint_every(&self) -> u64 {
        self.checkpoint_every
            .unwrap_or_else(manifest_chain::checkpoint_every)
    }

    /// Refuse a write against a store this build cannot write: format-1 and
    /// format-2 stores open read-only and are
    /// converted by an explicit command, never silently reinterpreted.
    pub(crate) fn require_writable_format(&self) -> Result<()> {
        self.require_writable_handle()?;
        if manifest_chain::is_writable_format(self.manifest.format) {
            return Ok(());
        }
        Err(manifest_chain::read_only_format_error(self.manifest.format))
    }

    /// Convert an older store in place: publish the current generation's
    /// content again as a format-3 checkpoint.
    ///
    /// Format 1 or format 2, the shape is the same. From format 2 the
    /// document barely changes at all — a `sha_kind` tag and a bumped
    /// `format` — and yet `manifest_sha` changes completely, because format 3
    /// changes what that digest *is* (a Merkle root over the ordered segment
    /// set rather than the sha of the whole document). That is exactly why
    /// this is a migration and not a silent reinterpretation.
    ///
    /// **Why this does not simply rewrite in place.** The obvious approach writes generation `G` as
    /// a checkpoint, then flips `CURRENT`. `manifest_sha` covers the
    /// `format` field, so the converted document hashes differently from the
    /// one `CURRENT` currently names — and rewriting `manifests/<G>.json` in
    /// place would leave a window in which `CURRENT` names a sha no file on
    /// disk has. A crash there is an unopenable store, which is invariant 1
    /// (single publication point; everything before `CURRENT` is invisible on
    /// crash) lost. So the checkpoint is published as generation `G+1` with
    /// identical logical content: the ordinary shape, where a crash before the
    /// flip leaves `G` intact and `G+1` orphaned. One file written, nothing
    /// else touched, and the generation counter advances by one — which the
    /// TCSR stamp already treats as a rebuild trigger.
    ///
    /// Idempotent: a store already at format 2 is returned unchanged.
    pub fn upgrade_manifests(&mut self) -> Result<UpgradeReport> {
        self.require_writable_handle()?;
        if self.in_batch() {
            return Err(EngineError::invariant(
                "cannot upgrade manifests while a batch is open",
            ));
        }
        if manifest_chain::is_writable_format(self.manifest.format) {
            return Ok(UpgradeReport {
                upgraded: false,
                from_format: self.manifest.format,
                generation: self.manifest.generation,
                manifest_sha: self.manifest.manifest_sha.clone(),
            });
        }
        if !crate::MANIFEST_FORMATS_READ_ONLY.contains(&self.manifest.format) {
            return Err(manifest_chain::read_only_format_error(self.manifest.format));
        }
        let from_format = self.manifest.format;
        let mut next = self.manifest.successor(self.manifest.created_tt);
        next.format = crate::MANIFEST_FORMAT_VERSION;
        next.seal();
        let (_, _, _, ckpt, _, _) = Self::publish(
            &self.root,
            None,
            &next,
            true,
            self.checkpoint_every(),
        )?;
        self.checkpoint_gen = ckpt;
        self.adopt(next);
        Ok(UpgradeReport {
            upgraded: true,
            from_format,
            generation: self.manifest.generation,
            manifest_sha: self.manifest.manifest_sha.clone(),
        })
    }

    /// Open a segment, verifying its checksums the first time this session
    /// touches it. Every read path must come through here — opening a segment
    /// directly would skip the check and could serve corrupt rows.
    ///
    /// A budget-evicted segment lands on the slow path again and simply
    /// reopens: `verified` still names it, so the checksum walk stays
    /// once-per-session and only the decode is repaid. Callers holding an
    /// `Arc` from before an eviction keep a valid segment either way —
    /// eviction drops the cache's reference, never the data under a reader.
    pub fn open_segment(&self, file: &str) -> Result<std::sync::Arc<crate::segment::Segment<MmapSource>>> {
        if let Some(seg) = self
            .segments
            .lock()
            .expect("segment-cache mutex poisoned")
            .get(file)
        {
            OpenPathCounters::bump(&self.open_counters.cached_hits, 1);
            OpenPathCounters::bump(&self.open_counters.touched_bytes, seg.resident_bytes());
            return Ok(seg);
        }
        OpenPathCounters::bump(&self.open_counters.cached_misses, 1);
        let path = self.root.join(file);
        let first_time = {
            let seen = self.verified.lock().expect("verified-set mutex poisoned");
            !seen.contains(file)
        };
        // mapped, not read: a point lookup touches a few pages, and reading
        // the whole file per open made `believed_*` cost a full scan even
        // when it wanted one row. `segment_source` serves the generation's
        // pinned map when one exists rather than mapping fresh.
        let open_t = ingest_phase::Timer::start();
        let mut seg = crate::segment::Segment::open(&path, self.segment_source(file, &path)?, first_time)?;
        // An event-lane segment that stores `vt_e` gets the
        // planner's `vt_e` staircase derived here, once per cached open, and
        // kept with the segment in the cache. Nothing is written.
        if self.designs.staircase && seg.wants_derived_vt_e_staircase() {
            let t = std::time::Instant::now();
            if seg.derive_vt_e_staircase(crate::defaults::DERIVED_VT_E_BLOCK_ROWS)? {
                crate::phase::since(crate::phase::Phase::StairDeriveNs, t);
                crate::phase::add(crate::phase::Phase::StairDerived, 1);
                crate::phase::add(
                    crate::phase::Phase::StairDerivedBytes,
                    seg.derived_vt_e_staircase().map_or(0, |d| d.heap_bytes()),
                );
            }
        }
        let seg = std::sync::Arc::new(seg);
        open_t.stop_ctx(ingest_phase::S::OpenCached);
        ingest_phase::add_ctx(ingest_phase::S::Opens, 1);
        OpenPathCounters::bump(&self.open_counters.decoded_bytes, seg.resident_bytes());
        OpenPathCounters::bump(&self.open_counters.touched_bytes, seg.resident_bytes());
        if first_time {
            OpenPathCounters::bump(&self.open_counters.first_touch_verifies, 1);
            self.verified
                .lock()
                .expect("verified-set mutex poisoned")
                .insert(file.to_string());
        }
        self.segments
            .lock()
            .expect("segment-cache mutex poisoned")
            .insert(file.to_string(), seg.clone());
        Ok(seg)
    }

    /// Open a segment for a one-pass streaming walk, without adding it to
    /// the session's segment cache.
    ///
    /// `open_segment` is right for point reads and scans that revisit
    /// segments across calls: caching the decoded columns pays for itself.
    /// A full-store fold that touches every segment exactly once is the
    /// opposite case — routing it through `open_segment` would leave every
    /// segment's decoded columns resident (up to the cache's byte budget,
    /// which defaults to half of physical RAM, i.e. effectively unbounded
    /// for this purpose) for the rest of the session, so peak RSS grows with
    /// the number of segments touched rather than staying at one segment's
    /// worth. This opens the file directly and returns an owned `Segment`
    /// the caller drops after folding it — the same one-at-a-time discipline
    /// `integrity.rs`'s row scan uses, and for the same reason.
    ///
    /// Checksums are still verified the first time this session touches a
    /// file (tracked in `verified`, shared with `open_segment` so the two
    /// paths never re-verify each other's work) — this only skips caching
    /// the *decoded* segment, not the corruption check.
    pub(crate) fn open_segment_uncached(&self, file: &str) -> Result<crate::segment::Segment<MmapSource>> {
        let path = self.root.join(file);
        let first_time = {
            let seen = self.verified.lock().expect("verified-set mutex poisoned");
            !seen.contains(file)
        };
        let open_t = ingest_phase::Timer::start();
        let seg = crate::segment::Segment::open(&path, self.segment_source(file, &path)?, first_time)?;
        open_t.stop_ctx(ingest_phase::S::OpenUncached);
        ingest_phase::add_ctx(ingest_phase::S::Opens, 1);
        OpenPathCounters::bump(&self.open_counters.uncached_opens, 1);
        OpenPathCounters::bump(&self.open_counters.decoded_bytes, seg.resident_bytes());
        OpenPathCounters::bump(&self.open_counters.touched_bytes, seg.resident_bytes());
        if first_time {
            OpenPathCounters::bump(&self.open_counters.first_touch_verifies, 1);
            self.verified
                .lock()
                .expect("verified-set mutex poisoned")
                .insert(file.to_string());
        }
        Ok(seg)
    }

    /// `open_segment` or `open_segment_uncached`, chosen by the caller.
    ///
    /// The rule the two modes encode: a read that addresses *rows* — the
    /// point-read path, which already holds exact `(file, row)` pairs — keeps
    /// the cache, because it will come back to the same segment; a read that
    /// walks *every segment once* does not, because caching it makes peak RSS
    /// grow with the store instead of with one segment. `stats_accum` adopted
    /// that discipline first (`read.rs`, "Streaming, addendum 2"); this is the
    /// same choice, made a parameter so the whole-store paths can share it.
    pub(crate) fn open_segment_mode(&self, file: &str, mode: OpenMode) -> Result<SegmentHandle> {
        match mode {
            OpenMode::Cached => Ok(SegmentHandle::Cached(self.open_segment(file)?)),
            OpenMode::Uncached => Ok(SegmentHandle::Uncached(Box::new(
                self.open_segment_uncached(file)?,
            ))),
        }
    }

    /// Walk every file this generation references, checking magic numbers,
    /// checksums, completion markers, and cross-references.
    ///
    /// Corruption must be *detected*, not merely survived: the durability
    /// objective is "never expose an undetected inconsistent generation".
    /// Problems are collected rather than raised on the first
    /// one, so an operator sees the whole picture instead of peeling the
    /// onion a file at a time.
    ///
    /// This is the *fast* mode of `gnomon store verify`. It answers "is every
    /// file this generation names intact, and does it hold what the manifest
    /// claims" — bounded by the bytes on disk, and nothing more. What it does
    /// not do is look inside a segment at the rows, walk the retained
    /// manifests it did not have to read, or check any invariant that spans
    /// two files; that is [`verify_full`](Self::verify_full).
    pub fn verify(&self) -> Result<VerifyReport> {
        self.verify_fast()
    }

    /// Fast mode. See [`verify`](Self::verify).
    pub fn verify_fast(&self) -> Result<VerifyReport> {
        Ok(self.verify_files()?.0)
    }

    /// The file-level walk both modes start from. Full mode wants the close
    /// records this pass already read, so it hands them back rather than
    /// making the caller read every run file a second time.
    fn verify_files(&self) -> Result<(VerifyReport, Vec<CloseRecord>)> {
        let generation = self.manifest.generation;
        let mut closes: Vec<CloseRecord> = Vec::new();
        let mut report = VerifyReport {
            generation,
            manifest_format: self.manifest.format,
            mode: "fast".into(),
            ..Default::default()
        };
        self.manifest.verify()?;

        // Invariant 4: this generation must be checkable *from disk alone*.
        // With format 2 that is no longer a single document read — it is the
        // checkpoint plus the delta chain above it, each record self-sha'd,
        // each link's `parent_sha` matched, and the reconstruction re-hashed
        // against what each delta claims. Doing it here rather than trusting
        // the in-memory manifest is the whole point: a handle that has been
        // open across a gc must still be able to prove its own view.
        match manifest_chain::reconstruct(&self.root, self.manifest.generation) {
            Ok(resolved) => {
                report.manifest_deltas = resolved.deltas;
                report.manifest_checkpoint = resolved.checkpoint;
                if resolved.manifest != self.manifest {
                    report.flag(Finding::error(
                        integrity::LAYER_MANIFEST,
                        "reconstruction-mismatch",
                        manifest_chain::manifest_path(Path::new(""), self.manifest.generation)
                            .to_string_lossy()
                            .into_owned(),
                        self.manifest.generation,
                        format!(
                            "generation {} reconstructs from disk to sha {} but this handle holds {}",
                            self.manifest.generation,
                            resolved.manifest.manifest_sha,
                            self.manifest.manifest_sha
                        ),
                    ));
                }
            }
            Err(e) => report.flag(Finding::error(
                integrity::LAYER_MANIFEST,
                "chain-broken",
                "",
                self.manifest.generation,
                format!("manifest chain: {}", e.message),
            )),
        }

        let m = &self.manifest;
        let segments: Vec<(&str, u32, &str)> = m
            .edge_lanes
            .event
            .iter()
            .chain(m.edge_lanes.interval.iter())
            .chain(m.node_store.iter())
            .map(|e| (e.file.as_str(), e.rows, e.sha.as_str()))
            .collect();

        for (file, claimed_rows, claimed_sha) in segments {
            let path = self.root.join(file);
            match crate::segment::MemorySource::load(&path)
                .and_then(|src| crate::segment::Segment::open(&path, src, true))
            {
                Ok(seg) => {
                    report.segments_checked += 1;
                    report.rows += seg.rows() as u64;
                    // Layout quality, not integrity: a batch-written segment
                    // holds exactly one tt_s run, and compaction's global
                    // re-sort can leave nearly one per row. Nothing about
                    // that is corrupt, so it is not a problem — but it is
                    // what the read path pays per materialized row, so it
                    // belongs somewhere a regression can be seen.
                    let runs = seg.tt_s_run_count() as u32;
                    report.max_tt_s_runs = report.max_tt_s_runs.max(runs);
                    report.tt_s_runs += runs as u64;
                    if seg.rows() != claimed_rows {
                        report.flag(Finding::error(
                            integrity::LAYER_SEGMENT,
                            "row-count-mismatch",
                            file,
                            generation,
                            format!(
                                "{file}: manifest claims {claimed_rows} rows, segment holds {}",
                                seg.rows()
                            ),
                        ));
                    }
                    // Compare the segment's content address against the one its
                    // entry names. Every checksum a segment carries is its
                    // own, so a file replaced by another *valid* segment of
                    // the same name passes all of them; only the manifest can
                    // say it is not the segment this generation published.
                    // Legacy entries (`""`, written by an older engine) name no
                    // address and are not checked. Here rather than on the
                    // open path: `open` must not pay a footer read per
                    // segment, and this walk has just read every file whole.
                    if !claimed_sha.is_empty() {
                        match crate::tcsr::segment_body_sha(&path) {
                            Ok(actual) if actual == claimed_sha => {}
                            Ok(actual) => report.flag(Finding::error(
                                integrity::LAYER_SEGMENT,
                                "content-address-mismatch",
                                file,
                                generation,
                                format!(
                                    "{file}: manifest names content {claimed_sha}, the file \
                                     records {actual} — it is not the segment this \
                                     generation published"
                                ),
                            )),
                            Err(e) => report.flag(Finding::error(
                                integrity::LAYER_SEGMENT,
                                "segment-unreadable",
                                file,
                                generation,
                                format!("{file}: {e}"),
                            )),
                        }
                    }
                }
                Err(e) => report.flag(Finding::error(
                    integrity::LAYER_SEGMENT,
                    if path.exists() {
                        "segment-unreadable"
                    } else {
                        "segment-missing"
                    },
                    file,
                    generation,
                    format!("{file}: {e}"),
                )),
            }
        }

        for run in &m.close_runs {
            let path = self.root.join(&run.file);
            match crate::visibility::read_close_run(&path) {
                Ok(records) => {
                    report.close_runs_checked += 1;
                    report.closes += records.len() as u64;
                    if records.len() as u32 != run.entries {
                        report.flag(Finding::error(
                            integrity::LAYER_CLOSE,
                            "entry-count-mismatch",
                            run.file.clone(),
                            generation,
                            format!(
                                "{}: manifest claims {} entries, file holds {}",
                                run.file,
                                run.entries,
                                records.len()
                            ),
                        ));
                    }
                    closes.extend(records);
                }
                Err(e) => report.flag(Finding::error(
                    integrity::LAYER_CLOSE,
                    if path.exists() {
                        "close-run-unreadable"
                    } else {
                        "close-run-missing"
                    },
                    run.file.clone(),
                    generation,
                    format!("{}: {e}", run.file),
                )),
            }
        }

        report.dict_records = self.dict.len();
        if self.dict.len() != m.dict.records {
            report.flag(Finding::error(
                integrity::LAYER_DICT,
                "record-count-mismatch",
                DICT,
                generation,
                format!(
                    "dictionary holds {} records, manifest claims {}",
                    self.dict.len(),
                    m.dict.records
                ),
            ));
        }
        Ok((report, closes))
    }

    /// Full mode: everything `verify_fast` checks, plus the invariants that
    /// span files or live inside a segment's rows.
    ///
    /// Three checks the fast pass structurally cannot make:
    ///
    /// * the **manifest parent chain** across every retained generation, not
    ///   just the links a reconstruction of `CURRENT` happens to walk;
    /// * **dictionary-code reference validity** - every code a row names must
    ///   exist in the dictionary prefix the manifest commits;
    /// * the **bitemporal row invariants**, including I1 disjointness of
    ///   believed versions per identity, checked against the bytes on disk
    ///   rather than against the write path that is supposed to maintain it.
    ///
    /// Read-only, like everything else here. A defect is a finding; nothing
    /// is trimmed, recovered or rewritten. This is the mode the corruption
    /// sweep uses as its oracle, so the answer has to describe the store as
    /// it found it.
    pub fn verify_full(&self) -> Result<VerifyReport> {
        let (mut report, closes) = self.verify_files()?;
        report.mode = "full".into();

        for f in self.digest_oracle() {
            report.flag(f);
        }
        for f in integrity::parent_chain(&self.root, self.manifest.generation) {
            report.flag(f);
        }
        for f in integrity::check_close_targets(&self.manifest, &closes) {
            report.flag(f);
        }

        let index = CloseIndex::from_records(closes);
        let (scan, findings) =
            integrity::check_rows(&self.root, &self.manifest, &self.dict, &index)?;
        report.rows_walked = scan.rows_walked;
        report.believed_rows = scan.believed_rows;
        report.identities_checked = scan.identities_checked;
        for f in findings {
            report.flag(f);
        }
        Ok(report)
    }

    /// The price of an incremental digest, paid in the mode nobody runs per
    /// commit.
    ///
    /// From format 3 `manifest_sha` is maintained by appending to a spine, so
    /// a bug in that update would be *self-consistent*: the store would seal
    /// with a wrong digest, publish it, and check it against itself forever.
    /// The defence is a recomputation that shares no code with the
    /// incremental path — [`Manifest::body_sha_canonical`], the whole tree
    /// rebuilt from the whole ordered segment set — compared against the
    /// value on disk in `CURRENT`, which is what was actually published
    /// rather than what this handle currently believes.
    ///
    /// `verify_files` already re-derives the handle's own digest from scratch
    /// and refuses outright if it disagrees; what this adds is the crossing
    /// to the published bytes. Read-only, like everything else in verify.
    fn digest_oracle(&self) -> Vec<Finding> {
        let path = self.root.join(CURRENT);
        let rel = CURRENT.to_string();
        let generation = self.manifest.generation;
        let text = match fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) => {
                return vec![Finding::error(
                    integrity::LAYER_MANIFEST,
                    "current-unreadable",
                    rel,
                    generation,
                    format!("CURRENT could not be read: {e}"),
                )]
            }
        };
        let mut parts = text.split_whitespace();
        let (Some(gen), Some(sha)) = (parts.next(), parts.next()) else {
            return vec![Finding::error(
                integrity::LAYER_MANIFEST,
                "current-malformed",
                rel,
                generation,
                format!("CURRENT must contain '<generation> <sha>', found {text:?}"),
            )];
        };
        let mut findings = Vec::new();
        // A handle that has committed since it opened is *ahead* of nothing —
        // it publishes CURRENT itself — but one pinned to an older generation
        // legitimately is behind, and that is not a defect.
        if gen.parse::<u64>() != Ok(generation) {
            return findings;
        }
        let oracle = self.manifest.body_sha_canonical();
        if oracle != sha {
            findings.push(Finding::error(
                integrity::LAYER_MANIFEST,
                "digest-oracle-mismatch",
                rel,
                generation,
                format!(
                    "generation {generation} recomputes from scratch to {oracle} but \
                     CURRENT publishes {sha} — the incrementally maintained digest and \
                     an independent recomputation of it disagree"
                ),
            ));
        }
        findings
    }

    /// Closes every read in this handle must honour: the committed ones, plus
    /// the open batch's, which are not durable yet but are this writer's own.
    ///
    /// Read-your-own-writes is not a convenience here. `apply_ops` asks what
    /// is believed *between* its ops to decide what to carve, so a batch that
    /// could not see its own corrections would carve against a version it had
    /// already stopped believing and write a fragment over the top of it
    /// (unlike DuckDB, where an uncommitted `UPDATE` was visible to the
    /// same connection, and this was not). Staged rows and closes against
    /// them were already visible; closes against *committed* rows — the only
    /// kind an ordinary correction makes — were the hole.
    ///
    /// Nothing leaks the other way: `pending_closes` belongs to the writing
    /// handle, and a second handle cannot have an open batch (single-writer).
    pub fn close_index(&self) -> Result<std::sync::Arc<CloseIndex>> {
        match &self.pending_overlay {
            Some(idx) => Ok(idx.clone()),
            None => self.committed_close_index(),
        }
    }

    /// Closes this generation makes visible, resolved for scanning. A reader
    /// on an older generation loads that generation's runs and therefore sees
    /// that generation's beliefs — never a newer correction: each handle
    /// caches against its *own* manifest generation, so a pinned older view
    /// serves its own generation's closes, not a newer handle's.
    /// `segment id -> filename` for both lanes, for this generation.
    ///
    /// Returned behind an `Arc` so callers share one map instead of each
    /// building its own; the caller looks names up rather than owning them.
    pub(crate) fn segment_files(&self) -> std::sync::Arc<SegmentFiles> {
        let generation = self.manifest.generation;
        if let Some((cached_gen, files)) = self
            .files_cache
            .lock()
            .expect("files-cache mutex poisoned")
            .as_ref()
        {
            if *cached_gen == generation {
                return files.clone();
            }
        }
        let m = &self.manifest;
        let files = std::sync::Arc::new(SegmentFiles {
            edge: m
                .edge_lanes
                .event
                .iter()
                .chain(m.edge_lanes.interval.iter())
                .map(|e| (segment_id_of(&e.file), e.file.clone()))
                .collect(),
            node: m
                .node_store
                .iter()
                .map(|e| (segment_id_of(&e.file), e.file.clone()))
                .collect(),
        });
        *self.files_cache.lock().expect("files-cache mutex poisoned") =
            Some((generation, files.clone()));
        files
    }

    pub(crate) fn committed_close_index(&self) -> Result<std::sync::Arc<CloseIndex>> {
        if self.current_only {
            // the stripped configuration has no closes by construction; a
            // run appearing anyway means the marker and the manifest
            // disagree, which must surface rather than be half-honoured
            if !self.manifest.close_runs.is_empty() {
                return Err(EngineError::invariant(
                    "current-only store has close runs — the CURRENT_ONLY \
                     marker does not match the manifest",
                ));
            }
            return Ok(std::sync::Arc::new(CloseIndex::default()));
        }
        let generation = self.manifest.generation;
        let runs = &self.manifest.close_runs;

        // Reuse the cached index when this generation only *appended* runs to
        // the one it was built from. Close-run files are immutable and
        // the manifest normally appends, so the cached index is a valid prefix
        // of the answer and only the new runs need reading — rather than all
        // of them, which was 37 ms at 999 runs and quadratic over a run of
        // corrections.
        //
        // The prefix check is deliberately strict, because a wrong answer here
        // is a silently wrong belief: `close_runs` is **not** append-only
        // across compaction, which folds runs into sidecars and empties the
        // list. Length, plus the file name *and* its sha at the last folded
        // index, must all still match; anything else falls back to a rebuild.
        // The entry is *taken*, not cloned, and the guard dropped before
        // anything else runs — both halves matter. The mutex is not
        // reentrant, and an earlier draft held the guard across the write
        // below, which deadlocked the whole suite. And a draft that *cloned*
        // the entry left the cache holding a second Arc, so `make_mut` below
        // cloned the whole map on every extend — O(all closes) memcpy per
        // commit, quietly contradicting the "in place" design. Taking the
        // entry makes this handle's copy the only one (unless a reader holds
        // a pinned Arc, which is exactly when a copy is correct). Every
        // return path below restores the cache. A concurrent reader that
        // finds the cache empty mid-flight rebuilds redundantly, which is
        // wasted work, never a wrong answer.
        let cached: Option<CloseCacheEntry> = self
            .close_cache
            .lock()
            .expect("close-cache mutex poisoned")
            .take();

        let mut reuse: Option<(std::sync::Arc<CloseIndex>, usize)> = None;
        if let Some((cached_gen, folded, last, idx)) = cached {
            if cached_gen == generation {
                let out = idx.clone();
                *self.close_cache.lock().expect("close-cache mutex poisoned") =
                    Some((cached_gen, folded, last, idx));
                return Ok(out);
            }
            let extends = runs.len() >= folded
                && match (folded, &last) {
                    (0, _) => true,
                    (n, Some((file, sha))) => runs
                        .get(n - 1)
                        .is_some_and(|r| &r.file == file && &r.sha == sha),
                    _ => false,
                };
            if extends {
                if folded == runs.len() {
                    // same run set, newer generation (a commit that closed
                    // nothing): the index is already the answer
                    *self.close_cache.lock().expect("close-cache mutex poisoned") =
                        Some((generation, folded, last, idx.clone()));
                    return Ok(idx);
                }
                reuse = Some((idx, folded));
            }
        }

        let (mut idx, from) = match reuse {
            Some((cached, folded)) => (cached, folded),
            None => (std::sync::Arc::new(CloseIndex::default()), 0),
        };
        {
            // copy-on-write, and genuinely in place in the
            // common case: the cache's Arc was taken above, so this handle
            // holds the only reference unless a reader is pinned to this very
            // index, and that pin is precisely when `make_mut`'s clone is
            // correct rather than waste.
            let target = std::sync::Arc::make_mut(&mut idx);
            for r in &runs[from..] {
                for rec in read_close_run(&self.root.join(&r.file))? {
                    target.close_row(rec.segment, rec.row, rec.tt_e);
                }
            }
        }
        let last = runs.last().map(|r| (r.file.clone(), r.sha.clone()));
        *self.close_cache.lock().expect("close-cache mutex poisoned") =
            Some((generation, runs.len(), last, idx.clone()));
        Ok(idx)
    }

    pub fn in_batch(&self) -> bool {
        self.batch_tt.is_some()
    }

    pub fn begin(&mut self, tt: i64) -> Result<()> {
        self.require_writable_handle()?;
        if let Some(open_tt) = self.batch_tt {
            return Err(EngineError::invariant(format!(
                "a batch at tt={open_tt} is already open (single-writer, no nesting)"
            )));
        }
        if tt <= self.manifest.created_tt && self.manifest.generation > 0 {
            return Err(EngineError::invariant(format!(
                "transaction time must advance: batch tt={tt} is not after \
                 generation {}'s tt={}",
                self.manifest.generation, self.manifest.created_tt
            )));
        }
        self.batch_tt = Some(tt);
        Ok(())
    }

    /// Register an entity, returning its dense id.
    ///
    /// Deliberately does not require an open batch: `apply_ops` registers
    /// entities before it knows which rows it will write, and a staged
    /// dictionary entry is discarded on rollback and published on commit
    /// either way. Visible to reads immediately; durable only at commit.
    pub fn ensure_entity(&mut self, uid: &str, label: &str) -> Result<u32> {
        self.dict.ensure(uid, label)
    }

    /// Layout policy. Changing it affects only future segments — lane
    /// assignment is physical, so already-written rows are unaffected.
    pub fn set_layout(&mut self, partitions: PartitionMap, segment_target_bytes: u64) {
        self.partitions = partitions;
        self.segment_target_bytes = segment_target_bytes;
    }

    pub fn stage_edge(&mut self, row: EdgeRow) -> Result<()> {
        self.require_batch()?;
        self.staging.push_edge(row);
        Ok(())
    }

    pub fn stage_node(&mut self, row: NodeRow) -> Result<()> {
        self.require_batch()?;
        self.staging.push_node(row);
        Ok(())
    }

    /// Stop believing a version from `tt_e` on.
    ///
    /// The target may be a row staged in this very batch (a carve that splits
    /// a version and closes the original) or one committed earlier. Staged
    /// hits fold into the segment's sidecar; committed hits become a close
    /// run. Either way the row itself is never touched — closing is data.
    ///
    /// Locating a committed row goes through the identity postings:
    /// candidates by vid prefix, the full vid verified at the row. Same
    /// answer the linear scan gave, without the per-close segment walk that
    /// made correction-heavy replay superlinear.
    pub fn close_version(&mut self, kind: RowKind, vid: Id96, tt_e: i64) -> Result<()> {
        self.close_version_at(kind, vid, None, tt_e)
    }

    /// [`Self::close_version`], told the row's `vt_s`.
    ///
    /// `vt_s` is the first half of the physical sort key, so knowing it turns
    /// locating the row from a whole-store postings probe into a manifest
    /// range prune plus one binary search (`read.rs::locate_vid`). It is
    /// optional because it is an optimisation and never an answer: passing
    /// `None` searches every segment and finds exactly the same row. The
    /// Python correction path always has it —`base.py::_supersede_edges`
    /// holds the whole `EdgeVersion` it is superseding — and passes it.
    pub fn close_version_at(
        &mut self,
        kind: RowKind,
        vid: Id96,
        vt_s: Option<i64>,
        tt_e: i64,
    ) -> Result<()> {
        if self.current_only {
            return Err(EngineError::invariant(
                "a current-only store cannot record a correction: closing a \
                 version creates the belief history this configuration \
                 deliberately does not keep",
            ));
        }
        self.require_batch()?;
        let staged_hit = match kind {
            RowKind::Edge => self.staging.has_edge_vid(vid),
            RowKind::Node => self.staging.has_node_vid(vid),
        };
        if staged_hit {
            ingest_phase::add(ingest_phase::G::ClosesStaged, 1);
            self.staged_closes.insert(vid, tt_e);
            return Ok(());
        }
        let _ctx = ingest_phase::CtxGuard::enter_if_unclaimed(ingest_phase::Ctx::Correct);
        let locate_t = ingest_phase::Timer::start();
        let located = self.locate_committed(kind, vid, vt_s)?;
        locate_t.stop(ingest_phase::G::CloseLocateNs);
        match located {
            Some((segment, row)) => {
                ingest_phase::add(ingest_phase::G::ClosesCommitted, 1);
                let overlay_t = ingest_phase::Timer::start();
                self.pending_closes.push(CloseRecord {
                    kind,
                    segment,
                    row,
                    tt_e,
                });
                if self.pending_overlay.is_none() {
                    let committed = self.committed_close_index()?;
                    self.pending_overlay =
                        Some(std::sync::Arc::new(CloseIndex::layered_over(committed)));
                }
                let overlay = self.pending_overlay.as_mut().expect("just set");
                std::sync::Arc::make_mut(overlay).close_row(segment, row, tt_e);
                overlay_t.stop(ingest_phase::G::CloseOverlayNs);
                Ok(())
            }
            None => Err(EngineError::not_found(format!(
                "no version {} to close",
                vid.to_hex()
            ))),
        }
    }

    /// Discard a version this batch staged and has already replaced.
    ///
    /// Closing it would record a belief that ran from `tt` to `tt` — held at
    /// no transaction time at all — and there is no such thing, so the row
    /// goes instead. Only staging is reachable: a committed row was
    /// written by an earlier transaction time and is closed, never retired,
    /// which is also why this can never touch a sealed segment.
    pub fn retire_version(&mut self, kind: RowKind, vid: Id96) -> Result<()> {
        self.require_batch()?;
        let removed = match kind {
            RowKind::Edge => self.staging.retire_edge(vid),
            RowKind::Node => self.staging.retire_node(vid),
        };
        if !removed {
            return Err(EngineError::not_found(format!(
                "no version {} staged in this batch to retire",
                vid.to_hex()
            )));
        }
        self.staged_closes.remove(&vid);
        Ok(())
    }

    /// Physical location of a committed version, by manifest range prune and
    /// binary search when `vt_s` is known.
    fn locate_committed(
        &self,
        kind: RowKind,
        vid: Id96,
        vt_s: Option<i64>,
    ) -> Result<Option<(u64, u32)>> {
        self.locate_vid(kind, vid, vt_s)
    }

    /// Rows staged in the open batch — the read-your-own-writes overlay that
    /// `apply_ops` depends on.
    pub fn staged_edges(&self) -> &[EdgeRow] {
        self.staging.edges()
    }

    /// Closes recorded against rows staged in this batch — the overlay that
    /// makes a same-batch carve visible to a read inside the same batch.
    pub(crate) fn staged_closes(&self) -> &HashMap<Id96, i64> {
        &self.staged_closes
    }

    pub fn staged_nodes(&self) -> &[NodeRow] {
        self.staging.nodes()
    }

    /// The staging buffers themselves, for the reads that go through the
    /// identity index instead of walking every staged row.
    pub(crate) fn staging(&self) -> &Staging {
        &self.staging
    }

    /// Publish the open batch as a new generation. `event_log` records where
    /// in the (already durable) log this generation ends.
    ///
    /// **The manifest is advanced in place.** It used to be cloned by
    /// `successor`, cloned again inside `seal`'s digest, and a third time by
    /// `publish`'s redundant `verify` — three O(live-segments) deep copies
    /// and two full serialize-and-sha256 passes, none of which the commit's
    /// timed phases counted, and which were 100% of the measured 1.8×
    /// last/first-decile growth. What is left is an append
    /// to four `Vec`s, a spine update per appended entry, and one 200-byte
    /// hash.
    ///
    /// In-place means a failure part-way has to be undone, which
    /// [`CommitBase`] does. Nothing is visible to any other reader until
    /// `CURRENT` flips regardless — that is invariant 1 and it has not moved
    /// — so the rollback is about this handle's own view, not about
    /// durability.
    pub fn commit(&mut self, event_log: EventLogRef) -> Result<u64> {
        let tt = self.require_batch()?;
        self.require_writable_format()?;
        let mut phases = CommitPhases::default();
        let commit_start = std::time::Instant::now();
        let capture_t = std::time::Instant::now();
        let base = CommitBase::capture(&self.manifest, &self.merkle);
        phases.capture_us = capture_t.elapsed().as_micros() as u64;
        match self.commit_inner(tt, event_log, &base, &mut phases, commit_start) {
            Ok(generation) => {
                record_commit_phases(&phases);
                Ok(generation)
            }
            Err(e) => {
                base.restore(&mut self.manifest, &mut self.merkle);
                Err(e)
            }
        }
    }

    fn commit_inner(
        &mut self,
        tt: i64,
        event_log: EventLogRef,
        base: &CommitBase,
        phases: &mut CommitPhases,
        commit_start: std::time::Instant,
    ) -> Result<u64> {
        // Resolved here, before `self.manifest` is borrowed as the successor
        // and before `staging` is borrowed mutably, so the seed is read off
        // the *committed* generation this commit supersedes — the state a
        // replay of the same log is also at when it reaches this record
        // (see `crate::sealsize`).
        let ratio_t = ingest_phase::Timer::start();
        let mut ratios = self.seal_ratios_for_batch(self.staging.staged_rows());
        ratio_t.stop(ingest_phase::G::CommitRatioSeedNs);
        let next = &mut self.manifest;
        next.generation = base.generation + 1;
        next.parent = Some(base.generation);
        next.created_tt = tt;
        next.manifest_sha = String::new();
        let state = self
            .merkle
            .as_mut()
            .expect("a writable store always carries a Merkle state");

        // step 2 — segments: written and fsynced before anything names them
        let phase = std::time::Instant::now();
        let mut next_id = next.next_segment_id;
        let mut sealed_files: Vec<(RowKind, String)> = Vec::new();
        let seg_dir = self.root.join("seg");
        let partitions = self.partitions;
        let target_bytes = self.segment_target_bytes;
        let _seal_ctx = ingest_phase::CtxGuard::enter(ingest_phase::Ctx::Seal);
        let sealed = self.staging.seal(
            &seg_dir,
            &partitions,
            target_bytes,
            &mut ratios,
            &mut next_id,
            &self.staged_closes,
        )?;
        next.next_segment_id = next_id;
        for (lane, entry) in sealed.edges {
            next.stats.n_edge_versions += entry.rows as u64;
            sealed_files.push((RowKind::Edge, entry.file.clone()));
            match lane {
                Lane::Event => {
                    state.push_edge_event(&entry);
                    next.edge_lanes.event.push(entry);
                }
                Lane::Interval => {
                    state.push_edge_interval(&entry);
                    next.edge_lanes.interval.push(entry);
                }
            }
        }
        for entry in sealed.nodes {
            next.stats.n_node_versions += entry.rows as u64;
            sealed_files.push((RowKind::Node, entry.file.clone()));
            state.push_node(&entry);
            next.node_store.push(entry);
        }
        phases.seal_us = phase.elapsed().as_micros() as u64;
        drop(_seal_ctx);
        crash_point("after_seal");

        // close run for corrections landing on already-committed rows,
        // durable before the manifest that lists it
        let phase = std::time::Instant::now();
        if !self.pending_closes.is_empty() {
            let file = format!("close/{:012}.tgc", next.generation);
            let path = self.root.join(&file);
            let entries = write_close_run(&path, &self.pending_closes)?;
            let run = CloseRunRef {
                file,
                entries,
                sha: String::new(),
            };
            state.push_close_run(&run);
            next.close_runs.push(run);
        }
        phases.closes_us = phase.elapsed().as_micros() as u64;
        crash_point("after_close_runs");

        // fold this batch into the running statistics rather than
        // invalidating them; staging is still intact here
        let phase = std::time::Instant::now();
        {
            *self.stats_summary.lock().expect("stats summary mutex poisoned") = None;
            let mut cell = self.stats.lock().expect("stats mutex poisoned");
            if let Some(acc) = cell.as_mut() {
                for r in self.staging.edges() {
                    acc.add_edge(r.vt_s, r.vt_e, &r.rel_type, r.src_id);
                }
                acc.n_node_versions += self.staging.nodes().len() as u64;
            }
        }
        phases.stats_us = phase.elapsed().as_micros() as u64;

        // step 3 — dictionary tail durable before anything references it
        let phase = std::time::Instant::now();
        let (records, bytes) = self.dict.commit_to_disk()?;
        phases.dict_us = phase.elapsed().as_micros() as u64;
        crash_point("after_dict");
        next.event_log = event_log;
        next.dict.records = records;
        next.dict.bytes = bytes;
        next.stats.n_entities = self.dict.len();
        // O(1) against the state the appends above kept in step, where
        // `seal()` would re-serialize every live segment — except below
        // format 3, where `seal_with` itself falls back to the O(segments)
        // whole-document rehash (`Manifest::legacy_body_sha`); timed either
        // way so that fallback shows up rather than vanishing into the gap
        // between `dict_us` and step 4.
        let phase = std::time::Instant::now();
        next.seal_with(state);
        phases.digest_us = phase.elapsed().as_micros() as u64;
        phases.segments_named = (next.edge_lanes.event.len()
            + next.edge_lanes.interval.len()
            + next.node_store.len()) as u64;

        // steps 4-5 — manifest record, then CURRENT
        let span = base.span();
        let (manifest_us, current_us, manifest_bytes, checkpoint_gen, delta_build_us, debug_verify_us) =
            Self::publish(
                &self.root,
                Some((&span, self.checkpoint_gen)),
                &self.manifest,
                false,
                self.checkpoint_every(),
            )?;
        phases.debug_verify_us = debug_verify_us;
        phases.delta_build_us = delta_build_us;
        phases.manifest_us = manifest_us;
        phases.current_us = current_us;
        phases.manifest_bytes = manifest_bytes;
        phases.manifest_checkpoint = checkpoint_gen == self.manifest.generation;
        self.checkpoint_gen = checkpoint_gen;
        let pin_t = ingest_phase::Timer::start();
        self.repin(base.generation, self.manifest.generation);
        // An ordinary commit only appends segments, but pin the new
        // set anyway (cheap — it is only the newly sealed files that are not
        // already pinned) so this handle's own reads never fall onto the
        // lazy path just because they were staged in the batch that just
        // published rather than seen by a prior `pin_generation_files` call.
        self.pin_generation_files();
        pin_t.stop(ingest_phase::G::CommitPinNs);
        // Step 6 — the identity filters of the segments just sealed.
        // After the publication point, and deliberately: the filter is a
        // rebuildable sidecar no manifest names, so a crash here costs a
        // rebuild rather than a generation. Outside the pin timer on
        // purpose: `commit_pin_ns` keeps meaning exactly what it meant, and
        // the filter build has its own slot, `commit_vidfilter_ns`.
        let phase = std::time::Instant::now();
        self.build_filters_for(&sealed_files);
        phases.vidfilter_us = phase.elapsed().as_micros() as u64;
        self.staging.clear();
        self.staged_closes.clear();
        self.pending_closes.clear();
        self.pending_overlay = None;
        self.batch_tt = None;
        phases.total_us = commit_start.elapsed().as_micros() as u64;
        self.last_commit = Some(*phases);
        Ok(self.manifest.generation)
    }

    /// Abandon the open batch. Staged dictionary entries are dropped; nothing
    /// was published, so the store is already at the previous generation.
    pub fn rollback(&mut self) -> Result<()> {
        self.require_batch()?;
        self.dict.discard_staged();
        self.staging.clear();
        self.staged_closes.clear();
        self.pending_closes.clear();
        self.pending_overlay = None;
        self.batch_tt = None;
        Ok(())
    }

    pub(crate) fn require_batch(&self) -> Result<i64> {
        self.batch_tt
            .ok_or_else(|| EngineError::invariant("no batch is open; call begin() first"))
    }
}

impl Drop for NativeStore {
    fn drop(&mut self) {
        crate::gc::unpin(&self.pin_key, self.manifest.generation);
    }
}

/// Running statistics. Counts cover every stored row, not just believed
/// ones, matching what the DuckDB adapter reports — the two backends have to
/// agree here or `estimate_cost` would diverge between them.
/// One live segment's transaction-time shape, as `segment_tt_profile` reports
/// it.
///
/// `tt_s_runs` against `rows` is the ratio that decides whether the folded
/// `(tt_min, tt_max)` bounds can answer a belief query for a segment without
/// reading a run, or whether every run has to be walked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentTtProfile {
    pub lane: &'static str,
    pub file: String,
    pub rows: u32,
    pub tt_s_runs: u32,
    pub tt_min: i64,
    pub tt_max: i64,
}

#[derive(Default, Clone)]
pub struct StatsAccum {
    pub n_node_versions: u64,
    pub n_edge_versions: u64,
    pub vt_min: Option<i64>,
    pub vt_max: Option<i64>,
    pub rel_type_counts: std::collections::HashMap<String, u64>,
    /// Out-degree per source, so the max is available without a group-by.
    pub out_degree: std::collections::HashMap<u32, u64>,
}

/// The statistics `stats()` reports, without the per-source map they are
/// folded from. `max_out_degree` is the map's maximum, taken once
/// when the summary is derived rather than on every read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatsSummary {
    pub n_node_versions: u64,
    pub n_edge_versions: u64,
    pub vt_min: Option<i64>,
    pub vt_max: Option<i64>,
    pub rel_type_counts: std::collections::HashMap<String, u64>,
    pub max_out_degree: u64,
}

impl StatsSummary {
    pub fn of(acc: &StatsAccum) -> Self {
        StatsSummary {
            n_node_versions: acc.n_node_versions,
            n_edge_versions: acc.n_edge_versions,
            vt_min: acc.vt_min,
            vt_max: acc.vt_max,
            rel_type_counts: acc.rel_type_counts.clone(),
            max_out_degree: acc.max_out_degree(),
        }
    }
}

/// One reading of [`NativeStore::memory_accounting`]: `(component, bytes)`
/// in a fixed order, counts and byte totals alike. A list rather than a map
/// so the order a receipt prints is the order that function declares, and so
/// the `current` and `peak` halves are positionally the same components.
pub type MemoryComponents = Vec<(&'static str, u64)>;

impl StatsAccum {
    /// Heap this accumulator owns, in bytes.
    ///
    /// `out_degree` is the part that scales: one bucket per *distinct source
    /// vertex*, held for the handle's life once `stats()` has built it. On a
    /// corpus whose entity count is linear in rows this is the accumulator,
    /// not the counts.
    pub fn heap_bytes(&self) -> u64 {
        let rel = self.rel_type_counts.capacity()
            * (std::mem::size_of::<String>() + std::mem::size_of::<u64>())
            + self
                .rel_type_counts
                .keys()
                .map(String::capacity)
                .sum::<usize>();
        let deg = self.out_degree.capacity()
            * (std::mem::size_of::<u32>() + std::mem::size_of::<u64>());
        (rel + deg) as u64
    }

    /// Fold in one edge version.
    pub fn add_edge(&mut self, vt_s: i64, vt_e: i64, rel_type: &str, src_id: u32) {
        self.n_edge_versions += 1;
        self.vt_min = Some(self.vt_min.map_or(vt_s, |m| m.min(vt_s)));
        // an open-ended interval contributes vt_s + 1, as DuckDB does
        let ve = if vt_e >= crate::OPEN_END { vt_s + 1 } else { vt_e };
        self.vt_max = Some(self.vt_max.map_or(ve, |m| m.max(ve)));
        *self.rel_type_counts.entry(rel_type.to_string()).or_default() += 1;
        *self.out_degree.entry(src_id).or_default() += 1;
    }

    pub fn max_out_degree(&self) -> u64 {
        self.out_degree.values().copied().max().unwrap_or(0)
    }
}

/// What `close_cache` holds: the generation the index is valid for,
/// how many close runs are folded into it, the last folded run's
/// `(file, sha)` — which is what proves the manifest still *starts* with what
/// was folded and has merely appended — and the index itself.
type CloseCacheEntry = (
    u64,
    usize,
    Option<(String, String)>,
    std::sync::Arc<CloseIndex>,
);

/// `segment id -> filename`, per lane, for one manifest generation.
pub(crate) struct SegmentFiles {
    pub(crate) edge: std::collections::HashMap<u64, String>,
    pub(crate) node: std::collections::HashMap<u64, String>,
}

impl SegmentFiles {
    pub(crate) fn of(&self, kind: RowKind) -> &std::collections::HashMap<u64, String> {
        match kind {
            RowKind::Edge => &self.edge,
            RowKind::Node => &self.node,
        }
    }
}

/// Physical locations of each logical identity, **per segment**, held under
/// a byte budget.
///
/// One segment's contribution is a [`SegmentPostings`]: the identity prefix
/// of every row, sorted, with the row's id beside it, plus a bit per row
/// saying whether that row was open when the segment was folded in. A hit is
/// only a candidate — the caller verifies the full identity — so a prefix
/// collision can never return the wrong version.
///
/// **Why per segment rather than one store-wide map.** The old shape was
/// three `HashMap<u64, Vec<(u64, u32)>>` over every row of the store. They
/// were built in full by the first point read (`read.rs::index_segments`
/// folded *every* not-yet-indexed segment) and never shrank: a 30M-row
/// build measured a persistent step from a 1.2–1.8 GB ingest floor to
/// 15.73 GB the moment the first correction landed, linear in row count and
/// so ≈ 500 GB at 1B. A
/// per-segment unit buys three things at once:
///
/// 1. **Eviction is O(1) and exact**, the same unit and LRU discipline
///    [`SegmentCache`] already uses — dropping a segment's entry drops
///    exactly that segment's bytes, where evicting a segment out of a
///    store-wide map would have meant walking every key.
/// 2. **A miss re-indexes one segment**, not the store.
/// 3. **The representation collapses.** A sorted `(keys, rows)` pair costs
///    12 B/row with no per-identity allocation at all, against ~150 B/row for
///    a `HashMap` slot plus a minimum-capacity `Vec` per identity — and the
///    open-version index becomes one bit per row instead of a third map.
///
/// Correctness never depends on residency: `read.rs`'s probes fold a segment
/// that is not resident on the spot and answer from it, exactly as
/// [`SegmentCache`] trades latency and not answers (`store.rs` 's
/// `OpenMode::Uncached` precedent: a posting for a
/// dropped segment is simply skipped and re-derived on the next lookup).
#[derive(Default)]
pub(crate) struct Postings {
    segments: std::collections::HashMap<u64, SegmentPostings>,
    /// Logical access clock, as in [`SegmentCache`]: bumped per touch,
    /// recorded per entry, O(n) victim scan at segment-count scale.
    clock: u64,
    total_bytes: u64,
    budget: Option<u64>,
    evictions: u64,
    /// Folds of a segment this handle had already folded once — the price of
    /// the budget, and the number that says whether it is set too low for the
    /// workload. Counted against `ever_folded` rather than against what is
    /// resident, so an eviction followed by a re-fold shows up as one.
    rebuilds: u64,
    /// Segment ids folded at least once by this handle. Bounded by segment
    /// count, not row count (8 B per live segment), and swept by
    /// [`Postings::retain_segments`] with everything else gc drops.
    ever_folded: std::collections::HashSet<u64>,
    /// Every fold ever performed, resident or transient, and the rows it
    /// walked. The instrument that attributed the 30M step to a call.
    folds: u64,
    rows_indexed: u64,
    /// Folds that were answered without being kept, because one segment's
    /// postings alone would not fit the budget.
    transient_folds: u64,
}

/// One segment's identity postings.
///
/// `keys[i]`/`rows[i]` is the identity prefix of row `rows[i]`, ordered by
/// key so a lookup is a `partition_point` and duplicates (prefix collisions,
/// and the several versions one identity has) are one contiguous run.
/// `open` is a bitset over *row ids*, set when the segment's sidecar said the
/// row was still believed at fold time.
#[derive(Default)]
pub(crate) struct SegmentPostings {
    keys: Vec<u64>,
    rows: Vec<u32>,
    open: Vec<u64>,
    bytes: u64,
    last_used: u64,
}

impl SegmentPostings {
    pub(crate) fn from_entries(mut entries: Vec<(u64, u32)>, open: Vec<u64>) -> Self {
        entries.sort_unstable();
        let mut keys = Vec::with_capacity(entries.len());
        let mut rows = Vec::with_capacity(entries.len());
        for (k, r) in entries {
            keys.push(k);
            rows.push(r);
        }
        let bytes = Self::accounted_bytes(keys.len(), open.len());
        Self {
            keys,
            rows,
            open,
            bytes,
            last_used: 0,
        }
    }

    /// What `n` rows of postings cost, used both to account a built entry and
    /// to decide *before* building whether the budget could ever hold it.
    pub(crate) fn accounted_bytes(rows: usize, open_words: usize) -> u64 {
        (rows as u64) * (std::mem::size_of::<u64>() + std::mem::size_of::<u32>()) as u64
            + (open_words as u64) * std::mem::size_of::<u64>() as u64
    }

    pub(crate) fn open_words_for(rows: u32) -> usize {
        (rows as usize).div_ceil(64)
    }

    /// Row ids of this segment whose identity prefix is `key`.
    pub(crate) fn rows_for(&self, key: u64) -> &[u32] {
        let lo = self.keys.partition_point(|&k| k < key);
        let hi = self.keys[lo..].partition_point(|&k| k == key) + lo;
        &self.rows[lo..hi]
    }

    pub(crate) fn is_open(&self, row: u32) -> bool {
        let (w, b) = ((row / 64) as usize, row % 64);
        self.open.get(w).is_some_and(|word| word & (1u64 << b) != 0)
    }

    /// Forget that `row` is open — the in-place prune `locate_open` performs
    /// when the committed close index shows a row has since been closed.
    /// Idempotent, and never widens the set.
    pub(crate) fn mark_closed(&mut self, row: u32) {
        let (w, b) = ((row / 64) as usize, row % 64);
        if let Some(word) = self.open.get_mut(w) {
            *word &= !(1u64 << b);
        }
    }

    /// Rows this segment contributed — the fold's own width, used to account
    /// it and to decide whether the budget could ever hold it.
    pub(crate) fn row_count(&self) -> u32 {
        self.keys.len() as u32
    }

}

impl Postings {
    pub(crate) fn new(budget: Option<u64>) -> Self {
        Self {
            budget,
            ..Default::default()
        }
    }

    pub(crate) fn set_budget(&mut self, budget: Option<u64>) {
        self.budget = budget;
        self.evict_to_budget(None);
    }

    pub(crate) fn get_mut(&mut self, seg: u64) -> Option<&mut SegmentPostings> {
        self.clock += 1;
        let clock = self.clock;
        let e = self.segments.get_mut(&seg)?;
        e.last_used = clock;
        Some(e)
    }

    /// Would the budget ever hold a segment of `rows` rows? `false` sends the
    /// caller down the transient path — fold, answer, drop — which is what
    /// keeps a 1-byte budget correct instead of thrashing on itself. Mirrors
    /// [`SegmentCache`]'s rule that the entry being inserted is never its own
    /// eviction victim.
    pub(crate) fn can_hold(&self, rows: u32) -> bool {
        match self.budget {
            None => true,
            Some(b) => {
                SegmentPostings::accounted_bytes(
                    rows as usize,
                    SegmentPostings::open_words_for(rows),
                ) <= b
            }
        }
    }

    pub(crate) fn insert(&mut self, seg: u64, mut sp: SegmentPostings) {
        self.clock += 1;
        sp.last_used = self.clock;
        let bytes = sp.bytes;
        if let Some(old) = self.segments.insert(seg, sp) {
            self.total_bytes -= old.bytes;
        }
        self.total_bytes += bytes;
        self.evict_to_budget(Some(seg));
    }

    fn evict_to_budget(&mut self, keep: Option<u64>) {
        let Some(budget) = self.budget else { return };
        while self.total_bytes > budget && self.segments.len() > 1 {
            let victim = self
                .segments
                .iter()
                .filter(|(id, _)| Some(**id) != keep)
                .min_by_key(|(_, e)| e.last_used)
                .map(|(id, _)| *id);
            match victim {
                Some(id) => {
                    let e = self.segments.remove(&id).expect("victim came from the map");
                    self.total_bytes -= e.bytes;
                    self.evictions += 1;
                }
                None => break,
            }
        }
    }

    /// Record one fold: its width, whether the budget let it be kept, and
    /// whether this handle had folded that segment before.
    pub(crate) fn note_fold(&mut self, seg: u64, rows: u32, kept: bool) {
        self.folds += 1;
        self.rows_indexed += rows as u64;
        if !self.ever_folded.insert(seg) {
            self.rebuilds += 1;
        }
        if !kept {
            self.transient_folds += 1;
        }
    }

    /// Drop every posting naming a segment gc has actually removed from
    /// disk. With the per-segment shape this is the whole prune: an
    /// entry *is* a segment, so there is no store-wide map left holding a
    /// dead segment's rows. Costs no information — the same rows are folded
    /// again under their new segment's id the next time something looks them
    /// up. `keep_ids` is the id set gc just proved is still referenced by a
    /// retained generation (`gc::gc`'s own `referenced`, translated from
    /// filenames via `segment_id_of`).
    pub(crate) fn retain_segments(&mut self, keep_ids: &std::collections::HashSet<u64>) {
        let total = &mut self.total_bytes;
        self.segments.retain(|id, e| {
            let kept = keep_ids.contains(id);
            if !kept {
                *total -= e.bytes;
            }
            kept
        });
        self.ever_folded.retain(|id| keep_ids.contains(id));
    }

    /// `(rows posted, rows still marked open)` for one identity prefix,
    /// summed over resident segments — the shape the regression tests
    /// assert on, now that a posting is a row of a segment rather than an
    /// entry in a store-wide map.
    #[cfg(test)]
    pub(crate) fn entries_for(&self, key: u64) -> (usize, usize) {
        let mut all = 0;
        let mut open = 0;
        for e in self.segments.values() {
            for &row in e.rows_for(key) {
                all += 1;
                if e.is_open(row) {
                    open += 1;
                }
            }
        }
        (all, open)
    }

    /// `(identity entries, resident segments, resident bytes, budget,
    /// evictions, rebuilds, folds, rows folded, transient folds)`.
    #[allow(clippy::type_complexity)]
    pub(crate) fn stats(&self) -> (usize, usize, u64, Option<u64>, u64, u64, u64, u64, u64) {
        (
            self.segments.values().map(|e| e.keys.len()).sum(),
            self.segments.len(),
            self.total_bytes,
            self.budget,
            self.evictions,
            self.rebuilds,
            self.folds,
            self.rows_indexed,
            self.transient_folds,
        )
    }
}

/// Per-row-kind probe counters for the identity filter.
#[derive(Default)]
pub(crate) struct FilterCounters {
    probes: std::sync::atomic::AtomicU64,
    positives: std::sync::atomic::AtomicU64,
    negatives: std::sync::atomic::AtomicU64,
    empty_positives: std::sync::atomic::AtomicU64,
}

/// Index of one row kind in the per-kind counter arrays. Written out rather
/// than `as usize` so a reordering of [`RowKind`]'s variants cannot silently
/// swap two lanes' receipts.
#[inline]
pub(crate) fn kind_slot(kind: RowKind) -> usize {
    match kind {
        RowKind::Edge => 0,
        RowKind::Node => 1,
    }
}

/// What one segment's filter says about one identity key.
///
/// `Absent` is not a third answer — it is the state before identity filters existed, where every
/// segment is a candidate — and it is kept distinct from `Positive` only so
/// that the in-situ ε measurement counts filter positives rather than
/// unfiltered segments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterVerdict {
    /// No filter for this segment; behave exactly as before.
    Absent,
    /// The segment may hold the key: fold and find out.
    Positive,
    /// **Exact**: the segment holds no stored row with this key.
    Negative,
}

/// This handle's mapped identity filters, under a byte ceiling.
///
/// Shaped on [`Postings`] deliberately: same LRU clock, same "the entry
/// being inserted is never its own eviction victim" rule, same
/// `retain_segments` sweep on gc. What differs is that a miss here is
/// cheap and never changes an answer — a filter that is not resident is
/// re-mapped, and a filter that does not exist leaves the probe exactly as
/// it would be without filters.
#[derive(Default)]
pub(crate) struct Filters {
    /// `None` means "looked, and there is no filter file for this segment"
    /// — remembered so a store without filters does not pay a failed open
    /// per segment per probe.
    segments: std::collections::HashMap<u64, Option<std::sync::Arc<crate::vidfilter::VidFilter>>>,
    clock: u64,
    last_used: std::collections::HashMap<u64, u64>,
    total_bytes: u64,
    budget: Option<u64>,
    evictions: u64,
    /// Filters mapped, probed and dropped because one alone would not fit
    /// the ceiling — the `can_hold` idiom, which is what keeps a one-byte
    /// ceiling correct rather than a thrash.
    transient: u64,
}

impl Filters {
    pub(crate) fn new(budget: Option<u64>) -> Self {
        Self {
            budget,
            ..Default::default()
        }
    }

    fn get(&mut self, seg: u64) -> Option<Option<std::sync::Arc<crate::vidfilter::VidFilter>>> {
        let hit = self.segments.get(&seg)?.clone();
        self.clock += 1;
        self.last_used.insert(seg, self.clock);
        Some(hit)
    }

    /// Would the ceiling ever hold a filter of `bytes` bytes?
    fn can_hold(&self, bytes: u64) -> bool {
        self.budget.is_none_or(|b| bytes <= b)
    }

    fn insert(
        &mut self,
        seg: u64,
        f: Option<std::sync::Arc<crate::vidfilter::VidFilter>>,
    ) -> bool {
        let bytes = f.as_ref().map_or(0, |v| v.extent_bytes());
        if !self.can_hold(bytes) {
            self.transient += 1;
            return false;
        }
        self.clock += 1;
        let clock = self.clock;
        if let Some(Some(old)) = self.segments.insert(seg, f) {
            self.total_bytes -= old.extent_bytes();
        }
        self.last_used.insert(seg, clock);
        self.total_bytes += bytes;
        self.evict_to_budget(Some(seg));
        true
    }

    fn evict_to_budget(&mut self, keep: Option<u64>) {
        let Some(budget) = self.budget else { return };
        while self.total_bytes > budget && self.segments.len() > 1 {
            let victim = self
                .segments
                .iter()
                .filter(|(id, f)| Some(**id) != keep && f.is_some())
                .min_by_key(|(id, _)| self.last_used.get(id).copied().unwrap_or(0))
                .map(|(id, _)| *id);
            match victim {
                Some(id) => {
                    if let Some(Some(f)) = self.segments.remove(&id) {
                        self.total_bytes -= f.extent_bytes();
                    }
                    self.last_used.remove(&id);
                    self.evictions += 1;
                }
                None => break,
            }
        }
    }

    /// Drop every filter naming a segment gc has removed from disk — the
    /// `nameidx::CACHE`-shaped leak this map exists not to be.
    pub(crate) fn retain_segments(&mut self, keep_ids: &std::collections::HashSet<u64>) {
        let total = &mut self.total_bytes;
        self.segments.retain(|id, f| {
            let kept = keep_ids.contains(id);
            if !kept {
                if let Some(v) = f {
                    *total -= v.extent_bytes();
                }
            }
            kept
        });
        self.last_used.retain(|id, _| keep_ids.contains(id));
    }

    /// `(resident filters, mapped bytes, budget, evictions, transient)`.
    fn stats(&self) -> (usize, u64, Option<u64>, u64, u64) {
        (
            self.segments.values().filter(|f| f.is_some()).count(),
            self.total_bytes,
            self.budget,
            self.evictions,
            self.transient,
        )
    }

    /// `(mapped bytes, heap bytes, resident filters)` — the class split
    /// `memory_accounting` reports. A filter is a file mapping once loaded;
    /// one whose write failed stays on the heap, and must be charged there.
    fn class_split(&self) -> (u64, u64, u64) {
        let (mut mapped, mut heap, mut n) = (0u64, 0u64, 0u64);
        for f in self.segments.values().flatten() {
            n += 1;
            if f.is_mapped() {
                mapped += f.extent_bytes();
            } else {
                heap += f.extent_bytes();
            }
        }
        (mapped, heap, n)
    }
}

/// Resolve the identity-postings byte budget.
///
/// `GNOMON_POSTINGS_BYTES` overrides in both directions and `0` means
/// unbounded, exactly as `GNOMON_SEGMENT_CACHE_BYTES` does for
/// [`cache_budget`]. The default is `min(RAM/8, POSTINGS_MAX_BYTES)` — see
/// that constant for why the ceiling is set where it is, and for why a
/// binding budget is a real cost rather than a free bound. Where physical
/// RAM cannot be read cheaply (anything but Linux) the ceiling alone
/// applies, so the bound is never simply absent.
pub(crate) fn postings_budget(env: Option<&str>, ram_bytes: Option<u64>) -> Option<u64> {
    if let Some(v) = env.and_then(|s| s.trim().parse::<u64>().ok()) {
        return if v == 0 { None } else { Some(v) };
    }
    Some(
        ram_bytes
            .map(|r| (r / 8).min(crate::defaults::POSTINGS_MAX_BYTES))
            .unwrap_or(crate::defaults::POSTINGS_MAX_BYTES),
    )
}

/// The store's open-segment cache, accounted in bytes.
///
/// Eviction unit: whole cached segments, least-recently-used first. The
/// entry being inserted is never the eviction victim — a single segment
/// larger than the whole budget still gets served (over budget transiently)
/// rather than thrashing on itself. All access happens under the same mutex
/// the unbounded map already took, so the read hot path gains no lock.
pub(crate) struct SegmentCache {
    entries: std::collections::HashMap<String, CacheEntry>,
    /// Logical access clock: bumped per touch, recorded per entry. Cheaper
    /// and simpler than a linked LRU list at segment-count scale (hundreds),
    /// where the O(n) victim scan is noise against the decode it replaces.
    clock: u64,
    total_bytes: u64,
    budget: Option<u64>,
    evictions: u64,
}

struct CacheEntry {
    seg: std::sync::Arc<crate::segment::Segment<MmapSource>>,
    bytes: u64,
    last_used: u64,
}

impl SegmentCache {
    fn new(budget: Option<u64>) -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            clock: 0,
            total_bytes: 0,
            budget,
            evictions: 0,
        }
    }

    /// What this cache charges for holding `seg`: **anonymous heap only**.
    ///
    /// `GNOMON_SEGMENT_CACHE_BYTES` bounds decoded bytes. The segment's file
    /// mapping is not charged to it and is not credited back on eviction,
    /// because those bytes are the store itself — file-backed pages the
    /// kernel manages, reclaimable under pressure on a disk-backed store and
    /// only pinned-*looking* on tmpfs, where they land in `RssShmem` and
    /// measure 1.0003x the store's segment bytes.
    ///
    /// Charging them was what produced the 1.64x overshoot at 1B: 30.79 GB
    /// of a 32 GB budget went on mapped bytes that were resident whether or
    /// not a segment was cached, leaving 21.8 GB of decoded columns on top
    /// and 52.6 GB of residency against a 32 GB setting. With decoded-only
    /// accounting the budget means what it says, and the mapped term is
    /// reported separately rather than mixed in.
    ///
    /// The parsed header is included because it is anonymous heap the
    /// `Segment` owns alone and `resident_bytes()` never counted at all.
    fn charge(&self, seg: &crate::segment::Segment<MmapSource>) -> u64 {
        seg.decoded_bytes() + seg.header_heap_bytes()
    }

    /// Drop least-recently-used entries until the effective budget holds.
    /// `keep` is never chosen as a victim — an entry just inserted is served
    /// over budget rather than evicted before it is used.
    fn evict_to_budget(&mut self, keep: Option<&str>) {
        let Some(budget) = self.budget else {
            return;
        };
        while self.total_bytes > budget && self.entries.len() > 1 {
            let victim = self
                .entries
                .iter()
                .filter(|(name, _)| Some(name.as_str()) != keep)
                .min_by_key(|(_, e)| e.last_used)
                .map(|(name, _)| name.clone());
            match victim {
                Some(name) => {
                    let e = self.entries.remove(&name).expect("victim came from the map");
                    self.total_bytes -= e.bytes;
                    self.evictions += 1;
                }
                None => break, // only the protected entry remains
            }
        }
    }

    fn get(&mut self, file: &str) -> Option<std::sync::Arc<crate::segment::Segment<MmapSource>>> {
        self.clock += 1;
        let clock = self.clock;
        self.entries.get_mut(file).map(|e| {
            e.last_used = clock;
            e.seg.clone()
        })
    }

    fn insert(&mut self, file: String, seg: std::sync::Arc<crate::segment::Segment<MmapSource>>) {
        let bytes = self.charge(&seg);
        self.clock += 1;
        if let Some(old) = self.entries.insert(
            file.clone(),
            CacheEntry {
                seg,
                bytes,
                last_used: self.clock,
            },
        ) {
            // two threads raced the same miss; the replaced entry is identical
            self.total_bytes -= old.bytes;
        }
        self.total_bytes += bytes;
        self.evict_to_budget(Some(&file));
    }

    fn retain_files(&mut self, keep: &std::collections::HashSet<String>) {
        let total = &mut self.total_bytes;
        self.entries.retain(|file, e| {
            let kept = keep.contains(file);
            if !kept {
                *total -= e.bytes;
            }
            kept
        });
    }

    fn set_budget(&mut self, budget: Option<u64>) {
        self.budget = budget;
        self.evict_to_budget(None);
    }

    fn stats(&self) -> (usize, u64, Option<u64>, u64) {
        (self.entries.len(), self.total_bytes, self.budget, self.evictions)
    }



    /// `(mapped, decoded, header_heap, rows)` over the live entries (memory
    /// accounting): the segment-cache half of the store's residency, split by
    /// what an eviction can actually reclaim.
    ///
    /// `mapped` + `decoded` is what [`Self::stats`] reports as accounted, but
    /// the budget treats the two alike and they do not behave alike: dropping
    /// an entry frees `decoded` (anonymous heap this segment alone owns) while
    /// `mapped` is returned only if nothing else holds the same mapping — and
    /// `pinned_sources` holds one for every segment of the open
    /// generation. `header_heap` is charged against no budget at all; see
    /// [`crate::segment::SegmentHeader::heap_bytes`].
    ///
    /// `rows` is the denominator the other three are read against: a
    /// component that scales per row and one that scales per segment are
    /// indistinguishable from a byte total alone, and under a binding budget
    /// the cached row count is not the store's row count.
    ///
    /// Walked rather than tracked incrementally: this is a receipt read at a
    /// progress line, not a hot path, and O(entries) is segment count.
    fn residency(&self) -> (u64, u64, u64, u64) {
        let mut mapped = 0u64;
        let mut decoded = 0u64;
        let mut header = 0u64;
        let mut rows = 0u64;
        for e in self.entries.values() {
            mapped += e.seg.mapped_bytes();
            decoded += e.seg.decoded_bytes();
            header += e.seg.header_heap_bytes();
            rows += u64::from(e.seg.header().rows);
        }
        (mapped, decoded, header, rows)
    }
}

/// Whether a segment open joins the session's decoded-segment cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenMode {
    /// Keep the decoded segment in the session cache — the right choice for a
    /// path that revisits the same segment across calls.
    Cached,
    /// Decode, use, drop. The right choice for a pass that touches every
    /// segment exactly once, where caching turns a one-segment working set
    /// into a store-sized one.
    Uncached,
}

/// A segment held either by the session cache or by the caller.
///
/// Exists only so one function body can serve both [`OpenMode`]s: the cached
/// open hands back the cache's `Arc`, the uncached open an owned `Segment`
/// the caller drops. Everything downstream reads through `Deref`, so the
/// row-materialising code is identical in both modes.
pub(crate) enum SegmentHandle {
    Cached(std::sync::Arc<crate::segment::Segment<MmapSource>>),
    Uncached(Box<crate::segment::Segment<MmapSource>>),
}

impl std::ops::Deref for SegmentHandle {
    type Target = crate::segment::Segment<MmapSource>;

    fn deref(&self) -> &Self::Target {
        match self {
            SegmentHandle::Cached(s) => s,
            SegmentHandle::Uncached(s) => s,
        }
    }
}

/// Resolve the pin-at-open ceiling: `GNOMON_PIN_MAX_SEGMENTS` if it
/// parses to a positive integer, otherwise `defaults::PIN_MAX_SEGMENTS`. A
/// tuning knob must never make a store fail to open, so garbage or zero is
/// treated as unset rather than as an error.
fn pin_max_segments() -> usize {
    std::env::var("GNOMON_PIN_MAX_SEGMENTS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(crate::defaults::PIN_MAX_SEGMENTS)
}

/// Resolve the segment-cache budget: the env override wins, otherwise half
/// of detected physical RAM capped at [`defaults::SEGMENT_CACHE_MAX_BYTES`],
/// otherwise unbounded.
///
/// The override is plain bytes; `0` means unbounded explicitly. Garbage is
/// treated as unset rather than as an error — a tuning knob must never make
/// a store fail to open. **The cap applies to the default only**: an explicit
/// `GNOMON_SEGMENT_CACHE_BYTES` above 32 GiB is still honoured verbatim, which
/// is what lets a job reproduce a pre-cap measurement exactly.
///
/// This is a behavior change. It binds only on
/// hosts with more than 64 GB of RAM: on a 16 GB laptop `RAM/2` is 8 GB and
/// nothing moves; on the 515 GB node of the 100M build record the default
/// falls from ~251 GB to 32 GiB.
pub(crate) fn cache_budget(env: Option<&str>, ram_bytes: Option<u64>) -> Option<u64> {
    if let Some(v) = env.and_then(|s| s.trim().parse::<u64>().ok()) {
        return if v == 0 { None } else { Some(v) };
    }
    ram_bytes.map(|r| (r / 2).min(crate::defaults::SEGMENT_CACHE_MAX_BYTES))
}

/// Total physical RAM, where the platform makes it cheap to ask (Linux
/// `/proc/meminfo`). `None` elsewhere — the cache is then unbounded by
/// default, which is exactly the behavior before the cap was added.
fn detected_ram_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// What a `verify` pass found. An empty `problems` list is the only
/// acceptable outcome for a healthy store.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerifyReport {
    pub generation: u64,
    /// On-disk manifest format this generation was read at: 3 is what this
    /// build writes (checkpoint/delta chain, `manifest_sha` a Merkle root);
    /// 1 and 2 open read-only.
    pub manifest_format: u32,
    /// Generation of the checkpoint the chain was replayed from, and how many
    /// deltas sat on top of it. `manifest_deltas == 0` means `CURRENT` names
    /// a checkpoint outright.
    pub manifest_checkpoint: u64,
    pub manifest_deltas: u64,
    pub segments_checked: u32,
    pub close_runs_checked: u32,
    pub rows: u64,
    pub closes: u64,
    pub dict_records: u32,
    /// Layout quality: `tt_s` runs summed over every live segment,
    /// and the worst single segment. `max_tt_s_runs == 1` per segment is what
    /// batch ingest writes; compaction re-sorts rows from all generations
    /// into global key order while each row keeps its origin `tt_s`, so it
    /// can leave this near the row count. Read cost per materialized row is
    /// logarithmic in it, and the whole-segment belief fast paths depend on
    /// it staying small, so it is reported rather than left to be inferred
    /// from a wall clock.
    pub tt_s_runs: u64,
    pub max_tt_s_runs: u32,
    /// Human-readable text for every *error* finding, in the order they were
    /// made. Kept alongside `findings` because it is what the CLI's prose
    /// report and every older caller already read; advisories deliberately
    /// stay out of it, so `problems.is_empty()` and "healthy" still mean the
    /// same thing they did.
    pub problems: Vec<String>,
    /// `"fast"` or `"full"` - which pass produced this report.
    pub mode: String,
    /// Every structured observation, advisories included. This is what the
    /// corruption tests match on.
    pub findings: Vec<Finding>,
    /// Full mode only: rows read out of live segments, of which
    /// `believed_rows` were still believed, spread over
    /// `identities_checked` identities.
    pub rows_walked: u64,
    pub believed_rows: u64,
    pub identities_checked: u64,
}

impl VerifyReport {
    pub fn is_healthy(&self) -> bool {
        self.problems.is_empty()
    }

    /// Record one finding. An error also lands in `problems`, which is what
    /// `is_healthy` and the prose report are built from; an advisory is
    /// listed and does not condemn the store.
    pub(crate) fn flag(&mut self, f: Finding) {
        if f.is_error() {
            self.problems.push(f.detail.clone());
        }
        self.findings.push(f);
    }

    /// Findings that make the store untrustworthy.
    pub fn errors(&self) -> impl Iterator<Item = &Finding> {
        self.findings.iter().filter(|f| f.is_error())
    }
}

/// What `upgrade_manifests` did. `upgraded == false` means the store was
/// already format 2 and nothing was written — the command is idempotent, so
/// running it twice is not an error.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpgradeReport {
    pub upgraded: bool,
    pub from_format: u32,
    pub generation: u64,
    pub manifest_sha: String,
}

// --------------------------------------------------------------------- //
// Per-segment TCSR: cache, build, and the neighbour primitive           //
// --------------------------------------------------------------------- //

/// Which endpoint(s) a traversal steps through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NeighborDir {
    Out,
    In,
    /// the union of both, merged into one `(vt_s, vid)`-ordered slice
    Both,
}

impl NeighborDir {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "out" => Ok(NeighborDir::Out),
            "in" => Ok(NeighborDir::In),
            "both" => Ok(NeighborDir::Both),
            other => Err(EngineError::invariant(format!(
                "direction must be 'out', 'in' or 'both', got {other:?}"
            ))),
        }
    }
}

/// Neighbour rows of one or more vertices, CSR-shaped.
///
/// The four value columns are exactly what `tcsr.neighbors` returns
/// (`gnomon/storage/tcsr.py`) in the same order, plus the physical addresses
/// the native TCSR column set already carries (`seg_id`, `seg_row`) so
/// `temporal_paths`' identity resolution through `edge_idents_at` is
/// unchanged.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NeighborRows {
    /// the *other* endpoint's dense id
    pub nbr: Vec<u32>,
    pub vt_s: Vec<i64>,
    pub vt_e: Vec<i64>,
    pub seg_id: Vec<u64>,
    pub seg_row: Vec<u32>,
    /// `us.len() + 1` offsets into the columns above
    pub offsets: Vec<u32>,
}

impl NeighborRows {
    pub fn len(&self) -> usize {
        self.nbr.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nbr.is_empty()
    }
}

/// One candidate row, carrying the sort key the traversal's output contract
/// is stated in.
struct Cand {
    vt_s: i64,
    vid: Id96,
    nbr: u32,
    vt_e: i64,
    seg_id: u64,
    seg_row: u32,
}

/// One segment a traversal will visit, held open for the call, with the
/// believed row ranges already decided for it.
/// Where one target segment's adjacency rows come from.
enum TargetAdj {
    /// The segment's per-segment temporal adjacency index.
    Index(std::sync::Arc<crate::tcsr::SegmentTcsr>),
    /// `GNOMON_TCSR=scan`: no index; see [`ScanAdjacency`].
    Scan(ScanAdjacency),
}

impl TargetAdj {
    /// The rows of this segment whose `d` endpoint is `u`, ascending.
    #[inline]
    fn slice(&self, d: crate::tcsr::Direction, u: u32) -> &[u32] {
        match self {
            TargetAdj::Index(i) => i.dir(d).slice(u),
            TargetAdj::Scan(s) => s.slice(d, u),
        }
    }
}

/// Adjacency found by scanning a segment's endpoint columns
/// (`GNOMON_TCSR=scan`): one pass per direction the call needs, keeping the
/// rows whose endpoint is in the call's frontier, bucketed per vertex in
/// ascending row order. That is exactly the index's slice: the index's
/// placement pass also walks rows in ascending order
/// (`ablation_tests::a_scanned_adjacency_bucket_is_the_index_slice`). A
/// frontier costs one scan per segment per call, which is how an engine
/// without an adjacency index expands a hop.
pub(crate) struct ScanAdjacency {
    out: HashMap<u32, Vec<u32>>,
    inn: HashMap<u32, Vec<u32>>,
}

impl ScanAdjacency {
    pub(crate) fn build(
        seg: &crate::segment::Segment<MmapSource>,
        us: &[u32],
        dirs: &[crate::tcsr::Direction],
    ) -> Result<Self> {
        let want: std::collections::HashSet<u32> = us.iter().copied().collect();
        let (mut out, mut inn) = (HashMap::new(), HashMap::new());
        let mut scanned = 0usize;
        for &d in dirs {
            let (col, map) = match d {
                crate::tcsr::Direction::Out => (seg.u32_column("src_id")?, &mut out),
                crate::tcsr::Direction::In => (seg.u32_column("dst_id")?, &mut inn),
            };
            scanned += col.len();
            for (r, v) in col.iter().enumerate() {
                if want.contains(v) {
                    map.entry(*v).or_insert_with(Vec::new).push(r as u32);
                }
            }
        }
        crate::vistouch::adj(scanned);
        Ok(Self { out, inn })
    }

    pub(crate) fn slice(&self, d: crate::tcsr::Direction, u: u32) -> &[u32] {
        let map = match d {
            crate::tcsr::Direction::Out => &self.out,
            crate::tcsr::Direction::In => &self.inn,
        };
        map.get(&u).map_or(&[], |v| v.as_slice())
    }
}

struct TcsrTarget {
    id: u64,
    seg: std::sync::Arc<crate::segment::Segment<MmapSource>>,
    adj: TargetAdj,
    /// Both halves of the belief predicate at this call's `as_of`, in the
    /// range domain. Ascending, disjoint and minimal, so
    /// membership is one binary search. Never empty — a segment whose ranges
    /// come back empty is dropped before it becomes a target — unless
    /// `per_row` is set.
    ranges: Vec<(usize, usize)>,
    /// Decide belief per adjacency row instead of building `ranges`
    /// for the whole segment (see [`traversal_tests_rows`]).
    per_row: bool,
    /// With `per_row`, test each adjacency row's own `tt_s` and
    /// effective `tt_e` (`GNOMON_RANGE_VISIBILITY=0`) rather than the
    /// window-first one-row range.
    row_test: bool,
}

/// Should this traversal call test belief per adjacency row rather
/// than build the segment's believed ranges?
///
/// The range build reads every close of the segment (and, when the call's
/// belief time splits the segment, every run or straddling code block);
/// a depth-first traversal makes one call per newly reached vertex, and a
/// fully compacted 10M store holds about a million closes, measured at 2-8
/// ms per call. The per-row test costs a few binary searches for each of
/// the `m` adjacency rows the call will actually look at. Both decide the
/// same rows, so this moves cost only.
fn traversal_tests_rows(m: usize, closes: usize, tt_items: usize) -> bool {
    #[cfg(test)]
    if let Some(forced) = FORCE_PER_ROW.with(|c| c.get()) {
        return forced;
    }
    let lg = |x: usize| (usize::BITS - x.leading_zeros()) as usize;
    m.saturating_mul(lg(closes) + lg(tt_items) + 4) < closes + tt_items
}

#[cfg(test)]
thread_local! {
    /// Tests pin the traversal's belief path, so both are exercised on the
    /// same fixture whatever the cost rule would pick.
    static FORCE_PER_ROW: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// Is row `r` believed at `as_of`? Exactly `in_ranges(r,
/// believed_ranges_closed(..))` for the whole segment: it *is* that
/// computation, over the one-row range `[r, r + 1)`, through the restricted
/// primitives whose equality with the clipped whole-segment answer is
/// pinned by `windowfirst_tests::exactness`.
#[allow(clippy::too_many_arguments)]
fn believed_row(
    tt: &crate::ttime::TtSView<'_>,
    bounds: (i64, i64),
    sidecar: crate::visibility::Sidecar<'_>,
    closes: &crate::visibility::CloseIndex,
    id: u64,
    rows: usize,
    as_of: i64,
    r: u32,
) -> bool {
    let (lo, hi) = (r as usize, r as usize + 1);
    let view = crate::visibility::close_view_in(sidecar, closes, id, lo, hi);
    !crate::scan::believed_ranges_closed_in(tt, bounds, as_of, rows, lo, hi, &view).is_empty()
}

/// Every column handle and lookup table of one segment, resolved **once per
/// call** rather than once per vertex.
///
/// This is the same lesson `ScanSet::select` records: re-resolving a column
/// by name, walking the `tt_s` runs, or rebuilding a `Vec` of allowed rel
/// codes per row costs more than the predicate itself. A traversal makes the
/// same mistake one level up if it does that work per frontier vertex.
struct TargetView<'a> {
    id: u64,
    adj: &'a TargetAdj,
    vt_s: &'a [i64],
    vt_e: Option<&'a [i64]>,
    src: &'a [u32],
    dst: &'a [u32],
    vid64: &'a [u64],
    vid_lo32: &'a [u32],
    rel_codes: Option<&'a [u16]>,
    rel_allowed: Option<Vec<bool>>,
    /// Borrowed from this segment's [`TcsrTarget`], where it was decided once
    /// for the whole call.
    ranges: &'a [(usize, usize)],
    /// Set when this segment's belief is decided per adjacency row.
    per_row: Option<PerRow<'a>>,
}

/// What [`believed_row`] needs for one segment, resolved once per call.
struct PerRow<'a> {
    tt: crate::ttime::TtSView<'a>,
    bounds: (i64, i64),
    sidecar: crate::visibility::Sidecar<'a>,
    rows: usize,
    /// The plain per-row test rather than [`believed_row`].
    exact: bool,
}

/// Is row `r` believed? A binary search over a list whose length is "one entry
/// per verdict change" — typically 1 for a current-belief traversal over
/// an uncorrected segment, i.e. a single bounds check.
#[inline]
fn in_ranges(r: u32, ranges: &[(usize, usize)]) -> bool {
    let r = r as usize;
    let i = ranges.partition_point(|(_, re)| *re <= r);
    i < ranges.len() && ranges[i].0 <= r
}

impl NativeStore {
    /// The TCSR index of one segment file: from this handle's cache, from
    /// `idx/tcsr/`, or built from the segment's endpoint columns.
    ///
    /// The stamp is the segment's own `body_sha`, so an index file can never
    /// be used against bytes it was not computed from — including the store
    /// restored from an older backup. Every other failure (missing,
    /// truncated, bad CRC, foreign id) also falls through to a rebuild:
    /// An index is never authoritative.
    ///
    /// A failure to *write* the rebuilt index is swallowed. The store
    /// answers the query and rebuilds next time — the `tcsr.py:131–135` rule.
    /// A read-only handle never writes it: the index it built is held
    /// in this handle's cache only, which is where a loaded one lives too.
    pub fn tcsr_index(&self, file: &str) -> Result<std::sync::Arc<crate::tcsr::SegmentTcsr>> {
        use std::sync::atomic::Ordering::Relaxed;

        let id = segment_id_of(file);
        if let Some(hit) = self
            .tcsr_cache
            .lock()
            .expect("tcsr-cache mutex poisoned")
            .get(&id)
        {
            return Ok(hit.clone());
        }
        let path = self.root.join(file);
        let sha = crate::tcsr::segment_body_sha(&path)?;
        let idx_path = crate::tcsr::index_path(&self.root, id);
        let idx = match crate::tcsr::SegmentTcsr::load(&idx_path, id, &sha) {
            Some(t) => {
                self.tcsr_loaded.fetch_add(1, Relaxed);
                std::sync::Arc::new(t)
            }
            None => {
                let seg = self.open_segment(file)?;
                let built = crate::tcsr::SegmentTcsr::build(
                    id,
                    seg.u32_column("src_id")?,
                    seg.u32_column("dst_id")?,
                    &sha,
                )?;
                if !self.read_only {
                    let _ = built.write(&idx_path);
                }
                self.tcsr_built.fetch_add(1, Relaxed);
                std::sync::Arc::new(built)
            }
        };
        self.tcsr_cache
            .lock()
            .expect("tcsr-cache mutex poisoned")
            .insert(id, idx.clone());
        Ok(idx)
    }

    /// `(segments cached, built, loaded, resident bytes, rows indexed,
    /// active vertices)` for this handle.
    ///
    /// `built` and `loaded` are the counters the index tests read: a commit must not
    /// move `built` for a segment that already had an index.
    ///
    /// The last two are what makes the memory model — "TCSR memory within 5 % of
    /// `4·(|V| + 1 + n)·2` bytes" — scorable at all. `resident_bytes` alone
    /// gives the measured side of that comparison and nothing for the
    /// predicted side: `n` is the number of rows actually indexed (not the
    /// store's row count — an unbelieved or unvisited segment has no index)
    /// and `|V|` the active vertices, summed over **both directions** of
    /// every cached segment. With `segments_cached` as `S`, the forecast is
    ///
    /// ```text
    /// 4 * (active_vertices + 2 * S + 2 * rows_indexed)
    /// ```
    ///
    /// — `+2S` because each direction's offsets array carries one terminator,
    /// and `2 * rows_indexed` because both directions permute every row. A
    /// dense direction reports its id-space width as its active count, which
    /// is exactly what it allocates (`DirIndex::m`).
    pub fn tcsr_stats(&self) -> (usize, u64, u64, u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        let cache = self.tcsr_cache.lock().expect("tcsr-cache mutex poisoned");
        let bytes = cache.values().map(|t| t.resident_bytes()).sum();
        let rows = cache.values().map(|t| t.rows() as u64).sum();
        let active: u64 = cache
            .values()
            .map(|t| {
                t.dir(crate::tcsr::Direction::Out).m() as u64
                    + t.dir(crate::tcsr::Direction::In).m() as u64
            })
            .sum();
        (
            cache.len(),
            self.tcsr_built.load(Relaxed),
            self.tcsr_loaded.load(Relaxed),
            bytes,
            rows,
            active,
        )
    }

    /// The same quantities per cached segment, ordered by segment id:
    /// `(seg_id, rows, out active, in active, resident bytes, out dense,
    /// in dense)`.
    ///
    /// The aggregate above hides the one thing that can make that model miss for a
    /// reason that is not a bug: a direction elected *dense*
    /// (`TCSR_DENSE_THRESHOLD`) allocates an offsets entry per vertex id in
    /// the segment's whole id space, not per vertex that appears, so its
    /// `|V|` is the id-space width. Per segment, with the election visible,
    /// a miss can be attributed rather than merely observed.
    pub fn tcsr_segment_stats(&self) -> Vec<(u64, u32, u32, u32, u64, bool, bool)> {
        let cache = self.tcsr_cache.lock().expect("tcsr-cache mutex poisoned");
        let mut out: Vec<_> = cache
            .values()
            .map(|t| {
                let o = t.dir(crate::tcsr::Direction::Out);
                let i = t.dir(crate::tcsr::Direction::In);
                (
                    t.seg_id(),
                    t.rows(),
                    o.m(),
                    i.m(),
                    t.resident_bytes(),
                    o.is_dense(),
                    i.is_dense(),
                )
            })
            .collect();
        out.sort_by_key(|r| r.0);
        out
    }

    /// Drop this handle's in-memory indexes (the `idx/` files stay).
    ///
    /// The residency probe needs a "nothing resident" condition it can
    /// return to without reopening the store, which is exactly the shape
    /// `del adapter._tcsr` gives the Python index.
    pub fn release_tcsr(&self) {
        self.tcsr_cache
            .lock()
            .expect("tcsr-cache mutex poisoned")
            .clear();
    }

    /// Every edge segment the current manifest names, with its id.
    fn tcsr_edge_files(&self) -> Vec<(String, u64)> {
        let m = self.manifest();
        m.edge_lanes
            .event
            .iter()
            .chain(m.edge_lanes.interval.iter())
            .map(|e| (e.file.clone(), segment_id_of(&e.file)))
            .collect()
    }

    /// Pre-build the TCSR index of every edge segment this generation names
    /// (`gnomon store build-indexes --tcsr`, and `compact(build_indexes=True)`).
    ///
    /// Returns `(segments, built, loaded)` for this call — the difference is
    /// what the tests assert on: after a write batch only the new segments can
    /// be in `built`.
    pub fn build_tcsr_indexes(&self) -> Result<(usize, u64, u64)> {
        use std::sync::atomic::Ordering::Relaxed;
        let before = (self.tcsr_built.load(Relaxed), self.tcsr_loaded.load(Relaxed));
        let files = self.tcsr_edge_files();
        for (file, _) in &files {
            self.tcsr_index(file)?;
        }
        Ok((
            files.len(),
            self.tcsr_built.load(Relaxed) - before.0,
            self.tcsr_loaded.load(Relaxed) - before.1,
        ))
    }

    // ------------------------------------------------------------------ //
    // The per-segment identity filter                                     //
    // ------------------------------------------------------------------ //

    /// `SegmentEntry.sha` for one segment of either kind — the filter's
    /// stamp, read off the manifest rather than the file, so an eager build
    /// over hundreds of segments costs no extra I/O. It is the
    /// segment footer's `body_sha`; an entry written by an older engine carries
    /// `""`, which stamps nothing beyond "a legacy entry" (see
    /// `SegmentEntry::sha`).
    ///
    /// The generalisation of `read.rs::node_segment_sha`, originally written for
    /// the name index and which this needed for edges too.
    pub(crate) fn segment_sha_of(&self, kind: RowKind, id: u64) -> Option<String> {
        self.manifest()
            .segments_of(kind)
            .find(|e| segment_id_of(&e.file) == id)
            .map(|e| e.sha.clone())
    }

    /// This handle's filter for one segment, mapping it if it is on disk and
    /// not already resident.
    ///
    /// `None` means "there is no filter for this segment", which is the
    /// case before identity filters existed and is always correct. The absence is remembered in the
    /// map, because otherwise a store that has never built filters would pay
    /// one failed `open` per segment per probe.
    fn vid_filter(
        &self,
        kind: RowKind,
        seg_id: u64,
        _file: &str,
    ) -> Option<std::sync::Arc<crate::vidfilter::VidFilter>> {
        use std::sync::atomic::Ordering::Relaxed;
        {
            let mut m = self.vid_filters.lock().expect("vid-filter mutex poisoned");
            if let Some(hit) = m.get(seg_id) {
                return hit;
            }
        }
        // map outside the lock: it is a syscall, and holding the mutex
        // across it serialises every reader behind one segment
        let sha = self.segment_sha_of(kind, seg_id);
        let loaded = sha.and_then(|sha| {
            crate::vidfilter::VidFilter::load(
                &crate::vidfilter::filter_path(&self.root, seg_id),
                seg_id,
                kind,
                &sha,
            )
        });
        let arc = loaded.map(std::sync::Arc::new);
        if arc.is_some() {
            self.filter_loaded.fetch_add(1, Relaxed);
        }
        let mut m = self.vid_filters.lock().expect("vid-filter mutex poisoned");
        // `insert` refuses a filter the ceiling could never hold; the probe
        // below still uses the one it just mapped, then drops it — the
        // map-probe-drop idiom `Postings::can_hold` establishes, and what
        // keeps a one-byte ceiling correct rather than a thrash
        m.insert(seg_id, arc.clone());
        arc
    }

    /// Ask one segment's filter about one identity key, counting the answer.
    ///
    /// The whole of the probe integration: a [`FilterVerdict::Negative`] is
    /// exact and the caller skips the segment without opening its postings;
    /// anything else leaves the caller's loop as it was.
    pub(crate) fn filter_verdict(
        &self,
        kind: RowKind,
        seg_id: u64,
        file: &str,
        key: u64,
    ) -> FilterVerdict {
        use std::sync::atomic::Ordering::Relaxed;
        // `GNOMON_VIDFILTER=ignore` consults no filter: every segment is
        // asked, exactly as on a store that has none
        if !self.designs.identity_filter {
            return FilterVerdict::Absent;
        }
        let Some(f) = self.vid_filter(kind, seg_id, file) else {
            return FilterVerdict::Absent;
        };
        let c = &self.filter_counters[kind_slot(kind)];
        c.probes.fetch_add(1, Relaxed);
        if f.may_contain(key) {
            c.positives.fetch_add(1, Relaxed);
            FilterVerdict::Positive
        } else {
            c.negatives.fetch_add(1, Relaxed);
            FilterVerdict::Negative
        }
    }

    /// Record that a filter positive turned out to hold no row of the key —
    /// a false positive by definition, which is how ε is measured in situ.
    /// Ignored for a segment that had no filter.
    pub(crate) fn note_filter_empty_positive(&self, kind: RowKind, verdict: FilterVerdict) {
        if verdict == FilterVerdict::Positive {
            self.filter_counters[kind_slot(kind)]
                .empty_positives
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// `(probes, positives, negatives, empty positives)` for one row kind's
    /// identity filter — the mechanism receipt, reported beside that kind's
    /// fold counters in `postings_index_stats`.
    pub fn filter_probe_stats(&self, kind: RowKind) -> (u64, u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        let c = &self.filter_counters[kind_slot(kind)];
        (
            c.probes.load(Relaxed),
            c.positives.load(Relaxed),
            c.negatives.load(Relaxed),
            c.empty_positives.load(Relaxed),
        )
    }

    /// Build, write and adopt one segment's filter from keys the caller has
    /// already derived. The **one** `load_or_build` every path goes through
    /// — seal, `build_vid_filters`, and the lazy build path inside `fold_segment`
    /// — so a pre-built filter and a lazily built one are the same bytes and
    /// there is no second place for the two to diverge (a property also
    /// insisted on for `build_tcsr_indexes`).
    ///
    /// Swallows every failure. A store that cannot take the write must still
    /// answer queries; it simply stays unfiltered.
    ///
    /// On a read-only handle nothing is written: the filter is adopted
    /// as the owned bytes it was built from — the path a failed write
    /// already took — so it serves this handle, is charged as
    /// `vid_filter_heap`, and is rebuilt by the next fold if the ceiling
    /// evicts it. The lazy build path used to write here from read-only readers.
    pub(crate) fn store_vid_filter_from_keys(
        &self,
        kind: RowKind,
        file: &str,
        rows: u32,
        keys: &[u64],
        lazy: bool,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        if !crate::vidfilter::build_at_seal_enabled() {
            return;
        }
        let seg_id = segment_id_of(file);
        {
            // already have one: nothing to do, and in particular do not
            // rewrite a file that is already correct
            let mut m = self.vid_filters.lock().expect("vid-filter mutex poisoned");
            if let Some(Some(_)) = m.get(seg_id) {
                return;
            }
        }
        let Some(sha) = self.segment_sha_of(kind, seg_id) else {
            return;
        };
        // Construct / write / re-map timed for the eager builds only
        // (seal, compaction, settle); the lazy build is a fold's side effect
        let t = ingest_phase::Timer::start();
        let f = crate::vidfilter::VidFilter::build(seg_id, kind, &sha, rows, keys.to_vec());
        if !lazy {
            t.stop(ingest_phase::G::VfConstructNs);
            ingest_phase::add(ingest_phase::G::VfBuilt, 1);
            ingest_phase::add(ingest_phase::G::VfKeys, keys.len() as u64);
        }
        let path = crate::vidfilter::filter_path(&self.root, seg_id);
        let t = ingest_phase::Timer::start();
        let written = !self.read_only && f.write(&path).is_ok();
        if !lazy {
            t.stop(ingest_phase::G::VfWriteNs);
        }
        if lazy {
            self.filter_lazy_built.fetch_add(1, Relaxed);
        } else {
            self.filter_built.fetch_add(1, Relaxed);
        }
        // Keep the **mapping**, not the bytes just built. A freshly built
        // filter owns its serialized extent on the heap; holding that would
        // make every seal-built filter anonymous memory for the life of the
        // handle, which is the opposite of the property the filter is chosen
        // for (file-backed, reclaimable, reported under `file_mapped_total`).
        // Re-mapping the file just written costs one `mmap` and makes the
        // accounting claim true for every build path, not only for filters loaded
        // by a later handle. If the write failed there is nothing to map,
        // and the owned bytes are kept: correct, merely heap.
        let t = ingest_phase::Timer::start();
        let adopt = if written {
            crate::vidfilter::VidFilter::load(&path, seg_id, kind, &sha).unwrap_or(f)
        } else {
            f
        };
        if !lazy {
            t.stop(ingest_phase::G::VfRemapNs);
        }
        self.vid_filters
            .lock()
            .expect("vid-filter mutex poisoned")
            .insert(seg_id, Some(std::sync::Arc::new(adopt)));
    }

    /// Segment files `next` names that the live generation does not — the
    /// ones a publish is about to create. Compaction and settle both reach
    /// the seal-time build through this, so a rewritten segment gets its
    /// filter on exactly the same rule a freshly staged one does.
    fn newly_named_segments(&self, next: &Manifest) -> Vec<(RowKind, String)> {
        let mut out = Vec::new();
        for kind in [RowKind::Edge, RowKind::Node] {
            let have: std::collections::HashSet<String> = self
                .manifest
                .segments_of(kind)
                .map(|e| e.file.clone())
                .collect();
            for e in next.segments_of(kind) {
                if !have.contains(&e.file) {
                    out.push((kind, e.file.clone()));
                }
            }
        }
        out
    }

    /// Build the identity filter of each segment a publish has just created.
    ///
    /// **The one discrepancy against the design worth stating.** Design
    /// plans the build from the rows the writer still holds, plumbing a
    /// `Fn(u32) -> Result<String>` uid closure into `staging::seal` and
    /// `merge::ChunkWriter`, on the argument that "no I/O is added". This
    /// reads the segment back instead — it has just been written and
    /// fsynced, so it is in page cache — and derives the keys through
    /// `read::identity_keys`, the same function the fold and the eager build
    /// use.
    ///
    /// The trade is one extra column decode per sealed segment against two
    /// writers’ signatures, two call sites’ plumbing, and — the part that
    /// decided it — a second place for the key derivation to live. "The
    /// filter is built over exactly the key the probe uses" is the
    /// property a false negative would violate, and it is far
    /// easier to hold as "there is one function" than as "two functions
    /// agree". The decode is measured rather than assumed: it is
    /// `vidfilter_us` in the commit phases, and the ≤ 1.08× build-wall gate
    /// is scored on it.
    ///
    /// Every failure is swallowed. A store that cannot open a segment it has
    /// just written has a real problem, but it is not this sidecar’s to
    /// report: the generation is already published, and an unfiltered
    /// segment is simply the behaviour from before identity filters existed.
    pub(crate) fn build_filters_for(&self, new: &[(RowKind, String)]) {
        if new.is_empty() || !crate::vidfilter::build_at_seal_enabled() {
            return;
        }
        // Its own ingest-timer context: this runs inside a commit's `Seal`
        // guard and a compactor's `Compact` guard, and the segment opens
        // below must not be charged to either (see `Ctx::Vidfilter`).
        let _ctx = ingest_phase::CtxGuard::enter(ingest_phase::Ctx::Vidfilter);
        for (kind, file) in new {
            let Ok(seg) = self.open_segment_uncached(file) else {
                continue;
            };
            let t = ingest_phase::Timer::start();
            let Ok(keys) = self.identity_keys(*kind, &seg) else {
                continue;
            };
            t.stop(ingest_phase::G::VfKeysNs);
            self.store_vid_filter_from_keys(*kind, file, seg.rows(), &keys, false);
        }
    }

    /// Pre-build the identity filter of every segment this generation names,
    /// both kinds (`gnomon store build-indexes --vidfilter`, settle, and
    /// `compact(build_indexes=True)`).
    ///
    /// **Eager, and that is the one place the identity filter departs from the TCSR and
    /// name-index precedent.** `adapter.py` states the project's default —
    /// indexes are built lazily at first use, because taxing the write path
    /// is the wrong trade — and for those two it is right. It is wrong here:
    /// The 1B correction ramp was measured as *one operation*, so the
    /// first use **is** the entire cost and a lazily built filter would be
    /// written during a whole-store pass that has already happened.
    ///
    /// Returns `(segments, built, loaded)` for this call.
    pub fn build_vid_filters(&self) -> Result<(usize, u64, u64)> {
        use std::sync::atomic::Ordering::Relaxed;
        let before = (
            self.filter_built.load(Relaxed),
            self.filter_loaded.load(Relaxed),
        );
        let mut n = 0usize;
        for kind in [RowKind::Edge, RowKind::Node] {
            let files: Vec<String> = self
                .manifest()
                .segments_of(kind)
                .map(|e| e.file.clone())
                .collect();
            for file in files {
                n += 1;
                let seg_id = segment_id_of(&file);
                // a filter already on disk is loaded, not rebuilt — the same
                // rule as the TCSR index: a commit must not move `built` for a segment that
                // already had one
                if self.vid_filter(kind, seg_id, &file).is_some() {
                    continue;
                }
                let seg = self.open_segment_uncached(&file)?;
                let keys = self.identity_keys(kind, &seg)?;
                self.store_vid_filter_from_keys(kind, &file, seg.rows(), &keys, false);
            }
        }
        Ok((
            n,
            self.filter_built.load(Relaxed) - before.0,
            self.filter_loaded.load(Relaxed) - before.1,
        ))
    }

    /// `(segments, built, lazy_built, loaded, mapped bytes, budget,
    /// evictions, transient, probes, positives, negatives, empty
    /// positives)` for this handle's identity filters.
    ///
    /// `folds ÷ probes` — the mechanism gate — is this call's `probes`
    /// against `postings_index_stats`' `folds`. `empty_positives ÷
    /// positives` measures ε in situ. `mapped bytes` belongs to
    /// `file_mapped_total`, never to `anonymous_total`: the filter is a file
    /// mapping, which is the property that makes it reclaimable where the
    /// postings heap it prunes is not.
    #[allow(clippy::type_complexity)]
    pub fn vidfilter_stats(
        &self,
    ) -> (
        usize,
        u64,
        u64,
        u64,
        u64,
        Option<u64>,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        let (segments, bytes, budget, evictions, transient) = self
            .vid_filters
            .lock()
            .expect("vid-filter mutex poisoned")
            .stats();
        let mut probe = (0u64, 0u64, 0u64, 0u64);
        for kind in [RowKind::Edge, RowKind::Node] {
            let k = self.filter_probe_stats(kind);
            probe = (probe.0 + k.0, probe.1 + k.1, probe.2 + k.2, probe.3 + k.3);
        }
        (
            segments,
            self.filter_built.load(Relaxed),
            self.filter_lazy_built.load(Relaxed),
            self.filter_loaded.load(Relaxed),
            bytes,
            budget,
            evictions,
            transient,
            probe.0,
            probe.1,
            probe.2,
            probe.3,
        )
    }

    /// Override the identity-filter byte ceiling for this handle (`None` =
    /// unbounded), for the same reason [`Self::set_segment_cache_budget`]
    /// exists: a sweep must be able to change it without mutating the
    /// process environment, which races other threads.
    pub fn set_vidfilter_budget(&self, budget: Option<u64>) {
        let mut m = self.vid_filters.lock().expect("vid-filter mutex poisoned");
        m.budget = budget;
        m.evict_to_budget(None);
    }

    /// Drop this handle's mapped filters (the `idx/vidfilter/` files stay).
    /// The residency probe's "nothing resident" condition, as
    /// [`Self::release_tcsr`] is for the TCSR index.
    pub fn release_vid_filters(&self) {
        let mut m = self.vid_filters.lock().expect("vid-filter mutex poisoned");
        *m = Filters::new(m.budget);
    }

    /// Neighbour rows of one vertex.
    pub fn neighbors_of(
        &self,
        u: u32,
        direction: NeighborDir,
        as_of_tt: i64,
        vt_min: Option<i64>,
        vt_max: Option<i64>,
        rel_types: Option<&[String]>,
    ) -> Result<NeighborRows> {
        self.neighbors_batch(&[u], direction, as_of_tt, vt_min, vt_max, rel_types)
    }

    /// The adjacency source for one traversal target: the segment's
    /// index, or under `GNOMON_TCSR=scan` a scan of its endpoint columns for
    /// the call's frontier.
    fn target_adjacency(
        &self,
        file: &str,
        seg: &crate::segment::Segment<MmapSource>,
        us: &[u32],
        dirs: &[crate::tcsr::Direction],
    ) -> Result<TargetAdj> {
        if self.designs.adjacency_index {
            Ok(TargetAdj::Index(self.tcsr_index(file)?))
        } else {
            Ok(TargetAdj::Scan(ScanAdjacency::build(seg, us, dirs)?))
        }
    }

    /// Neighbour rows of a whole frontier, CSR-shaped.
    ///
    /// The per-segment work — opening columns, resolving the rel-code
    /// allow-list, and computing the believed row ranges — is done
    /// **once per call**, not once per vertex, which is the entire argument
    /// for a batched entry point: a frontier expansion amortizes it.
    ///
    /// Output order is the columnar scan's own `(vt_s, vid)`, so a slice is
    /// interchangeable with `TemporalCSR.neighbors`' — which is grouped by a
    /// stable argsort over a scan already in that order. `Both` concatenates
    /// out before in and then sorts stably, reproducing
    /// `adjacency.py`'s `np.lexsort` byte for byte.
    pub fn neighbors_batch(
        &self,
        us: &[u32],
        direction: NeighborDir,
        as_of_tt: i64,
        vt_min: Option<i64>,
        vt_max: Option<i64>,
        rel_types: Option<&[String]>,
    ) -> Result<NeighborRows> {
        self.assert_full_belief(as_of_tt)?;
        let as_of = crate::clamp_tt(as_of_tt);
        let closes = self.close_index()?;
        let dirs: &[crate::tcsr::Direction] = match direction {
            NeighborDir::Out => &[crate::tcsr::Direction::Out],
            NeighborDir::In => &[crate::tcsr::Direction::In],
            NeighborDir::Both => &[crate::tcsr::Direction::Out, crate::tcsr::Direction::In],
        };

        let mut targets: Vec<TcsrTarget> = Vec::new();
        for (file, id) in self.tcsr_edge_files() {
            let seg = self.open_segment(&file)?;
            if seg.rows() == 0 {
                continue;
            }
            // segment-level prune, the cheap half of `ScanSet::prunes`: a
            // segment nothing believed yet at `as_of`, or one entirely
            // outside the valid-time window, cannot contribute a row.
            //
            // `GNOMON_SEGMENT_PRUNE=0` keeps every segment; the belief,
            // valid-time and relation tests below then reject its rows.
            let h = seg.header();
            if self.designs.segment_prune {
                let rel_miss = rel_types.is_some_and(|rels| {
                    !h.rel_types.is_empty() && !h.rel_types.iter().any(|r| rels.contains(r))
                });
                if seg.tt_s_bounds().0 > as_of
                    || vt_min.is_some_and(|t| h.vt_e_max <= t)
                    || vt_max.is_some_and(|t| h.vt_min >= t)
                    || rel_miss
                {
                    crate::vistouch::pruned(1);
                    continue;
                }
            }
            // `GNOMON_RANGE_VISIBILITY=0` builds no ranges at all; each
            // adjacency row is tested on its own in the loop below.
            if !self.designs.range_visibility {
                let adj = self.target_adjacency(&file, &seg, us, dirs)?;
                targets.push(TcsrTarget {
                    id,
                    seg,
                    adj,
                    ranges: Vec::new(),
                    per_row: true,
                    row_test: true,
                });
                continue;
            }
            // Belief, once per segment per call — not once per
            // vertex and not once per row. `close_view` resolves the sidecar
            // and the generation's close runs into one sorted list with the
            // precedence rules fix, and `believed_ranges_closed` subtracts
            // the rows a close has ended from the `tt_s <= as_of` verdict
            // ranges. What is left has one entry per *verdict change*, so a
            // frontier expansion over many vertices amortizes the whole
            // thing and each row costs one binary search.
            //
            // Ahead of `tcsr_index` deliberately: a segment nothing believes
            // at this `as_of` never has its index loaded or built at all.
            //
            // Unless the segment carries closes or its `tt_s` is split
            // by `as_of`. Then the whole-segment build reads all of that on
            // every call, and the index is loaded first to count the
            // adjacency rows this call will test (`m`): when testing those
            // rows one by one is cheaper (`traversal_tests_rows`), belief is
            // decided per row below and no ranges are built.
            let rows = seg.rows() as usize;
            let tt = seg.tt_s_view()?;
            let bounds = seg.tt_s_bounds();
            let k = seg.sidecar().slice().len() + closes.segment_close_count(id);
            let tt_items = if bounds.1 <= as_of {
                0
            } else if tt.is_dictionary() {
                rows
            } else {
                tt.runs().len()
            };
            // `GNOMON_WINDOW_FIRST=0` skips the per-row alternative and
            // runs the whole-segment path below (ranges first, index only when
            // something is believed).
            if self.designs.window_first && k + tt_items > 0 {
                let adj = self.target_adjacency(&file, &seg, us, dirs)?;
                let m: usize = us
                    .iter()
                    .map(|&u| dirs.iter().map(|&d| adj.slice(d, u).len()).sum::<usize>())
                    .sum();
                if traversal_tests_rows(m, k, tt_items) {
                    targets.push(TcsrTarget {
                        id,
                        seg,
                        adj,
                        ranges: Vec::new(),
                        per_row: true,
                        row_test: false,
                    });
                    continue;
                }
                let view = crate::visibility::close_view(seg.sidecar(), &closes, id);
                let ranges = crate::scan::believed_ranges_closed(&tt, bounds, as_of, rows, &view);
                if ranges.is_empty() {
                    continue;
                }
                targets.push(TcsrTarget {
                    id,
                    seg,
                    adj,
                    ranges,
                    per_row: false,
                    row_test: false,
                });
                continue;
            }
            let view = crate::visibility::close_view(seg.sidecar(), &closes, id);
            let ranges = crate::scan::believed_ranges_closed(&tt, bounds, as_of, rows, &view);
            if ranges.is_empty() {
                continue;
            }
            let adj = self.target_adjacency(&file, &seg, us, dirs)?;
            targets.push(TcsrTarget {
                id,
                seg,
                adj,
                ranges,
                per_row: false,
                row_test: false,
            });
        }

        let mut views: Vec<TargetView<'_>> = Vec::with_capacity(targets.len());
        for t in &targets {
            let seg = t.seg.as_ref();
            let h = seg.header();
            let vt_s = seg.i64_column("vt_s")?;
            views.push(TargetView {
                id: t.id,
                adj: &t.adj,
                vt_s,
                vt_e: if h.vt_e_elided {
                    None
                } else {
                    Some(seg.i64_column("vt_e")?)
                },
                src: seg.u32_column("src_id")?,
                dst: seg.u32_column("dst_id")?,
                vid64: seg.u64_column("vid64")?,
                vid_lo32: seg.u32_column("vid_lo32")?,
                rel_codes: seg.u16_column("rel_code").ok(),
                rel_allowed: rel_types
                    .map(|names| h.rel_types.iter().map(|r| names.contains(r)).collect()),
                ranges: t.ranges.as_slice(),
                per_row: if t.per_row {
                    Some(PerRow {
                        tt: seg.tt_s_view()?,
                        bounds: seg.tt_s_bounds(),
                        sidecar: seg.sidecar(),
                        rows: seg.rows() as usize,
                        exact: t.row_test,
                    })
                } else {
                    None
                },
            });
        }

        let mut out = NeighborRows {
            offsets: Vec::with_capacity(us.len() + 1),
            ..Default::default()
        };
        out.offsets.push(0);
        let mut cands: Vec<Cand> = Vec::new();
        let mut row_tests = 0usize;
        for &u in us {
            cands.clear();
            for t in &views {
                let (vt_s, vt_e_col) = (t.vt_s, t.vt_e);
                let (src, dst) = (t.src, t.dst);
                let (vid64, vid_lo32) = (t.vid64, t.vid_lo32);
                let (rel_codes, rel_allowed) = (t.rel_codes, t.rel_allowed.as_ref());
                let ranges = t.ranges;

                for &d in dirs {
                    for &r in t.adj.slice(d, u) {
                        let i = r as usize;
                        // ---- belief -------------------------------------
                        //
                        // Both halves of the predicate, already decided for
                        // the whole segment above (`close_view` +
                        // `believed_ranges_closed`). The index
                        // is built over *all stored rows* of an immutable
                        // segment, so this is where — and the only place
                        // where — belief enters a traversal.
                        //
                        // Or per adjacency row, through the same
                        // primitives restricted to the row, when that is
                        // cheaper than building the segment's ranges.
                        let believed = match &t.per_row {
                            None => in_ranges(r, ranges),
                            Some(p) if p.exact => {
                                row_tests += 1;
                                p.tt.at(r)? <= as_of
                                    && as_of
                                        < crate::read::effective_tt_e(&closes, &p.sidecar, t.id, r)
                            }
                            Some(p) => believed_row(
                                &p.tt, p.bounds, p.sidecar, &closes, t.id, p.rows, as_of, r,
                            ),
                        };
                        if !believed {
                            continue;
                        }
                        // ---- valid time ---------------------------------
                        let row_vt_s = vt_s[i];
                        let row_vt_e = match vt_e_col {
                            Some(c) => c[i],
                            None => row_vt_s + 1,
                        };
                        if vt_min.is_some_and(|t| row_vt_e <= t) {
                            continue;
                        }
                        if vt_max.is_some_and(|t| row_vt_s >= t) {
                            continue;
                        }
                        // ---- rel type -----------------------------------
                        if let (Some(codes), Some(allowed)) = (rel_codes, rel_allowed) {
                            if !allowed.get(codes[i] as usize).copied().unwrap_or(false) {
                                continue;
                            }
                        }
                        cands.push(Cand {
                            vt_s: row_vt_s,
                            vid: Id96 {
                                hi: vid64[i],
                                lo: vid_lo32[i],
                            },
                            nbr: if d == crate::tcsr::Direction::Out {
                                dst[i]
                            } else {
                                src[i]
                            },
                            vt_e: row_vt_e,
                            seg_id: t.id,
                            seg_row: r,
                        });
                    }
                }
            }
            // The scan's own order. Stable, so `Both`'s out-before-in
            // concatenation survives an exact `(vt_s, vid)` tie — which only
            // a self-loop can produce, and which `np.lexsort` breaks the
            // same way.
            cands.sort_by_key(|c| (c.vt_s, c.vid));
            for c in &cands {
                out.nbr.push(c.nbr);
                out.vt_s.push(c.vt_s);
                out.vt_e.push(c.vt_e);
                out.seg_id.push(c.seg_id);
                out.seg_row.push(c.seg_row);
            }
            out.offsets.push(out.nbr.len() as u32);
        }
        crate::vistouch::rows(row_tests);
        Ok(out)
    }
}

/// An edge scan's selection, `(segment id, selected rows)` per segment
/// with a selected row, and the scan's counters.
pub type EdgeSelection = (Vec<(u64, Vec<u32>)>, crate::scan::ScanStats);

/// One edge scan as the binding's `scan_edges` answers it: the
/// materialized columns, the scan's counters and stage times, and the time
/// spent opening the segments.
#[derive(Debug)]
pub struct EdgeScan {
    pub cols: crate::scan::EdgeColumns,
    pub stats: crate::scan::ScanStats,
    pub timings: crate::scan::ScanTimings,
    pub open_ns: u64,
}

/// A filtered scan's adjacency postings, from the store's
/// per-segment adjacency index, or under `GNOMON_TCSR=scan` from one scan of
/// the segment's endpoint columns for the filter's ids -- the same two
/// sources a traversal call uses ([`NativeStore::target_adjacency`]).
struct StorePostings<'s> {
    store: &'s NativeStore,
    files: &'s [String],
    segs: &'s [std::sync::Arc<crate::segment::Segment<MmapSource>>],
}

impl crate::scan::PostingsSource for StorePostings<'_> {
    fn postings(
        &self,
        idx: usize,
        ids: &[u32],
        src_only: bool,
        budget: usize,
    ) -> Result<Option<crate::scan::Postings>> {
        use crate::tcsr::Direction;
        // the and-form needs both endpoints in the set, so the rows whose
        // source is in it already hold every match
        let dirs: &[Direction] = if src_only {
            &[Direction::Out]
        } else {
            &[Direction::Out, Direction::In]
        };
        let adj = self
            .store
            .target_adjacency(&self.files[idx], &self.segs[idx], ids, dirs)?;
        let mut walked = 0usize;
        for &u in ids {
            for &d in dirs {
                walked += adj.slice(d, u).len();
            }
        }
        if walked >= budget {
            return Ok(None);
        }
        let mut rows: Vec<u32> = Vec::with_capacity(walked);
        for &u in ids {
            for &d in dirs {
                rows.extend_from_slice(adj.slice(d, u));
            }
        }
        // each slice is ascending; a row with both endpoints in the set
        // appears twice
        rows.sort_unstable();
        rows.dedup();
        Ok(Some(crate::scan::Postings {
            rows,
            walked: walked as u64,
        }))
    }
}

impl NativeStore {
    /// Open the live edge segments (event lane, then interval lane,
    /// as the manifest lists them), build the scan the binding's
    /// `scan_edges` runs over them, and hand it to `f` with each target's
    /// segment id.
    ///
    /// When the request carries an incidence filter of at most
    /// [`crate::defaults::ADJ_FILTER_MAX_IDS`] ids, the scan gets the
    /// store's adjacency postings; each segment then takes them where they
    /// name fewer rows than the scan would examine. `GNOMON_ADJ_FILTERED_SCAN=0`
    /// never attaches them, nor does `GNOMON_TCSR=python` (whose traversal
    /// index is the adapter's, and which must leave `idx/tcsr/` untouched).
    pub(crate) fn with_edge_scan<R>(
        &self,
        req: &crate::scan::ScanRequest,
        f: impl FnOnce(&crate::scan::ScanSet<'_, MmapSource>, &[u64]) -> Result<R>,
    ) -> Result<R> {
        // a current-only store must refuse a past-belief scan rather than
        // silently answer it from the surviving rows
        self.assert_full_belief(req.as_of_tt)?;
        let m = self.manifest();
        let mut files: Vec<String> = Vec::new();
        let mut lanes: Vec<Lane> = Vec::new();
        for (lane, entries) in [
            (Lane::Event, &m.edge_lanes.event),
            (Lane::Interval, &m.edge_lanes.interval),
        ] {
            for e in entries {
                files.push(e.file.clone());
                lanes.push(lane);
            }
        }
        let mut segs = Vec::with_capacity(files.len());
        for file in &files {
            // via the store so checksums are walked on first touch
            segs.push(self.open_segment(file)?);
        }
        let ids: Vec<u64> = files.iter().map(|f| segment_id_of(f)).collect();
        let targets: Vec<crate::scan::ScanTarget<'_, MmapSource>> = segs
            .iter()
            .zip(&lanes)
            .zip(&ids)
            .map(|((segment, lane), id)| crate::scan::ScanTarget {
                segment,
                lane: *lane,
                id: *id,
            })
            .collect();
        let postings = StorePostings {
            store: self,
            files: &files,
            segs: &segs,
        };
        let mut set = crate::scan::ScanSet::new(targets)
            .with_closes(self.close_index()?)
            .with_designs(self.designs);
        if self.designs.adjacency_filtered_scan
            && !self.tcsr_python
            && req
                .touching_ids
                .as_ref()
                .is_some_and(|t| t.len() <= crate::defaults::ADJ_FILTER_MAX_IDS)
        {
            set = set.with_postings(&postings);
        }
        f(&set, &ids)
    }

    /// The windowed columnar edge scan `scan_edges` serves (here rather than
    /// in the binding, so the engine's tests run the same path).
    pub fn scan_edges(&self, req: &crate::scan::ScanRequest) -> Result<EdgeScan> {
        let t_open = std::time::Instant::now();
        self.with_edge_scan(req, |set, _| {
            let open_ns = t_open.elapsed().as_nanos() as u64;
            let (cols, stats, timings) = set.materialize_edges_timed(req)?;
            Ok(EdgeScan {
                cols,
                stats,
                timings,
                open_ns,
            })
        })
    }

    /// The same scan's selection only: `(segment id, selected rows)` per
    /// segment with a selected row, in scan order, and its counters.
    pub fn select_edges(&self, req: &crate::scan::ScanRequest) -> Result<EdgeSelection> {
        self.with_edge_scan(req, |set, ids| {
            let (sel, stats) = set.select(req)?;
            Ok((
                sel.into_iter().map(|s| (ids[s.segment], s.rows)).collect(),
                stats,
            ))
        })
    }
}

/// Segment file id from its manifest path (`seg/000000000042.tgs` -> 42).
pub fn segment_id_of(file: &str) -> u64 {
    file.rsplit('/')
        .next()
        .and_then(|n| n.strip_suffix(".tgs"))
        .and_then(|n| n.parse().ok())
        .unwrap_or(u64::MAX)
}

/// Write via a temp file and rename, so readers see the old bytes or the new
/// bytes and never a partial write. The parent directory is fsynced too —
/// without it the rename itself can be lost on crash.
fn write_atomic(path: &Path, contents: &str) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp).map_err(|e| EngineError::from(e).at_file(&tmp))?;
        f.write_all(contents.as_bytes())
            .map_err(|e| EngineError::from(e).at_file(&tmp))?;
        f.sync_all().map_err(|e| EngineError::from(e).at_file(&tmp))?;
    }
    fs::rename(&tmp, path).map_err(|e| EngineError::from(e).at_file(path))?;
    if let Some(dir) = path.parent() {
        fsync_dir(dir)?;
    }
    Ok(())
}

fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(|e| EngineError::from(e).at_file(dir))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::{MemorySource, Segment};
    use crate::OPEN_END;

    fn tmp_root(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-store-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        p
    }

    fn commit_with(store: &mut NativeStore, tt: i64, uids: &[&str]) -> u64 {
        store.begin(tt).unwrap();
        for u in uids {
            store.ensure_entity(u, "Node").unwrap();
        }
        let prev = store.manifest().event_log.chain.clone();
        store
            .commit(EventLogRef {
                offset: tt as u64,
                chain: EventLogRef::extend_chain(&prev, format!("{{\"tt\":{tt}}}\n").as_bytes()),
            })
            .unwrap()
    }

    #[test]
    fn fresh_store_bootstraps_at_generation_zero() {
        let root = tmp_root("bootstrap");
        let s = NativeStore::open(&root).unwrap();
        assert_eq!(s.generation(), 0);
        assert!(root.join("CURRENT").exists());
        assert!(NativeStore::manifest_path(&root, 0).exists());
        assert_eq!(s.dict().len(), 0);
    }

    #[test]
    fn commits_advance_generations_and_survive_reopen() {
        let root = tmp_root("advance");
        let mut s = NativeStore::open(&root).unwrap();
        assert_eq!(commit_with(&mut s, 10, &["n1", "n2"]), 1);
        assert_eq!(commit_with(&mut s, 20, &["n3"]), 2);
        drop(s);

        let re = NativeStore::open(&root).unwrap();
        assert_eq!(re.generation(), 2);
        assert_eq!(re.dict().len(), 3);
        assert_eq!(re.dict().dense_id("n3"), Some(2));
        assert_eq!(re.manifest().parent, Some(1));
    }

    #[test]
    fn open_phases_default_is_all_zero_and_sums_to_zero() {
        // pure struct arithmetic, no I/O: the zero value every field of
        // `OpenPhases` must agree on, and the sum-of-named-phases invariant
        // an empty phase record trivially satisfies.
        let p = OpenPhases::default();
        assert_eq!(p.checkpoint_read_parse_us, 0);
        assert_eq!(p.merkle_verify_us, 0);
        assert_eq!(p.chain_format, 0);
        assert_eq!(p.state_build_us, 0);
        assert_eq!(p.delta_replay_us, 0);
        assert_eq!(p.delta_count, 0);
        assert_eq!(p.dictionary_open_us, 0);
        assert_eq!(p.other_us, 0);
        assert_eq!(p.total_us, 0);
        let named = p.checkpoint_read_parse_us
            + p.merkle_verify_us
            + p.state_build_us
            + p.delta_replay_us
            + p.dictionary_open_us;
        assert_eq!(named + p.other_us, p.total_us);
    }

    #[test]
    fn open_phases_account_for_every_delta_and_sum_within_the_total() {
        // An earlier evaluation could not score "manifest-chain open <= 70 ms" because
        // NativeAdapter exposed no open-phase timing. This is that timing's
        // contract: every key present, delta_count matching what was
        // written, and the named phases never outrunning the total.
        let root = tmp_root("open-phases");
        let mut s = NativeStore::open(&root).unwrap();
        s.set_checkpoint_every(Some(1_000_000)); // one checkpoint, then all deltas
        const N: i64 = 12;
        for tt in 1..=N {
            s.begin(tt * 10).unwrap();
            s.ensure_entity(&format!("n{tt}"), "Node").unwrap();
            s.commit(EventLogRef::default()).unwrap();
        }
        drop(s);

        for _ in 0..2 {
            // opening the same store twice must yield the same delta_count
            let re = NativeStore::open(&root).unwrap();
            assert_eq!(re.generation(), N as u64);
            let p = re.open_phases();
            assert_eq!(p.delta_count, N as u64, "one delta per commit above the genesis checkpoint");
            assert_eq!(p.chain_format, crate::MANIFEST_FORMAT_VERSION);
            let named = p.checkpoint_read_parse_us
                + p.merkle_verify_us
                + p.state_build_us
                + p.delta_replay_us
                + p.dictionary_open_us;
            assert!(
                p.total_us >= named,
                "total_us {} must cover the named phases {named} \
                 (other_us {})",
                p.total_us,
                p.other_us
            );
            assert_eq!(p.other_us, p.total_us - named);
        }
    }

    #[test]
    fn rollback_leaves_no_trace() {
        let root = tmp_root("rollback");
        let mut s = NativeStore::open(&root).unwrap();
        commit_with(&mut s, 10, &["kept"]);
        s.begin(20).unwrap();
        s.ensure_entity("dropped", "Node").unwrap();
        s.rollback().unwrap();
        assert_eq!(s.generation(), 1);
        assert_eq!(s.dict().dense_id("dropped"), None);
        drop(s);

        let re = NativeStore::open(&root).unwrap();
        assert_eq!(re.generation(), 1);
        assert_eq!(re.dict().len(), 1);
        assert_eq!(re.dict().dense_id("dropped"), None);
    }

    // --- staged rows -> segments ------------------------------- //

    fn edge_row(src: u32, dst: u32, vt_s: i64, tt_s: i64, i: u32) -> EdgeRow {
        use crate::derive::{edge_eid, version_vid};
        let disc = format!("#{i}");
        let eid = edge_eid("n1", "n2", "SENT_MSG_TO", &disc);
        EdgeRow {
            vid: version_vid(&eid.to_hex(), tt_s, vt_s),
            src_id: src,
            dst_id: dst,
            rel_type: "SENT_MSG_TO".into(),
            disc,
            vt_s,
            vt_e: vt_s + 1,
            tt_s,
            props: "{}".into(),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    fn read_segment(root: &Path, file: &str) -> crate::segment::Segment<crate::segment::MemorySource> {
        let path = root.join(file);
        let src = crate::segment::MemorySource::load(&path).unwrap();
        crate::segment::Segment::open(&path, src, true).unwrap()
    }

    #[test]
    fn staged_rows_become_readable_segments_on_commit() {
        let root = tmp_root("segments");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let b = s.ensure_entity("n2", "Node").unwrap();
        for i in 0..10u32 {
            s.stage_edge(edge_row(a, b, 1_000 + i as i64, 100, i)).unwrap();
        }
        assert_eq!(s.staged_edges().len(), 10, "read-your-own-writes");
        s.commit(EventLogRef::default()).unwrap();
        assert!(s.staged_edges().is_empty(), "commit drains staging");
        drop(s);

        let re = NativeStore::open(&root).unwrap();
        assert_eq!(re.manifest().stats.n_edge_versions, 10);
        assert_eq!(re.manifest().edge_lanes.event.len(), 1);
        assert!(re.manifest().edge_lanes.interval.is_empty());
        let seg = read_segment(&root, &re.manifest().edge_lanes.event[0].file);
        assert_eq!(seg.rows(), 10);
        assert_eq!(seg.i64_column("vt_s").unwrap()[0], 1_000);
    }

    #[test]
    fn rollback_writes_no_segments_and_advances_nothing() {
        let root = tmp_root("rollback-segments");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        s.ensure_entity("n1", "Node").unwrap();
        s.stage_edge(edge_row(0, 0, 5, 100, 0)).unwrap();
        s.rollback().unwrap();

        assert!(s.staged_edges().is_empty());
        assert_eq!(s.generation(), 0);
        assert_eq!(s.manifest().next_segment_id, 0);
        assert_eq!(
            fs::read_dir(root.join("seg")).unwrap().count(),
            0,
            "a rolled-back batch must leave no segment files"
        );
    }

    #[test]
    fn segment_ids_never_repeat_across_generations() {
        let root = tmp_root("segment-ids");
        let mut s = NativeStore::open(&root).unwrap();
        for gen in 0..3u32 {
            s.begin(100 + gen as i64).unwrap();
            let a = s.ensure_entity("n1", "Node").unwrap();
            s.stage_edge(edge_row(a, a, 10 + gen as i64, 100 + gen as i64, gen)).unwrap();
            s.commit(EventLogRef::default()).unwrap();
        }
        // each generation inherits its parent's segments and adds its own
        let files: Vec<String> = s
            .manifest()
            .edge_lanes
            .event
            .iter()
            .map(|e| e.file.clone())
            .collect();
        assert_eq!(files.len(), 3, "every generation's segment is still listed");
        let mut unique = files.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), files.len(), "ids were reused: {files:?}");
        assert_eq!(s.manifest().next_segment_id, 3);
        assert_eq!(s.manifest().stats.n_edge_versions, 3);
    }

    #[test]
    fn long_lived_facts_route_to_the_interval_lane() {
        let root = tmp_root("lanes");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let mut forever = edge_row(a, a, 0, 100, 0);
        forever.vt_e = crate::OPEN_END;
        s.stage_edge(forever).unwrap();
        s.stage_edge(edge_row(a, a, 5, 100, 1)).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        assert_eq!(s.manifest().edge_lanes.event.len(), 1);
        assert_eq!(s.manifest().edge_lanes.interval.len(), 1);
    }

    // --- belief visibility -------------------------------------- //

    fn scan_at(root: &Path, store: &NativeStore, as_of: i64) -> Vec<Id96> {
        use crate::scan::{ScanRequest, ScanSet, ScanTarget};
        let m = store.manifest();
        let mut segs = Vec::new();
        let mut ids = Vec::new();
        for e in m.edge_lanes.event.iter().chain(m.edge_lanes.interval.iter()) {
            let path = root.join(&e.file);
            segs.push(Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap());
            ids.push(super::segment_id_of(&e.file));
        }
        let targets: Vec<ScanTarget<'_, MemorySource>> = segs
            .iter()
            .zip(&ids)
            .map(|(segment, id)| ScanTarget {
                segment,
                lane: Lane::Event,
                id: *id,
            })
            .collect();
        let set = ScanSet::new(targets).with_closes(store.close_index().unwrap());
        let mut req = ScanRequest::current();
        req.as_of_tt = as_of;
        let (sel, _) = set.select(&req).unwrap();
        set.merged(&sel, None)
            .unwrap()
            .into_iter()
            .map(|(si, row)| segs[si].vid_at(row as usize).unwrap())
            .collect()
    }

    #[test]
    fn closing_a_version_hides_it_only_from_later_beliefs() {
        // the bi-temporal immutability property: a correction must not change
        // what the database believed *before* the correction happened
        let root = tmp_root("close-bitemporal");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let rows: Vec<EdgeRow> = (0..3).map(|i| edge_row(a, a, 10 + i as i64, 100, i)).collect();
        for r in &rows {
            s.stage_edge(r.clone()).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();
        assert_eq!(scan_at(&root, &s, OPEN_END).len(), 3);

        // a later batch stops believing the middle version
        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, rows[1].vid, 200).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        let now = scan_at(&root, &s, OPEN_END);
        assert_eq!(now.len(), 2, "the closed version is no longer believed");
        assert!(!now.contains(&rows[1].vid));

        let before = scan_at(&root, &s, 150);
        assert_eq!(before.len(), 3, "history must be unchanged by a correction");
        assert!(before.contains(&rows[1].vid));

        // and exactly at the close time it is already gone (half-open tt)
        assert_eq!(scan_at(&root, &s, 200).len(), 2);
        assert_eq!(scan_at(&root, &s, 199).len(), 3);
    }

    #[test]
    fn closes_survive_reopening_the_store() {
        let root = tmp_root("close-reopen");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let row = edge_row(a, a, 10, 100, 0);
        s.stage_edge(row.clone()).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, row.vid, 200).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        assert_eq!(s.manifest().close_runs.len(), 1);
        drop(s);

        let re = NativeStore::open(&root).unwrap();
        assert_eq!(re.close_index().unwrap().len(), 1);
        assert!(scan_at(&root, &re, OPEN_END).is_empty());
        assert_eq!(scan_at(&root, &re, 150).len(), 1);
    }

    #[test]
    fn a_version_closed_in_its_own_batch_folds_into_the_sidecar() {
        // a carve: the batch inserts a version and closes it again, so no
        // close run is needed — the segment carries its own sidecar
        let root = tmp_root("close-staged");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let keep = edge_row(a, a, 10, 100, 0);
        let doomed = edge_row(a, a, 20, 100, 1);
        s.stage_edge(keep.clone()).unwrap();
        s.stage_edge(doomed.clone()).unwrap();
        s.close_version(RowKind::Edge, doomed.vid, 100).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        assert!(
            s.manifest().close_runs.is_empty(),
            "a same-batch close needs no run file"
        );
        let entry = &s.manifest().edge_lanes.event[0];
        assert_eq!(entry.n_closed_folded, 1);
        assert!(!entry.all_current);

        let now = scan_at(&root, &s, OPEN_END);
        assert_eq!(now, vec![keep.vid], "only the surviving version is believed");
        // the row is still stored — closed is not deleted
        let path = root.join(&entry.file);
        let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        assert_eq!(seg.rows(), 2, "a closed row is retained, only hidden");
    }

    #[test]
    fn a_layered_batch_close_is_visible_to_its_own_scan() {
        // The uncommitted overlay, end to end through the scan path. A batch
        // reads belief to decide what to carve, so a batch that cannot see
        // its own corrections carves against a version it has already
        // stopped believing — and now that the `as_of < tt_e` half
        // of the predicate is answered in the range domain, the per-segment
        // close view has to resolve that overlay over the committed index
        // rather than only `CloseIndex::tt_e` doing so.
        let root = tmp_root("close-layered-scan");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let rows: Vec<EdgeRow> = (0..4).map(|i| edge_row(a, a, 10 + i as i64, 100, i)).collect();
        for r in &rows {
            s.stage_edge(r.clone()).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();
        assert_eq!(scan_at(&root, &s, OPEN_END).len(), 4);

        // a later batch, still open: nothing is in a run file yet
        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, rows[1].vid, 200).unwrap();
        s.close_version(RowKind::Edge, rows[2].vid, 200).unwrap();
        assert!(s.manifest().close_runs.is_empty(), "nothing committed yet");
        let mid = scan_at(&root, &s, OPEN_END);
        assert_eq!(mid.len(), 2, "the batch must see its own corrections");
        assert!(!mid.contains(&rows[1].vid));
        assert!(!mid.contains(&rows[2].vid));
        // two adjacent closed rows in the middle: the range really is split
        assert_eq!(mid, vec![rows[0].vid, rows[3].vid]);
        // and the past is untouched from inside the batch too
        assert_eq!(scan_at(&root, &s, 150).len(), 4);

        // rolling the batch back discards the overlay rather than merging it
        s.rollback().unwrap();
        assert_eq!(
            scan_at(&root, &s, OPEN_END).len(),
            4,
            "a rolled-back close must leave belief where it was"
        );

        // and the same closes, committed, answer identically
        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, rows[1].vid, 200).unwrap();
        s.close_version(RowKind::Edge, rows[2].vid, 200).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        assert_eq!(scan_at(&root, &s, OPEN_END), mid);
    }

    #[test]
    fn closing_an_unknown_version_is_not_found() {
        let root = tmp_root("close-missing");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let ghost = crate::derive::version_vid("nope", 1, 1);
        let err = match s.close_version(RowKind::Edge, ghost, 200) {
            Ok(()) => panic!("closing a nonexistent version must fail"),
            Err(e) => e,
        };
        assert_eq!(err.category, crate::error::Category::NotFound);
    }

    #[test]
    fn a_close_verifies_the_full_vid_not_just_its_prefix() {
        // the postings key is only the 64-bit prefix; a candidate whose lo
        // differs must be rejected, or a prefix collision could close the
        // wrong version
        let root = tmp_root("close-prefix");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let row = edge_row(a, a, 10, 100, 0);
        s.stage_edge(row.clone()).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        s.begin(200).unwrap();
        let imposter = Id96 {
            hi: row.vid.hi,
            lo: row.vid.lo.wrapping_add(1),
        };
        let err = s.close_version(RowKind::Edge, imposter, 200).unwrap_err();
        assert_eq!(err.category, crate::error::Category::NotFound);
        s.close_version(RowKind::Edge, row.vid, 200).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        assert_eq!(s.close_index().unwrap().len(), 1);
    }

    #[test]
    fn a_node_close_locates_through_the_postings_too() {
        let root = tmp_root("close-node");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let row = NodeRow {
            vid: crate::derive::version_vid("n1", 100, 10),
            uid_id: a,
            label: "Node".into(),
            vt_s: 10,
            vt_e: 11,
            tt_s: 100,
            props: "{}".into(),
            source: "ingest".into(),
            provenance_ref: None,
        };
        s.stage_node(row.clone()).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        s.begin(200).unwrap();
        s.close_version(RowKind::Node, row.vid, 200).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        assert!(s.believed_node_versions("n1", OPEN_END).unwrap().is_empty());
        assert_eq!(s.believed_node_versions("n1", 150).unwrap().len(), 1);
    }

    // --- bounded identity postings ------------------------------------- //

    /// Build a store whose edge segments have *overlapping* key ranges, and
    /// hand back every committed `(vid, vt_s)` with the manifest.
    ///
    /// Overlap is the point: with disjoint ranges a range prune is trivially
    /// right because at most one segment can ever be a candidate, and the
    /// test would not distinguish "prunes correctly" from "prunes to one".
    /// Committing batches whose `vt_s` domains interleave forces several
    /// candidates per probe and exercises the `<= key_hi` / `>= key_lo`
    /// boundary comparisons for real.
    fn store_with_overlapping_segments(name: &str) -> (PathBuf, NativeStore, Vec<(Id96, i64)>) {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        let mut rows = Vec::new();
        for batch in 0..6u32 {
            let tt = 100 + batch as i64;
            s.begin(tt).unwrap();
            let a = s.ensure_entity("n1", "Node").unwrap();
            let b = s.ensure_entity("n2", "Node").unwrap();
            // vt_s domains 0..40, 20..60, 40..80, ... — each batch overlaps
            // the previous one by half
            for i in 0..40u32 {
                let vt = (batch * 20 + i) as i64;
                let r = edge_row(a, b, vt, tt, batch * 40 + i);
                rows.push((r.vid, r.vt_s));
                s.stage_edge(r).unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        (root, s, rows)
    }

    // --- the budget bounds total segment residency --------------------- //

    /// Read every row of the store through the cached path, so the segment
    /// cache actually holds the generation's segments.
    fn touch_every_segment(s: &NativeStore) {
        for e in s
            .manifest()
            .segments_of(RowKind::Edge)
            .chain(s.manifest().segments_of(RowKind::Node))
            .map(|e| e.file.clone())
            .collect::<Vec<_>>()
        {
            let _ = s.open_segment(&e);
        }
    }

    fn component(s: &NativeStore, key: &str) -> u64 {
        s.memory_accounting()
            .0
            .into_iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v)
            .unwrap_or_else(|| panic!("no component {key}"))
    }

    #[test]
    fn the_budget_charges_decoded_heap_and_never_the_mapping() {
        // `GNOMON_SEGMENT_CACHE_BYTES` bounds anonymous bytes. A
        // segment's file mapping is the store itself -- file-backed pages the
        // kernel manages -- so it is not charged here and not credited back
        // on eviction. Charging it was what made a 32 GB budget produce
        // 52.6 GB of residency at 1B.
        let (_root, s, _rows) = store_with_overlapping_segments("w15-charge");
        touch_every_segment(&s);

        let accounted = component(&s, "segment_cache_accounted");
        let decoded = component(&s, "segment_cache_decoded");
        let header = component(&s, "segment_cache_header");
        let mapped = component(&s, "segment_cache_mapped");
        assert!(mapped > 0, "the fixture must have mapped bytes to exclude");
        assert_eq!(
            accounted,
            decoded + header,
            "the budget charges decoded heap and the parsed header, nothing else"
        );
    }

    #[test]
    fn the_three_classes_partition_what_the_accounting_names() {
        // `anonymous_total` is every heap component summed;
        // `file_mapped_total` is the store's own mapped bytes. They are
        // reported separately because they are bounded by different things
        // and land in different `/proc/self/status` fields.
        let (_root, s, _rows) = store_with_overlapping_segments("w15-classes");
        touch_every_segment(&s);
        let got = s.memory_accounting().0;
        let get = |k: &str| got.iter().find(|(n, _)| *n == k).map(|(_, v)| *v).unwrap();

        let anon = get("anonymous_total");
        let parts = ["segment_cache_decoded", "segment_cache_header", "postings",
                     "tcsr", "name_index_cache", "close_index", "staging",
                     "manifest", "dictionary", "stats_accum", "verified_set",
                     // Identity-filter bytes left on the heap (a filter
                     // whose write failed); zero in the normal case
                     "vid_filter_heap"];
        assert_eq!(anon, parts.iter().map(|k| get(k)).sum::<u64>());

        // the mapped class is the store's pinned segments plus the mapped
        // identity filters -- still exactly the sum of its named parts, which
        // is the property this test states; a filter is a file mapping and
        // belongs on this side, never on the anonymous one
        assert_eq!(
            get("file_mapped_total"),
            get("pinned_mapped") + get("vid_filter_mapped")
        );
        // the mapped class is the store, not a cache artefact: it does not
        // shrink when the cache is squeezed to nothing
        let mapped_before = get("file_mapped_total");
        s.set_segment_cache_budget(Some(1));
        let got2 = s.memory_accounting().0;
        let get2 = |k: &str| got2.iter().find(|(n, _)| *n == k).map(|(_, v)| *v).unwrap();
        assert_eq!(get2("file_mapped_total"), mapped_before);
        assert!(get2("segment_cache_decoded") <= get("segment_cache_decoded"));
    }

    #[test]
    fn the_decoded_budget_binds_what_it_says_it_binds() {
        // A budget of B bounds B bytes of decoded heap -- never B minus
        // something the operator was not told about. The cache never evicts
        // its last entry, so one segment's own heap is the whole allowance.
        let (_root, s, _rows) = store_with_overlapping_segments("w15-binds");
        touch_every_segment(&s);
        let full = component(&s, "segment_cache_accounted");
        assert!(full > 0);

        for budget in [full / 2, full / 4, full / 8] {
            s.set_segment_cache_budget(Some(budget));
            touch_every_segment(&s);
            let acc = component(&s, "segment_cache_accounted");
            let entries = component(&s, "segment_cache_entries").max(1);
            let largest_entry = acc / entries;
            assert!(
                acc <= budget + largest_entry,
                "budget {budget}: accounted {acc} over budget by more than \
                 the one entry the cache may not evict"
            );
        }
        assert!(
            component(&s, "segment_cache_evictions") > 0,
            "a binding budget must have evicted, or this proves nothing"
        );
    }

    #[test]
    fn residency_never_changes_an_answer() {
        // Correctness never depends on residency -- the property every
        // budget change in this engine has had to keep. A one-byte budget
        // and an unbounded one must locate every row identically.
        let (_root, s, rows) = store_with_overlapping_segments("w15-answers");
        s.set_segment_cache_budget(None);
        let unbounded: Vec<_> = rows
            .iter()
            .map(|(vid, vt_s)| s.locate_vid(RowKind::Edge, *vid, Some(*vt_s)).unwrap())
            .collect();
        s.set_segment_cache_budget(Some(1));
        let squeezed: Vec<_> = rows
            .iter()
            .map(|(vid, vt_s)| s.locate_vid(RowKind::Edge, *vid, Some(*vt_s)).unwrap())
            .collect();
        assert_eq!(unbounded, squeezed);
        assert!(
            component(&s, "segment_cache_evictions") > 0,
            "a one-byte budget must have evicted, or this proves nothing"
        );
    }

    #[test]
    fn locate_vid_range_prune_agrees_with_the_unpruned_search() {
        // `locate_vid` used to consult a `HashMap` entry
        // per row of the whole store; it now bounds the candidate segments by
        // the manifest's `key_lo`/`key_hi` for `(vt_s, vid)` and binary-
        // searches the survivors. The gate is equivalence: for every row ever
        // committed, the pruned probe must name exactly the segment and row
        // the unpruned linear search names.
        let (_root, s, rows) = store_with_overlapping_segments("locate-vid-prune");
        assert!(
            s.manifest().edge_lanes.event.len() >= 6,
            "the fixture must span several segments"
        );
        // the fixture is only honest if the ranges really do overlap
        let mut entries: Vec<_> = s.manifest().segments_of(RowKind::Edge).collect();
        entries.sort_by(|a, b| a.key_lo.cmp(&b.key_lo));
        assert!(
            entries.windows(2).any(|w| w[1].key_lo < w[0].key_hi),
            "the fixture's segment key ranges must overlap, or the prune is \
             trivially right"
        );

        for (vid, vt_s) in &rows {
            let pruned = s.locate_vid(RowKind::Edge, *vid, Some(*vt_s)).unwrap();
            let whole = s.locate_vid(RowKind::Edge, *vid, None).unwrap();
            assert_eq!(pruned, whole, "vid {} at vt_s {vt_s}", vid.to_hex());
            assert!(pruned.is_some(), "vid {} was committed", vid.to_hex());
        }

        // a vid that was never committed is None either way, and a *wrong*
        // vt_s must not manufacture a hit
        let absent = crate::derive::version_vid("nope", 1, 1);
        assert_eq!(s.locate_vid(RowKind::Edge, absent, Some(0)).unwrap(), None);
        assert_eq!(s.locate_vid(RowKind::Edge, absent, None).unwrap(), None);
    }

    #[test]
    fn locate_vid_finds_the_rows_that_sit_exactly_on_a_segment_boundary() {
        // The rows `key_lo` and `key_hi` name are the ones an off-by-one in
        // the range comparison drops: a strict `>`/`<` instead of `>=`/`<=`
        // loses the first and last row of every segment, and every other row
        // still resolves, so nothing else in the suite would notice.
        let (_root, s, _rows) = store_with_overlapping_segments("locate-vid-boundary");
        let mut checked = 0;
        for entry in s.manifest().segments_of(RowKind::Edge) {
            for (vt_s, hex) in [&entry.key_lo, &entry.key_hi] {
                let vid = Id96::from_hex(hex).unwrap();
                let got = s.locate_vid(RowKind::Edge, vid, Some(*vt_s)).unwrap();
                assert_eq!(
                    got,
                    s.locate_vid(RowKind::Edge, vid, None).unwrap(),
                    "boundary row ({vt_s}, {hex}) of {}",
                    entry.file
                );
                assert!(got.is_some(), "boundary row ({vt_s}, {hex}) must resolve");
                checked += 1;
            }
        }
        assert!(checked >= 12, "only {checked} boundary rows exercised");
    }

    #[test]
    fn sealed_segments_are_ordered_by_vt_s_then_vid() {
        // The invariant `locate_vid`'s binary search rests on. `row.rs`
        // sorts by this key and `scan.rs` already binary-searches `vt_s`
        // alone for window pruning, but nothing asserted the *composite*
        // order inside a sealed file — and a binary search over an unsorted
        // column fails by returning `None`, which reads as "no such version
        // to close" rather than as a bug.
        let (root, s, _rows) = store_with_overlapping_segments("segment-order");
        for entry in s.manifest().segments_of(RowKind::Edge) {
            let seg = read_segment(&root, &entry.file);
            let vt = seg.i64_column("vt_s").unwrap();
            let hi = seg.u64_column("vid64").unwrap();
            let lo = seg.u32_column("vid_lo32").unwrap();
            let keys: Vec<(i64, u64, u32)> =
                (0..vt.len()).map(|i| (vt[i], hi[i], lo[i])).collect();
            assert!(
                keys.windows(2).all(|w| w[0] < w[1]),
                "{} is not in strict (vt_s, vid) order",
                entry.file
            );
            assert_eq!(
                (keys[0].0, Id96 { hi: keys[0].1, lo: keys[0].2 }.to_hex()),
                (entry.key_lo.0, entry.key_lo.1.clone()),
                "{}'s key_lo must be its first row",
                entry.file
            );
        }
    }

    #[test]
    fn a_one_byte_postings_budget_gives_identical_answers_to_an_unbounded_one() {
        // The claim the postings budget rests on is that
        // correctness never depends on residency: a segment whose postings
        // were evicted (or were never insertable) is folded on the spot and
        // answers from the fold, exactly as an uncached segment open trades
        // latency and not answers. A 1-byte budget is the extreme of that —
        // *nothing* is ever resident — so if the two configurations agree row for row
        // over a correction-carrying store, every budget between them does
        // too.
        let (root, mut s, _rows) = store_with_overlapping_segments("postings-budget");
        // corrections, so `open_rows`' in-place prune is exercised as well as
        // the plain identity lookup
        let eids: Vec<String> = s
            .all_edge_versions()
            .unwrap()
            .iter()
            .map(|v| v.eid.clone())
            .collect();
        let to_close: Vec<(Id96, i64)> = {
            let mut v: Vec<(Id96, i64)> = s
                .all_edge_versions()
                .unwrap()
                .iter()
                .step_by(7)
                .map(|r| (Id96::from_hex(&r.vid).unwrap(), r.vt_s))
                .collect();
            v.truncate(20);
            v
        };
        s.begin(500).unwrap();
        for (vid, vt_s) in &to_close {
            s.close_version_at(RowKind::Edge, *vid, Some(*vt_s), 500).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();

        let answers = |s: &NativeStore| -> Vec<(String, Vec<String>, Vec<String>)> {
            eids.iter()
                .map(|eid| {
                    let current: Vec<String> = s
                        .believed_edge_versions(eid, OPEN_END)
                        .unwrap()
                        .iter()
                        .map(|v| v.vid.clone())
                        .collect();
                    let historical: Vec<String> = s
                        .believed_edge_versions(eid, 300)
                        .unwrap()
                        .iter()
                        .map(|v| v.vid.clone())
                        .collect();
                    (eid.clone(), current, historical)
                })
                .collect()
        };

        s.set_postings_budget(None);
        let unbounded = answers(&s);
        let (_, _, unbounded_bytes, ..) = s.postings_index_stats(RowKind::Edge);
        assert!(unbounded_bytes > 0, "the unbounded configuration must hold postings");

        // a fresh handle, so the answers cannot be served from the first
        // configuration's already-built postings
        let squeezed = NativeStore::open(&root).unwrap();
        squeezed.set_postings_budget(Some(1));
        let bounded = answers(&squeezed);
        assert_eq!(bounded, unbounded, "a 1-byte budget changed an answer");

        let (entries, resident, bytes, budget, _evictions, _rebuilds, folds, _rows, transient) =
            squeezed.postings_index_stats(RowKind::Edge);
        assert_eq!(
            (entries, resident, bytes, budget),
            (0, 0, 0, Some(1)),
            "a 1-byte budget must hold nothing at all"
        );
        assert_eq!(
            folds, transient,
            "every fold under a 1-byte budget is answered and dropped"
        );
        assert!(folds > 0, "the bounded configuration must actually have folded something");
    }

    #[test]
    fn the_postings_budget_evicts_by_segment_and_re_folds_on_the_next_probe() {
        // The middle of the range: a budget that holds *some* segments. The
        // eviction unit is a whole segment, least-recently-used first, as in
        // `SegmentCache` — and a probe that lands on an evicted segment folds
        // it again rather than missing the rows it held.
        let (_root, s, _rows) = store_with_overlapping_segments("postings-evict");
        let eids: Vec<String> = s
            .all_edge_versions()
            .unwrap()
            .iter()
            .map(|v| v.eid.clone())
            .collect();

        s.set_postings_budget(None);
        let warm: Vec<usize> = eids
            .iter()
            .map(|e| s.believed_edge_versions(e, OPEN_END).unwrap().len())
            .collect();
        let (_, resident_all, bytes_all, ..) = s.postings_index_stats(RowKind::Edge);
        assert!(resident_all >= 6, "the fixture must hold several segments");

        // room for roughly two segments' worth
        let budget = bytes_all / 3;
        s.set_postings_budget(Some(budget));
        let (_, resident_now, bytes_now, _, evictions, ..) = s.postings_index_stats(RowKind::Edge);
        assert!(
            bytes_now <= budget && resident_now < resident_all,
            "lowering the budget must evict: {bytes_now} B in {resident_now} \
             segments against a {budget} B budget"
        );
        assert!(evictions > 0, "eviction must be counted");

        let cold: Vec<usize> = eids
            .iter()
            .map(|e| s.believed_edge_versions(e, OPEN_END).unwrap().len())
            .collect();
        assert_eq!(cold, warm, "a re-fold after eviction changed an answer");

        let (_, resident_end, bytes_end, _, _, rebuilds, ..) =
            s.postings_index_stats(RowKind::Edge);
        assert!(bytes_end <= budget, "the budget must still hold after re-folds");
        assert!(
            rebuilds > 0,
            "probing {} identities over {resident_end} resident segments under \
             a budget of {} of the full {bytes_all} B must have re-folded \
             something",
            eids.len(),
            budget
        );
    }

    #[test]
    fn postings_budget_resolution() {
        use crate::defaults::POSTINGS_MAX_BYTES;
        // explicit override wins, 0 means unbounded, garbage falls through
        assert_eq!(postings_budget(Some("1048576"), Some(1 << 33)), Some(1 << 20));
        assert_eq!(postings_budget(Some("0"), Some(1 << 33)), None);
        assert_eq!(postings_budget(Some(" 42 "), None), Some(42));
        assert_eq!(postings_budget(Some("lots"), Some(1 << 33)), Some(1 << 30));
        // default: an eighth of detected RAM, under the ceiling
        assert_eq!(postings_budget(None, Some(1 << 33)), Some(1 << 30));
        assert_eq!(postings_budget(None, Some(503_680_000_000)), Some(POSTINGS_MAX_BYTES));
        // the ceiling must clear the whole size range: 1B rows of postings
        // at 12 B/row is ~12 GB, and a default that evicted there would turn
        // every correction at 1B into an O(store) re-fold
        assert_eq!(
            POSTINGS_MAX_BYTES.max(12_000_000_000),
            POSTINGS_MAX_BYTES,
            "the ceiling must clear 1B rows of postings at 12 B/row"
        );
        // unlike the segment cache, an undetectable RAM size still yields a
        // bound — the ceiling — rather than "unbounded"
        assert_eq!(postings_budget(None, None), Some(POSTINGS_MAX_BYTES));
    }

    #[test]
    fn corrections_at_scale_locate_without_building_postings() {
        // An earlier sweep over input scale
        // showed replay superlinear in correction volume because every close
        // walked every segment. Enough segments and closes here that the old
        // shape is exercised.
        //
        // A close locates through the manifest's key ranges and
        // a binary search, not through a vid postings map — so the gate is
        // now that it finds every row *and holds nothing*: the map that used
        // to come out of this loop was one `HashMap` entry per row of the
        // store, built by the first correction and never released.
        let root = tmp_root("close-scale");
        let mut s = NativeStore::open(&root).unwrap();
        let mut rows = Vec::new();
        for batch in 0..8u32 {
            let tt = 100 + batch as i64;
            s.begin(tt).unwrap();
            let a = s.ensure_entity("n1", "Node").unwrap();
            let b = s.ensure_entity("n2", "Node").unwrap();
            for i in 0..250u32 {
                let n = batch * 250 + i;
                let r = edge_row(a, b, n as i64, tt, n);
                rows.push(r.clone());
                s.stage_edge(r).unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        assert!(
            s.manifest().edge_lanes.event.len() >= 8,
            "the scale must span many segments"
        );

        s.begin(500).unwrap();
        for r in rows.iter().step_by(10) {
            s.close_version(RowKind::Edge, r.vid, 500).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();

        {
            let (entries, resident, bytes, ..) = s.postings_index_stats(RowKind::Edge);
            assert_eq!(
                (entries, resident, bytes),
                (0, 0, 0),
                "{} closes over 8 segments must locate by range prune alone — \
                 anything resident here is the obsolete O(store) vid map",
                rows.len() / 10
            );
        }

        let closed: std::collections::HashSet<String> =
            rows.iter().step_by(10).map(|r| r.vid.to_hex()).collect();
        assert_eq!(s.close_index().unwrap().len(), closed.len());
        for r in s.all_edge_versions().unwrap() {
            let expected = if closed.contains(&r.vid) { 500 } else { OPEN_END };
            assert_eq!(r.tt_e, expected, "vid {}", r.vid);
        }
    }

    #[test]
    fn the_open_index_shrinks_to_what_is_still_believed() {
        // Regression gate. The index must (a) get built at all, (b) hold
        // only rows a segment sealed open, and (c) *prune* on lookup as closes
        // arrive — without pruning it degenerates back into `by_identity` and
        // the O(depth) walk it replaced comes back with extra memory.
        let root = tmp_root("open-index");
        let mut s = NativeStore::open(&root).unwrap();
        let depth = 40usize;
        let mut vids = Vec::new();
        for d in 0..depth {
            let tt = 100 + d as i64;
            s.begin(tt).unwrap();
            let a = s.ensure_entity("n1", "Node").unwrap();
            let b = s.ensure_entity("n2", "Node").unwrap();
            let r = edge_row(a, b, 0, tt, 0);
            vids.push(r.vid);
            s.stage_edge(r).unwrap();
            // supersede the previous version, exactly as the correction path
            // does: closed, not retired, because it was believed at an
            // earlier transaction time
            if d > 0 {
                s.close_version(RowKind::Edge, vids[d - 1], tt).unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }

        // one identity, `depth` versions, exactly one of them still believed.
        // The lookup is also what triggers the prune, so it must come first.
        let eid = s.all_edge_versions().unwrap()[0].eid.clone();
        let believed = s.believed_edge_versions(&eid, OPEN_END).unwrap();
        assert_eq!(believed.len(), 1, "exactly one version of the identity is open");
        assert_eq!(believed[0].vid, vids[depth - 1].to_hex(), "the newest one");

        let held = {
            let p = s.edge_postings().lock().unwrap();
            let key = crate::derive::Id96::from_hex(&eid).unwrap().hi;
            let (all, open) = p.entries_for(key);
            assert_eq!(all, depth, "every version is still reachable by identity");
            open
        };
        assert_eq!(
            held, 1,
            "the open index must prune closed rows on lookup: it holds {held} \
             of {depth} versions, so it is tracking history rather than belief"
        );
    }

    #[test]
    fn compaction_cycles_do_not_multiply_the_identity_postings() {
        // Regression. `open_rows` has a lazy per-lookup prune
        // (`read.rs::locate_open`); `by_identity` and `by_vid` have none at
        // all, which was fine until compaction started running
        // periodically (`gnomon replay --compact-every`, and a long soak's
        // own compact()+gc() cadence): compaction reseals every row that
        // still exists — open *and* closed, since compaction never drops
        // one — into fresh segments every cycle, and the next lookup
        // re-indexes them there, on top of every earlier cycle's now-
        // unreachable entries that nothing ever freed. Observed in the
        // wild: `gnomon replay --compact-every 500` over a 24h soak's log
        // (1,074,952 batches, 314,897,038 bytes) was OOM-killed at ~83 GB
        // anon RSS after reaching manifest generation ~513,024 — ~160 KB
        // retained per replayed generation — while `store_digest()`-
        // relevant content was only ~1.7M entities: retention, not data.
        let root = tmp_root("compaction-postings-bound");
        let mut s = NativeStore::open(&root).unwrap();
        const CYCLES: usize = 8;
        const PER_CYCLE: usize = 25;
        const N: usize = CYCLES * PER_CYCLE;

        let mut vids = Vec::with_capacity(N);
        for d in 0..N {
            let tt = 100 + d as i64;
            s.begin(tt).unwrap();
            let a = s.ensure_entity("n1", "Node").unwrap();
            let b = s.ensure_entity("n2", "Node").unwrap();
            let r = edge_row(a, b, 0, tt, 0);
            vids.push(r.vid);
            s.stage_edge(r).unwrap();
            // supersede the previous version, exactly as a correction does
            // — this is also what triggers `index_segments` every commit.
            if d > 0 {
                s.close_version(RowKind::Edge, vids[d - 1], tt).unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();

            if (d + 1) % PER_CYCLE == 0 {
                s.compact().unwrap();
                s.gc(2).unwrap();
            }
        }

        // the read is what builds the postings at all now (a close no longer
        // does), so the gate reads before it measures
        let eid = s.all_edge_versions().unwrap()[0].eid.clone();
        assert_eq!(s.believed_edge_versions(&eid, OPEN_END).unwrap().len(), 1);
        let (by_identity_entries, by_vid_entries, indexed_len) =
            s.postings_stats(RowKind::Edge);

        // Every version ever committed legitimately lives in `by_identity`
        // forever (bi-temporal history is never dropped), so N is the
        // correct floor. The bug's shape is multiplicative in CYCLES, not
        // additive to it: unfixed, this comes out within a small constant
        // of PER_CYCLE * CYCLES^2 / 2 (each cycle re-indexes the entire
        // row set sealed so far) — tens of thousands of entries here,
        // against a few thousand fixed. A 3x margin over N catches the
        // multiplication while tolerating the legitimate double-booking of
        // the one or two generations `gc(keep_last=2)` still retains.
        assert!(
            by_identity_entries <= 3 * N,
            "by_identity holds {by_identity_entries} entries for {N} \
             versions ever committed over {CYCLES} compaction cycles — \
             compaction is re-indexing history that nothing ever prunes \
            "
        );
        assert!(
            by_vid_entries <= 3 * N,
            "by_vid holds {by_vid_entries} entries for {N} versions — same \
             leak as by_identity"
        );
        assert!(
            indexed_len <= 4 * PER_CYCLE,
            "`indexed` holds {indexed_len} segment ids after {CYCLES} \
             compactions of {PER_CYCLE} segments each — stale segment ids \
             from superseded compaction cycles are never forgotten"
        );
    }


    #[test]
    fn rolled_back_closes_leave_no_run() {
        let root = tmp_root("close-rollback");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let row = edge_row(a, a, 10, 100, 0);
        s.stage_edge(row.clone()).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, row.vid, 200).unwrap();
        s.rollback().unwrap();

        assert!(s.manifest().close_runs.is_empty());
        assert_eq!(scan_at(&root, &s, OPEN_END).len(), 1, "the close was abandoned");
    }

    // --- crash-step matrix --------------------------------------------- //

    #[test]
    fn crash_after_manifest_before_current_serves_previous_generation() {
        let root = tmp_root("crash-manifest");
        let mut s = NativeStore::open(&root).unwrap();
        commit_with(&mut s, 10, &["n1"]);
        drop(s);

        // simulate: generation 2's *delta* reached disk, CURRENT did not.
        // Reconstruction from CURRENT stops at 1 and never reads 2, so this
        // is the same orphaned-manifest case format 1 had.
        let g1 = manifest_chain::reconstruct(&root, 1).unwrap().manifest;
        let mut g2 = g1.successor(20);
        g2.stats.n_entities = 99;
        g2.seal();
        let d = ManifestDelta::between(&g1, &g2, 0).expect("expressible as a delta");
        write_atomic(&NativeStore::manifest_path(&root, 2), &d.to_json()).unwrap();

        let re = NativeStore::open(&root).unwrap();
        assert_eq!(re.generation(), 1, "orphaned delta must not be adopted");
        assert_eq!(re.dict().len(), 1);
        assert_eq!(re.manifest().stats.n_entities, 1);
        assert!(re.verify().unwrap().is_healthy());
    }

    // --- CURRENT missing: a populated store must refuse, not go empty --- //
    //
    // Corruption-sweep finding: deleting `native/CURRENT` from a
    // populated store used to make `open` treat the directory as fresh and
    // empty, so every query silently returned no rows. `open` must now
    // distinguish that from the one case where "CURRENT is absent" really
    // does mean "nothing published yet": the bootstrap crash between the
    // genesis manifest write and the very first `CURRENT` flip.

    #[test]
    fn empty_directory_without_current_still_opens_fresh() {
        let root = tmp_root("current-missing-empty");
        // `open` itself creates the subdirectories; nothing else is on disk.
        let s = NativeStore::open(&root).unwrap();
        assert_eq!(s.generation(), 0);
        assert!(root.join("CURRENT").exists());
    }

    // --- the commit path's working manifest ------------------------------- //

    #[test]
    fn a_commit_leaves_the_handle_holding_a_manifest_its_own_state_seals() {
        // the invariant the whole in-place commit path rests on: the carried
        // Merkle state is still the state of the manifest the handle holds,
        // commit after commit, and the O(1) seal agrees with the O(n) oracle
        let root = tmp_root("working-manifest");
        let mut s = NativeStore::open(&root).unwrap();
        for g in 1..=12i64 {
            commit_with(&mut s, g * 10, &[&format!("n{g}")]);
            let state = s.merkle.as_ref().expect("a writable store carries one");
            assert_eq!(
                state,
                &merkle::ManifestMerkle::from_manifest(s.manifest()),
                "the carried state drifted from the manifest at generation {g}"
            );
            assert_eq!(
                s.manifest().manifest_sha,
                s.manifest().body_sha_canonical(),
                "the incremental seal disagrees with the oracle at generation {g}"
            );
        }
        assert!(s.verify().unwrap().is_healthy());
    }

    #[test]
    fn the_delta_cut_from_the_append_span_is_the_one_a_full_diff_would_produce() {
        // `between`'s eight O(n) scans come off the
        // commit path. The record they used to produce must not change.
        let root = tmp_root("append-span");
        let mut s = NativeStore::open(&root).unwrap();
        commit_with(&mut s, 10, &["n1"]);
        let parent = s.manifest().clone();
        let ckpt = s.checkpoint_generation();
        commit_with(&mut s, 20, &["n2"]);
        let child = s.manifest().clone();

        let from_span = match manifest_chain::read_record(&root, child.generation).unwrap() {
            manifest_chain::ManifestRecord::Delta(d) => d,
            other => panic!("expected a delta, got {other:?}"),
        };
        let from_diff = ManifestDelta::between(&parent, &child, ckpt)
            .expect("the same pair is expressible as a delta either way");
        assert_eq!(from_span, from_diff);
        assert_eq!(from_span.apply(&parent), child);
    }

    #[test]
    fn a_rolled_back_commit_puts_the_handle_back_exactly_where_it_was() {
        // in-place mutation means a failure part-way has to be undone. This
        // exercises the guard directly: capture, make the mess a commit makes,
        // restore, and demand byte equality with what was captured.
        let root = tmp_root("commit-rollback");
        let mut s = NativeStore::open(&root).unwrap();
        commit_with(&mut s, 10, &["n1"]);
        commit_with(&mut s, 20, &["n2"]);

        let before = s.manifest().clone();
        let before_state = s.merkle.clone();
        let base = CommitBase::capture(&s.manifest, &s.merkle);

        // exactly the mutations `commit_inner` performs before it can fail
        {
            let m = &mut s.manifest;
            m.generation += 1;
            m.parent = Some(before.generation);
            m.created_tt = 999;
            m.manifest_sha = String::new();
            m.next_segment_id += 3;
            m.stats.n_node_versions += 7;
            m.dict.records += 2;
            m.event_log.offset += 64;
            let seg = crate::manifest::SegmentEntry {
                file: "seg/000000000099.tgs".into(),
                rows: 3,
                key_lo: (1, "a".into()),
                key_hi: (2, "b".into()),
                vt_min: 0,
                vt_max: 5,
                vt_e_max: 6,
                tt_s_min: 1,
                tt_s_max: 1,
                rel_codes: vec![1],
                n_closed_folded: 0,
                all_current: true,
                sha: "00000000000000ff".into(),
            };
            let state = s.merkle.as_mut().unwrap();
            state.push_node(&seg);
            m.node_store.push(seg.clone());
            state.push_edge_event(&seg);
            m.edge_lanes.event.push(seg.clone());
            let run = CloseRunRef {
                file: "close/000000000099.tgc".into(),
                entries: 1,
                sha: String::new(),
            };
            state.push_close_run(&run);
            m.close_runs.push(run);
            m.seal_with(s.merkle.as_ref().unwrap());
        }
        assert_ne!(s.manifest, before, "the fixture must actually dirty it");

        base.restore(&mut s.manifest, &mut s.merkle);
        assert_eq!(s.manifest, before);
        assert_eq!(s.merkle, before_state);
        assert_eq!(
            s.merkle.as_ref().unwrap(),
            &merkle::ManifestMerkle::from_manifest(&s.manifest)
        );

        // and the handle is still usable: the next commit is the one the
        // rolled-back attempt would have been
        let g = commit_with(&mut s, 30, &["n3"]);
        assert_eq!(g, before.generation + 1);
        assert!(s.verify().unwrap().is_healthy());
        drop(s);
        let re = NativeStore::open(&root).unwrap();
        assert_eq!(re.generation(), g);
    }

    #[test]
    fn compaction_and_gc_rebuild_the_state_rather_than_appending_to_it() {
        // both replace or truncate the segment set, so neither can extend a
        // spine. Both already force a checkpoint, so the rebuild is free —
        // what matters is that the handle is not left with a stale state.
        let root = tmp_root("rebuild-state");
        let mut s = NativeStore::open(&root).unwrap();
        for g in 1..=6i64 {
            commit_with(&mut s, g * 10, &[&format!("n{g}")]);
        }
        s.compact().unwrap();
        assert_eq!(
            s.merkle.as_ref().unwrap(),
            &merkle::ManifestMerkle::from_manifest(s.manifest()),
            "compaction left a stale Merkle state"
        );
        assert!(s.verify().unwrap().is_healthy());

        s.gc(2).unwrap();
        assert_eq!(
            s.merkle.as_ref().unwrap(),
            &merkle::ManifestMerkle::from_manifest(s.manifest())
        );
        commit_with(&mut s, 100, &["after"]);
        assert!(s.verify().unwrap().is_healthy());
        drop(s);
        assert!(NativeStore::open(&root)
            .unwrap()
            .verify()
            .unwrap()
            .is_healthy());
    }

    #[test]
    fn orphaned_genesis_manifest_alone_still_opens_fresh() {
        // exactly what a crash at `after_manifest` during the very first
        // `open` (publishing generation 0) leaves behind: the genesis
        // checkpoint on disk, no CURRENT, no segments, no dictionary tail.
        let root = tmp_root("current-missing-genesis-orphan");
        for sub in SUBDIRS {
            fs::create_dir_all(root.join(sub)).unwrap();
        }
        let genesis = Manifest::genesis();
        write_atomic(
            &NativeStore::manifest_path(&root, 0),
            &manifest_chain::checkpoint_json(&genesis),
        )
        .unwrap();
        assert!(!root.join("CURRENT").exists());

        let s = NativeStore::open(&root).unwrap();
        assert_eq!(s.generation(), 0);
        assert!(root.join("CURRENT").exists(), "the crash is healed");
        assert!(s.verify().unwrap().is_healthy());
    }

    #[test]
    fn populated_store_without_current_refuses_to_open() {
        let root = tmp_root("current-missing-populated");
        let mut s = NativeStore::open(&root).unwrap();
        commit_with(&mut s, 10, &["n1"]);
        commit_with(&mut s, 20, &["n2"]);
        drop(s);

        fs::remove_file(root.join("CURRENT")).unwrap();

        let err = open_err(&root);
        assert_eq!(err.category, crate::error::Category::Corrupt);
        assert!(err.message.contains("CURRENT"), "{}", err.message);
        assert!(
            err.remedy.as_deref().unwrap_or_default().contains("CURRENT"),
            "{:?}",
            err.remedy
        );
    }

    #[test]
    fn populated_store_with_only_segments_and_no_manifest_dir_entries_refuses() {
        // the manifests directory can itself still hold generation 0 (the
        // genesis checkpoint is not deleted by this scenario), but a real
        // segment on disk means the store is not "nothing published yet".
        let root = tmp_root("current-missing-with-segments");
        let mut s = NativeStore::open(&root).unwrap();
        commit_with(&mut s, 10, &["n1"]);
        drop(s);

        fs::remove_file(root.join("CURRENT")).unwrap();
        // even if every manifest after genesis vanished too, a segment file
        // alone is enough evidence that this store was not empty
        fs::remove_file(NativeStore::manifest_path(&root, 1)).unwrap();

        let err = open_err(&root);
        assert_eq!(err.category, crate::error::Category::Corrupt);
        assert!(err.message.contains("CURRENT"), "{}", err.message);
    }

    #[test]
    fn dictionary_tail_alone_is_enough_evidence_to_refuse() {
        // a store whose manifests and segments are both gone but whose
        // dictionary log still holds bytes: still not an empty directory.
        let root = tmp_root("current-missing-dict-only");
        for sub in SUBDIRS {
            fs::create_dir_all(root.join(sub)).unwrap();
        }
        fs::write(root.join(DICT), b"not actually a valid dict tail, just bytes").unwrap();

        let err = open_err(&root);
        assert_eq!(err.category, crate::error::Category::Corrupt);
        assert!(err.message.contains("CURRENT"), "{}", err.message);
    }

    // --- format 2: the manifest chain ------------------------- //

    fn record_of(root: &Path, g: u64) -> crate::manifest_chain::ManifestRecord {
        manifest_chain::read_record(root, g).unwrap()
    }

    /// `NativeStore` is not `Debug`, so `unwrap_err` cannot be used on it.
    fn open_err(root: &Path) -> EngineError {
        match NativeStore::open(root) {
            Ok(_) => panic!("a broken manifest chain must not open"),
            Err(e) => e,
        }
    }

    #[test]
    fn commits_write_deltas_and_a_checkpoint_every_k() {
        let root = tmp_root("chain-shape");
        let mut s = NativeStore::open(&root).unwrap();
        // a tiny K so the periodic checkpoint is reachable in a unit test
        s.set_checkpoint_every(Some(4));
        {
            for tt in 1..=9i64 {
                s.begin(tt * 10).unwrap();
                let a = s.ensure_entity(&format!("n{tt}"), "Node").unwrap();
                s.stage_edge(edge_row(a, a, tt, tt * 10, tt as u32)).unwrap();
                s.commit(EventLogRef::default()).unwrap();
            }
            assert_eq!(s.generation(), 9);
            assert_eq!(s.checkpoint_generation(), 8);

            let kinds: Vec<bool> = (0..=9).map(|g| record_of(&root, g).is_checkpoint()).collect();
            assert_eq!(
                kinds,
                vec![true, false, false, false, true, false, false, false, true, false],
                "generation 0 and every 4th generation are checkpoints"
            );

            // the delta really is the smaller document
            let delta_bytes = fs::metadata(NativeStore::manifest_path(&root, 9))
                .unwrap()
                .len();
            let ckpt_bytes = fs::metadata(NativeStore::manifest_path(&root, 8))
                .unwrap()
                .len();
            assert!(delta_bytes < ckpt_bytes, "{delta_bytes} vs {ckpt_bytes}");

            // and the phase record says which was written
            assert!(!s.last_commit_phases().unwrap().manifest_checkpoint);
        }
    }

    // --- commit-phase accounting helpers (shared by the two 60-commit
    // distribution tests below: the entity-only shape and the edge shape) -- //

    fn residual_bound(total_us: u64) -> u64 {
        if cfg!(debug_assertions) {
            std::cmp::max(1_000, total_us / 10) // max(1000us, 10%)
        } else {
            std::cmp::max(100, total_us / 50) // max(100us, 2%)
        }
    }

    fn median(mut xs: Vec<u64>) -> u64 {
        xs.sort_unstable();
        let n = xs.len();
        if n % 2 == 1 {
            xs[n / 2]
        } else {
            (xs[n / 2 - 1] + xs[n / 2]) / 2
        }
    }

    fn p90(mut xs: Vec<u64>) -> u64 {
        xs.sort_unstable();
        let n = xs.len();
        let idx = ((n as f64 - 1.0) * 0.9).round() as usize;
        xs[idx.min(n - 1)]
    }

    // The three worst commits by residual, for failure messages -- a
    // scheduler-hiccup failure and a systematic-leak failure look
    // different here (one outlier vs. three-in-a-row elevated values).
    fn worst_three(commits: &[(i64, u64, u64, u64)]) -> String {
        let mut v = commits.to_vec();
        v.sort_unstable_by_key(|a| std::cmp::Reverse(a.3));
        v.iter()
            .take(3)
            .map(|(idx, total, named_sum, residual)| {
                format!(
                    "commit {idx}: total_us={total} named_sum={named_sum} \
                     residual={residual}us"
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Sum of every phase `total_us` is supposed to account for -- the same
    /// set the pyo3 binding's `NAMED_PHASE_KEYS` covers in
    /// `tests/test_commit_phase_timing.py`.
    fn named_phase_sum(p: &CommitPhases) -> u64 {
        p.capture_us
            + p.seal_us
            + p.closes_us
            + p.stats_us
            + p.dict_us
            + p.digest_us
            + p.debug_verify_us
            + p.delta_build_us
            + p.manifest_us
            + p.current_us
            + p.vidfilter_us
    }

    #[test]
    fn commit_phases_fully_account_for_total_us_over_sixty_singleton_commits() {
        // An earlier diagnosis found that an untimed residual
        // (total_us minus the sum of every named phase) absorbed 100% of the
        // last/first-decile growth in engine-commit `total_us`, in every
        // format and configuration. This test adds `capture_us` (`CommitBase::
        // capture`), `digest_us` (`Manifest::seal_with`), and
        // `delta_build_us` (the delta-or-checkpoint JSON `publish` used to
        // build before its own timer started) to close that gap. This
        // asserts the invariant holds on *every* commit, not just on
        // average, over a run long enough to grow the segment/manifest
        // history the diagnosis's candidate mechanisms depend on.
        //
        // The CI runner (GitHub Actions, an unoptimized `cargo test` debug
        // build on a slow shared disk) cannot hold a 50us/2% bar: scheduler
        // and I/O jitter alone blow past it on an individual commit. A debug
        // build also pays real, non-representative overhead (e.g.
        // `store::publish`'s O(segments) `debug_assert_eq!`), so the bar
        // itself widens under `cfg!(debug_assertions)` -- this is about the
        // build profile, not the underlying instrumentation, which is
        // correct and unchanged.
        //
        // Even the widened bar is not CI-safe *per commit*: a shared runner
        // can preempt the test process for a scheduling quantum in the
        // middle of any single commit, inflating that one commit's residual
        // by an amount that has nothing to do with the engine. A hard
        // per-commit assertion therefore has an irreducible flake rate on
        // shared infrastructure no matter how wide the bar. What must not
        // regress is the *distribution*: a genuine, systematic accounting
        // leak shows up on (nearly) every commit, so it moves the median and
        // the tail together, and it grows with history size, so it moves the
        // back of the run relative to the front. A single preempted commit
        // can only move one point in the distribution and cannot manufacture
        // a first-to-last trend, so median/p90/no-growth checks catch a real
        // leak while tolerating scheduler noise.
        let root = tmp_root("phase-accounting");
        let mut s = NativeStore::open(&root).unwrap();
        let mut residuals = Vec::with_capacity(60);
        let mut totals = Vec::with_capacity(60);
        let mut commits: Vec<(i64, u64, u64, u64)> = Vec::with_capacity(60);
        for tt in 1..=60i64 {
            let uid = format!("n{tt}");
            let g = commit_with(&mut s, tt, &[uid.as_str()]);
            assert_eq!(g, tt as u64);
            let p = s.last_commit_phases().unwrap();
            let named_sum = p.capture_us
                + p.seal_us
                + p.closes_us
                + p.stats_us
                + p.dict_us
                + p.digest_us
                + p.debug_verify_us
                + p.delta_build_us
                + p.manifest_us
                + p.current_us
                + p.vidfilter_us;
            assert!(
                p.total_us >= named_sum,
                "commit {tt}: named phases ({named_sum}us) exceed total_us \
                 ({}us) -- a phase is double-counting another's window",
                p.total_us
            );
            let residual = p.total_us - named_sum;
            residuals.push(residual);
            totals.push(p.total_us);
            commits.push((tt, p.total_us, named_sum, residual));
        }

        let bar = residual_bound(median(totals.clone()));
        let median_residual = median(residuals.clone());
        let p90_residual = p90(residuals.clone());
        let first_ten_median = median(residuals[..10].to_vec());
        let last_ten_median = median(residuals[residuals.len() - 10..].to_vec());

        assert!(
            median_residual <= bar,
            "median residual over 60 commits ({median_residual}us) exceeds \
             bar={bar}us -- a systematic accounting leak, not a one-off \
             scheduler hiccup (worst three: {})",
            worst_three(&commits)
        );
        assert!(
            p90_residual <= 3 * bar,
            "p90 residual over 60 commits ({p90_residual}us) exceeds \
             3*bar={}us -- too many commits are elevated for this to be a \
             single scheduler hiccup (worst three: {})",
            3 * bar,
            worst_three(&commits)
        );
        assert!(
            last_ten_median <= first_ten_median + bar,
            "median residual grew from {first_ten_median}us (first 10 commits) \
             to {last_ten_median}us (last 10 commits), more than bar={bar}us \
             -- consistent with an O(segments) leak, not scheduler noise \
             (worst three: {})",
            worst_three(&commits)
        );
    }

    #[test]
    fn commit_phases_fully_account_for_total_us_over_sixty_singleton_edge_commits() {
        // Why this exists alongside the entity-only sibling above: that test's
        // `commit_with` helper only calls `ensure_entity`, which touches the
        // dictionary and stages no rows, so *every one of its 60 commits seals
        // zero segments* (`segments_named` stays 0) and `publish`'s
        // `debug_assert_eq!` manifest recompute -- `body_sha_canonical` ->
        // `Manifest::digest` -> `ManifestMerkle::from_manifest`, O(all segment
        // entries across the four lanes) -- stays O(1) there. It therefore
        // cannot observe an O(segments) accounting leak at all: the shape that
        // exposed one in a commit-cost comparison was `Store::ingest_events`, which stages an
        // edge row plus node rows per commit and so seals segments every
        // generation. This test is that shape, with the same distribution
        // invariant, so the O(segments) recompute is provably exercised (see
        // the `segments_named` assertion at the end) and `debug_verify_us`
        // must be the phase that absorbs it.
        let root = tmp_root("phase-accounting-edges");
        let mut s = NativeStore::open(&root).unwrap();
        let mut residuals = Vec::with_capacity(60);
        let mut totals = Vec::with_capacity(60);
        let mut debug_verifies = Vec::with_capacity(60);
        let mut commits: Vec<(i64, u64, u64, u64)> = Vec::with_capacity(60);
        let mut last_segments_named = 0u64;
        for tt in 1..=60i64 {
            s.begin(tt).unwrap();
            // both endpoints registered first so the staged rows reference
            // dense ids that exist, exactly as `ingest_events` does
            let src = s.ensure_entity(&format!("n{tt}"), "Node").unwrap();
            let dst = s.ensure_entity(&format!("n{}", tt + 1), "Node").unwrap();
            // one node row (the newly-seen endpoint) -> one node segment
            s.stage_node(NodeRow {
                vid: crate::derive::version_vid(&format!("n{}", tt + 1), tt, tt),
                uid_id: dst,
                label: "Node".into(),
                vt_s: tt,
                vt_e: tt + 1,
                tt_s: tt,
                props: "{}".into(),
                source: "ingest".into(),
                provenance_ref: None,
            })
            .unwrap();
            // one edge row -> one edge-event segment
            s.stage_edge(edge_row(src, dst, tt, tt, tt as u32)).unwrap();
            let prev = s.manifest().event_log.chain.clone();
            let g = s
                .commit(EventLogRef {
                    offset: tt as u64,
                    chain: EventLogRef::extend_chain(
                        &prev,
                        format!("{{\"tt\":{tt}}}\n").as_bytes(),
                    ),
                })
                .unwrap();
            assert_eq!(g, tt as u64);

            let p = s.last_commit_phases().unwrap();
            let named_sum = named_phase_sum(&p);
            assert!(
                p.total_us >= named_sum,
                "commit {tt}: named phases ({named_sum}us) exceed total_us \
                 ({}us) -- a phase is double-counting another's window",
                p.total_us
            );
            // the debug-only manifest recompute is timed, and compiles away
            // entirely in release -- a nonzero reading there would mean the
            // field had picked up someone else's window
            if !cfg!(debug_assertions) {
                assert_eq!(
                    p.debug_verify_us, 0,
                    "commit {tt}: debug_verify_us must be 0 in a release build, \
                     where publish's debug_assert_eq! does not compile"
                );
            }
            residuals.push(p.total_us - named_sum);
            totals.push(p.total_us);
            debug_verifies.push(p.debug_verify_us);
            commits.push((tt, p.total_us, named_sum, p.total_us - named_sum));
            last_segments_named = p.segments_named;
        }

        // the point of this shape: two segments sealed per generation, so the
        // manifest the debug recompute walks really did grow to 120 entries
        assert_eq!(
            last_segments_named, 120,
            "each of the 60 commits must seal one node and one edge segment \
             for this test to exercise the O(segments) manifest recompute"
        );
        if cfg!(debug_assertions) {
            // a from-scratch Merkle over 120 segment entries is far above 1us
            // in an unoptimized build; measured at ~2000us on the last commit
            assert!(
                debug_verifies[59] > 0,
                "debug_verify_us must be nonzero on the last commit of a debug \
                 build -- publish recomputes the whole manifest digest there \
                 over {last_segments_named} segment entries"
            );
        }

        let bar = residual_bound(median(totals.clone()));
        let median_residual = median(residuals.clone());
        let p90_residual = p90(residuals.clone());
        let first_ten_median = median(residuals[..10].to_vec());
        let last_ten_median = median(residuals[residuals.len() - 10..].to_vec());

        assert!(
            median_residual <= bar,
            "median residual over 60 edge commits ({median_residual}us) exceeds \
             bar={bar}us -- a systematic accounting leak, not a one-off \
             scheduler hiccup (worst three: {})",
            worst_three(&commits)
        );
        assert!(
            p90_residual <= 3 * bar,
            "p90 residual over 60 edge commits ({p90_residual}us) exceeds \
             3*bar={}us -- too many commits are elevated for this to be a \
             single scheduler hiccup (worst three: {})",
            3 * bar,
            worst_three(&commits)
        );
        assert!(
            last_ten_median <= first_ten_median + bar,
            "median residual grew from {first_ten_median}us (first 10 commits) \
             to {last_ten_median}us (last 10 commits), more than bar={bar}us \
             -- consistent with an O(segments) leak, not scheduler noise \
             (worst three: {})",
            worst_three(&commits)
        );
    }

    #[test]
    fn a_reconstructed_generation_equals_the_full_document_for_the_same_ops() {
        // the load-bearing equivalence: replaying deltas must land on exactly
        // the manifest a checkpoint at that generation would have held
        let root = tmp_root("chain-equals-full");
        let mut s = NativeStore::open(&root).unwrap();
        s.set_checkpoint_every(Some(1_000_000)); // deltas all the way down
        {
            let mut rows = Vec::new();
            for tt in 1..=6i64 {
                s.begin(tt * 10).unwrap();
                let a = s.ensure_entity(&format!("n{tt}"), "Node").unwrap();
                let r = edge_row(a, a, tt, tt * 10, tt as u32);
                rows.push(r.clone());
                s.stage_edge(r).unwrap();
                if tt == 4 {
                    s.close_version(RowKind::Edge, rows[0].vid, tt * 10).unwrap();
                }
                s.commit(EventLogRef::default()).unwrap();
            }
            // every generation is a delta except genesis
            assert_eq!(s.checkpoint_generation(), 0);
            for g in 1..=6 {
                assert!(!record_of(&root, g).is_checkpoint());
            }

            let resolved = manifest_chain::reconstruct(&root, 6).unwrap();
            assert_eq!(resolved.checkpoint, 0);
            assert_eq!(resolved.deltas, 6);
            assert_eq!(&resolved.manifest, s.manifest());
            // and the sha is the one CURRENT names, computed the format-1 way
            assert_eq!(resolved.manifest.digest(), s.manifest().manifest_sha);
            assert_eq!(
                fs::read_to_string(root.join(CURRENT)).unwrap().trim(),
                format!("6 {}", s.manifest().manifest_sha)
            );

            drop(s);
            let re = NativeStore::open(&root).unwrap();
            assert_eq!(re.generation(), 6);
            assert_eq!(re.all_edge_versions().unwrap().len(), 6);
            let report = re.verify().unwrap();
            assert!(report.is_healthy(), "{:?}", report.problems);
            assert_eq!(report.manifest_deltas, 6);
            assert_eq!(report.manifest_format, crate::MANIFEST_FORMAT_VERSION);
        }
    }

    #[test]
    fn a_break_anywhere_in_the_chain_is_corruption() {
        let root = tmp_root("chain-break");
        let mut s = NativeStore::open(&root).unwrap();
        s.set_checkpoint_every(Some(1_000_000)); // deltas all the way down
        {
            for tt in 1..=4i64 {
                commit_with(&mut s, tt * 10, &[&format!("n{tt}")]);
            }
            drop(s);
            let intact: Vec<String> = (0..=4)
                .map(|g| fs::read_to_string(NativeStore::manifest_path(&root, g)).unwrap())
                .collect();
            let restore = || {
                for (g, text) in intact.iter().enumerate() {
                    fs::write(NativeStore::manifest_path(&root, g as u64), text).unwrap();
                }
            };

            // (a) a gap: generation 2 is gone
            fs::remove_file(NativeStore::manifest_path(&root, 2)).unwrap();
            let err = open_err(&root);
            assert_eq!(err.category, crate::error::Category::Corrupt);
            restore();

            // (b) a reordered chain: generation 2 is replaced by 3's record,
            //     so the parent link and the running sha disagree
            fs::write(NativeStore::manifest_path(&root, 2), &intact[3]).unwrap();
            let err = open_err(&root);
            assert_eq!(err.category, crate::error::Category::Corrupt);
            restore();

            // (c) a tampered parent_sha, resealed so delta_sha still passes:
            //     the chain check has to be the thing that catches it
            let mut d: crate::manifest_chain::ManifestDelta =
                serde_json::from_str(&intact[3]).unwrap();
            d.parent_sha = "dead0000dead0000".into();
            d.seal();
            d.verify_self().unwrap();
            fs::write(NativeStore::manifest_path(&root, 3), d.to_json()).unwrap();
            let err = open_err(&root);
            assert_eq!(err.category, crate::error::Category::Corrupt);
            assert!(
                err.message.contains("chain broken"),
                "unhelpful message: {}",
                err.message
            );
            restore();

            // (d) a flipped byte inside a delta: the record's own sha
            let poisoned = intact[3].replace("\"next_segment_id\": 0", "\"next_segment_id\": 7");
            assert_ne!(poisoned, intact[3]);
            fs::write(NativeStore::manifest_path(&root, 3), poisoned).unwrap();
            assert_eq!(
                open_err(&root).category,
                crate::error::Category::Corrupt
            );
            restore();

            // and after every restore the store is healthy again
            assert!(NativeStore::open(&root).unwrap().verify().unwrap().is_healthy());
        }
    }

    #[test]
    fn compaction_always_emits_a_checkpoint() {
        let root = tmp_root("chain-compact");
        let mut s = NativeStore::open(&root).unwrap();
        s.set_checkpoint_every(Some(1_000_000)); // deltas all the way down
        {
            for tt in 1..=3i64 {
                s.begin(tt * 10).unwrap();
                let a = s.ensure_entity("n1", "Node").unwrap();
                s.stage_edge(edge_row(a, a, tt, tt * 10, tt as u32)).unwrap();
                s.commit(EventLogRef::default()).unwrap();
            }
            assert!(!record_of(&root, 3).is_checkpoint());
            s.compact().unwrap();
            let g = s.generation();
            assert!(
                record_of(&root, g).is_checkpoint(),
                "compaction replaces the whole segment list; the delta would \
                 be no smaller, and the chain should reset here"
            );
            assert_eq!(s.checkpoint_generation(), g);
            assert!(s.verify().unwrap().is_healthy());
        }
    }

    #[test]
    fn a_format_1_store_opens_read_only_and_refuses_writes_with_a_remedy() {
        let root = tmp_root("legacy-open");
        // a store exactly as the pre-change engine left it: one untagged,
        // format-1 document per generation
        fs::create_dir_all(root.join("manifests")).unwrap();
        for sub in SUBDIRS {
            fs::create_dir_all(root.join(sub)).unwrap();
        }
        let mut legacy = Manifest::genesis();
        legacy.format = crate::FORMAT_LEGACY;
        legacy.seal();
        write_atomic(
            &NativeStore::manifest_path(&root, 0),
            &serde_json::to_string_pretty(&legacy).unwrap(),
        )
        .unwrap();
        write_atomic(
            &root.join(CURRENT),
            &format!("0 {}\n", legacy.manifest_sha),
        )
        .unwrap();

        let mut s = NativeStore::open(&root).unwrap();
        assert_eq!(s.manifest_format(), crate::FORMAT_LEGACY);
        assert!(s.verify().unwrap().is_healthy(), "reads must still work");

        s.begin(10).unwrap();
        s.ensure_entity("n1", "Node").unwrap();
        let err = s.commit(EventLogRef::default()).unwrap_err();
        assert_eq!(err.category, crate::error::Category::Invariant);
        assert!(err.message.contains("read-only"), "{}", err.message);
        assert!(
            err.remedy
                .as_deref()
                .unwrap_or_default()
                .contains("upgrade-manifests"),
            "a refusal without a remedy is a dead end: {:?}",
            err.remedy
        );
        s.rollback().unwrap();
        assert!(s.gc(2).is_err(), "gc is a write path too");

        // the upgrade converts it, and then writing works
        let report = s.upgrade_manifests().unwrap();
        assert!(report.upgraded);
        assert_eq!(report.from_format, crate::FORMAT_LEGACY);
        assert_eq!(s.manifest_format(), crate::MANIFEST_FORMAT_VERSION);
        assert!(record_of(&root, s.generation()).is_checkpoint());
        assert!(s.verify().unwrap().is_healthy());
        // idempotent
        assert!(!s.upgrade_manifests().unwrap().upgraded);
        commit_with(&mut s, 20, &["n1"]);
        drop(s);
        assert_eq!(NativeStore::open(&root).unwrap().dict().len(), 1);
    }

    #[test]
    fn the_upgrade_preserves_the_generation_content_verbatim() {
        let root = tmp_root("legacy-upgrade");
        for sub in SUBDIRS {
            fs::create_dir_all(root.join(sub)).unwrap();
        }
        // build a real store, then rewrite its head as a format-1 document —
        // the same logical content the old engine would have written
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(10).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        s.stage_edge(edge_row(a, a, 1, 10, 0)).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        let content = s.manifest().clone();
        let before = s.verify().unwrap();
        drop(s);

        let mut legacy = content.clone();
        legacy.format = crate::FORMAT_LEGACY;
        legacy.seal();
        write_atomic(
            &NativeStore::manifest_path(&root, legacy.generation),
            &serde_json::to_string_pretty(&legacy).unwrap(),
        )
        .unwrap();
        write_atomic(
            &root.join(CURRENT),
            &format!("{} {}\n", legacy.generation, legacy.manifest_sha),
        )
        .unwrap();

        let mut s = NativeStore::open(&root).unwrap();
        assert_eq!(s.manifest_format(), crate::FORMAT_LEGACY);
        s.upgrade_manifests().unwrap();
        let after = s.verify().unwrap();

        // everything the report says about the *data* is identical; only the
        // generation counter, the format, and the chain fields move
        assert_eq!(after.segments_checked, before.segments_checked);
        assert_eq!(after.rows, before.rows);
        assert_eq!(after.closes, before.closes);
        assert_eq!(after.dict_records, before.dict_records);
        assert_eq!(after.problems, before.problems);
        assert_eq!(after.manifest_format, crate::MANIFEST_FORMAT_VERSION);
        assert_eq!(after.manifest_deltas, 0);
        let m = s.manifest();
        assert_eq!(m.node_store, content.node_store);
        assert_eq!(m.edge_lanes, content.edge_lanes);
        assert_eq!(m.close_runs, content.close_runs);
        assert_eq!(m.stats, content.stats);
        assert_eq!(m.dict, content.dict);
        assert_eq!(m.next_segment_id, content.next_segment_id);
        assert_eq!(m.event_log, content.event_log);
        assert_eq!(s.all_edge_versions().unwrap().len(), 1);
    }

    #[test]
    fn a_format_2_store_opens_read_only_and_upgrades_to_format_3() {
        // format 3 changes only what `manifest_sha` *is*, so a format-2 chain
        // must keep verifying under its own rule and refuse every write until
        // it is converted — the same promise format 1 got, one format later
        let root = tmp_root("format2-upgrade");
        for sub in SUBDIRS {
            fs::create_dir_all(root.join(sub)).unwrap();
        }
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(10).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        s.stage_edge(edge_row(a, a, 1, 10, 0)).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        let content = s.manifest().clone();
        let before = s.verify().unwrap();
        drop(s);

        // the head as the previous engine wrote it: format 2, tagged
        // checkpoint, `manifest_sha` the sha of the whole document
        let mut legacy = content.clone();
        legacy.format = 2;
        legacy.seal();
        assert_ne!(
            legacy.manifest_sha, content.manifest_sha,
            "the same content under the two rules must not share a digest"
        );
        write_atomic(
            &NativeStore::manifest_path(&root, legacy.generation),
            &manifest_chain::checkpoint_json(&legacy),
        )
        .unwrap();
        write_atomic(
            &root.join(CURRENT),
            &format!("{} {}\n", legacy.generation, legacy.manifest_sha),
        )
        .unwrap();

        let mut s = NativeStore::open(&root).unwrap();
        assert_eq!(s.manifest_format(), 2);
        assert!(s.merkle.is_none(), "a read-only store maintains no state");
        // reads and verification work
        let report = s.verify().unwrap();
        assert!(report.is_healthy(), "{:?}", report.problems);
        assert_eq!(report.manifest_format, 2);
        assert_eq!(s.all_edge_versions().unwrap().len(), 1);
        // writes do not
        let err = s.gc(1).unwrap_err();
        assert_eq!(err.category, crate::error::Category::Invariant);
        assert!(err.message.contains("format-2"), "{}", err.message);
        assert!(err
            .remedy
            .as_deref()
            .unwrap_or_default()
            .contains("upgrade-manifests"));

        let up = s.upgrade_manifests().unwrap();
        assert!(up.upgraded);
        assert_eq!(up.from_format, 2);
        assert_eq!(s.manifest_format(), crate::MANIFEST_FORMAT_VERSION);
        assert!(s.merkle.is_some());
        let after = s.verify_full().unwrap();
        assert!(after.is_healthy(), "{:?}", after.problems);
        assert_eq!(after.segments_checked, before.segments_checked);
        assert_eq!(after.rows, before.rows);
        assert_eq!(after.manifest_deltas, 0);
        assert_eq!(s.manifest().node_store, content.node_store);
        assert_eq!(s.manifest().edge_lanes, content.edge_lanes);
        assert_eq!(s.all_edge_versions().unwrap().len(), 1);
        // and writing works now
        commit_with(&mut s, 30, &["n2"]);
        assert!(s.verify_full().unwrap().is_healthy());
    }

    #[test]
    fn verify_full_re_derives_the_published_digest_from_scratch() {
        // An incrementally maintained digest could be wrong and
        // self-consistent. The oracle is a recomputation that shares no code
        // with the incremental path, crossed against what CURRENT publishes.
        let root = tmp_root("digest-oracle");
        let mut s = NativeStore::open(&root).unwrap();
        for g in 1..=4i64 {
            commit_with(&mut s, g * 10, &[&format!("n{g}")]);
        }
        let clean = s.verify_full().unwrap();
        assert!(clean.is_healthy(), "{:?}", clean.problems);

        // stand in for a bad incremental update: the value published is not
        // the value a from-scratch recomputation of this generation gives
        write_atomic(
            &root.join(CURRENT),
            &format!("{} 0000000000000000\n", s.generation()),
        )
        .unwrap();
        let report = s.verify_full().unwrap();
        assert!(!report.is_healthy());
        assert!(
            report
                .findings
                .iter()
                .any(|f| f.kind == "digest-oracle-mismatch"),
            "{:?}",
            report.problems
        );
        // the fast pass does not make this check — it is the price of an
        // incremental digest, paid in the mode nobody runs per commit
        assert!(s.verify().unwrap().is_healthy());
    }

    #[test]
    fn crash_after_dict_append_before_manifest_leaves_the_tail_invisible() {
        let root = tmp_root("crash-dict");
        let mut s = NativeStore::open(&root).unwrap();
        commit_with(&mut s, 10, &["n1"]);
        let committed_bytes = s.manifest().dict.bytes;
        drop(s);

        // simulate: a later batch appended to dict.log and died pre-manifest
        let mut d = Dictionary::open(root.join(DICT), 1, committed_bytes).unwrap();
        d.ensure("orphan", "Node").unwrap();
        d.commit_to_disk().unwrap();
        let orphaned_len = fs::metadata(root.join(DICT)).unwrap().len();
        assert!(orphaned_len > committed_bytes);

        // the orphan is invisible: the manifest's byte count is the only
        // authority on what the dictionary contains
        let mut re = NativeStore::open(&root).unwrap();
        assert_eq!(re.generation(), 1);
        assert_eq!(re.dict().len(), 1);
        assert_eq!(re.dict().dense_id("orphan"), None);

        // and the next commit reclaims those bytes by overwriting them —
        // open does not truncate, because open must not mutate the store
        // (a *live* writer's tail looks identical to a dead one's)
        commit_with(&mut re, 20, &["n2"]);
        assert_eq!(
            fs::metadata(root.join(DICT)).unwrap().len(),
            re.manifest().dict.bytes,
            "the writer rewrites from its committed offset and trims the rest"
        );
        assert_eq!(re.dict().dense_id("orphan"), None);
        assert_eq!(re.dict().dense_id("n2"), Some(1));
        drop(re);
        assert_eq!(NativeStore::open(&root).unwrap().dict().len(), 2);
    }

    /// Opening a store must not mutate it.
    ///
    /// The single writer spends a window inside every commit between step 3
    /// (the dictionary tail is fsynced) and step 5 (`CURRENT` flips). In that
    /// window `dict.log` is longer than the published generation claims —
    /// byte-for-byte indistinguishable from the orphaned tail a crashed batch
    /// leaves behind. A reader process that opens there and "cleans up" the
    /// tail destroys bytes the writer has already made durable and is about
    /// to name: the generation the writer then publishes is unreadable, so a
    /// commit that returned successfully has lost its durability guarantee to
    /// an unrelated reader. Every mutation must be scoped, and
    /// the cheapest scoping is not mutating at all.
    #[test]
    fn a_reader_opening_mid_commit_cannot_brick_the_generation_being_published() {
        let root = tmp_root("open-midcommit");
        let mut w = NativeStore::open(&root).unwrap();
        commit_with(&mut w, 10, &["n1"]);
        let base = w.manifest().dict.bytes;

        // the writer is inside commit(): step 3 has fsynced the tail for the
        // generation it is about to publish; steps 4-5 have not run
        let mut tail = Dictionary::open(root.join(DICT), 1, base).unwrap();
        tail.ensure("n2", "Node").unwrap();
        let (records, bytes) = tail.commit_to_disk().unwrap();
        assert!(bytes > base);

        // a reader process opens the store in exactly that window
        let reader = NativeStore::open(&root).unwrap();
        assert_eq!(reader.generation(), 1);
        assert_eq!(reader.dict().len(), 1, "a reader sees its own generation");
        drop(reader);
        assert_eq!(
            fs::metadata(root.join(DICT)).unwrap().len(),
            bytes,
            "a reader deleted a live writer's fsynced dictionary tail"
        );

        // steps 4-5 complete: what the writer promised is durable
        let mut next = w.manifest().successor(20);
        next.dict.records = records;
        next.dict.bytes = bytes;
        next.seal();
        let span = AppendSpan::of(w.manifest());
        NativeStore::publish(&root, Some((&span, 0)), &next, false, 512).unwrap();

        let re = NativeStore::open(&root).unwrap();
        assert_eq!(re.generation(), 2);
        assert_eq!(re.dict().len(), 2);
        assert_eq!(re.dict().dense_id("n2"), Some(1));
    }

    #[test]
    fn current_pointing_at_a_missing_manifest_is_corruption() {
        let root = tmp_root("missing-manifest");
        let mut s = NativeStore::open(&root).unwrap();
        commit_with(&mut s, 10, &["n1"]);
        drop(s);
        fs::remove_file(NativeStore::manifest_path(&root, 1)).unwrap();

        let err = match NativeStore::open(&root) {
            Ok(_) => panic!("must not open with a dangling CURRENT"),
            Err(e) => e,
        };
        assert_eq!(err.category, crate::error::Category::Corrupt);
        assert!(err.remedy.is_some(), "corruption must name a remedy");
    }

    #[test]
    fn tampered_manifest_is_rejected() {
        let root = tmp_root("tampered");
        let mut s = NativeStore::open(&root).unwrap();
        commit_with(&mut s, 10, &["n1"]);
        drop(s);

        let path = NativeStore::manifest_path(&root, 1);
        let text = fs::read_to_string(&path).unwrap();
        fs::write(&path, text.replace("\"n_entities\": 1", "\"n_entities\": 4")).unwrap();

        assert!(NativeStore::open(&root).is_err(), "checksum must catch edits");
    }

    #[test]
    fn malformed_current_is_corruption() {
        let root = tmp_root("bad-current");
        NativeStore::open(&root).unwrap();
        fs::write(root.join(CURRENT), "garbage\n").unwrap();
        let err = match NativeStore::open(&root) {
            Ok(_) => panic!("malformed CURRENT must not open"),
            Err(e) => e,
        };
        assert_eq!(err.category, crate::error::Category::Corrupt);
    }

    // --- byte-budget segment cache ------------------------------ //

    #[test]
    fn cache_budget_resolution() {
        // explicit override wins, 0 means unbounded, garbage falls through
        assert_eq!(cache_budget(Some("1048576"), Some(1 << 33)), Some(1 << 20));
        assert_eq!(cache_budget(Some("0"), Some(1 << 33)), None);
        assert_eq!(cache_budget(Some(" 42 "), None), Some(42));
        assert_eq!(cache_budget(Some("lots"), Some(1 << 33)), Some(1 << 32));
        assert_eq!(cache_budget(Some(""), None), None);
        // default: half of detected RAM, unbounded where undetectable
        assert_eq!(cache_budget(None, Some(1 << 33)), Some(1 << 32));
        assert_eq!(cache_budget(None, None), None);
    }

    /// The *default* budget is capped, the override is not.
    ///
    /// The RAM figures are parameters of `cache_budget`, not of the host, so
    /// this pins the rule on any machine the suite runs on — including the
    /// laptops where `RAM/2` never reaches the cap and the behaviour change
    /// would otherwise be invisible.
    #[test]
    fn the_default_cache_budget_is_capped_but_an_explicit_budget_is_not() {
        let cap = crate::defaults::SEGMENT_CACHE_MAX_BYTES;
        assert_eq!(cap, 32 * 1024 * 1024 * 1024, "the cap is 32 GiB");

        // 515.65 GB, the node that produced build-record-100m.json: the old
        // default implied ~251 GB of segment cache, and only the job script's
        // pinned 46.5 GB kept the measured floor at 107.3 GB.
        let b7_node = 515_650_000_000u64;
        assert_eq!(cache_budget(None, Some(b7_node)), Some(cap));
        // 188 GB on another workstation: also capped.
        assert_eq!(cache_budget(None, Some(188_000_000_000)), Some(cap));
        // exactly at the boundary — 64 GiB of RAM is the last size where the
        // old and new defaults agree
        assert_eq!(cache_budget(None, Some(cap * 2)), Some(cap));
        // below it the cap never binds: a 16 GiB laptop still gets RAM/2
        assert_eq!(cache_budget(None, Some(16 << 30)), Some(8 << 30));
        // an explicit override above the cap is honoured verbatim, which is
        // what lets a job reproduce a pre-cap measurement
        assert_eq!(
            cache_budget(Some("46500000000"), Some(b7_node)),
            Some(46_500_000_000)
        );
        assert_eq!(cache_budget(Some("0"), Some(b7_node)), None);
    }

    /// Results must be byte-identical under any budget: an evicted segment
    /// reopens transparently, and the checksum walk stays once-per-session.
    ///
    /// Residency is driven by explicit `open_segment` calls rather than by a
    /// whole-store walk. It used to be driven by `all_edge_versions`, which
    /// filled the cache as a side effect; that walk was made uncached,
    /// which is a property this file's own
    /// `whole_store_walks_stream_while_point_reads_still_cache` in `read.rs`
    /// now pins. What this test is about — answers not depending on
    /// residency — is unchanged, and the walk being budget-independent
    /// strengthens rather than weakens it.
    #[test]
    fn tiny_cache_budget_changes_memory_not_answers() {
        let root = tmp_root("cache-budget");
        let mut s = NativeStore::open(&root).unwrap();
        for batch in 0..6u32 {
            let tt = 100 + batch as i64;
            s.begin(tt).unwrap();
            let a = s.ensure_entity("n1", "Node").unwrap();
            let b = s.ensure_entity("n2", "Node").unwrap();
            for i in 0..50u32 {
                s.stage_edge(edge_row(a, b, (batch * 50 + i) as i64, tt, batch * 50 + i))
                    .unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        assert!(s.manifest().edge_lanes.event.len() >= 6);

        let unbounded: Vec<_> = s.all_edge_versions().unwrap();
        let files: Vec<String> = s
            .manifest()
            .edge_lanes
            .event
            .iter()
            .map(|e| e.file.clone())
            .collect();
        for f in &files {
            s.open_segment(f).unwrap();
        }
        let (entries, bytes, _, evictions) = s.segment_cache_stats();
        assert_eq!(entries, 6, "unbounded: every touched segment stays");
        assert!(bytes > 0);
        assert_eq!(evictions, 0);

        // one segment's worth of budget: touching them all must evict as it goes
        let one = s.open_segment(&files[0]).unwrap().resident_bytes();
        s.set_segment_cache_budget(Some(one));
        for f in &files {
            s.open_segment(f).unwrap();
        }
        let capped: Vec<_> = s.all_edge_versions().unwrap();
        assert_eq!(capped, unbounded, "answers must not depend on residency");

        let (entries, bytes, budget, evictions) = s.segment_cache_stats();
        assert!(evictions > 0, "a one-segment budget must have evicted");
        assert!(entries < 6, "the cache cannot hold every segment");
        assert!(
            bytes <= budget.unwrap() || entries == 1,
            "over budget with multiple entries: {bytes} of {budget:?}"
        );

        // and point reads through the postings still agree after evictions
        let eid = unbounded[0].eid.clone();
        let vids: Vec<String> = s
            .believed_edge_versions(&eid, OPEN_END)
            .unwrap()
            .iter()
            .map(|r| r.vid.clone())
            .collect();
        s.set_segment_cache_budget(None);
        let vids_unbounded: Vec<String> = s
            .believed_edge_versions(&eid, OPEN_END)
            .unwrap()
            .iter()
            .map(|r| r.vid.clone())
            .collect();
        assert_eq!(vids, vids_unbounded);
    }

    #[test]
    fn an_arc_held_across_an_eviction_stays_valid() {
        let root = tmp_root("cache-arc");
        let mut s = NativeStore::open(&root).unwrap();
        for batch in 0..3u32 {
            let tt = 100 + batch as i64;
            s.begin(tt).unwrap();
            let a = s.ensure_entity("n1", "Node").unwrap();
            s.stage_edge(edge_row(a, a, batch as i64, tt, batch)).unwrap();
            s.commit(EventLogRef::default()).unwrap();
        }
        let file = s.manifest().edge_lanes.event[0].file.clone();
        let held = s.open_segment(&file).unwrap();
        let vt_before = held.i64_column("vt_s").unwrap().to_vec();

        s.set_segment_cache_budget(Some(1)); // evicts everything but the MRU
        for e in &s.manifest().edge_lanes.event.clone() {
            s.open_segment(&e.file).unwrap();
        }
        let (_, _, _, evictions) = s.segment_cache_stats();
        assert!(evictions > 0);
        // the reader's view is untouched: eviction drops the cache's
        // reference, never the data under a live Arc
        assert_eq!(held.i64_column("vt_s").unwrap(), &vt_before[..]);
    }

    #[test]
    fn batches_do_not_nest_and_time_must_advance() {
        let root = tmp_root("batch-rules");
        let mut s = NativeStore::open(&root).unwrap();
        commit_with(&mut s, 10, &["n1"]);
        s.begin(20).unwrap();
        assert!(s.begin(21).is_err(), "single-writer: no nested batches");
        s.rollback().unwrap();
        assert!(s.begin(10).is_err(), "tt must be strictly monotone");
        assert!(s.begin(11).is_ok());
    }

    // --- per-segment TCSR traversal ------------------------------------ //

    /// Two batches, so the store holds two edge segments and the traversal
    /// has to merge across them. Vertex ids are the dictionary's dense ids
    /// for `n0..n{n-1}`.
    fn tcsr_store(root: &Path, n: u32) -> NativeStore {
        let mut s = NativeStore::open(root).unwrap();
        let mut ids = Vec::new();
        s.begin(100).unwrap();
        for i in 0..n {
            ids.push(s.ensure_entity(&format!("n{i}"), "Node").unwrap());
        }
        // batch 1: a ring at even vt
        for i in 0..n {
            let e = edge_row(ids[i as usize], ids[((i + 1) % n) as usize], 2 * i as i64, 100, i);
            s.stage_edge(e).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();
        // batch 2: a chord at odd vt, interleaved with batch 1's vt values
        s.begin(200).unwrap();
        for i in 0..n {
            let e = edge_row(ids[i as usize], ids[((i + 2) % n) as usize], 2 * i as i64 + 1, 200, 100 + i);
            s.stage_edge(e).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();
        s
    }

    /// The believed `(vt_s, vid, src, dst, seg_id, seg_row)` of every edge
    /// row in the store, straight from the segments — the oracle the index
    /// must agree with.
    fn all_edge_rows(s: &NativeStore, as_of: i64) -> Vec<(i64, Id96, u32, u32, u64, u32)> {
        let closes = s.close_index().unwrap();
        let mut out = Vec::new();
        for (file, id) in s.tcsr_edge_files() {
            let seg = s.open_segment(&file).unwrap();
            let tt_s = seg.tt_s_view().unwrap();
            let sidecar = seg.sidecar();
            let vt_s = seg.i64_column("vt_s").unwrap();
            let src = seg.u32_column("src_id").unwrap();
            let dst = seg.u32_column("dst_id").unwrap();
            let vid64 = seg.u64_column("vid64").unwrap();
            let lo = seg.u32_column("vid_lo32").unwrap();
            for r in 0..seg.rows() {
                let i = r as usize;
                if tt_s.at(r).unwrap() > as_of {
                    continue;
                }
                let from_run = closes.tt_e(id, r);
                let tt_e = if from_run != OPEN_END { from_run } else { sidecar.tt_e(r) };
                if as_of >= tt_e {
                    continue;
                }
                out.push((vt_s[i], Id96 { hi: vid64[i], lo: lo[i] }, src[i], dst[i], id, r));
            }
        }
        out.sort_by_key(|t| (t.0, t.1));
        out
    }

    #[test]
    fn neighbors_of_matches_the_segments_in_vt_s_vid_order() {
        let root = tmp_root("tcsr-neighbors");
        let s = tcsr_store(&root, 6);
        assert_eq!(s.manifest().edge_lanes.event.len(), 2, "two segments to merge");
        let oracle = all_edge_rows(&s, OPEN_END - 1);
        assert_eq!(oracle.len(), 12);

        for u in 0..6u32 {
            let got = s.neighbors_of(u, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
            let want: Vec<_> = oracle.iter().filter(|t| t.2 == u).collect();
            assert_eq!(got.len(), want.len(), "out-degree of {u}");
            assert_eq!(got.offsets, vec![0, want.len() as u32]);
            for (k, w) in want.iter().enumerate() {
                assert_eq!(got.vt_s[k], w.0, "vt_s at {k} of {u}");
                assert_eq!(got.nbr[k], w.3, "out-neighbour at {k} of {u}");
                assert_eq!((got.seg_id[k], got.seg_row[k]), (w.4, w.5));
                assert_eq!(got.vt_e[k], w.0 + 1, "the fixture is instantaneous");
            }

            let got = s.neighbors_of(u, NeighborDir::In, OPEN_END, None, None, None).unwrap();
            let want: Vec<_> = oracle.iter().filter(|t| t.3 == u).collect();
            assert_eq!(got.len(), want.len(), "in-degree of {u}");
            for (k, w) in want.iter().enumerate() {
                assert_eq!(got.nbr[k], w.2, "in-neighbour at {k} of {u}");
            }

            let both = s.neighbors_of(u, NeighborDir::Both, OPEN_END, None, None, None).unwrap();
            let want = oracle.iter().filter(|t| t.2 == u || t.3 == u).count();
            assert_eq!(both.len(), want, "both-degree of {u}");
            assert!(
                both.vt_s.windows(2).all(|w| w[0] <= w[1]),
                "`both` must come back in (vt_s, vid) order, not concatenated"
            );
        }
    }

    #[test]
    fn a_frontier_batch_is_the_concatenation_of_its_vertices() {
        let root = tmp_root("tcsr-batch");
        let s = tcsr_store(&root, 6);
        let us: Vec<u32> = vec![4, 0, 0, 2];
        let batch = s.neighbors_batch(&us, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert_eq!(batch.offsets.len(), us.len() + 1);
        for (k, &u) in us.iter().enumerate() {
            let one = s.neighbors_of(u, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
            let (a, b) = (batch.offsets[k] as usize, batch.offsets[k + 1] as usize);
            assert_eq!(&batch.nbr[a..b], &one.nbr[..], "vertex {u} at slot {k}");
            assert_eq!(&batch.seg_row[a..b], &one.seg_row[..]);
        }
    }

    #[test]
    fn the_valid_time_window_narrows_the_slice_the_way_a_scan_would() {
        let root = tmp_root("tcsr-window");
        let s = tcsr_store(&root, 6);
        let all = s.neighbors_of(3, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert_eq!(all.vt_s, vec![6, 7]);
        // vt_max is `vt_s < t`, vt_min is `vt_e > t` — the scan's predicate
        let cut = s.neighbors_of(3, NeighborDir::Out, OPEN_END, None, Some(7), None).unwrap();
        assert_eq!(cut.vt_s, vec![6]);
        let cut = s.neighbors_of(3, NeighborDir::Out, OPEN_END, Some(6), None, None).unwrap();
        assert_eq!(cut.vt_s, vec![6, 7], "vt_e = vt_s + 1 > 6 for both");
        let cut = s.neighbors_of(3, NeighborDir::Out, OPEN_END, Some(7), None, None).unwrap();
        assert_eq!(cut.vt_s, vec![7]);
        let none = s.neighbors_of(3, NeighborDir::Out, OPEN_END, None, Some(0), None).unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn an_as_of_before_the_second_batch_sees_only_the_first() {
        let root = tmp_root("tcsr-asof");
        let s = tcsr_store(&root, 6);
        let now = s.neighbors_of(1, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert_eq!(now.len(), 2);
        let then = s.neighbors_of(1, NeighborDir::Out, 150, None, None, None).unwrap();
        assert_eq!(then.len(), 1, "batch 2 was not believed at tt=150");
        assert_eq!(then.vt_s, vec![2]);
        let before = s.neighbors_of(1, NeighborDir::Out, 50, None, None, None).unwrap();
        assert!(before.is_empty());
    }

    #[test]
    fn a_close_hides_a_row_from_the_traversal_without_touching_the_index() {
        let root = tmp_root("tcsr-close");
        let mut s = tcsr_store(&root, 6);
        let before = s.neighbors_of(1, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert_eq!(before.len(), 2);
        let (seg_id, seg_row) = (before.seg_id[0], before.seg_row[0]);
        let file = s.segment_files().edge.get(&seg_id).cloned().unwrap();
        let vid = s.open_segment(&file).unwrap().vid_at(seg_row as usize).unwrap();

        let rows_before = s.tcsr_index(&file).unwrap().rows();
        s.begin(300).unwrap();
        s.close_version(RowKind::Edge, vid, 300).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        let after = s.neighbors_of(1, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert_eq!(after.len(), 1, "the closed row is no longer believed");
        assert_ne!((after.seg_id[0], after.seg_row[0]), (seg_id, seg_row));
        // ... and it is still visible as of a belief time before the close
        let at_250 = s.neighbors_of(1, NeighborDir::Out, 250, None, None, None).unwrap();
        assert_eq!(at_250.len(), 2);
        // the index itself is over *all stored rows*, so the close changed
        // nothing about it — that is the property the whole design rests on
        assert_eq!(s.tcsr_index(&file).unwrap().rows(), rows_before);
    }

    #[test]
    fn a_commit_does_not_rebuild_an_existing_segments_index() {
        let root = tmp_root("tcsr-f3-5");
        let mut s = tcsr_store(&root, 6);
        let (segments, built, loaded) = s.build_tcsr_indexes().unwrap();
        assert_eq!((segments, built, loaded), (2, 2, 0), "first build writes both");
        let (segments, built, loaded) = s.build_tcsr_indexes().unwrap();
        assert_eq!((segments, built, loaded), (2, 0, 0), "already cached in-handle");

        s.begin(300).unwrap();
        let z = s.ensure_entity("z", "Node").unwrap();
        s.stage_edge(edge_row(z, z, 999, 300, 900)).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        let (segments, built, loaded) = s.build_tcsr_indexes().unwrap();
        assert_eq!(
            (segments, built, loaded),
            (3, 1, 0),
            "a commit adds a segment; it must not invalidate the other two"
        );
        drop(s);

        // and a fresh handle *loads* the two files rather than rebuilding
        let re = NativeStore::open(&root).unwrap();
        let (segments, built, loaded) = re.build_tcsr_indexes().unwrap();
        assert_eq!((segments, built, loaded), (3, 0, 3), "every index came off disk");
    }

    #[test]
    fn an_index_file_that_does_not_match_its_segment_is_rebuilt() {
        let root = tmp_root("tcsr-stamp");
        let s = tcsr_store(&root, 6);
        s.build_tcsr_indexes().unwrap();
        let file = s.manifest().edge_lanes.event[0].file.clone();
        let want = s.neighbors_of(2, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        let id = segment_id_of(&file);
        let path = crate::tcsr::index_path(&root, id);
        drop(s);

        // forge the stamp: right shape, wrong segment
        let bytes = fs::read(&path).unwrap();
        let forged = crate::tcsr::SegmentTcsr::from_bytes(&bytes, id, &crate::tcsr::segment_body_sha(&root.join(&file)).unwrap()).unwrap();
        let re_stamped = crate::tcsr::SegmentTcsr::build(id, &[0], &[0], "not-this-segments-sha").unwrap();
        re_stamped.write(&path).unwrap();
        assert!(crate::tcsr::SegmentTcsr::load(&path, id, forged.body_sha()).is_none());

        let re = NativeStore::open(&root).unwrap();
        let got = re.neighbors_of(2, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert_eq!(got.nbr, want.nbr, "a foreign stamp must force a rebuild");
        let (_, built, _, _, _, _) = re.tcsr_stats();
        assert!(built >= 1, "the mismatched index was rebuilt, not trusted");
        // the rebuild overwrote the forged file
        assert!(crate::tcsr::SegmentTcsr::load(&path, id, forged.body_sha()).is_some());
    }

    #[test]
    fn a_read_only_index_directory_never_fails_a_query() {
        let root = tmp_root("tcsr-nowrite");
        let s = tcsr_store(&root, 6);
        // make the index directory un-writable by planting a *file* where
        // the directory has to go: `create_dir_all` then fails, `write`
        // fails, and the query must not notice
        let dir = crate::tcsr::index_dir(&root);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.parent().unwrap()).unwrap();
        fs::write(&dir, b"not a directory").unwrap();
        let got = s.neighbors_of(1, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert_eq!(got.len(), 2, "a failure to persist an index is never a query failure");
    }

    #[test]
    fn releasing_the_indexes_keeps_the_answers() {
        let root = tmp_root("tcsr-release");
        let s = tcsr_store(&root, 6);
        let want = s.neighbors_of(0, NeighborDir::Both, OPEN_END, None, None, None).unwrap();
        let (cached, _, _, bytes, _, _) = s.tcsr_stats();
        assert_eq!(cached, 2);
        assert!(bytes > 0);
        s.release_tcsr();
        assert_eq!(s.tcsr_stats().0, 0);
        assert_eq!(s.tcsr_stats().3, 0, "resident bytes go with the cache");
        assert_eq!(s.tcsr_stats().4, 0, "indexed rows go with the cache");
        assert!(s.tcsr_segment_stats().is_empty());
        let got = s.neighbors_of(0, NeighborDir::Both, OPEN_END, None, None, None).unwrap();
        assert_eq!(got, want);
    }

    /// The memory model's predicted side is computable from `tcsr_stats` alone.
    ///
    /// The model is "TCSR memory within 5 % of `4·(|V| + 1 + n)·2` bytes".
    /// Before this, `tcsr_stats` reported only the *measured* side: there was
    /// no `n` (rows actually indexed — not the store's row count, since an
    /// unvisited segment has no index) and no `|V|`, so the forecast could
    /// not be evaluated at any scale. With them the identity is exact, not
    /// approximate — `DirIndex::resident_bytes` is `4·(active + offsets +
    /// row_perm)` by construction — and the 5 % band exists for allocator
    /// slack above it, not for modelling error.
    #[test]
    fn tcsr_stats_carry_the_terms_its_memory_forecast_is_computed_from() {
        let root = tmp_root("tcsr-forecast");
        let s = tcsr_store(&root, 6);
        // touch both directions so every cached index is fully built
        s.neighbors_of(1, NeighborDir::Both, OPEN_END, None, None, None).unwrap();
        let (cached, _, _, bytes, rows, active) = s.tcsr_stats();
        assert!(cached > 0 && rows > 0 && active > 0, "nothing was indexed");

        let forecast = 4 * (active + 2 * cached as u64 + 2 * rows);
        assert_eq!(
            bytes, forecast,
            "resident bytes must equal 4*(active_vertices + 2*segments + \
             2*rows_indexed): {bytes} vs {forecast}"
        );

        // and the per-segment breakdown sums to the aggregate, with each
        // direction's dense/sparse election visible — a dense direction
        // allocates an offsets entry per id in the segment's whole id space,
        // which is the one way the forecast can be right and the intuition
        // behind it wrong
        let per = s.tcsr_segment_stats();
        assert_eq!(per.len(), cached);
        assert_eq!(per.iter().map(|r| r.4).sum::<u64>(), bytes);
        assert_eq!(per.iter().map(|r| r.1 as u64).sum::<u64>(), rows);
        assert_eq!(
            per.iter().map(|r| r.2 as u64 + r.3 as u64).sum::<u64>(),
            active
        );
        assert!(per.windows(2).all(|w| w[0].0 < w[1].0), "ordered by segment id");
    }

    #[test]
    fn a_vertex_with_no_edges_and_a_store_with_none_answer_empty() {
        let root = tmp_root("tcsr-empty");
        let mut s = NativeStore::open(&root).unwrap();
        commit_with(&mut s, 10, &["lonely"]);
        let got = s.neighbors_of(0, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert!(got.is_empty());
        assert_eq!(got.offsets, vec![0, 0]);
        assert_eq!(s.build_tcsr_indexes().unwrap(), (0, 0, 0));
        let batch = s.neighbors_batch(&[], NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert_eq!(batch.offsets, vec![0]);
    }

    #[test]
    fn a_rel_type_filter_selects_the_same_rows_a_scan_would() {
        let root = tmp_root("tcsr-rel");
        let s = tcsr_store(&root, 6);
        let all = s.neighbors_of(1, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert_eq!(all.len(), 2);
        let kept = s
            .neighbors_of(1, NeighborDir::Out, OPEN_END, None, None, Some(&["SENT_MSG_TO".to_string()]))
            .unwrap();
        assert_eq!(kept.nbr, all.nbr);
        let dropped = s
            .neighbors_of(1, NeighborDir::Out, OPEN_END, None, None, Some(&["NO_SUCH_REL".to_string()]))
            .unwrap();
        assert!(dropped.is_empty());
    }

    #[test]
    fn a_segment_nothing_believes_never_loads_its_index() {
        let root = tmp_root("tcsr-no-load");
        let s = tcsr_store(&root, 6);
        assert_eq!(s.manifest().edge_lanes.event.len(), 2);
        assert_eq!(s.tcsr_stats().0, 0, "nothing cached yet");

        // as_of before either batch: no segment is believed, so no index is
        // built or loaded — belief is decided ahead of `tcsr_index`
        let none = s.neighbors_of(1, NeighborDir::Out, 50, None, None, None).unwrap();
        assert!(none.is_empty());
        assert_eq!(
            s.tcsr_stats(),
            (0, 0, 0, 0, 0, 0),
            "an unbelieved segment costs nothing"
        );

        // as_of between them: exactly one segment is believed
        let one = s.neighbors_of(1, NeighborDir::Out, 150, None, None, None).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(s.tcsr_stats().0, 1, "only the believed segment was indexed");

        let both = s.neighbors_of(1, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert_eq!(both.len(), 2);
        assert_eq!(s.tcsr_stats().0, 2);
    }

    #[test]
    fn in_ranges_is_the_membership_the_row_loop_used_to_decide() {
        let ranges = [(0usize, 3usize), (5, 6), (9, 12)];
        let want: Vec<bool> = (0..14)
            .map(|r| ranges.iter().any(|(a, b)| r >= *a && r < *b))
            .collect();
        let got: Vec<bool> = (0..14u32).map(|r| in_ranges(r, &ranges)).collect();
        assert_eq!(got, want);
        assert!(!in_ranges(0, &[]), "no believed range believes nothing");
        assert!(in_ranges(u32::MAX - 1, &[(0, usize::MAX)]));
    }

    /// The seam itself: the range form must select exactly what the per-row
    /// form selected, at every `as_of` the store can be read at, on a segment
    /// that carries closes in both folded and unfolded form.
    ///
    /// `all_edge_rows` is deliberately still written as the **row loop** —
    /// `tt_s_at` plus the `close run outranks the sidecar` idiom — so this is
    /// a differential test between the two, not a restatement of one of them.
    #[test]
    fn the_range_form_selects_what_the_row_loop_selected_at_every_as_of() {
        let root = tmp_root("tcsr-seam");
        let mut s = tcsr_store(&root, 6);
        // close two rows in the first segment: one before compaction (so it
        // folds into the sidecar) and one after (so it stays a close run)
        let first = s.neighbors_of(1, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        let file = s.segment_files().edge.get(&first.seg_id[0]).cloned().unwrap();
        let vid = s.open_segment(&file).unwrap().vid_at(first.seg_row[0] as usize).unwrap();
        s.begin(300).unwrap();
        s.close_version(RowKind::Edge, vid, 300).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        s.compact().unwrap();

        let second = s.neighbors_of(2, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        let file = s.segment_files().edge.get(&second.seg_id[0]).cloned().unwrap();
        let vid = s.open_segment(&file).unwrap().vid_at(second.seg_row[0] as usize).unwrap();
        s.begin(400).unwrap();
        s.close_version(RowKind::Edge, vid, 400).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        let mut any_cut = false;
        for as_of in [50, 100, 150, 200, 250, 300, 350, 400, 450, OPEN_END - 1] {
            let oracle = all_edge_rows(&s, as_of);
            for u in 0..6u32 {
                for (d, pick) in [
                    (NeighborDir::Out, 2usize),
                    (NeighborDir::In, 3usize),
                ] {
                    let got = s.neighbors_of(u, d, as_of, None, None, None).unwrap();
                    let want: Vec<_> = oracle
                        .iter()
                        .filter(|t| if pick == 2 { t.2 == u } else { t.3 == u })
                        .map(|t| (t.4, t.5))
                        .collect();
                    let mine: Vec<_> = got
                        .seg_id
                        .iter()
                        .copied()
                        .zip(got.seg_row.iter().copied())
                        .collect();
                    assert_eq!(mine, want, "as_of={as_of} u={u} dir={d:?}");
                }
            }
            // the fixture really does cut rows somewhere in this sweep
            if oracle.len() < 12 {
                any_cut = true;
            }
        }
        assert!(
            any_cut,
            "no as_of in the sweep hid a row — the closes are not reaching the traversal"
        );
    }

    /// The per-row belief path and the range path select the same
    /// adjacency rows, and both match the row-loop oracle, at every `as_of`,
    /// with closes folded and in close runs. The cost rule is pinned each way
    /// so neither path depends on the fixture being large enough to choose it.
    #[test]
    fn per_row_and_range_traversal_belief_agree_at_every_as_of() {
        let root = tmp_root("tcsr-per-row");
        let mut s = tcsr_store(&root, 6);
        let first = s.neighbors_of(1, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        let file = s.segment_files().edge.get(&first.seg_id[0]).cloned().unwrap();
        let vid = s.open_segment(&file).unwrap().vid_at(first.seg_row[0] as usize).unwrap();
        s.begin(300).unwrap();
        s.close_version(RowKind::Edge, vid, 300).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        s.compact().unwrap();
        let second = s.neighbors_of(2, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        let file = s.segment_files().edge.get(&second.seg_id[0]).cloned().unwrap();
        let vid = s.open_segment(&file).unwrap().vid_at(second.seg_row[0] as usize).unwrap();
        s.begin(400).unwrap();
        s.close_version(RowKind::Edge, vid, 400).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        let mut any_cut = false;
        for as_of in [50, 100, 150, 200, 250, 300, 350, 400, 450, OPEN_END - 1] {
            let oracle = all_edge_rows(&s, as_of);
            if oracle.len() < 12 {
                any_cut = true;
            }
            for force in [true, false] {
                FORCE_PER_ROW.with(|c| c.set(Some(force)));
                for u in 0..6u32 {
                    for (d, pick) in [(NeighborDir::Out, 2usize), (NeighborDir::In, 3usize)] {
                        let got = s.neighbors_of(u, d, as_of, None, None, None).unwrap();
                        let want: Vec<_> = oracle
                            .iter()
                            .filter(|t| if pick == 2 { t.2 == u } else { t.3 == u })
                            .map(|t| (t.4, t.5))
                            .collect();
                        let mine: Vec<_> =
                            got.seg_id.iter().copied().zip(got.seg_row.iter().copied()).collect();
                        assert_eq!(mine, want, "per_row={force} as_of={as_of} u={u} dir={d:?}");
                    }
                }
                // and a batch over the whole frontier at once
                let us: Vec<u32> = (0..6).collect();
                let batch = s.neighbors_batch(&us, NeighborDir::Both, as_of, None, None, None).unwrap();
                FORCE_PER_ROW.with(|c| c.set(Some(!force)));
                let other = s.neighbors_batch(&us, NeighborDir::Both, as_of, None, None, None).unwrap();
                assert_eq!(batch, other, "as_of={as_of}: the two paths disagree on a batch");
            }
        }
        FORCE_PER_ROW.with(|c| c.set(None));
        assert!(any_cut, "no as_of in the sweep hid a row");
    }

    #[test]
    fn a_direction_string_that_is_not_a_direction_is_refused() {
        assert!(NeighborDir::parse("sideways").is_err());
        assert_eq!(NeighborDir::parse("out").unwrap(), NeighborDir::Out);
        assert_eq!(NeighborDir::parse("in").unwrap(), NeighborDir::In);
        assert_eq!(NeighborDir::parse("both").unwrap(), NeighborDir::Both);
    }
}
