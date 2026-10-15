//! Gnomon native bi-temporal storage engine — core.
//!
//! This crate has **no Python dependency**: it is a plain Rust library so it
//! can be unit-tested with `cargo test`, fuzzed, and later driven by a
//! standalone inspect/repair CLI. The PyO3 bindings live in
//! `gnomon-py`.
//!
//! Design: `ENGINE_BLUEPRINT.md` (v3). Binding contract:
//! `ENGINE_IMPLEMENTATION_SPEC.md`. The store is a deterministic
//! materialization of the JSONL event log, so the durability objective is
//! *never expose an undetected inconsistent generation* — roll back to the
//! last valid generation and replay the log suffix.

pub mod aggregate;
pub mod beliefdiff;
pub mod codec;
pub mod compact;
pub mod derive;
pub mod designs;
pub mod digest;
pub mod dict;
pub mod error;
pub mod gc;
pub mod ingest_phase;
pub mod ingest_raw;
pub mod integrity;
pub mod interval;
pub mod linfor;
pub mod manifest;
pub mod manifest_chain;
pub mod merge;
pub mod motif;
pub mod nameidx;
pub mod node_belief;
pub mod phase;
pub(crate) mod pool;
pub mod reach;
pub mod snapdiff;
pub mod read;
pub mod row;
pub mod scan;
pub mod sealsize;
pub mod segment;
pub mod settle;
pub mod staging;
pub mod store;
pub mod tcsr;
pub mod ttime;
pub mod vidfilter;
pub mod visibility;
pub mod vistouch;
#[cfg(test)]
mod windowfirst_tests;
#[cfg(test)]
mod ablation_tests;
#[cfg(test)]
mod scaling_tests;

pub use derive::{edge_eid, version_vid, Id96};
pub use dict::Dictionary;
pub use gc::GcReport;
pub use manifest::Manifest;
pub use row::{EdgeRow, Lane, NodeRow, RowKind};
pub use store::NativeStore;
pub use error::{Category, EngineError, Result};

/// Crate version, surfaced to Python for receipts.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// On-disk format version for the *physical* files — segment headers and
/// close-run headers. Bumped only by a breaking change to those bytes; every
/// segment and close-run file records it.
///
/// The manifest carries its own version ([`MANIFEST_FORMAT_VERSION`]). The
/// two were one constant until the delta-manifest change,
/// which rewrote
/// the manifest document and left every segment byte untouched — bumping the
/// shared constant would have made `segment.rs`'s exact-match check reject
/// every already-written segment, which is the opposite of the migration
/// story the format needs.
pub const FORMAT_VERSION: u32 = 1;

/// On-disk format version for `manifests/<G>.json`.
///
/// * **1** — the pre-2026-09-13 full-manifest-per-generation shape.
/// * **2** — the tagged checkpoint/delta document of the memo above, with
///   `manifest_sha` still the sha of the whole serialized document.
/// * **3** — the same document shape, with `manifest_sha` a **Merkle root**
///   over the ordered segment set. The document layout did not change; the *value* of
///   `manifest_sha` did, which is why it is a format bump. Records written at
///   format 3 carry `sha_kind: "merkle-v1"` so the digest rule is legible on
///   disk as well as derivable from `format`.
///
/// This build writes format 3; formats 1 and 2 open **read-only** and
/// `gnomon store upgrade-manifests` converts either of them.
pub const MANIFEST_FORMAT_VERSION: u32 = 3;

/// The oldest manifest format this build can read.
pub const FORMAT_LEGACY: u32 = 1;

/// The manifest formats this build can read but not write — everything below
/// [`MANIFEST_FORMAT_VERSION`] that it still understands. Kept as an explicit
/// list rather than a `<` test so that dropping support for a format is a
/// deliberate edit here rather than a silent consequence of a bump.
pub const MANIFEST_FORMATS_READ_ONLY: [u32; 2] = [1, 2];

/// The first manifest format whose `manifest_sha` is the Merkle root rather
/// than the sha of the whole serialized document.
pub const MANIFEST_FORMAT_MERKLE: u32 = 3;

/// Open-end sentinel — must equal `gnomon.core.model.OPEN_END`.
pub const OPEN_END: i64 = 1 << 62;

/// Format defaults. Recorded in each file header, so changing a
/// default never invalidates already-written data.
pub mod defaults {
    /// Rows per block — the codec and pruning unit, sized to stay in L2.
    pub const BLOCK_ROWS: u32 = 32_768;
    /// Target size of one sealed segment **on disk**, before splitting.
    ///
    /// Read this as a file size, not as an accounting quantity. The writers
    /// still decide when to seal by accumulating a per-row model
    /// ([`EDGE_ROW_MODEL_BYTES`] / [`NODE_ROW_MODEL_BYTES`]), but that model
    /// is divided through by a *measured* quotient between the bytes the
    /// encoder writes and the bytes the model accounted for, so the file
    /// lands here whatever the corpus compresses to — see `crate::sealsize`.
    ///
    /// It used to name the accounting quantity only: on the synthetic
    /// corpus the model ran ~58 B/row against an achieved ~30.5 B/row, so a
    /// segment "targeted at 128 MiB" occupied ~68 MB and a store carried
    /// about twice the segments its layout implied.
    pub const SEGMENT_TARGET_BYTES: u64 = 128 * 1024 * 1024;

    /// The writer's per-row accounting unit for an edge row, in bytes:
    /// `staging::EDGE_FIXED_BYTES + 4`.
    ///
    /// Flat, and deliberately so. It is not an estimate of anything the
    /// encoder does — `crate::sealsize::SealRatio` measures that — so its only
    /// requirement is that the writer, `merge::full_rows` and `settle`'s byte
    /// floor all use the same one, which earlier versions did not.
    pub const EDGE_ROW_MODEL_BYTES: u64 = 50;

    /// The writer's per-row accounting unit for a node row:
    /// `staging::NODE_FIXED_BYTES + 4`. See [`EDGE_ROW_MODEL_BYTES`].
    pub const NODE_ROW_MODEL_BYTES: u64 = 44;

    /// Widest the measured seal quotient may stretch the accounted target,
    /// as `NUM/DEN`: a segment may hold at most four times the rows the
    /// configured target implies at the flat model.
    ///
    /// **This is a sizing guard, not the memory bound.** The memory bound is
    /// [`CHUNK_RESIDENT_MAX_BYTES`], which is exact and which binds first at
    /// this ratio — 4 x 128 MiB accounted is ~10.7M rows and ~3.0 GB
    /// resident against a 2 GiB cap — so an anomalous observation cannot buy
    /// unbounded memory even at the clamp. What this bounds is the *sizing*:
    /// a corpus that encodes to better than a quarter of the model seals
    /// under target rather than chasing the quotient, which is the graceful
    /// direction. The measured synthetic corpus needs 1.62 and the most
    /// compressible fixture in `tests/sealsize.rs` needs 3.46, so it has
    /// headroom over both.
    pub const SEAL_RATIO_MAX_NUM: u64 = 4;
    /// Denominator of [`SEAL_RATIO_MAX_NUM`].
    pub const SEAL_RATIO_MAX_DEN: u64 = 1;
    /// Narrowest the measured seal quotient may shrink the accounted target,
    /// as `NUM/DEN`. Guards the segment count.
    pub const SEAL_RATIO_MIN_NUM: u64 = 1;
    /// Denominator of [`SEAL_RATIO_MIN_NUM`].
    pub const SEAL_RATIO_MIN_DEN: u64 = 2;

    /// Hard ceiling on the rows one chunk writer may hold resident, counted
    /// exactly (`crate::sealsize::edge_resident_bytes`) rather than modelled.
    ///
    /// The writers buffer owned rows — five `String`s each, ≈228 B resident
    /// against the 50 B accounted — so the effective target
    /// scales the resident set with it. At the measured quotient one edge
    /// writer holds ≈1.0 GB; this cap is what makes that a bound rather than
    /// an observation, and it binds before [`SEAL_RATIO_MAX_NUM`] would on a
    /// corpus with long strings. `GNOMON_CHUNK_RESIDENT_BYTES` overrides; `0`
    /// means no cap.
    pub const CHUNK_RESIDENT_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

    /// Bytes of `EdgeRow` that do not depend on any string's length: the
    /// scalars plus five `String`/`Option<String>` headers at 24 B.
    pub const EDGE_ROW_RESIDENT_FIXED: u64 = 12 + 4 + 4 + 8 * 3 + 24 * 5;
    /// Bytes of `NodeRow` that do not depend on any string's length: the
    /// scalars plus four `String`/`Option<String>` headers at 24 B.
    pub const NODE_ROW_RESIDENT_FIXED: u64 = 12 + 4 + 8 * 3 + 24 * 4;
    /// Allocator bucket charged to each non-empty owned string, so the
    /// resident cap over-counts rather than under-counts.
    pub const STRING_ALLOC_OVERHEAD: u64 = 16;
    /// Compaction trigger: runs per logical partition.
    pub const COMPACTION_RUNS_TRIGGER: u32 = 4;
    /// Ceiling on the *default* segment-cache budget.
    ///
    /// The default was half of detected physical RAM, which is a policy fit
    /// for a laptop and a hazard on a benchmark node: on the 515 GB host that
    /// produced `build-record-100m.json` it implies ~251 GB, and the only
    /// reason the measured resident floor was 107.3 GB rather than higher is
    /// that every Stage-1 job pinned `GNOMON_SEGMENT_CACHE_BYTES=46500000000`
    /// by hand. A memory claim that holds only because an operator remembered
    /// a job-script constant is not a claim about the engine, so the default
    /// is capped here instead. `GNOMON_SEGMENT_CACHE_BYTES` still overrides in
    /// both directions (`0` = unbounded), and on any host with under 64 GB of
    /// RAM the cap never binds.
    pub const SEGMENT_CACHE_MAX_BYTES: u64 = 32 * 1024 * 1024 * 1024;
    /// Ceiling on the *default* identity-postings byte budget, and the whole default where physical RAM cannot be read.
    ///
    /// The postings are the second O(store) holder a build keeps resident:
    /// a 30M-entity build stepped from a 1.2–1.8 GB ingest floor to
    /// 15.73 GB at its first correction and stayed there, linear in row count
    /// and so ≈ 500 GB at 1B.
    /// `GNOMON_POSTINGS_BYTES` overrides in both directions (`0` = unbounded).
    ///
    /// **It is a safety bound, not a routine policy, and the arithmetic is
    /// deliberate.** A segment's postings cost 12 B/row
    /// (`store.rs::SegmentPostings`), so 16 GiB holds ≈ 1.4 × 10⁹ rows: the
    /// whole range of measured sizes, 1B included (≈ 12 GB), sits *under* the default and never evicts. That is the intent.
    /// A budget that binds is correct — a probe folds what it needs on the
    /// spot and answers from it — but it is **expensive in a way the segment
    /// cache's is not**: identity is a hash, so no manifest range can prune
    /// it, and an identity point read therefore consults *every* live
    /// segment. Under a binding budget each such read re-folds whatever was
    /// evicted, i.e. O(store) per read, which is a linear scan
    /// with extra steps. So the bound exists to keep a pathological store
    /// (far more rows than 1B, or a deliberately tiny budget) from
    /// growing without limit, and `GNOMON_POSTINGS_BYTES` is how an operator
    /// trades wall for memory knowingly. `0` = unbounded.
    ///
    /// Half of [`SEGMENT_CACHE_MAX_BYTES`], and the default is
    /// `min(RAM/8, 16 GiB)` against the cache's `min(RAM/2, 32 GiB)`: the
    /// RAM fraction is an eighth rather than a half because the postings are
    /// the smaller holder by an order of magnitude, and on any host with
    /// under 128 GB of RAM the RAM term binds before the ceiling does.
    pub const POSTINGS_MAX_BYTES: u64 = 16 * 1024 * 1024 * 1024;
    /// Ceiling on the per-handle identity-filter mapping
    /// (`crate::vidfilter`). The default is
    /// `min(RAM/64, 4 GiB)`; `GNOMON_VIDFILTER_BYTES` overrides in both
    /// directions and `0` means unbounded.
    ///
    /// A sixty-fourth of RAM rather than the postings' eighth because the
    /// filter is far smaller than what it prunes: ≈18.2 bits per distinct
    /// identity key against the postings' 12 B/row, i.e. ≈2.30 GB for a
    /// whole 1B store against 12.25 GB of postings over the same rows. The
    /// whole-store need must fit comfortably or the probe maps and drops on
    /// every segment, so the 4 GiB ceiling sits above the largest scale the
    /// sequence reaches. These bytes are **file-backed**, so unlike the
    /// postings the kernel can reclaim them, and they are reported under
    /// `file_mapped_total` rather than `anonymous_total`.
    pub const VIDFILTER_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;
    /// Compaction trigger: unfolded close entries as a fraction of segment rows.
    pub const COMPACTION_CLOSE_FRACTION: f64 = 0.20;
    /// Incremental merge fan-in `F`: segments per incremental rewrite, and the run
    /// count at which a key-range cluster becomes eligible for one. It is the base of
    /// the write-amplification bound — each row is rewritten
    /// `O(log_F(n / rows_per_run))` times, 3.7 at 100M against the whole-store
    /// path's 101 — which is why it is 8 rather than
    /// [`COMPACTION_RUNS_TRIGGER`]'s 4: that constant sizes the advisory
    /// whole-store hint, where over-triggering is the safe direction, and
    /// here over-triggering is precisely the cost being removed.
    pub const CLUSTER_RUNS_TRIGGER: u32 = 8;
    /// Lane rule K: max adjacent partitions an event-lane interval may cross.
    pub const LANE_MAX_PARTITION_CROSSINGS: u32 = 2;
    /// TCSR switches to dense per-vertex offsets at or above this density.
    pub const TCSR_DENSE_THRESHOLD: f64 = 0.5;
    /// The largest incidence filter (`touching_ids`) a scan
    /// answers from adjacency postings.
    ///
    /// Whether postings pay is decided per segment, exactly: they are taken
    /// only when the posting rows of the filter's ids are fewer than the rows
    /// the scan would examine there (`scan::PostingsSource`), so a taken
    /// segment never examines more rows than the scan. This cap bounds only
    /// what finding that out costs: one slice lookup per id per segment
    /// (a binary search over the segment's active vertices, about 20 probes
    /// at a million), and on a handle without the segment's index, loading or
    /// building it. At 1,024 ids the lookups are about 20,000 probes, under
    /// one block of rows ([`BLOCK_ROWS`]), so asking never costs more than
    /// scanning a block. Filters past it (a motif node filter of thousands)
    /// take the scan without asking. The seed-driven snapshot
    /// (`ops_snapshot.SEED_EXPANSION_MAX_IDS`) uses the same bound for its
    /// frontier, so every scan it makes can take postings.
    pub const ADJ_FILTER_MAX_IDS: usize = 1024;
    /// Generations between full manifest checkpoints.
    /// Retained manifest bytes at SF1 scale go as G·(h+s) + s·G²/(2K) and
    /// worst-case open as one checkpoint parse + K delta parses; 512 puts the
    /// measured 25,451 MB pathology at ~62 MB while keeping worst-case open
    /// (~35 ms at G = 10,147) within 1.75× of the status quo's full-manifest
    /// parse at the same generation. `GNOMON_MANIFEST_CHECKPOINT_EVERY`
    /// overrides, so the A/B can sweep K without a rebuild.
    pub const MANIFEST_CHECKPOINT_EVERY: u64 = 512;
    /// Generations gc retains besides those pinned by live readers. 2 keeps
    /// one full generation of headroom behind `CURRENT` for post-incident
    /// inspection while still bounding manifest growth.
    pub const GC_KEEP_GENERATIONS: u64 = 2;
    /// Segment-count ceiling above which pin-at-open falls back to
    /// lazy per-segment loading instead of mapping the whole generation.
    /// ¼ of Linux's default `vm.max_map_count` (65,530): a compaction-bounded
    /// store sits two orders of magnitude under this (416 VMAs at 2.28M rows
    /// in the soak that motivated the fix; 46 segments at 100M events,
    /// `SEGMENT_TARGET_BYTES`-bounded); the failure mode this guards is a
    /// store whose segment count is *not* bounded by compaction
    /// (`--compact-every=0` held 3,579 segments at 800k events, extrapolating
    /// to ~450k at 100M — an order of magnitude over the ceiling).
    /// `GNOMON_PIN_MAX_SEGMENTS` overrides.
    pub const PIN_MAX_SEGMENTS: usize = 16_384;
    /// Candidate rows (sum over scan targets) below which the scan stages
    /// stay serial. Calibrated by a thread-width sweep: parallel select was *slower* than serial
    /// at every width 2–16 on a 1M-row store and 4.3× faster at 10M — the
    /// break-even sits between the scales, so the gate is row-count-based,
    /// not segment-count-based. 4M splits the measured decade.
    pub const PARALLEL_SCAN_MIN_ROWS: u64 = 4_000_000;
    /// Worker widths below this run serial: t=2 measured slower than t=1 at
    /// both 1M and 10M (spawn-and-merge overhead against a straggler-set
    /// finish line); the win only appears from 4 workers up.
    pub const PARALLEL_SCAN_MIN_THREADS: usize = 4;
    /// The row threshold of the **grouped aggregate's** own two parallel
    /// stages: the row loop of its select and its fold.
    ///
    /// [`PARALLEL_SCAN_MIN_ROWS`] was calibrated on 2026-08-01 for the scan
    /// stages, when a fan-out spawned threads per call. The fold is a
    /// different kind of work: tens of nanoseconds per row (hash-group
    /// lookup, distinct bitset) against a few per row for a select, and
    /// the stages run on the persistent pool, so a fan-out costs a
    /// wake-up rather than a spawn. Under the scan gate a 1M-row
    /// `agg.rel_bucket` folded on one worker: 29.4 ms of a 32.5 ms call at
    /// 29 ns/row. Its own threshold, calibrated by a sweep, takes it
    /// to the multi-worker fold.
    ///
    /// Only the aggregate's row path reads this. The count-by-bucket path
    /// never folds a row and keeps the scan gate for its planner,
    /// and so does every other scan: the global gate's measured costs at
    /// 1M (`series.count`, `burst.zscore`, 0.1-0.2 ms slower under a lower
    /// global gate) are not paid here. `GNOMON_AGG_PARALLEL_MIN_ROWS`
    /// overrides, for measurement only.
    pub const AGG_PARALLEL_MIN_ROWS: u64 = 131_072;
    /// Block size of the `vt_e` staircase **derived at open** for an
    /// event-lane segment that stores `vt_e`. The structure is
    /// held in memory only (never written), so its granularity is not a
    /// format question: 4,096 rows is the block size a simulation
    /// selected, and costs 8 bytes per block, about 20 KB on a
    /// 10M-row segment.
    pub const DERIVED_VT_E_BLOCK_ROWS: u32 = 4_096;
    /// Row-range units the read path aims to cut **per worker**.
    ///
    /// The units of both parallel read stages used to be one per opened
    /// segment, so a store's parallelism was a property of its layout: a
    /// settled 10M store has five live edge segments against
    /// `scan_threads()` = 16 and could not fill them however well they were
    /// scheduled. Units are now `(segment, row range)`, and this is the
    /// oversubscription factor that sets how many of them there are. One unit
    /// per worker makes a single slow unit the whole makespan again; very
    /// many units start to show the per-unit constant.
    /// `GNOMON_ROWS_PER_UNIT` overrides the derived size outright.
    pub const SUBUNIT_UNITS_PER_THREAD: usize = 4;
    /// Floor on a row-range unit, so a segment smaller than this stays
    /// exactly one unit and the per-segment shape is preserved wherever splitting
    /// would buy nothing. At 65,536 rows the row loop runs for tens of
    /// microseconds — two orders of magnitude over the claim-and-`Vec`-header
    /// constant of a unit.
    pub const MIN_ROWS_PER_UNIT: usize = 65_536;
    /// Byte budget for the aggregate fold's **concurrent per-worker partial
    /// group tables**, and the numerator of the fold-width cap.
    ///
    /// The unit of a parallel read is a row range, so a settled store
    /// that could only fill three fold workers now fills sixteen — and with
    /// them sixteen partial group maps where there were three. The bound on
    /// that state is `width × per-worker map`; what row-range units changed
    /// is the width at which it is evaluated, and on
    /// one store the product broke a peak-RSS band. The remedy is to make the
    /// width a function of the product rather than of the thread count:
    ///
    /// ```text
    /// width = min(threads, max(1, FOLD_MAP_BUDGET_BYTES / est_map_bytes))
    /// ```
    ///
    /// **The number, and its relation to the cache cap.** The engine caps the *resident*
    /// segment cache's default at [`SEGMENT_CACHE_MAX_BYTES`] = 32 GiB, on
    /// the argument that a memory claim holding only because a job script
    /// pinned a constant is a claim about the job script. The same argument
    /// applies here, so this is a fixed fraction of that ceiling rather than
    /// a fraction of detected RAM: **1/256 of it, i.e. 128 MiB**. The
    /// fraction is small on purpose and the ratio is the statement — the
    /// fold's per-query, per-worker *transient* state is budgeted at four
    /// hundredths of a percent of the store's long-lived resident cache, so
    /// a host that pins a smaller cache budget by hand does not silently
    /// acquire a larger fold. Unlike the cache cap, this budget does not read
    /// `/proc/meminfo`: a constant is reproducible across hosts, and the
    /// quantity it bounds is per query, not per store.
    ///
    /// `GNOMON_FOLD_BYTES` overrides in both directions; `0` means unbounded,
    /// which is `width = threads`.
    pub const FOLD_MAP_BUDGET_BYTES: u64 = SEGMENT_CACHE_MAX_BYTES / 256;
}

/// `as_of_tt = OPEN_END` means "current beliefs"; clamping keeps the
/// half-open belief predicate `tt_s <= as_of < tt_e` true for open rows.
/// Mirrors `gnomon.core.model.clamp_tt` exactly.
#[inline]
pub const fn clamp_tt(as_of_tt: i64) -> i64 {
    if as_of_tt > OPEN_END - 1 {
        OPEN_END - 1
    } else {
        as_of_tt
    }
}

/// Is a row with belief interval `[tt_s, tt_e)` believed at `as_of_tt`?
#[inline]
pub const fn believed_at(tt_s: i64, tt_e: i64, as_of_tt: i64) -> bool {
    let a = clamp_tt(as_of_tt);
    tt_s <= a && a < tt_e
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_end_matches_python() {
        assert_eq!(OPEN_END, 4_611_686_018_427_387_904);
    }

    #[test]
    fn clamp_tt_matches_python_semantics() {
        assert_eq!(clamp_tt(0), 0);
        assert_eq!(clamp_tt(OPEN_END), OPEN_END - 1);
        assert_eq!(clamp_tt(OPEN_END + 5), OPEN_END - 1);
        assert_eq!(clamp_tt(OPEN_END - 1), OPEN_END - 1);
    }

    #[test]
    fn open_rows_are_believed_at_open_end() {
        // the row every event-stream ingest writes: believed from tt_s, never closed
        assert!(believed_at(100, OPEN_END, OPEN_END));
        assert!(believed_at(100, OPEN_END, 100));
        assert!(!believed_at(100, OPEN_END, 99));
        // a closed row is invisible at or after its close time
        assert!(believed_at(100, 200, 199));
        assert!(!believed_at(100, 200, 200));
    }
}
