//! Which of the engine's designs a handle runs with.
//!
//! Every switch here turns **one** design off and keeps every answer
//! identical, so an ablation study can price what each design buys. Unset,
//! every design is in force: that is the shipped engine, and nothing on its
//! paths changes.
//!
//! | design | switch | effect when set |
//! |---|---|---|
//! | belief on row ranges | `GNOMON_RANGE_VISIBILITY=0` | each row the query can use gets a per-row belief test |
//! | window-first visibility | `GNOMON_WINDOW_FIRST=0` | belief for the whole segment, then clipped |
//! | segment pruning | `GNOMON_SEGMENT_PRUNE=0` | every candidate segment is opened and planned |
//! | per-segment adjacency index | `GNOMON_TCSR=scan` | adjacency by scanning endpoint columns, no index |
//! | identity filter (reads) | `GNOMON_VIDFILTER=ignore` | no filter is consulted (builds are untouched) |
//! | believed-node count | `GNOMON_EXISTENCE_PROBE=walk` | the ingest endpoint check walks instead of counting |
//! | seed-driven snapshot | `GNOMON_SEED_EXPANSION=0` | `snapshot_subgraph` builds the whole snapshot at the instant |
//! | adjacency postings for a filtered scan | `GNOMON_ADJ_FILTERED_SCAN=0` | a scan filtered to a few nodes tests every row of each segment |
//! | count by time bucket from believed ranges | `GNOMON_AGG_RANGE_COUNT=0` | `aggregate_edges` counts by folding selected rows |
//! | reachability kernel | `GNOMON_REACH_KERNEL=python` | `temporal_reachability` runs its NumPy fixpoint (the operator reads `designs()`) |
//! | snapshot-diff kernel | `GNOMON_DIFF_KERNEL=python` | `diff_snapshots` builds its identity maps in Python (the operator reads `designs()`) |
//! | raw-record bulk ingest | `GNOMON_INGEST_RAW=0` | an `ingest_events` op is staged by the Python row build |
//! | `vt_e` staircase (written for the interval lane, derived at open for the event lane) | `GNOMON_NO_STAIRCASE=1` | no `vt_e` prefix cut: no segment's staircase is consulted, written or derived, and none is derived at open |
//!
//! The write-time switch, `GNOMON_TIME_CODEC`, is not here: it is read when a
//! segment is written ([`crate::segment::TimeCodec`]).
//!
//! **Read once per handle, at open.** Not per call: an environment read is a
//! lock and a string compare, and per-query paths should not pay it; and
//! `std::env::set_var` in parallel tests races. Not per process either: a
//! harness must be able to open two handles under two settings and compare
//! them. A handle therefore answers with the environment it was opened
//! under, and [`crate::store::NativeStore::designs`] says which, so a
//! receipt can record the configuration that answered rather than the one
//! someone believed was set.
//!
//! A value that is not understood leaves the design on. A measurement knob
//! must never make a read fail.

/// The designs one handle runs with. `Designs::ALL` (the default) is the
/// shipped engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Designs {
    /// Belief computed on row ranges (`tt_s` runs or dictionary codes with
    /// per-block bounds, minus the segment's sorted closes). Off: a per-row
    /// belief test over the rows the query can use.
    pub range_visibility: bool,
    /// Belief computed for the query's row range first. Off: for the
    /// whole segment, then clipped.
    pub window_first: bool,
    /// Segments skipped on manifest or header bounds (valid time, belief
    /// time, relation names, key range). Off: every candidate is opened.
    pub segment_prune: bool,
    /// The per-segment temporal adjacency index for traversal. Off: one
    /// scan of each segment's endpoint columns per traversal call.
    pub adjacency_index: bool,
    /// The per-segment identity filter on the correction path. Off: no
    /// filter is consulted; every segment's postings are asked.
    pub identity_filter: bool,
    /// The believed-node count for the ingest endpoint check. Off: the
    /// walk ([`crate::read::ExistenceProbe::Walk`]).
    pub existence_count: bool,
    /// `snapshot_subgraph` expands from its seeds through
    /// filtered scans. The operator is Python (`ops_snapshot.py`); the
    /// engine only carries the switch, so it is read at open and recorded
    /// like the others. Off: the whole snapshot at the instant.
    pub seed_expansion: bool,
    /// A scan filtered to a few nodes takes each segment's
    /// adjacency postings where they name fewer rows than the scan would
    /// examine. Off: every row of each segment's ranges is tested.
    pub adjacency_filtered_scan: bool,
    pub agg_range_count: bool,
    pub reach_kernel: bool,
    pub diff_kernel: bool,
    /// An `ingest_events` op in the accepted shape is staged from
    /// its event-log record by the engine (`crate::ingest_raw`). Off: the
    /// Python row build stages it, as before. A write-path design, read at
    /// open like the others, so a build records which path built it.
    pub ingest_raw: bool,
    /// The `vt_e` prefix cut of the planner: an interval-lane segment's
    /// written staircase, and the one derived at open
    /// for an event-lane segment that stores `vt_e`. Off
    /// (`GNOMON_NO_STAIRCASE=1`, the switch's existing name and value): no
    /// staircase is consulted and none is derived, so every row of the
    /// window's `vt_s` range gets the per-row `vt_e` test. Read at open, like
    /// every other switch here.
    pub staircase: bool,
}

impl Designs {
    /// Every design in force: the shipped engine.
    pub const ALL: Designs = Designs {
        range_visibility: true,
        window_first: true,
        segment_prune: true,
        adjacency_index: true,
        identity_filter: true,
        existence_count: true,
        seed_expansion: true,
        adjacency_filtered_scan: true,
        agg_range_count: true,
        reach_kernel: true,
        diff_kernel: true,
        ingest_raw: true,
        staircase: true,
    };

    /// From the process environment.
    pub fn from_env() -> Designs {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// From any variable lookup, so the parsing is testable without
    /// touching the process environment.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Designs {
        let norm = |k: &str| get(k).map(|v| v.trim().to_ascii_lowercase());
        let off = |k: &str| matches!(norm(k).as_deref(), Some("0") | Some("false"));
        Designs {
            range_visibility: !off("GNOMON_RANGE_VISIBILITY"),
            window_first: !off("GNOMON_WINDOW_FIRST"),
            segment_prune: !off("GNOMON_SEGMENT_PRUNE"),
            adjacency_index: norm("GNOMON_TCSR").as_deref() != Some("scan"),
            identity_filter: norm("GNOMON_VIDFILTER").as_deref() != Some("ignore"),
            existence_count: norm("GNOMON_EXISTENCE_PROBE").as_deref() != Some("walk"),
            seed_expansion: !off("GNOMON_SEED_EXPANSION"),
            adjacency_filtered_scan: !off("GNOMON_ADJ_FILTERED_SCAN"),
            agg_range_count: !off("GNOMON_AGG_RANGE_COUNT"),
            reach_kernel: norm("GNOMON_REACH_KERNEL").as_deref() != Some("python"),
            diff_kernel: norm("GNOMON_DIFF_KERNEL").as_deref() != Some("python"),
            ingest_raw: !off("GNOMON_INGEST_RAW"),
            // the switch's own grammar, unchanged: exactly "1" turns it off
            staircase: get("GNOMON_NO_STAIRCASE").as_deref() != Some("1"),
        }
    }

    /// `(name, in force)` for every design, in a fixed order: what a
    /// receipt records.
    pub fn named(&self) -> [(&'static str, bool); 13] {
        [
            ("range_visibility", self.range_visibility),
            ("window_first", self.window_first),
            ("segment_prune", self.segment_prune),
            ("adjacency_index", self.adjacency_index),
            ("identity_filter", self.identity_filter),
            ("existence_count", self.existence_count),
            ("seed_expansion", self.seed_expansion),
            ("adjacency_filtered_scan", self.adjacency_filtered_scan),
            ("agg_range_count", self.agg_range_count),
            ("reach_kernel", self.reach_kernel),
            ("diff_kernel", self.diff_kernel),
            ("ingest_raw", self.ingest_raw),
            ("staircase", self.staircase),
        ]
    }
}

impl Default for Designs {
    fn default() -> Self {
        Self::ALL
    }
}
