//! One switch per engine design, each keeping every answer identical.
//!
//! Two kinds of test per switch:
//!
//! * **identity**: the same answer with the design on and off;
//! * **work**: a counter taken from the production path moves the way the
//!   switch says it should. A switch that silently does nothing fails here,
//!   and the falsifying value is named in each assertion.
//!
//! Switches are set per handle ([`NativeStore::set_designs`]) or per scan
//! ([`ScanSet::with_designs`]), never through the process environment, so
//! these tests run in parallel with everything else.

use std::sync::Arc;

use crate::designs::Designs;
use crate::scan::ScanSet;
use crate::store::NativeStore;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-ablation-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

// ---------------------------------------------------------------------- //
// plumbing                                                                 //
// ---------------------------------------------------------------------- //

/// Switches whose "off" value is `python` (the operator's NumPy path).
const KERNEL_SWITCHES: [&str; 4] =
    ["GNOMON_REACH_KERNEL", "GNOMON_DIFF_KERNEL", "GNOMON_MATCH_KERNEL", "GNOMON_PATHS_KERNEL"];

#[test]
fn unset_variables_leave_every_design_on() {
    assert_eq!(Designs::from_lookup(|_| None), Designs::ALL);
    assert_eq!(Designs::default(), Designs::ALL);
    // values that are not understood leave the design on: a measurement
    // knob must never make a read fail
    for v in ["", "1", "yes", "on", "garbage", "rust", "python"] {
        // `python` is the one value the kernel switches understand
        // (`GNOMON_REACH_KERNEL=python`), so it is not set on those here;
        // `1` is the one value `GNOMON_NO_STAIRCASE` understands, and
        // the opt-in small-commit switch and its two numeric parameters
        // understand it too, and so does the read kernels' thread count
        let d = Designs::from_lookup(|k| {
            (!(v == "python" && KERNEL_SWITCHES.contains(&k))
                && !(v == "1"
                    && (k == "GNOMON_NO_STAIRCASE"
                        || k.starts_with("GNOMON_SMALL_COMMIT")
                        || k == "GNOMON_MATCH_THREADS"
                        || k == "GNOMON_MATCH_PAR_ROWS")))
                .then(|| v.to_string())
        });
        assert_eq!(d, Designs::ALL, "value {v:?} turned a design off");
    }
}

#[test]
fn each_variable_turns_off_exactly_its_own_design() {
    let one = |k: &'static str, v: &'static str| {
        Designs::from_lookup(move |q| (q == k).then(|| v.to_string()))
    };
    type Case = (&'static str, &'static str, fn(&mut Designs));
    let cases: [Case; 27] = [
        ("GNOMON_RANGE_VISIBILITY", "0", |d| d.range_visibility = false),
        ("GNOMON_RANGE_VISIBILITY", "false", |d| d.range_visibility = false),
        ("GNOMON_WINDOW_FIRST", "0", |d| d.window_first = false),
        ("GNOMON_SEGMENT_PRUNE", " FALSE ", |d| d.segment_prune = false),
        ("GNOMON_TCSR", "scan", |d| d.adjacency_index = false),
        ("GNOMON_TCSR", " Scan", |d| d.adjacency_index = false),
        ("GNOMON_VIDFILTER", "ignore", |d| d.identity_filter = false),
        ("GNOMON_EXISTENCE_PROBE", "walk", |d| d.existence_count = false),
        ("GNOMON_EXISTENCE_PROBE", "WALK", |d| d.existence_count = false),
        // the seed-driven snapshot and the filtered-scan postings
        ("GNOMON_SEED_EXPANSION", "0", |d| d.seed_expansion = false),
        ("GNOMON_ADJ_FILTERED_SCAN", "0", |d| d.adjacency_filtered_scan = false),
        ("GNOMON_ADJ_FILTERED_SCAN", "False", |d| d.adjacency_filtered_scan = false),
        // the range-native kernels
        ("GNOMON_AGG_RANGE_COUNT", "0", |d| d.agg_range_count = false),
        ("GNOMON_REACH_KERNEL", "python", |d| d.reach_kernel = false),
        ("GNOMON_REACH_KERNEL", " Python ", |d| d.reach_kernel = false),
        ("GNOMON_DIFF_KERNEL", "python", |d| d.diff_kernel = false),
        // the staircase switch, read at open
        ("GNOMON_NO_STAIRCASE", "1", |d| d.staircase = false),
        // The traversal's effective-tt_e column
        ("GNOMON_TT_E_COLUMN", "0", |d| d.tt_e_column = false),
        // the pattern-match kernel
        ("GNOMON_MATCH_KERNEL", "python", |d| d.match_kernel = false),
        ("GNOMON_MATCH_KERNEL", "PYTHON", |d| d.match_kernel = false),
        // step 3: the read kernels' thread count, a number, at most 64
        ("GNOMON_MATCH_THREADS", "32", |d| d.match_threads_set = 32),
        ("GNOMON_MATCH_THREADS", " 2 ", |d| d.match_threads_set = 2),
        ("GNOMON_MATCH_THREADS", "500", |d| d.match_threads_set = 64),
        ("GNOMON_MATCH_PAR_ROWS", "64", |d| d.match_par_rows = 64),
        ("GNOMON_MATCH_PAR_ROWS", " 1000 ", |d| d.match_par_rows = 1000),
        // The temporal-paths kernel
        ("GNOMON_PATHS_KERNEL", "python", |d| d.paths_kernel = false),
        ("GNOMON_PATHS_KERNEL", " Python", |d| d.paths_kernel = false),
    ];
    for (k, v, f) in cases {
        let mut want = Designs::ALL;
        f(&mut want);
        assert_eq!(one(k, v), want, "{k}={v:?}");
    }
    // `GNOMON_VIDFILTER=0` stops building filters only; reads still use the
    // ones on disk (the existing switch keeps its meaning)
    assert_eq!(one("GNOMON_VIDFILTER", "0"), Designs::ALL);
    // and the staircase switch keeps its exact grammar: only "1"
    for v in ["0", "true", " 1", "yes"] {
        assert_eq!(one("GNOMON_NO_STAIRCASE", v), Designs::ALL, "GNOMON_NO_STAIRCASE={v:?}");
    }
    assert_eq!(Designs::ALL.named().iter().filter(|(_, on)| *on).count(), 17);
}

#[test]
fn a_handle_carries_its_designs_and_a_scan_carries_the_handles() {
    let root = tmp("plumbing");
    let mut s = NativeStore::open(&root).unwrap();
    // the test process does not set any of the variables
    assert_eq!(s.designs(), Designs::from_env());
    let off = Designs {
        window_first: false,
        segment_prune: false,
        ..Designs::ALL
    };
    s.set_designs(off);
    assert_eq!(s.designs(), off);
    let set: ScanSet<'_, crate::segment::MemorySource> = ScanSet::new(Vec::new());
    assert_eq!(set.designs(), Designs::ALL, "a bare scan runs every design");
    let set = set.with_designs(off).with_closes(Arc::default());
    assert_eq!(set.designs(), off);
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------- //
// the store fixture and the probe set every switch is held to              //
// ---------------------------------------------------------------------- //

pub(crate) mod fixture {
    use super::*;
    use crate::aggregate::{Agg, AggregateRequest, Dim};
    use crate::derive::{edge_eid, version_vid, Id96};
    use crate::manifest::EventLogRef;
    use crate::read::{Belief, VersionFilter};
    use crate::row::{EdgeRow, RowKind};
    use crate::scan::{ScanRequest, ScanTarget};
    use crate::store::NeighborDir;
    use crate::OPEN_END;

    /// Rows; ingest batch `b` writes the contiguous valid-time block
    /// `[b * K, (b + 1) * K)` at `tt = 100 + b`, so a size-tiered store has
    /// one segment per block and a window can prune all but one.
    pub const N: u32 = 6_000;
    pub const BATCHES: u32 = 12;
    const K: u32 = N / BATCHES;
    pub const ENTITIES: u32 = 40;
    /// Belief instants: before anything, mid-ingest, after ingest and
    /// before the corrections, between two correction commits.
    pub const AS_OF: [i64; 4] = [99, 105, 999, 1_001];
    /// Valid-time windows: inside one block, across a block boundary, a
    /// single tick, past the end, and the whole extent.
    pub const WINDOWS: [(i64, i64); 5] =
        [(1_000, 1_016), (2_490, 2_510), (0, 1), (5_990, 6_100), (-5, 7_000)];

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Layout {
        /// As ingested: one segment per batch, closes in close runs.
        Tiered,
        /// Full-store compaction after the corrections: one edge segment,
        /// dictionary-coded `tt_s`, closes folded into the sidecar.
        SettledAfter,
        /// Full-store compaction before the corrections: one large run-list
        /// segment whose closes sit in close runs, plus the corrections'
        /// own segments.
        SettledBefore,
    }

    pub const LAYOUTS: [Layout; 3] = [Layout::Tiered, Layout::SettledAfter, Layout::SettledBefore];

    pub struct Fixture {
        pub root: std::path::PathBuf,
        pub s: NativeStore,
        pub ids: Vec<u32>,
        pub rows: Vec<EdgeRow>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn edge(i: u32, ids: &[u32], tt_s: i64, props: &str) -> EdgeRow {
        let (s, d) = (i % ENTITIES, (i * 7 + 1) % ENTITIES);
        let (su, du) = (format!("e{s}"), format!("e{d}"));
        let disc = format!("#{i}");
        let rel = if i.is_multiple_of(3) { "A" } else { "B" };
        let eid = edge_eid(&su, &du, rel, &disc);
        EdgeRow {
            vid: version_vid(&eid.to_hex(), tt_s, i as i64),
            src_id: ids[s as usize],
            dst_id: ids[d as usize],
            rel_type: rel.into(),
            disc,
            vt_s: i as i64,
            vt_e: i as i64 + 1,
            tt_s,
            props: props.into(),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    /// Every 10th row (offset 3) is corrected: its version closed and a new
    /// one written, in three commits at `tt = 1000, 1001, 1002`.
    pub fn build(name: &str, layout: Layout) -> Fixture {
        build_with(name, layout, None)
    }

    /// [`build`], with the ingest batches (only) written under `ingest`'s
    /// time codec: corrections and compaction use the default.
    pub fn build_with(
        name: &str,
        layout: Layout,
        ingest: Option<crate::segment::TimeCodec>,
    ) -> Fixture {
        let root = tmp(&format!("{name}-{layout:?}"));
        let mut s = NativeStore::open(&root).unwrap();
        let (p, _) = {
            let (p, t) = s.layout();
            (*p, t)
        };
        s.set_layout(p, 1 << 32);
        let mut ids: Vec<u32> = Vec::new();
        let mut rows: Vec<EdgeRow> = Vec::with_capacity(N as usize);
        for b in 0..BATCHES {
            s.begin(100 + b as i64).unwrap();
            if ids.is_empty() {
                for e in 0..ENTITIES {
                    ids.push(s.ensure_entity(&format!("e{e}"), "Node").unwrap());
                }
            }
            for i in b * K..(b + 1) * K {
                let r = edge(i, &ids, 100 + b as i64, "{}");
                s.stage_edge(r.clone()).unwrap();
                rows.push(r);
            }
            match ingest {
                Some(c) => crate::segment::with_time_codec(c, || s.commit(EventLogRef::default())),
                None => s.commit(EventLogRef::default()),
            }
            .unwrap();
        }
        if layout == Layout::SettledBefore {
            s.settle(None).unwrap();
        }
        let corrected: Vec<EdgeRow> = rows.iter().filter(|r| r.vt_s % 10 == 3).cloned().collect();
        for (k, chunk) in corrected.chunks(200).enumerate() {
            let tt = 1_000 + k as i64;
            s.begin(tt).unwrap();
            for r in chunk {
                s.close_version_at(RowKind::Edge, r.vid, Some(r.vt_s), tt).unwrap();
                s.stage_edge(edge(r.vt_s as u32, &ids, tt, "{\"v\":2}")).unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        if layout == Layout::SettledAfter {
            s.settle(None).unwrap();
        }
        let f = Fixture { root, s, ids, rows };
        f.assert_shape(layout);
        f
    }

    impl Fixture {
        /// The fixture must take the shape its layout names, or a switch
        /// tested on it could be vacuous.
        fn assert_shape(&self, layout: Layout) {
            let files = self.s.edge_files();
            let mut dict = 0;
            let mut folded = 0;
            for (file, _) in &files {
                let seg = self.s.open_segment(file).unwrap();
                dict += seg.tt_s_view().unwrap().is_dictionary() as usize;
                folded += !seg.sidecar().all_current() as usize;
            }
            let runs = !self.s.close_index().unwrap().is_empty();
            match layout {
                Layout::Tiered => {
                    assert!(files.len() >= BATCHES as usize, "{layout:?}: {} segments", files.len());
                    assert!(runs, "{layout:?}: closes must sit in close runs");
                }
                Layout::SettledAfter => {
                    assert_eq!(files.len(), 1, "{layout:?}");
                    // (a store written under a non-adaptive time codec has no
                    // dictionary by construction)
                    if crate::segment::TimeCodec::from_env() == crate::segment::TimeCodec::Auto {
                        assert_eq!(dict, 1, "{layout:?}: tt_s must be dictionary-coded");
                    }
                    assert_eq!(folded, 1, "{layout:?}: closes must be folded");
                }
                Layout::SettledBefore => {
                    assert!(files.len() >= 2, "{layout:?}");
                    assert!(runs, "{layout:?}: closes must sit in close runs");
                }
            }
        }
    }

    pub fn now() -> i64 {
        crate::clamp_tt(OPEN_END)
    }

    /// Every probe, rendered to text: aggregates, belief diffs, version
    /// history, traversal, a planner scan built the way the binding builds
    /// one, and row location. Each `(name, answer)` must be identical under
    /// every switch.
    pub fn answers(f: &Fixture) -> Vec<(String, String)> {
        let s = &f.s;
        let mut out: Vec<(String, String)> = Vec::new();
        let mut as_ofs = AS_OF.to_vec();
        as_ofs.push(now());

        for &as_of in &as_ofs {
            for &(a, b) in &WINDOWS {
                let r = s
                    .aggregate_edges(&AggregateRequest {
                        as_of_tt: as_of,
                        t_a: a,
                        t_b: b,
                        rel_types: None,
                        dims: vec![Dim::TimeBucket { stride: 7 }, Dim::RelType],
                        aggs: vec![Agg::Count, Agg::CountDistinct { dst: true }],
                        max_groups: 1 << 20,
                    })
                    .unwrap();
                // Relation codes index the names of the segments the call
                // opened, so they are compared as names: which segments are
                // opened is exactly what pruning changes.
                let rels: Vec<&str> =
                    r.keys[1].iter().map(|&c| r.rel_names[c as usize].as_str()).collect();
                out.push((
                    format!("agg as_of={as_of} [{a},{b})"),
                    format!("{:?} {rels:?} {:?}", r.keys[0], r.aggs),
                ));
                let rel = s
                    .aggregate_edges(&AggregateRequest {
                        as_of_tt: as_of,
                        t_a: a,
                        t_b: b,
                        rel_types: Some(vec!["A".into()]),
                        dims: vec![Dim::Endpoint { dst: false }],
                        aggs: vec![Agg::Count],
                        max_groups: 1 << 20,
                    })
                    .unwrap();
                out.push((
                    format!("agg rel=A as_of={as_of} [{a},{b})"),
                    format!("{:?} {:?}", rel.keys, rel.aggs),
                ));
            }
        }
        // the shapes the range-count path answers -- by bucket,
        // ungrouped, and under a relation filter that splits every segment --
        // with the row counter beside the answer
        for &as_of in &as_ofs {
            for &(a, b) in &WINDOWS {
                for (dims, rels) in [
                    (vec![Dim::TimeBucket { stride: 7 }], None),
                    (vec![], None),
                    (vec![Dim::TimeBucket { stride: 50 }], Some(vec!["A".to_string()])),
                ] {
                    let r = s
                        .aggregate_edges(&AggregateRequest {
                            as_of_tt: as_of,
                            t_a: a,
                            t_b: b,
                            rel_types: rels.clone(),
                            dims: dims.clone(),
                            aggs: vec![Agg::Count],
                            max_groups: 1 << 20,
                        })
                        .unwrap();
                    out.push((
                        format!("agg count {dims:?} {rels:?} as_of={as_of} [{a},{b})"),
                        format!("{:?} {:?} rows_selected={}", r.keys, r.aggs, r.stats.rows_selected),
                    ));
                }
            }
        }
        for (t1, t2) in [(99, now()), (105, now()), (999, now()), (999, 1_001), (1_001, 1_002), (0, 105)] {
            for &(a, b) in &WINDOWS {
                let d = s.diff_belief(t1, t2, a, b, usize::MAX).unwrap();
                out.push((format!("diff ({t1},{t2}] [{a},{b})"), format!("{d:?}")));
            }
        }
        let mut vk_as_of = as_ofs.clone();
        vk_as_of.push(OPEN_END);
        for belief in [Belief::Current, Belief::Superseded, Belief::All] {
            for &as_of in &vk_as_of {
                for &(a, b) in &WINDOWS {
                    for rels in [None, Some(&["A".to_string()][..])] {
                        let keys = s
                            .version_keys(
                                RowKind::Edge,
                                &VersionFilter { as_of, t_a: a, t_b: b, belief, rel_types: rels },
                            )
                            .unwrap();
                        out.push((
                            format!("version_keys {belief:?} {as_of} [{a},{b}) {rels:?}"),
                            format!("{keys:?}"),
                        ));
                    }
                }
            }
        }
        for &as_of in &as_ofs {
            for dir in [NeighborDir::Out, NeighborDir::In, NeighborDir::Both] {
                for (vt_min, vt_max) in [(None, None), (Some(1_000), Some(1_500))] {
                    for rels in [None, Some(&["B".to_string()][..])] {
                        let n = s.neighbors_batch(&f.ids, dir, as_of, vt_min, vt_max, rels).unwrap();
                        out.push((
                            format!("neighbors {dir:?} {as_of} {vt_min:?}..{vt_max:?} {rels:?}"),
                            format!("{n:?}"),
                        ));
                    }
                }
            }
        }
        // the planner, through a ScanSet built as the binding's scan_edges
        // builds one: every live edge segment, the handle's closes and designs
        let segs: Vec<_> = s
            .edge_files()
            .iter()
            .map(|(file, id)| (s.open_segment(file).unwrap(), *id))
            .collect();
        let targets: Vec<ScanTarget<'_, crate::segment::MmapSource>> = segs
            .iter()
            .map(|(seg, id)| ScanTarget { segment: seg, lane: seg.header().lane, id: *id })
            .collect();
        let set = ScanSet::new(targets)
            .with_closes(s.close_index().unwrap())
            .with_designs(s.designs());
        for &as_of in &as_ofs {
            for &(a, b) in &WINDOWS {
                for touching in [None, Some(vec![f.ids[3], f.ids[11]])] {
                    let req = ScanRequest {
                        as_of_tt: as_of,
                        vt_min: Some(a),
                        vt_max: Some(b),
                        touching_ids: touching.clone(),
                        ..ScanRequest::default()
                    };
                    let (sel, stats) = set.select(&req).unwrap();
                    let rows: Vec<(u64, Vec<u32>)> =
                        sel.iter().map(|x| (segs[x.segment].1, x.rows.clone())).collect();
                    out.push((
                        format!("scan {as_of} [{a},{b}) {touching:?}"),
                        format!("{rows:?} rows_examined={}", stats.rows_examined),
                    ));
                }
            }
            let (sel, _) = set.select(&ScanRequest { as_of_tt: as_of, ..ScanRequest::default() }).unwrap();
            let n: usize = sel.iter().map(|x| x.rows.len()).sum();
            out.push((format!("scan whole {as_of}"), format!("{n}")));
        }
        for r in f.rows.iter().step_by(97) {
            let a = s.locate_vid(RowKind::Edge, r.vid, Some(r.vt_s)).unwrap();
            let b = s.locate_vid(RowKind::Edge, r.vid, None).unwrap();
            out.push((format!("locate {}", r.vt_s), format!("{a:?} {b:?}")));
        }
        let absent = Id96 { hi: 1, lo: 2 };
        out.push((
            "locate absent".into(),
            format!("{:?}", s.locate_vid(RowKind::Edge, absent, Some(1_000)).unwrap()),
        ));
        out
    }

    /// `answers` with `d`, compared probe by probe against every design on.
    pub fn assert_same_answers(f: &mut Fixture, d: Designs, ctx: &str) {
        f.s.set_designs(Designs::ALL);
        let want = answers(f);
        f.s.set_designs(d);
        let got = answers(f);
        f.s.set_designs(Designs::ALL);
        assert_eq!(want.len(), got.len());
        for ((name, w), (_, g)) in want.iter().zip(&got) {
            assert_eq!(w, g, "{ctx}: `{name}` changed under {d:?}");
        }
        // and the probes are not vacuous: something is believed somewhere
        assert!(want.iter().any(|(n, a)| n.starts_with("diff") && a.contains("Retracted")));
    }

    /// The thread's touch counts over one full `answers` pass with `d`.
    #[cfg(feature = "ingest-timers")]
    pub fn touch_of(f: &mut Fixture, d: Designs) -> crate::vistouch::VisTouch {
        f.s.set_designs(d);
        crate::vistouch::take();
        // the fixture is far below the parallel gates (4M rows), so the
        // planner and the fold run on this thread and every count lands here
        let _ = answers(f);
        let t = crate::vistouch::take();
        f.s.set_designs(Designs::ALL);
        t
    }
}

// ---------------------------------------------------------------------- //
// count by time bucket from believed ranges (GNOMON_AGG_RANGE_COUNT=0)     //
// ---------------------------------------------------------------------- //

#[test]
fn agg_range_count_off_answers_identically() {
    use fixture::*;
    for layout in LAYOUTS {
        let mut f = build("k2-id", layout);
        let off = Designs { agg_range_count: false, ..Designs::ALL };
        assert_same_answers(&mut f, off, &format!("{layout:?}"));
        let both = Designs { agg_range_count: false, window_first: false, ..Designs::ALL };
        assert_same_answers(&mut f, both, &format!("{layout:?} and window-first"));
    }
}

// ---------------------------------------------------------------------- //
// switch 4: segment pruning (GNOMON_SEGMENT_PRUNE=0)                         //
// ---------------------------------------------------------------------- //

#[test]
fn pruning_off_answers_identically() {
    use fixture::*;
    for layout in LAYOUTS {
        let mut f = build("s4-id", layout);
        let off = Designs { segment_prune: false, ..Designs::ALL };
        assert_same_answers(&mut f, off, &format!("{layout:?}"));
    }
}

#[test]
fn pruning_off_opens_every_segment_an_aggregate_could_skip() {
    use crate::aggregate::{Agg, AggregateRequest, Dim};
    use fixture::*;
    let mut f = build("s4-agg", Layout::Tiered);
    let req = AggregateRequest {
        as_of_tt: now(),
        t_a: 1_000,
        t_b: 1_016,
        rel_types: None,
        dims: vec![Dim::TimeBucket { stride: 1 }],
        aggs: vec![Agg::Count],
        max_groups: 1 << 20,
    };
    let on = f.s.aggregate_edges(&req).unwrap().stats;
    assert!(on.segments_pruned > 0, "the fixture must give the prune something to skip: {on:?}");
    f.s.set_designs(Designs { segment_prune: false, ..Designs::ALL });
    let off = f.s.aggregate_edges(&req).unwrap().stats;
    // falsifier: segments_pruned > 0 with the switch set
    assert_eq!(off.segments_pruned, 0, "the manifest prune still ran: {off:?}");
    assert_eq!(off.segments_opened, off.segments_total, "{off:?}");
    assert_eq!(off.segments_total, on.segments_total);
}

/// Every one of the six prune sites stops pruning: the planner, the
/// aggregate's manifest prune, the belief diff, version history, traversal
/// and row location.
#[cfg(feature = "ingest-timers")]
#[test]
fn pruning_off_prunes_nothing_at_any_site() {
    use fixture::*;
    let mut f = build("s4-touch", Layout::Tiered);
    let on = touch_of(&mut f, Designs::ALL);
    let off = touch_of(&mut f, Designs { segment_prune: false, ..Designs::ALL });
    assert!(on.pruned > 0, "every design on: nothing was pruned ({on:?}); the probes are vacuous");
    // falsifier: any prune counted with the switch set
    assert_eq!(off.pruned, 0, "GNOMON_SEGMENT_PRUNE=0 still pruned {} segments", off.pruned);
    // and per site, so one site that ignores the switch is named
    let site = |name: &str, d: Designs, run: &dyn Fn(&NativeStore)| {
        let mut f = build(&format!("s4-{name}"), Layout::Tiered);
        f.s.set_designs(d);
        crate::vistouch::take();
        run(&f.s);
        crate::vistouch::take().pruned
    };
    type Site = Box<dyn Fn(&NativeStore)>;
    let sites: Vec<(&str, Site)> = vec![
        ("diff", Box::new(|s: &NativeStore| {
            s.diff_belief(999, now(), 1_000, 1_016, usize::MAX).unwrap();
        })),
        ("history", Box::new(|s: &NativeStore| {
            s.version_keys(
                crate::row::RowKind::Edge,
                &crate::read::VersionFilter {
                    as_of: now(),
                    t_a: 1_000,
                    t_b: 1_016,
                    belief: crate::read::Belief::Current,
                    rel_types: None,
                },
            )
            .unwrap();
        })),
        ("traversal", Box::new(|s: &NativeStore| {
            s.neighbors_batch(&[0, 1, 2], crate::store::NeighborDir::Both, now(), Some(1_000), Some(1_016), None)
                .unwrap();
        })),
        ("locate", Box::new(|s: &NativeStore| {
            let r = &build_rows_probe(s);
            s.locate_vid(crate::row::RowKind::Edge, r.0, Some(r.1)).unwrap();
        })),
    ];
    for (name, run) in &sites {
        let on = site(name, Designs::ALL, run.as_ref());
        let off = site(name, Designs { segment_prune: false, ..Designs::ALL }, run.as_ref());
        assert!(on > 0, "{name}: nothing pruned with every design on");
        assert_eq!(off, 0, "{name}: pruned {off} segments under GNOMON_SEGMENT_PRUNE=0");
    }
}

/// A committed row's `(vid, vt_s)`, read back from the store (the row at
/// `vt_s = 1000`, in the fifth of the twelve ingest segments).
#[cfg(feature = "ingest-timers")]
fn build_rows_probe(s: &NativeStore) -> (crate::derive::Id96, i64) {
    for (file, _) in s.edge_files() {
        let seg = s.open_segment(&file).unwrap();
        let vt = seg.i64_column("vt_s").unwrap();
        if let Some(i) = vt.iter().position(|&t| t == 1_000) {
            return (seg.vid_at(i).unwrap(), 1_000);
        }
    }
    panic!("no row at vt_s = 1000");
}

// ---------------------------------------------------------------------- //
// switch 2: window-first visibility (GNOMON_WINDOW_FIRST=0)                  //
// ---------------------------------------------------------------------- //

#[test]
fn window_first_off_answers_identically() {
    use fixture::*;
    for layout in LAYOUTS {
        let mut f = build("s2-id", layout);
        let off = Designs { window_first: false, ..Designs::ALL };
        assert_same_answers(&mut f, off, &format!("{layout:?}"));
        let both = Designs { window_first: false, segment_prune: false, ..Designs::ALL };
        assert_same_answers(&mut f, both, &format!("{layout:?} with pruning off too"));
    }
}

/// Each of the four window-first sites goes back to whole-segment work: on the
/// settled store (one 6,600-row dictionary-coded segment, a belief time that
/// splits its first code block) a 16-tick window reads a few dozen items
/// window-first and thousands with the switch set.
#[cfg(feature = "ingest-timers")]
#[test]
fn window_first_off_reads_the_whole_segment_at_every_site() {
    use crate::read::{Belief, VersionFilter};
    use crate::row::RowKind;
    use crate::scan::{ScanRequest, ScanTarget};
    use crate::store::NeighborDir;
    use fixture::*;
    let mut f = build("s2-touch", Layout::SettledAfter);
    let off = Designs { window_first: false, ..Designs::ALL };
    type Site = Box<dyn Fn(&Fixture)>;
    let sites: Vec<(&str, Site)> = vec![
        ("planner", Box::new(|f: &Fixture| {
            let s = &f.s;
            let segs: Vec<_> =
                s.edge_files().iter().map(|(file, id)| (s.open_segment(file).unwrap(), *id)).collect();
            let targets: Vec<ScanTarget<'_, crate::segment::MmapSource>> = segs
                .iter()
                .map(|(seg, id)| ScanTarget { segment: seg, lane: seg.header().lane, id: *id })
                .collect();
            let set = ScanSet::new(targets).with_closes(s.close_index().unwrap()).with_designs(s.designs());
            let req = ScanRequest { as_of_tt: 105, ..ScanRequest::current().window(1_000, 1_016) };
            set.select(&req).unwrap();
        })),
        ("diff", Box::new(|f: &Fixture| {
            f.s.diff_belief(999, now(), 1_000, 1_016, usize::MAX).unwrap();
        })),
        ("history", Box::new(|f: &Fixture| {
            let vf = VersionFilter { as_of: 105, t_a: 1_000, t_b: 1_016, belief: Belief::Current, rel_types: None };
            f.s.version_keys(RowKind::Edge, &vf).unwrap();
        })),
        ("traversal", Box::new(|f: &Fixture| {
            // e4: 150 adjacency rows, none corrected, so window-first takes
            // the per-row path (e3's 300 rows make the range build cheaper)
            f.s.neighbors_batch(&[f.ids[4]], NeighborDir::Out, 105, None, None, None).unwrap();
        })),
    ];
    for (name, run) in &sites {
        f.s.set_designs(Designs::ALL);
        crate::vistouch::take();
        run(&f);
        let on = crate::vistouch::take();
        f.s.set_designs(off);
        run(&f);
        let whole = crate::vistouch::take();
        // window-first reads what the window holds; the whole-segment path
        // reads every straddling code block (4,096 codes here) and the
        // segment's closes. Falsifier: `whole` within 4x of `on`.
        assert!(on.items() <= 200, "{name}: window-first read {on:?}");
        assert!(
            whole.items() >= 4_000 && whole.items() >= 4 * on.items(),
            "{name}: GNOMON_WINDOW_FIRST=0 read {whole:?} against window-first {on:?}; \
             the switch is not reaching this site"
        );
    }
}

/// The scan planner's whole-segment path is the one whose cost follows the
/// segment: the same 16-row window in segments of 16,384 and 262,144 rows.
#[cfg(feature = "ingest-timers")]
#[test]
fn window_first_off_makes_a_windowed_scan_follow_the_segment_size() {
    use crate::derive::Id96;
    use crate::row::{EdgeRow, Lane};
    use crate::scan::ScanRequest;
    use crate::segment::{write_edge_segment, MemorySource, Segment, SegmentSpec};
    let dir = tmp("s2-sizes");
    std::fs::create_dir_all(&dir).unwrap();
    let mut items = Vec::new();
    for n in [16_384u32, 262_144] {
        let rows: Vec<EdgeRow> = (0..n)
            .map(|i| EdgeRow {
                vid: Id96 { hi: i as u64 + 1, lo: 7 },
                src_id: i % 97,
                dst_id: (i + 1) % 97,
                rel_type: "R".into(),
                disc: String::new(),
                vt_s: i as i64,
                vt_e: i as i64 + 1,
                // 0.4 runs per row: dictionary-coded, every block straddles 150
                tt_s: if i % 5 == 2 { 200 } else { 100 },
                props: "{}".into(),
                source: "s".into(),
                provenance_ref: None,
            })
            .collect();
        let path = dir.join(format!("{n}.tgs"));
        write_edge_segment(&path, &rows, &SegmentSpec::default()).unwrap();
        let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        let mut per = Vec::new();
        for d in [Designs::ALL, Designs { window_first: false, ..Designs::ALL }] {
            let set = ScanSet::from_pairs(vec![(&seg, Lane::Event)]).with_designs(d);
            let req = ScanRequest { as_of_tt: 150, ..ScanRequest::current().window(8_000, 8_016) };
            crate::vistouch::take();
            let (sel, _) = set.select_units_at(&req, 1, 1 << 20).unwrap();
            per.push((crate::vistouch::take().items(), sel));
        }
        assert_eq!(per[0].1, per[1].1, "n={n}: the two paths selected different rows");
        items.push((per[0].0, per[1].0));
    }
    let ((on_s, off_s), (on_l, off_l)) = (items[0], items[1]);
    assert_eq!(on_s, on_l, "window-first: the counts must not follow the segment ({items:?})");
    // falsifier: the whole-segment counts equal at both sizes
    assert!(off_l >= 8 * off_s, "GNOMON_WINDOW_FIRST=0: {items:?}; 16x more rows must read at least 8x more");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------- //
// switch 1: per-row belief test (GNOMON_RANGE_VISIBILITY=0)                  //
// ---------------------------------------------------------------------- //

/// The per-row primitive selects exactly the rows the range-domain
/// primitives select, clipped to the domain, in canonical form: random
/// segments of both `tt_s` shapes, closes folded and in layered close runs
/// (including run entries equal to `OPEN_END`), random domains, all three
/// modes, every belief time a read path can pass it (the unclamped
/// `OPEN_END` is `version_keys`' special case and never reaches it).
#[test]
fn the_per_row_belief_test_selects_exactly_the_range_domain_rows() {
    use crate::derive::Id96;
    use crate::row::{EdgeRow, RowKind};
    use crate::segment::{write_edge_segment, MemorySource, Segment, SegmentSpec};
    use crate::visibility::{believed_ranges_per_row, close_view, CloseIndex, CloseRecord, RowBelief};
    use crate::OPEN_END;
    let dir = tmp("s1-prop");
    std::fs::create_dir_all(&dir).unwrap();
    let mut seed = 0x2910_0429_u64;
    let mut rng = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let clip = |ranges: Vec<(usize, usize)>, domain: &[(usize, usize)]| {
        let mut out: Vec<(usize, usize)> = Vec::new();
        for &(a, b) in domain {
            for &(rs, re) in &ranges {
                let (x, y) = (rs.max(a), re.min(b));
                if x < y {
                    match out.last_mut() {
                        Some(last) if last.1 == x => last.1 = y,
                        _ => out.push((x, y)),
                    }
                }
            }
        }
        out
    };
    let mut shapes = [0usize; 2];
    for case in 0..160 {
        let n = 20 + (rng() % 3_000) as usize;
        let dictionary = case % 2 == 0;
        let flip = if dictionary { 3 } else { 200 };
        let mut tt = 100i64;
        let rows: Vec<EdgeRow> = (0..n)
            .map(|i| {
                if rng() % flip == 0 {
                    tt = 100 + (rng() % 8) as i64 * 50;
                }
                EdgeRow {
                    vid: Id96 { hi: i as u64 + 1, lo: 3 },
                    src_id: (i % 13) as u32,
                    dst_id: (i % 7) as u32,
                    rel_type: "R".into(),
                    disc: String::new(),
                    vt_s: i as i64,
                    vt_e: i as i64 + 1,
                    tt_s: tt,
                    props: "{}".into(),
                    source: "s".into(),
                    provenance_ref: None,
                }
            })
            .collect();
        let mut folded: Vec<(u32, i64)> = Vec::new();
        for r in 0..n as u32 {
            if rng() % 7 == 0 {
                folded.push((r, 120 + (rng() % 8) as i64 * 50));
            }
        }
        let path = dir.join(format!("{case}.tgs"));
        let spec = SegmentSpec {
            tt_s_dictionary: dictionary,
            closed_rows: folded,
            ..SegmentSpec::default()
        };
        write_edge_segment(&path, &rows, &spec).unwrap();
        let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        let tt_v = seg.tt_s_view().unwrap();
        shapes[tt_v.is_dictionary() as usize] += 1;
        let rec = |row: u32, tt_e: i64| CloseRecord { kind: RowKind::Edge, segment: 0, row, tt_e };
        let mut recs = Vec::new();
        for r in 0..n as u32 {
            match rng() % 9 {
                0 => recs.push(rec(r, 130 + (rng() % 8) as i64 * 50)),
                1 => recs.push(rec(r, OPEN_END)),
                _ => {}
            }
        }
        let committed = Arc::new(CloseIndex::from_records(recs));
        let closes = if case % 3 == 0 {
            let mut top = CloseIndex::layered_over(committed.clone());
            for r in 0..n as u32 {
                if rng() % 11 == 0 {
                    top.close_row(0, r, 140 + (rng() % 8) as i64 * 50);
                }
            }
            top
        } else {
            CloseIndex::clone(&committed)
        };
        let view = close_view(seg.sidecar(), &closes, 0);
        let bounds = seg.tt_s_bounds();
        let mut cuts: Vec<usize> = (0..4).map(|_| (rng() as usize) % (n + 1)).collect();
        cuts.sort_unstable();
        let domains: Vec<Vec<(usize, usize)>> = vec![
            vec![(0, n)],
            vec![(cuts[0], cuts[2])],
            vec![(cuts[0], cuts[1]), (cuts[1], cuts[3])],
            vec![(cuts[0], cuts[1]), (cuts[2], cuts[3])],
            vec![(cuts[1], cuts[1])],
        ];
        for as_of in [0, 99, 100, 149, 150, 300, 451, 999, OPEN_END - 1] {
            let modes = [
                (RowBelief::Current, crate::scan::believed_ranges_closed(&tt_v, bounds, as_of, n, &view)),
                (RowBelief::Written, crate::scan::believed_ranges_any(&tt_v, bounds, as_of, n)),
                (RowBelief::Superseded, crate::scan::believed_ranges_superseded(&tt_v, bounds, as_of, n, &view)),
            ];
            for (mode, whole) in &modes {
                for d in &domains {
                    let got = believed_ranges_per_row(&tt_v, seg.sidecar(), &closes, 0, as_of, d, n, *mode)
                        .unwrap();
                    assert_eq!(
                        got,
                        clip(whole.clone(), d),
                        "case {case} (dict={dictionary}) n={n} as_of={as_of} {mode:?} domain={d:?}"
                    );
                }
            }
        }
    }
    assert!(shapes[0] > 20 && shapes[1] > 20, "both tt_s shapes must be exercised: {shapes:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn range_visibility_off_answers_identically() {
    use fixture::*;
    for layout in LAYOUTS {
        let mut f = build("s1-id", layout);
        let off = Designs { range_visibility: false, ..Designs::ALL };
        assert_same_answers(&mut f, off, &format!("{layout:?}"));
        for (what, d) in [
            ("and window-first", Designs { window_first: false, ..off }),
            ("and pruning", Designs { segment_prune: false, ..off }),
        ] {
            assert_same_answers(&mut f, d, &format!("{layout:?} {what} off"));
        }
    }
}

/// With the switch set no run, code or close is read by any belief test,
/// and rows are tested one by one instead, at every site.
#[cfg(feature = "ingest-timers")]
#[test]
fn range_visibility_off_tests_rows_and_reads_no_ranges_at_any_site() {
    use crate::aggregate::{Agg, AggregateRequest, Dim};
    use crate::read::{Belief, VersionFilter};
    use crate::row::RowKind;
    use crate::store::NeighborDir;
    use fixture::*;
    for layout in LAYOUTS {
        let mut f = build("s1-touch", layout);
        let off = Designs { range_visibility: false, ..Designs::ALL };
        let on_all = touch_of(&mut f, Designs::ALL);
        let off_all = touch_of(&mut f, off);
        assert_eq!(on_all.rows, 0, "{layout:?}: every design on tested rows one by one: {on_all:?}");
        assert!(on_all.items() > 0, "{layout:?}: the probes read no ranges at all: {on_all:?}");
        // falsifier: rows == 0 with the switch set, or any range item read
        assert!(off_all.rows > 0, "{layout:?}: GNOMON_RANGE_VISIBILITY=0 tested no row: {off_all:?}");
        assert_eq!(off_all.items(), 0, "{layout:?}: range-domain items read with the switch set: {off_all:?}");

        let ids4: Vec<u32> = f.ids[..4].to_vec();
        type Site = Box<dyn Fn(&NativeStore)>;
        let sites: Vec<(&str, Site)> = vec![
            ("aggregate", Box::new(|s: &NativeStore| {
                s.aggregate_edges(&AggregateRequest {
                    as_of_tt: 105,
                    t_a: 0,
                    t_b: 7_000,
                    rel_types: None,
                    dims: vec![Dim::RelType],
                    aggs: vec![Agg::Count],
                    max_groups: 1 << 20,
                })
                .unwrap();
            })),
            ("diff", Box::new(|s: &NativeStore| {
                s.diff_belief(999, now(), 0, 7_000, usize::MAX).unwrap();
            })),
            ("history", Box::new(|s: &NativeStore| {
                let vf = VersionFilter { as_of: 1_001, t_a: 0, t_b: 7_000, belief: Belief::Superseded, rel_types: None };
                s.version_keys(RowKind::Edge, &vf).unwrap();
            })),
            ("traversal", Box::new(move |s: &NativeStore| {
                s.neighbors_batch(&ids4, NeighborDir::Both, 1_001, None, None, None).unwrap();
            })),
        ];
        for (name, run) in &sites {
            f.s.set_designs(off);
            crate::vistouch::take();
            run(&f.s);
            let t = crate::vistouch::take();
            f.s.set_designs(Designs::ALL);
            assert!(t.rows > 0 && t.items() == 0, "{layout:?} {name}: {t:?}");
        }
    }
}

// ---------------------------------------------------------------------- //
// switch 5: adjacency by scanning endpoint columns (GNOMON_TCSR=scan)        //
// ---------------------------------------------------------------------- //

/// The scan yields, per vertex and direction, exactly the index's slice:
/// the same rows, ascending.
#[test]
fn a_scanned_adjacency_bucket_is_the_index_slice() {
    use crate::store::ScanAdjacency;
    use crate::tcsr::Direction;
    use fixture::*;
    for layout in LAYOUTS {
        let f = build("s5-slices", layout);
        let us: Vec<u32> = (0..ENTITIES + 3).collect();
        let dirs = [Direction::Out, Direction::In];
        for (file, _) in f.s.edge_files() {
            let seg = f.s.open_segment(&file).unwrap();
            let idx = f.s.tcsr_index(&file).unwrap();
            let scan = ScanAdjacency::build(&seg, &us, &dirs).unwrap();
            for &u in &us {
                for d in dirs {
                    assert_eq!(scan.slice(d, u), idx.dir(d).slice(u), "{layout:?} {file} u={u} {d:?}");
                }
            }
            // a vertex outside the call's frontier has no bucket
            let one = ScanAdjacency::build(&seg, &[us[1]], &[Direction::Out]).unwrap();
            assert!(one.slice(Direction::Out, us[2]).is_empty());
            assert!(one.slice(Direction::In, us[1]).is_empty());
        }
    }
}

#[test]
fn adjacency_scan_answers_identically() {
    use fixture::*;
    for layout in LAYOUTS {
        let mut f = build("s5-id", layout);
        let off = Designs { adjacency_index: false, ..Designs::ALL };
        assert_same_answers(&mut f, off, &format!("{layout:?}"));
        for (what, d) in [
            ("and range visibility", Designs { range_visibility: false, ..off }),
            ("and window-first", Designs { window_first: false, ..off }),
            ("and pruning", Designs { segment_prune: false, ..off }),
        ] {
            assert_same_answers(&mut f, d, &format!("{layout:?} {what} off"));
        }
    }
}

/// With the switch set no adjacency index is loaded, built or written, and
/// the endpoint columns are scanned instead.
#[cfg(feature = "ingest-timers")]
#[test]
fn adjacency_scan_never_touches_an_index() {
    use crate::store::NeighborDir;
    use fixture::*;
    for layout in LAYOUTS {
        let mut f = build("s5-touch", layout);
        let tci = |f: &Fixture| {
            std::fs::read_dir(f.root.join("idx").join("tcsr"))
                .map(|d| d.count())
                .unwrap_or(0)
        };
        let before = tci(&f);
        f.s.set_designs(Designs { adjacency_index: false, ..Designs::ALL });
        crate::vistouch::take();
        for as_of in [105, 1_001, now()] {
            f.s.neighbors_batch(&f.ids, NeighborDir::Both, as_of, None, None, None).unwrap();
            f.s.neighbors_batch(&f.ids[..1], NeighborDir::Out, as_of, Some(1_000), Some(1_500), None).unwrap();
        }
        let scanned = crate::vistouch::take();
        let (_, built, loaded, ..) = f.s.tcsr_stats();
        // falsifier: an index built or loaded, or no row scanned
        assert_eq!((built, loaded), (0, 0), "{layout:?}: GNOMON_TCSR=scan used the index");
        assert_eq!(tci(&f), before, "{layout:?}: an index file was written");
        assert!(scanned.adj > 0, "{layout:?}: no endpoint row was scanned: {scanned:?}");
        f.s.set_designs(Designs::ALL);
        f.s.neighbors_batch(&f.ids, NeighborDir::Both, now(), None, None, None).unwrap();
        let t = crate::vistouch::take();
        let (_, built, loaded, ..) = f.s.tcsr_stats();
        assert!(built + loaded > 0, "{layout:?}: every design on, no index was used");
        assert_eq!(t.adj, 0, "{layout:?}: every design on, endpoint rows were scanned: {t:?}");
    }
}

// ---------------------------------------------------------------------- //
// switch 3: raw time columns (GNOMON_TIME_CODEC=raw | runs), write time      //
// ---------------------------------------------------------------------- //

mod time_codec {
    use super::*;
    use crate::codec::CODEC_RAW;
    use crate::derive::Id96;
    use crate::row::{EdgeRow, NodeRow};
    use crate::segment::{
        write_edge_segment, write_node_segment, MemorySource, Segment, SegmentSpec, TimeCodec,
        SEGMENT_FORMAT_RAW_TT, TT_S_RAW_COLUMN,
    };
    use crate::OPEN_END;

    #[test]
    fn the_variable_parses_to_three_codecs() {
        assert_eq!(TimeCodec::parse(None), TimeCodec::Auto);
        for v in ["", "auto", "0", "garbage"] {
            assert_eq!(TimeCodec::parse(Some(v)), TimeCodec::Auto, "{v:?}");
        }
        assert_eq!(TimeCodec::parse(Some("raw")), TimeCodec::Raw);
        assert_eq!(TimeCodec::parse(Some(" RAW ")), TimeCodec::Raw);
        assert_eq!(TimeCodec::parse(Some("runs")), TimeCodec::Runs);
    }

    /// `tt_s` shredded at 0.4 runs per row (dictionary-coded by default), an
    /// interval `vt_e` on every third row.
    fn rows(n: u32) -> Vec<EdgeRow> {
        (0..n)
            .map(|i| EdgeRow {
                vid: Id96 { hi: i as u64 + 1, lo: 9 },
                src_id: i % 11,
                dst_id: (i + 3) % 11,
                rel_type: "R".into(),
                disc: String::new(),
                vt_s: 1_000 + 7 * i as i64,
                vt_e: 1_000 + 7 * i as i64 + if i % 3 == 0 { 50 } else { 1 },
                tt_s: if i % 5 == 2 { 200 + (i % 4) as i64 } else { 100 },
                props: "{}".into(),
                source: "s".into(),
                provenance_ref: None,
            })
            .collect()
    }

    fn write(dir: &std::path::Path, name: &str, codec: TimeCodec, n: u32) -> (Segment<MemorySource>, u64) {
        let path = dir.join(format!("{name}.tgs"));
        let spec = SegmentSpec {
            time_codec: codec,
            closed_rows: vec![(4, 150), (9, 300)],
            ..SegmentSpec::default()
        };
        write_edge_segment(&path, &rows(n), &spec).unwrap();
        let bytes = std::fs::metadata(&path).unwrap().len();
        let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        (seg, bytes)
    }

    fn preamble_format(seg_path: &std::path::Path) -> u32 {
        let b = std::fs::read(seg_path).unwrap();
        u32::from_le_bytes(b[4..8].try_into().unwrap())
    }

    #[test]
    fn raw_writes_tt_s_and_the_time_columns_uncompressed_as_format_2() {
        let dir = tmp("s3-shape");
        std::fs::create_dir_all(&dir).unwrap();
        let n = 20_000;
        let (auto, auto_bytes) = write(&dir, "auto", TimeCodec::Auto, n);
        let (runs, _) = write(&dir, "runs", TimeCodec::Runs, n);
        let (raw, raw_bytes) = write(&dir, "raw", TimeCodec::Raw, n);

        assert!(auto.tt_s_view().unwrap().is_dictionary(), "the fixture must shred tt_s");
        assert_eq!(preamble_format(&dir.join("auto.tgs")), crate::FORMAT_VERSION);

        // runs: the run list at every density, nothing else changes
        let h = runs.header();
        assert!(!runs.tt_s_view().unwrap().is_dictionary());
        assert!(!h.tt_s_runs.is_empty() && h.tt_s_dict.is_empty() && h.tt_s_code_blocks.is_empty());
        assert_eq!(h.format, crate::FORMAT_VERSION);

        // raw: no runs, no dictionary, no block bounds; a raw tt_s column;
        // vt_s and vt_e raw; segment-level bounds kept; format 2, so a build
        // that knows only format 1 (FORMAT_VERSION) refuses the file
        let h = raw.header();
        assert!(h.tt_s_runs.is_empty() && h.tt_s_dict.is_empty() && h.tt_s_code_blocks.is_empty());
        assert_eq!(h.tt_s_raw_bounds, Some((100, 203)));
        assert_eq!(h.format, SEGMENT_FORMAT_RAW_TT);
        assert_eq!(preamble_format(&dir.join("raw.tgs")), SEGMENT_FORMAT_RAW_TT);
        assert_ne!(SEGMENT_FORMAT_RAW_TT, crate::FORMAT_VERSION);
        for col in ["vt_s", "vt_e", TT_S_RAW_COLUMN] {
            let d = h.column(col).unwrap_or_else(|| panic!("no column {col}"));
            assert_eq!(d.codec, CODEC_RAW, "{col}");
        }
        // other columns keep their codec choice
        assert_ne!(h.column("src_id").unwrap().codec, CODEC_RAW);
        assert!(raw.tt_s_view().unwrap().is_raw());
        assert_eq!(raw.tt_s_bounds(), auto.tt_s_bounds());
        // falsifier: the raw file no larger than the adaptive one
        assert!(raw_bytes > auto_bytes, "raw {raw_bytes} B vs adaptive {auto_bytes} B");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every shape reads back every row, and every belief primitive gives the
    /// same ranges on all three.
    #[test]
    fn all_three_shapes_answer_every_belief_primitive_identically() {
        use crate::visibility::{believed_ranges_per_row, close_view, CloseIndex, RowBelief};
        let dir = tmp("s3-prims");
        std::fs::create_dir_all(&dir).unwrap();
        let n = 9_000u32;
        let segs: Vec<_> = [TimeCodec::Auto, TimeCodec::Runs, TimeCodec::Raw]
            .iter()
            .map(|&c| write(&dir, &format!("{c:?}"), c, n).0)
            .collect();
        let want = rows(n);
        let closes = CloseIndex::default();
        for seg in &segs {
            let tt = seg.tt_s_view().unwrap();
            for (i, r) in want.iter().enumerate() {
                assert_eq!(tt.at(i as u32).unwrap(), r.tt_s);
            }
            assert_eq!(seg.i64_column("vt_s").unwrap().len(), n as usize);
        }
        let views: Vec<_> = segs.iter().map(|s| s.tt_s_view().unwrap()).collect();
        assert_eq!(views.iter().map(|v| v.run_count()).collect::<Vec<_>>(), vec![views[0].run_count(); 3]);
        for as_of in [0, 99, 100, 150, 200, 201, 203, 500, OPEN_END - 1] {
            let per_shape: Vec<_> = segs
                .iter()
                .map(|seg| {
                    let tt = seg.tt_s_view().unwrap();
                    let b = seg.tt_s_bounds();
                    let view = close_view(seg.sidecar(), &closes, 0);
                    (
                        crate::scan::believed_ranges(&tt, b, as_of, n as usize),
                        crate::scan::believed_ranges_in(&tt, b, as_of, n as usize, 1_234, 5_678),
                        crate::scan::believed_ranges_closed(&tt, b, as_of, n as usize, &view),
                        crate::beliefdiff::added_ranges(&tt, b, 100, as_of, n as usize),
                        crate::beliefdiff::added_ranges_in(&tt, b, 100, as_of, n as usize, 77, 4_100),
                        believed_ranges_per_row(&tt, seg.sidecar(), &closes, 0, as_of, &[(0, n as usize)], n as usize, RowBelief::Current).unwrap(),
                    )
                })
                .collect();
            assert_eq!(per_shape[0], per_shape[1], "auto vs runs at {as_of}");
            assert_eq!(per_shape[0], per_shape[2], "auto vs raw at {as_of}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn node_segments_take_the_raw_codec_too() {
        let dir = tmp("s3-nodes");
        std::fs::create_dir_all(&dir).unwrap();
        let rows: Vec<NodeRow> = (0..500u32)
            .map(|i| NodeRow {
                vid: Id96 { hi: i as u64 + 1, lo: 1 },
                uid_id: i,
                label: "Node".into(),
                vt_s: i as i64,
                vt_e: i as i64 + 1,
                tt_s: 100 + (i % 3) as i64,
                props: "{}".into(),
                source: "s".into(),
                provenance_ref: None,
            })
            .collect();
        let path = dir.join("n.tgs");
        let spec = SegmentSpec { time_codec: TimeCodec::Raw, ..SegmentSpec::default() };
        write_node_segment(&path, &rows, &spec).unwrap();
        let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        assert_eq!(seg.header().format, SEGMENT_FORMAT_RAW_TT);
        let tt = seg.tt_s_view().unwrap();
        assert!(tt.is_raw());
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(tt.at(i as u32).unwrap(), r.tt_s);
        }
        assert_eq!(seg.header().column("vt_s").unwrap().codec, CODEC_RAW);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The same logical store written three ways (every segment adaptive,
    /// every segment raw, and mixed: ingest raw, corrections and compaction
    /// adaptive) answers every probe identically, in all three
    /// configurations.
    #[test]
    fn a_store_written_raw_or_mixed_answers_identically() {
        use fixture::*;
        for layout in LAYOUTS {
            let auto = build("s3-auto", layout);
            let want = answers(&auto);
            for codec in [TimeCodec::Raw, TimeCodec::Runs] {
                let f = crate::segment::with_time_codec(codec, || build(&format!("s3-{codec:?}"), layout));
                let mut raw_segments = 0;
                for (file, _) in f.s.edge_files() {
                    let seg = f.s.open_segment(&file).unwrap();
                    raw_segments += seg.tt_s_view().unwrap().is_raw() as usize;
                    assert!(!seg.tt_s_view().unwrap().is_dictionary() || codec != TimeCodec::Runs);
                }
                if codec == TimeCodec::Raw {
                    assert_eq!(raw_segments, f.s.edge_files().len(), "{layout:?}: every segment raw");
                }
                let got = answers(&f);
                for ((name, w), (_, g)) in want.iter().zip(&got) {
                    assert_eq!(w, g, "{layout:?} {codec:?}: `{name}` changed");
                }
            }
        }
        // mixed: the ingest batches raw, the corrections adaptive, in one
        // size-tiered store
        let mixed = build_with("s3-mixed", Layout::Tiered, Some(TimeCodec::Raw));
        let auto = build("s3-mixed-ref", Layout::Tiered);
        let kinds: std::collections::BTreeSet<bool> = mixed
            .s
            .edge_files()
            .iter()
            .map(|(file, _)| mixed.s.open_segment(file).unwrap().tt_s_view().unwrap().is_raw())
            .collect();
        assert_eq!(kinds.len(), 2, "the store must hold raw and adaptive segments");
        assert_eq!(answers(&mixed), answers(&auto));
    }
}

// ---------------------------------------------------------------------- //
// the believed-node count (GNOMON_EXISTENCE_PROBE=walk) and the identity     //
// filter on reads (GNOMON_VIDFILTER=ignore)                                  //
// ---------------------------------------------------------------------- //

mod existence_and_identity {
    use super::*;
    use crate::derive::version_vid;
    use crate::manifest::EventLogRef;
    use crate::row::{NodeRow, RowKind};
    use crate::OPEN_END;

    /// Thirty node versions, every third closed by a later commit.
    fn nodes(name: &str) -> (std::path::PathBuf, Vec<String>) {
        let root = tmp(name);
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let mut vids = Vec::new();
        for e in 0..30u32 {
            let uid = format!("e{e}");
            let id = s.ensure_entity(&uid, "Node").unwrap();
            let vid = version_vid(&uid, 100, e as i64);
            s.stage_node(NodeRow {
                vid,
                uid_id: id,
                label: "Node".into(),
                vt_s: e as i64,
                vt_e: e as i64 + 1,
                tt_s: 100,
                props: "{}".into(),
                source: "s".into(),
                provenance_ref: None,
            })
            .unwrap();
            vids.push(vid);
        }
        s.commit(EventLogRef::default()).unwrap();
        s.begin(200).unwrap();
        for (e, vid) in vids.iter().enumerate() {
            if e % 3 == 0 {
                s.close_version(RowKind::Node, *vid, 200).unwrap();
            }
        }
        s.commit(EventLogRef::default()).unwrap();
        drop(s);
        let uids = (0..40).map(|e| format!("e{e}")).chain(["nobody".to_string()]).collect();
        (root, uids)
    }

    #[test]
    fn the_existence_walk_answers_as_the_count_does_and_never_builds_it() {
        let (root, uids) = nodes("d7-exist");
        let s = NativeStore::open(&root).unwrap();
        assert!(s.node_belief_stats().is_none());
        let counted = s.nodes_with_believed_versions(&uids, OPEN_END).unwrap();
        assert!(s.node_belief_stats().is_some(), "every design on: the count answered");
        assert_eq!(counted.len(), 20, "{counted:?}");

        let mut w = NativeStore::open(&root).unwrap();
        w.set_designs(Designs { existence_count: false, ..Designs::ALL });
        let walked = w.nodes_with_believed_versions(&uids, OPEN_END).unwrap();
        // falsifier: the count built under GNOMON_EXISTENCE_PROBE=walk
        assert!(w.node_belief_stats().is_none(), "the walk built the believed-node count");
        assert_eq!(walked, counted);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Identity lookups give the same versions with the filter consulted and
    /// ignored; ignored, no probe is made.
    #[test]
    fn ignoring_the_identity_filter_answers_identically_and_probes_nothing() {
        use fixture::*;
        let f = build("d7-filter", Layout::Tiered);
        let eids: Vec<String> = f
            .rows
            .iter()
            .step_by(37)
            .map(|r| {
                let (s, d) = ((r.vt_s as u32) % ENTITIES, (r.vt_s as u32 * 7 + 1) % ENTITIES);
                crate::derive::edge_eid(&format!("e{s}"), &format!("e{d}"), &r.rel_type, &r.disc).to_hex()
            })
            .collect();
        let probe = |s: &NativeStore| {
            eids.iter()
                .flat_map(|e| [99, 999, 1_001, now()].map(|t| format!("{:?}", s.believed_edge_versions(e, t).unwrap())))
                .collect::<Vec<_>>()
        };
        let on_handle = NativeStore::open_read_only(&f.root).unwrap();
        let on = probe(&on_handle);
        let (probes_on, ..) = on_handle.filter_probe_stats(RowKind::Edge);
        assert!(probes_on > 0, "every design on: the filter was never consulted");
        assert!(on.iter().any(|v| v != "[]"), "the lookups must find versions");

        let mut off_handle = NativeStore::open_read_only(&f.root).unwrap();
        off_handle.set_designs(Designs { identity_filter: false, ..Designs::ALL });
        let off = probe(&off_handle);
        let (probes_off, ..) = off_handle.filter_probe_stats(RowKind::Edge);
        // falsifier: any probe with the switch set
        assert_eq!(probes_off, 0, "GNOMON_VIDFILTER=ignore still probed {probes_off} times");
        assert_eq!(on, off);
    }
}
