//! Belief visibility must cost what the query's row range holds, not
//! what the segment holds.
//!
//! The engine's cost claim for visibility says what is *read*: the
//! transaction-time runs (or dictionary codes) and the closes that a query
//! needs. These tests pin that claim with item counts
//! ([`crate::vistouch`]) taken from the production path, not with timings.
//! Every count assertion carries the value the whole-segment engine
//! produces in its message, so a failure says which side of
//! the line the build is on.
//!
//! * **T1** (`a_windowed_scan_reads_the_same_items_at_every_segment_size`):
//!   one fixed 16-row valid-time window, the same rows and the same closes
//!   around it, in segments of 65,536, 262,144 and 1,048,576 rows. The
//!   counts must be identical across the three sizes and at most 34
//!   (= 2 × 16 + 2). Today they are proportional to the segment.
//! * **T2** (`rows_outside_the_window_do_not_change_what_a_windowed_query_reads`):
//!   two stores whose only difference is extra rows (and their closes)
//!   **after** the window, full-store compacted so the window's rows sit in
//!   one segment four times larger in the second store. `diff_belief` and
//!   `version_keys` over the window must read exactly the same number of
//!   items in both stores, and give the same answer.
//! * **Quirk pins**: the edge cases of today's whole-segment primitives
//!   that any restricted form must reproduce byte for byte.
//! * **L1**: the belief diff's range-list difference must be linear.

use std::sync::Arc;

use crate::derive::Id96;
use crate::row::{EdgeRow, Lane};
use crate::scan::{ScanRequest, ScanSet};
use crate::segment::{write_edge_segment, MemorySource, Segment, SegmentSpec};
use crate::visibility::{CloseIndex, CloseRecord};
use crate::OPEN_END;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-winfirst-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

// ---------------------------------------------------------------------- //
// T1: the scan planner, one segment, three sizes                           //
// ---------------------------------------------------------------------- //

/// `tt_s` alternates within every five rows, so the run list holds 0.4
/// runs per row (dictionary-coded by the writer) and **every** 4,096-row
/// code block straddles `T1_MID`: the shape full-store compaction leaves
/// behind when corrections are spread evenly through `(vt_s, vid)` order.
fn tt_of(i: u32) -> i64 {
    if i % 5 == 2 {
        200
    } else {
        100
    }
}

/// Every 10th row closed before `T1_MID`, every 10th (offset 5) after it.
fn close_of(i: u32) -> Option<i64> {
    match i % 10 {
        0 => Some(120),
        5 => Some(300),
        _ => None,
    }
}

const T1_MID: i64 = 150;
const WIN_LO: i64 = 40_000;
const WIN_HI: i64 = 40_016;
const SIZES: [u32; 3] = [65_536, 262_144, 1_048_576];

fn t1_rows(n: u32) -> Vec<EdgeRow> {
    (0..n)
        .map(|i| EdgeRow {
            // cheap, unique, and ascending with `vt_s`, so the rows are
            // already in `(vt_s, vid)` order
            vid: Id96 {
                hi: i as u64 + 1,
                lo: 7,
            },
            src_id: i % 97,
            dst_id: (i + 1) % 97,
            rel_type: "R".into(),
            disc: String::new(),
            vt_s: i as i64,
            vt_e: i as i64 + 1,
            tt_s: tt_of(i),
            props: "{}".into(),
            source: "s".into(),
            provenance_ref: None,
        })
        .collect()
}

/// How a variant carries `tt_s` and its closes.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// Dictionary column; closes folded into the sidecar.
    DictSidecar,
    /// Dictionary column; closes in close runs (the generation's index).
    DictRuns,
    /// Run list; closes folded into the sidecar.
    RunsSidecar,
    /// Run list; closes in close runs.
    RunsRuns,
}

impl Shape {
    fn dictionary(self) -> bool {
        matches!(self, Shape::DictSidecar | Shape::DictRuns)
    }
    fn sidecar(self) -> bool {
        matches!(self, Shape::DictSidecar | Shape::RunsSidecar)
    }
}

struct T1Seg {
    seg: Segment<MemorySource>,
    closes: Arc<CloseIndex>,
}

fn t1_segment(dir: &std::path::Path, n: u32, shape: Shape) -> T1Seg {
    let rows = t1_rows(n);
    let closed: Vec<(u32, i64)> = (0..n).filter_map(|i| close_of(i).map(|t| (i, t))).collect();
    let spec = SegmentSpec {
        tt_s_dictionary: shape.dictionary(),
        closed_rows: if shape.sidecar() { closed.clone() } else { Vec::new() },
        ..SegmentSpec::default()
    };
    let path = dir.join(format!("{n}-{shape:?}.tgs"));
    write_edge_segment(&path, &rows, &spec).unwrap();
    let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        seg.tt_s_view().unwrap().is_dictionary(),
        shape.dictionary(),
        "{shape:?}: the fixture must take the shape it names"
    );
    let closes = if shape.sidecar() {
        CloseIndex::default()
    } else {
        // `ScanSet::from_pairs` numbers segments from 0
        CloseIndex::from_records(closed.iter().map(|&(row, tt_e)| CloseRecord {
            kind: crate::row::RowKind::Edge,
            segment: 0,
            row,
            tt_e,
        }))
    };
    T1Seg {
        seg,
        closes: Arc::new(closes),
    }
}

/// The window's believed rows, the slow obvious way.
fn t1_expected(as_of: i64) -> Vec<u32> {
    (WIN_LO as u32..WIN_HI as u32)
        .filter(|&i| tt_of(i) <= as_of && close_of(i).is_none_or(|e| as_of < e))
        .collect()
}

#[cfg(feature = "ingest-timers")]
#[test]
fn a_windowed_scan_reads_the_same_items_at_every_segment_size() {
    let dir = tmp("t1");
    for shape in [Shape::DictSidecar, Shape::DictRuns, Shape::RunsSidecar, Shape::RunsRuns] {
        // (as_of, items per size)
        let mut seen: Vec<(i64, Vec<crate::vistouch::VisTouch>)> =
            vec![(T1_MID, Vec::new()), (OPEN_END - 1, Vec::new())];
        for &n in &SIZES {
            let f = t1_segment(&dir, n, shape);
            let set = ScanSet::from_pairs(vec![(&f.seg, Lane::Event)]).with_closes(f.closes.clone());
            for (as_of, per_size) in seen.iter_mut() {
                let req = ScanRequest {
                    as_of_tt: *as_of,
                    ..ScanRequest::current().window(WIN_LO, WIN_HI)
                };
                crate::vistouch::take();
                // one thread, so every count lands on this thread
                let (sel, _) = set.select_units_at(&req, 1, 1 << 20).unwrap();
                let touch = crate::vistouch::take();
                let got: Vec<u32> = sel.iter().flat_map(|s| s.rows.iter().copied()).collect();
                assert_eq!(got, t1_expected(*as_of), "{shape:?} n={n} as_of={as_of}: wrong rows");
                per_size.push(touch);
            }
        }
        for (as_of, per_size) in &seen {
            for (k, t) in per_size.iter().enumerate() {
                let n = SIZES[k] as u64;
                // whole-segment engine: codes = n on a dictionary
                // segment at T1_MID (every block straddles), runs = 2n/5 on
                // a run list at T1_MID, closes = n/5 per pass over the
                // segment's close list (one pass from the sidecar, two when
                // the closes sit in close runs: the copy and the walk)
                assert!(
                    t.items() <= 34,
                    "{shape:?} as_of={as_of} n={n}: a 16-row window read {t:?} \
                     ({} items); the bound is 34. The whole-segment engine reads \
                     codes={} runs={} closes={} here",
                    t.items(),
                    if shape.dictionary() && *as_of == T1_MID { n } else { 0 },
                    if !shape.dictionary() && *as_of == T1_MID { 2 * n / 5 } else { 0 },
                    if shape.sidecar() { n / 5 } else { 2 * n / 5 },
                );
            }
            assert!(
                per_size.windows(2).all(|w| w[0] == w[1]),
                "{shape:?} as_of={as_of}: the counts follow the segment size: {per_size:?}"
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Found by a timed run: resolving a run-list segment's `tt_s` view
/// must not walk its runs. `TtSView::from_header` folded the header's whole
/// run list into `(min, max)` on every call, so every reader that resolved
/// the view per segment per call paid O(runs) before reading anything:
/// 0.55 ms per `diff_belief` on a fully compacted 10M store written with run
/// lists (795,789 runs), whatever the window.
#[cfg(feature = "ingest-timers")]
#[test]
fn resolving_the_tt_s_view_reads_no_runs() {
    let dir = tmp("tt-view");
    let f = t1_segment(&dir, SIZES[1], Shape::RunsSidecar);
    crate::vistouch::take();
    let tt = f.seg.tt_s_view().unwrap();
    let t = crate::vistouch::take();
    assert_eq!(tt.bounds(), f.seg.tt_s_bounds(), "the view must carry the folded bounds");
    // whole-segment engine before this fix: runs = 104,859 here (the 2n/5 runs plus one)
    assert_eq!(t.runs, 0, "resolving the view walked {} runs", t.runs);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The window's answer itself does not depend on the touch counters, so it
/// is checked with the counters compiled out too.
#[test]
fn a_windowed_scan_answers_the_same_at_every_segment_size() {
    let dir = tmp("t1-answers");
    for shape in [Shape::DictSidecar, Shape::DictRuns, Shape::RunsSidecar, Shape::RunsRuns] {
        let f = t1_segment(&dir, SIZES[0], shape);
        let set = ScanSet::from_pairs(vec![(&f.seg, Lane::Event)]).with_closes(f.closes.clone());
        for as_of in [99, 100, 119, 120, T1_MID, 200, 299, 300, OPEN_END - 1] {
            let req = ScanRequest {
                as_of_tt: as_of,
                ..ScanRequest::current().window(WIN_LO, WIN_HI)
            };
            for threads in [1, 4] {
                let (sel, _) = set.select_units_at(&req, threads, 5).unwrap();
                let got: Vec<u32> = sel.iter().flat_map(|s| s.rows.iter().copied()).collect();
                assert_eq!(got, t1_expected(as_of), "{shape:?} as_of={as_of} threads={threads}");
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------- //
// T2: a store, with and without rows outside the window                    //
// ---------------------------------------------------------------------- //

mod store_level {
    use super::*;
    use crate::derive::{edge_eid, version_vid};
    use crate::manifest::EventLogRef;
    use crate::read::{Belief, VersionFilter};
    use crate::row::RowKind;
    use crate::store::NativeStore;

    /// Ingest batches: batch `b` writes the rows `i ≡ b (mod BATCHES)` at
    /// `tt = 100 + b`, so after full-store compaction `tt_s` changes on
    /// every row and every code block straddles `T1`.
    const BATCHES: u32 = 20;
    /// Belief instant between ingest batches 9 and 10.
    const T1: i64 = 109;
    const W_A: i64 = 5_000;
    const W_B: i64 = 5_016;

    fn edge(i: u32, src: u32, dst: u32, tt_s: i64) -> EdgeRow {
        let (su, du) = (format!("e{}", src), format!("e{}", dst));
        let disc = format!("#{i}");
        let eid = edge_eid(&su, &du, "R", &disc);
        EdgeRow {
            vid: version_vid(&eid.to_hex(), tt_s, i as i64),
            src_id: src,
            dst_id: dst,
            rel_type: "R".into(),
            disc,
            vt_s: i as i64,
            vt_e: i as i64 + 1,
            tt_s,
            props: "{}".into(),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    /// `n` edges at `vt_s = 0..n`; every 10th row (and every 10th at
    /// offset 3) is closed by a correction at `tt >= 1000`, in commits of
    /// 1,000. Fully compacted either **after** the corrections (closes
    /// folded into the sidecar) or **before** them (closes left in close
    /// runs).
    fn build(name: &str, n: u32, settle_after: bool) -> (std::path::PathBuf, NativeStore) {
        let root = {
            let mut p = std::env::temp_dir();
            p.push(format!("gnomon-winfirst-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            p
        };
        let mut s = NativeStore::open(&root).unwrap();
        let (p, _) = {
            let (p, t) = s.layout();
            (*p, t)
        };
        s.set_layout(p, 1 << 32);
        let mut ids: Vec<u32> = Vec::new();
        let mut rows: Vec<EdgeRow> = Vec::with_capacity(n as usize);
        for b in 0..BATCHES {
            s.begin(100 + b as i64).unwrap();
            if ids.is_empty() {
                for e in 0..64u32 {
                    ids.push(s.ensure_entity(&format!("e{e}"), "Node").unwrap());
                }
            }
            for i in (b..n).step_by(BATCHES as usize) {
                let r = edge(i, ids[(i % 64) as usize], ids[((i + 1) % 64) as usize], 100 + b as i64);
                s.stage_edge(r.clone()).unwrap();
                rows.push(r);
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        rows.sort_by_key(|r| r.vt_s);
        if !settle_after {
            s.settle(None).unwrap();
        }
        let closed: Vec<&EdgeRow> = rows.iter().filter(|r| r.vt_s % 10 == 0 || r.vt_s % 10 == 3).collect();
        for (k, chunk) in closed.chunks(1_000).enumerate() {
            let tt = 1_000 + k as i64;
            s.begin(tt).unwrap();
            for r in chunk {
                s.close_version(RowKind::Edge, r.vid, tt).unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        if settle_after {
            s.settle(None).unwrap();
        }
        (root, s)
    }

    /// Rows of the edge segment that holds the window: the fixture's whole
    /// point is that this grows while the window's rows do not.
    #[cfg(feature = "ingest-timers")]
    fn window_segment_rows(s: &NativeStore) -> u32 {
        let mut hit = Vec::new();
        for (file, _) in s.edge_files() {
            let seg = s.open_segment(&file).unwrap();
            let h = seg.header();
            if h.vt_min < W_B && h.vt_e_max > W_A {
                hit.push(seg.rows());
            }
        }
        assert_eq!(hit.len(), 1, "the window must fall in exactly one edge segment: {hit:?}");
        hit[0]
    }

    type Probe = (Vec<(String, String, i64)>, crate::vistouch::VisTouch);

    fn diff_probe(s: &NativeStore, t2: i64) -> Probe {
        crate::vistouch::take();
        let out = s.diff_belief(T1, t2, W_A, W_B, usize::MAX).unwrap();
        let touch = crate::vistouch::take();
        let rows = out
            .rows
            .iter()
            .map(|r| (r.change.as_str().to_string(), r.vid.clone(), r.tt))
            .collect();
        (rows, touch)
    }

    fn history_probe(s: &NativeStore, belief: Belief, as_of: i64) -> Probe {
        let f = VersionFilter {
            as_of,
            t_a: W_A,
            t_b: W_B,
            belief,
            rel_types: None,
        };
        crate::vistouch::take();
        let keys = s.version_keys(RowKind::Edge, &f).unwrap();
        let touch = crate::vistouch::take();
        let rows = keys
            .iter()
            .map(|k| {
                (
                    format!("{belief:?}"),
                    Id96 {
                        hi: k.vid_hi,
                        lo: k.vid_lo,
                    }
                    .to_hex(),
                    k.tt_s,
                )
            })
            .collect();
        (rows, touch)
    }

    fn probes(s: &NativeStore) -> Vec<(String, Probe)> {
        let now = crate::clamp_tt(OPEN_END);
        let mut v = vec![
            ("diff(T1, now)".to_string(), diff_probe(s, now)),
            ("diff(T1, 1500)".to_string(), diff_probe(s, 1_500)),
        ];
        for (belief, as_of) in [
            (Belief::Current, T1),
            (Belief::Current, now),
            (Belief::Superseded, now),
            (Belief::All, T1),
        ] {
            v.push((format!("version_keys({belief:?}, {as_of})"), history_probe(s, belief, as_of)));
        }
        v
    }

    #[cfg(feature = "ingest-timers")]
    #[test]
    fn rows_outside_the_window_do_not_change_what_a_windowed_query_reads() {
        for settle_after in [true, false] {
            let tag = if settle_after { "folded" } else { "runs" };
            let (ra, a) = build(&format!("t2a-{tag}"), 20_000, settle_after);
            let (rb, b) = build(&format!("t2b-{tag}"), 80_000, settle_after);
            let (na, nb) = (window_segment_rows(&a), window_segment_rows(&b));
            assert!(nb >= 4 * na - 4, "{tag}: the larger store must put the window in a larger segment ({na} vs {nb})");
            let (pa, pb) = (probes(&a), probes(&b));
            for ((name, (ans_a, t_a)), (_, (ans_b, t_b))) in pa.iter().zip(&pb) {
                assert!(!ans_a.is_empty() || name.contains("Superseded"), "{tag} {name}: the probe must have an answer");
                assert_eq!(ans_a, ans_b, "{tag} {name}: the window's answer must not depend on rows outside it");
                // whole-segment engine: the counts scale with the
                // window segment (20,000 vs 80,000 rows), e.g. the diff reads
                // every code of the segment for `believed_ranges(T1)`
                assert_eq!(
                    t_a, t_b,
                    "{tag} {name}: rows outside the window changed what the query read \
                     (window segment {na} rows: {t_a:?}; {nb} rows: {t_b:?})"
                );
                assert!(
                    t_a.items() <= 4 * (W_B - W_A) as u64 + 64,
                    "{tag} {name}: {t_a:?} is not bounded by the window's 16 rows"
                );
            }
            drop((a, b));
            let _ = std::fs::remove_dir_all(ra);
            let _ = std::fs::remove_dir_all(rb);
        }
    }

    /// Windows at the segment's edges, inside it, past it, empty and whole.
    fn windows(n: u32) -> Vec<(i64, i64)> {
        let n = n as i64;
        vec![
            (W_A, W_B),
            (0, 1),
            (0, 37),
            (n - 25, n),
            (n - 3, n + 50),
            (n + 10, n + 20),
            (17, 17),
            (1_234, 1_301),
            (-5, n + 5),
            (i64::MIN / 4, i64::MAX / 4),
        ]
    }

    /// Answer identity: a windowed belief diff is the whole-extent diff
    /// filtered to the window, row for row and in the same order, with the
    /// totals counted over the filtered population.
    #[test]
    fn a_windowed_belief_diff_is_the_whole_diff_filtered_to_the_window() {
        let now = crate::clamp_tt(OPEN_END);
        for settle_after in [true, false] {
            let (root, s) = build(&format!("s2-id-{settle_after}"), 3_000, settle_after);
            for (t1, t2) in [(T1, now), (T1, 1_500), (0, 105), (1_000, 1_002), (99, 119)] {
                let whole = s
                    .diff_belief(t1, t2, i64::MIN / 4, i64::MAX / 4, usize::MAX)
                    .unwrap();
                for (a, b) in windows(3_000) {
                    let got = s.diff_belief(t1, t2, a, b, usize::MAX).unwrap();
                    let want: Vec<_> = whole
                        .rows
                        .iter()
                        .filter(|r| r.vt_s < b && r.vt_e > a)
                        .cloned()
                        .collect();
                    let ctx = format!("settle_after={settle_after} ({t1}, {t2}] [{a}, {b})");
                    assert_eq!(got.rows, want, "{ctx}");
                    let added = want
                        .iter()
                        .filter(|r| r.change == crate::beliefdiff::BeliefChange::Added)
                        .count();
                    assert_eq!((got.added_total, got.retracted_total), (added, want.len() - added), "{ctx}");
                }
            }
            drop(s);
            let _ = std::fs::remove_dir_all(root);
        }
    }

    /// Answer identity: windowed `version_keys` is the whole-extent key
    /// list filtered to the window, in the same order, in all three belief
    /// modes, including the unclamped `as_of == OPEN_END` special case.
    #[test]
    fn windowed_version_keys_are_the_whole_keys_filtered_to_the_window() {
        let now = crate::clamp_tt(OPEN_END);
        for settle_after in [true, false] {
            let (root, s) = build(&format!("s3-id-{settle_after}"), 3_000, settle_after);
            for belief in [Belief::Current, Belief::Superseded, Belief::All] {
                for as_of in [99, T1, 1_000, 1_001, now, OPEN_END] {
                    let f = |t_a, t_b| VersionFilter {
                        as_of,
                        t_a,
                        t_b,
                        belief,
                        rel_types: None,
                    };
                    let whole = s.version_keys(RowKind::Edge, &f(i64::MIN / 4, i64::MAX / 4)).unwrap();
                    for (a, b) in windows(3_000) {
                        let got = s.version_keys(RowKind::Edge, &f(a, b)).unwrap();
                        // the fixture's edges are instantaneous at vt_s = row
                        let want: Vec<(u64, u32, i64)> = whole
                            .iter()
                            .filter(|k| {
                                let vt = k.row as i64;
                                vt < b && vt + 1 > a
                            })
                            .map(|k| (k.vid_hi, k.vid_lo, k.tt_s))
                            .collect();
                        let got: Vec<(u64, u32, i64)> =
                            got.iter().map(|k| (k.vid_hi, k.vid_lo, k.tt_s)).collect();
                        assert_eq!(got, want, "settle_after={settle_after} {belief:?} as_of={as_of} [{a}, {b})");
                    }
                }
            }
            drop(s);
            let _ = std::fs::remove_dir_all(root);
        }
    }

    /// The answers alone, with or without the counters.
    #[test]
    fn rows_outside_the_window_do_not_change_a_windowed_answer() {
        // both stores hold the window's rows ([W_A, W_B) = [5000, 5016))
        let (ra, a) = build("t2c-a", 6_000, true);
        let (rb, b) = build("t2c-b", 18_000, true);
        for ((name, (ans_a, _)), (_, (ans_b, _))) in probes(&a).iter().zip(&probes(&b)) {
            assert_eq!(ans_a, ans_b, "{name}");
        }
        drop((a, b));
        let _ = std::fs::remove_dir_all(ra);
        let _ = std::fs::remove_dir_all(rb);
    }
}

// ---------------------------------------------------------------------- //
// Quirk pins: what today's whole-segment primitives do at the edges         //
// ---------------------------------------------------------------------- //

mod quirks {
    use crate::scan::{believed_ranges, believed_ranges_closed};
    use crate::segment::header_with_runs;
    use crate::ttime::{TtCodes, TtSView};
    use crate::visibility::{close_view, CloseIndex, CloseRecord, CloseView, Sidecar};
    use crate::OPEN_END;

    /// Run list: rows before the first run are never believed, in the
    /// all-believed short-cut (`[first, rows)`) and in the mixed walk alike.
    #[test]
    fn run_list_rows_before_the_first_run_are_not_believed() {
        let h = header_with_runs(vec![(5, 10), (7, 20)], 12);
        let tt = TtSView::from_header(&h);
        let b = h.tt_s_bounds();
        assert_eq!(believed_ranges(&tt, b, 25, 12), vec![(5, 12)], "all-believed short-cut");
        assert_eq!(believed_ranges(&tt, b, 15, 12), vec![(5, 7)], "mixed walk");
        assert!(believed_ranges(&tt, b, 9, 12).is_empty(), "none believed");
    }

    /// Dictionary: a code column shorter than the segment's row count is
    /// read as far as it goes in the mixed walk, while the all-believed
    /// short-cut answers from the bounds over every row. Only a corrupt
    /// segment can show it; it is preserved, not fixed.
    #[test]
    fn dictionary_short_code_column_is_read_as_far_as_it_goes() {
        let mut h = header_with_runs(Vec::new(), 8);
        h.tt_s_dict = vec![10, 20];
        h.tt_s_code_blocks = vec![(0, 1)];
        h.tt_s_block_rows = 4096;
        let codes: [u16; 6] = [0, 1, 0, 0, 1, 0];
        let tt = TtSView::dictionary(&h, TtCodes::U16(&codes), (10, 20));
        assert_eq!(believed_ranges(&tt, (10, 20), 15, 8), vec![(0, 1), (2, 4), (5, 6)]);
        assert_eq!(believed_ranges(&tt, (10, 20), 25, 8), vec![(0, 8)]);
        assert!(believed_ranges(&tt, (10, 20), 5, 8).is_empty());
    }

    /// A close naming a row past the segment's end is stepped over.
    #[test]
    fn a_stale_close_past_the_segment_end_changes_nothing() {
        let h = header_with_runs(vec![(0, 10)], 10);
        let tt = TtSView::from_header(&h);
        let folded = [(3u32, 20i64), (12, 20)];
        let view = CloseView::Sidecar(&folded);
        assert_eq!(
            believed_ranges_closed(&tt, h.tt_s_bounds(), 30, 10, &view),
            vec![(0, 3), (4, 10)]
        );
    }

    /// A close-run entry whose `tt_e` is `OPEN_END` says nothing: the
    /// sidecar answers for that row, and with no sidecar entry the run
    /// entry is dropped.
    #[test]
    fn an_open_end_run_entry_defers_to_the_sidecar() {
        let rec = |row, tt_e| CloseRecord {
            kind: crate::row::RowKind::Edge,
            segment: 1,
            row,
            tt_e,
        };
        let runs = CloseIndex::from_records([rec(2, OPEN_END), rec(4, OPEN_END), rec(6, 60)]);
        let folded = [(2u32, 25i64), (6, 65)];
        let v = close_view(Sidecar::new(&folded), &runs, 1);
        assert_eq!(v.as_slice(), &[(2, 25), (6, 60)]);
        let v = close_view(Sidecar::new(&[]), &runs, 1);
        assert_eq!(v.as_slice(), &[(6, 60)]);
    }
}

// ---------------------------------------------------------------------- //
// L1: the range-list difference is linear                                  //
// ---------------------------------------------------------------------- //

#[cfg(feature = "ingest-timers")]
#[test]
fn the_belief_diff_range_difference_is_linear() {
    // two interleaved lists of 5,000 single-row ranges, disjoint
    let a: Vec<(usize, usize)> = (0..5_000).map(|k| (2 * k, 2 * k + 1)).collect();
    let b: Vec<(usize, usize)> = (0..5_000).map(|k| (2 * k + 1, 2 * k + 2)).collect();
    crate::vistouch::take();
    let out = crate::beliefdiff::range_subtract(&a, &b);
    let t = crate::vistouch::take();
    assert_eq!(out, a, "disjoint lists: nothing is subtracted");
    // whole-segment engine: 25,000,000 steps (|a| x |b|)
    assert!(
        t.steps <= 2 * (a.len() + b.len()) as u64,
        "range_subtract took {} steps over |a| = |b| = 5,000; linear is <= 20,000",
        t.steps
    );
}

/// The difference itself, against a row-set oracle, on lists whose ranges
/// overlap in every way two ranges can.
#[test]
fn the_belief_diff_range_difference_is_the_set_difference() {
    let mut seed = 0x2026_1003_u64;
    let mut rng = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    fn list(n: usize, rng: &mut impl FnMut() -> u64) -> Vec<(usize, usize)> {
        let mut v = Vec::new();
        let mut at = (rng() % 3) as usize;
        for _ in 0..n {
            let len = (rng() % 5) as usize + 1;
            v.push((at, at + len));
            at += len + (rng() % 4) as usize + 1;
        }
        v
    }
    for _ in 0..300 {
        let na = (rng() % 12) as usize;
        let nb = (rng() % 12) as usize;
        let a = list(na, &mut rng);
        let b = list(nb, &mut rng);
        let rows = |l: &[(usize, usize)]| -> std::collections::BTreeSet<usize> {
            l.iter().flat_map(|&(s, e)| s..e).collect()
        };
        let want: Vec<usize> = rows(&a).difference(&rows(&b)).copied().collect();
        let got = crate::beliefdiff::range_subtract(&a, &b);
        let flat: Vec<usize> = got.iter().flat_map(|&(s, e)| s..e).collect();
        assert_eq!(flat, want, "a={a:?} b={b:?}");
        assert!(got.windows(2).all(|w| w[0].1 < w[1].0), "canonical: {got:?}");
    }
}

// ---------------------------------------------------------------------- //
// T3: the restricted primitives are the whole-segment ones, clipped         //
// ---------------------------------------------------------------------- //

mod exactness {
    use crate::scan::{
        believed_ranges, believed_ranges_any, believed_ranges_any_in, believed_ranges_closed,
        believed_ranges_closed_in, believed_ranges_in, believed_ranges_superseded,
        believed_ranges_superseded_in,
    };
    use crate::segment::header_with_runs;
    use crate::ttime::{TtCodes, TtSView};
    use crate::visibility::{close_view, close_view_in, CloseIndex, CloseRecord, Sidecar};
    use crate::OPEN_END;

    /// The whole-segment answer intersected with `[lo, hi)`, re-coalesced:
    /// what every call site computed in the whole-segment engine.
    pub(super) fn clip(r: &[(usize, usize)], lo: usize, hi: usize) -> Vec<(usize, usize)> {
        let mut out: Vec<(usize, usize)> = Vec::new();
        for &(a, b) in r {
            let (a, b) = (a.max(lo), b.min(hi));
            if a >= b {
                continue;
            }
            match out.last_mut() {
                Some(l) if l.1 == a => l.1 = b,
                _ => out.push((a, b)),
            }
        }
        out
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            if n == 0 {
                0
            } else {
                self.next() % n
            }
        }
    }

    fn rec(row: u32, tt_e: i64) -> CloseRecord {
        CloseRecord {
            kind: crate::row::RowKind::Edge,
            segment: 1,
            row,
            tt_e,
        }
    }

    /// Random closes for a `rows`-row segment: a folded sidecar, a committed
    /// close-run index, optionally an open batch's overlay over it, with
    /// `OPEN_END` run entries and stale rows past the end mixed in.
    fn closes(rng: &mut Rng, rows: u32) -> (Vec<(u32, i64)>, CloseIndex) {
        let span = rows + 4;
        let mut folded: Vec<(u32, i64)> = Vec::new();
        let mut recs = Vec::new();
        for r in 0..span {
            if rng.below(4) == 0 {
                folded.push((r, 10 + rng.below(8) as i64 * 10));
            }
            match rng.below(6) {
                0 => recs.push(rec(r, 10 + rng.below(8) as i64 * 10)),
                1 => recs.push(rec(r, OPEN_END)),
                _ => {}
            }
        }
        let committed = CloseIndex::from_records(recs);
        let idx = if rng.below(2) == 0 {
            let mut top = CloseIndex::layered_over(std::sync::Arc::new(committed));
            for r in 0..span {
                if rng.below(7) == 0 {
                    top.close_row(1, r, 10 + rng.below(8) as i64 * 10);
                }
            }
            top
        } else {
            committed
        };
        (folded, idx)
    }

    fn domains(rng: &mut Rng, rows: usize) -> Vec<(usize, usize)> {
        let mut v = vec![(0, rows), (0, rows + 5), (0, 0), (rows, rows + 3)];
        for _ in 0..12 {
            let a = rng.below(rows as u64 + 3) as usize;
            let b = a + rng.below(rows as u64 / 2 + 3) as usize;
            v.push((a, b));
        }
        v
    }

    fn as_ofs(rng: &mut Rng) -> Vec<i64> {
        let mut v = vec![0, 5, 10, 45, 90, 200, OPEN_END - 1];
        for _ in 0..4 {
            v.push(rng.below(100) as i64);
        }
        v
    }

    fn check(name: &str, tt: &TtSView<'_>, rows: usize, rng: &mut Rng) {
        let bounds = tt.bounds();
        let (folded, idx) = closes(rng, rows as u32);
        let sidecar = Sidecar::new(&folded);
        let whole_view = close_view(sidecar, &idx, 1);
        for (lo, hi) in domains(rng, rows) {
            // the close view and the per-segment projection
            let want_view: Vec<(u32, i64)> = whole_view
                .as_slice()
                .iter()
                .copied()
                .filter(|&(r, _)| (r as usize) >= lo && (r as usize) < hi)
                .collect();
            let view_in = close_view_in(sidecar, &idx, 1, lo, hi);
            assert_eq!(view_in.as_slice(), &want_view[..], "{name} [{lo},{hi}): close view");
            let want_seg: Vec<(u32, i64)> = idx
                .segment_closes(1)
                .iter()
                .copied()
                .filter(|&(r, _)| (r as usize) >= lo && (r as usize) < hi)
                .collect();
            assert_eq!(idx.segment_closes_in(1, lo, hi).as_ref(), &want_seg[..], "{name}: segment closes");
            for as_of in as_ofs(rng) {
                let ctx = format!("{name} [{lo},{hi}) as_of={as_of}");
                assert_eq!(
                    believed_ranges_in(tt, bounds, as_of, rows, lo, hi),
                    clip(&believed_ranges(tt, bounds, as_of, rows), lo, hi),
                    "{ctx}: written"
                );
                assert_eq!(
                    believed_ranges_any_in(tt, bounds, as_of, rows, lo, hi),
                    clip(&believed_ranges_any(tt, bounds, as_of, rows), lo, hi),
                    "{ctx}: any"
                );
                let cur = clip(&believed_ranges_closed(tt, bounds, as_of, rows, &whole_view), lo, hi);
                let sup = clip(&believed_ranges_superseded(tt, bounds, as_of, rows, &whole_view), lo, hi);
                for view in [&view_in, &whole_view] {
                    assert_eq!(
                        believed_ranges_closed_in(tt, bounds, as_of, rows, lo, hi, view),
                        cur,
                        "{ctx}: current"
                    );
                    assert_eq!(
                        believed_ranges_superseded_in(tt, bounds, as_of, rows, lo, hi, view),
                        sup,
                        "{ctx}: superseded"
                    );
                }
            }
        }
    }

    #[test]
    fn restricted_primitives_equal_the_clipped_whole_segment_answers() {
        let mut rng = Rng(0x2026_1003_0026);
        for case in 0..250 {
            let rows = 1 + rng.below(90) as usize;
            // run list: first run may start late, starts may run past the
            // end (a stale header), times in any order
            let mut runs: Vec<(u32, i64)> = Vec::new();
            let mut at = rng.below(3) as u32;
            while (at as usize) < rows + 2 {
                runs.push((at, 10 + rng.below(9) as i64 * 10));
                at += 1 + rng.below(6) as u32;
            }
            let h = header_with_runs(runs, rows as u32);
            check(&format!("case {case} runs"), &TtSView::from_header(&h), rows, &mut rng);

            // dictionary: small blocks so ranges cross several, a code
            // column sometimes shorter than the segment, and a block-bounds
            // vector sometimes cut short (a header that lost it)
            let dict: Vec<i64> = (0..1 + rng.below(6) as i64).map(|k| 10 + 15 * k).collect();
            let n_codes = if rng.below(5) == 0 { rows.saturating_sub(1 + rng.below(4) as usize) } else { rows };
            let codes: Vec<u16> = (0..n_codes).map(|_| rng.below(dict.len() as u64) as u16).collect();
            let block_rows = 1 + rng.below(9) as usize;
            let mut blocks: Vec<(u32, u32)> = codes
                .chunks(block_rows)
                .map(|b| b.iter().fold((u32::MAX, 0u32), |(lo, hi), &c| (lo.min(c as u32), hi.max(c as u32))))
                .collect();
            if rng.below(6) == 0 {
                blocks.truncate(blocks.len() / 2);
            }
            let mut hd = header_with_runs(Vec::new(), rows as u32);
            hd.tt_s_dict = dict.clone();
            hd.tt_s_code_blocks = blocks;
            // 0 = an old header without the granularity: one block over all rows
            hd.tt_s_block_rows = if rng.below(8) == 0 { 0 } else { block_rows as u32 };
            let bounds = (dict[0], *dict.last().unwrap());
            let tt = TtSView::dictionary(&hd, TtCodes::U16(&codes), bounds);
            check(&format!("case {case} dict"), &tt, rows, &mut rng);
        }
    }
}
