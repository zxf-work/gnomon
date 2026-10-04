//! A parallel read's unit is a row range, and it must be exact.
//!
//! Both parallel read stages self-schedule, which fixed the order
//! units are claimed in. It left their number alone — one per opened segment
//! — so a store with few large segments could not fill the workers however
//! well they were scheduled, and a settled 10M store's scan ran serially
//! under the eight-unit gate. The unit is now `(segment, row range)`.
//!
//! Splitting a segment's rows has exactly three ways to be wrong, and none of
//! them is visible in a wall clock:
//!
//! 1. a row falls in **no** unit, at a boundary the cut mis-computes;
//! 2. a row falls in **two**, so the fold counts it twice;
//! 3. the answer starts depending on which worker claimed which unit.
//!
//! The first two are invisible in a total that happens to match, so every
//! assertion here is over the whole `(segment, row)` list rather than its
//! length, and the reference is the same scan cut into one unit per segment —
//! which is exactly what the per-segment engine did. The third is tested by
//! sweeping widths and repeating inside each width, because a claim counter
//! that is not reset per call drops every unit of the *second* call.
//!
//! The fixture is deliberately awkward for a cut: two generations far larger
//! than `MIN_ROWS_PER_UNIT` so they really split, four far smaller so they
//! must not, and corrections scattered through the large ones so the verdict
//! ranges arrive fragmented and a unit boundary has to land inside a run.

use gnomon_core::aggregate::{Agg, AggValues, AggregateOut, AggregateRequest, Dim};
use gnomon_core::derive::{edge_eid, version_vid};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::phase;
use gnomon_core::row::{EdgeRow, Lane, RowKind};
use gnomon_core::scan::{ScanRequest, ScanSet, ScanTarget, Selection};
use gnomon_core::store::NativeStore;
use gnomon_core::OPEN_END;

/// Rows per generation, one segment each. The first and third are several
/// times `defaults::MIN_ROWS_PER_UNIT` (65,536) so they split under the
/// derived unit size as well as under a forced one; the rest are far under it
/// and must stay one unit each, which is the floor's whole purpose.
const GEN_ROWS: [i64; 6] = [90_000, 40, 70_000, 30, 20_000, 12];
const SPAN: i64 = 1_000_000;
const ENTITIES: i64 = 7;

/// The phase counters, `GNOMON_SCAN_THREADS` and `GNOMON_ROWS_PER_UNIT` are all
/// process-global and cargo runs a file's tests on several threads. Every
/// test here holds this for its whole body. Poisoning is ignored on purpose:
/// a failed test must not turn the rest into spurious failures.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn tmp_root(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-w13-units-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// Build the fixture, then correct a scattering of its rows.
///
/// The corrections are the point: without them `believed_ranges_closed`
/// returns one range per segment and every unit boundary lands inside it, so
/// the case where a boundary must split a *run* — and the case where a unit
/// spans several runs — would never be exercised.
fn build(root: &std::path::Path) -> NativeStore {
    let mut store = NativeStore::open(root).unwrap();
    let mut counter = 0u32;
    let mut to_close: Vec<gnomon_core::derive::Id96> = Vec::new();
    for (g, n) in GEN_ROWS.iter().enumerate() {
        let tt = 100 + g as i64;
        store.begin(tt).unwrap();
        for i in 0..*n {
            let (uid_s, uid_d) = (
                format!("n{}", i % ENTITIES),
                format!("n{}", (i + 3) % ENTITIES),
            );
            let src_id = store.ensure_entity(&uid_s, "Node").unwrap();
            let dst_id = store.ensure_entity(&uid_d, "Node").unwrap();
            let rel = ["SENT", "RATED", "PAID"][(i % 3) as usize];
            let disc = format!("#{counter}");
            counter += 1;
            // generations tile valid time, so a whole-extent window opens
            // every segment and no manifest prune can mask a lost unit
            let vt_s = g as i64 * SPAN + i;
            let eid = edge_eid(&uid_s, &uid_d, rel, &disc);
            let vid = version_vid(&eid.to_hex(), tt, vt_s);
            // a close every ~1,700 rows of the big generations, which puts
            // several believed runs inside every 65,536-row unit and several
            // unit boundaries inside a run
            if *n > 1_000 && i % 1_699 == 7 {
                to_close.push(vid);
            }
            store
                .stage_edge(EdgeRow {
                    vid,
                    src_id,
                    dst_id,
                    rel_type: rel.to_string(),
                    disc,
                    vt_s,
                    vt_e: vt_s + 1 + (i % 11),
                    tt_s: tt,
                    props: "{}".to_string(),
                    source: "ingest".to_string(),
                    provenance_ref: None,
                })
                .unwrap();
        }
        store.commit(EventLogRef::default()).unwrap();
    }
    let tt_close = 100 + GEN_ROWS.len() as i64;
    store.begin(tt_close).unwrap();
    for vid in &to_close {
        store.close_version(RowKind::Edge, *vid, tt_close).unwrap();
    }
    store.commit(EventLogRef::default()).unwrap();
    store
}

/// Open the fixture's edge segments as a scan set, in manifest order.
fn segments(
    store: &NativeStore,
) -> Vec<std::sync::Arc<gnomon_core::segment::Segment<gnomon_core::segment::MmapSource>>> {
    store
        .segment_layout()
        .iter()
        .filter(|r| r.0.starts_with("edge"))
        .map(|r| store.open_segment(&r.1).expect("segment opens"))
        .collect()
}

fn scan_set<'a, S: gnomon_core::segment::SegmentSource>(
    segs: &'a [std::sync::Arc<gnomon_core::segment::Segment<S>>],
    store: &NativeStore,
) -> ScanSet<'a, S> {
    ScanSet::new(
        segs.iter()
            .enumerate()
            .map(|(i, s)| ScanTarget {
                segment: s.as_ref(),
                lane: Lane::Event,
                id: i as u64,
            })
            .collect(),
    )
    .with_closes(store.close_index().expect("close index"))
}

/// Every `(segment, row)` the scan selected, flattened in the order the
/// selections carry it. This is the whole answer of the select stage: two
/// scans that produce this same list produce the same rows in the same order
/// for every downstream consumer.
fn flat(sels: &[Selection]) -> Vec<(usize, u32)> {
    sels.iter()
        .flat_map(|s| s.rows.iter().map(move |&r| (s.segment, r)))
        .collect()
}

fn phase_of(name: &str) -> u64 {
    phase::snapshot_named()
        .into_iter()
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v)
        .unwrap_or_else(|| panic!("no phase named {name}"))
}

/// The requests a unit boundary has to survive. Each takes a different branch
/// of the row loop, and the last two are the ones where the per-segment code
/// clipped inside the loop rather than before it.
fn requests() -> Vec<(&'static str, ScanRequest)> {
    let end = GEN_ROWS.len() as i64 * SPAN;
    vec![
        (
            "whole-extent",
            ScanRequest {
                as_of_tt: OPEN_END,
                ..Default::default()
            },
        ),
        (
            "rel-filtered",
            ScanRequest {
                as_of_tt: OPEN_END,
                rel_types: Some(vec!["SENT".to_string(), "PAID".to_string()]),
                ..Default::default()
            },
        ),
        (
            "touching",
            ScanRequest::current().touching(vec![1, 4]),
        ),
        (
            // as_of before the corrections: every row is believed again, so
            // the verdict ranges have a different shape from the default one
            "as-of-before-the-closes",
            ScanRequest {
                as_of_tt: 100 + GEN_ROWS.len() as i64 - 1,
                ..Default::default()
            },
        ),
        (
            // a window whose cut lands inside the first big generation
            "window-inside-a-segment",
            ScanRequest::current().window(40_000, 2 * SPAN + 30_000),
        ),
        (
            "narrow-window",
            ScanRequest::current().window(SPAN * 2 + 100, SPAN * 2 + 140),
        ),
        (
            "empty-window",
            ScanRequest::current().window(end + 10, end + 20),
        ),
    ]
}

/// The reference: the same scan with every segment kept whole, which is what
/// the per-segment engine's unit was.
fn reference(set: &ScanSet<'_, impl gnomon_core::segment::SegmentSource>,
             req: &ScanRequest) -> (Vec<(usize, u32)>, u64, u64) {
    let (sels, stats) = set.select_units_at(req, 1, usize::MAX).unwrap();
    (flat(&sels), stats.rows_selected, stats.rows_examined)
}

#[test]
fn no_row_is_dropped_or_doubled_at_a_unit_boundary() {
    let _serial = serial();
    let root = tmp_root("boundary");
    let store = build(&root);
    let segs = segments(&store);
    let set = scan_set(&segs, &store);

    // xorshift, so the boundaries are arbitrary but the failure is
    // reproducible from the seed printed in the message
    let mut seed: u64 = 0x2026_0921_1300_0d13;
    let mut rng = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    for (name, req) in requests() {
        let (want, want_selected, want_examined) = reference(&set, &req);
        for case in 0..60 {
            // 1 is the pathological cut — one unit per row — and the rest
            // spread over the interesting decade around the fixture's
            // segment sizes, including sizes that divide a segment exactly
            let rpu = match case % 6 {
                0 => 1,
                1 => 2,
                2 => (rng() % 512 + 1) as usize,
                3 => (rng() % 40_000 + 1) as usize,
                4 => 45_000,
                _ => (rng() % 200_000 + 1) as usize,
            };
            let threads = [1usize, 2, 3, 16][(rng() % 4) as usize];
            let (sels, stats) = set.select_units_at(&req, threads, rpu).unwrap();
            let got = flat(&sels);
            assert_eq!(
                got, want,
                "{name}: rows_per_unit={rpu} threads={threads}: the selected \
                 (segment, row) list must not depend on where the units were \
                 cut — a row in no unit or in two shows here and nowhere else"
            );
            assert_eq!(
                stats.rows_selected, want_selected,
                "{name}: rows_per_unit={rpu} threads={threads}: rows_selected"
            );
            assert_eq!(
                stats.rows_examined, want_examined,
                "{name}: rows_per_unit={rpu} threads={threads}: rows_examined \
                 — a boundary that clipped twice inflates this while leaving \
                 the answer intact"
            );
            // ranges are never empty and never overlap, per segment
            let mut last: Option<(usize, u32)> = None;
            for &(seg, row) in &got {
                if let Some((ls, lr)) = last {
                    assert!(
                        seg > ls || (seg == ls && row > lr),
                        "{name}: rows_per_unit={rpu}: rows must stay strictly \
                         ascending within a segment: {last:?} then {:?}",
                        (seg, row)
                    );
                }
                last = Some((seg, row));
            }
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn select_concatenates_the_units_back_into_the_pre_w13_contract() {
    let _serial = serial();
    let root = tmp_root("coalesce");
    let store = build(&root);
    let segs = segments(&store);
    let set = scan_set(&segs, &store);

    for (name, req) in requests() {
        let (want, _, _) = reference(&set, &req);
        for &rpu in &[1usize, 999, 65_536, usize::MAX] {
            let (sels, _) = set.select_units_at(&req, 16, rpu).unwrap();
            let _ = sels;
            // `select` is the public contract: one ascending selection per
            // segment, segments ascending, whatever the unit size was
            let (coalesced, stats) = set.select(&req).unwrap();
            assert_eq!(
                flat(&coalesced),
                want,
                "{name}: select must return the same rows as the units it is \
                 built from"
            );
            let mut segs_seen: Vec<usize> = coalesced.iter().map(|s| s.segment).collect();
            let sorted = {
                let mut v = segs_seen.clone();
                v.sort_unstable();
                v
            };
            assert_eq!(segs_seen, sorted, "{name}: selections ascend by segment");
            segs_seen.dedup();
            assert_eq!(
                segs_seen.len(),
                coalesced.len(),
                "{name}: select must return at most one selection per segment \
                 — cluster_order keys a selection by its first and last row, \
                 and two selections of one segment would split a cluster"
            );
            assert_eq!(
                stats.rows_selected as usize,
                want.len(),
                "{name}: stats travel with the rows"
            );
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// The stats a caller reads must not move with the cut. `segments_total`,
/// `segments_pruned` and the staircase block counters are per *segment*, and
/// the whole design is that they are computed once per segment and
/// shared — so a unit-granular scan must report exactly what a segment-granular
/// one did.
#[test]
fn the_per_segment_stats_are_counted_once_however_many_units() {
    let _serial = serial();
    let root = tmp_root("stats");
    let store = build(&root);
    let segs = segments(&store);
    let set = scan_set(&segs, &store);

    for (name, req) in requests() {
        let (_, whole) = set.select_units_at(&req, 1, usize::MAX).unwrap();
        for &rpu in &[1usize, 37, 4_096, 65_536] {
            for &threads in &[1usize, 2, 16] {
                let (_, cut) = set.select_units_at(&req, threads, rpu).unwrap();
                assert_eq!(
                    (
                        cut.segments_total,
                        cut.segments_pruned,
                        cut.blocks_skipped,
                        cut.blocks_considered
                    ),
                    (
                        whole.segments_total,
                        whole.segments_pruned,
                        whole.blocks_skipped,
                        whole.blocks_considered
                    ),
                    "{name}: rows_per_unit={rpu} threads={threads}: the \
                     per-segment stats must be counted once per segment, not \
                     once per unit"
                );
                assert_eq!(
                    cut.segments_total,
                    GEN_ROWS.len(),
                    "{name}: every segment is still accounted"
                );
            }
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// Repeating the identical call must give the identical answer. A claim
/// counter shared across calls, or one whose bound is wrong, drops every unit
/// of the second call and is otherwise silent.
#[test]
fn repeated_runs_are_identical() {
    let _serial = serial();
    let root = tmp_root("determinism");
    let store = build(&root);
    let segs = segments(&store);
    let set = scan_set(&segs, &store);

    for (name, req) in requests() {
        let mut first: Option<Vec<(usize, u32)>> = None;
        for rep in 0..8 {
            let (sels, _) = set.select_units_at(&req, 16, 3_001).unwrap();
            let got = flat(&sels);
            match &first {
                None => first = Some(got),
                Some(want) => assert_eq!(
                    &got, want,
                    "{name}: rep {rep} differs from rep 0 — the schedule must \
                     not reach the answer"
                ),
            }
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// The lane's reason for existing: a scan over **one** segment must now use
/// more than one unit once that segment carries enough rows. Under the
/// per-segment rule this was one unit and therefore one worker, whatever
/// `scan_threads()` said — which is what left a settled 10M store scanning
/// serially.
///
/// No environment knob: the derived unit size alone must do this, or the
/// A/B's settled arms gain nothing.
#[test]
fn a_single_segment_scan_uses_more_than_one_unit() {
    let _serial = serial();
    let root = tmp_root("onesegment");
    let store = build(&root);
    let segs = segments(&store);
    let big = segs
        .iter()
        .max_by_key(|s| s.rows())
        .expect("fixture has segments")
        .clone();
    assert!(
        big.rows() as usize > gnomon_core::defaults::MIN_ROWS_PER_UNIT,
        "the fixture's largest segment must exceed the unit floor"
    );
    let one = [big.clone()];
    let set = scan_set(&one, &store);

    phase::reset();
    let (sels, stats) = set
        .select_units(&ScanRequest {
            as_of_tt: OPEN_END,
            ..Default::default()
        })
        .unwrap();
    let units = phase_of("sel_units");
    let planned = phase_of("sel_segments_planned");
    assert_eq!(planned, 1, "one segment planned");
    assert!(
        units > 1,
        "a {}-row single-segment scan must cut more than one unit, got {units}",
        big.rows()
    );
    assert!(
        sels.len() as u64 <= units,
        "a unit yields at most one selection"
    );
    assert_eq!(
        stats.rows_selected as usize,
        flat(&sels).len(),
        "stats travel with the rows"
    );

    // and the small segments must *not* split: that is what the floor buys
    let small = segs
        .iter()
        .min_by_key(|s| s.rows())
        .expect("fixture has segments")
        .clone();
    assert!((small.rows() as usize) < gnomon_core::defaults::MIN_ROWS_PER_UNIT);
    let one = [small];
    let set = scan_set(&one, &store);
    phase::reset();
    let _ = set
        .select_units(&ScanRequest {
            as_of_tt: OPEN_END,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        phase_of("sel_units"),
        1,
        "a segment under the floor must stay exactly one unit"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The end-to-end property the A/B rests on: the aggregate's answer must be
/// bit-identical across thread widths **and** across unit sizes.
///
/// Sweeping the unit size is the new half. Self-scheduling already licensed varying the
/// width — every partial state is integer-valued and merges commutatively —
/// but a unit size that splits a segment hands one segment's rows to several
/// workers, so a group is now assembled from partials that did not exist
/// before. The merge argument covers that unchanged; this test is what
/// converts the argument into a measurement. `1` is the pathological size
/// (one unit per row, tens of thousands of partial maps merged) and it must
/// give the same answer as one unit per segment.
#[test]
fn the_aggregate_answer_is_identical_across_widths_and_unit_sizes() {
    let _serial = serial();
    let root = tmp_root("aggregate");
    let store = build(&root);
    let req = AggregateRequest {
        as_of_tt: OPEN_END,
        t_a: 0,
        t_b: GEN_ROWS.len() as i64 * SPAN,
        rel_types: None,
        dims: vec![Dim::RelType, Dim::TimeBucket { stride: SPAN / 4 }],
        aggs: vec![
            Agg::Count,
            Agg::CountDistinct { dst: true },
            Agg::Min(gnomon_core::aggregate::Source::VtS),
            Agg::Max(gnomon_core::aggregate::Source::VtS),
            Agg::Mean(gnomon_core::aggregate::Source::Duration),
        ],
        max_groups: 100_000,
    };
    let answer = |o: &AggregateOut| (o.keys.clone(), o.rel_names.clone(), o.aggs.clone());
    let total = |o: &AggregateOut| -> i64 {
        match &o.aggs[0] {
            AggValues::Int(v) => v.iter().map(|x| x.unwrap_or(0)).sum(),
            other => panic!("count must be an integer column, got {other:?}"),
        }
    };

    // the stored rows minus the corrections, which the whole-extent scan at
    // `as_of = OPEN_END` no longer believes
    std::env::set_var("GNOMON_PARALLEL_MIN_ROWS", "1");
    std::env::remove_var("GNOMON_ROWS_PER_UNIT");
    std::env::set_var("GNOMON_SCAN_THREADS", "1");
    phase::reset();
    let base = store.aggregate_edges(&req).unwrap();
    let base_units = phase_of("sel_units");
    let rows_total = total(&base);
    assert!(rows_total > 0, "the fixture must answer something");

    for width in ["1", "2", "16"] {
        for rpu in ["1", "7", "1000", "65536", "18446744073709551615"] {
            std::env::set_var("GNOMON_SCAN_THREADS", width);
            std::env::set_var("GNOMON_ROWS_PER_UNIT", rpu);
            // repeat inside the cell: a claim counter not reset per call
            // drops every unit of the second call
            for rep in 0..3 {
                phase::reset();
                let out = store.aggregate_edges(&req).unwrap();
                assert_eq!(
                    answer(&out),
                    answer(&base),
                    "width={width} rows_per_unit={rpu} rep={rep}: every column \
                     of the answer must be identical"
                );
                assert_eq!(
                    total(&out),
                    rows_total,
                    "width={width} rows_per_unit={rpu} rep={rep}: a dropped or \
                     doubled unit shows in the total first"
                );
                assert_eq!(
                    phase_of("fold_units"),
                    phase_of("sel_units"),
                    "width={width} rows_per_unit={rpu} rep={rep}: the fold must \
                     consume every unit the select stage cut, once each"
                );
                assert_eq!(
                    phase_of("fold_rows_walked"),
                    rows_total as u64,
                    "width={width} rows_per_unit={rpu} rep={rep}: the fold must \
                     walk exactly the rows the units carry"
                );
            }
        }
    }
    // rows_per_unit=1 really did produce many more units than the default,
    // so the sweep above was not five spellings of the same schedule
    std::env::set_var("GNOMON_ROWS_PER_UNIT", "1");
    phase::reset();
    let _ = store.aggregate_edges(&req).unwrap();
    assert!(
        phase_of("sel_units") > base_units * 100,
        "the forced unit size must actually change the cut: {} against {base_units}",
        phase_of("sel_units")
    );

    std::env::remove_var("GNOMON_ROWS_PER_UNIT");
    std::env::remove_var("GNOMON_SCAN_THREADS");
    std::env::remove_var("GNOMON_PARALLEL_MIN_ROWS");
    let _ = std::fs::remove_dir_all(&root);
}
