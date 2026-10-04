//! A windowed aggregate opens only the segments whose manifest bounds
//! intersect its window.
//!
//! `aggregate_edges` used to build its target list from the **whole**
//! manifest and open every entry before `ScanSet::select` had a chance to
//! prune anything. With the
//! opens uncached that made every aggregate re-decode the whole store on
//! every call, and a 10-tick window cost 76 % of a whole-store aggregate at
//! 1M rows — the window had lost its selectivity entirely.
//!
//! The fix prunes on the manifest's own zone maps first
//! (`scan::prunes_bounds`, the same tier-1 predicate `ScanSet::prunes` uses)
//! and only then opens, cached. This file asserts the *mechanism* rather
//! than a duration: `AggregateOut::stats` reports how many segments the
//! manifest named and how many survived, so "a narrow window is cheap" is a
//! counted fact here and not a stopwatch reading. The latency consequence is
//! gated separately, from Python, in
//! `tests/test_aggregate_window_selectivity.py`.
//!
//! The fixture gives each generation a **disjoint** valid-time range, which
//! is what makes the assertion meaningful: with overlapping ranges every
//! segment intersects every window and a correct prune would still open them
//! all. A batch ingest writes one segment per generation here (the segment
//! target is far above one batch's bytes), so segment count is known exactly.

use gnomon_core::aggregate::{Agg, AggregateRequest, Dim};
use gnomon_core::derive::{edge_eid, version_vid};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::row::EdgeRow;
use gnomon_core::store::NativeStore;
use gnomon_core::OPEN_END;

const ROWS_PER_GEN: i64 = 20;
const GENS: i64 = 12;
/// Generation `g` owns `[g * SPAN, g * SPAN + ROWS_PER_GEN)`.
const SPAN: i64 = 1_000;

fn tmp_root(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-aggprune-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// `GENS` generations, each one segment, with disjoint `vt` ranges.
fn build(root: &std::path::Path) -> NativeStore {
    let mut store = NativeStore::open(root).unwrap();
    let mut counter = 0u32;
    for g in 0..GENS {
        let tt = 100 + g;
        store.begin(tt).unwrap();
        for i in 0..ROWS_PER_GEN {
            let (uid_s, uid_d) = (format!("n{}", i % 5), format!("n{}", (i + 1) % 5));
            let src_id = store.ensure_entity(&uid_s, "Node").unwrap();
            let dst_id = store.ensure_entity(&uid_d, "Node").unwrap();
            let rel = if i % 2 == 0 { "SENT" } else { "RATED" };
            let disc = format!("#{counter}");
            counter += 1;
            let vt_s = g * SPAN + i;
            let eid = edge_eid(&uid_s, &uid_d, rel, &disc);
            store
                .stage_edge(EdgeRow {
                    vid: version_vid(&eid.to_hex(), tt, vt_s),
                    src_id,
                    dst_id,
                    rel_type: rel.to_string(),
                    disc,
                    vt_s,
                    // event lane: `vt_e` is elided as `vt_s + 1`, so the
                    // manifest's `vt_e_max` is `vt_max + 1` and the prune's
                    // `vt_e_max <= vt_min` test is exact rather than
                    // conservative on this fixture
                    vt_e: vt_s + 1,
                    tt_s: tt,
                    props: "{}".to_string(),
                    source: "ingest".to_string(),
                    provenance_ref: None,
                })
                .unwrap();
        }
        store.commit(EventLogRef::default()).unwrap();
    }
    store
}

fn count_request(t_a: i64, t_b: i64) -> AggregateRequest {
    AggregateRequest {
        as_of_tt: OPEN_END,
        t_a,
        t_b,
        rel_types: None,
        dims: vec![Dim::RelType],
        aggs: vec![Agg::Count],
        max_groups: 1_000,
    }
}

fn total_count(out: &gnomon_core::aggregate::AggregateOut) -> i64 {
    match &out.aggs[0] {
        gnomon_core::aggregate::AggValues::Int(v) => v.iter().map(|c| c.unwrap_or(0)).sum(),
        other => panic!("count must be an integer column, got {other:?}"),
    }
}

#[test]
fn a_narrow_window_opens_only_the_segments_its_bounds_intersect() {
    let root = tmp_root("narrow");
    let store = build(&root);

    // The fixture has to be the shape the assertion assumes, or the gate is
    // vacuous: one segment per generation, and enough of them that opening
    // "only the intersecting ones" is a visible saving.
    let whole = store
        .aggregate_edges(&count_request(0, GENS * SPAN))
        .unwrap();
    assert_eq!(
        whole.stats.segments_total, GENS as usize,
        "expected one segment per generation; got {} for {GENS} generations",
        whole.stats.segments_total
    );
    assert_eq!(
        whole.stats.segments_pruned, 0,
        "a whole-extent window cannot prune anything"
    );
    assert_eq!(whole.stats.segments_opened, GENS as usize);
    assert_eq!(total_count(&whole), GENS * ROWS_PER_GEN);

    // One generation's range, so exactly one segment can contribute.
    let one = store.aggregate_edges(&count_request(3 * SPAN, 3 * SPAN + 10)).unwrap();
    assert_eq!(
        one.stats.segments_opened, 1,
        "a window inside generation 3's range must open exactly the one \
         segment whose manifest bounds intersect it, not {} of {}",
        one.stats.segments_opened, one.stats.segments_total
    );
    assert_eq!(one.stats.segments_pruned, GENS as usize - 1);
    assert_eq!(
        one.stats.segments_pruned + one.stats.segments_opened,
        one.stats.segments_total
    );
    assert_eq!(total_count(&one), 10, "the answer itself must be unchanged");

    // A window in the gap between two generations' ranges: every segment is
    // prunable, and the answer is empty rather than wrong.
    let gap = store
        .aggregate_edges(&count_request(3 * SPAN + ROWS_PER_GEN, 4 * SPAN))
        .unwrap();
    assert_eq!(gap.stats.segments_opened, 0, "nothing intersects the gap");
    assert_eq!(gap.stats.segments_pruned, GENS as usize);
    assert_eq!(total_count(&gap), 0);

    // A window spanning three generations opens three segments.
    let three = store
        .aggregate_edges(&count_request(2 * SPAN, 4 * SPAN + ROWS_PER_GEN))
        .unwrap();
    assert_eq!(three.stats.segments_opened, 3);
    assert_eq!(total_count(&three), 3 * ROWS_PER_GEN);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn pruning_does_not_change_the_answer_at_any_window() {
    let root = tmp_root("answers");
    let store = build(&root);

    // Every window, against a count computed from the fixture's own
    // arithmetic: pruning is only allowed to remove segments that could not
    // have contributed a row.
    for g in 0..GENS {
        for (t_a, t_b, expect) in [
            (g * SPAN, g * SPAN + ROWS_PER_GEN, ROWS_PER_GEN),
            (g * SPAN, g * SPAN + 1, 1),
            (g * SPAN + ROWS_PER_GEN - 1, g * SPAN + ROWS_PER_GEN, 1),
            (g * SPAN + ROWS_PER_GEN, (g + 1) * SPAN, 0),
        ] {
            let out = store.aggregate_edges(&count_request(t_a, t_b)).unwrap();
            assert_eq!(
                total_count(&out),
                expect,
                "window [{t_a}, {t_b}) over generation {g}: opened {} of {} segments",
                out.stats.segments_opened,
                out.stats.segments_total
            );
        }
    }

    // An `as_of_tt` before a generation's own transaction time must prune it
    // on the folded `tt_s` bound the manifest carries, the same fast reject
    // `ScanSet::prunes` applies from the header.
    let mut req = count_request(0, GENS * SPAN);
    req.as_of_tt = 100 + 3; // generations 0..=3 believed, 4.. not yet
    let early = store.aggregate_edges(&req).unwrap();
    assert_eq!(
        early.stats.segments_opened, 4,
        "as_of_tt must prune the generations written after it"
    );
    assert_eq!(total_count(&early), 4 * ROWS_PER_GEN);

    let _ = std::fs::remove_dir_all(&root);
}
