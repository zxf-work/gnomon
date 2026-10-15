//! The parallel fold must be scheduled by work, and must still be exact.
//!
//! The aggregate path's two parallel stages used to split their units into
//! contiguous chunks of `ceil(n/threads)`, which divides them equally by
//! *count*. Segment sizes in a tiered store are a geometric staircase, so on
//! that layout equal-count is not equal-work and the stage's makespan tracked
//! the layout instead of the rows: 4.65 M rows against an ideal of 1.88 M on
//! the measured 30M store, parallel efficiency 0.44 against a settled store's
//! 0.81. Workers now claim the next unclaimed unit instead.
//!
//! A self-scheduling loop has exactly two ways to be wrong, and neither is
//! visible in a wall clock: it can drop a unit, or hand the same unit to two
//! workers. Both change the answer, and both are silent on a fixture whose
//! segments are the same size. So the fixture here is deliberately skewed —
//! generations of 10, 20, 40 000, 30, 70 000, 50, 25 000, 10 rows — which is
//! the shape the fix targets and also the shape that makes a dropped unit
//! cheap to hide: lose the smallest and the total moves by one part in
//! 13,500.
//!
//! The assertions are exact counts and exact answers, never a duration: a
//! timing assertion on a shared host is a flake generator. The one timing
//! test here asserts only that the phase timers account for the wall they
//! claim to decompose, which is a property of the instrument, not of the
//! host's load.

use gnomon_core::aggregate::{Agg, AggValues, AggregateOut, AggregateRequest, Dim};
use gnomon_core::derive::{edge_eid, version_vid};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::phase;
use gnomon_core::row::EdgeRow;
use gnomon_core::store::NativeStore;
use gnomon_core::OPEN_END;

/// Rows per generation. Every generation is one segment, so this is the
/// per-unit work of the fold — and it is deliberately far from uniform.
///
/// Large enough that the whole-extent aggregate over it is milliseconds
/// rather than microseconds. That matters only for the accounting test: the
/// slice of a call the phases deliberately do not cover (thread spawn, the
/// belief assertion, the manifest read) is a fixed cost of tens of
/// microseconds, so on a 100 us query it is a quarter of the wall and on a
/// real one it is a rounding error. Sizing the fixture is what makes "within
/// 5 %" a statement about the decomposition instead of about the fixture.
const GEN_ROWS: [i64; 8] = [10, 20, 40_000, 30, 70_000, 50, 25_000, 10];
const SPAN: i64 = 1_000_000;

/// The phase counters and `GNOMON_SCAN_THREADS` are both process-global, and
/// cargo runs the tests in one binary on several threads. Every test in this
/// file therefore takes this lock for its whole body: without it a second
/// test's `aggregate_edges` lands in the first one's counter bracket and the
/// exact-count assertions read another test's work. Poisoning is ignored on
/// purpose — a failed test must not turn the rest into spurious failures.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn tmp_root(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-fold-sched-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn build(root: &std::path::Path) -> NativeStore {
    let mut store = NativeStore::open(root).unwrap();
    let mut counter = 0u32;
    for (g, n) in GEN_ROWS.iter().enumerate() {
        let tt = 100 + g as i64;
        store.begin(tt).unwrap();
        for i in 0..*n {
            let (uid_s, uid_d) = (format!("n{}", i % 7), format!("n{}", (i + 3) % 7));
            let src_id = store.ensure_entity(&uid_s, "Node").unwrap();
            let dst_id = store.ensure_entity(&uid_d, "Node").unwrap();
            let rel = ["SENT", "RATED", "PAID"][(i % 3) as usize];
            let disc = format!("#{counter}");
            counter += 1;
            // generations tile valid time, so a whole-extent window opens
            // every segment and no manifest prune can mask a lost unit
            let vt_s = g as i64 * SPAN + i;
            let eid = edge_eid(&uid_s, &uid_d, rel, &disc);
            store
                .stage_edge(EdgeRow {
                    vid: version_vid(&eid.to_hex(), tt, vt_s),
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
    store
}

fn whole_extent() -> AggregateRequest {
    AggregateRequest {
        as_of_tt: OPEN_END,
        t_a: 0,
        t_b: GEN_ROWS.len() as i64 * SPAN,
        rel_types: None,
        dims: vec![Dim::RelType, Dim::TimeBucket { stride: SPAN / 4 }],
        aggs: vec![
            Agg::Count,
            Agg::CountDistinct { dst: true },
            Agg::Min(gnomon_core::aggregate::Source::VtS),
            Agg::Mean(gnomon_core::aggregate::Source::Duration),
        ],
        max_groups: 100_000,
    }
}

/// Every column of the answer, so a schedule that lost a unit cannot hide
/// inside a total that happens to match.
fn answer(out: &AggregateOut) -> (Vec<Vec<i64>>, Vec<String>, Vec<AggValues>) {
    (out.keys.clone(), out.rel_names.clone(), out.aggs.clone())
}

fn total_count(out: &AggregateOut) -> i64 {
    match &out.aggs[0] {
        AggValues::Int(v) => v.iter().map(|x| x.unwrap_or(0)).sum(),
        other => panic!("count must be an integer column, got {other:?}"),
    }
}

fn phase_of(name: &str) -> u64 {
    phase::snapshot_named()
        .into_iter()
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v)
        .unwrap_or_else(|| panic!("no phase named {name}"))
}

/// Widths to sweep. 1 takes the serial path, 2 is the smallest fan-out the
/// gates admit, and 16 is more workers than there are units — the case where
/// a self-scheduling loop that mis-bounds its claim index runs off the end.
const WIDTHS: [&str; 3] = ["1", "2", "16"];

/// `GNOMON_SCAN_THREADS` is process-global, so the width sweep must not run
/// beside another test that reads it. One `#[test]` owning the sweep is the
/// whole guard; do not split these into separate test functions.
#[test]
fn the_answer_and_the_unit_accounting_are_exact_at_every_width() {
    let _serial = serial();
    let root = tmp_root("widths");
    let store = build(&root);
    let req = whole_extent();
    let rows_total: i64 = GEN_ROWS.iter().sum();

    let mut base: Option<AggregateOut> = None;
    for w in WIDTHS {
        std::env::set_var("GNOMON_SCAN_THREADS", w);
        // also force the row gate open, so width `w` really is the width
        std::env::set_var("GNOMON_PARALLEL_MIN_ROWS", "1");

        // repeat inside the width: a self-scheduling loop whose claim counter
        // is not reset per call drops every unit of the second call
        for rep in 0..4 {
            phase::reset();
            let out = store.aggregate_edges(&req).unwrap();

            assert_eq!(
                out.stats.segments_opened,
                GEN_ROWS.len(),
                "width={w} rep={rep}: the whole-extent window must open every \
                 segment"
            );
            assert_eq!(
                total_count(&out),
                rows_total,
                "width={w} rep={rep}: counts must total every stored row; a \
                 dropped or doubled selection shows here first"
            );

            // The fold's own accounting, which is what the claim loop can
            // corrupt without changing a wall clock.
            // The unit is `(segment, row range)`, not the segment, so
            // this is no longer `GEN_ROWS.len()` — the 70,000-row generation
            // splits. What must still hold exactly is that the fold consumes
            // every unit the select stage cut, once each.
            assert_eq!(
                phase_of("fold_units"),
                phase_of("sel_units"),
                "width={w} rep={rep}: every unit must be folded exactly once \
                 — no unit dropped, none claimed twice"
            );
            assert!(
                phase_of("sel_units") >= GEN_ROWS.len() as u64,
                "width={w} rep={rep}: a segment yields at least one unit"
            );
            assert_eq!(
                phase_of("sel_segments_planned"),
                GEN_ROWS.len() as u64,
                "width={w} rep={rep}: the units must come from every segment"
            );
            assert_eq!(
                phase_of("fold_rows_walked"),
                rows_total as u64,
                "width={w} rep={rep}: the fold must walk exactly the rows the \
                 selections carry"
            );
            assert_eq!(
                phase_of("sel_rows_examined"),
                rows_total as u64,
                "width={w} rep={rep}: the select stage must examine every row \
                 exactly once"
            );
            assert_eq!(
                out.stats.rows_selected, rows_total as u64,
                "width={w} rep={rep}"
            );

            match &base {
                None => base = Some(out),
                Some(b) => assert_eq!(
                    answer(&out),
                    answer(b),
                    "width={w} rep={rep}: the answer must not depend on the \
                     width, nor on which worker claimed which selection"
                ),
            }
        }
    }
    std::env::remove_var("GNOMON_SCAN_THREADS");
    std::env::remove_var("GNOMON_PARALLEL_MIN_ROWS");
    let _ = std::fs::remove_dir_all(&root);
}

/// The instrument's own contract: the phases the aggregate path reports must
/// account for the wall it spent, or the attribution built on them is a
/// subtraction against an unmeasured remainder.
///
/// Five per cent is the tolerance this test sets. It is a property of
/// the decomposition, not of the machine: the phases nest, so a loaded host
/// makes every term larger together.
#[test]
fn the_phase_timers_account_for_the_query_wall() {
    let _serial = serial();
    let root = tmp_root("account");
    let store = build(&root);
    let req = whole_extent();

    // warm: the first call decodes and verifies, which is real work the
    // phases deliberately do not separate out
    for _ in 0..3 {
        store.aggregate_edges(&req).unwrap();
    }

    phase::reset();
    let t = std::time::Instant::now();
    store.aggregate_edges(&req).unwrap();
    let wall_ns = t.elapsed().as_nanos() as u64;

    // the top-level decomposition: every phase of `aggregate_edges` that is
    // not nested inside another one
    let accounted: u64 = [
        "agg_labels_ns",
        "agg_prune_ns",
        "agg_open_ns",
        "agg_rel_names_ns",
        "agg_select_wall_ns",
        "agg_fold_wall_ns",
        "agg_finalize_ns",
    ]
    .iter()
    .map(|k| phase_of(k))
    .sum();

    assert_eq!(phase_of("agg_calls"), 1, "one bracketed call");
    assert!(
        accounted <= wall_ns,
        "the phases cannot exceed the wall they decompose: {accounted} > {wall_ns}"
    );
    let missing = wall_ns - accounted;
    // 5 % of the wall, or 100 us, whichever is larger. The second clause is
    // not slack: the unattributed part is a *constant* per call — spawning
    // the workers, `assert_full_belief`, reading the manifest — and on a
    // host where a scoped spawn is slow it can exceed 5 % of a millisecond
    // query while still being invisible on the 60-2,000 ms queries the phase
    // split is used to attribute (accounted fraction 0.96-0.98, measured on
    // the development machine at both scales and on both layouts). A phase that went missing
    // would blow both clauses at once.
    assert!(
        missing * 20 <= wall_ns || missing <= 100_000,
        "the phases must account for the query wall to within 5 % (or 100 us): \
         accounted {accounted} ns of {wall_ns} ns, {missing} ns unattributed"
    );

    // and the nested phases stay inside their stage. An earlier revision split the select
    // stage in two: the per-segment phases are the planning pass, which every
    // row-range unit of a segment shares, and `sel_busy_ns` is now the row
    // loop alone.
    assert!(
        phase_of("sel_closes_ns") + phase_of("sel_cols_ns") <= phase_of("sel_plan_busy_ns"),
        "the per-segment select phases must sum inside the planning pass"
    );
    assert!(
        phase_of("sel_rows_ns") <= phase_of("sel_busy_ns"),
        "the row loop must sum inside the row stage's busy time"
    );
    // `fold_cols_ns` is the once-per-segment column resolution, hoisted
    // out of the workers so the units of one segment share it — so it sits in
    // the fold stage's wall but not in its workers' busy time. The row loop
    // still does.
    assert!(
        phase_of("fold_rows_ns") <= phase_of("fold_busy_ns"),
        "the fold's row loop must sum inside the stage's busy time"
    );
    assert!(
        phase_of("fold_cols_ns") <= phase_of("agg_fold_wall_ns"),
        "the shared column resolution must sit inside the fold stage's wall"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// Selections must reach the fold in ascending segment order however the
/// workers claimed them. Nothing in the aggregate depends on that order — but
/// `ScanSet::select` is shared with the scan and materialize paths, whose
/// cluster ordering does, and the old chunking supplied it for free.
#[test]
fn select_returns_selections_in_segment_order() {
    let _serial = serial();
    use gnomon_core::scan::{ScanRequest, ScanSet, ScanTarget};

    let root = tmp_root("order");
    let store = build(&root);
    // Warm the aggregate path so the segments are open, then read the same
    // selections the fold would see.
    store.aggregate_edges(&whole_extent()).unwrap();

    let layout = store.segment_layout();
    let files: Vec<String> = layout
        .iter()
        .filter(|r| r.0.starts_with("edge"))
        .map(|r| r.1.clone())
        .collect();
    assert!(files.len() >= 4, "fixture must have several edge segments");

    let segs: Vec<_> = files
        .iter()
        .map(|f| store.open_segment(f).expect("segment opens"))
        .collect();
    let targets: Vec<ScanTarget<'_, _>> = segs
        .iter()
        .enumerate()
        .map(|(i, s)| ScanTarget {
            segment: s,
            lane: gnomon_core::row::Lane::Event,
            id: i as u64,
        })
        .collect();
    let set = ScanSet::new(targets);
    let (sel, _stats) = set
        .select(&ScanRequest {
            as_of_tt: OPEN_END,
            ..Default::default()
        })
        .unwrap();
    let idxs: Vec<usize> = sel.iter().map(|s| s.segment).collect();
    let mut sorted = idxs.clone();
    sorted.sort_unstable();
    assert_eq!(
        idxs, sorted,
        "select must return selections ascending by segment index"
    );
    let _ = std::fs::remove_dir_all(&root);
}
