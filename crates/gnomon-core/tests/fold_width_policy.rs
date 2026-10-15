//! The read path's two derived quantities — the fold's width and the
//! row-range unit's size — and the knobs that let a sweep site them.
//!
//! Once the unit of a parallel read became a row range, a settled store that
//! could fill three fold workers now fills sixteen — and allocates sixteen
//! partial group tables where it allocated three. That product is what broke
//! the peak-RSS band on one store, and the remedy is a budget: the fold may
//! hold `FOLD_MAP_BUDGET_BYTES` of partial tables and no more, so its width
//! is `min(threads, budget / estimated table size)`.
//!
//! A width policy has exactly three ways to be wrong:
//!
//! 1. it does not actually bind — the budget is computed and then ignored;
//! 2. it binds and **changes the answer**, because narrowing the fold or
//!    sharing a table between workers reorders the merge;
//! 3. it binds *differently on repeated calls*, so a query's cost — and, if
//!    (2) were ever true, its answer — depends on call order.
//!
//! All three are invisible in a wall clock and all three are asserted here.
//! The answer is compared whole (every key column, every aggregate column,
//! every rehydrated name) against a single reference taken at one worker, and
//! it is compared across both overflow policies, because `Narrow` and `Share`
//! assemble a group from different partials and the commutativity argument
//! that licenses that is the thing under test.
//!
//! The fixture is small but genuinely splittable: three generations, a forced
//! unit size that cuts them into far more units than there are workers (see
//! `pin`), corrections scattered through them so the verdict ranges arrive
//! fragmented, and a grouping whose cardinality is large enough that a forced
//! tiny budget really does drive the cap to one.

use gnomon_core::aggregate::{Agg, AggValues, AggregateOut, AggregateRequest, Dim, Source};
use gnomon_core::derive::{edge_eid, version_vid};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::phase;
use gnomon_core::row::{EdgeRow, RowKind};
use gnomon_core::store::NativeStore;
use gnomon_core::OPEN_END;

const GEN_ROWS: [i64; 3] = [90_000, 70_000, 40_000];
const SPAN: i64 = 1_000_000;
const ENTITIES: i64 = 64;

/// `GNOMON_FOLD_BYTES`, `GNOMON_FOLD_OVERFLOW`, `GNOMON_SCAN_THREADS` and the phase
/// counters are all process-global and cargo runs a file's tests on several
/// threads. Every test here holds this for its whole body; poisoning is
/// ignored so a failed test does not turn the rest into spurious failures.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn tmp_root(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-w14-fold-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

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
            let vt_s = g as i64 * SPAN + i;
            let eid = edge_eid(&uid_s, &uid_d, rel, &disc);
            let vid = version_vid(&eid.to_hex(), tt, vt_s);
            // a close every ~1,700 rows, so several believed runs land inside
            // every unit and several unit boundaries land inside a run
            if i % 1_699 == 7 {
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

fn phase_of(name: &str) -> u64 {
    phase::snapshot_named()
        .into_iter()
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v)
        .unwrap_or_else(|| panic!("no phase named {name}"))
}

/// A grouping with enough keys that a small budget really binds: one group
/// per (rel, endpoint, bucket).
fn request() -> AggregateRequest {
    AggregateRequest {
        as_of_tt: OPEN_END,
        t_a: 0,
        t_b: GEN_ROWS.len() as i64 * SPAN,
        rel_types: None,
        dims: vec![Dim::RelType, Dim::Endpoint { dst: false }],
        aggs: vec![
            Agg::Count,
            Agg::CountDistinct { dst: true },
            Agg::Min(Source::VtS),
            Agg::Max(Source::VtS),
            Agg::Mean(Source::Duration),
        ],
        max_groups: 1_000_000,
    }
}

/// Everything about an answer that a width policy could possibly move: the
/// key columns, the rehydrated rel names, and every aggregate column. Named
/// because the comparisons below hold one of these across a whole sweep, and
/// an anonymous triple of vectors at that width is a type nobody reads twice.
type Answer = (Vec<Vec<i64>>, Vec<String>, Vec<AggValues>);

fn answer(o: &AggregateOut) -> Answer {
    (o.keys.clone(), o.rel_names.clone(), o.aggs.clone())
}

fn total(o: &AggregateOut) -> i64 {
    match &o.aggs[0] {
        AggValues::Int(v) => v.iter().map(|x| x.unwrap_or(0)).sum(),
        other => panic!("count must be an integer column, got {other:?}"),
    }
}

/// Open the gate and pin the width; every test starts from here so a leaked
/// variable from another file cannot decide a cell.
///
/// The unit size is forced small because the fixture is small: at the derived
/// size its 200,000 rows fall under `MIN_ROWS_PER_UNIT` per segment and cut
/// into five units, which cannot fan out past five workers and so could not
/// tell a binding cap from an idle one. Other tests already cover the derived
/// cut; what this file needs is more units than workers.
fn pin(width: &str) {
    std::env::set_var("GNOMON_PARALLEL_MIN_ROWS", "1");
    std::env::set_var("GNOMON_SCAN_THREADS", width);
    std::env::set_var("GNOMON_ROWS_PER_UNIT", "4096");
}

fn unpin() {
    for v in [
        "GNOMON_PARALLEL_MIN_ROWS",
        "GNOMON_SCAN_THREADS",
        "GNOMON_ROWS_PER_UNIT",
        "GNOMON_FOLD_BYTES",
        "GNOMON_FOLD_OVERFLOW",
    ] {
        std::env::remove_var(v);
    }
}

/// 1. The budget binds, and it binds by the number the formula names.
///
/// A one-byte budget cannot pay for a second table, so `max(1)` in the cap is
/// the whole width and the fold runs serial however many units the select
/// stage cut. A budget of exactly `k` tables' worth must produce `k`. The
/// counter `fold_width_cap` records the budget's verdict and `fold_workers`
/// records what the fold did with it: asserting both is what separates "the
/// policy computed a cap" from "the fold obeyed it".
#[test]
fn a_tiny_budget_caps_the_partial_tables_and_the_fold_obeys_it() {
    let _serial = serial();
    let root = tmp_root("cap");
    let store = build(&root);
    let req = request();
    pin("16");
    std::env::set_var("GNOMON_FOLD_OVERFLOW", "narrow");

    // unbounded: the original shape, and the control for everything below
    std::env::set_var("GNOMON_FOLD_BYTES", "0");
    phase::reset();
    let wide = store.aggregate_edges(&req).unwrap();
    let wide_workers = phase_of("fold_workers");
    let wide_units = phase_of("fold_units");
    let est = phase_of("fold_map_bytes_est");
    assert!(
        wide_workers > 1 && wide_units > wide_workers,
        "the fixture must fan out to be worth capping: {wide_workers} workers \
         over {wide_units} units"
    );
    assert_eq!(
        phase_of("fold_width_cap"),
        16,
        "an unbounded budget must not cap anything"
    );
    assert!(est > 0, "a grouped aggregate must estimate a non-zero table");

    // one byte: no budget can pay for two tables, so the floor of one holds
    std::env::set_var("GNOMON_FOLD_BYTES", "1");
    phase::reset();
    let narrow = store.aggregate_edges(&req).unwrap();
    assert_eq!(phase_of("fold_width_cap"), 1, "a 1-byte budget caps at one");
    assert_eq!(
        phase_of("fold_workers"),
        1,
        "the cap must reach the fold, not only the counter"
    );
    assert_eq!(
        phase_of("fold_units"),
        wide_units,
        "narrowing the fold must not change how the select stage cut"
    );
    assert_eq!(
        answer(&narrow),
        answer(&wide),
        "the capped fold must answer exactly what the wide one did"
    );

    // a budget sized to k tables must produce k, for k strictly between the
    // floor and the thread count — the case where the formula, not one of its
    // two clamps, decides
    for k in [2u64, 3, 5] {
        std::env::set_var("GNOMON_FOLD_BYTES", (est * k).to_string());
        phase::reset();
        let out = store.aggregate_edges(&req).unwrap();
        assert_eq!(
            phase_of("fold_width_cap"),
            k,
            "a budget of exactly {k} tables must cap at {k}"
        );
        assert_eq!(phase_of("fold_workers"), k, "and the fold must run {k} wide");
        assert_eq!(answer(&out), answer(&wide), "at {k} tables the answer moves");
    }

    unpin();
    let _ = std::fs::remove_dir_all(&root);
}

/// 2. The answer survives every width, every budget and both overflow
///    policies — including the pathological cells at each end.
///
/// Width and budget are swept together because they are not the same knob:
/// the width says how many workers walk rows and the budget says how many
/// tables they may fill, and `Share` is the cell where those two numbers
/// differ. Three repetitions inside every cell catch the failure a claim
/// counter or a shared table that is not reset per call produces — which
/// shows on the *second* call, never the first.
#[test]
fn the_answer_is_identical_at_every_width_budget_and_overflow_policy() {
    let _serial = serial();
    let root = tmp_root("widths");
    let store = build(&root);
    let req = request();

    pin("1");
    std::env::set_var("GNOMON_FOLD_BYTES", "0");
    std::env::set_var("GNOMON_FOLD_OVERFLOW", "narrow");
    phase::reset();
    let base = store.aggregate_edges(&req).unwrap();
    let rows_total = total(&base);
    let groups = base.keys.first().map(|c| c.len()).unwrap_or(0);
    assert!(rows_total > 0 && groups > 100, "the fixture must answer something \
            with many groups: {rows_total} rows in {groups} groups");
    let est = {
        phase::reset();
        let _ = store.aggregate_edges(&req).unwrap();
        phase_of("fold_map_bytes_est")
    };

    for overflow in ["narrow", "share"] {
        std::env::set_var("GNOMON_FOLD_OVERFLOW", overflow);
        for width in ["1", "2", "16"] {
            pin(width);
            for budget in ["0", "1", "4096"] {
                std::env::set_var("GNOMON_FOLD_BYTES", budget);
                for rep in 0..3 {
                    phase::reset();
                    let out = store.aggregate_edges(&req).unwrap();
                    let cell = format!(
                        "overflow={overflow} width={width} budget={budget} rep={rep}"
                    );
                    assert_eq!(
                        answer(&out),
                        answer(&base),
                        "{cell}: every column of the answer must be identical"
                    );
                    assert_eq!(
                        total(&out),
                        rows_total,
                        "{cell}: a dropped or doubled unit shows in the total first"
                    );
                    assert_eq!(
                        phase_of("fold_units"),
                        phase_of("sel_units"),
                        "{cell}: the fold must consume every unit the select stage cut"
                    );
                    assert_eq!(
                        phase_of("fold_rows_walked"),
                        rows_total as u64,
                        "{cell}: the fold must walk exactly the rows the units carry"
                    );
                }
            }
            // the sizes between the clamps, where the cap is the formula's own
            // arithmetic rather than one of its bounds
            for k in [2u64, 7] {
                std::env::set_var("GNOMON_FOLD_BYTES", (est * k).to_string());
                phase::reset();
                let out = store.aggregate_edges(&req).unwrap();
                assert_eq!(
                    answer(&out),
                    answer(&base),
                    "overflow={overflow} width={width} tables={k}: the answer moves"
                );
            }
        }
    }

    unpin();
    let _ = std::fs::remove_dir_all(&root);
}

/// 3. `Share` really does keep the width the budget took away.
///
/// Without this the two policies would be indistinguishable from the answer
/// alone, and `Share` could quietly be a second spelling of `Narrow`. Under a
/// budget that admits one table, `Narrow` must fold with one worker and
/// `Share` must fold with the full width — same answer, different shape.
#[test]
fn share_keeps_the_width_that_narrow_gives_up() {
    let _serial = serial();
    let root = tmp_root("share");
    let store = build(&root);
    let req = request();
    pin("8");

    std::env::set_var("GNOMON_FOLD_BYTES", "1");
    std::env::set_var("GNOMON_FOLD_OVERFLOW", "narrow");
    phase::reset();
    let a = store.aggregate_edges(&req).unwrap();
    let narrow_workers = phase_of("fold_workers");

    std::env::set_var("GNOMON_FOLD_OVERFLOW", "share");
    phase::reset();
    let b = store.aggregate_edges(&req).unwrap();
    let share_workers = phase_of("fold_workers");

    assert_eq!(narrow_workers, 1, "narrow spends the width it cannot afford");
    assert!(
        share_workers > narrow_workers,
        "share must keep the row loop wide: {share_workers} against {narrow_workers}"
    );
    assert_eq!(
        phase_of("fold_map_bytes_max"),
        phase_of("fold_map_bytes"),
        "under a one-table budget share must account for exactly one table"
    );
    assert_eq!(answer(&a), answer(&b), "the two policies must agree exactly");

    unpin();
    let _ = std::fs::remove_dir_all(&root);
}

/// 4. The estimate bounds the thing it sizes.
///
/// The cap is only as good as `est_map_bytes`, and an estimator that reads
/// low by an order of magnitude would hand back a width the budget cannot
/// actually pay for — the failure that looks like a working policy right up
/// until the memory is measured. `fold_map_bytes_max` is the largest table
/// that really existed, in the same accounting, so the two are comparable
/// directly. The slack is for the table's growth policy, which doubles: a
/// map holding `n` entries can have reserved up to about `2n`.
#[test]
fn the_estimated_table_size_bounds_the_measured_one() {
    let _serial = serial();
    let root = tmp_root("estimate");
    let store = build(&root);
    let req = request();

    for width in ["1", "4", "16"] {
        pin(width);
        std::env::set_var("GNOMON_FOLD_BYTES", "0");
        std::env::set_var("GNOMON_FOLD_OVERFLOW", "narrow");
        phase::reset();
        let _ = store.aggregate_edges(&req).unwrap();
        let est = phase_of("fold_map_bytes_est");
        let measured_max = phase_of("fold_map_bytes_max");
        let measured_sum = phase_of("fold_map_bytes");
        let workers = phase_of("fold_workers");
        assert!(
            measured_max > 0 && est > 0,
            "width={width}: both quantities must be measured, got est={est} \
             max={measured_max}"
        );
        assert!(
            measured_max <= est * 4,
            "width={width}: the estimate must bound one table — measured \
             {measured_max} against an estimate of {est}"
        );
        assert!(
            measured_sum <= measured_max * workers,
            "width={width}: the sum over {workers} workers cannot exceed the \
             peak times the count — {measured_sum} against {measured_max}"
        );
    }

    unpin();
    let _ = std::fs::remove_dir_all(&root);
}

/// 5. An ungrouped aggregate is one group, and the budget must never narrow
///    it on the strength of a dimension it does not have.
///
/// The estimator multiplies dimension code spaces, and an empty product is
/// one. Getting that wrong in the other direction — a scalar aggregate
/// estimated at the dictionary's size — would cap the most common query shape
/// in the registry at a single worker.
#[test]
fn a_scalar_aggregate_is_never_narrowed_by_the_budget() {
    let _serial = serial();
    let root = tmp_root("scalar");
    let mut store = build(&root);
    // an ungrouped count is answered from range lengths and never
    // reaches the fold; this test is about the fold's width, so it runs the
    // row path (`GNOMON_AGG_RANGE_COUNT=0`)
    store.set_designs(gnomon_core::designs::Designs {
        agg_range_count: false,
        ..gnomon_core::designs::Designs::ALL
    });
    let req = AggregateRequest {
        dims: vec![],
        aggs: vec![Agg::Count],
        ..request()
    };
    pin("16");
    std::env::remove_var("GNOMON_FOLD_BYTES");
    std::env::set_var("GNOMON_FOLD_OVERFLOW", "narrow");
    phase::reset();
    let out = store.aggregate_edges(&req).unwrap();
    assert_eq!(out.keys.len(), 0, "an ungrouped call emits no key columns");
    assert_eq!(
        phase_of("fold_width_cap"),
        16,
        "one group under the default budget must not cap the width"
    );
    assert!(
        phase_of("fold_workers") > 1,
        "and the fold must still fan out"
    );

    unpin();
    let _ = std::fs::remove_dir_all(&root);
}

/// 6. The unit-size knobs move the formula's inputs, not its output.
///
/// The two constants were chosen rather than swept, and the only override
/// that existed — `GNOMON_ROWS_PER_UNIT` — says what the unit size *is*, which
/// makes it a test hook: a sweep using it measures sizes the engine would
/// never derive, at stores and widths where it would derive something else.
/// `GNOMON_UNITS_PER_THREAD` and `GNOMON_MIN_ROWS_PER_UNIT` replace the formula's
/// two inputs instead, so every point of a sweep is a size the shipped engine
/// really would choose. The assertions are on `sel_units`, which is the
/// formula's observable output: doubling the oversubscription factor must
/// (at least) not reduce the units, and a floor above the segment's rows must
/// collapse every segment to exactly one unit — which is the original shape,
/// and the strongest statement the floor makes.
#[test]
fn the_unit_size_knobs_move_the_derivation_and_not_the_answer() {
    let _serial = serial();
    let root = tmp_root("unitsize");
    let store = build(&root);
    let req = request();
    std::env::set_var("GNOMON_PARALLEL_MIN_ROWS", "1");
    std::env::set_var("GNOMON_SCAN_THREADS", "4");
    std::env::remove_var("GNOMON_ROWS_PER_UNIT");
    std::env::remove_var("GNOMON_FOLD_BYTES");
    std::env::set_var("GNOMON_FOLD_OVERFLOW", "narrow");

    // a floor under the derived size, so the oversubscription factor decides
    std::env::set_var("GNOMON_MIN_ROWS_PER_UNIT", "1024");
    let mut units_at: Vec<(u64, u64)> = Vec::new();
    let mut reference: Option<Answer> = None;
    for k in [1u64, 2, 4, 8] {
        std::env::set_var("GNOMON_UNITS_PER_THREAD", k.to_string());
        phase::reset();
        let out = store.aggregate_edges(&req).unwrap();
        units_at.push((k, phase_of("sel_units")));
        match &reference {
            None => reference = Some(answer(&out)),
            Some(r) => assert_eq!(&answer(&out), r, "k={k}: the answer moved"),
        }
    }
    for w in units_at.windows(2) {
        let ((k0, u0), (k1, u1)) = (w[0], w[1]);
        assert!(
            u1 >= u0,
            "k={k1} must not cut fewer units than k={k0}: {u1} against {u0}"
        );
    }
    assert!(
        units_at.last().unwrap().1 > units_at[0].1,
        "the factor must actually change the cut across {:?}",
        units_at
    );

    // a floor above every segment's row count: one unit per segment, which is
    // exactly what the original engine cut
    std::env::set_var("GNOMON_UNITS_PER_THREAD", "8");
    std::env::set_var("GNOMON_MIN_ROWS_PER_UNIT", "100000000");
    phase::reset();
    let out = store.aggregate_edges(&req).unwrap();
    assert_eq!(
        phase_of("sel_units"),
        phase_of("sel_segments_planned"),
        "a floor over the segment size must collapse to one unit per segment"
    );
    assert_eq!(
        &answer(&out),
        reference.as_ref().unwrap(),
        "the original cut must answer what every other cut answers"
    );

    std::env::remove_var("GNOMON_UNITS_PER_THREAD");
    std::env::remove_var("GNOMON_MIN_ROWS_PER_UNIT");
    unpin();
    let _ = std::fs::remove_dir_all(&root);
}
