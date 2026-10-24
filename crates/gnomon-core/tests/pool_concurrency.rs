//! Concurrent callers on one handle share the process-wide worker
//! pool and each get the single-threaded answer.
//!
//! Two Python threads calling the engine on one store (through any binding
//! that releases the GIL) is this: several OS threads entering the parallel
//! read stages at once. The pool's contract is that a caller also claims its
//! own indices and never waits for an index nobody picked up, so concurrent
//! and nested calls cannot deadlock; this holds the whole read path to it on
//! a store, with the stages forced wide (`GNOMON_PARALLEL_MIN_ROWS=1`) so the
//! planner, the row loop, the materializer, the concatenation and the fold
//! all fan out on every call.
//!
//! Its own test binary, so the process-global knobs it sets cannot reach
//! another file's tests.

use gnomon_core::aggregate::{Agg, AggregateRequest, Dim};
use gnomon_core::derive::{edge_eid, version_vid};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::row::EdgeRow;
use gnomon_core::scan::ScanRequest;
use gnomon_core::store::NativeStore;
use gnomon_core::OPEN_END;

fn build(root: &std::path::Path) -> NativeStore {
    let mut s = NativeStore::open(root).unwrap();
    let mut k = 0u32;
    for g in 0..6i64 {
        let tt = 100 + g;
        s.begin(tt).unwrap();
        for i in 0..3_000i64 {
            let (a, b) = (format!("n{}", i % 50), format!("n{}", (i * 7 + 1) % 50));
            let src_id = s.ensure_entity(&a, "Node").unwrap();
            let dst_id = s.ensure_entity(&b, "Node").unwrap();
            let rel = ["SENT", "RATED", "PAID"][(i % 3) as usize];
            let disc = format!("#{k}");
            k += 1;
            let vt_s = g * 10_000 + i;
            let eid = edge_eid(&a, &b, rel, &disc);
            s.stage_edge(EdgeRow {
                vid: version_vid(&eid.to_hex(), tt, vt_s),
                src_id,
                dst_id,
                rel_type: rel.into(),
                disc,
                vt_s,
                vt_e: if i % 4 == 0 { vt_s + 1 + i % 900 } else { vt_s + 1 },
                tt_s: tt,
                props: "{}".into(),
                source: "ingest".into(),
                provenance_ref: None,
            })
            .unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();
    }
    s
}

#[test]
fn concurrent_callers_on_one_handle_get_the_serial_answer() {
    let mut root = std::env::temp_dir();
    root.push(format!("gnomon-workers-pool-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let s = build(&root);
    let agg = AggregateRequest {
        as_of_tt: OPEN_END,
        t_a: 500,
        t_b: 55_000,
        rel_types: None,
        dims: vec![Dim::RelType, Dim::TimeBucket { stride: 997 }],
        aggs: vec![Agg::Count, Agg::CountDistinct { dst: true }],
        max_groups: 1 << 20,
    };
    let scan = ScanRequest {
        as_of_tt: OPEN_END,
        vt_min: Some(1_000),
        vt_max: Some(48_000),
        ..Default::default()
    };
    let answer = |s: &NativeStore| {
        let a = s.aggregate_edges(&agg).unwrap();
        let e = s.scan_edges(&scan).unwrap().cols;
        (format!("{:?} {:?}", a.keys, a.aggs), format!("{:?} {:?} {:?}", e.vt_s, e.src_id, e.vid))
    };
    // the serial reference, then every stage forced wide
    std::env::set_var("GNOMON_SCAN_THREADS", "1");
    let want = answer(&s);
    std::env::set_var("GNOMON_SCAN_THREADS", "8");
    std::env::set_var("GNOMON_PARALLEL_MIN_ROWS", "1");
    assert_eq!(answer(&s), want, "a single wide caller");
    std::thread::scope(|sc| {
        for t in 0..8 {
            let (s, want, answer) = (&s, &want, &answer);
            sc.spawn(move || {
                for round in 0..10 {
                    assert_eq!(&answer(s), want, "thread {t} round {round}");
                }
            });
        }
    });
    std::env::remove_var("GNOMON_SCAN_THREADS");
    std::env::remove_var("GNOMON_PARALLEL_MIN_ROWS");
    let _ = std::fs::remove_dir_all(&root);
}
