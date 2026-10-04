//! The aggregate query path decodes each segment once per session, not
//! once per call — and says so in counters rather than in a stopwatch.
//!
//! An earlier version opened the aggregate path's segments
//! `OpenMode::Uncached`, which made every call re-decode every segment it
//! touched; it now uses `Cached` behind a manifest-bounds prune (`aggregate.rs`, and
//! `aggregate_window_prune.rs` for the prune half). Nothing pinned the
//! *cached* half, and a later comparison read `segment_cache_stats()` as zero on a
//! 30M store and inferred a bypass from it. Residency alone cannot support
//! that inference: `entries = 0` is equally what an evicting budget, a
//! handle that was replaced underneath the reader, or a genuine bypass all
//! look like.
//!
//! So the assertions here are on cache *traffic* — `open_path_stats()`'s
//! `cached_misses` (opens that had to decode) and `decoded_bytes` — which
//! separate those cases. The four properties this file pins:
//!
//! 1. the aggregate path populates the session cache at all;
//! 2. repeated calls decode nothing further (the cache is *hit*, not merely
//!    written);
//! 3. an explicit byte budget is still respected, and a starved cache still
//!    answers — identically;
//! 4. pin-at-open still backs every one of those opens, so the
//!    cached path is not quietly re-mapping files gc may have unlinked.

use gnomon_core::aggregate::{Agg, AggValues, AggregateOut, AggregateRequest, Dim};
use gnomon_core::derive::{edge_eid, version_vid};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::row::EdgeRow;
use gnomon_core::store::NativeStore;
use gnomon_core::OPEN_END;

const ROWS_PER_GEN: i64 = 20;
const GENS: i64 = 12;
const SPAN: i64 = 1_000;

fn tmp_root(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-aggcache-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// `GENS` generations, one segment each, disjoint valid-time ranges — the
/// same fixture `aggregate_window_prune.rs` uses, so the two files describe
/// the two halves of that change over one shape.
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

fn whole_extent() -> AggregateRequest {
    AggregateRequest {
        as_of_tt: OPEN_END,
        t_a: 0,
        t_b: GENS * SPAN,
        rel_types: None,
        dims: vec![Dim::RelType],
        aggs: vec![Agg::Count],
        max_groups: 1_000,
    }
}

/// The whole answer, not just its total: a cache that changed an answer must
/// fail here, not be averaged away by a sum.
fn answer(out: &AggregateOut) -> (Vec<Vec<i64>>, Vec<String>, Vec<Option<i64>>) {
    let counts = match &out.aggs[0] {
        AggValues::Int(v) => v.clone(),
        other => panic!("count must be an integer column, got {other:?}"),
    };
    (out.keys.clone(), out.rel_names.clone(), counts)
}

/// `open_path_stats()`'s two decode counters, named at the call site.
fn decodes(s: &NativeStore) -> (u64, u64) {
    let (_hits, misses, uncached, decoded, ..) = s.open_path_stats();
    (misses + uncached, decoded)
}

#[test]
fn the_aggregate_path_decodes_each_segment_once_per_session() {
    let root = tmp_root("once");
    let store = build(&root);

    // A whole-extent window prunes nothing, so this is the worst case and the
    // shape of the registry's `series.count` / `agg.rel_bucket`: every
    // segment the manifest names is opened on every call.
    let first = store.aggregate_edges(&whole_extent()).unwrap();
    assert_eq!(first.stats.segments_pruned, 0);
    assert_eq!(first.stats.segments_opened, GENS as usize);

    let (entries, bytes, _budget, evictions) = store.segment_cache_stats();
    assert_eq!(
        entries, GENS as usize,
        "the aggregate path must leave every segment it opened in the \
         session cache; found {entries} of {GENS}"
    );
    assert!(bytes > 0, "a populated cache must account its bytes");
    assert_eq!(evictions, 0, "the default budget cannot bind on this fixture");

    // The half residency cannot show: are the entries being *read*? Decode
    // work must not grow across further calls at any window.
    let (decoded_after_first, bytes_after_first) = decodes(&store);
    for _ in 0..5 {
        let again = store.aggregate_edges(&whole_extent()).unwrap();
        assert_eq!(answer(&again), answer(&first), "the answer must not drift");
    }
    let narrow = AggregateRequest { t_a: 3 * SPAN, t_b: 3 * SPAN + 10, ..whole_extent() };
    store.aggregate_edges(&narrow).unwrap();

    assert_eq!(
        decodes(&store),
        (decoded_after_first, bytes_after_first),
        "six further aggregate calls decoded a segment again: the cache is \
         written but never hit"
    );

    // and the hits are real traffic, not an absence of calls
    let (hits, ..) = store.open_path_stats();
    assert!(
        hits >= 5 * GENS as u64,
        "five whole-extent calls over {GENS} segments must hit the cache at \
         least {} times; got {hits}",
        5 * GENS
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn every_cached_open_is_backed_by_the_pinned_generation_map() {
    let root = tmp_root("pinned");
    let store = build(&root);

    store.aggregate_edges(&whole_extent()).unwrap();
    let (.., pinned, fresh) = store.open_path_stats();
    assert_eq!(
        fresh, 0,
        "pin-at-open maps every file the generation names, so a \
         read-path open must never have to map one itself; {fresh} did"
    );
    assert!(
        pinned >= GENS as u64,
        "every segment the aggregate opened must have come from the pinned \
         map: {pinned} pinned sources for {GENS} segments"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_starved_cache_answers_identically_and_evicts_rather_than_growing() {
    let root = tmp_root("starved");
    let store = build(&root);

    let want = answer(&store.aggregate_edges(&whole_extent()).unwrap());
    let (_, full_bytes, _, _) = store.segment_cache_stats();
    assert!(full_bytes > 0);

    // One byte of budget: the insert-never-evicts-itself rule means exactly
    // one entry survives, and residency is not allowed to change an answer.
    store.set_segment_cache_budget(Some(1));
    let starved = answer(&store.aggregate_edges(&whole_extent()).unwrap());
    assert_eq!(starved, want, "the answer must not depend on cache residency");

    let (entries, bytes, budget, evictions) = store.segment_cache_stats();
    assert_eq!(budget, Some(1));
    assert_eq!(entries, 1, "a 1-byte budget keeps only the entry being inserted");
    assert!(bytes <= full_bytes);
    assert!(evictions > 0, "a starved cache must report the evictions it made");

    // Restoring the budget restores the behaviour, so the knob is a knob.
    store.set_segment_cache_budget(None);
    let (before, _) = decodes(&store);
    store.aggregate_edges(&whole_extent()).unwrap();
    let (after, _) = decodes(&store);
    store.aggregate_edges(&whole_extent()).unwrap();
    let (again, _) = decodes(&store);
    assert!(after > before, "the first call after the refill must re-decode");
    assert_eq!(again, after, "the second must not");

    let _ = std::fs::remove_dir_all(&root);
}

/// `touched_bytes` must price *reach* — the bytes a
/// query's segments hold — and must keep counting when nothing is decoded,
/// which is precisely the case `decoded_bytes` cannot report on. Plus
/// `selected_rows`, and the manifest layout the overlap fraction is computed
/// from.
#[test]
fn touched_bytes_prices_reach_while_decoded_bytes_prices_redecoding() {
    let root = tmp_root("reach");
    let store = build(&root);

    let out = store.aggregate_edges(&whole_extent()).unwrap();
    let (_h, _m, _u, decoded_1, touched_1, rows_1, ..) = store.open_path_stats();
    assert!(touched_1 >= decoded_1, "reach includes every decode");
    assert_eq!(
        rows_1,
        out.stats.rows_selected,
        "the handle counter and the per-call stat must be the same number"
    );
    assert_eq!(
        out.stats.rows_selected,
        (GENS * ROWS_PER_GEN) as u64,
        "a whole-extent window selects every stored row"
    );

    // Second call: fully cached, so decoding stops and reach does not.
    store.aggregate_edges(&whole_extent()).unwrap();
    let (_h, _m, _u, decoded_2, touched_2, rows_2, ..) = store.open_path_stats();
    assert_eq!(decoded_2, decoded_1, "a fully cached call decodes nothing");
    assert!(
        touched_2 > touched_1,
        "a fully cached call still reaches every segment it opened: \
         touched went {touched_1} -> {touched_2}"
    );
    assert_eq!(rows_2, 2 * rows_1, "and walks the same rows again");

    // A narrow window reaches less, which is the whole point of the prune.
    let narrow = AggregateRequest { t_a: 3 * SPAN, t_b: 3 * SPAN + 10, ..whole_extent() };
    let before = touched_2;
    store.aggregate_edges(&narrow).unwrap();
    let (_h, _m, _u, _d, touched_3, ..) = store.open_path_stats();
    assert!(
        touched_3 - before < (touched_2 - touched_1) / 2,
        "a one-segment window must reach far less than a whole-extent one: \
         {} vs {}",
        touched_3 - before,
        touched_2 - touched_1
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// The manifest layout an overlap fraction is computed from: one row per live
/// segment, with ordered composite key boundaries.
#[test]
fn segment_layout_reports_every_live_segment_with_ordered_key_bounds() {
    let root = tmp_root("layout");
    let store = build(&root);

    let layout = store.segment_layout();
    let edges: Vec<_> = layout.iter().filter(|r| r.0 == "edge_event").collect();
    assert_eq!(
        edges.len(),
        GENS as usize,
        "one edge segment per generation on this fixture"
    );
    assert_eq!(
        edges.iter().map(|r| u64::from(r.2)).sum::<u64>(),
        (GENS * ROWS_PER_GEN) as u64,
        "the layout's row counts must add up to the store's rows"
    );
    for r in &edges {
        assert!(r.3 <= r.4, "key_lo must not exceed key_hi in {}", r.1);
        assert!(r.5 <= r.6, "vt_min must not exceed vt_max in {}", r.1);
    }
    // this fixture gives each generation a disjoint vt range, so the segments
    // it produces must have disjoint key ranges too — the property a
    // range-prune needs and a tiered layout is not guaranteed to have
    let mut spans: Vec<_> = edges.iter().map(|r| (r.3.clone(), r.4.clone())).collect();
    spans.sort();
    for w in spans.windows(2) {
        assert!(
            w[0].1 <= w[1].0,
            "disjoint generations must produce non-overlapping key ranges: \
             {:?} then {:?}",
            w[0],
            w[1]
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}
