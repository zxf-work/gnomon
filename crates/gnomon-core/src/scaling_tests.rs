//! An incidence-filtered scan over a few nodes walks each segment's
//! adjacency postings instead of testing every row. Three kinds of test:
//!
//! * **identity**: the store's edge scan ([`NativeStore::select_edges`],
//!   [`NativeStore::scan_edges`], the path the binding's `scan_edges`
//!   takes) selects and materializes exactly the same rows with the
//!   adjacency path on and off, under every other ablation switch;
//! * **work**: with the path on, a one-node filter walks posting rows
//!   (`adj_rows_walked > 0`) and examines fewer rows than the scan; off, it
//!   walks none. The falsifying value is named in each assertion;
//! * **threshold**: a filter wider than [`crate::defaults::ADJ_FILTER_MAX_IDS`]
//!   never asks for postings, and a segment whose postings are not smaller
//!   than the rows the scan would examine takes the scan.

use crate::ablation_tests::fixture::*;
use crate::designs::Designs;
use crate::scan::ScanRequest;
use crate::store::NativeStore;

/// The filtered requests every configuration must answer identically:
/// belief instants x windows x incidence filters (one node, two nodes, the
/// and-form, an empty filter, an id no row carries) x relation filter.
fn requests(f: &Fixture) -> Vec<(String, ScanRequest)> {
    let mut as_ofs = AS_OF.to_vec();
    as_ofs.push(now());
    let filters: Vec<(&str, Vec<u32>, bool)> = vec![
        ("one", vec![f.ids[3]], false),
        ("two", vec![f.ids[3], f.ids[11]], false),
        ("both", vec![f.ids[3], f.ids[11]], true),
        ("self", vec![f.ids[0]], true),
        ("empty", vec![], false),
        ("absent", vec![1_000_000], false),
    ];
    let mut out = Vec::new();
    for &as_of in &as_ofs {
        let mut windows: Vec<(Option<i64>, Option<i64>)> =
            WINDOWS.iter().map(|&(a, b)| (Some(a), Some(b))).collect();
        windows.push((None, None));
        windows.push((Some(2_500), None));
        for &(vt_min, vt_max) in &windows {
            for (label, ids, both) in &filters {
                for rels in [None, Some(vec!["A".to_string()])] {
                    out.push((
                        format!("{as_of} {vt_min:?}..{vt_max:?} {label} {rels:?}"),
                        ScanRequest {
                            as_of_tt: as_of,
                            vt_min,
                            vt_max,
                            rel_types: rels.clone(),
                            touching_ids: Some(ids.clone()),
                            touching_both: *both,
                            ..ScanRequest::default()
                        },
                    ));
                }
            }
        }
    }
    out
}

/// Rows selected (by segment id) and the materialized columns of one request.
fn answer(s: &NativeStore, req: &ScanRequest) -> String {
    let (sel, _) = s.select_edges(req).unwrap();
    let mut wide = req.clone();
    wide.columns = Some(
        ["vt_e", "src_id", "dst_id", "vid", "rel_type", "disc", "props", "seg_id", "seg_row"]
            .iter()
            .map(|c| c.to_string())
            .collect(),
    );
    let scan = s.scan_edges(&wide).unwrap();
    format!("{sel:?} {:?}", scan.cols)
}

const OFF: Designs = Designs { adjacency_filtered_scan: false, ..Designs::ALL };

#[test]
fn a_filtered_scan_answers_identically_with_the_adjacency_path_on_and_off() {
    for layout in LAYOUTS {
        let mut f = build("w30-id", layout);
        let reqs = requests(&f);
        f.s.set_designs(OFF);
        let want: Vec<String> = reqs.iter().map(|(_, r)| answer(&f.s, r)).collect();
        for (what, d) in [
            ("every design on", Designs::ALL),
            ("adjacency index off (GNOMON_TCSR=scan)", Designs { adjacency_index: false, ..Designs::ALL }),
            ("range visibility off", Designs { range_visibility: false, ..Designs::ALL }),
            ("window-first off", Designs { window_first: false, ..Designs::ALL }),
            ("segment pruning off", Designs { segment_prune: false, ..Designs::ALL }),
            ("seed expansion off", Designs { seed_expansion: false, ..Designs::ALL }),
        ] {
            f.s.set_designs(d);
            for ((name, r), w) in reqs.iter().zip(&want) {
                assert_eq!(&answer(&f.s, r), w, "{layout:?} {what}: `{name}` changed");
            }
        }
        // and the probe set is not vacuous: a tenth of the requests select rows
        let hit = want.iter().filter(|a| !a.starts_with("[]")).count();
        assert!(hit * 10 > reqs.len(), "{layout:?}: {hit} of {} requests select rows", reqs.len());
    }
}

#[test]
fn a_one_node_filter_walks_postings_and_examines_fewer_rows_than_the_scan() {
    for layout in LAYOUTS {
        let mut f = build("w30-work", layout);
        let mut walked_any = false;
        for (name, req) in requests(&f) {
            let ids = req.touching_ids.clone().unwrap();
            if ids.len() != 1 || ids[0] != f.ids[3] {
                continue;
            }
            f.s.set_designs(Designs::ALL);
            let (_, on) = f.s.select_edges(&req).unwrap();
            f.s.set_designs(OFF);
            let (_, off) = f.s.select_edges(&req).unwrap();
            // falsifier: postings walked with the switch set
            assert_eq!((off.adj_rows_walked, off.adj_segments), (0, 0), "{layout:?} `{name}`: {off:?}");
            // falsifier: more rows examined through postings than by the scan
            assert!(on.rows_examined <= off.rows_examined, "{layout:?} `{name}`: {on:?} vs {off:?}");
            if on.adj_segments > 0 {
                walked_any = true;
                // falsifier: a segment took postings and examined no fewer rows
                assert!(on.rows_examined < off.rows_examined, "{layout:?} `{name}`: {on:?} vs {off:?}");
            }
        }
        assert!(walked_any, "{layout:?}: no one-node scan took the adjacency path");
        // the whole-extent scan at current belief: entity 3 is an endpoint of
        // about 2/40 of the rows, so postings must be taken in every segment
        // that is not pruned
        f.s.set_designs(Designs::ALL);
        let req = ScanRequest::current().touching(vec![f.ids[3]]);
        let (_, st) = f.s.select_edges(&req).unwrap();
        assert_eq!(st.adj_segments, st.segments_total - st.segments_pruned, "{layout:?}: {st:?}");
        // falsifier: a fifth of the store examined for one of 40 entities
        assert!(st.rows_examined * 5 < N as u64, "{layout:?}: {st:?}");
    }
}

#[test]
fn a_filter_wider_than_the_cap_or_the_segment_takes_the_scan() {
    let cap = crate::defaults::ADJ_FILTER_MAX_IDS;
    for layout in LAYOUTS {
        let mut f = build("w30-cap", layout);
        f.s.set_designs(Designs::ALL);
        // every entity: each segment's postings cover each row twice, never
        // fewer than the rows the scan examines
        let all = ScanRequest::current().touching(f.ids.clone());
        let (_, st) = f.s.select_edges(&all).unwrap();
        assert_eq!(st.adj_segments, 0, "{layout:?}: postings taken for a filter naming every entity: {st:?}");
        // past the cap: no segment's postings are even asked for
        let mut wide: Vec<u32> = (0..=cap as u32).map(|i| 2_000_000 + i).collect();
        wide.push(f.ids[3]);
        let req = ScanRequest::current().touching(wide);
        let (_, st) = f.s.select_edges(&req).unwrap();
        assert_eq!((st.adj_rows_walked, st.adj_segments), (0, 0), "{layout:?}: {st:?}");
        let (_, st) = f.s.select_edges(&ScanRequest::current().touching(vec![f.ids[3]])).unwrap();
        assert!(st.adj_segments > 0, "{layout:?}: {st:?}");
    }
}

/// `GNOMON_TCSR=scan`: the filtered scan's postings come from scanning the
/// endpoint columns (the traversal's scan adjacency); no index is loaded,
/// built or written.
#[cfg(feature = "ingest-timers")]
#[test]
fn without_the_index_the_filtered_scan_takes_the_scanned_adjacency() {
    for layout in LAYOUTS {
        let mut f = build("w30-tcsr-scan", layout);
        f.s.set_designs(Designs { adjacency_index: false, ..Designs::ALL });
        crate::vistouch::take();
        let (_, st) = f.s.select_edges(&ScanRequest::current().touching(vec![f.ids[3]])).unwrap();
        let t = crate::vistouch::take();
        let (_, built, loaded, ..) = f.s.tcsr_stats();
        assert_eq!((built, loaded), (0, 0), "{layout:?}: an index was used");
        assert!(st.adj_segments > 0, "{layout:?}: {st:?}");
        assert!(t.adj > 0, "{layout:?}: no endpoint row scanned: {t:?}");
    }
}

#[test]
fn posting_rows_intersect_the_planned_ranges_exactly() {
    use crate::scan::intersect_rows;
    let ranges = vec![(0usize, 4usize), (6, 7), (10, 20)];
    assert_eq!(intersect_rows(&[], &ranges), (vec![], 0));
    assert_eq!(intersect_rows(&[4, 5, 7, 8, 9, 20, 30], &ranges), (vec![], 0));
    assert_eq!(
        intersect_rows(&[0, 1, 3, 6, 9, 10, 11, 12, 19], &ranges),
        (vec![(0, 2), (3, 4), (6, 7), (10, 13), (19, 20)], 8)
    );
    assert_eq!(intersect_rows(&[2, 3], &[(0, 100)]), (vec![(2, 4)], 2));
}
