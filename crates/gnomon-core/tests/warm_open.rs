//! handle's first query used to build lazily, and builds the same things.
//!
//! The oracle is the lazy handle itself: the same store opened read-only with
//! the warm open off (`open_read_only_with(_, false)`) answers every read here
//! first-touch, through the old path. The warm handle must report a complete
//! warm open, hold the structures before any read (decoded segments, every
//! edge segment's adjacency index, the statistics summary, the label counts;
//! the node postings are left to the first point read, as on the lazy path) and then give the same answer to every read; and it must
//! write nothing under the store directory, as a read-only handle never does.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use gnomon_core::derive::{edge_eid, version_vid};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::row::{EdgeRow, NodeRow, RowKind};
use gnomon_core::store::{NativeStore, NeighborDir};
use gnomon_core::OPEN_END;

const GENS: i64 = 5;
const ROWS_PER_GEN: i64 = 40;
const SPAN: i64 = 1_000_000;
const LABELS: [&str; 3] = ["Person", "Post", "Forum"];
const RELS: [&str; 4] = ["KNOWS", "LIKES", "HAS_CREATOR", "REPLY_OF"];

fn tmp_root(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-warmopen-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn node_uid(i: i64) -> String {
    format!("n{}", i % 23)
}

fn edge_spec(g: i64, i: i64) -> (String, String, &'static str, String, i64, i64) {
    let src = node_uid(i);
    let dst = node_uid(i * 7 + 3);
    // one relationship rare in every segment, so the rare-row lists exist
    let rel = if i == 0 { RELS[3] } else { RELS[(i % 3) as usize] };
    let vt_s = g * SPAN + i;
    let vt_e = if i % 4 == 0 { OPEN_END } else { vt_s + 10 + i };
    (src, dst, rel, format!("#{g}-{i}"), vt_s, vt_e)
}

/// `GENS` generations, one sealed segment per lane each: nodes of three
/// labels with versions in several generations, edges of four relationship
/// types (open-ended and closed intervals), then a close of one version.
fn build(root: &Path) {
    let mut store = NativeStore::open(root).unwrap();
    for g in 0..GENS {
        let tt = 100 + g;
        store.begin(tt).unwrap();
        for i in 0..ROWS_PER_GEN {
            let (src, dst, rel, disc, vt_s, vt_e) = edge_spec(g, i);
            let src_id = store.ensure_entity(&src, "Node").unwrap();
            let dst_id = store.ensure_entity(&dst, "Node").unwrap();
            let eid = edge_eid(&src, &dst, rel, &disc);
            store
                .stage_edge(EdgeRow {
                    vid: version_vid(&eid.to_hex(), tt, vt_s),
                    src_id,
                    dst_id,
                    rel_type: rel.to_string(),
                    disc,
                    vt_s,
                    vt_e,
                    tt_s: tt,
                    props: "{}".to_string(),
                    source: "ingest".to_string(),
                    provenance_ref: None,
                })
                .unwrap();
        }
        for i in 0..9 {
            let uid = node_uid(g * 9 + i);
            let uid_id = store.ensure_entity(&uid, "Node").unwrap();
            store
                .stage_node(NodeRow {
                    vid: version_vid(&uid, tt, g * SPAN + i),
                    uid_id,
                    label: LABELS[((g + i) % 3) as usize].into(),
                    vt_s: g * SPAN + i,
                    vt_e: OPEN_END,
                    tt_s: tt,
                    props: format!("{{\"name\": \"v-{g}-{i}\"}}"),
                    source: "ingest".into(),
                    provenance_ref: None,
                })
                .unwrap();
        }
        store.commit(EventLogRef::default()).unwrap();
    }
    let (src, dst, rel, disc, vt_s, _) = edge_spec(0, 1);
    store.begin(100 + GENS).unwrap();
    store
        .close_version(RowKind::Edge, version_vid(&edge_eid(&src, &dst, rel, &disc).to_hex(), 100, vt_s), 100 + GENS)
        .unwrap();
    store.commit(EventLogRef::default()).unwrap();
}

type Tree = BTreeMap<String, (bool, u64, std::time::SystemTime)>;

fn tree(root: &Path) -> Tree {
    let mut out = Tree::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            let m = e.metadata().unwrap();
            let rel = e.path().strip_prefix(root).unwrap().display().to_string();
            out.insert(rel, (m.is_dir(), m.len(), m.modified().unwrap()));
            if m.is_dir() {
                stack.push(e.path());
            }
        }
    }
    out
}

fn store(name: &str) -> PathBuf {
    let root = tmp_root(name);
    build(&root);
    root
}

#[test]
fn the_warm_open_builds_the_lazy_structures_at_open() {
    let root = store("builds");
    let lazy = NativeStore::open_read_only_with(&root, false).unwrap();
    let warm = NativeStore::open_read_only_with(&root, true).unwrap();
    assert!(lazy.warm_phases().is_none(), "the lazy handle was not warmed");
    let ph = warm.warm_phases().expect("the warm handle was warmed");
    assert!(ph.complete, "every step of the warm open completed: {ph:?}");
    let edge_segs = warm.manifest().edge_lanes.event.len() + warm.manifest().edge_lanes.interval.len();
    let node_segs = warm.manifest().node_store.len();
    assert!(edge_segs >= GENS as usize && node_segs >= GENS as usize);
    assert_eq!(ph.segments as usize, edge_segs + node_segs);
    // before any read: every segment decoded and cached, every edge
    // segment's adjacency index built
    assert_eq!(warm.segment_cache_stats().0, edge_segs + node_segs);
    let m = warm.manifest();
    let edge_with_rows = m.edge_lanes.event.iter().chain(m.edge_lanes.interval.iter()).filter(|e| e.rows > 0).count();
    assert_eq!(warm.tcsr_stats().0, edge_with_rows);
    // the node postings are left to the first point read, as on the lazy path
    assert_eq!(warm.postings_index_stats(RowKind::Node).1, 0);
    warm.node_versions_of(&node_uid(1)).unwrap();
    assert!(warm.postings_index_stats(RowKind::Node).1 > 0);
    // and none of that on the lazy handle
    assert_eq!(lazy.segment_cache_stats().0, 0);
    assert_eq!(lazy.tcsr_stats().0, 0);
    assert_eq!(lazy.postings_index_stats(RowKind::Node).1, 0);
}

#[test]
fn a_warm_handle_answers_as_the_lazy_one() {
    let root = store("answers");
    let warm = NativeStore::open_read_only_with(&root, true).unwrap();
    let lazy = NativeStore::open_read_only_with(&root, false).unwrap();
    // the statistics `stats()` serves, and the full fold behind them
    let (ws, ls) = (warm.stats_summary().unwrap(), lazy.stats_summary().unwrap());
    assert_eq!(*ws, *ls);
    assert_eq!(*ws, gnomon_core::store::StatsSummary::of(&lazy.stats_accum().unwrap()));
    assert!(ws.n_edge_versions > 0 && ws.max_out_degree > 0 && !ws.rel_type_counts.is_empty());
    // the `match` compiler's label counts and end counts
    let wl = warm.match_label_counts().unwrap();
    assert_eq!(wl, lazy.match_label_counts().unwrap());
    assert_eq!(wl.len(), LABELS.len());
    assert_eq!(warm.match_rel_ends().unwrap(), lazy.match_rel_ends().unwrap());
    // adjacency (the index), both directions, current and past belief
    let ids: Vec<u32> = (0..warm.dict().len()).collect();
    for dir in [NeighborDir::Out, NeighborDir::In, NeighborDir::Both] {
        for as_of in [i64::MAX, 100 + GENS - 1, 102] {
            for rels in [None, Some(vec![RELS[3].to_string()])] {
                let w = warm.neighbors_batch(&ids, dir, as_of, None, None, rels.as_deref()).unwrap();
                let l = lazy.neighbors_batch(&ids, dir, as_of, None, None, rels.as_deref()).unwrap();
                assert_eq!(w, l, "{dir:?} as of {as_of} rels {rels:?}");
            }
        }
    }
    // identity point reads (the postings)
    for i in 0..23 {
        let uid = node_uid(i);
        assert_eq!(
            format!("{:?}", warm.node_versions_of(&uid).unwrap()),
            format!("{:?}", lazy.node_versions_of(&uid).unwrap()),
            "{uid}"
        );
    }
    for g in 0..GENS {
        for i in 0..ROWS_PER_GEN {
            let (src, dst, rel, disc, _, _) = edge_spec(g, i);
            let eid = edge_eid(&src, &dst, rel, &disc).to_hex();
            assert_eq!(
                format!("{:?}", warm.edge_versions_of(&eid).unwrap()),
                format!("{:?}", lazy.edge_versions_of(&eid).unwrap())
            );
        }
    }
}

#[test]
fn a_warm_open_writes_nothing() {
    let root = store("writes");
    // a legacy-shaped store: no sidecars, so every lazy build has work
    std::fs::remove_dir_all(root.join("idx")).unwrap();
    let before = tree(&root);
    {
        let warm = NativeStore::open_read_only_with(&root, true).unwrap();
        assert!(warm.warm_phases().unwrap().complete);
        let ids: Vec<u32> = (0..warm.dict().len()).collect();
        warm.neighbors_batch(&ids, NeighborDir::Both, i64::MAX, None, None, None).unwrap();
    }
    assert_eq!(tree(&root), before, "a warmed read-only handle wrote under the store");
}

#[test]
fn a_writer_is_never_warmed() {
    let root = store("writer");
    let w = NativeStore::open(&root).unwrap();
    assert!(w.warm_phases().is_none());
    assert_eq!(w.segment_cache_stats().0, 0);
}
