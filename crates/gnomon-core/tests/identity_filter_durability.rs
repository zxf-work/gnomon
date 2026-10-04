//! The identity-filter sidecar is written without an fsync. What
//! makes that safe is that every state a crash can leave the file in —
//! missing, empty, torn at any length, or with damaged bytes — is detected
//! when the filter is loaded, read as "no filter", answered correctly by the
//! unfiltered path, and rebuilt by the fold that answer needs. This file
//! holds the store to all four.

use gnomon_core::derive::{edge_eid, version_vid};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::row::{EdgeRow, NodeRow};
use gnomon_core::store::NativeStore;
use gnomon_core::vidfilter::{filter_dir, filter_path, VidFilter};
use gnomon_core::OPEN_END;

const GENS: i64 = 6;
const ROWS_PER_GEN: i64 = 40;
const SPAN: i64 = 1_000_000;

fn tmp_root(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-w22r3-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn edge_spec(g: i64, i: i64) -> (String, String, &'static str, String, i64) {
    (
        format!("n{}", i % 7),
        format!("n{}", (i + 3) % 7),
        ["SENT", "RATED", "PAID"][(i % 3) as usize],
        format!("#{g}-{i}"),
        g * SPAN + i,
    )
}

fn eid_hex(g: i64, i: i64) -> String {
    let (src, dst, rel, disc, _) = edge_spec(g, i);
    edge_eid(&src, &dst, rel, &disc).to_hex()
}

/// One sealed segment per lane per generation, every edge its own identity.
fn build(root: &std::path::Path) {
    let mut store = NativeStore::open(root).unwrap();
    for g in 0..GENS {
        let tt = 100 + g;
        store.begin(tt).unwrap();
        for i in 0..ROWS_PER_GEN {
            let (src, dst, rel, disc, vt_s) = edge_spec(g, i);
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
                    vt_e: vt_s + 1,
                    tt_s: tt,
                    props: "{}".to_string(),
                    source: "ingest".to_string(),
                    provenance_ref: None,
                })
                .unwrap();
        }
        let uid = format!("g{g}");
        let uid_id = store.ensure_entity(&uid, "Node").unwrap();
        store
            .stage_node(NodeRow {
                vid: version_vid(&uid, tt, g * SPAN),
                uid_id,
                label: "Node".into(),
                vt_s: g * SPAN,
                vt_e: OPEN_END,
                tt_s: tt,
                props: "{}".into(),
                source: "ingest".into(),
                provenance_ref: None,
            })
            .unwrap();
        store.commit(EventLogRef::default()).unwrap();
    }
}

fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    let _ = std::fs::remove_dir_all(to);
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let e = entry.unwrap();
        let dst = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_tree(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), &dst).unwrap();
        }
    }
}

/// Every answer the gate compares: present identities at several `as_of`,
/// identities the store never held, and every node uid.
type Answers = (
    Vec<Vec<gnomon_core::read::EdgeVersionOut>>,
    Vec<Vec<gnomon_core::read::NodeVersionOut>>,
);

fn answers(s: &NativeStore) -> Answers {
    let mut edges = Vec::new();
    for g in 0..GENS {
        for i in [0i64, 1, ROWS_PER_GEN / 2, ROWS_PER_GEN - 1] {
            for as_of in [OPEN_END, 100 + g, 99] {
                edges.push(s.believed_edge_versions(&eid_hex(g, i), as_of).unwrap());
            }
        }
    }
    for k in 0..16u64 {
        let eid = edge_eid("absent", "absent", "NONE", &format!("#{k}")).to_hex();
        edges.push(s.believed_edge_versions(&eid, OPEN_END).unwrap());
    }
    let nodes = (0..GENS)
        .map(|g| s.believed_node_versions(&format!("g{g}"), OPEN_END).unwrap())
        .collect();
    (edges, nodes)
}

/// The file names under `idx/vidfilter/` that belong to edge segments of the
/// live generation, as `(seg_id, path)`.
fn edge_filters(s: &NativeStore, root: &std::path::Path) -> Vec<(u64, std::path::PathBuf)> {
    let m = s.manifest();
    m.edge_lanes
        .event
        .iter()
        .chain(m.edge_lanes.interval.iter())
        .map(|e| {
            let stem = std::path::Path::new(&e.file)
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .to_string();
            let id: u64 = stem.parse().unwrap();
            (id, filter_path(root, id))
        })
        .collect()
}

#[test]
fn a_missing_empty_torn_or_damaged_filter_is_detected_answered_and_rebuilt() {
    let pristine = tmp_root("pristine");
    build(&pristine);
    let s = NativeStore::open(&pristine).unwrap();
    let expected = answers(&s);
    assert!(expected.0.iter().any(|v| !v.is_empty()), "the fixture holds present identities");
    let (victim_id, victim_path) = edge_filters(&s, &pristine)[1].clone();
    let good = std::fs::read(&victim_path).unwrap();
    drop(s);
    assert!(good.len() > 64, "the victim filter has an extent to damage");

    // every state a crash without an fsync can leave behind
    let mut damaged: Vec<(&str, Option<Vec<u8>>)> = vec![
        ("missing", None),
        ("empty", Some(Vec::new())),
        ("torn-header", Some(good[..12].to_vec())),
        ("torn-half", Some(good[..good.len() / 2].to_vec())),
        ("torn-last-byte", Some(good[..good.len() - 1].to_vec())),
        ("zero-filled", Some(vec![0u8; good.len()])),
    ];
    // a damaged byte inside the extent: only the CRC can see it
    let mut flipped = good.clone();
    let mid = good.len() - 12 - (good.len() - 12) / 4;
    flipped[mid] ^= 0x5a;
    damaged.push(("flipped-extent-byte", Some(flipped)));

    for (what, bytes) in damaged {
        let root = tmp_root(what);
        copy_tree(&pristine, &root);
        let path = filter_path(&root, victim_id);
        match &bytes {
            None => std::fs::remove_file(&path).unwrap(),
            Some(b) => std::fs::write(&path, b).unwrap(),
        }
        if let Some(b) = &bytes {
            // detected at load: whatever the stamp, these bytes are refused
            assert!(
                VidFilter::from_bytes(b, victim_id, gnomon_core::row::RowKind::Edge, "").is_none(),
                "{what}: damaged bytes were accepted"
            );
        }

        let s = NativeStore::open(&root).unwrap();
        // answered correctly meanwhile, by the unfiltered path
        assert_eq!(answers(&s), expected, "{what}: an answer changed");
        let st = s.vidfilter_stats();
        assert_eq!(st.2, 1, "{what}: exactly the damaged filter was rebuilt lazily");
        drop(s);
        // and rebuilt to the very bytes it should have had
        assert_eq!(std::fs::read(&path).unwrap(), good, "{what}: rebuilt filter differs");

        // a fresh handle now loads it and rebuilds nothing
        let s = NativeStore::open(&root).unwrap();
        assert_eq!(answers(&s), expected, "{what}: answers after the rebuild");
        assert_eq!(s.vidfilter_stats().2, 0, "{what}: nothing left to rebuild");
        drop(s);
        let _ = std::fs::remove_dir_all(&root);
    }
    // no stray temp files were left by the rebuilds
    assert!(std::fs::read_dir(filter_dir(&pristine))
        .unwrap()
        .all(|e| e.unwrap().file_name().to_string_lossy().ends_with(".tvf")));
    let _ = std::fs::remove_dir_all(&pristine);
}
