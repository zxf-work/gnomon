//! A handle opened read-only writes nothing under the store directory.
//!
//! Found on the measurement host: September stores opened by read-only
//! readers gained identity-filter sidecars under `idx/vidfilter/` — the lazy
//! build path inside `fold_segment` persisted its filter with no read-only check,
//! because the engine had no notion of a read-only handle at all. The same
//! was true of the per-segment adjacency index (`tcsr_index`), the sorted
//! name index (`nameidx::load_or_build`), the `SUBDIRS` creation in `open`
//! and the genesis publish `open` performs on a directory with no `CURRENT`.
//!
//! The oracle is a recursive listing of the store directory — every file and
//! directory with its size and mtime — taken before the handle opens and
//! after it is dropped. The fixture is a legacy-shaped store: written, then
//! stripped of its whole `idx/` directory, so every lazy build has something
//! to build and `open` has a directory to (wrongly) recreate.
//!
//! The read-write control at the end is what keeps the oracle honest: the
//! same reads on a read-write handle *do* persist all three sidecars.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use gnomon_core::derive::{edge_eid, version_vid};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::read::NameMatchMode;
use gnomon_core::row::{EdgeRow, NodeRow, RowKind};
use gnomon_core::store::{NativeStore, NeighborDir};
use gnomon_core::OPEN_END;

const GENS: i64 = 6;
const ROWS_PER_GEN: i64 = 30;
const SPAN: i64 = 1_000_000;

fn tmp_root(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-w27-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn edge_spec(g: i64, i: i64) -> (String, String, &'static str, String, i64) {
    let src = format!("n{}", i % 7);
    let dst = format!("n{}", (i + 3) % 7);
    let rel = ["SENT", "RATED", "PAID"][(i % 3) as usize];
    (src, dst, rel, format!("#{g}-{i}"), g * SPAN + i)
}

fn eid_hex(g: i64, i: i64) -> String {
    let (src, dst, rel, disc, _) = edge_spec(g, i);
    edge_eid(&src, &dst, rel, &disc).to_hex()
}

/// `GENS` generations, one sealed segment per lane each, nodes carrying a
/// `name` prop so the name index has something to index; then one more
/// generation that closes a version of generation 0, so the store has a
/// close run and a second belief epoch.
fn build(root: &Path) {
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
                props: format!("{{\"name\": \"alpha-{g}\"}}"),
                source: "ingest".into(),
                provenance_ref: None,
            })
            .unwrap();
        store.commit(EventLogRef::default()).unwrap();
    }
    // a correction: close one generation-0 version at a later belief time
    let (_, _, _, _, vt_s) = edge_spec(0, 1);
    store.begin(100 + GENS).unwrap();
    store
        .close_version(RowKind::Edge, version_vid(&eid_hex(0, 1), 100, vt_s), 100 + GENS)
        .unwrap();
    store.commit(EventLogRef::default()).unwrap();
}

/// A legacy-shaped copy: the whole `idx/` directory removed.
fn legacy(name: &str) -> PathBuf {
    let root = tmp_root(name);
    build(&root);
    std::fs::remove_dir_all(root.join("idx")).unwrap();
    root
}

type Tree = BTreeMap<String, (bool, u64, std::time::SystemTime)>;

/// Every entry under `root`, the root itself as `.`, with (is_dir, len,
/// mtime). A new, removed, resized or rewritten file shows here, and so does
/// a directory whose entry list changed.
fn tree(root: &Path) -> Tree {
    fn walk(base: &Path, dir: &Path, out: &mut Tree) {
        for e in std::fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            let p = e.path();
            let md = std::fs::symlink_metadata(&p).unwrap();
            let rel = p.strip_prefix(base).unwrap().to_string_lossy().to_string();
            out.insert(rel, (md.is_dir(), md.len(), md.modified().unwrap()));
            if md.is_dir() {
                walk(base, &p, out);
            }
        }
    }
    let mut out = Tree::new();
    let md = std::fs::metadata(root).unwrap();
    out.insert(".".into(), (true, md.len(), md.modified().unwrap()));
    walk(root, root, &mut out);
    out
}

fn assert_unchanged(before: &Tree, after: &Tree, what: &str) {
    let mut diffs = Vec::new();
    for k in before.keys().chain(after.keys()) {
        let (a, b) = (before.get(k), after.get(k));
        if a != b && !diffs.iter().any(|d: &String| d.starts_with(&format!("{k}:"))) {
            diffs.push(format!("{k}: {a:?} -> {b:?}"));
        }
    }
    assert!(
        diffs.is_empty(),
        "{what} wrote under the store directory:\n  {}",
        diffs.join("\n  ")
    );
}

/// Every read the engine has a lazy sidecar behind, and the whole-store
/// reads around them: identity folds on both kinds (the lazy filter build path),
/// traversal (the adjacency index), name lookups (the name index), the
/// belief diff, the explicit index builds, verify and digest.
fn read_everything(s: &NativeStore) {
    for g in 0..GENS {
        s.believed_edge_versions(&eid_hex(g, 2), OPEN_END).unwrap();
        s.believed_node_versions(&format!("g{g}"), OPEN_END).unwrap();
    }
    s.believed_edge_versions(&eid_hex(0, 1), 100).unwrap();
    for u in 0..7u32 {
        s.neighbors_of(u, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        s.neighbors_of(u, NeighborDir::In, 101, None, None, None).unwrap();
    }
    s.lookup_name("g1", NameMatchMode::Exact, OPEN_END, None).unwrap();
    s.lookup_name("alpha", NameMatchMode::Prefix, OPEN_END, None).unwrap();
    s.lookup_name("alpha-3", NameMatchMode::Exact, OPEN_END, None).unwrap();
    s.diff_belief(100, OPEN_END, -(1 << 60), 1 << 60, 1000).unwrap();
    s.all_edge_versions().unwrap();
    s.all_node_versions().unwrap();
    s.segment_tt_profile().unwrap();
    s.verify().unwrap();
    s.verify_full().unwrap();
    s.build_tcsr_indexes().unwrap();
    s.build_vid_filters().unwrap();
}

#[test]
fn a_read_only_handle_writes_nothing_on_a_legacy_store() {
    let root = legacy("ro-legacy");
    let before = tree(&root);
    {
        let s = NativeStore::open_read_only(&root).unwrap();
        assert!(s.is_read_only());
        read_everything(&s);
        // the lazy paths ran: an unchanged tree is the guard, not a
        // workload that never reached a build
        let vf = s.vidfilter_stats();
        assert!(vf.2 > 0, "no lazy filter was built");
        let tc = s.tcsr_stats();
        assert!(tc.1 > 0, "no adjacency index was built");
        // and what was built is held on the heap and charged there: there
        // is no file to map
        let (current, _) = s.memory_accounting();
        let heap = current
            .iter()
            .find(|(n, _)| *n == "vid_filter_heap")
            .map(|(_, v)| *v)
            .unwrap();
        assert!(heap > 0, "in-memory filters are accounted as heap");
    }
    assert_unchanged(&before, &tree(&root), "open_read_only on a legacy store");
    assert!(!root.join("idx").exists(), "open_read_only recreated idx/");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn the_in_memory_sidecars_give_the_answers_the_persisted_ones_do() {
    let ro_root = legacy("ro-answers");
    let rw_root = tmp_root("rw-answers");
    build(&rw_root);
    let ro = NativeStore::open_read_only(&ro_root).unwrap();
    let rw = NativeStore::open(&rw_root).unwrap();
    rw.build_vid_filters().unwrap();
    rw.build_tcsr_indexes().unwrap();
    for g in 0..GENS {
        for i in 0..ROWS_PER_GEN {
            let e = eid_hex(g, i);
            let a: Vec<_> = ro
                .believed_edge_versions(&e, OPEN_END)
                .unwrap()
                .into_iter()
                .map(|v| v.vid)
                .collect();
            let b: Vec<_> = rw
                .believed_edge_versions(&e, OPEN_END)
                .unwrap()
                .into_iter()
                .map(|v| v.vid)
                .collect();
            assert_eq!(a, b, "edge {g}/{i}");
        }
    }
    for u in 0..7u32 {
        let a = ro.neighbors_of(u, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        let b = rw.neighbors_of(u, NeighborDir::Out, OPEN_END, None, None, None).unwrap();
        assert_eq!((a.nbr, a.vt_s), (b.nbr, b.vt_s), "vertex {u}");
    }
    let a = ro.lookup_name("alpha", NameMatchMode::Prefix, OPEN_END, None).unwrap();
    let b = rw.lookup_name("alpha", NameMatchMode::Prefix, OPEN_END, None).unwrap();
    assert_eq!(a, b);
    drop((ro, rw));
    let _ = std::fs::remove_dir_all(&ro_root);
    let _ = std::fs::remove_dir_all(&rw_root);
}

#[test]
fn a_read_only_handle_refuses_every_writer_entry_point_before_writing() {
    let root = legacy("ro-refuse");
    let before = tree(&root);
    {
        let mut s = NativeStore::open_read_only(&root).unwrap();
        let msgs = vec![
            ("begin", s.begin(1 << 40).err()),
            ("compact", s.compact().err()),
            ("compact_incremental", s.compact_incremental(None).err()),
            ("compact_current_only", s.compact_current_only().err()),
            ("settle", s.settle(None).err()),
            ("gc", s.gc(1).err()),
            ("upgrade_manifests", s.upgrade_manifests().err()),
        ];
        for (name, e) in msgs {
            let e = e.unwrap_or_else(|| panic!("{name} was not refused"));
            assert!(e.to_string().contains("read-only"), "{name}: {e}");
        }
    }
    assert_unchanged(&before, &tree(&root), "refused writer entry points");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_read_only_open_of_no_store_creates_nothing() {
    let missing = tmp_root("ro-missing");
    assert!(NativeStore::open_read_only(&missing).is_err());
    assert!(!missing.exists(), "a read-only open created the directory");

    let empty = tmp_root("ro-empty");
    std::fs::create_dir_all(&empty).unwrap();
    let before = tree(&empty);
    assert!(NativeStore::open_read_only(&empty).is_err());
    assert_unchanged(&before, &tree(&empty), "read-only open of an empty directory");
    let _ = std::fs::remove_dir_all(&empty);
}

#[test]
fn a_read_write_handle_still_persists_every_lazy_sidecar() {
    let root = legacy("rw-control");
    let before = tree(&root);
    {
        let s = NativeStore::open(&root).unwrap();
        assert!(!s.is_read_only());
        read_everything(&s);
        assert!(s.vidfilter_stats().2 > 0);
    }
    let after = tree(&root);
    assert_ne!(before, after, "a read-write handle persisted nothing");
    for sub in ["vidfilter", "tcsr", "name"] {
        let dir = root.join("idx").join(sub);
        let n = std::fs::read_dir(&dir).map(|d| d.count()).unwrap_or(0);
        assert!(n > 0, "no {sub} sidecar persisted under {}", dir.display());
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// The name index's cache is process-global, keyed by store root. An index a
/// reader built (and, being a reader, did not write) must not be served to a
/// later read-write handle in the same process as a hit that skips the
/// persist that handle would otherwise have done.
#[test]
fn a_name_index_a_reader_built_is_persisted_by_the_next_writer_in_the_process() {
    let root = legacy("ro-then-rw-name");
    {
        let ro = NativeStore::open_read_only(&root).unwrap();
        ro.lookup_name("alpha", NameMatchMode::Prefix, OPEN_END, None).unwrap();
    }
    assert!(!root.join("idx").exists(), "the reader wrote");
    let node_segments = {
        let rw = NativeStore::open(&root).unwrap();
        rw.lookup_name("alpha", NameMatchMode::Prefix, OPEN_END, None).unwrap();
        rw.manifest().node_store.len()
    };
    let on_disk = std::fs::read_dir(root.join("idx").join("name"))
        .map(|d| d.count())
        .unwrap_or(0);
    assert!(node_segments > 0);
    assert_eq!(
        on_disk, node_segments,
        "the writer did not persist the name index the reader had cached"
    );
    let _ = std::fs::remove_dir_all(&root);
}
