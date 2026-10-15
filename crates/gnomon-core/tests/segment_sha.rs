//! A segment's manifest entry carries the segment's own content
//! address, and every sidecar stamped with it is rejected on any other
//! segment — including another segment that has the **same id**.
//!
//! `SegmentEntry.sha` was written as `""` by every path that describes a
//! segment (`staging.rs::entry_for`), while the footer of every segment file
//! already carried `sha256(body)[..16]`. The identity filter and the
//! sorted name index both take their stamp from the manifest field, so
//! both were stamped `""` and the check "a stamp mismatch orphans a stale
//! sidecar" could never fire. What kept them honest was that a *published*
//! segment id is never reused — which is not the same as "a sidecar never
//! meets a segment it was not built for": a store rebuilt in a directory
//! whose `idx/` survived, an `idx/` copied from another store, and a store
//! deleted and recreated at the same path inside one process all present a
//! sidecar to a different segment under the same id.
//!
//! The fixture is two stores of identical shape — one sealed edge segment
//! and one sealed node segment per generation, so both stores allocate the
//! same ids in the same order — whose identities and names differ only by a
//! tag. A sidecar of store `a` is then a well-formed, CRC-clean file for the
//! right id and the right kind that describes the wrong bytes.

use std::path::{Path, PathBuf};

use gnomon_core::derive::{edge_eid, version_vid};
use gnomon_core::manifest::{EventLogRef, SegmentEntry};
use gnomon_core::read::NameMatchMode;
use gnomon_core::row::{EdgeRow, NodeRow, RowKind};
use gnomon_core::store::{segment_id_of, NativeStore};
use gnomon_core::vidfilter::{filter_path, VidFilter};
use gnomon_core::OPEN_END;

const GENS: i64 = 6;
const ROWS_PER_GEN: i64 = 30;
const SPAN: i64 = 1_000_000;

fn tmp_root(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-segsha-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// `(src, dst, rel, disc, vt_s)` of edge `i` of generation `g` in the store
/// tagged `tag`. Only `disc` carries the tag, so both stores share their
/// endpoint dictionary shape and differ in every edge identity.
fn edge_spec(tag: &str, g: i64, i: i64) -> (String, String, &'static str, String, i64) {
    let src = format!("n{}", i % 5);
    let dst = format!("n{}", (i + 2) % 5);
    let rel = ["SENT", "PAID"][(i % 2) as usize];
    (src, dst, rel, format!("#{tag}-{g}-{i}"), g * SPAN + i)
}

fn eid_hex(tag: &str, g: i64, i: i64) -> String {
    let (src, dst, rel, disc, _) = edge_spec(tag, g, i);
    edge_eid(&src, &dst, rel, &disc).to_hex()
}

fn node_uid(tag: &str, g: i64) -> String {
    format!("{tag}-u{g}")
}

fn node_name(tag: &str, g: i64) -> String {
    format!("{tag}-name-{g}")
}

fn edge_row(store: &mut NativeStore, tag: &str, g: i64, i: i64, tt: i64) -> EdgeRow {
    let (src, dst, rel, disc, vt_s) = edge_spec(tag, g, i);
    let src_id = store.ensure_entity(&src, "Node").unwrap();
    let dst_id = store.ensure_entity(&dst, "Node").unwrap();
    let eid = edge_eid(&src, &dst, rel, &disc);
    EdgeRow {
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
    }
}

/// Stage and commit generation `g` of the store tagged `tag`: one batch,
/// so one event-lane edge segment and one node segment.
fn commit_gen(store: &mut NativeStore, tag: &str, g: i64) {
    let tt = 100 + g;
    store.begin(tt).unwrap();
    for i in 0..ROWS_PER_GEN {
        let r = edge_row(store, tag, g, i, tt);
        store.stage_edge(r).unwrap();
    }
    let uid = node_uid(tag, g);
    let uid_id = store.ensure_entity(&uid, "Node").unwrap();
    store
        .stage_node(NodeRow {
            vid: version_vid(&uid, tt, g * SPAN),
            uid_id,
            label: "Node".into(),
            vt_s: g * SPAN,
            vt_e: OPEN_END,
            tt_s: tt,
            props: format!(r#"{{"name":"{}"}}"#, node_name(tag, g)),
            source: "ingest".into(),
            provenance_ref: None,
        })
        .unwrap();
    store.commit(EventLogRef::default()).unwrap();
}

fn build(root: &Path, tag: &str) -> NativeStore {
    let mut store = NativeStore::open(root).unwrap();
    for g in 0..GENS {
        commit_gen(&mut store, tag, g);
    }
    store
}

fn entries(s: &NativeStore, kind: RowKind) -> Vec<SegmentEntry> {
    s.manifest().segments_of(kind).cloned().collect()
}

fn all_entries(s: &NativeStore) -> Vec<SegmentEntry> {
    let mut out = entries(s, RowKind::Edge);
    out.extend(entries(s, RowKind::Node));
    out
}

/// The content address the segment file records in its own footer.
fn footer_sha(root: &Path, file: &str) -> String {
    gnomon_core::tcsr::segment_body_sha(&root.join(file)).unwrap()
}

/// Every live entry of `s` names its segment's footer `body_sha`.
fn assert_entries_carry_footer_sha(s: &NativeStore, root: &Path, stage: &str) {
    let all = all_entries(s);
    assert!(!all.is_empty(), "{stage}: the store has no segments");
    for e in all {
        let want = footer_sha(root, &e.file);
        assert_eq!(
            e.sha, want,
            "{stage}: manifest entry for {} carries sha {:?}, its footer records {:?}",
            e.file, e.sha, want
        );
    }
}

/// The two stores of the fixture share their segment ids, kind for kind —
/// the precondition every cross-store test below rests on.
fn assert_same_ids(a: &NativeStore, b: &NativeStore) {
    for kind in [RowKind::Edge, RowKind::Node] {
        let ia: Vec<String> = entries(a, kind).into_iter().map(|e| e.file).collect();
        let ib: Vec<String> = entries(b, kind).into_iter().map(|e| e.file).collect();
        assert_eq!(ia, ib, "fixture stores must allocate the same {kind:?} segment ids");
    }
}

// --------------------------------------------------------------------- //
// T1 — the field is populated, on every path that writes a segment       //
// --------------------------------------------------------------------- //

/// Seal, incremental compaction, settle and whole-store compaction each
/// describe the segments they write through `entry_for`; after each, every
/// live entry carries its segment's footer `body_sha`.
#[test]
fn every_segment_entry_carries_its_footer_sha() {
    let root = tmp_root("t1");
    let mut s = build(&root, "a");
    assert_entries_carry_footer_sha(&s, &root, "after seal");

    // closes, so settle and compaction have something to fold
    s.begin(900).unwrap();
    for g in [1, 3] {
        let (src, dst, rel, disc, vt_s) = edge_spec("a", g, 4);
        let eid = edge_eid(&src, &dst, rel, &disc);
        s.close_version(RowKind::Edge, version_vid(&eid.to_hex(), 100 + g, vt_s), 900)
            .unwrap();
    }
    s.commit(EventLogRef::default()).unwrap();
    assert_entries_carry_footer_sha(&s, &root, "after a close-only commit");

    s.compact_incremental(None).unwrap();
    assert_entries_carry_footer_sha(&s, &root, "after incremental compaction");

    let settled = s.settle(None).unwrap();
    assert!(settled.rows_rewritten > 0, "fixture must actually settle");
    assert_entries_carry_footer_sha(&s, &root, "after settle");

    // one more generation after the closes (tt 1100 > 900), then compact
    commit_gen(&mut s, "a", 1000);
    let compacted = s.compact().unwrap();
    assert!(compacted.segments_after > 0, "fixture must actually compact");
    assert_entries_carry_footer_sha(&s, &root, "after whole-store compaction");

    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}

// --------------------------------------------------------------------- //
// T2 — the identity filter                                               //
// --------------------------------------------------------------------- //

/// Copy store `a`'s identity filter files over store `b`'s, id for id, and
/// reopen `b`. Each copied file is intact and names the right id and kind;
/// it describes `a`'s bytes. It must be refused — or `b` answers from
/// filters over keys it does not hold, and every one of its identities is a
/// false negative.
fn transplanted_filters(name: &str) -> (PathBuf, PathBuf, NativeStore) {
    let ra = tmp_root(&format!("{name}-a"));
    let rb = tmp_root(&format!("{name}-b"));
    let a = build(&ra, "a");
    let b = build(&rb, "b");
    assert_same_ids(&a, &b);
    let files: Vec<String> = all_entries(&b).into_iter().map(|e| e.file).collect();
    drop(a);
    drop(b);
    let mut copied = 0;
    for f in files {
        let id = segment_id_of(&f);
        let from = filter_path(&ra, id);
        if from.exists() {
            std::fs::copy(&from, filter_path(&rb, id)).unwrap();
            copied += 1;
        }
    }
    assert!(copied > 0, "store a wrote no filters to transplant");
    let b = NativeStore::open(&rb).unwrap();
    (ra, rb, b)
}

#[test]
fn a_filter_built_for_one_segment_is_rejected_on_another_with_the_same_id_edges() {
    let (ra, rb, b) = transplanted_filters("t2-edge");
    let mut missing = Vec::new();
    for g in 0..GENS {
        for i in 0..ROWS_PER_GEN {
            let got = b.believed_edge_versions(&eid_hex("b", g, i), OPEN_END).unwrap();
            if got.len() != 1 {
                missing.push((g, i, got.len()));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {} edge identities of store b were answered wrongly through store a's \
         filters (first: {:?})",
        missing.len(),
        GENS * ROWS_PER_GEN,
        missing.first()
    );
    drop(b);
    let _ = std::fs::remove_dir_all(&ra);
    let _ = std::fs::remove_dir_all(&rb);
}

#[test]
fn a_filter_built_for_one_segment_is_rejected_on_another_with_the_same_id_nodes() {
    let (ra, rb, b) = transplanted_filters("t2-node");
    let mut missing = Vec::new();
    for g in 0..GENS {
        let got = b.believed_node_versions(&node_uid("b", g), OPEN_END).unwrap();
        if got.len() != 1 {
            missing.push((g, got.len()));
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {} node identities of store b were answered wrongly through store a's \
         filters (first: {:?})",
        missing.len(),
        GENS,
        missing.first()
    );
    drop(b);
    let _ = std::fs::remove_dir_all(&ra);
    let _ = std::fs::remove_dir_all(&rb);
}

/// The direct statement: a filter loaded under the stamp of the segment it
/// was *not* built for is refused at the parse.
#[test]
fn a_filter_does_not_load_under_another_segments_entry() {
    let ra = tmp_root("t2-load-a");
    let rb = tmp_root("t2-load-b");
    let a = build(&ra, "a");
    let b = build(&rb, "b");
    assert_same_ids(&a, &b);
    for (ea, eb) in entries(&a, RowKind::Edge).iter().zip(entries(&b, RowKind::Edge).iter()) {
        let id = segment_id_of(&ea.file);
        let p = filter_path(&ra, id);
        assert!(
            VidFilter::load(&p, id, RowKind::Edge, &ea.sha).is_some(),
            "a's filter must load under a's own entry"
        );
        assert!(
            VidFilter::load(&p, id, RowKind::Edge, &eb.sha).is_none(),
            "a's filter for {} loaded under b's entry for the same id",
            ea.file
        );
    }
    drop(a);
    drop(b);
    let _ = std::fs::remove_dir_all(&ra);
    let _ = std::fs::remove_dir_all(&rb);
}

// --------------------------------------------------------------------- //
// T3 — the sorted name index                                             //
// --------------------------------------------------------------------- //

fn uids(hits: &[(String, u8, String, String)]) -> Vec<String> {
    let mut out: Vec<String> = hits.iter().map(|h| h.0.clone()).collect();
    out.sort();
    out
}

/// Every name of store `tag_here` resolves to its own node, and no name of
/// store `tag_other` resolves to anything.
fn assert_names_answer_from(s: &NativeStore, tag_here: &str, tag_other: &str) {
    for g in 0..GENS {
        let own = s
            .lookup_name(&node_name(tag_here, g), NameMatchMode::Exact, OPEN_END, None)
            .unwrap();
        assert_eq!(
            uids(&own),
            vec![node_uid(tag_here, g)],
            "name {:?} did not resolve to its own node",
            node_name(tag_here, g)
        );
        let foreign = s
            .lookup_name(&node_name(tag_other, g), NameMatchMode::Exact, OPEN_END, None)
            .unwrap();
        assert!(
            foreign.is_empty(),
            "name {:?}, which this store never held, resolved to {:?}",
            node_name(tag_other, g),
            uids(&foreign)
        );
    }
    let prefix = s
        .lookup_name(&format!("{tag_here}-name-"), NameMatchMode::Prefix, OPEN_END, None)
        .unwrap();
    assert_eq!(prefix.len() as i64, GENS, "prefix lookup over this store's names");
}

/// An `idx/name/` file copied from another store, under the same id.
#[test]
fn a_name_index_built_for_one_segment_is_rejected_on_another_with_the_same_id() {
    let ra = tmp_root("t3-file-a");
    let rb = tmp_root("t3-file-b");
    let a = build(&ra, "a");
    let b = build(&rb, "b");
    assert_same_ids(&a, &b);
    // build a's name indexes (lazily, through the query that uses them)
    assert_names_answer_from(&a, "a", "b");
    let files: Vec<String> = entries(&b, RowKind::Node).into_iter().map(|e| e.file).collect();
    drop(a);
    drop(b);
    let mut copied = 0;
    for f in files {
        let id = segment_id_of(&f);
        let from = gnomon_core::nameidx::index_path(&ra, id);
        if from.exists() {
            let to = gnomon_core::nameidx::index_path(&rb, id);
            std::fs::create_dir_all(to.parent().unwrap()).unwrap();
            std::fs::copy(&from, &to).unwrap();
            copied += 1;
        }
    }
    assert!(copied > 0, "store a wrote no name indexes to transplant");
    let b = NativeStore::open(&rb).unwrap();
    assert_names_answer_from(&b, "b", "a");
    drop(b);
    let _ = std::fs::remove_dir_all(&ra);
    let _ = std::fs::remove_dir_all(&rb);
}

/// The process-global name-index cache: a store deleted and recreated at the
/// same path inside one process must not be answered from the first store's
/// indexes.
#[test]
fn a_store_recreated_at_the_same_path_is_not_served_a_cached_name_index() {
    let root = tmp_root("t3-cache");
    let a = build(&root, "a");
    assert_names_answer_from(&a, "a", "b");
    drop(a);
    std::fs::remove_dir_all(&root).unwrap();
    let b = build(&root, "b");
    assert_names_answer_from(&b, "b", "a");
    drop(b);
    let _ = std::fs::remove_dir_all(&root);
}

/// A name index whose stamp matches but whose contents describe another
/// segment — the shape of a legacy `""`-stamped index meeting a different
/// segment under the same id, forged here by stamping store `a`'s index with
/// store `b`'s sha. Such an index can lose candidates (that is what the stamp
/// exists to prevent), but `lookup_name` must never return a node whose name
/// does not match the query, nor index past the segment's end.
#[test]
fn a_lying_name_index_never_returns_a_node_whose_name_does_not_match() {
    let ra = tmp_root("t3-lie-a");
    let rb = tmp_root("t3-lie-b");
    let a = build(&ra, "a");
    let b = build(&rb, "b");
    assert_same_ids(&a, &b);
    for (ea, eb) in entries(&a, RowKind::Node).iter().zip(entries(&b, RowKind::Node).iter()) {
        let id = segment_id_of(&ea.file);
        let seg = a.open_segment(&ea.file).unwrap();
        let forged = gnomon_core::nameidx::build(&seg, id, &eb.sha).unwrap();
        let to = gnomon_core::nameidx::index_path(&rb, id);
        std::fs::create_dir_all(to.parent().unwrap()).unwrap();
        gnomon_core::nameidx::write(&to, &forged).unwrap();
    }
    drop(a);
    drop(b);
    let b = NativeStore::open(&rb).unwrap();
    for g in 0..GENS {
        let foreign = b
            .lookup_name(&node_name("a", g), NameMatchMode::Exact, OPEN_END, None)
            .unwrap();
        assert!(
            foreign.is_empty(),
            "name {:?}, which store b never held, resolved to {:?} through a lying index",
            node_name("a", g),
            uids(&foreign)
        );
    }
    let foreign = b.lookup_name("a-name-", NameMatchMode::Prefix, OPEN_END, None).unwrap();
    assert!(foreign.is_empty(), "prefix lookup resolved {:?}", uids(&foreign));
    drop(b);
    let _ = std::fs::remove_dir_all(&ra);
    let _ = std::fs::remove_dir_all(&rb);
}

// --------------------------------------------------------------------- //
// T4 — an unpublished id is reused after a crash                         //
// --------------------------------------------------------------------- //

const CHILD_ROOT_ENV: &str = "GNOMON_SEGSHA_CRASH_CHILD_ROOT";

/// Child half of T4: a no-op unless the parent set [`CHILD_ROOT_ENV`] and
/// `GNOMON_CRASH_POINT=after_seal`, in which case it seals one batch into the
/// store and aborts before the generation is published.
#[test]
fn crash_child_after_seal() {
    let Ok(root) = std::env::var(CHILD_ROOT_ENV) else {
        return;
    };
    let mut s = NativeStore::open(Path::new(&root)).unwrap();
    commit_gen(&mut s, "crashed", GENS);
    unreachable!("GNOMON_CRASH_POINT=after_seal should have aborted the commit");
}

/// "Ids are never reused" holds for *published* ids only: a seal that
/// crashed before its generation was published leaves `next_segment_id`
/// where it was, so the next commit writes different bytes under the same
/// file name. A sidecar left under that id — here, a filter stamped the way
/// older code stamped every filter — must not answer for the new segment.
///
/// Passes on an older engine too, because the filter is built *after*
/// publication and the commit overwrites the planted file; this test guards
/// that ordering, and makes it unnecessary once the stamp is real.
#[test]
fn an_unpublished_segment_id_is_reused_without_trusting_any_sidecar() {
    let root = tmp_root("t4");
    let s = build(&root, "a");
    let next = s.manifest().next_segment_id;
    drop(s);

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child_after_seal", "--nocapture", "--test-threads", "1"])
        .env(CHILD_ROOT_ENV, &root)
        .env("GNOMON_CRASH_POINT", "after_seal")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child must have crashed at after_seal");
    let orphan = format!("seg/{next:012}.tgs");
    assert!(root.join(&orphan).exists(), "the crashed seal left no {orphan}");

    // a stale sidecar under the id the next commit will reuse: a filter over
    // the crashed batch's keys, stamped "" as every older filter was
    let crashed_keys: Vec<u64> = (0..ROWS_PER_GEN)
        .map(|i| {
            let (src, dst, rel, disc, _) = edge_spec("crashed", GENS, i);
            edge_eid(&src, &dst, rel, &disc).hi
        })
        .collect();
    VidFilter::build(next, RowKind::Edge, "", ROWS_PER_GEN as u32, crashed_keys)
        .write(&filter_path(&root, next))
        .unwrap();

    let mut s = NativeStore::open(&root).unwrap();
    assert_eq!(s.manifest().next_segment_id, next, "the crashed generation was published");
    commit_gen(&mut s, "b", GENS);
    assert!(
        entries(&s, RowKind::Edge).iter().any(|e| e.file == orphan),
        "the next commit did not reuse the unpublished id"
    );
    drop(s);

    let s = NativeStore::open(&root).unwrap();
    for i in 0..ROWS_PER_GEN {
        assert_eq!(
            s.believed_edge_versions(&eid_hex("b", GENS, i), OPEN_END).unwrap().len(),
            1,
            "edge {i} of the commit that reused id {next} is missing"
        );
        assert!(
            s.believed_edge_versions(&eid_hex("crashed", GENS, i), OPEN_END)
                .unwrap()
                .is_empty(),
            "an edge of the crashed, never-published batch is visible"
        );
    }
    for g in 0..GENS {
        assert_eq!(
            s.believed_edge_versions(&eid_hex("a", g, 0), OPEN_END).unwrap().len(),
            1
        );
    }
    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}

// --------------------------------------------------------------------- //
// T5 — verify reports a segment that is not the one its entry names      //
// --------------------------------------------------------------------- //

/// A segment file replaced by another *valid* segment of the same name and
/// row count passes every checksum it carries. Only the manifest's content
/// address can tell; `verify` (fast mode, which already reads every segment
/// whole) must report it.
#[test]
fn verify_reports_a_segment_swapped_for_another_valid_one() {
    let ra = tmp_root("t5-a");
    let rb = tmp_root("t5-b");
    let a = build(&ra, "a");
    let b = build(&rb, "b");
    assert_same_ids(&a, &b);
    let victim = entries(&a, RowKind::Edge)[2].clone();
    drop(a);
    drop(b);
    std::fs::copy(rb.join(&victim.file), ra.join(&victim.file)).unwrap();

    let a = NativeStore::open(&ra).unwrap();
    let report = a.verify_fast().unwrap();
    assert!(
        report
            .findings
            .iter()
            .any(|f| f.kind == "content-address-mismatch" && f.path == victim.file),
        "verify did not report {} as swapped; findings: {:?}",
        victim.file,
        report.findings.iter().map(|f| (f.kind, f.path.clone())).collect::<Vec<_>>()
    );
    drop(a);
    let _ = std::fs::remove_dir_all(&ra);
    let _ = std::fs::remove_dir_all(&rb);
}
