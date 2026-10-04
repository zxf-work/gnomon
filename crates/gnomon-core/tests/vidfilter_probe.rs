//! The identity filter is consulted before a fold, and changes what a
//! correction *costs* without changing any answer it gives.
//!
//! `vidfilter.rs`'s own tests pin the structure — exact-negative over
//! adversarial key sets, deterministic bytes, every load failure a rebuild.
//! These are the store-level gates: that the filtered probe and the
//! unfiltered one return the same `(file, row)` sets, that the digest does
//! not move, that a correction folds one segment instead of the store, that
//! a false positive costs exactly one fold and nothing else, that the map is
//! swept by gc, and that a ceiling of one byte changes memory and not
//! answers.
//!
//! **Why a fixture of many one-segment generations.** The mechanism is
//! `folds ÷ probes`, and it is only visible when the store has more than one
//! live segment: at `S = 1` a filter and no filter fold the same segment. So
//! each generation here commits one batch, which seals one segment per lane,
//! and each generation's edges carry identities that appear in no other —
//! which is also the corpus's own shape (`build_synth_store.py` stamps
//! `disc = "#i"`, so every bulk event is its own identity).

use gnomon_core::derive::{edge_eid, version_vid};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::row::{EdgeRow, NodeRow, RowKind};
use gnomon_core::store::NativeStore;
use gnomon_core::vidfilter::{filter_dir, filter_path, VidFilter};
use gnomon_core::OPEN_END;

const GENS: i64 = 8;
const ROWS_PER_GEN: i64 = 40;
const SPAN: i64 = 1_000_000;

fn tmp_root(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-w19-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// `(src uid, dst uid, rel, disc, vt_s)` of the `i`th edge of generation `g`
/// — a pure function, so a test can ask for an identity without holding the
/// store that wrote it.
fn edge_spec(g: i64, i: i64) -> (String, String, &'static str, String, i64) {
    let src = format!("n{}", i % 7);
    let dst = format!("n{}", (i + 3) % 7);
    let rel = ["SENT", "RATED", "PAID"][(i % 3) as usize];
    let disc = format!("#{g}-{i}");
    (src, dst, rel, disc, g * SPAN + i)
}

/// The identity key the probe and the filter both use: `eid.hi`.
fn key_of(g: i64, i: i64) -> u64 {
    let (src, dst, rel, disc, _) = edge_spec(g, i);
    edge_eid(&src, &dst, rel, &disc).hi
}

fn eid_hex(g: i64, i: i64) -> String {
    let (src, dst, rel, disc, _) = edge_spec(g, i);
    edge_eid(&src, &dst, rel, &disc).to_hex()
}

/// A store of `GENS` generations, one sealed segment per lane per
/// generation, every edge its own identity.
fn build(root: &std::path::Path) -> NativeStore {
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
        // one node version per generation, so the node lane has segments of
        // its own and `RowKind::Node` is exercised on the same rules
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
    store
}

fn filter_files(root: &std::path::Path) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(filter_dir(root))
        .map(|d| {
            d.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// `folds` for one row kind — the quantity the mechanism gate divides by
/// `probes`.
fn folds(s: &NativeStore, kind: RowKind) -> u64 {
    s.postings_index_stats(kind).6
}

fn probes(s: &NativeStore, kind: RowKind) -> u64 {
    s.filter_probe_stats(kind).0
}

// --------------------------------------------------------------------- //
// the seal-time build                                                    //
// --------------------------------------------------------------------- //

#[test]
fn every_sealed_segment_gets_a_filter_before_the_first_probe() {
    let root = tmp_root("seal-build");
    let s = build(&root);
    // one file per segment of both lanes, and no probe has run yet: the
    // whole point of the eager build is that the *first* correction is
    // already filtered, because the first correction is the entire cost the
    // lane exists to remove
    let live = s.manifest().edge_lanes.event.len()
        + s.manifest().edge_lanes.interval.len()
        + s.manifest().node_store.len();
    assert_eq!(filter_files(&root).len(), live, "{:?}", filter_files(&root));
    assert_eq!(probes(&s, RowKind::Edge), 0, "no probe has run yet");
    let st = s.vidfilter_stats();
    assert_eq!(st.1 as usize, live, "every filter was built, not loaded");
    assert_eq!(st.2, 0, "the lazy build path did not fire");
    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}

// --------------------------------------------------------------------- //
// the store-level gate: the same answers                                 //
// --------------------------------------------------------------------- //

/// The gate. For a store with filters and the same store with
/// `idx/vidfilter/` removed, every identity probe — current belief and
/// historical `as_of`, present identities and absent ones, both row kinds —
/// returns identical results.
#[test]
fn filtered_locate_equals_unfiltered_locate() {
    let with_root = tmp_root("gate-with");
    let without_root = tmp_root("gate-without");
    let s = build(&with_root);
    drop(s);
    // a byte-identical copy of the store, then the filters removed from it:
    // the same-source control the A/B protocol uses, small
    copy_tree(&with_root, &without_root);
    let _ = std::fs::remove_dir_all(filter_dir(&without_root));
    assert!(filter_files(&without_root).is_empty());

    let a = NativeStore::open(&with_root).unwrap();
    let b = NativeStore::open(&without_root).unwrap();

    let mut checked = 0;
    for g in 0..GENS {
        for i in [0i64, 1, ROWS_PER_GEN / 2, ROWS_PER_GEN - 1] {
            let eid = eid_hex(g, i);
            for as_of in [OPEN_END, 100 + g, 100 + GENS - 1, 99] {
                let va = a.believed_edge_versions(&eid, as_of).unwrap();
                let vb = b.believed_edge_versions(&eid, as_of).unwrap();
                assert_eq!(va, vb, "eid {eid} at as_of {as_of}");
                checked += 1;
            }
        }
        let uid = format!("g{g}");
        assert_eq!(
            a.believed_node_versions(&uid, OPEN_END).unwrap(),
            b.believed_node_versions(&uid, OPEN_END).unwrap()
        );
    }
    assert!(checked >= 100, "the gate checked only {checked} probes");

    // identities the store has never held: the filter's whole job, and the
    // case where a bug would show as a *missing* answer rather than a wrong
    // one — so assert both sides are empty rather than merely equal
    for k in 0..64u64 {
        let eid = edge_eid("absent", "absent", "NONE", &format!("#{k}")).to_hex();
        let va = a.believed_edge_versions(&eid, OPEN_END).unwrap();
        assert!(va.is_empty());
        assert_eq!(va, b.believed_edge_versions(&eid, OPEN_END).unwrap());
    }
    // and a uid that is not in the store
    assert!(a.believed_node_versions("nope", OPEN_END).unwrap().is_empty());

    drop((a, b));
    let _ = std::fs::remove_dir_all(&with_root);
    let _ = std::fs::remove_dir_all(&without_root);
}

/// `content_digest` and `store_digest` are computed over row *values*; a
/// sidecar holds no row. Asserted rather than argued, because it is the
/// claim the whole A/B rests on.
#[test]
fn digest_is_unchanged_by_building_filters() {
    let root = tmp_root("digest");
    let s = build(&root);
    let before_store = s.manifest().manifest_sha.clone();
    let before_content = s.content_digest(None).unwrap().digest.clone();
    drop(s);

    // remove the filters, reopen, rebuild them eagerly
    let _ = std::fs::remove_dir_all(filter_dir(&root));
    let s = NativeStore::open(&root).unwrap();
    let (segments, built, loaded) = s.build_vid_filters().unwrap();
    assert!(segments > 1 && built as usize == segments && loaded == 0);
    assert_eq!(s.manifest().manifest_sha, before_store, "store digest moved");
    assert_eq!(
        s.content_digest(None).unwrap().digest,
        before_content,
        "content digest moved"
    );
    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}

// --------------------------------------------------------------------- //
// the mechanism                                                          //
// --------------------------------------------------------------------- //

/// The mechanism gate, at unit scale: a correction's identity probe folds
/// **one** segment, not the store.
///
/// Stated as `folds ÷ probes ≤ 1.1`, which is the same inequality the Arm M
/// pre-registration scores on the development machine — here with `S` in the single digits
/// rather than the hundreds, so it pins the shape and not the magnitude.
#[test]
fn a_correction_folds_one_segment_not_the_store() {
    let with_root = tmp_root("mech-with");
    let without_root = tmp_root("mech-without");
    let s = build(&with_root);
    let segments = s.manifest().edge_lanes.event.len() + s.manifest().edge_lanes.interval.len();
    assert!(segments >= 4, "the fixture needs S > 1 to say anything");
    drop(s);
    copy_tree(&with_root, &without_root);
    let _ = std::fs::remove_dir_all(filter_dir(&without_root));

    // **One** correction, on a fresh handle each side. This is the whole
    // measurement: at `P = 1` the unfiltered configuration folds every live segment
    // and the filtered configuration folds the one that holds the identity. Running
    // several corrections first would hide it — the unfiltered configuration's
    // postings stay resident after its first probe, so folds stop at `S`
    // however many corrections follow, which is exactly why one has to
    // measure the *first* correction to see the ramp at all.
    let eid = eid_hex(GENS / 2, 3);
    let a = NativeStore::open(&with_root).unwrap();
    let b = NativeStore::open(&without_root).unwrap();
    let va = a.believed_edge_versions(&eid, OPEN_END).unwrap();
    assert_eq!(va, b.believed_edge_versions(&eid, OPEN_END).unwrap());
    assert_eq!(va.len(), 1, "fixture: one version per identity");

    let (fa, pa) = (folds(&a, RowKind::Edge), probes(&a, RowKind::Edge));
    let fb = folds(&b, RowKind::Edge);
    assert_eq!(pa, segments as u64, "every segment is asked, once each");
    assert_eq!(fa, 1, "the filtered probe folded {fa} segments, not one");
    assert_eq!(fb, segments as u64, "the unfiltered probe folds the store");

    // and the gate as the pre-registration states it, over a burst: `folds
    // / probes <= 1.1`, with `probes` the corrections rather than the
    // per-segment filter lookups
    let mut corrections = 1u64;
    for g in 0..GENS {
        a.believed_edge_versions(&eid_hex(g, 11), OPEN_END).unwrap();
        corrections += 1;
    }
    let fa = folds(&a, RowKind::Edge);
    assert!(
        (fa as f64) <= 1.1 * corrections as f64,
        "folds/probes = {fa}/{corrections} exceeds the 1.1 gate"
    );

    // epsilon in situ: at 2^-16 over a run this small there should be no
    // false positive at all, so a non-zero count is a construction bug
    let (probed, positives, negatives, empty) = a.filter_probe_stats(RowKind::Edge);
    assert_eq!(positives + negatives, probed);
    assert_eq!(empty, 0, "a false positive at unit scale is a construction bug");

    drop((a, b));
    let _ = std::fs::remove_dir_all(&with_root);
    let _ = std::fs::remove_dir_all(&without_root);

}

/// A false positive costs one extra fold and **nothing else**.
///
/// Forced deterministically rather than waited for: one segment's filter is
/// rebuilt over the *union* of two segments' keys and written under that
/// segment's own `body_sha`, so it is a valid filter for it that happens to
/// admit keys it does not hold. That is exactly what a false positive is.
#[test]
fn a_false_positive_costs_a_fold_and_nothing_else() {
    let root = tmp_root("false-positive");
    let s = build(&root);
    let files: Vec<(String, String)> = s
        .manifest()
        .segments_of(RowKind::Edge)
        .map(|e| (e.file.clone(), e.sha.clone()))
        .collect();
    // the fixture commits one generation per batch and every edge is
    // instantaneous, so generation `g`'s edges are the `g`th edge segment
    assert!(files.len() >= 2);
    drop(s);

    // rebuild the *first* segment's filter over the union of its own keys
    // and the second segment's, stamped with its own `body_sha`. It is a
    // perfectly valid filter for that segment which happens to admit keys it
    // does not hold — which is precisely what a false positive is, forced
    // rather than waited for.
    let (victim, sha) = files[0].clone();
    let id = gnomon_core::store::segment_id_of(&victim);
    let keys: Vec<u64> = (0..ROWS_PER_GEN)
        .flat_map(|i| [key_of(0, i), key_of(1, i)])
        .collect();
    let rows = keys.len() as u32;
    VidFilter::build(id, RowKind::Edge, &sha, rows, keys)
        .write(&filter_path(&root, id))
        .unwrap();

    // an identity of generation 1, which lives in the *second* segment
    let s = NativeStore::open(&root).unwrap();
    let eid = eid_hex(1, 3);
    let got = s.believed_edge_versions(&eid, OPEN_END).unwrap();
    assert_eq!(got.len(), 1, "the answer is unchanged by a false positive");
    assert_eq!(got[0].eid, eid);
    let (_, positives, _, empty) = s.filter_probe_stats(RowKind::Edge);
    assert_eq!(positives, 2, "the doctored segment answered positive too");
    assert_eq!(
        empty, 1,
        "exactly one positive held no row of the key — the false positive"
    );
    assert_eq!(
        folds(&s, RowKind::Edge),
        2,
        "one true fold plus exactly one extra, and nothing else"
    );
    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}

// --------------------------------------------------------------------- //
// residency: the ceiling and the gc sweep                                //
// --------------------------------------------------------------------- //

/// A one-byte ceiling changes memory, never answers — the `can_hold` idiom,
/// mirroring `a_one_byte_postings_budget_gives_identical_answers_to_an_
/// unbounded_one`.
#[test]
fn a_one_byte_filter_budget_gives_identical_answers() {
    let root = tmp_root("one-byte");
    let s = build(&root);
    drop(s);
    let unbounded = NativeStore::open(&root).unwrap();
    let tiny = NativeStore::open(&root).unwrap();
    tiny.set_vidfilter_budget(Some(1));
    for g in 0..GENS {
        for i in [0i64, 7, ROWS_PER_GEN - 1] {
            let eid = eid_hex(g, i);
            assert_eq!(
                unbounded.believed_edge_versions(&eid, OPEN_END).unwrap(),
                tiny.believed_edge_versions(&eid, OPEN_END).unwrap()
            );
        }
    }
    let st = tiny.vidfilter_stats();
    assert_eq!(st.0, 0, "nothing is resident under a one-byte ceiling");
    assert_eq!(st.4, 0, "and nothing is accounted");
    assert!(st.7 > 0, "every filter was mapped, probed and dropped");
    // the probes still happened, and still filtered
    assert!(probes(&tiny, RowKind::Edge) > 0);
    assert!(tiny.filter_probe_stats(RowKind::Edge).2 > 0, "and answered no");
    drop((unbounded, tiny));
    let _ = std::fs::remove_dir_all(&root);
}

/// gc's pass 4 is generic over `idx/*`, so `.tvf` is swept for free — and a
/// `.tvf.tmpNNN` leftover is not, because its stem does not parse as a
/// segment id. Both halves asserted, since an over-eager sweep and an
/// under-eager one fail in opposite directions.
#[test]
fn orphan_filters_are_swept_and_tmp_files_are_not() {
    let root = tmp_root("gc-sweep");
    let s = build(&root);
    drop(s);

    // an orphan: a filter for a segment id nothing names
    let orphan = filter_path(&root, 999_999);
    std::fs::write(&orphan, b"whatever").unwrap();
    let tmp = filter_dir(&root).join("000000999998.tvf.tmp4242");
    std::fs::write(&tmp, b"a crashed write").unwrap();

    let mut s = NativeStore::open(&root).unwrap();
    let live_before = filter_files(&root).len();
    let report = s.gc(1).unwrap();
    assert!(!orphan.exists(), "the orphan filter survived gc");
    assert!(tmp.exists(), "a tmp leftover must not parse as a segment id");
    assert!(report.index_files_removed >= 1);
    // the filters of live segments are untouched
    assert_eq!(filter_files(&root).len(), live_before - 1);
    // and every answer still holds
    for g in 0..GENS {
        let eid = eid_hex(g, 2);
        assert_eq!(s.believed_edge_versions(&eid, OPEN_END).unwrap().len(), 1);
    }
    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}

/// The map is per handle and is swept with the segments it names — the
/// `nameidx::CACHE` shape this deliberately is not.
#[test]
fn the_filter_map_is_swept_by_gc() {
    let root = tmp_root("map-sweep");
    let mut s = build(&root);
    for g in 0..GENS {
        s.believed_edge_versions(&eid_hex(g, 1), OPEN_END).unwrap();
    }
    let resident_before = s.vidfilter_stats().0;
    let bytes_before = s.vidfilter_stats().4;
    assert!(resident_before > 1 && bytes_before > 0);

    // a whole-store compaction replaces every segment with a new id, so
    // every entry in the map names a segment gc then removes
    s.compact().unwrap();
    s.gc(1).unwrap();
    let st = s.vidfilter_stats();
    assert!(
        st.0 <= s.manifest().segments_of(RowKind::Edge).count()
            + s.manifest().segments_of(RowKind::Node).count(),
        "the map still holds entries for segments gc removed: {} resident",
        st.0
    );
    // and the accounted bytes went with them rather than accumulating
    for g in 0..GENS {
        assert_eq!(
            s.believed_edge_versions(&eid_hex(g, 1), OPEN_END).unwrap().len(),
            1
        );
    }
    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}

/// Compaction writes new segments with new `body_sha`s, so the old filters
/// are orphans and the new segments get their own — through `install`, the
/// single publish point settle shares.
#[test]
fn compaction_rewrites_the_filters_with_the_segments() {
    let root = tmp_root("compaction");
    let mut s = build(&root);
    let before = filter_files(&root);
    s.compact().unwrap();
    let after = filter_files(&root);
    assert_ne!(before, after, "compaction produced no new filters");
    for kind in [RowKind::Edge, RowKind::Node] {
        for e in s.manifest().segments_of(kind) {
            let id = gnomon_core::store::segment_id_of(&e.file);
            let p = filter_path(&root, id);
            assert!(p.exists(), "no filter for compacted segment {}", e.file);
            assert!(
                VidFilter::load(&p, id, kind, &e.sha).is_some(),
                "the filter of {} is not stamped with its segment",
                e.file
            );
        }
    }
    for g in 0..GENS {
        assert_eq!(
            s.believed_edge_versions(&eid_hex(g, 5), OPEN_END).unwrap().len(),
            1
        );
    }
    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}

/// The legacy-store build path: a store with no `idx/vidfilter/` answers exactly
/// as before, writes filters on the way out of the folds it had to do
/// anyway, and is filtered from the second probe on.
///
/// Measured and reported — `lazy_built` — and relied on for nothing: for the
/// case the filter exists for, the first use is the entire cost.
#[test]
fn a_legacy_store_gains_filters_on_the_first_probe_and_is_filtered_after() {
    let root = tmp_root("legacy");
    let s = build(&root);
    drop(s);
    let _ = std::fs::remove_dir_all(filter_dir(&root));
    assert!(filter_files(&root).is_empty());

    let s = NativeStore::open(&root).unwrap();
    // first probe: unfiltered, folds every segment, writes their filters
    s.believed_edge_versions(&eid_hex(0, 1), OPEN_END).unwrap();
    let first = s.vidfilter_stats();
    assert_eq!(first.8, 0, "the first probe had nothing to consult");
    assert!(first.2 > 0, "and wrote filters on its way out");
    assert!(!filter_files(&root).is_empty());
    drop(s);

    // second probe, fresh handle: now filtered
    let s = NativeStore::open(&root).unwrap();
    let eid = eid_hex(GENS - 1, 1);
    assert_eq!(s.believed_edge_versions(&eid, OPEN_END).unwrap().len(), 1);
    assert!(probes(&s, RowKind::Edge) > 0, "the second probe consulted them");
    assert!(
        s.filter_probe_stats(RowKind::Edge).2 > 0,
        "and skipped segments"
    );
    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}

// --------------------------------------------------------------------- //
// helpers                                                                //
// --------------------------------------------------------------------- //

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

/// The accounting claim, as a test: a filter built at seal is held as a
/// **file mapping**, not as heap, and `memory_accounting` charges it to
/// `file_mapped_total` rather than to `anonymous_total`.
///
/// Before this was checked, a seal-built filter stayed in the handle's map
/// as the owned bytes it was built from -- anonymous memory for the life of
/// the handle -- while only filters *loaded* by a later handle were mapped.
/// The memory-class split would then have been wrong exactly for
/// the build process, which is the one whose memory is under study.
#[test]
fn seal_built_filters_are_file_backed_and_accounted_as_mapped() {
    let root = tmp_root("accounting");
    let s = build(&root);
    let (current, _) = s.memory_accounting();
    let get = |k: &str| {
        current
            .iter()
            .find(|(n, _)| *n == k)
            .map(|(_, v)| *v)
            .unwrap_or_else(|| panic!("memory_accounting has no `{k}`"))
    };
    let mapped = get("vid_filter_mapped");
    let heap = get("vid_filter_heap");
    let segments = get("vid_filter_segments");
    let on_disk: u64 = std::fs::read_dir(filter_dir(&root))
        .unwrap()
        .filter_map(|e| e.ok())
        .count() as u64;
    assert_eq!(segments, on_disk, "every sealed filter is resident");
    assert!(mapped > 0, "seal-built filters are mapped, not owned");
    assert_eq!(heap, 0, "no filter is left on the heap when its write succeeded");
    assert!(
        get("file_mapped_total") >= mapped,
        "the filter's mapped bytes are part of file_mapped_total"
    );
    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}
