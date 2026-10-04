//! The ingest-cost remedies change what writing a segment and its
//! identity filter *costs*, never what is written. This file pins that.
//!
//! A deterministic store is built through the engine API — explicit
//! transaction times, no wall clock — with the shapes the write path has to
//! encode: endpoint uids that need JSON escaping (quotes, backslashes,
//! control characters, non-ASCII), event-lane and interval-lane rows,
//! valid-time columns with and without a common divisor, transaction times
//! interleaved so that compaction outputs dictionary-code `tt_s`, closes that
//! compaction folds into sidecars, and node versions. Size-tiered compaction
//! then runs to quiescence, so compaction outputs and their filters are in
//! the pinned set alongside sealed ones.
//!
//! The golden digests below were computed with the engine **before** the
//! ingest-cost optimizations (plus observation-only timers). Every
//! optimization keeps them: identical segment bytes and
//! identical filter bytes, file for file.

use sha2::{Digest, Sha256};
use gnomon_core::derive::{edge_eid, version_vid, Id96};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::row::{EdgeRow, NodeRow, RowKind};
use gnomon_core::store::NativeStore;
use gnomon_core::vidfilter::filter_dir;
use gnomon_core::OPEN_END;

const GENS: i64 = 24;
const ROWS_PER_GEN: i64 = 200;
const DAY_US: i64 = 86_400_000_000;

/// Golden: sha256 over `(file name, bytes)` of every live `seg/*.tgs`.
const GOLDEN_SEGMENTS: &str = "db9780171038c4505467436840f5fa399a110e8f974f51f28b5d7d7a18df2b9e";
/// Golden: sha256 over `(file name, bytes)` of every `idx/vidfilter/*.tvf`.
///
/// Moved once, deliberately: a filter is stamped and its seed path
/// started from its segment entry's `sha`, which this populates with the
/// footer `body_sha` (it was `""` for every segment before). The earlier
/// value was `db8bec06b5355b1afe2e7ff25874ccb66798b4f0a07a6e4a9582f2a119b40e1c`.
/// `GOLDEN_SEGMENTS` did not move: this change writes no segment byte.
const GOLDEN_FILTERS: &str = "c36b577b93fe84b1a742e01231a117f06ae268b60e62e1ea609e4881eec1b155";

fn tmp_root(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("gnomon-w22-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// Endpoint uids that exercise every branch of the canonical-JSON escaper.
fn uid(k: i64) -> String {
    match k % 7 {
        0 => format!("n{k}"),
        1 => format!("ω-{k}"),
        2 => format!("quote\"{k}"),
        3 => format!("back\\slash{k}"),
        4 => format!("tab\t{k}"),
        5 => format!("ctrl\u{01}\u{1f}{k}"),
        _ => format!("İstanbul/{k}"),
    }
}

struct Spec {
    src: String,
    dst: String,
    rel: &'static str,
    disc: String,
    vt_s: i64,
    vt_e: i64,
    props: String,
}

fn spec(g: i64, i: i64) -> Spec {
    let src = uid((i * 13 + g) % 61);
    let dst = uid((i * 7 + 3 * g + 1) % 61);
    let rel = ["SENT", "RATED", "PAID"][(i % 3) as usize];
    // generation-interleaved valid times: after a merge the rows of many
    // generations alternate in key order, so `tt_s` has many runs
    // millisecond-grid valid times (an exact divisor of 1000 for the scale
    // variants to find), generation-interleaved
    let vt_s = (i * 1_003 + g * 17) * 1_000;
    let vt_e = match i % 5 {
        0 => OPEN_END,
        1 => vt_s + 40 * DAY_US, // crosses several partitions: interval lane
        _ => vt_s + 1_000 * (1 + i % 11),
    };
    Spec {
        src,
        dst,
        rel,
        disc: format!("#{g}-{i}"),
        vt_s,
        vt_e,
        props: if i % 3 == 0 { "{}".into() } else { format!("{{\"w\":{}}}", i % 9) },
    }
}

fn vid_of(s: &Spec, tt: i64) -> Id96 {
    let eid = edge_eid(&s.src, &s.dst, s.rel, &s.disc);
    version_vid(&eid.to_hex(), tt, s.vt_s)
}

fn build(root: &std::path::Path) -> NativeStore {
    let mut store = NativeStore::open(root).unwrap();
    for g in 0..GENS {
        let tt = 1_000 + 7 * g;
        store.begin(tt).unwrap();
        for i in 0..ROWS_PER_GEN {
            let s = spec(g, i);
            let src_id = store.ensure_entity(&s.src, "Node").unwrap();
            let dst_id = store.ensure_entity(&s.dst, "Node").unwrap();
            let vid = vid_of(&s, tt);
            store
                .stage_edge(EdgeRow {
                    vid,
                    src_id,
                    dst_id,
                    rel_type: s.rel.to_string(),
                    disc: s.disc,
                    vt_s: s.vt_s,
                    vt_e: s.vt_e,
                    tt_s: tt,
                    props: s.props,
                    source: "ingest".to_string(),
                    provenance_ref: None,
                })
                .unwrap();
        }
        // every third generation closes a few versions written three
        // generations earlier: close runs now, sidecars after compaction
        if g >= 3 && g % 3 == 0 {
            let old_tt = 1_000 + 7 * (g - 3);
            for i in (0..ROWS_PER_GEN).step_by(17) {
                let s = spec(g - 3, i);
                store.close_version(RowKind::Edge, vid_of(&s, old_tt), tt).unwrap();
            }
        }
        for k in 0..3 {
            let u = uid(g * 3 + k);
            let uid_id = store.ensure_entity(&u, "Node").unwrap();
            store
                .stage_node(NodeRow {
                    vid: version_vid(&u, tt, g * DAY_US + k),
                    uid_id,
                    label: "Node".into(),
                    vt_s: g * DAY_US + k,
                    vt_e: OPEN_END,
                    tt_s: tt,
                    props: "{}".into(),
                    source: "ingest".into(),
                    provenance_ref: None,
                })
                .unwrap();
        }
        store.commit(EventLogRef::default()).unwrap();
    }
    let mut rounds = 0;
    while store.needs_compaction() {
        store.compact_incremental(None).unwrap();
        rounds += 1;
        assert!(rounds < 64, "compaction did not quiesce");
    }
    assert!(rounds > 0, "the fixture must exercise compaction outputs");
    store
}

/// sha256 over `(name, bytes)` of the files in `dir` whose name ends in
/// `ext`, in name order; and how many there were.
fn dir_digest(dir: &std::path::Path, ext: &str) -> (String, usize) {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(ext))
        .collect();
    names.sort();
    let mut h = Sha256::new();
    for n in &names {
        h.update(n.as_bytes());
        h.update([0u8]);
        let b = std::fs::read(dir.join(n)).unwrap();
        h.update((b.len() as u64).to_le_bytes());
        h.update(&b);
    }
    let d = h.finalize();
    (d.iter().map(|b| format!("{b:02x}")).collect(), names.len())
}

/// Live segment files only: gc may not have removed every superseded one.
fn live_segment_digest(store: &NativeStore, root: &std::path::Path) -> (String, usize) {
    let m = store.manifest();
    let mut names: Vec<String> = m
        .edge_lanes
        .event
        .iter()
        .chain(m.edge_lanes.interval.iter())
        .chain(m.node_store.iter())
        .map(|e| e.file.clone())
        .collect();
    names.sort();
    let mut h = Sha256::new();
    for n in &names {
        h.update(n.as_bytes());
        h.update([0u8]);
        let b = std::fs::read(root.join("seg").join(
            std::path::Path::new(n).file_name().unwrap(),
        ))
        .unwrap();
        h.update((b.len() as u64).to_le_bytes());
        h.update(&b);
    }
    let d = h.finalize();
    (d.iter().map(|b| format!("{b:02x}")).collect(), names.len())
}

#[test]
fn segment_and_filter_bytes_match_the_pre_remedy_engine() {
    let root = tmp_root("bytes");
    let store = build(&root);
    let (seg, nseg) = live_segment_digest(&store, &root);
    let (flt, nflt) = dir_digest(&filter_dir(&root), ".tvf");
    eprintln!("golden: segments {seg} ({nseg} files), filters {flt} ({nflt} files)");
    assert!(nseg >= 4 && nflt >= nseg, "segments {nseg}, filters {nflt}");

    // the fixture is only worth pinning if it reaches the code paths the
    // remedies touch: every stored codec, an exact divisor, and a
    // dictionary-coded belief-time column among the live segments
    let m = store.manifest();
    let files: Vec<String> = m
        .edge_lanes
        .event
        .iter()
        .chain(m.edge_lanes.interval.iter())
        .chain(m.node_store.iter())
        .map(|e| e.file.clone())
        .collect();
    let (mut codecs, mut divided, mut dictionary) = (std::collections::BTreeSet::new(), 0, 0);
    for f in &files {
        let seg = store.open_segment(f).unwrap();
        for c in &seg.header().columns {
            codecs.insert(c.codec);
            let lin_divisor = seg
                .lin_column(&c.name)
                .unwrap()
                .map(|l| l.scale().1)
                .unwrap_or(1);
            if c.divisor > 1 || lin_divisor > 1 {
                divided += 1;
            }
        }
        if seg.tt_s_view().unwrap().is_dictionary() {
            dictionary += 1;
        }
    }
    eprintln!("fixture: codecs {codecs:?}, divided columns {divided}, dictionary tt_s {dictionary}");
    assert_eq!(codecs.into_iter().collect::<Vec<_>>(), vec![0, 1, 2], "raw, FOR and the linear model all stored");
    assert!(divided > 0, "an exact divisor is stored somewhere");
    assert!(dictionary > 0, "a compaction output dictionary-codes tt_s");
    assert_eq!(seg, GOLDEN_SEGMENTS, "segment bytes moved");
    assert_eq!(flt, GOLDEN_FILTERS, "filter bytes moved");
    drop(store);
    let _ = std::fs::remove_dir_all(&root);
}
