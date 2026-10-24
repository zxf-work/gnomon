//! A sealed segment must land on the configured target **on disk**.
//!
//! Previously, both writers accumulated a model of each row's unencoded
//! footprint against `SEGMENT_TARGET_BYTES` and the encoder then compressed
//! it ~1.87x, so every segment sealed at about half the configured size and
//! every store carried about twice the segments its layout implied. The
//! writers now divide the configured target through by a measured per-lane
//! quotient (`gnomon_core::sealsize`).
//!
//! These tests run at a shrunken `segment_target_bytes` — the same lever
//! `settle.rs`'s own fixtures use — because the property under test is a
//! ratio and is scale-free, while a real 128 MiB segment is not something a
//! unit test can afford. What is *not* scale-free is the per-file header and
//! the unshared part of the string heap, which are a larger share of a small
//! segment than of a large one; that is a reason the convergence here is
//! harder than in production, not easier, since the quotient has to absorb
//! them too.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use gnomon_core::derive::{version_vid, Id96};
use gnomon_core::manifest::EventLogRef;
use gnomon_core::row::EdgeRow;
use gnomon_core::segment::{MemorySource, Segment};
use gnomon_core::staging::PartitionMap;
use gnomon_core::store::NativeStore;

/// Shrunken byte target. 1 MiB is 20,971 rows of the 50 B model, enough that
/// a segment's header is a small share of it and that several chunks fit in
/// one commit.
const TARGET: u64 = 1024 * 1024;

fn tmp_root(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("gnomon-sealsize-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn edge(a: u32, b: u32, i: u64, tt: i64) -> EdgeRow {
    let vt = i as i64;
    EdgeRow {
        vid: version_vid(&format!("e{i}"), tt, vt),
        src_id: a,
        dst_id: b,
        rel_type: "MESSAGED".into(),
        // a discriminator drawn from a small pool: shared enough that the
        // dictionary earns its keep, which is what makes the encoder beat
        // the row model here as it does on the campaign corpus
        disc: format!("d{}", i % 64),
        vt_s: vt,
        vt_e: vt + 1,
        tt_s: tt,
        props: "{}".into(),
        source: "ingest".into(),
        provenance_ref: None,
    }
}

/// `batches` commits of `per` edge rows, at a shrunken byte target.
fn build(name: &str, batches: u32, per: u32) -> (PathBuf, NativeStore) {
    let root = tmp_root(name);
    let mut s = NativeStore::open(&root).unwrap();
    s.set_layout(PartitionMap::default(), TARGET);
    let mut n = 0u64;
    for batch in 0..batches {
        let tt = 100 + batch as i64;
        s.begin(tt).unwrap();
        let a = s.ensure_entity(&format!("n{batch}a"), "Node").unwrap();
        let b = s.ensure_entity(&format!("n{batch}b"), "Node").unwrap();
        for _ in 0..per {
            s.stage_edge(edge(a, b, n, tt)).unwrap();
            n += 1;
        }
        s.commit(EventLogRef::default()).unwrap();
    }
    (root, s)
}

/// Every live edge segment's `(rows, file bytes)`, in manifest order.
fn edge_segments(root: &Path, s: &NativeStore) -> Vec<(u32, u64)> {
    s.segment_layout()
        .into_iter()
        .filter(|(lane, ..)| *lane != "nodes")
        .map(|(_, file, rows, ..)| {
            let bytes = std::fs::metadata(root.join(&file)).unwrap().len();
            (rows, bytes)
        })
        .collect()
}

// --- 1. the headline property ------------------------------------------- //

#[test]
fn segments_after_the_first_seal_within_ten_percent_of_the_target_on_disk() {
    let _serial = serial();
    // one commit large enough to split into several chunks: 200k rows is
    // ~10 MB of the 50 B model against a 1 MiB target
    let (root, s) = build("ondisk", 1, 200_000);
    let segs = edge_segments(&root, &s);
    assert!(
        segs.len() >= 4,
        "fixture must split into several segments, got {}",
        segs.len()
    );

    // the last segment is whatever the batch had left over, and the first is
    // the one sealed before anything had been measured — the claim is about
    // the ones in between
    let converged = &segs[1..segs.len() - 1];
    assert!(!converged.is_empty());
    for (i, (rows, bytes)) in converged.iter().enumerate() {
        let frac = *bytes as f64 / TARGET as f64;
        assert!(
            (0.90..=1.10).contains(&frac),
            "segment {} (index {}, {rows} rows) sealed at {frac:.3} of target \
             ({bytes} B); previously this was ~0.53",
            i + 1,
            i + 1
        );
    }

    // and the first one is the uncorrected seal the correction is measured
    // from, so it is the *small* one — if it were not, nothing was corrected
    let first = segs[0].1 as f64 / TARGET as f64;
    let mean_after = converged.iter().map(|(_, b)| *b as f64).sum::<f64>()
        / converged.len() as f64
        / TARGET as f64;
    assert!(
        first < mean_after,
        "the first seal ({first:.3}) should be the uncorrected one, \
         against {mean_after:.3} after"
    );
}

#[test]
fn the_uncorrected_writer_seals_far_under_target_and_the_correction_fixes_it() {
    let _serial = serial();
    // The baseline configuration of the A/B, in a test: with the correction off, the
    // previous behaviour must be reproducible from the same binary, or the
    // A/B's "before" side is measuring a different build rather than a
    // different rule.
    //
    // The claim is relative, because how far under target the uncorrected
    // writer seals is a property of the corpus, not of the engine — 0.535 on
    // the campaign's synthetic corpus, 0.29 on this deliberately compressible
    // fixture. What is a property of the engine is that the correction closes
    // that gap and the control does not.
    let treated = mean_fraction(&build("gap-t", 1, 200_000));
    let control = {
        let _guard = EnvGuard::set("GNOMON_SEAL_ADAPTIVE", "0");
        mean_fraction(&build("gap-c", 1, 200_000))
    };
    assert!(
        control < 0.60,
        "the uncorrected writer should seal well under target, got {control:.3}"
    );
    assert!(
        (0.90..=1.10).contains(&treated),
        "the corrected writer should seal on target, got {treated:.3}"
    );
}

/// Mean on-disk fraction of the target over the *converged* edge segments:
/// every one but the first and the last.
///
/// The last is whatever the batch had left over rather than a seal. The first
/// is the one sealed before anything had been measured — the observation the
/// correction is derived *from*, which by construction cannot itself be
/// corrected. Both sides are scored the same way, so the comparison is fair;
/// the A/B at scale scores the same quantity as a lane mean, where the one
/// uncorrected segment is 1 of 20 rather than 1 of 4.
fn mean_fraction((root, s): &(PathBuf, NativeStore)) -> f64 {
    let segs = edge_segments(root, s);
    assert!(segs.len() >= 3, "fixture sealed too little to score");
    let converged = &segs[1..segs.len() - 1];
    converged.iter().map(|(_, b)| *b as f64).sum::<f64>() / converged.len() as f64
        / TARGET as f64
}

// --- 2. determinism --------------------------------------------- //

#[test]
fn two_builds_of_the_same_batches_cut_at_the_same_rows() {
    let _serial = serial();
    // The same log must produce the same segment boundaries. The
    // quotient is derived from committed row counts and file lengths, both
    // byte-identical across builds, and accumulated in integer arithmetic, so
    // the cut points must agree exactly — not approximately.
    let (root_a, a) = build("det-a", 3, 80_000);
    let (root_b, b) = build("det-b", 3, 80_000);
    let sa = edge_segments(&root_a, &a);
    let sb = edge_segments(&root_b, &b);
    assert!(sa.len() > 3, "fixture must produce several segments");
    assert_eq!(
        sa.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
        sb.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
        "two builds of the same batches cut at different rows"
    );
    assert_eq!(sa, sb, "two builds of the same batches wrote different bytes");
}

#[test]
fn a_reopened_store_continues_from_the_same_quotient() {
    let _serial = serial();
    // the seed lives in the committed manifest, not in the writer, so a
    // process that reopens a store mid-build must size its next segments
    // exactly as one that never closed it — which is what makes this
    // "recovery is a replay of the un-applied suffix" size-neutral.
    let root_a = tmp_root("reopen-a");
    let root_b = tmp_root("reopen-b");
    for (root, reopen) in [(&root_a, false), (&root_b, true)] {
        let mut s = NativeStore::open(root).unwrap();
        s.set_layout(PartitionMap::default(), TARGET);
        let mut n = 0u64;
        for batch in 0..3u32 {
            if reopen && batch > 0 {
                drop(s);
                s = NativeStore::open(root).unwrap();
                s.set_layout(PartitionMap::default(), TARGET);
            }
            let tt = 100 + batch as i64;
            s.begin(tt).unwrap();
            let a = s.ensure_entity(&format!("n{batch}a"), "Node").unwrap();
            let b = s.ensure_entity(&format!("n{batch}b"), "Node").unwrap();
            for _ in 0..80_000 {
                s.stage_edge(edge(a, b, n, tt)).unwrap();
                n += 1;
            }
            s.commit(EventLogRef::default()).unwrap();
        }
    }
    let a = NativeStore::open(&root_a).unwrap();
    let b = NativeStore::open(&root_b).unwrap();
    assert_eq!(
        edge_segments(&root_a, &a),
        edge_segments(&root_b, &b),
        "reopening the store between commits changed where it sealed"
    );
}

// --- 3. content neutrality ---------------------------------------------- //

#[test]
fn the_rows_are_the_same_rows_whatever_the_quotient_says() {
    let _serial = serial();
    // the correction moves boundaries; it must not move, drop, duplicate or
    // reorder a single row. Compared against the baseline, which is the
    // previous layout, so this is the unit-scale form of the A/B's content
    // digest gate.
    let (root_t, t) = build("neutral-t", 2, 120_000);
    let treated = all_rows(&root_t, &t);
    let control = {
        let _guard = EnvGuard::set("GNOMON_SEAL_ADAPTIVE", "0");
        let (root_c, c) = build("neutral-c", 2, 120_000);
        all_rows(&root_c, &c)
    };
    assert_ne!(
        treated.1, control.1,
        "the two sides should have cut the lane differently, or nothing was tested"
    );
    assert_eq!(
        treated.0, control.0,
        "the correction changed which rows the store holds"
    );
}

/// `(sorted (vt_s, vid, disc) of every live edge row, segment count)`, read
/// straight out of the segment files.
///
/// Nothing in these fixtures is ever closed, so no visibility resolution is
/// needed and the segments *are* the content — which is the point: the claim
/// is that the bytes the writer laid down describe the same rows however it
/// chose to cut them.
fn all_rows(root: &Path, s: &NativeStore) -> (Vec<(i64, Id96, String)>, usize) {
    let files: Vec<String> = s
        .segment_layout()
        .into_iter()
        .filter(|(lane, ..)| *lane != "nodes")
        .map(|(_, file, ..)| file)
        .collect();
    let mut rows = Vec::new();
    for f in &files {
        let path = root.join(f);
        let seg = Segment::open(&path, MemorySource::load(&path).unwrap(), true).unwrap();
        let vt_s = seg.i64_column("vt_s").unwrap().to_vec();
        let disc_ref = seg.u32_column("disc_ref").unwrap().to_vec();
        let strings = seg.strings().unwrap();
        for i in 0..vt_s.len() {
            rows.push((
                vt_s[i],
                seg.vid_at(i).unwrap(),
                strings.get(disc_ref[i]).unwrap().to_string(),
            ));
        }
    }
    rows.sort();
    (rows, files.len())
}

// --- 4. the memory bound ------------------------------------------------ //

#[test]
fn the_resident_cap_ends_a_chunk_before_the_byte_target_does() {
    let _serial = serial();
    // The correction raises the accounted target, and the writer's buffer is
    // owned rows at ~228 B resident against 50 B accounted, so the resident
    // set scales with it. The cap is the bound that makes that a stated
    // quantity rather than an observed one: set it low and it must be what
    // ends the chunk.
    let _guard = EnvGuard::set("GNOMON_CHUNK_RESIDENT_BYTES", "600000");
    let (root, s) = build("resident", 1, 100_000);
    let segs = edge_segments(&root, &s);
    // 600 kB of resident rows at ~280 B/row is ~2,100 rows, far below the
    // ~21,000 the 1 MiB accounted target would hold
    let rows: Vec<u32> = segs.iter().map(|(r, _)| *r).collect();
    let max = *rows.iter().max().unwrap();
    assert!(
        max < 8_000,
        "the resident cap did not bind: largest segment holds {max} rows"
    );
    assert!(
        segs.len() > 10,
        "expected many small segments under a tight resident cap, got {}",
        segs.len()
    );
}

#[test]
fn a_chunk_never_exceeds_the_stated_resident_formula() {
    let _serial = serial();
    // The bound the report states:
    //     resident(chunk) <= cap + one tie group
    // The tie rule can carry a chunk past the cap — a boundary that split a
    // tie group would make manifest pruning unsound — so the
    // overshoot is bounded by the largest tie group, not by nothing. This
    // fixture has one row per key, so the overshoot is one row.
    let cap: u64 = 600_000;
    let _guard = EnvGuard::set("GNOMON_CHUNK_RESIDENT_BYTES", &cap.to_string());
    let (root, s) = build("formula", 1, 100_000);
    for (rows, _) in edge_segments(&root, &s) {
        // the fixture's rows are uniform in size, so bytes/row is a constant
        // the test can recover from the cap itself rather than hardcode
        let resident = rows as u64 * per_row_resident();
        assert!(
            resident <= cap + per_row_resident(),
            "a chunk held {resident} B resident against a {cap} B cap"
        );
    }
}

/// `sealsize::edge_resident_bytes` for this fixture's rows, which are all the
/// same shape.
fn per_row_resident() -> u64 {
    gnomon_core::sealsize::edge_resident_bytes(&edge(1, 2, 0, 100))
}

// --- 5. the planner and the floor follow the writer --------------------- //

#[test]
fn settle_reaches_a_floor_consistent_with_the_corrected_seal_size() {
    let _serial = serial();
    // The settle byte floor is a count of the segments the writer can
    // produce, so it has to be computed against the target the writer seals
    // on. If it used the configured target instead it would come out at
    // twice the segments and settle would stop early — which is the failure
    // this asserts is absent: after a settle the store must be near the
    // floor implied by the *corrected* size, and a second settle must find
    // nothing to do.
    // many small commits: one under-target segment each, which is the
    // straggler shape a tiered build leaves and the shape settle exists for
    let (root, mut s) = build("floor", 60, 8_000);
    let before = edge_segments(&root, &s).len();
    let r1 = s.settle(None).unwrap();
    let after = edge_segments(&root, &s);
    assert!(
        r1.segments_after < r1.segments_before,
        "settle did nothing: {} -> {}",
        r1.segments_before,
        r1.segments_after
    );
    assert!(after.len() < before, "settle did not reduce the edge lane");

    // the settled segments must themselves be on target, not at half it —
    // the merge writer reads the same quotient the build writer wrote
    let interior = &after[..after.len().saturating_sub(1)];
    if !interior.is_empty() {
        let mean = interior.iter().map(|(_, b)| *b as f64).sum::<f64>()
            / interior.len() as f64
            / TARGET as f64;
        assert!(
            (0.85..=1.15).contains(&mean),
            "settled segments mean {mean:.3} of target"
        );
    }

    // idempotence: a store at its floor rewrites nothing
    let r2 = s.settle(None).unwrap();
    assert_eq!(
        r2.rows_rewritten, 0,
        "a second settle rewrote {} rows; the floor and the writer disagree",
        r2.rows_rewritten
    );
}

#[test]
fn the_tier_planner_does_not_drag_a_correctly_sized_segment_through_a_merge() {
    let _serial = serial();
    // `plan_lane` skips any segment already at the byte target, and "at the
    // target" is half the *accounted* capacity. With the correction on, a
    // segment that is on target on disk holds ~1.6x the rows the configured
    // target implies, so a planner reading the uncorrected target would call
    // it half-full and merge it — the write amplification the tier rule
    // exists to avoid. Measured as: compacting a store whose segments are
    // already on target rewrites nothing.
    let (root, mut s) = build("planner", 60, 8_000);
    s.settle(None).unwrap();
    let settled = edge_segments(&root, &s);
    assert!(!settled.is_empty());
    let report = s.compact_incremental(None).unwrap();
    assert_eq!(
        report.rows_rewritten, 0,
        "compaction rewrote {} rows of a store already at the byte target",
        report.rows_rewritten
    );
    assert_eq!(edge_segments(&root, &s), settled);
}

// --- test plumbing ------------------------------------------------------ //

/// Every test in this file runs under one lock.
///
/// Two of them set a process-wide environment variable — `GNOMON_SEAL_ADAPTIVE`
/// to pin the uncorrected baseline, `GNOMON_CHUNK_RESIDENT_BYTES` to make the
/// memory cap bind — and cargo runs a test binary's tests in threads of one
/// process, so an unserialised run would let one test's variable decide
/// another test's segment sizes. That is not a flake that would show up as a
/// flake: it would show up as the correction appearing not to work.
fn serial() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Sets an environment variable for the life of the guard.
///
/// Rust runs a crate's tests in threads of one process, so an env var set by
/// one test is visible to every other. Each test that needs one therefore
/// builds its own store under its own name and the guard restores the
/// previous value; the tests that must not see a set variable are the ones
/// that assert the *corrected* behaviour, and they would fail loudly rather
/// than silently if a guard leaked.
struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}
