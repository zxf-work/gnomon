//! `diff_snapshots` — the valid-time diff of two instants under one belief —
//! in the engine.
//!
//! # The answer, as the Python operator defines it
//!
//! `gnomon/temporal/ops_snapshot.py::diff_snapshots` takes the point states
//! `G(t1)` and `G(t2)` (believed versions with `vt_s <= t < vt_e`, as of one
//! belief time) and reports, by logical identity: nodes added and removed
//! (sorted by uid), edges added and removed (sorted by `eid`, each described
//! by `eid, src, dst, rel_type`), and `props_changed` — identities present at
//! both instants through *different* version rows whose content differs
//! (nodes: label or props; edges: props), nodes first by uid, then edges by
//! `eid`. Every list is paged at `limit` and carries its total.
//!
//! # What the engine does instead of dictionaries
//!
//! * **Rows before identities.** Both point states come out of the same
//!   segments, and a version is one stored row, so a row that is in both
//!   states is the same version at both instants: unchanged, whatever its
//!   identity. The planner's believed ranges at each instant become a sorted
//!   row list per segment, and a merge of the two lists retires every such
//!   row without hashing anything. On a store of long-lived edges that is
//!   nearly all of them.
//! * **Identities for the leftovers only.** What is left is joined on
//!   identity: nodes on the dense `uid_id` (no string), edges on `eid` —
//!   the key the operator itself joins on, so the join is the operator's by
//!   construction. An edge leftover is either added or removed (and then it
//!   is ordered by `eid` anyway) or matched; on a point-in-time diff nearly
//!   all of them are the former, so its sha256 is paid exactly where the
//!   answer needs it. The digests are computed on the worker pool, each key
//!   bucketed by its leading bits, and every bucket is joined in parallel.
//!   (`tests::tuple_equality_is_eid_equality` pins why the identity tuple and
//!   the `eid` are interchangeable as join keys for the reference below.)
//! * **Strings for the pages only.** uid/eid/rel strings are built for the
//!   page's rows.
//! * **Content compared as stored text, decided in Python where it differs.**
//!   A candidate whose props text and label are byte-equal at both instants
//!   cannot be a change (`json.loads` of equal text is equal), so it is
//!   dropped here. The few whose text differs go back with both texts and the
//!   operator applies its own comparison (`json.loads(a) != json.loads(b)`),
//!   so a non-canonical spelling of equal content can never be reported.
//!
//! `scope` (a seeded neighbourhood restriction) keeps the Python path, as
//! does an open write batch (the segment scan does not see staged rows).

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use crate::derive::{edge_eid_buf, Id96};
use crate::error::{EngineError, Result};
use crate::scan::{ScanRequest, ScanSet, ScanTarget};
use crate::segment::{MmapSource, Segment};
use crate::store::NativeStore;

/// A multiply-rotate hasher (the `FxHash` construction) for the identity
/// join: its keys are dense integers and short interned strings, which a
/// DoS-resistant `SipHash` would only make slower.
#[derive(Default, Clone, Copy)]
struct Fx(u64);

impl Hasher for Fx {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let (chunks, rest) = bytes.as_chunks::<8>();
        for c in chunks {
            self.add(u64::from_le_bytes(*c));
        }
        let mut tail = 0u64;
        for (i, b) in rest.iter().enumerate() {
            tail |= (*b as u64) << (8 * i);
        }
        self.add(tail ^ bytes.len() as u64);
    }
    #[inline]
    fn write_u32(&mut self, v: u32) {
        self.add(v as u64);
    }
    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.add(v);
    }
    #[inline]
    fn write_usize(&mut self, v: usize) {
        self.add(v as u64);
    }
}

impl Fx {
    #[inline]
    fn add(&mut self, v: u64) {
        self.0 = (self.0.rotate_left(5) ^ v).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
}

type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<Fx>>;

/// One added or removed edge, as the operator's `edge_desc` spells it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeDesc {
    pub eid: String,
    pub src: String,
    pub dst: String,
    pub rel_type: String,
}

/// A node present at both instants through two different versions whose
/// label or props text differ. `(uid, vid_a, vid_b, label_a, label_b,
/// props_a, props_b)`, `a` at `t1`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeCand {
    pub uid: String,
    pub vid_a: String,
    pub vid_b: String,
    pub label_a: String,
    pub label_b: String,
    pub props_a: String,
    pub props_b: String,
}

/// An edge present at both instants through two different versions whose
/// props text differs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeCand {
    pub eid: String,
    pub vid_a: String,
    pub vid_b: String,
    pub props_a: String,
    pub props_b: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapDiffOut {
    pub nodes_added: Vec<String>,
    pub nodes_added_total: usize,
    pub nodes_removed: Vec<String>,
    pub nodes_removed_total: usize,
    pub edges_added: Vec<EdgeDesc>,
    pub edges_added_total: usize,
    pub edges_removed: Vec<EdgeDesc>,
    pub edges_removed_total: usize,
    /// Sorted by uid; every node change candidate (the operator decides).
    pub node_cands: Vec<NodeCand>,
    /// Sorted by eid; every edge change candidate.
    pub edge_cands: Vec<EdgeCand>,
}

/// `(segment index, row)` of one point-state row.
type Loc = (u32, u32);

/// The believed rows valid at instant `t`, per planned segment, ascending.
fn point_rows<S: crate::segment::SegmentSource>(
    set: &ScanSet<'_, S>,
    n_targets: usize,
    as_of: i64,
    t: i64,
) -> Result<Vec<Vec<u32>>> {
    let req = ScanRequest {
        as_of_tt: as_of,
        vt_min: Some(t),
        vt_max: Some(t.saturating_add(1)),
        ..Default::default()
    };
    let (plans, _) = set.plan_believed(&req)?;
    let mut out: Vec<Vec<u32>> = vec![Vec::new(); n_targets];
    for p in &plans {
        let rows = &mut out[p.idx];
        rows.reserve(p.rows);
        for &(a, b) in &p.ranges {
            match p.vt_e {
                // elided: every row is `[vt_s, vt_s + 1)`, and the planner's
                // cut is exact on both ends
                None => rows.extend(a as u32..b as u32),
                // the scan's own remaining test, `vt_e > vt_min`
                Some(vt_e) => rows.extend((a..b).filter(|&i| vt_e[i] > t).map(|i| i as u32)),
            }
        }
    }
    Ok(out)
}

/// Rows only in `a`, rows only in `b` (both ascending): the versions that are
/// *not* the same row at both instants.
fn split_common(seg: u32, a: &[u32], b: &[u32], only_a: &mut Vec<Loc>, only_b: &mut Vec<Loc>) {
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                only_a.push((seg, a[i]));
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                only_b.push((seg, b[j]));
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    only_a.extend(a[i..].iter().map(|&r| (seg, r)));
    only_b.extend(b[j..].iter().map(|&r| (seg, r)));
}

/// Keep the `limit` smallest of `v` under `cmp`, sorted.
fn top_k<T>(v: &mut Vec<T>, limit: usize, cmp: impl Fn(&T, &T) -> std::cmp::Ordering) {
    if v.len() > limit {
        if limit == 0 {
            v.clear();
            return;
        }
        v.select_nth_unstable_by(limit - 1, &cmp);
        v.truncate(limit);
    }
    v.sort_unstable_by(cmp);
}

/// One segment's columns the edge diff reads.
struct EdgeCols<'s> {
    src: &'s [u32],
    dst: &'s [u32],
    rel: &'s [u16],
    disc: &'s [u32],
    props: &'s [u32],
    vid_hi: &'s [u64],
    vid_lo: &'s [u32],
    strings: crate::segment::StringView<'s>,
    rel_types: &'s [String],
}

impl<'s> EdgeCols<'s> {
    fn open(seg: &'s Segment<MmapSource>) -> Result<Self> {
        Ok(Self {
            src: seg.u32_column("src_id")?,
            dst: seg.u32_column("dst_id")?,
            rel: seg.u16_column("rel_code")?,
            disc: seg.u32_column("disc_ref")?,
            props: seg.u32_column("props_ref")?,
            vid_hi: seg.u64_column("vid64")?,
            vid_lo: seg.u32_column("vid_lo32")?,
            strings: seg.strings()?,
            rel_types: &seg.header().rel_types,
        })
    }

    fn rel(&self, i: usize) -> Result<&'s str> {
        self.rel_types
            .get(self.rel[i] as usize)
            .map(String::as_str)
            .ok_or_else(|| {
                EngineError::corrupt(format!("rel_code {} has no entry", self.rel[i])).at_row(i as u32)
            })
    }

    fn vid(&self, i: usize) -> Id96 {
        Id96 {
            hi: self.vid_hi[i],
            lo: self.vid_lo[i],
        }
    }
}

/// The identity tuple of an edge row: the four inputs of its `eid`.
type EdgeIdent<'s> = (u32, u32, &'s str, &'s str);

/// Buckets of the parallel join: a key's leading bits pick its bucket.
const JOIN_BUCKETS: usize = 64;

fn bucket_of(e: &Id96) -> usize {
    (e.hi >> 58) as usize
}

/// `(key(l), l)` for every `l`, computed on the pool in chunks and returned
/// as one bucket list per chunk (`[chunk][bucket]`), so the join can gather a
/// bucket without a serial scatter pass.
#[allow(clippy::type_complexity)]
fn keyed_buckets<F>(locs: &[Loc], width: usize, key: F) -> Result<Vec<Vec<Vec<(Id96, Loc)>>>>
where
    F: Fn(&Loc, &mut String) -> Result<Id96> + Sync,
{
    const CHUNK: usize = 8192;
    let tasks = locs.len().div_ceil(CHUNK);
    if tasks == 0 {
        return Ok(Vec::new());
    }
    let slots: Vec<std::sync::Mutex<Option<Vec<Vec<(Id96, Loc)>>>>> =
        (0..tasks).map(|_| std::sync::Mutex::new(None)).collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let done: Vec<Result<()>> = crate::pool::run(width.min(tasks), |_| {
        let mut buf = String::new();
        loop {
            let t = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if t >= tasks {
                return Ok(());
            }
            let mut b: Vec<Vec<(Id96, Loc)>> = vec![Vec::new(); JOIN_BUCKETS];
            for l in &locs[t * CHUNK..((t + 1) * CHUNK).min(locs.len())] {
                let e = key(l, &mut buf)?;
                b[bucket_of(&e)].push((e, *l));
            }
            *slots[t].lock().unwrap_or_else(|e| e.into_inner()) = Some(b);
        }
    });
    for d in done {
        d?;
    }
    Ok(slots
        .into_iter()
        .map(|s| s.into_inner().unwrap_or_else(|e| e.into_inner()).expect("every chunk ran"))
        .collect())
}

/// One bucket's join: rows only at `t1`, only at `t2`, and the identities at
/// both, `(eid, at t1, at t2)`.
#[derive(Default)]
struct Joined {
    only1: Vec<(Id96, Loc)>,
    only2: Vec<(Id96, Loc)>,
    pairs: Vec<(Id96, Loc, Loc)>,
}

#[allow(clippy::type_complexity)]
fn join_buckets(
    side1: &[Vec<Vec<(Id96, Loc)>>],
    side2: &[Vec<Vec<(Id96, Loc)>>],
    width: usize,
) -> Vec<Joined> {
    let next = std::sync::atomic::AtomicUsize::new(0);
    let parts: Vec<Vec<Joined>> = crate::pool::run(width.min(JOIN_BUCKETS), |_| {
        let mut mine = Vec::new();
        loop {
            let k = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if k >= JOIN_BUCKETS {
                return mine;
            }
            let n1: usize = side1.iter().map(|c| c[k].len()).sum();
            let mut at1: FxMap<Id96, (Loc, bool)> = FxMap::default();
            at1.reserve(n1);
            for chunk in side1 {
                for &(e, l) in &chunk[k] {
                    at1.insert(e, (l, false));
                }
            }
            let mut j = Joined::default();
            for chunk in side2 {
                for &(e, l) in &chunk[k] {
                    match at1.get_mut(&e) {
                        Some((l1, hit)) => {
                            *hit = true;
                            j.pairs.push((e, *l1, l));
                        }
                        None => j.only2.push((e, l)),
                    }
                }
            }
            j.only1 = at1
                .into_iter()
                .filter(|(_, (_, hit))| !hit)
                .map(|(e, (l, _))| (e, l))
                .collect();
            mine.push(j);
        }
    });
    parts.into_iter().flatten().collect()
}

impl NativeStore {
    /// The engine half of `diff_snapshots` without `scope` — see the module
    /// note. `None` inside an open write batch.
    pub fn diff_snapshots(&self, t1: i64, t2: i64, as_of_tt: i64, limit: usize) -> Result<Option<SnapDiffOut>> {
        if self.in_batch() {
            return Ok(None);
        }
        self.assert_full_belief(as_of_tt)?;
        let as_of = crate::clamp_tt(as_of_tt);
        let mut out = SnapDiffOut::default();
        let t = std::time::Instant::now();
        self.snapdiff_nodes(t1, t2, as_of, limit, &mut out)?;
        crate::phase::since(crate::phase::Phase::SnapNodes, t);
        self.snapdiff_edges(t1, t2, as_of, limit, &mut out)?;
        Ok(Some(out))
    }

    fn uid_str(&self, id: u32) -> Result<&str> {
        self.dict()
            .uid(id)
            .ok_or_else(|| EngineError::invariant(format!("dense id {id} is not in the dictionary")))
    }

    fn snapdiff_nodes(&self, t1: i64, t2: i64, as_of: i64, limit: usize, out: &mut SnapDiffOut) -> Result<()> {
        let files = self.node_files();
        let mut segs = Vec::with_capacity(files.len());
        for (file, id) in &files {
            segs.push((self.open_segment(file)?, *id));
        }
        let targets: Vec<ScanTarget<'_, MmapSource>> = segs
            .iter()
            .map(|(segment, id)| ScanTarget {
                segment,
                lane: segment.header().lane,
                id: *id,
            })
            .collect();
        let set = ScanSet::new(targets)
            .with_closes(self.close_index()?)
            .with_designs(self.designs());
        let r1 = point_rows(&set, segs.len(), as_of, t1)?;
        let r2 = point_rows(&set, segs.len(), as_of, t2)?;
        let (mut only1, mut only2) = (Vec::new(), Vec::new());
        for s in 0..segs.len() {
            split_common(s as u32, &r1[s], &r2[s], &mut only1, &mut only2);
        }
        // the dense id of each leftover row; a row in both states is the same
        // version at both instants and was retired above
        let uid_of = |&(s, r): &Loc| -> Result<u32> {
            Ok(segs[s as usize].0.u32_column("uid_id")?[r as usize])
        };
        let mut at1: FxMap<u32, Loc> = FxMap::default();
        at1.reserve(only1.len());
        for l in &only1 {
            at1.insert(uid_of(l)?, *l);
        }
        // A uid whose row is in both states has no other row valid at either
        // instant (one believed version per identity per instant, I1), so it
        // cannot appear among the leftovers: the leftovers alone decide.
        let mut added: Vec<u32> = Vec::new();
        let mut cands: Vec<(u32, Loc, Loc)> = Vec::new();
        let mut matched1: FxMap<u32, ()> = FxMap::default();
        for l in &only2 {
            let u = uid_of(l)?;
            match at1.get(&u) {
                Some(&l1) => {
                    matched1.insert(u, ());
                    cands.push((u, l1, *l));
                }
                None => added.push(u),
            }
        }
        let mut removed: Vec<u32> = Vec::new();
        for &u in at1.keys() {
            if !matched1.contains_key(&u) {
                removed.push(u);
            }
        }
        out.nodes_added_total = added.len();
        out.nodes_removed_total = removed.len();
        for (ids, page) in [(&mut added, &mut out.nodes_added), (&mut removed, &mut out.nodes_removed)] {
            let mut keyed: Vec<&str> = Vec::with_capacity(ids.len());
            for &u in ids.iter() {
                keyed.push(self.uid_str(u)?);
            }
            top_k(&mut keyed, limit, |a, b| a.cmp(b));
            *page = keyed.into_iter().map(str::to_string).collect();
        }
        // change candidates: different rows, so (normally) different vids;
        // equal label and props text cannot be a change
        let col = |(s, r): Loc, name: &str| -> Result<&str> {
            let seg = &segs[s as usize].0;
            seg.strings()?.get(seg.u32_column(name)?[r as usize])
        };
        let vid = |(s, r): Loc| -> Result<Id96> {
            let seg = &segs[s as usize].0;
            Ok(Id96 {
                hi: seg.u64_column("vid64")?[r as usize],
                lo: seg.u32_column("vid_lo32")?[r as usize],
            })
        };
        let mut kept: Vec<(&str, NodeCand)> = Vec::new();
        for (u, l1, l2) in cands {
            let (va, vb) = (vid(l1)?, vid(l2)?);
            if va == vb {
                continue;
            }
            let (la, lb) = (col(l1, "label_ref")?, col(l2, "label_ref")?);
            let (pa, pb) = (col(l1, "props_ref")?, col(l2, "props_ref")?);
            if la == lb && pa == pb {
                continue;
            }
            let uid = self.uid_str(u)?;
            kept.push((
                uid,
                NodeCand {
                    uid: uid.to_string(),
                    vid_a: va.to_hex(),
                    vid_b: vb.to_hex(),
                    label_a: la.to_string(),
                    label_b: lb.to_string(),
                    props_a: pa.to_string(),
                    props_b: pb.to_string(),
                },
            ));
        }
        kept.sort_by(|a, b| a.0.cmp(b.0));
        out.node_cands = kept.into_iter().map(|(_, c)| c).collect();
        Ok(())
    }

    fn snapdiff_edges(&self, t1: i64, t2: i64, as_of: i64, limit: usize, out: &mut SnapDiffOut) -> Result<()> {
        let req = ScanRequest {
            as_of_tt: as_of,
            ..Default::default()
        };
        use crate::phase::{since, Phase};
        self.with_edge_scan(&req, |set, _| {
            let t = std::time::Instant::now();
            let n = set.n_targets();
            let r1 = point_rows(set, n, as_of, t1)?;
            let r2 = point_rows(set, n, as_of, t2)?;
            since(Phase::SnapEdgePlan, t);
            let t = std::time::Instant::now();
            let mut cols: Vec<Option<EdgeCols<'_>>> = Vec::with_capacity(n);
            for (s, (a, b)) in r1.iter().zip(&r2).enumerate() {
                cols.push(if a.is_empty() && b.is_empty() {
                    None
                } else {
                    Some(EdgeCols::open(set.segment(s))?)
                });
            }
            let c = |s: u32| cols[s as usize].as_ref().expect("a segment with rows has columns");
            let (mut only1, mut only2) = (Vec::new(), Vec::new());
            for s in 0..n {
                split_common(s as u32, &r1[s], &r2[s], &mut only1, &mut only2);
            }
            drop((r1, r2));
            let ident = |&(s, r): &Loc| -> Result<EdgeIdent<'_>> {
                let cs = c(s);
                let i = r as usize;
                Ok((cs.src[i], cs.dst[i], cs.rel(i)?, cs.strings.get(cs.disc[i])?))
            };
            // The leftovers are joined on `eid`, exactly the key the operator
            // joins on. Every leftover needs it anyway: the unmatched ones are
            // the added and removed lists, which are ordered by it, and on a
            // point-in-time diff they are nearly all of the leftovers. One
            // sha256 per row, in parallel, each row's key bucketed by its
            // leading bits; then one hash join per bucket, in parallel.
            let width = crate::scan::scan_threads().max(1);
            let side1 = keyed_buckets(&only1, width, |l, buf| {
                let (s, d, rel, disc) = ident(l)?;
                Ok(edge_eid_buf(buf, self.uid_str(s)?, self.uid_str(d)?, rel, disc))
            })?;
            let side2 = keyed_buckets(&only2, width, |l, buf| {
                let (s, d, rel, disc) = ident(l)?;
                Ok(edge_eid_buf(buf, self.uid_str(s)?, self.uid_str(d)?, rel, disc))
            })?;
            drop((only1, only2));
            let joined = join_buckets(&side1, &side2, width);
            let (mut removed, mut added, mut pairs) = (Vec::new(), Vec::new(), Vec::new());
            for j in joined {
                removed.extend(j.only1);
                added.extend(j.only2);
                pairs.extend(j.pairs);
            }
            drop((side1, side2));
            out.edges_added_total = added.len();
            out.edges_removed_total = removed.len();
            since(Phase::SnapEdgeJoin, t);
            let t = std::time::Instant::now();

            for (mut keyed, page) in [(added, &mut out.edges_added), (removed, &mut out.edges_removed)] {
                top_k(&mut keyed, limit, |a: &(Id96, Loc), b: &(Id96, Loc)| a.0.cmp(&b.0));
                let mut descs = Vec::with_capacity(keyed.len());
                for (e, l) in keyed {
                    let (s, d, rel, _) = ident(&l)?;
                    descs.push(EdgeDesc {
                        eid: e.to_hex(),
                        src: self.uid_str(s)?.to_string(),
                        dst: self.uid_str(d)?.to_string(),
                        rel_type: rel.to_string(),
                    });
                }
                *page = descs;
            }

            // change candidates: a version change whose props text differs
            let mut kept: Vec<(Id96, EdgeCand)> = Vec::new();
            for (e, l1, l2) in pairs {
                let (c1, c2) = (c(l1.0), c(l2.0));
                let (i1, i2) = (l1.1 as usize, l2.1 as usize);
                let (va, vb) = (c1.vid(i1), c2.vid(i2));
                if va == vb {
                    continue;
                }
                let pa = c1.strings.get(c1.props[i1])?;
                let pb = c2.strings.get(c2.props[i2])?;
                if pa == pb {
                    continue;
                }
                kept.push((
                    e,
                    EdgeCand {
                        eid: e.to_hex(),
                        vid_a: va.to_hex(),
                        vid_b: vb.to_hex(),
                        props_a: pa.to_string(),
                        props_b: pb.to_string(),
                    },
                ));
            }
            kept.sort_by_key(|a| a.0);
            out.edge_cands = kept.into_iter().map(|(_, c)| c).collect();
            since(Phase::SnapEdgePages, t);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derive::{edge_eid, version_vid};
    use crate::manifest::EventLogRef;
    use crate::row::{EdgeRow, NodeRow, RowKind};
    use crate::OPEN_END;
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;

    fn tmp_root(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-snapdiff-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    /// The identity join's premise: two edge rows have the same `eid` exactly
    /// when their `(src, dst, rel_type, disc)` agree — including tuples whose
    /// parts would collide under a naive concatenation, and the JSON escapes.
    #[test]
    fn tuple_equality_is_eid_equality() {
        let parts = ["", "a", "b", "ab", "a,b", "\"", "\\", "a\"b", "[", "]", "\n", "中"];
        let mut seen: HashMap<Id96, (String, String, String, String)> = HashMap::new();
        for s in parts {
            for d in parts {
                for r in ["R", "S", "R,S"] {
                    for disc in parts {
                        let t = (s.to_string(), d.to_string(), r.to_string(), disc.to_string());
                        let e = edge_eid(s, d, r, disc);
                        if let Some(prev) = seen.insert(e, t.clone()) {
                            assert_eq!(prev, t, "two tuples share an eid");
                        }
                        assert_eq!(edge_eid(s, d, r, disc), e, "the eid is a function of the tuple");
                    }
                }
            }
        }
        assert_eq!(seen.len(), parts.len() * parts.len() * 3 * parts.len());
    }

    #[test]
    fn split_common_partitions_two_sorted_lists() {
        let (mut a, mut b) = (Vec::new(), Vec::new());
        split_common(7, &[1, 3, 5, 9], &[2, 3, 9, 10, 11], &mut a, &mut b);
        assert_eq!(a, vec![(7, 1), (7, 5)]);
        assert_eq!(b, vec![(7, 2), (7, 10), (7, 11)]);
    }

    /// A reference written the operator's way: two point states as
    /// identity -> (vid, label, props) maps, then the set and content diffs.
    #[allow(clippy::type_complexity)]
    fn reference(
        nodes: &[(NodeRow, i64)],
        edges: &[(EdgeRow, i64)],
        uid: &dyn Fn(u32) -> String,
        t1: i64,
        t2: i64,
        as_of: i64,
    ) -> (Vec<String>, Vec<String>, Vec<String>, Vec<String>, Vec<String>, Vec<String>) {
        let as_of = crate::clamp_tt(as_of);
        let believed = |tt_s: i64, tt_e: i64| tt_s <= as_of && as_of < tt_e;
        let nstate = |t: i64| -> BTreeMap<String, (Id96, String, String)> {
            nodes
                .iter()
                .filter(|(r, tt_e)| believed(r.tt_s, *tt_e) && r.vt_s <= t && t < r.vt_e)
                .map(|(r, _)| (uid(r.uid_id), (r.vid, r.label.clone(), r.props.clone())))
                .collect()
        };
        let estate = |t: i64| -> BTreeMap<String, (Id96, String, String, String, String)> {
            edges
                .iter()
                .filter(|(r, tt_e)| believed(r.tt_s, *tt_e) && r.vt_s <= t && t < r.vt_e)
                .map(|(r, _)| {
                    let (s, d) = (uid(r.src_id), uid(r.dst_id));
                    (
                        edge_eid(&s, &d, &r.rel_type, &r.disc).to_hex(),
                        (r.vid, r.props.clone(), s, d, r.rel_type.clone()),
                    )
                })
                .collect()
        };
        let (n1, n2, e1, e2) = (nstate(t1), nstate(t2), estate(t1), estate(t2));
        let minus = |a: &BTreeSet<&String>, b: &BTreeSet<&String>| -> Vec<String> {
            a.difference(b).map(|s| s.to_string()).collect()
        };
        let (k1, k2): (BTreeSet<_>, BTreeSet<_>) = (n1.keys().collect(), n2.keys().collect());
        let (f1, f2): (BTreeSet<_>, BTreeSet<_>) = (e1.keys().collect(), e2.keys().collect());
        let mut nc = Vec::new();
        for (u, (va, la, pa)) in &n1 {
            if let Some((vb, lb, pb)) = n2.get(u) {
                if va != vb && (la != lb || pa != pb) {
                    nc.push(u.clone());
                }
            }
        }
        let mut ec = Vec::new();
        for (e, (va, pa, ..)) in &e1 {
            if let Some((vb, pb, ..)) = e2.get(e) {
                if va != vb && pa != pb {
                    ec.push(e.clone());
                }
            }
        }
        (minus(&k2, &k1), minus(&k1, &k2), minus(&f2, &f1), minus(&f1, &f2), nc, ec)
    }

    /// End to end against the reference, over interval and instantaneous
    /// rows, a props correction that keeps the identity (a candidate), one
    /// that rewrites the same text (not a candidate), a label change, closes
    /// that make belief time matter, and every page boundary of each list.
    #[test]
    fn the_engine_diff_is_the_reference_diff() {
        let root = tmp_root("e2e");
        let mut s = NativeStore::open(&root).unwrap();
        let (p, _) = {
            let (p, t) = s.layout();
            (*p, t)
        };
        s.set_layout(p, 4096);
        let mut nodes: Vec<(NodeRow, i64)> = Vec::new();
        let mut edges: Vec<(EdgeRow, i64)> = Vec::new();
        s.begin(100).unwrap();
        let ids: Vec<u32> = (0..40).map(|k| s.ensure_entity(&format!("n{}", (k * 17) % 41), "L").unwrap()).collect();
        let name = |k: usize| format!("n{}", (k * 17) % 41);
        for (k, &id) in ids.iter().enumerate() {
            let (vs, ve) = ((k as i64 % 7) * 10, if k % 3 == 0 { OPEN_END } else { 50 + k as i64 });
            let r = NodeRow {
                vid: version_vid(&name(k), 100, vs),
                uid_id: id,
                label: "L".into(),
                vt_s: vs,
                vt_e: ve,
                tt_s: 100,
                props: format!("{{\"k\":{k}}}"),
                source: "ingest".into(),
                provenance_ref: None,
            };
            s.stage_node(r.clone()).unwrap();
            nodes.push((r, OPEN_END));
        }
        for i in 0..600u32 {
            let (a, b) = ((i * 7 % 40) as usize, (i * 13 % 40) as usize);
            let disc = format!("#{i}");
            let rel = if i % 4 == 0 { "S" } else { "R" };
            let eid = edge_eid(&name(a), &name(b), rel, &disc);
            let vs = (i % 90) as i64;
            let ve = if i % 5 == 0 { vs + 1 } else { vs + 1 + (i % 40) as i64 };
            let r = EdgeRow {
                vid: version_vid(&eid.to_hex(), 100, vs),
                src_id: ids[a],
                dst_id: ids[b],
                rel_type: rel.into(),
                disc,
                vt_s: vs,
                vt_e: ve,
                tt_s: 100,
                props: format!("{{\"i\":{}}}", i % 3),
                source: "ingest".into(),
                provenance_ref: None,
            };
            if s.stage_edge(r.clone()).is_ok() {
                edges.push((r, OPEN_END));
            }
        }
        s.commit(EventLogRef::default()).unwrap();
        // corrections at tt=200: split some edges at vt=40 into two versions,
        // the right half with new props (every other one) or the same text
        s.begin(200).unwrap();
        let mut fresh = Vec::new();
        for k in (0..edges.len()).step_by(9) {
            let (r, _) = edges[k].clone();
            if !(r.vt_s < 40 && 40 < r.vt_e) {
                continue;
            }
            s.close_version_at(RowKind::Edge, r.vid, Some(r.vt_s), 200).unwrap();
            edges[k].1 = 200;
            let eid = edge_eid(s.dict().uid(r.src_id).unwrap(), s.dict().uid(r.dst_id).unwrap(), &r.rel_type, &r.disc).to_hex();
            let left = EdgeRow { vid: version_vid(&eid, 200, r.vt_s), vt_e: 40, tt_s: 200, ..r.clone() };
            let props = if k % 2 == 0 { "{\"i\":99}".to_string() } else { r.props.clone() };
            let right = EdgeRow { vid: version_vid(&eid, 200, 40), vt_s: 40, tt_s: 200, props, ..r.clone() };
            for x in [left, right] {
                s.stage_edge(x.clone()).unwrap();
                fresh.push((x, OPEN_END));
            }
        }
        // a node relabelled from vt=30 on
        let (r, _) = nodes[1].clone();
        assert!(r.vt_s < 30 && 30 < r.vt_e, "the relabelled node must straddle vt=30");
        {
            s.close_version_at(RowKind::Node, r.vid, Some(r.vt_s), 200).unwrap();
            nodes[1].1 = 200;
            let u = name(1);
            let left = NodeRow { vid: version_vid(&u, 200, r.vt_s), vt_e: 30, tt_s: 200, ..r.clone() };
            let right = NodeRow { vid: version_vid(&u, 200, 30), vt_s: 30, tt_s: 200, label: "M".into(), ..r.clone() };
            for x in [left, right] {
                s.stage_node(x.clone()).unwrap();
                nodes.push((x, OPEN_END));
            }
        }
        s.commit(EventLogRef::default()).unwrap();
        edges.extend(fresh);
        let uid = |id: u32| s.dict().uid(id).unwrap().to_string();
        let mut nonempty = [0usize; 6];
        for as_of in [150, OPEN_END] {
            for (t1, t2) in [(10, 45), (0, 60), (35, 41), (45, 10), (20, 20), (80, 200)] {
                let want = reference(&nodes, &edges, &uid, t1, t2, as_of);
                let full = s.diff_snapshots(t1, t2, as_of, usize::MAX).unwrap().unwrap();
                let got = (
                    full.nodes_added.clone(),
                    full.nodes_removed.clone(),
                    full.edges_added.iter().map(|d| d.eid.clone()).collect::<Vec<_>>(),
                    full.edges_removed.iter().map(|d| d.eid.clone()).collect::<Vec<_>>(),
                    full.node_cands.iter().map(|c| c.uid.clone()).collect::<Vec<_>>(),
                    full.edge_cands.iter().map(|c| c.eid.clone()).collect::<Vec<_>>(),
                );
                assert_eq!(got, want, "as_of={as_of} t1={t1} t2={t2}");
                for (k, v) in [&want.0, &want.1, &want.2, &want.3, &want.4, &want.5].iter().enumerate() {
                    nonempty[k] += usize::from(!v.is_empty());
                }
                assert_eq!(
                    (full.nodes_added_total, full.nodes_removed_total, full.edges_added_total, full.edges_removed_total),
                    (want.0.len(), want.1.len(), want.2.len(), want.3.len())
                );
                // a description names the identity it describes
                let all: Vec<&(EdgeRow, i64)> = edges.iter().collect();
                for d in full.edges_added.iter().chain(&full.edges_removed) {
                    assert!(all.iter().any(|(r, _)| uid(r.src_id) == d.src
                        && uid(r.dst_id) == d.dst
                        && r.rel_type == d.rel_type
                        && edge_eid(&d.src, &d.dst, &d.rel_type, &r.disc).to_hex() == d.eid));
                }
                for limit in [0, 1, 2, 5] {
                    let page = s.diff_snapshots(t1, t2, as_of, limit).unwrap().unwrap();
                    let cut = |v: &Vec<String>| v[..limit.min(v.len())].to_vec();
                    assert_eq!(page.nodes_added, cut(&full.nodes_added));
                    assert_eq!(page.nodes_removed, cut(&full.nodes_removed));
                    assert_eq!(page.edges_added, full.edges_added[..limit.min(full.edges_added.len())].to_vec());
                    assert_eq!(page.edges_removed, full.edges_removed[..limit.min(full.edges_removed.len())].to_vec());
                    assert_eq!(page.edges_added_total, full.edges_added_total);
                    assert_eq!(page.node_cands, full.node_cands, "candidates are never paged");
                }
            }
        }
        assert!(nonempty.iter().all(|&n| n > 0), "every list must be exercised: {nonempty:?}");
        let _ = std::fs::remove_dir_all(&root);
    }
}
