//! Windowed point read: the believed versions of one identity that overlap a
//! valid-time window, found by seeking instead of by materializing the
//! identity's whole history.
//!
//! The write path asks this question once per operation. `assert`, `correct`
//! and `retract` each need the believed versions of one key that overlap the
//! valid time they write, and they used to fetch *every* believed version of
//! the key and filter in Python. A key that is updated at successive valid
//! times keeps every earlier interval believed — that is its valid-time
//! history — so under a skewed update stream the hottest keys carried tens
//! of thousands of believed versions, each decoded, converted and discarded
//! per write (the `vt=now` update series).
//!
//! **How it seeks.** A segment is sorted by `(vt_s, vid)` and its identity
//! postings list one key's rows in row order, so within a segment the key's
//! rows are in `vt_s` order. The rows that can overlap `[lo, hi)` are a
//! prefix of that list — those with `vt_s < hi` — found by binary search.
//! For current belief the prefix is walked backwards: rows already closed are
//! skipped (and pruned from the open bitset, exactly as
//! [`NativeStore::believed_node_versions`] does), and the walk stops at the
//! first *believed* row that ends at or before `lo`. Stopping there is sound
//! because the believed versions of one identity are pairwise disjoint in
//! valid time (the store's disjointness invariant, which every write op
//! maintains by carving), so a believed row that starts earlier also ends
//! earlier. The cost per segment is `O(log k + overlapping + closed rows
//! passed)` instead of `O(k)` materialized rows.
//!
//! **Exactness.** The postings key is a 64-bit identity prefix. A row of a
//! *different* identity sharing the prefix could only matter if it were the
//! row the walk stopped at, so the stopping row is materialized and checked
//! too; if it belongs to another identity the call answers by the full
//! history instead. Historical belief (`as_of_tt` before now) does not stop
//! early — closed rows are believed at some earlier time, so disjointness
//! says nothing about them — but still decodes only rows inside the window.
//!
//! The answer is exactly `believed_*_versions(identity, as_of_tt)` filtered
//! to the versions with `vt_s < hi && vt_e > lo`, in the same `(vt_s, vid)`
//! order, staged rows of the open batch and its pending closes included. The
//! oracle tests below hold it to that.

use crate::derive::Id96;
use crate::error::Result;
use crate::read::{uid_key, EdgeVersionOut, NodeVersionOut};
use crate::row::RowKind;
use crate::store::{segment_id_of, NativeStore, OpenMode};
use crate::{believed_at, OPEN_END};

/// Rows chosen from one segment, plus the row the backward walk stopped at
/// (current belief only), which the caller checks for a prefix collision.
struct Picked {
    file: String,
    rows: Vec<u32>,
    stop: Option<u32>,
}

impl NativeStore {
    /// Believed node versions of `uid` at `as_of_tt` with `vt_s < vt_hi` and
    /// `vt_e > vt_lo`, ordered by `(vt_s, vid)`. An empty window answers
    /// nothing.
    pub fn believed_node_versions_overlapping(
        &self,
        uid: &str,
        as_of_tt: i64,
        vt_lo: i64,
        vt_hi: i64,
    ) -> Result<Vec<NodeVersionOut>> {
        self.assert_full_belief(as_of_tt)?;
        if vt_lo >= vt_hi {
            return Ok(Vec::new());
        }
        let _ctx = crate::ingest_phase::CtxGuard::enter_if_unclaimed(crate::ingest_phase::Ctx::Correct);
        let t = crate::ingest_phase::Timer::start();
        let picked = self.window_rows(RowKind::Node, uid_key(uid), as_of_tt, vt_lo, vt_hi)?;
        t.stop_ctx(crate::ingest_phase::S::Locate);
        let t = crate::ingest_phase::Timer::start();
        let closes = self.close_index()?;
        let mut out = Vec::new();
        for p in picked {
            let id = segment_id_of(&p.file);
            let mut sel = p.rows.clone();
            if let Some(s) = p.stop {
                sel.push(s);
                sel.sort_unstable();
            }
            for (r, row) in self
                .node_rows_sel(&p.file, id, &closes, Some(&sel), OpenMode::Cached)?
                .into_iter()
                .zip(sel.iter().copied())
            {
                if Some(row) == p.stop {
                    if r.uid != uid {
                        // the walk stopped on another identity's row: answer
                        // from the whole history rather than trust the stop
                        t.stop_ctx(crate::ingest_phase::S::RowsMaterialize);
                        return self.overlapping_nodes_by_history(uid, as_of_tt, vt_lo, vt_hi);
                    }
                    continue;
                }
                if r.uid == uid && believed_at(r.tt_s, r.tt_e, as_of_tt) && r.vt_s < vt_hi && r.vt_e > vt_lo {
                    out.push(r);
                }
            }
        }
        t.stop_ctx(crate::ingest_phase::S::RowsMaterialize);
        let t = crate::ingest_phase::Timer::start();
        out.extend(self.staged_node_versions_for(uid)?.into_iter().filter(|r| {
            r.uid == uid && believed_at(r.tt_s, r.tt_e, as_of_tt) && r.vt_s < vt_hi && r.vt_e > vt_lo
        }));
        t.stop_ctx(crate::ingest_phase::S::Staged);
        out.sort_by_key(|r| (r.vt_s, r.vid.clone()));
        Ok(out)
    }

    /// Believed edge versions of `eid` at `as_of_tt` overlapping
    /// `[vt_lo, vt_hi)`, ordered by `(vt_s, vid)`.
    pub fn believed_edge_versions_overlapping(
        &self,
        eid: &str,
        as_of_tt: i64,
        vt_lo: i64,
        vt_hi: i64,
    ) -> Result<Vec<EdgeVersionOut>> {
        self.assert_full_belief(as_of_tt)?;
        let key = Id96::from_hex(eid).map(|i| i.hi)?;
        if vt_lo >= vt_hi {
            return Ok(Vec::new());
        }
        let _ctx = crate::ingest_phase::CtxGuard::enter_if_unclaimed(crate::ingest_phase::Ctx::Correct);
        let t = crate::ingest_phase::Timer::start();
        let picked = self.window_rows(RowKind::Edge, key, as_of_tt, vt_lo, vt_hi)?;
        t.stop_ctx(crate::ingest_phase::S::Locate);
        let t = crate::ingest_phase::Timer::start();
        let closes = self.close_index()?;
        let mut out = Vec::new();
        for p in picked {
            let id = segment_id_of(&p.file);
            let mut sel = p.rows.clone();
            if let Some(s) = p.stop {
                sel.push(s);
                sel.sort_unstable();
            }
            for (r, row) in self
                .edge_rows_sel(&p.file, id, &closes, Some(&sel), OpenMode::Cached)?
                .into_iter()
                .zip(sel.iter().copied())
            {
                if Some(row) == p.stop {
                    if r.eid != eid {
                        t.stop_ctx(crate::ingest_phase::S::RowsMaterialize);
                        return self.overlapping_edges_by_history(eid, as_of_tt, vt_lo, vt_hi);
                    }
                    continue;
                }
                if r.eid == eid && believed_at(r.tt_s, r.tt_e, as_of_tt) && r.vt_s < vt_hi && r.vt_e > vt_lo {
                    out.push(r);
                }
            }
        }
        t.stop_ctx(crate::ingest_phase::S::RowsMaterialize);
        let t = crate::ingest_phase::Timer::start();
        out.extend(self.staged_edge_versions_for(key)?.into_iter().filter(|r| {
            r.eid == eid && believed_at(r.tt_s, r.tt_e, as_of_tt) && r.vt_s < vt_hi && r.vt_e > vt_lo
        }));
        t.stop_ctx(crate::ingest_phase::S::Staged);
        out.sort_by_key(|r| (r.vt_s, r.vid.clone()));
        Ok(out)
    }

    /// The same answer from the identity's whole believed history: the
    /// reference the seek is held to, and its fallback on a prefix collision.
    pub fn overlapping_nodes_by_history(
        &self,
        uid: &str,
        as_of_tt: i64,
        vt_lo: i64,
        vt_hi: i64,
    ) -> Result<Vec<NodeVersionOut>> {
        Ok(self
            .believed_node_versions(uid, as_of_tt)?
            .into_iter()
            .filter(|r| r.vt_s < vt_hi && r.vt_e > vt_lo)
            .collect())
    }

    /// Edge counterpart of [`Self::overlapping_nodes_by_history`].
    pub fn overlapping_edges_by_history(
        &self,
        eid: &str,
        as_of_tt: i64,
        vt_lo: i64,
        vt_hi: i64,
    ) -> Result<Vec<EdgeVersionOut>> {
        Ok(self
            .believed_edge_versions(eid, as_of_tt)?
            .into_iter()
            .filter(|r| r.vt_s < vt_hi && r.vt_e > vt_lo)
            .collect())
    }

    /// Per segment, the committed rows of identity prefix `key` that can
    /// overlap `[lo, hi)` at `as_of_tt` (module docs).
    fn window_rows(&self, kind: RowKind, key: u64, as_of_tt: i64, lo: i64, hi: i64) -> Result<Vec<Picked>> {
        let current = Self::is_current_belief(as_of_tt);
        // committed closes only, as `locate_open` uses: the open batch's
        // overlay must never prune the bitset (it may roll back), so its
        // closes are applied as a filter instead
        let committed = self.committed_close_index()?;
        let none = std::collections::HashSet::new();
        let closed_here = if current { self.pending_closed_rows(kind) } else { &none };
        // The manifest's zone map first. A segment none of whose rows
        // starts before `hi` or ends after `lo` holds no row of any identity
        // that overlaps the window, so it is skipped before its identity
        // filter and postings are consulted -- the same `vt_min`/`vt_e_max`
        // test the scan planner and the windowed reads prune by, under the
        // same switch (`GNOMON_SEGMENT_PRUNE=0` walks every segment, as before).
        // Without it every point read paid a filter probe per segment of the
        // store, so its cost grew with the store (43 us a read in SF1
        // correction transactions at a 10 % correction rate).
        let prune = self.designs().segment_prune;
        let m = self.manifest();
        let entries: Vec<&crate::manifest::SegmentEntry> = match kind {
            RowKind::Edge => m.edge_lanes.event.iter().chain(m.edge_lanes.interval.iter()).collect(),
            RowKind::Node => m.node_store.iter().collect(),
        };
        let mut out = Vec::new();
        for entry in entries {
            if prune && (entry.vt_min >= hi || entry.vt_e_max <= lo) {
                continue;
            }
            let file = &entry.file;
            let seg_id = segment_id_of(file);
            let verdict = self.filter_verdict(kind, seg_id, file, key);
            if verdict == crate::store::FilterVerdict::Negative {
                continue;
            }
            let n = self.with_segment_postings(kind, seg_id, file, |e| e.rows_for(key).len())?;
            if n == 0 {
                self.note_filter_empty_positive(kind, verdict);
                continue;
            }
            let seg = self.open_segment(file)?;
            let vt_s = seg.i64_column("vt_s")?;
            let vt_e_col = if seg.header().vt_e_elided {
                None
            } else {
                Some(seg.i64_column("vt_e")?)
            };
            let vt_e = |r: u32| vt_e_col.map_or(vt_s[r as usize] + 1, |c| c[r as usize]);
            // historical belief only: the row's tt_s and its committed tt_e
            let hist = if current {
                None
            } else {
                Some((seg.tt_s_view()?, seg.sidecar()))
            };
            let picked = self.with_segment_postings(kind, seg_id, file, |e| {
                let rows = e.rows_for(key);
                // rows are in row order, hence in `vt_s` order
                let p = rows.partition_point(|&r| vt_s[r as usize] < hi);
                let mut keep = Vec::new();
                let mut stop = None;
                let mut prune = Vec::new();
                if current {
                    for &r in rows[..p].iter().rev() {
                        if !e.is_open(r) {
                            continue;
                        }
                        if committed.tt_e(seg_id, r) != OPEN_END {
                            prune.push(r);
                            continue;
                        }
                        if closed_here.contains(&(seg_id, r)) {
                            continue;
                        }
                        if vt_e(r) > lo {
                            keep.push(r);
                        } else {
                            stop = Some(r);
                            break;
                        }
                    }
                    for r in prune {
                        e.mark_closed(r);
                    }
                } else {
                    // A row believed at a historical `as_of_tt` was written by
                    // then and not closed by then. Both are read from the
                    // segment's columns and the committed close index, so a
                    // key whose rows all overlap the window (a transaction-
                    // time-only history, every version valid from 0) no longer
                    // materialises every one of them to keep one. The open
                    // batch cannot matter here: its closes and rows carry a
                    // transaction time past the frontier, so past any
                    // historical `as_of_tt`. The materialised rows are still
                    // tested in full by the caller.
                    let (tt_s, sidecar) = hist.as_ref().expect("historical belief");
                    keep.extend(rows[..p].iter().copied().filter(|&r| {
                        vt_e(r) > lo
                            && tt_s.at(r).map_or(true, |t| t <= as_of_tt)
                            && crate::read::effective_tt_e(&committed, sidecar, seg_id, r)
                                > as_of_tt
                    }));
                }
                keep.reverse();
                Picked {
                    file: file.clone(),
                    rows: keep,
                    stop,
                }
            })?;
            if !picked.rows.is_empty() || picked.stop.is_some() {
                out.push(picked);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derive::{edge_eid, version_vid};
    use crate::manifest::EventLogRef;
    use crate::row::{EdgeRow, NodeRow};
    use std::path::PathBuf;

    fn tmp_root(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-pointread-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn node(uid: &str, uid_id: u32, vt_s: i64, vt_e: i64, tt: i64, p: u32) -> NodeRow {
        NodeRow {
            vid: version_vid(uid, tt, vt_s),
            uid_id,
            label: "P".into(),
            vt_s,
            vt_e,
            tt_s: tt,
            props: format!(r#"{{"p":{p}}}"#),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    fn edge(a: u32, b: u32, disc: &str, vt_s: i64, vt_e: i64, tt: i64) -> EdgeRow {
        let eid = edge_eid("u0", "u1", "K", disc);
        EdgeRow {
            vid: version_vid(&eid.to_hex(), tt, vt_s),
            src_id: a,
            dst_id: b,
            rel_type: "K".into(),
            disc: disc.into(),
            vt_s,
            vt_e,
            tt_s: tt,
            props: "{}".into(),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    /// A small bi-temporal history with the shapes that matter: a key updated
    /// at successive valid times (the remainder and the new version both
    /// believed, the old one closed), whole-line replacements, several
    /// segments, incremental compaction folding closes, and an open batch
    /// with staged rows and pending closes.
    fn history(name: &str, compact_every: u32) -> (PathBuf, NativeStore, Vec<i64>) {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        let mut tt = 10i64;
        s.begin(tt).unwrap();
        let ids: Vec<u32> = (0..4).map(|i| s.ensure_entity(&format!("u{i}"), "P").unwrap()).collect();
        let mut open: Vec<Vec<NodeRow>> = vec![Vec::new(); 4];
        for (i, &id) in ids.iter().enumerate() {
            let r = node(&format!("u{i}"), id, 0, OPEN_END, tt, 0);
            open[i].push(r.clone());
            s.stage_node(r).unwrap();
        }
        let mut edges = Vec::new();
        for d in 0..6 {
            let r = edge(ids[0], ids[1], &format!("#{d}"), 0, OPEN_END, tt);
            edges.push(r.clone());
            s.stage_edge(r).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();
        let mut tts = vec![tt];
        for k in 1..=160u32 {
            tt += 1;
            s.begin(tt).unwrap();
            let i = (k % 4) as usize;
            let uid = format!("u{i}");
            // the open-ended version of u_i, split at valid time 100*k (or
            // replaced on the whole line every 7th update)
            let cur = open[i].pop().unwrap();
            s.close_version_at(RowKind::Node, cur.vid, Some(cur.vt_s), tt).unwrap();
            let at = if k % 7 == 0 { cur.vt_s } else { 100 * k as i64 };
            if at > cur.vt_s {
                let rem = node(&uid, ids[i], cur.vt_s, at, tt, k);
                s.stage_node(rem).unwrap();
            }
            let new = node(&uid, ids[i], at, OPEN_END, tt, k + 1000);
            open[i].push(new.clone());
            s.stage_node(new).unwrap();
            if k % 5 == 0 {
                let e = edges[(k as usize / 5) % edges.len()].clone();
                if e.vt_e > 50 * k as i64 && e.vt_s < 50 * k as i64 {
                    s.close_version_at(RowKind::Edge, e.vid, Some(e.vt_s), tt).unwrap();
                    let r = edge(e.src_id, e.dst_id, &e.disc, e.vt_s, 50 * k as i64, tt);
                    s.stage_edge(r.clone()).unwrap();
                    let n = edge(e.src_id, e.dst_id, &e.disc, 50 * k as i64, OPEN_END, tt);
                    s.stage_edge(n.clone()).unwrap();
                    let j = edges.iter().position(|x| x.vid == e.vid).unwrap();
                    edges[j] = n;
                }
            }
            s.commit(EventLogRef::default()).unwrap();
            tts.push(tt);
            if compact_every > 0 && k % compact_every == 0 {
                while s.needs_compaction() {
                    s.compact_incremental(None).unwrap();
                }
            }
        }
        // an open batch: one more split of u1, staged and with a pending close
        tt += 1;
        s.begin(tt).unwrap();
        let cur = open[1].pop().unwrap();
        s.close_version_at(RowKind::Node, cur.vid, Some(cur.vt_s), tt).unwrap();
        s.stage_node(node("u1", ids[1], cur.vt_s, 99_999, tt, 7)).unwrap();
        s.stage_node(node("u1", ids[1], 99_999, OPEN_END, tt, 8)).unwrap();
        tts.push(tt);
        (root, s, tts)
    }

    fn windows() -> Vec<(i64, i64)> {
        let mut w = vec![(0, OPEN_END), (0, 1), (99_999, OPEN_END), (16_000, OPEN_END), (-5, 0)];
        for lo in [0i64, 50, 99, 100, 101, 250, 777, 3_200, 8_000, 15_999, 16_000, 20_000] {
            for len in [1i64, 2, 99, 100, 500, 4_000, OPEN_END / 2] {
                w.push((lo, lo.saturating_add(len)));
            }
        }
        w
    }

    fn check(s: &NativeStore, tts: &[i64]) {
        let mut asofs = vec![OPEN_END];
        asofs.extend(tts.iter().step_by(13).copied());
        for as_of in asofs {
            for i in 0..5 {
                let uid = format!("u{i}");
                for &(lo, hi) in &windows() {
                    let got = s.believed_node_versions_overlapping(&uid, as_of, lo, hi).unwrap();
                    let want = s.overlapping_nodes_by_history(&uid, as_of, lo, hi).unwrap();
                    assert_eq!(got, want, "{uid} as_of {as_of} window [{lo}, {hi})");
                }
            }
            for d in 0..6 {
                let eid = edge_eid("u0", "u1", "K", &format!("#{d}")).to_hex();
                for &(lo, hi) in &windows() {
                    let got = s.believed_edge_versions_overlapping(&eid, as_of, lo, hi).unwrap();
                    let want = s.overlapping_edges_by_history(&eid, as_of, lo, hi).unwrap();
                    assert_eq!(got, want, "edge #{d} as_of {as_of} window [{lo}, {hi})");
                }
            }
        }
    }

    #[test]
    fn the_window_read_equals_the_filtered_history_inside_an_open_batch() {
        let (_root, s, tts) = history("open-batch", 0);
        check(&s, &tts);
    }

    /// The open batch's closed-row set is kept beside its closes: it
    /// names exactly the committed rows they closed (a second close of a row
    /// adds nothing), the window read inside the batch stays the filtered
    /// history, and both ends of a batch empty it.
    #[test]
    fn the_open_batch_closed_set_follows_its_closes_and_clears() {
        let (_root, mut s, tts) = history("closed-set", 0);
        let tt = *tts.last().unwrap();
        assert_eq!(s.pending_closed_rows(RowKind::Node).len(), 1, "history's open batch closed u1");
        let open: Vec<(Id96, i64)> = ["u0", "u2", "u3"]
            .iter()
            .map(|uid| {
                let v: Vec<_> = s
                    .believed_node_versions(uid, OPEN_END)
                    .unwrap()
                    .into_iter()
                    .filter(|v| v.vt_e == OPEN_END && v.tt_s < tt)
                    .collect();
                assert_eq!(v.len(), 1, "{uid}");
                (Id96::from_hex(&v[0].vid).unwrap(), v[0].vt_s)
            })
            .collect();
        for (k, &(vid, vt_s)) in open.iter().enumerate() {
            s.close_version_at(RowKind::Node, vid, Some(vt_s), tt).unwrap();
            assert_eq!(s.pending_closed_rows(RowKind::Node).len(), 2 + k);
        }
        // the same committed row closed again: one row, however many closes
        let _ = s.close_version_at(RowKind::Node, open[0].0, Some(open[0].1), tt);
        assert_eq!(s.pending_closed_rows(RowKind::Node).len(), 4);
        assert!(s.pending_closed_rows(RowKind::Edge).is_empty());
        check(&s, &tts);
        s.rollback().unwrap();
        assert!(s.pending_closed_rows(RowKind::Node).is_empty(), "rollback empties it");
        s.begin(tt).unwrap();
        let v = s.believed_node_versions("u2", OPEN_END).unwrap().into_iter()
            .find(|v| v.vt_e == OPEN_END).unwrap();
        s.close_version_at(RowKind::Node, Id96::from_hex(&v.vid).unwrap(), Some(v.vt_s), tt).unwrap();
        assert_eq!(s.pending_closed_rows(RowKind::Node).len(), 1);
        s.stage_node(node("u2", s.dict().dense_id("u2").unwrap(), v.vt_s, OPEN_END, tt, 9)).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        assert!(s.pending_closed_rows(RowKind::Node).is_empty(), "commit empties it");
    }

    #[test]
    fn the_window_read_equals_the_filtered_history_across_compactions() {
        let (_root, mut s, tts) = history("compacted", 9);
        check(&s, &tts);
        s.rollback().unwrap();
        check(&s, &tts[..tts.len() - 1]);
        while s.needs_compaction() {
            s.compact_incremental(None).unwrap();
        }
        check(&s, &tts[..tts.len() - 1]);
        s.compact().unwrap();
        check(&s, &tts[..tts.len() - 1]);
    }

    #[test]
    fn the_window_read_equals_the_filtered_history_after_reopen() {
        let (root, mut s, tts) = history("reopen", 4);
        s.rollback().unwrap();
        drop(s);
        let s = NativeStore::open_read_only(&root).unwrap();
        check(&s, &tts[..tts.len() - 1]);
    }

    #[test]
    fn a_hot_key_is_answered_without_decoding_its_history() {
        // one key split 600 times: the window read must decode a handful of
        // rows where the whole history decodes thousands
        let root = tmp_root("hot");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(1).unwrap();
        let id = s.ensure_entity("h", "P").unwrap();
        let mut cur = node("h", id, 0, OPEN_END, 1, 0);
        s.stage_node(cur.clone()).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        for k in 1..=600i64 {
            let tt = 1 + k;
            s.begin(tt).unwrap();
            s.close_version_at(RowKind::Node, cur.vid, Some(cur.vt_s), tt).unwrap();
            s.stage_node(node("h", id, cur.vt_s, 10 * k, tt, k as u32)).unwrap();
            cur = node("h", id, 10 * k, OPEN_END, tt, k as u32);
            s.stage_node(cur.clone()).unwrap();
            s.commit(EventLogRef::default()).unwrap();
            if k % 25 == 0 {
                while s.needs_compaction() {
                    s.compact_incremental(None).unwrap();
                }
            }
        }
        let full = s.believed_node_versions("h", OPEN_END).unwrap();
        assert_eq!(full.len(), 601);
        let picked = s.window_rows(RowKind::Node, uid_key("h"), OPEN_END, 6_001, OPEN_END).unwrap();
        let decoded: usize = picked.iter().map(|p| p.rows.len() + usize::from(p.stop.is_some())).sum();
        assert!(decoded <= 2 * picked.len().max(1) + 1, "decoded {decoded} rows over {} segments", picked.len());
        let got = s.believed_node_versions_overlapping("h", OPEN_END, 6_001, OPEN_END).unwrap();
        assert_eq!(got, s.overlapping_nodes_by_history("h", OPEN_END, 6_001, OPEN_END).unwrap());
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].vt_s, 6_000);
    }
}
