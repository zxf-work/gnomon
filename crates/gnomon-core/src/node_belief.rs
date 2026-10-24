//! How many currently believed node versions each dense entity id has — the
//! structure that answers bulk ingest's endpoint-existence question.
//!
//! **The question.** Before a bulk batch creates bare node versions for its
//! endpoints, `_ingest_events` asks which of them already have a node version
//! believed *now*. Over a build of `n` edges that is ~2n questions, and the
//! two ways the store could answer — materialize every stored node version,
//! or ask every live node segment's identity postings per endpoint — both
//! grow with the store, so the probe went from 5 % of ingest time at 10M rows
//! to 58 % at 1B (measured by the per-phase ingest breakdown).
//!
//! **The structure.** Node identity is already dense: every stored node row
//! carries a `uid_id` that the dictionary maps one-to-one onto its uid. So
//! the answer for current belief is a count per dense id — the number of rows
//! in the live node segments whose effective `tt_e` is `OPEN_END` — and a
//! question costs one dictionary lookup and one array read. It is the
//! unique-key index of a relational engine, specialised to dense keys.
//!
//! **Derived, never persisted, and maintained from the probe side only.** It
//! is not updated by commit or compaction. Each probe reconciles it against
//! what the handle's current manifest names, by difference:
//!
//! * a node segment the manifest names and the structure does not is read
//!   once (its `uid_id` column and its sidecar) and its open rows added;
//! * a node segment the structure holds and the manifest no longer names
//!   (compaction input, whole-store or current-only compaction) has its
//!   counted rows subtracted from the copy kept here — the file itself may
//!   already be gone;
//! * when the committed close index is not the one last seen, every live
//!   node segment's closes are re-diffed against the rows already
//!   subtracted for close runs, in both directions.
//!
//! Whatever produced the generation — a commit, any compaction, recovery
//! replay, a format upgrade — the diff is against the manifest, so no path
//! outside the probe needs to know this exists, and nothing written to disk
//! changes. A handle builds it at its first current-belief probe.
//!
//! **What "counted" means**, exactly as `read.rs::effective_tt_e` resolves
//! it: a row counts when its sidecar `tt_e` is `OPEN_END` and no committed
//! close run gives it a `tt_e` other than `OPEN_END` (a run entry whose
//! `tt_e` is `OPEN_END` has nothing to say, and the sidecar answers). The
//! open batch is never folded in: its closes and staged rows are applied by
//! the caller as a per-call overlay, which a rollback simply forgets.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::error::{EngineError, Result};
use crate::visibility::CloseIndex;
use crate::OPEN_END;

/// One segment as `reconcile` reads it: the `uid_id` column and the
/// sidecar's folded closes, sorted by row.
pub type SegmentLoad = (Vec<u32>, Vec<(u32, i64)>);

/// One live node segment's contribution, kept so it can be subtracted after
/// the segment leaves the manifest.
struct SegmentRows {
    /// `uid_id` of every row, in row order.
    uid: Vec<u32>,
    /// One bit per row: open according to the segment's own sidecar.
    open: Vec<u64>,
    /// Sorted rows that are open in the sidecar but closed by a committed
    /// close run, and so have been subtracted.
    run_closed: Vec<u32>,
}

impl SegmentRows {
    fn new(uid: Vec<u32>, sidecar_closed: &[(u32, i64)]) -> Self {
        let rows = uid.len();
        let mut open = vec![u64::MAX; rows.div_ceil(64)];
        if !rows.is_multiple_of(64) {
            if let Some(last) = open.last_mut() {
                *last = (1u64 << (rows % 64)) - 1;
            }
        }
        for &(row, tt_e) in sidecar_closed {
            if tt_e != OPEN_END && (row as usize) < rows {
                open[(row / 64) as usize] &= !(1u64 << (row % 64));
            }
        }
        Self {
            uid,
            open,
            run_closed: Vec::new(),
        }
    }

    #[inline]
    fn sidecar_open(&self, row: u32) -> bool {
        (row as usize) < self.uid.len() && self.open[(row / 64) as usize] & (1u64 << (row % 64)) != 0
    }

    #[inline]
    fn counted(&self, row: u32) -> bool {
        self.sidecar_open(row) && self.run_closed.binary_search(&row).is_err()
    }

    fn counted_rows(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.uid.len() as u32).filter(move |&r| self.counted(r))
    }

    fn heap_bytes(&self) -> u64 {
        (self.uid.capacity() * 4 + self.open.capacity() * 8 + self.run_closed.capacity() * 4) as u64
    }
}

/// Counters for receipts and tests: what the structure has done on this
/// handle. Counts, not timings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeBeliefStats {
    /// Probes that reconciled the structure against the manifest.
    pub reconciles: u64,
    /// Node segments read and added.
    pub segments_added: u64,
    /// Node segments subtracted after leaving the manifest.
    pub segments_removed: u64,
    /// Rows read from added segments.
    pub rows_read: u64,
    /// Times the close runs were re-diffed because the committed close index
    /// changed (or a segment was added).
    pub close_resyncs: u64,
    /// Node segments currently held.
    pub resident_segments: u64,
    /// Rows currently counted: the sum of every count.
    pub counted_rows: u64,
    /// Rows currently subtracted because a committed close run closes them.
    pub run_closed_rows: u64,
}

/// The per-dense-id count of believed node versions. See the module doc.
#[derive(Default)]
pub struct NodeBelief {
    counts: Vec<u32>,
    segments: HashMap<u64, SegmentRows>,
    /// The committed close index the close lists were last diffed against.
    /// Weak, so holding it never forces `committed_close_index` to copy the
    /// index when it extends it in place; an index extended that way, or
    /// dropped, no longer upgrades and so reads as changed.
    closes_seen: Option<Weak<CloseIndex>>,
    stats: NodeBeliefStats,
}

impl NodeBelief {
    fn add(&mut self, id: u32) {
        let i = id as usize;
        if i >= self.counts.len() {
            self.counts.resize(i + 1, 0);
        }
        self.counts[i] += 1;
        self.stats.counted_rows += 1;
    }

    fn sub(&mut self, id: u32) -> Result<()> {
        match self.counts.get_mut(id as usize) {
            Some(c) if *c > 0 => {
                *c -= 1;
                self.stats.counted_rows -= 1;
                Ok(())
            }
            _ => Err(EngineError::invariant(format!(
                "believed-node count for dense id {id} would go below zero"
            ))),
        }
    }

    /// Believed node versions of `id` in the committed generation last
    /// reconciled.
    #[inline]
    pub fn count(&self, id: u32) -> u32 {
        self.counts.get(id as usize).copied().unwrap_or(0)
    }

    /// The dense id of a committed row this structure currently counts, or
    /// `None` (not a live node segment, out of range, or already closed).
    pub fn counted_uid(&self, segment: u64, row: u32) -> Option<u32> {
        let s = self.segments.get(&segment)?;
        s.counted(row).then(|| s.uid[row as usize])
    }

    pub fn stats(&self) -> NodeBeliefStats {
        NodeBeliefStats {
            resident_segments: self.segments.len() as u64,
            ..self.stats
        }
    }

    pub fn heap_bytes(&self) -> u64 {
        let segs: u64 = self.segments.values().map(SegmentRows::heap_bytes).sum();
        let map = self.segments.capacity()
            * (std::mem::size_of::<u64>() + std::mem::size_of::<SegmentRows>());
        self.counts.capacity() as u64 * 4 + segs + map as u64
    }

    /// Bring the counts to the generation described by `live` (node segment
    /// id → file) and `closes` (its committed close index).
    ///
    /// `load(file)` returns one segment's `uid_id` column and its sidecar's
    /// folded closes (sorted by row). On an error nothing is left
    /// half-applied that a later call would trust: the caller drops the
    /// structure.
    pub fn reconcile(
        &mut self,
        live: &HashMap<u64, String>,
        closes: &Arc<CloseIndex>,
        mut load: impl FnMut(&str) -> Result<SegmentLoad>,
    ) -> Result<()> {
        self.stats.reconciles += 1;

        // 1. segments that left the manifest
        let gone: Vec<u64> = self
            .segments
            .keys()
            .filter(|id| !live.contains_key(id))
            .copied()
            .collect();
        for id in gone {
            let seg = self.segments.remove(&id).expect("listed above");
            for r in seg.counted_rows() {
                self.sub(seg.uid[r as usize])?;
            }
            self.stats.run_closed_rows -= seg.run_closed.len() as u64;
            self.stats.segments_removed += 1;
        }

        // 2. segments that joined it, in id order so a replay adds in the
        //    same order (the counts do not depend on it; the error, if any,
        //    names the same segment)
        let mut added: Vec<(&u64, &String)> =
            live.iter().filter(|(id, _)| !self.segments.contains_key(id)).collect();
        added.sort_unstable_by_key(|(id, _)| **id);
        let any_added = !added.is_empty();
        for (&id, file) in added {
            let (uid, sidecar) = load(file)?;
            let seg = SegmentRows::new(uid, &sidecar);
            self.stats.rows_read += seg.uid.len() as u64;
            for r in seg.counted_rows() {
                self.add(seg.uid[r as usize]);
            }
            self.segments.insert(id, seg);
            self.stats.segments_added += 1;
        }

        // 3. close runs, when the committed index is not the one last seen
        let same_closes = self
            .closes_seen
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|seen| Arc::ptr_eq(&seen, closes));
        if !same_closes || any_added {
            self.resync_closes(closes)?;
            self.closes_seen = Some(Arc::downgrade(closes));
            self.stats.close_resyncs += 1;
        }
        Ok(())
    }

    fn resync_closes(&mut self, closes: &CloseIndex) -> Result<()> {
        let ids: Vec<u64> = self.segments.keys().copied().collect();
        for id in ids {
            let seg = &self.segments[&id];
            let desired: Vec<u32> = if closes.touches(id) {
                closes
                    .segment_closes(id)
                    .iter()
                    .filter(|&&(row, tt_e)| tt_e != OPEN_END && seg.sidecar_open(row))
                    .map(|&(row, _)| row)
                    .collect()
            } else {
                Vec::new()
            };
            if desired == seg.run_closed {
                continue;
            }
            // both sorted by row: newly closed rows are subtracted, rows no
            // longer closed (never expected — a close is not withdrawn — but
            // exact either way) are added back
            let (mut newly, mut undone) = (Vec::new(), Vec::new());
            let (old, new) = (&seg.run_closed, &desired);
            let (mut i, mut j) = (0usize, 0usize);
            while i < old.len() || j < new.len() {
                if j == new.len() || (i < old.len() && old[i] < new[j]) {
                    undone.push(seg.uid[old[i] as usize]);
                    i += 1;
                } else if i == old.len() || new[j] < old[i] {
                    newly.push(seg.uid[new[j] as usize]);
                    j += 1;
                } else {
                    i += 1;
                    j += 1;
                }
            }
            let (before, after) = (old.len() as u64, new.len() as u64);
            for u in newly {
                self.sub(u)?;
            }
            for u in undone {
                self.add(u);
            }
            self.stats.run_closed_rows = self.stats.run_closed_rows - before + after;
            self.segments.get_mut(&id).expect("listed above").run_closed = desired;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(ids: &[u64]) -> HashMap<u64, String> {
        ids.iter().map(|&i| (i, format!("seg/{i}.tgs"))).collect()
    }

    #[test]
    fn open_bits_cover_exactly_the_rows() {
        for rows in [0usize, 1, 63, 64, 65, 130] {
            let s = SegmentRows::new(vec![0; rows], &[]);
            assert_eq!(s.counted_rows().count(), rows);
            assert!(!s.sidecar_open(rows as u32));
        }
    }

    #[test]
    fn adds_subtracts_and_follows_closes() {
        let mut nb = NodeBelief::default();
        let data: HashMap<String, SegmentLoad> = [
            ("seg/1.tgs".to_string(), (vec![5, 6, 5], vec![(1, 90)])),
            ("seg/2.tgs".to_string(), (vec![6, 7], vec![])),
        ]
        .into_iter()
        .collect();
        let load = |f: &str| Ok(data[f].clone());
        let empty = Arc::new(CloseIndex::default());
        nb.reconcile(&live(&[1, 2]), &empty, load).unwrap();
        assert_eq!((nb.count(5), nb.count(6), nb.count(7), nb.count(99)), (2, 1, 1, 0));

        // a committed run closes seg 2 row 0 (uid 6), and names a sidecar-
        // closed row and an OPEN_END entry, neither of which changes anything
        let closes = Arc::new(CloseIndex::from_records([
            crate::visibility::CloseRecord { kind: crate::row::RowKind::Node, segment: 2, row: 0, tt_e: 100 },
            crate::visibility::CloseRecord { kind: crate::row::RowKind::Node, segment: 1, row: 1, tt_e: 100 },
            crate::visibility::CloseRecord { kind: crate::row::RowKind::Node, segment: 1, row: 2, tt_e: OPEN_END },
        ]));
        nb.reconcile(&live(&[1, 2]), &closes, load).unwrap();
        assert_eq!((nb.count(5), nb.count(6), nb.count(7)), (2, 0, 1));
        assert_eq!(nb.counted_uid(2, 0), None);
        assert_eq!(nb.counted_uid(2, 1), Some(7));

        // segment 2 leaves the manifest: its counted row goes, its closed one
        // was already out
        nb.reconcile(&live(&[1]), &closes, load).unwrap();
        assert_eq!((nb.count(5), nb.count(6), nb.count(7)), (2, 0, 0));
        assert_eq!(nb.stats().counted_rows, 2);
        assert_eq!(nb.stats().run_closed_rows, 0);
    }

    #[test]
    fn an_index_that_is_not_the_one_seen_is_rediffed() {
        let mut nb = NodeBelief::default();
        let load = |_: &str| Ok((vec![1u32, 2], Vec::new()));
        let a = Arc::new(CloseIndex::default());
        nb.reconcile(&live(&[1]), &a, load).unwrap();
        let r0 = nb.stats().close_resyncs;
        nb.reconcile(&live(&[1]), &a, load).unwrap();
        assert_eq!(nb.stats().close_resyncs, r0, "same index, nothing added: no re-diff");
        let b = Arc::new(CloseIndex::default());
        nb.reconcile(&live(&[1]), &b, load).unwrap();
        assert_eq!(nb.stats().close_resyncs, r0 + 1);
    }

    #[test]
    fn a_count_that_would_go_negative_is_an_invariant_error() {
        let mut nb = NodeBelief::default();
        assert!(nb.sub(3).is_err());
    }
}
