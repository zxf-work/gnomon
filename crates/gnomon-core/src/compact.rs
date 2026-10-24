//! Compaction: fold close runs into segment sidecars and merge runs.
//!
//! **Compaction never drops a row.** A closed version is still the answer to
//! "what did we believe before the correction?", and historical belief
//! queries are the entire point of the system. Folding changes only *where*
//! a `tt_e` is written — from a standalone run file into the owning segment's
//! sidecar — never whether the row exists. That is what the equivalence test
//! below pins: the full logical listing and a sample of historical queries
//! must be byte-identical either side of a compaction.
//!
//! The whole-store path is the minimal implementation: it reads the logical
//! content back through the ordinary read path and re-seals it. That is
//! obviously correct (it reuses tested code) and bounded by memory rather
//! than by store size. A streaming, partition-at-a-time merge is the natural
//! successor once a store outgrows RAM; the file format does not change.
//!
//! Old segment files are left on disk: an older manifest may still reference
//! them, and a reader holding that generation must keep working. Reclaiming
//! them is generation collection's job (gc.rs), which removes a file only
//! once no retained generation names it.

use std::collections::{HashMap, HashSet};

use crate::derive::Id96;
use crate::error::{EngineError, Result};
use crate::manifest::{EdgeLanes, SegmentEntry};
use crate::ingest_phase::{self, Ctx, CtxGuard, Timer, G};
use crate::merge;
use crate::row::{EdgeRow, Lane, NodeRow};
use crate::staging::Staging;
use crate::store::NativeStore;
use crate::visibility::CloseIndex;
use crate::{defaults, OPEN_END};

/// Fraction of a generation's close-run records that may name dead segments
/// before the list is rewritten.
///
/// Dead records cost correctness nothing — they can never resolve — so the
/// only question is bookkeeping: below the threshold, reading and indexing
/// them is cheaper than rewriting the list and forcing the close cache's
/// full-rebuild path. Half is the design's own figure.
const CLOSE_RUN_DEAD_FRACTION: f64 = 0.5;

/// Fan-in of the close-run merge: a trailing stretch of this many runs whose
/// size tiers (`merge::size_tier`, base 8) are all at most `t` is merged into
/// one run, for the smallest `t` that has one.
///
/// Every commit that closes a committed row appends a one-record run, and
/// before this merge only the dead-fraction rule above ever shortened the
/// list. Closes against long-lived segments stay live until their segment is
/// folded at `COMPACTION_CLOSE_FRACTION` of its rows, so the list grew to one
/// entry per closing commit — tens of thousands — and every compaction
/// re-sealed, re-checkpointed and re-adopted all of it.
/// With the merge, every maximal trailing
/// stretch of runs at tier `<= t` is shorter than this, so the list holds
/// fewer than `CLOSE_RUN_FAN_IN` runs per tier, and a record is rewritten
/// about once per tier it climbs.
const CLOSE_RUN_FAN_IN: usize = 8;

/// What one compaction call did.
///
/// The last four fields exist so that compaction cost is measured rather than
/// estimated from checkpoint deltas, which carry no per-compaction timer:
/// merge rows/s and the total rows rewritten over a build are read from
/// these, accumulated per call by the build driver into `build-record.json`.
///
/// `rows_rewritten` counts rows **written to new segments** — for the
/// whole-store path that is every row in the store, for the incremental path
/// only the rows of the clusters it chose. It is deliberately not
/// `edge_rows + node_rows` in general: those report the *logical* store size
/// on the whole-store path, and there the two coincide only because it
/// rewrites everything.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct CompactionReport {
    pub segments_before: usize,
    pub segments_after: usize,
    pub closes_folded: usize,
    pub edge_rows: usize,
    pub node_rows: usize,
    /// Rows copied into freshly written segments by this call.
    pub rows_rewritten: u64,
    /// Key-range clusters rewritten. 0 on the whole-store path, which does
    /// not plan by cluster.
    pub clusters_touched: usize,
    /// Wall time of the call, measured inside the engine.
    pub wall_us: u64,
    /// Whether this was the partition-local path.
    pub incremental: bool,
}

impl NativeStore {
    /// Is any key-range cluster eligible for an incremental rewrite?
    ///
    /// Compaction stays explicit (`gnomon store compact`), never a background
    /// schedule — a single writer has no one to coordinate with and surprise
    /// rewrites are worse than a stale layout. What changed with incremental
    /// compaction is what the hint *means*. It used to be `!close_runs.is_empty() || segments >
    /// 4`: deliberately coarse, "it over-triggers on wide stores, which is
    /// the safe direction for a hint", and therefore useless as a driver
    /// trigger — `build_synth_store.py` ignored it and fired on an op counter
    /// instead, which is the `n²/(2P)` rewrite cost of a fixed cadence.
    ///
    /// It is now exactly the predicate `compact_incremental` plans against,
    /// so a driver can loop on it without either over-compacting or
    /// diverging from what the compactor will actually do. It opens no
    /// segment: the plan is arithmetic over the manifest's key ranges and row
    /// counts plus the already-cached close index.
    pub fn needs_compaction(&self) -> bool {
        let t = Timer::start();
        let out = self.plan_incremental().map(|p| !p.is_empty()).unwrap_or(false);
        t.stop(G::TriggerNs);
        ingest_phase::add(G::TriggerCalls, 1);
        out
    }

    /// The advisory hint for the whole-store path: any unfolded close run, or
    /// more than [`defaults::COMPACTION_RUNS_TRIGGER`] segments.
    ///
    /// Kept because it is the predicate the whole-store `compact()` pairs
    /// with, and `compact()` survives as the comparison baseline for the
    /// incremental path. A baseline that consulted the incremental path's
    /// trigger would not be a baseline.
    pub fn needs_whole_store_compaction(&self) -> bool {
        let m = self.manifest();
        let segments =
            m.edge_lanes.event.len() + m.edge_lanes.interval.len() + m.node_store.len();
        !m.close_runs.is_empty() || segments > defaults::COMPACTION_RUNS_TRIGGER as usize
    }

    /// Plan the incremental rewrites the current manifest calls for, per lane.
    ///
    /// No segment is opened: `key_lo`/`key_hi`/`rows`
    /// carry everything the cluster grouping and the triggers need, and the
    /// close counts come from the committed close index the store already
    /// caches per generation.
    pub(crate) fn plan_incremental(&self) -> Result<Vec<merge::MergeSet>> {
        let closes = self.committed_close_index()?;
        let m = self.manifest();
        let unfolded = merge::unfolded_by_segment(&closes, m);
        let lookup = |id: u64| unfolded.get(&id).copied().unwrap_or(0);
        let (_, target_bytes) = self.layout();
        // `plan_lane`'s "already at the byte target" skip must use the same
        // accounted target the writer flushes on, or a correctly sized
        // segment reads as half-full and gets dragged through a merge
        // (`merge::full_rows`).
        let ratios = self.seal_ratios();
        let mut out = Vec::new();
        for (slot, entries) in [
            (merge::LaneSlot::EdgeEvent, &m.edge_lanes.event),
            (merge::LaneSlot::EdgeInterval, &m.edge_lanes.interval),
            (merge::LaneSlot::Node, &m.node_store),
        ] {
            let effective = ratios.effective_target(slot, target_bytes);
            out.extend(merge::plan_lane(entries, slot, effective, &lookup));
        }
        Ok(out)
    }

    /// Rewrite only the key-range clusters that need it.
    ///
    /// `budget_rows` caps one call's work; `None` takes every eligible
    /// cluster. The logical store is unchanged — every row is copied, `tt_e`
    /// is relocated from a close run into the new segment's sidecar, and
    /// transaction time does not advance — so `store_digest` is invariant by
    /// construction.
    ///
    /// What this does *not* do, and why:
    ///
    /// * **It does not touch untouched segments.** Their files, `body_sha`,
    ///   sidecars, and therefore their `idx/tcsr/*.tci` and
    ///   `idx/name/*.tnx` all survive — both index kinds are keyed on the
    ///   segment's own `body_sha` (TCSR reads it from the footer; the name
    ///   index and the identity filter from the manifest entry, which carries
    ///   it; older entries carry `""`). A whole-store compaction
    ///   orphans every index file on every cycle; keeping them is the main
    ///   benefit of the incremental path.
    /// * **It does not empty `close_runs`.** The whole-store path can, since
    ///   it folds every close; a partial rewrite cannot, because one run may
    ///   name both rewritten and surviving segments. Records naming a dead
    ///   segment are harmless — `CloseIndex` is keyed `(segment, row)` and a
    ///   dead id is no longer in the file map — but they accumulate, so the
    ///   list is rewritten past a 0.5 dead fraction (step 3).
    /// * **It publishes a full checkpoint**, as `install` always has.
    ///   A splice is neither an append nor a removal the delta
    ///   record can express, and teaching the chain to express removals
    ///   would change what gc's reference set assumes. One manifest per
    ///   compaction is ~100 MB over a 100M build.
    pub fn compact_incremental(&mut self, budget_rows: Option<u64>) -> Result<CompactionReport> {
        if self.in_batch() {
            return Err(EngineError::invariant(
                "cannot compact while a batch is open",
            ));
        }
        self.require_writable_handle()?;
        let started = std::time::Instant::now();
        let _ctx = CtxGuard::enter(Ctx::Compact);
        let total_t = Timer::start();
        ingest_phase::add(G::IncCalls, 1);
        let segments_before = {
            let m = self.manifest();
            m.edge_lanes.event.len() + m.edge_lanes.interval.len() + m.node_store.len()
        };
        let plan_t = Timer::start();
        let plans = merge::take_within_budget(self.plan_incremental()?, budget_rows);
        plan_t.stop(G::IncPlanNs);
        if plans.is_empty() {
            total_t.stop(G::IncTotalNs);
            return Ok(CompactionReport {
                segments_before,
                segments_after: segments_before,
                incremental: true,
                wall_us: started.elapsed().as_micros() as u64,
                ..Default::default()
            });
        }

        let ci_t = Timer::start();
        let closes = self.committed_close_index()?;
        ci_t.stop(G::IncCloseIndexNs);
        // A physical rewrite: no belief changed, so tt does not advance (I5).
        let merge_t = Timer::start();
        let mut next = self.manifest().successor(self.manifest().created_tt);
        let mut next_id = next.next_segment_id;
        let target_bytes = {
            let (_, t) = self.layout();
            t
        };

        let mut rows_rewritten = 0u64;
        let mut closes_folded = 0usize;
        let mut replacements: Vec<(merge::LaneSlot, Vec<SegmentEntry>)> = Vec::new();
        let mut superseded: HashSet<String> = HashSet::new();
        // seeded once from the committed manifest and then carried across
        // every set this call rewrites, so the second group of a multi-group
        // compaction is corrected by the first group's own output
        let mut ratios = self.seal_ratios();

        for set in &plans {
            let files: Vec<(String, u64)> = {
                let m = self.manifest();
                let lane_entries = match set.slot {
                    merge::LaneSlot::EdgeEvent => &m.edge_lanes.event,
                    merge::LaneSlot::EdgeInterval => &m.edge_lanes.interval,
                    merge::LaneSlot::Node => &m.node_store,
                };
                set.members
                    .iter()
                    .map(|&i| {
                        let f = lane_entries[i].file.clone();
                        let id = crate::store::segment_id_of(&f);
                        (f, id)
                    })
                    .collect()
            };
            let out = match set.slot {
                merge::LaneSlot::Node => self.merge_node_cluster(
                    &files,
                    target_bytes,
                    &mut ratios,
                    &closes,
                    &mut next_id,
                )?,
                _ => self.merge_edge_cluster(
                    &files,
                    set.slot.lane(),
                    target_bytes,
                    &mut ratios,
                    &closes,
                    &mut next_id,
                )?,
            };
            rows_rewritten += out.rows;
            closes_folded += out.closes_folded;
            for (f, _) in &files {
                superseded.insert(f.clone());
            }
            replacements.push((set.slot, out.entries));
        }
        merge_t.stop(G::IncMergeNs);
        let splice_t = Timer::start();

        // Splice: drop the merged entries wherever they sat and append their
        // replacements. Lane vectors are not held in key order — a commit
        // appends, and successive commits interleave — so there is no
        // contiguous range to splice into, and nothing downstream depends on
        // the vector's order: the scan sorts by key and the digest never sees
        // a segment.
        for (slot, entries) in replacements {
            let lane = match slot {
                merge::LaneSlot::EdgeEvent => &mut next.edge_lanes.event,
                merge::LaneSlot::EdgeInterval => &mut next.edge_lanes.interval,
                merge::LaneSlot::Node => &mut next.node_store,
            };
            lane.retain(|e| !superseded.contains(&e.file));
            lane.extend(entries);
        }
        next.next_segment_id = next_id;

        // Step 3 — retire close-run records that now name only dead segments,
        // or, when that rule does not fire, merge the list's tail.
        let carried = if self.retire_close_runs(&mut next, &closes)? {
            None
        } else {
            self.merge_close_run_tail(&mut next, closes)?
        };

        splice_t.stop(G::IncSpliceNs);
        // Stats are unchanged: no row was created or destroyed (I1, I5).
        let seal_t = Timer::start();
        next.seal();
        seal_t.stop(G::IncManifestSealNs);
        let segments_after =
            next.edge_lanes.event.len() + next.edge_lanes.interval.len() + next.node_store.len();
        crate::store::crash_point("compact_before_install");
        self.install_carrying(next, carried)?;
        ingest_phase::add(G::IncPublishNs, self.install_ns.0);
        ingest_phase::add(G::IncAdoptNs, self.install_ns.1);
        ingest_phase::add(G::IncVidfilterNs, self.install_ns.2);
        ingest_phase::add(G::IncRowsRewritten, rows_rewritten);
        total_t.stop(G::IncTotalNs);

        Ok(CompactionReport {
            segments_before,
            segments_after,
            closes_folded,
            edge_rows: 0,
            node_rows: 0,
            rows_rewritten,
            clusters_touched: plans.len(),
            wall_us: started.elapsed().as_micros() as u64,
            incremental: true,
        })
    }

    /// Rewrite the close-run list down to the records that still name a live
    /// segment, when more than half of them no longer do.
    ///
    /// Returns whether it rewrote. The threshold is what keeps this from
    /// running every cycle: the list is immutable once written, so a rewrite
    /// costs one read of every run plus one new file, and it also forces the
    /// next `committed_close_index` onto its full-rebuild path — the close
    /// cache reuses its prediction only across an *append*, and says why:
    /// "`close_runs` is **not** append-only across compaction".
    ///
    /// The old run files stay on disk. A retained generation still names
    /// them, and gc removes a file only once no retained manifest does.
    fn retire_close_runs(&self, next: &mut crate::manifest::Manifest, closes: &CloseIndex) -> Result<bool> {
        if next.close_runs.is_empty() {
            return Ok(false);
        }
        let live = merge::live_segment_ids(next);
        if merge::dead_close_fraction(closes, &live) <= CLOSE_RUN_DEAD_FRACTION {
            return Ok(false);
        }
        let mut surviving: Vec<crate::visibility::CloseRecord> = Vec::new();
        for run in &next.close_runs {
            for rec in crate::visibility::read_close_run(&self.root().join(&run.file))? {
                if live.contains(&rec.segment) {
                    surviving.push(rec);
                }
            }
        }
        if surviving.is_empty() {
            next.close_runs = Vec::new();
            return Ok(true);
        }
        let file = format!("close/{:012}.tgc", next.generation);
        let entries = crate::visibility::write_close_run(&self.root().join(&file), &surviving)?;
        next.close_runs = vec![crate::manifest::CloseRunRef {
            file,
            entries,
            sha: String::new(),
        }];
        Ok(true)
    }

    /// Merge the trailing close runs [`plan_close_run_merge`] picks into one,
    /// dropping the records that name segments `next` no longer lists.
    ///
    /// Returns the committed close index of `next`, carried over from
    /// `closes` (the index of the generation being compacted, whose run list
    /// `next` still has) rather than rebuilt: the merged run holds exactly
    /// the replaced runs' records minus the dropped ones, so the new index is
    /// the old one with those records removed — O(dropped), where a rebuild
    /// would re-read every run. `None` when nothing was merged, which leaves
    /// the close cache's ordinary append-only reuse to carry the index.
    ///
    /// The merged run is written and fsynced before the manifest that names
    /// it; the replaced runs stay on disk for older generations until gc. A
    /// crash in between leaves an unreferenced file, which gc collects like
    /// any other orphan run.
    fn merge_close_run_tail(
        &self,
        next: &mut crate::manifest::Manifest,
        closes: std::sync::Arc<CloseIndex>,
    ) -> Result<Option<std::sync::Arc<CloseIndex>>> {
        let entries: Vec<u64> = next.close_runs.iter().map(|r| u64::from(r.entries)).collect();
        let Some(from) = plan_close_run_merge(&entries, CLOSE_RUN_FAN_IN) else {
            return Ok(None);
        };
        let live = merge::live_segment_ids(next);
        let mut kept: Vec<crate::visibility::CloseRecord> = Vec::new();
        let mut dropped: Vec<(u64, u32)> = Vec::new();
        for run in &next.close_runs[from..] {
            for rec in crate::visibility::read_close_run(&self.root().join(&run.file))? {
                if live.contains(&rec.segment) {
                    kept.push(rec);
                } else {
                    dropped.push((rec.segment, rec.row));
                }
            }
        }
        next.close_runs.truncate(from);
        if !kept.is_empty() {
            let file = format!("close/{:012}.tgc", next.generation);
            let n = crate::visibility::write_close_run(&self.root().join(&file), &kept)?;
            next.close_runs.push(crate::manifest::CloseRunRef {
                file,
                entries: n,
                sha: String::new(),
            });
        }
        crate::store::crash_point("compact_after_close_merge");
        // The cache holds a second reference to this very index; dropping it
        // first lets `make_mut` edit in place instead of copying every close.
        self.forget_close_cache();
        let mut idx = closes;
        let target = std::sync::Arc::make_mut(&mut idx);
        target.remove_rows(dropped);
        Ok(Some(idx))
    }

    /// Rewrite the store's content into fresh segments with every close
    /// folded in. Publishes a new generation and returns what it did.
    pub fn compact(&mut self) -> Result<CompactionReport> {
        if self.in_batch() {
            return Err(EngineError::invariant(
                "cannot compact while a batch is open",
            ));
        }
        self.require_writable_handle()?;
        let started = std::time::Instant::now();
        let m = self.manifest();
        let segments_before =
            m.edge_lanes.event.len() + m.edge_lanes.interval.len() + m.node_store.len();
        if segments_before == 0 {
            return Ok(CompactionReport::default());
        }
        let _ctx = CtxGuard::enter(Ctx::Compact);
        let total_t = Timer::start();
        ingest_phase::add(G::FullCalls, 1);

        // Read the logical content back — closes already applied — and stage
        // it again. Every `tt_e` that is not open becomes a folded close.
        let read_t = Timer::start();
        let edges = self.all_edge_versions()?;
        let nodes = self.all_node_versions()?;
        read_t.stop(G::FullReadNs);
        let restage_t = Timer::start();
        let mut staging = Staging::default();
        let mut closes: HashMap<Id96, i64> = HashMap::new();

        for e in &edges {
            let vid = Id96::from_hex(&e.vid)?;
            if e.tt_e != OPEN_END {
                closes.insert(vid, e.tt_e);
            }
            staging.push_edge(EdgeRow {
                vid,
                src_id: self.dense_or_err(&e.src)?,
                dst_id: self.dense_or_err(&e.dst)?,
                rel_type: e.rel_type.clone(),
                disc: e.disc.clone(),
                vt_s: e.vt_s,
                vt_e: e.vt_e,
                tt_s: e.tt_s,
                props: e.props.clone(),
                source: e.source.clone(),
                provenance_ref: e.provenance_ref.clone(),
            });
        }
        for n in &nodes {
            let vid = Id96::from_hex(&n.vid)?;
            if n.tt_e != OPEN_END {
                closes.insert(vid, n.tt_e);
            }
            staging.push_node(NodeRow {
                vid,
                uid_id: self.dense_or_err(&n.uid)?,
                label: n.label.clone(),
                vt_s: n.vt_s,
                vt_e: n.vt_e,
                tt_s: n.tt_s,
                props: n.props.clone(),
                source: n.source.clone(),
                provenance_ref: n.provenance_ref.clone(),
            });
        }

        restage_t.stop(G::FullRestageNs);
        // Compaction is a physical rewrite, so it does not advance
        // transaction time — no belief changed.
        let seal_t = Timer::start();
        let mut next = self.manifest().successor(self.manifest().created_tt);
        let mut next_id = next.next_segment_id;
        let (partitions, target_bytes) = {
            let (p, t) = self.layout();
            (*p, t)
        };
        let mut ratios = self.seal_ratios_for_batch(staging.staged_rows());
        let sealed = staging.seal(
            &self.root().join("seg"),
            &partitions,
            target_bytes,
            &mut ratios,
            &mut next_id,
            &closes,
        )?;

        next.next_segment_id = next_id;
        next.edge_lanes = EdgeLanes::default();
        next.node_store = Vec::new();
        next.close_runs = Vec::new(); // folded into the new sidecars
        for (lane, entry) in sealed.edges {
            match lane {
                Lane::Event => next.edge_lanes.event.push(entry),
                Lane::Interval => next.edge_lanes.interval.push(entry),
            }
        }
        next.node_store = sealed.nodes;
        next.stats.n_edge_versions = edges.len() as u64;
        next.stats.n_node_versions = nodes.len() as u64;
        next.stats.n_entities = self.dict().len();
        seal_t.stop(G::FullSealNs);
        let mseal_t = Timer::start();
        next.seal();
        mseal_t.stop(G::FullManifestSealNs);

        let segments_after =
            next.edge_lanes.event.len() + next.edge_lanes.interval.len() + next.node_store.len();
        crate::store::crash_point("compact_before_install");
        self.install(next)?;
        ingest_phase::add(G::FullPublishNs, self.install_ns.0);
        ingest_phase::add(G::FullAdoptNs, self.install_ns.1);
        ingest_phase::add(G::FullVidfilterNs, self.install_ns.2);
        ingest_phase::add(G::FullRowsRewritten, (edges.len() + nodes.len()) as u64);
        total_t.stop(G::FullTotalNs);

        Ok(CompactionReport {
            segments_before,
            segments_after,
            closes_folded: closes.len(),
            edge_rows: edges.len(),
            node_rows: nodes.len(),
            // the whole-store path rewrites every row, by definition
            rows_rewritten: (edges.len() + nodes.len()) as u64,
            clusters_touched: 0,
            wall_us: started.elapsed().as_micros() as u64,
            incremental: false,
        })
    }

    /// The current-only configuration: rewrite the store keeping **only the
    /// currently believed rows**, then stamp it `CURRENT_ONLY`.
    ///
    /// This is the one deliberate exception to "compaction never drops a
    /// row", and it exists purely so the evaluation plan's current-versus-
    /// bi-temporal overhead question can be measured on a store that
    /// genuinely lacks historical versions, close runs, sidecars, and the
    /// postings entries that indexed them. It is not reachable from the
    /// default compaction path, and the marker it writes makes the store
    /// refuse past-belief queries and corrections from then on
    /// (`assert_full_belief`, `close_version`).
    ///
    /// The current-belief answer set is unchanged: a row believed now has
    /// `tt_e == OPEN_END` and is kept verbatim, with its vid, tt_s, and
    /// props intact — so current-belief queries must hash identically
    /// against the full store they came from. That equivalence is the
    /// experiment's correctness gate.
    pub fn compact_current_only(&mut self) -> Result<CompactionReport> {
        if self.in_batch() {
            return Err(EngineError::invariant(
                "cannot compact while a batch is open",
            ));
        }
        self.require_writable_handle()?;
        let started = std::time::Instant::now();
        let m = self.manifest();
        let segments_before =
            m.edge_lanes.event.len() + m.edge_lanes.interval.len() + m.node_store.len();
        if segments_before == 0 {
            self.mark_current_only()?;
            return Ok(CompactionReport::default());
        }

        // Read the logical content back and keep only what is believed now.
        // Everything else — superseded versions and the closes that hid
        // them — is dropped, so the sealed segments are all-current by
        // construction and no close map is passed to seal.
        let edges = self.all_edge_versions()?;
        let nodes = self.all_node_versions()?;
        let dropped_closes = edges.iter().filter(|e| e.tt_e != OPEN_END).count()
            + nodes.iter().filter(|n| n.tt_e != OPEN_END).count();
        let mut staging = Staging::default();
        let mut kept_edges = 0usize;
        let mut kept_nodes = 0usize;

        for e in &edges {
            if e.tt_e != OPEN_END {
                continue;
            }
            kept_edges += 1;
            staging.push_edge(EdgeRow {
                vid: Id96::from_hex(&e.vid)?,
                src_id: self.dense_or_err(&e.src)?,
                dst_id: self.dense_or_err(&e.dst)?,
                rel_type: e.rel_type.clone(),
                disc: e.disc.clone(),
                vt_s: e.vt_s,
                vt_e: e.vt_e,
                tt_s: e.tt_s,
                props: e.props.clone(),
                source: e.source.clone(),
                provenance_ref: e.provenance_ref.clone(),
            });
        }
        for n in &nodes {
            if n.tt_e != OPEN_END {
                continue;
            }
            kept_nodes += 1;
            staging.push_node(NodeRow {
                vid: Id96::from_hex(&n.vid)?,
                uid_id: self.dense_or_err(&n.uid)?,
                label: n.label.clone(),
                vt_s: n.vt_s,
                vt_e: n.vt_e,
                tt_s: n.tt_s,
                props: n.props.clone(),
                source: n.source.clone(),
                provenance_ref: n.provenance_ref.clone(),
            });
        }

        // A physical rewrite: no belief changed, so tt does not advance.
        let mut next = self.manifest().successor(self.manifest().created_tt);
        let mut next_id = next.next_segment_id;
        let (partitions, target_bytes) = {
            let (p, t) = self.layout();
            (*p, t)
        };
        let mut ratios = self.seal_ratios_for_batch(staging.staged_rows());
        let sealed = staging.seal(
            &self.root().join("seg"),
            &partitions,
            target_bytes,
            &mut ratios,
            &mut next_id,
            &HashMap::new(),
        )?;

        next.next_segment_id = next_id;
        next.edge_lanes = EdgeLanes::default();
        next.node_store = Vec::new();
        next.close_runs = Vec::new(); // nothing hidden remains to describe
        for (lane, entry) in sealed.edges {
            match lane {
                Lane::Event => next.edge_lanes.event.push(entry),
                Lane::Interval => next.edge_lanes.interval.push(entry),
            }
        }
        next.node_store = sealed.nodes;
        next.stats.n_edge_versions = kept_edges as u64;
        next.stats.n_node_versions = kept_nodes as u64;
        next.stats.n_entities = self.dict().len();
        next.seal();

        let segments_after =
            next.edge_lanes.event.len() + next.edge_lanes.interval.len() + next.node_store.len();
        self.install(next)?;
        self.mark_current_only()?;
        // Unlike default compaction this *changed* the logical content, so a
        // cached stats snapshot over all versions is now wrong. Postings need
        // no flush: `locate` resolves hits through the current manifest.
        *self.stats_cell().lock().expect("stats mutex poisoned") = None;
        *self.stats_summary_cell().lock().expect("stats summary mutex poisoned") = None;

        Ok(CompactionReport {
            segments_before,
            segments_after,
            closes_folded: dropped_closes,
            edge_rows: kept_edges,
            node_rows: kept_nodes,
            rows_rewritten: (kept_edges + kept_nodes) as u64,
            clusters_touched: 0,
            wall_us: started.elapsed().as_micros() as u64,
            incremental: false,
        })
    }

    fn dense_or_err(&self, uid: &str) -> Result<u32> {
        self.dict()
            .dense_id(uid)
            .ok_or_else(|| EngineError::invariant(format!("uid {uid:?} left the dictionary")))
    }
}

/// Where the close-run merge starts, or `None` when there is nothing to merge.
///
/// Simulated on entry counts, cascading: find the smallest tier `t` for which
/// the maximal trailing stretch of runs at tier `<= t` is at least `fan`
/// long, collapse that stretch into one run of the summed size, and repeat
/// until no tier has such a stretch. A cascade always swallows the previous
/// step's product (it is the last run), so the whole plan is one suffix and
/// one physical merge. Counts are upper bounds — the merge also drops dead
/// records — so the real product is never larger than planned.
pub(crate) fn plan_close_run_merge(entries: &[u64], fan: usize) -> Option<usize> {
    let fan = fan.max(2);
    let mut v: Vec<(usize, u64)> = entries.iter().copied().enumerate().collect();
    let mut start: Option<usize> = None;
    loop {
        if v.len() < fan {
            break;
        }
        let tiers: Vec<u32> = v.iter().map(|&(_, e)| merge::size_tier(e, fan as u64)).collect();
        let max_tier = tiers.iter().copied().max().unwrap_or(0);
        let stretch = (0..=max_tier)
            .map(|t| tiers.iter().rev().take_while(|&&x| x <= t).count())
            .find(|&len| len >= fan);
        let Some(len) = stretch else { break };
        let at = v.len() - len;
        let first = v[at].0;
        let sum: u64 = v[at..].iter().map(|&(_, e)| e).sum();
        v.truncate(at);
        v.push((first, sum));
        start = Some(start.map_or(first, |s| s.min(first)));
    }
    start
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derive::{edge_eid, version_vid};
    use crate::manifest::EventLogRef;
    use crate::read::EdgeVersionOut;
    use crate::row::RowKind;
    use std::path::PathBuf;

    fn tmp_root(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-compact-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn edge(src_id: u32, dst_id: u32, src: &str, dst: &str, vt_s: i64, tt_s: i64, i: u32) -> EdgeRow {
        let disc = format!("#{i}");
        let eid = edge_eid(src, dst, "R", &disc);
        EdgeRow {
            vid: version_vid(&eid.to_hex(), tt_s, vt_s),
            src_id,
            dst_id,
            rel_type: "R".into(),
            disc,
            vt_s,
            vt_e: vt_s + 1,
            tt_s,
            props: format!(r#"{{"i":{i}}}"#),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    fn sorted(mut v: Vec<EdgeVersionOut>) -> Vec<EdgeVersionOut> {
        v.sort_by(|a, b| a.vid.cmp(&b.vid));
        v
    }

    /// Several batches, then corrections against committed rows, so the store
    /// has both multiple segments and unfolded close runs.
    fn seeded(name: &str) -> (PathBuf, NativeStore, Vec<EdgeRow>) {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        let mut all = Vec::new();
        for batch in 0..3u32 {
            let tt = 100 + batch as i64;
            s.begin(tt).unwrap();
            let a = s.ensure_entity("n1", "Node").unwrap();
            let b = s.ensure_entity("n2", "Node").unwrap();
            for i in 0..4u32 {
                let r = edge(a, b, "n1", "n2", (batch * 10 + i) as i64, tt, batch * 10 + i);
                all.push(r.clone());
                s.stage_edge(r).unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        // correct two committed rows in a later batch
        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, all[1].vid, 200).unwrap();
        s.close_version(RowKind::Edge, all[7].vid, 200).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        (root, s, all)
    }

    #[test]
    fn compaction_preserves_the_logical_store_exactly() {
        let (_root, mut s, _) = seeded("equivalence");
        let before = sorted(s.all_edge_versions().unwrap());
        let gen_before = s.generation();

        let report = s.compact().unwrap();
        let after = sorted(s.all_edge_versions().unwrap());

        assert_eq!(before, after, "compaction changed the logical content");
        assert_eq!(report.edge_rows, before.len());
        assert!(s.generation() > gen_before, "compaction publishes a generation");
    }

    #[test]
    fn a_close_after_compaction_resolves_through_the_new_segments() {
        // the postings-vs-manifest staleness rule: `seeded`'s corrections
        // built the vid postings over the pre-compaction segments, and
        // compaction then replaced every file in the manifest. A later
        // correction must land in a surviving segment — a stale candidate
        // is filtered out before any file is opened.
        let (_root, mut s, all) = seeded("close-after-compact");
        s.compact().unwrap();

        s.begin(300).unwrap();
        s.close_version(RowKind::Edge, all[3].vid, 300).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        for r in s.all_edge_versions().unwrap() {
            let expected = if r.vid == all[3].vid.to_hex() {
                300
            } else if r.vid == all[1].vid.to_hex() || r.vid == all[7].vid.to_hex() {
                200
            } else {
                OPEN_END
            };
            assert_eq!(r.tt_e, expected, "vid {}", r.vid);
        }
    }

    #[test]
    fn historical_queries_agree_either_side_of_a_compaction() {
        let (_root, mut s, _) = seeded("historical");
        let sample = [99i64, 100, 101, 150, 199, 200, 201, OPEN_END];
        let before: Vec<Vec<String>> = sample
            .iter()
            .map(|&as_of| {
                let mut v: Vec<String> = s
                    .all_edge_versions()
                    .unwrap()
                    .into_iter()
                    .filter(|r| crate::believed_at(r.tt_s, r.tt_e, as_of))
                    .map(|r| r.vid)
                    .collect();
                v.sort();
                v
            })
            .collect();

        s.compact().unwrap();

        for (i, &as_of) in sample.iter().enumerate() {
            let mut v: Vec<String> = s
                .all_edge_versions()
                .unwrap()
                .into_iter()
                .filter(|r| crate::believed_at(r.tt_s, r.tt_e, as_of))
                .map(|r| r.vid)
                .collect();
            v.sort();
            assert_eq!(v, before[i], "belief at as_of={as_of} changed");
        }
    }

    /// Generations whose valid times interleave, which is what a live ingest
    /// stream produces and what `examples/scan_probe.rs` (one transaction
    /// time per segment) cannot. Compaction re-sorts these into global
    /// `(vt_s, vid)` order while each row keeps its origin `tt_s`, so the
    /// run list ends up interleaved too.
    fn interleaved(name: &str) -> (PathBuf, NativeStore) {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        for batch in 0..4u32 {
            let tt = 100 + batch as i64;
            s.begin(tt).unwrap();
            let a = s.ensure_entity("n1", "Node").unwrap();
            let b = s.ensure_entity("n2", "Node").unwrap();
            if batch == 0 {
                // one version per node identity, so a node point read has an
                // unambiguous answer either side of the compaction
                for (uid_id, uid) in [(a, "n1"), (b, "n2")] {
                    s.stage_node(NodeRow {
                        vid: version_vid(uid, tt, 0),
                        uid_id,
                        label: "Node".into(),
                        vt_s: 0,
                        vt_e: OPEN_END,
                        tt_s: tt,
                        props: "{}".into(),
                        source: "ingest".into(),
                        provenance_ref: None,
                    })
                    .unwrap();
                }
            }
            for i in 0..6u32 {
                // stride by the batch count so successive batches interleave
                let vt_s = (i * 4 + batch) as i64;
                s.stage_edge(edge(a, b, "n1", "n2", vt_s, tt, i * 4 + batch))
                    .unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        (root, s)
    }

    fn edge_run_counts(s: &NativeStore) -> Vec<usize> {
        s.manifest()
            .edge_lanes
            .event
            .iter()
            .chain(s.manifest().edge_lanes.interval.iter())
            .map(|e| s.open_segment(&e.file).unwrap().tt_s_run_count())
            .collect()
    }

    /// A store whose segments do not agree about how `tt_s` is stored.
    ///
    /// This is not a contrived state — it is the steady state. A batch writes
    /// one transaction time and stays on the run list; compaction shreds
    /// transaction time and crosses the density threshold, so its output is
    /// dictionary-coded. A store that has been compacted and then written to
    /// again holds both shapes at once, and the manifest does not record
    /// which is which. Every answer must be blind to the difference.
    #[test]
    fn a_store_mixing_both_tt_s_representations_answers_identically() {
        let (_root, mut s) = interleaved("mixed-repr");
        let sample = [99i64, 100, 101, 102, 103, OPEN_END];
        let believed = |s: &NativeStore| -> Vec<Vec<String>> {
            sample
                .iter()
                .map(|&as_of| {
                    let mut v: Vec<String> = s
                        .all_edge_versions()
                        .unwrap()
                        .into_iter()
                        .filter(|r| crate::believed_at(r.tt_s, r.tt_e, as_of))
                        .map(|r| r.vid)
                        .collect();
                    v.sort();
                    v
                })
                .collect()
        };
        let shapes = |s: &NativeStore| -> (usize, usize) {
            let m = s.manifest();
            m.edge_lanes
                .event
                .iter()
                .chain(m.edge_lanes.interval.iter())
                .chain(m.node_store.iter())
                .fold((0, 0), |(runs, dict), e| {
                    let seg = s.open_segment(&e.file).unwrap();
                    if seg.tt_s_view().unwrap().is_dictionary() {
                        (runs, dict + 1)
                    } else {
                        (runs + 1, dict)
                    }
                })
        };

        let (runs_before, dict_before) = shapes(&s);
        assert_eq!(dict_before, 0, "a batch-written store is all run list");
        assert!(runs_before > 0);

        s.compact().unwrap();
        let full_after_compact = sorted(s.all_edge_versions().unwrap());
        let believed_after_compact = believed(&s);
        let (_, dict_mid) = shapes(&s);
        assert!(dict_mid > 0, "compaction must produce a dictionary-coded segment");

        // write again: the new batch lands on the run list beside it
        s.begin(200).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let b = s.ensure_entity("n2", "Node").unwrap();
        for i in 0..6u32 {
            s.stage_edge(edge(a, b, "n1", "n2", 1_000 + i as i64, 200, 900 + i))
                .unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();

        let (runs_after, dict_after) = shapes(&s);
        assert!(
            runs_after > 0 && dict_after > 0,
            "the fixture must really be mixed: {runs_after} run-list, {dict_after} dictionary"
        );

        // everything that was true before the new batch is still true
        let full = sorted(s.all_edge_versions().unwrap());
        for row in &full_after_compact {
            assert!(full.contains(row), "a row went missing across the mixed state");
        }
        for (i, &as_of) in sample.iter().enumerate() {
            let mut now: Vec<String> = s
                .all_edge_versions()
                .unwrap()
                .into_iter()
                .filter(|r| crate::believed_at(r.tt_s, r.tt_e, as_of))
                .filter(|r| r.tt_s < 200)
                .map(|r| r.vid)
                .collect();
            now.sort();
            assert_eq!(now, believed_after_compact[i], "belief at {as_of} moved");
        }
        assert!(s.verify().unwrap().is_healthy(), "a mixed store must verify clean");
    }

    /// The read-path pathology, reproduced small: compaction really does
    /// produce a multi-run segment, and every answer is unchanged by it.
    #[test]
    fn compaction_interleaves_transaction_times_without_changing_answers() {
        let (_root, mut s) = interleaved("interleaved");
        assert!(
            edge_run_counts(&s).iter().all(|&n| n == 1),
            "a batch writes exactly one transaction time per segment"
        );
        let sample = [99i64, 100, 101, 102, 103, OPEN_END];
        let believed = |s: &NativeStore| -> Vec<Vec<String>> {
            sample
                .iter()
                .map(|&as_of| {
                    let mut v: Vec<String> = s
                        .all_edge_versions()
                        .unwrap()
                        .into_iter()
                        .filter(|r| crate::believed_at(r.tt_s, r.tt_e, as_of))
                        .map(|r| r.vid)
                        .collect();
                    v.sort();
                    v
                })
                .collect()
        };
        let full_before = sorted(s.all_edge_versions().unwrap());
        let believed_before = believed(&s);

        s.compact().unwrap();

        assert_eq!(
            sorted(s.all_edge_versions().unwrap()),
            full_before,
            "compaction changed the full listing"
        );
        assert_eq!(believed(&s), believed_before, "compaction changed a belief");
        let runs = edge_run_counts(&s);
        assert_eq!(runs.len(), 1, "24 rows compact into one segment");
        assert!(
            // strictly more than the four generations that fed it: the runs
            // interleave, they were not merely concatenated in tt order
            runs[0] > 4,
            "the repro is not real: the compacted segment has {} run(s) over \
             {} rows",
            runs[0],
            full_before.len()
        );
        // and verify surfaces it as a layout metric rather than a problem
        let report = s.verify().unwrap();
        assert!(report.is_healthy());
        assert_eq!(report.max_tt_s_runs as usize, runs.iter().copied().max().unwrap());
        assert!(report.tt_s_runs >= report.max_tt_s_runs as u64);
    }

    /// Compaction replaces every live segment file and leaves the old ones on
    /// disk (an older generation still names them), and `install` does not
    /// clear the identity postings. It does not have to: every candidate is
    /// filtered through the current generation's `segment id -> file` map
    /// before a file is opened, that map is rebuilt whenever the generation
    /// changes, and segment ids are never reused. So a point read after a
    /// compaction returns the new rows exactly once.
    #[test]
    fn point_reads_after_compaction_return_exactly_the_live_rows() {
        let (_root, mut s) = interleaved("postings-after-compact");
        let eid = edge_eid("n1", "n2", "R", "#5").to_hex();
        // warm the postings against the pre-compaction segments
        let before = s.believed_edge_versions(&eid, OPEN_END).unwrap();
        assert_eq!(before.len(), 1);
        let before_uid = s.believed_node_versions("n1", OPEN_END).unwrap();
        assert_eq!(before_uid.len(), 1);

        s.compact().unwrap();

        let after = s.believed_edge_versions(&eid, OPEN_END).unwrap();
        assert_eq!(after, before, "the identity resolved to different rows");
        assert_eq!(
            s.believed_node_versions("n1", OPEN_END).unwrap(),
            before_uid
        );
        // every identity, not just the warmed one, and never twice
        for r in sorted(s.all_edge_versions().unwrap()) {
            let got = s.believed_edge_versions(&r.eid, OPEN_END).unwrap();
            assert_eq!(got.len(), 1, "eid {} resolved to {} rows", r.eid, got.len());
            assert_eq!(got[0], r);
        }
    }

    #[test]
    fn closes_are_folded_into_sidecars_and_the_runs_retire() {
        let (_root, mut s, _) = seeded("fold");
        assert_eq!(s.manifest().close_runs.len(), 1, "a run exists to fold");

        let report = s.compact().unwrap();
        assert_eq!(report.closes_folded, 2);
        assert!(
            s.manifest().close_runs.is_empty(),
            "folded runs must no longer be referenced"
        );
        let folded: u32 = s
            .manifest()
            .edge_lanes
            .event
            .iter()
            .map(|e| e.n_closed_folded)
            .sum();
        assert_eq!(folded, 2, "the closes moved into segment sidecars");
        assert!(s.manifest().edge_lanes.event.iter().any(|e| !e.all_current));
    }

    #[test]
    fn closed_rows_are_never_dropped() {
        let (_root, mut s, all) = seeded("retain");
        let rows_before = s.all_edge_versions().unwrap().len();
        s.compact().unwrap();
        let after = s.all_edge_versions().unwrap();

        assert_eq!(after.len(), rows_before, "a row went missing");
        for closed in [&all[1], &all[7]] {
            let found = after
                .iter()
                .find(|r| r.vid == closed.vid.to_hex())
                .expect("a closed version was dropped by compaction");
            assert_eq!(found.tt_e, 200, "its close time must survive folding");
        }
    }

    #[test]
    fn compaction_merges_segments_and_never_reuses_ids() {
        let (_root, mut s, _) = seeded("merge");
        let old_files: Vec<String> = s
            .manifest()
            .edge_lanes
            .event
            .iter()
            .map(|e| e.file.clone())
            .collect();
        let report = s.compact().unwrap();

        assert!(
            report.segments_after <= report.segments_before,
            "compaction should not fragment further"
        );
        let new_files: Vec<String> = s
            .manifest()
            .edge_lanes
            .event
            .iter()
            .map(|e| e.file.clone())
            .collect();
        for f in &new_files {
            assert!(!old_files.contains(f), "segment id {f} was reused");
        }
        // the superseded files are still on disk — nothing is deleted
        for f in &old_files {
            assert!(s.root().join(f).exists(), "{f} was deleted");
        }
    }

    #[test]
    fn compaction_survives_reopening_and_is_idempotent() {
        let (root, mut s, _) = seeded("idempotent");
        s.compact().unwrap();
        let once = sorted(s.all_edge_versions().unwrap());

        // a second pass has nothing left to fold and must change nothing
        let report = s.compact().unwrap();
        assert_eq!(report.closes_folded, 2, "closes stay folded, not lost");
        assert_eq!(sorted(s.all_edge_versions().unwrap()), once);
        drop(s);

        let re = NativeStore::open(&root).unwrap();
        assert_eq!(sorted(re.all_edge_versions().unwrap()), once);
        assert!(re.manifest().close_runs.is_empty());
    }

    #[test]
    fn compaction_is_refused_mid_batch_and_trivial_on_an_empty_store() {
        let root = tmp_root("guards");
        let mut s = NativeStore::open(&root).unwrap();
        assert_eq!(s.compact().unwrap(), CompactionReport::default());
        assert!(!s.needs_compaction());

        s.begin(100).unwrap();
        assert!(s.compact().is_err(), "must not compact with a batch open");
    }

    #[test]
    fn current_only_strip_keeps_the_current_belief_and_drops_the_rest() {
        let (root, mut s, all) = seeded("current-only");
        let believed_now: Vec<String> = {
            let mut v: Vec<String> = s
                .all_edge_versions()
                .unwrap()
                .into_iter()
                .filter(|r| r.tt_e == OPEN_END)
                .map(|r| r.vid)
                .collect();
            v.sort();
            v
        };
        let rows_before = s.all_edge_versions().unwrap().len();
        assert!(believed_now.len() < rows_before, "the seed must have history");

        let report = s.compact_current_only().unwrap();
        assert_eq!(report.closes_folded, 2, "two superseded versions dropped");
        assert!(s.current_only());
        assert!(s.manifest().close_runs.is_empty());

        // the current belief is exactly what survives — nothing more hidden
        let mut after: Vec<String> = s
            .all_edge_versions()
            .unwrap()
            .into_iter()
            .map(|r| {
                assert_eq!(r.tt_e, OPEN_END, "a closed row survived the strip");
                r.vid
            })
            .collect();
        after.sort();
        assert_eq!(after, believed_now, "the current belief changed");

        // segments must be all-current by construction
        assert!(s.manifest().edge_lanes.event.iter().all(|e| e.all_current));

        // the stripped store refuses what it can no longer answer: a
        // past-belief read errs, a current-belief read still works
        assert!(s.believed_node_versions("n1", 100).is_err());
        assert!(s.believed_node_versions("n1", OPEN_END).is_ok());
        s.begin(300).unwrap();
        assert!(
            s.close_version(RowKind::Edge, all[2].vid, 300).is_err(),
            "a correction must be refused on a current-only store"
        );
        s.rollback().unwrap();

        // the marker survives reopening
        drop(s);
        let re = NativeStore::open(&root).unwrap();
        assert!(re.current_only(), "CURRENT_ONLY marker was not honoured on open");
        assert!(re.believed_node_versions("n1", 100).is_err());
        let mut re_rows: Vec<String> =
            re.all_edge_versions().unwrap().into_iter().map(|r| r.vid).collect();
        re_rows.sort();
        assert_eq!(re_rows, believed_now);
    }

    #[test]
    fn current_only_strip_leaves_current_belief_queries_identical() {
        let (_root, mut s, _) = seeded("current-only-eq");
        let now = OPEN_END;
        let before = {
            let mut v: Vec<String> = s
                .all_edge_versions()
                .unwrap()
                .into_iter()
                .filter(|r| crate::believed_at(r.tt_s, r.tt_e, now))
                .map(|r| r.vid)
                .collect();
            v.sort();
            v
        };
        s.compact_current_only().unwrap();
        let mut after: Vec<String> = s
            .all_edge_versions()
            .unwrap()
            .into_iter()
            .filter(|r| crate::believed_at(r.tt_s, r.tt_e, now))
            .map(|r| r.vid)
            .collect();
        after.sort();
        assert_eq!(before, after, "current-belief answers must not change");
    }

    #[test]
    fn unfolded_closes_raise_the_compaction_hint() {
        let (_root, mut s, _) = seeded("hint");
        assert!(
            s.needs_whole_store_compaction(),
            "an unfolded close run should hint"
        );
        s.compact().unwrap();
        // after folding, the hint depends only on segment count
        let segments = s.manifest().edge_lanes.event.len()
            + s.manifest().edge_lanes.interval.len()
            + s.manifest().node_store.len();
        assert_eq!(
            s.needs_whole_store_compaction(),
            segments > defaults::COMPACTION_RUNS_TRIGGER as usize
        );
    }

    // ------------------------------------------------------------------ //
    // partition-local (incremental) compaction                           //
    // ------------------------------------------------------------------ //

    /// Enough overlapping segments in one key-range cluster to fire T1.
    ///
    /// Each batch strides its `vt_s` by the batch count, so successive
    /// generations interleave in composite-key order and every segment's
    /// range overlaps its neighbours' — a live ingest stream's shape, and the
    /// only shape in which a *cluster* is a meaningful unit at all.
    fn clustered(name: &str, batches: u32) -> (PathBuf, NativeStore, Vec<EdgeRow>) {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        let mut all = Vec::new();
        for batch in 0..batches {
            let tt = 100 + batch as i64;
            s.begin(tt).unwrap();
            let a = s.ensure_entity("n1", "Node").unwrap();
            let b = s.ensure_entity("n2", "Node").unwrap();
            if batch == 0 {
                for (uid_id, uid) in [(a, "n1"), (b, "n2")] {
                    s.stage_node(NodeRow {
                        vid: version_vid(uid, tt, 0),
                        uid_id,
                        label: "Node".into(),
                        vt_s: 0,
                        vt_e: OPEN_END,
                        tt_s: tt,
                        props: "{}".into(),
                        source: "ingest".into(),
                        provenance_ref: None,
                    })
                    .unwrap();
                }
            }
            for i in 0..6u32 {
                let vt_s = (i * batches + batch) as i64;
                let r = edge(a, b, "n1", "n2", vt_s, tt, i * batches + batch);
                all.push(r.clone());
                s.stage_edge(r).unwrap();
            }
            s.commit(crate::manifest::EventLogRef::default()).unwrap();
        }
        (root, s, all)
    }

    fn edge_files(s: &NativeStore) -> Vec<String> {
        s.manifest()
            .edge_lanes
            .event
            .iter()
            .chain(s.manifest().edge_lanes.interval.iter())
            .map(|e| e.file.clone())
            .collect()
    }

    /// I2, the direct analogue of `compaction_preserves_the_logical_store_exactly`.
    ///
    /// The store carries closes in **both** forms — one folded into a
    /// sidecar at seal time (a version created and corrected inside one
    /// batch) and one still an unfolded close run — because the merge writer
    /// resolves them through `effective_tt_e`, and a test that exercised only
    /// one form would not notice the other being dropped.
    #[test]
    fn incremental_compaction_preserves_the_logical_store_exactly() {
        let (_root, mut s, all) = clustered("inc-equivalence", 10);
        // an unfolded close run against a committed row
        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, all[3].vid, 200).unwrap();
        s.close_version(RowKind::Edge, all[41].vid, 200).unwrap();
        s.commit(crate::manifest::EventLogRef::default()).unwrap();
        // and a close folded into its own segment's sidecar at seal time
        s.begin(210).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let b = s.ensure_entity("n2", "Node").unwrap();
        let carved = edge(a, b, "n1", "n2", 7, 210, 999);
        s.stage_edge(carved.clone()).unwrap();
        s.close_version(RowKind::Edge, carved.vid, 210).unwrap();
        s.commit(crate::manifest::EventLogRef::default()).unwrap();
        assert!(
            s.manifest()
                .edge_lanes
                .event
                .iter()
                .any(|e| e.n_closed_folded > 0),
            "the seed must carry a folded close"
        );
        assert!(
            !s.manifest().close_runs.is_empty(),
            "the seed must carry an unfolded close run"
        );

        let before = sorted(s.all_edge_versions().unwrap());
        let nodes_before = s.all_node_versions().unwrap();
        let gen_before = s.generation();

        let report = s.compact_incremental(None).unwrap();
        assert!(report.incremental);
        assert!(report.clusters_touched > 0, "nothing was eligible");
        assert!(report.rows_rewritten > 0);

        assert_eq!(
            sorted(s.all_edge_versions().unwrap()),
            before,
            "incremental compaction changed the logical content"
        );
        assert_eq!(s.all_node_versions().unwrap(), nodes_before);
        assert!(s.generation() > gen_before);
    }

    /// I3 over a historical sample, either side of the rewrite.
    #[test]
    fn historical_queries_agree_either_side_of_an_incremental_compaction() {
        let (_root, mut s, all) = clustered("inc-historical", 10);
        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, all[5].vid, 200).unwrap();
        s.commit(crate::manifest::EventLogRef::default()).unwrap();

        let sample = [99i64, 100, 105, 109, 150, 199, 200, 201, OPEN_END];
        let believed = |s: &NativeStore| -> Vec<Vec<String>> {
            sample
                .iter()
                .map(|&as_of| {
                    let mut v: Vec<String> = s
                        .all_edge_versions()
                        .unwrap()
                        .into_iter()
                        .filter(|r| crate::believed_at(r.tt_s, r.tt_e, as_of))
                        .map(|r| r.vid)
                        .collect();
                    v.sort();
                    v
                })
                .collect()
        };
        let before = believed(&s);
        s.compact_incremental(None).unwrap();
        assert_eq!(believed(&s), before, "a belief changed");

        // and a correction after the rewrite still resolves (I4)
        s.begin(300).unwrap();
        s.close_version(RowKind::Edge, all[11].vid, 300).unwrap();
        s.commit(crate::manifest::EventLogRef::default()).unwrap();
        let after = s.all_edge_versions().unwrap();
        let hit = after.iter().find(|r| r.vid == all[11].vid.to_hex()).unwrap();
        assert_eq!(hit.tt_e, 300, "the later close did not land");
    }

    /// The incremental benefit itself: a rewrite touches one size tier, and every
    /// other segment's manifest entry — file, key range, sha — is *identical*
    /// bytes afterwards. That is what lets `idx/tcsr/*.tci` and
    /// `idx/name/*.tnx` survive, since both are keyed on the segment's own
    /// `body_sha` (the name index through the entry's `sha`).
    #[test]
    fn incremental_compaction_rewrites_only_the_eligible_tier() {
        let (_root, mut s, _) = clustered("inc-partial", 12);
        let before: Vec<crate::manifest::SegmentEntry> =
            s.manifest().edge_lanes.event.to_vec();
        assert!(
            before.len() > defaults::CLUSTER_RUNS_TRIGGER as usize,
            "the seed must hold more segments than one tier, got {}",
            before.len()
        );

        let report = s.compact_incremental(None).unwrap();
        assert!(report.incremental);

        let after: Vec<crate::manifest::SegmentEntry> =
            s.manifest().edge_lanes.event.to_vec();
        let before_files: HashSet<&str> = before.iter().map(|e| e.file.as_str()).collect();
        let after_files: HashSet<&str> = after.iter().map(|e| e.file.as_str()).collect();
        let kept: Vec<String> = before_files
            .intersection(&after_files)
            .map(|f| f.to_string())
            .collect();
        assert!(
            !kept.is_empty(),
            "a partial rewrite must leave some segment untouched; before {before_files:?} \
             after {after_files:?}"
        );
        let rewritten = before_files.len() - kept.len();
        assert_eq!(
            rewritten,
            defaults::CLUSTER_RUNS_TRIGGER as usize,
            "exactly one size tier should have been merged"
        );
        // an untouched segment's entry is byte-identical, not merely present
        for f in &kept {
            let b = before.iter().find(|e| e.file == *f).unwrap();
            let a = after.iter().find(|e| e.file == *f).unwrap();
            assert_eq!(a, b, "an untouched segment's manifest entry changed");
        }
    }

    /// I6 across the partial path, and the disk-retention rule: superseded
    /// files stay, because an older generation still names them.
    #[test]
    fn incremental_compaction_never_reuses_a_segment_id() {
        let (_root, mut s, _) = clustered("inc-ids", 12);
        let mut seen: HashSet<String> = edge_files(&s).into_iter().collect();
        let old = seen.clone();

        for _ in 0..4 {
            if !s.needs_compaction() {
                break;
            }
            s.compact_incremental(None).unwrap();
            for f in edge_files(&s) {
                if !old.contains(&f) {
                    assert!(seen.insert(f.clone()) || old.contains(&f), "id {f} was reused");
                }
            }
        }
        for f in &old {
            assert!(s.root().join(f).exists(), "{f} was deleted");
        }
    }

    /// The `staging.rs:565` property, across the splice boundary: the
    /// segments one rewrite produces must still have disjoint, ascending key
    /// ranges, or manifest pruning is unsound.
    #[test]
    fn a_rewrite_emits_disjoint_key_ranges() {
        let (_root, mut s, _) = clustered("inc-ranges", 12);
        let before: HashSet<String> = edge_files(&s).into_iter().collect();
        s.compact_incremental(None).unwrap();

        let mut fresh: Vec<&crate::manifest::SegmentEntry> = s
            .manifest()
            .edge_lanes
            .event
            .iter()
            .filter(|e| !before.contains(&e.file))
            .collect();
        assert!(!fresh.is_empty(), "the rewrite produced no segment");
        fresh.sort_by(|a, b| a.key_lo.cmp(&b.key_lo));
        for w in fresh.windows(2) {
            assert!(
                w[0].key_hi < w[1].key_lo,
                "one rewrite's ranges must stay disjoint: {:?} vs {:?}",
                w[0].key_hi,
                w[1].key_lo
            );
        }
    }

    /// A cluster rewrite merges at most `F` runs, so the output's
    /// `tt_s` run count is bounded by its inputs' — it does not approach the
    /// number of distinct transaction times in the store, which is what the
    /// whole-store re-sort does and what `scan.rs`'s `believed_ranges` doc
    /// comment measured at 2.1M runs.
    #[test]
    fn incremental_cycles_do_not_shatter_the_transaction_time_runs() {
        let (_root, mut s, _) = clustered("inc-runs", 12);
        let runs_of = |s: &NativeStore| -> Vec<usize> {
            s.manifest()
                .edge_lanes
                .event
                .iter()
                .map(|e| s.open_segment(&e.file).unwrap().tt_s_run_count())
                .collect()
        };
        s.compact_incremental(None).unwrap();
        let inc = *runs_of(&s).iter().max().unwrap();

        // the same store, compacted whole: every generation's rows are
        // re-sorted into one segment, so the runs interleave across all of
        // them
        let (_root2, mut whole, _) = clustered("inc-runs-whole", 12);
        whole.compact().unwrap();
        let full = *runs_of(&whole).iter().max().unwrap();

        assert!(
            inc < full,
            "the partial rewrite must shatter less than the whole-store one \
             ({inc} vs {full} runs)"
        );
        // Note what is *not* asserted. A natural expectation is that a cluster rewrite
        // "merges at most F = 8 runs, bounding the output at 8 × (runs per
        // input segment)". That bound does not hold: a k-way merge of fully
        // interleaved inputs alternates on every row, so the output's run
        // count is bounded by its *row* count, not by k — measured here at
        // 48 runs over 48 rows from eight single-run inputs. What survives
        // is the comparative claim, which is the one the read path cares
        // about: a partial rewrite interleaves only its own tier, while a
        // whole-store rewrite interleaves every generation in the store, and
        // it does so to every row on every cycle rather than to a row
        // log_F(n/run) times.
    }

    /// I1 across step 3: retiring the close-run list must not lose a single
    /// live `tt_e`, and a record naming a dead segment must never resolve.
    #[test]
    fn close_run_retirement_preserves_every_live_tt_e() {
        let (_root, mut s, all) = clustered("inc-closes", 12);
        // close rows in several different segments, so some runs name
        // segments the rewrite will supersede and some do not
        s.begin(200).unwrap();
        for i in [1usize, 7, 13, 25, 40, 55] {
            s.close_version(RowKind::Edge, all[i].vid, 200).unwrap();
        }
        s.commit(crate::manifest::EventLogRef::default()).unwrap();

        let want: HashMap<String, i64> = s
            .all_edge_versions()
            .unwrap()
            .into_iter()
            .map(|r| (r.vid, r.tt_e))
            .collect();

        for _ in 0..6 {
            if !s.needs_compaction() {
                break;
            }
            s.compact_incremental(None).unwrap();
            let got: HashMap<String, i64> = s
                .all_edge_versions()
                .unwrap()
                .into_iter()
                .map(|r| (r.vid, r.tt_e))
                .collect();
            assert_eq!(got, want, "a tt_e changed across an incremental cycle");
        }
        // Every close is accounted for exactly once, in one of the two
        // forms: folded into the sidecar of a segment the rewrite produced,
        // or still a run record naming a segment the rewrite left alone. A
        // partial compactor cannot promise the first — the whole-store one
        // could, by folding everything — and the sum is what I1 actually
        // requires.
        let folded: u32 = s
            .manifest()
            .edge_lanes
            .event
            .iter()
            .map(|e| e.n_closed_folded)
            .sum();
        let live = merge::live_segment_ids(s.manifest());
        let mut still_in_runs = 0usize;
        for run in &s.manifest().close_runs {
            for rec in crate::visibility::read_close_run(&s.root().join(&run.file)).unwrap() {
                if live.contains(&rec.segment) {
                    still_in_runs += 1;
                }
            }
        }
        assert!(folded > 0, "the rewrite folded nothing");
        assert_eq!(
            folded as usize + still_in_runs,
            6,
            "a close was lost or double-counted: {folded} folded, \
             {still_in_runs} still in runs"
        );
    }

    /// The trigger is the predicate the compactor plans against, so looping
    /// on it terminates — and terminates at a store that is *smaller*, not
    /// merely different.
    #[test]
    fn the_trigger_converges_and_the_segment_count_falls() {
        let (_root, mut s, _) = clustered("inc-converge", 20);
        let before = edge_files(&s).len();
        assert!(s.needs_compaction(), "20 overlapping segments must be eligible");

        let mut rounds = 0;
        while s.needs_compaction() {
            s.compact_incremental(None).unwrap();
            rounds += 1;
            assert!(rounds < 32, "the trigger did not converge in 32 rounds");
        }
        let after = edge_files(&s).len();
        assert!(
            after * 2 < before,
            "draining the trigger must more than halve the segment count: \
             {before} -> {after} in {rounds} round(s)"
        );
        // One call plans *every* eligible window, so 20 segments collapse in
        // a single round: two windows of eight merge, four are left over and
        // the two outputs join them, which is under the fan-in. What the
        // loop is really pinning is that the trigger and the compactor agree
        // — the predicate goes false because the work it named was done, not
        // because the compactor gave up.
        assert!(rounds >= 1);
    }

    /// The budget caps one call's work and never starves.
    #[test]
    fn a_row_budget_bounds_one_call() {
        let (_root, mut s, _) = clustered("inc-budget", 20);
        let report = s.compact_incremental(Some(1)).unwrap();
        assert_eq!(
            report.clusters_touched, 1,
            "a one-row budget still makes progress, but only one rewrite's worth"
        );
        assert!(report.rows_rewritten > 1, "the first set is taken whole");
    }

    /// Node segments carry their lane in their own header — `scan_nodes`
    /// reads `segment.header().lane` for every target — and there is one
    /// manifest vector for both lanes, so a merge that stamped a single lane
    /// on its output would silently move rows between lanes. The merge
    /// routes rows with the store's own `PartitionMap`, exactly as
    /// `Staging::seal` does, so every output segment's header agrees with
    /// its own rows.
    #[test]
    fn a_merged_node_segment_lands_in_the_lane_its_rows_belong_to() {
        let root = tmp_root("inc-node-lanes");
        let mut s = NativeStore::open(&root).unwrap();
        // one instantaneous and one open-ended node version per batch, so
        // both lanes are populated and a merge set spans them
        for batch in 0..12u32 {
            let tt = 100 + batch as i64;
            s.begin(tt).unwrap();
            for (suffix, vt_e) in [("i", None), ("l", Some(OPEN_END))] {
                let uid = format!("n{batch}{suffix}");
                let id = s.ensure_entity(&uid, "Node").unwrap();
                let vt_s = batch as i64;
                s.stage_node(NodeRow {
                    vid: version_vid(&uid, tt, vt_s),
                    uid_id: id,
                    label: "Node".into(),
                    vt_s,
                    vt_e: vt_e.unwrap_or(vt_s + 1),
                    tt_s: tt,
                    props: "{}".into(),
                    source: "ingest".into(),
                    provenance_ref: None,
                })
                .unwrap();
            }
            s.commit(crate::manifest::EventLogRef::default()).unwrap();
        }
        let before = {
            let mut v = s.all_node_versions().unwrap();
            v.sort_by(|a, b| a.vid.cmp(&b.vid));
            v
        };
        assert!(s.needs_compaction(), "24 node segments must be eligible");
        let report = s.compact_incremental(None).unwrap();
        assert!(report.rows_rewritten > 0);

        let partitions = *s.layout().0;
        for entry in &s.manifest().node_store {
            let seg = s.open_segment(&entry.file).unwrap();
            let header_lane = seg.header().lane;
            let vt_s = seg.i64_column("vt_s").unwrap();
            let vt_e = if seg.header().vt_e_elided {
                None
            } else {
                Some(seg.i64_column("vt_e").unwrap())
            };
            for i in 0..vt_s.len() {
                let e = vt_e.map(|c| c[i]).unwrap_or(vt_s[i] + 1);
                assert_eq!(
                    partitions.lane_for(vt_s[i], e),
                    header_lane,
                    "row {i} of {} is in the wrong lane",
                    entry.file
                );
            }
        }
        let after = {
            let mut v = s.all_node_versions().unwrap();
            v.sort_by(|a, b| a.vid.cmp(&b.vid));
            v
        };
        assert_eq!(after, before, "the node listing changed");
    }

    #[test]
    fn incremental_compaction_is_refused_mid_batch_and_trivial_on_an_empty_store() {
        let root = tmp_root("inc-guards");
        let mut s = NativeStore::open(&root).unwrap();
        let empty = s.compact_incremental(None).unwrap();
        assert_eq!(empty.clusters_touched, 0);
        assert_eq!(empty.rows_rewritten, 0);
        assert!(empty.incremental);
        assert!(!s.needs_compaction());

        s.begin(100).unwrap();
        assert!(
            s.compact_incremental(None).is_err(),
            "must not compact with a batch open"
        );
    }

    #[test]
    fn close_run_merge_plan_takes_the_smallest_tier_with_a_full_stretch() {
        let f = CLOSE_RUN_FAN_IN;
        assert_eq!(plan_close_run_merge(&[1; 7], f), None, "seven tier-0 runs wait");
        assert_eq!(plan_close_run_merge(&[1; 8], f), Some(0));
        assert_eq!(plan_close_run_merge(&[5000, 1, 1, 1, 1, 1, 1, 1, 1], f), Some(1),
            "a big run ahead of a full tier-0 stretch is left alone");
        // seven tier-1 runs and a full tier-0 stretch: the tier-0 merge yields an
        // eighth tier-1 run, and the cascade merges all of them
        let mut v = vec![8u64; 7];
        v.extend([1u64; 8]);
        assert_eq!(plan_close_run_merge(&v, f), Some(0));
        // a tier-0 run stranded ahead of a tier-1 run is absorbed by the
        // tier <= 1 stretch once that one is full
        let mut v = vec![64u64, 1, 8, 8, 8, 8, 8, 8, 8];
        assert_eq!(plan_close_run_merge(&v, f), Some(1));
        // seven runs in all is under the fan-in at every tier
        v.truncate(7);
        assert_eq!(plan_close_run_merge(&v, f), None);
        // eight runs in all at tier <= 2 are one stretch: the bound wins
        v.push(8);
        assert_eq!(plan_close_run_merge(&v, f), Some(0));
        assert_eq!(plan_close_run_merge(&[], f), None);
    }

    #[test]
    fn close_run_merge_bounds_the_list_under_one_record_appends() {
        // simulate the merge policy alone (no deaths): appends of 1, a merge
        // check after every append, as compaction would
        let f = CLOSE_RUN_FAN_IN;
        let mut runs: Vec<u64> = Vec::new();
        let mut longest = 0usize;
        for _ in 0..200_000 {
            runs.push(1);
            if let Some(from) = plan_close_run_merge(&runs, f) {
                let sum: u64 = runs[from..].iter().sum();
                runs.truncate(from);
                runs.push(sum);
            }
            longest = longest.max(runs.len());
        }
        assert_eq!(runs.iter().sum::<u64>(), 200_000, "no record lost");
        // every maximal trailing stretch at tier <= t is shorter than f, so
        // the list is under f runs per tier
        let tiers = merge::size_tier(200_000, f as u64) as usize + 1;
        assert!(longest < f * tiers, "list reached {longest} runs (bound {})", f * tiers);
    }

    /// One long-lived base segment, then single-op batches that each add an
    /// edge and close a base edge — the shape whose run list used to grow
    /// without bound — with the incremental trigger consulted after each.
    #[test]
    fn close_run_merge_carries_the_exact_close_index_and_bounds_the_list() {
        let root = tmp_root("close-merge");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let b = s.ensure_entity("n2", "Node").unwrap();
        let mut base = Vec::new();
        for i in 0..2_000u32 {
            let r = edge(a, b, "n1", "n2", i as i64, 100, i);
            base.push(r.clone());
            s.stage_edge(r).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();

        let rebuilt = |s: &NativeStore| {
            let mut recs = Vec::new();
            for run in &s.manifest().close_runs {
                recs.extend(crate::visibility::read_close_run(&s.root().join(&run.file)).unwrap());
            }
            CloseIndex::from_records(recs)
        };
        let mut compactions = 0usize;
        let mut merges = 0usize;
        let mut longest = 0usize;
        for k in 0..600u32 {
            let tt = 200 + k as i64;
            s.begin(tt).unwrap();
            let r = edge(a, b, "n1", "n2", 10_000 + k as i64, tt, 10_000 + k);
            s.stage_edge(r).unwrap();
            // a Zipf-ish mix: most closes hit the base, some hit recent rows
            let target = &base[((k * 7) % 2_000) as usize];
            s.close_version_at(RowKind::Edge, target.vid, Some(target.vt_s), tt).unwrap();
            s.commit(EventLogRef::default()).unwrap();
            while s.needs_compaction() {
                let runs_before = s.manifest().close_runs.len();
                s.compact_incremental(None).unwrap();
                compactions += 1;
                if s.manifest().close_runs.len() < runs_before {
                    merges += 1;
                }
                let carried = s.committed_close_index().unwrap();
                assert_eq!(*carried, rebuilt(&s), "carried close index differs from a rebuild \
                    after compaction {compactions} (generation {})", s.generation());
            }
            longest = longest.max(s.manifest().close_runs.len());
        }
        assert!(compactions > 20 && merges > 5, "the scenario must exercise the merge: \
            {compactions} compactions, {merges} merges");
        assert!(longest < 64, "close-run list reached {longest} runs");

        // the logical store, as this handle sees it, is what a fresh open of
        // the files on disk sees
        let here = sorted(s.all_edge_versions().unwrap());
        drop(s);
        let fresh = NativeStore::open_read_only(&root).unwrap();
        assert_eq!(sorted(fresh.all_edge_versions().unwrap()), here);
        assert!(fresh.verify().unwrap().is_healthy());
    }
}
