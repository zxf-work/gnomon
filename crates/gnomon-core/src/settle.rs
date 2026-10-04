//! `settle`: a quiescent, explicitly invoked, read-optimising merge
//! that returns a tiered store to its byte floor.
//!
//! **What it is, in one line.** A major compaction: one bounded-fan-in k-way
//! merge per lane slot over *every* live segment of that slot, rewriting each
//! row exactly once, at a moment nobody is waiting. The operation is standard
//! and old (RocksDB `CompactRange`, LevelDB manual compaction, Cassandra
//! `nodetool compact`, HBase major compaction); nothing here claims otherwise.
//!
//! **Why it exists.** `merge::plan_lane`'s tier rule merges `F` same-size-tier
//! segments and leaves every partially-filled tier window unmerged. Those
//! unmerged windows are simultaneously the rows it did *not* rewrite and the
//! segments a reader must open — one object, seen from the write side and the
//! read side. A whole-extent aggregate is pruned by nothing (`scan::
//! prunes_bounds` tests `tt_s_min`/`vt_min`/`vt_e_max`, and a whole-extent
//! request spans all three), so it pays a cost linear in live segments, while
//! a narrow-window query prunes to near-perfect selectivity and does not.
//! Settle removes the segment excess by rewriting the residue once, instead of
//! once per compaction cycle.
//!
//! **This module is a planner, not a writer.** `merge::merge_edge_cluster` and
//! `merge::merge_node_cluster` are already, line for line, a bounded streaming
//! k-way merge: inputs opened `open_segment_uncached` and dropped at return,
//! one row resident per input via `BinaryHeap<Reverse<Cursor>>`, deterministic
//! tie-breaking on input index, `write_edge_segment`/`write_node_segment` for
//! the encoding so the staircase and every header statistic are rebuilt
//! rather than reimplemented, `entry_for` shared with `staging.rs` so a merged
//! segment is described by the code that describes a freshly sealed one, and
//! `ChunkWriter::should_flush`'s one-row lookahead so an output boundary never
//! splits a tie group. Both are `pub(crate)` on `NativeStore`, so **`merge.rs`
//! is not edited at all** — and neither is `compact.rs`, `staging.rs`, or any
//! read-path module. That is the design's central claim and its diff-scope
//! obligation; it holds.
//!
//! The only thing `plan_lane` does that a settle must not do is *skip*: it
//! `continue`s past any segment already at the byte target and only emits a
//! set once the window reaches `F`. A settle emits every live segment of a
//! lane, in key order, in groups bounded by a declared byte budget.
//!
//! **Invocation is explicit.** `gnomon store settle --store <path>`, plus an
//! advisory [`NativeStore::needs_settle`] a driver may poll at quiescence.
//! This follows the engine's own standing rule, stated in `compact.rs`:
//! compaction stays explicit, never a background schedule, because a single
//! writer has no one to coordinate with and surprise rewrites are worse than a
//! stale layout. A settle rewrites strictly more than a compaction does, so
//! the rule binds harder here, not less.
//!
//! **Settle does not call gc.** It supersedes every segment it merges and
//! leaves the old files on disk for the retained generations, exactly as
//! `compact` does, so a store is transiently ~2x on disk until the operator
//! runs `gnomon store gc --keep 2`. The report carries the reclaimable byte
//! count so that transient is a stated precondition of the job rather than a
//! silent property.

use std::collections::HashSet;

use crate::error::{EngineError, Result};
use crate::manifest::SegmentEntry;
use crate::merge::{self, LaneSlot};
use crate::store::{segment_id_of, NativeStore};

/// Share of the byte target a segment's *modelled* bytes must reach before it
/// counts as one the writer actually sealed on the byte rule, rather than one
/// a small commit simply ended.
///
/// **This constant existed because the byte accounting and the bytes
/// disagreed; that gap has since been closed, and the constant survives for the other
/// half of its job.**
///
/// Previously both writers — `staging::chunk_end` on the build path and
/// `merge::ChunkWriter` on the merge path — accumulated *unencoded* row
/// bytes (`EDGE_FIXED_BYTES + disc.len() + 4`, ~58 B on the synthetic corpus)
/// against a target the encoder then compressed to 30.40-30.48 B/row, so a
/// segment accounted at 128 MiB sealed at ~65-70 MB on disk: a measured
/// mean/target of 0.488 at 30M and 0.518 at 100M. The writers now
/// divide the configured target through by a measured per-lane quotient
/// (`crate::sealsize`), so a sealed file lands at the configured size and the
/// callers here pass the corrected target rather than the configured one.
///
/// What is left for this constant is the discrimination that was always its
/// second job: [`per_row_model`] knows only the flat per-row figure, and a
/// segment's `rows x per_row_model` lands in `[0.5, 1.0] x target` — the
/// writer flushes at the *first* row past the target that is also a legal
/// tie boundary, and a chunk can be cut short by the resident cap — rather
/// than exactly at it. 0.5 is the threshold that separates "this segment
/// sealed on the byte rule, so its row count *is* this corpus's rows-per-seal"
/// from "this segment is one small commit, so only the model can say what a
/// seal holds". [`LaneCensus::rows_per_seal`] is the one place that
/// distinction is drawn, and its docstring works both cases through;
/// `store::seal_ratios` draws the same line, through `merge::full_rows_at`,
/// to decide which segments may be measured.
///
/// The original measurement, kept because it is what the correction was sized
/// against: at 10M entities a
/// sealed segment of 2,323,687 rows occupied 71,761,054 B, 0.5347 of the
/// target, while `rows * 58` -- the writer's own accumulator at the moment it
/// flushed -- was 1.0041 of it.
const SEALED_FRACTION: u64 = 2;

/// Hard cap on one merge group's input count, independent of the byte budget.
///
/// The budget bounds *bytes*, which is the honest bound and the one that lets
/// a lane of thousands of 0.2 MB node segments collapse in a single group.
/// This caps the file descriptors, mappings and heap cursors one merge holds,
/// so a pathological store cannot turn a byte budget into a VMA exhaustion.
const MAX_FAN_IN: usize = 4096;

/// The default settle budget where `GNOMON_SETTLE_BYTES` is unset, and the same
/// figure the identity postings' budget uses as its ceiling
/// (`defaults::POSTINGS_MAX_BYTES`). `0` means "no cap", which for a fan-in
/// budget means one group per lane.
///
/// **Deliberately not `min(RAM/8, 16 GiB)`, unlike the postings budget.** The
/// postings budget sizes a structure the store *holds*; this one is a ceiling
/// on how many input bytes one merge group may span, and a group never spans
/// more than the lane it is merging. A default above physical RAM therefore
/// cannot be spent unless a single lane already exceeds it, whereas reading
/// physical RAM here would mean making `store::detected_ram_bytes` visible —
/// a hunk in `store.rs`, outside "new modules plus CLI/PyO3 surface". The
/// campaign pins `GNOMON_SETTLE_BYTES` explicitly in every job, which is what
/// makes the memory forecast a claim about the engine rather than about a
/// host's RAM.
const SETTLE_MAX_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// What one settle call did. Serialized into `settle-record.json` by the
/// driver, alongside — never into — the build record the store was built with.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SettleReport {
    pub segments_before: usize,
    pub segments_after: usize,
    pub closes_folded: usize,
    /// Rows copied into freshly written segments by this call. `0` on a store
    /// already at its byte floor — that is the idempotence assertion.
    pub rows_rewritten: u64,
    /// Merge passes performed: `0` if nothing was eligible, otherwise `1`.
    /// See [`NativeStore::settle`] on why a settle is single-pass by
    /// construction.
    pub rounds: u32,
    /// Largest merge group actually planned, in segments.
    pub fan_in: usize,
    /// Merge groups planned across all lane slots.
    pub groups: usize,
    /// The resolved memory budget this call planned against.
    pub budget_bytes: u64,
    /// Live segments considered, and how many of them carried fewer rows than
    /// `merge.rs`'s own byte-target threshold — the quantity that tests, and
    /// the direct test of whether the segment excess is unfilled tier windows
    /// rather than segments already at the target. Computed with exactly
    /// `plan_lane`'s arithmetic: `(target_bytes / per_row_model) / 2` rows.
    pub inputs_total: usize,
    pub inputs_below_full_rows: usize,
    /// Bytes of superseded segment files a following `gc --keep 2` can
    /// reclaim — the ~2x disk transient, stated rather than implied.
    pub reclaimable_bytes: u64,
    pub generation_before: u64,
    pub generation_after: u64,
    pub wall_us: u64,
}

// ---------------------------------------------------------------------- //
// planning — arithmetic over manifest entries plus one stat per segment   //
// ---------------------------------------------------------------------- //

/// The fixed part of `staging::edge_row_bytes` / `staging::node_row_bytes`,
/// per lane slot.
///
/// **Not duplicated.** The per-row model is one definition
/// (`crate::sealsize::per_row_model`), used by the writer, the tier planner
/// and this floor alike. The indirection stays so the call sites below read
/// as they did, and `the_row_byte_model_agrees_with_the_merge_planner` still
/// pins the arithmetic against `plan_lane`.
fn per_row_model(slot: LaneSlot) -> u64 {
    crate::sealsize::per_row_model(slot)
}

/// Rows at which `merge::plan_lane` treats a segment as "at the byte target"
/// and refuses to merge it. Identical arithmetic to `merge::full_rows`.
fn full_rows_model(target_bytes: u64, slot: LaneSlot) -> u64 {
    ((target_bytes / per_row_model(slot).max(1)) / 2).max(1)
}

/// Output segments one merge group's writers can leave above the byte floor:
/// each group ends on a partial, and `merge_node_cluster` runs an event and an
/// interval `ChunkWriter` concurrently, so the node slot can end on two.
fn writers_per_group(slot: LaneSlot) -> usize {
    match slot {
        LaneSlot::Node => 2,
        _ => 1,
    }
}

/// One lane slot's live segments, in key order, with the two quantities the
/// planner needs: how many rows each holds, and what the lane's rows-per-seal
/// actually is on this corpus.
///
/// **Rows, not file bytes, and no segment is opened or stat'd.** The design
/// sketched the byte floor as `sum(rows) x B/row` from the manifest alone and
/// that is what this is — the advisory [`NativeStore::needs_settle`] therefore
/// keeps the property its docstring claims. An earlier draft measured file
/// bytes with one `fs::metadata` per segment, which is exact about the *bytes*
/// and wrong about the *count*: a segment file carries a header and a string
/// heap, so summing the file bytes of many small segments over-counts what
/// their merged content will occupy by the per-file overhead it is about to
/// remove, and the floor comes out at the count it started from. The row count
/// is the quantity the writer's own flush rule is a function of, so it is the
/// quantity the floor has to be derived from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SettleInput {
    pub file: String,
    pub id: u64,
    pub rows: u64,
}

/// What [`NativeStore::plan_settle`] returns: per lane slot, one merge group
/// of live segments in key order.
pub(crate) type SettlePlan = Vec<(LaneSlot, Vec<SettleInput>)>;

struct LaneCensus {
    slot: LaneSlot,
    /// The lane's live segments in key order.
    segments: Vec<SettleInput>,
    rows: u64,
    /// Largest live segment's row count.
    largest_rows: u64,
}

impl LaneCensus {
    fn take(slot: LaneSlot, entries: &[SegmentEntry]) -> Self {
        let mut segments = Vec::with_capacity(entries.len());
        let mut rows = 0u64;
        let mut largest_rows = 0u64;
        for i in merge::key_order(entries) {
            let e = &entries[i];
            let r = e.rows as u64;
            rows += r;
            largest_rows = largest_rows.max(r);
            segments.push(SettleInput {
                file: e.file.clone(),
                id: segment_id_of(&e.file),
                rows: r,
            });
        }
        Self {
            slot,
            segments,
            rows,
            largest_rows,
        }
    }

    /// Rows one sealed segment of this lane holds — the divisor the byte floor
    /// is a quotient of.
    ///
    /// Two cases, and getting the discrimination right is the whole of this
    /// function:
    ///
    /// * **Some segment has actually sealed on the byte rule.** Then its row
    ///   count *is* this corpus's rows-per-seal, including whatever `disc`
    ///   lengths and encoding ratio this data happens to have, and the model
    ///   cannot improve on it. Taking the largest is what makes a store that
    ///   is already at its floor stay there: the whole-store control's 30M
    ///   event lane holds 14 segments of ~2.14M rows, so `ceil(30.0M / 2.14M)`
    ///   is 14 and the lane is left alone. Using the model instead would give
    ///   `134,217,728 / 50 = 2.68M` rows per seal, a floor of 12, and a
    ///   whole-store rewrite of a store that had nothing to gain from one.
    /// * **Nothing has sealed yet** — every segment is one small commit, which
    ///   is the shape a fresh or unit-scale store has. Then the largest row
    ///   count says nothing about seals and the model is the only estimate
    ///   available: `target / per_row_model`.
    ///
    /// [`SEALED_FRACTION`] draws the line, and its docstring says why it is a
    /// half rather than a one.
    fn rows_per_seal(&self, target_bytes: u64) -> u64 {
        let per_row = per_row_model(self.slot).max(1);
        let modelled = (target_bytes / per_row).max(1);
        if self.largest_rows.saturating_mul(per_row) >= target_bytes / SEALED_FRACTION {
            self.largest_rows.max(1)
        } else {
            modelled
        }
    }

    /// `ceil(lane rows / rows per seal)`: the fewest segments this lane's
    /// content can occupy: the "byte floor", and never a fixed count.
    fn byte_floor(&self, target_bytes: u64) -> usize {
        if self.rows == 0 {
            return 0;
        }
        self.rows.div_ceil(self.rows_per_seal(target_bytes)) as usize
    }

    /// Modelled bytes of one segment, for the group budget.
    fn model_bytes(&self, rows: u64) -> u64 {
        rows.saturating_mul(per_row_model(self.slot))
    }
}

/// Group a lane's segments, in key order, into merges each bounded by the
/// budget.
///
/// **Bounded by bytes, not by a segment count.** The declared budget is a
/// memory budget, and a merge's residency is its inputs' mapped and decoded
/// bytes plus one output builder. Grouping on bytes is therefore the bound the
/// operator actually asked for, and it is what lets the node lane — 32
/// segments holding 0.6 % of a 30M store's bytes at 0.18 MB each, because
/// every commit appends one — collapse in a single group and a single pass,
/// where a fixed fan-in of 15 would need three groups and a second round to
/// reach the same place.
///
/// The bytes counted are the writer's own *modelled* bytes, `rows x 50` (or
/// 44), which run ~2x the achieved file bytes on this corpus
/// ([`SEALED_FRACTION`]) — comfortably above the ~1.7x a mapped, decoded
/// segment is actually resident for. So `sum(model bytes) <= budget` bounds
/// residency below the budget rather than at it, which is the direction a
/// memory bound should err in, and it needs no file to be opened or stat'd.
///
/// A segment whose model bytes exceed the whole group budget forms its own
/// group; a group of one is dropped by the caller, because rewriting a segment
/// alone changes nothing about it.
fn plan_groups(census: &LaneCensus, group_budget: u64) -> Vec<Vec<usize>> {
    let mut out: Vec<Vec<usize>> = Vec::new();
    let mut cur: Vec<usize> = Vec::new();
    let mut acc = 0u64;
    for (pos, seg) in census.segments.iter().enumerate() {
        let bytes = census.model_bytes(seg.rows);
        let would_exceed = !cur.is_empty() && acc.saturating_add(bytes) > group_budget;
        if would_exceed || cur.len() >= MAX_FAN_IN {
            out.push(std::mem::take(&mut cur));
            acc = 0;
        }
        cur.push(pos);
        acc = acc.saturating_add(bytes);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Resolve the settle budget: explicit argument, else `GNOMON_SETTLE_BYTES`,
/// else [`SETTLE_MAX_BYTES`]. `0` means unbounded — one group per lane.
///
/// Mirrors `store::postings_budget`'s resolution order, including the `0`
/// escape, so an operator who has learned one budget knob has learned both.
pub(crate) fn settle_budget(env: Option<&str>, explicit: Option<u64>) -> u64 {
    if let Some(b) = explicit {
        return b;
    }
    if let Some(v) = env {
        if let Ok(n) = v.trim().parse::<u64>() {
            return n;
        }
    }
    SETTLE_MAX_BYTES
}

impl NativeStore {
    /// Is any lane slot above its byte floor?
    ///
    /// Advisory, and it schedules nothing — the same contract
    /// `needs_compaction` carries. It opens no segment and touches no file:
    /// row counts and key ranges from the manifest are everything the plan
    /// needs. A driver may poll it at quiescence and decide for itself; the
    /// engine never decides.
    pub fn needs_settle(&self) -> bool {
        !self.plan_settle(None).is_empty()
    }

    /// The merges a settle would run: per lane slot, the groups of live
    /// segments in key order, for every lane not already at its byte floor.
    ///
    /// Returns `(slot, group)` pairs where a group holds indices into that
    /// lane's `Vec<SegmentEntry>`, in key order.
    pub(crate) fn plan_settle(&self, budget_bytes: Option<u64>) -> SettlePlan {
        let (_, target_bytes) = self.layout();
        let budget = settle_budget(
            std::env::var("GNOMON_SETTLE_BYTES").ok().as_deref(),
            budget_bytes,
        );
        let group_budget = if budget == 0 { u64::MAX } else { budget.max(1) };
        // The floor is a count of segments the writer can produce, so it has
        // to be computed against the target the writer actually seals on —
        // the configured target corrected by the lane's measured quotient
        // (`crate::sealsize`). Against the uncorrected one the floor comes
        // out at twice the segments and a settled store reads as already
        // settled.
        let ratios = self.seal_ratios();
        let m = self.manifest();
        let mut out = Vec::new();
        for (slot, entries) in [
            (LaneSlot::EdgeEvent, &m.edge_lanes.event),
            (LaneSlot::EdgeInterval, &m.edge_lanes.interval),
            (LaneSlot::Node, &m.node_store),
        ] {
            if entries.len() < 2 {
                continue;
            }
            let target_bytes = ratios.effective_target(slot, target_bytes);
            let census = LaneCensus::take(slot, entries);
            let groups = plan_groups(&census, group_budget);
            // Already at the byte floor? Then merging rewrites every row to
            // save nothing, and a settle that did so would not be idempotent.
            //
            // The floor is `T` segments; one pass over `g` groups ends each
            // group on a partial per writer, so a lane is at its floor when
            // `S <= T + g * writers`. This is the *only* skip rule in the
            // module — every other segment of an eligible lane is merged,
            // which is exactly what `plan_lane` does not do — and it is what
            // the idempotence test (a second settle rewrites nothing) asserts on.
            let floor = census.byte_floor(target_bytes);
            let allowance = groups.len() * writers_per_group(slot);
            if census.segments.len() <= floor + allowance {
                continue;
            }
            for g in groups {
                if g.len() < 2 {
                    continue; // rewriting one segment alone changes nothing
                }
                out.push((
                    slot,
                    g.into_iter().map(|p| census.segments[p].clone()).collect(),
                ));
            }
        }
        out
    }

    /// Merge every live segment of every lane that is above its byte floor,
    /// once, into segments at the byte target. The compaction operation.
    ///
    /// `budget_bytes` overrides `GNOMON_SETTLE_BYTES` for this call. The logical
    /// store is unchanged — every input row is pushed to a `ChunkWriter` with
    /// no filter anywhere, `tt_e` is relocated from a close run into the new
    /// segment's sidecar through the one spelling every read path uses
    /// (`read::effective_tt_e`), `vid` is copied verbatim as a raw `Id96`, and
    /// transaction time does not advance — so `store_digest` is invariant by
    /// construction, which is the provenance proof a settled campaign store
    /// rests on.
    ///
    /// **Single pass, by construction.** One pass leaves a lane within
    /// `g - 1` segments of its byte floor, for `g` groups; a second pass would
    /// rewrite every row again to recover at most those `g - 1`. That is a bad
    /// trade at any scale where `g` is small, and where `g` is large the store
    /// is far bigger than the declared budget and the answer is a larger
    /// budget, not a second rewrite. `rounds` is therefore 0 or 1, and
    /// `rows_rewritten == n and rounds == 1` is directly readable off the
    /// report. (The design sketched a multi-round loop and a small-budget test
    /// that asserts `rounds > 1`; the deviation and its reasoning are recorded
    /// in the lane report. A small budget here reports more `groups` and a
    /// `segments_after` above the floor, which carries the same information
    /// and costs one rewrite instead of several.)
    pub fn settle(&mut self, budget_bytes: Option<u64>) -> Result<SettleReport> {
        if self.in_batch() {
            return Err(EngineError::invariant(
                "cannot settle while a batch is open",
            ));
        }
        self.require_writable_handle()?;
        let started = std::time::Instant::now();
        let (_, target_bytes) = self.layout();
        let generation_before = self.manifest().generation;
        let segments_before = {
            let m = self.manifest();
            m.edge_lanes.event.len() + m.edge_lanes.interval.len() + m.node_store.len()
        };
        let budget = settle_budget(
            std::env::var("GNOMON_SETTLE_BYTES").ok().as_deref(),
            budget_bytes,
        );

        // The below-target census, over every live segment, before anything moves.
        let (inputs_total, inputs_below_full_rows) = {
            let ratios = self.seal_ratios();
            let m = self.manifest();
            let mut total = 0usize;
            let mut below = 0usize;
            for (slot, entries) in [
                (LaneSlot::EdgeEvent, &m.edge_lanes.event),
                (LaneSlot::EdgeInterval, &m.edge_lanes.interval),
                (LaneSlot::Node, &m.node_store),
            ] {
                let full_at =
                    full_rows_model(ratios.effective_target(slot, target_bytes), slot);
                for e in entries {
                    total += 1;
                    if (e.rows as u64) < full_at {
                        below += 1;
                    }
                }
            }
            (total, below)
        };

        let plans = self.plan_settle(budget_bytes);
        if plans.is_empty() {
            return Ok(SettleReport {
                segments_before,
                segments_after: segments_before,
                budget_bytes: budget,
                inputs_total,
                inputs_below_full_rows,
                generation_before,
                generation_after: generation_before,
                wall_us: started.elapsed().as_micros() as u64,
                ..Default::default()
            });
        }

        let closes = self.committed_close_index()?;
        // A physical rewrite: no belief changed, so tt does not advance (I5),
        // exactly as `compact_incremental` does it.
        let mut next = self.manifest().successor(self.manifest().created_tt);
        let mut next_id = next.next_segment_id;

        let mut rows_rewritten = 0u64;
        let mut closes_folded = 0usize;
        let mut fan_in = 0usize;
        let mut reclaimable_bytes = 0u64;
        let mut replacements: Vec<(LaneSlot, Vec<SegmentEntry>)> = Vec::new();
        let mut superseded: HashSet<String> = HashSet::new();
        // seeded from the committed manifest, then carried across every group
        // this settle rewrites
        let mut ratios = self.seal_ratios();

        for (slot, group) in &plans {
            fan_in = fan_in.max(group.len());
            let files: Vec<(String, u64)> =
                group.iter().map(|s| (s.file.clone(), s.id)).collect();
            let out = match slot {
                LaneSlot::Node => self.merge_node_cluster(
                    &files,
                    target_bytes,
                    &mut ratios,
                    &closes,
                    &mut next_id,
                )?,
                _ => self.merge_edge_cluster(
                    &files,
                    slot.lane(),
                    target_bytes,
                    &mut ratios,
                    &closes,
                    &mut next_id,
                )?,
            };
            rows_rewritten += out.rows;
            closes_folded += out.closes_folded;
            for SettleInput { file: f, .. } in group {
                // the one place a file is touched outside the merge itself:
                // `reclaimable_bytes` is what a following `gc --keep 2` frees,
                // and an operator planning the ~2x disk transient needs the
                // real figure, not the row model's. One `stat` per merged
                // input, never in `needs_settle`.
                reclaimable_bytes += std::fs::metadata(self.root().join(f))
                    .map(|md| md.len())
                    .unwrap_or(0);
                superseded.insert(f.clone());
            }
            replacements.push((*slot, out.entries));
        }

        // Splice, exactly as the incremental path does: drop the merged
        // entries wherever they sat and append their replacements. Lane
        // vectors are not held in key order and nothing downstream depends on
        // that order — the scan sorts by key and the digest never sees a
        // segment.
        for (slot, entries) in replacements {
            let lane = match slot {
                LaneSlot::EdgeEvent => &mut next.edge_lanes.event,
                LaneSlot::EdgeInterval => &mut next.edge_lanes.interval,
                LaneSlot::Node => &mut next.node_store,
            };
            lane.retain(|e| !superseded.contains(&e.file));
            lane.extend(entries);
        }
        next.next_segment_id = next_id;

        // A close run may be dropped only if no record in it can name a
        // surviving segment. The whole-store `compact()` may always drop them
        // and `compact_incremental` may never, "because one run may name both
        // rewritten and surviving segments". A settle is in between and must
        // test rather than assume: it rewrites every lane above its floor, and
        // a lane already at its floor keeps its segments.
        let all_superseded = merge::live_segment_ids(self.manifest())
            .len()
            == superseded.len();
        if all_superseded {
            next.close_runs = Vec::new(); // folded into the new sidecars
        }

        // Stats are unchanged: no row was created or destroyed (I1, I5).
        next.seal();
        let segments_after =
            next.edge_lanes.event.len() + next.edge_lanes.interval.len() + next.node_store.len();
        crate::store::crash_point("settle_before_install");
        let generation_after = self.install(next)?;

        Ok(SettleReport {
            segments_before,
            segments_after,
            closes_folded,
            rows_rewritten,
            rounds: 1,
            fan_in,
            groups: plans.len(),
            budget_bytes: budget,
            inputs_total,
            inputs_below_full_rows,
            reclaimable_bytes,
            generation_before,
            generation_after,
            wall_us: started.elapsed().as_micros() as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::EventLogRef;
    use crate::read::EdgeVersionOut;
    use crate::row::{EdgeRow, NodeRow, RowKind};
    use crate::derive::{edge_eid, version_vid};
    use crate::OPEN_END;
    use std::path::PathBuf;

    fn tmp_root(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-settle-{name}-{}", std::process::id()));
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

    /// `batches` commits of `per` rows each, then two corrections against
    /// committed rows — so the store has many under-target segments (one lane
    /// vector entry per commit, which is exactly the straggler shape a tiered
    /// build leaves) and unfolded close runs. `target` shrinks
    /// `SEGMENT_TARGET_BYTES` so a byte floor is reachable at unit scale.
    fn seeded(name: &str, batches: u32, per: u32, target: u64) -> (PathBuf, NativeStore, Vec<EdgeRow>) {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        let (partitions, _) = {
            let (p, t) = s.layout();
            (*p, t)
        };
        s.set_layout(partitions, target);
        let mut all = Vec::new();
        for batch in 0..batches {
            let tt = 100 + batch as i64;
            s.begin(tt).unwrap();
            let a = s.ensure_entity(&format!("n{batch}a"), "Node").unwrap();
            let b = s.ensure_entity(&format!("n{batch}b"), "Node").unwrap();
            // one node version per endpoint per commit: the node lane is the
            // segment-count problem on the campaign corpus (32 of 54 live
            // segments at 30M, in 0.6 % of the bytes), so the fixture must
            // have one
            for (uid_id, vt) in [(a, batch as i64 * 2), (b, batch as i64 * 2 + 1)] {
                let vid = version_vid(&format!("node-{uid_id}-{vt}"), tt, vt);
                s.stage_node(NodeRow {
                    vid,
                    uid_id,
                    label: "Node".into(),
                    vt_s: vt,
                    vt_e: vt + 1,
                    tt_s: tt,
                    props: "{}".into(),
                    source: "ingest".into(),
                    provenance_ref: None,
                })
                .unwrap();
            }
            for i in 0..per {
                let r = edge(a, b, "n1", "n2", (batch * per + i) as i64, tt, batch * per + i);
                all.push(r.clone());
                s.stage_edge(r).unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        s.begin(900).unwrap();
        s.close_version(RowKind::Edge, all[1].vid, 900).unwrap();
        s.close_version(RowKind::Edge, all[all.len() - 2].vid, 900).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        (root, s, all)
    }

    fn live_segments(s: &NativeStore) -> usize {
        let m = s.manifest();
        m.edge_lanes.event.len() + m.edge_lanes.interval.len() + m.node_store.len()
    }

    // -- the row-byte model, pinned against the planner it mirrors --------- //

    /// `per_row_model`/`full_rows_model` are copies of `merge.rs`'s private
    /// `edge_row_bytes_model`/`node_row_bytes_model`/`full_rows`, duplicated
    /// so this code puts no hunk in `merge.rs`. If `merge.rs` ever changes
    /// its model, this test is the thing that notices: `plan_lane` refuses to
    /// merge a segment at or above the threshold, so a store of segments just
    /// over `full_rows_model` must plan no incremental work at all.
    #[test]
    fn the_row_byte_model_agrees_with_the_merge_planner() {
        assert_eq!(per_row_model(LaneSlot::EdgeEvent), 50);
        assert_eq!(per_row_model(LaneSlot::EdgeInterval), 50);
        assert_eq!(per_row_model(LaneSlot::Node), 44);
        // 4 KiB target -> full at 4096/50/2 = 40 rows, the figure
        // `merge.rs`'s own `a_segment_at_the_byte_target_is_never_merge_material`
        // is written against.
        assert_eq!(full_rows_model(4096, LaneSlot::EdgeEvent), 40);
        let entries: Vec<crate::manifest::SegmentEntry> = (0..9u32)
            .map(|i| crate::manifest::SegmentEntry {
                file: format!("seg/{:012}.tgs", i + 1),
                rows: full_rows_model(4096, LaneSlot::EdgeEvent) as u32,
                key_lo: ((i * 10) as i64, "0".repeat(24)),
                key_hi: ((i * 10 + 9) as i64, "f".repeat(24)),
                vt_min: (i * 10) as i64,
                vt_max: (i * 10 + 9) as i64,
                vt_e_max: (i * 10 + 10) as i64,
                tt_s_min: 1,
                tt_s_max: 1,
                rel_codes: Vec::new(),
                n_closed_folded: 0,
                all_current: true,
                sha: String::new(),
            })
            .collect();
        assert!(
            merge::plan_lane(&entries, LaneSlot::EdgeEvent, 4096, &|_| 0).is_empty(),
            "segments at this module's full_rows_model must be at merge.rs's too"
        );
    }

    // -- the planner ------------------------------------------------------ //

    fn census(slot: LaneSlot, rows: &[u64]) -> LaneCensus {
        LaneCensus {
            slot,
            segments: rows
                .iter()
                .enumerate()
                .map(|(i, &r)| SettleInput {
                    file: format!("seg/{i:012}.tgs"),
                    id: i as u64,
                    rows: r,
                })
                .collect(),
            rows: rows.iter().sum(),
            largest_rows: rows.iter().copied().max().unwrap_or(0),
        }
    }

    #[test]
    fn the_budget_bounds_a_group_by_bytes_not_by_a_segment_count() {
        // ten segments of 10 rows = 500 modelled bytes each
        let c = census(LaneSlot::EdgeEvent, &[10; 10]);
        let groups = plan_groups(&c, 1500);
        assert_eq!(
            groups.iter().map(|g| g.len()).collect::<Vec<_>>(),
            vec![3, 3, 3, 1]
        );
        // a budget that holds the lane takes it in one
        assert_eq!(plan_groups(&c, 1 << 30).len(), 1);
        // and no segment is planned into two groups
        let mut seen = HashSet::new();
        for g in plan_groups(&c, 1500) {
            for p in g {
                assert!(seen.insert(p), "segment {p} planned into two groups");
            }
        }
        // the *count* does not grow with the lane at a fixed budget: this is
        // The planner's claim: a bigger lane splits into more
        // groups rather than opening more segments at once
        let big = census(LaneSlot::EdgeEvent, &[10; 40]);
        let small_max = plan_groups(&c, 1500).iter().map(|g| g.len()).max().unwrap();
        let big_max = plan_groups(&big, 1500).iter().map(|g| g.len()).max().unwrap();
        assert_eq!(small_max, big_max, "the fan-in grew with the lane");
        assert!(plan_groups(&big, 1500).len() > plan_groups(&c, 1500).len());
    }

    #[test]
    fn a_segment_larger_than_the_whole_budget_forms_its_own_group() {
        let c = census(LaneSlot::EdgeEvent, &[2, 180, 2]);
        assert_eq!(plan_groups(&c, 1000), vec![vec![0], vec![1], vec![2]]);
    }

    #[test]
    fn the_byte_floor_divides_lane_rows_by_a_sealed_segments_row_count() {
        let target = 128 * 1024 * 1024;
        // (a) nothing has sealed: 20 segments of 100 rows. 100 * 50 is far
        // under target/2, so the model carries the estimate and the floor is
        // one -- these are stragglers, not segments at the target.
        let stragglers = census(LaneSlot::EdgeEvent, &[100; 20]);
        assert_eq!(stragglers.rows_per_seal(target), target / 50);
        assert_eq!(stragglers.byte_floor(target), 1);

        // (b) the whole-store control's measured 30M event lane: 14 segments
        // of ~2.14M rows. Those *did* seal, so their own row count is the
        // divisor and the floor equals the count -- the lane is left alone,
        // which is what makes a settle idempotent.
        let settled = census(LaneSlot::EdgeEvent, &[2_142_876; 14]);
        assert_eq!(settled.rows_per_seal(target), 2_142_876);
        assert_eq!(settled.byte_floor(target), 14);
        // the model alone would have said 12, and rewritten 30M rows for
        // nothing -- this assertion is the reason `rows_per_seal` exists
        assert_eq!(settled.rows.div_ceil(target / 50) as usize, 12);

        // (c) the measured 30M treatment event lane: 22 segments, mean 1.36M
        // rows, with tier-merged members up at the seal size. Above the
        // floor, so it is merge material.
        let mut rows = vec![1_000_000u64; 21];
        rows.push(2_142_876);
        let tiered = census(LaneSlot::EdgeEvent, &rows);
        assert!(tiered.segments.len() > tiered.byte_floor(target) + 1);

        // (d) an empty lane has no floor and nothing to do
        assert_eq!(census(LaneSlot::EdgeEvent, &[]).byte_floor(target), 0);
    }

    #[test]
    fn the_node_lane_floor_collapses_a_crowd_of_tiny_segments() {
        // the measured 30M node lane: 32 segments, 300,002 rows between them
        let target = 128 * 1024 * 1024;
        let c = census(LaneSlot::Node, &[9_375; 32]);
        assert_eq!(c.byte_floor(target), 1, "300k node rows fit one segment");
        // and one group holds all 32 at the campaign's 4 GB budget, so the
        // lane collapses in a single pass
        assert_eq!(plan_groups(&c, 4_000_000_000).len(), 1);
    }

    #[test]
    fn the_budget_resolves_from_the_argument_then_the_env_then_the_default() {
        assert_eq!(settle_budget(Some("999"), Some(123)), 123);
        assert_eq!(settle_budget(Some("999"), None), 999);
        assert_eq!(settle_budget(None, None), SETTLE_MAX_BYTES);
        // a malformed env value falls through to the default rather than
        // silently planning against 0 (which means unbounded)
        assert_eq!(settle_budget(Some("not-a-number"), None), SETTLE_MAX_BYTES);
        // 0 is "unbounded", passed through rather than clamped away
        assert_eq!(settle_budget(Some("0"), None), 0);
    }

    // -- the operation ---------------------------------------------------- //

    #[test]
    fn settle_preserves_the_logical_store_exactly() {
        let (_root, mut s, _) = seeded("equivalence", 12, 6, 4096);
        let before = sorted(s.all_edge_versions().unwrap());
        let nodes_before = s.all_node_versions().unwrap().len();
        let report = s.settle(None).unwrap();
        assert!(report.rows_rewritten > 0, "the fixture must have work to do");
        let after = sorted(s.all_edge_versions().unwrap());
        assert_eq!(before, after, "settle changed the logical content");
        assert_eq!(s.all_node_versions().unwrap().len(), nodes_before);
    }

    #[test]
    fn settle_does_not_advance_transaction_time() {
        let (_root, mut s, _) = seeded("tt", 12, 6, 4096);
        let created_before = s.manifest().created_tt;
        let r = s.settle(None).unwrap();
        assert_eq!(s.manifest().created_tt, created_before, "settle advanced tt");
        assert_eq!(r.generation_after, r.generation_before + 1);
        assert!(r.generation_after > 0);
    }

    #[test]
    fn historical_belief_queries_agree_either_side_of_a_settle() {
        let (_root, mut s, _) = seeded("historical", 12, 6, 4096);
        let sample = [99i64, 100, 105, 111, 899, 900, 901, OPEN_END];
        let snap = |s: &NativeStore| -> Vec<Vec<String>> {
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
        let before = snap(&s);
        s.settle(None).unwrap();
        assert_eq!(before, snap(&s), "a historical belief query changed");
    }

    #[test]
    fn a_close_after_settle_resolves_through_the_new_segments() {
        let (_root, mut s, all) = seeded("close-after", 12, 6, 4096);
        s.settle(None).unwrap();
        s.begin(1000).unwrap();
        s.close_version(RowKind::Edge, all[3].vid, 1000).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        let last = all[all.len() - 2].vid.to_hex();
        for r in s.all_edge_versions().unwrap() {
            let expected = if r.vid == all[3].vid.to_hex() {
                1000
            } else if r.vid == all[1].vid.to_hex() || r.vid == last {
                900
            } else {
                OPEN_END
            };
            assert_eq!(r.tt_e, expected, "vid {}", r.vid);
        }
    }

    #[test]
    fn settle_reaches_the_byte_floor() {
        // 24 commits, so 24 under-target edge segments and 24 node segments
        let (_root, mut s, _) = seeded("floor", 24, 6, 4096);
        let before = live_segments(&s);
        assert!(before >= 24, "the fixture must start well above the floor");
        let r = s.settle(None).unwrap();
        assert_eq!(r.segments_before, before);
        assert!(
            r.segments_after < before,
            "settle left {} segments, started at {before}",
            r.segments_after
        );
        // the floor plus at most one partial per writer per group
        assert!(
            r.segments_after <= r.groups * 2 + 3,
            "segments_after {} above the floor plus lane partials (groups {})",
            r.segments_after,
            r.groups
        );
    }

    #[test]
    fn settle_rewrites_each_row_exactly_once() {
        let (_root, mut s, _) = seeded("once", 20, 6, 4096);
        let n = s.all_edge_versions().unwrap().len() as u64
            + s.all_node_versions().unwrap().len() as u64;
        let r = s.settle(None).unwrap();
        assert_eq!(r.rounds, 1);
        assert_eq!(
            r.rows_rewritten, n,
            "one settle must rewrite exactly the stored row count"
        );
    }

    #[test]
    fn settle_fan_in_is_bounded_by_the_budget() {
        let (_root, mut s, _) = seeded("budget", 20, 6, 4096);
        // an edge segment of 6 rows models 300 B, so 900 B of budget takes
        // three of them per group and the lane splits several ways
        let tight = s.settle(Some(900)).unwrap();
        assert!(tight.fan_in >= 2, "a group must still make progress");
        assert!(
            tight.groups > 1,
            "a tight budget must split the lane into several groups"
        );
        let (_root2, mut s2, _) = seeded("budget-wide", 20, 6, 4096);
        let wide = s2.settle(Some(1 << 34)).unwrap();
        assert!(
            wide.fan_in > tight.fan_in,
            "a larger budget must plan larger groups ({} vs {})",
            wide.fan_in,
            tight.fan_in
        );
        assert!(wide.groups <= tight.groups);
        // and the budget is reported, so a receipt says what it planned against
        assert_eq!(tight.budget_bytes, 900);
        // the work is the same either way -- a budget buys fewer output
        // partials, never fewer rewritten rows
        assert_eq!(tight.rows_rewritten, wide.rows_rewritten);
    }

    #[test]
    fn settle_is_refused_mid_batch_and_trivial_on_an_empty_store() {
        let root = tmp_root("guards");
        let mut s = NativeStore::open(&root).unwrap();
        let r = s.settle(None).unwrap();
        assert_eq!(r.segments_before, 0);
        assert_eq!(r.segments_after, 0);
        assert_eq!(r.rows_rewritten, 0);
        assert_eq!(r.rounds, 0);
        s.begin(100).unwrap();
        assert!(s.settle(None).is_err(), "settle must refuse mid-batch");
    }

    #[test]
    fn settle_never_reuses_segment_ids() {
        let (_root, mut s, _) = seeded("ids", 16, 6, 4096);
        let before: HashSet<String> = merge::live_segment_ids(s.manifest())
            .iter()
            .map(|i| i.to_string())
            .collect();
        let next_id_before = s.manifest().next_segment_id;
        s.settle(None).unwrap();
        let after: HashSet<String> = merge::live_segment_ids(s.manifest())
            .iter()
            .map(|i| i.to_string())
            .collect();
        assert!(
            before.is_disjoint(&after),
            "a settled segment reused a superseded id"
        );
        assert!(s.manifest().next_segment_id > next_id_before);
    }

    #[test]
    fn settle_output_key_ranges_are_disjoint_within_a_group() {
        let (_root, mut s, _) = seeded("disjoint", 16, 6, 4096);
        let ids_before: HashSet<u64> = merge::live_segment_ids(s.manifest());
        s.settle(None).unwrap();
        let m = s.manifest();
        // the outputs of one settle, in id order: consecutive ids come from
        // one group's ChunkWriter, so their ranges may not overlap
        let mut fresh: Vec<&SegmentEntry> = m
            .edge_lanes
            .event
            .iter()
            .chain(m.edge_lanes.interval.iter())
            .filter(|e| !ids_before.contains(&segment_id_of(&e.file)))
            .collect();
        fresh.sort_by_key(|e| segment_id_of(&e.file));
        for w in fresh.windows(2) {
            if segment_id_of(&w[1].file) != segment_id_of(&w[0].file) + 1 {
                continue; // a group boundary: ranges may legally overlap
            }
            assert!(
                w[0].key_hi < w[1].key_lo,
                "settle split a tie group: {:?} then {:?}",
                w[0].key_hi,
                w[1].key_lo
            );
        }
    }

    #[test]
    fn no_segment_is_planned_into_two_groups() {
        let (_root, s, _) = seeded("two-groups", 24, 6, 4096);
        let plans = s.plan_settle(Some(900));
        assert!(plans.len() >= 2, "the fixture should plan several groups");
        let mut seen = HashSet::new();
        for (slot, g) in &plans {
            for SettleInput { file, .. } in g {
                assert!(
                    seen.insert((*slot, file.clone())),
                    "segment {file} planned into two groups"
                );
            }
        }
    }

    #[test]
    fn settle_empties_the_close_runs_when_it_rewrites_every_segment() {
        let (_root, mut s, _) = seeded("close-runs", 20, 6, 4096);
        assert!(
            !s.manifest().close_runs.is_empty(),
            "the fixture must carry unfolded close runs"
        );
        let r = s.settle(None).unwrap();
        assert!(r.closes_folded >= 2, "the two corrections must fold");
        assert!(
            s.manifest().close_runs.is_empty(),
            "a settle that rewrote every segment must leave no close run"
        );
    }

    #[test]
    fn settle_node_rows_keep_their_lane() {
        let (_root, mut s, _) = seeded("node-lane", 16, 6, 4096);
        let before: Vec<(String, i64, i64)> = {
            let mut v: Vec<_> = s
                .all_node_versions()
                .unwrap()
                .into_iter()
                .map(|n| (n.vid, n.vt_s, n.vt_e))
                .collect();
            v.sort();
            v
        };
        s.settle(None).unwrap();
        let after: Vec<(String, i64, i64)> = {
            let mut v: Vec<_> = s
                .all_node_versions()
                .unwrap()
                .into_iter()
                .map(|n| (n.vid, n.vt_s, n.vt_e))
                .collect();
            v.sort();
            v
        };
        assert_eq!(before, after, "a node row moved or was lost across settle");
        // every surviving node segment must be readable through the lane its
        // own header records -- `merge_node_cluster` routes per row through
        // `PartitionMap::lane_for`, which is the one thing a lane-stamping
        // merge once got wrong
        for e in &s.manifest().node_store {
            let seg = s.open_segment(&e.file).unwrap();
            let lane = seg.header().lane;
            for r in 0..e.rows {
                let vt_s = seg.i64_column("vt_s").unwrap()[r as usize];
                let vt_e = seg
                    .i64_column("vt_e")
                    .map(|c| c[r as usize])
                    .unwrap_or(vt_s + 1);
                assert_eq!(
                    s.layout().0.lane_for(vt_s, vt_e),
                    lane,
                    "node row {r} of {} is in the wrong lane",
                    e.file
                );
            }
        }
    }

    /// The node lane is the segment-count problem on the campaign corpus: at
    /// 30M it holds 32 of 54 live segments (59 %) in 0.6 % of the bytes, at
    /// 0.18 MB each, because every commit appends one. A byte-bounded planner
    /// must take all of them in one group and one pass; a count-bounded one
    /// would need three groups and a second round to reach the same place.
    #[test]
    fn settle_collapses_a_many_tiny_segment_node_lane() {
        let (_root, mut s, _) = seeded("tiny-nodes", 40, 2, 4096);
        let nodes_before = s.manifest().node_store.len();
        assert!(
            nodes_before >= 40,
            "the fixture must start with many tiny node segments, had {nodes_before}"
        );
        let rows_before = s.all_node_versions().unwrap().len();
        let r = s.settle(None).unwrap();
        let nodes_after = s.manifest().node_store.len();
        assert!(
            nodes_after <= 2,
            "a tiny node lane must collapse to one segment per physical lane, left {nodes_after}"
        );
        assert_eq!(r.rounds, 1, "one pass, not several");
        assert_eq!(s.all_node_versions().unwrap().len(), rows_before);
    }

    #[test]
    fn a_second_settle_rewrites_nothing() {
        let (_root, mut s, _) = seeded("idempotent", 24, 6, 4096);
        let first = s.settle(None).unwrap();
        assert!(first.rows_rewritten > 0);
        let gen = s.manifest().generation;
        let second = s.settle(None).unwrap();
        assert_eq!(second.rows_rewritten, 0, "a second settle rewrote rows");
        assert_eq!(second.rounds, 0);
        assert_eq!(second.groups, 0);
        assert_eq!(second.segments_after, second.segments_before);
        assert_eq!(second.segments_before, first.segments_after);
        assert_eq!(
            s.manifest().generation,
            gen,
            "a settle with nothing to do must publish no generation"
        );
        assert!(!s.needs_settle(), "needs_settle must agree with the no-op");
    }

    #[test]
    fn needs_settle_is_true_on_a_tiered_store_and_false_once_settled() {
        let (_root, mut s, _) = seeded("advisory", 24, 6, 4096);
        assert!(s.needs_settle(), "a store of stragglers needs a settle");
        s.settle(None).unwrap();
        assert!(!s.needs_settle(), "a settled store does not");
    }

    #[test]
    fn settled_segments_carry_run_counts_comparable_to_a_whole_store_compaction() {
        // At unit scale: both a settle and the baseline's whole-store
        // reseal merge every generation into one segment, so their `tt_s_run`
        // structure should agree.
        let (_root, mut a, _) = seeded("runs-settle", 16, 6, 4096);
        let (_root2, mut b, _) = seeded("runs-compact", 16, 6, 4096);
        a.settle(None).unwrap();
        b.compact().unwrap();
        let runs = |s: &NativeStore| -> usize {
            let m = s.manifest();
            m.edge_lanes
                .event
                .iter()
                .chain(m.edge_lanes.interval.iter())
                .chain(m.node_store.iter())
                .map(|e| {
                    s.open_segment(&e.file)
                        .map(|g| g.tt_s_run_count())
                        .unwrap_or(0)
                })
                .sum()
        };
        let (ra, rb) = (runs(&a), runs(&b));
        assert!(rb > 0);
        assert!(
            (ra as f64) <= 1.2 * rb as f64,
            "settled run count {ra} is more than 1.2x the compacted {rb}"
        );
    }

    #[test]
    fn the_report_counts_inputs_below_the_merge_planners_byte_target() {
        // The below-target census: on a store of stragglers every input is below
        // `full_rows`, which is the diagnosis -- the segment excess is
        // unfilled tier windows, not segments already at the target.
        let (_root, s, _) = seeded("diagnosis", 16, 6, 4096);
        let r = {
            let mut s = s;
            s.settle(None).unwrap()
        };
        assert!(r.inputs_total >= 16);
        assert_eq!(
            r.inputs_below_full_rows, r.inputs_total,
            "every straggler must count as below the byte target"
        );
        assert!(r.reclaimable_bytes > 0, "gc must have something to reclaim");
    }
}
