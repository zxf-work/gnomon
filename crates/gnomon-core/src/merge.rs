//! Partition-local compaction: cluster planning and the k-way merge writer.
//!
//! `compact()` reads the whole logical store back through the read path and
//! reseals it, so under a fixed op cadence `P` a build of `n` rows rewrites
//! `≈ n²/(2P)` rows — 5.1e9 at 100M, 83.76 % of that build's wall, and
//! ~33 days of arithmetic at 1B. That cost is not in the file
//! format; it is in the *unit*. This module rewrites one **cluster** at a
//! time.
//!
//! **The unit.** A cluster is a maximal set of segments whose composite-key
//! ranges overlap — the grouping `ScanSet::cluster_order` (`scan.rs`) already
//! computes for the scan's fast path, and whose defining property is that
//! "everything in cluster *i* sorts strictly before everything in cluster
//! *i+1*, so clusters concatenate". Here it is computed over the manifest's
//! `key_lo`/`key_hi` with **no segment opened**: those are the full 96-bit
//! composite keys, written as fixed-width lowercase hex, so a byte-wise
//! string comparison is the numeric one.
//!
//! **Why not valid-time partitions**, which the original brief proposed:
//! `PartitionMap` is a lane-assignment device, and on the default synth
//! corpus (`--tick-us 1`, `vt_max` = 105 s against a 7-day partition width)
//! there is exactly one partition, so a partition-keyed compaction would be
//! a whole-store rewrite or a no-op and nothing in between.
//!
//! **Soundness of rewriting a subset.** Segments in one manifest already
//! overlap in key range — that is the premise `cluster_order` exists to
//! handle — so merging any subset is sound; the scan resolves overlap with
//! its heap. The cluster is what keeps the *output's* range narrow, and what
//! bounds the work to `O(rows in the cluster)`.
//!
//! **The writer calls `write_edge_segment`/`write_node_segment`** and never
//! its own encoder. The interval-lane staircase is built
//! inside those functions, as is every header statistic; a hand-rolled
//! encoder would drop the staircase silently and nothing downstream would
//! notice until a prefix-max search returned the wrong block.

use std::collections::{BinaryHeap, HashMap};
use std::cmp::Reverse;
use std::path::Path;

use crate::derive::Id96;
use crate::error::Result;
use crate::manifest::SegmentEntry;
use crate::read::effective_tt_e;
use crate::row::{EdgeRow, Lane, NodeRow, SortKey};
use crate::segment::{write_edge_segment, write_node_segment, SegmentSpec};
use crate::sealsize::SealRatios;
use crate::staging::{edge_row_bytes, entry_for, node_row_bytes};
use crate::store::{segment_id_of, NativeStore};
use crate::visibility::CloseIndex;
use crate::{defaults, OPEN_END};

/// Which manifest vector a merge set lives in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LaneSlot {
    EdgeEvent,
    EdgeInterval,
    Node,
}

impl LaneSlot {
    /// The edge slot a physical lane belongs to — [`Self::lane`]'s inverse,
    /// for the edge lanes only. `Staging::seal` needs it to key the measured
    /// seal quotient (`crate::sealsize`) by slot: staging routes rows to a
    /// `Lane`, while every planner and the manifest's own vectors are per
    /// slot. Never answers `Node`, which is not a lane but a vector.
    pub fn from_edge_lane(lane: Lane) -> Self {
        match lane {
            Lane::Event => LaneSlot::EdgeEvent,
            Lane::Interval => LaneSlot::EdgeInterval,
        }
    }

    /// The physical lane an **edge** rewrite keeps.
    ///
    /// Lane membership is a function of `(vt_s, vt_e)` and a rewrite changes
    /// neither, so an edge segment merged out of one manifest lane vector
    /// belongs to exactly that lane — which is also what keeps the staircase
    /// on the interval lane. Node segments have one vector and carry their
    /// lane in their own header, so `merge_node_cluster` derives it per row
    /// instead and this arm is never the answer for `Node`; it is `Interval`
    /// only because that is the lane a node with an open-ended `vt_e` — the
    /// common shape — lands in.
    pub fn lane(self) -> Lane {
        match self {
            LaneSlot::EdgeEvent => Lane::Event,
            LaneSlot::EdgeInterval | LaneSlot::Node => Lane::Interval,
        }
    }
}

/// What made a cluster eligible. Reported, not just tested: a build record
/// that says *why* it compacted is what separates "the trigger is wrong"
/// from "the merge is slow" when a forecast misses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trigger {
    /// T1 — the cluster holds at least `CLUSTER_RUNS_TRIGGER` segments that
    /// are not already at the byte target.
    Runs,
    /// T2 — unfolded closes reach `COMPACTION_CLOSE_FRACTION` of a segment's
    /// rows, so folding them retires close-run records.
    CloseFraction,
}

/// One rewrite: the segments to merge, and why.
#[derive(Clone, Debug)]
pub struct MergeSet {
    pub slot: LaneSlot,
    /// Indices into the lane's `Vec<SegmentEntry>`, in key order.
    pub members: Vec<usize>,
    /// Rows the merge will read and write.
    pub rows: u64,
    /// Unfolded closes the merge will fold into the new sidecars.
    pub closes: u64,
    pub trigger: Trigger,
}

// ---------------------------------------------------------------------- //
// planning — pure arithmetic over manifest entries, no segment opened      //
// ---------------------------------------------------------------------- //

/// Key order over a lane's entries: the order the merge windows walk.
///
/// Keys are `(i64, String)` where the string is `Id96::to_hex()` —
/// `format!("{:016x}{:08x}")`, fixed width and lowercase — so the derived
/// `Ord` on the tuple is the numeric order on the 96-bit composite key, and
/// nothing has to be parsed. Manifest lane vectors are *not* in key order: a
/// commit appends, and successive commits interleave.
pub fn key_order(entries: &[SegmentEntry]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..entries.len()).collect();
    order.sort_by(|&a, &b| entries[a].key_lo.cmp(&entries[b].key_lo));
    order
}

/// Which size tier a segment belongs to: how many times its row count
/// divides by the fan-in.
///
/// This is the whole write-amplification argument in one function. A merge
/// only ever combines `F` segments of the *same* tier, so its output is one
/// tier up and cannot merge again until `F` segments have reached that tier
/// too. A row is therefore rewritten once per tier it climbs —
/// `log_F(target_rows / rows_per_commit)`, ≈ 3 at 100M with `--batch 50000` —
/// instead of once per compaction cycle.
///
/// Without it the rule degenerates: merging eight *key-adjacent* segments
/// regardless of size drags the one big segment in the window through every
/// round. Measured on a 20k build — 760,927 rows rewritten against the
/// whole-store control's 44,872, i.e. the treatment losing to the thing it
/// replaces.
pub fn size_tier(rows: u64, fan_in: u64) -> u32 {
    let mut t = 0u32;
    let mut r = rows.max(1);
    while r >= fan_in.max(2) {
        r /= fan_in.max(2);
        t += 1;
    }
    t
}

/// Rows at which a segment counts as "at the byte target", from the writer's
/// own per-row model.
///
/// `target_bytes` must be the **accounted** target — what
/// `SealRatios::effective_target` returns for this slot, which is what the
/// writer flushes on. Passing the configured on-disk target instead would
/// make the planner call a correctly sized segment half-full and drag it
/// through a merge with nothing to gain, which is the amplification the tier
/// rule exists to avoid. Every caller resolves it the same way; the arm of
/// `the_row_byte_model_agrees_with_the_merge_planner` in `settle.rs` pins the
/// arithmetic across the two modules.
///
/// The per-row model is `sealsize::per_row_model` — the same 50 B / 44 B the
/// writer accounts each row at; `staging::edge_row_bytes` has no
/// `disc.len()` term, so that one model serves the writer, this
/// threshold and the settle floor (`crate::sealsize`).
///
/// The *half* is the judgement call: two half-full segments merge into one
/// full one, which is the last merge with anything to gain, so at or above
/// half the accounted capacity a segment stops being merge material.
///
/// This decides only **when** work happens, never what the answer is: an
/// over-eager threshold wastes a rewrite, an over-cautious one delays one.
fn full_rows(target_bytes: u64, slot: LaneSlot) -> u64 {
    ((target_bytes / crate::sealsize::per_row_model(slot).max(1)) / 2).max(1)
}

/// [`full_rows`] for callers outside this module.
///
/// `store::seal_ratios` needs the same "did this segment seal on the byte
/// rule" question the planner asks, and asking it a second way is how the two
/// drift apart — `settle.rs` carries a duplicated copy precisely because this
/// was private, and says so in its own docstring.
pub(crate) fn full_rows_at(target_bytes: u64, slot: LaneSlot) -> u64 {
    full_rows(target_bytes, slot)
}

/// Plan one lane: which segments each rewrite should merge, and why.
///
/// `unfolded` gives the number of close-run records naming a segment id —
/// `CloseIndex::segment_closes(id).len()`, an O(1) lookup on the committed
/// index the caller already holds.
///
/// **The unit is `F` same-size-tier segments taken in key order**, skipping
/// any segment already at the byte target. That is the substantive
/// refinement of the cluster rule, and it was arrived at by measuring two
/// wrong answers against the whole-store baseline on a 20k build:
///
/// 1. **Per-cluster.** A cluster is a maximal *overlapping*
///    group, and the synth corpus advances `vt_s` with the op index, so
///    successive commits occupy **disjoint** key ranges and almost every
///    cluster is a singleton. `runs(C) >= RUNS_TRIGGER` therefore never
///    fired: the build finished with 49 segments and
///    `needs_compaction() == false`. At 100M with `--batch 50000` that is
///    ~2,000 permanent segments, against the ≈40 the query model assumes.
/// 2. **Key-adjacent, ignoring size.** Converged to 7 segments but rewrote
///    **760,927** rows against the baseline's 44,872 — the incremental path losing to
///    the thing it replaces, because a window of eight key-neighbours drags
///    whichever one is already large through every round.
///
/// Tiers fix (2) and key order fixes (1). Soundness needs neither: segments
/// in one manifest already overlap in key range — that is why
/// `ScanSet::cluster_order` exists — so merging *any* subset is sound and the
/// scan resolves the result with its heap. What the two rules buy is cost:
/// key order keeps a merged segment's range narrow, and the tier keeps each
/// row's rewrite count at `log_F` of the store rather than one per cycle.
///
/// **The write-amplification cap is structural, not bookkeeping.** The
/// cluster rule states it as "a cluster is not re-eligible under T1 until it has
/// accumulated `RUNS_TRIGGER` new runs since its last rewrite", kept in a
/// rebuildable `idx/compact/state.json` sidecar. That needs a cluster
/// *identity* surviving a generation, which the manifest cannot express — a
/// cluster is derived from key ranges the rewrite itself changes. The tier
/// rule gives the same bound for free, with no sidecar, no new file for gc to
/// sweep, and nothing to rebuild after a crash. The worst case ("a
/// rewrite is at worst one whole-store pass") cannot arise: a merge set never
/// exceeds `F` segments, none of them at the byte target.
///
/// **T3 is subsumed, deliberately.** The third trigger — half a cluster's
/// segments below `SEGMENT_TARGET_BYTES/8` — fires on any two freshly
/// committed segments, and merging pairs is `log₂` amplification, three times
/// the `log₈` the cost model assumes. The tier rule already sweeps every
/// tiny-segment accumulation T3 was written for (`--batch 250`'s ~4,000
/// segments per cycle share a tier), so T3 is not a separate trigger here.
///
/// **T2 counts only *unfolded* closes.** Defining `close_frac` as
/// `Σ n_closed_folded / Σ rows` plus the run records. Taken literally it never
/// terminates: rewriting a segment with folded closes produces a segment with
/// the same folded closes, so anything once past the threshold is eligible
/// forever and compacts on every batch. Folding is what the rewrite reclaims,
/// so the trigger counts what is left to fold.
///
/// **No segment appears in two sets.** One call rewrites every set it plans,
/// so a segment in two of them would be read twice, written twice and
/// superseded once — duplicating its rows in the logical store. The `used`
/// set is that invariant, and `no_segment_is_planned_into_two_sets` pins it.
pub fn plan_lane(
    entries: &[SegmentEntry],
    slot: LaneSlot,
    target_bytes: u64,
    unfolded: &dyn Fn(u64) -> u64,
) -> Vec<MergeSet> {
    let fan_in = defaults::CLUSTER_RUNS_TRIGGER as usize;
    let full_at = full_rows(target_bytes, slot);
    let mut out: Vec<MergeSet> = Vec::new();
    let mut used: std::collections::HashSet<usize> = std::collections::HashSet::new();

    // --- T1: `F` same-tier segments, taken in key order.
    let mut open: HashMap<u32, Vec<usize>> = HashMap::new();
    for i in key_order(entries) {
        let rows = entries[i].rows as u64;
        if rows >= full_at {
            // already at the byte target: another merge has nothing to gain
            // from it, and dragging it through one is the amplification this
            // rule exists to avoid
            continue;
        }
        let w = open.entry(size_tier(rows, fan_in as u64)).or_default();
        w.push(i);
        if w.len() >= fan_in {
            let members = std::mem::take(w);
            used.extend(members.iter().copied());
            out.push(set_of(entries, slot, members, Trigger::Runs, unfolded));
        }
    }

    // --- T2: any remaining segment with enough unfolded closes to be worth
    // rewriting on its own account. Folding retires close-run records, which
    // is the one thing a rewrite shrinks rather than reshapes.
    for i in 0..entries.len() {
        if used.contains(&i) {
            continue;
        }
        let e = &entries[i];
        let n = unfolded(segment_id_of(&e.file));
        if n > 0 && (n as f64) >= defaults::COMPACTION_CLOSE_FRACTION * e.rows.max(1) as f64 {
            used.insert(i);
            out.push(set_of(
                entries,
                slot,
                vec![i],
                Trigger::CloseFraction,
                unfolded,
            ));
        }
    }
    out
}

fn set_of(
    entries: &[SegmentEntry],
    slot: LaneSlot,
    members: Vec<usize>,
    trigger: Trigger,
    unfolded: &dyn Fn(u64) -> u64,
) -> MergeSet {
    let rows = members.iter().map(|&i| entries[i].rows as u64).sum();
    let closes = members
        .iter()
        .map(|&i| unfolded(segment_id_of(&entries[i].file)))
        .sum();
    MergeSet {
        slot,
        members,
        rows,
        closes,
        trigger,
    }
}

/// Order the eligible sets by what each reclaims and take what fits in
/// `budget_rows`.
///
/// "Reclaims" is, in order: closes to fold (they retire close-run records,
/// the only thing that shrinks state rather than reshaping it), then segments
/// merged, then the smaller rewrite first. The first set is always taken even
/// when it alone exceeds the budget — a budget that can starve makes no
/// progress at all, and the merge is bounded by `CLUSTER_RUNS_TRIGGER`
/// segments regardless.
pub fn take_within_budget(mut sets: Vec<MergeSet>, budget_rows: Option<u64>) -> Vec<MergeSet> {
    sets.sort_by_key(|s| {
        (
            Reverse(s.closes),
            Reverse(s.members.len()),
            s.rows,
            s.slot,
        )
    });
    let Some(budget) = budget_rows else {
        return sets;
    };
    let mut taken = Vec::new();
    let mut acc = 0u64;
    for s in sets {
        if taken.is_empty() || acc + s.rows <= budget {
            acc += s.rows;
            taken.push(s);
        }
    }
    taken
}

// ---------------------------------------------------------------------- //
// the merge writer                                                        //
// ---------------------------------------------------------------------- //

/// What one merged cluster produced.
pub struct MergeOut {
    pub entries: Vec<SegmentEntry>,
    pub rows: u64,
    pub closes_folded: usize,
}

/// A heap entry: the next unconsumed row of one input, keyed for the merge.
struct Cursor {
    key: SortKey,
    input: usize,
    row: u32,
}

impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        (self.key, self.input) == (other.key, other.input)
    }
}
impl Eq for Cursor {}
impl PartialOrd for Cursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Cursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // `input` breaks key ties deterministically, so a merge of segments
        // that share a composite key is reproducible rather than
        // heap-order-dependent.
        (self.key, self.input).cmp(&(other.key, other.input))
    }
}

impl NativeStore {
    /// Merge one edge cluster into fresh segments, streaming.
    ///
    /// Inputs are opened **uncached** and are alive only for this call, so
    /// the memory is `fan-in × one segment` plus one output segment's
    /// builder — flat in the store's size, which is the whole point. Each row is copied column-to-column: no `edge_eid` sha256 (the
    /// dominant per-row cost of the read path), no dictionary lookup
    /// (endpoints are already dense ids), no `vid` hex. `write_edge_segment`
    /// does the encoding, so the staircase and every header statistic are
    /// rebuilt rather than reimplemented.
    pub(crate) fn merge_edge_cluster(
        &self,
        files: &[(String, u64)],
        lane: Lane,
        target_bytes: u64,
        ratios: &mut SealRatios,
        closes: &CloseIndex,
        next_id: &mut u64,
    ) -> Result<MergeOut> {
        let segs: Vec<_> = files
            .iter()
            .map(|(f, _)| self.open_segment_uncached(f))
            .collect::<Result<Vec<_>>>()?;

        struct Cols<'a> {
            id: u64,
            vt_s: &'a [i64],
            vt_e: Option<&'a [i64]>,
            vid_hi: &'a [u64],
            vid_lo: &'a [u32],
            src: &'a [u32],
            dst: &'a [u32],
            rel: &'a [u16],
            props: &'a [u32],
            disc: &'a [u32],
            source: &'a [u32],
            prov: &'a [u32],
            strings: crate::segment::StringView<'a>,
            sidecar: crate::visibility::Sidecar<'a>,
            header: &'a crate::segment::SegmentHeader,
            /// Resolved once per input, never per row: on a dictionary-coded
            /// segment this looks a column up by name.
            tt_s: crate::ttime::TtSView<'a>,
        }

        let mut cols = Vec::with_capacity(segs.len());
        for (i, seg) in segs.iter().enumerate() {
            let h = seg.header();
            cols.push(Cols {
                id: files[i].1,
                vt_s: seg.i64_column("vt_s")?,
                vt_e: if h.vt_e_elided {
                    None
                } else {
                    Some(seg.i64_column("vt_e")?)
                },
                vid_hi: seg.u64_column("vid64")?,
                vid_lo: seg.u32_column("vid_lo32")?,
                src: seg.u32_column("src_id")?,
                dst: seg.u32_column("dst_id")?,
                rel: seg.u16_column("rel_code")?,
                props: seg.u32_column("props_ref")?,
                disc: seg.u32_column("disc_ref")?,
                source: seg.u32_column("source_ref")?,
                prov: seg.u32_column("prov_ref")?,
                strings: seg.strings()?,
                sidecar: seg.sidecar(),
                header: h,
                tt_s: seg.tt_s_view()?,
            });
        }

        let key_at = |c: &Cols<'_>, row: u32| -> SortKey {
            let r = row as usize;
            (
                c.vt_s[r],
                Id96 {
                    hi: c.vid_hi[r],
                    lo: c.vid_lo[r],
                },
            )
        };

        let mut heap: BinaryHeap<Reverse<Cursor>> = BinaryHeap::with_capacity(cols.len());
        for (i, c) in cols.iter().enumerate() {
            if !c.vt_s.is_empty() {
                heap.push(Reverse(Cursor {
                    key: key_at(c, 0),
                    input: i,
                    row: 0,
                }));
            }
        }

        let slot = LaneSlot::from_edge_lane(lane);
        let input_rows: usize = cols.iter().map(|c| c.vt_s.len()).sum();
        let mut writer = ChunkWriter::new(
            self.root().join("seg"),
            lane,
            slot,
            target_bytes,
            ratios,
            input_rows,
        );
        let mut out = MergeOut {
            entries: Vec::new(),
            rows: 0,
            closes_folded: 0,
        };

        while let Some(Reverse(cur)) = heap.pop() {
            let c = &cols[cur.input];
            let r = cur.row as usize;
            let rel_type = c
                .header
                .rel_types
                .get(c.rel[r] as usize)
                .ok_or_else(|| {
                    crate::error::EngineError::corrupt(format!(
                        "rel_code {} has no entry",
                        c.rel[r]
                    ))
                    .at_row(cur.row)
                })?
                .clone();
            let row = EdgeRow {
                vid: Id96 {
                    hi: c.vid_hi[r],
                    lo: c.vid_lo[r],
                },
                src_id: c.src[r],
                dst_id: c.dst[r],
                rel_type,
                disc: c.strings.get(c.disc[r])?.to_string(),
                vt_s: c.vt_s[r],
                vt_e: c.vt_e.map(|v| v[r]).unwrap_or(c.vt_s[r] + 1),
                tt_s: c.tt_s.at(cur.row)?,
                props: c.strings.get(c.props[r])?.to_string(),
                source: c.strings.get(c.source[r])?.to_string(),
                provenance_ref: c.strings.get_opt(c.prov[r])?.map(str::to_string),
            };
            // the close run outranks the folded sidecar, exactly as every
            // read path resolves it — one spelling, `read::effective_tt_e`
            let tt_e = effective_tt_e(closes, &c.sidecar, c.id, cur.row);

            // advance this input before the flush decision, so the writer can
            // see whether the next row shares this one's key
            if (cur.row as usize + 1) < c.vt_s.len() {
                heap.push(Reverse(Cursor {
                    key: key_at(c, cur.row + 1),
                    input: cur.input,
                    row: cur.row + 1,
                }));
            }
            let next_key = heap.peek().map(|Reverse(n)| n.key);

            let bytes = edge_row_bytes(&row) as u64;
            writer.push(row, tt_e, bytes);
            out.rows += 1;
            if tt_e != OPEN_END {
                out.closes_folded += 1;
            }
            if writer.should_flush(ratios, next_key) {
                out.entries.push(writer.flush_edges(ratios, next_id)?);
            }
        }
        if !writer.is_empty() {
            out.entries.push(writer.flush_edges(ratios, next_id)?);
        }
        Ok(out)
    }

    /// The node analogue of [`merge_edge_cluster`](Self::merge_edge_cluster),
    /// with one difference that matters: **it routes rows to lanes itself.**
    ///
    /// An edge segment's lane is the manifest vector it sits in, so rewriting
    /// within a lane preserves it by construction. Node segments have one
    /// vector and carry their lane in their own header — `scan_nodes` reads
    /// `segment.header().lane` for every target — so a merge that stamped one
    /// lane on its output would move rows between lanes whenever a set mixed
    /// them, and `SegmentEntry` cannot record a lane to plan around (growing
    /// it would change the manifest format). The rows decide instead, through the store's own
    /// `PartitionMap::lane_for`, which is exactly what `Staging::seal`'s
    /// `split_lanes` does — so a merged node segment is indistinguishable
    /// from a freshly sealed one, staircase included.
    pub(crate) fn merge_node_cluster(
        &self,
        files: &[(String, u64)],
        target_bytes: u64,
        ratios: &mut SealRatios,
        closes: &CloseIndex,
        next_id: &mut u64,
    ) -> Result<MergeOut> {
        let segs: Vec<_> = files
            .iter()
            .map(|(f, _)| self.open_segment_uncached(f))
            .collect::<Result<Vec<_>>>()?;

        struct Cols<'a> {
            id: u64,
            vt_s: &'a [i64],
            vt_e: Option<&'a [i64]>,
            vid_hi: &'a [u64],
            vid_lo: &'a [u32],
            uid_id: &'a [u32],
            label: &'a [u32],
            props: &'a [u32],
            source: &'a [u32],
            prov: &'a [u32],
            strings: crate::segment::StringView<'a>,
            sidecar: crate::visibility::Sidecar<'a>,
            /// Resolved once per input, never per row: on a dictionary-coded
            /// segment this looks a column up by name.
            tt_s: crate::ttime::TtSView<'a>,
        }

        let mut cols = Vec::with_capacity(segs.len());
        for (i, seg) in segs.iter().enumerate() {
            let h = seg.header();
            cols.push(Cols {
                id: files[i].1,
                vt_s: seg.i64_column("vt_s")?,
                vt_e: if h.vt_e_elided {
                    None
                } else {
                    Some(seg.i64_column("vt_e")?)
                },
                vid_hi: seg.u64_column("vid64")?,
                vid_lo: seg.u32_column("vid_lo32")?,
                uid_id: seg.u32_column("uid_id")?,
                label: seg.u32_column("label_ref")?,
                props: seg.u32_column("props_ref")?,
                source: seg.u32_column("source_ref")?,
                prov: seg.u32_column("prov_ref")?,
                strings: seg.strings()?,
                sidecar: seg.sidecar(),
                tt_s: seg.tt_s_view()?,
            });
        }

        let key_at = |c: &Cols<'_>, row: u32| -> SortKey {
            let r = row as usize;
            (
                c.vt_s[r],
                Id96 {
                    hi: c.vid_hi[r],
                    lo: c.vid_lo[r],
                },
            )
        };

        let mut heap: BinaryHeap<Reverse<Cursor>> = BinaryHeap::with_capacity(cols.len());
        for (i, c) in cols.iter().enumerate() {
            if !c.vt_s.is_empty() {
                heap.push(Reverse(Cursor {
                    key: key_at(c, 0),
                    input: i,
                    row: 0,
                }));
            }
        }

        let partitions = *self.layout().0;
        let seg_dir = self.root().join("seg");
        // one slot, two lanes: the manifest keeps node segments in a single
        // vector, so both writers read and feed `LaneSlot::Node`'s quotient
        // the two lanes split the same input rows between them, so capping
        // each writer's reservation at the whole input over-reserves by at
        // most one lane's share — bounded, and the node lane is small
        let input_rows: usize = cols.iter().map(|c| c.vt_s.len()).sum();
        let mut event = ChunkWriter::new(
            seg_dir.clone(),
            Lane::Event,
            LaneSlot::Node,
            target_bytes,
            ratios,
            input_rows,
        );
        let mut interval = ChunkWriter::new(
            seg_dir,
            Lane::Interval,
            LaneSlot::Node,
            target_bytes,
            ratios,
            input_rows,
        );
        let mut out = MergeOut {
            entries: Vec::new(),
            rows: 0,
            closes_folded: 0,
        };

        while let Some(Reverse(cur)) = heap.pop() {
            let c = &cols[cur.input];
            let r = cur.row as usize;
            let row = NodeRow {
                vid: Id96 {
                    hi: c.vid_hi[r],
                    lo: c.vid_lo[r],
                },
                uid_id: c.uid_id[r],
                label: c.strings.get(c.label[r])?.to_string(),
                vt_s: c.vt_s[r],
                vt_e: c.vt_e.map(|v| v[r]).unwrap_or(c.vt_s[r] + 1),
                tt_s: c.tt_s.at(cur.row)?,
                props: c.strings.get(c.props[r])?.to_string(),
                source: c.strings.get(c.source[r])?.to_string(),
                provenance_ref: c.strings.get_opt(c.prov[r])?.map(str::to_string),
            };
            let tt_e = effective_tt_e(closes, &c.sidecar, c.id, cur.row);

            if (cur.row as usize + 1) < c.vt_s.len() {
                heap.push(Reverse(Cursor {
                    key: key_at(c, cur.row + 1),
                    input: cur.input,
                    row: cur.row + 1,
                }));
            }
            let next_key = heap.peek().map(|Reverse(n)| n.key);

            let bytes = node_row_bytes(&row) as u64;
            let writer = match partitions.lane_for(row.vt_s, row.vt_e) {
                Lane::Event => &mut event,
                Lane::Interval => &mut interval,
            };
            writer.push_node(row, tt_e, bytes);
            out.rows += 1;
            if tt_e != OPEN_END {
                out.closes_folded += 1;
            }
            // Each lane's stream is globally ordered on its own, so the
            // tie-group rule still holds per writer: `next_key` is the next
            // row of *either* lane, and a boundary that is safe against the
            // merged stream is safe against a subsequence of it.
            if writer.should_flush(ratios, next_key) {
                out.entries.push(writer.flush_nodes(ratios, next_id)?);
            }
        }
        for writer in [&mut event, &mut interval] {
            if !writer.is_empty() {
                out.entries.push(writer.flush_nodes(ratios, next_id)?);
            }
        }
        Ok(out)
    }
}

/// Buffers merged rows and seals one output segment at a time.
///
/// The byte target and the tie-group rule are `staging::chunk_by_size`'s,
/// applied to a stream instead of to a sorted slice: a chunk may only close
/// on a row whose composite key differs from the next one's, or segment key
/// ranges would overlap within a single rewrite and manifest pruning would be
/// unsound. The stream is globally ordered, so equal keys are
/// adjacent and the one-row lookahead is all the rule needs.
struct ChunkWriter {
    dir: std::path::PathBuf,
    lane: Lane,
    /// Which slot's quotient this writer reads and feeds. `merge_node_cluster`
    /// runs two writers on two lanes but one slot, so this is not derivable
    /// from `lane`.
    slot: LaneSlot,
    /// The configured **on-disk** target. The accounted target is this
    /// divided through by the slot's measured quotient, recomputed at every
    /// flush decision rather than cached, because the quotient moves as this
    /// writer seals.
    base_target: u64,
    /// Exact resident bytes at which the chunk seals regardless of the
    /// accounted target. `0` disables it.
    resident_cap: u64,
    /// Rows to reserve for the row vector at the start of each chunk.
    ///
    /// **Why reserve at all.** A `Vec` that grows geometrically holds the old
    /// buffer and the new one at once while it copies, so at the final
    /// doubling its transient footprint is three times its steady one — and
    /// the steady one is the dominant part of this writer's resident set.
    /// Left alone that term would grow with the corrected target and add
    /// ~1.4 GB to the peak of a 128 MiB-on-disk edge merge, which is larger
    /// than the buffer the correction is actually asking for. The row count
    /// one chunk will hold is knowable in advance — the smaller of what the
    /// inputs carry and what the accounted target admits — so it is reserved
    /// exactly once and the doubling never happens.
    reserve_rows: usize,
    edges: Vec<EdgeRow>,
    nodes: Vec<NodeRow>,
    closed: Vec<(u32, i64)>,
    acc: u64,
    resident: u64,
    last_key: Option<SortKey>,
}

impl ChunkWriter {
    /// `input_rows` is what the merge's inputs carry in total; it caps the
    /// reservation so a lane of a few thousand node rows does not reserve a
    /// seal's worth of them.
    fn new(
        dir: std::path::PathBuf,
        lane: Lane,
        slot: LaneSlot,
        base_target: u64,
        ratios: &SealRatios,
        input_rows: usize,
    ) -> Self {
        let per_row = crate::sealsize::per_row_model(slot).max(1);
        let admits = (ratios.effective_target(slot, base_target) / per_row) as usize;
        Self {
            dir,
            lane,
            slot,
            base_target,
            resident_cap: crate::sealsize::chunk_resident_cap(),
            reserve_rows: admits.min(input_rows),
            edges: Vec::new(),
            nodes: Vec::new(),
            closed: Vec::new(),
            acc: 0,
            resident: 0,
            last_key: None,
        }
    }

    /// Reserve the chunk's row vector once, at its first row.
    ///
    /// `flush_*` leaves a fresh zero-capacity `Vec` behind (`mem::take`), so
    /// a zero capacity is exactly the "this chunk has just started" signal.
    fn reserve<T>(v: &mut Vec<T>, rows: usize) {
        if v.capacity() == 0 && rows > 0 {
            v.reserve_exact(rows);
        }
    }

    fn push(&mut self, row: EdgeRow, tt_e: i64, bytes: u64) {
        Self::reserve(&mut self.edges, self.reserve_rows);
        if tt_e != OPEN_END {
            self.closed.push((self.edges.len() as u32, tt_e));
        }
        self.last_key = Some(row.sort_key());
        self.resident += crate::sealsize::edge_resident_bytes(&row);
        self.edges.push(row);
        self.acc += bytes;
    }

    fn push_node(&mut self, row: NodeRow, tt_e: i64, bytes: u64) {
        Self::reserve(&mut self.nodes, self.reserve_rows);
        if tt_e != OPEN_END {
            self.closed.push((self.nodes.len() as u32, tt_e));
        }
        self.last_key = Some(row.sort_key());
        self.resident += crate::sealsize::node_resident_bytes(&row);
        self.nodes.push(row);
        self.acc += bytes;
    }

    fn is_empty(&self) -> bool {
        self.edges.is_empty() && self.nodes.is_empty()
    }

    /// Seal here?
    ///
    /// Two independent reasons to be full, and the tie rule vetoes both: a
    /// boundary that split a tie group would overlap two segments' key ranges
    /// and make manifest pruning unsound, which a memory cap is
    /// not licence to do. The cap is why exceeding it is possible but
    /// bounded — the overshoot is one tie group, not one segment.
    fn should_flush(&self, ratios: &SealRatios, next_key: Option<SortKey>) -> bool {
        let target = ratios.effective_target(self.slot, self.base_target);
        let full = self.acc >= target
            || (self.resident_cap > 0 && self.resident >= self.resident_cap);
        full && next_key.is_some() && next_key != self.last_key && !self.is_empty()
    }

    fn path(&self, next_id: &mut u64) -> (std::path::PathBuf, u64) {
        let id = *next_id;
        *next_id += 1;
        (self.dir.join(format!("{id:012}.tgs")), id)
    }

    fn flush_edges(&mut self, ratios: &mut SealRatios, next_id: &mut u64) -> Result<SegmentEntry> {
        let t = crate::ingest_phase::Timer::start();
        let out = self.flush_edges_inner(ratios, next_id);
        t.stop(crate::ingest_phase::G::IncFlushNs);
        out
    }

    fn flush_edges_inner(&mut self, ratios: &mut SealRatios, next_id: &mut u64) -> Result<SegmentEntry> {
        let (path, _) = self.path(next_id);
        let rows = std::mem::take(&mut self.edges);
        let closed = std::mem::take(&mut self.closed);
        let modelled = std::mem::take(&mut self.acc);
        self.resident = 0;
        self.last_key = None;
        let entry = write_segment_and_entry(
            &path,
            &rows,
            &closed,
            self.lane,
            write_edge_segment,
        )?;
        observe_sealed(ratios, self.slot, &path, modelled);
        Ok(entry)
    }

    fn flush_nodes(&mut self, ratios: &mut SealRatios, next_id: &mut u64) -> Result<SegmentEntry> {
        let t = crate::ingest_phase::Timer::start();
        let out = self.flush_nodes_inner(ratios, next_id);
        t.stop(crate::ingest_phase::G::IncFlushNs);
        out
    }

    fn flush_nodes_inner(&mut self, ratios: &mut SealRatios, next_id: &mut u64) -> Result<SegmentEntry> {
        let (path, _) = self.path(next_id);
        let rows = std::mem::take(&mut self.nodes);
        let closed = std::mem::take(&mut self.closed);
        let modelled = std::mem::take(&mut self.acc);
        self.resident = 0;
        self.last_key = None;
        let entry = write_segment_and_entry(
            &path,
            &rows,
            &closed,
            self.lane,
            write_node_segment,
        )?;
        observe_sealed(ratios, self.slot, &path, modelled);
        Ok(entry)
    }
}

/// Fold one just-written segment's file size into `ratios`, against the
/// accounted bytes the writer had accumulated for exactly those rows.
///
/// A failed `stat` costs one observation and nothing else:
/// `SealRatio::observe` ignores a zero, so the previous estimate stands and
/// the segment — already written, about to be described — is unaffected.
fn observe_sealed(
    ratios: &mut SealRatios,
    slot: LaneSlot,
    path: &std::path::Path,
    modelled: u64,
) {
    let encoded = std::fs::metadata(path).map(|md| md.len()).unwrap_or(0);
    ratios.get_mut(slot).observe(encoded, modelled);
}

/// Seal one chunk and describe it, exactly as `Staging::seal` does.
///
/// `entry_for` is shared with staging rather than reimplemented: the manifest
/// entry a merged segment gets — its key range, its `vt`/`tt` bounds, its
/// `n_closed_folded`, its `all_current` flag — must be computed by the same
/// code that computes it for a freshly staged one, or the two paths could
/// describe identical bytes differently.
fn write_segment_and_entry<T>(
    path: &Path,
    rows: &[T],
    closed: &[(u32, i64)],
    lane: Lane,
    write: impl Fn(&Path, &[T], &SegmentSpec) -> Result<String>,
) -> Result<SegmentEntry>
where
    T: RowFields,
{
    let body_sha = write(
        path,
        rows,
        &SegmentSpec {
            lane,
            closed_rows: closed.to_vec(),
            ..Default::default()
        },
    )?;
    let mut entry = entry_for(
        path,
        body_sha,
        rows,
        |r| r.sort_key(),
        |r| r.vt_s(),
        |r| r.vt_e(),
        |r| r.tt_s(),
    )?;
    entry.n_closed_folded = closed.len() as u32;
    entry.all_current = closed.is_empty();
    Ok(entry)
}

/// The accessors `entry_for` needs, so one generic body serves both kinds.
pub trait RowFields {
    fn sort_key(&self) -> SortKey;
    fn vt_s(&self) -> i64;
    fn vt_e(&self) -> i64;
    fn tt_s(&self) -> i64;
}

impl RowFields for EdgeRow {
    fn sort_key(&self) -> SortKey {
        EdgeRow::sort_key(self)
    }
    fn vt_s(&self) -> i64 {
        self.vt_s
    }
    fn vt_e(&self) -> i64 {
        self.vt_e
    }
    fn tt_s(&self) -> i64 {
        self.tt_s
    }
}

impl RowFields for NodeRow {
    fn sort_key(&self) -> SortKey {
        NodeRow::sort_key(self)
    }
    fn vt_s(&self) -> i64 {
        self.vt_s
    }
    fn vt_e(&self) -> i64 {
        self.vt_e
    }
    fn tt_s(&self) -> i64 {
        self.tt_s
    }
}

/// Close-run records that name no live segment, as a fraction of all of them.
///
/// A dead record is harmless — `CloseIndex` is keyed `(segment, row)` and a
/// dead id is no longer in the generation's file map, so nothing ever
/// resolves it — but they accumulate, and every generation pays to read and
/// index them. Counted against the in-memory index rather than by re-reading
/// the run files, so the decision itself costs no I/O.
pub fn dead_close_fraction(closes: &CloseIndex, live: &std::collections::HashSet<u64>) -> f64 {
    let mut total = 0usize;
    let mut dead = 0usize;
    for (seg, _row) in closes.closed_addresses() {
        total += 1;
        if !live.contains(&seg) {
            dead += 1;
        }
    }
    if total == 0 {
        0.0
    } else {
        dead as f64 / total as f64
    }
}

/// Every segment id the manifest names, in any lane.
pub fn live_segment_ids(m: &crate::manifest::Manifest) -> std::collections::HashSet<u64> {
    m.edge_lanes
        .event
        .iter()
        .chain(m.edge_lanes.interval.iter())
        .chain(m.node_store.iter())
        .map(|e| segment_id_of(&e.file))
        .collect()
}

/// `segment id -> unfolded close count`, for the trigger.
pub fn unfolded_by_segment(
    closes: &CloseIndex,
    m: &crate::manifest::Manifest,
) -> HashMap<u64, u64> {
    let mut out = HashMap::new();
    for id in live_segment_ids(m) {
        let n = closes.segment_closes(id).len() as u64;
        if n > 0 {
            out.insert(id, n);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(file: &str, lo: (i64, &str), hi: (i64, &str), rows: u32) -> SegmentEntry {
        SegmentEntry {
            file: format!("seg/{file}"),
            rows,
            key_lo: (lo.0, lo.1.to_string()),
            key_hi: (hi.0, hi.1.to_string()),
            vt_min: lo.0,
            vt_max: hi.0,
            vt_e_max: hi.0 + 1,
            tt_s_min: 1,
            tt_s_max: 1,
            rel_codes: Vec::new(),
            n_closed_folded: 0,
            all_current: true,
            sha: String::new(),
        }
    }

    const A: &str = "000000000000000000000001";
    const B: &str = "000000000000000000000002";

    #[test]
    fn key_order_is_numeric_order_on_the_composite_key() {
        // manifest lane vectors are in commit order, not key order
        let entries = vec![
            entry("000000000001.tgs", (30, A), (40, B), 10),
            entry("000000000002.tgs", (0, A), (10, B), 10),
            entry("000000000003.tgs", (30, A), (50, B), 10),
        ];
        // the two sharing vt_s = 30 are separated by the vid half, and the
        // hex is fixed width so a byte comparison is the numeric one
        assert_eq!(key_order(&entries), vec![1, 0, 2]);
    }

    #[test]
    fn a_size_tier_is_how_many_times_the_row_count_divides_by_the_fan_in() {
        assert_eq!(size_tier(0, 8), 0);
        assert_eq!(size_tier(1, 8), 0);
        assert_eq!(size_tier(7, 8), 0);
        assert_eq!(size_tier(8, 8), 1);
        assert_eq!(size_tier(63, 8), 1);
        assert_eq!(size_tier(64, 8), 2);
        // a merge of eight same-tier segments lands one tier up, which is
        // the whole amplification argument
        assert_eq!(size_tier(8 * 400, 8), size_tier(400, 8) + 1);
    }

    #[test]
    fn a_merge_set_is_one_tier_of_f_segments() {
        // twelve segments of the same size, all in one key range
        let mut entries = Vec::new();
        for i in 0..12u32 {
            entries.push(entry(&format!("{:012}.tgs", i + 1), (0, A), (100, B), 100));
        }
        let sets = plan_lane(&entries, LaneSlot::EdgeEvent, 1 << 30, &|_| 0);
        assert_eq!(sets.len(), 1, "twelve same-tier segments make one set of eight");
        let s = &sets[0];
        assert_eq!(s.trigger, Trigger::Runs);
        assert_eq!(s.members.len(), defaults::CLUSTER_RUNS_TRIGGER as usize);
        assert_eq!(s.members, (0..8usize).collect::<Vec<_>>());
        // the remaining four wait rather than pair-merging, which is what
        // keeps the amplification at log_8 rather than log_2
    }

    /// The first of the two measured wrong answers (see `plan_lane`): the
    /// synth corpus advances `vt_s` with the op index, so successive commits
    /// occupy disjoint key ranges. A trigger keyed on overlap never fires.
    #[test]
    fn key_adjacent_segments_merge_even_though_no_two_overlap() {
        let entries: Vec<SegmentEntry> = (0..10u32)
            .map(|i| {
                let lo = (i * 10) as i64;
                entry(&format!("{:012}.tgs", i + 1), (lo, A), (lo + 9, B), 100)
            })
            .collect();
        // no two ranges touch
        for w in entries.windows(2) {
            assert!(w[0].key_hi < w[1].key_lo);
        }
        let sets = plan_lane(&entries, LaneSlot::EdgeEvent, 1 << 30, &|_| 0);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].members, (0..8usize).collect::<Vec<_>>());
    }

    /// The second: a window of key-neighbours that ignores size drags the
    /// one big segment through every round, which on a 20k build cost
    /// 760,927 rewritten rows against the whole-store control's 44,872.
    #[test]
    fn a_big_segment_is_never_merged_with_small_neighbours() {
        let mut entries: Vec<SegmentEntry> = (0..9u32)
            .map(|i| {
                let lo = (i * 10) as i64;
                entry(&format!("{:012}.tgs", i + 1), (lo, A), (lo + 9, B), 100)
            })
            .collect();
        entries[4].rows = 20_000; // four tiers up, sitting mid-key-order
        let sets = plan_lane(&entries, LaneSlot::EdgeEvent, 1 << 30, &|_| 0);
        assert_eq!(sets.len(), 1);
        assert!(
            !sets[0].members.contains(&4),
            "the big segment must not join its small neighbours' tier"
        );
        assert_eq!(sets[0].members, vec![0, 1, 2, 3, 5, 6, 7, 8]);
    }

    #[test]
    fn a_segment_at_the_byte_target_is_never_merge_material() {
        // 4 KiB target -> full at 4096/50/2 = 40 rows
        let entries: Vec<SegmentEntry> = (0..9u32)
            .map(|i| {
                let lo = (i * 10) as i64;
                entry(&format!("{:012}.tgs", i + 1), (lo, A), (lo + 9, B), 10_000)
            })
            .collect();
        assert!(
            plan_lane(&entries, LaneSlot::EdgeEvent, 4096, &|_| 0).is_empty(),
            "nine full segments have nothing to gain from a merge"
        );
    }

    /// One call rewrites every set it plans, so a segment in two sets would
    /// be read twice, written twice and superseded once — its rows
    /// duplicated in the logical store.
    #[test]
    fn no_segment_is_planned_into_two_sets() {
        let mut entries: Vec<SegmentEntry> = (0..25u32)
            .map(|i| {
                let lo = (i * 10) as i64;
                entry(&format!("{:012}.tgs", i + 1), (lo, A), (lo + 9, B), 100)
            })
            .collect();
        entries[12].rows = 10_000;
        // a close-heavy segment that the window rule also covers
        let unfolded = |id: u64| if id == 3 || id == 20 { 50 } else { 0 };
        let sets = plan_lane(&entries, LaneSlot::EdgeEvent, 4096, &unfolded);
        assert!(sets.len() >= 2, "the fixture should plan several sets");
        let mut seen = std::collections::HashSet::new();
        for s in &sets {
            for &m in &s.members {
                assert!(seen.insert(m), "segment {m} planned into two sets");
            }
        }
    }

    #[test]
    fn a_close_heavy_segment_triggers_on_its_own() {
        let entries = vec![
            entry("000000000001.tgs", (0, A), (100, B), 10_000),
            entry("000000000002.tgs", (0, A), (100, B), 10_000),
        ];
        // segment 1 has 30 % of its rows closed, segment 2 has none
        let unfolded = |id: u64| if id == 1 { 3_000 } else { 0 };
        let sets = plan_lane(&entries, LaneSlot::EdgeEvent, 4096, &unfolded);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].trigger, Trigger::CloseFraction);
        assert_eq!(sets[0].members, vec![0]);
        assert_eq!(sets[0].closes, 3_000);

        // below the fraction, nothing fires: a rewrite must reclaim something
        let sparse = |id: u64| if id == 1 { 10 } else { 0 };
        assert!(plan_lane(&entries, LaneSlot::EdgeEvent, 4096, &sparse).is_empty());
    }

    #[test]
    fn the_budget_takes_the_biggest_reclaim_first_and_never_starves() {
        let mk = |rows: u64, closes: u64, n: usize| MergeSet {
            slot: LaneSlot::EdgeEvent,
            members: (0..n).collect(),
            rows,
            closes,
            trigger: Trigger::Runs,
        };
        let sets = vec![mk(100, 0, 2), mk(50, 7, 2), mk(10, 0, 8)];
        let taken = take_within_budget(sets.clone(), Some(70));
        assert_eq!(taken.len(), 2, "the close-folding set, then the wide one");
        assert_eq!(taken[0].closes, 7);
        assert_eq!(taken[1].members.len(), 8);

        // a budget smaller than the first set still makes progress
        let taken = take_within_budget(sets, Some(1));
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].closes, 7);
    }
}
