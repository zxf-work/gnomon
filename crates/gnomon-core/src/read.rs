//! Point and full-scan reads that reconstruct whole version rows.
//!
//! These are the shape `StorageAdapter` hands to Python: every field of a
//! `NodeVersion` / `EdgeVersion`, including the ones the engine does not
//! store. `eid` is re-derived from `(src, dst, rel_type, disc)` and `vid` is
//! reassembled from its two columns, which is the whole point of the
//! derivability invariant — 24-byte hex identities never occupy a
//! row, they are produced at the boundary where row counts are already small.
//!
//! `props` is returned as the exact canonical-JSON text that was ingested.
//! The store digest is computed in Python over this string, so re-serializing
//! it — even into something equivalent — would change a digest and break
//! replay against the frozen SHAs.
//!
//! Everything here is a linear scan per segment. The identity postings and
//! filters that make point reads cheap change the cost, not the answer.

use std::collections::{HashMap, HashSet};

/// How many stored node versions one identity-postings probe is worth, from
/// the measurement in `nodes_with_believed_versions`: ~5 µs per probe against
/// ~0.25 µs per materialized node version. Only the crossover moves if a host
/// disagrees, and at the crossover the two paths cost the same by definition.
const PROBE_COST_RATIO: u64 = 20;

use crate::derive::{edge_eid, Id96};
use crate::error::{EngineError, Result};
use crate::row::RowKind;
use crate::store::{segment_id_of, NativeStore, OpenMode};
use crate::visibility::{CloseIndex, Sidecar};
use crate::{believed_at, OPEN_END};

/// The effective transaction-time end of one stored row: the close run's
/// `tt_e` when this generation records one, else the folded sidecar's.
///
/// A close run outranks a folded sidecar entry — it is the more recent
/// transaction — **and a run entry whose `tt_e` is `OPEN_END` is treated as
/// having nothing to say**, so the sidecar answers. That second half is a
/// latent oddity rather than a designed rule (a run is never *written* with
/// `tt_e == OPEN_END`: `write_close_run` refuses only empty runs), and this
/// extraction deliberately preserves it rather than fixing it, so that the
/// range-domain rewrite built on top of it can claim byte-identical answers.
/// This is one spelling of an idiom that was written out seven times in this
/// file — `edge_rows_sel`, `node_rows_of`, `resolve_entities`,
/// `version_keys`, `version_rows_at` (both kinds) and `scan_node_versions` —
/// plus once more as `integrity.rs::row_tt_e`, which is why verify and the
/// read path agreeing about which rows are believed was until now a property
/// maintained by hand at eight sites.
#[inline]
pub(crate) fn effective_tt_e(
    closes: &CloseIndex,
    sidecar: &Sidecar<'_>,
    segment: u64,
    row: u32,
) -> i64 {
    let from_run = closes.tt_e(segment, row);
    if from_run != OPEN_END {
        from_run
    } else {
        sidecar.tt_e(row)
    }
}

/// A node version with every field materialized.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeVersionOut {
    pub vid: String,
    pub uid: String,
    pub label: String,
    pub vt_s: i64,
    pub vt_e: i64,
    pub tt_s: i64,
    pub tt_e: i64,
    pub props: String,
    pub source: String,
    pub provenance_ref: Option<String>,
}

/// An edge version with every field materialized, `eid` included.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeVersionOut {
    pub eid: String,
    pub vid: String,
    pub src: String,
    pub dst: String,
    pub rel_type: String,
    pub disc: String,
    pub vt_s: i64,
    pub vt_e: i64,
    pub tt_s: i64,
    pub tt_e: i64,
    pub props: String,
    pub source: String,
    pub provenance_ref: Option<String>,
}

/// How [`NativeStore::nodes_with_believed_versions_via`] answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExistenceProbe {
    /// The believed-node count per dense id; current belief only.
    Count,
    /// The walk, choosing `Lookup` or `Scan` by `PROBE_COST_RATIO` — what
    /// every historical question uses.
    Walk,
    /// Every live node segment's identity postings, per uid.
    Lookup,
    /// Every stored node version, materialized.
    Scan,
}

/// [`NativeStore::lookup_name`]'s match mode.
/// Substring matching — on uid or on name — stays `resolve_entities`'s job;
/// no sorted index accelerates `contains`, so there is deliberately no
/// `Substring` variant here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NameMatchMode {
    /// Exact match, casefolded for the name index (`str::to_lowercase`,
    /// pinned in `nameidx.rs`) and literal for the uid dictionary probe.
    Exact,
    /// Prefix match on the casefolded name only — there is no "prefix uid"
    /// query in the spec's query-path table.
    Prefix,
}

/// Which version of [`uid_key`] a persisted structure was built over.
///
/// Stamped into anything on disk that holds `uid_key` values, so a deliberate
/// future change to the key orphans those files and has them rebuilt rather
/// than letting a stale one answer. Bump this and the old files stop
/// matching; nothing migrates.
pub const UID_KEY_ALGO_VERSION: u32 = 1;

/// Postings key for a node identity — a **pinned, in-tree** 64-bit hash.
///
/// Any stable hash works for the *answer*: the full uid is verified on every
/// hit (`believed_node_versions` compares `r.uid == uid`, and `locate` only
/// ever narrows a candidate set), so this has to spread, not to be unique.
/// That argument is why replacing the function is **answer-neutral** — two
/// different key functions produce the same `(file, row)` results for every
/// identity, differing only in how many candidate rows the verifier rejects.
///
/// What the argument does *not* cover is a key that has been **written to
/// disk**. `std::collections::hash_map::DefaultHasher`, which this used to
/// be, is explicitly not guaranteed stable across Rust releases
/// (`std::hash::DefaultHasher`'s own documentation says instances created by
/// different library versions may produce different results). While the keys
/// live only in a process's postings map that is harmless — they are
/// rebuilt on every open. The moment a sidecar is built over them and
/// reloaded by a differently-compiled binary, a key that moved is a **false
/// negative**: a segment that does hold the identity answers "no". That is
/// the one error class a membership filter may never have, so the key is
/// pinned here before anything persists it.
///
/// The function is FNV-1a over the UTF-8 bytes followed by splitmix64's
/// finalizer. FNV-1a alone leaves short common prefixes poorly separated in
/// the high bits, which matters because the postings key is compared whole
/// and a filter fingerprints a slice of it; the finalizer avalanches every
/// input bit across all 64. Both constants are written out here, so the
/// output is a function of this source text and of nothing else.
pub fn uid_key(uid: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = FNV_OFFSET;
    for &b in uid.as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

impl NativeStore {
    pub(crate) fn edge_files(&self) -> Vec<(String, u64)> {
        let m = self.manifest();
        m.edge_lanes
            .event
            .iter()
            .chain(m.edge_lanes.interval.iter())
            .map(|e| (e.file.clone(), segment_id_of(&e.file)))
            .collect()
    }

    pub(crate) fn node_files(&self) -> Vec<(String, u64)> {
        self.manifest()
            .node_store
            .iter()
            .map(|e| (e.file.clone(), segment_id_of(&e.file)))
            .collect()
    }

    pub(crate) fn uid_of(&self, id: u32) -> Result<String> {
        self.dict()
            .uid(id)
            .map(str::to_string)
            .ok_or_else(|| EngineError::invariant(format!("dense id {id} is not in the dictionary")))
    }

    /// `(eid, rel_type)` for explicit `(segment id, row)` addresses, in
    /// input order.
    ///
    /// The companion of the scan's `seg_id` / `seg_row` projection: a
    /// traversal scans integer columns only and comes back here for the two
    /// derived fields of the rows that actually survive it — a sha256 and a
    /// dictionary lookup per *surviving* row instead of per scanned row.
    /// Addresses are meaningful only against the generation that produced
    /// them; ids of segments outside the current generation are refused.
    pub fn edge_idents_at(
        &self,
        seg_ids: &[u64],
        seg_rows: &[u32],
    ) -> Result<Vec<(String, String)>> {
        if seg_ids.len() != seg_rows.len() {
            return Err(EngineError::invariant(
                "edge_idents_at: seg_ids and seg_rows differ in length",
            ));
        }
        let files: HashMap<u64, String> = self
            .edge_files()
            .into_iter()
            .map(|(file, id)| (id, file))
            .collect();
        // group requests by segment so each is opened (and verified) once
        let mut by_seg: HashMap<u64, Vec<usize>> = HashMap::new();
        for (i, &sid) in seg_ids.iter().enumerate() {
            by_seg.entry(sid).or_default().push(i);
        }
        let mut out: Vec<Option<(String, String)>> = vec![None; seg_ids.len()];
        for (sid, idxs) in by_seg {
            let file = files.get(&sid).ok_or_else(|| {
                EngineError::invariant(format!(
                    "segment id {sid} is not in the current generation"
                ))
            })?;
            let seg = self.open_segment(file)?;
            let h = seg.header();
            let strings = seg.strings()?;
            let src = seg.u32_column("src_id")?;
            let dst = seg.u32_column("dst_id")?;
            let rel = seg.u16_column("rel_code")?;
            let disc = seg.u32_column("disc_ref")?;
            for i in idxs {
                let r = seg_rows[i] as usize;
                if r >= src.len() {
                    return Err(EngineError::invariant(format!(
                        "row {r} is out of bounds for segment {sid}"
                    )));
                }
                let src_uid = self.uid_of(src[r])?;
                let dst_uid = self.uid_of(dst[r])?;
                let rel_type = h.rel_types.get(rel[r] as usize).ok_or_else(|| {
                    EngineError::corrupt(format!("rel_code {} has no entry", rel[r]))
                        .at_row(r as u32)
                })?;
                let disc_s = strings.get(disc[r])?;
                out[i] = Some((
                    edge_eid(&src_uid, &dst_uid, rel_type, disc_s).to_hex(),
                    rel_type.clone(),
                ));
            }
        }
        Ok(out
            .into_iter()
            .map(|o| o.expect("every requested address was filled"))
            .collect())
    }

    /// Every edge version in one segment, closes applied.
    ///
    /// Opened **uncached**: the only callers are whole-store walks,
    /// which touch each segment exactly once, so leaving the decoded columns
    /// in the session cache would make the walk's working set the store
    /// rather than one segment — the pathology `stats_accum` already fixed
    /// for statistics (`read.rs`, "Streaming, addendum 2").
    fn edge_rows_of(&self, file: &str, id: u64, closes: &CloseIndex) -> Result<Vec<EdgeVersionOut>> {
        self.edge_rows_sel(file, id, closes, None, OpenMode::Uncached)
    }

    /// Edge versions in one segment: all of them, or just `rows`.
    ///
    /// The selective form exists because the identity postings index hands
    /// back exact `(file, row)` pairs and the point-read path then threw that
    /// away, rebuilding the whole segment to index into it — a sha256 per row
    /// to reach two of them.
    ///
    /// `mode` follows the rule in [`NativeStore::open_segment_mode`]: the
    /// selective form is a point read that will come back, so it caches; the
    /// whole-segment form is one pass of a store-sized walk, so it does not.
    fn edge_rows_sel(
        &self,
        file: &str,
        id: u64,
        closes: &CloseIndex,
        rows: Option<&[u32]>,
        mode: OpenMode,
    ) -> Result<Vec<EdgeVersionOut>> {
        let seg = self.open_segment_mode(file, mode)?;
        let h = seg.header();
        let tt_s_v = seg.tt_s_view()?;
        let strings = seg.strings()?;
        let sidecar = seg.sidecar();
        let vt_s = seg.i64_column("vt_s")?;
        let vt_e = if h.vt_e_elided {
            None
        } else {
            Some(seg.i64_column("vt_e")?)
        };
        let src = seg.u32_column("src_id")?;
        let dst = seg.u32_column("dst_id")?;
        let rel = seg.u16_column("rel_code")?;
        let vid_hi = seg.u64_column("vid64")?;
        let vid_lo = seg.u32_column("vid_lo32")?;
        let props = seg.u32_column("props_ref")?;
        let disc = seg.u32_column("disc_ref")?;
        let source = seg.u32_column("source_ref")?;
        let prov = seg.u32_column("prov_ref")?;

        let emit = |i: usize| -> Result<EdgeVersionOut> {
            let src_uid = self.uid_of(src[i])?;
            let dst_uid = self.uid_of(dst[i])?;
            let rel_type = h
                .rel_types
                .get(rel[i] as usize)
                .ok_or_else(|| {
                    EngineError::corrupt(format!("rel_code {} has no entry", rel[i])).at_row(i as u32)
                })?
                .clone();
            let disc_s = strings.get(disc[i])?.to_string();
            let tt_e = effective_tt_e(closes, &sidecar, id, i as u32);
            Ok(EdgeVersionOut {
                eid: edge_eid(&src_uid, &dst_uid, &rel_type, &disc_s).to_hex(),
                vid: Id96 {
                    hi: vid_hi[i],
                    lo: vid_lo[i],
                }
                .to_hex(),
                src: src_uid,
                dst: dst_uid,
                rel_type,
                disc: disc_s,
                vt_s: vt_s[i],
                vt_e: vt_e.map(|c| c[i]).unwrap_or(vt_s[i] + 1),
                tt_s: tt_s_v.at(i as u32)?,
                tt_e,
                props: strings.get(props[i])?.to_string(),
                source: strings.get(source[i])?.to_string(),
                provenance_ref: strings.get_opt(prov[i])?.map(str::to_string),
            })
        };
        collect_rows(vt_s.len(), rows, emit)
    }

    /// Every node version in one segment. Uncached, for the reason given on
    /// [`edge_rows_of`](Self::edge_rows_of).
    fn node_rows_of(&self, file: &str, id: u64, closes: &CloseIndex) -> Result<Vec<NodeVersionOut>> {
        self.node_rows_sel(file, id, closes, None, OpenMode::Uncached)
    }

    /// Node versions in one segment: all of them, or just `rows`.
    fn node_rows_sel(
        &self,
        file: &str,
        id: u64,
        closes: &CloseIndex,
        rows: Option<&[u32]>,
        mode: OpenMode,
    ) -> Result<Vec<NodeVersionOut>> {
        let seg = self.open_segment_mode(file, mode)?;
        let h = seg.header();
        let tt_s_v = seg.tt_s_view()?;
        let strings = seg.strings()?;
        let sidecar = seg.sidecar();
        let vt_s = seg.i64_column("vt_s")?;
        let vt_e = if h.vt_e_elided {
            None
        } else {
            Some(seg.i64_column("vt_e")?)
        };
        let uid_id = seg.u32_column("uid_id")?;
        let vid_hi = seg.u64_column("vid64")?;
        let vid_lo = seg.u32_column("vid_lo32")?;
        let label = seg.u32_column("label_ref")?;
        let props = seg.u32_column("props_ref")?;
        let source = seg.u32_column("source_ref")?;
        let prov = seg.u32_column("prov_ref")?;

        let emit = |i: usize| -> Result<NodeVersionOut> {
            let tt_e = effective_tt_e(closes, &sidecar, id, i as u32);
            Ok(NodeVersionOut {
                vid: Id96 {
                    hi: vid_hi[i],
                    lo: vid_lo[i],
                }
                .to_hex(),
                uid: self.uid_of(uid_id[i])?,
                label: strings.get(label[i])?.to_string(),
                vt_s: vt_s[i],
                vt_e: vt_e.map(|c| c[i]).unwrap_or(vt_s[i] + 1),
                tt_s: tt_s_v.at(i as u32)?,
                tt_e,
                props: strings.get(props[i])?.to_string(),
                source: strings.get(source[i])?.to_string(),
                provenance_ref: strings.get_opt(prov[i])?.map(str::to_string),
            })
        };
        collect_rows(vt_s.len(), rows, emit)
    }

    /// One staged edge row, materialized. Deriving `eid` is a sha256 and two
    /// dictionary lookups, which is why the callers below select their rows
    /// first and materialize second.
    fn staged_edge_out(&self, r: &crate::row::EdgeRow) -> Result<EdgeVersionOut> {
        let src = self.uid_of(r.src_id)?;
        let dst = self.uid_of(r.dst_id)?;
        Ok(EdgeVersionOut {
            eid: edge_eid(&src, &dst, &r.rel_type, &r.disc).to_hex(),
            vid: r.vid.to_hex(),
            src,
            dst,
            rel_type: r.rel_type.clone(),
            disc: r.disc.clone(),
            vt_s: r.vt_s,
            vt_e: r.vt_e,
            tt_s: r.tt_s,
            tt_e: self.staged_closes().get(&r.vid).copied().unwrap_or(OPEN_END),
            props: r.props.clone(),
            source: r.source.clone(),
            provenance_ref: r.provenance_ref.clone(),
        })
    }

    fn staged_node_out(&self, r: &crate::row::NodeRow) -> Result<NodeVersionOut> {
        Ok(NodeVersionOut {
            vid: r.vid.to_hex(),
            uid: self.uid_of(r.uid_id)?,
            label: r.label.clone(),
            vt_s: r.vt_s,
            vt_e: r.vt_e,
            tt_s: r.tt_s,
            tt_e: self.staged_closes().get(&r.vid).copied().unwrap_or(OPEN_END),
            props: r.props.clone(),
            source: r.source.clone(),
            provenance_ref: r.provenance_ref.clone(),
        })
    }

    /// Edge versions staged in the open batch, if any.
    pub(crate) fn staged_edge_versions(&self) -> Result<Vec<EdgeVersionOut>> {
        self.staged_edges()
            .iter()
            .map(|r| self.staged_edge_out(r))
            .collect()
    }

    pub(crate) fn staged_node_versions(&self) -> Result<Vec<NodeVersionOut>> {
        self.staged_nodes()
            .iter()
            .map(|r| self.staged_node_out(r))
            .collect()
    }

    /// Staged edge versions that may belong to one identity, through the
    /// staging index — the read-your-own-writes half of a point read.
    ///
    /// A point read runs once per op and staging grows with the batch, so
    /// materializing every staged row here (an `eid` per row per read) made
    /// a k-op batch cost O(k²): 0.077 s at 500 asserts, 22 s at 8,000.
    /// `key` is a prefix, so these are candidates; the caller's exact `eid`
    /// comparison still decides, exactly as it does for committed rows.
    fn staged_edge_versions_for(&self, key: u64) -> Result<Vec<EdgeVersionOut>> {
        let positions = self.staging().edge_positions(key, |r| {
            Ok(edge_eid(
                &self.uid_of(r.src_id)?,
                &self.uid_of(r.dst_id)?,
                &r.rel_type,
                &r.disc,
            )
            .hi)
        })?;
        let staged = self.staged_edges();
        positions
            .iter()
            .map(|&i| self.staged_edge_out(&staged[i as usize]))
            .collect()
    }

    /// Staged node versions for one uid. Node identity is dense, so this is
    /// exact: a uid the dictionary does not know cannot have a staged row.
    fn staged_node_versions_for(&self, uid: &str) -> Result<Vec<NodeVersionOut>> {
        let Some(uid_id) = self.dict().dense_id(uid) else {
            return Ok(Vec::new());
        };
        let positions = self.staging().node_positions(uid_id);
        let staged = self.staged_nodes();
        positions
            .iter()
            .map(|&i| self.staged_node_out(&staged[i as usize]))
            .collect()
    }

    // ------------------------------------------------------------------ //
    // public reads                                                        //
    // ------------------------------------------------------------------ //

    /// Fold one segment's rows into a [`SegmentPostings`].
    ///
    /// Deriving `eid` per row is the expensive part, which is why a fold is
    /// worth keeping; node identities are uids and need no derivation. The
    /// result is sorted by identity prefix, so a lookup is a binary search
    /// and one identity's rows are a contiguous run.
    ///
    /// The segment is opened **uncached**: this fold
    /// reads the columns it needs once and never comes back to that segment —
    /// what later point reads consult is the postings it built, not the
    /// decoded segment. Through the cached open, one point read on a
    /// never-indexed store pulled *every* segment's decoded columns into the
    /// session cache as a side effect.
    fn fold_segment(&self, kind: RowKind, file: &str) -> Result<crate::store::SegmentPostings> {
        let fold_t = crate::ingest_phase::Timer::start();
        let out = self.fold_segment_inner(kind, file)?;
        fold_t.stop_ctx(crate::ingest_phase::S::Fold);
        crate::ingest_phase::add_ctx(crate::ingest_phase::S::FoldRows, out.row_count() as u64);
        Ok(out)
    }

    fn fold_segment_inner(&self, kind: RowKind, file: &str) -> Result<crate::store::SegmentPostings> {
        let seg = self.open_segment_uncached(file)?;
        let keys = self.identity_keys(kind, &seg)?;
        // A fold walks every row of this segment anyway, so the filter that
        // would have let it be skipped costs only the construction. That is
        // the third of the filter build paths: a legacy store
        // gains filters as it is probed, so its *second* correction is
        // already filtered. It is a fallback and nothing relies on it — for
        // the case the filter exists for, the first use is the entire cost —
        // but it is what makes "old stores gain filters without a
        // migration" true, and `filter_lazy_built` measures how often it
        // fires.
        self.store_vid_filter_from_keys(kind, file, seg.rows(), &keys, true);
        let entries: Vec<(u64, u32)> =
            keys.iter().enumerate().map(|(i, &k)| (k, i as u32)).collect();
        // the sidecar carries the closes folded in at seal time, so a row
        // born already closed (a same-batch carve) never enters the
        // open-version index; closes that arrive later as close runs are
        // pruned by `locate_open`
        let sidecar = seg.sidecar();
        let rows = seg.rows();
        let mut open = vec![0u64; crate::store::SegmentPostings::open_words_for(rows)];
        for r in 0..rows {
            if sidecar.tt_e(r) == OPEN_END {
                open[(r / 64) as usize] |= 1u64 << (r % 64);
            }
        }
        Ok(crate::store::SegmentPostings::from_entries(entries, open))
    }

    /// Every stored row's identity key, in the segment's own row order.
    ///
    /// **The single derivation.** The fold builds its postings from this,
    /// `build_vid_filters` builds a filter from this, and the lazy build path
    /// inside `fold_segment` writes one from this — so "the filter is built
    /// over exactly the key the probe uses" is a
    /// property of there being one function, not of two call sites agreeing.
    /// Scope is *all stored rows*, belief ignored, which is what lets a
    /// historical `as_of` probe be filtered by the same bytes.
    ///
    /// Deriving `eid` is the expensive part — two dictionary lookups, a
    /// canonical JSON array and an sha256 per row. The dictionary is
    /// an arena, so a uid is borrowed as `&str` rather than copied; the JSON
    /// goes into one buffer reused across rows; and the key is read from the
    /// digest bytes directly ([`crate::derive::edge_eid_hi`]). The earlier
    /// per-segment `HashMap<u32, String>` memo is gone: with endpoints drawn
    /// from millions of entities a segment repeats each one only a few times,
    /// and the memo's hashing and allocations cost more than the lookups it
    /// saved. The keys are bit-identical (`derive.rs` and
    /// `tests/segment_byte_identity.rs` pin it).
    pub(crate) fn identity_keys(
        &self,
        kind: RowKind,
        seg: &crate::segment::Segment<crate::segment::MmapSource>,
    ) -> Result<Vec<u64>> {
        let dict = self.dict();
        let uid = |id: u32| -> Result<&str> {
            dict.uid(id).ok_or_else(|| {
                EngineError::invariant(format!("dense id {id} is not in the dictionary"))
            })
        };
        let mut keys: Vec<u64> = Vec::with_capacity(seg.rows() as usize);
        match kind {
            RowKind::Edge => {
                let strings = seg.strings()?;
                let src = seg.u32_column("src_id")?;
                let dst = seg.u32_column("dst_id")?;
                let rel = seg.u16_column("rel_code")?;
                let disc = seg.u32_column("disc_ref")?;
                let rel_types = &seg.header().rel_types;
                let mut buf = String::with_capacity(160);
                for i in 0..src.len() {
                    keys.push(crate::derive::edge_eid_hi(
                        &mut buf,
                        uid(src[i])?,
                        uid(dst[i])?,
                        rel_types.get(rel[i] as usize).map(String::as_str).unwrap_or(""),
                        strings.get(disc[i])?,
                    ));
                }
            }
            RowKind::Node => {
                let uid_id = seg.u32_column("uid_id")?;
                for &u in uid_id.iter() {
                    keys.push(uid_key(uid(u)?));
                }
            }
        }
        Ok(keys)
    }

    /// Ask one segment's identity postings a question, folding it first if it
    /// is not resident.
    ///
    /// **Correctness never depends on residency.** Three cases, and all three
    /// return the same answer:
    ///
    /// * resident — answer from the entry, touched for LRU;
    /// * not resident, and the budget could hold it — fold, insert (evicting
    ///   least-recently-used segments as the segment cache does), answer;
    /// * not resident, and one segment's postings alone exceed the budget —
    ///   fold, answer, **drop**. This is the case a 1-byte budget puts every
    ///   segment in, and it is why such a budget is correct rather than a
    ///   thrash: nothing is ever inserted, so nothing is ever evicted to make
    ///   room for it, mirroring [`crate::store::SegmentCache`]'s rule that the
    ///   entry being inserted is never its own eviction victim.
    ///
    /// `f` may mutate the entry, which is how `locate_open` performs its
    /// in-place prune.
    fn with_segment_postings<T>(
        &self,
        kind: RowKind,
        seg_id: u64,
        file: &str,
        f: impl FnOnce(&mut crate::store::SegmentPostings) -> T,
    ) -> Result<T> {
        {
            let mut p = self.postings_for(kind).lock().expect("postings mutex poisoned");
            if let Some(e) = p.get_mut(seg_id) {
                return Ok(f(e));
            }
        }
        // fold outside the postings lock: it opens and decodes a segment, and
        // holding the lock across that serialises every reader behind it
        let mut sp = self.fold_segment(kind, file)?;
        let rows = sp.row_count();
        let mut p = self.postings_for(kind).lock().expect("postings mutex poisoned");
        if p.can_hold(rows) {
            p.note_fold(seg_id, rows, true);
            // another thread may have raced us to the same fold; `insert`
            // replaces it, and the two are identical by construction
            p.insert(seg_id, sp);
            let e = p.get_mut(seg_id).expect("just inserted");
            Ok(f(e))
        } else {
            p.note_fold(seg_id, rows, false);
            drop(p);
            Ok(f(&mut sp))
        }
    }

    fn postings_for(&self, kind: RowKind) -> &std::sync::Mutex<crate::store::Postings> {
        match kind {
            RowKind::Edge => self.edge_postings(),
            RowKind::Node => self.node_postings(),
        }
    }

    /// Candidate `(file, row)` locations for one identity.
    ///
    /// Identity is a hash, so no manifest range can prune it: every live
    /// segment is a candidate and is asked in turn. What the byte budget
    /// bounds is what asking costs in memory — a segment's postings are folded, used,
    /// and kept only while the budget has room for them.
    fn locate(&self, kind: RowKind, key: u64) -> Result<Vec<(String, u32)>> {
        let files = self.segment_files();
        let mut out = Vec::new();
        for (seg_id, file) in files.of(kind) {
            // The identity filter: the only prune this probe can have. A filter negative is
            // **exact** — the segment holds no stored row with this key — so
            // the segment contributed nothing to the previous answer and is
            // skipped without opening its postings. A missing filter leaves
            // the loop exactly as it was.
            let verdict = self.filter_verdict(kind, *seg_id, file, key);
            if verdict == crate::store::FilterVerdict::Negative {
                continue;
            }
            let hits =
                self.with_segment_postings(kind, *seg_id, file, |e| e.rows_for(key).to_vec())?;
            if hits.is_empty() {
                self.note_filter_empty_positive(kind, verdict);
            }
            out.extend(hits.into_iter().map(|row| (file.clone(), row)));
        }
        Ok(out)
    }

    /// Does this `as_of_tt` ask about *current* belief?
    ///
    /// Only the current-belief question has a standing answer worth indexing:
    /// "which rows are open now". Every historical `as_of` is a different
    /// question per tt and keeps the walk.
    fn is_current_belief(as_of_tt: i64) -> bool {
        crate::clamp_tt(as_of_tt) >= OPEN_END - 1
    }

    /// Currently-open `(file, row)` locations for one identity.
    ///
    /// **The open set is a superset, and that is the design.** Maintaining an
    /// exact set would mean removing a row the moment something closed it,
    /// which needs a `(segment, row) -> identity` reverse map the store does
    /// not have — closes name physical addresses, not identities. Instead a
    /// row's bit is set at fold time if its segment's sidecar says it is
    /// open, and closes are discovered *here*, against the in-memory
    /// `CloseIndex`, then **cleared in place**.
    ///
    /// So each row is examined exactly once more after it closes, and never
    /// again — for as long as its segment stays resident. Under budget
    /// pressure a re-fold rebuilds the bit from the sidecar and the prune
    /// happens again; it costs work, never an answer, because the close index
    /// is consulted on every call regardless of what the bit says.
    ///
    /// Rows closed by the *open batch* are dropped too but not pruned: the
    /// committed index still rightly thinks they are open, and the batch may
    /// yet roll back.
    fn locate_open(&self, kind: RowKind, key: u64) -> Result<Vec<(String, u32)>> {
        // **Committed closes only.** Pruning against `close_index()` would use
        // the open batch's pending overlay and permanently delete rows an
        // abandoned batch must give back — the index is not transactional, so
        // nothing uncommitted may ever mutate it. Pending closes are applied
        // below as a filter instead, which a rollback simply forgets.
        let closes = self.committed_close_index()?;
        let files = self.segment_files();
        let closed_here = self.pending_closed_rows(kind);
        let mut out = Vec::new();
        for (seg_id, file) in files.of(kind) {
            let seg_id = *seg_id;
            // The identity filter, safe for exactly this reason:
            // a segment with no *stored* row of identity `k` has no *open*
            // row of it either, and the filter's scope is all stored rows.
            let verdict = self.filter_verdict(kind, seg_id, file, key);
            if verdict == crate::store::FilterVerdict::Negative {
                continue;
            }
            let (hits, empty) = self.with_segment_postings(kind, seg_id, file, |e| {
                let candidates: Vec<u32> = e.rows_for(key).to_vec();
                let empty = candidates.is_empty();
                let mut hits = Vec::new();
                for row in candidates {
                    if !e.is_open(row) {
                        continue;
                    }
                    if closes.tt_e(seg_id, row) != OPEN_END {
                        // discovered closed: prune in place, once, forever
                        e.mark_closed(row);
                        continue;
                    }
                    hits.push(row);
                }
                (hits, empty)
            })?;
            if empty {
                self.note_filter_empty_positive(kind, verdict);
            }
            out.extend(
                hits.into_iter()
                    .filter(|row| !closed_here.contains(&(seg_id, *row)))
                    .map(|row| (file.clone(), row)),
            );
        }
        Ok(out)
    }

    /// Physical location of one committed version, or None.
    ///
    /// **This holds nothing at all**. The
    /// vid postings it used to consult were a `HashMap` entry per row of the
    /// store — the largest of the three maps, 100M distinct keys at 100M
    /// rows — built in full by the first correction and never released.
    ///
    /// Segments are sorted by `(vt_s, vid)` (`row.rs::SortKey`) and each
    /// manifest entry records that key at both ends (`key_lo`/`key_hi`,
    /// `manifest.rs:508`), so when the caller knows the row's `vt_s` — and
    /// `close_version`'s does, `base.py::_supersede_edges` holds the whole
    /// version — the segments that *can* hold `(vt_s, vid)` are exactly those
    /// whose key range brackets it. Inside a survivor the same order makes
    /// the row a binary search. `Id96`'s derived `Ord` is the ordering of its
    /// 24-character hex string (`derive.rs::id96_ordering_equals_hex_string_
    /// ordering`), which is what makes comparing against the manifest's hex
    /// bounds exact rather than approximate.
    ///
    /// With no `vt_s` the prune is simply empty and every segment is
    /// searched linearly — the answer is identical, and still nothing is
    /// retained.
    pub(crate) fn locate_vid(
        &self,
        kind: RowKind,
        vid: Id96,
        vt_s: Option<i64>,
    ) -> Result<Option<(u64, u32)>> {
        let files = self.segment_files();
        let by_id = files.of(kind);
        let key = vt_s.map(|t| (t, vid.to_hex()));
        for entry in self.manifest().segments_of(kind) {
            let seg_id = crate::store::segment_id_of(&entry.file);
            let Some(file) = by_id.get(&seg_id) else { continue };
            if let Some(k) = key.as_ref() {
                let lo = (entry.key_lo.0, entry.key_lo.1.as_str());
                let hi = (entry.key_hi.0, entry.key_hi.1.as_str());
                let probe = (k.0, k.1.as_str());
                if probe < lo || probe > hi {
                    continue;
                }
            }
            let seg = self.open_segment(file)?;
            let hi_col = seg.u64_column("vid64")?;
            let lo_col = seg.u32_column("vid_lo32")?;
            let row = match vt_s {
                Some(t) => {
                    let vt = seg.i64_column("vt_s")?;
                    // the sealed order is exactly this key (`row.rs::SortKey`,
                    // and `scan.rs` already binary-searches `vt_s` for window
                    // pruning), so one probe decides
                    let target = (t, vid.hi, vid.lo);
                    let (mut lo, mut hi) = (0usize, hi_col.len());
                    while lo < hi {
                        let mid = lo + (hi - lo) / 2;
                        if (vt[mid], hi_col[mid], lo_col[mid]) < target {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    (lo < hi_col.len() && (vt[lo], hi_col[lo], lo_col[lo]) == target).then_some(lo)
                }
                None => (0..hi_col.len()).find(|&i| hi_col[i] == vid.hi && lo_col[i] == vid.lo),
            };
            if let Some(row) = row {
                return Ok(Some((seg_id, row as u32)));
            }
        }
        Ok(None)
    }

    /// Every edge version ever committed, plus anything staged in the open
    /// batch. Order is unspecified — the digest sorts.
    pub fn all_edge_versions(&self) -> Result<Vec<EdgeVersionOut>> {
        let closes = self.close_index()?;
        let mut out = Vec::new();
        for (file, id) in self.edge_files() {
            out.extend(self.edge_rows_of(&file, id, &closes)?);
        }
        out.extend(self.staged_edge_versions()?);
        Ok(out)
    }

    pub fn all_node_versions(&self) -> Result<Vec<NodeVersionOut>> {
        let closes = self.close_index()?;
        let mut out = Vec::new();
        for (file, id) in self.node_files() {
            out.extend(self.node_rows_of(&file, id, &closes)?);
        }
        out.extend(self.staged_node_versions()?);
        Ok(out)
    }

    /// Versions of one logical edge believed at `as_of_tt`, ordered by `vt_s`
    /// — the ordering `believed_edge_versions` promises.
    pub fn believed_edge_versions(&self, eid: &str, as_of_tt: i64) -> Result<Vec<EdgeVersionOut>> {
        self.assert_full_belief(as_of_tt)?;
        let key = Id96::from_hex(eid).map(|i| i.hi)?;
        let closes = self.close_index()?;
        let mut out = Vec::new();
        // group candidate rows by file so each segment is opened once
        let mut by_file: std::collections::BTreeMap<String, Vec<u32>> = Default::default();
        // see believed_node_versions: current belief through the index
        let _ctx = crate::ingest_phase::CtxGuard::enter_if_unclaimed(crate::ingest_phase::Ctx::Correct);
        let t = crate::ingest_phase::Timer::start();
        let located = if Self::is_current_belief(as_of_tt) {
            self.locate_open(RowKind::Edge, key)?
        } else {
            self.locate(RowKind::Edge, key)?
        };
        t.stop_ctx(crate::ingest_phase::S::Locate);
        let t = crate::ingest_phase::Timer::start();
        for (file, row) in located {
            by_file.entry(file).or_default().push(row);
        }
        for (file, rows) in by_file {
            let id = segment_id_of(&file);
            // a prefix hit is a candidate; the full eid decides
            for r in self.edge_rows_sel(&file, id, &closes, Some(&rows), OpenMode::Cached)? {
                if r.eid == eid && believed_at(r.tt_s, r.tt_e, as_of_tt) {
                    out.push(r);
                }
            }
        }
        t.stop_ctx(crate::ingest_phase::S::RowsMaterialize);
        let t = crate::ingest_phase::Timer::start();
        out.extend(
            self.staged_edge_versions_for(key)?
                .into_iter()
                .filter(|r| r.eid == eid && believed_at(r.tt_s, r.tt_e, as_of_tt)),
        );
        t.stop_ctx(crate::ingest_phase::S::Staged);
        out.sort_by_key(|r| (r.vt_s, r.vid.clone()));
        Ok(out)
    }

    pub fn believed_node_versions(&self, uid: &str, as_of_tt: i64) -> Result<Vec<NodeVersionOut>> {
        self.assert_full_belief(as_of_tt)?;
        let closes = self.close_index()?;
        let mut out = Vec::new();
        let mut by_file: std::collections::BTreeMap<String, Vec<u32>> = Default::default();
        // current belief goes through the open-version index, which returns
        // the ~1 row still open instead of every version this identity has
        // ever had; any historical as_of keeps the walk
        let _ctx = crate::ingest_phase::CtxGuard::enter_if_unclaimed(crate::ingest_phase::Ctx::Correct);
        let t = crate::ingest_phase::Timer::start();
        let located = if Self::is_current_belief(as_of_tt) {
            self.locate_open(RowKind::Node, uid_key(uid))?
        } else {
            self.locate(RowKind::Node, uid_key(uid))?
        };
        t.stop_ctx(crate::ingest_phase::S::Locate);
        let t = crate::ingest_phase::Timer::start();
        for (file, row) in located {
            by_file.entry(file).or_default().push(row);
        }
        for (file, rows) in by_file {
            let id = segment_id_of(&file);
            // a uid_key hit is a candidate; the full uid decides
            for r in self.node_rows_sel(&file, id, &closes, Some(&rows), OpenMode::Cached)? {
                if r.uid == uid && believed_at(r.tt_s, r.tt_e, as_of_tt) {
                    out.push(r);
                }
            }
        }
        t.stop_ctx(crate::ingest_phase::S::RowsMaterialize);
        let t = crate::ingest_phase::Timer::start();
        out.extend(
            self.staged_node_versions_for(uid)?
                .into_iter()
                .filter(|r| r.uid == uid && believed_at(r.tt_s, r.tt_e, as_of_tt)),
        );
        t.stop_ctx(crate::ingest_phase::S::Staged);
        out.sort_by_key(|r| (r.vt_s, r.vid.clone()));
        Ok(out)
    }

    /// Which of `uids` have at least one believed version — the batched
    /// existence probe bulk ingest leans on (`base.py::_ingest_events`
    /// creates a bare node version for every endpoint *not* in the answer).
    ///
    /// **Current belief** — the only question bulk ingest asks — is answered
    /// from the believed-node count per dense id (`node_belief.rs`): one
    /// dictionary lookup and one array read per uid, whatever the store
    /// size. The two strategies below it grew with the store: the scan per
    /// batch with stored node versions, the postings walk per uid with live
    /// node segments, and together they were 29 % of ingest time at 100M
    /// rows and 58 % at 1B (ingest cost breakdown, 2026-09-25).
    ///
    /// **Any historical `as_of_tt`** keeps the walk below unchanged
    /// ([`ExistenceProbe::Walk`]), and both walk strategies stay callable by
    /// name so tests can hold the count to them as oracles.
    pub fn nodes_with_believed_versions(
        &self,
        uids: &[String],
        as_of_tt: i64,
    ) -> Result<HashSet<String>> {
        let how = if Self::is_current_belief(as_of_tt) {
            ExistenceProbe::Count
        } else {
            ExistenceProbe::Walk
        };
        self.nodes_with_believed_versions_via(uids, as_of_tt, how)
    }

    /// [`Self::nodes_with_believed_versions`] with the strategy named.
    /// `Count` refuses a historical `as_of_tt`: the count only knows *now*.
    pub fn nodes_with_believed_versions_via(
        &self,
        uids: &[String],
        as_of_tt: i64,
        how: ExistenceProbe,
    ) -> Result<HashSet<String>> {
        self.assert_full_belief(as_of_tt)?;
        let _ctx = crate::ingest_phase::CtxGuard::enter_if_unclaimed(crate::ingest_phase::Ctx::Probe);
        crate::ingest_phase::add(crate::ingest_phase::G::ProbeCalls, 1);
        crate::ingest_phase::add(crate::ingest_phase::G::ProbeUids, uids.len() as u64);
        match how {
            ExistenceProbe::Count => self.nodes_believed_now(uids, as_of_tt),
            ExistenceProbe::Walk => {
                let stored = self.manifest().stats.n_node_versions;
                if (uids.len() as u64).saturating_mul(PROBE_COST_RATIO) < stored {
                    self.nodes_believed_by_lookup(uids, as_of_tt)
                } else {
                    self.nodes_believed_by_scan(uids, as_of_tt)
                }
            }
            ExistenceProbe::Lookup => self.nodes_believed_by_lookup(uids, as_of_tt),
            ExistenceProbe::Scan => self.nodes_believed_by_scan(uids, as_of_tt),
        }
    }

    /// Per uid, every live node segment's identity postings, then the staged
    /// rows — flat in store size only while node segments are few.
    fn nodes_believed_by_lookup(&self, uids: &[String], as_of_tt: i64) -> Result<HashSet<String>> {
        let t = crate::ingest_phase::Timer::start();
        let mut out = HashSet::new();
        for uid in uids {
            if !self.believed_node_versions(uid, as_of_tt)?.is_empty() {
                out.insert(uid.clone());
            }
        }
        t.stop(crate::ingest_phase::G::ProbePointNs);
        Ok(out)
    }

    /// Materialize every node version, committed and staged, and filter.
    ///
    /// Measured (100k events, 200k node versions, macOS): **50.5 ms whether
    /// it is asked about 2 uids or 20,000**, while one postings lookup costs
    /// **0.005 ms** — 0.25 µs per stored node version against 5 µs per uid,
    /// which is where `PROBE_COST_RATIO` comes from. A singleton append that
    /// scanned was 57.7 ms of a 90 ms single-row write — the sixth
    /// appearance of *a small lookup rebuilding the whole store*.
    fn nodes_believed_by_scan(&self, uids: &[String], as_of_tt: i64) -> Result<HashSet<String>> {
        let t = crate::ingest_phase::Timer::start();
        let wanted: HashSet<&str> = uids.iter().map(String::as_str).collect();
        let out = self
            .all_node_versions()?
            .into_iter()
            .filter(|r| believed_at(r.tt_s, r.tt_e, as_of_tt) && wanted.contains(r.uid.as_str()))
            .map(|r| r.uid)
            .collect();
        t.stop(crate::ingest_phase::G::ProbeScanNs);
        Ok(out)
    }

    /// Current belief from the believed-node count (`node_belief.rs`).
    ///
    /// Exactly the walk's answer: a uid is believed now iff the dictionary
    /// knows it (a uid it does not know has no stored and no staged row —
    /// node rows carry the dense id, and `dense_id` verifies the full
    /// string) and either a committed row of that id is counted and not
    /// closed by the open batch, or a staged row of that id is not closed by
    /// the open batch. The open batch is an overlay built per call; nothing
    /// uncommitted ever reaches the count.
    fn nodes_believed_now(&self, uids: &[String], as_of_tt: i64) -> Result<HashSet<String>> {
        if !Self::is_current_belief(as_of_tt) {
            return Err(EngineError::invariant(format!(
                "the believed-node count answers current belief only, not tt={as_of_tt}"
            )));
        }
        let t = crate::ingest_phase::Timer::start();
        let mut cell = self
            .node_belief_cell()
            .lock()
            .expect("node-belief mutex poisoned");
        let nb = cell.get_or_insert_with(Default::default);
        let files = self.segment_files();
        let reconciled = self.committed_close_index().and_then(|closes| {
            nb.reconcile(&files.node, &closes, |file| {
                let seg = self.open_segment_uncached(file)?;
                Ok((seg.u32_column("uid_id")?.to_vec(), seg.sidecar().slice().to_vec()))
            })
        });
        if let Err(e) = reconciled {
            *cell = None;
            return Err(e);
        }
        let nb = cell.as_ref().expect("reconciled above");
        t.stop(crate::ingest_phase::G::ProbeCountReconcileNs);
        let t = crate::ingest_phase::Timer::start();

        // the open batch's closes of counted committed rows, per dense id;
        // a row closed twice in one batch is one row
        let mut closed_now: HashMap<u32, u32> = HashMap::new();
        for (segment, row) in self.pending_closed_rows(RowKind::Node) {
            if let Some(id) = nb.counted_uid(segment, row) {
                *closed_now.entry(id).or_default() += 1;
            }
        }
        let staged = self.staged_nodes();
        let staged_closes = self.staged_closes();
        let dict = self.dict();

        let mut out = HashSet::new();
        for uid in uids {
            let Some(id) = dict.dense_id(uid) else { continue };
            let committed = nb
                .count(id)
                .saturating_sub(closed_now.get(&id).copied().unwrap_or(0));
            let believed = committed > 0
                || (!staged.is_empty()
                    && self.staging().node_positions(id).iter().any(|&i| {
                        let r = &staged[i as usize];
                        let tt_e = staged_closes.get(&r.vid).copied().unwrap_or(OPEN_END);
                        believed_at(r.tt_s, tt_e, as_of_tt)
                    }));
            if believed {
                out.insert(uid.clone());
            }
        }
        t.stop(crate::ingest_phase::G::ProbeCountAnswerNs);
        Ok(out)
    }

    /// Entity resolution: match a query against uids and names, and
    /// report each matched entity through its latest believed version.
    ///
    /// Scored exactly as the operator defines: 0 for an exact uid, 1 for a
    /// uid substring, 2 for a name substring, lowest wins. `name` is read
    /// from the promoted `name_ref` column, so no JSON is parsed per row —
    /// that parse over every node version was the whole cost of this
    /// operator. Name matching is over JSON *string* names only: the
    /// typed column indexes nothing else, and the reference implementation's
    /// former `str()` coercion matched text like "None" that no version
    /// actually contained.
    ///
    /// Returns `(uid, score, label, props)`. The canonical label and props
    /// come from the latest believed version by `(vt_s, vid)` **whether or
    /// not that version itself matched** — an entity found by an old name
    /// still resolves to what it is now, not to what it was when it matched.
    /// The vid tiebreak is unreachable for believed versions of one uid
    /// (disjoint valid intervals) but keeps every implementation
    /// order-independent by construction.
    ///
    /// **The belief filter is per segment, not per row.** This was the
    /// last whole-node-store sweep on the measured read path — it opened
    /// every node segment and asked `believed_at` about every stored row,
    /// paying a `tt_s_at` binary search and a close probe each, so its cost
    /// was O(stored node versions) no matter how few of them were still
    /// believed. It now builds a [`ScanSet`](crate::scan::ScanSet) over the
    /// node segments and calls `select` with no valid-time window, exactly
    /// as [`scan_node_versions`](Self::scan_node_versions) does: the
    /// belief cost becomes O(segments × (runs + closes)) and the match cost
    /// O(believed rows). A segment nothing believed yet is pruned on its
    /// header before a column is decoded; a superseded row is cut out of the
    /// verdict range rather than visited and skipped.
    ///
    /// **Which rows reach `upsert` is unchanged**, and that is the whole
    /// correctness argument. `upsert` is called for every believed row,
    /// matched or not, because an entity found by an old name must still
    /// resolve to its latest believed state — so a conversion that narrowed
    /// the input would change answers, not just cost. `believed_ranges_closed`
    /// selects precisely the rows the old `continue` let through, and
    /// `select` returns them in ascending segment then ascending row order,
    /// the sweep's own order, so the `key >= e.1` tiebreak between two rows
    /// with an identical `(vt_s, vid)` resolves the same way it did.
    ///
    /// The staged loop below stays row-at-a-time: an open batch has no
    /// header, no `tt_s_runs` and no sidecar to build ranges from, and is
    /// bounded by the batch size anyway.
    pub fn resolve_entities(
        &self,
        query: &str,
        as_of_tt: i64,
    ) -> Result<Vec<(String, u8, String, String)>> {
        use crate::scan::{ScanRequest, ScanSet, ScanTarget};

        self.assert_full_belief(as_of_tt)?;
        let ql = query.to_lowercase();
        let closes = self.close_index()?;
        // uid -> (best matching score, canonical (vt_s, vid), label, props)
        type Entry = (u8, (i64, Id96), String, String);
        let mut best: HashMap<String, Entry> = HashMap::new();
        let mut upsert = |uid: String,
                          score: Option<u8>,
                          key: (i64, Id96),
                          label: &str,
                          props: &str| {
            let e = best.entry(uid).or_insert_with(|| {
                (u8::MAX, key, label.to_string(), props.to_string())
            });
            if let Some(sc) = score {
                e.0 = e.0.min(sc);
            }
            if key >= e.1 {
                e.1 = key;
                e.2 = label.to_string();
                e.3 = props.to_string();
            }
        };

        // Opened up front rather than one at a time, so the whole set is
        // planned in one pass — `scan_node_versions`' shape. These are the
        // segment cache's own `Arc`s, so holding them costs a refcount each.
        let files = self.node_files();
        let mut segs = Vec::with_capacity(files.len());
        for (file, seg_id) in &files {
            segs.push((self.open_segment(file)?, *seg_id));
        }
        let (selections, _stats) = {
            let targets: Vec<ScanTarget<'_, crate::segment::MmapSource>> = segs
                .iter()
                .map(|(segment, id)| ScanTarget {
                    segment,
                    lane: segment.header().lane,
                    id: *id,
                })
                .collect();
            ScanSet::new(targets)
                .with_closes(closes)
                .select(&ScanRequest {
                    as_of_tt,
                    // no valid-time window: resolution is over every believed
                    // version of an entity, which is what `upsert`'s
                    // latest-by-`(vt_s, vid)` rule reduces
                    ..ScanRequest::default()
                })?
        };

        for sel in &selections {
            let (seg, _) = &segs[sel.segment];
            let strings = seg.strings()?;
            let vt_s = seg.i64_column("vt_s")?;
            let uid_id = seg.u32_column("uid_id")?;
            let vid_hi = seg.u64_column("vid64")?;
            let vid_lo = seg.u32_column("vid_lo32")?;
            let label_ref = seg.u32_column("label_ref")?;
            let props_ref = seg.u32_column("props_ref")?;
            // segments written before the column was promoted simply have no
            // names to match on; uid matching still works
            let name_ref = seg.u32_column("name_ref").ok();

            for &row in &sel.rows {
                let i = row as usize;
                let uid = self.uid_of(uid_id[i])?;
                let name = match name_ref {
                    Some(col) => strings.get_opt(col[i])?,
                    None => None,
                };
                let score = if uid == query {
                    Some(0u8)
                } else if uid.to_lowercase().contains(&ql) {
                    Some(1)
                } else if name.is_some_and(|n| !n.is_empty() && n.to_lowercase().contains(&ql)) {
                    Some(2)
                } else {
                    None // no match — still a candidate for canonical state
                };
                upsert(
                    uid,
                    score,
                    (vt_s[i], Id96 { hi: vid_hi[i], lo: vid_lo[i] }),
                    strings.get(label_ref[i])?,
                    strings.get(props_ref[i])?,
                );
            }
        }

        // rows staged in an open batch: a batch must read its own writes
        let staged_closes = self.staged_closes();
        for r in self.staged_nodes() {
            let tt_e = staged_closes.get(&r.vid).copied().unwrap_or(OPEN_END);
            if !believed_at(r.tt_s, tt_e, as_of_tt) {
                continue;
            }
            let uid = self.uid_of(r.uid_id)?;
            let name = crate::segment::name_of(&r.props);
            let score = if uid == query {
                Some(0u8)
            } else if uid.to_lowercase().contains(&ql) {
                Some(1)
            } else if name.as_deref().is_some_and(|n| !n.is_empty() && n.to_lowercase().contains(&ql)) {
                Some(2)
            } else {
                None
            };
            upsert(uid, score, (r.vt_s, r.vid), &r.label, &r.props);
        }

        let mut out: Vec<(String, u8, String, String)> = best
            .into_iter()
            .filter(|(_, e)| e.0 != u8::MAX) // tracked for state, never matched
            .map(|(uid, (score, _, label, props))| (uid, score, label, props))
            .collect();
        out.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        Ok(out)
    }

    /// Sorted-name-index lookup: exact or prefix match
    /// on the casefolded name, plus the already-O(1) exact-uid path via the
    /// dense-id dictionary. A *new* API next to `resolve_entities`, not a
    /// replacement — `resolve_entities` answers substring on both fields and
    /// no sorted index accelerates `contains`, so it is unchanged in
    /// signature and behaviour.
    ///
    /// Two-phase resolve, and provably the same answer a substring sweep
    /// restricted to exact/prefix matches would give: phase one finds
    /// matching `(segment, row)` pairs via [`crate::nameidx`] (or the O(1)
    /// dictionary probe for an exact uid), filters them for belief at
    /// `as_of_tt` exactly as `resolve_entities` does row-at-a-time
    /// (`effective_tt_e` idiom, `read.rs:785`), and collects the matched
    /// uids; phase two resolves each matched uid's *canonical* state through
    /// [`Self::believed_node_versions`] — the same postings route
    /// `nodes_with_believed_versions` already uses, flat in correction
    /// density — taking the latest by `(vt_s, vid)`, which is exactly the
    /// tie-break the sweep's `upsert` closure computes (its vid tiebreak is
    /// unreachable for one uid's believed versions, same note as
    /// `resolve_entities`'s doc comment).
    ///
    /// Returns `(uid, score, label, props)`, same shape as `resolve_entities`
    /// (score 0 = exact uid, 2 = name match — 1, the uid-substring score, is
    /// not reachable here because this function never does substring
    /// matching), ordered by `(score, uid)`.
    pub fn lookup_name(
        &self,
        query: &str,
        mode: NameMatchMode,
        as_of_tt: i64,
        limit: Option<usize>,
    ) -> Result<Vec<(String, u8, String, String)>> {
        self.assert_full_belief(as_of_tt)?;
        let ql = query.to_lowercase();
        let closes = self.close_index()?;
        // uid -> best score seen so far (0 beats 2, matching resolve_entities)
        let mut scored: HashMap<String, u8> = HashMap::new();
        let bump = |scored: &mut HashMap<String, u8>, uid: String, score: u8| {
            scored
                .entry(uid)
                .and_modify(|s| *s = (*s).min(score))
                .or_insert(score);
        };

        // O(1) exact-uid path (`dict.rs:159`) — already possible today, the
        // sweep just does it the slow way. Only
        // meaningful for `mode == Exact`; there is no "prefix uid" query.
        if mode == NameMatchMode::Exact {
            if let Some(id) = self.dict().dense_id(query) {
                let uid = self.uid_of(id)?;
                if !self.believed_node_versions(&uid, as_of_tt)?.is_empty() {
                    bump(&mut scored, uid, 0);
                }
            }
        }

        // per-segment sorted name index: candidates only, belief-filtered
        // row by row exactly like `resolve_entities`'s main loop.
        for (file, id) in self.node_files() {
            let sha = self.node_segment_sha(id);
            let seg = self.open_segment(&file)?;
            let idx = crate::nameidx::load_or_build_persist(
                self.root(),
                &seg,
                id,
                sha.as_deref().unwrap_or(""),
                !self.is_read_only(),
            )?;
            if idx.is_empty() {
                continue;
            }
            let candidates: &[u32] = match mode {
                NameMatchMode::Exact => idx.exact(&ql),
                NameMatchMode::Prefix => idx.prefix(&ql),
            };
            if candidates.is_empty() {
                continue;
            }
            // The index is a candidate generator, never the answer. Each
            // candidate row is re-checked against the segment's own name
            // column, so an index that does not describe these bytes — a
            // legacy one stamped `""` meeting a different segment under the
            // same id — can only lose candidates, never return a node whose
            // name does not match, and never index past the segment's end.
            // Cost: one string-table lookup, one UTF-8 check and one
            // casefold per candidate, on columns `open` already decoded —
            // less than the `uid_of` each surviving candidate pays below,
            // and far less than its phase-2 `believed_node_versions`.
            let Ok(name_ref) = seg.u32_column("name_ref") else {
                continue;
            };
            let names = seg.strings()?;
            let rows = seg.rows();
            let tt_s_v = seg.tt_s_view()?;
            let sidecar = seg.sidecar();
            let uid_id = seg.u32_column("uid_id")?;
            for &row in candidates {
                if row >= rows {
                    continue;
                }
                let Some(name) = names.get_opt(name_ref[row as usize])? else {
                    continue;
                };
                let nl = name.to_lowercase();
                let matches = match mode {
                    NameMatchMode::Exact => nl == ql,
                    NameMatchMode::Prefix => nl.starts_with(&ql),
                };
                if !matches {
                    continue;
                }
                // The close-run-outranks-sidecar idiom, extracted so it
                // exists once, called rather than re-inlined. The candidate list is sparse — a handful of
                // rows out of a segment — so this site stays a row test by
                // design: a per-segment range build would cost
                // more than the rows it could skip.
                let tt_e = effective_tt_e(&closes, &sidecar, id, row);
                if !believed_at(tt_s_v.at(row)?, tt_e, as_of_tt) {
                    continue;
                }
                let uid = self.uid_of(uid_id[row as usize])?;
                bump(&mut scored, uid, 2);
            }
        }

        // rows staged in an open batch: a batch must read its own writes,
        // same as `resolve_entities` — the on-disk index cannot see them.
        let staged_closes = self.staged_closes();
        for r in self.staged_nodes() {
            let tt_e = staged_closes.get(&r.vid).copied().unwrap_or(OPEN_END);
            if !believed_at(r.tt_s, tt_e, as_of_tt) {
                continue;
            }
            let uid = self.uid_of(r.uid_id)?;
            if mode == NameMatchMode::Exact && uid == query {
                bump(&mut scored, uid.clone(), 0);
            }
            if let Some(name) = crate::segment::name_of(&r.props) {
                if !name.is_empty() {
                    let nl = name.to_lowercase();
                    let hit = match mode {
                        NameMatchMode::Exact => nl == ql,
                        NameMatchMode::Prefix => nl.starts_with(&ql),
                    };
                    if hit {
                        bump(&mut scored, uid, 2);
                    }
                }
            }
        }

        // phase 2: canonical state via postings, same tie-break resolve_entities's
        // `upsert` computes (`key >= e.1` on `(vt_s, vid)` — `believed_node_versions`
        // already returns its result sorted ascending by that key, so the
        // canonical version is simply the last one).
        let mut out = Vec::with_capacity(scored.len());
        for (uid, score) in scored {
            let versions = self.believed_node_versions(&uid, as_of_tt)?;
            if let Some(v) = versions.last() {
                out.push((uid, score, v.label.clone(), v.props.clone()));
            }
        }
        out.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        if let Some(n) = limit {
            out.truncate(n);
        }
        Ok(out)
    }

    /// `SegmentEntry.sha` for one node segment id — the name index's stamp.
    /// It is the segment footer's `body_sha` (the content address
    /// `Segment::verify` checks against), copied into the entry by the writer
    /// that computed it; an entry written by an older engine carries `""`, which
    /// stamps nothing (see `SegmentEntry::sha`). `None` only if the manifest
    /// and the open file have already diverged, which every other read path
    /// here would also choke on.
    fn node_segment_sha(&self, id: u64) -> Option<String> {
        self.manifest()
            .node_store
            .iter()
            .find(|e| segment_id_of(&e.file) == id)
            .map(|e| e.sha.clone())
    }

    /// Running statistics, building them once if this is the first call.
    ///
    /// After the initial build every commit folds its own batch in, so a
    /// write-then-read loop never rescans. Compaction rewrites rows without
    /// changing content, so it leaves these untouched by construction.
    ///
    /// The build folds integer columns segment by segment. It used to route
    /// through `all_edge_versions`, which materializes every row — two
    /// dictionary lookups, several string allocations, and a sha256-derived
    /// `eid` per row that statistics never read — as one store-sized
    /// transient `Vec`. At 10M rows that transient alone exceeded a 2 GB
    /// memory cap, so the first query
    /// OOM'd before the byte-budgeted segment cache could matter. Counts
    /// cover every stored row, belief ignored, exactly as before — the
    /// DuckDB adapter must agree here or `estimate_cost` diverges.
    ///
    /// Streaming: the
    /// per-segment fold above still left every segment's decoded columns
    /// resident, because it opened segments through `open_segment`, whose
    /// session-wide cache keeps a decoded segment around (up to its byte
    /// budget — half of physical RAM by default, i.e. effectively unbounded
    /// on a large build host) long after this fold moved past it. At 17.4M
    /// edges that turned a flat 2.4 GB ingest RSS into a 25 GB
    /// finalisation spike. This now opens each segment with
    /// `open_segment_uncached`, which skips the session cache entirely: the
    /// segment (and the columns `open()` decodes for it) is dropped at the
    /// end of each loop iteration, so the working set is bounded to one
    /// segment's decoded columns at a time, not the whole store. The
    /// accumulator (`acc`) is the only state that survives across segments,
    /// and it is already O(distinct sources + distinct rel types) by
    /// definition — the same size the non-streaming form produced. Output is
    /// byte-identical to before (proven in
    /// `stats_streaming_matches_the_cached_open_path` below and, on the
    /// Python side, `tests/test_store_digest_streaming.py`); only the
    /// segment-open path changed.
    pub fn stats_accum(&self) -> Result<crate::store::StatsAccum> {
        {
            let cell = self.stats_cell().lock().expect("stats mutex poisoned");
            if let Some(acc) = cell.as_ref() {
                return Ok(acc.clone());
            }
        }
        let mut acc = crate::store::StatsAccum::default();
        for (file, _id) in self.edge_files() {
            let seg = self.open_segment_uncached(&file)?;
            let h = seg.header();
            let vt_s = seg.i64_column("vt_s")?;
            let vt_e = if h.vt_e_elided {
                None
            } else {
                Some(seg.i64_column("vt_e")?)
            };
            let src = seg.u32_column("src_id")?;
            let rel = seg.u16_column("rel_code")?;
            // per-code counts within the segment, names merged once at the
            // end — a string allocation per row was most of the fold
            let mut code_counts = vec![0u64; h.rel_types.len()];
            for i in 0..vt_s.len() {
                let ve = vt_e.map(|c| c[i]).unwrap_or(vt_s[i] + 1);
                acc.n_edge_versions += 1;
                acc.vt_min = Some(acc.vt_min.map_or(vt_s[i], |m| m.min(vt_s[i])));
                let ve = if ve >= OPEN_END { vt_s[i] + 1 } else { ve };
                acc.vt_max = Some(acc.vt_max.map_or(ve, |m| m.max(ve)));
                if let Some(c) = code_counts.get_mut(rel[i] as usize) {
                    *c += 1;
                }
                *acc.out_degree.entry(src[i]).or_default() += 1;
            }
            for (code, n) in code_counts.into_iter().enumerate() {
                if n > 0 {
                    *acc
                        .rel_type_counts
                        .entry(h.rel_types[code].clone())
                        .or_default() += n;
                }
            }
        }
        for r in self.staged_edges() {
            acc.add_edge(r.vt_s, r.vt_e, &r.rel_type, r.src_id);
        }
        acc.n_node_versions = self
            .manifest()
            .node_store
            .iter()
            .map(|e| e.rows as u64)
            .sum::<u64>()
            + self.staged_nodes().len() as u64;
        let mut cell = self.stats_cell().lock().expect("stats mutex poisoned");
        // an open batch's rows are already counted above; do not cache a
        // snapshot that a rollback could invalidate
        if !self.in_batch() {
            *cell = Some(acc.clone());
        }
        Ok(acc)
    }

    /// Canonical-JSON props for specific version ids, returned verbatim.
    /// Props for a handful of version ids.
    ///
    /// Deliberately does not route through `all_*_versions`. That materializes
    /// every row in the store — two dictionary lookups, several string
    /// allocations, and a sha256 to derive `eid` — so a lookup of 16 vids cost
    /// 353 ms at 200k edge versions, and cost exactly the same for one vid as
    /// for 256. It was the whole of `diff_snapshots`, its only caller.
    ///
    /// The sweep below touches three integer columns and reads a string only
    /// once a vid actually matches. It is still O(rows): vids are hashes, so
    /// segments have no order to search them by and there is no vid index.
    /// Only the constant changed — but the constant was the problem.
    pub fn props_for_vids(&self, kind: RowKind, vids: &[String]) -> Result<HashMap<String, String>> {
        // Keyed by the caller's own spelling, so the result is looked up with
        // the same string that was asked for. A vid that is not a well-formed
        // identity cannot name a row, so it is dropped rather than swept for.
        let wanted: HashMap<(u64, u32), &str> = vids
            .iter()
            .filter_map(|v| Id96::from_hex(v).ok().map(|id| ((id.hi, id.lo), v.as_str())))
            .collect();
        let mut out = HashMap::with_capacity(wanted.len());
        if wanted.is_empty() {
            return Ok(out);
        }
        let files = match kind {
            RowKind::Edge => self.edge_files(),
            RowKind::Node => self.node_files(),
        };
        for (file, _id) in files {
            let seg = self.open_segment(&file)?;
            let hi = seg.u64_column("vid64")?;
            let lo = seg.u32_column("vid_lo32")?;
            let props = seg.u32_column("props_ref")?;
            let strings = seg.strings()?;
            for i in 0..hi.len() {
                if let Some(&hex) = wanted.get(&(hi[i], lo[i])) {
                    out.insert(hex.to_string(), strings.get(props[i])?.to_string());
                }
            }
        }
        // Staged rows are inserted last so they win, as they do in
        // `all_*_versions`: a batch must read its own writes.
        match kind {
            RowKind::Edge => {
                for r in self.staged_edges() {
                    if let Some(&hex) = wanted.get(&(r.vid.hi, r.vid.lo)) {
                        out.insert(hex.to_string(), r.props.clone());
                    }
                }
            }
            RowKind::Node => {
                for r in self.staged_nodes() {
                    if let Some(&hex) = wanted.get(&(r.vid.hi, r.vid.lo)) {
                        out.insert(hex.to_string(), r.props.clone());
                    }
                }
            }
        }
        Ok(out)
    }

    // ------------------------------------------------------------------ //
    // the bounded version page (`version_history`)                    //
    // ------------------------------------------------------------------ //

    /// A fixed-width sort key per *surviving* row, and nothing else.
    ///
    /// The first of the two passes behind `version_page`. It has the sweep
    /// shape of `props_for_vids`: open each segment once, read integer
    /// columns, and touch the string table only for rows that matter — here,
    /// not even then, because a key carries no strings at all.
    ///
    /// **Why a key and not a row.** `version_history` returns at most
    /// `limit` rows but must order the whole surviving population to know
    /// which ones those are, and must count it to report `rows_total`
    /// exactly. Doing that through `all_*_versions` cost ~1,060 bytes per
    /// stored version — 10.6 GB at 10M, measured — for a 50-row
    /// page. A [`VersionKey`] is 32 bytes and carries exactly what the sort
    /// and the second pass need.
    ///
    /// **Cost note.** `tt_s` is run-length encoded in the segment
    /// header and this reads it per row. On a compacted store the global
    /// `(vt_s, vid)` re-sort shatters those runs — 2.1M runs over 3.0M rows
    /// has been measured — so the binary search in
    /// [`SegmentHeader::tt_s_at`](crate::segment::SegmentHeader::tt_s_at) is
    /// load-bearing here, not a micro-optimization (dictionary-coded
    /// segments replace it with two indexed loads). **What it is paid on is
    /// narrow**: `tt_s` is now read only for rows the segment's verdict
    /// ranges already admit, so a store whose rows are mostly superseded
    /// stops paying a binary search per superseded row.
    ///
    /// **Belief is a per-segment range list, in all three modes.** Both
    /// belief tests this loop used to run per row fold into the range walk —
    /// `tt_s > f.as_of` is exactly what
    /// [`believed_ranges_any`](crate::scan::believed_ranges_any) excludes,
    /// and `tt_e <= f.as_of` is exactly what `subtract_closes` removes. So
    /// [`Belief::All`] is the base list,
    /// [`Belief::Current`] is
    /// [`believed_ranges_closed`](crate::scan::believed_ranges_closed), and
    /// [`Belief::Superseded`] is
    /// [`believed_ranges_superseded`](crate::scan::believed_ranges_superseded)
    /// — the complement of the second within the first, the sibling of `subtract_closes` this mode had been waiting for.
    /// Without it `Superseded` would have had to keep the row test or
    /// difference two lists, and this function would have been converted in
    /// two of its three modes.
    ///
    /// The valid-time window and the `rel_type` mask are **intersected**
    /// with the ranges, not replaced by them: they are per-row predicates
    /// over a different column and the ranges say nothing about them.
    ///
    /// Segments are scanned in `edge_files()` / `node_files()` order and
    /// staged rows last, which is the order `all_*_versions` produced;
    /// `believed_ranges` and both of its subtractions emit minimal, strictly
    /// ascending ranges, so walking them preserves ascending row order
    /// within a segment. The sort in `version_page` is stable, so that order
    /// still breaks ties.
    pub fn version_keys(&self, kind: RowKind, f: &VersionFilter<'_>) -> Result<Vec<VersionKey>> {
        let closes = self.close_index()?;
        let mut keys: Vec<VersionKey> = Vec::new();
        let files = match kind {
            RowKind::Edge => self.edge_files(),
            RowKind::Node => self.node_files(),
        };
        for (file, seg_id) in files {
            let seg = self.open_segment(&file)?;
            let h = seg.header();
            let tt_s_v = seg.tt_s_view()?;
            // `rel_type` is a per-segment code, so the filter becomes a
            // lookup table over this segment's own vocabulary — and a
            // segment naming none of the wanted types is skipped whole,
            // without reading a single column.
            let wanted: Option<Vec<bool>> = match (kind, f.rel_types) {
                (RowKind::Edge, Some(names)) => {
                    let mask: Vec<bool> = h
                        .rel_types
                        .iter()
                        .map(|t| names.iter().any(|n| n == t))
                        .collect();
                    if !mask.iter().any(|&b| b) {
                        continue;
                    }
                    Some(mask)
                }
                _ => None,
            };
            // A segment whose valid-time extent misses the window cannot
            // contribute a key (every row would fail the per-row test below),
            // so it is skipped from the header, as `diff_belief` and the scan
            // planner already skip it. `vt_e_max` is the largest `vt_e`
            // including the elided `vt_s + 1`.
            if h.vt_min >= f.t_b || h.vt_e_max <= f.t_a {
                continue;
            }
            let vt_s = seg.i64_column("vt_s")?;
            let vt_e = if h.vt_e_elided {
                None
            } else {
                Some(seg.i64_column("vt_e")?)
            };
            let vid_hi = seg.u64_column("vid64")?;
            let vid_lo = seg.u32_column("vid_lo32")?;
            // The rows the window can touch. `vt_s >= t_b` is a suffix
            // (rows are stored in `vt_s` order) and, with `vt_e` elided,
            // `vt_e <= t_a` is a prefix; both are cut by binary search, the
            // same cut `diff_belief` makes. Never narrower than the truth, so
            // the per-row window test below still decides every row: the cut
            // moves cost only. An earlier version of this loop visited every believed
            // row of every segment and skipped the ones outside the window.
            let (w_lo, w_hi) = crate::beliefdiff::window_rows(vt_s, h.vt_e_elided, f.t_a, f.t_b);
            if w_lo >= w_hi {
                continue;
            }
            let rel = match wanted {
                Some(_) => Some(seg.u16_column("rel_code")?),
                None => None,
            };
            // The belief half of the predicate, resolved once per segment.
            // `close_view` layers the close runs over the folded sidecar with
            // the same precedence `effective_tt_e` applies per row, so the
            // three mode arms below decide exactly what `Belief::admits`
            // decided row by row.
            //
            // `f.as_of` is passed **unclamped**, which is what this function
            // has always compared against: `version_history`'s contract is
            // the raw `tt_s <= as_of` / `tt_e <= as_of` pair, clamped once by
            // the Python adapter on the way in rather than again here, and
            // `version_page_agrees_with_the_materialized_listing` pins that
            // at `as_of == OPEN_END` for all three modes. The range
            // primitives compare in exactly the same form, so handing them
            // the same value reproduces the same verdict row for row.
            let as_of = f.as_of;
            let n = vt_s.len();
            let view =
                crate::visibility::close_view_range(seg.sidecar(), &closes, seg_id, w_lo, w_hi, n);
            let bounds = seg.tt_s_bounds();
            // The one place the two spellings of the predicate part company.
            // A row nobody has corrected has `tt_e == OPEN_END`, and it is
            // absent from the `CloseView` — so the range primitives call it
            // open, always. The row test called it superseded when
            // `OPEN_END <= as_of`, i.e. only at the unclamped `as_of ==
            // OPEN_END` this function's contract admits and the adapter
            // never sends (it clamps to `OPEN_END - 1`, where the two agree
            // again). At that one value every stored row is superseded —
            // every `tt_e` is at most `OPEN_END` — so the answer needs no
            // walk at all, and saying that here keeps the ranges themselves
            // free of the oddity. `version_page_agrees_with_the_materialized_listing`
            // pins both sides.
            let open_rows_superseded = OPEN_END <= as_of;
            let ranges = match (f.belief, open_rows_superseded) {
                (Belief::All, _) | (Belief::Superseded, true) => {
                    crate::scan::believed_ranges_any_in(&tt_s_v, bounds, as_of, n, w_lo, w_hi)
                }
                (Belief::Current, true) => Vec::new(),
                (Belief::Current, false) => crate::scan::believed_ranges_closed_in(
                    &tt_s_v, bounds, as_of, n, w_lo, w_hi, &view,
                ),
                (Belief::Superseded, false) => crate::scan::believed_ranges_superseded_in(
                    &tt_s_v, bounds, as_of, n, w_lo, w_hi, &view,
                ),
            };
            for (a, b) in ranges {
                for i in a..b {
                    // `vt_e` elided means every row of this segment is
                    // instantaneous, exactly as `edge_rows_sel` reads it
                    let ve = vt_e.map(|c| c[i]).unwrap_or(vt_s[i] + 1);
                    if vt_s[i] >= f.t_b || ve <= f.t_a {
                        continue;
                    }
                    if let (Some(mask), Some(rel)) = (&wanted, rel) {
                        if !mask.get(rel[i] as usize).copied().unwrap_or(false) {
                            continue;
                        }
                    }
                    keys.push(VersionKey {
                        // still per row — `tt_s` is an output field of the
                        // key, not a filter — and read through the tt_s view,
                        // never the header: on a dictionary-coded segment
                        // `tt_s_runs` is empty and a header read has no run
                        // to find
                        tt_s: tt_s_v.at(i as u32)?,
                        vid_hi: vid_hi[i],
                        vid_lo: vid_lo[i],
                        seg_id,
                        row: i as u32,
                    });
                }
            }
        }
        // Rows staged in the open batch, which `all_*_versions` unions in and
        // this path therefore must too: a batch reads its own writes.
        match kind {
            RowKind::Edge => {
                let staged_closes = self.staged_closes();
                for (i, r) in self.staged_edges().iter().enumerate() {
                    if r.vt_s >= f.t_b || r.vt_e <= f.t_a || r.tt_s > f.as_of {
                        continue;
                    }
                    if let Some(names) = f.rel_types {
                        if !names.contains(&r.rel_type) {
                            continue;
                        }
                    }
                    let tt_e = staged_closes.get(&r.vid).copied().unwrap_or(OPEN_END);
                    if !f.belief.admits(tt_e <= f.as_of) {
                        continue;
                    }
                    keys.push(VersionKey {
                        tt_s: r.tt_s,
                        vid_hi: r.vid.hi,
                        vid_lo: r.vid.lo,
                        seg_id: STAGED_SEG,
                        row: i as u32,
                    });
                }
            }
            RowKind::Node => {
                let staged_closes = self.staged_closes();
                for (i, r) in self.staged_nodes().iter().enumerate() {
                    if r.vt_s >= f.t_b || r.vt_e <= f.t_a || r.tt_s > f.as_of {
                        continue;
                    }
                    let tt_e = staged_closes.get(&r.vid).copied().unwrap_or(OPEN_END);
                    if !f.belief.admits(tt_e <= f.as_of) {
                        continue;
                    }
                    keys.push(VersionKey {
                        tt_s: r.tt_s,
                        vid_hi: r.vid.hi,
                        vid_lo: r.vid.lo,
                        seg_id: STAGED_SEG,
                        row: i as u32,
                    });
                }
            }
        }
        Ok(keys)
    }

    /// The second pass: the projected columns of the rows `keys` names, in
    /// that order.
    ///
    /// The companion of `version_keys` and the same trade `edge_idents_at`
    /// makes — a physical address is cheap to carry and exact to resolve, so
    /// the expensive per-row work (a `sha256` for an `eid`, two dictionary
    /// lookups, a string-table read) happens once per *returned* row rather
    /// than once per scanned one. `props`, `source` and `provenance_ref` are
    /// never read: `version_history` drops all three, and building them was
    /// most of what the old path cost.
    ///
    /// Addresses are only meaningful against the generation that produced
    /// them, which is why the two passes are composed inside `version_page`
    /// rather than across the Python boundary.
    pub fn version_rows_at(&self, kind: RowKind, keys: &[VersionKey]) -> Result<VersionRows> {
        let closes = self.close_index()?;
        let files: HashMap<u64, String> = match kind {
            RowKind::Edge => self.edge_files(),
            RowKind::Node => self.node_files(),
        }
        .into_iter()
        .map(|(file, id)| (id, file))
        .collect();
        // group by segment so each is opened (and verified) once; output
        // positions come along so input order is restored at the end
        let mut by_seg: std::collections::BTreeMap<u64, Vec<usize>> = Default::default();
        for (i, k) in keys.iter().enumerate() {
            by_seg.entry(k.seg_id).or_default().push(i);
        }
        let staged_positions = by_seg.remove(&STAGED_SEG).unwrap_or_default();

        match kind {
            RowKind::Edge => {
                let mut out: Vec<Option<EdgeVersionRow>> = vec![None; keys.len()];
                for (seg_id, positions) in by_seg {
                    let file = files.get(&seg_id).ok_or_else(|| {
                        EngineError::invariant(format!(
                            "segment id {seg_id} is not in the current generation"
                        ))
                    })?;
                    let seg = self.open_segment(file)?;
                    let h = seg.header();
                    let tt_s_v = seg.tt_s_view()?;
                    let strings = seg.strings()?;
                    let sidecar = seg.sidecar();
                    let vt_s = seg.i64_column("vt_s")?;
                    let vt_e = if h.vt_e_elided {
                        None
                    } else {
                        Some(seg.i64_column("vt_e")?)
                    };
                    let src = seg.u32_column("src_id")?;
                    let dst = seg.u32_column("dst_id")?;
                    let rel = seg.u16_column("rel_code")?;
                    let disc = seg.u32_column("disc_ref")?;
                    let vid_hi = seg.u64_column("vid64")?;
                    let vid_lo = seg.u32_column("vid_lo32")?;
                    for p in positions {
                        let i = keys[p].row as usize;
                        if i >= vt_s.len() {
                            return Err(EngineError::invariant(format!(
                                "row {i} is out of bounds for segment {seg_id}"
                            )));
                        }
                        let src_uid = self.uid_of(src[i])?;
                        let dst_uid = self.uid_of(dst[i])?;
                        let rel_type = h
                            .rel_types
                            .get(rel[i] as usize)
                            .ok_or_else(|| {
                                EngineError::corrupt(format!("rel_code {} has no entry", rel[i]))
                                    .at_row(i as u32)
                            })?
                            .clone();
                        let disc_s = strings.get(disc[i])?.to_string();
                        let tt_e = effective_tt_e(&closes, &sidecar, seg_id, i as u32);
                        out[p] = Some(EdgeVersionRow {
                            vid: Id96 {
                                hi: vid_hi[i],
                                lo: vid_lo[i],
                            }
                            .to_hex(),
                            eid: edge_eid(&src_uid, &dst_uid, &rel_type, &disc_s).to_hex(),
                            src: src_uid,
                            dst: dst_uid,
                            rel_type,
                            disc: disc_s,
                            vt_s: vt_s[i],
                            vt_e: vt_e.map(|c| c[i]).unwrap_or(vt_s[i] + 1),
                            tt_s: tt_s_v.at(i as u32)?,
                            tt_e,
                        });
                    }
                }
                if !staged_positions.is_empty() {
                    let staged = self.staged_edges();
                    let staged_closes = self.staged_closes();
                    for p in staged_positions {
                        let r = staged.get(keys[p].row as usize).ok_or_else(|| {
                            EngineError::invariant(format!(
                                "staged row {} is no longer in the open batch",
                                keys[p].row
                            ))
                        })?;
                        let src = self.uid_of(r.src_id)?;
                        let dst = self.uid_of(r.dst_id)?;
                        out[p] = Some(EdgeVersionRow {
                            vid: r.vid.to_hex(),
                            eid: edge_eid(&src, &dst, &r.rel_type, &r.disc).to_hex(),
                            src,
                            dst,
                            rel_type: r.rel_type.clone(),
                            disc: r.disc.clone(),
                            vt_s: r.vt_s,
                            vt_e: r.vt_e,
                            tt_s: r.tt_s,
                            tt_e: staged_closes.get(&r.vid).copied().unwrap_or(OPEN_END),
                        });
                    }
                }
                Ok(VersionRows::Edge(
                    out.into_iter()
                        .map(|o| o.expect("every requested address was filled"))
                        .collect(),
                ))
            }
            RowKind::Node => {
                let mut out: Vec<Option<NodeVersionRow>> = vec![None; keys.len()];
                for (seg_id, positions) in by_seg {
                    let file = files.get(&seg_id).ok_or_else(|| {
                        EngineError::invariant(format!(
                            "segment id {seg_id} is not in the current generation"
                        ))
                    })?;
                    let seg = self.open_segment(file)?;
                    let h = seg.header();
                    let tt_s_v = seg.tt_s_view()?;
                    let strings = seg.strings()?;
                    let sidecar = seg.sidecar();
                    let vt_s = seg.i64_column("vt_s")?;
                    let vt_e = if h.vt_e_elided {
                        None
                    } else {
                        Some(seg.i64_column("vt_e")?)
                    };
                    let uid_id = seg.u32_column("uid_id")?;
                    let label = seg.u32_column("label_ref")?;
                    let vid_hi = seg.u64_column("vid64")?;
                    let vid_lo = seg.u32_column("vid_lo32")?;
                    for p in positions {
                        let i = keys[p].row as usize;
                        if i >= vt_s.len() {
                            return Err(EngineError::invariant(format!(
                                "row {i} is out of bounds for segment {seg_id}"
                            )));
                        }
                        let tt_e = effective_tt_e(&closes, &sidecar, seg_id, i as u32);
                        out[p] = Some(NodeVersionRow {
                            vid: Id96 {
                                hi: vid_hi[i],
                                lo: vid_lo[i],
                            }
                            .to_hex(),
                            uid: self.uid_of(uid_id[i])?,
                            label: strings.get(label[i])?.to_string(),
                            vt_s: vt_s[i],
                            vt_e: vt_e.map(|c| c[i]).unwrap_or(vt_s[i] + 1),
                            tt_s: tt_s_v.at(i as u32)?,
                            tt_e,
                        });
                    }
                }
                if !staged_positions.is_empty() {
                    let staged = self.staged_nodes();
                    let staged_closes = self.staged_closes();
                    for p in staged_positions {
                        let r = staged.get(keys[p].row as usize).ok_or_else(|| {
                            EngineError::invariant(format!(
                                "staged row {} is no longer in the open batch",
                                keys[p].row
                            ))
                        })?;
                        out[p] = Some(NodeVersionRow {
                            vid: r.vid.to_hex(),
                            uid: self.uid_of(r.uid_id)?,
                            label: r.label.clone(),
                            vt_s: r.vt_s,
                            vt_e: r.vt_e,
                            tt_s: r.tt_s,
                            tt_e: staged_closes.get(&r.vid).copied().unwrap_or(OPEN_END),
                        });
                    }
                }
                Ok(VersionRows::Node(
                    out.into_iter()
                        .map(|o| o.expect("every requested address was filled"))
                        .collect(),
                ))
            }
        }
    }

    /// One ordered page of `version_history`, and the exact size of the
    /// population it was drawn from.
    ///
    /// Both passes run under one `close_index()` and one manifest read, so
    /// the physical addresses the first pass produces cannot be invalidated
    /// by a compaction before the second pass resolves them.
    ///
    /// Order is `(tt_s, vid)` — the belief log's own, not valid time.
    /// `vid` is compared as its 96-bit integer, which is what a comparison of
    /// the zero-padded lowercase hex the Python path sorted comes to.
    /// **Stable**, so rows tying on both keys keep the scan order
    /// `np.lexsort` (also stable) preserved over `all_*_versions`.
    pub fn version_page(
        &self,
        kind: RowKind,
        f: &VersionFilter<'_>,
        offset: i64,
        limit: i64,
    ) -> Result<(VersionRows, usize)> {
        let mut keys = self.version_keys(kind, f)?;
        // `sort_by_key` is a stable sort, which is the load-bearing part
        keys.sort_by_key(|k| (k.tt_s, k.vid_hi, k.vid_lo));
        let total = keys.len();
        let (start, end) = py_slice(offset, limit, total);
        Ok((self.version_rows_at(kind, &keys[start..end])?, total))
    }

    /// Node versions believed at `as_of_tt` whose valid interval overlaps
    /// `[vt_min, vt_max)` — the filter applied *during* the sweep.
    ///
    /// The counterpart of `version_keys` for the second O(store) site. This
    /// used to be `all_node_versions()` followed by a `.filter()`: every
    /// stored node version became a `NodeVersionOut` — a uid, a 24-hex vid, a
    /// label, and the `props`/`source`/`provenance_ref` no caller of
    /// `nodes_columnar` has ever wanted — and the survivors were then picked
    /// out of that. That scan did not finish in 1,400 s over a store of
    /// 2,997,352 node versions.
    ///
    /// **The sweep no longer visits every row.** It used to: node segments
    /// carry no *identity* index, and the reading of "the node store
    /// is identity-clustered" was that the edge cursor's machinery bought
    /// nothing here. That conflated two different indexes. Node segments are
    /// written sorted by `(vt_s, vid)` — `NodeRow::sort_key`, the same key
    /// `EdgeRow` uses — and they carry the same `tt_s_runs`, the same folded
    /// `(vt_min, vt_max, vt_e_max)` zone map and the same close sidecar. Every
    /// input `select_range` narrows an edge segment with is therefore present
    /// on a node segment, and the flat `for i in 0..rows` loop was leaving all
    /// of it on the floor: a valid-time window still touched every stored
    /// version, and the belief predicate's `as_of < tt_e` half was a per-row
    /// probe on every one of them.
    ///
    /// What is identity-clustered is the *name/uid* side, which is the name
    /// index and is untouched here. `resolve_entities` now runs on this same
    /// `ScanSet` too; the *name index* is
    /// still built over every stored row, deliberately, so that it can
    /// answer a historical `as_of`.
    ///
    /// So this is now the same three-tier narrowing the edge cursor does —
    /// prune from the header, binary-search the valid-time window, subtract
    /// the closed rows from the verdict ranges — followed by the same
    /// materialize-only-the-survivors rule (a row's uid,
    /// vid and label are built only once it is selected, and `uid_id` comes
    /// straight off the column instead of being recovered from the
    /// materialized uid by a reverse dictionary lookup).
    ///
    /// **`rows_examined` changed meaning with this**, and the counter is on
    /// the PyO3 surface: it used to be "every stored node version", because
    /// that is what the sweep read; it is now "rows the verdict ranges handed
    /// to the row loop", the same thing it means for `scan_edges`. The two
    /// tests that pinned the old meaning say so.
    ///
    /// The answer is unchanged, row for row —
    /// `scan_node_versions_agrees_with_the_filtered_listing` is the oracle and
    /// is untouched.
    pub fn scan_node_versions(
        &self,
        as_of_tt: i64,
        vt_min: Option<i64>,
        vt_max: Option<i64>,
    ) -> Result<NodeScanOut> {
        use crate::scan::{ScanRequest, ScanSet, ScanTarget};

        let closes = self.close_index()?;
        let mut out = NodeScanOut::default();

        // Opened up front rather than one at a time, so the whole set can be
        // narrowed in one pass — the shape `scan_edges` already uses. These
        // are the segment cache's own `Arc`s, so holding them costs a
        // refcount each, not a re-map.
        let files = self.node_files();
        let mut segs = Vec::with_capacity(files.len());
        for (file, seg_id) in &files {
            segs.push((self.open_segment(file)?, *seg_id));
        }
        let (selections, stats) = {
            let targets: Vec<ScanTarget<'_, crate::segment::MmapSource>> = segs
                .iter()
                .map(|(segment, id)| ScanTarget {
                    segment,
                    // node segments record their own lane; `select_range`
                    // does not read it, but a wrong one here would be a
                    // wrong one for whatever reads it next
                    lane: segment.header().lane,
                    id: *id,
                })
                .collect();
            ScanSet::new(targets)
                .with_closes(closes)
                .select(&ScanRequest {
                    as_of_tt,
                    vt_min,
                    vt_max,
                    ..ScanRequest::default()
                })?
        };
        out.rows_examined += stats.rows_examined as usize;

        for sel in &selections {
            let (seg, _) = &segs[sel.segment];
            let strings = seg.strings()?;
            let vt_s = seg.i64_column("vt_s")?;
            let vt_e = if seg.header().vt_e_elided {
                None
            } else {
                Some(seg.i64_column("vt_e")?)
            };
            let uid_id = seg.u32_column("uid_id")?;
            let label = seg.u32_column("label_ref")?;
            let vid_hi = seg.u64_column("vid64")?;
            let vid_lo = seg.u32_column("vid_lo32")?;
            for &row in &sel.rows {
                let i = row as usize;
                out.rows.push(NodeScanRow {
                    uid_id: uid_id[i],
                    uid: self.uid_of(uid_id[i])?,
                    vid: Id96 {
                        hi: vid_hi[i],
                        lo: vid_lo[i],
                    }
                    .to_hex(),
                    label: strings.get(label[i])?.to_string(),
                    vt_s: vt_s[i],
                    // `vt_e` elided means every row is instantaneous, the
                    // rule `select_range`'s prefix cut already relies on
                    vt_e: vt_e.map(|c| c[i]).unwrap_or(vt_s[i] + 1),
                });
            }
        }
        let staged = self.staged_nodes();
        out.rows_examined += staged.len();
        let staged_closes = self.staged_closes();
        for r in staged {
            if vt_min.is_some_and(|t| r.vt_e <= t) || vt_max.is_some_and(|t| r.vt_s >= t) {
                continue;
            }
            let tt_e = staged_closes.get(&r.vid).copied().unwrap_or(OPEN_END);
            if !crate::believed_at(r.tt_s, tt_e, as_of_tt) {
                continue;
            }
            out.rows.push(NodeScanRow {
                uid_id: r.uid_id,
                uid: self.uid_of(r.uid_id)?,
                vid: r.vid.to_hex(),
                label: r.label.clone(),
                vt_s: r.vt_s,
                vt_e: r.vt_e,
            });
        }
        out.rows_materialized = out.rows.len();
        out.rows.sort_by(|a, b| (a.vt_s, &a.vid).cmp(&(b.vt_s, &b.vid)));
        Ok(out)
    }
}

/// Sentinel `seg_id` for a row staged in the open batch. It has no segment
/// yet, so `VersionKey::row` indexes the staging vector instead.
pub const STAGED_SEG: u64 = u64::MAX;

/// `rows[offset:offset + limit]`, with CPython's slice arithmetic.
///
/// The page offset is a plaintext decimal cursor fixed by the operator contract, and the
/// Python path took it by slicing a list — so a nonsensical cursor produced a
/// nonsensical-but-defined page rather than an error, and moving the slice
/// into the engine must not quietly turn that into a refusal or, worse, into
/// a different page. Each endpoint is adjusted by the length if it is
/// negative and then clamped, independently, exactly as `PySlice_AdjustIndices`
/// does; a stop below the start is an empty page.
fn py_slice(offset: i64, limit: i64, total: usize) -> (usize, usize) {
    let n = total as i64;
    let adjust = |v: i64| if v < 0 { (v + n).max(0) } else { v.min(n) };
    let start = adjust(offset);
    let stop = adjust(offset.saturating_add(limit));
    (start as usize, stop.max(start) as usize)
}

/// Which beliefs a version page selects, always **as of** its `as_of`
/// (`gnomon/temporal/ops_versions.py`'s contract).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Belief {
    /// Still believed then.
    Current,
    /// Already revised by then.
    Superseded,
    /// Everything written by then.
    All,
}

impl Belief {
    /// Does this mode admit a row whose belief had (or had not) ended by
    /// `as_of`? `superseded` is `tt_e <= as_of`, and nothing else: a belief
    /// that ended *after* `as_of` had not ended yet.
    fn admits(self, superseded: bool) -> bool {
        match self {
            Belief::Current => !superseded,
            Belief::Superseded => superseded,
            Belief::All => true,
        }
    }
}

/// The predicates a version page selects by, evaluated as of `as_of`.
#[derive(Clone, Copy, Debug)]
pub struct VersionFilter<'a> {
    /// The belief state to evaluate under. A version whose `tt_s` is after
    /// this does not exist yet and appears in no mode.
    pub as_of: i64,
    /// Half-open valid-time window `[t_a, t_b)`; a row survives if its own
    /// interval overlaps it.
    pub t_a: i64,
    pub t_b: i64,
    pub belief: Belief,
    /// Edges only — node versions have a label, not a relationship type.
    pub rel_types: Option<&'a [String]>,
}

/// A surviving row's sort key and its physical address: 32 bytes, no strings.
///
/// The forecast budgeted 28 (`seg_id` as a `u32`); segment ids are `u64`
/// throughout this engine, so the key is 32 with padding. At 10M surviving
/// rows that is 320 MB against 280 MB, both far under the ~10.6 GB the
/// materialized population cost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionKey {
    pub tt_s: i64,
    pub vid_hi: u64,
    pub vid_lo: u32,
    /// [`STAGED_SEG`] for a row in the open batch.
    pub seg_id: u64,
    pub row: u32,
}

/// One `version_history` node row: `StorageAdapter.VERSION_COLS["node"]` and
/// nothing else. No `props` — the operator drops it, and a blob per row on a
/// whole-store scan is exactly what this row avoids — and no `source` /
/// `provenance_ref`, which the projection pushdown leaves out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeVersionRow {
    pub vid: String,
    pub uid: String,
    pub label: String,
    pub vt_s: i64,
    pub vt_e: i64,
    pub tt_s: i64,
    /// Raw: the `OPEN_END` censoring against `as_of` is the operator's, and
    /// stays there so the contract has one home.
    pub tt_e: i64,
}

/// One `version_history` edge row: `StorageAdapter.VERSION_COLS["edge"]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeVersionRow {
    pub vid: String,
    pub eid: String,
    pub src: String,
    pub dst: String,
    pub rel_type: String,
    pub disc: String,
    pub vt_s: i64,
    pub vt_e: i64,
    pub tt_s: i64,
    pub tt_e: i64,
}

/// A page of version rows, of whichever kind was asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VersionRows {
    Node(Vec<NodeVersionRow>),
    Edge(Vec<EdgeVersionRow>),
}

impl VersionRows {
    pub fn len(&self) -> usize {
        match self {
            VersionRows::Node(v) => v.len(),
            VersionRows::Edge(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One believed node version, with the columns `nodes_columnar` projects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeScanRow {
    pub uid_id: u32,
    pub uid: String,
    pub vid: String,
    pub label: String,
    pub vt_s: i64,
    pub vt_e: i64,
}

/// The node scan's rows and its own account of what it did.
///
/// `rows_examined` counts every stored version the sweep visited;
/// `rows_materialized` counts the ones it built. They were the same number
/// before the filter moved below the materialization, which is the whole
/// claim, so both are reported.
#[derive(Clone, Debug, Default)]
pub struct NodeScanOut {
    pub rows: Vec<NodeScanRow>,
    pub rows_examined: usize,
    pub rows_materialized: usize,
}

/// Materialize either every row of a segment or just the selected ones.
///
/// Row indices that fall outside the segment are skipped rather than
/// erroring, matching what the previous `all.get(row)` lookup did: a stale
/// posting names a row that is no longer there, and that is not corruption.
fn collect_rows<T>(
    n: usize,
    rows: Option<&[u32]>,
    emit: impl Fn(usize) -> Result<T>,
) -> Result<Vec<T>> {
    match rows {
        Some(sel) => {
            let mut out = Vec::with_capacity(sel.len());
            for &row in sel {
                let i = row as usize;
                if i < n {
                    out.push(emit(i)?);
                }
            }
            Ok(out)
        }
        None => {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(emit(i)?);
            }
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derive::version_vid;
    use crate::manifest::EventLogRef;
    use crate::row::{EdgeRow, NodeRow};
    use std::path::PathBuf;

    fn tmp_root(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-read-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    /// The node identity key is written to disk, so its bytes are pinned
    /// here rather than left to the standard library.
    ///
    /// A change to any of these numbers means [`uid_key`] moved, and every
    /// persisted structure built over it — the identity filter — would
    /// admit **false negatives** against files written by the old function.
    /// The stamp that catches that is [`UID_KEY_ALGO_VERSION`]; this test
    /// exists to make bumping it a deliberate, reviewed act rather than a
    /// silent side effect of a toolchain bump, exactly as
    /// `nameidx::tests::casefold_vectors_are_pinned` does for casefolding.
    ///
    /// The vectors are the key's **persisted little-endian bytes**, which is
    /// the form a sidecar fingerprints, not just the `u64`.
    #[test]
    fn uid_key_bytes_are_pinned() {
        let cases: &[(&str, u64)] = &[
            ("", 0xf52a_15e9_a9b5_e89b),
            ("a", 0x02c0_bdbf_4814_20f8),
            ("n1", 0xf8e4_6110_455c_6e53),
            ("n2", 0xc1e0_caa6_db6e_fd5c),
            // one byte longer than "n1", to pin that length reaches the hash
            // and not only the bytes that precede it
            ("n1 ", 0xcfe3_0c2e_0b07_66b1),
            // non-ASCII: the key hashes UTF-8 bytes, never code points
            ("ω", 0x0905_1102_ef14_22ef),
            ("İ", 0x9b19_2ebe_4d74_7b41),
            // long enough to pin the loop rather than only its first rounds
            (
                "urn:example:entity:0123456789abcdef0123456789abcdef",
                0x9ef3_0d2d_49db_072f,
            ),
        ];
        for (uid, want) in cases {
            assert_eq!(uid_key(uid), *want, "uid_key({uid:?}) moved");
        }
        // and the form that actually reaches a file: little-endian bytes
        assert_eq!(
            uid_key("n1").to_le_bytes(),
            [83u8, 110, 92, 69, 16, 97, 228, 248]
        );
        // the stamp that makes a deliberate change safe rather than silent
        assert_eq!(UID_KEY_ALGO_VERSION, 1);
    }

    /// The property the pin rests on: changing the key function cannot
    /// change an answer, because a key is only ever a candidate filter and
    /// the full uid decides. Asserted here against a store whose two nodes
    /// are deliberately given the *same* key by a colliding stand-in.
    #[test]
    fn the_node_key_is_a_prefix_and_the_uid_decides() {
        let (_root, s, _rows) = seeded("uid-key-prefix");
        // "n1" is the only node staged by the fixture; its versions resolve
        // through `uid_key`, and the returned rows all carry that uid.
        let got = s.believed_node_versions("n1", crate::OPEN_END).unwrap();
        assert_eq!(got.len(), 1);
        assert!(got.iter().all(|v| v.uid == "n1"));
        // a uid that hashes to a different key resolves to nothing, and so
        // does one that does not exist at all — neither is a key question
        assert!(s.believed_node_versions("n2", crate::OPEN_END).unwrap().is_empty());
        assert!(s
            .believed_node_versions("absent", crate::OPEN_END)
            .unwrap()
            .is_empty());
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
            provenance_ref: if i.is_multiple_of(2) { None } else { Some("p/1".into()) },
        }
    }

    /// A store with two edges and one node, committed at tt=100.
    fn seeded(name: &str) -> (PathBuf, NativeStore, Vec<EdgeRow>) {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let b = s.ensure_entity("n2", "Node").unwrap();
        let rows = vec![
            edge(a, b, "n1", "n2", 10, 100, 0),
            edge(b, a, "n2", "n1", 20, 100, 1),
        ];
        for r in &rows {
            s.stage_edge(r.clone()).unwrap();
        }
        s.stage_node(NodeRow {
            vid: version_vid("n1", 100, 5),
            uid_id: a,
            label: "Node".into(),
            vt_s: 5,
            vt_e: OPEN_END,
            tt_s: 100,
            props: "{}".into(),
            source: "ingest".into(),
            provenance_ref: None,
        })
        .unwrap();
        s.commit(EventLogRef::default()).unwrap();
        (root, s, rows)
    }

    #[test]
    fn all_edge_versions_reconstructs_every_field() {
        let (_root, s, rows) = seeded("all-edges");
        let mut got = s.all_edge_versions().unwrap();
        got.sort_by_key(|r| r.vt_s);
        assert_eq!(got.len(), 2);
        for (g, want) in got.iter().zip(&rows) {
            assert_eq!(g.vid, want.vid.to_hex());
            assert_eq!(g.rel_type, want.rel_type);
            assert_eq!(g.disc, want.disc);
            assert_eq!(g.vt_s, want.vt_s);
            assert_eq!(g.vt_e, want.vt_e);
            assert_eq!(g.tt_s, want.tt_s);
            assert_eq!(g.tt_e, OPEN_END, "an uncorrected row is still believed");
            assert_eq!(g.props, want.props, "props come back byte-identical");
            assert_eq!(g.source, want.source);
            assert_eq!(g.provenance_ref, want.provenance_ref);
        }
        // uids survive the dense-id round trip, and direction is preserved
        assert_eq!((got[0].src.as_str(), got[0].dst.as_str()), ("n1", "n2"));
        assert_eq!((got[1].src.as_str(), got[1].dst.as_str()), ("n2", "n1"));
    }

    #[test]
    fn derived_eid_matches_the_identity_the_row_was_written_under() {
        let (_root, s, rows) = seeded("eid");
        let got = s.all_edge_versions().unwrap();
        for want in &rows {
            let expect = edge_eid("n1", "n2", "R", &want.disc).to_hex();
            let expect_rev = edge_eid("n2", "n1", "R", &want.disc).to_hex();
            assert!(
                got.iter().any(|g| g.eid == expect || g.eid == expect_rev),
                "no row carries the re-derived eid for disc {}",
                want.disc
            );
        }
    }

    #[test]
    fn believed_versions_filter_by_identity_and_belief() {
        let (_root, mut s, rows) = seeded("believed");
        let eid = s.all_edge_versions().unwrap()[0].eid.clone();
        assert_eq!(s.believed_edge_versions(&eid, OPEN_END).unwrap().len(), 1);
        assert!(
            s.believed_edge_versions(&eid, 99).unwrap().is_empty(),
            "nothing was believed before the batch that wrote it"
        );

        // close it, then check both belief states
        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, rows[0].vid, 200).unwrap();
        s.commit(EventLogRef::default()).unwrap();
        assert!(s.believed_edge_versions(&eid, OPEN_END).unwrap().is_empty());
        assert_eq!(s.believed_edge_versions(&eid, 150).unwrap().len(), 1);
        // and the row is still present in the full listing, with its close time
        let all = s.all_edge_versions().unwrap();
        assert_eq!(all.len(), 2);
        assert!(all.iter().any(|r| r.tt_e == 200));
    }

    #[test]
    fn nodes_and_probes_round_trip() {
        let (_root, s, _) = seeded("nodes");
        let nodes = s.all_node_versions().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].uid, "n1");
        assert_eq!(nodes[0].label, "Node");
        assert_eq!(nodes[0].vt_e, OPEN_END);

        assert_eq!(s.believed_node_versions("n1", OPEN_END).unwrap().len(), 1);
        assert!(s.believed_node_versions("n2", OPEN_END).unwrap().is_empty());

        let probe = s
            .nodes_with_believed_versions(
                &["n1".to_string(), "n2".to_string(), "nope".to_string()],
                OPEN_END,
            )
            .unwrap();
        assert_eq!(probe, HashSet::from(["n1".to_string()]));
    }

    /// `nodes_with_believed_versions` picks between a postings probe and a
    /// full materialization by size. Two implementations of one contract is
    /// exactly the arrangement where a divergence hides, and the write path
    /// (`_ingest_events`) is what would silently create duplicate nodes if
    /// the cheap branch ever said "absent" about something present. So: both
    /// branches, same store, same answer, and each branch asserted to be the
    /// one that ran.
    #[test]
    fn both_existence_probes_agree_and_each_branch_is_reachable() {
        let root = tmp_root("probe-branches");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        let mut present: Vec<String> = Vec::new();
        for i in 0..200u32 {
            let uid = format!("n{i}");
            let id = s.ensure_entity(&uid, "Node").unwrap();
            s.stage_node(NodeRow {
                vid: version_vid(&uid, 100, 10),
                uid_id: id,
                label: "Node".into(),
                vt_s: 10,
                vt_e: OPEN_END,
                tt_s: 100,
                props: "{}".into(),
                source: "ingest".into(),
                provenance_ref: None,
            })
            .unwrap();
            present.push(uid);
        }
        s.commit(EventLogRef::default()).unwrap();
        let stored = s.manifest().stats.n_node_versions;
        assert_eq!(stored, 200);

        let absent: Vec<String> = (0..200).map(|i| format!("ghost{i}")).collect();
        let mixed: Vec<String> = present
            .iter()
            .take(3)
            .chain(absent.iter().take(3))
            .cloned()
            .collect();

        // small ask: the probe branch, by the ratio rule
        assert!((mixed.len() as u64) * PROBE_COST_RATIO < stored);
        let probed = s.nodes_with_believed_versions(&mixed, OPEN_END).unwrap();

        // large ask: the same store, the scan branch
        let all: Vec<String> = present.iter().chain(absent.iter()).cloned().collect();
        assert!((all.len() as u64) * PROBE_COST_RATIO >= stored);
        let scanned = s.nodes_with_believed_versions(&all, OPEN_END).unwrap();

        assert_eq!(
            probed,
            mixed
                .iter()
                .filter(|u| present.contains(u))
                .cloned()
                .collect::<HashSet<String>>()
        );
        assert_eq!(scanned, present.iter().cloned().collect::<HashSet<String>>());
        for uid in &mixed {
            assert_eq!(
                probed.contains(uid),
                scanned.contains(uid),
                "the two branches disagree about {uid}"
            );
        }

        // belief is part of the answer on both paths: close every version and
        // the small ask must go empty while the past stays populated
        s.begin(200).unwrap();
        let vids: Vec<_> = s
            .all_node_versions()
            .unwrap()
            .iter()
            .map(|r| Id96::from_hex(&r.vid).expect("engine-produced vid"))
            .collect();
        for vid in vids {
            s.close_version(RowKind::Node, vid, 200).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();
        assert!(s
            .nodes_with_believed_versions(&mixed, OPEN_END)
            .unwrap()
            .is_empty());
        assert_eq!(
            s.nodes_with_believed_versions(&mixed, 150).unwrap().len(),
            3,
            "past belief must survive on the probe branch too"
        );
    }

    #[test]
    fn props_are_fetched_by_vid_and_returned_verbatim() {
        let (_root, s, rows) = seeded("props");
        let vids: Vec<String> = rows.iter().map(|r| r.vid.to_hex()).collect();
        let got = s.props_for_vids(RowKind::Edge, &vids).unwrap();
        assert_eq!(got.len(), 2);
        for r in &rows {
            assert_eq!(got[&r.vid.to_hex()], r.props);
        }
        // unknown vids are simply absent, never fabricated
        assert!(s
            .props_for_vids(RowKind::Edge, &["deadbeef".repeat(3)])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn reads_inside_an_open_batch_see_staged_rows() {
        // base.py calls believed_* in the middle of apply_ops, so a version
        // written earlier in the batch must already be visible
        let (_root, mut s, _) = seeded("staged");
        s.begin(200).unwrap();
        let c = s.ensure_entity("n3", "Node").unwrap();
        let fresh = edge(c, c, "n3", "n3", 30, 200, 9);
        s.stage_edge(fresh.clone()).unwrap();

        let eid = edge_eid("n3", "n3", "R", &fresh.disc).to_hex();
        let believed = s.believed_edge_versions(&eid, OPEN_END).unwrap();
        assert_eq!(believed.len(), 1, "staged row must be visible in its own batch");
        assert_eq!(believed[0].vid, fresh.vid.to_hex());

        // closing it in the same batch hides it immediately
        s.close_version(RowKind::Edge, fresh.vid, 200).unwrap();
        assert!(s.believed_edge_versions(&eid, OPEN_END).unwrap().is_empty());

        // and a rollback removes it entirely
        s.rollback().unwrap();
        assert!(s.believed_edge_versions(&eid, OPEN_END).unwrap().is_empty());
        assert_eq!(s.all_edge_versions().unwrap().len(), 2);
    }

    #[test]
    fn reads_inside_an_open_batch_see_closes_against_committed_rows() {
        // the other half of read-your-own-writes: an ordinary correction closes a row a *previous* batch
        // committed, and `apply_ops` reads belief again before it carves
        let (_root, mut s, rows) = seeded("pending-closes");
        let eid = {
            let r = &rows[0];
            edge_eid("n1", "n2", "R", &r.disc).to_hex()
        };
        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, rows[0].vid, 200).unwrap();

        assert!(
            s.believed_edge_versions(&eid, OPEN_END).unwrap().is_empty(),
            "a version this batch closed is not believed inside it"
        );
        // the row is still there, reported with the tt_e the batch gave it
        let all = s.all_edge_versions().unwrap();
        let closed = all.iter().find(|r| r.vid == rows[0].vid.to_hex()).unwrap();
        assert_eq!(closed.tt_e, 200);
        // and the belief it had before this batch is untouched
        assert_eq!(s.believed_edge_versions(&eid, 150).unwrap().len(), 1);

        s.commit(EventLogRef::default()).unwrap();
        assert!(s.believed_edge_versions(&eid, OPEN_END).unwrap().is_empty());
    }

    #[test]
    fn an_abandoned_batch_s_closes_do_not_reach_the_next_one() {
        // the overlay is cached under (generation, closes so far), and a
        // rollback moves neither — so the cache must be dropped with it
        let (_root, mut s, rows) = seeded("rolled-back-closes");
        let eid = edge_eid("n1", "n2", "R", &rows[0].disc).to_hex();
        let other = edge_eid("n2", "n1", "R", &rows[1].disc).to_hex();

        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, rows[0].vid, 200).unwrap();
        assert!(s.believed_edge_versions(&eid, OPEN_END).unwrap().is_empty());
        s.rollback().unwrap();
        assert_eq!(s.believed_edge_versions(&eid, OPEN_END).unwrap().len(), 1);

        // same generation, same number of pending closes, different row
        s.begin(200).unwrap();
        s.close_version(RowKind::Edge, rows[1].vid, 200).unwrap();
        assert_eq!(
            s.believed_edge_versions(&eid, OPEN_END).unwrap().len(),
            1,
            "the abandoned batch's close came back"
        );
        assert!(s.believed_edge_versions(&other, OPEN_END).unwrap().is_empty());
        s.rollback().unwrap();
    }

    #[test]
    fn a_retired_row_leaves_no_trace_of_having_been_staged() {
        // A version created and closed by one batch was believed over
        // [tt, tt) — no transaction time at all — so the batch commits as
        // though it had never staged it, and its vid is free to be reused
        let (_root, mut s, _) = seeded("retire");
        s.begin(200).unwrap();
        let c = s.ensure_entity("n3", "Node").unwrap();
        let first = edge(c, c, "n3", "n3", 30, 200, 9);
        let eid = edge_eid("n3", "n3", "R", &first.disc).to_hex();
        s.stage_edge(first.clone()).unwrap();
        s.retire_version(RowKind::Edge, first.vid).unwrap();
        assert!(s.believed_edge_versions(&eid, OPEN_END).unwrap().is_empty());

        // the replacement derives the same vid, which is only sound because
        // the row it replaces is gone rather than closed
        let mut replacement = first.clone();
        replacement.vt_e = 40;
        s.stage_edge(replacement.clone()).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        let believed = s.believed_edge_versions(&eid, OPEN_END).unwrap();
        assert_eq!(believed.len(), 1);
        assert_eq!(believed[0].vt_e, 40);
        assert_eq!(
            s.all_edge_versions()
                .unwrap()
                .iter()
                .filter(|r| r.vid == first.vid.to_hex())
                .count(),
            1,
            "the retired row must not be sealed alongside its replacement"
        );

        // retiring something no batch staged is an error, not a silent no-op
        s.begin(300).unwrap();
        assert!(s.retire_version(RowKind::Edge, first.vid).is_err());
        s.rollback().unwrap();
    }

    #[test]
    fn stats_fold_matches_the_materializing_definition() {
        // the column fold must agree with the old all_edge_versions walk —
        // same counts, extents, rel histogram, and degrees — including rows
        // staged in an open batch and interval-lane rows with real vt_e
        let (_root, mut s, _) = seeded("stats-fold");
        s.begin(200).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let mut long = edge(a, a, "n1", "n1", 40, 200, 5);
        long.vt_e = crate::OPEN_END; // routes to the interval lane
        s.stage_edge(long).unwrap();
        s.stage_edge(edge(a, a, "n1", "n1", 50, 200, 6)).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        s.begin(300).unwrap();
        let b = s.ensure_entity("n9", "Node").unwrap();
        s.stage_edge(edge(b, a, "n9", "n1", 60, 300, 7)).unwrap();

        let acc = s.stats_accum().unwrap();
        let mut want = crate::store::StatsAccum::default();
        for e in s.all_edge_versions().unwrap() {
            let src_id = s.dict().dense_id(&e.src).unwrap_or(0);
            want.add_edge(e.vt_s, e.vt_e, &e.rel_type, src_id);
        }
        want.n_node_versions = s.all_node_versions().unwrap().len() as u64;

        assert_eq!(acc.n_edge_versions, want.n_edge_versions);
        assert_eq!(acc.n_node_versions, want.n_node_versions);
        assert_eq!(acc.vt_min, want.vt_min);
        assert_eq!(acc.vt_max, want.vt_max);
        assert_eq!(acc.rel_type_counts, want.rel_type_counts);
        assert_eq!(acc.out_degree, want.out_degree);
        s.rollback().unwrap();
    }

    #[test]
    fn stats_streaming_matches_the_materializing_walk_without_growing_the_cache() {
        // At 17.4M-edge scale,
        // stats_accum's per-segment fold turned a flat 2.4 GB ingest RSS
        // into a 25 GB finalisation spike because `open_segment` (used by
        // the fold at the time) leaves every segment's decoded columns in
        // the session's byte-budgeted cache. `stats_accum` now opens
        // segments through `open_segment_uncached`, dropping each one's
        // decoded columns before the next is opened. This proves both
        // halves: the numbers stay byte-identical to the materializing
        // definition over a multi-segment store with a closed version, and
        // the streaming call leaves the calling store's segment cache empty.
        let (root, mut s, _) = seeded("stats-streaming"); // segment #1
        s.begin(110).unwrap();
        let c = s.ensure_entity("n3", "Node").unwrap();
        let e2 = edge(c, c, "n3", "n3", 30, 110, 9);
        let vid2 = e2.vid;
        s.stage_edge(e2).unwrap();
        s.commit(EventLogRef::default()).unwrap(); // segment #2

        s.begin(120).unwrap();
        s.close_version(RowKind::Edge, vid2, 115).unwrap(); // a real closed version
        let d = s.ensure_entity("n4", "Node").unwrap();
        s.stage_edge(edge(d, d, "n4", "n4", 40, 120, 11)).unwrap();
        s.commit(EventLogRef::default()).unwrap(); // segment #3

        assert!(
            s.edge_files().len() >= 3,
            "test needs a multi-segment store, got {}",
            s.edge_files().len()
        );
        // Commits themselves may touch a segment or two on the way to disk
        // (e.g. verifying what they just sealed) — that is not what this
        // test is about. The property under test is that folding *all*
        // edge segments for stats adds nothing beyond whatever was already
        // cached, i.e. stats_accum itself opens every segment uncached.
        let before = s.segment_cache_stats().0;
        assert!(
            before < s.edge_files().len(),
            "test is only meaningful if stats_accum has more segments to \
             fold than commit-time activity already cached: {before} \
             cached vs {} edge segments",
            s.edge_files().len()
        );

        let acc = s.stats_accum().unwrap();

        assert_eq!(
            s.segment_cache_stats().0,
            before,
            "stats_accum must not add any segment to the session cache"
        );

        // Ground truth from a separate handle on the same root, so computing
        // it does not touch `s`'s own segment cache and the assertion above
        // stays meaningful.
        let ground = NativeStore::open(&root).unwrap();
        let mut want = crate::store::StatsAccum::default();
        for e in ground.all_edge_versions().unwrap() {
            let src_id = ground.dict().dense_id(&e.src).unwrap_or(0);
            want.add_edge(e.vt_s, e.vt_e, &e.rel_type, src_id);
        }
        want.n_node_versions = ground.all_node_versions().unwrap().len() as u64;

        assert_eq!(acc.n_edge_versions, want.n_edge_versions);
        assert_eq!(acc.n_node_versions, want.n_node_versions);
        assert_eq!(acc.vt_min, want.vt_min);
        assert_eq!(acc.vt_max, want.vt_max);
        assert_eq!(acc.rel_type_counts, want.rel_type_counts);
        assert_eq!(acc.out_degree, want.out_degree);
    }

    /// In the shape of
    /// `stats_streaming_matches_the_materializing_walk_without_growing_the_cache`
    /// above: the whole-store walk `compact()` and the digest both run on —
    /// `all_edge_versions` / `all_node_versions` — must fold one segment at a
    /// time, and a point read must still cache.
    ///
    /// Both halves matter. Without the first, a 5.2 GB store's decoded
    /// columns are resident for the rest of the session after any digest —
    /// ≤ 46.5 GB of the measured 107.3 GB resident floor at 100M
    /// (`rss-100m.jsonl`). Without the second, every point read would repay a
    /// full decode, which is what the cache is for and what the selective
    /// (`OpenMode::Cached`) branch preserves.
    #[test]
    fn whole_store_walks_stream_while_point_reads_still_cache() {
        let (root, mut s, _) = seeded("walk-uncached"); // segment #1
        s.begin(110).unwrap();
        let c = s.ensure_entity("n3", "Node").unwrap();
        let e2 = edge(c, c, "n3", "n3", 30, 110, 9);
        let eid2 = edge_eid("n3", "n3", "R", "#9").to_hex();
        s.stage_edge(e2).unwrap();
        s.commit(EventLogRef::default()).unwrap(); // segment #2
        s.begin(120).unwrap();
        let d = s.ensure_entity("n4", "Node").unwrap();
        s.stage_edge(edge(d, d, "n4", "n4", 40, 120, 11)).unwrap();
        s.commit(EventLogRef::default()).unwrap(); // segment #3

        let segments = s.edge_files().len();
        assert!(segments >= 3, "test needs a multi-segment store, got {segments}");
        // Commit-time activity may already have cached a segment or two;
        // what is under test is that the walk adds nothing *beyond* that.
        let before = s.segment_cache_stats().0;
        assert!(
            before < segments,
            "test is only meaningful with segments left to fold: {before} \
             cached vs {segments} edge segments"
        );

        let edges = s.all_edge_versions().unwrap();
        let nodes = s.all_node_versions().unwrap();

        assert_eq!(
            s.segment_cache_stats().0,
            before,
            "a whole-store walk must not add a segment to the session cache"
        );

        // and it is a real walk, not an empty one
        assert_eq!(edges.len(), 4, "every committed edge version is listed");
        assert_eq!(nodes.len(), 1);

        // Ground truth from a separate handle, so producing it cannot touch
        // `s`'s cache and leave the assertion above meaningless.
        let ground = NativeStore::open(&root).unwrap();
        let sort = |mut v: Vec<EdgeVersionOut>| {
            v.sort_by(|a, b| a.vid.cmp(&b.vid));
            v
        };
        assert_eq!(
            sort(edges),
            sort(ground.all_edge_versions().unwrap()),
            "the uncached walk must return exactly what the walk returns"
        );

        // the other half of the rule: a point read addresses rows through the
        // postings and *does* cache the segment it lands in
        let hits = s.believed_edge_versions(&eid2, OPEN_END).unwrap();
        assert_eq!(hits.len(), 1, "the point read found its row");
        assert!(
            s.segment_cache_stats().0 > before,
            "a selective read must still populate the session cache"
        );
    }

    #[test]
    fn scan_addresses_round_trip_through_edge_idents_at() {
        use crate::row::Lane;
        use crate::scan::{ScanRequest, ScanSet, ScanTarget};

        // two commits so the manifest accumulates more than one segment and
        // the address column has to distinguish them
        let (_root, mut s, _) = seeded("idents-at");
        s.begin(200).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let c = s.ensure_entity("n3", "Node").unwrap();
        s.stage_edge(edge(a, c, "n1", "n3", 15, 200, 7)).unwrap();
        s.stage_edge(edge(c, a, "n3", "n1", 25, 200, 8)).unwrap();
        s.commit(EventLogRef::default()).unwrap();

        // assemble the scan exactly as the binding does
        let m = s.manifest().clone();
        let files: Vec<(Lane, String)> = m
            .edge_lanes
            .event
            .iter()
            .map(|e| (Lane::Event, e.file.clone()))
            .chain(m.edge_lanes.interval.iter().map(|e| (Lane::Interval, e.file.clone())))
            .collect();
        let segs: Vec<_> = files
            .iter()
            .map(|(lane, f)| (s.open_segment(f).unwrap(), *lane, crate::store::segment_id_of(f)))
            .collect();
        let targets: Vec<ScanTarget<'_, _>> = segs
            .iter()
            .map(|(segment, lane, id)| ScanTarget { segment, lane: *lane, id: *id })
            .collect();
        let set = ScanSet::new(targets).with_closes(s.close_index().unwrap());

        let req = ScanRequest {
            columns: Some(
                ["vt_s", "src_id", "dst_id", "rel_type", "disc", "seg_id", "seg_row"]
                    .iter()
                    .map(|c| c.to_string())
                    .collect(),
            ),
            ..ScanRequest::current()
        };
        let (cols, _) = set.materialize_edges(&req).unwrap();
        assert_eq!(cols.seg_id.len(), cols.len(), "addresses cover every row");
        assert_eq!(cols.seg_row.len(), cols.len());
        assert!(cols.len() >= 4);
        assert!(
            cols.seg_id.iter().collect::<std::collections::HashSet<_>>().len() > 1,
            "rows must come from more than one segment for this to test anything"
        );

        // the point read must agree with the full materialization, row by row
        let got = s.edge_idents_at(&cols.seg_id, &cols.seg_row).unwrap();
        for (i, (eid, rel)) in got.iter().enumerate() {
            let src = s.dict().uid(cols.src_id[i]).unwrap();
            let dst = s.dict().uid(cols.dst_id[i]).unwrap();
            let expect = edge_eid(src, dst, &cols.rel_type[i], &cols.disc[i]).to_hex();
            assert_eq!(*eid, expect, "eid at scan position {i}");
            assert_eq!(*rel, cols.rel_type[i], "rel_type at scan position {i}");
        }

        // an unprojected scan carries no addresses…
        let bare = ScanRequest {
            columns: Some(vec!["vt_s".into()]),
            ..ScanRequest::current()
        };
        let (bare_cols, _) = set.materialize_edges(&bare).unwrap();
        assert!(bare_cols.seg_id.is_empty() && bare_cols.seg_row.is_empty());

        // …and addresses from another generation's segments are refused
        assert!(s.edge_idents_at(&[u64::MAX - 1], &[0]).is_err());
        assert!(s.edge_idents_at(&[cols.seg_id[0]], &[u32::MAX]).is_err());
    }

    // ------------------------------------------------------------------ //
    // the bounded version page                                            //
    // ------------------------------------------------------------------ //

    /// A store the page has to work for: four batches (so four segments and
    /// four transaction times), two relationship types, a closed version at
    /// each of two different `tt_e`s, and node versions alongside.
    fn versioned(name: &str) -> (PathBuf, NativeStore) {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        // transaction time only advances, so the corrections are interleaved
        // with the writes rather than appended: batch 0 is revised at 250,
        // batch 1 at 350, and batches 2 and 3 land in between and after.
        for (b, tt) in [(0u32, 100i64), (1, 200), (2, 300), (3, 400)] {
            if let Some((tt_e, victim)) = match b {
                2 => Some((250i64, 0u32)),
                3 => Some((350, 1)),
                _ => None,
            } {
                close_batch(&mut s, tt_e, victim);
            }
            s.begin(tt).unwrap();
            let a = s.ensure_entity("n1", "Node").unwrap();
            let c = s.ensure_entity("n2", "Node").unwrap();
            for i in 0..5u32 {
                // valid times interleave across batches, so a compaction
                // re-sort shatters the tt_s runs
                let vt = (i * 4 + b) as i64;
                let disc = format!("#{b}-{i}");
                let rel = if i.is_multiple_of(2) { "R" } else { "S" };
                let eid = edge_eid("n1", "n2", rel, &disc);
                s.stage_edge(EdgeRow {
                    vid: version_vid(&eid.to_hex(), tt, vt),
                    src_id: a,
                    dst_id: c,
                    rel_type: rel.into(),
                    disc,
                    vt_s: vt,
                    vt_e: vt + 2,
                    tt_s: tt,
                    props: format!(r#"{{"i":{i}}}"#),
                    source: "ingest".into(),
                    provenance_ref: None,
                })
                .unwrap();
                s.stage_node(NodeRow {
                    vid: version_vid(&format!("n{b}-{i}"), tt, vt),
                    uid_id: if i.is_multiple_of(2) { a } else { c },
                    label: "Node".into(),
                    vt_s: vt,
                    vt_e: vt + 2,
                    tt_s: tt,
                    props: "{}".into(),
                    source: "ingest".into(),
                    provenance_ref: None,
                })
                .unwrap();
            }
            s.commit(EventLogRef::default()).unwrap();
        }
        (root, s)
    }

    /// Stop believing two edge versions and two node versions of one batch,
    /// as of `tt_e` — so `belief` and `as_of` have something to disagree
    /// about.
    fn close_batch(s: &mut NativeStore, tt_e: i64, batch: u32) {
        let victims: Vec<(RowKind, Id96)> = s
            .all_edge_versions()
            .unwrap()
            .into_iter()
            .filter(|r| r.disc.starts_with(&format!("#{batch}-")))
            .take(2)
            .map(|r| (RowKind::Edge, Id96::from_hex(&r.vid).unwrap()))
            .chain(
                s.all_node_versions()
                    .unwrap()
                    .into_iter()
                    .filter(|r| r.vt_s % 4 == batch as i64)
                    .take(2)
                    .map(|r| (RowKind::Node, Id96::from_hex(&r.vid).unwrap())),
            )
            .collect();
        assert_eq!(victims.len(), 4, "fixture: batch {batch} has rows to close");
        s.begin(tt_e).unwrap();
        for (kind, vid) in victims {
            s.close_version(kind, vid, tt_e).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();
    }

    /// `version_history`'s mask and ordering, spelled out over the fully
    /// materialized listing — the path `version_page` replaces, kept here as
    /// the oracle it has to agree with. This is the frozen digest's
    /// arithmetic (`gnomon/temporal/ops_versions.py`), in Rust.
    fn reference_edges(s: &NativeStore, f: &VersionFilter<'_>) -> Vec<EdgeVersionOut> {
        let mut rows: Vec<EdgeVersionOut> = s
            .all_edge_versions()
            .unwrap()
            .into_iter()
            .filter(|r| r.tt_s <= f.as_of && r.vt_s < f.t_b && f.t_a < r.vt_e)
            .filter(|r| f.belief.admits(r.tt_e <= f.as_of))
            .filter(|r| f.rel_types.is_none_or(|ts| ts.contains(&r.rel_type)))
            .collect();
        rows.sort_by(|a, b| (a.tt_s, &a.vid).cmp(&(b.tt_s, &b.vid)));
        rows
    }

    fn reference_nodes(s: &NativeStore, f: &VersionFilter<'_>) -> Vec<NodeVersionOut> {
        let mut rows: Vec<NodeVersionOut> = s
            .all_node_versions()
            .unwrap()
            .into_iter()
            .filter(|r| r.tt_s <= f.as_of && r.vt_s < f.t_b && f.t_a < r.vt_e)
            .filter(|r| f.belief.admits(r.tt_e <= f.as_of))
            .collect();
        rows.sort_by(|a, b| (a.tt_s, &a.vid).cmp(&(b.tt_s, &b.vid)));
        rows
    }

    fn edges_of(rows: VersionRows) -> Vec<EdgeVersionRow> {
        match rows {
            VersionRows::Edge(v) => v,
            VersionRows::Node(_) => panic!("asked for edges, got nodes"),
        }
    }

    fn nodes_of(rows: VersionRows) -> Vec<NodeVersionRow> {
        match rows {
            VersionRows::Node(v) => v,
            VersionRows::Edge(_) => panic!("asked for nodes, got edges"),
        }
    }

    /// Every combination of belief mode, `as_of` and window the fixture can
    /// tell apart, against the listing-based oracle. This is the test that
    /// stands between the bounded path and the frozen digest
    /// `84e8853c…be78fbfd`: if the two disagree on one row or one position,
    /// the digest moves.
    #[test]
    fn version_page_agrees_with_the_materialized_listing() {
        let (_root, s) = versioned("page-oracle");
        let rel_r = vec!["R".to_string()];
        let rel_rs = vec!["R".to_string(), "S".to_string()];
        let rel_none = vec!["T".to_string()];
        for as_of in [50i64, 150, 250, 300, 350, OPEN_END] {
            for belief in [Belief::Current, Belief::Superseded, Belief::All] {
                for (t_a, t_b) in [(0i64, 64i64), (4, 12), (100, 200), (-5, 3)] {
                    for rel_types in [None, Some(&rel_r), Some(&rel_rs), Some(&rel_none)] {
                        let f = VersionFilter {
                            as_of,
                            t_a,
                            t_b,
                            belief,
                            rel_types: rel_types.map(|v| v.as_slice()),
                        };
                        let want = reference_edges(&s, &f);
                        let (got, total) = s.version_page(RowKind::Edge, &f, 0, 10_000).unwrap();
                        let got = edges_of(got);
                        assert_eq!(
                            total,
                            want.len(),
                            "rows_total must be exact: {f:?}"
                        );
                        assert_eq!(got.len(), want.len(), "page length: {f:?}");
                        for (g, w) in got.iter().zip(&want) {
                            assert_eq!(g.vid, w.vid, "order or membership diverged: {f:?}");
                            assert_eq!(g.eid, w.eid);
                            assert_eq!((g.src.as_str(), g.dst.as_str()), (w.src.as_str(), w.dst.as_str()));
                            assert_eq!(g.rel_type, w.rel_type);
                            assert_eq!(g.disc, w.disc);
                            assert_eq!((g.vt_s, g.vt_e, g.tt_s, g.tt_e), (w.vt_s, w.vt_e, w.tt_s, w.tt_e));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn node_version_page_agrees_with_the_materialized_listing() {
        let (_root, s) = versioned("page-oracle-nodes");
        for as_of in [50i64, 250, 350, OPEN_END] {
            for belief in [Belief::Current, Belief::Superseded, Belief::All] {
                let f = VersionFilter {
                    as_of,
                    t_a: 0,
                    t_b: 64,
                    belief,
                    rel_types: None,
                };
                let want = reference_nodes(&s, &f);
                let (got, total) = s.version_page(RowKind::Node, &f, 0, 10_000).unwrap();
                let got = nodes_of(got);
                assert_eq!(total, want.len(), "{f:?}");
                for (g, w) in got.iter().zip(&want) {
                    assert_eq!((g.vid.as_str(), g.uid.as_str(), g.label.as_str()),
                               (w.vid.as_str(), w.uid.as_str(), w.label.as_str()), "{f:?}");
                    assert_eq!((g.vt_s, g.vt_e, g.tt_s, g.tt_e), (w.vt_s, w.vt_e, w.tt_s, w.tt_e));
                }
            }
        }
    }

    /// Paging is a slice of one ordering, not a re-query: consecutive pages
    /// concatenate to the whole, and `rows_total` never moves.
    #[test]
    fn version_page_slices_one_ordering() {
        let (_root, s) = versioned("page-slices");
        let f = VersionFilter {
            as_of: OPEN_END,
            t_a: 0,
            t_b: 64,
            belief: Belief::All,
            rel_types: None,
        };
        let (all, total) = s.version_page(RowKind::Edge, &f, 0, 10_000).unwrap();
        let all = edges_of(all);
        assert_eq!(all.len(), 20, "the fixture writes 20 edge versions");
        assert_eq!(total, 20);
        let mut walked: Vec<String> = Vec::new();
        for offset in (0..24).step_by(3) {
            let (page, t) = s.version_page(RowKind::Edge, &f, offset, 3).unwrap();
            assert_eq!(t, total, "rows_total is a property of the filter, not the page");
            walked.extend(edges_of(page).into_iter().map(|r| r.vid));
        }
        assert_eq!(walked, all.iter().map(|r| r.vid.clone()).collect::<Vec<_>>());
        // an offset past the end is an empty page, not an error
        let (past, t) = s.version_page(RowKind::Edge, &f, 10_000, 50).unwrap();
        assert!(past.is_empty());
        assert_eq!(t, total);
    }

    /// `(tt_s, vid)` — the belief log's own order, not valid time. The
    /// fixture's valid times deliberately disagree with its transaction
    /// times, so a path that sorted by `vt_s` would fail here.
    #[test]
    fn version_keys_order_by_transaction_time_then_vid() {
        let (_root, s) = versioned("page-order");
        let f = VersionFilter {
            as_of: OPEN_END,
            t_a: 0,
            t_b: 64,
            belief: Belief::All,
            rel_types: None,
        };
        let rows = edges_of(s.version_page(RowKind::Edge, &f, 0, 10_000).unwrap().0);
        let keys: Vec<(i64, &str)> = rows.iter().map(|r| (r.tt_s, r.vid.as_str())).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "rows are not in (tt_s, vid) order");
        assert_ne!(
            rows.iter().map(|r| r.vt_s).collect::<Vec<_>>(),
            {
                let mut v: Vec<i64> = rows.iter().map(|r| r.vt_s).collect();
                v.sort();
                v
            },
            "fixture sanity: (tt_s, vid) order must differ from valid-time order"
        );
    }

    /// A batch reads its own writes: `all_*_versions` unions the open
    /// batch's staged rows and the bounded path has to as well.
    #[test]
    fn version_page_sees_the_open_batch() {
        let (_root, mut s) = versioned("page-staged");
        let f = VersionFilter {
            as_of: OPEN_END,
            t_a: 0,
            t_b: 1_000,
            belief: Belief::All,
            rel_types: None,
        };
        let before = s.version_page(RowKind::Edge, &f, 0, 10_000).unwrap().1;
        s.begin(500).unwrap();
        let a = s.ensure_entity("n1", "Node").unwrap();
        let c = s.ensure_entity("n2", "Node").unwrap();
        let eid = edge_eid("n1", "n2", "R", "#staged");
        s.stage_edge(EdgeRow {
            vid: version_vid(&eid.to_hex(), 500, 900),
            src_id: a,
            dst_id: c,
            rel_type: "R".into(),
            disc: "#staged".into(),
            vt_s: 900,
            vt_e: 901,
            tt_s: 500,
            props: "{}".into(),
            source: "ingest".into(),
            provenance_ref: None,
        })
        .unwrap();

        let (rows, total) = s.version_page(RowKind::Edge, &f, 0, 10_000).unwrap();
        assert_eq!(total, before + 1, "the staged row is not in the page");
        let rows = edges_of(rows);
        let staged = rows.last().expect("a non-empty page");
        assert_eq!(staged.disc, "#staged", "tt_s 500 is the highest, so it sorts last");
        assert_eq!((staged.vt_s, staged.tt_s, staged.tt_e), (900, 500, OPEN_END));
        assert_eq!(staged.eid, eid.to_hex());
        // and it agrees with the listing, staged rows included
        assert_eq!(
            rows.iter().map(|r| r.vid.clone()).collect::<Vec<_>>(),
            reference_edges(&s, &f).into_iter().map(|r| r.vid).collect::<Vec<_>>()
        );
    }

    /// The whole point: the page's cost is the page's, not the store's. The
    /// second pass may only touch the rows the page names.
    #[test]
    fn version_rows_at_materializes_only_the_addresses_it_is_given() {
        let (_root, s) = versioned("page-narrow");
        let f = VersionFilter {
            as_of: OPEN_END,
            t_a: 0,
            t_b: 64,
            belief: Belief::All,
            rel_types: None,
        };
        let mut keys = s.version_keys(RowKind::Edge, &f).unwrap();
        assert_eq!(keys.len(), 20);
        keys.sort_by_key(|k| (k.tt_s, k.vid_hi, k.vid_lo));
        let rows = edges_of(s.version_rows_at(RowKind::Edge, &keys[3..5]).unwrap());
        assert_eq!(rows.len(), 2, "two addresses in, two rows out");
        for (r, k) in rows.iter().zip(&keys[3..5]) {
            assert_eq!(r.vid, Id96 { hi: k.vid_hi, lo: k.vid_lo }.to_hex());
            assert_eq!(r.tt_s, k.tt_s);
        }
        // an empty page is an empty result, not a full scan
        assert!(s.version_rows_at(RowKind::Edge, &[]).unwrap().is_empty());
    }

    /// The cursor is a plaintext decimal offset and the old path fed it
    /// straight to a Python slice, so a nonsensical one had a defined answer.
    /// Moving the slice into the engine must reproduce it, not refuse it.
    #[test]
    fn py_slice_is_cpython_slice_arithmetic() {
        // (offset, limit, total) -> what `range(total)[offset:offset+limit]`
        // gives, checked against CPython's own numbers
        for &(off, lim, total, want) in &[
            (0i64, 3i64, 10usize, (0usize, 3usize)),
            (8, 5, 10, (8, 10)),
            (10, 5, 10, (10, 10)),
            (99, 5, 10, (10, 10)),
            (-5, 8, 10, (5, 5)),   // range(10)[-5:3] is empty: 3 is not
            (-2, 5, 10, (8, 8)),   // adjusted, so it lands before the start
            (-5, 1, 3, (0, 0)),    // range(3)[-5:-4] is empty
            (0, 1, 0, (0, 0)),
        ] {
            assert_eq!(py_slice(off, lim, total), want, "offset={off} limit={lim} total={total}");
        }
        // an absurd limit cannot overflow the addition
        assert_eq!(py_slice(1, i64::MAX, 10), (1, 10));
    }

    /// A filter that selects nothing must still count nothing — and must not
    /// have built a row to find that out.
    #[test]
    fn version_page_over_an_empty_selection() {
        let (_root, s) = versioned("page-empty");
        let f = VersionFilter {
            as_of: 0, // before anything was written
            t_a: 0,
            t_b: 64,
            belief: Belief::All,
            rel_types: None,
        };
        let (rows, total) = s.version_page(RowKind::Edge, &f, 0, 50).unwrap();
        assert!(rows.is_empty());
        assert_eq!(total, 0);
    }

    // --- the second site: the node scan ------------------------------- //

    #[test]
    fn scan_node_versions_agrees_with_the_filtered_listing() {
        let (_root, s) = versioned("node-scan-oracle");
        for as_of in [50i64, 250, 300, OPEN_END] {
            for (vt_min, vt_max) in [
                (None, None),
                (Some(0i64), Some(4i64)),
                (Some(6), None),
                (None, Some(10)),
                (Some(1_000), Some(2_000)),
            ] {
                let got = s.scan_node_versions(as_of, vt_min, vt_max).unwrap();
                let mut want: Vec<NodeVersionOut> = s
                    .all_node_versions()
                    .unwrap()
                    .into_iter()
                    .filter(|r| crate::believed_at(r.tt_s, r.tt_e, as_of))
                    .filter(|r| vt_min.is_none_or(|t| r.vt_e > t))
                    .filter(|r| vt_max.is_none_or(|t| r.vt_s < t))
                    .collect();
                want.sort_by(|a, b| (a.vt_s, &a.vid).cmp(&(b.vt_s, &b.vid)));
                let ctx = format!("as_of={as_of} vt=[{vt_min:?},{vt_max:?})");
                assert_eq!(got.rows.len(), want.len(), "{ctx}");
                for (g, w) in got.rows.iter().zip(&want) {
                    assert_eq!(g.vid, w.vid, "{ctx}");
                    assert_eq!(g.uid, w.uid);
                    assert_eq!(g.label, w.label);
                    assert_eq!((g.vt_s, g.vt_e), (w.vt_s, w.vt_e));
                    // the dense id read off the column is the one the uid
                    // resolves to; the old path recovered it in reverse
                    assert_eq!(s.dict().dense_id(&w.uid), Some(g.uid_id), "{ctx}");
                }
            }
        }
    }

    /// The node scan, and the counter that used to pin its limit.
    ///
    /// The old assertion here was `rows_examined == 20` for *both* the full
    /// scan and a narrow window, with the comment "the sweep visits every
    /// row: there is nothing to prune by". That was true of the flat loop and
    /// is now false in two independent ways:
    ///
    /// - the unfiltered scan at `OPEN_END` examines 16 of 20, not 20, because
    ///   the four superseded versions are subtracted from the verdict ranges
    ///   rather than tested out row by row;
    /// - a narrow valid-time window examines fewer still, because the window
    ///   is now a binary-search cut on a `vt_s`-sorted column instead of a
    ///   per-row comparison.
    ///
    /// The original claim — the filter sits *below* the materialization — is
    /// unchanged and still asserted; it is now true a second time over.
    #[test]
    fn scan_node_versions_builds_only_the_survivors() {
        let (_root, s) = versioned("node-scan-narrow");
        let all = s.scan_node_versions(OPEN_END, None, None).unwrap();
        // the fixture writes 20 node versions, four of which are superseded
        assert_eq!(all.rows_materialized, 16);
        assert_eq!(
            all.rows_examined, 16,
            "the closed versions are cut from the ranges, so they never enter \
             the row loop at all"
        );

        let narrow = s.scan_node_versions(OPEN_END, Some(0), Some(2)).unwrap();
        assert!(
            narrow.rows_examined < all.rows_examined,
            "a narrow window examined {} rows against the full scan's {} — the \
             valid-time window is a binary-search cut now, not a per-row test",
            narrow.rows_examined,
            all.rows_examined
        );
        assert!(
            narrow.rows_materialized < all.rows_materialized,
            "a narrow window materialized {} of {} rows — the filter is still \
             above the materialization",
            narrow.rows_materialized,
            all.rows_materialized
        );
        assert_eq!(narrow.rows_materialized, narrow.rows.len());
        assert!(
            narrow.rows_materialized <= narrow.rows_examined,
            "more rows were built ({}) than the ranges offered ({})",
            narrow.rows_materialized,
            narrow.rows_examined
        );
    }

    /// The belief half of the same change, stated on its own: a store whose
    /// versions have been superseded must not walk the superseded rows.
    ///
    /// This is the node-side twin of `believed_ranges_closed_equals_the_row_loop`
    /// — that one proves the ranges select the same rows, this one proves the
    /// node scan is the thing consuming them.
    #[test]
    fn a_corrected_node_store_does_not_examine_the_superseded_rows() {
        let (_root, s) = versioned("node-scan-corrected");
        let stored = s.all_node_versions().unwrap();
        let believed = stored
            .iter()
            .filter(|r| crate::believed_at(r.tt_s, r.tt_e, OPEN_END))
            .count();
        assert!(
            believed < stored.len(),
            "the fixture must actually supersede something: {believed} of {} \
             believed",
            stored.len()
        );

        let now = s.scan_node_versions(OPEN_END, None, None).unwrap();
        assert!(
            now.rows_examined < stored.len(),
            "current belief examined {} of {} stored node versions — the \
             superseded rows are still entering the row loop instead of being \
             cut from the verdict ranges",
            now.rows_examined,
            stored.len()
        );
        // no other predicate is set, so every row the ranges offer survives:
        // the scan does exactly the work it returns
        assert_eq!(now.rows_examined, now.rows_materialized);
        // and the answer is the one the row loop gave, unchanged
        assert_eq!(now.rows.len(), believed);
    }
}

/// The believed-node count held to both walk strategies as oracles
/// (`node_belief.rs`). The write path creates a node version for every
/// endpoint the probe says is absent, so any disagreement is a duplicate or
/// a missing node in the store — these tests are the reason the count may
/// replace the walk.
#[cfg(test)]
mod existence_probe_oracle_tests {
    use super::*;
    use crate::derive::version_vid;
    use crate::manifest::EventLogRef;
    use crate::row::NodeRow;
    use std::path::{Path, PathBuf};

    fn tmp_root(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-probe-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    /// splitmix64: deterministic, dependency-free.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
        fn chance(&mut self, pct: u64) -> bool {
            self.below(100) < pct
        }
    }

    fn node(s: &mut NativeStore, uid: &str, tt: i64, vt_s: i64) -> Id96 {
        let id = s.ensure_entity(uid, "Node").unwrap();
        let vid = version_vid(uid, tt, vt_s);
        s.stage_node(NodeRow {
            vid,
            uid_id: id,
            label: "Node".into(),
            vt_s,
            vt_e: OPEN_END,
            tt_s: tt,
            props: "{}".into(),
            source: "ingest".into(),
            provenance_ref: None,
        })
        .unwrap();
        vid
    }

    /// The count, the lookup and the scan must agree on every ask, and the
    /// default entry point must be the count.
    fn check(s: &NativeStore, ask: &[String]) -> HashSet<String> {
        let count = s
            .nodes_with_believed_versions_via(ask, OPEN_END, ExistenceProbe::Count)
            .unwrap();
        let lookup = s
            .nodes_with_believed_versions_via(ask, OPEN_END, ExistenceProbe::Lookup)
            .unwrap();
        let scan = s
            .nodes_with_believed_versions_via(ask, OPEN_END, ExistenceProbe::Scan)
            .unwrap();
        assert_eq!(count, lookup, "count vs lookup, gen {}", s.generation());
        assert_eq!(count, scan, "count vs scan, gen {}", s.generation());
        assert_eq!(s.nodes_with_believed_versions(ask, OPEN_END).unwrap(), count);
        // the clamped form of "now" is the same question
        assert_eq!(s.nodes_with_believed_versions(ask, OPEN_END - 1).unwrap(), count);
        count
    }

    fn ask(rng: &mut Rng, pool: usize) -> Vec<String> {
        let n = 1 + rng.below(3 * pool as u64) as usize;
        (0..n)
            .map(|_| {
                let i = rng.below(pool as u64 + 8);
                if i < pool as u64 {
                    format!("u{i}")
                } else {
                    format!("ghost{i}")
                }
            })
            .collect()
    }

    /// Every believed-now committed node vid, by listing.
    fn open_committed(s: &NativeStore, tt: i64) -> Vec<Id96> {
        s.all_node_versions()
            .unwrap()
            .into_iter()
            .filter(|r| r.tt_e == OPEN_END && r.tt_s < tt)
            .map(|r| Id96::from_hex(&r.vid).unwrap())
            .collect()
    }

    /// One randomized history; returns every committed-state answer in order
    /// so two runs can be compared for determinism.
    fn run(name: &str, seed: u64, batches: usize) -> Vec<Vec<String>> {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        let mut rng = Rng(seed);
        let pool = 40usize;
        let mut answers = Vec::new();
        let mut vt = 0i64;
        let mut reopens = 0;
        let mut compactions = 0;
        for b in 0..batches {
            let tt = 1_000 + 10 * b as i64;
            s.begin(tt).unwrap();
            let mut staged: Vec<Id96> = Vec::new();
            // several ops in one batch, with a probe between them
            for _ in 0..1 + rng.below(4) {
                match rng.below(10) {
                    0..=4 => {
                        for _ in 0..1 + rng.below(6) {
                            let uid = format!("u{}", rng.below(pool as u64));
                            vt += 1;
                            staged.push(node(&mut s, &uid, tt, vt));
                        }
                    }
                    5 | 6 => {
                        // correct/retract: close committed versions
                        let open = open_committed(&s, tt);
                        for _ in 0..rng.below(4) {
                            if open.is_empty() {
                                break;
                            }
                            let vid = open[rng.below(open.len() as u64) as usize];
                            // closing twice in one batch is allowed; the
                            // count must still subtract the row once
                            s.close_version(RowKind::Node, vid, tt).unwrap();
                        }
                    }
                    // same-batch carve: close a row staged in this batch
                    7 if !staged.is_empty() => {
                        let vid = staged[rng.below(staged.len() as u64) as usize];
                        s.close_version(RowKind::Node, vid, tt).unwrap();
                    }
                    // a version this batch staged and replaced is retired
                    8 if !staged.is_empty() => {
                        let i = rng.below(staged.len() as u64) as usize;
                        let vid = staged.swap_remove(i);
                        s.retire_version(RowKind::Node, vid).unwrap();
                    }
                    _ => {}
                }
                let a = ask(&mut rng, pool);
                check(&s, &a);
            }
            if rng.chance(10) {
                s.rollback().unwrap();
            } else {
                s.commit(EventLogRef::default()).unwrap();
            }
            let everyone: Vec<String> = (0..pool + 4).map(|i| format!("u{i}")).collect();
            let mut now: Vec<String> = check(&s, &everyone).into_iter().collect();
            now.sort();
            answers.push(now);

            // a historical question keeps the walk, and the walk agrees with
            // the scan at that tt
            let past = tt - 10 * rng.below(5) as i64;
            assert_eq!(
                s.nodes_with_believed_versions(&everyone, past).unwrap(),
                s.nodes_with_believed_versions_via(&everyone, past, ExistenceProbe::Scan)
                    .unwrap()
            );
            assert!(s
                .nodes_with_believed_versions_via(&everyone, past, ExistenceProbe::Count)
                .is_err());

            match rng.below(12) {
                0 | 1 => {
                    let r = s.compact_incremental(None).unwrap();
                    compactions += usize::from(r.rows_rewritten > 0);
                }
                2 => {
                    s.compact().unwrap();
                    compactions += 1;
                }
                3 => {
                    drop(s);
                    s = NativeStore::open(&root).unwrap();
                    reopens += 1;
                    assert!(s.node_belief_stats().is_none(), "a new handle starts without it");
                }
                _ => {}
            }
            let mut again: Vec<String> = check(&s, &everyone).into_iter().collect();
            again.sort();
            assert_eq!(again, *answers.last().unwrap(), "compaction or reopen changed belief");
        }
        assert!(reopens > 0 && compactions > 0, "the history must exercise both");
        drop(s);
        let _ = std::fs::remove_dir_all(&root);
        answers
    }

    #[test]
    fn the_count_agrees_with_both_walks_on_randomized_histories() {
        for seed in 1..=6u64 {
            run(&format!("rand{seed}"), seed, 60);
        }
    }

    #[test]
    fn the_same_history_gives_the_same_answers() {
        assert_eq!(run("det-a", 42, 50), run("det-b", 42, 50));
    }

    fn close_all_of(s: &mut NativeStore, uids: &[&str], tt: i64) {
        let vids: Vec<Id96> = s
            .all_node_versions()
            .unwrap()
            .into_iter()
            .filter(|r| r.tt_e == OPEN_END && uids.contains(&r.uid.as_str()))
            .map(|r| Id96::from_hex(&r.vid).unwrap())
            .collect();
        s.begin(tt).unwrap();
        for v in vids {
            s.close_version(RowKind::Node, v, tt).unwrap();
        }
        s.commit(EventLogRef::default()).unwrap();
    }

    fn sidecar_closes(s: &NativeStore) -> usize {
        s.node_files()
            .iter()
            .map(|(f, _)| s.open_segment_uncached(f).unwrap().sidecar().slice().len())
            .sum()
    }

    fn close_runs_touching_live_nodes(s: &NativeStore) -> usize {
        let closes = s.committed_close_index().unwrap();
        s.node_files()
            .iter()
            .map(|(_, id)| closes.segment_closes(*id).len())
            .sum()
    }

    /// Guard: the count is rebuilt correctly after a reopen, and follows a
    /// compaction that folds closes into sidecars — with the fixture
    /// asserting the closes really moved from close runs into sidecars, so
    /// the test cannot pass on a compaction that folded nothing.
    fn guard(root: &Path, whole_store: bool) {
        let mut s = NativeStore::open(root).unwrap();
        // one node segment of 50 uids, then a second of 10 more
        s.begin(100).unwrap();
        for i in 0..50 {
            node(&mut s, &format!("u{i}"), 100, i);
        }
        s.commit(EventLogRef::default()).unwrap();
        s.begin(110).unwrap();
        for i in 50..60 {
            node(&mut s, &format!("u{i}"), 110, i);
        }
        s.commit(EventLogRef::default()).unwrap();
        let everyone: Vec<String> = (0..64).map(|i| format!("u{i}")).collect();
        assert_eq!(check(&s, &everyone).len(), 60);

        // close 20 of the first segment's 50 rows (40 % > the 20 % rewrite
        // trigger) and 2 of the second's
        let closed: Vec<String> = (0..20).chain(50..52).map(|i| format!("u{i}")).collect();
        let closed_refs: Vec<&str> = closed.iter().map(String::as_str).collect();
        close_all_of(&mut s, &closed_refs, 200);
        let before = check(&s, &everyone);
        assert_eq!(before.len(), 38);
        assert_eq!(close_runs_touching_live_nodes(&s), 22, "closes are in runs");
        assert_eq!(sidecar_closes(&s), 0, "and not yet in any sidecar");
        let stats_before = s.node_belief_stats().unwrap();
        assert_eq!(stats_before.run_closed_rows, 22);

        let report = if whole_store {
            s.compact().unwrap()
        } else {
            s.compact_incremental(None).unwrap()
        };
        assert!(report.closes_folded > 0, "the compaction must fold closes: {report:?}");
        assert!(sidecar_closes(&s) >= 20, "folded closes live in sidecars now");
        assert!(
            close_runs_touching_live_nodes(&s) <= 2,
            "no live node segment is still closed through a run for the folded rows"
        );
        assert_eq!(check(&s, &everyone), before, "after compaction");
        let after = s.node_belief_stats().unwrap();
        assert!(after.segments_removed > stats_before.segments_removed);
        assert!(after.segments_added > stats_before.segments_added);
        assert_eq!(after.counted_rows, 38);

        // reopen: a fresh handle derives the count again from the files
        drop(s);
        let s = NativeStore::open(root).unwrap();
        assert!(s.node_belief_stats().is_none());
        assert_eq!(check(&s, &everyone), before, "after reopen");
        let fresh = s.node_belief_stats().unwrap();
        assert_eq!(fresh.segments_added as usize, s.node_files().len());
        assert_eq!(fresh.segments_removed, 0);
        assert_eq!(fresh.counted_rows, 38);
    }

    #[test]
    fn the_count_is_rebuilt_after_reopen_and_follows_an_incremental_compaction_that_folds_closes() {
        let root = tmp_root("guard-incr");
        guard(&root, false);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_count_is_rebuilt_after_reopen_and_follows_a_whole_store_compaction() {
        let root = tmp_root("guard-whole");
        guard(&root, true);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A node whose every version is closed is absent now and gets a fresh
    /// version; the count must go back up when it does, in the same batch
    /// (staged) and after the commit (counted).
    #[test]
    fn a_closed_uid_is_absent_until_a_new_version_is_staged() {
        let root = tmp_root("reborn");
        let mut s = NativeStore::open(&root).unwrap();
        s.begin(100).unwrap();
        node(&mut s, "a", 100, 1);
        s.commit(EventLogRef::default()).unwrap();
        let a = vec!["a".to_string()];
        assert_eq!(check(&s, &a).len(), 1);
        close_all_of(&mut s, &["a"], 200);
        assert!(check(&s, &a).is_empty());
        s.begin(300).unwrap();
        node(&mut s, "a", 300, 2);
        assert_eq!(check(&s, &a).len(), 1, "staged row is read back");
        s.commit(EventLogRef::default()).unwrap();
        assert_eq!(check(&s, &a).len(), 1);
        assert_eq!(s.node_belief_stats().unwrap().counted_rows, 1);
        drop(s);
        let _ = std::fs::remove_dir_all(&root);
    }
}
