//! A write transaction's per-operation ops (`assert_node`,
//! `assert_edge`, `retract`, `correct`) applied by the engine from the
//! transaction's event-log record.
//!
//! `gnomon/storage/base.py::apply_ops` runs these one at a time in Python: per
//! op a dictionary call, a windowed point read whose answer becomes Python
//! objects (props parsed with `json.loads`), the supersede calls and an
//! insert that serializes everything back. On an SF1 correction transaction
//! at a 10 % correction rate (~250k ops) that was ~5 s of Python and ~4 s of binding
//! conversion out of 11 s.
//!
//! [`NativeStore::apply_ops_raw`] does the same work here, op by op, with the
//! same engine primitives in the same order: `ensure_entity`, the windowed
//! point read (`pointread.rs`), retires then closes (`_supersede_*`), then
//! the staged rows in the order `insert_*_versions` stages them. Rows carry
//! the same vids (`version_vid`), labels, intervals, sources and props; props
//! read back from a version are kept as the canonical text they are stored
//! as (which is what `canonical_json(json.loads(text))` gives back), and props
//! from the record are rendered as `canonical_json` renders them
//! ([`crate::ingest_raw::canonical_props`]).
//!
//! **Three answers.** `Declined`: the record is not in a shape this function
//! reproduces exactly -- an op kind it does not take, a field missing or of
//! another type than Python would use, a `null` Python would carry into a
//! row, a float in props, an empty interval -- found by a full parse *before
//! anything is mutated*; the caller runs the Python path. `Refused`: an op
//! met what the Python path raises on (no believed version to retract or
//! correct, a correct across disagreeing labels, an engine error) after
//! earlier ops of the record were staged; the caller rolls the batch back and
//! runs the Python path, which raises at the same op, because apply is
//! deterministic. `Applied`: every op is staged. `GNOMON_INGEST_RAW=0`
//! ([`crate::designs::Designs::ingest_raw`]) declines every record.

use serde_json::{Map, Value};

use crate::derive::{edge_eid, version_vid, Id96};
use crate::error::Result;
use crate::ingest_phase::{add, Timer, G};
use crate::read::{EdgeVersionOut, NodeVersionOut};
use crate::row::{EdgeRow, NodeRow, RowKind};
use crate::store::NativeStore;
use crate::{clamp_tt, OPEN_END};

/// [`NativeStore::apply_ops_raw`]'s answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyRaw {
    Applied,
    Declined,
    Refused,
}

type M = Map<String, Value>;
type Shape<T> = std::result::Result<T, ()>;

fn opt_str<'a>(m: &'a M, k: &str) -> Shape<Option<&'a str>> {
    match m.get(k) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(()),
    }
}

fn req_str<'a>(m: &'a M, k: &str) -> Shape<&'a str> {
    opt_str(m, k)?.ok_or(())
}

fn opt_int(m: &M, k: &str) -> Shape<Option<i64>> {
    match m.get(k) {
        None => Ok(None),
        Some(Value::Number(n)) => n.as_i64().map(Some).ok_or(()),
        Some(_) => Err(()),
    }
}

fn req_int(m: &M, k: &str) -> Shape<i64> {
    opt_int(m, k)?.ok_or(())
}

fn props_of(v: &Value) -> Shape<String> {
    let mut s = String::new();
    crate::ingest_raw::canonical_props(v, &mut s).ok_or(())?;
    Ok(s)
}

/// `_interval(op["vt_s"], op.get("vt_e", OPEN_END))`, which refuses an
/// empty one.
fn interval(m: &M) -> Shape<(i64, i64)> {
    let lo = req_int(m, "vt_s")?;
    let hi = opt_int(m, "vt_e")?.unwrap_or(OPEN_END);
    if lo < hi {
        Ok((lo, hi))
    } else {
        Err(())
    }
}

/// `op.get("source", "ingest")` and `op.get("provenance_ref")`.
fn provenance(m: &M) -> Shape<(&str, Option<&str>)> {
    let source = opt_str(m, "source")?.unwrap_or("ingest");
    let prov = match m.get("provenance_ref") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.as_str()),
        Some(_) => return Err(()),
    };
    Ok((source, prov))
}

/// `_ref_from_op(op)`, with `EntityRef`'s checks.
enum Ref<'a> {
    Node(&'a str),
    Edge { src: &'a str, dst: &'a str, rel: &'a str, disc: &'a str },
}

fn entity_ref(m: &M) -> Shape<Ref<'_>> {
    let Some(Value::Object(r)) = m.get("ref") else {
        return Err(());
    };
    match req_str(r, "kind")? {
        "node" => {
            let uid = req_str(r, "uid")?;
            if uid.is_empty() {
                return Err(());
            }
            Ok(Ref::Node(uid))
        }
        "edge" => {
            let (src, dst, rel) = (req_str(r, "src")?, req_str(r, "dst")?, req_str(r, "rel_type")?);
            if src.is_empty() || dst.is_empty() || rel.is_empty() {
                return Err(());
            }
            Ok(Ref::Edge { src, dst, rel, disc: opt_str(r, "disc")?.unwrap_or("") })
        }
        _ => Err(()),
    }
}

enum Planned<'a> {
    AssertNode { uid: &'a str, label: &'a str, lo: i64, hi: i64, props: String, source: &'a str, prov: Option<&'a str> },
    AssertEdge {
        src: &'a str, dst: &'a str, rel: &'a str, disc: &'a str, labels: (&'a str, &'a str),
        lo: i64, hi: i64, props: String, source: &'a str, prov: Option<&'a str>,
    },
    Retract { r: Ref<'a>, t: i64 },
    Correct { r: Ref<'a>, lo: i64, hi: i64, props: String, source: &'a str, prov: Option<&'a str> },
    Ingest(usize),
}

fn plan_op(i: usize, v: &Value) -> Shape<Planned<'_>> {
    let Value::Object(m) = v else { return Err(()) };
    Ok(match req_str(m, "op")? {
        "assert_node" => {
            let (lo, hi) = interval(m)?;
            let (source, prov) = provenance(m)?;
            let props = match m.get("props") {
                None => "{}".to_string(),
                Some(p) => props_of(p)?,
            };
            Planned::AssertNode { uid: req_str(m, "uid")?, label: req_str(m, "label")?, lo, hi, props, source, prov }
        }
        "assert_edge" => {
            let (lo, hi) = interval(m)?;
            let (source, prov) = provenance(m)?;
            let props = match m.get("props") {
                None => "{}".to_string(),
                Some(p) => props_of(p)?,
            };
            Planned::AssertEdge {
                src: req_str(m, "src")?, dst: req_str(m, "dst")?, rel: req_str(m, "rel_type")?,
                disc: opt_str(m, "disc")?.unwrap_or(""),
                labels: (opt_str(m, "src_label")?.unwrap_or(""), opt_str(m, "dst_label")?.unwrap_or("")),
                lo, hi, props, source, prov,
            }
        }
        "retract" => {
            let r = entity_ref(m)?;
            let t = req_int(m, "t")?;
            t.checked_add(1).ok_or(())?;
            Planned::Retract { r, t }
        }
        "correct" => {
            let r = entity_ref(m)?;
            let props = props_of(m.get("props").ok_or(())?)?;
            let (lo, hi) = interval(m)?;
            let (source, prov) = provenance(m)?;
            Planned::Correct { r, lo, hi, props, source, prov }
        }
        "ingest_events" => Planned::Ingest(i),
        _ => return Err(()),
    })
}

/// `_remainder(vs, ve, cs, ce)`: the parts of `[vs, ve)` outside `[cs, ce)`.
fn remainder(vs: i64, ve: i64, cs: i64, ce: i64) -> Vec<(i64, i64)> {
    let mut out = Vec::with_capacity(2);
    if vs < cs {
        out.push((vs, ve.min(cs)));
    }
    if ce < ve {
        out.push((vs.max(ce), ve));
    }
    out
}

/// A version read back as Python's `_nodes`/`_edges` hold it: `source or
/// "ingest"`.
fn source_of(s: &str) -> String {
    if s.is_empty() { "ingest".to_string() } else { s.to_string() }
}

/// What a step of the record did.
enum Step {
    Done,
    Refuse,
}

impl NativeStore {
    /// Apply every op of the event-log record `record` at transaction time
    /// `tt`, exactly as `base.py::apply_ops` would (module docs). The batch
    /// must already be open at `tt` and hold nothing else.
    pub fn apply_ops_raw(&mut self, record: &[u8], tt: i64) -> Result<ApplyRaw> {
        if !self.designs().ingest_raw {
            return Ok(ApplyRaw::Declined);
        }
        self.require_batch()?;
        let t = Timer::start();
        let Ok(parsed) = serde_json::from_slice::<Value>(record) else {
            t.stop(G::ApplyRawParseNs);
            add(G::ApplyRawDeclined, 1);
            return Ok(ApplyRaw::Declined);
        };
        let Some(Value::Array(ops)) = parsed.get("ops") else {
            t.stop(G::ApplyRawParseNs);
            add(G::ApplyRawDeclined, 1);
            return Ok(ApplyRaw::Declined);
        };
        let mut plan = Vec::with_capacity(ops.len());
        for (i, op) in ops.iter().enumerate() {
            match plan_op(i, op) {
                Ok(p) => plan.push(p),
                Err(()) => {
                    t.stop(G::ApplyRawParseNs);
                    add(G::ApplyRawDeclined, 1);
                    return Ok(ApplyRaw::Declined);
                }
            }
        }
        t.stop(G::ApplyRawParseNs);
        for p in &plan {
            match self.apply_planned(record, p, tt) {
                Ok(Step::Done) => {}
                Ok(Step::Refuse) | Err(_) => {
                    add(G::ApplyRawRefused, 1);
                    return Ok(ApplyRaw::Refused);
                }
            }
        }
        add(G::ApplyRawApplied, 1);
        Ok(ApplyRaw::Applied)
    }

    fn ensure_timed(&mut self, uid: &str, label: &str) -> Result<u32> {
        let t = Timer::start();
        let r = self.ensure_entity(uid, label);
        t.stop(G::ApplyRawEnsureNs);
        r
    }

    fn stage_nodes_timed(&mut self, rows: Vec<NodeRow>) -> Result<()> {
        let t = Timer::start();
        let r = rows.into_iter().try_for_each(|row| self.stage_node(row));
        t.stop(G::ApplyRawStageNs);
        r
    }

    fn stage_edges_timed(&mut self, rows: Vec<EdgeRow>) -> Result<()> {
        let t = Timer::start();
        let r = rows.into_iter().try_for_each(|row| self.stage_edge(row));
        t.stop(G::ApplyRawStageNs);
        r
    }

    fn dense(&self, uid: &str) -> Result<u32> {
        self.dict()
            .dense_id(uid)
            .ok_or_else(|| crate::error::EngineError::not_found(format!("unknown uid: {uid}")))
    }

    /// `_supersede_*`: retire the versions this batch wrote, then close the
    /// rest, in that order.
    fn supersede(&mut self, kind: RowKind, versions: &[(&str, i64, i64)], tt: i64) -> Result<()> {
        let t = Timer::start();
        let r = self.supersede_untimed(kind, versions, tt);
        t.stop(G::ApplyRawSupersedeNs);
        r
    }

    fn supersede_untimed(&mut self, kind: RowKind, versions: &[(&str, i64, i64)], tt: i64) -> Result<()> {
        for &(vid, tt_s, _) in versions {
            if tt_s == tt {
                self.retire_version(kind, Id96::from_hex(vid)?)?;
            }
        }
        for &(vid, tt_s, vt_s) in versions {
            if tt_s != tt {
                self.close_version_at(kind, Id96::from_hex(vid)?, Some(vt_s), tt)?;
            }
        }
        Ok(())
    }

    fn nodes_over(&self, uid: &str, lo: i64, hi: i64) -> Result<Vec<NodeVersionOut>> {
        let t = Timer::start();
        let r = self.believed_node_versions_overlapping(uid, clamp_tt(OPEN_END), lo, hi);
        t.stop(G::ApplyRawReadNs);
        Ok(r?.into_iter().filter(|v| v.vt_s < hi && lo < v.vt_e).collect())
    }

    fn edges_over(&self, eid: &str, lo: i64, hi: i64) -> Result<Vec<EdgeVersionOut>> {
        let t = Timer::start();
        let r = self.believed_edge_versions_overlapping(eid, clamp_tt(OPEN_END), lo, hi);
        t.stop(G::ApplyRawReadNs);
        Ok(r?.into_iter().filter(|v| v.vt_s < hi && lo < v.vt_e).collect())
    }

    #[allow(clippy::too_many_lines)]
    fn apply_planned(&mut self, record: &[u8], p: &Planned<'_>, tt: i64) -> Result<Step> {
        match *p {
            Planned::Ingest(i) => {
                let t = Timer::start();
                let r = self.stage_ingest_events_raw(record, i, tt);
                t.stop(G::ApplyRawIngestNs);
                if r?.is_none() {
                    return Ok(Step::Refuse);
                }
            }
            Planned::AssertNode { uid, label, lo, hi, ref props, source, prov } => {
                self.ensure_timed(uid, label)?;
                let existing = self.nodes_over(uid, lo, hi)?;
                let uid_id = self.dense(uid)?;
                let mut rows = Vec::new();
                for v in &existing {
                    for (fs, fe) in remainder(v.vt_s, v.vt_e, lo, hi) {
                        rows.push(NodeRow {
                            vid: version_vid(uid, tt, fs), uid_id, label: v.label.clone(),
                            vt_s: fs, vt_e: fe, tt_s: tt, props: v.props.clone(),
                            source: source_of(&v.source), provenance_ref: v.provenance_ref.clone(),
                        });
                    }
                }
                let sup: Vec<(&str, i64, i64)> = existing.iter().map(|v| (v.vid.as_str(), v.tt_s, v.vt_s)).collect();
                self.supersede(RowKind::Node, &sup, tt)?;
                rows.push(NodeRow {
                    vid: version_vid(uid, tt, lo), uid_id, label: label.to_string(), vt_s: lo, vt_e: hi,
                    tt_s: tt, props: props.clone(), source: source.to_string(), provenance_ref: prov.map(str::to_string),
                });
                self.stage_nodes_timed(rows)?;
            }
            Planned::AssertEdge { src, dst, rel, disc, labels, lo, hi, ref props, source, prov } => {
                let eid = edge_eid(src, dst, rel, disc).to_hex();
                // `ensure_entities` keeps the first label of a uid named twice
                self.ensure_timed(src, labels.0)?;
                if dst != src {
                    self.ensure_timed(dst, labels.1)?;
                }
                let existing = self.edges_over(&eid, lo, hi)?;
                let (src_id, dst_id) = (self.dense(src)?, self.dense(dst)?);
                let mut rows = Vec::new();
                for v in &existing {
                    for (fs, fe) in remainder(v.vt_s, v.vt_e, lo, hi) {
                        rows.push(EdgeRow {
                            vid: version_vid(&eid, tt, fs), src_id, dst_id, rel_type: rel.to_string(),
                            disc: disc.to_string(), vt_s: fs, vt_e: fe, tt_s: tt, props: v.props.clone(),
                            source: source_of(&v.source), provenance_ref: v.provenance_ref.clone(),
                        });
                    }
                }
                let sup: Vec<(&str, i64, i64)> = existing.iter().map(|v| (v.vid.as_str(), v.tt_s, v.vt_s)).collect();
                self.supersede(RowKind::Edge, &sup, tt)?;
                rows.push(EdgeRow {
                    vid: version_vid(&eid, tt, lo), src_id, dst_id, rel_type: rel.to_string(),
                    disc: disc.to_string(), vt_s: lo, vt_e: hi, tt_s: tt, props: props.clone(),
                    source: source.to_string(), provenance_ref: prov.map(str::to_string),
                });
                self.stage_edges_timed(rows)?;
            }
            Planned::Retract { ref r, t } => match *r {
                Ref::Node(uid) => {
                    let hits: Vec<NodeVersionOut> = self
                        .nodes_over(uid, t, t + 1)?
                        .into_iter()
                        .filter(|v| v.vt_s <= t && t < v.vt_e)
                        .collect();
                    if hits.is_empty() {
                        return Ok(Step::Refuse);
                    }
                    let sup: Vec<(&str, i64, i64)> = hits.iter().map(|v| (v.vid.as_str(), v.tt_s, v.vt_s)).collect();
                    self.supersede(RowKind::Node, &sup, tt)?;
                    let mut rows = Vec::new();
                    for v in hits.iter().filter(|v| v.vt_s < t) {
                        rows.push(NodeRow {
                            vid: version_vid(&v.uid, tt, v.vt_s), uid_id: self.dense(&v.uid)?,
                            label: v.label.clone(), vt_s: v.vt_s, vt_e: t, tt_s: tt, props: v.props.clone(),
                            source: source_of(&v.source), provenance_ref: v.provenance_ref.clone(),
                        });
                    }
                    self.stage_nodes_timed(rows)?;
                }
                Ref::Edge { src, dst, rel, disc } => {
                    let eid = edge_eid(src, dst, rel, disc).to_hex();
                    let hits: Vec<EdgeVersionOut> = self
                        .edges_over(&eid, t, t + 1)?
                        .into_iter()
                        .filter(|v| v.vt_s <= t && t < v.vt_e)
                        .collect();
                    if hits.is_empty() {
                        return Ok(Step::Refuse);
                    }
                    let sup: Vec<(&str, i64, i64)> = hits.iter().map(|v| (v.vid.as_str(), v.tt_s, v.vt_s)).collect();
                    self.supersede(RowKind::Edge, &sup, tt)?;
                    let mut rows = Vec::new();
                    for v in hits.iter().filter(|v| v.vt_s < t) {
                        rows.push(EdgeRow {
                            vid: version_vid(&v.eid, tt, v.vt_s), src_id: self.dense(&v.src)?,
                            dst_id: self.dense(&v.dst)?, rel_type: v.rel_type.clone(), disc: v.disc.clone(),
                            vt_s: v.vt_s, vt_e: t, tt_s: tt, props: v.props.clone(),
                            source: source_of(&v.source), provenance_ref: v.provenance_ref.clone(),
                        });
                    }
                    self.stage_edges_timed(rows)?;
                }
            },
            Planned::Correct { ref r, lo, hi, ref props, source, prov } => match *r {
                Ref::Node(uid) => {
                    let hits = self.nodes_over(uid, lo, hi)?;
                    if hits.is_empty() || hits.iter().any(|v| v.label != hits[0].label) {
                        return Ok(Step::Refuse);
                    }
                    let sup: Vec<(&str, i64, i64)> = hits.iter().map(|v| (v.vid.as_str(), v.tt_s, v.vt_s)).collect();
                    self.supersede(RowKind::Node, &sup, tt)?;
                    let mut rows = Vec::new();
                    for v in &hits {
                        let uid_id = self.dense(&v.uid)?;
                        for (fs, fe) in remainder(v.vt_s, v.vt_e, lo, hi) {
                            rows.push(NodeRow {
                                vid: version_vid(&v.uid, tt, fs), uid_id, label: v.label.clone(),
                                vt_s: fs, vt_e: fe, tt_s: tt, props: v.props.clone(),
                                source: source_of(&v.source), provenance_ref: v.provenance_ref.clone(),
                            });
                        }
                    }
                    rows.push(NodeRow {
                        vid: version_vid(uid, tt, lo), uid_id: self.dense(uid)?, label: hits[0].label.clone(),
                        vt_s: lo, vt_e: hi, tt_s: tt, props: props.clone(), source: source.to_string(),
                        provenance_ref: prov.map(str::to_string),
                    });
                    self.stage_nodes_timed(rows)?;
                }
                Ref::Edge { src, dst, rel, disc } => {
                    let eid = edge_eid(src, dst, rel, disc).to_hex();
                    let hits = self.edges_over(&eid, lo, hi)?;
                    if hits.is_empty() {
                        return Ok(Step::Refuse);
                    }
                    let sup: Vec<(&str, i64, i64)> = hits.iter().map(|v| (v.vid.as_str(), v.tt_s, v.vt_s)).collect();
                    self.supersede(RowKind::Edge, &sup, tt)?;
                    let mut rows = Vec::new();
                    for v in &hits {
                        let (s, d) = (self.dense(&v.src)?, self.dense(&v.dst)?);
                        for (fs, fe) in remainder(v.vt_s, v.vt_e, lo, hi) {
                            rows.push(EdgeRow {
                                vid: version_vid(&v.eid, tt, fs), src_id: s, dst_id: d, rel_type: v.rel_type.clone(),
                                disc: v.disc.clone(), vt_s: fs, vt_e: fe, tt_s: tt, props: v.props.clone(),
                                source: source_of(&v.source), provenance_ref: v.provenance_ref.clone(),
                            });
                        }
                    }
                    let proto = &hits[0];
                    rows.push(EdgeRow {
                        vid: version_vid(&proto.eid, tt, lo), src_id: self.dense(&proto.src)?,
                        dst_id: self.dense(&proto.dst)?, rel_type: proto.rel_type.clone(), disc: proto.disc.clone(),
                        vt_s: lo, vt_e: hi, tt_s: tt, props: props.clone(), source: source.to_string(),
                        provenance_ref: prov.map(str::to_string),
                    });
                    self.stage_edges_timed(rows)?;
                }
            },
        }
        Ok(Step::Done)
    }
}
