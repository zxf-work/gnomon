//! The bulk `ingest_events` op staged straight from its
//! event-log record.
//!
//! The Python bulk path (`gnomon/storage/base.py::_ingest_events`) turns every
//! event into Python objects before the engine sees it: a dict per event from
//! the log's `json.loads`, two sha256 identities (`edge_eid`, `_vid`), an
//! `EdgeVersion` dataclass, a `canonical_json(props)`, then one list per
//! column for `stage_edges`, whose binding converts every string again. On
//! the dedicated server that row build and batch conversion was 39% of a 10M
//! ingest.
//!
//! [`NativeStore::stage_ingest_events_raw`] takes the record bytes the event
//! log already holds (replay reads them; a live write has just serialized
//! them) and does the same work here: parse the one op, derive the
//! identities, run the same endpoint-existence probe, ensure the same
//! entities in the same order, and stage the same node and edge rows in the
//! same order. No per-row Python object is built.
//!
//! **Same rows or nothing.** The op is accepted only in the shape whose
//! Python result this function reproduces exactly: `ingest_events` with no
//! `nodes` array, and events carrying `src`/`dst`/`rel_type` strings, an
//! integer `vt_s`, an optional integer-or-null `vt_e`, an optional string
//! `disc`, and **no `props`**. Anything else — a `props` key, a `null` where
//! Python would carry `None` into a row, a float or boolean time, a duplicate
//! key (Python keeps the last, serde refuses), a malformed record — returns
//! `Ok(None)` *before anything is mutated*, and the caller runs the Python
//! path, which then answers (or raises) exactly as it always did. The switch
//! `GNOMON_INGEST_RAW=0` ([`crate::designs::Designs::ingest_raw`]) makes every
//! call return `Ok(None)`.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;

use crate::derive::{edge_eid, version_vid};
use crate::error::Result;
use crate::ingest_phase::{add, Timer, G};
use crate::row::{EdgeRow, NodeRow};
use crate::store::NativeStore;
use crate::{clamp_tt, OPEN_END};

/// `Some(value)` whenever the key is present, `null` included, so a field can
/// tell "absent" (`None`) from "present and null" (`Some(None)`).
fn present<'de, D, T>(d: D) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(d).map(Some)
}

#[derive(Deserialize)]
struct Event<'a> {
    #[serde(borrow)]
    src: Cow<'a, str>,
    #[serde(borrow)]
    dst: Cow<'a, str>,
    #[serde(borrow)]
    rel_type: Cow<'a, str>,
    vt_s: i64,
    /// `ev.get("vt_e") or vt_s + 1`: absent, `null` and `0` all mean `vt_s + 1`
    #[serde(default)]
    vt_e: Option<i64>,
    #[serde(default, borrow, deserialize_with = "present")]
    disc: Option<Option<Cow<'a, str>>>,
    /// any `props` key at all takes the Python path
    #[serde(default, deserialize_with = "present")]
    props: Option<IgnoredAny>,
}

#[derive(Deserialize)]
struct Op<'a> {
    #[serde(borrow)]
    op: Cow<'a, str>,
    #[serde(default, borrow)]
    events: Option<Vec<Event<'a>>>,
    /// any `nodes` key at all takes the Python path
    #[serde(default, deserialize_with = "present")]
    nodes: Option<IgnoredAny>,
    #[serde(default, deserialize_with = "present")]
    offset: Option<Option<i64>>,
    #[serde(default, borrow, deserialize_with = "present")]
    source: Option<Option<Cow<'a, str>>>,
    #[serde(default, borrow)]
    provenance_ref: Option<Cow<'a, str>>,
    #[serde(default, borrow, deserialize_with = "present")]
    node_label: Option<Option<Cow<'a, str>>>,
}

/// The `ops[n]` element of a record, every other element skipped unparsed.
/// `Err` (→ ineligible) for a record with two `ops` keys.
struct NthOp(usize);

impl<'de> DeserializeSeed<'de> for NthOp {
    type Value = Option<Op<'de>>;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> std::result::Result<Self::Value, D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for NthOp {
    type Value = Option<Op<'de>>;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an ops array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error> {
        let mut out = None;
        let mut i = 0usize;
        loop {
            if i == self.0 {
                match seq.next_element::<Op<'de>>()? {
                    Some(op) => out = Some(op),
                    None => break,
                }
            } else if seq.next_element::<IgnoredAny>()?.is_none() {
                break;
            }
            i += 1;
        }
        Ok(out)
    }
}

struct Record(usize);

impl<'de> DeserializeSeed<'de> for Record {
    type Value = Option<Op<'de>>;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> std::result::Result<Self::Value, D::Error> {
        d.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for Record {
    type Value = Option<Op<'de>>;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an event-log record")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Self::Value, A::Error> {
        let mut out = None;
        let mut seen = false;
        while let Some(key) = map.next_key::<Cow<'de, str>>()? {
            if key == "ops" {
                if seen {
                    return Err(serde::de::Error::custom("duplicate ops"));
                }
                seen = true;
                out = map.next_value_seed(NthOp(self.0))?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(out)
    }
}

/// One event, ready to stage.
struct Planned {
    /// position in the op's `events`
    ev: usize,
    src: usize,
    dst: usize,
    vt_e: i64,
    disc: String,
}

/// What the Python path computes before its first engine call.
struct Plan<'b> {
    /// endpoints, in first-seen order (src before dst, event by event)
    uids: Vec<&'b str>,
    /// the smallest `vt_s` each endpoint was seen at
    first_seen: Vec<i64>,
    events: Vec<Planned>,
    source: String,
    provenance_ref: Option<String>,
    label: String,
}

impl NativeStore {
    /// Stage `ops[op_index]` of the event-log record `record` — an
    /// `ingest_events` op — at transaction time `tt`, exactly as the Python
    /// bulk path would. `Ok(Some((edge rows, node rows)))` when staged;
    /// `Ok(None)` when the op is not in the accepted shape or the design is
    /// switched off, with nothing mutated (see the module docs). The batch
    /// must already be open at `tt`.
    pub fn stage_ingest_events_raw(
        &mut self,
        record: &[u8],
        op_index: usize,
        tt: i64,
    ) -> Result<Option<(usize, usize)>> {
        if !self.designs().ingest_raw {
            return Ok(None);
        }
        self.require_batch()?;
        // -- parse and check: nothing is mutated before both succeed
        let t = Timer::start();
        let mut de = serde_json::Deserializer::from_slice(record);
        let parsed = Record(op_index)
            .deserialize(&mut de)
            .and_then(|op| de.end().map(|_| op));
        let op = match parsed {
            Ok(Some(op)) => op,
            _ => {
                t.stop(G::IngestRawParseNs);
                add(G::IngestRawFallbacks, 1);
                return Ok(None);
            }
        };
        let plan = plan(&op);
        t.stop(G::IngestRawParseNs);
        let Some(plan) = plan else {
            add(G::IngestRawFallbacks, 1);
            return Ok(None);
        };
        let evs = op.events.as_deref().unwrap_or(&[]);

        // -- the endpoint-existence probe, over the first-seen order
        let t = Timer::start();
        let ask: Vec<String> = plan.uids.iter().map(|u| u.to_string()).collect();
        let known = self.nodes_with_believed_versions(&ask, clamp_tt(OPEN_END))?;
        t.stop(G::IngestRawProbeNs);

        // -- every endpoint ensured, in first-seen order: new ones get their
        //    dense ids in exactly the order the Python path assigns them
        let t = Timer::start();
        let mut ids = Vec::with_capacity(plan.uids.len());
        for u in &plan.uids {
            ids.push(self.ensure_entity(u, &plan.label)?);
        }
        t.stop(G::IngestRawEnsureNs);

        // -- identities: node vids for the new endpoints (sorted by uid, as
        //    `sorted(new_uids)`), edge eids and vids in event order
        let t = Timer::start();
        let mut new_nodes: Vec<usize> = (0..plan.uids.len())
            .filter(|&k| !known.contains(plan.uids[k]))
            .collect();
        new_nodes.sort_unstable_by(|&a, &b| plan.uids[a].cmp(plan.uids[b]));
        let node_vids: Vec<_> = new_nodes
            .iter()
            .map(|&k| version_vid(plan.uids[k], tt, plan.first_seen[k]))
            .collect();
        let edge_vids: Vec<_> = plan
            .events
            .iter()
            .map(|e| {
                let ev = &evs[e.ev];
                let eid = edge_eid(&ev.src, &ev.dst, &ev.rel_type, &e.disc).to_hex();
                version_vid(&eid, tt, ev.vt_s)
            })
            .collect();
        t.stop(G::IngestRawIdentityNs);

        // -- stage nodes, then edges, as `insert_node_versions` then
        //    `insert_edge_versions` would
        let t = Timer::start();
        for (j, &k) in new_nodes.iter().enumerate() {
            self.stage_node(NodeRow {
                vid: node_vids[j],
                uid_id: ids[k],
                label: plan.label.clone(),
                vt_s: plan.first_seen[k],
                vt_e: OPEN_END,
                tt_s: tt,
                props: "{}".to_string(),
                source: plan.source.clone(),
                provenance_ref: plan.provenance_ref.clone(),
            })?;
        }
        for (i, e) in plan.events.iter().enumerate() {
            let ev = &evs[e.ev];
            self.stage_edge(EdgeRow {
                vid: edge_vids[i],
                src_id: ids[e.src],
                dst_id: ids[e.dst],
                rel_type: ev.rel_type.to_string(),
                disc: e.disc.clone(),
                vt_s: ev.vt_s,
                vt_e: e.vt_e,
                tt_s: tt,
                props: "{}".to_string(),
                source: plan.source.clone(),
                provenance_ref: plan.provenance_ref.clone(),
            })?;
        }
        t.stop(G::IngestRawStageNs);
        add(G::IngestRawEdgeRows, plan.events.len() as u64);
        add(G::IngestRawNodeRows, new_nodes.len() as u64);
        Ok(Some((plan.events.len(), new_nodes.len())))
    }
}

/// Checks the accepted shape and computes, without touching the store, what
/// the Python path computes before its first engine call. `None`: ineligible.
fn plan<'b>(op: &'b Op<'_>) -> Option<Plan<'b>> {
    if op.op != "ingest_events" || op.nodes.is_some() {
        return None;
    }
    let offset = match op.offset {
        None => 0i64,
        Some(Some(o)) => o,
        Some(None) => return None,
    };
    let source = match &op.source {
        None => "ingest".to_string(),
        Some(Some(s)) => s.to_string(),
        Some(None) => return None,
    };
    let label = match &op.node_label {
        None => "Node".to_string(),
        Some(Some(s)) => s.to_string(),
        Some(None) => return None,
    };
    let provenance_ref = op.provenance_ref.as_ref().map(|s| s.to_string());
    let evs = op.events.as_deref().unwrap_or(&[]);
    let mut index: HashMap<&'b str, usize> = HashMap::with_capacity(evs.len() / 4 + 1);
    let mut uids: Vec<&'b str> = Vec::new();
    let mut first_seen: Vec<i64> = Vec::new();
    let mut events = Vec::with_capacity(evs.len());
    for (i, ev) in evs.iter().enumerate() {
        if ev.props.is_some() {
            return None;
        }
        let disc = match &ev.disc {
            None => format!("#{}", offset.checked_add(i64::try_from(i).ok()?)?),
            Some(Some(d)) => d.to_string(),
            Some(None) => return None,
        };
        let vt_e = match ev.vt_e {
            None | Some(0) => ev.vt_s.checked_add(1)?,
            Some(v) => v,
        };
        let mut endpoint = |u: &'b str| -> usize {
            if let Some(&k) = index.get(u) {
                if ev.vt_s < first_seen[k] {
                    first_seen[k] = ev.vt_s;
                }
                return k;
            }
            let k = uids.len();
            uids.push(u);
            first_seen.push(ev.vt_s);
            index.insert(u, k);
            k
        };
        let src = endpoint(ev.src.as_ref());
        let dst = endpoint(ev.dst.as_ref());
        events.push(Planned { ev: i, src, dst, vt_e, disc });
    }
    Some(Plan { uids, first_seen, events, source, provenance_ref, label })
}
