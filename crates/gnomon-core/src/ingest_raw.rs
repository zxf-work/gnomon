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
//! Python result this function reproduces exactly: `ingest_events` with
//! events carrying `src`/`dst`/`rel_type` strings, an integer `vt_s`, an
//! optional integer-or-null `vt_e`, an optional string `disc`, and optional
//! `props` without a float (the events of the external benchmark snapshots
//! all carry `"props": {}`); and an optional `nodes` array (the same
//! snapshots' shape)
//! whose records carry `uid`/`label` strings, an integer `vt_s`, an optional
//! integer `vt_e` above it, optional `props` that are an object (or null) of
//! strings, integers, booleans, nulls, arrays and objects -- **no float**,
//! whose Python text this module cannot promise to reproduce -- and no
//! per-record `source` or `provenance_ref`. The props are written in
//! `canonical_json`'s exact form (sorted keys, compact, `ensure_ascii=False`
//! escapes; [`canonical_props`]). A uid twice in the array, or one that
//! already has a believed version, is a refusal the Python path raises. Anything else — a `props` key, a `null` where
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
    /// `ev.get("props", {})`, rendered as `canonical_json` renders it (a
    /// float takes the Python path)
    #[serde(default, deserialize_with = "present")]
    props: Option<serde_json::Value>,
}

/// One record of an op's `nodes` array.
#[derive(Deserialize)]
struct NodeRec<'a> {
    #[serde(borrow)]
    uid: Cow<'a, str>,
    #[serde(borrow)]
    label: Cow<'a, str>,
    vt_s: i64,
    /// `rec.get("vt_e", OPEN_END)`: absent is open-ended; `null` raises
    #[serde(default, deserialize_with = "present")]
    vt_e: Option<Option<i64>>,
    /// `rec.get("props") or {}`
    #[serde(default, deserialize_with = "present")]
    props: Option<serde_json::Value>,
    /// a per-record override takes the Python path
    #[serde(default, deserialize_with = "present")]
    source: Option<IgnoredAny>,
    #[serde(default, deserialize_with = "present")]
    provenance_ref: Option<IgnoredAny>,
}

#[derive(Deserialize)]
struct Op<'a> {
    #[serde(borrow)]
    op: Cow<'a, str>,
    #[serde(default, borrow)]
    events: Option<Vec<Event<'a>>>,
    /// `op.get("nodes")`: absent, `null` and `[]` all mean none
    #[serde(default, borrow, deserialize_with = "present")]
    nodes: Option<Option<Vec<NodeRec<'a>>>>,
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
    props: String,
}

/// One explicit node, ready to stage.
struct Explicit<'b> {
    uid: &'b str,
    label: &'b str,
    vt_s: i64,
    vt_e: i64,
    props: String,
}

/// `canonical_json(v)` -- `json.dumps(v, sort_keys=True, separators=(",",
/// ":"), ensure_ascii=False)` -- of a JSON value as Python's `json.loads`
/// would hold it, or `None` for a float anywhere (Python's float text is
/// `repr`, which this does not reproduce). Keys sort by code point, which is
/// UTF-8 byte order; strings escape exactly Python's set: `"` `\\` and the
/// controls below U+0020, `\b \f \n \r \t` short and the rest `\u00xx`.
pub(crate) fn canonical_props(v: &serde_json::Value, out: &mut String) -> Option<()> {
    use serde_json::Value;
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                out.push_str(&i.to_string());
            } else {
                // past i64 but within u64; a float is `None` here
                out.push_str(&n.as_u64()?.to_string());
            }
        }
        Value::String(s) => canonical_str(s, out),
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical_props(x, out)?;
            }
            out.push(']');
        }
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort_unstable();
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical_str(k, out);
                out.push(':');
                canonical_props(&m[k.as_str()], out)?;
            }
            out.push('}');
        }
    }
    Some(())
}

fn canonical_str(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
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
    /// the `nodes` array, in record order
    explicit: Vec<Explicit<'b>>,
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

        // -- explicit nodes: one that already has a believed version is the
        //    refusal `_ingest_node_rows` raises, so the Python path answers
        let t = Timer::start();
        if !plan.explicit.is_empty() {
            let ask: Vec<String> = plan.explicit.iter().map(|x| x.uid.to_string()).collect();
            if !self.nodes_with_believed_versions(&ask, clamp_tt(OPEN_END))?.is_empty() {
                t.stop(G::IngestRawProbeNs);
                add(G::IngestRawFallbacks, 1);
                return Ok(None);
            }
        }
        let explicit_at: HashMap<&str, usize> =
            plan.explicit.iter().enumerate().map(|(j, x)| (x.uid, j)).collect();

        // -- the endpoint-existence probe, over the first-seen order
        let ask: Vec<String> = plan.uids.iter().map(|u| u.to_string()).collect();
        let known = if ask.is_empty() {
            Default::default()
        } else {
            self.nodes_with_believed_versions(&ask, clamp_tt(OPEN_END))?
        };
        t.stop(G::IngestRawProbeNs);

        // -- entities: the explicit nodes in record order, then every other
        //    endpoint in first-seen order, as the one `ensure_entities` call
        //    of the Python path assigns their dense ids
        let t = Timer::start();
        let mut explicit_ids = Vec::with_capacity(plan.explicit.len());
        for x in &plan.explicit {
            explicit_ids.push(self.ensure_entity(x.uid, x.label)?);
        }
        let mut ids = Vec::with_capacity(plan.uids.len());
        for u in &plan.uids {
            ids.push(match explicit_at.get(u) {
                Some(&j) => explicit_ids[j],
                None => self.ensure_entity(u, &plan.label)?,
            });
        }
        t.stop(G::IngestRawEnsureNs);

        // -- identities: node vids for the new endpoints (sorted by uid, as
        //    `sorted(new_uids)`), edge eids and vids in event order
        let t = Timer::start();
        let mut new_nodes: Vec<usize> = (0..plan.uids.len())
            .filter(|&k| !known.contains(plan.uids[k]) && !explicit_at.contains_key(plan.uids[k]))
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
        let explicit_vids: Vec<_> = plan
            .explicit
            .iter()
            .map(|x| version_vid(x.uid, tt, x.vt_s))
            .collect();
        let t = Timer::start();
        for (j, x) in plan.explicit.iter().enumerate() {
            self.stage_node(NodeRow {
                vid: explicit_vids[j],
                uid_id: explicit_ids[j],
                label: x.label.to_string(),
                vt_s: x.vt_s,
                vt_e: x.vt_e,
                tt_s: tt,
                props: x.props.clone(),
                source: plan.source.clone(),
                provenance_ref: plan.provenance_ref.clone(),
            })?;
        }
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
                props: e.props.clone(),
                source: plan.source.clone(),
                provenance_ref: plan.provenance_ref.clone(),
            })?;
        }
        t.stop(G::IngestRawStageNs);
        add(G::IngestRawEdgeRows, plan.events.len() as u64);
        let nodes = plan.explicit.len() + new_nodes.len();
        add(G::IngestRawNodeRows, nodes as u64);
        Ok(Some((plan.events.len(), nodes)))
    }
}

/// Checks the accepted shape and computes, without touching the store, what
/// the Python path computes before its first engine call. `None`: ineligible.
fn plan<'b>(op: &'b Op<'_>) -> Option<Plan<'b>> {
    if op.op != "ingest_events" {
        return None;
    }
    let mut explicit = Vec::new();
    if let Some(Some(nodes)) = &op.nodes {
        let mut seen: std::collections::HashSet<&'b str> = std::collections::HashSet::with_capacity(nodes.len());
        explicit.reserve(nodes.len());
        for rec in nodes {
            if rec.source.is_some() || rec.provenance_ref.is_some() {
                return None;
            }
            let vt_e = match rec.vt_e {
                None => OPEN_END,
                Some(Some(v)) => v,
                Some(None) => return None,
            };
            if rec.vt_s >= vt_e || !seen.insert(rec.uid.as_ref()) {
                return None;
            }
            let props = match &rec.props {
                None | Some(serde_json::Value::Null) => "{}".to_string(),
                Some(v @ serde_json::Value::Object(_)) => {
                    let mut s = String::new();
                    canonical_props(v, &mut s)?;
                    s
                }
                Some(_) => return None,
            };
            explicit.push(Explicit { uid: rec.uid.as_ref(), label: rec.label.as_ref(), vt_s: rec.vt_s, vt_e, props });
        }
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
        let props = match &ev.props {
            None => "{}".to_string(),
            Some(serde_json::Value::Object(m)) if m.is_empty() => "{}".to_string(),
            Some(v) => {
                let mut s = String::new();
                canonical_props(v, &mut s)?;
                s
            }
        };
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
        events.push(Planned { ev: i, src, dst, vt_e, disc, props });
    }
    Some(Plan { uids, first_seen, events, source, provenance_ref, label, explicit })
}
