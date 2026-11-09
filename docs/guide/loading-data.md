# Loading data

There are four ways to put data into a store:

| way | call | one batch is | use it for |
|---|---|---|---|
| single writes | `assert_node`, `assert_edge`, `correct`, `retract` | one call | interactive edits, corrections, small updates |
| bulk batches | `store.ingest_events(events, nodes=...)` | up to 50,000 events | loading many nodes and events at once |
| a JSONL file | `gnomon ingest events.jsonl --store <dir>` | as `ingest_events` | loading a file from the shell |
| an event log | `gnomon replay log.jsonl --store <new dir>` | one log record | rebuilding a store, or loading history with your own belief times |

Each batch is written to the store's event log first, then applied, then
published, all at one belief time. Every batch costs a few synced file
writes, whatever its size. Write many rows in few batches.

The example for this page is
[`examples/loading_data.py`](../../examples/loading_data.py). Its output is
at the [end of the page](#the-example-output).

## Single writes

```python
store.assert_node(uid, label, props=None, vt_s=0, vt_e=OPEN_END)      # -> belief time
store.assert_edge(src, dst, rel_type, props=None, vt_s=0, vt_e=OPEN_END, disc="")
store.correct(ref, new_props, vt_s=0, vt_e=OPEN_END)
store.retract(ref, t)
```

Each returns the belief time of its batch. The rules, in short (see
[Concepts](concepts.md#writes-and-close-records)):

* An assert supersedes the believed versions it overlaps in valid time and
  keeps their parts outside the new interval.
* A correction does the same over `[vt_s, vt_e)`, but needs a believed
  version there and keeps its label.
* A retraction ends the version valid at `t` at `t`. Retracting at the
  version's own `vt_s` withdraws it completely.
* New versions carry exactly the props you pass. Pass the whole object.

### Endpoints are not created by `assert_edge`

`assert_edge` writes the edge only. If its endpoints have no node versions,
operators that build a snapshot graph (`snapshot_subgraph`, `match` at an
instant, `paths`) do not see the edge, because a snapshot edge needs both
endpoints in the snapshot. Assert the nodes too:

```python
store.assert_node("acct-5", "Account", {}, vt_s=0)
store.assert_node("acct-6", "Account", {}, vt_s=0)
store.assert_edge("acct-5", "acct-6", "TRUSTS", vt_s=0)
```

Bulk ingest, below, creates missing endpoints for you.

## Bulk batches

```python
store.ingest_events(events, node_label="Node", nodes=None)   # -> belief time of the last batch
```

* `events`: an iterable of dictionaries with `src`, `dst`, `rel_type`,
  `vt_s`, and optionally `vt_e`, `disc` and `props`. An event without
  `vt_e` is an instant: `vt_e = vt_s + 1`.
* `nodes`: optional node records `{uid, label, props?, vt_s, vt_e?}`. They
  are written first, so the events that follow see them.
* An endpoint with no believed node version and no node record gets a bare
  node version: label `node_label`, no props, valid from its first event.
* Events are split into batches of 50,000; each batch has its own belief
  time.

Bulk ingest only adds. It does not supersede: each event without a `disc`
is its own edge, and a node record for a uid that already has a believed
version is refused (`E_INVALID_ARG`). Use `assert_node` or `correct` to
change existing nodes.

An event whose `props` hold a float is loaded by a slower path than one
with only integers, strings and booleans. The stored data is the same.

### Name the edges you will correct

A bulk event without `disc` is named by its position in the stream:
`"#0"`, `"#1"`, ... The count continues across `ingest_events` calls on one
open `Store`, but it starts again at `"#0"` each time the store is opened.
So an event loaded in a later session with the same `src`, `dst` and
`rel_type` as one from an earlier session gets the same identity, and the
two become versions of one edge. To keep events apart, and to correct or
retract one later, give each event a `disc` you can rebuild, such as a
transaction id:

```python
store.ingest_events([
    {"src": "acct-1", "dst": "acct-3", "rel_type": "PAYS", "vt_s": 100,
     "props": {"amount": 250}, "disc": "tx-1"},
])
tx1 = EntityRef("edge", src="acct-1", dst="acct-3", rel_type="PAYS", disc="tx-1")
store.correct(tx1, {"amount": 205}, vt_s=100, vt_e=101)
```

An instant event lives over `[vt_s, vt_s + 1)`, so a correction of one
event uses that interval.

## Corrections and retractions

The example loads three payments and a `TRUSTS` edge, corrects payment
`tx-1` from 250 to 205, and ends the `TRUSTS` edge at valid time 300.
Asked now, the store shows the corrected state. Asked as of the bulk
batch's belief time, it shows what was believed then: amount 250, and the
`TRUSTS` edge running to 500. Nothing was lost.

A correction or retraction that finds nothing to act on is refused with
`E_NOT_FOUND`, and nothing is written.

## Many writer threads

A store has one writer, and `Store` serializes its write calls. If many
threads in the writer process each produce single writes,
`gnomon.write.GroupCommitWriter` gathers them into shared batches:

```python
from gnomon.write import GroupCommitWriter

writer = GroupCommitWriter(store).start()
# from any thread; each call blocks until its batch is durable, and
# returns the belief time of that batch
writer.assert_edge("hub", "m1", "LINK", vt_s=50)
writer.close()   # commits everything already submitted, then stops
```

It offers `assert_node`, `assert_edge`, `retract` and `correct`, with the
same arguments as `Store`.

## Load a JSONL file

`gnomon ingest` reads one event per line, in the format `ingest_events`
takes, and loads them in bulk:

```bash
gnomon ingest events.jsonl --store mystore
```

It does not take node records. Load labelled nodes from Python with
`ingest_events(..., nodes=...)`. See [Command line](cli.md#ingest).

## Replay an event log

Every store keeps its history in `<store>/eventlog.jsonl`: a header line,
then one JSON record per batch, `{"batch_id", "tt", "ops"}`. `gnomon
replay` builds a new store from such a log. It applies each record at its
recorded belief time, so the new store is identical to the old one, belief
times included:

```bash
gnomon replay mystore/eventlog.jsonl --store copy
```

The target must be a new, empty store directory. `--compact-every N`
compacts during a long replay.

### Your own belief times

A live store stamps belief times from the clock. If your history already
has dates for when each fact was recorded (revision times, ledger posting
times, publication dates), write an event log with those times and replay
it. Use the same classes the store uses:

```python
from gnomon.storage.base import make_op
from gnomon.storage.eventlog import EventLog

log = EventLog("revisions.jsonl")          # creates the file with its header
log.append(us("2022-01-10"), [             # one record = one batch at this belief time
    make_op("assert_node", uid="lyon", label="City", props={"population": 510_000},
            vt_s=us("2000-01-01"), vt_e=OPEN_END, source="ingest", provenance_ref=None),
])
log.append(us("2022-04-20"), [
    make_op("correct", ref={"kind": "node", "uid": "lyon"}, props={"population": 520_000},
            vt_s=us("2000-01-01"), vt_e=OPEN_END, source="ingest", provenance_ref=None),
])
```

then `gnomon replay revisions.jsonl --store kb`. The rules:

* Belief times must strictly increase from record to record; replay refuses
  a log that goes backwards.
* One record may hold several ops; they share its belief time.
* Op shapes are those the `Store` writes: `assert_node` (`uid`, `label`,
  `props`, `vt_s`, `vt_e`), `assert_edge` (`src`, `dst`, `rel_type`,
  `props`, `vt_s`, `vt_e`, `disc`), `correct` (`ref`, `props`, `vt_s`,
  `vt_e`), `retract` (`ref`, `t`), each with `source` and `provenance_ref`.
  A `ref` is `{"kind": "node", "uid": ...}` or `{"kind": "edge", "src":
  ..., "dst": ..., "rel_type": ..., "disc": ...}`.
* Use belief times in the past. After the replay, live writes continue
  with belief times after the last record.

[Scenarios](scenarios.md#a-revision-history-study) uses this to reproduce
an analysis at a past revision.

## The example output

[`examples/loading_data.py`](../../examples/loading_data.py) prints every
believed edge version as `(src, dst, disc, vt_s, vt_e, props)`:

<!-- example-output: loading_data.py -->
```text
after the bulk batch:
   ('acct-1', 'acct-3', 'tx-1', 100, 101, {'amount': 250})
   ('acct-3', 'acct-4', 'tx-2', 120, 121, {'amount': 240})
   ('acct-4', 'acct-1', '#2', 130, 131, {'amount': 230})
acct-4: Node 120 {}
now:
   ('acct-1', 'acct-2', '', 10, 300, {'level': 'low'})
   ('acct-1', 'acct-3', 'tx-1', 100, 101, {'amount': 205})
   ('acct-3', 'acct-4', 'tx-2', 120, 121, {'amount': 240})
   ('acct-4', 'acct-1', '#2', 130, 131, {'amount': 230})
as believed after the bulk batch:
   ('acct-1', 'acct-2', '', 10, 500, {'level': 'low'})
   ('acct-1', 'acct-3', 'tx-1', 100, 101, {'amount': 250})
   ('acct-3', 'acct-4', 'tx-2', 120, 121, {'amount': 240})
   ('acct-4', 'acct-1', '#2', 130, 131, {'amount': 230})
refused: E_NOT_FOUND no believed node version of acct-9 valid at 50
refused: E_INVALID_ARG
snapshot from acct-5 before its nodes exist: 0 nodes
after asserting both nodes: 2 nodes, 1 edge
stats: {'n_entities': 6, 'n_node_versions': 6, 'n_edge_versions': 7}
```

* The third payment had no `disc`, so it is named `#2`, its position in
  the batch.
* `acct-4` was only an endpoint. Bulk ingest created a bare `Node` version
  for it, valid from its first event at 120.
* `store.stats()` counts every stored version, superseded ones included:
  seven edge versions for five edges.
