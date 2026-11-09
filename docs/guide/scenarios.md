# Worked scenarios

Three complete studies, each one runnable file with small synthetic data.
They use the same pattern: record the belief time that matters, keep
writing, and then ask the same question at that belief time and now.

- [A payment audit](#a-payment-audit)
- [Entity resolution](#entity-resolution)
- [A revision-history study](#a-revision-history-study)

## A payment audit

File: [`examples/scenario_payment_audit.py`](../../examples/scenario_payment_audit.py).

**The situation.** On 2 March, account `acct-V` paid 4,900 to `acct-M`,
and `acct-M` paid on to `acct-X`. An alert on `acct-M` was opened at 10:00
and cleared by an analyst at 16:00. After the clearing, three things were
learned:

1. a delayed feed reported a transfer from `acct-M` to `acct-Z` at 09:45;
2. the amount of the `acct-M` to `acct-X` transfer had been entered as 240
   and was really 2,400;
3. `acct-Z` belongs to K. Olsen, who owns `acct-V`: the money went back to
   its sender.

**The audit asks:** what did the analyst see when clearing the alert, and
what changed since?

**How.** Valid times are the real times of the transfers. The alert's
status is a node with a new version from 16:00; the write that records
"cleared" returns its belief time, and the example keeps it:

```python
t_cleared = store.assert_node("alert-17", "Alert",
                              {"account": "acct-M", "status": "cleared"},
                              vt_s=us("2021-03-02T16:00"))
```

Every question is then asked twice, with `as_of_tt=t_cleared` and with the
default (now):

* `temporal_reachability` from `acct-V` within the day: where could the
  money have gone, following transfers in time order?
* `aggregate_events` with `endpoint_filter` on `acct-M`: how much did
  `acct-M` pay out, and to whom?
* `diff_belief` from `t_cleared` to now, over that day: every version
  added or withdrawn since the clearing.
* `match`: who owns the accounts `acct-M` paid that day?

The example also reruns the reachability call pinned at `t_cleared` after
the later writes and compares `result_digest`: an answer pinned to a past
belief time does not change.

<!-- example-output: scenario_payment_audit.py -->
```text
alert-17 as believed at clearing: ['open', 'cleared']
money trail from acct-V, at clearing: [('acct-M', '09:00'), ('acct-X', '09:30')]
money trail from acct-V, now: [('acct-M', '09:00'), ('acct-X', '09:30'), ('acct-Z', '09:45')]
pinned answer unchanged after later writes: True | pinned: True
paid out by acct-M, at clearing: [('acct-X', 240)]
paid out by acct-M, now: [('acct-X', 2400), ('acct-Z', 2400)]
belief changes about that day since clearing: 3 added, 1 retracted
   added     acct-M->acct-Z PAYS
   added     acct-M->acct-X PAYS
   retracted acct-M->acct-X PAYS
   added     p-olsen->acct-Z OWNS
owners of accounts paid by acct-M, at clearing: [('acct-X', 'L. Xu')]
owners of accounts paid by acct-M, now: [('acct-X', 'L. Xu'), ('acct-Z', 'K. Olsen')]
```

**Reading it.** At clearing, the analyst saw 4,900 come in and 240 go out:
the money seemed to stay in `acct-M`. Now the store shows 4,800 leaving
within the hour, half of it to an account of the original sender. The diff
lists exactly what changed: the late transfer, the corrected amount (the
old version retracted, the new one added) and the ownership link. The
audit can show both pictures, and can prove which one the analyst had.

## Entity resolution

File: [`examples/scenario_entity_resolution.py`](../../examples/scenario_entity_resolution.py).

**The situation.** Two customer records, `cust-1001` ("J. Smith") and
`cust-2002` ("John Smith"), each hold a card. A matching step decides they
are one person. Later a reviewer finds the match wrong and splits them.

**How.** The merge is a belief, so it is a write: a `SAME_AS` edge between
the records, valid for all time, with the matcher's score.

```python
t_merged = store.assert_edge("cust-2002", "cust-1001", "SAME_AS", {"score": 0.93}, vt_s=0)
```

The split withdraws that edge. Retracting at the edge's own `vt_s` ends it
over its whole valid interval:

```python
t_split = store.retract(EntityRef("edge", src="cust-2002", dst="cust-1001",
                                  rel_type="SAME_AS"), 0)
```

Queries resolve a customer to "the person" through zero or one `SAME_AS`
step in either direction, a bounded walk in `match`:

```python
{"from": "c", "to": "same", "rel_type": "SAME_AS", "dir": "both", "hops": {"min": 0, "max": 1}}
```

Each question is asked at three belief times: before the merge, while it
was believed, and after the split.

<!-- example-output: scenario_entity_resolution.py -->
```text
records matching 'smith': [('cust-1001', 'J. Smith'), ('cust-2002', 'John Smith')]
cards of cust-1001's person, before the merge: ['card-1']
cards of cust-1001's person, merged: ['card-1', 'card-2']
cards of cust-1001's person, after the split: ['card-1']
payments by cust-1001's person, before the merge: 1
payments by cust-1001's person, merged: 3
payments by cust-1001's person, after the split: 1
SAME_AS version: {'score': 0.93} written at t_merged: True | withdrawn at t_split
```

**Reading it.** While the merge was believed, the person behind
`cust-1001` held two cards and made three payments. Before and after, one
card and one payment. A report produced during the merge can be
reproduced exactly, and the belief log shows when the merge was made and
when it was withdrawn. Nothing about the two records themselves was
rewritten.

The same approach works for merges you never withdraw, and for links with
a limited valid interval: two records that were one account only from a
given date.

## A revision-history study

File: [`examples/scenario_revision_history.py`](../../examples/scenario_revision_history.py).

**The situation.** A small knowledge base of cities is edited in dated
revisions: two cities on 10 January, Marseille and a misspelled duplicate
on 15 February, a corrected population for Lyon on 20 April, the duplicate
removed on 5 May, Hamburg added on 1 June. A study run on 1 March counted
cities and population per country. Months later, someone wants to
reproduce it, and to see how the corrections changed it.

**How.** Here the revisions already carry dates, so the belief times
should be those dates, not the time of loading. The example writes the
revisions as an event log, one record per revision at its date, and
replays it into a store (see
[Loading data](loading-data.md#your-own-belief-times)):

```python
log = EventLog(tmp / "revisions.jsonl")
for day, ops in REVISIONS:
    log.append(us(day), ops)
gnomon_cli(["replay", str(tmp / "revisions.jsonl"), "--store", str(tmp / "kb")])
```

The study is one `match` call with a group, at valid time 1 January 2022:

```python
call_operator(store.adapter, "match", {
    **ANALYSIS, "scope": {"vt": {"instant": us("2022-01-01")}, "as_of_tt": as_of_tt}})
```

The example runs it in March, from a store with the first two revisions
only. Then it replays the full log into a new store and runs it again,
pinned at 1 March, and now. It also asks:

* `state_series` on the **transaction axis**: how many cities the
  knowledge base believed in, every 30 days;
* `version_history` with `belief: "superseded"`: which nodes were revised;
* `diff_belief` between 1 March and 31 May: what those revisions changed.

The two JSON lines are printed by `gnomon replay`.

<!-- example-output: scenario_revision_history.py -->
```text
{"batches": 2, "stats": {"n_entities": 7, "n_node_versions": 7, "n_edge_versions": 5, "vt_min": 946684800000000, "vt_max": 946684800000001, "rel_type_counts": {"IN": 5}, "max_out_degree": 1}}
study at 2022-03-01, run in March: [('France', 4, 4350000), ('Germany', 1, 3600000)]
{"batches": 5, "stats": {"n_entities": 8, "n_node_versions": 9, "n_edge_versions": 6, "vt_min": 946684800000000, "vt_max": 946684800000001, "rel_type_counts": {"IN": 6}, "max_out_degree": 1}}
study at 2022-03-01, rerun later:  [('France', 4, 4350000), ('Germany', 1, 3600000)]
same result digest: True
study now:                         [('France', 3, 3490000), ('Germany', 2, 5400000)]
cities believed, every 30 days from 2022-01-01: [0, 3, 5, 5, 5, 4, 5]
superseded node versions: ['lyon', 'marseile']
changes from March to May:
   added     node lyon
   retracted node lyon
   retracted edge marseile->fr
   retracted node marseile
a live write is believed after the last revision: True
first log record: {"label": "Country", "op": "assert_node", "props": {"name": "France"}, "provenance_ref": null, "source": "ingest", "uid": "fr", "vt_e": 4611686018427387904, "vt_s": 946684800000000}
```

**Reading it.**

* The study pinned at 1 March gives the same rows and the same
  `result_digest` from both stores: France with 4 cities (the duplicate
  included), Germany with 1. Now, France has 3 cities and Lyon's corrected
  population, and Germany has 2.
* The belief series shows the knowledge base growing to 3 cities, then 5,
  dropping to 4 when the duplicate is removed, and back to 5 with Hamburg.
* Correcting Lyon superseded one version; removing the duplicate
  superseded another. The diff shows the same changes from the other side:
  Lyon's old version retracted and its new one added, the duplicate and
  its edge retracted.
* The replayed store accepts live writes afterwards. They are believed
  after the last revision.

For a large history, replay with `--compact-every` or compact afterwards;
see [Operations](operations.md#compaction-settle-and-gc).
