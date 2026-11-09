# Command line

Installing the package installs the `gnomon` command. It loads data,
answers operator calls, and maintains a store. Every subcommand documents
its options with `--help`:

```text
$ gnomon --help
usage: gnomon [-h] {ingest,replay,call,store,check} ...

positional arguments:
  {ingest,replay,call,store,check}
    ingest              ingest an event JSONL file into a store
    replay              rebuild a store from a recorded event log (byte-
                        identical; preserves transaction times, unlike a fresh
                        ingest)
    call                call one operator against a store
    store               native store maintenance
    check               full integrity check of a store (alias of `store
                        verify --full`)

options:
  -h, --help            show this help message and exit
```

The outputs below are from one real session, started in an empty
directory. Belief times (`last_tt`, `tt_q`), identities and digests differ
on every run. [`examples/cli_tour.py`](../../examples/cli_tour.py) runs the
same commands and prints only what does not change; its output is at the
[end of the page](#the-tour).

## ingest

`gnomon ingest <events.jsonl> --store <dir>` loads one JSON event per line,
as `Store.ingest_events` does (see [Loading data](loading-data.md#bulk-batches)).
The file used here:

```text
{"src": "alice", "dst": "bob", "rel_type": "KNOWS", "vt_s": 100, "vt_e": 900}
{"src": "bob", "dst": "carol", "rel_type": "KNOWS", "vt_s": 200, "vt_e": 900}
{"src": "alice", "dst": "carol", "rel_type": "MSG", "vt_s": 250, "props": {"text": "hi"}}
```

```text
$ gnomon ingest events.jsonl --store mystore
{"last_tt": 1794233034593572, "stats": {"n_entities": 3, "n_node_versions": 3, "n_edge_versions": 3, "vt_min": 100, "vt_max": 900, "rel_type_counts": {"KNOWS": 2, "MSG": 1}, "max_out_degree": 2}}
```

`last_tt` is the belief time of the last batch. The store is created if it
does not exist.

## call

`gnomon call --store <dir> <operator> '<json arguments>'` opens the store
read-only, runs one operator and prints the envelope. It applies the
default cost ceilings.

```text
$ gnomon call --store mystore temporal_reachability '{"src": "alice", "window": {"t_a": 0, "t_b": 1000}}'
{
 "op": "temporal_reachability",
 "args_echo": {
  "src": "alice",
  "window": {
   "t_a": 0,
   "t_b": 1000
  },
  "as_of_tt": 4611686018427387904,
  "delta_max_wait": null,
  "direction": "out",
  "limit": 100,
  "cursor": null
 },
 "dataset_extent": {
  "vt_min": 100,
  "vt_max": 900
 },
 "truncated": false,
 "rows": [
  {
   "uid": "bob",
   "earliest_arrival": 100
  },
  {
   "uid": "carol",
   "earliest_arrival": 200
  }
 ],
 "rows_total": 2,
 "cursor": null,
 "tt_q": 1794233034593572,
 "pinned": false,
 "clamped": false,
 "dependency": {
```

(The output continues with the `dependency` and `plan_ir` objects and ends
with `"result_digest"`; it is cut here.) An error prints the error payload
and exits with status 1:

```text
$ gnomon call --store mystore entity_history '{"uid": 7}'
{
 "error": "E_SCHEMA",
 "message": "invalid args for entity_history: 7 is not of type 'string'",
 "details": {
  "path": [
   "uid"
  ],
  "expected": {
   "type": "string"
  }
 }
}
```

## store verify and check

`gnomon store verify` checks every file the current generation names
against its checksums. `--full` adds the cross-file invariants, the
bi-temporal row invariants and the event log's hash chain. It is
read-only, so it is safe while a writer is running. Exit status: 0
healthy, 1 problems found, 2 the store cannot be opened.

```text
$ gnomon store verify --store mystore
store:      mystore
mode:       fast
generation: 1
checked:    2 segments (6 rows), 0 close runs (0 closes), 3 dictionary records
layout:     2 tt_s runs across live segments, 1 in the worst one

verdict: healthy — every referenced file passed its checksums and cross-references
$ gnomon store verify --store mystore --full
store:      mystore
mode:       full
generation: 1
checked:    2 segments (6 rows), 0 close runs (0 closes), 3 dictionary records
walked:     6 rows (6 believed) across 6 identities
layout:     2 tt_s runs across live segments, 1 in the worst one

verdict: healthy — every referenced file passed its checksums, every cross-reference resolved, and every bitemporal invariant held
```

`gnomon check <dir>` is `store verify --full`; `--json` prints the report
as JSON:

```text
$ gnomon check mystore --json
{"believed_rows": 6, "close_runs_checked": 0, "closes": 0, "closes_by_lane": {"edge_event": {"n_closed_folded": 0, "rows": 3, "segments": 1, "segments_all_current": 1}, "edge_interval": {"n_closed_folded": 0, "rows": 0, "segments": 0, "segments_all_current": 0}, "node": {"n_closed_folded": 0, "rows": 3, "segments": 1, "segments_all_current": 1}}, "dict_records": 3, "findings": [], "generation": 1, "healthy": true, "identities_checked": 6, "manifest_checkpoint": 0, "manifest_deltas": 1, "manifest_format": 3, "max_tt_s_runs": 1, "mode": "full", "problems": [], "readable": true, "rows": 6, "rows_walked": 6, "segments_checked": 2, "store": "mystore", "tt_s_runs": 2}
```

A store that fails the check is rebuilt from its event log with `gnomon
replay`.

## store compact, settle, gc

Maintenance never runs on its own. See [Operations](operations.md#compaction-settle-and-gc)
for when to run each one.

```bash
gnomon store compact --store mystore
gnomon store settle --store mystore
gnomon store gc --store mystore --keep 2
```

```text
$ gnomon store gc --store mystore --keep 2
{"manifests_removed": 1, "segments_removed": 0, "close_runs_removed": 0, "bytes_reclaimed": 638, "generations_retained": 2, "index_files_removed": 0}
```

`compact` and `settle` print a JSON report in the same style: segment
counts before and after, rows rewritten, close records folded, and the
call's own wall time in microseconds (`wall_us`). `settle` also reports
`reclaimable_bytes`, the space `gc` can then free, and the indexes it
rebuilt. The [tour](#the-tour) below lists the keys of the `compact`
report.

* `compact`: incremental, size-tiered merging of segments, folding close
  records.
* `settle [--budget-bytes N] [--no-index]`: a one-shot merge of every live
  segment, for a read-optimised layout. Run it while no writer is active.
* `gc [--keep N]`: delete manifests and files that no retained generation
  names (the current one, the last `N`, and those pinned by readers in this
  process).

## store build-indexes

Builds the rebuildable per-segment indexes ahead of time, instead of on
first use: `--tcsr` (adjacency), `--name` (name index), `--vidfilter`
(identity filters), or all of them with no flag.

```text
$ gnomon store build-indexes --store mystore
{"tcsr": {"segments": 1, "built": 0, "loaded": 1}, "name": {"segments_built": 0}, "vidfilter": {"segments": 2, "built": 0, "loaded": 2}}
```

## store digest

The **content digest** covers every version but not the fields the clock
sets (`vid`, `tt_s`, `tt_e`), so two independent loads of the same data
agree on it. `--streaming` computes the same value in bounded memory.

```text
$ gnomon store digest --store mystore
{"digest": "37f14f3b47f6890ccbf443c077be4485b4327e37e737811c82fc5341b0a2e2a8", "ambiguous_ties": 0, "streaming": false}
```

With `--streaming` the same `digest` is printed, with the sort's own
counters (rows held, runs spilled to disk, merge rounds, the byte budget).

## replay

`gnomon replay <eventlog.jsonl> --store <new dir> [--compact-every N]`
rebuilds a store from an event log, at the recorded belief times. The copy
has the same content digest:

```text
$ gnomon replay mystore/eventlog.jsonl --store copy
{"batches": 1, "stats": {"n_entities": 3, "n_node_versions": 3, "n_edge_versions": 3, "vt_min": 100, "vt_max": 900, "rel_type_counts": {"KNOWS": 2, "MSG": 1}, "max_out_degree": 2}}
$ gnomon store digest --store copy
{"digest": "37f14f3b47f6890ccbf443c077be4485b4327e37e737811c82fc5341b0a2e2a8", "ambiguous_ties": 0, "streaming": false}
```

## store backup and restore

`backup` copies the event log, the store's authoritative record, with a
manifest that identifies it. `restore` replays the backup into a new store
and checks the result against the manifest. `--store` always names the
store acted on or created; `--dest` always names the backup directory.

```text
$ gnomon store backup --store mystore --dest bak
{
 "store_identity": "fc0f949fff39815c307df622677c0b3d10a6bba9f360dda035f7c01c3e686719",
 "generation": 2,
 "manifest_sha": "44e3fd68ef97cc55",
 "log_sha256": "f599f55452fd9dd315afe6b26692eb0b121012cec0183a8e257f7d1fba3a676c",
 "log_records": 1,
 "logical_digest": "642a9db84ea18878c73deae5f21b0cbf6c3305625115633041a650efe558501a",
 "gnomon_version": "0.1.0",
 "commit": null
}
$ gnomon store restore --dest bak --store restored
{
 "verdict": "PASS",
 "store_identity": "fc0f949fff39815c307df622677c0b3d10a6bba9f360dda035f7c01c3e686719",
 "expected_store_identity": "fc0f949fff39815c307df622677c0b3d10a6bba9f360dda035f7c01c3e686719",
 "logical_digest": "642a9db84ea18878c73deae5f21b0cbf6c3305625115633041a650efe558501a",
 "expected_logical_digest": "642a9db84ea18878c73deae5f21b0cbf6c3305625115633041a650efe558501a"
}
```

`restore` exits with status 1 and `FAIL` on any mismatch, and refuses a
backed-up log whose checksum does not match its manifest.

## store upgrade-manifests

Converts a store written by an older engine (manifest format 1 or 2) to
format 3. Such stores answer queries but refuse writes until upgraded.

```text
$ gnomon store upgrade-manifests --store mystore
store:      mystore
unchanged:  already at manifest format 3
```

## The tour

[`examples/cli_tour.py`](../../examples/cli_tour.py) runs the commands
above in a temporary directory, through the `gnomon` script next to the
running Python. It prints exit codes and the parts of each output that do
not change from run to run:

<!-- example-output: cli_tour.py -->
```text
ingest: 0 {'n_node_versions': 3, 'n_edge_versions': 3}
call: 0 [('bob', 100), ('carol', 200)]
call with a bad argument: 1 E_SCHEMA
verify: 0 verdict: healthy
check: 0
compact: 0 ['closes_folded', 'clusters_touched', 'edge_rows', 'incremental', 'node_rows', 'rows_rewritten', 'segments_after', 'segments_before', 'wall_us']
settle: 0
gc: 0 ['bytes_reclaimed', 'close_runs_removed', 'generations_retained', 'index_files_removed', 'manifests_removed', 'segments_removed']
build-indexes: 0
digest, streaming equals in-memory: True
replay: 0 1 batch(es)
replayed copy has the same content digest: True
backup: 0 ['backup_manifest.json', 'eventlog.jsonl']
restore: 0 PASS
store layout: ['eventlog.jsonl', 'native', 'writer.lock'] ['CURRENT', 'close', 'dict.log', 'idx', 'manifests', 'seg']
```
