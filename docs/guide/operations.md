# Operations

How a store lives on disk, what makes it durable, how to maintain it, and
what to tune. The [architecture](../architecture.md) page has the full
design; this page is the practical summary.

## Store layout

A store is a directory:

```
<store>/
  eventlog.jsonl          write-ahead log: a header line, then one JSON record per batch
  writer.lock             held by the single writer for its lifetime
  native/
    CURRENT               "<generation> <manifest_sha>": the publication point
    manifests/            one manifest record per generation
    seg/                  immutable column segments (node rows and edge rows)
    close/                immutable close runs (belief-time ends recorded later)
    dict.log              append-only dictionary: uid -> dense id
    idx/                  rebuildable per-segment indexes (never authoritative)
```

Files under `native/seg/` and `native/close/` are written once and never
changed. Indexes under `native/idx/` can be deleted at any time; they are
rebuilt on use.

## Durability

Each write batch commits in this order, each step durable before the next:

1. the batch is appended to `eventlog.jsonl` and synced;
2. its segment and close-run files are written and synced;
3. the dictionary tail is appended and synced;
4. a new manifest is written and renamed into place;
5. `CURRENT` is replaced atomically. The batch is now visible.

A crash before step 5 leaves the previous generation in force. The next
writer to open the store replays the part of the event log the engine had
not applied, so an acknowledged write is never lost. The event log is the
authoritative record: the rest of the store is a deterministic product of
it, and `gnomon replay` can rebuild it.

By default every batch publishes its own generation. With
`GNOMON_SMALL_COMMIT=1`, set before the store is opened, a batch is
acknowledged once its log record is synced, and generations are published
in groups (every `GNOMON_SMALL_COMMIT_N` batches or
`GNOMON_SMALL_COMMIT_MS` milliseconds). Answers, the event log and digests
do not change; readers in other processes see new data a little later.
See [configuration](../configuration.md#commit-path).

## The writer and the readers

* **One writer.** The first process to open a store without `read_only`
  holds `writer.lock`. A second writer fails at once with
  `WriterLockedError`. Within the writer process, write calls are
  serialized; `GroupCommitWriter` batches calls from many threads
  ([Loading data](loading-data.md#many-writer-threads)).
* **Many readers.** Open with `gnomon.open(path, read_only=True)` in every
  process that does not write. A reader never writes under the store
  directory and never blocks the writer or is blocked by it.
* **A reader sees one generation.** A read-only handle keeps the
  generation it opened, for as long as it stays open. Commits made after
  that are not visible to it. Open a new handle to see them.
* The writer's own handle always reads its own commits.
* Maintenance commands (`compact`, `settle`, `gc`) open the store as a
  writer, so they cannot run while another process writes.

The [limitations](../limitations.md#scope) page lists what is out of scope:
one machine, one writer, POSIX file-system semantics.

## Compaction, settle and gc

Every batch adds at least one segment. A store written in many small
batches therefore collects many small segments and unfolded close runs,
and reads slow down. Maintenance never runs in the background; you run it.

| task | command | Python | when |
|---|---|---|---|
| incremental compaction | `gnomon store compact --store <store>` | `store.adapter.compact()` | regularly, on a store that takes many small writes |
| settle | `gnomon store settle --store <store>` | `store.adapter.settle()` | once loading is done, before a read-heavy period; no writer may be active |
| garbage collection | `gnomon store gc --store <store> --keep 2` | `store.adapter.gc(keep_last=2)` | after `compact` or `settle`, to delete what they superseded |

* Compaction and settle change the layout only. Content, belief times and
  every digest stay the same.
* Neither deletes anything; `gc` does. Until it runs, the store can take up
  to twice its size on disk.
* `gc` keeps the current generation, the last `--keep` generations, and any
  generation a reader in the same process has pinned. Readers in other
  processes map their files when they open, so on a local POSIX file system
  they keep reading after `gc` (for stores up to `GNOMON_PIN_MAX_SEGMENTS`
  live segments; see [limitations](../limitations.md#known-open-items)).
* A long replay can compact as it goes: `gnomon replay ... --compact-every N`.
* `gnomon store build-indexes` builds the per-segment indexes ahead of
  time, so the first queries of a long-running process do not pay for
  them. `settle` rebuilds them by default.

## Integrity, backup and restore

* `gnomon store verify --store <store>` checks every file of the current
  generation against its checksums. `--full`, or `gnomon check <store>`, also
  checks the manifest chain, dictionary references, the bi-temporal
  invariants and the event log's hash chain. Both are read-only.
* A store that fails the check is rebuilt from its event log:
  `gnomon replay <store>/eventlog.jsonl --store <new store>`.
* `gnomon store backup --store <store> --dest <backup>` copies the event
  log with a manifest; `gnomon store restore --dest <backup> --store <new
  store>` replays it into a new store and checks the result. See [Command line](cli.md#store-backup-and-restore).
* To compare two stores loaded independently from the same data, compare
  content digests (`gnomon store digest`). `store.digest()` (the store
  digest) includes belief times, so it matches only between a store and its
  replays.

## Configuration

The engine needs no configuration. Every knob is an environment variable,
read by the process that opens the store; the full list is in
[configuration](../configuration.md). The ones most often useful:

| variable | default | use |
|---|---|---|
| `GNOMON_SEGMENT_CACHE_BYTES` | half of RAM, at most 32 GiB | memory for decoded segment columns |
| `GNOMON_SCAN_THREADS` | one per core | width of the parallel read stages |
| `GNOMON_MATCH_THREADS` | the available cores | threads of the pattern and path kernels |
| `GNOMON_SETTLE_BYTES` | 16 GiB | peak input bytes of one settle merge group |
| `GNOMON_DIGEST_BYTES` | 2 GiB | sort buffer of the streaming content digest |
| `GNOMON_SMALL_COMMIT` | off | grouped publication of commits, above |
| `GNOMON_TIME_COEFF_SCALE` | 1.0 | scales the cost check's time estimates for a slower or faster machine |

The **design switches** in that page turn one engine design off at a time,
with the same answers. They exist to measure what each design is worth;
leave them unset in normal use. `store.adapter.designs()` reports what an
open handle runs with.

## Performance tips

These follow from the design described in [architecture](../architecture.md)
and the known limits in [limitations](../limitations.md).

**Writing**

* Write in batches. Every batch pays for a few synced file writes and a new
  generation, whatever its size. Use `ingest_events` for many events, and
  its `nodes` argument for many labelled nodes, rather than one
  `assert_node` call per node.
* Keep event properties to integers, strings and booleans where you can.
  Events whose `props` hold a float are loaded by a slower path.
* Compact a store that takes many small writes, and settle it before heavy
  reading. Many small segments make every read open more files.

**Reading**

* Narrow every call: a valid-time window, seeds, `rel_types`, fewer hops.
  The engine prunes segments by their time and key bounds, and the cost
  check prices what is left.
* To read one node at one time, pass `t_valid` or `window` to
  `entity_history`: it then reads only the versions it needs instead of the
  whole history.
* Keep a long-running process for repeated queries. Decoded segments and
  indexes are cached per process, and `gnomon store build-indexes` builds
  the indexes ahead of time.
* Read large answers page by page with `limit` and `cursor`. A `match`
  cursor is an offset, so each page runs the plan again: prefer a
  `return.limit` or an aggregation to reading every row.
* On very large stores the cost check is conservative. If a call you know
  to be small is refused, narrow it, raise `cost_ceilings` for that call,
  or pass `skip_cost_check=True` ([Querying](querying.md#the-cost-check)).
