# Architecture

Gnomon has three layers:

```
 Python  gnomon.Store ─ write-ahead event log (eventlog.jsonl), single-writer lock
         gnomon.storage.base ─ bi-temporal write semantics (carve / retract / correct)
         gnomon.temporal ─ operator registry, argument schemas, result envelopes
           │ PyO3 (crates/gnomon-py → gnomon._engine)
 Rust    gnomon-core ─ segments, close runs, manifests, compaction, indexes, scans
           │
 Disk    <store>/native/  immutable files + one atomically replaced pointer
```

The store is a deterministic materialization of its event log. That shapes
the durability design: the engine never has to make a partial physical
update recoverable; it only has to never expose an inconsistent generation.
After a crash it rolls back to the last published generation and the Python
layer replays the event-log suffix the engine had not applied.

## Store layout

```
<store>/
  eventlog.jsonl          write-ahead log: a header line, then one JSON record per batch
  writer.lock             held (flock) by the single writer for its lifetime
  native/
    CURRENT               "<generation> <manifest_sha>" — the publication point
    manifests/<G>.json    one manifest record per generation (checkpoint or delta)
    seg/<id>.tgs          immutable columnar segments (node rows and edge rows)
    close/<id>.tgc        immutable close runs (belief-time ends recorded after the fact)
    dict.log              append-only dictionary: uid string -> dense u32 id
    idx/                  rebuildable per-segment indexes (never authoritative)
```

## Commit protocol

Each write batch commits in this order, each step durable before the next:

1. the batch record is appended to `eventlog.jsonl` and fsynced (by the
   Python `Store`, before the engine is called);
2. the batch's new segment files and close-run file are written and fsynced;
3. the dictionary tail is appended and fsynced;
4. the new manifest record is written, fsynced and renamed into place;
5. `CURRENT` is replaced atomically — the generation is now published.

A crash before step 5 is invisible: the next open reads the old `CURRENT`,
ignores the orphaned files and truncates the dictionary tail. Each manifest
records the event-log offset and a rolling hash chain over the log prefix it
has applied, so recovery replays exactly the missing suffix and refuses a log
that does not match.

## Segments

A segment (`.tgs`) is written once and never modified. That one property
gives snapshot isolation, safe `mmap`, and lock-free concurrent readers.

* Rows are sorted by `(vt_s, vid)` and stored column by column in blocks of
  32,768 rows. Fixed-width hot columns (`vt_s`, `vt_e`, endpoints, relation
  code, ...) are 64-byte aligned for zero-copy typed views; variable-width
  values (properties, labels, discriminators) live in a per-segment string
  table referenced by index.
* Every column extent and the string table carry a CRC32C; the whole body
  carries a SHA-256 (`body_sha`), recorded in the footer and in the
  manifest entry. A trailing magic marks a complete file.
* Edge rows are routed at seal time into two physical classes: short-lived
  **event** rows, and long-lived **interval** rows with an explicit `vt_e`,
  which carry a per-block running maximum of `vt_e` (a prefix-max
  "staircase") so a valid-time window search can skip blocks that end
  before it starts. The class is physical only: it never affects identity,
  answers or digests.
* Segment headers carry zone maps (the ranges of `vt_s`, `vt_e` and `tt_s`
  per segment, and per-block bounds where a column needs them) that let
  scans prune whole segments and blocks.

### Time encoding

Times are `int64` microseconds. Columns are compressed per block:

* **Codec 1, frame of reference** — per block, the minimum and the bit width
  of `max - min`, then each value's delta bit-packed at that width. A
  constant block costs no payload at all.
* **Codec 2, linear frame of reference** — per block, an integer intercept
  and fixed-point slope, then each value's residual against that line. It
  suits `vt_s`, which rises steadily in sort order. The writer tries the
  candidate codecs on each column and keeps the smallest; no floating point
  is stored, so encodings are identical across machines.
* **Transaction time** (`tt_s`) is stored either as runs `(first_row, tt_s)`
  — one per batch, ideal for freshly sealed segments — or, once compaction
  has interleaved rows from many batches, as a dictionary of the distinct
  times plus a bit-packed per-row code column with per-block bounds. The
  writer measures and picks; readers see one view either way.

## Close runs and sidecars

Superseding a version means setting its `tt_e`, but a sealed segment cannot
be changed. Each commit instead appends an immutable **close run**: a list of
`(segment, row, tt_e)` records. The manifest lists the live close runs, and
because a reader pins one manifest generation, it sees exactly the closes
that existed at that generation — never a later one.

Compaction later folds close runs into a per-segment **sidecar** in the
segment header, in one of two representations: an `all_current` flag (no
row closed; costs nothing on a scan) or a sorted sparse list of
`(row, tt_e)` pairs. Folding moves where a `tt_e` is written; it never
drops a row.

### What a belief test costs

A read decides belief only for the rows it can use. Rows are stored in
`vt_s` order, so a valid-time window maps to a contiguous row range of each
segment, found by binary search; the belief state (the transaction-time
runs or dictionary codes, and the close records) is computed for that row
range alone. The cost of visibility is therefore proportional to the
`tt_s` runs, the straddling dictionary codes and the close records inside
the query’s row range, not to the segment’s totals, and a segment nothing
has corrected skips the work entirely. When a request covers most of a
segment, the engine computes the whole segment’s belief state once
instead. Traversals choose between building a segment’s believed row ranges
and testing belief per adjacency row, whichever reads less. The engine
counts the runs, codes and closes each belief test reads (`phase_stats`),
so this cost claim is checked by tests on counts rather than timings.

## Manifest generations

A manifest names everything that defines visibility at one generation: the
live segments (with their key ranges, row counts and `body_sha`), the live
close runs, the dictionary length, and the event-log offset and hash chain.
Readers pin a generation and therefore see a coherent snapshot.

Manifests are JSON so an operator can read them with `cat`. On disk the
current format (3) is a chain: a **checkpoint** holds the whole logical
manifest (written at generation 0, every 512th generation, and by
compaction and gc); a **delta** holds only what changed since its parent.
Each record carries its own checksum and its parent's hash. `manifest_sha`,
the digest of a generation's full logical content, is a Merkle root over the
ordered segment set, so a commit costs `O(appended + log n)` hashing rather
than re-hashing every live segment. Stores written in formats 1 and 2 open
read-only until `gnomon store upgrade-manifests` converts them.

## Compaction, settle and gc

Each batch seals at least one new segment, so a store that is written in
many small batches accumulates many small segments and unfolded close runs.
Three explicit maintenance operations bound that; none runs in the
background.

* **Compaction (incremental, size-tiered).** Segments are grouped into
  *clusters* — maximal sets whose composite-key ranges overlap, computed
  from the manifest without opening any file. Within a size tier, a set of
  same-size segments is merged by a bounded-fan-in streaming k-way merge
  (one row resident per input) into new segments near the target size
  (128 MiB), folding their close runs on the way. Segments already at the
  target are left alone, so each row is rewritten about once per tier
  rather than once per compaction. `Store.adapter.compact()` and
  `gnomon store compact` run it.
* **Settle.** The tier rule leaves partially filled tiers unmerged, and those
  are exactly the segments a whole-extent query must open. `settle` is a
  one-shot major compaction run at quiescence: one bounded k-way merge over
  every live segment, in key order, in groups bounded by a byte budget
  (`--budget-bytes` / `GNOMON_SETTLE_BYTES`), rewriting each row once and
  ending at the store's byte floor. The logical content, transaction times
  and every digest are unchanged. It rebuilds the per-segment indexes of the
  segments it writes unless told not to.
* **Garbage collection.** Compaction and settle supersede files but delete
  nothing, because a reader may still hold an older generation. `gc` removes
  a manifest or file exactly when no retained generation names it; a
  generation is retained if `CURRENT` names it, if it is among the last
  `--keep` generations (default 2), or if a live reader in this process has
  pinned it. Readers map every file of their generation at open, so files
  stay readable to them even after `gc` unlinks them.

## Per-segment indexes

Indexes live under `native/idx/`, one file per segment and index kind. Each
is stamped with its segment's `body_sha`; any load failure or stamp mismatch
means "rebuild silently", and a failure to write one never fails a query. An
index is never authoritative.

* **Temporal adjacency (TCSR).** A compressed sparse row index over all
  stored rows of one edge segment, in both directions, holding only `u32`
  offsets and a row permutation; times and endpoints are read through the
  permutation from the segment's own mapped columns. Belief filtering
  happens at traversal time, so a commit invalidates nothing — new segments
  simply have no index yet. Used by neighbourhood, path and reachability
  operators.
* **Name index.** For node segments, a sorted array of
  `(casefolded name, row)` pairs, answering exact and prefix name lookups
  by binary search (`lookup_name`). It covers every stored row, not only
  current ones, so it is as immutable as its segment and also answers
  historical `as_of` probes; candidates are re-checked for belief and for
  the actual name at query time.
* **Identity filter.** A correction or retraction must first find the
  believed versions of one identity. Identities are hashes and the store is
  clustered on `(vt_s, vid)`, so key ranges cannot prune that probe; without
  help it would visit every segment. Each segment therefore gets an immutable
  membership filter over the identity keys of all its stored rows, built at
  seal time (with an explicit rebuild and a lazy build on first use as
  fallbacks for older stores). The filter is a binary fuse filter with 16-bit
  fingerprints (false-positive rate about 1.5e-5), falling back to a Bloom
  filter, and finally to no filter. False negatives are impossible by
  construction; a false positive costs one extra segment visit.

## Readers, writers and concurrency

* One writer per store, enforced by `writer.lock`.
* Readers open with `gnomon.open(path, read_only=True)`. A read-only handle
  runs no recovery, writes nothing under the store directory (lazily built
  indexes are kept in memory for that handle), pins its generation, and
  never blocks or is blocked by the writer.
* Inside one process, `Store` serializes write batches; for many concurrent
  writer threads, `gnomon.write.GroupCommitWriter` coalesces submissions
  into shared commits.
* Large scans run in parallel above 4M rows (`GNOMON_SCAN_THREADS` sets the
  width); smaller ones run on one core.
* Decoded segments are cached per process within a byte budget
  (`GNOMON_SEGMENT_CACHE_BYTES`).

## Digests

Several hashes exist, and they answer different questions:

| digest | over | changes when | use |
|---|---|---|---|
| `body_sha` | one segment file's bytes | never (files are immutable) | integrity, index stamps |
| `manifest_sha` | one generation's logical manifest | every commit | snapshot identity, `CURRENT` check |
| **store digest** (`Store.digest()`) | every node and edge version, all fields | any write | "is this exactly the same store?" — two replays of one event log agree |
| **content digest** (`gnomon store digest`) | every version minus `vid`, `tt_s`, `tt_e` | any logical change | "do these two independent loads hold the same data?" |
| `result_digest` | one operator payload | when the answer changes | comparing answers across runs and backends |

The store digest includes transaction times, which come from the wall clock
at commit, so two independent loads of the same events always differ on it.
The content digest drops exactly the clock-derived fields and sorts rows
totally, so it is layout-independent: it is unchanged by compaction or
settle and equal across independent loads. The engine computes it either in
memory or as a streaming external merge sort bounded by a byte budget
(`--streaming`), with identical results.

## Verification

`gnomon store verify` checks every file the current generation names against
its recorded checksums. `--full` (or `gnomon check`) adds the cross-file
invariants: the manifest hash chain across retained generations, dictionary
references, the bi-temporal row invariants, the event log's framing and hash
chain against the applied offset, and the persisted adjacency index. It is
read-only; a damaged store is rebuilt from its event log with
`gnomon replay`.
