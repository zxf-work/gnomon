# Gnomon

Gnomon is a storage engine for **bi-temporal property graphs**. Every node
and edge version carries two independent time intervals: *valid time* — when
the fact held in the world — and *belief time* (transaction time) — when the
store believed it. Corrections never overwrite: they are appended, and the
superseded belief stays queryable. Any question can therefore be asked
"as the data stands now" or "as we believed it at time T", and the two
answers are both exact and reproducible.

The engine is written in Rust (`crates/gnomon-core`) with a thin Python
layer (`gnomon`) for the write semantics, the operator registry and the
tooling.

## Contents

- [Why](#why)
- [Highlights](#highlights)
- [Install](#install)
- [Quickstart](#quickstart)
- [Command line](#command-line)
- [Documentation](#documentation)
- [Repository layout](#repository-layout)
- [Tests](#tests)
- [Benchmarks](#benchmarks)
- [Limitations](#limitations)
- [License](#license)

## Why

Graphs built from logs, feeds and records are revised after the fact: a
message is re-attributed, a relationship is found to have ended earlier
than recorded, a property was wrong for a month. A store that overwrites
loses the ability to explain a past answer; a store that keeps snapshots
pays for every copy and still cannot say *when* a fact became known.
Gnomon keeps one immutable history in which every version records both
when it was true and when it was believed, so:

* a correction is an append, not an update, and can always be audited;
* "what did we believe last Tuesday about March?" is an ordinary query;
* answers pinned to a past belief state are byte-identical forever, even
  as the store keeps changing.

## Highlights

* **Immutable columnar segments** with per-block compression (frame of
  reference and linear frame of reference), zone maps, CRC32C per column
  and a SHA-256 per file; readers `mmap` them without locks.
* **Corrections as appended close records.** Ending a belief writes a small
  immutable close run; compaction later folds it into the segment. No row
  is ever dropped.
* **Atomic generations.** Each commit publishes a new manifest generation
  (checkpoint-plus-delta chain, Merkle-rooted) and flips one pointer;
  readers pin a generation and see a consistent snapshot.
* **Write-ahead event log** with a rolling hash chain; recovery replays
  exactly the unapplied suffix.
* **Size-tiered incremental compaction**, a one-shot `settle` for
  read-optimised layouts, and generation-aware garbage collection.
* **Per-segment indexes** that are rebuildable and never authoritative:
  temporal adjacency (CSR), a sorted name index, and an exact-negative
  identity filter that keeps corrections from scanning the whole store.
* **Fifteen temporal graph operators** — histories, snapshots, diffs,
  time-respecting paths and reachability, motif counting, grouped
  aggregation, burst detection, interval joins — each schema-validated,
  paginated, deterministic, and evaluable at any belief time.
* **Two digests:** a store digest for exact identity and a content digest
  that two independent loads of the same data agree on (computable in
  bounded memory).

## Install

Requirements: Rust 1.98.1 (pinned in `rust-toolchain.toml`), Python 3.11+,
and maturin. Full instructions: [docs/install.md](docs/install.md).

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop --release --extras dev
```

## Quickstart

Save this as `quickstart.py` and run it with the package installed. It
ingests a few edges, applies a correction, and asks the same question of the
current beliefs and of an earlier belief state:

```python
import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()
store = gnomon.open("quickstart-store")  # a directory; created if absent

# alice lives in Lyon from time 0 on (times are int64 microseconds)
t1 = store.assert_node("alice", "Person", {"city": "Lyon"}, vt_s=0)

# a few edges, ingested as one batch
store.ingest_events([
    {"src": "alice", "dst": "bob", "rel_type": "KNOWS", "vt_s": 100, "vt_e": 900},
    {"src": "bob", "dst": "carol", "rel_type": "KNOWS", "vt_s": 200, "vt_e": 900},
])

# a correction: we learn that alice moved to Paris at time 300
t2 = store.correct(EntityRef("node", uid="alice"), {"city": "Paris"}, vt_s=300)


def cities(as_of_tt=OPEN_END):
    env = call_operator(store.adapter, "entity_history",
                        {"uid": "alice", "as_of_tt": as_of_tt})
    return [(r["vt_s"], "open" if r["vt_e"] == OPEN_END else r["vt_e"],
             r["props"]["city"]) for r in env["rows"]]


print("current beliefs:      ", cities())
print("as believed at t1:    ", cities(t1))

snap = call_operator(store.adapter, "snapshot_subgraph",
                     {"seeds": ["alice"], "t_valid": 500, "hops": 2})
print("2 hops from alice at 500:", sorted(n["uid"] for n in snap["nodes"]))

fixed = call_operator(store.adapter, "version_history",
                      {"kind": "node", "window": {"t_a": 0, "t_b": 1000},
                       "belief": "superseded"})
print("superseded node versions:", fixed["rows_total"])
store.close()
```

Output:

```
current beliefs:       [(0, 300, 'Lyon'), (300, 'open', 'Paris')]
as believed at t1:     [(0, 'open', 'Lyon')]
2 hops from alice at 500: ['alice', 'bob', 'carol']
superseded node versions: 1
```

The correction did not overwrite anything: the version believed at `t1`
(Lyon from 0 onwards) is still in the store, closed in belief time, and
answers every question asked "as of `t1`". The store is an ordinary
directory and can be inspected with the CLI:

```bash
gnomon store verify --store quickstart-store --full
gnomon call --store quickstart-store entity_history '{"uid": "alice"}'
```

[docs/data-model.md](docs/data-model.md) walks through the same ideas with
a correction and a retraction on an edge.

## Command line

The `gnomon` command covers loading and store maintenance:

```bash
gnomon ingest events.jsonl --store mystore      # bulk-load an event stream
gnomon replay old/eventlog.jsonl --store copy    # rebuild a store from its event log
gnomon call --store mystore entity_history '{"uid": "alice"}'
gnomon store compact --store mystore             # incremental compaction
gnomon store settle  --store mystore             # one-shot read-optimising merge
gnomon store gc      --store mystore --keep 2    # reclaim superseded files
gnomon store digest  --store mystore [--streaming]   # content digest
gnomon store verify  --store mystore [--full]    # integrity check (also: gnomon check)
gnomon store build-indexes --store mystore       # pre-build per-segment indexes
gnomon store backup  --store mystore --dest bak  # quiesced copy + manifest
gnomon store restore --dest bak --store restored # replay a backup and verify it
```

`gnomon <command> --help` documents every option.

## Documentation

| document | contents |
|---|---|
| [docs/data-model.md](docs/data-model.md) | valid time and belief time, versions, assert / retract / correct, current vs as-of reads, a worked example |
| [docs/architecture.md](docs/architecture.md) | store layout, commit protocol, segments and time encoding, close runs, manifest generations, compaction / settle / gc, per-segment indexes, identity filter, digests |
| [docs/operators.md](docs/operators.md) | every operator: what it answers, its parameters, which clock it ranges over |
| [docs/install.md](docs/install.md) | toolchain, building the engine and the Python package, wheels, build features |
| [docs/testing.md](docs/testing.md) | the Rust and Python test suites |
| [docs/benchmarks.md](docs/benchmarks.md) | the synthetic generator, ingest timing with per-phase breakdown, the query-registry runner, small and large runs |
| [docs/configuration.md](docs/configuration.md) | environment variables that tune the engine |
| [docs/limitations.md](docs/limitations.md) | scope and known open items |

## Repository layout

```
crates/gnomon-core/   the engine (Rust, no Python dependency)
crates/gnomon-py/     PyO3 binding, built as gnomon._engine
gnomon/               Python package: Store, storage interface, native adapter,
                      DuckDB reference backend, operators, CLI
scripts/              benchmark drivers: synthetic store builder, query-registry
                      harness, event-log freezer, receipt checker
benchmarks/schema/    JSON Schema for benchmark receipts
tests/                Python test suite (the Rust tests live with the crates)
docs/                 documentation
```

## Tests

```bash
cargo test --release
python -m pytest tests
```

Details, including the timer-free build and the reference-backend run, are
in [docs/testing.md](docs/testing.md).

## Benchmarks

```bash
# build a synthetic store of 100k entities, timing ingest per phase
python scripts/build_synth_store.py --n-entities 100000 --out runs/synth-100k
# run the 19-query registry against it
python scripts/eval_harness.py --store runs/synth-100k --log runs/synth-100k/eventlog.jsonl --json runs/q-100k.json
```

Sizes, protocol and output fields: [docs/benchmarks.md](docs/benchmarks.md).

## Limitations

Single node, single writer, explicit maintenance, no query language. See
[docs/limitations.md](docs/limitations.md) for the full list and the known
open items.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
