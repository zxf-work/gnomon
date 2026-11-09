# FAQ and troubleshooting

- [Build and platform](#build-and-platform)
- [Time units](#time-units)
- [Writes and identities](#writes-and-identities)
- [Readers and the writer](#readers-and-the-writer)
- [Answers and refusals](#answers-and-refusals)

## Build and platform

**Which platforms are supported?**
Gnomon is developed and tested on Linux x86-64. It assumes a local POSIX
file system ([limitations](../limitations.md#scope)). Windows is not
supported: the writer lock uses `fcntl`. Other systems are not tested.

**`maturin develop` says it cannot find a virtual environment.**
Create and activate one first:
`python3 -m venv .venv && . .venv/bin/activate`, then
`pip install maturin` and `maturin develop --release --extras dev`.

**The build cannot find `cargo` or the pinned Rust version.**
Install `rustup`. The repository pins its Rust version in
`rust-toolchain.toml`, and `rustup` downloads that version the first time
you build in the checkout.

**The build fails at the link step (`linker 'cc' not found`).**
Install a C toolchain, such as `build-essential` on Debian or Ubuntu.

**`pip` refuses to install: Python is too old.**
Gnomon needs Python 3.11 or newer.

**`ImportError: ... gnomon._engine`.**
The compiled engine is missing from the environment you are running. Run
`maturin develop --release` in the activated virtual environment. After
changing Rust code, run it again; Python changes need no rebuild.

**Do I need the `dev` extra?**
Only to run the tests. A wheel (`maturin build --release`) needs no Rust
toolchain to install. See [install](../install.md).

## Time units

**What unit are times in?**
`int64` microseconds since the Unix epoch, UTC, for every valid time,
belief time, window, stride and `delta`. One second is `1_000_000`.

**I passed `time.time()` and got nonsense.**
`time.time()` returns seconds as a float. Use `int(time.time() * 1_000_000)`,
or convert dates as in [Concepts](concepts.md#time-units-and-open_end).
Times from JavaScript or many databases are milliseconds: multiply by
1,000. Operator arguments must be integers; a float is refused with
`E_SCHEMA`.

**My window misses the last event.**
Windows are half-open: `{"t_a": 0, "t_b": 1000}` holds `0` to `999`. An
event at 1000 is outside.

**A bulk event has no `vt_e`. How long does it last?**
One microsecond: `vt_e = vt_s + 1`. A snapshot at `t_valid = vt_s` sees it;
one at `vt_s + 1` does not. To correct such an event, use the interval
`[vt_s, vt_s + 1)`.

**`as_of_tt` did not filter anything.**
`as_of_tt` selects a belief state, not a valid time. To ask about the world
at a time, use `t_valid` or `window`. To ask what was believed at a time,
use `as_of_tt`. They combine.

**I asked for an `as_of_tt` in the future.**
The answer is served at the store's current belief time, and the envelope
says `"clamped": true`. Later writes can still change it. Pin a belief time
the store has reached for an answer that never changes.

**How do I get belief times that match my real history?**
A live store assigns belief times when it commits. To load history with the
dates it was recorded, write an event log with those times and replay it;
see [Loading data](loading-data.md#your-own-belief-times).

**A series call fails with `E_LIMIT: bucket count ... exceeds cap 2000`.**
`window` divided by `stride` is more than 2,000 buckets. Use a larger
`stride`; the message says how large.

## Writes and identities

**My edge is in the store, but snapshots and paths do not show it.**
`assert_edge` does not create node versions for its endpoints, and a
snapshot edge needs both endpoints in the snapshot. Assert the nodes, or
load edges with `ingest_events`, which creates missing endpoints. See
[Loading data](loading-data.md#endpoints-are-not-created-by-assert_edge).

**A correction lost some of my properties.**
A new version carries exactly the props you pass. Pass the whole property
object to `correct` and `assert_node`.

**`correct` or `retract` fails with `E_NOT_FOUND`.**
There is no believed version to act on: the identity is wrong, the valid
time misses it, or it was already retracted. For an edge, check `disc`:
`assert_edge` uses `""`, and bulk events without a `disc` are `"#0"`,
`"#1"`, and so on. `version_history` with `ref` shows an identity's rows.

**Events loaded on different days merged into one edge.**
Bulk events without a `disc` are named by position, and the count starts
at `"#0"` each time a store is opened. Two events with the same `src`,
`dst` and `rel_type` loaded in different sessions can get the same name.
Give events a `disc`, such as a transaction id.

**`ingest_events` refuses my node records with `E_INVALID_ARG`.**
A node record in a bulk batch may not supersede: the uid already has a
believed version. Change existing nodes with `assert_node` or `correct`.

**How do I delete a fact that was never true?**
Retract it at its own `vt_s`: `store.retract(ref, vt_s)`. It is withdrawn
over its whole valid interval from now on, and stays visible to questions
asked as of earlier belief times. Gnomon never deletes history.

**`entity_history` fails with `E_NOT_FOUND: unknown uid`.**
No version of that uid was ever written. Use `resolve_entities` to find
uids by name.

## Readers and the writer

**`WriterLockedError: another process is already the writer`.**
Only one process may open a store for writing. Open every other process
with `gnomon.open(path, read_only=True)`. The maintenance commands
`compact`, `settle` and `gc` are writers too.

**My reader does not see new writes.**
A read-only handle keeps the generation it opened. Close it and open a new
one to see later commits.

**A crashed writer left `writer.lock` behind.**
The lock is held by the process, not by the file. When the process exits,
the lock is released, and the next writer opens normally and recovers any
batch the crash left unapplied.

## Answers and refusals

**A call is refused with `E_COST`.**
The cost check estimated the call above its budget. Narrow it (window,
seeds, hops, `rel_types`), or from Python pass `cost_ceilings` or
`skip_cost_check=True`. `gnomon call` has no option for this. See
[Querying](querying.md#the-cost-check).

**`result_digest` differs between two stores loaded from the same data.**
Belief times differ between independent loads, and so do the version ids
derived from them. Answers that include belief times or version ids differ
too. Compare stores by content digest (`gnomon store digest`), or build
both from one event log with `gnomon replay`.

**Rows with equal keys come back in a different order on another load.**
Some operators break ties by version id, which depends on belief time. Sort
on the fields you care about when you compare across loads.

**Are float properties exact?**
They are stored as written. Envelopes round floats to nine decimals before
computing `result_digest`, so digests agree across platforms. The `sum`
aggregate is exact: integers stay integers, and float sums are correctly
rounded.
