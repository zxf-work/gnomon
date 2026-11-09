# Getting started

This page takes you from a clone of the repository to a store that answers
the same question two ways: as the data stands now, and as it was believed
at an earlier time.

## 1. Build and install

Gnomon is a Python package over a Rust engine. You build it from source.
You need Rust (the version is pinned in `rust-toolchain.toml`; `rustup`
installs it on first use), Python 3.11 or newer, a C toolchain for linking,
and maturin. It is developed and tested on Linux x86-64.

```bash
git clone https://github.com/zxf-work/gnomon
cd gnomon
python3 -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop --release --extras dev
python -c "import gnomon, gnomon._engine; print(gnomon.__version__)"
```

`maturin develop` compiles the engine and installs the package into the
virtual environment. The `dev` extra adds the test tools. The `gnomon`
command is installed at the same time. See [install](../install.md) for
wheels and build features, and the [FAQ](faq.md#build-and-platform) if the
build fails.

## 2. A first store

A store is a directory. `gnomon.open` creates it if it does not exist.
Every write is one batch and returns that batch's **belief time**: the
time at which the store started to believe what you wrote.

The full program is [`examples/getting_started.py`](../../examples/getting_started.py).
Its main steps:

```python
import gnomon
from gnomon import OPEN_END, EntityRef
from gnomon.temporal.algebra import call_operator, ensure_all_registered

ensure_all_registered()               # once per process, before call_operator

store = gnomon.open(path)             # a directory; created if absent

# Facts, with their valid time. Times are int64 microseconds.
t1 = store.assert_node("alice", "Person", {"name": "Alice", "city": "Lyon"}, vt_s=0)
store.assert_node("bob", "Person", {"name": "Bob", "city": "Lyon"}, vt_s=0)
t2 = store.assert_edge("alice", "bob", "KNOWS", {"since": 2019}, vt_s=100)

# A correction: from valid time 300 on, alice lived in Paris.
t3 = store.correct(EntityRef("node", uid="alice"),
                   {"name": "Alice", "city": "Paris"}, vt_s=300)
```

Questions go through `call_operator(store.adapter, <operator>, <args>)`.
Each operator takes `as_of_tt`, the belief time to answer at. The default,
`OPEN_END`, means "the current beliefs":

```python
env = call_operator(store.adapter, "entity_history",
                    {"uid": "alice", "as_of_tt": t2})
env["rows"]        # alice's versions as believed at t2
```

The example then retracts the `KNOWS` edge at valid time 500, asks who
alice is linked to at valid times 400 and 600, closes the store, and opens
it again read-only. Its output:

<!-- example-output: getting_started.py -->
```text
belief times increase: True
alice now:            [(0, 300, 'Lyon'), (300, 'open', 'Paris')]
alice as believed t2: [(0, 'open', 'Lyon')]
city at valid time 200: Lyon
friends at 600, now:            []
friends at 600, as believed t3: ['bob']
friends at 400, now:            ['bob']
reader sees the retraction: True
reader stats: {'n_node_versions': 4, 'n_edge_versions': 2}
```

What happened:

* **Now**, alice lived in Lyon over `[0, 300)` and in Paris from 300 on.
  `open` stands for `OPEN_END`: the interval has no end yet.
* **As believed at `t2`**, before the correction, she lived in Lyon from 0
  on. The correction did not overwrite that version. It closed it in belief
  time and wrote two new versions: Lyon over `[0, 300)` and Paris from 300.
* The retraction ended the edge at valid time 500. At valid time 600, alice
  has no friend now, but she had one as believed at `t3`, before the
  retraction. At valid time 400 the edge still holds.
* The store counts four node versions: alice's original version, her two
  corrected versions, and bob's. Superseded versions stay in the store.

## 3. Readers and the writer

One process at a time may write a store; it holds `<store>/writer.lock`.
Any number of processes may read it at the same time:

```python
reader = gnomon.open(path, read_only=True)
```

A read-only handle never writes under the store directory and never blocks
the writer. Close every handle with `store.close()`.

## 4. The same store from the shell

The `gnomon` command works on the same directory:

```bash
gnomon call --store people entity_history '{"uid": "alice"}'
gnomon store verify --store people --full
```

See [Command line](cli.md).

## Next

* [Concepts](concepts.md): the two clocks, versions, and what "as of" means.
* [Loading data](loading-data.md): bulk loads, corrections, retractions.
* [Querying](querying.md): every operator family, with examples.
