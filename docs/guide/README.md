# Gnomon user guide

This guide teaches you to use Gnomon, a storage engine for bi-temporal
property graphs, from a first store to day-to-day operation. Every code
example in it is a file in [`examples/`](../../examples) that you can run,
and every output shown is copied from a real run of that file. The test
`tests/test_examples.py` runs each example and checks that it still prints
exactly what this guide shows.

## Reading order

| page | read it when you want to |
|---|---|
| [Getting started](getting-started.md) | build the package, open a store, write a fact, correct it, and ask "now" and "as believed then" |
| [Concepts](concepts.md) | understand valid time and belief time, versions and close records, `OPEN_END`, time units, identities, what "as of" means for each operator, and the cost check |
| [Loading data](loading-data.md) | write single facts, bulk batches and event streams; correct and retract; rebuild a store from an event log, including one with your own belief times |
| [Querying](querying.md) | ask questions: snapshots, paths and reachability, histories and belief diffs, counts and series, name search, patterns, pagination, and the cost check |
| [Worked scenarios](scenarios.md) | see three complete studies: a payment audit, an entity-resolution merge and its reversal, and a revision-history study |
| [Command line](cli.md) | load, query, check and maintain a store from the shell |
| [Operations](operations.md) | run a store in practice: layout, durability, readers and the writer, compaction, settle and gc, backup, configuration and performance tips |
| [Python API reference](api-reference.md) | look up the public entry points, their parameters and what they return |
| [FAQ and troubleshooting](faq.md) | fix a build, a refusal, or a wrong answer caused by time units or identities |

New users: read Getting started, then Concepts, then Querying. The
scenarios show how the pieces fit together.

## The examples

| file | what it shows | guide page |
|---|---|---|
| [`getting_started.py`](../../examples/getting_started.py) | a first store: write, correct, retract, ask twice, open read-only | [Getting started](getting-started.md) |
| [`loading_data.py`](../../examples/loading_data.py) | single writes, a bulk batch with labelled nodes, `disc`, corrections and retractions, refusals | [Loading data](loading-data.md) |
| [`query_snapshots.py`](../../examples/query_snapshots.py) | `snapshot_subgraph`, `diff_snapshots`, `neighborhood_evolution`, windowed `entity_history` | [Querying](querying.md#snapshots) |
| [`query_paths.py`](../../examples/query_paths.py) | `temporal_reachability`, `temporal_paths`, `paths` | [Querying](querying.md#paths-and-reachability) |
| [`query_history.py`](../../examples/query_history.py) | `entity_history`, `version_history`, `diff_belief` | [Querying](querying.md#history-and-belief) |
| [`query_aggregates.py`](../../examples/query_aggregates.py) | `aggregate_events`, `graph_metric_timeseries`, `state_series`, `burst_detection`, motifs, `co_active` | [Querying](querying.md#counts-series-and-windows) |
| [`query_patterns.py`](../../examples/query_patterns.py) | `resolve_entities`, `lookup_name`, `match`, `compute`, cursors | [Querying](querying.md#name-search) |
| [`cost_check.py`](../../examples/cost_check.py) | a refusal by the cost check, and how to narrow, raise or skip | [Querying](querying.md#the-cost-check) |
| [`scenario_payment_audit.py`](../../examples/scenario_payment_audit.py) | what was known when an alert was cleared | [Scenarios](scenarios.md#a-payment-audit) |
| [`scenario_entity_resolution.py`](../../examples/scenario_entity_resolution.py) | two records merged into one person, then split again | [Scenarios](scenarios.md#entity-resolution) |
| [`scenario_revision_history.py`](../../examples/scenario_revision_history.py) | an analysis reproduced at a past revision, with dated belief times | [Scenarios](scenarios.md#a-revision-history-study) |
| [`cli_tour.py`](../../examples/cli_tour.py) | every main `gnomon` subcommand | [Command line](cli.md) |

Each example is self-contained. It writes its store under a temporary
directory, removes it at the end, and needs nothing but the installed
package. Run one with:

```bash
python examples/getting_started.py
```

Run them all, with their output checks:

```bash
python -m pytest tests/test_examples.py
```

## Reference pages

The guide links to the reference pages in [`docs/`](..) where they hold the
detail: [data model](../data-model.md), [operators](../operators.md),
[architecture](../architecture.md), [configuration](../configuration.md),
[install](../install.md), [testing](../testing.md) and
[limitations](../limitations.md).
