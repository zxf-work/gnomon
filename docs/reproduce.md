# Reproducing the evaluation

This page reruns the evaluation's comparison: Gnomon against DuckDB, PostgreSQL,
ClickHouse, Neo4j, Memgraph, XTDB, LadybugDB, AeonG and Raphtory, and
against its own operators on DuckDB storage, over five workloads. The
systems, the protocol and the layout of the kit are in
[benchmarks](benchmarks.md#the-comparison-kit).

There are two ways to run it:

* **one point on one machine**, with Docker: every step of the kit at a small
  size, which shows the kit working and the systems agreeing on every
  answer (minutes to hours);
* **the full stages on a Slurm cluster**, with Apptainer: the jobs that
  produced the evaluation's numbers (whole nodes, days of node time).

## Install

Build the engine and the comparison kit's Python packages in a virtual
environment at the root of the checkout (the kit looks for `.venv` there):

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop --release --extras dev,eval
pip install -r <(grep -v '^#' benchmarks/compare/kit/python.lock)   # LadybugDB, Raphtory
```

Servers run in containers. The images are pinned by digest in
`benchmarks/compare/kit/images.lock` (PostgreSQL 16.15, ClickHouse 26.8.1,
Neo4j 5.26.31 community, Memgraph 3.12.0, XTDB 2.1.0, and AeonG's authors'
toolchain image). AeonG itself is built from its source by
`benchmarks/compare/kit/build_aeong.sh` (Docker) or
`benchmarks/compare/kit/apptainer/aeong.def` (Apptainer), at the commit and
with the three patches in `benchmarks/compare/kit/aeong/`.

## One point on one machine

W1 (uniform graph) at 1M edge versions, at 0 and 10 % corrections, with
Gnomon, Gnomon on DuckDB storage, PostgreSQL and DuckDB (SQL). Docker must be
able to run the PostgreSQL image of `images.lock`.

```bash
export EXTCMP_ROOT=$PWD/../extcmp-work      # records, stores, server data
export EXTCMP_REPO=$PWD                     # this checkout, with .venv built
# freeze the 1M log, inject 10 % corrections, load, and check every answer
EXTCMP_SYSTEMS=native,duckdb,postgres \
  benchmarks/compare/kit/run_beliefq.sh --commit HEAD --sizes 1000000 --densities 0,10 \
    --rotations 1 --steps freeze,inject,loads,verify
# DuckDB (SQL) on the same two logs, checked against Gnomon
benchmarks/compare/kit/run_duckdbsql.sh --points "K1M-d0 K1M-d10" --steps loads,verify
```

Each point's answer check is in `$EXTCMP_ROOT/results/<point>/verdicts.txt`:
one line per system and query, `equivalent` when the system's hashed answer
is Gnomon's, `not expressible` where a system has no form of the query (the
belief diff on Gnomon-on-DuckDB storage, for example). Add `timed,tables` to the steps (and `--rotations 2`) for timings;
on a shared machine they are not comparable with the evaluation's. Without
`EXTCMP_SYSTEMS`, every system runs, AeonG included; `run_campaign.sh` runs
the registry families the same way.

## Layout on the host

A cluster run keeps everything under one directory, `$GNOMON_BENCH_ROOT`.
Each stage has its own directory there, its own clone of this repository
(`src/`, built in place) and its own `site.env`, copied from
`benchmarks/site.env.example`:

| directory under `$GNOMON_BENCH_ROOT` | what |
|---|---|
| `final/w1`, `final/w2`, `final/w3`, `final/thr` | the W1-W3 timed stages and the thread-matched control |
| `confirm/w1`, `confirm/w2`, `confirm/w3` | the Gnomon-only confirmation mode of the same stages |
| `studies/<study>` | W4, W5 and the studies (`benchmarks/stages/studies/<study>`) |
| `v3/eventlogs` | the frozen W1 logs, `SHA256SUMS` = `benchmarks/compare/eventlogs/SHA256SUMS-W1` |
| `w2-powerlaw`, `w3-snb`, `w4-finbench`, `w5-wikidata` | the workloads' data, logs, stores and parameter files |
| `sif/` | the Apptainer images (`SIF_DIR`) |

`benchmarks/compare/kit/stage_hpc.sh` prepares a login node with internet
access: the Rust toolchain and `uv`, the repository, the Python packages, and
every image of `images.lock` pulled by digest into `SIF_DIR`. Compute nodes
need no network.

Jobs take the account and partitions from the environment
(`SBATCH_ACCOUNT`, and `SBATCH_PARTITION_SHARED`, `_WHOLE`, `_BIGMEM` for the
three node classes of `benchmarks/compare/kit/slurm/stages.tsv`); `sbatch -p`
and `-A` on the command line work as usual. Whole-node jobs ask for
`--exclusive` and 128 tasks. Node-local scratch is `$LOCAL` (the
scheduler's, else `$TMPDIR`, else `/tmp`). Each stage guard refuses to run
unless the engine build matches `EXTCMP_ENGINE_SO_SHA256`
(`sha256sum src/gnomon/_engine*.so`) and the stage's clone has no local
changes.

## The workloads

### W1 uniform graph and W2 power-law graph

Both are synthetic and deterministic.

```bash
python scripts/freeze_synth_log.py --scale 1000000 --out eventlogs/synth-1000000.eventlog.jsonl
python scripts/freeze_powerlaw_log.py --scale 1000000 --gamma 2.1 --seed 2101 \
    --out eventlogs/powerlaw-1000000.eventlog.jsonl
```

Corrections are injected into a replayed store by
`scripts/inject_corrections.py` (seed 0, batch 1000; `run_beliefq.sh` does it
for W1, `benchmarks/compare/w2/jobs/prep.sbatch` for W2). The frozen logs'
sha256 are in `benchmarks/compare/eventlogs/`: a regenerated log should match.
The parameter files are the evaluation's: W1's in `benchmarks/compare/p1/`
(curated instances) and `benchmarks/compare/params/w1-single/`
(single-parameter instances; copy them to `final/w1/params-single`), W2's in
`benchmarks/stages/w2/params/` (hub stratum and curated instances; built by
`benchmarks/compare/w2/jobs/params.sbatch`).

### W3 SNB-derived

| input | version | source |
|---|---|---|
| LDBC SNB Datagen | 0.5.1, image `ldbc/datagen-standalone:0.5.1-2.12_spark3.2` by digest | pulled by `benchmarks/workloads/snb/jobs/datagen.sbatch` |
| LDBC SNB BI parameter generator | `https://github.com/ldbc/ldbc_snb_bi` at `47dd38b40844ecdb0e42e5a610c369535304786d` | clone into `w3-snb/ext/ldbc_snb_bi` |
| LDBC SNB Interactive v2 parameter generator | `https://github.com/ldbc/ldbc_snb_interactive_v2_driver` at `30a73a28614b3da326c851d7833ced9b17f11987` | clone into `w3-snb/ext/ldbc_snb_interactive_v2_driver` |

The jobs, in order: `datagen.sbatch` (SF1 and SF10, raw mode with factor
tables and BI mode), `freeze.sbatch` (the replayed history with commit times
and the injected correction streams, as frozen logs), `paramgen.sbatch`
(LDBC's own curation on the factor tables) and `msgparams.sbatch`, then the
store builds of `benchmarks/stages/stores/jobs/w3.sbatch` (Gnomon replay, the
version rows, the history tables, the DuckDB check database) and
`benchmarks/workloads/snb/jobs/ingest.sbatch` (DuckDB, PostgreSQL, Neo4j,
Memgraph). The query texts per system are in
`benchmarks/workloads/snb/{queries,cypher}`, the timed parameter set in
`benchmarks/stages/w3/params/`.

### W4 FinBench-derived

| input | version | source |
|---|---|---|
| LDBC FinBench datagen | 0.1.0, `https://github.com/ldbc/ldbc_finbench_datagen` at `eddcc0551861eaefeb9b37497b10de1bb0f52672` | clone into `w4-finbench/downloads/ldbc_finbench_datagen`; built with Maven and Java 8 by the job |
| Apache Spark | 3.2.2 for Hadoop 3.2, `https://archive.apache.org/dist/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz`, sha256 `1ce4dfdf30bf3f6bc6d06bc7b9f34da333db4345e7f48e894d1bc0207775f59b` | into `w4-finbench/downloads/` |

`benchmarks/compare/w4/jobs/datagen.sbatch` builds the datagen and generates
SF1 and SF10; `convert.sbatch` turns the raw data into frozen logs at the
correction densities; `benchmarks/stages/stores/jobs/w4.sbatch` builds the
stores of every system and checks them against each other. The parameter
file of the evaluation is `benchmarks/compare/params/w4/params-sf10.json` (copy it
to `w4-finbench/params/`; its `meta` field names the log's metadata file by
its base name).

### W5 Wikidata revision history

| input | version | source |
|---|---|---|
| Wikidata page histories | the 2019-01-01 dump, item `wikidatawiki-20190101` of the Internet Archive | `https://archive.org/download/wikidatawiki-20190101/`; the item's file list (`https://archive.org/metadata/wikidatawiki-20190101`, saved as `w5-wikidata/raw/ia-wikidatawiki-20190101.json`; ours had sha256 `0e167cbaa66e20dfb2d68fa6362b2c19e891c4282e6fdb83ffa011f022ec3a65`) gives each file's md5 |
| TGB `tkgl-wikidata` edge list | the Temporal Graph Benchmark's release | downloaded by `benchmarks/workloads/wikidata/jobs/fetch_tgb.sbatch` |

The jobs, in order: `fetch.sbatch <minpage> <maxpage>` (the history files of
a page range, md5-checked against the file list), `extract.sbatch`,
`buildlog.sbatch` (the frozen log of the entity set), `digest.sbatch`, then
the answer check `w5_check.sh` through `benchmarks/compare/kit/slurm/job_cmd.sbatch`.
The frozen log's sha256 is in `benchmarks/workloads/wikidata/SHA256SUMS-W5-wikidata-v2`;
the evaluation's parameter files are in `benchmarks/compare/params/w5/` (copy them
to `w5-wikidata/params-c3/`).

## The stages on a cluster

Each workload runs as a gate job (untimed: loads, persisted stores, every
answer compared) and then timed jobs, one point per whole node:

| workload | gate | timed | points |
|---|---|---|---|
| W1 | `stages/w1/jobs/w1_gate.sbatch` (`W1_POINTS`) | `stages/w1/jobs/w1_timed.sbatch` (`W1_POINTS`, `W1_GATE`) | `K1M K10M K10M-d0 K10M-d1 K10M-d10`; `w1_d10build.sbatch` first builds the Gnomon-on-DuckDB-storage store of `K10M-d10` |
| W2 | `stages/w2/jobs/w2_gate.sbatch` (`W2_POINTS`) | `stages/w2/jobs/w2_timed.sbatch` | `K1M K1M-d0 K1M-d1 K1M-d10 K10M K10M-d0 K10M-d1 K10M-d10` |
| W3 | `stages/w3/jobs/w3_gate.sbatch` (`W3_GATE_POINT`, `W3_GATE_SYS`), then `w3_verdict.sbatch` | `stages/w3/jobs/w3_timed.sbatch` (`W3_POINT`) | `SF1`, `SF10`; AeonG: `w3_aeong.sbatch` |
| W4 | `stages/studies/w4/jobs/w4.sbatch` (`W4_STEPS=gate,timed`) | the same job | one job per correction density |
| W5 | `stages/studies/w5/jobs/w5.sbatch` | the same job | the Wikidata log |
| Raphtory | `stages/studies/raphtory/jobs/raphtory.sbatch` (`RAP_MODE=gate`) | the same job, `RAP_MODE=timed`, `RAP_GATE` | `RAP_POINTS` (W1, W2, W5) |

All paths are under `benchmarks/`. Every job's header lists its variables,
exit codes and the node it needs. The studies behind the evaluation's other
sections are under `benchmarks/stages/studies/`, one directory each, with a
job file and the tools that read its records:

| study | directory |
|---|---|
| design ablations, W1 and W2; fixed-window configurations | `abl/`, `abl4/` (configurations: `benchmarks/compare/kit/lib/ablation_configs.json`) |
| components, W3 and W5 | `w3abl/`, `w5abl/` |
| W3 stores by correction density, lag and history length | `w3d/`; DuckDB with 64 GB: `w3d64/` |
| peak memory, store sizes, the writer path, commit cost | `mem/`, `sizes/`, `sizes2/`, `writer/`, `commit/` |
| ingest | `ingest/` |
| 100M edge versions | `t10/`, `covy/`, `s100m/` |
| out-of-order logs | `ooo/` |
| whole answers against DuckDB and PostgreSQL | `fullsets/`, `fullsets-x/` |
| the general match path against the dedicated operators | `matchpath/` |
| where the time goes (DuckDB plans), warm open, mixed concurrency | `causes/`, `warm/`, `conc/` |
| Raphtory's two forms | `raphtory/`, `raphlike/` |
| thread-matched control | `benchmarks/stages/threads/` |

Several report tools read the records of earlier runs by directory name.
Those names carry a run label (`J01` ... `J66`) where the evaluation's runs
had the scheduler's job id, for example `final/w1/results/gate-J19`. Name the
result directory of the corresponding run the same way, or edit the path in
the tool. The labels the tools use:

| label | run (directory as the tools read it) |
|---|---|
| J04 | `w5-wikidata/runs/W5-wikidata-v2-J04`: the W5 log build |
| J19, J20 | `final/w1/results/gate-J19`, `timed-J20`: W1 gate and timed |
| J21, J22 | `final/w2/results/gate-J21`, `gate-J22`: W2 gates |
| J23, J24, J25 | `final/w2/results/timed-J23` ... `timed-J25`: W2 timed |
| J26, J27, J33 | `final/w3/results/gate-J26`, `gate-J27`, `rerun-J33`: W3 SF1 gate |
| J28, J30, J34, J36, J37, J39 | `final/w3/results/gate-J34`, ...: W3 SF10 gate and its extensions |
| J31, J40, J41 | `final/w3/results/timed-J31` (SF1), `timed-J40` (SF10), `etime-J41` (SF1 engine time) |
| J38 | `final/w3/results/aeong-J38`: W3 AeonG |
| J42, J43 | `confirm/w1/results/gate-J42`, `timed-J43` |
| J44, J45, J46 | `confirm/w2/results/gate-J44`, `timed-J45`, `timed-J46` |
| J47 | `confirm/w3/results/timed-J47` |
| J48 | `studies/w4/results/sf10-r10-J48` |
| J49 | `studies/w5/results/W5-wikidata-v2-J49` |
| J50 | the W4 store rebuild the warm-open study checks against |
| J51 | the timed ingest study |
| J52 | `logs-reordered/J52`: the out-of-order logs |
| J53 | `studies/mem/results/w2-J53` |
| J54, J57, J58 | `fullsets-J54`, `fullsets-J57`, `fullsets-J58` |
| J55 | `studies/t10/results/timed-w2-J55` |
| J56 | `k100m/results/timed-k100m-J56` |
| J59 | `matchpath/results/timed-J59` |
| J60 | `k100m-logsrc/results/merged4-J60` |
| J61 ... J66 | `s100m-a/results/timed-s100m-J61` ... `J66`: the 100M points |

## Versions

The engine is this repository. The W1-W3 stage scripts are the versions that
timed Gnomon in the evaluation; the other systems' timings came from earlier
versions of the same scripts, which differ by a Gnomon-only mode and a drift
sample that stay off unless `C3_CONFIRM=1`
([benchmarks](benchmarks.md#the-comparison-kit)).
