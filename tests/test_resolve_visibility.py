"""Entity resolution pays for the rows it still believes, not for the store.

`resolve_entities` was the last whole-node-store belief sweep on the
measured read path. It opened every node segment and asked `believed_at` about
every stored row — a `tt_s_runs` binary search and a close probe each — before
it could decide whether the row was even a candidate. Its cost was therefore
O(stored node versions) whatever the answer contained, and in particular it
did not fall when corrections superseded most of the store: a superseded row
was *visited and skipped*, which is strictly more work than a believed one
that matches nothing.

It now runs on the same `ScanSet` the edge scan and the node scan
use: prune from the segment header, then subtract the closed rows from the
verdict ranges per segment, then look at the rows that survive. The belief
cost becomes O(segments x (runs + closes)) and the match cost O(believed
rows).

**What this file gates.** The primary assertion is the engine's own
`sel_rows_examined` counter, for the reason `test_node_scan_scaling.py`
gives at length and does not need repeating: the counter is exact and
host-independent, and the per-row wall clock on a path whose dominant cost is
materialising a uid string does not separate. Here the counter is also the
*only* honest statistic, because the old sweep reported no counter at all —
it never went through `select`, so `sel_rows_examined` for a `resolve_entities`
call was zero on the older engine. The claim is therefore stated against the
store's own census rather than against a second engine build:

- the number of rows the old sweep visited is, by construction, every stored
  node version, which the manifest reports exactly (`rows` summed over the
  live node segments);
- the number the converted path visits is `sel_rows_examined`, and it must
  equal the *believed* node versions — stored minus the closes folded into the
  sidecars — and nothing else.

Two independent things have to hold for that to be worth anything, and both
are asserted: the corrected store must actually have superseded rows
(otherwise the two numbers are equal and the gate passes vacuously), and the
answer must not have moved (`test_resolve_matches_the_row_loop_oracle`).

These run against the **native** engine specifically, whatever
GNOMON_TEST_BACKEND says: verdict ranges and close sidecars are native segment
concepts and a DuckDB run would pass every test here vacuously.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# The fixture: 40 generations x 500 rows, interleaved so that compaction's
# global `(vt_s, vid)` sort leaves one `tt_s` run per row. That is the layout
# the sweep was most expensive on — a binary search per row — and a tidy one
# would hide part of what this change removes.
ROWS_PER_BATCH = 500
N_ROWS = 20_000
CORRECTION_PCT = 10.0


def _native_adapter(tmp):
    """The native engine, regardless of GNOMON_TEST_BACKEND (see module doc)."""
    native = pytest.importorskip(
        "gnomon.storage.native",
        reason="the resolve gate measures the native read path",
    )
    a = native.NativeAdapter(tmp)
    a.paranoid = False  # the paranoid disjointness check is O(versions) per op
    return a


def _node_ops(vts, mark: int):
    """One node version per vt. `name` is what `resolve_entities` matches on
    and is deliberately shared by a known fraction of the rows, so the query
    below has a non-trivial answer to get wrong."""
    return [
        {
            "op": "assert_node",
            "uid": f"n{vt}",
            "label": "N",
            "props": {"name": f"entity-{vt % 7}-{vt}", "v": vt * mark},
            "vt_s": vt,
            "vt_e": vt + 1,
        }
        for vt in vts
    ]


def _build(tmp: Path, n_rows: int = N_ROWS, corr_pct: float = 0.0):
    a = _native_adapter(tmp)
    n_gens = n_rows // ROWS_PER_BATCH
    tt = 1000
    for g in range(n_gens):
        tt += 1
        a.begin()
        a.apply_ops(_node_ops([i * n_gens + g for i in range(ROWS_PER_BATCH)], 1), tt)
        a.commit()
    n_corr = round(n_rows * corr_pct / 100.0)
    if n_corr:
        step = max(1, n_rows // n_corr)
        tt += 1
        a.begin()
        a.apply_ops(_node_ops(range(0, n_rows, step), -1), tt)
        a.commit()
    a.compact()
    return a


def _census(store: Path) -> tuple[int, int, bool]:
    """`(stored node versions, closes folded into sidecars, all_current)`.

    Straight from the manifest of the current generation, which is what
    "the store's census" means: `rows` is what the
    old sweep visited, and `rows - n_closed_folded` is what the
    converted path must visit.

    Read from the manifest rather than from `verify()` because `verify()["closes"]` counts entries in close *run* files, which
    `compact()` has emptied on both stores.
    """
    generation = int((store / "CURRENT").read_text().split()[0])
    m = json.loads((store / "manifests" / f"{generation:020d}.json").read_text())
    entries = m["node_store"]
    assert entries, f"no live node segments in generation {generation} of {store}"
    return (
        sum(e["rows"] for e in entries),
        sum(e["n_closed_folded"] for e in entries),
        all(e["all_current"] for e in entries),
    )


def _believed_by_row_loop(adapter) -> int:
    """The original row-loop predicate, run in Python over every stored node version:
    `NodeVersion.believed_at`, the model's own definition, applied row by
    row. Independent of the manifest and of the engine's range logic."""
    from gnomon.core.model import OPEN_END, clamp_tt

    as_of = clamp_tt(OPEN_END)
    return sum(1 for v in adapter.all_node_versions() if v.believed_at(as_of))


def _rows_examined(adapter, query: str) -> tuple[list, int]:
    """One `resolve_entities` call, bracketed by the phase counters.

    `phase_stats()` is process-global by design, so the reset has to be
    immediately before the call and the read immediately after, with nothing
    else touching the engine in between.
    """
    adapter.reset_phase_stats()
    out = adapter.resolve_entities(query)
    return out, int(adapter.phase_stats()["sel_rows_examined"])


# --- 1. the primary gate: what resolution examines ------------------------ #


def test_a_corrected_store_resolves_over_only_the_believed_rows(tmp_path):
    """The counter gate. The old sweep visited every stored node version;
    the new one visits the believed ones, and nothing in between."""
    corr = _build(tmp_path / "corr", corr_pct=CORRECTION_PCT)

    stored, folded, all_current = _census(tmp_path / "corr")
    # fixture-is-real: a gate on "the superseded rows are skipped"
    # is an assertion about nothing if the configuration superseded nothing
    assert folded > 0 and not all_current, (
        f"the corrected configuration folded {folded} closes and all_current="
        f"{all_current}: it has no superseded rows to skip"
    )
    believed = stored - folded
    # and the manifest arithmetic itself is cross-checked against the row
    # loop the conversion replaced, because `n_closed_folded` counts only the
    # closes a *compaction* folded into sidecars. `_build` compacts, so the
    # two agree here — but a corrected store that has not been re-compacted
    # reports zero folded closes while carrying thousands of superseded rows,
    # and a gate reading the manifest alone would score that as "nothing to
    # skip" and pass a conversion that skipped nothing.
    assert believed == _believed_by_row_loop(corr), (
        f"manifest says {believed} believed of {stored} stored, the row loop "
        f"says {_believed_by_row_loop(corr)}"
    )

    _, examined = _rows_examined(corr, "entity-3")
    assert examined == believed, (
        f"resolution examined {examined} rows; the store holds {stored} node "
        f"versions of which {folded} are superseded, so the verdict ranges "
        f"offer exactly {believed}. Examining {stored} would mean the belief "
        f"test is back to being per row."
    )
    assert examined < stored, (
        f"examined ({examined}) did not fall below stored ({stored}) — with "
        f"{folded} folded closes it must"
    )


def test_a_clean_store_examines_every_row_and_that_is_correct(tmp_path):
    """The other side of the same claim, and the reason the c=0 configuration of the
    A/B is pre-registered at 1.0 rather than at a speed-up: when nothing has
    been superseded there is nothing to skip, and the converted path must
    examine the whole store just as the sweep did. A gate that only checked
    the corrected configuration could be passed by a filter that drops rows."""
    clean = _build(tmp_path / "clean")
    stored, folded, all_current = _census(tmp_path / "clean")
    assert (folded, all_current) == (0, True)
    assert stored == _believed_by_row_loop(clean), (
        "an uncorrected store must believe every row it stores"
    )

    _, examined = _rows_examined(clean, "entity-3")
    assert examined == stored, (
        f"an uncorrected store examined {examined} of {stored} rows: the "
        f"ranges must cover the whole segment when nothing is closed"
    )


# --- 2. the answer has not moved ------------------------------------------ #


def _oracle(adapter, query: str, as_of: int) -> list:
    """`resolve_entities` recomputed in Python, the way the portable fallback
    in `gnomon/temporal/ops_snapshot.py` computes it: a row loop over every
    believed node version, scored exactly as `resolve_entities` defines it — 0 for an exact
    uid, 1 for a uid substring, 2 for a name substring, lowest wins — with
    the label and name taken from the latest believed version by
    `(vt_s, vid)` whether or not that version is the one that matched.

    The uncorrected sweep this change replaced *is* this loop, so agreeing with
    it is the direct before/after statement: same population, same scores,
    same canonical state, decided per row here and per segment there.
    """
    ql = query.lower()
    best: dict[str, tuple[int, tuple[int, str], str, object]] = {}
    for v in adapter.all_node_versions():
        if not v.believed_at(as_of):
            continue
        name = v.props.get("name")
        if v.uid == query:
            score = 0
        elif ql in v.uid.lower():
            score = 1
        elif isinstance(name, str) and name and ql in name.lower():
            score = 2
        else:
            score = None
        key = (v.vt_s, v.vid)
        cur = best.get(v.uid)
        if cur is None:
            cur = (1 << 30, key, v.label, name)
        s = cur[0] if score is None else min(cur[0], score)
        # the canonical state is the latest believed version's, matched or
        # not — an entity found by an old name resolves to what it is now
        best[v.uid] = (
            (s, key, v.label, name) if key >= cur[1] else (s, cur[1], cur[2], cur[3])
        )
    out = [(uid, e[0], e[2], e[3]) for uid, e in best.items() if e[0] != 1 << 30]
    out.sort(key=lambda r: (r[1], r[0]))
    return out


@pytest.mark.parametrize("corr_pct", [0.0, 1.0, CORRECTION_PCT])
@pytest.mark.parametrize("query", ["entity-3", "n199", "entity-", "nothing-here"])
def test_resolve_matches_the_row_loop_oracle(tmp_path, corr_pct, query):
    """Byte-identical answers, at every correction density the A/B measures.

    The conversion must not narrow which rows reach the canonical-state
    upsert, only skip rows that were being skipped anyway — an entity found
    by an old name still has to resolve to its latest believed state. That is
    what the unmatched-but-tracked rows in the oracle above check.
    """
    from gnomon.core.model import OPEN_END, clamp_tt

    a = _build(tmp_path / f"s{corr_pct}", n_rows=4_000, corr_pct=corr_pct)
    as_of = clamp_tt(OPEN_END)
    got = [
        (u, int(s), str(lab), json.loads(p).get("name"))
        for u, s, lab, p in a.resolve_entities(query)
    ]
    want = _oracle(a, query, as_of)
    assert got == want


def test_resolution_is_deterministic_across_thread_counts(tmp_path, monkeypatch):
    """`select` fans out over row-range units and the units come back in
    whatever order the workers claimed them; `resolve_entities` folds them
    into a `(vt_s, vid)` tiebreak that must not notice."""
    a = _build(tmp_path / "threads", n_rows=8_000, corr_pct=CORRECTION_PCT)
    answers = []
    for threads in ("1", "2", "4", "8"):
        monkeypatch.setenv("GNOMON_SCAN_THREADS", threads)
        answers.append(
            [(u, int(s), str(lab), str(p)) for u, s, lab, p in a.resolve_entities("entity-3")]
        )
    assert all(x == answers[0] for x in answers[1:])
