"""The streaming content digest equals the posthoc one, bit for bit.

`content_digest` — `store_digest`'s fields minus `vid`/`tt_s`/`tt_e`, all three
stamped from the wall-clock transaction clock — is the only store identity two
independent ingests of the same logical events can share, and so the
one the scale A/Bs are compared on. The posthoc pass that computes it
(`scripts/build_synth_store.py::content_digest`) materialises every logical row
into sorted Python lists: measured at tens of GB at 100M rows, and OOM-killed
at 300M. The engine computes the same value internally, in memory bounded by a
byte budget and the merge fan-in.

That makes equality with the posthoc pass the whole contract, and this module
is where it is proved: not against a second Rust implementation (`digest.rs`'s
own unit tests do that) but against **the reference function itself**, imported
from the driver, on stores the driver built.

Everything here runs at `--n-entities` 100 to 6,000. The 100k/1M equality runs
and the memory measurement live on the development host, not in this suite.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

import gnomon
from gnomon.core.model import EntityRef, OPEN_END

ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, relpath: str) -> Any:
    scripts_dir = str(ROOT / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location(name, ROOT / relpath)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


B = _load_module("build_synth_store", "scripts/build_synth_store.py")


def _native(store: "gnomon.Store") -> Any:
    return store.adapter


def _workload(store: "gnomon.Store") -> None:
    """Node and edge versions, a correction, a retraction and a bulk ingest —
    so both populations have belief history rather than being append-only, and
    the corrections give rows that tie on `(eid, vt_s, vt_e)` and are separated
    only by the digest key's fourth component, the props text."""
    for i in range(12):
        store.assert_node(f"n{i}", "N", {"i": i, "tag": f"t{i % 3}"}, vt_s=0, vt_e=1000)
    for i in range(30):
        u, v = f"n{i % 12}", f"n{(i + 1) % 12}"
        store.assert_edge(u, v, "R", {"w": i}, vt_s=i * 10, vt_e=i * 10 + 50)
    store.correct(EntityRef(kind="node", uid="n0"),
                  {"i": 0, "corrected": True}, vt_s=0, vt_e=1000)
    store.correct(EntityRef(kind="node", uid="n1"), {"i": 1}, vt_s=0, vt_e=1000)
    store.retract(EntityRef(kind="edge", src="n1", dst="n2", rel_type="R"), t=15)
    store.ingest_events([
        {"src": f"n{i % 12}", "dst": f"n{(i + 3) % 12}", "rel_type": "S",
         "vt_s": 2000 + i, "vt_e": OPEN_END if i % 5 else 2500 + i}
        for i in range(40)
    ])


# --------------------------------------------------------------------- #
# equality with the reference the campaign actually used
# --------------------------------------------------------------------- #

def test_streaming_equals_the_posthoc_reference(tmp_path: Path):
    store = gnomon.open(tmp_path / "s", backend="native")
    try:
        _workload(store)
        want = B.content_digest(store)
        got = _native(store).content_digest_streaming(budget_bytes=0)
        assert got["digest"] == want
        assert got["ambiguous_ties"] == 0
        # and the library transcription of the reference agrees with it, so
        # the CLI's non-streaming path is the same notion
        assert _native(store).content_digest() == want
    finally:
        store.close()


@pytest.mark.parametrize("budget", [1, 64, 4096, 1 << 16, 0])
def test_the_value_is_independent_of_the_budget(tmp_path: Path, budget: int):
    """A budget small enough to spill a run per row must not change the value —
    the property that makes the memory bound free rather than a trade."""
    store = gnomon.open(tmp_path / f"s{budget}", backend="native")
    try:
        _workload(store)
        want = B.content_digest(store)
        rep = _native(store).content_digest_streaming(budget_bytes=budget)
        assert rep["digest"] == want
        if budget == 1:
            assert rep["runs_spilled"] > 10
            assert rep["spill_bytes"] > 0
        if budget == 0:
            assert rep["runs_spilled"] == 0
            assert rep["spill_bytes"] == 0
    finally:
        store.close()


def test_rows_held_does_not_track_the_row_count(tmp_path: Path):
    """The memory claim, counted rather than inferred from RSS: one budget,
    two stores an order of magnitude apart, and the rows resident at once must
    not grow with the population."""
    small = gnomon.open(tmp_path / "small", backend="native")
    big = gnomon.open(tmp_path / "big", backend="native")
    try:
        _workload(small)
        for _ in range(12):
            _workload(big)
        a = _native(small).content_digest_streaming(budget_bytes=4096)
        b = _native(big).content_digest_streaming(budget_bytes=4096)
        rows_a = a["rows_edges"] + a["rows_nodes"]
        rows_b = b["rows_edges"] + b["rows_nodes"]
        assert rows_b > 8 * rows_a, (rows_a, rows_b)
        # sublinear by a wide margin: the buffer is capped by the budget and
        # the merge parks one record per run, with the run count itself capped
        assert b["rows_held_max"] * 2 < a["rows_held_max"] * rows_b / rows_a
        assert a["digest"] == B.content_digest(small)
        assert b["digest"] == B.content_digest(big)
    finally:
        small.close()
        big.close()


def test_equal_across_compact_and_settle(tmp_path: Path):
    """The digest is over logical content, so neither compaction policy nor a
    settle may move it — and it must still equal the posthoc pass after each."""
    store = gnomon.open(tmp_path / "s", backend="native")
    try:
        _workload(store)
        before = _native(store).content_digest_streaming(budget_bytes=4096)["digest"]
        assert before == B.content_digest(store)

        store.adapter.compact()
        mid = _native(store).content_digest_streaming(budget_bytes=4096)["digest"]
        assert mid == before
        assert mid == B.content_digest(store)

        store.adapter.settle()
        after = _native(store).content_digest_streaming(budget_bytes=4096)["digest"]
        assert after == before
        assert after == B.content_digest(store)
    finally:
        store.close()


# --------------------------------------------------------------------- #
# the corpus the campaign's recorded values were computed on
# --------------------------------------------------------------------- #

@pytest.mark.parametrize("named_fraction", [0.0, 0.5])
def test_streaming_equals_posthoc_on_a_synth_store(tmp_path: Path, named_fraction: float):
    """The driver's own corpus, at both ends of `--named-fraction`: the
    corrections, the retractions and the partial mid-interval carves are the
    shapes that put several versions of one identity at the same valid
    interval, separated only by props."""
    out = tmp_path / f"synth-{named_fraction}"
    B.build(out, 2_000, 0, 500, 1_000_000, "native", False, None,
            named_fraction=named_fraction)
    store = gnomon.open(out, backend="native", read_only=True)
    try:
        want = B.content_digest(store)
        rep = _native(store).content_digest_streaming(budget_bytes=1 << 14)
        assert rep["digest"] == want
        assert rep["ambiguous_ties"] == 0
        assert rep["runs_spilled"] > 0, "this budget must exercise the merge"
    finally:
        store.close()


def test_the_interval_lane_is_covered(tmp_path: Path):
    """`--interval-fraction 0.5` puts half the edges in the interval lane,
    which is a different segment shape (explicit `vt_e`, staircase index) and
    a different manifest vector — both must be walked."""
    out = tmp_path / "synth-iv"
    B.build(out, 2_000, 0, 500, 1_000_000, "native", False, None,
            tick_us=1, interval_fraction=0.5, named_fraction=0.1)
    store = gnomon.open(out, backend="native", read_only=True)
    try:
        assert _native(store).content_digest_streaming(budget_bytes=1 << 14)["digest"] \
            == B.content_digest(store)
    finally:
        store.close()


# --------------------------------------------------------------------- #
# the encoding hazards, end to end
# --------------------------------------------------------------------- #

#: Every shape where a Rust JSON encoder and CPython's could disagree, in one
#: props dict: floats whose `repr` uses an exponent CPython spells `1e+30` and
#: `1e-07` where Rust spells `1e30`/`1e-7`; a negative zero; an integral float
#: that must keep its `.0`; an int wider than 2**53; the seven short escapes;
#: a C0 control and DEL (escaped by the *sort key*'s `ensure_ascii=True` and
#: not by the canonical form); a non-BMP character, which `ensure_ascii`
#: renders as a UTF-16 surrogate pair; and nesting, whose separators the sort
#: key spaces out at every depth.
HAZARD_PROPS = {
    "f": 1.5,
    "big": 1e30,
    "tiny": 1e-7,
    "negzero": -0.0,
    "whole": 3.0,
    "wide": 2 ** 62,
    "esc": 'a"b\\c\nd\te\r\x0c\x08',
    "ctl": "\x01\x1f\x7f",
    "uni": "\u00e9\u4e2d\U0001f600",
    "nested": {"z": [1, {"k": None}, True], "a": [], "\u00e9": {}},
    "empty": {},
}


def test_the_encoding_hazards_agree_with_the_reference(tmp_path: Path):
    """The risk that ruled out a Rust
    digest, exercised rather than argued: floats, controls, non-BMP
    characters and nesting in props, and non-ASCII in every string field that
    is *not* props (uid, label, src/dst, rel_type, disc, source,
    provenance_ref), which the engine re-encodes itself."""
    store = gnomon.open(tmp_path / "s", backend="native")
    try:
        store.assert_node("n\u00e9\U0001f600", "L\u4e2d", dict(HAZARD_PROPS),
                          vt_s=0, vt_e=10)
        store.assert_node('q"\\\n', "L", {"uni": "z"}, vt_s=0, vt_e=10)
        store.assert_edge("n\u00e9\U0001f600", 'q"\\\n', "R\u4e2d",
                          dict(HAZARD_PROPS), vt_s=1, vt_e=5, disc='d#\u00e9"')
        store.ingest_events([{
            "src": "n\u00e9\U0001f600", "dst": 'q"\\\n', "rel_type": "S",
            "vt_s": 20, "vt_e": 30, "props": {"uni": "\U0001f600", "f": 1e-7},
            "disc": "#\u4e2d", "source": "agent\u00e9",
            "provenance_ref": "p/\u00e9\U0001f600",
        }])
        want = B.content_digest(store)
        rep = _native(store).content_digest_streaming(budget_bytes=1)
        assert rep["digest"] == want
        assert rep["ambiguous_ties"] == 0
        assert rep["runs_spilled"] > 3, "a 1-byte budget must spill per row"
    finally:
        store.close()


def test_non_ascii_props_order_is_the_ascii_rendering(tmp_path: Path):
    """The fourth key component is `json.dumps(props, sort_keys=True)` — the
    *default*-separator, `ensure_ascii=True` rendering — and it orders
    differently from the canonical text whenever a non-ASCII character meets a
    character above `\\` (0x5C). Two versions of one identity at one valid
    interval, separated only by that, put the difference on the digest's own
    path: get the rendering wrong and the two rows come out swapped."""
    store = gnomon.open(tmp_path / "s", backend="native")
    try:
        # same uid, same vt, props differing only in a character whose raw and
        # escaped orders disagree: raw 'é' (U+00E9) > 'z', escaped '\u00e9' < 'z'
        store.assert_node("n0", "N", {"k": "\u00e9"}, vt_s=0, vt_e=10)
        store.assert_node("n0", "N", {"k": "z"}, vt_s=0, vt_e=10)
        want = B.content_digest(store)
        assert _native(store).content_digest_streaming(budget_bytes=0)["digest"] == want
        assert _native(store).content_digest_streaming(budget_bytes=1)["digest"] == want
    finally:
        store.close()


def test_an_ambiguous_tie_is_reported_rather_than_hidden(tmp_path: Path):
    """Two rows can tie on the reference's whole sort key — `(uid, vt_s,
    vt_e, props)` — and still differ, because `label`, `source` and
    `provenance_ref` are hashed and not keyed on. Asserting one node twice
    with a new label and the same props over the same valid interval is the
    shortest such store: the first version is closed in tt, the row stays
    (that is belief history), and stripping tt leaves two rows the key cannot
    separate.

    Python's `sorted` is stable, so the reference's answer there is whatever
    order `all_node_versions()` returned — documented unspecified, and free to
    change under a compaction. The engine cannot reproduce an order that is
    not defined. What it can do is say when it is in that position, and
    `ambiguous_ties` is that receipt; it is 0 on every other store in this
    file and on every store the synth driver builds.

    The two agree on *this* store, and that is recorded rather than relied on:
    what the assertion below actually pins is that the streaming digest does
    not move under a compaction, which is the property `content_digest` exists
    to have.
    """
    store = gnomon.open(tmp_path / "s", backend="native")
    try:
        store.assert_node("n0", "Alpha", {"w": 1}, vt_s=0, vt_e=10)
        store.assert_node("n0", "Beta", {"w": 1}, vt_s=0, vt_e=10)
        rep = _native(store).content_digest_streaming(budget_bytes=0)
        assert rep["ambiguous_ties"] == 1, rep
        assert rep["rows_nodes"] == 2

        before = rep["digest"]
        store.adapter.compact()
        after = _native(store).content_digest_streaming(budget_bytes=0)["digest"]
        assert after == before, "the streaming digest must not move under compaction"
    finally:
        store.close()


# --------------------------------------------------------------------- #
# the CLI surface
# --------------------------------------------------------------------- #

def test_cli_digest_prints_the_same_value_both_ways(tmp_path: Path, capsys):
    import json as _json

    from gnomon.cli import main

    out = tmp_path / "s"
    store = gnomon.open(out, backend="native")
    try:
        _workload(store)
        want = B.content_digest(store)
    finally:
        store.close()

    assert main(["store", "digest", "--store", str(out)]) in (0, None)
    plain = _json.loads(capsys.readouterr().out.strip())
    assert plain["digest"] == want
    assert plain["streaming"] is False

    assert main(["store", "digest", "--store", str(out), "--streaming",
                 "--budget-bytes", "4096"]) in (0, None)
    streamed = _json.loads(capsys.readouterr().out.strip())
    assert streamed["digest"] == want
    assert streamed["budget_bytes"] == 4096
    assert streamed["rows_edges"] > 0 and streamed["rows_nodes"] > 0


# --------------------------------------------------------------------- #
# Addendum -- the Python reference's sort is now total
# --------------------------------------------------------------------- #
#
# `content_digest`'s sort key -- (uid|eid, vt_s, vt_e, props text) -- does
# not separate two versions of the same identity over the same valid
# interval and the same props that differ only in `label`/`source`/
# `provenance_ref` (hashed, not keyed on). Before this addendum, a tie there
# was broken by whatever order `all_*_versions()` happened to return --
# documented unspecified, so the reference's own value depended on layout:
# two independently-built stores holding the *same logical content* could
# digest differently depending only on the order their tied rows were
# stored in. `content_digest`/`content_digest_report` now append each row's
# own canonical bytes as a fifth, total-order-making sort component -- the
# same tie-break `crates/gnomon-core/src/digest.rs::cmp_rec` already
# used -- and report how many rows needed it as `ambiguous_ties`, the same
# name and the same counting rule (adjacent rows on the sorted output, once
# per row) as the Rust side's `ContentDigestReport.ambiguous_ties`.

def test_the_python_reference_is_layout_independent_on_a_tie(tmp_path: Path):
    """One node asserted twice with a new label, the same props, over the
    same valid interval -- the shortest tie on the reference's whole
    four-component key (`test_an_ambiguous_tie_is_reported_rather_than_hidden`'s
    fixture, reused here at the reference level). Ingesting the two versions
    in opposite order across two stores gives the same two stripped rows
    back from `all_node_versions()`, but in the opposite physical (tt)
    order -- exactly the "unspecified" layout difference the reference's
    old, non-total key was exposed to.

    Before this addendum this test fails: the two stores' `content_digest()`
    disagree (verified directly against the pre-addendum four-component key
    while writing this test -- `fd0d1045…` vs `96729c77…` on this exact
    fixture). With the fifth, canonical-bytes component appended, both
    orders sort the tied rows the same way and the digests agree.
    """
    store_ab = gnomon.open(tmp_path / "ab", backend="native")
    store_ba = gnomon.open(tmp_path / "ba", backend="native")
    try:
        store_ab.assert_node("n0", "Alpha", {"w": 1}, vt_s=0, vt_e=10)
        store_ab.assert_node("n0", "Beta", {"w": 1}, vt_s=0, vt_e=10)

        store_ba.assert_node("n0", "Beta", {"w": 1}, vt_s=0, vt_e=10)
        store_ba.assert_node("n0", "Alpha", {"w": 1}, vt_s=0, vt_e=10)

        rep_ab = B.content_digest_report(store_ab)
        rep_ba = B.content_digest_report(store_ba)

        assert rep_ab["ambiguous_ties"] > 0, rep_ab
        assert rep_ba["ambiguous_ties"] > 0, rep_ba
        assert rep_ab["digest"] == rep_ba["digest"]
        # and the thin `content_digest()` wrapper agrees with its own report
        assert B.content_digest(store_ab) == rep_ab["digest"]
        assert B.content_digest(store_ba) == rep_ba["digest"]
    finally:
        store_ab.close()
        store_ba.close()


def test_the_rust_streaming_digest_agrees_with_the_python_reference_on_the_tie(
    tmp_path: Path,
):
    """The same two differently-ordered stores as above, checked against the
    engine's streaming digest rather than the posthoc reference:
    `digest.rs` already broke ties on the row's own canonical bytes (it is
    the reference this addendum matches), so it was already
    layout-independent on this fixture. What this test adds is the
    three-way equality the addendum is for: both orders' streaming digest
    equal each other *and* equal the now-fixed Python reference, and both
    sides count the same `ambiguous_ties`."""
    store_ab = gnomon.open(tmp_path / "ab", backend="native")
    store_ba = gnomon.open(tmp_path / "ba", backend="native")
    try:
        store_ab.assert_node("n0", "Alpha", {"w": 1}, vt_s=0, vt_e=10)
        store_ab.assert_node("n0", "Beta", {"w": 1}, vt_s=0, vt_e=10)

        store_ba.assert_node("n0", "Beta", {"w": 1}, vt_s=0, vt_e=10)
        store_ba.assert_node("n0", "Alpha", {"w": 1}, vt_s=0, vt_e=10)

        py_ab = B.content_digest_report(store_ab)
        py_ba = B.content_digest_report(store_ba)
        rust_ab = _native(store_ab).content_digest_streaming(budget_bytes=0)
        rust_ba = _native(store_ba).content_digest_streaming(budget_bytes=0)

        assert rust_ab["digest"] == rust_ba["digest"]
        assert rust_ab["digest"] == py_ab["digest"] == py_ba["digest"]
        assert rust_ab["ambiguous_ties"] == py_ab["ambiguous_ties"] > 0
        assert rust_ba["ambiguous_ties"] == py_ba["ambiguous_ties"] > 0

        # the CLI's non-streaming path (`content_digest_report`) reports the
        # same tie count as the streaming one, on the same store
        assert (_native(store_ab).content_digest_report()["ambiguous_ties"]
                == rust_ab["ambiguous_ties"])
    finally:
        store_ab.close()
        store_ba.close()


def test_a_recorded_receipt_reproduces_bit_for_bit_with_the_total_sort(
    tmp_path: Path,
):
    """The smallest cell with a recorded digest receipt -- cell
    F, 100,000 entities, `--seed 0 --batch 50000 --compact-every 1000000
    --named-fraction 0.0`
    built fresh on the laptop and digested with the
    changed reference. No cell in that audit has a sort-key tie
    (`ambiguous_ties == 0` everywhere recorded), so this is exactly the claim
    that matters: the fifth, total-order-making sort component is a no-op on
    every store the campaign has actually built, and this recorded value in
    particular is unchanged.

    ~100k rows, ~15 s on a laptop -- the audit's own 1M/10M cells (the ones
    with digests actually pinned as `4ca32fd8…`/`ff9a15e2…`) are not
    reproduced here; those need a larger machine.
    """
    recorded_digest = ("019ea38fb24b729237f546a4656e4697e1d81f122d60aa33dec"
                       "a9e3229e81591")
    out = tmp_path / "store-f"
    B.build(out, n_entities=100_000, seed=0, batch=50_000, compact_every=1_000_000,
           backend="native", resume=False, stop_at_ops=None, digest_mode="none",
           named_fraction=0.0)
    store = gnomon.open(out, backend="native", read_only=True)
    try:
        rep = B.content_digest_report(store)
        assert rep["digest"] == recorded_digest
        assert rep["ambiguous_ties"] == 0
    finally:
        store.close()
