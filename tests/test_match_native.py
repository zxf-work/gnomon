"""`match`: the engine's plan executor against the reference route.

Both routes run one compiled plan, so any difference is an execution
difference. Held on the random bi-temporal stores and arguments of
`test_match_oracle.py` (every scope and belief mode, sub-patterns, closures,
`distinct_on`, two stages, paging) and on the fixed corpus there.
Every call must be answered by the engine (none may fall back), except the
two the executor hands back by design: an open write batch and a row the
evaluator refuses.
"""

from __future__ import annotations

import os
import random
import sys
from pathlib import Path

import pytest

from gnomon.core.errors import InvalidArgError
from gnomon.core.model import canonical_json
from gnomon.temporal import ops_match as OM
from gnomon.temporal.algebra import ensure_all_registered
from gnomon.temporal.ops_match import compile_match, run, validate

from . import test_match_oracle as T

pytestmark = pytest.mark.skipif(os.environ.get("GNOMON_TEST_BACKEND") != "native",
                                reason="the engine executor is the native backend's")

REPO = Path(__file__).resolve().parents[1]


def native_and_reference(adapter, args):
    filled = validate(args)
    try:
        compile_match(filled)
    except InvalidArgError:
        return None
    before = OM.ROUTES["native"]
    got = run(adapter, filled, route="native")
    took_native = OM.ROUTES["native"] > before
    ref = run(adapter, filled, route="reference", reference_bypass="test store")
    return got, ref, took_native


@pytest.mark.parametrize("name", sorted(T.CORPUS))
def test_corpus_native(social, name):
    a, _ = social
    args, _ = T.CORPUS[name]
    got, ref, native = native_and_reference(a, args)
    assert native, OM.LAST_DECLINE[0]
    assert canonical_json(got) == canonical_json(ref)


@pytest.fixture(scope="module")
def social():
    a, oracle = T.build_social()
    yield a, oracle
    a.close()


@pytest.mark.parametrize("case", range(T.EXAMPLES))
def test_random_arguments_native_equals_reference(case):
    ensure_all_registered()
    a, _oracle, max_tt = T.random_store(case % 5)
    rng = random.Random(5000 + case)
    compared = 0
    for _ in range(12):
        args = T.random_args(rng, max_tt)
        try:
            pair = native_and_reference(a, args)
        except InvalidArgError:
            continue                     # a data-dependent refusal, on both routes
        if pair is None:
            continue
        got, ref, native = pair
        assert canonical_json(got) == canonical_json(ref), args
        assert native, (OM.LAST_DECLINE[0], args)
        compared += 1
    assert compared > 0


def test_inside_an_open_batch_the_reference_answers(social):
    a, _ = social
    args, _ = T.CORPUS["friends both ways"]
    a.begin()
    try:
        # a staged friendship the engine's segments do not hold yet
        a.apply_ops([T.edge("p3", "p6", "KNOWS", vt_s=12)], 99)
        before = dict(OM.ROUTES)
        got = run(a, validate(args), route="native")
        assert OM.ROUTES["native"] == before["native"]
        assert OM.ROUTES["reference"] == before["reference"] + 1
        # the columnar scans the reference reads see published generations
        assert {r["f"] for r in got["rows"]} == {"p1", "p2", "p4"}
    finally:
        a.rollback()


def test_the_parallel_fold_equals_the_serial_one(tmp_path):
    """Every aggregate folded on the pool (`GNOMON_MATCH_FOLD_ROWS=1`), in a
    fresh process, against the reference route: groups keep their order and
    each state its first-seen tie when partial folds merge."""
    import subprocess

    script = tmp_path / "fold.py"
    script.write_text(
        "import random, sys\n"
        f"sys.path.insert(0, {str(REPO)!r})\n"
        "from tests import test_match_oracle as T\n"
        "from tests.test_match_native import native_and_reference\n"
        "from gnomon.core.errors import InvalidArgError\n"
        "from gnomon.core.model import canonical_json\n"
        "from gnomon.temporal.algebra import ensure_all_registered\n"
        "ensure_all_registered()\n"
        "n = 0\n"
        "for case in range(30):\n"
        "    a, _o, mt = T.random_store(case % 5)\n"
        "    rng = random.Random(9000 + case)\n"
        "    for _ in range(8):\n"
        "        args = T.random_args(rng, mt)\n"
        "        if 'group' not in args['return']:\n"
        "            continue\n"
        "        try:\n"
        "            pair = native_and_reference(a, args)\n"
        "        except InvalidArgError:\n"
        "            continue\n"
        "        if pair is None:\n"
        "            continue\n"
        "        got, ref, native = pair\n"
        "        assert native and canonical_json(got) == canonical_json(ref), args\n"
        "        n += 1\n"
        "assert n > 20, n\n"
        "print('ok', n)\n")
    env = dict(os.environ, GNOMON_MATCH_FOLD_ROWS="1", GNOMON_TEST_BACKEND="native")
    out = subprocess.run([sys.executable, str(script)], env=env, capture_output=True,
                         text=True, cwd=REPO, timeout=900)
    assert out.returncode == 0, out.stdout[-2000:] + out.stderr[-4000:]
    assert out.stdout.startswith("ok")


@pytest.mark.parametrize("case", range(T.EXAMPLES))
def test_random_regular_path_queries_native_equals_reference(case):
    ensure_all_registered()
    a, _oracle, max_tt = T.random_store(case % 5)
    rng = random.Random(7000 + case)
    for _ in range(8):
        args = T.random_rpq_args(rng, max_tt)
        got, ref, native = native_and_reference(a, args)
        assert native, (OM.LAST_DECLINE[0], args)
        assert canonical_json(got) == canonical_json(ref), args
