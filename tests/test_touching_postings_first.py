"""A touching read takes its adjacency postings before the belief work.

The scan planner computed each segment's believed ranges over the whole request
domain and only then intersected them with an incidence filter's postings: on
FinBench SF1 (718k closes) a one-vertex read paid every close of every segment,
3-7 ms, against 0.1 ms on K10M. The postings now come first and the belief work
covers their rows. Belief is decided per row, so the rows are the same; this
pins that on corrected, retracted, multi-segment stores against the old order
(`GNOMON_WINDOW_FIRST=0`) and against no postings at all
(`GNOMON_ADJ_FILTERED_SCAN=0`), for windows, belief times and endpoint sets --
a filter over most of the endpoints too, whose postings make many ranges and
take the old stage with the old decision.
"""

from __future__ import annotations

import random
import tempfile

import numpy as np
import pytest

from gnomon.core.model import OPEN_END

UIDS = [f"n{i}" for i in range(12)]
T_MAX = 80


def _store(seed: int):
    from gnomon.storage.native import NativeAdapter

    tmp = tempfile.TemporaryDirectory(prefix="gnomon-test-touch-")
    a = NativeAdapter(tmp.name)
    rng = random.Random(seed)
    tt = 1
    for k in range(60):
        u, v = rng.sample(UIDS, 2)
        s = rng.randrange(0, T_MAX - 2)
        e = OPEN_END if rng.random() < 0.5 else s + rng.randrange(1, T_MAX - s)
        ref = {"kind": "edge", "src": u, "dst": v, "rel_type": "R", "disc": str(k % 3)}
        op = rng.choice(["e", "e", "e", "c", "r"])
        ops = {"e": [{"op": "assert_edge", "src": u, "dst": v, "rel_type": rng.choice(["R", "S"]),
                      "props": {}, "vt_s": s, "vt_e": e, "disc": str(k % 3)}],
               "c": [{"op": "correct", "ref": ref, "props": {"c": k}, "vt_s": s, "vt_e": e}],
               "r": [{"op": "retract", "ref": ref, "t": rng.randrange(0, T_MAX)}]}[op]
        a.begin()
        try:
            a.apply_ops(ops, tt)
            a.commit()
        except Exception:  # noqa: BLE001 - an op the store refuses
            a.rollback()
        tt += 1
    a.close()
    return tmp, tt


def _reads(path, n_tt: int, seed: int):
    from gnomon.storage.native import NativeAdapter

    ad = NativeAdapter(path, read_only=True)
    ad._store.reset_phase_stats()
    try:
        rng = random.Random(seed)
        out = []
        for _ in range(40):
            ids = [int(i) for i in ad.dense_ids(rng.sample(UIDS, rng.choice([1, 1, 2, 3, 10])))]
            lo = rng.choice([None, rng.randrange(0, T_MAX)])
            hi = None if lo is None else rng.choice([lo + 1, lo + rng.randrange(1, 30), OPEN_END])
            as_of = rng.choice([OPEN_END, rng.randrange(1, n_tt)])
            e = ad.edges_columnar(as_of_tt=as_of, vt_min=lo, vt_max=hi,
                                  rel_types=rng.choice([None, ["R"]]),
                                  columns=("vid", "src_id", "dst_id", "vt_s", "vt_e"),
                                  touching_ids=ids, touching_both=rng.random() < 0.2)
            out.append(sorted(zip(e["vid"], np.asarray(e["src_id"]).tolist(),
                                  np.asarray(e["dst_id"]).tolist(),
                                  np.asarray(e["vt_s"]).tolist(), np.asarray(e["vt_e"]).tolist())))
        return out, ad._store.phase_stats().get("sel_adj_segments", 0)
    finally:
        ad.close()


@pytest.mark.parametrize("seed", range(6))
def test_postings_first_reads_the_rows_of_the_old_order(seed, monkeypatch):
    tmp, n_tt = _store(seed)
    with tmp:
        for k in ("GNOMON_WINDOW_FIRST", "GNOMON_ADJ_FILTERED_SCAN"):
            monkeypatch.delenv(k, raising=False)
        new, posted = _reads(tmp.name, n_tt, 100 + seed)
        monkeypatch.setenv("GNOMON_WINDOW_FIRST", "0")
        old, _ = _reads(tmp.name, n_tt, 100 + seed)
        monkeypatch.delenv("GNOMON_WINDOW_FIRST")
        monkeypatch.setenv("GNOMON_ADJ_FILTERED_SCAN", "0")
        none, unposted = _reads(tmp.name, n_tt, 100 + seed)
    assert any(new), "every read was empty: the store is not exercising anything"
    assert posted > 0 and unposted == 0, (posted, unposted)
    assert new == old
    assert new == none
