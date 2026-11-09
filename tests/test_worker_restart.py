"""(sysworker.SysWorker for the kit's P1 path; timed_runner.Worker for the shared timed runner). A fake system: the
store open takes OPEN_S, the first call in a process is cold (COLD_S), later calls are instant; "slow" hangs."""
import json
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

OPEN_S, COLD_S = 3.0, 1.5
STATE = {"cold": True}


def _call(qid):
    if qid == "slow":
        time.sleep(120)
    if STATE["cold"]:
        time.sleep(COLD_S)
        STATE["cold"] = False
    return {"rows": [1], "rows_total": 1}


class Fake:
    name = "fake"

    def open(self):
        time.sleep(OPEN_S)
        STATE["cold"] = True

    def settings(self):
        return {}

    def fn_for(self, q):
        return lambda: _call(q.id)

    def close(self):
        pass


@pytest.mark.parametrize("reprime", [True, False])
def test_sysworker_next_cell_after_kill(monkeypatch, reprime):
    monkeypatch.setenv("EXTCMP_CALL_CEILING_S", "2")
    monkeypatch.setenv("EXTCMP_INPROC_WORKER_GRACE_S", "1.5")
    import eval_harness as H
    import sysworker as SW
    fast, slow = H.Query("fast", "x", {}, "", ()), H.Query("slow", "x", {}, "", ())
    w = SW.SysWorker(Fake, "fake")                # the store open (3 s) is outside every backstop (3.5 s)
    w.call(fast)                                   # the window prime (cold)
    if reprime:
        w.reprime = fast
    with pytest.raises(SW.WorkerTimeout):
        w.call(slow)                               # killed at the backstop; restarted (and re-primed)
    t = time.perf_counter()
    assert w.call(fast)["rows"] == [1]
    ms = (time.perf_counter() - t) * 1e3
    if reprime:
        assert ms < 1000, f"the next cell paid the fresh worker's cold state: {ms:.0f} ms"
        assert any("re-primed" in e["event"] and "ok" in e["event"] for e in w.events)
    else:                                          # control: without the re-prime the cold call lands in the cell
        assert ms >= COLD_S * 1e3
    w.close()


def test_timed_runner_next_cell_after_kill(monkeypatch, tmp_path):
    import timed_runner as TR
    import tr_catalogs as TC
    monkeypatch.setattr(TR, "CEIL_S", 1.0)
    monkeypatch.setattr(TR, "GRACE_S", 0.5)
    monkeypatch.setattr(TR, "PRIME_KILL_S", 10.0)
    cells = [("fast", "s", 0, None), ("slow", "s", 0, None), ("fast", "s", 1, None)]

    def open_(a):
        Fake().open()                              # 3 s: longer than the ceiling + grace
        return TC.Bound(lambda q, s, p: ((lambda: _call(q)), lambda r: ("h", len(r["rows"]))), None, {})
    monkeypatch.setitem(TC.CATALOGS, "fake", (lambda a: cells, open_))
    monkeypatch.setitem(TC.PROTOCOLS, "fake", (1, 2, 1, 1))
    out = tmp_path / "block.json"
    monkeypatch.setattr(sys, "argv", ["timed_runner.py", "fake", "--system", "fake", "--out", str(out)])
    assert TR.main() == 0
    res = json.loads(out.read_text())["results"]
    assert res[0]["ok"], res[0]                    # its cold prime (1.5 s > the 1 s ceiling) is outside the ceiling
    assert not res[1]["ok"] and res[1]["censored"], res[1]
    assert res[2]["ok"], res[2]                    # fresh worker: store open and re-prime before its ceiling
    assert "primed" in res[2]["protocol"]
    assert res[2]["prime_ms"] >= COLD_S * 1e3 and max(res[2]["timings_ms"]) < 1000


# --------------------------------------------------------------------------- #
# the priming rule with a server system (PostgreSQL): the prime runs with the server cap at PRIME_KILL_S - 2 s,
# the cap is back at the cell's value for the warm-ups. Needs a running PostgreSQL (the kit's server start;
# KF_TEST_PG=1 and the libpq environment of kit/lib/env.sh), else skipped.
@pytest.mark.skipif(os.environ.get("KF_TEST_PG") != "1", reason="needs a PostgreSQL server (KF_TEST_PG=1)")
def test_timed_runner_prime_server_cap_postgres(monkeypatch, tmp_path):
    import timed_runner as TR
    import tr_catalogs as TC
    monkeypatch.setattr(TR, "CEIL_S", 1.0)
    monkeypatch.setattr(TR, "GRACE_S", 0.5)

    def open_(a):
        import psycopg
        import prime_cap as PC
        con = psycopg.connect("dbname=postgres", autocommit=True)
        con.execute("SET statement_timeout = '1s'")          # the cell's server cap: below the 1.5 s kill
        PC.hook(lambda sec: con.execute(f"SET statement_timeout = '{int(sec) if sec else 1}s'"))
        first = {"cold": True}

        def make(q, s, p):
            def fn():
                if first["cold"]:                              # the cold prime: 2 s, above the cell's 1 s cap
                    first["cold"] = False
                    con.execute("SELECT pg_sleep(2)")
                return con.execute("SHOW statement_timeout").fetchone()[0]
            return fn, lambda r: (r, 1)
        return TC.Bound(make, None, {})
    monkeypatch.setitem(TC.CATALOGS, "fakepg", (lambda a: [("cold", "s", 0, None)], open_))
    monkeypatch.setitem(TC.PROTOCOLS, "fakepg", (1, 2, 1, 1))
    out = tmp_path / "block.json"
    monkeypatch.setattr(sys, "argv", ["timed_runner.py", "fakepg", "--system", "postgres", "--out", str(out)])
    assert TR.main() == 0
    c = json.loads(out.read_text())["results"][0]
    assert c["ok"], c                                          # the 2 s prime ran under the raised server cap
    assert c["prime_ms"] >= 2000
    assert c["hash"] == "1s", c                                # the warm-ups and reps ran with the cap reset
