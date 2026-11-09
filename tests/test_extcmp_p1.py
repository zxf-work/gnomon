"""P1 parameter-file mode of the comparison layer (scripts/extcmp.py, extcmp_p1.py).

1. Mode off: the records, the verdicts and the tables are what the comparison
   layer wrote before the mode existed (the kit commit EXTCMP_P1_BASE, default
   records; query records identical except the timing fields and host facts
   (which differ between any two runs); stdout identical once timings are
   masked; window.sh passes no extra argument when EXTCMP_PARAMS is unset.
2. Mode on: every instance is one cell keyed `<id>#<instance>`, answers match
   the registry call with the same arguments, a wrong argument name is
   refused, the gate excludes per instance, the protocol counts its calls.

Needs the built engine (a small synthetic store is replayed under tmp_path)
and the repository's git history for the base commit.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
BASE = os.environ.get("EXTCMP_P1_BASE", "188d6e60")
SCALE = 3000

sys.path.insert(0, str(SCRIPTS))


def _git_show(path: str) -> str | None:
    r = subprocess.run(["git", "-C", str(ROOT), "show", f"{BASE}:{path}"], capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    base_src = _git_show("scripts/extcmp.py")
    if base_src is None:
        pytest.skip(f"base commit {BASE} not in this repository")
    tmp = tmp_path_factory.mktemp("p1")
    old = tmp / "old"
    shutil.copytree(SCRIPTS, old)
    (old / "extcmp.py").write_text(base_src)
    (old / "extcmp_p1.py").unlink()                     # mode off must not need it
    import eval_harness as H
    data = H.build_dataset(SCALE)
    log = tmp / "synth.eventlog.jsonl"
    shutil.copyfile(data.log, log)
    work = tmp / "work"
    subprocess.run([sys.executable, str(SCRIPTS / "extcmp.py"), "ref", "--log", str(log),
                    "--workdir", str(work), "--size-tag", "T"], check=True, capture_output=True)
    return {"tmp": tmp, "old": old / "extcmp.py", "new": SCRIPTS / "extcmp.py", "log": log,
            "work": work, "ref": work / "stores" / "native-T"}


def _run(script: Path, env, *args) -> subprocess.CompletedProcess:
    e = dict(os.environ, EXTCMP_RECORD_LABEL="p1-test")
    e.pop("EXTCMP_EXCLUDE", None)
    return subprocess.run([sys.executable, str(script), *map(str, args)], capture_output=True,
                          text=True, env=e, check=True)


TIMING = {"wall_ms", "p50_ms", "p95_ms", "timings_ms", "warmup_ms", "n", "protocol", "capped"}


def _strip(rec: dict) -> dict:
    # host facts and timings differ between any two runs; the manifest's git
    # fields describe where the script file lives (the base copy is outside git)
    rec = {k: v for k, v in rec.items() if k not in ("host", "open_s")}
    if "manifest" in rec:
        rec["manifest"] = {k: v for k, v in rec["manifest"].items()
                           if not k.startswith(("commit", "dirty", "git", "utc", "created"))}
    rec["results"] = [{k: v for k, v in c.items() if k not in TIMING} for c in rec["results"]]
    return rec


def _mask(out: str) -> str:
    return re.sub(r"ok=(True|False) [0-9.]+ ", r"ok=\1 # ", out)


@pytest.mark.parametrize("mode,extra", [("verify", []), ("timed", ["--family", "P"]), ("timed", ["--family", "T"])])
def test_mode_off_query_records_unchanged(env, mode, extra):
    recs, outs = [], []
    for tag, script in (("old", env["old"]), ("new", env["new"])):
        j = env["tmp"] / f"q-{tag}-{mode}-{'-'.join(extra)}.json"
        r = _run(script, env, "query", "--system", "native", "--log", env["log"], "--ref", env["ref"],
                 "--workdir", env["work"], "--size-tag", "T", "--mode", mode, *extra, "--json", j)
        recs.append(json.loads(j.read_text()))
        outs.append(_mask(r.stdout))
    assert _strip(recs[0]) == _strip(recs[1])
    assert set(recs[0]) == set(recs[1])                 # no new top-level field
    assert [set(c) for c in recs[0]["results"]] == [set(c) for c in recs[1]["results"]]
    assert outs[0] == outs[1]


def test_mode_off_verdict_and_tables_byte_identical(env):
    j = env["tmp"] / "native-verify.json"
    _run(env["new"], env, "query", "--system", "native", "--log", env["log"], "--ref", env["ref"],
         "--workdir", env["work"], "--size-tag", "T", "--mode", "verify", "--json", j)
    rec = json.loads(j.read_text())
    other = dict(rec, system="postgres")
    other["results"] = [dict(c) for c in rec["results"]]
    other["results"][0]["hash"] = "0" * 32                  # one DIFFERS
    other["results"][0]["proj_hash"] = "0" * 32
    other["results"][1] = {"query": other["results"][1]["query"], "ok": False, "error": "not written for this system"}
    o = env["tmp"] / "pg.json"
    o.write_text(json.dumps(other))
    outs = []
    for tag, script in (("old", env["old"]), ("new", env["new"])):
        vj = env["tmp"] / f"verdicts-{tag}.json"
        r = _run(script, env, "verdict", "--base", j, o, "--json", vj)
        outs.append((r.stdout, vj.read_bytes()))
        rot = env["tmp"] / f"rot-{tag}" / "rot1"
        rot.mkdir(parents=True)
        (rot / "P-native.json").write_text(json.dumps(rec))
        (rot / "P-postgres.json").write_text(json.dumps(other))
        r2 = _run(script, env, "bverdict", "--dir", rot)
        outs.append((r2.stdout, (rot / "table.json").read_bytes()))
    assert outs[0] == outs[2] and outs[1] == outs[3]


def test_window_sh_passes_nothing_when_unset():
    kit = Path(os.environ.get("P1_KIT", ROOT / "benchmarks/compare/kit"))
    if not (kit / "lib/window.sh").exists():
        pytest.skip("kit not in this tree (set P1_KIT)")
    w = (kit / "lib/window.sh").read_text()
    assert w.count('${EXTCMP_PARAMS:+--params "$EXTCMP_PARAMS"}') == 2
    r = subprocess.run(["bash", "-c", 'set -u; unset EXTCMP_PARAMS P1_VTAG; '
                        'printf "[%s]" a ${EXTCMP_PARAMS:+--params "$EXTCMP_PARAMS"} '
                        'verify-x${P1_VTAG:+-$P1_VTAG}.json'], capture_output=True, text=True)
    assert r.stdout == "[a][verify-x.json]"


def _param_file(env, instances) -> Path:
    import extcmp_p1 as P1
    p = env["tmp"] / f"params-{len(list(env['tmp'].glob('params-*')))}.json"
    p.write_text(json.dumps({"format": P1.FORMAT, "version": "test", "log": {"sha256": "x"},
                             "instances": instances}))
    return p


def test_mode_on_one_cell_per_instance(env):
    import eval_harness as H
    import extcmp as X
    data = X.dataset(env["log"])
    reg = {q.id: q for q in H.registry(data.t0, data.t1, data.tt_epoch1, data.filter_uids)}
    w = {"t_a": data.t0, "t_b": data.t0 + (data.t1 - data.t0) // 4}
    insts = [{"query": "hist.single", "instance": "a", "stratum": "s1", "args": {"uid": "n2"}},
             {"query": "hist.single", "instance": "b", "stratum": "s2", "args": {"uid": "n1"}},
             {"query": "reach.window", "instance": "c", "stratum": "s1",
              "args": {"src": "n3", "window": w}}]
    pf = _param_file(env, insts)
    j = env["tmp"] / "p1-native.json"
    _run(env["new"], env, "query", "--system", "native", "--log", env["log"], "--ref", env["ref"],
         "--workdir", env["work"], "--size-tag", "T", "--mode", "verify", "--ids",
         "hist.single,reach.window,snap.hop2", "--params", pf, "--json", j, "--answers", env["tmp"] / "ans")
    rec = json.loads(j.read_text())
    keys = [X.cell_key(c) for c in rec["results"]]
    assert keys == ["hist.single#a", "hist.single#b", "reach.window#c"]   # snap.hop2: no instance, not asked
    assert rec["param_file"]["n_instances"] == 3 and rec["protocol_p1"]["reps"] == 10
    assert (env["tmp"] / "ans" / "hist.single#a.json").exists()
    # instance b is the registry's own argument set: same answer hash as mode off
    j0 = env["tmp"] / "p1-off.json"
    _run(env["new"], env, "query", "--system", "native", "--log", env["log"], "--ref", env["ref"],
         "--workdir", env["work"], "--size-tag", "T", "--mode", "verify", "--ids", "hist.single", "--json", j0)
    off = json.loads(j0.read_text())["results"][0]
    assert reg["hist.single"].args == {"uid": "n1"}
    assert rec["results"][1]["hash"] == off["hash"]
    assert rec["results"][0]["hash"] != off["hash"]
    # gate: a differing instance is excluded for that system only
    other = dict(rec, system="postgres", results=[dict(c) for c in rec["results"]])
    other["results"][2]["hash"] = other["results"][2]["proj_hash"] = "f" * 32
    d = env["tmp"] / "gate"
    d.mkdir()
    (d / "verify-native-P.json").write_text(json.dumps(rec))
    (d / "verify-postgres-P.json").write_text(json.dumps(other))
    import extcmp_p1 as P1
    assert P1.gate(d, 10, d / "excluded.json") == 0
    assert json.loads((d / "excluded.json").read_text()) == {"postgres": {"reach.window#c": "excluded: DIFFERS"}}


def test_mode_on_refuses_wrong_argument_names(env):
    pf = _param_file(env, [{"query": "hist.single", "instance": "x", "args": {"uid": "n1", "limit": 5}}])
    r = subprocess.run([sys.executable, str(env["new"]), "query", "--system", "native", "--log", str(env["log"]),
                        "--ref", str(env["ref"]), "--workdir", str(env["work"]), "--size-tag", "T",
                        "--mode", "verify", "--ids", "hist.single", "--params", str(pf)],
                       capture_output=True, text=True)
    assert r.returncode != 0 and "differ from the registry" in (r.stderr + r.stdout)


def test_protocol_counts(monkeypatch):
    import time as _t
    import extcmp_p1 as P1
    calls = []

    def fast():
        calls.append(1)
        return {"x": 1}
    _, timed, warm, proto = P1.measure(fast)
    assert (len(warm), len(timed), len(calls)) == (2, 10, 12) and proto.startswith("standard")
    monkeypatch.setenv("EXTCMP_P1_SLOW_S", "0.001")
    calls.clear()

    def slow():
        calls.append(1)
        _t.sleep(0.003)
        return {"x": 1}
    _, timed, warm, proto = P1.measure(slow)
    assert (len(warm), len(timed), len(calls)) == (1, 3, 4) and proto.startswith("slow")
    assert P1.is_server_timeout("QueryCanceled: canceling statement due to statement timeout")
    assert not P1.is_server_timeout("DIFFERS")


def test_ceiling(monkeypatch):
    import time as _t
    import extcmp_p1 as P1
    monkeypatch.setenv("EXTCMP_P1_CEILING_S", "0.001")

    def slow():
        _t.sleep(0.003)
        return {"x": 1}
    with pytest.raises(P1.CeilingReached) as e:
        P1.measure(slow)
    assert P1.is_server_timeout(f"CeilingReached: {e.value}")


def test_first_call_retried_once(monkeypatch):
    import time as _t
    import extcmp_p1 as P1
    monkeypatch.setenv("EXTCMP_P1_CEILING_S", "0.05")
    calls = []

    def cold_then_warm():
        calls.append(1)
        if len(calls) == 1:
            _t.sleep(0.08)                      # a cold first call above the ceiling
        return {"x": 1}

    class Alarm(Exception):
        pass
    payload, wall, notes = P1.verify_call(cold_then_warm, 60, Alarm, object())
    assert len(calls) == 2 and wall < 50 and "first_call_ms" in notes
    calls.clear()
    _, timed, warm, proto = P1.measure(cold_then_warm)
    assert len(calls) == 13 and len(timed) == 10 and proto.startswith("standard")

    def always_slow():
        _t.sleep(0.08)
        return {"x": 1}
    with pytest.raises(P1.CeilingReached):
        P1.measure(always_slow)
    out = P1.verify_call(always_slow, 60, Alarm, object())
    assert out[1] > 50                          # above the ceiling twice: the gate excludes it


def test_mode_on_timed_cell(env):
    import extcmp as X
    pf = _param_file(env, [{"query": "hist.single", "instance": "t", "stratum": "s", "args": {"uid": "n3"}}])
    j = env["tmp"] / "p1-timed.json"
    _run(env["new"], env, "query", "--system", "native", "--log", env["log"], "--ref", env["ref"],
         "--workdir", env["work"], "--size-tag", "T", "--mode", "timed", "--family", "P", "--params", pf, "--json", j)
    rec = json.loads(j.read_text())
    (c,) = rec["results"]
    assert X.cell_key(c) == "hist.single#t" and c["ok"] and c["n"] == 10 and len(c["warmup_ms"]) == 2
    assert rec["window_prime"]["primed"] is True


def test_alarm_on_first_call_is_retried():
    import extcmp_p1 as P1
    calls, recovered = [], []

    class Alarm(Exception):
        pass

    def alarm_then_ok():
        calls.append(1)
        if len(calls) == 1:
            raise Alarm("client-side limit reached")
        return {"x": 1}
    _, timed, warm, proto = P1.measure(alarm_then_ok, Alarm, lambda: recovered.append(1))
    assert recovered == [1] and len(timed) == 10 and len(calls) == 13
