""""""
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parents[1] / "tools"
KIT = Path(__file__).resolve().parents[4] / "benchmarks/compare/kit/lib"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


hub = load("abl_hub_report", HERE / "abl_hub_report.py")
par = load("abl_w2_params", HERE / "abl_w2_params.py")
post = load("abl_postcheck", HERE / "abl_postcheck.py")
sched = load("abl_schedule", KIT / "abl_schedule.py")


def cell(q, i, p50=None, h="a", stratum="p50", **kw):
    c = {"query": q, "instance": i, "stratum": stratum, "ok": p50 is not None, "p50_ms": p50, "hash": h}
    c.update(kw)
    return c


def write(d, name, cells):
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{name}.json").write_text(json.dumps({"results": cells}))


def test_hub_report_splits_and_bounds(tmp_path):
    f = tmp_path / "K10M-d0/rot1/T"
    write(f, "unset~x", [cell("snap.hop2", "h1", 100.0, stratum="hub"), cell("snap.hop2", "n1", 10.0)])
    write(f, "x", [cell("snap.hop2", "h1", None, stratum="hub", censored=True, error=">10 s, aborted"), cell("snap.hop2", "n1", 30.0)])
    s = hub.summarize(tmp_path)
    by = {r["group"]: r for r in s["rows"]}
    assert by["hub"]["n_censored"] == 1 and by["hub"]["median_ratio"] == 100.0     # 10,000 ms / 100 ms, a lower bound
    assert by["non-hub"]["n_censored"] == 0 and by["non-hub"]["median_ratio"] == 3.0


def test_hub_report_mismatch_is_listed_not_used(tmp_path):
    f = tmp_path / "K10M-d0/rot1/T"
    write(f, "unset~x", [cell("snap.hop2", "n1", 10.0, h="a")])
    write(f, "x", [cell("snap.hop2", "n1", 30.0, h="b")])
    s = hub.summarize(tmp_path)
    assert s["answer_mismatches"] and not s["rows"]


def test_params_keep_every_hub_and_are_deterministic():
    ins = [{"query": "q", "instance": f"n{i}", "class": "c", "stratum": "p50"} for i in range(10)]
    ins += [{"query": "q", "instance": f"h{i}", "class": "c", "stratum": "hub"} for i in range(4)]
    a = par.pick({"instances": ins}, "K10M", 3)
    b = par.pick({"instances": list(reversed(ins))}, "K10M", 3)
    assert sum(1 for i in a if i["stratum"] == "hub") == 4
    assert sum(1 for i in a if i["stratum"] != "hub") == 3
    assert {i["instance"] for i in a} == {i["instance"] for i in b}


def test_postcheck(tmp_path):
    assert post.problems(tmp_path)                                    # no report
    (tmp_path / "report.json").write_text(json.dumps({"points": {"K": {"answer_mismatches": [], "missing_pairs": []}}}))
    assert post.problems(tmp_path) == []
    (tmp_path / "window-failed.txt").write_text("WINDOW FAILED x\n")
    assert post.problems(tmp_path)


def test_new_arms_are_gated_and_not_in_the_default_list():
    for a in sched.NEW_ARMS6:
        assert a in sched.ARMS and a not in sched.DEFAULT_ARMS
    assert "range_visibility_off" in sched.DEFAULT_ARMS and "aa" in sched.DEFAULT_ARMS


def test_pair_check(tmp_path):
    chk = KIT / "abl_pair_check.py"
    f = tmp_path / "T"
    write(f, "unset~x", [cell("snap.hop2", "n1", 10.0, h="a")])
    assert subprocess.run([sys.executable, str(chk), str(f), "unset~x"]).returncode == 0      # the partner has not run yet
    write(f, "x", [cell("snap.hop2", "n1", 12.0, h="a")])
    assert subprocess.run([sys.executable, str(chk), str(f), "x"]).returncode == 0
    write(f, "x", [cell("snap.hop2", "n1", 12.0, h="b")])
    assert subprocess.run([sys.executable, str(chk), str(f), "x"]).returncode == 7


def test_hub_report_confirmation_is_labelled_and_filtered(tmp_path):
    lab = tmp_path / "lab"
    write(lab / "K10M-d0/rot1/T", "unset~x", [cell("snap.hop2", "h1", 100.0, stratum="hub")])
    write(lab / "K10M-d0/rot1/T", "x", [cell("snap.hop2", "h1", 200.0, stratum="hub")])
    c = tmp_path / "conf"
    write(c / "K10M/rot1", "T-native", [cell("snap.hop2", "h1", 100.0, stratum="hub"), cell("snap.hop2", "zz", 100.0)])
    write(c / "K10M/rot1", "T-duckdb", [cell("snap.hop2", "h1", 500.0, stratum="hub"), cell("snap.hop2", "zz", 900.0)])
    s = hub.summarize(lab, c, {"h1"})
    r = [x for x in s["rows"] if x["arm"].startswith("duckdb_storage")]
    assert len(r) == 1 and r[0]["median_ratio"] == 5.0 and r[0]["group"] == "hub"
