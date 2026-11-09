"""R-10M skip list (scripts/r10m.py) on synthetic W2 1M timed records."""
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import r10m  # noqa: E402

CEIL = ">10 s, aborted (CeilingReached: call took 10432 ms (client-side limit 10 s))"


def _write(root: Path, point: str, rot: int, fam: str, system: str, cells: list[dict]) -> None:
    d = root / point / f"rot{rot}"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{fam}-{system}.json").write_text(json.dumps({"system": system, "results": cells}))


def _cell(q, i, ok=False, error=None, aborted=False, excluded=False):
    c = {"query": q, "instance": i, "ok": ok}
    if error:
        c["error"] = error
    if aborted:
        c["aborted"] = True
    if excluded:
        c["excluded"] = True
    return c


def test_every_instance_censored_in_every_rotation_is_skipped(tmp_path):
    for rot in (1, 2):
        _write(tmp_path, "K1M", rot, "T", "neo4j", [
            _cell("paths.k", "a", error=CEIL, aborted=True), _cell("paths.k", "b", error=CEIL, aborted=True),
            _cell("reach.window", "a", error=CEIL, aborted=True),
            _cell("reach.window", "b", ok=(rot == 2), error=None if rot == 2 else CEIL, aborted=rot != 2),
            _cell("snap.hop2", "a", error=CEIL, aborted=True),
            _cell("snap.hop2", "b", error="ClientError: syntax"),           # an error that is not the ceiling
        ])
    d = r10m.build(tmp_path, "K1M", "neo4j")
    assert sorted(d["skip"]) == ["paths.k"]
    assert d["skip"]["paths.k"] == {"instances": 4, "censored": 4}
    assert "reach.window" in d["run"] and "snap.hop2" in d["run"]
    assert d["label"] == "> 10 s (R-10M: all instances censored at 1M)"


def test_rule_is_per_system_and_gnomon_is_not_special(tmp_path):
    _write(tmp_path, "K1M-d10", 1, "B", "native", [_cell("b.reach@T", "a", error=CEIL, aborted=True)])
    _write(tmp_path, "K1M-d10", 1, "B", "postgres", [_cell("b.reach@T", "a", ok=True)])
    assert sorted(r10m.build(tmp_path, "K1M-d10", "native")["skip"]) == ["b.reach@T"]
    assert r10m.build(tmp_path, "K1M-d10", "postgres")["skip"] == {}


def test_server_side_limits_count_as_censored_and_exclusions_do_not(tmp_path):
    _write(tmp_path, "K1M", 1, "N", "postgres", [
        _cell("coactive.narrow", "a", error="QueryCanceled: canceling statement due to statement timeout"),
        _cell("coactive.narrow", "b", error="excluded: DIFFERS", excluded=True),
    ])
    assert sorted(r10m.build(tmp_path, "K1M", "postgres")["skip"]) == ["coactive.narrow"]


def test_no_1m_record_skips_nothing(tmp_path):
    out = tmp_path / "skip.json"
    subprocess.run([sys.executable, str(Path(r10m.__file__)), "build", "--from", str(tmp_path), "--point", "K1M",
                    "--system", "clickhouse", "--out", str(out)], check=True)
    d = json.loads(out.read_text())
    assert d["skip"] == {} and d["files"] == []
