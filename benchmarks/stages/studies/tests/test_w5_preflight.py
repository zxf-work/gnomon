""""""
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location("w5_preflight", Path(__file__).resolve().parents[1] / "tools" / "w5_preflight.py")
pf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pf)


def setup(tmp_path, file_cells, rec_cells):
    (tmp_path / "params-T-reg.json").write_text(json.dumps({"instances": [{"query": q, "instance": i} for q, i in file_cells]}))
    (tmp_path / "params-T-bel.json").write_text(json.dumps({"instances": []}))
    for s in ("duckdbsql", "postgres", "neo4j"):
        (tmp_path / f"verify-{s}-reg.json").write_text(json.dumps({"results": [{"query": q, "instance": i} for q, i in rec_cells]}))
        (tmp_path / f"verify-{s}-bel.json").write_text(json.dumps({"results": []}))


def test_equal_sets_pass(tmp_path):
    setup(tmp_path, [("a", "1")], [("a", "1")])
    assert pf.check(str(tmp_path), "T", str(tmp_path), False)[0] == []


def test_a_missing_or_extra_cell_fails(tmp_path):
    setup(tmp_path, [("a", "1")], [("a", "2")])
    assert pf.check(str(tmp_path), "T", str(tmp_path), False)[0]


def test_signed_refuses_other_files(tmp_path):
    setup(tmp_path, [("a", "1")], [("a", "1")])
    (tmp_path / "params-W5-wikidata-v2-reg.json").write_text("x")
    (tmp_path / "params-W5-wikidata-v2-bel.json").write_text("x")
    (tmp_path / "match-W5-wikidata-v2.json").write_text("x")
    assert any("sha256" in b for b in pf.check(str(tmp_path), "T", str(tmp_path), True)[0])
