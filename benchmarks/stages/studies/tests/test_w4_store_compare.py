""""""
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location("c", Path(__file__).resolve().parents[1] / "tools" / "w4_store_compare.py")
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)


def test_compare(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    (a / "seg").mkdir(parents=True); (b / "seg").mkdir(parents=True)
    (a / "seg/x").write_text("1"); (b / "seg/x").write_text("1")
    (a / "seg/y").write_text("2"); (b / "seg/y").write_text("33")
    (a / "only").write_text("4"); (b / "seg/z").write_text("4")
    r = c.compare(c.manifest(a), c.manifest(b))
    assert r["identical"] == 1 and r["differ"][0][0] == "seg/y" and r["only_a"] == [("only", 1)] and r["only_b"] == [("seg/z", 1)]
    assert r["contents_only_a"] == 1 and r["contents_only_b"] == 1      # "2" and "33"; "4" is under another name
