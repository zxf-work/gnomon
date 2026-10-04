"""`eval_harness.canonical_hash` judges the answer, not the call metadata.

Two payloads that differ only in envelope metadata fields (the operator
name, the echoed arguments, the dataset extent, the digest) must hash equal;
two that differ in content must not.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _harness():
    sys.path.insert(0, str(ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location(
        "eval_harness_hash_under_test", ROOT / "scripts" / "eval_harness.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod          # dataclasses resolve their module here
    spec.loader.exec_module(mod)
    return mod


ANSWER = {"rows": [{"t_a": 0, "t_b": 10, "value": 3}], "rows_total": 1,
          "truncated": False, "n_buckets": 1}


def test_payloads_differing_only_in_metadata_hash_equal():
    H = _harness()
    a = dict(ANSWER, op="graph_metric_timeseries",
             dataset_extent={"vt_min": 0, "vt_max": 10})
    b = dict(ANSWER)                          # no metadata at all
    c = dict(ANSWER, op="graph_metric_timeseries", args_echo={"window": None},
             dataset_extent={"vt_min": 5, "vt_max": 99}, result_digest="b" * 64)
    assert H.canonical_hash(a) == H.canonical_hash(b) == H.canonical_hash(c)


def test_payloads_differing_in_content_do_not_hash_equal():
    H = _harness()
    a = dict(ANSWER, dataset_extent={"vt_min": 0, "vt_max": 10})
    b = dict(ANSWER, rows=[{"t_a": 0, "t_b": 10, "value": 4}],
             dataset_extent={"vt_min": 0, "vt_max": 10})
    assert H.canonical_hash(a) != H.canonical_hash(b)


def test_every_envelope_metadata_field_is_excluded():
    from gnomon.temporal.algebra import ENVELOPE_META_FIELDS
    H = _harness()
    for key in ENVELOPE_META_FIELDS:
        assert key in H.VOLATILE, key
