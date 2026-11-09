#!/usr/bin/env python3
"""The kit's scripts/w3_snb_run.py (same arguments) with the stage's Cypher text overrides (tools/w3_timed.py apply_override)."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import w3_timed  # noqa: E402

w3_timed.apply_override()
import w3_snb_run as R  # noqa: E402,E501  (the repo and scripts paths were put on sys.path by apply_override)

sys.argv = ["w3_snb_run.py"] + sys.argv[1:]
sys.exit(R.main())
