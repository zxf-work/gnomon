"""The runnable examples (`examples/*.py`) and the outputs the guide shows.

Every example runs in a child process. Its standard output must equal,
line for line, the output block the user guide (`docs/guide/*.md`) prints
for it. A block is a fenced ``text`` block directly after a line

    <!-- example-output: <file name> -->

so the guide cannot drift from what the examples print. Each example must
have exactly one such block.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = sorted((ROOT / "examples").glob("*.py"))
GUIDE = sorted((ROOT / "docs" / "guide").glob("*.md"))

_BLOCK = re.compile(r"<!-- example-output: ([\w.-]+) -->\n```text\n(.*?)```", re.DOTALL)


def _documented() -> dict[str, list[str]]:
    """{example file name: [the guide's output blocks for it]}"""
    found: dict[str, list[str]] = {}
    for page in GUIDE:
        for name, body in _BLOCK.findall(page.read_text(encoding="utf-8")):
            found.setdefault(name, []).append(body)
    return found


DOCUMENTED = _documented()


def _lines(text: str) -> list[str]:
    return [line.rstrip() for line in text.strip("\n").splitlines()]


def test_examples_exist():
    assert EXAMPLES, "no examples found"
    assert GUIDE, "no guide pages found"


def test_every_output_block_names_an_example():
    names = {p.name for p in EXAMPLES}
    assert sorted(set(DOCUMENTED) - names) == []


@pytest.mark.parametrize("example", EXAMPLES, ids=lambda p: p.name)
def test_example_prints_the_documented_output(example: Path, tmp_path: Path):
    blocks = DOCUMENTED.get(example.name, [])
    assert len(blocks) == 1, f"{example.name}: expected one output block in the guide"
    env = {**os.environ, "TMPDIR": str(tmp_path)}
    run = subprocess.run([sys.executable, str(example)], check=False, cwd=tmp_path, env=env,
                         capture_output=True, text=True, timeout=600)
    assert run.returncode == 0, run.stderr
    assert _lines(run.stdout) == _lines(blocks[0])
    # each example cleans up after itself
    assert [p.name for p in tmp_path.iterdir()] == []
