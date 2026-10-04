#!/usr/bin/env python3
"""One spelling of "which commit was this measured at", that survives a
compute node with no `git` installed.

Some compute nodes have no `git` binary. A harness that resolves its commit
by shelling out therefore records `unknown` there, and a naive harness turns
that into something worse than a missing value: computing `dirty` as
`bool(sh(...))` where `sh` returns the string `"unknown"` on failure makes a
perfectly clean checkout on a git-less node report `dirty: true`, and an
`"unknown0000"`-style placeholder fails the result-manifest schema's
`^[0-9a-f]{7,40}(-dirty)?$` outright.

This module honours a `GNOMON_COMMIT` environment variable first, then `git`,
with the two details that matter made explicit.

**A missing answer is `None`, never `False` and never `True`.** `dirty()`
returns `None` when it could not be determined. Callers that must emit a
boolean should emit `null`; a run whose cleanliness is unknown is not the
same as a clean run, and it is certainly not a dirty one.

**The fallback is schema-valid and obviously fake.** Forty zeros satisfies
the result-manifest pattern and could not be mistaken for a real SHA, so a
receipt written without provenance is loud rather than malformed.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

#: What the result-manifest schema will accept in `git_commit`.
SHA_RE = re.compile(r"^[0-9a-f]{7,40}(-dirty)?$")

#: A schema-valid value that is unmistakably not a commit.
UNKNOWN_SHA = "0" * 40

ROOT = Path(__file__).resolve().parent.parent


def _run(*cmd: str, cwd: Path | None = None) -> str | None:
    try:
        return subprocess.run(cmd, cwd=str(cwd or ROOT), capture_output=True,
                              text=True, check=True).stdout.strip()
    except Exception:                                        # noqa: BLE001
        return None


def env_commit() -> str | None:
    """`GNOMON_COMMIT`, if it is set and looks like a commit.

    Validated rather than trusted: a typo'd or templated value that does not
    match the schema pattern would otherwise be written into a receipt and
    fail validation far from here, where the cause is no longer visible.
    """
    v = (os.environ.get("GNOMON_COMMIT") or "").strip().lower()
    return v if v and SHA_RE.match(v) else None


def dirty(cwd: Path | None = None) -> bool | None:
    """Whether the working tree has uncommitted *tracked* changes.

    `None` means "could not be determined" — no git, or not a repository.
    Never guess: this value gates whether a measurement may be quoted.

    `-uno` because untracked files on a runner are scratch, not provenance
    dirt (a build artefact or a receipt written next to the checkout is not
    a modification of the measured code).
    """
    if env_commit() is not None and os.environ.get("GNOMON_COMMIT_DIRTY") is None:
        # provenance was supplied out of band; the tree here may not even be
        # the tree that was built, so this process cannot answer the question
        return None
    out = _run("git", "status", "--porcelain", "-uno", cwd=cwd)
    return None if out is None else bool(out)


def commit(short: bool = False, cwd: Path | None = None,
           with_dirty: bool = True) -> str:
    """The commit this code was built from, as the schema wants it.

    Order: `GNOMON_COMMIT` (verbatim, already validated), then `git`, then the
    obviously-fake fallback. `-dirty` is appended only when the tree was
    *determined* to be dirty — never when that could not be determined, and
    never on top of a `GNOMON_COMMIT` that already carries the suffix.
    """
    env = env_commit()
    if env is not None:
        return env
    # `HEAD` is not optional and `--short=12` is not a revision. Spelling this
    # `"--short=12" if short else "HEAD"` meant the short form ran
    # `git rev-parse --short=12` with nothing to parse, which fails with
    # "Needed a single revision" -- so every caller passing short=True got the
    # 40-zero fallback from a checkout where git was working perfectly.
    argv = ["git", "rev-parse"]
    if short:
        argv.append("--short=12")
    argv.append("HEAD")
    sha = _run(*argv, cwd=cwd)
    if not sha:
        return UNKNOWN_SHA
    sha = sha.lower()
    if with_dirty and dirty(cwd) is True:
        sha += "-dirty"
    return sha


def describe(cwd: Path | None = None) -> dict[str, object]:
    """Both fields at once, for a manifest block.

    `dirty` is `None` rather than a boolean when unknown, so a consumer can
    tell "clean" from "nobody could say".
    """
    return {"commit": commit(cwd=cwd, with_dirty=False),
            "dirty": dirty(cwd),
            "commit_source": ("GNOMON_COMMIT" if env_commit() is not None
                              else "git" if _run("git", "rev-parse", "HEAD",
                                                 cwd=cwd) else "unavailable")}
