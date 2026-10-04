"""Command line: thin argparse wrappers over the library — no logic here."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any


#: What `--backend` means when it is not given: nothing, so `gnomon.open`
#: decides — an existing store keeps the layout it was written with, and a
#: new one gets `DEFAULT_BACKEND` (native). Naming "native" here would make
#: an ingest into a pre-existing DuckDB reference store create an empty
#: native store beside it, which would read as data loss.
BACKEND_DEFAULT: str | None = None
BACKENDS = ["native", "duckdb"]
BACKEND_HELP = ("storage backend; default: the existing store's layout, or "
                "the native engine for a new store. duckdb is an optional "
                "reference backend (`pip install gnomon[duckdb]`).")


def build_parser() -> argparse.ArgumentParser:
    """The full argument parser, separate from `main` so tests can inspect
    the defaults a clean install actually gets."""
    p = argparse.ArgumentParser(prog="gnomon")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_ing = sub.add_parser("ingest", help="ingest an event JSONL file into a store")
    p_ing.add_argument("events_jsonl")
    p_ing.add_argument("--store", required=True)
    p_ing.add_argument("--backend", default=BACKEND_DEFAULT,
                       choices=BACKENDS, help=BACKEND_HELP)

    p_rep = sub.add_parser("replay", help="rebuild a store from a recorded "
                           "event log (byte-identical; preserves transaction "
                           "times, unlike a fresh ingest)")
    p_rep.add_argument("eventlog_jsonl")
    p_rep.add_argument("--store", required=True)
    p_rep.add_argument("--backend", default=BACKEND_DEFAULT,
                       choices=BACKENDS, help=BACKEND_HELP)
    p_rep.add_argument("--compact-every", type=int, default=None,
                       help="compact()+gc() the store every N applied batches "
                            "during replay, bounding the O(batches^2) "
                            "uncompacted-manifest cost on a long "
                            "history (native backend only)")

    p_call = sub.add_parser("call", help="call one operator against a store")
    p_call.add_argument("--store", required=True)
    p_call.add_argument("op")
    p_call.add_argument("args_json")

    p_store = sub.add_parser(
        "store", help="native store maintenance",
        description="native store maintenance. backup/restore: "
                    "`--store` always names the store this "
                    "invocation acts on or creates; `--dest` always names "
                    "the backup directory. `store backup --store <src> "
                    "--dest <backup_dir>` writes a quiesced copy (the "
                    "event log, the authoritative artifact, plus a "
                    "manifest record) to <backup_dir>. `store restore "
                    "--dest <backup_dir> --store <new_store>` replays that "
                    "backed-up log into a fresh store at <new_store> "
                    "(reusing `gnomon replay`'s path), then compares the "
                    "result's identity and logical digest against the "
                    "backup manifest, printing PASS/FAIL with a nonzero "
                    "exit on any mismatch — including a tampered log, "
                    "caught before replay even starts.")
    p_store.add_argument(
        "action", choices=["gc", "compact", "settle", "digest", "verify",
                           "build-indexes", "upgrade-manifests", "backup",
                           "restore"],
        help="gc: drop superseded generations and the files only they "
             "reference. compact: re-sort the live rows into fresh segments. "
             "settle: a one-shot, read-optimising merge over every live "
             "segment of every lane that is above its byte floor, run at "
             "quiescence. The incremental compactor buys its write saving by "
             "leaving partially-filled size-tier windows unmerged, and those "
             "same windows are the segments a whole-extent query must open; "
             "settle pays that residue off once, rewriting each row exactly "
             "once and ending at the store's byte floor (store bytes over the "
             "segment target, per lane, plus one partial per lane — never a "
             "fixed segment count). Memory is bounded by --budget-bytes / "
             "GNOMON_SETTLE_BYTES and is flat in the store's size. The logical "
             "store, transaction time and every digest are unchanged, so a "
             "store may be settled in place without becoming a different "
             "build. It rebuilds the per-segment indexes it orphans unless "
             "--no-index is given, and it never calls gc: run "
             "`store gc --keep 2` afterwards to reclaim the superseded "
             "segments it reports as reclaimable_bytes. "
             "digest: print the store's content_digest — store_digest's "
             "fields minus vid/tt_s/tt_e, all three stamped from the "
             "wall-clock transaction clock, so this is the identity two "
             "independent builds of the same logical events can actually be "
             "compared on. The default pass sorts every logical row "
             "in memory; --streaming computes the same value inside the "
             "engine in memory bounded by --budget-bytes and the merge "
             "fan-in, which is what makes the pass survivable at 300M rows "
             "and beyond. Read-only: no segment, manifest or generation is "
             "touched. "
             "build-indexes: pre-build the rebuildable indexes under "
             "native/idx/ for every segment the current generation names "
             "(--tcsr: the per-segment adjacency index; --name: "
             "the sorted name index; pass either or both), "
             "instead of letting the first query pay for them. Indexes are "
             "never authoritative and always rebuildable, so this is safe to "
             "run, safe to skip, and idempotent — an index already on disk "
             "and still fresh is left untouched, not rebuilt. It is the "
             "deployment answer for a latency-sensitive long-running process, and "
             "for a benchmark store that must be measured at steady state. "
             "verify: checksum-walk everything this generation names. "
             "upgrade-manifests: convert a store written by an earlier engine "
             "(on-disk manifest format 1, one full manifest per generation, "
             "or format 2, the checkpoint-plus-delta chain with a "
             "whole-document manifest_sha) to format 3, which this build "
             "writes: the same chain, with manifest_sha a Merkle root over "
             "the ordered segment set. A format-1 or format-2 store opens and "
             "reads fine but refuses every write until this is run. It "
             "republishes the current content as one checkpoint and flips "
             "CURRENT: one manifest written, no segment, close run, "
             "dictionary or event-log byte touched, and it is safe to run "
             "twice. The generation counter advances by one and manifest_sha "
             "changes, so any persisted TCSR index rebuilds on next use and "
             "build receipts must not be compared on manifest_sha across "
             "formats.")
    p_store.add_argument("--store", required=True,
                         help="gc/compact/settle/digest/verify/backup: the "
                              "store to act on. restore: the fresh store to "
                              "create")
    p_store.add_argument("--dest", default=None,
                         help="backup: directory to write the backup into. "
                              "restore: the backup directory to restore from "
                              "(required for both)")
    p_store.add_argument("--keep", type=int, default=2,
                         help="gc: generations to retain besides those "
                              "pinned by live readers (default 2)")
    p_store.add_argument("--tcsr", action="store_true",
                         help="build-indexes: build the per-segment temporal "
                              "CSR (native/idx/tcsr/). With no index flag at "
                              "all, every kind is built.")
    p_store.add_argument("--json", action="store_true",
                         help="verify: emit JSON instead of prose")
    p_store.add_argument("--name", action="store_true",
                         help="build-indexes: build the sorted name index "
                              "(native/idx/name/). With no "
                              "index flag at all, every kind is built.")
    p_store.add_argument("--vidfilter", action="store_true",
                         help="build-indexes: build the per-segment identity "
                              "filter (native/idx/vidfilter/). The engine "
                              "builds these at seal, so this is the "
                              "migration path for a store written before "
                              "they existed, not the normal way they arrive. "
                              "With no index flag at all, every kind is "
                              "built.")
    p_store.add_argument("--streaming", action="store_true",
                         help="digest: compute the content digest inside the "
                              "engine, in memory bounded by --budget-bytes "
                              "and the merge fan-in instead of by the row "
                              "count. Byte-identical to the default pass, "
                              "which materialises every logical row and is "
                              "the reference the streaming one is proved "
                              "against; at 100M rows that pass alone is tens "
                              "of GB, which is what this flag is for.")
    p_store.add_argument("--budget-bytes", type=int, default=None,
                         help="settle: cap on the input bytes one merge group "
                              "spans, and so on settle's peak memory. "
                              "Overrides GNOMON_SETTLE_BYTES for this call; 0 "
                              "means unbounded (one group per lane). Flat in "
                              "the store's size either way — a bigger budget "
                              "buys fewer output partials, not less memory "
                              "growth. digest --streaming: cap on the sort "
                              "buffer, overriding GNOMON_DIGEST_BYTES; 0 means "
                              "unbounded, which is the non-streaming pass's "
                              "memory profile.")
    p_store.add_argument("--no-index", dest="build_indexes",
                         action="store_false", default=True,
                         help="settle: skip rebuilding the per-segment TCSR, "
                              "name and identity-filter indexes that the "
                              "merge orphans. Off "
                              "by default because a settled store whose "
                              "indexes are cold makes the first queries pay a "
                              "full-file sha256 and an index build per "
                              "segment — which is exactly the latency settle "
                              "exists to improve. Provided for ablation.")
    mode = p_store.add_mutually_exclusive_group()
    mode.add_argument("--fast", dest="mode", action="store_const", const="fast",
                      help="verify: the file walk only — every segment, close "
                           "run, dictionary and manifest this generation "
                           "names, checksummed against what the manifest "
                           "claims. The default.")
    mode.add_argument("--full", dest="mode", action="store_const", const="full",
                      help="verify: everything --fast checks, plus the "
                           "invariants that span files — the manifest parent "
                           "chain across every retained generation, "
                           "dictionary-code reference validity, the "
                           "bitemporal row invariants, the event log's "
                           "framing and hash chain against the applied "
                           "cursor, and the persisted TCSR index. "
                           "Read-only: a torn log tail is reported, never "
                           "trimmed.")
    p_store.set_defaults(mode="fast")

    p_check = sub.add_parser(
        "check", help="full integrity check of a store (alias of "
                      "`store verify --full`)",
        description="Read-only, exhaustive integrity check: every layer, "
                    "every finding reported as {layer, kind, path, "
                    "generation, detail, severity}. Exits 0 when the store "
                    "is clean, 1 when there are findings, and 2 when the "
                    "store cannot be opened at all. Nothing is repaired — "
                    "the remedy for a corrupt store is `gnomon replay` from "
                    "its event log.")
    p_check.add_argument("store", help="the store to check")
    p_check.add_argument("--json", action="store_true",
                         help="emit the report as JSON instead of prose")

    return p


def _store_verify(path: str, mode: str, as_json: bool) -> int:
    """`gnomon store verify` / `gnomon check`. Exit 0 clean, 1 findings, 2 unreadable.

    Opened **read-only**, always. A read-write handle runs crash recovery on
    open, and recovery trims a torn event-log tail — so a writer handle would
    quietly repair the very defect full mode exists to report, and the check
    would then pass on a store it had just changed. Read-only also means a
    monitoring process can check a store its writer is still using.

    Exit 2 is reserved for "cannot be checked": the store will not open at
    all, because `CURRENT` is malformed, a manifest fails its own checksum,
    or the dictionary is shorter than the manifest commits. Those are
    refusals the engine makes before any report exists, and they are a
    different answer from "checked, and here is what is wrong" — the
    corruption sweep has to tell the two apart.

    The adapter is constructed directly rather than through `gnomon.open`,
    which is not a shortcut: `Store.__init__` seeds its clock by scanning the
    whole event log, and that scan *raises* on the first record it cannot
    parse. Going through it would turn every torn tail into exit 2 —
    "unreadable" — when the store itself is perfectly readable and the log's
    tail is precisely the finding full mode exists to report.
    """
    from pathlib import Path

    from gnomon.storage.native import NativeAdapter

    engine_dir = Path(path) / "native"
    try:
        if not engine_dir.is_dir():
            raise FileNotFoundError(f"there is no native store at {engine_dir}")
        store = NativeAdapter(engine_dir, read_only=True)
    except Exception as e:
        report = {"store": path, "mode": mode, "readable": False,
                  "reason": f"{type(e).__name__}: {e}"}
        if as_json:
            print(json.dumps(report, sort_keys=True))
        else:
            print(f"store:      {path}")
            print(f"\nverdict: UNREADABLE — {report['reason']}")
            print("\nthis store cannot be checked at all; rebuild it from its "
                  "event log with `gnomon replay`")
        return 2
    try:
        r = store.verify(mode=mode)
    finally:
        store.close()
    # the adapter names the engine directory it walked; the report names the
    # store the operator asked about, so the two exits agree on the subject
    r["store"] = path
    r["readable"] = True

    if as_json:
        print(json.dumps(r, sort_keys=True))
        return 0 if r["healthy"] else 1

    findings = r["findings"]
    errors = [f for f in findings if f["severity"] == "error"]
    advisories = [f for f in findings if f["severity"] != "error"]
    print(f"store:      {path}")
    print(f"mode:       {r['mode']}")
    print(f"generation: {r['generation']}")
    print(f"checked:    {r['segments_checked']} segments "
          f"({r['rows']} rows), {r['close_runs_checked']} close runs "
          f"({r['closes']} closes), {r['dict_records']} dictionary "
          f"records")
    if r["mode"] == "full":
        print(f"walked:     {r['rows_walked']} rows "
              f"({r['believed_rows']} believed) across "
              f"{r['identities_checked']} identities")
    print(f"layout:     {r['tt_s_runs']} tt_s runs across live "
          f"segments, {r['max_tt_s_runs']} in the worst one")
    for title, group in (("PROBLEMS", errors), ("ADVISORIES", advisories)):
        if not group:
            continue
        print(f"\n{title} ({len(group)}):")
        for f in group:
            where = f" {f['path']}" if f["path"] else ""
            print(f"  - [{f['layer']}/{f['kind']}]{where}: {f['detail']}")
    if errors:
        print("\nverdict: CORRUPT — do not trust this store; rebuild "
              "it from its event log with `gnomon replay`")
    elif r["mode"] == "full":
        print("\nverdict: healthy — every referenced file passed its "
              "checksums, every cross-reference resolved, and every "
              "bitemporal invariant held")
    else:
        print("\nverdict: healthy — every referenced file passed its "
              "checksums and cross-references")
    return 0 if not errors else 1


#: `store backup`'s manifest filename inside `--dest` — read back verbatim
#: by `store restore`. Not a store-format file: it lives in
#: the backup directory, never inside a live store's own layout.
BACKUP_MANIFEST_NAME = "backup_manifest.json"


def _git_commit() -> str | None:
    """Best-effort `git rev-parse HEAD`, or None outside a git checkout."""
    import subprocess

    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                             text=True, timeout=5)
        return out.stdout.strip() or None
    except OSError:
        return None


def _store_build_indexes(path: str, tcsr: bool = False, name: bool = False,
                         vidfilter: bool = False) -> int:
    """`gnomon store build-indexes` - pre-build `native/idx/` (`--tcsr` the
    per-segment adjacency index, `--name` the sorted
    name index, `--vidfilter` the per-segment identity filter).

    With no index flag, every kind is built; a flag narrows it (pass both
    to build both explicitly). Opened through `NativeAdapter` directly
    rather than `gnomon.open`, for the same reason `upgrade-manifests` is:
    this touches nothing but derived files and has no business replaying an
    event-log suffix to do it.
    """
    from pathlib import Path as _IdxPath

    from gnomon.storage.native import NativeAdapter

    kinds = {"tcsr": tcsr, "name": name, "vidfilter": vidfilter}
    if not any(kinds.values()):
        kinds = dict.fromkeys(kinds, True)
    adapter = NativeAdapter(_IdxPath(path) / "native")
    try:
        report = adapter.build_indexes(tcsr=kinds["tcsr"], name=kinds["name"],
                                       vidfilter=kinds["vidfilter"])
    finally:
        adapter.close()
    print(json.dumps(report))
    return 0


def _store_backup(src: str, dest: str | None) -> int:
    """`gnomon store backup --store <src> --dest <dest>`: a quiesced copy.

    The event log is the authoritative artifact — it is
    copied verbatim, byte for byte — plus a manifest record identifying
    exactly what was backed up: `store_identity`, the source generation and
    its `manifest_sha`, the copied log's own sha256/record count, and the
    tooling that made the backup. `read_only=True` matters here: it is the
    one open mode that never runs `Store._recover` or publishes a
    generation (`gnomon/store.py::Store._recover`'s "recovery is a writer's
    act"), so backing up a store never mutates it — this snapshots exactly
    what is currently durable on disk, nothing more.
    """
    import hashlib
    import shutil
    from pathlib import Path

    import gnomon
    from gnomon.storage.eventlog import EventLog

    if not dest:
        print("store backup: --dest is required", file=sys.stderr)
        return 2
    src_path, dest_path = Path(src), Path(dest)
    dest_path.mkdir(parents=True, exist_ok=True)

    store = gnomon.open(src_path, backend="native", read_only=True)
    try:
        store_identity = store.store_identity
        generation = store.adapter.generation
        manifest_sha = store.adapter._store.manifest_sha()
        logical_digest = store.digest()
    finally:
        store.close()

    src_log = src_path / "eventlog.jsonl"
    dest_log = dest_path / "eventlog.jsonl"
    shutil.copyfile(src_log, dest_log)
    log_sha256 = hashlib.sha256(dest_log.read_bytes()).hexdigest()
    log_records = sum(1 for _ in EventLog(dest_log).batches())

    manifest = {
        "store_identity": store_identity,
        "generation": generation,
        "manifest_sha": manifest_sha,
        "log_sha256": log_sha256,
        "log_records": log_records,
        # not one of the plan's named fields, but restore needs a content
        # check beyond "the bytes are the bytes I copied" — this is the
        # backend-independent check (`StorageAdapter.store_digest`) that
        # verifies replaying that log actually reconstructs the same store.
        "logical_digest": logical_digest,
        "gnomon_version": getattr(gnomon, "__version__", None),
        "commit": _git_commit(),
    }
    (dest_path / BACKUP_MANIFEST_NAME).write_text(json.dumps(manifest, indent=1) + "\n")
    print(json.dumps(manifest, indent=1))
    return 0


def _store_restore(dest: str | None, new_store: str) -> int:
    """`gnomon store restore --dest <dest> --store <new_store>`: replay a
    backup's log into a fresh store, then verify it against the manifest.

    Restore is deliberately just `gnomon replay` plus a check: the backed-up
    log is copied into `new_store` and replayed there
    (`gnomon.storage.eventlog.replay`, `thread_cursor=True` — the same call
    `gnomon replay`'s CLI action makes), and the result's `store_identity` and
    logical digest are compared against what `store backup` recorded.
    PASS/FAIL is printed either way; the exit code is nonzero on any
    mismatch, including one caught before replay even runs: the backed-up
    log's sha256 is checked against the manifest first, so a tampered log
    fails loudly instead of silently replaying into something else.
    """
    import hashlib
    import shutil
    from pathlib import Path

    import gnomon
    from gnomon.storage.eventlog import replay

    if not dest:
        print("store restore: --dest is required", file=sys.stderr)
        return 2
    dest_path = Path(dest)
    manifest_path = dest_path / BACKUP_MANIFEST_NAME
    if not manifest_path.exists():
        print(f"store restore: no backup manifest at {manifest_path}", file=sys.stderr)
        return 2
    manifest = json.loads(manifest_path.read_text())

    backed_up_log = dest_path / "eventlog.jsonl"
    if not backed_up_log.exists():
        print(f"store restore: no backed-up log at {backed_up_log}", file=sys.stderr)
        return 2
    actual_sha = hashlib.sha256(backed_up_log.read_bytes()).hexdigest()
    if actual_sha != manifest.get("log_sha256"):
        print(f"FAIL: backed-up log does not match its manifest "
              f"(sha256 {actual_sha} != recorded {manifest.get('log_sha256')}) "
              f"— refusing to restore a tampered log", file=sys.stderr)
        return 1

    store = gnomon.open(new_store, backend="native")
    dst_log = Path(store.path) / "eventlog.jsonl"
    if backed_up_log.resolve() != dst_log.resolve():
        shutil.copyfile(backed_up_log, dst_log)
    replay(dst_log, store.adapter, thread_cursor=True)

    got_identity = store.store_identity
    got_digest = store.digest()
    store.close()

    ok = (got_identity == manifest.get("store_identity")
         and got_digest == manifest.get("logical_digest"))
    print(json.dumps({
        "verdict": "PASS" if ok else "FAIL",
        "store_identity": got_identity,
        "expected_store_identity": manifest.get("store_identity"),
        "logical_digest": got_digest,
        "expected_logical_digest": manifest.get("logical_digest"),
    }, indent=1))
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "ingest":
        import gnomon
        store = gnomon.open(args.store, backend=args.backend)
        with open(args.events_jsonl) as f:
            tt = store.ingest_events(json.loads(line) for line in f if line.strip())
        print(json.dumps({"last_tt": tt, "stats": store.stats()}, default=str))
        store.close()
    elif args.cmd == "replay":
        import shutil
        from pathlib import Path

        import gnomon
        from gnomon.storage.eventlog import replay
        # replay writes into a fresh store. The log is copied into place
        # *first* and replayed from the copy, so the replay cursor a
        # cursor-keeping backend records (suffix recovery) names
        # offsets in the store's own log — and an interrupted replay simply
        # resumes from its cursor on the next open.
        store = gnomon.open(args.store, backend=args.backend)
        dst = Path(store.path) / "eventlog.jsonl"
        if Path(args.eventlog_jsonl).resolve() != dst.resolve():
            shutil.copyfile(args.eventlog_jsonl, dst)
        n = replay(dst, store.adapter, thread_cursor=True,
                   compact_every=args.compact_every)
        print(json.dumps({"batches": n, "stats": store.stats()}, default=str))
        store.close()
    elif args.cmd == "call":
        import gnomon
        from gnomon.core.errors import GnomonError
        from gnomon.temporal.algebra import call_operator, ensure_all_registered
        ensure_all_registered()
        store = gnomon.open(args.store, read_only=True)
        try:
            res = call_operator(store.adapter, args.op, json.loads(args.args_json))
        except GnomonError as e:
            res = e.to_payload()
        print(json.dumps(res, indent=1))
        store.close()
        if "error" in res:
            return 1
    elif args.cmd == "store":
        import gnomon
        if args.action == "upgrade-manifests":
            # Deliberately *not* through `gnomon.open`: that opens the event log
            # and may replay a suffix, which is a write — and a write is the
            # one thing a format-1 or format-2 store refuses until this
            # command has run.
            # The engine handle alone is enough, and touches nothing else.
            from pathlib import Path as _StorePath

            from gnomon.storage.native import NativeAdapter
            adapter = NativeAdapter(_StorePath(args.store) / "native")
            report = adapter.upgrade_manifests()
            adapter.close()
            if report["upgraded"]:
                print(f"store:      {args.store}")
                print(f"upgraded:   manifest format {report['from_format']} "
                      f"-> {report['to_format']}")
                print(f"generation: {report['generation']} "
                      f"(sha {report['manifest_sha']})")
                print("\nOne checkpoint written and CURRENT flipped; segments, "
                      "close runs, the dictionary and the event log are "
                      "untouched. Persisted TCSR indexes will rebuild on next "
                      "use, and receipts must not be compared on manifest_sha "
                      "across the two formats.")
            else:
                print(f"store:      {args.store}")
                print(f"unchanged:  already at manifest format "
                      f"{report['to_format']}")
            return 0

        if args.action == "backup":
            return _store_backup(args.store, args.dest)
        if args.action == "restore":
            return _store_restore(args.dest, args.store)

        if args.action == "verify":
            return _store_verify(args.store, args.mode, args.json)

        if args.action == "build-indexes":
            return _store_build_indexes(args.store, tcsr=args.tcsr,
                                        name=args.name,
                                        vidfilter=args.vidfilter)

        # `digest` is the one read-only action here: it opens no writer, takes
        # no lock and runs no recovery, so it can be run against a store a
        # writer already holds — which is the point of an identity check.
        store = gnomon.open(args.store, backend="native",
                          read_only=args.action == "digest")
        if args.action == "gc":
            report = store.adapter.gc(keep_last=args.keep)
        elif args.action == "settle":
            report = store.adapter.settle(budget_bytes=args.budget_bytes,
                                          build_indexes=args.build_indexes)
        elif args.action == "digest":
            # `content_digest`, not `store_digest`: the tt-independent one, so
            # two independent builds of the same logical events are comparable.
            # --streaming picks the engine's bounded pass; both must
            # print the same value for the same store.
            if args.streaming:
                report = store.adapter.content_digest_streaming(
                    budget_bytes=args.budget_bytes)
            else:
                cd_report = store.adapter.content_digest_report()
                report = {"digest": cd_report["digest"],
                          "ambiguous_ties": cd_report["ambiguous_ties"],
                          "streaming": False}
        else:
            report = store.adapter.compact()
        print(json.dumps(report))
        store.close()
    elif args.cmd == "check":
        return _store_verify(args.store, "full", args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
