"""The identity-filter files under `native/idx/vidfilter/` are trusted only
when every byte checks out at load.

A filter answers "this segment holds no row of identity k", and a point read
or a superseding write skips the segment on that answer. The file's footer
CRC once covered only the filter array, so a changed digit in the header's
`seed` still loaded: the filter then answered "no" for keys the segment
holds, point reads lost that segment's rows, `correct` could not find the
version it was asked to replace, and `assert_edge` left the old version
believed beside the new one. `store verify` does not read these files, so it
reported such a store healthy; only after that `assert_edge` did
`verify --full` report the two overlapping believed versions.

The tests below damage that one digit in a file the engine wrote, and turn a
file back into the earlier layout (format 1, CRC over the array only), then
check that reads, writes and `build-indexes --vidfilter` behave as if the
file were absent: answers come from the segments, and a rebuilt file of the
current format replaces it. Every store is built on the native engine, which
is the only engine with these files.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import gnomon
from gnomon.core.model import EntityRef, edge_eid

#: The `.tvf` layout: magic, format u32, header_len u32, header JSON, zero
#: padding to 8, the filter array, then crc32c u32 | footer_len u32 | magic.
FORMAT = 2
ALIGN = 8

# four batches of 50 events over 60 identities; each identity has versions in
# several segments, at valid times [1000*b + j, +500)
IDENT = [(f"s{i % 17}", f"d{i}", "R", f"k{i}") for i in range(60)]


def _events(b: int) -> list[dict]:
    out = []
    for j in range(50):
        src, dst, rel, disc = IDENT[(b * 50 + j) % len(IDENT)]
        vs = b * 1000 + j
        out.append({"src": src, "dst": dst, "rel_type": rel, "disc": disc,
                    "vt_s": vs, "vt_e": vs + 500, "props": {"w": b}})
    return out


def _build(root: Path) -> None:
    s = gnomon.open(root, backend="native")
    for b in range(4):
        s.ingest_events(_events(b))
    s.close()


def _filters(root: Path) -> list[Path]:
    return sorted((root / "native" / "idx" / "vidfilter").glob("*.tvf"))


def _header(b: bytes) -> dict:
    n = int.from_bytes(b[8:12], "little")
    return json.loads(b[12:12 + n])


def _first_edge_filter(root: Path) -> Path:
    return next(p for p in _filters(root) if _header(p.read_bytes())["kind"] == "edge")


def _crc32c(data: bytes) -> int:
    table = _crc32c.table  # type: ignore[attr-defined]
    crc = 0xFFFFFFFF
    for byte in data:
        crc = table[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def _table() -> list[int]:
    out = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ 0x82F63B78 if c & 1 else c >> 1
        out.append(c)
    return out


_crc32c.table = _table()  # type: ignore[attr-defined]


def _change_seed_digit(p: Path) -> None:
    """One interior digit of the header's `seed`, changed into another digit:
    the header still parses and every other header check still passes."""
    b = bytearray(p.read_bytes())
    i = b.index(b'"seed":') + len(b'"seed":') + 3
    assert chr(b[i]).isdigit()
    b[i] ^= 0x01
    assert chr(b[i]).isdigit()
    p.write_bytes(bytes(b))


def _to_format_1(p: Path) -> None:
    """Rewrite a filter file into the format-1 layout: format number 1 and the
    footer CRC over the filter array alone."""
    b = bytearray(p.read_bytes())
    start = -(-(12 + int.from_bytes(b[8:12], "little")) // ALIGN) * ALIGN
    crc_at = len(b) - 12
    b[4:8] = (1).to_bytes(4, "little")
    b[crc_at:crc_at + 4] = _crc32c(bytes(b[start:crc_at])).to_bytes(4, "little")
    p.write_bytes(bytes(b))


def _is_current_format(p: Path) -> bool:
    b = p.read_bytes()
    return (b[:4] == b"TGVF" and int.from_bytes(b[4:8], "little") == FORMAT
            and b[-4:] == b"TGVE"
            and int.from_bytes(b[-12:-8], "little") == _crc32c(b[:-12]))


def _point_reads(root: Path) -> list[list[tuple]]:
    s = gnomon.open(root, read_only=True)
    try:
        return [sorted((v.vt_s, v.vt_e, v.props.get("w"))
                       for v in s.adapter.believed_edge_versions(
                           edge_eid(*i), src=i[0], dst=i[1]))
                for i in IDENT]
    finally:
        s.close()


def test_the_engine_writes_files_whose_every_byte_is_checked(tmp_path):
    root = tmp_path / "s"
    _build(root)
    files = _filters(root)
    assert files and all(_is_current_format(p) for p in files)


def test_a_changed_seed_digit_hides_no_row(tmp_path):
    root = tmp_path / "s"
    _build(root)
    want = _point_reads(root)
    _change_seed_digit(_first_edge_filter(root))
    assert _point_reads(root) == want


def test_writes_through_a_damaged_filter_find_and_supersede_the_version(tmp_path):
    root = tmp_path / "s"
    _build(root)
    p = _first_edge_filter(root)
    src, dst, rel, disc = IDENT[0]   # its first version, [0, 500), is in that segment
    _change_seed_digit(p)

    s = gnomon.open(root)
    ref = EntityRef("edge", src=src, dst=dst, rel_type=rel, disc=disc)
    s.correct(ref, {"w": 77}, vt_s=0, vt_e=500)
    s.assert_edge(src, dst, rel, {"w": 88}, vt_s=0, vt_e=500, disc=disc)
    s.close()

    s = gnomon.open(root, read_only=True)
    try:
        believed = sorted((v.vt_s, v.vt_e, v.props.get("w"))
                          for v in s.adapter.believed_edge_versions(
                              edge_eid(src, dst, rel, disc), src=src, dst=dst))
        assert [v for v in believed if v[0] < 500] == [(0, 500, 88)], believed
        report = s.adapter.verify("full")
    finally:
        s.close()
    assert report["healthy"], report["problems"]


def test_build_indexes_replaces_damaged_and_format_1_files(tmp_path):
    root = tmp_path / "s"
    _build(root)
    want = _point_reads(root)
    edge_files = [p for p in _filters(root) if _header(p.read_bytes())["kind"] == "edge"]
    assert len(edge_files) >= 2
    _change_seed_digit(edge_files[0])
    _to_format_1(edge_files[1])
    assert not _is_current_format(edge_files[0])
    assert not _is_current_format(edge_files[1])

    # a read-only handle answers from the segments, and writes nothing
    before = {p.name: p.read_bytes() for p in _filters(root)}
    assert _point_reads(root) == want
    assert {p.name: p.read_bytes() for p in _filters(root)} == before

    s = gnomon.open(root)
    try:
        s.adapter.build_indexes(vidfilter=True)
    finally:
        s.close()
    files = _filters(root)
    assert len(files) == len(before)
    assert all(_is_current_format(p) for p in files), [p.name for p in files if not _is_current_format(p)]
    assert _point_reads(root) == want

    # a fresh handle now loads every file and rebuilds none
    s = gnomon.open(root, read_only=True)
    try:
        for i in IDENT:
            s.adapter.believed_edge_versions(edge_eid(*i), src=i[0], dst=i[1])
        st = s.adapter.vidfilter_stats()
    finally:
        s.close()
    assert st["lazy_built"] == 0 and st["built"] == 0, st
    assert st["loaded"] >= len(edge_files), st


def test_a_rebuilt_file_is_byte_for_byte_the_one_first_written(tmp_path):
    root = tmp_path / "s"
    _build(root)
    p = _first_edge_filter(root)
    good = p.read_bytes()
    copy = tmp_path / "c"
    shutil.copytree(root, copy)
    q = copy / p.relative_to(root)
    _to_format_1(q)
    s = gnomon.open(copy)
    try:
        s.adapter.build_indexes(vidfilter=True)
    finally:
        s.close()
    assert q.read_bytes() == good
