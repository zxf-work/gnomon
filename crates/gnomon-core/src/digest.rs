//! The streaming **content digest** — store identity in memory bounded by a
//! byte budget and a merge fan-in, never by the store's row count.
//!
//! # What it computes, and against what
//!
//! `content_digest` is the *layout-independent* identity of a store: every
//! field `store_digest()` hashes, minus `vid`/`tt_s`/`tt_e`, all three of
//! which derive from the wall-clock transaction clock and so legitimately
//! differ between two independent ingests of the same logical events.
//! It is therefore the only digest on which the two sides of an A/B may be compared, and
//! the campaign's recorded 1M/10M values are equalities this module must
//! reproduce **bit for bit** or it is useless.
//!
//! The reference implementation is `scripts/build_synth_store.py::content_digest`:
//!
//! ```python
//! payload["nodes"] = sorted((_strip_tt(v.to_json()) for v in adapter.all_node_versions()),
//!                           key=lambda r: (r["uid"], r["vt_s"], r["vt_e"],
//!                                          json.dumps(r["props"], sort_keys=True)))
//! payload["edges"] = sorted((_strip_tt(v.to_json()) for v in adapter.all_edge_versions()),
//!                           key=lambda r: (r["eid"], r["vt_s"], r["vt_e"],
//!                                          json.dumps(r["props"], sort_keys=True)))
//! return sha256_hex(canonical_json(payload))
//! ```
//!
//! That fixes the byte stream this module feeds a running `Sha256` exactly:
//!
//! ```text
//! {"edges":[<row>,<row>,...],"nodes":[<row>,...]}
//! ```
//!
//! — `"edges"` first because `canonical_json` sorts the two top-level keys,
//! compact separators throughout, and each `<row>` the canonical JSON of one
//! stripped version with its own keys sorted:
//!
//! ```text
//! {"disc":…,"dst":…,"eid":…,"props":…,"provenance_ref":…,"rel_type":…,
//!  "source":…,"src":…,"vt_e":…,"vt_s":…}                       (edge)
//! {"label":…,"props":…,"provenance_ref":…,"source":…,"uid":…,
//!  "vt_e":…,"vt_s":…}                                          (node)
//! ```
//!
//! Compact separators are what make a streaming fold possible at all: a row's
//! own text never depends on where in the outer array it lands, so the whole
//! payload's bytes are the concatenation of per-row bytes and three literals.
//!
//! # Two encoding hazards, and why neither bites
//!
//! A Rust digest was declined because
//! reproducing Python's JSON encoder exactly is a real risk. Both halves of
//! that risk are removed here rather than argued away:
//!
//! * **`props` is never re-encoded.** The engine stores props as the exact
//!   canonical-JSON text the writer handed it (`row.rs`'s "stored byte-for-byte
//!   as received", and the adapter's `canonical_json(r.props)` on the way in),
//!   and `canonical_json(json.loads(t)) == t` for any `t` that `canonical_json`
//!   itself produced. So the reference's re-encoding of `row["props"]` is, byte
//!   for byte, the text already on disk. Number formatting — the one place a
//!   Rust encoder and CPython's `repr` genuinely diverge (`1e+30` vs `1e30`)
//!   — is therefore never exercised.
//! * **Every other field is a string or an `i64`**, and strings go through
//!   [`crate::derive::escape_into`], which is already Python's
//!   `ensure_ascii=False` escape table (it has to be: `edge_eid` hashes a
//!   canonical JSON array built with it).
//!
//! # The sort key is not the segment key, and that is the whole design problem
//!
//! Segments are sealed in `(vt_s, vid)` order (`row.rs::SortKey`), clusters
//! concatenate in that order, and `merge.rs`'s k-way merge streams it for free.
//! The digest's order is `(eid|uid, vt_s, vt_e, props)` — a different key, on a
//! field (`eid`) that is not even stored. No permutation of the stored order
//! yields it, and a sequential `Sha256` is not permutation-invariant, so there
//! is no order-free construction that reproduces the recorded values either.
//!
//! What *is* possible without touching the digest's definition is to build the
//! sorted runs and then merge them: this module fills a byte-bounded buffer,
//! sorts it, spills it, and finishes with exactly the bounded-fan-in
//! `Reverse`-ordered heap merge `merge.rs` uses — over runs rather than over
//! segments. Peak memory is `budget + fan_in` records plus one open segment; it
//! does not move with the row count. The price is `O(rows)` of scratch **disk**,
//! which the posthoc pass does not pay and which the report states as
//! `spill_bytes`.
//!
//! # Where the reference is ambiguous, and how that is reported
//!
//! Python's `sorted` is stable, so rows that tie on all four key components
//! keep `all_*_versions()`'s own order — and that order is documented
//! unspecified (`read.rs`: "Order is unspecified — the digest sorts"). Two such
//! rows can only differ in `label`, `source` or `provenance_ref`; if they do,
//! the reference's value is not well defined. This module breaks such ties by
//! the row's own canonical bytes (a total order), and counts every occurrence
//! in `ambiguous_ties`. A store with `ambiguous_ties == 0` — every store in the
//! test matrix, and every store the ingest path builds — has exactly one
//! possible reference value, and this module produces it.

use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use sha2::{Digest, Sha256};

use crate::derive::{edge_eid, escape_into, Id96};
use crate::dict::Dictionary;
use crate::error::{EngineError, Result};
use crate::row::RowKind;
use crate::store::NativeStore;

/// Hard cap on one merge round's open runs, independent of the byte budget —
/// the same reason `settle::MAX_FAN_IN` exists: a byte budget must not be
/// convertible into file-descriptor or VMA exhaustion. Above this the merge
/// cascades into rounds, which the report counts.
const MAX_FAN_IN: usize = 512;

/// The default budget where `GNOMON_DIGEST_BYTES` is unset and no explicit
/// argument is given. Deliberately modest: this bounds a buffer the pass
/// *holds*, unlike `settle`'s budget which bounds the input bytes a merge
/// group *spans*, and the whole point of the module is that a store too large
/// to hold is still digestible. `0` means "no cap" — one run, no spill, and
/// the posthoc pass's memory profile.
const DIGEST_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// I/O buffer for a spilled run, both directions.
const RUN_IO_BYTES: usize = 256 * 1024;

/// Resolve the digest budget: explicit argument, else `GNOMON_DIGEST_BYTES`,
/// else [`DIGEST_MAX_BYTES`]. `0` means unbounded.
///
/// Mirrors `settle::settle_budget`'s resolution order exactly, including the
/// `0` escape, so an operator who has learned one budget knob has learned both.
pub(crate) fn digest_budget(env: Option<&str>, explicit: Option<u64>) -> u64 {
    if let Some(b) = explicit {
        return b;
    }
    if let Some(v) = env {
        if let Ok(n) = v.trim().parse::<u64>() {
            return n;
        }
    }
    DIGEST_MAX_BYTES
}

/// What one `content_digest` call did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContentDigestReport {
    /// The digest itself — 64 lowercase hex characters.
    pub digest: String,
    pub rows_edges: u64,
    pub rows_nodes: u64,
    /// The high-water mark of **rows resident at once**: the sort buffer's
    /// peak occupancy, or a merge round's fan-in, whichever was larger. This
    /// is the quantity the memory claim is about, counted directly rather
    /// than inferred from RSS.
    pub rows_held_max: u64,
    /// Runs spilled to scratch. `0` means the store fit the budget and
    /// nothing touched the disk.
    pub runs_spilled: u64,
    /// Cascading merge rounds. `0` when nothing spilled, `1` whenever the
    /// runs fit one [`MAX_FAN_IN`] merge.
    pub merge_rounds: u64,
    /// Largest fan-in any one merge round used.
    pub max_fan_in: u64,
    /// Scratch bytes written. The cost this trades peak RSS for.
    pub spill_bytes: u64,
    pub budget_bytes: u64,
    /// Adjacent output rows that tie on the reference's whole sort key yet
    /// differ in bytes — see the module header. Zero means the reference
    /// value is unambiguous.
    pub ambiguous_ties: u64,
    pub segments_read: u64,
    pub wall_us: u64,
}

// --------------------------------------------------------------------- //
// the Python-compatible sort key over an already-canonical props text
// --------------------------------------------------------------------- //

/// The bytes of `json.dumps(obj, sort_keys=True)` — Python's **default**
/// separators (`", "`, `": "`) and `ensure_ascii=True` — produced on the fly
/// from `obj`'s canonical-JSON text, without materializing either string.
///
/// The reference's fourth key component is that rendering, not the canonical
/// one, and the two do not always order the same way: `ensure_ascii` turns a
/// non-ASCII character into a `\` (0x5C), which sorts *before* characters it
/// sorts after when raw. (Measured on random props: the two orders disagree on
/// ~1.8 % of pairs.) So the key has to be the ASCII rendering, and this is it
/// — a byte iterator, so a comparison stops at the first difference and
/// allocates nothing.
///
/// The separator difference alone never changes an ordering — two strings
/// share a prefix up to their first difference, and the inserted spaces sit at
/// structurally identical positions within a shared prefix — but rendering it
/// costs nothing here and keeps the key literally equal to Python's.
struct AsciiJson<'a> {
    it: std::str::Chars<'a>,
    in_str: bool,
    buf: [u8; 12],
    n: usize,
    i: usize,
}

impl<'a> AsciiJson<'a> {
    fn new(canonical: &'a str) -> Self {
        Self {
            it: canonical.chars(),
            in_str: false,
            buf: [0; 12],
            n: 0,
            i: 0,
        }
    }

    fn put(&mut self, b: u8) {
        self.buf[self.n] = b;
        self.n += 1;
    }

    /// `\uxxxx`, lowercase — Python's `'\\u{0:04x}'`.
    fn put_u(&mut self, code: u32) {
        self.put(b'\\');
        self.put(b'u');
        for shift in [12, 8, 4, 0] {
            let nib = (code >> shift) & 0xf;
            self.put(char::from_digit(nib, 16).expect("nibble is a hex digit") as u8);
        }
    }

    /// A character canonical JSON should only ever produce as ASCII; encoded
    /// defensively rather than truncated if it is not.
    fn put_char(&mut self, c: char) {
        let mut b = [0u8; 4];
        for byte in c.encode_utf8(&mut b).as_bytes() {
            self.put(*byte);
        }
    }
}

impl Iterator for AsciiJson<'_> {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        loop {
            if self.i < self.n {
                let b = self.buf[self.i];
                self.i += 1;
                return Some(b);
            }
            let c = self.it.next()?;
            self.n = 0;
            self.i = 0;
            if self.in_str {
                match c {
                    // an escape the canonical text already carries: copied
                    // verbatim, both characters, so a `\"` never ends the
                    // string and a `\\` never swallows the quote after it
                    '\\' => {
                        self.put(b'\\');
                        if let Some(d) = self.it.next() {
                            self.put_char(d);
                        }
                    }
                    '"' => {
                        self.in_str = false;
                        self.put(b'"');
                    }
                    c if (c as u32) < 0x20 || (c as u32) > 0x7e => {
                        let cp = c as u32;
                        if cp > 0xffff {
                            // Python emits a UTF-16 surrogate pair
                            let v = cp - 0x1_0000;
                            self.put_u(0xd800 + (v >> 10));
                            self.put_u(0xdc00 + (v & 0x3ff));
                        } else {
                            self.put_u(cp);
                        }
                    }
                    c => self.put(c as u8),
                }
            } else {
                match c {
                    '"' => {
                        self.in_str = true;
                        self.put(b'"');
                    }
                    ',' | ':' => {
                        self.put(c as u8);
                        self.put(b' ');
                    }
                    c => self.put_char(c),
                }
            }
        }
    }
}

/// Order two props texts the way the reference's fourth key component does.
fn cmp_props(a: &str, b: &str) -> Ordering {
    AsciiJson::new(a).cmp(AsciiJson::new(b))
}

// --------------------------------------------------------------------- //
// one row, ready to hash
// --------------------------------------------------------------------- //

/// A row's canonical bytes plus everything its sort key needs.
///
/// The identity is kept out of `json` for comparison: inside `json` it is
/// *escaped*, and escaping is not order-preserving (a raw `"` is 0x22, its
/// escape starts 0x5C). Edges carry the derived `eid` as an [`Id96`], whose
/// derived `Ord` is exactly its hex string's order; nodes carry the
/// dictionary's dense id and resolve the uid through the store's dictionary at
/// comparison time, which is an arena index and costs no memory per row.
struct Rec {
    eid: Id96,
    uid_id: u32,
    vt_s: i64,
    vt_e: i64,
    props_off: u32,
    props_len: u32,
    json: Box<[u8]>,
}

impl Rec {
    fn props(&self) -> &str {
        let a = self.props_off as usize;
        let b = a + self.props_len as usize;
        // built by this module from `&str` pieces, so always valid UTF-8
        std::str::from_utf8(&self.json[a..b]).unwrap_or("")
    }

    /// Bytes this record holds, for the budget accounting.
    fn heap_bytes(&self) -> u64 {
        (std::mem::size_of::<Rec>() + self.json.len()) as u64
    }
}

/// The reference's four-component sort key.
fn cmp_key(kind: RowKind, dict: &Dictionary, a: &Rec, b: &Rec) -> Ordering {
    let id = match kind {
        RowKind::Edge => a.eid.cmp(&b.eid),
        RowKind::Node => dict.uid(a.uid_id).unwrap_or("").cmp(dict.uid(b.uid_id).unwrap_or("")),
    };
    id.then_with(|| a.vt_s.cmp(&b.vt_s))
        .then_with(|| a.vt_e.cmp(&b.vt_e))
        .then_with(|| cmp_props(a.props(), b.props()))
}

/// The reference's key, made total by the row's own canonical bytes. Where
/// that last component decides, the reference itself has no defined answer;
/// `finish` counts those cases on the sorted output rather than here, since a
/// comparator sees the same pair many times.
fn cmp_rec(kind: RowKind, dict: &Dictionary, a: &Rec, b: &Rec) -> Ordering {
    cmp_key(kind, dict, a, b).then_with(|| a.json.cmp(&b.json))
}

// --------------------------------------------------------------------- //
// row encoding — canonical JSON, Python-exact
// --------------------------------------------------------------------- //

/// `,"key":` — the separator-free spelling `canonical_json` produces.
fn key_into(name: &str, out: &mut String, first: bool) {
    if !first {
        out.push(',');
    }
    out.push('"');
    out.push_str(name);
    out.push_str("\":");
}

fn opt_into(v: Option<&str>, out: &mut String) {
    match v {
        Some(s) => escape_into(s, out),
        None => out.push_str("null"),
    }
}

/// The canonical JSON of one stripped edge version, and the byte range of its
/// `props` value inside it. Keys in `canonical_json`'s sorted order.
#[allow(clippy::too_many_arguments)]
fn edge_json(
    disc: &str,
    dst: &str,
    eid: &str,
    props: &str,
    provenance_ref: Option<&str>,
    rel_type: &str,
    source: &str,
    src: &str,
    vt_e: i64,
    vt_s: i64,
) -> (String, u32, u32) {
    let mut o = String::with_capacity(160 + props.len());
    o.push('{');
    key_into("disc", &mut o, true);
    escape_into(disc, &mut o);
    key_into("dst", &mut o, false);
    escape_into(dst, &mut o);
    key_into("eid", &mut o, false);
    escape_into(eid, &mut o);
    key_into("props", &mut o, false);
    let off = o.len() as u32;
    o.push_str(props);
    let len = props.len() as u32;
    key_into("provenance_ref", &mut o, false);
    opt_into(provenance_ref, &mut o);
    key_into("rel_type", &mut o, false);
    escape_into(rel_type, &mut o);
    key_into("source", &mut o, false);
    escape_into(source, &mut o);
    key_into("src", &mut o, false);
    escape_into(src, &mut o);
    key_into("vt_e", &mut o, false);
    push_i64(vt_e, &mut o);
    key_into("vt_s", &mut o, false);
    push_i64(vt_s, &mut o);
    o.push('}');
    (o, off, len)
}

/// The canonical JSON of one stripped node version, and its `props` range.
fn node_json(
    label: &str,
    props: &str,
    provenance_ref: Option<&str>,
    source: &str,
    uid: &str,
    vt_e: i64,
    vt_s: i64,
) -> (String, u32, u32) {
    let mut o = String::with_capacity(120 + props.len());
    o.push('{');
    key_into("label", &mut o, true);
    escape_into(label, &mut o);
    key_into("props", &mut o, false);
    let off = o.len() as u32;
    o.push_str(props);
    let len = props.len() as u32;
    key_into("provenance_ref", &mut o, false);
    opt_into(provenance_ref, &mut o);
    key_into("source", &mut o, false);
    escape_into(source, &mut o);
    key_into("uid", &mut o, false);
    escape_into(uid, &mut o);
    key_into("vt_e", &mut o, false);
    push_i64(vt_e, &mut o);
    key_into("vt_s", &mut o, false);
    push_i64(vt_s, &mut o);
    o.push('}');
    (o, off, len)
}

/// An `i64` as JSON. Python renders an `int` with no exponent, no separators
/// and no sign for zero — which is `i64`'s own `Display`.
fn push_i64(v: i64, out: &mut String) {
    use std::fmt::Write as _;
    let _ = write!(out, "{v}");
}

// --------------------------------------------------------------------- //
// scratch
// --------------------------------------------------------------------- //

/// A scratch directory under `TMPDIR`, removed when the pass ends — including
/// on an early return, which is why it owns a `Drop`.
struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new() -> Result<Self> {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "gnomon-digest-scratch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path(&self, kind: RowKind, round: usize, n: usize) -> PathBuf {
        let k = match kind {
            RowKind::Edge => "e",
            RowKind::Node => "n",
        };
        self.dir.join(format!("run-{k}-{round}-{n}.bin"))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// `[hi u64][lo u32][uid_id u32][vt_s i64][vt_e i64][props_off u32][props_len u32][json_len u32][json]`
const REC_HEAD: usize = 8 + 4 + 4 + 8 + 8 + 4 + 4 + 4;

fn write_rec(w: &mut BufWriter<File>, r: &Rec) -> Result<u64> {
    let mut head = [0u8; REC_HEAD];
    head[0..8].copy_from_slice(&r.eid.hi.to_le_bytes());
    head[8..12].copy_from_slice(&r.eid.lo.to_le_bytes());
    head[12..16].copy_from_slice(&r.uid_id.to_le_bytes());
    head[16..24].copy_from_slice(&r.vt_s.to_le_bytes());
    head[24..32].copy_from_slice(&r.vt_e.to_le_bytes());
    head[32..36].copy_from_slice(&r.props_off.to_le_bytes());
    head[36..40].copy_from_slice(&r.props_len.to_le_bytes());
    head[40..44].copy_from_slice(&(r.json.len() as u32).to_le_bytes());
    w.write_all(&head)?;
    w.write_all(&r.json)?;
    Ok((REC_HEAD + r.json.len()) as u64)
}

/// One spilled run, read back one record at a time.
struct RunReader {
    r: BufReader<File>,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            r: BufReader::with_capacity(RUN_IO_BYTES, File::open(path)?),
        })
    }

    fn next(&mut self) -> Result<Option<Rec>> {
        let mut head = [0u8; REC_HEAD];
        match self.r.read_exact(&mut head) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let n = u32::from_le_bytes(head[40..44].try_into().expect("4 bytes")) as usize;
        let mut json = vec![0u8; n];
        self.r.read_exact(&mut json)?;
        Ok(Some(Rec {
            eid: Id96 {
                hi: u64::from_le_bytes(head[0..8].try_into().expect("8 bytes")),
                lo: u32::from_le_bytes(head[8..12].try_into().expect("4 bytes")),
            },
            uid_id: u32::from_le_bytes(head[12..16].try_into().expect("4 bytes")),
            vt_s: i64::from_le_bytes(head[16..24].try_into().expect("8 bytes")),
            vt_e: i64::from_le_bytes(head[24..32].try_into().expect("8 bytes")),
            props_off: u32::from_le_bytes(head[32..36].try_into().expect("4 bytes")),
            props_len: u32::from_le_bytes(head[36..40].try_into().expect("4 bytes")),
            json: json.into_boxed_slice(),
        }))
    }
}

// --------------------------------------------------------------------- //
// the bounded k-way merge
// --------------------------------------------------------------------- //

/// Min-heap sift-down over run indices, ordered by `less`.
fn sift_down<F>(heap: &mut [usize], mut i: usize, less: &F)
where
    F: Fn(usize, usize) -> bool,
{
    let n = heap.len();
    loop {
        let (l, r) = (2 * i + 1, 2 * i + 2);
        let mut m = i;
        if l < n && less(heap[l], heap[m]) {
            m = l;
        }
        if r < n && less(heap[r], heap[m]) {
            m = r;
        }
        if m == i {
            return;
        }
        heap.swap(i, m);
        i = m;
    }
}

/// k-way merge over spilled runs — one buffered record per run, and nothing
/// else. This is `merge.rs`'s heap-of-cursors shape, over runs instead of over
/// segments: the heap holds a run index, the record it is parked on lives in
/// `slots`, and the run index breaks key ties deterministically exactly as
/// `merge::Cursor`'s `input` field does.
///
/// A `BinaryHeap<Reverse<_>>` cannot be used directly because the order needs
/// the store's dictionary, which `Ord` has nowhere to come from; the four
/// lines of `sift_down` above are the price, and the bound — `fan_in` records
/// resident — is identical.
fn merge_runs(
    kind: RowKind,
    dict: &Dictionary,
    runs: &[PathBuf],
    emit: &mut dyn FnMut(Rec) -> Result<()>,
) -> Result<()> {
    let mut readers: Vec<RunReader> = runs
        .iter()
        .map(|p| RunReader::open(p))
        .collect::<Result<Vec<_>>>()?;
    let mut slots: Vec<Option<Rec>> = Vec::with_capacity(readers.len());
    for rdr in readers.iter_mut() {
        slots.push(rdr.next()?);
    }
    let mut heap: Vec<usize> = (0..slots.len()).filter(|&i| slots[i].is_some()).collect();

    macro_rules! sift {
        ($from:expr) => {{
            let s = &slots;
            let less = |a: usize, b: usize| -> bool {
                let ra = s[a].as_ref().expect("heap holds live slots");
                let rb = s[b].as_ref().expect("heap holds live slots");
                match cmp_rec(kind, dict, ra, rb) {
                    Ordering::Less => true,
                    Ordering::Greater => false,
                    Ordering::Equal => a < b,
                }
            };
            sift_down(&mut heap, $from, &less);
        }};
    }

    let n = heap.len();
    for start in (0..n / 2).rev() {
        sift!(start);
    }
    while !heap.is_empty() {
        let top = heap[0];
        let rec = slots[top].take().expect("heap holds live slots");
        emit(rec)?;
        match readers[top].next()? {
            Some(next) => {
                slots[top] = Some(next);
                sift!(0);
            }
            None => {
                let last = heap.pop().expect("non-empty");
                if !heap.is_empty() {
                    heap[0] = last;
                    sift!(0);
                }
            }
        }
    }
    Ok(())
}

// --------------------------------------------------------------------- //
// the pass
// --------------------------------------------------------------------- //

/// The byte-bounded sorter: fill, sort, spill; then merge with bounded fan-in.
struct Sorter<'a> {
    kind: RowKind,
    dict: &'a Dictionary,
    budget: u64,
    buf: Vec<Rec>,
    buf_bytes: u64,
    runs: Vec<PathBuf>,
    scratch: &'a Scratch,
    round: usize,
    rep: ContentDigestReport,
}

impl<'a> Sorter<'a> {
    fn new(kind: RowKind, dict: &'a Dictionary, budget: u64, scratch: &'a Scratch) -> Self {
        Self {
            kind,
            dict,
            budget,
            buf: Vec::new(),
            buf_bytes: 0,
            runs: Vec::new(),
            scratch,
            round: 0,
            rep: ContentDigestReport::default(),
        }
    }

    fn push(&mut self, r: Rec) -> Result<()> {
        self.buf_bytes += r.heap_bytes();
        self.buf.push(r);
        self.rep.rows_held_max = self.rep.rows_held_max.max(self.buf.len() as u64);
        if self.budget > 0 && self.buf_bytes >= self.budget {
            self.spill()?;
        }
        Ok(())
    }

    fn sort_buf(&mut self) {
        let (kind, dict) = (self.kind, self.dict);
        self.buf.sort_unstable_by(|a, b| cmp_rec(kind, dict, a, b));
    }

    fn spill(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.sort_buf();
        let path = self.scratch.path(self.kind, self.round, self.runs.len());
        let mut w = BufWriter::with_capacity(RUN_IO_BYTES, File::create(&path)?);
        let mut bytes = 0u64;
        for r in &self.buf {
            bytes += write_rec(&mut w, r)?;
        }
        w.flush()?;
        self.rep.spill_bytes += bytes;
        self.rep.runs_spilled += 1;
        self.runs.push(path);
        self.buf.clear();
        self.buf_bytes = 0;
        Ok(())
    }

    /// Emit every record in the reference's order.
    ///
    /// `emit` sees rows already ordered; this also compares each row with its
    /// predecessor to count `ambiguous_ties` — once per row, on the sorted
    /// output, rather than once per comparison inside the sort.
    fn finish(mut self, emit: &mut dyn FnMut(&Rec)) -> Result<ContentDigestReport> {
        let (kind, dict) = (self.kind, self.dict);
        let mut prev: Option<Rec> = None;
        let mut ties = 0u64;

        if self.runs.is_empty() {
            // the whole kind fit the budget: sort in place, no disk at all
            self.sort_buf();
            for r in self.buf.drain(..) {
                if let Some(p) = &prev {
                    if cmp_key(kind, dict, p, &r) == Ordering::Equal && p.json != r.json {
                        ties += 1;
                    }
                }
                emit(&r);
                prev = Some(r);
            }
            self.rep.ambiguous_ties += ties;
            return Ok(self.rep);
        }
        self.spill()?;

        // cascade until one round's fan-in holds every remaining run
        while self.runs.len() > MAX_FAN_IN {
            self.round += 1;
            self.rep.merge_rounds += 1;
            let groups: Vec<Vec<PathBuf>> =
                self.runs.chunks(MAX_FAN_IN).map(|c| c.to_vec()).collect();
            let mut next: Vec<PathBuf> = Vec::with_capacity(groups.len());
            for (i, group) in groups.into_iter().enumerate() {
                let path = self.scratch.path(self.kind, self.round, i);
                let mut w = BufWriter::with_capacity(RUN_IO_BYTES, File::create(&path)?);
                let fan = group.len() as u64;
                self.rep.max_fan_in = self.rep.max_fan_in.max(fan);
                self.rep.rows_held_max = self.rep.rows_held_max.max(fan);
                let mut bytes = 0u64;
                merge_runs(kind, dict, &group, &mut |r| {
                    bytes += write_rec(&mut w, &r)?;
                    Ok(())
                })?;
                w.flush()?;
                self.rep.spill_bytes += bytes;
                for p in &group {
                    let _ = std::fs::remove_file(p);
                }
                next.push(path);
            }
            self.runs = next;
        }

        let fan = self.runs.len() as u64;
        self.rep.max_fan_in = self.rep.max_fan_in.max(fan);
        self.rep.rows_held_max = self.rep.rows_held_max.max(fan);
        self.rep.merge_rounds += 1;
        let runs = std::mem::take(&mut self.runs);
        merge_runs(kind, dict, &runs, &mut |r| {
            if let Some(p) = &prev {
                if cmp_key(kind, dict, p, &r) == Ordering::Equal && p.json != r.json {
                    ties += 1;
                }
            }
            emit(&r);
            prev = Some(r);
            Ok(())
        })?;
        self.rep.ambiguous_ties += ties;
        Ok(self.rep)
    }
}

impl NativeStore {
    /// The store's **content digest**, computed in bounded memory.
    ///
    /// Byte-identical to `scripts/build_synth_store.py::content_digest` for
    /// the same store — see the module header for the encoding and the order
    /// it reproduces, and for the one place the reference is ambiguous
    /// (reported as `ambiguous_ties`, and zero on every store the ingest path
    /// builds).
    ///
    /// `budget_bytes` bounds the sort buffer; `None` reads `GNOMON_DIGEST_BYTES`
    /// and falls back to [`DIGEST_MAX_BYTES`]. `0` means unbounded, which is
    /// the posthoc pass's memory profile and is never what a scale run wants.
    pub fn content_digest(&self, budget_bytes: Option<u64>) -> Result<ContentDigestReport> {
        let t0 = Instant::now();
        let budget = digest_budget(
            std::env::var("GNOMON_DIGEST_BYTES").ok().as_deref(),
            budget_bytes,
        );
        let scratch = Scratch::new()?;
        let mut h = Sha256::new();
        let mut rep = ContentDigestReport {
            budget_bytes: budget,
            ..Default::default()
        };

        h.update(b"{\"edges\":[");
        let (edges, n_edges) = self.fold_kind(RowKind::Edge, budget, &scratch, &mut h)?;
        h.update(b"],\"nodes\":[");
        let (nodes, n_nodes) = self.fold_kind(RowKind::Node, budget, &scratch, &mut h)?;
        h.update(b"]}");

        rep.rows_edges = n_edges;
        rep.rows_nodes = n_nodes;
        for sub in [&edges, &nodes] {
            rep.rows_held_max = rep.rows_held_max.max(sub.rows_held_max);
            rep.runs_spilled += sub.runs_spilled;
            rep.merge_rounds += sub.merge_rounds;
            rep.max_fan_in = rep.max_fan_in.max(sub.max_fan_in);
            rep.spill_bytes += sub.spill_bytes;
            rep.ambiguous_ties += sub.ambiguous_ties;
            rep.segments_read += sub.segments_read;
        }
        rep.digest = hex_of(h.finalize());
        rep.wall_us = t0.elapsed().as_micros() as u64;
        Ok(rep)
    }

    /// One kind's rows, in the reference's order, folded into `h`.
    fn fold_kind(
        &self,
        kind: RowKind,
        budget: u64,
        scratch: &Scratch,
        h: &mut Sha256,
    ) -> Result<(ContentDigestReport, u64)> {
        let mut sorter = Sorter::new(kind, self.dict(), budget, scratch);
        let files: Vec<String> = {
            let m = self.manifest();
            match kind {
                RowKind::Edge => m
                    .edge_lanes
                    .event
                    .iter()
                    .chain(m.edge_lanes.interval.iter())
                    .map(|e| e.file.clone())
                    .collect(),
                RowKind::Node => m.node_store.iter().map(|e| e.file.clone()).collect(),
            }
        };
        let segments = files.len() as u64;
        for file in &files {
            // one segment resident at a time — the same reason `read.rs`'s
            // whole-store walks open uncached
            let seg = self.open_segment_uncached(file)?;
            let hdr = seg.header();
            let strings = seg.strings()?;
            let vt_s = seg.i64_column("vt_s")?;
            let vt_e = if hdr.vt_e_elided {
                None
            } else {
                Some(seg.i64_column("vt_e")?)
            };
            let props = seg.u32_column("props_ref")?;
            let source = seg.u32_column("source_ref")?;
            let prov = seg.u32_column("prov_ref")?;
            match kind {
                RowKind::Edge => {
                    let src = seg.u32_column("src_id")?;
                    let dst = seg.u32_column("dst_id")?;
                    let rel = seg.u16_column("rel_code")?;
                    let disc = seg.u32_column("disc_ref")?;
                    for i in 0..vt_s.len() {
                        let src_uid = self.uid_for(src[i])?;
                        let dst_uid = self.uid_for(dst[i])?;
                        let rel_type = hdr.rel_types.get(rel[i] as usize).ok_or_else(|| {
                            EngineError::corrupt(format!("rel_code {} has no entry", rel[i]))
                                .at_row(i as u32)
                        })?;
                        let disc_s = strings.get(disc[i])?;
                        let eid = edge_eid(src_uid, dst_uid, rel_type, disc_s);
                        let ve = vt_e.map(|c| c[i]).unwrap_or(vt_s[i] + 1);
                        let (json, off, len) = edge_json(
                            disc_s,
                            dst_uid,
                            &eid.to_hex(),
                            strings.get(props[i])?,
                            strings.get_opt(prov[i])?,
                            rel_type,
                            strings.get(source[i])?,
                            src_uid,
                            ve,
                            vt_s[i],
                        );
                        sorter.push(Rec {
                            eid,
                            uid_id: 0,
                            vt_s: vt_s[i],
                            vt_e: ve,
                            props_off: off,
                            props_len: len,
                            json: json.into_bytes().into_boxed_slice(),
                        })?;
                    }
                }
                RowKind::Node => {
                    let uid_id = seg.u32_column("uid_id")?;
                    let label = seg.u32_column("label_ref")?;
                    for i in 0..vt_s.len() {
                        let ve = vt_e.map(|c| c[i]).unwrap_or(vt_s[i] + 1);
                        let (json, off, len) = node_json(
                            strings.get(label[i])?,
                            strings.get(props[i])?,
                            strings.get_opt(prov[i])?,
                            strings.get(source[i])?,
                            self.uid_for(uid_id[i])?,
                            ve,
                            vt_s[i],
                        );
                        sorter.push(Rec {
                            eid: Id96 { hi: 0, lo: 0 },
                            uid_id: uid_id[i],
                            vt_s: vt_s[i],
                            vt_e: ve,
                            props_off: off,
                            props_len: len,
                            json: json.into_bytes().into_boxed_slice(),
                        })?;
                    }
                }
            }
        }

        // anything staged in the open batch is part of `all_*_versions()` and
        // so part of the digest; `gnomon store digest` never sees one, but a
        // caller that digests mid-batch must still agree with the reference.
        match kind {
            RowKind::Edge => {
                for v in self.staged_edge_versions()? {
                    let eid = Id96::from_hex(&v.eid)?;
                    let (json, off, len) = edge_json(
                        &v.disc,
                        &v.dst,
                        &v.eid,
                        &v.props,
                        v.provenance_ref.as_deref(),
                        &v.rel_type,
                        &v.source,
                        &v.src,
                        v.vt_e,
                        v.vt_s,
                    );
                    sorter.push(Rec {
                        eid,
                        uid_id: 0,
                        vt_s: v.vt_s,
                        vt_e: v.vt_e,
                        props_off: off,
                        props_len: len,
                        json: json.into_bytes().into_boxed_slice(),
                    })?;
                }
            }
            RowKind::Node => {
                for v in self.staged_node_versions()? {
                    let uid_id = self.dict().dense_id(&v.uid).ok_or_else(|| {
                        EngineError::invariant(format!(
                            "staged node uid {:?} is not in the dictionary",
                            v.uid
                        ))
                    })?;
                    let (json, off, len) = node_json(
                        &v.label,
                        &v.props,
                        v.provenance_ref.as_deref(),
                        &v.source,
                        &v.uid,
                        v.vt_e,
                        v.vt_s,
                    );
                    sorter.push(Rec {
                        eid: Id96 { hi: 0, lo: 0 },
                        uid_id,
                        vt_s: v.vt_s,
                        vt_e: v.vt_e,
                        props_off: off,
                        props_len: len,
                        json: json.into_bytes().into_boxed_slice(),
                    })?;
                }
            }
        }

        let mut rows = 0u64;
        let mut sink = |r: &Rec| {
            if rows > 0 {
                h.update(b",");
            }
            h.update(&r.json);
            rows += 1;
        };
        let mut rep = sorter.finish(&mut sink)?;
        rep.segments_read = segments;
        Ok((rep, rows))
    }

    /// The dictionary's uid for a dense id — `read.rs::uid_of` without the
    /// `String` allocation, which at two per edge row is worth avoiding on a
    /// whole-store pass.
    fn uid_for(&self, id: u32) -> Result<&str> {
        self.dict()
            .uid(id)
            .ok_or_else(|| EngineError::corrupt(format!("dense id {id} has no dictionary entry")))
    }
}

/// Lowercase hex, the spelling `derive::sha256_hex_bytes` uses.
fn hex_of(bytes: impl IntoIterator<Item = u8>) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).expect("nibble"));
        s.push(char::from_digit((b & 0xf) as u32, 16).expect("nibble"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derive::version_vid;
    use crate::manifest::EventLogRef;
    use crate::row::{EdgeRow, NodeRow};

    fn tmp_root(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("gnomon-digest-t-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    // ---- the encoder, against Python's own spellings ------------------- //

    #[test]
    fn a_row_encodes_exactly_as_canonical_json_would() {
        let (j, off, len) = edge_json(
            "#3",
            "n2",
            &"a".repeat(24),
            r#"{"w":1}"#,
            None,
            "R",
            "ingest",
            "n1",
            7,
            3,
        );
        assert_eq!(
            j,
            concat!(
                r##"{"disc":"#3","dst":"n2","eid":"aaaaaaaaaaaaaaaaaaaaaaaa","##,
                r#""props":{"w":1},"provenance_ref":null,"rel_type":"R","#,
                r#""source":"ingest","src":"n1","vt_e":7,"vt_s":3}"#
            )
        );
        assert_eq!(&j[off as usize..(off + len) as usize], r#"{"w":1}"#);

        let (j, _, _) = node_json("N", "{}", Some("p/1"), "agent", "n\"x", 9, 2);
        assert_eq!(
            j,
            r#"{"label":"N","props":{},"provenance_ref":"p/1","source":"agent","uid":"n\"x","vt_e":9,"vt_s":2}"#
        );
    }

    #[test]
    fn the_sort_key_is_pythons_default_separator_ascii_rendering() {
        // json.dumps({"a": 1, "b": [2, 3]}, sort_keys=True)
        let got: Vec<u8> = AsciiJson::new(r#"{"a":1,"b":[2,3]}"#).collect();
        assert_eq!(String::from_utf8(got).unwrap(), r#"{"a": 1, "b": [2, 3]}"#);

        // non-ASCII is escaped, and a non-BMP char becomes a surrogate pair
        let got: Vec<u8> = AsciiJson::new("{\"k\":\"\u{e9}\u{1f600}\"}").collect();
        assert_eq!(
            String::from_utf8(got).unwrap(),
            "{\"k\": \"\\u00e9\\ud83d\\ude00\"}"
        );

        // DEL is escaped by ensure_ascii=True and left raw by the canonical form
        let got: Vec<u8> = AsciiJson::new("{\"k\":\"\u{7f}\"}").collect();
        assert_eq!(String::from_utf8(got).unwrap(), "{\"k\": \"\\u007f\"}");

        // an escape already in the canonical text is copied, and does not end
        // the string: the `,` after it is still a structural comma
        let got: Vec<u8> = AsciiJson::new(r#"{"a":"\"","b":1}"#).collect();
        assert_eq!(String::from_utf8(got).unwrap(), r#"{"a": "\"", "b": 1}"#);

        // a trailing backslash escape followed by the closing quote
        let got: Vec<u8> = AsciiJson::new(r#"{"a":"x\\","b":2}"#).collect();
        assert_eq!(String::from_utf8(got).unwrap(), r#"{"a": "x\\", "b": 2}"#);
    }

    #[test]
    fn props_order_follows_the_ascii_rendering_not_the_canonical_one() {
        // raw 'é' (U+00E9) sorts after 'z'; its é escape sorts before,
        // because the escape starts with a backslash (0x5C < 'z')
        let a = "{\"k\":\"\u{e9}\"}";
        let b = r#"{"k":"z"}"#;
        assert_eq!(a.cmp(b), Ordering::Greater);
        assert_eq!(cmp_props(a, b), Ordering::Less);
    }

    // ---- store fixtures ------------------------------------------------ //

    fn edge(
        src_id: u32,
        dst_id: u32,
        src: &str,
        dst: &str,
        vt_s: i64,
        tt_s: i64,
        i: u32,
    ) -> EdgeRow {
        let disc = format!("#{i}");
        let eid = edge_eid(src, dst, "R", &disc);
        EdgeRow {
            vid: version_vid(&eid.to_hex(), tt_s, vt_s),
            src_id,
            dst_id,
            rel_type: "R".into(),
            disc,
            vt_s,
            vt_e: vt_s + 1,
            tt_s,
            props: format!(r#"{{"i":{i}}}"#),
            source: "ingest".into(),
            provenance_ref: None,
        }
    }

    /// `batches` commits of `per` event-lane edges, one long-lived
    /// interval-lane edge and two node versions each, then two closes — a
    /// multi-segment tiered layout with both edge lanes populated, closed
    /// rows, and a non-null `provenance_ref`.
    fn seeded(
        name: &str,
        batches: u32,
        per: u32,
        target: u64,
    ) -> (PathBuf, NativeStore, Vec<EdgeRow>) {
        let root = tmp_root(name);
        let mut s = NativeStore::open(&root).unwrap();
        let (partitions, _) = {
            let (p, t) = s.layout();
            (*p, t)
        };
        s.set_layout(partitions, target);
        let mut all = Vec::new();
        for batch in 0..batches {
            let tt = 100 + batch as i64;
            let (ua, ub) = (format!("n{batch}a"), format!("n{batch}b"));
            s.begin(tt).unwrap();
            let a = s.ensure_entity(&ua, "Node").unwrap();
            let b = s.ensure_entity(&ub, "Node").unwrap();
            for (uid_id, vt) in [(a, batch as i64 * 2), (b, batch as i64 * 2 + 1)] {
                let vid = version_vid(&format!("node-{uid_id}-{vt}"), tt, vt);
                s.stage_node(NodeRow {
                    vid,
                    uid_id,
                    label: "Node".into(),
                    vt_s: vt,
                    vt_e: vt + 1,
                    tt_s: tt,
                    props: format!(r#"{{"b":{batch}}}"#),
                    source: "ingest".into(),
                    provenance_ref: None,
                })
                .unwrap();
            }
            for i in 0..per {
                let r = edge(a, b, &ua, &ub, (batch * per + i) as i64, tt, batch * per + i);
                all.push(r.clone());
                s.stage_edge(r).unwrap();
            }
            let disc = format!("iv#{batch}");
            let eid = edge_eid(&ua, &ub, "R", &disc);
            let iv = EdgeRow {
                vid: version_vid(&eid.to_hex(), tt, batch as i64),
                src_id: a,
                dst_id: b,
                rel_type: "R".into(),
                disc,
                vt_s: batch as i64,
                vt_e: batch as i64 + 10_000,
                tt_s: tt,
                props: format!(r#"{{"iv":{batch}}}"#),
                source: "ingest".into(),
                provenance_ref: Some("p/1".into()),
            };
            all.push(iv.clone());
            s.stage_edge(iv).unwrap();
            s.commit(EventLogRef::default()).unwrap();
        }
        s.begin(900).unwrap();
        s.close_version(RowKind::Edge, all[1].vid, 900).unwrap();
        s.close_version(RowKind::Edge, all[all.len() - 2].vid, 900)
            .unwrap();
        s.commit(EventLogRef::default()).unwrap();
        (root, s, all)
    }

    /// An independent, deliberately unstreamed reference: materialize every
    /// version through the public read path, strip tt, sort, hash. This is the
    /// Rust transcription of `content_digest`, written against
    /// `all_*_versions()` rather than against segments, so the two
    /// implementations share no code below the row encoder.
    fn reference(s: &NativeStore) -> String {
        let mut edges: Vec<(String, i64, i64, String, String)> = s
            .all_edge_versions()
            .unwrap()
            .into_iter()
            .map(|v| {
                let (j, _, _) = edge_json(
                    &v.disc,
                    &v.dst,
                    &v.eid,
                    &v.props,
                    v.provenance_ref.as_deref(),
                    &v.rel_type,
                    &v.source,
                    &v.src,
                    v.vt_e,
                    v.vt_s,
                );
                (v.eid, v.vt_s, v.vt_e, v.props, j)
            })
            .collect();
        let mut nodes: Vec<(String, i64, i64, String, String)> = s
            .all_node_versions()
            .unwrap()
            .into_iter()
            .map(|v| {
                let (j, _, _) = node_json(
                    &v.label,
                    &v.props,
                    v.provenance_ref.as_deref(),
                    &v.source,
                    &v.uid,
                    v.vt_e,
                    v.vt_s,
                );
                (v.uid, v.vt_s, v.vt_e, v.props, j)
            })
            .collect();
        let key = |a: &(String, i64, i64, String, String), b: &(String, i64, i64, String, String)| {
            a.0.cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then(a.2.cmp(&b.2))
                .then(cmp_props(&a.3, &b.3))
                .then(a.4.cmp(&b.4))
        };
        edges.sort_by(key);
        nodes.sort_by(key);
        let mut h = Sha256::new();
        h.update(b"{\"edges\":[");
        for (i, e) in edges.iter().enumerate() {
            if i > 0 {
                h.update(b",");
            }
            h.update(e.4.as_bytes());
        }
        h.update(b"],\"nodes\":[");
        for (i, e) in nodes.iter().enumerate() {
            if i > 0 {
                h.update(b",");
            }
            h.update(e.4.as_bytes());
        }
        h.update(b"]}");
        hex_of(h.finalize())
    }

    #[test]
    fn the_streaming_digest_equals_the_reference_on_a_tiered_store() {
        let (root, s, _) = seeded("ref", 6, 12, 4096);
        let want = reference(&s);
        let big = s.content_digest(Some(0)).unwrap();
        assert_eq!(big.digest, want);
        assert_eq!(big.runs_spilled, 0);
        assert_eq!(big.ambiguous_ties, 0);
        assert!(big.segments_read >= 2, "fixture must be multi-segment");
        assert!(big.rows_edges > 0 && big.rows_nodes > 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_value_does_not_depend_on_the_budget_or_on_spilling() {
        let (root, s, _) = seeded("budget", 6, 12, 4096);
        let want = s.content_digest(Some(0)).unwrap().digest;
        let mut spilled = 0u64;
        for budget in [1u64, 64, 512, 4096, 1 << 20] {
            let r = s.content_digest(Some(budget)).unwrap();
            assert_eq!(r.digest, want, "budget {budget} changed the digest");
            if budget == 1 {
                spilled = r.runs_spilled;
                assert!(r.merge_rounds >= 2, "both kinds must have merged");
            }
        }
        // a budget of one byte really did spill a run per row
        assert!(spilled > 10, "budget=1 must spill many runs, got {spilled}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn rows_held_is_bounded_by_the_budget_and_the_fan_in() {
        // The bound this module claims is `budget / row + MAX_FAN_IN`, not a
        // constant: a single-round merge parks one record per run, and the
        // run count is `rows * row / budget` until the cap turns it into a
        // cascade. Both halves are checked -- here that the bound holds and
        // the growth is far sublinear in the row count, and below that the
        // cap really does bind.
        let (r1, s1, _) = seeded("mem-small", 3, 8, 4096);
        let (r2, s2, _) = seeded("mem-big", 24, 40, 4096);
        let a = s1.content_digest(Some(4096)).unwrap();
        let b = s2.content_digest(Some(4096)).unwrap();
        let rows_a = a.rows_edges + a.rows_nodes;
        let rows_b = b.rows_edges + b.rows_nodes;
        assert!(
            rows_b > 8 * rows_a,
            "fixtures must differ by about an order of magnitude: {a:?} {b:?}"
        );
        // no record is smaller than its fixed header, so this is a true bound
        let bound = 4096 / (std::mem::size_of::<Rec>() as u64) + MAX_FAN_IN as u64;
        assert!(b.rows_held_max <= bound, "{} > {bound}", b.rows_held_max);
        // sublinear by a wide margin: rows grew >8x, rows held by <4x
        assert!(
            b.rows_held_max * 2 < a.rows_held_max * rows_b / rows_a,
            "rows held tracked the row count too closely: {} -> {} while rows {} -> {}",
            a.rows_held_max,
            b.rows_held_max,
            rows_a,
            rows_b
        );
        assert_eq!(a.digest, reference(&s1));
        assert_eq!(b.digest, reference(&s2));
        let _ = std::fs::remove_dir_all(&r1);
        let _ = std::fs::remove_dir_all(&r2);
    }

    #[test]
    fn the_fan_in_is_capped_and_the_merge_cascades() {
        // One byte of budget spills a run per row, so a fixture with more
        // than MAX_FAN_IN edge rows forces a cascade -- the path that keeps
        // the bound a constant instead of growing with the run count.
        let (root, s, _) = seeded("cascade", 24, 40, 4096);
        let r = s.content_digest(Some(1)).unwrap();
        assert!(
            r.runs_spilled > MAX_FAN_IN as u64,
            "fixture must exceed the cap: {} runs",
            r.runs_spilled
        );
        assert!(r.max_fan_in <= MAX_FAN_IN as u64, "fan-in {}", r.max_fan_in);
        assert!(r.merge_rounds >= 3, "a cascade plus the final round");
        assert!(r.rows_held_max <= MAX_FAN_IN as u64 + 1);
        assert_eq!(r.digest, reference(&s));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn settle_does_not_change_the_digest() {
        let (root, mut s, _) = seeded("settle", 8, 10, 4096);
        let before = s.content_digest(Some(512)).unwrap();
        let segs_before = s.manifest().edge_lanes.event.len();
        let rep = s.settle(None).unwrap();
        assert!(rep.rows_rewritten > 0, "fixture must actually settle");
        let after = s.content_digest(Some(512)).unwrap();
        assert_eq!(before.digest, after.digest);
        assert_eq!(before.rows_edges, after.rows_edges);
        assert_eq!(before.rows_nodes, after.rows_nodes);
        assert!(
            s.manifest().edge_lanes.event.len() <= segs_before,
            "settle should not grow the lane"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn incremental_compaction_does_not_change_the_digest() {
        let (root, mut s, _) = seeded("compact", 10, 10, 4096);
        let before = s.content_digest(Some(512)).unwrap();
        let rep = s.compact_incremental(None).unwrap();
        assert!(rep.rows_rewritten > 0, "fixture must actually compact");
        let after = s.content_digest(Some(512)).unwrap();
        assert_eq!(before.digest, after.digest);
        assert_eq!(after.digest, reference(&s));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn staged_rows_are_part_of_the_digest() {
        let (root, mut s, _) = seeded("staged", 3, 6, 4096);
        let before = s.content_digest(Some(0)).unwrap();
        s.begin(1000).unwrap();
        let a = s.ensure_entity("zz", "Node").unwrap();
        s.stage_edge(edge(a, a, "zz", "zz", 5_000, 1000, 9999))
            .unwrap();
        let mid = s.content_digest(Some(0)).unwrap();
        assert_ne!(before.digest, mid.digest, "a staged row must count");
        assert_eq!(mid.rows_edges, before.rows_edges + 1);
        assert_eq!(mid.digest, reference(&s));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_budget_knob_resolves_like_settles() {
        assert_eq!(digest_budget(None, None), DIGEST_MAX_BYTES);
        assert_eq!(digest_budget(Some("4096"), None), 4096);
        assert_eq!(digest_budget(Some("4096"), Some(64)), 64);
        assert_eq!(digest_budget(Some("garbage"), None), DIGEST_MAX_BYTES);
        assert_eq!(digest_budget(Some("0"), None), 0);
    }

    #[test]
    fn an_empty_store_digests_the_empty_payload() {
        let root = tmp_root("empty");
        let s = NativeStore::open(&root).unwrap();
        let r = s.content_digest(Some(0)).unwrap();
        assert_eq!(r.rows_edges, 0);
        assert_eq!(r.rows_nodes, 0);
        assert_eq!(r.digest, reference(&s));
        assert_eq!(
            r.digest,
            crate::derive::sha256_hex(r#"{"edges":[],"nodes":[]}"#)
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn scratch_is_removed_when_it_is_dropped() {
        let sc = Scratch::new().unwrap();
        let dir = sc.dir.clone();
        assert!(dir.is_dir(), "scratch must exist while the pass runs");
        drop(sc);
        assert!(!dir.exists(), "scratch must be gone when the pass ends");
    }

    #[test]
    fn a_small_budget_really_spills_and_still_agrees() {
        let (root, s, _) = seeded("scratch", 4, 10, 4096);
        let r = s.content_digest(Some(64)).unwrap();
        assert!(r.spill_bytes > 0, "this budget must spill");
        assert!(r.runs_spilled > 1);
        assert_eq!(r.digest, reference(&s));
        let _ = std::fs::remove_dir_all(&root);
    }
}
