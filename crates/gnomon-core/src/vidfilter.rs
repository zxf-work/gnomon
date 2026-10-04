//! Per-segment identity filter for the correction path.
//!
//! A correction resolves an identity before it can close a version, and
//! identity is a hash: the store is clustered on `(vt_s, vid)`, so no
//! manifest key range can prune an identity probe and `read.rs::locate`
//! asks **every** live segment in turn, folding each one it is not already
//! holding. One fold derives an `eid` per row — a canonical JSON array and
//! an sha256 — so the first correction on a never-probed store walks the
//! whole store. This module is the prune that ordering cannot provide: one
//! immutable membership filter per segment, over that segment's distinct
//! identity keys, consulted before any fold.
//!
//! **Exact-negative, and that is the whole contract.** A false positive
//! costs one extra fold and nothing else. A false negative would silently
//! drop a version from an answer, so three things carry it:
//!
//! 1. the filter is built over **every stored row's** identity key, not over
//!    currently-believed rows — the scope decision `nameidx.rs:10-21` makes
//!    for the same reason, and the one that lets a historical `as_of` probe
//!    be filtered too. A segment with no row of identity `k` has
//!    no *open* row of identity `k`, which is the whole visibility argument;
//! 2. the key is exactly the key the probe uses — `eid.hi` for edges
//!    (`derive::edge_eid`), `read::uid_key` for nodes — and the latter is
//!    pinned in-tree precisely because this module writes it to disk;
//! 3. a construction that fails falls back, never lies: fuse16 over a
//!    deterministic seed sequence, then a Bloom extent at a comparable ε, then
//!    no filter at all, which is today's behaviour and always correct.
//!
//! **Index rule**, verbatim from `tcsr.rs:23-28`: an
//! index is rebuildable and *never authoritative*. Every load failure —
//! missing file, bad magic, wrong format, failed CRC, truncated extent, a
//! `body_sha` that is not the segment's, a `key_algo_version` this build
//! does not write — returns `None`, meaning "rebuild silently". A failure to
//! *write* one never fails a query.
//!
//! ### Sizing, and why the LSM convention does not transfer
//!
//! A false positive here costs a **whole-segment fold** (an sha256 and two
//! dictionary lookups per row of that segment), not a 4 KB block read. Over
//! a probe against `S` live segments the false-positive fold overhead is
//! `f = S·ε`, so the sizing rule is `ε ≤ f/S` rather than the ~1 % an LSM
//! store would pick: at `S = 509` (the 1B edge lane) a 1 % filter leaves 473
//! of 509 segments folded anyway. fuse16's `ε ≈ 2⁻¹⁶ = 1.5e-5` gives
//! `f = 0.008` there, at ≈18.2 bits per distinct key.
//!
//! ### File format — `idx/vidfilter/<seg_id:012>.tvf`
//!
//! ```text
//! magic "TGVF" | format u32
//! header_len u32 | header JSON | pad to 8
//!    header: { seg_id, body_sha, kind, key_algo_version, keys, seed,
//!              variant, bits_per_key, fingerprint_bits, … }
//! extent: the filter array (u16 LE fingerprints, or Bloom bit bytes)
//! footer: crc32c u32 | footer_len u32 | magic "TGVE"
//! ```
//!
//! The extent length is derivable from the header, so the footer carries
//! only the checksum. The stamp is the segment's own `body_sha`: a segment
//! file's content determines its filter completely, which is what makes an
//! orphan harmless (`gc.rs:273-286`) and what makes the filter independent of
//! any column-codec change — a codec that rewrites a segment produces a new
//! `body_sha`, the stamp mismatches, and the filter rebuilds.
//!
//! The stamp is read off the manifest entry (`SegmentEntry::sha`,
//! `store.rs::segment_sha_of`), not the file. Older engines did not fill
//! that field in, so every filter was stamped `""`, every seed sequence started
//! from `seed_origin("")`, and the stamp check above could not fire: what
//! kept a filter from answering for another segment was only that published
//! segment ids are not reused and that filters are built after publication.
//! Entries written since carry the footer's `body_sha`; a legacy entry keeps
//! `""`, and its `""`-stamped filter stays valid for it and for nothing else.
//!
//! ### Determinism
//!
//! Same segment ⇒ same bytes. The key sequence is sorted and deduplicated,
//! so it does not depend on row order; the seed sequence starts from the
//! segment's `body_sha` and advances by a fixed rule; the internal hash is
//! written out here rather than taken from the standard library; and **the
//! sizing arithmetic is integer**. The reference binary-fuse implementation
//! sizes its array through `floor(log(n)/log(3.33) + 2.25)` and a `round` of
//! a floating-point size factor; `libm`'s `log` is not guaranteed to agree
//! bit-for-bit across platforms, and a one-slot difference in array length
//! is a different file. [`SEGMENT_LENGTH_MIN_SIZE`] and
//! [`SIZE_FACTOR_Q10`] replace both with pinned integer tables.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{EngineError, Result};
use crate::row::RowKind;
use crate::segment::MmapSource;

const MAGIC: &[u8; 4] = b"TGVF";
const FOOTER_MAGIC: &[u8; 4] = b"TGVE";
const PREAMBLE: usize = 12; // magic + format + header_len
const ALIGN: usize = 8;

/// On-disk format of the `.tvf` file. Independent of the segment format:
/// this is a rebuildable cache, so a bump here orphans filter files and
/// rebuilds them rather than migrating anything (`tcsr.rs:56-59`).
///
/// **Format 2 is pre-planned and deliberately not built**: a
/// sorted `(key, row)` extent — the on-disk form of `SegmentPostings` — is
/// *exact*, needs no filter, and turns a probe into a binary search over a
/// mapped array, but costs 12.1 GB of sidecars at 1B against this format's
/// 2.30 GB. It is a disk-for-wall trade that deserves its own measurement,
/// and a format bump costs nothing because it orphans and rebuilds.
pub const VIDFILTER_FORMAT: u32 = 1;

/// Which construction a filter file holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Variant {
    /// Binary fuse filter with 16-bit fingerprints (Graf and Lemire, 2022).
    /// ≈18.2 bits per distinct key, ε ≈ 2⁻¹⁶, three lookups per probe.
    Fuse16,
    /// Classic Bloom at [`BLOOM_BITS_PER_KEY`] bits per key. The fallback
    /// when peeling fails, and — by flipping [`DEFAULT_VARIANT`] — the
    /// alternative default a measurement can elect.
    Bloom,
    /// No structure: every probe is a positive. The last step of the sequence,
    /// which is exactly today's behaviour and therefore always correct.
    /// Never written to disk; a construction that gets here writes nothing
    /// and the segment stays unfiltered.
    AlwaysPositive,
    /// A segment with no rows: every probe is a negative.
    Empty,
}

/// The construction this build elects first.
///
/// **The one constant that decides the construction.** fuse16 and an 18-bit Bloom are
/// within 2 % on space at equal ε; fuse16 wins on probe cost (3 lookups
/// against 12) and on being immutable by construction, which is a sealed
/// segment's own contract, and loses on being able to fail to peel — which
/// is why the Bloom path exists anyway, as the fallback. Setting this to
/// [`Variant::Bloom`] switches the default without touching anything else;
/// the fallback then simply never fires.
pub const DEFAULT_VARIANT: Variant = Variant::Fuse16;

/// Bits per key for the Bloom construction. 18 puts it at ε ≈ 1.7e-4,
/// which is `f = 0.09` at the 1B edge lane's `S = 509` — inside the same
/// `f ≤ 0.1` sizing rule fuse16 clears with three orders of margin.
pub const BLOOM_BITS_PER_KEY: u64 = 18;

/// Hash functions per Bloom probe: `round(0.693 · b)` at `b = 18`.
const BLOOM_HASHES: u32 = 12;

/// How many seeds the fuse sequence tries before falling back to Bloom.
/// A single binary-fuse construction peels with probability ≈ 1 − 2⁻⁴⁰ at
/// realistic sizes; eight is a sequence for the small and adversarial sets a
/// test can build, not for production.
const FUSE_SEEDS: u32 = 8;

/// Smallest key count at which the fuse array's segment-length exponent
/// reaches each index — the integer replacement for the reference
/// implementation's `floor(log(size)/log(3.33) + 2.25)`.
///
/// Verified equal to that expression for every size in `1..5000`, every
/// power of ten to 1e9 and every power of two to 2³¹. Index 18 is the
/// reference's own cap (262,144 slots per fuse segment).
const SEGMENT_LENGTH_MIN_SIZE: [u64; 19] = [
    1, 1, 1, 3, 9, 28, 92, 304, 1010, 3362, 11193, 37273, 124118, 413310, 1376322, 4583150,
    15261887, 50822082, 169237530,
];

/// Fuse array size factor in Q10 fixed point, indexed by `floor(log2(keys))`
/// — the integer replacement for `max(1.125, 0.875 + 0.25·log(1e6)/log(n))`.
///
/// Each bucket carries the factor at its **smallest** member, and the factor
/// falls with `n`, so the array is never smaller than the reference's and
/// the peeling success probability is never worse. At and above 2²⁰ keys the
/// factor is exactly 1.125 (`1152/1024`), which is the regime every
/// production-sized segment is in.
const SIZE_FACTOR_Q10: [u64; 41] = [
    2048, 5999, 3448, 2597, 2172, 1917, 1747, 1625, 1534, 1463, 1407, 1360, 1322, 1289, 1261, 1237,
    1215, 1197, 1180, 1165, 1152, 1152, 1152, 1152, 1152, 1152, 1152, 1152, 1152, 1152, 1152, 1152,
    1152, 1152, 1152, 1152, 1152, 1152, 1152, 1152, 1152,
];

/// Murmur3's 64-bit finalizer, written out so the filter's bytes are a
/// function of this source text and of nothing else.
#[inline]
fn mix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

/// splitmix64's step, used to walk the deterministic seed sequence.
#[inline]
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[inline]
fn mulhi(a: u64, b: u64) -> u64 {
    (((a as u128) * (b as u128)) >> 64) as u64
}

/// The seed sequence's origin: the segment's own `body_sha`, folded to 64
/// bits. Two segments therefore fail independently, and one segment's
/// sequence is the same on every machine that builds it.
fn seed_origin(body_sha: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in body_sha.as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    mix64(h)
}

/// Geometry of one fuse16 array, derived from the distinct key count alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FuseShape {
    segment_length: u32,
    segment_count: u32,
    segment_count_length: u32,
    array_length: u32,
}

impl FuseShape {
    /// Integer transcription of `binary_fuse_allocate`, with the two
    /// floating-point steps replaced by the pinned tables above. The
    /// wrapping arithmetic is the reference's own: at `keys <= 1` the
    /// initial segment count underflows and the two corrections below put it
    /// back, which is the behaviour the reference relies on.
    fn for_keys(keys: u64) -> Self {
        let mut e = 0usize;
        for (i, &min) in SEGMENT_LENGTH_MIN_SIZE.iter().enumerate() {
            if keys >= min {
                e = i;
            }
        }
        let segment_length = 1u32 << e;
        let capacity = if keys <= 1 {
            0u32
        } else {
            let bucket = (63 - keys.leading_zeros()) as usize;
            let f = SIZE_FACTOR_Q10[bucket.min(SIZE_FACTOR_Q10.len() - 1)];
            // round-half-up in Q10, as the reference's `round` does
            (((keys * f) + 512) >> 10) as u32
        };
        let init_segment_count = (capacity.wrapping_add(segment_length).wrapping_sub(1)
            / segment_length)
            .wrapping_sub(2);
        let mut array_length = init_segment_count.wrapping_add(2).wrapping_mul(segment_length);
        let mut segment_count = array_length.wrapping_add(segment_length - 1) / segment_length;
        segment_count = if segment_count <= 2 { 1 } else { segment_count - 2 };
        array_length = (segment_count + 2) * segment_length;
        Self {
            segment_length,
            segment_count,
            segment_count_length: segment_count * segment_length,
            array_length,
        }
    }

    #[inline]
    fn mask(&self) -> u32 {
        self.segment_length - 1
    }

    /// The three slots one hashed key occupies.
    #[inline]
    fn slots(&self, hash: u64) -> [u32; 3] {
        let h0 = mulhi(hash, self.segment_count_length as u64) as u32;
        let h1 = h0 + self.segment_length;
        let h2 = h1 + self.segment_length;
        [
            h0,
            h1 ^ (((hash >> 18) as u32) & self.mask()),
            h2 ^ ((hash as u32) & self.mask()),
        ]
    }
}

#[inline]
fn fingerprint16(hash: u64) -> u16 {
    (hash ^ (hash >> 32)) as u16
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct FilterHeader {
    seg_id: u64,
    body_sha: String,
    kind: String,
    /// [`crate::read::UID_KEY_ALGO_VERSION`] at build time. A node filter
    /// built over an older key function is orphaned, not trusted — the whole
    /// reason that key is pinned.
    key_algo_version: u32,
    /// Distinct identity keys the filter was built over.
    keys: u64,
    /// Rows the segment holds, for the `keys ≪ rows` accounting the report
    /// quotes; not part of any decision.
    rows: u32,
    seed: u64,
    variant: Variant,
    /// `extent bytes · 8 / keys`, rounded, as a receipt; not a decision.
    bits_per_key: u32,
    fingerprint_bits: u32,
    /// fuse16 geometry, `0` for other variants.
    segment_length: u32,
    segment_count: u32,
    array_length: u32,
    /// Bloom geometry, `0` for other variants.
    bloom_bits: u64,
    bloom_hashes: u32,
}

/// Where a loaded filter's bytes live.
///
/// A freshly built filter owns its serialized bytes; a loaded one is a
/// **file-backed mapping**, which is the single most useful property this
/// structure has. Unlike the anonymous postings heap it partly replaces, its
/// pages land in `RssFile`/`RssShmem` and the kernel can reclaim them under
/// pressure, so the memory-class split must charge it to
/// `file_mapped_total` and never to `anonymous_total`.
enum Backing {
    Owned(Vec<u8>),
    Mapped(MmapSource),
}

impl Backing {
    #[inline]
    fn bytes(&self) -> &[u8] {
        match self {
            Backing::Owned(v) => v,
            Backing::Mapped(m) => {
                use crate::segment::SegmentSource;
                m.bytes()
            }
        }
    }
}

/// One segment's identity filter.
pub struct VidFilter {
    header: FilterHeader,
    backing: Backing,
    /// Byte range of the filter array inside `backing`.
    extent: std::ops::Range<usize>,
    shape: FuseShape,
}

impl VidFilter {
    // ---------------------------------------------------------------- //
    // construction                                                      //
    // ---------------------------------------------------------------- //

    /// Build a filter over one segment's identity keys.
    ///
    /// `keys` is every stored row's key, in any order and with duplicates:
    /// this sorts and deduplicates, which is both what the fuse construction
    /// needs and what makes the bytes independent of row order.
    ///
    /// **Never fails.** The sequence is fuse16 over [`FUSE_SEEDS`] seeds, then
    /// a Bloom extent, then [`Variant::AlwaysPositive`], which is today's
    /// unfiltered behaviour. The caller does not have to handle a build
    /// error because there is no answer a failed build can change.
    pub fn build(
        seg_id: u64,
        kind: RowKind,
        body_sha: &str,
        rows: u32,
        mut keys: Vec<u64>,
    ) -> Self {
        keys.sort_unstable();
        keys.dedup();
        let origin = seed_origin(body_sha);
        let n = keys.len() as u64;

        if n == 0 {
            return Self::structureless(seg_id, kind, body_sha, rows, 0, Variant::Empty);
        }

        if DEFAULT_VARIANT == Variant::Fuse16 {
            let mut state = origin;
            for _ in 0..FUSE_SEEDS {
                let seed = splitmix64(&mut state);
                if let Some(extent) = fuse16_populate(&keys, seed) {
                    let shape = FuseShape::for_keys(n);
                    return Self::from_extent(
                        seg_id,
                        kind,
                        body_sha,
                        rows,
                        n,
                        seed,
                        Variant::Fuse16,
                        shape,
                        0,
                        0,
                        extent,
                    );
                }
            }
        }

        let mut state = origin;
        let seed = splitmix64(&mut state);
        let (bits, extent) = bloom_populate(&keys, seed);
        Self::from_extent(
            seg_id,
            kind,
            body_sha,
            rows,
            n,
            seed,
            Variant::Bloom,
            FuseShape::for_keys(0),
            bits,
            BLOOM_HASHES,
            extent,
        )
    }

    /// The `Empty` / `AlwaysPositive` steps: a header and no extent.
    fn structureless(
        seg_id: u64,
        kind: RowKind,
        body_sha: &str,
        rows: u32,
        keys: u64,
        variant: Variant,
    ) -> Self {
        Self::from_extent(
            seg_id,
            kind,
            body_sha,
            rows,
            keys,
            0,
            variant,
            FuseShape::for_keys(0),
            0,
            0,
            Vec::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_extent(
        seg_id: u64,
        kind: RowKind,
        body_sha: &str,
        rows: u32,
        keys: u64,
        seed: u64,
        variant: Variant,
        shape: FuseShape,
        bloom_bits: u64,
        bloom_hashes: u32,
        extent: Vec<u8>,
    ) -> Self {
        let bits_per_key = if keys == 0 {
            0
        } else {
            ((extent.len() as u64 * 8 * 10 + keys * 5) / (keys * 10)) as u32
        };
        let header = FilterHeader {
            seg_id,
            body_sha: body_sha.to_string(),
            kind: match kind {
                RowKind::Edge => "edge".into(),
                RowKind::Node => "node".into(),
            },
            key_algo_version: crate::read::UID_KEY_ALGO_VERSION,
            keys,
            rows,
            seed,
            variant,
            bits_per_key,
            fingerprint_bits: match variant {
                Variant::Fuse16 => 16,
                _ => 0,
            },
            segment_length: shape.segment_length,
            segment_count: shape.segment_count,
            array_length: shape.array_length,
            bloom_bits,
            bloom_hashes,
        };
        let len = extent.len();
        Self {
            header,
            backing: Backing::Owned(extent),
            extent: 0..len,
            shape,
        }
    }

    // ---------------------------------------------------------------- //
    // probe                                                             //
    // ---------------------------------------------------------------- //

    /// May this segment hold identity `key`?
    ///
    /// `false` is **exact**: the segment provably has no stored row with
    /// that key, so it has no *open* row with it either, and skipping it
    /// cannot change an answer. `true` means "fold and find out".
    #[inline]
    pub fn may_contain(&self, key: u64) -> bool {
        match self.header.variant {
            Variant::Empty => false,
            Variant::AlwaysPositive => true,
            Variant::Fuse16 => {
                let bytes = &self.backing.bytes()[self.extent.clone()];
                let hash = mix64(key.wrapping_add(self.header.seed));
                let slots = self.shape.slots(hash);
                let mut f = fingerprint16(hash);
                for s in slots {
                    let at = (s as usize) * 2;
                    // a header that lies about its geometry is rejected at
                    // load; this bound is the belt to that brace, and a
                    // short read answers `true`, which is never wrong
                    let Some(w) = bytes.get(at..at + 2) else {
                        return true;
                    };
                    f ^= u16::from_le_bytes([w[0], w[1]]);
                }
                f == 0
            }
            Variant::Bloom => {
                let bytes = &self.backing.bytes()[self.extent.clone()];
                let bits = self.header.bloom_bits;
                if bits == 0 {
                    return true;
                }
                let h1 = mix64(key.wrapping_add(self.header.seed));
                let h2 = mix64(h1) | 1;
                let mut probe = h1;
                for _ in 0..self.header.bloom_hashes {
                    let bit = probe % bits;
                    let (byte, off) = ((bit / 8) as usize, (bit % 8) as u8);
                    match bytes.get(byte) {
                        Some(b) if b & (1u8 << off) != 0 => {}
                        Some(_) => return false,
                        None => return true,
                    }
                    probe = probe.wrapping_add(h2);
                }
                true
            }
        }
    }

    // ---------------------------------------------------------------- //
    // accessors                                                         //
    // ---------------------------------------------------------------- //

    pub fn seg_id(&self) -> u64 {
        self.header.seg_id
    }

    pub fn body_sha(&self) -> &str {
        &self.header.body_sha
    }

    pub fn variant(&self) -> Variant {
        self.header.variant
    }

    pub fn keys(&self) -> u64 {
        self.header.keys
    }

    pub fn bits_per_key(&self) -> u32 {
        self.header.bits_per_key
    }

    /// Bytes this filter occupies where it lives — mapped for a loaded one,
    /// heap for one just built. The quantity `GNOMON_VIDFILTER_BYTES` bounds
    /// and `file_mapped_total` reports.
    pub fn extent_bytes(&self) -> u64 {
        self.extent.len() as u64
    }

    /// True when the bytes are a file mapping rather than this process's
    /// heap — the memory-class split needs this to stay exact.
    pub fn is_mapped(&self) -> bool {
        matches!(self.backing, Backing::Mapped(_))
    }

    // ---------------------------------------------------------------- //
    // serialization                                                     //
    // ---------------------------------------------------------------- //

    /// Serialize to the `.tvf` byte layout documented at the top of this
    /// module. A deterministic function of the header and the extent.
    pub fn to_bytes(&self) -> Vec<u8> {
        let header_json =
            serde_json::to_vec(&self.header).expect("the vidfilter header is serializable");
        let extent = &self.backing.bytes()[self.extent.clone()];
        let mut buf = Vec::with_capacity(PREAMBLE + header_json.len() + ALIGN + extent.len() + 12);
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VIDFILTER_FORMAT.to_le_bytes());
        buf.extend_from_slice(&(header_json.len() as u32).to_le_bytes());
        buf.extend_from_slice(&header_json);
        while buf.len() % ALIGN != 0 {
            buf.push(0);
        }
        buf.extend_from_slice(extent);
        let crc = crc32c::crc32c(extent);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&4u32.to_le_bytes());
        buf.extend_from_slice(FOOTER_MAGIC);
        buf
    }

    /// Validate bytes and take a view of the extent inside them, or `None`
    /// to mean "rebuild".
    fn parse(
        backing: Backing,
        seg_id: u64,
        kind: RowKind,
        body_sha: &str,
    ) -> Option<Self> {
        let bytes = backing.bytes();
        if bytes.len() < PREAMBLE + 12 || &bytes[..4] != MAGIC {
            return None;
        }
        if &bytes[bytes.len() - 4..] != FOOTER_MAGIC {
            return None;
        }
        if u32::from_le_bytes(bytes[4..8].try_into().ok()?) != VIDFILTER_FORMAT {
            return None;
        }
        let header_len = u32::from_le_bytes(bytes[8..12].try_into().ok()?) as usize;
        let header_end = PREAMBLE.checked_add(header_len)?;
        let header: FilterHeader = serde_json::from_slice(bytes.get(PREAMBLE..header_end)?).ok()?;
        if header.seg_id != seg_id || header.body_sha != body_sha {
            return None;
        }
        if header.kind
            != match kind {
                RowKind::Edge => "edge",
                RowKind::Node => "node",
            }
        {
            return None;
        }
        if header.key_algo_version != crate::read::UID_KEY_ALGO_VERSION {
            return None;
        }
        // an `AlwaysPositive` filter is never written; a file claiming to be
        // one is not something this build produced
        if header.variant == Variant::AlwaysPositive {
            return None;
        }

        let footer_end = bytes.len() - 8;
        if u32::from_le_bytes(bytes[footer_end..footer_end + 4].try_into().ok()?) != 4 {
            return None;
        }
        let crc_at = footer_end.checked_sub(4)?;
        let crc = u32::from_le_bytes(bytes[crc_at..footer_end].try_into().ok()?);

        let start = header_end.next_multiple_of(ALIGN);
        // the alignment padding is outside both the header JSON and the
        // extent CRC, so it is the one region damage could hide in
        if bytes.get(header_end..start)?.iter().any(|&b| b != 0) {
            return None;
        }
        let want = match header.variant {
            Variant::Fuse16 => (header.array_length as usize).checked_mul(2)?,
            Variant::Bloom => (header.bloom_bits as usize).div_ceil(8),
            Variant::Empty | Variant::AlwaysPositive => 0,
        };
        let end = start.checked_add(want)?;
        if end != crc_at {
            return None;
        }
        if crc32c::crc32c(bytes.get(start..end)?) != crc {
            return None;
        }

        // structural backstop behind the checksum: a file that passes its
        // CRC can still describe a geometry the probe would index out of,
        // and `may_contain`'s own bound check would then answer `true`
        // forever instead of filtering. Checked once, at load.
        let shape = FuseShape {
            segment_length: header.segment_length,
            segment_count: header.segment_count,
            segment_count_length: header
                .segment_count
                .checked_mul(header.segment_length)?,
            array_length: header.array_length,
        };
        if header.variant == Variant::Fuse16 {
            if shape != FuseShape::for_keys(header.keys) {
                return None;
            }
            if !header.segment_length.is_power_of_two() {
                return None;
            }
        }
        if header.variant == Variant::Bloom
            && (header.bloom_hashes == 0 || header.bloom_bits == 0)
        {
            return None;
        }

        Some(Self {
            header,
            backing,
            extent: start..end,
            shape,
        })
    }

    /// Parse owned bytes written by [`Self::to_bytes`].
    pub fn from_bytes(
        bytes: &[u8],
        seg_id: u64,
        kind: RowKind,
        body_sha: &str,
    ) -> Option<Self> {
        Self::parse(Backing::Owned(bytes.to_vec()), seg_id, kind, body_sha)
    }

    /// Map the filter file for one segment, or `None` to mean "rebuild".
    /// Never an error: this is a cache.
    ///
    /// The mapping is held per **handle** (`store.rs`'s filter map), swept by
    /// `retain_segments` on gc with the segments it belongs to — explicitly
    /// not a process-global cache: `nameidx::CACHE` is unbounded, has no gc
    /// hook, and is a known latent leak. Do not repeat it.
    pub fn load(path: &Path, seg_id: u64, kind: RowKind, body_sha: &str) -> Option<Self> {
        let src = MmapSource::load(path).ok()?;
        Self::parse(Backing::Mapped(src), seg_id, kind, body_sha)
    }

    /// Write via tmp + rename, so a reader of a live system sees old bytes,
    /// new bytes, or nothing. Errors are the caller's to swallow: a store
    /// that cannot take the write must still answer queries, it just
    /// rebuilds next time. An `AlwaysPositive` filter writes nothing — there
    /// is nothing a file could say that the absence of one does not.
    ///
    /// **Not fsynced**. The filter is a rebuildable sidecar that no
    /// manifest names, so durability buys nothing: after a crash the file
    /// may be missing, empty or torn, and every one of those fails `parse`
    /// (magic, format, header, the CRC32C over the extent, the geometry
    /// check) and reads as "no filter", which is the unfiltered behaviour and
    /// correct; the next fold of that segment writes it again. An fsync per
    /// filter was one per sealed segment and one per compaction output —
    /// 14,505 of them in a 300M build on a volume where each costs 11–25 ms.
    pub fn write(&self, path: &Path) -> Result<()> {
        use std::io::Write;
        if self.header.variant == Variant::AlwaysPositive {
            return Ok(());
        }
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| EngineError::from(e).at_file(dir))?;
        }
        let tmp = path.with_extension(format!("tvf.tmp{}", std::process::id()));
        let bytes = self.to_bytes();
        {
            let mut f =
                std::fs::File::create(&tmp).map_err(|e| EngineError::from(e).at_file(&tmp))?;
            f.write_all(&bytes)
                .map_err(|e| EngineError::from(e).at_file(&tmp))?;
        }
        std::fs::rename(&tmp, path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            EngineError::from(e).at_file(path)
        })
    }
}

/// One attempt at a binary-fuse16 construction, or `None` if the key set did
/// not peel at this seed.
///
/// A transcription of `binary_fuse16_populate` (Graf and Lemire, 2022), with
/// the reference's duplicate handling removed because [`VidFilter::build`]
/// deduplicates first, and its floating-point sizing replaced by
/// [`FuseShape`]. Transient cost is ≈24 bytes per key — the same order as
/// the fold's own `Vec` — and it is released before the extent is written.
fn fuse16_populate(keys: &[u64], seed: u64) -> Option<Vec<u8>> {
    let size = keys.len();
    let shape = FuseShape::for_keys(size as u64);
    let capacity = shape.array_length as usize;

    let mut block_bits = 1u32;
    while (1u32 << block_bits) < shape.segment_count {
        block_bits += 1;
    }
    let block = 1usize << block_bits;

    let mut reverse_order = vec![0u64; size + 1];
    let mut reverse_h = vec![0u8; size];
    let mut alone = vec![0u32; capacity];
    let mut t2count = vec![0u8; capacity];
    let mut t2hash = vec![0u64; capacity];
    let mut start_pos = vec![0u32; block];

    // the sentinel that makes the "slot taken" probe below terminate
    reverse_order[size] = 1;

    for (i, s) in start_pos.iter_mut().enumerate() {
        *s = (((i as u64) * (size as u64)) >> block_bits) as u32;
    }
    let mask_block = (block - 1) as u64;
    for &k in keys {
        let hash = mix64(k.wrapping_add(seed));
        let mut seg = if block_bits >= 64 {
            0
        } else {
            hash >> (64 - block_bits)
        };
        while reverse_order[start_pos[seg as usize] as usize] != 0 {
            seg = (seg + 1) & mask_block;
        }
        reverse_order[start_pos[seg as usize] as usize] = hash;
        start_pos[seg as usize] += 1;
    }

    for &hash in reverse_order.iter().take(size) {
        let [h0, h1, h2] = shape.slots(hash);
        t2count[h0 as usize] = t2count[h0 as usize].wrapping_add(4);
        t2hash[h0 as usize] ^= hash;
        t2count[h1 as usize] = t2count[h1 as usize].wrapping_add(4);
        t2count[h1 as usize] ^= 1;
        t2hash[h1 as usize] ^= hash;
        t2count[h2 as usize] = t2count[h2 as usize].wrapping_add(4);
        t2count[h2 as usize] ^= 2;
        t2hash[h2 as usize] ^= hash;
        // an overflowed 6-bit occupancy counter means this seed cannot be
        // used; the sequence moves on
        if t2count[h0 as usize] < 4 || t2count[h1 as usize] < 4 || t2count[h2 as usize] < 4 {
            return None;
        }
    }

    // peel: repeatedly remove a key that is alone in one of its three slots
    let mut q = 0usize;
    for (i, &c) in t2count.iter().enumerate() {
        alone[q] = i as u32;
        q += usize::from((c >> 2) == 1);
    }
    let mut stack = 0usize;
    while q > 0 {
        q -= 1;
        let index = alone[q] as usize;
        if (t2count[index] >> 2) != 1 {
            continue;
        }
        let hash = t2hash[index];
        let [h0, h1, h2] = shape.slots(hash);
        let h012 = [h0, h1, h2, h0, h1];
        let found = (t2count[index] & 3) as usize;
        reverse_h[stack] = found as u8;
        reverse_order[stack] = hash;
        stack += 1;
        for step in 1..=2usize {
            let other = h012[found + step] as usize;
            alone[q] = other as u32;
            q += usize::from((t2count[other] >> 2) == 2);
            t2count[other] = t2count[other].wrapping_sub(4);
            t2count[other] ^= mod3((found + step) as u8);
            t2hash[other] ^= hash;
        }
    }
    if stack != size {
        return None;
    }

    // assign fingerprints in reverse peel order, so each key's own slot is
    // written after the two it xors against
    let mut fp = vec![0u16; capacity];
    for i in (0..size).rev() {
        let hash = reverse_order[i];
        let f = fingerprint16(hash);
        let found = reverse_h[i] as usize;
        let [h0, h1, h2] = shape.slots(hash);
        let h012 = [h0, h1, h2, h0, h1];
        fp[h012[found] as usize] =
            f ^ fp[h012[found + 1] as usize] ^ fp[h012[found + 2] as usize];
    }

    let mut out = Vec::with_capacity(capacity * 2);
    for v in fp {
        out.extend_from_slice(&v.to_le_bytes());
    }
    Some(out)
}

#[inline]
fn mod3(x: u8) -> u8 {
    if x > 2 {
        x - 3
    } else {
        x
    }
}

/// The Bloom step: `(bits, extent)` at [`BLOOM_BITS_PER_KEY`] bits per
/// distinct key, [`BLOOM_HASHES`] probes, Kirsch–Mitzenmacher double
/// hashing. It cannot fail, which is the whole reason it is here.
fn bloom_populate(keys: &[u64], seed: u64) -> (u64, Vec<u8>) {
    let bits = (keys.len() as u64 * BLOOM_BITS_PER_KEY).max(64);
    let mut out = vec![0u8; (bits as usize).div_ceil(8)];
    for &k in keys {
        let h1 = mix64(k.wrapping_add(seed));
        let h2 = mix64(h1) | 1;
        let mut probe = h1;
        for _ in 0..BLOOM_HASHES {
            let bit = probe % bits;
            out[(bit / 8) as usize] |= 1u8 << (bit % 8) as u8;
            probe = probe.wrapping_add(h2);
        }
    }
    (bits, out)
}

/// `<root>/idx/vidfilter/<seg_id:012>.tvf`.
pub fn filter_path(root: &Path, seg_id: u64) -> PathBuf {
    root.join("idx")
        .join("vidfilter")
        .join(format!("{seg_id:012}.tvf"))
}

/// `<root>/idx/vidfilter`.
pub fn filter_dir(root: &Path) -> PathBuf {
    root.join("idx").join("vidfilter")
}

/// Is the seal-time build enabled? `GNOMON_VIDFILTER=0` turns it off — the
/// switch the build-time gate compares against.
///
/// **Default on, against the project's "indexes are built lazily at first
/// use" policy** (`adapter.py:781-784`), and the departure is deliberate.
/// The 1B correction ramp measures as *one* operation: the first
/// correction's identity probe is the entire 11.5-minute cost. A filter
/// built lazily on first use would therefore be written during a whole-store
/// pass that has already happened, and would provably not help the case the
/// filter exists for. The lazy path stays as the
/// correctness fallback and as the no-migration story for existing stores.
pub fn build_at_seal_enabled() -> bool {
    !matches!(
        std::env::var("GNOMON_VIDFILTER").ok().as_deref(),
        Some("0") | Some("false")
    )
}

/// The per-handle byte ceiling on mapped filters, `min(RAM/64, 4 GiB)` by
/// default. `GNOMON_VIDFILTER_BYTES` overrides in both directions and `0`
/// means unbounded, exactly as `GNOMON_POSTINGS_BYTES` does for the postings.
///
/// A segment whose filter would not fit is mapped, probed and dropped — the
/// `can_hold` idiom (`store.rs:2602`) — so a one-byte ceiling is correct
/// rather than a thrash. At 1B the whole-store need is 2.30 GB against a
/// 4 GiB default.
pub fn filter_budget(env: Option<&str>, ram_bytes: Option<u64>) -> Option<u64> {
    if let Some(v) = env.and_then(|s| s.trim().parse::<u64>().ok()) {
        return if v == 0 { None } else { Some(v) };
    }
    Some(
        ram_bytes
            .map(|r| (r / 64).min(crate::defaults::VIDFILTER_MAX_BYTES))
            .unwrap_or(crate::defaults::VIDFILTER_MAX_BYTES),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys_of(n: usize, salt: u64) -> Vec<u64> {
        (0..n as u64).map(|i| mix64(i.wrapping_add(salt))).collect()
    }

    fn built(keys: Vec<u64>) -> VidFilter {
        let rows = keys.len() as u32;
        VidFilter::build(7, RowKind::Edge, "abc123", rows, keys)
    }

    // ------------------------------------------------------------ //
    // exact-negative                                                //
    // ------------------------------------------------------------ //

    /// The contract, over random and worst-case key sets. A false positive
    /// costs a fold; a false negative would drop a version from an answer,
    /// so **every** stored key must be admitted, always.
    #[test]
    fn a_filter_admits_every_stored_row() {
        let cases: Vec<Vec<u64>> = vec![
            vec![],
            vec![0],
            vec![u64::MAX],
            // all-identical identities: m = 1, r large
            vec![42; 10_000],
            // two identities over many rows: m << r
            (0..10_000u64).map(|i| i % 2).collect(),
            // all-distinct
            keys_of(1, 0),
            keys_of(2, 0),
            keys_of(3, 0),
            keys_of(7, 0),
            keys_of(63, 0),
            keys_of(64, 0),
            keys_of(1_000, 0),
            keys_of(10_000, 1),
            keys_of(50_000, 2),
            // adversarial: keys that share their high bits, which is where
            // the fuse construction's segment choice comes from
            (0..20_000u64).map(|i| i << 40).collect(),
            // …and their low bits, which is where two of the three slot
            // offsets come from
            (0..20_000u64).map(|i| i << 18).collect(),
            // a dense run, the shape a dictionary-coded key space produces
            (0..20_000u64).collect(),
        ];
        for keys in cases {
            let f = built(keys.clone());
            for &k in &keys {
                assert!(
                    f.may_contain(k),
                    "false negative at {k:#x} in a set of {} ({:?})",
                    keys.len(),
                    f.variant()
                );
            }
            // and through the file format, which is how it is actually used
            let bytes = f.to_bytes();
            if f.variant() != Variant::AlwaysPositive {
                let back = VidFilter::from_bytes(&bytes, 7, RowKind::Edge, "abc123")
                    .expect("round-trips");
                for &k in &keys {
                    assert!(back.may_contain(k), "false negative after a round trip");
                }
            }
        }
    }

    /// An empty segment filters *everything* out, which is the one case
    /// where the filter's answer is better than a fold's and not merely
    /// cheaper.
    #[test]
    fn an_empty_segment_admits_nothing() {
        let f = built(vec![]);
        assert_eq!(f.variant(), Variant::Empty);
        assert_eq!(f.keys(), 0);
        for k in keys_of(100, 9) {
            assert!(!f.may_contain(k));
        }
    }

    // ------------------------------------------------------------ //
    // false-positive rate                                           //
    // ------------------------------------------------------------ //

    /// ε within 2× of the design's `2⁻¹⁶`. Measured over keys the filter has
    /// never seen; the count is large enough that the band is a real test
    /// and small enough to stay a unit test.
    #[test]
    fn the_false_positive_rate_is_within_twice_the_design_epsilon() {
        let f = built(keys_of(200_000, 11));
        assert_eq!(f.variant(), Variant::Fuse16);
        let trials = 2_000_000u64;
        let fp = (0..trials)
            .map(|i| mix64(i ^ 0xdead_beef_0000_0000))
            .filter(|&k| f.may_contain(k))
            .count() as f64;
        let eps = fp / trials as f64;
        let design = 1.0 / 65_536.0;
        assert!(
            eps <= 2.0 * design,
            "measured epsilon {eps:.3e} exceeds twice the design {design:.3e}"
        );
        // and the bits it costs are the design's ≈18.2
        assert!(
            (17..=19).contains(&f.bits_per_key()),
            "bits per key {} is not the design's ~18.2",
            f.bits_per_key()
        );
    }

    #[test]
    fn the_bloom_fallback_is_exact_negative_too_and_is_sized_as_designed() {
        let keys = keys_of(20_000, 13);
        let seed = 0x1234_5678;
        let (bits, extent) = bloom_populate(&keys, seed);
        assert_eq!(bits, 20_000 * BLOOM_BITS_PER_KEY);
        let f = VidFilter::from_extent(
            7,
            RowKind::Edge,
            "abc123",
            20_000,
            keys.len() as u64,
            seed,
            Variant::Bloom,
            FuseShape::for_keys(0),
            bits,
            BLOOM_HASHES,
            extent,
        );
        for &k in &keys {
            assert!(f.may_contain(k), "bloom false negative");
        }
        let trials = 200_000u64;
        let fp = (0..trials)
            .map(|i| mix64(i ^ 0x5555_0000_0000_0000))
            .filter(|&k| f.may_contain(k))
            .count() as f64;
        // b = 18 puts classic Bloom at ~1.7e-4; allow 4x
        assert!(fp / trials as f64 <= 7e-4, "bloom epsilon {}", fp / trials as f64);
        assert!(!f.to_bytes().is_empty());
    }

    /// The last step answers every probe positively, which is exactly the
    /// unfiltered behaviour, and is never written to disk.
    #[test]
    fn a_construction_failure_falls_back_and_still_answers() {
        let f = VidFilter::structureless(7, RowKind::Edge, "s", 3, 3, Variant::AlwaysPositive);
        for k in keys_of(50, 4) {
            assert!(f.may_contain(k));
        }
        let dir = std::env::temp_dir().join(format!("gnomon-vf-ap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = filter_path(&dir, 7);
        f.write(&path).expect("writing an always-positive is a no-op");
        assert!(!path.exists(), "an always-positive filter leaves no file");
        // and a file that claims to be one is refused rather than trusted
        let bytes = f.to_bytes();
        assert!(VidFilter::from_bytes(&bytes, 7, RowKind::Edge, "s").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ------------------------------------------------------------ //
    // determinism                                           //
    // ------------------------------------------------------------ //

    #[test]
    fn the_same_segment_builds_the_same_bytes_twice() {
        for n in [0usize, 1, 2, 17, 1_000, 40_000] {
            let a = built(keys_of(n, 5)).to_bytes();
            let b = built(keys_of(n, 5)).to_bytes();
            assert_eq!(a, b, "{n} keys did not rebuild identically");
        }
    }

    /// Row order must not reach the bytes: the key sequence is sorted and
    /// deduplicated first, so a segment and a permutation of it agree.
    #[test]
    fn row_order_and_duplicate_rows_do_not_reach_the_bytes() {
        let keys = keys_of(5_000, 6);
        let mut shuffled = keys.clone();
        shuffled.reverse();
        let mut doubled = keys.clone();
        doubled.extend(keys.iter().copied());
        assert_eq!(built(keys.clone()).to_bytes(), built(shuffled).to_bytes());
        // the same distinct keys over twice as many rows: the extent is the
        // same, the header's `rows` is not, so compare the extents
        let a = built(keys.clone());
        let b = VidFilter::build(7, RowKind::Edge, "abc123", keys.len() as u32, doubled);
        assert_eq!(a.keys(), b.keys());
        assert_eq!(a.to_bytes(), b.to_bytes());
    }

    /// The trip-wire a toolchain or a constant change trips. A change here
    /// means the bytes moved and [`VIDFILTER_FORMAT`] must be bumped
    /// deliberately — the idiom `nameidx::tests::casefold_vectors_are_pinned`
    /// established.
    #[test]
    fn a_golden_vector_pins_the_bytes_of_a_small_fixed_segment() {
        let keys: Vec<u64> = vec![
            0x0000_0000_0000_0001,
            0x0000_0000_0000_0002,
            0xdead_beef_cafe_babe,
            0xffff_ffff_ffff_ffff,
            0x0123_4567_89ab_cdef,
        ];
        let f = VidFilter::build(3, RowKind::Edge, "0f0f0f0f", 5, keys.clone());
        assert_eq!(f.variant(), Variant::Fuse16);
        let bytes = f.to_bytes();
        let sha = crate::derive::sha256_hex_bytes(&bytes);
        assert_eq!(
            sha, "30ea6c2f94fd58458c10b3a6b02722cba700d81cbf371fcb173e0ff926da2a9f",
            "the filter's bytes moved: {} bytes, sha {sha}",
            bytes.len()
        );
    }

    #[test]
    fn the_seed_ladder_is_a_function_of_the_body_sha() {
        let keys = keys_of(1_000, 7);
        let a = VidFilter::build(1, RowKind::Edge, "sha-a", 1_000, keys.clone());
        let b = VidFilter::build(1, RowKind::Edge, "sha-b", 1_000, keys.clone());
        assert_ne!(a.to_bytes(), b.to_bytes(), "two shas produced one sequence");
        let a2 = VidFilter::build(1, RowKind::Edge, "sha-a", 1_000, keys);
        assert_eq!(a.to_bytes(), a2.to_bytes());
    }

    /// The sizing tables are the integer replacement for the reference's
    /// `log`/`round`, and this is what pins them.
    #[test]
    fn the_array_geometry_is_integer_and_pinned() {
        // a production-sized edge segment: 1.965 M keys at 1B
        let s = FuseShape::for_keys(1_965_000);
        assert_eq!(s.segment_length, 1 << 14);
        // 1.125 · keys, rounded up to whole fuse segments
        assert_eq!(s.array_length, (s.segment_count + 2) * s.segment_length);
        let bits = (s.array_length as f64 * 16.0) / 1_965_000.0;
        assert!(
            (17.0..19.0).contains(&bits),
            "{bits} bits per key is not the design's ~18.2"
        );
        // the degenerate ends do not panic and do not underflow
        for n in [0u64, 1, 2, 3] {
            let s = FuseShape::for_keys(n);
            assert!(s.array_length >= s.segment_length);
            assert_eq!(s.array_length, (s.segment_count + 2) * s.segment_length);
        }
    }

    // ------------------------------------------------------------ //
    // the index rule: every failure is a rebuild                       //
    // ------------------------------------------------------------ //

    #[test]
    fn a_stale_or_corrupt_filter_rebuilds_silently() {
        let f = built(keys_of(500, 8));
        let good = f.to_bytes();
        assert!(VidFilter::from_bytes(&good, 7, RowKind::Edge, "abc123").is_some());
        // wrong segment, wrong stamp, wrong kind
        assert!(VidFilter::from_bytes(&good, 8, RowKind::Edge, "abc123").is_none());
        assert!(VidFilter::from_bytes(&good, 7, RowKind::Edge, "other").is_none());
        assert!(VidFilter::from_bytes(&good, 7, RowKind::Node, "abc123").is_none());
        // bad magic
        assert!(VidFilter::from_bytes(b"not a filter at all", 7, RowKind::Edge, "abc").is_none());
        assert!(VidFilter::from_bytes(b"", 7, RowKind::Edge, "abc").is_none());
        // every single-byte flip is caught by the CRC, the magic, the
        // format, the header parse or the geometry check
        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0xff;
            assert!(
                VidFilter::from_bytes(&bad, 7, RowKind::Edge, "abc123").is_none(),
                "a flipped byte at {i} was accepted"
            );
        }
        // and so is truncation at every length
        for cut in 0..good.len() {
            assert!(VidFilter::from_bytes(&good[..cut], 7, RowKind::Edge, "abc123").is_none());
        }
    }

    #[test]
    fn a_filter_built_over_an_older_key_function_is_orphaned() {
        let f = built(keys_of(100, 10));
        let mut header = f.header.clone();
        header.key_algo_version = crate::read::UID_KEY_ALGO_VERSION + 1;
        let stale = VidFilter {
            header,
            backing: Backing::Owned(f.backing.bytes()[f.extent.clone()].to_vec()),
            extent: 0..f.extent.len(),
            shape: f.shape,
        };
        assert!(
            VidFilter::from_bytes(&stale.to_bytes(), 7, RowKind::Edge, "abc123").is_none(),
            "a filter over a moved node key must never be trusted"
        );
    }

    #[test]
    fn a_header_that_lies_about_its_geometry_is_refused() {
        let f = built(keys_of(5_000, 12));
        let mut header = f.header.clone();
        header.array_length *= 2;
        let lying = VidFilter {
            header,
            backing: Backing::Owned(f.backing.bytes()[f.extent.clone()].to_vec()),
            extent: 0..f.extent.len(),
            shape: f.shape,
        };
        assert!(VidFilter::from_bytes(&lying.to_bytes(), 7, RowKind::Edge, "abc123").is_none());
    }

    #[test]
    fn write_then_load_round_trips_and_a_missing_file_is_none() {
        let dir = std::env::temp_dir().join(format!("gnomon-vf-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = filter_path(&dir, 7);
        assert!(VidFilter::load(&path, 7, RowKind::Edge, "abc123").is_none());
        let keys = keys_of(2_000, 14);
        let f = built(keys.clone());
        f.write(&path).expect("writes");
        let back = VidFilter::load(&path, 7, RowKind::Edge, "abc123").expect("loads");
        assert!(back.is_mapped(), "a loaded filter is file-backed");
        assert_eq!(back.extent_bytes(), f.extent_bytes());
        for &k in &keys {
            assert!(back.may_contain(k));
        }
        assert!(VidFilter::load(&path, 7, RowKind::Edge, "nope").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn filter_paths_are_zero_padded_under_idx_vidfilter() {
        let p = filter_path(Path::new("/store/native"), 42);
        assert!(p.ends_with("idx/vidfilter/000000000042.tvf"), "{p:?}");
        assert_eq!(filter_dir(Path::new("/store/native")), p.parent().unwrap());
    }

    // ------------------------------------------------------------ //
    // knobs                                                         //
    // ------------------------------------------------------------ //

    #[test]
    fn the_byte_ceiling_reads_its_env_in_both_directions() {
        assert_eq!(filter_budget(Some("0"), Some(1 << 40)), None);
        assert_eq!(filter_budget(Some("4096"), Some(1 << 40)), Some(4096));
        // RAM/64, under the ceiling
        assert_eq!(filter_budget(None, Some(64 << 30)), Some(1 << 30));
        // and the ceiling itself above it
        assert_eq!(
            filter_budget(None, Some(1 << 50)),
            Some(crate::defaults::VIDFILTER_MAX_BYTES)
        );
        assert_eq!(
            filter_budget(None, None),
            Some(crate::defaults::VIDFILTER_MAX_BYTES)
        );
        // junk is not a budget of zero
        assert_eq!(
            filter_budget(Some("nonsense"), Some(64 << 30)),
            Some(1 << 30)
        );
    }
}
