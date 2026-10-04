//! Column compression: per-block frame-of-reference bit-packing (codec 1).
//!
//! One codec, chosen by measurement (a storage breakdown at 1M rows): the store's bytes are dominated by integer columns whose values
//! sit in narrow ranges — `vt_s`/`vt_e` are sorted or nearly so, the four
//! string-ref columns are constant or sequential within a segment, and
//! `src_id`/`dst_id`/`rel_code` are bounded by the dictionary. Frame-of-
//! reference handles all of those with one mechanism: per block, store the
//! minimum and the bit width of (max - min), then pack each delta at that
//! width. A constant block costs one bit per row of overhead-free truth:
//! width 0, no payload at all. The `vid` halves are sha256 prefixes and do
//! not compress; the writer measures and keeps them raw.
//!
//! Decoding is a full-column materialization, not random access. That is a
//! deliberate pairing with the store's segment cache: a segment file is
//! immutable, so it is decoded once per process and the hot path keeps
//! serving plain slices. Nothing in the scan loop changed shape.
//!
//! Layout (little-endian):
//!   u32 n_values | u32 block_len | blocks...
//! block: i64 min | u8 width (0..=64) | ceil(len * width / 8) packed bytes
//!
//! Every read is bounds-checked: a corrupt stream must produce an error,
//! never a panic or an over-read — decode runs even when checksum
//! verification is skipped, so it is itself a structural validator.

use crate::error::{EngineError, Result};

pub const CODEC_RAW: u32 = 0;
pub const CODEC_FOR: u32 = 1;
/// Per-block linear frame-of-reference; see [`crate::linfor`]. `codec` has
/// been a `u32` reserved for exactly this since format v0, so adding it is a
/// value change and `FORMAT_VERSION` stays 1: an old build meets codec 2 and
/// fails its bounds check loudly rather than misreading, and a new build
/// meets codec 0 or 1 and takes the path it always took.
pub const CODEC_LINMODEL: u32 = 2;

const BLOCK: usize = 4096;

/// Pack `vals` as frame-of-reference deltas. Total order of bytes is fully
/// determined by the input, so identical columns encode identically —
/// segment bytes stay deterministic.
pub fn encode_i64(vals: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + vals.len());
    out.extend_from_slice(&(vals.len() as u32).to_le_bytes());
    out.extend_from_slice(&(BLOCK as u32).to_le_bytes());
    for block in vals.chunks(BLOCK) {
        let min = block.iter().copied().min().expect("chunks are non-empty");
        // wrapping: the delta of any i64 pair fits u64
        let max_delta = block
            .iter()
            .map(|&v| v.wrapping_sub(min) as u64)
            .max()
            .expect("chunks are non-empty");
        let width = 64 - max_delta.leading_zeros() as usize; // 0..=64
        out.extend_from_slice(&min.to_le_bytes());
        out.push(width as u8);
        if width == 0 {
            continue; // constant block: the min is the whole story
        }
        let mut acc: u128 = 0;
        let mut bits = 0usize;
        for &v in block {
            acc |= ((v.wrapping_sub(min) as u64) as u128) << bits;
            bits += width;
            while bits >= 8 {
                out.push((acc & 0xFF) as u8);
                acc >>= 8;
                bits -= 8;
            }
        }
        if bits > 0 {
            out.push((acc & 0xFF) as u8);
        }
    }
    out
}

/// `encode_i64(vals).len()`, computed without packing a bit: the
/// trial encode compares candidates by size and materialises only the one
/// it keeps. Same block walk, same widths; held equal by a test.
pub fn encoded_len_i64(vals: &[i64]) -> usize {
    let mut n = 8;
    for block in vals.chunks(BLOCK) {
        let min = block.iter().copied().min().expect("chunks are non-empty");
        let max_delta = block
            .iter()
            .map(|&v| v.wrapping_sub(min) as u64)
            .max()
            .expect("chunks are non-empty");
        let width = 64 - max_delta.leading_zeros() as usize;
        n += 8 + 1 + (block.len() * width).div_ceil(8);
    }
    n
}

// ------------------------------------------------------------------- //
// exact scale divisor                                                  //
// ------------------------------------------------------------------- //

/// The exact scale of a column: `(base, divisor)` such that every value is
/// `base + divisor * k` for some integer `k`.
///
/// This is not a model and it is not lossy. SNAP's temporal datasets are
/// Unix **seconds** lifted into microseconds, so the greatest common divisor
/// of the first differences is exactly 1,000,000 on every real store
/// measured — twenty bits of every packed delta are provably zero, and
/// dividing them out recovers them exactly. On the synthetic corpus the
/// divisor is the generator's tick before compaction and drops to 200 after
/// compaction mixes tick phases, so it is emphatically *not* a constant an
/// encoder may assume: it has to be measured per column and recorded.
///
/// `gcd` over `i64` is a deterministic integer computation, so determinism holds
/// by construction: the same values give the same divisor on any host.
///
/// Returns `None` when there is nothing to divide out — a divisor of 1, a
/// constant or empty column, or (the honest bail-out) a quotient that does
/// not fit an `i64`. Correctness never depends on the answer: a column the
/// scale declines simply encodes as it did before.
pub fn exact_scale(vals: &[i64]) -> Option<(i64, i64)> {
    let &base = vals.first()?;
    // The greatest common divisor in u64 — a difference of two i64
    // has magnitude at most 2^64 - 1, so nothing is lost against the u128
    // it used to be computed in — and the range check below on the column's
    // extremes only. Same answer on every input (tests hold it to the
    // previous form).
    let mut g: u64 = 0;
    for w in vals.windows(2) {
        let d = ((w[1] as i128) - (w[0] as i128)).unsigned_abs() as u64;
        g = gcd_u64(g, d);
        if g == 1 {
            return None; // nothing to recover; the common case, exited early
        }
    }
    // a constant column has no first differences to share a factor
    if g == 0 || g > i64::MAX as u64 {
        return None;
    }
    let divisor = g as i64;
    // every quotient must survive as an i64. `(v - base) / g` with
    // truncation is monotone in `v`, so the extremes decide for every value
    let (lo, hi) = vals
        .iter()
        .fold((i64::MAX, i64::MIN), |(lo, hi), &v| (lo.min(v), hi.max(v)));
    let (base128, g128) = (base as i128, g as i128);
    for v in [lo, hi] {
        let q = (v as i128 - base128) / g128;
        if q < i64::MIN as i128 || q > i64::MAX as i128 {
            return None;
        }
    }
    Some((base, divisor))
}

/// `(v - base) / divisor` for every value. The caller has already proved
/// with [`exact_scale`] that this is exact and in range.
pub fn scale_down(vals: &[i64], base: i64, divisor: i64) -> Vec<i64> {
    let (b, d) = (base as i128, divisor as i128);
    // In i64 whenever `v - base` fits, which is every value of a
    // real column; i128 only for the differences that do not. Truncating
    // division of the same integer by the same divisor either way.
    vals.iter()
        .map(|&v| match v.checked_sub(base) {
            Some(x) => x / divisor,
            None => ((v as i128 - b) / d) as i64,
        })
        .collect()
}

/// `base + divisor * k`, in place, undoing [`scale_down`].
///
/// Checked, not wrapping: these three numbers come off disk, and a corrupt
/// header must produce an error rather than a plausible wrong timestamp.
/// `divisor == 1 && base == 0` is the identity every older column
/// carries, and it returns without touching the values.
pub fn scale_up(vals: &mut [i64], base: i64, divisor: i64) -> Result<()> {
    if divisor == 1 && base == 0 {
        return Ok(());
    }
    if divisor < 1 {
        return Err(EngineError::corrupt(format!(
            "column scale divisor {divisor} is not a positive integer"
        )));
    }
    for v in vals.iter_mut() {
        *v = v
            .checked_mul(divisor)
            .and_then(|s| s.checked_add(base))
            .ok_or_else(|| {
                EngineError::corrupt(format!(
                    "scaled value {v} x {divisor} + {base} overflows i64"
                ))
            })?;
    }
    Ok(())
}

fn gcd_u64(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let t = a % b;
        a = b;
        b = t;
    }
    a
}

#[cfg(test)]
fn gcd_u128(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        let t = a % b;
        a = b;
        b = t;
    }
    a
}

pub fn decode_i64(bytes: &[u8], expect: usize) -> Result<Vec<i64>> {
    let bad = |what: &str| EngineError::corrupt(format!("FOR stream: {what}"));
    let take = |at: usize, n: usize| -> Result<&[u8]> {
        bytes.get(at..at + n).ok_or_else(|| bad("truncated"))
    };
    let n = u32::from_le_bytes(take(0, 4)?.try_into().unwrap()) as usize;
    let block_len = u32::from_le_bytes(take(4, 4)?.try_into().unwrap()) as usize;
    if n != expect {
        return Err(bad(&format!("has {n} values, column expects {expect}")));
    }
    if block_len == 0 || block_len > 1 << 20 {
        return Err(bad(&format!("implausible block length {block_len}")));
    }
    let mut out = Vec::with_capacity(n);
    let mut at = 8usize;
    while out.len() < n {
        let len = (n - out.len()).min(block_len);
        let min = i64::from_le_bytes(take(at, 8)?.try_into().unwrap());
        let width = *take(at + 8, 1)?.first().unwrap() as usize;
        at += 9;
        if width > 64 {
            return Err(bad(&format!("impossible bit width {width}")));
        }
        if width == 0 {
            out.resize(out.len() + len, min);
            continue;
        }
        let payload = len * width;
        let payload_bytes = payload.div_ceil(8);
        let packed = take(at, payload_bytes)?;
        at += payload_bytes;
        let mut acc: u128 = 0;
        let mut bits = 0usize;
        let mut byte_at = 0usize;
        let mask: u128 = if width == 64 { u128::MAX >> 64 } else { (1u128 << width) - 1 };
        for _ in 0..len {
            while bits < width {
                acc |= (packed[byte_at] as u128) << bits;
                byte_at += 1;
                bits += 8;
            }
            out.push(min.wrapping_add((acc & mask) as u64 as i64));
            acc >>= width;
            bits -= width;
        }
    }
    if at > bytes.len() {
        return Err(bad("truncated"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(vals: &[i64]) {
        let enc = encode_i64(vals);
        let dec = decode_i64(&enc, vals.len()).unwrap();
        assert_eq!(dec, vals);
    }

    #[test]
    fn roundtrips_the_shapes_the_store_contains() {
        roundtrip(&[]);
        roundtrip(&[42]);
        roundtrip(&(0..10_000i64).collect::<Vec<_>>()); // sequential refs
        roundtrip(&vec![7; 10_000]); // constant column
        roundtrip(&(0..10_000i64).map(|i| 1_600_000_000_000_000 + i * 977).collect::<Vec<_>>());
        // adversarial: full-range values must survive (and stay raw in
        // practice, but correctness cannot depend on the writer's choice)
        roundtrip(&[i64::MIN, i64::MAX, 0, -1, 1]);
        let mut x: u64 = 0x9E3779B97F4A7C15;
        let noise: Vec<i64> = (0..5000)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as i64
            })
            .collect();
        roundtrip(&noise);
    }

    #[test]
    fn constant_blocks_cost_nothing_but_headers() {
        let enc = encode_i64(&vec![5; 100_000]);
        // 25 blocks x 9 bytes + 8 stream header
        assert!(enc.len() < 300, "constant column encoded to {} bytes", enc.len());
    }

    #[test]
    fn sorted_timestamps_compress_hard() {
        let vals: Vec<i64> = (0..100_000).map(|i| 1_600_000_000_000_000 + i).collect();
        let enc = encode_i64(&vals);
        assert!(enc.len() < vals.len() * 3, "sorted i64 encoded to {} bytes", enc.len());
    }

    #[test]
    fn corrupt_streams_error_rather_than_panic() {
        let enc = encode_i64(&(0..10_000i64).collect::<Vec<_>>());
        assert!(decode_i64(&enc, 9_999).is_err(), "wrong expected count");
        assert!(decode_i64(&enc[..enc.len() - 3], 10_000).is_err(), "truncation");
        assert!(decode_i64(&enc[..6], 10_000).is_err(), "no room for a block");
        let mut evil = enc.clone();
        evil[16] = 99; // width byte (stream header 8 + block min 8) > 64
        assert!(decode_i64(&evil, 10_000).is_err(), "impossible width");
    }
}

#[cfg(test)]
mod scale_tests {
    use super::*;

    /// Round-trip through the scale must be the identity on every value, or
    /// the divisor is not exact and has no business being offered.
    fn roundtrip(vals: &[i64]) -> Option<(i64, i64)> {
        let scale = exact_scale(vals);
        if let Some((base, divisor)) = scale {
            assert!(divisor >= 2, "a divisor of {divisor} is not worth recording");
            let mut back = scale_down(vals, base, divisor);
            scale_up(&mut back, base, divisor).unwrap();
            assert_eq!(back, vals, "scale {base}/{divisor} is not exact");
        }
        scale
    }

    #[test]
    fn the_shapes_the_store_contains() {
        // The case the change is for: Unix seconds lifted into microseconds,
        // with the *irregular* gaps real event data has. Regular gaps would
        // give a coarser divisor and prove less — the claim is that a
        // seconds-resolution clock leaves exactly 1e6 behind however the
        // events are spaced.
        let mut t = 1_600_000_000_000_000i64;
        let secs: Vec<i64> = (0..10_000)
            .map(|i: i64| {
                let v = t;
                t += (1 + (i * 7919) % 4001) * 1_000_000;
                v
            })
            .collect();
        assert_eq!(roundtrip(&secs), Some((1_600_000_000_000_000, 1_000_000)));

        // the synthetic tick, and the coarser divisor a compaction leaves
        let tick: Vec<i64> = (0..5_000).map(|i| 1_000 + i * 31_449_600).collect();
        assert_eq!(roundtrip(&tick), Some((1_000, 31_449_600)));

        // a column with no common factor bails, which is the common case
        assert_eq!(roundtrip(&(0..10_000i64).collect::<Vec<_>>()), None);

        // degenerate inputs have no first differences to share a factor
        assert_eq!(roundtrip(&[]), None);
        assert_eq!(roundtrip(&[42]), None);
        assert_eq!(roundtrip(&vec![7; 10_000]), None);
    }

    #[test]
    fn adversarial_columns_are_exact_or_declined() {
        // all-negative, on a grid
        let neg: Vec<i64> = (0..1_000).map(|i| -5_000_000_000 + i * 250_000).collect();
        assert_eq!(roundtrip(&neg), Some((-5_000_000_000, 250_000)));

        // straddling zero
        let across: Vec<i64> = (-500..500).map(|i| i * 1_000_000).collect();
        assert_eq!(roundtrip(&across), Some((-500_000_000, 1_000_000)));

        // descending, on a grid -- nothing here assumes the column ascends
        let down: Vec<i64> = (0..1_000).map(|i| 9_000_000 - i * 3_000).collect();
        assert_eq!(roundtrip(&down), Some((9_000_000, 3_000)));

        // i64 extremes: the differences overflow i64, so the gcd is taken in
        // i128 and the quotients are range-checked before the scale is
        // offered. Either answer is acceptable; a wrong value is not.
        roundtrip(&[i64::MIN, i64::MAX]);
        roundtrip(&[i64::MIN, 0, i64::MAX]);
        roundtrip(&[i64::MIN, i64::MIN + 2, i64::MIN + 4]);
        roundtrip(&[i64::MAX - 4, i64::MAX - 2, i64::MAX]);

        // a single huge step -- the divisor is the step itself, and the
        // quotient must still fit
        roundtrip(&[0, i64::MAX]);
        roundtrip(&[-1, i64::MAX]);

        // random noise, which must never produce a divisor
        let mut x: u64 = 0x9E3779B97F4A7C15;
        let noise: Vec<i64> = (0..5_000)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as i64
            })
            .collect();
        roundtrip(&noise);
    }

    /// A scaled column really is smaller under the codec that ships, and by
    /// exactly the divisor's width: `log2(1e6) = 19.93` bits per row.
    ///
    /// Stated as a *difference* in bits/row rather than as a ratio, because
    /// the difference is the mechanism and the ratio is an artefact of how
    /// wide the column happened to be. Frame-of-reference packs
    /// `v - block_min` at `log2(range)` bits and the low `log2(g)` of those
    /// are always zero; taking them out is worth those bits and no others.
    #[test]
    fn dividing_the_scale_out_shrinks_the_stream() {
        let mut t = 1_600_000_000_000_000i64;
        let vals: Vec<i64> = (0..100_000)
            .map(|i: i64| {
                let v = t;
                t += (1 + (i * 7919) % 4001) * 1_000_000;
                v
            })
            .collect();
        let plain = encode_i64(&vals);
        let (base, divisor) = exact_scale(&vals).unwrap();
        let scaled = encode_i64(&scale_down(&vals, base, divisor));
        let bits = |e: &Vec<u8>| e.len() as f64 * 8.0 / vals.len() as f64;
        let saved = bits(&plain) - bits(&scaled);
        assert!(
            (19.0..=21.0).contains(&saved),
            "scaled {:.1} b/r against plain {:.1} b/r — saved {saved:.2}, expected              the divisor's own width (log2(1e6) = 19.93)",
            bits(&scaled),
            bits(&plain)
        );
    }

    /// The three numbers come off disk. A corrupt header must produce an
    /// error, not a plausible wrong timestamp.
    #[test]
    fn a_corrupt_scale_errors_rather_than_lying() {
        let mut v = vec![1i64, 2, 3];
        assert!(scale_up(&mut v, 0, 0).is_err(), "divisor 0");
        assert!(scale_up(&mut v, 0, -5).is_err(), "negative divisor");
        assert_eq!(v, vec![1, 2, 3], "a rejected scale must not have half-applied");

        let mut big = vec![i64::MAX / 2];
        assert!(scale_up(&mut big, 0, 1_000_000).is_err(), "overflowing product");
        let mut edge = vec![i64::MAX];
        assert!(scale_up(&mut edge, 1, 1).is_err(), "overflowing sum");

        // the identity is free and exact
        let mut id = vec![i64::MIN, -1, 0, i64::MAX];
        scale_up(&mut id, 0, 1).unwrap();
        assert_eq!(id, vec![i64::MIN, -1, 0, i64::MAX]);
    }
}

// ------------------------------------------------------------------- //
// string heap (codec: STRINGS_PACKED)                                 //
// ------------------------------------------------------------------- //

/// Sentinel in the heap's first four bytes. A raw heap starts with its
/// string count; `u32::MAX` there is impossible (refs are u32 with MAX
/// reserved as NULL), so an old build reading a packed heap fails its
/// bounds checks instead of misreading text.
pub const HEAP_MAGIC: u32 = u32::MAX;

/// Pack a raw string heap (`count | offsets (n+1) x u32 | payload`).
///
/// The two halves compress differently and are packed separately: offsets
/// are monotone (their deltas are string lengths) so the FOR codec takes
/// them to well under a byte each, while the payload is text and goes
/// through DEFLATE. Measured on a real heap the split beats one DEFLATE
/// stream over everything by 1.4x.
///
/// Layout: u32 HEAP_MAGIC | u32 n | u32 for_len | u32 deflate_len
///         | FOR(offsets) | DEFLATE(payload)
pub fn pack_heap(raw: &[u8]) -> Option<Vec<u8>> {
    if raw.len() < 8 {
        return None;
    }
    let n = u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
    let off_end = 4 + 4 * (n + 1);
    // a count equal to the magic could not be told apart when reading
    if n == u32::MAX as usize || raw.len() < off_end {
        return None;
    }
    let offsets: Vec<i64> = raw[4..off_end]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| u32::from_le_bytes(*c) as i64)
        .collect();
    let packed_offsets = encode_i64(&offsets);
    let deflated = miniz_oxide::deflate::compress_to_vec(&raw[off_end..], 6);
    let mut out = Vec::with_capacity(16 + packed_offsets.len() + deflated.len());
    out.extend_from_slice(&HEAP_MAGIC.to_le_bytes());
    out.extend_from_slice(&(n as u32).to_le_bytes());
    out.extend_from_slice(&(packed_offsets.len() as u32).to_le_bytes());
    out.extend_from_slice(&(deflated.len() as u32).to_le_bytes());
    out.extend_from_slice(&packed_offsets);
    out.extend_from_slice(&deflated);
    Some(out)
}

/// Is this heap packed? Raw heaps keep their count in the first word.
pub fn heap_is_packed(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && u32::from_le_bytes(bytes[..4].try_into().unwrap()) == HEAP_MAGIC
}

/// Reconstruct the raw heap layout from a packed one.
pub fn unpack_heap(bytes: &[u8]) -> Result<Vec<u8>> {
    let bad = |what: &str| EngineError::corrupt(format!("packed string heap: {what}"));
    if bytes.len() < 16 || !heap_is_packed(bytes) {
        return Err(bad("missing magic"));
    }
    let n = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let for_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let deflate_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let for_end = 16usize.checked_add(for_len).ok_or_else(|| bad("truncated"))?;
    let total = for_end.checked_add(deflate_len).ok_or_else(|| bad("truncated"))?;
    if bytes.len() < total {
        return Err(bad("truncated"));
    }
    let offsets = decode_i64(&bytes[16..for_end], n + 1)?;
    let payload_len = offsets.last().copied().unwrap_or(0);
    if !(0..=(1i64 << 32)).contains(&payload_len) {
        return Err(bad("implausible payload length"));
    }
    let payload = miniz_oxide::inflate::decompress_to_vec_with_limit(
        &bytes[for_end..total],
        payload_len as usize,
    )
    .map_err(|e| bad(&format!("inflate failed: {e}")))?;
    if payload.len() != payload_len as usize {
        return Err(bad("payload length disagrees with offsets"));
    }
    let mut raw = Vec::with_capacity(4 + 4 * (n + 1) + payload.len());
    raw.extend_from_slice(&(n as u32).to_le_bytes());
    for &o in &offsets {
        if o < 0 || o > payload_len {
            return Err(bad("offset out of range"));
        }
        raw.extend_from_slice(&(o as u32).to_le_bytes());
    }
    raw.extend_from_slice(&payload);
    Ok(raw)
}

#[cfg(test)]
mod heap_tests {
    use super::*;

    fn raw_heap(strings: &[&str]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(strings.len() as u32).to_le_bytes());
        let mut cursor = 0u32;
        for s in strings {
            out.extend_from_slice(&cursor.to_le_bytes());
            cursor += s.len() as u32;
        }
        out.extend_from_slice(&cursor.to_le_bytes());
        for s in strings {
            out.extend_from_slice(s.as_bytes());
        }
        out
    }

    #[test]
    fn packed_heaps_roundtrip() {
        for strings in [
            vec![],
            vec![""],
            vec!["{}", "ingest", "élève"],
            (0..5000).map(|i| format!("#{i}")).collect::<Vec<_>>()
                .iter().map(String::as_str).collect(),
        ] {
            let raw = raw_heap(&strings);
            let packed = pack_heap(&raw).unwrap();
            assert!(heap_is_packed(&packed));
            assert_eq!(unpack_heap(&packed).unwrap(), raw, "{strings:?}");
        }
    }

    #[test]
    fn disc_shaped_heaps_shrink_hard() {
        let strings: Vec<String> = (0..50_000).map(|i| format!("#{i}")).collect();
        let refs: Vec<&str> = strings.iter().map(String::as_str).collect();
        let raw = raw_heap(&refs);
        let packed = pack_heap(&raw).unwrap();
        // 2.4x measured on this worst case (every string distinct); real
        // heaps with repeated props/source strings do better
        assert!(
            packed.len() * 2 < raw.len(),
            "packed {} vs raw {}", packed.len(), raw.len()
        );
    }

    #[test]
    fn corrupt_packed_heaps_error_rather_than_panic() {
        let strings: Vec<String> = (0..1000).map(|i| format!("#{i}")).collect();
        let refs: Vec<&str> = strings.iter().map(String::as_str).collect();
        let packed = pack_heap(&raw_heap(&refs)).unwrap();
        assert!(unpack_heap(&packed[..packed.len() - 2]).is_err(), "truncation");
        assert!(unpack_heap(&packed[..12]).is_err(), "header truncation");
        let mut evil = packed.clone();
        evil[4] = 0xFF; evil[5] = 0xFF; // absurd count
        assert!(unpack_heap(&evil).is_err(), "count mismatch vs offsets");
    }
}

/// The cheaper scale arithmetic and the size-only length are held to
/// the forms they replace, over the shapes the store holds and adversarial
/// extremes.
#[cfg(test)]
mod w22_tests {
    use super::*;

    /// `exact_scale` as it was originally (u128 GCD, per-value range check).
    fn exact_scale_reference(vals: &[i64]) -> Option<(i64, i64)> {
        let &base = vals.first()?;
        let mut g: u128 = 0;
        for w in vals.windows(2) {
            let d = (w[1] as i128) - (w[0] as i128);
            g = gcd_u128(g, d.unsigned_abs());
            if g == 1 {
                return None;
            }
        }
        if g == 0 || g > i64::MAX as u128 {
            return None;
        }
        let divisor = g as i64;
        let base128 = base as i128;
        let g128 = g as i128;
        for &v in vals {
            let q = (v as i128 - base128) / g128;
            if q < i64::MIN as i128 || q > i64::MAX as i128 {
                return None;
            }
        }
        Some((base, divisor))
    }

    fn scale_down_reference(vals: &[i64], base: i64, divisor: i64) -> Vec<i64> {
        let (b, d) = (base as i128, divisor as i128);
        vals.iter().map(|&v| ((v as i128 - b) / d) as i64).collect()
    }

    /// splitmix64, so the columns are a function of the seed
    fn rng(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub(crate) fn columns() -> Vec<Vec<i64>> {
        let mut out: Vec<Vec<i64>> = vec![
            vec![],
            vec![7],
            vec![5; 100],
            vec![i64::MIN, i64::MAX],
            vec![i64::MAX, i64::MIN, 0],
            vec![i64::MIN, 0, i64::MIN],
            vec![i64::MIN + 2, i64::MAX - 1, 4],
            vec![-6, 0, 6, i64::MAX - (i64::MAX % 6) - 6],
            vec![i64::MIN, i64::MIN + 4, i64::MIN + 8],
            vec![i64::MAX, i64::MAX - 4, i64::MIN + 3],
        ];
        let mut st = 42u64;
        for k in 0..400u64 {
            let n = 1 + (rng(&mut st) % 9000) as usize;
            let div = [1i64, 2, 3, 200, 1000, 1_000_000, 7 * 86_400_000_000][(k % 7) as usize];
            let base = (rng(&mut st) as i64) >> (k % 40);
            let col: Vec<i64> = match k % 6 {
                // rising timestamps on a grid
                0 => (0..n as i64).map(|i| base.wrapping_add(i * div * (1 + (i % 3)))).collect(),
                // grid values, unsorted
                1 => (0..n).map(|_| base.wrapping_add(div * ((rng(&mut st) % 100_000) as i64))).collect(),
                // grid with one stray value
                2 => (0..n as i64)
                    .map(|i| if i == n as i64 / 2 { base + 1 } else { base.wrapping_add(i * div) })
                    .collect(),
                // ids
                3 => (0..n).map(|_| (rng(&mut st) % 50_000) as i64).collect(),
                // hashes: no structure
                4 => (0..n).map(|_| rng(&mut st) as i64).collect(),
                // near the ends of the range
                _ => (0..n)
                    .map(|_| if rng(&mut st).is_multiple_of(2) { i64::MAX - (rng(&mut st) % 1000) as i64 * div } else { i64::MIN + (rng(&mut st) % 1000) as i64 * div })
                    .collect(),
            };
            out.push(col);
        }
        out
    }

    #[test]
    fn exact_scale_and_scale_down_match_the_forms_they_replace() {
        let mut divided = 0;
        for col in columns() {
            let got = exact_scale(&col);
            assert_eq!(got, exact_scale_reference(&col), "{:?}", &col[..col.len().min(8)]);
            if let Some((b, d)) = got {
                divided += 1;
                assert_eq!(scale_down(&col, b, d), scale_down_reference(&col, b, d));
            }
        }
        assert!(divided > 50, "the fixture must exercise the scaled path ({divided})");
    }

    #[test]
    fn the_size_only_length_is_the_encoded_length() {
        for col in columns() {
            assert_eq!(encoded_len_i64(&col), encode_i64(&col).len());
        }
    }
}
