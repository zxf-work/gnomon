//! Codec 2 — per-block linear frame-of-reference (`linFOR`).
//!
//! # What it is, and what it is not
//!
//! Frame-of-reference (codec 1) stores, per block, the minimum and the bit
//! width of `max - min`, then packs each value's delta from the minimum.
//! `linFOR` replaces the constant with a line: per block, an intercept and a
//! fixed-point slope, then packs each value's residual against that line.
//! On a column that rises steadily — which `vt_s` does, because `staging.rs`
//! cluster-orders by `(vt_s, vid)` — the line removes the block's whole span
//! from the residual and leaves only the jitter.
//!
//! **None of this is novel and the engine claims nothing.** A per-partition
//! linear model with bit-packed residuals over the row index, lossless, with
//! random access, is `la_vector` (Boffa et al., 2022); its instantiation as a columnar codec, on an
//! almost-sorted seconds-resolution timestamp column, with a range predicate
//! pushed into the partition, is `LeCo` (Liu et al., 2024), which also states that frame-of-reference, run-length and
//! delta coding are special cases of the same framework. `NeaTS` (Guerra et
//! al., 2025) generalises it to nonlinear families. Prometheus 1.x
//! shipped random-access-plus-linear-model chunks in 2016 and withdrew them
//! in 2.x on space. Those four are the comparators this codec should be
//! benchmarked against, and `la_vector` and `LeCo` are public code.
//!
//! What is deliberately *not* built here, on the design's measurement:
//! a whole-column piecewise-linear approximation (worse than a plain
//! per-block fit on three of four real segments, and 2.6–5.1× on point
//! lookup), a per-segment residual width (worse on eleven of fourteen
//! measured columns), and a quadratic or spline fit (worth at most 0.3
//! bits/row before paying for its extra coefficient).
//!
//! # Determinism
//!
//! Every stored quantity is an integer function of the block's values in
//! stored order. The slope is the endpoint line — `(last - first) /
//! (rows - 1)` — taken in fixed point at a shift chosen by an integer rule,
//! and the intercept is then moved to the minimum residual so the residuals
//! are unsigned and as narrow as they can be. No floating point is computed,
//! let alone stored, so two encodes of the same column on two hosts produce
//! the same bytes.
//!
//! The fit is fixed as well as the format. Two different fits of one column
//! produce different bytes even when both are valid, so the *procedure* is
//! part of the format here, not an encoder's private choice.
//!
//! # Layout (little-endian)
//!
//! ```text
//! u32 n | u32 block_rows | u8 flags | i64 base | i64 divisor
//! u64 block_offset[nblocks]        byte offset of each block from blocks_start
//! i64 block_first_value[nblocks]   the column's own value at each block start
//! blocks...
//! block: i64 intercept | i64 slope_num | u8 shift | u8 width | packed residuals
//! ```
//!
//! `(base, divisor)` is the exact scale (see [`crate::codec::exact_scale`]);
//! the stream holds `(v - base) / divisor` and the model is fitted to that,
//! which is where the divisor's ~20 bits/row on real data come from. The
//! codec carries its own scale rather than borrowing `ColumnDesc`'s so the
//! per-block fit sees divided values — a line through undivided microseconds
//! would have to carry the divisor in its residuals.
//!
//! The two parallel vectors are what the block header cannot give. Residual
//! widths vary per block, so a block's start is not computable from its
//! index — hence `block_offset`, 64 bits per block, 0.06 bits/row at 1024.
//! `block_first_value` is the same cost again and buys the range path: it is
//! a sparse index over the column that a range filter binary-searches
//! without touching a residual.
//!
//! Row `i` is read with a shift for its block index, one offset load, one
//! multiply-shift-add and one bit extract. There is no search anywhere on
//! the point path — which is the property a whole-column PLA gives up.

use crate::error::{EngineError, Result};

/// Rows per model block.
///
/// 1024, the design's measured configuration: at 1024 the per-block linear fit
/// gives 11.7–16.0 bits/row for `vt_s` on scale-divided real data, against
/// 17.0–21.1 for scaled frame-of-reference and 36.9–43.3 for the codec in
/// the store today. 4096 is slightly worse on every real column measured
/// (13.6–17.5) because a longer block is a worse fit for a line, and the
/// per-block overhead it saves is already under 0.3 bits/row.
pub const BLOCK_ROWS: usize = 1024;

const PREAMBLE: usize = 4 + 4 + 1 + 8 + 8;
const BLOCK_HEADER: usize = 8 + 8 + 1 + 1;

/// Bit 0: the column is non-decreasing, so the block-first-value vector is
/// sorted and the range path is valid. Nothing else is defined; the
/// remaining bits are written zero and ignored on read.
const FLAG_SORTED: u8 = 1;

/// Arithmetic shift right — floor division by `2^sh`, the same on every
/// platform Rust targets, and the only rounding this codec performs.
#[inline]
fn arsh(v: i64, sh: u8) -> i64 {
    v >> sh
}

/// The fixed-point shift for a block, by an integer rule.
///
/// The largest `sh` in `0..=31` for which `|dx| << sh` still fits an `i64`.
/// That bound is what makes the whole read path `i64`: `slope_num` is at
/// most `(|dx| << sh) / (m - 1)` in magnitude and it is only ever multiplied
/// by a `d` below `m`, so `slope_num * d` cannot exceed `|dx| << sh` and
/// therefore cannot overflow. No `i128` on the point path.
fn shift_for(dx: i128) -> u8 {
    let a = dx.unsigned_abs();
    if a == 0 {
        return 0;
    }
    let mut sh = 0u8;
    while sh < 31 && (a << (sh + 1)) <= i64::MAX as u128 {
        sh += 1;
    }
    sh
}

/// One block's fitted model and residuals.
struct Block {
    intercept: i64,
    slope_num: i64,
    shift: u8,
    width: u8,
    residuals: Vec<u64>,
}

/// Fit one block. `None` means this column cannot be linFOR-coded — the
/// residuals or the intercept left the `i64` range — and the caller falls
/// back to a codec that can hold it.
fn fit(x: &[i64]) -> Option<Block> {
    let m = x.len();
    debug_assert!(m > 0);
    let first = x[0];
    if m == 1 {
        return Some(Block {
            intercept: first,
            slope_num: 0,
            shift: 0,
            width: 0,
            residuals: Vec::new(),
        });
    }
    // endpoint fit, in fixed point
    let dx = (x[m - 1] as i128) - (first as i128);
    let shift = shift_for(dx);
    let den = (m - 1) as i128;
    let slope_num = i64::try_from((dx << shift) / den).ok()?;

    // residuals against the line anchored at the block's first value
    let f = first as i128;
    let mut residuals_i: Vec<i128> = Vec::with_capacity(m);
    let (mut lo, mut hi) = (i128::MAX, i128::MIN);
    for (d, &v) in x.iter().enumerate() {
        let pred = f + arsh(slope_num.wrapping_mul(d as i64), shift) as i128;
        let r = (v as i128) - pred;
        lo = lo.min(r);
        hi = hi.max(r);
        residuals_i.push(r);
    }
    // move the intercept to the residual minimum: the residuals become
    // unsigned and as narrow as this line allows
    let span = (hi - lo).unsigned_abs();
    let width = (128 - span.leading_zeros()) as u8;
    if width > 64 {
        return None;
    }
    let intercept = i64::try_from(f + lo).ok()?;
    Some(Block {
        intercept,
        slope_num,
        shift,
        width,
        residuals: residuals_i.into_iter().map(|r| (r - lo) as u64).collect(),
    })
}

/// Bytes one fitted block occupies.
fn block_len(m: usize, width: u8) -> usize {
    BLOCK_HEADER + (m * width as usize).div_ceil(8)
}

/// Encode `vals` as codec 2, or `None` when this codec cannot hold them
/// exactly. The caller trial-encodes and keeps the smallest arm, so `None`
/// is never a failure — it is one arm declining.
///
/// `scale` asks for the exact divisor to be taken out before the line is
/// fitted. **The two are not freely composable and the caller must try
/// both.** On one 10M φ=0.0 segment the divisor makes the model *worse* —
/// 0.01 bits/row undivided against 2.00 divided — because dividing changes
/// the grid the fixed-point slope has to land on, and a line that was exact
/// on the original values is no longer exact on the quotients. Dividing is
/// still exact; it is the *fit* that gets worse. Whichever is smaller wins,
/// per column, by measurement.
pub fn encode(vals: &[i64], scale: bool) -> Option<Vec<u8>> {
    if vals.is_empty() {
        return None;
    }
    let (base, divisor) = if scale {
        crate::codec::exact_scale(vals).unwrap_or((0, 1))
    } else {
        (0, 1)
    };
    if divisor == 1 && base == 0 {
        encode_prepared(vals, 0, 1, vals)
    } else {
        encode_prepared(vals, base, divisor, &crate::codec::scale_down(vals, base, divisor))
    }
}

/// [`encode`] with the scale already applied by the caller: `x` is `vals`
/// itself for `(base, divisor) = (0, 1)`, else `scale_down(vals, base,
/// divisor)`. The trial encode computes the scale once per column and hands
/// it to both arms that use it.
pub(crate) fn encode_prepared(vals: &[i64], base: i64, divisor: i64, x: &[i64]) -> Option<Vec<u8>> {
    if vals.is_empty() {
        return None;
    }
    let blocks: Vec<Block> = x
        .chunks(BLOCK_ROWS)
        .map(fit)
        .collect::<Option<Vec<_>>>()?;
    let nb = blocks.len();

    let mut out = Vec::with_capacity(PREAMBLE + nb * 16 + vals.len());
    out.extend_from_slice(&(vals.len() as u32).to_le_bytes());
    out.extend_from_slice(&(BLOCK_ROWS as u32).to_le_bytes());
    out.push(if vals.windows(2).all(|w| w[0] <= w[1]) { FLAG_SORTED } else { 0 });
    out.extend_from_slice(&base.to_le_bytes());
    out.extend_from_slice(&divisor.to_le_bytes());

    let mut off = 0u64;
    for (b, blk) in blocks.iter().enumerate() {
        out.extend_from_slice(&off.to_le_bytes());
        let m = x[b * BLOCK_ROWS..].len().min(BLOCK_ROWS);
        off += block_len(m, blk.width) as u64;
    }
    for b in 0..nb {
        out.extend_from_slice(&vals[b * BLOCK_ROWS].to_le_bytes());
    }
    for blk in &blocks {
        out.extend_from_slice(&blk.intercept.to_le_bytes());
        out.extend_from_slice(&blk.slope_num.to_le_bytes());
        out.push(blk.shift);
        out.push(blk.width);
        if blk.width == 0 {
            continue; // an exact fit costs nothing but its header
        }
        let mut acc: u128 = 0;
        let mut bits = 0usize;
        for &r in &blk.residuals {
            acc |= (r as u128) << bits;
            bits += blk.width as usize;
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
    Some(out)
}

/// `encode_prepared(vals, _, _, x).map(|b| b.len())` without fitting
/// residual vectors or packing a bit: the per-block width is all the length
/// depends on. `None` exactly when `encode_prepared` declines.
pub(crate) fn encoded_len_prepared(vals: &[i64], x: &[i64]) -> Option<usize> {
    if vals.is_empty() {
        return None;
    }
    let mut n = PREAMBLE;
    for block in x.chunks(BLOCK_ROWS) {
        n += 16 + block_len(block.len(), fit_width(block)?);
    }
    Some(n)
}

/// [`fit`]'s width, by the same arithmetic and the same refusals, keeping
/// only the residual range.
fn fit_width(x: &[i64]) -> Option<u8> {
    let m = x.len();
    debug_assert!(m > 0);
    if m == 1 {
        return Some(0);
    }
    let first = x[0];
    let dx = (x[m - 1] as i128) - (first as i128);
    let shift = shift_for(dx);
    let den = (m - 1) as i128;
    let slope_num = i64::try_from((dx << shift) / den).ok()?;
    let f = first as i128;
    let (mut lo, mut hi) = (i128::MAX, i128::MIN);
    for (d, &v) in x.iter().enumerate() {
        let pred = f + arsh(slope_num.wrapping_mul(d as i64), shift) as i128;
        let r = (v as i128) - pred;
        lo = lo.min(r);
        hi = hi.max(r);
    }
    let span = (hi - lo).unsigned_abs();
    let width = (128 - span.leading_zeros()) as u8;
    if width > 64 {
        return None;
    }
    i64::try_from(f + lo).ok()?;
    Some(width)
}

/// A parsed codec-2 stream: random access, and a range path that never
/// materialises the column.
///
/// Every field was bounds-checked by [`LinColumn::parse`], so the accessors
/// below index rather than re-check — and `parse` runs on every open, so a
/// corrupt extent is an error there rather than a panic here.
pub struct LinColumn<'a> {
    bytes: &'a [u8],
    n: usize,
    block_rows: usize,
    flags: u8,
    base: i64,
    divisor: i64,
    offsets_at: usize,
    firsts_at: usize,
    blocks_at: usize,
    nblocks: usize,
}

impl<'a> LinColumn<'a> {
    pub fn parse(bytes: &'a [u8], expect: usize) -> Result<Self> {
        let bad = |what: String| EngineError::corrupt(format!("linFOR stream: {what}"));
        let at = |o: usize, n: usize| -> Result<&[u8]> {
            bytes.get(o..o + n).ok_or_else(|| bad("truncated".into()))
        };
        let n = u32::from_le_bytes(at(0, 4)?.try_into().unwrap()) as usize;
        let block_rows = u32::from_le_bytes(at(4, 4)?.try_into().unwrap()) as usize;
        if n != expect {
            return Err(bad(format!("has {n} values, column expects {expect}")));
        }
        if block_rows == 0 || block_rows > 1 << 20 {
            return Err(bad(format!("implausible block length {block_rows}")));
        }
        let flags = at(8, 1)?[0];
        let base = i64::from_le_bytes(at(9, 8)?.try_into().unwrap());
        let divisor = i64::from_le_bytes(at(17, 8)?.try_into().unwrap());
        if divisor < 1 {
            return Err(bad(format!("scale divisor {divisor} is not a positive integer")));
        }
        let nblocks = n.div_ceil(block_rows);
        let offsets_at = PREAMBLE;
        let firsts_at = offsets_at + nblocks * 8;
        let blocks_at = firsts_at + nblocks * 8;
        if bytes.len() < blocks_at {
            return Err(bad("truncated before the blocks".into()));
        }
        let c = Self {
            bytes, n, block_rows, flags, base, divisor,
            offsets_at, firsts_at, blocks_at, nblocks,
        };
        // Validate every block header and payload extent once, here, so the
        // accessors can index. Decode is itself a structural validator, and
        // stays one when checksum verification is skipped.
        for b in 0..nblocks {
            let m = c.block_len_rows(b);
            let start = c.block_start(b)?;
            let width = *c
                .bytes
                .get(start + 17)
                .ok_or_else(|| bad(format!("block {b} header is truncated")))?;
            if width > 64 {
                return Err(bad(format!("block {b} has impossible bit width {width}")));
            }
            let end = start + block_len(m, width);
            if end > bytes.len() {
                return Err(bad(format!("block {b} extends past the extent")));
            }
        }
        Ok(c)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.n
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// The exact scale this stream carries: the model was fitted to
    /// `(v - base) / divisor`.
    #[inline]
    pub fn scale(&self) -> (i64, i64) {
        (self.base, self.divisor)
    }

    /// How many model blocks the column is cut into — also the length of
    /// the sparse index the range path searches.
    #[inline]
    pub fn blocks(&self) -> usize {
        self.nblocks
    }

    /// Is the column non-decreasing? The range path needs this and says so
    /// rather than assuming it.
    #[inline]
    pub fn is_sorted(&self) -> bool {
        self.flags & FLAG_SORTED != 0
    }

    #[inline]
    fn block_len_rows(&self, b: usize) -> usize {
        (self.n - b * self.block_rows).min(self.block_rows)
    }

    fn block_start(&self, b: usize) -> Result<usize> {
        let o = self
            .bytes
            .get(self.offsets_at + b * 8..self.offsets_at + b * 8 + 8)
            .ok_or_else(|| EngineError::corrupt("linFOR stream: offset vector is truncated"))?;
        let off = u64::from_le_bytes(o.try_into().unwrap()) as usize;
        self.blocks_at
            .checked_add(off)
            .ok_or_else(|| EngineError::corrupt("linFOR stream: block offset overflows"))
    }

    /// The column's own value at the start of block `b` — the sparse index.
    #[inline]
    pub fn block_first(&self, b: usize) -> i64 {
        let o = self.firsts_at + b * 8;
        i64::from_le_bytes(self.bytes[o..o + 8].try_into().unwrap())
    }

    /// Row `i`. O(1): a shift for the block index, one offset load, one
    /// multiply-shift-add, one bit extract. No search.
    #[inline]
    pub fn get(&self, i: usize) -> i64 {
        debug_assert!(i < self.n);
        let b = i / self.block_rows;
        let d = i - b * self.block_rows;
        let off = u64::from_le_bytes(
            self.bytes[self.offsets_at + b * 8..self.offsets_at + b * 8 + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let s = self.blocks_at + off;
        let intercept = i64::from_le_bytes(self.bytes[s..s + 8].try_into().unwrap());
        let slope_num = i64::from_le_bytes(self.bytes[s + 8..s + 16].try_into().unwrap());
        let shift = self.bytes[s + 16];
        let width = self.bytes[s + 17] as usize;
        let res = if width == 0 {
            0u64
        } else {
            read_bits(&self.bytes[s + BLOCK_HEADER..], d * width, width)
        };
        // Wrapping throughout: the encoder proved the true value fits an
        // i64, and addition modulo 2^64 gets there whatever the
        // intermediates do.
        let x = intercept
            .wrapping_add(arsh(slope_num.wrapping_mul(d as i64), shift))
            .wrapping_add(res as i64);
        x.wrapping_mul(self.divisor).wrapping_add(self.base)
    }

    /// Materialise the whole column. Used at open when something is going to
    /// read it as a slice; the range path below never calls it.
    pub fn decode(&self) -> Vec<i64> {
        let mut out = Vec::with_capacity(self.n);
        for b in 0..self.nblocks {
            let m = self.block_len_rows(b);
            let off = u64::from_le_bytes(
                self.bytes[self.offsets_at + b * 8..self.offsets_at + b * 8 + 8]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let s = self.blocks_at + off;
            let intercept = i64::from_le_bytes(self.bytes[s..s + 8].try_into().unwrap());
            let slope_num = i64::from_le_bytes(self.bytes[s + 8..s + 16].try_into().unwrap());
            let shift = self.bytes[s + 16];
            let width = self.bytes[s + 17] as usize;
            let payload = &self.bytes[s + BLOCK_HEADER..];
            // the model is hoisted out of the row loop: one multiply-shift-
            // add per row, and the block header is read once per 1024 rows
            for d in 0..m {
                let res = if width == 0 {
                    0u64
                } else {
                    read_bits(payload, d * width, width)
                };
                let x = intercept
                    .wrapping_add(arsh(slope_num.wrapping_mul(d as i64), shift))
                    .wrapping_add(res as i64);
                out.push(x.wrapping_mul(self.divisor).wrapping_add(self.base));
            }
        }
        out
    }

    /// How many leading values are `< v`, without materialising the column.
    ///
    /// Two levels. The block-first-value vector is a dense, cache-resident
    /// `i64` array — `nblocks` is the column's rows over 1024 — so the outer
    /// binary search makes `log2(n/1024)` probes into it and touches no
    /// residual at all. Inside the located block the model is inverted to
    /// bound the answer before any residual is read: residuals are unsigned,
    /// so `intercept + slope·d/2^sh` is a lower bound on the value and
    /// `+ (2^width - 1)` is an upper one, and the two bounds cross within a
    /// few rows whenever the line is a good fit — which is the case the
    /// codec is chosen in. The bounded interval is then searched, so a poor
    /// fit costs a within-block binary search rather than a wrong answer.
    ///
    /// Requires [`LinColumn::is_sorted`]; the caller checks.
    pub fn partition_point_lt(&self, v: i64) -> usize {
        debug_assert!(self.is_sorted());
        if self.n == 0 {
            return 0;
        }
        // last block whose first value is < v; every earlier block is
        // wholly below v, every later one wholly at or above it
        let b = self
            .nblocks
            .checked_sub(1)
            .map(|last| {
                let mut lo = 0usize;
                let mut hi = last;
                if self.block_first(0) >= v {
                    return 0usize;
                }
                while lo < hi {
                    let mid = (lo + hi).div_ceil(2);
                    if self.block_first(mid) < v {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
                lo
            })
            .unwrap_or(0);
        if self.block_first(b) >= v {
            return b * self.block_rows;
        }
        b * self.block_rows + self.within_block_lt(b, v)
    }

    /// Rows whose value is in `[lo, hi)`.
    pub fn count_in_range(&self, lo: i64, hi: i64) -> usize {
        let a = self.partition_point_lt(lo);
        let b = self.partition_point_lt(hi);
        b.saturating_sub(a)
    }

    /// The offset within block `b` of the first value `>= v`, assuming the
    /// block's own first value is `< v`.
    ///
    /// Every quantity here is `i64`. That is not a style preference: the
    /// first version inverted the model in `i128`, which put two 128-bit
    /// divisions on each boundary — a libcall apiece — and made a range
    /// count *lose* to a binary search over the decoded column by 4.7x. The
    /// shift rule in `shift_for` is what makes `i64` safe: it bounds
    /// `|dx| << shift` inside an `i64`, and the two clamps below bound
    /// `target - intercept` by the block's own span before anything is
    /// shifted or divided.
    fn within_block_lt(&self, b: usize, v: i64) -> usize {
        let m = self.block_len_rows(b);
        let off = u64::from_le_bytes(
            self.bytes[self.offsets_at + b * 8..self.offsets_at + b * 8 + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let s = self.blocks_at + off;
        let intercept = i64::from_le_bytes(self.bytes[s..s + 8].try_into().unwrap());
        let slope_num = i64::from_le_bytes(self.bytes[s + 8..s + 16].try_into().unwrap());
        let shift = self.bytes[s + 16];
        let width = self.bytes[s + 17] as usize;
        let payload = &self.bytes[s + BLOCK_HEADER..];

        // `v` in the stream's own scale: the smallest x with
        // `base + divisor * x >= v`. Taken in i128 only when the subtraction
        // would overflow, which needs `v` and `base` most of an i64 apart.
        let target: i64 = match v.checked_sub(self.base) {
            Some(d) => div_ceil_i64(d, self.divisor),
            None => {
                let q = div_ceil_i128((v as i128) - (self.base as i128), self.divisor as i128);
                q.clamp(i64::MIN as i128, i64::MAX as i128) as i64
            }
        };

        let value = |d: usize| -> i64 {
            let res = if width == 0 {
                0
            } else {
                read_bits(payload, d * width, width) as i64
            };
            intercept
                .wrapping_add(arsh(slope_num.wrapping_mul(d as i64), shift))
                .wrapping_add(res)
        };

        // Invert the model to bound the answer before reading any residual.
        // Residuals are unsigned, so `intercept + slope·d/2^sh` is a lower
        // bound on the value at `d` and `+ (2^width - 1)` is an upper one;
        // the two bounds cross within a few rows whenever the line fits,
        // which is the case the codec is chosen in.
        let (mut lo, mut hi) = (0usize, m);
        let mut mid = m / 2;
        if slope_num > 0 {
            let last = arsh(slope_num.wrapping_mul((m - 1) as i64), shift);
            let rmax: i64 = if width == 0 {
                0
            } else if width >= 63 {
                i64::MAX
            } else {
                (1i64 << width) - 1
            };
            // smallest d with `intercept + slope·d/2^sh >= t`, for a `t`
            // already clamped into the block's own span
            let step = |t: i64| -> usize {
                if t <= intercept {
                    return 0;
                }
                // `t - intercept` can leave the i64 range when a window
                // bound at an extreme meets a block whose intercept is at
                // the other one. The answer there is "past this block",
                // which is what `m` says, so the check is the answer rather
                // than a guard around it.
                let Some(delta) = t.checked_sub(intercept) else {
                    return m;
                };
                if delta > last {
                    return m;
                }
                // `delta <= last <= |dx| << shift <= i64::MAX >> shift`
                div_ceil_i64(delta << shift, slope_num).clamp(0, m as i64) as usize
            };
            hi = step(target);
            lo = step(target.saturating_sub(rmax));
            if lo > hi {
                lo = 0;
                hi = m;
            }
            mid = step(target.saturating_sub(rmax / 2));
        }

        if lo >= hi {
            return lo;
        }
        // Inside the bound, gallop from the model's own *centre* estimate
        // rather than bisecting the whole interval. The bound is set by the
        // residual's extremes, which are rare; the typical residual sits
        // near the middle, so the answer is usually a handful of rows from
        // that estimate — and a handful of rows is one or two cache lines of
        // packed residuals, read sequentially, against `log2(interval)`
        // scattered probes over the whole bound.
        let start = mid.clamp(lo, hi - 1);
        let (mut a, mut b2) = (lo, hi);
        if value(start) < target {
            // answer is above `start`: double the step until it overshoots
            a = start + 1;
            let mut step_n = 1usize;
            while a < b2 {
                let probe = (a + step_n - 1).min(b2 - 1);
                if value(probe) < target {
                    a = probe + 1;
                    step_n *= 2;
                } else {
                    b2 = probe;
                    break;
                }
            }
        } else {
            b2 = start;
            let mut step_n = 1usize;
            while a < b2 {
                let probe = b2.saturating_sub(step_n).max(a);
                if value(probe) < target {
                    a = probe + 1;
                    break;
                }
                b2 = probe;
                if probe == a {
                    break;
                }
                step_n *= 2;
            }
        }
        while a < b2 {
            let m2 = a + (b2 - a) / 2;
            if value(m2) < target {
                a = m2 + 1;
            } else {
                b2 = m2;
            }
        }
        a
    }
}

/// Ceiling division for a positive divisor, in `i64`.
#[inline]
fn div_ceil_i64(a: i64, b: i64) -> i64 {
    debug_assert!(b > 0);
    let q = a / b;
    if a % b > 0 {
        q + 1
    } else {
        q
    }
}

/// Ceiling division that is correct for negative numerators — `i128::div`
/// truncates toward zero, which is a different function below zero and is
/// exactly where a range boundary lands on a column of negative timestamps.
fn div_ceil_i128(a: i128, b: i128) -> i128 {
    debug_assert!(b > 0);
    let q = a / b;
    if a % b > 0 {
        q + 1
    } else {
        q
    }
}

/// Extract one packed residual.
///
/// This is the innermost operation of both the point path and the range
/// path, and the range path calls it once per probe — so its cost is the
/// range path's cost. The first version built a `u128` a byte at a time,
/// and a range count that should have beaten a binary search over the
/// decoded column instead lost to it by 4.7x, entirely here: ~30 probes per
/// count, each a nine-iteration loop with a bounds check per byte, against
/// the decoded search's ~38 aligned loads.
///
/// The common case — a residual width of 57 bits or less, so `width + shift`
/// fits 64 — is one unaligned 8-byte load and two instructions. The wide
/// case needs a `u128`. The tail of the payload takes the padded path,
/// because the writer pads only to a byte boundary and the last residual can
/// end fewer than eight bytes from the extent's end.
#[inline]
fn read_bits(payload: &[u8], bit_off: usize, width: usize) -> u64 {
    debug_assert!((1..=64).contains(&width));
    let byte = bit_off / 8;
    let shift = bit_off % 8;
    let mask: u64 = if width == 64 { u64::MAX } else { (1u64 << width) - 1 };
    if width + shift <= 64 {
        if let Some(w) = payload.get(byte..byte + 8) {
            return (u64::from_le_bytes(w.try_into().unwrap()) >> shift) & mask;
        }
    } else if let Some(w) = payload.get(byte..byte + 16) {
        let v = u128::from_le_bytes(w.try_into().unwrap()) >> shift;
        return (v as u64) & mask;
    }
    // near the end of the extent: pad rather than over-read
    let mut buf = [0u8; 16];
    let end = payload.len().min(byte + 16);
    buf[..end - byte].copy_from_slice(&payload[byte..end]);
    let v = u128::from_le_bytes(buf) >> shift;
    (v as u64) & mask
}

/// Materialise a codec-2 extent. Mirrors [`crate::codec::decode_i64`].
pub fn decode_i64(bytes: &[u8], expect: usize) -> Result<Vec<i64>> {
    Ok(LinColumn::parse(bytes, expect)?.decode())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode, decode, and check every value — plus every value again
    /// through the random-access path, because `decode` hoists the block
    /// header out of the row loop and `get` does not, and a divergence
    /// between them is a bug that a decode-only test cannot see.
    fn roundtrip(vals: &[i64]) -> Option<Vec<u8>> {
        // both settings of the scale arm: they are separate candidates, not
        // a composition, and each has to be exact on its own
        let unscaled = roundtrip_one(vals, false);
        let scaled = roundtrip_one(vals, true);
        assert_eq!(unscaled.is_some(), scaled.is_some() || vals.len() < 2);
        scaled.or(unscaled)
    }

    fn roundtrip_one(vals: &[i64], scale: bool) -> Option<Vec<u8>> {
        let enc = encode(vals, scale)?;
        let col = LinColumn::parse(&enc, vals.len()).expect("a stream this encoder wrote");
        assert_eq!(col.len(), vals.len());
        assert_eq!(col.decode(), vals, "bulk decode");
        for (i, &v) in vals.iter().enumerate() {
            assert_eq!(col.get(i), v, "random access at row {i}");
        }
        assert_eq!(
            col.is_sorted(),
            vals.windows(2).all(|w| w[0] <= w[1]),
            "the sorted flag must describe the column"
        );
        assert_eq!(col.blocks(), vals.len().div_ceil(BLOCK_ROWS));
        for b in 0..col.blocks() {
            assert_eq!(col.block_first(b), vals[b * BLOCK_ROWS], "block {b} first value");
        }
        Some(enc)
    }

    #[test]
    fn the_shapes_the_store_contains() {
        roundtrip(&[42]).unwrap();
        roundtrip(&vec![7; 10_000]).unwrap();
        roundtrip(&(0..10_000i64).collect::<Vec<_>>()).unwrap();
        roundtrip(&(0..10_000i64).map(|i| -i).collect::<Vec<_>>()).unwrap();
        roundtrip(&(0..10_000i64).map(|i| 1_600_000_000_000_000 + i * 977).collect::<Vec<_>>())
            .unwrap();
        // the real shape: seconds resolution, irregular gaps
        let mut t = 1_600_000_000_000_000i64;
        let real: Vec<i64> = (0..50_000)
            .map(|i: i64| {
                let v = t;
                t += (1 + (i * 7919) % 4001) * 1_000_000;
                v
            })
            .collect();
        roundtrip(&real).unwrap();
        // an empty column has no first value to anchor a block on
        assert!(encode(&[], true).is_none());
        assert!(encode(&[], false).is_none());
    }

    #[test]
    fn adversarial_columns_round_trip_or_decline() {
        // all-negative
        roundtrip(&(0..5_000i64).map(|i| -9_000_000_000 + i * 13).collect::<Vec<_>>()).unwrap();
        // strictly decreasing — the slope is negative and the shift rule has
        // to hold for it too
        roundtrip(&(0..5_000i64).map(|i| 9_000_000_000 - i * 13).collect::<Vec<_>>()).unwrap();
        // straddling zero
        roundtrip(&(-2_500..2_500i64).map(|i| i * 1_000).collect::<Vec<_>>()).unwrap();
        // i64 extremes, and a span no line can hold in i64 residuals — the
        // encoder must decline rather than wrap
        for v in [
            vec![i64::MIN, i64::MAX],
            vec![i64::MIN, 0, i64::MAX],
            vec![i64::MAX, i64::MIN],
            vec![i64::MIN; 3],
            vec![i64::MAX; 3],
        ] {
            if let Some(enc) = encode(&v, true) {
                let col = LinColumn::parse(&enc, v.len()).unwrap();
                assert_eq!(col.decode(), v, "declined-or-exact was neither for {v:?}");
            }
        }
        // xorshift noise: nothing for a line to find, and it must still be
        // exact wherever it is accepted
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

    /// Every length either side of a block boundary, and either side of
    /// several: the residual bit offsets restart per block, the last block
    /// is partial, and an off-by-one in `block_len` shows up as a wrong
    /// value rather than as an error.
    #[test]
    fn block_boundaries_are_exact() {
        for n in [
            1usize, 2, 3,
            BLOCK_ROWS - 1, BLOCK_ROWS, BLOCK_ROWS + 1,
            2 * BLOCK_ROWS - 1, 2 * BLOCK_ROWS, 2 * BLOCK_ROWS + 1,
            5 * BLOCK_ROWS + 7,
        ] {
            // a shape whose residuals are non-trivial, so widths differ
            // block to block and the offset vector is doing real work
            let vals: Vec<i64> = (0..n as i64)
                .map(|i| 1_000_000 + i * 5_000 + (i * i % 997))
                .collect();
            roundtrip(&vals).unwrap_or_else(|| panic!("n = {n} declined"));
        }
    }

    /// Two encodes of the same values must be byte-identical: the fit is
    /// part of the format, not the encoder's private choice.
    #[test]
    fn encoding_is_a_function_of_the_values() {
        let mut t = 1_600_000_000_000_000i64;
        let vals: Vec<i64> = (0..20_000)
            .map(|i: i64| {
                let v = t;
                t += (1 + (i * 31) % 97) * 1_000_000;
                v
            })
            .collect();
        for scale in [true, false] {
            assert_eq!(encode(&vals, scale).unwrap(), encode(&vals, scale).unwrap());
            // and the same values reached by a different route encode the same
            let copy: Vec<i64> = vals.iter().rev().rev().copied().collect();
            assert_eq!(encode(&copy, scale).unwrap(), encode(&vals, scale).unwrap());
        }
    }

    /// The range path must agree with a `partition_point` over the decoded
    /// column at every boundary, including boundaries that fall between two
    /// stored values, before a block, after the last, and on a duplicate.
    #[test]
    fn the_range_path_agrees_with_a_decoded_binary_search() {
        let shapes: Vec<Vec<i64>> = vec![
            // seconds resolution, irregular gaps, several blocks
            {
                let mut t = 1_600_000_000_000_000i64;
                (0..20_000)
                    .map(|i: i64| {
                        let v = t;
                        t += (1 + (i * 7919) % 4001) * 1_000_000;
                        v
                    })
                    .collect()
            },
            // long plateaus: every boundary lands on a duplicate
            (0..8_000i64).map(|i| 500 + i / 37).collect(),
            // a constant column: the model has no slope to invert
            vec![12_345i64; 5_000],
            // negative and straddling zero
            (-4_000..4_000i64).map(|i| i * 3).collect(),
            // the i64 extremes as *values*: a window bound at one end and a
            // block intercept at the other is where the model inversion's
            // subtraction leaves the i64 range
            vec![i64::MIN, i64::MIN / 2, 0, i64::MAX / 2, i64::MAX],
            (0..2_000i64).map(|i| i64::MIN + i * 4_611_686_018_427_387).collect(),
            (0..2_000i64).map(|i| i64::MAX - 4_611_686_018_427_387 * (1_999 - i)).collect(),
            // one huge jump between two blocks
            (0..4_000i64).map(|i| if i < 2_000 { i } else { i + 1_000_000_000 }).collect(),
            // a single row
            vec![7i64],
        ];
        for vals in shapes {
            // A shape the codec declines is not a failure -- the trial
            // encode would simply pick another arm -- but a shape it
            // *accepts* must answer every boundary exactly.
            let Some(enc) = encode(&vals, true) else {
                continue;
            };
            let col = LinColumn::parse(&enc, vals.len()).unwrap();
            assert!(col.is_sorted(), "every shape here ascends");
            let mut probes: Vec<i64> = Vec::new();
            for &v in &vals {
                probes.extend([v.saturating_sub(1), v, v.saturating_add(1)]);
            }
            probes.extend([i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX]);
            probes.push(vals[0].saturating_sub(1_000_000));
            probes.push(vals[vals.len() - 1].saturating_add(1_000_000));
            for t in probes {
                assert_eq!(
                    col.partition_point_lt(t),
                    vals.partition_point(|&x| x < t),
                    "cut at {t} over {} rows",
                    vals.len()
                );
            }
            // and the count form, over windows that start and end anywhere
            for w in [1i64, 10, 1_000, 1_000_000_000] {
                for &start in &[vals[0], vals[vals.len() / 2], vals[vals.len() - 1]] {
                    let (lo, hi) = (start, start.saturating_add(w));
                    assert_eq!(
                        col.count_in_range(lo, hi),
                        vals.iter().filter(|&&x| x >= lo && x < hi).count(),
                        "count in [{lo}, {hi})"
                    );
                }
            }
        }
    }

    /// A corrupt stream must produce an error, never a panic or an over-read
    /// — decode runs even when checksum verification is skipped, so it is
    /// itself a structural validator.
    #[test]
    fn corrupt_streams_error_rather_than_panic() {
        let vals: Vec<i64> = (0..10_000i64).map(|i| 1_000 + i * 7).collect();
        let enc = encode(&vals, true).unwrap();
        assert!(LinColumn::parse(&enc, 9_999).is_err(), "wrong expected count");
        assert!(LinColumn::parse(&enc[..enc.len() - 3], 10_000).is_err(), "truncation");
        assert!(LinColumn::parse(&enc[..10], 10_000).is_err(), "header truncation");
        assert!(LinColumn::parse(&[], 0).is_err(), "no stream at all");

        let mut evil = enc.clone();
        evil[4] = 0; evil[5] = 0; evil[6] = 0; evil[7] = 0; // block_rows = 0
        assert!(LinColumn::parse(&evil, 10_000).is_err(), "zero block length");

        let mut evil = enc.clone();
        evil[17..25].copy_from_slice(&0i64.to_le_bytes()); // divisor = 0
        assert!(LinColumn::parse(&evil, 10_000).is_err(), "zero divisor");

        let mut evil = enc.clone();
        evil[17..25].copy_from_slice(&(-3i64).to_le_bytes());
        assert!(LinColumn::parse(&evil, 10_000).is_err(), "negative divisor");

        // an impossible residual width in the first block header
        let nb = 10_000usize.div_ceil(BLOCK_ROWS);
        let blocks_at = PREAMBLE + nb * 16;
        let mut evil = enc.clone();
        evil[blocks_at + 17] = 99;
        assert!(LinColumn::parse(&evil, 10_000).is_err(), "impossible bit width");

        // a block offset pointing past the extent
        let mut evil = enc.clone();
        evil[PREAMBLE..PREAMBLE + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(LinColumn::parse(&evil, 10_000).is_err(), "offset past the extent");
    }

    /// The claim, measured: a line beats a constant on a column that rises,
    /// and the scale divisor is inside the same stream.
    #[test]
    fn a_rising_column_costs_less_under_a_line_than_under_a_constant() {
        let mut t = 1_600_000_000_000_000i64;
        let vals: Vec<i64> = (0..100_000)
            .map(|i: i64| {
                let v = t;
                t += (1 + (i * 7919) % 4001) * 1_000_000;
                v
            })
            .collect();
        let lin = encode(&vals, true).unwrap();
        let col = LinColumn::parse(&lin, vals.len()).unwrap();
        assert_eq!(col.scale(), (vals[0], 1_000_000), "the stream carries its own scale");
        let for_bits = crate::codec::encode_i64(&vals).len() as f64 * 8.0 / vals.len() as f64;
        let lin_bits = lin.len() as f64 * 8.0 / vals.len() as f64;
        assert!(
            lin_bits < for_bits * 0.6,
            "linFOR {lin_bits:.1} b/r against FOR {for_bits:.1} b/r"
        );
    }
}
