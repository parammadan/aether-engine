//! Binary vector quantization: compress embeddings ~30x and scan them with bit ops.
//!
//! # The idea
//! An L2-normalized embedding's *direction* carries the meaning; for high-dimensional
//! vectors, even just the SIGN of each dimension preserves a surprising amount of it
//! (two vectors' angular similarity is monotonically related to the Hamming distance
//! between their sign patterns). So each dimension becomes one bit:
//!
//!   - 384 dims × 4 bytes (f32)  = 1536 bytes   →   384 bits + 4-byte correction = 52 bytes
//!
//! ~30x smaller stored form (32x for the bits alone), and similarity ranking becomes
//! XOR + popcount over u64 words — an order of magnitude cheaper than float dot products.
//!
//! # Accuracy: two-tier search
//! Bits alone lose precision, so quantized search runs in two tiers:
//!   1. **candidate generation**: Hamming-rank ALL vectors (cheap), keep the top
//!      `k × OVERSAMPLE`;
//!   2. **rescore**: exact f32 dot product on just those candidates, return the top k.
//! The compression pays in tier 1 (the full scan touches only bits); the accuracy comes
//! back in tier 2 (exact math on a handful of vectors). The per-vector `correction`
//! (mean absolute magnitude) is stored for asymmetric estimation variants; the two-tier
//! pipeline here doesn't need it for ranking, since Hamming order is scale-free.
//!
//! # What this is honest about
//! The original f32 vectors are retained for the rescore tier (in a disk-backed system
//! they would live cold on disk and page in for rescoring; in this in-memory engine they
//! share RAM). The ~30x figure is the compression of the *scanned representation* — the
//! thing that determines scan cost and, at scale, the working set.

/// How many candidates the binary tier hands to the rescore tier, per requested result.
pub const OVERSAMPLE: usize = 4;

/// A vector compressed to one sign bit per dimension plus a scalar correction.
#[derive(Clone, Debug)]
pub struct QuantizedVector {
    /// Bit `i % 64` of word `i / 64` is set iff dimension `i` is positive.
    bits: Vec<u64>,
    /// Mean absolute magnitude of the original vector — rescales the sign pattern back
    /// toward the original's mass for asymmetric dot estimation.
    pub correction: f32,
    dims: usize,
}

impl QuantizedVector {
    pub fn quantize(v: &[f32]) -> Self {
        let words = v.len().div_ceil(64);
        let mut bits = vec![0u64; words];
        let mut abs_sum = 0f32;
        for (i, &x) in v.iter().enumerate() {
            if x > 0.0 {
                bits[i / 64] |= 1 << (i % 64);
            }
            abs_sum += x.abs();
        }
        let dims = v.len().max(1);
        Self { bits, correction: abs_sum / dims as f32, dims: v.len() }
    }

    /// Hamming distance between two sign patterns: XOR + popcount, word by word.
    /// Lower = more aligned directions.
    pub fn hamming(&self, other: &Self) -> u32 {
        self.bits
            .iter()
            .zip(&other.bits)
            .map(|(a, b)| (a ^ b).count_ones())
            .sum()
    }

    /// Bytes this compressed form occupies (bits + correction).
    pub fn memory_bytes(&self) -> usize {
        self.bits.len() * 8 + std::mem::size_of::<f32>()
    }

    pub fn dims(&self) -> usize {
        self.dims
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantize_sets_bits_for_positive_dims_and_stores_mean_magnitude() {
        let q = QuantizedVector::quantize(&[0.5, -0.5, 0.25, -0.25]);
        // dims 0 and 2 positive -> bits 0 and 2 set.
        assert_eq!(q.bits[0] & 0b1111, 0b0101);
        assert!((q.correction - 0.375).abs() < 1e-6); // mean |x|
        assert_eq!(q.dims(), 4);
    }

    #[test]
    fn hamming_is_zero_on_self_and_counts_flipped_signs() {
        let a = QuantizedVector::quantize(&[1.0, -1.0, 1.0, -1.0]);
        let b = QuantizedVector::quantize(&[1.0, 1.0, -1.0, -1.0]); // dims 1,2 flipped
        assert_eq!(a.hamming(&a), 0);
        assert_eq!(a.hamming(&b), 2);
        assert_eq!(b.hamming(&a), 2); // symmetric
    }

    #[test]
    fn hamming_orders_by_angular_closeness() {
        // near = one sign flip away; far = mostly flipped.
        let base = QuantizedVector::quantize(&[1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0]);
        let near = QuantizedVector::quantize(&[1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0, -1.0]);
        let far = QuantizedVector::quantize(&[-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, -1.0]);
        assert!(base.hamming(&near) < base.hamming(&far));
    }

    #[test]
    fn compression_ratio_is_about_thirty_x_at_384_dims() {
        let v = vec![0.05f32; 384];
        let q = QuantizedVector::quantize(&v);
        let original = 384 * std::mem::size_of::<f32>(); // 1536
        assert_eq!(q.memory_bytes(), 384 / 8 + 4); // 52
        let ratio = original as f64 / q.memory_bytes() as f64;
        assert!(ratio > 29.0, "expected ~30x, got {ratio:.1}x");
    }
}
