//! A small t-digest: a mergeable sketch for approximate percentiles.
//!
//! # Why a sketch at all
//! Percentiles are the one aggregate that CANNOT be merged from naive per-shard results:
//! the median of medians is not the median. A t-digest can — merging two digests is
//! concatenating their centroids and re-compressing, which is associative — so each shard
//! builds a digest over its own data and the coordinator merges them into one the whole
//! cluster's percentiles read from. Being able to say *why* p99 across shards needs a
//! sketch is exactly the point of the aggregation item.
//!
//! # The construction (deliberately the simple, correct variant)
//! Buffer raw samples; on demand, sort and greedily form centroids under a size bound that
//! is tighter near the tails (where percentile accuracy matters) and looser in the middle.
//! The bound uses the standard scale function k(q) = δ/(2π) · arcsin(2q − 1): a centroid may
//! absorb weight only while its running quantile stays within one "k-step" of where it
//! started. This is O(n log n) per compression, which is fine — we compress once per query,
//! not per sample. Accuracy is measured (not assumed) against exact percentiles in tests.

use std::f64::consts::PI;

/// One centroid: a cluster of samples summarized by their mean and total weight (count).
#[derive(Clone, Copy, Debug)]
pub struct Centroid {
    pub mean: f64,
    pub weight: f64,
}

/// A t-digest. Holds compressed centroids plus a small buffer of un-merged raw samples that
/// are folded in lazily (before any query or merge).
#[derive(Clone, Debug)]
pub struct TDigest {
    centroids: Vec<Centroid>,
    buffer: Vec<f64>,
    count: f64,
    /// Compression δ: higher = more centroids = more accurate, more memory. 100 is typical.
    delta: f64,
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new()
    }
}

impl TDigest {
    pub fn new() -> Self {
        Self::with_delta(100.0)
    }

    pub fn with_delta(delta: f64) -> Self {
        Self { centroids: Vec::new(), buffer: Vec::new(), count: 0.0, delta }
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0.0
    }

    pub fn count(&self) -> f64 {
        self.count
    }

    /// Add one sample.
    pub fn add(&mut self, x: f64) {
        if x.is_finite() {
            self.buffer.push(x);
            self.count += 1.0;
            // Bound the buffer so a long ingest doesn't grow it without bound.
            if self.buffer.len() as f64 >= self.delta * 10.0 {
                self.compress();
            }
        }
    }

    /// Serialize to the wire form (parallel mean/weight arrays). Compresses first so the
    /// buffer is folded in.
    pub fn to_parts(&mut self) -> (Vec<f64>, Vec<f64>) {
        self.compress();
        (
            self.centroids.iter().map(|c| c.mean).collect(),
            self.centroids.iter().map(|c| c.weight).collect(),
        )
    }

    /// Reconstruct from wire parts.
    pub fn from_parts(means: &[f64], weights: &[f64]) -> Self {
        let mut d = Self::new();
        for (&mean, &weight) in means.iter().zip(weights) {
            if weight > 0.0 {
                d.centroids.push(Centroid { mean, weight });
                d.count += weight;
            }
        }
        d.centroids.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap());
        d
    }

    /// Merge another digest into this one (associative). This is why per-shard digests
    /// combine at the coordinator: dump both centroid sets together and re-compress.
    pub fn merge(&mut self, other: &TDigest) {
        for c in &other.centroids {
            self.centroids.push(*c);
            self.count += c.weight;
        }
        for &x in &other.buffer {
            self.buffer.push(x);
            self.count += 1.0;
        }
        self.compress();
    }

    /// Fold buffered samples into centroids and re-cluster everything under the scale bound.
    fn compress(&mut self) {
        if self.buffer.is_empty() && self.centroids.len() <= 1 {
            return;
        }
        // Gather all weighted points (existing centroids + buffered singletons), sorted.
        let mut points: Vec<Centroid> = std::mem::take(&mut self.centroids);
        for x in self.buffer.drain(..) {
            points.push(Centroid { mean: x, weight: 1.0 });
        }
        points.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap());

        let total: f64 = points.iter().map(|c| c.weight).sum();
        if total == 0.0 {
            self.count = 0.0;
            return;
        }

        // Greedy clustering: `cum` is the weight before the current centroid. A centroid
        // may absorb the next point only while the quantile span [cum, cum+w] it would then
        // cover stays within one scale-step, k(q_right) − k(q_left) ≤ 1. That bound is
        // tight near the tails and loose in the middle, so the tails get more centroids.
        let mut merged: Vec<Centroid> = Vec::new();
        let mut cur = points[0];
        let mut cum = 0.0; // total weight strictly before `cur`
        for p in &points[1..] {
            let proposed = cur.weight + p.weight;
            let q_left = cum / total;
            let q_right = (cum + proposed) / total;
            if self.k(q_right) - self.k(q_left) <= 1.0 {
                cur.mean = (cur.mean * cur.weight + p.mean * p.weight) / proposed;
                cur.weight = proposed;
            } else {
                cum += cur.weight;
                merged.push(cur);
                cur = *p;
            }
        }
        merged.push(cur);

        self.centroids = merged;
        self.count = total;
    }

    /// The scale function k(q) = (δ / 2π) · arcsin(2q − 1). Tighter near 0 and 1.
    fn k(&self, q: f64) -> f64 {
        let q = q.clamp(0.0, 1.0);
        (self.delta / (2.0 * PI)) * (2.0 * q - 1.0).asin()
    }

    /// The value at quantile `q` in [0, 1], by interpolating across centroid midpoints.
    pub fn quantile(&self, q: f64) -> f64 {
        if self.centroids.is_empty() {
            return f64::NAN;
        }
        if self.centroids.len() == 1 {
            return self.centroids[0].mean;
        }
        let target = q.clamp(0.0, 1.0) * self.count;

        // Walk centroids; each centroid c is centered at cumulative weight (acc + c.weight/2).
        let mut acc = 0.0;
        let mut prev_center = 0.0;
        let mut prev_mean = self.centroids[0].mean;
        for (i, c) in self.centroids.iter().enumerate() {
            let center = acc + c.weight / 2.0;
            if target <= center {
                if i == 0 {
                    return c.mean;
                }
                // Linear interpolation between the previous centroid's center and this one's.
                let t = (target - prev_center) / (center - prev_center);
                return prev_mean + t * (c.mean - prev_mean);
            }
            prev_center = center;
            prev_mean = c.mean;
            acc += c.weight;
        }
        self.centroids.last().unwrap().mean
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exact percentile of a sample set, for comparison.
    fn exact(mut xs: Vec<f64>, p: f64) -> f64 {
        xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p / 100.0) * (xs.len() as f64 - 1.0)).round() as usize;
        xs[idx.min(xs.len() - 1)]
    }

    fn rel_err(approx: f64, exact: f64, spread: f64) -> f64 {
        (approx - exact).abs() / spread.max(1.0)
    }

    #[test]
    fn percentiles_track_exact_within_bounds_uniform() {
        let xs: Vec<f64> = (0..10_000).map(|i| i as f64).collect();
        let mut d = TDigest::new();
        for &x in &xs {
            d.add(x);
        }
        for p in [50.0, 90.0, 95.0, 99.0] {
            let approx = d.quantile(p / 100.0);
            let err = rel_err(approx, exact(xs.clone(), p), 10_000.0);
            assert!(err < 0.02, "p{p}: approx {approx} vs exact, rel err {err:.4}");
        }
    }

    #[test]
    fn merging_shard_digests_equals_one_over_the_union() {
        // Three "shards" each digest a disjoint slice; the merged digest's percentiles
        // track the exact percentiles of the whole set — the property aggregation needs.
        let all: Vec<f64> = (0..9_000).map(|i| (i as f64 * 1.7) % 5000.0).collect();
        let mut shards: Vec<TDigest> = (0..3).map(|_| TDigest::new()).collect();
        for (i, &x) in all.iter().enumerate() {
            shards[i % 3].add(x);
        }
        let mut merged = TDigest::new();
        for s in &shards {
            merged.merge(s);
        }
        for p in [50.0, 90.0, 99.0] {
            let approx = merged.quantile(p / 100.0);
            let err = rel_err(approx, exact(all.clone(), p), 5000.0);
            assert!(err < 0.03, "merged p{p}: rel err {err:.4}");
        }
    }

    #[test]
    fn roundtrip_through_wire_parts_preserves_percentiles() {
        let mut d = TDigest::new();
        for i in 0..5_000 {
            d.add((i as f64).sqrt());
        }
        let p99_before = d.quantile(0.99);
        let (means, weights) = d.to_parts();
        let restored = TDigest::from_parts(&means, &weights);
        let p99_after = restored.quantile(0.99);
        assert!((p99_before - p99_after).abs() < 1e-9, "wire roundtrip changed p99");
    }

    #[test]
    fn empty_digest_is_nan_not_a_panic() {
        let d = TDigest::new();
        assert!(d.quantile(0.5).is_nan());
        assert!(d.is_empty());
    }
}
