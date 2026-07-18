//! Text embedding: map a document's text to a fixed-dimension vector for semantic search.
//!
//! # Why this lives in `common`
//! Embeddings are a *distributed contract*, exactly like the shard hash: a follower re-embeds
//! the documents its leader replicates to it, and a query vector must live in the same space
//! as every shard's document vectors. If two nodes embedded differently, replicas would
//! diverge and cross-shard scores would be incomparable. So the embedder is shared, and every
//! implementation must be deterministic across processes and machines.
//!
//! # The baseline: feature hashing (the "hashing trick")
//! [`HashEmbedder`] embeds a text as a bag of tokens hashed into a fixed number of buckets:
//! each token's FNV-1a hash picks a bucket (and a sign bit), the bucket accumulates ±1, and
//! the final vector is L2-normalized so dot product = cosine similarity. Properties:
//!   - **Deterministic everywhere, forever** — FNV-1a is seedless and fully specified (the
//!     same argument as the shard key in [`crate::shard`]).
//!   - **Real lexical-overlap semantics**: texts sharing tokens land in shared buckets, so
//!     "Boeing 737" is closer to "Boeing 777" than to "Airbus A320". No model file, no
//!     network, no native deps — trivially testable.
//!   - **Honest limitation**: it captures token overlap, not meaning ("plane" and "aircraft"
//!     don't land near each other). A learned model (e.g. a MiniLM ONNX export) is the
//!     drop-in upgrade behind the same trait; the interface and the distributed plumbing
//!     don't change.

use crate::shard::fnv1a_64;

/// Dimensionality of the [`HashEmbedder`]'s space. The authoritative dimension for any
/// embedder is [`Embedder::dim`] — different implementations have different dims (a learned
/// model is typically 384), and every node in a cluster must use the same embedder, so the
/// dim doubles as a cheap cross-node consistency check on the wire.
pub const EMBED_DIM: usize = 128;

/// A deterministic text embedder. Implementations MUST produce identical output for identical
/// input on every node — replicas and queries depend on it.
pub trait Embedder: Send + Sync {
    /// Embed `text` into an L2-normalized vector of [`Self::dim`] elements.
    fn embed(&self, text: &str) -> Vec<f32>;
    fn dim(&self) -> usize;
}

/// Feature-hashing embedder: token -> FNV-1a -> (bucket, sign) -> accumulate -> L2 normalize.
#[derive(Default, Clone, Copy)]
pub struct HashEmbedder;

impl Embedder for HashEmbedder {
    fn embed(&self, text: &str) -> Vec<f32> {
        let mut v = vec![0.0f32; EMBED_DIM];
        for token in tokenize(text) {
            let h = fnv1a_64(token.as_bytes());
            let bucket = (h % EMBED_DIM as u64) as usize;
            // An independent bit of the hash decides the sign; signed accumulation keeps the
            // expected dot product of unrelated texts near zero (standard hashing trick).
            let sign = if (h >> 63) & 1 == 1 { 1.0 } else { -1.0 };
            v[bucket] += sign;
        }
        l2_normalize(&mut v);
        v
    }

    fn dim(&self) -> usize {
        EMBED_DIM
    }
}

/// Cosine similarity of two L2-normalized vectors is just their dot product.
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn l2_normalize(v: &mut [f32]) {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
}

/// Same tokenization rule as the keyword index: lowercase alphanumeric runs.
fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedding_is_deterministic_and_normalized() {
        let e = HashEmbedder;
        let a = e.embed("Boeing 737 SFO JFK");
        let b = e.embed("Boeing 737 SFO JFK");
        assert_eq!(a, b);
        assert_eq!(a.len(), EMBED_DIM);
        let norm: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5, "norm was {norm}");
    }

    #[test]
    fn shared_tokens_mean_higher_similarity() {
        let e = HashEmbedder;
        let boeing_737 = e.embed("Boeing 737");
        let boeing_777 = e.embed("Boeing 777");
        let airbus = e.embed("Airbus A320");

        let same_maker = dot(&boeing_737, &boeing_777);
        let cross_maker = dot(&boeing_737, &airbus);
        assert!(
            same_maker > cross_maker,
            "expected shared-token similarity ({same_maker}) > disjoint ({cross_maker})"
        );
    }

    #[test]
    fn empty_text_embeds_to_zero_vector_without_panic() {
        let e = HashEmbedder;
        let v = e.embed("   ");
        assert_eq!(v.len(), EMBED_DIM);
        assert!(v.iter().all(|x| *x == 0.0));
    }
}
