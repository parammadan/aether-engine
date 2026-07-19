//! Vector (semantic) index over a shard's documents: HNSW approximate nearest-neighbour
//! search over deterministic text embeddings.
//!
//! # Design
//! - Documents are embedded from their text fields (the same fields the keyword index
//!   covers) with the shared, deterministic embedder from `common::embed` — so replicas that
//!   re-embed replicated documents converge to the same vectors, and a query embedded once
//!   at the caller is comparable across every shard.
//! - The index is HNSW (`hnsw_rs`), searched with cosine distance. Scores returned to the
//!   caller are cosine *similarity* (1 - distance), so vector hits merge across shards by the
//!   same score-descending rule as keyword hits.
//! - Inserts are incremental, matching the ingestion loop; no batch rebuild.

use std::collections::HashMap;
use std::sync::Arc;

use common::embed::{dot, Embedder, HashEmbedder};
use common::pb::FlightDocument;
use hnsw_rs::prelude::*;

use crate::quant::{QuantizedVector, OVERSAMPLE};

/// HNSW construction/search parameters. Modest values tuned for an in-memory index of tens
/// of thousands of points; revisit with measurements if the corpus grows.
const MAX_CONNECTIONS: usize = 16; // M: graph degree per layer
const EF_CONSTRUCTION: usize = 200; // breadth of the candidate list while building
const EF_SEARCH: usize = 64; // breadth of the candidate list while searching
const MAX_LAYERS: usize = 16;
const CAPACITY_HINT: usize = 100_000;

/// Below this many documents, search scans vectors exactly instead of using the HNSW graph.
/// Exact search at tiny N is both faster and deterministic — HNSW's randomized structure can
/// have recall holes on very small graphs (a real gap while a shard warms up), and only earns
/// its approximation once the corpus is large enough that a scan would hurt.
const FLAT_SEARCH_THRESHOLD: usize = 256;

/// A scored vector-search hit borrowing the stored document.
pub struct VectorHit<'a> {
    pub doc: &'a FlightDocument,
    /// Cosine similarity in [-1, 1]; higher is more similar.
    pub score: f64,
}

/// The vector index for one shard: an HNSW graph over embedded documents plus the document
/// store the graph's ids point into.
///
/// Inserts are **upserts keyed by `icao24`** (each ingestion poll re-observes the same
/// aircraft). HNSW cannot delete points, so the upsert exploits a domain fact: the embeddable
/// text (callsign/route/type) almost never changes between observations — only position and
/// velocity do. Same text → just overwrite the stored document, no new vector (the common
/// case). Changed text → insert a fresh vector under the SAME slot id; search deduplicates by
/// slot keeping the best match, and the superseded vector lingers in the graph (bounded by
/// how rarely text actually changes — an accepted, documented cost of no-delete HNSW).
pub struct VectorIndex {
    embedder: Arc<dyn Embedder>,
    docs: Vec<FlightDocument>,
    /// Current embeddable text per slot, to detect when a re-observation needs a new vector.
    texts: Vec<String>,
    /// Current embedding per slot (superseded vectors are NOT kept here, unlike the graph).
    /// Powers the exact-scan path below [`FLAT_SEARCH_THRESHOLD`] and the rescore tier of
    /// quantized search.
    slot_vectors: Vec<Vec<f32>>,
    /// Binary-quantized form of each slot's current embedding (~30x smaller; scanned with
    /// XOR+popcount). Kept in lockstep with `slot_vectors`.
    quantized: Vec<QuantizedVector>,
    /// When set, search runs the two-tier quantized pipeline (Hamming candidates → exact
    /// rescore) instead of flat/HNSW.
    quantized_mode: bool,
    /// `icao24 -> slot`, the upsert key.
    by_key: HashMap<String, usize>,
    /// Total vectors ever inserted into the graph (== docs + superseded duplicates). Used to
    /// clamp search over-fetch: asking HNSW for more neighbours than the graph holds returns
    /// unreliable counts on tiny graphs.
    vectors_total: usize,
    hnsw: Hnsw<'static, f32, DistCosine>,
}

impl VectorIndex {
    /// Default index over the deterministic hash embedder.
    pub fn new() -> Self {
        Self::with_embedder(Arc::new(HashEmbedder))
    }

    /// Index over a caller-chosen embedder. Every node in a cluster must be constructed with
    /// the SAME embedder (same implementation, same model) — embeddings are a cross-node
    /// contract, and mixed embedders make replica vectors and query scores incomparable.
    pub fn with_embedder(embedder: Arc<dyn Embedder>) -> Self {
        Self {
            embedder,
            docs: Vec::new(),
            texts: Vec::new(),
            by_key: HashMap::new(),
            slot_vectors: Vec::new(),
            quantized: Vec::new(),
            quantized_mode: false,
            vectors_total: 0,
            hnsw: Hnsw::new(MAX_CONNECTIONS, CAPACITY_HINT, MAX_LAYERS, EF_CONSTRUCTION, DistCosine {}),
        }
    }

    /// Dimensionality of this index's embedding space (a query vector must match it).
    pub fn dim(&self) -> usize {
        self.embedder.dim()
    }

    /// Number of distinct documents (aircraft) indexed.
    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// The text a document is embedded from: the same fields the keyword index covers.
    fn embeddable_text(doc: &FlightDocument) -> String {
        format!("{} {} {} {}", doc.callsign, doc.origin, doc.destination, doc.aircraft_type)
    }

    /// Upsert one document by `icao24` (see the type-level docs for the strategy).
    pub fn insert(&mut self, doc: FlightDocument) {
        let text = Self::embeddable_text(&doc);

        if let Some(&slot) = self.by_key.get(&doc.icao24) {
            self.docs[slot] = doc;
            if self.texts[slot] != text {
                // Text actually changed: index a fresh vector under the same slot.
                let vector = self.embedder.embed(&text);
                self.hnsw.insert((&vector, slot));
                self.vectors_total += 1;
                self.quantized[slot] = QuantizedVector::quantize(&vector);
                self.slot_vectors[slot] = vector;
                self.texts[slot] = text;
            }
            return;
        }

        let slot = self.docs.len();
        let vector = self.embedder.embed(&text);
        self.hnsw.insert((&vector, slot));
        self.vectors_total += 1;
        self.by_key.insert(doc.icao24.clone(), slot);
        self.quantized.push(QuantizedVector::quantize(&vector));
        self.slot_vectors.push(vector);
        self.docs.push(doc);
        self.texts.push(text);
    }

    /// Switch search to the two-tier quantized pipeline (Hamming candidate scan over the
    /// ~30x-compressed forms, exact rescore of the survivors).
    pub fn set_quantized(&mut self, on: bool) {
        self.quantized_mode = on;
    }

    /// k-nearest-neighbour search for an already-embedded query vector. Returns hits scored
    /// by cosine similarity, best first. `k == 0` returns nothing.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<VectorHit<'_>> {
        if k == 0 || self.docs.is_empty() {
            return Vec::new();
        }
        if self.quantized_mode {
            self.search_quantized(query, k)
        } else if self.docs.len() <= FLAT_SEARCH_THRESHOLD {
            self.search_flat(query, k)
        } else {
            self.search_hnsw(query, k)
        }
    }

    /// Two-tier quantized search: tier 1 Hamming-ranks every slot's compressed form
    /// (XOR+popcount over u64 words — the full scan touches ~30x less memory than f32);
    /// tier 2 rescores the `k × OVERSAMPLE` best candidates with exact dot products.
    fn search_quantized(&self, query: &[f32], k: usize) -> Vec<VectorHit<'_>> {
        let q = QuantizedVector::quantize(query);

        // Tier 1: cheap candidate generation over the bits.
        let mut candidates: Vec<(u32, usize)> = self
            .quantized
            .iter()
            .enumerate()
            .map(|(slot, d)| (q.hamming(d), slot))
            .collect();
        let keep = (k * OVERSAMPLE).min(candidates.len());
        candidates.sort_unstable_by_key(|(h, _)| *h);
        candidates.truncate(keep);

        // Tier 2: exact rescore of the survivors only.
        let mut hits: Vec<VectorHit<'_>> = candidates
            .into_iter()
            .map(|(_, slot)| VectorHit {
                doc: &self.docs[slot],
                score: dot(query, &self.slot_vectors[slot]) as f64,
            })
            .collect();
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.doc.icao24.cmp(&b.doc.icao24))
        });
        hits.truncate(k);
        hits
    }

    /// Exact scan over the current vector per slot: deterministic, no superseded duplicates,
    /// and cheap at small N. (Vectors are L2-normalized, so dot product = cosine similarity.)
    fn search_flat(&self, query: &[f32], k: usize) -> Vec<VectorHit<'_>> {
        let mut hits: Vec<VectorHit<'_>> = self
            .slot_vectors
            .iter()
            .enumerate()
            .map(|(slot, v)| VectorHit {
                doc: &self.docs[slot],
                score: dot(query, v) as f64,
            })
            .collect();
        // Score descending; ties broken by icao24 for a stable, deterministic order.
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.doc.icao24.cmp(&b.doc.icao24))
        });
        hits.truncate(k);
        hits
    }

    /// Approximate search via the HNSW graph. Over-fetches and deduplicates by slot, because
    /// an upserted document whose text changed leaves a superseded vector behind (same slot
    /// id, older geometry).
    fn search_hnsw(&self, query: &[f32], k: usize) -> Vec<VectorHit<'_>> {
        // Headroom for superseded duplicates, bounded two ways: a pathological doc can't
        // inflate it, and we never ask the graph for more neighbours than it holds.
        let fetch = (k * 2).min(k + 32).min(self.vectors_total).max(k.min(self.vectors_total));
        let neighbours = self.hnsw.search(query, fetch, EF_SEARCH.max(fetch));

        let mut best: Vec<VectorHit<'_>> = Vec::with_capacity(k);
        let mut seen = std::collections::HashSet::new();
        for n in neighbours {
            // Neighbours arrive best-first; keep the first (closest) hit per slot.
            if seen.insert(n.d_id) {
                best.push(VectorHit {
                    doc: &self.docs[n.d_id],
                    score: 1.0 - n.distance as f64, // cosine distance -> cosine similarity
                });
                if best.len() == k {
                    break;
                }
            }
        }
        best
    }
}

impl Default for VectorIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(icao24: &str, callsign: &str, origin: &str, destination: &str, aircraft: &str) -> FlightDocument {
        FlightDocument {
            icao24: icao24.to_string(),
            callsign: callsign.to_string(),
            origin: origin.to_string(),
            destination: destination.to_string(),
            aircraft_type: aircraft.to_string(),
            ..Default::default()
        }
    }

    fn sample() -> VectorIndex {
        let mut idx = VectorIndex::new();
        idx.insert(doc("a1", "UAL231", "United States", "JFK", "Boeing 737"));
        idx.insert(doc("b2", "DAL45", "France", "LAX", "Airbus A320"));
        idx.insert(doc("c3", "UAL900", "United States", "SFO", "Boeing 777"));
        idx
    }

    #[test]
    fn nearest_neighbour_is_the_lexically_closest_document() {
        let idx = sample();
        // A query sharing tokens with the Boeing/United docs should rank one of them first.
        let query = HashEmbedder.embed("Boeing United States");
        let hits = idx.search(&query, 3);
        assert_eq!(hits.len(), 3);
        assert!(
            hits[0].doc.icao24 == "a1" || hits[0].doc.icao24 == "c3",
            "expected a Boeing/United doc first, got {}",
            hits[0].doc.icao24
        );
        // Best-first ordering.
        assert!(hits[0].score >= hits[1].score && hits[1].score >= hits[2].score);
        // The Airbus/France doc should be the worst match of the three.
        assert_eq!(hits[2].doc.icao24, "b2");
    }

    #[test]
    fn k_limits_the_result_count() {
        let idx = sample();
        let query = HashEmbedder.embed("Boeing");
        assert_eq!(idx.search(&query, 2).len(), 2);
        assert!(idx.search(&query, 0).is_empty());
    }

    #[test]
    fn empty_index_returns_nothing() {
        let idx = VectorIndex::new();
        let query = HashEmbedder.embed("anything");
        assert!(idx.search(&query, 5).is_empty());
    }

    #[test]
    fn reinserting_an_aircraft_does_not_duplicate_results() {
        let mut idx = VectorIndex::new();
        // The common case: same aircraft re-observed many polls with unchanged text.
        for _ in 0..5 {
            idx.insert(doc("a1", "UAL231", "United States", "JFK", "Boeing 737"));
        }
        idx.insert(doc("b2", "DAL45", "France", "LAX", "Airbus A320"));

        assert_eq!(idx.len(), 2); // two aircraft, not six documents
        let query = HashEmbedder.embed("UAL231 Boeing");
        let hits = idx.search(&query, 5);
        assert_eq!(hits.len(), 2); // one hit per aircraft — no duplicates
        assert_eq!(hits[0].doc.icao24, "a1");
    }

    #[test]
    fn hnsw_path_finds_close_neighbours_at_scale() {
        // Push past FLAT_SEARCH_THRESHOLD so search takes the graph path. Assertions are
        // approximate-tolerant: HNSW guarantees *near*-nearest, not exact recall.
        let mut idx = VectorIndex::new();
        for i in 0..300 {
            idx.insert(doc(
                &format!("id{i:04}"),
                &format!("FL{i:04}"),
                if i % 3 == 0 { "United States" } else { "France" },
                "XXX",
                if i % 2 == 0 { "Boeing 737" } else { "Airbus A320" },
            ));
        }
        assert!(idx.len() > FLAT_SEARCH_THRESHOLD);

        // Query for a specific flight's exact text: its own vector is the true nearest.
        let query = HashEmbedder.embed("FL0042 United States XXX Boeing 737");
        let hits = idx.search(&query, 10);
        assert_eq!(hits.len(), 10);
        // Scores arrive best-first.
        for pair in hits.windows(2) {
            assert!(pair[0].score >= pair[1].score);
        }
        // The exact match should be among the near-nearest returned.
        assert!(
            hits.iter().any(|h| h.doc.icao24 == "id0042"),
            "expected id0042 within the top 10 approximate neighbours"
        );
    }

    #[test]
    fn quantized_mode_agrees_with_exact_search_on_the_top_hit() {
        let mut exact = VectorIndex::new();
        let mut quant = VectorIndex::new();
        quant.set_quantized(true);
        for i in 0..40 {
            let d = doc(
                &format!("id{i:04}"),
                &format!("FL{i:04}"),
                if i % 2 == 0 { "United States" } else { "France" },
                "XXX",
                if i % 2 == 0 { "Boeing 737" } else { "Airbus A320" },
            );
            exact.insert(d.clone());
            quant.insert(d);
        }

        let query = HashEmbedder.embed("FL0006 United States XXX Boeing 737");
        let exact_top = exact.search(&query, 1);
        let quant_top = quant.search(&query, 1);
        assert_eq!(exact_top[0].doc.icao24, "id0006");
        // The two-tier pipeline rescores with exact math, so the top hit must agree.
        assert_eq!(quant_top[0].doc.icao24, exact_top[0].doc.icao24);
        assert!((quant_top[0].score - exact_top[0].score).abs() < 1e-6);
    }

    #[test]
    fn changed_text_updates_the_match_without_duplicating() {
        let mut idx = VectorIndex::new();
        idx.insert(doc("a1", "UAL231", "United States", "JFK", "Boeing 737"));
        // Re-observed with a changed callsign: a new vector under the same slot.
        idx.insert(doc("a1", "UAL500", "United States", "ORD", "Boeing 737"));

        assert_eq!(idx.len(), 1);
        let query = HashEmbedder.embed("UAL500 ORD Boeing");
        let hits = idx.search(&query, 5);
        // The slot appears once (dedup), and serves the CURRENT document.
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc.callsign, "UAL500");
    }
}
