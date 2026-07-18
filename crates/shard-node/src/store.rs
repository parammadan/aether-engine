//! The shard's document store: one keyword (inverted) index and one vector (HNSW) index,
//! updated together behind a single lock so the two views of the shard can never diverge —
//! every inserted document is immediately findable both lexically and semantically.

use std::sync::Arc;

use common::embed::Embedder;
use common::pb::FlightDocument;

use crate::index::{InvertedIndex, SearchResults};
use crate::vector::{VectorHit, VectorIndex};

pub struct ShardStore {
    keyword: InvertedIndex,
    vector: VectorIndex,
}

impl ShardStore {
    /// Store over the default deterministic hash embedder.
    pub fn new() -> Self {
        Self {
            keyword: InvertedIndex::new(),
            vector: VectorIndex::new(),
        }
    }

    /// Store over a caller-chosen embedder (must be identical on every node — see
    /// [`VectorIndex::with_embedder`]).
    pub fn with_embedder(embedder: Arc<dyn Embedder>) -> Self {
        Self {
            keyword: InvertedIndex::new(),
            vector: VectorIndex::with_embedder(embedder),
        }
    }

    /// Dimensionality of the vector side's embedding space.
    pub fn embed_dim(&self) -> usize {
        self.vector.dim()
    }

    /// Index one document in both views.
    pub fn insert(&mut self, doc: FlightDocument) {
        self.vector.insert(doc.clone());
        self.keyword.insert(doc);
    }

    /// Keyword search (see [`InvertedIndex::search`]).
    pub fn search(&self, query: &str, limit: usize) -> SearchResults<'_> {
        self.keyword.search(query, limit)
    }

    /// Vector search for an already-embedded query (see [`VectorIndex::search`]).
    pub fn vector_search(&self, query: &[f32], k: usize) -> Vec<VectorHit<'_>> {
        self.vector.search(query, k)
    }

    /// Number of documents indexed.
    pub fn len(&self) -> usize {
        self.keyword.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keyword.is_empty()
    }
}

impl Default for ShardStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::embed::{Embedder, HashEmbedder};

    fn doc(icao24: &str, callsign: &str, origin: &str) -> FlightDocument {
        FlightDocument {
            icao24: icao24.to_string(),
            callsign: callsign.to_string(),
            origin: origin.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn one_insert_feeds_both_indexes() {
        let mut store = ShardStore::new();
        store.insert(doc("a1", "UAL231", "United States"));
        store.insert(doc("b2", "DAL45", "France"));

        // Keyword view.
        assert_eq!(store.search("ual231", 10).total_matched, 1);

        // Vector view: same corpus, semantically queried.
        let query = HashEmbedder.embed("UAL231 United States");
        let hits = store.vector_search(&query, 2);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].doc.icao24, "a1");

        assert_eq!(store.len(), 2);
    }
}
