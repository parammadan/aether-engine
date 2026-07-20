//! The `ShardSearch` gRPC service: serves keyword and vector queries against this shard's
//! document store.
//!
//! # Concurrency model
//! The store is wrapped in `Arc<RwLock<ShardStore>>`:
//!   - `Search` / `VectorSearch` take a **read** lock — many queries run concurrently.
//!   - ingestion takes a **write** lock to insert documents.
//! We use `std::sync::RwLock`, not `tokio::sync::RwLock`, because the search is short,
//! CPU-bound work done entirely synchronously inside the handler — we never hold the guard
//! across an `.await`, which is the one thing that would make a std lock in async code
//! dangerous. If searches ever get heavy we'd move them to `spawn_blocking`; for a
//! small in-memory index this is the simpler, correct choice.

use std::sync::{Arc, RwLock};

use tonic::{Request, Response, Status};

use common::pb::shard_search_server::ShardSearch;
use common::pb::{
    AggregateRequest, AggregateResponse, SearchHit, SearchRequest, SearchResponse,
    VectorSearchRequest,
};

use crate::store::ShardStore;

/// How many neighbours a vector search returns when the caller doesn't say. k-NN has no
/// "unlimited" — k is the query's semantics — so 0 maps to a sane default instead.
const DEFAULT_KNN: usize = 10;

/// gRPC handler holding a shared, lockable document store and this shard's id.
pub struct ShardSearchService {
    store: Arc<RwLock<ShardStore>>,
    shard_id: String,
    /// Live virtual-shard table, when this node runs virtual-shard placement. Lets each
    /// hit name the vshard that owns its document (`hash(icao24) % V`); absent under plain
    /// `hash % N` or single-node, where the vshard concept doesn't apply.
    assignments: Option<Arc<RwLock<Vec<u32>>>>,
}

impl ShardSearchService {
    pub fn new(store: Arc<RwLock<ShardStore>>, shard_id: String) -> Self {
        Self { store, shard_id, assignments: None }
    }

    /// Give the search path the live vshard table so hits can carry their owning vshard.
    pub fn with_assignments(mut self, assignments: Arc<RwLock<Vec<u32>>>) -> Self {
        self.assignments = Some(assignments);
        self
    }

    /// The virtual shard owning `icao24` under the current table, or -1 when this node
    /// isn't running virtual-shard placement.
    fn owning_vshard(&self, icao24: &str) -> i32 {
        match &self.assignments {
            Some(a) => {
                let v = a.read().map(|t| t.len()).unwrap_or(0);
                if v == 0 {
                    -1
                } else {
                    (common::shard::fnv1a_64(icao24.as_bytes()) % v as u64) as i32
                }
            }
            None => -1,
        }
    }

    /// Build a hit with its provenance block attached at construction.
    fn hit(&self, doc: common::pb::FlightDocument, score: f64, index: common::pb::IndexKind) -> SearchHit {
        let provenance = common::pb::HitProvenance {
            source_group: self.shard_id.clone(),
            observed_at: doc.observed_at,
            index: index as i32,
            score,
            owning_vshard: self.owning_vshard(&doc.icao24),
        };
        SearchHit { document: Some(doc), score, provenance: Some(provenance) }
    }
}

#[tonic::async_trait]
impl ShardSearch for ShardSearchService {
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let limit = req.limit as usize;
        if let Some(f) = &req.filter {
            common::filter::validate(f).map_err(|e| Status::invalid_argument(e))?;
        }

        // A poisoned lock means a prior handler panicked mid-write; surface it as an
        // internal error rather than propagating the panic.
        let store = self
            .store
            .read()
            .map_err(|_| Status::internal("store lock poisoned"))?;

        // With a filter, rank the FULL match set, filter, then truncate — so
        // `total_matched` counts filtered matches and the top-k is the filtered top-k.
        let fetch = if req.filter.is_some() { 0 } else { limit };
        let results = store.search(&req.query, fetch);

        // Map internal scored hits to the wire type. We clone each stored document into the
        // response; the read guard is dropped at the end of this scope (no `.await` held).
        let mut hits: Vec<SearchHit> = results
            .hits
            .iter()
            .filter(|hit| common::filter::passes(hit.doc, req.filter.as_ref()))
            .map(|hit| self.hit(hit.doc.clone(), hit.score, common::pb::IndexKind::IndexKeyword))
            .collect();
        let total_matched = if req.filter.is_some() { hits.len() } else { results.total_matched };
        if req.filter.is_some() && limit != 0 && hits.len() > limit {
            hits.truncate(limit);
        }

        Ok(Response::new(SearchResponse {
            hits,
            total_matched: total_matched as u64,
            shard_id: self.shard_id.clone(),
            // Coverage fields are a coordinator-level concept; a single shard leaves them 0.
            shards_queried: 0,
            shards_answered: 0,
            manifest: None,
        }))
    }

    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let k = if req.limit == 0 { DEFAULT_KNN } else { req.limit as usize };
        if let Some(f) = &req.filter {
            common::filter::validate(f).map_err(|e| Status::invalid_argument(e))?;
        }

        let store = self
            .store
            .read()
            .map_err(|_| Status::internal("store lock poisoned"))?;

        // The embedding dimension is part of the wire contract: a wrong-sized vector means
        // the caller embedded with a different model than this shard (or a version skew),
        // so reject it loudly instead of scoring garbage.
        let expected = store.embed_dim();
        if req.vector.len() != expected {
            return Err(Status::invalid_argument(format!(
                "query vector has {} dims, this shard embeds at {expected}",
                req.vector.len()
            )));
        }

        // k-NN + filter: over-fetch (a filtered kNN can't know in advance how many of the
        // k nearest survive), filter, truncate to k. 4x is a pragmatic headroom, honestly
        // partial when a filter is very selective — the alternative is scanning everything.
        let fetch = if req.filter.is_some() { k.saturating_mul(4) } else { k };
        let mut hits: Vec<SearchHit> = store
            .vector_search(&req.vector, fetch)
            .iter()
            .filter(|hit| common::filter::passes(hit.doc, req.filter.as_ref()))
            .map(|hit| self.hit(hit.doc.clone(), hit.score, common::pb::IndexKind::IndexVector))
            .collect();
        hits.truncate(k);

        // k-NN returns the k best neighbours; there is no cluster-wide "total matched"
        // notion, so report exactly what we returned.
        let total = hits.len() as u64;

        Ok(Response::new(SearchResponse {
            hits,
            total_matched: total,
            shard_id: self.shard_id.clone(),
            shards_queried: 0,
            shards_answered: 0,
            manifest: None,
        }))
    }

    async fn aggregate(
        &self,
        request: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        let req = request.into_inner();
        if let Some(f) = &req.filter {
            common::filter::validate(f).map_err(|e| Status::invalid_argument(e))?;
        }
        let store = self
            .store
            .read()
            .map_err(|_| Status::internal("store lock poisoned"))?;
        // One pass over the matching (and filtered) documents; the coordinator merges.
        let matched: Vec<&common::pb::FlightDocument> = store
            .matching(&req.query)
            .into_iter()
            .filter(|d| common::filter::passes(d, req.filter.as_ref()))
            .collect();
        let partial = crate::agg::partial(&matched, &req);
        Ok(Response::new(AggregateResponse {
            partial: Some(partial),
            percentiles: Vec::new(), // resolved by the coordinator after merge
            manifest: None,
        }))
    }
}
