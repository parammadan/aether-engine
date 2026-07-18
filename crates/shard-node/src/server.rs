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
use common::pb::{SearchHit, SearchRequest, SearchResponse, VectorSearchRequest};

use crate::store::ShardStore;

/// How many neighbours a vector search returns when the caller doesn't say. k-NN has no
/// "unlimited" — k is the query's semantics — so 0 maps to a sane default instead.
const DEFAULT_KNN: usize = 10;

/// gRPC handler holding a shared, lockable document store and this shard's id.
pub struct ShardSearchService {
    store: Arc<RwLock<ShardStore>>,
    shard_id: String,
}

impl ShardSearchService {
    pub fn new(store: Arc<RwLock<ShardStore>>, shard_id: String) -> Self {
        Self { store, shard_id }
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

        // A poisoned lock means a prior handler panicked mid-write; surface it as an
        // internal error rather than propagating the panic.
        let store = self
            .store
            .read()
            .map_err(|_| Status::internal("store lock poisoned"))?;

        let results = store.search(&req.query, limit);

        // Map internal scored hits to the wire type. We clone each stored document into the
        // response; the read guard is dropped at the end of this scope (no `.await` held).
        let hits: Vec<SearchHit> = results
            .hits
            .iter()
            .map(|hit| SearchHit {
                document: Some(hit.doc.clone()),
                score: hit.score,
            })
            .collect();

        Ok(Response::new(SearchResponse {
            hits,
            total_matched: results.total_matched as u64,
            shard_id: self.shard_id.clone(),
            // Coverage fields are a coordinator-level concept; a single shard leaves them 0.
            shards_queried: 0,
            shards_answered: 0,
        }))
    }

    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let k = if req.limit == 0 { DEFAULT_KNN } else { req.limit as usize };

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

        let hits: Vec<SearchHit> = store
            .vector_search(&req.vector, k)
            .iter()
            .map(|hit| SearchHit {
                document: Some(hit.doc.clone()),
                score: hit.score,
            })
            .collect();

        // k-NN returns the k best neighbours; there is no cluster-wide "total matched"
        // notion, so report exactly what we returned.
        let total = hits.len() as u64;

        Ok(Response::new(SearchResponse {
            hits,
            total_matched: total,
            shard_id: self.shard_id.clone(),
            shards_queried: 0,
            shards_answered: 0,
        }))
    }
}
