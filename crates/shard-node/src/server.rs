//! The `ShardSearch` gRPC service: serves queries against this shard's inverted index.
//!
//! # Concurrency model (defensible, logged in DECISIONS ADR-0005)
//! The index is wrapped in `Arc<RwLock<InvertedIndex>>`:
//!   - `Search` takes a **read** lock — many queries run concurrently.
//!   - ingestion (a later Q1 step) takes a **write** lock to insert documents.
//! We use `std::sync::RwLock`, not `tokio::sync::RwLock`, because the search is short,
//! CPU-bound work done entirely synchronously inside the handler — we never hold the guard
//! across an `.await`, which is the one thing that would make a std lock in async code
//! dangerous. If searches ever get heavy we'd move them to `spawn_blocking`; for a small
//! in-memory Q1 index this is the simpler, correct choice.

use std::sync::{Arc, RwLock};

use tonic::{Request, Response, Status};

use common::pb::shard_search_server::ShardSearch;
use common::pb::{SearchHit, SearchRequest, SearchResponse};

use crate::index::InvertedIndex;

/// gRPC handler holding a shared, lockable index and this shard's id.
pub struct ShardSearchService {
    index: Arc<RwLock<InvertedIndex>>,
    shard_id: String,
}

impl ShardSearchService {
    pub fn new(index: Arc<RwLock<InvertedIndex>>, shard_id: String) -> Self {
        Self { index, shard_id }
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
        let index = self
            .index
            .read()
            .map_err(|_| Status::internal("index lock poisoned"))?;

        let results = index.search(&req.query, limit);

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
        }))
    }
}
