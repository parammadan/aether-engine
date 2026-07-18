//! The `Coordinator` gRPC service: node registration and scatter-gather search.

use std::sync::{Arc, RwLock};

use tonic::{Request, Response, Status};

use common::pb::coordinator_server::Coordinator;
use common::pb::{
    ListReplicasRequest, ListReplicasResponse, RegisterNodeRequest, RegisterNodeResponse,
    SearchRequest, SearchResponse,
};

use crate::fanout::{merge_search_responses, scatter_gather};
use crate::registry::Registry;

/// gRPC handler over a shared registry. The registry sits behind an `RwLock` because many
/// nodes may register concurrently (writes) while query fan-out reads the shard map.
pub struct CoordinatorService {
    registry: Arc<RwLock<Registry>>,
}

impl CoordinatorService {
    pub fn new(registry: Arc<RwLock<Registry>>) -> Self {
        Self { registry }
    }
}

#[tonic::async_trait]
impl Coordinator for CoordinatorService {
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        let mut registry = self
            .registry
            .write()
            .map_err(|_| Status::internal("registry lock poisoned"))?;
        let resp = registry.register(req);
        Ok(Response::new(resp))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let limit = req.limit as usize;

        // Snapshot the leader addresses and release the lock BEFORE any await — we must not
        // hold the std RwLock guard across the network fan-out.
        let leaders = {
            let registry = self
                .registry
                .read()
                .map_err(|_| Status::internal("registry lock poisoned"))?;
            registry.leader_addresses()
        };
        let shards_queried = leaders.len() as u32;

        // Fan out concurrently, then merge. Missing shards produce partial (not failed)
        // results — coverage is reported in the response.
        let responses = scatter_gather(leaders, req).await;
        let merged = merge_search_responses(responses, limit, shards_queried);

        Ok(Response::new(merged))
    }

    async fn list_replicas(
        &self,
        request: Request<ListReplicasRequest>,
    ) -> Result<Response<ListReplicasResponse>, Status> {
        let shard_id = request.into_inner().shard_id;
        let registry = self
            .registry
            .read()
            .map_err(|_| Status::internal("registry lock poisoned"))?;
        Ok(Response::new(ListReplicasResponse {
            addresses: registry.follower_addresses(shard_id),
        }))
    }
}
