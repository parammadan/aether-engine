//! The `Coordinator` gRPC service. Piece 1 implements `RegisterNode`; scatter-gather
//! `Search` is added in piece 2.

use std::sync::{Arc, RwLock};

use tonic::{Request, Response, Status};

use common::pb::coordinator_server::Coordinator;
use common::pb::{RegisterNodeRequest, RegisterNodeResponse};

use crate::registry::Registry;

/// gRPC handler over a shared registry. The registry sits behind an `RwLock` because many
/// nodes may register concurrently (writes) while future query fan-out reads the shard map.
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
}
