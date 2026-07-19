//! The `Coordinator` gRPC service: node registration and scatter-gather search.

use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use common::pb::coordinator_server::Coordinator;
use common::pb::shard_search_client::ShardSearchClient;
use common::pb::{
    ClusterStateRequest, ClusterStateResponse, DrainRequest, DrainResponse, HeartbeatRequest,
    HeartbeatResponse,
    ListReplicasRequest, ListReplicasResponse, NodeState, RegisterNodeRequest,
    RegisterNodeResponse, ReassignVShardRequest, ReassignVShardResponse, SearchRequest,
    SearchResponse, SearchUpdate, ShardMembersRequest, ShardMembersResponse,
    VShardAssignments, VShardAssignmentsRequest,
};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::fanout::{merge_search_responses, scatter_gather, ProgressiveMerge};
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
    type SearchStreamStream =
        Pin<Box<dyn Stream<Item = Result<SearchUpdate, Status>> + Send + 'static>>;

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

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let now = Instant::now();
        let mut registry = self
            .registry
            .write()
            .map_err(|_| Status::internal("registry lock poisoned"))?;
        let mut role = registry.heartbeat(&req.node_id, now);
        // A raft-elected leader's report rewires routing: raft did the arbitration.
        if req.raft_leader && role.is_some() {
            if let Some(shard) = registry.report_raft_leader(&req.node_id, now) {
                println!("coordinator: raft leader '{}' now routes shard {shard}", req.node_id);
            }
            role = Some(common::pb::NodeRole::Leader);
        }
        Ok(Response::new(HeartbeatResponse {
            known: role.is_some(),
            current_role: role.unwrap_or(common::pb::NodeRole::Unspecified) as i32,
        }))
    }

    async fn list_shard_members(
        &self,
        request: Request<ShardMembersRequest>,
    ) -> Result<Response<ShardMembersResponse>, Status> {
        let shard_id = request.into_inner().shard_id;
        let registry = self
            .registry
            .read()
            .map_err(|_| Status::internal("registry lock poisoned"))?;
        let members = registry
            .members_of(shard_id, Instant::now())
            .into_iter()
            .map(|n| NodeState {
                node_id: n.node_id,
                address: n.address,
                role: n.role as i32,
                shard_id: n.shard_id,
                millis_since_seen: n.since_seen.as_millis() as u64,
                draining: n.draining,
            })
            .collect();
        Ok(Response::new(ShardMembersResponse { members }))
    }

    async fn drain_node(
        &self,
        request: Request<DrainRequest>,
    ) -> Result<Response<DrainResponse>, Status> {
        let node_id = request.into_inner().node_id;
        let mut registry = self
            .registry
            .write()
            .map_err(|_| Status::internal("registry lock poisoned"))?;
        let ok = registry.mark_draining(&node_id);
        let message = if ok {
            println!("coordinator: '{node_id}' marked draining");
            format!("'{node_id}' marked draining; its group leader will remove it")
        } else {
            format!("unknown node '{node_id}'")
        };
        Ok(Response::new(DrainResponse { ok, message }))
    }

    async fn get_cluster_state(
        &self,
        _request: Request<ClusterStateRequest>,
    ) -> Result<Response<ClusterStateResponse>, Status> {
        let registry = self
            .registry
            .read()
            .map_err(|_| Status::internal("registry lock poisoned"))?;
        let nodes = registry
            .snapshot(Instant::now())
            .into_iter()
            .map(|n| NodeState {
                node_id: n.node_id,
                address: n.address,
                role: n.role as i32,
                shard_id: n.shard_id,
                millis_since_seen: n.since_seen.as_millis() as u64,
                draining: n.draining,
            })
            .collect();
        Ok(Response::new(ClusterStateResponse {
            shard_count: registry.shard_count(),
            nodes,
            vshard_group: registry.vshard_assignments(),
        }))
    }

    async fn get_v_shard_assignments(
        &self,
        _request: Request<VShardAssignmentsRequest>,
    ) -> Result<Response<VShardAssignments>, Status> {
        let registry = self
            .registry
            .read()
            .map_err(|_| Status::internal("registry lock poisoned"))?;
        Ok(Response::new(VShardAssignments {
            group_of: registry.vshard_assignments(),
        }))
    }

    async fn reassign_v_shard(
        &self,
        request: Request<ReassignVShardRequest>,
    ) -> Result<Response<ReassignVShardResponse>, Status> {
        let req = request.into_inner();
        let mut registry = self
            .registry
            .write()
            .map_err(|_| Status::internal("registry lock poisoned"))?;
        match registry.reassign_vshard(req.vshard, req.group) {
            Ok(()) => {
                println!("coordinator: vshard {} reassigned to group {}", req.vshard, req.group);
                Ok(Response::new(ReassignVShardResponse {
                    ok: true,
                    message: format!("vshard {} -> group {}", req.vshard, req.group),
                }))
            }
            Err(message) => Ok(Response::new(ReassignVShardResponse { ok: false, message })),
        }
    }

    async fn search_stream(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<Self::SearchStreamStream>, Status> {
        let req = request.into_inner();
        let limit = req.limit as usize;

        // Snapshot leaders and release the lock before any await.
        let leaders = {
            let registry = self
                .registry
                .read()
                .map_err(|_| Status::internal("registry lock poisoned"))?;
            registry.leader_addresses()
        };
        let shards_queried = leaders.len() as u32;

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            let mut merge = ProgressiveMerge::new(shards_queried, limit);

            // Query every leader concurrently; fold + emit an update as each one reports.
            let mut set = JoinSet::new();
            for addr in leaders {
                let query = req.clone();
                set.spawn(async move {
                    let mut client = ShardSearchClient::connect(format!("http://{addr}")).await.ok()?;
                    client.search(query).await.ok().map(|r| r.into_inner())
                });
            }

            while let Some(joined) = set.join_next().await {
                if let Ok(Some(resp)) = joined {
                    merge.add(resp);
                    // Client hung up — stop streaming.
                    if tx.send(Ok(merge.snapshot(false))).await.is_err() {
                        return;
                    }
                }
            }

            // Final update once every shard has reported (or failed).
            let _ = tx.send(Ok(merge.snapshot(true))).await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
