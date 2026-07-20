//! The `Coordinator` gRPC service: node registration and scatter-gather search.

use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use common::pb::coordinator_server::Coordinator;
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

use crate::fanout::{merge_search_responses, scatter_gather, scatter_gather_vector, ProgressiveMerge};
use crate::registry::Registry;

/// The bearer credential a caller presented, to be replayed when a follower forwards a
/// mutating RPC to the control-group leader (the leader authorizes it too).
fn bearer_of<T>(req: &Request<T>) -> Option<String> {
    req.metadata().get("authorization").and_then(|v| v.to_str().ok()).map(String::from)
}

/// Wrap a forwarded message, re-attaching the caller's bearer credential.
fn forward<T>(msg: T, bearer: &Option<String>) -> Request<T> {
    let mut req = Request::new(msg);
    if let Some(h) = bearer {
        if let Ok(v) = h.parse() {
            req.metadata_mut().insert("authorization", v);
        }
    }
    req
}

/// gRPC handler over a shared registry. The registry sits behind an `RwLock` because many
/// nodes may register concurrently (writes) while query fan-out reads the shard map.
pub struct CoordinatorService {
    registry: Arc<RwLock<Registry>>,
    /// When this coordinator is a replica of a state group, operator-intent mutations
    /// (reassign, drain) commit through it instead of writing the registry directly.
    control: Option<Arc<crate::control::ControlPlane>>,
    /// Scoped-token policy for client-facing RPCs (disabled = every check passes).
    auth: Arc<crate::auth::Auth>,
}

impl CoordinatorService {
    pub fn new(registry: Arc<RwLock<Registry>>) -> Self {
        Self { registry, control: None, auth: Arc::new(crate::auth::Auth::default()) }
    }

    pub fn with_control(
        registry: Arc<RwLock<Registry>>,
        control: Arc<crate::control::ControlPlane>,
    ) -> Self {
        Self { registry, control: Some(control), auth: Arc::new(crate::auth::Auth::default()) }
    }

    /// Attach a scoped-token policy (from `Auth::from_env`).
    pub fn with_auth(mut self, auth: Arc<crate::auth::Auth>) -> Self {
        self.auth = auth;
        self
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
        self.auth.require(&request, crate::auth::Scope::Read)?;
        let req = request.into_inner();
        if let Some(f) = &req.filter {
            common::filter::validate(f).map_err(Status::invalid_argument)?;
        }
        let limit = req.limit as usize;

        // Snapshot the leader addresses and release the lock BEFORE any await — we must not
        // hold the std RwLock guard across the network fan-out.
        let (leaders, placement_version) = {
            let registry = self
                .registry
                .read()
                .map_err(|_| Status::internal("registry lock poisoned"))?;
            (registry.leader_addresses(), registry.placement_version())
        };
        let shards_queried = leaders.len() as u32;

        // Fan out concurrently, then merge. Missing shards produce partial (not failed)
        // results — coverage and the omission reasons are reported in the manifest.
        let started = std::time::Instant::now();
        let fanout = scatter_gather(leaders, req).await;
        let merged = merge_search_responses(
            fanout,
            limit,
            shards_queried,
            placement_version,
            started.elapsed().as_millis() as u64,
        );

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

    async fn vector_search(
        &self,
        request: Request<common::pb::VectorSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.auth.require(&request, crate::auth::Scope::Read)?;
        let req = request.into_inner();
        if let Some(f) = &req.filter {
            common::filter::validate(f).map_err(Status::invalid_argument)?;
        }
        let limit = req.limit as usize;
        let (leaders, placement_version) = {
            let registry = self
                .registry
                .read()
                .map_err(|_| Status::internal("registry lock poisoned"))?;
            (registry.leader_addresses(), registry.placement_version())
        };
        let shards_queried = leaders.len() as u32;
        let started = std::time::Instant::now();
        let fanout = scatter_gather_vector(leaders, req).await;
        Ok(Response::new(merge_search_responses(
            fanout,
            limit,
            shards_queried,
            placement_version,
            started.elapsed().as_millis() as u64,
        )))
    }

    async fn aggregate(
        &self,
        request: Request<common::pb::AggregateRequest>,
    ) -> Result<Response<common::pb::AggregateResponse>, Status> {
        self.auth.require(&request, crate::auth::Scope::Read)?;
        let req = request.into_inner();
        if let Some(f) = &req.filter {
            common::filter::validate(f).map_err(Status::invalid_argument)?;
        }
        let kind = req.kind();
        let requested = req.percentiles.clone();

        let (leaders, placement_version) = {
            let registry = self
                .registry
                .read()
                .map_err(|_| Status::internal("registry lock poisoned"))?;
            (registry.leader_addresses(), registry.placement_version())
        };
        let shards_queried = leaders.len() as u32;

        let started = std::time::Instant::now();
        let (partials, omitted) = crate::agg::scatter_aggregate(leaders, req).await;
        let shards_answered = partials.len() as u32;
        let (merged, percentiles) = crate::agg::merge_partials(kind, partials, &requested);

        // Aggregations carry the same coverage manifest as search: which shards contributed
        // (partial coverage means the aggregate summarizes only the shards that answered).
        let manifest = crate::fanout::build_manifest(
            &[],
            shards_queried,
            shards_answered,
            omitted,
            0,
            placement_version,
            started.elapsed().as_millis() as u64,
        );

        Ok(Response::new(common::pb::AggregateResponse {
            partial: Some(merged),
            percentiles,
            manifest: Some(manifest),
        }))
    }

    async fn drain_node(
        &self,
        request: Request<DrainRequest>,
    ) -> Result<Response<DrainResponse>, Status> {
        self.auth.require(&request, crate::auth::Scope::Operator)?;
        let bearer = bearer_of(&request);
        let node_id = request.into_inner().node_id;

        // Replicated authority: validate against the LOCAL view (any replica's view knows
        // every live node, since nodes register with all coordinators), then commit the
        // marker through the group so it survives this coordinator's death.
        if let Some(control) = &self.control {
            let known = self
                .registry
                .read()
                .map_err(|_| Status::internal("registry lock poisoned"))?
                .knows_node(&node_id);
            if !known {
                return Ok(Response::new(DrainResponse {
                    ok: false,
                    message: format!("unknown node '{node_id}'"),
                }));
            }
            let cmd = crate::control::Command::DrainNode { node_id: node_id.clone() };
            match control.propose(&cmd).await {
                Ok(()) => {
                    println!("coordinator: '{node_id}' marked draining (replicated)");
                    return Ok(Response::new(DrainResponse {
                        ok: true,
                        message: format!("'{node_id}' marked draining; its group leader will remove it"),
                    }));
                }
                Err(crate::control::ProposeError::Leader(addr)) => {
                    // A follower took the call: forward the whole RPC to the leader.
                    let mut client = common::net::channel(&addr)
                        .await
                        .map(common::pb::coordinator_client::CoordinatorClient::new)
                        .map_err(|e| Status::unavailable(format!("control leader at {addr} unreachable: {e}")))?;
                    return client.drain_node(forward(DrainRequest { node_id }, &bearer)).await;
                }
                Err(crate::control::ProposeError::Unavailable(msg)) => {
                    return Err(Status::unavailable(msg));
                }
            }
        }

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
        request: Request<ClusterStateRequest>,
    ) -> Result<Response<ClusterStateResponse>, Status> {
        self.auth.require(&request, crate::auth::Scope::Read)?;
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
        self.auth.require(&request, crate::auth::Scope::Operator)?;
        let bearer = bearer_of(&request);
        let req = request.into_inner();

        // Replicated authority: validate here (table length + N are the same on every
        // replica), commit through the group, forward to the leader if we're a follower.
        if let Some(control) = &self.control {
            let valid = self
                .registry
                .read()
                .map_err(|_| Status::internal("registry lock poisoned"))?
                .validate_reassign(req.vshard, req.group);
            if let Err(message) = valid {
                return Ok(Response::new(ReassignVShardResponse { ok: false, message }));
            }
            let cmd = crate::control::Command::ReassignVShard { vshard: req.vshard, group: req.group };
            match control.propose(&cmd).await {
                Ok(()) => {
                    println!(
                        "coordinator: vshard {} reassigned to group {} (replicated)",
                        req.vshard, req.group
                    );
                    return Ok(Response::new(ReassignVShardResponse {
                        ok: true,
                        message: format!("vshard {} -> group {}", req.vshard, req.group),
                    }));
                }
                Err(crate::control::ProposeError::Leader(addr)) => {
                    let mut client = common::net::channel(&addr)
                        .await
                        .map(common::pb::coordinator_client::CoordinatorClient::new)
                        .map_err(|e| Status::unavailable(format!("control leader at {addr} unreachable: {e}")))?;
                    return client.reassign_v_shard(forward(req, &bearer)).await;
                }
                Err(crate::control::ProposeError::Unavailable(msg)) => {
                    return Err(Status::unavailable(msg));
                }
            }
        }

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
        self.auth.require(&request, crate::auth::Scope::Read)?;
        let req = request.into_inner();
        let limit = req.limit as usize;

        // Snapshot leaders and release the lock before any await.
        let (leaders, placement_version) = {
            let registry = self
                .registry
                .read()
                .map_err(|_| Status::internal("registry lock poisoned"))?;
            (registry.leader_addresses(), registry.placement_version())
        };
        let shards_queried = leaders.len() as u32;

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            let mut merge = ProgressiveMerge::new(shards_queried, limit, placement_version);

            // Query every leader concurrently under the fan-out deadline; fold + emit an
            // update as each one reports. A slow shard times out like a dead one, so the
            // final `complete` update is bounded too.
            let mut set = JoinSet::new();
            for addr in leaders {
                set.spawn(crate::fanout::query_leader(addr, req.clone()));
            }

            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok(Ok(resp)) => {
                        merge.add(resp);
                        // Client hung up — stop streaming.
                        if tx.send(Ok(merge.snapshot(false))).await.is_err() {
                            return;
                        }
                    }
                    Ok(Err(o)) => merge.omit(o), // omitted shard: recorded for the final manifest
                    Err(_) => {}                 // task panic — treat as absent
                }
            }

            // Final update once every shard has reported (or failed).
            let _ = tx.send(Ok(merge.snapshot(true))).await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
