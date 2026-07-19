//! Talking to the coordinator control plane: register this node's shard on startup so the
//! coordinator can discover it and route/fan-out to it.
//!
//! The control plane may be more than one coordinator. The rule that keeps that simple:
//! **writes fan out, reads fail over.** Registration and heartbeats go to EVERY
//! coordinator (each one's view of the cluster is rebuilt independently, so each must
//! hear from us directly); lookups (group membership, vshard table) take the first
//! coordinator that answers, in list order.

use std::sync::Arc;
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::{HeartbeatRequest, NodeRole, RegisterNodeRequest};

pub type ClusterError = Box<dyn std::error::Error + Send + Sync>;

/// The coordinator endpoints this node talks to, in preference order.
#[derive(Clone, Debug)]
pub struct Coordinators(Arc<Vec<String>>);

impl Coordinators {
    pub fn new(addrs: Vec<String>) -> Option<Self> {
        if addrs.is_empty() {
            None
        } else {
            Some(Self(Arc::new(addrs)))
        }
    }

    /// `AETHER_COORDINATOR_ADDRS` (comma-separated, preference order) wins; the singular
    /// `AETHER_COORDINATOR_ADDR` still works so nothing existing has to change.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var("AETHER_COORDINATOR_ADDRS")
            .or_else(|_| std::env::var("AETHER_COORDINATOR_ADDR"))
            .ok()?;
        Self::new(raw.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
    }

    pub fn addrs(&self) -> &[String] {
        &self.0
    }

    /// First coordinator that accepts a connection, in list order — the read path.
    /// One pass, no retries: pollers call this every tick, so persistence lives there.
    pub async fn first_healthy(
        &self,
    ) -> Option<CoordinatorClient<tonic::transport::Channel>> {
        for addr in self.0.iter() {
            if let Ok(c) = CoordinatorClient::connect(format!("http://{addr}")).await {
                return Some(c);
            }
        }
        None
    }

    /// Register this node with EVERY coordinator. Succeeds if at least one accepted —
    /// a down replica catches up later via the heartbeat re-register path.
    pub async fn register_all(
        &self,
        node_id: &str,
        address: &str,
        shard_id: u32,
        role: NodeRole,
    ) -> Result<u32, ClusterError> {
        let mut cluster_size = None;
        for addr in self.0.iter() {
            match register_with_coordinator(addr, node_id, address, shard_id, role).await {
                Ok(n) => cluster_size = Some(n),
                Err(e) => eprintln!("warning: could not register with coordinator at {addr}: {e}"),
            }
        }
        cluster_size.ok_or_else(|| "no coordinator accepted the registration".into())
    }
}

/// Register this node with the coordinator, retrying the initial connect since the
/// coordinator may still be coming up. Returns the coordinator's reported cluster size (N)
/// on success, or an error if the coordinator rejects the registration or never appears.
pub async fn register_with_coordinator(
    coordinator_addr: &str,
    node_id: &str,
    address: &str,
    shard_id: u32,
    role: NodeRole,
) -> Result<u32, ClusterError> {
    let endpoint = format!("http://{coordinator_addr}");

    // Retry connect: nodes and coordinator start independently, in any order.
    let mut client = {
        let mut last_err = None;
        let mut client = None;
        for _ in 0..20 {
            match CoordinatorClient::connect(endpoint.clone()).await {
                Ok(c) => {
                    client = Some(c);
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
        client.ok_or_else(|| last_err.expect("loop ran at least once"))?
    };

    let resp = client
        .register_node(RegisterNodeRequest {
            node_id: node_id.to_string(),
            address: address.to_string(),
            shard_id,
            role: role as i32,
        })
        .await?
        .into_inner();

    if !resp.accepted {
        return Err(format!("coordinator rejected registration: {}", resp.message).into());
    }
    Ok(resp.cluster_size)
}

/// Periodically heartbeat the coordinator so it knows this node is alive. Runs forever; a
/// failed heartbeat is logged and retried on the next tick (a transient coordinator blip
/// must not take the node down).
///
/// Two authority models, depending on `raft`:
/// - **Legacy (raft = None):** the node tracks its *current* role, seeded from
///   `initial_role` but updated from every heartbeat response — the coordinator's promotion
///   is authoritative, and on a coordinator restart the node re-registers with the TRACKED
///   role, not the boot-time one.
/// - **Consensus-managed (raft = Some):** authority is inverted. The node reports whether
///   it currently leads its shard's raft group (read from raft metrics each tick), and
///   IGNORES the coordinator's role opinion — elections happen in the group, and the
///   coordinator's map is just a routing view kept fresh by these reports.
pub async fn run_heartbeat(
    coordinators: Coordinators,
    node_id: String,
    address: String,
    shard_id: u32,
    initial_role: NodeRole,
    interval: Duration,
    raft: Option<(crate::raft::Raft, u64)>,
) {
    let mut current_role = initial_role;
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;

        let raft_leader = raft
            .as_ref()
            .map(|(r, my_id)| r.metrics().borrow().current_leader == Some(*my_id))
            .unwrap_or(false);

        // EVERY coordinator hears the beat: each replica's liveness view is its own, so
        // proving aliveness to one says nothing to the others. A dead replica just skips
        // its slot this tick.
        for coordinator_addr in coordinators.addrs() {
            let Ok(mut client) =
                CoordinatorClient::connect(format!("http://{coordinator_addr}")).await
            else {
                continue; // this coordinator unreachable this tick; the rest still get theirs
            };

            match client
                .heartbeat(HeartbeatRequest { node_id: node_id.clone(), raft_leader })
                .await
            {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if resp.known {
                        // Legacy mode only: adopt the coordinator's view of our role. Under
                        // raft, leadership comes from the group, not the coordinator.
                        if raft.is_none() {
                            match resp.current_role() {
                                NodeRole::Unspecified => {}
                                role => {
                                    if role != current_role {
                                        println!("node '{node_id}': role is now {role:?} (was {current_role:?})");
                                    }
                                    current_role = role;
                                }
                            }
                        }
                    } else {
                        // This coordinator forgot us (restart, or it was down when we
                        // booted); re-register with who we ARE now, not who we were at boot.
                        let role = if raft.is_some() { NodeRole::Follower } else { current_role };
                        let _ = register_with_coordinator(
                            coordinator_addr,
                            &node_id,
                            &address,
                            shard_id,
                            role,
                        )
                        .await;
                    }
                }
                Err(e) => eprintln!("heartbeat to {coordinator_addr} failed: {e}"),
            }
        }
    }
}
