//! Talking to the coordinator control plane: register this node's shard on startup so the
//! coordinator can discover it and route/fan-out to it.

use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::{HeartbeatRequest, NodeRole, RegisterNodeRequest};

pub type ClusterError = Box<dyn std::error::Error + Send + Sync>;

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
/// The node tracks its *current* role, seeded from `initial_role` but updated from every
/// heartbeat response: if the coordinator promoted this node (follower -> leader on
/// failover), the next heartbeat teaches it. When the coordinator reports the node unknown
/// (e.g. the coordinator restarted and lost its shard map), the node re-registers with the
/// TRACKED role — not the boot-time one — so a restart can't silently demote a promoted
/// leader back to follower.
pub async fn run_heartbeat(
    coordinator_addr: String,
    node_id: String,
    address: String,
    shard_id: u32,
    initial_role: NodeRole,
    interval: Duration,
) {
    let mut current_role = initial_role;
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let Ok(mut client) = CoordinatorClient::connect(format!("http://{coordinator_addr}")).await
        else {
            continue; // coordinator unreachable this tick; try again next tick
        };
        match client.heartbeat(HeartbeatRequest { node_id: node_id.clone() }).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                if resp.known {
                    // Adopt the coordinator's view of our role (ignore Unspecified).
                    match resp.current_role() {
                        NodeRole::Unspecified => {}
                        role => {
                            if role != current_role {
                                println!("node '{node_id}': role is now {role:?} (was {current_role:?})");
                            }
                            current_role = role;
                        }
                    }
                } else {
                    // Coordinator forgot us; re-register with who we ARE now, not who we
                    // were at boot.
                    let _ = register_with_coordinator(
                        &coordinator_addr,
                        &node_id,
                        &address,
                        shard_id,
                        current_role,
                    )
                    .await;
                }
            }
            Err(e) => eprintln!("heartbeat failed: {e}"),
        }
    }
}
