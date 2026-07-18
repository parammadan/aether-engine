//! Talking to the coordinator control plane: register this node's shard on startup so the
//! coordinator can discover it and route/fan-out to it.

use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::{NodeRole, RegisterNodeRequest};

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
