//! Report cluster topology through the coordinator: shard count, and each node's shard,
//! role (leader/follower), liveness, and drain status.
//!
//!   cargo run -p coordinator --example cluster_state
//!
//! Addresses from AETHER_COORDINATOR_ADDRS (comma-separated, first healthy wins) or
//! AETHER_COORDINATOR_ADDR (default 127.0.0.1:50050).

use common::pb::{ClusterStateRequest, NodeRole};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addrs = common::client::coordinator_addrs("127.0.0.1:50050");
    let mut client = common::client::connect_first_healthy(&addrs).await?;
    let state = client
        .get_cluster_state(common::net::with_token(ClusterStateRequest {}))
        .await?
        .into_inner();

    println!("{} shard group(s)", state.shard_count);
    for node in &state.nodes {
        let role = NodeRole::try_from(node.role).unwrap_or(NodeRole::Unspecified);
        println!(
            "- shard {} · {} · {:?} · seen {:.1}s ago{}",
            node.shard_id,
            node.node_id,
            role,
            node.millis_since_seen as f64 / 1000.0,
            if node.draining { " · draining" } else { "" }
        );
    }
    if !state.vshard_group.is_empty() {
        println!("virtual shards -> groups: {:?}", state.vshard_group);
    }
    Ok(())
}
