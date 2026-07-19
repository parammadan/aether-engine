//! Client-side coordinator endpoints (dashboard, examples, agents): parse an address
//! list and connect to the first coordinator that answers, in preference order.
//!
//! Clients are pure readers of the control plane, so unlike nodes (which must prove
//! their liveness to every coordinator) a client only ever needs ONE that works —
//! try-next-on-error is the whole failover story.

use crate::pb::coordinator_client::CoordinatorClient;
use tonic::transport::Channel;

/// Split a comma-separated address list; falls back to `default` if it comes up empty.
pub fn parse_addr_list(raw: &str, default: &str) -> Vec<String> {
    let list: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    if list.is_empty() {
        vec![default.to_string()]
    } else {
        list
    }
}

/// Coordinator addresses from the environment: `AETHER_COORDINATOR_ADDRS` (list) wins,
/// the singular `AETHER_COORDINATOR_ADDR` still works, `default` covers neither.
pub fn coordinator_addrs(default: &str) -> Vec<String> {
    let raw = std::env::var("AETHER_COORDINATOR_ADDRS")
        .or_else(|_| std::env::var("AETHER_COORDINATOR_ADDR"))
        .unwrap_or_else(|_| default.to_string());
    parse_addr_list(&raw, default)
}

/// Connect to the first coordinator in the list that accepts the connection.
pub async fn connect_first_healthy(
    addrs: &[String],
) -> Result<CoordinatorClient<Channel>, String> {
    for addr in addrs {
        if let Ok(client) = CoordinatorClient::connect(format!("http://{addr}")).await {
            return Ok(client);
        }
    }
    Err(format!("no coordinator reachable (tried: {})", addrs.join(", ")))
}
