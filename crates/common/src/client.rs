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

/// A one-line, human-readable summary of a query manifest, for CLIs and agents: coverage,
/// what was dropped and why, dedup count, freshness age, and the placement version. The
/// same auditable facts a stranger would need to trust the result.
pub fn manifest_summary(m: &crate::pb::QueryManifest) -> String {
    let mut parts = vec![format!("answered {}/{} shards", m.shards_answered, m.shards_queried)];
    if !m.omitted.is_empty() {
        let omitted: Vec<String> =
            m.omitted.iter().map(|o| format!("{} ({})", o.address, o.reason)).collect();
        parts.push(format!("omitted: {}", omitted.join(", ")));
    }
    if m.deduped > 0 {
        parts.push(format!("{} cross-shard duplicates dropped", m.deduped));
    }
    if m.freshest_observed_at > 0 {
        let age_ms = now_ms().saturating_sub(m.freshest_observed_at);
        parts.push(format!("freshest {}s old", age_ms / 1000));
    }
    parts.push(format!("placement v{}", m.placement_version));
    parts.push(format!("{}ms", m.elapsed_ms));
    parts.join(" · ")
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Parse CLI filter arguments into a wire Filter: `field=value` (text equals),
/// `field=min..max` (numeric range; either side may be empty), `field=true|false`
/// (boolean). Returns None for an empty list.
pub fn parse_filter_args(args: &[String]) -> Result<Option<crate::pb::Filter>, String> {
    use crate::pb::{filter_condition::Test, Filter, FilterCondition, NumericRange};
    let mut conditions = Vec::new();
    for arg in args {
        let (field, value) = arg
            .split_once('=')
            .ok_or_else(|| format!("filter '{arg}' must look like field=value or field=min..max"))?;
        let test = if let Some((lo, hi)) = value.split_once("..") {
            let parse = |s: &str| -> Result<Option<f64>, String> {
                if s.is_empty() {
                    Ok(None)
                } else {
                    s.parse().map(Some).map_err(|_| format!("'{s}' is not a number in '{arg}'"))
                }
            };
            Test::Range(NumericRange { min: parse(lo)?, max: parse(hi)? })
        } else if value == "true" || value == "false" {
            Test::Is(value == "true")
        } else {
            Test::Equals(value.to_string())
        };
        conditions.push(FilterCondition { field: field.to_string(), test: Some(test) });
    }
    Ok(if conditions.is_empty() { None } else { Some(crate::pb::Filter { conditions }) })
}

/// Connect to the first coordinator in the list that accepts the connection.
pub async fn connect_first_healthy(
    addrs: &[String],
) -> Result<CoordinatorClient<Channel>, String> {
    for addr in addrs {
        if let Ok(channel) = crate::net::channel(addr).await {
            return Ok(CoordinatorClient::new(channel));
        }
    }
    Err(format!("no coordinator reachable (tried: {})", addrs.join(", ")))
}
