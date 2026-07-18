//! Query the whole cluster through the coordinator (scatter-gather).
//!
//!   cargo run -p coordinator                                      # start the coordinator
//!   cargo run -p shard-node                                       # start shard node(s)
//!   cargo run -p coordinator --example cluster_query -- united 5  # search across all shards
//!
//! Args: <query> [limit]. Address from AETHER_COORDINATOR_ADDR (default 127.0.0.1:50050).

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::SearchRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("AETHER_COORDINATOR_ADDR").unwrap_or_else(|_| "127.0.0.1:50050".to_string());
    let query = std::env::args().nth(1).unwrap_or_default();
    let limit: u32 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(5);

    let mut client = CoordinatorClient::connect(format!("http://{addr}")).await?;
    let resp = client
        .search(SearchRequest { query: query.clone(), limit })
        .await?
        .into_inner();

    println!(
        "cluster — query '{}' matched {} across {}/{} shards (showing {}):",
        query, resp.total_matched, resp.shards_answered, resp.shards_queried, resp.hits.len()
    );
    for hit in resp.hits {
        let d = hit.document.unwrap_or_default();
        println!("  {:6}  {:9}  {:<20}  score={}", d.icao24, d.callsign, d.origin, hit.score);
    }
    Ok(())
}
