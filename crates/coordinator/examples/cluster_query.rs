//! Query the whole cluster through the coordinator (scatter-gather).
//!
//!   cargo run -p coordinator                                      # start the coordinator
//!   cargo run -p shard-node                                       # start shard node(s)
//!   cargo run -p coordinator --example cluster_query -- united 5  # search across all shards
//!
//! Args: <query> [limit]. Addresses from AETHER_COORDINATOR_ADDRS (comma-separated,
//! first healthy wins) or AETHER_COORDINATOR_ADDR (default 127.0.0.1:50050).

use common::pb::SearchRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addrs = common::client::coordinator_addrs("127.0.0.1:50050");
    let query = std::env::args().nth(1).unwrap_or_default();
    let limit: u32 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(5);

    let mut client = common::client::connect_first_healthy(&addrs).await?;
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
