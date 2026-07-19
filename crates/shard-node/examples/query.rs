//! Tiny gRPC client for poking a running shard node.
//!
//!   # terminal 1: start a node (ingests live OpenSky, serves on :50051)
//!   cargo run -p shard-node
//!   # terminal 2: query it
//!   cargo run -p shard-node --example query -- germany 5
//!
//! Args: <query> [limit]. Address from AETHER_SHARD_ADDR (default 127.0.0.1:50051).

use common::pb::shard_search_client::ShardSearchClient;
use common::pb::SearchRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::var("AETHER_SHARD_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let query = std::env::args().nth(1).unwrap_or_default();
    let limit: u32 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(5);

    let mut client = ShardSearchClient::new(common::net::channel(&addr).await?);
    let resp = client
        .search(SearchRequest { query: query.clone(), limit })
        .await?
        .into_inner();

    println!(
        "shard '{}' — query '{}' matched {} (showing {}):",
        resp.shard_id, query, resp.total_matched, resp.hits.len()
    );
    for hit in resp.hits {
        let d = hit.document.unwrap_or_default();
        println!("  {:6}  {:9}  {:<20}  score={}", d.icao24, d.callsign, d.origin, hit.score);
    }
    Ok(())
}
