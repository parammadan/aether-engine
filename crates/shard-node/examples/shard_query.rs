//! Query ONE shard node directly (bypassing the coordinator) — a debugging probe for
//! checking what a specific member's own store holds.
//!
//! Args: <addr> [query] [limit]   e.g.  shard_query 10.0.0.5:50051 synthetica 3

use common::pb::shard_search_client::ShardSearchClient;
use common::pb::SearchRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args().nth(1).expect("usage: shard_query <addr> [query] [limit]");
    let query = std::env::args().nth(2).unwrap_or_else(|| "synthetica".to_string());
    let limit: u32 = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(3);

    let mut client = ShardSearchClient::connect(format!("http://{addr}")).await?;
    let resp = client.search(SearchRequest { query, limit }).await?.into_inner();

    println!("total_matched={}", resp.total_matched);
    for hit in resp.hits {
        let d = hit.document.unwrap_or_default();
        println!("  {} {} ({} -> {}) score={:.3}", d.icao24, d.callsign, d.origin, d.destination, hit.score);
    }
    Ok(())
}
