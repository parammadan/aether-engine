//! Stream a query through the coordinator and watch results converge as shards report.
//!
//!   cargo run -p coordinator                                       # start the coordinator
//!   cargo run -p shard-node                                        # start shard node(s)
//!   cargo run -p coordinator --example stream_query -- united 5    # streaming search
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
    let mut stream = client
        .search_stream(common::net::with_token(SearchRequest { query: query.clone(), limit, filter: None }))
        .await?
        .into_inner();

    while let Some(update) = stream.message().await? {
        let tag = if update.complete { "final " } else { "update" };
        println!(
            "[{tag}] {}/{} shards, {} matches, top {}:",
            update.shards_answered, update.shards_queried, update.total_matched, update.hits.len()
        );
        for hit in &update.hits {
            let d = hit.document.clone().unwrap_or_default();
            println!("    {:6}  {:9}  {:<18}  score={}", d.icao24, d.callsign, d.origin, hit.score);
        }
    }
    Ok(())
}
