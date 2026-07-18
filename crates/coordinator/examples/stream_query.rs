//! Stream a query through the coordinator and watch results converge as shards report.
//!
//!   cargo run -p coordinator                                       # start the coordinator
//!   cargo run -p shard-node                                        # start shard node(s)
//!   cargo run -p coordinator --example stream_query -- united 5    # streaming search
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
    let mut stream = client
        .search_stream(SearchRequest { query: query.clone(), limit })
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
