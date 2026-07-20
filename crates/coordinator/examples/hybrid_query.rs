//! Hybrid search across the cluster: keyword + vector, fused by RRF at the coordinator.
//!
//!   cargo run -p coordinator --example hybrid_query -- "united boeing" 5
//!
//! Args: <query> [limit] [field=value ...]. Addresses from AETHER_COORDINATOR_ADDRS /
//! AETHER_COORDINATOR_ADDR (default 127.0.0.1:50050).

use common::pb::SearchRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    common::net::install_crypto();
    let query = std::env::args().nth(1).unwrap_or_default();
    let limit: u32 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(5);
    let filter = common::client::parse_filter_args(&std::env::args().skip(3).collect::<Vec<_>>())?;

    let addrs = common::client::coordinator_addrs("127.0.0.1:50050");
    let mut client = common::client::connect_first_healthy(&addrs).await?;
    let resp = client
        .hybrid_search(common::net::with_token(SearchRequest { query: query.clone(), limit, filter }))
        .await?
        .into_inner();

    println!("hybrid (RRF) — '{}' — {} fused hits:", query, resp.hits.len());
    for hit in &resp.hits {
        let d = hit.document.clone().unwrap_or_default();
        let how = match hit.provenance.as_ref().map(|p| p.index()) {
            Some(common::pb::IndexKind::IndexKeyword) => "keyword",
            Some(common::pb::IndexKind::IndexVector) => "vector",
            _ => "?",
        };
        println!("  {:6}  {:9}  {:<18}  rrf={:.4}  via {how}", d.icao24, d.callsign, d.origin, hit.score);
    }
    if let Some(m) = &resp.manifest {
        println!("provenance: {}", common::client::manifest_summary(m));
    }
    Ok(())
}
