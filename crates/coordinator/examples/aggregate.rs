//! Aggregate across the whole cluster through the coordinator.
//!
//!   cargo run -p coordinator --example aggregate -- value_counts origin
//!   cargo run -p coordinator --example aggregate -- percentiles altitude
//!   cargo run -p coordinator --example aggregate -- count
//!
//! Args: <kind> [field] [interval]. Addresses from AETHER_COORDINATOR_ADDRS /
//! AETHER_COORDINATOR_ADDR (default 127.0.0.1:50050).

use common::pb::{AggKind, AggregateRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    common::net::install_crypto();
    let kind_arg = std::env::args().nth(1).unwrap_or_else(|| "count".to_string());
    let field = std::env::args().nth(2).unwrap_or_default();
    let interval: f64 = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(0.0);

    let kind = match kind_arg.as_str() {
        "count" => AggKind::AggCount,
        "value_counts" => AggKind::AggValueCounts,
        "time_histogram" => AggKind::AggTimeHistogram,
        "numeric_histogram" => AggKind::AggNumericHistogram,
        "geo_grid" => AggKind::AggGeoGrid,
        "percentiles" => AggKind::AggPercentiles,
        other => return Err(format!("unknown kind: {other}").into()),
    };

    let addrs = common::client::coordinator_addrs("127.0.0.1:50050");
    let mut client = common::client::connect_first_healthy(&addrs).await?;
    let resp = client
        .aggregate(common::net::with_token(AggregateRequest {
            query: String::new(),
            kind: kind as i32,
            field,
            interval,
            percentiles: vec![50.0, 90.0, 99.0],
            filter: None,
        }))
        .await?
        .into_inner();

    if let Some(p) = &resp.partial {
        match kind {
            AggKind::AggCount => println!("count: {}", p.count),
            AggKind::AggPercentiles => {
                for pc in &resp.percentiles {
                    println!("p{}: {:.2}", pc.p, pc.value);
                }
            }
            _ => {
                let mut buckets: Vec<(&String, &u64)> = p.buckets.iter().collect();
                buckets.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
                for (k, v) in buckets {
                    println!("{k:<24} {v}");
                }
                println!("({} matched)", p.count);
            }
        }
    }
    if let Some(m) = &resp.manifest {
        println!("provenance: {}", common::client::manifest_summary(m));
    }
    Ok(())
}
