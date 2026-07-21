//! Closed-loop load generator: fan `concurrency` workers at the coordinator for `duration`
//! seconds, each issuing back-to-back queries, and report throughput (QPS) and latency
//! percentiles (p50/p95/p99). The number a distributed search engine lives or dies by.
//!
//!   cargo run -p coordinator --release --example loadgen -- search 10 32
//!   cargo run -p coordinator --release --example loadgen -- count  10 64
//!
//! Args: <mode: search|count> [duration_secs=10] [concurrency=32]. Build --release for
//! numbers that mean anything. Addresses from AETHER_COORDINATOR_ADDRS / _ADDR.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::pb::{AggKind, AggregateRequest, SearchRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::net::install_crypto();
    let mode = std::env::args().nth(1).unwrap_or_else(|| "search".to_string());
    let duration = Duration::from_secs(std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(10));
    let concurrency: usize = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(32);

    let addrs = common::client::coordinator_addrs("127.0.0.1:50050");
    let client = common::client::connect_first_healthy(&addrs).await?;

    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();

    // One worker per concurrency slot; each records the latency of every request it completes.
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let mut client = client.clone();
        let stop = stop.clone();
        let mode = mode.clone();
        workers.push(tokio::spawn(async move {
            let mut lats: Vec<u64> = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                let ok = match mode.as_str() {
                    "count" => client
                        .aggregate(common::net::with_token(AggregateRequest {
                            query: String::new(),
                            kind: AggKind::AggCount as i32,
                            field: String::new(),
                            interval: 0.0,
                            percentiles: vec![],
                            filter: None,
                        }))
                        .await
                        .is_ok(),
                    _ => client
                        .search(common::net::with_token(SearchRequest {
                            query: "Synthetica".into(),
                            limit: 10,
                            filter: None,
                        }))
                        .await
                        .is_ok(),
                };
                if ok {
                    lats.push(t.elapsed().as_micros() as u64);
                }
            }
            lats
        }));
    }

    tokio::time::sleep(duration).await;
    stop.store(true, Ordering::Relaxed);

    let mut all: Vec<u64> = Vec::new();
    for w in workers {
        all.extend(w.await.unwrap());
    }
    let elapsed = start.elapsed().as_secs_f64();

    if all.is_empty() {
        println!("no successful requests — is the cluster up?");
        return Ok(());
    }
    all.sort_unstable();
    let pct = |p: f64| all[((p / 100.0 * all.len() as f64) as usize).min(all.len() - 1)] as f64 / 1000.0;
    let total = all.len();
    println!("mode={mode} concurrency={concurrency} duration={:.1}s", elapsed);
    println!("requests: {total}");
    println!("throughput: {:.0} QPS", total as f64 / elapsed);
    println!(
        "latency ms: p50={:.2} p95={:.2} p99={:.2} max={:.2}",
        pct(50.0),
        pct(95.0),
        pct(99.0),
        *all.last().unwrap() as f64 / 1000.0
    );
    Ok(())
}
