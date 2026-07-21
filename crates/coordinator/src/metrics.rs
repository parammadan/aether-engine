//! Prometheus-format operational metrics for the coordinator.
//!
//! Counters and a latency histogram for the query path, plus cluster gauges read from the
//! registry at scrape time. Exposed over a tiny dependency-free HTTP server on a separate
//! port (`AETHER_METRICS_ADDR`, default 127.0.0.1:9090) so `curl :9090/metrics` — or a
//! Prometheus scraper — sees live counts without touching the gRPC data plane.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::registry::Registry;

/// Upper bounds (ms) for the latency histogram buckets; the final +Inf bucket is implicit
/// in the total count.
const LATENCY_BUCKETS_MS: [u64; 6] = [1, 5, 10, 50, 100, 500];

#[derive(Default)]
pub struct Metrics {
    queries_total: AtomicU64,
    query_errors_total: AtomicU64,
    aggregates_total: AtomicU64,
    aggregate_errors_total: AtomicU64,
    /// Cumulative query latency in ms (for an average alongside the histogram).
    query_latency_ms_sum: AtomicU64,
    /// Cumulative counts per latency bucket (le 1,5,10,50,100,500 ms).
    latency_buckets: [AtomicU64; 6],
}

impl Metrics {
    /// Record one completed search: latency into the histogram, and an error tick if it failed.
    pub fn record_query(&self, elapsed: Duration, ok: bool) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.query_errors_total.fetch_add(1, Ordering::Relaxed);
        }
        let ms = elapsed.as_millis() as u64;
        self.query_latency_ms_sum.fetch_add(ms, Ordering::Relaxed);
        for (i, &bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if ms <= bound {
                self.latency_buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record one completed aggregate.
    pub fn record_aggregate(&self, ok: bool) {
        self.aggregates_total.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.aggregate_errors_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Render the current values as a Prometheus text-format exposition. Cluster gauges
    /// (node/shard counts) are read from the registry at scrape time.
    fn render(&self, registry: &Arc<RwLock<Registry>>) -> String {
        let (leaders, shards) = registry
            .read()
            .map(|r| (r.leaders_registered(), r.shard_count()))
            .unwrap_or((0, 0));
        let mut out = String::with_capacity(1024);
        let counter = |out: &mut String, name: &str, help: &str, v: u64| {
            let _ = write!(out, "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}\n");
        };
        counter(&mut out, "aether_queries_total", "Search queries handled by the coordinator.", self.queries_total.load(Ordering::Relaxed));
        counter(&mut out, "aether_query_errors_total", "Search queries that returned an error.", self.query_errors_total.load(Ordering::Relaxed));
        counter(&mut out, "aether_aggregates_total", "Aggregate queries handled.", self.aggregates_total.load(Ordering::Relaxed));
        counter(&mut out, "aether_aggregate_errors_total", "Aggregate queries that returned an error.", self.aggregate_errors_total.load(Ordering::Relaxed));

        // Latency histogram (cumulative buckets, Prometheus convention).
        let total = self.queries_total.load(Ordering::Relaxed);
        let _ = write!(out, "# HELP aether_query_latency_ms Search latency in milliseconds.\n# TYPE aether_query_latency_ms histogram\n");
        for (i, bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            let c = self.latency_buckets[i].load(Ordering::Relaxed);
            let _ = write!(out, "aether_query_latency_ms_bucket{{le=\"{bound}\"}} {c}\n");
        }
        let _ = write!(out, "aether_query_latency_ms_bucket{{le=\"+Inf\"}} {total}\n");
        let _ = write!(out, "aether_query_latency_ms_sum {}\n", self.query_latency_ms_sum.load(Ordering::Relaxed));
        let _ = write!(out, "aether_query_latency_ms_count {total}\n");

        // Cluster gauges.
        let _ = write!(out, "# HELP aether_cluster_leaders Shard leaders currently registered.\n# TYPE aether_cluster_leaders gauge\naether_cluster_leaders {leaders}\n");
        let _ = write!(out, "# HELP aether_cluster_shards Configured shard count.\n# TYPE aether_cluster_shards gauge\naether_cluster_shards {shards}\n");
        out
    }
}

/// Serve `/metrics` over a minimal HTTP/1.1 responder — no framework, one endpoint. Any
/// request gets the current exposition; the connection closes after one response.
pub async fn serve(addr: std::net::SocketAddr, metrics: Arc<Metrics>, registry: Arc<RwLock<Registry>>) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("metrics: could not bind {addr}: {e}");
            return;
        }
    };
    println!("metrics: Prometheus exposition on http://{addr}/metrics");
    loop {
        let Ok((mut sock, _)) = listener.accept().await else { continue };
        let metrics = metrics.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            // Drain the request line/headers (we ignore them — one endpoint).
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            let body = metrics.render(&registry);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.shutdown().await;
        });
    }
}
