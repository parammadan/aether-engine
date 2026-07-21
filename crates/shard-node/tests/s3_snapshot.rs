//! Cold-boot from the S3 snapshot tier: a node ingests, snapshots (uploaded to S3), is
//! killed, has its ENTIRE local state wiped, and restarts with ingestion off — anything
//! in its store afterwards can only have come from the S3 object. This is the
//! disaster-recovery path, and it runs against any S3-compatible endpoint (MinIO in CI),
//! so it is exercised on every push rather than only in a gated live run.
//!
//! Skips (loudly) when AETHER_S3_ENDPOINT is unset — local runs without an S3 stand-in.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_client::ShardSearchClient;
use common::pb::SearchRequest;
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

struct Cluster {
    children: HashMap<String, Child>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn start_coordinator() -> String {
    let registry = Arc::new(RwLock::new(Registry::new(1)));
    let service = CoordinatorService::new(registry);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(CoordinatorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    addr
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

#[allow(clippy::too_many_arguments)]
fn spawn_node(
    coordinator: &str,
    port: u16,
    data_dir: &PathBuf,
    s3_prefix: &str,
    ingest: bool,
) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_shard-node"));
    cmd.env("AETHER_NODE_ID", "s3-node")
        .env("AETHER_SHARD_ADDR", format!("127.0.0.1:{port}"))
        .env("AETHER_SHARD_INDEX", "0")
        .env("AETHER_SHARD_COUNT", "1")
        .env("AETHER_CONSENSUS", "raft")
        .env("AETHER_GROUP_SIZE", "1") // single-member group: quorum of one, snapshots fast
        .env("AETHER_COORDINATOR_ADDR", coordinator)
        .env("AETHER_HEARTBEAT_SECS", "1")
        .env("AETHER_SOURCE", "synthetic")
        .env("AETHER_POLL_SECS", "1")
        .env("AETHER_SNAPSHOT_LOGS", "5") // snapshot every ~5 log entries
        .env("AETHER_DATA_DIR", data_dir)
        .env("AETHER_S3_BUCKET", std::env::var("AETHER_S3_BUCKET").unwrap_or_else(|_| "aether-test".into()))
        .env("AETHER_S3_PREFIX", s3_prefix)
        .env("AETHER_INGEST", if ingest { "on" } else { "off" })
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn().expect("spawn shard-node")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_boot_recovers_the_store_from_s3_after_total_local_loss() {
    if std::env::var("AETHER_S3_ENDPOINT").is_err() {
        eprintln!("SKIPPED: set AETHER_S3_ENDPOINT (e.g. MinIO) to exercise the S3 tier");
        return;
    }

    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("s3-snap-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let data_dir = root.join("node");
    // Unique prefix per run so reruns never read a previous run's snapshots.
    let prefix = format!(
        "test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );

    let coordinator = start_coordinator().await;
    let port = free_port();
    let mut cluster = Cluster { children: HashMap::new() };
    cluster
        .children
        .insert("first".into(), spawn_node(&coordinator, port, &data_dir, &prefix, true));

    // Ingest until well past the snapshot threshold (snapshot every ~5 entries).
    let endpoint = format!("http://127.0.0.1:{port}");
    let mut direct = loop {
        if let Ok(c) = ShardSearchClient::connect(endpoint.clone()).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut before_kill = 0u64;
    for _ in 0..120 {
        before_kill = direct
            .search(SearchRequest { query: "synthetica".into(), limit: 1, filter: None })
            .await
            .map(|r| r.into_inner().total_matched)
            .unwrap_or(0);
        if before_kill >= 50 {
            break; // ≥10 batches -> at least one snapshot built and uploaded
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(before_kill >= 50, "never reached snapshot volume (got {before_kill})");
    // Give the snapshot build+upload a beat to complete past the threshold.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- Kill, then destroy ALL local state: the disaster scenario ---
    let mut child = cluster.children.remove("first").unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    std::fs::remove_dir_all(&data_dir).unwrap();

    // --- Restart with ingestion OFF: anything it now holds came from S3 ---
    cluster
        .children
        .insert("second".into(), spawn_node(&coordinator, port, &data_dir, &prefix, false));
    let mut direct = loop {
        if let Ok(c) = ShardSearchClient::connect(endpoint.clone()).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut recovered = 0u64;
    for _ in 0..60 {
        recovered = direct
            .search(SearchRequest { query: "synthetica".into(), limit: 1, filter: None })
            .await
            .map(|r| r.into_inner().total_matched)
            .unwrap_or(0);
        if recovered > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        recovered >= 5,
        "cold boot recovered {recovered} docs from S3; expected at least one snapshot's worth"
    );

    // And it recovered the ORIGINAL documents, not regenerated ones: with ingestion off,
    // a specific early synthetic callsign can only exist via the snapshot.
    let syn0 = direct
        .search(SearchRequest { query: "SYN0".into(), limit: 1, filter: None })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(syn0.total_matched, 1, "the first ingested document must survive the disaster");

    let _ = std::fs::remove_dir_all(&root);
}
