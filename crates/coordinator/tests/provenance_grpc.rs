//! Provenance end to end: real shard search services behind a coordinator, so every hit
//! carries a block the SHARD built and every response carries a manifest the COORDINATOR
//! assembled. A degraded query (one shard down) must say exactly what it dropped and why.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::{FlightDocument, IndexKind, NodeRole, RegisterNodeRequest, SearchRequest};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use shard_node::server::ShardSearchService;
use shard_node::store::ShardStore;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn doc(icao24: &str, callsign: &str, observed_at: i64) -> FlightDocument {
    FlightDocument {
        icao24: icao24.to_string(),
        callsign: callsign.to_string(),
        origin: "Synthetica".to_string(),
        observed_at,
        ..Default::default()
    }
}

/// A real shard search service over a store seeded with `docs`, on an ephemeral port.
async fn start_real_shard(shard_label: &str, docs: Vec<FlightDocument>) -> (String, tokio::task::JoinHandle<()>) {
    let store = ShardStore::new();
    let store = Arc::new(RwLock::new(store));
    {
        let mut s = store.write().unwrap();
        for d in docs {
            s.insert(d);
        }
    }
    let service = ShardSearchService::new(store, shard_label.to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(ShardSearchServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (addr, handle)
}

async fn start_coordinator(registry: Arc<RwLock<Registry>>) -> CoordinatorClient<tonic::transport::Channel> {
    let service = CoordinatorService::new(registry);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(CoordinatorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    loop {
        if let Ok(c) = CoordinatorClient::connect(format!("http://{addr}")).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn register(registry: &Arc<RwLock<Registry>>, shard_id: u32, addr: &str) {
    registry.write().unwrap().register(RegisterNodeRequest {
        node_id: format!("n{shard_id}"),
        address: addr.to_string(),
        shard_id,
        role: NodeRole::Leader as i32,
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_hit_carries_provenance_and_the_manifest_reports_coverage() {
    let t0 = now_ms();
    let (s0, _h0) = start_real_shard("shard-0", vec![doc("aaa", "SYN1", t0 - 5000), doc("bbb", "SYN2", t0 - 1000)]).await;
    let (s1, _h1) = start_real_shard("shard-1", vec![doc("ccc", "SYN3", t0 - 200)]).await;

    let registry = Arc::new(RwLock::new(Registry::new(2)));
    register(&registry, 0, &s0);
    register(&registry, 1, &s1);
    let mut client = start_coordinator(registry).await;

    let resp = client
        .search(SearchRequest { query: "synthetica".into(), limit: 10, filter: None })
        .await
        .unwrap()
        .into_inner();

    // Every hit carries a complete provenance block, built at its shard.
    assert!(!resp.hits.is_empty());
    for hit in &resp.hits {
        let p = hit.provenance.as_ref().expect("every hit carries provenance");
        assert!(p.source_group == "shard-0" || p.source_group == "shard-1", "names its shard");
        assert_eq!(p.index, IndexKind::IndexKeyword as i32, "keyword path");
        assert!(p.observed_at > 0, "freshness stamped");
        assert_eq!(p.owning_vshard, -1, "no virtual-shard placement in this test");
    }

    // The manifest reports full coverage and a sane freshness envelope.
    let m = resp.manifest.as_ref().expect("merged response carries a manifest");
    assert_eq!(m.shards_queried, 2);
    assert_eq!(m.shards_answered, 2);
    assert!(m.omitted.is_empty(), "nothing omitted when all shards answer");
    assert!(m.freshest_observed_at >= m.stalest_observed_at && m.stalest_observed_at > 0);
    assert!(m.freshest_observed_at >= t0 - 200, "freshest is the newest observation");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_shard_is_named_in_the_manifest_with_a_reason() {
    let t0 = now_ms();
    let (s0, _h0) = start_real_shard("shard-0", vec![doc("aaa", "SYN1", t0)]).await;
    let (s1, h1) = start_real_shard("shard-1", vec![doc("ccc", "SYN3", t0)]).await;

    let registry = Arc::new(RwLock::new(Registry::new(2)));
    register(&registry, 0, &s0);
    register(&registry, 1, &s1);
    let mut client = start_coordinator(registry).await;

    // Kill shard 1: its listener closes, so the coordinator's dial is refused → unreachable.
    h1.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let resp = client
        .search(SearchRequest { query: "synthetica".into(), limit: 10, filter: None })
        .await
        .unwrap()
        .into_inner();

    let m = resp.manifest.as_ref().expect("manifest present even when degraded");
    assert_eq!(m.shards_queried, 2);
    assert_eq!(m.shards_answered, 1, "one shard answered");
    assert_eq!(m.omitted.len(), 1, "the dead shard is named, not silently dropped");
    assert_eq!(m.omitted[0].address, s1, "names WHICH shard");
    assert_eq!(m.omitted[0].reason, "unreachable", "and WHY");

    // The surviving hits still carry full provenance — degradation doesn't corrupt them.
    assert!(resp.hits.iter().all(|h| h.provenance.is_some()));
}
