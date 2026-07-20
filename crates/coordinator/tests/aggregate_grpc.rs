//! Aggregation end to end over real shard services, including honest degradation: a
//! value-counts aggregate merges partials from two shards, and when one shard dies the
//! merged result covers only the survivor and the manifest names the omission — no error.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::{AggKind, AggregateRequest, FlightDocument, NodeRole, RegisterNodeRequest};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use shard_node::server::ShardSearchService;
use shard_node::store::ShardStore;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn doc(icao24: &str, origin: &str) -> FlightDocument {
    FlightDocument { icao24: icao24.to_string(), origin: origin.to_string(), ..Default::default() }
}

async fn start_shard(label: &str, docs: Vec<FlightDocument>) -> (String, tokio::task::JoinHandle<()>) {
    let store = Arc::new(RwLock::new(ShardStore::new()));
    {
        let mut s = store.write().unwrap();
        for d in docs {
            s.insert(d);
        }
    }
    let service = ShardSearchService::new(store, label.to_string());
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

fn value_counts(field: &str) -> AggregateRequest {
    AggregateRequest {
        query: String::new(),
        kind: AggKind::AggValueCounts as i32,
        field: field.to_string(),
        interval: 0.0,
        percentiles: vec![],
        filter: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_merges_partials_across_shards() {
    let (s0, _h0) = start_shard("shard-0", vec![doc("a", "SFO"), doc("b", "SFO"), doc("c", "JFK")]).await;
    let (s1, _h1) = start_shard("shard-1", vec![doc("d", "SFO"), doc("e", "LAX")]).await;

    let registry = Arc::new(RwLock::new(Registry::new(2)));
    register(&registry, 0, &s0);
    register(&registry, 1, &s1);
    let mut client = start_coordinator(registry).await;

    let resp = client.aggregate(value_counts("origin")).await.unwrap().into_inner();
    let p = resp.partial.unwrap();
    assert_eq!(p.count, 5, "all five docs counted across both shards");
    // SFO: 2 on shard-0 + 1 on shard-1 = 3; JFK: 1; LAX: 1.
    assert_eq!(p.buckets.get("SFO"), Some(&3), "per-bucket sum merged across shards");
    assert_eq!(p.buckets.get("JFK"), Some(&1));
    assert_eq!(p.buckets.get("LAX"), Some(&1));

    let m = resp.manifest.unwrap();
    assert_eq!(m.shards_answered, 2);
    assert!(m.omitted.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_shard_degrades_the_aggregate_honestly() {
    let (s0, _h0) = start_shard("shard-0", vec![doc("a", "SFO"), doc("b", "JFK")]).await;
    let (s1, h1) = start_shard("shard-1", vec![doc("c", "SFO"), doc("d", "LAX")]).await;

    let registry = Arc::new(RwLock::new(Registry::new(2)));
    register(&registry, 0, &s0);
    register(&registry, 1, &s1);
    let mut client = start_coordinator(registry).await;

    // Kill shard 1 before aggregating.
    h1.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let resp = client.aggregate(value_counts("origin")).await.unwrap().into_inner();
    let p = resp.partial.unwrap();
    // Only shard-0's two docs are counted — the aggregate covers what answered.
    assert_eq!(p.count, 2, "aggregate reflects only the surviving shard");
    assert_eq!(p.buckets.get("SFO"), Some(&1));
    assert_eq!(p.buckets.get("JFK"), Some(&1));

    let m = resp.manifest.unwrap();
    assert_eq!(m.shards_queried, 2);
    assert_eq!(m.shards_answered, 1, "one shard answered");
    assert_eq!(m.omitted.len(), 1, "the dead shard is named in the aggregate's manifest");
    assert_eq!(m.omitted[0].address, s1);
    assert_eq!(m.omitted[0].reason, "unreachable");
}
