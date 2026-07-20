//! Federation across two independent clusters: a federated query merges both clusters'
//! results, and when one cluster is down the query returns the survivor's data with the
//! dead cluster NAMED in the coverage manifest — the same honest degradation the
//! coordinator does over shards, one level up.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::{FlightDocument, NodeRole, RegisterNodeRequest, SearchRequest};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use federation::federate_search;
use shard_node::server::ShardSearchService;
use shard_node::store::ShardStore;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Start one full cluster (coordinator + a shard holding `docs`); return the coordinator
/// address and a handle that, when aborted, takes the whole cluster offline.
async fn start_cluster(label: &str, docs: Vec<FlightDocument>) -> (String, Vec<tokio::task::JoinHandle<()>>) {
    let store = Arc::new(RwLock::new(ShardStore::new()));
    {
        let mut s = store.write().unwrap();
        for d in docs {
            s.insert(d);
        }
    }
    let shard = ShardSearchService::new(store, format!("{label}-shard"));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let shard_addr = l.local_addr().unwrap().to_string();
    let h1 = tokio::spawn(async move {
        let _ = Server::builder().add_service(ShardSearchServer::new(shard))
            .serve_with_incoming(TcpListenerStream::new(l)).await;
    });
    let registry = Arc::new(RwLock::new(Registry::new(1)));
    registry.write().unwrap().register(RegisterNodeRequest {
        node_id: format!("{label}-n0"), address: shard_addr, shard_id: 0, role: NodeRole::Leader as i32,
    });
    let svc = CoordinatorService::new(registry);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coord = l.local_addr().unwrap().to_string();
    let h2 = tokio::spawn(async move {
        let _ = Server::builder().add_service(CoordinatorServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(l)).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    (coord, vec![h1, h2])
}

fn doc(icao24: &str, origin: &str) -> FlightDocument {
    FlightDocument { icao24: icao24.into(), callsign: icao24.into(), origin: origin.into(), ..Default::default() }
}

fn ids(resp: &common::pb::SearchResponse) -> Vec<String> {
    resp.hits.iter().filter_map(|h| h.document.as_ref().map(|d| d.icao24.clone())).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn federated_query_merges_both_clusters_and_degrades_honestly() {
    let (c1, _h1) = start_cluster("us", vec![doc("us1", "synthetica"), doc("us2", "synthetica")]).await;
    let (c2, h2) = start_cluster("eu", vec![doc("eu1", "synthetica"), doc("eu2", "synthetica")]).await;

    // Both up: the federated result contains hits from BOTH clusters, coverage 2/2.
    let resp = federate_search(
        vec![c1.clone(), c2.clone()],
        SearchRequest { query: "synthetica".into(), limit: 50, filter: None },
    )
    .await;
    let got = ids(&resp);
    assert!(["us1", "us2", "eu1", "eu2"].iter().all(|id| got.contains(&id.to_string())),
        "federated result must merge both clusters: {got:?}");
    let m = resp.manifest.as_ref().unwrap();
    assert_eq!(m.shards_queried, 2, "two clusters queried");
    assert_eq!(m.shards_answered, 2, "both answered");
    assert!(m.omitted.is_empty());

    // Kill the EU cluster; a federated query returns the US survivor's data, and the dead
    // cluster is named in coverage — partial, not failed.
    for h in h2 {
        h.abort();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let resp = federate_search(
        vec![c1.clone(), c2.clone()],
        SearchRequest { query: "synthetica".into(), limit: 50, filter: None },
    )
    .await;
    let got = ids(&resp);
    assert!(got.contains(&"us1".to_string()) && got.contains(&"us2".to_string()), "survivor's data present");
    assert!(!got.contains(&"eu1".to_string()), "dead cluster's data absent");
    let m = resp.manifest.as_ref().unwrap();
    assert_eq!(m.shards_queried, 2);
    assert_eq!(m.shards_answered, 1, "one cluster answered");
    assert_eq!(m.omitted.len(), 1, "the dead cluster is named");
    assert_eq!(m.omitted[0].address, c2);
}
