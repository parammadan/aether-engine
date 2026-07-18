//! End-to-end test: start the ShardSearch gRPC server on an ephemeral port, then query it
//! with the generated client. Proves the wire path (client -> gRPC -> index -> response),
//! not just the in-process index.

use std::sync::{Arc, RwLock};

use common::pb::shard_search_client::ShardSearchClient;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::{FlightDocument, SearchRequest};
use shard_node::index::InvertedIndex;
use shard_node::server::ShardSearchService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn doc(icao24: &str, callsign: &str, origin: &str, destination: &str, aircraft: &str) -> FlightDocument {
    FlightDocument {
        icao24: icao24.to_string(),
        callsign: callsign.to_string(),
        origin: origin.to_string(),
        destination: destination.to_string(),
        aircraft_type: aircraft.to_string(),
        ..Default::default()
    }
}

/// Boots the server on 127.0.0.1:<ephemeral> and returns a connected client.
async fn start_server_and_client() -> ShardSearchClient<tonic::transport::Channel> {
    let mut index = InvertedIndex::new();
    index.insert(doc("a1b2c3", "UAL231", "SFO", "JFK", "Boeing 737"));
    index.insert(doc("d4e5f6", "DAL45", "ATL", "LAX", "Airbus A320"));
    index.insert(doc("aa11bb", "UAL900", "ORD", "SFO", "Boeing 777"));
    let index = Arc::new(RwLock::new(index));

    // Bind to :0 so the OS hands us a free port — no fixed-port races between test runs.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let service = ShardSearchService::new(index, "shard-test".to_string());
    tokio::spawn(async move {
        Server::builder()
            .add_service(ShardSearchServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Retry connect briefly while the spawned server binds.
    let endpoint = format!("http://{addr}");
    for _ in 0..20 {
        if let Ok(client) = ShardSearchClient::connect(endpoint.clone()).await {
            return client;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("server did not come up at {endpoint}");
}

#[tokio::test]
async fn search_over_grpc_returns_matching_hits() {
    let mut client = start_server_and_client().await;

    // "sfo" is a1b2c3's origin and aa11bb's destination -> 2 matches.
    let resp = client
        .search(SearchRequest { query: "sfo".to_string(), limit: 10 })
        .await
        .expect("search RPC failed")
        .into_inner();

    assert_eq!(resp.shard_id, "shard-test");
    assert_eq!(resp.total_matched, 2);
    assert_eq!(resp.hits.len(), 2);
    let ids: Vec<String> = resp
        .hits
        .iter()
        .map(|h| h.document.as_ref().unwrap().icao24.clone())
        .collect();
    assert!(ids.contains(&"a1b2c3".to_string()));
    assert!(ids.contains(&"aa11bb".to_string()));
}

#[tokio::test]
async fn search_respects_limit_but_reports_total() {
    let mut client = start_server_and_client().await;

    let resp = client
        .search(SearchRequest { query: "boeing".to_string(), limit: 1 })
        .await
        .expect("search RPC failed")
        .into_inner();

    assert_eq!(resp.total_matched, 2); // both Boeings matched
    assert_eq!(resp.hits.len(), 1); // limit applied
}

#[tokio::test]
async fn search_with_no_match_is_empty() {
    let mut client = start_server_and_client().await;

    let resp = client
        .search(SearchRequest { query: "helicopter".to_string(), limit: 10 })
        .await
        .expect("search RPC failed")
        .into_inner();

    assert_eq!(resp.total_matched, 0);
    assert!(resp.hits.is_empty());
}
