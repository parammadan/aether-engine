//! Live smoke test against the real OpenSky API. Ignored by default (needs network and is
//! subject to OpenSky rate limits). Run explicitly:
//!
//!   cargo test -p shard-node --test opensky_live -- --ignored --nocapture

use std::sync::{Arc, RwLock};

use common::pb::shard_search_client::ShardSearchClient;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::SearchRequest;
use shard_node::index::InvertedIndex;
use shard_node::ingest::{FlightSource, OpenSkySource};
use shard_node::server::ShardSearchService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[tokio::test]
#[ignore = "hits the live OpenSky network"]
async fn opensky_returns_flight_documents() {
    let source = OpenSkySource::from_env();
    let docs = source.fetch().await.expect("OpenSky fetch failed");

    println!("OpenSky returned {} flight documents", docs.len());
    assert!(!docs.is_empty(), "expected at least one flight");

    // Spot-check the mapping produced sane identities.
    let sample = &docs[0];
    assert!(!sample.icao24.is_empty(), "every doc must have an icao24 (shard key)");
}

/// Full end-to-end, with live data: ingest one real OpenSky snapshot, then search it over
/// gRPC on one node.
#[tokio::test]
#[ignore = "hits the live OpenSky network"]
async fn end_to_end_live_search_over_grpc() {
    // 1. Ingest one live snapshot into the index.
    let docs = OpenSkySource::from_env().fetch().await.expect("OpenSky fetch failed");
    let total = docs.len();
    let mut index = InvertedIndex::new();
    for doc in docs {
        index.insert(doc);
    }
    let index = Arc::new(RwLock::new(index));

    // 2. Serve it over gRPC on an ephemeral port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = ShardSearchService::new(index, "shard-live".to_string());
    tokio::spawn(async move {
        Server::builder()
            .add_service(ShardSearchServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // 3. Query it as a client. "united" matches the origin_country "United States" —
    //    always present in a global snapshot.
    let endpoint = format!("http://{addr}");
    let mut client = loop {
        if let Ok(c) = ShardSearchClient::connect(endpoint.clone()).await {
            break c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    let resp = client
        .search(SearchRequest { query: "united".to_string(), limit: 5 })
        .await
        .expect("search RPC failed")
        .into_inner();

    println!(
        "ingested {total} live flights; query 'united' matched {} over gRPC",
        resp.total_matched
    );
    assert!(resp.total_matched > 0, "expected US-registered aircraft in a global snapshot");
    assert!(!resp.hits.is_empty());
}
