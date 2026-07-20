//! End-to-end: a leader replicates a batch to a follower, and the follower can then serve
//! that data over gRPC — i.e. it's a live replica, ready to be promoted.

use std::sync::{Arc, RwLock};

use common::pb::replication_server::ReplicationServer;
use common::pb::shard_search_client::ShardSearchClient;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::{FlightDocument, SearchRequest};
use shard_node::store::ShardStore;
use shard_node::replication::{replicate_to_followers, ReplicationService};
use shard_node::server::ShardSearchService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn doc(icao24: &str, callsign: &str) -> FlightDocument {
    FlightDocument { icao24: icao24.to_string(), callsign: callsign.to_string(), ..Default::default() }
}

/// Start a follower node (serves both ShardSearch and Replication over one address).
async fn start_follower() -> String {
    let index = Arc::new(RwLock::new(ShardStore::new()));
    let search = ShardSearchService::new(index.clone(), "shard-0".to_string());
    let replication = ReplicationService::new(index);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        Server::builder()
            .add_service(ShardSearchServer::new(search))
            .add_service(ReplicationServer::new(replication))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    addr
}

async fn connect(addr: &str) -> ShardSearchClient<tonic::transport::Channel> {
    let endpoint = format!("http://{addr}");
    for _ in 0..40 {
        if let Ok(c) = ShardSearchClient::connect(endpoint.clone()).await {
            return c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("follower never came up at {addr}");
}

#[tokio::test]
async fn leader_replicates_to_follower_which_then_serves_the_data() {
    let follower = start_follower().await;
    let mut client = connect(&follower).await; // also ensures the server is ready

    // Before replication the follower is empty.
    let before = client
        .search(SearchRequest { query: "ual231".to_string(), limit: 10, filter: None })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(before.total_matched, 0);

    // The leader replicates a batch to the follower.
    replicate_to_followers(
        vec![follower.clone()],
        0,
        vec![doc("a1", "UAL231"), doc("b2", "DAL45")],
    )
    .await;

    // The follower now serves the replicated data.
    let after = client
        .search(SearchRequest { query: "ual231".to_string(), limit: 10, filter: None })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(after.total_matched, 1);
    assert_eq!(after.hits[0].document.as_ref().unwrap().icao24, "a1");
}
