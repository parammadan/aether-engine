//! Client-side coordinator failover: every client (dashboard, examples, MCP agent) uses
//! the same list-parsing + first-healthy-connect helpers, so this exercises the shared
//! mechanism once: a dead address at the FRONT of the list must cost nothing but a
//! connect attempt — never an error surfaced to the caller.

use std::sync::{Arc, RwLock};

use common::pb::coordinator_server::CoordinatorServer;
use common::pb::ClusterStateRequest;
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[test]
fn addr_lists_parse_with_trim_and_fallback() {
    assert_eq!(
        common::client::parse_addr_list("a:1, b:2 ,c:3", "d:4"),
        vec!["a:1", "b:2", "c:3"]
    );
    assert_eq!(common::client::parse_addr_list("", "d:4"), vec!["d:4"]);
    assert_eq!(common::client::parse_addr_list(" , ,", "d:4"), vec!["d:4"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_first_address_fails_over_to_the_live_coordinator() {
    // A port with nothing listening: bind, learn the port, drop the listener.
    let dead = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };

    let live = {
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
    };

    let addrs = vec![dead.clone(), live];
    let mut client = common::client::connect_first_healthy(&addrs)
        .await
        .expect("must fail over past the dead address");
    let state = client
        .get_cluster_state(ClusterStateRequest {})
        .await
        .expect("the connected coordinator must actually serve")
        .into_inner();
    assert_eq!(state.nodes.len(), 0, "fresh coordinator: empty registry");

    // And a list that is ALL dead reports every address it tried, not a panic.
    let err = common::client::connect_first_healthy(&[dead.clone()]).await.err().unwrap();
    assert!(err.contains(&dead), "error must name the addresses tried: {err}");
}
