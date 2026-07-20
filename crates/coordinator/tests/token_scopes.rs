//! Scoped tokens, enforced server-side. The negative paths ARE the feature: a read
//! token cannot mutate, an unknown token cannot do anything, a missing token is refused
//! when auth is on. The MCP agent's read credential, replayed against a mutating RPC, is
//! denied — its read-only guarantee is enforced by the server, not merely by which RPCs
//! its binary happens to link.

use std::io::Write;
use std::sync::{Arc, RwLock};

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::{ClusterStateRequest, DrainRequest, SearchRequest};
use coordinator::auth::Auth;
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request};

/// A coordinator whose client-facing RPCs enforce a two-token file (read + operator).
async fn start_authed_coordinator() -> String {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    writeln!(f, "# scopes\nread-token read\noperator-token operator").unwrap();
    let (_file, path) = f.keep().unwrap();
    std::env::set_var("AETHER_TOKENS_FILE", &path);
    let auth = Arc::new(Auth::from_env().unwrap());
    std::env::remove_var("AETHER_TOKENS_FILE"); // don't leak into other tests/processes

    let registry = Arc::new(RwLock::new(Registry::new(1)));
    let service = CoordinatorService::new(registry).with_auth(auth);
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

fn with_bearer<T>(msg: T, token: Option<&str>) -> Request<T> {
    let mut req = Request::new(msg);
    if let Some(t) = token {
        req.metadata_mut().insert("authorization", format!("Bearer {t}").parse().unwrap());
    }
    req
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scopes_are_enforced_on_client_facing_rpcs() {
    let addr = start_authed_coordinator().await;
    let mut c = loop {
        if let Ok(c) = CoordinatorClient::connect(format!("http://{addr}")).await {
            break c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };

    // READ scope: a read token queries and reads state; it CANNOT drain.
    assert!(
        c.get_cluster_state(with_bearer(ClusterStateRequest {}, Some("read-token"))).await.is_ok(),
        "read token must read cluster state"
    );
    assert!(
        c.search(with_bearer(SearchRequest { query: "x".into(), limit: 1, filter: None }, Some("read-token"))).await.is_ok(),
        "read token must query"
    );
    let denied = c
        .drain_node(with_bearer(DrainRequest { node_id: "n".into() }, Some("read-token")))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied, "read token must NOT drain");

    // OPERATOR scope: the mutating RPC is authorized (unknown node → ok:false, not a
    // permission error — authorization passed, the node just isn't registered).
    let resp = c
        .drain_node(with_bearer(DrainRequest { node_id: "n".into() }, Some("operator-token")))
        .await
        .expect("operator token must be authorized to drain")
        .into_inner();
    assert!(!resp.ok, "unknown node drains to ok:false, but the CALL was authorized");

    // MISSING token: refused when auth is on.
    assert_eq!(
        c.search(with_bearer(SearchRequest { query: "x".into(), limit: 1, filter: None }, None)).await.unwrap_err().code(),
        Code::Unauthenticated,
        "no token must be refused"
    );

    // UNKNOWN token: refused.
    assert_eq!(
        c.get_cluster_state(with_bearer(ClusterStateRequest {}, Some("garbage"))).await.unwrap_err().code(),
        Code::Unauthenticated,
        "unknown token must be refused"
    );
}
