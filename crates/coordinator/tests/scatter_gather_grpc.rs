//! End-to-end scatter-gather: the coordinator fans a query across real (stub) shard-leader
//! gRPC servers and merges the results. Uses in-test stub shards so this stays a focused
//! coordinator test.

use std::sync::{Arc, RwLock};

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_server::{ShardSearch, ShardSearchServer};
use common::pb::{
    FlightDocument, NodeRole, RegisterNodeRequest, SearchHit, SearchRequest, SearchResponse,
};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// A shard server that always returns a canned response.
struct StubShard {
    response: SearchResponse,
}

#[tonic::async_trait]
impl ShardSearch for StubShard {
    async fn search(
        &self,
        _request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Ok(Response::new(self.response.clone()))
    }

    async fn vector_search(
        &self,
        _request: Request<common::pb::VectorSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Ok(Response::new(self.response.clone()))
    }
}

fn hit(icao24: &str, score: f64) -> SearchHit {
    SearchHit {
        document: Some(FlightDocument { icao24: icao24.to_string(), ..Default::default() }),
        score,
        provenance: None,
    }
}

fn shard_reply(shard_id: &str, total: u64, hits: Vec<SearchHit>) -> SearchResponse {
    SearchResponse { hits, total_matched: total, shard_id: shard_id.to_string(), shards_queried: 0, shards_answered: 0, manifest: None }
}

async fn spawn_on_ephemeral<F>(make: F) -> String
where
    F: FnOnce(tokio::net::TcpListener) -> tokio::task::JoinHandle<()>,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    make(listener);
    addr
}

async fn start_stub_shard(response: SearchResponse) -> String {
    spawn_on_ephemeral(|listener| {
        tokio::spawn(async move {
            Server::builder()
                .add_service(ShardSearchServer::new(StubShard { response }))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        })
    })
    .await
}

/// Like `start_stub_shard`, but returns the server task handle so the test can "kill" the
/// node by aborting it (closing its listener).
async fn start_killable_shard(response: SearchResponse) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(ShardSearchServer::new(StubShard { response }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (addr, handle)
}

async fn start_coordinator(registry: Arc<RwLock<Registry>>) -> CoordinatorClient<tonic::transport::Channel> {
    let service = CoordinatorService::new(registry);
    let addr = spawn_on_ephemeral(|listener| {
        tokio::spawn(async move {
            Server::builder()
                .add_service(CoordinatorServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        })
    })
    .await;

    let endpoint = format!("http://{addr}");
    loop {
        if let Ok(c) = CoordinatorClient::connect(endpoint.clone()).await {
            break c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
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

#[tokio::test]
async fn coordinator_merges_results_across_shards() {
    // Two shards with disjoint hits.
    let a0 = start_stub_shard(shard_reply("shard-0", 2, vec![hit("aaa", 1.0), hit("bbb", 3.0)])).await;
    let a1 = start_stub_shard(shard_reply("shard-1", 1, vec![hit("ccc", 2.0)])).await;

    let registry = Arc::new(RwLock::new(Registry::new(2)));
    register(&registry, 0, &a0);
    register(&registry, 1, &a1);

    let mut client = start_coordinator(registry).await;
    let resp = client
        .search(SearchRequest { query: "x".to_string(), limit: 10 })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.total_matched, 3); // 2 + 1 across shards
    assert_eq!(resp.shards_queried, 2);
    assert_eq!(resp.shards_answered, 2);
    let order: Vec<&str> = resp
        .hits
        .iter()
        .map(|h| h.document.as_ref().unwrap().icao24.as_str())
        .collect();
    assert_eq!(order, vec!["bbb", "ccc", "aaa"]); // globally ranked by score
}

#[tokio::test]
async fn search_stream_converges_across_shards_and_marks_complete() {
    let a0 = start_stub_shard(shard_reply("shard-0", 2, vec![hit("aaa", 1.0), hit("bbb", 3.0)])).await;
    let a1 = start_stub_shard(shard_reply("shard-1", 1, vec![hit("ccc", 2.0)])).await;

    let registry = Arc::new(RwLock::new(Registry::new(2)));
    register(&registry, 0, &a0);
    register(&registry, 1, &a1);

    let mut client = start_coordinator(registry).await;
    let mut stream = client
        .search_stream(SearchRequest { query: "x".to_string(), limit: 10 })
        .await
        .unwrap()
        .into_inner();

    let mut updates = Vec::new();
    while let Some(update) = stream.message().await.unwrap() {
        updates.push(update);
    }

    // Progressive: at least one non-final update, then exactly one final.
    assert!(updates.len() >= 2, "expected progressive updates, got {}", updates.len());
    assert!(updates.iter().any(|u| !u.complete), "no intermediate update");
    let last = updates.last().unwrap();
    assert!(last.complete);
    assert_eq!(last.shards_answered, 2);
    assert_eq!(last.shards_queried, 2);
    assert_eq!(last.total_matched, 3);
    let order: Vec<&str> = last
        .hits
        .iter()
        .map(|h| h.document.as_ref().unwrap().icao24.as_str())
        .collect();
    assert_eq!(order, vec!["bbb", "ccc", "aaa"]); // globally ranked once converged
}

#[tokio::test]
async fn search_stream_completes_even_when_a_shard_dies_mid_aggregation() {
    let a0 = start_stub_shard(shard_reply("shard-0", 1, vec![hit("aaa", 1.0)])).await;

    let registry = Arc::new(RwLock::new(Registry::new(2)));
    register(&registry, 0, &a0);
    register(&registry, 1, "127.0.0.1:2"); // dead shard: its update never arrives

    let mut client = start_coordinator(registry).await;
    let mut stream = client
        .search_stream(SearchRequest { query: "x".to_string(), limit: 10 })
        .await
        .unwrap()
        .into_inner();

    let mut last = None;
    while let Some(update) = stream.message().await.unwrap() {
        last = Some(update);
    }

    // The stream still completes, converged on the surviving shard (partial coverage).
    let last = last.unwrap();
    assert!(last.complete);
    assert_eq!(last.shards_answered, 1);
    assert_eq!(last.shards_queried, 2);
    assert_eq!(last.hits.len(), 1);
    assert_eq!(last.hits[0].document.as_ref().unwrap().icao24, "aaa");
}

#[tokio::test]
async fn failover_promotes_follower_and_queries_route_to_it() {
    use std::time::{Duration, Instant};

    // Two nodes for shard 0 with distinguishable data, so we can tell who answered.
    let leader_addr = start_stub_shard(shard_reply("shard-0", 1, vec![hit("leaderdoc", 1.0)])).await;
    let follower_addr = start_stub_shard(shard_reply("shard-0", 1, vec![hit("followerdoc", 1.0)])).await;

    let registry = Arc::new(RwLock::new(Registry::new(1)));
    registry.write().unwrap().register(RegisterNodeRequest {
        node_id: "L".to_string(),
        address: leader_addr,
        shard_id: 0,
        role: NodeRole::Leader as i32,
    });
    registry.write().unwrap().register(RegisterNodeRequest {
        node_id: "F".to_string(),
        address: follower_addr,
        shard_id: 0,
        role: NodeRole::Follower as i32,
    });

    let mut client = start_coordinator(registry.clone()).await;

    // Before failover the query is served by the leader.
    let before = client
        .search(SearchRequest { query: "x".to_string(), limit: 10 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(before.hits[0].document.as_ref().unwrap().icao24, "leaderdoc");

    // The leader dies: the follower stays fresh, then the reaper reaps + promotes.
    {
        let mut reg = registry.write().unwrap();
        let now = Instant::now().checked_add(Duration::from_secs(60)).unwrap();
        reg.heartbeat("F", now);
        reg.reap_dead(now, Duration::from_secs(30));
        reg.promote_orphaned_shards();
    }

    // After failover the same query is now served by the promoted follower.
    let after = client
        .search(SearchRequest { query: "x".to_string(), limit: 10 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(after.shards_answered, 1);
    assert_eq!(after.hits[0].document.as_ref().unwrap().icao24, "followerdoc");
}

#[tokio::test]
async fn queries_keep_flowing_when_a_leader_is_killed_under_load() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    // Shard 0: a killable leader and a follower, with distinguishable data.
    let (leader_addr, leader_handle) =
        start_killable_shard(shard_reply("shard-0", 1, vec![hit("leaderdoc", 1.0)])).await;
    let follower_addr = start_stub_shard(shard_reply("shard-0", 1, vec![hit("followerdoc", 1.0)])).await;

    let registry = Arc::new(RwLock::new(Registry::new(1)));
    register(&registry, 0, &leader_addr);
    registry.write().unwrap().register(RegisterNodeRequest {
        node_id: "F".to_string(),
        address: follower_addr,
        shard_id: 0,
        role: NodeRole::Follower as i32,
    });

    let client = start_coordinator(registry.clone()).await;

    // A background load generator hammering the coordinator with queries.
    let outcomes: Arc<Mutex<Vec<Result<String, String>>>> = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let load = tokio::spawn({
        let outcomes = outcomes.clone();
        let stop = stop.clone();
        let mut client = client.clone();
        async move {
            while !stop.load(Ordering::Relaxed) {
                let outcome = match client.search(SearchRequest { query: "x".to_string(), limit: 10 }).await {
                    Ok(resp) => Ok(resp
                        .into_inner()
                        .hits
                        .first()
                        .map(|h| h.document.as_ref().unwrap().icao24.clone())
                        .unwrap_or_else(|| "EMPTY".to_string())),
                    Err(status) => Err(status.to_string()),
                };
                outcomes.lock().unwrap().push(outcome);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    });

    // Serve from the leader for a bit.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // KILL the leader mid-load, and let the coordinator's failover run (reap + promote).
    leader_handle.abort();
    {
        let mut reg = registry.write().unwrap();
        let now = Instant::now().checked_add(Duration::from_secs(60)).unwrap();
        reg.heartbeat("F", now); // follower is alive
        reg.reap_dead(now, Duration::from_secs(30)); // dead leader dropped
        reg.promote_orphaned_shards(); // follower promoted
    }

    // Keep serving from the promoted follower for a bit, then stop the load.
    tokio::time::sleep(Duration::from_millis(250)).await;
    stop.store(true, Ordering::Relaxed);
    load.await.unwrap();

    let outcomes = outcomes.lock().unwrap();
    assert!(outcomes.len() > 10, "load generator barely ran ({} queries)", outcomes.len());

    // The query stream never stopped: not a single RPC failed, even across the kill.
    let errors: Vec<_> = outcomes.iter().filter_map(|o| o.as_ref().err()).collect();
    assert!(errors.is_empty(), "a query failed at the RPC level: {errors:?}");

    // We served from the leader before the kill and from the promoted follower after it.
    assert!(
        outcomes.iter().any(|o| matches!(o, Ok(w) if w == "leaderdoc")),
        "never saw the leader answer"
    );
    assert!(
        outcomes.iter().any(|o| matches!(o, Ok(w) if w == "followerdoc")),
        "never failed over to the follower"
    );
}

#[tokio::test]
async fn coordinator_returns_partial_results_when_a_shard_is_down() {
    // Shard 0 is live; shard 1's registered address isn't serving.
    let a0 = start_stub_shard(shard_reply("shard-0", 1, vec![hit("aaa", 1.0)])).await;

    let registry = Arc::new(RwLock::new(Registry::new(2)));
    register(&registry, 0, &a0);
    register(&registry, 1, "127.0.0.1:2"); // connection refused

    let mut client = start_coordinator(registry).await;
    let resp = client
        .search(SearchRequest { query: "x".to_string(), limit: 10 })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.shards_queried, 2);
    assert_eq!(resp.shards_answered, 1); // partial: one shard down
    assert_eq!(resp.hits.len(), 1);
    assert_eq!(resp.hits[0].document.as_ref().unwrap().icao24, "aaa");
}
