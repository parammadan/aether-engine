//! Fan-out deadlines: a shard that is SLOW (up, but hung) must be bounded exactly like one
//! that is dead — omitted from coverage within the per-shard timeout — on all three query
//! paths (unary, vector, streaming). Without this, one sick node sets every query's tail
//! latency.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_server::{ShardSearch, ShardSearchServer};
use common::pb::{
    FlightDocument, NodeRole, RegisterNodeRequest, SearchHit, SearchRequest, SearchResponse,
    VectorSearchRequest,
};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// A healthy shard answering instantly with one hit.
struct FastShard;

/// A shard that is alive but takes far longer than any reasonable deadline.
struct SlowShard;

fn one_hit(icao24: &str) -> SearchResponse {
    SearchResponse {
        hits: vec![SearchHit {
            document: Some(FlightDocument { icao24: icao24.to_string(), ..Default::default() }),
            score: 1.0,
            provenance: None,
        }],
        total_matched: 1,
        shard_id: "stub".to_string(),
        shards_queried: 0,
        shards_answered: 0,
        manifest: None,
    }
}

#[tonic::async_trait]
impl ShardSearch for FastShard {
    async fn search(&self, _r: Request<SearchRequest>) -> Result<Response<SearchResponse>, Status> {
        Ok(Response::new(one_hit("fast")))
    }
    async fn vector_search(
        &self,
        _r: Request<VectorSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Ok(Response::new(one_hit("fast")))
    }
    async fn aggregate(
        &self,
        _r: Request<common::pb::AggregateRequest>,
    ) -> Result<Response<common::pb::AggregateResponse>, Status> {
        Ok(Response::new(common::pb::AggregateResponse::default()))
    }
}

#[tonic::async_trait]
impl ShardSearch for SlowShard {
    async fn search(&self, _r: Request<SearchRequest>) -> Result<Response<SearchResponse>, Status> {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(Response::new(one_hit("slow")))
    }
    async fn vector_search(
        &self,
        _r: Request<VectorSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(Response::new(one_hit("slow")))
    }
    async fn aggregate(
        &self,
        _r: Request<common::pb::AggregateRequest>,
    ) -> Result<Response<common::pb::AggregateResponse>, Status> {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(Response::new(common::pb::AggregateResponse::default()))
    }
}

async fn spawn_shard<S: ShardSearch>(svc: S) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(ShardSearchServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    addr
}

/// One fast + one slow shard registered as leaders of a 2-shard cluster; returns a client.
async fn start_cluster() -> CoordinatorClient<tonic::transport::Channel> {
    // Short deadline so the tests are quick; the slow shard sleeps 10s regardless. Set
    // before the first query so the process-wide deadline picks it up.
    std::env::set_var("AETHER_SHARD_TIMEOUT_MS", "300");

    let fast = spawn_shard(FastShard).await;
    let slow = spawn_shard(SlowShard).await;

    let registry = Arc::new(RwLock::new(Registry::new(2)));
    for (i, addr) in [fast, slow].into_iter().enumerate() {
        registry.write().unwrap().register(RegisterNodeRequest {
            node_id: format!("n{i}"),
            address: addr,
            shard_id: i as u32,
            role: NodeRole::Leader as i32,
        });
    }

    let service = CoordinatorService::new(registry);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(CoordinatorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    let endpoint = format!("http://{addr}");
    loop {
        if let Ok(c) = CoordinatorClient::connect(endpoint.clone()).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The slow shard sleeps 10s; with a 300ms deadline every query must return well under 2s.
const BOUND: Duration = Duration::from_secs(2);

#[tokio::test]
async fn unary_search_is_bounded_by_the_deadline_not_the_slowest_shard() {
    let mut client = start_cluster().await;

    let t0 = Instant::now();
    let resp = client
        .search(SearchRequest { query: "x".to_string(), limit: 10, filter: None })
        .await
        .unwrap()
        .into_inner();
    let elapsed = t0.elapsed();

    assert!(elapsed < BOUND, "query took {elapsed:?}; the slow shard set the latency");
    assert_eq!(resp.shards_queried, 2);
    assert_eq!(resp.shards_answered, 1); // the slow shard is partial coverage, not latency
    assert_eq!(resp.hits[0].document.as_ref().unwrap().icao24, "fast");
}

#[tokio::test]
async fn vector_search_is_bounded_by_the_deadline() {
    let mut client = start_cluster().await;

    let t0 = Instant::now();
    let resp = client
        .vector_search(VectorSearchRequest { vector: vec![0.0; 128], limit: 10, filter: None })
        .await
        .unwrap()
        .into_inner();
    let elapsed = t0.elapsed();

    assert!(elapsed < BOUND, "vector query took {elapsed:?}");
    assert_eq!(resp.shards_queried, 2);
    assert_eq!(resp.shards_answered, 1);
}

#[tokio::test]
async fn streaming_search_completes_within_the_deadline_bound() {
    let mut client = start_cluster().await;

    let t0 = Instant::now();
    let mut stream = client
        .search_stream(SearchRequest { query: "x".to_string(), limit: 10, filter: None })
        .await
        .unwrap()
        .into_inner();
    let mut last = None;
    while let Some(update) = stream.message().await.unwrap() {
        last = Some(update);
    }
    let elapsed = t0.elapsed();

    let last = last.expect("stream should emit at least the final update");
    assert!(elapsed < BOUND, "stream took {elapsed:?} to complete");
    assert!(last.complete);
    assert_eq!(last.shards_queried, 2);
    assert_eq!(last.shards_answered, 1); // slow shard's update never arrived — by deadline
}
