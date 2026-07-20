//! The NLQ loop, driven by a scripted model against a REAL cluster (coordinator + shard),
//! so protocol, budgets, and provenance composition are all tested without a Bedrock call.
//! Real planning quality is the live eval's job; this proves the machinery around it.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::{FlightDocument, NodeRole, RegisterNodeRequest};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use nlq::{run, Budget, EngineTools, FakeModel, Step};
use serde_json::json;
use shard_node::server::ShardSearchService;
use shard_node::store::ShardStore;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Serialize the tests: each starts its OWN cluster on its OWN runtime (a server spawned
/// on one #[tokio::test] runtime dies when that test ends, so it can't be shared), and the
/// tools read a process-global env var — so the tests must not overlap. This mutex makes
/// each test's "set env → run" atomic across the whole binary.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_cluster() -> String {
    let store = Arc::new(RwLock::new(ShardStore::new()));
    {
        let mut s = store.write().unwrap();
        for (i, origin) in ["France", "France", "USA"].iter().enumerate() {
            s.insert(FlightDocument {
                icao24: format!("ac{i}"),
                callsign: format!("FL{i}"),
                origin: origin.to_string(),
                altitude: 2000.0 + i as f64 * 1000.0,
                ..Default::default()
            });
        }
    }
    let shard = ShardSearchService::new(store, "shard-0".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let shard_addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(ShardSearchServer::new(shard))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });

    let registry = Arc::new(RwLock::new(Registry::new(1)));
    registry.write().unwrap().register(RegisterNodeRequest {
        node_id: "n0".into(),
        address: shard_addr,
        shard_id: 0,
        role: NodeRole::Leader as i32,
    });
    let service = CoordinatorService::new(registry);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coord = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(CoordinatorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    // The tools read AETHER_COORDINATOR_ADDR from the environment.
    std::env::set_var("AETHER_COORDINATOR_ADDR", &coord);

    // Deterministic readiness: wait until a real query is actually routed to the shard
    // (both servers bound AND the leader registered), so no test races startup.
    use common::pb::coordinator_client::CoordinatorClient;
    use common::pb::SearchRequest;
    for _ in 0..200 {
        if let Ok(mut c) = CoordinatorClient::connect(format!("http://{coord}")).await {
            if let Ok(resp) = c
                .search(SearchRequest { query: "france".into(), limit: 1, filter: None })
                .await
            {
                if resp.into_inner().shards_answered == 1 {
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    coord
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scripted_plan_runs_real_tools_and_composes_provenance() {
    let _guard = SERIAL.lock().await;
    let _coord = start_cluster().await;

    // A two-hop plan: aggregate France-count, then answer. Both tool results carry a
    // provenance line, which the loop must lift into the answer's evidence.
    let model = FakeModel::new(vec![
        Step::CallTool {
            name: "aggregate_flights".into(),
            args: json!({ "kind": "value_counts", "field": "origin" }),
        },
        Step::CallTool {
            name: "search_flights".into(),
            args: json!({ "query": "france" }),
        },
        Step::Answer("There are flights from France and the USA.".into()),
    ]);

    let answer = run(model.as_ref(), &EngineTools, "what origins are flying?", Budget::default()).await;

    assert!(!answer.budget_exhausted);
    assert_eq!(answer.tool_calls, 2, "both scripted tool calls ran");
    assert!(answer.text.contains("France"));
    // Each tool result emitted a provenance line; the answer composed them.
    assert_eq!(answer.provenance.len(), 2, "provenance composed from both tool calls");
    assert!(answer.provenance.iter().all(|p| p.contains("answered")), "real manifest lines");

    let rendered = nlq::render(&answer);
    assert!(rendered.contains("— evidence —"), "rendered answer shows its evidence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn budget_exhaustion_returns_a_labeled_partial_not_a_hang() {
    let _guard = SERIAL.lock().await;
    let _coord = start_cluster().await;

    // A model that only ever calls tools, never answers — the budget must stop it.
    let model = FakeModel::new(vec![]); // empty script → FakeModel keeps calling cluster_state
    let answer = run(
        model.as_ref(),
        &EngineTools,
        "loop forever?",
        Budget { max_tool_calls: 3 },
    )
    .await;

    assert!(answer.budget_exhausted, "budget must halt a non-terminating planner");
    assert_eq!(answer.tool_calls, 3, "stopped exactly at the budget");
    assert!(answer.text.contains("partial answer"), "the partial is labeled: {}", answer.text);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tool_error_is_observed_not_fatal() {
    let _guard = SERIAL.lock().await;
    let _coord = start_cluster().await;

    // The model calls a nonexistent tool, sees the error as an observation, then answers.
    let model = FakeModel::new(vec![
        Step::CallTool { name: "delete_everything".into(), args: json!({}) },
        Step::Answer("I could not do that.".into()),
    ]);
    let answer = run(model.as_ref(), &EngineTools, "drop the data", Budget::default()).await;

    assert!(!answer.budget_exhausted);
    assert_eq!(answer.tool_calls, 1);
    assert!(answer.text.contains("could not"), "the model answered around the tool error");
}
