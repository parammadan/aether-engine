//! The capstone's guarantees, tested: the briefing agent composes a real, provenance-
//! carrying report from a live cluster (read-only), and its single egress is governed —
//! email goes only to allowlisted recipients, a non-allowlisted one is refused and nothing
//! is sent to it, and the cluster is unchanged by the whole run.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use briefing::{brief_and_send, Allowlist, Emailer};
use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::{FlightDocument, NodeRole, RegisterNodeRequest};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use nlq::HeuristicModel;
use shard_node::server::ShardSearchService;
use shard_node::store::ShardStore;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// One cluster per test, process-global env — serialize (see nlq's tests for why).
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Records every send; sends nothing real.
#[derive(Default)]
struct MockEmailer {
    sent: std::sync::Mutex<Vec<String>>,
}
#[async_trait]
impl Emailer for MockEmailer {
    async fn send(&self, to: &str, _subject: &str, _body: &str) -> Result<(), String> {
        self.sent.lock().unwrap().push(to.to_string());
        Ok(())
    }
}

async fn start_cluster() -> String {
    let store = Arc::new(RwLock::new(ShardStore::new()));
    {
        let mut s = store.write().unwrap();
        for i in 0..6 {
            s.insert(FlightDocument {
                icao24: format!("ac{i}"),
                callsign: format!("SYN{i}"),
                origin: "Synthetica".into(),
                aircraft_type: if i % 2 == 0 { "TestJet" } else { "MockLiner" }.into(),
                altitude: 1000.0 * i as f64,
                ..Default::default()
            });
        }
    }
    let shard = ShardSearchService::new(store, "shard-0".into());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let shard_addr = l.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder().add_service(ShardSearchServer::new(shard))
            .serve_with_incoming(TcpListenerStream::new(l)).await;
    });
    let registry = Arc::new(RwLock::new(Registry::new(1)));
    registry.write().unwrap().register(RegisterNodeRequest {
        node_id: "n0".into(), address: shard_addr, shard_id: 0, role: NodeRole::Leader as i32,
    });
    let svc = CoordinatorService::new(registry);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coord = l.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder().add_service(CoordinatorServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(l)).await;
    });
    std::env::set_var("AETHER_COORDINATOR_ADDR", &coord);
    // readiness
    for _ in 0..200 {
        if let Ok(mut c) = CoordinatorClient::connect(format!("http://{coord}")).await {
            if let Ok(r) = c.search(common::pb::SearchRequest { query: "synthetica".into(), limit: 1, filter: None }).await {
                if r.into_inner().shards_answered == 1 { break; }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    coord
}

async fn placement_version(coord: &str) -> u64 {
    // A cheap proxy for "cluster state unchanged": a query's manifest carries the placement
    // version, which any mutation would bump.
    let mut c = CoordinatorClient::connect(format!("http://{coord}")).await.unwrap();
    c.search(common::pb::SearchRequest { query: "synthetica".into(), limit: 1, filter: None })
        .await.unwrap().into_inner().manifest.unwrap().placement_version
}

fn questions() -> Vec<String> {
    ["how many flights?", "which aircraft types are flying?", "is the cluster healthy?"]
        .into_iter().map(String::from).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn briefing_composes_real_content_and_emails_only_allowlisted_recipients() {
    let _g = SERIAL.lock().await;
    let coord = start_cluster().await;
    let before = placement_version(&coord).await;

    let allow = Allowlist::new(vec!["ops@aether.io".into()]);
    let emailer = MockEmailer::default();
    // One allowlisted, one NOT.
    let recipients = vec!["ops@aether.io".to_string(), "attacker@evil.com".to_string()];

    let result = brief_and_send(&HeuristicModel, &questions(), &recipients, &allow, &emailer).await;

    // The non-allowlisted recipient causes an error naming it...
    let errs = result.unwrap_err();
    assert!(errs.iter().any(|e| e.contains("attacker@evil.com") && e.contains("not allowlisted")));
    // ...and email went ONLY to the allowlisted address.
    let sent = emailer.sent.lock().unwrap().clone();
    assert_eq!(sent, vec!["ops@aether.io".to_string()], "email leaked to a non-allowlisted recipient");

    // The briefing was real: composing it produced provenance from live tool calls.
    let brief = briefing::compose(&HeuristicModel, &questions()).await;
    assert!(brief.body.contains("flights"), "briefing has real content");
    assert!(!brief.provenance.is_empty(), "briefing carries provenance from the cluster");

    // Read-only: the whole run didn't mutate the cluster (placement version unchanged).
    assert_eq!(placement_version(&coord).await, before, "the briefing agent mutated cluster state");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_allowlist_sends_nothing() {
    let _g = SERIAL.lock().await;
    let _coord = start_cluster().await;
    let emailer = MockEmailer::default();
    let result =
        brief_and_send(&HeuristicModel, &questions(), &["ops@aether.io".into()], &Allowlist::default(), &emailer).await;
    assert!(result.is_err(), "fail-closed: an empty allowlist permits no email");
    assert!(emailer.sent.lock().unwrap().is_empty(), "nothing sent under an empty allowlist");
}
