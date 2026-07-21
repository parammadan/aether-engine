//! Structured filters end to end: filtered results equal a manual post-filter of the
//! unfiltered results (the pushdown changes WHERE the work happens, never the answer);
//! a bad filter is a loud InvalidArgument from the coordinator, not partial coverage.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::coordinator_server::CoordinatorServer;
use common::pb::filter_condition::Test;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::{
    AggKind, AggregateRequest, Filter, FilterCondition, FlightDocument, NodeRole, NumericRange,
    RegisterNodeRequest, SearchRequest,
};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use shard_node::server::ShardSearchService;
use shard_node::store::ShardStore;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::Code;

fn doc(icao24: &str, origin: &str, altitude: f64) -> FlightDocument {
    FlightDocument {
        icao24: icao24.to_string(),
        origin: origin.to_string(),
        altitude,
        ..Default::default()
    }
}

async fn start_shard(label: &str, docs: Vec<FlightDocument>) -> String {
    let store = Arc::new(RwLock::new(ShardStore::new()));
    {
        let mut s = store.write().unwrap();
        for d in docs {
            s.insert(d);
        }
    }
    let service = ShardSearchService::new(store, label.to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(ShardSearchServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    addr
}

async fn start_cluster() -> CoordinatorClient<tonic::transport::Channel> {
    // Two shards: France/US mix with varied altitudes.
    let s0 = start_shard(
        "shard-0",
        vec![doc("a", "France", 1000.0), doc("b", "France", 5000.0), doc("c", "USA", 2000.0)],
    )
    .await;
    let s1 = start_shard(
        "shard-1",
        vec![doc("d", "France", 2500.0), doc("e", "USA", 900.0)],
    )
    .await;

    let registry = Arc::new(RwLock::new(Registry::new(2)));
    for (i, addr) in [s0, s1].iter().enumerate() {
        registry.write().unwrap().register(RegisterNodeRequest {
            node_id: format!("n{i}"),
            address: addr.clone(),
            shard_id: i as u32,
            role: NodeRole::Leader as i32,
        });
    }
    let service = CoordinatorService::new(registry);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(CoordinatorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    loop {
        if let Ok(c) = CoordinatorClient::connect(format!("http://{addr}")).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn france_below_3000() -> Filter {
    Filter {
        conditions: vec![
            FilterCondition { field: "origin".into(), test: Some(Test::Equals("france".into())) },
            FilterCondition {
                field: "altitude".into(),
                test: Some(Test::Range(NumericRange { min: None, max: Some(3000.0) })),
            },
        ],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_search_equals_a_manual_post_filter() {
    let mut client = start_cluster().await;

    // Unfiltered, then filter by hand.
    let all = client
        .search(SearchRequest { query: "france usa".into(), limit: 0, filter: None })
        .await
        .unwrap()
        .into_inner();
    let manual: Vec<String> = all
        .hits
        .iter()
        .filter_map(|h| h.document.clone())
        .filter(|d| d.origin.eq_ignore_ascii_case("france") && d.altitude <= 3000.0)
        .map(|d| d.icao24)
        .collect();

    // Server-side filter must produce the same set (pushdown changes nothing but cost).
    let filtered = client
        .search(SearchRequest {
            query: "france usa".into(),
            limit: 0,
            filter: Some(france_below_3000()),
        })
        .await
        .unwrap()
        .into_inner();
    let mut got: Vec<String> =
        filtered.hits.iter().filter_map(|h| h.document.clone()).map(|d| d.icao24).collect();
    let mut want = manual.clone();
    got.sort();
    want.sort();
    assert_eq!(got, want, "filtered results must equal a manual post-filter");
    assert_eq!(filtered.total_matched, want.len() as u64, "total counts FILTERED matches");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_aggregate_counts_only_matching_docs() {
    let mut client = start_cluster().await;
    let resp = client
        .aggregate(AggregateRequest {
            query: String::new(),
            kind: AggKind::AggValueCounts as i32,
            field: "origin".into(),
            interval: 0.0,
            percentiles: vec![],
            filter: Some(france_below_3000()),
        })
        .await
        .unwrap()
        .into_inner();
    let p = resp.partial.unwrap();
    // France ≤3000m: a (1000) + d (2500) — b is at 5000, USA excluded entirely.
    assert_eq!(p.count, 2);
    assert_eq!(p.buckets.get("France"), Some(&2));
    assert_eq!(p.buckets.get("USA"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bad_filter_is_a_loud_error_not_partial_coverage() {
    let mut client = start_cluster().await;
    // A bad filter is a TYPE MISMATCH on a built-in column — `equals` on the numeric
    // `altitude`. (Unknown field NAMES are no longer errors: they're valid generic fields a
    // connector may define, so validation can't tell a typo from a real generic field.)
    let bad = Filter {
        conditions: vec![FilterCondition {
            field: "altitude".into(),
            test: Some(Test::Equals("high".into())),
        }],
    };
    let err = client
        .search(SearchRequest { query: "france".into(), limit: 0, filter: Some(bad.clone()) })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "a built-in type mismatch must be rejected loudly");
    assert!(err.message().contains("altitude"), "names the offending field: {}", err.message());

    let err = client
        .aggregate(AggregateRequest {
            query: String::new(),
            kind: AggKind::AggCount as i32,
            field: String::new(),
            interval: 0.0,
            percentiles: vec![],
            filter: Some(bad),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}
