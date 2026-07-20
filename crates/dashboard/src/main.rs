//! Live cluster dashboard + chaos harness.
//!
//! Spawns a real cluster (coordinator + shard nodes) as child processes, then serves a
//! single-page UI on http://127.0.0.1:8080 that shows, live over WebSocket:
//!   - every node's health (from the coordinator's own cluster-state view),
//!   - a continuous query stream's throughput and coverage (queries run every second),
//!   - an event log (kills, spawns, promotions — promotions are detected by diffing roles).
//! The "kill" button SIGKILLs the actual process; recovery is the cluster's own reaper +
//! promotion doing its job. "Add follower" spawns a fresh process that registers and
//! catches up from replication.
//!
//! Config via env:
//!   AETHER_SHARD_COUNT      shards to run (default 2; one leader + one follower each)
//!   AETHER_DASHBOARD_ADDR   HTTP listen address (default 127.0.0.1:8080)

mod supervisor;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::Router;
use common::pb::{ClusterStateRequest, DrainRequest, NodeRole, SearchRequest};
use serde_json::json;
use supervisor::Supervisor;

struct App {
    supervisor: Supervisor,
    /// Coordinator endpoints in preference order; every control-plane read or drain
    /// goes to the first one that answers.
    coordinator_addrs: Vec<String>,
    state_rx: tokio::sync::watch::Receiver<String>,
    events: Mutex<Vec<serde_json::Value>>,
    query_ok: AtomicU64,
    query_err: AtomicU64,
    started: Instant,
}

impl App {
    fn push_event(&self, msg: String) {
        let mut events = self.events.lock().unwrap();
        events.push(json!({
            "at_ms": self.started.elapsed().as_millis() as u64,
            "msg": msg,
        }));
        let len = events.len();
        if len > 50 {
            events.drain(0..len - 50);
        }
    }
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    common::net::install_crypto();
    let shard_count: u32 = env_or("AETHER_SHARD_COUNT", 2);
    let http_addr: String =
        std::env::var("AETHER_DASHBOARD_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    // Remote mode: observe an EXISTING cluster (e.g. on EC2) instead of spawning one.
    // Kill/add buttons only manage local children, so against a remote cluster they
    // simply have nothing to act on. May be a comma-separated coordinator list; reads
    // go to the first coordinator that answers.
    let remote = std::env::var("AETHER_DASHBOARD_REMOTE").ok();
    let coordinator_addrs = match &remote {
        Some(list) => common::client::parse_addr_list(list, "127.0.0.1:50050"),
        None => vec!["127.0.0.1:50050".to_string()],
    };
    let raft = std::env::var("AETHER_CONSENSUS").map(|c| c.eq_ignore_ascii_case("raft")).unwrap_or(false);

    // Locally spawned children register with the first (only) local coordinator.
    let supervisor = Supervisor::new(shard_count, coordinator_addrs[0].clone(), raft);
    let spawned = if remote.is_none() {
        supervisor.spawn_coordinator()?;
        // Give the coordinator a beat to bind before nodes try to register (they retry anyway).
        tokio::time::sleep(Duration::from_millis(400)).await;
        supervisor.spawn_initial_topology()?
    } else {
        println!("dashboard: remote mode, observing {}", coordinator_addrs.join(", "));
        Vec::new()
    };

    let (state_tx, state_rx) = tokio::sync::watch::channel("{}".to_string());
    let app = Arc::new(App {
        supervisor,
        coordinator_addrs,
        state_rx,
        events: Mutex::new(Vec::new()),
        query_ok: AtomicU64::new(0),
        query_err: AtomicU64::new(0),
        started: Instant::now(),
    });
    for node in spawned {
        app.push_event(format!("spawned {node}"));
    }
    app.push_event("spawned coordinator".to_string());

    // Poller: once a second, snapshot cluster state + run one live query; publish as JSON.
    tokio::spawn(poller(app.clone(), state_tx));

    let router = Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/styles.css", get(styles_css))
        .route("/ws", get(ws_upgrade))
        .route("/api/ask", get(api_ask))
        .route("/api/state", get(api_state))
        .route("/api/kill/:node_id", post(api_kill))
        .route("/api/drain/:node_id", post(api_drain))
        .route("/api/add-follower/:shard_id", post(api_add_follower))
        .with_state(app.clone());

    println!("dashboard on http://{http_addr}  (ctrl-c stops the whole cluster)");
    let listener = tokio::net::TcpListener::bind(&http_addr).await?;

    // Make ctrl-c tear down every child process — never orphan the cluster.
    let shutdown_app = app.clone();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown_app.supervisor.kill_all();
        })
        .await?;
    Ok(())
}

/// Once a second: fetch the coordinator's cluster view, run one query through it, detect
/// promotions by diffing roles, and publish the combined snapshot for the UI.
type Coord = common::pb::coordinator_client::CoordinatorClient<tonic::transport::Channel>;

/// Run one aggregate through the coordinator; return (buckets, resolved percentiles).
async fn run_agg(
    client: &mut Coord,
    kind: common::pb::AggKind,
    field: &str,
    interval: f64,
    percentiles: &[f64],
) -> Option<(std::collections::HashMap<String, u64>, Vec<common::pb::Percentile>)> {
    let resp = client
        .aggregate(common::net::with_token(common::pb::AggregateRequest {
            query: String::new(),
            kind: kind as i32,
            field: field.to_string(),
            interval,
            percentiles: percentiles.to_vec(),
            filter: None,
        }))
        .await
        .ok()?
        .into_inner();
    resp.partial.map(|p| (p.buckets, resp.percentiles))
}

/// Turn a bucket map into sorted `{<key_name>, count}` rows (biggest first, top `n`).
fn bucket_rows(buckets: std::collections::HashMap<String, u64>, key_name: &str, n: usize) -> serde_json::Value {
    let mut rows: Vec<(String, u64)> = buckets.into_iter().collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    rows.truncate(n);
    json!(rows.into_iter().map(|(k, v)| json!({ key_name: k, "count": v })).collect::<Vec<_>>())
}

async fn poller(app: Arc<App>, state_tx: tokio::sync::watch::Sender<String>) {
    // The live query's term must match what the configured source actually produces:
    // OpenSky documents carry origin countries ("United States"...); synthetic ones all
    // carry "Synthetica". Overridable for custom demos.
    let query_term = std::env::var("AETHER_DASHBOARD_QUERY").unwrap_or_else(|_| {
        if std::env::var("AETHER_SOURCE").as_deref() == Ok("synthetic") {
            "synthetica".to_string()
        } else {
            "united".to_string()
        }
    });
    let mut prev_roles: HashMap<String, i32> = HashMap::new();
    // Time-series ring buffer (last ~120 ticks): query latency, matched count, and errors
    // over time. A node kill draws itself here — the latency blip and the recovery.
    let mut series: std::collections::VecDeque<serde_json::Value> = std::collections::VecDeque::new();
    let mut prev_err = 0u64;
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    loop {
        ticker.tick().await;

        let mut coordinator_reachable = false;
        let mut nodes_json = Vec::new();
        let mut shard_count = 0;
        let mut vshard_group: Vec<u32> = Vec::new();
        let mut last_query = json!(null);
        let mut by_origin = json!([]);
        let mut by_aircraft = json!([]);
        let mut altitude_hist = json!([]);
        let mut altitude_pcts = json!([]);
        let mut geo_cells = json!([]);
        let mut last_ms = 0u64;
        let mut last_matched = 0u64;

        if let Ok(mut client) = common::client::connect_first_healthy(&app.coordinator_addrs).await {
            // Cluster topology as the coordinator sees it.
            if let Ok(resp) = client.get_cluster_state(common::net::with_token(ClusterStateRequest {})).await {
                coordinator_reachable = true;
                let state = resp.into_inner();
                shard_count = state.shard_count;
                vshard_group = state.vshard_group.clone();
                let managed = app.supervisor.managed();
                for node in &state.nodes {
                    let role = NodeRole::try_from(node.role).unwrap_or(NodeRole::Unspecified);
                    // A follower that shows up as leader was promoted — failover happened.
                    if let Some(&prev) = prev_roles.get(&node.node_id) {
                        if prev == NodeRole::Follower as i32 && node.role == NodeRole::Leader as i32 {
                            app.push_event(format!(
                                "PROMOTED {} to leader of shard {}",
                                node.node_id, node.shard_id
                            ));
                        }
                    }
                    prev_roles.insert(node.node_id.clone(), node.role);
                    nodes_json.push(json!({
                        "node_id": node.node_id,
                        "address": node.address,
                        "role": format!("{role:?}"),
                        "shard_id": node.shard_id,
                        "millis_since_seen": node.millis_since_seen,
                        "has_process": managed.contains(&node.node_id),
                        "draining": node.draining,
                    }));
                }
            }

            // One live query through the scatter-gather path.
            let t0 = Instant::now();
            match client.search(common::net::with_token(SearchRequest { query: query_term.clone(), limit: 3, filter: None })).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    app.query_ok.fetch_add(1, Ordering::Relaxed);
                    last_ms = t0.elapsed().as_millis() as u64;
                    last_matched = r.total_matched;
                    // Surface the provenance manifest so the panel shows coverage, drops,
                    // freshness, and the placement version behind each live query.
                    let provenance = r.manifest.as_ref().map(|m| {
                        json!({
                            "summary": common::client::manifest_summary(m),
                            "omitted": m.omitted.iter()
                                .map(|o| json!({ "address": o.address, "reason": o.reason }))
                                .collect::<Vec<_>>(),
                            "deduped": m.deduped,
                            "freshest_observed_at": m.freshest_observed_at,
                            "placement_version": m.placement_version,
                        })
                    });
                    last_query = json!({
                        "ok": true,
                        "total_matched": r.total_matched,
                        "answered": r.shards_answered,
                        "queried": r.shards_queried,
                        "ms": t0.elapsed().as_millis() as u64,
                        "provenance": provenance,
                    });
                }
                Err(status) => {
                    app.query_err.fetch_add(1, Ordering::Relaxed);
                    last_query = json!({ "ok": false, "error": status.to_string() });
                }
            }

            // Live aggregates: every panel below is a VIEW over a real distributed
            // aggregation (each shard's partial, merged at the coordinator) — the data
            // layer the charts render, proven here as numbers.
            by_origin = run_agg(&mut client, common::pb::AggKind::AggValueCounts, "origin", 0.0, &[])
                .await
                .map(|(buckets, _)| bucket_rows(buckets, "origin", 8))
                .unwrap_or_else(|| json!([]));

            // Geo-density grid (10° cells): the map panel's source.
            geo_cells = run_agg(&mut client, common::pb::AggKind::AggGeoGrid, "", 10.0, &[])
                .await
                .map(|(buckets, _)| {
                    buckets
                        .into_iter()
                        .filter_map(|(cell, count)| {
                            let (lat, lon) = cell.split_once(',')?;
                            Some(json!({ "lat": lat.parse::<f64>().ok()?, "lon": lon.parse::<f64>().ok()?, "count": count }))
                        })
                        .collect::<Vec<_>>()
                        .into()
                })
                .unwrap_or_else(|| json!([]));

            // Value-counts by aircraft type (categorical bars).
            by_aircraft = run_agg(&mut client, common::pb::AggKind::AggValueCounts, "aircraft_type", 0.0, &[])
                .await
                .map(|(b, _)| bucket_rows(b, "aircraft_type", 8))
                .unwrap_or_else(|| json!([]));

            // Altitude distribution (2000 m buckets) + percentiles (the t-digest).
            altitude_hist = run_agg(&mut client, common::pb::AggKind::AggNumericHistogram, "altitude", 2000.0, &[])
                .await
                .map(|(b, _)| {
                    let mut rows: Vec<(f64, u64)> =
                        b.into_iter().filter_map(|(k, v)| Some((k.parse::<f64>().ok()?, v))).collect();
                    rows.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                    json!(rows.into_iter().map(|(k, v)| json!({ "bucket": k, "count": v })).collect::<Vec<_>>())
                })
                .unwrap_or_else(|| json!([]));
            altitude_pcts = run_agg(&mut client, common::pb::AggKind::AggPercentiles, "altitude", 0.0, &[50.0, 90.0, 99.0])
                .await
                .map(|(_, pcts)| json!(pcts.into_iter().map(|p| json!({ "p": p.p, "value": p.value })).collect::<Vec<_>>()))
                .unwrap_or_else(|| json!([]));
        }

        // Append this tick to the time-series (latency, matched, and whether an error
        // occurred this tick — the node-kill signature is a latency blip + an error tick).
        let err_now = app.query_err.load(Ordering::Relaxed);
        series.push_back(json!({
            "t": app.started.elapsed().as_secs(),
            "ms": last_ms,
            "matched": last_matched,
            "errored": err_now > prev_err,
        }));
        prev_err = err_now;
        while series.len() > 120 {
            series.pop_front();
        }

        // The WebSocket snapshot contract (v1): every panel is a pure render of a slice of
        // this. Bump `v` on a breaking shape change; the SPA reads it for compatibility.
        let snapshot = json!({
            "v": 1,
            "shard_count": shard_count,
            "vshard_group": vshard_group,
            "coordinator": { "addr": app.supervisor.coordinator_addr, "reachable": coordinator_reachable },
            "nodes": nodes_json,
            "query": {
                "ok": app.query_ok.load(Ordering::Relaxed),
                "err": app.query_err.load(Ordering::Relaxed),
                "last": last_query,
            },
            "aggregate": {
                "by_origin": by_origin,
                "by_aircraft": by_aircraft,
                "geo_cells": geo_cells,
                "altitude_hist": altitude_hist,
                "altitude_pcts": altitude_pcts,
            },
            "series": series.iter().cloned().collect::<Vec<_>>(),
            "events": app.events.lock().unwrap().clone(),
        });
        let _ = state_tx.send(snapshot.to_string());
    }
}

async fn index() -> Html<&'static str> {
    Html(include_str!("web/index.html"))
}

async fn app_js() -> impl IntoResponse {
    ([("content-type", "text/javascript; charset=utf-8")], include_str!("web/app.js"))
}

async fn styles_css() -> impl IntoResponse {
    ([("content-type", "text/css; charset=utf-8")], include_str!("web/styles.css"))
}

/// The NLQ search bar: run one question through the read-only tool loop and return the
/// composed answer + its provenance evidence. Uses the live Bedrock model when configured
/// (AETHER_BEDROCK_MODEL), else the offline heuristic planner — the loop, tools, and
/// provenance composition are identical either way.
async fn api_ask(axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>) -> impl IntoResponse {
    let question = params.get("q").cloned().unwrap_or_default();
    if question.trim().is_empty() {
        return ([("content-type", "application/json")], json!({ "error": "empty question" }).to_string());
    }
    let answer = match nlq::bedrock::from_env().await {
        Some(model) => nlq::run(model.as_ref(), &nlq::EngineTools, &question, nlq::Budget::default()).await,
        None => nlq::run(&nlq::HeuristicModel, &nlq::EngineTools, &question, nlq::Budget::default()).await,
    };
    let body = json!({
        "answer": answer.text,
        "provenance": answer.provenance,
        "tool_calls": answer.tool_calls,
        "budget_exhausted": answer.budget_exhausted,
    });
    ([("content-type", "application/json")], body.to_string())
}

async fn api_state(State(app): State<Arc<App>>) -> impl IntoResponse {
    let body = app.state_rx.borrow().clone();
    ([("content-type", "application/json")], body)
}

async fn ws_upgrade(ws: WebSocketUpgrade, State(app): State<Arc<App>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_push(socket, app))
}

/// Push every state snapshot to the browser as it is published.
async fn ws_push(mut socket: WebSocket, app: Arc<App>) {
    let mut rx = app.state_rx.clone();
    loop {
        let payload = rx.borrow_and_update().clone();
        if socket.send(Message::Text(payload)).await.is_err() {
            return; // client went away
        }
        if rx.changed().await.is_err() {
            return; // publisher went away
        }
    }
}

async fn api_kill(State(app): State<Arc<App>>, Path(node_id): Path<String>) -> impl IntoResponse {
    if app.supervisor.kill(&node_id) {
        app.push_event(format!("KILLED {node_id} (SIGKILL)"));
        (axum::http::StatusCode::OK, "killed")
    } else {
        (axum::http::StatusCode::NOT_FOUND, "no such managed node")
    }
}

/// Deliberately drain a node: the coordinator marks it, its group's leader removes it from
/// consensus. The tile shows "draining"; once its store plateaus, kill it — a relocation.
async fn api_drain(State(app): State<Arc<App>>, Path(node_id): Path<String>) -> impl IntoResponse {
    match common::client::connect_first_healthy(&app.coordinator_addrs).await {
        Ok(mut client) => match client.drain_node(common::net::with_token(DrainRequest { node_id: node_id.clone() })).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                if resp.ok {
                    app.push_event(format!("DRAINING {node_id} — leader will remove it from the group"));
                    (axum::http::StatusCode::OK, resp.message)
                } else {
                    (axum::http::StatusCode::NOT_FOUND, resp.message)
                }
            }
            Err(e) => (axum::http::StatusCode::BAD_GATEWAY, e.to_string()),
        },
        Err(e) => (axum::http::StatusCode::BAD_GATEWAY, e.to_string()),
    }
}

async fn api_add_follower(
    State(app): State<Arc<App>>,
    Path(shard_id): Path<u32>,
) -> impl IntoResponse {
    match app.supervisor.add_follower(shard_id) {
        Ok(node_id) => {
            if app.supervisor.is_raft() {
                // The group's leader will admit it live: learner (catch-up) -> voter.
                app.push_event(format!("spawned {node_id} — joining the raft group"));
            } else {
                app.push_event(format!("spawned {node_id}"));
            }
            (axum::http::StatusCode::OK, node_id)
        }
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}
