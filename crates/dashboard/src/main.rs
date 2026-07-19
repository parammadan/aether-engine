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
        .route("/ws", get(ws_upgrade))
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
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    loop {
        ticker.tick().await;

        let mut coordinator_reachable = false;
        let mut nodes_json = Vec::new();
        let mut shard_count = 0;
        let mut vshard_group: Vec<u32> = Vec::new();
        let mut last_query = json!(null);

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
            match client.search(common::net::with_token(SearchRequest { query: query_term.clone(), limit: 3 })).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    app.query_ok.fetch_add(1, Ordering::Relaxed);
                    last_query = json!({
                        "ok": true,
                        "total_matched": r.total_matched,
                        "answered": r.shards_answered,
                        "queried": r.shards_queried,
                        "ms": t0.elapsed().as_millis() as u64,
                    });
                }
                Err(status) => {
                    app.query_err.fetch_add(1, Ordering::Relaxed);
                    last_query = json!({ "ok": false, "error": status.to_string() });
                }
            }
        }

        let snapshot = json!({
            "shard_count": shard_count,
            "vshard_group": vshard_group,
            "coordinator": { "addr": app.supervisor.coordinator_addr, "reachable": coordinator_reachable },
            "nodes": nodes_json,
            "query": {
                "ok": app.query_ok.load(Ordering::Relaxed),
                "err": app.query_err.load(Ordering::Relaxed),
                "last": last_query,
            },
            "events": app.events.lock().unwrap().clone(),
        });
        let _ = state_tx.send(snapshot.to_string());
    }
}

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
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
