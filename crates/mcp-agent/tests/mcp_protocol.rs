//! End-to-end MCP protocol test: spawn the real `aether-mcp` binary against a live
//! coordinator + shard node, drive the protocol over stdio (initialize → tools/list →
//! tools/call for all three tools), and assert the agent gets real answers — through a
//! surface that has no mutating code path.

use std::io::{BufRead, BufReader as StdBufReader, Write};
use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common::pb::coordinator_server::CoordinatorServer;
use common::pb::replication_server::ReplicationServer;
use common::pb::shard_search_server::ShardSearchServer;
use common::pb::{FlightDocument, NodeRole, RegisterNodeRequest};
use coordinator::registry::Registry;
use coordinator::service::CoordinatorService;
use serde_json::{json, Value};
use shard_node::replication::ReplicationService;
use shard_node::server::ShardSearchService;
use shard_node::store::ShardStore;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn doc(icao24: &str, callsign: &str, origin: &str) -> FlightDocument {
    FlightDocument {
        icao24: icao24.to_string(),
        callsign: callsign.to_string(),
        origin: origin.to_string(),
        aircraft_type: "TestJet".to_string(),
        ..Default::default()
    }
}

/// Boot a one-shard cluster in-process (coordinator + one serving node with data).
async fn start_cluster() -> String {
    // The shard node, with a couple of searchable documents.
    let mut store = ShardStore::new();
    store.insert(doc("a1b2c3", "UAL231", "United States"));
    store.insert(doc("d4e5f6", "AFR006", "France"));
    let store = Arc::new(RwLock::new(store));
    let shard_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let shard_addr = shard_listener.local_addr().unwrap().to_string();
    let search = ShardSearchService::new(store.clone(), "shard-0".to_string());
    let replication = ReplicationService::new(store);
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(ShardSearchServer::new(search))
            .add_service(ReplicationServer::new(replication))
            .serve_with_incoming(TcpListenerStream::new(shard_listener))
            .await;
    });

    // The coordinator, with the shard registered as leader.
    let registry = Arc::new(RwLock::new(Registry::new(1)));
    registry.write().unwrap().register(RegisterNodeRequest {
        node_id: "n0".to_string(),
        address: shard_addr,
        shard_id: 0,
        role: NodeRole::Leader as i32,
    });
    let service = CoordinatorService::new(registry);
    let coord_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coord_addr = coord_listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(CoordinatorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(coord_listener))
            .await;
    });
    coord_addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_speaks_mcp_and_answers_with_real_cluster_data() {
    let coord_addr = start_cluster().await;

    let mut agent = Command::new(env!("CARGO_BIN_EXE_aether-mcp"))
        .env("AETHER_COORDINATOR_ADDR", &coord_addr)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn aether-mcp");
    let mut stdin = agent.stdin.take().unwrap();
    let mut stdout = StdBufReader::new(agent.stdout.take().unwrap());

    // Drive the protocol from a blocking thread (plain pipes), with an overall timeout.
    let driver = tokio::task::spawn_blocking(move || {
        let mut send = |v: Value| {
            let mut s = v.to_string();
            s.push('\n');
            stdin.write_all(s.as_bytes()).unwrap();
            stdin.flush().unwrap();
        };
        let mut recv = || -> Value {
            let mut line = String::new();
            stdout.read_line(&mut line).unwrap();
            serde_json::from_str(&line).unwrap()
        };

        // initialize
        send(json!({"jsonrpc":"2.0","id":1,"method":"initialize",
                    "params":{"protocolVersion":"2024-11-05","capabilities":{},
                              "clientInfo":{"name":"test","version":"0"}}}));
        let init = recv();
        assert_eq!(init["result"]["serverInfo"]["name"], "aether-query-agent");
        send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));

        // tools/list: exactly the three read-only tools.
        send(json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}));
        let tools = recv();
        let names: Vec<&str> = tools["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["search_flights", "semantic_search_flights", "cluster_state"]);

        // keyword search finds the seeded document.
        send(json!({"jsonrpc":"2.0","id":3,"method":"tools/call",
                    "params":{"name":"search_flights","arguments":{"query":"ual231"}}}));
        let hits = recv();
        let text = hits["result"]["content"][0]["text"].as_str().unwrap();
        assert_eq!(hits["result"]["isError"], false);
        assert!(text.contains("UAL231"), "keyword tool answer: {text}");

        // semantic search runs end-to-end (embed -> coordinator fan-out -> merge).
        send(json!({"jsonrpc":"2.0","id":4,"method":"tools/call",
                    "params":{"name":"semantic_search_flights",
                              "arguments":{"query":"AFR006 France","limit":2}}}));
        let sem = recv();
        let text = sem["result"]["content"][0]["text"].as_str().unwrap();
        assert_eq!(sem["result"]["isError"], false);
        assert!(text.contains("AFR006"), "semantic tool answer: {text}");

        // topology telemetry.
        send(json!({"jsonrpc":"2.0","id":5,"method":"tools/call",
                    "params":{"name":"cluster_state","arguments":{}}}));
        let state = recv();
        let text = state["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("shard 0") && text.contains("Leader"), "state: {text}");

        // unknown tool -> tool-level error, not a protocol failure.
        send(json!({"jsonrpc":"2.0","id":6,"method":"tools/call",
                    "params":{"name":"reassign_vshard","arguments":{}}}));
        let refused = recv();
        assert_eq!(refused["result"]["isError"], true, "mutating-sounding tools must not exist");
    });

    tokio::time::timeout(Duration::from_secs(30), driver)
        .await
        .expect("mcp session timed out")
        .expect("mcp session panicked");
    let _ = agent.kill();
    let _ = agent.wait();
}
