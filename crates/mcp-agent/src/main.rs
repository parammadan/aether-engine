//! `aether-mcp` — a read-only MCP server over the cluster.
//!
//! Speaks the Model Context Protocol (JSON-RPC 2.0, newline-delimited over stdio) and
//! exposes three tools an agent can call:
//!   - `search_flights`          keyword search through the coordinator's scatter-gather
//!   - `semantic_search_flights` embeds the query text and runs vector scatter-gather
//!   - `cluster_state`           topology as the coordinator sees it (read-only telemetry)
//!
//! # The read-only boundary (deliberate, structural)
//! The agent is a CLIENT of the engine, outside the data plane. This binary contains no
//! call to any mutating RPC — no registration, no drain, no reassignment, no writes — so
//! the worst a misbehaving agent can produce is a wrong result set, never corrupted
//! cluster state. Deterministic machinery (consensus, reconcilers) owns mutation.
//!
//! # Embedder note
//! `semantic_search_flights` embeds with the deterministic hash embedder — the cluster
//! default. A cluster running a different embedder rejects the query vector loudly (the
//! shard checks dimensions), which surfaces as a tool error rather than silent garbage.
//!
//! Config: AETHER_COORDINATOR_ADDRS (comma-separated, first healthy wins) or
//! AETHER_COORDINATOR_ADDR (default 127.0.0.1:50050).
//! Protocol messages go to stdout ONLY; diagnostics go to stderr.

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};


// =============================================================================
// MCP protocol loop (JSON-RPC 2.0, newline-delimited over stdio)
// =============================================================================

fn rpc_result(id: &Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn rpc_error(id: &Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

async fn handle(msg: &Value) -> Option<Value> {
    let method = msg.get("method").and_then(|m| m.as_str())?;
    let id = msg.get("id");

    // Notifications (no id) get no response.
    let Some(id) = id else { return None };

    match method {
        "initialize" => Some(rpc_result(
            id,
            json!({
                "protocolVersion": msg
                    .pointer("/params/protocolVersion")
                    .cloned()
                    .unwrap_or_else(|| json!("2024-11-05")),
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "aether-query-agent", "version": env!("CARGO_PKG_VERSION") }
            }),
        )),
        "ping" => Some(rpc_result(id, json!({}))),
        "tools/list" => Some(rpc_result(id, json!({ "tools": agent_tools::definitions() }))),
        "tools/call" => {
            let name = msg.pointer("/params/name").and_then(|v| v.as_str()).unwrap_or("");
            let empty = json!({});
            let args = msg.pointer("/params/arguments").unwrap_or(&empty);
            // Tool failures are tool-level results (isError), not protocol errors: the
            // agent should see "the coordinator is down" as an answer it can act on.
            let (text, is_error) = match agent_tools::call(name, args).await {
                Ok(text) => (text, false),
                Err(e) => (e, true),
            };
            Some(rpc_result(
                id,
                json!({ "content": [{ "type": "text", "text": text }], "isError": is_error }),
            ))
        }
        _ => Some(rpc_error(id, -32601, &format!("method not found: {method}"))),
    }
}

#[tokio::main]
async fn main() {
    common::net::install_crypto();
    eprintln!("aether-mcp: read-only query agent over coordinator(s) {}", common::client::coordinator_addrs("127.0.0.1:50050").join(", "));
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();

    while let Ok(Some(line)) = lines.next_line().await {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(msg) = serde_json::from_str::<Value>(line) else {
            eprintln!("aether-mcp: skipping non-JSON line");
            continue;
        };
        if let Some(response) = handle(&msg).await {
            let mut payload = response.to_string();
            payload.push('\n');
            if stdout.write_all(payload.as_bytes()).await.is_err() {
                break; // client hung up
            }
            let _ = stdout.flush().await;
        }
    }
}
