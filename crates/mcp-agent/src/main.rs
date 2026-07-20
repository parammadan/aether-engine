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

use common::embed::{Embedder, HashEmbedder};
use common::pb::coordinator_client::CoordinatorClient;
use common::pb::{ClusterStateRequest, NodeRole, SearchRequest, VectorSearchRequest};

async fn connect() -> Result<CoordinatorClient<tonic::transport::Channel>, String> {
    common::client::connect_first_healthy(&common::client::coordinator_addrs("127.0.0.1:50050"))
        .await
}

// =============================================================================
// Tools (all read-only)
// =============================================================================

fn tool_definitions() -> Value {
    json!([
        {
            "name": "search_flights",
            "description": "Keyword search over live flight documents (callsign, origin, \
                            destination, aircraft type). Fans out across every shard and \
                            returns a merged, ranked result.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "keywords, e.g. a callsign or a country" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 50, "description": "max results (default 5)" },
                    "filters": { "type": "array", "description": "structured conditions, ANDed. Each: {field, equals} for text (origin, aircraft_type, callsign...), {field, min?, max?} for numeric (altitude, velocity, observed_at...), {field, is} for on_ground.", "items": { "type": "object" } }
                },
                "required": ["query"]
            }
        },
        {
            "name": "semantic_search_flights",
            "description": "Semantic (vector) search: the query text is embedded and matched \
                            by meaning against flight documents, fanned across every shard.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "free-text description of what to find" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 50, "description": "max results (default 5)" }
                },
                "required": ["query"]
            }
        },
        {
            "name": "cluster_state",
            "description": "Current cluster topology as the coordinator sees it: shards, \
                            leaders and followers, liveness, and virtual-shard placement. \
                            Read-only telemetry.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "aggregate_flights",
            "description": "Summarize flights across the whole cluster: counts, value-counts \
                            (per origin/aircraft type/...), histograms (time or a numeric \
                            field), geo-density grid, or percentiles of a numeric field. \
                            Each shard computes a partial; the coordinator merges them and \
                            reports coverage.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "enum": ["count", "value_counts", "time_histogram", "numeric_histogram", "geo_grid", "percentiles"], "description": "which aggregation" },
                    "query": { "type": "string", "description": "optional keyword filter (empty = all flights)" },
                    "field": { "type": "string", "description": "field for value_counts (origin, aircraft_type, ...) or numeric_histogram/percentiles (altitude, velocity, ...)" },
                    "interval": { "type": "number", "description": "bucket width: ms for time, units for numeric, degrees for geo_grid" },
                    "percentiles": { "type": "array", "items": { "type": "number" }, "description": "for percentiles, e.g. [50, 95, 99]" },
                    "filters": { "type": "array", "description": "structured conditions, ANDed. Each: {field, equals} for text, {field, min?, max?} for numeric, {field, is} for on_ground.", "items": { "type": "object" } }
                },
                "required": ["kind"]
            }
        }
    ])
}

fn format_hits(resp: &common::pb::SearchResponse) -> String {
    let mut out = format!(
        "{} matched across {}/{} shards\n",
        resp.total_matched, resp.shards_answered, resp.shards_queried
    );
    for hit in &resp.hits {
        let d = hit.document.clone().unwrap_or_default();
        let via = hit.provenance.as_ref().map(|p| p.source_group.as_str()).unwrap_or("?");
        out.push_str(&format!(
            "- {} {} ({} -> {}) {} score={:.3} [via {via}]\n",
            d.icao24, d.callsign, d.origin, d.destination, d.aircraft_type, hit.score
        ));
    }
    // The agent can quote its own audit trail: which shards answered, what was dropped,
    // how fresh the data is. An answer built on this inherits the provenance for free.
    if let Some(m) = &resp.manifest {
        out.push_str(&format!("\nprovenance: {}\n", common::client::manifest_summary(m)));
    }
    out
}

/// Parse the tools' `filters` argument into the wire Filter. Shape errors are tool errors
/// (the model can read them and fix its call); semantic errors (unknown field) surface
/// from the server's own validation.
fn parse_filters(args: &Value) -> Result<Option<common::pb::Filter>, String> {
    use common::pb::{filter_condition::Test, Filter, FilterCondition, NumericRange};
    let Some(list) = args.get("filters").and_then(|v| v.as_array()) else {
        return Ok(None);
    };
    let mut conditions = Vec::new();
    for c in list {
        let field = c
            .get("field")
            .and_then(|v| v.as_str())
            .ok_or("each filter needs a 'field'")?
            .to_string();
        let test = if let Some(eq) = c.get("equals").and_then(|v| v.as_str()) {
            Test::Equals(eq.to_string())
        } else if c.get("min").is_some() || c.get("max").is_some() {
            Test::Range(NumericRange {
                min: c.get("min").and_then(|v| v.as_f64()),
                max: c.get("max").and_then(|v| v.as_f64()),
            })
        } else if let Some(b) = c.get("is").and_then(|v| v.as_bool()) {
            Test::Is(b)
        } else {
            return Err(format!("filter on '{field}' needs 'equals', 'min'/'max', or 'is'"));
        };
        conditions.push(FilterCondition { field, test: Some(test) });
    }
    Ok(if conditions.is_empty() { None } else { Some(Filter { conditions }) })
}

async fn call_tool(name: &str, args: &Value) -> Result<String, String> {
    let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(5) as u32;
    match name {
        "search_flights" => {
            let query = args
                .get("query")
                .and_then(|v| v.as_str())
                .ok_or("missing required argument: query")?;
            let mut client = connect().await?;
            let resp = client
                .search(common::net::with_token(SearchRequest { query: query.to_string(), limit, filter: parse_filters(args)? }))
                .await
                .map_err(|e| e.to_string())?
                .into_inner();
            Ok(format_hits(&resp))
        }
        "semantic_search_flights" => {
            let query = args
                .get("query")
                .and_then(|v| v.as_str())
                .ok_or("missing required argument: query")?;
            let vector = HashEmbedder.embed(query);
            let mut client = connect().await?;
            let resp = client
                .vector_search(common::net::with_token(VectorSearchRequest { vector, limit, filter: None }))
                .await
                .map_err(|e| e.to_string())?
                .into_inner();
            Ok(format_hits(&resp))
        }
        "cluster_state" => {
            let mut client = connect().await?;
            let state = client
                .get_cluster_state(common::net::with_token(ClusterStateRequest {}))
                .await
                .map_err(|e| e.to_string())?
                .into_inner();
            let mut out = format!("{} shard group(s)\n", state.shard_count);
            for node in &state.nodes {
                let role = NodeRole::try_from(node.role).unwrap_or(NodeRole::Unspecified);
                out.push_str(&format!(
                    "- shard {} · {} · {:?} · seen {:.1}s ago{}\n",
                    node.shard_id,
                    node.node_id,
                    role,
                    node.millis_since_seen as f64 / 1000.0,
                    if node.draining { " · draining" } else { "" }
                ));
            }
            if !state.vshard_group.is_empty() {
                out.push_str(&format!("virtual shards -> groups: {:?}\n", state.vshard_group));
            }
            Ok(out)
        }
        "aggregate_flights" => {
            use common::pb::{AggKind, AggregateRequest};
            let kind = match args.get("kind").and_then(|v| v.as_str()).unwrap_or("count") {
                "count" => AggKind::AggCount,
                "value_counts" => AggKind::AggValueCounts,
                "time_histogram" => AggKind::AggTimeHistogram,
                "numeric_histogram" => AggKind::AggNumericHistogram,
                "geo_grid" => AggKind::AggGeoGrid,
                "percentiles" => AggKind::AggPercentiles,
                other => return Err(format!("unknown aggregation kind: {other}")),
            };
            let req = AggregateRequest {
                query: args.get("query").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                kind: kind as i32,
                field: args.get("field").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                interval: args.get("interval").and_then(|v| v.as_f64()).unwrap_or(0.0),
                percentiles: args
                    .get("percentiles")
                    .and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|x| x.as_f64()).collect())
                    .unwrap_or_default(),
                filter: parse_filters(args)?,
            };
            let mut client = connect().await?;
            let resp = client
                .aggregate(common::net::with_token(req))
                .await
                .map_err(|e| e.to_string())?
                .into_inner();
            Ok(format_aggregate(kind, &resp))
        }
        other => Err(format!("unknown tool: {other}")),
    }
}

/// Render a merged aggregate for the agent: count, bucket table (biggest first), or
/// resolved percentiles — plus the provenance line so the answer carries its coverage.
fn format_aggregate(kind: common::pb::AggKind, resp: &common::pb::AggregateResponse) -> String {
    use common::pb::AggKind;
    let mut out = String::new();
    if let Some(p) = &resp.partial {
        match kind {
            AggKind::AggCount => out.push_str(&format!("count: {}\n", p.count)),
            AggKind::AggPercentiles => {
                for pc in &resp.percentiles {
                    out.push_str(&format!("p{}: {:.2}\n", pc.p, pc.value));
                }
            }
            _ => {
                let mut buckets: Vec<(&String, &u64)> = p.buckets.iter().collect();
                buckets.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
                for (k, v) in buckets.iter().take(25) {
                    out.push_str(&format!("{k}: {v}\n"));
                }
                out.push_str(&format!("({} matched)\n", p.count));
            }
        }
    }
    if let Some(m) = &resp.manifest {
        out.push_str(&format!("\nprovenance: {}\n", common::client::manifest_summary(m)));
    }
    out
}

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
        "tools/list" => Some(rpc_result(id, json!({ "tools": tool_definitions() }))),
        "tools/call" => {
            let name = msg.pointer("/params/name").and_then(|v| v.as_str()).unwrap_or("");
            let empty = json!({});
            let args = msg.pointer("/params/arguments").unwrap_or(&empty);
            // Tool failures are tool-level results (isError), not protocol errors: the
            // agent should see "the coordinator is down" as an answer it can act on.
            let (text, is_error) = match call_tool(name, args).await {
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
