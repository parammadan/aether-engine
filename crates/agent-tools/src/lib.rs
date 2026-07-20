//! The read-only tool surface: definitions (JSON schema) + dispatch. One implementation
//! shared by every agent front-end — the MCP server speaks it over JSON-RPC, the
//! natural-language service hands it to a planning model — so the surface cannot drift
//! between them, and "read-only" is a property of THIS crate's dependency graph (no
//! mutating RPC is linked) enforced again server-side by token scope.

use serde_json::{json, Value};

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

pub fn definitions() -> Value {
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

pub async fn call(name: &str, args: &Value) -> Result<String, String> {
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

