# Aether

A distributed spatial-vector search engine, written from scratch in Rust.

A coordinator (control plane) fans queries across **N shard nodes** (data plane), each of
which replicates to a follower so the cluster keeps serving when a node dies. It ingests
live flight telemetry from the [OpenSky Network](https://opensky-network.org/) as its
document source.

## Architecture

```
                         ┌──────────────────────────┐
        query ──────────▶│        Coordinator       │   control plane
                         │  discovery · shard map   │   (nodes register at runtime → N)
                         │  scatter-gather · merge  │
                         └───────┬──────────┬───────┘
                        Search   │          │  Search        (same gRPC contract,
                     ┌───────────┘          └───────────┐     fanned out then merged)
                     ▼                                  ▼
             ┌───────────────┐                  ┌───────────────┐
             │  Shard node 0 │  ...  N shards   │ Shard node N-1│   data plane
             │  inverted idx │                  │  inverted idx │
             │  leader       │                  │  leader       │
             └──────┬────────┘                  └──────┬────────┘
                    │ replicate                        │ replicate
                    ▼                                  ▼
             ┌───────────────┐                  ┌───────────────┐
             │   follower    │                  │   follower    │   promoted on failover
             └───────────────┘                  └───────────────┘
```

- **Shard key:** `icao24` (aircraft id) — high-cardinality and evenly distributed, so
  `hash(icao24) % N` gives balanced shards. Cluster size `N` is a runtime parameter.
  See `crates/common/src/shard.rs`.
- **Wire contract:** gRPC via `tonic` + Protocol Buffers (`proto/aether.proto`). No JSON on
  the data plane.

## Workspace layout

```
aether-engine/
  proto/              gRPC contract (.proto) — single source of truth for the wire format
  crates/
    common/           generated contract, shard-key hashing, embedders, filters
    coordinator/      control plane: discovery, shard map, scatter-gather, merge, hybrid rank
    shard-node/       data plane: inverted index, vector index, aggregations, ingestion
    consensus/        openraft integration: per-shard raft groups over the document log
    testkit/          in-process cluster harness for integration tests
    dashboard/        live UI + chaos harness (spawns a real cluster as child processes)
    agent-tools/      the read-only tool surface (search / aggregate / filter / topology)
    mcp-agent/        Model Context Protocol server exposing the read-only tools to an LLM
    nlq/              natural-language query loop (plan → tools → provenance-carrying answer)
    memory/           agent memory as a governed index type (namespaced, TTL'd, quota'd)
    federation/       cross-cluster search: scatter-gather over coordinators
    briefing/         the capstone: a scheduled agent with one audited email egress
```

## Status

A single shard node ingests live flight data from OpenSky into an in-memory inverted index
and serves keyword search over gRPC — the `aether.v1` contract, shard-key hashing, the
inverted index, the `ShardSearch` server, and a pull-based ingestion loop with backpressure.
Verified end-to-end against live data (~13k flights).

The coordinator serves dynamic node registration (holding an N-parameterized shard map) and
scatter-gather search: it fans a query across all shard leaders concurrently, merges the
hits into one ranked list, and reports coverage (partial results if a shard is down). Shard
nodes register on startup and ingest only the documents they own (`hash(icao24) % N`). A
shard leader replicates each indexed batch to its follower(s), which hold the same slice of
data and can be promoted to serve it if the leader fails. Nodes heartbeat the coordinator,
which drops any node that goes silent and promotes a live follower to leader in its place, so
a shard whose leader dies keeps being served without interruption — verified by a test that
kills a leader under continuous query load and asserts the query stream never breaks.

Queries can also be **streamed**: `SearchStream` emits a refined result each time a shard
reports, so results materialize progressively and keep converging even if a shard dies
mid-aggregation.

Each shard also serves **semantic vector search** (`VectorSearch`): documents are embedded
into a vector space and queried by k-nearest-neighbour over an HNSW index (with an exact
scan below a size threshold, where approximate search doesn't pay). The default embedder is
a deterministic feature-hashing baseline; an ONNX sentence-transformer (quantized MiniLM)
is available behind the `onnx` feature — fetch the model with `scripts/fetch-model.sh` and
select it with `AETHER_EMBEDDER=onnx AETHER_ONNX_MODEL_DIR=...`. Every node in a cluster
must use the same embedder: embeddings are a cross-node contract, and shards reject query
vectors whose dimension doesn't match their own.

Vectors can be **binary-quantized** (AETHER_VECTOR=quantized): each dimension collapses to
its sign bit plus a per-vector correction — 1536 B becomes 52 B (~30x) — and search runs
in two tiers: a Hamming scan over the compressed forms (XOR+popcount, measured ~5 ns/doc,
~69x faster than exact) generates 4x-oversampled candidates, which are then rescored with
exact f32 dot products. Measured on a clustered corpus: recall@10 of 1.0 versus the exact
scan, at 29.5x compression of the scanned representation.

## Consensus

Shards can run under real consensus via [`openraft`](https://github.com/databendlabs/openraft)
(`AETHER_CONSENSUS=raft`): the members serving one shard form a raft group whose replicated
log is the document stream, with the shard's store as the state machine — leader election,
quorum-committed writes, and split-brain handling by construction (raft shards run 3+
members, since a group of 2 cannot survive a failure). Members discover their group through
the coordinator and the member with the smallest raft id initializes it; the **elected**
leader ingests, writing every batch through the log so it commits into all members' stores;
heartbeats report raft leadership, so the coordinator's shard map is a *view* of raft state
rather than an authority. Groups grow live: a joining node (started with
`AETHER_RAFT_JOIN=1`) is admitted by the elected leader as a learner — catching up from
replication with zero quorum impact — then promoted to voter, while queries and ingestion
continue uninterrupted. Verified in-process (election, quorum-searchable writes,
re-election) and across real processes: three shard-node binaries form a group, the elected
leader is SIGKILLed, the survivors re-elect, and query routing follows the new leader.

Placement can run on **virtual shards** (AETHER_VSHARDS on the coordinator): documents map
to a fixed number of virtual shards — the modulus never changes — and a coordinator-owned
table assigns virtual shards to groups. Reassigning one moves ingestion between groups
live (nodes follow the table, no restarts), while the coordinator deduplicates merged
results by freshest observation so queries stay correct during the overlap. Verified
across six real processes in two raft groups: every virtual shard moved off a group under
query load, its leader stopped ingesting while the cluster kept growing, with zero query
errors and no duplicate results.

## Live dashboard / chaos harness

`cargo run -p dashboard` spawns a whole cluster (coordinator + a leader and follower per
shard) as **real child processes** and serves a live UI at `http://127.0.0.1:8080`: node
health per shard, a continuous query stream's throughput and coverage, and an event log.
The **kill −9** button SIGKILLs the actual process — watch coverage degrade to partial,
the follower get promoted (~6s), and coverage return, with zero failed queries. **Add
follower** spawns a fresh node that registers and catches up from replication. Ctrl-C
tears down every child.

## Aggregations & analytics

Beyond retrieval, shards compute **aggregations** in the same scatter-gather pass: value
counts (top origins, aircraft types), numeric histograms and a geo-density map, and
**percentiles** via a mergeable [t-digest](https://github.com/tdunning/t-digest) — each
shard streams a compact digest and the coordinator merges them into cluster-wide quantiles
without shipping raw values. Every aggregation composes with a **structured filter**
(equality, ranges, geo-bounds over the document fields), so "the p99 altitude of aircraft
over France above 10,000 m" is one filtered aggregate fanned across the cluster. Filters and
aggregations share the read path, so partial results under a downed shard degrade the same
honest way search does.

## Ranking: keyword, vector, hybrid

Keyword (BM25) and vector (cosine) ranking each win different queries — BM25's IDF nails
rare exact terms, the embedder captures breadth — so the coordinator fuses them with
**Reciprocal Rank Fusion**. The choice is *measured*, not assumed: a ranking-eval harness
(`crates/coordinator/tests/ranking_eval.rs`) scores NDCG@10 over a judgment set and gates in
CI, so no ranking change ships without a number. A deliberately simple adaptive router was
built, measured against RRF, and **rejected with numbers** — cheap query features couldn't
tell a rare *relevance* signal from a rare *distractor*, so RRF stands.

## Natural language, live

`crates/nlq` is a planning loop: a model reads a plain-English question, calls the read-only
tools until it can answer, and the answer carries the **merged provenance** of every tool
result (which shards answered, freshness, coverage). The loop runs under a hard tool-call
budget — a confused model costs a bounded number of calls, then returns an honestly-labeled
partial answer rather than hanging. The loop is generic over the planner: CI drives it with
a scripted model (no network), and live it runs against **any OpenAI-compatible endpoint**
(`--features openai` — OpenAI, Groq's free hosted-Llama tier, Together, OpenRouter) or
**Bedrock** (`--features bedrock`). An env-gated eval harness scores routing over a judgment
set and, when no live model is reachable, falls back to an offline heuristic planner so it
always runs (6/6 on the routing smoke-eval).

## Agent platform: memory, federation, generality, the capstone

- **Governed memory** (`crates/memory`): agent memory as a first-class index type rather
  than an unmanaged side store — namespace-isolated (cross-namespace recall impossible by
  construction), per-writer quota'd, TTL'd (expiry frees quota), and writer-attributed. It
  lives in a store separate from the read-only telemetry index, so the one place an agent
  may write can never touch the data plane.
- **Cross-cluster federation** (`crates/federation`): one query fanned across independent
  clusters, treating each coordinator as a super-shard and reusing the coordinator's own
  merge/coverage — scatter-gather one level up. A downed cluster is *named* in the coverage
  manifest, not silently dropped; cross-cluster freshness is honestly advisory (independent
  clocks).
- **SIEM generality**: pointing the *same* engine at security events (a source behind the
  existing ingestion trait) with **zero new distributed machinery** — detection breakdowns,
  entity pivots and timelines are just the existing filtered aggregations. It also surfaced
  a real modeling lesson (per-event records key by event id, not entity, or the upsert
  collapses the stream).
- **The capstone** (`crates/briefing`): a scheduled agent that reads the cluster in plain
  English and composes a provenance-carrying briefing, whose single outward action is an
  email hand-off — triple-gated by a recipient allowlist, a dry-run default, and a
  build-time feature for real SMTP. Everything else stays structurally read-only.

## Performance

`./scripts/bench.sh` is a closed-loop load generator (`coordinator/examples/loadgen.rs`):
it fans N concurrent workers at the coordinator, issuing back-to-back queries, and reports
throughput and latency percentiles. Measured on one Apple M1 (8 cores, 8 GB), 32 concurrent
clients, keyword search over a synthetic corpus:

| shards | throughput | p50 | p99 |
|-------:|-----------:|----:|----:|
| 1 | ~3700 QPS | 4.0 ms | 50 ms |
| 2 | ~1800 QPS | 5.6 ms | 60 ms |
| 4 | ~580 QPS | 60 ms | 133 ms |

Read these honestly: **all shards here are co-located on one 8-core host**, so adding shards
adds scatter-gather fan-out and merge cost plus CPU contention *without adding hardware* —
throughput therefore falls with shard count. This measures per-host coordination overhead,
not horizontal scale-out; the scale-out win requires one shard per machine (each shard adds
its own cores and index), where the coordinator's fan-out is the point. Single-digit-ms p50
at 1–2 shards shows the query path itself is interactive; the tail grows once 32 clients ×
fan-out saturates the shared cores. Run `./scripts/bench.sh 10 64 count` to vary duration,
concurrency, and query kind.

## Run

A single node:

```bash
cargo run -p shard-node                                    # serves on 127.0.0.1:50051
cargo run -p shard-node --example query -- united 5        # query it
```

A cluster (each in its own terminal):

```bash
AETHER_SHARD_COUNT=2 cargo run -p coordinator
AETHER_SHARD_INDEX=0 AETHER_SHARD_COUNT=2 AETHER_SHARD_ADDR=127.0.0.1:50051 AETHER_COORDINATOR_ADDR=127.0.0.1:50050 cargo run -p shard-node
AETHER_SHARD_INDEX=1 AETHER_SHARD_COUNT=2 AETHER_SHARD_ADDR=127.0.0.1:50052 AETHER_COORDINATOR_ADDR=127.0.0.1:50050 cargo run -p shard-node
cargo run -p coordinator --example cluster_query -- united 5
```

## Observability

The coordinator exposes Prometheus metrics on a **separate** port (`AETHER_METRICS_ADDR`,
default `127.0.0.1:9090`, or `off`), so scraping never touches the gRPC data plane:

```bash
curl -s http://127.0.0.1:9090/metrics
# aether_queries_total / aether_query_errors_total
# aether_aggregates_total / aether_aggregate_errors_total
# aether_query_latency_ms histogram (le=1,5,10,50,100,500,+Inf, _sum, _count)
# aether_cluster_leaders / aether_cluster_shards  (gauges, read from the registry at scrape)
```

The query path is instrumented in `crates/coordinator/src/metrics.rs`; the exposition is
served by a tiny dependency-free HTTP responder. Point Prometheus at `:9090` and the query
histogram and cluster gauges are ready to graph or alert on.

## Deploy (containers)

A real multi-container cluster — one coordinator + three shard nodes, each in its own
container addressing the others by **service name** (not localhost), so the bind/advertise
split works across separate network namespaces exactly as it would across hosts:

```bash
docker compose up --build            # coordinator + 3 shards
# query it from the host:
AETHER_COORDINATOR_ADDR=127.0.0.1:50050 cargo run -p coordinator --example cluster_query -- Synthetica 5
docker compose down
```

The image is a multi-stage build (`Dockerfile`): compile the release binaries once, ship
them on `debian-slim`; the same image runs either role, selected by `command:` in
`docker-compose.yml`. Verified end-to-end: a fan-out query returns hits merged across 3/3
containerized shards.

## Build

```bash
cargo build            # requires protoc on PATH (brew install protobuf)
cargo test -p common   # shard-key hashing tests
```

## License

MIT.

## Agent access (MCP, read-only)

`cargo run -p mcp-agent` starts `aether-mcp`, a [Model Context Protocol](https://modelcontextprotocol.io)
server over stdio that lets an LLM agent query the cluster natively with three tools:
keyword search, semantic search, and cluster topology. The boundary is structural — the
binary only links the read RPCs (`Search`, `VectorSearch`, `GetClusterState`), so an agent
can at worst return a wrong result set; all mutation (elections, membership, placement)
belongs to the cluster's own deterministic machinery.
