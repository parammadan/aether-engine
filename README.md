# Aether

A distributed spatial-vector search engine, written from scratch in Rust.

Aether is a **fault-tolerant, horizontally scalable** search cluster: a coordinator
(control plane) fans queries across **N shard nodes** (data plane), each of which
replicates to a follower so the cluster keeps serving when a node dies. It ingests live
flight telemetry from the [OpenSky Network](https://opensky-network.org/) — a stand-in for
the log/event streams a search engine typically indexes.

> **Honest framing.** This is a distributed search engine with **chaos-verified design**:
> sharding, replication, and failover that are documented, tested, and exercised by killing
> nodes under load. It is **built to scale** — the cluster size *N* is a runtime parameter,
> never hardcoded — but it has **not been operated at scale**. The goal is to understand the
> hard distributed-systems tradeoffs firsthand, not to reproduce a production system's scale.

## Why it exists

A study of the problems a real distributed search engine solves — sharding, replication,
consensus, failover, live rebalancing — by building a from-scratch implementation and being
able to defend every design decision. Optimized for **defensibility**, not feature count.

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
  `hash(icao24) % N` gives balanced shards. See `crates/common/src/shard.rs`.
- **Wire contract:** gRPC via `tonic` + Protocol Buffers (`proto/aether.proto`). No JSON on
  the data plane.

## Workspace layout

```
aether-engine/
  proto/              gRPC contract (.proto) — single source of truth for the wire format
  crates/
    common/           generated contract + shard-key hashing (shared by both binaries)
    coordinator/      control plane: discovery, shard map, scatter-gather   [stub in Q1]
    shard-node/       data plane: inverted index + ShardSearch gRPC server  [stub in Q1]
```

## Roadmap (24 months, one quarter at a time)

**Year 1 — build the real distributed system**
- **Q1** Single-node keyword search end-to-end over gRPC (proto-first; no embeddings/geo).
- **Q2** The spine: N-parameterized coordinator, `hash(icao24) % N` sharding,
  scatter-gather, leader→follower replication.
- **Q3** Failover + chaos testing: kill a leader under load, follower is promoted, queries
  keep serving. *The core demo.*
- **Q4** Streaming aggregations; add HNSW vector index + ONNX embeddings; live cluster
  dashboard (kill-node / add-node buttons).

**Year 2 — depth on the hard problems**
- **Q5** Real consensus via `openraft` (leader election, log replication, split-brain).
- **Q6** Live shard rebalancing — migrate shards on add/remove without dropping queries.
- **Q7** One deep vertical: vector quantization (compression).
- **Q8** Contribute PRs to `opensearch-project`; optional **read-only** MCP query agent.

**Guiding rule:** build the horizontal spine (nodes surviving failure) before any
single-node vertical optimization (HNSW, SIMD, geo). Depth is added last, on top of a
spine that already works and is understood.

## Build

```bash
cargo build            # requires protoc on PATH (brew install protobuf)
cargo test -p common   # shard-key hashing tests
```

## License

MIT.
