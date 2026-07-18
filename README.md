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
    common/           generated contract + shard-key hashing (shared by both binaries)
    coordinator/      control plane: discovery, shard map, scatter-gather
    shard-node/       data plane: inverted index + ShardSearch gRPC server
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
a shard whose leader dies keeps being served without interruption.

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

## Build

```bash
cargo build            # requires protoc on PATH (brew install protobuf)
cargo test -p common   # shard-key hashing tests
```

## License

MIT.
