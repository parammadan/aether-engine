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

**Q1 complete (single node):** a shard node ingests live flight data from OpenSky into an
in-memory inverted index and serves keyword search over gRPC — the `aether.v1` contract,
shard-key hashing, the inverted index, the `ShardSearch` server, and the ingestion loop
(pull-based, with backpressure). Verified end-to-end against live data (~13k flights).

**Q2 in progress — the spine.** The coordinator now serves dynamic node registration and
holds an N-parameterized shard map; shard nodes register on startup and ingest only the
documents they own (`hash(icao24) % N`). Next: scatter-gather query fan-out, then
leader→follower replication.

Run it:

```bash
# terminal 1 — start a node (ingests live OpenSky, serves on :50051)
cargo run -p shard-node
# terminal 2 — query it
cargo run -p shard-node --example query -- united 5
```

## Build

```bash
cargo build            # requires protoc on PATH (brew install protobuf)
cargo test -p common   # shard-key hashing tests
```

## License

MIT.
