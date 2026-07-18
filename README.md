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

Early work in progress. A single shard node serves keyword search over gRPC: the
`aether.v1` contract, shard-key hashing, an in-memory inverted index, and the
`ShardSearch` gRPC server are in place (index currently seeded with sample documents).
Next: live ingestion from OpenSky. The coordinator (control plane) is still a stub.

Run it:

```bash
cargo run -p shard-node        # serves ShardSearch on 127.0.0.1:50051
```

## Build

```bash
cargo build            # requires protoc on PATH (brew install protobuf)
cargo test -p common   # shard-key hashing tests
```

## License

MIT.
