//! Shard node library: the data-plane building blocks, kept in a lib so they are
//! unit-testable in isolation (the thin binary in `main.rs` just wires them together).
//!
//! Q1: the in-memory inverted (keyword) index, the `ShardSearch` gRPC service, and
//! OpenSky ingestion (pull loop + backpressure). Wired together, a single shard node
//! ingests live flight data and serves keyword search over gRPC.

pub mod index;
pub mod ingest;
pub mod server;
