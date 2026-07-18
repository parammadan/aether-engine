//! Shard node library: the data-plane building blocks, kept in a lib so they are
//! unit-testable in isolation (the thin binary in `main.rs` just wires them together).
//!
//! Q1 so far: the in-memory inverted (keyword) index and the `ShardSearch` gRPC service.
//! Still to come this quarter: OpenSky ingestion with backpressure to fill the index.

pub mod index;
pub mod server;
