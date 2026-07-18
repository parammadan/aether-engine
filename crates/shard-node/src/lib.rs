//! Shard node library: the data-plane building blocks, kept in a lib so they are
//! unit-testable in isolation (the thin binary in `main.rs` just wires them together).
//!
//! - [`index`]:  in-memory inverted (keyword) index.
//! - [`server`]: the `ShardSearch` gRPC service.
//! - [`ingest`]: OpenSky ingestion (pull loop + backpressure), shard-aware.
//! - [`cluster`]: registering this node with the coordinator control plane.
//! - [`replication`]: leader → follower document replication.

pub mod cluster;
pub mod index;
pub mod ingest;
pub mod replication;
pub mod server;
pub mod store;
pub mod vector;
