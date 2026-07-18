//! Aether coordinator (control plane).
//!
//! Q2 piece 1: dynamic node discovery — shard nodes register at runtime and the coordinator
//! maintains an N-parameterized shard map ([`registry`]) served over gRPC ([`service`]).
//! Still to come this quarter: scatter-gather query fan-out, then replication.

pub mod registry;
pub mod service;
