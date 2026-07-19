//! Aether coordinator (control plane).
//!
//! Dynamic node discovery — shard nodes register at runtime and the coordinator maintains an
//! N-parameterized shard map ([`registry`]) — plus scatter-gather query fan-out ([`fanout`]),
//! both served over gRPC ([`service`]).

pub mod auth;
pub mod control;
pub mod fanout;
pub mod registry;
pub mod service;
