//! Shared types and cross-cutting logic for the Aether cluster.
//!
//! Contents:
//!   - [`pb`]: the generated gRPC contract (messages + service stubs) from `proto/aether.proto`.
//!   - [`shard`]: the shard-key hashing that maps a document's `icao24` to a shard.

/// The generated protobuf/gRPC code for package `aether.v1`.
///
/// `include_proto!` pastes in the file `tonic-build` wrote to `$OUT_DIR` at build time.
/// Everything the coordinator and shard nodes send on the wire comes from here, so both
/// binaries depend on `common` and therefore share one identical contract.
pub mod pb {
    tonic::include_proto!("aether.v1");
}

pub mod shard;
