//! Aether shard node (data plane) — STUB.
//!
//! Responsibilities (built across the rest of Q1): ingest flight observations from the
//! OpenSky stream with backpressure, build an inverted (keyword) index over the text
//! fields, and serve the `ShardSearch.Search` RPC over gRPC.
//!
//! Q1 stop-line: today we only landed the contract + shard-key hash. This binary just
//! demonstrates the shared logic links; ingestion, index, and server are next sessions.

fn main() {
    // Prove the shared shard-key function is reachable from the data plane.
    use std::num::NonZeroU32;
    let shard = common::shard::shard_for("abc123", NonZeroU32::new(1).unwrap());
    println!("aether-shard-node: not implemented yet. Demo shard for 'abc123' with N=1 -> {shard}.");
}
