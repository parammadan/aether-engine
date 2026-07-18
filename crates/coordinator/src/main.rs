//! Aether coordinator (control plane) — STUB.
//!
//! Responsibilities (built in Q2, not now): discover shard nodes via `RegisterNode`,
//! own the shard map (`hash(icao24) % N`), and scatter-gather `Search` across shards.
//!
//! Q1 stop-line: no logic here yet. This binary exists so the workspace builds and the
//! architecture is visible; it intentionally does nothing but prove it links `common`.

fn main() {
    // Touch the shared contract so the dependency is real, not decorative.
    let _ = common::pb::NodeRole::Leader;
    println!("aether-coordinator: not implemented yet (control plane lands in Q2).");
}
