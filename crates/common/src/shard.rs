//! Shard-key hashing: map a document's `icao24` to a shard index in `0..N`.
//!
//! # Why `icao24` is the shard key
//! `icao24` is the 24-bit ICAO transponder address — one per aircraft, high-cardinality,
//! and roughly uniformly distributed. Hashing it spreads documents evenly across shards.
//! We deliberately do NOT shard by `origin_airport`: airport traffic is heavily skewed
//! (a hub like JFK would pin all its flights to a single shard — a hotspot), and it also
//! isn't stable per document. Even, stable placement is worth more than locality here.
//!
//! # Why a hand-rolled FNV-1a instead of the standard-library hashers
//! The shard function is a *distributed contract*: the coordinator and every shard node,
//! on every machine and every restart, must compute the SAME shard for the same `icao24`.
//! That rules out the obvious choices:
//!   - `HashMap`'s default hasher (`RandomState`) is seeded randomly per process, so it
//!     would place the same aircraft on different shards in different processes. Fatal here.
//!   - `std::hash::DefaultHasher` is deterministic today, but std explicitly does not
//!     guarantee its algorithm is stable across Rust releases — a compiler upgrade could
//!     silently re-shard the whole cluster.
//! FNV-1a is a fixed, fully specified algorithm with no seed: identical output forever,
//! on every node. It has good avalanche behavior for short ASCII keys like `icao24`, and
//! it's ~10 lines we fully understand and can defend line by line.

use std::num::NonZeroU32;

// 64-bit FNV-1a constants (from the FNV specification).
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Deterministic 64-bit FNV-1a hash of `key`.
///
/// FNV-1a: start from the offset basis, then for each byte XOR it in *first* and multiply
/// by the prime *second* (the "1a" variant — the XOR-before-multiply ordering gives better
/// dispersion than the original FNV-1). `wrapping_mul` makes the multiply the intended
/// mod-2^64 arithmetic instead of overflowing in debug builds.
pub fn fnv1a_64(key: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in key {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Map an `icao24` to a shard index in `0..shard_count`.
///
/// `shard_count` is a [`NonZeroU32`] so "N must be at least 1" is enforced by the type
/// system — there is no `% 0` panic to guard against, and callers cannot accidentally pass
/// a hardcoded literal without acknowledging it's a runtime cluster parameter.
///
/// Placement is `hash(icao24) % N`. This is plain modulo hashing: simple and perfectly
/// balanced for a fixed N, at the cost of remapping most keys when N changes. That tradeoff
/// is fine for Q1 (N is fixed at startup); live resizing without mass remapping is the
/// Q6 rebalancing problem, where consistent hashing / a shard-migration protocol comes in.
pub fn shard_for(icao24: &str, shard_count: NonZeroU32) -> u32 {
    let hash = fnv1a_64(icao24.as_bytes());
    (hash % shard_count.get() as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(x: u32) -> NonZeroU32 {
        NonZeroU32::new(x).unwrap()
    }

    #[test]
    fn fnv1a_matches_known_vectors() {
        // Reference values from the FNV-1a 64-bit spec — pins the algorithm so a future
        // refactor that changes the hash (and thus re-shards the cluster) fails loudly.
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
    }

    #[test]
    fn shard_is_deterministic() {
        let id = "a1b2c3";
        assert_eq!(shard_for(id, n(8)), shard_for(id, n(8)));
    }

    #[test]
    fn shard_is_always_in_range() {
        for i in 0..10_000u32 {
            let icao24 = format!("{i:06x}");
            let s = shard_for(&icao24, n(7));
            assert!(s < 7, "shard {s} out of range for N=7");
        }
    }

    #[test]
    fn single_shard_collapses_to_zero() {
        // With N=1 every document maps to shard 0 — the single-node Q1 case.
        assert_eq!(shard_for("deadbeef", n(1)), 0);
    }

    #[test]
    fn distribution_is_roughly_even() {
        // Sanity check, not a statistical proof: 10k synthetic ids across 8 shards should
        // land within ~35% of the 1,250 average. Catches a badly broken hash/modulo.
        let shard_count = 8usize;
        let mut counts = vec![0usize; shard_count];
        let total = 10_000u32;
        for i in 0..total {
            let icao24 = format!("{i:06x}");
            counts[shard_for(&icao24, n(shard_count as u32)) as usize] += 1;
        }
        let avg = total as usize / shard_count;
        for (shard, &c) in counts.iter().enumerate() {
            let low = avg - avg / 3;
            let high = avg + avg / 3;
            assert!(c >= low && c <= high, "shard {shard} got {c}, expected ~{avg}");
        }
    }
}
