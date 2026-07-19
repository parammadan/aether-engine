//! Quantization measurement: memory, recall, and scan speed of binary-quantized vector
//! search versus exact f32 scan, on deterministic pseudo-random unit vectors.
//!
//! The recall assertions use generous floors so this runs in CI; run with `--nocapture`
//! for the full report:
//!
//!   cargo test -p shard-node --test quant_bench -- --nocapture

use std::time::Instant;

use shard_node::quant::{QuantizedVector, OVERSAMPLE};

const DIMS: usize = 384;
const DOCS: usize = 2000;
const QUERIES: usize = 50;
const K: usize = 10;

/// Deterministic xorshift64* — no rand dependency, identical vectors on every run.
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545F4914F6CDD1D)
}

fn rand_unit_vec(state: &mut u64, dims: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dims)
        .map(|_| (xorshift(state) as f64 / u64::MAX as f64) as f32 - 0.5)
        .collect();
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    v.iter_mut().for_each(|x| *x /= norm);
    v
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Exact top-k slots by dot product (the ground truth).
fn exact_top_k(query: &[f32], docs: &[Vec<f32>], k: usize) -> Vec<usize> {
    let mut scored: Vec<(f32, usize)> =
        docs.iter().enumerate().map(|(i, d)| (dot(query, d), i)).collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    scored.into_iter().take(k).map(|(_, i)| i).collect()
}

/// Hamming top-n slots against the quantized forms.
fn hamming_top_n(query: &QuantizedVector, docs: &[QuantizedVector], n: usize) -> Vec<usize> {
    let mut scored: Vec<(u32, usize)> =
        docs.iter().enumerate().map(|(i, d)| (query.hamming(d), i)).collect();
    scored.sort_unstable_by_key(|(h, _)| *h);
    scored.into_iter().take(n).map(|(_, i)| i).collect()
}

fn recall(truth: &[usize], got: &[usize]) -> f64 {
    let hits = got.iter().filter(|i| truth.contains(i)).count();
    hits as f64 / truth.len() as f64
}

#[test]
fn quantized_search_holds_recall_at_thirty_x_compression() {
    // --- Corpus: CLUSTERED unit vectors, like real embeddings (semantically similar
    // documents huddle around shared directions). Purely random vectors would make
    // ground-truth ranks 2..k orthogonal noise that no quantizer can — or should —
    // preserve; recall@k only means something where genuine neighbours exist.
    let mut state = 0x5EED_5EED_5EED_5EEDu64;
    const CLUSTERS: usize = 100;
    let centers: Vec<Vec<f32>> = (0..CLUSTERS).map(|_| rand_unit_vec(&mut state, DIMS)).collect();
    let jitter = |state: &mut u64, center: &[f32]| -> Vec<f32> {
        let noise = rand_unit_vec(state, DIMS);
        let mut v: Vec<f32> = center.iter().zip(&noise).map(|(c, n)| c + 0.25 * n).collect();
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        v.iter_mut().for_each(|x| *x /= norm);
        v
    };
    let docs: Vec<Vec<f32>> = (0..DOCS)
        .map(|i| jitter(&mut state, &centers[i % CLUSTERS]))
        .collect();
    let quantized: Vec<QuantizedVector> = docs.iter().map(|d| QuantizedVector::quantize(d)).collect();

    // Queries: fresh samples from random clusters — each has ~20 true neighbours.
    let queries: Vec<Vec<f32>> = (0..QUERIES)
        .map(|_| {
            let c = (xorshift(&mut state) as usize) % CLUSTERS;
            jitter(&mut state, &centers[c])
        })
        .collect();

    // --- Memory ---
    let original_bytes = DIMS * std::mem::size_of::<f32>();
    let quant_bytes = quantized[0].memory_bytes();
    let ratio = original_bytes as f64 / quant_bytes as f64;

    // --- Recall: binary-only vs two-tier (binary candidates + exact rescore) ---
    let mut recall_binary = 0.0;
    let mut recall_rescored = 0.0;
    for q in &queries {
        let truth = exact_top_k(q, &docs, K);
        let qq = QuantizedVector::quantize(q);

        let binary_only = hamming_top_n(&qq, &quantized, K);
        recall_binary += recall(&truth, &binary_only);

        let candidates = hamming_top_n(&qq, &quantized, K * OVERSAMPLE);
        let mut rescored: Vec<(f32, usize)> =
            candidates.into_iter().map(|i| (dot(q, &docs[i]), i)).collect();
        rescored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        let rescored: Vec<usize> = rescored.into_iter().take(K).map(|(_, i)| i).collect();
        recall_rescored += recall(&truth, &rescored);
    }
    recall_binary /= QUERIES as f64;
    recall_rescored /= QUERIES as f64;

    // --- Scan speed: full corpus, exact f32 vs binary Hamming ---
    let q = &queries[0];
    let qq = QuantizedVector::quantize(q);
    let t0 = Instant::now();
    let mut sink = 0f32;
    for d in &docs {
        sink += dot(q, d);
    }
    let exact_scan = t0.elapsed();
    let t1 = Instant::now();
    let mut sink2 = 0u32;
    for d in &quantized {
        sink2 += qq.hamming(d);
    }
    let hamming_scan = t1.elapsed();
    std::hint::black_box((sink, sink2));

    println!("== quantization report ({DOCS} docs x {DIMS} dims, {QUERIES} queries, k={K}) ==");
    println!("memory/vector : {original_bytes} B -> {quant_bytes} B  ({ratio:.1}x; bits alone {}x)", DIMS * 32 / DIMS);
    println!("recall@{K}     : binary-only {recall_binary:.3} | rescored({}x oversample) {recall_rescored:.3}", OVERSAMPLE);
    println!(
        "full scan     : exact {:>8.1?} ({:.0} ns/doc) | hamming {:>8.1?} ({:.0} ns/doc) | {:.1}x faster",
        exact_scan,
        exact_scan.as_nanos() as f64 / DOCS as f64,
        hamming_scan,
        hamming_scan.as_nanos() as f64 / DOCS as f64,
        exact_scan.as_nanos() as f64 / hamming_scan.as_nanos().max(1) as f64
    );

    // Floors, not targets — generous so CI never flakes on a slow machine or unlucky seed.
    assert!(ratio > 29.0, "compression fell below ~30x: {ratio:.1}");
    assert!(
        recall_rescored >= 0.85,
        "two-tier recall@{K} too low: {recall_rescored:.3} (binary-only was {recall_binary:.3})"
    );
    assert!(
        recall_rescored >= recall_binary,
        "rescoring must not make recall worse"
    );
}
