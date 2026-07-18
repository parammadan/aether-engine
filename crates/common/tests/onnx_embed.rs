//! Live tests for the ONNX embedder. Ignored by default: they need the model files on disk
//! (run `scripts/fetch-model.sh` first) and the `onnx` feature enabled:
//!
//!   cargo test -p common --features onnx --test onnx_embed -- --ignored --nocapture
//!
//! Reads the model directory from AETHER_ONNX_MODEL_DIR (default: models/all-MiniLM-L6-v2
//! at the workspace root).
#![cfg(feature = "onnx")]

use std::path::PathBuf;

use common::embed::{dot, Embedder};
use common::embed_onnx::OnnxEmbedder;

fn model_dir() -> PathBuf {
    std::env::var("AETHER_ONNX_MODEL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            // workspace root = two levels up from this crate
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../models/all-MiniLM-L6-v2")
        })
}

#[test]
#[ignore = "needs model files on disk (scripts/fetch-model.sh)"]
fn embeds_deterministically_at_384_dims() {
    let embedder = OnnxEmbedder::from_dir(&model_dir()).expect("model should load");

    let a = embedder.embed("Boeing 737 departing San Francisco");
    let b = embedder.embed("Boeing 737 departing San Francisco");

    assert_eq!(embedder.dim(), 384);
    assert_eq!(a.len(), 384);
    assert_eq!(a, b, "same text must embed identically on the same node");
    let norm: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 1e-4, "expected L2-normalized output, norm={norm}");
}

#[test]
#[ignore = "needs model files on disk (scripts/fetch-model.sh)"]
fn captures_meaning_not_just_token_overlap() {
    let embedder = OnnxEmbedder::from_dir(&model_dir()).expect("model should load");

    // Zero shared tokens between "plane" and "aircraft" phrasings — a hash embedder scores
    // them near zero. A learned model must still see they mean the same thing.
    let plane = embedder.embed("a plane flying from Paris");
    let aircraft = embedder.embed("an aircraft departing France");
    let breakfast = embedder.embed("scrambled eggs and toast for breakfast");

    let related = dot(&plane, &aircraft);
    let unrelated = dot(&plane, &breakfast);
    println!("similarity(plane,aircraft)={related:.3} vs similarity(plane,breakfast)={unrelated:.3}");
    assert!(
        related > unrelated + 0.15,
        "expected semantic neighbours to clearly beat unrelated text: {related} vs {unrelated}"
    );
}
