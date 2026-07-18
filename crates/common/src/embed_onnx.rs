//! Learned text embeddings via ONNX Runtime: a MiniLM-class sentence-transformer exported to
//! ONNX, producing 384-dim vectors that capture *meaning*, not just token overlap — the
//! upgrade over [`crate::embed::HashEmbedder`], behind the same [`Embedder`] trait.
//!
//! # The cross-node contract still holds — by configuration, not by construction
//! The hash embedder is deterministic by construction. A learned model is deterministic only
//! if **every node runs the same model file and the same runtime**: same weights, same
//! tokenizer, same ONNX Runtime version (and in strictness, the same CPU architecture —
//! float kernels can differ across ISAs). Operationally that means the model directory is
//! cluster config, and changing the model is a cluster-wide restart: vectors are derived
//! data, rebuilt from the live stream, so a model change is a re-ingest with new config —
//! never mix embedders within one cluster.
//!
//! # Pipeline (standard sentence-transformers recipe)
//! tokenize (WordPiece, truncated) → run the transformer → mean-pool the last hidden state
//! over the attention mask → L2 normalize (so dot product = cosine similarity, matching the
//! rest of the vector path).
//!
//! # Files
//! Model files are NOT in git (binary weights, tens of MB). `OnnxEmbedder::from_dir` expects
//! a directory containing `model_quantized.onnx` and `tokenizer.json` — see
//! `scripts/fetch-model.sh`.

use std::path::Path;
use std::sync::Mutex;

use ort::session::builder::GraphOptimizationLevel;
use ort::session::Session;
use ort::value::Tensor;
use tokenizers::Tokenizer;

use crate::embed::Embedder;

/// MiniLM-L6 hidden size.
const ONNX_EMBED_DIM: usize = 384;
/// Truncation bound: flight documents are a handful of tokens, so this is generous headroom,
/// and it caps worst-case inference latency.
const MAX_TOKENS: usize = 256;

pub type OnnxError = Box<dyn std::error::Error + Send + Sync>;

/// An [`Embedder`] backed by an ONNX sentence-transformer.
pub struct OnnxEmbedder {
    tokenizer: Tokenizer,
    /// ONNX Runtime sessions require exclusive access to run; a mutex serializes embeddings.
    /// Fine at this scale — ingestion embeds in one task, and queries embed once each.
    session: Mutex<Session>,
}

impl OnnxEmbedder {
    /// Load from a directory holding `model_quantized.onnx` + `tokenizer.json`.
    pub fn from_dir(dir: &Path) -> Result<Self, OnnxError> {
        let mut tokenizer = Tokenizer::from_file(dir.join("tokenizer.json"))?;
        tokenizer.with_truncation(Some(tokenizers::TruncationParams {
            max_length: MAX_TOKENS,
            ..Default::default()
        }))?;

        // ort errors are stringified at this boundary: they can carry non-Send payloads, and
        // our error type is Send+Sync so it can cross task boundaries.
        let builder = Session::builder().map_err(|e| e.to_string())?;
        let builder = builder
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(|e| e.to_string())?;
        // Single-threaded inference: deterministic op ordering, and throughput comes from
        // the fact that our texts are tiny, not from intra-op parallelism.
        let mut builder = builder.with_intra_threads(1).map_err(|e| e.to_string())?;
        let session = builder
            .commit_from_file(dir.join("model_quantized.onnx"))
            .map_err(|e| e.to_string())?;

        Ok(Self {
            tokenizer,
            session: Mutex::new(session),
        })
    }

    fn embed_inner(&self, text: &str) -> Result<Vec<f32>, OnnxError> {
        let encoding = self.tokenizer.encode(text, true)?;
        let seq = encoding.get_ids().len().max(1);

        let ids: Vec<i64> = encoding.get_ids().iter().map(|&x| x as i64).collect();
        let mask: Vec<i64> = encoding.get_attention_mask().iter().map(|&x| x as i64).collect();
        let type_ids: Vec<i64> = encoding.get_type_ids().iter().map(|&x| x as i64).collect();

        // ort errors are stringified: they can carry non-Send payloads and our error type
        // must be Send+Sync.
        let input_ids = Tensor::from_array(([1usize, seq], ids)).map_err(|e| e.to_string())?;
        let attention = Tensor::from_array(([1usize, seq], mask.clone())).map_err(|e| e.to_string())?;
        let types = Tensor::from_array(([1usize, seq], type_ids)).map_err(|e| e.to_string())?;

        let mut session = self.session.lock().map_err(|_| "onnx session lock poisoned")?;
        let outputs = session
            .run(ort::inputs![
                "input_ids" => input_ids,
                "attention_mask" => attention,
                "token_type_ids" => types,
            ])
            .map_err(|e| e.to_string())?;

        // last_hidden_state: [1, seq, ONNX_EMBED_DIM]
        let (_, hidden) = outputs["last_hidden_state"]
            .try_extract_tensor::<f32>()
            .map_err(|e| e.to_string())?;

        // Mean-pool token vectors where the attention mask is 1, then L2 normalize.
        let mut pooled = vec![0.0f32; ONNX_EMBED_DIM];
        let mut count = 0.0f32;
        for (token_idx, &m) in mask.iter().enumerate() {
            if m == 0 {
                continue;
            }
            count += 1.0;
            let base = token_idx * ONNX_EMBED_DIM;
            for (d, value) in pooled.iter_mut().enumerate() {
                *value += hidden[base + d];
            }
        }
        if count > 0.0 {
            for value in pooled.iter_mut() {
                *value /= count;
            }
        }
        let norm = pooled.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for value in pooled.iter_mut() {
                *value /= norm;
            }
        }
        Ok(pooled)
    }
}

impl Embedder for OnnxEmbedder {
    fn embed(&self, text: &str) -> Vec<f32> {
        // The Embedder contract is infallible; an inference failure yields a zero vector
        // (matches nothing) and a loud log, rather than poisoning the ingest path.
        match self.embed_inner(text) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("onnx embed failed for {text:?}: {e}");
                vec![0.0; ONNX_EMBED_DIM]
            }
        }
    }

    fn dim(&self) -> usize {
        ONNX_EMBED_DIM
    }
}
