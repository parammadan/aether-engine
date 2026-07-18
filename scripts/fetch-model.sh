#!/usr/bin/env bash
# Fetch the quantized MiniLM sentence-transformer (ONNX) + tokenizer used by the optional
# `onnx` embedder. Model weights are intentionally not checked into git.
#
#   ./scripts/fetch-model.sh            # downloads into ./models/all-MiniLM-L6-v2
#   AETHER_ONNX_MODEL_DIR=$PWD/models/all-MiniLM-L6-v2 cargo run -p shard-node --features onnx
set -euo pipefail

DIR="${1:-models/all-MiniLM-L6-v2}"
BASE="https://huggingface.co/Xenova/all-MiniLM-L6-v2/resolve/main"

mkdir -p "$DIR"
echo "fetching tokenizer.json ..."
curl -fsSL "$BASE/tokenizer.json" -o "$DIR/tokenizer.json"
echo "fetching model_quantized.onnx (~23 MB) ..."
curl -fsSL "$BASE/onnx/model_quantized.onnx" -o "$DIR/model_quantized.onnx"

echo "done: $DIR"
ls -lh "$DIR"
