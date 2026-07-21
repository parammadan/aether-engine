#!/usr/bin/env bash
# Fully local, keyless live NLQ eval: a small LLM (Qwen2.5-3B) runs on-device via
# llama.cpp behind an OpenAI-compatible server, and drives the real NLQ loop over a live
# cluster. No cloud, no API key. Reproducible on any Apple Silicon Mac with ~2GB free.
#
#   brew install llama.cpp
#   ./scripts/live-eval-local.sh
set -euo pipefail
PORT=8081
MODEL_HF="Qwen/Qwen2.5-3B-Instruct-GGUF:Q4_K_M"

# 1) Start the local OpenAI-compatible model server (--jinja enables tool calling).
if ! curl -s "http://localhost:$PORT/v1/models" | grep -q Qwen; then
  echo "starting llama-server ($MODEL_HF)…"
  nohup llama-server -hf "$MODEL_HF" --jinja --port "$PORT" --ctx-size 8192 >/tmp/llama.log 2>&1 &
  for _ in $(seq 1 60); do
    curl -s "http://localhost:$PORT/v1/models" | grep -q Qwen && break; sleep 5
  done
fi
MODEL_ID=$(curl -s "http://localhost:$PORT/v1/models" | python3 -c "import sys,json;print(json.load(sys.stdin)['data'][0]['id'])")

# 2) Bring up a 2-shard synthetic cluster.
cargo build -q -p coordinator -p shard-node -p nlq --features openai --bin eval
pkill -f target/debug/shard-node 2>/dev/null || true; pkill -f target/debug/coordinator 2>/dev/null || true; sleep 1
AETHER_SHARD_COUNT=2 ./target/debug/coordinator >/tmp/lc.log 2>&1 & sleep 1
for i in 0 1; do
  AETHER_NODE_ID=s$i AETHER_SHARD_ADDR=127.0.0.1:5006$i AETHER_SHARD_INDEX=$i AETHER_SHARD_COUNT=2 \
    AETHER_SOURCE=synthetic AETHER_POLL_SECS=1 ./target/debug/shard-node >/tmp/ls$i.log 2>&1 &
done
sleep 7

# 3) Run the eval with the local model as the planner.
AETHER_OPENAI_BASE_URL="http://localhost:$PORT/v1" \
AETHER_OPENAI_API_KEY=local-no-key \
AETHER_OPENAI_MODEL="$MODEL_ID" \
  ./target/debug/eval crates/nlq/eval/questions.json

pkill -f target/debug/shard-node 2>/dev/null || true; pkill -f target/debug/coordinator 2>/dev/null || true
