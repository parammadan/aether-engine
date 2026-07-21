//! An OpenAI-compatible planning model, behind the `openai` feature. The same
//! `/chat/completions` shape is spoken by OpenAI, Groq, Together, OpenRouter and Fireworks,
//! so ONE client covers a live real LLM without AWS and without local compute — point it at
//! whichever endpoint you have a key for. The loop is identical to every other `Model`; only
//! the planner changes. Off by default, so the default build and CI link no HTTP client.
//!
//! Env: `AETHER_OPENAI_API_KEY` (required), `AETHER_OPENAI_MODEL` (required, e.g.
//! `llama-3.3-70b-versatile` on Groq or `gpt-4o-mini` on OpenAI), `AETHER_OPENAI_BASE_URL`
//! (default `https://api.openai.com/v1`; Groq is `https://api.groq.com/openai/v1`).

use std::sync::Arc;

use crate::Model;

/// Build the model from the environment, or `None` when the feature is off or no key/model
/// is configured (the caller then falls back to the offline heuristic).
pub async fn from_env() -> Option<Arc<dyn Model>> {
    #[cfg(feature = "openai")]
    {
        imp::from_env()
    }
    #[cfg(not(feature = "openai"))]
    {
        None
    }
}

#[cfg(feature = "openai")]
mod imp {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::{json, Value};

    use crate::{Model, Step, Turn};

    pub struct OpenAiModel {
        client: reqwest::Client,
        base_url: String,
        api_key: String,
        model: String,
    }

    pub fn from_env() -> Option<Arc<dyn Model>> {
        let api_key = std::env::var("AETHER_OPENAI_API_KEY").ok()?;
        let model = std::env::var("AETHER_OPENAI_MODEL").ok()?;
        let base_url = std::env::var("AETHER_OPENAI_BASE_URL")
            .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
        Some(Arc::new(OpenAiModel { client: reqwest::Client::new(), base_url, api_key, model }))
    }

    #[async_trait]
    impl Model for OpenAiModel {
        async fn next_step(&self, turn: Turn<'_>) -> Result<Step, String> {
            // Same message shape as the Bedrock planner: the question, then each prior tool
            // result as an observation the model plans against. Kept deliberately simple and
            // provider-agnostic (plain user turns, not the assistant/tool role dance) so any
            // OpenAI-compatible endpoint drives the identical loop.
            let mut messages = vec![json!({ "role": "user", "content": turn.question })];
            for (name, out) in turn.observations {
                messages.push(json!({
                    "role": "user",
                    "content": format!("[tool {name} returned]\n{out}"),
                }));
            }

            let body = json!({
                "model": self.model,
                "messages": messages,
                "tools": tools(&agent_tools::definitions())?,
                "temperature": 0,
            });

            let resp = self
                .client
                .post(format!("{}/chat/completions", self.base_url))
                .bearer_auth(&self.api_key)
                .json(&body)
                .send()
                .await
                .map_err(|e| format!("openai request: {e}"))?;
            let status = resp.status();
            let text = resp.text().await.map_err(|e| format!("openai read body: {e}"))?;
            if !status.is_success() {
                return Err(format!("openai {status}: {text}"));
            }
            let v: Value = serde_json::from_str(&text).map_err(|e| format!("openai parse: {e}"))?;

            let message = &v["choices"][0]["message"];
            // A tool call takes precedence over any content, exactly as in the Bedrock path.
            if let Some(call) = message["tool_calls"].as_array().and_then(|a| a.first()) {
                let name = call["function"]["name"].as_str().ok_or("tool call missing name")?;
                // OpenAI encodes arguments as a JSON *string*; parse it (empty => {}).
                let raw = call["function"]["arguments"].as_str().unwrap_or("{}");
                let args: Value = if raw.trim().is_empty() {
                    json!({})
                } else {
                    serde_json::from_str(raw).map_err(|e| format!("tool args parse: {e}"))?
                };
                return Ok(Step::CallTool { name: name.to_string(), args });
            }
            let answer = message["content"].as_str().unwrap_or("").to_string();
            Ok(Step::Answer(answer))
        }
    }

    /// Shared tool definitions (name/description/inputSchema) → OpenAI's `tools` array.
    fn tools(defs: &Value) -> Result<Value, String> {
        let arr = defs.as_array().ok_or("tool defs must be an array")?;
        let tools: Vec<Value> = arr
            .iter()
            .map(|d| {
                json!({
                    "type": "function",
                    "function": {
                        "name": d["name"],
                        "description": d["description"],
                        "parameters": d["inputSchema"],
                    }
                })
            })
            .collect();
        Ok(Value::Array(tools))
    }
}
