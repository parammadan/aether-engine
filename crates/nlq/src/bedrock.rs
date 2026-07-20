//! The Bedrock-backed planning model, behind the `bedrock` feature so the default build
//! (and CI) links no AWS SDK and can never make a live call. Enabled for the env-gated
//! live eval only. Either way the loop is identical — this is just a `Model` impl.

use std::sync::Arc;

use crate::Model;

/// Construct the live model from the environment (`AETHER_BEDROCK_MODEL` + AWS creds), or
/// `None` when no model is configured or the feature is off. The loop is the same; only
/// the planner changes.
pub async fn from_env() -> Option<Arc<dyn Model>> {
    #[cfg(feature = "bedrock")]
    {
        imp::from_env().await
    }
    #[cfg(not(feature = "bedrock"))]
    {
        None
    }
}

#[cfg(feature = "bedrock")]
mod imp {
    use std::sync::Arc;

    use async_trait::async_trait;
    use aws_sdk_bedrockruntime::types::{
        ContentBlock, ConversationRole, Message, Tool, ToolConfiguration, ToolInputSchema,
        ToolSpecification,
    };
    use aws_sdk_bedrockruntime::Client;
    use serde_json::{json, Value};

    use crate::{Model, Step, Turn};

    /// A Bedrock Converse planning model over the read-only tool surface.
    pub struct BedrockModel {
        client: Client,
        model_id: String,
    }

    pub async fn from_env() -> Option<Arc<dyn Model>> {
        let model_id = std::env::var("AETHER_BEDROCK_MODEL").ok()?;
        let config = aws_config::load_from_env().await;
        Some(Arc::new(BedrockModel { client: Client::new(&config), model_id }))
    }

    #[async_trait]
    impl Model for BedrockModel {
        async fn next_step(&self, turn: Turn<'_>) -> Result<Step, String> {
            // Build the Converse message list: the question, then each prior tool result as
            // an observation the model planned against.
            let mut messages = vec![Message::builder()
                .role(ConversationRole::User)
                .content(ContentBlock::Text(turn.question.to_string()))
                .build()
                .map_err(|e| e.to_string())?];
            for (name, out) in turn.observations {
                messages.push(
                    Message::builder()
                        .role(ConversationRole::User)
                        .content(ContentBlock::Text(format!("[tool {name} returned]\n{out}")))
                        .build()
                        .map_err(|e| e.to_string())?,
                );
            }

            let tool_config = tool_config(&agent_tools::definitions())?;
            let resp = self
                .client
                .converse()
                .model_id(&self.model_id)
                .set_messages(Some(messages))
                .tool_config(tool_config)
                .send()
                .await
                .map_err(|e| format!("bedrock converse: {e}"))?;

            let out = resp.output().ok_or("no output from model")?;
            let msg = out.as_message().map_err(|_| "output was not a message")?;
            for block in msg.content() {
                if let Ok(tu) = block.as_tool_use() {
                    let args = document_to_json(tu.input());
                    return Ok(Step::CallTool { name: tu.name().to_string(), args });
                }
            }
            // No tool call → the model's text is the answer.
            let text = msg
                .content()
                .iter()
                .filter_map(|b| b.as_text().ok())
                .cloned()
                .collect::<Vec<_>>()
                .join("");
            Ok(Step::Answer(text))
        }
    }

    /// Translate the shared tool definitions (JSON schema) into Bedrock's ToolConfiguration.
    fn tool_config(defs: &Value) -> Result<ToolConfiguration, String> {
        let mut tools = Vec::new();
        for def in defs.as_array().ok_or("tool defs must be an array")? {
            let name = def["name"].as_str().ok_or("tool needs a name")?;
            let desc = def["description"].as_str().unwrap_or("");
            let schema = json_to_document(&def["inputSchema"]);
            let spec = ToolSpecification::builder()
                .name(name)
                .description(desc)
                .input_schema(ToolInputSchema::Json(schema))
                .build()
                .map_err(|e| e.to_string())?;
            tools.push(Tool::ToolSpec(spec));
        }
        ToolConfiguration::builder()
            .set_tools(Some(tools))
            .build()
            .map_err(|e| e.to_string())
    }

    /// serde_json::Value → aws_smithy_types::Document (no built-in conversion exists).
    fn json_to_document(v: &Value) -> aws_smithy_types::Document {
        use aws_smithy_types::{Document, Number};
        match v {
            Value::Null => Document::Null,
            Value::Bool(b) => Document::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Document::Number(Number::NegInt(i))
                } else {
                    Document::Number(Number::Float(n.as_f64().unwrap_or(0.0)))
                }
            }
            Value::String(s) => Document::String(s.clone()),
            Value::Array(a) => Document::Array(a.iter().map(json_to_document).collect()),
            Value::Object(o) => {
                Document::Object(o.iter().map(|(k, v)| (k.clone(), json_to_document(v))).collect())
            }
        }
    }

    /// aws_smithy_types::Document → serde_json::Value (for a tool_use's input args).
    fn document_to_json(d: &aws_smithy_types::Document) -> Value {
        use aws_smithy_types::{Document, Number};
        match d {
            Document::Null => Value::Null,
            Document::Bool(b) => json!(b),
            Document::Number(Number::NegInt(i)) => json!(i),
            Document::Number(Number::PosInt(u)) => json!(u),
            Document::Number(Number::Float(f)) => json!(f),
            Document::String(s) => json!(s),
            Document::Array(a) => Value::Array(a.iter().map(document_to_json).collect()),
            Document::Object(o) => {
                Value::Object(o.iter().map(|(k, v)| (k.clone(), document_to_json(v))).collect())
            }
        }
    }
}
