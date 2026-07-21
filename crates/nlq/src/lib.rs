//! The natural-language query loop: a planning model calls the read-only engine tools
//! until it produces an answer, under a hard budget, composing the provenance of every
//! tool result into the final trust line.
//!
//! The loop is generic over [`Model`]: a scripted [`FakeModel`] drives it deterministically
//! in tests and CI (no network, no Bedrock), and the real Bedrock Converse client (the
//! binary) plugs into the SAME loop — so budgets, provenance composition, and failure
//! paths are all tested without a live model call.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

/// What the planning model decides to do on each turn.
#[derive(Debug, Clone)]
pub enum Step {
    /// Call a tool with these JSON arguments.
    CallTool { name: String, args: Value },
    /// Stop and answer with this text.
    Answer(String),
}

/// One turn's input for the model: the question plus the tool results observed so far.
pub struct Turn<'a> {
    pub question: &'a str,
    /// (tool name, tool output) for each call made so far, in order.
    pub observations: &'a [(String, String)],
}

/// A planning model. The real one calls Bedrock; the fake one replays a script.
#[async_trait]
pub trait Model: Send + Sync {
    async fn next_step(&self, turn: Turn<'_>) -> Result<Step, String>;
}

/// How a tool call is executed. The default runs the real `agent_tools`; tests can
/// substitute a stub, but the shipped loop always uses the read-only tool surface.
#[async_trait]
pub trait ToolRunner: Send + Sync {
    async fn run(&self, name: &str, args: &Value) -> Result<String, String>;
}

/// The production runner: dispatches to the shared read-only tool surface.
pub struct EngineTools;

#[async_trait]
impl ToolRunner for EngineTools {
    async fn run(&self, name: &str, args: &Value) -> Result<String, String> {
        agent_tools::call(name, args).await
    }
}

/// Budget: the loop stops after this many tool calls even if the model wants more, so a
/// confused model costs a bounded number of calls, never a runaway. Over budget returns
/// the partial answer built so far, honestly labeled — a visible degradation, not a hang.
#[derive(Clone, Copy)]
pub struct Budget {
    pub max_tool_calls: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self { max_tool_calls: 6 }
    }
}

/// The composed result of a natural-language query.
pub struct Answer {
    pub text: String,
    /// Whether the budget was exhausted before the model chose to answer.
    pub budget_exhausted: bool,
    /// Provenance lines lifted from each tool result, in call order — the merged audit
    /// trail the answer stands on.
    pub provenance: Vec<String>,
    pub tool_calls: usize,
}

/// Run one natural-language question to an answer, driving `model` around `tools` under
/// `budget`. Deterministic given a deterministic model — the whole loop is testable.
pub async fn run(
    model: &dyn Model,
    tools: &dyn ToolRunner,
    question: &str,
    budget: Budget,
) -> Answer {
    let mut observations: Vec<(String, String)> = Vec::new();
    let mut provenance: Vec<String> = Vec::new();
    let mut calls = 0usize;

    loop {
        let step = match model.next_step(Turn { question, observations: &observations }).await {
            Ok(s) => s,
            Err(e) => {
                // Model/transport failure: a visible, labeled degradation, never a hang.
                return Answer {
                    text: format!("could not complete the query: {e}"),
                    budget_exhausted: false,
                    provenance,
                    tool_calls: calls,
                };
            }
        };

        match step {
            Step::Answer(text) => {
                return Answer { text, budget_exhausted: false, provenance, tool_calls: calls };
            }
            Step::CallTool { name, args } => {
                if calls >= budget.max_tool_calls {
                    // Out of budget: hand back what we have, clearly labeled.
                    let partial = observations
                        .last()
                        .map(|(_, out)| out.clone())
                        .unwrap_or_else(|| "no results gathered".to_string());
                    return Answer {
                        text: format!(
                            "[partial answer — tool-call budget of {} reached]\n{partial}",
                            budget.max_tool_calls
                        ),
                        budget_exhausted: true,
                        provenance,
                        tool_calls: calls,
                    };
                }
                calls += 1;
                let output = match tools.run(&name, &args).await {
                    Ok(out) => out,
                    Err(e) => format!("tool '{name}' error: {e}"),
                };
                // Lift any provenance line the tool emitted, so the answer can quote it.
                for line in output.lines() {
                    if let Some(p) = line.strip_prefix("provenance: ") {
                        provenance.push(p.to_string());
                    }
                }
                observations.push((name, output));
            }
        }
    }
}

/// Format an answer with its composed provenance appended — what a caller prints.
pub fn render(answer: &Answer) -> String {
    let mut out = answer.text.clone();
    if !answer.provenance.is_empty() {
        out.push_str("\n\n— evidence —\n");
        for (i, p) in answer.provenance.iter().enumerate() {
            out.push_str(&format!("  [{}] {p}\n", i + 1));
        }
    }
    out
}

// =============================================================================
// Fake model: a scripted planner for tests and CI (no Bedrock).
// =============================================================================

/// A deterministic model that replays a fixed sequence of steps regardless of what the
/// tools return — enough to exercise the loop's control flow, budgets, and provenance
/// composition without a live model. Real planning quality is the live eval's job.
pub struct FakeModel {
    script: std::sync::Mutex<std::collections::VecDeque<Step>>,
}

impl FakeModel {
    pub fn new(steps: Vec<Step>) -> Arc<Self> {
        Arc::new(Self { script: std::sync::Mutex::new(steps.into()) })
    }
}

#[async_trait]
impl Model for FakeModel {
    async fn next_step(&self, _turn: Turn<'_>) -> Result<Step, String> {
        // When the script runs out, keep asking for the same (harmless) tool so a test can
        // observe budget exhaustion; a well-formed script ends in Answer before that.
        Ok(self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Step::CallTool {
                name: "cluster_state".to_string(),
                args: json!({}),
            }))
    }
}

pub mod bedrock;
pub mod openai;

// =============================================================================
// Heuristic model: a keyword-routing planner for offline demos (no Bedrock).
// =============================================================================

/// A tiny rule-based planner: it maps a question to ONE tool call by keywords, then answers
/// with that tool's output. Not an LLM — it makes the NLQ bar functional offline and in CI,
/// exercising the real loop, real tools, and real provenance composition. The Bedrock model
/// replaces it for genuine language understanding when configured.
pub struct HeuristicModel;

#[async_trait]
impl Model for HeuristicModel {
    async fn next_step(&self, turn: Turn<'_>) -> Result<Step, String> {
        // Once a tool has answered, summarize it — one hop is enough for these intents.
        if let Some((_, out)) = turn.observations.last() {
            return Ok(Step::Answer(format!("Here's what I found:\n{out}")));
        }
        let q = turn.question.to_lowercase();
        let numeric = ["altitude", "velocity", "heading"]
            .into_iter()
            .find(|f| q.contains(*f))
            .unwrap_or("altitude");
        // Cluster/health intent wins first: a question like "is the cluster healthy? how
        // many shards answered?" is about topology, even though it also says "how many".
        let step = if q.contains("cluster") || q.contains("shard") || q.contains("health") || q.contains("nodes") {
            Step::CallTool { name: "cluster_state".into(), args: json!({}) }
        } else if q.contains("percentile") || q.contains("p50") || q.contains("p90") || q.contains("p99") {
            Step::CallTool {
                name: "aggregate_flights".into(),
                args: json!({ "kind": "percentiles", "field": numeric, "percentiles": [50, 90, 99] }),
            }
        } else if q.contains("how many") || q.contains("count") {
            Step::CallTool { name: "aggregate_flights".into(), args: json!({ "kind": "count" }) }
        } else if q.contains("aircraft") || q.contains("origin") || q.contains("countries") || q.contains("by ") {
            let field = if q.contains("aircraft") { "aircraft_type" } else { "origin" };
            Step::CallTool {
                name: "aggregate_flights".into(),
                args: json!({ "kind": "value_counts", "field": field }),
            }
        } else {
            Step::CallTool { name: "search_flights".into(), args: json!({ "query": turn.question }) }
        };
        Ok(step)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn first_tool(q: &str) -> (String, Value) {
        match HeuristicModel.next_step(Turn { question: q, observations: &[] }).await.unwrap() {
            Step::CallTool { name, args } => (name, args),
            Step::Answer(_) => panic!("expected a tool call on the first turn"),
        }
    }

    #[tokio::test]
    async fn heuristic_routes_intents_to_the_right_tool() {
        assert_eq!(first_tool("how many flights?").await.0, "aggregate_flights");
        let (name, args) = first_tool("altitude percentiles").await;
        assert_eq!(name, "aggregate_flights");
        assert_eq!(args["kind"], "percentiles");
        assert_eq!(args["field"], "altitude");
        assert_eq!(first_tool("flights by aircraft").await.1["field"], "aircraft_type");
        assert_eq!(first_tool("is the cluster healthy?").await.0, "cluster_state");
        assert_eq!(first_tool("UAL231").await.0, "search_flights");
    }

    #[tokio::test]
    async fn heuristic_answers_once_a_tool_has_reported() {
        let obs = [("cluster_state".to_string(), "2 shard groups".to_string())];
        let step = HeuristicModel.next_step(Turn { question: "x", observations: &obs }).await.unwrap();
        assert!(matches!(step, Step::Answer(t) if t.contains("2 shard groups")));
    }
}
