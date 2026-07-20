//! Live NLQ eval (env-gated, COSTS MONEY): runs the graded question set through the real
//! Bedrock loop against a running cluster, grades each answer behaviorally (did it call the
//! expected tool, does the text contain the expected substrings), and prints a table plus a
//! pass rate. Never runs in CI — it needs `--features bedrock`, AETHER_BEDROCK_MODEL, AWS
//! creds, and a live coordinator.
//!
//!   AETHER_BEDROCK_MODEL=... AETHER_COORDINATOR_ADDR=... \
//!     cargo run -p nlq --features bedrock --bin eval -- crates/nlq/eval/questions.json

use nlq::Budget;
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    common::net::install_crypto();
    let path = std::env::args().nth(1).unwrap_or_else(|| "crates/nlq/eval/questions.json".into());
    let spec: Value = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
    let questions = spec["questions"].as_array().ok_or("questions must be an array")?;

    // Prefer the live Bedrock model when configured; otherwise fall back to the offline
    // heuristic planner so the eval runs with NO AWS and no errors — a router smoke-eval
    // rather than a language-quality eval, labeled as such.
    let (model, label): (std::sync::Arc<dyn nlq::Model>, &str) = match nlq::bedrock::from_env().await {
        Some(m) => (m, "bedrock"),
        None => (std::sync::Arc::new(nlq::HeuristicModel), "heuristic (offline)"),
    };
    println!("model: {label}");

    let mut passed = 0usize;
    println!("== NLQ live eval ({} questions) ==", questions.len());
    for q in questions {
        let id = q["id"].as_str().unwrap_or("?");
        let ask = q["ask"].as_str().unwrap_or("");
        let want_tools: Vec<&str> =
            q["expect_tools"].as_array().map(|a| a.iter().filter_map(|v| v.as_str()).collect()).unwrap_or_default();
        let want_text: Vec<String> = q["expect_contains"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).map(|s| s.to_lowercase()).collect())
            .unwrap_or_default();

        // Run through the real loop; a TracingTools wrapper records which tools were called.
        let tools = TracingTools::default();
        let answer = nlq::run(model.as_ref(), &tools, ask, Budget::default()).await;
        let called = tools.called.lock().unwrap().clone();

        let tools_ok = want_tools.iter().all(|t| called.iter().any(|c| c == t));
        let text_lc = answer.text.to_lowercase();
        let text_ok = want_text.iter().all(|s| text_lc.contains(s));
        let ok = tools_ok && text_ok;
        passed += ok as usize;

        println!(
            "  [{}] {id:<22} tools={called:?} (want {want_tools:?}) {}",
            if ok { "PASS" } else { "FAIL" },
            if ok { "" } else { "<-- mismatch" }
        );
    }
    println!("== {passed}/{} passed ==", questions.len());
    Ok(())
}

/// A ToolRunner that records tool names while delegating to the real engine tools.
#[derive(Default)]
struct TracingTools {
    called: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl nlq::ToolRunner for TracingTools {
    async fn run(&self, name: &str, args: &Value) -> Result<String, String> {
        self.called.lock().unwrap().push(name.to_string());
        nlq::EngineTools.run(name, args).await
    }
}
