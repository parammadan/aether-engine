//! `aether-briefing` — the capstone. Compose a plain-English briefing of the cluster and
//! hand it off by email, on a cadence or once. The single outward action is gated:
//!   - recipients must be on AETHER_BRIEFING_RECIPIENTS (fail-closed allowlist);
//!   - sending is DRY-RUN by default — nothing leaves unless a real emailer is wired AND
//!     AETHER_BRIEFING_SEND=send is set;
//!   - everything else is read-only, structurally (no mutating RPC linked).
//!
//! Config: AETHER_BRIEFING_RECIPIENTS (comma list), AETHER_BRIEFING_SEND (dry-run|send;
//! default dry-run), AETHER_BRIEFING_CADENCE_SECS (0 = run once), AETHER_BEDROCK_MODEL
//! (else the offline heuristic planner), AETHER_COORDINATOR_ADDR(S).

use std::sync::Arc;

use briefing::{brief_and_send, Allowlist, DryRunEmailer, Emailer};
use nlq::Model;

fn questions() -> Vec<String> {
    // The standing briefing set — the questions an operator wants answered every cycle.
    [
        "how many flights are there?",
        "which aircraft types are flying?",
        "what are the altitude percentiles?",
        "is the cluster healthy?",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    common::net::install_crypto();

    let allowlist = Allowlist::from_env();
    let recipients: Vec<String> = std::env::var("AETHER_BRIEFING_RECIPIENTS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let cadence: u64 = std::env::var("AETHER_BRIEFING_CADENCE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Dry-run is the default sink. A real SMTP emailer would be selected here behind an
    // explicit flag; until then, "send" is still inert — the safe default.
    let send = std::env::var("AETHER_BRIEFING_SEND").as_deref() == Ok("send");
    let emailer: Arc<dyn Emailer> = Arc::new(DryRunEmailer::default());
    if send {
        eprintln!("briefing: AETHER_BRIEFING_SEND=send, but no real emailer is wired — staying dry-run (safe default)");
    }

    let model: Box<dyn Model> = match nlq::bedrock::from_env().await {
        Some(m) => {
            // Arc<dyn Model> -> a borrow for the loop.
            eprintln!("briefing: using the Bedrock planner");
            return run_loop(m.as_ref(), &questions(), &recipients, &allowlist, emailer.as_ref(), cadence).await;
        }
        None => {
            eprintln!("briefing: using the offline heuristic planner (set AETHER_BEDROCK_MODEL for Bedrock)");
            Box::new(nlq::HeuristicModel)
        }
    };
    run_loop(model.as_ref(), &questions(), &recipients, &allowlist, emailer.as_ref(), cadence).await
}

async fn run_loop(
    model: &dyn Model,
    questions: &[String],
    recipients: &[String],
    allowlist: &Allowlist,
    emailer: &dyn Emailer,
    cadence: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        match brief_and_send(model, questions, recipients, allowlist, emailer).await {
            Ok(delivered) => println!("briefing: delivered to {} recipient(s)", delivered.len()),
            Err(errs) => {
                for e in &errs {
                    eprintln!("briefing: {e}");
                }
            }
        }
        if cadence == 0 {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_secs(cadence)).await;
    }
}
