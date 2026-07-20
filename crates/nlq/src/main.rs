//! `aether-nlq` — ask the cluster a question in plain English.
//!
//!   aether-nlq "how many flights from France are below 3000m?"
//!
//! The planning model runs the same read-only tool loop the library tests exercise. The
//! real Bedrock Converse client is wired in the live-eval step (env-gated, costs money);
//! until a model is configured this prints how to enable one, rather than pretending.

use nlq::Budget;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    common::net::install_crypto();
    let question: String = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    if question.is_empty() {
        eprintln!("usage: aether-nlq <question>");
        std::process::exit(2);
    }

    // The live Bedrock model is provided by the (env-gated) live-eval build; the default
    // binary carries no model, so it says so plainly instead of guessing.
    match nlq::bedrock::from_env().await {
        Some(model) => {
            let answer = nlq::run(model.as_ref(), &nlq::EngineTools, &question, Budget::default()).await;
            println!("{}", nlq::render(&answer));
        }
        None => {
            eprintln!(
                "no planning model configured. Set AETHER_BEDROCK_MODEL (and AWS creds) to \
                 enable the live model — see the item-11 live-eval notes. CI and tests drive \
                 the loop with a scripted model, never Bedrock."
            );
            std::process::exit(1);
        }
    }
    Ok(())
}
