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

    // The live model is provided by an (env-gated) build — an OpenAI-compatible endpoint or
    // Bedrock; the default binary carries no model, so it says so plainly instead of guessing.
    let model = match nlq::openai::from_env().await {
        Some(m) => Some(m),
        None => nlq::bedrock::from_env().await,
    };
    match model {
        Some(model) => {
            let answer = nlq::run(model.as_ref(), &nlq::EngineTools, &question, Budget::default()).await;
            println!("{}", nlq::render(&answer));
        }
        None => {
            eprintln!(
                "no planning model configured. Set AETHER_OPENAI_API_KEY + AETHER_OPENAI_MODEL \
                 (any OpenAI-compatible endpoint, e.g. Groq) or AETHER_BEDROCK_MODEL + AWS creds \
                 to enable the live model. CI and tests drive the loop with a scripted model."
            );
            std::process::exit(1);
        }
    }
    Ok(())
}
