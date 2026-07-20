//! The capstone briefing agent: run a fixed set of questions through the NLQ loop, compose
//! a provenance-carrying briefing, and hand it off by email — the single outward action in
//! a two-year read-only system, gated by a recipient allowlist and a dry-run default.
//!
//! The read-only guarantee is structural: this crate links `agent-tools` (no mutating RPC)
//! and the NLQ loop, nothing that can change cluster state. The ONE exception is
//! [`Emailer::send`], and it is reached only after [`Allowlist`] approves every recipient.

use async_trait::async_trait;

use nlq::{Budget, EngineTools, Model};

/// A recipient allowlist. Email may go ONLY to addresses on it — the governance around the
/// single egress. An empty allowlist permits nothing (fail-closed).
#[derive(Clone, Default)]
pub struct Allowlist(Vec<String>);

impl Allowlist {
    pub fn new(addrs: Vec<String>) -> Self {
        Self(addrs.into_iter().map(|a| a.trim().to_lowercase()).filter(|a| !a.is_empty()).collect())
    }
    /// From `AETHER_BRIEFING_RECIPIENTS` (comma-separated). Absent/empty ⇒ fail-closed.
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("AETHER_BRIEFING_RECIPIENTS")
                .unwrap_or_default()
                .split(',')
                .map(String::from)
                .collect(),
        )
    }
    pub fn allows(&self, addr: &str) -> bool {
        self.0.iter().any(|a| a == &addr.trim().to_lowercase())
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The email sink. A real deployment plugs in SMTP behind a feature; the default
/// [`DryRunEmailer`] sends nothing and just records the intent — the safe default, so a
/// misconfigured schedule can't spam anyone.
#[async_trait]
pub trait Emailer: Send + Sync {
    async fn send(&self, to: &str, subject: &str, body: &str) -> Result<(), String>;
}

/// Sends nothing; records what it WOULD have sent. The default, so nothing goes out unless
/// a real emailer is deliberately configured.
#[derive(Default)]
pub struct DryRunEmailer {
    pub sent: std::sync::Mutex<Vec<(String, String)>>, // (to, subject)
}

#[async_trait]
impl Emailer for DryRunEmailer {
    async fn send(&self, to: &str, subject: &str, body: &str) -> Result<(), String> {
        println!("[dry-run] would email {to}: {subject}\n{body}\n---");
        self.sent.lock().unwrap().push((to.to_string(), subject.to_string()));
        Ok(())
    }
}

/// The composed briefing: the report text plus the merged provenance behind it.
pub struct Briefing {
    pub subject: String,
    pub body: String,
    pub provenance: Vec<String>,
}

/// Run each question through the read-only NLQ loop and compose one briefing. Pure read
/// path — no email, no mutation.
pub async fn compose(model: &dyn Model, questions: &[String]) -> Briefing {
    let mut body = String::from("Aether cluster briefing\n=======================\n\n");
    let mut provenance = Vec::new();
    for q in questions {
        let answer = nlq::run(model, &EngineTools, q, Budget::default()).await;
        body.push_str(&format!("Q: {q}\n{}\n\n", answer.text));
        provenance.extend(answer.provenance);
    }
    if !provenance.is_empty() {
        body.push_str("— evidence —\n");
        for (i, p) in provenance.iter().enumerate() {
            body.push_str(&format!("  [{}] {p}\n", i + 1));
        }
    }
    Briefing { subject: "Aether cluster briefing".to_string(), body, provenance }
}

/// Compose a briefing and hand it off to each ALLOWLISTED recipient. A recipient not on the
/// allowlist is refused (and named in the returned errors) — the email never leaves for
/// them. Returns the recipients actually delivered to. `send=false` composes only (the
/// dry-run default at the binary level passes a `DryRunEmailer`, so even "send" is inert
/// there).
pub async fn brief_and_send(
    model: &dyn Model,
    questions: &[String],
    recipients: &[String],
    allowlist: &Allowlist,
    emailer: &dyn Emailer,
) -> Result<Vec<String>, Vec<String>> {
    let briefing = compose(model, questions).await;
    let mut delivered = Vec::new();
    let mut errors = Vec::new();
    for r in recipients {
        if !allowlist.allows(r) {
            errors.push(format!("recipient not allowlisted: {r}"));
            continue;
        }
        match emailer.send(r, &briefing.subject, &briefing.body).await {
            Ok(()) => delivered.push(r.clone()),
            Err(e) => errors.push(format!("send to {r} failed: {e}")),
        }
    }
    if errors.is_empty() {
        Ok(delivered)
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlist_is_fail_closed_and_case_insensitive() {
        let a = Allowlist::new(vec!["Ops@Aether.io".into(), " ".into()]);
        assert!(a.allows("ops@aether.io"));
        assert!(a.allows("OPS@AETHER.IO"));
        assert!(!a.allows("evil@example.com"));
        assert!(Allowlist::default().allows("anyone@anywhere").eq(&false), "empty allowlist permits nothing");
    }
}
