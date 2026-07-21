//! Real SMTP delivery for the briefing hand-off, behind the `smtp` feature.
//!
//! This is the ONE place the system sends something outward, so it stays triple-gated:
//! the [`crate::Allowlist`] still decides recipients (this only transports), sending is
//! opt-in (a real config must be present), and the feature is off by default so the
//! shipped binary links no mail stack at all. Nothing here can send without deliberate
//! configuration.

use std::sync::Arc;

use crate::Emailer;

/// Build an SMTP emailer from the environment, or `None` if the feature is off or the
/// config is incomplete (in which case the caller keeps the dry-run default).
///
/// Env: `AETHER_SMTP_HOST`, `AETHER_SMTP_PORT` (default 587), `AETHER_SMTP_USER`,
/// `AETHER_SMTP_PASS`, `AETHER_SMTP_FROM`.
pub fn from_env() -> Option<Arc<dyn Emailer>> {
    #[cfg(feature = "smtp")]
    {
        imp::from_env()
    }
    #[cfg(not(feature = "smtp"))]
    {
        None
    }
}

#[cfg(feature = "smtp")]
mod imp {
    use std::sync::Arc;

    use async_trait::async_trait;
    use lettre::message::header::ContentType;
    use lettre::transport::smtp::authentication::Credentials;
    use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

    use crate::Emailer;

    pub struct SmtpEmailer {
        transport: AsyncSmtpTransport<Tokio1Executor>,
        from: String,
    }

    pub fn from_env() -> Option<Arc<dyn Emailer>> {
        let host = std::env::var("AETHER_SMTP_HOST").ok()?;
        let user = std::env::var("AETHER_SMTP_USER").ok()?;
        let pass = std::env::var("AETHER_SMTP_PASS").ok()?;
        let from = std::env::var("AETHER_SMTP_FROM").ok()?;
        let port: u16 = std::env::var("AETHER_SMTP_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(587);

        let transport = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&host)
            .ok()?
            .port(port)
            .credentials(Credentials::new(user, pass))
            .build();
        Some(Arc::new(SmtpEmailer { transport, from }))
    }

    #[async_trait]
    impl Emailer for SmtpEmailer {
        async fn send(&self, to: &str, subject: &str, body: &str) -> Result<(), String> {
            let email = Message::builder()
                .from(self.from.parse().map_err(|e| format!("bad from address: {e}"))?)
                .to(to.parse().map_err(|e| format!("bad recipient {to}: {e}"))?)
                .subject(subject)
                .header(ContentType::TEXT_PLAIN)
                .body(body.to_string())
                .map_err(|e| format!("build message: {e}"))?;
            self.transport.send(email).await.map_err(|e| format!("smtp send: {e}"))?;
            Ok(())
        }
    }
}
