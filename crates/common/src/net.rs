//! Cluster transport security, in one place: every internal gRPC connection — client
//! and server side — is built here, so "turn on mTLS" is one environment variable and
//! not a per-call-site audit.
//!
//! `AETHER_TLS_DIR` opts in: it names a directory produced by the cert tooling
//! (`ca.crt` + `<role>.crt`/`<role>.key`, role from `AETHER_TLS_ROLE`). When set,
//! servers REQUIRE client certificates signed by the CA (mutual TLS — a caller without
//! an identity can't even say hello), and clients present their identity and verify the
//! server against the same CA. Unset, everything is plaintext for local development —
//! and both modes stay tested, because an untested security path is decoration.
//!
//! Verification name: clients verify the server as `localhost` (every cluster cert
//! carries that SAN alongside its IPs). Within a single-CA cluster this pins "someone
//! this CA signed", not "this specific host" — the honest scope of a dev-CA trust
//! model, recorded as such.

use std::path::PathBuf;
use std::sync::OnceLock;

use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity, ServerTlsConfig};

pub type NetError = Box<dyn std::error::Error + Send + Sync>;

/// Install the process-wide rustls crypto provider. Idempotent and safe to call from
/// anywhere; rustls 0.23 refuses to auto-pick when more than one backend is linked (ours
/// are, transitively via the AWS SDK), so every binary and test that may touch TLS calls
/// this once at startup. A no-op in plaintext mode, but cheap enough to always run.
pub fn install_crypto() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

struct TlsMaterial {
    ca: Certificate,
    identity: Identity,
}

/// The TLS material, loaded once. `None` = plaintext mode.
fn material() -> Option<&'static TlsMaterial> {
    static MATERIAL: OnceLock<Option<TlsMaterial>> = OnceLock::new();
    MATERIAL
        .get_or_init(|| {
            let dir = PathBuf::from(std::env::var("AETHER_TLS_DIR").ok()?);
            let role = std::env::var("AETHER_TLS_ROLE").unwrap_or_else(|_| "operator".into());
            let read = |name: &str| match std::fs::read(dir.join(name)) {
                Ok(bytes) => Some(bytes),
                Err(e) => {
                    // Half-configured TLS must fail loudly, not fall back to plaintext.
                    panic!("AETHER_TLS_DIR set but {name} unreadable in {dir:?}: {e}");
                }
            };
            Some(TlsMaterial {
                ca: Certificate::from_pem(read("ca.crt")?),
                identity: Identity::from_pem(read(&format!("{role}.crt"))?, read(&format!("{role}.key"))?),
            })
        })
        .as_ref()
}

/// Whether cluster TLS is enabled in this process.
pub fn tls_enabled() -> bool {
    material().is_some()
}

/// A connected channel to `addr` (host:port), TLS'd when the cluster runs TLS.
pub async fn channel(addr: &str) -> Result<Channel, NetError> {
    match material() {
        Some(m) => {
            let tls = ClientTlsConfig::new()
                .ca_certificate(m.ca.clone())
                .identity(m.identity.clone())
                .domain_name("localhost");
            Ok(Channel::from_shared(format!("https://{addr}"))?
                .tls_config(tls)?
                .connect()
                .await?)
        }
        None => Ok(Channel::from_shared(format!("http://{addr}"))?.connect().await?),
    }
}

/// Attach this process's bearer token (`AETHER_TOKEN`) to an outgoing request, if set.
/// A no-op when unset — plaintext/no-auth dev needs nothing. Client-facing callers wrap
/// their requests with this; internal node RPCs don't (they authenticate by mTLS cert).
pub fn with_token<T>(msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    if let Ok(token) = std::env::var("AETHER_TOKEN") {
        if let Ok(v) = format!("Bearer {token}").parse() {
            req.metadata_mut().insert("authorization", v);
        }
    }
    req
}

/// Server-side config: `Some` = require mTLS (identity presented, client certs demanded
/// and verified against the CA), `None` = plaintext mode.
pub fn server_tls() -> Option<ServerTlsConfig> {
    material().map(|m| {
        ServerTlsConfig::new()
            .identity(m.identity.clone())
            .client_ca_root(m.ca.clone())
    })
}
