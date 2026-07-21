//! Scoped bearer tokens on the client-facing RPCs.
//!
//! mTLS (common::net) answers *who is connecting*; this answers *what they may do*. A
//! token file (`AETHER_TOKENS_FILE`, lines `<token> <scope>`, `#` comments) maps opaque
//! bearer tokens to a scope:
//!   - `read`      — may query and read cluster state;
//!   - `operator`  — may also mutate (drain a node, reassign a vshard). Implies read.
//!
//! Unset file ⇒ auth OFF (local dev), and every check passes. The node-facing RPCs
//! (register, heartbeat, list_*) take NO token — they're authenticated by the member
//! certificate under mTLS; tokens gate only what an external operator or client calls.
//!
//! No external IdP: the decision here is *scoping*, not identity federation. Static
//! tokens keep the trust model auditable in one file.

use std::collections::HashMap;

use tonic::{Request, Status};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Scope {
    Read,
    Operator,
}

impl Scope {
    fn rank(self) -> u8 {
        match self {
            Scope::Read => 1,
            Scope::Operator => 2,
        }
    }
    /// Does a token holding `self` satisfy a requirement of `needed`? (operator ⊇ read)
    fn satisfies(self, needed: Scope) -> bool {
        self.rank() >= needed.rank()
    }
    fn parse(s: &str) -> Option<Scope> {
        match s {
            "read" => Some(Scope::Read),
            "operator" => Some(Scope::Operator),
            _ => None,
        }
    }
}

/// Token→scope table. Empty ⇒ auth disabled.
#[derive(Clone, Default)]
pub struct Auth {
    tokens: HashMap<String, Scope>,
    enabled: bool,
}

impl Auth {
    /// Load from `AETHER_TOKENS_FILE` if set; otherwise auth is disabled.
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let Some(path) = std::env::var("AETHER_TOKENS_FILE").ok() else {
            return Ok(Self::default());
        };
        let contents = std::fs::read_to_string(&path)?;
        let mut tokens = HashMap::new();
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut it = line.split_whitespace();
            let (Some(tok), Some(scope)) = (it.next(), it.next()) else {
                return Err(format!("bad token line: {line:?}").into());
            };
            let scope = Scope::parse(scope).ok_or_else(|| format!("unknown scope: {scope:?}"))?;
            tokens.insert(tok.to_string(), scope);
        }
        if tokens.is_empty() {
            return Err(format!("{path} defines no tokens").into());
        }
        println!("auth: {} scoped token(s) loaded", tokens.len());
        Ok(Self { tokens, enabled: true })
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Enforce that the request carries a token granting at least `needed`. A no-op when
    /// auth is disabled. `Unauthenticated` for a missing/unknown token, `PermissionDenied`
    /// for a known token of insufficient scope — the two failures a caller must tell apart.
    pub fn require<T>(&self, req: &Request<T>, needed: Scope) -> Result<(), Status> {
        if !self.enabled {
            return Ok(());
        }
        let token = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        let scope = self
            .tokens
            .get(token)
            .ok_or_else(|| Status::unauthenticated("unknown token"))?;
        if scope.satisfies(needed) {
            Ok(())
        } else {
            Err(Status::permission_denied(format!("{needed:?} scope required")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth() -> Auth {
        let mut tokens = HashMap::new();
        tokens.insert("r-tok".to_string(), Scope::Read);
        tokens.insert("o-tok".to_string(), Scope::Operator);
        Auth { tokens, enabled: true }
    }

    fn req_with(token: Option<&str>) -> Request<()> {
        let mut r = Request::new(());
        if let Some(t) = token {
            r.metadata_mut().insert("authorization", format!("Bearer {t}").parse().unwrap());
        }
        r
    }

    #[test]
    fn scope_matrix() {
        let a = auth();
        // read token: reads ok, mutations denied.
        assert!(a.require(&req_with(Some("r-tok")), Scope::Read).is_ok());
        assert_eq!(
            a.require(&req_with(Some("r-tok")), Scope::Operator).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        // operator token: both ok.
        assert!(a.require(&req_with(Some("o-tok")), Scope::Read).is_ok());
        assert!(a.require(&req_with(Some("o-tok")), Scope::Operator).is_ok());
        // no token / unknown token: unauthenticated.
        assert_eq!(
            a.require(&req_with(None), Scope::Read).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            a.require(&req_with(Some("nope")), Scope::Read).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn disabled_auth_allows_everything() {
        let a = Auth::default();
        assert!(a.require(&req_with(None), Scope::Operator).is_ok());
    }
}
