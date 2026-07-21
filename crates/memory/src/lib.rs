//! Agent memory as a governed index type.
//!
//! Every agent framework bolts memory on as an unmanaged side store. Here it is a real
//! index type with SEMANTICS: entries live in a named namespace, carry a TTL and a writer
//! attribution stamp, are quota'd per writer, and are searchable. Telemetry stays
//! absolutely read-only; memory is the ONE place an agent may write, and these four
//! properties — isolation, TTL, attribution, quota — are what make that exception safe.
//!
//! Storage scope (honest): this is the governed memory LAYER with its own
//! store, deliberately separate from the flight telemetry index (structural namespace
//! isolation). Distributing it across the shard raft groups and unifying the two stores
//! under one generic document type is a larger refactor, deferred.

use std::collections::HashMap;

/// One remembered item. `id` is assigned by the store; provenance is `writer` + `created_ms`.
#[derive(Clone, Debug)]
pub struct MemoryEntry {
    pub id: u64,
    pub namespace: String,
    pub text: String,
    pub writer: String,
    pub created_ms: u64,
    pub ttl_secs: u64,
}

impl MemoryEntry {
    fn expired_at(&self, now_ms: u64) -> bool {
        self.ttl_secs != 0 && now_ms >= self.created_ms + self.ttl_secs * 1000
    }
}

/// Why a write was refused — the governance boundary, surfaced loudly (never a silent drop).
#[derive(Debug, PartialEq)]
pub enum WriteError {
    /// This writer already holds the per-namespace quota.
    QuotaExceeded { writer: String, quota: usize },
    /// Empty namespace/text/writer — memory requires attribution and content.
    Malformed(&'static str),
}

/// A governed, namespaced memory store. One instance is the memory LAYER; the flight
/// telemetry index is a different type entirely, so a memory write can never touch it.
pub struct MemoryStore {
    by_ns: HashMap<String, Vec<MemoryEntry>>,
    next_id: u64,
    /// Max live entries per (namespace, writer). The quota that bounds an agent's footprint.
    quota_per_writer: usize,
}

impl MemoryStore {
    pub fn new(quota_per_writer: usize) -> Self {
        Self { by_ns: HashMap::new(), next_id: 0, quota_per_writer }
    }

    /// Govern-and-write one entry. Enforces attribution, quota (over LIVE entries — expired
    /// ones are swept first so TTL frees quota), and stamps provenance. The only write path.
    pub fn remember(
        &mut self,
        namespace: &str,
        text: &str,
        writer: &str,
        ttl_secs: u64,
        now_ms: u64,
    ) -> Result<u64, WriteError> {
        if namespace.trim().is_empty() {
            return Err(WriteError::Malformed("namespace is required"));
        }
        if text.trim().is_empty() {
            return Err(WriteError::Malformed("text is required"));
        }
        if writer.trim().is_empty() {
            return Err(WriteError::Malformed("writer attribution is required"));
        }
        self.sweep_namespace(namespace, now_ms);
        let live_by_writer = self
            .by_ns
            .get(namespace)
            .map(|v| v.iter().filter(|e| e.writer == writer).count())
            .unwrap_or(0);
        if live_by_writer >= self.quota_per_writer {
            return Err(WriteError::QuotaExceeded { writer: writer.to_string(), quota: self.quota_per_writer });
        }
        let id = self.next_id;
        self.next_id += 1;
        self.by_ns.entry(namespace.to_string()).or_default().push(MemoryEntry {
            id,
            namespace: namespace.to_string(),
            text: text.to_string(),
            writer: writer.to_string(),
            created_ms: now_ms,
            ttl_secs,
        });
        Ok(id)
    }

    /// Recall: token-overlap search WITHIN a namespace, freshest first, skipping expired.
    /// Cross-namespace recall is impossible by construction — the namespace IS the key.
    pub fn recall(&self, namespace: &str, query: &str, limit: usize, now_ms: u64) -> Vec<&MemoryEntry> {
        let terms: Vec<String> = tokenize(query);
        let mut hits: Vec<(usize, &MemoryEntry)> = self
            .by_ns
            .get(namespace)
            .map(|v| v.iter().collect::<Vec<_>>())
            .unwrap_or_default()
            .into_iter()
            .filter(|e| !e.expired_at(now_ms))
            .filter_map(|e| {
                let toks = tokenize(&e.text);
                let overlap = terms.iter().filter(|t| toks.contains(t)).count();
                // Empty query = list everything (overlap 0 still returned); else require ≥1.
                if terms.is_empty() || overlap > 0 {
                    Some((overlap, e))
                } else {
                    None
                }
            })
            .collect();
        // More overlap first, then freshest (higher created_ms) first.
        hits.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.created_ms.cmp(&a.1.created_ms)));
        let mut out: Vec<&MemoryEntry> = hits.into_iter().map(|(_, e)| e).collect();
        if limit != 0 && out.len() > limit {
            out.truncate(limit);
        }
        out
    }

    /// Evict expired entries in a namespace; return how many were dropped.
    fn sweep_namespace(&mut self, namespace: &str, now_ms: u64) -> usize {
        if let Some(v) = self.by_ns.get_mut(namespace) {
            let before = v.len();
            v.retain(|e| !e.expired_at(now_ms));
            before - v.len()
        } else {
            0
        }
    }

    /// Evict expired entries across all namespaces (the periodic TTL sweep); returns count.
    pub fn sweep(&mut self, now_ms: u64) -> usize {
        let namespaces: Vec<String> = self.by_ns.keys().cloned().collect();
        namespaces.iter().map(|ns| self.sweep_namespace(ns, now_ms)).sum()
    }

    /// Live entry count in a namespace (expired excluded).
    pub fn len(&self, namespace: &str, now_ms: u64) -> usize {
        self.by_ns.get(namespace).map(|v| v.iter().filter(|e| !e.expired_at(now_ms)).count()).unwrap_or(0)
    }
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const HR: u64 = 3600;

    #[test]
    fn writes_are_attributed_and_recallable_within_a_namespace() {
        let mut m = MemoryStore::new(10);
        m.remember("agent-a", "user prefers metric units", "agent-a", HR, 1000).unwrap();
        m.remember("agent-a", "user is in Berlin", "agent-a", HR, 2000).unwrap();
        let hits = m.recall("agent-a", "units", 5, 3000);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].writer, "agent-a");
        assert!(hits[0].text.contains("metric"));
    }

    #[test]
    fn namespaces_are_isolated() {
        let mut m = MemoryStore::new(10);
        m.remember("agent-a", "secret from a", "agent-a", HR, 1000).unwrap();
        m.remember("agent-b", "secret from b", "agent-b", HR, 1000).unwrap();
        // A recall in one namespace never sees the other's entries.
        let a = m.recall("agent-a", "secret", 5, 2000);
        assert_eq!(a.len(), 1);
        assert!(a[0].text.contains("from a"));
        assert_eq!(m.recall("agent-a", "", 0, 2000).len(), 1, "namespace a holds exactly its own");
    }

    #[test]
    fn quota_rejection_is_loud_and_ttl_frees_it() {
        let mut m = MemoryStore::new(2);
        m.remember("ns", "one", "w", 1, 0).unwrap();
        m.remember("ns", "two", "w", 1, 0).unwrap();
        // Third write by the same writer is refused — loudly, with the quota named.
        let err = m.remember("ns", "three", "w", 1, 0).unwrap_err();
        assert_eq!(err, WriteError::QuotaExceeded { writer: "w".into(), quota: 2 });
        // A different writer has its own quota.
        assert!(m.remember("ns", "other", "w2", 1, 0).is_ok());
        // After the 1s TTL, the writer's entries expire → quota is free again.
        assert!(m.remember("ns", "later", "w", 1, 2000).is_ok(), "TTL should free the quota");
    }

    #[test]
    fn ttl_expiry_removes_from_recall_and_sweep_counts_it() {
        let mut m = MemoryStore::new(10);
        m.remember("ns", "ephemeral", "w", 1, 0).unwrap(); // expires at 1000ms
        assert_eq!(m.recall("ns", "ephemeral", 5, 500).len(), 1, "live before TTL");
        assert_eq!(m.recall("ns", "ephemeral", 5, 1500).len(), 0, "gone after TTL");
        assert_eq!(m.sweep(1500), 1, "sweep evicts the expired entry");
        assert_eq!(m.len("ns", 1500), 0);
    }

    #[test]
    fn malformed_writes_are_refused() {
        let mut m = MemoryStore::new(10);
        assert!(matches!(m.remember("", "t", "w", 0, 0), Err(WriteError::Malformed(_))));
        assert!(matches!(m.remember("ns", "", "w", 0, 0), Err(WriteError::Malformed(_))));
        assert!(matches!(m.remember("ns", "t", "", 0, 0), Err(WriteError::Malformed(_))));
    }
}
