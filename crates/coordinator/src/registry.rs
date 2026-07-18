//! The coordinator's live view of cluster topology: which node leads each shard, and which
//! nodes are followers, built up from node registrations at runtime.
//!
//! # N-parameterized
//! `shard_count` (N) is fixed at construction from config — never a hardcoded literal. The
//! registry rejects registrations for a `shard_id` outside `0..N`, and reports N back to
//! nodes so they never assume a cluster size. This is what makes "horizontally scalable" a
//! claim backed by code: change N in config and the whole control plane follows.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use common::pb::{NodeRole, RegisterNodeRequest, RegisterNodeResponse};

/// A registered node's connection info.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub node_id: String,
    pub address: String,
    pub role: NodeRole,
    /// When we last heard from this node (registration or heartbeat). Used to reap dead nodes.
    pub last_seen: Instant,
}

/// The coordinator's shard map. One leader per shard (last registration wins, which also
/// models a follower being promoted and re-registering as leader on failover), plus followers.
pub struct Registry {
    shard_count: u32,
    leaders: HashMap<u32, NodeInfo>,
    followers: HashMap<u32, Vec<NodeInfo>>,
}

impl Registry {
    /// Create a registry for a cluster of `shard_count` (N) shards.
    pub fn new(shard_count: u32) -> Self {
        Self {
            shard_count,
            leaders: HashMap::new(),
            followers: HashMap::new(),
        }
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Handle a node registration, updating the shard map. Returns the wire response
    /// (including current N so the node never assumes cluster size).
    pub fn register(&mut self, req: RegisterNodeRequest) -> RegisterNodeResponse {
        // Reject a shard_id the cluster doesn't have — a misconfigured node must not silently
        // own a shard outside 0..N.
        if req.shard_id >= self.shard_count {
            return self.reject(format!(
                "shard_id {} out of range for N={}",
                req.shard_id, self.shard_count
            ));
        }

        let role = req.role(); // prost accessor: i32 field -> NodeRole enum
        let info = NodeInfo {
            node_id: req.node_id,
            address: req.address,
            role,
            last_seen: Instant::now(),
        };

        match role {
            NodeRole::Leader => {
                self.leaders.insert(req.shard_id, info);
            }
            NodeRole::Follower => {
                // Idempotent: re-registering the same node updates it rather than duplicating.
                let followers = self.followers.entry(req.shard_id).or_default();
                match followers.iter_mut().find(|n| n.node_id == info.node_id) {
                    Some(existing) => *existing = info,
                    None => followers.push(info),
                }
            }
            NodeRole::Unspecified => {
                return self.reject("role unspecified".to_string());
            }
        }

        RegisterNodeResponse {
            accepted: true,
            cluster_size: self.shard_count,
            message: format!("registered shard {} as {:?}", req.shard_id, role),
        }
    }

    fn reject(&self, message: String) -> RegisterNodeResponse {
        RegisterNodeResponse {
            accepted: false,
            cluster_size: self.shard_count,
            message,
        }
    }

    /// Leader addresses for query fan-out (scatter-gather).
    pub fn leader_addresses(&self) -> Vec<String> {
        self.leaders.values().map(|n| n.address.clone()).collect()
    }

    /// Follower addresses for a shard, so its leader knows where to replicate.
    pub fn follower_addresses(&self, shard_id: u32) -> Vec<String> {
        self.followers
            .get(&shard_id)
            .map(|nodes| nodes.iter().map(|n| n.address.clone()).collect())
            .unwrap_or_default()
    }

    /// How many distinct shards currently have a leader.
    pub fn leaders_registered(&self) -> usize {
        self.leaders.len()
    }

    /// True once every shard in `0..N` has a registered leader — the cluster is fully staffed
    /// and ready to serve complete results.
    pub fn all_shards_have_leader(&self) -> bool {
        self.leaders.len() as u32 == self.shard_count
    }

    /// Refresh a node's liveness (called on heartbeat). Returns false if the node isn't known,
    /// which tells the node to register again (e.g. after a coordinator restart).
    pub fn heartbeat(&mut self, node_id: &str) -> bool {
        let now = Instant::now();
        for node in self.leaders.values_mut() {
            if node.node_id == node_id {
                node.last_seen = now;
                return true;
            }
        }
        for nodes in self.followers.values_mut() {
            for node in nodes.iter_mut() {
                if node.node_id == node_id {
                    node.last_seen = now;
                    return true;
                }
            }
        }
        false
    }

    /// Remove nodes we haven't heard from within `timeout` (as of `now`) and return them, so a
    /// dead node stops being routed to. `now` is a parameter so this is deterministic to test.
    pub fn reap_dead(&mut self, now: Instant, timeout: Duration) -> Vec<NodeInfo> {
        let mut removed = Vec::new();

        let dead_shards: Vec<u32> = self
            .leaders
            .iter()
            .filter(|(_, node)| now.saturating_duration_since(node.last_seen) > timeout)
            .map(|(shard, _)| *shard)
            .collect();
        for shard in dead_shards {
            if let Some(node) = self.leaders.remove(&shard) {
                removed.push(node);
            }
        }

        for nodes in self.followers.values_mut() {
            let mut i = 0;
            while i < nodes.len() {
                if now.saturating_duration_since(nodes[i].last_seen) > timeout {
                    removed.push(nodes.remove(i));
                } else {
                    i += 1;
                }
            }
        }

        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(node_id: &str, shard_id: u32, role: NodeRole) -> RegisterNodeRequest {
        RegisterNodeRequest {
            node_id: node_id.to_string(),
            address: format!("127.0.0.1:600{shard_id}"),
            shard_id,
            role: role as i32,
        }
    }

    #[test]
    fn accepts_leader_in_range_and_reports_n() {
        let mut reg = Registry::new(3);
        let resp = reg.register(req("n0", 0, NodeRole::Leader));
        assert!(resp.accepted);
        assert_eq!(resp.cluster_size, 3);
        assert_eq!(reg.leaders_registered(), 1);
    }

    #[test]
    fn rejects_shard_id_out_of_range() {
        let mut reg = Registry::new(3);
        let resp = reg.register(req("bad", 5, NodeRole::Leader));
        assert!(!resp.accepted);
        assert_eq!(resp.cluster_size, 3);
        assert_eq!(reg.leaders_registered(), 0);
    }

    #[test]
    fn rejects_unspecified_role() {
        let mut reg = Registry::new(1);
        let resp = reg.register(req("n0", 0, NodeRole::Unspecified));
        assert!(!resp.accepted);
    }

    #[test]
    fn all_shards_have_leader_when_every_shard_led() {
        let mut reg = Registry::new(2);
        assert!(!reg.all_shards_have_leader());
        reg.register(req("n0", 0, NodeRole::Leader));
        reg.register(req("n1", 1, NodeRole::Leader));
        assert!(reg.all_shards_have_leader());
        assert_eq!(reg.leader_addresses().len(), 2);
    }

    #[test]
    fn re_registering_a_shard_leader_replaces_it() {
        // Models failover: a promoted node re-registers as the shard's leader.
        let mut reg = Registry::new(1);
        reg.register(req("old", 0, NodeRole::Leader));
        let mut newer = req("new", 0, NodeRole::Leader);
        newer.address = "127.0.0.1:9999".to_string();
        reg.register(newer);
        assert_eq!(reg.leaders_registered(), 1);
        assert_eq!(reg.leader_addresses(), vec!["127.0.0.1:9999".to_string()]);
    }

    #[test]
    fn followers_are_tracked_separately() {
        let mut reg = Registry::new(1);
        reg.register(req("leader", 0, NodeRole::Leader));
        reg.register(req("follower", 0, NodeRole::Follower));
        assert_eq!(reg.leaders_registered(), 1); // follower is not a leader
    }

    #[test]
    fn reap_removes_dead_nodes_and_keeps_fresh_ones() {
        let mut reg = Registry::new(2);
        reg.register(req("l0", 0, NodeRole::Leader));
        reg.register(req("l1", 1, NodeRole::Leader));

        // Just after registration nothing is dead.
        let t = Instant::now();
        assert!(reg.reap_dead(t, Duration::from_secs(30)).is_empty());

        // 60s later with a 30s timeout, both leaders are reaped.
        let later = t.checked_add(Duration::from_secs(60)).unwrap();
        let removed = reg.reap_dead(later, Duration::from_secs(30));
        assert_eq!(removed.len(), 2);
        assert_eq!(reg.leaders_registered(), 0);
        assert!(reg.leader_addresses().is_empty());
    }

    #[test]
    fn heartbeat_keeps_a_node_alive_and_reports_unknown() {
        let mut reg = Registry::new(1);
        reg.register(req("l0", 0, NodeRole::Leader));

        assert!(reg.heartbeat("l0")); // known
        assert!(!reg.heartbeat("ghost")); // unknown -> should re-register

        // A reap a moment after the heartbeat leaves it alive.
        let soon = Instant::now().checked_add(Duration::from_secs(1)).unwrap();
        assert!(reg.reap_dead(soon, Duration::from_secs(30)).is_empty());
        assert_eq!(reg.leaders_registered(), 1);
    }

    #[test]
    fn re_registering_a_follower_does_not_duplicate_it() {
        let mut reg = Registry::new(1);
        reg.register(req("f0", 0, NodeRole::Follower));
        reg.register(req("f0", 0, NodeRole::Follower)); // same node_id again
        assert_eq!(reg.follower_addresses(0).len(), 1);
    }

    #[test]
    fn follower_addresses_lists_only_that_shards_followers() {
        let mut reg = Registry::new(2);
        let mut f0 = req("f0", 0, NodeRole::Follower);
        f0.address = "127.0.0.1:7000".to_string();
        reg.register(f0);
        let mut f1 = req("f1", 1, NodeRole::Follower);
        f1.address = "127.0.0.1:7001".to_string();
        reg.register(f1);

        assert_eq!(reg.follower_addresses(0), vec!["127.0.0.1:7000".to_string()]);
        assert_eq!(reg.follower_addresses(1), vec!["127.0.0.1:7001".to_string()]);
        assert!(reg.follower_addresses(0).len() == 1);
    }
}
