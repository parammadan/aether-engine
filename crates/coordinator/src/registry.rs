//! The coordinator's live view of cluster topology: which node leads each shard, and which
//! nodes are followers, built up from node registrations at runtime.
//!
//! # N-parameterized
//! `shard_count` (N) is fixed at construction from config — never a hardcoded literal. The
//! registry rejects registrations for a `shard_id` outside `0..N`, and reports N back to
//! nodes so they never assume a cluster size. This is what makes "horizontally scalable" a
//! claim backed by code: change N in config and the whole control plane follows.

use std::collections::HashMap;

use common::pb::{NodeRole, RegisterNodeRequest, RegisterNodeResponse};

/// A registered node's connection info.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub node_id: String,
    pub address: String,
    pub role: NodeRole,
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
        };

        match role {
            NodeRole::Leader => {
                self.leaders.insert(req.shard_id, info);
            }
            NodeRole::Follower => {
                self.followers.entry(req.shard_id).or_default().push(info);
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
