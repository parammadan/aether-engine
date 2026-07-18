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

/// One node in a [`Registry::snapshot`]: its identity plus how stale its liveness is.
#[derive(Clone, Debug)]
pub struct NodeSnapshot {
    pub node_id: String,
    pub address: String,
    pub role: NodeRole,
    pub shard_id: u32,
    pub since_seen: Duration,
}

impl NodeSnapshot {
    fn from_info(info: &NodeInfo, shard_id: u32, now: Instant) -> Self {
        Self {
            node_id: info.node_id.clone(),
            address: info.address.clone(),
            role: info.role,
            shard_id,
            since_seen: now.saturating_duration_since(info.last_seen),
        }
    }
}

/// Default liveness window: how recently a node must have been seen for the registry to
/// treat it as alive when guarding leader registrations. Kept in sync with the reaper's
/// timeout by the binary's config.
const DEFAULT_LIVENESS_TIMEOUT: Duration = Duration::from_secs(15);

/// The coordinator's shard map: one leader per shard plus its followers.
///
/// Leader registrations are guarded: a shard's live leader cannot be overwritten by a
/// *different* node registering as leader (see [`Registry::register_at`]). This is a
/// stopgap against a restarted stale leader evicting a promoted follower — NOT a
/// split-brain solution; two nodes can still both believe they lead across a partition,
/// and only consensus can rule that out.
pub struct Registry {
    shard_count: u32,
    liveness_timeout: Duration,
    leaders: HashMap<u32, NodeInfo>,
    followers: HashMap<u32, Vec<NodeInfo>>,
}

impl Registry {
    /// Create a registry for a cluster of `shard_count` (N) shards.
    pub fn new(shard_count: u32) -> Self {
        Self {
            shard_count,
            liveness_timeout: DEFAULT_LIVENESS_TIMEOUT,
            leaders: HashMap::new(),
            followers: HashMap::new(),
        }
    }

    /// Override the liveness window used by the leader-registration guard (the binary passes
    /// the same value the reaper uses, so "alive" means one thing everywhere).
    pub fn with_liveness_timeout(mut self, timeout: Duration) -> Self {
        self.liveness_timeout = timeout;
        self
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Handle a node registration, updating the shard map. Returns the wire response
    /// (including current N so the node never assumes cluster size).
    pub fn register(&mut self, req: RegisterNodeRequest) -> RegisterNodeResponse {
        self.register_at(req, Instant::now())
    }

    /// [`Registry::register`] with an injected clock, so the leader-liveness guard is
    /// deterministic to test.
    pub fn register_at(&mut self, req: RegisterNodeRequest, now: Instant) -> RegisterNodeResponse {
        // Reject a shard_id the cluster doesn't have — a misconfigured node must not silently
        // own a shard outside 0..N.
        if req.shard_id >= self.shard_count {
            return self.reject(format!(
                "shard_id {} out of range for N={}",
                req.shard_id, self.shard_count
            ));
        }

        let role = req.role(); // prost accessor: i32 field -> NodeRole enum

        // STOPGAP guard, not consensus: a *different* node may not take over a shard whose
        // current leader is still live (seen within the liveness window). This stops a
        // restarted stale leader — with an empty, freshly-booted index — from evicting a
        // promoted follower that holds the data. A stale incumbent may be replaced (that is
        // failover-by-registration), and a node may always re-register as itself. What this
        // cannot do is arbitrate a partition where two nodes both believe they lead; that is
        // a consensus problem, deliberately out of scope here.
        if role == NodeRole::Leader {
            if let Some(current) = self.leaders.get(&req.shard_id) {
                let current_is_live =
                    now.saturating_duration_since(current.last_seen) <= self.liveness_timeout;
                if current_is_live && current.node_id != req.node_id {
                    return self.reject(format!(
                        "shard {} already has a live leader '{}'; rejecting takeover by '{}'",
                        req.shard_id, current.node_id, req.node_id
                    ));
                }
            }
        }

        let info = NodeInfo {
            node_id: req.node_id,
            address: req.address,
            role,
            last_seen: now,
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

    /// Refresh a node's liveness (called on heartbeat). Returns the coordinator's current
    /// view of the node's role — `Some(role)` if known (so e.g. a promoted follower learns it
    /// now leads), or `None` if unknown, which tells the node to register again (e.g. after a
    /// coordinator restart). `now` is a parameter so liveness/promotion are deterministic to
    /// test.
    pub fn heartbeat(&mut self, node_id: &str, now: Instant) -> Option<NodeRole> {
        for node in self.leaders.values_mut() {
            if node.node_id == node_id {
                node.last_seen = now;
                return Some(NodeRole::Leader);
            }
        }
        for nodes in self.followers.values_mut() {
            for node in nodes.iter_mut() {
                if node.node_id == node_id {
                    node.last_seen = now;
                    return Some(NodeRole::Follower);
                }
            }
        }
        None
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

    /// A point-in-time view of every known node, for observability (dashboards/tooling).
    pub fn snapshot(&self, now: Instant) -> Vec<NodeSnapshot> {
        let mut nodes = Vec::new();
        for (&shard_id, info) in &self.leaders {
            nodes.push(NodeSnapshot::from_info(info, shard_id, now));
        }
        for (&shard_id, followers) in &self.followers {
            for info in followers {
                nodes.push(NodeSnapshot::from_info(info, shard_id, now));
            }
        }
        // Stable order for consumers: by shard, leaders first, then node id.
        nodes.sort_by(|a, b| {
            a.shard_id
                .cmp(&b.shard_id)
                .then_with(|| (a.role != NodeRole::Leader).cmp(&(b.role != NodeRole::Leader)))
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
        nodes
    }

    /// For any shard that has followers but no leader (its leader was reaped), promote one
    /// follower to leader and update the shard map. Returns `(shard_id, promoted node_id)` for
    /// each promotion. Because the promoted node already holds the replicated data and serves
    /// `ShardSearch`, scatter-gather starts routing to it immediately — this is failover.
    pub fn promote_orphaned_shards(&mut self) -> Vec<(u32, String)> {
        let mut promotions = Vec::new();

        let shards_with_followers: Vec<u32> = self.followers.keys().copied().collect();
        for shard in shards_with_followers {
            if self.leaders.contains_key(&shard) {
                continue; // shard still has a live leader
            }
            // Take one follower (the borrow of `followers` ends on this line).
            let candidate = self.followers.get_mut(&shard).and_then(|f| f.pop());
            if let Some(mut node) = candidate {
                node.role = NodeRole::Leader;
                let node_id = node.node_id.clone();
                self.leaders.insert(shard, node);
                promotions.push((shard, node_id));

                if self.followers.get(&shard).map_or(false, |f| f.is_empty()) {
                    self.followers.remove(&shard);
                }
            }
        }

        promotions
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
    fn live_leader_cannot_be_overwritten_by_another_node() {
        // Stopgap guard: a restarted stale leader (fresh empty index) must not evict the
        // shard's live leader (e.g. a promoted follower that holds the data).
        let mut reg = Registry::new(1);
        let t0 = Instant::now();
        reg.register_at(req("incumbent", 0, NodeRole::Leader), t0);

        // A different node claiming leadership while the incumbent is live -> rejected.
        let resp = reg.register_at(req("usurper", 0, NodeRole::Leader), t0);
        assert!(!resp.accepted);
        assert_eq!(reg.leaders_registered(), 1);
        assert_eq!(reg.leader_addresses(), vec!["127.0.0.1:6000".to_string()]);
    }

    #[test]
    fn stale_leader_can_be_replaced_and_self_reregistration_is_allowed() {
        let mut reg = Registry::new(1);
        let t0 = Instant::now();
        reg.register_at(req("incumbent", 0, NodeRole::Leader), t0);

        // The same node may always re-register as itself (heartbeat-recovery path).
        let same = reg.register_at(req("incumbent", 0, NodeRole::Leader), t0);
        assert!(same.accepted);

        // Once the incumbent is stale (unseen past the liveness window), another node may
        // take over — failover-by-registration.
        let later = t0.checked_add(Duration::from_secs(60)).unwrap();
        let mut takeover = req("successor", 0, NodeRole::Leader);
        takeover.address = "127.0.0.1:9999".to_string();
        let resp = reg.register_at(takeover, later);
        assert!(resp.accepted);
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

        let now = Instant::now();
        assert_eq!(reg.heartbeat("l0", now), Some(NodeRole::Leader)); // known, with current role
        assert_eq!(reg.heartbeat("ghost", now), None); // unknown -> should re-register

        // A reap a moment after the heartbeat leaves it alive.
        let soon = now.checked_add(Duration::from_secs(1)).unwrap();
        assert!(reg.reap_dead(soon, Duration::from_secs(30)).is_empty());
        assert_eq!(reg.leaders_registered(), 1);
    }

    #[test]
    fn dead_leader_with_live_follower_is_failed_over() {
        let mut reg = Registry::new(1);
        let t0 = Instant::now();
        reg.register(req("leader", 0, NodeRole::Leader));
        let mut follower = req("follower", 0, NodeRole::Follower);
        follower.address = "127.0.0.1:7000".to_string();
        reg.register(follower);

        // 60s later the follower heartbeats (fresh) but the leader does not.
        let now = t0.checked_add(Duration::from_secs(60)).unwrap();
        reg.heartbeat("follower", now);

        // The reaper drops the stale leader...
        let removed = reg.reap_dead(now, Duration::from_secs(30));
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].node_id, "leader");
        assert!(!reg.all_shards_have_leader()); // shard 0 momentarily leaderless

        // ...then promotes the live follower to leader.
        let promotions = reg.promote_orphaned_shards();
        assert_eq!(promotions, vec![(0u32, "follower".to_string())]);
        assert!(reg.all_shards_have_leader());
        assert_eq!(reg.leader_addresses(), vec!["127.0.0.1:7000".to_string()]);
        assert!(reg.follower_addresses(0).is_empty()); // the follower became the leader

        // The promoted node's next heartbeat tells it about its new role, so it re-registers
        // as leader (not its boot-time role) if the coordinator ever restarts.
        assert_eq!(reg.heartbeat("follower", now), Some(NodeRole::Leader));
    }

    #[test]
    fn orphaned_shard_without_a_follower_stays_leaderless() {
        let mut reg = Registry::new(1);
        let t0 = Instant::now();
        reg.register(req("leader", 0, NodeRole::Leader));

        let now = t0.checked_add(Duration::from_secs(60)).unwrap();
        reg.reap_dead(now, Duration::from_secs(30)); // leader gone, no follower to promote
        assert!(reg.promote_orphaned_shards().is_empty());
        assert!(!reg.all_shards_have_leader());
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
