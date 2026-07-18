//! Process supervisor: spawns the coordinator and shard nodes as real child processes and
//! can SIGKILL them on demand. This is what makes the dashboard's "kill" button an honest
//! chaos experiment — a process dies mid-flight and the cluster's own liveness + failover
//! machinery (heartbeats, reaper, promotion) does the recovery; nothing is simulated.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
use std::sync::Mutex;

pub struct Supervisor {
    children: Mutex<HashMap<String, Child>>,
    next_port: AtomicU16,
    follower_seq: AtomicU32,
    shard_count: u32,
    pub coordinator_addr: String,
}

impl Supervisor {
    pub fn new(shard_count: u32, coordinator_addr: String) -> Self {
        Self {
            children: Mutex::new(HashMap::new()),
            next_port: AtomicU16::new(50051),
            follower_seq: AtomicU32::new(1),
            shard_count,
            coordinator_addr,
        }
    }

    /// The coordinator/shard-node binaries live next to this executable (same target dir).
    fn bin(name: &str) -> PathBuf {
        let mut path = std::env::current_exe().expect("current_exe");
        path.pop();
        path.push(name);
        path
    }

    pub fn spawn_coordinator(&self) -> io::Result<()> {
        let child = Command::new(Self::bin("coordinator"))
            .env("AETHER_COORDINATOR_ADDR", &self.coordinator_addr)
            .env("AETHER_SHARD_COUNT", self.shard_count.to_string())
            // Snappy failover for a live demo: drop a silent node after 6s, reap every 2s.
            .env("AETHER_LIVENESS_TIMEOUT_SECS", "6")
            .spawn()?;
        self.children.lock().unwrap().insert("coordinator".to_string(), child);
        Ok(())
    }

    fn spawn_shard_node(&self, node_id: &str, shard_id: u32, role: &str) -> io::Result<()> {
        let port = self.next_port.fetch_add(1, Ordering::SeqCst);
        let child = Command::new(Self::bin("shard-node"))
            .env("AETHER_SHARD_ADDR", format!("127.0.0.1:{port}"))
            .env("AETHER_SHARD_INDEX", shard_id.to_string())
            .env("AETHER_SHARD_COUNT", self.shard_count.to_string())
            .env("AETHER_ROLE", role)
            .env("AETHER_COORDINATOR_ADDR", &self.coordinator_addr)
            .env("AETHER_NODE_ID", node_id)
            .env("AETHER_HEARTBEAT_SECS", "2")
            .env("AETHER_POLL_SECS", "10")
            .spawn()?;
        self.children.lock().unwrap().insert(node_id.to_string(), child);
        Ok(())
    }

    /// Initial topology: one leader + one follower per shard.
    pub fn spawn_initial_topology(&self) -> io::Result<Vec<String>> {
        let mut spawned = Vec::new();
        for shard in 0..self.shard_count {
            let leader = format!("shard{shard}-leader");
            self.spawn_shard_node(&leader, shard, "leader")?;
            spawned.push(leader);
            let follower = format!("shard{shard}-f0");
            self.spawn_shard_node(&follower, shard, "follower")?;
            spawned.push(follower);
        }
        Ok(spawned)
    }

    /// Spawn an additional follower for a shard (a fresh node registering into a live
    /// cluster and catching up from replication).
    pub fn add_follower(&self, shard_id: u32) -> io::Result<String> {
        let seq = self.follower_seq.fetch_add(1, Ordering::SeqCst);
        let node_id = format!("shard{shard_id}-f{seq}");
        self.spawn_shard_node(&node_id, shard_id, "follower")?;
        Ok(node_id)
    }

    /// SIGKILL a managed process. Returns false if we don't manage that node id.
    pub fn kill(&self, node_id: &str) -> bool {
        let mut children = self.children.lock().unwrap();
        match children.remove(node_id) {
            Some(mut child) => {
                let _ = child.kill();
                let _ = child.wait(); // reap the zombie
                true
            }
            None => false,
        }
    }

    /// Node ids we currently manage (i.e. that have a live child process).
    pub fn managed(&self) -> Vec<String> {
        self.children.lock().unwrap().keys().cloned().collect()
    }

    /// Kill everything (shutdown / ctrl-c).
    pub fn kill_all(&self) {
        let mut children = self.children.lock().unwrap();
        for (_, mut child) in children.drain() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        self.kill_all();
    }
}
