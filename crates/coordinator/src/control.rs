//! The coordinator state group: replicating operator intent across coordinator replicas.
//!
//! State is split by its true authority. The shard map is a **derived view** — heartbeats
//! and raft-leader reports rebuild it on every replica independently in seconds, so
//! replicating it would be consensus over stale gossip. What genuinely needs to survive a
//! coordinator's death is the **operator-intent state**: the vshard placement table and
//! the drain set. It's tiny, it changes rarely, and losing it loses real decisions — so
//! that, and only that, gets raft.
//!
//! The machinery is the shared `consensus` crate (same WAL, transport, and generic state
//! machine the shard groups run on); the application here applies placement/drain
//! commands to the same registry the serving path reads.
//!
//! Determinism rule: apply must depend only on replicated state, never on this replica's
//! local view. Validation that consults the view (does this node exist?) happens at the
//! PROPOSING coordinator, before the command enters the log.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

use crate::registry::Registry;

/// One replicated operator-intent command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    ReassignVShard { vshard: u32, group: u32 },
    DrainNode { node_id: String },
}

/// Snapshot form of the authoritative state.
#[derive(Serialize, Deserialize, Default)]
struct Authority {
    vshards: Vec<u32>,
    draining: Vec<String>,
}

/// The state-machine application: committed commands mutate the registry's authoritative
/// fields (the same registry queries read, so a committed reassignment routes immediately).
#[derive(Clone)]
pub struct ControlApp(pub Arc<RwLock<Registry>>);

impl consensus::StateMachineApp for ControlApp {
    fn apply(&self, payload: &[u8]) -> u32 {
        match bincode::deserialize::<Command>(payload) {
            Ok(Command::ReassignVShard { vshard, group }) => {
                // Validated at the proposer; the checks it re-runs here read only
                // replicated state (table length) and static config (N), so every
                // replica reaches the same verdict.
                let _ = self.0.write().unwrap().reassign_vshard(vshard, group);
                1
            }
            Ok(Command::DrainNode { node_id }) => {
                self.0.write().unwrap().apply_drain_marker(&node_id);
                1
            }
            Err(e) => {
                eprintln!("control: undecodable command skipped: {e}");
                0
            }
        }
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        let (vshards, draining) = self.0.read().unwrap().authority();
        bincode::serialize(&Authority { vshards, draining }).expect("authority serializes")
    }

    fn restore(&self, bytes: &[u8]) -> u32 {
        let auth: Authority = bincode::deserialize(bytes).unwrap_or_default();
        let count = (auth.vshards.len() + auth.draining.len()) as u32;
        self.0.write().unwrap().set_authority(auth.vshards, auth.draining);
        count
    }

    fn unit(&self) -> &'static str {
        "authority records"
    }
}

/// Why a proposal couldn't be committed HERE — either it belongs on the leader (forward
/// the whole RPC there) or the group can't take writes right now.
pub enum ProposeError {
    /// Another replica leads; its coordinator gRPC address (from raft membership).
    Leader(String),
    Unavailable(String),
}

/// This coordinator's membership in the state group.
pub struct ControlPlane {
    pub raft: consensus::Raft,
    my_id: u64,
}

impl ControlPlane {
    /// Build from env: `AETHER_CONTROL_ID` (this replica's id) and `AETHER_CONTROL_PEERS`
    /// ("1=host:port,2=host:port,..." — every replica's coordinator gRPC address, where
    /// the raft transport is also served). Both unset ⇒ single-coordinator mode, no group.
    ///
    /// The peer list is static config, deliberately: coordinators are the discovery
    /// service, so they cannot discover each other through themselves.
    pub async fn from_env(
        registry: Arc<RwLock<Registry>>,
    ) -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let (Ok(id), Ok(peers_raw)) = (
            std::env::var("AETHER_CONTROL_ID"),
            std::env::var("AETHER_CONTROL_PEERS"),
        ) else {
            return Ok(None);
        };
        let my_id: u64 = id.parse()?;
        let mut peers: BTreeMap<u64, BasicNode> = BTreeMap::new();
        for part in peers_raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (id, addr) = part
                .split_once('=')
                .ok_or_else(|| format!("bad AETHER_CONTROL_PEERS entry: {part}"))?;
            peers.insert(id.trim().parse()?, BasicNode::new(addr.trim()));
        }
        if !peers.contains_key(&my_id) {
            return Err(format!("AETHER_CONTROL_ID {my_id} is not in AETHER_CONTROL_PEERS").into());
        }

        let app = ControlApp(registry);
        let config = Arc::new(consensus::raft_config().validate()?);
        let raft = match std::env::var("AETHER_DATA_DIR").ok() {
            // Durable: same WAL + snapshot-dir machinery the shard groups use.
            Some(dir) => {
                let dir = std::path::PathBuf::from(dir);
                let log_store = consensus::wal::WalLogStore::open(&dir)?;
                let sm = consensus::storage::StateMachineStore::<ControlApp>::with_snapshot_dir(
                    app,
                    dir.join("snapshots"),
                )?;
                consensus::Raft::new(
                    my_id,
                    config,
                    consensus::network::GrpcRaftNetworkFactory,
                    log_store,
                    sm,
                )
                .await?
            }
            None => {
                consensus::Raft::new(
                    my_id,
                    config,
                    consensus::network::GrpcRaftNetworkFactory,
                    consensus::storage::LogStore::default(),
                    consensus::storage::StateMachineStore::<ControlApp>::new(app),
                )
                .await?
            }
        };

        // Deterministic bootstrap over a static set: the smallest id initializes; an
        // "already initialized" refusal means a peer (or a previous life) got there first.
        if peers.keys().min() == Some(&my_id) {
            let boot_raft = raft.clone();
            let members = peers.clone();
            tokio::spawn(async move {
                match boot_raft.initialize(members).await {
                    Ok(()) => println!("control: initialized coordinator state group"),
                    Err(e) => println!("control: initialize skipped ({e})"),
                }
            });
        }

        println!("control: replica {my_id} of a {}-coordinator state group", peers.len());
        Ok(Some(Self { raft, my_id }))
    }

    /// Commit one command through the group. `Err(Leader(addr))` means this replica is a
    /// follower and the caller should forward the original RPC to `addr`.
    pub async fn propose(&self, cmd: &Command) -> Result<(), ProposeError> {
        let payload = bincode::serialize(cmd).expect("command serializes");
        match self.raft.client_write(consensus::Payload(payload)).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let Some(forward) = e.forward_to_leader() {
                    if let Some(node) = &forward.leader_node {
                        // Not us: hand the caller the leader's address.
                        if forward.leader_id != Some(self.my_id) {
                            return Err(ProposeError::Leader(node.addr.clone()));
                        }
                    }
                }
                Err(ProposeError::Unavailable(format!("control group cannot commit: {e}")))
            }
        }
    }
}
