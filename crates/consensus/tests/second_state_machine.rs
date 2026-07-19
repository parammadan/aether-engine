//! The reusability proof: a raft group whose state machine is NOT a document store —
//! here, a tiny key-value map — running on the exact same plumbing (type config, log
//! store, generic state machine, snapshot persistence). If this compiles and passes,
//! a second group with its own application is a matter of writing one `StateMachineApp`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use consensus::network::GrpcRaftNetworkFactory;
use consensus::storage::{LogStore, StateMachineStore};
use consensus::{raft_config, Payload, StateMachineApp};
use openraft::BasicNode;

/// A key-value application: payloads are "key=value" strings, state is a map.
#[derive(Clone, Default)]
struct KvApp(Arc<Mutex<BTreeMap<String, String>>>);

impl StateMachineApp for KvApp {
    fn apply(&self, payload: &[u8]) -> u32 {
        let text = String::from_utf8_lossy(payload);
        let Some((k, v)) = text.split_once('=') else { return 0 };
        self.0.lock().unwrap().insert(k.to_string(), v.to_string());
        1
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        bincode::serialize(&*self.0.lock().unwrap()).expect("map serializes")
    }

    fn restore(&self, bytes: &[u8]) -> u32 {
        let map: BTreeMap<String, String> = bincode::deserialize(bytes).unwrap_or_default();
        let count = map.len() as u32;
        *self.0.lock().unwrap() = map;
        count
    }

    fn unit(&self) -> &'static str {
        "keys"
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_kv_state_machine_runs_on_the_shared_plumbing() {
    let dir = std::env::temp_dir().join(format!("aether-kv-sm-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let app = KvApp::default();
    let sm: StateMachineStore<KvApp> =
        StateMachineStore::with_snapshot_dir(app.clone(), dir.clone()).unwrap();
    let raft = consensus::Raft::new(
        1,
        Arc::new(raft_config().validate().unwrap()),
        GrpcRaftNetworkFactory,
        LogStore::default(),
        sm,
    )
    .await
    .unwrap();

    // Single-member group: this member IS the quorum, so writes commit immediately.
    raft.initialize(BTreeMap::from([(1u64, BasicNode::new("unused:0"))]))
        .await
        .unwrap();
    raft.wait(Some(std::time::Duration::from_secs(5)))
        .metrics(|m| m.current_leader == Some(1), "self-elected")
        .await
        .unwrap();

    for (k, v) in [("region", "us-east-2"), ("groups", "2"), ("region", "eu-west-1")] {
        raft.client_write(Payload(format!("{k}={v}").into_bytes())).await.unwrap();
    }

    {
        let map = app.0.lock().unwrap();
        assert_eq!(map.get("region").map(String::as_str), Some("eu-west-1"), "upsert semantics");
        assert_eq!(map.len(), 2, "two distinct keys");
    }

    // Snapshot through raft, then restore into a FRESH app from the persisted file —
    // the durable path is application-agnostic too.
    raft.trigger().snapshot().await.unwrap();
    raft.wait(Some(std::time::Duration::from_secs(5)))
        .metrics(|m| m.snapshot.is_some(), "snapshot built")
        .await
        .unwrap();
    raft.shutdown().await.unwrap();

    let fresh = KvApp::default();
    let _sm2: StateMachineStore<KvApp> =
        StateMachineStore::with_snapshot_dir(fresh.clone(), dir.clone()).unwrap();
    let map = fresh.0.lock().unwrap();
    assert_eq!(map.get("region").map(String::as_str), Some("eu-west-1"));
    assert_eq!(map.len(), 2, "snapshot restored the full map");

    let _ = std::fs::remove_dir_all(&dir);
}
