//! mTLS end to end: a raft group whose EVERY hop — registration, heartbeats, the raft
//! transport that carries elections and log replication, and client fan-out — runs over
//! mutual TLS. If the group forms, elects, ingests, and answers a routed query with all
//! of that encrypted and client-authenticated, the security layer is real, not decorative.
//!
//! The negative path (no cert / wrong CA refused) is asserted in `mtls_denied.rs`; kept
//! separate so a handshake bug can't make the happy path silently pass by falling back.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::{ClusterStateRequest, NodeRole, SearchRequest};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

struct Cluster {
    children: HashMap<String, Child>,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for (_, c) in self.children.iter_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Generate a cert set into a scratch dir via the repo script; return the dir.
fn gen_certs() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("aether-mtls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/gen-certs.sh");
    let status = Command::new("bash")
        .arg(script)
        .arg(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run gen-certs.sh");
    assert!(status.success(), "cert generation failed");
    dir
}

fn read(dir: &PathBuf, name: &str) -> Vec<u8> {
    std::fs::read(dir.join(name)).unwrap()
}

/// The coordinator BINARY, over mTLS on a fixed port. Spawned as a child (not in-process)
/// on purpose: the coordinator's own fan-out to the shards reads AETHER_TLS_DIR from its
/// environment, so it must run in a process that has that env — an in-process coordinator
/// would inherit the TEST's env and dial the TLS shards in plaintext.
fn spawn_tls_coordinator(dir: &PathBuf, port: u16) -> Child {
    // The coordinator binary is a sibling of this test binary under target/<profile>/;
    // CARGO_BIN_EXE_coordinator isn't exported to a crate that doesn't define it.
    let exe = std::env::current_exe().unwrap();
    let coordinator = exe.parent().unwrap().parent().unwrap().join("coordinator");
    Command::new(coordinator)
        .env("AETHER_COORDINATOR_ADDR", format!("127.0.0.1:{port}"))
        .env("AETHER_SHARD_COUNT", "1")
        .env("AETHER_TLS_DIR", dir)
        .env("AETHER_TLS_ROLE", "coordinator")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn coordinator")
}

/// An operator-identity mTLS client channel to `addr`, retrying while the coordinator
/// child comes up.
async fn operator_channel(dir: &PathBuf, addr: &str) -> Channel {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(read(dir, "ca.crt")))
        .identity(Identity::from_pem(read(dir, "operator.crt"), read(dir, "operator.key")))
        .domain_name("localhost");
    for _ in 0..80 {
        let attempt = Channel::from_shared(format!("https://{addr}"))
            .unwrap()
            .tls_config(tls.clone())
            .unwrap()
            .connect()
            .await;
        if let Ok(ch) = attempt {
            return ch;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("operator mTLS connect never succeeded");
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_group_forms_and_serves_entirely_over_mtls() {
    common::net::install_crypto();
    let dir = gen_certs();
    let coord_port = free_port();
    let coord_addr = format!("127.0.0.1:{coord_port}");

    let mut children = HashMap::new();
    children.insert("coordinator".to_string(), spawn_tls_coordinator(&dir, coord_port));
    for i in 0..3 {
        let child = Command::new(env!("CARGO_BIN_EXE_shard-node"))
            .env("AETHER_NODE_ID", format!("m{i}"))
            .env("AETHER_SHARD_ADDR", format!("127.0.0.1:{}", free_port()))
            .env("AETHER_SHARD_INDEX", "0")
            .env("AETHER_SHARD_COUNT", "1")
            .env("AETHER_CONSENSUS", "raft")
            .env("AETHER_GROUP_SIZE", "3")
            .env("AETHER_COORDINATOR_ADDR", &coord_addr)
            .env("AETHER_HEARTBEAT_SECS", "1")
            .env("AETHER_SOURCE", "synthetic")
            .env("AETHER_POLL_SECS", "1")
            // The whole cluster runs TLS: members serve mTLS and dial peers/coordinator
            // with their member identity.
            .env("AETHER_TLS_DIR", &dir)
            .env("AETHER_TLS_ROLE", "member")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn member");
        children.insert(format!("m{i}"), child);
    }
    let _cluster = Cluster { children };

    // Over mTLS: a leader must be elected (proves the raft transport handshakes) and the
    // group must be committing (proves log replication rides the TLS transport).
    let mut client = CoordinatorClient::new(operator_channel(&dir, &coord_addr).await);
    let mut leader = None;
    for _ in 0..120 {
        let state = client.get_cluster_state(ClusterStateRequest {}).await.unwrap().into_inner();
        if let Some(l) = state.nodes.iter().find(|n| n.role == NodeRole::Leader as i32) {
            let resp = client
                .search(SearchRequest { query: "synthetica".into(), limit: 1 })
                .await
                .unwrap()
                .into_inner();
            if resp.total_matched > 10 {
                leader = Some(l.node_id.clone());
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(leader.is_some(), "no committing leader emerged over mTLS");

    // A routed query returns real hits, all of it TLS from client to shard and back.
    let resp = client
        .search(SearchRequest { query: "synthetica".into(), limit: 3 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.shards_answered, 1);
    assert!(!resp.hits.is_empty(), "the encrypted query path returned no hits");

    let _ = std::fs::remove_dir_all(&dir);
}
