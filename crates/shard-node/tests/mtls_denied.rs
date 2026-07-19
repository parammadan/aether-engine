//! mTLS negative paths — the actual feature. A server that requires client certificates
//! must REFUSE a caller who presents none, and refuse one whose cert is signed by a
//! different CA. If either connects, the "mutual" in mutual TLS is a lie.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::ClusterStateRequest;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::Request;

fn gen_certs(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("aether-mtls-neg-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/gen-certs.sh");
    let status = Command::new("bash")
        .arg(script)
        .arg(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run gen-certs.sh");
    assert!(status.success());
    dir
}

fn read(dir: &PathBuf, name: &str) -> Vec<u8> {
    std::fs::read(dir.join(name)).unwrap()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// The coordinator binary, requiring mTLS against `dir`'s CA.
fn spawn_tls_coordinator(dir: &PathBuf, port: u16) -> Child {
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

/// Wait until the server's TLS port accepts a PROPERLY-authenticated client — proves it
/// is up before we test that it rejects improper ones.
async fn wait_ready(good: &PathBuf, addr: &str) {
    for _ in 0..80 {
        if good_channel(good, addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("authed coordinator never came up");
}

async fn good_channel(dir: &PathBuf, addr: &str) -> Result<Channel, tonic::transport::Error> {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(read(dir, "ca.crt")))
        .identity(Identity::from_pem(read(dir, "operator.crt"), read(dir, "operator.key")))
        .domain_name("localhost");
    Channel::from_shared(format!("https://{addr}")).unwrap().tls_config(tls).unwrap().connect().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_without_a_cert_and_a_wrong_ca_client_are_both_refused() {
    common::net::install_crypto();
    let good = gen_certs("good");
    let evil = gen_certs("evil"); // a DIFFERENT CA entirely
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let mut child = spawn_tls_coordinator(&good, port);

    wait_ready(&good, &addr).await;

    // 1) No client identity: server-side ca root demands one → handshake/RPC fails.
    let no_cert = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(read(&good, "ca.crt")))
        .domain_name("localhost");
    let result = async {
        let ch = Channel::from_shared(format!("https://{addr}"))
            .unwrap()
            .tls_config(no_cert)
            .unwrap()
            .connect()
            .await?;
        CoordinatorClient::new(ch)
            .get_cluster_state(Request::new(ClusterStateRequest {}))
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }
    .await;
    assert!(result.is_err(), "a client presenting NO certificate must be refused");

    // 2) Wrong CA: a valid cert, but signed by a CA this server doesn't trust.
    let wrong_ca = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(read(&good, "ca.crt")))
        .identity(Identity::from_pem(read(&evil, "operator.crt"), read(&evil, "operator.key")))
        .domain_name("localhost");
    let result = async {
        let ch = Channel::from_shared(format!("https://{addr}"))
            .unwrap()
            .tls_config(wrong_ca)
            .unwrap()
            .connect()
            .await?;
        CoordinatorClient::new(ch)
            .get_cluster_state(Request::new(ClusterStateRequest {}))
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }
    .await;
    assert!(result.is_err(), "a client whose cert is from a foreign CA must be refused");

    // Control: the properly-authenticated client still works, so we rejected the bad
    // ones for the RIGHT reason and didn't just break the server.
    let mut good_client = CoordinatorClient::new(good_channel(&good, &addr).await.unwrap());
    assert!(good_client
        .get_cluster_state(Request::new(ClusterStateRequest {}))
        .await
        .is_ok());

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&good);
    let _ = std::fs::remove_dir_all(&evil);
}
