//! Fault injection for integration tests: a controllable TCP proxy.
//!
//! A partition on localhost has to be *manufactured* — every port is always reachable,
//! so tests put a [`Proxy`] in front of each direction of each peer link and cut traffic
//! by flipping a switch. The control is deterministic and immediate: `block()` aborts
//! every live splice and makes new connections fail on arrival; nothing in the proxy
//! sleeps, polls, or races the traffic it carries.
//!
//! Directionality: one proxy carries the connections ONE peer dials at another (requests
//! and their responses — application-level direction, not TCP-segment direction). A pair
//! of nodes A and B therefore gets two proxies, A→B and B→A, and blocking exactly one of
//! them is an asymmetric partition.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// One direction of one peer link: everything dialed at `listen_addr()` is spliced to
/// the target — until blocked.
pub struct Proxy {
    addr: String,
    blocked: Arc<AtomicBool>,
    splices: Arc<Mutex<Vec<JoinHandle<()>>>>,
    accept_loop: JoinHandle<()>,
}

impl Proxy {
    /// Bind an ephemeral port and splice every accepted connection to `target`.
    pub async fn spawn(target: String) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
        let addr = listener.local_addr().unwrap().to_string();
        let blocked = Arc::new(AtomicBool::new(false));
        let splices: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));

        let accept_blocked = blocked.clone();
        let accept_splices = splices.clone();
        let accept_loop = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else { return };
                if accept_blocked.load(Ordering::SeqCst) {
                    // The partition: the connection dies on arrival. (A deliberate close,
                    // not a black hole — deterministic failure beats waiting out timeouts
                    // in tests, and the caller's error path is the same NetworkError.)
                    drop(inbound);
                    continue;
                }
                let target = target.clone();
                let handle = tokio::spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(&target).await {
                        let _ = copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                let mut splices = accept_splices.lock().unwrap();
                splices.retain(|h| !h.is_finished());
                splices.push(handle);
            }
        });

        Proxy { addr, blocked, splices, accept_loop }
    }

    /// The address peers should dial instead of the real target.
    pub fn listen_addr(&self) -> &str {
        &self.addr
    }

    /// Cut this direction NOW: live connections are severed, new ones die on arrival.
    pub fn block(&self) {
        self.blocked.store(true, Ordering::SeqCst);
        for handle in self.splices.lock().unwrap().drain(..) {
            handle.abort();
        }
    }

    /// Restore this direction; the next dial goes through.
    pub fn unblock(&self) {
        self.blocked.store(false, Ordering::SeqCst);
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked.load(Ordering::SeqCst)
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept_loop.abort();
        for handle in self.splices.lock().unwrap().drain(..) {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// An echo server for the proxy to front.
    async fn echo_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { return };
                tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 || sock.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    async fn round_trip(addr: &str) -> std::io::Result<Vec<u8>> {
        let mut sock = TcpStream::connect(addr).await?;
        sock.write_all(b"ping").await?;
        let mut buf = vec![0u8; 4];
        sock.read_exact(&mut buf).await?;
        Ok(buf)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn splices_blocks_and_heals() {
        let target = echo_server().await;
        let proxy = Proxy::spawn(target).await;

        // Open: bytes flow through.
        assert_eq!(round_trip(proxy.listen_addr()).await.unwrap(), b"ping");

        // Blocked: a LIVE connection is severed mid-use...
        let mut live = TcpStream::connect(proxy.listen_addr()).await.unwrap();
        live.write_all(b"ping").await.unwrap();
        let mut buf = vec![0u8; 4];
        live.read_exact(&mut buf).await.unwrap();
        proxy.block();
        live.write_all(b"ping").await.ok(); // may buffer locally
        assert!(
            live.read_exact(&mut buf).await.is_err(),
            "a blocked proxy must sever live connections"
        );

        // ...and a NEW connection dies on arrival.
        assert!(
            round_trip(proxy.listen_addr()).await.is_err(),
            "a blocked proxy must refuse new traffic"
        );

        // Healed: traffic flows again.
        proxy.unblock();
        assert_eq!(round_trip(proxy.listen_addr()).await.unwrap(), b"ping");
    }
}
