//! Headless dashboard test: spawn the real dashboard (which spawns a real local cluster),
//! and assert every panel's WebSocket/snapshot payload is well-formed and carries REAL
//! cluster data — then kill a node and prove the surface stays coherent and records it.
//! The panels are pure renders of this snapshot, so a well-formed snapshot IS the panels.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::Value;

struct Dash(Child);
impl Drop for Dash {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn state(addr: &str) -> Option<Value> {
    let r = reqwest::get(format!("http://{addr}/api/state")).await.ok()?;
    r.json::<Value>().await.ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_panel_payload_is_well_formed_and_survives_a_node_kill() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let dash = Dash(
        Command::new(env!("CARGO_BIN_EXE_dashboard"))
            .env("AETHER_DASHBOARD_ADDR", &addr)
            .env("AETHER_SOURCE", "synthetic")
            .env("AETHER_SHARD_COUNT", "2")
            .env("AETHER_POLL_SECS", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn dashboard"),
    );

    // Wait until the snapshot is fully populated: nodes registered, geo + altitude
    // aggregates non-empty, and the time-series accumulating.
    let mut snap = None;
    for _ in 0..120 {
        if let Some(s) = state(&addr).await {
            let agg = &s["aggregate"];
            let ready = s["nodes"].as_array().map(|a| !a.is_empty()).unwrap_or(false)
                && agg["geo_cells"].as_array().map(|a| !a.is_empty()).unwrap_or(false)
                && agg["altitude_pcts"].as_array().map(|a| !a.is_empty()).unwrap_or(false)
                && s["series"].as_array().map(|a| a.len() >= 2).unwrap_or(false);
            if ready {
                snap = Some(s);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let snap = snap.expect("dashboard never produced a fully-populated snapshot");

    // Contract version.
    assert_eq!(snap["v"], 1);

    // Geo cells carry real coordinates + counts (the map is real, not placeholder).
    let cells = snap["aggregate"]["geo_cells"].as_array().unwrap();
    assert!(cells.iter().all(|c| c["lat"].is_number() && c["lon"].is_number() && c["count"].as_u64().unwrap_or(0) > 0));

    // Altitude percentiles are ordered p50 <= p90 <= p99 and within the synthetic range.
    let pcts = snap["aggregate"]["altitude_pcts"].as_array().unwrap();
    let v = |i: usize| pcts[i]["value"].as_f64().unwrap();
    assert!(v(0) <= v(1) && v(1) <= v(2), "percentiles must be monotonic");
    assert!(v(2) <= 12001.0, "p99 within the 0..12000 synthetic altitude range");

    // The throughput/provenance panel: the live query answered from both shards.
    let last = &snap["query"]["last"];
    assert_eq!(last["ok"], true);
    assert!(last["provenance"]["summary"].as_str().unwrap_or("").contains("2/2"));

    // Kill a node; the surface must record it and keep serving (ok count keeps rising).
    let victim = snap["nodes"][0]["node_id"].as_str().unwrap().to_string();
    let ok_before = state(&addr).await.unwrap()["query"]["ok"].as_u64().unwrap();
    reqwest::Client::new()
        .post(format!("http://{addr}/api/kill/{victim}"))
        .send()
        .await
        .unwrap();

    let mut recorded = false;
    let mut kept_serving = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let Some(s) = state(&addr).await else { continue };
        let events = s["events"].as_array().unwrap();
        recorded |= events.iter().any(|e| e["msg"].as_str().unwrap_or("").contains(&victim));
        let ok_now = s["query"]["ok"].as_u64().unwrap();
        // Series stays well-formed throughout (each point has the expected fields).
        assert!(s["series"].as_array().unwrap().iter().all(|p| p["ms"].is_number() && p["errored"].is_boolean()));
        if recorded && ok_now > ok_before {
            kept_serving = true;
            break;
        }
    }
    assert!(recorded, "the kill was never recorded in the event log");
    assert!(kept_serving, "the cluster stopped serving queries after the kill");
    drop(dash);
}
