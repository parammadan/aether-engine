//! Ingestion: pull flight observations from a source and feed them into the index.
//!
//! # Shape
//! A **pull-based** producer/consumer with backpressure:
//!   - a *producer* polls a [`FlightSource`] on an interval for a snapshot of documents;
//!   - it hands each batch to a *consumer* over a **bounded** channel;
//!   - the consumer takes the index write lock and inserts the batch.
//!
//! Backpressure is structural: if the consumer (indexing) falls behind, the bounded channel
//! fills and the producer's `send().await` blocks — so we stop pulling new snapshots until
//! the index catches up, instead of growing an unbounded in-memory queue. Pull (not push)
//! means the source can't outrun us; we fetch only when ready.
//!
//! The [`FlightSource`] trait keeps the loop testable with a fake in-memory source — no live
//! network needed for unit tests. [`OpenSkySource`] is the real implementation.

use std::num::NonZeroU32;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use common::pb::FlightDocument;
use common::shard::shard_for;

use crate::store::ShardStore;

pub type IngestError = Box<dyn std::error::Error + Send + Sync>;

/// A fixed `hash % N` slice: this node indexes a document only if
/// `hash(icao24) % count == index`.
#[derive(Clone, Copy, Debug)]
pub struct ShardAssignment {
    pub index: u32,
    pub count: NonZeroU32,
}

impl ShardAssignment {
    /// Does this shard own the given aircraft? Uses the same `shard_for` the coordinator and
    /// every other node use, so ownership is consistent cluster-wide.
    pub fn owns(&self, icao24: &str) -> bool {
        shard_for(icao24, self.count) == self.index
    }
}

/// What this node keeps from the ingest stream.
#[derive(Clone)]
pub enum Ownership {
    /// Everything (single-node).
    All,
    /// Fixed `hash % N` placement.
    Modulo(ShardAssignment),
    /// Virtual-shard placement: keep a document when the coordinator's table maps its
    /// virtual shard to this node's group. The table is a live view (refreshed by a
    /// background poller), so reassigning a virtual shard moves ingestion between groups
    /// within one refresh — no restarts, and the modulus never changes.
    Mapped {
        group: u32,
        assignments: Arc<RwLock<Vec<u32>>>,
    },
}

impl Ownership {
    /// The live vshard table when this node runs virtual-shard placement (so the search
    /// path can name each hit's owning vshard); `None` under `hash % N` or single-node.
    pub fn assignments(&self) -> Option<Arc<RwLock<Vec<u32>>>> {
        match self {
            Ownership::Mapped { assignments, .. } => Some(assignments.clone()),
            _ => None,
        }
    }

    pub fn owns(&self, icao24: &str) -> bool {
        match self {
            Ownership::All => true,
            Ownership::Modulo(assignment) => assignment.owns(icao24),
            Ownership::Mapped { group, assignments } => {
                let table = assignments.read().expect("assignments lock poisoned");
                // Before the first fetch (empty table) own nothing: briefly ingesting
                // nothing is safe; ingesting the wrong slice is not.
                let Some(v) = NonZeroU32::new(table.len() as u32) else { return false };
                let vshard = common::shard::vshard_for(icao24, v);
                table.get(vshard as usize).copied() == Some(*group)
            }
        }
    }
}

/// Keep a live copy of the coordinator's virtual-shard table (used by
/// [`Ownership::Mapped`]). Polls every couple of seconds, asking whichever coordinator
/// answers first; a fetch failure keeps the last known table (stale placement beats no
/// placement).
pub async fn run_vshard_view(
    coordinators: crate::cluster::Coordinators,
    assignments: Arc<RwLock<Vec<u32>>>,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(2));
    loop {
        ticker.tick().await;
        let Some(mut client) = coordinators.first_healthy().await else {
            continue;
        };
        if let Ok(resp) = client
            .get_v_shard_assignments(common::pb::VShardAssignmentsRequest {})
            .await
        {
            let table = resp.into_inner().group_of;
            *assignments.write().expect("assignments lock poisoned") = table;
        }
    }
}

/// A source of flight observations. One `fetch` returns one snapshot (a batch of documents).
#[async_trait]
pub trait FlightSource: Send + Sync {
    async fn fetch(&self) -> Result<Vec<FlightDocument>, IngestError>;
}

// Allow choosing a source at runtime without making every loop generic-caller care.
#[async_trait]
impl FlightSource for Box<dyn FlightSource> {
    async fn fetch(&self) -> Result<Vec<FlightDocument>, IngestError> {
        self.as_ref().fetch().await
    }
}

/// A deterministic, network-free source: every poll fabricates a fresh batch of distinct
/// aircraft (all with origin "Synthetica", so one term matches everything it has ever
/// produced). For offline demos and chaos tests where the ingestion PIPELINE is the thing
/// under test, not the upstream feed. `seed` namespaces ids so two producers (e.g. an old
/// and a newly elected leader) can never collide.
pub struct SyntheticSource {
    seed: u32,
    batch_size: usize,
    seq: std::sync::atomic::AtomicU32,
}

impl SyntheticSource {
    pub fn new(seed: u32, batch_size: usize) -> Self {
        Self { seed, batch_size, seq: std::sync::atomic::AtomicU32::new(0) }
    }
}

#[async_trait]
impl FlightSource for SyntheticSource {
    async fn fetch(&self) -> Result<Vec<FlightDocument>, IngestError> {
        // Origin stays the constant "Synthetica" token (everything downstream searches for
        // it), but aircraft type and the geo/numeric fields get a deterministic-but-varied
        // spread so the aggregation-backed panels (geo-density map, altitude/velocity
        // histograms, percentiles) show a real distribution instead of a pile at (0,0).
        // Derived from the sequence number via a cheap hash, so a run is reproducible.
        let crafts = ["TestJet", "MockLiner", "StubProp", "FixtureWing"];
        let mut docs = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            let s = self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let h = common::shard::fnv1a_64(&s.to_le_bytes());
            let f = |shift: u32, span: f64, base: f64| ((h >> shift) % 1000) as f64 / 1000.0 * span + base;
            docs.push(FlightDocument {
                icao24: format!("{:04x}{:06x}", self.seed, s),
                callsign: format!("SYN{s}"),
                origin: "Synthetica".to_string(),
                aircraft_type: crafts[((h >> 8) % crafts.len() as u64) as usize].to_string(),
                latitude: f(16, 140.0, -60.0),   // -60..80
                longitude: f(26, 360.0, -180.0), // -180..180
                altitude: f(36, 12000.0, 0.0),   // 0..12000 m
                velocity: f(44, 300.0, 0.0),     // 0..300 m/s
                observed_at: s as i64,
                ..Default::default()
            });
        }
        Ok(docs)
    }
}

/// A SECURITY-EVENT source — the SIEM generality proof. Security events have the same shape
/// the engine already handles (a high-cardinality entity key, a relentless stream, hybrid
/// queries over structured + text fields), so they map straight onto the existing document
/// schema with ZERO new distributed machinery. The field mapping (documented so queries
/// read sensibly):
///   icao24        = unique EVENT id             — THE SHARD KEY (see the learning below)
///   origin        = source IP (the entity)      — value-counts = noisiest source; pivot key
///   destination   = target host
///   aircraft_type = event category              — value-counts = detection breakdown
///   callsign      = actor / user
///   altitude      = severity 0..127             — histograms / percentiles / filters
///   on_ground     = resolved flag
///
/// Design note: the engine's key is an UPSERT key — right for aircraft, where one
/// icao24 is one evolving entity re-observed over time. Security events are the opposite —
/// immutable per-event records where one entity (a source IP) emits MANY events — so the
/// document key must be the EVENT id (keying by entity would collapse the stream to one
/// row per IP). The entity becomes a high-cardinality FIELD that aggregations pivot on, and
/// correlation is a filtered value-counts. Still zero new distributed machinery.
pub struct SecuritySource {
    seed: u32,
    batch_size: usize,
    seq: std::sync::atomic::AtomicU32,
}

impl SecuritySource {
    pub fn new(seed: u32, batch_size: usize) -> Self {
        Self { seed, batch_size, seq: std::sync::atomic::AtomicU32::new(0) }
    }
}

#[async_trait]
impl FlightSource for SecuritySource {
    async fn fetch(&self) -> Result<Vec<FlightDocument>, IngestError> {
        // A small pool of actors/targets so co-occurrence aggregations find real structure
        // (a few noisy source IPs, a handful of event categories).
        let categories = ["auth_fail", "port_scan", "malware", "exfil", "privilege_escalation"];
        let actors = ["alice", "bob", "carol", "dave", "svc-account"];
        let sources = ["10.0.0.5", "10.0.0.9", "192.168.1.20", "172.16.0.4"];
        let mut docs = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            let s = self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let h = common::shard::fnv1a_64(&s.to_le_bytes());
            let pick = |shift: u32, n: usize| ((h >> shift) % n as u64) as usize;
            docs.push(FlightDocument {
                icao24: format!("evt-{:04x}{:06x}", self.seed, s), // unique event id = shard/doc key
                origin: sources[pick(4, sources.len())].to_string(), // source IP = the entity to pivot on
                destination: format!("host-{}", pick(20, 8)),
                aircraft_type: categories[pick(28, categories.len())].to_string(), // event category
                callsign: actors[pick(12, actors.len())].to_string(),              // actor / user
                altitude: (h >> 36 & 0x7f) as f64,                                 // severity 0..127
                observed_at: s as i64,
                ..Default::default()
            });
        }
        Ok(docs)
    }
}

/// A GENERIC HTTP/REST connector — the first "ingests from anywhere" source. Polls a URL
/// returning JSON (an array of objects, or a single object) and maps each record onto a
/// GENERIC document: string values become keyword-searchable `text` fields, numeric values
/// become aggregatable `number` fields, and the id comes from a configured field (else the
/// record index). Nothing is disguised as a flight — the record's own field names are kept,
/// and every downstream capability (search, filter, aggregate, provenance) addresses them.
///
/// Env: `AETHER_HTTP_URL` (required), `AETHER_HTTP_ID_FIELD` (default "id"),
/// `AETHER_HTTP_ID_PREFIX` (namespaces ids so this connector's docs never collide with
/// another source's; default "http").
pub struct HttpSource {
    client: reqwest::Client,
    url: String,
    id_field: String,
    id_prefix: String,
    /// Dot path to the records array (e.g. "features" for GeoJSON, "data" for many APIs).
    /// Empty = the response is itself the array (or a single object).
    records_path: Vec<String>,
}

impl HttpSource {
    pub fn from_env() -> Self {
        let path = std::env::var("AETHER_HTTP_RECORDS_PATH").unwrap_or_default();
        Self {
            client: reqwest::Client::new(),
            url: std::env::var("AETHER_HTTP_URL").unwrap_or_default(),
            id_field: std::env::var("AETHER_HTTP_ID_FIELD").unwrap_or_else(|_| "id".to_string()),
            id_prefix: std::env::var("AETHER_HTTP_ID_PREFIX").unwrap_or_else(|_| "http".to_string()),
            records_path: path.split('.').filter(|s| !s.is_empty()).map(String::from).collect(),
        }
    }
}

/// Recursively flatten a JSON object into a generic document: strings → `text`, numbers →
/// `number`, bools → `text`. Nested objects are flattened with DOTTED field names
/// (`properties.mag`, `geometry.type`) so keys at different levels never collide — real
/// feeds like GeoJSON carry a `type` at three levels, and a bare leaf key would clobber
/// them. A `coordinates` array `[lon, lat, ...]` (GeoJSON) sets the document's geo.
fn flatten_into(prefix: &str, obj: &serde_json::Map<String, serde_json::Value>, doc: &mut FlightDocument) {
    for (k, val) in obj {
        let key = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        match val {
            serde_json::Value::String(s) => {
                doc.text.insert(key, s.clone());
            }
            serde_json::Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    doc.number.insert(key, f);
                }
            }
            serde_json::Value::Bool(b) => {
                doc.text.insert(key, b.to_string());
            }
            serde_json::Value::Object(inner) => flatten_into(&key, inner, doc),
            serde_json::Value::Array(a) if k == "coordinates" => {
                let nums: Vec<f64> = a.iter().filter_map(|x| x.as_f64()).collect();
                if nums.len() >= 2 {
                    doc.longitude = nums[0];
                    doc.latitude = nums[1];
                }
            }
            _ => {} // other arrays / null skipped
        }
    }
}

/// Map one JSON record onto a generic document (recursively flattened). The id is
/// `<prefix>-<id_field value>` or `<prefix>-<index>`.
fn record_to_doc(rec: &serde_json::Value, id_field: &str, id_prefix: &str, idx: usize) -> FlightDocument {
    let mut doc = FlightDocument::default();
    let id = rec
        .get(id_field)
        .map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .unwrap_or_else(|| idx.to_string());
    doc.icao24 = format!("{id_prefix}-{id}");
    if let Some(obj) = rec.as_object() {
        flatten_into("", obj, &mut doc);
    }
    doc
}

#[async_trait]
impl FlightSource for HttpSource {
    async fn fetch(&self) -> Result<Vec<FlightDocument>, IngestError> {
        if self.url.is_empty() {
            return Err("AETHER_HTTP_URL is not set".into());
        }
        let resp = self.client.get(&self.url).send().await?.error_for_status()?;
        let v: serde_json::Value = resp.json().await?;
        Ok(json_to_docs(&v, &self.id_field, &self.id_prefix, &self.records_path))
    }
}

/// Parse a JSON value into generic documents: navigate `records_path` to the array (empty =
/// top level), then map each element (a single object yields one document).
fn json_to_docs(
    v: &serde_json::Value,
    id_field: &str,
    id_prefix: &str,
    records_path: &[String],
) -> Vec<FlightDocument> {
    let mut cur = v;
    for seg in records_path {
        match cur.get(seg) {
            Some(next) => cur = next,
            None => return vec![],
        }
    }
    let records: Vec<&serde_json::Value> = match cur {
        serde_json::Value::Array(a) => a.iter().collect(),
        obj @ serde_json::Value::Object(_) => vec![obj],
        _ => vec![],
    };
    records
        .iter()
        .enumerate()
        .map(|(i, rec)| record_to_doc(rec, id_field, id_prefix, i))
        .collect()
}

/// An S3 ingest connector — index JSON objects from an S3 bucket (a data-lake source). Lists
/// objects under a prefix and maps each object's JSON records onto generic fields, exactly
/// like the HTTP connector. Works against real S3 or any S3-compatible store (MinIO,
/// LocalStack) via `AETHER_S3_ENDPOINT` — so it's testable with no real AWS account.
///
/// Env: `AETHER_S3_INGEST_BUCKET` (required), `AETHER_S3_INGEST_PREFIX` (default ""),
/// `AETHER_S3_ENDPOINT` (optional, for MinIO/LocalStack), `AETHER_HTTP_ID_FIELD` (default
/// "id"), plus the standard AWS region/credentials env.
pub struct S3Source {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    id_field: String,
}

impl S3Source {
    pub async fn from_env() -> Self {
        let bucket = std::env::var("AETHER_S3_INGEST_BUCKET").unwrap_or_default();
        let prefix = std::env::var("AETHER_S3_INGEST_PREFIX").unwrap_or_default();
        let id_field = std::env::var("AETHER_HTTP_ID_FIELD").unwrap_or_else(|_| "id".to_string());
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region));
        let endpoint = std::env::var("AETHER_S3_ENDPOINT").ok();
        if let Some(ep) = &endpoint {
            loader = loader.endpoint_url(ep);
        }
        let shared = loader.load().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&shared);
        if endpoint.is_some() {
            builder = builder.force_path_style(true); // MinIO/LocalStack serve by path
        }
        Self { client: aws_sdk_s3::Client::from_conf(builder.build()), bucket, prefix, id_field }
    }
}

#[async_trait]
impl FlightSource for S3Source {
    async fn fetch(&self) -> Result<Vec<FlightDocument>, IngestError> {
        if self.bucket.is_empty() {
            return Err("AETHER_S3_INGEST_BUCKET is not set".into());
        }
        let listing = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .send()
            .await
            .map_err(|e| format!("s3 list: {e}"))?;

        let mut docs = Vec::new();
        for obj in listing.contents() {
            let Some(key) = obj.key() else { continue };
            let got = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| format!("s3 get {key}: {e}"))?;
            let bytes = got.body.collect().await.map_err(|e| format!("s3 body {key}: {e}"))?.into_bytes();
            let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                continue; // skip non-JSON objects
            };
            // Namespace ids by the object key so records from different objects never collide.
            let id_prefix = format!("s3:{key}");
            docs.extend(json_to_docs(&v, &self.id_field, &id_prefix, &[]));
        }
        Ok(docs)
    }
}

/// Run the ingestion loop: poll `source` every `poll_interval`, inserting each snapshot into
/// `index`, keeping only the documents this node owns per `ownership` (everything, a fixed
/// `hash % N` slice, or a live virtual-shard mapping).
/// When `replicate_tx` is `Some`, the documents actually indexed from each batch are also
/// forwarded on that channel (a leader replicates them to its followers).
/// Runs until `max_polls` is reached (`None` = forever, the production case); `max_polls`
/// exists so tests can drive a deterministic, finite run.
pub async fn run_ingestion<S: FlightSource + 'static>(
    source: S,
    index: Arc<RwLock<ShardStore>>,
    poll_interval: Duration,
    max_polls: Option<usize>,
    ownership: Ownership,
    replicate_tx: Option<tokio::sync::mpsc::Sender<Vec<FlightDocument>>>,
) {
    // Small bounded buffer: a few snapshots of slack, then backpressure kicks in.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<FlightDocument>>(4);

    // Consumer: drain batches into the index. A slow consumer fills the channel and throttles
    // the producer — that is the backpressure.
    let consumer_index = index.clone();
    let consumer = tokio::spawn(async move {
        while let Some(batch) = rx.recv().await {
            // Collect the documents we actually index (for replication), then insert them.
            let mut indexed = Vec::new();
            {
                let mut idx = consumer_index.write().expect("index lock poisoned");
                for doc in batch {
                    // Keep only documents this node owns.
                    if ownership.owns(&doc.icao24) {
                        if replicate_tx.is_some() {
                            indexed.push(doc.clone());
                        }
                        idx.insert(doc);
                    }
                }
            } // release the write lock before any await

            if let Some(tx) = &replicate_tx {
                // Forward what we indexed to the replication task. If it has gone away, keep
                // ingesting — replication is best-effort and must not stall the data plane.
                let _ = tx.send(indexed).await;
            }
        }
    });

    // Producer: pull snapshots on an interval.
    let mut polls = 0usize;
    loop {
        if let Some(limit) = max_polls {
            if polls >= limit {
                break;
            }
        }
        match source.fetch().await {
            Ok(batch) if !batch.is_empty() => {
                // Blocks here if the consumer is behind (bounded channel) — backpressure.
                if tx.send(batch).await.is_err() {
                    break; // consumer gone
                }
            }
            Ok(_) => {} // empty snapshot, nothing to do
            Err(e) => eprintln!("ingest: fetch failed: {e}"),
        }
        polls += 1;
        if max_polls.is_none_or(|limit| polls < limit) {
            tokio::time::sleep(poll_interval).await;
        }
    }

    drop(tx); // let the consumer finish draining, then exit
    let _ = consumer.await;
}

// ===========================================================================
// OpenSky source
// ===========================================================================

const OPENSKY_STATES_URL: &str = "https://opensky-network.org/api/states/all";

/// Pulls live state vectors from the OpenSky Network REST API. Anonymous access works but is
/// rate-limited; credentials (from env) raise the limit.
pub struct OpenSkySource {
    client: reqwest::Client,
    url: String,
    auth: Option<(String, String)>,
}

impl OpenSkySource {
    /// Build a source, reading optional `OPENSKY_USERNAME` / `OPENSKY_PASSWORD` from env.
    pub fn from_env() -> Self {
        let auth = match (std::env::var("OPENSKY_USERNAME"), std::env::var("OPENSKY_PASSWORD")) {
            (Ok(u), Ok(p)) if !u.is_empty() && !p.is_empty() => Some((u, p)),
            _ => None,
        };
        Self {
            client: reqwest::Client::new(),
            url: OPENSKY_STATES_URL.to_string(),
            auth,
        }
    }
}

/// The `/states/all` response: a timestamp plus an array of heterogeneous state vectors.
/// Each state vector is a positional array of mixed types, so we parse the elements as
/// untyped JSON values and index into them by position in [`map_state`].
#[derive(Deserialize)]
struct OpenSkyResponse {
    states: Option<Vec<Vec<serde_json::Value>>>,
}

#[async_trait]
impl FlightSource for OpenSkySource {
    async fn fetch(&self) -> Result<Vec<FlightDocument>, IngestError> {
        let mut req = self.client.get(&self.url);
        if let Some((user, pass)) = &self.auth {
            req = req.basic_auth(user, Some(pass));
        }
        let resp = req.send().await?.error_for_status()?;
        let body: OpenSkyResponse = resp.json().await?;
        let states = body.states.unwrap_or_default();
        Ok(states.iter().filter_map(|s| map_state(s)).collect())
    }
}

/// Map one OpenSky state vector (positional array) to a `FlightDocument`.
/// Field order per the OpenSky API: 0 icao24, 1 callsign, 2 origin_country, 3 time_position,
/// 5 longitude, 6 latitude, 7 baro_altitude, 8 on_ground, 9 velocity, 10 true_track,
/// 11 vertical_rate. Missing/null fields fall back to defaults; a state with no `icao24`
/// is skipped (returns `None`) since it can't be sharded.
fn map_state(s: &[serde_json::Value]) -> Option<FlightDocument> {
    let icao24 = s.first()?.as_str()?.trim().to_string();
    if icao24.is_empty() {
        return None;
    }
    let str_at = |i: usize| s.get(i).and_then(|v| v.as_str()).unwrap_or("").trim().to_string();
    let f64_at = |i: usize| s.get(i).and_then(|v| v.as_f64()).unwrap_or(0.0);

    Some(FlightDocument {
        icao24,
        callsign: str_at(1),
        observed_at: s.get(3).and_then(|v| v.as_i64()).unwrap_or(0) * 1000, // seconds -> millis
        latitude: f64_at(6),
        longitude: f64_at(5),
        altitude: f64_at(7),
        origin: str_at(2), // origin_country (states/all has no route origin/destination)
        destination: String::new(),
        aircraft_type: String::new(),
        velocity: f64_at(9),
        heading: f64_at(10),
        vertical_rate: f64_at(11),
        on_ground: s.get(8).and_then(|v| v.as_bool()).unwrap_or(false),
        tenant_id: String::new(),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn http_connector_maps_records_onto_generic_fields() {
        // A non-flight record maps onto GENERIC fields by its own names: strings → text,
        // numbers → number, id namespaced — nothing disguised as a flight.
        let rec = json!({
            "id": "T1",
            "subject": "cannot login",
            "priority": "high",
            "response_time_ms": 320,
            "resolved": false,
            "nested": {"ignored": 1}
        });
        let doc = record_to_doc(&rec, "id", "http", 0);
        assert_eq!(doc.icao24, "http-T1", "id is namespaced by the connector prefix");
        assert_eq!(doc.text.get("subject").map(String::as_str), Some("cannot login"));
        assert_eq!(doc.text.get("priority").map(String::as_str), Some("high"));
        assert_eq!(doc.number.get("response_time_ms").copied(), Some(320.0));
        assert_eq!(doc.text.get("resolved").map(String::as_str), Some("false"), "bool → text");
        assert!(!doc.text.contains_key("nested"), "nested objects are skipped (flat model)");
        // The flight-specific columns stay empty — this is a generic document.
        assert!(doc.origin.is_empty() && doc.callsign.is_empty());
    }

    #[test]
    fn json_to_docs_handles_array_and_single_object() {
        // Both the HTTP and S3 connectors funnel through json_to_docs. An array yields one
        // doc per element; a lone object yields one doc; anything else yields none.
        let arr = json!([{"id": "a", "name": "x"}, {"id": "b", "name": "y"}]);
        assert_eq!(json_to_docs(&arr, "id", "http", &[]).len(), 2);
        let obj = json!({"id": "solo", "name": "z"});
        let docs = json_to_docs(&obj, "id", "s3:file.json", &[]);
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].icao24, "s3:file.json-solo", "object-key-namespaced id");
        assert!(json_to_docs(&json!(42), "id", "http", &[]).is_empty(), "scalars yield no docs");

        // GeoJSON-style nested feed via a records path — flatten + geo extraction.
        let geo = json!({
            "features": [{
                "id": "eq1",
                "properties": {"mag": 3.2, "place": "10km W of Anza, CA"},
                "geometry": {"coordinates": [-116.7, 33.5, 4.6]}
            }]
        });
        let docs = json_to_docs(&geo, "id", "quake", &["features".to_string()]);
        assert_eq!(docs.len(), 1, "records path navigates into 'features'");
        assert_eq!(docs[0].number.get("properties.mag").copied(), Some(3.2), "nested field, dotted");
        assert_eq!(docs[0].text.get("properties.place").map(String::as_str), Some("10km W of Anza, CA"));
        assert!((docs[0].latitude - 33.5).abs() < 1e-6, "geo lat from coordinates[1]");
        assert!((docs[0].longitude - -116.7).abs() < 1e-6, "geo lon from coordinates[0]");
    }

    #[test]
    fn map_state_maps_positional_fields() {
        // A representative OpenSky state vector.
        let state: Vec<serde_json::Value> = serde_json::from_value(json!([
            "abc123", "UAL231 ", "United States", 1_700_000_000i64, 1_700_000_001i64,
            -122.4, 37.6, 10000.0, false, 250.0, 90.0, 5.0
        ]))
        .unwrap();

        let doc = map_state(&state).expect("should map");
        assert_eq!(doc.icao24, "abc123");
        assert_eq!(doc.callsign, "UAL231"); // trimmed
        assert_eq!(doc.origin, "United States");
        assert_eq!(doc.observed_at, 1_700_000_000_000); // seconds -> millis
        assert_eq!(doc.longitude, -122.4);
        assert_eq!(doc.latitude, 37.6);
        assert_eq!(doc.altitude, 10000.0);
        assert!(!doc.on_ground);
        assert_eq!(doc.velocity, 250.0);
        assert_eq!(doc.heading, 90.0);
        assert_eq!(doc.vertical_rate, 5.0);
    }

    #[test]
    fn map_state_tolerates_nulls_and_skips_missing_icao24() {
        // Nulls in optional positions -> defaults; present icao24 still maps.
        let state: Vec<serde_json::Value> = serde_json::from_value(json!([
            "def456", null, null, null, null, null, null, null, null, null, null, null
        ]))
        .unwrap();
        let doc = map_state(&state).expect("should map with defaults");
        assert_eq!(doc.icao24, "def456");
        assert_eq!(doc.callsign, "");
        assert_eq!(doc.longitude, 0.0);

        // Null icao24 -> skipped.
        let no_id: Vec<serde_json::Value> = serde_json::from_value(json!([null, "X"])).unwrap();
        assert!(map_state(&no_id).is_none());
    }

    /// In-memory source for driving the loop without a network.
    struct FakeSource {
        batch: Vec<FlightDocument>,
    }

    #[async_trait]
    impl FlightSource for FakeSource {
        async fn fetch(&self) -> Result<Vec<FlightDocument>, IngestError> {
            Ok(self.batch.clone())
        }
    }

    fn doc(icao24: &str, callsign: &str) -> FlightDocument {
        FlightDocument {
            icao24: icao24.to_string(),
            callsign: callsign.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn ingestion_populates_index_from_source() {
        let index = Arc::new(RwLock::new(ShardStore::new()));
        let source = FakeSource {
            batch: vec![doc("a1", "UAL1"), doc("b2", "DAL2")],
        };

        // Two polls of the same 2-aircraft snapshot: re-observations upsert, so the store
        // holds 2 documents (one per aircraft), not 4.
        run_ingestion(source, index.clone(), Duration::from_millis(0), Some(2), Ownership::All, None).await;

        let idx = index.read().unwrap();
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.search("ual1", 10).total_matched, 1); // one hit per aircraft, no dupes
    }

    #[tokio::test]
    async fn ingestion_keeps_only_owned_shard() {
        // With N=2, a shard-0 node must index only documents whose hash(icao24) % 2 == 0.
        // Use enough ids that both shards are non-empty regardless of the exact hash split.
        let count = NonZeroU32::new(2).unwrap();
        let ids: Vec<String> = (0..20u32).map(|i| format!("{i:04x}")).collect();
        // callsign = id so each indexed doc is searchable by its id (the index covers text
        // fields, not icao24 itself).
        let batch: Vec<FlightDocument> = ids.iter().map(|id| doc(id, id)).collect();
        let expected_shard0 = ids.iter().filter(|id| shard_for(id, count) == 0).count();
        assert!(expected_shard0 > 0 && expected_shard0 < ids.len(), "need a non-trivial split");

        let index = Arc::new(RwLock::new(ShardStore::new()));
        let source = FakeSource { batch };
        let assignment = ShardAssignment { index: 0, count };
        run_ingestion(source, index.clone(), Duration::from_millis(0), Some(1), Ownership::Modulo(assignment), None).await;

        let idx = index.read().unwrap();
        assert_eq!(idx.len(), expected_shard0);
        // Every indexed doc really is owned by shard 0.
        for id in &ids {
            let want = shard_for(id, count) == 0;
            assert_eq!(idx.search(id, 10).total_matched, if want { 1 } else { 0 });
        }
    }

    #[tokio::test]
    async fn indexed_documents_are_forwarded_for_replication() {
        let index = Arc::new(RwLock::new(ShardStore::new()));
        let source = FakeSource { batch: vec![doc("a1", "UAL1"), doc("b2", "DAL2")] };

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<FlightDocument>>(8);
        let collector = tokio::spawn(async move {
            let mut all = Vec::new();
            while let Some(batch) = rx.recv().await {
                all.extend(batch);
            }
            all
        });

        run_ingestion(source, index, Duration::from_millis(0), Some(1), Ownership::All, Some(tx)).await;

        let forwarded = collector.await.unwrap();
        assert_eq!(forwarded.len(), 2); // both indexed docs were forwarded to replication
    }
}
