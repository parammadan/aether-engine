//! Backpressure under an ingestion flood: a source that produces as fast as it is polled
//! must NOT be able to grow memory unboundedly when indexing stalls. The bounded channel is
//! the mechanism — once it fills, the producer's `send().await` parks, so it stops fetching.
//!
//! The test stalls the consumer deterministically by holding the store's write lock (that is
//! what a slow indexer looks like to the loop), floods with a zero-interval source that
//! counts its fetches, and asserts the fetch count plateaus at ~(channel bound + in-flight)
//! instead of tracking the flood — then releases the stall and asserts ingestion resumes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use common::pb::FlightDocument;
use shard_node::ingest::{run_ingestion, FlightSource, IngestError};
use shard_node::store::ShardStore;

/// Produces a fresh 1-document batch instantly on every poll and counts the polls.
struct FloodSource {
    fetches: Arc<AtomicUsize>,
}

#[async_trait]
impl FlightSource for FloodSource {
    async fn fetch(&self) -> Result<Vec<FlightDocument>, IngestError> {
        let n = self.fetches.fetch_add(1, Ordering::SeqCst);
        Ok(vec![FlightDocument {
            icao24: format!("{n:06x}"), // distinct aircraft per poll, so progress is visible
            callsign: format!("FL{n}"),
            ..Default::default()
        }])
    }
}

// Multi-threaded runtime: the stalled consumer legitimately blocks a worker thread on the
// store's std write lock; the producer and the test body need other threads to run on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_throttles_when_indexing_stalls_and_resumes_after() {
    let store = Arc::new(RwLock::new(ShardStore::new()));
    let fetches = Arc::new(AtomicUsize::new(0));

    // Stall indexing BEFORE the flood starts: the consumer will park on this lock.
    let stall = store.write().unwrap();

    let ingestion = tokio::spawn(run_ingestion(
        FloodSource { fetches: fetches.clone() },
        store.clone(),
        Duration::from_millis(0), // poll as fast as allowed — the flood
        None,                     // run forever; the test aborts it at the end
        None,
        None,
    ));

    // Let the flood run against the stalled consumer, then measure twice.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let plateau_first = fetches.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let plateau_second = fetches.load(Ordering::SeqCst);

    // Throttled: with a channel bound of 4, the producer can be at most a few batches ahead
    // (1 taken by the consumer + 4 buffered + 1 fetched-and-awaiting-send). A flood without
    // backpressure would have fetched thousands of times in 400ms at zero interval.
    assert!(
        plateau_second <= 8,
        "producer kept fetching while indexing was stalled: {plateau_second} fetches"
    );
    // And it has actually stopped, not merely slowed: no progress between the two readings.
    assert!(
        plateau_second - plateau_first <= 1,
        "fetch count still growing under stall: {plateau_first} -> {plateau_second}"
    );

    // Clear the stall: the pipeline must drain and resume pulling.
    drop(stall);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let resumed = fetches.load(Ordering::SeqCst);
    assert!(
        resumed > plateau_second + 10,
        "ingestion did not resume after the stall cleared: {plateau_second} -> {resumed}"
    );
    assert!(
        store.read().unwrap().len() > 10,
        "documents were not indexed after the stall cleared"
    );

    ingestion.abort();
}
