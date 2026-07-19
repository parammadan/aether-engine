//! Replication: a shard leader forwards the documents it indexes to its follower(s), so a
//! follower holds the same slice of data and can be promoted to serve it if the leader dies
//! (primary-backup). The leader is the client; followers serve the `Replication` service.

use std::sync::{Arc, RwLock};

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::replication_client::ReplicationClient;
use common::pb::replication_server::Replication;
use common::pb::{FlightDocument, ListReplicasRequest, ReplicateRequest, ReplicateResponse};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinSet;
use tonic::{Request, Response, Status};

use crate::store::ShardStore;

/// Follower-side service: applies replicated documents into the local store. Documents are
/// re-embedded locally with the shared deterministic embedder, so the follower's vector
/// index converges to the leader's without shipping vectors on the wire.
pub struct ReplicationService {
    store: Arc<RwLock<ShardStore>>,
}

impl ReplicationService {
    pub fn new(store: Arc<RwLock<ShardStore>>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl Replication for ReplicationService {
    async fn replicate(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        let documents = request.into_inner().documents;
        let applied = documents.len() as u32;
        {
            let mut store = self
                .store
                .write()
                .map_err(|_| Status::internal("store lock poisoned"))?;
            for doc in documents {
                store.insert(doc);
            }
        }
        Ok(Response::new(ReplicateResponse { applied }))
    }
}

/// Push one batch to every follower concurrently. Best-effort: an unreachable follower is
/// logged and skipped — the leader keeps serving, and the follower catches up on the next
/// batch. (Async, eventually-consistent replication; strong consistency is a later concern.)
pub async fn replicate_to_followers(
    followers: Vec<String>,
    shard_id: u32,
    documents: Vec<FlightDocument>,
) {
    if followers.is_empty() || documents.is_empty() {
        return;
    }
    let mut set = JoinSet::new();
    for addr in followers {
        let request = ReplicateRequest { documents: documents.clone(), shard_id };
        set.spawn(async move {
            match common::net::channel(&addr).await.map(ReplicationClient::new) {
                Ok(mut client) => {
                    if let Err(e) = client.replicate(request).await {
                        eprintln!("replication: follower {addr} rejected batch: {e}");
                    }
                }
                Err(e) => eprintln!("replication: could not reach follower {addr}: {e}"),
            }
        });
    }
    while set.join_next().await.is_some() {}
}

/// Leader-side replication loop: drain indexed batches, discover this shard's followers from
/// the coordinator (so replica addresses are never hardcoded), and replicate to them. Runs
/// until the batch channel closes.
pub async fn run_replication(
    coordinator_addr: String,
    shard_id: u32,
    mut batches: Receiver<Vec<FlightDocument>>,
) {
    while let Some(documents) = batches.recv().await {
        let followers = discover_followers(&coordinator_addr, shard_id).await;
        replicate_to_followers(followers, shard_id, documents).await;
    }
}

/// Ask the coordinator which followers serve this shard.
async fn discover_followers(coordinator_addr: &str, shard_id: u32) -> Vec<String> {
    let Ok(mut client) = common::net::channel(coordinator_addr)
        .await
        .map(CoordinatorClient::new)
    else {
        return Vec::new();
    };
    match client.list_replicas(ListReplicasRequest { shard_id }).await {
        Ok(resp) => resp.into_inner().addresses,
        Err(e) => {
            eprintln!("replication: list_replicas failed: {e}");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(icao24: &str, callsign: &str) -> FlightDocument {
        FlightDocument {
            icao24: icao24.to_string(),
            callsign: callsign.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn replicate_applies_documents_to_the_index() {
        let index = Arc::new(RwLock::new(ShardStore::new()));
        let service = ReplicationService::new(index.clone());

        let resp = service
            .replicate(Request::new(ReplicateRequest {
                documents: vec![doc("a1", "UAL1"), doc("b2", "DAL2")],
                shard_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.applied, 2);
        let idx = index.read().unwrap();
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.search("ual1", 10).total_matched, 1);
    }
}
