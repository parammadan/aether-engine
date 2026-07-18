//! Server side of the raft transport: hands each incoming consensus message to this node's
//! raft instance and returns the raft's own `Result`, serde-encoded, so the caller can
//! distinguish a remote raft error from a transport failure.

use tonic::{Request, Response, Status};

use common::pb::raft_transport_server::RaftTransport;
use common::pb::RaftPayload;

use super::{Raft, TypeConfig};

pub struct RaftTransportService {
    raft: Raft,
}

impl RaftTransportService {
    pub fn new(raft: Raft) -> Self {
        Self { raft }
    }
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Response<RaftPayload>, Status> {
    let data = serde_json::to_vec(value).map_err(|e| Status::internal(e.to_string()))?;
    Ok(Response::new(RaftPayload { data }))
}

fn decode<T: serde::de::DeserializeOwned>(payload: &RaftPayload) -> Result<T, Status> {
    serde_json::from_slice(&payload.data).map_err(|e| Status::invalid_argument(e.to_string()))
}

#[tonic::async_trait]
impl RaftTransport for RaftTransportService {
    async fn append_entries(
        &self,
        request: Request<RaftPayload>,
    ) -> Result<Response<RaftPayload>, Status> {
        let rpc: openraft::raft::AppendEntriesRequest<TypeConfig> = decode(&request.into_inner())?;
        let result = self.raft.append_entries(rpc).await;
        encode(&result)
    }

    async fn vote(&self, request: Request<RaftPayload>) -> Result<Response<RaftPayload>, Status> {
        let rpc: openraft::raft::VoteRequest<super::NodeId> = decode(&request.into_inner())?;
        let result = self.raft.vote(rpc).await;
        encode(&result)
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftPayload>,
    ) -> Result<Response<RaftPayload>, Status> {
        let rpc: openraft::raft::InstallSnapshotRequest<TypeConfig> = decode(&request.into_inner())?;
        let result = self.raft.install_snapshot(rpc).await;
        encode(&result)
    }
}
