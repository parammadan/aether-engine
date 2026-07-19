//! The raft network client: openraft calls these methods to reach its peers; we carry its
//! serde-encoded messages over the `RaftTransport` gRPC service. The server returns the
//! remote raft's `Result` verbatim (also serde-encoded), so remote raft errors surface as
//! `RemoteError` and transport failures as `Unreachable`/`NetworkError` — the distinctions
//! openraft's replication logic keys off.

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use common::pb::raft_transport_client::RaftTransportClient;
use common::pb::RaftPayload;

use crate::{NodeId, TypeConfig};

/// Creates a network client per peer; the peer's address comes from its `BasicNode`
/// membership entry, so transport addressing lives in the raft membership itself.
#[derive(Default, Clone)]
pub struct GrpcRaftNetworkFactory;

impl RaftNetworkFactory<TypeConfig> for GrpcRaftNetworkFactory {
    type Network = GrpcRaftClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        GrpcRaftClient { target, addr: node.addr.clone() }
    }
}

pub struct GrpcRaftClient {
    target: NodeId,
    addr: String,
}

impl GrpcRaftClient {
    /// One round trip: serde-encode the request, call the given RPC, decode the remote
    /// raft's own Result. `E` is the remote raft error type for that RPC.
    async fn round_trip<Req, Resp, E>(
        &mut self,
        which: &str,
        req: Req,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, E>>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
        E: std::error::Error + serde::de::DeserializeOwned,
    {
        // Connect per call; tonic reconnect/pooling is an optimization for later.
        let mut client = RaftTransportClient::connect(format!("http://{}", self.addr))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let payload = RaftPayload {
            data: serde_json::to_vec(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?,
        };
        let response = match which {
            "append" => client.append_entries(payload).await,
            "vote" => client.vote(payload).await,
            _ => client.install_snapshot(payload).await,
        }
        .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let result: Result<Resp, E> = serde_json::from_slice(&response.into_inner().data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

impl RaftNetwork<TypeConfig> for GrpcRaftClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.round_trip("append", rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.round_trip("snapshot", rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.round_trip("vote", rpc).await
    }
}
