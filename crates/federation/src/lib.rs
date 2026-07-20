//! Cross-cluster federation: fan one query out to several independent clusters and merge.
//!
//! The mechanics are pure reuse — a cluster's coordinator is treated as a "super-shard",
//! so federating is scatter-gather one level up over the SAME deadline, coverage, dedup,
//! and ranking the coordinator already applies to shards. The genuinely hard part is
//! SEMANTIC, not mechanical: clusters keep independent clocks, so the freshness envelope
//! across them is advisory (see the federated manifest note) and the embedder is a
//! cross-CLUSTER contract now, not just cross-node.

use std::time::Duration;

use common::pb::coordinator_client::CoordinatorClient;
use common::pb::{OmittedShard, QueryManifest, SearchRequest, SearchResponse};
use tokio::task::JoinSet;

/// Per-cluster deadline (`AETHER_FEDERATION_TIMEOUT_MS`, default 3000). One outer timeout
/// bounds a slow OR dead cluster identically — a partitioned cluster can't stall the
/// federated query, exactly as a slow shard can't stall a single-cluster one.
fn cluster_timeout() -> Duration {
    let ms = std::env::var("AETHER_FEDERATION_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);
    Duration::from_millis(ms)
}

/// One cluster's answer, tagged with which cluster it came from.
struct ClusterReply {
    cluster: String,
    resp: SearchResponse,
}

/// Federate a search across `clusters` (coordinator addresses). Returns a merged response
/// whose manifest reports per-cluster coverage and a cross-cluster freshness envelope.
pub async fn federate_search(clusters: Vec<String>, request: SearchRequest) -> SearchResponse {
    let started = std::time::Instant::now();
    let limit = request.limit as usize;
    let clusters_queried = clusters.len() as u32;

    let mut set = JoinSet::new();
    for cluster in clusters {
        let req = request.clone();
        set.spawn(async move {
            let attempt = tokio::time::timeout(cluster_timeout(), async {
                let mut c = common::net::channel(&cluster).await.ok().map(CoordinatorClient::new)?;
                c.search(req).await.ok().map(|r| r.into_inner())
            })
            .await;
            match attempt {
                Ok(Some(resp)) => Ok(ClusterReply { cluster, resp }),
                Ok(None) => Err(OmittedShard { address: cluster, reason: "unreachable".into() }),
                Err(_) => Err(OmittedShard { address: cluster, reason: "timeout".into() }),
            }
        });
    }

    let mut replies = Vec::new();
    let mut omitted = Vec::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok(r)) => replies.push(r),
            Ok(Err(o)) => omitted.push(o),
            Err(_) => {}
        }
    }

    // Reuse the coordinator's merge: each cluster's already-merged response is folded in as
    // if it were a shard response. Dedup-by-freshest handles the same aircraft appearing in
    // two clusters; ranking and limit are identical.
    let clusters_answered = replies.len() as u32;
    let per_cluster: Vec<(String, u64)> =
        replies.iter().map(|r| (r.cluster.clone(), r.resp.total_matched)).collect();
    let fanout = coordinator::fanout::Fanout {
        responses: replies.into_iter().map(|r| r.resp).collect(),
        omitted: omitted.clone(),
    };
    // placement_version is per-cluster and not comparable across clusters, so 0 at the
    // federation level; elapsed is measured here. Coverage below is re-stamped in cluster
    // terms.
    let mut merged = coordinator::fanout::merge_search_responses(fanout, limit, clusters_queried, 0, 0);

    // Re-stamp the manifest in CLUSTER terms: queried/answered count clusters, omitted names
    // the clusters that didn't answer, and the freshness envelope spans clusters (advisory —
    // independent clocks). The per-hit provenance from each shard is preserved untouched.
    let (freshest, stalest) = merged
        .manifest
        .as_ref()
        .map(|m| (m.freshest_observed_at, m.stalest_observed_at))
        .unwrap_or((0, 0));
    merged.shard_id = "federation".into();
    merged.shards_queried = clusters_queried;
    merged.shards_answered = clusters_answered;
    merged.manifest = Some(QueryManifest {
        shards_queried: clusters_queried,
        shards_answered: clusters_answered,
        omitted,
        deduped: merged.manifest.map(|m| m.deduped).unwrap_or(0),
        freshest_observed_at: freshest,
        stalest_observed_at: stalest,
        elapsed_ms: started.elapsed().as_millis() as u64,
        placement_version: 0,
    });
    let _ = per_cluster; // (per-cluster matched counts available for a richer manifest later)
    merged
}
