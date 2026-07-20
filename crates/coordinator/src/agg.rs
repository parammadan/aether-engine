//! Aggregation scatter-gather: fan an `Aggregate` out to every shard leader, then MERGE
//! the partials. Merging is closed over `AggregatePartial` (same type in, same type out),
//! which is the whole design — counts and bucket maps merge by summing, the t-digest merges
//! by centroid union. Reuses the search fan-out's deadline and omission machinery so a slow
//! or dead shard degrades coverage, never correctness.

use common::pb::shard_search_client::ShardSearchClient;
use common::pb::{AggKind, AggregatePartial, AggregateRequest, Percentile, TDigest};
use common::tdigest;
use tokio::task::JoinSet;

use crate::fanout::{omitted_shard, shard_timeout};

/// Query every leader's `Aggregate` concurrently under the fan-out deadline, collecting the
/// partials that answer and naming the shards that don't.
pub async fn scatter_aggregate(
    leaders: Vec<String>,
    request: AggregateRequest,
) -> (Vec<AggregatePartial>, Vec<common::pb::OmittedShard>) {
    let mut set = JoinSet::new();
    for addr in leaders {
        let req = request.clone();
        set.spawn(async move {
            let attempt = tokio::time::timeout(shard_timeout(), async {
                let mut client = common::net::channel(&addr).await.ok().map(ShardSearchClient::new)?;
                client.aggregate(req).await.ok().map(|r| r.into_inner())
            })
            .await;
            match attempt {
                Ok(Some(resp)) => Ok(resp.partial),
                Ok(None) => Err(omitted_shard(addr, "unreachable")),
                Err(_) => Err(omitted_shard(addr, "timeout")),
            }
        });
    }
    let mut partials = Vec::new();
    let mut omitted = Vec::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok(Some(p))) => partials.push(p),
            Ok(Ok(None)) => {}                 // shard answered with no partial (shouldn't happen)
            Ok(Err(o)) => omitted.push(o),
            Err(_) => {}                       // task panic — treat as absent
        }
    }
    (partials, omitted)
}

/// Merge shard partials into one, and resolve the requested percentiles from the merged
/// digest. `Fanout`-style coverage is passed through the manifest by the caller.
pub fn merge_partials(
    kind: AggKind,
    partials: Vec<AggregatePartial>,
    requested_percentiles: &[f64],
) -> (AggregatePartial, Vec<Percentile>) {
    let mut count = 0u64;
    let mut buckets: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let mut digest = tdigest::TDigest::new();

    for p in partials {
        count += p.count;
        for (k, v) in p.buckets {
            *buckets.entry(k).or_insert(0) += v; // per-bucket sum — associative
        }
        if let Some(d) = p.digest {
            let shard = tdigest::TDigest::from_parts(&d.centroid_mean, &d.centroid_weight);
            digest.merge(&shard); // centroid union — associative
        }
    }

    let percentiles = if kind == AggKind::AggPercentiles && !digest.is_empty() {
        requested_percentiles
            .iter()
            .map(|&p| Percentile { p, value: digest.quantile(p / 100.0) })
            .collect()
    } else {
        Vec::new()
    };

    let (centroid_mean, centroid_weight) = digest.to_parts();
    let digest_msg = (kind == AggKind::AggPercentiles).then_some(TDigest { centroid_mean, centroid_weight });

    (
        AggregatePartial { kind: kind as i32, count, buckets, digest: digest_msg },
        percentiles,
    )
}
