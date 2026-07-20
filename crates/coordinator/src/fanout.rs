//! Scatter-gather: fan a query out to every shard leader concurrently, then merge the
//! per-shard responses into one globally-ranked result.
//!
//! Split into a pure [`merge_search_responses`] (unit-testable without a network) and an
//! async [`scatter_gather`] that does the concurrent RPCs.

use std::cmp::Ordering;
use std::sync::OnceLock;
use std::time::Duration;

use common::pb::shard_search_client::ShardSearchClient;
use common::pb::{SearchHit, SearchRequest, SearchResponse, SearchUpdate};
use tokio::task::JoinSet;

/// Per-shard fan-out deadline (`AETHER_SHARD_TIMEOUT_MS`, default 2000ms). One outer
/// timeout bounds connect + request together, so a shard that is *slow* — hung, GC-ing,
/// half-partitioned — is bounded exactly like one that is dead: without this, a query's
/// tail latency is the sickest node's latency. Read once; the deadline is process config.
pub fn shard_timeout() -> Duration {
    static CELL: OnceLock<Duration> = OnceLock::new();
    *CELL.get_or_init(|| {
        let ms = std::env::var("AETHER_SHARD_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2000);
        Duration::from_millis(ms)
    })
}

/// Query one shard leader under the fan-out deadline. Both failure modes collapse into
/// the same partial-coverage result (`None`), but the log line distinguishes them —
/// `reason=timeout` (up, but slow) vs `reason=unreachable` (down or erroring) — because
/// an operator debugging coverage drops needs to know which problem they have.
pub(crate) async fn query_leader(
    addr: String,
    req: SearchRequest,
) -> Result<SearchResponse, common::pb::OmittedShard> {
    let attempt = tokio::time::timeout(shard_timeout(), async {
        let mut client = common::net::channel(&addr).await.ok().map(ShardSearchClient::new)?;
        client.search(req).await.ok().map(|r| r.into_inner())
    })
    .await;
    match attempt {
        Ok(Some(resp)) => Ok(resp),
        Ok(None) => {
            eprintln!("fanout: shard omitted addr={addr} reason=unreachable");
            Err(omitted(addr, "unreachable"))
        }
        Err(_) => {
            eprintln!("fanout: shard omitted addr={addr} reason=timeout");
            Err(omitted(addr, "timeout"))
        }
    }
}

fn omitted(address: String, reason: &str) -> common::pb::OmittedShard {
    common::pb::OmittedShard { address, reason: reason.to_string() }
}

/// The outcome of a fan-out: the shards that answered, and the ones that didn't (with why).
/// Carrying the omissions — not just a count — is what lets the query manifest say exactly
/// which shard was dropped and whether it timed out or was unreachable.
pub struct Fanout {
    pub responses: Vec<SearchResponse>,
    pub omitted: Vec<common::pb::OmittedShard>,
}

/// Global ranking of hits: score descending, ties broken by `icao24` for a stable order.
fn cmp_hits(a: &SearchHit, b: &SearchHit) -> Ordering {
    b.score
        .partial_cmp(&a.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| icao24_of(a).cmp(icao24_of(b)))
}

/// Merge per-shard responses into one response, ranked by score.
///
/// - Hits from all shards are concatenated and sorted by score descending (ties broken by
///   `icao24` for a deterministic order), then truncated to `limit` (`0` = no limit).
/// - `total_matched` sums the per-shard totals, so it reflects matches across the whole
///   cluster before `limit`.
/// - Coverage: `shards_queried` is how many leaders we asked; `shards_answered` is how many
///   of these responses we got. `answered < queried` ⇒ partial results (a shard was down).
pub fn merge_search_responses(
    fanout: Fanout,
    limit: usize,
    shards_queried: u32,
    placement_version: u64,
    elapsed_ms: u64,
) -> SearchResponse {
    let Fanout { responses, omitted } = fanout;
    let shards_answered = responses.len() as u32;

    let mut hits = Vec::new();
    let mut total_matched = 0u64;
    for mut resp in responses {
        total_matched += resp.total_matched;
        hits.append(&mut resp.hits);
    }

    let before_dedup = hits.len();
    let mut hits = dedup_by_aircraft(hits);
    let deduped = (before_dedup - hits.len()) as u32;
    hits.sort_by(cmp_hits);

    if limit != 0 && hits.len() > limit {
        hits.truncate(limit);
    }

    let manifest = build_manifest(
        &hits,
        shards_queried,
        shards_answered,
        omitted,
        deduped,
        placement_version,
        elapsed_ms,
    );

    SearchResponse {
        hits,
        total_matched,
        shard_id: "coordinator".to_string(),
        shards_queried,
        shards_answered,
        manifest: Some(manifest),
    }
}

/// Assemble the per-query provenance manifest. The freshness envelope is computed over the
/// hits actually returned (post-truncation), so it describes the result the caller holds.
pub(crate) fn build_manifest(
    hits: &[SearchHit],
    shards_queried: u32,
    shards_answered: u32,
    omitted: Vec<common::pb::OmittedShard>,
    deduped: u32,
    placement_version: u64,
    elapsed_ms: u64,
) -> common::pb::QueryManifest {
    let observed: Vec<i64> = hits
        .iter()
        .filter_map(|h| h.provenance.as_ref().map(|p| p.observed_at))
        .filter(|&t| t > 0)
        .collect();
    common::pb::QueryManifest {
        shards_queried,
        shards_answered,
        omitted,
        deduped,
        freshest_observed_at: observed.iter().copied().max().unwrap_or(0),
        stalest_observed_at: observed.iter().copied().min().unwrap_or(0),
        elapsed_ms,
        placement_version,
    }
}

fn icao24_of(hit: &SearchHit) -> &str {
    hit.document.as_ref().map(|d| d.icao24.as_str()).unwrap_or("")
}

/// Does `a` supersede `b` for the same aircraft? Freshest observation wins (higher
/// `observed_at`); ties go to the higher score.
fn supersedes(a: &SearchHit, b: &SearchHit) -> bool {
    let ta = a.document.as_ref().map(|d| d.observed_at).unwrap_or(0);
    let tb = b.document.as_ref().map(|d| d.observed_at).unwrap_or(0);
    ta > tb || (ta == tb && a.score > b.score)
}

/// Collapse duplicate aircraft across shard responses, keeping the freshest observation.
/// Shards normally hold disjoint slices, so this is a no-op — duplicates appear only while
/// a virtual shard's ownership moves between groups and both briefly hold copies. Hits are
/// deduplicated; `total_matched` stays a per-shard sum and may transiently double-count the
/// overlap (an exact cluster-wide count would require exchanging full id sets).
fn dedup_by_aircraft(hits: Vec<SearchHit>) -> Vec<SearchHit> {
    let mut best: std::collections::HashMap<String, SearchHit> = std::collections::HashMap::new();
    for hit in hits {
        let key = icao24_of(&hit).to_string();
        match best.get(&key) {
            Some(existing) if !supersedes(&hit, existing) => {}
            _ => {
                best.insert(key, hit);
            }
        }
    }
    best.into_values().collect()
}

/// Incrementally merges shard responses as they stream in, maintaining the best-so-far
/// top-`limit` hits. Each `add` folds in one shard; `snapshot` produces the current
/// `SearchUpdate` for the client. Maintaining top-k incrementally is valid: a hit outside the
/// current top-k can never re-enter as more shards arrive.
pub struct ProgressiveMerge {
    limit: usize,
    hits: Vec<SearchHit>,
    total_matched: u64,
    shards_answered: u32,
    shards_queried: u32,
    omitted: Vec<common::pb::OmittedShard>,
    deduped: u32,
    placement_version: u64,
    started: std::time::Instant,
}

impl ProgressiveMerge {
    pub fn new(shards_queried: u32, limit: usize, placement_version: u64) -> Self {
        Self {
            limit,
            hits: Vec::new(),
            total_matched: 0,
            shards_answered: 0,
            shards_queried,
            omitted: Vec::new(),
            deduped: 0,
            placement_version,
            started: std::time::Instant::now(),
        }
    }

    /// Record a shard that did not answer (with why), for the final manifest.
    pub fn omit(&mut self, omitted: common::pb::OmittedShard) {
        self.omitted.push(omitted);
    }

    /// Fold one shard's response into the running result.
    pub fn add(&mut self, resp: SearchResponse) {
        self.shards_answered += 1;
        self.total_matched += resp.total_matched;
        self.hits.extend(resp.hits);
        let before = self.hits.len();
        self.hits = dedup_by_aircraft(std::mem::take(&mut self.hits));
        self.deduped += (before - self.hits.len()) as u32;
        self.hits.sort_by(cmp_hits);
        if self.limit != 0 && self.hits.len() > self.limit {
            self.hits.truncate(self.limit);
        }
    }

    /// The current progressive result. `complete` marks the final update — and only then
    /// is the manifest attached, since coverage/dedup/omissions aren't final until every
    /// shard has reported or timed out.
    pub fn snapshot(&self, complete: bool) -> SearchUpdate {
        let manifest = complete.then(|| {
            build_manifest(
                &self.hits,
                self.shards_queried,
                self.shards_answered,
                self.omitted.clone(),
                self.deduped,
                self.placement_version,
                self.started.elapsed().as_millis() as u64,
            )
        });
        SearchUpdate {
            hits: self.hits.clone(),
            total_matched: self.total_matched,
            shards_answered: self.shards_answered,
            shards_queried: self.shards_queried,
            complete,
            manifest,
        }
    }
}

/// Vector variant of [`scatter_gather`]: fan an already-embedded query vector to every
/// leader's `VectorSearch`. Same availability posture — unreachable shards are omitted and
/// reported as coverage.
pub async fn scatter_gather_vector(
    leaders: Vec<String>,
    request: common::pb::VectorSearchRequest,
) -> Fanout {
    let mut set = JoinSet::new();
    for addr in leaders {
        let req = request.clone();
        set.spawn(async move {
            let attempt = tokio::time::timeout(shard_timeout(), async {
                let mut client =
                    common::net::channel(&addr).await.ok().map(ShardSearchClient::new)?;
                client.vector_search(req).await.ok().map(|r| r.into_inner())
            })
            .await;
            match attempt {
                Ok(Some(resp)) => Ok(resp),
                Ok(None) => {
                    eprintln!("fanout: shard omitted addr={addr} reason=unreachable");
                    Err(omitted(addr, "unreachable"))
                }
                Err(_) => {
                    eprintln!("fanout: shard omitted addr={addr} reason=timeout");
                    Err(omitted(addr, "timeout"))
                }
            }
        });
    }
    let mut fanout = Fanout { responses: Vec::new(), omitted: Vec::new() };
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok(resp)) => fanout.responses.push(resp),
            Ok(Err(o)) => fanout.omitted.push(o),
            Err(_) => {} // task join error (panic) — treat as absent
        }
    }
    fanout
}

/// Query every leader address concurrently and collect the responses that succeed. A leader
/// that can't be reached or errors is simply omitted — the caller reports coverage so the
/// result is *partial*, not a failure of the whole query.
pub async fn scatter_gather(leaders: Vec<String>, request: SearchRequest) -> Fanout {
    let mut set = JoinSet::new();

    for addr in leaders {
        let req = request.clone();
        set.spawn(query_leader(addr, req));
    }

    let mut fanout = Fanout { responses: Vec::new(), omitted: Vec::new() };
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok(resp)) => fanout.responses.push(resp),
            Ok(Err(o)) => fanout.omitted.push(o),
            Err(_) => {} // task join error (panic) — treat as absent
        }
    }
    fanout
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::pb::{FlightDocument, SearchHit};

    fn hit(icao24: &str, score: f64) -> SearchHit {
        SearchHit {
            document: Some(FlightDocument {
                icao24: icao24.to_string(),
                ..Default::default()
            }),
            score,
            provenance: None,
        }
    }

    fn shard_response(shard_id: &str, total: u64, hits: Vec<SearchHit>) -> SearchResponse {
        SearchResponse {
            hits,
            total_matched: total,
            shard_id: shard_id.to_string(),
            shards_queried: 0,
            shards_answered: 0,
            manifest: None,
        }
    }

    #[test]
    fn merges_and_ranks_across_shards() {
        let r0 = shard_response("shard-0", 2, vec![hit("aaa", 1.0), hit("bbb", 3.0)]);
        let r1 = shard_response("shard-1", 1, vec![hit("ccc", 2.0)]);

        let merged = merge_search_responses(Fanout { responses: vec![r0, r1], omitted: vec![] }, 10, 2, 0, 0);

        assert_eq!(merged.total_matched, 3); // 2 + 1 across shards
        assert_eq!(merged.shards_queried, 2);
        assert_eq!(merged.shards_answered, 2);
        // Globally ranked by score desc: bbb(3) > ccc(2) > aaa(1).
        let order: Vec<&str> = merged
            .hits
            .iter()
            .map(|h| h.document.as_ref().unwrap().icao24.as_str())
            .collect();
        assert_eq!(order, vec!["bbb", "ccc", "aaa"]);
    }

    #[test]
    fn duplicate_aircraft_across_shards_collapse_to_the_freshest_observation() {
        // During a virtual-shard move, both groups briefly hold the same aircraft. The old
        // group has a stale observation; the new group has a fresh one.
        let mut stale = hit("aaa", 9.0);
        stale.document.as_mut().unwrap().observed_at = 100;
        let mut fresh = hit("aaa", 2.0);
        fresh.document.as_mut().unwrap().observed_at = 200;

        let r0 = shard_response("group-0", 1, vec![stale]);
        let r1 = shard_response("group-1", 2, vec![fresh, hit("bbb", 1.0)]);

        let merged = merge_search_responses(Fanout { responses: vec![r0, r1], omitted: vec![] }, 10, 2, 0, 0);

        // One hit per aircraft, and it's the FRESH one (higher observed_at beats score).
        assert_eq!(merged.hits.len(), 2);
        let aaa = merged
            .hits
            .iter()
            .find(|h| h.document.as_ref().unwrap().icao24 == "aaa")
            .unwrap();
        assert_eq!(aaa.document.as_ref().unwrap().observed_at, 200);
    }

    #[test]
    fn progressive_merge_also_dedups_across_shards() {
        let mut merge = ProgressiveMerge::new(2, 10, 0);
        let mut stale = hit("aaa", 9.0);
        stale.document.as_mut().unwrap().observed_at = 100;
        let mut fresh = hit("aaa", 2.0);
        fresh.document.as_mut().unwrap().observed_at = 200;

        merge.add(shard_response("group-0", 1, vec![stale]));
        merge.add(shard_response("group-1", 1, vec![fresh]));

        let snap = merge.snapshot(true);
        assert_eq!(snap.hits.len(), 1);
        assert_eq!(snap.hits[0].document.as_ref().unwrap().observed_at, 200);
    }

    #[test]
    fn limit_applies_to_merged_list_but_total_is_clusterwide() {
        let r0 = shard_response("shard-0", 5, vec![hit("aaa", 1.0), hit("bbb", 3.0)]);
        let r1 = shard_response("shard-1", 4, vec![hit("ccc", 2.0)]);

        let merged = merge_search_responses(Fanout { responses: vec![r0, r1], omitted: vec![] }, 1, 2, 0, 0);

        assert_eq!(merged.hits.len(), 1);
        assert_eq!(merged.hits[0].document.as_ref().unwrap().icao24, "bbb"); // top score
        assert_eq!(merged.total_matched, 9); // full cluster count, not limited
    }

    #[test]
    fn coverage_reports_partial_when_a_shard_is_missing() {
        // Asked 3 leaders, only 2 answered.
        let r0 = shard_response("shard-0", 1, vec![hit("aaa", 1.0)]);
        let r1 = shard_response("shard-1", 1, vec![hit("bbb", 2.0)]);

        let merged = merge_search_responses(Fanout { responses: vec![r0, r1], omitted: vec![] }, 10, 3, 0, 0);

        assert_eq!(merged.shards_queried, 3);
        assert_eq!(merged.shards_answered, 2); // partial: one shard down
    }

    #[test]
    fn no_shards_answered_is_empty_not_a_panic() {
        let merged = merge_search_responses(Fanout { responses: vec![], omitted: vec![] }, 10, 3, 0, 0);
        assert!(merged.hits.is_empty());
        assert_eq!(merged.total_matched, 0);
        assert_eq!(merged.shards_answered, 0);
        assert_eq!(merged.shards_queried, 3);
    }

    #[test]
    fn progressive_merge_accumulates_and_ranks_as_shards_arrive() {
        let mut merge = ProgressiveMerge::new(2, 10, 0);

        merge.add(shard_response("shard-0", 2, vec![hit("aaa", 1.0), hit("bbb", 3.0)]));
        let first = merge.snapshot(false);
        assert_eq!(first.shards_answered, 1);
        assert!(!first.complete);

        merge.add(shard_response("shard-1", 1, vec![hit("ccc", 2.0)]));
        let last = merge.snapshot(true);
        assert_eq!(last.shards_answered, 2);
        assert_eq!(last.total_matched, 3);
        assert!(last.complete);
        let order: Vec<&str> = last
            .hits
            .iter()
            .map(|h| h.document.as_ref().unwrap().icao24.as_str())
            .collect();
        assert_eq!(order, vec!["bbb", "ccc", "aaa"]);
    }

    #[test]
    fn progressive_merge_maintains_top_k() {
        let mut merge = ProgressiveMerge::new(2, 1, 0);
        merge.add(shard_response("shard-0", 5, vec![hit("aaa", 1.0)]));
        merge.add(shard_response("shard-1", 5, vec![hit("bbb", 3.0)]));
        let snap = merge.snapshot(true);
        assert_eq!(snap.hits.len(), 1);
        assert_eq!(snap.hits[0].document.as_ref().unwrap().icao24, "bbb"); // higher score wins
        assert_eq!(snap.total_matched, 10); // total is cluster-wide, not limited
    }
}
