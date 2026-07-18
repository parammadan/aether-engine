//! Scatter-gather: fan a query out to every shard leader concurrently, then merge the
//! per-shard responses into one globally-ranked result.
//!
//! Split into a pure [`merge_search_responses`] (unit-testable without a network) and an
//! async [`scatter_gather`] that does the concurrent RPCs.

use std::cmp::Ordering;

use common::pb::shard_search_client::ShardSearchClient;
use common::pb::{SearchHit, SearchRequest, SearchResponse, SearchUpdate};
use tokio::task::JoinSet;

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
    responses: Vec<SearchResponse>,
    limit: usize,
    shards_queried: u32,
) -> SearchResponse {
    let shards_answered = responses.len() as u32;

    let mut hits = Vec::new();
    let mut total_matched = 0u64;
    for mut resp in responses {
        total_matched += resp.total_matched;
        hits.append(&mut resp.hits);
    }

    hits.sort_by(cmp_hits);

    if limit != 0 && hits.len() > limit {
        hits.truncate(limit);
    }

    SearchResponse {
        hits,
        total_matched,
        shard_id: "coordinator".to_string(),
        shards_queried,
        shards_answered,
    }
}

fn icao24_of(hit: &SearchHit) -> &str {
    hit.document.as_ref().map(|d| d.icao24.as_str()).unwrap_or("")
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
}

impl ProgressiveMerge {
    pub fn new(shards_queried: u32, limit: usize) -> Self {
        Self {
            limit,
            hits: Vec::new(),
            total_matched: 0,
            shards_answered: 0,
            shards_queried,
        }
    }

    /// Fold one shard's response into the running result.
    pub fn add(&mut self, resp: SearchResponse) {
        self.shards_answered += 1;
        self.total_matched += resp.total_matched;
        self.hits.extend(resp.hits);
        self.hits.sort_by(cmp_hits);
        if self.limit != 0 && self.hits.len() > self.limit {
            self.hits.truncate(self.limit);
        }
    }

    /// The current progressive result. `complete` marks the final update.
    pub fn snapshot(&self, complete: bool) -> SearchUpdate {
        SearchUpdate {
            hits: self.hits.clone(),
            total_matched: self.total_matched,
            shards_answered: self.shards_answered,
            shards_queried: self.shards_queried,
            complete,
        }
    }
}

/// Query every leader address concurrently and collect the responses that succeed. A leader
/// that can't be reached or errors is simply omitted — the caller reports coverage so the
/// result is *partial*, not a failure of the whole query.
pub async fn scatter_gather(leaders: Vec<String>, request: SearchRequest) -> Vec<SearchResponse> {
    let mut set = JoinSet::new();

    for addr in leaders {
        let req = request.clone();
        set.spawn(async move {
            let mut client = ShardSearchClient::connect(format!("http://{addr}")).await.ok()?;
            let resp = client.search(req).await.ok()?;
            Some(resp.into_inner())
        });
    }

    let mut responses = Vec::new();
    while let Some(joined) = set.join_next().await {
        if let Ok(Some(resp)) = joined {
            responses.push(resp);
        }
    }
    responses
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
        }
    }

    fn shard_response(shard_id: &str, total: u64, hits: Vec<SearchHit>) -> SearchResponse {
        SearchResponse {
            hits,
            total_matched: total,
            shard_id: shard_id.to_string(),
            shards_queried: 0,
            shards_answered: 0,
        }
    }

    #[test]
    fn merges_and_ranks_across_shards() {
        let r0 = shard_response("shard-0", 2, vec![hit("aaa", 1.0), hit("bbb", 3.0)]);
        let r1 = shard_response("shard-1", 1, vec![hit("ccc", 2.0)]);

        let merged = merge_search_responses(vec![r0, r1], 10, 2);

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
    fn limit_applies_to_merged_list_but_total_is_clusterwide() {
        let r0 = shard_response("shard-0", 5, vec![hit("aaa", 1.0), hit("bbb", 3.0)]);
        let r1 = shard_response("shard-1", 4, vec![hit("ccc", 2.0)]);

        let merged = merge_search_responses(vec![r0, r1], 1, 2);

        assert_eq!(merged.hits.len(), 1);
        assert_eq!(merged.hits[0].document.as_ref().unwrap().icao24, "bbb"); // top score
        assert_eq!(merged.total_matched, 9); // full cluster count, not limited
    }

    #[test]
    fn coverage_reports_partial_when_a_shard_is_missing() {
        // Asked 3 leaders, only 2 answered.
        let r0 = shard_response("shard-0", 1, vec![hit("aaa", 1.0)]);
        let r1 = shard_response("shard-1", 1, vec![hit("bbb", 2.0)]);

        let merged = merge_search_responses(vec![r0, r1], 10, 3);

        assert_eq!(merged.shards_queried, 3);
        assert_eq!(merged.shards_answered, 2); // partial: one shard down
    }

    #[test]
    fn no_shards_answered_is_empty_not_a_panic() {
        let merged = merge_search_responses(vec![], 10, 3);
        assert!(merged.hits.is_empty());
        assert_eq!(merged.total_matched, 0);
        assert_eq!(merged.shards_answered, 0);
        assert_eq!(merged.shards_queried, 3);
    }

    #[test]
    fn progressive_merge_accumulates_and_ranks_as_shards_arrive() {
        let mut merge = ProgressiveMerge::new(2, 10);

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
        let mut merge = ProgressiveMerge::new(2, 1);
        merge.add(shard_response("shard-0", 5, vec![hit("aaa", 1.0)]));
        merge.add(shard_response("shard-1", 5, vec![hit("bbb", 3.0)]));
        let snap = merge.snapshot(true);
        assert_eq!(snap.hits.len(), 1);
        assert_eq!(snap.hits[0].document.as_ref().unwrap().icao24, "bbb"); // higher score wins
        assert_eq!(snap.total_matched, 10); // total is cluster-wide, not limited
    }
}
