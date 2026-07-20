//! Reciprocal Rank Fusion of two rankings.
//!
//! Keyword (BM25) and vector (cosine) scores live on different, incomparable scales —
//! summing or averaging them means inventing a conversion nobody can defend. RRF sidesteps
//! that: it fuses by RANK, not score. A document's fused score is `Σ 1/(k + rank_r)` over
//! the rankers that returned it (rank is 1-based; `k=60` is the standard constant that
//! damps the top few ranks so a single #1 can't dominate). Training-free, and a strong
//! baseline precisely because it ignores the magnitudes it has no basis to compare.

use std::collections::HashMap;

use common::pb::SearchHit;

/// The RRF damping constant. 60 is the value from the original Cormack et al. paper and
/// the OpenSearch/Elasticsearch default — a doc ranked #1 by one ranker contributes
/// 1/61 ≈ 0.0164, so agreement across rankers matters more than a lone top rank.
pub const RRF_K: f64 = 60.0;

fn icao24_of(hit: &SearchHit) -> String {
    hit.document.as_ref().map(|d| d.icao24.clone()).unwrap_or_default()
}

/// Fuse two ranked hit lists by RRF, returning the top `limit` (0 = all) by fused score.
/// A document present in both rankings accumulates both contributions; its returned hit
/// keeps the keyword-side provenance when it has one (so `index` reflects a real match),
/// falling back to the vector-side hit otherwise. The `score` field becomes the RRF score.
pub fn rrf_fuse(keyword: Vec<SearchHit>, vector: Vec<SearchHit>, limit: usize) -> Vec<SearchHit> {
    let mut fused: HashMap<String, (f64, SearchHit)> = HashMap::new();

    let mut fold = |list: Vec<SearchHit>, prefer: bool| {
        for (i, hit) in list.into_iter().enumerate() {
            let key = icao24_of(&hit);
            if key.is_empty() {
                continue;
            }
            let contribution = 1.0 / (RRF_K + (i as f64 + 1.0));
            match fused.get_mut(&key) {
                Some((score, existing)) => {
                    *score += contribution;
                    // Keyword pass runs first with prefer=true and seeds the entry; the
                    // vector pass only replaces the stored hit if none was preferred.
                    if prefer {
                        *existing = hit;
                    }
                }
                None => {
                    fused.insert(key, (contribution, hit));
                }
            }
        }
    };
    fold(keyword, true);
    fold(vector, false);

    let mut out: Vec<SearchHit> = fused
        .into_values()
        .map(|(score, mut hit)| {
            hit.score = score;
            hit
        })
        .collect();
    // Fused score descending; ties broken by icao24 for a stable, deterministic order.
    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| icao24_of(a).cmp(&icao24_of(b)))
    });
    if limit != 0 && out.len() > limit {
        out.truncate(limit);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::pb::FlightDocument;

    fn hit(icao24: &str) -> SearchHit {
        SearchHit {
            document: Some(FlightDocument { icao24: icao24.into(), ..Default::default() }),
            score: 0.0,
            provenance: None,
        }
    }
    fn ids(hits: &[SearchHit]) -> Vec<String> {
        hits.iter().map(icao24_of).collect()
    }

    #[test]
    fn a_doc_ranked_by_both_beats_a_doc_ranked_by_one() {
        // "b" is #2 in keyword and #1 in vector; "a" is #1 in keyword only. Agreement wins.
        let keyword = vec![hit("a"), hit("b"), hit("c")];
        let vector = vec![hit("b"), hit("d")];
        let fused = rrf_fuse(keyword, vector, 0);
        assert_eq!(fused[0].document.as_ref().unwrap().icao24, "b", "agreed-upon doc ranks first");
        // b: 1/(60+2) + 1/(60+1); a: 1/(60+1). So b > a.
        let b = fused.iter().find(|h| icao24_of(h) == "b").unwrap().score;
        let a = fused.iter().find(|h| icao24_of(h) == "a").unwrap().score;
        assert!(b > a);
    }

    #[test]
    fn union_of_both_lists_is_returned() {
        let keyword = vec![hit("a"), hit("b")];
        let vector = vec![hit("b"), hit("c")];
        let mut got = ids(&rrf_fuse(keyword, vector, 0));
        got.sort();
        assert_eq!(got, vec!["a", "b", "c"], "fusion returns the union, deduped");
    }

    #[test]
    fn limit_truncates_the_fused_list() {
        let keyword = vec![hit("a"), hit("b"), hit("c")];
        let vector = vec![hit("d")];
        assert_eq!(rrf_fuse(keyword, vector, 2).len(), 2);
    }

    #[test]
    fn empty_inputs_are_fine() {
        assert!(rrf_fuse(vec![], vec![], 10).is_empty());
    }
}
