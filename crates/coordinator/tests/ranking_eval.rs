//! Ranking eval harness: NDCG@10 over judgment lists for keyword-only, vector-only, and
//! RRF-hybrid rankers, on one corpus. The discipline of the item — no ranking knob ships
//! without a measurement. RRF must be at least as good as the better single ranker on the
//! mixed judgment set; the numbers are printed so a change that regresses relevance is
//! visible, and the harness runs in CI as a regression gate.

use std::collections::HashSet;

use common::embed::{Embedder, HashEmbedder};
use common::pb::{FlightDocument, SearchHit};
use coordinator::hybrid::rrf_fuse;
use shard_node::store::ShardStore;

fn doc(icao24: &str, callsign: &str, origin: &str, destination: &str, aircraft: &str) -> FlightDocument {
    FlightDocument {
        icao24: icao24.into(),
        callsign: callsign.into(),
        origin: origin.into(),
        destination: destination.into(),
        aircraft_type: aircraft.into(),
        ..Default::default()
    }
}

fn as_hit(icao24: &str) -> SearchHit {
    SearchHit {
        document: Some(FlightDocument { icao24: icao24.into(), ..Default::default() }),
        score: 0.0,
        provenance: None,
    }
}

/// DCG@k with binary relevance (1 if the doc is in `relevant`, else 0), gain / log2(rank+1).
fn dcg_at_k(ranked: &[String], relevant: &HashSet<String>, k: usize) -> f64 {
    ranked
        .iter()
        .take(k)
        .enumerate()
        .filter(|(_, id)| relevant.contains(*id))
        .map(|(i, _)| 1.0 / ((i as f64 + 2.0).log2()))
        .sum()
}

/// NDCG@k: DCG normalized by the ideal DCG (all relevant docs ranked first).
fn ndcg_at_k(ranked: &[String], relevant: &HashSet<String>, k: usize) -> f64 {
    let dcg = dcg_at_k(ranked, relevant, k);
    let ideal: f64 = (0..relevant.len().min(k)).map(|i| 1.0 / ((i as f64 + 2.0).log2())).sum();
    if ideal == 0.0 {
        0.0
    } else {
        dcg / ideal
    }
}

fn ids(hits: &[SearchHit]) -> Vec<String> {
    hits.iter().filter_map(|h| h.document.as_ref().map(|d| d.icao24.clone())).collect()
}

/// A judgment: a query and the set of icao24 that are relevant to it.
struct Judgment {
    query: &'static str,
    relevant: &'static [&'static str],
}

#[test]
fn rrf_hybrid_is_at_least_as_good_as_the_better_single_ranker() {
    // A corpus with CONTROLLED token frequencies, so the two rankers genuinely diverge:
    // BM25 weights by rarity (IDF), the hash embedder's cosine weights every token equally
    // (breadth). Each ranker therefore wins a different kind of query, and RRF — combining
    // complementary strengths — should match or beat the better one on the mixed mean.
    let mut store = ShardStore::new();
    // 8 common "Boeing" docs and 6 "Airbus" docs (both tokens are common).
    for i in 0..8 {
        store.insert(doc(&format!("boe{i}"), &format!("BO{i}"), "United States", "JFK", "Boeing 737"));
    }
    for i in 0..6 {
        store.insert(doc(&format!("air{i}"), &format!("AI{i}"), "France", "CDG", "Airbus A320"));
    }
    // 2 rare "Concorde" docs — the relevance signal for the keyword-favoring query.
    store.insert(doc("con0", "CC0", "France", "JFK", "Concorde supersonic"));
    store.insert(doc("con1", "CC1", "United Kingdom", "JFK", "Concorde supersonic"));
    // 1 trap doc carrying a unique junk token but NOT an Airbus — the IDF trap that a
    // rarity-weighted ranker over-promotes for the vector-favoring query.
    store.insert(doc("trap", "TR0", "United States", "zzqx", "Boeing 787"));

    let judgments = [
        // KEYWORD-FAVORING: "concorde" is rare, so BM25's IDF ranks the 2 concorde docs
        // above the 8 boeings; the equal-weight cosine mixes them in among the boeings.
        Judgment { query: "boeing concorde", relevant: &["con0", "con1"] },
        // VECTOR-FAVORING: "zzqx" is rarest, so BM25 over-promotes the single non-Airbus
        // trap doc to the top; breadth-weighted cosine keeps the 6 Airbus docs ranked high.
        Judgment {
            query: "airbus zzqx",
            relevant: &["air0", "air1", "air2", "air3", "air4", "air5"],
        },
        // EXACT LOOKUP: a distinctive callsign — both do well; the control case.
        Judgment { query: "CC0", relevant: &["con0"] },
    ];

    let k = 10;
    let (mut kw_sum, mut vec_sum, mut rrf_sum) = (0.0, 0.0, 0.0);
    println!("\n{:<26} {:>8} {:>8} {:>8}", "query", "keyword", "vector", "rrf");
    for j in &judgments {
        let relevant: HashSet<String> = j.relevant.iter().map(|s| s.to_string()).collect();

        let kw: Vec<SearchHit> = store
            .search(j.query, 20)
            .hits
            .iter()
            .map(|h| as_hit(&h.doc.icao24))
            .collect();
        let vq = HashEmbedder.embed(j.query);
        let vec: Vec<SearchHit> =
            store.vector_search(&vq, 20).iter().map(|h| as_hit(&h.doc.icao24)).collect();
        let rrf = rrf_fuse(kw.clone(), vec.clone(), 20);

        let kw_n = ndcg_at_k(&ids(&kw), &relevant, k);
        let vec_n = ndcg_at_k(&ids(&vec), &relevant, k);
        let rrf_n = ndcg_at_k(&ids(&rrf), &relevant, k);
        kw_sum += kw_n;
        vec_sum += vec_n;
        rrf_sum += rrf_n;
        println!("{:<26} {kw_n:>8.3} {vec_n:>8.3} {rrf_n:>8.3}", j.query);
    }
    let n = judgments.len() as f64;
    let (kw_avg, vec_avg, rrf_avg) = (kw_sum / n, vec_sum / n, rrf_sum / n);
    println!("{:<26} {kw_avg:>8.3} {vec_avg:>8.3} {rrf_avg:>8.3}", "MEAN NDCG@10");

    // The acceptance bar: fused ranking is at least as good as the better single ranker on
    // the mixed set (a tiny epsilon absorbs float noise). This is the number that would
    // justify — or kill — anything adaptive built on top.
    let better = kw_avg.max(vec_avg);
    assert!(
        rrf_avg + 1e-9 >= better,
        "RRF ({rrf_avg:.3}) should be >= the better single ranker ({better:.3})"
    );
}
