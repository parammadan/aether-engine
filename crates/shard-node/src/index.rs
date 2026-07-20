//! In-memory inverted (keyword) index over a shard's flight documents.
//!
//! # What it is
//! A classic inverted index: `term -> postings`, where each posting is a document that
//! contains the term plus how often. Searching a query means looking up each query term's
//! postings and combining them — O(matching postings) instead of scanning every document.
//!
//! # Scope and deliberate simplifications
//! - **Keyword only.** No embeddings / vector search yet. This indexes the text
//!   fields (`callsign`, `origin`, `destination`, `aircraft_type`).
//! - **Exact-term matching.** Tokens are whole alphanumeric runs, lowercased. A callsign
//!   like `UAL231` is one token `ual231`; searching `ual` will NOT match it. Prefix /
//!   edge-ngram tokenization is a later refinement — starting exact keeps scoring honest
//!   and the structure easy to reason about.
//! - **Term-frequency scoring.** A document's score is the sum of term frequencies of the
//!   matching query terms (OR semantics: any term may match). Simple and defensible;
//!   TF-IDF / BM25 (which down-weight common terms) is a deliberate later upgrade.
//! - **In-memory, not persisted.** A shard's index lives in RAM.

use std::cmp::Ordering;
use std::collections::HashMap;

use common::pb::FlightDocument;

/// Internal document id: an index into `docs`. A flight *observation* is a document, so
/// this is per-observation and monotonic — distinct from `icao24`, which repeats across a
/// given aircraft's many observations.
type DocId = u32;

/// One occurrence record: a document that contains a term, and how many times.
struct Posting {
    doc_id: DocId,
    term_freq: u32,
}

/// A scored search result borrowing the stored document.
pub struct ScoredHit<'a> {
    pub doc: &'a FlightDocument,
    pub score: f64,
}

/// Results of a search: the (possibly truncated) hits plus the total number of documents
/// that matched before `limit` was applied — the coordinator needs that count later for
/// correct cross-shard "N of M" reporting.
pub struct SearchResults<'a> {
    pub total_matched: usize,
    pub hits: Vec<ScoredHit<'a>>,
}

/// The inverted index for one shard.
///
/// Inserts are **upserts keyed by `icao24`**: a document's identity is the aircraft, and each
/// ingestion poll re-observes the same aircraft, so a repeat insert replaces the stored
/// document in place (old postings removed, new ones added) instead of appending a duplicate.
/// Without this, every poll would duplicate the whole fleet — duplicate hits in results and
/// unbounded growth.
pub struct InvertedIndex {
    /// `docs[doc_id]` is the stored document; `None` is a retired slot (its aircraft was
    /// removed). Slots are retired rather than shifted because posting lists embed doc
    /// ids — compacting would invalidate every posting. Retired slots go on the free list
    /// and are reused by later inserts, so the vector doesn't leak under churn.
    docs: Vec<Option<FlightDocument>>,
    /// `term -> postings`.
    postings: HashMap<String, Vec<Posting>>,
    /// `icao24 -> doc slot`, the upsert key.
    by_key: HashMap<String, DocId>,
    /// Retired slots available for reuse.
    free: Vec<DocId>,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            docs: Vec::new(),
            postings: HashMap::new(),
            by_key: HashMap::new(),
            free: Vec::new(),
        }
    }

    /// Number of distinct documents (aircraft) indexed.
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// All stored documents (for building snapshots of the shard's state).
    pub fn documents(&self) -> Vec<FlightDocument> {
        self.docs.iter().flatten().cloned().collect()
    }

    /// Per-term frequencies over the document's indexed text fields.
    fn term_freqs(doc: &FlightDocument) -> HashMap<String, u32> {
        let mut freqs: HashMap<String, u32> = HashMap::new();
        for field in [&doc.callsign, &doc.origin, &doc.destination, &doc.aircraft_type] {
            for token in tokenize(field) {
                *freqs.entry(token).or_insert(0) += 1;
            }
        }
        freqs
    }

    /// Remove `doc_id`'s postings for every term of `doc`. Cheap in practice: a flight
    /// document has a handful of short text fields, so this touches a few posting lists.
    fn remove_postings(&mut self, doc_id: DocId, doc: &FlightDocument) {
        for term in Self::term_freqs(doc).into_keys() {
            if let Some(list) = self.postings.get_mut(&term) {
                list.retain(|p| p.doc_id != doc_id);
                if list.is_empty() {
                    self.postings.remove(&term);
                }
            }
        }
    }

    /// Upsert one document by `icao24`: a new aircraft takes a retired slot (or appends);
    /// a re-observed aircraft replaces its stored document in place (old postings out,
    /// new postings in).
    pub fn insert(&mut self, doc: FlightDocument) {
        if let Some(&doc_id) = self.by_key.get(&doc.icao24) {
            let old = std::mem::replace(&mut self.docs[doc_id as usize], Some(doc))
                .expect("by_key never points at a retired slot");
            self.remove_postings(doc_id, &old);
            // Frequencies are computed up front so the immutable borrow of the stored doc
            // ends before we mutate the postings map.
            let freqs = Self::term_freqs(self.docs[doc_id as usize].as_ref().unwrap());
            for (term, term_freq) in freqs {
                self.postings
                    .entry(term)
                    .or_default()
                    .push(Posting { doc_id, term_freq });
            }
            return;
        }

        let freqs = Self::term_freqs(&doc);
        let key = doc.icao24.clone();
        let doc_id = match self.free.pop() {
            Some(slot) => {
                self.docs[slot as usize] = Some(doc);
                slot
            }
            None => {
                self.docs.push(Some(doc));
                (self.docs.len() - 1) as DocId
            }
        };
        self.by_key.insert(key, doc_id);
        for (term, term_freq) in freqs {
            self.postings.entry(term).or_default().push(Posting { doc_id, term_freq });
        }
    }

    /// Remove one aircraft by `icao24`: its postings are deleted, its slot retired for
    /// reuse. Returns whether it was present. After this, the document is unfindable by
    /// any term and gone from `documents()` — a removal, not a tombstone.
    pub fn remove(&mut self, icao24: &str) -> bool {
        let Some(doc_id) = self.by_key.remove(icao24) else {
            return false;
        };
        let doc = self.docs[doc_id as usize]
            .take()
            .expect("by_key never points at a retired slot");
        self.remove_postings(doc_id, &doc);
        self.free.push(doc_id);
        true
    }

    /// Search the index. OR semantics: a document matches if it contains any query term.
    /// Score = sum of the term frequencies of the matching query terms. Results are sorted
    /// by score descending; `limit == 0` means "no limit".
    pub fn search(&self, query: &str, limit: usize) -> SearchResults<'_> {
        let terms = tokenize(query);
        if terms.is_empty() {
            return SearchResults {
                total_matched: 0,
                hits: Vec::new(),
            };
        }

        // Accumulate a score per matched document.
        let mut scores: HashMap<DocId, f64> = HashMap::new();
        for term in terms {
            if let Some(postings) = self.postings.get(&term) {
                for posting in postings {
                    *scores.entry(posting.doc_id).or_insert(0.0) += posting.term_freq as f64;
                }
            }
        }

        let total_matched = scores.len();

        let mut hits: Vec<ScoredHit<'_>> = scores
            .into_iter()
            .map(|(doc_id, score)| ScoredHit {
                doc: self.docs[doc_id as usize]
                    .as_ref()
                    .expect("postings never reference a retired slot"),
                score,
            })
            .collect();

        // Sort by score desc; ties broken by doc_id asc for a stable, deterministic order.
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.doc.icao24.cmp(&b.doc.icao24))
        });

        if limit != 0 && hits.len() > limit {
            hits.truncate(limit);
        }

        SearchResults {
            total_matched,
            hits,
        }
    }
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Split text into lowercase alphanumeric tokens. Anything non-alphanumeric is a separator,
/// so `"Boeing 737"` -> `["boeing", "737"]`. Case-folding makes matching case-insensitive.
fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(icao24: &str, callsign: &str, origin: &str, destination: &str, aircraft: &str) -> FlightDocument {
        FlightDocument {
            icao24: icao24.to_string(),
            callsign: callsign.to_string(),
            origin: origin.to_string(),
            destination: destination.to_string(),
            aircraft_type: aircraft.to_string(),
            ..Default::default()
        }
    }

    fn sample() -> InvertedIndex {
        let mut idx = InvertedIndex::new();
        idx.insert(doc("a1", "UAL231", "SFO", "JFK", "Boeing 737"));
        idx.insert(doc("b2", "DAL45", "ATL", "LAX", "Airbus A320"));
        idx.insert(doc("c3", "UAL900", "ORD", "SFO", "Boeing 777"));
        idx
    }

    #[test]
    fn empty_query_returns_nothing() {
        let idx = sample();
        let r = idx.search("   ", 10);
        assert_eq!(r.total_matched, 0);
        assert!(r.hits.is_empty());
    }

    #[test]
    fn matches_across_documents_or_semantics() {
        // "sfo" is doc a1's origin and doc c3's destination -> 2 matches.
        let idx = sample();
        let r = idx.search("SFO", 10);
        assert_eq!(r.total_matched, 2);
        let ids: Vec<&str> = r.hits.iter().map(|h| h.doc.icao24.as_str()).collect();
        assert!(ids.contains(&"a1") && ids.contains(&"c3"));
    }

    #[test]
    fn tokenization_is_case_insensitive() {
        let idx = sample();
        assert_eq!(idx.search("boeing", 10).total_matched, 2);
        assert_eq!(idx.search("BOEING", 10).total_matched, 2);
    }

    #[test]
    fn exact_callsign_token_matches_but_partial_does_not() {
        // Documents the exact-term limitation: full callsign matches, prefix does not.
        let idx = sample();
        assert_eq!(idx.search("ual231", 10).total_matched, 1);
        assert_eq!(idx.search("ual", 10).total_matched, 0);
    }

    #[test]
    fn term_frequency_ranks_higher() {
        // A doc mentioning the term twice (origin + destination) outscores a single mention.
        let mut idx = InvertedIndex::new();
        idx.insert(doc("hi", "X1", "SFO", "SFO", "")); // "sfo" twice -> tf 2
        idx.insert(doc("lo", "X2", "SFO", "JFK", "")); // "sfo" once  -> tf 1
        let r = idx.search("sfo", 10);
        assert_eq!(r.total_matched, 2);
        assert_eq!(r.hits[0].doc.icao24, "hi");
        assert!(r.hits[0].score > r.hits[1].score);
    }

    #[test]
    fn limit_truncates_hits_but_not_total_matched() {
        let idx = sample();
        let r = idx.search("boeing", 1);
        assert_eq!(r.total_matched, 2); // both Boeings matched
        assert_eq!(r.hits.len(), 1); // but only one returned
    }

    #[test]
    fn removed_aircraft_are_unfindable_and_slots_are_reused() {
        let mut idx = sample();
        assert!(idx.remove("a1"));
        assert!(!idx.remove("a1"), "double remove is a no-op");

        assert_eq!(idx.len(), 2);
        assert_eq!(idx.search("ual231", 10).total_matched, 0, "removed text unfindable");
        assert_eq!(idx.search("boeing", 10).total_matched, 1, "shared term keeps the survivor");
        assert!(idx.documents().iter().all(|d| d.icao24 != "a1"), "gone from snapshots too");

        // A new aircraft reuses the retired slot: the backing vector must not grow.
        let before = idx.docs.len();
        idx.insert(doc("d4", "SWA100", "DEN", "PHX", "Boeing 737"));
        assert_eq!(idx.docs.len(), before, "retired slot reused, no growth");
        assert_eq!(idx.search("swa100", 10).total_matched, 1);
    }

    /// The 8.1 property: an index after ARBITRARY insert/remove interleavings is
    /// indistinguishable from a fresh build of the surviving set — same counts, same
    /// results for every probe. Deterministic pseudo-random interleavings (seeded LCG),
    /// several seeds.
    #[test]
    fn interleaved_inserts_and_removes_equal_a_fresh_build_of_survivors() {
        for seed in [3u64, 17, 4242] {
            let mut rng = seed;
            let mut next = move || {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (rng >> 33) as usize
            };

            let keys: Vec<String> = (0..40).map(|i| format!("ac{i:02}")).collect();
            let origins = ["SFO", "JFK", "ATL", "ORD", "DEN"];
            let crafts = ["Boeing 737", "Airbus A320", "Boeing 777"];

            let mut idx = InvertedIndex::new();
            let mut surviving: std::collections::HashMap<String, FlightDocument> =
                std::collections::HashMap::new();

            for step in 0..400 {
                let key = &keys[next() % keys.len()];
                if next() % 3 == 0 {
                    idx.remove(key);
                    surviving.remove(key);
                } else {
                    let d = doc(
                        key,
                        &format!("FL{step}"),
                        origins[next() % origins.len()],
                        origins[next() % origins.len()],
                        crafts[next() % crafts.len()],
                    );
                    surviving.insert(key.clone(), d.clone());
                    idx.insert(d);
                }
            }

            // Fresh build of exactly the survivors.
            let mut fresh = InvertedIndex::new();
            for d in surviving.values() {
                fresh.insert(d.clone());
            }

            assert_eq!(idx.len(), fresh.len(), "seed {seed}: document counts diverged");
            for probe in ["sfo", "jfk", "atl", "ord", "den", "boeing", "airbus", "737", "a320", "777"] {
                let a = idx.search(probe, 0);
                let b = fresh.search(probe, 0);
                assert_eq!(a.total_matched, b.total_matched, "seed {seed}: '{probe}' counts diverged");
                let ids = |r: &SearchResults| -> Vec<String> {
                    let mut v: Vec<String> = r.hits.iter().map(|h| h.doc.icao24.clone()).collect();
                    v.sort();
                    v
                };
                assert_eq!(ids(&a), ids(&b), "seed {seed}: '{probe}' result sets diverged");
            }
        }
    }

    #[test]
    fn reinserting_an_aircraft_updates_in_place_instead_of_duplicating() {
        let mut idx = InvertedIndex::new();
        idx.insert(doc("a1", "UAL231", "SFO", "JFK", "Boeing 737"));
        // The same aircraft re-observed with a changed callsign/route.
        idx.insert(doc("a1", "UAL500", "SFO", "ORD", "Boeing 737"));

        assert_eq!(idx.len(), 1); // still one document — not two
        assert_eq!(idx.search("ual231", 10).total_matched, 0); // old text unfindable
        assert_eq!(idx.search("ual500", 10).total_matched, 1); // new text findable
        assert_eq!(idx.search("jfk", 10).total_matched, 0); // stale posting removed
        // Shared terms still match exactly once (no ghost duplicate posting).
        assert_eq!(idx.search("boeing", 10).total_matched, 1);
    }
}
