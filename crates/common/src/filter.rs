//! Structured-filter evaluation: validate a filter's shape once (a type-mismatched test on
//! a built-in column = loud error, never a silent no-op), then test documents against it.
//! Unknown field NAMES are not errors — they're generic connector-defined fields, matched
//! from the document's `text`/`number` maps. Lives in `common` so the coordinator can
//! validate BEFORE fanning out (a bad filter is the caller's error, not partial coverage)
//! and the shard applies the same rules when evaluating.

use crate::pb::{filter_condition::Test, Filter, FilterCondition, FlightDocument};

/// Text fields addressable by an `equals` condition (compared case-insensitively,
/// consistent with the keyword index's case folding).
const TEXT_FIELDS: &[&str] =
    &["callsign", "origin", "destination", "aircraft_type", "tenant_id", "icao24"];

/// Numeric fields addressable by a `range` condition. `observed_at` (epoch ms) makes a
/// range on it the time-window filter.
const NUMERIC_FIELDS: &[&str] =
    &["altitude", "velocity", "heading", "vertical_rate", "latitude", "longitude", "observed_at"];

const BOOL_FIELDS: &[&str] = &["on_ground"];

/// Check every condition names a known field with the right kind of test. Returns a
/// human-readable error naming the offending condition.
pub fn validate(filter: &Filter) -> Result<(), String> {
    for cond in &filter.conditions {
        let field = cond.field.as_str();
        match &cond.test {
            Some(Test::Equals(_)) => {
                // A known numeric/bool column with `equals` is a real type mismatch; any other
                // name is a text column — built-in or a generic `text` field a connector defined.
                if NUMERIC_FIELDS.contains(&field) || BOOL_FIELDS.contains(&field) {
                    return Err(format!("'{field}' is not a text field (equals applies to text/generic fields)"));
                }
            }
            Some(Test::Range(r)) => {
                // Symmetric: a known text/bool column with `range` is a mismatch; any other name
                // is a numeric column — built-in or a generic `number` field.
                if TEXT_FIELDS.contains(&field) || BOOL_FIELDS.contains(&field) {
                    return Err(format!("'{field}' is not a numeric field (range applies to numeric/generic fields)"));
                }
                if let (Some(min), Some(max)) = (r.min, r.max) {
                    if min > max {
                        return Err(format!("'{field}' range has min {min} > max {max}"));
                    }
                }
            }
            Some(Test::Is(_)) => {
                if !BOOL_FIELDS.contains(&field) {
                    return Err(format!(
                        "'{field}' is not a boolean field (is applies to: {})",
                        BOOL_FIELDS.join(", ")
                    ));
                }
            }
            None => return Err(format!("condition on '{field}' has no test")),
        }
    }
    Ok(())
}

fn text_value<'a>(doc: &'a FlightDocument, field: &str) -> &'a str {
    match field {
        "callsign" => &doc.callsign,
        "origin" => &doc.origin,
        "destination" => &doc.destination,
        "aircraft_type" => &doc.aircraft_type,
        "tenant_id" => &doc.tenant_id,
        "icao24" => &doc.icao24,
        // Generic text field supplied by a connector.
        _ => doc.text.get(field).map(String::as_str).unwrap_or(""),
    }
}

fn numeric_value(doc: &FlightDocument, field: &str) -> f64 {
    match field {
        "altitude" => doc.altitude,
        "velocity" => doc.velocity,
        "heading" => doc.heading,
        "vertical_rate" => doc.vertical_rate,
        "latitude" => doc.latitude,
        "longitude" => doc.longitude,
        "observed_at" => doc.observed_at as f64,
        // Generic numeric field supplied by a connector.
        _ => doc.number.get(field).copied().unwrap_or(f64::NAN),
    }
}

fn condition_holds(doc: &FlightDocument, cond: &FilterCondition) -> bool {
    match &cond.test {
        Some(Test::Equals(want)) => text_value(doc, &cond.field).eq_ignore_ascii_case(want),
        Some(Test::Range(r)) => {
            let v = numeric_value(doc, &cond.field);
            r.min.is_none_or(|min| v >= min) && r.max.is_none_or(|max| v <= max)
        }
        Some(Test::Is(want)) => doc.on_ground == *want,
        None => false, // rejected by validate(); defensively match nothing
    }
}

/// Does `doc` pass the (already-validated) filter? Empty/absent filter passes everything.
pub fn passes(doc: &FlightDocument, filter: Option<&Filter>) -> bool {
    match filter {
        None => true,
        Some(f) => f.conditions.iter().all(|c| condition_holds(doc, c)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::NumericRange;

    fn doc() -> FlightDocument {
        FlightDocument {
            icao24: "abc123".into(),
            callsign: "UAL231".into(),
            origin: "France".into(),
            altitude: 2500.0,
            observed_at: 1_000_000,
            on_ground: false,
            ..Default::default()
        }
    }

    fn equals(field: &str, v: &str) -> FilterCondition {
        FilterCondition { field: field.into(), test: Some(Test::Equals(v.into())) }
    }
    fn range(field: &str, min: Option<f64>, max: Option<f64>) -> FilterCondition {
        FilterCondition { field: field.into(), test: Some(Test::Range(NumericRange { min, max })) }
    }
    fn filter(conds: Vec<FilterCondition>) -> Filter {
        Filter { conditions: conds }
    }

    #[test]
    fn equals_is_case_insensitive_and_anded() {
        let f = filter(vec![equals("origin", "france"), equals("callsign", "ual231")]);
        assert!(validate(&f).is_ok());
        assert!(passes(&doc(), Some(&f)));
        let f = filter(vec![equals("origin", "france"), equals("callsign", "nope")]);
        assert!(!passes(&doc(), Some(&f)), "AND semantics: one failing condition fails all");
    }

    #[test]
    fn ranges_are_inclusive_and_half_open() {
        assert!(passes(&doc(), Some(&filter(vec![range("altitude", Some(2500.0), None)]))));
        assert!(passes(&doc(), Some(&filter(vec![range("altitude", None, Some(2500.0))]))));
        assert!(!passes(&doc(), Some(&filter(vec![range("altitude", Some(3000.0), None)]))));
        // Time window = a range on observed_at.
        assert!(passes(&doc(), Some(&filter(vec![range("observed_at", Some(999_999.0), Some(1_000_001.0))]))));
    }

    #[test]
    fn boolean_field_filters() {
        let f = filter(vec![FilterCondition { field: "on_ground".into(), test: Some(Test::Is(false)) }]);
        assert!(validate(&f).is_ok());
        assert!(passes(&doc(), Some(&f)));
    }

    #[test]
    fn builtin_type_mismatches_are_loud_errors() {
        assert!(validate(&filter(vec![equals("altitude", "high")])).is_err(), "numeric field with equals");
        assert!(validate(&filter(vec![range("origin", Some(0.0), None)])).is_err(), "text field with range");
        assert!(validate(&filter(vec![range("altitude", Some(5.0), Some(1.0))])).is_err(), "min > max");
    }

    #[test]
    fn generic_connector_fields_are_addressable() {
        // A field not in the built-in schema is a generic text/number field (a connector
        // defined it). It validates and is matched from the doc's `text`/`number` maps.
        let mut d = FlightDocument::default();
        d.text.insert("severity_label".into(), "high".into());
        d.number.insert("latency_ms".into(), 42.0);

        let f = filter(vec![equals("severity_label", "HIGH")]);
        assert!(validate(&f).is_ok(), "unknown text field is a valid generic field");
        assert!(passes(&d, Some(&f)), "generic text field matched case-insensitively");

        let f = filter(vec![range("latency_ms", Some(40.0), Some(50.0))]);
        assert!(validate(&f).is_ok(), "unknown numeric field is a valid generic field");
        assert!(passes(&d, Some(&f)), "generic numeric field matched by range");
        assert!(!passes(&d, Some(&filter(vec![range("latency_ms", Some(100.0), None)]))));
    }

    #[test]
    fn empty_filter_passes_everything() {
        assert!(passes(&doc(), None));
        assert!(passes(&doc(), Some(&filter(vec![]))));
    }
}
