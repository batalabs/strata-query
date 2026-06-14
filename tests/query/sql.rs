//! Mirror of `src/query/sql.rs` unit tests, expressed through the public API.

use strata_query::{parse_query, AggFunc, QueryPredicate, Row};

#[test]
fn test_query_predicate_exact_match() {
    let mut query = QueryPredicate::new();
    query.add_filter("status".to_string(), vec!["ERROR".to_string()]);
    let mut row = Row::new();
    row.insert("status".to_string(), "ERROR".to_string());
    assert!(query.matches(&row));
}

#[test]
fn test_query_predicate_no_match() {
    let mut query = QueryPredicate::new();
    query.add_filter("status".to_string(), vec!["ERROR".to_string()]);
    let mut row = Row::new();
    row.insert("status".to_string(), "OK".to_string());
    assert!(!query.matches(&row));
}

#[test]
fn test_parse_simple_query() {
    let parsed = parse_query("SELECT * FROM trips WHERE status = 'ERROR'").unwrap();
    assert_eq!(parsed.table, "trips");
    assert!(parsed.aggregations.is_empty());
}

#[test]
fn test_parse_aggregation_query() {
    let parsed = parse_query("SELECT COUNT(*) FROM trips WHERE fare >= 100").unwrap();
    assert_eq!(parsed.aggregations.len(), 1);
    assert_eq!(parsed.aggregations[0].func, AggFunc::Count);
    assert_eq!(parsed.aggregations[0].column, "*");
}

#[test]
fn test_parse_multi_aggregation() {
    let parsed = parse_query(
        "SELECT COUNT(*), SUM(fare), AVG(fare), MIN(fare), MAX(fare) FROM trips WHERE fare >= 100",
    )
    .unwrap();
    assert_eq!(parsed.aggregations.len(), 5);
    assert_eq!(parsed.aggregations[0].func, AggFunc::Count);
    assert_eq!(parsed.aggregations[0].column, "*");
    assert_eq!(parsed.aggregations[1].func, AggFunc::Sum);
    assert_eq!(parsed.aggregations[1].column, "fare");
    assert_eq!(parsed.aggregations[2].func, AggFunc::Avg);
    assert_eq!(parsed.aggregations[3].func, AggFunc::Min);
    assert_eq!(parsed.aggregations[4].func, AggFunc::Max);
}

#[test]
fn between_parses_into_range_filter() {
    let parsed = parse_query("SELECT * FROM trips WHERE fare BETWEEN 50 AND 500").unwrap();
    let range = parsed
        .predicate
        .get_range("fare")
        .expect("BETWEEN should produce a range filter");
    assert_eq!(range.min, 50.0);
    assert_eq!(range.max, 500.0);
}

#[test]
fn in_list_parses_into_exact_filter() {
    let parsed = parse_query("SELECT * FROM trips WHERE status IN ('OK', 'WARN')").unwrap();
    let values = parsed
        .predicate
        .get_exact_values("status")
        .expect("IN should produce an exact filter");
    assert_eq!(values, &vec!["OK".to_string(), "WARN".to_string()]);
}

#[test]
fn comparison_operators_build_a_bounded_range() {
    // `fare >= 100 AND fare <= 500` narrows a single dimension into [100, 500].
    let parsed = parse_query("SELECT * FROM trips WHERE fare >= 100 AND fare <= 500").unwrap();
    let range = parsed.predicate.get_range("fare").unwrap();
    assert_eq!(range.min, 100.0);
    assert_eq!(range.max, 500.0);
}

#[test]
fn strict_inequalities_route_like_inclusive_bounds() {
    // `>` and `<` map to min/max filters (bucket-level pruning is inclusive).
    let parsed = parse_query("SELECT * FROM trips WHERE fare > 10 AND fare < 1000").unwrap();
    let range = parsed.predicate.get_range("fare").unwrap();
    assert_eq!(range.min, 10.0);
    assert_eq!(range.max, 1000.0);
}

#[test]
fn equality_and_range_on_different_columns_coexist() {
    let parsed = parse_query("SELECT * FROM trips WHERE status = 'ERROR' AND fare >= 100").unwrap();
    assert_eq!(
        parsed.predicate.get_exact_values("status"),
        Some(&vec!["ERROR".to_string()])
    );
    assert_eq!(parsed.predicate.get_range("fare").unwrap().min, 100.0);
}

#[test]
fn unsupported_or_is_ignored_not_errored() {
    // OR is not modeled; the parser must not error, and must not invent a
    // filter it cannot represent soundly. Either column may be absent.
    let parsed =
        parse_query("SELECT * FROM trips WHERE status = 'OK' OR status = 'ERROR'").unwrap();
    // No exact filter should be produced for an OR (would be unsound to AND them).
    assert!(parsed.predicate.get_exact_values("status").is_none());
    assert_eq!(parsed.table, "trips");
}

#[test]
fn unsupported_like_is_ignored_gracefully() {
    // LIKE has no predicate representation; parsing succeeds with no filter.
    let parsed = parse_query("SELECT * FROM trips WHERE status LIKE 'ER%'").unwrap();
    assert_eq!(parsed.predicate.all_filters().count(), 0);
    assert_eq!(parsed.table, "trips");
}

#[test]
fn limit_is_extracted() {
    let parsed = parse_query("SELECT * FROM trips WHERE status = 'OK' LIMIT 10").unwrap();
    assert_eq!(parsed.limit, Some(10));
}

#[test]
fn no_where_clause_yields_empty_predicate() {
    let parsed = parse_query("SELECT * FROM trips").unwrap();
    assert_eq!(parsed.predicate.all_filters().count(), 0);
}

#[test]
fn non_select_statement_errors() {
    assert!(parse_query("DELETE FROM trips WHERE id = 1").is_err());
    assert!(parse_query("not valid sql at all ;;;").is_err());
}
