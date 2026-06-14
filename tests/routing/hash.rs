//! Mirror of `src/routing/hash.rs` unit tests, expressed through the public API.

use strata_query::routing::hash::{
    compute_bitset_index, compute_query_bitset_indices, compute_segment_key, route_dimension,
    route_numeric, route_numeric_range, route_value,
};
use strata_query::{DimensionType, Row};

// --- Hash routing tests ---

#[test]
fn test_route_value_deterministic() {
    let bucket1 = route_value("test", 10);
    let bucket2 = route_value("test", 10);
    assert_eq!(bucket1, bucket2);
}

#[test]
fn test_route_value_range() {
    for _ in 0..100 {
        let bucket = route_value("random_value", 10);
        assert!(bucket < 10);
    }
}

#[test]
fn test_hash_distribution() {
    // Use enough values to reliably hit multiple buckets
    let mut buckets = std::collections::HashSet::new();
    for i in 0..100 {
        buckets.insert(route_value(&format!("value_{}", i), 10));
    }
    // With 100 values and 10 buckets, expect most buckets to be hit
    assert!(
        buckets.len() >= 5,
        "Expected >= 5 distinct buckets, got {}",
        buckets.len()
    );
}

// --- MARS numeric routing tests ---

#[test]
fn test_route_numeric_basic() {
    // Values in [1, 2) → bucket 0
    assert_eq!(route_numeric("1.0", 8), 0);
    assert_eq!(route_numeric("1.5", 8), 0);

    // Values in [2, 4) → bucket 1
    assert_eq!(route_numeric("2.0", 8), 1);
    assert_eq!(route_numeric("3.0", 8), 1);
    assert_eq!(route_numeric("3.99", 8), 1);

    // Values in [4, 8) → bucket 2
    assert_eq!(route_numeric("4.0", 8), 2);
    assert_eq!(route_numeric("7.0", 8), 2);

    // Values in [8, 16) → bucket 3
    assert_eq!(route_numeric("8.0", 8), 3);
    assert_eq!(route_numeric("15.0", 8), 3);
}

#[test]
fn test_route_numeric_large_values() {
    // 2^10 = 1024 → bucket 10
    assert_eq!(route_numeric("1024", 16), 10);
    // 2^15 = 32768 → bucket 15
    assert_eq!(route_numeric("32768", 16), 15);
    // Values beyond max bucket clamp to last bucket
    assert_eq!(route_numeric("1000000", 16), 15);
}

#[test]
fn test_route_numeric_edge_cases() {
    // Non-positive → bucket 0
    assert_eq!(route_numeric("0", 8), 0);
    assert_eq!(route_numeric("-5.0", 8), 0);
    // Unparseable → bucket 0
    assert_eq!(route_numeric("not_a_number", 8), 0);
    assert_eq!(route_numeric("", 8), 0);
}

#[test]
fn test_route_numeric_deterministic() {
    for val in &["1.0", "42.5", "1000.0", "0.001"] {
        let b1 = route_numeric(val, 16);
        let b2 = route_numeric(val, 16);
        assert_eq!(b1, b2, "MARS routing should be deterministic for {}", val);
    }
}

// --- MARS range routing tests ---

#[test]
fn test_route_numeric_range_basic() {
    // Range [1, 7] → buckets 0 (1-2), 1 (2-4), 2 (4-8)
    let buckets = route_numeric_range(1.0, 7.0, 8);
    assert_eq!(buckets, vec![0, 1, 2]);
}

#[test]
fn test_route_numeric_range_narrow() {
    // Range [4, 6] → only bucket 2 (4-8)
    let buckets = route_numeric_range(4.0, 6.0, 8);
    assert_eq!(buckets, vec![2]);
}

#[test]
fn test_route_numeric_range_wide() {
    // Range [1, 1000] → buckets 0 through 9 (2^9 = 512 ≤ 1000)
    let buckets = route_numeric_range(1.0, 1000.0, 16);
    assert_eq!(buckets, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn test_route_numeric_range_near_optimality() {
    // For ratio r = max/min, we touch at most ceil(log2(r)) + 1 buckets
    // Range [100, 200] → ratio = 2, so at most 2 buckets
    let buckets = route_numeric_range(100.0, 200.0, 16);
    assert!(
        buckets.len() <= 2,
        "Expected ≤ 2 buckets for ratio 2, got {}",
        buckets.len()
    );
}

#[test]
fn test_route_numeric_range_empty() {
    let buckets = route_numeric_range(10.0, 5.0, 8);
    assert!(buckets.is_empty());
}

// --- Unified routing tests ---

#[test]
fn test_route_dimension_dispatches_correctly() {
    // Same value "42" routed differently based on type
    let cat_bucket = route_dimension("42", DimensionType::Categorical, 16);
    let num_bucket = route_dimension("42", DimensionType::Numeric, 16);

    // Numeric should give floor(log2(42)) = 5
    assert_eq!(num_bucket, 5);
    // Categorical is hash-based, just verify it's in range
    assert!(cat_bucket < 16);
}

// --- Segment key and bitset tests ---

#[test]
fn test_compute_segment_key_with_types() {
    let mut row = Row::new();
    row.insert("status".to_string(), "OK".to_string());
    row.insert("fare".to_string(), "150.0".to_string());
    row.insert("hour".to_string(), "9".to_string());

    let dimensions = vec!["status".to_string(), "fare".to_string(), "hour".to_string()];
    let dim_types = vec![
        DimensionType::Categorical,
        DimensionType::Numeric,
        DimensionType::Categorical,
    ];

    let key = compute_segment_key(&row, &dimensions, &dim_types, &[4, 16]);
    assert!(key.0 < 4);
    assert!(key.1 < 16);

    // Deterministic
    let key2 = compute_segment_key(&row, &dimensions, &dim_types, &[4, 16]);
    assert_eq!(key, key2);
}

#[test]
fn test_compute_bitset_index_with_types() {
    let mut row = Row::new();
    row.insert("status".to_string(), "OK".to_string());
    row.insert("fare".to_string(), "150.0".to_string());
    row.insert("hour".to_string(), "9".to_string());

    let dimensions = vec!["status".to_string(), "fare".to_string(), "hour".to_string()];
    let dim_types = vec![
        DimensionType::Categorical,
        DimensionType::Numeric,
        DimensionType::Categorical,
    ];

    let index = compute_bitset_index(&row, &dimensions, &dim_types, &[4, 16, 24]);
    assert!(index < 4 * 16 * 24);

    let index2 = compute_bitset_index(&row, &dimensions, &dim_types, &[4, 16, 24]);
    assert_eq!(index, index2);
}

#[test]
fn test_compute_query_bitset_indices() {
    let dim1_values = vec!["OK".to_string()];
    let dim2_values = vec!["NYC".to_string()];
    let dim3_values = vec!["9".to_string(), "10".to_string()];

    let indices =
        compute_query_bitset_indices(&dim1_values, &dim2_values, &dim3_values, &[4, 4, 24]);

    assert_eq!(indices.len(), 2);
    for idx in indices {
        assert!(idx < 4 * 4 * 24);
    }
}

// --- Backward compatibility tests ---

#[test]
fn test_compute_segment_key_backward_compat() {
    let mut row = Row::new();
    row.insert("status".to_string(), "OK".to_string());
    row.insert("region".to_string(), "NYC".to_string());
    row.insert("hour".to_string(), "9".to_string());

    let dimensions = vec![
        "status".to_string(),
        "region".to_string(),
        "hour".to_string(),
    ];

    // All categorical (backward compatible)
    let key = compute_segment_key(&row, &dimensions, &[], &[4, 4]);
    assert!(key.0 < 4);
    assert!(key.1 < 4);
}
