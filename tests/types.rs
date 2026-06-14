//! Mirror of `src/types.rs` unit tests, expressed through the public API.

use std::collections::HashMap;

use strata_query::{
    ColumnStats, ColumnType, DimensionType, ExistenceBitset, Row, SegmentKey, SegmentMetadata,
    TableSchema,
};

#[test]
fn test_existence_bitset() {
    let mut bitset = ExistenceBitset::new(100);
    assert_eq!(bitset.size(), 100);
    assert_eq!(bitset.count_set_bits(), 0);

    bitset.set(5);
    bitset.set(10);
    bitset.set(99);

    assert!(bitset.get(5));
    assert!(bitset.get(10));
    assert!(bitset.get(99));
    assert!(!bitset.get(0));
    assert!(!bitset.get(50));

    assert_eq!(bitset.count_set_bits(), 3);
}

#[test]
fn test_bitset_intersection() {
    let mut bitset1 = ExistenceBitset::new(100);
    let mut bitset2 = ExistenceBitset::new(100);

    bitset1.set(5);
    bitset1.set(10);

    bitset2.set(10);
    bitset2.set(20);

    // Should intersect at bit 10
    assert!(bitset1.intersects(&bitset2));

    let mut bitset3 = ExistenceBitset::new(100);
    bitset3.set(30);
    bitset3.set(40);

    // Should not intersect
    assert!(!bitset1.intersects(&bitset3));
}

#[test]
fn test_segment_key() {
    let key1 = SegmentKey::new(1, 2);
    let key2 = SegmentKey::new(1, 2);
    let key3 = SegmentKey::new(2, 1);

    assert_eq!(key1, key2);
    assert_ne!(key1, key3);
}

#[test]
fn test_row() {
    let mut row = Row::new();
    row.insert("status".to_string(), "OK".to_string());
    row.insert("region".to_string(), "NYC".to_string());

    assert_eq!(row.get("status"), Some(&"OK".to_string()));
    assert_eq!(row.get("region"), Some(&"NYC".to_string()));
    assert_eq!(row.get("missing"), None);
}

#[test]
fn test_segment_metadata() {
    let meta = SegmentMetadata::new(
        SegmentKey::new(1, 2),
        vec![
            "status".to_string(),
            "region".to_string(),
            "hour".to_string(),
        ],
        vec![
            DimensionType::Categorical,
            DimensionType::Categorical,
            DimensionType::Categorical,
        ],
        [8, 4, 24],
    );

    assert_eq!(meta.key, SegmentKey::new(1, 2));
    assert_eq!(meta.bitset_size(), 8 * 4 * 24);
    assert_eq!(meta.row_count, 0);
}

#[test]
fn segment_metadata_persists_schema() {
    let mut meta = SegmentMetadata::new(
        SegmentKey::new(0, 0),
        vec!["fare".to_string(), "status".to_string(), "hour".to_string()],
        vec![
            DimensionType::Numeric,
            DimensionType::Categorical,
            DimensionType::Categorical,
        ],
        [16, 4, 24],
    );
    meta.schema = Some(TableSchema::from_pairs(vec![
        ("fare", ColumnType::Float64),
        ("status", ColumnType::Utf8),
        ("hour", ColumnType::Int64),
    ]));

    let json = serde_json::to_string(&meta).unwrap();
    let back: SegmentMetadata = serde_json::from_str(&json).unwrap();

    let sch = back.schema.expect("schema should round-trip through JSON");
    assert_eq!(sch.column_type("fare"), Some(ColumnType::Float64));
    assert_eq!(sch.column_type("hour"), Some(ColumnType::Int64));
    assert_eq!(sch.column_type("status"), Some(ColumnType::Utf8));
}

// --- ExistenceBitset edge cases ---

#[test]
fn bitset_set_get_out_of_bounds_is_safe() {
    let mut bitset = ExistenceBitset::new(8);
    // Setting beyond `size` must be a no-op, not a panic.
    bitset.set(8);
    bitset.set(1000);
    // Reading beyond `size` must return false, not panic.
    assert!(!bitset.get(8));
    assert!(!bitset.get(1000));
    assert_eq!(bitset.count_set_bits(), 0);
}

#[test]
fn bitset_intersects_requires_equal_size() {
    let mut a = ExistenceBitset::new(16);
    let mut b = ExistenceBitset::new(8);
    a.set(3);
    b.set(3);
    // Even though bit 3 is set in both, differing sizes => no intersection.
    assert!(!a.intersects(&b));

    // Same size, shared bit => intersects.
    let mut c = ExistenceBitset::new(16);
    c.set(3);
    assert!(a.intersects(&c));

    // Same size, disjoint bits => no intersection.
    let mut d = ExistenceBitset::new(16);
    d.set(4);
    assert!(!a.intersects(&d));
}

#[test]
fn bitset_merge_is_bitwise_or() {
    let mut a = ExistenceBitset::new(16);
    a.set(1);
    a.set(2);

    let mut b = ExistenceBitset::new(16);
    b.set(2); // overlap, must not double anything
    b.set(5);

    a.merge(&b);
    assert!(a.get(1));
    assert!(a.get(2));
    assert!(a.get(5));
    assert_eq!(a.count_set_bits(), 3);
}

#[test]
fn bitset_merge_handles_unequal_sizes() {
    // merge iterates over the min of the two sizes, so the larger bitset
    // only absorbs bits within the smaller's range.
    let mut big = ExistenceBitset::new(16);
    let mut small = ExistenceBitset::new(4);
    small.set(0);
    small.set(3);
    big.merge(&small);
    assert!(big.get(0));
    assert!(big.get(3));
    assert_eq!(big.count_set_bits(), 2);
}

#[test]
fn bitset_bytes_round_trip() {
    let mut bitset = ExistenceBitset::new(20);
    bitset.set(0);
    bitset.set(7); // byte boundary
    bitset.set(8);
    bitset.set(19);

    let bytes = bitset.to_bytes();
    let restored = ExistenceBitset::from_bytes(&bytes, 20);

    for i in 0..20 {
        assert_eq!(
            bitset.get(i),
            restored.get(i),
            "bit {i} differs after round-trip"
        );
    }
    assert_eq!(restored.count_set_bits(), 4);
}

// --- ColumnType::parse_as_f64 ---

#[test]
fn column_type_parse_int64() {
    assert_eq!(ColumnType::Int64.parse_as_f64("42"), Some(42.0));
    assert_eq!(ColumnType::Int64.parse_as_f64("-7"), Some(-7.0));
    // A float string is not a valid i64.
    assert_eq!(ColumnType::Int64.parse_as_f64("3.5"), None);
    assert_eq!(ColumnType::Int64.parse_as_f64("nope"), None);
}

#[test]
fn column_type_parse_float64() {
    assert_eq!(ColumnType::Float64.parse_as_f64("3.5"), Some(3.5));
    assert_eq!(ColumnType::Float64.parse_as_f64("100"), Some(100.0));
    assert_eq!(
        ColumnType::Float64.parse_as_f64("nan").map(|v| v.is_nan()),
        Some(true)
    );
    assert_eq!(ColumnType::Float64.parse_as_f64("junk"), None);
}

#[test]
fn column_type_parse_utf8_is_never_numeric() {
    assert_eq!(ColumnType::Utf8.parse_as_f64("123"), None);
    assert_eq!(ColumnType::Utf8.parse_as_f64("anything"), None);
}

#[test]
fn column_type_parse_boolean() {
    for truthy in ["true", "TRUE", "True", "1", "yes", "YES"] {
        assert_eq!(
            ColumnType::Boolean.parse_as_f64(truthy),
            Some(1.0),
            "{truthy} should be 1.0"
        );
    }
    for falsy in ["false", "FALSE", "0", "no", "NO"] {
        assert_eq!(
            ColumnType::Boolean.parse_as_f64(falsy),
            Some(0.0),
            "{falsy} should be 0.0"
        );
    }
    assert_eq!(ColumnType::Boolean.parse_as_f64("maybe"), None);
    assert_eq!(ColumnType::Boolean.parse_as_f64("2"), None);
}

// --- TableSchema ---

#[test]
fn table_schema_from_pairs_and_lookup() {
    let schema = TableSchema::from_pairs(vec![
        ("id", ColumnType::Int64),
        ("price", ColumnType::Float64),
        ("name", ColumnType::Utf8),
    ]);
    assert_eq!(schema.column_type("id"), Some(ColumnType::Int64));
    assert_eq!(schema.column_type("price"), Some(ColumnType::Float64));
    assert_eq!(schema.column_type("name"), Some(ColumnType::Utf8));
    // Absent column => None.
    assert_eq!(schema.column_type("missing"), None);
    // from_pairs marks every column nullable.
    assert!(schema.columns.iter().all(|c| c.nullable));
}

// --- Row typed accessors ---

#[test]
fn row_with_fields_constructs_from_map() {
    let mut map = HashMap::new();
    map.insert("a".to_string(), "1".to_string());
    map.insert("b".to_string(), "two".to_string());
    let row = Row::with_fields(map);
    assert_eq!(row.get("a"), Some(&"1".to_string()));
    assert_eq!(row.get("b"), Some(&"two".to_string()));
}

#[test]
fn row_get_i64_valid_and_invalid() {
    let mut row = Row::new();
    row.insert("n".into(), "42".into());
    row.insert("bad".into(), "3.5".into());
    row.insert("text".into(), "abc".into());
    assert_eq!(row.get_i64("n"), Some(42));
    assert_eq!(row.get_i64("bad"), None); // float is not i64
    assert_eq!(row.get_i64("text"), None);
    assert_eq!(row.get_i64("missing"), None);
}

#[test]
fn row_get_f64_valid_and_invalid() {
    let mut row = Row::new();
    row.insert("f".into(), "3.25".into());
    row.insert("i".into(), "10".into());
    row.insert("text".into(), "xyz".into());
    assert_eq!(row.get_f64("f"), Some(3.25));
    assert_eq!(row.get_f64("i"), Some(10.0));
    assert_eq!(row.get_f64("text"), None);
    assert_eq!(row.get_f64("missing"), None);
}

#[test]
fn row_get_bool_variants() {
    let mut row = Row::new();
    row.insert("t".into(), "TRUE".into());
    row.insert("one".into(), "1".into());
    row.insert("y".into(), "yes".into());
    row.insert("f".into(), "false".into());
    row.insert("zero".into(), "0".into());
    row.insert("n".into(), "no".into());
    row.insert("junk".into(), "maybe".into());
    assert_eq!(row.get_bool("t"), Some(true));
    assert_eq!(row.get_bool("one"), Some(true));
    assert_eq!(row.get_bool("y"), Some(true));
    assert_eq!(row.get_bool("f"), Some(false));
    assert_eq!(row.get_bool("zero"), Some(false));
    assert_eq!(row.get_bool("n"), Some(false));
    assert_eq!(row.get_bool("junk"), None);
    assert_eq!(row.get_bool("missing"), None);
}

#[test]
fn row_get_numeric_uses_type_hint() {
    let mut row = Row::new();
    row.insert("i".into(), "100".into());
    row.insert("f".into(), "2.5".into());
    row.insert("b".into(), "yes".into());
    row.insert("s".into(), "label".into());

    assert_eq!(row.get_numeric("i", Some(ColumnType::Int64)), Some(100.0));
    assert_eq!(row.get_numeric("f", Some(ColumnType::Float64)), Some(2.5));
    assert_eq!(row.get_numeric("b", Some(ColumnType::Boolean)), Some(1.0));
    // No hint => best-effort f64 parse.
    assert_eq!(row.get_numeric("f", None), Some(2.5));
    // Non-numeric text with no hint => None.
    assert_eq!(row.get_numeric("s", None), None);
    // Missing key => None regardless of hint.
    assert_eq!(row.get_numeric("missing", Some(ColumnType::Int64)), None);
}

// --- SegmentMetadata bucket counts ---

fn sample_metadata() -> SegmentMetadata {
    SegmentMetadata::new(
        SegmentKey::new(0, 0),
        vec!["a".into(), "b".into(), "c".into()],
        vec![DimensionType::Categorical; 3],
        [2, 2, 2], // 8 bitset slots
    )
}

#[test]
fn segment_metadata_bucket_count_in_and_out_of_range() {
    let mut meta = sample_metadata();
    assert_eq!(meta.get_bucket_count(0), 0);

    meta.increment_bucket_count(0);
    meta.increment_bucket_count(0);
    meta.increment_bucket_count(7); // last valid index
    assert_eq!(meta.get_bucket_count(0), 2);
    assert_eq!(meta.get_bucket_count(7), 1);

    // Out-of-range increment is a no-op; out-of-range get returns 0.
    meta.increment_bucket_count(8);
    meta.increment_bucket_count(999);
    assert_eq!(meta.get_bucket_count(8), 0);
    assert_eq!(meta.get_bucket_count(999), 0);
}

#[test]
fn segment_metadata_sum_bucket_counts() {
    let mut meta = sample_metadata();
    meta.increment_bucket_count(1);
    meta.increment_bucket_count(1);
    meta.increment_bucket_count(3);
    // Sum over [1, 3] = 2 + 1 = 3; index 5 contributes 0; out-of-range 100 contributes 0.
    assert_eq!(meta.sum_bucket_counts(&[1, 3, 5, 100]), 3);
    assert_eq!(meta.sum_bucket_counts(&[]), 0);
}

#[test]
fn segment_metadata_bitset_size_matches_product() {
    let meta = SegmentMetadata::new(
        SegmentKey::new(0, 0),
        vec!["a".into(), "b".into(), "c".into()],
        vec![DimensionType::Categorical; 3],
        [8, 4, 24],
    );
    assert_eq!(meta.bitset_size(), 8 * 4 * 24);
}

#[test]
fn segment_metadata_update_column_stats_numeric_vs_categorical() {
    let mut meta = SegmentMetadata::new(
        SegmentKey::new(0, 0),
        vec!["status".into(), "fare".into(), "hour".into()],
        vec![
            DimensionType::Categorical,
            DimensionType::Numeric,
            DimensionType::Categorical,
        ],
        [4, 16, 24],
    );
    let numeric_dims = [false, true, false];

    let mut row = Row::new();
    row.insert("status".into(), "OK".into());
    row.insert("fare".into(), "100.0".into());
    row.insert("hour".into(), "9".into());
    meta.update_column_stats(&row, &numeric_dims, None);

    let mut row2 = Row::new();
    row2.insert("status".into(), "ERROR".into());
    row2.insert("fare".into(), "300.0".into());
    row2.insert("hour".into(), "10".into());
    meta.update_column_stats(&row2, &numeric_dims, None);

    // Numeric dimension accumulates real min/max/sum.
    let fare = meta.column_stats.get("fare").expect("fare stats");
    assert!(fare.is_numeric);
    assert_eq!(fare.count, 2);
    assert_eq!(fare.sum, 400.0);
    assert_eq!(fare.min, 100.0);
    assert_eq!(fare.max, 300.0);

    // Categorical dimension only counts.
    let status = meta.column_stats.get("status").expect("status stats");
    assert!(!status.is_numeric);
    assert_eq!(status.count, 2);
    assert_eq!(status.sum, 0.0);
}

// --- ColumnStats ---

#[test]
fn column_stats_new_numeric_and_categorical_defaults() {
    let n = ColumnStats::new_numeric();
    assert!(n.is_numeric);
    assert_eq!(n.count, 0);
    assert_eq!(n.min, f64::MAX);
    assert_eq!(n.max, f64::MIN);

    let c = ColumnStats::new_categorical();
    assert!(!c.is_numeric);
    assert_eq!(c.count, 0);
    assert_eq!(c.min, 0.0);
    assert_eq!(c.max, 0.0);
}

#[test]
fn column_stats_update_numeric_tracks_min_max_sum() {
    let mut s = ColumnStats::new_numeric();
    s.update("10", true);
    s.update("3", true);
    s.update("20", true);
    assert_eq!(s.count, 3);
    assert_eq!(s.sum, 33.0);
    assert_eq!(s.min, 3.0);
    assert_eq!(s.max, 20.0);

    // Unparseable values still bump count but not numeric aggregates.
    s.update("oops", true);
    assert_eq!(s.count, 4);
    assert_eq!(s.sum, 33.0);
}

#[test]
fn column_stats_update_categorical_only_counts() {
    let mut s = ColumnStats::new_categorical();
    s.update("anything", false);
    s.update("else", false);
    assert_eq!(s.count, 2);
    assert_eq!(s.sum, 0.0);
    assert_eq!(s.min, 0.0);
    assert_eq!(s.max, 0.0);
}

#[test]
fn column_stats_update_with_type_uses_column_type() {
    let mut s = ColumnStats::new_numeric();
    // Boolean column: "yes" => 1.0, "no" => 0.0.
    s.update_with_type("yes", true, Some(ColumnType::Boolean));
    s.update_with_type("no", true, Some(ColumnType::Boolean));
    assert_eq!(s.count, 2);
    assert_eq!(s.sum, 1.0);
    assert_eq!(s.min, 0.0);
    assert_eq!(s.max, 1.0);

    // Int64 column ignores a float-looking string (not a valid i64).
    let mut t = ColumnStats::new_numeric();
    t.update_with_type("3.5", true, Some(ColumnType::Int64));
    assert_eq!(t.count, 1);
    assert_eq!(t.sum, 0.0); // 3.5 rejected by Int64 parse
}

#[test]
fn column_stats_merge_combines_aggregates() {
    let mut a = ColumnStats::new_numeric();
    a.update("5", true);
    a.update("15", true);

    let mut b = ColumnStats::new_numeric();
    b.update("2", true);
    b.update("30", true);

    a.merge(&b);
    assert_eq!(a.count, 4);
    assert_eq!(a.sum, 52.0);
    assert_eq!(a.min, 2.0);
    assert_eq!(a.max, 30.0);
}
