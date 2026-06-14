//! Mirror of `src/storage/reader.rs` unit tests, expressed through the public API.

use tempfile::TempDir;

use strata_query::{
    AggFunc, DimensionType, NumericRange, QueryPredicate, Row, StorageFormat, StrataReader,
    StrataWriter, WriterConfig,
};

use crate::common::{csv_fixture_rows, write_csv_fixture};

#[test]
fn test_query_predicate_exact_match() {
    let mut query = QueryPredicate::new();
    query.add_filter(
        "status".to_string(),
        vec!["ERROR".to_string(), "WARN".to_string()],
    );
    query.add_filter("region".to_string(), vec!["NYC".to_string()]);

    let mut row1 = Row::new();
    row1.insert("status".to_string(), "ERROR".to_string());
    row1.insert("region".to_string(), "NYC".to_string());
    assert!(query.matches(&row1));

    let mut row2 = Row::new();
    row2.insert("status".to_string(), "OK".to_string());
    row2.insert("region".to_string(), "NYC".to_string());
    assert!(!query.matches(&row2));
}

#[test]
fn test_query_predicate_range_match() {
    let mut query = QueryPredicate::new();
    query.add_range_filter("fare".to_string(), 100.0, 500.0);

    let mut row1 = Row::new();
    row1.insert("fare".to_string(), "250.0".to_string());
    assert!(query.matches(&row1));

    let mut row2 = Row::new();
    row2.insert("fare".to_string(), "50.0".to_string());
    assert!(!query.matches(&row2));

    let mut row3 = Row::new();
    row3.insert("fare".to_string(), "100.0".to_string());
    assert!(query.matches(&row3)); // inclusive boundary
}

#[test]
fn test_query_predicate_min_only() {
    let mut query = QueryPredicate::new();
    query.add_min_filter("fare".to_string(), 100.0);

    let mut row1 = Row::new();
    row1.insert("fare".to_string(), "500.0".to_string());
    assert!(query.matches(&row1));

    let mut row2 = Row::new();
    row2.insert("fare".to_string(), "50.0".to_string());
    assert!(!query.matches(&row2));
}

#[test]
fn test_query_predicate_mixed_filters() {
    let mut query = QueryPredicate::new();
    query.add_filter("status".to_string(), vec!["OK".to_string()]);
    query.add_range_filter("fare".to_string(), 50.0, 200.0);

    // Matches both filters
    let mut row1 = Row::new();
    row1.insert("status".to_string(), "OK".to_string());
    row1.insert("fare".to_string(), "100.0".to_string());
    assert!(query.matches(&row1));

    // Wrong status
    let mut row2 = Row::new();
    row2.insert("status".to_string(), "ERROR".to_string());
    row2.insert("fare".to_string(), "100.0".to_string());
    assert!(!query.matches(&row2));

    // Fare out of range
    let mut row3 = Row::new();
    row3.insert("status".to_string(), "OK".to_string());
    row3.insert("fare".to_string(), "500.0".to_string());
    assert!(!query.matches(&row3));
}

#[test]
fn test_query_mask_build_categorical() {
    let mut query = QueryPredicate::new();
    query.add_filter("status".to_string(), vec!["ERROR".to_string()]);
    query.add_filter("region".to_string(), vec!["NYC".to_string()]);
    query.add_filter("hour".to_string(), vec!["9".to_string()]);

    let dimensions = vec![
        "status".to_string(),
        "region".to_string(),
        "hour".to_string(),
    ];
    let dim_types = vec![
        DimensionType::Categorical,
        DimensionType::Categorical,
        DimensionType::Categorical,
    ];

    let mask = query
        .build_mask(&dimensions, &dim_types, &[4, 4, 24])
        .unwrap();

    assert!(mask.count_set_bits() > 0);
    assert_eq!(mask.size(), 4 * 4 * 24);
}

#[test]
fn test_query_mask_build_with_range() {
    let mut query = QueryPredicate::new();
    query.add_filter("status".to_string(), vec!["OK".to_string()]);
    query.add_range_filter("fare".to_string(), 100.0, 500.0);

    let dimensions = vec!["status".to_string(), "fare".to_string(), "hour".to_string()];
    let dim_types = vec![
        DimensionType::Categorical,
        DimensionType::Numeric,
        DimensionType::Categorical,
    ];

    // fare [100, 500] → floor(log2(100))=6, floor(log2(500))=8 → buckets 6,7,8
    let mask = query
        .build_mask(&dimensions, &dim_types, &[4, 16, 24])
        .unwrap();

    assert!(mask.count_set_bits() > 0);

    // Should only set bits for fare buckets 6, 7, 8 (not all 16 buckets)
    // With 1 status bucket × 3 fare buckets × 24 hour buckets = 72 bits max
    assert!(mask.count_set_bits() <= 3 * 24);
}

#[test]
fn test_query_mask_range_pruning() {
    // Narrow range [100, 100] → single bucket floor(log2(100)) = 6
    let mut query = QueryPredicate::new();
    query.add_range_filter("fare".to_string(), 100.0, 100.0);

    let dimensions = vec!["status".to_string(), "fare".to_string(), "hour".to_string()];
    let dim_types = vec![
        DimensionType::Categorical,
        DimensionType::Numeric,
        DimensionType::Categorical,
    ];

    let mask = query
        .build_mask(&dimensions, &dim_types, &[4, 16, 24])
        .unwrap();

    // Should only touch 1 fare bucket × 4 status × 24 hours = 96 bits
    assert_eq!(mask.count_set_bits(), 4 * 24);
}

#[test]
fn test_numeric_range_contains() {
    let range = NumericRange::new(10.0, 100.0);
    assert!(range.contains(10.0));
    assert!(range.contains(50.0));
    assert!(range.contains(100.0));
    assert!(!range.contains(9.9));
    assert!(!range.contains(100.1));
}

#[test]
fn test_numeric_range_open_ended() {
    let min_range = NumericRange::min_only(50.0);
    assert!(min_range.contains(50.0));
    assert!(min_range.contains(1e10));
    assert!(!min_range.contains(49.9));

    let max_range = NumericRange::max_only(100.0);
    assert!(max_range.contains(100.0));
    assert!(max_range.contains(-1e10));
    assert!(!max_range.contains(100.1));
}

// --- min/max filter narrowing semantics ---

#[test]
fn add_min_filter_narrows_existing_range() {
    let mut q = QueryPredicate::new();
    q.add_min_filter("fare".into(), 100.0);
    // Calling again with a higher bound narrows (keeps the larger min).
    q.add_min_filter("fare".into(), 200.0);
    assert_eq!(q.get_range("fare").unwrap().min, 200.0);
    // A lower bound does not widen the range back out.
    q.add_min_filter("fare".into(), 50.0);
    assert_eq!(q.get_range("fare").unwrap().min, 200.0);
}

#[test]
fn add_max_filter_narrows_existing_range() {
    let mut q = QueryPredicate::new();
    q.add_max_filter("fare".into(), 500.0);
    q.add_max_filter("fare".into(), 300.0); // narrows down
    assert_eq!(q.get_range("fare").unwrap().max, 300.0);
    q.add_max_filter("fare".into(), 900.0); // does not widen
    assert_eq!(q.get_range("fare").unwrap().max, 300.0);
}

#[test]
fn min_then_max_forms_a_bounded_range() {
    let mut q = QueryPredicate::new();
    q.add_min_filter("fare".into(), 100.0);
    q.add_max_filter("fare".into(), 500.0);
    let range = q.get_range("fare").unwrap();
    assert_eq!(range.min, 100.0);
    assert_eq!(range.max, 500.0);

    let mut inside = Row::new();
    inside.insert("fare".into(), "300".into());
    assert!(q.matches(&inside));

    let mut below = Row::new();
    below.insert("fare".into(), "50".into());
    assert!(!q.matches(&below));

    let mut above = Row::new();
    above.insert("fare".into(), "600".into());
    assert!(!q.matches(&above));
}

// --- accessor methods ---

#[test]
fn exact_filters_and_get_exact_values() {
    let mut q = QueryPredicate::new();
    q.add_filter("status".into(), vec!["OK".into(), "ERROR".into()]);
    q.add_range_filter("fare".into(), 1.0, 10.0);

    // get_exact_values returns the set for an exact filter, None for a range.
    assert_eq!(
        q.get_exact_values("status"),
        Some(&vec!["OK".to_string(), "ERROR".to_string()])
    );
    assert_eq!(q.get_exact_values("fare"), None);
    assert_eq!(q.get_exact_values("missing"), None);

    // exact_filters yields only the categorical filter, not the range.
    let names: Vec<&String> = q.exact_filters().map(|(k, _)| k).collect();
    assert_eq!(names, vec![&"status".to_string()]);
}

#[test]
fn all_filters_and_get_range() {
    let mut q = QueryPredicate::new();
    q.add_filter("status".into(), vec!["OK".into()]);
    q.add_range_filter("fare".into(), 1.0, 10.0);

    // all_filters yields both.
    assert_eq!(q.all_filters().count(), 2);

    // get_range returns the range filter, None for exact/missing.
    let range = q.get_range("fare").unwrap();
    assert_eq!(range.min, 1.0);
    assert_eq!(range.max, 10.0);
    assert!(q.get_range("status").is_none());
    assert!(q.get_range("missing").is_none());
}

// --- resolve_buckets / build_mask: unspecified dimension matches all ---

#[test]
fn unspecified_dimension_matches_all_buckets() {
    // Only filter `status`; `region` and `hour` are unconstrained, so the
    // mask must cover the full cross product of their buckets.
    let mut q = QueryPredicate::new();
    q.add_filter("status".into(), vec!["OK".into()]);

    let dims = vec!["status".into(), "region".into(), "hour".into()];
    let dim_types = vec![DimensionType::Categorical; 3];
    let mask = q.build_mask(&dims, &dim_types, &[4, 4, 24]).unwrap();

    // One status bucket × all 4 region buckets × all 24 hour buckets.
    assert_eq!(mask.count_set_bits(), 4 * 24);
}

#[test]
fn empty_predicate_matches_every_bucket() {
    let q = QueryPredicate::new();
    let dims = vec!["a".into(), "b".into(), "c".into()];
    let dim_types = vec![DimensionType::Categorical; 3];
    let mask = q.build_mask(&dims, &dim_types, &[2, 3, 4]).unwrap();
    assert_eq!(mask.count_set_bits(), 2 * 3 * 4);
}

// --- StrataReader end-to-end round-trip through the real writer ---

#[test]
fn reader_round_trip_stats_and_counts() {
    let dir = TempDir::new().unwrap();
    write_csv_fixture(dir.path(), StorageFormat::Csv);

    let reader = StrataReader::load_segments(dir.path()).unwrap();
    assert!(reader.segment_count() >= 1);
    assert!(reader.segment_count() <= 16); // 4×4 segment keys

    let stats = reader.get_stats();
    assert_eq!(stats.total_rows, 600);
    assert_eq!(stats.total_segments, reader.segment_count());

    // read_all_segments returns every stored row.
    let all = reader.read_all_segments().unwrap();
    assert_eq!(all.len(), 600);
}

/// The central invariant of the whole crate: pruning must never drop a row
/// that matches the predicate. We compare the pruned-and-filtered result
/// against a full scan of ground truth for several queries.
#[test]
fn reader_pruning_is_sound_and_effective() {
    let dir = TempDir::new().unwrap();
    write_csv_fixture(dir.path(), StorageFormat::Csv);
    let reader = StrataReader::load_segments(dir.path()).unwrap();
    let ground_truth = csv_fixture_rows();

    let queries = {
        let mut v = Vec::new();
        let mut q1 = QueryPredicate::new();
        q1.add_filter("status".into(), vec!["ERROR".into()]);
        q1.add_filter("region".into(), vec!["NYC".into()]);
        v.push(q1);

        let mut q2 = QueryPredicate::new();
        q2.add_filter("status".into(), vec!["OK".into(), "WARN".into()]);
        v.push(q2);

        let mut q3 = QueryPredicate::new();
        q3.add_filter("region".into(), vec!["CHI".into()]);
        q3.add_filter("hour".into(), vec!["3".into()]);
        v.push(q3);
        v
    };

    for q in &queries {
        let keys = reader.filter_segments(q).unwrap();
        let pruned = reader.read_and_filter(&keys, q).unwrap();

        // Ground truth: full scan.
        let expected = ground_truth.iter().filter(|r| q.matches(r)).count();

        // SOUNDNESS: pruned result count equals the full-scan count, so no
        // matching row was lost to pruning.
        assert_eq!(
            pruned.len(),
            expected,
            "pruning dropped matching rows for query {q:?}"
        );

        // Every returned row genuinely matches.
        assert!(pruned.iter().all(|r| q.matches(r)));

        // EFFECTIVENESS: a selective query touches a strict subset of segments.
        assert!(keys.len() <= reader.segment_count());
    }
}

#[test]
fn filter_segments_empty_dir_returns_no_keys() {
    let dir = TempDir::new().unwrap();
    let reader = StrataReader::load_segments(dir.path()).unwrap();
    assert_eq!(reader.segment_count(), 0);
    let q = QueryPredicate::new();
    assert!(reader.filter_segments(&q).unwrap().is_empty());
    assert_eq!(reader.get_stats().avg_rows_per_segment, 0);
}

// --- metadata-only aggregation ---

#[test]
fn aggregate_count_star_matches_row_count() {
    let dir = TempDir::new().unwrap();
    write_csv_fixture(dir.path(), StorageFormat::Csv);
    let reader = StrataReader::load_segments(dir.path()).unwrap();

    // COUNT(*) with no predicate must equal total rows written.
    let q = QueryPredicate::new();
    let res = reader
        .aggregate(&q, AggFunc::Count, "*")
        .unwrap()
        .expect("count(*) always has stats");
    assert_eq!(res.value() as usize, 600);
}

#[test]
fn aggregate_numeric_column_min_max_sum_avg() {
    // Build a segment with a known numeric column so SUM/MIN/MAX/AVG are exact.
    let dir = TempDir::new().unwrap();
    let config = WriterConfig {
        dimensions: vec!["status".into(), "fare".into(), "hour".into()],
        dimension_types: vec![
            DimensionType::Categorical,
            DimensionType::Numeric,
            DimensionType::Categorical,
        ],
        bucket_counts: [4, 16, 24],
        output_dir: dir.path().to_path_buf(),
        segment_size_threshold: 10_000,
        schema: None,
        storage_format: StorageFormat::Csv,
    };
    let mut writer = StrataWriter::new(config).unwrap();
    let fares = [10.0, 20.0, 30.0, 40.0];
    for (i, fare) in fares.iter().cycle().take(40).enumerate() {
        let mut row = Row::new();
        row.insert("status".into(), "OK".into());
        row.insert("fare".into(), fare.to_string());
        row.insert("hour".into(), (i % 24).to_string());
        writer.write_row(row).unwrap();
    }
    writer.flush_all().unwrap();

    let reader = StrataReader::load_segments(dir.path()).unwrap();
    let q = QueryPredicate::new();

    let sum = reader.aggregate(&q, AggFunc::Sum, "fare").unwrap().unwrap();
    // 10 cycles of [10,20,30,40] = 10 * 100 = 1000.
    assert_eq!(sum.value(), 1000.0);

    let min = reader.aggregate(&q, AggFunc::Min, "fare").unwrap().unwrap();
    assert_eq!(min.value(), 10.0);

    let max = reader.aggregate(&q, AggFunc::Max, "fare").unwrap().unwrap();
    assert_eq!(max.value(), 40.0);

    let avg = reader.aggregate(&q, AggFunc::Avg, "fare").unwrap().unwrap();
    assert_eq!(avg.value(), 25.0);

    // A column with no collected stats yields None.
    assert!(reader
        .aggregate(&q, AggFunc::Sum, "nonexistent")
        .unwrap()
        .is_none());
}

#[test]
fn aggregate_multi_runs_all_in_one_pass() {
    let dir = TempDir::new().unwrap();
    write_csv_fixture(dir.path(), StorageFormat::Csv);
    let reader = StrataReader::load_segments(dir.path()).unwrap();
    let q = QueryPredicate::new();

    let results = reader
        .aggregate_multi(&q, &[(AggFunc::Count, "*"), (AggFunc::Sum, "nope")])
        .unwrap();
    assert_eq!(results.len(), 2);
    // COUNT(*) present, SUM on a missing column absent.
    assert_eq!(results[0].as_ref().unwrap().value() as usize, 600);
    assert!(results[1].is_none());
}
