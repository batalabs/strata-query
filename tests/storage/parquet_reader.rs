//! Mirror of `src/storage/parquet_reader.rs` unit tests, expressed through the
//! public API.
//!
//! NOTE: the original inline `extract_numeric_bounds_covers_all_int_float_variants`
//! tested the private associated fn `ParquetStrataReader::extract_numeric_bounds`
//! against hand-constructed Parquet `Statistics`. As that test's own comment
//! noted, the categorical Parquet writer only emits Utf8 columns, so this branch
//! is never reached end-to-end; there is no faithful public-API path that
//! drives typed numeric Parquet stats through the reader and observes bound
//! extraction. It was dropped during the migration rather than weakened.

use std::path::Path;

use anyhow::Result;
use tempfile::TempDir;

use strata_query::{
    DimensionType, ParquetStrataReader, ParquetStrataWriter, ParquetWriterConfig, QueryPredicate,
    Row, SegmentKey,
};

fn cat_config(dir: &Path) -> ParquetWriterConfig {
    ParquetWriterConfig {
        dimensions: vec![
            "status".to_string(),
            "region".to_string(),
            "hour".to_string(),
        ],
        dimension_types: vec![DimensionType::Categorical; 3],
        bucket_counts: [4, 4, 24],
        output_dir: dir.to_path_buf(),
        segment_size_threshold: 100,
        compression: parquet::basic::Compression::SNAPPY,
        schema: None,
    }
}

fn row3(status: &str, region: &str, hour: &str) -> Row {
    let mut row = Row::new();
    row.insert("status".to_string(), status.to_string());
    row.insert("region".to_string(), region.to_string());
    row.insert("hour".to_string(), hour.to_string());
    row
}

/// Write the given rows with a fresh writer and return a loaded reader.
fn write_and_load(dir: &Path, rows: &[Row]) -> Result<ParquetStrataReader> {
    let mut writer = ParquetStrataWriter::new(cat_config(dir))?;
    for r in rows {
        writer.write_row(r.clone())?;
    }
    writer.flush_all()?;
    ParquetStrataReader::load_segments(dir)
}

#[test]
fn test_parquet_reader_basic() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let rows: Vec<Row> = (0..200)
        .map(|i| {
            row3(
                if i % 2 == 0 { "OK" } else { "ERROR" },
                "NYC",
                &format!("{}", i % 24),
            )
        })
        .collect();
    let reader = write_and_load(temp_dir.path(), &rows)?;
    assert!(reader.segment_count() > 0);

    let mut query = QueryPredicate::new();
    query.add_filter("status".to_string(), vec!["ERROR".to_string()]);
    query.add_filter("region".to_string(), vec!["NYC".to_string()]);
    query.add_filter("hour".to_string(), vec!["9".to_string()]);

    let keys = reader.filter_segments(&query)?;
    let results = reader.read_and_filter(&keys, &query)?;
    assert!(!results.is_empty());
    Ok(())
}

/// Round-trip: every written row must be readable back via `read_segments`
/// of all segment keys, with field values preserved exactly.
#[test]
fn round_trip_preserves_all_rows_and_values() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let rows = vec![
        row3("OK", "NYC", "9"),
        row3("ERROR", "LA", "14"),
        row3("WARN", "NYC", "3"),
    ];
    let reader = write_and_load(temp_dir.path(), &rows)?;

    // Read back every segment.
    let all_keys = reader.filter_segments(&QueryPredicate::new())?;
    let read_back = reader.read_segments(&all_keys)?;
    assert_eq!(read_back.len(), rows.len(), "row count must round-trip");

    // Each original (status,region,hour) triple must appear in the read-back.
    for original in &rows {
        let found = read_back.iter().any(|r| {
            r.get("status") == original.get("status")
                && r.get("region") == original.get("region")
                && r.get("hour") == original.get("hour")
        });
        assert!(found, "row {:?} did not round-trip", original.fields);
    }
    Ok(())
}

/// Loading an empty directory yields a reader with zero segments and empty
/// results for any query (no panic, no false positives).
#[test]
fn empty_directory_loads_zero_segments() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let reader = ParquetStrataReader::load_segments(temp_dir.path())?;
    assert_eq!(reader.segment_count(), 0);

    let mut query = QueryPredicate::new();
    query.add_filter("status".to_string(), vec!["OK".to_string()]);
    assert!(reader.filter_segments(&query)?.is_empty());
    assert_eq!(reader.count_from_metadata(&query)?, 0);
    assert_eq!(reader.count_exact(&query)?, 0);
    Ok(())
}

/// **Pruning soundness**: the set of rows returned after segment pruning must
/// equal the rows a full scan would return. Pruning may never drop a matching
/// row (no false negatives).
#[test]
fn pruning_has_no_false_negatives() -> Result<()> {
    let temp_dir = TempDir::new()?;
    // Spread data across many segments.
    let mut rows = Vec::new();
    for i in 0..400 {
        rows.push(row3(
            ["OK", "ERROR", "WARN"][i % 3],
            &format!("region_{}", i % 4),
            &format!("{}", i % 24),
        ));
    }
    let reader = write_and_load(temp_dir.path(), &rows)?;

    let mut query = QueryPredicate::new();
    query.add_filter("status".to_string(), vec!["ERROR".to_string()]);
    query.add_filter("region".to_string(), vec!["region_2".to_string()]);

    // Pruned path.
    let pruned_keys = reader.filter_segments(&query)?;
    let pruned_rows = reader.read_and_filter(&pruned_keys, &query)?;

    // Full-scan ground truth (read every segment, then filter).
    let all_keys = reader.filter_segments(&QueryPredicate::new())?;
    let full = reader.read_segments(&all_keys)?;
    let ground_truth = full.iter().filter(|r| query.matches(r)).count();

    assert_eq!(
        pruned_rows.len(),
        ground_truth,
        "pruning dropped matching rows (false negative)"
    );
    assert!(ground_truth > 0, "fixture must contain matches");

    // And pruning must actually prune something (fewer segments than total).
    assert!(
        pruned_keys.len() < reader.segment_count(),
        "a 2-dimension equality query should prune segments"
    );
    Ok(())
}

/// `count_exact` must agree with the number of rows `read_and_filter`
/// returns for the same predicate (both are exact, just different paths).
#[test]
fn count_exact_matches_read_and_filter() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut rows = Vec::new();
    for i in 0..300 {
        rows.push(row3(
            if i % 5 == 0 { "ERROR" } else { "OK" },
            "NYC",
            &format!("{}", i % 24),
        ));
    }
    let reader = write_and_load(temp_dir.path(), &rows)?;

    let mut query = QueryPredicate::new();
    query.add_filter("status".to_string(), vec!["ERROR".to_string()]);
    query.add_filter("region".to_string(), vec!["NYC".to_string()]);

    let keys = reader.filter_segments(&query)?;
    let scanned = reader.read_and_filter(&keys, &query)?.len() as u64;
    let counted = reader.count_exact(&query)?;
    assert_eq!(counted, scanned, "count_exact disagrees with row scan");
    // 60 ERROR rows expected (i % 5 == 0 over 300).
    assert_eq!(counted, 60);
    Ok(())
}

/// `count_from_metadata` is documented as an *upper bound* (approximate due
/// to hash collisions). It must be >= the exact count and never undercount.
#[test]
fn count_from_metadata_is_an_upper_bound() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut rows = Vec::new();
    for i in 0..300 {
        rows.push(row3(
            if i % 4 == 0 { "ERROR" } else { "OK" },
            ["NYC", "LA"][i % 2],
            &format!("{}", i % 24),
        ));
    }
    let reader = write_and_load(temp_dir.path(), &rows)?;

    let mut query = QueryPredicate::new();
    query.add_filter("status".to_string(), vec!["ERROR".to_string()]);
    query.add_filter("region".to_string(), vec!["NYC".to_string()]);
    query.add_filter("hour".to_string(), vec!["8".to_string()]);

    let approx = reader.count_from_metadata(&query)?;
    let exact = reader.count_exact(&query)?;
    assert!(
        approx >= exact,
        "metadata count {approx} undercounted exact {exact}"
    );
    Ok(())
}

/// An all-same-value segment: every row identical. Round-trip count and an
/// equality query on the constant value must return all rows.
#[test]
fn all_same_value_segment() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let rows: Vec<Row> = (0..50).map(|_| row3("OK", "NYC", "9")).collect();
    let reader = write_and_load(temp_dir.path(), &rows)?;

    // All identical rows route to a single segment.
    assert_eq!(reader.segment_count(), 1);

    let mut query = QueryPredicate::new();
    query.add_filter("status".to_string(), vec!["OK".to_string()]);
    query.add_filter("region".to_string(), vec!["NYC".to_string()]);
    query.add_filter("hour".to_string(), vec!["9".to_string()]);

    let keys = reader.filter_segments(&query)?;
    let got = reader.read_and_filter(&keys, &query)?;
    assert_eq!(got.len(), 50, "all 50 identical rows must match");
    Ok(())
}

/// A query with a value present in no segment prunes to nothing and reads
/// zero rows, and crucially returns an empty (not error) result.
#[test]
fn query_for_absent_value_returns_empty() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let rows: Vec<Row> = (0..40)
        .map(|i| row3("OK", "NYC", &format!("{}", i % 24)))
        .collect();
    let reader = write_and_load(temp_dir.path(), &rows)?;

    let mut query = QueryPredicate::new();
    // 'ERROR' was never written.
    query.add_filter("status".to_string(), vec!["ERROR".to_string()]);
    query.add_filter("region".to_string(), vec!["NYC".to_string()]);

    let keys = reader.filter_segments(&query)?;
    let got = reader.read_and_filter(&keys, &query)?;
    assert!(got.is_empty(), "absent value must yield no rows");
    Ok(())
}

/// `read_segments` for a key that has no file on disk must be a no-op, not an
/// error; exercises the `parquet_file.exists()` guard.
#[test]
fn read_missing_segment_key_is_empty_not_error() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let reader = write_and_load(temp_dir.path(), &[row3("OK", "NYC", "9")])?;
    // A key that almost certainly was not produced.
    let bogus = SegmentKey(250, 250);
    let rows = reader.read_segments(&[bogus])?;
    assert!(rows.is_empty());
    Ok(())
}

/// `get_file_sizes` must report one segment per file pair and non-zero parquet
/// bytes, with avg = total/count.
#[test]
fn file_size_stats_are_consistent() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let rows: Vec<Row> = (0..120)
        .map(|i| {
            row3(
                ["OK", "ERROR"][i % 2],
                &format!("r{}", i % 3),
                &format!("{}", i % 24),
            )
        })
        .collect();
    let reader = write_and_load(temp_dir.path(), &rows)?;

    let stats = reader.get_file_sizes()?;
    assert_eq!(stats.segment_count, reader.segment_count());
    assert!(
        stats.total_parquet_size > 0,
        "parquet files must have bytes"
    );
    assert!(stats.total_metadata_size > 0, "meta files must have bytes");
    assert_eq!(
        stats.avg_parquet_size,
        stats.total_parquet_size / stats.segment_count as u64
    );
    Ok(())
}
