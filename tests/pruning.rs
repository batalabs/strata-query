//! End-to-end pruning and crash-recovery tests over the public crate surface.
//!
//! Folded in from the former top-level `tests/integration_test.rs` (synthetic
//! and skewed-data pruning effectiveness, hash distribution) and
//! `tests/pruning_recovery_test.rs` (WAL crash-recovery roundtrips and the
//! Parquet multi-column pruning property). These exercise behavior the per-module
//! unit tests can't reach: whole-pipeline pruning rates and recovery semantics.

use anyhow::Result;
use tempfile::TempDir;

use strata_query::test_data::{
    analyze_data_distribution, generate_skewed_data_seeded, generate_synthetic_data_seeded,
};
use strata_query::{
    parse_query, DimensionType, ParquetStrataReader, ParquetStrataWriter, ParquetWriterConfig,
    QueryPredicate, Row, StorageFormat, StrataReader, StrataWriter, WriterConfig,
};

/// Fixed seed for all data generation in this suite. A seeded RNG keeps the
/// statistical assertions below (pruning rates, hash distribution) reproducible.
const TEST_SEED: u64 = 0x5734_2A1C_u64;

/// Build a 3-categorical-dimension CSV writer config rooted at `dir`.
fn csv_config(dir: &std::path::Path, threshold: usize) -> WriterConfig {
    WriterConfig {
        dimensions: vec!["status".into(), "region".into(), "hour".into()],
        dimension_types: vec![DimensionType::Categorical; 3],
        bucket_counts: [4, 4, 24],
        output_dir: dir.to_path_buf(),
        segment_size_threshold: threshold,
        schema: None,
        storage_format: StorageFormat::Csv,
    }
}

fn row3(status: &str, region: &str, hour: &str) -> Row {
    let mut row = Row::new();
    row.insert("status".into(), status.into());
    row.insert("region".into(), region.into());
    row.insert("hour".into(), hour.into());
    row
}

// ---------------------------------------------------------------------------
// Synthetic-data pruning effectiveness (was integration_test.rs)
// ---------------------------------------------------------------------------

/// End-to-end: correctness (all matching rows returned) AND pruning
/// effectiveness (selective queries touch far fewer than all segments).
#[test]
fn end_to_end_pruning_is_correct_and_effective() -> Result<()> {
    let data = generate_synthetic_data_seeded(10_000, TEST_SEED);
    let data_copy = data.clone();

    let temp_dir = TempDir::new()?;
    let mut writer = StrataWriter::new(csv_config(temp_dir.path(), 1000))?;
    for row in data {
        writer.write_row(row)?;
    }
    writer.flush_all()?;

    let reader = StrataReader::load_segments(temp_dir.path())?;
    let reader_stats = reader.get_stats();
    assert_eq!(reader_stats.total_rows, 10_000, "all rows should be stored");

    // Query 1: single value per dimension (most selective).
    let mut query1 = QueryPredicate::new();
    query1.add_filter("status".into(), vec!["ERROR".into()]);
    query1.add_filter("region".into(), vec!["NYC".into()]);
    query1.add_filter("hour".into(), vec!["9".into()]);

    let keys1 = reader.filter_segments(&query1)?;
    let results1 = reader.read_and_filter(&keys1, &query1)?;
    let truth1 = data_copy.iter().filter(|r| query1.matches(r)).count();
    assert_eq!(results1.len(), truth1, "Query 1 must match ground truth");

    let pruning_rate1 = keys1.len() as f64 / reader_stats.total_segments as f64 * 100.0;
    assert!(
        pruning_rate1 < 50.0,
        "Query 1 should touch < 50% of segments (got {pruning_rate1:.1}%)"
    );

    // Query 2: multiple values per dimension (less selective).
    let mut query2 = QueryPredicate::new();
    query2.add_filter("status".into(), vec!["ERROR".into(), "WARN".into()]);
    query2.add_filter("region".into(), vec!["NYC".into(), "SF".into()]);
    query2.add_filter("hour".into(), vec!["9".into(), "10".into(), "11".into()]);

    let keys2 = reader.filter_segments(&query2)?;
    let results2 = reader.read_and_filter(&keys2, &query2)?;
    let truth2 = data_copy.iter().filter(|r| query2.matches(r)).count();
    assert_eq!(results2.len(), truth2, "Query 2 must match ground truth");

    // Query 3: very selective (single hour).
    let mut query3 = QueryPredicate::new();
    query3.add_filter("status".into(), vec!["ERROR".into()]);
    query3.add_filter("region".into(), vec!["LA".into()]);
    query3.add_filter("hour".into(), vec!["15".into()]);

    let keys3 = reader.filter_segments(&query3)?;
    let results3 = reader.read_and_filter(&keys3, &query3)?;
    let truth3 = data_copy.iter().filter(|r| query3.matches(r)).count();
    assert_eq!(results3.len(), truth3, "Query 3 must match ground truth");

    // Significant improvement vs naive full scan for the selective query.
    let improvement1 = reader_stats.total_segments as f64 / keys1.len().max(1) as f64;
    assert!(
        improvement1 > 2.0,
        "selective query should see >= 2x fewer segments (got {improvement1:.1}x)"
    );

    Ok(())
}

/// Skewed (more realistic) distribution: a rare-combination query must still
/// return exactly the ground-truth matches.
#[test]
fn skewed_data_pruning_matches_ground_truth() -> Result<()> {
    let data = generate_skewed_data_seeded(10_000, TEST_SEED);
    let data_copy = data.clone();

    let temp_dir = TempDir::new()?;
    let mut writer = StrataWriter::new(csv_config(temp_dir.path(), 500))?;
    for row in data {
        writer.write_row(row)?;
    }
    writer.flush_all()?;

    let reader = StrataReader::load_segments(temp_dir.path())?;

    let mut rare_query = QueryPredicate::new();
    rare_query.add_filter("status".into(), vec!["WARN".into()]);
    rare_query.add_filter("region".into(), vec!["CHI".into()]);
    rare_query.add_filter("hour".into(), vec!["3".into()]);

    let keys = reader.filter_segments(&rare_query)?;
    let results = reader.read_and_filter(&keys, &rare_query)?;
    let truth = data_copy.iter().filter(|r| rare_query.matches(r)).count();
    assert_eq!(results.len(), truth);

    Ok(())
}

/// Hash routing must distribute 10K rows across multiple (but at most R*S)
/// segments.
#[test]
fn hash_routing_distributes_across_segments() -> Result<()> {
    let data = generate_synthetic_data_seeded(10_000, TEST_SEED);
    let temp_dir = TempDir::new()?;
    // Keep all data in the initial segments (no threshold flush splitting).
    let mut writer = StrataWriter::new(csv_config(temp_dir.path(), 10_000))?;
    for row in data {
        writer.write_row(row)?;
    }
    writer.flush_all()?;

    let reader = StrataReader::load_segments(temp_dir.path())?;
    // Sanity: the seeded data really does spread across statuses/regions.
    let stats = analyze_data_distribution(&generate_synthetic_data_seeded(10_000, TEST_SEED));
    assert!(stats.status_distribution.len() >= 2);

    assert!(
        reader.segment_count() >= 3,
        "hash routing should create multiple segments"
    );
    assert!(
        reader.segment_count() <= 16,
        "should not exceed R*S = 16 segments"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// WAL crash recovery (was pruning_recovery_test.rs)
// ---------------------------------------------------------------------------

/// Simulate a crash: write rows but never call `flush_all`, so the WAL is left
/// non-empty. A fresh writer on the same dir must replay the WAL and the rows
/// must become queryable after a normal flush.
#[test]
fn wal_recovers_unflushed_rows_after_crash() -> Result<()> {
    let dir = TempDir::new()?;

    // High threshold so nothing flushes to a segment during the "crashed" run;
    // the only durable record of these rows is the WAL.
    {
        let mut writer = StrataWriter::new(csv_config(dir.path(), usize::MAX))?;
        writer.write_row(row3("ERROR", "NYC", "9"))?;
        writer.write_row(row3("ERROR", "NYC", "9"))?;
        writer.write_row(row3("OK", "LA", "14"))?;
        // Drop WITHOUT flush_all() → simulated crash. WAL stays on disk.
    }

    // A WAL must have survived the crash.
    let wal_path = dir.path().join("wal.log");
    assert!(
        wal_path.exists(),
        "WAL should persist after a crash (no flush_all)"
    );

    // Recovery: a fresh writer replays the WAL and flushes recovered rows.
    {
        let mut writer = StrataWriter::new(csv_config(dir.path(), usize::MAX))?;
        writer.flush_all()?;
    }

    // The recovered rows must now be queryable.
    let reader = StrataReader::load_segments(dir.path())?;
    assert_eq!(
        reader.get_stats().total_rows,
        3,
        "all 3 in-flight rows must be recovered from the WAL"
    );

    let parsed = parse_query("SELECT * FROM t WHERE status = 'ERROR' AND region = 'NYC'")?;
    let keys = reader.filter_segments(&parsed.predicate)?;
    let rows = reader.read_and_filter(&keys, &parsed.predicate)?;
    assert_eq!(rows.len(), 2, "both ERROR/NYC rows must survive recovery");

    Ok(())
}

/// A clean shutdown (`flush_all`) truncates the WAL, and reopening the directory
/// must NOT double-count rows (no replay of already-persisted data).
#[test]
fn clean_flush_truncates_wal_and_does_not_double_count() -> Result<()> {
    let dir = TempDir::new()?;

    {
        let mut writer = StrataWriter::new(csv_config(dir.path(), 1000))?;
        for i in 0..50 {
            writer.write_row(row3("OK", "NYC", &format!("{}", i % 24)))?;
        }
        writer.flush_all()?; // clean shutdown → WAL truncated
    }

    assert!(
        !dir.path().join("wal.log").exists()
            || std::fs::metadata(dir.path().join("wal.log"))?.len() == 0,
        "WAL must be empty/absent after a clean flush"
    );

    // Reopen and flush again: must not replay or duplicate the 50 rows.
    {
        let mut writer = StrataWriter::new(csv_config(dir.path(), 1000))?;
        writer.flush_all()?;
    }

    let reader = StrataReader::load_segments(dir.path())?;
    assert_eq!(
        reader.get_stats().total_rows,
        50,
        "rows must not be double-counted after a clean reopen"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Parquet multi-column pruning property (was pruning_recovery_test.rs)
// ---------------------------------------------------------------------------

/// Multi-column pruning property on the Parquet path: for a two-dimension
/// equality query the pruned result must equal the full-scan ground truth
/// (soundness) AND must read strictly fewer segments than exist (effectiveness),
/// verified across the (r/R)×(s/S) grid.
#[test]
fn parquet_multi_column_pruning_is_sound_and_effective() -> Result<()> {
    let dir = TempDir::new()?;

    let config = ParquetWriterConfig {
        dimensions: vec!["status".into(), "region".into(), "hour".into()],
        dimension_types: vec![DimensionType::Categorical; 3],
        bucket_counts: [4, 4, 24],
        output_dir: dir.path().to_path_buf(),
        segment_size_threshold: 1000,
        compression: parquet::basic::Compression::SNAPPY,
        schema: None,
    };

    // Spread rows across the full status×region grid so many segments exist.
    let statuses = ["OK", "ERROR", "WARN", "FATAL"];
    let regions = ["NYC", "LA", "SF", "CHI"];
    let mut writer = ParquetStrataWriter::new(config)?;
    for i in 0..2000usize {
        writer.write_row(row3(
            statuses[i % statuses.len()],
            regions[(i / statuses.len()) % regions.len()],
            &format!("{}", i % 24),
        ))?;
    }
    writer.flush_all()?;

    let reader = ParquetStrataReader::load_segments(dir.path())?;
    let total_segments = reader.segment_count();
    assert!(total_segments > 4, "fixture must span many segments");

    let all_keys = reader.filter_segments(&QueryPredicate::new())?;
    let all_rows = reader.read_segments(&all_keys)?;

    for status in &statuses {
        for region in &regions {
            let mut q = QueryPredicate::new();
            q.add_filter("status".into(), vec![(*status).into()]);
            q.add_filter("region".into(), vec![(*region).into()]);

            let pruned_keys = reader.filter_segments(&q)?;
            let pruned_rows = reader.read_and_filter(&pruned_keys, &q)?;
            let ground_truth = all_rows.iter().filter(|r| q.matches(r)).count();

            assert_eq!(
                pruned_rows.len(),
                ground_truth,
                "pruning false negative for status={status}, region={region}"
            );

            assert!(
                pruned_keys.len() < total_segments,
                "no pruning happened for status={status}, region={region} \
                 ({} of {} segments scanned)",
                pruned_keys.len(),
                total_segments
            );
        }
    }

    Ok(())
}

/// Parquet `count_exact` over the public API must equal the number of rows the
/// scan path returns for the same predicate, pinning the two count paths
/// against each other end-to-end.
#[test]
fn parquet_count_exact_agrees_with_scan_end_to_end() -> Result<()> {
    let dir = TempDir::new()?;
    let config = ParquetWriterConfig {
        dimensions: vec!["status".into(), "region".into(), "hour".into()],
        dimension_types: vec![DimensionType::Categorical; 3],
        bucket_counts: [4, 4, 24],
        output_dir: dir.path().to_path_buf(),
        segment_size_threshold: 1000,
        compression: parquet::basic::Compression::SNAPPY,
        schema: None,
    };
    let mut writer = ParquetStrataWriter::new(config)?;
    for i in 0..500usize {
        writer.write_row(row3(
            if i % 7 == 0 { "ERROR" } else { "OK" },
            "NYC",
            &format!("{}", i % 24),
        ))?;
    }
    writer.flush_all()?;

    let reader = ParquetStrataReader::load_segments(dir.path())?;
    let mut q = QueryPredicate::new();
    q.add_filter("status".into(), vec!["ERROR".into()]);
    q.add_filter("region".into(), vec!["NYC".into()]);

    let keys = reader.filter_segments(&q)?;
    let scanned = reader.read_and_filter(&keys, &q)?.len() as u64;
    let counted = reader.count_exact(&q)?;
    assert_eq!(counted, scanned);
    // 500 rows, every 7th is ERROR → ceil(500/7) = 72.
    assert_eq!(counted, 72);
    Ok(())
}
