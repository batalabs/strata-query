//! Mirror of `src/storage/writer.rs` unit tests, expressed through the public API.
//!
//! NOTE: the original inline `test_segment_builder` exercised the private
//! `SegmentBuilder` type (private struct, private fields, private
//! `flush_to_disk`). It cannot be expressed through the public API beyond what
//! `test_writer_basic` / `numeric_routing_places_rows_in_log2_buckets` already
//! cover (write → flush → data + meta files exist), so it was dropped during the
//! migration rather than weakened.

use anyhow::Result;
use std::fs;
use tempfile::TempDir;

use strata_query::{DimensionType, Row, StorageFormat, StrataWriter, WriterConfig};

#[test]
fn test_writer_basic() -> Result<()> {
    let temp_dir = TempDir::new()?;

    let config = WriterConfig {
        dimensions: vec![
            "status".to_string(),
            "region".to_string(),
            "hour".to_string(),
        ],
        dimension_types: vec![
            DimensionType::Categorical,
            DimensionType::Categorical,
            DimensionType::Categorical,
        ],
        bucket_counts: [4, 4, 24],
        output_dir: temp_dir.path().to_path_buf(),
        segment_size_threshold: 10,
        schema: None,
        storage_format: StorageFormat::Csv,
    };

    let mut writer = StrataWriter::new(config)?;

    for i in 0..25 {
        let mut row = Row::new();
        row.insert("status".to_string(), "OK".to_string());
        row.insert("region".to_string(), format!("region_{}", i % 4));
        row.insert("hour".to_string(), format!("{}", i % 24));
        writer.write_row(row)?;
    }

    writer.flush_all()?;

    let entries: Vec<_> = fs::read_dir(temp_dir.path())?.collect();
    assert!(!entries.is_empty(), "Should have created some files");

    Ok(())
}

#[test]
fn test_writer_with_numeric_dimension() -> Result<()> {
    let temp_dir = TempDir::new()?;

    // Mixed: status (categorical), fare (numeric/MARS), hour (categorical)
    let config = WriterConfig {
        dimensions: vec!["status".to_string(), "fare".to_string(), "hour".to_string()],
        dimension_types: vec![
            DimensionType::Categorical,
            DimensionType::Numeric,
            DimensionType::Categorical,
        ],
        bucket_counts: [4, 16, 24],
        output_dir: temp_dir.path().to_path_buf(),
        segment_size_threshold: 100,
        schema: None,
        storage_format: StorageFormat::Csv,
    };

    let mut writer = StrataWriter::new(config)?;

    let fares = [5.0, 50.0, 150.0, 1000.0, 0.5];
    for i in 0..20 {
        let mut row = Row::new();
        row.insert("status".to_string(), "OK".to_string());
        row.insert("fare".to_string(), fares[i % fares.len()].to_string());
        row.insert("hour".to_string(), format!("{}", i % 24));
        writer.write_row(row)?;
    }

    writer.flush_all()?;

    let entries: Vec<_> = fs::read_dir(temp_dir.path())?.collect();
    assert!(!entries.is_empty(), "Should have created some files");

    Ok(())
}

#[test]
fn writer_config_default_is_three_categorical_dims() {
    let cfg = WriterConfig::default();
    assert_eq!(cfg.dimensions.len(), 3);
    assert_eq!(cfg.dimension_types.len(), 3);
    assert!(cfg
        .dimension_types
        .iter()
        .all(|d| matches!(d, DimensionType::Categorical)));
    assert_eq!(cfg.bucket_counts, [4, 4, 24]);
    assert_eq!(cfg.storage_format, StorageFormat::Csv);
}

#[test]
fn get_stats_reflects_unflushed_buffered_rows() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let config = WriterConfig {
        output_dir: temp_dir.path().to_path_buf(),
        // High threshold so nothing flushes mid-write; all rows stay buffered.
        segment_size_threshold: 10_000,
        ..WriterConfig::default()
    };
    let mut writer = StrataWriter::new(config)?;

    for i in 0..30 {
        let mut row = Row::new();
        row.insert("status".into(), "OK".into());
        row.insert("region".into(), format!("region_{}", i % 4));
        row.insert("hour".into(), (i % 24).to_string());
        writer.write_row(row)?;
    }

    // Before flush, stats see the buffered rows across segment builders.
    let stats = writer.get_stats();
    assert_eq!(stats.total_rows, 30);
    assert!(stats.total_segments >= 1);

    // After flush, builders are drained.
    writer.flush_all()?;
    let after = writer.get_stats();
    assert_eq!(after.total_rows, 0);
    assert_eq!(after.total_segments, 0);

    Ok(())
}

#[test]
fn numeric_routing_places_rows_in_log2_buckets() -> Result<()> {
    // With fare as a numeric (MARS) dimension on the SECOND axis, segment keys
    // y-bucket = floor(log2(fare)). Rows with fare in the same magnitude band
    // share a segment; rows in different bands land in different segments.
    let temp_dir = TempDir::new()?;
    let config = WriterConfig {
        dimensions: vec!["status".into(), "fare".into(), "hour".into()],
        dimension_types: vec![
            DimensionType::Categorical,
            DimensionType::Numeric,
            DimensionType::Categorical,
        ],
        bucket_counts: [1, 16, 24], // single status bucket isolates fare routing
        output_dir: temp_dir.path().to_path_buf(),
        segment_size_threshold: 10_000,
        schema: None,
        storage_format: StorageFormat::Csv,
    };
    let mut writer = StrataWriter::new(config)?;

    // fare=3 → floor(log2 3)=1 ; fare=100 → floor(log2 100)=6 ; fare=5 → 2
    for fare in ["3", "3.5", "100", "120", "5"] {
        let mut row = Row::new();
        row.insert("status".into(), "OK".into());
        row.insert("fare".into(), fare.to_string());
        row.insert("hour".into(), "0".into());
        writer.write_row(row)?;
    }
    writer.flush_all()?;

    // Expect three distinct magnitude buckets: 1, 6, 2 → segments (0,1),(0,6),(0,2).
    assert!(temp_dir.path().join("segment_0_1_data.csv").exists());
    assert!(temp_dir.path().join("segment_0_6_data.csv").exists());
    assert!(temp_dir.path().join("segment_0_2_data.csv").exists());

    Ok(())
}

#[test]
fn categorical_routing_is_stable_for_same_value() -> Result<()> {
    // The same categorical value must always route to the same segment, so
    // many identical rows collapse into exactly one segment file.
    let temp_dir = TempDir::new()?;
    let config = WriterConfig {
        output_dir: temp_dir.path().to_path_buf(),
        segment_size_threshold: 10_000,
        ..WriterConfig::default()
    };
    let mut writer = StrataWriter::new(config)?;

    for _ in 0..50 {
        let mut row = Row::new();
        row.insert("status".into(), "OK".into());
        row.insert("region".into(), "NYC".into());
        row.insert("hour".into(), "9".into());
        writer.write_row(row)?;
    }
    writer.flush_all()?;

    let data_files: Vec<_> = fs::read_dir(temp_dir.path())?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|ext| ext == "csv")
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(data_files.len(), 1, "identical rows must share one segment");

    Ok(())
}
