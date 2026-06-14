//! Shared test helpers for the integration-test mirror.
//!
//! These are NOT test runners (this file lives in a subdirectory, so cargo does
//! not compile it as its own test crate). Top-level entry files `mod common;`
//! this module when they need the helpers.
//!
//! Everything here drives only the public `strata_query` API.

#![allow(dead_code)]

use std::path::Path;

use strata_query::{
    DimensionType, ParquetWriterConfig, Row, StorageFormat, StrataWriter, WriterConfig,
};

/// Build a 3-categorical-dimension CSV writer config rooted at `dir`.
pub fn csv_config(dir: &Path, threshold: usize) -> WriterConfig {
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

/// Build a default 3-categorical-dimension Parquet writer config rooted at `dir`.
pub fn parquet_config(dir: &Path) -> ParquetWriterConfig {
    ParquetWriterConfig {
        dimensions: vec!["status".into(), "region".into(), "hour".into()],
        dimension_types: vec![DimensionType::Categorical; 3],
        bucket_counts: [4, 4, 24],
        output_dir: dir.to_path_buf(),
        segment_size_threshold: 100,
        compression: parquet::basic::Compression::SNAPPY,
        schema: None,
    }
}

/// Construct a `Row` from the three core (status, region, hour) dimensions.
pub fn row3(status: &str, region: &str, hour: &str) -> Row {
    let mut row = Row::new();
    row.insert("status".into(), status.into());
    row.insert("region".into(), region.into());
    row.insert("hour".into(), hour.into());
    row
}

/// Write a fixed, fully-known categorical dataset (600 rows spread across all
/// status/region/hour combinations) through the public CSV writer so reader
/// tests can assert exact behavior against ground truth.
pub fn write_csv_fixture(dir: &Path, format: StorageFormat) {
    let mut config = csv_config(dir, 1000);
    config.storage_format = format;
    let mut writer = StrataWriter::new(config).unwrap();

    let statuses = ["OK", "ERROR", "WARN"];
    let regions = ["NYC", "SF", "LA", "CHI"];
    for i in 0..600usize {
        let mut row = Row::new();
        row.insert("status".into(), statuses[i % 3].into());
        row.insert("region".into(), regions[(i / 3) % 4].into());
        row.insert("hour".into(), ((i / 12) % 24).to_string());
        writer.write_row(row).unwrap();
    }
    writer.flush_all().unwrap();
}

/// Ground-truth rows matching [`write_csv_fixture`] (same construction, no IO).
pub fn csv_fixture_rows() -> Vec<Row> {
    let statuses = ["OK", "ERROR", "WARN"];
    let regions = ["NYC", "SF", "LA", "CHI"];
    (0..600usize)
        .map(|i| {
            let mut row = Row::new();
            row.insert("status".into(), statuses[i % 3].into());
            row.insert("region".into(), regions[(i / 3) % 4].into());
            row.insert("hour".into(), ((i / 12) % 24).to_string());
            row
        })
        .collect()
}
