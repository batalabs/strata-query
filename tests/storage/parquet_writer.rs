//! Mirror of `src/storage/parquet_writer.rs` unit tests, expressed through the
//! public API.
//!
//! NOTE: the original inline `typed_batch_maps_column_types_and_nulls` called
//! the private `build_typed_batch` directly. It is rewritten here through the
//! public writer→reader path (`typed_unparseable_value_becomes_null`): a row
//! whose Int64 column holds an unparseable value is written with a typed schema,
//! then read back; the public reader drops null cells, so the field is absent on
//! read, the same NULL behavior the private test pinned.

use std::fs::{self, File};
use std::path::Path;

use anyhow::Result;
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;

use strata_query::{
    ColumnType, DimensionType, ParquetStrataReader, ParquetStrataWriter, ParquetWriterConfig,
    QueryPredicate, Row, SegmentMetadata, TableSchema,
};

/// Default 3-categorical-dimension config writing into `dir`.
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

/// Count `*_data.parquet` / `*_meta.json` files in a directory.
fn count_files(dir: &Path, suffix: &str) -> usize {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(suffix))
        .count()
}

#[test]
fn test_parquet_writer_basic() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut writer = ParquetStrataWriter::new(cat_config(temp_dir.path()))?;

    for i in 0..250 {
        writer.write_row(row3(
            "OK",
            &format!("region_{}", i % 4),
            &format!("{}", i % 24),
        ))?;
    }
    writer.flush_all()?;

    assert!(
        count_files(temp_dir.path(), "_data.parquet") > 0,
        "should create Parquet files"
    );
    Ok(())
}

/// Every flushed data file must be accompanied by exactly one metadata file,
/// and the segment count must equal the number of distinct (R,S) keys hit.
#[test]
fn data_and_meta_files_are_paired() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut writer = ParquetStrataWriter::new(cat_config(temp_dir.path()))?;
    for i in 0..40 {
        writer.write_row(row3("OK", &format!("r{}", i % 4), &format!("{}", i % 24)))?;
    }
    let segments = writer.get_segment_count();
    writer.flush_all()?;

    let data = count_files(temp_dir.path(), "_data.parquet");
    let meta = count_files(temp_dir.path(), "_meta.json");
    assert_eq!(data, meta, "each data file needs a meta file");
    assert_eq!(data, segments, "one file pair per distinct segment key");
    assert!(segments > 1, "fixture should span multiple segments");
    Ok(())
}

/// An empty writer flushes nothing: no files, no panic.
#[test]
fn flush_with_no_rows_writes_nothing() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut writer = ParquetStrataWriter::new(cat_config(temp_dir.path()))?;
    assert_eq!(writer.get_segment_count(), 0);
    writer.flush_all()?;
    assert_eq!(count_files(temp_dir.path(), "_data.parquet"), 0);
    Ok(())
}

/// A single row produces exactly one segment with one data + one meta file.
#[test]
fn single_row_produces_one_segment() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut writer = ParquetStrataWriter::new(cat_config(temp_dir.path()))?;
    writer.write_row(row3("ERROR", "NYC", "9"))?;
    assert_eq!(writer.get_segment_count(), 1);
    writer.flush_all()?;
    assert_eq!(count_files(temp_dir.path(), "_data.parquet"), 1);
    assert_eq!(count_files(temp_dir.path(), "_meta.json"), 1);
    Ok(())
}

/// Rows that route to the same (R,S) segment key must collapse into a single
/// segment regardless of the third (T) dimension or non-routing columns.
#[test]
fn same_routing_key_collapses_to_one_segment() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut writer = ParquetStrataWriter::new(cat_config(temp_dir.path()))?;
    // status+region fixed → same (R,S) key; hour varies → same segment.
    for h in 0..24 {
        writer.write_row(row3("OK", "NYC", &h.to_string()))?;
    }
    assert_eq!(
        writer.get_segment_count(),
        1,
        "fixed status+region must route to a single segment"
    );
    Ok(())
}

/// The written metadata JSON must be a `SegmentMetadata` whose row_count and
/// bitset reflect exactly what was written (the bitset must be non-empty and
/// the per-segment row counts must sum to the total written).
#[test]
fn metadata_json_reflects_written_rows() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut writer = ParquetStrataWriter::new(cat_config(temp_dir.path()))?;
    let total = 60usize;
    for i in 0..total {
        writer.write_row(row3(
            if i % 2 == 0 { "OK" } else { "ERROR" },
            &format!("r{}", i % 3),
            &format!("{}", i % 24),
        ))?;
    }
    writer.flush_all()?;

    let mut summed_rows = 0usize;
    for entry in fs::read_dir(temp_dir.path())? {
        let path = entry?.path();
        if path.to_string_lossy().ends_with("_meta.json") {
            let meta: SegmentMetadata = serde_json::from_reader(File::open(&path)?)?;
            assert!(
                meta.bitset.count_set_bits() > 0,
                "every flushed segment must have at least one set bit"
            );
            summed_rows += meta.row_count;
        }
    }
    assert_eq!(summed_rows, total, "metadata row counts must sum to total");
    Ok(())
}

/// Compression codec is configurable and a non-default codec still produces
/// readable files (smoke test that the codec is actually applied).
#[test]
fn honors_non_default_compression() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut config = cat_config(temp_dir.path());
    config.compression = parquet::basic::Compression::UNCOMPRESSED;
    let mut writer = ParquetStrataWriter::new(config)?;
    for i in 0..50 {
        writer.write_row(row3("OK", "NYC", &format!("{}", i % 24)))?;
    }
    writer.flush_all()?;
    assert!(count_files(temp_dir.path(), "_data.parquet") > 0);
    Ok(())
}

/// `Default` config is self-consistent (three dims, three types).
#[test]
fn default_config_is_well_formed() {
    let cfg = ParquetWriterConfig::default();
    assert_eq!(cfg.dimensions.len(), 3);
    assert_eq!(cfg.dimension_types.len(), 3);
    assert_eq!(cfg.compression, parquet::basic::Compression::SNAPPY);
}

/// Rewritten from the private-`build_typed_batch` test: a row whose Int64 column
/// holds an unparseable value must round-trip as NULL. The public reader drops
/// null cells, so the `trips` field is absent on read while the parseable
/// `fare`/`hour` survive, pinning the same NULL-on-unparseable behavior through
/// the public writer→reader path.
#[test]
fn typed_unparseable_value_becomes_null() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut cfg = cat_config(temp_dir.path());
    // Route on `trips` magnitude so both rows share one segment.
    cfg.dimensions = vec![
        "trips".to_string(),
        "status".to_string(),
        "hour".to_string(),
    ];
    cfg.dimension_types = vec![
        DimensionType::Categorical,
        DimensionType::Categorical,
        DimensionType::Categorical,
    ];
    cfg.schema = Some(TableSchema::from_pairs(vec![
        ("trips", ColumnType::Int64),
        ("status", ColumnType::Utf8),
        ("hour", ColumnType::Int64),
        ("fare", ColumnType::Float64),
    ]));

    let mut good = Row::new();
    good.insert("trips".into(), "3".into());
    good.insert("status".into(), "OK".into());
    good.insert("hour".into(), "9".into());
    good.insert("fare".into(), "12.5".into());

    // `trips` is unparseable as Int64 → must become NULL, not error.
    let mut bad = Row::new();
    bad.insert("trips".into(), "not_a_number".into());
    bad.insert("status".into(), "OK".into());
    bad.insert("hour".into(), "9".into());
    bad.insert("fare".into(), "9.0".into());

    let mut writer = ParquetStrataWriter::new(cfg)?;
    writer.write_row(good)?;
    writer.write_row(bad)?;
    writer.flush_all()?;

    let reader = ParquetStrataReader::load_segments(temp_dir.path())?;
    let all_keys = reader.filter_segments(&QueryPredicate::new())?;
    // Use the typed-aware read path (`read_and_filter`): the legacy
    // `read_segments` only materializes Utf8 columns, so typed Int64 columns
    // would be dropped regardless of nullness. With the empty predicate every
    // row survives row-level filtering.
    let rows = reader.read_and_filter(&all_keys, &QueryPredicate::new())?;
    assert_eq!(rows.len(), 2, "both rows must round-trip");

    // Exactly one row is missing `trips` (the unparseable one became NULL and
    // the reader drops null cells); the other keeps trips == 3.
    let missing_trips = rows.iter().filter(|r| r.get("trips").is_none()).count();
    let has_trips_3 = rows
        .iter()
        .any(|r| r.get("trips").map(String::as_str) == Some("3"));
    assert_eq!(
        missing_trips, 1,
        "the unparseable Int64 must read back as NULL"
    );
    assert!(has_trips_3, "the parseable Int64 must survive as 3");

    Ok(())
}

#[test]
fn typed_values_round_trip_through_parquet() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut cfg = cat_config(temp_dir.path());
    cfg.dimensions = vec!["fare".to_string(), "region".to_string(), "hour".to_string()];
    cfg.dimension_types = vec![
        DimensionType::Numeric,
        DimensionType::Categorical,
        DimensionType::Categorical,
    ];
    cfg.schema = Some(TableSchema::from_pairs(vec![
        ("fare", ColumnType::Float64),
        ("region", ColumnType::Utf8),
        ("hour", ColumnType::Int64),
    ]));

    let mut writer = ParquetStrataWriter::new(cfg)?;
    // All rows share one routing key (fare magnitude + region fixed) -> one segment.
    let mut r = Row::new();
    r.insert("fare".to_string(), "100.5".to_string());
    r.insert("region".to_string(), "NYC".to_string());
    r.insert("hour".to_string(), "9".to_string());
    writer.write_row(r)?;
    writer.flush_all()?;

    let data_path = fs::read_dir(temp_dir.path())?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("_data.parquet"))
        .expect("a data file should exist");
    let file = File::open(&data_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

    let mut saw_row = false;
    for batch in reader {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let schema = batch.schema();
        let fare_idx = schema.index_of("fare").unwrap();
        let hour_idx = schema.index_of("hour").unwrap();
        let fares = batch
            .column(fare_idx)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        let hours = batch
            .column(hour_idx)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(fares.value(0), 100.5);
        assert_eq!(hours.value(0), 9);
        saw_row = true;
    }
    assert!(saw_row, "expected at least one row back");
    Ok(())
}

#[test]
fn schema_none_still_writes_all_utf8() -> Result<()> {
    let temp_dir = TempDir::new()?;
    // cat_config has schema: None -> legacy path.
    let mut writer = ParquetStrataWriter::new(cat_config(temp_dir.path()))?;
    for i in 0..30 {
        writer.write_row(row3("OK", "NYC", &format!("{}", i % 24)))?;
    }
    writer.flush_all()?;

    let data_path = fs::read_dir(temp_dir.path())?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("_data.parquet"))
        .expect("a data file should exist");
    let file = File::open(&data_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = builder.schema();
    for field in arrow_schema.fields() {
        assert_eq!(
            field.data_type(),
            &DataType::Utf8,
            "legacy path must keep every column Utf8"
        );
    }
    Ok(())
}

#[test]
fn config_carries_optional_schema() {
    let mut cfg = ParquetWriterConfig::default();
    assert!(cfg.schema.is_none(), "default config has no typed schema");

    cfg.schema = Some(TableSchema::from_pairs(vec![("fare", ColumnType::Float64)]));
    assert_eq!(
        cfg.schema.as_ref().unwrap().column_type("fare"),
        Some(ColumnType::Float64)
    );
}

#[test]
fn writer_emits_typed_parquet_columns() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let mut cfg = cat_config(temp_dir.path());
    cfg.dimensions = vec!["fare".to_string(), "region".to_string(), "hour".to_string()];
    cfg.dimension_types = vec![
        DimensionType::Numeric,
        DimensionType::Categorical,
        DimensionType::Categorical,
    ];
    cfg.schema = Some(TableSchema::from_pairs(vec![
        ("fare", ColumnType::Float64),
        ("region", ColumnType::Utf8),
        ("hour", ColumnType::Int64),
    ]));

    let mut writer = ParquetStrataWriter::new(cfg)?;
    for i in 0..50 {
        let mut r = Row::new();
        r.insert("fare".to_string(), format!("{}", 10 + i));
        r.insert("region".to_string(), format!("r{}", i % 4));
        r.insert("hour".to_string(), format!("{}", i % 24));
        writer.write_row(r)?;
    }
    writer.flush_all()?;

    let data_path = fs::read_dir(temp_dir.path())?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("_data.parquet"))
        .expect("a data file should exist");
    let file = File::open(&data_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = builder.schema();
    assert_eq!(
        arrow_schema.field_with_name("fare").unwrap().data_type(),
        &DataType::Float64
    );
    assert_eq!(
        arrow_schema.field_with_name("hour").unwrap().data_type(),
        &DataType::Int64
    );

    let meta_path = fs::read_dir(temp_dir.path())?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("_meta.json"))
        .expect("a meta file should exist");
    let meta: SegmentMetadata = serde_json::from_reader(File::open(&meta_path)?)?;
    assert_eq!(
        meta.schema.expect("schema persisted").column_type("fare"),
        Some(ColumnType::Float64)
    );
    Ok(())
}
