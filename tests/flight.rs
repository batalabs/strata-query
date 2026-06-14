//! Mirror of `src/flight.rs` unit tests, expressed through the public API.

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use tempfile::TempDir;

use strata_query::flight::{execute_query, rows_to_batch};
use strata_query::{DimensionType, Row, StorageFormat, StrataWriter, WriterConfig};

fn string_col<'a>(batch: &'a RecordBatch, name: &str) -> &'a arrow::array::StringArray {
    let idx = batch.schema().index_of(name).unwrap();
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
}

#[test]
fn rows_to_batch_empty_input_yields_empty_batch() {
    let batch = rows_to_batch(&[]).unwrap();
    assert_eq!(batch.num_rows(), 0);
    // Empty input falls back to the planning-metadata schema (5 columns).
    assert_eq!(batch.num_columns(), 5);
}

#[test]
fn rows_to_batch_single_row() {
    let mut row = Row::new();
    row.insert("status".into(), "OK".into());
    row.insert("fare".into(), "100".into());

    let batch = rows_to_batch(&[row]).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(string_col(&batch, "status").value(0), "OK");
    assert_eq!(string_col(&batch, "fare").value(0), "100");
}

#[test]
fn rows_to_batch_multiple_rows_sorted_columns() {
    let mut r0 = Row::new();
    r0.insert("zeta".into(), "z0".into());
    r0.insert("alpha".into(), "a0".into());
    let mut r1 = Row::new();
    r1.insert("zeta".into(), "z1".into());
    r1.insert("alpha".into(), "a1".into());

    let batch = rows_to_batch(&[r0, r1]).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);

    // Column order is the sorted field names: alpha before zeta.
    let schema = batch.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["alpha", "zeta"]);

    let alpha = string_col(&batch, "alpha");
    assert_eq!(alpha.value(0), "a0");
    assert_eq!(alpha.value(1), "a1");
    let zeta = string_col(&batch, "zeta");
    assert_eq!(zeta.value(0), "z0");
    assert_eq!(zeta.value(1), "z1");
}

#[test]
fn rows_to_batch_missing_column_is_null() {
    // First row defines the column set; second row omits "fare".
    let mut r0 = Row::new();
    r0.insert("status".into(), "OK".into());
    r0.insert("fare".into(), "100".into());
    let mut r1 = Row::new();
    r1.insert("status".into(), "WARN".into());

    let batch = rows_to_batch(&[r0, r1]).unwrap();
    assert_eq!(batch.num_rows(), 2);

    let fare = string_col(&batch, "fare");
    assert!(!fare.is_null(0));
    assert_eq!(fare.value(0), "100");
    assert!(fare.is_null(1));
}

/// Write a small deterministic dataset with a numeric `value` dimension so
/// MARS range pruning is exercised by [`execute_query`].
fn write_numeric_dataset(dir: &std::path::Path) {
    let config = WriterConfig {
        dimensions: vec!["status".into(), "value".into(), "hour".into()],
        dimension_types: vec![
            DimensionType::Categorical,
            DimensionType::Numeric,
            DimensionType::Categorical,
        ],
        bucket_counts: [4, 16, 24],
        output_dir: dir.to_path_buf(),
        segment_size_threshold: 100,
        schema: None,
        storage_format: StorageFormat::Csv,
    };

    let mut writer = StrataWriter::new(config).unwrap();

    // Fixed values: exactly 3 fall in [100, 500] (150, 200, 450).
    let values = [50.0_f64, 80.0, 150.0, 200.0, 450.0, 800.0, 1200.0];
    for (i, v) in values.iter().enumerate() {
        let mut row = Row::new();
        row.insert("status".into(), "OK".into());
        row.insert("value".into(), v.to_string());
        row.insert("hour".into(), format!("{}", i % 24));
        writer.write_row(row).unwrap();
    }
    writer.flush_all().unwrap();
}

#[test]
fn execute_query_range_returns_exact_in_range_rows() {
    let dir = TempDir::new().unwrap();
    write_numeric_dataset(dir.path());

    let batch = execute_query(
        dir.path(),
        "SELECT * FROM t WHERE value >= 100 AND value <= 500",
    )
    .unwrap();

    // Exactly 3 of the 7 written values are in [100, 500]: 150, 200, 450.
    // This asserts exact correctness, the property the REST path's
    // analogous bug violated.
    assert_eq!(batch.num_rows(), 3);

    let value_col = string_col(&batch, "value");
    let mut got: Vec<f64> = (0..value_col.len())
        .map(|i| value_col.value(i).parse::<f64>().unwrap())
        .collect();
    got.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(got, vec![150.0, 200.0, 450.0]);
}

#[test]
fn execute_query_count_aggregation() {
    let dir = TempDir::new().unwrap();
    write_numeric_dataset(dir.path());

    let batch = execute_query(dir.path(), "SELECT COUNT(*) FROM t").unwrap();

    // Aggregation path: 3-column function/column/value batch, one row.
    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.num_rows(), 1);

    assert_eq!(string_col(&batch, "function").value(0), "COUNT");

    let value_idx = batch.schema().index_of("value").unwrap();
    let value = batch
        .column(value_idx)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    // All 7 rows counted.
    assert_eq!(value.value(0), 7.0);
}
