//! Transport-agnostic query execution for the Flight SQL server.
//!
//! This module holds the testable core that the `strata_flight_server` binary
//! relied on but could not unit-test (per the Rust Book ch11-03: binaries can't
//! be integration-tested, so logic belongs in the library). The binary is now a
//! thin gRPC adapter that calls [`execute_query`] and streams the resulting
//! [`RecordBatch`]; everything here is free of tonic/`Status`/gRPC types.

use std::sync::Arc;

use arrow::array::{Float64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use crate::storage::reader::StrataReader;
use crate::types::Row;
use crate::{parse_query, AggFunc};

/// Schema advertised for a queried table.
///
/// This is **planning metadata only**: it is a fixed set of columns used to
/// answer FlightInfo/prepared-statement schema requests, and does not reflect
/// the actual columns returned by [`execute_query`]. Preserved as-is from the
/// original binary; fixing it to be table-aware is out of scope.
///
/// # Examples
///
/// ```
/// use strata_query::flight::schema_for_table;
///
/// let schema = schema_for_table("anything");
/// assert_eq!(schema.fields().len(), 5);
/// assert_eq!(schema.field(0).name(), "status");
/// ```
pub fn schema_for_table(_table_name: &str) -> Schema {
    Schema::new(vec![
        Field::new("status", DataType::Utf8, true),
        Field::new("fare", DataType::Utf8, true),
        Field::new("hour", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("distance", DataType::Utf8, true),
    ])
}

/// Schema for the `do_get_tables` listing (table name + row count).
///
/// # Examples
///
/// ```
/// use strata_query::flight::tables_schema;
///
/// let schema = tables_schema();
/// assert_eq!(schema.field(0).name(), "table_name");
/// assert_eq!(schema.field(1).name(), "table_rows");
/// ```
pub fn tables_schema() -> Schema {
    Schema::new(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_rows", DataType::Int64, false),
    ])
}

/// Convert STRATA [`Row`]s into an Arrow [`RecordBatch`] of `Utf8` columns.
///
/// Columns are taken from the first row's field names, sorted for a stable
/// order. A row missing a column yields a null cell for that column. An empty
/// input yields an empty batch (using the planning-metadata schema) rather than
/// panicking.
///
/// # Examples
///
/// ```
/// use strata_query::flight::rows_to_batch;
/// use strata_query::Row;
///
/// let mut row = Row::new();
/// row.insert("a".into(), "1".into());
/// row.insert("b".into(), "2".into());
///
/// let batch = rows_to_batch(&[row]).unwrap();
/// assert_eq!(batch.num_rows(), 1);
/// assert_eq!(batch.num_columns(), 2);
/// ```
pub fn rows_to_batch(rows: &[Row]) -> Result<RecordBatch, ArrowError> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(schema_for_table(""))));
    }

    let mut col_names: Vec<String> = rows[0].fields.keys().cloned().collect();
    col_names.sort();

    let mut builders: Vec<(String, StringBuilder)> = col_names
        .iter()
        .map(|n| (n.clone(), StringBuilder::new()))
        .collect();

    for row in rows {
        for (i, col_name) in col_names.iter().enumerate() {
            if let Some(val) = row.get(col_name) {
                builders[i].1.append_value(val);
            } else {
                builders[i].1.append_null();
            }
        }
    }

    let fields: Vec<Field> = builders
        .iter()
        .map(|(name, _)| Field::new(name, DataType::Utf8, true))
        .collect();

    let arrays: Vec<Arc<dyn arrow::array::Array>> = builders
        .into_iter()
        .map(|(_, mut b)| Arc::new(b.finish()) as Arc<dyn arrow::array::Array>)
        .collect();

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
}

/// Parse and execute a SQL query against a STRATA data directory.
///
/// This is the transport-agnostic core lifted out of the Flight server's
/// `do_get_fallback`. It returns the same [`RecordBatch`] the gRPC handler used
/// to build, for both code paths:
///
/// - **Aggregation queries** (`parsed.aggregations` non-empty): returns a
///   three-column batch (`function`, `column`, `value`) with one row per
///   requested aggregation, computed from segment metadata via
///   [`StrataReader::aggregate_multi`].
/// - **Row queries**: prunes via [`StrataReader::filter_segments`], reads
///   matching rows via [`StrataReader::read_and_filter`], applies `parsed.limit`,
///   and converts to a batch via [`rows_to_batch`].
///
/// Errors are returned as [`anyhow::Error`]; the binary maps them to
/// `tonic::Status` at the call site.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use strata_query::flight::execute_query;
///
/// let batch = execute_query(Path::new("./strata_data/trips"), "SELECT * FROM trips").unwrap();
/// println!("{} rows", batch.num_rows());
/// ```
pub fn execute_query(data_dir: &std::path::Path, sql: &str) -> anyhow::Result<RecordBatch> {
    let parsed = parse_query(sql)?;

    let reader = StrataReader::load_segments(data_dir)?;
    let matching_keys = reader.filter_segments(&parsed.predicate)?;

    // Aggregation queries use the metadata-only path.
    if !parsed.aggregations.is_empty() {
        let agg_args: Vec<(AggFunc, &str)> = parsed
            .aggregations
            .iter()
            .map(|a| (a.func.clone(), a.column.as_str()))
            .collect();

        let results = reader.aggregate_multi(&parsed.predicate, &agg_args)?;

        let mut func_builder = StringBuilder::new();
        let mut col_builder = StringBuilder::new();
        let mut val_builder = Float64Builder::new();

        for (req, res) in parsed.aggregations.iter().zip(results.iter()) {
            let func_name = match req.func {
                AggFunc::Count => "COUNT",
                AggFunc::Sum => "SUM",
                AggFunc::Avg => "AVG",
                AggFunc::Min => "MIN",
                AggFunc::Max => "MAX",
            };
            func_builder.append_value(func_name);
            col_builder.append_value(&req.column);
            val_builder.append_value(res.as_ref().map(|r| r.value()).unwrap_or(0.0));
        }

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("function", DataType::Utf8, false),
                Field::new("column", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ])),
            vec![
                Arc::new(func_builder.finish()),
                Arc::new(col_builder.finish()),
                Arc::new(val_builder.finish()),
            ],
        )?;

        return Ok(batch);
    }

    let rows = reader.read_and_filter(&matching_keys, &parsed.predicate)?;

    let limited_rows: Vec<Row> = if let Some(limit) = parsed.limit {
        rows.into_iter().take(limit as usize).collect()
    } else {
        rows
    };

    let batch = rows_to_batch(&limited_rows)?;
    Ok(batch)
}
