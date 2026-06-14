use crate::routing::hash::{compute_bitset_index, compute_segment_key};
use crate::types::{ColumnType, DimensionType, Row, SegmentKey, SegmentMetadata, TableSchema};
use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Build a typed Arrow `RecordBatch` from `rows` according to `schema`.
///
/// One column is emitted per schema column, in schema order. A value that is
/// missing or fails to parse as the column's type becomes NULL; this is safe
/// because pruning is sound regardless of payload nulls, and the engine
/// re-applies predicates on read.
fn build_typed_batch(rows: &[Row], schema: &TableSchema) -> Result<(Arc<Schema>, RecordBatch)> {
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|c| {
            let dt = match c.col_type {
                ColumnType::Int64 => DataType::Int64,
                ColumnType::Float64 => DataType::Float64,
                ColumnType::Utf8 => DataType::Utf8,
                ColumnType::Boolean => DataType::Boolean,
            };
            Field::new(c.name.as_str(), dt, true)
        })
        .collect();
    let arrow_schema = Arc::new(Schema::new(fields));

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());
    for col in &schema.columns {
        let array: ArrayRef = match col.col_type {
            ColumnType::Int64 => {
                let v: Vec<Option<i64>> = rows.iter().map(|r| r.get_i64(&col.name)).collect();
                Arc::new(Int64Array::from(v))
            }
            ColumnType::Float64 => {
                let v: Vec<Option<f64>> = rows.iter().map(|r| r.get_f64(&col.name)).collect();
                Arc::new(Float64Array::from(v))
            }
            ColumnType::Boolean => {
                let v: Vec<Option<bool>> = rows.iter().map(|r| r.get_bool(&col.name)).collect();
                Arc::new(BooleanArray::from(v))
            }
            ColumnType::Utf8 => {
                let v: Vec<Option<String>> =
                    rows.iter().map(|r| r.get(&col.name).cloned()).collect();
                Arc::new(StringArray::from(v))
            }
        };
        columns.push(array);
    }

    let batch = RecordBatch::try_new(arrow_schema.clone(), columns)
        .context("Failed to create typed RecordBatch")?;
    Ok((arrow_schema, batch))
}

/// Legacy path: build an all-`Utf8` `RecordBatch` from the union of row keys
/// (sorted). Used when no typed schema is configured.
fn build_string_batch(rows: &[Row]) -> Result<(Arc<Schema>, RecordBatch)> {
    let first_row = rows.first().context("No rows to write")?;
    let mut column_names: Vec<String> = first_row.fields.keys().cloned().collect();
    column_names.sort();

    let fields: Vec<Field> = column_names
        .iter()
        .map(|name| Field::new(name.as_str(), DataType::Utf8, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let mut columns: Vec<ArrayRef> = Vec::new();
    for col_name in &column_names {
        let values: Vec<Option<String>> =
            rows.iter().map(|row| row.get(col_name).cloned()).collect();
        columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
    }

    let batch =
        RecordBatch::try_new(schema.clone(), columns).context("Failed to create RecordBatch")?;
    Ok((schema, batch))
}

/// Configuration for the STRATA Parquet writer.
#[derive(Debug, Clone)]
pub struct ParquetWriterConfig {
    /// Names of the three routing dimensions `[dim1, dim2, dim3]`
    pub dimensions: Vec<String>,

    /// Routing strategy for each dimension.
    /// Defaults to all `Categorical` if empty.
    pub dimension_types: Vec<DimensionType>,

    /// Number of buckets for each dimension `[R, S, T]`
    pub bucket_counts: [u8; 3],

    /// Directory to write segment files to
    pub output_dir: PathBuf,

    /// Maximum number of rows per segment before flushing
    pub segment_size_threshold: usize,

    /// Parquet compression codec (default: Snappy)
    pub compression: parquet::basic::Compression,

    /// Optional typed schema. When `Some`, columns are written as typed Arrow
    /// arrays (Int64/Float64/Utf8/Boolean). When `None`, all columns are Utf8
    /// (legacy behavior).
    pub schema: Option<TableSchema>,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        ParquetWriterConfig {
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
            output_dir: PathBuf::from("./strata_parquet_output"),
            segment_size_threshold: 10_000,
            compression: parquet::basic::Compression::SNAPPY,
            schema: None,
        }
    }
}

/// STRATA writer that outputs Parquet files
pub struct ParquetStrataWriter {
    config: ParquetWriterConfig,
    segment_builders: HashMap<SegmentKey, ParquetSegmentBuilder>,
}

impl ParquetStrataWriter {
    pub fn new(config: ParquetWriterConfig) -> Result<Self> {
        fs::create_dir_all(&config.output_dir).context("Failed to create output directory")?;

        Ok(ParquetStrataWriter {
            config,
            segment_builders: HashMap::new(),
        })
    }

    pub fn write_row(&mut self, row: Row) -> Result<()> {
        let segment_key = compute_segment_key(
            &row,
            &self.config.dimensions,
            &self.config.dimension_types,
            &[self.config.bucket_counts[0], self.config.bucket_counts[1]],
        );

        let builder = self.segment_builders.entry(segment_key).or_insert_with(|| {
            ParquetSegmentBuilder::new(
                segment_key,
                self.config.dimensions.clone(),
                self.config.dimension_types.clone(),
                self.config.bucket_counts,
            )
        });

        let bitset_index = compute_bitset_index(
            &row,
            &self.config.dimensions,
            &self.config.dimension_types,
            &self.config.bucket_counts,
        );

        builder.metadata.bitset.set(bitset_index);
        builder.metadata.increment_bucket_count(bitset_index);
        builder.add_row(row);

        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        for (_, mut builder) in self.segment_builders.drain() {
            if !builder.all_rows.is_empty() {
                builder.rows = builder.all_rows.clone();
                builder.metadata.schema = self.config.schema.clone();
                builder.flush_to_parquet(
                    &self.config.output_dir,
                    self.config.compression,
                    self.config.schema.as_ref(),
                )?;
            }
        }
        Ok(())
    }

    pub fn get_segment_count(&self) -> usize {
        self.segment_builders.len()
    }
}

/// Builder for a single Parquet segment
struct ParquetSegmentBuilder {
    metadata: SegmentMetadata,
    rows: Vec<Row>,
    all_rows: Vec<Row>, // Accumulate all rows across flushes
}

impl ParquetSegmentBuilder {
    fn new(
        key: SegmentKey,
        dimensions: Vec<String>,
        dimension_types: Vec<DimensionType>,
        bucket_counts: [u8; 3],
    ) -> Self {
        ParquetSegmentBuilder {
            metadata: SegmentMetadata::new(key, dimensions, dimension_types, bucket_counts),
            rows: Vec::new(),
            all_rows: Vec::new(),
        }
    }

    fn add_row(&mut self, row: Row) {
        self.rows.push(row.clone());
        self.all_rows.push(row);
        self.metadata.row_count += 1;
    }

    fn flush_to_parquet(
        &self,
        output_dir: &Path,
        compression: parquet::basic::Compression,
        schema: Option<&TableSchema>,
    ) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }

        let key = self.metadata.key;
        let base_name = format!("segment_{}_{}", key.0, key.1);

        let parquet_path = output_dir.join(format!("{}_data.parquet", base_name));
        self.write_parquet_file(&parquet_path, compression, schema)?;

        let meta_path = output_dir.join(format!("{}_meta.json", base_name));
        self.write_metadata_json(&meta_path)?;

        Ok(())
    }

    fn write_parquet_file(
        &self,
        path: &Path,
        compression: parquet::basic::Compression,
        schema: Option<&TableSchema>,
    ) -> Result<()> {
        let (arrow_schema, batch) = match schema {
            Some(s) => build_typed_batch(&self.rows, s)?,
            None => build_string_batch(&self.rows)?,
        };

        let file = File::create(path).context("Failed to create Parquet file")?;
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props))
            .context("Failed to create ArrowWriter")?;
        writer
            .write(&batch)
            .context("Failed to write RecordBatch")?;
        writer.close().context("Failed to close ParquetWriter")?;
        Ok(())
    }

    fn write_metadata_json(&self, path: &Path) -> Result<()> {
        let file = File::create(path).context("Failed to create metadata JSON file")?;
        serde_json::to_writer_pretty(file, &self.metadata)
            .context("Failed to serialize metadata")?;
        Ok(())
    }
}
