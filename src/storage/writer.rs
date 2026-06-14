use crate::routing::hash::{compute_bitset_index, compute_segment_key};
use crate::types::{DimensionType, Row, SegmentKey, SegmentMetadata, StorageFormat};
use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Configuration for the STRATA writer.
///
/// Each of the three routing dimensions can independently use either:
/// - **Categorical** routing (hash-based) for strings like status, region
/// - **Numeric** routing (MARS `floor(log₂(v))`) for values like fare, amount, distance
///
/// The first two dimensions determine segment placement; all three contribute to
/// the existence bitset index.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Names of the three routing dimensions `[dim1, dim2, dim3]`
    pub dimensions: Vec<String>,

    /// Routing strategy for each dimension.
    /// Defaults to `[Categorical, Categorical, Categorical]` if empty.
    pub dimension_types: Vec<DimensionType>,

    /// Number of buckets for each dimension `[R, S, T]`
    pub bucket_counts: [u8; 3],

    /// Directory to write segment files to
    pub output_dir: PathBuf,

    /// Maximum number of rows per segment before flushing
    pub segment_size_threshold: usize,

    /// Optional table schema for typed column access and validation.
    /// When set, column stats use accurate type-aware parsing.
    pub schema: Option<crate::types::TableSchema>,

    /// Storage format for segment data files.
    /// Parquet offers better compression and faster reads; CSV is backward compatible.
    pub storage_format: crate::types::StorageFormat,
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig {
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
            output_dir: PathBuf::from("./strata_output"),
            segment_size_threshold: 10_000,
            schema: None,
            storage_format: crate::types::StorageFormat::Csv,
        }
    }
}

/// Main writer that routes rows to segments and maintains existence bitsets
pub struct StrataWriter {
    config: WriterConfig,
    segment_builders: HashMap<SegmentKey, SegmentBuilder>,
    /// Optional write-ahead log for crash recovery.
    wal: Option<crate::storage::wal::WalWriter>,
}

impl StrataWriter {
    /// Create a new STRATA writer with the given configuration.
    ///
    /// A write-ahead log (WAL) is automatically created in the output directory.
    /// Rows are logged before writing. On startup, any existing WAL is replayed
    /// to recover in-flight rows from a previous crash.
    pub fn new(config: WriterConfig) -> Result<Self> {
        // Create output directory if it doesn't exist
        fs::create_dir_all(&config.output_dir).context("Failed to create output directory")?;

        // WAL setup
        let wal_path = config.output_dir.join("wal.log");
        let wal = if crate::storage::wal::WalWriter::exists_and_nonempty(&wal_path) {
            // Replay existing WAL from a previous crash
            let rows =
                crate::storage::wal::replay_wal(&wal_path).context("Failed to replay WAL")?;
            if !rows.is_empty() {
                tracing::info!("Replaying {} rows from WAL", rows.len());
            }
            // Replay rows through a temporary writer with a very high threshold
            // so no intermediate flushes happen (avoids partial metadata overwrites)
            let mut replay_config = config.clone();
            replay_config.segment_size_threshold = usize::MAX;
            let mut replay_writer = StrataWriter {
                config: replay_config,
                segment_builders: HashMap::new(),
                wal: None,
            };
            for row in &rows {
                replay_writer.write_row_internal(row)?;
            }
            // Flush all replayed rows to disk
            for (_, builder) in replay_writer.segment_builders.drain() {
                builder.flush_to_disk_with_format(&config.output_dir, config.storage_format)?;
            }

            // Open a fresh WAL (old one was consumed during replay)
            Some(crate::storage::wal::WalWriter::create(&wal_path)?)
        } else {
            Some(crate::storage::wal::WalWriter::create(&wal_path)?)
        };

        Ok(StrataWriter {
            config,
            segment_builders: HashMap::new(),
            wal,
        })
    }

    /// Write a single row to the appropriate segment.
    ///
    /// This is the main entry point for ingestion. The row is routed to a segment
    /// based on its first two dimension values (hash for categorical, MARS for numeric),
    /// and its bitset index is computed from all three dimensions.
    pub fn write_row(&mut self, row: Row) -> Result<()> {
        // WAL: log before write for crash recovery
        if let Some(ref mut wal) = self.wal {
            wal.append(&row)?;
        }
        self.write_row_internal(&row)
    }

    /// Internal write logic without WAL (used for replay recovery).
    fn write_row_internal(&mut self, row: &Row) -> Result<()> {
        // Step 1: Compute which segment this row belongs to (based on first 2 dimensions)
        let segment_key = compute_segment_key(
            row,
            &self.config.dimensions,
            &self.config.dimension_types,
            &[self.config.bucket_counts[0], self.config.bucket_counts[1]],
        );

        // Step 2: Get or create the segment builder for this key
        let builder = self.segment_builders.entry(segment_key).or_insert_with(|| {
            SegmentBuilder::new(
                segment_key,
                self.config.dimensions.clone(),
                self.config.dimension_types.clone(),
                self.config.bucket_counts,
            )
        });

        // Step 3: Compute bitset index (based on all 3 dimensions)
        let bitset_index = compute_bitset_index(
            row,
            &self.config.dimensions,
            &self.config.dimension_types,
            &self.config.bucket_counts,
        );

        // Step 4: Update the segment's existence bitset and bucket counts
        builder.metadata.bitset.set(bitset_index);
        builder.metadata.increment_bucket_count(bitset_index);

        // Step 4b: Update per-column statistics for aggregation pushdown
        let numeric_flags: Vec<bool> = self
            .config
            .dimension_types
            .iter()
            .map(|dt| matches!(dt, DimensionType::Numeric))
            .collect();
        builder
            .metadata
            .update_column_stats(row, &numeric_flags, self.config.schema.as_ref());

        // Step 5: Add row to the segment
        builder.add_row(row.clone());

        // Step 6: flush if the segment is full; remove and flush to avoid
        // overwriting on a subsequent flush for the same segment key
        if builder.should_flush(self.config.segment_size_threshold) {
            // Remove the builder so we can flush it (can't borrow self while flushing)
            if let Some(builder) = self.segment_builders.remove(&segment_key) {
                builder.flush_to_disk_with_format(
                    &self.config.output_dir,
                    self.config.storage_format,
                )?;
            }
        }

        Ok(())
    }

    /// Flush all remaining segments to disk.
    /// Call this when done writing all rows.
    /// After flushing, the WAL is truncated since all rows are safely persisted.
    pub fn flush_all(&mut self) -> Result<()> {
        for (_, builder) in self.segment_builders.drain() {
            builder
                .flush_to_disk_with_format(&self.config.output_dir, self.config.storage_format)?;
        }

        // WAL: truncate after a successful flush; all rows are now in segment files
        if let Some(wal) = self.wal.take() {
            wal.truncate()?;
        }

        Ok(())
    }

    /// Get statistics about the write session
    pub fn get_stats(&self) -> WriterStats {
        let total_segments = self.segment_builders.len();
        let total_rows: usize = self.segment_builders.values().map(|b| b.rows.len()).sum();

        WriterStats {
            total_segments,
            total_rows,
        }
    }
}

/// Builder for a single segment
/// Accumulates rows and maintains the existence bitset
struct SegmentBuilder {
    metadata: SegmentMetadata,
    rows: Vec<Row>,
}

impl SegmentBuilder {
    fn new(
        key: SegmentKey,
        dimensions: Vec<String>,
        dimension_types: Vec<DimensionType>,
        bucket_counts: [u8; 3],
    ) -> Self {
        SegmentBuilder {
            metadata: SegmentMetadata::new(key, dimensions, dimension_types, bucket_counts),
            rows: Vec::new(),
        }
    }

    fn add_row(&mut self, row: Row) {
        self.rows.push(row);
        self.metadata.row_count += 1;
    }

    fn should_flush(&self, threshold: usize) -> bool {
        self.rows.len() >= threshold
    }

    /// Flush this segment to disk as two files:
    /// 1. segment_{x}_{y}_data.csv - The actual row data
    /// 2. segment_{x}_{y}_meta.json - Metadata including bitset
    #[allow(dead_code)]
    fn flush_to_disk(&self, output_dir: &Path) -> Result<()> {
        self.flush_to_disk_with_format(output_dir, StorageFormat::Csv)
    }

    /// Flush segment to disk using the specified storage format.
    fn flush_to_disk_with_format(&self, output_dir: &Path, format: StorageFormat) -> Result<()> {
        let key = self.metadata.key;
        let base_name = format!("segment_{}_{}", key.0, key.1);

        // Write data file (appends for CSV, atomic for Parquet)
        match format {
            StorageFormat::Csv => {
                let data_path = output_dir.join(format!("{}_data.csv", base_name));
                self.write_data_csv(&data_path)?;
            }
            StorageFormat::Parquet => {
                let data_path = output_dir.join(format!("{}_data.parquet", base_name));
                let tmp_path = output_dir.join(format!(
                    "{}_data.parquet.tmp.{}",
                    base_name, self.metadata.row_count
                ));
                self.write_data_parquet(&tmp_path)?;
                self.atomic_rename(&tmp_path, &data_path)?;
            }
        }

        // Atomic metadata write; write_metadata_json handles merge internally
        let meta_path = output_dir.join(format!("{}_meta.json", base_name));
        self.write_metadata_json(&meta_path)?;

        Ok(())
    }

    /// Atomically rename a temp file to its final destination.
    fn atomic_rename(&self, tmp: &Path, final_path: &Path) -> Result<()> {
        std::fs::rename(tmp, final_path)
            .with_context(|| format!("Failed to rename {:?} -> {:?}", tmp, final_path))
    }

    /// Write rows to CSV file
    /// Appends to the file if it already exists
    fn write_data_csv(&self, path: &Path) -> Result<()> {
        let file_exists = path.exists();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .context("Failed to open data CSV file")?;

        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(file);

        // Get all column names from first row (if any)
        if let Some(first_row) = self.rows.first() {
            let mut headers: Vec<String> = first_row.fields.keys().cloned().collect();
            headers.sort(); // Sort for consistency

            // Write header only if file is new
            if !file_exists {
                writer.write_record(&headers)?;
            }

            // Write all rows
            for row in &self.rows {
                let mut values = Vec::new();
                for header in &headers {
                    let value = row.get(header).map(|s| s.as_str()).unwrap_or("");
                    values.push(value);
                }
                writer.write_record(&values)?;
            }
        }

        writer.flush()?;
        Ok(())
    }

    /// Write rows to Parquet file with Snappy compression.
    fn write_data_parquet(&self, path: &Path) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }

        // Determine schema from first row
        let first_row = self.rows.first().context("No rows to write")?;
        let mut column_names: Vec<String> = first_row.fields.keys().cloned().collect();
        column_names.sort();

        // Create Arrow schema (all Utf8 for compatibility)
        let fields: Vec<Field> = column_names
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(ArrowSchema::new(fields));

        // Build Arrow arrays
        let mut columns: Vec<ArrayRef> = Vec::new();
        for col_name in &column_names {
            let values: Vec<Option<&str>> = self
                .rows
                .iter()
                .map(|row| row.get(col_name).map(|s| s.as_str()))
                .collect();
            let array = StringArray::from(values);
            columns.push(Arc::new(array) as ArrayRef);
        }

        let batch = RecordBatch::try_new(schema.clone(), columns)
            .context("Failed to create RecordBatch")?;

        // Write to Parquet with Snappy compression
        let file = File::create(path).context("Failed to create Parquet file")?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .context("Failed to create ArrowWriter")?;
        writer.write(&batch).context("Failed to write batch")?;
        writer.close().context("Failed to close ParquetWriter")?;

        Ok(())
    }

    /// Write metadata to JSON file, merging with existing metadata if present.
    ///
    /// When a segment is flushed multiple times (e.g., threshold flush then flush_all),
    /// we need to accumulate the row_count and merge column_stats rather than overwrite.
    fn write_metadata_json(&self, path: &Path) -> Result<()> {
        let merged = if path.exists() {
            // Load existing metadata and merge
            let existing: SegmentMetadata = serde_json::from_reader(
                File::open(path).context("Failed to read existing metadata")?,
            )
            .context("Failed to parse existing metadata")?;

            let mut merged = self.metadata.clone();
            // Accumulate row_count
            merged.row_count += existing.row_count;
            // Merge bitset (OR the bits)
            merged.bitset.merge(&existing.bitset);
            // Accumulate bucket counts
            for i in 0..existing
                .bucket_counts_array
                .len()
                .min(merged.bucket_counts_array.len())
            {
                merged.bucket_counts_array[i] += existing.bucket_counts_array[i];
            }
            // Merge column stats
            for (col, existing_stats) in &existing.column_stats {
                if let Some(merged_stats) = merged.column_stats.get_mut(col) {
                    merged_stats.merge(existing_stats);
                } else {
                    merged
                        .column_stats
                        .insert(col.clone(), existing_stats.clone());
                }
            }
            merged
        } else {
            self.metadata.clone()
        };

        let file = File::create(path).context("Failed to create metadata JSON file")?;
        serde_json::to_writer_pretty(file, &merged).context("Failed to serialize metadata")?;
        Ok(())
    }
}

/// Statistics about the write session
#[derive(Debug, Clone)]
pub struct WriterStats {
    pub total_segments: usize,
    pub total_rows: usize,
}
