use crate::storage::reader::QueryPredicate;
use crate::types::{DimensionType, Row, SegmentKey, SegmentMetadata};
use anyhow::{Context, Result};
use arrow::array::{Array, AsArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};

/// Reader for STRATA Parquet segments with bitset-based pruning
pub struct ParquetStrataReader {
    segments: Vec<SegmentMetadata>,
    data_dir: PathBuf,
}

impl ParquetStrataReader {
    /// Load segment metadata from a directory containing Parquet files
    pub fn load_segments(data_dir: &Path) -> Result<Self> {
        let mut segments = Vec::new();

        let entries = fs::read_dir(data_dir).context("Failed to read data directory")?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json")
                && path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .map(|s| s.starts_with("segment_") && s.ends_with("_meta.json"))
                    .unwrap_or(false)
            {
                let file = File::open(&path)?;
                let reader = BufReader::new(file);
                let metadata: SegmentMetadata = serde_json::from_reader(reader)
                    .context(format!("Failed to parse metadata from {:?}", path))?;
                segments.push(metadata);
            }
        }

        Ok(ParquetStrataReader {
            segments,
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Filter segments using the query predicate (same logic as CSV version).
    pub fn filter_segments(&self, query: &QueryPredicate) -> Result<Vec<SegmentKey>> {
        if self.segments.is_empty() {
            return Ok(Vec::new());
        }

        let first_seg = &self.segments[0];

        // Backward-compatible: default to all categorical if no dimension_types stored
        let dim_types = if first_seg.dimension_types.is_empty() {
            vec![DimensionType::Categorical; first_seg.dimensions.len()]
        } else {
            first_seg.dimension_types.clone()
        };

        let query_mask =
            query.build_mask(&first_seg.dimensions, &dim_types, &first_seg.bucket_counts)?;

        let mut result = Vec::new();
        for segment in &self.segments {
            if segment.bitset.intersects(&query_mask) {
                result.push(segment.key);
            }
        }

        Ok(result)
    }

    /// Read actual row data from specified Parquet segments
    pub fn read_segments(&self, keys: &[SegmentKey]) -> Result<Vec<Row>> {
        let mut all_rows = Vec::new();

        for key in keys {
            let parquet_file = self
                .data_dir
                .join(format!("segment_{}_{}_data.parquet", key.0, key.1));

            if parquet_file.exists() {
                let rows = self.read_parquet_file(&parquet_file)?;
                all_rows.extend(rows);
            }
        }

        Ok(all_rows)
    }

    /// Read and filter rows from Parquet segments with row group pushdown.
    ///
    /// Three-level pruning:
    /// 1. **Segment pruning**: bitset intersection skips entire segments
    /// 2. **Row group pruning**: Parquet column stats skip entire row groups
    /// 3. **Row filtering**: remaining rows filtered by query.matches()
    pub fn read_and_filter(&self, keys: &[SegmentKey], query: &QueryPredicate) -> Result<Vec<Row>> {
        let mut all_rows = Vec::new();

        for key in keys {
            let parquet_file = self
                .data_dir
                .join(format!("segment_{}_{}_data.parquet", key.0, key.1));

            if parquet_file.exists() {
                let rows = self.read_parquet_file_with_pushdown(&parquet_file, query)?;
                all_rows.extend(rows);
            }
        }

        Ok(all_rows)
    }

    /// Read a Parquet file with row group pruning based on column statistics.
    ///
    /// Uses Parquet's per-row-group min/max stats to skip entire row groups
    /// that can't contain matching rows. For numeric range predicates like
    /// `fare >= 100 AND fare <= 500`, this avoids decoding row groups where
    /// all fares are < 100 or > 500.
    fn read_parquet_file_with_pushdown(
        &self,
        path: &Path,
        query: &QueryPredicate,
    ) -> Result<Vec<Row>> {
        let file = File::open(path).context("Failed to open Parquet file")?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .context("Failed to create Parquet reader builder")?;

        // Step 1: Determine which row groups to read based on column statistics
        let metadata = builder.metadata();
        let row_group_indices = self.prune_row_groups(metadata, query);

        // If all row groups pruned, return empty
        if row_group_indices.is_empty() {
            return Ok(Vec::new());
        }

        // Step 2: Build reader that only reads surviving row groups
        let builder = builder.with_row_groups(row_group_indices);
        let reader = builder.build().context("Failed to build reader")?;

        // Step 3: Read and filter remaining rows
        let mut rows = Vec::new();

        for batch_result in reader {
            let batch = batch_result.context("Failed to read RecordBatch")?;
            let schema = batch.schema();

            for row_idx in 0..batch.num_rows() {
                let mut row = Row::new();

                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let col_name = field.name().clone();
                    let array = batch.column(col_idx);

                    let value = match array.data_type() {
                        DataType::Utf8 => {
                            let str_array = array.as_string::<i32>();
                            if str_array.is_null(row_idx) {
                                continue;
                            }
                            str_array.value(row_idx).to_string()
                        }
                        DataType::Float64 => {
                            let float_array =
                                array.as_any().downcast_ref::<Float64Array>().unwrap();
                            if float_array.is_null(row_idx) {
                                continue;
                            }
                            format!("{}", float_array.value(row_idx))
                        }
                        DataType::Int64 => {
                            let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                            if int_array.is_null(row_idx) {
                                continue;
                            }
                            format!("{}", int_array.value(row_idx))
                        }
                        _ => continue,
                    };

                    row.insert(col_name, value);
                }

                // Final row-level filter (catches anything stats couldn't prune)
                if query.matches(&row) {
                    rows.push(row);
                }
            }
        }

        Ok(rows)
    }

    /// Prune row groups using Parquet column statistics.
    ///
    /// For each range filter (e.g., fare >= 100 AND fare <= 500):
    /// - If row_group_max < query_min → skip (all values below range)
    /// - If row_group_min > query_max → skip (all values above range)
    ///
    /// For exact match filters (e.g., status = 'ERROR'):
    /// - If row_group_min > value OR row_group_max < value → skip
    ///
    /// Returns indices of row groups that may contain matching rows.
    fn prune_row_groups(&self, metadata: &ParquetMetaData, query: &QueryPredicate) -> Vec<usize> {
        let num_row_groups = metadata.num_row_groups();
        let mut surviving = Vec::with_capacity(num_row_groups);

        'row_group: for rg_idx in 0..num_row_groups {
            let row_group = metadata.row_group(rg_idx);

            // Check each filter against this row group's column stats
            for (col_name, filter) in query.all_filters() {
                // Find the column in the row group by name
                let col_idx = (0..row_group.num_columns()).find(|i| {
                    let col_path = row_group.column(*i).column_path().string();
                    col_path == *col_name || col_path.ends_with(&format!(".{}", col_name))
                });

                let Some(col_idx) = col_idx else {
                    continue; // Column not in this file, can't prune
                };

                let col_meta = row_group.column(col_idx);
                let statistics = match col_meta.statistics() {
                    Some(stats) => stats,
                    _ => continue, // No stats available, can't prune
                };

                match filter {
                    crate::storage::reader::DimensionFilter::Exact(values) => {
                        // For string exact-match: check if any value could be
                        // within the column's value range
                        let any_possible = match statistics {
                            parquet::file::statistics::Statistics::ByteArray(bs) => {
                                match (bs.min_opt(), bs.max_opt()) {
                                    (Some(min_b), Some(max_b)) => {
                                        let min_str = std::str::from_utf8(min_b.data());
                                        let max_str = std::str::from_utf8(max_b.data());
                                        match (min_str, max_str) {
                                            (Ok(min_s), Ok(max_s)) => values.iter().any(|v| {
                                                v.as_str() >= min_s && v.as_str() <= max_s
                                            }),
                                            _ => true,
                                        }
                                    }
                                    _ => true, // No min/max, can't prune
                                }
                            }
                            _ => true, // Non-byte array, can't compare strings
                        };
                        if !any_possible {
                            continue 'row_group;
                        }
                    }
                    crate::storage::reader::DimensionFilter::Range(range) => {
                        // For numeric range: check overlap between
                        // [row_group_min, row_group_max] and [query_min, query_max]
                        let (rg_min, rg_max) = Self::extract_numeric_bounds(statistics);

                        if let (Some(rg_min), Some(rg_max)) = (rg_min, rg_max) {
                            let q_min = if range.min == f64::MIN {
                                f64::NEG_INFINITY
                            } else {
                                range.min
                            };
                            let q_max = if range.max == f64::MAX {
                                f64::INFINITY
                            } else {
                                range.max
                            };

                            // No overlap if row_group_max < query_min
                            // or row_group_min > query_max
                            if rg_max < q_min || rg_min > q_max {
                                continue 'row_group;
                            }
                        }
                    }
                }
            }

            surviving.push(rg_idx);
        }

        surviving
    }

    /// Extract min/max as f64 from Parquet statistics.
    fn extract_numeric_bounds(
        statistics: &parquet::file::statistics::Statistics,
    ) -> (Option<f64>, Option<f64>) {
        use parquet::file::statistics::Statistics;
        match statistics {
            Statistics::Int32(s) => {
                let min = s.min_opt().map(|&v| v as f64);
                let max = s.max_opt().map(|&v| v as f64);
                (min, max)
            }
            Statistics::Int64(s) => {
                let min = s.min_opt().map(|v| *v as f64);
                let max = s.max_opt().map(|v| *v as f64);
                (min, max)
            }
            Statistics::Float(s) => {
                let min = s.min_opt().map(|v| *v as f64);
                let max = s.max_opt().map(|v| *v as f64);
                (min, max)
            }
            Statistics::Double(s) => {
                let min = s.min_opt().copied();
                let max = s.max_opt().copied();
                (min, max)
            }
            _ => (None, None),
        }
    }

    /// Get total number of segments
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Answer COUNT(*) query from metadata alone - NO DATA READ!
    /// This is the key optimization: we sum pre-computed bucket counts.
    /// NOTE: This is APPROXIMATE due to hash collisions.
    pub fn count_from_metadata(&self, query: &QueryPredicate) -> Result<u64> {
        if self.segments.is_empty() {
            return Ok(0);
        }

        let first_seg = &self.segments[0];
        let bucket_counts = &first_seg.bucket_counts;

        // Compute which bucket indices match the query
        let query_indices =
            self.compute_query_bucket_indices(query, &first_seg.dimensions, bucket_counts);

        let mut total_count: u64 = 0;

        for segment in &self.segments {
            // Check if segment could have matching data (bitset intersection)
            // Then sum the counts for matching buckets
            for &idx in &query_indices {
                total_count += segment.get_bucket_count(idx) as u64;
            }
        }

        Ok(total_count)
    }

    /// EXACT COUNT using pruning + efficient Arrow filtering (no Row conversion!)
    /// 1. Use bitset to prune segments
    /// 2. Read only pruned segments with Arrow
    /// 3. Filter directly on Arrow arrays (vectorized, no HashMap overhead)
    pub fn count_exact(&self, query: &QueryPredicate) -> Result<u64> {
        if self.segments.is_empty() {
            return Ok(0);
        }

        // Step 1: Prune segments using bitset
        let filtered_keys = self.filter_segments(query)?;

        // Step 2: Count matching rows in each segment using Arrow directly
        let mut total_count: u64 = 0;

        for key in &filtered_keys {
            let parquet_file = self
                .data_dir
                .join(format!("segment_{}_{}_data.parquet", key.0, key.1));

            if parquet_file.exists() {
                let count = self.count_in_parquet_file(&parquet_file, query)?;
                total_count += count;
            }
        }

        Ok(total_count)
    }

    /// Count matching rows in a Parquet file using Arrow filtering (no Row conversion).
    fn count_in_parquet_file(&self, path: &Path, query: &QueryPredicate) -> Result<u64> {
        let file = File::open(path).context("Failed to open Parquet file")?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let mut count: u64 = 0;

        // Build filter sets from exact-match filters (only categorical dimensions)
        let filter_sets: Vec<(String, HashSet<String>)> = query
            .exact_filters()
            .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
            .collect();

        for batch_result in reader {
            let batch = batch_result?;

            let mut column_arrays: Vec<(&str, &StringArray)> = Vec::new();

            for (dim_name, _) in &filter_sets {
                if let Some(col) = batch.column_by_name(dim_name) {
                    if let Some(str_array) = col.as_any().downcast_ref::<StringArray>() {
                        column_arrays.push((dim_name, str_array));
                    }
                }
            }

            for row_idx in 0..batch.num_rows() {
                let mut matches = true;

                for (dim_name, values_set) in &filter_sets {
                    let col_match = column_arrays.iter().find(|(name, _)| *name == dim_name);

                    if let Some((_, str_array)) = col_match {
                        if str_array.is_null(row_idx) {
                            matches = false;
                            break;
                        }
                        let value = str_array.value(row_idx);
                        if !values_set.contains(value) {
                            matches = false;
                            break;
                        }
                    } else {
                        matches = false;
                        break;
                    }
                }

                if matches {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Compute bucket indices for a query (cross product of dimension buckets).
    fn compute_query_bucket_indices(
        &self,
        query: &QueryPredicate,
        dimensions: &[String],
        bucket_counts: &[u8; 3],
    ) -> Vec<usize> {
        let r = bucket_counts[0] as usize;
        let s = bucket_counts[1] as usize;
        let t = bucket_counts[2] as usize;

        // Get bucket indices for each dimension using unified routing
        let dim1_buckets: Vec<usize> = match query.get_exact_values(&dimensions[0]) {
            Some(vals) if !vals.is_empty() => vals
                .iter()
                .map(|v| crate::routing::hash::route_value(v, bucket_counts[0]) as usize)
                .collect(),
            _ => (0..r).collect(),
        };

        let dim2_buckets: Vec<usize> = match query.get_exact_values(&dimensions[1]) {
            Some(vals) if !vals.is_empty() => vals
                .iter()
                .map(|v| crate::routing::hash::route_value(v, bucket_counts[1]) as usize)
                .collect(),
            _ => (0..s).collect(),
        };

        let dim3_buckets: Vec<usize> = match query.get_exact_values(&dimensions[2]) {
            Some(vals) if !vals.is_empty() => vals
                .iter()
                .map(|v| crate::routing::hash::route_value(v, bucket_counts[2]) as usize)
                .collect(),
            _ => (0..t).collect(),
        };

        let mut indices = Vec::new();
        for &x in &dim1_buckets {
            for &y in &dim2_buckets {
                for &z in &dim3_buckets {
                    indices.push(x * s * t + y * t + z);
                }
            }
        }

        indices
    }

    /// Read a Parquet file and convert to Rows
    fn read_parquet_file(&self, path: &Path) -> Result<Vec<Row>> {
        let file = File::open(path).context("Failed to open Parquet file")?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .context("Failed to create ParquetRecordBatchReaderBuilder")?;

        let reader = builder.build().context("Failed to build reader")?;

        let mut rows = Vec::new();

        // Read all batches
        for batch in reader {
            let batch = batch.context("Failed to read RecordBatch")?;
            let schema = batch.schema();

            // Convert each row in the batch
            for row_idx in 0..batch.num_rows() {
                let mut row = Row::new();

                // Extract each column value
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let col_name = field.name().clone();
                    let array = batch.column(col_idx);

                    // Handle different data types
                    let value = match array.data_type() {
                        DataType::Utf8 => {
                            let string_array = array.as_string::<i32>();
                            if string_array.is_null(row_idx) {
                                continue; // Skip null values
                            }
                            string_array.value(row_idx).to_string()
                        }
                        _ => {
                            // For non-string types, we need to handle them properly
                            // Since our data is written as strings, this shouldn't happen
                            // but let's handle it gracefully
                            continue;
                        }
                    };

                    row.insert(col_name, value);
                }

                rows.push(row);
            }
        }

        Ok(rows)
    }

    /// Get file size statistics
    pub fn get_file_sizes(&self) -> Result<FileSizeStats> {
        let mut total_parquet_size = 0u64;
        let mut total_metadata_size = 0u64;
        let mut segment_count = 0;

        for segment in &self.segments {
            let key = segment.key;
            let parquet_file = self
                .data_dir
                .join(format!("segment_{}_{}_data.parquet", key.0, key.1));
            let meta_file = self
                .data_dir
                .join(format!("segment_{}_{}_meta.json", key.0, key.1));

            if parquet_file.exists() {
                let metadata = fs::metadata(&parquet_file)?;
                total_parquet_size += metadata.len();
            }

            if meta_file.exists() {
                let metadata = fs::metadata(&meta_file)?;
                total_metadata_size += metadata.len();
            }

            segment_count += 1;
        }

        Ok(FileSizeStats {
            total_parquet_size,
            total_metadata_size,
            segment_count,
            avg_parquet_size: if segment_count > 0 {
                total_parquet_size / segment_count as u64
            } else {
                0
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct FileSizeStats {
    pub total_parquet_size: u64,
    pub total_metadata_size: u64,
    pub segment_count: usize,
    pub avg_parquet_size: u64,
}
