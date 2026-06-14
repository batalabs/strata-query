use crate::routing::hash::{route_dimension, route_numeric_range};
use crate::types::{DimensionType, ExistenceBitset, Row, SegmentKey, SegmentMetadata};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Range filter for numeric dimensions
// ---------------------------------------------------------------------------

/// A numeric range constraint for use in query predicates.
///
/// Supports inclusive range queries like `WHERE fare >= 100 AND fare <= 500`.
/// When used with MARS routing, only buckets overlapping `[min, max]` are
/// checked, enabling provable pruning of entire bucket ranges.
#[derive(Debug, Clone)]
pub struct NumericRange {
    pub min: f64,
    pub max: f64,
}

impl NumericRange {
    pub fn new(min: f64, max: f64) -> Self {
        NumericRange { min, max }
    }

    /// Create an open-ended range `[min, +∞)`
    pub fn min_only(min: f64) -> Self {
        NumericRange { min, max: f64::MAX }
    }

    /// Create an open-ended range `(-∞, max]`
    pub fn max_only(max: f64) -> Self {
        NumericRange { min: f64::MIN, max }
    }

    /// Check whether a value falls within this range
    pub fn contains(&self, value: f64) -> bool {
        value >= self.min && value <= self.max
    }
}

// ---------------------------------------------------------------------------
// Query predicate
// ---------------------------------------------------------------------------

/// Filter type for a single dimension in a query predicate.
#[derive(Debug, Clone)]
pub enum DimensionFilter {
    /// Exact-match filter: value must be in the given set (categorical).
    Exact(Vec<String>),
    /// Range filter: value must fall within `[min, max]` (numeric).
    Range(NumericRange),
}

/// Represents a multi-column query predicate.
///
/// Supports both exact-match filters (for categorical dimensions) and
/// range filters (for numeric dimensions with MARS routing).
///
/// # Examples
/// ```no_run
/// use strata_query::{QueryPredicate, NumericRange};
///
/// let mut query = QueryPredicate::new();
/// // Categorical: status IN ('ERROR', 'WARN')
/// query.add_filter("status".into(), vec!["ERROR".into(), "WARN".into()]);
/// // Numeric: fare >= 100 AND fare <= 500
/// query.add_range_filter("fare".into(), 100.0, 500.0);
/// ```
#[derive(Debug, Clone)]
pub struct QueryPredicate {
    /// Map from dimension name to filter (exact or range).
    filters: HashMap<String, DimensionFilter>,
}

impl QueryPredicate {
    /// Create a new empty query predicate.
    pub fn new() -> Self {
        QueryPredicate {
            filters: HashMap::new(),
        }
    }

    /// Add an exact-match filter for a categorical dimension.
    ///
    /// Translates to `WHERE dimension IN (values...)`.
    pub fn add_filter(&mut self, dimension: String, values: Vec<String>) {
        self.filters
            .insert(dimension, DimensionFilter::Exact(values));
    }

    /// Add a range filter for a numeric dimension.
    ///
    /// Translates to `WHERE dimension >= min AND dimension <= max`.
    /// With MARS routing, only buckets overlapping the range are checked.
    pub fn add_range_filter(&mut self, dimension: String, min: f64, max: f64) {
        self.filters.insert(
            dimension,
            DimensionFilter::Range(NumericRange::new(min, max)),
        );
    }

    /// Add a lower-bound filter: `WHERE dimension >= min`.
    ///
    /// If a range filter already exists for this dimension, the min is narrowed.
    pub fn add_min_filter(&mut self, dimension: String, min: f64) {
        self.filters
            .entry(dimension)
            .and_modify(|existing| {
                if let DimensionFilter::Range(ref mut range) = existing {
                    range.min = range.min.max(min);
                }
            })
            .or_insert(DimensionFilter::Range(NumericRange::min_only(min)));
    }

    /// Add an upper-bound filter: `WHERE dimension <= max`.
    ///
    /// If a range filter already exists for this dimension, the max is narrowed.
    pub fn add_max_filter(&mut self, dimension: String, max: f64) {
        self.filters
            .entry(dimension)
            .and_modify(|existing| {
                if let DimensionFilter::Range(ref mut range) = existing {
                    range.max = range.max.min(max);
                }
            })
            .or_insert(DimensionFilter::Range(NumericRange::max_only(max)));
    }

    /// Build a query mask bitset from the predicate and segment metadata.
    ///
    /// For each dimension:
    /// - **Exact filter** → routes each value via hash (categorical) or MARS (numeric)
    /// - **Range filter** → computes overlapping MARS buckets via `route_numeric_range`
    /// - **Unspecified** → matches all buckets for that dimension
    ///
    /// # Arguments
    /// * `dimensions` - Ordered dimension names `[dim1, dim2, dim3]`
    /// * `dimension_types` - Routing strategy for each dimension
    /// * `bucket_counts` - Bucket counts `[R, S, T]`
    pub fn build_mask(
        &self,
        dimensions: &[String],
        dimension_types: &[DimensionType],
        bucket_counts: &[u8; 3],
    ) -> Result<ExistenceBitset> {
        let r = bucket_counts[0] as usize;
        let s = bucket_counts[1] as usize;
        let t = bucket_counts[2] as usize;
        let bitset_size = r * s * t;

        let mut mask = ExistenceBitset::new(bitset_size);

        // Resolve each dimension to a set of bucket indices
        let dim_buckets: [Vec<usize>; 3] = [
            self.resolve_buckets(0, dimensions, dimension_types, bucket_counts),
            self.resolve_buckets(1, dimensions, dimension_types, bucket_counts),
            self.resolve_buckets(2, dimensions, dimension_types, bucket_counts),
        ];

        // Set bits for the cross product of all bucket combinations
        for &x in &dim_buckets[0] {
            for &y in &dim_buckets[1] {
                for &z in &dim_buckets[2] {
                    let index = x * s * t + y * t + z;
                    mask.set(index);
                }
            }
        }

        Ok(mask)
    }

    /// Resolve a dimension's filter to a set of bucket indices.
    ///
    /// - Exact filters route each value via the appropriate strategy (hash or MARS).
    /// - Range filters use `route_numeric_range` for MARS-based bucket enumeration.
    /// - Unspecified dimensions match all buckets.
    fn resolve_buckets(
        &self,
        dim_idx: usize,
        dimensions: &[String],
        dimension_types: &[DimensionType],
        bucket_counts: &[u8; 3],
    ) -> Vec<usize> {
        let num_buckets = bucket_counts[dim_idx] as usize;
        let dim_name = &dimensions[dim_idx];
        let dim_type = dimension_types
            .get(dim_idx)
            .copied()
            .unwrap_or(DimensionType::Categorical);

        match self.filters.get(dim_name) {
            Some(DimensionFilter::Exact(values)) => values
                .iter()
                .map(|v| route_dimension(v, dim_type, bucket_counts[dim_idx]) as usize)
                .collect(),

            Some(DimensionFilter::Range(range)) => {
                // Range filter requires numeric routing
                route_numeric_range(range.min, range.max, bucket_counts[dim_idx])
                    .into_iter()
                    .map(|b| b as usize)
                    .collect()
            }

            None => (0..num_buckets).collect(),
        }
    }

    /// Check if a row matches this predicate (exact-value and range filters).
    pub fn matches(&self, row: &Row) -> bool {
        for (dimension, filter) in &self.filters {
            let Some(row_value) = row.get(dimension) else {
                return false;
            };

            match filter {
                DimensionFilter::Exact(values) => {
                    if !values.contains(row_value) {
                        return false;
                    }
                }
                DimensionFilter::Range(range) => {
                    let Ok(v) = row_value.parse::<f64>() else {
                        return false;
                    };
                    if !range.contains(v) {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Returns an iterator over exact-match filters (dimension name → values).
    ///
    /// Used by Parquet readers for Arrow-based filtering where only
    /// categorical exact-match filters are applied directly on arrays.
    pub fn exact_filters(&self) -> impl Iterator<Item = (&String, &Vec<String>)> {
        self.filters.iter().filter_map(|(k, v)| match v {
            DimensionFilter::Exact(vals) => Some((k, vals)),
            _ => None,
        })
    }

    /// Get exact-match values for a dimension, if an exact filter is set.
    ///
    /// Returns `None` for range filters or unfiltered dimensions.
    pub fn get_exact_values(&self, dimension: &str) -> Option<&Vec<String>> {
        match self.filters.get(dimension)? {
            DimensionFilter::Exact(vals) => Some(vals),
            _ => None,
        }
    }

    /// Get all filters (both exact-match and range).
    ///
    /// Used by Parquet row group pushdown to inspect all predicates.
    pub fn all_filters(&self) -> impl Iterator<Item = (&String, &DimensionFilter)> {
        self.filters.iter()
    }

    /// Get the range filter for a dimension, if one exists.
    pub fn get_range(&self, dimension: &str) -> Option<&NumericRange> {
        match self.filters.get(dimension)? {
            DimensionFilter::Range(range) => Some(range),
            _ => None,
        }
    }
}

impl Default for QueryPredicate {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Reader for STRATA segments with bitset-based pruning.
///
/// Loads segment metadata from disk and uses existence bitsets to prune segments
/// that provably contain no matching rows. For numeric dimensions with MARS routing,
/// range queries skip entire bucket ranges; this is the key performance win.
pub struct StrataReader {
    /// Metadata for all segments (loaded from `*_meta.json` files)
    segments: Vec<SegmentMetadata>,

    /// Directory containing segment files
    data_dir: PathBuf,
}

impl StrataReader {
    /// Load segment metadata from a directory.
    ///
    /// Reads all `segment_*_meta.json` files and deserializes the metadata
    /// (including existence bitsets and dimension types).
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

        Ok(StrataReader {
            segments,
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Filter segments using the query predicate.
    ///
    /// Returns segment keys that are provably non-empty for the given query.
    /// This is the core pruning operation: segments whose existence bitset
    /// does not intersect the query mask are skipped entirely.
    pub fn filter_segments(&self, query: &QueryPredicate) -> Result<Vec<SegmentKey>> {
        if self.segments.is_empty() {
            return Ok(Vec::new());
        }

        let first_seg = &self.segments[0];

        // Backward-compatible: if segment has no dimension_types, default to all categorical
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

    /// Read actual row data from specified segments.
    /// Auto-detects Parquet vs CSV based on file extension.
    pub fn read_segments(&self, keys: &[SegmentKey]) -> Result<Vec<Row>> {
        let mut all_rows = Vec::new();

        for key in keys {
            // Try Parquet first, fall back to CSV
            let parquet_file = self
                .data_dir
                .join(format!("segment_{}_{}_data.parquet", key.0, key.1));
            let csv_file = self
                .data_dir
                .join(format!("segment_{}_{}_data.csv", key.0, key.1));

            if parquet_file.exists() {
                let rows = self.read_parquet_file(&parquet_file)?;
                all_rows.extend(rows);
            } else if csv_file.exists() {
                let rows = self.read_csv_file(&csv_file)?;
                all_rows.extend(rows);
            }
        }

        Ok(all_rows)
    }

    /// Read and filter rows from specified segments using the query predicate.
    pub fn read_and_filter(&self, keys: &[SegmentKey], query: &QueryPredicate) -> Result<Vec<Row>> {
        let all_rows = self.read_segments(keys)?;

        let filtered: Vec<Row> = all_rows
            .into_iter()
            .filter(|row| query.matches(row))
            .collect();

        Ok(filtered)
    }

    /// Read all segments without pruning (for comparison/benchmarks).
    pub fn read_all_segments(&self) -> Result<Vec<Row>> {
        let all_keys: Vec<SegmentKey> = self.segments.iter().map(|s| s.key).collect();
        self.read_segments(&all_keys)
    }

    /// Get total number of segments.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Get statistics about segment distribution.
    pub fn get_stats(&self) -> ReaderStats {
        let total_rows: usize = self.segments.iter().map(|s| s.row_count).sum();
        let total_segments = self.segments.len();

        ReaderStats {
            total_segments,
            total_rows,
            avg_rows_per_segment: if total_segments > 0 {
                total_rows / total_segments
            } else {
                0
            },
        }
    }

    fn read_csv_file(&self, path: &Path) -> Result<Vec<Row>> {
        let file = File::open(path)?;
        let mut reader = csv::ReaderBuilder::new().flexible(true).from_reader(file);

        let headers = reader.headers()?.clone();
        let mut rows = Vec::new();

        for result in reader.records() {
            let record = result?;
            let mut row = Row::new();

            for (i, value) in record.iter().enumerate() {
                if let Some(header) = headers.get(i) {
                    row.insert(header.to_string(), value.to_string());
                }
            }

            rows.push(row);
        }

        Ok(rows)
    }

    /// Read rows from a Parquet segment file.
    fn read_parquet_file(&self, path: &Path) -> Result<Vec<Row>> {
        let file = File::open(path)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let mut rows = Vec::new();
        for batch_result in reader {
            let batch = batch_result?;
            let schema = batch.schema();

            for row_idx in 0..batch.num_rows() {
                let mut row = Row::new();
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let col = batch.column(col_idx);
                    let value = if col.is_null(row_idx) {
                        continue;
                    } else if let Some(arr) =
                        col.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        arr.value(row_idx).to_string()
                    } else if let Some(arr) =
                        col.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        arr.value(row_idx).to_string()
                    } else if let Some(arr) =
                        col.as_any().downcast_ref::<arrow::array::Float64Array>()
                    {
                        arr.value(row_idx).to_string()
                    } else if let Some(arr) =
                        col.as_any().downcast_ref::<arrow::array::Int32Array>()
                    {
                        arr.value(row_idx).to_string()
                    } else if let Some(arr) =
                        col.as_any().downcast_ref::<arrow::array::BooleanArray>()
                    {
                        arr.value(row_idx).to_string()
                    } else {
                        continue;
                    };
                    row.insert(field.name().clone(), value);
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }
}

/// Statistics about loaded segments.
#[derive(Debug, Clone)]
pub struct ReaderStats {
    pub total_segments: usize,
    pub total_rows: usize,
    pub avg_rows_per_segment: usize,
}

// ---------------------------------------------------------------------------
// Aggregation queries (metadata-only)
// ---------------------------------------------------------------------------

/// Supported aggregate functions.
#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Result of a metadata-only aggregation query.
#[derive(Debug, Clone)]
pub struct AggregationResult {
    pub function: AggFunc,
    pub column: String,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl AggregationResult {
    pub fn value(&self) -> f64 {
        match self.function {
            AggFunc::Count => self.count as f64,
            AggFunc::Sum => self.sum,
            AggFunc::Avg => {
                if self.count > 0 {
                    self.sum / self.count as f64
                } else {
                    0.0
                }
            }
            AggFunc::Min => self.min,
            AggFunc::Max => self.max,
        }
    }
}

impl StrataReader {
    /// Execute a metadata-only aggregation across pruned segments.
    ///
    /// Returns aggregation results without reading any row data.
    /// For COUNT, uses row_count per segment. For SUM/AVG/MIN/MAX,
    /// uses per-column statistics collected during writes.
    ///
    /// Returns None if the column has no statistics (pre-aggregation data).
    pub fn aggregate(
        &self,
        pred: &QueryPredicate,
        func: AggFunc,
        column: &str,
    ) -> Result<Option<AggregationResult>> {
        let matching_keys = self.filter_segments(pred)?;

        let mut total_count: u64 = 0;
        let mut total_sum: f64 = 0.0;
        let mut global_min: f64 = f64::MAX;
        let mut global_max: f64 = f64::MIN;
        let mut has_stats = false;

        for key in &matching_keys {
            if let Some(seg) = self.segments.iter().find(|s| s.key == *key) {
                if func == AggFunc::Count && column == "*" {
                    total_count += seg.row_count as u64;
                    has_stats = true;
                    continue;
                }

                if let Some(stats) = seg.column_stats.get(column) {
                    has_stats = true;
                    total_count += stats.count;
                    total_sum += stats.sum;
                    if stats.min < global_min {
                        global_min = stats.min;
                    }
                    if stats.max > global_max {
                        global_max = stats.max;
                    }
                }
            }
        }

        if !has_stats {
            return Ok(None);
        }

        // For COUNT on a specific column, just use total_count
        if func == AggFunc::Count && column != "*" {
            return Ok(Some(AggregationResult {
                function: func,
                column: column.to_string(),
                count: total_count,
                sum: 0.0,
                min: 0.0,
                max: 0.0,
            }));
        }

        if func == AggFunc::Count {
            return Ok(Some(AggregationResult {
                function: func,
                column: column.to_string(),
                count: total_count,
                sum: 0.0,
                min: 0.0,
                max: 0.0,
            }));
        }

        Ok(Some(AggregationResult {
            function: func,
            column: column.to_string(),
            count: total_count,
            sum: total_sum,
            min: global_min,
            max: global_max,
        }))
    }

    /// Execute multiple aggregations at once (same predicate, different functions/columns).
    pub fn aggregate_multi(
        &self,
        pred: &QueryPredicate,
        aggregations: &[(AggFunc, &str)],
    ) -> Result<Vec<Option<AggregationResult>>> {
        let matching_keys = self.filter_segments(pred)?;

        let mut results: Vec<Option<AggregationResult>> = Vec::with_capacity(aggregations.len());

        for (func, column) in aggregations {
            let mut total_count: u64 = 0;
            let mut total_sum: f64 = 0.0;
            let mut global_min: f64 = f64::MAX;
            let mut global_max: f64 = f64::MIN;
            let mut has_stats = false;

            for key in &matching_keys {
                if let Some(seg) = self.segments.iter().find(|s| s.key == *key) {
                    if *func == AggFunc::Count && *column == "*" {
                        total_count += seg.row_count as u64;
                        has_stats = true;
                        continue;
                    }

                    if let Some(stats) = seg.column_stats.get(*column) {
                        has_stats = true;
                        total_count += stats.count;
                        total_sum += stats.sum;
                        if stats.min < global_min {
                            global_min = stats.min;
                        }
                        if stats.max > global_max {
                            global_max = stats.max;
                        }
                    }
                }
            }

            if !has_stats {
                results.push(None);
            } else {
                results.push(Some(AggregationResult {
                    function: func.clone(),
                    column: column.to_string(),
                    count: total_count,
                    sum: total_sum,
                    min: global_min,
                    max: global_max,
                }));
            }
        }

        Ok(results)
    }
}
