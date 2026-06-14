use bitvec::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Storage format for segment data files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum StorageFormat {
    /// CSV text files (legacy, backward compatible).
    #[default]
    Csv,
    /// Apache Parquet columnar files (better compression, faster reads).
    Parquet,
}

/// Routing strategy for a dimension.
///
/// - `Categorical`: Uses hash-based routing (AHasher). Best for low-cardinality strings
///   like status codes, regions, categories. No ordering is preserved.
/// - `Numeric`: Uses MARS routing (`floor(log₂(v))`). Preserves magnitude structure,
///   enabling range query pruning (e.g., `WHERE fare >= 100` skips entire buckets).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum DimensionType {
    #[default]
    Categorical,
    Numeric,
}

/// Configuration for a single routing dimension.
///
/// Each dimension has a name, a routing strategy, and a bucket count.
/// Dimensions are ordered: the first two determine segment placement,
/// and all three contribute to the existence bitset index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionConfig {
    pub name: String,
    pub dim_type: DimensionType,
}

impl DimensionConfig {
    pub fn categorical(name: impl Into<String>) -> Self {
        DimensionConfig {
            name: name.into(),
            dim_type: DimensionType::Categorical,
        }
    }

    pub fn numeric(name: impl Into<String>) -> Self {
        DimensionConfig {
            name: name.into(),
            dim_type: DimensionType::Numeric,
        }
    }
}

/// Represents a segment key based on the first two dimensions (x_bucket, y_bucket)
/// This is used to route rows to specific segments during writes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentKey(pub u8, pub u8);

impl SegmentKey {
    pub fn new(x: u8, y: u8) -> Self {
        SegmentKey(x, y)
    }
}

/// Compact bitset that encodes which value combinations exist in a segment
/// For 3 dimensions with R, S, T buckets, stores R×S×T bits
/// Example: 8×4×24 = 768 bits = 96 bytes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExistenceBitset {
    #[serde(with = "bitset_serde")]
    bits: BitVec<u8, Lsb0>,
    size: usize,
}

impl ExistenceBitset {
    /// Create a new bitset with the specified total size (R * S * T)
    pub fn new(size: usize) -> Self {
        ExistenceBitset {
            bits: bitvec![u8, Lsb0; 0; size],
            size,
        }
    }

    /// Set a bit at the given index to true
    pub fn set(&mut self, index: usize) {
        if index < self.size {
            self.bits.set(index, true);
        }
    }

    /// Check if a bit at the given index is set
    pub fn get(&self, index: usize) -> bool {
        if index < self.size {
            self.bits[index]
        } else {
            false
        }
    }

    /// Check if this bitset intersects with another (used for query filtering)
    /// Returns true if any bit is set in both bitsets
    pub fn intersects(&self, other: &ExistenceBitset) -> bool {
        if self.size != other.size {
            return false;
        }

        // Check if any bit position is set in both bitsets
        for i in 0..self.size {
            if self.bits[i] && other.bits[i] {
                return true;
            }
        }
        false
    }

    /// Get the size of the bitset
    pub fn size(&self) -> usize {
        self.size
    }

    /// Count how many bits are set to true
    pub fn count_set_bits(&self) -> usize {
        self.bits.count_ones()
    }

    /// Merge another bitset into this one (bitwise OR).
    /// Used when appending rows to an existing segment.
    pub fn merge(&mut self, other: &ExistenceBitset) {
        for i in 0..self.size.min(other.size) {
            if other.bits[i] {
                self.bits.set(i, true);
            }
        }
    }

    /// Convert to bytes for serialization
    pub fn to_bytes(&self) -> Vec<u8> {
        self.bits.as_raw_slice().to_vec()
    }

    /// Create from bytes
    pub fn from_bytes(bytes: &[u8], size: usize) -> Self {
        let bits = BitVec::from_slice(bytes);
        ExistenceBitset { bits, size }
    }
}

/// Custom serde module for BitVec serialization
mod bitset_serde {
    use super::*;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(bits: &BitVec<u8, Lsb0>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = bits.as_raw_slice();
        let encoded = STANDARD.encode(bytes);
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BitVec<u8, Lsb0>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = STANDARD.decode(&s).map_err(serde::de::Error::custom)?;
        Ok(BitVec::from_slice(&bytes))
    }
}

/// Column data types for schema enforcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnType {
    Int64,
    Float64,
    Utf8,
    Boolean,
}

impl ColumnType {
    /// Parse a string value as this type and return the f64 representation
    /// (for numeric stats tracking).
    pub fn parse_as_f64(&self, s: &str) -> Option<f64> {
        match self {
            ColumnType::Int64 => s.parse::<i64>().ok().map(|v| v as f64),
            ColumnType::Float64 => s.parse::<f64>().ok(),
            ColumnType::Utf8 => None,
            ColumnType::Boolean => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Some(1.0),
                "false" | "0" | "no" => Some(0.0),
                _ => None,
            },
        }
    }
}

/// Schema for a table column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub col_type: ColumnType,
    pub nullable: bool,
}

/// Table schema defining column names and types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(columns: Vec<ColumnSchema>) -> Self {
        TableSchema { columns }
    }

    /// Create a schema from (name, type) pairs.
    pub fn from_pairs(pairs: Vec<(&str, ColumnType)>) -> Self {
        TableSchema {
            columns: pairs
                .into_iter()
                .map(|(name, col_type)| ColumnSchema {
                    name: name.to_string(),
                    col_type,
                    nullable: true,
                })
                .collect(),
        }
    }

    /// Get the type of a column, if known.
    pub fn column_type(&self, name: &str) -> Option<ColumnType> {
        self.columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.col_type)
    }
}

/// Simple row representation with string-typed columns.
///
/// Values are stored as strings for CSV compatibility. Typed accessors
/// (`get_i64`, `get_f64`, `get_bool`) parse on access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub fields: HashMap<String, String>,
}

impl Row {
    pub fn new() -> Self {
        Row {
            fields: HashMap::new(),
        }
    }

    pub fn with_fields(fields: HashMap<String, String>) -> Self {
        Row { fields }
    }

    /// Insert a string value (always works, regardless of schema).
    pub fn insert(&mut self, key: String, value: String) {
        self.fields.insert(key, value);
    }

    /// Get the raw string value.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.fields.get(key)
    }

    /// Get a value parsed as i64.
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.fields.get(key).and_then(|v| v.parse::<i64>().ok())
    }

    /// Get a value parsed as f64.
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.fields.get(key).and_then(|v| v.parse::<f64>().ok())
    }

    /// Get a value parsed as bool.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.fields
            .get(key)
            .and_then(|v| match v.to_lowercase().as_str() {
                "true" | "1" | "yes" => Some(true),
                "false" | "0" | "no" => Some(false),
                _ => None,
            })
    }

    /// Get a numeric value, using the column type hint for accurate parsing.
    pub fn get_numeric(&self, key: &str, col_type: Option<ColumnType>) -> Option<f64> {
        let raw = self.fields.get(key)?;
        match col_type {
            Some(ColumnType::Int64) => raw.parse::<i64>().ok().map(|v| v as f64),
            Some(ColumnType::Float64) => raw.parse::<f64>().ok(),
            Some(ColumnType::Boolean) => self.get_bool(key).map(|b| if b { 1.0 } else { 0.0 }),
            _ => raw.parse::<f64>().ok(),
        }
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

/// Metadata stored per segment including routing information and existence bitset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
    /// The segment key (x_bucket, y_bucket)
    pub key: SegmentKey,

    /// Names of the routing dimensions in order [dim1, dim2, dim3]
    pub dimensions: Vec<String>,

    /// Routing type for each dimension (categorical hash vs numeric MARS).
    /// Defaults to empty (treated as all categorical) for backward compatibility.
    #[serde(default)]
    pub dimension_types: Vec<DimensionType>,

    /// Number of buckets for each dimension [R, S, T]
    pub bucket_counts: [u8; 3],

    /// Compact existence bitset (R×S×T bits)
    pub bitset: ExistenceBitset,

    /// Number of rows in this segment
    pub row_count: usize,

    /// Count of rows per bucket combination (R×S×T counts)
    /// This enables answering COUNT queries from metadata alone!
    #[serde(default)]
    pub bucket_counts_array: Vec<u32>,

    /// Per-column statistics for aggregation pushdown.
    ///
    /// For numeric columns, stores min, max, sum, and count so that
    /// COUNT, SUM, AVG, MIN, MAX can be answered without scanning rows.
    /// For categorical columns, stores only count.
    #[serde(default)]
    pub column_stats: HashMap<String, ColumnStats>,

    /// Typed table schema for this segment's columns.
    ///
    /// `None` for legacy string-only segments. When present, readers know
    /// each column's Arrow type without inferring it from data.
    #[serde(default)]
    pub schema: Option<TableSchema>,
}

impl SegmentMetadata {
    pub fn new(
        key: SegmentKey,
        dimensions: Vec<String>,
        dimension_types: Vec<DimensionType>,
        bucket_counts: [u8; 3],
    ) -> Self {
        let bitset_size =
            (bucket_counts[0] as usize) * (bucket_counts[1] as usize) * (bucket_counts[2] as usize);

        SegmentMetadata {
            key,
            dimensions,
            dimension_types,
            bucket_counts,
            bitset: ExistenceBitset::new(bitset_size),
            row_count: 0,
            bucket_counts_array: vec![0u32; bitset_size],
            column_stats: HashMap::new(),
            schema: None,
        }
    }

    /// Increment count for a specific bucket combination
    pub fn increment_bucket_count(&mut self, index: usize) {
        if index < self.bucket_counts_array.len() {
            self.bucket_counts_array[index] += 1;
        }
    }

    /// Get count for a specific bucket combination
    pub fn get_bucket_count(&self, index: usize) -> u32 {
        if index < self.bucket_counts_array.len() {
            self.bucket_counts_array[index]
        } else {
            0
        }
    }

    /// Sum counts for multiple bucket indices (for query answering)
    pub fn sum_bucket_counts(&self, indices: &[usize]) -> u64 {
        indices
            .iter()
            .map(|&i| self.get_bucket_count(i) as u64)
            .sum()
    }

    /// Update column statistics for a row being added to this segment.
    pub fn update_column_stats(
        &mut self,
        row: &Row,
        numeric_dims: &[bool],
        schema: Option<&TableSchema>,
    ) {
        for (i, dim_name) in self.dimensions.iter().enumerate() {
            let is_numeric = numeric_dims.get(i).copied().unwrap_or(false);
            if let Some(value) = row.get(dim_name) {
                let stats = self
                    .column_stats
                    .entry(dim_name.clone())
                    .or_insert_with(|| {
                        if is_numeric {
                            ColumnStats::new_numeric()
                        } else {
                            ColumnStats::new_categorical()
                        }
                    });
                let col_type = schema.and_then(|s| s.column_type(dim_name));
                stats.update_with_type(value, is_numeric, col_type);
            }
        }
        // Also track non-dimension columns
        for (col_name, value) in &row.fields {
            if !self.dimensions.contains(col_name) {
                let col_type = schema.and_then(|s| s.column_type(col_name));
                let is_numeric = col_type.is_none_or(|ct| !matches!(ct, ColumnType::Utf8));
                let stats = self
                    .column_stats
                    .entry(col_name.clone())
                    .or_insert_with(|| {
                        if is_numeric {
                            ColumnStats::new_numeric()
                        } else {
                            ColumnStats::new_categorical()
                        }
                    });
                stats.update_with_type(value, is_numeric, col_type);
            }
        }
    }

    /// Calculate the total bitset size (R * S * T)
    pub fn bitset_size(&self) -> usize {
        (self.bucket_counts[0] as usize)
            * (self.bucket_counts[1] as usize)
            * (self.bucket_counts[2] as usize)
    }
}

/// Statistics for a single column within a segment.
///
/// Enables answering aggregation queries (COUNT, SUM, AVG, MIN, MAX)
/// purely from metadata without reading any row data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Number of non-null values.
    pub count: u64,
    /// Sum of all values (parsed as f64). Zero for categorical columns.
    pub sum: f64,
    /// Minimum value (parsed as f64). f64::INFINITY if no values.
    pub min: f64,
    /// Maximum value (parsed as f64). f64::NEG_INFINITY if no values.
    pub max: f64,
    /// Whether this column is numeric (has valid sum/min/max).
    pub is_numeric: bool,
}

impl ColumnStats {
    pub fn new_numeric() -> Self {
        ColumnStats {
            count: 0,
            sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
            is_numeric: true,
        }
    }

    pub fn new_categorical() -> Self {
        ColumnStats {
            count: 0,
            sum: 0.0,
            min: 0.0,
            max: 0.0,
            is_numeric: false,
        }
    }

    /// Update stats with a new value.
    pub fn update(&mut self, value: &str, is_numeric: bool) {
        self.count += 1;
        if is_numeric {
            if let Ok(v) = value.parse::<f64>() {
                self.sum += v;
                if v < self.min {
                    self.min = v;
                }
                if v > self.max {
                    self.max = v;
                }
            }
        }
    }

    /// Update stats using type-aware parsing.
    pub fn update_with_type(
        &mut self,
        value: &str,
        is_numeric: bool,
        col_type: Option<ColumnType>,
    ) {
        self.count += 1;
        if is_numeric {
            let v = if let Some(ct) = col_type {
                ct.parse_as_f64(value)
            } else {
                value.parse::<f64>().ok()
            };
            if let Some(v) = v {
                self.sum += v;
                if v < self.min {
                    self.min = v;
                }
                if v > self.max {
                    self.max = v;
                }
            }
        }
    }

    /// Merge another ColumnStats into this one (for multi-segment aggregation).
    pub fn merge(&mut self, other: &ColumnStats) {
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }
}
