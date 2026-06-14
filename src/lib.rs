//! STRATA - Stratified Range-Aware Table Architecture
//!
//! A research prototype for efficient multi-column query pruning in columnar databases.
//!
//! STRATA improves multi-column query performance by:
//! 1. Hash-based segment routing during writes (based on 1-2 low-cardinality columns)
//! 2. Compact existence bitsets per segment (encoding which value combinations exist)
//! 3. Provable query pruning (O((r/R)×(s/S)) segments read for queries with r and s values)
//!
//! # Example
//!
//! ```no_run
//! use strata_query::*;
//! use std::path::Path;
//!
//! // Configure and create a writer
//! let config = WriterConfig {
//!     dimensions: vec!["status".into(), "region".into(), "hour".into()],
//!     dimension_types: vec![
//!         DimensionType::Categorical,
//!         DimensionType::Categorical,
//!         DimensionType::Categorical,
//!     ],
//!     bucket_counts: [4, 4, 24],
//!     output_dir: Path::new("./data").to_path_buf(),
//!     segment_size_threshold: 10000,
//!     schema: None,
//!     storage_format: StorageFormat::Csv,
//! };
//!
//! let mut writer = StrataWriter::new(config).unwrap();
//!
//! // Write rows...
//! // writer.write_row(row).unwrap();
//!
//! writer.flush_all().unwrap();
//!
//! // Query the data
//! let reader = StrataReader::load_segments(Path::new("./data")).unwrap();
//!
//! let mut query = QueryPredicate::new();
//! query.add_filter("status".into(), vec!["ERROR".into()]);
//! query.add_filter("region".into(), vec!["NYC".into()]);
//! query.add_filter("hour".into(), vec!["9".into()]);
//!
//! // Prune segments using bitset intersection
//! let filtered_keys = reader.filter_segments(&query).unwrap();
//!
//! // Read only the necessary segments
//! let results = reader.read_and_filter(&filtered_keys, &query).unwrap();
//! ```

// Lint policy. CI enforces these as hard errors via
// `cargo clippy --all-targets -- -D warnings`.
// `clippy::pedantic` is intentionally not enabled: it currently surfaces several
// hundred findings and would gate the build without a corresponding cleanup of
// validated, paper-backing code.
#![warn(clippy::all)]

pub mod flight;
pub mod query;
pub mod routing;
pub mod server;
pub mod storage;
pub mod test_data;
pub mod types;

// Re-export commonly used types. These crate-root paths are the stable public
// API surface (e.g. `strata_query::ParquetStrataReader`); the module layout behind
// them is free to change.
pub use query::sql::{parse_query, ParsedQuery};
pub use storage::parquet_reader::{FileSizeStats, ParquetStrataReader};
pub use storage::parquet_writer::{ParquetStrataWriter, ParquetWriterConfig};
pub use storage::reader::{
    AggFunc, AggregationResult, NumericRange, QueryPredicate, ReaderStats, StrataReader,
};
pub use storage::writer::{StrataWriter, WriterConfig, WriterStats};
pub use types::{
    ColumnSchema, ColumnStats, ColumnType, DimensionConfig, DimensionType, ExistenceBitset, Row,
    SegmentKey, SegmentMetadata, StorageFormat, TableSchema,
};
