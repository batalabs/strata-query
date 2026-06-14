//! Storage layer: segment writers, readers, and the write-ahead log.
//!
//! Two storage backends share STRATA's routing and existence-bitset model:
//! a CSV/text backend ([`writer`]/[`reader`]) and a Parquet backend
//! ([`parquet_writer`]/[`parquet_reader`]). [`wal`] provides crash-recovery for
//! in-flight writes.

pub mod parquet_reader;
pub mod parquet_writer;
pub mod reader;
pub mod wal;
pub mod writer;
