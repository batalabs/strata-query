//! Entry crate for the `storage` mirror. Declares submodules that hold the
//! actual `#[test]` functions. `common` provides shared fixtures.
//!
//! Submodule paths are given explicitly: a top-level `tests/*.rs` file is a
//! crate root, so its child modules resolve relative to `tests/`, not
//! `tests/storage/`.

#[path = "common/mod.rs"]
mod common;

#[path = "storage/parquet_reader.rs"]
mod parquet_reader;
#[path = "storage/parquet_writer.rs"]
mod parquet_writer;
#[path = "storage/reader.rs"]
mod reader;
#[path = "storage/wal.rs"]
mod wal;
#[path = "storage/writer.rs"]
mod writer;
