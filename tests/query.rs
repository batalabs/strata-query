//! Entry crate for the `query` mirror. Declares submodules that hold the actual
//! `#[test]` functions.
//!
//! The submodule path is explicit: a top-level `tests/*.rs` file is a crate
//! root, so its child modules resolve relative to `tests/`, not `tests/query/`.

#[path = "query/sql.rs"]
mod sql;
