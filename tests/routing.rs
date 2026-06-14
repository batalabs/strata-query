//! Entry crate for the `routing` mirror. Declares submodules that hold the
//! actual `#[test]` functions.
//!
//! The submodule path is explicit: a top-level `tests/*.rs` file is a crate
//! root, so its child modules resolve relative to `tests/`, not `tests/routing/`.

#[path = "routing/hash.rs"]
mod hash;
