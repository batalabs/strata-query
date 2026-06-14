//! Query layer: SQL parsing into STRATA query predicates.
//!
//! [`sql`] turns a SQL string into a [`crate::storage::reader::QueryPredicate`]
//! that the storage layer can prune and evaluate against.

pub mod sql;
