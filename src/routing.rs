//! Segment routing: hash-based dimension routing and existence-bitset index math.
//!
//! This is the core of STRATA's pruning model. [`hash`] maps dimension values to
//! segment buckets and computes the per-segment existence-bitset index for a row.

pub mod hash;
