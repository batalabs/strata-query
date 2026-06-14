use crate::types::{DimensionType, Row, SegmentKey};

// ---------------------------------------------------------------------------
// Core routing functions
// ---------------------------------------------------------------------------

/// Routes a string value to a bucket using fast non-cryptographic hashing.
///
/// Best suited for categorical dimensions (status codes, regions, categories)
/// where ordering doesn't matter.
///
/// The hash uses **fixed seeds**, so routing is deterministic and stable across
/// process runs. This is required for persistence: a value written in one process
/// must route to the same bucket when queried in another, or the stored existence
/// bitsets and the query-time mask would disagree (causing false negatives).
/// `AHasher::default()` must NOT be used here; it seeds from process-random state.
///
/// # Arguments
/// * `value` - The string value to hash
/// * `num_buckets` - Total number of buckets (must be > 0)
///
/// # Returns
/// Bucket index in range `[0, num_buckets)`
pub fn route_value(value: &str, num_buckets: u8) -> u8 {
    // Fixed seeds (digits of π) → stable, reproducible routing across processes.
    let state = ahash::RandomState::with_seeds(
        0x243f_6a88_85a3_08d3,
        0x1319_8a2e_0370_7344,
        0xa409_3822_299f_31d0,
        0x082e_fa98_ec4e_6c89,
    );
    let hash = state.hash_one(value);
    (hash % num_buckets as u64) as u8
}

/// MARS routing: maps a numeric value to a bucket using `floor(log₂(|v|))`.
///
/// This is the core MARS (Magnitude-Aware Routing System) function. It preserves
/// magnitude ordering so that range queries (e.g. `WHERE fare >= 100`) can skip
/// entire buckets. Bucket `k` contains values in `[2^k, 2^(k+1))`.
///
/// **Near-optimality guarantee**: For any ratio-based query `v ∈ [a, b]`,
/// the number of buckets touched is at most `⌈log₂(b/a)⌉ + 1`.
///
/// # Arguments
/// * `value` - The numeric value (stored as a string in the row, parsed as f64)
/// * `num_buckets` - Total number of buckets (must be > 0)
///
/// # Returns
/// Bucket index in range `[0, num_buckets)`. Returns `0` for non-positive or
/// unparseable values.
pub fn route_numeric(value: &str, num_buckets: u8) -> u8 {
    let v: f64 = match value.parse() {
        Ok(v) => v,
        Err(_) => return 0,
    };

    if v <= 0.0 {
        return 0;
    }

    // floor(log2(v)) gives the magnitude bucket
    let log_bucket = v.log2().floor() as u8;

    // Clamp to valid bucket range
    log_bucket.min(num_buckets - 1)
}

/// Returns all bucket indices that overlap with a numeric range `[min, max]`.
///
/// Used during query processing to determine which MARS buckets could contain
/// values in the given range. Only those buckets need to be checked; all
/// others can be pruned.
///
/// # Complexity
/// At most `⌈log₂(max/min)⌉ + 1` buckets are returned (the near-optimality bound).
///
/// # Arguments
/// * `min` - Minimum value of the range (inclusive)
/// * `max` - Maximum value of the range (inclusive)
/// * `num_buckets` - Total number of buckets
///
/// # Returns
/// Sorted, deduplicated vector of bucket indices.
pub fn route_numeric_range(min: f64, max: f64, num_buckets: u8) -> Vec<u8> {
    if num_buckets == 0 || min > max {
        return Vec::new();
    }

    let lo = min.max(0.0);
    let hi = max.max(0.0);

    if hi <= 0.0 {
        return vec![0];
    }

    let start_bucket = if lo <= 0.0 {
        0u8
    } else {
        (lo.log2().floor() as u8).min(num_buckets - 1)
    };

    let end_bucket = if hi <= 0.0 {
        0u8
    } else {
        (hi.log2().floor() as u8).min(num_buckets - 1)
    };

    (start_bucket..=end_bucket).collect()
}

/// Unified routing function that dispatches to hash or MARS routing
/// based on the dimension type.
///
/// # Arguments
/// * `value` - The value as a string (parsed to f64 for numeric dimensions)
/// * `dim_type` - Whether to use categorical (hash) or numeric (MARS) routing
/// * `num_buckets` - Total number of buckets
///
/// # Returns
/// Bucket index in range `[0, num_buckets)`
pub fn route_dimension(value: &str, dim_type: DimensionType, num_buckets: u8) -> u8 {
    match dim_type {
        DimensionType::Categorical => route_value(value, num_buckets),
        DimensionType::Numeric => route_numeric(value, num_buckets),
    }
}

// ---------------------------------------------------------------------------
// Segment key and bitset index computation
// ---------------------------------------------------------------------------

/// Computes the segment key for a row based on the first two routing dimensions.
///
/// Each dimension is routed independently: categorical dimensions use hashing,
/// numeric dimensions use MARS routing. The resulting `(x_bucket, y_bucket)` pair
/// determines which segment file the row is written to.
///
/// # Arguments
/// * `row` - The row to route
/// * `dimensions` - Names of routing dimensions `[dim1, dim2, dim3]`
/// * `dimension_types` - Routing strategy for each dimension
/// * `bucket_counts` - Number of buckets for first two dimensions `[R, S]`
///
/// # Returns
/// `SegmentKey(x_bucket, y_bucket)` where `x ∈ [0, R)` and `y ∈ [0, S)`
pub fn compute_segment_key(
    row: &Row,
    dimensions: &[String],
    dimension_types: &[DimensionType],
    bucket_counts: &[u8; 2],
) -> SegmentKey {
    let dim1_value = row.get(&dimensions[0]).map(|s| s.as_str()).unwrap_or("");
    let dim2_value = row.get(&dimensions[1]).map(|s| s.as_str()).unwrap_or("");

    let dt1 = dimension_types
        .first()
        .copied()
        .unwrap_or(DimensionType::Categorical);
    let dt2 = dimension_types
        .get(1)
        .copied()
        .unwrap_or(DimensionType::Categorical);

    let x_bucket = route_dimension(dim1_value, dt1, bucket_counts[0]);
    let y_bucket = route_dimension(dim2_value, dt2, bucket_counts[1]);

    SegmentKey::new(x_bucket, y_bucket)
}

/// Computes the bitset index for a row based on all three routing dimensions.
///
/// The index is calculated as: `x * S * T + y * T + z` where `x`, `y`, `z` are the
/// bucket indices for the three dimensions (routed via hash or MARS depending on type).
///
/// # Arguments
/// * `row` - The row to compute index for
/// * `dimensions` - Names of routing dimensions `[dim1, dim2, dim3]`
/// * `dimension_types` - Routing strategy for each dimension
/// * `bucket_counts` - Number of buckets for each dimension `[R, S, T]`
///
/// # Returns
/// Index into the bitset in range `[0, R*S*T)`
pub fn compute_bitset_index(
    row: &Row,
    dimensions: &[String],
    dimension_types: &[DimensionType],
    bucket_counts: &[u8; 3],
) -> usize {
    let dim1_value = row.get(&dimensions[0]).map(|s| s.as_str()).unwrap_or("");
    let dim2_value = row.get(&dimensions[1]).map(|s| s.as_str()).unwrap_or("");
    let dim3_value = row.get(&dimensions[2]).map(|s| s.as_str()).unwrap_or("");

    let dt1 = dimension_types
        .first()
        .copied()
        .unwrap_or(DimensionType::Categorical);
    let dt2 = dimension_types
        .get(1)
        .copied()
        .unwrap_or(DimensionType::Categorical);
    let dt3 = dimension_types
        .get(2)
        .copied()
        .unwrap_or(DimensionType::Categorical);

    let x = route_dimension(dim1_value, dt1, bucket_counts[0]) as usize;
    let y = route_dimension(dim2_value, dt2, bucket_counts[1]) as usize;
    let z = route_dimension(dim3_value, dt3, bucket_counts[2]) as usize;

    let s = bucket_counts[1] as usize;
    let t = bucket_counts[2] as usize;

    x * s * t + y * t + z
}

/// Computes all bitset indices for exact-match queries on all three dimensions.
///
/// Used for categorical queries where you know the exact values to match.
/// Each dimension's values are independently routed and the cross product is computed.
///
/// # Arguments
/// * `dim1_values` - Values for first dimension
/// * `dim2_values` - Values for second dimension
/// * `dim3_values` - Values for third dimension
/// * `bucket_counts` - Number of buckets for each dimension `[R, S, T]`
///
/// # Returns
/// Vector of all bitset indices corresponding to the value combinations
pub fn compute_query_bitset_indices(
    dim1_values: &[String],
    dim2_values: &[String],
    dim3_values: &[String],
    bucket_counts: &[u8; 3],
) -> Vec<usize> {
    let mut indices = Vec::new();

    let s = bucket_counts[1] as usize;
    let t = bucket_counts[2] as usize;

    for dim1_val in dim1_values {
        for dim2_val in dim2_values {
            for dim3_val in dim3_values {
                let x = route_value(dim1_val, bucket_counts[0]) as usize;
                let y = route_value(dim2_val, bucket_counts[1]) as usize;
                let z = route_value(dim3_val, bucket_counts[2]) as usize;

                let index = x * s * t + y * t + z;
                indices.push(index);
            }
        }
    }

    indices
}
