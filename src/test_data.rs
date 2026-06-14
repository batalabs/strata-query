use crate::types::Row;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Generate synthetic test data for STRATA benchmarking
///
/// Creates rows with three dimensions:
/// - status: ['OK', 'ERROR', 'WARN'] (3 values)
/// - region: ['NYC', 'SF', 'LA', 'CHI'] (4 values)
/// - hour: [0-23] (24 values)
///
/// This gives us 3 * 4 * 24 = 288 possible value combinations
///
/// Uses an unseeded thread-local RNG, so output varies between runs.
/// For reproducible data (e.g. in tests with statistical assertions), use
/// [`generate_synthetic_data_seeded`].
///
/// # Arguments
/// * `n` - Number of rows to generate
///
/// # Returns
/// Vector of randomly generated rows
pub fn generate_synthetic_data(n: usize) -> Vec<Row> {
    generate_synthetic_data_with_rng(n, &mut rand::thread_rng())
}

/// Reproducible variant of [`generate_synthetic_data`].
///
/// Seeds a `StdRng` with `seed`, so the same seed always produces byte-for-byte
/// identical output. Used by integration tests that assert statistical thresholds
/// (hash distribution, pruning rates) which would otherwise flake under an
/// unseeded RNG.
pub fn generate_synthetic_data_seeded(n: usize, seed: u64) -> Vec<Row> {
    generate_synthetic_data_with_rng(n, &mut StdRng::seed_from_u64(seed))
}

/// Shared row-generation logic, parameterized over the RNG so both the
/// unseeded and seeded entry points produce identical distributions.
fn generate_synthetic_data_with_rng<R: Rng>(n: usize, rng: &mut R) -> Vec<Row> {
    let mut rows = Vec::with_capacity(n);

    let statuses = ["OK", "ERROR", "WARN"];
    let regions = ["NYC", "SF", "LA", "CHI"];

    for _ in 0..n {
        let mut row = Row::new();

        // Randomly select values
        let status = statuses[rng.gen_range(0..statuses.len())];
        let region = regions[rng.gen_range(0..regions.len())];
        let hour = rng.gen_range(0..24);

        row.insert("status".to_string(), status.to_string());
        row.insert("region".to_string(), region.to_string());
        row.insert("hour".to_string(), hour.to_string());

        // Add some extra data to make it more realistic
        row.insert(
            "timestamp".to_string(),
            format!("2024-01-01T{}:00:00Z", hour),
        );
        row.insert(
            "request_id".to_string(),
            format!("req_{}", rng.gen::<u32>()),
        );

        rows.push(row);
    }

    rows
}

/// Generate synthetic data with skewed distribution
/// Some combinations appear much more frequently than others
/// This is more realistic for real-world data
///
/// Uses an unseeded thread-local RNG. For reproducible data, use
/// [`generate_skewed_data_seeded`].
pub fn generate_skewed_data(n: usize) -> Vec<Row> {
    generate_skewed_data_with_rng(n, &mut rand::thread_rng())
}

/// Reproducible variant of [`generate_skewed_data`].
///
/// Seeds a `StdRng` with `seed` so output is deterministic across runs.
pub fn generate_skewed_data_seeded(n: usize, seed: u64) -> Vec<Row> {
    generate_skewed_data_with_rng(n, &mut StdRng::seed_from_u64(seed))
}

/// Shared skewed-row-generation logic, parameterized over the RNG.
fn generate_skewed_data_with_rng<R: Rng>(n: usize, rng: &mut R) -> Vec<Row> {
    let mut rows = Vec::with_capacity(n);

    for _ in 0..n {
        let mut row = Row::new();

        // Skewed distribution:
        // - 80% OK, 15% ERROR, 5% WARN
        // - 40% NYC, 30% SF, 20% LA, 10% CHI
        // - Business hours (9-17) are 2x more common
        let rand_val = rng.gen_range(0..100);
        let status = if rand_val < 80 {
            "OK"
        } else if rand_val < 95 {
            "ERROR"
        } else {
            "WARN"
        };

        let rand_val = rng.gen_range(0..100);
        let region = if rand_val < 40 {
            "NYC"
        } else if rand_val < 70 {
            "SF"
        } else if rand_val < 90 {
            "LA"
        } else {
            "CHI"
        };

        // Business hours are more common
        let hour = if rng.gen_bool(0.67) {
            // 67% chance of business hours
            rng.gen_range(9..18)
        } else {
            rng.gen_range(0..24)
        };

        row.insert("status".to_string(), status.to_string());
        row.insert("region".to_string(), region.to_string());
        row.insert("hour".to_string(), hour.to_string());

        row.insert(
            "timestamp".to_string(),
            format!("2024-01-01T{}:00:00Z", hour),
        );
        row.insert(
            "request_id".to_string(),
            format!("req_{}", rng.gen::<u32>()),
        );

        rows.push(row);
    }

    rows
}

/// Count unique value combinations in the generated data
/// Useful for understanding data distribution
pub fn analyze_data_distribution(rows: &[Row]) -> DataStats {
    use std::collections::HashSet;

    let mut combinations = HashSet::new();
    let mut status_counts = std::collections::HashMap::new();
    let mut region_counts = std::collections::HashMap::new();
    let mut hour_counts = std::collections::HashMap::new();

    for row in rows {
        let status = row.get("status").map(|s| s.as_str()).unwrap_or("");
        let region = row.get("region").map(|s| s.as_str()).unwrap_or("");
        let hour = row.get("hour").map(|s| s.as_str()).unwrap_or("");

        combinations.insert((status.to_string(), region.to_string(), hour.to_string()));

        *status_counts.entry(status.to_string()).or_insert(0) += 1;
        *region_counts.entry(region.to_string()).or_insert(0) += 1;
        *hour_counts.entry(hour.to_string()).or_insert(0) += 1;
    }

    DataStats {
        total_rows: rows.len(),
        unique_combinations: combinations.len(),
        status_distribution: status_counts,
        region_distribution: region_counts,
        hour_distribution: hour_counts,
    }
}

#[derive(Debug)]
pub struct DataStats {
    pub total_rows: usize,
    pub unique_combinations: usize,
    pub status_distribution: std::collections::HashMap<String, usize>,
    pub region_distribution: std::collections::HashMap<String, usize>,
    pub hour_distribution: std::collections::HashMap<String, usize>,
}
