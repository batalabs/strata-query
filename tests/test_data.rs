//! Mirror of `src/test_data.rs` unit tests, expressed through the public API.

use strata_query::test_data::{
    analyze_data_distribution, generate_skewed_data, generate_skewed_data_seeded,
    generate_synthetic_data, generate_synthetic_data_seeded,
};

#[test]
fn test_generate_synthetic_data() {
    let data = generate_synthetic_data(1000);
    assert_eq!(data.len(), 1000);

    // Check that all rows have required fields
    for row in &data {
        assert!(row.get("status").is_some());
        assert!(row.get("region").is_some());
        assert!(row.get("hour").is_some());
    }
}

#[test]
fn test_data_distribution() {
    let data = generate_synthetic_data(10000);
    let stats = analyze_data_distribution(&data);

    assert_eq!(stats.total_rows, 10000);

    // Should have good distribution of statuses
    assert!(stats.status_distribution.len() >= 2);

    // Should have good distribution of regions
    assert!(stats.region_distribution.len() >= 2);

    // With 10K rows and random distribution, we should see many combinations
    // but not necessarily all 288 possible ones
    assert!(stats.unique_combinations > 50);
}

#[test]
fn test_skewed_data() {
    let data = generate_skewed_data(10000);
    let stats = analyze_data_distribution(&data);

    // With skewed data, OK should be much more common than ERROR/WARN
    let ok_count = stats.status_distribution.get("OK").unwrap_or(&0);
    let error_count = stats.status_distribution.get("ERROR").unwrap_or(&0);

    assert!(ok_count > error_count);
}

/// Same seed must produce byte-for-byte identical rows. This is the
/// invariant the integration tests rely on to stay deterministic.
#[test]
fn seeded_synthetic_is_reproducible() {
    let a = generate_synthetic_data_seeded(500, 42);
    let b = generate_synthetic_data_seeded(500, 42);

    assert_eq!(a.len(), b.len());
    for (ra, rb) in a.iter().zip(b.iter()) {
        // HashMap equality is order-independent, exactly what we want.
        assert_eq!(ra.fields, rb.fields);
    }
}

/// Different seeds should (with overwhelming probability) diverge, proving
/// the seed actually drives the output rather than being ignored.
#[test]
fn seeded_synthetic_differs_across_seeds() {
    let a = generate_synthetic_data_seeded(500, 1);
    let b = generate_synthetic_data_seeded(500, 2);

    // The request_id field is drawn from a u32 per row, so at least one
    // row must differ between two different seeds.
    let any_diff = a
        .iter()
        .zip(b.iter())
        .any(|(ra, rb)| ra.fields != rb.fields);
    assert!(any_diff, "different seeds produced identical data");
}

/// The seeded skewed generator must also be reproducible.
#[test]
fn seeded_skewed_is_reproducible() {
    let a = generate_skewed_data_seeded(500, 7);
    let b = generate_skewed_data_seeded(500, 7);

    assert_eq!(a.len(), b.len());
    for (ra, rb) in a.iter().zip(b.iter()) {
        assert_eq!(ra.fields, rb.fields);
    }
}

/// The seeded generator must yield the same logical distribution as the
/// documented schema: only the three known status values, four regions,
/// and hours in `[0, 24)`.
#[test]
fn seeded_synthetic_respects_schema() {
    let data = generate_synthetic_data_seeded(2000, 99);
    for row in &data {
        let status = row.get("status").unwrap();
        assert!(
            ["OK", "ERROR", "WARN"].contains(&status.as_str()),
            "unexpected status {status}"
        );
        let region = row.get("region").unwrap();
        assert!(
            ["NYC", "SF", "LA", "CHI"].contains(&region.as_str()),
            "unexpected region {region}"
        );
        let hour: u32 = row.get("hour").unwrap().parse().unwrap();
        assert!(hour < 24, "hour {hour} out of range");
    }
}
