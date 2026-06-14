use std::path::Path;
use strata_query::*;

fn main() -> anyhow::Result<()> {
    println!("STRATA MVP - Stratified Range-Aware Table Architecture");
    println!("======================================================\n");

    // Generate synthetic data
    println!("Generating 10,000 synthetic rows...");
    let data = test_data::generate_synthetic_data(10_000);

    let stats = test_data::analyze_data_distribution(&data);
    println!("Data statistics:");
    println!("  Total rows: {}", stats.total_rows);
    println!("  Unique combinations: {}", stats.unique_combinations);
    println!();

    // Configure writer
    let output_dir = Path::new("./strata_output");
    std::fs::create_dir_all(output_dir)?;

    let config = WriterConfig {
        dimensions: vec![
            "status".to_string(),
            "region".to_string(),
            "hour".to_string(),
        ],
        dimension_types: vec![
            DimensionType::Categorical,
            DimensionType::Categorical,
            DimensionType::Categorical,
        ],
        bucket_counts: [4, 4, 24],
        output_dir: output_dir.to_path_buf(),
        segment_size_threshold: 1000,
        schema: None,
        storage_format: strata_query::StorageFormat::Csv,
    };

    // Write data
    println!("Writing data with STRATA (R=4, S=4, T=24)...");
    let mut writer = StrataWriter::new(config)?;

    for row in data {
        writer.write_row(row)?;
    }

    writer.flush_all()?;
    println!("Write complete!");
    println!();

    // Read and query
    println!("Loading segments...");
    let reader = StrataReader::load_segments(output_dir)?;
    let reader_stats = reader.get_stats();

    println!("Segment statistics:");
    println!("  Total segments: {}", reader_stats.total_segments);
    println!("  Total rows: {}", reader_stats.total_rows);
    println!("  Avg rows/segment: {}", reader_stats.avg_rows_per_segment);
    println!();

    // Query 1: Single value per dimension
    println!("Query 1: status='ERROR' AND region='NYC' AND hour='9'");
    let mut query1 = QueryPredicate::new();
    query1.add_filter("status".to_string(), vec!["ERROR".to_string()]);
    query1.add_filter("region".to_string(), vec!["NYC".to_string()]);
    query1.add_filter("hour".to_string(), vec!["9".to_string()]);

    let filtered_keys1 = reader.filter_segments(&query1)?;
    let results1 = reader.read_and_filter(&filtered_keys1, &query1)?;

    println!(
        "  Segments touched: {} / {} ({:.1}%)",
        filtered_keys1.len(),
        reader_stats.total_segments,
        (filtered_keys1.len() as f64 / reader_stats.total_segments as f64) * 100.0
    );
    println!("  Matching rows: {}", results1.len());
    println!();

    // Query 2: Multiple values
    println!("Query 2: status IN ['ERROR','WARN'] AND region='NYC' AND hour IN [9-11]");
    let mut query2 = QueryPredicate::new();
    query2.add_filter(
        "status".to_string(),
        vec!["ERROR".to_string(), "WARN".to_string()],
    );
    query2.add_filter("region".to_string(), vec!["NYC".to_string()]);
    query2.add_filter(
        "hour".to_string(),
        vec!["9".to_string(), "10".to_string(), "11".to_string()],
    );

    let filtered_keys2 = reader.filter_segments(&query2)?;
    let results2 = reader.read_and_filter(&filtered_keys2, &query2)?;

    println!(
        "  Segments touched: {} / {} ({:.1}%)",
        filtered_keys2.len(),
        reader_stats.total_segments,
        (filtered_keys2.len() as f64 / reader_stats.total_segments as f64) * 100.0
    );
    println!("  Matching rows: {}", results2.len());
    println!();

    println!("✓ STRATA demonstration complete!");
    println!("  Output directory: {:?}", output_dir);

    Ok(())
}
