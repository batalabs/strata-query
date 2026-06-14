/// Scale experiment for the MARS paper: does the MAR-vs-streaming wall-clock advantage track the
/// BYTE advantage as data grows? At small (memory-resident) sizes, query wall-clock is dominated by
/// fixed overhead, so the speedup understates the byte saving. We tile a real column to increasing
/// sizes and show the speedup converge toward the byte ratio as that fixed overhead amortizes.
///
/// This is memory-bandwidth-bound (the working set fits in the machine's 63 GB RAM), so it is a
/// CONSERVATIVE lower bound on the disk/object-store I/O-bound advantage: slower storage only
/// amplifies the benefit of reading fewer bytes.
///
/// Usage: cargo run --release --features duckdb-compare --bin mars_scale
use anyhow::{Context, Result};
use duckdb::Connection;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const RG: usize = 100_000;
const THRESH: f64 = 100.0; // selective: fare >= $100

fn main() -> Result<()> {
    let file = PathBuf::from("../mars/mars/data/nyc_taxi_2024-01.parquet");
    let conn = Connection::open_in_memory()?;
    conn.execute(
        &format!(
            "CREATE TABLE base AS SELECT CAST(fare_amount AS DOUBLE) AS v FROM read_parquet('{}') \
         WHERE fare_amount IS NOT NULL AND CAST(fare_amount AS DOUBLE) > 0",
            file.display()
        ),
        [],
    )?;
    let base_n: i64 = conn.query_row("SELECT COUNT(*) FROM base", [], |r| r.get(0))?;
    println!("============================================================");
    println!(
        "  MARS scale experiment: wall-clock vs bytes, fare >= {}",
        THRESH
    );
    println!("  base rows: {}  (tiled by replication)", base_n);
    println!("============================================================");
    println!(
        "{:>5}{:>14}{:>11}{:>11}{:>10}{:>11}{:>11}{:>10}",
        "x", "rows", "stream_ms", "mar_ms", "speedup", "stream_MB", "mar_MB", "byte_x"
    );

    for k in [1usize, 10, 30, 60] {
        let conn = Connection::open_in_memory()?;
        // Re-create base in this connection (fresh, to drop caches between scales).
        conn.execute(
            &format!(
            "CREATE TABLE base AS SELECT CAST(fare_amount AS DOUBLE) AS v FROM read_parquet('{}') \
             WHERE fare_amount IS NOT NULL AND CAST(fare_amount AS DOUBLE) > 0", file.display()),
            [],
        )?;
        match run_scale(&conn, k) {
            Ok((rows, s_ms, m_ms, s_mb, m_mb)) => {
                println!(
                    "{:>5}{:>14}{:>11.1}{:>11.1}{:>9.1}x{:>11.1}{:>11.1}{:>9.0}x",
                    k,
                    rows,
                    s_ms,
                    m_ms,
                    s_ms / m_ms.max(0.001),
                    s_mb,
                    m_mb,
                    s_mb / m_mb.max(0.001)
                );
            }
            Err(e) => println!("  x{} ERROR: {:#}", k, e),
        }
    }
    println!("\n  Interpretation: as rows grow, fixed query overhead amortizes and the wall-clock");
    println!("  speedup converges toward the byte ratio. Memory-bandwidth-bound (conservative).");
    Ok(())
}

fn run_scale(conn: &Connection, k: usize) -> Result<(i64, f64, f64, f64, f64)> {
    // Tile by cross join with range(k): rows = base * k. Add +/-1% jitter so replicas are DISTINCT
    // values (otherwise exact duplicates compress artificially well and inflate the byte ratio).
    // The jitter preserves the magnitude distribution, so MAR's pruning fraction is unchanged.
    conn.execute(
        &format!(
            "CREATE TABLE big AS SELECT b.v * (1.0 + (random() - 0.5) * 0.02) AS v \
         FROM base b CROSS JOIN range({}) r WHERE b.v * (1.0 + (random() - 0.5) * 0.02) > 0",
            k
        ),
        [],
    )?;
    let rows: i64 = conn.query_row("SELECT COUNT(*) FROM big", [], |r| r.get(0))?;

    let dir = std::env::temp_dir().join(format!("mars_scale_{}", k));
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    let stream = dir.join("stream.parquet");
    let tdir = dir.join("mar");
    conn.execute(
        &format!(
            "COPY (SELECT v FROM big) TO '{}' (FORMAT PARQUET, ROW_GROUP_SIZE {})",
            stream.display(),
            RG
        ),
        [],
    )?;
    conn.execute(
        &format!(
        "COPY (SELECT v, CAST(least(15, greatest(0, floor(log2(v)))) AS INTEGER) AS b FROM big) \
         TO '{}' (FORMAT PARQUET, PARTITION_BY (b), ROW_GROUP_SIZE {})", tdir.display(), RG),
        [],
    )?;
    // Free the in-memory tiled table: the timed queries read only the on-disk Parquet files, and
    // holding ~1.4 GB resident during the scan adds memory pressure that distorts the largest scale.
    conn.execute("DROP TABLE big", [])?;

    // MAR touches buckets b where 2^(b+1) > THRESH, i.e. floor(log2(THRESH)) .. 15.
    let lo = (THRESH.log2().floor() as i64).max(0);
    let mar_files: Vec<String> = (lo..=15)
        .map(|b| format!("'{}/b={}/*.parquet'", tdir.display(), b))
        .filter(|g| {
            // keep only buckets that exist on disk
            let p = g.trim_matches('\'');
            let dirp = std::path::Path::new(p).parent().unwrap();
            dirp.exists()
        })
        .collect();

    let stream_sql = format!(
        "SELECT COUNT(*) FROM read_parquet('{}') WHERE v >= {}",
        stream.display(),
        THRESH
    );
    let mar_sql = format!(
        "SELECT COUNT(*) FROM read_parquet([{}]) WHERE v >= {}",
        mar_files.join(","),
        THRESH
    );

    // correctness
    let s_rows: i64 = conn.query_row(&stream_sql, [], |r| r.get(0))?;
    let m_rows: i64 = conn.query_row(&mar_sql, [], |r| r.get(0))?;
    if s_rows != m_rows {
        anyhow::bail!("mismatch stream {} vs mar {}", s_rows, m_rows);
    }

    let s_ms = bench(conn, &stream_sql)?;
    let m_ms = bench(conn, &mar_sql)?;

    // bytes (column v) of stream (all row groups overlap) vs MAR touched buckets
    let s_mb = col_bytes(conn, &stream.display().to_string())? / 1e6;
    let mut m_b = 0.0;
    for b in lo..=15 {
        let g = format!("{}/b={}/*.parquet", tdir.display(), b);
        if std::path::Path::new(&format!("{}/b={}", tdir.display(), b)).exists() {
            m_b += col_bytes(conn, &g)?;
        }
    }
    let _ = fs::remove_dir_all(&dir);
    Ok((rows, s_ms, m_ms, s_mb, m_b / 1e6))
}

fn bench(conn: &Connection, sql: &str) -> Result<f64> {
    for _ in 0..3 {
        let _: i64 = conn.query_row(sql, [], |r| r.get(0)).context("warmup")?;
    }
    let mut t = Vec::new();
    for _ in 0..9 {
        let s = Instant::now();
        let _: i64 = conn.query_row(sql, [], |r| r.get(0))?;
        t.push(s.elapsed().as_secs_f64() * 1000.0);
    }
    t.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Ok(t[t.len() / 2])
}

fn col_bytes(conn: &Connection, path: &str) -> Result<f64> {
    let sql = format!(
        "SELECT COALESCE(SUM(total_compressed_size),0)::DOUBLE FROM parquet_metadata('{}') WHERE path_in_schema='v'",
        path);
    Ok(conn.query_row(&sql, [], |r| r.get::<_, f64>(0))?)
}
