/// MARS (numeric magnitude routing) vs DuckDB across MULTIPLE real datasets.
///
/// For the MARS paper (VLDB Journal). Isolates the numeric router by routing ONLY on the numeric
/// dimension (categoricals held constant) and compares, per dataset and per query, THREE Parquet
/// layouts executed by DuckDB, with a MATCHED row-group size so the comparison is byte-fair:
///   stream  arrival-order Parquet (row-group min/max spans the domain on unsorted data => no
///           skipping => scans ~the whole file). The STREAMING case.
///   sorted  value-sorted Parquet (row-group min/max skips well). Requires a prior global
///           O(N log N) sort -- the reorganization MARS avoids. The STATIC case.
///   mars    one Parquet segment per magnitude bucket (b = clamp(floor(log2 v),0,15)), value
///           stored native DOUBLE. STRATA's real reader selects which buckets a query touches;
///           MARS scans only those segment files.
///
/// We report, per query: median wall-clock ms; BYTES scanned by each layout (the scale-independent
/// cloud-OLAP cost: each method skips with its own mechanism -- row-group min/max for stream/sorted,
/// magnitude buckets for MARS), computed from real Parquet row-group statistics; modeled $/query at
/// $5/TB; segments/row-groups touched; near-optimality bound vs actual; and a row-count equality
/// check. Queries use OPERATOR-REALISTIC absolute thresholds (not powers of two), so the segment
/// counts are honest (a threshold lands inside a bucket and scans that whole bucket).
///
/// Usage: cargo run --release --features duckdb-compare --bin mars_multi [-- --limit N]
use anyhow::{Context, Result};
use duckdb::Connection;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use strata_mvp::parquet_reader::ParquetStrataReader;
use strata_mvp::parquet_writer::{ParquetStrataWriter, ParquetWriterConfig};
use strata_mvp::reader::QueryPredicate;
use strata_mvp::types::Row;
use strata_mvp::DimensionType;

const RG: usize = 100_000; // matched row-group size (rows) across all layouts
const DOLLARS_PER_TB: f64 = 5.0; // Athena/BigQuery-style on-demand scan price

#[derive(Clone)]
struct Dataset {
    name: &'static str,
    src: String,
    numexpr: &'static str,
    unit: &'static str,
    /// Two operator-realistic tail thresholds (deep, moderate), NOT powers of two.
    abs: (f64, f64),
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut limit: Option<usize> = None;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--limit" {
            i += 1;
            limit = Some(args[i].parse()?);
        }
        i += 1;
    }
    let data = PathBuf::from("../mars/mars/data");
    let dpath = |f: &str| data.join(f).display().to_string();

    let datasets = vec![
        Dataset {
            name: "NYC taxi 2024-01 fare",
            src: format!("read_parquet('{}')", dpath("nyc_taxi_2024-01.parquet")),
            numexpr: "fare_amount",
            unit: "USD",
            abs: (500.0, 100.0),
        },
        Dataset {
            name: "NYC taxi 2019-01 fare",
            src: format!("read_parquet('{}')", dpath("nyc_taxi_2019-01.parquet")),
            numexpr: "fare_amount",
            unit: "USD",
            abs: (500.0, 100.0),
        },
        Dataset {
            name: "Binance BTC trade qty",
            src: format!(
                "read_csv_auto('{}', header=false)",
                dpath("BTCUSDT-trades-2024-01-15.csv")
            ),
            numexpr: "column2",
            unit: "BTC",
            abs: (10.0, 1.0),
        },
        Dataset {
            name: "USGS earthquakes depth",
            src: format!("read_csv_auto('{}')", dpath("usgs_quakes_2024.csv")),
            numexpr: "depth",
            unit: "km",
            abs: (300.0, 100.0),
        },
        // Real larger-scale check: two real taxi months concatenated (genuinely distinct rows,
        // NOT tiled) -> does the real byte ratio hold as the real data grows?
        Dataset {
            name: "NYC taxi 2019+2024 concat",
            src: format!(
                "read_parquet(['{}','{}'], union_by_name=true)",
                dpath("nyc_taxi_2019-01.parquet"),
                dpath("nyc_taxi_2024-01.parquet")
            ),
            numexpr: "fare_amount",
            unit: "USD",
            abs: (500.0, 100.0),
        },
    ];

    println!("============================================================");
    println!(
        "  MARS (magnitude routing) vs DuckDB -- {} real datasets",
        datasets.len()
    );
    println!(
        "  Byte-fair: matched row-group size {} rows; bytes from real Parquet stats.",
        RG
    );
    println!(
        "  Queries use operator-realistic (non-2^k) thresholds. limit: {:?}",
        limit
    );
    println!("============================================================");

    let mut summary: Vec<String> = Vec::new();
    for ds in &datasets {
        match run_dataset(ds, limit) {
            Ok(lines) => summary.extend(lines),
            Err(e) => println!("  [{}] ERROR: {:#}", ds.name, e),
        }
    }

    println!("\n============================================================");
    println!(
        "  COMBINED SUMMARY (bytes scanned & $/query at ${}/TB)",
        DOLLARS_PER_TB
    );
    println!("============================================================");
    println!(
        "{:<24}{:<16}{:>8}{:>8}{:>8}{:>10}{:>10}{:>10}{:>8}{:>6}",
        "dataset",
        "query",
        "str_ms",
        "srt_ms",
        "mar_ms",
        "str_MB",
        "srt_MB",
        "mar_MB",
        "mar/str",
        "ok"
    );
    for l in &summary {
        println!("{}", l);
    }
    Ok(())
}

fn run_dataset(ds: &Dataset, limit: Option<usize>) -> Result<Vec<String>> {
    println!("\n------------------------------------------------------------");
    println!(
        "  Dataset: {}  (numeric: {} [{}])",
        ds.name, ds.numexpr, ds.unit
    );

    let conn = Connection::open_in_memory()?;
    let lim = limit.map(|l| format!(" LIMIT {}", l)).unwrap_or_default();
    conn.execute(
        &format!(
            "CREATE VIEW d AS SELECT CAST({} AS DOUBLE) AS v FROM {} \
         WHERE {} IS NOT NULL AND CAST({} AS DOUBLE) > 0{}",
            ds.numexpr, ds.src, ds.numexpr, ds.numexpr, lim
        ),
        [],
    )?;
    let n: i64 = conn.query_row("SELECT COUNT(*) FROM d", [], |r| r.get(0))?;
    println!("  rows (positive): {}", n);
    if n == 0 {
        anyhow::bail!("no positive rows");
    }

    let (vmin, vmax): (f64, f64) = conn.query_row("SELECT min(v), max(v) FROM d", [], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })?;
    let q = |p: f64| -> Result<f64> {
        Ok(
            conn.query_row(&format!("SELECT quantile_cont(v, {}) FROM d", p), [], |r| {
                r.get(0)
            })?,
        )
    };
    let (p50, p90) = (q(0.50)?, q(0.90)?);
    println!(
        "  min {:.4}  p50 {:.4}  p90 {:.4}  max {:.2}",
        vmin, p50, p90, vmax
    );

    let tag = ds.name.replace(|c: char| !c.is_alphanumeric(), "_");
    let nat = std::env::temp_dir().join(format!("mm_nat_{}.parquet", tag));
    let sorted = std::env::temp_dir().join(format!("mm_sort_{}.parquet", tag));
    let tdir = std::env::temp_dir().join(format!("mm_typed_{}", tag));
    for p in [&nat, &sorted] {
        let _ = fs::remove_file(p);
    }
    if tdir.exists() {
        fs::remove_dir_all(&tdir)?;
    }

    // All three layouts: native DOUBLE column `v`, MATCHED row-group size.
    conn.execute(
        &format!(
            "COPY (SELECT v FROM d) TO '{}' (FORMAT PARQUET, ROW_GROUP_SIZE {})",
            nat.display(),
            RG
        ),
        [],
    )?;
    conn.execute(
        &format!(
            "COPY (SELECT v FROM d ORDER BY v) TO '{}' (FORMAT PARQUET, ROW_GROUP_SIZE {})",
            sorted.display(),
            RG
        ),
        [],
    )?;
    conn.execute(
        &format!(
            "COPY (SELECT v, CAST(least(15, greatest(0, floor(log2(v)))) AS INTEGER) AS b FROM d) \
         TO '{}' (FORMAT PARQUET, PARTITION_BY (b), ROW_GROUP_SIZE {})",
            tdir.display(),
            RG
        ),
        [],
    )?;
    conn.execute(
        &format!(
            "CREATE VIEW d_nat AS SELECT v FROM read_parquet('{}')",
            nat.display()
        ),
        [],
    )?;
    conn.execute(
        &format!(
            "CREATE VIEW d_sorted AS SELECT v FROM read_parquet('{}')",
            sorted.display()
        ),
        [],
    )?;

    // STRATA segments (string MVP) -- authoritative bucket selection + correctness.
    let segdir = std::env::temp_dir().join(format!("mm_segs_{}", tag));
    if segdir.exists() {
        fs::remove_dir_all(&segdir)?;
    }
    fs::create_dir_all(&segdir)?;
    let cfg = ParquetWriterConfig {
        dimensions: vec!["val".into(), "c1".into(), "c2".into()],
        dimension_types: vec![
            DimensionType::Numeric,
            DimensionType::Categorical,
            DimensionType::Categorical,
        ],
        bucket_counts: [16, 1, 1],
        output_dir: segdir.clone(),
        segment_size_threshold: 1_000_000,
        compression: parquet::basic::Compression::SNAPPY,
    };
    let mut w = ParquetStrataWriter::new(cfg)?;
    {
        let mut stmt = conn.prepare("SELECT v FROM d")?;
        let rows = stmt.query_map([], |r| r.get::<_, f64>(0))?;
        for v in rows {
            let v = v?;
            let mut row = Row::new();
            row.insert("val".into(), format!("{}", v));
            row.insert("c1".into(), "0".into());
            row.insert("c2".into(), "0".into());
            w.write_row(row)?;
        }
    }
    w.flush_all()?;
    let reader = ParquetStrataReader::load_segments(&segdir)?;
    let total_segs = reader.segment_count();
    println!(
        "  MARS segments (magnitude buckets present): {}",
        total_segs
    );
    let mars_conn = Connection::open_in_memory()?;

    let big = vmax * 10.0 + 1.0;
    let (deep, modr) = ds.abs;
    let queries: Vec<(String, f64, f64)> = vec![
        (format!("v>={:.0} (deep)", deep), deep, big),
        (format!("v>={:.0}", modr), modr, big),
        ("[p50,p90]".to_string(), p50, p90),
        (format!("v>=p50 ({:.2})", p50), p50, big),
    ];

    // whole-file byte size of the streaming layout (column v), for fractions
    let file_bytes = col_bytes_total_one(&conn, &nat.display().to_string())?;
    println!(
        "  stream file (col v): {:.1} MB over matched row groups",
        file_bytes / 1e6
    );

    println!(
        "\n  {:<18}{:>8}{:>8}{:>8}{:>9}{:>9}{:>9}{:>7}{:>7}{:>6}",
        "query",
        "str_ms",
        "srt_ms",
        "mar_ms",
        "str_MB",
        "srt_MB",
        "mar_MB",
        "segs",
        "bnd/act",
        "ok"
    );
    let (warmup, runs) = (3usize, 9usize);
    let mut lines = Vec::new();
    for (label, a, b) in &queries {
        let tail = *b >= big;
        let pred = if tail {
            format!("v >= {}", a)
        } else {
            format!("v BETWEEN {} AND {}", a, b)
        };
        let str_ms = bench(
            &conn,
            &format!("SELECT COUNT(*) FROM d_nat WHERE {}", pred),
            warmup,
            runs,
        )?;
        let srt_ms = bench(
            &conn,
            &format!("SELECT COUNT(*) FROM d_sorted WHERE {}", pred),
            warmup,
            runs,
        )?;
        let truth: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM d_nat WHERE {}", pred),
            [],
            |r| r.get(0),
        )?;

        // STRATA selects touched magnitude buckets (real routing).
        let mut qp = QueryPredicate::new();
        qp.add_range_filter("val".into(), *a, *b);
        let keys = reader.filter_segments(&qp)?;
        let files: Vec<String> = keys
            .iter()
            .map(|k| format!("'{}/b={}/*.parquet'", tdir.display(), k.0))
            .collect();
        let where_v = if tail {
            format!("v >= {}", a)
        } else {
            format!("v BETWEEN {} AND {}", a, b)
        };
        let (mar_ms, mar_rows) = if files.is_empty() {
            (0.0, 0i64)
        } else {
            let sql = format!(
                "SELECT COUNT(*) FROM read_parquet([{}]) WHERE {}",
                files.join(","),
                where_v
            );
            let ms = bench(&mars_conn, &sql, warmup, runs)?;
            let rows: i64 = mars_conn.query_row(&sql, [], |r| r.get(0))?;
            (ms, rows)
        };

        // BYTES scanned by each layout's native skipping mechanism.
        let (str_b, str_hit, str_tot) =
            bytes_overlap(&conn, &nat.display().to_string(), *a, *b, tail)?;
        let (srt_b, srt_hit, _) =
            bytes_overlap(&conn, &sorted.display().to_string(), *a, *b, tail)?;
        // MARS scans the touched bucket files in full (magnitude pruning only); sum per bucket.
        let mut mar_b = 0.0;
        for k in &keys {
            let glob = format!("{}/b={}/*.parquet", tdir.display(), k.0);
            mar_b += col_bytes_total_one(&conn, &glob)?;
        }

        let ratio = (if tail { vmax } else { *b } / a).max(1.0);
        let bound = (ratio.log2().ceil() as i64) + 1;
        let actual = keys.len() as i64;
        let ok = if truth == mar_rows { "Y" } else { "N" };

        println!(
            "  {:<18}{:>8.1}{:>8.1}{:>8.1}{:>9.1}{:>9.1}{:>9.1}{:>4}/{:<2}{:>4}/{:<2}{:>6}",
            label,
            str_ms,
            srt_ms,
            mar_ms,
            str_b / 1e6,
            srt_b / 1e6,
            mar_b / 1e6,
            keys.len(),
            total_segs,
            bound,
            actual,
            ok
        );

        let dollars = |bytes: f64| bytes / 1e12 * DOLLARS_PER_TB;
        lines.push(format!(
            "{:<24}{:<16}{:>8.1}{:>8.1}{:>8.1}{:>10.1}{:>10.1}{:>10.1}{:>7.1}%{:>6}  (rg {}/{} str, {} srt; ${:.2e} str -> ${:.2e} mar)",
            ds.name, label.chars().take(15).collect::<String>(),
            str_ms, srt_ms, mar_ms, str_b/1e6, srt_b/1e6, mar_b/1e6,
            100.0 * mar_b / str_b.max(1.0), ok,
            str_hit, str_tot, srt_hit, dollars(str_b), dollars(mar_b)));
    }

    let _ = fs::remove_dir_all(&segdir);
    let _ = fs::remove_dir_all(&tdir);
    let _ = fs::remove_file(&sorted);
    let _ = fs::remove_file(&nat);
    Ok(lines)
}

/// Bytes of column `v` in row groups whose [min,max] overlaps [a,b]; also (hit, total) row groups.
/// `path` is a single unquoted file path or glob.
fn bytes_overlap(
    conn: &Connection,
    path: &str,
    a: f64,
    b: f64,
    tail: bool,
) -> Result<(f64, i64, i64)> {
    let hi = if tail {
        "1e308".to_string()
    } else {
        format!("{}", b)
    };
    let sql = format!(
        "SELECT \
           COALESCE(SUM(CASE WHEN CAST(stats_max_value AS DOUBLE) >= {a} AND CAST(stats_min_value AS DOUBLE) <= {hi} THEN total_compressed_size ELSE 0 END),0)::DOUBLE, \
           COALESCE(SUM(CASE WHEN CAST(stats_max_value AS DOUBLE) >= {a} AND CAST(stats_min_value AS DOUBLE) <= {hi} THEN 1 ELSE 0 END),0)::BIGINT, \
           COUNT(*)::BIGINT \
         FROM parquet_metadata('{path}') WHERE path_in_schema = 'v'",
        a = a, hi = hi, path = path);
    Ok(conn.query_row(&sql, [], |r| {
        Ok((
            r.get::<_, f64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })?)
}

/// Total bytes of column `v` across all row groups of one file or glob (single unquoted path).
fn col_bytes_total_one(conn: &Connection, path: &str) -> Result<f64> {
    let sql = format!(
        "SELECT COALESCE(SUM(total_compressed_size),0)::DOUBLE FROM parquet_metadata('{}') WHERE path_in_schema = 'v'",
        path);
    Ok(conn.query_row(&sql, [], |r| r.get::<_, f64>(0))?)
}

fn bench(conn: &Connection, sql: &str, warmup: usize, runs: usize) -> Result<f64> {
    for _ in 0..warmup {
        let _: i64 = conn.query_row(sql, [], |r| r.get(0)).context("warmup")?;
    }
    let mut t = Vec::new();
    for _ in 0..runs {
        let s = Instant::now();
        let _: i64 = conn.query_row(sql, [], |r| r.get(0))?;
        t.push(s.elapsed().as_secs_f64() * 1000.0);
    }
    t.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Ok(t[runs / 2])
}
