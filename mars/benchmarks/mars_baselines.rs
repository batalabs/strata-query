/// Additional baselines for the MARS paper: data-DEPENDENT boundaries.
///
/// The pruning-relevant form of a learned/histogram index is data-dependent segment boundaries:
/// partition the column into B segments by its own quantiles (equi-depth), so each segment holds an
/// equal share of rows and its [min,max] is tight. Such boundaries prune range queries as well as,
/// or better than, MAR's fixed magnitude boundaries on dense regions -- BUT they must be LEARNED
/// from the data (an O(N) pass = Pruning Lag) and re-learned under drift. MAR's boundaries are free
/// and fixed from t=0.
///
/// We measure, per dataset, on the selective query: bytes scanned by streaming, equi-depth, and MAR;
/// and the equi-depth BUILD cost (the quantile pass) that MAR does not pay.
///
/// Usage: cargo run --release --features duckdb-compare --bin mars_baselines
use anyhow::Result;
use duckdb::Connection;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const RG: usize = 100_000;
const B: usize = 16; // segments for both schemes

struct Case {
    name: &'static str,
    src: String,
    numexpr: &'static str,
    thresh: f64,
}

fn main() -> Result<()> {
    let data = PathBuf::from("../mars/mars/data");
    let dp = |f: &str| data.join(f).display().to_string();
    let cases = vec![
        Case {
            name: "taxi 2024 fare>=100",
            src: format!("read_parquet('{}')", dp("nyc_taxi_2024-01.parquet")),
            numexpr: "fare_amount",
            thresh: 100.0,
        },
        Case {
            name: "taxi 2019 fare>=100",
            src: format!("read_parquet('{}')", dp("nyc_taxi_2019-01.parquet")),
            numexpr: "fare_amount",
            thresh: 100.0,
        },
        Case {
            name: "BTC qty>=10",
            src: format!(
                "read_csv_auto('{}', header=false)",
                dp("BTCUSDT-trades-2024-01-15.csv")
            ),
            numexpr: "column2",
            thresh: 10.0,
        },
        Case {
            name: "USGS depth>=300",
            src: format!("read_csv_auto('{}')", dp("usgs_quakes_2024.csv")),
            numexpr: "depth",
            thresh: 300.0,
        },
    ];

    println!("============================================================");
    println!("  BASELINES: data-dependent (equi-depth) boundaries vs MAR");
    println!(
        "  Bytes scanned for the selective query, and the equi-depth BUILD cost (Pruning Lag)."
    );
    println!("============================================================");
    println!(
        "{:<22}{:>11}{:>10}{:>10}{:>13}{:>12}",
        "case", "stream_MB", "eqd_MB", "mar_MB", "eqd_build_ms", "mar_build"
    );

    for c in &cases {
        match run_case(c) {
            Ok((s, e, m, build)) => println!(
                "{:<22}{:>11.2}{:>10.2}{:>10.2}{:>13.1}{:>12}",
                c.name, s, e, m, build, "0 (none)"
            ),
            Err(err) => println!("  {} ERROR: {:#}", c.name, err),
        }
    }
    println!("\n  Equi-depth prunes comparably to MAR but pays an O(N) build (Pruning Lag) per (re)build;");
    println!("  MAR's boundaries are data-independent: zero build, fixed from the first row.");
    Ok(())
}

fn run_case(c: &Case) -> Result<(f64, f64, f64, f64)> {
    let conn = Connection::open_in_memory()?;
    conn.execute(
        &format!(
            "CREATE TABLE d AS SELECT CAST({} AS DOUBLE) AS v FROM {} \
         WHERE {} IS NOT NULL AND CAST({} AS DOUBLE) > 0",
            c.numexpr, c.src, c.numexpr, c.numexpr
        ),
        [],
    )?;
    let (vmin, vmax): (f64, f64) = conn.query_row("SELECT min(v), max(v) FROM d", [], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })?;

    // Equi-depth BUILD: one pass computing the B-1 internal quantile boundaries. Time it.
    let qsel: Vec<String> = (1..B)
        .map(|i| format!("quantile_cont(v, {})", i as f64 / B as f64))
        .collect();
    let build_sql = format!("SELECT {} FROM d", qsel.join(", "));
    let t = Instant::now();
    let bounds: Vec<f64> = conn.query_row(&build_sql, [], |r| {
        let mut v = Vec::with_capacity(B - 1);
        for i in 0..(B - 1) {
            v.push(r.get::<_, f64>(i)?);
        }
        Ok(v)
    })?;
    let build_ms = t.elapsed().as_secs_f64() * 1000.0;

    let tag = c.name.replace(|ch: char| !ch.is_alphanumeric(), "_");
    let edir = std::env::temp_dir().join(format!("mb_eqd_{}", tag));
    let mdir = std::env::temp_dir().join(format!("mb_mar_{}", tag));
    for d in [&edir, &mdir] {
        if d.exists() {
            fs::remove_dir_all(d)?;
        }
    }

    // Equi-depth partition: bucket = number of boundaries <= v (0..B-1).
    let case = bounds
        .iter()
        .map(|b| format!("(v >= {})::INTEGER", b))
        .collect::<Vec<_>>()
        .join(" + ");
    conn.execute(&format!(
        "COPY (SELECT v, {} AS eb FROM d) TO '{}' (FORMAT PARQUET, PARTITION_BY (eb), ROW_GROUP_SIZE {})",
        case, edir.display(), RG), [])?;
    // MAR partition: magnitude bucket.
    conn.execute(
        &format!(
            "COPY (SELECT v, CAST(least(15, greatest(0, floor(log2(v)))) AS INTEGER) AS b FROM d) \
         TO '{}' (FORMAT PARQUET, PARTITION_BY (b), ROW_GROUP_SIZE {})",
            mdir.display(),
            RG
        ),
        [],
    )?;
    // Streaming: arrival-order single file.
    let sfile = std::env::temp_dir().join(format!("mb_str_{}.parquet", tag));
    conn.execute(
        &format!(
            "COPY (SELECT v FROM d) TO '{}' (FORMAT PARQUET, ROW_GROUP_SIZE {})",
            sfile.display(),
            RG
        ),
        [],
    )?;

    let a = c.thresh;
    // Equi-depth edges: e[0]=min, e[i]=bounds[i-1], e[B]=max. Bucket i covers [e[i],e[i+1]); tail a..inf
    // touches bucket i iff upper edge e[i+1] >= a.
    let mut edges = vec![vmin];
    edges.extend(bounds.iter().cloned());
    edges.push(vmax);
    let mut eqd_b = 0.0;
    for i in 0..B {
        if edges[i + 1] >= a {
            let g = format!("{}/eb={}/*.parquet", edir.display(), i);
            if std::path::Path::new(&format!("{}/eb={}", edir.display(), i)).exists() {
                eqd_b += col_bytes(&conn, &g)?;
            }
        }
    }
    // MAR touched: buckets b with 2^(b+1) > a.
    let lo = (a.log2().floor() as i64).max(0);
    let mut mar_b = 0.0;
    for b in lo..=15 {
        if std::path::Path::new(&format!("{}/b={}", mdir.display(), b)).exists() {
            mar_b += col_bytes(&conn, &format!("{}/b={}/*.parquet", mdir.display(), b))?;
        }
    }
    let str_b = col_bytes(&conn, &sfile.display().to_string())?;

    for d in [&edir, &mdir] {
        let _ = fs::remove_dir_all(d);
    }
    let _ = fs::remove_file(&sfile);
    Ok((str_b / 1e6, eqd_b / 1e6, mar_b / 1e6, build_ms))
}

fn col_bytes(conn: &Connection, path: &str) -> Result<f64> {
    let sql = format!(
        "SELECT COALESCE(SUM(total_compressed_size),0)::DOUBLE FROM parquet_metadata('{}') WHERE path_in_schema='v'",
        path);
    Ok(conn.query_row(&sql, [], |r| r.get::<_, f64>(0))?)
}
