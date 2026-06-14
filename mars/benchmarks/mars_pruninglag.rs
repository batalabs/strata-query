/// Pruning Lag, measured. For a fixed selective tail query on REAL streaming-order data, how much
/// can each scheme prune as rows accumulate?
///   zone-map  Fixed-size segments in ARRIVAL order, each keeping [min,max]. A segment is
///             skippable for `v >= T` iff its max < T. On unsorted streams segments quickly span
///             the domain, so almost nothing is skippable -- this is Pruning Lag.
///   MAR       Segments are magnitude buckets. A bucket is skippable iff its whole range is below
///             T. This depends only on values, not arrival order, so it is CONSTANT from row 1.
///
/// Output: a curve of prunable fraction (of ingested rows) vs rows ingested, for both schemes.
///
/// Usage: cargo run --release --features duckdb-compare --bin mars_pruninglag
use anyhow::Result;
use duckdb::Connection;
use std::path::PathBuf;

const SEG: usize = 100_000; // arrival-order segment size (rows)
const COMPACT_SEGS: usize = 5; // LSM: compact (sort) every COMPACT_SEGS segments

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
            name: "NYC taxi 2024 fare>=100",
            src: format!("read_parquet('{}')", dp("nyc_taxi_2024-01.parquet")),
            numexpr: "fare_amount",
            thresh: 100.0,
        },
        Case {
            name: "Binance BTC qty>=1",
            src: format!(
                "read_csv_auto('{}', header=false)",
                dp("BTCUSDT-trades-2024-01-15.csv")
            ),
            numexpr: "column2",
            thresh: 1.0,
        },
    ];

    println!("============================================================");
    println!("  PRUNING LAG (measured): prunable fraction vs rows ingested");
    println!("  arrival-order zone map  vs  MAR magnitude buckets");
    println!("============================================================");

    for c in &cases {
        let conn = Connection::open_in_memory()?;
        conn.execute(
            &format!(
                "CREATE VIEW d AS SELECT CAST({} AS DOUBLE) AS v FROM {} \
             WHERE {} IS NOT NULL AND CAST({} AS DOUBLE) > 0",
                c.numexpr, c.src, c.numexpr, c.numexpr
            ),
            [],
        )?;
        let mut vals: Vec<f64> = Vec::new();
        {
            let mut stmt = conn.prepare("SELECT v FROM d")?;
            let it = stmt.query_map([], |r| r.get::<_, f64>(0))?;
            for v in it {
                vals.push(v?);
            }
        }
        let t = c.thresh;
        println!("\n  {}  ({} rows, threshold {})", c.name, vals.len(), t);
        println!(
            "  {:>12}{:>16}{:>12}{:>14}",
            "rows", "zone-map prune%", "LSM prune%", "MAR prune%"
        );

        // MAR: a magnitude bucket b is fully below T iff 2^(b+1) <= T.
        let mar_skippable = |v: f64| -> bool {
            let b = v.log2().floor().max(0.0);
            2f64.powf(b + 1.0) <= t
        };
        let mut seg_max = f64::MIN;
        let mut seg_rows = 0usize;
        let mut zm_skippable_rows = 0usize; // rows in closed arrival segments with max < T
        let mut closed_rows = 0usize;
        let mut mar_skip = 0usize;
        // LSM model: rows below T that have been COMPACTED (sorted) are skippable; the uncompacted
        // L0 tail is not. We snapshot the below-T count at each compaction; between compactions new
        // arrivals dilute the prunable fraction, producing a sawtooth.
        let mut cum_below = 0usize; // rows < T seen so far (arrival order)
        let mut compacted_below = 0usize; // below-T rows already compacted into sorted runs
        let report_every = (vals.len() / SEG / 12).max(1); // ~12 checkpoints
        let mut seg_idx = 0usize;
        for (i, &v) in vals.iter().enumerate() {
            if mar_skippable(v) {
                mar_skip += 1;
            }
            if v < t {
                cum_below += 1;
            }
            seg_max = seg_max.max(v);
            seg_rows += 1;
            if seg_rows == SEG {
                if seg_max < t {
                    zm_skippable_rows += seg_rows;
                }
                closed_rows += seg_rows;
                seg_idx += 1;
                seg_max = f64::MIN;
                seg_rows = 0;
                if seg_idx.is_multiple_of(COMPACT_SEGS) {
                    compacted_below = cum_below;
                } // compaction
                if seg_idx.is_multiple_of(report_every) {
                    let ingested = i + 1;
                    let zm = 100.0 * zm_skippable_rows as f64 / closed_rows as f64;
                    let lsm = 100.0 * compacted_below as f64 / ingested as f64;
                    let mar = 100.0 * mar_skip as f64 / ingested as f64;
                    println!(
                        "  {:>12}{:>15.1}%{:>11.1}%{:>13.1}%",
                        ingested, zm, lsm, mar
                    );
                }
            }
        }
        let zm = if closed_rows > 0 {
            100.0 * zm_skippable_rows as f64 / closed_rows as f64
        } else {
            0.0
        };
        let lsm = 100.0 * compacted_below as f64 / vals.len() as f64;
        let mar = 100.0 * mar_skip as f64 / vals.len() as f64;
        println!(
            "  {:>12}{:>15.1}%{:>11.1}%{:>13.1}%  <- final",
            vals.len(),
            zm,
            lsm,
            mar
        );
    }
    Ok(())
}
