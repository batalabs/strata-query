# MARS reproducibility artifact

This directory reproduces the experiments in the paper *MARS: Magnitude-Aware Routing
for Zero-Wait Range Pruning* (The VLDB Journal). MARS is the numeric-dimension router of
the STRATA engine in this repository; this folder bundles the benchmark scripts, the
reference harness sources, and the dataset instructions needed to regenerate the reported
numbers.

The cost metric throughout is **bytes scanned** (the quantity cloud OLAP engines bill),
measured from real Parquet row-group statistics rather than modeled.

## Datasets

All three datasets are public; none are redistributed here. Download them into a local
`data/` directory next to this README.

| Dataset | Used for | Source |
| --- | --- | --- |
| NYC TLC yellow taxi (2019-01, 2024-01) | taxi fare queries | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page (mirror: https://registry.opendata.aws/nyc-tlc-trip-records-pds/) |
| Binance BTCUSDT spot trades (2024-01-15) | trade-quantity queries, dense-bucket | https://data.binance.vision/ (file `data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-15.zip`) |
| USGS earthquake catalog (2024) | earthquake-depth queries | https://earthquake.usgs.gov/fdsnws/event/1/ |

For the dense-bucket scripts below, unzip the Binance file so the raw CSV is at
`data/BTCUSDT-trades-2024-01-15.csv` (column 2 is the trade quantity).

## Prerequisites

- [DuckDB CLI](https://duckdb.org/docs/installation/) (tested on v1.5.1) for the
  dense-bucket scripts. No Python required.
- A Rust toolchain only if you want to run the reference harness in `benchmarks/`
  (see note below).

## Reproduce the dense-bucket refinement (Table 7, "Recovering the Dense Bucket")

From this `mars/` directory:

```sh
duckdb < scripts/dense_subroute.sql   # writes seg_base/ and seg_ref/ Parquet layouts
duckdb < scripts/measure.sql          # prints touched bytes + correctness per threshold
```

`measure.sql` prints, for each query `qty >= a`, the baseline and refined bytes scanned,
the refined read as a percentage of the baseline total, and a `PASS/FAIL` correctness
check against a full-scan ground truth. Expected headline (baseline column-v total
3,744,386 bytes):

| `qty >=` | baseline | refined | reduction | correct |
| --- | --- | --- | --- | --- |
| 10  (control, already sparse) | 0.02% | 0.02% | none | PASS |
| 1   | 100% | 0.43% | 234x | PASS |
| 0.5 | 100% | 1.35% | 74x | PASS |
| 0.1 | 100% | 16.0% | 6.3x | PASS |

The full measured transcript, including the USGS upper-bucket variant, is in
[`results/results_dense.txt`](results/results_dense.txt).

## Reference benchmark harness (other tables and figures)

The Rust sources under `benchmarks/` are the harness that produced the remaining results.
They are the same binaries named in the paper:

| Source | Reproduces |
| --- | --- |
| `benchmarks/mars_multi.rs` | bytes scanned on selective queries; the gradient as queries widen (Table 5, Table 6, Figure 3) |
| `benchmarks/mars_pruninglag.rs` | Pruning Lag on a real stream vs zone map and LSM (Figure 2) |
| `benchmarks/mars_baselines.rs` | MARS vs a data-dependent equi-depth index (Table 8) |
| `benchmarks/mars_scale.rs` | scaling stress test (Table 10) |

These link against the `strata-query` library crate and the DuckDB Rust crate and were run
as standalone binaries. They are included as the authoritative reference for how each number
was produced. The dense-bucket result above is the fully self-contained, dependency-light
path and is the recommended starting point for reproduction.
