-- Dense-bucket sub-routing refinement for MARS magnitude routing (paper Table 7, "Recovering
-- the Dense Bucket"). Writes two Parquet layouts of the Binance BTC trade-quantity column:
--   1. baseline MARS  : clamped 16-bucket magnitude routing, b = clamp(floor(log2 v), 0, 15).
--                       The sub-unit mass collapses into bucket 0.
--   2. refined MARS    : bucket 0 replaced by sub-buckets routed by TRUE magnitude
--                       sub = clamp(floor(log2 v), -17, 0). Boundaries stay powers of two,
--                       so the refinement is data-independent (zero-wait preserved).
-- Method mirrors benchmarks/mars_multi.rs: per-segment Parquet via COPY ... PARTITION_BY,
-- matched ROW_GROUP_SIZE = 100000, "bytes scanned" = real total_compressed_size of the
-- touched segments (measured in measure.sql). Pure DuckDB CLI (tested on v1.5.1).
--
-- Run from the `mars/` directory after placing the dataset at data/ (see README):
--   duckdb < scripts/dense_subroute.sql
--   duckdb < scripts/measure.sql

.mode box

-- BTC positive-quantity column (column2 of the raw Binance daily trades CSV).
CREATE VIEW d AS
  SELECT CAST(column2 AS DOUBLE) AS v
  FROM read_csv_auto('data/BTCUSDT-trades-2024-01-15.csv', header=false)
  WHERE column2 IS NOT NULL AND CAST(column2 AS DOUBLE) > 0;

-- ============ LAYOUT 1: baseline MARS (clamped 16-bucket magnitude routing) ============
COPY (
  SELECT v, CAST(least(15, greatest(0, floor(log2(v)))) AS INTEGER) AS b FROM d
) TO 'seg_base'
  (FORMAT PARQUET, PARTITION_BY (b), ROW_GROUP_SIZE 100000, OVERWRITE_OR_IGNORE);

-- ============ LAYOUT 2: refined MARS (bucket 0 replaced by magnitude sub-buckets) ============
-- Buckets >= 1 keep the clamped magnitude b ("bK"); bucket 0 is split by true magnitude
-- sub = clamp(floor(log2 v), -17, 0) ("sK"). Floor -17 is chosen because the smallest value
-- (1e-5) has floor(log2) = -17, so nothing is clamped away.
COPY (
  SELECT v,
    CASE
      WHEN floor(log2(v)) >= 1
        THEN 'b' || CAST(least(15, floor(log2(v))) AS INTEGER)
      ELSE 's' || CAST(greatest(-17, floor(log2(v))) AS INTEGER)
    END AS seg
  FROM d
) TO 'seg_ref'
  (FORMAT PARQUET, PARTITION_BY (seg), ROW_GROUP_SIZE 100000, OVERWRITE_OR_IGNORE);
