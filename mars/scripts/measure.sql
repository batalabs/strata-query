-- Measurement for the dense-bucket refinement (paper Table 7). Reads the segment Parquet
-- written by dense_subroute.sql and reports, per query threshold v >= a:
--   * baseline touched bytes and refined touched bytes (real Parquet total_compressed_size
--     of column v in the touched segments; a segment is touched iff its value-max >= a),
--   * the refined read as a percentage of the baseline column-v total,
--   * a correctness check: rows from the refined layout (filtered v >= a) must equal the
--     full-scan ground truth (zero false negatives).
-- Run from the `mars/` directory, after dense_subroute.sql:  duckdb < scripts/measure.sql

.mode box

-- Per-segment column-v metadata (min/max/size) for each layout.
CREATE VIEW base_meta AS
  SELECT file_name, CAST(stats_min_value AS DOUBLE) mn, CAST(stats_max_value AS DOUBLE) mx,
         total_compressed_size sz
  FROM parquet_metadata('seg_base/*/*.parquet')
  WHERE path_in_schema = 'v';
CREATE VIEW ref_meta AS
  SELECT file_name, CAST(stats_min_value AS DOUBLE) mn, CAST(stats_max_value AS DOUBLE) mx,
         total_compressed_size sz
  FROM parquet_metadata('seg_ref/*/*.parquet')
  WHERE path_in_schema = 'v';

-- Column-v total bytes per layout (the denominator for "% of bytes").
CREATE TABLE tot AS
  SELECT (SELECT SUM(sz) FROM base_meta) AS base_total,
         (SELECT SUM(sz) FROM ref_meta)  AS ref_total;
SELECT 'storage totals (bytes)' AS label, base_total, ref_total FROM tot;

-- Full-scan ground truth.
CREATE VIEW d AS
  SELECT CAST(column2 AS DOUBLE) AS v
  FROM read_csv_auto('data/BTCUSDT-trades-2024-01-15.csv', header=false)
  WHERE column2 IS NOT NULL AND CAST(column2 AS DOUBLE) > 0;

-- Touched bytes per threshold. A segment is touched by v >= a iff its value-max >= a,
-- the same min/max overlap test a STRATA-style reader makes from segment statistics.
WITH thresholds(a) AS (VALUES (10.0), (1.0), (0.5), (0.1))
SELECT
  t.a AS "qty>=",
  (SELECT COALESCE(SUM(sz), 0) FROM base_meta WHERE mx >= t.a) AS base_bytes,
  (SELECT COALESCE(SUM(sz), 0) FROM ref_meta  WHERE mx >= t.a) AS ref_bytes,
  round(100.0 * (SELECT COALESCE(SUM(sz), 0) FROM base_meta WHERE mx >= t.a)
        / (SELECT base_total FROM tot), 2) AS base_pct,
  round(100.0 * (SELECT COALESCE(SUM(sz), 0) FROM ref_meta WHERE mx >= t.a)
        / (SELECT base_total FROM tot), 2) AS ref_pct
FROM thresholds t
ORDER BY t.a DESC;

-- Correctness: refined layout filtered by v >= a must return exactly the ground-truth rows.
WITH thresholds(a) AS (VALUES (10.0), (1.0), (0.5), (0.1))
SELECT
  t.a AS "qty>=",
  (SELECT COUNT(*) FROM d WHERE v >= t.a) AS truth_rows,
  (SELECT COUNT(*) FROM read_parquet('seg_ref/*/*.parquet') WHERE v >= t.a) AS refined_rows,
  CASE WHEN (SELECT COUNT(*) FROM d WHERE v >= t.a)
          = (SELECT COUNT(*) FROM read_parquet('seg_ref/*/*.parquet') WHERE v >= t.a)
       THEN 'PASS' ELSE 'FAIL' END AS correct
FROM thresholds t
ORDER BY t.a DESC;
