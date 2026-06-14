//! Mirror of `src/server.rs` unit tests, plus the folded-in `server_e2e_test.rs`
//! end-to-end SQL coverage, expressed through the public API.
//!
//! Router tests drive the REST layer via `tower::ServiceExt::oneshot` against a
//! `build_router(state)` (both now public for embedding/testing). State is seeded
//! by writing segments with the public `StrataWriter` into a temp dir and loading
//! them via `TableState::new`, the same on-disk path the production server uses.
//!
//! The two original `resolve_dimension_types` unit tests called a private fn.
//! They are rewritten here as `numeric_dimension_enables_range_pruning` /
//! `categorical_dimension_does_not_magnitude_route`, which assert the *behavior*
//! that routing decision controls (MARS range pruning) through the public writer
//! and reader rather than poking the private mapping.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tower::ServiceExt; // for `oneshot`

use strata_query::server::{build_router, AppState, TableState};
use strata_query::{
    parse_query, DimensionType, QueryPredicate, Row, StorageFormat, StrataReader, StrataWriter,
    WriterConfig,
};

// ---------------------------------------------------------------------------
// Router test harness (seeds state via the public writer + TableState::new)
// ---------------------------------------------------------------------------

/// The five-row fixture the original inline server tests used.
fn fixture_rows() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("OK", "NYC", "9"),
        ("ERROR", "NYC", "9"),
        ("OK", "LA", "14"),
        ("WARN", "NYC", "3"),
        ("ERROR", "LA", "9"),
    ]
}

/// Build a `TableState` backed by a temp directory and seed it with the five-row
/// fixture by writing real segments through the public `StrataWriter`, then
/// loading them via `TableState::new`. Returns the state plus the `TempDir`
/// guard (kept alive so the data files survive for the test).
fn seeded_table() -> (Arc<TableState>, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = WriterConfig {
        dimensions: vec!["status".into(), "region".into(), "hour".into()],
        dimension_types: vec![DimensionType::Categorical; 3],
        bucket_counts: [4, 4, 24],
        output_dir: dir.path().to_path_buf(),
        segment_size_threshold: 1000,
        schema: None,
        storage_format: StorageFormat::Csv,
    };

    {
        let mut writer = StrataWriter::new(config.clone()).unwrap();
        for (status, region, hour) in fixture_rows() {
            let mut row = Row::new();
            row.insert("status".into(), status.into());
            row.insert("region".into(), region.into());
            row.insert("hour".into(), hour.into());
            writer.write_row(row).unwrap();
        }
        writer.flush_all().unwrap();
    }

    let table = Arc::new(TableState::new(dir.path().to_path_buf(), config).unwrap());
    (table, dir)
}

/// Construct an `AppState`-backed router with a single pre-seeded table `events`.
fn app_with_events() -> (axum::Router, tempfile::TempDir) {
    let (table, dir) = seeded_table();
    let map: HashMap<String, Arc<TableState>> = [("events".to_string(), table)].into();
    let state: AppState = Arc::new(RwLock::new(map));
    (build_router(state), dir)
}

/// Drive one request through the router and return (status, parsed JSON).
async fn call(router: axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

fn post_json(uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

// ---------------------------------------------------------------------------
// Router tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn health_reports_table_count() {
    let (router, _dir) = app_with_events();
    let (status, body) = call(router, get("/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert_eq!(body["tables"], 1);
}

#[tokio::test]
async fn query_returns_matching_rows() {
    let (router, _dir) = app_with_events();
    let req = post_json(
        "/query",
        json!({ "sql": "SELECT * FROM events WHERE status = 'ERROR'" }),
    );
    let (status, body) = call(router, req).await;
    assert_eq!(status, StatusCode::OK);
    // Two ERROR rows in the fixture.
    assert_eq!(body["row_count"], 2, "body: {body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
    // Pruning telemetry must be present and sane.
    let scanned = body["segments_scanned"].as_u64().unwrap();
    let total = body["segments_total"].as_u64().unwrap();
    assert!(scanned <= total, "scanned {scanned} > total {total}");
    // Every returned row really is an ERROR row.
    for row in body["rows"].as_array().unwrap() {
        let cols = body["columns"].as_array().unwrap();
        let status_idx = cols.iter().position(|c| c == "status").unwrap();
        assert_eq!(row[status_idx], "ERROR");
    }
}

#[tokio::test]
async fn query_empty_result_is_ok_not_error() {
    let (router, _dir) = app_with_events();
    // 'NOPE' exists nowhere; the result must be a clean 200 with zero rows, not a 500.
    let req = post_json(
        "/query",
        json!({ "sql": "SELECT * FROM events WHERE status = 'NOPE'" }),
    );
    let (status, body) = call(router, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["row_count"], 0);
    assert!(body["rows"].as_array().unwrap().is_empty());
    assert!(body["columns"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn query_aggregation_count_uses_metadata_path() {
    let (router, _dir) = app_with_events();
    let req = post_json("/query", json!({ "sql": "SELECT COUNT(*) FROM events" }));
    let (status, body) = call(router, req).await;
    assert_eq!(status, StatusCode::OK);
    // Aggregation path returns no data rows but an `aggregations` array.
    assert_eq!(body["row_count"], 0);
    let aggs = body["aggregations"]
        .as_array()
        .expect("aggregations present");
    assert_eq!(aggs.len(), 1);
    assert_eq!(aggs[0]["function"], "COUNT");
    // COUNT(*) over the 5-row fixture must equal 5.
    assert_eq!(aggs[0]["value"], 5.0, "body: {body}");
}

#[tokio::test]
async fn query_unknown_table_is_404() {
    let (router, _dir) = app_with_events();
    let req = post_json(
        "/query",
        json!({ "sql": "SELECT * FROM ghost WHERE status = 'OK'" }),
    );
    let (status, body) = call(router, req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body["error"].as_str().unwrap().contains("ghost"),
        "error should name the missing table: {body}"
    );
}

#[tokio::test]
async fn query_unparseable_sql_is_400() {
    let (router, _dir) = app_with_events();
    let req = post_json("/query", json!({ "sql": "THIS IS NOT SQL" }));
    let (status, _body) = call(router, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn query_malformed_json_body_is_rejected() {
    let (router, _dir) = app_with_events();
    // Missing the required `sql` field → axum's Json extractor rejects it.
    let req = post_json("/query", json!({ "wrong": "field" }));
    let (status, _body) = call(router, req).await;
    assert!(
        status.is_client_error(),
        "expected 4xx for bad request body, got {status}"
    );
}

#[tokio::test]
async fn get_table_returns_info() {
    let (router, _dir) = app_with_events();
    let (status, body) = call(router, get("/tables/events")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "events");
    assert_eq!(body["rows"], 5);
    assert!(body["segments"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn get_table_unknown_is_404() {
    let (router, _dir) = app_with_events();
    let (status, body) = call(router, get("/tables/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"].as_str().unwrap().contains("nope"));
}

#[tokio::test]
async fn list_tables_lists_seeded_table() {
    let (router, _dir) = app_with_events();
    let (status, body) = call(router, get("/tables")).await;
    assert_eq!(status, StatusCode::OK);
    let tables = body["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0]["name"], "events");
    assert_eq!(tables[0]["rows"], 5);
}

#[tokio::test]
async fn load_appends_to_existing_table_and_refreshes_reader() {
    let (router, _dir) = app_with_events();
    // Append three more rows to the existing `events` table. Because the table
    // already exists in state, `/load` writes to its (temp-dir) data_dir.
    let req = post_json(
        "/load",
        json!({
            "table": "events",
            "data": "status,region,hour\nOK,NYC,1\nOK,NYC,2\nERROR,LA,3\n",
        }),
    );
    let (status, body) = call(router.clone(), req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows_loaded"], 3);
    assert_eq!(body["table"], "events");

    // The cached reader must have been refreshed: a follow-up query sees 8 rows total.
    let req = post_json("/query", json!({ "sql": "SELECT COUNT(*) FROM events" }));
    let (status, body) = call(router, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["aggregations"][0]["value"], 8.0, "body: {body}");
}

#[tokio::test]
async fn load_invalid_csv_is_client_error() {
    let (router, _dir) = app_with_events();
    // CSV with a row whose field count disagrees with the header is a parse
    // error inside `load_csv` → BadRequest (400).
    let req = post_json(
        "/load",
        json!({
            "table": "events",
            "data": "status,region,hour\nOK,NYC\n",
        }),
    );
    let (status, _body) = call(router, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Routing-type behavior (rewritten from the private `resolve_dimension_types`
// unit tests). The decision those tests pinned only matters because it controls
// whether a dimension gets MARS magnitude routing (range pruning). We assert
// that observable behavior through the public writer/reader instead.
// ---------------------------------------------------------------------------

/// Write the same numeric dataset under a given routing type for the `value`
/// dimension and return the loaded reader plus its temp dir.
fn write_value_dataset(numeric: bool) -> (StrataReader, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let value_type = if numeric {
        DimensionType::Numeric
    } else {
        DimensionType::Categorical
    };
    let config = WriterConfig {
        dimensions: vec!["status".into(), "value".into(), "hour".into()],
        dimension_types: vec![
            DimensionType::Categorical,
            value_type,
            DimensionType::Categorical,
        ],
        bucket_counts: [4, 16, 24],
        output_dir: dir.path().to_path_buf(),
        segment_size_threshold: 1000,
        schema: None,
        storage_format: StorageFormat::Csv,
    };
    let mut writer = StrataWriter::new(config).unwrap();
    // 150, 200, 450 fall in [100, 500]; 50, 80, 800, 1200 do not.
    for v in [50.0_f64, 80.0, 150.0, 200.0, 450.0, 800.0, 1200.0] {
        let mut row = Row::new();
        row.insert("status".into(), "OK".into());
        row.insert("value".into(), v.to_string());
        row.insert("hour".into(), "9".into());
        writer.write_row(row).unwrap();
    }
    writer.flush_all().unwrap();
    (StrataReader::load_segments(dir.path()).unwrap(), dir)
}

/// A `Numeric` (MARS) `value` dimension must place rows in `⌊log₂ v⌋` buckets so
/// that a range predicate prunes by magnitude and still returns exactly the
/// in-range rows. This is the guarantee `resolve_dimension_types` -> `Numeric`
/// is responsible for.
#[test]
fn numeric_dimension_enables_range_pruning() -> Result<()> {
    let (reader, _dir) = write_value_dataset(true);

    let mut q = QueryPredicate::new();
    q.add_range_filter("value".into(), 100.0, 500.0);

    let keys = reader.filter_segments(&q)?;
    let rows = reader.read_and_filter(&keys, &q)?;

    // Exactly 3 in-range values: 150, 200, 450.
    assert_eq!(
        rows.len(),
        3,
        "numeric routing must return exact range rows"
    );

    // Effectiveness: the values span magnitudes 5..=10, so several segments
    // exist and the range query must touch strictly fewer than all of them.
    let all = reader.filter_segments(&QueryPredicate::new())?;
    assert!(
        keys.len() < all.len(),
        "magnitude routing must let a range query prune segments \
         ({} of {} touched)",
        keys.len(),
        all.len()
    );
    Ok(())
}

/// With the `value` dimension hash-routed (categorical), a numeric range
/// predicate cannot prune by magnitude: routing a range over a hashed dimension
/// does not enumerate the right buckets, so the bitset mask misses the segments
/// holding in-range rows and the pruned result undercounts the true matches.
/// This is the failure mode that makes the `Numeric` choice load-bearing.
#[test]
fn categorical_dimension_does_not_magnitude_route() -> Result<()> {
    let (reader, _dir) = write_value_dataset(false);

    let mut q = QueryPredicate::new();
    q.add_range_filter("value".into(), 100.0, 500.0);

    let keys = reader.filter_segments(&q)?;
    let pruned = reader.read_and_filter(&keys, &q)?;

    // Ground truth via a full scan + row-level match (always correct).
    let all_keys = reader.filter_segments(&QueryPredicate::new())?;
    let full = reader.read_segments(&all_keys)?;
    let ground_truth = full.iter().filter(|r| q.matches(r)).count();
    assert_eq!(ground_truth, 3, "the data really has 3 in-range rows");

    // Hash routing cannot map a numeric *range* onto its buckets, so segment
    // pruning drops in-range rows: the pruned count is strictly below truth.
    assert!(
        pruned.len() < ground_truth,
        "categorical routing should fail to find all range rows via pruning \
         (got {} of {})",
        pruned.len(),
        ground_truth
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Folded-in end-to-end SQL coverage (was tests/server_e2e_test.rs).
//
// This drives the writer → reader → SQL-bridge path end to end against a fixed
// dataset with MARS numeric routing for `fare`.
// ---------------------------------------------------------------------------

#[test]
fn end_to_end_sql_queries_over_mars_routed_fare() -> Result<()> {
    let temp_dir = tempfile::TempDir::new()?;

    let csv_data = "status,fare,hour
OK,150.50,9
ERROR,25.00,14
OK,500.00,9
WARN,75.00,3
OK,300.00,21
ERROR,45.00,11
OK,1200.00,16
WARN,8.50,22";

    let mut csv_reader = csv::Reader::from_reader(csv_data.as_bytes());
    let headers = csv_reader.headers()?.clone();

    // fare → MARS numeric routing; status/hour → hash routing.
    let config = WriterConfig {
        dimensions: vec!["fare".into(), "status".into(), "hour".into()],
        dimension_types: vec![
            DimensionType::Numeric,
            DimensionType::Categorical,
            DimensionType::Categorical,
        ],
        bucket_counts: [16, 4, 24],
        output_dir: temp_dir.path().to_path_buf(),
        segment_size_threshold: 100,
        schema: None,
        storage_format: StorageFormat::Csv,
    };

    let mut writer = StrataWriter::new(config)?;
    for record in csv_reader.records() {
        let record = record?;
        let mut row = Row::new();
        for (i, header) in headers.iter().enumerate() {
            if let Some(val) = record.get(i) {
                row.insert(header.to_string(), val.to_string());
            }
        }
        writer.write_row(row)?;
    }
    writer.flush_all()?;

    let reader = StrataReader::load_segments(temp_dir.path())?;

    // Helper: run a SQL query and return the matched row count.
    let run = |sql: &str| -> Result<usize> {
        let parsed = parse_query(sql)?;
        let keys = reader.filter_segments(&parsed.predicate)?;
        Ok(reader.read_and_filter(&keys, &parsed.predicate)?.len())
    };

    // Test 1: Simple equality.
    assert_eq!(
        run("SELECT * FROM trips WHERE status = 'ERROR'")?,
        2,
        "expected 2 ERROR rows"
    );

    // Test 2: Numeric range with MARS pruning (150.50, 500.00, 300.00).
    assert_eq!(
        run("SELECT * FROM trips WHERE fare >= 100 AND fare <= 500")?,
        3,
        "expected 3 rows with fare in [100,500]"
    );

    // Test 3: Combined categorical + numeric range (4 OK rows with fare >= 100).
    assert_eq!(
        run("SELECT * FROM trips WHERE status = 'OK' AND fare >= 100")?,
        4,
        "expected 4 OK rows with fare >= 100"
    );

    // Test 4: No matches.
    assert_eq!(
        run("SELECT * FROM trips WHERE fare >= 10000")?,
        0,
        "expected 0 rows"
    );

    // Test 5: BETWEEN (25.00, 75.00, 45.00; 8.50 is below 20).
    assert_eq!(
        run("SELECT * FROM trips WHERE fare BETWEEN 20 AND 80")?,
        3,
        "expected 3 rows with fare in [20,80]"
    );

    // Test 6: IN list (4 ERROR/WARN rows).
    assert_eq!(
        run("SELECT * FROM trips WHERE status IN ('ERROR', 'WARN')")?,
        4,
        "expected 4 ERROR/WARN rows"
    );

    Ok(())
}
