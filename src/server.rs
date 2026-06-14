//! Production-grade Strata server with:
//! - Cached readers (metadata loaded once, refreshed on writes)
//! - RwLock per table (concurrent reads, exclusive writes)
//! - Atomic segment writes (write to temp, rename)
//! - Streaming writes (bounded memory)

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};

use crate::{
    parse_query, AggFunc, DimensionType, Row, StorageFormat, StrataReader, StrataWriter,
    WriterConfig,
};

// ---------------------------------------------------------------------------
// Cached Table State
// ---------------------------------------------------------------------------

/// A loaded table with a cached reader for fast queries.
///
/// The reader is created once and cached. After a write, the reader
/// is refreshed to pick up new segments.
///
/// Public for embedding and integration testing: [`build_router`] takes an
/// [`AppState`] built from these, so the type is part of the embedding surface.
pub struct TableState {
    data_dir: std::path::PathBuf,
    config: WriterConfig,
    /// Cached reader: loaded once, reused across queries.
    reader: RwLock<StrataReader>,
    /// Total rows written since startup.
    row_count: std::sync::atomic::AtomicUsize,
}

impl TableState {
    /// Create a new table state, loading existing segments from disk.
    ///
    /// Public for embedding and integration testing: pre-write segments to
    /// `data_dir` with [`StrataWriter`] and this picks them up on load.
    pub fn new(data_dir: std::path::PathBuf, config: WriterConfig) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&data_dir)?;
        let reader = StrataReader::load_segments(&data_dir)?;
        let row_count = reader.get_stats().total_rows;
        Ok(TableState {
            data_dir,
            config,
            reader: RwLock::new(reader),
            row_count: std::sync::atomic::AtomicUsize::new(row_count),
        })
    }

    /// Execute a query using the cached reader (concurrent reads allowed).
    async fn query(&self, sql: &str) -> Result<QueryResponse, ServerError> {
        let start = std::time::Instant::now();
        let parsed = parse_query(sql).map_err(|e| ServerError::BadRequest(e.to_string()))?;

        // Acquire read lock so multiple queries can run concurrently
        let reader = self.reader.read().await;
        let segments_total = reader.segment_count();

        // Prune segments
        let matching_keys = reader
            .filter_segments(&parsed.predicate)
            .map_err(|e| ServerError::Internal(e.to_string()))?;
        let segments_scanned = matching_keys.len();

        // Aggregation path (metadata-only)
        if !parsed.aggregations.is_empty() {
            let agg_args: Vec<(AggFunc, &str)> = parsed
                .aggregations
                .iter()
                .map(|a| (a.func.clone(), a.column.as_str()))
                .collect();

            let results = reader
                .aggregate_multi(&parsed.predicate, &agg_args)
                .map_err(|e| ServerError::Internal(e.to_string()))?;

            let agg_results: Vec<AggregationResult> = parsed
                .aggregations
                .iter()
                .zip(results.iter())
                .map(|(req, res)| {
                    let value = res.as_ref().map(|r| r.value()).unwrap_or(0.0);
                    let func_name = match req.func {
                        AggFunc::Count => "COUNT",
                        AggFunc::Sum => "SUM",
                        AggFunc::Avg => "AVG",
                        AggFunc::Min => "MIN",
                        AggFunc::Max => "MAX",
                    };
                    AggregationResult {
                        function: func_name.to_string(),
                        column: req.column.clone(),
                        value,
                    }
                })
                .collect();

            let elapsed_ms = start.elapsed().as_millis() as u64;
            info!(
                "Aggregation: {} aggs, {}/{} segments (metadata-only), {}ms",
                agg_results.len(),
                segments_scanned,
                segments_total,
                elapsed_ms
            );

            return Ok(QueryResponse {
                columns: parsed
                    .aggregations
                    .iter()
                    .map(|a| format!("{:?}({})", a.func, a.column))
                    .collect(),
                rows: vec![],
                row_count: 0,
                segments_scanned,
                segments_total,
                elapsed_ms,
                aggregations: Some(agg_results),
            });
        }

        // Row scan path
        let rows = reader
            .read_and_filter(&matching_keys, &parsed.predicate)
            .map_err(|e| ServerError::Internal(e.to_string()))?;

        let limited_rows: Vec<_> = if let Some(limit) = parsed.limit {
            rows.into_iter().take(limit as usize).collect()
        } else {
            rows
        };

        let columns = if let Some(first) = limited_rows.first() {
            let mut cols: Vec<String> = first.fields.keys().cloned().collect();
            cols.sort();
            cols
        } else {
            vec![]
        };

        let row_data: Vec<Vec<String>> = limited_rows
            .iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|col| row.get(col).cloned().unwrap_or_default())
                    .collect()
            })
            .collect();

        let row_count = row_data.len();
        let elapsed_ms = start.elapsed().as_millis() as u64;
        info!(
            "Query: {} rows, {}/{} segments, {}ms",
            row_count, segments_scanned, segments_total, elapsed_ms
        );

        Ok(QueryResponse {
            columns,
            rows: row_data,
            row_count,
            segments_scanned,
            segments_total,
            elapsed_ms,
            aggregations: None,
        })
    }

    /// Load CSV data: write in streaming fashion, then refresh the cached reader.
    async fn load_csv(
        &self,
        table_name: &str,
        csv_data: &str,
        config: &WriterConfig,
    ) -> Result<LoadResponse, ServerError> {
        let start = std::time::Instant::now();

        // Parse CSV
        let mut csv_reader = csv::Reader::from_reader(csv_data.as_bytes());
        let headers = csv_reader
            .headers()
            .map_err(|e| ServerError::BadRequest(format!("Invalid CSV: {}", e)))?
            .clone();

        // Write in streaming fashion (flush segments as they fill)
        let mut writer = StrataWriter::new(config.clone())
            .map_err(|e| ServerError::Internal(format!("Failed to create writer: {}", e)))?;

        let mut row_count: usize = 0;
        for result in csv_reader.records() {
            let record =
                result.map_err(|e| ServerError::BadRequest(format!("CSV parse error: {}", e)))?;
            let mut row = Row::new();
            for (i, value) in record.iter().enumerate() {
                if let Some(header) = headers.get(i) {
                    row.insert(header.to_string(), value.to_string());
                }
            }
            writer
                .write_row(row)
                .map_err(|e| ServerError::Internal(format!("Write error: {}", e)))?;
            row_count += 1;
        }
        writer
            .flush_all()
            .map_err(|e| ServerError::Internal(format!("Flush error: {}", e)))?;

        // Refresh the cached reader (write lock, exclusive during refresh)
        let new_reader = StrataReader::load_segments(&self.data_dir)
            .map_err(|e| ServerError::Internal(format!("Failed to refresh reader: {}", e)))?;

        {
            let mut cached_reader = self.reader.write().await;
            *cached_reader = new_reader;
        }

        self.row_count
            .fetch_add(row_count, std::sync::atomic::Ordering::Relaxed);
        let elapsed_ms = start.elapsed().as_millis() as u64;

        info!(
            "Loaded {} rows into '{}' in {}ms",
            row_count, table_name, elapsed_ms
        );

        let stats = self.reader.read().await.get_stats();
        Ok(LoadResponse {
            table: table_name.to_string(),
            rows_loaded: row_count,
            segments: stats.total_segments,
            elapsed_ms,
        })
    }

    /// Get table info.
    async fn info(&self, name: &str) -> TableInfo {
        let reader = self.reader.read().await;
        let stats = reader.get_stats();
        TableInfo {
            name: name.to_string(),
            segments: stats.total_segments,
            rows: stats.total_rows,
            avg_rows_per_segment: stats.avg_rows_per_segment,
        }
    }
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[allow(dead_code)]
enum ServerError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            ServerError::NotFound(msg) => write!(f, "Not found: {}", msg),
            ServerError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct QueryRequest {
    sql: String,
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    row_count: usize,
    segments_scanned: usize,
    segments_total: usize,
    elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    aggregations: Option<Vec<AggregationResult>>,
}

#[derive(Debug, Serialize)]
struct AggregationResult {
    function: String,
    column: String,
    value: f64,
}

#[derive(Debug, Deserialize)]
struct LoadRequest {
    /// Table name to create or append to.
    table: String,
    /// CSV data as a string.
    data: String,
    /// Column names to use for routing (max 3).
    #[serde(default)]
    dimensions: Vec<String>,
    /// Subset of `dimensions` to route numerically (MARS, `⌊log₂ v⌋`), enabling
    /// range pruning. Any dimension not listed here is hash-routed (categorical).
    #[serde(default)]
    numeric_dimensions: Vec<String>,
    /// Bucket counts for each dimension [R, S, T].
    #[serde(default = "default_bucket_counts")]
    bucket_counts: [u8; 3],
    /// Segment size threshold (rows per segment before flush).
    #[serde(default = "default_segment_threshold")]
    segment_size_threshold: usize,
}

fn default_bucket_counts() -> [u8; 3] {
    [16, 16, 24]
}

fn default_segment_threshold() -> usize {
    10_000
}

#[derive(Debug, Serialize)]
struct LoadResponse {
    table: String,
    rows_loaded: usize,
    segments: usize,
    elapsed_ms: u64,
}

#[derive(Debug, Serialize)]
struct TableInfo {
    name: String,
    segments: usize,
    rows: usize,
    avg_rows_per_segment: usize,
}

#[derive(Debug, Serialize)]
struct TablesResponse {
    tables: Vec<TableInfo>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    tables: usize,
}

// ---------------------------------------------------------------------------
// Axum handlers
// ---------------------------------------------------------------------------

/// Shared application state: a map of table name to cached [`TableState`].
///
/// Public for embedding and integration testing so callers can construct a
/// seeded state and hand it to [`build_router`].
pub type AppState = Arc<RwLock<HashMap<String, Arc<TableState>>>>;

async fn query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    info!("Query: {}", req.sql);

    let parsed = parse_query(&req.sql).map_err(|e| bad_request(&e.to_string()))?;
    let tables = state.read().await;
    let table = tables
        .get(&parsed.table)
        .ok_or_else(|| not_found(&format!("Table '{}' not found", parsed.table)))?;

    let result = table.query(&req.sql).await.map_err(|e| match e {
        ServerError::BadRequest(msg) => bad_request(&msg),
        ServerError::NotFound(msg) => not_found(&msg),
        ServerError::Internal(msg) => internal_error(&msg),
    })?;

    Ok(Json(result))
}

/// Resolve per-dimension routing types: dimensions named in `numeric` use MARS
/// (`⌊log₂ v⌋`) routing, which enables range pruning; all others are hash-routed.
fn resolve_dimension_types(dimensions: &[String], numeric: &[String]) -> Vec<DimensionType> {
    dimensions
        .iter()
        .map(|d| {
            if numeric.contains(d) {
                DimensionType::Numeric
            } else {
                DimensionType::Categorical
            }
        })
        .collect()
}

async fn load_csv(
    State(state): State<AppState>,
    Json(req): Json<LoadRequest>,
) -> Result<Json<LoadResponse>, (StatusCode, Json<ErrorResponse>)> {
    info!("Loading CSV into table '{}'", req.table);

    let tables = state.read().await;

    // Check if table already exists
    if let Some(table) = tables.get(&req.table) {
        // Append to existing table
        let result = table
            .load_csv(&req.table, &req.data, &table.config)
            .await
            .map_err(|e| match e {
                ServerError::BadRequest(msg) => bad_request(&msg),
                _ => internal_error(&e.to_string()),
            })?;
        return Ok(Json(result));
    }

    // New table: drop read lock, acquire write lock
    drop(tables);
    let mut tables = state.write().await;

    // Double-check after acquiring write lock
    if tables.contains_key(&req.table) {
        // Another request created it while we were waiting
        let table = tables.get(&req.table).unwrap();
        let result = table
            .load_csv(&req.table, &req.data, &table.config)
            .await
            .map_err(|e| match e {
                ServerError::BadRequest(msg) => bad_request(&msg),
                _ => internal_error(&e.to_string()),
            })?;
        return Ok(Json(result));
    }

    // Create new table
    let data_dir = std::env::current_dir()
        .unwrap_or_default()
        .join("strata_data")
        .join(&req.table);

    let dimensions = if req.dimensions.is_empty() {
        vec!["col1".into(), "col2".into(), "col3".into()]
    } else {
        req.dimensions.clone()
    };
    // Dimensions named in `numeric_dimensions` use MARS (⌊log₂ v⌋) routing so that
    // range queries prune by magnitude; everything else is hash-routed.
    let dimension_types = resolve_dimension_types(&dimensions, &req.numeric_dimensions);
    let bucket_counts = req.bucket_counts;

    let config = WriterConfig {
        dimensions,
        dimension_types,
        bucket_counts,
        output_dir: data_dir.clone(),
        segment_size_threshold: req.segment_size_threshold,
        schema: None,
        storage_format: StorageFormat::Csv,
    };

    let table_state = Arc::new(
        TableState::new(data_dir, config.clone())
            .map_err(|e| internal_error(&format!("Failed to create table: {}", e)))?,
    );

    let result = table_state
        .load_csv(&req.table, &req.data, &config)
        .await
        .map_err(|e| match e {
            ServerError::BadRequest(msg) => bad_request(&msg),
            _ => internal_error(&e.to_string()),
        })?;

    tables.insert(req.table.clone(), table_state);
    Ok(Json(result))
}

async fn list_tables(
    State(state): State<AppState>,
) -> Result<Json<TablesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let tables = state.read().await;
    let mut table_infos = Vec::new();
    for (name, table) in tables.iter() {
        table_infos.push(table.info(name).await);
    }
    Ok(Json(TablesResponse {
        tables: table_infos,
    }))
}

async fn get_table(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<TableInfo>, (StatusCode, Json<ErrorResponse>)> {
    let tables = state.read().await;
    let table = tables
        .get(&name)
        .ok_or_else(|| not_found(&format!("Table '{}' not found", name)))?;
    Ok(Json(table.info(&name).await))
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let tables = state.read().await;
    Json(HealthResponse {
        status: "ok".to_string(),
        tables: tables.len(),
    })
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn bad_request(msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

fn not_found(msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

fn internal_error(msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    error!("Internal error: {}", msg);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

// ---------------------------------------------------------------------------
// Server entry point
// ---------------------------------------------------------------------------

/// Build the application router with all routes wired to the given state.
///
/// Public for embedding and integration testing: the REST layer can be
/// exercised via `tower::ServiceExt::oneshot` without binding a TCP socket. The
/// route table and middleware here are the single source of truth for both the
/// production server and the tests.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/query", post(query))
        .route("/load", post(load_csv))
        .route("/tables", get(list_tables))
        .route("/tables/{name}", get(get_table))
        .route("/health", get(health))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

pub async fn run_server(port: u16) -> anyhow::Result<()> {
    let state: AppState = Arc::new(RwLock::new(HashMap::new()));

    // Load any existing tables from strata_data/
    {
        let data_root = std::env::current_dir()
            .unwrap_or_default()
            .join("strata_data");
        if data_root.exists() {
            let mut tables = state.write().await;
            for entry in std::fs::read_dir(&data_root)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let table_name = entry.file_name().to_string_lossy().to_string();
                    info!("Loading existing table '{}' from disk", table_name);

                    let config = WriterConfig {
                        dimensions: vec!["col1".into(), "col2".into(), "col3".into()],
                        dimension_types: vec![DimensionType::Categorical; 3],
                        bucket_counts: [16, 16, 24],
                        output_dir: entry.path(),
                        segment_size_threshold: 10_000,
                        schema: None,
                        storage_format: StorageFormat::Csv,
                    };

                    match TableState::new(entry.path(), config.clone()) {
                        Ok(ts) => {
                            tables.insert(table_name.clone(), Arc::new(ts));
                            info!("Table '{}' loaded", table_name);
                        }
                        Err(e) => {
                            warn!("Failed to load table '{}': {}", table_name, e);
                        }
                    }
                }
            }
        }
    }

    let app = build_router(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("Strata server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
