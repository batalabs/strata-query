//! Strata Production Server
//!
//! REST API for loading data and querying with SQL.
//!
//! # Endpoints
//!
//! - `POST /query`: Execute a SQL query
//! - `POST /load`: Load CSV data into Strata segments
//! - `GET /tables`: List loaded tables
//! - `GET /tables/{name}`: Get table info
//! - `GET /health`: Health check
//!
//! # Production features
//!
//! - Cached readers (metadata loaded once, refreshed on writes)
//! - RwLock per table (concurrent reads, exclusive writes)
//! - Atomic segment writes (write to temp, rename)
//! - Auto-discovers existing tables on startup

use tracing_subscriber::filter::LevelFilter;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    let port: u16 = std::env::var("STRATA_PORT")
        .unwrap_or_else(|_| "3131".to_string())
        .parse()
        .unwrap_or(3131);

    // Create a minimal tokio runtime and run the async server
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(strata_query::server::run_server(port))?;

    Ok(())
}
