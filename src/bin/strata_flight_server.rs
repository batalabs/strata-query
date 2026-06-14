//! Strata Arrow Flight SQL Server
//!
//! BI tools connect via gRPC on port 41415 and send SQL queries.
//!
//! Usage: cargo run --release --bin strata_flight_server

use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Int64Builder, StringBuilder};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, Any,
    CommandGetDbSchemas, CommandGetTableTypes, CommandGetTables, CommandStatementQuery,
    CommandStatementUpdate, ProstMessageExt, SqlInfo,
};
use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket,
};
use futures::{Stream, StreamExt};
use prost::Message;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use strata_query::{parse_query, DimensionType, StrataReader, WriterConfig};

struct TableData {
    data_dir: std::path::PathBuf,
    _config: WriterConfig,
    _row_count: usize,
}

#[derive(Clone)]
struct StrataFlightServiceImpl {
    tables: Arc<RwLock<HashMap<String, TableData>>>,
}

// Demo-only handshake token: this server accepts/echoes a single fixed value.
// This is NOT real authentication. Do not expose the server to untrusted clients.
const FAKE_TOKEN: &str = "strata_token";

#[tonic::async_trait]
impl FlightSqlService for StrataFlightServiceImpl {
    type FlightService = StrataFlightServiceImpl;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let result = Ok(HandshakeResponse {
            protocol_version: 0,
            payload: FAKE_TOKEN.into(),
        });
        let output = futures::stream::iter(vec![result]);
        let mut response: Response<Pin<Box<dyn Stream<Item = _> + Send>>> =
            Response::new(Box::pin(output));
        let token = format!("Bearer {}", FAKE_TOKEN);
        response.metadata_mut().append(
            "authorization",
            tonic::metadata::MetadataValue::from_str(&token).unwrap(),
        );
        Ok(response)
    }

    async fn register_sql_info(&self, _id: i32, _info: &SqlInfo) {}

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query;
        info!("Flight SQL: {}", sql);

        let parsed = parse_query(&sql).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let schema = strata_query::flight::schema_for_table(&parsed.table);

        let ticket_bytes: prost::bytes::Bytes = sql.as_bytes().to_vec().into();
        let info = FlightInfo::new()
            .with_descriptor(FlightDescriptor::new_cmd(
                CommandStatementQuery {
                    query: sql,
                    transaction_id: None,
                }
                .as_any()
                .encode_to_vec(),
            ))
            .with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(Ticket {
                        ticket: ticket_bytes,
                    })
                    .with_location("grpc://0.0.0.0:41415"),
            )
            .try_with_schema(&schema)
            .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

        Ok(Response::new(info))
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = strata_query::flight::tables_schema();
        let ticket = Ticket {
            ticket: CommandGetTables::default().as_any().encode_to_vec().into(),
        };
        let info = FlightInfo::new()
            .with_descriptor(FlightDescriptor::new_cmd(
                CommandGetTables::default().as_any().encode_to_vec(),
            ))
            .with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(ticket)
                    .with_location("grpc://0.0.0.0:41415"),
            )
            .try_with_schema(&schema)
            .map_err(|e: ArrowError| Status::internal(e.to_string()))?;
        Ok(Response::new(info))
    }

    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Use get_flight_info_tables"))
    }

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Use get_flight_info_tables"))
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ticket_data = request.into_inner().ticket;

        // Treat ticket as SQL query string
        let sql = String::from_utf8(ticket_data.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid ticket: {}", e)))?;

        info!("do_get: {}", sql);
        let parsed = parse_query(&sql).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let tables = self.tables.read().await;
        let table = tables
            .get(&parsed.table)
            .ok_or_else(|| Status::not_found(format!("Table '{}' not found", parsed.table)))?;

        let batch = strata_query::flight::execute_query(&table.data_dir, &sql)
            .map_err(|e| Status::internal(e.to_string()))?;

        info!("do_get: {} rows", batch.num_rows());

        self.send_batch(batch).await
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.send_tables_batch().await
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let sql = query.query;
        info!("Creating prepared statement: {}", sql);

        let parsed = parse_query(&sql).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let schema = strata_query::flight::schema_for_table(&parsed.table);

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: sql.into_bytes().into(),
            dataset_schema: IpcMessage::try_from(SchemaAsIpc::new(
                &schema,
                &arrow::ipc::writer::IpcWriteOptions::default(),
            ))
            .map_err(|e: ArrowError| Status::internal(e.to_string()))?
            .0,
            ..Default::default()
        })
    }

    async fn do_put_statement_update(
        &self,
        _query: CommandStatementUpdate,
        _request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Strata is read-only via Flight SQL"))
    }
}

impl StrataFlightServiceImpl {
    fn make_stream(
        batch: RecordBatch,
    ) -> Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> {
        let schema = batch.schema();
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(vec![Ok::<_, FlightError>(batch)]));

        Box::pin(encoder.map(|r: Result<FlightData, FlightError>| {
            r.map_err(|e: FlightError| Status::internal(e.to_string()))
        }))
    }

    async fn send_batch(
        &self,
        batch: RecordBatch,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Ok(Response::new(Self::make_stream(batch)))
    }

    async fn send_tables_batch(
        &self,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let tables = self.tables.read().await;

        let mut name_builder = StringBuilder::new();
        let mut rows_builder = Int64Builder::new();

        for name in tables.keys() {
            name_builder.append_value(name);
            rows_builder.append_value(0i64);
        }

        let batch = RecordBatch::try_new(
            Arc::new(strata_query::flight::tables_schema()),
            vec![
                Arc::new(name_builder.finish()),
                Arc::new(rows_builder.finish()),
            ],
        )
        .map_err(|e| Status::internal(e.to_string()))?;

        self.send_batch(batch).await
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let tables: Arc<RwLock<HashMap<String, TableData>>> = Arc::new(RwLock::new(HashMap::new()));

    // Auto-load tables from ./strata_data/
    {
        let mut t = tables.write().await;
        let data_dir = std::path::Path::new("./strata_data");
        if data_dir.exists() {
            for entry in std::fs::read_dir(data_dir).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    let table_name = entry.file_name().to_string_lossy().to_string();
                    if let Ok(reader) = StrataReader::load_segments(&entry.path()) {
                        let stats = reader.get_stats();
                        t.insert(
                            table_name.clone(),
                            TableData {
                                data_dir: entry.path(),
                                _config: WriterConfig {
                                    dimensions: vec!["fare".into(), "status".into(), "hour".into()],
                                    dimension_types: vec![
                                        DimensionType::Numeric,
                                        DimensionType::Categorical,
                                        DimensionType::Categorical,
                                    ],
                                    bucket_counts: [16, 4, 24],
                                    output_dir: entry.path().to_path_buf(),
                                    segment_size_threshold: 10_000,
                                    schema: None,
                                    storage_format: strata_query::StorageFormat::Csv,
                                },
                                _row_count: stats.total_rows,
                            },
                        );
                        info!("Loaded '{}' ({} rows)", table_name, stats.total_rows);
                    }
                }
            }
        }
    }

    let addr = "0.0.0.0:41415".parse().unwrap();
    let service = StrataFlightServiceImpl { tables };

    info!("🚀 Strata Flight SQL server on grpc://0.0.0.0:41415");
    info!("   Connect with any Arrow Flight SQL client");

    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await
        .unwrap();
}
