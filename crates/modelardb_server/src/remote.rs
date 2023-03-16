/* Copyright 2021 The ModelarDB Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Implementation of a request handler for Apache Arrow Flight in the form of
//! `FlightServiceHandler`. An Apache Arrow Flight server that process requests
//! using `FlightServiceHandler` can be started with
//! `start_arrow_flight_server()`.

use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str;
use std::sync::Arc;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    utils, Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, IpcMessage, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use bytes::Bytes;
use datafusion::arrow::array::{ArrayRef, ListBuilder, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::common::DFSchema;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::ParquetReadOptions;
use futures::{stream, Stream, StreamExt};
use modelardb_common::schemas::METRIC_SCHEMA;
use modelardb_common::types::TimestampBuilder;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{self, Sender};
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info};

use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::metadata::MetadataManager;
use crate::parser::{self, ValidStatement};
use crate::query::ModelTable;
use crate::storage::{StorageEngine, COMPRESSED_DATA_FOLDER};
use crate::{Context, NodeType};

/// Start an Apache Arrow Flight server on 0.0.0.0:`port` that pass `context` to
/// the methods that process the requests through `FlightServiceHandler`.
pub fn start_apache_arrow_flight_server(
    context: Arc<Context>,
    runtime: &Arc<Runtime>,
    port: i16,
) -> Result<(), Box<dyn Error>> {
    let localhost_with_port = "0.0.0.0:".to_owned() + &port.to_string();
    let localhost_with_port: SocketAddr = localhost_with_port.parse()?;
    let handler = FlightServiceHandler {
        context,
        dictionaries_by_id: HashMap::new(),
    };
    let flight_service_server = FlightServiceServer::new(handler);
    info!("Starting Apache Arrow Flight on {}.", localhost_with_port);
    runtime
        .block_on(async {
            Server::builder()
                .add_service(flight_service_server)
                .serve(localhost_with_port)
                .await
        })
        .map_err(|e| e.into())
}

/// Read [`RecordBatches`](RecordBatch) from `query_result_stream` and send them one at a time to
/// [`FlightService`] using `sender`. Returns [`Status`] with the code [`tonic::Code::Internal`] if
/// the result cannot be sent through `sender`.
async fn send_query_result(
    df_schema: DFSchema,
    mut query_result_stream: SendableRecordBatchStream,
    sender: Sender<Result<FlightData, Status>>,
) -> Result<(), Status> {
    // Serialize and send the schema.
    let options = IpcWriteOptions::default();
    let schema_as_flight_data = SchemaAsIpc::new(&df_schema.into(), &options).into();
    send_flight_data(&sender, Ok(schema_as_flight_data)).await?;

    // Serialize and send the query result.
    let data_generator = IpcDataGenerator::default();
    let writer_options = IpcWriteOptions::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);

    while let Some(maybe_record_batch) = query_result_stream.next().await {
        // If a record batch is not returned the client is informed about the error.
        let record_batch = match maybe_record_batch {
            Ok(record_batch) => record_batch,
            Err(error) => {
                let status = Status::invalid_argument(error.to_string());
                return send_flight_data(&sender, Err(status)).await;
            }
        };

        // unwrap() is safe as the result is produced by Apache Arrow DataFusion.
        let (encoded_dictionaries, encoded_batch) = data_generator
            .encoded_batch(&record_batch, &mut dictionary_tracker, &writer_options)
            .unwrap();

        for encoded_dictionary in encoded_dictionaries {
            send_flight_data(&sender, Ok(encoded_dictionary.into())).await?;
        }
        send_flight_data(&sender, Ok(encoded_batch.into())).await?;
    }

    Ok(())
}

/// Send `flight_data_or_error` to [`FlightService`] using `sender`. Returns [`Status`] with the
/// code [`tonic::Code::Internal`] if the result cannot be send through `sender`.
async fn send_flight_data(
    sender: &Sender<Result<FlightData, Status>>,
    flight_data_or_error: Result<FlightData, Status>,
) -> Result<(), Status> {
    sender
        .send(flight_data_or_error)
        .await
        .map_err(|error| Status::internal(error.to_string()))
}

/// Parse the arguments in `data` and return an [`Amazon S3`](object_store::aws::AmazonS3) object
/// store if `data` contains the necessary arguments. If `data` is missing arguments or if the
/// created [`Amazon S3`](AmazonS3) object store connection is invalid, [`Status`] is returned.
async fn parse_s3_arguments(data: &[u8]) -> Result<Arc<dyn ObjectStore>, Status> {
    let (endpoint, offset_data) = extract_argument(data)?;
    let (bucket_name, offset_data) = extract_argument(offset_data)?;
    let (access_key_id, offset_data) = extract_argument(offset_data)?;
    let (secret_access_key, _offset_data) = extract_argument(offset_data)?;

    let s3 = AmazonS3Builder::new()
        .with_region("")
        .with_endpoint(endpoint)
        .with_bucket_name(bucket_name)
        .with_access_key_id(access_key_id)
        .with_secret_access_key(secret_access_key)
        .build()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;

    // Check that the connection is valid with the given arguments.
    match s3.get(&Path::from("")).await {
        Ok(_) => Ok(Arc::new(s3)),
        Err(error) => Err(Status::invalid_argument(error.to_string())),
    }
}

/// Parse the arguments in `data` and return an [`Azure Blob Storage`](object_store::azure::MicrosoftAzure)
/// object store if `data` contains the necessary arguments. If `data` is missing arguments or if the created
/// [`Azure Blob Storage`](MicrosoftAzure) object store connection is invalid, [`Status`] is returned.
async fn parse_azure_blob_storage_arguments(data: &[u8]) -> Result<Arc<dyn ObjectStore>, Status> {
    let (account, offset_data) = extract_argument(data)?;
    let (access_key, offset_data) = extract_argument(offset_data)?;
    let (container_name, _offset_data) = extract_argument(offset_data)?;

    let azure_blob_storage = MicrosoftAzureBuilder::new()
        .with_account(account)
        .with_access_key(access_key)
        .with_container_name(container_name)
        .build()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;

    // Check that the connection is valid with the given arguments.
    match azure_blob_storage.get(&Path::from("")).await {
        Ok(_) => Ok(Arc::new(azure_blob_storage)),
        Err(error) => Err(Status::invalid_argument(error.to_string())),
    }
}

/// Assumes `data` is a slice containing one or more arguments with the following format:
/// size of argument (2 bytes) followed by the argument (size bytes). Returns a tuple containing
/// the first argument and `data` with the extracted argument's bytes removed.
fn extract_argument(data: &[u8]) -> Result<(&str, &[u8]), Status> {
    let size_bytes: [u8; 2] = data[..2]
        .try_into()
        .map_err(|_| Status::internal("Size of argument is not 2 bytes."))?;

    let size = u16::from_be_bytes(size_bytes) as usize;

    let argument = str::from_utf8(&data[2..(size + 2)])
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let remaining_bytes = &data[(size + 2)..];

    Ok((argument, remaining_bytes))
}

/// Handler for processing Apache Arrow Flight requests.
/// [`FlightServiceHandler`] is based on the [Apache Arrow Flight examples]
/// published under Apache2.
///
/// [Apache Arrow Flight examples]: https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples
struct FlightServiceHandler {
    /// Singleton that provides access to the system's components.
    context: Arc<Context>,
    /// Pre-allocated static argument for [`utils::flight_data_to_arrow_batch`].
    /// For more information about the use of dictionaries in Apache Arrow see
    /// the [Arrow Columnar Format].
    ///
    /// [Arrow Columnar Format]: https://arrow.apache.org/docs/format/Columnar.html
    dictionaries_by_id: HashMap<i64, ArrayRef>,
}

impl FlightServiceHandler {
    /// Return the schema of `table_name` if the table exists in the default
    /// database schema, otherwise a [`Status`] indicating at what level the
    /// lookup failed is returned.
    async fn schema_of_table_in_default_database_schema(
        &self,
        table_name: &str,
    ) -> Result<SchemaRef, Status> {
        let database_schema = self.default_database_schema()?;

        let table = database_schema
            .table(table_name)
            .await
            .ok_or_else(|| Status::not_found("Table does not exist."))?;

        Ok(table.schema())
    }

    /// Return the default database schema if it exists, otherwise a [`Status`]
    /// indicating at what level the lookup failed is returned.
    fn default_database_schema(&self) -> Result<Arc<dyn SchemaProvider>, Status> {
        let session = self.context.session.clone();

        let catalog = session
            .catalog("datafusion")
            .ok_or_else(|| Status::internal("Default catalog does not exist."))?;

        let schema = catalog
            .schema("public")
            .ok_or_else(|| Status::internal("Default schema does not exist."))?;

        Ok(schema)
    }

    /// Return the table stored as the first element in
    /// [`FlightDescriptor.path`], otherwise a [`Status`] that specifies that
    /// the table name is missing.
    fn table_name_from_flight_descriptor<'a>(
        &'a self,
        flight_descriptor: &'a FlightDescriptor,
    ) -> Result<&String, Status> {
        flight_descriptor
            .path
            .get(0)
            .ok_or_else(|| Status::invalid_argument("No table name in FlightDescriptor.path."))
    }

    /// Lookup the [`ModelTableMetadata`] of the model table with name
    /// `table_name` if it exists. Specifically, the method returns:
    /// * [`ModelTableMetadata`] if a model table with the name `table_name`
    /// exists.
    /// * [`None`] if a table with the name `table_name` exists.
    /// * [`Status`] if the default catalog, the default schema, a table with
    /// the name `table_name`, or a model table with the name `table_name` does
    /// not exists.
    async fn model_table_metadata_from_default_database_schema(
        &self,
        table_name: &str,
    ) -> Result<Option<Arc<ModelTableMetadata>>, Status> {
        let database_schema = self.default_database_schema()?;

        let table = database_schema
            .table(table_name)
            .await
            .ok_or_else(|| Status::not_found("Table does not exist."))?;

        if let Some(model_table) = table.as_any().downcast_ref::<ModelTable>() {
            Ok(Some(model_table.model_table_metadata()))
        } else {
            Ok(None)
        }
    }

    /// Return [`Status`] if a table named `table_name` exists in the default catalog.
    async fn check_if_table_exists(&self, table_name: &str) -> Result<(), Status> {
        let maybe_schema = self.schema_of_table_in_default_database_schema(table_name);
        if maybe_schema.await.is_ok() {
            let message = format!("Table with name '{table_name}' already exists.");
            return Err(Status::already_exists(message));
        }
        Ok(())
    }

    /// While there is still more data to receive, ingest the data into the
    /// table.
    async fn ingest_into_table(
        &self,
        table_name: &str,
        schema: &SchemaRef,
        flight_data_stream: &mut Streaming<FlightData>,
    ) -> Result<(), Status> {
        // Retrieve the data until the request does not contain any more data.
        while let Some(flight_data) = flight_data_stream.next().await {
            let record_batch = self.flight_data_to_record_batch(&flight_data?, schema)?;
            let storage_engine = self.context.storage_engine.write().await;

            // Write record_batch to the table with table_name as a compressed Apache Parquet file.
            storage_engine
                .insert_record_batch(table_name, record_batch)
                .await
                .map_err(|error| {
                    Status::internal(format!(
                        "Data could not be ingested into {table_name}: {error}"
                    ))
                })?;
        }

        Ok(())
    }

    /// While there is still more data to receive, ingest the data into the
    /// storage engine.
    async fn ingest_into_model_table(
        &self,
        model_table_metadata: &ModelTableMetadata,
        flight_data_stream: &mut Streaming<FlightData>,
    ) -> Result<(), Status> {
        // Retrieve the data until the request does not contain any more data.
        while let Some(flight_data) = flight_data_stream.next().await {
            let data_points =
                self.flight_data_to_record_batch(&flight_data?, &model_table_metadata.schema)?;
            let mut storage_engine = self.context.storage_engine.write().await;

            // Note that the storage engine returns when the data is stored in memory, which means
            // the data could be lost if the system crashes right after ingesting the data.
            storage_engine
                .insert_data_points(model_table_metadata, &data_points)
                .await
                .map_err(|error| {
                    Status::internal(format!("Data could not be ingested: {error}"))
                })?;
        }

        Ok(())
    }

    /// Convert `flight_data` to a [`RecordBatch`].
    fn flight_data_to_record_batch(
        &self,
        flight_data: &FlightData,
        schema: &SchemaRef,
    ) -> Result<RecordBatch, Status> {
        debug_assert_eq!(flight_data.flight_descriptor, None);

        utils::flight_data_to_arrow_batch(flight_data, schema.clone(), &self.dictionaries_by_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))
    }

    /// Create a normal table, register it with Apache Arrow DataFusion's
    /// catalog, and save it to the [`MetadataManager`]. If the table exists,
    /// the Apache Parquet file cannot be created, or if the table cannot be
    /// saved to the [`MetadataManager`], return [`Status`] error.
    async fn register_and_save_table(
        &self,
        table_name: String,
        schema: Schema,
    ) -> Result<(), Status> {
        // Ensure the folder for storing the table data exists.
        let metadata_manager = &self.context.metadata_manager;
        let folder_path = metadata_manager
            .local_data_folder()
            .join(COMPRESSED_DATA_FOLDER)
            .join(&table_name);
        fs::create_dir_all(&folder_path)?;

        // Create an empty Apache Parquet file to save the schema.
        let file_path = folder_path.join("empty_for_schema.parquet");
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));
        StorageEngine::write_batch_to_apache_parquet_file(empty_batch, &file_path, None)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Save the table in the Apache Arrow Datafusion catalog.
        self.context
            .session
            .register_parquet(
                &table_name,
                folder_path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Persist the new table to the metadata database.
        self.context
            .metadata_manager
            .save_table_metadata(&table_name)
            .map_err(|error| Status::internal(error.to_string()))?;

        info!("Created table '{}'.", table_name);

        Ok(())
    }

    /// Create a model table, register it with Apache Arrow DataFusion's
    /// catalog, and save it to the [`MetadataManager`]. If the table exists or
    /// if the table cannot be saved to the [`MetadataManager`], return
    /// [`Status`] error.
    fn register_and_save_model_table(
        &self,
        model_table_metadata: ModelTableMetadata,
    ) -> Result<(), Status> {
        // Save the model table in the Apache Arrow DataFusion catalog.
        let model_table_metadata = Arc::new(model_table_metadata);

        self.context
            .session
            .register_table(
                model_table_metadata.name.as_str(),
                ModelTable::new(self.context.clone(), model_table_metadata.clone()),
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Persist the new model table to the metadata database.
        self.context
            .metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .map_err(|error| Status::internal(error.to_string()))?;

        info!("Created model table '{}'.", model_table_metadata.name);
        Ok(())
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceHandler {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;

    /// Not implemented.
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Provide the name of all tables in the catalog.
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let table_names = self.default_database_schema()?.table_names();
        let flight_descriptor = FlightDescriptor::new_path(table_names);
        let flight_info = FlightInfo::new(
            IpcMessage(Bytes::new()),
            Some(flight_descriptor),
            vec![],
            -1,
            -1,
        );

        let output = stream::once(async { Ok(flight_info) });
        Ok(Response::new(Box::pin(output)))
    }

    /// Not implemented.
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Provide the schema of a table in the catalog. The name of the table must
    /// be provided as the first element in `FlightDescriptor.path`.
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let flight_descriptor = request.into_inner();
        let table_name = self.table_name_from_flight_descriptor(&flight_descriptor)?;
        let schema = self
            .schema_of_table_in_default_database_schema(table_name)
            .await?;

        let options = IpcWriteOptions::default();
        let schema_as_ipc = SchemaAsIpc::new(&schema, &options);
        let schema_result = schema_as_ipc
            .try_into()
            .map_err(|error: ArrowError| Status::internal(error.to_string()))?;
        Ok(Response::new(schema_result))
    }

    /// Execute a SQL query provided in UTF-8 and return the schema of the query
    /// result followed by the query result.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        // Extract the query.
        let query = str::from_utf8(&ticket.ticket)
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            .to_owned();

        // Plan the query.
        info!("Executing the query: {}.", query);
        let session = self.context.session.clone();
        let data_frame = session
            .sql(&query)
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Execute the query.
        let df_schema = data_frame.schema().to_owned();
        let query_result_stream = data_frame
            .execute_stream()
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Send the result, a channel is needed as sync is not implemented for RecordBatchStream.
        // A buffer size of two is used based on Apache Arrow DataFusion and Apache Arrow Ballista.
        let (sender, receiver) = mpsc::channel(2);

        task::spawn(async move {
            // Errors cannot be sent to the client if there is an error with the channel, if such an
            // error occurs it is logged using error!(). Simply calling await! on the JoinHandle
            // returned by task::spawn is also not an option as it waits until send_query_result()
            // returns and thus creates a deadlock since the results are never read from receiver.
            if let Err(error) = send_query_result(df_schema, query_result_stream, sender).await {
                error!(
                    "Failed to send the result for '{}' due to: {}.",
                    query, error
                );
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    /// Insert data points into a table. The name of the table must be provided
    /// as the first element of `FlightDescriptor.path` and the schema of the
    /// data points must match the schema of the table. If the data points are
    /// all inserted an empty stream is returned as confirmation, otherwise, a
    /// `Status` specifying what error occurred is returned.
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut flight_data_stream = request.into_inner();

        // Extract the table name and schema.
        let flight_data = flight_data_stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Missing FlightData."))??;

        let flight_descriptor = flight_data
            .flight_descriptor
            .ok_or_else(|| Status::invalid_argument("Missing FlightDescriptor."))?;
        let table_name = self.table_name_from_flight_descriptor(&flight_descriptor)?;
        let normalized_table_name = MetadataManager::normalize_name(table_name);

        // Handle the data based on whether it is a normal table or a model table.
        if let Some(model_table_metadata) = self
            .model_table_metadata_from_default_database_schema(&normalized_table_name)
            .await?
        {
            debug!("Writing data to model table '{}'.", normalized_table_name);
            self.ingest_into_model_table(&model_table_metadata, &mut flight_data_stream)
                .await?;
        } else {
            debug!("Writing data to table '{}'.", normalized_table_name);
            let schema = self
                .schema_of_table_in_default_database_schema(&normalized_table_name)
                .await?;
            self.ingest_into_table(&normalized_table_name, &schema, &mut flight_data_stream)
                .await?;
        }

        // Confirm the data was received.
        Ok(Response::new(Box::pin(stream::empty())))
    }

    /// Not implemented.
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Perform a specific action based on the type of the action in `request`. Currently the
    /// following actions are supported:
    /// * `CommandStatementUpdate`: Execute a SQL query containing a command that does not
    /// return a result. These commands can be `CREATE TABLE table_name(...` which creates a
    /// normal table, and `CREATE MODEL TABLE table_name(...` which creates a model table.
    /// * `FlushMemory`: Flush all data that is currently in memory to disk. This compresses the
    /// uncompressed data currently in memory and then flushes all compressed data in the storage
    /// engine to disk.
    /// * `FlushEdge`: An extension of the `FlushMemory` action that first flushes all data that is
    /// currently in memory to disk and then flushes all compressed data on disk to the remote
    /// object store. Note that data is only transferred to the remote object store if one was
    /// provided when starting the server.
    /// * `CollectMetrics`: Collect internal metrics describing the amount of memory used for
    /// uncompressed and compressed data, disk space used, and the number of data points ingested
    /// over time. Note that the metrics are cleared when collected, thus only the metrics
    /// recorded since the last call to `CollectMetrics` are returned.
    /// * `UpdateRemoteObjectStore`: Update the remote object store, overriding the current
    /// remote object store, if it exists. Each argument in the body should start with the size
    /// of the argument, immediately followed by the argument value. The first argument should be
    /// the object store type, specifically either 's3' or 'azureblobstorage'. The remaining
    /// arguments should be the arguments required to connect to the object store.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        info!("Received request to perform action '{}'.", action.r#type);

        if action.r#type == "CommandStatementUpdate" {
            // Read the SQL from the action.
            let sql = str::from_utf8(&action.body)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            info!("Received request to execute '{}'.", sql);

            // Parse the SQL.
            let statement = parser::tokenize_and_parse_sql(sql)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            // Perform semantic checks to ensure the parsed SQL is supported.
            let valid_statement = parser::semantic_checks_for_create_table(&statement)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            // Create the table or model table if it does not already exists.
            match valid_statement {
                ValidStatement::CreateTable { name, schema } => {
                    self.check_if_table_exists(&name).await?;
                    self.register_and_save_table(name, schema).await?;
                }
                ValidStatement::CreateModelTable(model_table_metadata) => {
                    self.check_if_table_exists(&model_table_metadata.name)
                        .await?;
                    self.register_and_save_model_table(model_table_metadata)?;
                }
            };

            // Confirm the table was created.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "FlushMemory" {
            self.context
                .storage_engine
                .write()
                .await
                .flush()
                .await
                .map_err(Status::internal)?;

            // Confirm the data was flushed.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "FlushEdge" {
            let mut storage_engine = self.context.storage_engine.write().await;
            storage_engine.flush().await.map_err(Status::internal)?;
            storage_engine.transfer().await?;

            // Confirm the data was flushed.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "CollectMetrics" {
            let mut storage_engine = self.context.storage_engine.write().await;
            let metrics = storage_engine.collect_metrics().await;

            // Extract the data from the metrics and insert it into Apache Arrow array builders.
            let mut metric_builder = StringBuilder::new();
            let mut timestamps_builder = ListBuilder::new(TimestampBuilder::new());
            let mut values_builder = ListBuilder::new(UInt32Builder::new());

            for (metric_name, (timestamps, values)) in metrics.iter() {
                metric_builder.append_value(metric_name.to_string());

                timestamps_builder
                    .values()
                    .append_slice(timestamps.values());
                timestamps_builder.append(true);

                values_builder.values().append_slice(values.values());
                values_builder.append(true);
            }

            let schema = METRIC_SCHEMA.clone();

            // Finish the builders and create the record batch containing the metrics.
            let batch = RecordBatch::try_new(
                schema.0.clone(),
                vec![
                    Arc::new(metric_builder.finish()),
                    Arc::new(timestamps_builder.finish()),
                    Arc::new(values_builder.finish()),
                ],
            )
            .unwrap();

            // Write the schema and corresponding record batch to a stream.
            let options = IpcWriteOptions::default();
            let mut writer = StreamWriter::try_new_with_options(vec![], &schema.0, options)
                .map_err(|error| Status::internal(error.to_string()))?;

            writer
                .write(&batch)
                .map_err(|error| Status::internal(error.to_string()))?;
            let batch_bytes = writer
                .into_inner()
                .map_err(|error| Status::internal(error.to_string()))?;

            Ok(Response::new(Box::pin(stream::once(async {
                Ok(arrow_flight::Result {
                    body: batch_bytes.into(),
                })
            }))))
        } else if action.r#type == "UpdateRemoteObjectStore" {
            // If the type of the new remote object store is not "s3" or "azureblobstorage", return an error.
            let (object_store_type, offset_data) = extract_argument(&action.body)?;

            let object_store = match object_store_type {
                "s3" => parse_s3_arguments(offset_data).await,
                "azureblobstorage" => parse_azure_blob_storage_arguments(offset_data).await,
                _ => Err(Status::unimplemented(format!(
                    "{object_store_type} is currently not supported."
                ))),
            }?;

            // Update the object store used for data transfers.
            let mut storage_engine = self.context.storage_engine.write().await;
            storage_engine
                .update_remote_data_folder(object_store)
                .await
                .map_err(|error| {
                    Status::internal(format!(
                        "Could not update remote data folder: {error}"
                    ))
                })?;

            // If on a cloud node, both the remote data folder and the query data folder should be updated.
            if self.context.metadata_manager.node_type == NodeType::Cloud {
                // TODO: The query data folder should be updated in the session context.
            }

            // Confirm the remote object store was updated.
            Ok(Response::new(Box::pin(stream::empty())))
        } else {
            Err(Status::unimplemented("Action not implemented."))
        }
    }

    /// Return all available actions, including both a name and a description for each action.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let create_command_statement_update_action = ActionType {
            r#type: "CommandStatementUpdate".to_owned(),
            description: "Execute a SQL query containing a single command that produce no results."
                .to_owned(),
        };

        let flush_data_to_disk = ActionType {
            r#type: "FlushMemory".to_owned(),
            description: "Flush the uncompressed data to disk by compressing and saving the data."
                .to_owned(),
        };

        let flush_edge_action = ActionType {
            r#type: "FlushEdge".to_owned(),
            description: "Flush uncompressed data to disk by compressing and saving the data and \
            transfer all compressed data to the remote object store."
                .to_owned(),
        };

        let collect_metrics = ActionType {
            r#type: "CollectMetrics".to_owned(),
            description:
            "Collect internal metrics describing the amount of used memory for uncompressed \
            and compressed data, used disk space, and ingested data points over time. The metrics are \
            cleared when collected."
                .to_owned(),
        };

        let update_remote_object_store = ActionType {
            r#type: "UpdateRemoteObjectStore".to_owned(),
            description: "Update the remote object store, overriding the current remote object \
            store, if it exists."
                .to_owned(),
        };

        let output = stream::iter(vec![
            Ok(create_command_statement_update_action),
            Ok(flush_data_to_disk),
            Ok(flush_edge_action),
            Ok(collect_metrics),
            Ok(update_remote_object_store),
        ]);

        Ok(Response::new(Box::pin(output)))
    }
}
