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
//! [`FlightServiceHandler`]. An Apache Arrow Flight server that process requests
//! using [`FlightServiceHandler`] can be started with [`start_apache_arrow_flight_server()`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::result::Result as StdResult;
use std::str;
use std::sync::Arc;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use datafusion::arrow::array::{
    ArrayRef, ListBuilder, StringArray, StringBuilder, UInt32Builder, UInt64Array,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use modelardb_common::{arguments, remote};
use modelardb_storage::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_types::functions;
use modelardb_types::schemas::{CONFIGURATION_SCHEMA, METRIC_SCHEMA};
use modelardb_types::types::TimestampBuilder;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{self, Sender};
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info};

use crate::context::Context;
use crate::error::{ModelarDbServerError, Result};
use crate::ClusterMode;

/// Start an Apache Arrow Flight server on 0.0.0.0:`port` that passes `context` to the methods that
/// process the requests through [`FlightServiceHandler`].
pub fn start_apache_arrow_flight_server(
    context: Arc<Context>,
    runtime: &Arc<Runtime>,
    port: u16,
) -> Result<()> {
    let localhost_with_port = "0.0.0.0:".to_owned() + &port.to_string();
    let localhost_with_port: SocketAddr = localhost_with_port.parse().map_err(|error| {
        ModelarDbServerError::InvalidArgument(format!(
            "Unable to parse {localhost_with_port}: {error}"
        ))
    })?;
    let handler = FlightServiceHandler::new(context);

    // Increase the maximum message size from 4 MiB to 16 MiB to allow bulk-loading larger batches.
    let flight_service_server =
        FlightServiceServer::new(handler).max_decoding_message_size(16777216);

    info!("Starting Apache Arrow Flight on {}.", localhost_with_port);
    runtime
        .block_on(async {
            Server::builder()
                .add_service(flight_service_server)
                .serve(localhost_with_port)
                .await
        })
        .map_err(|error| error.into())
}

/// Read [`RecordBatches`](RecordBatch) from `query_result_stream` and send them one at a time to
/// [`FlightService`] using `sender`. Returns [`Status`] with the code [`tonic::Code::Internal`] if
/// the result cannot be sent through `sender`.
async fn send_query_result(
    mut query_result_stream: SendableRecordBatchStream,
    sender: Sender<StdResult<FlightData, Status>>,
) -> StdResult<(), Status> {
    // Serialize and send the schema.
    let schema = query_result_stream.schema();
    let options = IpcWriteOptions::default();
    let schema_as_flight_data = SchemaAsIpc::new(&schema, &options).into();
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
    sender: &Sender<StdResult<FlightData, Status>>,
    flight_data_or_error: StdResult<FlightData, Status>,
) -> StdResult<(), Status> {
    sender
        .send(flight_data_or_error)
        .await
        .map_err(|error| Status::internal(error.to_string()))
}

/// Write the schema and corresponding record batch to a stream within a gRPC response.
fn send_record_batch(
    schema: SchemaRef,
    batch: RecordBatch,
) -> StdResult<Response<<FlightServiceHandler as FlightService>::DoActionStream>, Status> {
    let options = IpcWriteOptions::default();
    let mut writer = StreamWriter::try_new_with_options(vec![], &schema, options)
        .map_err(|error| Status::internal(error.to_string()))?;

    writer
        .write(&batch)
        .map_err(|error| Status::internal(error.to_string()))?;
    let batch_bytes = writer
        .into_inner()
        .map_err(|error| Status::internal(error.to_string()))?;

    Ok(Response::new(Box::pin(stream::once(async {
        Ok(FlightResult {
            body: batch_bytes.into(),
        })
    }))))
}

/// Handler for processing Apache Arrow Flight requests.
/// [`FlightServiceHandler`] is based on the [Apache Arrow Flight examples]
/// published under Apache2.
///
/// [Apache Arrow Flight examples]: https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples
struct FlightServiceHandler {
    /// Singleton that provides access to the system's components.
    context: Arc<Context>,
    /// Pre-allocated static argument for
    /// [`flight_data_to_arrow_batch`](arrow_flight::utils::flight_data_to_arrow_batch).
    /// For more information about the use of dictionaries in Apache Arrow see
    /// the [Arrow Columnar Format].
    ///
    /// [Arrow Columnar Format]: https://arrow.apache.org/docs/format/Columnar.html
    dictionaries_by_id: HashMap<i64, ArrayRef>,
}

impl FlightServiceHandler {
    pub fn new(context: Arc<Context>) -> FlightServiceHandler {
        Self {
            context,
            dictionaries_by_id: HashMap::new(),
        }
    }

    /// While there is still more data to receive, ingest the data into the normal table.
    async fn ingest_into_normal_table(
        &self,
        table_name: &str,
        schema: &SchemaRef,
        flight_data_stream: &mut Streaming<FlightData>,
    ) -> StdResult<(), Status> {
        // Retrieve the data until the request does not contain any more data.
        while let Some(flight_data) = flight_data_stream.next().await {
            let record_batch = remote::flight_data_to_record_batch(
                &flight_data?,
                schema,
                &self.dictionaries_by_id,
            )?;
            let storage_engine = self.context.storage_engine.write().await;

            // Write record_batch to the normal table with table_name as a compressed Apache Parquet file.
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

    /// While there is still more data to receive, ingest the data into the storage engine.
    async fn ingest_into_model_table(
        &self,
        model_table_metadata: Arc<ModelTableMetadata>,
        flight_data_stream: &mut Streaming<FlightData>,
    ) -> StdResult<(), Status> {
        // Retrieve the data until the request does not contain any more data.
        while let Some(flight_data) = flight_data_stream.next().await {
            let data_points = remote::flight_data_to_record_batch(
                &flight_data?,
                &model_table_metadata.schema,
                &self.dictionaries_by_id,
            )?;
            let mut storage_engine = self.context.storage_engine.write().await;

            // Note that the storage engine returns when the data is stored in memory, which means
            // the data could be lost if the system crashes right after ingesting the data.
            storage_engine
                .insert_data_points(model_table_metadata.clone(), data_points)
                .await
                .map_err(|error| {
                    Status::internal(format!("Data could not be ingested: {error}"))
                })?;
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceHandler {
    type HandshakeStream = BoxStream<'static, StdResult<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, StdResult<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, StdResult<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, StdResult<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, StdResult<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, StdResult<FlightResult, Status>>;
    type ListActionsStream = BoxStream<'static, StdResult<ActionType, Status>>;

    /// Not implemented.
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> StdResult<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Provide the name of all tables in the catalog.
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> StdResult<Response<Self::ListFlightsStream>, Status> {
        let table_names = self
            .context
            .default_database_schema()
            .map_err(|error| Status::internal(error.to_string()))?
            .table_names();
        let flight_descriptor = FlightDescriptor::new_path(table_names);
        let flight_info = FlightInfo::new().with_descriptor(flight_descriptor);

        let output = stream::once(async { Ok(flight_info) });
        Ok(Response::new(Box::pin(output)))
    }

    /// Not implemented.
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> StdResult<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Not implemented.
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> StdResult<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Provide the schema of a table in the catalog. The name of the table must be provided as the
    /// first element in `FlightDescriptor.path`.
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> StdResult<Response<SchemaResult>, Status> {
        let flight_descriptor = request.into_inner();
        let table_name = remote::table_name_from_flight_descriptor(&flight_descriptor)?;
        let schema = self
            .context
            .schema_of_table_in_default_database_schema(table_name)
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        let options = IpcWriteOptions::default();
        let schema_as_ipc = SchemaAsIpc::new(&schema, &options);
        let schema_result = schema_as_ipc
            .try_into()
            .map_err(|error: ArrowError| Status::internal(error.to_string()))?;
        Ok(Response::new(schema_result))
    }

    /// Execute a SQL query provided in UTF-8 and return the schema of the query result followed by
    /// the query result.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> StdResult<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        // Extract the query.
        let query = str::from_utf8(&ticket.ticket)
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            .to_owned();

        // Plan the query.
        info!("Executing the statement: {}.", query);
        let session_context = self.context.session_context.clone();
        let data_frame = session_context
            .sql(&query)
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Execute the query.
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
            if let Err(error) = send_query_result(query_result_stream, sender).await {
                error!(
                    "Failed to send the result for '{}' due to: {}.",
                    query, error
                );
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    /// Insert data points into a table. The name of the table must be provided as the first element
    /// of `FlightDescriptor.path` and the schema of the data points must match the schema of the
    /// table. If the data points are all inserted an empty stream is returned as confirmation,
    /// otherwise, a `Status` specifying what error occurred is returned.
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> StdResult<Response<Self::DoPutStream>, Status> {
        let mut flight_data_stream = request.into_inner();

        // Extract the table name and schema.
        let flight_data = flight_data_stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Missing FlightData."))??;

        let flight_descriptor = flight_data
            .flight_descriptor
            .ok_or_else(|| Status::invalid_argument("Missing FlightDescriptor."))?;
        let table_name = remote::table_name_from_flight_descriptor(&flight_descriptor)?;
        let normalized_table_name = functions::normalize_name(table_name);

        // Handle the data based on whether it is a normal table or a model table.
        if let Some(model_table_metadata) = self
            .context
            .model_table_metadata_from_default_database_schema(&normalized_table_name)
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?
        {
            debug!("Writing data to model table '{}'.", normalized_table_name);
            self.ingest_into_model_table(model_table_metadata, &mut flight_data_stream)
                .await?;
        } else {
            debug!("Writing data to normal table '{}'.", normalized_table_name);
            let schema = self
                .context
                .schema_of_table_in_default_database_schema(&normalized_table_name)
                .await
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            self.ingest_into_normal_table(&normalized_table_name, &schema, &mut flight_data_stream)
                .await?;
        }

        // Confirm the data was received.
        Ok(Response::new(Box::pin(stream::empty())))
    }

    /// Not implemented.
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> StdResult<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Perform a specific action based on the type of the action in `request`. Currently, the
    /// following actions are supported:
    /// * `CreateTable`: Execute a SQL query containing a command that creates a table.
    /// These commands can be `CREATE TABLE table_name(...` which creates a normal table, and
    /// `CREATE MODEL TABLE table_name(...` which creates a model table.
    /// * `DropTable`: Drop a table previously created with `CreateTable`. The name of
    /// table that should be dropped must be provided in the body of the action. All data in the
    /// table, both in memory and on disk, is deleted.
    /// * `TruncateTable`: Truncate a table previously created with `CreateTable`. The name of
    /// the table that should be truncated must be provided in the body of the action. All data in
    /// the table, both in memory and on disk, is deleted.
    /// * `FlushMemory`: Flush all data that is currently in memory to disk. This compresses the
    /// uncompressed data currently in memory and then flushes all compressed data in the storage
    /// engine to disk.
    /// * `FlushNode`: An extension of the `FlushMemory` action that first flushes all data that is
    /// currently in memory to disk and then flushes all compressed data on disk to the remote
    /// object store. Note that data is only transferred to the remote object store if one was
    /// provided when starting the server.
    /// * `KillNode`: An extension of the `FlushNode` action that first flushes all data to disk,
    /// then flushes all compressed data to the remote object store, and finally kills the process
    /// that is running the server. Note that since the process is killed, a conventional response
    /// cannot be returned.
    /// * `CollectMetrics`: Collect internal metrics describing the amount of memory used for
    /// uncompressed and compressed data, disk space used, and the number of data points ingested
    /// over time. Note that the metrics are cleared when collected, thus only the metrics
    /// recorded since the last call to `CollectMetrics` are returned.
    /// * `GetConfiguration`: Get the current server configuration. The value of each setting in the
    /// configuration is returned in a single [`RecordBatch`].
    /// * `UpdateConfiguration`: Update a single setting in the configuration. Each argument in the
    /// body should start with the size of the argument, immediately followed by the argument value.
    /// The first argument should be the setting to update. The second argument should be the new
    /// value of the setting as an unsigned integer.
    /// * `NodeType`: Get the type of the node. The type is always `server`. The type of the node
    /// is returned as a string.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> StdResult<Response<Self::DoActionStream>, Status> {
        let metadata = request.metadata().clone();
        let action = request.into_inner();
        info!("Received request to perform action '{}'.", action.r#type);

        // If the server was started with a manager, validate the request.
        let configuration_manager = self.context.configuration_manager.read().await;
        if let ClusterMode::MultiNode(manager) = &configuration_manager.cluster_mode {
            manager
                .validate_action_request(&action.r#type, &metadata)
                .map_err(|error| Status::unauthenticated(error.to_string()))?
        };

        // Manually drop the read lock on the configuration manager to avoid deadlock issues.
        std::mem::drop(configuration_manager);

        if action.r#type == "CreateTable" {
            // Read the SQL from the action.
            let sql = str::from_utf8(&action.body)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            info!("Received request to execute '{}'.", sql);

            self.context
                .parse_and_create_table(sql)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Confirm the table was created.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "DropTable" {
            // Read the table name from the action body.
            let table_name = str::from_utf8(&action.body)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            info!("Received request to drop table '{}'.", table_name);

            self.context
                .drop_table(table_name)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Confirm the table was dropped.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "TruncateTable" {
            // Read the table name from the action body.
            let table_name = str::from_utf8(&action.body)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            info!("Received request to truncate table '{}'.", table_name);

            self.context
                .truncate_table(table_name)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Confirm the table was truncated.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "FlushMemory" {
            self.context
                .storage_engine
                .write()
                .await
                .flush()
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Confirm the data was flushed.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "FlushNode" {
            let mut storage_engine = self.context.storage_engine.write().await;
            storage_engine
                .flush()
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
            storage_engine
                .transfer()
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Confirm the data was flushed.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "KillNode" {
            let mut storage_engine = self.context.storage_engine.write().await;
            storage_engine
                .flush()
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
            storage_engine
                .transfer()
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Since the process is killed, a conventional response cannot be given. If the action
            // returns a "Stream removed" message, the edge was successfully flushed and killed.
            std::process::exit(0);
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

            send_record_batch(schema.0, batch)
        } else if action.r#type == "GetConfiguration" {
            // Extract the configuration data from the configuration manager.
            let configuration_manager = self.context.configuration_manager.read().await;
            let settings = [
                "uncompressed_reserved_memory_in_bytes",
                "compressed_reserved_memory_in_bytes",
                "transfer_batch_size_in_bytes",
                "transfer_time_in_seconds",
                "ingestion_threads",
                "compression_threads",
                "writer_threads",
            ];
            let values = vec![
                Some(configuration_manager.uncompressed_reserved_memory_in_bytes() as u64),
                Some(configuration_manager.compressed_reserved_memory_in_bytes() as u64),
                configuration_manager
                    .transfer_batch_size_in_bytes()
                    .map(|n| n as u64),
                configuration_manager
                    .transfer_time_in_seconds()
                    .map(|n| n as u64),
                Some(configuration_manager.ingestion_threads as u64),
                Some(configuration_manager.compression_threads as u64),
                Some(configuration_manager.writer_threads as u64),
            ];

            let schema = CONFIGURATION_SCHEMA.clone();

            // Create the record batch with the current configuration.
            let batch = RecordBatch::try_new(
                schema.0.clone(),
                vec![
                    Arc::new(StringArray::from_iter_values(settings)),
                    Arc::new(UInt64Array::from(values)),
                ],
            )
            .unwrap();

            send_record_batch(schema.0, batch)
        } else if action.r#type == "UpdateConfiguration" {
            let (setting, offset_data) = arguments::decode_argument(&action.body)
                .map_err(|error| Status::internal(error.to_string()))?;
            let (new_value, _offset_data) = arguments::decode_argument(offset_data)
                .map_err(|error| Status::internal(error.to_string()))?;

            // Parse the new value into None if it is empty and a usize integer if it is not empty.
            let new_value: Option<usize> = (!new_value.is_empty())
                .then(|| {
                    new_value.parse().map_err(|error| {
                        Status::invalid_argument(format!(
                            "New value for {setting} is not valid: {error}"
                        ))
                    })
                })
                .transpose()?;

            let mut configuration_manager = self.context.configuration_manager.write().await;
            let storage_engine = self.context.storage_engine.clone();

            let invalid_empty_error =
                Status::invalid_argument(format!("New value for {setting} cannot be empty."));

            match setting {
                "multivariate_reserved_memory_in_bytes" => {
                    let new_value = new_value.ok_or(invalid_empty_error)?;

                    configuration_manager
                        .set_multivariate_reserved_memory_in_bytes(new_value, storage_engine)
                        .await;

                    Ok(())
                }
                "uncompressed_reserved_memory_in_bytes" => {
                    let new_value = new_value.ok_or(invalid_empty_error)?;

                    configuration_manager
                        .set_uncompressed_reserved_memory_in_bytes(new_value, storage_engine)
                        .await
                        .map_err(|error| Status::internal(error.to_string()))
                }
                "compressed_reserved_memory_in_bytes" => {
                    let new_value = new_value.ok_or(invalid_empty_error)?;

                    configuration_manager
                        .set_compressed_reserved_memory_in_bytes(new_value, storage_engine)
                        .await
                        .map_err(|error| Status::internal(error.to_string()))
                }
                "transfer_batch_size_in_bytes" => configuration_manager
                    .set_transfer_batch_size_in_bytes(new_value, storage_engine)
                    .await
                    .map_err(|error| Status::internal(error.to_string())),
                "transfer_time_in_seconds" => configuration_manager
                    .set_transfer_time_in_seconds(new_value, storage_engine)
                    .await
                    .map_err(|error| Status::internal(error.to_string())),
                "ingestion_threads" | "compression_threads" | "writer_threads" => {
                    Err(Status::unimplemented(format!(
                        "{setting} is not an updatable setting in the server configuration."
                    )))
                }
                _ => Err(Status::unimplemented(format!(
                    "{setting} is not a setting in the server configuration."
                ))),
            }?;

            // Confirm the configuration was updated.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "NodeType" {
            let flight_result = FlightResult {
                body: "server".bytes().collect(),
            };

            Ok(Response::new(Box::pin(stream::once(async {
                Ok(flight_result)
            }))))
        } else {
            Err(Status::unimplemented("Action not implemented."))
        }
    }

    /// Return all available actions, including both a name and a description for each action.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> StdResult<Response<Self::ListActionsStream>, Status> {
        let create_table_action = ActionType {
            r#type: "CreateTable".to_owned(),
            description: "Execute a SQL query containing a command that creates a table."
                .to_owned(),
        };

        let drop_table_action = ActionType {
            r#type: "DropTable".to_owned(),
            description: "Drop a table and all its data.".to_owned(),
        };

        let truncate_table_action = ActionType {
            r#type: "TruncateTable".to_owned(),
            description: "Delete all data from a table.".to_owned(),
        };

        let flush_memory_action = ActionType {
            r#type: "FlushMemory".to_owned(),
            description: "Flush the uncompressed data to disk by compressing and saving the data."
                .to_owned(),
        };

        let flush_node_action = ActionType {
            r#type: "FlushNode".to_owned(),
            description: "Flush uncompressed data to disk by compressing and saving the data and \
                          transfer all compressed data to the remote object store."
                .to_owned(),
        };

        let kill_node_action = ActionType {
            r#type: "KillNode".to_owned(),
            description: "Flush uncompressed data to disk by compressing and saving the data, \
                          transfer all compressed data to the remote object store, and kill the \
                          process running the server."
                .to_owned(),
        };

        let collect_metrics_action = ActionType {
            r#type: "CollectMetrics".to_owned(),
            description: "Collect internal metrics describing the amount of memory used for \
                          multivariate data, uncompressed data, compressed data, used disk space, \
                          and ingested data points over time. The metrics are cleared when \
                          collected."
                .to_owned(),
        };

        let get_configuration_action = ActionType {
            r#type: "GetConfiguration".to_owned(),
            description: "Get the current server configuration.".to_owned(),
        };

        let update_configuration_action = ActionType {
            r#type: "UpdateConfiguration".to_owned(),
            description: "Update a specific setting in the server configuration.".to_owned(),
        };

        let node_type_action = ActionType {
            r#type: "NodeType".to_owned(),
            description: "Get the type of the node.".to_owned(),
        };

        let output = stream::iter(vec![
            Ok(create_table_action),
            Ok(drop_table_action),
            Ok(truncate_table_action),
            Ok(flush_memory_action),
            Ok(flush_node_action),
            Ok(kill_node_action),
            Ok(collect_metrics_action),
            Ok(get_configuration_action),
            Ok(update_configuration_action),
            Ok(node_type_action),
        ]);

        Ok(Response::new(Box::pin(output)))
    }
}
