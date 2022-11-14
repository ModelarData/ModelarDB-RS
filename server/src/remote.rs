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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str;
use std::sync::Arc;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::utils;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, IpcMessage, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use datafusion::arrow::{
    array::ArrayRef, datatypes::Schema, datatypes::SchemaRef, error::ArrowError,
    ipc::writer::IpcWriteOptions, record_batch::RecordBatch,
};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::prelude::ParquetReadOptions;
use futures::{stream, Stream, StreamExt};
use tokio::runtime::Runtime;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info};
use tracing::field::debug;

use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::metadata::MetadataManager;
use crate::parser::{self, ValidStatement};
use crate::storage::StorageEngine;
use crate::storage::uncompressed_data_manager;
use crate::tables::ModelTable;
use crate::Context;

/// Start an Apache Arrow Flight server on 0.0.0.0:`port` that pass `context` to
/// the methods that process the requests through `FlightServiceHandler`.
pub fn start_arrow_flight_server(
    context: Arc<Context>,
    runtime: &Arc<Runtime>,
    port: i16,
) -> Result<(), Box<dyn Error>> {
    let localhost_with_port = "0.0.0.0:".to_owned() + &port.to_string();
    let localhost_with_port: SocketAddr = localhost_with_port.parse()?;
    let handler = FlightServiceHandler {
        context: context.clone(),
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
    fn get_schema_of_table_in_default_database_schema(
        &self,
        table_name: &str,
    ) -> Result<SchemaRef, Status> {
        let database_schema = self.get_default_database_schema()?;

        let table = database_schema
            .table(table_name)
            .ok_or_else(|| Status::not_found("Table does not exist."))?;

        Ok(table.schema())
    }

    /// Return the default database schema if it exists, otherwise a [`Status`]
    /// indicating at what level the lookup failed is returned.
    fn get_default_database_schema(&self) -> Result<Arc<dyn SchemaProvider>, Status> {
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
    fn get_table_name_from_flight_descriptor<'a>(
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
    fn get_model_table_metadata_from_default_database_schema(
        &self,
        table_name: &str,
    ) -> Result<Option<Arc<ModelTableMetadata>>, Status> {
        let database_schema = self.get_default_database_schema()?;

        let table = database_schema
            .table(table_name)
            .ok_or_else(|| Status::not_found("Table does not exist."))?;

        if let Some(model_table) = table.as_any().downcast_ref::<ModelTable>() {
            Ok(Some(model_table.get_model_table_metadata()))
        } else {
            Ok(None)
        }
    }

    /// Return [`Status`] if a table named `table_name` exists in the default catalog.
    fn check_if_table_exists(&self, table_name: &str) -> Result<(), Status> {
        let maybe_schema = self.get_schema_of_table_in_default_database_schema(table_name);
        if maybe_schema.is_ok() {
            let message = format!("Table with name '{}' already exists.", table_name);
            return Err(Status::already_exists(message));
        }
        Ok(())
    }

    /// While there is still more data to receive, ingest the data into the
    /// table.
    fn ingest_into_table(
        &self,
        table_name: &str,
        _schema: SchemaRef,
        _flight_data_stream: Streaming<FlightData>,
    ) {
        // TODO: Implement this.
        error!(
            "Received data for table '{}' but ingesting data into tables is not supported yet.",
            table_name
        );
    }

    /// While there is still more data to receive, ingest the data into the
    /// storage engine.
    async fn ingest_into_model_table(
        &self,
        model_table_metadata: &ModelTableMetadata,
        flight_data_stream: &mut Streaming<FlightData>,
    ) -> Result<(), Status> {

        debug!("reached");
        // Retrieve the data until the request does not contain any more data.
        while let Some(flight_data) = flight_data_stream.next().await {
            let flight_data = flight_data?;
            debug_assert_eq!(flight_data.flight_descriptor, None);

            // Convert the flight data to a record batch.
            let data_points = utils::flight_data_to_arrow_batch(
                &flight_data,
                model_table_metadata.schema.clone(),
                &self.dictionaries_by_id,
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

            let mut storage_engine = self.context.storage_engine.write().await;

            // Note that the storage engine returns when the data is stored in memory, which means
            // the data could be lost if the system crashes right after ingesting the data.
            storage_engine
                .insert_data_points(model_table_metadata, &data_points)
                .map_err(|error| {
                    Status::internal(format!("Data could not be ingested: {}", error))
                })?;
        }

        Ok(())
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
        let folder_path = metadata_manager.get_data_folder_path().join(&table_name);
        fs::create_dir(&folder_path)?;

        // Create an empty Apache Parquet file to save the schema.
        let file_path = folder_path.join("empty_for_schema.parquet");
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));
        StorageEngine::write_batch_to_apache_parquet_file(empty_batch, &file_path)
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
    async fn flush_data_to_disk(
        &self
    ) -> Result<(), Status> {
        let context = self.context.clone();

        context.storage_engine.write().await.flush();

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
        let table_names = self.get_default_database_schema()?.table_names();
        let flight_descriptor = FlightDescriptor::new_path(table_names);
        let flight_info =
            FlightInfo::new(IpcMessage(vec![]), Some(flight_descriptor), vec![], -1, -1);

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
        let table_name = self.get_table_name_from_flight_descriptor(&flight_descriptor)?;
        let schema = self.get_schema_of_table_in_default_database_schema(table_name)?;

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
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Execute the query.
        info!("Executing the query: {}.", query);
        let session = self.context.session.clone();
        let data_frame = session
            .sql(query)
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let record_batches = data_frame
            .collect()
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Serialize the schema.
        let options = IpcWriteOptions::default();
        let schema_as_flight_data =
            SchemaAsIpc::new(&data_frame.schema().clone().into(), &options).into();
        let mut result_set: Vec<Result<FlightData, Status>> = vec![Ok(schema_as_flight_data)];

        // Serialize the query result.
        let mut record_batches_as_flight_data: Vec<Result<FlightData, Status>> = record_batches
            .iter()
            .flat_map(|result| {
                let (flight_dictionaries, flight_batch) =
                    utils::flight_data_from_arrow_batch(result, &options);
                flight_dictionaries
                    .into_iter()
                    .chain(std::iter::once(flight_batch))
                    .map(Ok)
            })
            .collect();
        result_set.append(&mut record_batches_as_flight_data);

        // Transmit the schema and the query result.
        let output = stream::iter(result_set);
        Ok(Response::new(Box::pin(output)))
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

       // debug_assert_eq!(flight_data.data_body.len(), 0);

        let flight_descriptor = flight_data
            .flight_descriptor
            .ok_or_else(|| Status::invalid_argument("Missing FlightDescriptor."))?;
        let table_name = self.get_table_name_from_flight_descriptor(&flight_descriptor)?;
        let normalized_table_name = MetadataManager::normalize_name(&table_name);

        // Handle the data based on whether it is a normal table or a model table.
        if let Some(model_table_metadata) =
            self.get_model_table_metadata_from_default_database_schema(&normalized_table_name)?
        {
            debug!("Writing data to model table '{}'.", normalized_table_name);
            self.ingest_into_model_table(&*model_table_metadata, &mut flight_data_stream)
                .await?;
        } else {
            debug!("Writing data to table '{}'.", normalized_table_name);
            let schema =
                self.get_schema_of_table_in_default_database_schema(&normalized_table_name)?;
            self.ingest_into_table(&normalized_table_name, schema, flight_data_stream);
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

    /// Perform a specific action based on the type of the action in `request`.
    /// Currently only the action `CommandStatementUpdate` is supported which
    /// executes a SQL query containing a command that does not return a result.
    /// These commands can be `CREATE TABLE table_name(...` which creates a
    /// normal table, and `CREATE MODEL TABLE table_name(...` which creates a
    /// model table. A model table is a specialized table for efficiently
    /// storing multivariate time series with tags within a per field error
    /// bound. Thus, model tables can only contain a single timestamp column,
    /// field columns, and tag columns. If no error bound is defined for a field
    /// column it defaults to an error bound of 0% (lossless compression).
    ///
    /// Examples of CREATE TABLE and CREATE MODEL TABLE commands:
    /// * CREATE TABLE company(id INTEGER, name TEXT)
    /// * CREATE MODEL TABLE wind_turbines(timestamp TIMESTAMP,
    ///   field_lossless FIELD, field_lossy FIELD(5), tag_one TAG, tag_two TAG)
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
                    self.check_if_table_exists(&name)?;
                    self.register_and_save_table(name, schema).await?;
                }
                ValidStatement::CreateModelTable(model_table_metadata) => {
                    self.check_if_table_exists(&model_table_metadata.name)?;
                    self.register_and_save_model_table(model_table_metadata)?;
                }
            };

            // Confirm the table was created.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "Flush"{
            self.flush_data_to_disk().await.expect("Failed to flush data to disk.");

            // Confirm the data was flushed.
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

        let flush_data_to_disk = ActionType{
            r#type: "Flush".to_owned(),
            description: "Flush the uncompressed data to disk by compressing and saving the data."
                .to_owned()
        };

        let output = stream::iter(vec![Ok(create_command_statement_update_action), Ok(flush_data_to_disk)]);
        Ok(Response::new(Box::pin(output)))
    }
}
