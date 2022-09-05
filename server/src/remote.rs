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
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::{mem, str};
use std::sync::{Arc, RwLockWriteGuard};

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::utils;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, IpcMessage, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{
    array::ArrayRef, datatypes::SchemaRef, error::ArrowError, ipc::writer::IpcWriteOptions,
};
use futures::{stream, Stream, StreamExt};
use object_store;
use rusqlite::{Connection, params};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::catalog::{NewModelTableMetadata, TableMetadata};
use crate::storage::StorageEngine;
use crate::{Catalog, Context};

/// Start an Apache Arrow Flight server on 0.0.0.0:`port` that pass `context` to
/// the methods that process the requests through `FlightServiceHandler`.
pub fn start_arrow_flight_server(context: Arc<Context>, port: i16) -> Result<(), Box<dyn Error>> {
    let localhost_with_port = "0.0.0.0:".to_owned() + &port.to_string();
    let localhost_with_port: SocketAddr = localhost_with_port.parse()?;
    let handler = FlightServiceHandler {
        context: context.clone(),
        dictionaries_by_id: HashMap::new(),
    };
    let flight_service_server = FlightServiceServer::new(handler);
    info!("Starting Apache Arrow Flight on {}.", localhost_with_port);
    context
        .runtime
        .block_on(async {
            Server::builder()
                .add_service(flight_service_server)
                .serve(localhost_with_port)
                .await
        })
        .map_err(|e| e.into())
}

/// Handler for processing Apache Arrow Flight requests. `FlightServiceHandler`
/// is based on the [Apache Arrow Flight examples] published under Apache2.
///
/// [Apache Arrow Flight examples]: https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples
struct FlightServiceHandler {
    /// Singleton that provides access to the catalog, asynchronous runtime, and
    /// query engine.
    context: Arc<Context>,
    /// Pre-allocated static argument for `utils::flight_data_to_arrow_batch`.
    /// For more information about the use of dictionaries in Apache Arrow see
    /// the [Arrow Columnar Format].
    ///
    /// [Arrow Columnar Format]: https://arrow.apache.org/docs/format/Columnar.html
    dictionaries_by_id: HashMap<i64, ArrayRef>,
}

impl FlightServiceHandler {
    /// Return the schema of `table_name` if the table exists in the default
    /// catalog, otherwise a `Status` indicating at what level the lookup failed
    /// is returned.
    fn get_table_schema_from_default_catalog(&self, table_name: &str) -> Result<SchemaRef, Status> {
        let session = self.context.session.clone();

        let catalog = session
            .catalog("datafusion")
            .ok_or_else(|| Status::internal("Default catalog does not exist."))?;

        let schema = catalog
            .schema("public")
            .ok_or_else(|| Status::internal("Default schema does not exist."))?;

        let table = schema
            .table(table_name)
            .ok_or_else(|| Status::not_found("Table does not exist."))?;

        Ok(table.schema())
    }

    /// Return the table stored as the first element in `FlightDescriptor.path`,
    /// otherwise a `Status` that specifies that the table name is missing.
    fn get_table_name_from_flight_descriptor<'a>(
        &'a self,
        flight_descriptor: &'a FlightDescriptor,
    ) -> Result<&String, Status> {
        flight_descriptor
            .path
            .get(0)
            .ok_or_else(|| Status::invalid_argument("No table name in FlightDescriptor.path."))
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
        let table_names = {
            // unwrap() is safe to use since read() only fails if the RwLock is poisoned.
            let catalog = self.context.catalog.read().unwrap();
            catalog.table_and_model_table_names()
        };

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
        let schema = self.get_table_schema_from_default_catalog(table_name)?;

        // IpcMessages are transferred as SchemaResults for compatibility with
        // the return type of get_schema() and to ensure the SchemaResult match
        // what is expected by the other Arrow Flight implementations until
        // https://github.com/apache/arrow-rs/issues/2445 is fixed.
        let options = IpcWriteOptions::default();
        let schema_as_ipc = SchemaAsIpc::new(&schema, &options);
        let ipc_message: IpcMessage = schema_as_ipc
            .try_into()
            .map_err(|error: ArrowError| Status::internal(error.to_string()))?;

        let schema_result = SchemaResult {
            schema: ipc_message.0,
        };
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
        let mut request = request.into_inner();

        // Extract the table name.
        let flight_data = request
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Missing FlightData."))??;
        debug_assert_eq!(flight_data.data_body.len(), 0);
        let flight_descriptor = flight_data
            .flight_descriptor
            .ok_or_else(|| Status::invalid_argument("Missing FlightDescriptor."))?;
        let table_name = self.get_table_name_from_flight_descriptor(&flight_descriptor)?;

        // Extract the schema.
        let schema = self
            .get_table_schema_from_default_catalog(&table_name)
            .map_err(|error| {
                error!("Received RecordBatch for the missing table {}.", table_name);
                error
            })?;

        // Log how many data points were received to make it possible to check
        // that the expected number was received without the StorageEngine.
        while let Some(flight_data) = request.next().await {
            let flight_data = flight_data?;
            debug_assert_eq!(flight_data.flight_descriptor, None);
            let record_batch = utils::flight_data_to_arrow_batch(
                &flight_data,
                schema.clone(),
                &self.dictionaries_by_id,
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
            info!(
                "Received RecordBatch with {} data points for the table {}.",
                record_batch.num_rows(),
                table_name
            );
            // TODO: forward the data points to the StorageEngine.
        }

        // Confirm the data points were received.
        Ok(Response::new(Box::pin(stream::empty())))
    }

    /// Not implemented.
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Perform a specific action based on the type of the action in `request`. Currently supports
    /// two actions: `CreateTable` and `CreateModelTable`. `CreateTable` creates a normal table
    /// in the catalog when given a table name and schema. `CreateModelTable` creates a table
    /// that is specialized for efficiently storing multivariate time series with tags within
    /// a user-defined error bound. This action takes a table name, a schema, a list of indices
    /// specifying which columns are metadata tag columns, and an index specifying which column
    /// is the timestamp column.
    ///
    /// The data is given in the action body and must have the following format:
    /// The first two bytes are the length x of the first argument. The next x bytes are the first
    /// argument. This pattern repeats until all arguments are consumed.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        info!("Received request to perform action '{}'.", action.r#type);

        if action.r#type == "CreateTable" || action.r#type == "CreateModelTable" {
            // Extract the table name from the action body.
            let (table_name_bytes, offset_data) = extract_argument_bytes(&action.body);
            let table_name = str::from_utf8(table_name_bytes)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            // Check if the table name is a valid object_store path and database table name.
            object_store::path::Path::parse(table_name)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            if table_name.contains(char::is_whitespace) {
                return Err(Status::invalid_argument("Table name cannot contain whitespace.".to_owned()));
            }

            // Extract the schema from the action body.
            let (schema_bytes, offset_data) = extract_argument_bytes(offset_data);
            let ipc_message = IpcMessage(Vec::from(schema_bytes));
            let schema = Schema::try_from(ipc_message)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            // Get the write lock for the catalog to ensure that multiple writes cannot happen at
            // the same time. unwrap() is safe to use since write() only fails if the RwLock is poisoned.
            let catalog = self.context.catalog.write().unwrap();

            if action.r#type == "CreateTable" {
                create_table(catalog, table_name.to_owned(), schema)?;
            } else {
                // Extract the tag column indices from the action body. Note that since we assume
                // each tag column index is one byte, we directly use the slice of bytes as the list
                // of tag column indices.
                let (tag_indices, offset_data) = extract_argument_bytes(offset_data);

                // Extract the timestamp column index from the action body.
                let (timestamp_index, _offset_data) = extract_argument_bytes(offset_data);

                create_model_table(
                    catalog,
                    table_name.to_owned(),
                    schema,
                    tag_indices.to_vec(),
                    timestamp_index[0],
                )?;
            }

            // Confirm the table was created.
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
        let create_table_action = ActionType {
            r#type: "CreateTable".to_owned(),
            description: "Given a table name and a schema, create a table and add it to the \
            catalog.".to_owned(),
        };

        let create_model_table_action = ActionType {
            r#type: "CreateModelTable".to_owned(),
            description: "Given a table name, a schema, a list of tag column indices, and the \
            timestamp column index, create a model table and add it to the catalog.".to_owned()
        };

        let output = stream::iter(vec![Ok(create_table_action), Ok(create_model_table_action)]);
        Ok(Response::new(Box::pin(output)))
    }
}

/// Assumes `data` is a slice containing one or more arguments for an Action stored using the following
/// format: size of argument (2 bytes) followed by argument (length bytes). Returns a tuple containing
/// the first argument's bytes and `data` with the extracted argument's bytes removed.
fn extract_argument_bytes(data: &[u8]) -> (&[u8], &[u8]) {
    let size_bytes: [u8; 2] = data[..2].try_into().expect("Slice with incorrect length.");
    let size = u16::from_be_bytes(size_bytes) as usize;

    let argument_bytes = &data[2..(size + 2)];
    let remaining_bytes = &data[(size + 2)..];

    (argument_bytes, remaining_bytes)
}

/// Create a normal table and add it to the catalog. If the table already exists or if the Apache
/// Parquet file cannot be created, return [`Status`] error.
fn create_table(
    mut catalog: RwLockWriteGuard<Catalog>,
    table_name: String,
    schema: Schema,
) -> Result<(), Status> {
    // If the table already exists, return an error.
    let mut existing_tables = catalog.table_metadata.iter();
    if existing_tables.any(|table| table.name == table_name) {
        let message = format!("Table with name '{}' already exists.", table_name);
        return Err(Status::already_exists(message));
    }

    let file_name = format!("{}.parquet", table_name);
    let file_path = catalog.data_folder_path.join(file_name);

    // Create an empty Apache Parquet file to save the schema.
    let empty_batch = RecordBatch::new_empty(Arc::new(schema));
    StorageEngine::write_batch_to_apache_parquet_file(empty_batch, file_path.as_path())
        .map_err(|error| Status::invalid_argument(error.to_string()))?;

    // Save the table metadata in the catalog.
    let path_str = file_path.to_str().unwrap().to_string();
    let new_table_metadata = TableMetadata {
        name: table_name.clone(),
        path: path_str,
    };

    info!("Created table '{}'.", table_name);
    catalog.table_metadata.push(new_table_metadata);

    Ok(())
}

/// Create a model table, add it to the metadata database tables, and add it to the catalog. If the
/// table already exists or if the metadata cannot be written to the metadata database, return [`Status`] error.
fn create_model_table(
    mut catalog: RwLockWriteGuard<Catalog>,
    table_name: String,
    schema: Schema,
    tag_column_indices: Vec<u8>,
    timestamp_column_index: u8,
) -> Result<(), Status> {
    // If the table already exists, return an error.
    let mut existing_tables = catalog.new_model_table_metadata.iter();
    if existing_tables.any(|table| table.name == table_name) {
        let message = format!("Model table with name '{}' already exists.", table_name);
        return Err(Status::already_exists(message));
    }

    let model_table_metadata = NewModelTableMetadata::try_new(
        table_name.clone(),
        schema.clone(),
        tag_column_indices,
        timestamp_column_index,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;

    // Convert the schema to bytes so it can be saved as a BLOB in the metadata database.
    let options = IpcWriteOptions::default();
    let schema_as_ipc = SchemaAsIpc::new(&schema, &options);
    let ipc_message: IpcMessage = schema_as_ipc.try_into().map_err(|error: ArrowError| {
        Status::internal(error.to_string())
    })?;

    // Persist the new model table to the metadata database.
    let database_path = catalog.data_folder_path.join("metadata.sqlite3");
    save_model_table_to_database(database_path, &model_table_metadata, ipc_message.0)
        .map_err(|error| Status::internal(error.to_string()))?;

    info!("Created model table '{}'.", table_name);
    catalog.new_model_table_metadata.push(model_table_metadata);

    Ok(())
}

/// Save the created model table to the metadata database. This includes creating a tags table for the
/// model table, adding a row to the model_table_metadata table, and adding a row to the
/// model_table_field_columns table for each field column.
fn save_model_table_to_database(
    database_path: PathBuf,
    model_table_metadata: &NewModelTableMetadata,
    schema_bytes: Vec<u8>,
) -> Result<(), rusqlite::Error> {
    // Create a transaction to ensure the database state is consistent across tables.
    let mut connection = Connection::open(database_path)?;
    let transaction = connection.transaction()?;

    // Add a column definition for each tag field in the schema.
    let mut tag_columns = "".to_owned();
    let mut tag_indices = model_table_metadata.tag_column_indices.iter().peekable();

    while let Some(index) = tag_indices.next() {
        // If it is the last column in the query, adding a "," results in an SQL syntax error.
        let suffix = if tag_indices.peek().is_none() {""} else {", "};
        let field = model_table_metadata.schema.field(*index as usize);

        tag_columns.push_str(format!("{} TEXT NOT NULL{}", field.name(), suffix).as_str());
    }

    // Create a table_name_tags SQLite table to save the 54-bit tag hashes when ingesting data.
    // The query is executed with a formatted string since CREATE TABLE cannot take parameters.
    transaction.execute(format!("CREATE TABLE {}_tags (hash BIGINT PRIMARY KEY, {})",
        model_table_metadata.name, tag_columns).as_str(), ())?;

    // Add a new row in the model_table_metadata table to persist the model table.
    transaction.execute(
        "INSERT INTO model_table_metadata (table_name, schema, timestamp_column_index, tag_column_indices)
             VALUES (?1, ?2, ?3, ?4)",
        params![
            model_table_metadata.name,
            schema_bytes,
            model_table_metadata.timestamp_column_index,
            model_table_metadata.tag_column_indices
        ]
    )?;

    // Add a row for each field column to the model_table_field_columns table.
    let mut insert_statement = transaction.prepare(
        "INSERT INTO model_table_field_columns (table_name, column_name, column_index)
        VALUES (?1, ?2, ?3)")?;

    for (index, field) in model_table_metadata.schema.fields().iter().enumerate() {
        // Only add a row for the field if it is not the timestamp or a tag.
        let in_tag_indices = model_table_metadata.tag_column_indices.contains(&(index as u8));
        let is_timestamp = index == model_table_metadata.timestamp_column_index as usize;

        if !is_timestamp && !in_tag_indices {
            insert_statement.execute(params![model_table_metadata.name, field.name(), index])?;
        }
    }

    // Explicitly drop the statement to drop the borrow of "transaction" before the commit.
    mem::drop(insert_statement);

    transaction.commit()
}
