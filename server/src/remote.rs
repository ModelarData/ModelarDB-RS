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
use std::pin::Pin;
use std::str;
use std::sync::{Arc, RwLockWriteGuard};

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::utils;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, IpcMessage, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use datafusion::arrow::{array::ArrayRef, datatypes::SchemaRef, ipc::writer::IpcWriteOptions, error::ArrowError};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use futures::{stream, Stream, StreamExt};
use object_store;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::{Catalog, Context};
use crate::storage::StorageEngine;

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
        let catalog = self.context.catalog.read().unwrap();
        let table_names = catalog.table_and_model_table_names();
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
    /// that can be used for ingestion when given a table name, a schema, a list of indices
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
        info!("Received request to perform action: {}", action.r#type);

        if action.r#type == "CreateTable" || action.r#type == "CreateModelTable" {
            // Extract the table name from the action body.
            let (table_name_bytes, table_name_offset) = extract_argument_bytes(&action.body);
            let table_name = str::from_utf8(table_name_bytes)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            // Check if the table name is a valid object_store path.
            object_store::path::Path::parse(table_name)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            // Extract the schema from the action body.
            let offset_data = &action.body[table_name_offset..];
            let (schema_bytes, schema_offset) = extract_argument_bytes(offset_data);
            let ipc_message = IpcMessage(Vec::from(schema_bytes));
            let schema = Schema::try_from(ipc_message)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            if action.r#type == "CreateTable" {
                let mut catalog = self.context.catalog.write().unwrap();
                create_table(catalog, table_name.to_owned(), schema);
            } else {
                let offset_data = &action.body[(table_name_offset + schema_offset)..];
                let (tag_indices, tag_offset) = extract_argument_bytes(offset_data);

                let offset = (table_name_offset + schema_offset + tag_offset);
                let offset_data = &action.body[offset..];
                let (timestamp_index, _offset) = extract_argument_bytes(offset_data);

                // TODO: If the table already exists, return an error.
                // TODO: Create a new model table metadata.

                // TODO: If it passes the checks, create a table_tags SQLite table.
                // TODO: Create a new row in the model_table_metadata table.
                // TODO: Add the field columns to the columns table.
                // TODO: Maybe do all this as a transaction?
                // TODO: Create test to ensure this happens.

                // TODO: The table should be added to the catalog (as a model table?).
            }

            // Confirm the table was created.
            Ok(Response::new(Box::pin(stream::empty())))
        } else {
            Err(Status::unimplemented("Action not implemented."))
        }
    }

    /// Return all available actions, including both a name of the action and a description.
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
            description: "Given a table name, a schema, and a list of tag column indices, \
            create a model table and add it to the catalog.".to_owned()
        };

        let output = stream::iter(vec![Ok(create_table_action), Ok(create_model_table_action)]);
        Ok(Response::new(Box::pin(output)))
    }
}

/// Given an array of bytes, extract a slice that contains the first argument. It is assumed that the
/// length of the argument is in the first two bytes. A tuple with the argument bytes and an offset
/// specifying where the next argument starts, is returned.
fn extract_argument_bytes(data: &[u8]) -> (&[u8], usize) {
    let size_bytes: [u8; 2] = data[..2].try_into().expect("Slice with incorrect length.");
    let size = u16::from_be_bytes(size_bytes) as usize;

    let argument_bytes = &data[2..(size + 2)];

    (argument_bytes, size + 2)
}

/// Create a normal table. If the table already exists or if the Apache Parquet file cannot be
/// created, return [`Status`] error.
fn create_table(
    mut catalog: RwLockWriteGuard<Catalog>,
    table_name: String,
    schema: Schema
) -> Result<(), Status> {
    let file_name = format!("{}.parquet", table_name);
    let file_path = catalog.data_folder_path.join(file_name);

    // Save the table in the catalog.
    let path_str = file_path.to_str().unwrap().to_string();
    catalog.insert_table(table_name.to_owned(), path_str)
        .map_err(|error| Status::already_exists(error.to_string()))?;

    // TODO: If this fails, the table should not be added to the catalog.
    // Create an empty Apache Parquet file to save the schema.
    let empty_batch = RecordBatch::new_empty(Arc::new(schema));
    StorageEngine::write_batch_to_apache_parquet_file(empty_batch, file_path.as_path())
        .map_err(|error| Status::invalid_argument(error.to_string()))?;

    Ok(())
}
