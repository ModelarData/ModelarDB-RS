/* Copyright 2023 The ModelarDB Contributors
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

//! Implementation of a request handler for Apache Arrow Flight in the form of [`FlightServiceHandler`].
//! An Apache Arrow Flight server that process requests using [`FlightServiceHandler`] can be started
//! with [`start_apache_arrow_flight_server()`].

use std::error::Error;
use std::net::SocketAddr;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::str;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use futures::{Stream, stream};
use modelardb_storage::parser;
use modelardb_storage::parser::ModelarDbStatement;
use modelardb_types::flight::protocol;
use modelardb_types::types::{Table, TimeSeriesTableMetadata};
use prost::Message;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use crate::Context;
use crate::error::{ModelarDbManagerError, Result};
use crate::metadata::ManagerMetadata;

/// Start an Apache Arrow Flight server on 0.0.0.0:`port`.
pub async fn start_apache_arrow_flight_server(context: Arc<Context>, port: u16) -> Result<()> {
    let localhost_with_port = "0.0.0.0:".to_owned() + &port.to_string();
    let localhost_with_port: SocketAddr = localhost_with_port.parse().map_err(|error| {
        ModelarDbManagerError::InvalidArgument(format!(
            "Unable to parse {localhost_with_port}: {error}"
        ))
    })?;
    let handler = FlightServiceHandler::new(context);
    let flight_service_server = FlightServiceServer::new(handler);

    info!("Starting Apache Arrow Flight on {}.", localhost_with_port);

    Server::builder()
        .add_service(flight_service_server)
        .serve(localhost_with_port)
        .await
        .map_err(|error| error.into())
}

/// Convert an `error` to a [`Status`] with [`tonic::Code::InvalidArgument`] as the code and `error`
/// converted to a [`String`] as the message.
pub fn error_to_status_invalid_argument(error: impl Error) -> Status {
    Status::invalid_argument(error.to_string())
}

/// Convert an `error` to a [`Status`] with [`tonic::Code::Internal`] as the code and `error`
/// converted to a [`String`] as the message.
pub fn error_to_status_internal(error: impl Error) -> Status {
    Status::internal(error.to_string())
}

/// Handler for processing Apache Arrow Flight requests.
/// [`FlightServiceHandler`] is based on the [Apache Arrow Flight examples]
/// published under Apache2.
///
/// [Apache Arrow Flight examples]: https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples
struct FlightServiceHandler {
    /// Singleton that provides access to the system's components.
    context: Arc<Context>,
}

impl FlightServiceHandler {
    pub fn new(context: Arc<Context>) -> FlightServiceHandler {
        Self { context }
    }

    /// Return the schema of the table with the name `table_name`. If the table does not exist or
    /// the schema cannot be retrieved, return [`Status`].
    async fn table_schema(&self, table_name: &str) -> StdResult<Arc<Schema>, Status> {
        let data_folder = &self.context.remote_data_folder;

        if data_folder
            .is_normal_table(table_name)
            .await
            .map_err(error_to_status_internal)?
        {
            let delta_table = self
                .context
                .remote_data_folder
                .delta_table(table_name)
                .await
                .map_err(error_to_status_internal)?;

            let schema = delta_table
                .get_schema()
                .map_err(error_to_status_internal)?
                .try_into()
                .map_err(error_to_status_internal)?;

            Ok(Arc::new(schema))
        } else if data_folder
            .is_time_series_table(table_name)
            .await
            .map_err(error_to_status_internal)?
        {
            let time_series_table_metadata = data_folder
                .time_series_table_metadata_for_time_series_table(table_name)
                .await
                .map_err(error_to_status_internal)?;

            Ok(time_series_table_metadata.query_schema)
        } else {
            Err(Status::invalid_argument(format!(
                "Table with name '{table_name}' does not exist.",
            )))
        }
    }

    /// Return [`Ok`] if a table named `table_name` does not exist already in the metadata
    /// database, otherwise return [`Status`].
    async fn check_if_table_exists(&self, table_name: &str) -> StdResult<(), Status> {
        let existing_tables = self
            .context
            .remote_data_folder
            .table_names()
            .await
            .map_err(error_to_status_internal)?;

        if existing_tables
            .iter()
            .any(|existing_table| existing_table == table_name)
        {
            let message = format!("Table with name '{table_name}' already exists.");
            Err(Status::already_exists(message))
        } else {
            Ok(())
        }
    }

    /// Create a normal table, save it to the metadata Delta Lake and create it for each node
    /// controlled by the manager. If the normal table cannot be saved to the metadata Delta Lake or
    /// created for each node, return [`Status`].
    async fn save_and_create_cluster_normal_table(
        &self,
        table_name: &str,
        schema: &Schema,
    ) -> StdResult<(), Status> {
        // Create an empty Delta Lake table.
        self.context
            .remote_data_folder
            .create_normal_table(table_name, schema)
            .await
            .map_err(error_to_status_internal)?;

        // Persist the new normal table to the metadata Delta Lake.
        self.context
            .remote_data_folder
            .save_normal_table_metadata(table_name)
            .await
            .map_err(error_to_status_internal)?;

        // Register and save the table to each node in the cluster.
        let protobuf_bytes =
            modelardb_types::flight::encode_and_serialize_normal_table_metadata(table_name, schema)
                .map_err(error_to_status_internal)?;

        let action = Action {
            r#type: "CreateTable".to_owned(),
            body: protobuf_bytes.into(),
        };

        self.context
            .cluster
            .read()
            .await
            .cluster_do_action(action, &self.context.key)
            .await
            .map_err(error_to_status_internal)?;

        info!("Created normal table '{}'.", table_name);

        Ok(())
    }

    /// Create a time series table, save it to the metadata Delta Lake and create it for each node
    /// controlled by the manager. If the time series table cannot be saved to the metadata Delta
    /// Lake or created for each node, return [`Status`].
    async fn save_and_create_cluster_time_series_table(
        &self,
        time_series_table_metadata: Arc<TimeSeriesTableMetadata>,
    ) -> StdResult<(), Status> {
        // Create an empty Delta Lake table.
        self.context
            .remote_data_folder
            .create_time_series_table(&time_series_table_metadata)
            .await
            .map_err(error_to_status_internal)?;

        // Persist the new time series table to the metadata Delta Lake.
        self.context
            .remote_data_folder
            .save_time_series_table_metadata(&time_series_table_metadata)
            .await
            .map_err(error_to_status_internal)?;

        // Register and save the time series table to each node in the cluster.
        let protobuf_bytes =
            modelardb_types::flight::encode_and_serialize_time_series_table_metadata(
                &time_series_table_metadata,
            )
            .map_err(error_to_status_internal)?;

        let action = Action {
            r#type: "CreateTable".to_owned(),
            body: protobuf_bytes.into(),
        };

        self.context
            .cluster
            .read()
            .await
            .cluster_do_action(action, &self.context.key)
            .await
            .map_err(error_to_status_internal)?;

        info!(
            "Created time series table '{}'.",
            time_series_table_metadata.name
        );

        Ok(())
    }

    /// Drop the table from the metadata Delta Lake, the data Delta Lake, and from each node
    /// controlled by the manager. If the table does not exist or the table cannot be dropped from
    /// the remote data folder and from each node, return [`Status`].
    async fn drop_cluster_table(&self, table_name: &str) -> StdResult<(), Status> {
        // Drop the table from the remote data folder metadata Delta Lake. This will return an error
        // if the table does not exist.
        self.context
            .remote_data_folder
            .drop_table_metadata(table_name)
            .await
            .map_err(error_to_status_internal)?;

        // Drop the table from the remote data folder data Delta lake.
        self.context
            .remote_data_folder
            .drop_table(table_name)
            .await
            .map_err(error_to_status_internal)?;

        // Drop the table from the nodes controlled by the manager.
        self.context
            .cluster
            .read()
            .await
            .cluster_do_get(&format!("DROP TABLE {table_name}"), &self.context.key)
            .await
            .map_err(error_to_status_internal)?;

        Ok(())
    }

    /// Truncate the table in the remote data folder and at each node controlled by the manager. If
    /// the table does not exist or the table cannot be truncated in the remote data folder and at
    /// each node, return [`Status`].
    async fn truncate_cluster_table(&self, table_name: &str) -> StdResult<(), Status> {
        if self.check_if_table_exists(table_name).await.is_ok() {
            return Err(Status::invalid_argument(format!(
                "Table with name '{table_name}' does not exist.",
            )));
        }

        // Truncate the table in the remote data folder data Delta lake.
        self.context
            .remote_data_folder
            .truncate_table(table_name)
            .await
            .map_err(error_to_status_internal)?;

        // Truncate the table in the nodes controlled by the manager.
        self.context
            .cluster
            .read()
            .await
            .cluster_do_get(&format!("TRUNCATE TABLE {table_name}"), &self.context.key)
            .await
            .map_err(error_to_status_internal)?;

        Ok(())
    }

    /// Vacuum the table in the remote data folder and at each node controlled by the manager. If
    /// the table does not exist or the table cannot be vacuumed in the remote data folder
    /// and at each node, return [`Status`].
    async fn vacuum_cluster_table(
        &self,
        table_name: &str,
        maybe_retention_period_in_seconds: Option<u64>,
    ) -> StdResult<(), Status> {
        // Vacuum the table in the remote data folder Delta lake.
        self.context
            .remote_data_folder
            .vacuum_table(table_name, maybe_retention_period_in_seconds)
            .await
            .map_err(error_to_status_internal)?;

        // Vacuum the table in the nodes controlled by the manager.
        let vacuum_sql = if let Some(retention_period) = maybe_retention_period_in_seconds {
            format!("VACUUM {table_name} RETAIN {retention_period}")
        } else {
            format!("VACUUM {table_name}")
        };

        self.context
            .cluster
            .read()
            .await
            .cluster_do_get(&vacuum_sql, &self.context.key)
            .await
            .map_err(error_to_status_internal)?;

        Ok(())
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceHandler {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = StdResult<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = StdResult<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream =
        Pin<Box<dyn Stream<Item = StdResult<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = StdResult<PutResult, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = StdResult<FlightData, Status>> + Send + Sync + 'static>>;
    type DoActionStream = Pin<
        Box<dyn Stream<Item = StdResult<arrow_flight::Result, Status>> + Send + Sync + 'static>,
    >;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = StdResult<ActionType, Status>> + Send + Sync + 'static>>;

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
        // Retrieve the table names from the metadata Delta Lake.
        let table_names = self
            .context
            .remote_data_folder
            .table_names()
            .await
            .map_err(error_to_status_internal)?;

        let flight_descriptor = FlightDescriptor::new_path(table_names);
        let flight_info = FlightInfo::new().with_descriptor(flight_descriptor);

        let output = stream::once(async { Ok(flight_info) });
        Ok(Response::new(Box::pin(output)))
    }

    /// Given a query, return [`FlightInfo`] containing [`FlightEndpoints`](FlightEndpoint)
    /// describing which cloud nodes should be used to execute the query. The query must be
    /// provided in `FlightDescriptor.cmd`.
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> StdResult<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();

        // Extract the query.
        let query = str::from_utf8(&flight_descriptor.cmd)
            .map_err(error_to_status_invalid_argument)?
            .to_owned();

        // Retrieve the cloud node that should execute the given query.
        let mut cluster = self.context.cluster.write().await;
        let cloud_node = cluster
            .query_node()
            .map_err(|error| Status::failed_precondition(error.to_string()))?;

        info!(
            "Assigning query '{query}' to cloud node with url '{}'.",
            cloud_node.url
        );

        // All data in the query result should be retrieved using a single endpoint.
        let endpoint = FlightEndpoint::new()
            .with_ticket(Ticket::new(query))
            .with_location(cloud_node.url);

        // schema is empty and total_records and total_bytes are -1 since we do not know anything
        // about the result of the query at this point.
        let flight_info = FlightInfo::new()
            .with_descriptor(flight_descriptor)
            .with_endpoint(endpoint)
            .with_ordered(true);

        Ok(Response::new(flight_info))
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
        let table_name = flight_descriptor
            .path
            .first()
            .ok_or_else(|| Status::invalid_argument("No table name in FlightDescriptor.path."))?;

        let schema = self.table_schema(table_name).await?;

        let options = IpcWriteOptions::default();
        let schema_as_ipc = SchemaAsIpc::new(&schema, &options);
        let schema_result = schema_as_ipc.try_into().map_err(error_to_status_internal)?;

        Ok(Response::new(schema_result))
    }

    /// Execute a SQL statement provided in UTF-8 and return the schema of the result followed by
    /// the result itself. Currently, CREATE TABLE, CREATE TIME SERIES TABLE, TRUNCATE TABLE,
    /// DROP TABLE, and VACUUM are supported.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> StdResult<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        // Extract the query.
        let sql = str::from_utf8(&ticket.ticket)
            .map_err(error_to_status_invalid_argument)?
            .to_owned();

        // Parse the query.
        info!("Received SQL: '{}'.", sql);

        let modelardb_statement = parser::tokenize_and_parse_sql_statement(&sql)
            .map_err(error_to_status_invalid_argument)?;

        // Execute the statement.
        info!("Executing SQL: '{}'.", sql);

        match modelardb_statement {
            ModelarDbStatement::CreateNormalTable { name, schema } => {
                self.check_if_table_exists(&name).await?;
                self.save_and_create_cluster_normal_table(&name, &schema)
                    .await?;
            }
            ModelarDbStatement::CreateTimeSeriesTable(time_series_table_metadata) => {
                self.check_if_table_exists(&time_series_table_metadata.name)
                    .await?;
                self.save_and_create_cluster_time_series_table(time_series_table_metadata)
                    .await?;
            }
            ModelarDbStatement::TruncateTable(table_names) => {
                for table_name in table_names {
                    self.truncate_cluster_table(&table_name).await?;
                }
            }
            ModelarDbStatement::DropTable(table_names) => {
                for table_name in table_names {
                    self.drop_cluster_table(&table_name).await?;
                }
            }
            ModelarDbStatement::Vacuum(mut table_names, maybe_retention_period_in_seconds) => {
                // Vacuum all tables if no table names are provided.
                if table_names.is_empty() {
                    table_names = self
                        .context
                        .remote_data_folder
                        .table_names()
                        .await
                        .map_err(error_to_status_internal)?;
                }

                for table_name in table_names {
                    self.vacuum_cluster_table(&table_name, maybe_retention_period_in_seconds)
                        .await?;
                }
            }
            // .. is not used so a compile error is raised if a new ModelarDbStatement is added.
            ModelarDbStatement::Statement(_) | ModelarDbStatement::IncludeSelect(..) => {
                return Err(Status::invalid_argument(
                    "Expected CREATE TABLE, CREATE TIME SERIES TABLE, TRUNCATE TABLE, or DROP TABLE.",
                ));
            }
        };

        // Confirm the SQL statement was executed by returning a stream with a schema but no data.
        // stream::empty() cannot be used since do_get requires a schema in the response.
        let options = IpcWriteOptions::default();
        let schema_as_flight_data = SchemaAsIpc::new(&Schema::empty(), &options).into();
        let output = stream::once(async { Ok(schema_as_flight_data) });

        Ok(Response::new(Box::pin(output)))
    }

    /// Not implemented.
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> StdResult<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
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
    /// * `CreateTable`: Create the table given in the [`TableMetadata`](protocol::TableMetadata)
    /// protobuf message in the action body. The table is created for each node in the cluster of
    /// nodes controlled by the manager.
    /// * `RegisterNode`: Register either an edge or cloud node with the manager. The url and mode
    /// of the node must be provided in the action body as a [`NodeMetadata`](protocol::NodeMetadata)
    /// protobuf message. The node is added to the cluster of nodes controlled by the manager and
    /// the key and object store used in the cluster is returned as a
    /// [`ManagerMetadata`](protocol::ManagerMetadata) protobuf message.
    /// * `RemoveNode`: Remove the node given in the [`NodeMetadata`](protocol::NodeMetadata)
    /// protobuf message in the action body. The node is removed from the cluster of nodes
    /// controlled by the manager and the process running on the node is killed.
    /// * `NodeType`: Get the type of the node. The type is always `manager`. The type of the node
    /// is returned as a string.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> StdResult<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        info!("Received request to perform action '{}'.", action.r#type);

        if action.r#type == "CreateTable" {
            // Deserialize and extract the table metadata from the protobuf message in the action body.
            let table_metadata =
                modelardb_types::flight::deserialize_and_extract_table_metadata(&action.body)
                    .map_err(error_to_status_invalid_argument)?;

            match table_metadata {
                Table::NormalTable(table_name, schema) => {
                    self.check_if_table_exists(&table_name).await?;
                    self.save_and_create_cluster_normal_table(&table_name, &schema)
                        .await?;
                }
                Table::TimeSeriesTable(metadata) => {
                    self.check_if_table_exists(&metadata.name).await?;
                    self.save_and_create_cluster_time_series_table(Arc::new(metadata))
                        .await?;
                }
            }

            // Confirm the tables were created.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "RegisterNode" {
            // Extract the node from the action body.
            let node_metadata = protocol::NodeMetadata::decode(action.body)
                .map_err(error_to_status_invalid_argument)?;
            let node = modelardb_types::flight::decode_node_metadata(&node_metadata)
                .map_err(error_to_status_invalid_argument)?;

            // Use the cluster to register the node in memory. This returns an error if the node is
            // already registered.
            self.context
                .cluster
                .write()
                .await
                .register_node(node.clone())
                .map_err(error_to_status_internal)?;

            // Use the metadata manager to persist the node to the metadata Delta Lake. Note that if
            // this fails, the metadata Delta Lake and the cluster will be out of sync until the
            // manager is restarted.
            self.context
                .remote_data_folder
                .save_node(node)
                .await
                .map_err(error_to_status_internal)?;

            let manager_metadata = protocol::ManagerMetadata {
                key: self
                    .context
                    .key
                    .to_str()
                    .expect("key should not contain invalid characters.")
                    .to_owned(),
                storage_configuration: Some(self.context.remote_storage_configuration.clone()),
            };

            let protobuf_bytes = manager_metadata.encode_to_vec();

            // Return the key for the manager and the storage configuration for the remote object store.
            Ok(Response::new(Box::pin(stream::once(async {
                Ok(FlightResult {
                    body: protobuf_bytes.into(),
                })
            }))))
        } else if action.r#type == "RemoveNode" {
            let node_metadata = protocol::NodeMetadata::decode(action.body)
                .map_err(error_to_status_invalid_argument)?;

            // Remove the node with the given url from the metadata Delta Lake.
            self.context
                .remote_data_folder
                .remove_node(&node_metadata.url)
                .await
                .map_err(error_to_status_internal)?;

            // Remove the node with the given url from the cluster and kill it. Note that if this fails,
            // the cluster and metadata Delta Lake will be out of sync until the manager is restarted.
            self.context
                .cluster
                .write()
                .await
                .remove_node(&node_metadata.url, &self.context.key)
                .await
                .map_err(error_to_status_internal)?;

            // Confirm the node was removed.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "NodeType" {
            let flight_result = FlightResult {
                body: "manager".bytes().collect(),
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
        let create_tables_action = ActionType {
            r#type: "CreateTable".to_owned(),
            description: "Create the table given in the protobuf message in the action body."
                .to_owned(),
        };

        let register_node_action = ActionType {
            r#type: "RegisterNode".to_owned(),
            description: "Register either an edge or cloud node with the manager.".to_owned(),
        };

        let remove_node_action = ActionType {
            r#type: "RemoveNode".to_owned(),
            description: "Remove a node from the manager and kill the process running on the node."
                .to_owned(),
        };

        let node_type_action = ActionType {
            r#type: "NodeType".to_owned(),
            description: "Get the type of the node.".to_owned(),
        };

        let output = stream::iter(vec![
            Ok(create_tables_action),
            Ok(register_node_action),
            Ok(remove_node_action),
            Ok(node_type_action),
        ]);

        Ok(Response::new(Box::pin(output)))
    }
}
