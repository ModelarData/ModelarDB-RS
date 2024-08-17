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
use std::str;
use std::str::FromStr;
use std::sync::Arc;

use arrow::error::ArrowError;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use futures::{stream, Stream};
use modelardb_common::arguments::{decode_argument, encode_argument};
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::parser::ValidStatement;
use modelardb_common::types::ServerMode;
use modelardb_common::{parser, remote};
use tokio::runtime::Runtime;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use crate::cluster::Node;
use crate::Context;

/// Start an Apache Arrow Flight server on 0.0.0.0:`port`.
pub fn start_apache_arrow_flight_server(
    context: Arc<Context>,
    runtime: &Arc<Runtime>,
    port: u16,
) -> Result<(), Box<dyn Error>> {
    let localhost_with_port = "0.0.0.0:".to_owned() + &port.to_string();
    let localhost_with_port: SocketAddr = localhost_with_port.parse()?;
    let handler = FlightServiceHandler::new(context);
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
}

impl FlightServiceHandler {
    pub fn new(context: Arc<Context>) -> FlightServiceHandler {
        Self { context }
    }

    /// Return [`Ok`] if a table named `table_name` does not exist already in the metadata
    /// database, otherwise return [`Status`].
    async fn check_if_table_exists(&self, table_name: &str) -> Result<(), Status> {
        let existing_tables = self
            .context
            .remote_data_folder
            .metadata_manager
            .table_metadata_column("table_name")
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

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

    /// Create a normal table, save it to the metadata database and create it for each node
    /// controlled by the manager. If the table cannot be saved to the metadata database or
    /// created for each node, return [`Status`].
    async fn save_and_create_cluster_tables(
        &self,
        table_name: String,
        sql: &str,
    ) -> Result<(), Status> {
        // Persist the new table to the metadata database.
        self.context
            .remote_data_folder
            .metadata_manager
            .table_metadata_manager
            .save_table_metadata(&table_name, sql)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        // Register and save the table to each node in the cluster.
        self.context
            .cluster
            .read()
            .await
            .create_tables(&table_name, sql, &self.context.key)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        info!("Created table '{}'.", table_name);

        Ok(())
    }

    /// Create a model table, save it to the metadata database and create it for each node
    /// controlled by the manager. If the table cannot be saved to the metadata database or
    /// created for each node, return [`Status`].
    async fn save_and_create_cluster_model_tables(
        &self,
        model_table_metadata: ModelTableMetadata,
        sql: &str,
    ) -> Result<(), Status> {
        // Persist the new model table to the metadata database.
        self.context
            .remote_data_folder
            .metadata_manager
            .table_metadata_manager
            .save_model_table_metadata(&model_table_metadata, sql)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        // Register and save the model table to each node in the cluster.
        self.context
            .cluster
            .read()
            .await
            .create_tables(&model_table_metadata.name, sql, &self.context.key)
            .await
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
        // Retrieve the table names from the metadata database.
        let table_names = self
            .context
            .remote_data_folder
            .metadata_manager
            .table_metadata_column("table_name")
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

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
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();

        // Extract the query.
        let query = str::from_utf8(&flight_descriptor.cmd)
            .map_err(|error| Status::invalid_argument(error.to_string()))?
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
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Provide the schema of a table in the catalog. The name of the table must be provided as the
    /// first element in `FlightDescriptor.path`.
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let flight_descriptor = request.into_inner();
        let table_name = remote::table_name_from_flight_descriptor(&flight_descriptor)?;

        let table_sql = self
            .context
            .remote_data_folder
            .metadata_manager
            .table_sql(table_name)
            .await
            .map_err(|error| {
                Status::not_found(format!(
                    "Table with name '{table_name}' does not exist: {error}",
                ))
            })?;

        // Parse the SQL and perform semantic checks to extract the schema from the statement.
        // unwrap() is safe since the SQL was parsed and checked when the table was created.
        let statement = parser::tokenize_and_parse_sql(table_sql.as_str()).unwrap();
        let valid_statement = parser::semantic_checks_for_create_table(statement).unwrap();

        let schema = match valid_statement {
            ValidStatement::CreateTable { schema, .. } => Arc::new(schema),
            ValidStatement::CreateModelTable(model_table_metadata) => model_table_metadata.schema,
        };

        let options = IpcWriteOptions::default();
        let schema_as_ipc = SchemaAsIpc::new(&schema, &options);
        let schema_result = schema_as_ipc
            .try_into()
            .map_err(|error: ArrowError| Status::internal(error.to_string()))?;

        Ok(Response::new(schema_result))
    }

    /// Not implemented.
    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Not implemented.
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Not implemented.
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Perform a specific action based on the type of the action in `request`. Currently, the
    /// following actions are supported:
    /// * `InitializeDatabase`: Given a list of existing table names, respond with the SQL required
    /// to create the tables and model tables that are missing in the list. The list of table names
    /// is also checked to make sure all given tables actually exist.
    /// * `UpdateRemoteObjectStore`: Update the remote object store, overriding the current
    /// remote object store. Each argument in the body should start with the size of the argument,
    /// immediately followed by the argument value. The first argument should be the object store
    /// type, specifically either 's3' or 'azureblobstorage'. The remaining arguments should be
    /// the arguments required to connect to the object store. The remote object store is updated
    /// for all nodes controlled by the manager.
    /// * `CommandStatementUpdate`: Execute a SQL query containing a command that does not
    /// return a result. These commands can be `CREATE TABLE table_name(...` which creates a
    /// normal table, and `CREATE MODEL TABLE table_name(...` which creates a model table.
    /// The table is created for all nodes controlled by the manager.
    /// * `RegisterNode`: Register either an edge or cloud node with the manager. The node is added
    /// to the cluster of nodes controlled by the manager and the key and object store used in the
    /// cluster is returned.
    /// * `RemoveNode`: Remove a node from the cluster of nodes controlled by the manager and
    /// kill the process running on the node. The specific node to remove is given through the
    /// uniquely identifying URL of the node.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        info!("Received request to perform action '{}'.", action.r#type);

        if action.r#type == "InitializeDatabase" {
            // Extract the list of comma seperated tables that already exist in the node.
            let node_tables: Vec<&str> = str::from_utf8(&action.body)
                .map_err(|error| Status::invalid_argument(error.to_string()))?
                .split(',')
                .filter(|table| !table.is_empty())
                .collect();

            // Get the table names in the clusters current database schema.
            let cluster_tables = self
                .context
                .remote_data_folder
                .metadata_manager
                .table_metadata_column("table_name")
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Check that all the node's tables exist in the cluster's database schema already.
            let invalid_node_tables: Vec<&str> = node_tables
                .iter()
                .filter(|table| !cluster_tables.contains(&table.to_string()))
                .cloned()
                .collect();

            if invalid_node_tables.is_empty() {
                // For each table that does not already exist in the node, retrieve the SQL
                // required to create it.
                let missing_cluster_tables = cluster_tables
                    .iter()
                    .filter(|table| !node_tables.contains(&table.as_str()));

                let mut table_sql_queries: Vec<String> = vec![];

                for table in missing_cluster_tables {
                    table_sql_queries.push(
                        self.context
                            .remote_data_folder
                            .metadata_manager
                            .table_sql(table)
                            .await
                            .map_err(|error| Status::internal(error.to_string()))?,
                    )
                }

                let response_body = table_sql_queries.join(";").as_bytes().to_vec();

                // Return the SQL for the tables that need to be created in the requesting node.
                Ok(Response::new(Box::pin(stream::once(async {
                    Ok(FlightResult {
                        body: response_body.into(),
                    })
                }))))
            } else {
                Err(Status::invalid_argument(format!(
                    "The table(s) '{}' do not exist in the cluster database schema.",
                    invalid_node_tables.join(", ")
                )))
            }
        } else if action.r#type == "UpdateRemoteObjectStore" {
            // TODO: Should be removed in separate PR.

            // For each node in the cluster, update the remote object store.
            self.context
                .cluster
                .read()
                .await
                .update_remote_object_stores(action.body, &self.context.key)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Confirm the remote object store was updated.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "CommandStatementUpdate" {
            // Read the SQL from the action.
            let sql = str::from_utf8(&action.body)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            info!("Received request to execute '{}'.", sql);

            // Parse the SQL.
            let statement = parser::tokenize_and_parse_sql(sql)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            // Perform semantic checks to ensure the parsed SQL is supported.
            let valid_statement = parser::semantic_checks_for_create_table(statement)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            // Create the table or model table if it does not already exist.
            match valid_statement {
                ValidStatement::CreateTable { name, .. } => {
                    self.check_if_table_exists(&name).await?;
                    self.save_and_create_cluster_tables(name, sql).await?;
                }
                ValidStatement::CreateModelTable(model_table_metadata) => {
                    self.check_if_table_exists(&model_table_metadata.name)
                        .await?;
                    self.save_and_create_cluster_model_tables(model_table_metadata, sql)
                        .await?;
                }
            };

            // Confirm the table was created.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "RegisterNode" {
            // Extract the node from the action body.
            let (url, offset_data) = decode_argument(&action.body)?;
            let (mode, _offset_data) = decode_argument(offset_data)?;

            let server_mode = ServerMode::from_str(mode).map_err(Status::invalid_argument)?;
            let node = Node::new(url.to_string(), server_mode.clone());

            // Use the metadata manager to persist the node to the metadata database.
            self.context
                .remote_data_folder
                .metadata_manager
                .save_node(&node)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Use the cluster to register the node in memory. Note that if this fails, the cluster
            // and metadata database will be out of sync until the manager is restarted.
            self.context
                .cluster
                .write()
                .await
                .register_node(node)
                .map_err(|error| Status::internal(error.to_string()))?;

            // unwrap() is safe since the key cannot contain invalid characters.
            let mut response_body = encode_argument(self.context.key.to_str().unwrap());

            let mut connection_info = self.context.remote_data_folder.connection_info.clone();
            response_body.append(&mut connection_info);

            // Return the key for the manager and the connection info for the remote object store.
            Ok(Response::new(Box::pin(stream::once(async {
                Ok(FlightResult {
                    body: response_body.into(),
                })
            }))))
        } else if action.r#type == "RemoveNode" {
            let (url, _offset_data) = decode_argument(&action.body)?;

            // Remove the node with the given url from the metadata database.
            self.context
                .remote_data_folder
                .metadata_manager
                .remove_node(url)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Remove the node with the given url from the cluster and kill it. Note that if this fails,
            // the cluster and metadata database will be out of sync until the manager is restarted.
            self.context
                .cluster
                .write()
                .await
                .remove_node(url, &self.context.key)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Confirm the node was removed.
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
        let initialize_database_action = ActionType {
            r#type: "InitializeDatabase".to_owned(),
            description: "Return the SQL required to create all tables and models tables \
            currently in the manager's database schema."
                .to_owned(),
        };

        let update_remote_object_store_action = ActionType {
            r#type: "UpdateRemoteObjectStore".to_owned(),
            description:
                "Update the remote object store, overriding the current remote object store."
                    .to_owned(),
        };

        let command_statement_update_action = ActionType {
            r#type: "CommandStatementUpdate".to_owned(),
            description:
                "Execute a SQL query containing a single command that produces no results."
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

        let output = stream::iter(vec![
            Ok(initialize_database_action),
            Ok(update_remote_object_store_action),
            Ok(command_statement_update_action),
            Ok(register_node_action),
            Ok(remove_node_action),
        ]);

        Ok(Response::new(Box::pin(output)))
    }
}
