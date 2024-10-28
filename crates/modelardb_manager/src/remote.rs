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

use std::net::SocketAddr;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::str;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use futures::{stream, Stream};
use modelardb_common::arguments;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::parser::ValidStatement;
use modelardb_common::{parser, remote};
use modelardb_types::types::ServerMode;
use tokio::runtime::Runtime;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use crate::cluster::Node;
use crate::error::{ModelarDbManagerError, Result};
use crate::Context;

/// Start an Apache Arrow Flight server on 0.0.0.0:`port`.
pub fn start_apache_arrow_flight_server(
    context: Arc<Context>,
    runtime: &Arc<Runtime>,
    port: u16,
) -> Result<()> {
    let localhost_with_port = "0.0.0.0:".to_owned() + &port.to_string();
    let localhost_with_port: SocketAddr = localhost_with_port.parse().map_err(|error| {
        ModelarDbManagerError::InvalidArgument(format!(
            "Unable to parse {localhost_with_port}: {error}"
        ))
    })?;
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
        .map_err(|error| error.into())
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
    async fn check_if_table_exists(&self, table_name: &str) -> StdResult<(), Status> {
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

    /// Create a normal table, save it to the metadata Delta Lake and create it for each node
    /// controlled by the manager. If the table cannot be saved to the metadata Delta Lake or
    /// created for each node, return [`Status`].
    async fn save_and_create_cluster_tables(
        &self,
        table_name: &str,
        schema: &Schema,
        sql: &str,
    ) -> StdResult<(), Status> {
        // Create an empty Delta Lake table.
        self.context
            .remote_data_folder
            .delta_lake
            .create_delta_lake_table(table_name, schema)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        // Persist the new table to the metadata Delta Lake.
        self.context
            .remote_data_folder
            .metadata_manager
            .table_metadata_manager
            .save_table_metadata(table_name, sql)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        // Register and save the table to each node in the cluster.
        self.context
            .cluster
            .read()
            .await
            .create_table(sql, &self.context.key)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        info!("Created table '{}'.", table_name);

        Ok(())
    }

    /// Create a model table, save it to the metadata Delta Lake and create it for each node
    /// controlled by the manager. If the table cannot be saved to the metadata Delta Lake or
    /// created for each node, return [`Status`].
    async fn save_and_create_cluster_model_tables(
        &self,
        model_table_metadata: Arc<ModelTableMetadata>,
        sql: &str,
    ) -> StdResult<(), Status> {
        // Create an empty Delta Lake table.
        self.context
            .remote_data_folder
            .delta_lake
            .create_delta_lake_model_table(&model_table_metadata.name)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        // Persist the new model table to the metadata Delta Lake.
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
            .create_table(sql, &self.context.key)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        info!("Created model table '{}'.", model_table_metadata.name);

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
            .metadata_manager
            .table_metadata_manager
            .drop_table_metadata(table_name)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        // Drop the table from the remote data folder data Delta lake.
        self.context
            .remote_data_folder
            .delta_lake
            .drop_delta_lake_table(table_name)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        // Drop the table from the nodes controlled by the manager.
        self.context
            .cluster
            .read()
            .await
            .drop_table(table_name, &self.context.key)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        Ok(())
    }

    /// Truncate the table in the metadata Delta Lake, the data Delta Lake, and in each node
    /// controlled by the manager. If the table does not exist or the table cannot be truncated in
    /// the remote data folder and in each node, return [`Status`].
    async fn truncate_cluster_table(&self, table_name: &str) -> StdResult<(), Status> {
        // Truncate the table in the remote data folder metadata Delta Lake. This will return an
        // error if the table does not exist.
        self.context
            .remote_data_folder
            .metadata_manager
            .table_metadata_manager
            .truncate_table_metadata(table_name)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        // Truncate the table in the remote data folder data Delta lake.
        self.context
            .remote_data_folder
            .delta_lake
            .truncate_delta_lake_table(table_name)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        // Truncate the table in the nodes controlled by the manager.
        self.context
            .cluster
            .read()
            .await
            .truncate_table(table_name, &self.context.key)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

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
    ) -> StdResult<Response<FlightInfo>, Status> {
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
            ValidStatement::CreateModelTable(model_table_metadata) => {
                model_table_metadata.schema.clone()
            }
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
    ) -> StdResult<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
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
    /// * `InitializeDatabase`: Given a list of existing table names, respond with the SQL required
    /// to create the tables and model tables that are missing in the list. The list of table names
    /// is also checked to make sure all given tables actually exist.
    /// * `CreateTable`: Execute a SQL query containing a command that creates a table. These
    /// commands can be `CREATE TABLE table_name(...` which creates a normal table, and
    /// `CREATE MODEL TABLE table_name(...` which creates a model table. The table is created
    /// for all nodes controlled by the manager.
    /// * `DropTable`: Drop a table previously created with `CreateTable`. The name of
    /// the table that should be dropped must be provided in the body of the action. The table is
    /// dropped for all nodes controlled by the manager and all data in the table, both locally on
    /// the nodes and in the remote object store, is deleted.
    /// * `TruncateTable`: Truncate a table previously created with `CreateTable`. The name of
    /// the table that should be truncated must be provided in the body of the action. The table is
    /// truncated for all nodes controlled by the manager and all data in the table, both locally on
    /// the nodes and in the remote object store, is deleted.
    /// * `RegisterNode`: Register either an edge or cloud node with the manager. The node is added
    /// to the cluster of nodes controlled by the manager and the key and object store used in the
    /// cluster is returned.
    /// * `RemoveNode`: Remove a node from the cluster of nodes controlled by the manager and
    /// kill the process running on the node. The specific node to remove is given through the
    /// uniquely identifying URL of the node.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> StdResult<Response<Self::DoActionStream>, Status> {
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
        } else if action.r#type == "CreateTable" {
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
                ValidStatement::CreateTable { name, schema } => {
                    self.check_if_table_exists(&name).await?;
                    self.save_and_create_cluster_tables(&name, &schema, sql)
                        .await?;
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
        } else if action.r#type == "DropTable" {
            // Extract the table name from the action body.
            let table_name = str::from_utf8(&action.body)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            info!("Received request to drop table '{}'.", table_name);

            // Drop the table from the metadata Delta Lake, the data Delta Lake, and from each
            // node controlled by the manager.
            self.drop_cluster_table(table_name).await?;

            // Confirm the table was dropped.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "TruncateTable" {
            // Extract the table name from the action body.
            let table_name = str::from_utf8(&action.body)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            info!("Received request to truncate table '{}'.", table_name);

            // Truncate the table in the metadata Delta Lake, the data Delta Lake, and from each
            // node controlled by the manager.
            self.truncate_cluster_table(table_name).await?;

            // Confirm the table was truncated.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "RegisterNode" {
            // Extract the node from the action body.
            let (url, offset_data) = arguments::decode_argument(&action.body)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let (mode, _offset_data) = arguments::decode_argument(offset_data)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            let server_mode = ServerMode::from_str(mode)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let node = Node::new(url.to_string(), server_mode.clone());

            // Use the cluster to register the node in memory. This returns an error if the node is
            // already registered.
            self.context
                .cluster
                .write()
                .await
                .register_node(node.clone())
                .map_err(|error| Status::internal(error.to_string()))?;

            // Use the metadata manager to persist the node to the metadata Delta Lake. Note that if
            // this fails, the metadata Delta Lake and the cluster will be out of sync until the
            // manager is restarted.
            self.context
                .remote_data_folder
                .metadata_manager
                .save_node(node)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // unwrap() is safe since the key cannot contain invalid characters.
            let mut response_body = arguments::encode_argument(self.context.key.to_str().unwrap());

            let mut connection_info = self.context.remote_data_folder.connection_info.clone();
            response_body.append(&mut connection_info);

            // Return the key for the manager and the connection info for the remote object store.
            Ok(Response::new(Box::pin(stream::once(async {
                Ok(FlightResult {
                    body: response_body.into(),
                })
            }))))
        } else if action.r#type == "RemoveNode" {
            let (url, _offset_data) = arguments::decode_argument(&action.body)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;

            // Remove the node with the given url from the metadata Delta Lake.
            self.context
                .remote_data_folder
                .metadata_manager
                .remove_node(url)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;

            // Remove the node with the given url from the cluster and kill it. Note that if this fails,
            // the cluster and metadata Delta Lake will be out of sync until the manager is restarted.
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
    ) -> StdResult<Response<Self::ListActionsStream>, Status> {
        let initialize_database_action = ActionType {
            r#type: "InitializeDatabase".to_owned(),
            description: "Return the SQL required to create all tables and models tables \
            currently in the manager's database schema."
                .to_owned(),
        };

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
            Ok(create_table_action),
            Ok(drop_table_action),
            Ok(truncate_table_action),
            Ok(register_node_action),
            Ok(remove_node_action),
        ]);

        Ok(Response::new(Box::pin(output)))
    }
}
