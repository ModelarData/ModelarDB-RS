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

//! Interface to connect to and interact with the manager, used if the server is started with a
//! manager and needs to interact with it to initialize the metadata database and transfer metadata.

use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightData, FlightDescriptor};
use bytes::Bytes;
use datafusion::arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use datafusion::arrow::record_batch::RecordBatch;
use futures::stream;
use modelardb_common::arguments;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::types::ServerMode;
use object_store::ObjectStore;
use tonic::metadata::MetadataMap;
use tonic::Request;

use crate::context::Context;
use crate::PORT;

/// Manages metadata related to the manager and provides functionality for interacting with the manager.
#[derive(Clone)]
pub struct Manager {
    /// The gRPC URL of the manager's Apache Arrow Flight server.
    pub(crate) url: String,
    /// Key received from the manager when registering, used to validate future requests that are
    /// only allowed to come from the manager.
    pub(crate) key: String,
}

impl Manager {
    /// Register the server as a node in the cluster and retrieve the key and remote object store
    /// connection information from the manager. If the key and connection information could not be
    /// retrieved or a connection to the remote object store could not be established,
    /// [`ModelarDbError`] is returned.
    pub(crate) async fn register_node(
        manager_url: &str,
        server_mode: ServerMode,
    ) -> Result<(Self, Arc<dyn ObjectStore>), ModelarDbError> {
        let mut flight_client = FlightServiceClient::connect(manager_url.to_owned())
            .await
            .map_err(|error| {
                ModelarDbError::ClusterError(format!("Could not connect to manager: {error}"))
            })?;

        // Add the url and mode of the server to the action request.
        let localhost_with_port = "grpc://127.0.0.1:".to_owned() + &PORT.to_string();
        let mut body = arguments::encode_argument(localhost_with_port.as_str());
        body.append(&mut arguments::encode_argument(
            server_mode.to_string().as_str(),
        ));

        let action = Action {
            r#type: "RegisterNode".to_owned(),
            body: body.into(),
        };

        // Extract the connection information for the remote object store from the response.
        let maybe_response = flight_client
            .do_action(Request::new(action))
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?
            .into_inner()
            .message()
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

        if let Some(response) = maybe_response {
            let (key, offset_data) = arguments::decode_argument(&response.body)
                .map_err(|error| ModelarDbError::ImplementationError(error.to_string()))?;

            Ok((
                Self {
                    url: manager_url.to_owned(),
                    key: key.to_owned(),
                },
                arguments::parse_object_store_arguments(offset_data)
                    .await
                    .map_err(|error| ModelarDbError::ImplementationError(error.to_string()))?,
            ))
        } else {
            Err(ModelarDbError::ImplementationError(
                "Response for request to register the node is empty.".to_owned(),
            ))
        }
    }

    /// Initialize the local database schema with the tables and model tables from the managers
    /// database schema. If the tables to create could not be retrieved from the manager, or the
    /// tables could not be created, return [`ModelarDbError`].
    pub(crate) async fn retrieve_and_create_tables(
        &self,
        context: &Arc<Context>,
    ) -> Result<(), ModelarDbError> {
        let existing_tables = context.default_database_schema()?.table_names();

        // Retrieve the tables to create from the manager.
        let mut flight_client = FlightServiceClient::connect(self.url.clone())
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

        // Add the already existing tables to the action request.
        let action = Action {
            r#type: "InitializeDatabase".to_owned(),
            body: existing_tables.join(",").into_bytes().into(),
        };

        // Extract the SQL for the tables that need to be created from the response.
        let response = flight_client
            .do_action(Request::new(action))
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

        let maybe_message = response
            .into_inner()
            .message()
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

        if let Some(message) = maybe_message {
            let table_sql_queries = std::str::from_utf8(&message.body)
                .map_err(|error| ModelarDbError::TableError(error.to_string()))?
                .split(';')
                .filter(|sql| !sql.is_empty());

            // For each table to create, register and save the table in the metadata database.
            for sql in table_sql_queries {
                context.parse_and_create_table(sql, context).await?;
            }

            Ok(())
        } else {
            Err(ModelarDbError::ImplementationError(
                "Response for request to initialize database is empty.".to_owned(),
            ))
        }
    }

    /// Transfer `metadata` to the `metadata_table_name` table in the managers metadata
    /// database. If `metadata` could not be transferred, return [`ModelarDbError`].
    pub async fn transfer_metadata(
        &self,
        metadata: RecordBatch,
        metadata_table_name: &str,
    ) -> Result<(), ModelarDbError> {
        let mut flight_client = FlightServiceClient::connect(self.url.clone())
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

        // Put the table name in the flight descriptor of the first flight data in the stream.
        let flight_descriptor = FlightDescriptor::new_path(vec![metadata_table_name.to_owned()]);
        let mut flight_data = vec![FlightData {
            flight_descriptor: Some(flight_descriptor),
            data_header: Bytes::new(),
            app_metadata: Bytes::new(),
            data_body: Bytes::new(),
        }];

        // Write the metadata in the record batch into Arrow IPC format so it can be transferred.
        let data_generator = IpcDataGenerator::default();
        let writer_options = IpcWriteOptions::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);

        let (_encoded_dictionaries, encoded_batch) = data_generator
            .encoded_batch(&metadata, &mut dictionary_tracker, &writer_options)
            .unwrap();

        flight_data.push(encoded_batch.into());

        // Stream the metadata to the Apache Arrow Flight client of the manager.
        let flight_data_stream = stream::iter(flight_data);
        flight_client
            .do_put(flight_data_stream)
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

        Ok(())
    }

    /// If the requested action is restricted to only be called by the manager, check that the
    /// request actually came from the manager. If the request is valid, return [`Ok`], otherwise
    /// return [`ModelarDbError`].
    pub fn validate_action_request(
        &self,
        action_type: &str,
        metadata: &MetadataMap,
    ) -> Result<(), ModelarDbError> {
        // If the server is started with a manager, these actions require a manager key.
        let restricted_actions = [
            "CommandStatementUpdate",
            "UpdateRemoteObjectStore",
            "KillEdge",
        ];

        if restricted_actions.iter().any(|&a| a == action_type) {
            let request_key = metadata
                .get("x-manager-key")
                .ok_or(ModelarDbError::ClusterError(
                    "Missing manager key.".to_owned(),
                ))?;

            if &self.key != request_key {
                return Err(ModelarDbError::ClusterError(
                    "Manager key is invalid.".to_owned(),
                ));
            }
        }

        Ok(())
    }
}
