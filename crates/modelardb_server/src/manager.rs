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

use std::str;
use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightData, FlightDescriptor};
use datafusion::arrow::array::{StringArray, UInt64Array};
use datafusion::arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use datafusion::arrow::record_batch::RecordBatch;
use futures::stream;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::metadata::compressed_file::CompressedFile;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::metadata::TableMetadataManager;
use modelardb_common::schemas::TAG_METADATA_SCHEMA;
use modelardb_common::types::ServerMode;
use modelardb_common::{arguments, metadata};
use object_store::ObjectStore;
use sqlx::Postgres;
use tokio::sync::RwLock;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;
use tonic::Request;

use crate::context::Context;
use crate::PORT;

/// Manages metadata related to the manager and provides functionality for interacting with the manager.
#[derive(Clone, Debug)]
pub struct Manager {
    /// Flight client that is connected to the Apache Arrow Flight server of the manager.
    flight_client: Arc<RwLock<FlightServiceClient<Channel>>>,
    /// Key received from the manager when registering, used to validate future requests that are
    /// only allowed to come from the manager.
    key: String,
    /// Metadata for the tables and model tables in the remote data folder.
    pub(crate) table_metadata_manager: Arc<TableMetadataManager<Postgres>>,
}

impl Manager {
    pub fn new(
        flight_client: Arc<RwLock<FlightServiceClient<Channel>>>,
        key: String,
        table_metadata_manager: Arc<TableMetadataManager<Postgres>>,
    ) -> Self {
        Self {
            flight_client,
            key,
            table_metadata_manager,
        }
    }

    /// Register the server as a node in the cluster and retrieve the key and connection information
    /// from the manager. If the key and connection information could not be retrieved or a
    /// connection to the metadata database or remote object store could not be established,
    /// [`ModelarDbError`] is returned.
    pub(crate) async fn register_node(
        manager_url: &str,
        server_mode: ServerMode,
    ) -> Result<(Self, Arc<dyn ObjectStore>), ModelarDbError> {
        let flight_client = Arc::new(RwLock::new(
            FlightServiceClient::connect(manager_url.to_owned())
                .await
                .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?,
        ));

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

        let message = do_action_and_extract_result(flight_client.clone(), action).await?;

        // Extract the key and connection information for the metadata database and remote object
        // store from the response.
        let (key, offset_data) = arguments::decode_argument(&message.body)
            .map_err(|error| ModelarDbError::ImplementationError(error.to_string()))?;

        // Use the connection information to create a metadata manager for the remote metadata database.
        let (connection, offset_data) = arguments::parse_postgres_arguments(offset_data)
            .await
            .map_err(|error| ModelarDbError::ImplementationError(error.to_string()))?;

        let table_metadata_manager = metadata::new_table_metadata_manager(Postgres, connection);

        let manager = Manager::new(
            flight_client,
            key.to_owned(),
            Arc::new(table_metadata_manager),
        );

        let remote_object_store = arguments::parse_object_store_arguments(offset_data)
            .await
            .map_err(|error| ModelarDbError::ImplementationError(error.to_string()))?;

        Ok((manager, remote_object_store))
    }

    /// Initialize the local database schema with the tables and model tables from the managers
    /// database schema. If the tables to create could not be retrieved from the manager, or the
    /// tables could not be created, return [`ModelarDbError`].
    pub(crate) async fn retrieve_and_create_tables(
        &self,
        context: &Arc<Context>,
    ) -> Result<(), ModelarDbError> {
        let existing_tables = context.default_database_schema()?.table_names();

        // Add the already existing tables to the action request.
        let action = Action {
            r#type: "InitializeDatabase".to_owned(),
            body: existing_tables.join(",").into_bytes().into(),
        };

        let message = do_action_and_extract_result(self.flight_client.clone(), action).await?;

        // Extract the SQL for the tables that need to be created from the response.
        let table_sql_queries = str::from_utf8(&message.body)
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?
            .split(';')
            .filter(|sql| !sql.is_empty());

        // For each table to create, register and save the table in the metadata database.
        for sql in table_sql_queries {
            context.parse_and_create_table(sql, context).await?;
        }

        Ok(())
    }

    /// Convert the compressed file metadata to a record batch and transfer it to the
    /// `model_table_name_compressed_files` metadata table in the manager. If the metadata could
    /// not be transferred, return [`ModelarDbError`].
    pub async fn transfer_compressed_file_metadata(
        &self,
        model_table_name: &str,
        column_index: usize,
        compressed_file: CompressedFile,
    ) -> Result<(), ModelarDbError> {
        let metadata = compressed_file.to_record_batch(model_table_name, column_index);
        self.transfer_metadata(metadata, &format!("{model_table_name}_compressed_files"))
            .await
    }

    /// Convert the tag metadata to a record batch and transfer it to the `model_table_name_tags`
    /// metadata table in the manager. If the metadata could not be transferred,
    /// return [`ModelarDbError`].
    pub async fn transfer_tag_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
        tag_hash: u64,
        tag_values: &[String],
    ) -> Result<(), ModelarDbError> {
        // Convert the tag columns and tag values to strings so they can be inserted into a record batch.
        let tag_columns: String = model_table_metadata
            .tag_column_indices
            .iter()
            .map(|index| model_table_metadata.schema.field(*index).name().clone())
            .collect::<Vec<String>>()
            .join(",");

        let values = tag_values
            .iter()
            .map(|value| format!("'{value}'"))
            .collect::<Vec<String>>()
            .join(",");

        // unwrap() is safe since the columns match the schema and all columns are of the same length.
        let metadata = RecordBatch::try_new(
            TAG_METADATA_SCHEMA.0.clone(),
            vec![
                Arc::new(StringArray::from(vec![model_table_metadata.name.clone()])),
                Arc::new(UInt64Array::from(vec![tag_hash])),
                Arc::new(StringArray::from(vec![tag_columns])),
                Arc::new(StringArray::from(vec![values])),
            ],
        )
        .unwrap();

        self.transfer_metadata(metadata, &format!("{}_tags", model_table_metadata.name))
            .await
    }

    /// Transfer `metadata` to the `metadata_table_name` table in the managers metadata
    /// database. If `metadata` could not be transferred, return [`ModelarDbError`].
    async fn transfer_metadata(
        &self,
        metadata: RecordBatch,
        metadata_table_name: &str,
    ) -> Result<(), ModelarDbError> {
        // Put the table name in the flight descriptor of the first flight data in the stream.
        let flight_descriptor = FlightDescriptor::new_path(vec![metadata_table_name.to_owned()]);
        let mut flight_data = vec![FlightData::new().with_descriptor(flight_descriptor)];

        // Convert the metadata in the record batch to the Arrow IPC format so it can be transferred.
        let data_generator = IpcDataGenerator::default();
        let writer_options = IpcWriteOptions::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);

        let (_encoded_dictionaries, encoded_batch) = data_generator
            .encoded_batch(&metadata, &mut dictionary_tracker, &writer_options)
            .unwrap();

        flight_data.push(encoded_batch.into());

        // Stream the metadata to the Apache Arrow Flight server of the manager.
        self.flight_client
            .write()
            .await
            .do_put(stream::iter(flight_data))
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

        Ok(())
    }

    /// If `action_type` is `CommandStatementUpdate`, `UpdateRemoteObjectStore`, or `KillEdge`,
    /// check that the request actually came from the manager. If the request is valid, return
    /// [`Ok`], otherwise return [`ModelarDbError`].
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
                return Err(ModelarDbError::ClusterError(format!(
                    "Manager key '{:?}' is invalid.",
                    request_key
                )));
            }
        }

        Ok(())
    }
}

/// Execute `action` using `flight_client` and extract the message inside the response. If `action`
/// could not be executed or the response is invalid or empty, return [`ModelarDbError`].
async fn do_action_and_extract_result(
    flight_client: Arc<RwLock<FlightServiceClient<Channel>>>,
    action: Action,
) -> Result<arrow_flight::Result, ModelarDbError> {
    let response = flight_client
        .write()
        .await
        .do_action(Request::new(action.clone()))
        .await
        .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

    // Extract the message from the response.
    let maybe_message = response
        .into_inner()
        .message()
        .await
        .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

    // Handle that the response is potentially empty.
    maybe_message.ok_or_else(|| {
        ModelarDbError::ImplementationError(format!(
            "Response for action request '{}' is empty.",
            action.r#type
        ))
    })
}

impl PartialEq for Manager {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_common::test;
    use uuid::Uuid;

    const UNRESTRICTED_ACTIONS: [&str; 5] = [
        "FlushMemory",
        "FlushEdge",
        "CollectMetrics",
        "GetConfiguration",
        "UpdateConfiguration",
    ];

    const RESTRICTED_ACTIONS: [&str; 3] = [
        "CommandStatementUpdate",
        "UpdateRemoteObjectStore",
        "KillEdge",
    ];

    // Tests for validate_action_request().
    #[tokio::test]
    async fn test_validate_unrestricted_action_request() {
        let manager = create_manager();
        let request_metadata = MetadataMap::new();

        for action_type in UNRESTRICTED_ACTIONS {
            assert!(manager
                .validate_action_request(action_type, &request_metadata)
                .is_ok());
        }
    }

    #[tokio::test]
    async fn test_validate_restricted_action_request() {
        let manager = create_manager();
        let mut request_metadata = MetadataMap::new();
        request_metadata.append("x-manager-key", manager.key.parse().unwrap());

        for action_type in RESTRICTED_ACTIONS {
            assert!(manager
                .validate_action_request(action_type, &request_metadata)
                .is_ok());
        }
    }

    #[tokio::test]
    async fn test_validate_restricted_action_request_without_key() {
        let manager = create_manager();
        let request_metadata = MetadataMap::new();

        for action_type in RESTRICTED_ACTIONS {
            assert!(manager
                .validate_action_request(action_type, &request_metadata)
                .is_err());
        }
    }

    #[tokio::test]
    async fn test_validate_restricted_action_request_with_invalid_key() {
        let manager = create_manager();
        let mut request_metadata = MetadataMap::new();
        request_metadata.append("x-manager-key", Uuid::new_v4().to_string().parse().unwrap());

        for action_type in RESTRICTED_ACTIONS {
            assert!(manager
                .validate_action_request(action_type, &request_metadata)
                .is_err());
        }
    }

    fn create_manager() -> Manager {
        let (lazy_metadata_manager, lazy_flight_client) = test::lazy_connections();

        Manager::new(
            Arc::new(RwLock::new(lazy_flight_client)),
            Uuid::new_v4().to_string(),
            Arc::new(lazy_metadata_manager),
        )
    }
}
