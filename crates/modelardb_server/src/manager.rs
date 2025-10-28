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
//! manager and needs to interact with it to initialize the metadata Delta Lake.

use std::sync::Arc;
use std::{env, str};

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Result as FlightResult};
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::TableProvider;
use modelardb_storage::data_folder::DataFolder;
use modelardb_types::flight::protocol;
use modelardb_types::types::{Node, ServerMode, TimeSeriesTableMetadata};
use prost::Message;
use tokio::sync::RwLock;
use tonic::Request;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;

use crate::PORT;
use crate::context::Context;
use crate::error::{ModelarDbServerError, Result};

/// Manages metadata related to the manager and provides functionality for interacting with the manager.
#[derive(Clone, Debug, PartialEq)]
pub struct Manager {
    /// Key received from the manager when registering, used to validate future requests that are
    /// only allowed to come from the manager.
    key: String,
}

impl Manager {
    pub fn new(key: String) -> Self {
        Self { key }
    }

    /// Register the server as a node in the cluster and retrieve the key and connection information
    /// from the manager. If the key and connection information could not be retrieved,
    /// [`ModelarDbServerError`] is returned.
    pub(crate) async fn register_node(
        manager_url: &str,
        server_mode: ServerMode,
    ) -> Result<(Self, protocol::manager_metadata::StorageConfiguration)> {
        let flight_client = Arc::new(RwLock::new(
            FlightServiceClient::connect(manager_url.to_owned())
                .await
                .map_err(|error| {
                    ModelarDbServerError::InvalidArgument(format!(
                        "Could not connect to manager at '{manager_url}': {error}",
                    ))
                })?,
        ));

        let ip_address = env::var("MODELARDBD_IP_ADDRESS").unwrap_or("127.0.0.1".to_string());
        let url_with_port = format!("grpc://{ip_address}:{}", &PORT.to_string());

        // Add the url and mode of the server to the action request.
        let node = Node::new(url_with_port, server_mode);
        let node_metadata = modelardb_types::flight::encode_node(&node)?;

        let action = Action {
            r#type: "RegisterNode".to_owned(),
            body: node_metadata.encode_to_vec().into(),
        };

        let message = do_action_and_extract_result(&flight_client, action).await?;

        // Extract the key and the storage configuration for the remote object store from the response.
        let manager_metadata = protocol::ManagerMetadata::decode(message.body)?;

        Ok((
            Manager::new(manager_metadata.key),
            manager_metadata
                .storage_configuration
                .expect("The manager should have a remote storage configuration."),
        ))
    }

    /// Initialize the local database schema with the normal tables and time series tables from the
    /// manager's database schema using the remote data folder. If the tables to create could not be
    /// retrieved from the remote data folder, or the tables could not be created,
    /// return [`ModelarDbServerError`].
    pub(crate) async fn retrieve_and_create_tables(&self, context: &Arc<Context>) -> Result<()> {
        let local_data_folder = &context.data_folders.local_data_folder;

        let remote_data_folder = &context
            .data_folders
            .maybe_remote_data_folder
            .clone()
            .ok_or(ModelarDbServerError::InvalidState(
                "Remote data folder is missing.".to_owned(),
            ))?;

        validate_local_tables_exist_remotely(local_data_folder, remote_data_folder).await?;

        // Validate that all tables that are in both the local and remote data folder are identical.
        let missing_normal_tables =
            validate_normal_tables(local_data_folder, remote_data_folder).await?;

        let missing_time_series_tables =
            validate_time_series_tables(local_data_folder, remote_data_folder).await?;

        // For each table that does not already exist locally, create the table.
        for (table_name, schema) in missing_normal_tables {
            context.create_normal_table(&table_name, &schema).await?;
        }

        for time_series_table_metadata in missing_time_series_tables {
            context
                .create_time_series_table(&time_series_table_metadata)
                .await?;
        }

        Ok(())
    }

    /// Validate the request by checking that the key in the request metadata matches the key of the
    /// manager. If the request is valid, return [`Ok`], otherwise return [`ModelarDbServerError`].
    pub fn validate_request(&self, request_metadata: &MetadataMap) -> Result<()> {
        let request_key =
            request_metadata
                .get("x-manager-key")
                .ok_or(ModelarDbServerError::InvalidState(
                    "Missing manager key.".to_owned(),
                ))?;

        if &self.key != request_key {
            Err(ModelarDbServerError::InvalidState(format!(
                "Manager key '{request_key:?}' is invalid.",
            )))
        } else {
            Ok(())
        }
    }
}

/// Execute `action` using `flight_client` and extract the message inside the response. If `action`
/// could not be executed or the response is invalid or empty, return [`ModelarDbServerError`].
async fn do_action_and_extract_result(
    flight_client: &RwLock<FlightServiceClient<Channel>>,
    action: Action,
) -> Result<FlightResult> {
    let response = flight_client
        .write()
        .await
        .do_action(Request::new(action.clone()))
        .await?;

    // Extract the message from the response.
    let maybe_message = response.into_inner().message().await?;

    // Handle that the response is potentially empty.
    maybe_message.ok_or_else(|| {
        ModelarDbServerError::InvalidArgument(format!(
            "Response for action request '{}' is empty.",
            action.r#type
        ))
    })
}

/// Validate that all tables in the local data folder exist in the remote data folder. If any table
/// does not exist in the remote data folder, return [`ModelarDbServerError`].
async fn validate_local_tables_exist_remotely(
    local_data_folder: &DataFolder,
    remote_data_folder: &DataFolder,
) -> Result<()> {
    let local_table_names = local_data_folder.table_names().await?;
    let remote_table_names = remote_data_folder.table_names().await?;

    let invalid_tables: Vec<String> = local_table_names
        .iter()
        .filter(|table| !remote_table_names.contains(table))
        .cloned()
        .collect();

    if !invalid_tables.is_empty() {
        return Err(ModelarDbServerError::InvalidState(format!(
            "The following tables do not exist in the remote data folder: {}.",
            invalid_tables.join(", ")
        )));
    }

    Ok(())
}

/// For each normal table in the remote data folder, if the table also exists in the local data
/// folder, validate that the schemas are identical. If the schemas are not identical, return
/// [`ModelarDbServerError`]. Return a vector containing the name and schema of each normal table
/// that is in the remote data folder but not in the local data folder.
async fn validate_normal_tables(
    local_data_folder: &DataFolder,
    remote_data_folder: &DataFolder,
) -> Result<Vec<(String, Arc<Schema>)>> {
    let mut missing_normal_tables = vec![];

    let remote_normal_tables = remote_data_folder.normal_table_names().await?;

    for table_name in remote_normal_tables {
        let remote_schema = normal_table_schema(remote_data_folder, &table_name).await?;

        if let Ok(local_schema) = normal_table_schema(local_data_folder, &table_name).await {
            if remote_schema != local_schema {
                return Err(ModelarDbServerError::InvalidState(format!(
                    "The normal table '{table_name}' has a different schema in the local data \
                    folder compared to the remote data folder.",
                )));
            }
        } else {
            missing_normal_tables.push((table_name, remote_schema));
        }
    }

    Ok(missing_normal_tables)
}

/// Retrieve the schema of a normal table from the Delta Lake in the data folder. If the table does
/// not exist, or the schema could not be retrieved, return [`ModelarDbServerError`].
async fn normal_table_schema(data_folder: &DataFolder, table_name: &str) -> Result<Arc<Schema>> {
    let delta_table = data_folder.delta_table(table_name).await?;
    Ok(TableProvider::schema(&delta_table))
}

/// For each time series table in the remote data folder, if the table also exists in the local
/// data folder, validate that the metadata is identical. If the metadata is not identical, return
/// [`ModelarDbServerError`]. Return a vector containing the metadata of each time series table
/// that is in the remote data folder but not in the local data folder.
async fn validate_time_series_tables(
    local_data_folder: &DataFolder,
    remote_data_folder: &DataFolder,
) -> Result<Vec<TimeSeriesTableMetadata>> {
    let mut missing_time_series_tables = vec![];

    let remote_time_series_tables = remote_data_folder.time_series_table_names().await?;

    for table_name in remote_time_series_tables {
        let remote_metadata = remote_data_folder
            .time_series_table_metadata_for_time_series_table(&table_name)
            .await?;

        if let Ok(local_metadata) = local_data_folder
            .time_series_table_metadata_for_time_series_table(&table_name)
            .await
        {
            if remote_metadata != local_metadata {
                return Err(ModelarDbServerError::InvalidState(format!(
                    "The time series table '{table_name}' has different metadata in the local data \
                    folder compared to the remote data folder.",
                )));
            }
        } else {
            missing_time_series_tables.push(remote_metadata);
        }
    }

    Ok(missing_time_series_tables)
}

#[cfg(test)]
mod tests {
    use super::*;

    use uuid::Uuid;

    // Tests for validate_request().
    #[tokio::test]
    async fn test_validate_request() {
        let manager = create_manager();
        let mut request_metadata = MetadataMap::new();
        request_metadata.append("x-manager-key", manager.key.parse().unwrap());

        assert!(manager.validate_request(&request_metadata).is_ok());
    }

    #[tokio::test]
    async fn test_validate_request_without_key() {
        let manager = create_manager();
        let request_metadata = MetadataMap::new();

        let result = manager.validate_request(&request_metadata);

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid State Error: Missing manager key."
        );
    }

    #[tokio::test]
    async fn test_validate_request_with_invalid_key() {
        let manager = create_manager();
        let mut request_metadata = MetadataMap::new();

        let key = Uuid::new_v4().to_string();
        request_metadata.append("x-manager-key", key.parse().unwrap());

        let result = manager.validate_request(&request_metadata);

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid State Error: Manager key '\"{key}\"' is invalid.")
        );
    }

    fn create_manager() -> Manager {
        Manager::new(Uuid::new_v4().to_string())
    }
}
