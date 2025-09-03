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
use datafusion::catalog::TableProvider;
use modelardb_types::flight::protocol;
use modelardb_types::types::{Node, ServerMode};
use prost::Message;
use tokio::sync::RwLock;
use tonic::Request;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;

use crate::PORT;
use crate::context::Context;
use crate::error::{ModelarDbServerError, Result};

/// Manages metadata related to the manager and provides functionality for interacting with the manager.
#[derive(Clone, Debug)]
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
            FlightServiceClient::connect(manager_url.to_owned()).await?,
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

        // unwrap() is safe since the manager always has a remote storage configuration.
        Ok((
            Manager::new(manager_metadata.key),
            manager_metadata.storage_configuration.unwrap(),
        ))
    }

    /// Initialize the local database schema with the normal tables and time series tables from the
    /// managers database schema using the remote data folder. If the tables to create could not be
    /// retrieved from the remote data folder, or the tables could not be created,
    /// return [`ModelarDbServerError`].
    pub(crate) async fn retrieve_and_create_tables(&self, context: &Arc<Context>) -> Result<()> {
        let local_metadata_manager = &context
            .data_folders
            .local_data_folder
            .table_metadata_manager;

        let remote_data_folder = &context
            .data_folders
            .maybe_remote_data_folder
            .clone()
            .ok_or(ModelarDbServerError::InvalidState(
                "Remote data folder is missing.".to_owned(),
            ))?;
        let remote_metadata_manager = &remote_data_folder.table_metadata_manager;

        let local_table_names = local_metadata_manager.table_names().await?;
        let remote_table_names = remote_metadata_manager.table_names().await?;

        // Check that all the local tables exist in the cluster's database schema already.
        let invalid_node_tables: Vec<String> = local_table_names
            .iter()
            .filter(|table| !remote_table_names.contains(table))
            .cloned()
            .collect();

        if !invalid_node_tables.is_empty() {
            return Err(ModelarDbServerError::InvalidState(format!(
                "The following tables do not exist in the cluster's database schema: {invalid_node_tables:?}.",
            )));
        }

        // For each table that does not already exist locally, create the table.
        let missing_cluster_tables = remote_table_names
            .iter()
            .filter(|table| !local_table_names.contains(table));

        for table_name in missing_cluster_tables {
            if remote_metadata_manager.is_normal_table(table_name).await? {
                let delta_table = remote_data_folder
                    .delta_lake
                    .delta_table(table_name)
                    .await?;

                let schema = TableProvider::schema(&delta_table);
                context.create_normal_table(table_name, &schema).await?;
            } else {
                let time_series_table_metadata = remote_metadata_manager
                    .time_series_table_metadata_for_time_series_table(table_name)
                    .await?;

                context
                    .create_time_series_table(&time_series_table_metadata)
                    .await?;
            };
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

/// Partial equality is implemented so PartialEq can be derived for [`ClusterMode`](crate::ClusterMode).
/// It cannot be derived for [`Manager`] since both `flight_client` and `table_metadata_manager`
/// does not support equality comparisons.
impl PartialEq for Manager {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
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

        assert!(manager.validate_request(&request_metadata).is_err());
    }

    #[tokio::test]
    async fn test_validate_request_with_invalid_key() {
        let manager = create_manager();
        let mut request_metadata = MetadataMap::new();
        request_metadata.append("x-manager-key", Uuid::new_v4().to_string().parse().unwrap());

        assert!(manager.validate_request(&request_metadata).is_err());
    }

    fn create_manager() -> Manager {
        Manager::new(Uuid::new_v4().to_string())
    }
}
