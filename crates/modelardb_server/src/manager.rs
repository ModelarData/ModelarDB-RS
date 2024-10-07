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
use arrow_flight::Action;
use modelardb_common::arguments;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::types::ServerMode;
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
}

impl Manager {
    pub fn new(flight_client: Arc<RwLock<FlightServiceClient<Channel>>>, key: String) -> Self {
        Self { flight_client, key }
    }

    /// Register the server as a node in the cluster and retrieve the key and connection information
    /// from the manager. If the key and connection information could not be retrieved,
    /// [`ModelarDbError`] is returned.
    pub(crate) async fn register_node(
        manager_url: &str,
        server_mode: ServerMode,
    ) -> Result<(Self, Vec<u8>), ModelarDbError> {
        let flight_client = Arc::new(RwLock::new(
            FlightServiceClient::connect(manager_url.to_owned())
                .await
                .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?,
        ));

        let ip_address = env::var("MODELARDBD_IP_ADDRESS").unwrap_or("127.0.0.1".to_string());

        // Add the url and mode of the server to the action request.
        let url_with_port = format!("grpc://{ip_address}:{}", &PORT.to_string());
        let mut body = arguments::encode_argument(url_with_port.as_str());
        body.append(&mut arguments::encode_argument(
            server_mode.to_string().as_str(),
        ));

        let action = Action {
            r#type: "RegisterNode".to_owned(),
            body: body.into(),
        };

        let message = do_action_and_extract_result(&flight_client, action).await?;

        // Extract the key and the connection information for the remote object store from the response.
        let (key, offset_data) = arguments::decode_argument(&message.body)
            .map_err(|error| ModelarDbError::ImplementationError(error.to_string()))?;

        Ok((
            Manager::new(flight_client, key.to_owned()),
            offset_data.into(),
        ))
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

        let message = do_action_and_extract_result(&self.flight_client, action).await?;

        // Extract the SQL for the tables that need to be created from the response.
        let table_sql_queries = str::from_utf8(&message.body)
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?
            .split(';')
            .filter(|sql| !sql.is_empty());

        // For each table to create, register and save the table in the metadata Delta Lake.
        for sql in table_sql_queries {
            context.parse_and_create_table(sql).await?;
        }

        Ok(())
    }

    /// If `action_type` is `CommandStatementUpdate` or `KillNode`, check that the request actually
    /// came from the manager. If the request is valid, return [`Ok`], otherwise return
    /// [`ModelarDbError`].
    pub fn validate_action_request(
        &self,
        action_type: &str,
        metadata: &MetadataMap,
    ) -> Result<(), ModelarDbError> {
        // If the server is started with a manager, these actions require a manager key.
        let restricted_actions = ["CommandStatementUpdate", "KillNode"];

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
    flight_client: &RwLock<FlightServiceClient<Channel>>,
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

    const UNRESTRICTED_ACTIONS: [&str; 5] = [
        "FlushMemory",
        "FlushNode",
        "CollectMetrics",
        "GetConfiguration",
        "UpdateConfiguration",
    ];

    const RESTRICTED_ACTIONS: [&str; 2] = ["CommandStatementUpdate", "KillNode"];

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
        let channel = Channel::builder("grpc://server:9999".parse().unwrap()).connect_lazy();
        let lazy_flight_client = FlightServiceClient::new(channel);

        Manager::new(
            Arc::new(RwLock::new(lazy_flight_client)),
            Uuid::new_v4().to_string(),
        )
    }
}
