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

//! Management of the cluster of nodes that are currently controlled by the manager.

use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::info;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::types::ServerMode;
use tonic::codegen::Bytes;
use tonic::Request;
use uuid::Uuid;

/// A single ModelarDB server that is controlled by the manager. The node can either be an edge node
/// or a cloud node. A node cannot be another manager.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// Arrow Flight URL for the node. This URL uniquely identifies the node.
    pub url: String,
    /// The mode the node was started in.
    pub mode: ServerMode,
}

impl Node {
    pub fn new(url: String, mode: ServerMode) -> Self {
        Self { url, mode }
    }
}

/// Stores the currently managed nodes in the cluster and allows for performing operations that need
/// to be applied to every single node in the cluster.
pub struct Cluster {
    /// The nodes that are currently managed by the cluster.
    nodes: Vec<Node>,
}

impl Cluster {
    pub fn new() -> Self {
        Self { nodes: vec![] }
    }

    /// Checks if the node is already registered and adds it to the current nodes if not. If it
    /// already exists, [`ConfigurationError`](ModelarDbError::ConfigurationError) is returned.
    pub fn register_node(&mut self, node: Node) -> Result<(), ModelarDbError> {
        if self.nodes.iter().any(|n| n.url == node.url) {
            Err(ModelarDbError::ConfigurationError(format!(
                "A node with the url `{}` is already registered.",
                node.url
            )))
        } else {
            self.nodes.push(node);
            Ok(())
        }
    }

    /// Remove the node with an url matching `url` from the current nodes, flush the node, and
    /// finally kill the process running on the node. If no node with `url` exists,
    /// [`ConfigurationError`](ModelarDbError::ConfigurationError) is returned.
    pub async fn remove_node(&mut self, url: &str, key: Uuid) -> Result<(), ModelarDbError> {
        if self.nodes.iter().any(|n| n.url == url) {
            self.nodes.retain(|n| n.url != url);

            // Flush the node and kill the process running on the node.
            let mut flight_client = FlightServiceClient::connect(format!("grpc://{url}"))
                .await
                .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

            let action = Action {
                r#type: "KillEdge".to_owned(),
                body: vec![].into(),
            };

            // Add the key to the request metadata to authenticate that the request is from the manager.
            // unwrap() is safe since a UUID cannot contain invalid characters.
            let mut request = Request::new(action);

            request
                .metadata_mut()
                .insert("x-manager-key", key.to_string().parse().unwrap());

            // TODO: Retry the request if the wrong error was returned.
            // Since the process is killed, the error from the request is ignored.
            let _ = flight_client.do_action(request).await;

            Ok(())
        } else {
            Err(ModelarDbError::ConfigurationError(format!(
                "A node with the url `{url}` does not exist."
            )))
        }
    }

    /// For each node in the cluster, use the `CommandStatementUpdate` action to create the table
    /// given by `sql`. If the table was successfully created for each node, return
    /// [`Ok`], otherwise return [`ClusterError`](ModelarDbError::ClusterError).
    pub async fn create_tables(
        &self,
        table_name: &str,
        sql: Bytes,
        key: Uuid,
    ) -> Result<(), ModelarDbError> {
        let mut create_table_futures = FuturesUnordered::new();

        for node in &self.nodes {
            let future =
                tryhard::retry_fn(|| self.create_table(node.url.clone(), sql.clone(), key))
                    .retries(10)
                    .exponential_backoff(Duration::from_secs(1));

            create_table_futures.push(future);
        }

        while let Some(result) = create_table_futures.next().await {
            info!(
                "Created table '{}' on node with url '{}'.",
                table_name, result?
            );
        }

        Ok(())
    }

    /// Connect to the Apache Arrow flight client given by `url` and use the `CommandStatementUpdate`
    /// action to create the table given by `sql`. If the table was successfully created, return
    /// the url of the node, otherwise return [`ClusterError`](ModelarDbError::ClusterError).
    async fn create_table(
        &self,
        url: String,
        sql: Bytes,
        key: Uuid,
    ) -> Result<String, ModelarDbError> {
        let mut flight_client = FlightServiceClient::connect(format!("grpc://{url}"))
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

        let action = Action {
            r#type: "CommandStatementUpdate".to_owned(),
            body: sql,
        };

        // Add the key to the request metadata to authenticate that the request is from the manager.
        // unwrap() is safe since a UUID cannot contain invalid characters.
        let mut request = Request::new(action);

        request
            .metadata_mut()
            .insert("x-manager-key", key.to_string().parse().unwrap());

        flight_client
            .do_action(request)
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

        Ok(url)
    }
}

impl Default for Cluster {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // Tests for Cluster.
    #[test]
    fn test_register_node() {
        let node = Node::new("localhost".to_string(), ServerMode::Edge);
        let mut cluster = Cluster::new();

        assert!(cluster.register_node(node.clone()).is_ok());
        assert!(cluster.nodes.contains(&node));
    }

    #[test]
    fn test_register_already_registered_node() {
        let node = Node::new("localhost".to_string(), ServerMode::Edge);
        let mut cluster = Cluster::new();

        assert!(cluster.register_node(node.clone()).is_ok());
        assert!(cluster.register_node(node).is_err());
    }

    #[tokio::test]
    async fn test_remove_node_invalid_url() {
        let mut cluster = Cluster::new();
        assert!(cluster
            .remove_node("invalid_url", Uuid::new_v4())
            .await
            .is_err());
    }
}
