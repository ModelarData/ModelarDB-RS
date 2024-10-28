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

use std::collections::VecDeque;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::info;
use modelardb_types::types::ServerMode;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::Request;

use crate::error::{ModelarDbManagerError, Result};

/// A single ModelarDB server that is controlled by the manager. The node can either be an edge node
/// or a cloud node. A node cannot be another manager.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// Apache Arrow Flight URL for the node. This URL uniquely identifies the node.
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
    /// Queue of cloud nodes used to determine which cloud node should execute a query in
    /// a round-robin fashion.
    query_queue: VecDeque<Node>,
}

impl Cluster {
    pub fn new() -> Self {
        Self {
            nodes: vec![],
            query_queue: VecDeque::new(),
        }
    }

    /// Checks if the node is already registered and adds it to the current nodes if not. If it
    /// already exists, [`ModelarDbManagerError`] is returned.
    pub fn register_node(&mut self, node: Node) -> Result<()> {
        if self
            .nodes
            .iter()
            .any(|n| n.url.to_lowercase() == node.url.to_lowercase())
        {
            Err(ModelarDbManagerError::InvalidArgument(format!(
                "A node with the url `{}` is already registered.",
                node.url
            )))
        } else {
            // Also add it to the query queue if it is a cloud node.
            if node.mode == ServerMode::Cloud {
                self.query_queue.push_back(node.clone());
            }

            self.nodes.push(node);

            Ok(())
        }
    }

    /// Remove the node with a url matching `url` from the current nodes, flush the node, and
    /// finally kill the process running on the node. If no node with `url` exists,
    /// [`ModelarDbManagerError`] is returned.
    pub async fn remove_node(&mut self, url: &str, key: &MetadataValue<Ascii>) -> Result<()> {
        if self
            .nodes
            .iter()
            .any(|n| n.url.to_lowercase() == url.to_lowercase())
        {
            self.nodes.retain(|n| n.url != url);
            self.query_queue.retain(|n| n.url != url);

            // Flush the node and kill the process running on the node.
            let mut flight_client = FlightServiceClient::connect(url.to_owned()).await?;

            let action = Action {
                r#type: "KillNode".to_owned(),
                body: vec![].into(),
            };

            // Add the key to the request metadata to indicate that the request is from the manager.
            let mut request = Request::new(action);
            request.metadata_mut().insert("x-manager-key", key.clone());

            // TODO: Retry the request if the wrong error was returned.
            // Since the process is killed, the error from the request is ignored.
            let _ = flight_client.do_action(request).await;

            Ok(())
        } else {
            Err(ModelarDbManagerError::InvalidArgument(format!(
                "A node with the url `{url}` does not exist."
            )))
        }
    }

    /// Return the cloud node in the cluster that is currently most capable of running a query. If
    /// there are no cloud nodes in the cluster, return [`ModelarDbManagerError`].
    pub fn query_node(&mut self) -> Result<Node> {
        if let Some(query_node) = self.query_queue.pop_front() {
            // Add the cloud node back to the queue.
            self.query_queue.push_back(query_node.clone());

            Ok(query_node)
        } else {
            Err(ModelarDbManagerError::InvalidState(
                "There are no cloud nodes to execute the query in the cluster.".to_owned(),
            ))
        }
    }

    /// For each node in the cluster, use the `CreateTable` action to create the table given by
    /// `sql`. If the table was successfully created for each node, return [`Ok`], otherwise return
    /// [`ModelarDbManagerError`].
    pub async fn create_tables(
        &self,
        table_name: &str,
        sql: &str,
        key: &MetadataValue<Ascii>,
    ) -> Result<()> {
        let action = Action {
            r#type: "CreateTable".to_owned(),
            body: sql.to_owned().into(),
        };

        let mut create_table_futures: FuturesUnordered<_> = self
            .nodes
            .iter()
            .map(|node| self.connect_and_do_action(&node.url, action.clone(), key))
            .collect();

        // TODO: Fix issue where we return immediately if we encounter an error. If it is a
        //       connection error, we either need to retry later or remove the node.
        // Run the futures concurrently and log when the table has been created on each node.
        while let Some(result) = create_table_futures.next().await {
            info!(
                "Created table '{}' on node with url '{}'.",
                table_name, result?
            );
        }

        Ok(())
    }

    /// For each node in the cluster, use the `DropTable` action to drop the table given by
    /// `table_name`. If the table was successfully dropped for each node, return [`Ok`], otherwise
    /// return [`ModelarDbManagerError`].
    pub async fn drop_tables(&self, table_name: &str, key: &MetadataValue<Ascii>) -> Result<()> {
        let action = Action {
            r#type: "DropTable".to_owned(),
            body: table_name.to_owned().into(),
        };

        let mut drop_table_futures: FuturesUnordered<_> = self
            .nodes
            .iter()
            .map(|node| self.connect_and_do_action(&node.url, action.clone(), key))
            .collect();

        // Run the futures concurrently and log when the table has been dropped on each node.
        while let Some(result) = drop_table_futures.next().await {
            info!(
                "Dropped table '{}' on node with url '{}'.",
                table_name, result?
            );
        }

        Ok(())
    }

    /// Connect to the Apache Arrow flight client given by `url` and make a request to do `action`.
    /// If the action was successfully executed, return the url of the node, otherwise return
    /// [`ModelarDbManagerError`].
    async fn connect_and_do_action(
        &self,
        url: &str,
        action: Action,
        key: &MetadataValue<Ascii>,
    ) -> Result<String> {
        let mut flight_client = FlightServiceClient::connect(url.to_owned()).await?;

        // Add the key to the request metadata to indicate that the request is from the manager.
        let mut request = Request::new(action);
        request.metadata_mut().insert("x-manager-key", key.clone());

        flight_client.do_action(request).await?;

        Ok(url.to_owned())
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

    use uuid::Uuid;

    // Tests for Cluster.
    #[test]
    fn test_register_node() {
        let node = Node::new("localhost".to_owned(), ServerMode::Edge);
        let mut cluster = Cluster::new();

        assert!(cluster.register_node(node.clone()).is_ok());
        assert!(cluster.nodes.contains(&node));
        assert!(cluster.query_queue.is_empty());

        let cloud_node = Node::new("cloud".to_owned(), ServerMode::Cloud);

        assert!(cluster.register_node(cloud_node.clone()).is_ok());
        assert!(cluster.nodes.contains(&cloud_node));
        assert!(cluster.query_queue.contains(&cloud_node));
    }

    #[test]
    fn test_register_already_registered_node() {
        let node = Node::new("localhost".to_owned(), ServerMode::Edge);
        let mut cluster = Cluster::new();

        assert!(cluster.register_node(node.clone()).is_ok());
        assert!(cluster.register_node(node).is_err());
    }

    #[tokio::test]
    async fn test_remove_node_invalid_url() {
        let mut cluster = Cluster::new();
        assert!(cluster
            .remove_node("invalid_url", &Uuid::new_v4().to_string().parse().unwrap())
            .await
            .is_err());
    }

    #[test]
    fn test_query_node_round_robin() {
        let cloud_node_1 = Node::new("cloud_1".to_owned(), ServerMode::Cloud);
        let cloud_node_2 = Node::new("cloud_2".to_owned(), ServerMode::Cloud);
        let mut cluster = Cluster::new();

        assert!(cluster.register_node(cloud_node_1.clone()).is_ok());
        assert!(cluster.register_node(cloud_node_2.clone()).is_ok());

        assert_eq!(cluster.query_node().unwrap(), cloud_node_1);
        assert_eq!(cluster.query_node().unwrap(), cloud_node_2);
        assert_eq!(cluster.query_node().unwrap(), cloud_node_1);
        assert_eq!(cluster.query_node().unwrap(), cloud_node_2);
    }

    #[test]
    fn test_query_node_no_cloud_nodes() {
        let node = Node::new("localhost".to_owned(), ServerMode::Edge);
        let mut cluster = Cluster::new();

        assert!(cluster.register_node(node).is_ok());
        assert!(cluster.query_node().is_err());
    }
}
