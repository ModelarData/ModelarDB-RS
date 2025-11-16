/* Copyright 2025 The ModelarDB Contributors
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

//! Functionality to perform operations on every node in the cluster.

use std::str::FromStr;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Ticket};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log::info;
use modelardb_storage::data_folder::DataFolder;
use modelardb_storage::data_folder::cluster::ClusterMetadata;
use modelardb_types::types::Node;
use rand::rng;
use rand::seq::IteratorRandom;
use tonic::Request;
use tonic::metadata::{Ascii, MetadataValue};

use crate::error::{ModelarDbServerError, Result};

/// Stores the currently managed nodes in the cluster and allows for performing operations that need
/// to be applied to every single node in the cluster.
pub struct Cluster {
    /// Key identifying the cluster. The key is used to validate communication within the cluster
    /// between nodes.
    key: MetadataValue<Ascii>,
    /// The remote data folder that each node in the cluster should be synchronized with.
    /// When a table is created, dropped, vacuumed, or truncated, it is done in the
    /// remote data folder first.
    remote_data_folder: DataFolder,
}

impl Cluster {
    /// Try to retrieve the cluster key from the remote data folder and create a new cluster instance.
    /// If the cluster key could not be retrieved from the remote data folder or the cluster metadata
    /// tables do not exist and could not be created, return [`ModelarDbServerError`].
    pub async fn try_new(remote_data_folder: DataFolder) -> Result<Self> {
        remote_data_folder
            .create_and_register_cluster_metadata_tables()
            .await?;

        let key = remote_data_folder.cluster_key().await?.to_string();

        // Convert the key to a MetadataValue since it is used in tonic requests.
        let key = MetadataValue::from_str(&key).expect("UUID Version 4 should be valid ASCII.");

        Ok(Self {
            key,
            remote_data_folder,
        })
    }

    /// Return the cloud node in the cluster that is currently most capable of running a query.
    /// Note that the most capable node is currently selected at random. If there are no cloud nodes
    /// in the cluster, return [`ModelarDbServerError`].
    pub async fn query_node(&mut self) -> Result<Node> {
        let nodes = self.remote_data_folder.nodes().await?;

        let cloud_nodes = nodes
            .iter()
            .filter(|n| n.mode == modelardb_types::types::ServerMode::Cloud);

        let mut rng = rng();
        cloud_nodes.choose(&mut rng).cloned().ok_or_else(|| {
            ModelarDbServerError::InvalidState(
                "There are no cloud nodes to execute the query in the cluster.".to_owned(),
            )
        })
    }

    /// For each node in the cluster, execute the given `sql` statement with the cluster key as
    /// metadata. If the statement was successfully executed for each node, return [`Ok`], otherwise
    /// return [`ModelarDbServerError`].
    pub async fn cluster_do_get(&self, sql: &str) -> Result<()> {
        let nodes = self.remote_data_folder.nodes().await?;

        let mut do_get_futures: FuturesUnordered<_> = nodes
            .iter()
            .map(|node| self.connect_and_do_get(&node.url, sql))
            .collect();

        // TODO: Fix issue where we return immediately if we encounter an error. If it is a
        //       connection error, we either need to retry later or remove the node.
        // Run the futures concurrently and log when the statement has been executed on each node.
        while let Some(result) = do_get_futures.next().await {
            info!("Executed statement `{sql}` on node with url '{}'.", result?);
        }

        Ok(())
    }

    /// Connect to the Apache Arrow Flight server given by `url` and execute the given `sql`
    /// statement with the cluster key as metadata. If the statement was successfully executed,
    /// return the url of the node to simplify logging, otherwise return [`ModelarDbServerError`].
    async fn connect_and_do_get(&self, url: &str, sql: &str) -> Result<String> {
        let mut flight_client = FlightServiceClient::connect(url.to_owned()).await?;

        // Add the key to the request metadata to indicate that the request is a cluster operation.
        let mut request = Request::new(Ticket::new(sql.to_owned()));
        request
            .metadata_mut()
            .insert("x-cluster-key", self.key.clone());

        flight_client.do_get(request).await?;

        Ok(url.to_owned())
    }

    /// For each node in the cluster, execute the given `action` with the cluster key as metadata.
    /// If the action was successfully executed for each node, return [`Ok`], otherwise return
    /// [`ModelarDbServerError`].
    pub async fn cluster_do_action(&self, action: Action) -> Result<()> {
        let nodes = self.remote_data_folder.nodes().await?;

        let mut action_futures: FuturesUnordered<_> = nodes
            .iter()
            .map(|node| self.connect_and_do_action(&node.url, action.clone()))
            .collect();

        // Run the futures concurrently and log when the action has been executed on each node.
        while let Some(result) = action_futures.next().await {
            info!(
                "Executed action `{}` on node with url '{}'.",
                action.r#type, result?
            );
        }

        Ok(())
    }

    /// Connect to the Apache Arrow Flight server given by `url` and make a request to do `action`
    /// with the cluster key as metadata. If the action was successfully executed, return the url
    /// of the node to simplify logging, otherwise return [`ModelarDbServerError`].
    async fn connect_and_do_action(&self, url: &str, action: Action) -> Result<String> {
        let mut flight_client = FlightServiceClient::connect(url.to_owned()).await?;

        // Add the key to the request metadata to indicate that the request is a cluster operation.
        let mut request = Request::new(action);
        request
            .metadata_mut()
            .insert("x-cluster-key", self.key.clone());

        flight_client.do_action(request).await?;

        Ok(url.to_owned())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use modelardb_types::types::ServerMode;
    use tempfile::TempDir;

    // Tests for Cluster.
    #[tokio::test]
    async fn test_query_node() {
        let (_temp_dir, mut cluster) = create_cluster_with_edge().await;

        let cloud_node = Node::new("cloud".to_owned(), ServerMode::Cloud);
        cluster
            .remote_data_folder
            .save_node(cloud_node.clone())
            .await
            .unwrap();

        assert_eq!(cluster.query_node().await.unwrap(), cloud_node);
    }

    #[tokio::test]
    async fn test_query_node_no_cloud_nodes() {
        let (_temp_dir, mut cluster) = create_cluster_with_edge().await;
        let result = cluster.query_node().await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid State Error: There are no cloud nodes to execute the query in the cluster."
        );
    }

    async fn create_cluster_with_edge() -> (TempDir, Cluster) {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::open_local_url(temp_dir_url).await.unwrap();

        let cluster = Cluster::try_new(local_data_folder.clone()).await.unwrap();

        local_data_folder
            .save_node(Node::new("edge".to_owned(), ServerMode::Edge))
            .await
            .unwrap();

        (temp_dir, cluster)
    }
}
