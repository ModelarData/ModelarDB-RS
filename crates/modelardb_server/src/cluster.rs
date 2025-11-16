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

use rand::rng;
use rand::seq::IteratorRandom;

use modelardb_storage::data_folder::DataFolder;
use modelardb_storage::data_folder::cluster::ClusterMetadata;
use modelardb_types::types::Node;

use crate::error::{ModelarDbServerError, Result};

/// Stores the currently managed nodes in the cluster and allows for performing operations that need
/// to be applied to every single node in the cluster.
pub struct Cluster {
    /// Key identifying the cluster. The key is used to validate communication within the cluster
    /// between nodes.
    key: String,
    /// The remote data folder that each node in the cluster should be synchronized with.
    /// When a table is created, dropped, vacuumed, or truncated, it is done in the
    /// remote data folder first.
    remote_data_folder: DataFolder,
}

impl Cluster {
    /// Try to retrieve the cluster key from the remote data folder and create a new cluster
    /// instance. If the cluster key could not be retrieved from the remote data folder, return
    /// [`ModelarDbServerError`].
    pub async fn try_new(remote_data_folder: DataFolder) -> Result<Self> {
        let key = remote_data_folder.cluster_key().await?.to_string();

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
}
