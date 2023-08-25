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

use modelardb_common::errors::ModelarDbError;
use modelardb_common::types::ServerMode;

/// A single ModelarDB server that is controlled by the manager. The node can either be an edge node
/// or a cloud node. A node cannot be another manager.
#[derive(Clone, PartialEq)]
pub struct ClusterNode {
    /// Arrow Flight URL for the node. This URL uniquely identifies the node.
    pub url: String,
    /// The mode the node was started in.
    pub mode: ServerMode,
}

impl ClusterNode {
    pub fn new(url: String, mode: ServerMode) -> Self {
        Self { url, mode }
    }
}

/// Stores the currently managed nodes in the cluster and allows for performing operations that need
/// to be applied to every single node in the cluster.
pub struct ClusterManager {
    /// The nodes that are currently managed by the cluster manager.
    nodes: Vec<ClusterNode>,
}

impl ClusterManager {
    pub fn new(nodes: Vec<ClusterNode>) -> Self {
        Self { nodes }
    }

    /// Checks if the cluster node is already registered and adds it to the current nodes if not. If
    /// it already exists, [`ConfigurationError`](ModelarDbError::ConfigurationError) is returned.
    pub fn register_node(&mut self, cluster_node: ClusterNode) -> Result<(), ModelarDbError> {
        let node_urls: Vec<String> = self.nodes.iter().map(|node| node.url.clone()).collect();

        if node_urls.contains(&cluster_node.url) {
            Err(ModelarDbError::ConfigurationError(format!(
                "A cluster node with the url `{}` is already registered.",
                cluster_node.url
            )))
        } else {
            self.nodes.push(cluster_node);
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // Tests for ClusterManager.
    #[test]
    fn test_register_node() {
        let cluster_node = ClusterNode::new("localhost".to_string(), ServerMode::Edge);
        let mut cluster_manager = ClusterManager::new(vec![]);

        assert!(cluster_manager.register_node(cluster_node.clone()).is_ok());
        assert!(cluster_manager.nodes.contains(&cluster_node));
    }

    #[test]
    fn test_register_already_registered_node() {
        let cluster_node = ClusterNode::new("localhost".to_string(), ServerMode::Edge);
        let mut cluster_manager = ClusterManager::new(vec![cluster_node.clone()]);

        assert!(cluster_manager.register_node(cluster_node).is_err());
    }
}
