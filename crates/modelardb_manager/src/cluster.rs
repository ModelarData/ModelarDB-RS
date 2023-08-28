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
#[derive(Debug, Clone, PartialEq)]
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
        if self.nodes.iter().any(|node| node.url == cluster_node.url) {
            Err(ModelarDbError::ConfigurationError(format!(
                "A cluster node with the url `{}` is already registered.",
                cluster_node.url
            )))
        } else {
            self.nodes.push(cluster_node);
            Ok(())
        }
    }

    /// Remove the cluster node with an url matching `url` from the current nodes. If no cluster node
    /// with `url` exists, [`ConfigurationError`](ModelarDbError::ConfigurationError) is returned.
    pub fn remove_node(&mut self, url: &str) -> Result<(), ModelarDbError> {
        if self.nodes.iter().any(|node| node.url == url) {
            self.nodes.retain(|node| node.url != url);
            Ok(())
        } else {
            Err(ModelarDbError::ConfigurationError(format!(
                "A cluster node with the url `{url}` does not exist."
            )))
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

    #[test]
    fn test_remove_node() {
        let cluster_node_1 = ClusterNode::new("localhost_1".to_string(), ServerMode::Edge);
        let cluster_node_2 = ClusterNode::new("localhost_2".to_string(), ServerMode::Edge);

        let mut cluster_manager =
            ClusterManager::new(vec![cluster_node_1.clone(), cluster_node_2.clone()]);

        cluster_manager.remove_node("localhost_1").unwrap();
        assert_eq!(cluster_manager.nodes, vec![cluster_node_2]);
    }

    #[test]
    fn test_remove_node_invalid_url() {
        let mut cluster_manager = ClusterManager::new(vec![]);
        assert!(cluster_manager.remove_node("invalid_url").is_err());
    }
}
