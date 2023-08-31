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
    pub fn new(nodes: Vec<Node>) -> Self {
        Self { nodes }
    }

    /// Checks if the node is already registered and adds it to the current nodes if not. If it
    /// already exists, [`ConfigurationError`](ModelarDbError::ConfigurationError) is returned.
    pub fn register_node(&mut self, node: Node) -> Result<(), ModelarDbError> {
        if self.nodes.iter().any(|node| node.url == node.url) {
            Err(ModelarDbError::ConfigurationError(format!(
                "A node with the url `{}` is already registered.",
                node.url
            )))
        } else {
            self.nodes.push(node);
            Ok(())
        }
    }

    /// Remove the node with an url matching `url` from the current nodes. If no node with `url`
    /// exists, [`ConfigurationError`](ModelarDbError::ConfigurationError) is returned.
    pub fn remove_node(&mut self, url: &str) -> Result<(), ModelarDbError> {
        if self.nodes.iter().any(|node| node.url == url) {
            self.nodes.retain(|node| node.url != url);
            Ok(())
        } else {
            Err(ModelarDbError::ConfigurationError(format!(
                "A node with the url `{url}` does not exist."
            )))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // Tests for Cluster.
    #[test]
    fn test_register_node() {
        let node = Node::new("localhost".to_string(), ServerMode::Edge);
        let mut cluster = Cluster::new(vec![]);

        assert!(cluster.register_node(node.clone()).is_ok());
        assert!(cluster.nodes.contains(&node));
    }

    #[test]
    fn test_register_already_registered_node() {
        let node = Node::new("localhost".to_string(), ServerMode::Edge);
        let mut cluster = Cluster::new(vec![node.clone()]);

        assert!(cluster.register_node(node).is_err());
    }

    #[test]
    fn test_remove_node() {
        let node_1 = Node::new("localhost_1".to_string(), ServerMode::Edge);
        let node_2 = Node::new("localhost_2".to_string(), ServerMode::Edge);

        let mut cluster = Cluster::new(vec![node_1.clone(), node_2.clone()]);

        cluster.remove_node("localhost_1").unwrap();
        assert_eq!(cluster.nodes, vec![node_2]);
    }

    #[test]
    fn test_remove_node_invalid_url() {
        let mut cluster = Cluster::new(vec![]);
        assert!(cluster.remove_node("invalid_url").is_err());
    }
}
