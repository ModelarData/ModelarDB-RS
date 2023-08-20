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

use modelardb_common::types::ServerMode;

/// A single ModelarDB server that is controlled by the manager. The node can either be an edge node
/// or a cloud node. A node cannot be another manager.
pub struct ClusterNode {
    /// Arrow Flight URL for the node. This URL uniquely identifies the node.
    url: String,
    /// The mode the node was started in.
    mode: ServerMode,
}

impl ClusterNode {
    pub fn new(url: String, mode: ServerMode) -> Self {
        Self {
            url,
            mode
        }
    }
}

/// Stores the currently managed nodes in the cluster and allows for performing operations that need
/// to be applied to every single node in the cluster.
pub struct ClusterManager {
    /// The nodes that are currently managed by the cluster manager.
    nodes: Vec<ClusterNode>
}

impl ClusterManager {
    pub fn new() -> Self {
        Self {
            nodes: vec![]
        }
    }
}
