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

//! Management of the Delta Lake for the cluster. Metadata which is unique to the cluster, such as
//! the key and the nodes, is handled here.

use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use deltalake::DeltaTableError;
use deltalake::datafusion::logical_expr::{col, lit};
use modelardb_types::types::{Node, ServerMode};
use uuid::Uuid;

use crate::data_folder::DataFolder;
use crate::error::Result;
use crate::{register_metadata_table, sql_and_concat};

/// Trait that extends [`DataFolder`] to provide management of the Delta Lake for the cluster.
#[allow(async_fn_in_trait)]
pub trait ClusterMetadata {
    async fn create_and_register_cluster_metadata_tables(&self) -> Result<()>;
    async fn cluster_key(&self) -> Result<Uuid>;
    async fn save_node(&self, node: Node) -> Result<()>;
    async fn remove_node(&self, url: &str) -> Result<()>;
    async fn nodes(&self) -> Result<Vec<Node>>;
}

impl ClusterMetadata for DataFolder {
    /// If they do not already exist, create the tables that are specific to the cluster and
    /// register them with the Apache DataFusion session context.
    /// * The `cluster_metadata` table contains metadata for the cluster itself. It is assumed that
    ///   this table will only have a single row since there can only be a single cluster.
    /// * The `nodes` table contains metadata for each node that is in the cluster.
    ///
    /// If the tables exist or were created, return [`Ok`], otherwise return
    /// [`ModelarDbStorageError`](crate::error::ModelarDbStorageError).
    async fn create_and_register_cluster_metadata_tables(&self) -> Result<()> {
        // Create and register the cluster_metadata table if it does not exist.
        let delta_table = self
            .create_metadata_table(
                "cluster_metadata",
                &Schema::new(vec![Field::new("key", DataType::Utf8, false)]),
            )
            .await?;

        register_metadata_table(self.session_context(), "cluster_metadata", delta_table).await?;

        // Create and register the nodes table if it does not exist.
        let delta_table = self
            .create_metadata_table(
                "nodes",
                &Schema::new(vec![
                    Field::new("url", DataType::Utf8, false),
                    Field::new("mode", DataType::Utf8, false),
                ]),
            )
            .await?;

        register_metadata_table(self.session_context(), "nodes", delta_table).await?;

        Ok(())
    }

    /// Retrieve the key for the cluster from the `cluster_metadata` table. If a key does not
    /// already exist, create one and save it to the Delta Lake. If a key could not be retrieved
    /// or created, return [`ModelarDbStorageError`](crate::error::ModelarDbStorageError).
    async fn cluster_key(&self) -> Result<Uuid> {
        let sql = "SELECT key FROM metadata.cluster_metadata";
        let batch = sql_and_concat(self.session_context(), sql).await?;

        let keys = modelardb_types::array!(batch, 0, StringArray);
        if keys.is_empty() {
            let cluster_key = Uuid::new_v4();

            // Add a new row to the cluster_metadata table to persist the key.
            self.write_columns_to_metadata_table(
                "cluster_metadata",
                vec![Arc::new(StringArray::from(vec![cluster_key.to_string()]))],
            )
            .await?;

            Ok(cluster_key)
        } else {
            let cluster_key: String = keys.value(0).to_owned();

            Ok(cluster_key
                .parse()
                .map_err(|error: uuid::Error| DeltaTableError::Generic(error.to_string()))?)
        }
    }

    /// Save the node to the Delta Lake and return [`Ok`]. If the node could not be saved, return
    /// [`ModelarDbStorageError`](crate::error::ModelarDbStorageError).
    async fn save_node(&self, node: Node) -> Result<()> {
        self.write_columns_to_metadata_table(
            "nodes",
            vec![
                Arc::new(StringArray::from(vec![node.url])),
                Arc::new(StringArray::from(vec![node.mode.to_string()])),
            ],
        )
        .await?;

        Ok(())
    }

    /// Remove the row in the `nodes` table that corresponds to the node with `url` and return
    /// [`Ok`]. If the row could not be removed, return
    /// [`ModelarDbStorageError`](crate::error::ModelarDbStorageError).
    async fn remove_node(&self, url: &str) -> Result<()> {
        let delta_table = self.metadata_delta_table("nodes").await?;

        delta_table
            .delete()
            .with_predicate(col("url").eq(lit(url)))
            .await?;

        Ok(())
    }

    /// Return the nodes currently in the cluster that have been persisted to the Delta Lake. If the
    /// nodes could not be retrieved, [`ModelarDbStorageError`](crate::error::ModelarDbStorageError)
    /// is returned.
    async fn nodes(&self) -> Result<Vec<Node>> {
        let mut nodes: Vec<Node> = vec![];

        let sql = "SELECT url, mode FROM metadata.nodes";
        let batch = sql_and_concat(self.session_context(), sql).await?;

        let url_array = modelardb_types::array!(batch, 0, StringArray);
        let mode_array = modelardb_types::array!(batch, 1, StringArray);

        for row_index in 0..batch.num_rows() {
            let url = url_array.value(row_index).to_owned();
            let mode = mode_array.value(row_index).to_owned();

            let server_mode = ServerMode::from_str(&mode)
                .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

            nodes.push(Node::new(url, server_mode));
        }

        Ok(nodes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    // Tests for ClusterMetadata.
    #[tokio::test]
    async fn test_create_cluster_metadata_tables() {
        let (_temp_dir, data_folder) = create_data_folder().await;

        // Verify that the tables were created, registered, and has the expected columns.
        assert!(
            data_folder
                .session_context()
                .sql("SELECT key FROM metadata.cluster_metadata")
                .await
                .is_ok()
        );

        assert!(
            data_folder
                .session_context()
                .sql("SELECT url, mode FROM metadata.nodes")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_new_cluster_key() {
        let (_temp_dir, data_folder) = create_data_folder().await;

        // Verify that the cluster key is created and saved correctly.
        let cluster_key = data_folder.cluster_key().await.unwrap();

        let sql = "SELECT key FROM metadata.cluster_metadata";
        let batch = sql_and_concat(data_folder.session_context(), sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![cluster_key.to_string()])
        );
    }

    #[tokio::test]
    async fn test_existing_cluster_key() {
        let (_temp_dir, data_folder) = create_data_folder().await;

        // Verify that only a single key is created and saved when retrieving multiple times.
        let cluster_key_1 = data_folder.cluster_key().await.unwrap();
        let cluster_key_2 = data_folder.cluster_key().await.unwrap();

        let sql = "SELECT key FROM metadata.cluster_metadata";
        let batch = sql_and_concat(data_folder.session_context(), sql)
            .await
            .unwrap();

        assert_eq!(cluster_key_1, cluster_key_2);
        assert_eq!(batch.column(0).len(), 1);
    }

    #[tokio::test]
    async fn test_save_node() {
        let (_temp_dir, data_folder) = create_data_folder().await;

        let node_1 = Node::new("url_1".to_string(), ServerMode::Edge);
        data_folder.save_node(node_1.clone()).await.unwrap();

        let node_2 = Node::new("url_2".to_string(), ServerMode::Edge);
        data_folder.save_node(node_2.clone()).await.unwrap();

        // Verify that the nodes are saved correctly.
        let sql = "SELECT url, mode FROM metadata.nodes";
        let batch = sql_and_concat(data_folder.session_context(), sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![node_2.url.clone(), node_1.url.clone()])
        );
        assert_eq!(
            **batch.column(1),
            StringArray::from(vec![node_2.mode.to_string(), node_1.mode.to_string()])
        );
    }

    #[tokio::test]
    async fn test_remove_node() {
        let (_temp_dir, data_folder) = create_data_folder().await;

        let node_1 = Node::new("url_1".to_string(), ServerMode::Edge);
        data_folder.save_node(node_1.clone()).await.unwrap();

        let node_2 = Node::new("url_2".to_string(), ServerMode::Edge);
        data_folder.save_node(node_2.clone()).await.unwrap();

        data_folder.remove_node(&node_1.url).await.unwrap();

        // Verify that node_1 is removed correctly.
        let sql = "SELECT url, mode FROM metadata.nodes";
        let batch = sql_and_concat(data_folder.session_context(), sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![node_2.url.clone()])
        );
        assert_eq!(
            **batch.column(1),
            StringArray::from(vec![node_2.mode.to_string()])
        );
    }

    #[tokio::test]
    async fn test_nodes() {
        let (_temp_dir, data_folder) = create_data_folder().await;

        let node_1 = Node::new("url_1".to_string(), ServerMode::Edge);
        data_folder.save_node(node_1.clone()).await.unwrap();

        let node_2 = Node::new("url_2".to_string(), ServerMode::Edge);
        data_folder.save_node(node_2.clone()).await.unwrap();

        let nodes = data_folder.nodes().await.unwrap();

        assert_eq!(nodes, vec![node_2, node_1]);
    }

    async fn create_data_folder() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();

        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        data_folder
            .create_and_register_cluster_metadata_tables()
            .await
            .unwrap();

        (temp_dir, data_folder)
    }
}
