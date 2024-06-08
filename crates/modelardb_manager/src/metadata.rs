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

//! Management of the metadata delta lake for the manager. Metadata which is unique to the manager,
//! such as metadata about registered edges, is handled here.

use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use deltalake::datafusion::logical_expr::{col, lit};
use deltalake::kernel::{DataType, StructField};
use deltalake::DeltaTableError;
use modelardb_common::array;
use modelardb_common::metadata::table_metadata_manager::TableMetadataManager;
use modelardb_common::metadata::MetadataDeltaLake;
use modelardb_common::types::ServerMode;
use uuid::Uuid;

use crate::cluster::Node;

/// Stores the metadata required for reading from and writing to the tables and model tables and
/// persisting edges. The data that needs to be persisted is stored in the metadata delta lake.
pub struct MetadataManager {
    /// Delta lake with functionality to read and write to and from the manager metadata tables.
    metadata_delta_lake: MetadataDeltaLake,
    /// Metadata manager used to interface with the subset of the manager metadata delta lake
    /// related to tables and model tables.
    pub(crate) table_metadata_manager: TableMetadataManager,
}

impl MetadataManager {
    /// Create a new [`MetadataManager`] that saves the metadata to a remote object store given by
    /// `connection_info` and initialize the metadata tables. If `connection_info` could not be
    /// parsed or the metadata tables could not be created, return [`DeltaTableError`].
    pub async fn try_from_connection_info(
        connection_info: &[u8],
    ) -> Result<MetadataManager, DeltaTableError> {
        let metadata_manager = Self {
            metadata_delta_lake: MetadataDeltaLake::try_from_connection_info(connection_info)
                .await?,
            table_metadata_manager: TableMetadataManager::try_from_connection_info(connection_info)
                .await?,
        };

        // Create the necessary tables in the metadata delta lake.
        metadata_manager
            .create_manager_metadata_delta_lake_tables()
            .await?;

        Ok(metadata_manager)
    }

    /// If they do not already exist, create the tables that are specific to the manager metadata
    /// delta lake.
    /// * The `manager_metadata` table contains metadata for the manager itself. It is assumed that
    /// this table will only have a single row since there can only be a single manager.
    /// * The `nodes` table contains metadata for each node that is controlled by the manager.
    ///
    /// If the tables exist or were created, return [`Ok`], otherwise return [`DeltaTableError`].
    async fn create_manager_metadata_delta_lake_tables(&self) -> Result<(), DeltaTableError> {
        // Create the manager_metadata table if it does not exist.
        self.metadata_delta_lake
            .create_delta_lake_table(
                "manager_metadata",
                vec![StructField::new("key", DataType::STRING, false)],
            )
            .await?;

        // Create the nodes table if it does not exist.
        self.metadata_delta_lake
            .create_delta_lake_table(
                "nodes",
                vec![
                    StructField::new("url", DataType::STRING, false),
                    StructField::new("mode", DataType::STRING, false),
                ],
            )
            .await?;

        Ok(())
    }

    /// Retrieve the key for the manager from the `manager_metadata` table. If a key does not
    /// already exist, create one and save it to the delta lake. If a key could not be retrieved
    /// or created, return [`DeltaTableError`].
    pub async fn manager_key(&self) -> Result<Uuid, DeltaTableError> {
        let batch = self
            .metadata_delta_lake
            .query_table("manager_metadata", "SELECT key FROM manager_metadata")
            .await?;

        let keys = array!(batch, 0, StringArray);
        if keys.is_empty() {
            let manager_key = Uuid::new_v4();

            // Add a new row to the manager_metadata table to persist the key.
            self.metadata_delta_lake
                .append_to_table(
                    "manager_metadata",
                    vec![Arc::new(StringArray::from(vec![manager_key.to_string()]))],
                )
                .await?;

            Ok(manager_key)
        } else {
            let manager_key: String = keys.value(0).to_owned();

            Ok(manager_key
                .parse()
                .map_err(|error: uuid::Error| DeltaTableError::Generic(error.to_string()))?)
        }
    }

    /// Save the node to the metadata delta lake and return [`Ok`]. If the node could not be saved,
    /// return [`DeltaTableError`].
    pub async fn save_node(&self, node: &Node) -> Result<(), DeltaTableError> {
        self.metadata_delta_lake
            .append_to_table(
                "nodes",
                vec![
                    Arc::new(StringArray::from(vec![node.url.clone()])),
                    Arc::new(StringArray::from(vec![node.mode.to_string()])),
                ],
            )
            .await?;

        Ok(())
    }

    /// Remove the row in the `nodes` table that corresponds to the node with `url` and return
    /// [`Ok`]. If the row could not be removed, return [`DeltaTableError`].
    pub async fn remove_node(&self, url: &str) -> Result<(), DeltaTableError> {
        let ops = self
            .metadata_delta_lake
            .metadata_table_delta_ops("nodes")
            .await?;

        ops.delete().with_predicate(col("url").eq(lit(url))).await?;

        Ok(())
    }

    /// Return the nodes currently controlled by the manager that have been persisted to the
    /// metadata delta lake. If the nodes could not be retrieved, [`DeltaTableError`] is returned.
    pub async fn nodes(&self) -> Result<Vec<Node>, DeltaTableError> {
        let mut nodes: Vec<Node> = vec![];

        let batch = self
            .metadata_delta_lake
            .query_table("nodes", "SELECT url, mode FROM nodes")
            .await?;

        let url_array = array!(batch, 0, StringArray);
        let mode_array = array!(batch, 1, StringArray);

        for row_index in 0..batch.num_rows() {
            let url = url_array.value(row_index).to_owned();
            let mode = mode_array.value(row_index).to_owned();

            let server_mode = ServerMode::from_str(&mode)
                .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

            nodes.push(Node::new(url, server_mode));
        }

        Ok(nodes)
    }

    /// Return the SQL query used to create the table with the name `table_name`. If a table with
    /// that name does not exist, return [`DeltaTableError`].
    pub async fn table_sql(&self, table_name: &str) -> Result<String, DeltaTableError> {
        let batch = self
            .metadata_delta_lake
            .query_table(
                "table_metadata",
                &format!("SELECT sql FROM table_metadata WHERE table_name = '{table_name}'"),
            )
            .await?;

        let table_sql = array!(batch, 0, StringArray);
        if table_sql.is_empty() {
            let batch = self
                .metadata_delta_lake
                .query_table(
                    "model_table_metadata",
                    &format!(
                        "SELECT sql FROM model_table_metadata WHERE table_name = '{table_name}'"
                    ),
                )
                .await?;

            let model_table_sql = array!(batch, 0, StringArray);
            if model_table_sql.is_empty() {
                Err(DeltaTableError::Generic(format!(
                    "No table with the name '{table_name}' exists."
                )))
            } else {
                Ok(model_table_sql.value(0).to_owned())
            }
        } else {
            Ok(table_sql.value(0).to_owned())
        }
    }

    /// Retrieve all rows of `column` from both the table_metadata and model_table_metadata tables.
    /// If the column could not be retrieved, either because it does not exist or because it could
    /// not be converted to a string, return [`DeltaTableError`].
    pub async fn table_metadata_column(
        &self,
        column: &str,
    ) -> Result<Vec<String>, DeltaTableError> {
        // Retrieve the column from both tables containing table metadata.
        let table_metadata_batch = self
            .metadata_delta_lake
            .query_table(
                "table_metadata",
                &format!("SELECT {column} FROM table_metadata"),
            )
            .await?;

        let model_table_metadata_batch = self
            .metadata_delta_lake
            .query_table(
                "model_table_metadata",
                &format!("SELECT {column} FROM model_table_metadata"),
            )
            .await?;

        let table_metadata_column = array!(table_metadata_batch, 0, StringArray);
        let model_table_metadata_column = array!(model_table_metadata_batch, 0, StringArray);

        Ok(table_metadata_column
            .iter()
            .chain(model_table_metadata_column.iter())
            .map(|column_value| column_value.unwrap().to_string())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_common::test;
    use object_store::path::Path;
    use tempfile::TempDir;

    // Tests for MetadataManager.
    #[tokio::test]
    async fn test_create_manager_metadata_delta_lake_tables() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        // Verify that the tables were created, registered, and has the expected columns.
        assert!(metadata_manager
            .metadata_delta_lake
            .query_table("manager_metadata", "SELECT key FROM manager_metadata")
            .await
            .is_ok());

        assert!(metadata_manager
            .metadata_delta_lake
            .query_table("nodes", "SELECT url, mode FROM nodes")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_new_manager_key() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        // Verify that the manager key is created and saved correctly.
        let manager_key = metadata_manager.manager_key().await.unwrap();

        let batch = metadata_manager
            .metadata_delta_lake
            .query_table("manager_metadata", "SELECT key FROM manager_metadata")
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![manager_key.to_string()])
        );
    }

    #[tokio::test]
    async fn test_existing_manager_key() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        // Verify that only a single key is created and saved when retrieving multiple times.
        let manager_key_1 = metadata_manager.manager_key().await.unwrap();
        let manager_key_2 = metadata_manager.manager_key().await.unwrap();

        let batch = metadata_manager
            .metadata_delta_lake
            .query_table("manager_metadata", "SELECT key FROM manager_metadata")
            .await
            .unwrap();

        assert_eq!(manager_key_1, manager_key_2);
        assert_eq!(batch.column(0).len(), 1);
    }

    #[tokio::test]
    async fn test_save_node() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        let node_1 = Node::new("url_1".to_string(), ServerMode::Edge);
        metadata_manager.save_node(&node_1).await.unwrap();

        let node_2 = Node::new("url_2".to_string(), ServerMode::Edge);
        metadata_manager.save_node(&node_2).await.unwrap();

        // Verify that the nodes are saved correctly.
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table("nodes", "SELECT url, mode FROM nodes")
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
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        let node_1 = Node::new("url_1".to_string(), ServerMode::Edge);
        metadata_manager.save_node(&node_1).await.unwrap();

        let node_2 = Node::new("url_2".to_string(), ServerMode::Edge);
        metadata_manager.save_node(&node_2).await.unwrap();

        metadata_manager.remove_node(&node_1.url).await.unwrap();

        // Verify that node_1 is removed correctly.
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table("nodes", "SELECT url, mode FROM nodes")
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
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        let node_1 = Node::new("url_1".to_string(), ServerMode::Edge);
        metadata_manager.save_node(&node_1).await.unwrap();

        let node_2 = Node::new("url_2".to_string(), ServerMode::Edge);
        metadata_manager.save_node(&node_2).await.unwrap();

        let nodes = metadata_manager.nodes().await.unwrap();

        assert_eq!(nodes, vec![node_2, node_1]);
    }

    #[tokio::test]
    async fn test_table_sql_for_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        metadata_manager
            .table_metadata_manager
            .save_table_metadata("table_1", "CREATE TABLE table_1")
            .await
            .unwrap();

        let sql = metadata_manager.table_sql("table_1").await.unwrap();
        assert_eq!(sql, "CREATE TABLE table_1");
    }

    #[tokio::test]
    async fn test_table_sql_for_model_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .table_metadata_manager
            .save_model_table_metadata(&model_table_metadata, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        let sql = metadata_manager
            .table_sql(&model_table_metadata.name)
            .await
            .unwrap();

        assert_eq!(sql, test::MODEL_TABLE_SQL);
    }

    #[tokio::test]
    async fn test_table_sql_for_invalid_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        let result = metadata_manager.table_sql("invalid_table").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_table_metadata_column() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        metadata_manager
            .table_metadata_manager
            .save_table_metadata("table_1", "CREATE TABLE table_1")
            .await
            .unwrap();

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .table_metadata_manager
            .save_model_table_metadata(&model_table_metadata, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        let table_names = metadata_manager
            .table_metadata_column("table_name")
            .await
            .unwrap();

        let table_sql = metadata_manager.table_metadata_column("sql").await.unwrap();

        assert_eq!(table_names, vec!["table_1", &model_table_metadata.name]);
        assert_eq!(
            table_sql,
            vec!["CREATE TABLE table_1", test::MODEL_TABLE_SQL]
        );
    }

    #[tokio::test]
    async fn test_table_metadata_column_for_invalid_column() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        let result = metadata_manager
            .table_metadata_column("invalid_column")
            .await;

        assert!(result.is_err());
    }

    async fn create_metadata_manager() -> (TempDir, MetadataManager) {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = Path::from_absolute_path(temp_dir.path()).unwrap();

        let table_metadata_manager = TableMetadataManager::try_from_path(path.clone())
            .await
            .unwrap();

        let metadata_manager = MetadataManager {
            metadata_delta_lake: MetadataDeltaLake::from_path(path),
            table_metadata_manager,
        };

        metadata_manager
            .create_manager_metadata_delta_lake_tables()
            .await
            .unwrap();

        (temp_dir, metadata_manager)
    }
}
