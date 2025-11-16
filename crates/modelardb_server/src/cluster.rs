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
use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Ticket};
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::TableProvider;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log::info;
use modelardb_storage::data_folder::DataFolder;
use modelardb_storage::data_folder::cluster::ClusterMetadata;
use modelardb_types::types::{Node, TimeSeriesTableMetadata};
use rand::rng;
use rand::seq::IteratorRandom;
use tonic::Request;
use tonic::metadata::{Ascii, MetadataValue};

use crate::context::Context;
use crate::error::{ModelarDbServerError, Result};

/// Stores the currently managed nodes in the cluster and allows for performing operations that need
/// to be applied to every single node in the cluster.
#[derive(Clone)]
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
    /// Create and register the cluster metadata tables and save the given `node` in the cluster.
    /// It is assumed that `node` corresponds to the local system running `modelardbd`. If the
    /// cluster metadata tables do not exist and could not be created or the node could not be
    /// saved, return [`ModelarDbServerError`].
    pub async fn try_new(node: Node, remote_data_folder: DataFolder) -> Result<Self> {
        remote_data_folder
            .create_and_register_cluster_metadata_tables()
            .await?;

        remote_data_folder.save_node(node).await?;

        let key = remote_data_folder.cluster_key().await?.to_string();

        // Convert the key to a MetadataValue since it is used in tonic requests.
        let key = MetadataValue::from_str(&key).expect("UUID Version 4 should be valid ASCII.");

        Ok(Self {
            key,
            remote_data_folder,
        })
    }

    /// Initialize the local database schema with the normal tables and time series tables from the
    /// cluster's database schema using the remote data folder. If the tables to create could not be
    /// retrieved from the remote data folder, or the tables could not be created,
    /// return [`ModelarDbServerError`].
    pub(crate) async fn retrieve_and_create_tables(&self, context: &Arc<Context>) -> Result<()> {
        let local_data_folder = &context.data_folders.local_data_folder;

        validate_local_tables_exist_remotely(local_data_folder, &self.remote_data_folder).await?;

        // Validate that all tables that are in both the local and remote data folder are identical.
        let missing_normal_tables =
            validate_normal_tables(local_data_folder, &self.remote_data_folder).await?;

        let missing_time_series_tables =
            validate_time_series_tables(local_data_folder, &self.remote_data_folder).await?;

        // For each table that does not already exist locally, create the table.
        for (table_name, schema) in missing_normal_tables {
            context.create_normal_table(&table_name, &schema).await?;
        }

        for time_series_table_metadata in missing_time_series_tables {
            context
                .create_time_series_table(&time_series_table_metadata)
                .await?;
        }

        Ok(())
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

/// Validate that all tables in the local data folder exist in the remote data folder. If any table
/// does not exist in the remote data folder, return [`ModelarDbServerError`].
async fn validate_local_tables_exist_remotely(
    local_data_folder: &DataFolder,
    remote_data_folder: &DataFolder,
) -> Result<()> {
    let local_table_names = local_data_folder.table_names().await?;
    let remote_table_names = remote_data_folder.table_names().await?;

    let invalid_tables: Vec<String> = local_table_names
        .iter()
        .filter(|table| !remote_table_names.contains(table))
        .cloned()
        .collect();

    if !invalid_tables.is_empty() {
        return Err(ModelarDbServerError::InvalidState(format!(
            "The following tables do not exist in the remote data folder: {}.",
            invalid_tables.join(", ")
        )));
    }

    Ok(())
}

/// For each normal table in the remote data folder, if the table also exists in the local data
/// folder, validate that the schemas are identical. If the schemas are not identical, return
/// [`ModelarDbServerError`]. Return a vector containing the name and schema of each normal table
/// that is in the remote data folder but not in the local data folder.
async fn validate_normal_tables(
    local_data_folder: &DataFolder,
    remote_data_folder: &DataFolder,
) -> Result<Vec<(String, Arc<Schema>)>> {
    let mut missing_normal_tables = vec![];

    let remote_normal_tables = remote_data_folder.normal_table_names().await?;

    for table_name in remote_normal_tables {
        let remote_schema = normal_table_schema(remote_data_folder, &table_name).await?;

        if let Ok(local_schema) = normal_table_schema(local_data_folder, &table_name).await {
            if remote_schema != local_schema {
                return Err(ModelarDbServerError::InvalidState(format!(
                    "The normal table '{table_name}' has a different schema in the local data \
                    folder compared to the remote data folder.",
                )));
            }
        } else {
            missing_normal_tables.push((table_name, remote_schema));
        }
    }

    Ok(missing_normal_tables)
}

/// Retrieve the schema of a normal table from the Delta Lake in the data folder. If the table does
/// not exist, or the schema could not be retrieved, return [`ModelarDbServerError`].
async fn normal_table_schema(data_folder: &DataFolder, table_name: &str) -> Result<Arc<Schema>> {
    let delta_table = data_folder.delta_table(table_name).await?;
    Ok(TableProvider::schema(&delta_table))
}

/// For each time series table in the remote data folder, if the table also exists in the local
/// data folder, validate that the metadata is identical. If the metadata is not identical, return
/// [`ModelarDbServerError`]. Return a vector containing the metadata of each time series table
/// that is in the remote data folder but not in the local data folder.
async fn validate_time_series_tables(
    local_data_folder: &DataFolder,
    remote_data_folder: &DataFolder,
) -> Result<Vec<TimeSeriesTableMetadata>> {
    let mut missing_time_series_tables = vec![];

    let remote_time_series_tables = remote_data_folder.time_series_table_names().await?;

    for table_name in remote_time_series_tables {
        let remote_metadata = remote_data_folder
            .time_series_table_metadata_for_time_series_table(&table_name)
            .await?;

        if let Ok(local_metadata) = local_data_folder
            .time_series_table_metadata_for_time_series_table(&table_name)
            .await
        {
            if remote_metadata != local_metadata {
                return Err(ModelarDbServerError::InvalidState(format!(
                    "The time series table '{table_name}' has different metadata in the local data \
                    folder compared to the remote data folder.",
                )));
            }
        } else {
            missing_time_series_tables.push(remote_metadata);
        }
    }

    Ok(missing_time_series_tables)
}

#[cfg(test)]
mod test {
    use super::*;

    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field};
    use modelardb_test::table::{NORMAL_TABLE_NAME, TIME_SERIES_TABLE_NAME};
    use modelardb_types::types::{ArrowTimestamp, ArrowValue, ErrorBound, ServerMode};
    use tempfile::TempDir;

    use crate::ClusterMode;
    use crate::data_folders::DataFolders;

    // Tests for Cluster.
    #[tokio::test]
    async fn test_retrieve_and_create_missing_local_normal_table() {
        let local_temp_dir = tempfile::tempdir().unwrap();
        let remote_temp_dir = tempfile::tempdir().unwrap();

        let context = create_context(&local_temp_dir, &remote_temp_dir).await;
        let data_folders = context.data_folders.clone();

        // Create a normal table in the remote data folder that should be retrieved and created
        // in the local data folder and one that already exists.
        create_normal_table(
            "normal_table_1",
            "column",
            data_folders.local_data_folder.clone(),
        )
        .await;

        create_normal_table(
            "normal_table_1",
            "column",
            data_folders.maybe_remote_data_folder.clone().unwrap(),
        )
        .await;

        create_normal_table(
            "normal_table_2",
            "column",
            data_folders.maybe_remote_data_folder.clone().unwrap(),
        )
        .await;

        retrieve_and_create_tables(&context).await.unwrap();

        assert_eq!(
            vec!["normal_table_2", "normal_table_1"],
            data_folders.local_data_folder.table_names().await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_retrieve_and_create_invalid_local_normal_table() {
        let local_temp_dir = tempfile::tempdir().unwrap();
        let remote_temp_dir = tempfile::tempdir().unwrap();

        let context = create_context(&local_temp_dir, &remote_temp_dir).await;
        let data_folders = context.data_folders.clone();

        // Create a normal table in the local data folder with the same name as a normal table in
        // the remote data folder, but with a different schema.
        create_normal_table(
            NORMAL_TABLE_NAME,
            "local",
            data_folders.local_data_folder.clone(),
        )
        .await;

        create_normal_table(
            NORMAL_TABLE_NAME,
            "remote",
            data_folders.maybe_remote_data_folder.clone().unwrap(),
        )
        .await;

        let result = retrieve_and_create_tables(&context).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid State Error: The normal table '{NORMAL_TABLE_NAME}' has a different schema \
                in the local data folder compared to the remote data folder."
            )
        );
    }

    #[tokio::test]
    async fn test_retrieve_and_create_missing_local_time_series_table() {
        let local_temp_dir = tempfile::tempdir().unwrap();
        let remote_temp_dir = tempfile::tempdir().unwrap();

        let context = create_context(&local_temp_dir, &remote_temp_dir).await;
        let data_folders = context.data_folders.clone();

        // Create a time series table in the remote data folder that should be retrieved and created
        // in the local data folder and one that already exists.
        create_time_series_table(
            "time_series_table_1",
            "field",
            data_folders.local_data_folder.clone(),
        )
        .await;

        create_time_series_table(
            "time_series_table_1",
            "field",
            data_folders.maybe_remote_data_folder.clone().unwrap(),
        )
        .await;

        create_time_series_table(
            "time_series_table_2",
            "field",
            data_folders.maybe_remote_data_folder.clone().unwrap(),
        )
        .await;

        retrieve_and_create_tables(&context).await.unwrap();

        assert_eq!(
            vec!["time_series_table_2", "time_series_table_1"],
            data_folders.local_data_folder.table_names().await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_retrieve_and_create_invalid_local_time_series_table() {
        let local_temp_dir = tempfile::tempdir().unwrap();
        let remote_temp_dir = tempfile::tempdir().unwrap();

        let context = create_context(&local_temp_dir, &remote_temp_dir).await;
        let data_folders = context.data_folders.clone();

        // Create a time series table in the local data folder with the same name as a time series
        // table in the remote data folder, but with a different schema.
        create_time_series_table(
            TIME_SERIES_TABLE_NAME,
            "local",
            data_folders.local_data_folder.clone(),
        )
        .await;

        create_time_series_table(
            TIME_SERIES_TABLE_NAME,
            "remote",
            data_folders.maybe_remote_data_folder.clone().unwrap(),
        )
        .await;

        let result = retrieve_and_create_tables(&context).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid State Error: The time series table '{TIME_SERIES_TABLE_NAME}' has different \
                metadata in the local data folder compared to the remote data folder."
            )
        );
    }

    #[tokio::test]
    async fn test_retrieve_and_create_missing_remote_table() {
        let local_temp_dir = tempfile::tempdir().unwrap();
        let remote_temp_dir = tempfile::tempdir().unwrap();

        let context = create_context(&local_temp_dir, &remote_temp_dir).await;
        let data_folders = context.data_folders.clone();

        // Create tables in the local data folder that are not in the remote data folder.
        create_normal_table(
            NORMAL_TABLE_NAME,
            "local",
            data_folders.local_data_folder.clone(),
        )
        .await;

        create_time_series_table(
            TIME_SERIES_TABLE_NAME,
            "local",
            data_folders.local_data_folder.clone(),
        )
        .await;

        let result = retrieve_and_create_tables(&context).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid State Error: The following tables do not exist in the remote data folder: \
                {NORMAL_TABLE_NAME}, {TIME_SERIES_TABLE_NAME}."
            )
        );
    }

    /// Create a normal table named `table_name` with a single column named `column_name` in
    /// `data_folder`.
    async fn create_normal_table(table_name: &str, column_name: &str, data_folder: DataFolder) {
        let schema = Schema::new(vec![Field::new(column_name, ArrowValue::DATA_TYPE, false)]);

        data_folder
            .create_normal_table(table_name, &schema)
            .await
            .unwrap();

        data_folder
            .save_normal_table_metadata(table_name)
            .await
            .unwrap();
    }

    /// Create a time series table named `table_name` with a field column named `column_name` in
    /// `data_folder`.
    async fn create_time_series_table(
        table_name: &str,
        column_name: &str,
        data_folder: DataFolder,
    ) {
        let query_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new(column_name, ArrowValue::DATA_TYPE, false),
        ]));

        let time_series_table_metadata = TimeSeriesTableMetadata::try_new(
            table_name.to_owned(),
            query_schema,
            vec![ErrorBound::Lossless, ErrorBound::Lossless],
            vec![None, None],
        )
        .unwrap();

        data_folder
            .create_time_series_table(&time_series_table_metadata)
            .await
            .unwrap();

        data_folder
            .save_time_series_table_metadata(&time_series_table_metadata)
            .await
            .unwrap();
    }

    /// Call [`Cluster::retrieve_and_create_tables`] if the [`ClusterMode`] of `context` is
    /// [`ClusterMode::MultiNode`] and panic if not.
    async fn retrieve_and_create_tables(context: &Arc<Context>) -> Result<()> {
        if let ClusterMode::MultiNode(cluster) =
            &context.configuration_manager.read().await.cluster_mode
        {
            cluster.retrieve_and_create_tables(context).await
        } else {
            panic!("Cluster should be a MultiNode cluster.")
        }
    }

    /// Create a [`Context`] for an edge node within a cluster. Note that both the local and remote
    /// data folder in the context uses a local [`DataFolder`].
    async fn create_context(local_temp_dir: &TempDir, remote_temp_dir: &TempDir) -> Arc<Context> {
        let temp_dir_url = local_temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::open_local_url(temp_dir_url).await.unwrap();

        let edge_node = Node::new("edge".to_owned(), ServerMode::Edge);
        let cluster = create_cluster_with_node(remote_temp_dir, edge_node).await;

        Arc::new(
            Context::try_new(
                DataFolders::new(
                    local_data_folder.clone(),
                    Some(cluster.remote_data_folder.clone()),
                    local_data_folder,
                ),
                ClusterMode::MultiNode(cluster),
            )
            .await
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_query_node() {
        let temp_dir = tempfile::tempdir().unwrap();
        let cloud_node = Node::new("cloud".to_owned(), ServerMode::Cloud);

        let mut cluster = create_cluster_with_node(&temp_dir, cloud_node.clone()).await;

        let query_node = cluster.query_node().await.unwrap();

        assert_eq!(query_node, cloud_node);
    }

    #[tokio::test]
    async fn test_query_node_no_cloud_nodes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let edge_node = Node::new("edge".to_owned(), ServerMode::Edge);

        let mut cluster = create_cluster_with_node(&temp_dir, edge_node).await;

        let result = cluster.query_node().await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid State Error: There are no cloud nodes to execute the query in the cluster."
        );
    }

    /// Create a [`Cluster`] that uses a local [`DataFolder`] for the remote data folder.
    async fn create_cluster_with_node(temp_dir: &TempDir, node: Node) -> Cluster {
        let temp_dir_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::open_local_url(temp_dir_url).await.unwrap();

        Cluster::try_new(node, local_data_folder.clone())
            .await
            .unwrap()
    }
}
