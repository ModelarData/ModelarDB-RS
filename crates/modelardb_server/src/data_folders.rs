/* Copyright 2024 The ModelarDB Contributors
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

//! Implementation of a struct that provides access to the local and remote data storage components.

use modelardb_storage::data_folder::DataFolder;
use modelardb_types::types::ServerMode;

use crate::ClusterMode;
use crate::Result;
use crate::error::ModelarDbServerError;
use crate::manager::Manager;

/// Folders for storing metadata and data in Apache Parquet files locally and remotely.
#[derive(Clone)]
pub struct DataFolders {
    /// Folder for storing metadata and data in Apache Parquet files on the local file system.
    pub local_data_folder: DataFolder,
    /// Folder for storing metadata and data in Apache Parquet files in a remote object store.
    pub maybe_remote_data_folder: Option<DataFolder>,
    /// Folder from which metadata and data in Apache Parquet files will be read during query execution.
    /// It is equivalent to `local_data_folder` when deployed on the edge and `remote_data_folder`
    /// when deployed in the cloud.
    pub query_data_folder: DataFolder,
}

impl DataFolders {
    pub fn new(
        local_data_folder: DataFolder,
        maybe_remote_data_folder: Option<DataFolder>,
        query_data_folder: DataFolder,
    ) -> Self {
        Self {
            local_data_folder,
            maybe_remote_data_folder,
            query_data_folder,
        }
    }

    /// Parse the given command line arguments into a [`ClusterMode`] and an instance of
    /// [`DataFolders`]. If the necessary command line arguments are not provided, too many
    /// arguments are provided, or if the arguments are malformed, [`ModelarDbServerError`] is
    /// returned.
    pub async fn try_from_command_line_arguments(
        arguments: &[&str],
    ) -> Result<(ClusterMode, Self)> {
        // Match the provided command line arguments to the supported inputs.
        match arguments {
            &["edge", local_data_folder_url] | &[local_data_folder_url] => {
                let local_data_folder = DataFolder::open_local_url(local_data_folder_url).await?;

                Ok((
                    ClusterMode::SingleNode,
                    Self::new(local_data_folder.clone(), None, local_data_folder),
                ))
            }
            &["cloud", local_data_folder_url, manager_url] => {
                let (manager, storage_configuration) =
                    Manager::register_node(manager_url, ServerMode::Cloud).await?;

                let local_data_folder = DataFolder::open_local_url(local_data_folder_url).await?;

                let remote_data_folder =
                    DataFolder::open_object_store(storage_configuration).await?;

                Ok((
                    ClusterMode::MultiNode(manager),
                    Self::new(
                        local_data_folder,
                        Some(remote_data_folder.clone()),
                        remote_data_folder,
                    ),
                ))
            }
            &["edge", local_data_folder_url, manager_url]
            | &[local_data_folder_url, manager_url] => {
                let (manager, storage_configuration) =
                    Manager::register_node(manager_url, ServerMode::Edge).await?;

                let local_data_folder = DataFolder::open_local_url(local_data_folder_url).await?;

                let remote_data_folder =
                    DataFolder::open_object_store(storage_configuration).await?;

                Ok((
                    ClusterMode::MultiNode(manager),
                    Self::new(
                        local_data_folder.clone(),
                        Some(remote_data_folder),
                        local_data_folder,
                    ),
                ))
            }
            _ => Err(ModelarDbServerError::InvalidArgument(
                "Too few, too many, or malformed arguments.".to_owned(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for try_from_command_line_arguments().
    #[tokio::test]
    async fn test_try_from_empty_command_line_arguments() {
        let result = DataFolders::try_from_command_line_arguments(&[]).await;

        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid Argument Error: Too few, too many, or malformed arguments.".to_owned()
        );
    }

    #[tokio::test]
    async fn test_try_from_edge_command_line_arguments_without_manager() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_str = temp_dir.path().to_str().unwrap();

        assert_single_node_without_remote_data_folder(&["edge", temp_dir_str]).await;
    }

    #[tokio::test]
    async fn test_try_from_edge_command_line_arguments_without_server_mode_and_manager() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_str = temp_dir.path().to_str().unwrap();

        assert_single_node_without_remote_data_folder(&[temp_dir_str]).await;
    }

    async fn assert_single_node_without_remote_data_folder(input: &[&str]) {
        let (cluster_mode, data_folders) = DataFolders::try_from_command_line_arguments(input)
            .await
            .unwrap();

        assert_eq!(cluster_mode, ClusterMode::SingleNode);
        assert!(data_folders.maybe_remote_data_folder.is_none());
    }

    #[tokio::test]
    async fn test_try_from_incomplete_cloud_command_line_arguments() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_str = temp_dir.path().to_str().unwrap();

        let result = DataFolders::try_from_command_line_arguments(&["cloud", temp_dir_str]).await;

        assert_eq!(
            result.err().unwrap().to_string(),
            format!(
                "Invalid Argument Error: Could not connect to manager at '{temp_dir_str}': transport error",
            )
        );
    }
}
