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

use std::sync::Arc;

use deltalake_core::DeltaTableError;
use modelardb_common::metadata::table_metadata_manager::TableMetadataManager;
use modelardb_common::storage::DeltaLake;
use modelardb_common::types::ServerMode;
use object_store::path::Path;

use crate::ClusterMode;

/// Folder for storing metadata and Apache Parquet files.
#[derive(Clone)]
struct DataFolder {
    /// Delta Lake for storing metadata and Apache Parquet files.
    pub delta_lake: Arc<DeltaLake>,
    /// Metadata manager for providing access to metadata related to tables
    pub table_metadata_manager: Arc<TableMetadataManager>,
}

impl DataFolder {
    pub fn new(
        delta_lake: Arc<DeltaLake>,
        table_metadata_manager: Arc<TableMetadataManager>,
    ) -> Self {
        Self {
            delta_lake,
            table_metadata_manager,
        }
    }
}

/// Return a [`DataFolder`] created from `local_data_folder`. If the folder does not exist, it is
/// created. If the folder does not exist and could not be created or if the metadata tables could
/// not be created, [`DeltaTableError`] is returned.
async fn create_local_data_folder(local_data_folder: &str) -> Result<DataFolder, DeltaTableError> {
    let delta_lake = DeltaLake::try_from_local_path(local_data_folder)?;

    let table_metadata_manager =
        TableMetadataManager::try_from_path(Path::from(local_data_folder)).await?;

    Ok(DataFolder::new(
        Arc::new(delta_lake),
        Arc::new(table_metadata_manager),
    ))
}

/// Return a [`DataFolder`] created from `connection_info`. If the connection information could not
/// be parsed or if the metadata tables could not be created, [`DeltaTableError`] is returned.
async fn create_remote_data_folder(connection_info: &[u8]) -> Result<DataFolder, DeltaTableError> {
    let remote_delta_lake = DeltaLake::try_remote_from_connection_info(connection_info).await?;

    let remote_table_metadata_manager =
        TableMetadataManager::try_from_connection_info(connection_info).await?;

    Ok(DataFolder::new(
        Arc::new(remote_delta_lake),
        Arc::new(remote_table_metadata_manager),
    ))
}

/// Folders for storing metadata and Apache Parquet files locally and remotely.
pub struct DataFolders {
    /// Folder for storing metadata and Apache Parquet files on the local file system.
    pub local_data_folder: DataFolder,
    /// Folder for storing Apache Parquet files in a remote object store.
    pub remote_data_folder: Option<DataFolder>,
    /// Folder from which Apache Parquet files will be read during query execution. It is equivalent
    /// to `local_data_folder` when deployed on the edge and `remote_data_folder` when deployed
    /// in the cloud.
    pub query_data_folder: DataFolder,
}

impl DataFolders {
    pub fn new(
        local_data_folder: DataFolder,
        remote_data_folder: Option<DataFolder>,
        query_data_folder: DataFolder,
    ) -> Self {
        Self {
            local_data_folder,
            remote_data_folder,
            query_data_folder,
        }
    }

