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

use modelardb_common::metadata::table_metadata_manager::TableMetadataManager;
use modelardb_common::storage::DeltaLake;

/// Folder for storing metadata and Apache Parquet files.
#[derive(Clone)]
struct DataFolder {
    /// Delta Lake for storing metadata and Apache Parquet files.
    delta_lake: Arc<DeltaLake>,
    /// Metadata manager for providing access to metadata related to tables
    table_metadata_manager: Arc<TableMetadataManager>,
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

    pub fn delta_lake(&self) -> Arc<DeltaLake> {
        self.delta_lake.clone()
    }

    pub fn table_metadata_manager(&self) -> Arc<TableMetadataManager> {
        self.table_metadata_manager.clone()
    }
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
