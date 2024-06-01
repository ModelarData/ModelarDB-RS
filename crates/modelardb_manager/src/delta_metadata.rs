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

use deltalake::DeltaTableError;

use modelardb_common::metadata::table_metadata_manager::TableMetadataManager;
use modelardb_common::metadata::MetadataDeltaLake;

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
        Ok(())
    }
}
