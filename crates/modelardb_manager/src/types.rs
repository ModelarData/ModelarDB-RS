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

//! Wrappers for [`ObjectStore`] and [`MetadataManager`] to support saving the connection
//! information with the object that the connection information is associated with.

use std::sync::Arc;

use object_store::ObjectStore;

use crate::metadata::MetadataManager;

/// Stores the connection information with the remote data folder to ensure that the information
/// is consistent with the remote data folder.
pub struct RemoteDataFolder {
    /// Connection information saved as bytes to make it possible to transfer the information using
    /// Arrow Flight.
    connection_info: Vec<u8>,
    /// Folder for storing Apache Parquet files in a remote object store.
    object_store: Arc<dyn ObjectStore>,
}

impl RemoteDataFolder {
    pub fn new(connection_info: Vec<u8>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            connection_info,
            object_store,
        }
    }

    pub fn connection_info(&self) -> &Vec<u8> {
        &self.connection_info
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }
}

/// Stores the connection information with the manager manager to ensure that the information
/// is consistent with the metadata manager.
pub struct RemoteMetadataManager {
    /// Connection information saved as bytes to make it possible to transfer the information using
    /// Arrow Flight.
    connection_info: Vec<u8>,
    /// Manager for the access to the remote metadata database.
    metadata_manager: MetadataManager,
}

impl RemoteMetadataManager {
    pub fn new(connection_info: Vec<u8>, metadata_manager: MetadataManager) -> Self {
        Self {
            connection_info,
            metadata_manager,
        }
    }

    pub fn connection_info(&self) -> &Vec<u8> {
        &self.connection_info
    }

    pub fn metadata_manager(&self) -> &MetadataManager {
        &self.metadata_manager
    }
}
