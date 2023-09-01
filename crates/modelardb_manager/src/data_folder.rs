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

//! Wrapper for [`ObjectStore`] to support saving the connection information with the remote data
//! folder.

use std::sync::Arc;

use object_store::ObjectStore;

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
