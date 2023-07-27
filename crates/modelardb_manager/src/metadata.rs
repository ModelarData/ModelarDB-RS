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

//! Management of the metadata database for the manager. Metadata which is unique to the manager,
//! such as metadata about registered edges, is handled here.

use sqlx::PgPool;
use modelardb_common::metadata::CommonMetadataManager;

/// Store's the metadata required for reading from and writing to the tables and model tables and
/// persisting edges. The data that needs to be persisted are stored in the metadata database.
#[derive(Clone)]
pub struct MetadataManager {
    /// Common functionality shared between this metadata manager and the server metadata manager.
    common_metadata_manager: CommonMetadataManager,
    /// Pool of connections to the metadata database.
    metadata_database_pool: PgPool,
}

impl MetadataManager {
    pub fn new(metadata_database_pool: PgPool) -> Self {
        Self {
            common_metadata_manager: CommonMetadataManager::new(),
            metadata_database_pool
        }
    }
}
