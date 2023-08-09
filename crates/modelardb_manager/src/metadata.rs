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

use modelardb_common::metadata;
use sqlx::PgPool;

/// Store's the metadata required for reading from and writing to the tables and model tables and
/// persisting edges. The data that needs to be persisted are stored in the metadata database.
#[derive(Clone)]
pub struct MetadataManager {
    /// Pool of connections to the metadata database.
    metadata_database_pool: PgPool,
}

impl MetadataManager {
    /// Return [`MetadataManager`] if the necessary tables could be created in the metadata database,
    /// otherwise return [`sqlx::Error`].
    pub async fn try_new(metadata_database_pool: PgPool) -> Result<Self, sqlx::Error> {
        // Create the necessary tables in the metadata database.
        metadata::create_metadata_database_tables(
            &metadata_database_pool,
            metadata::MetadataDatabaseType::PostgreSQL,
        )
        .await?;

        Ok(Self {
            metadata_database_pool,
        })
    }

    /// Save the created table to the metadata database. This consists of adding a row to the
    /// table_metadata table with the `name` of the created table.
    pub async fn save_table_metadata(&self, name: &str) -> Result<(), sqlx::Error> {
        metadata::save_table_metadata(&self.metadata_database_pool, name.to_string()).await?;

        Ok(())
    }
}
