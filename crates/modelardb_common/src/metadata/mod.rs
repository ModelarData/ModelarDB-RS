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

//! Common metadata functionality shared between the server metadata manager and the manager
//! metadata manager.

use sqlx::{Error, Executor, SqlitePool};

pub mod model_table_metadata;

/// Common metadata functionality used to save model table metadata in both the server metadata
/// manager and the manager metadata manager.
#[derive(Clone)]
pub struct CommonMetadataManager {}

impl CommonMetadataManager {
    // TODO: Move because both the server and manager need to create these base tables.
    /// If they do not already exist, create the tables used for table and model table metadata.
    /// * The table_metadata table contains the metadata for tables.
    /// * The model_table_metadata table contains the main metadata for model tables.
    /// * The model_table_hash_table_name contains a mapping from each tag hash to the name of the
    /// model table that contains the time series with that tag hash.
    /// * The model_table_field_columns table contains the name, index, error bound, and generation
    /// expression of the field columns in each model table.
    /// If the tables exist or were created, return [`Ok`], otherwise return [`Error`].
    pub async fn create_metadata_database_tables(
        metadata_database_pool: &SqlitePool,
    ) -> Result<(), Error> {
        // Create the table_metadata SQLite table if it does not exist.
        metadata_database_pool
            .execute(
                "CREATE TABLE IF NOT EXISTS table_metadata (
                table_name TEXT PRIMARY KEY
                ) STRICT",
            )
            .await?;

        // Create the model_table_metadata SQLite table if it does not exist.
        metadata_database_pool
            .execute(
                "CREATE TABLE IF NOT EXISTS model_table_metadata (
                table_name TEXT PRIMARY KEY,
                query_schema BLOB NOT NULL
                ) STRICT",
            )
            .await?;

        // Create the model_table_hash_name SQLite table if it does not exist.
        metadata_database_pool
            .execute(
                "CREATE TABLE IF NOT EXISTS model_table_hash_table_name (
                hash INTEGER PRIMARY KEY,
                table_name TEXT
                ) STRICT",
            )
            .await?;

        // Create the model_table_field_columns SQLite table if it does not
        // exist. Note that column_index will only use a maximum of 10 bits.
        // generated_column_* is NULL if the fields are stored as segments.
        metadata_database_pool
            .execute(
                "CREATE TABLE IF NOT EXISTS model_table_field_columns (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                column_index INTEGER NOT NULL,
                error_bound REAL NOT NULL,
                generated_column_expr TEXT,
                generated_column_sources BLOB,
                PRIMARY KEY (table_name, column_name)
                ) STRICT",
            )
            .await?;

        Ok(())
    }
}
