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

use std::str::FromStr;

use futures::{StreamExt, TryStreamExt};
use modelardb_common::errors::ModelarDbError;
use modelardb_common::metadata;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::types::ServerMode;
use sqlx::{Executor, PgPool, Row};

use crate::cluster::Node;

/// Stores the metadata required for reading from and writing to the tables and model tables and
/// persisting edges. The data that needs to be persisted is stored in the metadata database.
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

        MetadataManager::create_manager_metadata_database_tables(&metadata_database_pool).await?;

        Ok(Self {
            metadata_database_pool,
        })
    }

    /// If they do not already exist, create the tables that are specific to the manager metadata
    /// database.
    /// * The nodes table contains metadata for each node that is controlled by the manager.
    /// If the tables exist or were created, return [`Ok`], otherwise return [`sqlx::Error`].
    async fn create_manager_metadata_database_tables(
        metadata_database_pool: &PgPool,
    ) -> Result<(), sqlx::Error> {
        // Create the nodes table if it does not exist.
        metadata_database_pool
            .execute(
                "CREATE TABLE IF NOT EXISTS nodes (
                url TEXT PRIMARY KEY,
                mode TEXT NOT NULL
                )",
            )
            .await?;

        Ok(())
    }

    /// Save the created table to the metadata database. This consists of adding a row to the
    /// table_metadata table with the `name` of the table and the `sql` used to create the table.
    pub async fn save_table_metadata(&self, name: String, sql: String) -> Result<(), sqlx::Error> {
        metadata::save_table_metadata(&self.metadata_database_pool, name, sql).await
    }

    // TODO: Move to common metadata manager to avoid duplicated code when the issue with generic
    //       functions that use transactions is fixed.
    /// Save the created model table to the metadata database. This includes creating a tags table
    /// for the model table, creating a compressed files table for the model table, adding a row to
    /// the model_table_metadata table, and adding a row to the model_table_field_columns table for
    /// each field column.
    pub async fn save_model_table_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
        sql: &str,
    ) -> Result<(), sqlx::Error> {
        // Convert the query schema to bytes so it can be saved as a BYTEA in the metadata database.
        let query_schema_bytes =
            metadata::try_convert_schema_to_blob(&model_table_metadata.query_schema)?;

        // Create a transaction to ensure the database state is consistent across tables.
        let mut transaction = self.metadata_database_pool.begin().await?;

        // Add a column definition for each tag column in the query schema.
        let tag_columns: String = model_table_metadata
            .tag_column_indices
            .iter()
            .map(|index| {
                let field = model_table_metadata.query_schema.field(*index);
                format!("{} TEXT NOT NULL", field.name())
            })
            .collect::<Vec<String>>()
            .join(",");

        // Create a table_name_tags table to save the 54-bit tag hashes when ingesting data.
        let maybe_separator = if tag_columns.is_empty() { "" } else { ", " };
        transaction
            .execute(
                format!(
                    "CREATE TABLE {}_tags (hash INTEGER PRIMARY KEY{}{})",
                    model_table_metadata.name, maybe_separator, tag_columns
                )
                .as_str(),
            )
            .await?;

        // Create a table_name_compressed_files table to save the name of the table's files.
        transaction
            .execute(
                format!(
                    "CREATE TABLE {}_compressed_files (file_name BYTEA PRIMARY KEY, field_column INTEGER,
                     start_time INTEGER, end_time INTEGER, min_value REAL, max_value REAL)",
                    model_table_metadata.name
                )
                    .as_str(),
            )
            .await?;

        // Add a new row in the model_table_metadata table to persist the model table.
        transaction
            .execute(
                sqlx::query(
                    "INSERT INTO model_table_metadata (table_name, query_schema, sql) VALUES ($1, $2, $3)",
                )
                .bind(model_table_metadata.name.as_str())
                .bind(query_schema_bytes)
                .bind(sql),
            )
            .await?;

        // Add a row for each field column to the model_table_field_columns table.
        let insert_statement =
            "INSERT INTO model_table_field_columns (table_name, column_name, column_index,
             error_bound, generated_column_expr, generated_column_sources)
             VALUES ($1, $2, $3, $4, $5, $6)";

        for (query_schema_index, field) in model_table_metadata
            .query_schema
            .fields()
            .iter()
            .enumerate()
        {
            // Only add a row for the field if it is not the timestamp or a tag.
            let is_timestamp = query_schema_index == model_table_metadata.timestamp_column_index;
            let in_tag_indices = model_table_metadata
                .tag_column_indices
                .contains(&query_schema_index);

            if !is_timestamp && !in_tag_indices {
                let (generated_column_expr, generated_column_sources) =
                    if let Some(generated_column) =
                        &model_table_metadata.generated_columns[query_schema_index]
                    {
                        (
                            Some(generated_column.original_expr.clone()),
                            Some(metadata::convert_slice_usize_to_vec_u8(
                                &generated_column.source_columns,
                            )),
                        )
                    } else {
                        (None, None)
                    };

                // error_bounds matches schema and not query_schema to simplify looking up the error
                // bound during ingestion as it occurs far more often than creation of model tables.
                let error_bound =
                    if let Ok(schema_index) = model_table_metadata.schema.index_of(field.name()) {
                        model_table_metadata.error_bounds[schema_index].into_inner()
                    } else {
                        0.0
                    };

                // query_schema_index is simply cast as a model table contains at most 1024 columns.
                sqlx::query(insert_statement)
                    .bind(&model_table_metadata.name)
                    .bind(field.name())
                    .bind(query_schema_index as i64)
                    .bind(error_bound)
                    .bind(generated_column_expr)
                    .bind(generated_column_sources)
                    .execute(&mut *transaction)
                    .await?;
            }
        }

        transaction.commit().await
    }

    /// Retrieve the names of all tables in the metadata database, including both normal tables and
    /// model tables and return them. If the table names could not be retrieved, return [`sqlx::Error`].
    pub async fn table_names(&self) -> Result<Vec<String>, sqlx::Error> {
        let mut table_names: Vec<String> = vec![];

        // Retrieve the table_name column from both tables containing table metadata.
        let table_rows = sqlx::query("SELECT table_name FROM table_metadata")
            .fetch(&self.metadata_database_pool);
        let model_table_rows = sqlx::query("SELECT table_name FROM model_table_metadata")
            .fetch(&self.metadata_database_pool);

        let mut rows = table_rows.chain(model_table_rows);

        while let Some(row) = rows.try_next().await? {
            table_names.push(row.try_get("table_name")?)
        }

        Ok(table_names)
    }

    /// Save the node to the metadata database and return [`Ok`]. If the node could not be saved,
    /// return [`sqlx::Error`].
    pub async fn save_node(&self, node: &Node) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO nodes (url, mode) VALUES ($1, $2)")
            .bind(&node.url)
            .bind(node.mode.to_string())
            .execute(&self.metadata_database_pool)
            .await?;

        Ok(())
    }

    /// Remove the row in the nodes table that corresponds to the node with `url` and return [`Ok`].
    /// If the row could not be removed, return [`sqlx::Error`].
    pub async fn remove_node(&self, url: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM nodes WHERE url = $1")
            .bind(url)
            .execute(&self.metadata_database_pool)
            .await?;

        Ok(())
    }

    /// Return the nodes currently controlled by the manager that have been persisted to the
    /// metadata database. If the nodes could not be retrieved, [`sqlx::Error`] is returned.
    pub async fn nodes(&self) -> Result<Vec<Node>, sqlx::Error> {
        let mut nodes: Vec<Node> = vec![];

        let mut rows =
            sqlx::query("SELECT url, mode FROM nodes").fetch(&self.metadata_database_pool);

        while let Some(row) = rows.try_next().await? {
            let server_mode = ServerMode::from_str(row.try_get("mode")?).map_err(|error| {
                sqlx::Error::ColumnDecode {
                    index: "mode".to_string(),
                    source: Box::new(ModelarDbError::DataRetrievalError(error.to_string())),
                }
            })?;

            nodes.push(Node::new(row.try_get("url")?, server_mode))
        }

        Ok(nodes)
    }
}
