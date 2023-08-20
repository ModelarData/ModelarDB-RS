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
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use sqlx::{Executor, PgPool};

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
        metadata::save_table_metadata(&self.metadata_database_pool, name.to_string()).await
    }

    // TODO: Move to common metadata manager to avoid duplicated code.
    /// Save the created model table to the metadata database. This includes creating a tags table
    /// for the model table, creating a compressed files table for the model table, adding a row to
    /// the model_table_metadata table, and adding a row to the model_table_field_columns table for
    /// each field column.
    pub async fn save_model_table_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
    ) -> Result<(), sqlx::Error> {
        // Convert the query schema to bytes so it can be saved as a bytea in the metadata database.
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
                    "CREATE TABLE {}_compressed_files (file_name bytea PRIMARY KEY, field_column INTEGER,
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
                    "INSERT INTO model_table_metadata (table_name, query_schema) VALUES ($1, $2)",
                )
                .bind(model_table_metadata.name.as_str())
                .bind(query_schema_bytes),
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
}
