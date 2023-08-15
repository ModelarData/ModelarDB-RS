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

use arrow_flight::{IpcMessage, SchemaAsIpc};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::{error::ArrowError, ipc::writer::IpcWriteOptions};
use sqlx::database::HasArguments;
use sqlx::{Database, Error, Executor, IntoArguments};

pub mod model_table_metadata;

/// The database providers that are currently supported by the metadata database.
pub enum MetadataDatabaseType {
    SQLite,
    PostgreSQL,
}

/// If they do not already exist, create the tables used for table and model table metadata.
/// * The table_metadata table contains the metadata for tables.
/// * The model_table_metadata table contains the main metadata for model tables.
/// * The model_table_hash_table_name contains a mapping from each tag hash to the name of the
/// model table that contains the time series with that tag hash.
/// * The model_table_field_columns table contains the name, index, error bound, and generation
/// expression of the field columns in each model table.
/// If the tables exist or were created, return [`Ok`], otherwise return [`Error`].
pub async fn create_metadata_database_tables<'a, DB, E>(
    metadata_database_pool: E,
    database_type: MetadataDatabaseType,
) -> Result<(), Error>
where
    DB: Database,
    E: Executor<'a, Database = DB> + Copy,
{
    let strict = match database_type {
        MetadataDatabaseType::SQLite => "STRICT",
        _ => "",
    };

    let binary_type = match database_type {
        MetadataDatabaseType::SQLite => "BLOB",
        _ => "bytea",
    };

    // Create the table_metadata SQLite table if it does not exist.
    metadata_database_pool
        .execute(
            format!(
                "CREATE TABLE IF NOT EXISTS table_metadata (
            table_name TEXT PRIMARY KEY
            ) {strict}"
            )
            .as_str(),
        )
        .await?;

    // Create the model_table_metadata SQLite table if it does not exist.
    metadata_database_pool
        .execute(
            format!(
                "CREATE TABLE IF NOT EXISTS model_table_metadata (
            table_name TEXT PRIMARY KEY,
            query_schema {binary_type} NOT NULL
            ) {strict}"
            )
            .as_str(),
        )
        .await?;

    // Create the model_table_hash_name SQLite table if it does not exist.
    metadata_database_pool
        .execute(
            format!(
                "CREATE TABLE IF NOT EXISTS model_table_hash_table_name (
            hash INTEGER PRIMARY KEY,
            table_name TEXT
            ) {strict}"
            )
            .as_str(),
        )
        .await?;

    // Create the model_table_field_columns SQLite table if it does not
    // exist. Note that column_index will only use a maximum of 10 bits.
    // generated_column_* is NULL if the fields are stored as segments.
    metadata_database_pool
        .execute(
            format!(
                "CREATE TABLE IF NOT EXISTS model_table_field_columns (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            column_index INTEGER NOT NULL,
            error_bound REAL NOT NULL,
            generated_column_expr TEXT,
            generated_column_sources {binary_type},
            PRIMARY KEY (table_name, column_name)
            ) {strict}"
            )
            .as_str(),
        )
        .await?;

    Ok(())
}

/// Save the created table to the metadata database. This consists of adding a row to the
/// table_metadata table with the `name` of the created table.
pub async fn save_table_metadata<'a, DB, E>(
    metadata_database_pool: E,
    name: String,
) -> Result<(), Error>
where
    DB: Database,
    E: Executor<'a, Database = DB> + Copy,
    String: sqlx::Encode<'a, DB>,
    String: sqlx::Type<DB>,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
{
    // Add a new row in the table_metadata table to persist the table.
    sqlx::query("INSERT INTO table_metadata (table_name) VALUES (?1)")
        .bind(name)
        .execute(metadata_database_pool)
        .await?;

    Ok(())
}

/// Convert a [`Schema`] to [`Vec<u8>`].
fn convert_schema_to_blob(schema: &Schema) -> Result<Vec<u8>, Error> {
    let options = IpcWriteOptions::default();
    let schema_as_ipc = SchemaAsIpc::new(schema, &options);
    let ipc_message: IpcMessage =
        schema_as_ipc
            .try_into()
            .map_err(|error: ArrowError| Error::ColumnDecode {
                index: "query_schema".to_owned(),
                source: Box::new(error),
            })?;
    Ok(ipc_message.0.to_vec())
}
