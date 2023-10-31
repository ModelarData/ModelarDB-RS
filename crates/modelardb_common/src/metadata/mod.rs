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

//! Common metadata manager including functionality shared between the server metadata database and
//! the manager metadata database.

pub mod model_table_metadata;
mod compressed_file;

use std::mem;

use arrow_flight::{IpcMessage, SchemaAsIpc};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::{error::ArrowError, ipc::writer::IpcWriteOptions};
use sqlx::{Database, Error, Executor};

use crate::errors::ModelarDbError;

/// The database providers that are currently supported by the metadata database.
pub enum MetadataDatabaseType {
    SQLite,
    PostgreSQL,
}

/// If they do not already exist, create the tables in the metadata database used for table and
/// model table metadata.
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
    let (strict, binary_type) = match database_type {
        MetadataDatabaseType::SQLite => ("STRICT", "BLOB"),
        MetadataDatabaseType::PostgreSQL => ("", "BYTEA"),
    };

    // Create the table_metadata table if it does not exist.
    metadata_database_pool
        .execute(
            format!(
                "CREATE TABLE IF NOT EXISTS table_metadata (
                table_name TEXT PRIMARY KEY,
                sql TEXT NOT NULL
                ) {strict}"
            )
            .as_str(),
        )
        .await?;

    // Create the model_table_metadata table if it does not exist.
    metadata_database_pool
        .execute(
            format!(
                "CREATE TABLE IF NOT EXISTS model_table_metadata (
                table_name TEXT PRIMARY KEY,
                query_schema {binary_type} NOT NULL,
                sql TEXT NOT NULL
                ) {strict}"
            )
            .as_str(),
        )
        .await?;

    // Create the model_table_hash_name table if it does not exist.
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

    // Create the model_table_field_columns table if it does not exist. Note that column_index will
    // only use a maximum of 10 bits. generated_column_* is NULL if the fields are stored as segments.
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
/// table_metadata table with the `name` of the table and the `sql` used to create the table.
pub async fn save_table_metadata<'a, DB, E>(
    metadata_database_pool: E,
    name: &str,
    sql: &str,
) -> Result<(), Error>
where
    DB: Database,
    E: Executor<'a, Database = DB> + Copy,
{
    // Add a new row in the table_metadata table to persist the table.
    metadata_database_pool
        .execute(
            format!("INSERT INTO table_metadata (table_name, sql) VALUES ('{name}', '{sql}')")
                .as_str(),
        )
        .await?;

    Ok(())
}

/// Convert a [`Schema`] to [`Vec<u8>`].
pub fn try_convert_schema_to_blob(schema: &Schema) -> Result<Vec<u8>, Error> {
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

/// Return [`Schema`] if `schema_bytes` can be converted to an Apache Arrow schema, otherwise
/// [`Error`].
pub fn try_convert_blob_to_schema(schema_bytes: Vec<u8>) -> Result<Schema, Error> {
    let ipc_message = IpcMessage(schema_bytes.into());
    Schema::try_from(ipc_message).map_err(|error| Error::ColumnDecode {
        index: "query_schema".to_owned(),
        source: Box::new(error),
    })
}

/// Convert a [`&[usize]`] to a [`Vec<u8>`].
pub fn convert_slice_usize_to_vec_u8(usizes: &[usize]) -> Vec<u8> {
    usizes.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// Convert a [`&[u8]`] to a [`Vec<usize>`] if the length of `bytes` divides evenly by
/// [`mem::size_of::<usize>()`], otherwise [`Error`] is returned.
pub fn try_convert_slice_u8_to_vec_usize(bytes: &[u8]) -> Result<Vec<usize>, Error> {
    if bytes.len() % mem::size_of::<usize>() != 0 {
        Err(Error::ColumnDecode {
            index: "generated_column_sources".to_owned(),
            source: Box::new(ModelarDbError::ImplementationError(
                "Blob is not a vector of usizes".to_owned(),
            )),
        })
    } else {
        // unwrap() is safe as bytes divides evenly by mem::size_of::<usize>().
        Ok(bytes
            .chunks(mem::size_of::<usize>())
            .map(|byte_slice| usize::from_le_bytes(byte_slice.try_into().unwrap()))
            .collect())
    }
}

/// Normalize `name` to allow direct comparisons between names.
pub fn normalize_name(name: &str) -> String {
    name.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;
    use std::sync::Arc;

    use datafusion::arrow::array::ArrowPrimitiveType;
    use datafusion::arrow::datatypes::{Field, Schema};
    use futures::TryStreamExt;
    use proptest::{collection, num, prop_assert_eq, proptest};
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::{Row, SqlitePool};

    use crate::types::ArrowValue;

    // Tests for metadata database functions.
    #[tokio::test]
    async fn test_create_metadata_database_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_database_pool = connect_to_metadata_database(temp_dir.path()).await;

        create_metadata_database_tables(&metadata_database_pool, MetadataDatabaseType::SQLite)
            .await
            .unwrap();

        // Verify that the tables were created and has the expected columns.
        metadata_database_pool
            .execute("SELECT table_name FROM table_metadata")
            .await
            .unwrap();

        metadata_database_pool
            .execute("SELECT table_name, query_schema FROM model_table_metadata")
            .await
            .unwrap();

        metadata_database_pool
            .execute("SELECT hash, table_name FROM model_table_hash_table_name")
            .await
            .unwrap();

        metadata_database_pool
            .execute(
                "SELECT table_name, column_name, column_index, error_bound, generated_column_expr,
                 generated_column_sources FROM model_table_field_columns",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_save_table_metadata() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_database_pool = connect_to_metadata_database(temp_dir.path()).await;

        create_metadata_database_tables(&metadata_database_pool, MetadataDatabaseType::SQLite)
            .await
            .unwrap();

        let table_name = "table_name";
        let sql = "CREATE TABLE table_name(timestamp TIMESTAMP, values REAL, metadata REAL)";

        save_table_metadata(&metadata_database_pool, table_name, sql)
            .await
            .unwrap();

        // Retrieve the table from the metadata database.
        let mut rows = metadata_database_pool.fetch("SELECT table_name, sql FROM table_metadata");

        let row = rows.try_next().await.unwrap().unwrap();
        let retrieved_table_name = row.try_get::<&str, _>(0).unwrap();
        assert_eq!(table_name, retrieved_table_name);

        let retrieved_sql = row.try_get::<&str, _>(1).unwrap();
        assert_eq!(sql, retrieved_sql);

        assert!(rows.try_next().await.unwrap().is_none());
    }

    async fn connect_to_metadata_database(path: &Path) -> SqlitePool {
        let options = SqliteConnectOptions::new()
            .filename(path.join("metadata.sqlite3"))
            .create_if_missing(true);

        SqlitePool::connect_with(options).await.unwrap()
    }

    // Tests for conversion functions.
    #[test]
    fn test_invalid_blob_to_schema() {
        assert!(try_convert_blob_to_schema(vec!(1, 2, 4, 8)).is_err());
    }

    #[test]
    fn test_blob_to_schema_and_schema_to_blob() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
        ]));

        // Serialize a schema to bytes.
        let bytes = try_convert_schema_to_blob(&schema).unwrap();

        // Deserialize the bytes to a schema.
        let retrieved_schema = try_convert_blob_to_schema(bytes).unwrap();
        assert_eq!(*schema, retrieved_schema);
    }

    proptest! {
        #[test]
        fn test_usize_to_u8_and_u8_to_usize(values in collection::vec(num::usize::ANY, 0..50)) {
            let bytes = convert_slice_usize_to_vec_u8(&values);
            let usizes = try_convert_slice_u8_to_vec_usize(&bytes).unwrap();
            prop_assert_eq!(values, usizes);
        }
    }

    // Tests for normalize_name().
    #[test]
    fn test_normalize_table_name_lowercase_no_effect() {
        assert_eq!("table_name", normalize_name("table_name"));
    }

    #[test]
    fn test_normalize_table_name_uppercase() {
        assert_eq!("table_name", normalize_name("TABLE_NAME"));
    }

    #[test]
    fn test_normalize_table_name_mixed_case() {
        assert_eq!("table_name", normalize_name("Table_Name"));
    }
}
