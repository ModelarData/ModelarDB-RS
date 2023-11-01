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

//! Table metadata manager that includes functionality shared between the server metadata database
//! and the manager metadata database. Note that the entire server metadata database can be accessed
//! through this metadata manager, while it only supports a subset of the manager metadata database.

mod compressed_file;
pub mod model_table_metadata;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::mem;

use arrow_flight::{IpcMessage, SchemaAsIpc};
use dashmap::DashMap;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::{error::ArrowError, ipc::writer::IpcWriteOptions};
use futures::TryStreamExt;
use sqlx::{AnyPool, Database, Error, Executor, Row};

use crate::errors::ModelarDbError;
use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::types::UnivariateId;

/// The database providers that are currently supported by the metadata database.
#[derive(Clone)]
pub enum MetadataDatabaseType {
    SQLite,
    PostgreSQL,
}

/// Stores the metadata required for reading from and writing to the tables and model tables.
/// The data that needs to be persisted is stored in the metadata database.
#[derive(Clone)]
pub struct TableMetadataManager {
    /// The type of the database, used to handle small differences in SQL syntax between providers.
    metadata_database_type: MetadataDatabaseType,
    /// Pool of connections to the metadata database.
    metadata_database_pool: AnyPool,
    /// Cache of tag value hashes used to signify when to persist new unsaved tag combinations.
    tag_value_hashes: DashMap<String, u64>,
}

impl TableMetadataManager {
    /// Create a [`TableMetadataManager`] and initialize the metadata database with the tables used for
    /// table and model table metadata. If the tables could not be created, [`Error`] is returned.
    pub async fn try_new(
        metadata_database_type: MetadataDatabaseType,
        metadata_database_pool: AnyPool,
    ) -> Result<Self, Error> {
        let metadata_manager = Self {
            metadata_database_type,
            metadata_database_pool,
            tag_value_hashes: DashMap::new(),
        };

        // Create the necessary tables in the metadata database.
        metadata_manager.create_metadata_database_tables().await?;

        Ok(metadata_manager)
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
    pub async fn create_metadata_database_tables(&self) -> Result<(), Error> {
        let (strict, binary_type) = match self.metadata_database_type {
            MetadataDatabaseType::SQLite => ("STRICT", "BLOB"),
            MetadataDatabaseType::PostgreSQL => ("", "BYTEA"),
        };

        // Create the table_metadata table if it does not exist.
        self.metadata_database_pool
            .execute(&*format!(
                "CREATE TABLE IF NOT EXISTS table_metadata (
                     table_name TEXT PRIMARY KEY,
                     sql TEXT NOT NULL
                 ) {strict}"
            ))
            .await?;

        // Create the model_table_metadata table if it does not exist.
        self.metadata_database_pool
            .execute(&*format!(
                "CREATE TABLE IF NOT EXISTS model_table_metadata (
                     table_name TEXT PRIMARY KEY,
                     query_schema {binary_type} NOT NULL,
                     sql TEXT NOT NULL
                 ) {strict}"
            ))
            .await?;

        // Create the model_table_hash_name table if it does not exist.
        self.metadata_database_pool
            .execute(&*format!(
                "CREATE TABLE IF NOT EXISTS model_table_hash_table_name (
                     hash INTEGER PRIMARY KEY,
                     table_name TEXT
                 ) {strict}"
            ))
            .await?;

        // Create the model_table_field_columns table if it does not exist. Note that column_index will
        // only use a maximum of 10 bits. generated_column_* is NULL if the fields are stored as segments.
        self.metadata_database_pool
            .execute(&*format!(
                "CREATE TABLE IF NOT EXISTS model_table_field_columns (
                     table_name TEXT NOT NULL,
                     column_name TEXT NOT NULL,
                     column_index INTEGER NOT NULL,
                     error_bound REAL NOT NULL,
                     generated_column_expr TEXT,
                     generated_column_sources {binary_type},
                     PRIMARY KEY (table_name, column_name)
                 ) {strict}"
            ))
            .await?;

        Ok(())
    }

    /// Return the tag hash for the given list of tag values either by retrieving it from a cache
    /// or, if the combination of tag values is not in the cache, by computing a new hash. If the
    /// hash is not in the cache, it is both saved to the cache, persisted to the model_table_tags
    /// table if it does not already contain it, and persisted to the model_table_hash_table_name if
    /// it does not already contain it. If the model_table_tags or the model_table_hash_table_name
    /// table cannot be accessed, [`Error`] is returned.
    pub async fn lookup_or_compute_tag_hash(
        &self,
        model_table_metadata: &ModelTableMetadata,
        tag_values: &[String],
    ) -> Result<u64, Error> {
        let cache_key = {
            let mut cache_key_list = tag_values.to_vec();
            cache_key_list.push(model_table_metadata.name.clone());

            cache_key_list.join(";")
        };

        // Check if the tag hash is in the cache. If it is, retrieve it. If it is not, create a new
        // one and save it both in the cache and in the model_table_tags table. There is a minor
        // race condition because the check if a tag hash is in the cache and the addition of the
        // hash is done without taking a lock on the tag_value_hashes. However, by allowing a hash
        // to possible be computed more than once, the cache can be used without an explicit lock.
        if let Some(tag_hash) = self.tag_value_hashes.get(&cache_key) {
            Ok(*tag_hash)
        } else {
            // Generate the 54-bit tag hash based on the tag values of the record batch and model
            // table name.
            let tag_hash = {
                let mut hasher = DefaultHasher::new();
                hasher.write(cache_key.as_bytes());

                // The 64-bit hash is shifted to make the 10 least significant bits 0.
                hasher.finish() << 10
            };

            // Save the tag hash in the cache and in the metadata database model_table_tags table.
            self.tag_value_hashes.insert(cache_key, tag_hash);

            // tag_column_indices are computed with from the schema so they can be used with input.
            let tag_columns: String = model_table_metadata
                .tag_column_indices
                .iter()
                .map(|index| model_table_metadata.schema.field(*index).name().clone())
                .collect::<Vec<String>>()
                .join(",");

            let values = tag_values
                .iter()
                .map(|value| format!("'{value}'"))
                .collect::<Vec<String>>()
                .join(",");

            // Create a transaction to ensure the database state is consistent across tables.
            let mut transaction = self.metadata_database_pool.begin().await?;

            // SQLite (https://www.sqlite.org/datatype3.html) and PostgreSQL
            // (https://www.postgresql.org/docs/current/datatype.html) both use signed integers.
            let signed_tag_hash = i64::from_ne_bytes(tag_hash.to_ne_bytes());

            // ON CONFLICT DO NOTHING is used to silently fail when trying to insert an already
            // existing hash. This purposely occurs if the hash has already been written to the
            // metadata database but is no longer stored in the cache, e.g., if the system has
            // been restarted.
            let maybe_separator = if tag_columns.is_empty() { "" } else { ", " };
            transaction
                .execute(&*format!(
                    "INSERT INTO {}_tags (hash{}{}) VALUES ({}{}{}) ON CONFLICT DO NOTHING",
                    model_table_metadata.name,
                    maybe_separator,
                    tag_columns,
                    signed_tag_hash,
                    maybe_separator,
                    values,
                ))
                .await?;

            transaction
                .execute(&*format!(
                    "INSERT INTO model_table_hash_table_name (hash, table_name) VALUES ({}, '{}')
                     ON CONFLICT DO NOTHING",
                    signed_tag_hash, model_table_metadata.name
                ))
                .await?;

            transaction.commit().await?;

            Ok(tag_hash)
        }
    }

    /// Extract the first 54-bits from `univariate_id` which is a hash computed from tags.
    pub fn univariate_id_to_tag_hash(univariate_id: u64) -> u64 {
        univariate_id & 18446744073709550592
    }

    /// Extract the last 10-bits from `univariate_id` which is the index of the time series column.
    pub fn univariate_id_to_column_index(univariate_id: u64) -> u16 {
        (univariate_id & 1023) as u16
    }

    /// Return the name of the table that contains the time series with `univariate_id`. Returns an
    /// [`Error`] if the necessary data cannot be retrieved from the metadata database.
    pub async fn univariate_id_to_table_name(&self, univariate_id: u64) -> Result<String, Error> {
        // SQLite (https://www.sqlite.org/datatype3.html) and PostgreSQL
        // (https://www.postgresql.org/docs/current/datatype.html) both use signed integers.
        let tag_hash = Self::univariate_id_to_tag_hash(univariate_id);
        let signed_tag_hash = i64::from_ne_bytes(tag_hash.to_ne_bytes());

        let select_statement = format!(
            "SELECT table_name FROM model_table_hash_table_name WHERE hash = {signed_tag_hash}",
        );

        self.metadata_database_pool
            .fetch_one(&*select_statement)
            .await?
            .try_get(0)
    }

    /// Return a mapping from tag hashes to the tags in the columns with the names in
    /// `tag_column_names` for the time series in the model table with the name `model_table_name`.
    /// Returns an [`Error`] if the necessary data cannot be retrieved from the metadata database.
    pub async fn mapping_from_hash_to_tags(
        &self,
        model_table_name: &str,
        tag_column_names: &Vec<&str>,
    ) -> Result<HashMap<u64, Vec<String>>, Error> {
        // Return an empty HashMap if no tag column names are passed to keep the signature simple.
        if tag_column_names.is_empty() {
            return Ok(HashMap::new());
        }

        let select_statement = format!(
            "SELECT hash,{} FROM {model_table_name}_tags",
            tag_column_names.join(","),
        );

        let mut rows = self.metadata_database_pool.fetch(&*select_statement);

        let mut hash_to_tags = HashMap::new();
        while let Some(row) = rows.try_next().await? {
            // SQLite (https://www.sqlite.org/datatype3.html) and PostgreSQL
            // (https://www.postgresql.org/docs/current/datatype.html) both use signed integers.
            let signed_tag_hash: i64 = row.try_get(0)?;
            let tag_hash = u64::from_ne_bytes(signed_tag_hash.to_ne_bytes());

            // Add all of the tags in order so they can be directly appended to each row.
            let mut tags = Vec::with_capacity(tag_column_names.len());
            for tag_column_index in 1..=tag_column_names.len() {
                tags.push(row.try_get(tag_column_index)?);
            }

            hash_to_tags.insert(tag_hash, tags);
        }

        Ok(hash_to_tags)
    }

    /// Compute the 64-bit univariate ids of the univariate time series to retrieve from the storage
    /// engine using the field columns, tag names, and tag values in the query. Returns a [`Error`]
    /// if the necessary data cannot be retrieved from the metadata database.
    #[allow(dead_code)]
    pub async fn compute_univariate_ids_using_fields_and_tags(
        &self,
        table_name: &str,
        columns: Option<&Vec<usize>>,
        fallback_field_column: u64,
        tag_predicates: &[(&str, &str)],
    ) -> Result<Vec<UnivariateId>, Error> {
        // Construct a query that extracts the field columns in the table being queried which
        // overlaps with the columns being requested by the query.
        let query_field_columns = if let Some(columns) = columns {
            let column_predicates: Vec<String> = columns
                .iter()
                .map(|column| format!("column_index = {column}"))
                .collect();

            format!(
                "SELECT column_index FROM model_table_field_columns WHERE table_name = '{}' AND {}",
                table_name,
                column_predicates.join(" OR ")
            )
        } else {
            format!(
                "SELECT column_index FROM model_table_field_columns WHERE table_name = '{table_name}'"
            )
        };

        // Construct a query that extracts the hashes of the multivariate time series in the table
        // with tag values that match those in the query.
        let query_hashes = {
            if tag_predicates.is_empty() {
                format!("SELECT hash FROM {}_tags", table_name)
            } else {
                let predicates: Vec<String> = tag_predicates
                    .iter()
                    .map(|(tag, tag_value)| format!("{tag} = '{tag_value}'"))
                    .collect();

                format!(
                    "SELECT hash FROM {}_tags WHERE {}",
                    table_name,
                    predicates.join(" AND ")
                )
            }
        };

        // Retrieve the hashes using the queries and reconstruct the univariate ids.
        self.compute_univariate_ids_using_metadata_database(
            &query_field_columns,
            fallback_field_column,
            &query_hashes,
        )
        .await
    }

    /// Compute the 64-bit univariate ids of the univariate time series to retrieve from the storage
    /// engine using the two queries constructed from the fields, tag names, and tag values in the
    /// user's query. Returns the univariate ids if successful, otherwise, if the data cannot be
    /// retrieved from the metadata database, [`Error`] is returned.
    async fn compute_univariate_ids_using_metadata_database(
        &self,
        query_field_columns: &str,
        fallback_field_column: u64,
        query_hashes: &str,
    ) -> Result<Vec<u64>, Error> {
        // Retrieve the field columns.
        let mut rows = self.metadata_database_pool.fetch(query_field_columns);

        let mut field_columns = vec![];
        while let Some(row) = rows.try_next().await? {
            // SQLite (https://www.sqlite.org/datatype3.html) and PostgreSQL
            // (https://www.postgresql.org/docs/current/datatype.html) both use signed integers.
            let signed_field_column: i64 = row.try_get(0)?;
            let field_column = u64::from_ne_bytes(signed_field_column.to_ne_bytes());

            field_columns.push(field_column);
        }

        // Add the fallback field column if the query did not request data for
        // any fields as the storage engine otherwise does not return any data.
        if field_columns.is_empty() {
            field_columns.push(fallback_field_column);
        }

        // Retrieve the hashes and compute the univariate ids;
        let mut rows = self.metadata_database_pool.fetch(query_hashes);

        let mut univariate_ids = vec![];
        while let Some(row) = rows.try_next().await? {
            // SQLite (https://www.sqlite.org/datatype3.html) and PostgreSQL
            // (https://www.postgresql.org/docs/current/datatype.html) both use signed integers.
            let signed_tag_hash: i64 = row.try_get(0)?;
            let tag_hash = u64::from_ne_bytes(signed_tag_hash.to_ne_bytes());

            for field_column in &field_columns {
                univariate_ids.push(tag_hash | field_column);
            }
        }

        Ok(univariate_ids)
    }
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
