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
use chrono::{TimeZone, Utc};
use dashmap::DashMap;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::{error::ArrowError, ipc::writer::IpcWriteOptions};
use datafusion::common::DFSchema;
use futures::TryStreamExt;
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectMeta;
use sqlx::any::AnyRow;
use sqlx::database::HasArguments;
use sqlx::query::Query;
use sqlx::{AnyPool, Database, Error, Executor, Row};

use crate::errors::ModelarDbError;
use crate::metadata::compressed_file::CompressedFile;
use crate::metadata::model_table_metadata::{GeneratedColumn, ModelTableMetadata};
use crate::parser;
use crate::types::{ErrorBound, Timestamp, UnivariateId, Value};

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

        let mut transaction = self.metadata_database_pool.begin().await?;

        // Create the table_metadata table if it does not exist.
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS table_metadata (
                 table_name TEXT PRIMARY KEY,
                 sql TEXT NOT NULL
             ) {strict}"
        ))
        .execute(&mut *transaction)
        .await?;

        // Create the model_table_metadata table if it does not exist.
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS model_table_metadata (
                 table_name TEXT PRIMARY KEY,
                 query_schema {binary_type} NOT NULL,
                 sql TEXT NOT NULL
             ) {strict}"
        ))
        .execute(&mut *transaction)
        .await?;

        // Create the model_table_hash_name table if it does not exist.
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS model_table_hash_table_name (
                 hash INTEGER PRIMARY KEY,
                 table_name TEXT
             ) {strict}"
        ))
        .execute(&mut *transaction)
        .await?;

        // Create the model_table_field_columns table if it does not exist. Note that column_index will
        // only use a maximum of 10 bits. generated_column_* is NULL if the fields are stored as segments.
        sqlx::query(&format!(
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
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;

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

            let maybe_separator = if tag_columns.is_empty() { "" } else { ", " };
            let model_table_name = &model_table_metadata.name;

            // ON CONFLICT DO NOTHING is used to silently fail when trying to insert an already
            // existing hash. This purposely occurs if the hash has already been written to the
            // metadata database but is no longer stored in the cache, e.g., if the system has
            // been restarted.
            sqlx::query(&format!(
                "INSERT INTO {model_table_name}_tags (hash{maybe_separator}{tag_columns})
                 VALUES ($1{maybe_separator}{values})
                 ON CONFLICT DO NOTHING",
            ))
            .bind(signed_tag_hash)
            .execute(&mut *transaction)
            .await?;

            sqlx::query(&format!(
                "INSERT INTO model_table_hash_table_name (hash, table_name)
                 VALUES ($1, $2)
                 ON CONFLICT DO NOTHING",
            ))
            .bind(signed_tag_hash)
            .bind(model_table_name)
            .execute(&mut *transaction)
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

        sqlx::query("SELECT table_name FROM model_table_hash_table_name WHERE hash = $1")
            .bind(signed_tag_hash)
            .fetch_one(&self.metadata_database_pool)
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

        let mut rows = sqlx::query(&select_statement).fetch(&self.metadata_database_pool);

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
                "SELECT column_index FROM model_table_field_columns WHERE table_name = '{table_name}' AND {}",
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
                format!("SELECT hash FROM {table_name}_tags")
            } else {
                let predicates: Vec<String> = tag_predicates
                    .iter()
                    .map(|(tag, tag_value)| format!("{tag} = '{tag_value}'"))
                    .collect();

                format!(
                    "SELECT hash FROM {table_name}_tags WHERE {}",
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
        let mut rows = sqlx::query(query_field_columns).fetch(&self.metadata_database_pool);

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
        let mut rows = sqlx::query(query_hashes).fetch(&self.metadata_database_pool);

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

    /// Store information about `compressed_file` which contains compressed segments for the column
    /// at `query_schema_index` in the model table with `model_table_name`. An [`Error`] is returned
    /// if:
    /// * The end time is before the start time in `compressed_file`.
    /// * The max value is smaller than the min value in `compressed_file`.
    /// * The metadata database could not be modified.
    /// * A model table with `model_table_name` does not exist.
    pub async fn save_compressed_file(
        &self,
        model_table_name: &str,
        query_schema_index: usize,
        compressed_file: &CompressedFile,
    ) -> Result<(), Error> {
        Self::validate_compressed_file(compressed_file)?;

        let insert_statement = format!(
            "INSERT INTO {model_table_name}_compressed_files VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
        );

        Self::create_insert_compressed_file_query(
            &insert_statement,
            query_schema_index,
            compressed_file,
        )
        .execute(&self.metadata_database_pool)
        .await?;

        Ok(())
    }

    /// Replace the `compressed_files_to_delete` with `replacement_compressed_file` (or nothing if
    /// [`None`] is passed) in the column at `query_schema_index` for the model table with
    /// `model_table_name`. Returns [`Error`] if:
    /// * `compressed_files_to_delete` is empty.
    /// * The end time is before the start time in `replacement_compressed_file`.
    /// * The max value is smaller than the min value in `replacement_compressed_file`.
    /// * Less than the number of files in `compressed_files_to_delete` was deleted.
    /// * The metadata database could not be modified.
    /// * A model table with `model_table_name` does not exist.
    pub async fn replace_compressed_files(
        &self,
        model_table_name: &str,
        query_schema_index: usize,
        compressed_files_to_delete: &[ObjectMeta],
        replacement_compressed_file: Option<&CompressedFile>,
    ) -> Result<(), Error> {
        if compressed_files_to_delete.is_empty() {
            return Err(Error::Configuration(Box::new(
                ModelarDbError::DataRetrievalError(
                    "At least one file to delete must be provided.".to_owned(),
                ),
            )));
        }

        if let Some(compressed_file) = replacement_compressed_file {
            Self::validate_compressed_file(compressed_file)?;
        }

        let mut transaction = self.metadata_database_pool.begin().await?;

        let file_paths_to_delete: Vec<String> = compressed_files_to_delete
            .iter()
            .map(|object_meta| format!("'{}'", object_meta.location))
            .collect();

        let compressed_files_to_delete_in = file_paths_to_delete.join(",");

        // sqlx does not yet support binding arrays to IN (...) and recommends generating them.
        // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
        // query_schema_index is simply cast as a model table contains at most 1024 columns.
        let delete_from_result = sqlx::query(&format!(
            "DELETE FROM {model_table_name}_compressed_files
             WHERE field_column = $1
             AND file_path IN ({compressed_files_to_delete_in})",
        ))
        .bind(query_schema_index as i64)
        .execute(&mut *transaction)
        .await?;

        // The cast to usize is safe as only compressed_files_to_delete.len() can be deleted.
        if compressed_files_to_delete.len() != delete_from_result.rows_affected() as usize {
            return Err(Error::Configuration(Box::new(
                ModelarDbError::ImplementationError(
                    "Less than the expected number of files were deleted from the metadata database.".to_owned(),
                ),
            )));
        }

        if let Some(compressed_file) = replacement_compressed_file {
            let insert_statement = format!(
                "INSERT INTO {model_table_name}_compressed_files VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
            );

            Self::create_insert_compressed_file_query(
                &insert_statement,
                query_schema_index,
                compressed_file,
            )
            .execute(&mut *transaction)
            .await?;
        }

        transaction.commit().await
    }

    /// Check that the start time is before the end time and the minimum value is smaller than the
    /// maximum value in `compressed_file`.
    fn validate_compressed_file(compressed_file: &CompressedFile) -> Result<(), Error> {
        if compressed_file.start_time > compressed_file.end_time {
            return Err(Error::Configuration(Box::new(
                ModelarDbError::DataRetrievalError(format!(
                    "Start time '{}' cannot be after end time '{}'.",
                    compressed_file.start_time, compressed_file.end_time
                )),
            )));
        };

        if compressed_file.min_value > compressed_file.max_value {
            return Err(Error::Configuration(Box::new(
                ModelarDbError::DataRetrievalError(format!(
                    "Min value '{}' cannot be larger than max value '{}'.",
                    compressed_file.min_value, compressed_file.max_value
                )),
            )));
        };

        Ok(())
    }

    /// Create a [`Query`] that, when executed, stores `compressed_file` in the metadata database
    /// for the column at `query_schema_index` using `insert_statement`.
    fn create_insert_compressed_file_query<'a, DB: Database>(
        insert_statement: &'a str,
        query_schema_index: usize,
        compressed_file: &'a CompressedFile,
    ) -> Query<'a, DB, <DB as HasArguments<'a>>::Arguments>
    where
        i64: sqlx::Encode<'a, DB>,
        i64: sqlx::Type<DB>,
        f32: sqlx::Encode<'a, DB>,
        f32: sqlx::Type<DB>,
        String: sqlx::Encode<'a, DB>,
        String: sqlx::Type<DB>,
    {
        let min_value = Self::rewrite_special_value_to_normal_value(compressed_file.min_value);
        let max_value = Self::rewrite_special_value_to_normal_value(compressed_file.max_value);

        let created_at = compressed_file
            .file_metadata
            .last_modified
            .timestamp_millis();

        // query_schema_index is simply cast as a model table contains at most 1024 columns.
        // size is simply cast as it is unrealistic for a file to use more bytes than the max value
        // of a signed 64-bit integer as it can represent a file with a size up to ~9000 PB.
        sqlx::query(insert_statement)
            .bind(compressed_file.file_metadata.location.to_string())
            .bind(query_schema_index as i64)
            .bind(compressed_file.file_metadata.size as i64)
            .bind(created_at)
            .bind(compressed_file.start_time)
            .bind(compressed_file.end_time)
            .bind(min_value)
            .bind(max_value)
    }

    /// Return an [`ObjectMeta`] for each compressed file that belongs to the column at `query_schema_index`
    /// in the model table with `model_table_name` within the given range of time and value. The
    /// files are returned in sorted order by their start time. If no files belong to the column at
    /// `query_schema_index` for the table with `model_table_name` an empty [`Vec`] is returned,
    /// while an [`Error`] is returned if:
    /// * The end time is before the start time.
    /// * The max value is smaller than the min value.
    /// * The metadata database could not be accessed.
    /// * A model table with `model_table_name` does not exist.
    pub async fn compressed_files(
        &self,
        model_table_name: &str,
        query_schema_index: usize,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        min_value: Option<Value>,
        max_value: Option<Value>,
    ) -> Result<Vec<ObjectMeta>, Error> {
        // Set default values for the parts of the time and value range that are not defined.
        let start_time = start_time.unwrap_or(0);
        let end_time = end_time.unwrap_or(Timestamp::MAX);

        if start_time > end_time {
            return Err(Error::Configuration(Box::new(
                ModelarDbError::DataRetrievalError(format!(
                    "Start time '{start_time}' cannot be after end time '{end_time}'."
                )),
            )));
        };

        let min_value = min_value.unwrap_or(Value::NEG_INFINITY);
        let max_value = max_value.unwrap_or(Value::INFINITY);

        if min_value > max_value {
            return Err(Error::Configuration(Box::new(
                ModelarDbError::DataRetrievalError(format!(
                    "Min value '{min_value}' cannot be larger than max value '{max_value}'."
                )),
            )));
        };

        let min_value = Self::rewrite_special_value_to_normal_value(min_value);
        let max_value = Self::rewrite_special_value_to_normal_value(max_value);

        let select_statement = format!(
            "SELECT * FROM {model_table_name}_compressed_files
             WHERE field_column = ($1)
             AND ($2) <= end_time AND start_time <= ($3)
             AND ($4) <= max_value AND min_value <= ($5)
             ORDER BY start_time",
        );

        // query_schema_index is simply cast as a model table contains at most 1024 columns.
        let mut rows = sqlx::query(&select_statement)
            .bind(query_schema_index as i64)
            .bind(start_time)
            .bind(end_time)
            .bind(min_value)
            .bind(max_value)
            .fetch(&self.metadata_database_pool);

        let mut files = vec![];
        while let Some(row) = rows.try_next().await? {
            files.push(Self::convert_compressed_file_row_to_object_meta(row)?);
        }

        Ok(files)
    }

    /// Rewrite the special values in [`Value`] (Negative Infinity, Infinity, and NaN) to the closet
    /// normal values in [`Value`] to simplify storing and querying them through the metadata database.
    fn rewrite_special_value_to_normal_value(value: Value) -> Value {
        // Pattern matching on float values is not supported in Rust.
        if value == Value::NEG_INFINITY {
            Value::MIN
        } else if value == Value::INFINITY || value.is_nan() {
            Value::MAX
        } else {
            value
        }
    }

    /// Convert a row in the table_name_compressed_files table to an [`ObjectMeta`]. If the
    /// necessary column values could not be extracted from the row, return [`Error`].
    fn convert_compressed_file_row_to_object_meta(row: AnyRow) -> Result<ObjectMeta, Error> {
        let file_path: String = row.try_get("file_path")?;

        // unwrap() is safe as the created_at timestamp cannot be out of range.
        let last_modified = Utc
            .timestamp_millis_opt(row.try_get("created_at")?)
            .unwrap();

        let size: i64 = row.try_get("size")?;

        Ok(ObjectMeta {
            location: ObjectStorePath::from(file_path),
            last_modified,
            size: size as usize,
            e_tag: None,
        })
    }

    /// Save the created table to the metadata database. This consists of adding a row to the
    /// table_metadata table with the `name` of the table and the `sql` used to create the table.
    pub async fn save_table_metadata(&self, name: &str, sql: &str) -> Result<(), Error> {
        sqlx::query(&format!(
            "INSERT INTO table_metadata (table_name, sql) VALUES ($1, $2)"
        ))
        .bind(name)
        .bind(sql)
        .execute(&self.metadata_database_pool)
        .await?;

        Ok(())
    }

    /// Save the created model table to the metadata database. This includes creating a tags table
    /// for the model table, creating a compressed files table for the model table, adding a row to
    /// the model_table_metadata table, and adding a row to the model_table_field_columns table for
    /// each field column.
    pub async fn save_model_table_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
        sql: &str,
    ) -> Result<(), Error> {
        // Convert the query schema to bytes so it can be saved as a BLOB in the metadata database.
        let query_schema_bytes =
            Self::try_convert_schema_to_blob(&model_table_metadata.query_schema)?;

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

        let maybe_separator = if tag_columns.is_empty() { "" } else { ", " };

        // Create a table_name_tags table to save the 54-bit tag hashes when ingesting data.
        sqlx::query(&format!(
            "CREATE TABLE {}_tags (
                 hash INTEGER PRIMARY KEY{maybe_separator}
                 {tag_columns}
             ) STRICT",
            model_table_metadata.name
        ))
        .execute(&mut *transaction)
        .await?;

        // Create a table_name_compressed_files table to save the metadata of the table's files.
        sqlx::query(&format!(
            "CREATE TABLE {}_compressed_files (
                 file_path TEXT PRIMARY KEY,
                 field_column INTEGER,
                 size INTEGER,
                 created_at INTEGER,
                 start_time INTEGER,
                 end_time INTEGER,
                 min_value REAL,
                 max_value REAL
             ) STRICT",
            model_table_metadata.name
        ))
        .execute(&mut *transaction)
        .await?;

        // Add a new row in the model_table_metadata table to persist the model table.
        sqlx::query(
            "INSERT INTO model_table_metadata (table_name, query_schema, sql) VALUES ($1, $2, $3)",
        )
        .bind(&model_table_metadata.name)
        .bind(query_schema_bytes)
        .bind(sql)
        .execute(&mut *transaction)
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
                            Some(Self::convert_slice_usize_to_vec_u8(
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

    /// Convert a [`&[usize]`] to a [`Vec<u8>`].
    pub fn convert_slice_usize_to_vec_u8(usizes: &[usize]) -> Vec<u8> {
        usizes.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    /// Return the error bounds for the columns in the model table with `table_name`. If a model
    /// table with `table_name` does not exist, [`Error`] is returned.
    async fn error_bounds(
        &self,
        table_name: &str,
        query_schema_columns: usize,
    ) -> Result<Vec<ErrorBound>, Error> {
        let select_statement = format!(
            "SELECT column_index, error_bound FROM model_table_field_columns
             WHERE table_name = $1 ORDER BY column_index"
        );

        let mut rows = sqlx::query(&select_statement)
            .bind(table_name)
            .fetch(&self.metadata_database_pool);

        let mut column_to_error_bound =
            vec![ErrorBound::try_new(0.0).unwrap(); query_schema_columns];

        while let Some(row) = rows.try_next().await? {
            // SQLite (https://www.sqlite.org/datatype3.html) and PostgreSQL
            // (https://www.postgresql.org/docs/current/datatype.html) both use signed integers and
            // column_index is stored as an i64 instead of an u64 as a model table has at most 1024 columns.
            let error_bound_index: i64 = row.try_get(0)?;

            // unwrap() is safe as the error bounds are checked before they are stored.
            column_to_error_bound[error_bound_index as usize] =
                ErrorBound::try_new(row.try_get(1)?).unwrap();
        }

        Ok(column_to_error_bound)
    }

    /// Return the generated columns for the model table with `table_name` and `df_schema`.
    /// If a model table with `table_name` does not exist, [`Error`] is returned.
    async fn generated_columns(
        &self,
        table_name: &str,
        df_schema: &DFSchema,
    ) -> Result<Vec<Option<GeneratedColumn>>, Error> {
        let select_statement = format!(
            "SELECT column_index, generated_column_expr, generated_column_sources
             FROM model_table_field_columns WHERE table_name = $1 ORDER BY column_index"
        );

        let mut rows = sqlx::query(&select_statement)
            .bind(table_name)
            .fetch(&self.metadata_database_pool);

        let mut generated_columns = vec![None; df_schema.fields().len()];

        while let Some(row) = rows.try_next().await? {
            if let Some(original_expr) = row.try_get::<Option<&str>, _>(1)? {
                // unwrap() is safe as the expression is checked before it is written to the database.
                let expr = parser::parse_sql_expression(df_schema, original_expr).unwrap();
                let source_columns = row.try_get::<Option<&[u8]>, _>(2)?.unwrap();

                let generated_column = GeneratedColumn {
                    expr,
                    source_columns: Self::try_convert_slice_u8_to_vec_usize(source_columns)
                        .unwrap(),
                    original_expr: None,
                };

                // SQLite (https://www.sqlite.org/datatype3.html) and PostgreSQL
                // (https://www.postgresql.org/docs/current/datatype.html) both use signed integers and
                // column_index is stored as an i64 instead of an u64 as a model table has at most 1024 columns.
                let generated_columns_index: i64 = row.try_get(0)?;
                generated_columns[generated_columns_index as usize] = Some(generated_column);
            }
        }

        Ok(generated_columns)
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
