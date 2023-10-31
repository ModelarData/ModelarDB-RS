/* Copyright 2022 The ModelarDB Contributors
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

//! Management of the metadata database which stores the metadata required for the tables and
//! model tables. Tables can store arbitrary data, while model tables can only store time series as
//! segments containing metadata and models. At runtime, the location of the data for the tables and
//! the model table metadata are stored in Apache Arrow DataFusion's catalog, while this module stores
//! a mapping from model table name and tag values to hashes. These hashes can be combined with the
//! corresponding model table's field column indices to uniquely identify each univariate time series
//! stored in the storage engine by a univariate id.

pub(crate) mod compressed_file;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, str};

use chrono::{TimeZone, Utc};
use dashmap::DashMap;
use datafusion::common::{DFSchema, ToDFSchema};
use datafusion::execution::options::ParquetReadOptions;
use futures::TryStreamExt;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::metadata::model_table_metadata::{GeneratedColumn, ModelTableMetadata};
use modelardb_common::types::{ErrorBound, Timestamp, UnivariateId, Value};
use modelardb_common::{metadata, parser};
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectMeta;
use sqlx::database::HasArguments;
use sqlx::error::Error;
use sqlx::query::Query;
use sqlx::sqlite::{SqliteConnectOptions, SqliteRow};
use sqlx::{Executor, Result, Row, Sqlite, SqlitePool};
use tracing::{error, info, warn};

use crate::context::Context;
use crate::metadata::compressed_file::CompressedFile;
use crate::query::ModelTable;
use crate::storage::COMPRESSED_DATA_FOLDER;

/// Name used for the file containing the SQLite database storing the metadata.
pub const METADATA_DATABASE_NAME: &str = "metadata.sqlite3";

/// Stores the metadata required for reading from and writing to the tables and model tables.
/// The data that needs to be persisted is stored in the metadata database.
#[derive(Clone)]
pub struct MetadataManager {
    /// Folder for storing metadata and Apache Parquet files on the local file
    /// system.
    local_data_folder: PathBuf,
    /// Pool of connections to the metadata database.
    metadata_database_pool: SqlitePool,
    /// Cache of tag value hashes used to signify when to persist new unsaved
    /// tag combinations.
    tag_value_hashes: DashMap<String, u64>,
}

impl MetadataManager {
    /// Return [`MetadataManager`] if a pool of connections to the metadata database in
    /// `local_data_folder` can be made, otherwise [`Error`] is returned.
    pub async fn try_new(local_data_folder: &Path) -> Result<Self> {
        if !Self::is_path_a_data_folder(local_data_folder) {
            warn!("The data folder is not empty and does not contain data from ModelarDB");
        }

        // Specify the metadata database's path and that it should be created if it does not exist.
        let options = SqliteConnectOptions::new()
            .filename(local_data_folder.join(METADATA_DATABASE_NAME))
            .create_if_missing(true);

        // Create the metadata manager with the default values.
        let metadata_manager = Self {
            local_data_folder: local_data_folder.to_path_buf(),
            metadata_database_pool: SqlitePool::connect_with(options).await?,
            tag_value_hashes: DashMap::new(),
        };

        // Create the necessary tables in the metadata database.
        metadata::create_metadata_database_tables(
            &metadata_manager.metadata_database_pool,
            metadata::MetadataDatabaseType::SQLite,
        )
        .await?;

        // Return the metadata manager.
        Ok(metadata_manager)
    }

    /// Return [`true`] if `path` is a data folder, otherwise [`false`].
    fn is_path_a_data_folder(path: &Path) -> bool {
        if let Ok(files_and_folders) = fs::read_dir(path) {
            files_and_folders.count() == 0 || path.join(METADATA_DATABASE_NAME).exists()
        } else {
            false
        }
    }

    /// Return the path of the local data folder.
    pub fn local_data_folder(&self) -> &Path {
        &self.local_data_folder
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
    ) -> Result<u64> {
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

            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
            let signed_tag_hash = i64::from_ne_bytes(tag_hash.to_ne_bytes());

            // OR IGNORE is used to silently fail when trying to insert an already existing hash.
            // This purposely occurs if the hash has already been written to the metadata database
            // but is no longer stored in the cache, e.g., if the system has been restarted.
            let maybe_separator = if tag_columns.is_empty() { "" } else { ", " };
            transaction
                .execute(
                    format!(
                        "INSERT OR IGNORE INTO {}_tags (hash{}{}) VALUES ({}{}{})",
                        model_table_metadata.name,
                        maybe_separator,
                        tag_columns,
                        signed_tag_hash,
                        maybe_separator,
                        values
                    )
                    .as_str(),
                )
                .await?;

            transaction.execute(
                format!(
                    "INSERT OR IGNORE INTO model_table_hash_table_name (hash, table_name) VALUES ({}, '{}')",
                    signed_tag_hash, model_table_metadata.name
                )
                .as_str(),
            ).await?;

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
    pub async fn univariate_id_to_table_name(&self, univariate_id: u64) -> Result<String> {
        let mut connection = self.metadata_database_pool.acquire().await?;

        // SQLite use signed integers https://www.sqlite.org/datatype3.html.
        let tag_hash = Self::univariate_id_to_tag_hash(univariate_id);
        let signed_tag_hash = i64::from_ne_bytes(tag_hash.to_ne_bytes());

        let select_statement = format!(
            "SELECT table_name FROM model_table_hash_table_name WHERE hash = {signed_tag_hash}",
        );

        sqlx::query(&select_statement)
            .fetch_one(&mut *connection)
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
    ) -> Result<HashMap<u64, Vec<String>>> {
        // Return an empty HashMap if no tag column names are passed to keep the signature simple.
        if tag_column_names.is_empty() {
            return Ok(HashMap::new());
        }

        let mut connection = self.metadata_database_pool.acquire().await?;

        let select_statement = format!(
            "SELECT hash,{} FROM {model_table_name}_tags",
            tag_column_names.join(","),
        );
        let mut rows = sqlx::query(&select_statement).fetch(&mut *connection);

        let mut hash_to_tags = HashMap::new();
        while let Some(row) = rows.try_next().await? {
            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
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
    ) -> Result<Vec<UnivariateId>> {
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
                format!("SELECT hash FROM {table_name}_tags")
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
    /// user's query. Returns a [`Result`] with an [`Error`] if the data cannot be retrieved from
    /// the metadata database, otherwise the univariate ids are returned.
    async fn compute_univariate_ids_using_metadata_database(
        &self,
        query_field_columns: &str,
        fallback_field_column: u64,
        query_hashes: &str,
    ) -> Result<Vec<u64>> {
        // Retrieve the field columns.
        let mut rows = sqlx::query(query_field_columns).fetch(&self.metadata_database_pool);

        let mut field_columns = vec![];
        while let Some(row) = rows.try_next().await? {
            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
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
            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
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
    ) -> Result<()> {
        Self::validate_compressed_file(compressed_file)?;

        let insert_statement = format!(
            "INSERT INTO {model_table_name}_compressed_files VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
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
    ) -> Result<()> {
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
        let delete_from_result = sqlx::query(
            format!(
                "DELETE FROM {model_table_name}_compressed_files
                 WHERE field_column = {query_schema_index}
                 AND file_path IN ({compressed_files_to_delete_in})",
            )
            .as_str(),
        )
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
                "INSERT INTO {model_table_name}_compressed_files VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
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
    fn validate_compressed_file(compressed_file: &CompressedFile) -> Result<()> {
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
    fn create_insert_compressed_file_query<'a>(
        insert_statement: &'a str,
        query_schema_index: usize,
        compressed_file: &'a CompressedFile,
    ) -> Query<'a, Sqlite, <Sqlite as HasArguments<'a>>::Arguments> {
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
            .bind(compressed_file.file_metadata.location.as_ref())
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
    ) -> Result<Vec<ObjectMeta>> {
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
             WHERE field_column = (?1)
             AND (?2) <= end_time AND start_time <= (?3)
             AND (?4) <= max_value AND min_value <= (?5)
             ORDER BY start_time",
        );

        // query_schema_index is simply cast as a model table contains at most 1024 columns.
        let mut rows = sqlx::query(select_statement.as_str())
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
    fn convert_compressed_file_row_to_object_meta(row: SqliteRow) -> Result<ObjectMeta> {
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
    pub async fn save_table_metadata(&self, name: &str, sql: &str) -> Result<()> {
        metadata::save_table_metadata(&self.metadata_database_pool, name, sql).await
    }

    /// Read the rows in the table_metadata table and use these to register tables in Apache Arrow
    /// DataFusion. If the metadata database could not be opened or the table could not be queried,
    /// return [`Error`].
    pub async fn register_tables(&self, context: &Arc<Context>) -> Result<()> {
        let mut rows = sqlx::query("SELECT table_name FROM table_metadata")
            .fetch(&self.metadata_database_pool);

        while let Some(row) = rows.try_next().await? {
            if let Err(error) = self.register_table(&row, context).await {
                error!("Failed to register table due to: {}.", error);
            }
        }

        Ok(())
    }

    /// Use a row from the table_metadata table to register the table in Apache Arrow DataFusion.
    /// If the metadata database could not be opened or the table could not be queried, return
    /// [`Error`].
    async fn register_table(&self, row: &SqliteRow, context: &Arc<Context>) -> Result<()> {
        let name: &str = row.try_get(0)?;

        // Compute the path to the folder containing data for the table.
        let table_folder_path = self
            .local_data_folder()
            .join(COMPRESSED_DATA_FOLDER)
            .join(name);

        let table_folder = table_folder_path.to_str().ok_or_else(|| {
            Error::Configuration(Box::new(ModelarDbError::ConfigurationError(format!(
                "Path for table is not UTF-8: '{name}'"
            ))))
        })?;

        // Register table.
        context
            .session
            .register_parquet(name, table_folder, ParquetReadOptions::default())
            .await
            .map_err(|error| Error::Configuration(Box::new(error)))?;

        info!("Registered table '{}'.", name);
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
    ) -> Result<()> {
        // Convert the query schema to bytes so it can be saved as a BLOB in the metadata database.
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

        // Create a table_name_tags SQLite table to save the 54-bit tag hashes when ingesting data.
        let maybe_separator = if tag_columns.is_empty() { "" } else { ", " };
        transaction
            .execute(
                format!(
                    "CREATE TABLE {}_tags (hash INTEGER PRIMARY KEY{}{}) STRICT",
                    model_table_metadata.name, maybe_separator, tag_columns
                )
                .as_str(),
            )
            .await?;

        // Create a table_name_compressed_files SQLite table to save the metadata of the table's files.
        transaction
            .execute(
                format!(
                    "CREATE TABLE {}_compressed_files (file_path TEXT PRIMARY KEY,
                     field_column INTEGER, size INTEGER, created_at INTEGER, start_time INTEGER,
                     end_time INTEGER, min_value REAL, max_value REAL) STRICT",
                    model_table_metadata.name
                )
                .as_str(),
            )
            .await?;

        // Add a new row in the model_table_metadata table to persist the model table.
        transaction
            .execute(
                sqlx::query(
                    "INSERT INTO model_table_metadata (table_name, query_schema, sql) VALUES (?1, ?2, ?3)",
                )
                .bind(&model_table_metadata.name)
                .bind(query_schema_bytes)
                .bind(sql),
            )
            .await?;

        // Add a row for each field column to the model_table_field_columns table.
        let insert_statement =
            "INSERT INTO model_table_field_columns (table_name, column_name, column_index,
             error_bound, generated_column_expr, generated_column_sources)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)";

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

    /// Convert the rows in the model_table_metadata table to [`ModelTableMetadata`] and use these
    /// to register model tables in Apache Arrow DataFusion. If the metadata database could not be
    /// opened or the table could not be queried, return [`Error`].
    pub async fn register_model_tables(&self, context: &Arc<Context>) -> Result<()> {
        let mut rows = sqlx::query("SELECT table_name, query_schema FROM model_table_metadata")
            .fetch(&self.metadata_database_pool);

        while let Some(row) = rows.try_next().await? {
            if let Err(error) = self.register_model_table(&row, context).await {
                error!("Failed to register model table due to: {}.", error);
            }
        }

        Ok(())
    }

    /// Convert a row from the model_table_metadata table to a [`ModelTableMetadata`] and use it to
    /// register model tables in Apache Arrow DataFusion. If the metadata database could not be
    /// opened or the table could not be queried, return [`Error`].
    async fn register_model_table(&self, row: &SqliteRow, context: &Arc<Context>) -> Result<()> {
        let table_name: &str = row.try_get(0)?;

        // Convert the BLOBs to the concrete types.
        let query_schema_bytes = row.try_get(1)?;
        let query_schema = metadata::try_convert_blob_to_schema(query_schema_bytes)?;

        let error_bounds = self
            .error_bounds(table_name, query_schema.fields().len())
            .await?;

        // unwrap() is safe as the schema is checked before it is written to the metadata database.
        let df_query_schema = query_schema.clone().to_dfschema().unwrap();
        let generated_columns = self.generated_columns(table_name, &df_query_schema).await?;

        // Create model table metadata.
        let model_table_metadata = Arc::new(
            ModelTableMetadata::try_new(
                table_name.to_owned(),
                Arc::new(query_schema),
                error_bounds,
                generated_columns,
            )
            .map_err(|error| Error::Configuration(Box::new(error)))?,
        );

        // Register model table.
        context
            .session
            .register_table(
                table_name,
                ModelTable::new(context.clone(), model_table_metadata),
            )
            .map_err(|error| Error::Configuration(Box::new(error)))?;

        info!("Registered model table '{}'.", table_name);
        Ok(())
    }

    /// Return the error bounds for the model table with `table_name` and `query_schema_columns`
    /// using `connection`. If a model table with `table_name` does not exist, [`Error`] is
    /// returned.
    async fn error_bounds(
        &self,
        table_name: &str,
        query_schema_columns: usize,
    ) -> Result<Vec<ErrorBound>> {
        let select_statement = format!(
            "SELECT column_index, error_bound FROM model_table_field_columns
             WHERE table_name = '{table_name}' ORDER BY column_index"
        );

        let mut rows = sqlx::query(&select_statement).fetch(&self.metadata_database_pool);

        let mut column_to_error_bound =
            vec![ErrorBound::try_new(0.0).unwrap(); query_schema_columns];

        while let Some(row) = rows.try_next().await? {
            // SQLite use signed integers https://www.sqlite.org/datatype3.html and column_index is
            // stored as an i64 instead of an u64 as a model table has at most 1024 columns.
            let error_bound_index: i64 = row.try_get(0)?;

            // unwrap() is safe as the error bounds are checked before they are stored.
            column_to_error_bound[error_bound_index as usize] =
                ErrorBound::try_new(row.try_get(1)?).unwrap();
        }

        Ok(column_to_error_bound)
    }

    /// Return the generated columns for the model table with `table_name` and `df_schema` using
    /// `connection`. If a model table with `table_name` does not exist, [`Error`] is returned.
    async fn generated_columns(
        &self,
        table_name: &str,
        df_schema: &DFSchema,
    ) -> Result<Vec<Option<GeneratedColumn>>> {
        let select_statement = format!(
            "SELECT column_index, generated_column_expr, generated_column_sources
             FROM model_table_field_columns WHERE table_name = '{table_name}' ORDER BY column_index"
        );

        let mut rows = sqlx::query(&select_statement).fetch(&self.metadata_database_pool);

        let mut generated_columns = vec![None; df_schema.fields().len()];

        while let Some(row) = rows.try_next().await? {
            if let Some(original_expr) = row.try_get::<Option<&str>, _>(1)? {
                // unwrap() is safe as the expression is checked before it is written to the database.
                let expr = parser::parse_sql_expression(df_schema, original_expr).unwrap();
                let source_columns = row.try_get::<Option<&[u8]>, _>(2)?.unwrap();

                let generated_column = GeneratedColumn {
                    expr,
                    source_columns: metadata::try_convert_slice_u8_to_vec_usize(source_columns)
                        .unwrap(),
                    original_expr: None,
                };

                // SQLite use signed integers https://www.sqlite.org/datatype3.html and column_index
                // is stored as an i64 instead of an u64 as a model table has at most 1024 columns.
                let generated_columns_index: i64 = row.try_get(0)?;
                generated_columns[generated_columns_index as usize] = Some(generated_column);
            }
        }

        Ok(generated_columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    use chrono::SubsecRound;
    use modelardb_common::test;
    use once_cell::sync::Lazy;
    use proptest::{num, prop_assert_eq, proptest};
    use uuid::Uuid;

    use crate::common_test;

    static SEVEN_COMPRESSED_FILES: Lazy<Vec<CompressedFile>> = Lazy::new(|| {
        vec![
            create_compressed_file(0, 0, 37.0, 73.0),
            create_compressed_file(0, Timestamp::MAX, 37.0, 73.0),
            create_compressed_file(100, 200, 37.0, 73.0),
            create_compressed_file(300, 400, Value::NAN, Value::NAN),
            create_compressed_file(500, 600, Value::NEG_INFINITY, 73.0),
            create_compressed_file(700, 800, 37.0, Value::INFINITY),
            create_compressed_file(Timestamp::MAX, Timestamp::MAX, 37.0, 73.0),
        ]
    });

    // Tests for MetadataManager.
    #[test]
    fn test_a_non_empty_folder_without_metadata_is_not_a_data_folder() {
        let temp_dir = tempfile::tempdir().unwrap();
        create_empty_folder(temp_dir.path(), "folder");
        assert!(!MetadataManager::is_path_a_data_folder(temp_dir.path()));
    }

    #[test]
    fn test_an_empty_folder_is_a_data_folder() {
        let temp_dir = tempfile::tempdir().unwrap();
        assert!(MetadataManager::is_path_a_data_folder(temp_dir.path()));
    }

    #[test]
    fn test_a_non_empty_folder_with_metadata_is_a_data_folder() {
        let temp_dir = tempfile::tempdir().unwrap();
        create_empty_folder(temp_dir.path(), "table_folder");
        fs::create_dir(temp_dir.path().join(METADATA_DATABASE_NAME)).unwrap();
        assert!(MetadataManager::is_path_a_data_folder(temp_dir.path()));
    }

    fn create_empty_folder(path: &Path, name: &str) -> PathBuf {
        let path_buf = path.join(name);
        fs::create_dir(&path_buf).unwrap();
        path_buf
    }

    #[tokio::test]
    async fn test_get_data_folder_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path();
        let metadata_manager = MetadataManager::try_new(temp_dir.path()).await.unwrap();
        assert_eq!(temp_dir_path, metadata_manager.local_data_folder());
    }

    #[tokio::test]
    async fn test_get_new_tag_hash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = MetadataManager::try_new(temp_dir.path()).await.unwrap();

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, common_test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        let result = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await;
        assert!(result.is_ok());

        // When a new tag hash is retrieved, the hash should be saved in the cache.
        assert_eq!(metadata_manager.tag_value_hashes.len(), 1);

        // It should also be saved in the metadata database table.
        let mut rows = metadata_manager
            .metadata_database_pool
            .fetch("SELECT * FROM model_table_tags");

        assert_eq!(
            rows.try_next()
                .await
                .unwrap()
                .unwrap()
                .try_get::<&str, _>(1)
                .unwrap(),
            "tag1"
        );
    }

    #[tokio::test]
    async fn test_get_existing_tag_hash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = MetadataManager::try_new(temp_dir.path()).await.unwrap();

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, common_test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await
            .unwrap();
        assert_eq!(metadata_manager.tag_value_hashes.len(), 1);

        // When getting the same tag hash again, it should just be retrieved from the cache.
        let result = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await;

        assert!(result.is_ok());
        assert_eq!(metadata_manager.tag_value_hashes.len(), 1);
    }

    proptest! {
        #[test]
        fn test_univariate_id_to_tag_hash_and_column_index(
            tag_hash in num::u64::ANY,
            column_index in num::u16::ANY,
        ) {
            // Combine tag hash and column index into a univariate id.
            let tag_hash = tag_hash << 10; // 54-bits is used for the tag hash.
            let column_index = column_index % 1024; // 10-bits is used for the column index.
            let univariate_id = tag_hash | column_index as u64;

            // Split the univariate_id into the tag hash and column index.
            let computed_tag_hash = MetadataManager::univariate_id_to_tag_hash(univariate_id);
            let computed_column_index = MetadataManager::univariate_id_to_column_index(univariate_id);

            // Original and split should match.
            prop_assert_eq!(tag_hash, computed_tag_hash);
            prop_assert_eq!(column_index, computed_column_index);
        }
    }

    #[tokio::test]
    async fn test_compute_univariate_ids_using_fields_and_tags_for_missing_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = MetadataManager::try_new(temp_dir.path()).await.unwrap();

        assert!(metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", None, 10, &[])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_compute_univariate_ids_using_fields_and_tags_for_empty_model_table() {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = MetadataManager::try_new(temp_dir.path()).await.unwrap();

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, common_test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        // Lookup univariate ids using fields and tags for an empty table.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", None, 10, &[])
            .await
            .unwrap();
        assert!(univariate_ids.is_empty());
    }

    #[tokio::test]
    async fn test_compute_univariate_ids_using_no_fields_and_tags_for_model_table() {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = MetadataManager::try_new(temp_dir.path()).await.unwrap();
        initialize_model_table_with_tag_values(&mut metadata_manager, &["tag_value1"]).await;

        // Lookup all univariate ids for the table by not passing any fields or tags.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", None, 10, &[])
            .await
            .unwrap();
        assert_eq!(2, univariate_ids.len());
    }

    #[tokio::test]
    async fn test_compute_univariate_ids_for_the_fallback_column_using_only_a_tag_column_for_model_table(
    ) {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = MetadataManager::try_new(temp_dir.path()).await.unwrap();
        initialize_model_table_with_tag_values(&mut metadata_manager, &["tag_value1"]).await;

        // Lookup the univariate ids for the fallback column by only requesting tag columns.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", Some(&vec![0]), 10, &[])
            .await
            .unwrap();
        assert_eq!(1, univariate_ids.len());
    }

    #[tokio::test]
    async fn test_compute_the_univariate_ids_for_a_specific_field_column_for_model_table() {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = MetadataManager::try_new(temp_dir.path()).await.unwrap();
        initialize_model_table_with_tag_values(
            &mut metadata_manager,
            &["tag_value1", "tag_value2"],
        )
        .await;

        // Lookup all univariate ids for the table by not requesting ids for columns.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", None, 10, &[])
            .await
            .unwrap();
        assert_eq!(4, univariate_ids.len());

        // Lookup univariate ids for the table by requesting ids for a specific field column.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", Some(&vec![1]), 10, &[])
            .await
            .unwrap();
        assert_eq!(2, univariate_ids.len());
    }

    #[tokio::test]
    async fn test_compute_the_univariate_ids_for_a_specific_tag_value_for_model_table() {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = MetadataManager::try_new(temp_dir.path()).await.unwrap();
        initialize_model_table_with_tag_values(
            &mut metadata_manager,
            &["tag_value1", "tag_value2"],
        )
        .await;

        // Lookup all univariate ids for the table by not requesting ids for a tag value.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", None, 10, &[])
            .await
            .unwrap();
        assert_eq!(4, univariate_ids.len());

        // Lookup univariate ids for the table by requesting ids for a specific tag value.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags(
                "model_table",
                None,
                10,
                &[("tag", "tag_value1")],
            )
            .await
            .unwrap();
        assert_eq!(2, univariate_ids.len());
    }

    async fn initialize_model_table_with_tag_values(
        metadata_manager: &mut MetadataManager,
        tag_values: &[&str],
    ) {
        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, common_test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        for tag_value in tag_values {
            let tag_value = tag_value.to_string();
            metadata_manager
                .lookup_or_compute_tag_hash(&model_table_metadata, &[tag_value])
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_save_compressed_file_for_column_in_existing_model_table() {
        let compressed_file = create_compressed_file(100, 200, 37.0, 73.0);

        // An assert is purposely not used so the test fails with information about why it failed.
        create_metadata_manager_with_named_model_table_and_save_files(
            "model_table",
            &[compressed_file],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_save_compressed_file_for_column_in_missing_model_table() {
        let compressed_file = create_compressed_file(100, 200, 37.0, 73.0);
        assert!(
            create_metadata_manager_with_named_model_table_and_save_files(
                "table_model",
                &[compressed_file]
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_save_compressed_file_with_invalid_timestamps_for_column_in_existing_model_table()
    {
        let compressed_file = create_compressed_file(200, 100, 37.0, 73.0);
        assert!(
            create_metadata_manager_with_named_model_table_and_save_files(
                "model_table",
                &[compressed_file]
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_save_compressed_file_with_invalid_values_for_column_in_existing_model_table() {
        let compressed_file = create_compressed_file(100, 200, 73.0, 37.0);
        assert!(
            create_metadata_manager_with_named_model_table_and_save_files(
                "model_table",
                &[compressed_file]
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_replace_compressed_files_with_nothing() {
        let compressed_files = SEVEN_COMPRESSED_FILES.to_vec();
        let compressed_files_to_delete = &[
            compressed_files[0].file_metadata.clone(),
            compressed_files[1].file_metadata.clone(),
            compressed_files[2].file_metadata.clone(),
        ];
        let returned_object_metas =
            create_metadata_manager_with_named_model_table_save_files_delete_and_get(
                &compressed_files,
                compressed_files_to_delete,
                None,
            )
            .await;

        assert_eq!(4, returned_object_metas.len());
        assert_eq!(compressed_files[3].file_metadata, returned_object_metas[0]);
        assert_eq!(compressed_files[4].file_metadata, returned_object_metas[1]);
        assert_eq!(compressed_files[5].file_metadata, returned_object_metas[2]);
        assert_eq!(compressed_files[6].file_metadata, returned_object_metas[3]);
    }

    #[tokio::test]
    async fn test_replace_compressed_files_with_replacement_file() {
        let compressed_files = SEVEN_COMPRESSED_FILES.to_vec();
        let compressed_files_to_delete = &[
            compressed_files[0].file_metadata.clone(),
            compressed_files[1].file_metadata.clone(),
            compressed_files[2].file_metadata.clone(),
        ];
        let replacement_file = create_compressed_file(100, 200, 37.0, 73.0);

        let returned_object_metas =
            create_metadata_manager_with_named_model_table_save_files_delete_and_get(
                &compressed_files,
                compressed_files_to_delete,
                Some(&replacement_file),
            )
            .await;

        assert_eq!(5, returned_object_metas.len());
        assert_eq!(replacement_file.file_metadata, returned_object_metas[0]);
        assert_eq!(compressed_files[3].file_metadata, returned_object_metas[1]);
        assert_eq!(compressed_files[4].file_metadata, returned_object_metas[2]);
        assert_eq!(compressed_files[5].file_metadata, returned_object_metas[3]);
        assert_eq!(compressed_files[6].file_metadata, returned_object_metas[4]);
    }

    async fn create_metadata_manager_with_named_model_table_save_files_delete_and_get(
        compressed_files: &[CompressedFile],
        compressed_files_to_delete: &[ObjectMeta],
        replacement_file: Option<&CompressedFile>,
    ) -> Vec<ObjectMeta> {
        // create_metadata_manager_with_named_model_table_and_save_files() is not reused as dropping
        // the TempDir which created the folder containing the database leaves it in read-only mode.
        let temp_dir = tempfile::tempdir().unwrap();

        let metadata_manager = MetadataManager::try_new(temp_dir.path()).await.unwrap();
        let model_table_metadata = test::model_table_metadata();

        metadata_manager
            .save_model_table_metadata(&model_table_metadata, common_test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        for compressed_file in compressed_files {
            metadata_manager
                .save_compressed_file("model_table", 1, compressed_file)
                .await
                .unwrap();
        }

        metadata_manager
            .replace_compressed_files(
                "model_table",
                1,
                compressed_files_to_delete,
                replacement_file,
            )
            .await
            .unwrap();

        metadata_manager
            .compressed_files("model_table", 1, None, None, None, None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_replace_compressed_files_without_files_to_delete() {
        assert_that_replace_compressed_files_fails(Some(&[]), None).await;
    }

    #[tokio::test]
    async fn test_replace_compressed_files_with_invalid_timestamps() {
        let replacement_file = create_compressed_file(200, 100, 37.0, 73.0);
        assert_that_replace_compressed_files_fails(None, Some(&replacement_file)).await;
    }

    #[tokio::test]
    async fn test_replace_compressed_files_with_invalid_values() {
        let replacement_file = create_compressed_file(100, 200, 73.0, 37.0);
        assert_that_replace_compressed_files_fails(None, Some(&replacement_file)).await;
    }

    async fn assert_that_replace_compressed_files_fails(
        compressed_files_to_delete: Option<&[ObjectMeta]>,
        replacement_compressed_file: Option<&CompressedFile>,
    ) {
        // create_metadata_manager_with_named_model_table_and_save_files() is not reused as dropping
        // the TempDir which created the folder containing the database leaves it in read-only mode.
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager_and_save_model_table(temp_dir.path()).await;

        let compressed_files = SEVEN_COMPRESSED_FILES.to_vec();
        for compressed_file in &compressed_files {
            metadata_manager
                .save_compressed_file("model_table", 1, compressed_file)
                .await
                .unwrap();
        }

        let compressed_files_first_object_meta = &[compressed_files[0].file_metadata.clone()];
        let compressed_files_to_delete =
            compressed_files_to_delete.unwrap_or(compressed_files_first_object_meta);

        assert!(metadata_manager
            .replace_compressed_files(
                "model_table",
                1,
                compressed_files_to_delete,
                replacement_compressed_file,
            )
            .await
            .is_err());
    }

    async fn create_metadata_manager_and_save_model_table(temp_dir: &Path) -> MetadataManager {
        let metadata_manager = MetadataManager::try_new(temp_dir).await.unwrap();
        let model_table_metadata = test::model_table_metadata();

        metadata_manager
            .save_model_table_metadata(&model_table_metadata, common_test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        metadata_manager
    }

    #[tokio::test]
    async fn test_compressed_files_file_ends_within_time_range() {
        assert!(
            is_compressed_file_within_time_and_value_range(Some(150), Some(250), None, None).await
        )
    }

    #[tokio::test]
    async fn test_compressed_files_file_starts_within_time_range() {
        assert!(
            is_compressed_file_within_time_and_value_range(Some(50), Some(150), None, None).await
        )
    }

    #[tokio::test]
    async fn test_compressed_files_file_is_within_time_range() {
        assert!(
            is_compressed_file_within_time_and_value_range(Some(50), Some(250), None, None).await
        )
    }

    #[tokio::test]
    async fn test_compressed_files_file_is_before_time_range() {
        assert!(
            !is_compressed_file_within_time_and_value_range(Some(225), Some(275), None, None).await
        )
    }

    #[tokio::test]
    async fn test_compressed_files_file_is_after_time_range() {
        assert!(
            !is_compressed_file_within_time_and_value_range(Some(25), Some(75), None, None).await
        )
    }

    #[tokio::test]
    async fn test_compressed_files_without_filtering() {
        let (compressed_files, returned_object_metas) =
            create_metadata_manager_with_model_table_save_seven_files_and_get_files(
                None, None, None, None,
            )
            .await;

        assert_eq!(compressed_files.len(), returned_object_metas.len());
        for (compressed_file, returned_object_meta) in
            compressed_files.iter().zip(returned_object_metas)
        {
            assert_eq!(compressed_file.file_metadata, returned_object_meta);
        }
    }

    #[tokio::test]
    async fn test_compressed_files_with_minimum_timestamp() {
        let (compressed_files, returned_object_metas) =
            create_metadata_manager_with_model_table_save_seven_files_and_get_files(
                Some(400),
                None,
                None,
                None,
            )
            .await;

        assert_eq!(5, returned_object_metas.len());
        assert_eq!(compressed_files[1].file_metadata, returned_object_metas[0]);
        assert_eq!(compressed_files[3].file_metadata, returned_object_metas[1]);
        assert_eq!(compressed_files[4].file_metadata, returned_object_metas[2]);
        assert_eq!(compressed_files[5].file_metadata, returned_object_metas[3]);
        assert_eq!(compressed_files[6].file_metadata, returned_object_metas[4]);
    }

    #[tokio::test]
    async fn test_compressed_files_with_maximum_timestamp() {
        let (compressed_files, returned_object_metas) =
            create_metadata_manager_with_model_table_save_seven_files_and_get_files(
                None,
                Some(400),
                None,
                None,
            )
            .await;

        assert_eq!(4, returned_object_metas.len());
        assert_eq!(compressed_files[0].file_metadata, returned_object_metas[0]);
        assert_eq!(compressed_files[1].file_metadata, returned_object_metas[1]);
        assert_eq!(compressed_files[2].file_metadata, returned_object_metas[2]);
        assert_eq!(compressed_files[3].file_metadata, returned_object_metas[3]);
    }

    #[tokio::test]
    async fn test_compressed_files_with_minimum_and_maximum_timestamp() {
        let (compressed_files, returned_object_metas) =
            create_metadata_manager_with_model_table_save_seven_files_and_get_files(
                Some(400),
                Some(400),
                None,
                None,
            )
            .await;

        assert_eq!(2, returned_object_metas.len());
        assert_eq!(compressed_files[1].file_metadata, returned_object_metas[0]);
        assert_eq!(compressed_files[3].file_metadata, returned_object_metas[1]);
    }

    #[tokio::test]
    async fn test_compressed_files_file_max_is_within_value_range() {
        assert!(
            is_compressed_file_within_time_and_value_range(None, None, Some(50.0), Some(100.0))
                .await
        )
    }

    #[tokio::test]
    async fn test_compressed_files_file_min_is_within_value_range() {
        assert!(
            is_compressed_file_within_time_and_value_range(None, None, Some(0.0), Some(50.0)).await
        )
    }

    #[tokio::test]
    async fn test_compressed_files_file_is_within_value_range() {
        assert!(
            is_compressed_file_within_time_and_value_range(None, None, Some(0.0), Some(100.0))
                .await
        )
    }

    #[tokio::test]
    async fn test_compressed_files_file_is_less_than_value_range() {
        assert!(
            !is_compressed_file_within_time_and_value_range(None, None, Some(100.0), Some(150.0))
                .await
        )
    }

    #[tokio::test]
    async fn test_compressed_files_file_is_greater_than_value_range() {
        assert!(
            !is_compressed_file_within_time_and_value_range(None, None, Some(0.0), Some(25.0))
                .await
        )
    }

    async fn is_compressed_file_within_time_and_value_range(
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        min_value: Option<Value>,
        max_value: Option<Value>,
    ) -> bool {
        let compressed_files = vec![create_compressed_file(100, 200, 37.0, 73.0)];

        let returned_object_metas =
            create_metadata_manager_with_model_table_save_files_and_get_files(
                &compressed_files,
                start_time,
                end_time,
                min_value,
                max_value,
            )
            .await;

        compressed_files.len() == returned_object_metas.len()
    }

    fn create_compressed_file(
        start_time: Timestamp,
        end_time: Timestamp,
        min_value: Value,
        max_value: Value,
    ) -> CompressedFile {
        let file_metadata = ObjectMeta {
            location: ObjectStorePath::from(format!("test/{}.parquet", Uuid::new_v4())),
            last_modified: Utc::now().round_subsecs(3),
            size: 0,
            e_tag: None,
        };

        CompressedFile {
            file_metadata,
            start_time,
            end_time,
            min_value,
            max_value,
        }
    }

    #[tokio::test]
    async fn test_compressed_files_with_minimum_value() {
        let (compressed_files, returned_object_metas) =
            create_metadata_manager_with_model_table_save_seven_files_and_get_files(
                None,
                None,
                Some(80.0),
                None,
            )
            .await;

        assert_eq!(2, returned_object_metas.len());
        assert_eq!(compressed_files[3].file_metadata, returned_object_metas[0]);
        assert_eq!(compressed_files[5].file_metadata, returned_object_metas[1]);
    }

    #[tokio::test]
    async fn test_compressed_files_with_maximum_value() {
        let (compressed_files, returned_object_metas) =
            create_metadata_manager_with_model_table_save_seven_files_and_get_files(
                None,
                None,
                None,
                Some(80.0),
            )
            .await;

        assert_eq!(6, returned_object_metas.len());
        assert_eq!(compressed_files[0].file_metadata, returned_object_metas[0]);
        assert_eq!(compressed_files[1].file_metadata, returned_object_metas[1]);
        assert_eq!(compressed_files[2].file_metadata, returned_object_metas[2]);
        assert_eq!(compressed_files[4].file_metadata, returned_object_metas[3]);
        assert_eq!(compressed_files[5].file_metadata, returned_object_metas[4]);
        assert_eq!(compressed_files[6].file_metadata, returned_object_metas[5]);
    }

    #[tokio::test]
    async fn test_compressed_files_with_minimum_and_maximum_value() {
        let (compressed_files, returned_object_metas) =
            create_metadata_manager_with_model_table_save_seven_files_and_get_files(
                None,
                None,
                Some(75.0),
                Some(80.0),
            )
            .await;

        assert_eq!(1, returned_object_metas.len());
        assert_eq!(compressed_files[5].file_metadata, returned_object_metas[0]);
    }

    async fn create_metadata_manager_with_model_table_save_seven_files_and_get_files(
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        min_value: Option<Value>,
        max_value: Option<Value>,
    ) -> (Vec<CompressedFile>, Vec<ObjectMeta>) {
        let compressed_files = SEVEN_COMPRESSED_FILES.to_vec();

        let returned_files = create_metadata_manager_with_model_table_save_files_and_get_files(
            &compressed_files,
            start_time,
            end_time,
            min_value,
            max_value,
        )
        .await;

        (compressed_files, returned_files)
    }

    async fn create_metadata_manager_with_model_table_save_files_and_get_files(
        compressed_files: &[CompressedFile],
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        min_value: Option<Value>,
        max_value: Option<Value>,
    ) -> Vec<ObjectMeta> {
        let metadata_manager = create_metadata_manager_with_named_model_table_and_save_files(
            "model_table",
            compressed_files,
        )
        .await
        .unwrap();

        metadata_manager
            .compressed_files("model_table", 1, start_time, end_time, min_value, max_value)
            .await
            .unwrap()
    }

    async fn create_metadata_manager_with_named_model_table_and_save_files(
        model_table_name: &str,
        compressed_files: &[CompressedFile],
    ) -> Result<MetadataManager> {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager_and_save_model_table(temp_dir.path()).await;

        for compressed_file in compressed_files {
            metadata_manager
                .save_compressed_file(model_table_name, 1, compressed_file)
                .await?;
        }

        Ok(metadata_manager)
    }

    #[tokio::test]
    async fn test_register_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let context = common_test::test_context(temp_dir.path()).await;

        context
            .metadata_manager
            .save_table_metadata(
                "table_name",
                "CREATE TABLE table_name(timestamp TIMESTAMP, values REAL, metadata REAL)",
            )
            .await
            .unwrap();

        // Register the table with Apache Arrow DataFusion.
        context
            .metadata_manager
            .register_tables(&context)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_save_model_table_metadata() {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = MetadataManager::try_new(temp_dir.path()).await.unwrap();

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, common_test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        // Retrieve the tables with metadata for the model table from the metadata database.
        let mut rows = metadata_manager
            .metadata_database_pool
            .fetch("SELECT hash FROM model_table_tags");
        assert!(rows.try_next().await.unwrap().is_none());

        let mut rows = metadata_manager.metadata_database_pool.fetch(
            "SELECT file_path, field_column, size, created_at, start_time, end_time,
             min_value, max_value FROM model_table_compressed_files",
        );
        assert!(rows.try_next().await.unwrap().is_none());

        // Retrieve the rows in model_table_metadata from the metadata database.
        let mut rows = metadata_manager
            .metadata_database_pool
            .fetch("SELECT table_name, query_schema FROM model_table_metadata");

        let row = rows.try_next().await.unwrap().unwrap();
        assert_eq!("model_table", row.try_get::<&str, _>(0).unwrap());
        assert_eq!(
            metadata::try_convert_schema_to_blob(&model_table_metadata.query_schema).unwrap(),
            row.try_get::<Vec<u8>, _>(1).unwrap()
        );

        assert!(rows.try_next().await.unwrap().is_none());

        // Retrieve the rows in model_table_field_columns from the metadata database.
        let mut rows = metadata_manager.metadata_database_pool.fetch(
            "SELECT table_name, column_name, column_index, error_bound, generated_column_expr,
             generated_column_sources FROM model_table_field_columns",
        );

        let row = rows.try_next().await.unwrap().unwrap();
        assert_eq!("model_table", row.try_get::<&str, _>(0).unwrap());
        assert_eq!("field_1", row.try_get::<&str, _>(1).unwrap());
        assert_eq!(1, row.try_get::<i32, _>(2).unwrap());
        assert_eq!(1.0, row.try_get::<f32, _>(3).unwrap());
        assert_eq!(None, row.try_get::<Option<&str>, _>(4).unwrap());
        assert_eq!(None, row.try_get::<Option<Vec<u8>>, _>(5).unwrap());

        let row = rows.try_next().await.unwrap().unwrap();
        assert_eq!("model_table", row.try_get::<&str, _>(0).unwrap());
        assert_eq!("field_2", row.try_get::<&str, _>(1).unwrap());
        assert_eq!(2, row.try_get::<i32, _>(2).unwrap());
        assert_eq!(5.0, row.try_get::<f32, _>(3).unwrap());
        assert_eq!(None, row.try_get::<Option<&str>, _>(4).unwrap());
        assert_eq!(None, row.try_get::<Option<Vec<u8>>, _>(5).unwrap());

        assert!(rows.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_register_model_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let context = common_test::test_context(temp_dir.path()).await;

        let model_table_metadata = test::model_table_metadata();
        context
            .metadata_manager
            .save_model_table_metadata(&model_table_metadata, common_test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        // Register the model table with Apache Arrow DataFusion.
        context
            .metadata_manager
            .register_model_tables(&context)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_error_bound() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = common_test::test_context(temp_dir.path()).await;

        let model_table_metadata = test::model_table_metadata();
        context
            .metadata_manager
            .save_model_table_metadata(&model_table_metadata, common_test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        let error_bounds = context
            .metadata_manager
            .error_bounds("model_table", 4)
            .await
            .unwrap();
        let percentages: Vec<f32> = error_bounds
            .iter()
            .map(|error_bound| error_bound.into_inner())
            .collect();

        assert_eq!(percentages, &[0.0, 1.0, 5.0, 0.0]);
    }

    #[tokio::test]
    async fn test_generated_columns() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = common_test::test_context(temp_dir.path()).await;

        let model_table_metadata = test::model_table_metadata();
        context
            .metadata_manager
            .save_model_table_metadata(&model_table_metadata, common_test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        let df_schema = model_table_metadata.query_schema.to_dfschema().unwrap();
        let generated_columns = context
            .metadata_manager
            .generated_columns("model_table", &df_schema)
            .await
            .unwrap();

        for generated_column in generated_columns {
            assert!(generated_column.is_none());
        }
    }
}
