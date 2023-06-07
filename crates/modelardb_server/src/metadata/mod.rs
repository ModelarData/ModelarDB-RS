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

//! Management of the metadata database which stores the system's configuration and the metadata
//! required for the tables and model tables. Tables can store arbitrary data, while model tables
//! can only store time series as segments containing metadata and models. At runtime, the location
//! of the data for the tables and the model table metadata are stored in Apache Arrow DataFusion's
//! catalog, while this module stores the system's configuration and a mapping from model table name
//! and tag values to hashes. These hashes can be combined with the corresponding model table's
//! field column indices to uniquely identify each univariate time series stored in the storage
//! engine by a univariate id.

pub mod model_table_metadata;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::hash::Hasher;
use std::mem;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;

use arrow_flight::{IpcMessage, SchemaAsIpc};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::{error::ArrowError, ipc::writer::IpcWriteOptions};
use datafusion::common::{DFSchema, ToDFSchema};
use datafusion::execution::options::ParquetReadOptions;
use futures::TryStreamExt;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::types::UnivariateId;
use modelardb_compression::ErrorBound;
use sqlx::error::Error;
use sqlx::sqlite::{SqliteConnectOptions, SqliteRow};
use sqlx::{Executor, Result, Row, SqlitePool};
use tracing::{error, info, warn};

use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::parser;
use crate::query::ModelTable;
use crate::storage::COMPRESSED_DATA_FOLDER;
use crate::{Context, ServerMode};

use self::model_table_metadata::GeneratedColumn;

/// Name used for the file containing the SQLite database storing the metadata.
pub const METADATA_DATABASE_NAME: &str = "metadata.sqlite3";

/// Store's the system's configuration and the metadata required for reading from and writing to the
/// tables and model tables. The data that needs to be persisted are stored in the metadata
/// database.
#[derive(Clone)]
pub struct MetadataManager {
    /// Folder for storing metadata and Apache Parquet files on the local file
    /// system.
    local_data_folder: PathBuf,
    /// Pool of connections to the metadata database.
    metadata_database_pool: SqlitePool,
    /// Cache of tag value hashes used to signify when to persist new unsaved
    /// tag combinations.
    tag_value_hashes: HashMap<String, u64>,
    /// The mode of the server used to determine the behaviour when modifying the remote object store.
    pub server_mode: ServerMode,
    /// Amount of memory to reserve for storing uncompressed data buffers.
    pub uncompressed_reserved_memory_in_bytes: usize,
    /// Amount of memory to reserve for storing compressed data buffers.
    pub compressed_reserved_memory_in_bytes: usize,
}

impl MetadataManager {
    /// Return [`MetadataManager`] if a connection can be made to the metadata database in
    /// `local_data_folder`, otherwise [`Error`](sqlx::Error) is returned.
    pub async fn try_new(local_data_folder: &Path, server_mode: ServerMode) -> Result<Self> {
        if !Self::is_path_a_data_folder(local_data_folder) {
            warn!("The data folder is not empty and does not contain data from ModelarDB");
        }

        // Compute the path to the metadata database.
        let options = SqliteConnectOptions::new()
            .filename(local_data_folder.join(METADATA_DATABASE_NAME))
            .create_if_missing(true);

        // Create the metadata manager with the default values.
        let metadata_manager = Self {
            local_data_folder: local_data_folder.to_path_buf(),
            metadata_database_pool: SqlitePool::connect_with(options).await?,
            tag_value_hashes: HashMap::new(),
            server_mode,
            // Default values for parameters.
            uncompressed_reserved_memory_in_bytes: 512 * 1024 * 1024, // 512 MiB
            compressed_reserved_memory_in_bytes: 512 * 1024 * 1024,   // 512 MiB
        };

        // Create the necessary tables in the metadata database.
        metadata_manager.create_metadata_database_tables().await?;

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

    /// If they do not already exist, create the tables used for table and model table metadata.
    /// * The table_metadata table contains the metadata for tables.
    /// * The model_table_metadata table contains the main metadata for model tables.
    /// * The model_table_hash_table_name contains a mapping from each tag hash to the name of the
    /// model table that contains the time series with that tag hash.
    /// * The model_table_field_columns table contains the name, index, error bound, and generation
    /// expression of the field columns in each model table.
    /// If the tables exist or were created, return [`Ok`], otherwise return [`sqlx::Error`].
    async fn create_metadata_database_tables(&self) -> Result<()> {
        // Create the table_metadata SQLite table if it does not exist.
        self.metadata_database_pool
            .execute(
                "CREATE TABLE IF NOT EXISTS table_metadata (
                table_name TEXT PRIMARY KEY
                ) STRICT",
            )
            .await?;

        // Create the model_table_metadata SQLite table if it does not exist.
        self.metadata_database_pool
            .execute(
                "CREATE TABLE IF NOT EXISTS model_table_metadata (
                table_name TEXT PRIMARY KEY,
                query_schema BLOB NOT NULL
                ) STRICT",
            )
            .await?;

        // Create the model_table_hash_name SQLite table if it does not exist.
        self.metadata_database_pool
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
        self.metadata_database_pool
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

    /// Return the path of the local data folder.
    pub fn local_data_folder(&self) -> &Path {
        &self.local_data_folder
    }

    /// Return the tag hash for the given list of tag values either by retrieving it from a cache
    /// or, if the combination of tag values is not in the cache, by computing a new hash. If the
    /// hash is not in the cache, it is both saved to the cache, persisted to the model_table_tags
    /// table if it does not already contain it, and persisted to the model_table_hash_table_name if
    /// it does not already contain it. If the model_table_tags or the model_table_hash_table_name
    /// table cannot be accessed, [`sqlx::Error`] is returned.
    pub async fn lookup_or_compute_tag_hash(
        &mut self,
        model_table_metadata: &ModelTableMetadata,
        tag_values: &[String],
    ) -> Result<u64> {
        let cache_key = {
            let mut cache_key_list = tag_values.to_vec();
            cache_key_list.push(model_table_metadata.name.clone());

            cache_key_list.join(";")
        };

        // Check if the tag hash is in the cache. If it is, retrieve it. If it is not, create a new
        // one and save it both in the cache and in the model_table_tags table.
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

    /// Return the error bound for `univariate_id`. Returns an [`Error`](sqlx::Error) if the
    /// necessary data cannot be retrieved from the metadata database.
    pub async fn error_bound(&self, univariate_id: u64) -> Result<ErrorBound> {
        let mut connection = self.metadata_database_pool.acquire().await?;

        // SQLite use signed integers https://www.sqlite.org/datatype3.html.
        let tag_hash = MetadataManager::univariate_id_to_tag_hash(univariate_id);
        let column_index = MetadataManager::univariate_id_to_column_index(univariate_id);
        let signed_tag_hash = i64::from_ne_bytes(tag_hash.to_ne_bytes());
        let select_statement = format!(
            "SELECT error_bound FROM model_table_field_columns, model_table_hash_table_name
             WHERE model_table_field_columns.table_name = model_table_hash_table_name.table_name
             AND hash = {signed_tag_hash} AND column_index = {column_index}",
        );
        let mut rows = sqlx::query(&select_statement).fetch(&mut connection);

        // unwrap() is safe as the error bound is checked before it is written to the metadata database.
        let percentage: f32 = rows.try_next().await?.unwrap().try_get(0)?;
        Ok(ErrorBound::try_new(percentage).unwrap())
    }

    /// Extract the first 54-bits from `univariate_id` which is a hash computed from tags.
    pub fn univariate_id_to_tag_hash(univariate_id: u64) -> u64 {
        univariate_id & 18446744073709550592
    }

    /// Extract the last 10-bits from `univariate_id` which is the index of the time series column.
    pub fn univariate_id_to_column_index(univariate_id: u64) -> u16 {
        (univariate_id & 1023) as u16
    }

    /// Return a mapping from tag hash to table names. Returns an [`Error`](sqlx::Error) if the
    /// necessary data cannot be retrieved from the metadata database.
    pub async fn mapping_from_hash_to_table_name(&self) -> Result<HashMap<u64, String>> {
        let mut connection = self.metadata_database_pool.acquire().await?;

        let select_statement = "SELECT hash, table_name FROM model_table_hash_table_name";
        let mut rows = sqlx::query(select_statement).fetch(&mut connection);

        let mut hash_to_table_name = HashMap::new();
        while let Some(row) = rows.try_next().await? {
            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
            let signed_tag_hash: i64 = row.try_get(0)?;
            let tag_hash = u64::from_ne_bytes(signed_tag_hash.to_ne_bytes());
            hash_to_table_name.insert(tag_hash, row.try_get(1)?);
        }

        Ok(hash_to_table_name)
    }

    /// Return a mapping from tag hashes to the tags in the columns with the names in
    /// `tag_column_names` for the time series in the model table with the name `model_table_name`.
    /// Returns an [`Error`](sqlx::Error) if the necessary data cannot be retrieved from the
    /// metadata database.
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
        let mut rows = sqlx::query(&select_statement).fetch(&mut connection);

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
    /// engine using the field columns, tag names, and tag values in the query. Returns a
    /// [`Error`](sqlx::Error) if the necessary data cannot be retrieved from the metadata database.
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
    /// user's query. Returns a [`Result`] with an [`Error`](sqlx::Error) if the data cannot be
    /// retrieved from the metadata database, otherwise the univariate ids are returned.
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

    /// Normalize `name` to allow direct comparisons between names.
    pub fn normalize_name(name: &str) -> String {
        name.to_lowercase()
    }

    /// Save the created table to the metadata database. This consists of adding a row to the
    /// table_metadata table with the `name` of the created table.
    pub async fn save_table_metadata(&self, name: &str) -> Result<()> {
        // Add a new row in the table_metadata table to persist the table.
        sqlx::query("INSERT INTO table_metadata (table_name) VALUES (?1)")
            .bind(name)
            .execute(&self.metadata_database_pool)
            .await?;

        Ok(())
    }

    /// Read the rows in the table_metadata table and use these to register tables in Apache Arrow
    /// DataFusion. If the metadata database could not be opened or the table could not be queried,
    /// return [`sqlx::Error`].
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
    /// for the model table, adding a row to the model_table_metadata table, and adding a row to
    /// the model_table_field_columns table for each field column.
    pub async fn save_model_table_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
    ) -> Result<()> {
        // Convert the query schema to bytes so it can be saved as a BLOB in the metadata database.
        let query_schema_bytes =
            MetadataManager::convert_schema_to_blob(&model_table_metadata.query_schema)?;

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
        // The query is executed with a formatted string since CREATE TABLE cannot take parameters.
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

        // Add a new row in the model_table_metadata table to persist the model table.
        transaction
            .execute(
                sqlx::query(
                    "INSERT INTO model_table_metadata (table_name, query_schema) VALUES (?1, ?2)",
                )
                .bind(&model_table_metadata.name)
                .bind(query_schema_bytes),
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
                            Some(MetadataManager::convert_slice_usize_to_vec_u8(
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

                sqlx::query(insert_statement)
                    .bind(&model_table_metadata.name)
                    .bind(field.name())
                    .bind(query_schema_index as i64) // TODO: fix usize to size
                    .bind(error_bound)
                    .bind(generated_column_expr)
                    .bind(generated_column_sources)
                    .execute(&mut transaction)
                    .await?;
            }
        }
        transaction.commit().await
    }

    /// Convert the rows in the model_table_metadata table to [`ModelTableMetadata`] and use these
    /// to register model tables in Apache Arrow DataFusion. If the metadata database could not be
    /// opened or the table could not be queried, return [`sqlx::Error`].
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
        let query_schema = MetadataManager::convert_blob_to_schema(query_schema_bytes)?;

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

    /// Convert a [`Schema`] to [`Vec<u8>`].
    fn convert_schema_to_blob(schema: &Schema) -> Result<Vec<u8>> {
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
    /// [`Error`](sqlx::Error).
    fn convert_blob_to_schema(schema_bytes: Vec<u8>) -> Result<Schema> {
        let ipc_message = IpcMessage(schema_bytes.into());
        Schema::try_from(ipc_message).map_err(|error| Error::ColumnDecode {
            index: "query_schema".to_owned(),
            source: Box::new(error),
        })
    }

    /// Convert a [`&[usize]`] to a [`Vec<u8>`].
    fn convert_slice_usize_to_vec_u8(usizes: &[usize]) -> Vec<u8> {
        usizes.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    /// Convert a [`&[u8]`] to a [`Vec<usize>`] if the length of `bytes` divides evenly by
    /// [`mem::size_of::<usize>()`], otherwise [`Error`](sqlx::Error) is returned.
    fn convert_slice_u8_to_vec_usize(bytes: &[u8]) -> Result<Vec<usize>> {
        if bytes.len() % mem::size_of::<usize>() != 0 {
            Err(Error::ColumnDecode {
                index: "generated_column_sources".to_owned(),
                source: Box::new(ModelarDbError::ConfigurationError(
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

    /// Return the error bounds for the model table with `table_name` and `query_schema_columns`
    /// using `connection`. If a model table with `table_name` does not exist, [`sqlx::Error`] is
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
            // unwrap() is safe as the error bounds are checked before they are stored.
            // TODO: fix conversion between i64 and usize
            let error_bound_index: i64 = row.try_get(0)?;
            column_to_error_bound[error_bound_index as usize] =
                ErrorBound::try_new(row.try_get(1)?).unwrap();
        }

        Ok(column_to_error_bound)
    }

    /// Return the generated columns for the model table with `table_name` and `df_schema` using
    /// `connection`. If a model table with `table_name` does not exist, [`sqlx::Error`] is
    /// returned.
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
            if let Some(original_expr) = row.try_get::<Option<String>, usize>(1)? {
                // unwrap() is safe as the expression is checked before it is written to the database.
                let expr = parser::parse_sql_expression(df_schema, &original_expr).unwrap();
                let source_columns = row.try_get::<Option<&[u8]>, usize>(2)?.unwrap();

                let generated_column = GeneratedColumn {
                    expr,
                    source_columns: MetadataManager::convert_slice_u8_to_vec_usize(source_columns)
                        .unwrap(),
                    original_expr: None,
                };

                // TODO: fix conversion between i64 and usize
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

    use proptest::{collection, num, prop_assert_eq, proptest};

    use crate::common_test;

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
    async fn test_create_metadata_database_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = common_test::test_metadata_manager(temp_dir.path()).await;

        // Verify that the tables were created and has the expected columns.
        metadata_manager
            .metadata_database_pool
            .execute("SELECT table_name FROM table_metadata")
            .await
            .unwrap();

        metadata_manager
            .metadata_database_pool
            .execute("SELECT table_name, query_schema FROM model_table_metadata")
            .await
            .unwrap();

        metadata_manager
            .metadata_database_pool
            .execute("SELECT hash, table_name FROM model_table_hash_table_name")
            .await
            .unwrap();

        metadata_manager
            .metadata_database_pool
            .execute(
                "SELECT table_name, column_name, column_index, error_bound, generated_column_expr,
                 generated_column_sources FROM model_table_field_columns",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_get_data_folder_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path();
        let metadata_manager = common_test::test_metadata_manager(temp_dir_path).await;
        assert_eq!(temp_dir_path, metadata_manager.local_data_folder());
    }

    #[tokio::test]
    async fn test_get_new_tag_hash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = common_test::test_metadata_manager(temp_dir.path()).await;

        let model_table_metadata = common_test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();

        let result = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await;
        assert!(result.is_ok());

        // When a new tag hash is retrieved, the hash should be saved in the cache.
        assert_eq!(metadata_manager.tag_value_hashes.keys().len(), 1);

        // It should also be saved in the metadata database table.
        let mut rows = metadata_manager
            .metadata_database_pool
            .fetch("SELECT * FROM model_table_tags");

        assert_eq!(
            rows.try_next()
                .await
                .unwrap()
                .unwrap()
                .get::<String, usize>(1),
            "tag1"
        );
    }

    #[tokio::test]
    async fn test_get_existing_tag_hash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = common_test::test_metadata_manager(temp_dir.path()).await;

        let model_table_metadata = common_test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();

        metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await
            .unwrap();
        assert_eq!(metadata_manager.tag_value_hashes.keys().len(), 1);

        // When getting the same tag hash again, it should just be retrieved from the cache.
        let result = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await;

        assert!(result.is_ok());
        assert_eq!(metadata_manager.tag_value_hashes.keys().len(), 1);
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
        let metadata_manager = common_test::test_metadata_manager(temp_dir.path()).await;

        assert!(metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", None, 10, &[])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_compute_univariate_ids_using_fields_and_tags_for_empty_model_table() {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = common_test::test_metadata_manager(temp_dir.path()).await;

        let model_table_metadata = common_test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
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
        let mut metadata_manager = common_test::test_metadata_manager(temp_dir.path()).await;
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
        let mut metadata_manager = common_test::test_metadata_manager(temp_dir.path()).await;
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
        let mut metadata_manager = common_test::test_metadata_manager(temp_dir.path()).await;
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
        let mut metadata_manager = common_test::test_metadata_manager(temp_dir.path()).await;
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
        let model_table_metadata = common_test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
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

    #[test]
    fn test_normalize_table_name_lowercase_no_effect() {
        assert_eq!("table_name", MetadataManager::normalize_name("table_name"));
    }

    #[test]
    fn test_normalize_table_name_uppercase() {
        assert_eq!("table_name", MetadataManager::normalize_name("TABLE_NAME"));
    }

    #[tokio::test]
    async fn test_save_table_metadata() {
        // Save a table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = common_test::test_metadata_manager(temp_dir.path()).await;

        let table_name = "table_name";
        metadata_manager
            .save_table_metadata(table_name)
            .await
            .unwrap();

        // Retrieve the table from the metadata database.
        let mut rows = metadata_manager
            .metadata_database_pool
            .fetch("SELECT table_name FROM table_metadata");

        let row = rows.try_next().await.unwrap().unwrap();
        let retrieved_table_name = row.try_get::<String, usize>(0).unwrap();
        assert_eq!(table_name, retrieved_table_name);

        assert!(rows.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_register_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let context = common_test::test_context(temp_dir.path()).await;

        context
            .metadata_manager
            .save_table_metadata("table_name")
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
        let metadata_manager = common_test::test_metadata_manager(temp_dir.path()).await;

        let model_table_metadata = common_test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();

        // Retrieve the model table from the metadata database.
        let mut rows = metadata_manager
            .metadata_database_pool
            .fetch("SELECT hash FROM model_table_tags");
        assert!(rows.try_next().await.unwrap().is_none());

        // Retrieve the rows in model_table_metadata from the metadata database.
        let mut rows = metadata_manager
            .metadata_database_pool
            .fetch("SELECT table_name, query_schema FROM model_table_metadata");

        let row = rows.try_next().await.unwrap().unwrap();
        assert_eq!("model_table", row.try_get::<String, usize>(0).unwrap());
        assert_eq!(
            MetadataManager::convert_schema_to_blob(&model_table_metadata.query_schema).unwrap(),
            row.try_get::<Vec<u8>, usize>(1).unwrap()
        );

        assert!(rows.try_next().await.unwrap().is_none());

        // Retrieve the rows in model_table_field_columns from the metadata database.
        let mut rows = metadata_manager.metadata_database_pool.fetch(
            "SELECT table_name, column_name, column_index, error_bound, generated_column_expr,
             generated_column_sources FROM model_table_field_columns",
        );

        let row = rows.try_next().await.unwrap().unwrap();
        assert_eq!("model_table", row.get::<String, usize>(0));
        assert_eq!("field_1", row.get::<String, usize>(1));
        assert_eq!(1, row.get::<i32, usize>(2));
        assert_eq!(1.0, row.get::<f32, usize>(3));
        assert_eq!(None, row.get::<Option<String>, usize>(4));
        assert_eq!(None, row.get::<Option<Vec<u8>>, usize>(5));

        let row = rows.try_next().await.unwrap().unwrap();
        assert_eq!("model_table", row.get::<String, usize>(0));
        assert_eq!("field_2", row.get::<String, usize>(1));
        assert_eq!(2, row.get::<i32, usize>(2));
        assert_eq!(5.0, row.get::<f32, usize>(3));
        assert_eq!(None, row.get::<Option<String>, usize>(4));
        assert_eq!(None, row.get::<Option<Vec<u8>>, usize>(5));

        assert!(rows.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_register_model_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let context = common_test::test_context(temp_dir.path()).await;

        let model_table_metadata = common_test::model_table_metadata();
        context
            .metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();

        // Register the model table with Apache Arrow DataFusion.
        context
            .metadata_manager
            .register_model_tables(&context)
            .await
            .unwrap();
    }

    #[test]
    fn test_blob_to_schema_empty() {
        assert!(MetadataManager::convert_blob_to_schema(vec!(1, 2, 4, 8)).is_err());
    }

    #[test]
    fn test_blob_to_schema_and_schema_to_blob() {
        let schema = common_test::model_table_metadata().schema;

        // Serialize a schema to bytes.
        let bytes = MetadataManager::convert_schema_to_blob(&schema).unwrap();

        // Deserialize the bytes to a schema.
        let retrieved_schema = MetadataManager::convert_blob_to_schema(bytes).unwrap();
        assert_eq!(*schema, retrieved_schema);
    }

    proptest! {
        #[test]
        fn test_usize_to_u8_and_u8_to_usize(values in collection::vec(num::usize::ANY, 0..50)) {
            let bytes = MetadataManager::convert_slice_usize_to_vec_u8(&values);
            let usizes = MetadataManager::convert_slice_u8_to_vec_usize(&bytes).unwrap();
            prop_assert_eq!(values, usizes);
        }
    }

    #[tokio::test]
    async fn test_error_bound() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = common_test::test_context(temp_dir.path()).await;

        let model_table_metadata = common_test::model_table_metadata();
        context
            .metadata_manager
            .save_model_table_metadata(&model_table_metadata)
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

        let model_table_metadata = common_test::model_table_metadata();
        context
            .metadata_manager
            .save_model_table_metadata(&model_table_metadata)
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
