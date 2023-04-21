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
use std::error::Error;
use std::fs;
use std::hash::Hasher;
use std::mem;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;

use arrow_flight::{IpcMessage, SchemaAsIpc};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::{error::ArrowError, ipc::writer::IpcWriteOptions};
use datafusion::execution::options::ParquetReadOptions;
use modelardb_common::types::UnivariateId;
use modelardb_compression::models::ErrorBound;
use rusqlite::types::Type::Blob;
use rusqlite::{params, Connection, Result, Row};
use tokio::runtime::Runtime;
use tracing::{error, info, warn};

use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::query::ModelTable;
use crate::storage::COMPRESSED_DATA_FOLDER;
use crate::{Context, ServerMode};

/// Name used for the file containing the SQLite database storing the metadata.
pub const METADATA_DATABASE_NAME: &str = "metadata.sqlite3";

/// Store's the system's configuration and the metadata required for reading from and writing to the
/// tables and model tables. The data that needs to be persisted are stored in the metadata
/// database.
#[derive(Clone)]
pub struct MetadataManager {
    /// Location of the metadata database.
    metadata_database_path: PathBuf,
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
    /// `local_data_folder`, otherwise [`Error`](rusqlite::Error) is returned.
    pub fn try_new(local_data_folder: &Path, server_mode: ServerMode) -> Result<Self> {
        if !Self::is_path_a_data_folder(local_data_folder) {
            warn!("The data folder is not empty and does not contain data from ModelarDB");
        }

        // Compute the path to the metadata database.
        let metadata_database_path = local_data_folder.join(METADATA_DATABASE_NAME);

        // Create the metadata manager with the default values.
        let metadata_manager = Self {
            metadata_database_path,
            tag_value_hashes: HashMap::new(),
            server_mode,
            // Default values for parameters.
            uncompressed_reserved_memory_in_bytes: 512 * 1024 * 1024, // 512 MiB
            compressed_reserved_memory_in_bytes: 512 * 1024 * 1024,   // 512 MiB
        };

        // Create the necessary tables in the metadata database.
        metadata_manager.create_metadata_database_tables()?;

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
    /// * The model_table_field_columns table contains the name and index of the field columns in
    /// each model table.
    /// If the tables exist or were created, return [`Ok`], otherwise return [`rusqlite::Error`].
    fn create_metadata_database_tables(&self) -> Result<()> {
        let connection = Connection::open(&self.metadata_database_path)?;

        // Create the table_metadata SQLite table if it does not exist.
        connection.execute(
            "CREATE TABLE IF NOT EXISTS table_metadata (
                table_name TEXT PRIMARY KEY
        ) STRICT",
            (),
        )?;

        // Create the model_table_metadata SQLite table if it does not exist.
        connection.execute(
            "CREATE TABLE IF NOT EXISTS model_table_metadata (
                table_name TEXT PRIMARY KEY,
                schema BLOB NOT NULL,
                timestamp_column_index INTEGER NOT NULL,
                tag_column_indices BLOB NOT NULL,
                error_bounds BLOB NOT NULL
        ) STRICT",
            (),
        )?;

        // Create the model_table_hash_name SQLite table if it does not exist.
        connection.execute(
            "CREATE TABLE IF NOT EXISTS model_table_hash_table_name (
                hash INTEGER PRIMARY KEY,
                table_name TEXT
        ) STRICT",
            (),
        )?;

        // Create the model_table_field_columns SQLite table if it does not
        // exist. Note that column_index will only use a maximum of 10 bits.
        connection.execute(
            "CREATE TABLE IF NOT EXISTS model_table_field_columns (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                column_index INTEGER NOT NULL,
                PRIMARY KEY (table_name, column_name)
        ) STRICT",
            (),
        )?;

        Ok(())
    }

    /// Return the path of the local data folder.
    pub fn local_data_folder(&self) -> &Path {
        // unwrap() is safe as metadata_database_path is created by self.
        self.metadata_database_path.parent().unwrap()
    }

    /// Return the tag hash for the given list of tag values either by retrieving it from a cache
    /// or, if the combination of tag values is not in the cache, by computing a new hash. If the
    /// hash is not in the cache, it is both saved to the cache, persisted to the model_table_tags
    /// table if it does not already contain it, and persisted to the model_table_hash_table_name if
    /// it does not already contain it. If the model_table_tags or the model_table_hash_table_name
    /// table cannot be accessed, [`rusqlite::Error`] is returned.
    pub fn lookup_or_compute_tag_hash(
        &mut self,
        model_table: &ModelTableMetadata,
        tag_values: &[String],
    ) -> Result<u64, rusqlite::Error> {
        let cache_key = {
            let mut cache_key_list = tag_values.to_vec();
            cache_key_list.push(model_table.name.clone());

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

            let tag_columns: String = model_table
                .tag_column_indices
                .iter()
                .map(|index| model_table.schema.field(*index).name().clone())
                .collect::<Vec<String>>()
                .join(",");

            let values = tag_values
                .iter()
                .map(|value| format!("'{value}'"))
                .collect::<Vec<String>>()
                .join(",");

            // Create a transaction to ensure the database state is consistent across tables.
            let mut connection = Connection::open(&self.metadata_database_path)?;
            let transaction = connection.transaction()?;

            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
            let signed_tag_hash = i64::from_ne_bytes(tag_hash.to_ne_bytes());

            // OR IGNORE is used to silently fail when trying to insert an already existing hash.
            // This purposely occurs if the hash has already been written to the metadata database
            // but is no longer stored in the cache, e.g., if the system has been restarted.
            let maybe_separator = if tag_columns.is_empty() { "" } else { ", " };
            transaction.execute(
                format!(
                    "INSERT OR IGNORE INTO {}_tags (hash{}{}) VALUES ({}{}{})",
                    model_table.name,
                    maybe_separator,
                    tag_columns,
                    signed_tag_hash,
                    maybe_separator,
                    values
                )
                .as_str(),
                (),
            )?;

            transaction.execute(
                format!(
                    "INSERT OR IGNORE INTO model_table_hash_table_name (hash, table_name) VALUES ({}, '{}')",
                    signed_tag_hash, model_table.name
                )
                .as_str(),
                (),
            )?;

            transaction.commit()?;

            Ok(tag_hash)
        }
    }

    /// Return the error bound for `univariate_id`. Returns an [`Error`](rusqlite::Error) if the
    /// necessary data cannot be retrieved from the metadata database.
    pub fn error_bound(&self, univariate_id: u64) -> Result<ErrorBound> {
        // Open a connection to the database containing the metadata.
        let connection = Connection::open(&self.metadata_database_path)?;

        // SQLite use signed integers https://www.sqlite.org/datatype3.html.
        let tag_hash = MetadataManager::univariate_id_to_tag_hash(univariate_id);
        let signed_tag_hash = i64::from_ne_bytes(tag_hash.to_ne_bytes());
        let mut select_statement = connection.prepare(&format!(
            "SELECT error_bounds FROM model_table_metadata, model_table_hash_table_name
             WHERE model_table_metadata.table_name = model_table_hash_table_name.table_name
             AND hash = {signed_tag_hash}",
        ))?;
        let mut rows = select_statement.query([])?;

        // unwrap() is safe as a model table must be created before data can be inserted into it.
        let error_bounds_bytes = rows.next()?.unwrap().get::<usize, Vec<u8>>(0)?;
        let error_bounds =
            MetadataManager::convert_slice_u8_to_vec_error_bounds(&error_bounds_bytes)?;
        let column_index = MetadataManager::univariate_id_to_column_index(univariate_id);
        Ok(error_bounds[column_index as usize])
    }

    /// Extract the first 54-bits from `univariate_id` which is a hash computed from tags.
    pub fn univariate_id_to_tag_hash(univariate_id: u64) -> u64 {
        univariate_id & 18446744073709550592
    }

    /// Extract the last 10-bits from `univariate_id` which is the index of the time series column.
    pub fn univariate_id_to_column_index(univariate_id: u64) -> u16 {
        (univariate_id & 1023) as u16
    }

    /// Return a mapping from tag hash to table names. Returns an [`Error`](rusqlite::Error) if the
    /// necessary data cannot be retrieved from the metadata database.
    pub fn mapping_from_hash_to_table_name(&self) -> Result<HashMap<u64, String>> {
        // Open a connection to the database containing the metadata.
        let connection = Connection::open(&self.metadata_database_path)?;

        let mut select_statement =
            connection.prepare("SELECT hash, table_name FROM model_table_hash_table_name")?;
        let mut rows = select_statement.query([])?;

        let mut hash_to_table_name = HashMap::new();
        while let Some(row) = rows.next()? {
            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
            let signed_tag_hash = row.get::<usize, i64>(0)?;
            let tag_hash = u64::from_ne_bytes(signed_tag_hash.to_ne_bytes());
            hash_to_table_name.insert(tag_hash, row.get::<usize, String>(1)?);
        }

        Ok(hash_to_table_name)
    }

    /// Return a mapping from tag hashes to the tags in the columns with the names in
    /// `tag_column_names` for the time series in the model table with the name `model_table_name`.
    /// Returns an [`Error`](rusqlite::Error) if the necessary data cannot be retrieved from the
    /// metadata database.
    pub fn mapping_from_hash_to_tags(
        &self,
        model_table_name: &str,
        tag_column_names: &Vec<&str>,
    ) -> Result<HashMap<u64, Vec<String>>> {
        // Return an empty HashMap if no tag column names are passed to keep the signature simple.
        if tag_column_names.is_empty() {
            return Ok(HashMap::new());
        }

        // Open a connection to the database containing the metadata.
        let connection = Connection::open(&self.metadata_database_path)?;

        let mut select_statement = connection.prepare(&format!(
            "SELECT hash,{} FROM {model_table_name}_tags",
            tag_column_names.join(","),
        ))?;
        let mut rows = select_statement.query([])?;

        let mut hash_to_tags = HashMap::new();
        while let Some(row) = rows.next()? {
            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
            let signed_tag_hash = row.get::<usize, i64>(0)?;
            let tag_hash = u64::from_ne_bytes(signed_tag_hash.to_ne_bytes());

            // Add all of the tags in order so they can be directly appended to each row.
            let mut tags = Vec::with_capacity(tag_column_names.len());
            for tag_column_index in 1..=tag_column_names.len() {
                tags.push(row.get::<usize, String>(tag_column_index)?);
            }
            hash_to_tags.insert(tag_hash, tags);
        }

        Ok(hash_to_tags)
    }

    /// Compute the 64-bit univariate ids of the univariate time series to retrieve from the storage
    /// engine using the field columns, tag names, and tag values in the query. Returns a
    /// [`Error`](rusqlite::Error) if the necessary data cannot be retrieved from the metadata
    /// database.
    pub fn compute_univariate_ids_using_fields_and_tags(
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
    }

    /// Compute the 64-bit univariate ids of the univariate time series to retrieve from the storage
    /// engine using the two queries constructed from the fields, tag names, and tag values in the
    /// user's query. Returns a [`Result`] with an [`Error`](rusqlite::Error) if the data cannot be
    /// retrieved from the metadata database, otherwise the univariate ids are returned.
    fn compute_univariate_ids_using_metadata_database(
        &self,
        query_field_columns: &str,
        fallback_field_column: u64,
        query_hashes: &str,
    ) -> Result<Vec<u64>> {
        // Open a connection to the database containing the metadata.
        let connection = Connection::open(&self.metadata_database_path)?;

        // Retrieve the field columns.
        let mut select_statement = connection.prepare(query_field_columns)?;
        let mut rows = select_statement.query([])?;

        let mut field_columns = vec![];
        while let Some(row) = rows.next()? {
            field_columns.push(row.get::<usize, u64>(0)?);
        }

        // Add the fallback field column if the query did not request data for
        // any fields as the storage engine otherwise does not return any data.
        if field_columns.is_empty() {
            field_columns.push(fallback_field_column);
        }

        // Retrieve the hashes and compute the univariate ids;
        let mut select_statement = connection.prepare(query_hashes)?;
        let mut rows = select_statement.query([])?;

        let mut univariate_ids = vec![];
        while let Some(row) = rows.next()? {
            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
            let signed_tag_hash = row.get::<usize, i64>(0)?;
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
    pub fn save_table_metadata(&self, name: &str) -> Result<()> {
        // Add a new row in the table_metadata table to persist the table.
        let connection = Connection::open(&self.metadata_database_path)?;
        connection.execute(
            "INSERT INTO table_metadata (table_name) VALUES (?1)",
            params![name,],
        )?;
        Ok(())
    }

    /// Read the rows in the table_metadata table and use these to register tables in Apache Arrow
    /// DataFusion. If the metadata database could not be opened or the table could not be queried,
    /// return [`rusqlite::Error`].
    pub fn register_tables(
        &self,
        context: &Arc<Context>,
        runtime: &Arc<Runtime>,
    ) -> Result<(), rusqlite::Error> {
        let connection = Connection::open(&self.metadata_database_path)?;

        let mut select_statement = connection.prepare("SELECT table_name FROM table_metadata")?;

        let mut rows = select_statement.query(())?;

        while let Some(row) = rows.next()? {
            if let Err(error) = self.register_table(row, context, runtime) {
                error!("Failed to register table due to: {}.", error);
            }
        }

        Ok(())
    }

    /// Use a row from the table_metadata table to register the table in Apache Arrow DataFusion. If
    /// the metadata database could not be opened or the table could not be queried, return
    /// [`Error`].
    fn register_table(
        &self,
        row: &Row,
        context: &Arc<Context>,
        runtime: &Arc<Runtime>,
    ) -> Result<(), Box<dyn Error>> {
        let name = row.get::<usize, String>(0)?;

        // Compute the path to the folder containing data for the table.
        let table_folder_path = self
            .local_data_folder()
            .join(COMPRESSED_DATA_FOLDER)
            .join(&name);
        let table_folder = table_folder_path
            .to_str()
            .ok_or_else(|| format!("Path for table is not UTF-8: '{name}'"))?;

        // Register table.
        runtime.block_on(context.session.register_parquet(
            &name,
            table_folder,
            ParquetReadOptions::default(),
        ))?;

        info!("Registered table '{}'.", name);
        Ok(())
    }

    /// Save the created model table to the metadata database. This includes creating a tags table
    /// for the model table, adding a row to the model_table_metadata table, and adding a row to the
    /// model_table_field_columns table for each field column.
    pub fn save_model_table_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
    ) -> Result<()> {
        // Convert the schema to bytes so it can be saved as a BLOB in the metadata database.
        let schema_bytes = MetadataManager::convert_schema_to_blob(&model_table_metadata.schema)?;

        // Create a transaction to ensure the database state is consistent across tables.
        let mut connection = Connection::open(&self.metadata_database_path)?;
        let transaction = connection.transaction()?;

        // Add a column definition for each tag column in the schema.
        let tag_columns: String = model_table_metadata
            .tag_column_indices
            .iter()
            .map(|index| {
                let field = model_table_metadata.schema.field(*index);
                format!("{} TEXT NOT NULL", field.name())
            })
            .collect::<Vec<String>>()
            .join(",");

        // Create a table_name_tags SQLite table to save the 54-bit tag hashes when ingesting data.
        // The query is executed with a formatted string since CREATE TABLE cannot take parameters.
        let maybe_separator = if tag_columns.is_empty() { "" } else { ", " };
        transaction.execute(
            format!(
                "CREATE TABLE {}_tags (hash INTEGER PRIMARY KEY{}{}) STRICT",
                model_table_metadata.name, maybe_separator, tag_columns
            )
            .as_str(),
            (),
        )?;

        // Add a new row in the model_table_metadata table to persist the model table.
        transaction.execute(
            "INSERT INTO model_table_metadata (table_name, schema, timestamp_column_index,
             tag_column_indices, error_bounds) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                model_table_metadata.name,
                schema_bytes,
                model_table_metadata.timestamp_column_index,
                MetadataManager::convert_slice_usize_to_vec_u8(
                    &model_table_metadata.tag_column_indices
                ),
                MetadataManager::convert_slice_error_bounds_to_vec_u8(
                    &model_table_metadata.error_bounds
                )
            ],
        )?;

        // Add a row for each field column to the model_table_field_columns table.
        let mut insert_statement = transaction.prepare(
            "INSERT INTO model_table_field_columns (table_name, column_name, column_index)
        VALUES (?1, ?2, ?3)",
        )?;

        for (index, field) in model_table_metadata.schema.fields().iter().enumerate() {
            // Only add a row for the field if it is not the timestamp or a tag.
            let is_timestamp = index == model_table_metadata.timestamp_column_index;
            let in_tag_indices = model_table_metadata.tag_column_indices.contains(&index);

            if !is_timestamp && !in_tag_indices {
                insert_statement.execute(params![
                    model_table_metadata.name,
                    field.name(),
                    index
                ])?;
            }
        }

        // Explicitly drop the statement to drop the borrow of "transaction" before the commit.
        mem::drop(insert_statement);

        transaction.commit()
    }

    /// Convert the rows in the model_table_metadata table to [`ModelTableMetadata`] and use these
    /// to register model tables in Apache Arrow DataFusion. If the metadata database could not be
    /// opened or the table could not be queried, return [`rusqlite::Error`].
    pub fn register_model_tables(&self, context: &Arc<Context>) -> Result<(), rusqlite::Error> {
        let connection = Connection::open(&self.metadata_database_path)?;

        let mut select_statement = connection.prepare(
            "SELECT table_name, schema, timestamp_column_index, tag_column_indices, error_bounds
            FROM model_table_metadata",
        )?;

        let mut rows = select_statement.query(())?;

        while let Some(row) = rows.next()? {
            if let Err(error) = MetadataManager::register_model_table(row, context) {
                error!("Failed to register model table due to: {}.", error);
            }
        }

        Ok(())
    }

    /// Convert a row from the model_table_metadata table to a [`ModelTableMetadata`] and use it to
    /// register model tables in Apache Arrow DataFusion. If the metadata database could not be
    /// opened or the table could not be queried, return [`Error`].
    fn register_model_table(row: &Row, context: &Arc<Context>) -> Result<(), Box<dyn Error>> {
        let name = row.get::<usize, String>(0)?;

        // Convert the BLOBs to the concrete types.
        let schema_bytes = row.get::<usize, Vec<u8>>(1)?;
        let schema = MetadataManager::convert_blob_to_schema(schema_bytes)?;

        let tag_column_indices_bytes = row.get::<usize, Vec<u8>>(3)?;
        let tag_column_indices =
            MetadataManager::convert_slice_u8_to_vec_usize(&tag_column_indices_bytes)?;

        let error_bounds_bytes = row.get::<usize, Vec<u8>>(4)?;
        let error_bounds =
            MetadataManager::convert_slice_u8_to_vec_error_bounds(&error_bounds_bytes)?;

        // Create model table metadata.
        let model_table_metadata = Arc::new(ModelTableMetadata {
            name: name.clone(),
            schema: Arc::new(schema),
            timestamp_column_index: row.get(2)?,
            tag_column_indices,
            error_bounds,
        });

        // Register model table.
        context.session.register_table(
            name.as_str(),
            ModelTable::new(context.clone(), model_table_metadata),
        )?;

        info!("Registered model table '{}'.", name);
        Ok(())
    }

    /// Convert a [`Schema`] to [`Vec<u8>`].
    fn convert_schema_to_blob(schema: &Schema) -> Result<Vec<u8>> {
        let options = IpcWriteOptions::default();
        let schema_as_ipc = SchemaAsIpc::new(schema, &options);
        let ipc_message: IpcMessage = schema_as_ipc.try_into().map_err(|error: ArrowError| {
            rusqlite::Error::InvalidColumnType(1, error.to_string(), Blob)
        })?;
        Ok(ipc_message.0.to_vec())
    }

    /// Return [`Schema`] if `schema_bytes` can be converted to an Apache Arrow
    /// schema, otherwise [`Error`](rusqlite::Error).
    fn convert_blob_to_schema(schema_bytes: Vec<u8>) -> Result<Schema> {
        let ipc_message = IpcMessage(schema_bytes.into());
        Schema::try_from(ipc_message).map_err(|error| {
            let message = format!("Blob is not a valid Schema: {error}");
            rusqlite::Error::InvalidColumnType(1, message, Blob)
        })
    }

    /// Convert a [`&[usize]`] to a [`Vec<u8>`].
    fn convert_slice_usize_to_vec_u8(usizes: &[usize]) -> Vec<u8> {
        usizes.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    /// Convert a [`&[u8]`] to a [`Vec<usize>`] if the length of `bytes` divides
    /// evenly by [`mem::size_of::<usize>()`], otherwise
    /// [`Error`](rusqlite::Error) is returned.
    fn convert_slice_u8_to_vec_usize(bytes: &[u8]) -> Result<Vec<usize>> {
        if bytes.len() % mem::size_of::<usize>() != 0 {
            // rusqlite defines UNKNOWN_COLUMN as usize::MAX;
            Err(rusqlite::Error::InvalidColumnType(
                usize::MAX,
                "Blob is not a vector of usizes".to_owned(),
                Blob,
            ))
        } else {
            // unwrap() is safe as bytes divides evenly by mem::size_of::<usize>().
            Ok(bytes
                .chunks(mem::size_of::<usize>())
                .map(|byte_slice| usize::from_le_bytes(byte_slice.try_into().unwrap()))
                .collect())
        }
    }

    /// Convert a [`&[ErrorBound]`] to a [`Vec<u8>`].
    fn convert_slice_error_bounds_to_vec_u8(error_bounds: &[ErrorBound]) -> Vec<u8> {
        error_bounds
            .iter()
            .flat_map(|eb| eb.to_le_bytes())
            .collect()
    }

    /// Convert a [`&[u8]`] to a [`Vec<ErrorBound>`] if the length of `bytes` divides evenly by
    /// [`mem::size_of::<f32>()`], otherwise [`Error`](rusqlite::Error) is returned.
    fn convert_slice_u8_to_vec_error_bounds(bytes: &[u8]) -> Result<Vec<ErrorBound>> {
        if bytes.len() % mem::size_of::<f32>() != 0 {
            // rusqlite defines UNKNOWN_COLUMN as usize::MAX.
            Err(rusqlite::Error::InvalidColumnType(
                usize::MAX,
                "Blob is not a vector of error bounds".to_owned(),
                Blob,
            ))
        } else {
            Ok(bytes
                .chunks(mem::size_of::<f32>())
                .map(|byte_slice| {
                    // unwrap() is safe as bytes divides evenly by mem::size_of::<f32>().
                    let error_bound_value = f32::from_le_bytes(byte_slice.try_into().unwrap());
                    // unwrap() is safe as the error bound was checked on write.
                    ErrorBound::try_new(error_bound_value).unwrap()
                })
                .collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    use proptest::strategy::Strategy;
    use proptest::{collection, num, prop_assert_eq, proptest};

    use crate::metadata::test_util;

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

    #[test]
    fn test_create_metadata_database_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = test_util::test_metadata_manager(temp_dir.path());
        let metadata_database_path = metadata_manager.metadata_database_path;

        // Verify that the tables were created and has the expected columns.
        let connection = Connection::open(metadata_database_path).unwrap();
        connection
            .execute("SELECT table_name FROM table_metadata", params![])
            .unwrap();

        connection
            .execute(
                "SELECT table_name, schema, timestamp_column_index,
                 tag_column_indices FROM model_table_metadata",
                params![],
            )
            .unwrap();

        connection
            .execute(
                "SELECT hash, table_name FROM model_table_hash_table_name",
                params![],
            )
            .unwrap();

        connection
            .execute(
                "SELECT table_name, column_name, column_index FROM model_table_field_columns",
                params![],
            )
            .unwrap();
    }

    #[test]
    fn test_get_data_folder_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path();
        let metadata_manager = test_util::test_metadata_manager(temp_dir_path);
        assert_eq!(temp_dir_path, metadata_manager.local_data_folder());
    }

    #[test]
    fn test_get_new_tag_hash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = test_util::test_metadata_manager(temp_dir.path());

        let model_table_metadata = test_util::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .unwrap();

        let result = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()]);
        assert!(result.is_ok());

        // When a new tag hash is retrieved, the hash should be saved in the cache.
        assert_eq!(metadata_manager.tag_value_hashes.keys().len(), 1);

        // It should also be saved in the metadata database table.
        let database_path = metadata_manager
            .local_data_folder()
            .join(METADATA_DATABASE_NAME);
        let connection = Connection::open(database_path).unwrap();

        let mut statement = connection
            .prepare("SELECT * FROM model_table_tags")
            .unwrap();
        let mut rows = statement.query(()).unwrap();

        assert_eq!(
            rows.next()
                .unwrap()
                .unwrap()
                .get::<usize, String>(1)
                .unwrap(),
            "tag1"
        );
    }

    #[test]
    fn test_get_existing_tag_hash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = test_util::test_metadata_manager(temp_dir.path());

        let model_table_metadata = test_util::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .unwrap();

        metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .unwrap();
        assert_eq!(metadata_manager.tag_value_hashes.keys().len(), 1);

        // When getting the same tag hash again, it should just be retrieved from the cache.
        let result = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()]);

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

    #[test]
    fn test_compute_univariate_ids_using_fields_and_tags_for_missing_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = test_util::test_metadata_manager(temp_dir.path());

        assert!(metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", None, 10, &[])
            .is_err());
    }

    #[test]
    fn test_compute_univariate_ids_using_fields_and_tags_for_empty_model_table() {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = test_util::test_metadata_manager(temp_dir.path());

        let model_table_metadata = test_util::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .unwrap();

        // Lookup univariate ids using fields and tags for an empty table.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", None, 10, &[])
            .unwrap();
        assert!(univariate_ids.is_empty());
    }

    #[test]
    fn test_compute_univariate_ids_using_no_fields_and_tags_for_model_table() {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = test_util::test_metadata_manager(temp_dir.path());
        initialize_model_table_with_tag_values(&mut metadata_manager, &["tag_value1"]);

        // Lookup all univariate ids for the table by not passing any fields or tags.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", None, 10, &[])
            .unwrap();
        assert_eq!(2, univariate_ids.len());
    }

    #[test]
    fn test_compute_univariate_ids_for_the_fallback_column_using_only_a_tag_column_for_model_table()
    {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = test_util::test_metadata_manager(temp_dir.path());
        initialize_model_table_with_tag_values(&mut metadata_manager, &["tag_value1"]);

        // Lookup the univariate ids for the fallback column by only requesting tag columns.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", Some(&vec![0]), 10, &[])
            .unwrap();
        assert_eq!(1, univariate_ids.len());
    }

    #[test]
    fn test_compute_the_univariate_ids_for_a_specific_field_column_for_model_table() {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = test_util::test_metadata_manager(temp_dir.path());
        initialize_model_table_with_tag_values(
            &mut metadata_manager,
            &["tag_value1", "tag_value2"],
        );

        // Lookup all univariate ids for the table by not requesting ids for columns.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", None, 10, &[])
            .unwrap();
        assert_eq!(4, univariate_ids.len());

        // Lookup univariate ids for the table by requesting ids for a specific field column.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", Some(&vec![1]), 10, &[])
            .unwrap();
        assert_eq!(2, univariate_ids.len());
    }

    #[test]
    fn test_compute_the_univariate_ids_for_a_specific_tag_value_for_model_table() {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = test_util::test_metadata_manager(temp_dir.path());
        initialize_model_table_with_tag_values(
            &mut metadata_manager,
            &["tag_value1", "tag_value2"],
        );

        // Lookup all univariate ids for the table by not requesting ids for a tag value.
        let univariate_ids = metadata_manager
            .compute_univariate_ids_using_fields_and_tags("model_table", None, 10, &[])
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
            .unwrap();
        assert_eq!(2, univariate_ids.len());
    }

    fn initialize_model_table_with_tag_values(
        metadata_manager: &mut MetadataManager,
        tag_values: &[&str],
    ) {
        let model_table_metadata = test_util::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .unwrap();

        for tag_value in tag_values {
            let tag_value = tag_value.to_string();
            metadata_manager
                .lookup_or_compute_tag_hash(&model_table_metadata, &[tag_value])
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

    #[test]
    fn test_save_table_metadata() {
        // Save a table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = test_util::test_metadata_manager(temp_dir.path());

        let table_name = "table_name";
        metadata_manager.save_table_metadata(table_name).unwrap();

        // Retrieve the table from the metadata database.
        let connection =
            Connection::open(metadata_manager.metadata_database_path.to_str().unwrap()).unwrap();
        let mut select_statement = connection
            .prepare("SELECT table_name FROM table_metadata")
            .unwrap();
        let mut rows = select_statement.query(()).unwrap();

        let row = rows.next().unwrap().unwrap();
        let retrieved_table_name = row.get::<usize, String>(0).unwrap();
        assert_eq!(table_name, retrieved_table_name);

        assert!(rows.next().unwrap().is_none());
    }

    #[test]
    fn test_register_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::new().unwrap());
        let context = test_util::test_context(temp_dir.path());

        context
            .metadata_manager
            .save_table_metadata("table_name")
            .unwrap();

        // Register the table with Apache Arrow DataFusion.
        context
            .metadata_manager
            .register_tables(&context, &runtime)
            .unwrap();
    }

    #[test]
    fn test_save_model_table_metadata() {
        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = test_util::test_metadata_manager(temp_dir.path());

        let model_table_metadata = test_util::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .unwrap();

        // Retrieve the model table from the metadata database.
        let connection =
            Connection::open(metadata_manager.metadata_database_path.to_str().unwrap()).unwrap();

        // Check that an empty model_table_tags exists in the metadata database.
        let mut select_statement = connection
            .prepare("SELECT hash FROM model_table_tags")
            .unwrap();
        let mut rows = select_statement.query(()).unwrap();
        assert!(rows.next().unwrap().is_none());

        // Retrieve the rows in model_table_metadata from the metadata database.
        let mut select_statement = connection
            .prepare(
                "SELECT table_name, schema, timestamp_column_index,
                        tag_column_indices, error_bounds FROM model_table_metadata",
            )
            .unwrap();
        let mut rows = select_statement.query(()).unwrap();

        let row = rows.next().unwrap().unwrap();
        assert_eq!("model_table", row.get::<usize, String>(0).unwrap());
        assert_eq!(
            MetadataManager::convert_schema_to_blob(&model_table_metadata.schema).unwrap(),
            row.get::<usize, Vec<u8>>(1).unwrap()
        );
        assert_eq!(1, row.get::<usize, i32>(2).unwrap());
        assert_eq!(
            vec![0, 0, 0, 0, 0, 0, 0, 0],
            row.get::<usize, Vec<u8>>(3).unwrap()
        );
        assert_eq!(
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            row.get::<usize, Vec<u8>>(4).unwrap()
        );

        assert!(rows.next().unwrap().is_none());

        // Retrieve the rows in model_table_field_columns from the metadata database.
        let mut select_statement = connection
            .prepare(
                "SELECT table_name, column_name, column_index
                      FROM model_table_field_columns",
            )
            .unwrap();
        let mut rows = select_statement.query(()).unwrap();

        let row = rows.next().unwrap().unwrap();
        assert_eq!("model_table", row.get::<usize, String>(0).unwrap());
        assert_eq!("field_1", row.get::<usize, String>(1).unwrap());
        assert_eq!(2, row.get::<usize, i32>(2).unwrap());

        let row = rows.next().unwrap().unwrap();
        assert_eq!("model_table", row.get::<usize, String>(0).unwrap());
        assert_eq!("field_2", row.get::<usize, String>(1).unwrap());
        assert_eq!(3, row.get::<usize, i32>(2).unwrap());

        assert!(rows.next().unwrap().is_none());
    }

    #[test]
    fn test_register_model_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let context = test_util::test_context(temp_dir.path());

        let model_table_metadata = test_util::model_table_metadata();
        context
            .metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .unwrap();

        // Register the model table with Apache Arrow DataFusion.
        context
            .metadata_manager
            .register_model_tables(&context)
            .unwrap();
    }

    #[test]
    fn test_blob_to_schema_empty() {
        assert!(MetadataManager::convert_blob_to_schema(vec!(1, 2, 4, 8)).is_err());
    }

    #[test]
    fn test_blob_to_schema_and_schema_to_blob() {
        let model_table_metadata = test_util::model_table_metadata();
        let schema = model_table_metadata.schema;

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

        #[test]
        fn test_error_bound_to_u8_and_u8_to_error_bound(
            percentages in collection::vec(num::f32::POSITIVE, 0..50).prop_map(|percentages| 
                percentages.iter().map(|percentage| percentage % 100.0).collect::<Vec<f32>>())
        ) {
            // The stragegy computing percentages is not perfect as it cannot generate the value
            // 100.0, but it does not seem possible to create a simple stragegy that can generate
            // any positive f32 value from 0.0 to 100.0.
            let error_bounds: Vec<ErrorBound> = percentages
                .iter()
                .map(|percentage| ErrorBound::try_new(*percentage).unwrap())
                .collect();
            let bytes = MetadataManager::convert_slice_error_bounds_to_vec_u8(&error_bounds);
            let usizes = MetadataManager::convert_slice_u8_to_vec_error_bounds(&bytes).unwrap();

            prop_assert_eq!(percentages, usizes);
        }
    }
}

#[cfg(test)]
/// Module with utility functions that return the metadata needed by unit tests.
pub mod test_util {
    use super::*;

    use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
    use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
    use datafusion::execution::runtime_env::RuntimeEnv;
    use modelardb_common::types::{ArrowTimestamp, ArrowValue};
    use modelardb_compression::models::ErrorBound;
    use tokio::sync::RwLock;

    use crate::storage::StorageEngine;

    /// Return a [`Context`] with the metadata manager created by `get_test_metadata_manager()` and
    /// the data folder set to `path`.
    pub fn test_context(path: &Path) -> Arc<Context> {
        let metadata_manager = test_metadata_manager(path);
        let session = test_session_context();
        let runtime = Runtime::new().unwrap();
        let storage_engine = RwLock::new(
            runtime
                .block_on(async {
                    StorageEngine::try_new(path.to_owned(), None, metadata_manager.clone(), true)
                        .await
                })
                .unwrap(),
        );

        Arc::new(Context {
            metadata_manager,
            session,
            storage_engine,
        })
    }

    /// Return a [`MetadataManager`] with 5 MiBs reserved for uncompressed data, 5 MiBs reserved for
    /// compressed data, and the data folder set to `path`. Reducing the amount of reserved memory
    /// makes it faster to run unit tests.
    pub fn test_metadata_manager(path: &Path) -> MetadataManager {
        let mut metadata_manager = MetadataManager::try_new(path, ServerMode::Edge).unwrap();

        metadata_manager.uncompressed_reserved_memory_in_bytes = 5 * 1024 * 1024; // 5 MiB
        metadata_manager.compressed_reserved_memory_in_bytes = 5 * 1024 * 1024; // 5 MiB

        metadata_manager
    }

    /// Return a [`SessionContext`] without any additional optimizer rules.
    pub fn test_session_context() -> SessionContext {
        let config = SessionConfig::new();
        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionState::with_config_rt(config, runtime);
        SessionContext::with_state(state)
    }

    /// Return a [`ModelTableMetadata`] for a model table with a schema containing a tag column, a
    /// timestamp column, and two field columns.
    pub fn model_table_metadata() -> ModelTableMetadata {
        let schema = Schema::new(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
        ]);

        let error_bounds = vec![
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
        ];

        ModelTableMetadata::try_new("model_table".to_owned(), schema, 1, vec![0], error_bounds)
            .unwrap()
    }
}
