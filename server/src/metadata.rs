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

//! Management of the metadata database which stores the system's configuration
//! and the metadata required for the tables and model tables. Tables can store
//! arbitrary data, while model tables can only store time series as segments
//! containing metadata and models. At runtime the location of the data for the
//! tables and models tables are stored in Apache Arrow DataFusion's catalog,
//! while this module stores the system's configuration and the metadata for the
//! model tables that cannot be stored in Apache Arrow DataFusion's catalog.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::hash::Hasher;
use std::mem;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;

use arrow_flight::IpcMessage;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use datafusion::execution::options::ParquetReadOptions;
use rusqlite::types::Type::Blob;
use rusqlite::{params, Connection, Result, Row};
use tracing::{error, info, warn};

use crate::errors::ModelarDBError;
use crate::tables::ModelTable;
use crate::types::{
    ArrowTimestamp, ArrowValue, CompressedSchema, TimeSeriesId, UncompressedSchema,
};
use crate::Context;

/// Name used for the file containing the SQLite database storing the metadata.
pub const METADATA_SQLITE_NAME: &str = "metadata.sqlite3";

#[derive(Clone)]
pub struct MetadataManager {
    metadata_database_path: PathBuf,
    uncompressed_schema: UncompressedSchema,
    compressed_schema: CompressedSchema,
    /// Cache of tag value hashes used to signify when to persist new unsaved tag combinations.
    tag_value_hashes: HashMap<String, u64>,
    //
    /// Amount of memory to reserve for storing
    /// [`UncompressedSegments`](crate::storage::segment::UncompressedSegment).
    pub uncompressed_reserved_memory_in_bytes: usize,
    /// Amount of memory to reserve for storing compressed segments.
    pub compressed_reserved_memory_in_bytes: usize,
}

impl MetadataManager {
    /// Return [`MetadataManager`] if a connection can be made to the metadata
    /// database in `data_folder_path`, otherwise [`Error`](rusqlite::Error) is
    /// returned.
    pub fn try_new(data_folder_path: &Path) -> Result<Self> {
        if !Self::is_path_a_data_folder(data_folder_path) {
            warn!("The data folder is not empty and does not contain data from ModelarDB");
        }

        // Compute the path to the metadata database.
        let metadata_database_path = data_folder_path.join(METADATA_SQLITE_NAME);

        // Initialize the schema for record batches containing data points.
        let uncompressed_schema = UncompressedSchema(Arc::new(Schema::new(vec![
            Field::new("timestamps", ArrowTimestamp::DATA_TYPE, false),
            Field::new("values", ArrowValue::DATA_TYPE, false),
        ])));

        // Initialize the schema for record batches containing segments.
        let compressed_schema = CompressedSchema(Arc::new(Schema::new(vec![
            Field::new("model_type_id", DataType::UInt8, false),
            Field::new("timestamps", DataType::Binary, false),
            Field::new("start_time", ArrowTimestamp::DATA_TYPE, false),
            Field::new("end_time", ArrowTimestamp::DATA_TYPE, false),
            Field::new("values", DataType::Binary, false),
            Field::new("min_value", ArrowValue::DATA_TYPE, false),
            Field::new("max_value", ArrowValue::DATA_TYPE, false),
            Field::new("error", DataType::Float32, false),
        ])));

        // Create the metadata manager with the default values.
        let metadata_manager = Self {
            metadata_database_path,
            uncompressed_schema,
            compressed_schema,
            tag_value_hashes: HashMap::new(),
            // Default values for parameters.
            uncompressed_reserved_memory_in_bytes: 512 * 1024 * 1024, // 512 MiB
            compressed_reserved_memory_in_bytes: 512 * 1024 * 1024,   // 512 MiB
        };

        // Create the necessary tables in the metadata database.
        metadata_manager.create_metadata_database_tables()?;

        // Return the metadata manager.
        Ok(metadata_manager)
    }

    /// Return the path of the data folder.
    pub fn get_data_folder_path(&self) -> &Path {
        // unwrap() is safe as metadata_database_path is created by self.
        self.metadata_database_path.parent().unwrap()
    }

    /// Return the [`RecordBatch`] schema used for uncompressed segments.
    pub fn get_uncompressed_schema(&self) -> UncompressedSchema {
        self.uncompressed_schema.clone()
    }

    /// Return the [`RecordBatch`] schema used for compressed segments.
    pub fn get_compressed_schema(&self) -> CompressedSchema {
        self.compressed_schema.clone()
    }

    /// Return the tag hash for the given list of tag values either by
    /// retrieving it from a cache or, if the combination of tag values is not
    /// in the cache, by computing a new hash. If the hash is not in the cache,
    /// it is both saved to the cache and persisted to the model_table_tags
    /// table if it does not already contain it. If the model_table_tags table
    /// cannot be accessed, [`rusqlite::Error`] is returned.
    pub fn get_or_compute_tag_hash(
        &mut self,
        model_table: &ModelTableMetadata,
        tag_values: &Vec<String>,
    ) -> Result<u64, rusqlite::Error> {
        let cache_key = {
            let mut cache_key_list = tag_values.clone();
            cache_key_list.push(model_table.name.clone());

            cache_key_list.join(";")
        };

        // Check if the tag hash is in the cache. If it is, retrieve it. If it is not, create a new
        // one and save it both in the cache and in the model_table_tags table.
        if let Some(tag_hash) = self.tag_value_hashes.get(&cache_key) {
            Ok(*tag_hash)
        } else {
            // Generate the 54-bit tag hash based on the tag values of the record batch and model table name.
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
                .map(|index| model_table.schema.field(*index as usize).name().clone())
                .collect::<Vec<String>>()
                .join(",");

            let values = tag_values
                .iter()
                .map(|value| format!("'{}'", value))
                .collect::<Vec<String>>()
                .join(",");

            let connection = Connection::open(&self.metadata_database_path)?;

            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
            let signed_tag_hash = i64::from_ne_bytes(tag_hash.to_ne_bytes());

            // OR IGNORE is used to silently fail when trying to insert an already existing hash.
            connection.execute(
                format!(
                    "INSERT OR IGNORE INTO {}_tags (hash,{}) VALUES ({},{})",
                    model_table.name, tag_columns, signed_tag_hash, values
                )
                .as_str(),
                (),
            )?;

            Ok(tag_hash)
        }
    }

    /// Compute the 64-bit keys of the univariate time series to retrieve from
    /// the storage engine using the fields, tag, and tag values in the query.
    /// Returns a [`Error`](rusqlite::Error) if the necessary data cannot be
    /// retrieved from the metadata database.
    pub fn compute_keys_using_fields_and_tags(
        &self,
        table_name: &str,
        columns: &Option<Vec<usize>>,
        fallback_field_column: u64,
        tag_predicates: &[(&str, &str)],
    ) -> Result<Vec<TimeSeriesId>> {
        // Construct a query that extracts the field columns in the table being
        // queried which overlaps with the columns being requested by the query.
        let query_field_columns = if columns.is_none() {
            format!(
                "SELECT column_index FROM model_table_field_columns WHERE table_name = '{}'",
                table_name
            )
        } else {
            let column_predicates: Vec<String> = columns
                .clone()
                .unwrap()
                .iter()
                .map(|column| format!("column_index = {}", column))
                .collect();

            format!(
                "SELECT column_index FROM model_table_field_columns WHERE table_name = '{}' AND {}",
                table_name,
                column_predicates.join(" OR ")
            )
        };

        // Construct a query that extracts the hashes of the multivariate time
        // series in the table with tag values that match those in the query.
        let query_hashes = {
            if tag_predicates.is_empty() {
                format!("SELECT hash FROM {}_tags", table_name)
            } else {
                let predicates: Vec<String> = tag_predicates
                    .iter()
                    .map(|(tag, tag_value)| format!("{} = {}", tag, tag_value))
                    .collect();

                format!(
                    "SELECT hash FROM {}_tags WHERE {}",
                    table_name,
                    predicates.join(" AND ")
                )
            }
        };

        // Retrieve the hashes using the queries and reconstruct the keys.
        self.compute_keys_using_metadata_database(
            &query_field_columns,
            fallback_field_column,
            &query_hashes,
        )
    }

    /// Normalize `table name` to allow direct comparisons between table names.
    pub fn normalize_table_name(table_name: &str) -> String {
        table_name.to_lowercase()
    }

    /// Save the created table to the metadata database. This mainly includes
    /// adding a row to the table_metadata table with both the name and schema.
    pub fn save_table_metadata(&self, name: &str) -> Result<()> {
        // Add a new row in the model_table_metadata table to persist the model table.
        let connection = Connection::open(&self.metadata_database_path)?;
        connection.execute(
            "INSERT INTO table_metadata (table_name) VALUES (?1)",
            params![name,],
        )?;
        Ok(())
    }

    /// Read the rows in the table_metadata table and use these to register
    /// tables in Apache Arrow DataFusion. If the metadata database could not be
    /// opened or the table could not be queried, return [`rusqlite::Error`].
    pub fn register_tables(&self, context: &Arc<Context>) -> Result<(), rusqlite::Error> {
        let connection = Connection::open(&self.metadata_database_path)?;

        let mut select_statement =
            connection.prepare("SELECT table_name, schema, FROM table_metadata")?;

        let mut rows = select_statement.query(())?;

        while let Some(row) = rows.next()? {
            if let Err(error) = self.register_table(row, &context) {
                error!("Failed to register table due to: {}.", error);
            }
        }

        Ok(())
    }

    /// Save the created model table to the metadata database. This includes
    /// creating a tags table for the model table, adding a row to the
    /// model_table_metadata table, and adding a row to the
    /// model_table_field_columns table for each field column.
    pub fn save_model_table_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
        schema_bytes: Vec<u8>,
    ) -> Result<()> {
        // Create a transaction to ensure the database state is consistent across tables.
        let mut connection = Connection::open(&self.metadata_database_path)?;
        let transaction = connection.transaction()?;

        // Add a column definition for each tag field in the schema.
        let tag_columns: String = model_table_metadata
            .tag_column_indices
            .iter()
            .map(|index| {
                let field = model_table_metadata.schema.field(*index as usize);
                format!("{} TEXT NOT NULL", field.name())
            })
            .collect::<Vec<String>>()
            .join(",");

        // Create a table_name_tags SQLite table to save the 54-bit tag hashes when ingesting data.
        // The query is executed with a formatted string since CREATE TABLE cannot take parameters.
        transaction.execute(
            format!(
                "CREATE TABLE {}_tags (hash INTEGER PRIMARY KEY, {}) STRICT",
                model_table_metadata.name, tag_columns
            )
            .as_str(),
            (),
        )?;

        // Add a new row in the model_table_metadata table to persist the model table.
        transaction.execute(
            "INSERT INTO model_table_metadata (table_name, schema, timestamp_column_index, tag_column_indices)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                model_table_metadata.name,
                schema_bytes,
                model_table_metadata.timestamp_column_index,
                model_table_metadata.tag_column_indices
            ]
        )?;

        // Add a row for each field column to the model_table_field_columns table.
        let mut insert_statement = transaction.prepare(
            "INSERT INTO model_table_field_columns (table_name, column_name, column_index)
        VALUES (?1, ?2, ?3)",
        )?;

        for (index, field) in model_table_metadata.schema.fields().iter().enumerate() {
            // Only add a row for the field if it is not the timestamp or a tag.
            let is_timestamp = index == model_table_metadata.timestamp_column_index as usize;
            let in_tag_indices = model_table_metadata
                .tag_column_indices
                .contains(&(index as u8));

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

    /// Convert the rows in the model_table_metadata table to
    /// [`ModelTableMetadata`] and use these to register model tables in Apache
    /// Arrow DataFusion. If the metadata database could not be opened or the
    /// table could not be queried, return [`rusqlite::Error`].
    pub fn register_model_tables(&self, context: &Arc<Context>) -> Result<(), rusqlite::Error> {
        let connection = Connection::open(&self.metadata_database_path)?;

        let mut select_statement = connection.prepare(
            "SELECT table_name, schema, timestamp_column_index, tag_column_indices
            FROM model_table_metadata",
        )?;

        let mut rows = select_statement.query(())?;

        while let Some(row) = rows.next()? {
            if let Err(error) = MetadataManager::register_model_table(row, &context) {
                error!("Failed to register model table due to: {}.", error);
            }
        }

        Ok(())
    }

    /// Use a row from the table_metadata table to register the table in Apache
    /// Arrow DataFusion. If the metadata database could not be opened or the
    /// table could not be queried, return [`Error`].
    fn register_table(&self, row: &Row, context: &Arc<Context>) -> Result<(), Box<dyn Error>> {
        let name = row.get::<usize, String>(0)?;

        // Compute the path to the folder containing data for the table.
        let table_folder_path = self.get_data_folder_path().join(&name);
        let table_folder = table_folder_path
            .to_str()
            .ok_or_else(|| format!("Path for table is not UTF-8: '{}'", name))?;

        // Register model table.
        context.runtime.block_on(context.session.register_parquet(
            &name,
            table_folder,
            ParquetReadOptions::default(),
        ))?;

        info!("Registered model table '{}'.", name);
        Ok(())
    }

    /// Convert a row from the model_table_metadata table to a
    /// [`ModelTableMetadata`] and use it to register model tables in Apache
    /// Arrow DataFusion. If the metadata database could not be opened or the
    /// table could not be queried, return [`Error`].
    fn register_model_table(row: &Row, context: &Arc<Context>) -> Result<(), Box<dyn Error>> {
        let name = row.get::<usize, String>(0)?;

        // Convert the BLOB to an Apache Arrow Schema.
        let schema_bytes = row.get::<usize, Vec<u8>>(1)?;
        let schema = MetadataManager::blob_to_schema(schema_bytes)?;

        // Create model table metadata.
        let model_table_metadata = Arc::new(ModelTableMetadata {
            name: name.clone(),
            schema: Arc::new(schema),
            timestamp_column_index: row.get(2)?,
            tag_column_indices: row.get(3)?,
        });

        // Register model table.
        context.session.register_table(
            name.as_str(),
            ModelTable::new(context.clone(), model_table_metadata),
        )?;

        info!("Registered model table '{}'.", name);
        Ok(())
    }

    /// Return [`Schema`] if `schema_bytes` can be converted to an Apache Arrow
    /// schema, otherwise [`Error`](rusqlite::Error).
    fn blob_to_schema(schema_bytes: Vec<u8>) -> Result<Schema> {
        let ipc_message = IpcMessage(schema_bytes);
        Schema::try_from(ipc_message).map_err(|error| {
            let message = format!("Blob is not a valid Schema: {}", error);
            rusqlite::Error::InvalidColumnType(1, message, Blob)
        })
    }

    /// Compute the 64-bit keys of the univariate time series to retrieve from
    /// the storage engine using the two queries constructed from the fields,
    /// tag, and tag values in the user's query. Returns a [`RusqliteResult`]
    /// with an [`Error`](rusqlite::Error) if the data cannot be retrieved from
    /// the metadata database, otherwise the keys are returned.
    fn compute_keys_using_metadata_database(
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

        // Retrieve the hashes and compute the keys;
        let mut select_statement = connection.prepare(&query_hashes)?;
        let mut rows = select_statement.query([])?;

        let mut keys = vec![];
        while let Some(row) = rows.next()? {
            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
            let signed_tag_hash = row.get::<usize, i64>(0)?;
            let tag_hash = u64::from_ne_bytes(signed_tag_hash.to_ne_bytes());

            for field_column in &field_columns {
                keys.push(tag_hash | field_column);
            }
        }
        Ok(keys)
    }

    /// If they do not already exist, create the tables used for table and model
    /// table metadata. A "table_metadata" that can persist tables is created, a
    /// "model_table_metadata" table that can persist model tables is created,
    /// and a "columns" table that can save the index of field columns in
    /// specific model tables is created. If the tables exist or were created,
    /// return [`Ok`], otherwise return [`rusqlite::Error`].
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
                tag_column_indices BLOB NOT NULL
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

    /// Return [`true`] if `path` is a data folder, otherwise [`false`].
    fn is_path_a_data_folder(path: &Path) -> bool {
        if let Ok(files_and_folders) = fs::read_dir(path) {
            files_and_folders.count() == 0 || path.join(METADATA_SQLITE_NAME).exists()
        } else {
            false
        }
    }
}

/// Metadata required to ingest data into a model table and query a model table.
#[derive(Debug, Clone)]
pub struct ModelTableMetadata {
    /// Name of the model table.
    pub name: String,
    /// Schema of the data in the model table.
    pub schema: Arc<Schema>,
    /// Index of the timestamp column in the schema.
    pub timestamp_column_index: u8,
    /// Indices of the tag columns in the schema.
    pub tag_column_indices: Vec<u8>,
}

impl ModelTableMetadata {
    /// Create a new model table with the given metadata. If any of the following conditions are
    /// true, [`ConfigurationError`](ModelarDBError::ConfigurationError) is returned:
    /// * The timestamp or tag column indices does not match `schema`.
    /// * The types of the fields are not correct.
    /// * The timestamp column index is in the tag column indices.
    /// * There are duplicates in the tag column indices.
    /// * There are more than 1024 columns.
    /// * There are no field columns.
    pub fn try_new(
        name: String,
        schema: Schema,
        tag_column_indices: Vec<u8>,
        timestamp_column_index: u8,
    ) -> Result<Self, ModelarDBError> {
        // If the timestamp index is in the tag indices, return an error.
        if tag_column_indices.contains(&timestamp_column_index) {
            return Err(ModelarDBError::ConfigurationError(
                "The timestamp column cannot be a tag column.".to_owned(),
            ));
        };

        if let Some(timestamp_field) = schema.fields.get(timestamp_column_index as usize) {
            // If the field of the timestamp column is not of type ArrowTimestamp, return an error.
            if !timestamp_field
                .data_type()
                .equals_datatype(&ArrowTimestamp::DATA_TYPE)
            {
                return Err(ModelarDBError::ConfigurationError(format!(
                    "The timestamp column with index '{}' is not of type '{}'.",
                    timestamp_column_index,
                    ArrowTimestamp::DATA_TYPE
                )));
            }
        } else {
            // If the index of the timestamp column does not match the schema, return an error.
            return Err(ModelarDBError::ConfigurationError(format!(
                "The timestamp column index '{}' does not match a field in the schema.",
                timestamp_column_index
            )));
        }

        for tag_column_index in &tag_column_indices {
            if let Some(tag_field) = schema.fields.get(*tag_column_index as usize) {
                // If the fields of the tag columns is not of type Utf8, return an error.
                if !tag_field.data_type().equals_datatype(&DataType::Utf8) {
                    return Err(ModelarDBError::ConfigurationError(format!(
                        "The tag column with index '{}' is not of type '{}'.",
                        tag_column_index,
                        DataType::Utf8
                    )));
                }
            } else {
                // If the indices for the tag columns does not match the schema, return an error.
                return Err(ModelarDBError::ConfigurationError(format!(
                    "The tag column index '{}' does not match a field in the schema.",
                    tag_column_index
                )));
            }
        }

        let field_column_indices: Vec<usize> = (0..schema.fields().len())
            .filter(|index| {
                // TODO: Change this cast when indices in the action body are changed to use two bytes.
                let index = *index as u8;
                index != timestamp_column_index && !tag_column_indices.contains(&index)
            })
            .collect();

        // If there are no field columns, return an error.
        if field_column_indices.is_empty() {
            return Err(ModelarDBError::ConfigurationError(
                "There needs to be at least one field column.".to_owned(),
            ));
        } else {
            for field_column_index in &field_column_indices {
                // unwrap() is safe to use since the indices are collected from the schema fields.
                let field = schema.fields.get(*field_column_index).unwrap();

                // If the fields of the field columns is not of type ArrowValue, return an error.
                if !field.data_type().equals_datatype(&ArrowValue::DATA_TYPE) {
                    return Err(ModelarDBError::ConfigurationError(format!(
                        "The field column with index '{}' is not of type '{}'.",
                        field_column_index,
                        ArrowValue::DATA_TYPE
                    )));
                }
            }
        }

        // If there are duplicate tag columns, return an error. HashSet.insert() can be used to check
        // for uniqueness since it returns true or false depending on if the inserted element already exists.
        let mut uniq = HashSet::new();
        if !tag_column_indices
            .clone()
            .into_iter()
            .all(|x| uniq.insert(x))
        {
            return Err(ModelarDBError::ConfigurationError(
                "The tag column indices cannot have duplicates.".to_owned(),
            ));
        }

        // If there are more than 1024 columns, return an error. This limitation is necessary
        // since 10 bits are used to identify the column index of the data in the 64-bit hash key.
        if schema.fields.len() > 1024 {
            return Err(ModelarDBError::ConfigurationError(
                "There cannot be more than 1024 columns in the model table.".to_owned(),
            ));
        }

        Ok(Self {
            name,
            schema: Arc::new(schema.clone()),
            timestamp_column_index,
            tag_column_indices,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field};
    use tempfile::tempdir;

    use crate::metadata::test_util;
    use crate::types::{ArrowTimestamp, ArrowValue};

    // Tests for MetadataManager.
    #[test]
    fn test_get_data_folder_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path();
        let metadata_manager = test_util::get_test_metadata_manager(temp_dir_path);
        assert_eq!(temp_dir_path, metadata_manager.get_data_folder_path());
    }

    #[test]
    fn test_get_uncompressed_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = test_util::get_test_metadata_manager(temp_dir.path());
        assert_eq!(
            metadata_manager.uncompressed_schema.0,
            metadata_manager.get_uncompressed_schema().0
        );
    }

    #[test]
    fn test_get_compressed_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = test_util::get_test_metadata_manager(temp_dir.path());
        assert_eq!(
            metadata_manager.compressed_schema.0,
            metadata_manager.get_compressed_schema().0
        );
    }

    #[test]
    fn test_get_new_tag_hash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = test_util::get_test_metadata_manager(temp_dir.path());

        let model_table_metadata = test_util::get_model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, vec![])
            .unwrap();

        let result = metadata_manager
            .get_or_compute_tag_hash(&model_table_metadata, &vec!["tag1".to_owned()]);
        assert!(result.is_ok());

        // When a new tag hash is retrieved, the hash should be saved in the cache.
        assert_eq!(metadata_manager.tag_value_hashes.keys().len(), 1);

        // It should also be saved in the metadata database table.
        let database_path = metadata_manager
            .get_data_folder_path()
            .join(METADATA_SQLITE_NAME);
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
        let mut metadata_manager = test_util::get_test_metadata_manager(temp_dir.path());

        let model_table_metadata = test_util::get_model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, vec![])
            .unwrap();

        metadata_manager
            .get_or_compute_tag_hash(&model_table_metadata, &vec!["tag1".to_owned()])
            .unwrap();
        assert_eq!(metadata_manager.tag_value_hashes.keys().len(), 1);

        // When getting the same tag hash again, it should just be retrieved from the cache.
        let result = metadata_manager
            .get_or_compute_tag_hash(&model_table_metadata, &vec!["tag1".to_owned()]);

        assert!(result.is_ok());
        assert_eq!(metadata_manager.tag_value_hashes.keys().len(), 1);
    }

    // TODO: add tests for compute_keys_using_fields_and_tags.
    // TODO: add tests for normalize_table_name.
    // TODO: add tests for save_table_metadata.
    // TODO: add tests for register_tables.
    // TODO: add tests for register_table.
    // TODO: add tests for save_model_table_metadata.
    // TODO: add tests for register_model_tables.
    // TODO: add tests for register_model_table.
    // TODO: add tests for blob_to_schema.
    // TODO: add tests for compute_keys_using_metadata_database.
    // TODO: add tests for create_metadata_database_tables.

    #[test]
    fn test_a_non_empty_folder_without_metadata_is_not_a_data_folder() {
        let temp_dir = tempdir().unwrap();
        create_empty_folder(temp_dir.path(), "folder");
        assert!(!MetadataManager::is_path_a_data_folder(temp_dir.path()));
    }

    #[test]
    fn test_an_empty_folder_is_a_data_folder() {
        let temp_dir = tempdir().unwrap();
        assert!(MetadataManager::is_path_a_data_folder(temp_dir.path()));
    }

    #[test]
    fn test_a_non_empty_folder_with_metadata_is_a_data_folder() {
        let temp_dir = tempdir().unwrap();
        create_empty_folder(temp_dir.path(), "table_folder");
        fs::create_dir(temp_dir.path().join(METADATA_SQLITE_NAME)).unwrap();
        assert!(MetadataManager::is_path_a_data_folder(temp_dir.path()));
    }

    fn create_empty_folder(path: &Path, name: &str) -> PathBuf {
        let path_buf = path.join(name);
        fs::create_dir(&path_buf).unwrap();
        path_buf
    }

    // Tests for ModelTableMetadata.
    #[test]
    fn test_can_create_model_table_metadata() {
        let schema = get_model_table_schema();
        let result = ModelTableMetadata::try_new("table_name".to_owned(), schema, vec![0, 1, 2], 3);

        assert!(result.is_ok());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_timestamp_index_in_tag_indices() {
        let schema = get_model_table_schema();
        let result = ModelTableMetadata::try_new("table_name".to_owned(), schema, vec![0, 1, 2], 0);

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_timestamp_index() {
        let schema = get_model_table_schema();
        let result =
            ModelTableMetadata::try_new("table_name".to_owned(), schema, vec![0, 1, 2], 10);

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_timestamp_type() {
        let schema = Schema::new(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("timestamp", DataType::UInt8, false),
            Field::new("value", ArrowValue::DATA_TYPE, false),
        ]);

        let result = create_simple_model_table_metadata(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_tag_index() {
        let schema = get_model_table_schema();
        let result =
            ModelTableMetadata::try_new("table_name".to_owned(), schema, vec![0, 1, 10], 3);

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_tag_type() {
        let schema = Schema::new(vec![
            Field::new("tag", DataType::UInt8, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("value", ArrowValue::DATA_TYPE, false),
        ]);

        let result = create_simple_model_table_metadata(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_no_fields() {
        let schema = Schema::new(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        ]);

        let result = create_simple_model_table_metadata(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_field_type() {
        let schema = Schema::new(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("value", DataType::UInt8, false),
        ]);

        let result = create_simple_model_table_metadata(schema);
        assert!(result.is_err());
    }

    /// Return metadata for a model table with one tag column and the timestamp column at index 1.
    fn create_simple_model_table_metadata(
        schema: Schema,
    ) -> Result<ModelTableMetadata, ModelarDBError> {
        ModelTableMetadata::try_new("table_name".to_owned(), schema, vec![0], 1)
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_duplicate_tag_indices() {
        let schema = get_model_table_schema();
        let result = ModelTableMetadata::try_new("table_name".to_owned(), schema, vec![0, 1, 1], 3);

        assert!(result.is_err());
    }

    fn get_model_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("location", DataType::Utf8, false),
            Field::new("install_year", DataType::Utf8, false),
            Field::new("model", DataType::Utf8, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("power_output", ArrowValue::DATA_TYPE, false),
            Field::new("wind_speed", ArrowValue::DATA_TYPE, false),
            Field::new("temperature", ArrowValue::DATA_TYPE, false),
        ])
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_too_many_fields() {
        // Create 1025 fields that can be used to initialize a schema.
        let fields = (0..1025)
            .map(|i| Field::new(format!("field_{}", i).as_str(), DataType::Float32, false))
            .collect::<Vec<Field>>();

        let table_name = "table_name".to_owned();
        let result = ModelTableMetadata::try_new(table_name, Schema::new(fields), vec![0, 1, 2], 3);

        assert!(result.is_err());
    }
}

#[cfg(test)]
/// Module with utility functions that return the metadata needed by unit tests.
pub mod test_util {
    use super::*;

    use tempfile;

    pub fn get_test_metadata_manager(path: &Path) -> MetadataManager {
        let mut metadata_manager = MetadataManager::try_new(path).unwrap();

        metadata_manager.uncompressed_reserved_memory_in_bytes = 5 * 1024 * 1024; // 5 MiB
        metadata_manager.compressed_reserved_memory_in_bytes = 5 * 1024 * 1024; // 5 MiB

        metadata_manager
    }

    pub fn get_model_table_metadata() -> ModelTableMetadata {
        let schema = Schema::new(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("value_1", ArrowValue::DATA_TYPE, false),
            Field::new("value_2", ArrowValue::DATA_TYPE, false),
        ]);

        ModelTableMetadata::try_new("model_table".to_owned(), schema, vec![0], 1).unwrap()
    }

    pub fn get_uncompressed_schema() -> UncompressedSchema {
        let temp_dir = tempfile::tempdir().unwrap();
        get_test_metadata_manager(temp_dir.path()).get_uncompressed_schema()
    }

    pub fn get_compressed_schema() -> CompressedSchema {
        let temp_dir = tempfile::tempdir().unwrap();
        get_test_metadata_manager(temp_dir.path()).get_compressed_schema()
    }
}
