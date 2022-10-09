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
use std::collections::HashMap;
use std::hash::Hasher;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use rusqlite::{params, Connection, Result};

use crate::catalog;
use crate::catalog::ModelTableMetadata;
use crate::types::{
    ArrowTimestamp, ArrowValue, CompressedSchema, TimeSeriesId, UncompressedSchema,
};

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

// TODO: is use of pub without getters and setters recommended in the Rust community?
impl MetadataManager {
    /// Return [`MetadataManager`] if a connection can be made to the metadata
    /// database in `data_folder_path`, otherwise [`Error`](rusqlite::Error) is
    /// returned.
    pub fn try_new(data_folder_path: &Path) -> Result<Self> {
        // Compute the path to the metadata database.
        let metadata_database_path = data_folder_path.join(catalog::METADATA_SQLITE_NAME);
        MetadataManager::create_model_table_metadata_tables(&metadata_database_path)?;

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
        Ok(Self {
            metadata_database_path,
            uncompressed_schema,
            compressed_schema,
            tag_value_hashes: HashMap::new(),
            // Default values for parameters.
            uncompressed_reserved_memory_in_bytes: 512 * 1024 * 1024, // 512 MiB
            compressed_reserved_memory_in_bytes: 512 * 1024 * 1024,   // 512 MiB
        })
    }

    /// Return the [`RecordBatch`] schema used for uncompressed segments.
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

            // TODO: Move this to the metadata component when it exists.
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

    /// Save the created model table to the metadata database. This includes
    /// creating a tags table for the model table, adding a row to the
    /// model_table_metadata table, and adding a row to the
    /// model_table_field_columns table for each field column.
    pub fn save_model_table_to_database(
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

    /// If they do not already exist, create the tables used for model table
    /// metadata. A "model_table_metadata" table that can persist model tables
    /// is created. A "columns" table that can save the index of field columns
    /// in specific tables is also created. If the tables already exist or were
    /// successfully created, return [`Ok`], otherwise return
    /// [`rusqlite::Error`].
    fn create_model_table_metadata_tables(metadata_database_path: &Path) -> Result<()> {
        let connection = Connection::open(&metadata_database_path)?;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::metadata::test_util;

    // Tests for get_or_compute_tag_hash().
    #[test]
    fn test_get_new_tag_hash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = test_util::get_test_metadata_manager(temp_dir.path());

        let model_table_metadata = test_util::get_model_table_metadata();
        metadata_manager
            .save_model_table_to_database(&model_table_metadata, vec![])
            .unwrap();

        let result = metadata_manager
            .get_or_compute_tag_hash(&model_table_metadata, &vec!["tag1".to_owned()]);
        assert!(result.is_ok());

        // When a new tag hash is retrieved, the hash should be saved in the cache.
        assert_eq!(metadata_manager.tag_value_hashes.keys().len(), 1);

        // It should also be saved in the metadata database table.
        let database_path = metadata_manager
            .get_data_folder_path()
            .join(catalog::METADATA_SQLITE_NAME);
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
            .save_model_table_to_database(&model_table_metadata, vec![])
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
