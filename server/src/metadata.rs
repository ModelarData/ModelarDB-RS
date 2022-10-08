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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use rusqlite::{Connection, Result};

use crate::catalog;
use crate::types::TimeSeriesId;
use crate::types::{ArrowTimestamp, ArrowValue, CompressedSchema, UncompressedSchema};

pub struct MetadataManager {
    metadata_database_path: PathBuf,
    uncompressed_schema: UncompressedSchema,
    compressed_schema: CompressedSchema,
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
        Connection::open(&metadata_database_path)?;

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
            // Default values for parameters.
            uncompressed_reserved_memory_in_bytes: 512 * 1024 * 1024, // 512 MiB
            compressed_reserved_memory_in_bytes: 512 * 1024 * 1024,   // 512 MiB
        })
    }

    /// Return the [`RecordBatch`] schema used for uncompressed segments.
    pub fn get_uncompressed_schema(&self) -> UncompressedSchema {
        self.uncompressed_schema.clone()
    }

    /// Return the [`RecordBatch`] schema used for compressed segments.
    pub fn get_compressed_schema(&self) -> CompressedSchema {
        self.compressed_schema.clone()
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
}

#[cfg(test)]
/// Module with utility functions that return the metadata needed by unit tests.
pub mod test_util {
    use super::*;

    use tempfile;

    pub fn get_test_metadata_manager() -> MetadataManager {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut metadata_manager = MetadataManager::try_new(temp_dir.path()).unwrap();

        metadata_manager.uncompressed_reserved_memory_in_bytes = 5 * 1024 * 1024; // 5 MiB
        metadata_manager.compressed_reserved_memory_in_bytes = 5 * 1024 * 1024; // 5 MiB

        metadata_manager
    }

    pub fn get_uncompressed_schema() -> UncompressedSchema {
        get_test_metadata_manager().get_uncompressed_schema()
    }

    pub fn get_compressed_schema() -> CompressedSchema {
        get_test_metadata_manager().get_compressed_schema()
    }
}
