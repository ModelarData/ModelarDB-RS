/* Copyright 2021 The ModelarDB Contributors
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

//! Metadata for the tables and model tables in the data folder. Tables can
//! store arbitrary data, while model tables can only store time series as
//! models.

use std::collections::HashSet;
use std::fs;
use std::fs::DirEntry;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::sync::Arc;

use arrow_flight::IpcMessage;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Schema};
use rusqlite::types::Type::Blob;
use rusqlite::Connection;
use tracing::{error, info, warn};

use crate::errors::ModelarDBError;
use crate::storage::StorageEngine;
use crate::types::{ArrowTimestamp, ArrowValue};

/// Name used for the file containing the SQLite database storing the metadata.
pub const METADATA_SQLITE_NAME: &str = "metadata.sqlite3";

/// Metadata for the tables and model tables in the data folder.
pub struct Catalog {
    /// Folder containing the tables and model tables.
    pub data_folder_path: PathBuf,
    /// Metadata for the tables in the data folder.
    pub table_metadata: Vec<TableMetadata>,
    /// Metadata for the model tables in the data folder.
    pub model_table_metadata: Vec<Arc<ModelTableMetadata>>,
}

impl Catalog {
    /// Scan `data_folder` for tables and model tables and construct a `Catalog`
    /// that contains the metadata necessary to query these tables. Returns
    /// `Error` if the contents of `data_folder` cannot be read.
    pub fn try_new(data_folder_path: &Path) -> Result<Self, Error> {
        if !Self::is_path_a_data_folder(data_folder_path) {
            warn!("The data folder is not empty and does not contain data from ModelarDB");
        }

        // Add tables that only exists as Apache Parquet files to the catalog.
        let mut table_metadata = vec![];
        for maybe_dir_entry in fs::read_dir(data_folder_path)? {
            // The check for if maybe_dir_entry is an error is not performed in
            // try_adding_table_metadata() as it does not seem possible to
            // return an error without also moving it out of a reference.
            if let Ok(ref dir_entry) = maybe_dir_entry {
                if let Err(maybe_error) =
                    Self::try_adding_table_metadata(dir_entry, &mut table_metadata)
                {
                    error!(
                        "Cannot read metadata from '{}': {}",
                        dir_entry.path().to_string_lossy(),
                        maybe_error,
                    );
                }
            } else {
                let reason = maybe_dir_entry.unwrap_err();
                error!("Unable to read file or folder: {}", reason);
            }
        }

        // Add model tables to the catalog with data from the metadata database.
        let model_table_metadata =
            Self::get_model_table_metadata(data_folder_path).unwrap_or_else(|error| {
                error!(
                    "Cannot register model tables from model_table_metadata: {}",
                    error
                );
                vec![]
            });

        Ok(Self {
            data_folder_path: data_folder_path.to_path_buf(),
            table_metadata,
            model_table_metadata,
        })
    }

    /// If `dir_entry` is a table, the metadata required to query the table is
    /// added to `table_metadata`, if not the function returns without changing
    /// `table_metadata`. An `Error` is returned if `dir_entry` is not UTF-8.
    fn try_adding_table_metadata(
        dir_entry: &DirEntry,
        table_metadata: &mut Vec<TableMetadata>,
    ) -> Result<(), Error> {
        let path = dir_entry.path();
        let path_str = path.to_str().ok_or_else(|| {
            Error::new(ErrorKind::InvalidData, "File or folder path is not UTF-8.")
        })?;

        // The extra let binding is required to create a longer lived value.
        let file_name = dir_entry.file_name();
        let file_name = file_name.to_str().ok_or_else(|| {
            Error::new(ErrorKind::InvalidData, "File or folder name is not UTF-8.")
        })?;

        // HACK: workaround for DataFusion converting table names to lowercase.
        let normalized_file_or_folder_name = file_name.to_ascii_lowercase();

        // Check if the file or folder is METADATA_SQLITE_NAME, so it can be
        // skipped without logging any errors, a table, or neither.
        if path.ends_with(METADATA_SQLITE_NAME) {
            // Skip without emitting any errors.
        } else if Self::is_path_a_table(&path) {
            info!("Found table '{}'.", normalized_file_or_folder_name);
            table_metadata.push(TableMetadata::new(
                normalized_file_or_folder_name,
                path_str.to_owned(),
            ));
        } else if Self::is_path_a_model_table(&path) {
            // Skip without emitting any errors.
        } else {
            error!(
                "File or folder '{}' is not a table or model table.",
                path_str
            );
        }
        Ok(())
    }

    /// Return `true` if `path` is a data folder, otherwise `false`.
    fn is_path_a_data_folder(path: &Path) -> bool {
        if let Ok(files_and_folders) = fs::read_dir(path) {
            files_and_folders.count() == 0 || path.join(METADATA_SQLITE_NAME).exists()
        } else {
            false
        }
    }

    /// Return `true` if `path` is a readable table, otherwise `false`.
    fn is_path_a_table(path: &Path) -> bool {
        if path.is_file() {
            StorageEngine::is_path_an_apache_parquet_file(&path)
        } else if path.is_dir() {
            Self::count_apache_aparquet_file_in_folder(&path) > 0
        } else {
            false
        }
    }

    /// Return `true` if `path` is a readable model table, otherwise `false`.
    fn is_path_a_model_table(path: &Path) -> bool {
        let compressed_path = path.join("compressed");
        compressed_path.exists() && Self::count_apache_aparquet_file_in_folder(&compressed_path) > 0
    }

    /// Return the number of Apache Parquet files in the folder at `path`.
    fn count_apache_aparquet_file_in_folder(path: &Path) -> usize {
        let mut number_of_apache_parquet_files = 0;

        if let Ok(files_or_folders) = fs::read_dir(&path) {
            for file_or_folder in files_or_folders {
                if let Ok(maybe_apache_parquet_file) = file_or_folder {
                    if StorageEngine::is_path_an_apache_parquet_file(
                        &maybe_apache_parquet_file.path(),
                    ) {
                        number_of_apache_parquet_files += 1;
                    }
                }
            }
        }

        number_of_apache_parquet_files
    }

    // TODO: Move to the metadata component when it exists.
    // TODO: Store metadata about tables and models in METADATA_SQLITE_NAME.
    /// Convert the rows in the model_table_metadata table to a list of
    /// [`ModelTableMetadata`] and return it. If the metadata database could not
    /// be opened or the table could not be queried, return [`rusqlite::Error`].
    fn get_model_table_metadata(
        data_folder_path: &Path,
    ) -> Result<Vec<Arc<ModelTableMetadata>>, rusqlite::Error> {
        let database_path = data_folder_path.join(METADATA_SQLITE_NAME);
        let connection = Connection::open(database_path)?;

        let mut select_statement = connection.prepare(
            "SELECT table_name, schema, timestamp_column_index, tag_column_indices
            FROM model_table_metadata",
        )?;

        let mut rows = select_statement.query(())?;
        let mut model_table_metadata = vec![];

        // TODO: Maybe only log errors from single rows instead of failing the entire table read.
        while let Some(row) = rows.next()? {
            let name = row.get::<usize, String>(0)?;

            // Since the schema is saved as a BLOB in the metadata database, convert it back to an Arrow schema.
            let schema_bytes = row.get::<usize, Vec<u8>>(1)?;
            let ipc_message = IpcMessage(schema_bytes);
            let schema = Schema::try_from(ipc_message).map_err(|error| {
                let message = format!("Blob is not a valid Schema: {}", error);
                rusqlite::Error::InvalidColumnType(1, message, Blob)
            })?;

            model_table_metadata.push(Arc::new(ModelTableMetadata {
                name: name.clone(),
                schema,
                timestamp_column_index: row.get(2)?,
                tag_column_indices: row.get(3)?,
            }));

            info!("Found model table '{}'.", name);
        }

        Ok(model_table_metadata)
    }
}

/// Metadata required to query a table.
#[derive(Debug)]
pub struct TableMetadata {
    /// Name of the table.
    pub name: String,
    /// Location of the table's file or folder in an `ObjectStore`.
    pub path: String, // Not ObjectStorePath as register_parquet() need '/path'.
}

impl TableMetadata {
    fn new(file_or_folder_name: String, file_or_folder_path: String) -> Self {
        // The full extension is removed manually as file_prefix is not stable.
        let name = if let Some(index) = file_or_folder_name.find('.') {
            file_or_folder_name[0..index].to_owned()
        } else {
            file_or_folder_name
        };

        Self {
            name,
            path: file_or_folder_path,
        }
    }
}

/// Metadata required to ingest data into a model table and query a model table.
#[derive(Debug, Clone)]
pub struct ModelTableMetadata {
    /// Name of the model table.
    pub name: String,
    /// Schema of the data in the model table.
    pub schema: Schema,
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
            schema: schema.clone(),
            timestamp_column_index,
            tag_column_indices,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field};

    use crate::types::{ArrowTimestamp, ArrowValue};

    // Tests for TableMetadata.
    #[test]
    fn test_table_metadata_zero_suffixes() {
        let table_metadata = TableMetadata::new("folder".to_owned(), "/path/to/folder".to_owned());
        assert_eq!("folder", table_metadata.name);
    }

    #[test]
    fn test_table_metadata_single_suffix() {
        let table_metadata =
            TableMetadata::new("file.parquet".to_owned(), "/path/to/folder".to_owned());
        assert_eq!("file", table_metadata.name);
    }

    #[test]
    fn test_table_metadata_multiple_suffixes() {
        let table_metadata = TableMetadata::new(
            "file.snappy.parquet".to_owned(),
            "/path/to/folder".to_owned(),
        );
        assert_eq!("file", table_metadata.name);
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

        let schema = get_model_table_schema();
        let result = ModelTableMetadata::try_new(
            "table_name".to_owned(),
            Schema::new(fields),
            vec![0, 1, 2],
            3,
        );

        assert!(result.is_err());
    }
}
