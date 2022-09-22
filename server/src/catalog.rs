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
use std::fs::{read_dir, DirEntry};
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::str;
use std::sync::Arc;
use std::{fs::File, path::Path};

use arrow_flight::IpcMessage;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int32Builder, StringBuilder,
};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::record::RowAccessor;
use object_store::path::Path as ObjectStorePath;
use rusqlite::types::Type::Blob;
use rusqlite::Connection;
use tracing::{error, info, warn};
use crate::errors::ModelarDBError;

use crate::storage::StorageEngine;
use crate::types::{ArrowTimestamp, ArrowValue};

/// Metadata for the tables and model tables in the data folder.
pub struct Catalog {
    /// Folder containing the tables and model tables.
    pub data_folder_path: PathBuf,
    /// Metadata for the tables in the data folder.
    pub table_metadata: Vec<TableMetadata>,
    /// Metadata for the model tables in the data folder.
    pub new_model_table_metadata: Vec<NewModelTableMetadata>,
    /// Metadata for the model tables in the data folder.
    pub model_table_metadata: Vec<Arc<ModelTableMetadata>>,
}

impl Catalog {
    /// Scan `data_folder` for tables and model tables and construct a `Catalog`
    /// that contains the metadata necessary to query these tables. Returns
    /// `Error` if the contents of `data_folder` cannot be read.
    pub fn try_new(data_folder_path: &Path) -> Result<Self, Error> {
        let mut table_metadata = vec![];
        let mut model_table_metadata = vec![];
        let model_table_legacy_segment_file_schema = Arc::new(Schema::new(vec![
            Field::new("gid", DataType::Int32, false),
            Field::new("start_time", DataType::Int64, false),
            Field::new("end_time", DataType::Int64, false),
            Field::new("mtid", DataType::Int32, false),
            Field::new("model", DataType::Binary, false),
            Field::new("gaps", DataType::Binary, false),
        ]));

        if Self::is_path_a_table(data_folder_path) {
            warn!("The data folder is a table, please use the parent directory.");
        } else if Self::is_path_a_model_table(data_folder_path) {
            warn!("The data folder is a model table, please use the parent directory.");
        }

        for maybe_dir_entry in read_dir(data_folder_path)? {
            // The check if maybe_dir_entry is an error is not performed in
            // try_adding_table_or_model_table_metadata() as it does not seem
            // possible to return an error without moving it out of a reference.
            if let Ok(ref dir_entry) = maybe_dir_entry {
                if let Err(maybe_error) = Self::try_adding_table_or_model_table_metadata(
                    dir_entry,
                    &mut table_metadata,
                    &mut model_table_metadata,
                    &model_table_legacy_segment_file_schema,
                ) {
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

        // Add model tables using the data from the model_table_metadata metadata database table.
        let new_model_table_metadata = Self::get_new_model_table_metadata(data_folder_path)
            .unwrap_or_else(|error| {
                error!(
                    "Cannot register model tables from model_table_metadata: {}",
                    error
                );
                vec![]
            });

        Ok(Self {
            data_folder_path: data_folder_path.to_path_buf(),
            table_metadata,
            new_model_table_metadata,
            model_table_metadata,
        })
    }

    /// Determine if `dir_entry` is a table, a model table, or neither. If
    /// `dir_entry` is a table the metadata required to query the table is added
    /// to `table_metadata`, and if `dir_entry` is a model table the metadata
    /// required to query the model table is added to `model_table_metadata`.
    fn try_adding_table_or_model_table_metadata(
        dir_entry: &DirEntry,
        table_metadata: &mut Vec<TableMetadata>,
        model_table_metadata: &mut Vec<Arc<ModelTableMetadata>>,
        model_table_legacy_segment_file_schema: &SchemaRef,
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

        // Check if the file or folder is a table, a model table, or neither.
        if Self::is_path_a_table(&path) {
            table_metadata.push(TableMetadata::new(
                normalized_file_or_folder_name,
                path_str.to_owned(),
            ));
            info!("Found table '{}'.", path_str);
        } else if Self::is_path_a_model_table(&path) {
            model_table_metadata.push(ModelTableMetadata::try_new(
                normalized_file_or_folder_name, // Only folder for model tables.
                path_str.to_owned(),
                &model_table_legacy_segment_file_schema,
            )?);
            info!("Found model table '{}'.", path_str);
        } else {
            error!(
                "File or folder '{}' is not a table or model table.",
                path_str
            );
        }
        Ok(())
    }

    /// Return `true` if `path` is a readable table, otherwise `false`.
    fn is_path_a_table(path: &Path) -> bool {
        if path.is_file() {
            StorageEngine::is_path_an_apache_parquet_file(&path)
        } else if path.is_dir() {
            let table_folder = read_dir(&path);
            table_folder.is_ok()
                && table_folder.unwrap().all(|result| {
                    result.is_ok()
                        && StorageEngine::is_path_an_apache_parquet_file(&result.unwrap().path())
                })
        } else {
            false
        }
    }

    /// Return `true` if `path_to_folder` is a model table, otherwise `false`.
    fn is_path_a_model_table(path_to_folder: &Path) -> bool {
        if path_to_folder.is_dir() {
            // Construction of the Paths is duplicated as it seems impossible to
            // create a function that accepts a Path and returns a new Path.
            let mut model_type = path_to_folder.to_path_buf();
            model_type.push("model_type.parquet");
            let model_type = model_type.as_path();

            let mut time_series = path_to_folder.to_path_buf();
            time_series.push("time_series.parquet");
            let time_series = time_series.as_path();

            let mut segment = path_to_folder.to_path_buf();
            segment.push("segment");
            let segment = segment.as_path();
            let segment_folder = read_dir(&segment);

            StorageEngine::is_path_an_apache_parquet_file(model_type)
                && StorageEngine::is_path_an_apache_parquet_file(time_series)
                && segment_folder.is_ok()
                && segment_folder.unwrap().all(|result| {
                    result.is_ok()
                        && StorageEngine::is_path_an_apache_parquet_file(&result.unwrap().path())
                })
        } else {
            false
        }
    }

    // TODO: Move to the metadata component when it exists.
    // TODO: Rename to "get_model_table_metadata" when the old version is removed.
    /// Convert the rows in the model_table_metadata table to a list of [`NewModelTableMetadata`]
    /// and return it. If the metadata database could not be opened or the table could not be queried,
    /// return [`rusqlite::Error`].
    fn get_new_model_table_metadata(
        data_folder_path: &Path,
    ) -> Result<Vec<NewModelTableMetadata>, rusqlite::Error> {
        let database_path = data_folder_path.join("metadata.sqlite3");
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

            model_table_metadata.push(NewModelTableMetadata {
                name: name.clone(),
                schema,
                timestamp_column_index: row.get(2)?,
                tag_column_indices: row.get(3)?,
            });

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

// TODO: Change name to "ModelTableMetadata" when the old version is removed.
/// Metadata required to ingest data into a model table and query a model table.
#[derive(Clone)]
pub struct NewModelTableMetadata {
    /// Name of the model table.
    pub name: String,
    /// Schema of the data in the model table.
    pub schema: Schema,
    /// Index of the timestamp column in the schema.
    pub timestamp_column_index: u8,
    /// Indices of the tag columns in the schema.
    pub tag_column_indices: Vec<u8>,
}

impl NewModelTableMetadata {
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
                "The timestamp column cannot be a tag column.".to_owned()
            ));
        };

        if let Some(timestamp_field) = schema.fields.get(timestamp_column_index as usize) {
            // If the field of the timestamp column is not of type ArrowTimestamp, return an error.
            if !timestamp_field.data_type().equals_datatype(&ArrowTimestamp::DATA_TYPE) {
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

        let field_column_indices: Vec<usize> = (0..schema.fields().len()).filter(|index| {
            // TODO: Change this cast when indices in the action body are changed to use two bytes.
            let index = *index as u8;
            index != timestamp_column_index && !tag_column_indices.contains(&index)
        }).collect();

        // If there are no field columns, return an error.
        if field_column_indices.is_empty() {
            return Err(ModelarDBError::ConfigurationError(
                "There needs to be at least one field column.".to_owned()
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
                "The tag column indices cannot have duplicates.".to_owned()
            ));
        }

        // If there are more than 1024 columns, return an error. This limitation is necessary
        // since 10 bits are used to identify the column index of the data in the 64-bit hash key.
        if schema.fields.len() > 1024 {
            return Err(ModelarDBError::ConfigurationError(
                "There cannot be more than 1024 columns in the model table.".to_owned()
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

// TODO: update after implementing the compression component.
/// Metadata required to query a model table.
#[derive(Debug)]
pub struct ModelTableMetadata {
    /// Name of the model table.
    pub name: String,
    /// Location of the model table's segment folder in an `ObjectStore`.
    pub segment_path: ObjectStorePath,
    // TODO: remove segment_file_legacy_schema when refactoring query engine.
    /// Schema of the Apache Parquet files used by the [JVM-based version of
    /// ModelarDB] for storing segments.
    ///
    /// [JVM-based version of ModelarDB]: https://github.com/modelardata/ModelarDB
    pub segment_file_legacy_schema: SchemaRef,
    /// Cache that maps from time series id to sampling interval for all time
    /// series in the table.
    pub sampling_intervals: Int32Array,
    // TODO: rename dimension members to tags when refactoring query engine.
    /// Cache that maps from time series id to data warehouse dimension members
    /// for all time series in the table. Each `Array` in
    /// `denormalized_dimensions` is a level in a dimension.
    pub denormalized_dimensions: Vec<ArrayRef>,
}

impl ModelTableMetadata {
    /// Read and check the metadata for a model table produced by the [JVM-based
    /// version of ModelarDB]. Return `ParquetError` if the metadata cannot be
    /// read or if the model table uses unsupported model types.
    ///
    /// [JVM-based version of ModelarDB]: https://github.com/modelardata/ModelarDB
    fn try_new(
        folder_name: String,
        folder_path: String,
        segment_file_legacy_schema: &SchemaRef,
    ) -> Result<Arc<Self>, ParquetError> {
        // Pre-compute the path to the segment folder in the object store.
        let segment_path = Self::create_object_store_path_to_segment_folder(&folder_path)?;

        // Ensure only supported model types are used.
        Self::check_model_type_metadata(&folder_path)?;

        // Read time series metadata. The sampling intervals and dimension
        // members are shifted by one as the time series ids start at one.
        let time_series_file = folder_path.clone() + "/time_series.parquet";
        let path = Path::new(&time_series_file);
        if let Ok(rows) = StorageEngine::read_entire_apache_parquet_file(path) {
            let sampling_intervals = Self::extract_and_shift_int32_array(&rows, 2)?;
            let denormalized_dimensions = // Columns with metadata as strings.
                Self::extract_and_shift_denormalized_dimensions(&rows, 4)?;

            Ok(Arc::new(Self {
                name: folder_name,
                segment_path,
                segment_file_legacy_schema: segment_file_legacy_schema.clone(),
                sampling_intervals,
                denormalized_dimensions,
            }))
        } else {
            Err(ParquetError::General(format!(
                "Unable to read metadata for folder '{}'.",
                folder_name
            )))
        }
    }

    /// Create an `ObjectStorePath` to the segment folder in
    /// `model_table_folder`.
    fn create_object_store_path_to_segment_folder(
        model_table_folder: &str,
    ) -> Result<ObjectStorePath, ParquetError> {
        let segment_folder = model_table_folder.to_owned() + "/segment";
        let path = Path::new(&segment_folder);
        ObjectStorePath::from_filesystem_path(path)
            .map_err(|error| ParquetError::General(error.to_string()))
    }

    /// Check that the model table only use supported model types.
    fn check_model_type_metadata(table_folder: &String) -> Result<(), ParquetError> {
        let model_types_file = table_folder.clone() + "/model_type.parquet";
        let path = Path::new(&model_types_file);
        if let Ok(file) = File::open(&path) {
            let reader = SerializedFileReader::new(file)?;
            let rows = reader.get_row_iter(None)?;
            for row in rows {
                let mtid = row.get_int(0)?;
                let name = row.get_bytes(1)?.as_utf8()?;
                match (mtid, name) {
                    (1, "dk.aau.modelardb.core.models.UncompressedModelType") => (),
                    (2, "dk.aau.modelardb.core.models.PMC_MeanModelType") => (),
                    (3, "dk.aau.modelardb.core.models.SwingFilterModelType") => (),
                    (4, "dk.aau.modelardb.core.models.FacebookGorillaModelType") => (),
                    _ => return Err(ParquetError::General("Unsupported model type.".to_owned())),
                }
            }
        }
        Ok(())
    }

    /// Read the array at `column_index` from `rows`, cast it to `Int32Array`,
    /// and increase the index of all values in the array with one.
    fn extract_and_shift_int32_array(
        rows: &RecordBatch,
        column_index: usize,
    ) -> Result<Int32Array, ParquetError> {
        let column = rows
            .column(column_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                ParquetError::ArrowError("Unable to read sampling intervals.".to_owned())
            })?
            .values();

        // -1 is stored at index 0 as time series ids starting at 1 is used for
        // lookup. Time series ids starting at 1 is used for compatibility with
        // the JVM-based version of ModelarDB.
        let mut shifted_column = Int32Builder::with_capacity(column.len() + 1);
        shifted_column.append_value(-1);
        shifted_column.append_slice(column);
        Ok(shifted_column.finish())
    }

    /// Read the arrays from `first_column_index` to `rows.num_columns()` from
    /// `rows`, cast them to `BinaryArray`, and increase the index of all values
    /// in the arrays with one.
    fn extract_and_shift_denormalized_dimensions(
        rows: &RecordBatch,
        first_column_index: usize,
    ) -> Result<Vec<ArrayRef>, ParquetError> {
        let mut denormalized_dimensions: Vec<ArrayRef> = vec![];
        for level_column_index in first_column_index..rows.num_columns() {
            let level = Self::extract_and_shift_string_array(rows, level_column_index)?;
            denormalized_dimensions.push(level);
        }
        Ok(denormalized_dimensions)
    }

    /// Read the array at `column_index` from `rows`, cast it to `BinaryArray`,
    /// and increase the index of all values in the array with one.
    fn extract_and_shift_string_array(
        rows: &RecordBatch,
        column_index: usize,
    ) -> Result<ArrayRef, ParquetError> {
        let error_message = "Unable to read tags.";

        let schema = rows.schema();
        let name = schema.field(column_index).name();
        let column = rows
            .column(column_index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| ParquetError::ArrowError(error_message.to_owned()))?;

        // The level's name is at index 0 as ids from 1 is used for lookup.
        let mut shifted_column =
            StringBuilder::with_capacity(column.len(), column.get_buffer_memory_size());
        shifted_column.append_value(name);
        for maybe_member_as_bytes in column {
            let member_as_bytes = maybe_member_as_bytes
                .ok_or_else(|| ParquetError::ArrowError(error_message.to_owned()))?;
            let member_as_str = str::from_utf8(member_as_bytes)?;
            shifted_column.append_value(member_as_str);
        }
        Ok(Arc::new(shifted_column.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::ArrowPrimitiveType;
    use proptest::num;
    use proptest::string;
    use proptest::{prop_assert, prop_assert_eq, proptest};

    use crate::types::{ArrowTimestamp, ArrowValue};

    // Tests for Catalog.
    #[test]
    fn test_empty_catalog_table_and_model_table_names() {
        let catalog = Catalog {
            data_folder_path: PathBuf::from(""),
            table_metadata: vec![],
            new_model_table_metadata: vec![],
            model_table_metadata: vec![],
        };
        assert!(catalog.table_and_model_table_names().is_empty())
    }

    #[test]
    fn test_catalog_table_and_model_table_names() {
        let mut catalog = Catalog {
            data_folder_path: PathBuf::from(""),
            table_metadata: vec![],
            new_model_table_metadata: vec![],
            model_table_metadata: vec![],
        };

        catalog.table_metadata.push(TableMetadata {
            name: "table".to_owned(),
            path: "".to_owned(),
        });

        catalog
            .model_table_metadata
            .push(Arc::new(ModelTableMetadata {
                name: "model_table".to_owned(),
                segment_path: ObjectStorePath::parse("").unwrap(),
                segment_file_legacy_schema: Arc::new(Schema::new(vec![])),
                sampling_intervals: Int32Array::builder(0).finish(),
                denormalized_dimensions: vec![],
            }));

        assert_eq!(
            vec!("table", "model_table"),
            catalog.table_and_model_table_names()
        )
    }

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

    // Tests for NewModelTableMetadata.
    #[test]
    fn test_can_create_model_table_metadata() {
        let schema = get_model_table_schema();
        let result = NewModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            vec![0, 1, 2],
            3,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_timestamp_index_in_tag_indices() {
        let schema = get_model_table_schema();
        let result = NewModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            vec![0, 1, 2],
            0,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_invalid_timestamp_index() {
        let schema = get_model_table_schema();
        let result = NewModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            vec![0, 1, 2],
            10,
        );

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
        let result = NewModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            vec![0, 1, 10],
            3,
        );

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
        schema: Schema
    ) -> Result<NewModelTableMetadata, ModelarDBError> {
        NewModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            vec![0],
            1,
        )
    }

    #[test]
    fn test_cannot_create_model_table_metadata_with_duplicate_tag_indices() {
        let schema = get_model_table_schema();
        let result = NewModelTableMetadata::try_new(
            "table_name".to_owned(),
            schema,
            vec![0, 1, 1],
            3,
        );

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
        let result = NewModelTableMetadata::try_new(
            "table_name".to_owned(),
            Schema::new(fields),
            vec![0, 1, 2],
            3,
        );

        assert!(result.is_err());
    }

    // Tests for ModelTableMetadata.
    #[test]
    fn test_extract_and_shift_empty_int32_array() {
        let array = Int32Array::from(vec![] as Vec<i32>);
        assert_eq!(0, array.len());

        let record_batch = create_record_batch_with_int32_array(array);
        let maybe_shifted_array =
            ModelTableMetadata::extract_and_shift_int32_array(&record_batch, 0);
        assert!(maybe_shifted_array.is_ok());

        let shifted_array = maybe_shifted_array.unwrap();
        assert_eq!(1, shifted_array.len());
        assert_eq!(-1, shifted_array.value(0));
    }

    proptest! {
    #[test]
    fn test_extract_and_shift_int32_array_with_value(value in num::i32::ANY) {
        let array = Int32Array::from(vec![value]);
        assert_eq!(1, array.len());

        let record_batch = create_record_batch_with_int32_array(array);
        let maybe_shifted_array =
            ModelTableMetadata::extract_and_shift_int32_array(&record_batch, 0);
        prop_assert!(maybe_shifted_array.is_ok());

        let shifted_array = maybe_shifted_array.unwrap();
        prop_assert_eq!(2, shifted_array.len());
        assert_eq!(-1, shifted_array.value(0));
        prop_assert_eq!(value, shifted_array.value(1));
    }
    }

    fn create_record_batch_with_int32_array(array: Int32Array) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("array", DataType::Int32, false)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_extract_and_shift_empty_text_array() {
        let array = BinaryArray::from_vec(vec![]);
        assert_eq!(0, array.len());

        let record_batch = create_record_batch_with_string_array(array);
        let maybe_shifted_array =
            ModelTableMetadata::extract_and_shift_string_array(&record_batch, 0);
        assert!(maybe_shifted_array.is_ok());

        // The extra let binding is required to create a longer lived value.
        let shifted_array = maybe_shifted_array.unwrap();
        let shifted_array = shifted_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(1, shifted_array.len());
        assert_eq!("array", shifted_array.value(0));
    }

    proptest! {
    #[test]
    fn test_extract_and_shift_text_array_with_value(
        value in string::string_regex("[a-zA-Z]+").unwrap()
    ) {
        let array = BinaryArray::from_vec(vec![value.as_bytes()]);
        assert_eq!(1, array.len());

        let record_batch = create_record_batch_with_string_array(array);
        let maybe_shifted_array =
            ModelTableMetadata::extract_and_shift_string_array(&record_batch, 0);
        prop_assert!(maybe_shifted_array.is_ok());

        // The extra let binding is required to create a longer lived value.
        let shifted_array = maybe_shifted_array.unwrap();
        let shifted_array = shifted_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        prop_assert_eq!(2, shifted_array.len());
        assert_eq!("array", shifted_array.value(0));
        prop_assert_eq!(value, shifted_array.value(1));
    }
    }

    fn create_record_batch_with_string_array(array: BinaryArray) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("array", DataType::Binary, false)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
    }
}
