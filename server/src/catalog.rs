/* Copyright 2021 The MiniModelarDB Contributors
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

use std::fs::{read_dir, DirEntry};
use std::io::{Error, ErrorKind, Read};
use std::str;
use std::sync::Arc;
use std::{fs::File, path::Path};

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int32Builder, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::record::RowAccessor;
use object_store::path::Path as ObjectStorePath;
use tracing::{error, info, warn};

/// Metadata for the tables and model tables in the data folder.
#[derive(Debug)]
pub struct Catalog {
    pub table_metadata: Vec<TableMetadata>,
    pub model_table_metadata: Vec<Arc<ModelTableMetadata>>,
}

impl Catalog {
    /// Scan `data_folder` for tables and model tables and construct a `Catalog`
    /// that contains the metadata necessary to query these tables.
    pub fn try_new(data_folder: &str) -> Result<Self, Error> {
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

        if Self::is_path_a_model_table(Path::new(data_folder)) {
            warn!("The data folder contains a model table, please use the parent directory.");
        }

        for maybe_dir_entry in read_dir(data_folder)? {
            // The check if maybe_dir_entry is an error is not performed in
            // try_adding_metadata_for_table_or_model_table as it does not seem
            // possible to return an error without moving it out of a reference.
            if let Ok(ref dir_entry) = maybe_dir_entry {
                if let Err(table_initialization_error) =
                    Self::try_adding_metadata_for_table_or_model_table(
                        dir_entry,
                        &mut table_metadata,
                        &mut model_table_metadata,
                        &model_table_legacy_segment_file_schema,
                    )
                {
                    error!(
                        "Cannot read metadata from {} due to {}.",
                        dir_entry.path().to_string_lossy(),
                        table_initialization_error,
                    );
                }
            } else {
                let reason = maybe_dir_entry.unwrap_err();
                error!("Unable to read file or folder {}.", reason);
            }
        }

        Ok(Catalog {
            table_metadata,
            model_table_metadata,
        })
    }

    /// Return the name of all tables and model tables in the `Catalog`.
    pub fn table_names(&self) -> Vec<String> {
        let mut table_names: Vec<String> = vec![];
        for table_metadata in &self.table_metadata {
            table_names.push(table_metadata.name.clone());
        }

        for model_table_metadata in &self.model_table_metadata {
            table_names.push(model_table_metadata.name.clone());
        }
        table_names
    }

    /// Determine if `dir_entry` is a table, a model table, or neither. If
    /// `dir_entry` is a table the metadata required for the table is added to
    /// `table_metadata`, and if `dir_entry` is a model table the metadata
    /// required for the model table is added to `model_table_metadata`.
    fn try_adding_metadata_for_table_or_model_table(
        dir_entry: &DirEntry,
        table_metadata: &mut Vec<TableMetadata>,
        model_table_metadata: &mut Vec<Arc<ModelTableMetadata>>,
        model_table_legacy_segment_file_schema: &SchemaRef,
    ) -> Result<(), Error> {
        // The extra let binding is required to crate a longer lived value.
        let path = dir_entry.path();
        let path = path.to_str().ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "File or folder path is not UTF-8.")
        })?;

        // The extra let binding is required to crate a longer lived value.
        let file_name = dir_entry.file_name();
        let file_name = file_name.to_str().ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "File or folder name is not UTF-8.")
        })?;

        // HACK: workaround for DataFusion converting table names to lowercase.
        let normalized_file_name = file_name.to_ascii_lowercase();

        // Check if the file or folder is a table, a model table, or neither.
        if Self::is_dir_entry_a_table(&dir_entry) {
            // Initialize table.
            table_metadata.push(TableMetadata::new(normalized_file_name, path.to_string()));
            info!("Found table {}.", path);
        } else if Self::is_path_a_model_table(&dir_entry.path()) {
            // Initialize model table.
            model_table_metadata.push(ModelTableMetadata::try_new(
                normalized_file_name,
                path.to_string(),
                &model_table_legacy_segment_file_schema,
            )?);
            info!("Found model table {}.", path);
        } else {
            error!("File or folder is not a table or model table {}.", path);
        }
        Ok(())
    }

    /// Return `true` if `dir_entry` is a readable table, otherwise `false`.
    fn is_dir_entry_a_table(dir_entry: &DirEntry) -> bool {
        if let Ok(metadata) = dir_entry.metadata() {
            if metadata.is_file() {
                Self::is_dir_entry_a_parquet_file(dir_entry)
            } else if metadata.is_dir() {
                if let Ok(mut data_folder) = read_dir(dir_entry.path()) {
                    data_folder.all(|result| {
                        result.is_ok() && Self::is_dir_entry_a_parquet_file(&result.unwrap())
                    })
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Return `true` if `dir_entry` is an readable Apache Parquet file,
    /// otherwise `false`.
    fn is_dir_entry_a_parquet_file(dir_entry: &DirEntry) -> bool {
        if let Ok(mut file) = File::open(dir_entry.path()) {
            let mut magic_bytes = vec![0u8; 4];
            let _ = file.read_exact(&mut magic_bytes);
            magic_bytes == [80, 65, 82, 49] // Magic bytes PAR1.
        } else {
            false
        }
    }

    /// Return `true` if `path_to_folder` is a model table, otherwise `false`.
    fn is_path_a_model_table(path_to_folder: &Path) -> bool {
        if path_to_folder.exists() && path_to_folder.is_dir() {
            return ["model_type.parquet", "time_series.parquet", "segment"]
                .iter()
                .map(|required_file_name| {
                    let mut path_buf = path_to_folder.to_path_buf();
                    path_buf.push(required_file_name);
                    Path::new(&path_buf).exists()
                })
                .all(|exists| exists);
        }
        false
    }
}

/// Metadata for a table.
#[derive(Debug)]
pub struct TableMetadata {
    /// Name of the table.
    pub name: String,
    /// Location of the table's file or folder in an `ObjectStore`.
    pub folder: String, // Not ObjectStorePath as register_parquet expects &str
}

impl TableMetadata {
    fn new(file_name: String, path: String) -> Self {
        let name = if let Some(index) = file_name.find('.') {
            file_name[0..index].to_string()
        } else {
            file_name
        };

        Self { name, folder: path }
    }
}

/// Metadata for a model table.
#[derive(Debug)]
pub struct ModelTableMetadata {
    /// Name of the model table.
    pub name: String,
    /// Location of the table's segment folder in an `ObjectStore`.
    pub segment_folder: ObjectStorePath,
    // TODO: remove segment_group_file_schema when refactoring query engine.
    /// Schema of the Apache Parquet files used by the [JVM-based version of
    /// ModelarDB] for storing segments.
    ///
    /// [JVM-based version of ModelarDB]: https://github.com/modelardata/ModelarDB
    pub segment_group_file_schema: SchemaRef,
    /// Cache that maps from time series id to sampling interval for all time
    /// series in the table.
    pub sampling_intervals: Int32Array,
    /// Cache that maps from time series id to members for all time series in
    /// the table. Each `Array` in `denormalized_dimensions` is a level in a
    /// dimension.
    pub denormalized_dimensions: Vec<ArrayRef>,
}

impl ModelTableMetadata {
    // TODO: rewrite after implementing the compression component.
    /// Read and check the metadata for a model table produced by the [JVM-based
    /// version of ModelarDB]. Return `ParquetError` if the metadata cannot be
    /// read or if the model table uses unsupported model types.
    ///
    /// [JVM-based version of ModelarDB]: https://github.com/modelardata/ModelarDB
    fn try_new(
        table_name: String,
        table_folder: String,
        segment_group_file_schema: &SchemaRef,
    ) -> Result<Arc<Self>, ParquetError> {
        // Pre-compute the path to the segment folder in the object store.
        let segment_folder = Self::table_folder_to_segment_folder_object_store_path(&table_folder)?;

        // Ensure only supported model types are used.
        Self::check_model_type_metdata(&table_folder)?;

        // Read time series metadata.
        let time_series_file = table_folder.clone() + "/time_series.parquet";
        let path = Path::new(&time_series_file);
        if let Ok(file) = File::open(&path) {
            let rows = Self::read_entire_parquet_file(&table_name, file)?;
            let sampling_intervals = Self::extract_and_shift_int32_column(&rows, 2)?;
            let denormalized_dimensions =
                Self::extract_and_shift_denormalized_dimensions(&rows, 4)?;

            Ok(Arc::new(Self {
                name: table_name,
                segment_folder,
                segment_group_file_schema: segment_group_file_schema.clone(),
                sampling_intervals,
                denormalized_dimensions,
            }))
        } else {
            Err(ParquetError::General(format!(
                "Unable to read metadata for {}.",
                table_name
            )))
        }
    }

    /// Create a `ObjectStorePath` to the segment folder in `table_folder`.
    fn table_folder_to_segment_folder_object_store_path(
        table_folder: &str,
    ) -> Result<ObjectStorePath, ParquetError> {
        let segment_folder = table_folder.to_owned() + "/segment";
        let path = Path::new(&segment_folder);
        ObjectStorePath::from_filesystem_path(path)
            .map_err(|error| ParquetError::General(error.to_string()))
    }

    /// Read all rows in the Apache Parquet `file` for the model table with
    /// `table_name`.
    fn read_entire_parquet_file(
        table_name: &String,
        file: File,
    ) -> Result<RecordBatch, ParquetError> {
        let reader = SerializedFileReader::new(file)?;
        let parquet_metadata = reader.metadata();
        let row_count = parquet_metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .sum::<i64>() as usize;

        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
        let mut record_batch_reader = arrow_reader.get_record_reader(row_count)?;
        let rows = record_batch_reader.next().ok_or_else(|| {
            ParquetError::General(format!("No metadata exists for {}.", *table_name))
        })??;
        Ok(rows)
    }

    /// Check that the model table only use supported model types.
    fn check_model_type_metdata(table_folder: &String) -> Result<(), ParquetError> {
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
                    _ => return Err(ParquetError::General("unsupported model type".to_string())),
                }
            }
        }
        Ok(())
    }

    /// Read the array at `column_index` from `rows`, cast it to `Int32Array`,
    /// and increase the index of all values in the array with one.
    fn extract_and_shift_int32_column(
        rows: &RecordBatch,
        column_index: usize,
    ) -> Result<Int32Array, ParquetError> {
        let column = rows
            .column(column_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ParquetError::ArrowError("Unable to read ids".to_owned()))?
            .values();

        // -1 is stored at index 0 as ids starting at 1 is used for lookup.
        let mut shifted_column = Int32Builder::new(column.len() + 1);
        shifted_column.append_value(-1)?;
        shifted_column.append_slice(column)?;
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
            let level = Self::extract_and_shift_text_column(rows, level_column_index)?;
            denormalized_dimensions.push(level);
        }
        Ok(denormalized_dimensions)
    }

    /// Read the array at `column_index` from `rows`, cast it to `BinaryArray`,
    /// and increase the index of all values in the array with one.
    fn extract_and_shift_text_column(
        rows: &RecordBatch,
        column_index: usize,
    ) -> Result<ArrayRef, ParquetError> {
        let error_message = "Unable to read tags";

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
        shifted_column.append_value(name)?;
        for maybe_member_as_bytes in column {
            let member_as_bytes = maybe_member_as_bytes
                .ok_or_else(|| ParquetError::ArrowError(error_message.to_owned()))?;
            let member_as_str = str::from_utf8(member_as_bytes)?;
            shifted_column.append_value(member_as_str)?;
        }
        Ok(Arc::new(shifted_column.finish()))
    }
}
