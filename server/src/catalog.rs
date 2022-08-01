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
use std::io::Read;
use std::str;
use std::sync::Arc;
use std::{fs::File, fs::OpenOptions, path::Path};

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int32Builder, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::record::RowAccessor;
use tracing::{error, info};
use object_store::path::Path as ObjectStorePath;

/// Metadata for a table.
#[derive(Debug)]
pub struct TableMetadata {
    /// Name of the table.
    pub name: String,
    /// Location of the table's file or folder in an `ObjectStore`.
    pub folder: ObjectStorePath,
}

/// Metadata for a model table.
#[derive(Debug)]
pub struct ModelTableMetadata {
    /// Name of the model table.
    pub name: String,
    /// Location of the table's segment folder in an `ObjectStore`.
    pub segment_folder: ObjectStorePath,
    /// Schema of the Apache Parquet files used by the [JVM-based version of
    /// ModelarDB] for storing segments.
    ///
    /// [JVM-based version of ModelarDB]: https://github.com/modelardata/ModelarDB
    pub segment_group_file_schema: Arc<Schema>,
    /// Cache that maps from time series id to sampling interval for all time
    /// series in the table.
    pub sampling_intervals: Int32Array,
    /// Cache that maps from time series id to members for all time series in
    /// the table. Each `Array` in `denormalized_dimensions` is a level in a
    /// dimension.
    pub denormalized_dimensions: Vec<ArrayRef>,
}

/// Metadata for the tables and model tables in the data folder.
#[derive(Debug)]
pub struct Catalog {
    pub table_metadata: Vec<TableMetadata>,
    pub model_table_metadata: Vec<Arc<ModelTableMetadata>>,
}

impl Catalog {
    /// Scan `data_folder` for tables and model tables and construct a `Catalog`
    /// that contains the metadata necessary to query these tables.
    pub fn new(data_folder: &str) -> Self {
        let mut table_metadata = vec![];
        let mut model_table_metadata = vec![];
        let model_table_segment_group_file_schema = Arc::new(Schema::new(vec![
            Field::new("gid", DataType::Int32, false),
            Field::new("start_time", DataType::Int64, false),
            Field::new("end_time", DataType::Int64, false),
            Field::new("mtid", DataType::Int32, false),
            Field::new("model", DataType::Binary, false),
            Field::new("gaps", DataType::Binary, false),
        ]));

        if let Ok(data_folder) = read_dir(data_folder) {
            for dir_entry in data_folder {
                if let Ok(dir_entry) = dir_entry {
                    let dir_entry_path = dir_entry.path();
                    if let Some(path) = dir_entry_path.to_str() {
                        let file_name = dir_entry.file_name().to_str().unwrap().to_string();
                        // HACK: workaround for datafusion 8.0.0 lowercasing table names in queries.
                        let normalized_file_name = file_name.to_ascii_lowercase();
                        if Self::is_dir_entry_a_table(&dir_entry) {
                            table_metadata
                                .push(Self::new_table_metadata(normalized_file_name, path.to_string()));
                            info!("Initialized table {}.", path);
                        } else if Self::is_dir_entry_a_model_table(&dir_entry) {
                            if let Ok(mtd) = Self::read_model_table_metadata(
                                normalized_file_name,
                                path.to_string(),
                                &model_table_segment_group_file_schema,
                            ) {
                                model_table_metadata.push(mtd);
                                info!("Initialized model table {}.", path);
                            } else {
                                error!("Unsupported model table {}.", path);
                            }
                        } else {
                            error!("Unsupported file or folder {}.", path);
                        }
                    } else {
                        error!("Name of file or folder is not UTF-8.");
                    }
                } else {
                    let message = dir_entry.unwrap_err().to_string();
                    error!("Unable to read file or folder {}.", &message);
                }
            }
        } else {
            error!("Unable to open data folder {}.", &data_folder);
        }
        Catalog {
            table_metadata,
            model_table_metadata,
        }
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

    /// Return `true` if `dir_entry` is a table, otherwise `false`.
    fn is_dir_entry_a_table(dir_entry: &DirEntry) -> bool {
        // TODO: check the files for tables and model tables have the correct schema.
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

    /// Return `true` if `dir_entry` is an Apache Parquet file, otherwise `false`.
    fn is_dir_entry_a_parquet_file(dir_entry: &DirEntry) -> bool {
        // Write permissions are required on Microsoft Windows.
        let mut file = OpenOptions::new()
            .write(true)
            .open(dir_entry.path())
            .unwrap();
        let mut magic_bytes = vec![0u8; 4];
        let _ = file.read_exact(&mut magic_bytes);
        magic_bytes == [80, 65, 82, 49] //Magic bytes PAR1
    }

    fn new_table_metadata(file_name: String, path: String) -> TableMetadata {
        let name = if let Some(index) = file_name.find('.') {
            file_name[0..index].to_string()
        } else {
            file_name
        };

	let folder = ObjectStorePath::parse(path).unwrap();
        TableMetadata { name, folder }
    }

    /// Return `true` if `dir_entry` is a model table, otherwise `false`.
    fn is_dir_entry_a_model_table(dir_entry: &DirEntry) -> bool {
        if let Ok(metadata) = dir_entry.metadata() {
            if metadata.is_dir() {
                return ["model_type.parquet", "time_series.parquet", "segment"]
                    .iter()
                    .map(|required_file_name| {
                        let mut path = dir_entry.path();
                        path.push(required_file_name);
                        Path::new(&path).exists()
                    })
                    .all(|exists| exists);
            }
        }
        false
    }

    /// Read and check the metadata for a model table. Return `ParquetError` if the
    /// metadata cannot be read or if the model table uses unsupported model types.
    fn read_model_table_metadata(
        table_name: String,
        table_folder: String,
        segment_group_file_schema: &Arc<Schema>,
    ) -> Result<Arc<ModelTableMetadata>, ParquetError> {
        // Ensure only supported model types are used.
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

        // Read time series metadata.
        // TODO: read tids and gids so data from time series groups can be correctly decompressed.
        let time_series_file = table_folder.clone() + "/time_series.parquet";
        let path = Path::new(&time_series_file);
        if let Ok(file) = File::open(&path) {
            let reader = SerializedFileReader::new(file)?;
            let parquet_metadata = reader.metadata();
            let row_count = parquet_metadata
                .row_groups()
                .iter()
                .map(|rg| rg.num_rows())
                .sum::<i64>() as usize;

            let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
            let mut record_batch_reader = arrow_reader.get_record_reader(row_count)?;
            let rows = record_batch_reader.next().unwrap()?; //TODO: handle empty Parquet files

            let sampling_intervals = Self::extract_and_shift_int32_column(&rows, 2)?;
            let denormalized_dimensions = Self::extract_and_shift_denormalized_dimensions(&rows, 4)?;

            Ok(Arc::new(ModelTableMetadata {
                name: table_name, // TODO: replace replace() with conversion from local path to object store path?
                segment_folder: ObjectStorePath::parse(table_folder.replace("\\", "/") + "/segment").unwrap(),
                segment_group_file_schema: segment_group_file_schema.clone(),
                sampling_intervals,
                denormalized_dimensions,
            }))
        } else {
            Err(ParquetError::General(format!(
                "unable to read metadata for {}",
                table_name
            )))
        }
    }

    /// Read the array at `column_index` from `rows`, cast it to `Int32Array`, and
    /// increase the index of all values in the array with one.
    fn extract_and_shift_int32_column(
        rows: &RecordBatch,
        column_index: usize,
    ) -> Result<Int32Array, ParquetError> {
        let column = rows
            .column(column_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values();
        let mut shifted_column = Int32Builder::new(column.len() + 1);
        shifted_column.append_value(-1)?; //-1 is stored at index 0 as ids starting at 1 is used for lookup
        shifted_column.append_slice(column)?;
        Ok(shifted_column.finish())
    }

    /// Read the arrays from `first_column_index` to `rows.num_columns()` from
    /// `rows`, cast them to `BinaryArray`, and increase the index of all values in
    /// the arrays with one.
    fn extract_and_shift_denormalized_dimensions(
        rows: &RecordBatch,
        first_column_index: usize,
    ) -> Result<Vec<ArrayRef>, ParquetError> {
        let mut denormalized_dimensions: Vec<ArrayRef> = vec![];
        for level_column_index in first_column_index..rows.num_columns() {
            //TODO: support dimensions with levels that are not strings?
            let level = Self::extract_and_shift_text_column(rows, level_column_index)?;
            denormalized_dimensions.push(level);
        }
        Ok(denormalized_dimensions)
    }

    /// Read the array at `column_index` from `rows`, cast it to `BinaryArray`, and
    /// increase the index of all values in the array with one.
    fn extract_and_shift_text_column(
        rows: &RecordBatch,
        column_index: usize,
    ) -> Result<ArrayRef, ParquetError> {
        let schema = rows.schema();
        let name = schema.field(column_index).name();
        let column = rows
            .column(column_index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut shifted_column =
            StringBuilder::with_capacity(column.len(), column.get_buffer_memory_size());
        shifted_column.append_value(name)?; //The level's name is stored at index 0 as ids starting at 1 is used for lookup
        for member_as_bytes in column {
            let member_as_str = str::from_utf8(member_as_bytes.unwrap())?;
            shifted_column.append_value(member_as_str)?;
        }
        Ok(Arc::new(shifted_column.finish()))
    }
}
