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
use std::fs::{read_dir, DirEntry};
use std::io::Read;
use std::sync::Arc;
use std::{fs::File, path::Path};
use std::str;
use std::fs::OpenOptions;

use tracing::{debug, error, info, warn, Level, event, instrument, span};

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, Int32Array, Int32Builder, StringBuilder};
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::parquet::arrow::{ParquetFileArrowReader, ArrowReader};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::record::RowAccessor;

/** Public Types **/
#[derive(Debug)]
pub struct Catalog {
    pub table_metadata: Vec<TableMetadata>,
    pub model_table_metadata: Vec<Arc<ModelTableMetadata>>,
}

#[derive(Debug)]
pub struct TableMetadata {
    pub name: String,
    pub path: String,
}

#[derive(Debug)]
pub struct ModelTableMetadata {
    pub name: String,
    pub segment_folder: String,
    pub segment_group_file_schema: Arc<Schema>,
    pub sampling_intervals: Int32Array,
    pub denormalized_dimensions: Vec<ArrayRef>,
}

/** Public Methods **/
impl Catalog {
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
}

/** Public Functions **/
pub fn new(data_folder: &str) -> Catalog {
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
                    if is_dir_entry_a_table(&dir_entry) {
                        table_metadata.push(new_table_metadata(normalized_file_name, path.to_string()));
                        info!("INFO: initialized table {}", path);
                    } else if is_dir_entry_a_model_table(&dir_entry) {
                        if let Ok(mtd) = read_model_table_metadata(
                            normalized_file_name,
                            path.to_string(),
                            &model_table_segment_group_file_schema,
                        ) {
                            model_table_metadata.push(mtd);
                            info!("INFO: initialized model table {}", path);
                        } else {
                            error!("ERROR: unsupported model table {}", path);
                        }
                    } else {
                        error!("ERROR: unsupported file or folder {}", path);
                    }
                } else {
                    error!("ERROR: name of file or folder is not UTF-8");
                }
            } else {
                let message = dir_entry.unwrap_err().to_string();
                error!("ERROR: unable to read file or folder {}", &message);
            }
        }
    } else {
        error!("ERROR: unable to open data folder {}", &data_folder);
    }
    Catalog {
        table_metadata,
        model_table_metadata,
    }
}

/** Private Methods **/
// TODO: check the files for tables and model tables have the correct schema.
fn is_dir_entry_a_table(dir_entry: &DirEntry) -> bool {
    if let Ok(metadata) = dir_entry.metadata() {
	if metadata.is_file() {
	    is_dir_entry_a_parquet_file(dir_entry)
	} else if metadata.is_dir() {
	    if let Ok(mut data_folder) = read_dir(dir_entry.path()) {
		data_folder.all(|result|  result.is_ok() && is_dir_entry_a_parquet_file(&result.unwrap()))
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

fn is_dir_entry_a_parquet_file(dir_entry: &DirEntry) -> bool {
    let mut file = OpenOptions::new().write(true).open(dir_entry.path()).unwrap();
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

    TableMetadata { name, path }
}

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
        let row_count = parquet_metadata.row_groups().iter().map(|rg| rg.num_rows()).sum::<i64>() as usize;

        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
        let mut record_batch_reader = arrow_reader.get_record_reader(row_count)?;
        let rows = record_batch_reader.next().unwrap()?; //TODO: handle empty Parquet files

        let sampling_intervals = extract_and_shift_int32_column(&rows, 2)?;
        let denormalized_dimensions = extract_and_shift_denormalized_dimensions(&rows, 4)?;

        Ok(Arc::new(ModelTableMetadata {
            name: table_name,
            segment_folder: table_folder + "/segment",
            segment_group_file_schema: segment_group_file_schema.clone(),
            sampling_intervals,
            denormalized_dimensions
        }))
    } else {
        Err(ParquetError::General(format!("unable to read metadata for {}", table_name)))
    }
}

fn extract_and_shift_int32_column(rows: &RecordBatch, column_index: usize) -> Result<Int32Array, ParquetError> {
    let column = rows.column(column_index).as_any().downcast_ref::<Int32Array>().unwrap().values();
    let mut shifted_column = Int32Builder::new(column.len() + 1);
    shifted_column.append_value(-1)?; //-1 is stored at index 0 as ids starting at 1 is used for lookup
    shifted_column.append_slice(column)?;
    Ok(shifted_column.finish())
}

fn extract_and_shift_denormalized_dimensions(rows: &RecordBatch, first_column_index: usize) -> Result<Vec<ArrayRef>, ParquetError> {
    let mut denormalized_dimensions: Vec<ArrayRef> = vec!();
    for level_column_index in first_column_index..rows.num_columns() {
        //TODO: support dimensions with levels that are not strings?
        let level = extract_and_shift_text_column(rows, level_column_index)?;
        denormalized_dimensions.push(level);
    }
    Ok(denormalized_dimensions)
}

fn extract_and_shift_text_column(rows: &RecordBatch, column_index: usize) -> Result<ArrayRef, ParquetError> {
    let schema = rows.schema();
    let name = schema.field(column_index).name();
    let column = rows.column(column_index).as_any().downcast_ref::<BinaryArray>().unwrap();
    let mut shifted_column = StringBuilder::with_capacity(column.len(), column.get_buffer_memory_size());
    shifted_column.append_value(name)?; //The level's name is stored at index 0 as ids starting at 1 is used for lookup
    for member_as_bytes in column {
        let member_as_str = str::from_utf8(member_as_bytes.unwrap())?;
        shifted_column.append_value(member_as_str)?;
    }
    Ok(Arc::new(shifted_column.finish()))
}
