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
use std::{fs::File, path::Path};

use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::record::RowAccessor;

/** Public Types **/
pub struct Catalog {
    pub tables: Vec<Table>,
    pub model_tables: Vec<ModelTable>,
}

pub struct Table {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct ModelTable {
    pub name: String,
    pub segment_folder: String,
    pub sampling_intervals: Vec<i32>,
}

/** Public Methods **/
impl Catalog {
    pub fn table_names(&self) -> Vec<String> {
        let mut table_names: Vec<String> = vec![];
        for table in &self.tables {
            table_names.push(table.name.clone());
        }

        for model_table in &self.model_tables {
            table_names.push(model_table.name.clone());
        }
        table_names
    }
}

/** Public Functions **/
pub fn new(data_folder: &str) -> Catalog {
    let mut tables = vec![];
    let mut model_tables = vec![];
    if let Ok(data_folder) = read_dir(data_folder) {
        for dir_entry in data_folder {
            if let Ok(dir_entry) = dir_entry {
                let dir_entry_path = dir_entry.path();
                if let Some(path) = dir_entry_path.to_str() {
                    let file_name = dir_entry.file_name().to_str().unwrap().to_string();
                    if is_parquet_file(&dir_entry) {
                        tables.push(new_table(file_name, path.to_string()));
                        eprintln!("INFO: initialized table {}", path);
                    } else if is_model_folder(&dir_entry) {
                        if let Ok(model_table) = new_model_table(file_name, path.to_string()) {
                            model_tables.push(model_table);
                            eprintln!("INFO: initialized model table {}", path);
                        } else {
                            eprintln!("ERROR: unsupported model table {}", path);
                        }
                    } else {
                        eprintln!("ERROR: unsupported file or folder {}", path);
                    }
                } else {
                    eprintln!("ERROR: name of file or folder is not UTF-8");
                }
            } else {
                let message = dir_entry.unwrap_err().to_string();
                eprintln!("ERROR: unable to read file or folder {}", &message);
            }
        }
    } else {
        eprintln!("ERROR: unable to open data folder {}", &data_folder);
    }
    Catalog {
        tables,
        model_tables,
    }
}

/** Private Methods **/
fn is_parquet_file(dir_entry: &DirEntry) -> bool {
    let mut file = File::open(dir_entry.path()).unwrap();
    let mut magic_bytes = vec![0u8; 4];
    let _ = file.read_exact(&mut magic_bytes);
    magic_bytes == [80, 65, 82, 49] //Magic bytes PAR1
}

fn new_table(file_name: String, path: String) -> Table {
    let name = if let Some(index) = file_name.find('.') {
        file_name[0..index].to_string()
    } else {
        file_name
    };

    Table { name, path }
}

fn is_model_folder(dir_entry: &DirEntry) -> bool {
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

fn new_model_table(table_name: String, table_folder: String) -> Result<ModelTable, ParquetError> {
    //Ensure only supported model types are used
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

    //TODO: Read groups and members
    let time_series_file = table_folder.clone() + "/time_series.parquet";
    let path = Path::new(&time_series_file);
    let mut sampling_intervals = vec![0]; //Tid indexing is one based
    if let Ok(file) = File::open(&path) {
        let reader = SerializedFileReader::new(file)?;
        let rows = reader.get_row_iter(None)?;
        for row in rows {
            sampling_intervals.push(row.get_int(2)?);
        }
    }

    Ok(ModelTable {
        name: table_name,
        segment_folder: table_folder + "/segment",
        sampling_intervals,
    })
}
