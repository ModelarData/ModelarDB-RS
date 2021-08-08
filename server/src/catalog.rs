use std::fs::{read_dir, DirEntry};
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
pub fn build_catalog(data_folder: &str) -> Catalog {
    let mut tables = vec![];
    let mut model_tables = vec![];
    if let Ok(data_folder) = read_dir(data_folder) {
        for dir_entry in data_folder {
            if let Ok(dir_entry) = dir_entry {
                let dir_entry_path = dir_entry.path();
                if let Some(path) = dir_entry_path.to_str() {
                    let file_name = dir_entry.file_name().to_str().unwrap().to_string();
                    if is_parquet_file(&dir_entry) {
                        let name = if let Some(index) = file_name.find('.') {
                            file_name[0..index].to_string()
                        } else {
                            file_name
                        };
                        tables.push(Table {
                            name,
                            path: path.to_string(),
                        });
                    } else if is_model_folder(&dir_entry) {
                        //TODO: check model_types and read time_series
                        let table_folder = path.to_string();
                        let segment_folder = table_folder.clone() + "/segment";
                        let time_series_file = table_folder.clone() + "/time_series.parquet";
                        if let Ok(sampling_intervals) = read_sampling_intervals(&time_series_file) {
                            model_tables.push(ModelTable {
                                name: file_name,
                                segment_folder,
                                sampling_intervals,
                            });
                        } else {
                            eprintln!("ERROR: no sampling interval for {}", path);
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

fn is_parquet_file(dir_entry: &DirEntry) -> bool {
    if let Some(extension) = dir_entry.path().extension() {
        extension == "parquet"
    } else {
        false
    }
}

fn read_sampling_intervals(time_series_file: &str) -> Result<Vec<i32>, ParquetError> {
    let path = Path::new(time_series_file);
    let mut sampling_intervals = vec![];
    sampling_intervals.push(0); //Tid indexing is one based
    if let Ok(file) = File::open(&path) {
        let reader = SerializedFileReader::new(file)?;
        let rows = reader.get_row_iter(None)?;
        for row in rows {
            sampling_intervals.push(row.get_int(2)?);
        }
    }
    Ok(sampling_intervals)
}
