use std::fs::DirEntry;
use std::path::Path;

/** Public Types **/
//TODO: reuse existing type in datafusion catalog stop copying?
pub struct Catalog {
    pub cores: usize,
    pub tables: Vec<Table>,
    pub model_tables: Vec<ModelTable>
}

pub struct Table {
    pub name: String,
    pub path: String
}

#[derive(Debug)]
pub struct ModelTable {
    pub name: String,
    pub segment_folder: String,
    pub sampling_intervals: Vec<i32>
}


/** Public Methods **/
pub fn build_catalog(data_folder: &str) -> Catalog {
    let mut tables = vec!();
    let mut model_tables = vec!();
    if let Ok(data_folder) = std::fs::read_dir(data_folder) {
	for dir_entry in data_folder {
	    if let Ok(dir_entry) = dir_entry {
		let dir_entry_path = dir_entry.path();
		if let Some(path) = dir_entry_path.to_str() {
		    let file_name = dir_entry.file_name()
			.to_str().unwrap().to_string();
		    if is_parquet_file(&dir_entry) {
			let name = if let Some(index) = file_name.find(".") {
			    file_name[0..index].to_string()
			} else {
			    file_name
			};
			tables.push(Table { name, path: path.to_string() });
		    } else if is_model_folder(&dir_entry) {
			//TODO: check model_types and read time_series
			let segment_folder = path.to_string() + "/segment";
			model_tables.push(
			    ModelTable { name: file_name, segment_folder,
					 sampling_intervals: vec![60000;30] });
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
    //TODO: read cores from system instead of being hard-codded
    Catalog { cores: 4, tables, model_tables }
}

/** Private Methods **/
fn is_model_folder(dir_entry: &DirEntry) -> bool {
    if let Ok(metadata) = dir_entry.metadata() {
	if metadata.is_dir() {
	    return ["model_type.parquet", "time_series.parquet", "segment"]
		.iter().map(|required_file_name| {
		    let mut path = dir_entry.path();
		    path.push(required_file_name);
		    Path::new(&path).exists()
		}).all(|exists| exists)
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
