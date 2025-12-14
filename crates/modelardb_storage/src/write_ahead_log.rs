/* Copyright 2025 The ModelarDB Contributors
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

//! Implementation of types that provide a write-ahead log for ModelarDB that can be used to
//! efficiently persist data and operations on disk to avoid data loss and enable crash recovery.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use crate::WRITE_AHEAD_LOG_FOLDER;
use crate::data_folder::DataFolder;
use crate::error::{ModelarDbStorageError, Result};


/// Write-ahead log that logs data on a per-table level and operations separately.
pub struct WriteAheadLog {
    log_folder_path: PathBuf,
    table_logs: HashMap<String, WriteAheadLogFile>,
    operation_log: WriteAheadLogFile,
}

impl WriteAheadLog {
    /// Create a new [`WriteAheadLog`] that stores the log in the root of `local_data_folder` in
    /// the [`WRITE_AHEAD_LOG_FOLDER`] folder. If the folder does not exist, it is created. If the
    /// log could not be created, return [`ModelarDbStorageError`].
    pub async fn try_new(local_data_folder: &DataFolder) -> Result<Self> {
        // Create the folder for the write-ahead log if it does not exist.
        let location = local_data_folder.location();
        let log_folder_path = PathBuf::from(format!("{location}/{WRITE_AHEAD_LOG_FOLDER}"));

        std::fs::create_dir_all(log_folder_path.clone())?;

        let mut write_ahead_log = Self {
            log_folder_path: log_folder_path.clone(),
            table_logs: HashMap::new(),
            operation_log: WriteAheadLogFile::try_new(log_folder_path.join("operations.wal"))?,
        };

        // For each time series table, create a log file if it does not already exist.
        for table_name in local_data_folder.time_series_table_names().await? {
            write_ahead_log.create_table_log(&table_name)?;
        }

        Ok(write_ahead_log)
    }

    /// Create a new [`WriteAheadLogFile`] for the table with the given name. If a log already
    /// exists in the map or the log file could not be created, return [`ModelarDbStorageError`].
    /// Note that if the log file already exists, but it is not present in the map, the existing
    /// log file will be added to the map.
    pub fn create_table_log(&mut self, table_name: &str) -> Result<()> {
        if !self.table_logs.contains_key(table_name) {
            let table_log_path = self.log_folder_path.join(format!("{}.wal", table_name));

            self.table_logs.insert(
                table_name.to_owned(),
                WriteAheadLogFile::try_new(table_log_path)?,
            );

            Ok(())
        } else {
            Err(ModelarDbStorageError::InvalidState(format!(
                "Table log for table '{table_name}' already exists",
            )))
        }
    }
}

/// Wrapper around a [`File`] that enforces that [`sync_all()`](File::sync_all) is called
/// immediately after writing to ensure that all data is on disk before returning. Note that
/// an exclusive lock is held on the file while it is being written to.
struct WriteAheadLogFile {
    file: File,
}

impl WriteAheadLogFile {
    /// Create a new [`WriteAheadLogFile`] that appends to the file at `file_path`. If the file does
    /// not exist, it is created. If the file could not be created, return [`ModelarDbStorageError`].
    fn try_new(file_path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)?;

        Ok(Self { file })
    }
}
