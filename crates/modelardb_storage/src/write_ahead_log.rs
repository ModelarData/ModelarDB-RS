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
use std::fs::File;
use std::path::PathBuf;

use crate::WRITE_AHEAD_LOG_FOLDER;
use crate::data_folder::DataFolder;
use crate::error::Result;


/// Write-ahead log that logs data on a per-table level and operations separately.
pub struct WriteAheadLog {
    log_folder_path: PathBuf,
    table_logs: HashMap<String, WriteAheadLogFile>,
    operation_log: WriteAheadLogFile,
}

impl WriteAheadLog {
    /// Create a new [`WriteAheadLog`] that stores the log in the root of `local_data_folder` in
    /// the [`WRITE_AHEAD_LOG_FOLDER`] folder. If the folder does not exist, it is created. If the
    /// log could not be created, return [`ModelarDbStorageError`](crate::error::ModelarDbStorageError).
    pub fn try_new(local_data_folder: &DataFolder) -> Result<Self> {
        // Create the folder for the write-ahead log if it does not exist.
        let location = local_data_folder.location();
        let log_folder_path = PathBuf::from(format!("{location}/{WRITE_AHEAD_LOG_FOLDER}"));

        std::fs::create_dir(log_folder_path.clone())?;

        Ok(Self {
            log_folder_path: log_folder_path.clone(),
            table_logs: HashMap::new(),
            operation_log: WriteAheadLogFile::try_new(log_folder_path.join("operations.wal"))?,
        })
    }
}

/// Wrapper around a [`File`] that enforces that [`sync_all()`](File::sync_all) is called
/// immediately after writing to ensure that all data is on disk before returning. Note that
/// an exclusive lock is held on the file while it is being written to.
struct WriteAheadLogFile {
    file: File,
}

impl WriteAheadLogFile {
    /// Create a new [`WriteAheadLogFile`] that writes to the file at `file_path`. If the file could
    /// not be created, return [`ModelarDbStorageError`](crate::error::ModelarDbStorageError).
    fn try_new(file_path: PathBuf) -> Result<Self> {
        let file = File::create(file_path)?;
        Ok(Self { file })
    }
}
