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

use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use modelardb_types::types::TimeSeriesTableMetadata;

use crate::WRITE_AHEAD_LOG_FOLDER;
use crate::data_folder::DataFolder;
use crate::error::{ModelarDbStorageError, Result};

const OPERATIONS_LOG_FILE: &str = "operations.wal";

/// Write-ahead log that logs data on a per-table level and operations separately.
pub struct WriteAheadLog {
    /// Path to the folder that contains the write-ahead log.
    folder_path: PathBuf,
    /// Logs for each table. The key is the table name, and the value is the log file for that table.
    table_logs: HashMap<String, WriteAheadLogFile>,
    /// Log file for operations that are not associated with a specific table.
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
            folder_path: log_folder_path.clone(),
            table_logs: HashMap::new(),
            operation_log: WriteAheadLogFile::try_new(
                log_folder_path.join(OPERATIONS_LOG_FILE),
                &operations_log_schema(),
            )?,
        };

        // For each time series table, create a log file if it does not already exist.
        for metadata in local_data_folder.time_series_table_metadata().await? {
            write_ahead_log.create_table_log(&metadata)?;
        }

        Ok(write_ahead_log)
    }

    /// Create a new [`WriteAheadLogFile`] for the table with the given metadata. If a log already
    /// exists in the map or the log file could not be created, return [`ModelarDbStorageError`].
    /// Note that if the log file already exists, but it is not present in the map, the existing
    /// log file will be added to the map.
    pub fn create_table_log(
        &mut self,
        time_series_table_metadata: &TimeSeriesTableMetadata,
    ) -> Result<()> {
        let table_name = time_series_table_metadata.name.clone();

        if !self.table_logs.contains_key(&table_name) {
            let table_log_path = self.folder_path.join(format!("{}.wal", table_name));

            self.table_logs.insert(
                table_name,
                WriteAheadLogFile::try_new(table_log_path, &time_series_table_metadata.schema)?,
            );

            Ok(())
        } else {
            Err(ModelarDbStorageError::InvalidState(format!(
                "Table log for table '{table_name}' already exists",
            )))
        }
    }

    /// Remove the log file for the table with the given name. If the log file does not exist or
    /// could not be removed, return [`ModelarDbStorageError`].
    pub fn remove_table_log(&mut self, table_name: &str) -> Result<()> {
        let log_path;

        if let Some(log_file) = self.table_logs.remove(table_name) {
            log_path = log_file.path;
            // log_file is dropped here as it goes out of scope which automatically closes its
            // internal file handle.
        } else {
            return Err(ModelarDbStorageError::InvalidState(format!(
                "Table log for table '{table_name}' does not exist",
            )));
        }

        // Now that the file handle is closed, the file can be removed.
        std::fs::remove_file(log_path)?;

        Ok(())
    }
}

/// Wrapper around a [`File`] that enforces that [`sync_all()`](File::sync_all) is called
/// immediately after writing to ensure that all data is on disk before returning. Note that
/// an exclusive lock is held on the file while it is being written to.
struct WriteAheadLogFile {
    /// Path to the file that the log is written to.
    path: PathBuf,
    /// Writer to write data in IPC streaming format to the log file.
    writer: StreamWriter<File>,
}

impl WriteAheadLogFile {
    /// Create a new [`WriteAheadLogFile`] that appends data with `schema` to the file at
    /// `file_path`. If the file does not exist, it is created. If the file could not be created,
    /// return [`ModelarDbStorageError`].
    fn try_new(file_path: PathBuf, schema: &Schema) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path.clone())?;

        let writer = StreamWriter::try_new(file, schema)?;

        Ok(Self {
            path: file_path,
            writer,
        })
    }

    /// Append the given data to the log file and sync the file to ensure that all data is on disk.
    /// If the data could not be appended or the file could not be synced, return
    /// [`ModelarDbStorageError`].
    fn append_and_sync(&mut self, data: &RecordBatch) -> Result<()> {
        self.writer.write(data)?;

        // Flush the writer's internal buffers to the file.
        self.writer.flush()?;

        // Get a reference to the underlying file handle and sync to disk.
        self.writer.get_ref().sync_all()?;

        Ok(())
    }

    /// Read all data from the log file. This can be called even if the [`StreamWriter`] has not
    /// been finished, meaning the log file is missing the end-of-stream bytes. If the file
    /// could not be read, return [`ModelarDbStorageError`].
    fn read_all(file_path: &PathBuf) -> Result<Vec<RecordBatch>> {
        // TODO: Maybe reuse the file handle instead of opening a new one.
        let file = File::open(file_path)?;
        let reader = StreamReader::try_new(file, None)?;

        let mut batches = Vec::new();
        for maybe_batch in reader {
            match maybe_batch {
                Ok(batch) => batches.push(batch),
                // TODO: Maybe handle the specific error for end of file.
                Err(_) => break,
            }
        }

        Ok(batches)
    }
}

/// Return the schema for the operations log that is stored in [`OPERATIONS_LOG_FILE`].
fn operations_log_schema() -> Schema {
    Schema::new(vec![Field::new("operation", DataType::Utf8, false)])
}
