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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Mutex;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError::IpcError;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use modelardb_types::types::TimeSeriesTableMetadata;

use crate::WRITE_AHEAD_LOG_FOLDER;
use crate::data_folder::DataFolder;
use crate::error::{ModelarDbStorageError, Result};

const OPERATIONS_LOG_FOLDER: &str = "operations";

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

        // Since the std:fs API is used, the location must be a local path.
        if location.contains("://") {
            return Err(ModelarDbStorageError::InvalidState(format!(
                "Write-ahead log location '{location}' is not a local path."
            )));
        }

        let log_folder_path = PathBuf::from(format!("{location}/{WRITE_AHEAD_LOG_FOLDER}"));

        std::fs::create_dir_all(log_folder_path.clone())?;

        let mut write_ahead_log = Self {
            folder_path: log_folder_path.clone(),
            table_logs: HashMap::new(),
            operation_log: WriteAheadLogFile::try_new(
                log_folder_path.join(OPERATIONS_LOG_FOLDER),
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
            let table_log_path = self.folder_path.join(&table_name);

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
                "Table log for table '{table_name}' does not exist.",
            )));
        }

        // Now that the file handle is closed, the file can be removed.
        std::fs::remove_file(log_path)?;

        Ok(())
    }

    /// Append data to the log for the given table and sync the file to ensure that all data is on
    /// disk. Only requires read access to the log since the internal Mutex handles write
    /// synchronization. Return the batch id given to the appended data. If a table log does not
    /// exist or the data could not be appended, return [`ModelarDbStorageError`].
    pub fn append_to_table_log(&self, table_name: &str, data: &RecordBatch) -> Result<u64> {
        let log_file = self.table_logs.get(table_name).ok_or_else(|| {
            ModelarDbStorageError::InvalidState(format!(
                "Table log for table '{table_name}' does not exist."
            ))
        })?;

        log_file.append_and_sync(data)
    }

    /// Mark the given batch ids as saved to disk in the corresponding table log. If a large enough
    /// contiguous prefix of batches is marked as persisted, the log file is trimmed to remove
    /// the persisted data. If a table log does not exist or the log file could not be trimmed,
    /// return [`ModelarDbStorageError`].
    pub fn mark_batches_as_persisted_in_table_log(
        &self,
        table_name: &str,
        batch_ids: HashSet<u64>,
    ) -> Result<()> {
        let log_file = self.table_logs.get(table_name).ok_or_else(|| {
            ModelarDbStorageError::InvalidState(format!(
                "Table log for table '{table_name}' does not exist."
            ))
        })?;

        log_file.mark_batches_as_persisted(batch_ids)
    }
}

/// Wrapper around a [`File`] that enforces that [`sync_data()`](File::sync_data) is called
/// immediately after writing to ensure that all data is on disk before returning. Note that
/// an exclusive lock is held on the file while it is being written to.
struct WriteAheadLogFile {
    /// Path to the file that the log is written to.
    path: PathBuf,
    /// Writer to write data in IPC streaming format to the log file.
    writer: Mutex<StreamWriter<File>>,
    /// The offset encoded in the WAL file name. This represents the number of batches that have
    /// been removed from the start of the file across all previous truncations.
    batch_offset: u64,
    /// The batch id to give to the next batch of data appended to the log file. This is incremented
    /// after each append, so the batch id given to data is monotonically increasing.
    next_batch_id: Mutex<u64>,
    /// Batch ids that have been confirmed as saved to disk. Used to determine whether a
    /// contiguous prefix of batches can be trimmed from the start of the log file.
    persisted_batch_ids: Mutex<BTreeSet<u64>>,
}

impl WriteAheadLogFile {
    /// Create a new [`WriteAheadLogFile`] that appends data with `schema` to a file in
    /// `folder_path`. If the file does not exist, it is created. If the file could not be created,
    /// return [`ModelarDbStorageError`].
    fn try_new(folder_path: PathBuf, schema: &Schema) -> Result<Self> {
        std::fs::create_dir_all(folder_path.clone())?;

        let (file_path, batch_offset) =
            find_existing_wal_file(&folder_path)?.unwrap_or_else(|| (folder_path.join("0.wal"), 0));

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(file_path.clone())?;

        let file_len = file.metadata()?.len();

        let writer = StreamWriter::try_new(file, schema)?;

        // If the file already had data, the StreamWriter wrote a duplicate schema header.
        // Truncate back to the original length to remove it, then seek to the end so
        // subsequent writes append correctly.
        if file_len > 0 {
            writer.get_ref().set_len(file_len)?;
            writer.get_ref().seek(SeekFrom::End(0))?;
        }

        // Count existing batches to reconstruct the next batch ID.
        let batch_count = if file_len > 0 {
            let file = File::open(&file_path)?;
            let reader = StreamReader::try_new(file, None)?;
            reader.take_while(|r| r.is_ok()).count() as u64
        } else {
            0
        };

        Ok(Self {
            path: file_path,
            writer: Mutex::new(writer),
            batch_offset,
            next_batch_id: Mutex::new(batch_offset + batch_count),
            persisted_batch_ids: Mutex::new(BTreeSet::new()),
        })
    }

    /// Append the given data to the log file and sync the file to ensure that all data is on disk.
    /// Return the batch id given to the appended data. If the data could not be appended or the
    /// file could not be synced, return [`ModelarDbStorageError`].
    fn append_and_sync(&self, data: &RecordBatch) -> Result<u64> {
        // Acquire the mutex to ensure only one thread can write at a time.
        let mut writer = self.writer.lock().expect("Mutex should not be poisoned.");
        let mut next_batch_id = self
            .next_batch_id
            .lock()
            .expect("Mutex should not be poisoned.");

        writer.write(data)?;

        // Flush the writer's internal buffers to the file.
        writer.flush()?;

        // Get a reference to the underlying file handle and sync to disk. Note that file metadata
        // such as modification timestamps and permissions are not updated since we only sync data.
        writer.get_ref().sync_data()?;

        // Increment the batch id for the next batch of data.
        let current_batch_id = *next_batch_id;
        *next_batch_id += 1;

        Ok(current_batch_id)
    }

    /// Mark the given batch ids as saved to disk. If a large enough contiguous prefix of batches
    /// is marked as persisted, the log file is trimmed to remove the persisted data. If the
    /// file could not be trimmed, return [`ModelarDbStorageError`].
    fn mark_batches_as_persisted(&self, batch_ids: HashSet<u64>) -> Result<()> {
        let mut persisted = self
            .persisted_batch_ids
            .lock()
            .expect("Mutex should not be poisoned.");

        persisted.extend(batch_ids);

        // Walk forward from batch_offset to find the contiguous prefix watermark.
        let mut watermark = self.batch_offset;
        while persisted.contains(&watermark) {
            watermark += 1;
        }

        // If watermark advanced, we have a contiguous prefix ending at watermark - 1.
        let max_prefix_batch_id = if watermark > self.batch_offset {
            Some(watermark - 1)
        } else {
            None
        };

        Ok(())
    }

    /// Read all data from the log file. This can be called even if the [`StreamWriter`] has not
    /// been finished, meaning the log file is missing the end-of-stream bytes. If the file
    /// could not be read, return [`ModelarDbStorageError`].
    fn read_all(&self) -> Result<Vec<RecordBatch>> {
        // Acquire the mutex to ensure data is not being written while reading. Note that reading
        // should only occur during recovery, which should make concurrent writes improbable.
        // However, since performance is not critical during recovery, the mutex is held anyway.
        let _writer = self.writer.lock().unwrap();

        let file = File::open(&self.path)?;
        let reader = StreamReader::try_new(file, None)?;

        let mut batches = Vec::new();
        for maybe_batch in reader {
            match maybe_batch {
                Ok(batch) => batches.push(batch),
                Err(IpcError(msg)) => {
                    // Check if it is an UnexpectedEof error, which is expected when reading
                    // an incomplete stream without the end-of-stream marker.
                    if msg.contains("UnexpectedEof") || msg.contains("unexpected end of file") {
                        break;
                    }
                    return Err(IpcError(msg).into());
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(batches)
    }
}

/// Find an existing WAL file in `folder_path` and return its path and the offset parsed from its
/// name if it exists, otherwise return `Ok(None)`.
fn find_existing_wal_file(folder_path: &PathBuf) -> Result<Option<(PathBuf, u64)>> {
    Ok(std::fs::read_dir(folder_path)?
        .filter_map(|maybe_file| maybe_file.ok())
        .filter_map(|file| {
            let path = file.path();
            let offset = path.file_stem()?.to_str()?.parse::<u64>().ok()?;
            Some((path, offset))
        })
        .next())
}

/// Return the schema for the operations log that is stored in [`OPERATIONS_LOG_FOLDER`].
fn operations_log_schema() -> Schema {
    Schema::new(vec![Field::new("operation", DataType::Utf8, false)])
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_test::table;
    use modelardb_test::table::TIME_SERIES_TABLE_NAME;

    #[test]
    fn test_try_new_creates_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let folder_path = temp_dir.path().join(TIME_SERIES_TABLE_NAME);

        let metadata = table::time_series_table_metadata();
        let wal_file = WriteAheadLogFile::try_new(folder_path.clone(), &metadata.schema).unwrap();

        assert!(wal_file.path.exists());
        assert_eq!(*wal_file.next_batch_id.lock().unwrap(), 0);
    }

    #[test]
    fn test_read_all_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let folder_path = temp_dir.path().join(TIME_SERIES_TABLE_NAME);

        let metadata = table::time_series_table_metadata();
        let wal_file = WriteAheadLogFile::try_new(folder_path, &metadata.schema).unwrap();

        let batches = wal_file.read_all().unwrap();
        assert!(batches.is_empty());
        assert_eq!(*wal_file.next_batch_id.lock().unwrap(), 0);
    }

    #[test]
    fn test_append_and_read_single_batch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let folder_path = temp_dir.path().join(TIME_SERIES_TABLE_NAME);

        let metadata = table::time_series_table_metadata();
        let wal_file = WriteAheadLogFile::try_new(folder_path, &metadata.schema).unwrap();
        let batch = table::uncompressed_time_series_table_record_batch(5);

        wal_file.append_and_sync(&batch).unwrap();

        let batches = wal_file.read_all().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], batch);
        assert_eq!(*wal_file.next_batch_id.lock().unwrap(), 1);
    }

    #[test]
    fn test_append_and_read_multiple_batches() {
        let temp_dir = tempfile::tempdir().unwrap();
        let folder_path = temp_dir.path().join(TIME_SERIES_TABLE_NAME);

        let metadata = table::time_series_table_metadata();
        let wal_file = WriteAheadLogFile::try_new(folder_path, &metadata.schema).unwrap();

        let batch_1 = table::uncompressed_time_series_table_record_batch(10);
        let batch_2 = table::uncompressed_time_series_table_record_batch(20);
        let batch_3 = table::uncompressed_time_series_table_record_batch(30);

        wal_file.append_and_sync(&batch_1).unwrap();
        wal_file.append_and_sync(&batch_2).unwrap();
        wal_file.append_and_sync(&batch_3).unwrap();

        let batches = wal_file.read_all().unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0], batch_1);
        assert_eq!(batches[1], batch_2);
        assert_eq!(batches[2], batch_3);
        assert_eq!(*wal_file.next_batch_id.lock().unwrap(), 3);
    }

    #[test]
    fn test_reopen_existing_file_and_append() {
        let temp_dir = tempfile::tempdir().unwrap();
        let folder_path = temp_dir.path().join(TIME_SERIES_TABLE_NAME);

        let metadata = table::time_series_table_metadata();
        let batch_1 = table::uncompressed_time_series_table_record_batch(10);
        {
            let wal_file =
                WriteAheadLogFile::try_new(folder_path.clone(), &metadata.schema).unwrap();
            wal_file.append_and_sync(&batch_1).unwrap();
        }

        let batch_2 = table::uncompressed_time_series_table_record_batch(20);
        let wal_file = WriteAheadLogFile::try_new(folder_path, &metadata.schema).unwrap();
        wal_file.append_and_sync(&batch_2).unwrap();

        let batches = wal_file.read_all().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], batch_1);
        assert_eq!(batches[1], batch_2);
        assert_eq!(*wal_file.next_batch_id.lock().unwrap(), 2);
    }

    #[test]
    fn test_reopen_existing_file_and_read_without_append() {
        let temp_dir = tempfile::tempdir().unwrap();
        let folder_path = temp_dir.path().join(TIME_SERIES_TABLE_NAME);

        let metadata = table::time_series_table_metadata();
        let batch = table::uncompressed_time_series_table_record_batch(10);
        {
            let wal_file =
                WriteAheadLogFile::try_new(folder_path.clone(), &metadata.schema).unwrap();
            wal_file.append_and_sync(&batch).unwrap();
        }

        let wal_file = WriteAheadLogFile::try_new(folder_path, &metadata.schema).unwrap();
        let batches = wal_file.read_all().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], batch);
        assert_eq!(*wal_file.next_batch_id.lock().unwrap(), 1);
    }

    #[test]
    fn test_file_size_not_changed_on_reopen() {
        let temp_dir = tempfile::tempdir().unwrap();
        let folder_path = temp_dir.path().join(TIME_SERIES_TABLE_NAME);

        let metadata = table::time_series_table_metadata();
        let batch = table::uncompressed_time_series_table_record_batch(10);

        let wal_file_path = {
            let wal_file =
                WriteAheadLogFile::try_new(folder_path.clone(), &metadata.schema).unwrap();
            wal_file.append_and_sync(&batch).unwrap();

            wal_file.path.clone()
        };

        let size_before = std::fs::metadata(&wal_file_path).unwrap().len();

        let wal_file_path = {
            let wal_file =
                WriteAheadLogFile::try_new(folder_path.clone(), &metadata.schema).unwrap();

            wal_file.path.clone()
        };

        let size_after = std::fs::metadata(&wal_file_path).unwrap().len();
        assert_eq!(size_before, size_after);
    }
}
