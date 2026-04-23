/* Copyright 2026 The ModelarDB Contributors
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
//! efficiently persist data on disk to avoid data loss and enable crash recovery. Each table has
//! its own segmented log consisting of an active segment that is appended to and zero or more
//! closed segments that are read-only. The active segment is closed once the approximate size
//! of its batches reaches a configured threshold, and closed segments are deleted once all of
//! their batches have been persisted to the Delta Lake.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use arrow::datatypes::Schema;
use arrow::error::ArrowError::IpcError;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use deltalake::DeltaTable;
use modelardb_types::types::TimeSeriesTableMetadata;
use tracing::{debug, info, warn};

use crate::WRITE_AHEAD_LOG_FOLDER;
use crate::data_folder::DataFolder;
use crate::error::{ModelarDbStorageError, Result};

/// Write-ahead log that logs data on a per-table level.
pub struct WriteAheadLog {
    /// Path to the folder that contains the write-ahead log.
    folder_path: PathBuf,
    /// Logs for each table. The key is the table name, and the value is the table log for that table.
    table_logs: HashMap<String, SegmentedLog>,
    /// The approximate maximum size, in bytes, of a single WAL segment file before it is closed and
    /// a new one is started.
    segment_size_threshold_in_bytes: u64,
}

impl WriteAheadLog {
    /// Create a new [`WriteAheadLog`] that stores the WAL in the root of `local_data_folder` in
    /// the [`WRITE_AHEAD_LOG_FOLDER`] folder. `local_data_folder` must be in a local path since the
    /// WAL uses the [`std::fs`] API to avoid the overhead of the `ObjectStore` API and to allow the
    /// use of [`sync_data()`](File::sync_data). If the folder does not exist, it is created. If
    /// `local_data_folder` is not in a local path or the WAL could not be created, return
    /// [`ModelarDbStorageError`].
    pub async fn try_new(
        local_data_folder: &DataFolder,
        segment_size_threshold_in_bytes: u64,
    ) -> Result<Self> {
        // Create the folder for the write-ahead log if it does not exist.
        let location = local_data_folder.location();

        if location.contains("://") {
            return Err(ModelarDbStorageError::InvalidState(format!(
                "Write-ahead log location '{location}' is not a local path."
            )));
        }

        let log_folder_path = PathBuf::from(location).join(WRITE_AHEAD_LOG_FOLDER);

        std::fs::create_dir_all(&log_folder_path)?;

        let mut write_ahead_log = Self {
            folder_path: log_folder_path.clone(),
            table_logs: HashMap::new(),
            segment_size_threshold_in_bytes,
        };

        // For each time series table, create a table log if it does not already exist.
        for metadata in local_data_folder.time_series_table_metadata().await? {
            let delta_table = local_data_folder.delta_table(&metadata.name).await?;
            write_ahead_log.create_table_log(&metadata).await?;

            // Load the persisted batch ids from the commit history of the delta table. This is
            // only necessary when initializing the WAL for an existing table.
            let table_log = write_ahead_log.table_log(&metadata.name)?;
            table_log
                .load_persisted_batches_from_delta_table(delta_table)
                .await?;
        }

        info!(
            path = %log_folder_path.display(),
            table_count = write_ahead_log.table_logs.len(),
            "WAL initialized."
        );

        Ok(write_ahead_log)
    }

    /// Create a new segmented log for the table with the given metadata. If a table log already
    /// exists in the map or the table log could not be created, return [`ModelarDbStorageError`].
    /// Note that if the table log already exists, but it is not present in the map, the existing
    /// table log will be added to the map.
    pub async fn create_table_log(
        &mut self,
        time_series_table_metadata: &TimeSeriesTableMetadata,
    ) -> Result<()> {
        let table_name = time_series_table_metadata.name.clone();

        if !self.table_logs.contains_key(&table_name) {
            let table_log_path = self.folder_path.join(&table_name);
            let table_log = SegmentedLog::try_new(
                table_log_path,
                &time_series_table_metadata.schema,
                self.segment_size_threshold_in_bytes,
            )?;

            debug!(
                table = %table_name,
                folder_path = %table_log.folder_path.display(),
                "WAL table log created."
            );

            self.table_logs.insert(table_name, table_log);

            Ok(())
        } else {
            Err(ModelarDbStorageError::InvalidState(format!(
                "Table log for table '{table_name}' already exists.",
            )))
        }
    }

    /// Remove the table log for the table with the given name. If the table log does not exist or
    /// could not be removed, return [`ModelarDbStorageError`].
    pub fn remove_table_log(&mut self, table_name: &str) -> Result<()> {
        let log_path;

        if let Some(table_log) = self.table_logs.remove(table_name) {
            log_path = table_log.folder_path;
            // table_log is dropped here as it goes out of scope which automatically closes its
            // internal file handle.
        } else {
            return Err(ModelarDbStorageError::InvalidState(format!(
                "Table log for table '{table_name}' does not exist.",
            )));
        }

        // Now that the file handle is closed, the files can be removed.
        std::fs::remove_dir_all(&log_path)?;

        debug!(
            table = %table_name,
            folder_path = %log_path.display(),
            "WAL table log removed."
        );

        Ok(())
    }

    /// Append data to the table log for the given table and sync the file to ensure that all data
    /// is on disk. Only requires read access to the write-ahead log since the internal Mutex
    /// handles write synchronization. Return the batch id given to the appended data. If a table
    /// log does not exist or the data could not be appended, return [`ModelarDbStorageError`].
    pub fn append_to_table_log(&self, table_name: &str, data: &RecordBatch) -> Result<u64> {
        let table_log = self.table_log(table_name)?;
        table_log.append_and_sync(data)
    }

    /// Mark the given batch ids as saved to disk in the corresponding table log. Fully persisted
    /// segment files are deleted. If a table log does not exist or a segment file could not be
    /// deleted, return [`ModelarDbStorageError`].
    pub fn mark_batches_as_persisted_in_table_log(
        &self,
        table_name: &str,
        batch_ids: HashSet<u64>,
    ) -> Result<()> {
        let table_log = self.table_log(table_name)?;
        table_log.mark_batches_as_persisted(batch_ids)
    }

    /// Return pairs of (batch_id, batch) for all batches in the corresponding table log that have
    /// not yet been persisted. If the table log does not exist or the batches could not be read
    /// from the table log, return [`ModelarDbStorageError`].
    pub fn unpersisted_batches_in_table_log(
        &self,
        table_name: &str,
    ) -> Result<Vec<(u64, RecordBatch)>> {
        let table_log = self.table_log(table_name)?;
        table_log.unpersisted_batches()
    }

    /// Truncate the table log for the given table name, removing all closed segment files from
    /// disk, and starting a new active segment file. If the table log does not exist or the data
    /// could not be truncated, return [`ModelarDbStorageError`].
    pub fn truncate_table_log(&self, table_name: &str) -> Result<()> {
        let table_log = self.table_log(table_name)?;
        table_log.truncate()
    }

    /// Get the table log for the table with the given name. If the table log does not exist, return
    /// [`ModelarDbStorageError`].
    fn table_log(&self, table_name: &str) -> Result<&SegmentedLog> {
        self.table_logs.get(table_name).ok_or_else(|| {
            ModelarDbStorageError::InvalidState(format!(
                "Table log for table '{table_name}' does not exist."
            ))
        })
    }

    /// Set the segment size threshold for the write-ahead log and all existing table logs.
    /// New table logs created after this call will also use the new threshold.
    pub fn set_segment_size_threshold_in_bytes(&mut self, segment_size_threshold_in_bytes: u64) {
        self.segment_size_threshold_in_bytes = segment_size_threshold_in_bytes;

        for table_log in self.table_logs.values_mut() {
            table_log.segment_size_threshold_in_bytes = segment_size_threshold_in_bytes;
        }
    }
}

/// A closed WAL segment file. The file contains all batches with ids in `[start_id, end_id]`
/// and will not be written to again.
struct ClosedSegment {
    /// Path to the segment file on disk.
    path: PathBuf,
    /// Batch id of the first batch in this segment.
    start_id: u64,
    /// Batch id of the last batch in this segment (inclusive).
    end_id: u64,
}

impl ClosedSegment {
    /// Return `true` if every batch id in this segment is present in `persisted`.
    fn is_fully_persisted(&self, persisted: &BTreeSet<u64>) -> bool {
        // Iterate in reverse since newer (higher) ids are least likely to be persisted, allowing
        // all() to short-circuit earlier for partially persisted segments.
        (self.start_id..=self.end_id)
            .rev()
            .all(|id| persisted.contains(&id))
    }
}

/// The currently active WAL segment being written to. All fields are mutated together
/// when closing the active segment and are protected by the mutex in [`SegmentedLog`].
struct ActiveSegment {
    /// Path to the active segment file.
    path: PathBuf,
    /// Batch id of the first batch written to this segment.
    start_id: u64,
    /// Writer to write data in Apache Arrow IPC streaming format to this segment file.
    writer: StreamWriter<File>,
    /// The batch id to give to the next batch of data. Monotonically increasing across segments.
    next_batch_id: u64,
    /// Approximate size in bytes of all batches written to this segment. This is an approximation
    /// since [`get_array_memory_size()`](RecordBatch::get_array_memory_size()) is used to avoid
    /// the overhead of getting the actual file size.
    approximate_size: u64,
}

impl ActiveSegment {
    /// Create a new [`ActiveSegment`] in `folder_path` with the given `start_id` and `schema`.
    /// If the file could not be created, return [`ModelarDbStorageError`].
    fn try_new(folder_path: PathBuf, schema: &Schema, start_id: u64) -> Result<Self> {
        let path = folder_path.join(format!("{start_id}-.arrows"));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        let writer = StreamWriter::try_new(file, schema)?;

        debug!(
            path = %path.display(),
            "WAL file created."
        );

        Ok(Self {
            path,
            start_id,
            writer,
            next_batch_id: start_id,
            approximate_size: 0,
        })
    }
}

/// Segmented log that appends data in Apache Arrow IPC streaming format to segment files in a
/// folder. At any point in time there is exactly one active segment being written to plus zero or
/// more closed segments that are read-only. The active segment is closed once the approximate
/// size of its batches reaches `segment_size_threshold_in_bytes`. Appending enforces that
/// [`sync_data()`](File::sync_data) is called immediately after writing to ensure that all data is
/// on disk before returning. Note that an exclusive lock is held on the file while it is being
/// written to, to ensure that no other thread can write to it.
struct SegmentedLog {
    /// Folder that contains all segment files for this log.
    folder_path: PathBuf,
    /// Arrow schema shared by every segment in this log.
    schema: Schema,
    /// The active segment currently being written to.
    active_segment: Mutex<ActiveSegment>,
    /// Closed, read-only segment files ordered by `start_id`.
    closed_segments: Mutex<Vec<ClosedSegment>>,
    /// Batch ids that have been confirmed as saved to disk. Used to determine when closed segments
    /// can be deleted.
    persisted_batch_ids: Mutex<BTreeSet<u64>>,
    /// The approximate maximum size, in bytes, of a single WAL segment file before it is closed and
    /// a new one is started.
    segment_size_threshold_in_bytes: u64,
}

impl SegmentedLog {
    /// Create a new [`SegmentedLog`] that appends data with `schema` to segment files in
    /// `folder_path`. Existing closed segment files are appended to the closed-segment list.
    /// A fresh active segment is always created on start-up. If the folder or file could not be
    /// created, return [`ModelarDbStorageError`].
    fn try_new(
        folder_path: PathBuf,
        schema: &Schema,
        segment_size_threshold_in_bytes: u64,
    ) -> Result<Self> {
        std::fs::create_dir_all(&folder_path)?;

        let leftover_next_id = close_leftover_active_segment(&folder_path)?;

        // Collect all closed segment files already on disk and sort them by start_id.
        let mut closed_segments = find_closed_segments(&folder_path)?;
        closed_segments.sort_by_key(|s| s.start_id);

        // The next batch id is one past the end of the last closed segment, the next id
        // recovered from a leftover active segment, or 0 if there is neither.
        let next_id = closed_segments
            .last()
            .map(|s| s.end_id + 1)
            .or(leftover_next_id)
            .unwrap_or(0);

        if !closed_segments.is_empty() {
            debug!(
                folder_path = %folder_path.display(),
                closed_segment_count = closed_segments.len(),
                next_batch_id = next_id,
                "Found closed WAL segments."
            );
        }

        // Always create a fresh active segment on startup to avoid writing into the middle of
        // an existing Apache Arrow IPC stream.
        let active_file = ActiveSegment::try_new(folder_path.clone(), schema, next_id)?;

        Ok(Self {
            folder_path,
            schema: schema.clone(),
            active_segment: Mutex::new(active_file),
            closed_segments: Mutex::new(closed_segments),
            persisted_batch_ids: Mutex::new(BTreeSet::new()),
            segment_size_threshold_in_bytes,
        })
    }

    /// Append the given data to the active segment and sync the file to ensure that all data is on
    /// disk. Return the batch id given to the appended data. Close the active segment and start a
    /// new one if `segment_size_threshold_in_bytes` is reached. If the data could not be appended
    /// or the file could not be synced, return [`ModelarDbStorageError`].
    fn append_and_sync(&self, data: &RecordBatch) -> Result<u64> {
        // Acquire the mutex to ensure only one thread can write at a time.
        let mut active = self
            .active_segment
            .lock()
            .expect("Mutex should not be poisoned.");

        active.writer.write(data)?;

        // Flush the writer's internal buffers to the file.
        active.writer.flush()?;

        // Get a reference to the underlying file handle and sync to disk. Note that file metadata
        // such as modification timestamps and permissions are not updated since we only sync data.
        // Only syncing data reduces disk operations and improves performance.
        active.writer.get_ref().sync_data()?;

        // Increment the batch id for the next batch of data.
        let current_batch_id = active.next_batch_id;
        active.next_batch_id += 1;

        active.approximate_size += data.get_array_memory_size() as u64;

        debug!(
            path = %active.path.display(),
            batch_id = current_batch_id,
            row_count = data.num_rows(),
            segment_approximate_size = active.approximate_size,
            "Appended batch to WAL file."
        );

        // Close the active segment and start a new one if the threshold has been reached.
        if active.approximate_size >= self.segment_size_threshold_in_bytes {
            self.close_active_segment(&mut active)?;
        }

        Ok(current_batch_id)
    }

    /// Close the current active segment by renaming it to its final `{start_id}-{end_id}.arrows`
    /// name and open a fresh active segment. The end id is added to the file name to avoid having
    /// to read the entire file to determine the end id later. The caller must hold the
    /// `active_segment` lock.
    fn close_active_segment(&self, active: &mut ActiveSegment) -> Result<()> {
        let mut closed_segments = self
            .closed_segments
            .lock()
            .expect("Mutex should not be poisoned.");

        let end_id = active.next_batch_id - 1;

        debug!(
            path = %active.path.display(),
            start_id = active.start_id,
            end_id,
            "Closing active WAL segment."
        );

        // Finish the current writer so the Apache Arrow IPC end-of-stream marker is written.
        active.writer.finish()?;

        // Rename the active file to its permanent name that includes the end id.
        let closed_path = self
            .folder_path
            .join(format!("{}-{end_id}.arrows", active.start_id));
        std::fs::rename(&active.path, &closed_path)?;

        closed_segments.push(ClosedSegment {
            path: closed_path,
            start_id: active.start_id,
            end_id,
        });

        // Open a new active segment.
        let next_id = end_id + 1;
        *active = ActiveSegment::try_new(self.folder_path.clone(), &self.schema, next_id)?;

        Ok(())
    }

    /// Mark the given batch ids as saved to disk. Any closed segment whose entire batch-id range
    /// is now persisted is deleted from disk and removed from the in-memory list. If a segment file
    /// could not be deleted, return [`ModelarDbStorageError`].
    fn mark_batches_as_persisted(&self, batch_ids: HashSet<u64>) -> Result<()> {
        debug!(
            folder_path = %self.folder_path.display(),
            batch_ids = ?batch_ids,
            "Marking batches as persisted."
        );

        let mut persisted = self
            .persisted_batch_ids
            .lock()
            .expect("Mutex should not be poisoned.");

        persisted.extend(batch_ids);

        let mut closed_segments = self
            .closed_segments
            .lock()
            .expect("Mutex should not be poisoned.");

        // Identify and delete fully persisted segments.
        let (to_delete, to_retain): (Vec<_>, Vec<_>) = closed_segments
            .drain(..)
            .partition(|segment| segment.is_fully_persisted(&persisted));

        *closed_segments = to_retain;

        for segment in to_delete {
            debug!(
                path = %segment.path.display(),
                "Deleting fully persisted WAL segment."
            );

            std::fs::remove_file(&segment.path)?;

            // Remove the persisted ids for this segment as they are no longer needed.
            for id in segment.start_id..=segment.end_id {
                persisted.remove(&id);
            }
        }

        Ok(())
    }

    /// Update the in-memory set of persisted batch ids from the commit history of `delta_table`
    /// and delete any fully persisted closed segment files. If the commit history could not be
    /// read or a segment file could not be deleted, return [`ModelarDbStorageError`].
    async fn load_persisted_batches_from_delta_table(&self, delta_table: DeltaTable) -> Result<()> {
        let mut persisted_batch_ids = HashSet::new();

        let history = delta_table.history(None).await?;
        for commit in history.into_iter() {
            if let Some(batch_ids) = commit.info.get("batchIds") {
                let batch_ids: Vec<u64> = serde_json::from_value(batch_ids.clone()).expect(
                    "The batchIds field in the commit metadata should be a JSON array of u64 values.",
                );

                persisted_batch_ids.extend(batch_ids);
            }
        }

        debug!(
            folder_path = %self.folder_path.display(),
            batch_ids = ?persisted_batch_ids,
            "Loaded persisted batch ids from Delta table commit history."
        );

        self.mark_batches_as_persisted(persisted_batch_ids)
    }

    /// Return pairs of (batch_id, batch) for all batches in the log that have not yet been
    /// persisted according to the current in-memory `persisted_batch_ids` set. If the batches
    /// could not be read from the segment files, return [`ModelarDbStorageError`].
    fn unpersisted_batches(&self) -> Result<Vec<(u64, RecordBatch)>> {
        let persisted = self
            .persisted_batch_ids
            .lock()
            .expect("Mutex should not be poisoned.");

        Ok(self
            .all_batches()?
            .into_iter()
            .filter(|(batch_id, _)| !persisted.contains(batch_id))
            .collect())
    }

    /// Read all data from all segment files (closed and active) in order and return them as pairs
    /// of (batch_id, batch). If any file could not be read, return [`ModelarDbStorageError`].
    fn all_batches(&self) -> Result<Vec<(u64, RecordBatch)>> {
        // Acquire the mutex to ensure data is not being written while reading.
        let active = self
            .active_segment
            .lock()
            .expect("Mutex should not be poisoned.");

        let closed_segments = self
            .closed_segments
            .lock()
            .expect("Mutex should not be poisoned.");

        let mut all_batches = Vec::new();
        for segment in closed_segments.iter() {
            let batches = read_batches_from_path(&segment.path)?;
            all_batches.extend((segment.start_id..=segment.end_id).zip(batches));
        }

        // Append the active segment's batches to the end of the list.
        let active_batches = read_batches_from_path(&active.path)?;
        if !active_batches.is_empty() {
            all_batches.extend((active.start_id..=active.next_batch_id - 1).zip(active_batches));
        }

        debug!(
            folder_path = %self.folder_path.display(),
            closed_segment_count = closed_segments.len(),
            batch_count = all_batches.len(),
            "Read all batches from WAL files."
        );

        Ok(all_batches)
    }

    /// Truncate all data in the log by deleting all closed segment files from disk and starting a
    /// new active segment file. If a closed segment file or the active segment file could not be
    /// deleted or a new active segment file could not be created, return [`ModelarDbStorageError`].
    fn truncate(&self) -> Result<()> {
        // Acquire the mutexes to ensure the log is not being written to while truncating.
        let mut persisted_batch_ids = self
            .persisted_batch_ids
            .lock()
            .expect("Mutex should not be poisoned.");

        let mut active = self
            .active_segment
            .lock()
            .expect("Mutex should not be poisoned.");

        let mut closed_segments = self
            .closed_segments
            .lock()
            .expect("Mutex should not be poisoned.");

        persisted_batch_ids.clear();

        // Delete all closed segments from disk.
        for segment in closed_segments.drain(..) {
            debug!(
                path = %segment.path.display(),
                "Deleting closed WAL segment due to table truncate."
            );

            std::fs::remove_file(&segment.path)?;
        }

        // Delete the active segment from disk.
        debug!(
            path = %active.path.display(),
            "Deleting active WAL segment due to table truncate."
        );

        std::fs::remove_file(&active.path)?;

        // Continue generating ids from the next unused batch id to avoid id collisions.
        let next_id = active.next_batch_id;

        // Open a new active segment
        *active = ActiveSegment::try_new(self.folder_path.clone(), &self.schema, next_id)?;

        Ok(())
    }
}

/// If a leftover active segment (`{start_id}-.arrows`) exists in `folder_path`, rename it to
/// its final `{start_id}-{end_id}.arrows` name so it is picked up as a closed segment. If the
/// file contains no batches, it is removed instead. In both cases, the next batch id implied by
/// the leftover is returned so the caller can ensure batch id continuity. If no leftover exists,
/// return `None`. If the file could not be renamed or removed, return [`ModelarDbStorageError`].
fn close_leftover_active_segment(folder_path: &Path) -> Result<Option<u64>> {
    let Some(active_path) = std::fs::read_dir(folder_path)?
        .filter_map(|maybe_entry| maybe_entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .is_some_and(|stem| stem.ends_with('-'))
        })
    else {
        return Ok(None);
    };

    let stem = active_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .expect("Active WAL segment stem should be '{start_id}-'.");

    let start_id: u64 = stem[..stem.len() - 1]
        .parse()
        .expect("Active WAL segment stem should start with a valid u64.");

    let batches = read_batches_from_path(&active_path)?;

    if batches.is_empty() {
        std::fs::remove_file(&active_path)?;
        debug!(path = %active_path.display(), "Removed empty leftover active WAL segment.");
        Ok(Some(start_id))
    } else {
        let end_id = start_id + batches.len() as u64 - 1;
        let closed_path = folder_path.join(format!("{start_id}-{end_id}.arrows"));

        warn!(
            path = %active_path.display(),
            closed_path = %closed_path.display(),
            batch_count = batches.len(),
            "Closed leftover active WAL segment from unclean shutdown."
        );

        std::fs::rename(&active_path, closed_path)?;
        Ok(Some(end_id + 1))
    }
}

/// Collect all closed segment files in `folder_path`. Closed segments have names of the form
/// `{start_id}-{end_id}.arrows` where both `start_id` and `end_id` are valid `u64` values.
fn find_closed_segments(folder_path: &Path) -> Result<Vec<ClosedSegment>> {
    let mut segments = Vec::new();

    for entry in std::fs::read_dir(folder_path)? {
        let path = entry?.path();
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .expect("WAL file should have a valid UTF-8 stem.");

        if let Some((start_id, end_id)) = stem
            .split_once('-')
            .and_then(|(s, e)| Some((s.parse::<u64>().ok()?, e.parse::<u64>().ok()?)))
        {
            segments.push(ClosedSegment {
                path,
                start_id,
                end_id,
            });
        } else {
            return Err(ModelarDbStorageError::InvalidState(format!(
                "Unexpected file found in WAL folder: {}.",
                path.display()
            )));
        }
    }

    Ok(segments)
}

/// Read all [`RecordBatches`](RecordBatch) from the file at `path`. Tolerates a missing
/// end-of-stream marker, which is normal for the active segment since [`StreamWriter::finish()`]
/// has not been called yet. If the file could not be read, return [`ModelarDbStorageError`].
fn read_batches_from_path(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader = StreamReader::try_new(file, None)?;

    let mut batches = Vec::new();
    for maybe_batch in reader {
        match maybe_batch {
            Ok(batch) => batches.push(batch),
            Err(IpcError(msg)) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_test::table;
    use modelardb_test::table::TIME_SERIES_TABLE_NAME;
    use tempfile::TempDir;

    const SEGMENT_SIZE_THRESHOLD_IN_BYTES: u64 = 64 * 1024; // 64 KiB.

    // Tests for WriteAheadLog.
    #[tokio::test]
    async fn test_try_new_without_tables_creates_empty_wal() {
        let (_temp_dir, wal) = new_empty_write_ahead_log().await;

        assert!(wal.table_logs.is_empty());
    }

    #[tokio::test]
    async fn test_try_new_with_existing_table_creates_table_log() {
        let (_temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let wal = WriteAheadLog::try_new(&data_folder, SEGMENT_SIZE_THRESHOLD_IN_BYTES)
            .await
            .unwrap();

        assert_eq!(wal.table_logs.len(), 1);
        assert!(wal.table_logs.contains_key(TIME_SERIES_TABLE_NAME));
    }

    #[tokio::test]
    async fn test_try_new_loads_persisted_batch_ids() {
        let (_temp_dir, data_folder) = create_data_folder_with_time_series_table().await;

        // Simulate that data was written to the table in a previous process to ensure that the
        // WAL can load already persisted batch ids from the commit history.
        write_compressed_segments_with_batch_ids(&data_folder, HashSet::from([0, 1, 2])).await;

        let wal = WriteAheadLog::try_new(&data_folder, SEGMENT_SIZE_THRESHOLD_IN_BYTES)
            .await
            .unwrap();

        let persisted = wal.table_logs[TIME_SERIES_TABLE_NAME]
            .persisted_batch_ids
            .lock()
            .unwrap();

        assert_eq!(*persisted, BTreeSet::from([0, 1, 2]));
    }

    #[tokio::test]
    async fn test_try_new_fails_for_non_local_data_folder() {
        let data_folder = DataFolder::open_memory().await.unwrap();
        let result = WriteAheadLog::try_new(&data_folder, SEGMENT_SIZE_THRESHOLD_IN_BYTES).await;

        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid State Error: Write-ahead log location 'memory:///modelardb' is not a local path."
        );
    }

    #[tokio::test]
    async fn test_create_table_log_adds_log_for_table() {
        let (_temp_dir, mut wal) = new_empty_write_ahead_log().await;
        let metadata = table::time_series_table_metadata();

        assert!(wal.table_logs.is_empty());

        wal.create_table_log(&metadata).await.unwrap();

        assert!(wal.table_logs.contains_key(TIME_SERIES_TABLE_NAME));
    }

    #[tokio::test]
    async fn test_create_table_log_fails_if_table_log_already_exists() {
        let (_temp_dir, mut wal) = new_empty_write_ahead_log().await;
        let metadata = table::time_series_table_metadata();

        wal.create_table_log(&metadata).await.unwrap();
        let result = wal.create_table_log(&metadata).await;

        assert_eq!(
            result.err().unwrap().to_string(),
            format!(
                "Invalid State Error: Table log for table '{TIME_SERIES_TABLE_NAME}' already exists.",
            )
        );
    }

    #[tokio::test]
    async fn test_remove_table_log_removes_log_and_directory() {
        let (_temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let mut wal = WriteAheadLog::try_new(&data_folder, SEGMENT_SIZE_THRESHOLD_IN_BYTES)
            .await
            .unwrap();

        let log_path = wal.table_logs[TIME_SERIES_TABLE_NAME].folder_path.clone();
        assert!(log_path.exists());
        assert!(wal.table_logs.contains_key(TIME_SERIES_TABLE_NAME));

        wal.remove_table_log(TIME_SERIES_TABLE_NAME).unwrap();

        assert!(!wal.table_logs.contains_key(TIME_SERIES_TABLE_NAME));
        assert!(!log_path.exists());
    }

    #[tokio::test]
    async fn test_remove_and_recreate_table_log_resets_batch_ids() {
        let (_temp_dir, mut wal) = new_empty_write_ahead_log().await;

        let metadata = table::time_series_table_metadata();
        let batch = table::uncompressed_time_series_table_record_batch(10);

        wal.create_table_log(&metadata).await.unwrap();

        wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
            .unwrap();
        wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
            .unwrap();

        wal.remove_table_log(TIME_SERIES_TABLE_NAME).unwrap();
        wal.create_table_log(&metadata).await.unwrap();

        assert_eq!(
            wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_remove_table_log_fails_if_table_log_does_not_exist() {
        let (_temp_dir, mut wal) = new_empty_write_ahead_log().await;

        let result = wal.remove_table_log(TIME_SERIES_TABLE_NAME);

        assert_eq!(
            result.err().unwrap().to_string(),
            format!(
                "Invalid State Error: Table log for table '{TIME_SERIES_TABLE_NAME}' does not exist.",
            )
        );
    }

    #[tokio::test]
    async fn test_append_to_table_log_returns_incrementing_batch_ids() {
        let (_temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let wal = WriteAheadLog::try_new(&data_folder, SEGMENT_SIZE_THRESHOLD_IN_BYTES)
            .await
            .unwrap();

        let batch = table::uncompressed_time_series_table_record_batch(10);

        assert_eq!(
            wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
                .unwrap(),
            0
        );
        assert_eq!(
            wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
                .unwrap(),
            1
        );
        assert_eq!(
            wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn test_append_to_table_log_fails_if_table_log_does_not_exist() {
        let (_temp_dir, wal) = new_empty_write_ahead_log().await;

        let batch = table::uncompressed_time_series_table_record_batch(10);
        let result = wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch);

        assert_eq!(
            result.err().unwrap().to_string(),
            format!(
                "Invalid State Error: Table log for table '{TIME_SERIES_TABLE_NAME}' does not exist.",
            )
        );
    }

    #[tokio::test]
    async fn test_mark_batches_as_persisted_in_table_log_removes_from_unpersisted() {
        let (_temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let wal = WriteAheadLog::try_new(&data_folder, SEGMENT_SIZE_THRESHOLD_IN_BYTES)
            .await
            .unwrap();

        let batch = table::uncompressed_time_series_table_record_batch(10);
        wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
            .unwrap();
        wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
            .unwrap();

        let unpersisted = wal
            .unpersisted_batches_in_table_log(TIME_SERIES_TABLE_NAME)
            .unwrap();
        assert_eq!(unpersisted.len(), 2);

        wal.mark_batches_as_persisted_in_table_log(TIME_SERIES_TABLE_NAME, HashSet::from([0, 1]))
            .unwrap();

        let unpersisted = wal
            .unpersisted_batches_in_table_log(TIME_SERIES_TABLE_NAME)
            .unwrap();

        assert!(unpersisted.is_empty());
    }

    #[tokio::test]
    async fn test_mark_batches_as_persisted_in_table_log_fails_if_table_log_does_not_exist() {
        let (_temp_dir, wal) = new_empty_write_ahead_log().await;

        let result =
            wal.mark_batches_as_persisted_in_table_log(TIME_SERIES_TABLE_NAME, HashSet::new());

        assert_eq!(
            result.err().unwrap().to_string(),
            format!(
                "Invalid State Error: Table log for table '{TIME_SERIES_TABLE_NAME}' does not exist.",
            )
        );
    }

    #[tokio::test]
    async fn test_unpersisted_batches_in_table_log_returns_all_when_none_persisted() {
        let (_temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let wal = WriteAheadLog::try_new(&data_folder, SEGMENT_SIZE_THRESHOLD_IN_BYTES)
            .await
            .unwrap();

        let batch = table::uncompressed_time_series_table_record_batch(10);
        wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
            .unwrap();
        wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
            .unwrap();

        let unpersisted = wal
            .unpersisted_batches_in_table_log(TIME_SERIES_TABLE_NAME)
            .unwrap();

        assert_eq!(unpersisted.len(), 2);
    }

    #[tokio::test]
    async fn test_unpersisted_batches_in_table_log_fails_if_table_log_does_not_exist() {
        let (_temp_dir, wal) = new_empty_write_ahead_log().await;

        let result = wal.unpersisted_batches_in_table_log(TIME_SERIES_TABLE_NAME);

        assert_eq!(
            result.err().unwrap().to_string(),
            format!(
                "Invalid State Error: Table log for table '{TIME_SERIES_TABLE_NAME}' does not exist.",
            )
        );
    }

    #[tokio::test]
    async fn test_truncate_table_log_clears_unpersisted_batches_and_preserves_batch_ids() {
        let (_temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let wal = WriteAheadLog::try_new(&data_folder, SEGMENT_SIZE_THRESHOLD_IN_BYTES)
            .await
            .unwrap();

        let batch = table::uncompressed_time_series_table_record_batch(10);

        assert_eq!(
            wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
                .unwrap(),
            0
        );
        assert_eq!(
            wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
                .unwrap(),
            1
        );

        let unpersisted = wal
            .unpersisted_batches_in_table_log(TIME_SERIES_TABLE_NAME)
            .unwrap();
        assert_eq!(unpersisted.len(), 2);

        wal.truncate_table_log(TIME_SERIES_TABLE_NAME).unwrap();

        let unpersisted = wal
            .unpersisted_batches_in_table_log(TIME_SERIES_TABLE_NAME)
            .unwrap();
        assert!(unpersisted.is_empty());

        assert_eq!(
            wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn test_truncate_table_log_fails_if_table_log_does_not_exist() {
        let (_temp_dir, wal) = new_empty_write_ahead_log().await;

        let result = wal.truncate_table_log(TIME_SERIES_TABLE_NAME);

        assert_eq!(
            result.err().unwrap().to_string(),
            format!(
                "Invalid State Error: Table log for table '{TIME_SERIES_TABLE_NAME}' does not exist.",
            )
        );
    }

    #[tokio::test]
    async fn test_set_segment_size_threshold_in_bytes_updates_existing_table_logs() {
        let (_temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let mut wal = WriteAheadLog::try_new(&data_folder, SEGMENT_SIZE_THRESHOLD_IN_BYTES)
            .await
            .unwrap();

        let batch = table::uncompressed_time_series_table_record_batch(10);
        let batch_memory_size = batch.get_array_memory_size() as u64;

        // Set the threshold to exactly one batch so the next append closes the segment.
        wal.set_segment_size_threshold_in_bytes(batch_memory_size);
        assert_eq!(wal.segment_size_threshold_in_bytes, batch_memory_size);

        let table_log = wal.table_log(TIME_SERIES_TABLE_NAME).unwrap();
        assert_eq!(table_log.segment_size_threshold_in_bytes, batch_memory_size);

        wal.append_to_table_log(TIME_SERIES_TABLE_NAME, &batch)
            .unwrap();

        let closed_segments = table_log.closed_segments.lock().unwrap();
        assert_eq!(closed_segments.len(), 1);
    }

    async fn new_empty_write_ahead_log() -> (TempDir, WriteAheadLog) {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();
        let wal = WriteAheadLog::try_new(&data_folder, SEGMENT_SIZE_THRESHOLD_IN_BYTES)
            .await
            .unwrap();

        (temp_dir, wal)
    }

    // Tests for SegmentedLog.
    #[test]
    fn test_try_new_creates_active_segment() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let active = segmented_log.active_segment.lock().unwrap();
        assert!(active.path.exists());
        assert_eq!(active.next_batch_id, 0);

        assert!(segmented_log.closed_segments.lock().unwrap().is_empty());
    }

    #[test]
    fn test_read_all_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batches = segmented_log.all_batches().unwrap();
        assert!(batches.is_empty());

        let active = segmented_log.active_segment.lock().unwrap();
        assert_eq!(active.next_batch_id, 0);
    }

    #[test]
    fn test_append_and_read_single_batch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batch = table::uncompressed_time_series_table_record_batch(10);
        segmented_log.append_and_sync(&batch).unwrap();

        let batches = segmented_log.all_batches().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], (0, batch));

        let active = segmented_log.active_segment.lock().unwrap();
        assert_eq!(active.next_batch_id, 1);
    }

    #[test]
    fn test_append_and_read_multiple_batches() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batch_1 = table::uncompressed_time_series_table_record_batch(10);
        let batch_2 = table::uncompressed_time_series_table_record_batch(20);
        let batch_3 = table::uncompressed_time_series_table_record_batch(30);

        segmented_log.append_and_sync(&batch_1).unwrap();
        segmented_log.append_and_sync(&batch_2).unwrap();
        segmented_log.append_and_sync(&batch_3).unwrap();

        let batches = segmented_log.all_batches().unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0], (0, batch_1));
        assert_eq!(batches[1], (1, batch_2));
        assert_eq!(batches[2], (2, batch_3));

        let active = segmented_log.active_segment.lock().unwrap();
        assert_eq!(active.next_batch_id, 3);
    }

    #[test]
    fn test_segment_closes_at_threshold() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batch = table::uncompressed_time_series_table_record_batch(10);
        let segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);

        let closed = segmented_log.closed_segments.lock().unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].start_id, 0);
        assert_eq!(closed[0].end_id, segment_batch_count - 1);

        let active = segmented_log.active_segment.lock().unwrap();
        assert_eq!(active.start_id, segment_batch_count);
    }

    #[test]
    fn test_reopen_loads_closed_segments() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (folder_path, segmented_log) = new_segmented_log(&temp_dir);

        // Write enough batches to close the active segment, then drop.
        let batch = table::uncompressed_time_series_table_record_batch(10);
        let segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);
        drop(segmented_log);

        // The closed segment should be detected and the next id should continue.
        let segmented_log = open_segmented_log(&folder_path);

        let active = segmented_log.active_segment.lock().unwrap();
        assert_eq!(active.next_batch_id, segment_batch_count);
        assert_eq!(segmented_log.closed_segments.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_reopen_and_append_continues_batch_ids() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (folder_path, segmented_log) = new_segmented_log(&temp_dir);

        // Write enough batches to close the active segment, then drop.
        let batch = table::uncompressed_time_series_table_record_batch(10);
        let segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);
        drop(segmented_log);

        let segmented_log = open_segmented_log(&folder_path);
        segmented_log.append_and_sync(&batch).unwrap();

        let batches = segmented_log.all_batches().unwrap();
        assert_eq!(batches.len() as u64, segment_batch_count + 1);

        let active = segmented_log.active_segment.lock().unwrap();
        assert_eq!(active.next_batch_id, segment_batch_count + 1);
    }

    #[test]
    fn test_close_leftover_active_segment_on_reopen() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batch = table::uncompressed_time_series_table_record_batch(10);

        // Write enough batches to close the active segment and append to a new active segment,
        // then drop.
        let segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);
        segmented_log.append_and_sync(&batch).unwrap();
        segmented_log.append_and_sync(&batch).unwrap();
        drop(segmented_log);

        // On re-open the leftover active segment should be closed, leaving two closed segments
        // and a fresh active segment starting after them.
        let segmented_log = open_segmented_log(&folder_path);

        let closed = segmented_log.closed_segments.lock().unwrap();
        assert_eq!(closed.len(), 2);
        assert_eq!(closed[0].start_id, 0);
        assert_eq!(closed[0].end_id, segment_batch_count - 1);
        assert_eq!(closed[1].start_id, segment_batch_count);
        assert_eq!(closed[1].end_id, segment_batch_count + 1);

        let active = segmented_log.active_segment.lock().unwrap();
        assert_eq!(active.next_batch_id, segment_batch_count + 2);
    }

    #[test]
    fn test_delete_leftover_empty_active_segment_on_reopen() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create a segmented log and immediately drop it without writing anything.
        // This leaves an empty "{start_id}-.arrows" active segment.
        let (folder_path, segmented_log) = new_segmented_log(&temp_dir);
        drop(segmented_log);

        // On re-open, the empty leftover active segment should be removed.
        let segmented_log = open_segmented_log(&folder_path);

        assert!(segmented_log.closed_segments.lock().unwrap().is_empty());
        let active = segmented_log.active_segment.lock().unwrap();
        assert_eq!(active.next_batch_id, 0);
        assert!(active.path.exists());

        // Only the new active segment file should exist.
        let file_count = std::fs::read_dir(&folder_path).unwrap().count();
        assert_eq!(file_count, 1);
    }

    #[test]
    fn test_reopen_with_empty_leftover_after_all_segments_persisted_preserves_batch_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batch = table::uncompressed_time_series_table_record_batch(10);

        // Write enough batches to close two segments, persist all, then drop.
        fill_segment_to_threshold(&segmented_log, &batch);
        let segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);

        let ids: HashSet<u64> = (0..segment_batch_count * 2).collect();
        segmented_log.mark_batches_as_persisted(ids).unwrap();

        // Closed segments are deleted. Only the empty active segment remains.
        assert!(segmented_log.closed_segments.lock().unwrap().is_empty());
        assert!(segmented_log.all_batches().unwrap().is_empty());
        drop(segmented_log);

        // On re-open, next_batch_id must continue from where it left off.
        let segmented_log = open_segmented_log(&folder_path);

        let active = segmented_log.active_segment.lock().unwrap();
        assert_eq!(active.next_batch_id, segment_batch_count * 2);
    }

    #[test]
    fn test_mark_batches_as_persisted_deletes_fully_persisted_segment() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batch = table::uncompressed_time_series_table_record_batch(10);
        let segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);

        let segment_path = segmented_log.closed_segments.lock().unwrap()[0]
            .path
            .clone();
        assert!(segment_path.exists());

        let ids: HashSet<u64> = (0..segment_batch_count).collect();
        segmented_log.mark_batches_as_persisted(ids).unwrap();

        assert!(!segment_path.exists());
        assert!(segmented_log.closed_segments.lock().unwrap().is_empty());
    }

    #[test]
    fn test_mark_batches_as_persisted_retains_partially_persisted_segment() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batch = table::uncompressed_time_series_table_record_batch(10);
        let segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);

        let segment_path = segmented_log.closed_segments.lock().unwrap()[0]
            .path
            .clone();

        // Only persist a subset of the batch ids in the closed segment.
        let partial_ids: HashSet<u64> = (0..segment_batch_count - 1).collect();
        segmented_log
            .mark_batches_as_persisted(partial_ids)
            .unwrap();

        // Segment should still exist since not all ids are persisted.
        assert!(segment_path.exists());
        assert_eq!(segmented_log.closed_segments.lock().unwrap().len(), 1);

        // When persisting the last batch, the segment should be deleted.
        segmented_log
            .mark_batches_as_persisted(HashSet::from([segment_batch_count - 1]))
            .unwrap();

        assert!(!segment_path.exists());
        assert!(segmented_log.closed_segments.lock().unwrap().is_empty());
    }

    #[test]
    fn test_multiple_fully_persisted_segments_all_deleted() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batch = table::uncompressed_time_series_table_record_batch(10);

        // Close five full segments.
        let mut segment_batch_count = 0;
        for _ in 0..5 {
            segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);
        }

        assert_eq!(segmented_log.closed_segments.lock().unwrap().len(), 5);

        let ids: HashSet<u64> = (0..segment_batch_count * 5).collect();
        segmented_log.mark_batches_as_persisted(ids).unwrap();

        assert!(segmented_log.closed_segments.lock().unwrap().is_empty());
        assert!(segmented_log.persisted_batch_ids.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_no_batch_ids_in_history_leaves_persisted_set_empty() {
        let (temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let (_wal_dir, segmented_log) = new_segmented_log(&temp_dir);

        let delta_table = data_folder
            .delta_table(TIME_SERIES_TABLE_NAME)
            .await
            .unwrap();

        segmented_log
            .load_persisted_batches_from_delta_table(delta_table)
            .await
            .unwrap();

        assert!(segmented_log.persisted_batch_ids.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_load_persisted_batches_loads_single_commit() {
        let (temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let (_wal_dir, segmented_log) = new_segmented_log(&temp_dir);

        let delta_table =
            write_compressed_segments_with_batch_ids(&data_folder, HashSet::from([0, 1, 2])).await;

        segmented_log
            .load_persisted_batches_from_delta_table(delta_table)
            .await
            .unwrap();

        let persisted = segmented_log.persisted_batch_ids.lock().unwrap();
        assert_eq!(*persisted, BTreeSet::from([0, 1, 2]));
    }

    #[tokio::test]
    async fn test_load_persisted_batches_loads_multiple_commits() {
        let (temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let (_wal_dir, segmented_log) = new_segmented_log(&temp_dir);

        write_compressed_segments_with_batch_ids(&data_folder, HashSet::from([0, 1, 2])).await;
        let delta_table =
            write_compressed_segments_with_batch_ids(&data_folder, HashSet::from([3, 4, 5])).await;

        segmented_log
            .load_persisted_batches_from_delta_table(delta_table)
            .await
            .unwrap();

        let persisted = segmented_log.persisted_batch_ids.lock().unwrap();
        assert_eq!(*persisted, BTreeSet::from([0, 1, 2, 3, 4, 5]));
    }

    #[tokio::test]
    async fn test_load_persisted_batches_deletes_fully_persisted_closed_segment() {
        let (temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let (_wal_dir, segmented_log) = new_segmented_log(&temp_dir);

        let batch = table::uncompressed_time_series_table_record_batch(10);
        let segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);

        let segment_path = segmented_log.closed_segments.lock().unwrap()[0]
            .path
            .clone();
        assert!(segment_path.exists());

        let all_ids: HashSet<u64> = (0..segment_batch_count).collect();
        let delta_table = write_compressed_segments_with_batch_ids(&data_folder, all_ids).await;

        segmented_log
            .load_persisted_batches_from_delta_table(delta_table)
            .await
            .unwrap();

        assert!(!segment_path.exists());
        assert!(segmented_log.closed_segments.lock().unwrap().is_empty());
        assert!(segmented_log.persisted_batch_ids.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_load_persisted_batches_retains_partially_persisted_closed_segment() {
        let (temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        let (_wal_dir, segmented_log) = new_segmented_log(&temp_dir);

        let batch = table::uncompressed_time_series_table_record_batch(10);
        let segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);

        let segment_path = segmented_log.closed_segments.lock().unwrap()[0]
            .path
            .clone();

        let partial_ids: HashSet<u64> = (0..segment_batch_count - 1).collect();
        let delta_table = write_compressed_segments_with_batch_ids(&data_folder, partial_ids).await;

        segmented_log
            .load_persisted_batches_from_delta_table(delta_table)
            .await
            .unwrap();

        assert!(segment_path.exists());
        assert_eq!(segmented_log.closed_segments.lock().unwrap().len(), 1);
        assert_eq!(
            segmented_log.persisted_batch_ids.lock().unwrap().len() as u64,
            segment_batch_count - 1
        );
    }

    async fn create_data_folder_with_time_series_table() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();
        let metadata = table::time_series_table_metadata();

        data_folder
            .create_time_series_table(&metadata)
            .await
            .unwrap();

        (temp_dir, data_folder)
    }

    async fn write_compressed_segments_with_batch_ids(
        data_folder: &DataFolder,
        batch_ids: HashSet<u64>,
    ) -> DeltaTable {
        let compressed_segments = table::compressed_segments_record_batch();

        data_folder
            .write_record_batches_with_batch_ids(
                TIME_SERIES_TABLE_NAME,
                vec![compressed_segments],
                batch_ids,
            )
            .await
            .unwrap()
    }

    #[test]
    fn test_unpersisted_batches_returns_empty_when_no_batches_written() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let unpersisted = segmented_log.unpersisted_batches().unwrap();
        assert!(unpersisted.is_empty());
    }

    #[test]
    fn test_unpersisted_batches_returns_all_when_none_persisted() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batch_1 = table::uncompressed_time_series_table_record_batch(10);
        let batch_2 = table::uncompressed_time_series_table_record_batch(20);
        segmented_log.append_and_sync(&batch_1).unwrap();
        segmented_log.append_and_sync(&batch_2).unwrap();

        let unpersisted = segmented_log.unpersisted_batches().unwrap();
        assert_eq!(unpersisted.len(), 2);
        assert_eq!(unpersisted[0], (0, batch_1));
        assert_eq!(unpersisted[1], (1, batch_2));
    }

    #[test]
    fn test_unpersisted_batches_returns_empty_when_all_persisted() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batch = table::uncompressed_time_series_table_record_batch(10);
        segmented_log.append_and_sync(&batch).unwrap();
        segmented_log.append_and_sync(&batch).unwrap();

        let unpersisted = segmented_log.unpersisted_batches().unwrap();
        assert_eq!(unpersisted.len(), 2);

        segmented_log
            .mark_batches_as_persisted(HashSet::from([0, 1]))
            .unwrap();

        let unpersisted = segmented_log.unpersisted_batches().unwrap();
        assert!(unpersisted.is_empty());
    }

    #[test]
    fn test_unpersisted_batches_filters_persisted_batch_ids() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        let batch_1 = table::uncompressed_time_series_table_record_batch(10);
        let batch_2 = table::uncompressed_time_series_table_record_batch(20);
        let batch_3 = table::uncompressed_time_series_table_record_batch(30);
        segmented_log.append_and_sync(&batch_1).unwrap();
        segmented_log.append_and_sync(&batch_2).unwrap();
        segmented_log.append_and_sync(&batch_3).unwrap();

        let unpersisted = segmented_log.unpersisted_batches().unwrap();
        assert_eq!(unpersisted.len(), 3);
        assert_eq!(unpersisted[1], (1, batch_2));

        segmented_log
            .mark_batches_as_persisted(HashSet::from([1]))
            .unwrap();

        let unpersisted = segmented_log.unpersisted_batches().unwrap();
        assert_eq!(unpersisted.len(), 2);
        assert_eq!(unpersisted[0], (0, batch_1));
        assert_eq!(unpersisted[1], (2, batch_3));
    }

    #[test]
    fn test_unpersisted_batches_returns_batches_across_closed_and_active_segments() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        // Fill one full segment and write two more into the active segment.
        let batch = table::uncompressed_time_series_table_record_batch(10);
        let segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);
        segmented_log.append_and_sync(&batch).unwrap();
        segmented_log.append_and_sync(&batch).unwrap();

        // Persist one batch id in the closed segment and one in the active segment.
        segmented_log
            .mark_batches_as_persisted(HashSet::from([0, segment_batch_count + 1]))
            .unwrap();

        assert_eq!(segmented_log.closed_segments.lock().unwrap().len(), 1);

        let unpersisted = segmented_log.unpersisted_batches().unwrap();
        assert_eq!(unpersisted.len() as u64, segment_batch_count);
        assert_eq!(unpersisted.first().unwrap(), &(1, batch.clone()));
        assert_eq!(unpersisted.last().unwrap(), &(segment_batch_count, batch));
    }

    #[test]
    fn test_truncate_clears_files_and_state() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (folder_path, segmented_log) = new_segmented_log(&temp_dir);

        // Fill five full segments and write two more into the active segment.
        let batch = table::uncompressed_time_series_table_record_batch(10);
        for _ in 0..5 {
            fill_segment_to_threshold(&segmented_log, &batch);
        }

        segmented_log
            .mark_batches_as_persisted(HashSet::from([0, 1, 2]))
            .unwrap();

        segmented_log.append_and_sync(&batch).unwrap();
        segmented_log.append_and_sync(&batch).unwrap();

        // Ensure we have five closed segments in memory and on disk, and one active segment.
        assert_eq!(segmented_log.closed_segments.lock().unwrap().len(), 5);
        assert_eq!(std::fs::read_dir(&folder_path).unwrap().count(), 6);

        segmented_log.truncate().unwrap();

        // Verify in-memory state is cleared.
        assert_eq!(segmented_log.closed_segments.lock().unwrap().len(), 0);
        assert_eq!(segmented_log.persisted_batch_ids.lock().unwrap().len(), 0);

        // Verify the old closed segments and the old active segment were deleted from disk,
        // and exactly one new active segment was created.
        assert_eq!(std::fs::read_dir(&folder_path).unwrap().count(), 1);

        let unpersisted = segmented_log.unpersisted_batches().unwrap();
        assert!(unpersisted.is_empty());
    }

    #[test]
    fn test_truncate_preserves_batch_ids() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_folder_path, segmented_log) = new_segmented_log(&temp_dir);

        // Fill one full segment and write two more into the active segment.
        let batch = table::uncompressed_time_series_table_record_batch(10);
        let segment_batch_count = fill_segment_to_threshold(&segmented_log, &batch);
        segmented_log.append_and_sync(&batch).unwrap();
        segmented_log.append_and_sync(&batch).unwrap();

        let expected_next_id = segment_batch_count + 2;

        // Truncate should reset the state but carry over the next_batch_id.
        segmented_log.truncate().unwrap();

        let active = segmented_log.active_segment.lock().unwrap();
        assert_eq!(active.start_id, expected_next_id);
        assert_eq!(active.next_batch_id, expected_next_id);
    }

    #[test]
    fn test_truncate_empty_segmented_log() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (folder_path, segmented_log) = new_segmented_log(&temp_dir);

        // Truncate the segmented log before appending any data.
        segmented_log.truncate().unwrap();

        // Verify in-memory state remains empty.
        assert!(segmented_log.closed_segments.lock().unwrap().is_empty());
        assert!(segmented_log.persisted_batch_ids.lock().unwrap().is_empty());

        // Verify the active segment ID is still 0, so it has not artificially incremented.
        let active = segmented_log.active_segment.lock().unwrap();
        assert_eq!(active.start_id, 0);
        assert_eq!(active.next_batch_id, 0);
        drop(active);

        // Verify the old active segment was successfully replaced and exactly one file remains.
        assert_eq!(std::fs::read_dir(&folder_path).unwrap().count(), 1);
        let unpersisted = segmented_log.unpersisted_batches().unwrap();
        assert!(unpersisted.is_empty());
    }

    /// Fill the segment with `batch` until it reaches [`SEGMENT_SIZE_THRESHOLD_IN_BYTES`]. Return
    /// the number of batches that were appended.
    fn fill_segment_to_threshold(segmented_log: &SegmentedLog, batch: &RecordBatch) -> u64 {
        let memory_size = batch.get_array_memory_size();
        let threshold_batch_count = SEGMENT_SIZE_THRESHOLD_IN_BYTES.div_ceil(memory_size as u64);

        for _ in 0..threshold_batch_count {
            segmented_log.append_and_sync(batch).unwrap();
        }

        threshold_batch_count
    }

    fn open_segmented_log(folder_path: &Path) -> SegmentedLog {
        let metadata = table::time_series_table_metadata();
        SegmentedLog::try_new(
            folder_path.to_path_buf(),
            &metadata.schema,
            SEGMENT_SIZE_THRESHOLD_IN_BYTES,
        )
        .unwrap()
    }

    fn new_segmented_log(temp_dir: &TempDir) -> (PathBuf, SegmentedLog) {
        let folder_path = temp_dir.path().join(TIME_SERIES_TABLE_NAME);
        let segmented_log = open_segmented_log(&folder_path);
        (folder_path, segmented_log)
    }
}
