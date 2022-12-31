/* Copyright 2022 The ModelarDB Contributors
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

//! Support for managing all compressed data that is inserted into the [`StorageEngine`].

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Error as IOError;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use futures::StreamExt;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use tracing::{debug, info};

use crate::errors::ModelarDbError;
use crate::storage::compressed_data_buffer::CompressedDataBuffer;
use crate::storage::data_transfer::DataTransfer;
use crate::storage::{StorageEngine, COMPRESSED_DATA_FOLDER};
use crate::types::{CompressedSchema, Timestamp, Value};

/// Stores data points compressed as models in memory to batch compressed data before saving it to
/// Apache Parquet files.
pub(super) struct CompressedDataManager {
    /// Component that transfers saved compressed data to the remote data folder when it is necessary.
    pub(super) data_transfer: Option<DataTransfer>,
    /// Path to the folder containing all compressed data managed by the [`StorageEngine`].
    local_data_folder: PathBuf,
    /// The compressed segments before they are saved to persistent storage.
    compressed_data_buffers: HashMap<String, CompressedDataBuffer>,
    /// FIFO queue of table names referring to [`CompressedDataBuffer`] that can be saved to
    /// persistent storage.
    compressed_queue: VecDeque<String>,
    /// How many bytes of memory that are left for storing compressed segments. A signed integer is
    /// used since compressed data is inserted and then the remaining bytes are checked. This means
    /// that the remaining bytes can briefly be negative until compressed data is saved to disk.
    compressed_remaining_memory_in_bytes: isize,
    /// Reference to the schema for compressed data buffers.
    compressed_schema: CompressedSchema,
}

impl CompressedDataManager {
    /// Return a [`CompressedDataManager`] if the required folder can be created in
    /// `local_data_folder`, otherwise [`IOError`] is returned.
    pub(super) fn try_new(
        data_transfer: Option<DataTransfer>,
        local_data_folder: PathBuf,
        compressed_reserved_memory_in_bytes: usize,
        compressed_schema: CompressedSchema,
    ) -> Result<Self, IOError> {
        // Ensure the folder required by the compressed data manager exists.
        fs::create_dir_all(local_data_folder.join(COMPRESSED_DATA_FOLDER))?;

        Ok(Self {
            data_transfer,
            local_data_folder,
            compressed_data_buffers: HashMap::new(),
            compressed_queue: VecDeque::new(),
            compressed_remaining_memory_in_bytes: compressed_reserved_memory_in_bytes as isize,
            compressed_schema,
        })
    }

    /// Write `record_batch` to the table with `table_name` as a compressed Apache Parquet file.
    pub(super) async fn insert_record_batch(
        &self,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> Result<(), ParquetError> {
        debug!(
            "Received record batch with {} rows for the table '{}'.",
            record_batch.num_rows(),
            table_name
        );
        let local_file_path = self
            .local_data_folder
            .join(COMPRESSED_DATA_FOLDER)
            .join(table_name);

        // Create the folder structure if it does not already exist.
        fs::create_dir_all(local_file_path.as_path())?;

        // Create a path that uses the current timestamp as the filename.
        let since_the_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| ParquetError::General(error.to_string()))?;
        let file_name = format!("{}.parquet", since_the_epoch.as_millis());
        let file_path = local_file_path.join(file_name);

        StorageEngine::write_batch_to_apache_parquet_file(record_batch, file_path.as_path())
    }

    /// Insert the `compressed_segments` into the in-memory compressed data buffer for the table
    /// with `table_name`. If `compressed_segments` is saved successfully, return [`Ok`], otherwise
    /// return [`IOError`].
    pub(super) async fn insert_compressed_segments(
        &mut self,
        table_name: &str,
        compressed_segments: RecordBatch,
    ) -> Result<(), IOError> {
        debug!(
            "Inserting batch with {} rows into compressed data buffer for table '{}'.",
            compressed_segments.num_rows(),
            table_name
        );

        // Since the compressed segments are already in memory, insert the segments into the
        // compressed data buffer first and check if the reserved memory limit is exceeded after.
        let segments_size = if let Some(compressed_data_buffer) =
            self.compressed_data_buffers.get_mut(table_name)
        {
            debug!(
                "Found existing compressed data buffer for table '{}'.",
                table_name
            );

            compressed_data_buffer.append_compressed_segments(compressed_segments)
        } else {
            debug!(
                "Could not find compressed data buffer for table '{}'. Creating compressed data buffer.",
                table_name
            );

            let mut compressed_data_buffer = CompressedDataBuffer::new();
            let segment_size =
                compressed_data_buffer.append_compressed_segments(compressed_segments);

            self.compressed_data_buffers
                .insert(table_name.to_owned(), compressed_data_buffer);
            self.compressed_queue.push_back(table_name.to_owned());

            segment_size
        };

        self.compressed_remaining_memory_in_bytes -= segments_size as isize;

        // If the reserved memory limit is exceeded, save compressed data to disk.
        if self.compressed_remaining_memory_in_bytes < 0 {
            self.save_compressed_data_to_free_memory().await?;
        }

        Ok(())
    }

    /// Return an [`ObjectMeta`] for each compressed file in `query_data_folder` that belongs to the
    /// table with `table_name` and contains compressed segments within the given range of time and
    /// value. If some compressed data that belongs to `table_name` is still in memory, save it to
    /// disk first. If no files belongs to the table with `table_name` an empty [`Vec`] is returned,
    /// while a [`DataRetrievalError`](ModelarDbError::DataRetrievalError) is returned if:
    /// * A table with `table_name` does not exist.
    /// * The compressed files could not be listed.
    /// * The end time is before the start time.
    /// * The max value is smaller than the min value.
    pub(super) async fn save_and_get_saved_compressed_files(
        &mut self,
        table_name: &str,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        min_value: Option<Value>,
        max_value: Option<Value>,
        query_data_folder: &Arc<dyn ObjectStore>,
    ) -> Result<Vec<ObjectMeta>, ModelarDbError> {
        // If there is any compressed data in memory, save it first.
        if self.compressed_data_buffers.contains_key(table_name) {
            // Remove the data from the queue of compressed time series that are ready to be saved.
            // unwrap() is safe since `compressed_data` has the same tables as `compressed_queue`.
            let data_index = self
                .compressed_queue
                .iter()
                .position(|x| *x == table_name)
                .unwrap();
            self.compressed_queue.remove(data_index);

            self.save_compressed_data(table_name)
                .await
                .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?;
        }

        // Set default values for the parts of the time and value range that is not defined.
        let start_time = start_time.unwrap_or(0);
        let end_time = end_time.unwrap_or(Timestamp::MAX);

        if start_time > end_time {
            return Err(ModelarDbError::DataRetrievalError(format!(
                "Start time '{}' cannot be after end time '{}'.",
                start_time, end_time
            )));
        };

        let min_value = min_value.unwrap_or(Value::NEG_INFINITY);
        let max_value = max_value.unwrap_or(Value::INFINITY);

        if min_value > max_value {
            return Err(ModelarDbError::DataRetrievalError(format!(
                "Min value '{}' cannot be larger than max value '{}'.",
                min_value, max_value
            )));
        };

        // List all files in query_data_folder for the table named table_name.
        let table_path =
            ObjectStorePath::from(format!("{}/{}/", COMPRESSED_DATA_FOLDER, table_name));
        let table_files = query_data_folder
            .list(Some(&table_path))
            .await
            .map_err(|error| {
                ModelarDbError::DataRetrievalError(format!(
                    "Compressed data could not be listed for table '{}': {}",
                    table_name, error
                ))
            })?;

        // Return all relevant Apache Parquet files stored for the table named table_name.
        let table_relevant_apache_parquet_files = table_files
            .filter_map(|maybe_meta| async {
                if let Ok(object_meta) = maybe_meta {
                    is_object_meta_relevant(
                        object_meta,
                        start_time,
                        end_time,
                        min_value,
                        max_value,
                        query_data_folder,
                    )
                    .await
                } else {
                    None
                }
            })
            .collect::<Vec<ObjectMeta>>()
            .await;

        Ok(table_relevant_apache_parquet_files)
    }

    /// Save [`CompressedDataBuffers`](CompressedDataBuffer) to disk until the reserved memory limit
    /// is no longer exceeded. If all of the data is saved successfully, return [`Ok`], otherwise
    /// return [`IOError`].
    fn save_compressed_data_to_free_memory(&mut self) -> Result<(), IOError> {
        debug!("Out of memory for compressed data. Saving compressed data to disk.");

        while self.compressed_remaining_memory_in_bytes < 0 {
            let table_name = self.compressed_queue.pop_front().unwrap();
            self.save_compressed_data(&table_name)?;
        }
        Ok(())
    }

    /// Flush the data that the [`CompressedDataManager`] is currently managing.
    pub(super) fn flush(&mut self) -> Result<(), IOError> {
        info!(
            "Flushing the remaining {} compressed data buffers.",
            self.compressed_queue.len()
        );

        while !self.compressed_queue.is_empty() {
            let table = self.compressed_queue.pop_front().unwrap();
            self.save_compressed_data(&table)?;
        }

        Ok(())
    }

    /// Save the compressed data that belongs to the table with `table_name` to disk. The size of
    /// the saved compressed data is added back to the remaining reserved memory. If the data is
    /// saved successfully, return [`Ok`], otherwise return [`IOError`].
    fn save_compressed_data(&mut self, table_name: &str) -> Result<(), IOError> {
        debug!("Saving compressed time series to disk.");

        let mut compressed_data_buffer = self.compressed_data_buffers.remove(table_name).unwrap();
        let folder_path = self
            .local_data_folder
            .join(COMPRESSED_DATA_FOLDER)
            .join(table_name);

        compressed_data_buffer
            .save_to_apache_parquet(folder_path.as_path(), &self.compressed_schema)?;
        self.compressed_remaining_memory_in_bytes += compressed_data_buffer.size_in_bytes as isize;

        debug!(
            "Saved {} bytes of compressed data to disk. Remaining reserved bytes: {}.",
            compressed_data_buffer.size_in_bytes, self.compressed_remaining_memory_in_bytes
        );

        Ok(())
    }
}

/// Return the [`ObjectMeta`] passed as `object_meta` if it represents an Apache Parquet file and if
/// the timestamps and values in its filename overlaps with the time range given by `start_time` and
/// `end_time` and the value range given by `min_value` and `max_value`, otherwise [`None`] is
/// returned. Assumes the name of Apache Parquet files represented by `object_meta` has the
/// following format: `start-timestamp_end-timestamp_min-value_max-value.parquet`, where both
/// `start-timestamp` and `end-timestamp` are of the same unit as `start_time` and `end_time`.
async fn is_object_meta_relevant(
    object_meta: ObjectMeta,
    start_time: Timestamp,
    end_time: Timestamp,
    min_value: Value,
    max_value: Value,
    query_data_folder: &Arc<dyn ObjectStore>,
) -> Option<ObjectMeta> {
    let location = &object_meta.location;
    if StorageEngine::is_path_an_apache_parquet_file(query_data_folder, location).await {
        // unwrap() is safe as the file is created by the storage engine.
        let last_location_part = location.parts().last().unwrap();

        if is_compressed_file_within_time_and_value_range(
            last_location_part.as_ref(),
            start_time,
            end_time,
            min_value,
            max_value,
        ) {
            return Some(object_meta);
        }
    }
    None
}

/// Return [`true`] if the timestamps and values in `file name` overlaps with the time range given
/// by `start_time` and `end_time` and the value range given by `min_value` and `max_value`,
/// otherwise [`false`]. Assumes `file_name` has the following format:
/// `start-timestamp_end-timestamp_min-value_max-value.parquet`, where both `start-timestamp` and
/// `end-timestamp` are of the same unit as `start_time` and `end_time`.
fn is_compressed_file_within_time_and_value_range(
    file_name: &str,
    start_time: Timestamp,
    end_time: Timestamp,
    min_value: Value,
    max_value: Value,
) -> bool {
    // unwrap() is safe to use since the file name structure is generated by the storage engine.
    let dot_index = file_name.rfind('.').unwrap();
    let mut split = file_name[0..dot_index].split('_');

    let file_start_time = split.next().unwrap().parse::<Timestamp>().unwrap();
    let file_end_time = split.next().unwrap().parse::<Timestamp>().unwrap();
    let file_ends_after_start = file_end_time >= start_time;
    let file_starts_before_end = file_start_time <= end_time;

    let file_min_value = split.next().unwrap().parse::<Value>().unwrap();
    let file_max_value = split.next().unwrap().parse::<Value>().unwrap();
    let file_max_greater_than_min = file_max_value >= min_value;
    let file_min_less_than_max = file_min_value <= max_value;

    // Return true if the file's compressed segments ends at or after the start time, starts at or
    // before the end time, has a maximum value that is larger than or equal to the minimum
    // requested, and has a minimum value that is smaller than or equal to the maximum requested.
    file_ends_after_start
        && file_starts_before_end
        && file_max_greater_than_min
        && file_min_less_than_max
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use datafusion::arrow::compute;
    use object_store::local::LocalFileSystem;
    use tempfile::{self, TempDir};

    use crate::array;
    use crate::metadata::test_util as metadata_test_util;
    use crate::storage::{self, test_util};
    use crate::types::{TimestampArray, ValueArray};

    const TABLE_NAME: &str = "table";

    // Tests for insert_record_batch().
    #[tokio::test]
    async fn test_insert_record_batch() {
        let record_batch = test_util::compressed_segments_record_batch();
        let (temp_dir, data_manager) = create_compressed_data_manager();

        let local_data_folder = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
        let table_folder =
            ObjectStorePath::parse(format!("{}/{}/", COMPRESSED_DATA_FOLDER, TABLE_NAME)).unwrap();

        let table_folder_files = local_data_folder
            .list(Some(&table_folder))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(table_folder_files.is_empty());

        data_manager
            .insert_record_batch(TABLE_NAME, record_batch)
            .await
            .unwrap();

        let table_folder_files = local_data_folder
            .list(Some(&table_folder))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(table_folder_files.len(), 1);
    }

    // Tests for insert_compressed_segments().
    #[test]
    fn test_can_insert_compressed_segment_into_new_compressed_data_buffer() {
        let segments = test_util::compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager
            .insert_compressed_segments(TABLE_NAME, segments)
            .unwrap();

        assert!(data_manager
            .compressed_data_buffers
            .contains_key(TABLE_NAME));
        assert_eq!(
            data_manager.compressed_queue.pop_front().unwrap(),
            TABLE_NAME
        );
        assert!(
            data_manager
                .compressed_data_buffers
                .get(TABLE_NAME)
                .unwrap()
                .size_in_bytes
                > 0
        );
    }

    #[test]
    fn test_can_insert_compressed_segment_into_existing_compressed_data_buffer() {
        let segments = test_util::compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager
            .insert_compressed_segments(TABLE_NAME, segments.clone())
            .unwrap();
        let previous_size = data_manager
            .compressed_data_buffers
            .get(TABLE_NAME)
            .unwrap()
            .size_in_bytes;

        data_manager
            .insert_compressed_segments(TABLE_NAME, segments)
            .unwrap();

        assert!(
            data_manager
                .compressed_data_buffers
                .get(TABLE_NAME)
                .unwrap()
                .size_in_bytes
                > previous_size
        );
    }

    #[test]
    fn test_save_first_compressed_data_buffer_if_out_of_memory() {
        let segments = test_util::compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();
        let reserved_memory = data_manager.compressed_remaining_memory_in_bytes as usize;

        // Insert compressed data into the storage engine until data is saved to Apache Parquet.
        let max_compressed_segments = reserved_memory / test_util::COMPRESSED_SEGMENTS_SIZE;
        for _ in 0..max_compressed_segments + 1 {
            data_manager
                .insert_compressed_segments(TABLE_NAME, segments.clone())
                .unwrap();
        }

        // The compressed data should be saved to the table_name folder in the compressed folder.
        let local_data_folder = Path::new(&data_manager.local_data_folder);
        let compressed_path =
            local_data_folder.join(format!("{}/{}", COMPRESSED_DATA_FOLDER, TABLE_NAME));
        assert_eq!(compressed_path.read_dir().unwrap().count(), 1);
    }

    #[test]
    fn test_remaining_bytes_decremented_when_inserting_compressed_segments() {
        let segments = test_util::compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();
        let reserved_memory = data_manager.compressed_remaining_memory_in_bytes;

        data_manager
            .insert_compressed_segments(TABLE_NAME, segments)
            .unwrap();

        assert!(reserved_memory > data_manager.compressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_incremented_when_saving_compressed_segments() {
        let segments = test_util::compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager
            .insert_compressed_segments(TABLE_NAME, segments.clone())
            .unwrap();

        // Set the remaining memory to a negative value since data is only saved when out of memory.
        data_manager.compressed_remaining_memory_in_bytes = -1;
        data_manager.save_compressed_data_to_free_memory().unwrap();

        assert!(-1 < data_manager.compressed_remaining_memory_in_bytes);
    }

    /// Create a [`CompressedDataManager`] with a folder that is deleted once the test is finished.
    fn create_compressed_data_manager() -> (TempDir, CompressedDataManager) {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = metadata_test_util::test_metadata_manager(temp_dir.path());

        let local_data_folder = temp_dir.path().to_path_buf();
        (
            temp_dir,
            CompressedDataManager::try_new(
                None,
                local_data_folder,
                metadata_manager.compressed_reserved_memory_in_bytes,
                metadata_manager.compressed_schema(),
            )
            .unwrap(),
        )
    }

    // Tests for save_and_get_saved_compressed_files().
    #[tokio::test]
    async fn test_can_get_compressed_file_for_table() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        // Insert compressed segments into the same table.
        let segment = test_util::compressed_segments_record_batch();
        data_manager
            .insert_compressed_segments(TABLE_NAME, segment.clone())
            .unwrap();
        data_manager
            .insert_compressed_segments(TABLE_NAME, segment)
            .unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .save_and_get_saved_compressed_files(TABLE_NAME, None, None, None, None, &object_store)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_get_no_compressed_files_for_non_existent_table() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.save_and_get_saved_compressed_files(
            TABLE_NAME,
            None,
            None,
            None,
            None,
            &object_store,
        );

        assert!(result.await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_can_get_compressed_file_with_start_time() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager();
        let (segment_1, _segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0);

        // If we have a start time after the first segments ends, only the file containing the
        // second segment should be retrieved.
        let end_times = array!(segment_1, 3, TimestampArray);
        let start_time = Some(end_times.value(end_times.len() - 1) + 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.save_and_get_saved_compressed_files(
            TABLE_NAME,
            start_time,
            None,
            None,
            None,
            &object_store,
        );
        let files = result.await.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the table name, the start time,
        // end time, min value, and max value of the second segment.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!(
                "{}/{}/2000_2005_15.2_44.2.parquet",
                COMPRESSED_DATA_FOLDER, TABLE_NAME
            )
        );
    }

    #[tokio::test]
    async fn test_can_get_compressed_file_with_min_value() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager();
        let (segment_1, _segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0);

        // If we have a min value higher then the max value in the first segment, only the file
        // containing the second segment should be retrieved.
        let max_values = array!(segment_1, 6, ValueArray);
        let min_value = Some(compute::max(max_values).unwrap() + 1.0);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.save_and_get_saved_compressed_files(
            TABLE_NAME,
            None,
            None,
            min_value,
            None,
            &object_store,
        );
        let files = result.await.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the table name, the start time,
        // end time, min value, and max value of the second segment.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!(
                "{}/{}/2000_2005_15.2_44.2.parquet",
                COMPRESSED_DATA_FOLDER, TABLE_NAME
            )
        );
    }

    #[tokio::test]
    async fn test_can_get_compressed_file_with_end_time() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager();
        let (_segment_1, segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0);

        // If we have an end time before the second segment starts, only the file containing the
        // first segment should be retrieved.
        let start_times = array!(segment_2, 2, TimestampArray);
        let end_time = Some(start_times.value(1) - 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.save_and_get_saved_compressed_files(
            TABLE_NAME,
            None,
            end_time,
            None,
            None,
            &object_store,
        );
        let files = result.await.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the table name, the start time,
        // end time, min value, and max value of the first segment.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!(
                "{}/{}/1000_1005_5.2_34.2.parquet",
                COMPRESSED_DATA_FOLDER, TABLE_NAME
            )
        );
    }

    #[tokio::test]
    async fn test_can_get_compressed_file_with_max_value() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager();
        let (_segment_1, segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0);

        // If we have a max value lower then the min value in the second segment, only the file
        // containing the first segment should be retrieved.
        let min_values = array!(segment_2, 5, ValueArray);
        let max_value = Some(compute::min(min_values).unwrap() - 1.0);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.save_and_get_saved_compressed_files(
            TABLE_NAME,
            None,
            None,
            None,
            max_value,
            &object_store,
        );
        let files = result.await.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the table name, the start time,
        // end time, min value, and max value of the first segment.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!(
                "{}/{}/1000_1005_5.2_34.2.parquet",
                COMPRESSED_DATA_FOLDER, TABLE_NAME
            )
        );
    }

    #[tokio::test]
    async fn test_can_get_compressed_files_with_start_time_and_end_time() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        // Insert 4 segments with a ~1 second time difference between segment 1 and 2 and segment 3 and 4.
        let (segment_1, _segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0);
        let (_segment_3, segment_4) = insert_separated_segments(&mut data_manager, 1000, 0.0);

        // If we have a start time after the first segment and an end time before the fourth
        // segment, only the files containing the second and third segment should be retrieved.
        let end_times = array!(segment_1, 3, TimestampArray);
        let start_times = array!(segment_4, 2, TimestampArray);

        let start_time = Some(end_times.value(end_times.len() - 1) + 100);
        let end_time = Some(start_times.value(1) - 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.save_and_get_saved_compressed_files(
            TABLE_NAME,
            start_time,
            end_time,
            None,
            None,
            &object_store,
        );
        let mut files = result.await.unwrap();
        assert_eq!(files.len(), 2);

        // Sort files as save_and_get_saved_compressed_files() does not guarantee an ordering.a
        files.sort_unstable_by(|a, b| a.location.cmp(&b.location));

        // The files containing the second and third segment should be returned and their path
        // should contain the table name, the start time, end time, min value, and max value of the
        // segment in each file.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!(
                "{}/{}/2000_2005_15.2_44.2.parquet",
                COMPRESSED_DATA_FOLDER, TABLE_NAME
            )
        );
        let file_path = files.get(1).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!(
                "{}/{}/2000_2005_5.2_34.2.parquet",
                COMPRESSED_DATA_FOLDER, TABLE_NAME
            )
        );
    }

    #[tokio::test]
    async fn test_can_get_compressed_files_with_min_value_and_max_value() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        // Insert 4 segments with a ~1 second time difference between segment 1 and 2 and segment 3 and 4.
        let (segment_1, _segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0);
        let (_segment_3, segment_4) = insert_separated_segments(&mut data_manager, 1000, 100.0);

        // If we have a min value higher the first segment and a max value lower than the fourth
        // segment, only the files containing the second and third segment should be retrieved.
        let max_values = array!(segment_1, 6, ValueArray);
        let min_values = array!(segment_4, 5, ValueArray);

        let min_value = Some(compute::max(max_values).unwrap() + 1.0);
        let max_value = Some(compute::min(min_values).unwrap() - 1.0);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.save_and_get_saved_compressed_files(
            TABLE_NAME,
            None,
            None,
            min_value,
            max_value,
            &object_store,
        );
        let mut files = result.await.unwrap();
        assert_eq!(files.len(), 2);

        // Sort files as save_and_get_saved_compressed_files() does not guarantee an ordering.
        files.sort_unstable_by(|a, b| a.location.cmp(&b.location));

        // The files containing the second and third segment should be returned and their path
        // should contain the table name, the start time, end time, min value, and max value of the
        // segment in each file.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!(
                "{}/{}/2000_2005_105.2_134.2.parquet",
                COMPRESSED_DATA_FOLDER, TABLE_NAME
            )
        );
        let file_path = files.get(1).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!(
                "{}/{}/2000_2005_15.2_44.2.parquet",
                COMPRESSED_DATA_FOLDER, TABLE_NAME
            )
        );
    }

    /// Create and insert two compressed segments with a 1 second time difference offset by
    /// `start_time` and values with difference of 10 offset by `value_offset`.
    fn insert_separated_segments(
        data_manager: &mut CompressedDataManager,
        start_time: i64,
        value_offset: f32,
    ) -> (RecordBatch, RecordBatch) {
        let segment_1 =
            test_util::compressed_segments_record_batch_with_time(1000 + start_time, value_offset);
        data_manager
            .insert_compressed_segments(TABLE_NAME, segment_1.clone())
            .unwrap();
        data_manager.flush().unwrap();

        let segment_2 = test_util::compressed_segments_record_batch_with_time(
            2000 + start_time,
            10.0 + value_offset,
        );
        data_manager
            .insert_compressed_segments(TABLE_NAME, segment_2.clone())
            .unwrap();
        data_manager.flush().unwrap();

        (segment_1, segment_2)
    }

    #[tokio::test]
    async fn test_cannot_get_compressed_files_where_end_time_is_before_start_time() {
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        let segment = test_util::compressed_segments_record_batch();
        data_manager
            .insert_compressed_segments(TABLE_NAME, segment)
            .unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let result = data_manager.save_and_get_saved_compressed_files(
            TABLE_NAME,
            Some(10),
            Some(1),
            None,
            None,
            &object_store,
        );

        assert!(result
            .await
            .unwrap_err()
            .to_string()
            .contains("cannot be after end time"));
    }

    #[tokio::test]
    async fn test_cannot_get_compressed_files_where_max_value_is_smaller_than_min_value() {
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        let segment = test_util::compressed_segments_record_batch();
        data_manager
            .insert_compressed_segments(TABLE_NAME, segment)
            .unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let result = data_manager.save_and_get_saved_compressed_files(
            TABLE_NAME,
            None,
            None,
            Some(10.0),
            Some(1.0),
            &object_store,
        );

        assert!(result
            .await
            .unwrap_err()
            .to_string()
            .contains("cannot be larger than max value"));
    }

    #[tokio::test]
    async fn test_can_get_saved_compressed_files() {
        let segments = test_util::compressed_segments_record_batch();
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager
            .insert_compressed_segments(TABLE_NAME, segments.clone())
            .unwrap();
        data_manager.save_compressed_data(TABLE_NAME).unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .save_and_get_saved_compressed_files(TABLE_NAME, None, None, None, None, &object_store)
            .await;
        assert!(result.is_ok());

        let files = result.unwrap();
        assert_eq!(files.len(), 1);

        // The file should have the first start time and the last end time as the file name.
        let file_name = storage::create_time_and_value_range_file_name(&segments);
        let expected_file_path = format!("{}/{}/{}", COMPRESSED_DATA_FOLDER, TABLE_NAME, file_name);

        assert_eq!(
            files.get(0).unwrap().location,
            ObjectStorePath::parse(expected_file_path).unwrap()
        )
    }

    #[tokio::test]
    async fn test_save_in_memory_compressed_data_when_getting_saved_compressed_files() {
        let segments = test_util::compressed_segments_record_batch_with_time(1000, 0.0);
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager
            .insert_compressed_segments(TABLE_NAME, segments)
            .unwrap();
        data_manager.save_compressed_data(TABLE_NAME).unwrap();

        // This second inserted segment should be saved when the compressed files are retrieved.
        let segment_2 = test_util::compressed_segments_record_batch_with_time(2000, 0.0);
        data_manager
            .insert_compressed_segments(TABLE_NAME, segment_2.clone())
            .unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .save_and_get_saved_compressed_files(TABLE_NAME, None, None, None, None, &object_store)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_get_no_saved_compressed_files_from_non_existent_table() {
        let segments = test_util::compressed_segments_record_batch();
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager
            .insert_compressed_segments(TABLE_NAME, segments)
            .unwrap();
        data_manager.save_compressed_data(TABLE_NAME).unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .save_and_get_saved_compressed_files("NO_TABLE", None, None, None, None, &object_store)
            .await;
        assert!(result.unwrap().is_empty());
    }

    // Tests for is_compressed_file_within_time_and_value_range().
    #[test]
    fn test_compressed_file_ends_within_time_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "1_10_0.0_-10.0.parquet",
            5,
            15,
            -10.0,
            0.0,
        ))
    }

    #[test]
    fn test_compressed_file_starts_within_time_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "10_20_0_-10.0.parquet",
            5,
            15,
            -10.0,
            0.0,
        ))
    }

    #[test]
    fn test_compressed_file_is_within_time_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "10_20_0.0_-10.0.parquet",
            1,
            30,
            -10.0,
            0.0,
        ))
    }

    #[test]
    fn test_compressed_file_is_before_time_range() {
        assert!(!is_compressed_file_within_time_and_value_range(
            "1_10_0.0_-10.0.parquet",
            20,
            30,
            -10.0,
            0.0,
        ))
    }

    #[test]
    fn test_compressed_file_is_after_time_range() {
        assert!(!is_compressed_file_within_time_and_value_range(
            "20_30_0.0_-10.0.parquet",
            1,
            10,
            -10.0,
            0.0,
        ))
    }

    #[test]
    fn test_compressed_file_max_within_value_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "1_10_5.0_10.0.parquet",
            1,
            10,
            7.5,
            15.0,
        ))
    }

    #[test]
    fn test_compressed_file_min_within_value_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "1_10_5.0_10.0.parquet",
            1,
            10,
            0.0,
            7.5,
        ))
    }

    #[test]
    fn test_compressed_file_is_within_value_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "1_10_5.0_7.5.parquet",
            1,
            10,
            0.0,
            10.0,
        ))
    }

    #[test]
    fn test_compressed_file_is_less_than_value_range() {
        assert!(!is_compressed_file_within_time_and_value_range(
            "1_10_0.0_10.0.parquet",
            1,
            10,
            20.0,
            30.0,
        ))
    }

    #[test]
    fn test_compressed_file_is_greater_than_value_range() {
        assert!(!is_compressed_file_within_time_and_value_range(
            "1_10_20.0_30.0.parquet",
            1,
            10,
            0.0,
            10.0,
        ))
    }
}
