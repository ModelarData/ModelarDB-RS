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

use std::fs;
use std::io::ErrorKind::Other;
use std::io::{Error as IOError, ErrorKind};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::Receiver;
use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use futures::StreamExt;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::types::{Timestamp, Value};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::storage::compressed_data_buffer::CompressedDataBuffer;
use crate::storage::data_transfer::DataTransfer;
use crate::storage::types::MemoryPool;
use crate::storage::{Metric, StorageEngine, COMPRESSED_DATA_FOLDER};

use super::compressed_data_buffer::CompressedSegmentBatch;

/// Stores data points compressed as models in memory to batch compressed data before saving it to
/// Apache Parquet files.
pub(super) struct CompressedDataManager {
    /// Component that transfers saved compressed data to the remote data folder when it is necessary.
    pub(super) data_transfer: RwLock<Option<DataTransfer>>,
    /// Path to the folder containing all compressed data managed by the [`StorageEngine`].
    pub(crate) local_data_folder: PathBuf,
    /// The compressed segments before they are saved to persistent storage. The key is the name of
    /// the table and the index of the column the compressed segments represents data points for so
    /// the Apache Parquet files can be partitioned by table and then column.
    compressed_data_buffers: DashMap<(String, u16), CompressedDataBuffer>,
    /// FIFO queue of table names and column indices referring to [`CompressedDataBuffer`] that can
    /// be saved to persistent storage.
    compressed_queue: SegQueue<(String, u16)>,
    /// Track how much memory is left for storing uncompressed and compressed data.
    memory_pool: Arc<MemoryPool>,
    /// Metric for the used compressed memory in bytes, updated every time the used memory changes.
    pub(super) used_compressed_memory_metric: Mutex<Metric>,
    /// Metric for the total used disk space in bytes, updated every time a new compressed file is
    /// saved to disk.
    pub used_disk_space_metric: Arc<Mutex<Metric>>,
}

impl CompressedDataManager {
    /// Return a [`CompressedDataManager`] if the required folder can be created in
    /// `local_data_folder`, otherwise [`IOError`] is returned.
    pub(super) fn try_new(
        data_transfer: RwLock<Option<DataTransfer>>,
        local_data_folder: PathBuf,
        memory_pool: Arc<MemoryPool>,
        used_disk_space_metric: Arc<Mutex<Metric>>,
    ) -> Result<Self, IOError> {
        // Ensure the folder required by the compressed data manager exists.
        fs::create_dir_all(local_data_folder.join(COMPRESSED_DATA_FOLDER))?;

        Ok(Self {
            data_transfer,
            local_data_folder,
            compressed_data_buffers: DashMap::new(),
            compressed_queue: SegQueue::new(),
            memory_pool,
            used_compressed_memory_metric: Mutex::new(Metric::new()),
            used_disk_space_metric,
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

        StorageEngine::write_batch_to_apache_parquet_file(record_batch, file_path.as_path(), None)
    }

    /// Insert the `compressed_segments` into the in-memory compressed data buffer for the table
    /// with `table_name` and the column at `column_index`. If `compressed_segments` is saved
    /// successfully, return [`Ok`], otherwise return [`IOError`].
    pub(super) async fn insert_compressed_segments(
        &self,
        compressed_data_receiver: &Receiver<CompressedSegmentBatch>,
    ) -> Result<(), IOError> {
        let compressed_segment_batch = compressed_data_receiver
            .recv()
            .map_err(|error| IOError::new(ErrorKind::BrokenPipe, error))?;

        let column_index = compressed_segment_batch.column_index();
        let model_table_name = compressed_segment_batch.model_table_name().to_owned();

        debug!(
            "Inserting batch with {} rows into compressed data buffer for column '{}' in table '{}'.",
            compressed_segment_batch.compressed_segments.num_rows(),
            column_index,
            model_table_name
        );

        // Since the compressed segments are already in memory, insert the segments into the
        // compressed data buffer first and check if the reserved memory limit is exceeded after.
        let segments_size = if let Some(mut compressed_data_buffer) = self
            .compressed_data_buffers
            .get_mut(&(model_table_name.clone(), column_index))
        {
            debug!(
                "Found existing compressed data buffer for column '{}' in table '{}'.",
                column_index, model_table_name
            );

            compressed_data_buffer
                .append_compressed_segments(compressed_segment_batch.compressed_segments)
        } else {
            debug!(
                "Could not find compressed data buffer for column '{}' in table '{}'. Creating compressed data buffer.",
                column_index,
                model_table_name
            );

            let mut compressed_data_buffer = CompressedDataBuffer::new(
                compressed_segment_batch.univariate_id,
                compressed_segment_batch.model_table_metadata,
            );
            let segment_size = compressed_data_buffer
                .append_compressed_segments(compressed_segment_batch.compressed_segments);

            let key = (model_table_name.to_owned(), column_index);
            self.compressed_data_buffers
                .insert(key.clone(), compressed_data_buffer);
            self.compressed_queue.push(key);

            segment_size
        };

        // Update the remaining memory for compressed data and record the change.
        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_compressed_memory_metric
            .lock()
            .unwrap()
            .append(segments_size as isize, true);

        // If the reserved memory limit is exceeded, save compressed data to disk.
        while !self
            .memory_pool
            .try_reserve_compressed_memory(segments_size)
        {
            self.save_compressed_data_to_free_memory(segments_size)
                .await?;
        }

        Ok(())
    }

    /// Return an [`ObjectMeta`] for each compressed file in `query_data_folder` that belongs to the
    /// column at `column_index` in the table with `table_name` and contains compressed segments
    /// within the given range of time and value. If no files belong to the column at `column_index`
    /// for the table with `table_name` an empty [`Vec`] is returned, while a
    /// [`DataRetrievalError`](ModelarDbError::DataRetrievalError) is returned if:
    /// * A table with `table_name` does not exist.
    /// * The compressed files could not be listed.
    /// * A column with `column_index` does not exist.
    /// * The end time is before the start time.
    /// * The max value is smaller than the min value.
    pub(super) async fn get_saved_compressed_files(
        &self,
        table_name: &str,
        column_index: u16,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        min_value: Option<Value>,
        max_value: Option<Value>,
        query_data_folder: &Arc<dyn ObjectStore>,
    ) -> Result<Vec<ObjectMeta>, ModelarDbError> {
        // Set default values for the parts of the time and value range that is not defined.
        let start_time = start_time.unwrap_or(0);
        let end_time = end_time.unwrap_or(Timestamp::MAX);

        if start_time > end_time {
            return Err(ModelarDbError::DataRetrievalError(format!(
                "Start time '{start_time}' cannot be after end time '{end_time}'."
            )));
        };

        let min_value = min_value.unwrap_or(Value::NEG_INFINITY);
        let max_value = max_value.unwrap_or(Value::INFINITY);

        if min_value > max_value {
            return Err(ModelarDbError::DataRetrievalError(format!(
                "Min value '{min_value}' cannot be larger than max value '{max_value}'."
            )));
        };

        // List all files in query_data_folder for the table named table_name.
        let table_path = ObjectStorePath::from(format!(
            "{COMPRESSED_DATA_FOLDER}/{table_name}/{column_index}"
        ));
        let table_files = query_data_folder
            .list(Some(&table_path))
            .await
            .map_err(|error| {
                ModelarDbError::DataRetrievalError(format!(
                    "Compressed data could not be listed for column '{column_index}' in table '{table_name}': {error}"
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

    /// Save [`CompressedDataBuffers`](CompressedDataBuffer) to disk until at least `size_in_bytes`
    /// bytes of memory is available. If all of the data is saved successfully, return [`Ok`],
    /// otherwise return [`IOError`].
    async fn save_compressed_data_to_free_memory(
        &self,
        size_in_bytes: usize,
    ) -> Result<(), IOError> {
        debug!("Out of memory for compressed data. Saving compressed data to disk.");

        while self.memory_pool.remaining_compressed_memory_in_bytes() < size_in_bytes as isize {
            let (table_name, column_index) = self
                .compressed_queue
                .pop()
                .expect("Not enough compressed data to free up the required memory.");
            self.save_compressed_data(&table_name, column_index).await?;
        }
        Ok(())
    }

    /// Flush the data that the [`CompressedDataManager`] is currently managing.
    pub(super) async fn flush(&self) -> Result<(), IOError> {
        info!(
            "Flushing the remaining {} compressed data buffers.",
            self.compressed_queue.len()
        );

        while !self.compressed_queue.is_empty() {
            let (table_name, column_index) = self.compressed_queue.pop().unwrap();
            self.save_compressed_data(&table_name, column_index).await?;
        }

        Ok(())
    }

    /// Save the compressed data that belongs to the column at `column_index` in the table with
    /// `table_name` to disk. The size of the saved compressed data is added back to the remaining
    /// reserved memory. If the data is saved successfully, return [`Ok`], otherwise return
    /// [`IOError`].
    async fn save_compressed_data(
        &self,
        table_name: &str,
        column_index: u16,
    ) -> Result<(), IOError> {
        debug!("Saving compressed time series to disk.");

        let mut compressed_data_buffer = self
            .compressed_data_buffers
            .remove(&(table_name.to_owned(), column_index))
            .unwrap()
            .1;

        let folder_path = self
            .local_data_folder
            .join(COMPRESSED_DATA_FOLDER)
            .join(table_name)
            .join(column_index.to_string());

        let file_path = compressed_data_buffer.save_to_apache_parquet(folder_path.as_path())?;

        // Update the remaining memory for compressed data and record the change.
        let freed_memory = compressed_data_buffer.size_in_bytes;
        self.memory_pool.free_compressed_memory(freed_memory);

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_compressed_memory_metric
            .lock()
            .unwrap()
            .append(-(freed_memory as isize), true);

        // Record the change to the used disk space.
        let file_size = file_path.metadata()?.len() as isize;

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_disk_space_metric
            .lock()
            .unwrap()
            .append(file_size, true);

        debug!(
            "Saved {} bytes of compressed data to disk. Remaining reserved bytes: {}.",
            freed_memory,
            self.memory_pool.remaining_compressed_memory_in_bytes()
        );

        // Pass the saved compressed file to the data transfer component if a remote data folder
        // was provided. If the total size of the files related to table_name have reached the
        // transfer threshold, the files are transferred to the remote object store.
        if let Some(data_transfer) = &*self.data_transfer.read().await {
            data_transfer
                .add_compressed_file(table_name, column_index, file_path.as_path())
                .await
                .map_err(|error| IOError::new(Other, error.to_string()))?;
        }

        Ok(())
    }

    /// Change the amount of memory for compressed data in bytes according to `value_change`. If
    /// less than zero bytes remain, save compressed data to free memory. If all the data is saved
    /// successfully return [`Ok`], otherwise return [`IOError`].
    pub(super) async fn adjust_compressed_remaining_memory_in_bytes(
        &self,
        value_change: isize,
    ) -> Result<(), IOError> {
        self.memory_pool.adjust_compressed_memory(value_change);
        self.save_compressed_data_to_free_memory(0).await?;

        Ok(())
    }
}

/// Return the [`ObjectMeta`] passed as `object_meta` if it represents an Apache Parquet file and if
/// the timestamps and values in its filename overlap with the time range given by `start_time` and
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

    // create_time_and_value_range_file_name() considers NaN to be greater than all other non-null
    // values. Thus, file_min_value is NaN if all of the segments in the file with file_name only
    // contain NaN values while file_max_value is NaN if any of the segments contains a NaN value.
    // The total_cmp() method is used instead of the comparison operators to take NaN into account.
    let file_min_value = split.next().unwrap().parse::<Value>().unwrap();
    let file_max_value = split.next().unwrap().parse::<Value>().unwrap();
    let file_max_greater_than_min = file_max_value.total_cmp(&min_value).is_ge();
    let file_min_less_than_max = file_min_value.total_cmp(&max_value).is_le()
        || (file_min_value.is_nan() && max_value.is_infinite()); // INFINITY means no upper bound.

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
    use modelardb_common::types::{TimestampArray, ValueArray};
    use object_store::local::LocalFileSystem;
    use ringbuf::Rb;
    use tempfile::{self, TempDir};

    use crate::common_test;
    use crate::storage::{self, TEST_UUID};
    use crate::PORT;

    const TABLE_NAME: &str = "table";
    const COLUMN_INDEX: u16 = 5;

    // Tests for insert_record_batch().
    #[tokio::test]
    async fn test_insert_record_batch() {
        let record_batch = common_test::compressed_segments_record_batch();
        let (temp_dir, data_manager) = create_compressed_data_manager().await;

        let local_data_folder = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
        let table_folder =
            ObjectStorePath::parse(format!("{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}/")).unwrap();

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
    #[tokio::test]
    async fn test_can_insert_compressed_segment_into_new_compressed_data_buffer() {
        let segments = common_test::compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager().await;
        let key = (TABLE_NAME.to_owned(), COLUMN_INDEX);

        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segments)
            .await
            .unwrap();

        assert!(data_manager.compressed_data_buffers.contains_key(&key));
        assert_eq!(data_manager.compressed_queue.pop_front().unwrap(), key);
        assert!(
            data_manager
                .compressed_data_buffers
                .get(&key)
                .unwrap()
                .size_in_bytes
                > 0
        );
    }

    #[tokio::test]
    async fn test_can_insert_compressed_segment_into_existing_compressed_data_buffer() {
        let segments = common_test::compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager().await;
        let key = (TABLE_NAME.to_owned(), COLUMN_INDEX);

        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segments.clone())
            .await
            .unwrap();
        let previous_size = data_manager
            .compressed_data_buffers
            .get(&key)
            .unwrap()
            .size_in_bytes;

        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segments)
            .await
            .unwrap();

        assert!(
            data_manager
                .compressed_data_buffers
                .get(&key)
                .unwrap()
                .size_in_bytes
                > previous_size
        );
    }

    #[tokio::test]
    async fn test_save_first_compressed_data_buffer_if_out_of_memory() {
        let segments = common_test::compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager().await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_compressed_memory_in_bytes() as usize;

        // Insert compressed data into the storage engine until data is saved to Apache Parquet.
        let max_compressed_segments = reserved_memory / common_test::COMPRESSED_SEGMENTS_SIZE;
        for _ in 0..max_compressed_segments + 1 {
            data_manager
                .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segments.clone())
                .await
                .unwrap();
        }

        // The compressed data should be saved to the table_name folder in the compressed folder.
        let local_data_folder = Path::new(&data_manager.local_data_folder);
        let compressed_path =
            local_data_folder.join(format!("{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}"));
        assert_eq!(compressed_path.read_dir().unwrap().count(), 1);
    }

    #[tokio::test]
    async fn test_remaining_bytes_decremented_when_inserting_compressed_segments() {
        let segments = common_test::compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager().await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_compressed_memory_in_bytes();

        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segments)
            .await
            .unwrap();

        assert!(
            reserved_memory
                > data_manager
                    .memory_pool
                    .remaining_compressed_memory_in_bytes()
        );
        assert_eq!(data_manager.used_compressed_memory_metric.values().len(), 1);
    }

    #[tokio::test]
    async fn test_remaining_memory_incremented_when_saving_compressed_segments() {
        let segments = common_test::compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager().await;

        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segments.clone())
            .await
            .unwrap();

        // Set the remaining memory to a negative value since data is only saved when out of memory.
        data_manager.memory_pool.adjust_compressed_memory(
            -(data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes()
                + 1),
        );

        data_manager
            .save_compressed_data_to_free_memory(0)
            .await
            .unwrap();

        assert!(
            -1 < data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes()
        );
        assert_eq!(data_manager.used_compressed_memory_metric.values().len(), 2);
        assert_eq!(
            data_manager
                .used_disk_space_metric
                .read()
                .await
                .values()
                .len(),
            1
        );
    }

    // Tests for save_and_get_saved_compressed_files().
    #[tokio::test]
    async fn test_can_get_compressed_file_for_table() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager().await;

        // Insert compressed segments into the same table.
        let segment = common_test::compressed_segments_record_batch();
        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segment.clone())
            .await
            .unwrap();
        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segment)
            .await
            .unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .get_saved_compressed_files(
                TABLE_NAME,
                COLUMN_INDEX,
                None,
                None,
                None,
                None,
                &object_store,
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_get_no_compressed_files_for_non_existent_table() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager().await;

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.get_saved_compressed_files(
            TABLE_NAME,
            COLUMN_INDEX,
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
        let (temp_dir, mut data_manager) = create_compressed_data_manager().await;
        let (segment_1, _segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0).await;

        // If we have a start time after the first segments ends, only the file containing the
        // second segment should be retrieved.
        let end_times = modelardb_common::array!(segment_1, 3, TimestampArray);
        let start_time = Some(end_times.value(end_times.len() - 1) + 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.get_saved_compressed_files(
            TABLE_NAME,
            COLUMN_INDEX,
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
        assert_eq!(file_path, format_path("2000_2005_15.2_44.2"));
    }

    #[tokio::test]
    async fn test_can_get_compressed_file_with_min_value() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager().await;
        let (segment_1, _segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0).await;

        // If we have a min value higher then the max value in the first segment, only the file
        // containing the second segment should be retrieved.
        let max_values = modelardb_common::array!(segment_1, 6, ValueArray);
        let min_value = Some(compute::max(max_values).unwrap() + 1.0);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.get_saved_compressed_files(
            TABLE_NAME,
            COLUMN_INDEX,
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
        assert_eq!(file_path, format_path("2000_2005_15.2_44.2"));
    }

    #[tokio::test]
    async fn test_can_get_compressed_file_with_end_time() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager().await;
        let (_segment_1, segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0).await;

        // If we have an end time before the second segment starts, only the file containing the
        // first segment should be retrieved.
        let start_times = modelardb_common::array!(segment_2, 2, TimestampArray);
        let end_time = Some(start_times.value(1) - 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.get_saved_compressed_files(
            TABLE_NAME,
            COLUMN_INDEX,
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
        assert_eq!(file_path, format_path("1000_1005_5.2_34.2"));
    }

    #[tokio::test]
    async fn test_can_get_compressed_file_with_max_value() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager().await;
        let (_segment_1, segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0).await;

        // If we have a max value lower then the min value in the second segment, only the file
        // containing the first segment should be retrieved.
        let min_values = modelardb_common::array!(segment_2, 5, ValueArray);
        let max_value = Some(compute::min(min_values).unwrap() - 1.0);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.get_saved_compressed_files(
            TABLE_NAME,
            COLUMN_INDEX,
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
        assert_eq!(file_path, format_path("1000_1005_5.2_34.2"));
    }

    #[tokio::test]
    async fn test_can_get_compressed_files_with_start_time_and_end_time() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager().await;

        // Insert 4 segments with a ~1 second time difference between segment 1 and 2 and segment 3 and 4.
        let (segment_1, _segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0).await;
        let (_segment_3, segment_4) = insert_separated_segments(&mut data_manager, 1000, 0.0).await;

        // If we have a start time after the first segment and an end time before the fourth
        // segment, only the files containing the second and third segment should be retrieved.
        let end_times = modelardb_common::array!(segment_1, 3, TimestampArray);
        let start_times = modelardb_common::array!(segment_4, 2, TimestampArray);

        let start_time = Some(end_times.value(end_times.len() - 1) + 100);
        let end_time = Some(start_times.value(1) - 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.get_saved_compressed_files(
            TABLE_NAME,
            COLUMN_INDEX,
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
        assert_eq!(file_path, format_path("2000_2005_15.2_44.2"));
        let file_path = files.get(1).unwrap().location.to_string();
        assert_eq!(file_path, format_path("2000_2005_5.2_34.2"));
    }

    #[tokio::test]
    async fn test_can_get_compressed_files_with_min_value_and_max_value() {
        let (temp_dir, mut data_manager) = create_compressed_data_manager().await;

        // Insert 4 segments with a ~1 second time difference between segment 1 and 2 and segment 3 and 4.
        let (segment_1, _segment_2) = insert_separated_segments(&mut data_manager, 0, 0.0).await;
        let (_segment_3, segment_4) =
            insert_separated_segments(&mut data_manager, 1000, 100.0).await;

        // If we have a min value higher the first segment and a max value lower than the fourth
        // segment, only the files containing the second and third segment should be retrieved.
        let max_values = modelardb_common::array!(segment_1, 6, ValueArray);
        let min_values = modelardb_common::array!(segment_4, 5, ValueArray);

        let min_value = Some(compute::max(max_values).unwrap() + 1.0);
        let max_value = Some(compute::min(min_values).unwrap() - 1.0);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.get_saved_compressed_files(
            TABLE_NAME,
            COLUMN_INDEX,
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
        assert_eq!(file_path, format_path("2000_2005_105.2_134.2"));
        let file_path = files.get(1).unwrap().location.to_string();
        assert_eq!(file_path, format_path("2000_2005_15.2_44.2"));
    }

    /// Return a full path to the file with `file_name`.
    fn format_path(file_name: &str) -> String {
        format!(
            "{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}/{COLUMN_INDEX}/{file_name}_{TEST_UUID}_{}.parquet",
            *PORT
        )
    }

    /// Create and insert two compressed segments with a 1 second time difference offset by
    /// `start_time` and values with difference of 10 offset by `value_offset`.
    async fn insert_separated_segments(
        data_manager: &mut CompressedDataManager,
        start_time: i64,
        value_offset: f32,
    ) -> (RecordBatch, RecordBatch) {
        let segment_1 = common_test::compressed_segments_record_batch_with_time(
            1000 + start_time,
            value_offset,
        );
        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segment_1.clone())
            .await
            .unwrap();
        data_manager.flush().await.unwrap();

        let segment_2 = common_test::compressed_segments_record_batch_with_time(
            2000 + start_time,
            10.0 + value_offset,
        );
        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segment_2.clone())
            .await
            .unwrap();
        data_manager.flush().await.unwrap();

        (segment_1, segment_2)
    }

    #[tokio::test]
    async fn test_cannot_get_compressed_files_where_end_time_is_before_start_time() {
        let (_temp_dir, mut data_manager) = create_compressed_data_manager().await;

        let segment = common_test::compressed_segments_record_batch();
        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segment)
            .await
            .unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let result = data_manager.get_saved_compressed_files(
            TABLE_NAME,
            COLUMN_INDEX,
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
        let (_temp_dir, mut data_manager) = create_compressed_data_manager().await;

        let segment = common_test::compressed_segments_record_batch();
        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segment)
            .await
            .unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let result = data_manager.get_saved_compressed_files(
            TABLE_NAME,
            COLUMN_INDEX,
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
        let segments = common_test::compressed_segments_record_batch();
        let (temp_dir, mut data_manager) = create_compressed_data_manager().await;

        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segments.clone())
            .await
            .unwrap();
        data_manager
            .save_compressed_data(TABLE_NAME, COLUMN_INDEX)
            .await
            .unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .get_saved_compressed_files(
                TABLE_NAME,
                COLUMN_INDEX,
                None,
                None,
                None,
                None,
                &object_store,
            )
            .await;
        assert!(result.is_ok());

        let files = result.unwrap();
        assert_eq!(files.len(), 1);

        // The file should have the first start time and the last end time as the file name.
        let file_name = storage::create_time_and_value_range_file_name(&segments);
        let expected_file_path =
            format!("{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}/{COLUMN_INDEX}/{file_name}");

        assert_eq!(
            files.get(0).unwrap().location,
            ObjectStorePath::parse(expected_file_path).unwrap()
        )
    }

    #[tokio::test]
    async fn test_save_in_memory_compressed_data_when_getting_saved_compressed_files() {
        let segments = common_test::compressed_segments_record_batch_with_time(1000, 0.0);
        let (temp_dir, mut data_manager) = create_compressed_data_manager().await;

        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segments)
            .await
            .unwrap();
        data_manager
            .save_compressed_data(TABLE_NAME, COLUMN_INDEX)
            .await
            .unwrap();

        // This second inserted segment should be saved when the compressed files are retrieved.
        let segment_2 = common_test::compressed_segments_record_batch_with_time(2000, 0.0);
        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segment_2.clone())
            .await
            .unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .get_saved_compressed_files(
                TABLE_NAME,
                COLUMN_INDEX,
                None,
                None,
                None,
                None,
                &object_store,
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_get_no_saved_compressed_files_from_non_existent_table() {
        let segments = common_test::compressed_segments_record_batch();
        let (temp_dir, mut data_manager) = create_compressed_data_manager().await;

        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segments)
            .await
            .unwrap();
        data_manager
            .save_compressed_data(TABLE_NAME, COLUMN_INDEX)
            .await
            .unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .get_saved_compressed_files(
                "NO_TABLE",
                COLUMN_INDEX,
                None,
                None,
                None,
                None,
                &object_store,
            )
            .await;
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_increase_compressed_remaining_memory_in_bytes() {
        let (_temp_dir, mut data_manager) = create_compressed_data_manager().await;

        data_manager
            .adjust_compressed_remaining_memory_in_bytes(10000)
            .await
            .unwrap();

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes(),
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize + 10000
        )
    }

    #[tokio::test]
    async fn test_decrease_compressed_remaining_memory_in_bytes() {
        let (_temp_dir, mut data_manager) = create_compressed_data_manager().await;

        // Insert data that should be saved when the remaining memory is decreased.
        let segments = common_test::compressed_segments_record_batch();
        data_manager
            .insert_compressed_segments(TABLE_NAME, COLUMN_INDEX, segments)
            .await
            .unwrap();

        data_manager
            .adjust_compressed_remaining_memory_in_bytes(
                -(common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize),
            )
            .await
            .unwrap();

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes(),
            0
        );

        // There should no longer be any compressed data in memory.
        assert_eq!(data_manager.compressed_data_buffers.values().len(), 0);
    }

    #[tokio::test]
    #[should_panic(expected = "Not enough compressed data to free up the required memory.")]
    async fn test_panic_if_decreasing_compressed_remaining_memory_in_bytes_below_zero() {
        let (_temp_dir, mut data_manager) = create_compressed_data_manager().await;

        data_manager
            .adjust_compressed_remaining_memory_in_bytes(
                -((common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES + 1) as isize),
            )
            .await
            .unwrap();
    }

    /// Create a [`CompressedDataManager`] with a folder that is deleted once the test is finished.
    async fn create_compressed_data_manager() -> (TempDir, CompressedDataManager) {
        let temp_dir = tempfile::tempdir().unwrap();

        let local_data_folder = temp_dir.path().to_path_buf();

        let memory_pool = Arc::new(MemoryPool::new(
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        ));

        (
            temp_dir,
            CompressedDataManager::try_new(
                None,
                local_data_folder,
                memory_pool,
                Arc::new(RwLock::new(Metric::new())),
            )
            .unwrap(),
        )
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
    fn test_compressed_file_max_within_nan_value_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "1_10_5.0_10.0.parquet",
            1,
            10,
            7.5,
            f32::NAN,
        ))
    }

    #[test]
    fn test_compressed_file_max_is_nan_and_within_value_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "1_10_5.0_NaN.parquet",
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
    fn test_compressed_file_with_only_nan_is_within_nan_value_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "1_10_NaN_NaN.parquet",
            1,
            10,
            f32::NAN,
            f32::NAN,
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

    #[test]
    fn test_compressed_file_is_outside_nan_value_range() {
        assert!(!is_compressed_file_within_time_and_value_range(
            "1_10_5.0_7.5.parquet",
            1,
            10,
            Value::NAN,
            Value::NAN,
        ))
    }

    #[test]
    fn test_compressed_file_with_only_nan_is_outside_value_range() {
        assert!(!is_compressed_file_within_time_and_value_range(
            "1_10_NaN_NaN.parquet",
            1,
            10,
            0.0,
            10.0,
        ))
    }
}
