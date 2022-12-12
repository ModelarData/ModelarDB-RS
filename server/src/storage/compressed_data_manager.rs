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
use std::io::Error as IOError;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use tracing::info;
use tracing::{debug, debug_span};

use crate::errors::ModelarDbError;
use crate::storage::compressed_data_buffer::CompressedDataBuffer;
use crate::storage::data_transfer::DataTransfer;
use crate::types::{CompressedSchema, Timestamp, Value};
use crate::StorageEngine;

/// Stores data points compressed as models in memory to batch compressed data before saving it to
/// Apache Parquet files.
pub(super) struct CompressedDataManager {
    /// Component that transfers saved compressed data to the remote data folder when it is necessary.
    pub(super) data_transfer: Option<DataTransfer>,
    /// Path to the folder containing all compressed data managed by the [`StorageEngine`].
    local_data_folder: PathBuf,
    /// The compressed segments before they are saved to persistent storage.
    compressed_data: HashMap<String, CompressedDataBuffer>,
    /// FIFO queue of table names referring to [`CompressedDataBuffer`] that can be saved to
    /// persistent storage.
    compressed_queue: VecDeque<String>,
    /// How many bytes of memory that are left for storing compressed segments. A signed integer is
    /// used since compressed data is inserted and then the remaining bytes are checked. This means
    /// that the remaining bytes can briefly be negative until compressed data is saved to disk.
    compressed_remaining_memory_in_bytes: isize,
    /// Reference to the schema for record batches containing compressed segments.
    compressed_schema: CompressedSchema,
}

impl CompressedDataManager {
    pub(super) fn new(
        data_transfer: Option<DataTransfer>,
        local_data_folder: PathBuf,
        compressed_reserved_memory_in_bytes: usize,
        compressed_schema: CompressedSchema,
    ) -> Self {
        Self {
            data_transfer,
            local_data_folder,
            compressed_data: HashMap::new(),
            compressed_queue: VecDeque::new(),
            compressed_remaining_memory_in_bytes: compressed_reserved_memory_in_bytes as isize,
            compressed_schema,
        }
    }

    /// Insert the `compressed_segments` into the in-memory compressed data buffer for the table
    /// with `table_name`. If `compressed_segments` is saved successfully, return [`Ok`], otherwise
    /// return [`IOError`].
    pub(super) fn insert_compressed_segment(
        &mut self,
        table_name: &str,
        compressed_segments: RecordBatch,
    ) -> Result<(), IOError> {
        let _span = debug_span!(
            "insert_compressed_segment",
            table_name = table_name.to_owned()
        )
        .entered();

        debug!(
            "Inserting batch with {} rows into compressed data buffer.",
            compressed_segments.num_rows()
        );

        // Since the compressed segments are already in memory, insert the segments into the
        // compressed data buffer first and check if the reserved memory limit is exceeded after.
        let segments_size =
            if let Some(compressed_data_buffer) = self.compressed_data.get_mut(table_name) {
                debug!("Found existing compressed data buffer.");

                compressed_data_buffer.append_compressed_segments(compressed_segments)
            } else {
                debug!("Could not find compressed data buffer. Creating compressed data buffer.");

                let mut time_series = CompressedDataBuffer::new();
                let segment_size = time_series.append_compressed_segments(compressed_segments);

                self.compressed_data
                    .insert(table_name.to_owned(), time_series);
                self.compressed_queue.push_back(table_name.to_owned());

                segment_size
            };

        self.compressed_remaining_memory_in_bytes -= segments_size as isize;

        // If the reserved memory limit is exceeded, save compressed data to disk.
        if self.compressed_remaining_memory_in_bytes < 0 {
            self.save_compressed_data_to_free_memory()?;
        }

        Ok(())
    }

    /// Return an [`ObjectMeta`] for each compressed file in `query_data_folder` that belongs to the
    /// table with `table_name`. If some compressed data that belongs to `table_name` is still in
    /// memory, save it to disk first. If no files belongs to the table with `table_name` an empty
    /// [`Vec`] is returned, while a [`DataRetrievalError`](ModelarDbError::DataRetrievalError) is
    /// returned if the compressed files could not be listed.
    pub(super) async fn save_and_get_saved_compressed_files(
        &mut self,
        table_name: &str,
        query_data_folder: &Arc<dyn ObjectStore>,
    ) -> Result<Vec<ObjectMeta>, ModelarDbError> {
        // If there is any compressed data in memory, save it first.
        if self.compressed_data.contains_key(table_name) {
            // Remove the data from the queue of compressed time series that are ready to be saved.
            // unwrap() is safe since `compressed_data` has the same tables as `compressed_queue`.
            let data_index = self
                .compressed_queue
                .iter()
                .position(|x| *x == table_name)
                .unwrap();
            self.compressed_queue.remove(data_index);

            self.save_compressed_data(table_name)
                .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?;
        }

        // List all files in query_data_folder for the table with table_name.
        let table_path = ObjectStorePath::from(format!("compressed/{}/", table_name));
        let table_files = query_data_folder
            .list(Some(&table_path))
            .await
            .map_err(|error| {
                ModelarDbError::DataRetrievalError(format!(
                    "Compressed data could not be listed for table '{}': {}",
                    table_name, error
                ))
            })?;

        // Return all Apache Parquet files stored for the table with table_name.
        let table_files = table_files
            .filter_map(|maybe_meta| async {
                if let Ok(meta) = maybe_meta {
                    if StorageEngine::is_path_an_apache_parquet_file(
                        query_data_folder,
                        &meta.location,
                    )
                    .await
                    {
                        return Some(meta);
                    };
                };
                None
            })
            .collect::<Vec<ObjectMeta>>()
            .await;

        Ok(table_files)
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
            "Flushing the remaining {} batches of compressed data buffer.",
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

        let mut compressed_data_buffer = self.compressed_data.remove(table_name).unwrap();
        let folder_path = self.local_data_folder.join("compressed").join(table_name);

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

/// Return [`true`] if the timestamps and values in `file name` overlaps with the time range given
/// by `start_time` and `end_time` and the value range given by `min_value` and `max_value`,
/// otherwise [`false`]. Assumes `file_name` has the following format:
/// `start_timestamp-end_timestamp-min_value-max_value.parquet`, where both `start_timestamp` and
/// `end_timestamp` are of the same unit as `start_time` and `end_time`.
pub(super) fn is_compressed_file_within_time_and_value_range(
    file_name: &str,
    start_time: Timestamp,
    end_time: Timestamp,
    min_value: Value,
    max_value: Value,
) -> bool {
    // unwrap() is safe to use since the file name structure is generated by the storage engine.
    let dot_index = file_name.rfind('.').unwrap();
    let mut split = file_name[0..dot_index].split('-');

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

    use object_store::local::LocalFileSystem;
    use tempfile::{tempdir, TempDir};

    use crate::metadata::test_util as metadata_test_util;
    use crate::storage;
    use crate::storage::test_util;

    const TABLE_NAME: &str = "table";

    #[test]
    fn test_can_insert_compressed_segment_into_new_time_series() {
        let segment = test_util::get_compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(TABLE_NAME, segment);

        assert!(data_manager.compressed_data.contains_key(TABLE_NAME));
        assert_eq!(
            data_manager.compressed_queue.pop_front().unwrap(),
            TABLE_NAME
        );
        assert!(
            data_manager
                .compressed_data
                .get(TABLE_NAME)
                .unwrap()
                .size_in_bytes
                > 0
        );
    }

    #[test]
    fn test_can_insert_compressed_segment_into_existing_time_series() {
        let segment = test_util::get_compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(TABLE_NAME, segment.clone());
        let previous_size = data_manager
            .compressed_data
            .get(TABLE_NAME)
            .unwrap()
            .size_in_bytes;

        data_manager.insert_compressed_segment(TABLE_NAME, segment);

        assert!(
            data_manager
                .compressed_data
                .get(TABLE_NAME)
                .unwrap()
                .size_in_bytes
                > previous_size
        );
    }

    #[test]
    fn test_save_first_compressed_time_series_if_out_of_memory() {
        let segment = test_util::get_compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();
        let reserved_memory = data_manager.compressed_remaining_memory_in_bytes as usize;

        // Insert compressed data into the storage engine until data is saved to Apache Parquet.
        let max_compressed_segments = reserved_memory / test_util::COMPRESSED_SEGMENTS_SIZE;
        for _ in 0..max_compressed_segments + 1 {
            data_manager.insert_compressed_segment(TABLE_NAME, segment.clone());
        }

        // The compressed data should be saved to the table_name folder in the compressed folder.
        let local_data_folder = Path::new(&data_manager.local_data_folder);
        let compressed_path = local_data_folder.join(format!("compressed/{}", TABLE_NAME));
        assert_eq!(compressed_path.read_dir().unwrap().count(), 1);
    }

    #[test]
    fn test_remaining_bytes_decremented_when_inserting_compressed_segments() {
        let segment = test_util::get_compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();
        let reserved_memory = data_manager.compressed_remaining_memory_in_bytes;

        data_manager.insert_compressed_segment(TABLE_NAME, segment);

        assert!(reserved_memory > data_manager.compressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_incremented_when_saving_compressed_segments() {
        let segment = test_util::get_compressed_segments_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(TABLE_NAME, segment.clone());

        // Set the remaining memory to a negative value since data is only saved when out of memory.
        data_manager.compressed_remaining_memory_in_bytes = -1;
        data_manager.save_compressed_data_to_free_memory();

        assert!(-1 < data_manager.compressed_remaining_memory_in_bytes);
    }

    /// Create a [`CompressedDataManager`] with a folder that is deleted once the test is finished.
    fn create_compressed_data_manager() -> (TempDir, CompressedDataManager) {
        let temp_dir = tempdir().unwrap();
        let metadata_manager = metadata_test_util::get_test_metadata_manager(temp_dir.path());

        let local_data_folder = temp_dir.path().to_path_buf();
        (
            temp_dir,
            CompressedDataManager::new(
                None,
                local_data_folder,
                metadata_manager.compressed_reserved_memory_in_bytes,
                metadata_manager.get_compressed_schema(),
            ),
        )
    }

    // Tests for get_saved_compressed_files().
    #[tokio::test]
    async fn test_can_get_saved_compressed_files() {
        let segment = test_util::get_compressed_segments_record_batch();
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(TABLE_NAME, segment.clone());
        data_manager.save_compressed_data(TABLE_NAME).unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .save_and_get_saved_compressed_files(TABLE_NAME, &object_store)
            .await;
        assert!(result.is_ok());

        let files = result.unwrap();
        assert_eq!(files.len(), 1);

        // The file should have the first start time and the last end time as the file name.
        let file_name = storage::create_time_and_value_range_file_name(&segment);
        let expected_file_path = format!("compressed/{}/{}", TABLE_NAME, file_name);

        assert_eq!(
            files.get(0).unwrap().location,
            ObjectStorePath::parse(expected_file_path).unwrap()
        )
    }

    #[tokio::test]
    async fn test_save_in_memory_compressed_data_when_getting_saved_compressed_files() {
        let segment = test_util::get_compressed_segments_record_batch_with_time(1000);
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(TABLE_NAME, segment);
        data_manager.save_compressed_data(TABLE_NAME).unwrap();

        // This second inserted segment should be saved when the compressed files are retrieved.
        let segment_2 = test_util::get_compressed_segments_record_batch_with_time(2000);
        data_manager.insert_compressed_segment(TABLE_NAME, segment_2.clone());

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .save_and_get_saved_compressed_files(TABLE_NAME, &object_store)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_get_no_saved_compressed_files_from_non_existent_table() {
        let segment = test_util::get_compressed_segments_record_batch();
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(TABLE_NAME, segment);
        data_manager.save_compressed_data(TABLE_NAME).unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .save_and_get_saved_compressed_files("NO_TABLE", &object_store)
            .await;
        assert!(result.unwrap().is_empty());
    }

    // Tests for is_compressed_file_within_time_and_value_range().
    #[test]
    fn test_compressed_file_ends_within_time_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "1-10-0.0-10.0.parquet",
            5,
            15,
            0.0,
            10.0
        ))
    }

    #[test]
    fn test_compressed_file_starts_within_time_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "10-20-0-10.0.parquet",
            5,
            15,
            0.0,
            10.0
        ))
    }

    #[test]
    fn test_compressed_file_is_within_time_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "10-20-0.0-10.0.parquet",
            1,
            30,
            0.0,
            10.0
        ))
    }

    #[test]
    fn test_compressed_file_is_before_time_range() {
        assert!(!is_compressed_file_within_time_and_value_range(
            "1-10-0.0-10.0.parquet",
            20,
            30,
            0.0,
            10.0
        ))
    }

    #[test]
    fn test_compressed_file_is_after_time_range() {
        assert!(!is_compressed_file_within_time_and_value_range(
            "20-30-0.0-10.0.parquet",
            1,
            10,
            0.0,
            10.0
        ))
    }

    #[test]
    fn test_compressed_file_max_within_value_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "1-10-5.0-10.0.parquet",
            1,
            10,
            7.5,
            15.0
        ))
    }

    #[test]
    fn test_compressed_file_min_within_value_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "1-10-5.0-10.0.parquet",
            1,
            10,
            0.0,
            7.5
        ))
    }

    #[test]
    fn test_compressed_file_is_within_value_range() {
        assert!(is_compressed_file_within_time_and_value_range(
            "1-10-5.0-7.5.parquet",
            1,
            10,
            0.0,
            10.0
        ))
    }

    #[test]
    fn test_compressed_file_is_less_than_value_range() {
        assert!(!is_compressed_file_within_time_and_value_range(
            "1-10-0.0-10.0.parquet",
            1,
            10,
            20.0,
            30.0
        ))
    }

    #[test]
    fn test_compressed_file_is_greater_than_value_range() {
        assert!(!is_compressed_file_within_time_and_value_range(
            "1-10-20.0-30.0.parquet",
            1,
            10,
            0.0,
            10.0
        ))
    }
}
