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
use crate::storage::data_transfer::DataTransfer;
use crate::storage::time_series::CompressedTimeSeries;
use crate::types::{CompressedSchema, Timestamp};
use crate::StorageEngine;

/// Stores data points compressed as models in memory to batch compressed data before saving it to
/// Apache Parquet files.
pub(super) struct CompressedDataManager {
    /// Component that transfers saved compressed data to the remote data folder when it is necessary.
    pub(super) data_transfer: Option<DataTransfer>,
    /// Path to the folder containing all compressed data managed by the [`StorageEngine`].
    data_folder_path: PathBuf,
    /// The compressed segments before they are saved to persistent storage.
    compressed_data: HashMap<u64, CompressedTimeSeries>,
    /// FIFO queue of keys referring to [`CompressedTimeSeries`] that can be saved to persistent storage.
    compressed_queue: VecDeque<u64>,
    /// How many bytes of memory that are left for storing compressed segments.
    /// Signed integer since compressed data is inserted first and the remaining bytes are checked after.
    /// This means that the remaining bytes can be negative briefly until compressed data is saved to disk.
    compressed_remaining_memory_in_bytes: isize,
    /// Reference to the schema for compressed segments.
    compressed_schema: CompressedSchema,
}

impl CompressedDataManager {
    pub(super) fn new(
        data_transfer: Option<DataTransfer>,
        data_folder_path: PathBuf,
        compressed_reserved_memory_in_bytes: usize,
        compressed_schema: CompressedSchema,
    ) -> Self {
        Self {
            data_transfer,
            data_folder_path,
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            compressed_data: HashMap::new(),
            compressed_queue: VecDeque::new(),
            compressed_remaining_memory_in_bytes: compressed_reserved_memory_in_bytes as isize,
            compressed_schema,
        }
    }

    /// Insert `segment` into the in-memory compressed time series buffer.
    pub(super) fn insert_compressed_segment(&mut self, key: u64, segment: RecordBatch) {
        let _span = debug_span!("insert_compressed_segment", key = key.clone()).entered();
        debug!(
            "Inserting batch with {} rows into compressed time series.",
            segment.num_rows()
        );

        // Since the compressed segment is already in memory, insert the segment into the structure
        // first and check if the reserved memory limit is exceeded after.
        let segment_size = if let Some(time_series) = self.compressed_data.get_mut(&key) {
            debug!("Found existing compressed time series.");

            time_series.append_segment(segment)
        } else {
            debug!("Could not find compressed time series. Creating compressed time series.");

            let mut time_series = CompressedTimeSeries::new();
            let segment_size = time_series.append_segment(segment);

            self.compressed_data.insert(key.clone(), time_series);
            self.compressed_queue.push_back(key.clone());

            segment_size
        };

        self.compressed_remaining_memory_in_bytes -= segment_size as isize;

        // If the reserved memory limit is exceeded, save compressed data to disk.
        if self.compressed_remaining_memory_in_bytes < 0 {
            self.save_compressed_data_to_free_memory();
        }
    }

    /// Return an [`ObjectMeta`] for each compressed file in `query_data_folder`
    /// that corresponds to `key`. If some compressed data that corresponds to
    /// `key` is still in memory, save the data to disk first. If `key` does not
    /// correspond to any compressed files, an empty [`Vec`] is returned, while
    /// a [`DataRetrievalError`](ModelarDbError::DataRetrievalError) is returned
    /// if the compressed files could not be listed.
    pub(super) async fn save_and_get_saved_compressed_files(
        &mut self,
        key: &u64,
        query_data_folder: &Arc<dyn ObjectStore>,
    ) -> Result<Vec<(String, ObjectMeta)>, ModelarDbError> {
        // If there is any compressed data in memory, save it first.
        if self.compressed_data.contains_key(key) {
            // Remove the data from the queue of compressed time series that are ready to be saved.
            // unwrap() is safe since `compressed_data` contains the same keys as `compressed_queue`.
            let data_index = self.compressed_queue.iter().position(|x| x == key).unwrap();
            self.compressed_queue.remove(data_index);

            self.save_compressed_data(key);
        }

        // List all files in query_data_folder for key.
        let key_path = ObjectStorePath::from(format!("{}/compressed/", key));
        let key_files = query_data_folder
            .list(Some(&key_path))
            .await
            .map_err(|error| {
                ModelarDbError::DataRetrievalError(format!(
                    "Compressed data could not be listed for key '{}': {}",
                    key,
                    error.to_string()
                ))
            })?;

        // Return all Apache Parquet files stored for key.
        let key_files = key_files
            .filter_map(|maybe_meta| async {
                if let Ok(meta) = maybe_meta {
                    if StorageEngine::is_path_an_apache_parquet_file(
                        &query_data_folder,
                        &meta.location,
                    )
                    .await
                    {
                        return Some((key.to_string(), meta));
                    };
                };
                None
            })
            .collect::<Vec<(String, ObjectMeta)>>()
            .await;

        Ok(key_files)
    }

    /// Save [`CompressedTimeSeries`] to disk until the reserved memory limit is no longer exceeded.
    fn save_compressed_data_to_free_memory(&mut self) {
        debug!("Out of memory for compressed data. Saving compressed data to disk.");

        while self.compressed_remaining_memory_in_bytes < 0 {
            let key = self.compressed_queue.pop_front().unwrap();
            self.save_compressed_data(&key);
        }
    }

    /// Flush the data that the [`CompressedDataManager`] is currently managing.
    pub(super) fn flush(&mut self) {
        info!(
            "Flushing the remaining {} batches of compressed segments.",
            self.compressed_queue.len()
        );

        while !self.compressed_queue.is_empty() {
            let key = self.compressed_queue.pop_front().unwrap();
            self.save_compressed_data(&key);
        }
    }

    /// Save the compressed data corresponding to `key` to disk. The size of the saved compressed
    /// data is added back to the remaining reserved memory. If the data is saved successfully,
    /// return [`Ok`], otherwise return [`IOError`].
    fn save_compressed_data(&mut self, key: &u64) -> Result<(), IOError> {
        debug!("Saving compressed time series to disk.");

        let mut time_series = self.compressed_data.remove(&key).unwrap();
        let folder_path = self.data_folder_path.join(key.to_string());

        time_series.save_to_apache_parquet(folder_path.as_path(), &self.compressed_schema)?;
        self.compressed_remaining_memory_in_bytes += time_series.size_in_bytes as isize;

        debug!(
            "Saved {} bytes of compressed data to disk. Remaining reserved bytes: {}.",
            time_series.size_in_bytes, self.compressed_remaining_memory_in_bytes
        );

        Ok(())
    }
}

/// Return [`true`] if `file name` contains timestamps that overlaps with the
/// time range given by `start_time` and `end_time`, otherwise [`false`].
/// Assumes `file_name` has the following format:
/// `start_timestamp-end_timestamp`, where both `start_timestamp` and
/// `end_timestamp` are of the same unit as `start_time` and `end_time`.
pub(super) fn is_compressed_file_within_time_range(
    file_name: &str,
    start_time: Timestamp,
    end_time: Timestamp,
) -> bool {
    let hyphen_index = file_name.find('-').unwrap();
    let dot_index = file_name.find('.').unwrap();

    // unwrap() is safe to use since the file name structure is internally generated.
    let file_start_time = file_name[0..hyphen_index].parse::<i64>().unwrap();
    let file_end_time = file_name[hyphen_index + 1..dot_index]
        .parse::<i64>()
        .unwrap();

    let ends_after_start = file_end_time >= start_time;
    let starts_before_end = file_start_time <= end_time;

    // Return true if the file ends after the start time and starts before the end time.
    ends_after_start && starts_before_end
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

    const KEY: u64 = 1;

    #[test]
    fn test_can_insert_compressed_segment_into_new_time_series() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(KEY, segment);

        assert!(data_manager.compressed_data.contains_key(&KEY));
        assert_eq!(data_manager.compressed_queue.pop_front().unwrap(), KEY);
        assert!(
            data_manager
                .compressed_data
                .get(&KEY)
                .unwrap()
                .size_in_bytes
                > 0
        );
    }

    #[test]
    fn test_can_insert_compressed_segment_into_existing_time_series() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(KEY, segment.clone());
        let previous_size = data_manager
            .compressed_data
            .get(&KEY)
            .unwrap()
            .size_in_bytes;

        data_manager.insert_compressed_segment(KEY, segment);

        assert!(
            data_manager
                .compressed_data
                .get(&KEY)
                .unwrap()
                .size_in_bytes
                > previous_size
        );
    }

    #[test]
    fn test_save_first_compressed_time_series_if_out_of_memory() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();
        let reserved_memory = data_manager.compressed_remaining_memory_in_bytes as usize;

        // Insert compressed data into the storage engine until data is saved to Apache Parquet.
        let max_compressed_segments = reserved_memory / test_util::COMPRESSED_SEGMENT_SIZE;
        for _ in 0..max_compressed_segments + 1 {
            data_manager.insert_compressed_segment(KEY, segment.clone());
        }

        // The compressed data should be saved to the "compressed" folder under the key.
        let data_folder_path = Path::new(&data_manager.data_folder_path);
        let compressed_path = data_folder_path.join(format!("{}/compressed", KEY));
        assert_eq!(compressed_path.read_dir().unwrap().count(), 1);
    }

    #[test]
    fn test_remaining_bytes_decremented_when_inserting_compressed_segment() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();
        let reserved_memory = data_manager.compressed_remaining_memory_in_bytes;

        data_manager.insert_compressed_segment(KEY, segment);

        assert!(reserved_memory > data_manager.compressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_incremented_when_saving_compressed_time_series() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(KEY, segment.clone());

        // Set the remaining memory to a negative value since data is only saved when out of memory.
        data_manager.compressed_remaining_memory_in_bytes = -1;
        data_manager.save_compressed_data_to_free_memory();

        assert!(-1 < data_manager.compressed_remaining_memory_in_bytes);
    }

    /// Create a [`CompressedDataManager`] with a folder that is deleted once the test is finished.
    fn create_compressed_data_manager() -> (TempDir, CompressedDataManager) {
        let temp_dir = tempdir().unwrap();
        let metadata_manager = metadata_test_util::get_test_metadata_manager(temp_dir.path());

        let data_folder_path = temp_dir.path().to_path_buf();
        (
            temp_dir,
            CompressedDataManager::new(
                None,
                data_folder_path,
                metadata_manager.compressed_reserved_memory_in_bytes,
                metadata_manager.get_compressed_schema(),
            ),
        )
    }

    // Tests for get_saved_compressed_files().
    #[tokio::test]
    async fn test_can_get_saved_compressed_files() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(KEY, segment.clone());
        data_manager.save_compressed_data(&KEY);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .save_and_get_saved_compressed_files(&KEY, &object_store)
            .await;
        assert!(result.is_ok());

        let files = result.unwrap();
        assert_eq!(files.len(), 1);

        // The file should have the first start time and the last end time as the file name.
        let file_name = storage::create_time_range_file_name(&segment);
        let expected_file_path = format!("{}/compressed/{}", KEY, file_name);

        assert_eq!(
            files.get(0).unwrap().1.location,
            ObjectStorePath::parse(expected_file_path).unwrap()
        )
    }

    #[tokio::test]
    async fn test_save_in_memory_compressed_data_when_getting_saved_compressed_files() {
        let segment = test_util::get_compressed_segment_record_batch_with_time(1000);
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(KEY, segment);
        data_manager.save_compressed_data(&KEY);

        // This second inserted segment should be saved when the compressed files are retrieved.
        let segment_2 = test_util::get_compressed_segment_record_batch_with_time(2000);
        data_manager.insert_compressed_segment(KEY, segment_2.clone());

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .save_and_get_saved_compressed_files(&KEY, &object_store)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_get_no_saved_compressed_files_from_non_existent_key() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(KEY, segment);
        data_manager.save_compressed_data(&KEY);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .save_and_get_saved_compressed_files(&999, &object_store)
            .await;
        assert!(result.unwrap().is_empty());
    }

    // Tests for is_compressed_file_within_time_range().
    #[test]
    fn test_compressed_file_ends_within_time_range() {
        assert!(is_compressed_file_within_time_range("1-10.parquet", 5, 15))
    }

    #[test]
    fn test_compressed_file_starts_within_time_range() {
        assert!(is_compressed_file_within_time_range("10-20.parquet", 5, 15))
    }

    #[test]
    fn test_compressed_file_is_within_time_range() {
        assert!(is_compressed_file_within_time_range("10-20.parquet", 1, 30))
    }

    #[test]
    fn test_compressed_file_is_before_time_range() {
        assert!(!is_compressed_file_within_time_range(
            "1-10.parquet",
            20,
            30,
        ))
    }

    #[test]
    fn test_compressed_file_is_after_time_range() {
        assert!(!is_compressed_file_within_time_range(
            "20-30.parquet",
            1,
            10,
        ))
    }
}
