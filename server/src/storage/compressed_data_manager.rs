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
use std::path:: PathBuf;

use datafusion::arrow::record_batch::RecordBatch;
use tracing::{info, info_span};

use crate::errors::ModelarDBError;
use crate::storage::time_series::CompressedTimeSeries;
use crate::StorageEngine;

/// Signed integer since compressed data is inserted first and the remaining bytes are checked after.
/// This means that the remaining bytes can be negative briefly until compressed data is saved to disk.
const COMPRESSED_RESERVED_MEMORY_IN_BYTES: isize = 5000;

/// Stores data points compressed as models in memory to batch compressed data before saving it to
/// Apache Parquet files.
pub struct CompressedDataManager {
    /// Path to the folder containing all compressed data managed by the [`StorageEngine`].
    data_folder_path: PathBuf,
    /// The compressed segments before they are saved to persistent storage.
    compressed_data: HashMap<u64, CompressedTimeSeries>,
    /// FIFO queue of keys referring to [`CompressedTimeSeries`] that can be saved to persistent storage.
    compressed_queue: VecDeque<u64>,
    /// How many bytes of memory that are left for storing compressed segments.
    compressed_remaining_memory_in_bytes: isize,
}

impl CompressedDataManager {
    pub fn new(data_folder_path: PathBuf) -> Self {
        Self {
            data_folder_path,
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            compressed_data: HashMap::new(),
            compressed_queue: VecDeque::new(),
            compressed_remaining_memory_in_bytes: COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        }
    }

    /// Insert `segment` into the in-memory compressed time series buffer.
    pub fn insert_compressed_segment(&mut self, key: u64, segment: RecordBatch) {
        let _span = info_span!("insert_compressed_segment", key = key.clone()).entered();
        info!(
            "Inserting batch with {} rows into compressed time series.",
            segment.num_rows()
        );

        // Since the compressed segment is already in memory, insert the segment into the structure
        // first and check if the reserved memory limit is exceeded after.
        let segment_size = if let Some(time_series) = self.compressed_data.get_mut(&key) {
            info!("Found existing compressed time series.");

            time_series.append_segment(segment)
        } else {
            info!("Could not find compressed time series. Creating compressed time series.");

            let mut time_series = CompressedTimeSeries::new();
            let segment_size = time_series.append_segment(segment);

            self.compressed_data.insert(key.clone(), time_series);
            self.compressed_queue.push_back(key.clone());

            segment_size
        };

        self.compressed_remaining_memory_in_bytes -= segment_size as isize;

        // If the reserved memory limit is exceeded, save compressed data to disk.
        if self.compressed_remaining_memory_in_bytes < 0 {
            self.free_compressed_data_memory();
        }
    }

    /// Return the file path to each on-disk compressed file that corresponds to `key`. If some
    /// compressed data that corresponds to `key` is still in memory, save the data to disk first.
    /// If `key` does not correspond to any data, [`DataRetrievalError`](ModelarDBError::DataRetrievalError) is returned.
    pub fn get_saved_compressed_files(
        &mut self,
        key: &u64
    ) -> Result<Vec<PathBuf>, ModelarDBError> {
        // If there is any compressed data in memory, save it first.
        if self.compressed_data.contains_key(key) {
            // Remove the data from the queue of compressed time series that are ready to be saved.
            // unwrap() is safe since `compressed_data` contains the same keys as `compressed_queue`.
            let data_index = self.compressed_queue.iter().position(|x| x == key).unwrap();
            self.compressed_queue.remove(data_index);

            self.save_compressed_data(key);
        }

        // Read the directory that contains the compressed data corresponding to the key.
        let key_path = self.data_folder_path.join(format!("{}/compressed", key));
        let dir = fs::read_dir(key_path).map_err(
            |error| {
                ModelarDBError::DataRetrievalError(format!(
                    "Compressed data could not be found for key '{}': {}",
                    key,
                    error.to_string()
                ))
            },
        )?;

        // Return all files in the path that are parquet files.
        Ok(dir.filter_map(|maybe_dir_entry| {
            if let Ok(dir_entry) = maybe_dir_entry {
                if StorageEngine::is_path_an_apache_parquet_file(dir_entry.path().as_path()) {
                    Some(dir_entry.path())
                } else {
                    None
                }
            } else {
                None
            }
        }).collect())
    }

    /// Save [`CompressedTimeSeries`] to disk until the reserved memory limit is no longer exceeded.
    fn free_compressed_data_memory(&mut self) {
        info!("Out of memory for compressed data. Saving compressed data to disk.");

        while self.compressed_remaining_memory_in_bytes < 0 {
            let key = self.compressed_queue.pop_front().unwrap();
            self.save_compressed_data(&key);
        }
    }

    /// Save the compressed data corresponding to `key` to disk. The size of the saved compressed
    /// data is added back to the remaining reserved memory.
    fn save_compressed_data(&mut self, key: &u64) {
        info!("Saving compressed time series with key '{}' to disk.", key);

        let mut time_series = self.compressed_data.remove(&key).unwrap();
        let time_series_size = time_series.size_in_bytes.clone();

        let folder_path = self.data_folder_path.join(key.to_string());
        time_series.save_to_apache_parquet(folder_path.as_path());

        self.compressed_remaining_memory_in_bytes += time_series_size as isize;

        info!(
            "Saved {} bytes of compressed data to disk. Remaining reserved bytes: {}.",
            time_series_size, self.compressed_remaining_memory_in_bytes
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use tempfile::{tempdir, TempDir};

    use crate::storage::test_util;
    use crate::types::TimestampArray;

    const KEY: u64 = 1;

    #[test]
    #[should_panic(expected = "Schema of RecordBatch does not match compressed segment schema.")]
    fn test_panic_if_inserting_invalid_compressed_segment() {
        let invalid = test_util::get_invalid_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(KEY, invalid);
    }

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
        data_manager.free_compressed_data_memory();

        assert!(-1 < data_manager.compressed_remaining_memory_in_bytes);
    }

    /// Create a [`CompressedDataManager`] with a folder that is deleted once the test is finished.
    fn create_compressed_data_manager() -> (TempDir, CompressedDataManager) {
        let temp_dir = tempdir().unwrap();

        let data_folder_path = temp_dir.path().to_path_buf();
        (temp_dir, CompressedDataManager::new(data_folder_path))
    }

    #[test]
    fn test_can_get_saved_compressed_files() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(KEY, segment.clone());
        data_manager.save_compressed_data(&KEY);

        let result = data_manager.get_saved_compressed_files(&KEY);
        assert!(result.is_ok());

        let files = result.unwrap();
        assert_eq!(files.len(), 1);

        // The file should have the first start time and the last end time as the file name.
        let start_times: &TimestampArray = segment.column(2).as_any().downcast_ref().unwrap();
        let end_times: &TimestampArray = segment.column(3).as_any().downcast_ref().unwrap();
        let expected_file_path = format!(
            "{}/compressed/{}-{}.parquet",
            KEY,
            start_times.value(0),
            end_times.value(end_times.len() - 1)
        );

        let expected_full_path = data_manager.data_folder_path.join(expected_file_path);
        assert_eq!(*files.get(0).unwrap(), expected_full_path)
    }

    #[test]
    fn test_save_in_memory_compressed_data_when_getting_saved_compressed_files() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(KEY, segment);
        data_manager.save_compressed_data(&KEY);

        // This second inserted segment should be saved when the compressed files are retrieved.
        let segment_2 = test_util::get_compressed_segment_record_batch();
        data_manager.insert_compressed_segment(KEY, segment_2.clone());

        let result = data_manager.get_saved_compressed_files(&KEY);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_cannot_get_saved_compressed_files_from_non_existent_key() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(KEY, segment);
        data_manager.save_compressed_data(&KEY);

        let result = data_manager.get_saved_compressed_files(&999);
        assert!(result.is_err());
    }
}
