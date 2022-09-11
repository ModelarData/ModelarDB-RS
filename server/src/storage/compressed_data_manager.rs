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
use std::path::PathBuf;

use datafusion::arrow::record_batch::RecordBatch;
use tracing::{info, info_span};

use crate::storage::time_series::CompressedTimeSeries;

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
            self.save_compressed_data();
        }
    }

    /// Save [`CompressedTimeSeries`] to disk until the reserved memory limit is no longer exceeded.
    fn save_compressed_data(&mut self) {
        info!("Out of memory for compressed data. Saving compressed data to disk.");

        while self.compressed_remaining_memory_in_bytes < 0 {
            let key = self.compressed_queue.pop_front().unwrap();
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use tempfile::{tempdir, TempDir};

    use crate::storage::{test_util, StorageEngine};

    #[test]
    #[should_panic(expected = "Schema of RecordBatch does not match compressed segment schema.")]
    fn test_panic_if_inserting_invalid_compressed_segment() {
        let invalid = test_util::get_invalid_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(1, invalid);
    }

    #[test]
    fn test_can_insert_compressed_segment_into_new_time_series() {
        let key: u64 = 1;
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(key, segment);

        assert!(data_manager.compressed_data.contains_key(&key));
        assert_eq!(data_manager.compressed_queue.pop_front().unwrap(), key);
        assert!(
            data_manager
                .compressed_data
                .get(&key)
                .unwrap()
                .size_in_bytes
                > 0
        );
    }

    #[test]
    fn test_can_insert_compressed_segment_into_existing_time_series() {
        let key: u64 = 1;
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(key, segment.clone());
        let previous_size = data_manager
            .compressed_data
            .get(&key)
            .unwrap()
            .size_in_bytes;

        data_manager.insert_compressed_segment(key, segment);

        assert!(
            data_manager
                .compressed_data
                .get(&key)
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
            data_manager.insert_compressed_segment(1, segment.clone());
        }

        // The compressed data should be saved to the "compressed" folder under the key.
        let data_folder_path = Path::new(&data_manager.data_folder_path);
        let compressed_path = data_folder_path.join("1/compressed");
        assert_eq!(compressed_path.read_dir().unwrap().count(), 1);
    }

    #[test]
    fn test_remaining_bytes_decremented_when_inserting_compressed_segment() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();
        let reserved_memory = data_manager.compressed_remaining_memory_in_bytes;

        data_manager.insert_compressed_segment(1, segment);

        assert!(reserved_memory > data_manager.compressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_incremented_when_saving_compressed_time_series() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut data_manager) = create_compressed_data_manager();

        data_manager.insert_compressed_segment(1, segment.clone());

        // Set the remaining memory to a negative value since data is only saved when out of memory.
        data_manager.compressed_remaining_memory_in_bytes = -1;
        data_manager.save_compressed_data();

        assert!(-1 < data_manager.compressed_remaining_memory_in_bytes);
    }

    /// Create a [`CompressedDataManager`] with a folder that is deleted once the test is finished.
    fn create_compressed_data_manager() -> (TempDir, CompressedDataManager) {
        let temp_dir = tempdir().unwrap();

        let data_folder_path = temp_dir.path().to_path_buf();
        (temp_dir, CompressedDataManager::new(data_folder_path))
    }
}
