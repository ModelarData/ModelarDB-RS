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

//! Support for managing all compressed data that is inserted into the storage engine.

use std::collections::{HashMap, VecDeque};

use datafusion::arrow::record_batch::RecordBatch;
use tracing::{info, info_span};

use crate::storage::time_series::CompressedTimeSeries;

// Signed integer since compressed data is inserted first and the remaining bytes are checked after.
// This means that the remaining bytes can be negative briefly until compressed data is saved to disk.
const COMPRESSED_RESERVED_MEMORY_IN_BYTES: isize = 5000;

/// Stores data points compressed as models in memory to batch compressed data before saving it to
/// Apache Parquet files.
struct CompressedDataManager {
    /// Path to the folder containing all compressed data managed by the storage engine.
    storage_folder_path: String,
    /// The compressed segments before they are saved to persistent storage.
    compressed_data: HashMap<String, CompressedTimeSeries>,
    /// Prioritized queue of time series keys referring to data that can be saved to persistent storage.
    compressed_queue: VecDeque<String>,
    /// How many bytes of memory that are left for storing compressed segments.
    compressed_remaining_memory_in_bytes: isize,
}

impl CompressedDataManager {
    pub fn new(storage_folder_path: String) -> Self {
        Self {
            storage_folder_path,
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            compressed_data: HashMap::new(),
            compressed_queue: VecDeque::new(),
            compressed_remaining_memory_in_bytes: COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        }
    }

    /// Insert `segment` into the in-memory compressed time series buffer.
    pub fn insert_compressed_data(&mut self, key: String, segment: RecordBatch) {
        let _span = info_span!("insert_compressed_segment", key = key.clone()).entered();
        info!(
            "Inserting batch with {} rows into compressed time series.",
            segment.num_rows()
        );

        // Since the compressed segment is already in memory, insert the segment in to the structure
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

    /// Save compressed time series to disk until the reserved memory limit is no longer exceeded.
    fn save_compressed_data(&mut self) {
        info!("Out of memory to store compressed data. Saving compressed data to disk.");

        while self.compressed_remaining_memory_in_bytes < 0 {
            let key = self.compressed_queue.pop_front().unwrap();
            info!("Saving compressed time series with key '{}' to disk.", key);

            let mut time_series = self.compressed_data.remove(&key).unwrap();
            let time_series_size = time_series.size_in_bytes.clone();

            let folder_path = format!("{}/{}", self.storage_folder_path.clone(), key.clone());
            time_series.save_to_apache_parquet(folder_path);

            self.compressed_remaining_memory_in_bytes += time_series_size as isize;

            info!(
                "Saved {} bytes of compressed data to disk. Remaining reserved bytes: {}.",
                time_series_size, self.compressed_remaining_memory_in_bytes
            );
        }
    }
}
