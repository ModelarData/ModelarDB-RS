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

//! Support for managing all uncompressed data that is ingested into the [`StorageEngine`].

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use datafusion::arrow::record_batch::RecordBatch;

use tracing::{info, info_span};
use crate::catalog::NewModelTableMetadata;

use crate::storage::segment::{FinishedSegment, SegmentBuilder};
use crate::types::{TimestampArray, ValueArray};

const UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES: usize = 5000;

/// Converts raw MQTT messages to uncompressed data points and stores uncompressed data points
/// temporarily in an in-memory buffer that spills to Apache Parquet files. When finished the data
/// is made available for compression.
pub struct UncompressedDataManager {
    /// Path to the folder containing all uncompressed data managed by the [`StorageEngine`].
    data_folder_path: PathBuf,
    /// The [`UncompressedSegments`](UncompressedSegment) while they are being built.
    uncompressed_data: HashMap<u64, SegmentBuilder>,
    /// FIFO queue of [`FinishedSegments`](FinishedSegment) that are ready for compression.
    finished_queue: VecDeque<FinishedSegment>,
    /// How many bytes of memory that are left for storing [`UncompressedSegments`](UncompressedSegment).
    uncompressed_remaining_memory_in_bytes: usize,
}

impl UncompressedDataManager {
    pub fn new(data_folder_path: PathBuf) -> Self {
        Self {
            data_folder_path,
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            uncompressed_data: HashMap::new(),
            finished_queue: VecDeque::new(),
            uncompressed_remaining_memory_in_bytes: UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
        }
    }

    /// Parse `data` and insert it into the in-memory buffer. The data is first parsed into multiple
    /// univariate time series based on `model_table`. These individual time series are then
    /// inserted into the storage engine. Return [`Ok`] if the data was successfully inserted,
    /// otherwise return [`Err`].
    pub fn insert_data(
        &mut self,
        model_table: NewModelTableMetadata,
        data: RecordBatch
    ) -> Result<(), String> {
        let _span = info_span!("insert_data", table = model_table.name).entered();
        info!("Received record batch with {} data points for the table '{}'.",
            data.num_rows(), model_table.name);

        // TODO: Generate the 54-bit tag hash based on the tag values of the record batch.
        // TODO: Create a univariate time series for each field column in the model table schema.
        // TODO: For each univariate time series, create a 64-bit key that uniquely identifies it.
        // TODO: Send each separate time series to be further processed.
    }

    /// Remove the oldest [`FinishedSegment`] from the queue and return it. Return [`None`] if the
    /// queue of [`FinishedSegments`](FinishedSegment) is empty.
    pub fn get_finished_segment(&mut self) -> Option<FinishedSegment> {
        if let Some(finished_segment) = self.finished_queue.pop_front() {
            // Add the memory size of the removed FinishedSegment back to the remaining bytes.
            self.uncompressed_remaining_memory_in_bytes +=
                finished_segment.uncompressed_segment.get_memory_size();

            Some(finished_segment)
        } else {
            None
        }
    }

    // TODO: Test that you cant insert timestamp and values of different lengths.
    /// Insert `data` into the segment identified by `key`. If a segment does not already exist,
    /// a new segment is created. Return [`Ok`] if the data was successfully inserted,
    /// otherwise return [`Err`].
    fn insert_into_segment(
        &mut self,
        mut timestamps: TimestampArray,
        mut values: ValueArray,
        key: u64
    ) -> Result<(), String> {
        // Get the segment from the uncompressed data. Create a new segment if it does not exist.
        let mut segment = self.uncompressed_data.get_mut(&key).unwrap_or_else(|| &mut {
            info!("Could not find segment. Creating new segment.");
            let mut segment = self.create_segment_builder();
            self.uncompressed_data.insert(key, segment.clone());

            segment
        });

        // Insert data into the segment. If data is returned, it means the segment is full.
        while let Some((remaining_timestamps, remaining_values)) = segment.insert_data(timestamps, values) {
            info!("Segment is full, moving it to the queue of finished segments.");

            // Since this is only reachable if the segment exists in the HashMap, unwrap is safe to use.
            let full_segment = self.uncompressed_data.remove(&key).unwrap();
            self.enqueue_segment(key, full_segment);

            // Since there is still data remaining, create a new segment to hold the remaining data.
            segment = &mut self.create_segment_builder();
            self.uncompressed_data.insert(key, segment.clone());

            timestamps = remaining_timestamps;
            values = remaining_values;
        }

        // TODO: Maybe add check for if the segment is full.

        Ok(())
    }

    /// Create a new segment builder and remove its size from the uncompressed remaining memory.
    fn create_segment_builder(&mut self) -> SegmentBuilder {
        // If there is not enough memory for a new segment, spill a finished segment.
        if SegmentBuilder::get_memory_size() > self.uncompressed_remaining_memory_in_bytes {
            self.spill_finished_segment();
        }

        // Create a new segment and reduce the remaining amount of reserved memory by its size.
        let segment = SegmentBuilder::new();
        self.uncompressed_remaining_memory_in_bytes -= SegmentBuilder::get_memory_size();

        info!("Created segment. Remaining reserved bytes: {}.",
            self.uncompressed_remaining_memory_in_bytes);

        segment
    }

    /// Move `segment_builder` to the queue of [`FinishedSegments`](FinishedSegment).
    fn enqueue_segment(&mut self, key: u64, segment_builder: SegmentBuilder) {
        let finished_segment = FinishedSegment {
            key,
            uncompressed_segment: Box::new(segment_builder),
        };

        self.finished_queue.push_back(finished_segment);
    }

    /// Spill the first in-memory [`FinishedSegment`] in the queue of [`FinishedSegments`](FinishedSegment).
    /// If no in-memory [`FinishedSegments`](FinishedSegment) could be found, [`panic`](std::panic).
    fn spill_finished_segment(&mut self) {
        info!("Not enough memory to create segment. Spilling an already finished segment.");

        // Iterate through the finished segments to find a segment that is in memory.
        for finished in self.finished_queue.iter_mut() {
            if let Ok(file_path) = finished.spill_to_apache_parquet(self.data_folder_path.as_path()) {
                // Add the size of the segment back to the remaining reserved bytes.
                self.uncompressed_remaining_memory_in_bytes += SegmentBuilder::get_memory_size();

                info!(
                    "Spilled the segment to '{}'. Remaining reserved bytes: {}.",
                    file_path.display(), self.uncompressed_remaining_memory_in_bytes
                );
                return ();
            }
        }

        // TODO: All uncompressed and compressed data should be saved to disk first.
        // If not able to find any in-memory finished segments, we should panic.
        panic!("Not enough reserved memory to hold all necessary segment builders.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tempfile::{TempDir, tempdir};

    use crate::storage::{BUILDER_CAPACITY, StorageEngine};

    #[test]
    fn test_cannot_insert_invalid_message() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();

        let message = Message::new("ModelarDB/test", "invalid", 1);
        data_manager.insert_data(message.clone());

        assert!(data_manager.uncompressed_data.is_empty());
    }

    #[test]
    fn test_can_insert_message_into_new_segment() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let key = insert_generated_message(&mut data_manager, "ModelarDB/test".to_owned());

        assert!(data_manager.uncompressed_data.contains_key(&key));
        assert_eq!(data_manager.uncompressed_data.get(&key).unwrap().get_length(), 1);
    }

    #[test]
    fn test_can_insert_message_into_existing_segment() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let key = insert_multiple_messages(2, &mut data_manager);

        assert!(data_manager.uncompressed_data.contains_key(&key));
        assert_eq!(data_manager.uncompressed_data.get(&key).unwrap().get_length(), 2);
    }

    #[test]
    fn test_can_get_finished_segment_when_finished() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let key = insert_multiple_messages(BUILDER_CAPACITY, &mut data_manager);

        assert!(data_manager.get_finished_segment().is_some());
    }

    #[test]
    fn test_can_get_multiple_finished_segments_when_multiple_finished() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let key = insert_multiple_messages(BUILDER_CAPACITY * 2, &mut data_manager);

        assert!(data_manager.get_finished_segment().is_some());
        assert!(data_manager.get_finished_segment().is_some());
    }

    #[test]
    fn test_cannot_get_finished_segment_when_not_finished() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();

        assert!(data_manager.get_finished_segment().is_none());
    }

    #[test]
    fn test_spill_first_finished_segment_if_out_of_memory() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        // Insert messages into the StorageEngine until a segment is spilled to Apache Parquet.
        // If there is enough memory to hold n full segments, we need n + 1 to spill a segment.
        let max_full_segments = reserved_memory / SegmentBuilder::get_memory_size();
        let message_count = (max_full_segments * BUILDER_CAPACITY) + 1;
        insert_multiple_messages(message_count, &mut data_manager);

        // The first FinishedSegment should have a memory size of 0 since it is spilled to disk.
        let first_finished = data_manager.finished_queue.pop_front().unwrap();
        assert_eq!(first_finished.uncompressed_segment.get_memory_size(), 0);

        // The FinishedSegment should be spilled to the "uncompressed" folder under the key.
        let data_folder_path = Path::new(&data_manager.data_folder_path);
        let uncompressed_path = data_folder_path.join("modelardb-test/uncompressed");
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1);
    }

    #[test]
    fn test_ignore_already_spilled_segments_when_spilling() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        insert_multiple_messages(BUILDER_CAPACITY * 2, &mut data_manager);

        data_manager.spill_finished_segment();
        // When spilling one more, the first FinishedSegment should be ignored since it is already spilled.
        data_manager.spill_finished_segment();

        data_manager.finished_queue.pop_front();
        // The second FinishedSegment should have a memory size of 0 since it is spilled to disk.
        let second_finished = data_manager.finished_queue.pop_front().unwrap();
        assert_eq!(second_finished.uncompressed_segment.get_memory_size(), 0);

        // The finished segments should be spilled to the "uncompressed" folder under the key.
        let data_folder_path = Path::new(&data_manager.data_folder_path);
        let uncompressed_path = data_folder_path.join("modelardb-test/uncompressed");
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 2);
    }

    #[test]
    fn test_remaining_memory_incremented_when_spilling_finished_segment() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();

        insert_multiple_messages(BUILDER_CAPACITY, &mut data_manager);
        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();
        data_manager.spill_finished_segment();

        assert!(remaining_memory < data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_decremented_when_creating_new_segment() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        insert_generated_message(&mut data_manager, "ModelarDB/test".to_owned());

        assert!(reserved_memory > data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_incremented_when_popping_in_memory() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let key = insert_multiple_messages(BUILDER_CAPACITY, &mut data_manager);

        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();
        data_manager.get_finished_segment();

        assert!(remaining_memory < data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_not_incremented_when_popping_spilled() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let key = insert_multiple_messages(BUILDER_CAPACITY, &mut data_manager);

        data_manager.spill_finished_segment();
        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();

        // Since the FinishedSegment is not in memory, the remaining memory should not increase when popped.
        data_manager.get_finished_segment();

        assert_eq!(remaining_memory, data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    #[should_panic(expected = "Not enough reserved memory to hold all necessary segment builders.")]
    fn test_panic_if_not_enough_reserved_memory() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        // If there is enough reserved memory to hold n builders, we need to create n + 1 to panic.
        for i in 0..(reserved_memory / SegmentBuilder::get_memory_size()) + 1 {
            insert_generated_message(&mut data_manager, i.to_string());
        }
    }

    /// Generate `count` data points for the same time series and insert them into `data_manager`.
    /// Return the key, which is the same for all generated data points.
    fn insert_multiple_messages(count: usize, data_manager: &mut UncompressedDataManager) -> String {
        let mut key = String::new();

        for _ in 0..count {
            key = insert_generated_message(data_manager, "ModelarDB/test".to_owned());
        }

        key
    }

    /// Generate a [`DataPoint`] and insert it into `data_manager`. Return the [`DataPoint`] key.
    fn insert_generated_message(data_manager: &mut UncompressedDataManager, topic: String) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let payload = format!("[{}, 30]", timestamp);
        let message = Message::new(topic, payload, 1);

        data_manager.insert_data(message.clone());

        DataPoint::from_message(&message)
            .unwrap()
            .generate_unique_key()
    }

    /// Create an [`UncompressedDataManager`] with a folder that is deleted once the test is finished.
    fn create_uncompressed_data_manager() -> (TempDir, UncompressedDataManager) {
        let temp_dir = tempdir().unwrap();

        let data_folder_path = temp_dir.path().to_path_buf();
        (temp_dir, UncompressedDataManager::new(data_folder_path))
    }
}
