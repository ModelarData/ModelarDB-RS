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

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::Hasher;
use std::path::PathBuf;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use rusqlite::Connection;
use tracing::{info, info_span};

use crate::catalog::NewModelTableMetadata;
use crate::storage::segment::{FinishedSegment, SegmentBuilder};
use crate::types::{Timestamp, TimestampArray, Value, ValueArray};

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
    /// Cache of tag value hashes used to signify when to persist new unsaved tag combinations.
    tag_value_hashes: HashMap<String, u64>,
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
            tag_value_hashes: HashMap::new(),
            uncompressed_remaining_memory_in_bytes: UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
        }
    }

    // TODO: The insert data point function has been fully tested by this still needs tests.
    /// Parse `data` and insert it into the in-memory buffer. The data is first parsed into multiple
    /// univariate time series based on `model_table`. These individual time series are then
    /// inserted into the storage engine. Return [`Ok`] if the data was successfully inserted,
    /// otherwise return [`Err`].
    pub fn insert_data(
        &mut self,
        model_table: NewModelTableMetadata,
        data: RecordBatch,
    ) -> Result<(), String> {
        let _span = info_span!("insert_data", table = model_table.name).entered();
        info!(
            "Received record batch with {} data points for the table '{}'.",
            data.num_rows(),
            model_table.name
        );

        // Prepare the timestamp column for iteration.
        let timestamp_index = model_table.timestamp_column_index as usize;
        let timestamps: &TimestampArray = data
            .column(timestamp_index)
            .as_any()
            .downcast_ref()
            .unwrap();

        // Prepare the tag columns for iteration.
        let tag_column_arrays: Vec<&StringArray> = model_table
            .tag_column_indices
            .iter()
            .map(|index| {
                data.column(*index as usize)
                    .as_any()
                    .downcast_ref()
                    .unwrap()
            })
            .collect();

        // Prepare the field columns for iteration. The column index is saved with the corresponding array.
        let field_column_arrays: Vec<(usize, &ValueArray)> = model_table
            .schema
            .fields()
            .iter()
            .filter_map(|field| {
                let index = model_table.schema.index_of(field.name().as_str()).unwrap();

                // Field columns are the columns that are not the timestamp column or one of the tag columns.
                let not_timestamp_column = index != model_table.timestamp_column_index as usize;
                let not_tag_column = !model_table.tag_column_indices.contains(&(index as u8));

                if not_timestamp_column && not_tag_column {
                    Some((
                        index,
                        data.column(index as usize).as_any().downcast_ref().unwrap(),
                    ))
                } else {
                    None
                }
            })
            .collect();

        // For each row in the data, generate a tag hash, extract the individual measurements,
        // and insert them into the storage engine.
        for (index, timestamp) in timestamps.iter().enumerate() {
            let tag_values = tag_column_arrays
                .iter()
                .map(|array| array.value(index).to_string())
                .collect();

            let tag_hash = self
                .get_tag_hash(model_table.clone(), tag_values)
                .map_err(|error| format!("Tag hash could not be saved: {}", error.to_string()))?;

            // For each field column, generate the 64-bit key, create a single data point, and
            // insert the data point into the in-memory buffer.
            for (field_index, field_column_array) in &field_column_arrays {
                let key = tag_hash | *field_index as u64;
                let value = field_column_array.value(index);

                // unwrap() is safe to use since the timestamps array cannot contain null values.
                self.insert_data_point(key, timestamp.unwrap(), value);
            }
        }

        Ok(())
    }

    /// Return the tag hash for the given list of tag values either by retrieving it from the cache
    /// or, if it is a new combination of tag values, generating a new hash. If a new hash is
    /// created, the hash is saved both in the cache and persisted to the model_table_tags table. If
    /// the table could not be accessed, return [`rusqlite::Error`].
    fn get_tag_hash(
        &mut self,
        model_table: NewModelTableMetadata,
        tag_values: Vec<String>,
    ) -> Result<u64, rusqlite::Error> {
        let cache_key = {
            let mut cache_key_list = tag_values.clone();
            cache_key_list.push(model_table.name.clone());

            cache_key_list.join("")
        };

        // Check if the tag hash is in the cache. If it is, retrieve it. If it is not, create a new
        // one and save it both in the cache and in the model_table_tags table.
        if let Some(tag_hash) = self.tag_value_hashes.get(cache_key.as_str()) {
            Ok(*tag_hash)
        } else {
            // Generate the 54-bit tag hash based on the tag values of the record batch and model table name.
            let tag_hash = {
                let mut hasher = DefaultHasher::new();
                hasher.write(model_table.name.as_bytes());

                for tag_value in &tag_values {
                    hasher.write(tag_value.as_bytes());
                }

                // The 64-bit hash is shifted to make the final 10 bits 0.
                hasher.finish() << 10
            };

            // Save the tag hash in the cache and in the metadata database model_table_tags table.
            self.tag_value_hashes.insert(cache_key, tag_hash);

            // TODO: Move this to the metadata component when it exists.
            let tag_columns: String = model_table
                .tag_column_indices
                .iter()
                .map(|index| model_table.schema.field(*index as usize).name().clone())
                .collect::<Vec<String>>()
                .join(",");

            let values = tag_values
                .iter()
                .map(|value| format!("'{}'", value))
                .collect::<Vec<String>>()
                .join(",");

            let database_path = self.data_folder_path.join("metadata.sqlite3");
            let connection = Connection::open(database_path)?;

            // OR IGNORE is used to silently fail when trying to insert an already existing hash.
            connection.execute(
                format!(
                    "INSERT OR IGNORE INTO {}_tags (hash,{}) VALUES ({},{})",
                    model_table.name, tag_columns, tag_hash, values
                )
                .as_str(),
                (),
            )?;

            Ok(tag_hash)
        }
    }

    /// Insert a single data point into the in-memory buffer. Return [`OK`] if the data point was
    /// inserted successfully, otherwise return [`Err`].
    fn insert_data_point(
        &mut self,
        key: u64,
        timestamp: Timestamp,
        value: Value,
    ) -> Result<(), String> {
        info!(
            "Inserting data point ({}, {}) into segment.",
            timestamp, value
        );

        if let Some(segment) = self.uncompressed_data.get_mut(&key) {
            info!("Found existing segment.");
            segment.insert_data(timestamp, value);

            if segment.is_full() {
                info!("Segment is full, moving it to the queue of finished segments.");

                // Since this is only reachable if the segment exists in the HashMap, unwrap is safe to use.
                let full_segment = self.uncompressed_data.remove(&key).unwrap();
                self.enqueue_segment(key, full_segment)
            }
        } else {
            info!("Could not find segment. Creating segment.");

            // If there is not enough memory for a new segment, spill a finished segment.
            if SegmentBuilder::get_memory_size() > self.uncompressed_remaining_memory_in_bytes {
                self.spill_finished_segment();
            }

            // Create a new segment and reduce the remaining amount of reserved memory by its size.
            let mut segment = SegmentBuilder::new();
            self.uncompressed_remaining_memory_in_bytes -= SegmentBuilder::get_memory_size();

            info!(
                "Created segment. Remaining reserved bytes: {}.",
                self.uncompressed_remaining_memory_in_bytes
            );

            segment.insert_data(timestamp, value);
            self.uncompressed_data.insert(key, segment);
        }

        Ok(())
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
            if let Ok(file_path) = finished.spill_to_apache_parquet(self.data_folder_path.as_path())
            {
                // Add the size of the segment back to the remaining reserved bytes.
                self.uncompressed_remaining_memory_in_bytes += SegmentBuilder::get_memory_size();

                info!(
                    "Spilled the segment to '{}'. Remaining reserved bytes: {}.",
                    file_path.display(),
                    self.uncompressed_remaining_memory_in_bytes
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

    use tempfile::{tempdir, TempDir};

    use crate::storage::BUILDER_CAPACITY;

    #[test]
    fn test_can_insert_data_point_into_new_segment() {
        let key = 1;
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        insert_data_points(1, &mut data_manager, key);

        assert!(data_manager.uncompressed_data.contains_key(&key));
        assert_eq!(
            data_manager
                .uncompressed_data
                .get(&key)
                .unwrap()
                .get_length(),
            1
        );
    }

    #[test]
    fn test_can_insert_message_into_existing_segment() {
        let key = 1;
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        insert_data_points(2, &mut data_manager, key);

        assert!(data_manager.uncompressed_data.contains_key(&key));
        assert_eq!(
            data_manager
                .uncompressed_data
                .get(&key)
                .unwrap()
                .get_length(),
            2
        );
    }

    #[test]
    fn test_can_get_finished_segment_when_finished() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        insert_data_points(BUILDER_CAPACITY, &mut data_manager, 1);

        assert!(data_manager.get_finished_segment().is_some());
    }

    #[test]
    fn test_can_get_multiple_finished_segments_when_multiple_finished() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        insert_data_points(BUILDER_CAPACITY * 2, &mut data_manager, 1);

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
        insert_data_points(message_count, &mut data_manager, 1);

        // The first FinishedSegment should have a memory size of 0 since it is spilled to disk.
        let first_finished = data_manager.finished_queue.pop_front().unwrap();
        assert_eq!(first_finished.uncompressed_segment.get_memory_size(), 0);

        // The FinishedSegment should be spilled to the "uncompressed" folder under the key.
        let data_folder_path = Path::new(&data_manager.data_folder_path);
        let uncompressed_path = data_folder_path.join("1/uncompressed");
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1);
    }

    #[test]
    fn test_ignore_already_spilled_segments_when_spilling() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        insert_data_points(BUILDER_CAPACITY * 2, &mut data_manager, 1);

        data_manager.spill_finished_segment();
        // When spilling one more, the first FinishedSegment should be ignored since it is already spilled.
        data_manager.spill_finished_segment();

        data_manager.finished_queue.pop_front();
        // The second FinishedSegment should have a memory size of 0 since it is spilled to disk.
        let second_finished = data_manager.finished_queue.pop_front().unwrap();
        assert_eq!(second_finished.uncompressed_segment.get_memory_size(), 0);

        // The finished segments should be spilled to the "uncompressed" folder under the key.
        let data_folder_path = Path::new(&data_manager.data_folder_path);
        let uncompressed_path = data_folder_path.join("1/uncompressed");
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 2);
    }

    #[test]
    fn test_remaining_memory_incremented_when_spilling_finished_segment() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();

        insert_data_points(BUILDER_CAPACITY, &mut data_manager, 1);
        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();
        data_manager.spill_finished_segment();

        assert!(remaining_memory < data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_decremented_when_creating_new_segment() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        insert_data_points(1, &mut data_manager, 1);

        assert!(reserved_memory > data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_incremented_when_popping_in_memory() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        insert_data_points(BUILDER_CAPACITY, &mut data_manager, 1);

        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();
        data_manager.get_finished_segment();

        assert!(remaining_memory < data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_not_incremented_when_popping_spilled() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        insert_data_points(BUILDER_CAPACITY, &mut data_manager, 1);

        data_manager.spill_finished_segment();
        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();

        // Since the FinishedSegment is not in memory, the remaining memory should not increase when popped.
        data_manager.get_finished_segment();

        assert_eq!(
            remaining_memory,
            data_manager.uncompressed_remaining_memory_in_bytes
        );
    }

    #[test]
    #[should_panic(expected = "Not enough reserved memory to hold all necessary segment builders.")]
    fn test_panic_if_not_enough_reserved_memory() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        // If there is enough reserved memory to hold n builders, we need to create n + 1 to panic.
        for i in 0..(reserved_memory / SegmentBuilder::get_memory_size()) + 1 {
            insert_data_points(1, &mut data_manager, i as u64);
        }
    }

    /// Insert `count` data points into `data_manager`.
    fn insert_data_points(count: usize, data_manager: &mut UncompressedDataManager, key: u64){
        let value: Value = 30.0;

        for i in 0..count {
            data_manager.insert_data_point(key, i as i64, value);
        }
    }

    /// Create an [`UncompressedDataManager`] with a folder that is deleted once the test is finished.
    fn create_uncompressed_data_manager() -> (TempDir, UncompressedDataManager) {
        let temp_dir = tempdir().unwrap();

        let data_folder_path = temp_dir.path().to_path_buf();
        (temp_dir, UncompressedDataManager::new(data_folder_path))
    }
}
