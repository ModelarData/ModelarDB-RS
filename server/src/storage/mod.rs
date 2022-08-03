/* Copyright 2022 The MiniModelarDB Contributors
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

//! Converts raw MQTT messages to uncompressed data points, stores uncompressed data points
//! temporarily in an in-memory buffer that spills to Apache Parquet files, and stores data points
//! compressed as models in Apache Parquet files.

mod data_point;
mod segment;

use std::collections::vec_deque::VecDeque;
use std::collections::HashMap;
use std::fs;
use std::fs::File;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Encoding;
use datafusion::parquet::file::properties::WriterProperties;
use paho_mqtt::Message;
use tracing::{error, info, info_span};

use crate::storage::data_point::DataPoint;
use crate::storage::segment::{FinishedSegment, SegmentBuilder};
use crate::types::Timestamp;

// TODO: Add new constant that defines how much memory is used for compressed vs. uncompressed (maybe make this dynamic?).
// TODO: Add a new field to the StorageEngine that can hold compressed data.
// TODO: Rename the current data field to "uncompressed_data".

// TODO: To ensure that data is saved in the correct order we should have a queue for the compressed data.
// TODO: It should be a queue of CompressedTimeSeries structs.

// TODO: The struct should have a compressed_segments field with a list of segments that is appended to.
// TODO: To make it easier to save maybe also save the path when first creating it (key and first timestamp).
// TODO: To make it possible to update the remaining bytes when saving to a file it should have the continuously updated total size.

// TODO: It should have a method to append to this field that checks the given batch to ensure it has the correct schema.
// TODO: It should have a method that can save the compressed segments to a single file.

// TODO: When the "save_compressed_data" method on the SE is called it should check if there already is a compressed time series.
// TODO: If there is, append to the compressed time series.
// TODO: If there is not, create a new and add it to the compressed data queue.

// TODO: Since the size of compressed segments is not constant we need to have a private function in the struct to calculate the size in bytes.
// TODO: This size should be returned to the storage engine so the remaining bytes can be updated. Maybe do this before saving.
// TODO: If there is not enough space for the compressed segment. Pop the first first compressed time series and add the size back.
// TODO: Since the incoming segment can be very large we might need to save multiple time series to disk.
// TODO: If there is none left in the queue, we have to save the given compressed segment directly (maybe just create compressed time series and save immediately).
// TODO: If there is eventually enough space save the compressed segment in a compressed time series after.

// TODO: Look into using prioritized queue.
// TODO: To ensure we can do lookup with the key, we need a hashmap, maybe a prioritized hashmap??
// TODO: Maybe use both a hashmap for storage and a queue for choosing which time series should be saved.

// TODO: Switch folder structure to key/uncompressed – key/compressed.
// TODO: Add a max compressed file size constant. If a compressed time series reaches this size it should be saved to a file instead of appended to further.

// Note that the initial capacity has to be a multiple of 64 bytes to avoid the actual capacity
// being larger due to internal alignment when allocating memory for the builders.
const INITIAL_BUILDER_CAPACITY: usize = 64;
const RESERVED_MEMORY_IN_BYTES: usize = 5000;

/// Manages all uncompressed data, both while being built and when finished.
pub struct StorageEngine {
    /// The uncompressed segments while they are being built.
    data: HashMap<String, SegmentBuilder>,
    /// Prioritized queue of finished segments that are ready for compression.
    compression_queue: VecDeque<FinishedSegment>,
    /// How many bytes of memory that are left for storing uncompressed segments.
    remaining_memory_in_bytes: usize,
}

impl StorageEngine {
    pub fn new() -> Self {
        Self {
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            data: HashMap::new(),
            compression_queue: VecDeque::new(),
            remaining_memory_in_bytes: RESERVED_MEMORY_IN_BYTES,
        }
    }

    /// Parse `message` and insert it into the in-memory buffer.
    pub fn insert_message(&mut self, message: Message) {
        match DataPoint::from_message(&message) {
            Ok(data_point) => {
                let key = data_point.generate_unique_key();
                let _span = info_span!("insert_message", key = key.clone()).entered();

                info!("Inserting data point '{}' into segment.", data_point);

                if let Some(segment) = self.data.get_mut(&key) {
                    info!("Found existing segment.");

                    segment.insert_data(&data_point);

                    if segment.is_full() {
                        info!("Segment is full, moving it to the compression queue.");

                        let full_segment = self.data.remove(&key).unwrap();
                        self.enqueue_segment(key, full_segment)
                    }
                } else {
                    info!("Could not find segment. Creating segment.");

                    // If there is not enough memory for a new segment, spill a finished segment.
                    if SegmentBuilder::get_memory_size() > self.remaining_memory_in_bytes {
                        self.spill_finished_segment();
                    }

                    // Create a new segment and reduce the remaining amount of reserved memory by its size.
                    let mut segment = SegmentBuilder::new();
                    self.remaining_memory_in_bytes -= SegmentBuilder::get_memory_size();
                    info!("Created segment. Remaining bytes: {}.", self.remaining_memory_in_bytes);

                    segment.insert_data(&data_point);
                    self.data.insert(key, segment);
                }
            }
            Err(e) => error!("Message could not be inserted into storage: {:?}", e),
        }
    }

    /// Remove the oldest finished segment from the compression queue and return it. Return `None`
    /// if the compression queue is empty.
    pub fn get_finished_segment(&mut self) -> Option<FinishedSegment> {
        if let Some(finished_segment) = self.compression_queue.pop_front() {
            // Add the memory size of the removed finished segment back to the remaining bytes.
            self.remaining_memory_in_bytes +=
                finished_segment.uncompressed_segment.get_memory_size();

            Some(finished_segment)
        } else {
            None
        }
    }

    /// Write `batch` to a persistent Apache Parquet file on disk.
    pub fn save_compressed_data(key: String, first_timestamp: Timestamp, batch: RecordBatch) {
        let folder_path = format!("compressed/{}", key);
        fs::create_dir_all(&folder_path);

        let path = format!("{}/{}.parquet", folder_path, first_timestamp);
        write_batch_to_parquet(batch, path);
    }

    /// Move `segment_builder` to the compression queue.
    fn enqueue_segment(&mut self, key: String, segment_builder: SegmentBuilder) {
        let finished_segment = FinishedSegment {
            key,
            uncompressed_segment: Box::new(segment_builder),
        };

        self.compression_queue.push_back(finished_segment);
    }

    /// Spill the first in-memory finished segment in the compression queue. If no in-memory
    /// finished segments could be found, panic.
    fn spill_finished_segment(&mut self) {
        info!("Not enough memory to create segment. Spilling an already finished segment.");

        // Iterate through the finished segments to find a segment that is in memory.
        for finished in self.compression_queue.iter_mut() {
            if let Ok(path) = finished.spill_to_parquet() {
                // Add the size of the segment back to the remaining reserved bytes.
                self.remaining_memory_in_bytes += SegmentBuilder::get_memory_size();

                info!(
                    "Spilled the segment to '{}'. Remaining bytes: {}.",
                    path, self.remaining_memory_in_bytes
                );
                return ();
            }
        }

        // If not able to find any in-memory finished segments, we should panic.
        panic!("Not enough reserved memory to hold all necessary segment builders.");
    }
}

// TODO: Test using more efficient encoding. Plain encoding makes it easier to read the files externally.
/// Write `batch` to an Apache Parquet file at the location given by `path`.
fn write_batch_to_parquet(batch: RecordBatch, path: String) {
    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).expect("Writing batch.");
    writer.close().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_cannot_insert_invalid_message() {
        let mut storage_engine = StorageEngine::new();

        let message = Message::new("ModelarDB/test", "invalid", 1);
        storage_engine.insert_message(message.clone());

        assert!(storage_engine.data.is_empty());
    }

    #[test]
    fn test_can_insert_message_into_new_segment() {
        let mut storage_engine = StorageEngine::new();
        let key = insert_generated_message(&mut storage_engine, "ModelarDB/test".to_owned());

        assert!(storage_engine.data.contains_key(&key));
        assert_eq!(storage_engine.data.get(&key).unwrap().get_length(), 1);
    }

    #[test]
    fn test_can_insert_message_into_existing_segment() {
        let mut storage_engine = StorageEngine::new();
        let key = insert_multiple_messages(2, &mut storage_engine);

        assert!(storage_engine.data.contains_key(&key));
        assert_eq!(storage_engine.data.get(&key).unwrap().get_length(), 2);
    }

    #[test]
    fn test_can_get_finished_segment_when_finished() {
        let mut storage_engine = StorageEngine::new();
        let key = insert_multiple_messages(INITIAL_BUILDER_CAPACITY, &mut storage_engine);

        assert!(storage_engine.get_finished_segment().is_some());
    }

    #[test]
    fn test_can_get_multiple_finished_segments_when_multiple_finished() {
        let mut storage_engine = StorageEngine::new();
        let key = insert_multiple_messages(INITIAL_BUILDER_CAPACITY * 2, &mut storage_engine);

        assert!(storage_engine.get_finished_segment().is_some());
        assert!(storage_engine.get_finished_segment().is_some());
    }

    #[test]
    fn test_cannot_get_finished_segment_when_not_finished() {
        let mut storage_engine = StorageEngine::new();

        assert!(storage_engine.get_finished_segment().is_none());
    }

    #[test]
    fn test_remaining_memory_decremented_when_creating_new_segment() {
        let mut storage_engine = StorageEngine::new();
        let initial_remaining_memory = storage_engine.remaining_memory_in_bytes;

        insert_generated_message(&mut storage_engine, "ModelarDB/test".to_owned());

        assert!(initial_remaining_memory > storage_engine.remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_incremented_when_popping_in_memory() {
        let mut storage_engine = StorageEngine::new();
        let key = insert_multiple_messages(INITIAL_BUILDER_CAPACITY, &mut storage_engine);

        let previous_remaining_memory = storage_engine.remaining_memory_in_bytes.clone();
        storage_engine.get_finished_segment();

        assert!(previous_remaining_memory < storage_engine.remaining_memory_in_bytes);
    }

    #[test]
    #[should_panic(expected = "Not enough reserved memory to hold all necessary segment builders.")]
    fn test_panic_if_not_enough_reserved_memory() {
        let mut storage_engine = StorageEngine::new();
        let reserved_memory = storage_engine.remaining_memory_in_bytes;

        // If there is enough reserved memory to hold n builders, we need to create n + 1 to panic.
        for i in 0..(reserved_memory / SegmentBuilder::get_memory_size()) + 1
        {
            insert_generated_message(&mut storage_engine, i.to_string());
        }
    }

    /// Generate `count` data points for the same time series and insert them into `storage_engine`.
    /// Return the key, which is the same for all generated data points.
    fn insert_multiple_messages(count: usize, storage_engine: &mut StorageEngine) -> String {
        let mut key = String::new();

        for _ in 0..count {
            key = insert_generated_message(storage_engine, "ModelarDB/test".to_owned());
        }

        key
    }

    /// Generate a data point and insert it into `storage_engine`. Return the data point key.
    fn insert_generated_message(storage_engine: &mut StorageEngine, topic: String) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let payload = format!("[{}, 30]", timestamp);
        let message = Message::new(topic, payload, 1);

        storage_engine.insert_message(message.clone());

        DataPoint::from_message(&message)
            .unwrap()
            .generate_unique_key()
    }
}
