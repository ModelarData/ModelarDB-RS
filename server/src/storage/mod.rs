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
mod time_series;

use std::collections::vec_deque::VecDeque;
use std::collections::HashMap;
use std::fs::File;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Encoding;
use datafusion::parquet::file::properties::WriterProperties;
use paho_mqtt::Message;
use tracing::{info, info_span};

use crate::storage::data_point::DataPoint;
use crate::storage::segment::{FinishedSegment, SegmentBuilder};
use crate::storage::time_series::CompressedTimeSeries;

// TODO: Look into moving handling of uncompressed and compressed data into separate structs.
// TODO: Look into custom errors for all errors in storage engine.
// TODO: How should error returns from "save_to_parquet" be handled?

// Note that the initial capacity has to be a multiple of 64 bytes to avoid the actual capacity
// being larger due to internal alignment when allocating memory for the builders.
const INITIAL_BUILDER_CAPACITY: usize = 64;
const UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES: usize = 5000;
const COMPRESSED_RESERVED_MEMORY_IN_BYTES: isize = 5000;
const MAX_COMPRESSED_FILE_SIZE_IN_BYTES: usize = 1024;

/// Manages all uncompressed data, both while being built and when finished.
pub struct StorageEngine {
    /// The uncompressed segments while they are being built.
    uncompressed_data: HashMap<String, SegmentBuilder>,
    /// Prioritized queue of finished segments that are ready for compression.
    finished_queue: VecDeque<FinishedSegment>,
    /// The compressed segments before they are saved to persistent storage.
    compressed_data: HashMap<String, CompressedTimeSeries>,
    /// Prioritized queue of time series keys that can be saved to persistent storage.
    compression_queue: VecDeque<String>,
    /// How many bytes of memory that are left for storing uncompressed segments.
    uncompressed_remaining_memory_in_bytes: usize,
    /// How many bytes of memory that are left for storing compressed segments.
    compressed_remaining_memory_in_bytes: isize,
}

impl StorageEngine {
    pub fn new() -> Self {
        Self {
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            uncompressed_data: HashMap::new(),
            finished_queue: VecDeque::new(),
            compressed_data: HashMap::new(),
            compression_queue: VecDeque::new(),
            uncompressed_remaining_memory_in_bytes: UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            compressed_remaining_memory_in_bytes: COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        }
    }

    /// Parse `message` and insert it into the in-memory buffer. Return Ok if the message was
    /// successfully inserted, otherwise return Err.
    pub fn insert_message(&mut self, message: Message) -> Result<(), String> {
        let data_point = DataPoint::from_message(&message)?;
        let key = data_point.generate_unique_key();
        let _span = info_span!("insert_message", key = key.clone()).entered();

        info!("Inserting data point '{}' into segment.", data_point);

        if let Some(segment) = self.uncompressed_data.get_mut(&key) {
            info!("Found existing segment.");

            segment.insert_data(&data_point);

            if segment.is_full() {
                info!("Segment is full, moving it to the compression queue.");

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
                "Created segment. Remaining bytes: {}.",
                self.uncompressed_remaining_memory_in_bytes
            );

            segment.insert_data(&data_point);
            self.uncompressed_data.insert(key, segment);
        }

        Ok(())
    }

    /// Remove the oldest finished segment from the compression queue and return it. Return `None`
    /// if the compression queue is empty.
    pub fn get_finished_segment(&mut self) -> Option<FinishedSegment> {
        if let Some(finished_segment) = self.finished_queue.pop_front() {
            // Add the memory size of the removed finished segment back to the remaining bytes.
            self.uncompressed_remaining_memory_in_bytes +=
                finished_segment.uncompressed_segment.get_memory_size();

            Some(finished_segment)
        } else {
            None
        }
    }

    // TODO: Check if the max compressed file size is reached and save if so.
    /// Insert `batch` into the in-memory compressed time series buffer.
    pub fn insert_compressed_data(&mut self, key: String, batch: RecordBatch) {
        let _span = info_span!("insert_compressed_segment", key = key.clone()).entered();
        info!("Inserting batch with {} rows into compressed time series.", batch.num_rows());

        let compressed_segment_size;

        // Since the compressed segment is already in memory, insert the segment in to the structure
        // first and check if the reserved memory limit is exceeded after.
        if let Some(time_series) = self.compressed_data.get_mut(&key) {
            info!("Found existing compressed time series.");

            compressed_segment_size = time_series.append_segment(batch);
        } else {
            info!("Could not find compressed time series. Creating compressed time series.");

            let mut time_series = CompressedTimeSeries::new();
            compressed_segment_size = time_series.append_segment(batch);

            self.compressed_data.insert(key.clone(), time_series);
            self.compression_queue.push_back(key.clone());
        }

        self.compressed_remaining_memory_in_bytes -= compressed_segment_size as isize;

        // If the reserved memory limit is exceeded, save compressed data to disk.
        if self.compressed_remaining_memory_in_bytes < 0 {
            self.save_compressed_data();
        }
    }

    /// Move `segment_builder` to the compression queue.
    fn enqueue_segment(&mut self, key: String, segment_builder: SegmentBuilder) {
        let finished_segment = FinishedSegment {
            key,
            uncompressed_segment: Box::new(segment_builder),
        };

        self.finished_queue.push_back(finished_segment);
    }

    /// Spill the first in-memory finished segment in the compression queue. If no in-memory
    /// finished segments could be found, panic.
    fn spill_finished_segment(&mut self) {
        info!("Not enough memory to create segment. Spilling an already finished segment.");

        // Iterate through the finished segments to find a segment that is in memory.
        for finished in self.finished_queue.iter_mut() {
            if let Ok(path) = finished.spill_to_parquet() {
                // Add the size of the segment back to the remaining reserved bytes.
                self.uncompressed_remaining_memory_in_bytes += SegmentBuilder::get_memory_size();

                info!(
                    "Spilled the segment to '{}'. Remaining bytes: {}.",
                    path, self.uncompressed_remaining_memory_in_bytes
                );
                return ();
            }
        }

        // If not able to find any in-memory finished segments, we should panic.
        panic!("Not enough reserved memory to hold all necessary segment builders.");
    }

    /// Save compressed time series to disk until the reserved memory limit is no longer exceeded.
    fn save_compressed_data(&mut self) {
        info!("Out of memory to store compressed data. Saving compressed data to disk.");

        while self.compressed_remaining_memory_in_bytes < 0 {
            let key = self.compression_queue.pop_front().unwrap();
            info!("Saving compressed time series with key '{}' to disk.", key);

            let mut time_series = self.compressed_data.remove(&key).unwrap();
            let time_series_size = time_series.size_in_bytes.clone();
            time_series.save_to_parquet(key.clone());

            self.compressed_remaining_memory_in_bytes += time_series_size as isize;
        }
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

    use crate::storage::time_series::test_util;

    // Tests for uncompressed data.
    #[test]
    fn test_cannot_insert_invalid_message() {
        let mut storage_engine = StorageEngine::new();

        let message = Message::new("ModelarDB/test", "invalid", 1);
        storage_engine.insert_message(message.clone());

        assert!(storage_engine.uncompressed_data.is_empty());
    }

    #[test]
    fn test_can_insert_message_into_new_segment() {
        let mut storage_engine = StorageEngine::new();
        let key = insert_generated_message(&mut storage_engine, "ModelarDB/test".to_owned());

        assert!(storage_engine.uncompressed_data.contains_key(&key));
        assert_eq!(storage_engine.uncompressed_data.get(&key).unwrap().get_length(), 1);
    }

    #[test]
    fn test_can_insert_message_into_existing_segment() {
        let mut storage_engine = StorageEngine::new();
        let key = insert_multiple_messages(2, &mut storage_engine);

        assert!(storage_engine.uncompressed_data.contains_key(&key));
        assert_eq!(storage_engine.uncompressed_data.get(&key).unwrap().get_length(), 2);
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
        let initial_remaining_memory = storage_engine.uncompressed_remaining_memory_in_bytes;

        insert_generated_message(&mut storage_engine, "ModelarDB/test".to_owned());

        assert!(initial_remaining_memory > storage_engine.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_incremented_when_popping_in_memory() {
        let mut storage_engine = StorageEngine::new();
        let key = insert_multiple_messages(INITIAL_BUILDER_CAPACITY, &mut storage_engine);

        let previous_remaining_memory = storage_engine.uncompressed_remaining_memory_in_bytes.clone();
        storage_engine.get_finished_segment();

        assert!(previous_remaining_memory < storage_engine.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    #[should_panic(expected = "Not enough reserved memory to hold all necessary segment builders.")]
    fn test_panic_if_not_enough_reserved_memory() {
        let mut storage_engine = StorageEngine::new();
        let reserved_memory = storage_engine.uncompressed_remaining_memory_in_bytes;

        // If there is enough reserved memory to hold n builders, we need to create n + 1 to panic.
        for i in 0..(reserved_memory / SegmentBuilder::get_memory_size()) + 1 {
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

    // Tests for compressed data.
    #[test]
    #[should_panic(expected = "Schema of record batch does not match compressed segment schema.")]
    fn test_cannot_insert_invalid_compressed_segment() {
        let invalid = test_util::get_invalid_compressed_segment_record_batch();
        let mut storage_engine = StorageEngine::new();

        storage_engine.insert_compressed_data("key".to_owned(), invalid);
    }

    #[test]
    fn test_can_insert_compressed_segment_into_new_time_series() {
        let segment = test_util::get_compressed_segment_record_batch();
        let mut storage_engine = StorageEngine::new();

        storage_engine.insert_compressed_data("key".to_owned(), segment);

        assert!(storage_engine.compressed_data.contains_key("key"));
        assert_eq!(storage_engine.compression_queue.pop_front().unwrap(), "key");

        assert!(storage_engine.compressed_data.get("key").unwrap().size_in_bytes > 0);
    }

    #[test]
    fn test_can_insert_compressed_segment_into_existing_time_series() {
        let segment = test_util::get_compressed_segment_record_batch();
        let mut storage_engine = StorageEngine::new();

        storage_engine.insert_compressed_data("key".to_owned(), segment.clone());
        let previous_size = storage_engine.compressed_data.get("key").unwrap().size_in_bytes;
        storage_engine.insert_compressed_data("key".to_owned(), segment);

        assert!(storage_engine.compressed_data.get("key").unwrap().size_in_bytes > previous_size);
    }

    #[test]
    fn test_remaining_bytes_decremented_when_inserting() {
        let segment = test_util::get_compressed_segment_record_batch();
        let mut storage_engine = StorageEngine::new();
        let initial_remaining_memory = storage_engine.compressed_remaining_memory_in_bytes;

        storage_engine.insert_compressed_data("key".to_owned(), segment);

        assert!(storage_engine.compressed_remaining_memory_in_bytes < initial_remaining_memory);
    }
}
