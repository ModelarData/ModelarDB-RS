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

use paho_mqtt::Message;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Encoding;
use datafusion::parquet::file::properties::WriterProperties;
use tracing::{error, info};

use crate::storage::data_point::DataPoint;
use crate::storage::segment::{SpilledSegment, SegmentBuilder, UncompressedSegment};
use crate::types::Timestamp;

// Note that the initial capacity has to be a multiple of 64 bytes to avoid the actual capacity
// being larger due to internal alignment when allocating memory for the builders.
const INITIAL_BUILDER_CAPACITY: usize = 64;
const RESERVED_BYTES: usize = 500;

// TODO: Add test for decrementing the remaining bytes when creating a builder.
// TODO: Add test for buffering unbuffered finished segments if there is not enough space when creating a builder.
// TODO: Add test for panicking if trying to buffer unbuffered and there are none.
// TODO: Add test for incrementing the remaining bytes when a builder is finished.

// TODO: Add test for checking that we cannot buffer a buffered finished segment.
// TODO: Add test for checking that we can buffer a unbuffered finished segment.

// TODO: Maybe remove finished segment and add get_key to uncompressed segment.
// TODO: Maybe split insert message into separate functions to avoid one large function.

/// Manages all uncompressed data, both while being built and when finished.
pub struct StorageEngine {
    /// The uncompressed segments while they are being built.
    data: HashMap<String, SegmentBuilder>,
    /// Prioritized queue of finished segments that are ready for compression.
    compression_queue: VecDeque<Box<dyn UncompressedSegment>>,
    /// How many bytes of memory that are left for storing uncompressed segments.
    remaining_bytes: usize,
}

impl StorageEngine {
    pub fn new() -> Self {
        Self {
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            data: HashMap::new(),
            compression_queue: VecDeque::new(),
            remaining_bytes: RESERVED_BYTES
        }
    }

    /// Parse `message` and insert it into the in-memory buffer.
    pub fn insert_message(&mut self, message: Message) {
        match DataPoint::from_message(&message) {
            Ok(data_point) => {
                let key = data_point.generate_unique_key();

                info!("Inserting data point {:?} into segment with key '{}'.", data_point, key);

                if let Some(segment) = self.data.get_mut(&key) {
                    info!("Found existing segment with key '{}'.", key);

                    segment.insert_data(&data_point);

                    if segment.is_full() {
                        info!("Segment is full, moving it to the compression queue.");

                        let full_segment = self.data.remove(&key).unwrap();
                        self.enqueue_segment(key, full_segment)
                    }
                } else {
                    info!("Could not find segment with key '{}'. Creating segment.", key);

                    // If there is not enough space for a new segment, spill a finished segment.
                    if SegmentBuilder::get_memory_size() > self.remaining_bytes {
                        self.spill_finished_segment();
                    }

                    // Create a new segment and remove the size from the reserved remaining memory.
                    let mut segment = SegmentBuilder::new();
                    self.remaining_bytes -= SegmentBuilder::get_memory_size();
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
            self.remaining_bytes += finished_segment.get_memory_size();

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

    /// Move `segment_builder` to the the compression queue. If necessary, spill the data to Parquet first.
    fn enqueue_segment(&mut self, key: String, segment_builder: SegmentBuilder) {
        info!("Saving the finished segment. Remaining bytes: {}", self.remaining_bytes);

        let builder_size = SegmentBuilder::get_memory_size();

        // If there is not enough space for the finished segment, spill the data to a Parquet file.
        if builder_size > self.remaining_bytes {
            info!("Not enough memory for the finished segment. Spilling the data to a file.");

            let spilled_segment = SpilledSegment::new(key.clone(), segment_builder);
            self.compression_queue.push_back(Box::new(spilled_segment));
        } else {
            info!("Saving the finished segment in memory.");
            self.compression_queue.push_back(Box::new(segment_builder));

            // Since it is saved in memory, remove the size of the segment from the remaining bytes.
            self.remaining_bytes -= builder_size;
        }
    }

    /// Spill the first in-memory finished segment in the compression queue and return Ok.
    /// If the spill failed return Err and if no in-memory finished segments could be found, panic.
    fn spill_finished_segment(&mut self) {
        info!("Not enough memory to create segment. Spilling an already finished segment.");

        // Iterate through the finished segments to find a segment that is in memory.
        for finished in self.compression_queue.iter_mut() {
            if finished.get_memory_size() > 0 {
                info!("Spilling the segment with key '{}' to a Parquet file.", finished.key);
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
        let key = insert_generated_message(&mut storage_engine);

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
    fn test_remaining_bytes_decremented_when_queuing_in_memory() {
        let mut storage_engine = StorageEngine::new();
        let initial_remaining_bytes = storage_engine.remaining_bytes.clone();
        let key = insert_multiple_messages(INITIAL_BUILDER_CAPACITY, &mut storage_engine);

        assert!(initial_remaining_bytes > storage_engine.remaining_bytes);
    }

    #[test]
    fn test_remaining_bytes_incremented_when_popping_in_memory() {
        let mut storage_engine = StorageEngine::new();
        let key = insert_multiple_messages(INITIAL_BUILDER_CAPACITY, &mut storage_engine);

        let previous_remaining_bytes = storage_engine.remaining_bytes.clone();
        storage_engine.get_finished_segment();

        assert!(previous_remaining_bytes < storage_engine.remaining_bytes);
    }

	/// Generate `count` data points for the same time series and insert them into `storage_engine`.
    /// Return the key, which is the same for all generated data points.
    fn insert_multiple_messages(count: usize, storage_engine: &mut StorageEngine) -> String {
        let mut key = String::new();

        for _ in 0..count {
            key = insert_generated_message(storage_engine);
        }

        key
    }

    /// Generate a data point and insert it into `storage_engine`. Return the data point key.
    fn insert_generated_message(storage_engine: &mut StorageEngine) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let payload = format!("[{}, 30]", timestamp);
        let message = Message::new("ModelarDB/test", payload, 1);

        storage_engine.insert_message(message.clone());

        DataPoint::from_message(&message)
            .unwrap()
            .generate_unique_key()
    }
}
