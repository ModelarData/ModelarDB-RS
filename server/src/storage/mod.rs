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

//! Support for formatting uncompressed data, storing uncompressed data both
//! in-memory and in a parquet file data buffer, and storing compressed data.

mod data_point;
mod segment;

use crate::storage::data_point::DataPoint;
use crate::storage::segment::{BufferedSegment, FinishedSegment, SegmentBuilder, UncompressedSegment};
use paho_mqtt::Message;
use std::collections::vec_deque::VecDeque;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Encoding;
use datafusion::parquet::file::properties::WriterProperties;

type Timestamp = i64;
type Value = f32;
type MetaData = Vec<String>;

// TODO: This should be dynamic.
const SENSOR_COUNT: usize = 2;

const INITIAL_BUILDER_CAPACITY: usize = 50;
const RESERVED_BYTES: usize = 5000;

/// Keeping track of all uncompressed data, either in memory or in a file buffer. The data field should
/// not be directly modified and is therefore only changed when using "insert_data".
pub struct StorageEngine {
    /// The uncompressed segments while they are being built.
    data: HashMap<String, SegmentBuilder>,
    /// Prioritized queue of finished segments that are ready for compression.
    compression_queue: VecDeque<FinishedSegment>,
    /// How many bytes of memory that are left for storing finished uncompressed segments.
    remaining_bytes: usize,
}

impl StorageEngine {
    pub fn new() -> Self {
        // Based on the sensor count and builder capacity, calculate the total bytes needed to store all builders.
        // Since the builder capacity is only a lower bound, we need to create a builder to get the actual size.
        let temp_builder = SegmentBuilder::new();
        let builder_size = temp_builder.get_memory_size();

        StorageEngine {
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            data: HashMap::new(),
            compression_queue: VecDeque::new(),
            remaining_bytes: RESERVED_BYTES - (builder_size * SENSOR_COUNT)
        }
    }

    /// Format `message` and insert it into the in-memory storage.
    pub fn insert_message(&mut self, message: Message) {
        match DataPoint::from_message(&message) {
            Ok(data_point) => {
                let key = data_point.generate_unique_key();

                println!("Inserting data point {:?} into segment with key '{}'.", data_point, key);

                if let Some(segment) = self.data.get_mut(&*key) {
                    println!("Found existing segment with key '{}'.", key);

                    segment.insert_data(&data_point);

                    if segment.is_full() {
                        println!("Segment is full, moving it to the compression queue.");

                        let full_segment = self.data.remove(&*key).unwrap();
                        self.queue_segment(key, full_segment)
                    }
                } else {
                    println!("Could not find segment with key '{}'. Creating segment.", key);

                    let mut segment = SegmentBuilder::new();
                    segment.insert_data(&data_point);

                    self.data.insert(key, segment);
                }
            }
            Err(e) => eprintln!("Message could not be inserted into storage: {:?}", e),
        }
    }

    /// If possible, return the oldest finished segment from the compression queue.
    pub fn get_finished_segment(&mut self) -> Option<FinishedSegment> {
        if let Some(finished_segment) = self.compression_queue.pop_front() {
            // Add the memory size of the removed finished segment back to the remaining bytes.
            self.remaining_bytes += finished_segment.uncompressed_segment.get_memory_size();

            Some(finished_segment)
        } else {
            None
        }
    }

    /// Write `data` to persistent parquet file storage.
    pub fn save_compressed_data(key: String, first_timestamp: Timestamp, data: RecordBatch) {
        let folder_path = format!("compressed/{}", key);
        fs::create_dir_all(&folder_path);

        let path = format!("{}/{}.parquet", folder_path, first_timestamp);
        write_batch_to_parquet(data, path);
    }

    /// Move `segment_builder` to the the compression queue. If necessary, buffer the data first.
    fn queue_segment(&mut self, key: String, segment_builder: SegmentBuilder) {
        println!("Remaining bytes: {}", self.remaining_bytes);

        let uncompressed_segment: Box<dyn UncompressedSegment>;
        let builder_size = segment_builder.get_memory_size();

        // If there is not enough space for the finished segment, buffer the data first.
        if builder_size > self.remaining_bytes {
            println!("Not enough memory for the finished segment. Buffering the segment.");

            let buffered = BufferedSegment::new(key.clone(), segment_builder);
            uncompressed_segment = Box::new(buffered);
        } else {
            uncompressed_segment = Box::new(segment_builder);

            // If not buffering the data, remove the size of the finished segment from the remaining bytes.
            self.remaining_bytes -= builder_size;
        }

        let queued_segment = FinishedSegment { key, uncompressed_segment };
        self.compression_queue.push_back(queued_segment);
    }
}

/// Write `batch` to a parquet file at the location given by `path`.
fn write_batch_to_parquet(batch: RecordBatch, path: String) {
    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        // TODO: Test using more efficient encoding. Plain encoding makes it easier to read the files externally.
        .set_encoding(Encoding::PLAIN)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

    writer.write(&batch).expect("Writing batch.");
    writer.close().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Generate a random data point and insert it into `storage_engine`. Return the data point key.
    fn insert_generated_message(storage_engine: &mut StorageEngine) -> String {
        let value = rand::thread_rng().gen_range(0..100);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let payload = format!("[{}, {}]", timestamp, value);
        let message = Message::new("ModelarDB/test", payload, 1);

        storage_engine.insert_message(message.clone());

        DataPoint::from_message(&message).unwrap().generate_unique_key()
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
        let key = insert_multiple_messages(INITIAL_BUILDER_CAPACITY * 2, &mut storage_engine);

        assert!(storage_engine.get_finished_segment().is_some());
    }

    #[test]
    fn test_can_get_multiple_finished_segments_when_multiple_finished() {
        let mut storage_engine = StorageEngine::new();
        let key = insert_multiple_messages(INITIAL_BUILDER_CAPACITY * 3, &mut storage_engine);

        assert!(storage_engine.get_finished_segment().is_some());
        assert!(storage_engine.get_finished_segment().is_some());
    }

    #[test]
    fn test_cannot_get_finished_segment_when_not_finished() {
        let mut storage_engine = StorageEngine::new();

        assert!(storage_engine.get_finished_segment().is_none());
    }

    #[test]
    fn test_segment_buffered_when_out_of_memory() {
        // TODO: Implement this test. This requires I/O.
    }

    #[test]
    fn test_remaining_bytes_decremented_when_queuing_in_memory() {
        let mut storage_engine = StorageEngine::new();
        let initial_remaining_bytes = storage_engine.remaining_bytes.clone();
        let key = insert_multiple_messages(INITIAL_BUILDER_CAPACITY * 2, &mut storage_engine);

        assert!(initial_remaining_bytes > storage_engine.remaining_bytes);
    }

    #[test]
    fn test_remaining_bytes_incremented_when_popping_in_memory() {
        let mut storage_engine = StorageEngine::new();
        let key = insert_multiple_messages(INITIAL_BUILDER_CAPACITY * 2, &mut storage_engine);

        let previous_remaining_bytes = storage_engine.remaining_bytes.clone();
        storage_engine.get_finished_segment();

        assert!(previous_remaining_bytes < storage_engine.remaining_bytes);
    }

    #[test]
    fn test_remaining_bytes_not_incremented_when_popping_buffered() {
        // TODO: Implement this test. This requires I/O.
    }
}
