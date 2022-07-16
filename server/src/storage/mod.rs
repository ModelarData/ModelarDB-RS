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
use crate::storage::segment::SegmentBuilder;
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

const INITIAL_BUILDER_CAPACITY: usize = 50;

/// Keeping track of all uncompressed data, either in memory or in a file buffer. The data field should
/// not be directly modified and is therefore only changed when using "insert_data".
pub struct StorageEngine {
    /// The uncompressed segments while they are being built.
    data: HashMap<String, SegmentBuilder>,
    /// Prioritized queue of finished segments that are ready for compression.
    compression_queue: VecDeque<SegmentBuilder>,
}

impl StorageEngine {
    pub fn new() -> Self {
        StorageEngine {
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            data: HashMap::new(),
            compression_queue: VecDeque::new(),
        }
    }

    /// Format the given message and insert it into the in-memory storage.
    pub fn insert_message(&mut self, message: Message) {
        match DataPoint::from_message(&message) {
            Ok(data_point) => {
                let key = data_point.generate_unique_key();

                println!("Inserting data point {:?} into key '{}'.", data_point, key);

                if let Some(segment) = self.data.get_mut(&*key) {
                    println!("Found existing segment with key '{}'.", key);

                    segment.insert_data(&data_point);

                    if segment.is_full() {
                        println!("Segment is full, moving it to the compression queue.");

                        let finished_segment = self.data.remove(&*key).unwrap();
                        self.compression_queue.push_back(finished_segment);
                    }
                } else {
                    println!(
                        "Could not find segment with key '{}'. Creating segment.",
                        key
                    );

                    let mut segment = SegmentBuilder::new(&data_point);
                    segment.insert_data(&data_point);

                    self.data.insert(key, segment);
                }
            }
            Err(e) => eprintln!("Message could not be inserting into storage: {:?}", e),
        }
    }

    /// If possible, return the oldest finished segment from the compression queue.
    pub fn get_finished_segment(&mut self) -> Option<SegmentBuilder> {
        self.compression_queue.pop_front()
    }

    /// Write `data` to persistent parquet file storage.
    pub fn save_compressed_data(key: String, data: RecordBatch) {
        fs::create_dir_all("compressed");

        let path = format!("{}/{}.parquet", folder_name, key);
        write_batch_to_parquet(data, path);
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
        let mut key = String::new();
        let mut storage_engine = StorageEngine::new();

        for _ in 0..2 {
            key = insert_generated_message(&mut storage_engine);
        }

        assert!(storage_engine.data.contains_key(&key));
        assert_eq!(storage_engine.data.get(&key).unwrap().get_length(), 2);
    }

    #[test]
    fn test_can_get_finished_segment_when_finished() {
        let mut key = String::new();
        let mut storage_engine = StorageEngine::new();

        for _ in 0..INITIAL_BUILDER_CAPACITY * 2 {
            key = insert_generated_message(&mut storage_engine);
        }

        assert!(storage_engine.get_finished_segment().is_some());
    }

    #[test]
    fn test_can_get_multiple_finished_segments_when_multiple_finished() {
        let mut key = String::new();
        let mut storage_engine = StorageEngine::new();

        for _ in 0..INITIAL_BUILDER_CAPACITY * 3 {
            key = insert_generated_message(&mut storage_engine);
        }

        assert!(storage_engine.get_finished_segment().is_some());
        assert!(storage_engine.get_finished_segment().is_some());
    }

    #[test]
    fn test_cannot_get_finished_segment_when_not_finished() {
        let mut storage_engine = StorageEngine::new();

        assert!(storage_engine.get_finished_segment().is_none());
    }
}
