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

//! Converts raw MQTT messages to uncompressed data points and stores uncompressed data points
//! temporarily in an in-memory buffer.

mod data_point;
mod segment;

use std::collections::vec_deque::VecDeque;
use std::collections::HashMap;

use paho_mqtt::Message;

use crate::storage::data_point::DataPoint;
use crate::storage::segment::SegmentBuilder;

type MetaData = Vec<String>;

const INITIAL_BUILDER_CAPACITY: usize = 50;

/// Keeping track of all uncompressed data, both while being built and when finished.
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

    /// Parse `message` and insert it into the in-memory buffer.
    pub fn insert_message(&mut self, message: Message) {
        match DataPoint::from_message(&message) {
            Ok(data_point) => {
                let key = data_point.generate_unique_key();

                println!("Inserting data point {:?} into key '{}'.", data_point, key);

                if let Some(segment) = self.data.get_mut(&key) {
                    println!("Found existing segment with key '{}'.", key);

                    segment.insert_data(&data_point);

                    if segment.is_full() {
                        println!("Segment is full, moving it to the compression queue.");

                        let finished_segment = self.data.remove(&key).unwrap();
                        self.compression_queue.push_back(finished_segment);
                    }
                } else {
                    println!("Could not find segment with key '{}'. Creating segment.", key);

                    let mut segment = SegmentBuilder::new(&data_point);
                    segment.insert_data(&data_point);

                    self.data.insert(key, segment);
                }
            }
            Err(e) => eprintln!("Message could not be inserted into storage: {:?}", e),
        }
    }

    /// Remove the oldest finished segment from the compression queue and return it. Return `None`
    /// if the compression queue is empty.
    pub fn get_finished_segment(&mut self) -> Option<SegmentBuilder> {
        self.compression_queue.pop_front()
    }
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

    fn insert_generated_message(storage_engine: &mut StorageEngine) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let payload = format!("[{}, 30]", timestamp);
        let message = Message::new("ModelarDB/test", payload, 1);

        storage_engine.insert_message(message.clone());

        DataPoint::from_message(&message).unwrap().generate_unique_key()
    }

    #[test]
    fn test_cannot_get_finished_segment_when_not_finished() {
        let mut storage_engine = StorageEngine::new();

        assert!(storage_engine.get_finished_segment().is_none());
    }
}
