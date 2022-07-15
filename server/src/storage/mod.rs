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
use datafusion::arrow::record_batch::RecordBatch;
use paho_mqtt::Message;
use std::collections::{HashMap};
use std::collections::vec_deque::VecDeque;

type Timestamp = i64;
type Value = f32;
type MetaData = Vec<String>;

const INITIAL_BUILDER_CAPACITY: usize = 50;

/// Keeping track of all uncompressed data, either in memory or in a file buffer. The fields should
/// not be directly modified and are therefore only changed when using "insert_data".
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
        let data_point = DataPoint::from_message(&message);
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
            println!("Could not find segment with key '{}'. Creating segment.", key);

            let mut segment = SegmentBuilder::new(&data_point);
            segment.insert_data(&data_point);

            self.data.insert(key, segment);
        }
    }

    /// If possible, return the oldest finished segment from the compression queue.
    pub fn get_finished_segment(&mut self) -> Option<SegmentBuilder> {
        self.compression_queue.pop_front()
    }
}

// TODO: Test for inserting a message into a segment that does not exist.
// TODO: Test for inserting a message into a segment that does exist.
// TODO: Test for inserting a message into a segment that becomes full.
// TODO: Test for getting a finished segment while there is none.
// TODO: Test for getting a finished segment when they are at least one.
