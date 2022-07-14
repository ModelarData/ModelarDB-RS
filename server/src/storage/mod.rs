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
mod time_series;

use std::collections::{HashMap, VecDeque};
use datafusion::arrow::record_batch::RecordBatch;
use paho_mqtt::Message;
use crate::storage::data_point::DataPoint;
use crate::storage::time_series::{TimeSeriesBuilder};

type Timestamp = i64;
type Value = f32;
type MetaData = Vec<String>;

const INITIAL_BUILDER_CAPACITY: usize = 50;

/// Keeping track of all uncompressed data, either in memory or in a file buffer. The fields should
/// not be directly modified and are therefore only changed when using "insert_data".
pub struct StorageEngine {
    /// The uncompressed time series while they are being built.
    data: HashMap<String, TimeSeriesBuilder>,
    /// Prioritized queue of finished time series that are ready for compression.
    compression_queue: VecDeque<TimeSeriesBuilder>
}

impl StorageEngine {
    pub fn new() -> Self {
        StorageEngine {
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            data: HashMap::new(),
            compression_queue: VecDeque::new()
        }
    }

    /// Format the given message and insert it into the in-memory storage.
    pub fn insert_message(&mut self, message: Message) {
        let data_point = DataPoint::from_message(&message);
        let key = data_point.generate_unique_key();

        println!("Inserting data point {:?} into key '{}'.", data_point, key);

        if let Some(time_series) = self.data.get_mut(&*key) {
            println!("Found existing time series with key '{}'.", key);

            time_series.insert_data(&data_point);

            if time_series.is_full() {
                println!("Time series is full, moving it to the compression queue.");

                let finished_time_series = self.data.remove(&*key).unwrap();
                self.compression_queue.push_back(finished_time_series);
            }
        } else {
            println!("Could not find time series with key '{}'. Creating time series.", key);

            let mut time_series = TimeSeriesBuilder::new(&data_point);
            time_series.insert_data(&data_point);

            self.data.insert(key, time_series);
        }
    }

    /// If possible, return the oldest finished time series from the compression queue.
    pub fn get_finished_time_series(&mut self) -> Option<TimeSeriesBuilder> {
        self.compression_queue.pop_front()
    }
}