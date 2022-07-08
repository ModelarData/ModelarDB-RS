//! Module containing support for formatting uncompressed data, storing uncompressed data both
//! in-memory and in a parquet file data buffer, and storing compressed data.
//!
//! The interface for interacting with the storage engine is the public "StorageEngine" struct that
//! exposes the public "new" and "insert_data" functions. The storage engine should always be
//! initialized with "StorageEngine::new()". Using "insert_data", sensor data can be inserted into
//! the engine where it is further processed to reach the final compressed state.
//!
/* Copyright 2021 The MiniModelarDB Contributors
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
use std::collections::{HashMap, VecDeque};
use datafusion::arrow::array::{PrimitiveBuilder};
use datafusion::arrow::datatypes::{Float32Type, TimestampMillisecondType};

type TimeStamp = TimestampMillisecondType;
type Value = Float32Type;

struct TimeSeries {
    timestamps: PrimitiveBuilder<TimeStamp>,
    values: PrimitiveBuilder<Value>,
    metadata: Vec<String>,
}

struct BufferedTimeSeries {
    path: String,
    metadata: Vec<String>,
}

struct QueuedTimeSeries {
    key: String,
    start_timestamp: TimeStamp,
}

/// Storage engine struct responsible for keeping track of all uncompressed data and invoking the
/// compressor to move the uncompressed data into persistent model-based storage. The fields should
/// not be directly modified and are therefore only changed when using "insert_data".
///
/// # Fields
/// * `data` - Hash map from the time series ID to the in-memory uncompressed data of the time series.
/// * `data_buffer` - Hash map from the time series ID to the path of the parquet file buffer.
/// * `compression_queue` - Prioritized queue of time series that can be compressed.
pub struct StorageEngine {
    data: HashMap<String, TimeSeries>,
    data_buffer: HashMap<String, BufferedTimeSeries>,
    compression_queue: VecDeque<QueuedTimeSeries>
}

impl Default for StorageEngine {
    fn default() -> Self {
        StorageEngine {
            data: HashMap::new(),
            data_buffer: HashMap::new(),
            compression_queue: VecDeque::new()
        }
    }
}

impl StorageEngine {
    pub fn new() -> Self {
        Default::default()
    }

    // TODO: Create a function to receive new messages.
    // TODO: Convert the message to the internal format used in the storage engine.
    // TODO: When it is formatted it should be inserted in to the data field.
    // TODO: If it already exists it should be just be appended to the builders.

    // TODO: If it does not exist a new entry should be added that also adds the metadata.
    // TODO: It should also be pushed onto the "to-be-compressed" queue.
}

