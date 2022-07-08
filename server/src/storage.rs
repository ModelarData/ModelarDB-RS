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
use datafusion::arrow::array::{Float32Array, PrimitiveBuilder, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{Float32Type, TimestampMillisecondType};
use paho_mqtt::Message;

type TimeStamp = TimestampMillisecondType;
type Value = Float32Type;
type MetaData = Vec<String>;

#[derive(Debug)]
struct DataPoint {
    timestamp: i64,
    value: f32,
    metadata: MetaData
}

#[derive(Debug)]
struct TimeSeries {
    timestamps: PrimitiveBuilder<TimeStamp>,
    values: PrimitiveBuilder<Value>,
    metadata: MetaData
}

struct BufferedTimeSeries {
    path: String,
    metadata: MetaData
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
            // TODO: Maybe create with estimated capacity to avoid reallocation.
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

    /// Format the given message and insert it into the in-memory storage.
    pub fn insert_message(mut self, message: Message) {
        let data_point = format_message(&message);
        let key = generate_unique_key(&data_point);

        println!("Inserting data point {:?} into key {}", data_point, key);

        if let Some(time_series) = self.data.get_mut(&*key) {
            println!("Found existing time series with key {}", key);

            // If the key exists, add the timestamp and value to the builders.
            time_series.timestamps.append_value(data_point.timestamp).unwrap();
            time_series.values.append_value(data_point.value).unwrap();

            println!("Inserted data point into {:?}", time_series)
        } else {
            println!("Could not find time series with key {}", key);

            // If the key does not already exist, create a new entry.
            let mut time_series = TimeSeries {
                timestamps: TimestampMillisecondArray::builder(100),
                values: Float32Array::builder(100),
                metadata: data_point.metadata
            };

            time_series.timestamps.append_value(data_point.timestamp).unwrap();
            time_series.values.append_value(data_point.value).unwrap();

            println!("Inserted data point into {:?}", time_series);

            self.data.insert(key, time_series);
        }
    }
    // TODO: When it is formatted it should be inserted in to the data field.
    // TODO: If it already exists it should be just be appended to the builders.

    // TODO: If it does not exist a new entry should be added that also adds the metadata.
    // TODO: It should also be pushed onto the "to-be-compressed" queue.
}

/// Given a raw MQTT message, extract the message components and return them as a data point.
fn format_message(message: &Message) -> DataPoint {
    let message_payload = message.payload_str();
    let first_last_off: &str = &message_payload[1..message_payload.len() - 1];

    let timestamp_value: Vec<&str> = first_last_off.split(", ").collect();
    let timestamp = timestamp_value[0].parse::<i64>().unwrap();
    let value = timestamp_value[1].parse::<f32>().unwrap();

    DataPoint {
        timestamp,
        value,
        metadata: vec![message.topic().to_string()]
    }
}

// TODO: Currently the only information we have to uniquely identify a sensor is the ID. If this changes, change this function.
/// Generates an unique key for a time series based on the information in the message.
fn generate_unique_key(data_point: &DataPoint) -> String {
    data_point.metadata.join("-")
}