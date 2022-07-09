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
use datafusion::arrow::array::{ArrayBuilder, Float32Array, PrimitiveBuilder, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{Float32Type, TimestampMillisecondType};
use paho_mqtt::Message;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fmt::{Formatter, Write};

type TimeStamp = TimestampMillisecondType;
type Value = Float32Type;
type MetaData = Vec<String>;

#[derive(Debug)]
struct DataPoint {
    timestamp: i64,
    value: f32,
    metadata: MetaData,
}

struct TimeSeries {
    timestamps: PrimitiveBuilder<TimeStamp>,
    values: PrimitiveBuilder<Value>,
    metadata: MetaData,
}

impl fmt::Display for TimeSeries {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&*format!("Time series with {} data point(s) (", self.timestamps.len()));
        f.write_str(&*format!("timestamp capacity: {}, ", self.timestamps.capacity()));
        f.write_str(&*format!("values capacity: {})", self.values.capacity()));

        Ok(())
    }
}

struct BufferedTimeSeries {
    path: String,
    metadata: MetaData,
}

struct QueuedTimeSeries {
    key: String,
    start_timestamp: i64,
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
    compression_queue: VecDeque<QueuedTimeSeries>,
}

impl Default for StorageEngine {
    fn default() -> Self {
        StorageEngine {
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            data: HashMap::new(),
            data_buffer: HashMap::new(),
            compression_queue: VecDeque::new(),
        }
    }
}

// TODO: Fix use of types so it uses the type alias for timestamps and values instead of i64 and f32.
impl StorageEngine {
    pub fn new() -> Self {
        Default::default()
    }

    /// Format the given message and insert it into the in-memory storage.
    pub fn insert_message(&mut self, message: Message) {
        let data_point = format_message(&message);
        let key = generate_unique_key(&data_point);

        println!("Inserting data point {:?} into key '{}'.", data_point, key);

        if let Some(time_series) = self.data.get_mut(&*key) {
            println!("Found existing time series with key '{}'.", key);

            update_time_series(&data_point, time_series);
        } else {
            println!("Could not find time series with key '{}'. Creating time series.", key);
            let time_series = create_time_series(&data_point);

            self.queue_time_series(key.clone(), data_point.timestamp);
            self.data.insert(key, time_series);

            // TODO: Check if the current memory use is larger than the threshold.
            // TODO: If so, move the first n (start with n=1) to the parquet data buffer.
            // TODO: Maybe keep track how many messages until we should check again to avoid issue with no new time series causing no checks.
        }

        println!() // Formatting newline.
    }

    /// Push the time series referenced by the given key on to the compression queue.
    fn queue_time_series(&mut self, key: String, timestamp: i64) {
        println!("Pushing time series with key '{}' to the back of the compression queue.", key);

        let queued_time_series = QueuedTimeSeries {
            key: key.clone(),
            start_timestamp: timestamp
        };

        self.compression_queue.push_back(queued_time_series);
    }
}

/** Private Functions **/
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
        metadata: vec![message.topic().to_string()],
    }
}

// TODO: Currently the only information we have to uniquely identify a sensor is the ID. If this changes, change this function.
/// Generates an unique key for a time series based on the information in the message.
fn generate_unique_key(data_point: &DataPoint) -> String {
    data_point.metadata.join("-")
}

/// Create a new time series struct and add the timestamp and value to the time series array builders.
fn create_time_series(data_point: &DataPoint) -> TimeSeries {
    let mut time_series = TimeSeries {
        timestamps: TimestampMillisecondArray::builder(100),
        values: Float32Array::builder(100),
        metadata: data_point.metadata.to_vec(),
    };

    update_time_series(&data_point, &mut time_series);
    time_series
}

/// Add the timestamp and value from the data point to the time series array builders.
fn update_time_series(data_point: &DataPoint, time_series: &mut TimeSeries) {
    time_series.timestamps.append_value(data_point.timestamp).unwrap();
    time_series.values.append_value(data_point.value).unwrap();

    println!("Inserted data point into {}.", time_series)
}
