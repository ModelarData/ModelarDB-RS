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
use datafusion::arrow::array::{
    ArrayBuilder, Float32Array, PrimitiveArray, PrimitiveBuilder, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::TimeUnit::Microsecond;
use datafusion::arrow::datatypes::{
    DataType, Field, Float32Type, Schema, TimestampMicrosecondType,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Encoding;
use datafusion::parquet::file::properties::WriterProperties;
use paho_mqtt::Message;
use std::collections::{HashMap, VecDeque};
use std::fmt::Formatter;
use std::fs::File;
use std::sync::Arc;
use std::{fmt, mem};

type Timestamp = i64;
type Value = f32;
type MetaData = Vec<String>;

const RESERVED_MEMORY_BYTES: usize = 5000;
const BUFFER_COUNT: u16 = 1;
const INITIAL_BUILDER_CAPACITY: usize = 100;

#[derive(Debug)]
struct DataPoint {
    timestamp: Timestamp,
    value: Value,
    metadata: MetaData,
}

struct TimeSeries {
    timestamps: PrimitiveBuilder<TimestampMicrosecondType>,
    values: PrimitiveBuilder<Float32Type>,
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

impl TimeSeries {
    fn new(metadata: MetaData) -> TimeSeries {
        TimeSeries {
            // Note that the actual internal capacity might be slightly larger than these values. Apache
            // arrow defines the argument as being the lower bound for how many items the builder can hold.
            timestamps: TimestampMicrosecondArray::builder(INITIAL_BUILDER_CAPACITY),
            values: Float32Array::builder(INITIAL_BUILDER_CAPACITY),
            metadata,
        }
    }
}

struct BufferedTimeSeries {
    path: String,
    metadata: MetaData,
}

struct QueuedTimeSeries {
    key: String,
    start_timestamp: Timestamp,
}

/// Storage engine struct responsible for keeping track of all uncompressed data and invoking the
/// compressor to move the uncompressed data into persistent model-based storage. The fields should
/// not be directly modified and are therefore only changed when using "insert_data".
///
/// # Fields
/// * `data` - Hash map from the time series ID to the in-memory uncompressed data of the time series.
/// * `data_buffer` - Hash map from the time series ID to the path of the parquet file buffer.
/// * `compression_queue` - Prioritized queue of time series that can be compressed.
/// * `remaining_bytes` - Continuously updated tracker of how many of the reserved bytes are remaining.
pub struct StorageEngine {
    // TODO: Look into using a BTreeMap to avoid having both a compression queue and a data field.
    data: HashMap<String, TimeSeries>,
    data_buffer: HashMap<String, BufferedTimeSeries>,
    compression_queue: VecDeque<QueuedTimeSeries>,
    remaining_bytes: usize,
}

impl StorageEngine {
    pub fn new() -> Self {
        StorageEngine {
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            data: HashMap::new(),
            data_buffer: HashMap::new(),
            compression_queue: VecDeque::new(),
            remaining_bytes: RESERVED_MEMORY_BYTES,
        }
    }

    /// Format the given message and insert it into the in-memory storage.
    pub fn insert_message(&mut self, message: Message) {
        println!("Remaining bytes: {}", self.remaining_bytes);

        let mut needed_bytes = 0;
        let data_point = format_message(&message);
        let key = generate_unique_key(&data_point);

        println!("Inserting data point {:?} into key '{}'.", data_point, key);

        if let Some(time_series) = self.data.get_mut(&*key) {
            println!("Found existing time series with key '{}'.", key);

            update_time_series(&data_point, time_series);

            // If further updates will trigger reallocation of the builder, find how many bytes are required.
            needed_bytes = get_needed_memory_for_update(&time_series);
        } else {
            println!("Could not find time series with key '{}'. Creating time series.", key);

            self.manage_memory_use(get_needed_memory_for_create());

            let mut time_series = TimeSeries::new(data_point.metadata.to_vec());
            update_time_series(&data_point, &mut time_series);

            self.queue_time_series(key.clone(), data_point.timestamp);
            self.data.insert(key, time_series);
        }

        // TODO: Ideally this should happen before updating or immediately after.
        // Managing memory use for updating last to avoid problem with ownership of self.
        self.manage_memory_use(needed_bytes);
        println!() // Formatting newline.
    }

    /** Private Methods **/
    // TODO: We have to always be sure of the remaining bytes to avoid "leaking".
    // TODO: If all time series are buffered it should return to the initial reserved bytes.
    // TODO: Fix problem where the needed bytes are larger than the total freed bytes.
    /// Based on the given needed bytes, buffer data if necessary and update the remaining reserved bytes.
    fn manage_memory_use(&mut self, needed_bytes: usize) {
        if needed_bytes > self.remaining_bytes {
            println!("Not enough memory. Moving {} time series to data buffer.", BUFFER_COUNT);

            // Move the BUFFER_COUNT first time series from the compression queue to the data buffer.
            for _n in 0..BUFFER_COUNT {
                if let Some(queued_time_series) = self.compression_queue.pop_front() {
                    let key = &*queued_time_series.key;
                    println!("Moving time series with key '{}' to data buffer.", key);

                    // Finish the builders and write them to the parquet file buffer.
                    let mut time_series = self.data.get_mut(key).unwrap();
                    let size = get_size_of_time_series(time_series);

                    let timestamps = time_series.timestamps.finish();
                    let values = time_series.values.finish();
                    let path = format!(
                        "{}_{}.parquet",
                        key.replace("/", "-"),
                        queued_time_series.start_timestamp
                    );

                    write_data_to_parquet(timestamps, values, path.to_owned());

                    // Add the buffered time series to the data buffer hashmap to save the path.
                    let buffered_time_series = BufferedTimeSeries {
                        path: path.to_owned(),
                        metadata: time_series.metadata.to_vec(),
                    };

                    self.data_buffer.insert(key.to_owned(), buffered_time_series);

                    println!("Freeing {} bytes from the reserved memory.", size);
                    self.data.remove(key);

                    // Update the remaining bytes to reflect that data has been moved to the buffer.
                    self.remaining_bytes = self.remaining_bytes + size;
                }
            }
            // TODO: It might be necessary to shrink the hashmap to fit dependent on how it handles replacing with insert.
        }

        self.remaining_bytes = self.remaining_bytes - needed_bytes;
    }

    /// Push the time series referenced by the given key on to the compression queue.
    fn queue_time_series(&mut self, key: String, timestamp: Timestamp) {
        println!("Pushing time series with key '{}' to the back of the compression queue.", key);

        let queued_time_series = QueuedTimeSeries {
            key: key.clone(),
            start_timestamp: timestamp,
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
    let timestamp = timestamp_value[0].parse::<Timestamp>().unwrap();
    let value = timestamp_value[1].parse::<Value>().unwrap();

    DataPoint {
        timestamp,
        value,
        metadata: vec![message.topic().to_string()],
    }
}

// TODO: This could be moved to the data point struct implementation.
// TODO: Currently the only information we have to uniquely identify a sensor is the ID. If this changes, change this function.
/// Generates an unique key for a time series based on the information in the message.
fn generate_unique_key(data_point: &DataPoint) -> String {
    data_point.metadata.join("-")
}

/// Check if there is enough memory available to create a new time series, initiate buffering if not.
fn get_needed_memory_for_create() -> usize {
    let needed_bytes_timestamps = mem::size_of::<Timestamp>() * INITIAL_BUILDER_CAPACITY;
    let needed_bytes_values = mem::size_of::<Value>() * INITIAL_BUILDER_CAPACITY;

    needed_bytes_timestamps + needed_bytes_values
}

// TODO: This could be moved to the struct implementation.
/// Return the size in bytes of the given time series. Note that only the size of the builders are considered.
fn get_size_of_time_series(time_series: &TimeSeries) -> usize {
    (mem::size_of::<Timestamp>() * time_series.timestamps.capacity())
        + (mem::size_of::<Value>() * time_series.values.capacity())
}

// TODO: This could be moved to the struct implementation.
/// Check if an update will expand the capacity of the builders. If so, get the needed bytes for the new capacity.
fn get_needed_memory_for_update(time_series: &TimeSeries) -> usize {
    let len = time_series.timestamps.len();
    let mut needed_bytes_timestamps: usize = 0;
    let mut needed_bytes_values: usize = 0;

    // If the current length is equal to the capacity, adding one more value will trigger reallocation.
    if len == time_series.timestamps.capacity() {
        needed_bytes_timestamps = mem::size_of::<Timestamp>() * time_series.timestamps.capacity();
    }

    // Note that there is no guarantee that the timestamps capacity is equal to the values capacity.
    if len == time_series.values.capacity() {
        needed_bytes_values = mem::size_of::<Value>() * time_series.values.capacity();
    }

    needed_bytes_timestamps + needed_bytes_values
}

// TODO: This could be moved to the struct implementation.
/// Add the timestamp and value from the data point to the time series array builders.
fn update_time_series(data_point: &DataPoint, time_series: &mut TimeSeries) {
    time_series.timestamps.append_value(data_point.timestamp).unwrap();
    time_series.values.append_value(data_point.value).unwrap();

    println!("Inserted data point into {}.", time_series)
}

// TODO: This could *maybe* be moved to the struct implementation.
/// Write the given arrow arrays to a parquet file with the given path.
fn write_data_to_parquet(
    timestamps: PrimitiveArray<TimestampMicrosecondType>,
    values: PrimitiveArray<Float32Type>,
    path: String,
) {
    let schema = Schema::new(vec![
        Field::new("timestamps", DataType::Timestamp(Microsecond, None), false),
        Field::new("values", DataType::Float32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(timestamps), Arc::new(values)]
    ).unwrap();

    // Write the record batch to the parquet file buffer.
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
