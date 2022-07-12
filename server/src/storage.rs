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
mod data_point;

use datafusion::arrow::array::{
    ArrayBuilder, Float32Array, PrimitiveBuilder, TimestampMicrosecondArray,
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
use crate::storage::data_point::DataPoint;

type Timestamp = i64;
type Value = f32;
type MetaData = Vec<String>;

const RESERVED_MEMORY_BYTES: usize = 3500;
const BUFFER_COUNT: u16 = 1;
const INITIAL_BUILDER_CAPACITY: usize = 100;

// TODO: Move time series structs into separate file.
// TODO: Maybe also move data point struct and format message function into separate file.
/// Struct representing a single time series consisting of a series of timestamps and values.
/// Note that since array builders are used, the data can only be read once the builders are
/// finished and can not be further appended to after.
///
/// # Fields
/// * `timestamps` - Arrow array builder consisting of timestamps with microsecond precision.
/// * `values` - Arrow array builder consisting of float values.
/// * `metadata` - List of metadata used to uniquely identify the time series (and related sensor).
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

    /// Return the size in bytes of the given time series. Note that only the size of the builders are considered.
    fn get_size(&self) -> usize {
        (mem::size_of::<Timestamp>() * self.timestamps.capacity())
            + (mem::size_of::<Value>() * self.values.capacity())
    }

    /// Check if there is enough memory available to create a new time series, initiate buffering if not.
    fn get_needed_memory_for_create() -> usize {
        let needed_bytes_timestamps = mem::size_of::<Timestamp>() * INITIAL_BUILDER_CAPACITY;
        let needed_bytes_values = mem::size_of::<Value>() * INITIAL_BUILDER_CAPACITY;

        needed_bytes_timestamps + needed_bytes_values
    }

    /// Check if an update will expand the capacity of the builders. If so, get the needed bytes for the new capacity.
    fn get_needed_memory_for_update(&self) -> usize {
        let len = self.timestamps.len();
        let mut needed_bytes_timestamps: usize = 0;
        let mut needed_bytes_values: usize = 0;

        // If the current length is equal to the capacity, adding one more value will trigger reallocation.
        if len == self.timestamps.capacity() {
            needed_bytes_timestamps = mem::size_of::<Timestamp>() * self.timestamps.capacity();
        }

        // Note that there is no guarantee that the timestamps capacity is equal to the values capacity.
        if len == self.values.capacity() {
            needed_bytes_values = mem::size_of::<Value>() * self.values.capacity();
        }

        needed_bytes_timestamps + needed_bytes_values
    }

    /// Add the timestamp and value from the data point to the time series array builders.
    fn insert_data(&mut self, data_point: &DataPoint) {
        self.timestamps.append_value(data_point.timestamp).unwrap();
        self.values.append_value(data_point.value).unwrap();

        println!("Inserted data point into {}.", self)
    }

    /// Finishes the array builders and returns the data in a structured record batch.
    fn get_data(&mut self) -> RecordBatch {
        let timestamps = self.timestamps.finish();
        let values = self.values.finish();

        let schema = Schema::new(vec![
            Field::new("timestamps", DataType::Timestamp(Microsecond, None), false),
            Field::new("values", DataType::Float32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(timestamps), Arc::new(values)]
        ).unwrap()
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

/// Struct responsible for keeping track of all uncompressed data, either in memory or in a file buffer.
/// The struct also provides a queue to prioritize data for compression. The fields should
/// not be directly modified and are therefore only changed when using "insert_data".
///
/// # Fields
/// * `data` - Hash map from the time series ID to the in-memory uncompressed data of the time series.
/// * `data_buffer` - Hash map from the time series ID to the path of the parquet file buffer.
/// * `compression_queue` - Prioritized queue of time series that can be compressed.
/// * `remaining_bytes` - Continuously updated tracker of how many of the reserved bytes are remaining.
pub struct StorageEngine {
    data: HashMap<String, TimeSeries>,
    data_buffer: HashMap<String, BufferedTimeSeries>,
    compression_queue: VecDeque<QueuedTimeSeries>,
    remaining_bytes: usize,
}

// TODO: For compression to work we need a way to access the compression queue.
// TODO: The field is private so we need a method that pops the first item, retrieves the data from
//       in-memory or the buffer and returns it in a record batch.
// TODO: We also need support saving the compression result. This should also be on the storage engine.
impl StorageEngine {
    pub fn new() -> Self {
        StorageEngine {
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            data: HashMap::new(),
            // TODO: Instead of having to look in two different places when getting the data for
            //       compression, have a single hashmap with elements with a trait (rust book).
            //       The trait should have a function to get the data and the compressor can then just
            //       use this function. This would have a different implementation for buffered and normal.
            data_buffer: HashMap::new(),
            compression_queue: VecDeque::new(),
            remaining_bytes: RESERVED_MEMORY_BYTES,
        }
    }

    /// Format the given message and insert it into the in-memory storage.
    pub fn insert_message(&mut self, message: Message) {
        println!("Remaining bytes: {}", self.remaining_bytes);

        let data_point = DataPoint::from_message(&message);
        let key = data_point.generate_unique_key();

        println!("Inserting data point {:?} into key '{}'.", data_point, key);

        if let Some(time_series) = self.data.get_mut(&*key) {
            println!("Found existing time series with key '{}'.", key);

            time_series.insert_data(&data_point);

            // If further updates will trigger reallocation of the builder, ensure there is enough memory.
            let needed_bytes = time_series.get_needed_memory_for_update();
            self.manage_memory_use(needed_bytes);
        } else {
            println!("Could not find time series with key '{}'. Creating time series.", key);

            let needed_bytes = TimeSeries::get_needed_memory_for_create();
            self.manage_memory_use(needed_bytes);

            let mut time_series = TimeSeries::new(data_point.metadata.to_vec());
            time_series.insert_data(&data_point);

            self.queue_time_series(key.clone(), data_point.timestamp);
            self.data.insert(key, time_series);
        }

        println!() // Formatting newline.
    }

    // TODO: We have to always be sure of the remaining bytes to avoid "leaking".
    // TODO: If all time series are buffered it should return to the initial reserved bytes.
    // TODO: Fix problem where the needed bytes are larger than the total freed bytes.
    /// Based on the given needed bytes, buffer data if necessary and update the remaining reserved bytes.
    fn manage_memory_use(&mut self, needed_bytes: usize) {
        if needed_bytes > self.remaining_bytes {
            println!("Not enough memory. Moving {} time series to data buffer.", BUFFER_COUNT);

            // Move the BUFFER_COUNT first time series from the compression queue to the data buffer.
            for _n in 0..BUFFER_COUNT {
                // TODO: We should not pop since they still need to be compressed.
                // TODO: We also need to find the first BUFFER_COUNT elements that are not buffered yet.
                if let Some(queued_time_series) = self.compression_queue.pop_front() {
                    let key = &*queued_time_series.key;

                    let path = format!(
                        "{}_{}.parquet",
                        key.replace("/", "-"),
                        queued_time_series.start_timestamp
                    );

                    let mut time_series = self.data.get_mut(key).unwrap();
                    let ts_size = time_series.get_size();

                    // Finish the builders and write them to the parquet file buffer.
                    let batch = time_series.get_data();
                    write_batch_to_parquet(batch, path.to_owned());

                    // Add the buffered time series to the data buffer hashmap to save the path.
                    let buffered_time_series = BufferedTimeSeries {
                        path: path.to_owned(),
                        metadata: time_series.metadata.to_vec(),
                    };

                    self.data_buffer.insert(key.to_owned(), buffered_time_series);

                    println!("Freeing {} bytes from the reserved memory.", ts_size);
                    self.data.remove(key);

                    // Update the remaining bytes to reflect that data has been moved to the buffer.
                    self.remaining_bytes = self.remaining_bytes + ts_size;
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

/// Write the given record batch to a parquet file with the given path.
fn write_batch_to_parquet(batch: RecordBatch, path: String) {
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
