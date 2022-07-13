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
//!
//! The interface for interacting with the storage engine is the public "StorageEngine" struct that
//! exposes the public "new" and "insert_data" functions. The storage engine should always be
//! initialized with "StorageEngine::new()". Using "insert_data", sensor data can be inserted into
//! the engine where it is further processed to reach the final compressed state.

mod data_point;
mod time_series;

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Encoding;
use datafusion::parquet::file::properties::WriterProperties;
use paho_mqtt::Message;
use crate::storage::data_point::DataPoint;
use crate::storage::time_series::{BufferedTimeSeries, QueuedTimeSeries, TimeSeriesBuilder};

type Timestamp = i64;
type Value = f32;
type MetaData = Vec<String>;

const RESERVED_MEMORY_BYTES: usize = 3500;
const BUFFER_COUNT: u16 = 1;
const INITIAL_BUILDER_CAPACITY: usize = 100;

/// Keeping track of all uncompressed data, either in memory or in a file buffer. Also provides a
/// queue to prioritize data for compression. The fields should not be directly modified and are
/// therefore only changed when using "insert_data".
pub struct StorageEngine {
    /// The uncompressed time series while they are being built.
    data: HashMap<String, TimeSeriesBuilder>,
    /// The uncompressed time series, saved in a parquet file buffer.
    data_buffer: HashMap<String, BufferedTimeSeries>,
    /// Prioritized queue of time series that can be compressed.
    compression_queue: VecDeque<QueuedTimeSeries>,
    /// Continuously updated tracker of how many of the reserved bytes are remaining.
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
            // TODO: Fix the problem where if we increase capacity but does use the space yet and that ts is then
            //       buffered. The extra space is not accounted for.
            // If further updates will trigger reallocation of the builder, ensure there is enough memory.
            let needed_bytes = time_series.get_needed_memory_for_update();
            self.manage_memory_use(needed_bytes);
        } else {
            println!("Could not find time series with key '{}'. Creating time series.", key);

            let needed_bytes = TimeSeriesBuilder::get_needed_memory_for_create();
            self.manage_memory_use(needed_bytes);

            let mut time_series = TimeSeriesBuilder::new(data_point.metadata.to_vec());
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
                    let mut buffered_time_series = BufferedTimeSeries {
                        path: path.to_owned(),
                        metadata: time_series.metadata.to_vec(),
                    };

                    // TODO: When we have two buffered time series from the same sensor we have a key duplicate. Fix this.
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

// TODO: create a folder for the buffered data.
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
