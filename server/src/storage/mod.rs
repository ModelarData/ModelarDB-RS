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
}

impl StorageEngine {
    pub fn new() -> Self {
        StorageEngine {
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            data: HashMap::new(),
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
        } else {
            println!("Could not find time series with key '{}'. Creating time series.", key);

            let mut time_series = TimeSeriesBuilder::new(data_point.metadata.to_vec());
            time_series.insert_data(&data_point);

            self.data.insert(key, time_series);
        }

        println!() // Formatting newline.
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
