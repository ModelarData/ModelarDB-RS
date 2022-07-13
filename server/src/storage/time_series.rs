//! Module containing support for different kinds of stored time series.
//!
//! The main TimeSeriesBuilder struct provides support for inserting and storing data in a in-memory time
//! series. Furthermore, the data can be retrieved as a structured record batch. BufferedTimeSeries
//! provides a simple struct to keep track of time series that have been saved to a file buffer.
//! Similarly the data can be retrieved from the buffer as a record batch. Finally, the QueuedTimeSeries
//! struct provides a simple representation that can be inserted into a queue.
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
use std::{fmt, mem};
use std::fmt::Formatter;
use std::fs::File;
use std::sync::Arc;
use datafusion::arrow::array::{ArrayBuilder, Float32Array, PrimitiveBuilder, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Float32Type, Schema, TimestampMicrosecondType};
use datafusion::arrow::datatypes::TimeUnit::Microsecond;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::parquet::arrow::{ArrowReader, ParquetFileArrowReader, ProjectionMask};
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use crate::storage::data_point::DataPoint;
use crate::storage::{INITIAL_BUILDER_CAPACITY, MetaData, Timestamp, Value};

/// Struct representing a single time series being built, consisting of a series of timestamps and
/// values. Note that since array builders are used, the data can only be read once the builders are
/// finished and can not be further appended to after.
///
/// # Fields
/// * `timestamps` - Arrow array builder consisting of timestamps with microsecond precision.
/// * `values` - Arrow array builder consisting of float values.
/// * `metadata` - List of metadata used to uniquely identify the time series (and related sensor).
pub struct TimeSeriesBuilder {
    timestamps: PrimitiveBuilder<TimestampMicrosecondType>,
    values: PrimitiveBuilder<Float32Type>,
    pub metadata: MetaData,
}

impl fmt::Display for TimeSeriesBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&*format!("Time series with {} data point(s) (", self.timestamps.len()));
        f.write_str(&*format!("timestamp capacity: {}, ", self.timestamps.capacity()));
        f.write_str(&*format!("values capacity: {})", self.values.capacity()));

        Ok(())
    }
}

impl TimeSeriesBuilder {
    pub fn new(metadata: MetaData) -> TimeSeriesBuilder {
        TimeSeriesBuilder {
            // Note that the actual internal capacity might be slightly larger than these values. Apache
            // arrow defines the argument as being the lower bound for how many items the builder can hold.
            timestamps: TimestampMicrosecondArray::builder(INITIAL_BUILDER_CAPACITY),
            values: Float32Array::builder(INITIAL_BUILDER_CAPACITY),
            metadata,
        }
    }

    /// Return the size in bytes of the given time series. Note that only the size of the builders are considered.
    pub fn get_size(&self) -> usize {
        (mem::size_of::<Timestamp>() * self.timestamps.capacity())
            + (mem::size_of::<Value>() * self.values.capacity())
    }

    /// Check if there is enough memory available to create a new time series, initiate buffering if not.
    pub fn get_needed_memory_for_create() -> usize {
        let needed_bytes_timestamps = mem::size_of::<Timestamp>() * INITIAL_BUILDER_CAPACITY;
        let needed_bytes_values = mem::size_of::<Value>() * INITIAL_BUILDER_CAPACITY;

        needed_bytes_timestamps + needed_bytes_values
    }

    /// Check if an update will expand the capacity of the builders. If so, get the needed bytes for the new capacity.
    pub fn get_needed_memory_for_update(&self) -> usize {
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
    pub fn insert_data(&mut self, data_point: &DataPoint) {
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

pub struct BufferedTimeSeries {
    pub path: String,
    pub metadata: MetaData,
}

impl BufferedTimeSeries {
    fn get_data(&mut self) -> RecordBatch {
        let file = File::open(&self.path).unwrap();
        let file_reader = SerializedFileReader::new(file).unwrap();

        let file_metadata = file_reader.metadata().file_metadata();
        let mask = ProjectionMask::leaves(file_metadata.schema_descr(), [0]);

        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

        println!("Converted arrow schema is: {}", arrow_reader.get_schema().unwrap());
        println!("Arrow schema after projection is: {}",
                 arrow_reader.get_schema_by_columns(mask.clone()).unwrap());

        let mut unprojected = arrow_reader.get_record_reader(2048).unwrap();
        println!("Unprojected reader schema: {}", unprojected.schema());

        let mut record_batch_reader = arrow_reader.get_record_reader_by_columns(mask, 2048).unwrap();
        // TODO: Fix problem where the values are missing. This might be a print issue.
        record_batch_reader.next().unwrap().unwrap()
    }
}

pub struct QueuedTimeSeries {
    pub key: String,
    pub start_timestamp: Timestamp,
}