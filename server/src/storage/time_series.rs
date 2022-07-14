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

//! Support for different kinds of stored time series.
//!
//! The main TimeSeriesBuilder struct provides support for inserting and storing data in a in-memory time
//! series. Furthermore, the data can be retrieved as a structured record batch. BufferedTimeSeries
//! provides a simple struct to keep track of time series that have been saved to a file buffer.
//! Similarly the data can be retrieved from the buffer as a record batch. Finally, the QueuedTimeSeries
//! struct provides a simple representation that can be inserted into a queue.

use std::{fmt};
use std::fmt::Formatter;
use std::sync::Arc;
use datafusion::arrow::array::{ArrayBuilder, Float32Array, PrimitiveBuilder, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Float32Type, Schema, TimestampMicrosecondType};
use datafusion::arrow::datatypes::TimeUnit::Microsecond;
use datafusion::arrow::record_batch::{RecordBatch};
use crate::storage::data_point::DataPoint;
use crate::storage::{INITIAL_BUILDER_CAPACITY, MetaData};

/// A single time series being built, consisting of a series of timestamps and values. Note that
/// since array builders are used, the data can only be read once the builders are finished and
/// can not be further appended to after.
pub struct TimeSeriesBuilder {
    /// Builder consisting of timestamps with microsecond precision.
    timestamps: PrimitiveBuilder<TimestampMicrosecondType>,
    /// Builder consisting of float values.
    values: PrimitiveBuilder<Float32Type>,
    /// Metadata used to uniquely identify the time series (and related sensor).
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

    /// If at least one of the builders are at capacity, return true.
    pub fn is_full(&self) -> bool {
        // The length is always the same for both builders.
        let length = self.timestamps.len();

        length == self.timestamps.capacity() || length == self.values.capacity()
    }

    /// Add the timestamp and value from the data point to the time series array builders.
    pub fn insert_data(&mut self, data_point: &DataPoint) {
        self.timestamps.append_value(data_point.timestamp).unwrap();
        self.values.append_value(data_point.value).unwrap();

        println!("Inserted data point into {}.", self)
    }

    /// Finish the array builders and return the data in a structured record batch.
    pub fn get_data(&mut self) -> RecordBatch {
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