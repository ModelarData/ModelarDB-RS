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

//! Provides support for inserting and storing data in an in-memory segment. Furthermore, the data
//! can be retrieved as a structured record batch.

use std::fmt::Formatter;
use std::fs::File;
use std::sync::Arc;
use std::{fmt, fs};

use datafusion::arrow::array::ArrayBuilder;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Encoding;
use datafusion::parquet::file::properties::WriterProperties;

use crate::storage::data_point::DataPoint;
use crate::storage::{INITIAL_BUILDER_CAPACITY, MetaData};
use crate::types::{ArrowTimestamp, ArrowValue, Timestamp, TimestampBuilder, ValueBuilder};

/// A single segment being built, consisting of an ordered sequence of timestamps and values. Note
/// that since array builders are used, the data can only be read once the builders are finished and
/// cannot be further appended to after.
pub struct SegmentBuilder {
    /// Builder consisting of timestamps with microsecond precision.
    timestamps: TimestampBuilder,
    /// Builder consisting of float values.
    values: ValueBuilder,
    /// Metadata used to uniquely identify the segment (and related sensor).
    metadata: MetaData,
    /// First timestamp used to distinguish between segments from the same sensor.
    first_timestamp: Timestamp,
}

impl fmt::Display for SegmentBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&format!("Segment with {} data point(s) (", self.timestamps.len()));
        f.write_str(&format!("timestamp capacity: {}, ", self.timestamps.capacity()));
        f.write_str(&format!("values capacity: {})", self.values.capacity()));

        Ok(())
    }
}

impl SegmentBuilder {
    pub fn new(data_point: &DataPoint) -> Self {
        Self {
            // Note that the actual internal capacity might be slightly larger than these values. Apache
            // Arrow defines the argument as being the lower bound for how many items the builder can hold.
            timestamps: TimestampBuilder::new(INITIAL_BUILDER_CAPACITY),
            values: ValueBuilder::new(INITIAL_BUILDER_CAPACITY),
            metadata: data_point.metadata.to_vec(),
            first_timestamp: data_point.timestamp,
        }
    }

    /// Return `true` if the segment is full, meaning additional data points cannot be appended.
    pub fn is_full(&self) -> bool {
        self.get_length() == self.get_capacity()
    }

    /// Return how many data points the segment currently contains.
    pub fn get_length(&self) -> usize {
        // The length is always the same for both builders.
        self.timestamps.len()
    }

    /// Return how many data points the segment can contain.
    fn get_capacity(&self) -> usize {
        usize::min(self.timestamps.capacity(), self.values.capacity())
    }

    /// Add the timestamp and value from `data_point` to the segment's array builders.
    pub fn insert_data(&mut self, data_point: &DataPoint) {
        self.timestamps.append_value(data_point.timestamp);
        self.values.append_value(data_point.value);

        println!("Inserted data point into {}.", self);
    }

    /// Finish the array builders and return the data in a structured record batch.
    pub fn get_record_batch(&mut self) -> RecordBatch {
        let timestamps = self.timestamps.finish();
        let values = self.values.finish();

        let schema = Schema::new(vec![
            Field::new("timestamps", ArrowTimestamp::DATA_TYPE, false),
            Field::new("values", ArrowValue::DATA_TYPE, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(timestamps), Arc::new(values)]
        ).unwrap()
    }

    /// Write `batch` to a persistent Apache Parquet file on disk.
    pub fn save_compressed_data(&self, batch: RecordBatch) {
        let folder_name = self.metadata.join("-");
        fs::create_dir_all(&folder_name);

        let path = format!("{}/{}.parquet", folder_name, self.first_timestamp);
        write_batch_to_parquet(batch, path);
    }
}

/// Write `batch` to an Apache Parquet file at the location given by `path`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use paho_mqtt::Message;

    #[test]
    fn test_can_get_length() {
        let (data_point, mut segment_builder) = get_empty_segment_builder();

        assert_eq!(segment_builder.get_length(), 0);
    }

    #[test]
    fn test_can_insert_data_point() {
        let (data_point, mut segment_builder) = get_empty_segment_builder();
        segment_builder.insert_data(&data_point);

        assert_eq!(segment_builder.get_length(), 1);
    }

    #[test]
    fn test_can_check_segment_is_full() {
        let (data_point, mut segment_builder) = get_empty_segment_builder();

        for _ in 0..segment_builder.get_capacity() {
            segment_builder.insert_data(&data_point)
        }

        assert!(segment_builder.is_full());
    }

    #[test]
    fn test_can_check_segment_is_not_full() {
        let (data_point, mut segment_builder) = get_empty_segment_builder();

        assert!(!segment_builder.is_full());
    }

    #[test]
    fn test_can_get_data() {
        let (data_point, mut segment_builder) = get_empty_segment_builder();
        segment_builder.insert_data(&data_point);
        segment_builder.insert_data(&data_point);

        let data = segment_builder.get_record_batch();
        assert_eq!(data.num_columns(), 2);
        assert_eq!(data.num_rows(), 2);
    }

    fn get_empty_segment_builder() -> (DataPoint, SegmentBuilder) {
        let message = Message::new("ModelarDB/test", "[1657878396943245, 30]", 1);

        let data_point = DataPoint::from_message(&message).unwrap();
        let segment_builder = SegmentBuilder::new(&data_point);

        (data_point, segment_builder)
    }
}
