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

//! Support for different kinds of stored segments.
//!
//! The main SegmentBuilder struct provides support for inserting and storing data in an in-memory
//! segment. BufferedSegment provides support for storing uncompressed data in a parquet buffer.
//! Finally, FinishedSegment provides a generalized interface for using the segments in the compressor.

use crate::storage::data_point::DataPoint;
use crate::storage::{write_batch_to_parquet, INITIAL_BUILDER_CAPACITY, Timestamp, Value};
use datafusion::arrow::array::{
    Array, ArrayBuilder, Float32Builder, TimestampMicrosecondArray, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::TimeUnit::Microsecond;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::{RecordBatch};
use datafusion::parquet::arrow::{ArrowReader, ParquetFileArrowReader, ProjectionMask};
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use std::fmt::Formatter;
use std::fs::File;
use std::sync::Arc;
use std::{fmt, fs, mem};

pub trait UncompressedSegment {
    fn get_data(&mut self) -> RecordBatch;

    fn get_memory_size(&self) -> usize;
}

/// A single segment being built, consisting of a series of timestamps and values. Note that
/// since array builders are used, the data can only be read once the builders are finished and
/// can not be further appended to after.
pub struct SegmentBuilder {
    /// Builder consisting of timestamps with microsecond precision.
    timestamps: TimestampMicrosecondBuilder,
    /// Builder consisting of float values.
    values: Float32Builder,
}

impl fmt::Display for SegmentBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&*format!("Segment with {} data point(s) (", self.timestamps.len()));
        f.write_str(&*format!("timestamp capacity: {}, ", self.timestamps.capacity()));
        f.write_str(&*format!("values capacity: {})", self.values.capacity()));

        Ok(())
    }
}

impl UncompressedSegment for SegmentBuilder {
    /// Finish the array builders and return the data in a structured record batch.
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

    /// Return the total size of the builder in bytes. Note that this is independent of the length.
    fn get_memory_size(&self) -> usize {
        (self.timestamps.capacity() * mem::size_of::<Timestamp>())
            + (self.values.capacity() * mem::size_of::<Value>())
    }
}

impl SegmentBuilder {
    pub fn new() -> Self {
        Self {
            // Note that the actual internal capacity might be slightly larger than these values. Apache
            // arrow defines the argument as being the lower bound for how many items the builder can hold.
            timestamps: TimestampMicrosecondBuilder::new(INITIAL_BUILDER_CAPACITY),
            values: Float32Builder::new(INITIAL_BUILDER_CAPACITY),
        }
    }

    /// Return how many data points the segment currently contains.
    pub fn get_length(&self) -> usize {
        // The length is always the same for both builders.
        self.timestamps.len()
    }

    /// If at least one of the builders are at capacity, return true.
    pub fn is_full(&self) -> bool {
        let length = self.get_length();
        length == self.timestamps.capacity() || length == self.values.capacity()
    }

    /// Add the timestamp and value from the data point to the segment array builders.
    pub fn insert_data(&mut self, data_point: &DataPoint) {
        self.timestamps.append_value(data_point.timestamp).unwrap();
        self.values.append_value(data_point.value).unwrap();

        println!("Inserted data point into {}.", self)
    }
}

/// A single segment that has been saved to a parquet file buffer due to memory constraints.
pub struct BufferedSegment {
    /// Path to the file containing the uncompressed data in the segment.
    path: String,
}

impl UncompressedSegment for BufferedSegment {
    /// Retrieve the data from the parquet file buffer and return it in a structured record batch.
    fn get_data(&mut self) -> RecordBatch {
        let file = File::open(&self.path).unwrap();
        let file_reader = SerializedFileReader::new(file).unwrap();

        // Specify that we want to read the first two columns (timestamps, values) from the file.
        let file_metadata = file_reader.metadata().file_metadata();
        let mask = ProjectionMask::leaves(file_metadata.schema_descr(), [0, 1]);

        // Convert the read data into a structured record batch.
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
        let mut record_batch_reader = arrow_reader
            .get_record_reader_by_columns(mask, 2048)
            .unwrap();

        record_batch_reader.next().unwrap().unwrap()
    }

    /// Return 0 since the data is not kept in memory.
    fn get_memory_size(&self) -> usize {
        0
    }
}

impl BufferedSegment {
    /// Retrieve the data from the segment builder, save it to a parquet file, and save the path.
    pub fn new(key: String, mut segment_builder: SegmentBuilder) -> Self {
        let folder_path = format!("uncompressed/{}", key);
        fs::create_dir_all(&folder_path);

        let data = segment_builder.get_data();
        let timestamps: &TimestampMicrosecondArray = data.column(0).as_any().downcast_ref().unwrap();

        let path = format!("{}/{}.parquet", folder_path, timestamps.value(0));
        write_batch_to_parquet(data, path.clone());

        Self { path }
    }
}

/// Representing either an in-memory or buffered segment that is finished and ready for compression.
pub struct FinishedSegment {
    pub key: String,
    pub uncompressed_segment: Box<dyn UncompressedSegment>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use paho_mqtt::Message;

    fn get_empty_segment_builder() -> (DataPoint, SegmentBuilder) {
        let message = Message::new("ModelarDB/test", "[1657878396943245, 30]", 1);

        let data_point = DataPoint::from_message(&message).unwrap();
        let segment_builder = SegmentBuilder::new();

        (data_point, segment_builder)
    }

    // Tests for SegmentBuilder.
    #[test]
    fn test_can_get_segment_builder_memory_size() {
        // TODO: Implement this test.
        // TODO: Since this is dependent on the builder size and actual capacity, how do we get this?
    }

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

        for _ in 0..segment_builder.timestamps.capacity() {
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

        let data = segment_builder.get_data();
        assert_eq!(data.num_columns(), 2);
        assert_eq!(data.num_rows(), 2);
    }

    // Tests for BufferedSegment.
    #[test]
    fn test_can_get_buffered_segment_length() {
        let buffered_segment = BufferedSegment { path: "".to_string() };

        assert_eq!(buffered_segment.get_memory_size(), 0)
    }
}
