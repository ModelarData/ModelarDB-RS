/* Copyright 2022 The ModelarDB Contributors
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

//! Support for different kinds of uncompressed segments. The SegmentBuilder struct provides support
//! for inserting and storing data in an in-memory segment. SpilledSegment provides support for
//! storing uncompressed data in Apache Parquet files.

use std::fmt::Formatter;
use std::fs::File;
use std::io::ErrorKind::Other;
use std::sync::Arc;
use std::{fmt, fs, mem};

use datafusion::arrow::array::ArrayBuilder;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::{ArrowReader, ParquetFileArrowReader, ProjectionMask};
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use tracing::info;

use crate::storage::data_point::DataPoint;
use crate::storage::{write_batch_to_apache_parquet, INITIAL_BUILDER_CAPACITY, StorageEngine};
use crate::types::{
    ArrowTimestamp, ArrowValue, Timestamp, TimestampArray, TimestampBuilder, Value, ValueBuilder,
};

/// Shared functionality between different types of uncompressed segments, such as segment builders
/// and spilled segments.
pub trait UncompressedSegment {
    fn get_record_batch(&mut self) -> RecordBatch;

    fn get_memory_size(&self) -> usize;

    // Since both segment builders and spilled segments are present in the compression queue, both
    // structs need to implement spilling to Apache Parquet, with already spilled segments returning Err.
    fn spill_to_apache_parquet(&mut self, key: String) -> Result<SpilledSegment, std::io::Error>;
}

/// A single segment being built, consisting of an ordered sequence of timestamps and values. Note
/// that since array builders are used, the data can only be read once the builders are finished and
/// cannot be further appended to after.
pub struct SegmentBuilder {
    /// Builder consisting of timestamps.
    timestamps: TimestampBuilder,
    /// Builder consisting of float values.
    values: ValueBuilder,
}

impl SegmentBuilder {
    pub fn new() -> Self {
        Self {
            timestamps: TimestampBuilder::new(INITIAL_BUILDER_CAPACITY),
            values: ValueBuilder::new(INITIAL_BUILDER_CAPACITY),
        }
    }

    /// Return the total size of the builder in bytes. Note that this is constant.
    pub fn get_memory_size() -> usize {
        (INITIAL_BUILDER_CAPACITY * mem::size_of::<Timestamp>())
            + (INITIAL_BUILDER_CAPACITY * mem::size_of::<Value>())
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
        // The capacity is always the same for both builders.
        self.timestamps.capacity()
    }

    /// Add the timestamp and value from `data_point` to the segment's array builders.
    pub fn insert_data(&mut self, data_point: &DataPoint) {
        self.timestamps.append_value(data_point.timestamp);
        self.values.append_value(data_point.value);

        info!("Inserted data point into segment with {}.", self)
    }
}

impl fmt::Display for SegmentBuilder {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{} data point(s) (Capacity: {})", self.get_length(), self.get_capacity())
    }
}

impl UncompressedSegment for SegmentBuilder {
    /// Finish the array builders and return the data in a structured record batch.
    fn get_record_batch(&mut self) -> RecordBatch {
        let timestamps = self.timestamps.finish();
        let values = self.values.finish();

        let schema = StorageEngine::get_uncompressed_segment_schema();

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(timestamps), Arc::new(values)],
        ).unwrap()
    }

    /// Return the total size of the uncompressed segment in bytes.
    fn get_memory_size(&self) -> usize {
        SegmentBuilder::get_memory_size()
    }

    /// Spill the in-memory segment to an Apache Parquet file and return Ok when finished.
    fn spill_to_apache_parquet(&mut self, key: String) -> Result<SpilledSegment, std::io::Error> {
        let batch = self.get_record_batch();
        Ok(SpilledSegment::new(key.clone(), batch))
    }
}

/// A single segment that has been spilled to an Apache Parquet file due to memory constraints.
pub struct SpilledSegment {
    /// Path to the Apache Parquet file containing the uncompressed data in the segment.
    path: String,
}

impl SpilledSegment {
    /// Spill the data in `batch` to an Apache Parquet file, and return a spilled segment with the path.
    pub fn new(key: String, batch: RecordBatch) -> Self {
        // TODO: "storage" should be replaced by a user-defined storage folder.
        let folder_path = format!("storage/{}/uncompressed", key);
        fs::create_dir_all(&folder_path);

        // Create a path that uses the first timestamp as the filename.
        let timestamps: &TimestampArray = batch.column(0).as_any().downcast_ref().unwrap();
        let path = format!("{}/{}.parquet", folder_path, timestamps.value(0));

        write_batch_to_apache_parquet(batch, path.clone());

        Self { path }
    }
}

impl UncompressedSegment for SpilledSegment {
    /// Retrieve the data from the Apache Parquet file and return it in a structured record batch.
    fn get_record_batch(&mut self) -> RecordBatch {
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

    /// Since the data is not kept in memory, return 0.
    fn get_memory_size(&self) -> usize {
        0
    }

    /// Since the segment has already been spilled, return Err.
    fn spill_to_apache_parquet(&mut self, __key: String) -> Result<SpilledSegment, std::io::Error> {
        Err(std::io::Error::new(
            Other,
            format!("The segment has already been spilled to '{}'.", &self.path),
        ))
    }
}

/// Representing either an in-memory or spilled segment that is finished and ready for compression.
pub struct FinishedSegment {
    pub key: String,
    pub uncompressed_segment: Box<dyn UncompressedSegment>,
}

impl FinishedSegment {
    /// If in memory, spill the segment to an Apache Parquet file and return the path, otherwise return Err.
    pub fn spill_to_apache_parquet(&mut self) -> Result<String, std::io::Error> {
        let spilled = self
            .uncompressed_segment
            .spill_to_apache_parquet(self.key.clone())?;

        let path = spilled.path.clone();
        self.uncompressed_segment = Box::new(spilled);

        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paho_mqtt::Message;

    // Tests for SegmentBuilder.
    #[test]
    fn test_get_segment_builder_memory_size() {
        let mut segment_builder = SegmentBuilder::new();

        let expected = (segment_builder.timestamps.capacity() * mem::size_of::<Timestamp>())
            + (segment_builder.values.capacity() * mem::size_of::<Value>());

        assert_eq!(SegmentBuilder::get_memory_size(), expected)
    }

    #[test]
    fn test_get_length() {
        let mut segment_builder = SegmentBuilder::new();

        assert_eq!(segment_builder.get_length(), 0);
    }

    #[test]
    fn test_can_insert_data_point() {
        let data_point = get_data_point();
        let mut segment_builder = SegmentBuilder::new();

        segment_builder.insert_data(&data_point);

        assert_eq!(segment_builder.get_length(), 1);
    }

    #[test]
    fn test_check_segment_is_full() {
        let data_point = get_data_point();
        let mut segment_builder = SegmentBuilder::new();

        for _ in 0..segment_builder.get_capacity() {
            segment_builder.insert_data(&data_point)
        }

        assert!(segment_builder.is_full());
    }

    #[test]
    fn test_check_segment_is_not_full() {
        let mut segment_builder = SegmentBuilder::new();

        assert!(!segment_builder.is_full());
    }

    #[test]
    fn test_get_data_from_segment_builder() {
        let data_point = get_data_point();
        let mut segment_builder = SegmentBuilder::new();

        segment_builder.insert_data(&data_point);
        segment_builder.insert_data(&data_point);

        let data = segment_builder.get_record_batch();
        assert_eq!(data.num_columns(), 2);
        assert_eq!(data.num_rows(), 2);
    }

    fn get_data_point() -> DataPoint {
        let message = Message::new("ModelarDB/test", "[1657878396943245, 30]", 1);
        DataPoint::from_message(&message).unwrap()
    }

    // Tests for SpilledSegment.
    #[test]
    fn test_get_spilled_segment_memory_size() {
        let spilled_segment = SpilledSegment {
            path: "path".to_owned(),
        };

        assert_eq!(spilled_segment.get_memory_size(), 0)
    }

    #[test]
    fn test_cannot_spill_already_spilled_segment() {
        let mut spilled_segment = SpilledSegment {
            path: "path".to_owned(),
        };

        assert!(spilled_segment.spill_to_apache_parquet("key".to_owned()).is_err())
    }
}
