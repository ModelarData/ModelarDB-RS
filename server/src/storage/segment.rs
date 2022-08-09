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
use std::io::ErrorKind::Other;
use std::io::Error as IOError;
use std::sync::Arc;
use std::{fmt, fs, mem};
use std::path::Path;

use datafusion::arrow::array::ArrayBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use tracing::info;

use crate::storage::data_point::DataPoint;
use crate::storage::{BUILDER_CAPACITY, StorageEngine};
use crate::types::{Timestamp, TimestampArray, TimestampBuilder, Value, ValueBuilder};

/// Shared functionality between different types of uncompressed segments, such as segment builders
/// and spilled segments.
pub trait UncompressedSegment {
    fn get_record_batch(&mut self) -> Result<RecordBatch, ParquetError>;

    fn get_memory_size(&self) -> usize;

    // Since both segment builders and spilled segments are present in the compression queue, both
    // structs need to implement spilling to Apache Parquet, with already spilled segments returning Err.
    fn spill_to_apache_parquet(&mut self, folder_path: String) -> Result<SpilledSegment, IOError>;
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
            timestamps: TimestampBuilder::new(BUILDER_CAPACITY),
            values: ValueBuilder::new(BUILDER_CAPACITY),
        }
    }

    /// Return the total size of the builder in bytes. Note that this is constant.
    pub fn get_memory_size() -> usize {
        (BUILDER_CAPACITY * mem::size_of::<Timestamp>())
            + (BUILDER_CAPACITY * mem::size_of::<Value>())
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
        debug_assert!(
            !self.is_full(),
            "Cannot insert data into full segment builder."
        );

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
    fn get_record_batch(&mut self) -> Result<RecordBatch, ParquetError> {
        debug_assert!(
            self.is_full(),
            "Cannot get record batch from segment builder that is not full."
        );

        let timestamps = self.timestamps.finish();
        let values = self.values.finish();

        let schema = StorageEngine::get_uncompressed_segment_schema();

        Ok(RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(timestamps), Arc::new(values)],
        ).unwrap())
    }

    /// Return the total size of the uncompressed segment in bytes.
    fn get_memory_size(&self) -> usize {
        SegmentBuilder::get_memory_size()
    }

    /// Spill the in-memory segment to an Apache Parquet file and return Ok when finished.
    fn spill_to_apache_parquet(&mut self, folder_path: String) -> Result<SpilledSegment, IOError> {
        // Since the schema is constant and the columns are always the same length, creating the
        // record batch should never fail and unwrap is therefore safe to use.
        let batch = self.get_record_batch().unwrap();
        Ok(SpilledSegment::new(folder_path, batch))
    }
}

/// A single segment that has been spilled to an Apache Parquet file due to memory constraints.
pub struct SpilledSegment {
    // TODO: Maybe change this to an actual Path instead of a String.
    /// Path to the Apache Parquet file containing the uncompressed data in the segment.
    path: String,
}

impl SpilledSegment {
    /// Spill the data in `segment` to an Apache Parquet file, and return a spilled segment with the path.
    pub fn new(folder_path: String, segment: RecordBatch) -> Self {
        debug_assert!(
            segment.schema() == Arc::new(StorageEngine::get_uncompressed_segment_schema()),
            "Schema of record batch does not match uncompressed segment schema."
        );

        let complete_path = format!("{}/uncompressed", folder_path);
        fs::create_dir_all(&complete_path);

        // Create a path that uses the first timestamp as the filename.
        let timestamps: &TimestampArray = segment.column(0).as_any().downcast_ref().unwrap();
        let path = format!("{}/{}.parquet", complete_path, timestamps.value(0));

        StorageEngine::write_batch_to_apache_parquet_file(segment, Path::new(&path.clone()));

        Self { path }
    }
}

impl UncompressedSegment for SpilledSegment {
    /// Retrieve the data from the Apache Parquet file and return it in a structured record batch.
    fn get_record_batch(&mut self) -> Result<RecordBatch, ParquetError> {
        let path = Path::new(&self.path);
        let segment = StorageEngine::read_entire_apache_parquet_file(path)?;

		debug_assert!(
            segment.schema() == Arc::new(StorageEngine::get_uncompressed_segment_schema()),
            "Schema of record batch does not match uncompressed segment schema."
        );

        Ok(segment)
    }

    /// Since the data is not kept in memory, return 0.
    fn get_memory_size(&self) -> usize {
        0
    }

    /// Since the segment has already been spilled, return Err.
    fn spill_to_apache_parquet(&mut self, _folder_path: String) -> Result<SpilledSegment, IOError> {
        Err(IOError::new(
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
    pub fn spill_to_apache_parquet(
        &mut self,
        storage_folder_path: String,
    ) -> Result<String, IOError> {
        let folder_path = format!("{}/{}", storage_folder_path, self.key.clone());
        let spilled = self
            .uncompressed_segment
            .spill_to_apache_parquet(folder_path)?;

        let path = spilled.path.clone();
        self.uncompressed_segment = Box::new(spilled);

        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
    use paho_mqtt::Message;
    use tempfile::tempdir;

    use crate::storage::time_series::test_util;
    use crate::types::{ArrowValue, ValueArray};

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
        let mut segment_builder = SegmentBuilder::new();
        insert_multiple_data_points(segment_builder.get_capacity(), &mut segment_builder);

        assert!(segment_builder.is_full());
    }

    #[test]
    fn test_check_segment_is_not_full() {
        let mut segment_builder = SegmentBuilder::new();

        assert!(!segment_builder.is_full());
    }

    #[test]
    #[should_panic(expected = "Cannot insert data into full segment builder.")]
    fn test_panic_if_inserting_data_point_when_full() {
        let mut segment_builder = SegmentBuilder::new();

        insert_multiple_data_points(segment_builder.get_capacity() + 1, &mut segment_builder);
    }

    #[test]
    fn test_get_record_batch_from_segment_builder() {
        let mut segment_builder = SegmentBuilder::new();
        insert_multiple_data_points(segment_builder.get_capacity(), &mut segment_builder);

        let capacity = segment_builder.get_capacity();
        let data = segment_builder.get_record_batch().unwrap();
        assert_eq!(data.num_columns(), 2);
        assert_eq!(data.num_rows(), capacity);
    }

    #[test]
    #[should_panic(expected = "Cannot get record batch from segment builder that is not full.")]
    fn test_panic_if_getting_record_batch_when_not_full() {
        let mut segment_builder = SegmentBuilder::new();

        segment_builder.get_record_batch();
    }

    #[test]
    fn test_can_spill_full_segment_builder() {
        let mut segment_builder = SegmentBuilder::new();
        insert_multiple_data_points(segment_builder.get_capacity(), &mut segment_builder);

        let temp_dir = tempdir().unwrap();
        let folder_path = temp_dir.path().to_str().unwrap().to_string();
        segment_builder.spill_to_apache_parquet(folder_path);

        let uncompressed_path = temp_dir.path().join("uncompressed");
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1)
    }

    #[test]
    #[should_panic(expected = "Cannot get record batch from segment builder that is not full.")]
    fn test_panic_if_spilling_segment_builder_when_not_full() {
        let mut segment_builder = SegmentBuilder::new();
        segment_builder.spill_to_apache_parquet("folder_path".to_owned());
    }

    /// Insert `count` generated data points into `segment_builder`.
    fn insert_multiple_data_points(count: usize, segment_builder: &mut SegmentBuilder) {
        let data_point = get_data_point();

        for _ in 0..count {
            segment_builder.insert_data(&data_point);
        }
    }

    /// Create a data point with a constant timestamp and value.
    fn get_data_point() -> DataPoint {
        let message = Message::new("ModelarDB/test", "[1657878396943245, 30]", 1);
        DataPoint::from_message(&message).unwrap()
    }

    // Tests for SpilledSegment.
    #[test]
    #[should_panic(expected = "Schema of record batch does not match uncompressed segment schema.")]
    fn test_panic_if_creating_spilled_segment_with_invalid_segment() {
        // Create a record batch that is missing timestamps.
        let values = ValueArray::from(vec![2.5, 3.3, 3.2]);
        let schema = Schema::new(vec![Field::new("values", ArrowValue::DATA_TYPE, false)]);
        let record_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values)]).unwrap();

        SpilledSegment::new("folder_path".to_owned(), record_batch);
    }

    #[test]
    fn test_get_spilled_segment_memory_size() {
        let spilled_segment = SpilledSegment {
            path: "path".to_owned(),
        };

        assert_eq!(spilled_segment.get_memory_size(), 0)
    }

    #[test]
    fn test_get_record_batch_from_spilled_segment() {
        let mut segment_builder = SegmentBuilder::new();
        let capacity = segment_builder.get_capacity();
        insert_multiple_data_points(capacity, &mut segment_builder);

        let temp_dir = tempdir().unwrap();
        let folder_path = temp_dir.path().to_str().unwrap().to_string();
        let mut spilled_segment = segment_builder.spill_to_apache_parquet(folder_path).unwrap();

        let data = spilled_segment.get_record_batch().unwrap();
        assert_eq!(data.num_columns(), 2);
        assert_eq!(data.num_rows(), capacity);
    }

    #[test]
    #[should_panic(expected = "Schema of record batch does not match uncompressed segment schema.")]
    fn test_panic_if_getting_record_batch_from_invalid_spilled_segment() {
        let temp_dir = tempdir().unwrap();

        // Save a compressed segment to the file where SpilledSegment expects an uncompressed segment.
        let segment = test_util::get_compressed_segment_record_batch();
        let path = temp_dir.path().join("test.parquet").to_str().unwrap().to_string();
        StorageEngine::write_batch_to_apache_parquet_file(segment, Path::new(&path.clone()));

        let mut spilled_segment = SpilledSegment { path };

        spilled_segment.get_record_batch();
    }

    #[test]
    fn test_cannot_spill_already_spilled_segment() {
        let mut spilled_segment = SpilledSegment {
            path: "path".to_owned(),
        };

        let result = spilled_segment.spill_to_apache_parquet("folder_path".to_owned());
        assert!(result.is_err())
    }
}
