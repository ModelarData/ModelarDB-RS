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

//! Support for managing multiple compressed segments from the same time series.

use std::fs;
use std::io::ErrorKind::Other;
use std::sync::Arc;

use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use crate::storage::write_batch_to_apache_parquet;
use crate::types::{ArrowTimestamp, ArrowValue, TimestampArray};

/// A single compressed time series, containing one or more compressed segments and providing
/// functionality for appending segments and saving all segments to a single Apache Parquet file.
pub struct CompressedTimeSeries {
    /// Compressed segments that make up the sequential compressed data of the time series.
    compressed_segments: Vec<RecordBatch>,
    /// Continuously updated total sum of the size of the compressed segments.
    pub size_in_bytes: usize,
}

impl CompressedTimeSeries {
    pub fn new() -> Self {
        Self {
            compressed_segments: Vec::new(),
            size_in_bytes: 0,
        }
    }

    /// Append `segment` to the compressed data in the time series and return the size of `segment` in bytes.
    pub fn append_segment(&mut self, segment: RecordBatch) -> usize {
        let segment_size = Self::get_size_of_segment(&segment);

        debug_assert!(
            segment.schema() == Arc::new(Self::get_compressed_segment_schema()),
            "Schema of record batch does not match compressed segment schema."
        );

        self.compressed_segments.push(segment);
        self.size_in_bytes += segment_size;

        segment_size
    }

    /// If the compressed segments are successfully saved to an Apache Parquet file, return Ok,
    /// otherwise return Err.
    pub fn save_to_apache_parquet(&mut self, key: String) -> Result<(), std::io::Error> {
        if self.compressed_segments.is_empty() {
            Err(std::io::Error::new(
                Other,
                "The compressed time series does not contain any compressed data.",
            ))
        } else {
            // Combine the segments into a single record batch.
            let schema = Self::get_compressed_segment_schema();
            let batch = RecordBatch::concat(&Arc::new(schema), &*self.compressed_segments).unwrap();

            // TODO: "storage" should be replaced by a user-defined storage folder.
            // Create the folder structure if it does not already exist.
            let folder_path = format!("storage/{}/compressed", key);
            fs::create_dir_all(&folder_path)?;

            // Create a path that uses the first timestamp as the filename to better support
            // pruning data that is too new or too old when executing a specific query.
            let start_times: &TimestampArray = batch.column(2).as_any().downcast_ref().unwrap();
            let path = format!("{}/{}.parquet", folder_path, start_times.value(0));

            write_batch_to_apache_parquet(batch, path.clone());

            Ok(())
        }
    }

    /// Return the size in bytes of `segment`.
    fn get_size_of_segment(segment: &RecordBatch) -> usize {
        let mut total_size: usize = 0;

        for column in segment.columns() {
            // TODO: How is this calculated internally?
            total_size += column.get_array_memory_size()
        }

        total_size
    }

    /// Return the record batch schema used for compressed segments.
    fn get_compressed_segment_schema() -> Schema {
        Schema::new(vec![
            Field::new("model_type_id", DataType::UInt8, false),
            Field::new("timestamps", DataType::Binary, false),
            Field::new("start_time", ArrowTimestamp::DATA_TYPE, false),
            Field::new("end_time", ArrowTimestamp::DATA_TYPE, false),
            Field::new("values", DataType::Binary, false),
            Field::new("min_value", ArrowValue::DATA_TYPE, false),
            Field::new("max_value", ArrowValue::DATA_TYPE, false),
            Field::new("error", DataType::Float32, false),
        ])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::UInt8Array;
    use datafusion::arrow::datatypes::DataType::UInt8;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    use crate::storage::time_series::{test_util, CompressedTimeSeries};

    #[test]
    fn test_can_append_valid_compressed_segment() {
        let mut time_series = CompressedTimeSeries::new();
        time_series.append_segment(test_util::get_compressed_segment_record_batch());

        assert_eq!(time_series.compressed_segments.len(), 1)
    }

    #[test]
    #[should_panic(expected = "Schema of record batch does not match compressed segment schema.")]
    fn test_panic_if_appending_invalid_compressed_segment() {
        let invalid = test_util::get_invalid_compressed_segment_record_batch();

        let mut time_series = CompressedTimeSeries::new();
        time_series.append_segment(invalid);
    }

    #[test]
    fn test_compressed_time_series_size_updated_when_appending() {
        let mut time_series = CompressedTimeSeries::new();
        time_series.append_segment(test_util::get_compressed_segment_record_batch());

        assert!(time_series.size_in_bytes > 0);
    }

    #[test]
    fn test_cannot_save_empty_compressed_segments_to_apache_parquet() {
        let mut empty_time_series = CompressedTimeSeries::new();
        let result = empty_time_series.save_to_apache_parquet("key".to_owned());

        assert!(result.is_err());
    }

    #[test]
    fn test_get_size_of_segment() {
        let segment = test_util::get_compressed_segment_record_batch();

        assert_eq!(
            CompressedTimeSeries::get_size_of_segment(&segment),
            test_util::COMPRESSED_SEGMENT_SIZE,
        );
    }
}

#[cfg(test)]
/// Separate module for compressed segment utility functions since they are needed to test both
/// CompressedTimeSeries and StorageEngine.
pub mod test_util {
    use std::sync::Arc;

    use datafusion::arrow::array::{BinaryArray, Float32Array, UInt8Array};
    use datafusion::arrow::datatypes::DataType::UInt8;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    use crate::storage::time_series::CompressedTimeSeries;
    use crate::types::{TimestampArray, ValueArray};

    pub const COMPRESSED_SEGMENT_SIZE: usize = 2032;

    /// Return a record batch that only has a single column, and therefore does not match the
    /// compressed segment schema.
    pub fn get_invalid_compressed_segment_record_batch() -> RecordBatch {
        let model_type_id = UInt8Array::from(vec![2, 3, 3]);
        let schema = Schema::new(vec![Field::new("model_type_id", UInt8, false)]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(model_type_id)]).unwrap()
    }

    /// Return a generated compressed segment with three model segments.
    pub fn get_compressed_segment_record_batch() -> RecordBatch {
        let model_type_id = UInt8Array::from(vec![2, 3, 3]);
        let timestamps = BinaryArray::from_vec(vec![b"000", b"001", b"010"]);
        let start_time = TimestampArray::from(vec![1, 2, 3]);
        let end_time = TimestampArray::from(vec![2, 3, 4]);
        let values = BinaryArray::from_vec(vec![b"1111", b"1000", b"0000"]);
        let min_value = ValueArray::from(vec![5.2, 10.3, 30.2]);
        let max_value = ValueArray::from(vec![20.2, 12.2, 34.2]);
        let error = Float32Array::from(vec![0.2, 0.5, 0.1]);

        let schema = CompressedTimeSeries::get_compressed_segment_schema();

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(model_type_id),
                Arc::new(timestamps),
                Arc::new(start_time),
                Arc::new(end_time),
                Arc::new(values),
                Arc::new(min_value),
                Arc::new(max_value),
                Arc::new(error),
            ],
        )
        .unwrap()
    }
}
