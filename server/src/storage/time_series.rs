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

use datafusion::arrow::array::{Array, UInt8Array};
use datafusion::arrow::datatypes::DataType::{Float32, List, UInt8};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use crate::storage::write_batch_to_parquet;
use crate::types::{ArrowTimestamp, ArrowValue, TimestampArray};

/// A single compressed time series, containing one or more compressed segments in order and providing
/// functionality for appending more segments and saving all segments to a single Parquet file.
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

    // TODO: Maybe use debug assert to check that the schema is correct.
    /// Append `segment` to the compressed data in the time series and return the size `segment` in bytes.
    pub fn append_segment(&mut self, segment: RecordBatch) -> usize {
        let segment_size = CompressedTimeSeries::get_size_of_segment(&segment);

        self.compressed_segments.push(segment);
        self.size_in_bytes += segment_size;

        segment_size
    }

    /// If the compressed segments are successfully saved to Parquet, return Ok, otherwise return Err.
    pub fn save_to_parquet(&mut self, key: String) -> Result<(), std::io::Error> {
        if self.compressed_segments.is_empty() {
            Err(std::io::Error::new(
                Other,
                "The time series does not contain any compressed data.",
            ))
        } else {
            // Combine the segments into a single record batch.
            let schema = CompressedTimeSeries::get_compressed_segment_schema();
            let batch = RecordBatch::concat(&Arc::new(schema), &*self.compressed_segments).unwrap();

            // Create the folder structure if it does not already exist.
            let folder_path = format!("storage/{}/compressed", key);
            fs::create_dir_all(&folder_path)?;

            // Create a path that uses the first timestamp as the filename.
            let timestamps: &TimestampArray = batch.column(0).as_any().downcast_ref().unwrap();
            let path = format!("{}/{}.parquet", folder_path, timestamps.value(0));

            write_batch_to_parquet(batch, path.clone());

            Ok(())
        }
    }

    /// Return the size in bytes of `segment`.
    fn get_size_of_segment(segment: &RecordBatch) -> usize {
        let mut total_size: usize = 0;

        for column in segment.columns() {
            total_size += column.data().get_array_memory_size()
        }

        total_size
    }

    /// Return the record batch schema used for compressed segments.
    fn get_compressed_segment_schema() -> Schema {
        let timestamp_field = Field::new("timestamp", UInt8, false);
        let value_field = Field::new("value", UInt8, false);

        Schema::new(vec![
            Field::new("model_type_id", UInt8, false),
            Field::new("timestamps", List(Box::new(timestamp_field)), false),
            Field::new("start_time", ArrowTimestamp::DATA_TYPE, false),
            Field::new("end_time", ArrowTimestamp::DATA_TYPE, false),
            Field::new("values", List(Box::new(value_field)), false),
            Field::new("min_value", ArrowValue::DATA_TYPE, false),
            Field::new("max_value", ArrowValue::DATA_TYPE, false),
            Field::new("error", Float32, false),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_append_valid_compressed_segment() {}

    #[test]
    fn test_cannot_append_invalid_compressed_segment() {}

    #[test]
    fn test_compressed_time_series_size_updated_when_appending() {}

    #[test]
    fn test_can_save_compressed_segments_to_parquet() {
        // TODO: This requires I/O.
    }

    #[test]
    fn test_cannot_save_empty_compressed_segments_to_parquet() {}

    #[test]
    fn test_get_size_of_segment() {}
}
