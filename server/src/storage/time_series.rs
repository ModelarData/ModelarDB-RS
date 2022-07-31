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

/// Support for managing multiple compressed segments from the same time series.

use datafusion::arrow::array::Array;
use datafusion::arrow::record_batch::RecordBatch;

use crate::errors::MiniModelarDBError;

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

    /// If `segment` has the correct schema, append it to the compressed data and return Ok,
    /// otherwise return `CompressionError`.
    pub fn append_segment(&mut self, segment: RecordBatch) -> Result<usize, MiniModelarDBError> {
        // TODO: Check that the segment has the correct schema.
        // TODO: If so, append it to the compressed segments.
        let segment_size = CompressedTimeSeries::get_size_of_segment(segment);
        self.size_in_bytes += segment_size;

        Ok(segment_size)
    }

    // TODO: Should return error if there are not any segments to save.
    /// If the compressed segments are successfully saved to Parquet, return Ok, otherwise return Err.
    pub fn save_to_parquet(&mut self, key: String) -> Result<(), std::io::Error> {
        // TODO: Create the folder structure if it does not already exist.
        // TODO: Combine the segments into a single record batch.
        // TODO: Save the batch to a Parquet file.
        Ok(())
    }

    /// Return the size in bytes of `segment`.
    fn get_size_of_segment(segment: RecordBatch) -> usize {
        let mut total_size: usize = 0;

        for column in segment.columns() {
            total_size += column.data().get_array_memory_size()
        }

        total_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_append_segment_with_valid_schema() {

    }

    #[test]
    fn test_cannot_append_segment_with_invalid_schema() {

    }

    #[test]
    fn test_compressed_time_series_size_updated_when_appending() {

    }

    #[test]
    fn test_can_save_compressed_segments_to_parquet() {
        // TODO: This requires I/O.
    }

    #[test]
    fn test_cannot_save_empty_compressed_segments_to_parquet() {

    }

    #[test]
    fn test_get_size_of_segment() {

    }
}