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

use datafusion::arrow::record_batch::RecordBatch;

/// A single compressed time series, containing one or more compressed segments in order and providing
/// functionality for appending more segments and saving all segments to a single Parquet file.
pub struct CompressedTimeSeries {
    /// Compressed segments that make up the sequential compressed data of the time series.
    compressed_segments: Vec<RecordBatch>,
    /// Continuously updated total sum of the size of the compressed segments.
    size_in_bytes: usize,
}

impl CompressedTimeSeries {
    // TODO: Should return a compression error instead.
    /// If `segment` has the correct schema, append it to the compressed data and return Ok, otherwise return Err.
    pub fn append_segment(segment: RecordBatch) -> Result<(), ()> {
        // TODO: Check that the segment has the correct schema.
        // TODO: If so, append it to the compressed segments.
        Ok(())
    }

    /// If the compressed segments can be successfully saved to Parquet, return Ok, otherwise return Err.
    pub fn save_time_series(key: String) -> Result<(), std::io::Error> {
        // TODO: Create the folder structure if it does not already exist.
        // TODO: Combine the segments into a single record batch.
        // TODO: Save the batch to a Parquet file.
        Ok(())
    }
}