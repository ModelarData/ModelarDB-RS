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

//! Support for managing multiple compressed segments from the same time series.

use std::fs;
use std::io::Error as IOError;
use std::io::ErrorKind::Other;
use std::path::Path;

use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;

use crate::storage;
use crate::storage::StorageEngine;
use crate::types::CompressedSchema;

/// A single compressed time series, containing one or more compressed segments and providing
/// functionality for appending segments and saving all segments to a single Apache Parquet file.
pub(super) struct CompressedTimeSeries {
    /// Compressed segments that make up the sequential compressed data of the [`CompressedTimeSeries`].
    compressed_segments: Vec<RecordBatch>,
    /// Continuously updated total sum of the size of the compressed segments.
    pub(super) size_in_bytes: usize,
}

impl CompressedTimeSeries {
    pub(super) fn new() -> Self {
        Self {
            compressed_segments: Vec::new(),
            size_in_bytes: 0,
        }
    }

    /// Append `segment` to the compressed data in the [`CompressedTimeSeries`] and return the size
    /// of `segment` in bytes. It is assumed that `segment` is sorted by time.
    pub(super) fn append_segment(&mut self, segment: RecordBatch) -> usize {
        let segment_size = Self::get_size_of_segment(&segment);

        self.compressed_segments.push(segment);
        self.size_in_bytes += segment_size;

        segment_size
    }

    /// If the compressed segments are successfully saved to an Apache Parquet file return [`Ok`],
    /// otherwise return [`IOError`].
    pub(super) fn save_to_apache_parquet(
        &mut self,
        folder_path: &Path,
        compressed_schema: &CompressedSchema,
    ) -> Result<(), IOError> {
        debug_assert!(
            !self.compressed_segments.is_empty(),
            "Cannot save CompressedTimeSeries with no data."
        );

        // Combine the segments into a single RecordBatch.
        let batch = compute::concat_batches(&compressed_schema.0, &*self.compressed_segments).unwrap();

        // Create the folder structure if it does not already exist.
        let complete_folder_path = folder_path.join("compressed");
        fs::create_dir_all(complete_folder_path.as_path())?;

        // Create a path that uses the first start timestamp and the last end timestamp as the file
        // name to better support pruning data that is too new or too old when executing a specific query.
        let file_name = storage::create_time_range_file_name(&batch);
        let file_path = complete_folder_path.join(file_name);
        StorageEngine::write_batch_to_apache_parquet_file(batch, file_path.as_path())
            .map_err(|error| IOError::new(Other, error.to_string()))?;

        Ok(())
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    use crate::metadata::test_util as metadata_test_util;
    use crate::storage::test_util;

    #[test]
    fn test_can_append_valid_compressed_segment() {
        let mut time_series = CompressedTimeSeries::new();
        time_series.append_segment(test_util::get_compressed_segment_record_batch());

        assert_eq!(time_series.compressed_segments.len(), 1)
    }

    #[test]
    fn test_compressed_time_series_size_updated_when_appending() {
        let mut time_series = CompressedTimeSeries::new();
        time_series.append_segment(test_util::get_compressed_segment_record_batch());

        assert!(time_series.size_in_bytes > 0);
    }

    #[test]
    fn test_can_save_compressed_segments_to_apache_parquet() {
        let mut time_series = CompressedTimeSeries::new();
        let segment = test_util::get_compressed_segment_record_batch();
        time_series.append_segment(segment.clone());

        let temp_dir = tempdir().unwrap();
        time_series.save_to_apache_parquet(
            temp_dir.path(),
            &metadata_test_util::get_compressed_schema(),
        ).unwrap();

        // Data should be saved to a file with the first start time and last end time as the file name.
        let file_path = format!("compressed/{}", storage::create_time_range_file_name(&segment));
        assert!(temp_dir.path().join(file_path).exists());
    }

    #[test]
    #[should_panic(expected = "Cannot save CompressedTimeSeries with no data.")]
    fn test_panic_if_saving_empty_compressed_segments_to_apache_parquet() {
        let mut empty_time_series = CompressedTimeSeries::new();

        empty_time_series.save_to_apache_parquet(
            Path::new("key"),
            &metadata_test_util::get_compressed_schema(),
        ).unwrap();
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
