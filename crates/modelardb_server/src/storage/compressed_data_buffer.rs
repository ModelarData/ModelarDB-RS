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

//! Buffer for compressed segments from the same table.

use std::fs;
use std::io::Error as IOError;
use std::io::ErrorKind::Other;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::format::SortingColumn;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::schemas::COMPRESSED_SCHEMA;

use crate::metadata::MetadataManager;
use crate::storage;
use crate::storage::StorageEngine;

/// Compressed segments representing data points from a column in a model table as one
/// [`RecordBatch`].
#[derive(Clone, Debug)]
pub(super) struct CompressedSegmentBatch {
    /// Univariate id that uniquely identifies the univariate time series the compressed segments
    /// represents data points for.
    univariate_id: u64,
    /// Metadata of the model table to insert the data points into.
    model_table_metadata: Arc<ModelTableMetadata>,
    /// Compressed segments representing the data points to insert.
    pub(super) compressed_segments: RecordBatch,
}

impl CompressedSegmentBatch {
    pub(super) fn new(
        univariate_id: u64,
        model_table_metadata: Arc<ModelTableMetadata>,
        compressed_segments: RecordBatch,
    ) -> Self {
        Self {
            univariate_id,
            model_table_metadata,
            compressed_segments,
        }
    }

    /// Return the name of the table the buffer stores data for.
    pub(super) fn model_table_name(&self) -> String {
        self.model_table_metadata.name.clone()
    }

    /// Return the index of the column the buffer stores data for.
    pub(super) fn column_index(&self) -> u16 {
        MetadataManager::univariate_id_to_column_index(self.univariate_id)
    }
}

/// A single compressed buffer, containing one or more compressed segments for a column in a model
/// table as one or more [RecordBatches](RecordBatch) and providing functionality for appending
/// segments and saving all segments to a single Apache Parquet file.
pub(super) struct CompressedDataBuffer {
    /// Compressed segments that make up the compressed data in the [`CompressedDataBuffer`].
    compressed_segments: Vec<RecordBatch>,
    /// Continuously updated total sum of the size of the compressed segments.
    pub(super) size_in_bytes: usize,
}

impl CompressedDataBuffer {
    pub(super) fn new() -> Self {
        Self {
            compressed_segments: Vec::new(),
            size_in_bytes: 0,
        }
    }

    /// Append `compressed_segments` to the [`CompressedDataBuffer`] and return the size of
    /// `compressed_segments` in bytes. It is assumed that `compressed_segments` is sorted by time.
    pub(super) fn append_compressed_segments(&mut self, compressed_segments: RecordBatch) -> usize {
        let segment_size = Self::size_of_compressed_segments(&compressed_segments);

        self.compressed_segments.push(compressed_segments);
        self.size_in_bytes += segment_size;

        segment_size
    }

    /// If the compressed segments are successfully saved to an Apache Parquet file return the
    /// path to the saved file, otherwise return [`IOError`].
    pub(super) fn save_to_apache_parquet(
        &mut self,
        folder_path: &Path,
    ) -> Result<PathBuf, IOError> {
        debug_assert!(
            !self.compressed_segments.is_empty(),
            "Cannot save CompressedDataBuffer with no data."
        );

        // Combine the compressed segments into a single RecordBatch.
        let batch =
            compute::concat_batches(&COMPRESSED_SCHEMA.0, &self.compressed_segments).unwrap();

        // Create the folder structure if it does not already exist.
        fs::create_dir_all(folder_path)?;

        // Create a new file that includes the start timestamp of the first segment in batch, the
        // end timestamp of the last segment in batch, the minimum value stored in batch, and the
        // maximum value stored in batch as the file name to support efficiently pruning files that
        // only contains data points with timestamps and values that are not relevant for a query.
        let file_name = storage::StorageEngine::create_time_and_value_range_file_name(&batch);
        let file_path = folder_path.join(file_name);

        // Specify that the file must be sorted by univariate_id and then by start_time.
        let sorting_columns = Some(vec![
            SortingColumn::new(0, false, false),
            SortingColumn::new(2, false, false),
        ]);

        StorageEngine::write_batch_to_apache_parquet_file(
            batch,
            file_path.as_path(),
            sorting_columns,
        )
        .map_err(|error| IOError::new(Other, error.to_string()))?;

        Ok(file_path)
    }

    /// Return the size in bytes of `compressed_segments`.
    fn size_of_compressed_segments(compressed_segments: &RecordBatch) -> usize {
        let mut total_size: usize = 0;

        // Compute the total number of bytes of memory used by the columns.
        for column in compressed_segments.columns() {
            // Recursively compute the total number of bytes of memory used by a single column. It
            // is both the size of the types, e.g., Array, ArrayData, Buffer, and Bitmap, and the
            // column's values in Apache Arrow format as buffers and the null bitmap if it exists.
            // Apache Arrow Columnar Format: https://arrow.apache.org/docs/format/Columnar.html.
            total_size += column.get_array_memory_size()
        }

        total_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::common_test;

    #[test]
    fn test_can_append_valid_compressed_segments() {
        let mut compressed_data_buffer = CompressedDataBuffer::new();
        compressed_data_buffer
            .append_compressed_segments(common_test::compressed_segments_record_batch());

        assert_eq!(compressed_data_buffer.compressed_segments.len(), 1)
    }

    #[test]
    fn test_compressed_data_buffer_size_updated_when_appending() {
        let mut compressed_data_buffer = CompressedDataBuffer::new();
        compressed_data_buffer
            .append_compressed_segments(common_test::compressed_segments_record_batch());

        assert!(compressed_data_buffer.size_in_bytes > 0);
    }

    #[test]
    fn test_can_save_compressed_data_buffer_to_apache_parquet() {
        let mut compressed_data_buffer = CompressedDataBuffer::new();
        let segment = common_test::compressed_segments_record_batch();
        compressed_data_buffer.append_compressed_segments(segment.clone());

        let temp_dir = tempfile::tempdir().unwrap();
        compressed_data_buffer
            .save_to_apache_parquet(temp_dir.path())
            .unwrap();

        // Data should be saved to a file with the start time, end time, min value, and max value of
        // the compressed segments the file contains as the file name.
        let file_path = storage::StorageEngine::create_time_and_value_range_file_name(&segment);
        assert!(temp_dir.path().join(file_path).exists());
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "Cannot save CompressedDataBuffer with no data.")]
    fn test_panic_if_saving_empty_compressed_data_buffer_to_apache_parquet() {
        let mut empty_compressed_data_buffer = CompressedDataBuffer::new();

        empty_compressed_data_buffer
            .save_to_apache_parquet(Path::new("table"))
            .unwrap();
    }

    #[test]
    fn test_get_size_of_compressed_data_buffer() {
        let compressed_data_buffer = common_test::compressed_segments_record_batch();

        assert_eq!(
            CompressedDataBuffer::size_of_compressed_segments(&compressed_data_buffer),
            common_test::COMPRESSED_SEGMENTS_SIZE,
        );
    }
}
