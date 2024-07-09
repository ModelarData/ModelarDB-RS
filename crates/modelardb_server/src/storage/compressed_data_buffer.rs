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

use std::io::{Error as IOError, ErrorKind};
use std::sync::Arc;

use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::schemas::COMPRESSED_SCHEMA;

/// Compressed segments representing data points from a column in a model table as one
/// [`RecordBatch`].
#[derive(Clone, Debug)]
pub(super) struct CompressedSegmentBatch {
    /// Metadata of the model table to insert the data points into.
    pub(super) model_table_metadata: Arc<ModelTableMetadata>,
    /// Compressed segments representing the data points to insert.
    pub(super) compressed_segments: Vec<RecordBatch>,
}

impl CompressedSegmentBatch {
    pub(super) fn new(
        model_table_metadata: Arc<ModelTableMetadata>,
        compressed_segments: Vec<RecordBatch>,
    ) -> Self {
        Self {
            model_table_metadata,
            compressed_segments,
        }
    }

    /// Return the name of the table the batch stores data for.
    pub(super) fn model_table_name(&self) -> &str {
        &self.model_table_metadata.name
    }
}

/// A single compressed buffer, containing one or more compressed segments for a time series in a
/// model table as one or more [RecordBatches](RecordBatch) per column and providing functionality
/// for appending segments and saving all segments to a single Apache Parquet file.
pub(super) struct CompressedDataBuffer {
    /// Compressed segments that make up the compressed data in the [`CompressedDataBuffer`].
    compressed_segments: Vec<RecordBatch>,
    /// Continuously updated total sum of the size of the compressed segments.
    pub(super) size_in_bytes: usize,
}

impl CompressedDataBuffer {
    pub(super) fn new() -> Self {
        Self {
            compressed_segments: vec![],
            size_in_bytes: 0,
        }
    }

    /// Append `compressed_segments` to the [`CompressedDataBuffer`] and return the size of
    /// `compressed_segments` in bytes if their schema is [`COMPRESSED_SCHEMA`], otherwise
    /// [`IOError`] is returned.
    pub(super) fn append_compressed_segments(
        &mut self,
        mut compressed_segments: Vec<RecordBatch>,
    ) -> Result<usize, IOError> {
        if compressed_segments
            .iter()
            .any(|compressed_segments| compressed_segments.schema() != COMPRESSED_SCHEMA.0)
        {
            return Err(IOError::new(
                ErrorKind::InvalidInput,
                "Compressed segments must all use COMPRESSED_SCHEMA.".to_owned(),
            ));
        }

        let mut compressed_segments_size = 0;
        for compressed_segment_batch in compressed_segments.drain(0..) {
            compressed_segments_size +=
                Self::size_of_compressed_segments(&compressed_segment_batch);
            self.compressed_segments.push(compressed_segment_batch);
            self.size_in_bytes += compressed_segments_size;
        }

        Ok(compressed_segments_size)
    }

    /// Return the compressed segments as a single [`RecordBatch`].
    pub(super) async fn record_batch(self) -> RecordBatch {
        // unwrap() is safe as the schema of the record batches are checked when appended.
        compute::concat_batches(
            &self.compressed_segments[0].schema(),
            &self.compressed_segments,
        )
        .unwrap()
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

    use modelardb_common::test;

    #[test]
    fn test_can_append_valid_compressed_segments() {
        let mut compressed_data_buffer = CompressedDataBuffer::new();

        compressed_data_buffer
            .append_compressed_segments(vec![
                test::compressed_segments_record_batch(),
                test::compressed_segments_record_batch(),
            ])
            .unwrap();

        assert_eq!(compressed_data_buffer.compressed_segments.len(), 2);
        assert_eq!(compressed_data_buffer.compressed_segments[0].num_rows(), 3);
        assert_eq!(compressed_data_buffer.compressed_segments[1].num_rows(), 3);
    }

    #[test]
    fn test_compressed_data_buffer_size_updated_when_appending() {
        let mut compressed_data_buffer = CompressedDataBuffer::new();

        compressed_data_buffer
            .append_compressed_segments(vec![
                test::compressed_segments_record_batch(),
                test::compressed_segments_record_batch(),
            ])
            .unwrap();

        assert!(compressed_data_buffer.size_in_bytes > 0);
    }

    #[tokio::test]
    async fn test_can_get_single_record_batch_from_compressed_data_buffer() {
        let mut compressed_data_buffer = CompressedDataBuffer::new();
        let compressed_segments = vec![
            test::compressed_segments_record_batch(),
            test::compressed_segments_record_batch(),
        ];
        compressed_data_buffer
            .append_compressed_segments(compressed_segments)
            .unwrap();

        let record_batch = compressed_data_buffer.record_batch().await;
        assert_eq!(record_batch.num_columns(), 11);
        assert_eq!(record_batch.num_rows(), 6);
    }

    #[tokio::test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "index out of bounds: the len is 0 but the index is 0")]
    async fn test_panic_if_returning_record_batch_from_empty_compressed_data_buffer() {
        CompressedDataBuffer::new().record_batch().await;
    }

    #[test]
    fn test_get_size_of_compressed_data_buffer() {
        let compressed_data_buffer = test::compressed_segments_record_batch();

        assert_eq!(
            CompressedDataBuffer::size_of_compressed_segments(&compressed_data_buffer),
            test::COMPRESSED_SEGMENTS_SIZE,
        );
    }
}
