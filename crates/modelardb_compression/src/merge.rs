/* Copyright 2023 The ModelarDB Contributors
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

//! Implementation of [`try_merge_segments`] and its helper functions which merges compressed
//! segment in a [`RecordBatch`] if possible within the error bound. Segments with similar models
//! may be produced if a time series does not change significantly between each batch of data points
//! that are compressed together or even within a batch if the time series oscillates.

use std::collections::HashMap;

use arrow::array::{BinaryArray, Float32Array, UInt64Array, UInt8Array};
use arrow::record_batch::RecordBatch;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::schemas::COMPRESSED_SCHEMA;
use modelardb_common::types::{TimestampArray, TimestampBuilder, ValueArray};

use crate::models::timestamps;
use crate::types::CompressedSegmentBatchBuilder;

/// Merge segments in `compressed_segments` if their schema is [`COMPRESSED_SCHEMA`] and they:
/// * Are from same time series.
/// * Contain the exact same models.
/// * Are consecutive in terms of time.
/// Assumes that if the consecutive segments A, B, and C exist for a time series and the segments A
/// and C are in `compressed_segments` then B is also in `compressed_segments`. If only A and C are
/// in `compressed_segments` a segment that overlaps with B will be created if A and C are merged.
pub fn try_merge_segments(compressed_segments: RecordBatch) -> Result<RecordBatch, ModelarDbError> {
    if compressed_segments.schema() != COMPRESSED_SCHEMA.0 {
        return Err(ModelarDbError::CompressionError(
            "The schema for the compressed segments is incorrect.".to_owned(),
        ));
    }

    // Extract the columns from the RecordBatch.
    modelardb_common::arrays!(
        compressed_segments,
        univariate_ids,
        model_type_ids,
        start_times,
        end_times,
        timestamps,
        min_values,
        max_values,
        values,
        residuals,
        errors
    );

    // For each segment, check if it can be merged with another adjacent segment.
    let num_rows = compressed_segments.num_rows();
    let mut can_segments_be_merged = false;
    let mut univariate_id_to_previous_index = HashMap::new();
    let mut indices_to_merge_per_univariate_id = HashMap::new();

    for current_index in 0..num_rows {
        let univariate_id = univariate_ids.value(current_index);
        let previous_index = *univariate_id_to_previous_index
            .get(&univariate_id)
            .unwrap_or(&current_index);

        if can_models_be_merged(
            previous_index,
            current_index,
            univariate_ids,
            model_type_ids,
            min_values,
            max_values,
            values,
            residuals,
        ) {
            indices_to_merge_per_univariate_id
                .entry(univariate_id)
                .or_insert_with(Vec::new)
                .push(Some(current_index));

            can_segments_be_merged = previous_index != current_index;
        } else {
            // unwrap() is safe as a segment is guaranteed to match itself.
            let indices_to_merge = indices_to_merge_per_univariate_id
                .get_mut(&univariate_id)
                .unwrap();
            indices_to_merge.push(None);
            indices_to_merge.push(Some(current_index));
        }

        univariate_id_to_previous_index.insert(univariate_id, current_index);
    }

    // If none of the segments can be merged return the original compressed
    // segments, otherwise return the smaller set of merged compressed segments.
    if can_segments_be_merged {
        let mut merged_compressed_segments = CompressedSegmentBatchBuilder::new(num_rows);
        let mut index_of_last_segment = 0;

        let mut timestamp_builder = TimestampBuilder::new();
        for (_, mut indices_to_merge) in indices_to_merge_per_univariate_id {
            indices_to_merge.push(None);
            for maybe_index in indices_to_merge {
                if let Some(index) = maybe_index {
                    // Merge timestamps.
                    let start_time = start_times.value(index);
                    let end_time = end_times.value(index);
                    let timestamps = timestamps.value(index);

                    timestamps::decompress_all_timestamps(
                        start_time,
                        end_time,
                        timestamps,
                        &mut timestamp_builder,
                    );

                    index_of_last_segment = index;
                } else {
                    let timestamps = timestamp_builder.finish();
                    let compressed_timestamps =
                        timestamps::compress_residual_timestamps(timestamps.values());

                    // Merge segments. The last segment's model is used for the merged
                    // segment as all of the segments contain the exact same model.
                    merged_compressed_segments.append_compressed_segment(
                        univariate_ids.value(index_of_last_segment),
                        model_type_ids.value(index_of_last_segment),
                        timestamps.value(0),
                        timestamps.value(timestamps.len() - 1),
                        &compressed_timestamps,
                        min_values.value(index_of_last_segment),
                        max_values.value(index_of_last_segment),
                        values.value(index_of_last_segment),
                        residuals.value(index_of_last_segment),
                        errors.value(index_of_last_segment),
                    );
                }
            }
        }
        Ok(merged_compressed_segments.finish())
    } else {
        Ok(compressed_segments)
    }
}

/// Return [`true`] if the segment at `previous_index` does not store any residuals and the models
/// at `previous_index` and `current_index` represent values from the same time series, are of the
/// same type, and are equivalent, otherwise [`false`]. Assumes the arrays are the same length and
/// that `previous_index` and `current_index` only access values in the arrays.
fn can_models_be_merged(
    previous_index: usize,
    current_index: usize,
    univariate_ids: &UInt64Array,
    model_type_ids: &UInt8Array,
    min_values: &ValueArray,
    max_values: &ValueArray,
    values: &BinaryArray,
    residuals: &BinaryArray,
) -> bool {
    // The query pipeline assumes residuals are values located after the values reconstructed by
    // the main model in the segment, thus residuals at previous_index is currently not supported.
    if previous_index != current_index && !residuals.value(previous_index).is_empty() {
        return false;
    }

    // The f32s are converted to u32s with the same bitwise representation as f32
    // and f64 does not implement std::hash::Hash and thus they cannot be hashed.
    (
        univariate_ids.value(previous_index),
        model_type_ids.value(previous_index),
        min_values.value(previous_index).to_bits(),
        max_values.value(previous_index).to_bits(),
        values.value(previous_index),
    ) == (
        univariate_ids.value(current_index),
        model_type_ids.value(current_index),
        min_values.value(current_index).to_bits(),
        max_values.value(current_index).to_bits(),
        values.value(current_index),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::UInt8Array;
    use modelardb_common::schemas::UNCOMPRESSED_SCHEMA;

    // Tests for try_merge_segments().
    #[test]
    fn test_merge_compressed_segments_empty_batch_wrong_schema() {
        assert!(try_merge_segments(RecordBatch::new_empty(UNCOMPRESSED_SCHEMA.0.clone())).is_err())
    }

    #[test]
    fn test_merge_compressed_segments_empty_batch_correct_schema() {
        let merged_record_batch =
            try_merge_segments(RecordBatch::new_empty(COMPRESSED_SCHEMA.0.clone())).unwrap();
        assert_eq!(0, merged_record_batch.num_rows())
    }

    #[test]
    fn test_merge_compressed_segments_batch() {
        // merge_segments() currently merge segments with equivalent models.
        let univariate_id = 1;
        let model_type_id = 1;
        let values = &[];
        let residuals = &[];
        let min_value = 5.0;
        let max_value = 5.0;

        // Add a mix of different segments that can be merged into two segments.
        let mut compressed_record_batch_builder = CompressedSegmentBatchBuilder::new(10);

        for start_time in (100..2100).step_by(400) {
            compressed_record_batch_builder.append_compressed_segment(
                univariate_id,
                model_type_id,
                start_time,
                start_time + 100,
                &[],
                min_value,
                max_value,
                values,
                residuals,
                0.0,
            );
        }

        for start_time in (2500..4500).step_by(400) {
            compressed_record_batch_builder.append_compressed_segment(
                univariate_id,
                model_type_id + 1,
                start_time + 200,
                start_time + 300,
                &[],
                -min_value,
                -max_value,
                values,
                residuals,
                10.0,
            );
        }

        let compressed_record_batch = compressed_record_batch_builder.finish();
        let merged_record_batch = try_merge_segments(compressed_record_batch).unwrap();

        // Extract the columns from the RecordBatch.
        modelardb_common::arrays!(
            merged_record_batch,
            _univariate_ids,
            _model_type_ids,
            start_times,
            end_times,
            timestamps,
            min_values,
            max_values,
            values,
            _residuals,
            errors
        );

        // Assert that the number of segments are correct.
        assert_eq!(2, merged_record_batch.num_rows());

        // Assert that the timestamps are correct.
        let mut decompressed_timestamps = TimestampBuilder::with_capacity(10);
        timestamps::decompress_all_timestamps(
            start_times.value(0),
            end_times.value(0),
            timestamps.value(0),
            &mut decompressed_timestamps,
        );
        assert_eq!(10, decompressed_timestamps.finish().len());

        timestamps::decompress_all_timestamps(
            start_times.value(1),
            end_times.value(1),
            timestamps.value(1),
            &mut decompressed_timestamps,
        );
        assert_eq!(10, decompressed_timestamps.finish().len());

        // Assert that the models are correct.
        let (positive, negative) = if start_times.value(0) == 100 {
            (0, 1)
        } else {
            (1, 0)
        };

        let value: &[u8] = &[];
        assert_eq!(value, values.value(positive));
        assert_eq!(min_value, min_values.value(positive));
        assert_eq!(max_value, max_values.value(positive));

        assert_eq!(value, values.value(negative));
        assert_eq!(-min_value, min_values.value(negative));
        assert_eq!(-max_value, max_values.value(negative));

        // Assert that the errors are correct.
        assert_eq!(0.0, errors.value(positive));
        assert_eq!(10.0, errors.value(negative));
    }

    // Tests for can_models_be_merged().
    #[test]
    fn test_equal_models_without_residuals_can_be_merged() {
        assert!(can_models_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([1, 1]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[1], [1]]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_equal_models_with_residuals_in_second_can_be_merged() {
        assert!(can_models_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([1, 1]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[1], [1]]),
            &BinaryArray::from_iter_values([vec![], vec![1]])
        ))
    }

    #[test]
    fn test_models_with_different_univariate_ids_cannot_be_merged() {
        assert!(!can_models_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 2]),
            &UInt8Array::from_iter_values([1, 1]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[1], [1]]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_models_with_different_model_types_cannot_be_merged() {
        assert!(!can_models_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([1, 2]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[1], [1]]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_models_with_different_min_values_cannot_be_merged() {
        assert!(!can_models_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([1, 1]),
            &ValueArray::from_iter_values([1.0, 2.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[1], [1]]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_models_with_different_max_values_cannot_be_merged() {
        assert!(!can_models_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([1, 1]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 3.0]),
            &BinaryArray::from_iter_values([[1], [1]]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_models_with_different_values_cannot_be_merged() {
        assert!(!can_models_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([1, 1]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[1], [2]]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_models_with_residuals_in_first_cannot_be_merged() {
        assert!(!can_models_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([1, 1]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[1], [1]]),
            &BinaryArray::from_iter_values([&vec![1], &vec![]])
        ))
    }

    #[test]
    fn test_models_with_residuals_in_both_cannot_be_merged() {
        assert!(!can_models_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([1, 1]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[1], [1]]),
            &BinaryArray::from_iter_values([[1], [1]])
        ))
    }
}
