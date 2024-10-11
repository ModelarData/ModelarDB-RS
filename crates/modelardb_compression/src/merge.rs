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

use arrow::array::{Array, BinaryArray, Float32Array, UInt64Array, UInt8Array};
use arrow::record_batch::RecordBatch;
use modelardb_types::errors::ModelarDbError;
use modelardb_types::schemas::COMPRESSED_SCHEMA;
use modelardb_types::types::{TimestampArray, TimestampBuilder, ValueArray};

use crate::models::{self, timestamps};
use crate::types::CompressedSegmentBatchBuilder;

/// Merge segments in `compressed_segments` if their schema is [`COMPRESSED_SCHEMA`] and they:
/// * Are from same time series.
/// * Contain the exact same models.
/// * Are consecutive in terms of time.
///
/// Assumes that the segments for each `univariate_id` are sorted by time and if the consecutive
/// segments A, B, and C exist for a `univariate_id` and the segments A and C are in
/// `compressed_segments` then B is also in `compressed_segments`. If only A and C are in
/// `compressed_segments` a segment that overlaps with B will be created if A and C are merged.
pub fn try_merge_segments(compressed_segments: RecordBatch) -> Result<RecordBatch, ModelarDbError> {
    if compressed_segments.schema() != COMPRESSED_SCHEMA.0 {
        return Err(ModelarDbError::CompressionError(
            "The schema for the compressed segments is incorrect.".to_owned(),
        ));
    }

    modelardb_types::arrays!(
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

    let maybe_merge_indices_per_univariate_id = compute_mergeable_segments(
        univariate_ids,
        model_type_ids,
        start_times,
        end_times,
        min_values,
        max_values,
        values,
        residuals,
    );

    if let Some(merge_indices_per_univariate_id) = maybe_merge_indices_per_univariate_id {
        merge_segments(
            merge_indices_per_univariate_id,
            univariate_ids,
            model_type_ids,
            start_times,
            end_times,
            timestamps,
            min_values,
            max_values,
            values,
            residuals,
            errors,
        )
    } else {
        Ok(compressed_segments)
    }
}

/// Return which segments can be merged together for each `univariate_id`, if any, otherwise
/// [`None`] is returned. Assumes the arrays are the same length. The segments to be merged together
/// per `univariate_id` are sequences of `Some(segment_index)` separated by [`None`].
fn compute_mergeable_segments(
    univariate_ids: &UInt64Array,
    model_type_ids: &UInt8Array,
    start_times: &TimestampArray,
    end_times: &TimestampArray,
    min_values: &ValueArray,
    max_values: &ValueArray,
    values: &BinaryArray,
    residuals: &BinaryArray,
) -> Option<HashMap<u64, Vec<Option<usize>>>> {
    let mut can_any_segments_be_merged = false;
    let mut univariate_id_to_previous_index = HashMap::new();
    let mut merge_indices_per_univariate_id = HashMap::new();

    for current_index in 0..univariate_ids.len() {
        let univariate_id = univariate_ids.value(current_index);
        let previous_index = *univariate_id_to_previous_index
            .get(&univariate_id)
            .unwrap_or(&current_index);

        if segments_can_be_merged(
            previous_index,
            current_index,
            univariate_ids,
            model_type_ids,
            start_times,
            end_times,
            min_values,
            max_values,
            values,
            residuals,
        ) {
            merge_indices_per_univariate_id
                .entry(univariate_id)
                .or_insert_with(Vec::new)
                .push(Some(current_index));

            // segments_can_be_merged() always returns true if previous_index == current_index, but
            // fewer segments are only created if there are multiple different segments to merge.
            can_any_segments_be_merged |= previous_index != current_index;
        } else {
            // unwrap() is safe as a segment is guaranteed to match itself.
            let merge_indices = merge_indices_per_univariate_id
                .get_mut(&univariate_id)
                .unwrap();

            merge_indices.push(None);
            merge_indices.push(Some(current_index));
        }

        univariate_id_to_previous_index.insert(univariate_id, current_index);
    }

    // Mark the end of the last sequence of segments for each univariate id.
    for merge_indices in &mut merge_indices_per_univariate_id.values_mut() {
        merge_indices.push(None);
    }

    if can_any_segments_be_merged {
        Some(merge_indices_per_univariate_id)
    } else {
        None
    }
}

/// Return [`true`] if the segments are from the same time series, the time intervals the segments
/// represent data for do not overlap due to out of order data points, the segment at
/// `previous_index` does not store any residuals, their models are of the same type, and their
/// models can be merged, otherwise [`false`]. Assumes the arrays are the same length and that
/// `previous_index` and `current_index` both access values in the arrays.
fn segments_can_be_merged(
    previous_index: usize,
    current_index: usize,
    univariate_ids: &UInt64Array,
    model_type_ids: &UInt8Array,
    start_times: &TimestampArray,
    end_times: &TimestampArray,
    min_values: &ValueArray,
    max_values: &ValueArray,
    values: &BinaryArray,
    residuals: &BinaryArray,
) -> bool {
    // A segment can always be merged with itself.
    if previous_index == current_index {
        return true;
    }

    // Only segments from the same time series can be merged.
    if univariate_ids.value(previous_index) != univariate_ids.value(current_index) {
        return false;
    }

    // Segments with overlapping time intervals occurs when data points are being ingested
    // out-of-order across different batches. Such segments cannot be merged as the query engine
    // assumes all columns in a model table has the same sort order, and it would change the order
    // for a single column in the table if segments with out-of-order data points are merged.
    if end_times.value(previous_index) >= start_times.value(current_index) {
        return false;
    }

    // The query pipeline assumes residuals are values located after the values reconstructed by
    // the main model in the segment, thus residuals at previous_index is currently not supported.
    if !residuals.value(previous_index).is_empty() {
        return false;
    }

    models::can_be_merged(
        model_type_ids.value(previous_index),
        start_times.value(previous_index),
        end_times.value(previous_index),
        min_values.value(previous_index),
        max_values.value(previous_index),
        values.value(previous_index),
        model_type_ids.value(current_index),
        start_times.value(current_index),
        end_times.value(current_index),
        min_values.value(current_index),
        max_values.value(current_index),
        values.value(current_index),
    )
}

/// Merge compressed segments according to `merge_indices_per_univariate_id`. Assumes the arrays
/// are the same length.
fn merge_segments(
    merge_indices_per_univariate_id: HashMap<u64, Vec<Option<usize>>>,
    univariate_ids: &UInt64Array,
    model_type_ids: &UInt8Array,
    start_times: &TimestampArray,
    end_times: &TimestampArray,
    timestamps: &BinaryArray,
    min_values: &ValueArray,
    max_values: &ValueArray,
    values: &BinaryArray,
    residuals: &BinaryArray,
    errors: &Float32Array,
) -> Result<RecordBatch, ModelarDbError> {
    let mut merged_compressed_segments = CompressedSegmentBatchBuilder::new(univariate_ids.len());

    // merge_indices are sequences of segments to be merged in the form of Some(segment_index)
    // separated by None, while segment_index is the index of a segment stored in the arrays.
    for merge_indices in merge_indices_per_univariate_id.values() {
        let mut start_merge_index = 0;
        let mut end_merge_index = 0;

        for index in 0..merge_indices.len() {
            if merge_indices[index].is_some() {
                end_merge_index = index;
            } else if start_merge_index == end_merge_index {
                // unwrap() is safe as the range of index into merge_indices are all Some.
                let segment_index = merge_indices[start_merge_index].unwrap();

                // This sequence of mergeable segments only contain one segment.
                merged_compressed_segments.append_compressed_segment(
                    univariate_ids.value(segment_index),
                    model_type_ids.value(segment_index),
                    start_times.value(segment_index),
                    end_times.value(segment_index),
                    timestamps.value(segment_index),
                    min_values.value(segment_index),
                    max_values.value(segment_index),
                    values.value(segment_index),
                    residuals.value(segment_index),
                    errors.value(segment_index),
                );

                start_merge_index = end_merge_index + 2;
            } else {
                // Merge timestamps and compute the new segment's minimum and maximum value.
                let mut timestamp_builder = TimestampBuilder::new();

                // NaN is used as the initial value as it will be overwritten be any value. Thus, it
                // is the only initial value that preserves a minimum and maximum value that is NaN.
                let mut min_value = f32::NAN;
                let mut max_value = f32::NAN;

                for maybe_segment_index in merge_indices
                    .iter()
                    .take(end_merge_index + 1)
                    .skip(start_merge_index)
                {
                    // unwrap() is safe as the range of index into merge_indices are all Some.
                    let segment_index = maybe_segment_index.unwrap();

                    timestamps::decompress_all_timestamps(
                        start_times.value(segment_index),
                        end_times.value(segment_index),
                        timestamps.value(segment_index),
                        &mut timestamp_builder,
                    );

                    min_value = f32::min(min_value, min_values.value(segment_index));
                    max_value = f32::max(max_value, max_values.value(segment_index));
                }

                let timestamps = timestamp_builder.finish();
                let compressed_timestamps =
                    timestamps::compress_residual_timestamps(timestamps.values());

                // Merge the models.
                // unwrap() is safe as the range of index into merge_indices are all Some.
                let segment_index = merge_indices[end_merge_index].unwrap();
                let model_type_id = model_type_ids.value(segment_index);
                let values = values.value(segment_index);
                let residuals = residuals.value(segment_index);
                let values = models::merge(model_type_id, min_value, max_value, values);

                // The metadata is the same for all segments, so it is read from the last segment. In
                // addition, since only the last segment may contain residuals, they are copied.
                merged_compressed_segments.append_compressed_segment(
                    univariate_ids.value(segment_index),
                    model_type_ids.value(segment_index),
                    timestamps.value(0),
                    timestamps.value(timestamps.len() - 1),
                    &compressed_timestamps,
                    min_value,
                    max_value,
                    &values,
                    residuals,
                    errors.value(segment_index),
                );

                start_merge_index = end_merge_index + 2;
            }
        }
    }

    Ok(merged_compressed_segments.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::array::{UInt64Builder, UInt8Array};
    use arrow::compute;
    use modelardb_common::test::ERROR_BOUND_ZERO;
    use modelardb_types::schemas::UNCOMPRESSED_SCHEMA;
    use modelardb_types::types::{ErrorBound, ValueBuilder};

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
        // Create uncompressed data points.
        let uncompressed_timestamps =
            Arc::new(TimestampArray::from_iter_values((10..8010).step_by(10)));

        let uncompressed_values = {
            let mut uncompressed_values_builder = ValueBuilder::new();
            uncompressed_values_builder.append_slice(&[100.0; 200]);
            uncompressed_values_builder.append_slice(&[100.0; 200]);

            uncompressed_values_builder
                .append_slice(&(0..200).map(|value| value as f32).collect::<Vec<f32>>());
            uncompressed_values_builder
                .append_slice(&(200..400).map(|value| value as f32).collect::<Vec<f32>>());

            Arc::new(uncompressed_values_builder.finish())
        };

        // Compress and merge segments.
        let compressed_segments = compress(200, &uncompressed_timestamps, &uncompressed_values);
        let merged_compressed_segments = try_merge_segments(compressed_segments.clone()).unwrap();

        // Decompress merged segments.
        modelardb_types::arrays!(
            merged_compressed_segments,
            univariate_ids,
            model_type_ids,
            start_times,
            end_times,
            timestamps,
            min_values,
            max_values,
            values,
            residuals,
            _errors
        );

        let mut univariate_id_builder = UInt64Builder::new();
        let mut timestamp_builder = TimestampBuilder::new();
        let mut value_builder = ValueBuilder::new();

        for row_index in 0..merged_compressed_segments.num_rows() {
            crate::grid(
                univariate_ids.value(row_index),
                model_type_ids.value(row_index),
                start_times.value(row_index),
                end_times.value(row_index),
                timestamps.value(row_index),
                min_values.value(row_index),
                max_values.value(row_index),
                values.value(row_index),
                residuals.value(row_index),
                &mut univariate_id_builder,
                &mut timestamp_builder,
                &mut value_builder,
            );
        }

        let decompressed_timestamps = Arc::new(timestamp_builder.finish());
        let decompressed_values = Arc::new(value_builder.finish());

        // Assert that the segments were created, were merged, and can be decompressed correctly.
        assert_eq!(4, compressed_segments.num_rows());
        assert_eq!(2, merged_compressed_segments.num_rows());
        assert_eq!(uncompressed_timestamps, decompressed_timestamps);
        assert_eq!(uncompressed_values, decompressed_values);
    }

    fn compress(
        batch_size: usize,
        uncompressed_timestamps: &TimestampArray,
        uncompressed_values: &ValueArray,
    ) -> RecordBatch {
        let mut index = 0;
        let mut input_batches = vec![];

        while index < uncompressed_timestamps.len() {
            let compressed_segments = crate::try_compress(
                1,
                ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
                &uncompressed_timestamps.slice(index, batch_size),
                &uncompressed_values.slice(index, batch_size),
            )
            .unwrap();

            input_batches.push(compressed_segments);
            index += batch_size;
        }

        compute::concat_batches(&input_batches[0].schema(), &input_batches).unwrap()
    }

    // Tests for can_models_be_merged().
    #[test]
    fn test_equivalent_pmc_mean_models_can_be_merged() {
        assert!(segments_can_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([models::PMC_MEAN_ID, models::PMC_MEAN_ID]),
            &TimestampArray::from_iter_values([100, 300]),
            &TimestampArray::from_iter_values([200, 400]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[], []]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_different_pmc_mean_models_cannot_be_merged() {
        assert!(!segments_can_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([models::PMC_MEAN_ID, models::PMC_MEAN_ID]),
            &TimestampArray::from_iter_values([100, 300]),
            &TimestampArray::from_iter_values([200, 400]),
            &ValueArray::from_iter_values([1.0, 1.5]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[], []]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_equivalent_swing_mean_models_can_be_merged() {
        assert!(segments_can_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([models::SWING_ID, models::SWING_ID]),
            &TimestampArray::from_iter_values([100, 300]),
            &TimestampArray::from_iter_values([200, 400]),
            &ValueArray::from_iter_values([1.0, 3.0]),
            &ValueArray::from_iter_values([2.0, 4.0]),
            &BinaryArray::from_iter_values([[], []]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_different_swing_models_cannot_be_merged() {
        assert!(!segments_can_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([models::SWING_ID, models::SWING_ID]),
            &TimestampArray::from_iter_values([100, 300]),
            &TimestampArray::from_iter_values([200, 400]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[], []]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_models_with_different_univariate_ids_cannot_be_merged() {
        assert!(!segments_can_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 2]),
            &UInt8Array::from_iter_values([models::PMC_MEAN_ID, models::PMC_MEAN_ID]),
            &TimestampArray::from_iter_values([100, 300]),
            &TimestampArray::from_iter_values([200, 400]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[], []]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_models_with_overlapping_timestamps_cannot_be_merged() {
        assert!(!segments_can_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([models::PMC_MEAN_ID, models::PMC_MEAN_ID]),
            &TimestampArray::from_iter_values([100, 200]),
            &TimestampArray::from_iter_values([300, 400]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[], []]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_models_with_different_model_types_cannot_be_merged() {
        assert!(!segments_can_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([models::PMC_MEAN_ID, models::SWING_ID]),
            &TimestampArray::from_iter_values([100, 300]),
            &TimestampArray::from_iter_values([200, 400]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[], []]),
            &BinaryArray::from_iter_values([[], []])
        ))
    }

    #[test]
    fn test_models_with_residuals_in_first_segment_cannot_be_merged() {
        assert!(!segments_can_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([models::PMC_MEAN_ID, models::PMC_MEAN_ID]),
            &TimestampArray::from_iter_values([100, 300]),
            &TimestampArray::from_iter_values([200, 400]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[], []]),
            &BinaryArray::from_iter_values([&vec![1], &vec![]])
        ))
    }

    #[test]
    fn test_models_with_residuals_in_second_segment_can_be_merged() {
        assert!(segments_can_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([models::PMC_MEAN_ID, models::PMC_MEAN_ID]),
            &TimestampArray::from_iter_values([100, 300]),
            &TimestampArray::from_iter_values([200, 400]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[], []]),
            &BinaryArray::from_iter_values([&vec![], &vec![1]])
        ))
    }

    #[test]
    fn test_models_with_residuals_in_both_segments_cannot_be_merged() {
        assert!(!segments_can_be_merged(
            0,
            1,
            &UInt64Array::from_iter_values([1, 1]),
            &UInt8Array::from_iter_values([models::PMC_MEAN_ID, models::PMC_MEAN_ID]),
            &TimestampArray::from_iter_values([100, 300]),
            &TimestampArray::from_iter_values([200, 400]),
            &ValueArray::from_iter_values([1.0, 1.0]),
            &ValueArray::from_iter_values([2.0, 2.0]),
            &BinaryArray::from_iter_values([[], []]),
            &BinaryArray::from_iter_values([[1], [1]])
        ))
    }
}
