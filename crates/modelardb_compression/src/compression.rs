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

//! Compress batches of sorted data points represented by a [`TimestampArray`] and a [`ValueArray`]
//! using the model types in [`models`] to produce compressed segments containing metadata and
//! models.

use arrow::record_batch::RecordBatch;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::schemas::COMPRESSED_SCHEMA;
use modelardb_common::types::{TimestampArray, ValueArray};

use crate::models::gorilla::Gorilla;
use crate::models::{self, timestamps, GORILLA_ID};
use crate::types::{
    CompressedSegmentBatchBuilder, CompressedSegmentBuilder, ErrorBound, ModelBuilder,
};

/// Maximum number of residuals that can be stored as part of a compressed segment. The number of
/// residuals in a segment is stored as the last value in the `residuals` [`BinaryArray`] so
/// [`GridExec`](crate::query::grid_exec::GridExec) can compute which timestamps are associated
/// with the residuals, so an [`u8`] is used for simplicity. Longer sub-sequences of data points
/// that are marked as residuals are stored as separate segments to allow for efficient pruning.
const RESIDUAL_VALUES_MAX_LENGTH: u8 = 255;

/// Compress `uncompressed_timestamps` using a start time, end time, and a sampling interval if
/// regular and delta-of-deltas followed by a variable length binary encoding if irregular.
/// `uncompressed_values` is compressed within `error_bound` using the model types in `models`.
/// Assumes `uncompressed_timestamps` and `uncompressed_values` are sorted according to
/// `uncompressed_timestamps`. Returns [`CompressionError`](ModelarDbError::CompressionError) if
/// `uncompressed_timestamps` and `uncompressed_values` have different lengths, otherwise the
/// resulting compressed segments are returned as a [`RecordBatch`] with the [`COMPRESSED_SCHEMA`]
/// schema.
pub fn try_compress(
    univariate_id: u64,
    error_bound: ErrorBound,
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
) -> Result<RecordBatch, ModelarDbError> {
    // The uncompressed data must be passed as arrays instead of a RecordBatch as a TimestampArray
    // and a ValueArray is the only supported input. However, as a result it is necessary to verify
    // they have the same length.
    if uncompressed_timestamps.len() != uncompressed_values.len() {
        return Err(ModelarDbError::CompressionError(
            "Uncompressed timestamps and uncompressed values have different lengths.".to_owned(),
        ));
    }

    // If there is no uncompressed data to compress, an empty [`RecordBatch`] can be returned.
    if uncompressed_timestamps.is_empty() {
        return Ok(RecordBatch::new_empty(COMPRESSED_SCHEMA.0.clone()));
    }

    // Enough memory for end_index compressed segments are allocated to never require reallocation
    // as one compressed segment is created per data point in the absolute worst case.
    let end_index = uncompressed_timestamps.len();
    let mut compressed_segment_batch_builder = CompressedSegmentBatchBuilder::new(end_index);

    // Compress the uncompressed timestamps and uncompressed values.
    let mut current_start_index = 0;
    let mut previous_model: Option<CompressedSegmentBuilder> = None;
    while current_start_index < end_index {
        // Select a model to represent the values from current_start_index to index within
        // error_bound where index < end_index.
        let model = fit_next_model(
            current_start_index,
            error_bound,
            uncompressed_timestamps,
            uncompressed_values,
        );

        // The model will only be stored as part of a compressed segment if it uses less storage
        // space per value than the uncompressed values it represents.
        if model.bytes_per_value <= models::VALUE_SIZE_IN_BYTES as f32 {
            // Flush the previous model and any residual value if either exists.
            if current_start_index > 0 {
                store_compressed_segments_with_model_and_or_residuals(
                    univariate_id,
                    error_bound,
                    previous_model,
                    current_start_index - 1,
                    uncompressed_timestamps,
                    uncompressed_values,
                    &mut compressed_segment_batch_builder,
                );
            }

            // Start fitting the next model to the first value located right after this model.
            current_start_index = model.end_index + 1;

            // The model will be stored as part of a segment when the next model is selected so the
            // few residual values that may exist between them can be stored as part of the segment
            // with previous_model instead of as a separate segment.
            previous_model = Some(model);
        } else {
            // The potentially lossy models could not efficiently encode the sub-sequence starting
            // at current_start_index, so residual values will instead be compressed using Gorilla.
            current_start_index += 1;
        }
    }

    store_compressed_segments_with_model_and_or_residuals(
        univariate_id,
        error_bound,
        previous_model,
        end_index - 1,
        uncompressed_timestamps,
        uncompressed_values,
        &mut compressed_segment_batch_builder,
    );

    Ok(compressed_segment_batch_builder.finish())
}

// This method is defined as pub(crate) so it can be used in the tests in types.rs.
/// Create a model that represents the values in `uncompressed_values` from `start_index` to
/// index within `error_bound` where index <= `end_index`.
pub(crate) fn fit_next_model(
    current_start_index: usize,
    error_bound: ErrorBound,
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
) -> CompressedSegmentBuilder {
    let mut model_builder = ModelBuilder::new(current_start_index, error_bound);

    let mut current_index = current_start_index;
    let end_index = uncompressed_timestamps.len();
    while model_builder.can_fit_more() && current_index < end_index {
        let timestamp = uncompressed_timestamps.value(current_index);
        let value = uncompressed_values.value(current_index);
        model_builder.try_to_update_models(timestamp, value);
        current_index += 1;
    }

    model_builder.finish()
}

/// Create segment(s) that store `maybe_model` and residual values as either:
/// - One compressed segment that stores `maybe_model` and residuals if the number of
/// residuals are less than or equal to [`RESIDUAL_VALUES_MAX_LENGTH`].
/// - Two compressed segments with the first storing `maybe_model` and the second storing
/// residuals if the number of residuals are greater than [`RESIDUAL_VALUES_MAX_LENGTH`].
/// - One compressed segment that stores residuals as a single model if `maybe_model` is
/// [`None`].
fn store_compressed_segments_with_model_and_or_residuals(
    univariate_id: u64,
    error_bound: ErrorBound,
    maybe_model: Option<CompressedSegmentBuilder>,
    residuals_end_index: usize,
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
    compressed_segment_batch_builder: &mut CompressedSegmentBatchBuilder,
) {
    // If the first values in `uncompressed_values` are residuals they cannot be part of a segment.
    if let Some(model) = maybe_model {
        if (residuals_end_index - model.end_index) <= RESIDUAL_VALUES_MAX_LENGTH.into() {
            // Few or no residuals exists so the model and any residuals are put into one segment.
            model.finish(
                univariate_id,
                error_bound,
                residuals_end_index,
                uncompressed_timestamps,
                uncompressed_values,
                compressed_segment_batch_builder,
            );
        } else {
            // Many residuals exists so the model and residuals are put into two segments.
            let model_end_index = model.end_index;

            model.finish(
                univariate_id,
                error_bound,
                model_end_index, // No residuals are stored.
                uncompressed_timestamps,
                uncompressed_values,
                compressed_segment_batch_builder,
            );

            compress_and_store_residuals_in_a_separate_segment(
                univariate_id,
                error_bound,
                model_end_index + 1,
                residuals_end_index,
                uncompressed_timestamps,
                uncompressed_values,
                compressed_segment_batch_builder,
            );
        }
    } else {
        // The residuals are stored as a separate segment as the first sub-sequence of values in
        // `uncompressed_values` are residuals, thus the residuals must be stored in a segment.
        compress_and_store_residuals_in_a_separate_segment(
            univariate_id,
            error_bound,
            0,
            residuals_end_index,
            uncompressed_timestamps,
            uncompressed_values,
            compressed_segment_batch_builder,
        );
    }
}

/// For the time series with `univariate_id`, compress the values from `start_index` to and
/// including `end_index` in `uncompressed_values` using [`Gorilla`] and store the resulting model
/// with the corresponding timestamps from `uncompressed_timestamps` as a segment in
/// `compressed_segment_batch_builder`.
fn compress_and_store_residuals_in_a_separate_segment(
    univariate_id: u64,
    error_bound: ErrorBound,
    start_index: usize,
    end_index: usize,
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
    compressed_segment_batch_builder: &mut CompressedSegmentBatchBuilder,
) {
    // Compress the timestamps for the values stored in this segment without residuals.
    let start_time = uncompressed_timestamps.value(start_index);
    let end_time = uncompressed_timestamps.value(end_index);
    let timestamps = timestamps::compress_residual_timestamps(
        &uncompressed_timestamps.values()[start_index..=end_index],
    );

    // Compute metadata and compress the values stored in this segment without residuals.
    let uncompressed_values = &uncompressed_values.values()[start_index..=end_index];
    let mut gorilla = Gorilla::new(error_bound);
    gorilla.compress_values(uncompressed_values);

    let (values, min_value, max_value) = gorilla.model();

    compressed_segment_batch_builder.append_compressed_segment(
        univariate_id,
        GORILLA_ID,
        start_time,
        end_time,
        &timestamps,
        min_value,
        max_value,
        &values,
        &[],
        f32::NAN, // TODO: compute and store the actual error.
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{BinaryArray, Float32Array, UInt64Array, UInt64Builder, UInt8Array};
    use modelardb_common::types::{Timestamp, TimestampBuilder, Value, ValueBuilder};

    use crate::models;
    use crate::test_util::{self, StructureOfValues};

    const ERROR_BOUND_ZERO: f32 = 0.0;
    const ERROR_BOUND_FIVE: f32 = 5.0;
    const TRY_COMPRESS_TEST_LENGTH: usize = 50;

    // Tests for try_compress().
    #[test]
    fn test_try_compress_empty_time_series() {
        let values = vec![];
        let timestamps = vec![];
        let (_, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);

        assert_eq!(0, compressed_record_batch.num_rows())
    }

    #[test]
    fn test_try_compress_regular_constant_time_series() {
        let timestamps = test_util::generate_timestamps(TRY_COMPRESS_TEST_LENGTH, false);
        let mut values =
            test_util::generate_values(&timestamps, StructureOfValues::Constant, None, None);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_try_compress_irregular_constant_time_series() {
        let timestamps = test_util::generate_timestamps(TRY_COMPRESS_TEST_LENGTH, true);
        let mut values =
            test_util::generate_values(&timestamps, StructureOfValues::Constant, None, None);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_try_compress_regular_almost_constant_time_series() {
        let timestamps = test_util::generate_timestamps(TRY_COMPRESS_TEST_LENGTH, false);
        let mut values = test_util::generate_values(
            &timestamps,
            StructureOfValues::Random,
            Some(9.8),
            Some(10.2),
        );

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_FIVE);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_try_compress_irregular_almost_constant_time_series() {
        let timestamps = test_util::generate_timestamps(TRY_COMPRESS_TEST_LENGTH, true);
        let mut values = test_util::generate_values(
            &timestamps,
            StructureOfValues::Random,
            Some(9.8),
            Some(10.2),
        );

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_FIVE);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_try_compress_regular_linear_time_series() {
        let timestamps = test_util::generate_timestamps(TRY_COMPRESS_TEST_LENGTH, false);
        let mut values =
            test_util::generate_values(&timestamps, StructureOfValues::Linear, None, None);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_try_compress_irregular_linear_time_series() {
        let timestamps = test_util::generate_timestamps(TRY_COMPRESS_TEST_LENGTH, true);
        let mut values =
            test_util::generate_values(&timestamps, StructureOfValues::Linear, None, None);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_FIVE);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_try_compress_regular_almost_linear_time_series() {
        let timestamps = test_util::generate_timestamps(TRY_COMPRESS_TEST_LENGTH, false);
        let mut values = test_util::generate_values(
            &timestamps,
            StructureOfValues::AlmostLinear,
            Some(9.8),
            Some(10.2),
        );

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_FIVE);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_try_compress_irregular_almost_linear_time_series() {
        let timestamps = test_util::generate_timestamps(TRY_COMPRESS_TEST_LENGTH, true);
        let mut values = test_util::generate_values(
            &timestamps,
            StructureOfValues::AlmostLinear,
            Some(9.8),
            Some(10.2),
        );

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_FIVE);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_try_compress_regular_random_time_series() {
        let timestamps = test_util::generate_timestamps(TRY_COMPRESS_TEST_LENGTH, false);
        let mut values = test_util::generate_values(
            &timestamps,
            StructureOfValues::Random,
            Some(0.0),
            Some(f32::MAX),
        );

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::GORILLA_ID],
        )
    }

    #[test]
    fn test_try_compress_irregular_random_time_series() {
        let timestamps = test_util::generate_timestamps(TRY_COMPRESS_TEST_LENGTH, true);
        let mut values = test_util::generate_values(
            &timestamps,
            StructureOfValues::Random,
            Some(0.0),
            Some(f32::MAX),
        );

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::GORILLA_ID],
        )
    }

    #[test]
    fn test_try_compress_regular_random_linear_constant_time_series() {
        let timestamps = test_util::generate_timestamps(3 * TRY_COMPRESS_TEST_LENGTH, false);
        try_compress_random_linear_constant_time_series(timestamps);
    }

    #[test]
    fn test_try_compress_irregular_random_linear_constant_time_series() {
        let timestamps = test_util::generate_timestamps(3 * TRY_COMPRESS_TEST_LENGTH, true);
        try_compress_random_linear_constant_time_series(timestamps);
    }

    fn try_compress_random_linear_constant_time_series(timestamps: Vec<i64>) {
        let mut random = test_util::generate_values(
            &timestamps[0..TRY_COMPRESS_TEST_LENGTH],
            StructureOfValues::Random,
            Some(0.0),
            Some(f32::MAX),
        );
        let mut linear = test_util::generate_values(
            &timestamps[TRY_COMPRESS_TEST_LENGTH..2 * TRY_COMPRESS_TEST_LENGTH],
            StructureOfValues::Linear,
            None,
            None,
        );
        let mut constant = test_util::generate_values(
            &timestamps[2 * TRY_COMPRESS_TEST_LENGTH..],
            StructureOfValues::Constant,
            None,
            None,
        );
        let mut values = vec![];
        values.append(&mut random);
        values.append(&mut linear);
        values.append(&mut constant);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::GORILLA_ID, models::SWING_ID, models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_try_compress_constant_linear_random_regular_time_series() {
        let timestamps = test_util::generate_timestamps(3 * TRY_COMPRESS_TEST_LENGTH, false);
        try_compress_constant_linear_random_time_series(timestamps);
    }

    #[test]
    fn test_try_compress_constant_linear_random_irregular_time_series() {
        let timestamps = test_util::generate_timestamps(3 * TRY_COMPRESS_TEST_LENGTH, true);
        try_compress_constant_linear_random_time_series(timestamps);
    }

    fn try_compress_constant_linear_random_time_series(timestamps: Vec<i64>) {
        let mut constant = test_util::generate_values(
            &timestamps[0..TRY_COMPRESS_TEST_LENGTH],
            StructureOfValues::Constant,
            None,
            None,
        );
        let mut linear = test_util::generate_values(
            &timestamps[TRY_COMPRESS_TEST_LENGTH..2 * TRY_COMPRESS_TEST_LENGTH],
            StructureOfValues::Linear,
            None,
            None,
        );
        let mut random = test_util::generate_values(
            &timestamps[2 * TRY_COMPRESS_TEST_LENGTH..],
            StructureOfValues::Random,
            Some(0.0),
            Some(f32::MAX),
        );
        let mut values = vec![];
        values.append(&mut constant);
        values.append(&mut linear);
        values.append(&mut random);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);
        let uncompressed_values = ValueArray::from_iter_values(values.drain(..));

        assert_compressed_record_batch_with_segments_from_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID, models::SWING_ID],
        )
    }

    fn create_uncompressed_time_series(
        timestamps: &[Timestamp],
        values: &[Value],
    ) -> (TimestampArray, ValueArray) {
        let mut timestamps_builder = TimestampBuilder::with_capacity(timestamps.len());
        timestamps_builder.append_slice(timestamps);
        let mut values_builder = ValueBuilder::with_capacity(values.len());
        values_builder.append_slice(values);
        (timestamps_builder.finish(), values_builder.finish())
    }

    fn create_and_compress_time_series(
        values: &[f32],
        timestamps: &[i64],
        error_bound: f32,
    ) -> (TimestampArray, RecordBatch) {
        let (uncompressed_timestamps, uncompressed_values) =
            create_uncompressed_time_series(timestamps, values);
        let error_bound = ErrorBound::try_new(error_bound).unwrap();
        let compressed_record_batch = try_compress(
            1,
            error_bound,
            &uncompressed_timestamps,
            &uncompressed_values,
        )
        .unwrap();
        (uncompressed_timestamps, compressed_record_batch)
    }

    fn assert_compressed_record_batch_with_segments_from_time_series(
        uncompressed_timestamps: &TimestampArray,
        uncompressed_values: &ValueArray,
        compressed_record_batch: &RecordBatch,
        expected_model_type_ids: &[u8],
    ) {
        assert_eq!(
            expected_model_type_ids.len(),
            compressed_record_batch.num_rows()
        );

        let mut univariate_id_builder = UInt64Builder::new();
        let mut timestamp_builder = TimestampBuilder::new();
        let mut value_builder = ValueBuilder::new();

        modelardb_common::arrays!(
            compressed_record_batch,
            univariate_ids,
            model_type_ids,
            start_times,
            end_times,
            timestamps,
            min_values,
            max_values,
            values,
            residuals,
            _error_array
        );

        for (row_index, expected_model_type_id) in expected_model_type_ids.iter().enumerate() {
            models::grid(
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

            assert_eq!(model_type_ids.value(row_index), *expected_model_type_id);
        }

        let decompressed_timestamps = timestamp_builder.finish();
        let decompressed_values = value_builder.finish();

        assert_eq!(uncompressed_timestamps, &decompressed_timestamps);
        assert_eq!(decompressed_timestamps.len(), decompressed_values.len());
        assert_eq!(uncompressed_values.len(), decompressed_values.len());
    }

    // Tests for compress_and_store_residuals_in_a_separate_segment().
    #[test]
    fn test_compress_and_store_residuals_in_a_separate_segment() {
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let uncompressed_timestamps = TimestampArray::from_iter_values((100..=500).step_by(100));
        let uncompressed_values = ValueArray::from(vec![73.0, 37.0, 37.0, 37.0, 73.0]);
        let mut compressed_segment_batch_builder = CompressedSegmentBatchBuilder::new(1);

        compress_and_store_residuals_in_a_separate_segment(
            0,
            error_bound,
            0,
            uncompressed_timestamps.len() - 1,
            &uncompressed_timestamps,
            &uncompressed_values,
            &mut compressed_segment_batch_builder,
        );

        let compressed_segments = compressed_segment_batch_builder.finish();
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

        assert_eq!(1, compressed_segments.num_rows());
        assert_eq!(0, univariate_ids.value(0));
        assert_eq!(GORILLA_ID, model_type_ids.value(0));
        assert_eq!(100, start_times.value(0));
        assert_eq!(500, end_times.value(0));
        assert_eq!(1, timestamps.value(0).len());
        assert_eq!(5, timestamps.value(0)[0]);
        assert_eq!(37.0, min_values.value(0));
        assert_eq!(73.0, max_values.value(0));
        assert_eq!(8, values.value(0).len());
        assert!(residuals.value(0).is_empty());
        assert!(errors.value(0).is_nan());
    }
}
