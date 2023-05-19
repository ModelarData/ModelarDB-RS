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
use modelardb_common::types::{TimestampArray, Value, ValueArray};

use crate::models::gorilla::Gorilla;
use crate::models::{self, timestamps, GORILLA_ID};
use crate::types::{
    CompressedModelBuilder, CompressedSegmentBatchBuilder, ErrorBound, SelectedModel,
};

/// Maximum number of residuals that can be stored as part of a compressed segment. The number of
/// residuals in a segment is stored as the last value in the `residuals` [`BinaryArray`] so
/// [`GridExec`](crate::query::grid_exec::GridExec) can compute which timestamps are associated
/// with the residuals, so an [`u8`] is used for simplicity. Longer sub-sequences of data points
/// that are marked as residuals are stored as separate segments to allow for efficient pruning.
const RESIDUAL_VALUES_MAX_LENGTH: u8 = 0; // TODO: re-enable residuals after fixing the problem with overriding start/end and min/max.
                                          //const RESIDUAL_VALUES_MAX_LENGTH: u8 = 255;

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
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
    error_bound: ErrorBound,
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
    let mut compressed_record_batch_builder = CompressedSegmentBatchBuilder::new(end_index);

    // Compress the uncompressed timestamps and uncompressed values.
    let mut current_start_index = 0;
    let mut previous_selected_model: Option<SelectedModel> = None;
    while current_start_index < end_index {
        // Select a model to represent the values from current_start_index to index within
        // error_bound where index < end_index.
        let compressed_model_builder = CompressedModelBuilder::new(
            current_start_index,
            end_index,
            uncompressed_timestamps,
            uncompressed_values,
            error_bound,
        );

        let selected_model = compressed_model_builder.finish();

        // The selected model will only be stored as part of a compressed segment if it uses less
        // storage space per value than the uncompressed values it represents.
        if selected_model.bytes_per_value <= models::VALUE_SIZE_IN_BYTES as f32 {
            // Flush the previously selected model and any residual value if either exists.
            if current_start_index > 0 {
                store_compressed_segments_with_model_and_or_residuals(
                    univariate_id,
                    error_bound,
                    &previous_selected_model,
                    current_start_index - 1,
                    uncompressed_timestamps,
                    uncompressed_values,
                    &mut compressed_record_batch_builder,
                );
            }

            // Start fitting the next model to the first value located right after this model.
            current_start_index = selected_model.end_index + 1;

            // The selected model will be stored as part of a segment when the next model is
            // selected so the few residual values that may exist between them can be stored as
            // part of the segment with previous_selected_model instead of as a separate segment.
            previous_selected_model = Some(selected_model);
        } else {
            // The potentially lossy models could not efficiently encode the sub-sequence starting
            // at current_start_index, so residual values will instead be compressed using Gorilla.
            current_start_index += 1;
        }
    }

    store_compressed_segments_with_model_and_or_residuals(
        univariate_id,
        error_bound,
        &previous_selected_model,
        end_index - 1,
        uncompressed_timestamps,
        uncompressed_values,
        &mut compressed_record_batch_builder,
    );

    Ok(compressed_record_batch_builder.finish())
}

/// Create segment(s) that store `maybe_selected_model` and residual values as either:
/// - One compressed segment that stores `maybe_selected_model` and residuals if the number of
/// residuals are less than or equal to [`RESIDUAL_VALUES_MAX_LENGTH`].
/// - Two compressed segments with the first storing `maybe_selected_model` and the second storing
/// residuals if the number of residuals are greater than [`RESIDUAL_VALUES_MAX_LENGTH`].
/// - One compressed segment that stores residuals as a single model if `maybe_selected_model` is
/// [`None`].
fn store_compressed_segments_with_model_and_or_residuals(
    univariate_id: u64,
    error_bound: ErrorBound,
    maybe_selected_model: &Option<SelectedModel>,
    residual_end_index: usize,
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
    compressed_record_batch_builder: &mut CompressedSegmentBatchBuilder,
) {
    // If the first values in `uncompressed_values` are residuals they cannot be part of a segment.
    if let Some(selected_model) = maybe_selected_model {
        if (residual_end_index - selected_model.end_index) <= RESIDUAL_VALUES_MAX_LENGTH.into() {
            // Few or no residuals exists so the model and any residuals are put into one segment.
            store_selected_model_and_any_residual_in_a_segment(
                univariate_id,
                error_bound,
                selected_model,
                residual_end_index,
                uncompressed_timestamps,
                uncompressed_values,
                compressed_record_batch_builder,
            );
        } else {
            // Many residuals exists so the selected model and residuals are put into two segments.
            store_selected_model_and_any_residual_in_a_segment(
                univariate_id,
                error_bound,
                selected_model,
                selected_model.end_index, // No residuals are stored.
                uncompressed_timestamps,
                uncompressed_values,
                compressed_record_batch_builder,
            );

            store_residuals_as_a_model_in_a_separate_segment(
                univariate_id,
                error_bound,
                selected_model.end_index + 1,
                residual_end_index,
                uncompressed_timestamps,
                uncompressed_values,
                compressed_record_batch_builder,
            );
        }
    } else {
        // The residuals are stored as a separate segment as the first sub-sequence of values in
        // `uncompressed_values` are residuals, thus the residuals must be stored in a segment.
        store_residuals_as_a_model_in_a_separate_segment(
            univariate_id,
            error_bound,
            0,
            residual_end_index,
            uncompressed_timestamps,
            uncompressed_values,
            compressed_record_batch_builder,
        );
    }
}

/// Create a compressed segment that represents the values from `selected_model.start_index` to and
/// including `selected_model.end_index` as `selected_model.values` and, if
/// `selected_model.end_index < residuals_end_index`, the residuals from `selected_model.end_index
/// + 1` to and including `residuals_end_index`. Assumes the number of residuals are less than or
/// equal to [`RESIDUAL_VALUES_MAX_LENGTH`].
fn store_selected_model_and_any_residual_in_a_segment(
    univariate_id: u64,
    error_bound: ErrorBound,
    selected_model: &SelectedModel,
    residuals_end_index: usize,
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
    compressed_record_batch_builder: &mut CompressedSegmentBatchBuilder,
) {
    // Metadata that may be updated by residuals.
    let mut min_value = selected_model.min_value;
    let mut max_value = selected_model.max_value;
    let mut residuals = vec![];

    // Compress residual values using Gorilla if any exists.
    if selected_model.end_index < residuals_end_index {
        let model_last_value = uncompressed_values.value(selected_model.end_index);
        let residuals_start_index = selected_model.end_index + 1;
        let selected_model_for_residuals = compress_residual_value_range(
            error_bound,
            Some(model_last_value),
            residuals_start_index,
            residuals_end_index,
            uncompressed_values,
        );

        min_value = Value::min(min_value, selected_model_for_residuals.min_value);
        max_value = Value::max(max_value, selected_model_for_residuals.max_value);

        let residuals_length = (residuals_end_index - residuals_start_index) as u8 + 1;
        residuals = selected_model_for_residuals.values;
        residuals.push(residuals_length);
    };

    compress_timestamps_and_store_segment_in_batch_builder(
        univariate_id,
        selected_model.model_type_id,
        selected_model.start_index,
        residuals_end_index,
        min_value,
        max_value,
        &selected_model.values,
        &residuals,
        uncompressed_timestamps,
        compressed_record_batch_builder,
    );
}

/// For the time series with `univariate_id`, compress the values from `start_index` to and
/// including `end_index` in `uncompressed_values` using [`Gorilla`] and store the resulting model
/// with the corresponding timestamps from `uncompressed_timestamps` as a segment in
/// `compressed_record_batch_builder`.
fn store_residuals_as_a_model_in_a_separate_segment(
    univariate_id: u64,
    error_bound: ErrorBound,
    start_index: usize,
    end_index: usize,
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
    compressed_record_batch_builder: &mut CompressedSegmentBatchBuilder,
) {
    let selected_model = compress_residual_value_range(
        error_bound,
        None,
        start_index,
        end_index,
        uncompressed_values,
    );

    compress_timestamps_and_store_segment_in_batch_builder(
        univariate_id,
        selected_model.model_type_id,
        selected_model.start_index,
        selected_model.end_index,
        selected_model.min_value,
        selected_model.max_value,
        &selected_model.values,
        &[],
        uncompressed_timestamps,
        compressed_record_batch_builder,
    );
}

/// Compress the values from `start_index` to `end_index` in `uncompressed_values` using
/// [`Gorilla`]. If `maybe_model_last_value` is [`Some`] the first value is XOR'ed with it.
fn compress_residual_value_range(
    error_bound: ErrorBound,
    maybe_model_last_value: Option<f32>,
    start_index: usize,
    end_index: usize,
    uncompressed_values: &ValueArray,
) -> SelectedModel {
    let uncompressed_values = &uncompressed_values.values()[start_index..=end_index];

    let min_value = uncompressed_values
        .iter()
        .fold(Value::NAN, |current_min, value| {
            Value::min(current_min, *value)
        });

    let max_value = uncompressed_values
        .iter()
        .fold(Value::NAN, |current_max, value| {
            Value::max(current_max, *value)
        });

    let mut gorilla = Gorilla::new(error_bound);
    if let Some(last_value) = maybe_model_last_value {
        gorilla.compress_values_without_first(uncompressed_values, last_value);
    } else {
        gorilla.compress_values(uncompressed_values);
    }

    let bytes_per_value = gorilla.bytes_per_value();
    let values = gorilla.compressed_values();

    SelectedModel {
        model_type_id: GORILLA_ID,
        start_index,
        end_index,
        min_value,
        max_value,
        values,
        bytes_per_value,
    }
}

/// Compress the timestamps from `start_index` to `end_index` and store them with the model
/// (`min_value`, `max_value`, and `values`) and possible `residuals` in a compressed segment.
#[allow(clippy::too_many_arguments)]
fn compress_timestamps_and_store_segment_in_batch_builder(
    univariate_id: u64,
    model_type_id: u8,
    start_index: usize,
    end_index: usize,
    min_value: Value,
    max_value: Value,
    values: &[u8],
    residuals: &[u8],
    uncompressed_timestamps: &TimestampArray,
    compressed_record_batch_builder: &mut CompressedSegmentBatchBuilder,
) {
    let start_time = uncompressed_timestamps.value(start_index);
    let end_time = uncompressed_timestamps.value(end_index);
    let timestamps = timestamps::compress_residual_timestamps(
        &uncompressed_timestamps.values()[start_index..=end_index],
    );

    let error = f32::NAN; // TODO: compute and store the actual error.

    compressed_record_batch_builder.append_compressed_segment(
        univariate_id,
        model_type_id,
        start_time,
        end_time,
        &timestamps,
        min_value,
        max_value,
        values,
        residuals,
        error,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{BinaryArray, Float32Array, UInt64Array, UInt64Builder, UInt8Array};
    use modelardb_common::types::{Timestamp, TimestampBuilder, ValueBuilder};

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
            &[models::PMC_MEAN_ID, models::SWING_ID, models::GORILLA_ID],
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
            &uncompressed_timestamps,
            &uncompressed_values,
            error_bound,
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

        assert_eq!(
            uncompressed_timestamps.len(),
            timestamp_builder.finish().len()
        );
        assert_eq!(uncompressed_values.len(), value_builder.finish().len());
    }

    // Tests for compress_residual_value_range().
    #[test]
    fn test_compress_all_residual_value_range() {
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let uncompressed_values = ValueArray::from(vec![73.0, 37.0, 37.0, 37.0, 73.0]);
        let selected_model = compress_residual_value_range(
            error_bound,
            None,
            0,
            uncompressed_values.len() - 1,
            &uncompressed_values,
        );

        assert_eq!(GORILLA_ID, selected_model.model_type_id);
        assert_eq!(uncompressed_values.len() - 1, selected_model.end_index);
        assert_eq!(37.0, selected_model.min_value);
        assert_eq!(73.0, selected_model.max_value);
        assert_eq!(8, selected_model.values.len());
    }

    #[test]
    fn test_compress_some_residual_value_range() {
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let uncompressed_values = ValueArray::from(vec![73.0, 37.0, 37.0, 37.0, 73.0]);
        let selected_model =
            compress_residual_value_range(error_bound, None, 1, 3, &uncompressed_values);

        assert_eq!(GORILLA_ID, selected_model.model_type_id);
        assert_eq!(3, selected_model.end_index);
        assert_eq!(37.0, selected_model.min_value);
        assert_eq!(37.0, selected_model.max_value);
        assert_eq!(5, selected_model.values.len());
    }
}
