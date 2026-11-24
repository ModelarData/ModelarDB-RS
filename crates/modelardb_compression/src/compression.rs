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

use std::sync::Arc;

use arrow::array::StringViewArray;
use arrow::compute::{self, SortColumn, SortOptions};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use modelardb_types::types::{ErrorBound, TimeSeriesTableMetadata, TimestampArray, ValueArray};

use crate::error::{ModelarDbCompressionError, Result};
use crate::models::macaque_v::MacaqueV;
use crate::models::{self, MACAQUE_V_ID, timestamps};
use crate::types::{CompressedSegmentBatchBuilder, CompressedSegmentBuilder, ModelBuilder};

/// Maximum number of residuals that can be stored as part of a compressed segment. The number of
/// residuals in a segment is stored as the last value in the `residuals` [`BinaryViewArray`] so
/// [`GridExec`](crate::query::grid_exec::GridExec) can compute which timestamps are associated
/// with the residuals, so an [`u8`] is used for simplicity. Longer sub-sequences of data points
/// that are marked as residuals are stored as separate segments to allow for efficient pruning.
const RESIDUAL_VALUES_MAX_LENGTH: u8 = 255;

/// Compress the `uncompressed_time_series` from the table with `time_series_table_metadata` and
/// return the resulting segments.
pub fn try_compress_multivariate_time_series(
    time_series_table_metadata: &TimeSeriesTableMetadata,
    uncompressed_time_series: &RecordBatch,
) -> Result<Vec<RecordBatch>> {
    // Sort by all tags and then time to simplify splitting the data into time series.
    let sorted_uncompressed_data =
        sort_time_series_by_tags_and_time(time_series_table_metadata, uncompressed_time_series)?;

    // Split the sorted uncompressed data into time series and compress them separately.
    let mut compressed_data = vec![];

    let tag_column_arrays: Vec<&StringViewArray> = time_series_table_metadata
        .tag_column_indices
        .iter()
        .map(|index| modelardb_types::array!(sorted_uncompressed_data, *index, StringViewArray))
        .collect();

    let mut tag_values = Vec::with_capacity(tag_column_arrays.len());
    for tag_column_array in &tag_column_arrays {
        tag_values.push(tag_column_array.value(0).to_owned());
    }

    // The index of the first data point of each time series must be stored so slices
    // containing only data points for each time series can be extracted and compressed.
    let mut row_index_start = 0;
    for row_index in 0..sorted_uncompressed_data.num_rows() {
        // If any of the tags differ, the data point is from a new time series.
        let mut is_new_time_series = false;
        for tag_column_index in 0..tag_column_arrays.len() {
            is_new_time_series |= tag_values[tag_column_index]
                != tag_column_arrays[tag_column_index].value(row_index);
        }

        if is_new_time_series {
            let time_series_length = row_index - row_index_start;
            let uncompressed_time_series =
                sorted_uncompressed_data.slice(row_index_start, time_series_length);

            try_split_and_compress_univariate_time_series(
                time_series_table_metadata,
                &uncompressed_time_series,
                &tag_values,
                &mut compressed_data,
            )?;

            for (tag_column_index, tag_column_array) in tag_column_arrays.iter().enumerate() {
                tag_values[tag_column_index] = tag_column_array.value(row_index).to_owned();
            }

            row_index_start = row_index;
        }
    }

    let time_series_length = sorted_uncompressed_data.num_rows() - row_index_start;
    let uncompressed_time_series =
        sorted_uncompressed_data.slice(row_index_start, time_series_length);

    try_split_and_compress_univariate_time_series(
        time_series_table_metadata,
        &uncompressed_time_series,
        &tag_values,
        &mut compressed_data,
    )?;

    Ok(compressed_data)
}

/// Sort the `uncompressed_data` from the time series table with `time_series_table_metadata`
/// according to its tags and then timestamps.
fn sort_time_series_by_tags_and_time(
    time_series_table_metadata: &TimeSeriesTableMetadata,
    uncompressed_time_series: &RecordBatch,
) -> Result<RecordBatch> {
    let mut sort_columns = vec![];

    let sort_options = Some(SortOptions {
        descending: false,
        nulls_first: false,
    });

    for tag_column_index in &time_series_table_metadata.tag_column_indices {
        let tag_column = uncompressed_time_series.column(*tag_column_index);
        sort_columns.push(SortColumn {
            values: (*tag_column).clone(),
            options: sort_options,
        });
    }

    let timestamp_column_index = time_series_table_metadata.timestamp_column_index;
    let timestamp_column = uncompressed_time_series.column(timestamp_column_index);
    sort_columns.push(SortColumn {
        values: (*timestamp_column).clone(),
        options: sort_options,
    });

    let indices = compute::lexsort_to_indices(&sort_columns, None)?;
    let sorted_columns = compute::take_arrays(uncompressed_time_series.columns(), &indices, None)?;
    RecordBatch::try_new(uncompressed_time_series.schema(), sorted_columns)
        .map_err(|error| error.into())
}

/// Compress the field columns in `uncompressed_time_series` from the table with
/// `time_series_table_metadata` using [`try_compress_univariate_time_series()`] and append the
/// result to `compressed_data`. It is assumed that all data points in `uncompressed_time_series`
/// have the same tags as in `tag_values`.
pub fn try_split_and_compress_univariate_time_series(
    time_series_table_metadata: &TimeSeriesTableMetadata,
    uncompressed_time_series: &RecordBatch,
    tag_values: &[String],
    compressed_time_series: &mut Vec<RecordBatch>,
) -> Result<()> {
    let uncompressed_timestamps = modelardb_types::array!(
        uncompressed_time_series,
        time_series_table_metadata.timestamp_column_index,
        TimestampArray
    );

    for field_column_index in &time_series_table_metadata.field_column_indices {
        let uncompressed_values =
            modelardb_types::array!(uncompressed_time_series, *field_column_index, ValueArray);

        let error_bound = time_series_table_metadata.error_bounds[*field_column_index];

        let compressed_segments = try_compress_univariate_time_series(
            uncompressed_timestamps,
            uncompressed_values,
            error_bound,
            time_series_table_metadata.compressed_schema.clone(),
            tag_values.to_vec(),
            *field_column_index as i16,
        )
        .expect("uncompressed_timestamps and uncompressed_values should have the same length.");

        compressed_time_series.push(compressed_segments);
    }

    Ok(())
}

/// Compress `uncompressed_timestamps` using a start time, end time, and a sampling interval if
/// regular and delta-of-deltas followed by a variable length binary encoding if irregular.
/// `uncompressed_values` is compressed within `error_bound` using the model types in `models`.
/// Assumes `uncompressed_timestamps` and `uncompressed_values` are sorted according to
/// `uncompressed_timestamps`. The resulting compressed segments have the schema in `compressed_schema`
/// with the tag columns populated by the values in `tag_values` and the field column index populated
/// by `field_column_index`. Returns [`ModelarDbCompressionError`] if `uncompressed_timestamps` and
/// `uncompressed_values` have different lengths or if `compressed_schema` is not a valid schema for
/// compressed segments, otherwise the resulting compressed segments are returned as a
/// [`RecordBatch`] with the `compressed_schema` schema.
pub fn try_compress_univariate_time_series(
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
    error_bound: ErrorBound,
    compressed_schema: Arc<Schema>,
    tag_values: Vec<String>,
    field_column_index: i16,
) -> Result<RecordBatch> {
    // The uncompressed data must be passed as arrays instead of a RecordBatch as a TimestampArray
    // and a ValueArray is the only supported input. However, as a result it is necessary to verify
    // they have the same length.
    if uncompressed_timestamps.len() != uncompressed_values.len() {
        return Err(ModelarDbCompressionError::InvalidArgument(
            "Uncompressed timestamps and uncompressed values have different lengths.".to_owned(),
        ));
    }

    // If there is no uncompressed data to compress, an empty [`RecordBatch`] can be returned.
    if uncompressed_timestamps.is_empty() {
        return Ok(RecordBatch::new_empty(compressed_schema));
    }

    // Enough memory for end_index compressed segments are allocated to never require reallocation
    // as one compressed segment is created per data point in the absolute worst case.
    let end_index = uncompressed_timestamps.len();
    let mut compressed_segment_batch_builder = CompressedSegmentBatchBuilder::new(
        compressed_schema,
        tag_values,
        field_column_index,
        end_index,
    );

    // Compress the uncompressed timestamps and uncompressed values.
    let mut current_start_index = 0;
    let mut previous_model: Option<CompressedSegmentBuilder> = None;
    while current_start_index < end_index {
        // Select a model to represent the values from current_start_index to index within
        // error_bound where index < end_index if any of them can represent any values.
        let maybe_model = fit_next_model(
            current_start_index,
            error_bound,
            uncompressed_timestamps,
            uncompressed_values,
        );

        // A model will be stored as part of a compressed segment if it could represent some values
        // and it uses less storage space per value than the uncompressed values it represents.
        if let Some(model) = maybe_model
            && model.bytes_per_value <= models::VALUE_SIZE_IN_BYTES as f32
        {
            // Flush the previous model and any residual value if either exists.
            if current_start_index > 0 {
                store_compressed_segments_with_model_and_or_residuals(
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
            // The structural models could not efficiently represent the sub-sequence starting at
            // current_start_index, so residual values will instead be compressed using MacaqueV.
            current_start_index += 1;
        }
    }

    store_compressed_segments_with_model_and_or_residuals(
        error_bound,
        previous_model,
        end_index - 1,
        uncompressed_timestamps,
        uncompressed_values,
        &mut compressed_segment_batch_builder,
    );

    Ok(compressed_segment_batch_builder.finish())
}

/// Create a model that represents the values in `uncompressed_values` from `start_index` to index
/// within `error_bound` where index <= `end_index`, or [`None`] if no model can represent the first
/// value. This method is defined as `pub(crate)` so it can be used in the tests in types.rs.
pub(crate) fn fit_next_model(
    current_start_index: usize,
    error_bound: ErrorBound,
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
) -> Option<CompressedSegmentBuilder> {
    let mut model_builder = ModelBuilder::new(current_start_index, error_bound);

    let mut current_index = current_start_index;
    let end_index = uncompressed_timestamps.len();

    // A do-while loop is emulated using can_fit_more to not duplicate let timestamp and let value.
    let mut can_fit_more = true;
    while can_fit_more && current_index < end_index {
        let timestamp = uncompressed_timestamps.value(current_index);
        let value = uncompressed_values.value(current_index);
        can_fit_more = model_builder.try_to_update_models(timestamp, value);
        current_index += 1;
    }

    // uncompressed_timestamps and uncompressed_values can validate it is within the error bounds.
    model_builder.finish(uncompressed_timestamps, uncompressed_values)
}

/// Create segment(s) that store `maybe_model` and residual values as either:
/// - One compressed segment that stores `maybe_model` and residuals if the number of
///   residuals are less than or equal to [`RESIDUAL_VALUES_MAX_LENGTH`].
/// - Two compressed segments with the first storing `maybe_model` and the second storing
///   residuals if the number of residuals are greater than [`RESIDUAL_VALUES_MAX_LENGTH`].
/// - One compressed segment that stores residuals as a single model if `maybe_model` is
///   [`None`].
fn store_compressed_segments_with_model_and_or_residuals(
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
                error_bound,
                residuals_end_index,
                uncompressed_timestamps,
                uncompressed_values,
                compressed_segment_batch_builder,
            );
        } else {
            // Many residuals exist, so the model and residuals are put into two segments.
            let model_end_index = model.end_index;

            model.finish(
                error_bound,
                model_end_index, // No residuals are stored.
                uncompressed_timestamps,
                uncompressed_values,
                compressed_segment_batch_builder,
            );

            compress_and_store_residuals_in_a_separate_segment(
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
            error_bound,
            0,
            residuals_end_index,
            uncompressed_timestamps,
            uncompressed_values,
            compressed_segment_batch_builder,
        );
    }
}

/// Compress the values from `start_index` to and including `end_index` in `uncompressed_values`
/// using [`Gorilla`] and store the resulting model with the corresponding timestamps from
/// `uncompressed_timestamps` as a segment in `compressed_segment_batch_builder`.
fn compress_and_store_residuals_in_a_separate_segment(
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
    let mut macaque_v = MacaqueV::new(error_bound);
    macaque_v.compress_values(uncompressed_values);

    let (values, min_value, max_value) = macaque_v.model();

    compressed_segment_batch_builder.append_compressed_segment(
        MACAQUE_V_ID,
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
    use std::ops::Range;

    use super::*;

    use arrow::array::{ArrayBuilder, BinaryViewArray, Float32Array, Int8Array};
    use modelardb_test::ERROR_BOUND_FIVE;
    use modelardb_test::data_generation::{self, ValuesStructure};
    use modelardb_test::table;
    use modelardb_types::types::{Timestamp, TimestampBuilder, ValueBuilder};
    use proptest::num::f32 as ProptestValue;
    use proptest::{collection, property_test};

    use crate::{MODEL_TYPE_NAMES, models};

    const TAG_VALUE: &str = "tag";
    const ADD_NOISE_RANGE: Option<Range<f32>> = Some(1.0..1.05);
    const TRY_COMPRESS_TEST_LENGTH: usize = 50;

    /// Return a strategy that generates quiet and signaling NaNs with random signs and payloads.
    /// `QUIET_NAN` and `SIGNALING_NAN` select the classes of floats to generate. Thus, only NaNs
    /// are generated and both classes generate random payloads. Signaling NaNs are included as
    /// calculations with a signaling NaN makes it quiet. `POSITIVE` and `NEGATIVE` control the sign
    /// bit of the NaNs and without them, only NaNs with a positive sign bit are generated.
    fn nan_strategy() -> ProptestValue::Any {
        ProptestValue::QUIET_NAN
            | ProptestValue::SIGNALING_NAN
            | ProptestValue::POSITIVE
            | ProptestValue::NEGATIVE
    }

    /// Start times used for generating timestamps for the time series in the tests. Multiple
    /// different start times are used to ensures that models are always created correctly. For
    /// example, the second is the microseconds since the Unix epoch and was added due to #441.
    const START_TIMES: [Timestamp; 2] = [0, 1767233520004960];

    // Tests for try_compress_univariate_time_series().
    #[test]
    fn test_try_compress_empty_time_series_within_lossless_error_bound() {
        let compressed_record_batch = try_compress_univariate_time_series(
            &TimestampBuilder::new().finish(),
            &ValueBuilder::new().finish(),
            ErrorBound::Lossless,
            compressed_schema(),
            vec![TAG_VALUE.to_owned()],
            0,
        )
        .unwrap();
        assert_eq!(0, compressed_record_batch.num_rows());
    }

    #[test]
    fn test_try_compress_regular_constant_time_series_within_lossless_error_bound() {
        generate_compress_and_assert_known_segment(
            false,
            ValuesStructure::Constant(None),
            ErrorBound::Lossless,
            &[models::PMC_MEAN_ID],
        );
    }

    #[test]
    fn test_try_compress_irregular_constant_time_series_within_lossless_error_bound() {
        generate_compress_and_assert_known_segment(
            true,
            ValuesStructure::Constant(None),
            ErrorBound::Lossless,
            &[models::PMC_MEAN_ID],
        );
    }

    #[test]
    fn test_try_compress_time_series_with_mixed_zeros_within_lossless_error_bound() {
        compress_and_assert_values_within_lossless_error_bound(vec![
            0.0, 0.0, -0.0, 0.0, 0.0, -0.0, -0.0, 0.0,
        ]);
    }

    #[test]
    fn test_try_compress_time_series_with_negative_zeros_within_lossless_error_bound() {
        compress_and_assert_values_within_lossless_error_bound(vec![-0.0; 8]);
    }

    #[test]
    fn test_try_compress_time_series_with_negative_zero_before_zeros_within_lossless_error_bound() {
        let mut values = vec![-0.0];
        values.extend([0.0; 49]);
        compress_and_assert_values_within_lossless_error_bound(values);
    }

    #[test]
    fn test_try_compress_linear_time_series_through_negative_zero_within_lossless_error_bound() {
        compress_and_assert_values_within_lossless_error_bound(vec![
            1.0, 0.5, -0.0, -0.5, -1.0, -1.5, -2.0, -2.5,
        ]);
    }

    #[property_test]
    fn test_try_compress_time_series_with_nans_with_different_payloads_within_lossless_error_bound(
        #[strategy = collection::vec(nan_strategy(), 1..5)] nans: Vec<f32>,
        #[strategy = collection::vec(0..5_usize, 1..100)] indices: Vec<usize>,
    ) {
        // A few NaNs are selected repeatedly instead of generating each NaN in the sequence, as
        // randomly generated NaNs practically never have the same bits. Thus, the sequence would
        // not contain runs of the same NaN and PMC-Mean and Swing would always reject the second.
        let values = indices
            .iter()
            .map(|index| nans[index % nans.len()])
            .collect();
        compress_and_assert_values_within_lossless_error_bound(values);
    }

    fn compress_and_assert_values_within_lossless_error_bound(values: Vec<f32>) {
        let uncompressed_timestamps = data_generation::generate_timestamps(values.len(), false);
        let uncompressed_values = ValueArray::from(values);

        compress_and_assert_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            ErrorBound::Lossless,
            None,
        );
    }

    #[test]
    fn test_try_compress_time_series_with_duplicate_timestamps_within_lossless_error_bound() {
        compress_and_assert_values_with_timestamps(
            vec![0, 0, 100, 100, 200, 200, 300, 300],
            vec![1.0, 10.0, 2.0, 20.0, 3.0, 30.0, 4.0, 40.0],
            ErrorBound::Lossless,
        );
    }

    #[test]
    fn test_try_compress_time_series_with_two_equal_timestamps_within_lossless_error_bound() {
        compress_and_assert_values_with_timestamps(
            vec![0, 0],
            vec![1.0, 2.0],
            ErrorBound::Lossless,
        );
    }

    #[test]
    fn test_try_compress_time_series_with_many_equal_timestamps_within_lossless_error_bound() {
        compress_and_assert_values_with_timestamps(
            vec![0; 20],
            (0..20).map(|value| value as f32).collect(),
            ErrorBound::Lossless,
        );
    }

    #[test]
    fn test_try_compress_time_series_with_many_equal_timestamps_within_relative_error_bound_five() {
        compress_and_assert_values_with_timestamps(
            vec![0; 20],
            (0..20).map(|value| 100.0 + 4.0 * value as f32).collect(),
            ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
        );
    }

    fn compress_and_assert_values_with_timestamps(
        timestamps: Vec<Timestamp>,
        values: Vec<f32>,
        error_bound: ErrorBound,
    ) {
        compress_and_assert_time_series(
            &TimestampArray::from(timestamps),
            &ValueArray::from(values),
            error_bound,
            None,
        );
    }

    #[test]
    fn test_try_compress_regular_almost_constant_time_series_within_absolute_error_bound_five() {
        generate_compress_and_assert_known_segment(
            false,
            ValuesStructure::Random(9.8..10.2),
            ErrorBound::try_new_absolute(ERROR_BOUND_FIVE).unwrap(),
            &[models::PMC_MEAN_ID],
        );
    }

    #[test]
    fn test_try_compress_regular_almost_constant_time_series_within_relative_error_bound_five() {
        generate_compress_and_assert_known_segment(
            false,
            ValuesStructure::Random(9.8..10.2),
            ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
            &[models::PMC_MEAN_ID],
        );
    }

    #[test]
    fn test_try_compress_irregular_almost_constant_time_series_within_absolute_error_bound_five() {
        generate_compress_and_assert_known_segment(
            true,
            ValuesStructure::Random(9.8..10.2),
            ErrorBound::try_new_absolute(ERROR_BOUND_FIVE).unwrap(),
            &[models::PMC_MEAN_ID],
        );
    }

    #[test]
    fn test_try_compress_irregular_almost_constant_time_series_within_relative_error_bound_five() {
        generate_compress_and_assert_known_segment(
            true,
            ValuesStructure::Random(9.8..10.2),
            ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
            &[models::PMC_MEAN_ID],
        );
    }

    #[test]
    fn test_try_compress_regular_linear_time_series_within_lossless_error_bound() {
        generate_compress_and_assert_known_segment(
            false,
            ValuesStructure::Linear(None),
            ErrorBound::Lossless,
            &[models::SWING_ID],
        );
    }

    #[test]
    fn test_try_compress_irregular_linear_time_series_within_lossless_error_bound() {
        generate_compress_and_assert_known_segment(
            true,
            ValuesStructure::Linear(None),
            ErrorBound::Lossless,
            &[models::SWING_ID],
        );
    }

    #[test]
    fn test_try_compress_regular_almost_linear_time_series_within_absolute_error_bound_five() {
        generate_compress_and_assert_known_segment(
            false,
            ValuesStructure::Linear(ADD_NOISE_RANGE),
            ErrorBound::try_new_absolute(ERROR_BOUND_FIVE).unwrap(),
            &[models::SWING_ID],
        );
    }

    #[test]
    fn test_try_compress_regular_almost_linear_time_series_within_relative_error_bound_five() {
        generate_compress_and_assert_known_segment(
            false,
            ValuesStructure::Linear(ADD_NOISE_RANGE),
            ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
            &[models::SWING_ID],
        );
    }

    #[test]
    fn test_try_compress_irregular_almost_linear_time_series_within_absolute_error_bound_five() {
        generate_compress_and_assert_known_segment(
            true,
            ValuesStructure::Linear(ADD_NOISE_RANGE),
            ErrorBound::try_new_absolute(ERROR_BOUND_FIVE).unwrap(),
            &[models::SWING_ID],
        );
    }

    #[test]
    fn test_try_compress_irregular_almost_linear_time_series_within_relative_error_bound_five() {
        generate_compress_and_assert_known_segment(
            true,
            ValuesStructure::Linear(ADD_NOISE_RANGE),
            ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
            &[models::SWING_ID],
        );
    }

    #[test]
    fn test_try_compress_regular_random_time_series_within_lossless_error_bound() {
        generate_compress_and_assert_known_segment(
            false,
            ValuesStructure::largest_random_without_overflow(),
            ErrorBound::Lossless,
            &[models::MACAQUE_V_ID],
        );
    }

    #[test]
    fn test_try_compress_irregular_random_time_series_within_lossless_error_bound() {
        generate_compress_and_assert_known_segment(
            true,
            ValuesStructure::largest_random_without_overflow(),
            ErrorBound::Lossless,
            &[models::MACAQUE_V_ID],
        );
    }

    fn generate_compress_and_assert_known_segment(
        irregular: bool,
        values_structure: ValuesStructure,
        error_bound: ErrorBound,
        expected_model_type_ids: &[i8],
    ) {
        let uncompressed_timestamps = data_generation::generate_timestamps(10, irregular);
        let uncompressed_values =
            data_generation::generate_values(uncompressed_timestamps.values(), values_structure);

        compress_and_assert_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            error_bound,
            Some(expected_model_type_ids),
        );
    }

    #[test]
    fn test_try_compress_regular_random_linear_constant_time_series_within_lossless_error_bound() {
        generate_compress_and_assert_known_time_series(
            ErrorBound::Lossless,
            false,
            &[models::MACAQUE_V_ID, models::SWING_ID, models::PMC_MEAN_ID],
            &[models::MACAQUE_V_ID, models::SWING_ID, models::PMC_MEAN_ID],
        );
    }

    #[test]
    fn test_try_compress_irregular_random_linear_constant_time_series_within_lossless_error_bound()
    {
        generate_compress_and_assert_known_time_series(
            ErrorBound::Lossless,
            true,
            &[models::MACAQUE_V_ID, models::SWING_ID, models::PMC_MEAN_ID],
            &[models::MACAQUE_V_ID, models::SWING_ID, models::PMC_MEAN_ID],
        );
    }

    #[test]
    fn test_try_compress_regular_constant_linear_random_time_series_within_lossless_error_bound() {
        generate_compress_and_assert_known_time_series(
            ErrorBound::Lossless,
            false,
            &[models::PMC_MEAN_ID, models::SWING_ID, models::MACAQUE_V_ID],
            &[models::PMC_MEAN_ID, models::SWING_ID],
        );
    }

    #[test]
    fn test_try_compress_irregular_constant_linear_random_time_series_within_lossless_error_bound()
    {
        generate_compress_and_assert_known_time_series(
            ErrorBound::Lossless,
            true,
            &[models::PMC_MEAN_ID, models::SWING_ID, models::MACAQUE_V_ID],
            &[models::PMC_MEAN_ID, models::SWING_ID],
        );
    }

    fn generate_compress_and_assert_known_time_series(
        error_bound: ErrorBound,
        generate_irregular_timestamps: bool,
        generate_model_type_ids: &[i8],
        expected_model_type_ids: &[i8],
    ) {
        let uncompressed_timestamps = data_generation::generate_timestamps(
            3 * TRY_COMPRESS_TEST_LENGTH,
            generate_irregular_timestamps,
        );

        let mut uncompressed_timestamps_start_index = 0;
        let mut uncompressed_values = ValueBuilder::with_capacity(3 * TRY_COMPRESS_TEST_LENGTH);
        for model_type_id in generate_model_type_ids {
            let uncompressed_timestamps_end_index =
                uncompressed_timestamps_start_index + TRY_COMPRESS_TEST_LENGTH;

            let values_structure = match *model_type_id {
                models::PMC_MEAN_ID => ValuesStructure::Constant(None),
                models::SWING_ID => ValuesStructure::Linear(None),
                models::MACAQUE_V_ID => ValuesStructure::largest_random_without_overflow(),
                _ => panic!("Unknown model type."),
            };

            let values = data_generation::generate_values(
                &uncompressed_timestamps.values()
                    [uncompressed_timestamps_start_index..uncompressed_timestamps_end_index],
                values_structure,
            );

            uncompressed_values.append_slice(values.values());
            uncompressed_timestamps_start_index = uncompressed_timestamps_end_index;
        }

        let uncompressed_values = uncompressed_values.finish();
        assert_eq!(uncompressed_timestamps.len(), uncompressed_values.len());

        compress_and_assert_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            error_bound,
            Some(expected_model_type_ids),
        );
    }

    #[test]
    fn test_try_compress_regular_synthetic_time_series_without_noise_within_lossless_error_bound() {
        generate_compress_and_assert_time_series(ErrorBound::Lossless, false, None)
    }

    #[test]
    fn test_try_compress_regular_synthetic_time_series_without_noise_within_absolute_error_bound_five()
     {
        generate_compress_and_assert_time_series(
            ErrorBound::try_new_absolute(ERROR_BOUND_FIVE).unwrap(),
            false,
            None,
        )
    }

    #[test]
    fn test_try_compress_regular_synthetic_time_series_without_noise_within_relative_error_bound_five()
     {
        generate_compress_and_assert_time_series(
            ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
            false,
            None,
        )
    }

    #[test]
    fn test_try_compress_regular_synthetic_time_series_with_noise_within_lossless_error_bound() {
        generate_compress_and_assert_time_series(ErrorBound::Lossless, false, ADD_NOISE_RANGE)
    }

    #[test]
    fn test_try_compress_regular_synthetic_time_series_with_noise_within_absolute_error_bound_five()
    {
        generate_compress_and_assert_time_series(
            ErrorBound::try_new_absolute(ERROR_BOUND_FIVE).unwrap(),
            false,
            ADD_NOISE_RANGE,
        )
    }

    #[test]
    fn test_try_compress_regular_synthetic_time_series_with_noise_within_relative_error_bound_five()
    {
        generate_compress_and_assert_time_series(
            ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
            false,
            ADD_NOISE_RANGE,
        )
    }

    #[test]
    fn test_try_compress_irregular_synthetic_time_series_without_noise_within_lossless_error_bound()
    {
        generate_compress_and_assert_time_series(ErrorBound::Lossless, true, None)
    }

    #[test]
    fn test_try_compress_irregular_synthetic_time_series_without_noise_within_absolute_error_bound_five()
     {
        generate_compress_and_assert_time_series(
            ErrorBound::try_new_absolute(ERROR_BOUND_FIVE).unwrap(),
            true,
            None,
        )
    }

    #[test]
    fn test_try_compress_irregular_synthetic_time_series_without_noise_within_relative_error_bound_five()
     {
        generate_compress_and_assert_time_series(
            ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
            true,
            None,
        )
    }

    #[test]
    fn test_try_compress_irregular_synthetic_time_series_with_noise_within_lossless_error_bound() {
        generate_compress_and_assert_time_series(ErrorBound::Lossless, true, ADD_NOISE_RANGE)
    }

    #[test]
    fn test_try_compress_irregular_synthetic_time_series_with_noise_within_absolute_error_bound_five()
     {
        generate_compress_and_assert_time_series(
            ErrorBound::try_new_absolute(ERROR_BOUND_FIVE).unwrap(),
            true,
            ADD_NOISE_RANGE,
        )
    }

    #[test]
    fn test_try_compress_irregular_synthetic_time_series_with_noise_within_relative_error_bound_five()
     {
        generate_compress_and_assert_time_series(
            ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
            true,
            ADD_NOISE_RANGE,
        )
    }

    fn generate_compress_and_assert_time_series(
        error_bound: ErrorBound,
        generate_irregular_timestamps: bool,
        add_noise_range: Option<Range<f32>>,
    ) {
        let (uncompressed_timestamps, uncompressed_values) =
            data_generation::generate_univariate_time_series(
                1000 * TRY_COMPRESS_TEST_LENGTH,
                TRY_COMPRESS_TEST_LENGTH..10 * TRY_COMPRESS_TEST_LENGTH + 1,
                generate_irregular_timestamps,
                add_noise_range,
                100.0..200.0,
            );

        compress_and_assert_time_series(
            &uncompressed_timestamps,
            &uncompressed_values,
            error_bound,
            None,
        );
    }

    /// Compress `uncompressed_timestamps` shifted by each of the [`START_TIMES`] and
    /// `uncompressed_values` within `error_bound` and assert that the values can be reconstructed
    /// within `error_bound`. If `maybe_expected_model_type_ids` is [`Some`], also assert that the
    /// compressed segments were created by these model types.
    fn compress_and_assert_time_series(
        uncompressed_timestamps: &TimestampArray,
        uncompressed_values: &ValueArray,
        error_bound: ErrorBound,
        maybe_expected_model_type_ids: Option<&[i8]>,
    ) {
        for start_time in START_TIMES {
            let uncompressed_timestamps =
                uncompressed_timestamps.unary(|timestamp| timestamp + start_time);

            let compressed_record_batch = try_compress_univariate_time_series(
                &uncompressed_timestamps,
                uncompressed_values,
                error_bound,
                compressed_schema(),
                vec![TAG_VALUE.to_owned()],
                0,
            )
            .unwrap();

            assert_compressed_record_batch_with_unknown_segments_from_time_series(
                error_bound,
                &uncompressed_timestamps,
                uncompressed_values,
                &compressed_record_batch,
            );

            if let Some(expected_model_type_ids) = maybe_expected_model_type_ids {
                let model_type_ids = modelardb_types::array!(compressed_record_batch, 0, Int8Array);
                assert_eq!(model_type_ids.values(), expected_model_type_ids);
            }
        }
    }

    fn assert_compressed_record_batch_with_unknown_segments_from_time_series(
        error_bound: ErrorBound,
        uncompressed_timestamps: &TimestampArray,
        uncompressed_values: &ValueArray,
        compressed_record_batch: &RecordBatch,
    ) {
        let mut timestamp_builder = TimestampBuilder::new();
        let mut value_builder = ValueBuilder::new();

        modelardb_types::arrays!(
            compressed_record_batch,
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

        let mut index_to_model_type = vec![];
        for row_index in 0..compressed_record_batch.num_rows() {
            let start_index = value_builder.len();

            models::grid(
                model_type_ids.value(row_index),
                start_times.value(row_index),
                end_times.value(row_index),
                timestamps.value(row_index),
                min_values.value(row_index),
                max_values.value(row_index),
                values.value(row_index),
                residuals.value(row_index),
                &mut timestamp_builder,
                &mut value_builder,
            );

            let end_index = value_builder.len();
            index_to_model_type.push((start_index..end_index, model_type_ids.value(row_index)));
        }

        let decompressed_timestamps = timestamp_builder.finish();
        let decompressed_values = value_builder.finish();

        assert_eq!(decompressed_timestamps.len(), decompressed_values.len());
        assert_eq!(uncompressed_timestamps, &decompressed_timestamps);
        assert_eq!(uncompressed_values.len(), decompressed_values.len());
        for index in 0..uncompressed_values.len() {
            let real_value = uncompressed_values.value(index);
            let approximate_value = decompressed_values.value(index);
            let model_type_id = index_to_model_type
                .iter()
                .find(|range_key| range_key.0.contains(&index))
                .unwrap()
                .1;
            let model_type_name = MODEL_TYPE_NAMES[model_type_id as usize];

            // A lossless error bound requires the exact bits to be reconstructed at all indices.
            if error_bound == ErrorBound::Lossless {
                assert_eq!(
                    real_value.to_bits(),
                    approximate_value.to_bits(),
                    "{approximate_value} from {model_type_name} at index {index} is not bitwise equal to {real_value}.",
                );
            } else {
                assert!(
                    models::is_value_within_error_bound(error_bound, real_value, approximate_value),
                    "{approximate_value} from {model_type_name} at index {index} is outside {error_bound:?} of {real_value}.",
                );
            }
        }
    }

    // Tests for compress_and_store_residuals_in_a_separate_segment().
    #[test]
    fn test_compress_and_store_residuals_in_a_separate_segment() {
        let uncompressed_timestamps = TimestampArray::from_iter_values((100..=500).step_by(100));
        let uncompressed_values = ValueArray::from(vec![73.0, 37.0, 37.0, 37.0, 73.0]);

        let mut compressed_segment_batch_builder = CompressedSegmentBatchBuilder::new(
            compressed_schema(),
            vec![TAG_VALUE.to_owned()],
            0,
            1,
        );

        compress_and_store_residuals_in_a_separate_segment(
            ErrorBound::Lossless,
            0,
            uncompressed_timestamps.len() - 1,
            &uncompressed_timestamps,
            &uncompressed_values,
            &mut compressed_segment_batch_builder,
        );

        let compressed_record_batch = compressed_segment_batch_builder.finish();
        modelardb_types::arrays!(
            compressed_record_batch,
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

        assert_eq!(1, compressed_record_batch.num_rows());
        assert_eq!(MACAQUE_V_ID, model_type_ids.value(0));
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

    fn compressed_schema() -> Arc<Schema> {
        table::time_series_table_metadata().compressed_schema
    }
}
