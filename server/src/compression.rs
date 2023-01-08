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

//! Compress `UncompressedDataBuffers` provided by [`StorageEngine`](crate::storage::StorageEngine)
//! using the model types in [`models`](crate::models) to produce compressed segments which are
//! returned to [`StorageEngine`](crate::storage::StorageEngine).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    BinaryArray, BinaryBuilder, Float32Array, Float32Builder, UInt64Array, UInt64Builder,
    UInt8Array, UInt8Builder,
};
use datafusion::arrow::record_batch::RecordBatch;

use crate::errors::ModelarDbError;
use crate::models::{
    gorilla::Gorilla, pmc_mean::PMCMean, swing::Swing, timestamps, ErrorBound, SelectedModel,
};
use crate::types::{
    CompressedSchema, Timestamp, TimestampArray, TimestampBuilder, Value, ValueArray, ValueBuilder,
};

// TODO: use Gorilla as a fallback to remove GORILLA_MAXIMUM_LENGTH.
/// Maximum number of data points that models of type Gorilla can represent per
/// compressed segment. As models of type Gorilla use lossless compression they
/// will never exceed the user-defined error bounds.
pub const GORILLA_MAXIMUM_LENGTH: usize = 50;

/// Compress `uncompressed_timestamps` using a start time, end time, and a
/// sampling interval if regular and delta-of-deltas followed by a variable
/// length binary encoding if irregular. `uncompressed_values` is compressed
/// within `error_bound` using the model types in [`models`](crate::models).
/// Assumes `uncompressed_timestamps` and `uncompressed_values` are sorted
/// according to `uncompressed_timestamps`. Returns
/// [`CompressionError`](ModelarDbError::CompressionError) if
/// `uncompressed_timestamps` and `uncompressed_values` have different lengths,
/// otherwise the resulting compressed segments are returned as a
/// [`RecordBatch`] with the schema provided as `compressed_schema`.
pub fn try_compress(
    univariate_id: u64,
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
    error_bound: ErrorBound,
    compressed_schema: &CompressedSchema,
) -> Result<RecordBatch, ModelarDbError> {
    // The uncompressed data must be passed as arrays instead of a RecordBatch
    // as a TimestampArray and a ValueArray is the only supported input.
    // However, as a result it is necessary to verify they have the same length.
    if uncompressed_timestamps.len() != uncompressed_values.len() {
        return Err(ModelarDbError::CompressionError(
            "Uncompressed timestamps and uncompressed values have different lengths.".to_owned(),
        ));
    }

    // Enough memory for end_index elements are allocated to never require
    // reallocation as one model is created per data point in the worst case.
    let end_index = uncompressed_timestamps.len();
    let mut compressed_record_batch_builder = CompressedSegmentBatchBuilder::new(end_index);

    // Compress the uncompressed timestamps and uncompressed values.
    let mut current_index = 0;
    while current_index < end_index {
        // Create a compressed segment that represents the data points from
        // current_index to index within error_bound where index <= end_index.
        let compressed_segment_builder = CompressedSegmentBuilder::new(
            univariate_id,
            current_index,
            end_index,
            uncompressed_timestamps,
            uncompressed_values,
            error_bound,
        );
        current_index = compressed_segment_builder.finish(&mut compressed_record_batch_builder);
    }
    Ok(compressed_record_batch_builder.finish(compressed_schema))
}

/// Merges the segments in `compressed_segments` that contain equivalent models.
/// Assumes that the segments in `compressed_segments` are all from the same
/// time series and that the segments are all sorted according to time.
pub fn merge_segments(compressed_segments: RecordBatch) -> RecordBatch {
    // TODO: merge segments with none equivalent models.

    // Extract the columns from the RecordBatch.
    let univariate_ids = crate::array!(compressed_segments, 0, UInt64Array);
    let model_type_ids = crate::array!(compressed_segments, 1, UInt8Array);
    let start_times = crate::array!(compressed_segments, 2, TimestampArray);
    let end_times = crate::array!(compressed_segments, 3, TimestampArray);
    let timestamps = crate::array!(compressed_segments, 4, BinaryArray);
    let min_values = crate::array!(compressed_segments, 5, ValueArray);
    let max_values = crate::array!(compressed_segments, 6, ValueArray);
    let values = crate::array!(compressed_segments, 7, BinaryArray);
    let errors = crate::array!(compressed_segments, 8, Float32Array);

    // For each segment, check if it can be merged with another segment.
    let num_rows = compressed_segments.num_rows();
    let mut compressed_segments_to_merge = HashMap::with_capacity(num_rows);

    for index in 0..num_rows {
        // f32 are converted to u32 with the same bitwise representation as f32
        // and f64 does not implement std::hash::Hash and thus cannot be hashed.
        let model = (
            model_type_ids.value(index),
            values.value(index),
            min_values.value(index).to_bits(),
            max_values.value(index).to_bits(),
        );

        // Lookup the entry in the HashMap for model, create an empty Vec if an
        // entry for model did not exist, and append index to the entry's Vec.
        compressed_segments_to_merge
            .entry(model)
            .or_insert_with(Vec::new)
            .push(index);
    }

    // If none of the segments can be merged return the original compressed
    // segments, otherwise return the smaller set of merged compressed segments.
    if compressed_segments_to_merge.len() < num_rows {
        let mut merged_compressed_segments = CompressedSegmentBatchBuilder::new(num_rows);
        for (_, indices) in compressed_segments_to_merge {
            // Merge timestamps.
            let mut timestamp_builder = TimestampBuilder::new();
            for index in &indices {
                let start_time = start_times.value(*index);
                let end_time = end_times.value(*index);
                let timestamps = timestamps.value(*index);
                timestamps::decompress_all_timestamps(
                    start_time,
                    end_time,
                    timestamps,
                    &mut timestamp_builder,
                );
            }
            let timestamps = timestamp_builder.finish();
            let compressed_timestamps =
                timestamps::compress_residual_timestamps(timestamps.values());

            // Merge segments. The first segment's model is used for the merged
            // segment as all of the segments contain the exact same model.
            let index = indices[0];
            merged_compressed_segments.append_compressed_segment(
                univariate_ids.value(index),
                model_type_ids.value(index),
                timestamps.value(0),
                timestamps.value(timestamps.len() - 1),
                &compressed_timestamps,
                min_values.value(index),
                max_values.value(index),
                values.value(index),
                errors.value(index),
            );
        }
        merged_compressed_segments.finish(&CompressedSchema(compressed_segments.schema()))
    } else {
        compressed_segments
    }
}

/// A compressed segment being built from an uncompressed segment using the
/// model types in [`models`](crate::models). Each of the model types is used to
/// fit models to the data points, and then the model that uses the fewest
/// number of bytes per value is selected.
struct CompressedSegmentBuilder<'a> {
    /// The id of the time series from which the compressed segment is created.
    univariate_id: u64,
    /// The regular timestamps of the uncompressed segment the compressed
    /// segment is being built from.
    uncompressed_timestamps: &'a TimestampArray,
    /// The values of the uncompressed segment the compressed segment is being
    /// built from.
    uncompressed_values: &'a ValueArray,
    /// Index of the first data point in `uncompressed_timestamps` and
    /// `uncompressed_values` the compressed segment represents.
    start_index: usize,
    /// Constant function that currently represents the values in
    /// `uncompressed_values` from `start_index` to `start_index` +
    /// `pmc_mean.length`.
    pmc_mean: PMCMean,
    /// Indicates if `pmc_mean` could represent all values in
    /// `uncompressed_values` from `start_index` to `current_index` in `new()`.
    pmc_mean_could_fit_all: bool,
    /// Linear function that represents the values in `uncompressed_values` from
    /// `start_index` to `start_index` + `swing.length`.
    swing: Swing,
    /// Indicates if `swing` could represent all values in `uncompressed_values`
    /// from `start_index` to `current_index` in `new()`.
    swing_could_fit_all: bool,
    /// Values in `uncompressed_values` from `start_index` to `start_index` +
    /// `gorilla.length` compressed using lossless compression.
    gorilla: Gorilla,
}

impl<'a> CompressedSegmentBuilder<'a> {
    /// Create a compressed segment that represents the regular timestamps in
    /// `uncompressed_timestamps` and the values in `uncompressed_values` from
    /// `start_index` to index within `error_bound` where index <= `end_index`.
    fn new(
        univariate_id: u64,
        start_index: usize,
        end_index: usize,
        uncompressed_timestamps: &'a TimestampArray,
        uncompressed_values: &'a ValueArray,
        error_bound: ErrorBound,
    ) -> Self {
        let mut compressed_segment_builder = Self {
            univariate_id,
            uncompressed_timestamps,
            uncompressed_values,
            start_index,
            pmc_mean: PMCMean::new(error_bound),
            pmc_mean_could_fit_all: true,
            swing: Swing::new(error_bound),
            swing_could_fit_all: true,
            gorilla: Gorilla::new(),
        };

        let mut current_index = start_index;
        while compressed_segment_builder.can_fit_more() && current_index < end_index {
            let timestamp = uncompressed_timestamps.value(current_index);
            let value = uncompressed_values.value(current_index);
            compressed_segment_builder.try_to_update_models(timestamp, value);
            current_index += 1;
        }
        compressed_segment_builder
    }

    /// Attempt to update the current models to also represent the `value` of
    /// the data point collected at `timestamp`.
    fn try_to_update_models(&mut self, timestamp: Timestamp, value: Value) {
        debug_assert!(
            self.can_fit_more(),
            "The current models cannot be fitted to additional data points."
        );

        self.pmc_mean_could_fit_all = self.pmc_mean_could_fit_all && self.pmc_mean.fit_value(value);

        self.swing_could_fit_all =
            self.swing_could_fit_all && self.swing.fit_data_point(timestamp, value);

        // Gorilla uses lossless compression and cannot exceed the error bound.
        if self.gorilla.length < GORILLA_MAXIMUM_LENGTH {
            self.gorilla.compress_value(value);
        }
    }

    /// Return [`true`] if any of the current models can represent additional
    /// values, otherwise [`false`].
    fn can_fit_more(&self) -> bool {
        self.pmc_mean_could_fit_all
            || self.swing_could_fit_all
            || self.gorilla.length < GORILLA_MAXIMUM_LENGTH
    }

    /// Store the model that requires the smallest number of bits per value in
    /// `compressed_record_batch_builder`. Returns the index of the first value
    /// in `uncompressed_values` the selected model could not represent.
    fn finish(self, compressed_record_batch_builder: &mut CompressedSegmentBatchBuilder) -> usize {
        // The model that uses the fewest number of bytes per value is selected.
        let SelectedModel {
            model_type_id,
            end_index,
            min_value,
            max_value,
            values,
        } = SelectedModel::new(
            self.start_index,
            self.pmc_mean,
            self.swing,
            self.gorilla,
            self.uncompressed_values,
        );

        // Add timestamps and error.
        let start_time = self.uncompressed_timestamps.value(self.start_index);
        let end_time = self.uncompressed_timestamps.value(end_index);
        let timestamps = timestamps::compress_residual_timestamps(
            &self.uncompressed_timestamps.values()[self.start_index..=end_index],
        );
        let error = f32::NAN; // TODO: compute and store the actual error.

        compressed_record_batch_builder.append_compressed_segment(
            self.univariate_id,
            model_type_id,
            start_time,
            end_time,
            &timestamps,
            min_value,
            max_value,
            &values,
            error,
        );
        end_index + 1
    }
}

/// A batch of compressed segments being built.
struct CompressedSegmentBatchBuilder {
    /// Univariate ids of each compressed segment in the batch.
    univariate_ids: UInt64Builder,
    /// Model type ids of each compressed segment in the batch.
    model_type_ids: UInt8Builder,
    /// First timestamp of each compressed segment in the batch.
    start_times: TimestampBuilder,
    /// Last timestamp of each compressed segment in the batch.
    end_times: TimestampBuilder,
    /// Data required in addition to `start_times` and `end_times` to
    /// reconstruct the timestamps of each compressed segment in the batch.
    timestamps: BinaryBuilder,
    /// Minimum value of each compressed segment in the batch.
    min_values: ValueBuilder,
    /// Maximum value of each compressed segment in the batch.
    max_values: ValueBuilder,
    /// Data required in addition to `min_value` and `max_value` to reconstruct
    /// the values of each compressed segment in the batch within an error
    /// bound.
    values: BinaryBuilder,
    /// Actual error of each compressed segment in the batch.
    error: Float32Builder,
}

impl CompressedSegmentBatchBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            univariate_ids: UInt64Builder::with_capacity(capacity),
            model_type_ids: UInt8Builder::with_capacity(capacity),
            start_times: TimestampBuilder::with_capacity(capacity),
            end_times: TimestampBuilder::with_capacity(capacity),
            timestamps: BinaryBuilder::with_capacity(capacity, capacity),
            min_values: ValueBuilder::with_capacity(capacity),
            max_values: ValueBuilder::with_capacity(capacity),
            values: BinaryBuilder::with_capacity(capacity, capacity),
            error: Float32Builder::with_capacity(capacity),
        }
    }

    /// Append a compressed segment to the builder.
    #[allow(clippy::too_many_arguments)]
    fn append_compressed_segment(
        &mut self,
        univariate_id: u64,
        model_type_id: u8,
        start_time: Timestamp,
        end_time: Timestamp,
        timestamps: &[u8],
        min_value: Value,
        max_value: Value,
        values: &[u8],
        error: f32,
    ) {
        self.univariate_ids.append_value(univariate_id);
        self.model_type_ids.append_value(model_type_id);
        self.start_times.append_value(start_time);
        self.end_times.append_value(end_time);
        self.timestamps.append_value(timestamps);
        self.min_values.append_value(min_value);
        self.max_values.append_value(max_value);
        self.values.append_value(values);
        self.error.append_value(error);
    }

    /// Return [`RecordBatch`] of compressed segments and consume the builder.
    fn finish(mut self, compressed_schema: &CompressedSchema) -> RecordBatch {
        RecordBatch::try_new(
            compressed_schema.0.clone(),
            vec![
                Arc::new(self.univariate_ids.finish()),
                Arc::new(self.model_type_ids.finish()),
                Arc::new(self.start_times.finish()),
                Arc::new(self.end_times.finish()),
                Arc::new(self.timestamps.finish()),
                Arc::new(self.min_values.finish()),
                Arc::new(self.max_values.finish()),
                Arc::new(self.values.finish()),
                Arc::new(self.error.finish()),
            ],
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::UInt8Array;
    use rand::{thread_rng, Rng};

    use crate::compression::test_util as compression_test_util;
    use crate::metadata::test_util;
    use crate::models;
    use compression_test_util::StructureOfValues;

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
        let values = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::Constant,
            None,
            None,
        );
        let timestamps = compression_test_util::generate_timestamps(values.len(), false);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);

        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_try_compress_irregular_constant_time_series() {
        let values = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::Constant,
            None,
            None,
        );
        let timestamps = compression_test_util::generate_timestamps(values.len(), true);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);

        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_try_compress_regular_almost_constant_time_series() {
        // To generate almost constant time series, Random with a small
        // margin between min_step and max_step is selected for generate_values.
        let values = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::Random,
            Some(9.8),
            Some(10.2),
        );
        let timestamps = compression_test_util::generate_timestamps(values.len(), false);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_FIVE);

        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_try_compress_irregular_almost_constant_time_series() {
        // To generate almost constant time series, the Random enum with a small
        // margin between min_step and max_step is used in generate_values.
        let values = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::Random,
            Some(9.8),
            Some(10.2),
        );
        let timestamps = compression_test_util::generate_timestamps(values.len(), true);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_FIVE);

        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_try_compress_regular_linear_time_series() {
        let values = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::Linear,
            None,
            None,
        );
        let timestamps = compression_test_util::generate_timestamps(values.len(), false);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);

        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_try_compress_irregular_linear_time_series() {
        // Create a random linear equation and generate random timestamps.
        // A linear equation must be created instead of using generate_values,
        // since randomly generated irregular timestamps combined with linear values
        // would not fit the equation.
        let a: i64 = thread_rng().gen_range(-10..10);
        let b: i64 = thread_rng().gen_range(1..50);
        let timestamps = compression_test_util::generate_timestamps(TRY_COMPRESS_TEST_LENGTH, true);

        // Calculate the corresponding values on the y axis. These values are not f32 initially
        // because precision errors may occur, and try_compress therefore fails to make a Swing model within
        // ERROR_BOUND_ZERO.
        let i64values: Vec<i64> = timestamps.iter().map(|&x| a * x + b).collect();
        let values: Vec<f32> = i64values.iter().map(|&x| x as f32).collect();

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_FIVE);

        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_try_compress_regular_almost_linear_time_series() {
        let values = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::AlmostLinear,
            Some(9.0),
            Some(11.0),
        );
        let timestamps = compression_test_util::generate_timestamps(values.len(), false);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_FIVE);

        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_try_compress_irregular_almost_linear_time_series() {
        let values = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::AlmostLinear,
            Some(9.8),
            Some(10.2),
        );
        let timestamps = compression_test_util::generate_timestamps(values.len(), true);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, 15.0);

        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_try_compress_regular_random_time_series() {
        let values = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::Random,
            Some(0.0),
            Some(f32::MAX),
        );
        let timestamps = compression_test_util::generate_timestamps(values.len(), false);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);

        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::GORILLA_ID],
        )
    }

    #[test]
    fn test_try_compress_irregular_random_time_series() {
        let values = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::Random,
            Some(0.0),
            Some(f32::MAX),
        );
        let timestamps = compression_test_util::generate_timestamps(values.len(), true);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);

        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::GORILLA_ID],
        )
    }

    #[test]
    fn test_try_compress_regular_random_linear_constant_time_series() {
        let mut constant = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::Constant,
            None,
            None,
        );
        let mut linear = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::Linear,
            None,
            None,
        );
        let mut random = compression_test_util::generate_values(
            TRY_COMPRESS_TEST_LENGTH,
            StructureOfValues::Random,
            Some(0.0),
            Some(f32::MAX),
        );
        let mut values = vec![];
        values.append(&mut random);
        values.append(&mut linear);
        values.append(&mut constant);
        let timestamps = compression_test_util::generate_timestamps(values.len(), false);

        let (uncompressed_timestamps, compressed_record_batch) =
            create_and_compress_time_series(&values, &timestamps, ERROR_BOUND_ZERO);

        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::GORILLA_ID, models::SWING_ID, models::PMC_MEAN_ID],
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
        values: &Vec<f32>,
        timestamps: &Vec<i64>,
        error_bound: f32,
    ) -> (TimestampArray, RecordBatch) {
        let (uncompressed_timestamps, uncompressed_values) =
            create_uncompressed_time_series(&timestamps, &values);
        let error_bound = ErrorBound::try_new(error_bound).unwrap();
        let compressed_record_batch = try_compress(
            1,
            &uncompressed_timestamps,
            &uncompressed_values,
            error_bound,
            &test_util::compressed_schema(),
        )
        .unwrap();
        (uncompressed_timestamps, compressed_record_batch)
    }

    fn assert_compressed_record_batch_with_segments_from_regular_time_series(
        uncompressed_timestamps: &TimestampArray,
        compressed_record_batch: &RecordBatch,
        expected_model_type_ids: &[u8],
    ) {
        assert_eq!(
            expected_model_type_ids.len(),
            compressed_record_batch.num_rows()
        );

        let mut total_compressed_length = 0;
        for segment in 0..expected_model_type_ids.len() {
            let expected_model_type_id = expected_model_type_ids[segment];
            let model_type_id =
                crate::array!(compressed_record_batch, 1, UInt8Array).value(segment);
            assert_eq!(expected_model_type_id, model_type_id);

            let start_time =
                crate::array!(compressed_record_batch, 2, TimestampArray).value(segment);
            let end_time = crate::array!(compressed_record_batch, 3, TimestampArray).value(segment);
            let timestamps = crate::array!(compressed_record_batch, 4, BinaryArray).value(segment);

            total_compressed_length += models::len(start_time, end_time, timestamps);
        }
        assert_eq!(uncompressed_timestamps.len(), total_compressed_length);
    }

    // Tests for merge_segments().
    #[test]
    fn test_merge_compressed_segments_empty_batch() {
        let merged_record_batch = merge_segments(
            CompressedSegmentBatchBuilder::new(0).finish(&test_util::compressed_schema()),
        );
        assert_eq!(0, merged_record_batch.num_rows())
    }

    #[test]
    fn test_merge_compressed_segments_batch() {
        // merge_segments() currently merge segments with equivalent models.
        let univariate_id = 1;
        let model_type_id = 1;
        let values = &[];
        let min_value = 5.0;
        let max_value = 5.0;

        // Add a mix of different segments that can be merged into two segments.
        let mut compressed_record_batch_builder = CompressedSegmentBatchBuilder::new(10);

        for start_time in (100..2000).step_by(400) {
            compressed_record_batch_builder.append_compressed_segment(
                univariate_id,
                model_type_id,
                start_time,
                start_time + 100,
                &[],
                min_value,
                max_value,
                values,
                0.0,
            );

            compressed_record_batch_builder.append_compressed_segment(
                univariate_id,
                model_type_id + 1,
                start_time + 200,
                start_time + 300,
                &[],
                -min_value,
                -max_value,
                values,
                10.0,
            );
        }

        let compressed_record_batch =
            compressed_record_batch_builder.finish(&test_util::compressed_schema());
        let merged_record_batch = merge_segments(compressed_record_batch);

        // Extract the columns from the RecordBatch.
        let start_times = crate::array!(merged_record_batch, 2, TimestampArray);
        let end_times = crate::array!(merged_record_batch, 3, TimestampArray);
        let timestamps = crate::array!(merged_record_batch, 4, BinaryArray);
        let min_values = crate::array!(merged_record_batch, 5, ValueArray);
        let max_values = crate::array!(merged_record_batch, 6, ValueArray);
        let values = crate::array!(merged_record_batch, 7, BinaryArray);
        let errors = crate::array!(merged_record_batch, 8, Float32Array);

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
}

#[cfg(test)]
/// Separate module for utility functions.
pub mod test_util {
    use rand::distributions::Uniform;
    use rand::{thread_rng, Rng};

    pub enum StructureOfValues {
        Constant,
        Random,
        Linear,
        AlmostLinear,
    }

    /// Generate constant/random/linear/almost-linear test values with the
    /// [ThreadRng](rand::rngs::thread::ThreadRng) randomizer. Select the length using `length` and type of
    /// values to be generated using [`StructureOfValues`]. If `Random` is selected, min_step and max_step
    /// is the range of values which can be generated. If `AlmostLinear` is selected, `min_step`
    /// and `max_step` is the maximum and minimum change that should be applied from one value to the next.
    /// Returns the generated values as a [`Vec`].
    pub fn generate_values(
        length: usize,
        data_type: StructureOfValues,
        min: Option<f32>,
        max: Option<f32>,
    ) -> Vec<f32> {
        let mut randomizer = thread_rng();
        let mut values: Vec<f32> = vec![];

        match data_type {
            // Generates almost linear data.
            StructureOfValues::AlmostLinear => {
                let mut random_linear = vec![];
                let mut previous_value: f32 = 0.0;
                for _ in 0..length {
                    let next_value = (randomizer.sample(Uniform::from(min.unwrap()..max.unwrap())))
                        + previous_value;
                    random_linear.push(next_value);
                    previous_value = next_value;
                }
                values.append(&mut random_linear);
            }

            // Generates linear data.
            StructureOfValues::Linear => {
                let mut linear =
                    Vec::from_iter((10..(length + 1) * 10).step_by(10).map(|v| v as f32));
                values.append(&mut linear);
            }

            // Generates randomized data.
            StructureOfValues::Random => {
                let mut random = vec![];
                for _ in 0..length {
                    random.push(randomizer.sample(Uniform::from(min.unwrap()..max.unwrap())));
                }
                values.append(&mut random);
            }

            // Generates constant data.
            StructureOfValues::Constant => {
                let mut constant = vec![10.0; length as usize];
                values.append(&mut constant);
            }
        }

        values
    }

    /// Generate regular/irregular timestamps with the [ThreadRng](rand::rngs::thread::ThreadRng) randomizer.
    /// Select the length and type of timestamps to be generated using the parameters `length` and `irregular`.
    /// Returns the generated timestamps as a [`Vec`].
    pub fn generate_timestamps(length: usize, irregular: bool) -> Vec<i64> {
        let mut timestamps = vec![];
        if irregular {
            let mut randomizer = thread_rng();
            let mut previous_timestamp: i64 = 0;
            for _ in 0..length {
                let next_timestamp =
                    (randomizer.sample(Uniform::from(10..20))) + previous_timestamp;
                timestamps.push(next_timestamp);
                previous_timestamp = next_timestamp;
            }
        } else {
            timestamps = Vec::from_iter((100..(length + 1) as i64 * 100).step_by(100));
        }

        timestamps
    }
}
