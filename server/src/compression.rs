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

//! Compress `UncompressedSegment`s provided by `StorageEngine` using the model
//! types in `models` to produce compressed segments which are returned to
//! `StorageEngine`.

use std::sync::Arc;

use datafusion::arrow::array::{BinaryBuilder, Float32Builder, UInt8Builder};
use datafusion::arrow::record_batch::RecordBatch;

use crate::errors::ModelarDBError;
use crate::models;
use crate::models::{gorilla::Gorilla, pmcmean::PMCMean, swing::Swing, ErrorBound, SelectedModel};
use crate::storage::StorageEngine;
use crate::types::{Timestamp, TimestampArray, TimestampBuilder, Value, ValueArray, ValueBuilder};

// TODO: use Gorilla as a fallback to remove GORILLA_MAXIMUM_LENGTH.
/// Maximum number of data points that models of type Gorilla can represent per
/// compressed segment. As models of type Gorilla use lossless compression they
/// will never exceed the user-defined error bounds.
const GORILLA_MAXIMUM_LENGTH: usize = 50;

// TODO: support irregular univariate time series.
/// Compress the regular `uncompressed_timestamps` using a start time, end time,
/// and a sampling interval, and `uncompressed_values` within `error_bound`
/// using the model types in `models`. Returns `CompressionError` if
/// `uncompressed_timestamps` and `uncompressed_values` have different lengths,
/// otherwise the resulting compressed segments are returned as a `RecordBatch`
/// with the schema provided by `get_compressed_segment_schema_ref()`.
pub fn try_compress(
    uncompressed_timestamps: &TimestampArray,
    uncompressed_values: &ValueArray,
    error_bound: ErrorBound,
) -> Result<RecordBatch, ModelarDBError> {
    // The uncompressed data must be passed as arrays instead of a RecordBatch
    // as a TimestampArray and a ValueArray is the only supported input.
    // However, as a result it is necessary to verify they have the same length.
    if uncompressed_timestamps.len() != uncompressed_values.len() {
        return Err(ModelarDBError::CompressionError(
            "Uncompressed timestamps and uncompressed values have different lengths.".to_owned(),
        ));
    }

    // num_data points is allocated to not reallocate as one model is created
    // per data point in the worst cast, however, usually a lot fewer are used.
    let num_data_points = uncompressed_timestamps.len();
    let mut compressed_record_batch_builder = CompressedRecordBatchBuilder::new(num_data_points);

    // Compress the uncompressed timestamps and uncompressed values.
    let mut current_row = 0;
    while current_row < num_data_points {
        // Create a compressed segment that represents the timestamps and values
        // from current_row to row_n within error_bound where row_n <= num_rows.
        let compressed_segment_builder = create_next_compressed_segment(
            current_row,
            num_data_points,
            &uncompressed_timestamps,
            &uncompressed_values,
            error_bound,
        );
        current_row += compressed_segment_builder.finish(&mut compressed_record_batch_builder);
    }
    Ok(compressed_record_batch_builder.finnish())
}

/// Create a compressed segment that represents the regular timestamps in
/// `uncompressed_timestamps` and the values in `uncompressed_values` from
/// `start_index` to index_n within `error_bound` where index_n <= `end_index`.
fn create_next_compressed_segment<'a>(
    start_index: usize,
    end_index: usize,
    uncompressed_timestamps: &'a TimestampArray,
    uncompressed_values: &'a ValueArray,
    error_bound: ErrorBound,
) -> CompressedSegmentBuilder<'a> {
    let mut compressed_segment_builder =
        CompressedSegmentBuilder::new(uncompressed_timestamps, uncompressed_values, error_bound);

    let mut current_index = start_index;
    while compressed_segment_builder.can_fit_more() && current_index < end_index {
        let timestamp = uncompressed_timestamps.value(current_index);
        let value = uncompressed_values.value(current_index);
        compressed_segment_builder.try_to_update_models(timestamp, value);
        current_index += 1;
    }
    compressed_segment_builder
}

/// A single compressed segment being build from an uncompressed segment using
/// the model types in `models`.
struct CompressedSegmentBuilder<'a> {
    /// The regular timestamps of the uncompressed segment the compressed
    /// segment is being build from.
    uncompressed_timestamps: &'a TimestampArray,
    /// The values of the uncompressed segment the compressed segment is being
    /// build from.
    uncompressed_values: &'a ValueArray,
    /// Index of the first data point in `self.uncompressed_timestamps` and
    /// `self.uncompressed_values` the compressed segment represents.
    start_index: usize,
    /// Constant function that represents the values in
    /// `self.uncompressed_values` from `self.start_index` to `self.start_index`
    /// + `self.pmc_mean.length`.
    pmc_mean: PMCMean,
    /// Indicates if `self.pmc_mean` could represent all values in
    /// `self.uncompressed_values` from `self.start_index` to `self.start_index`
    /// + `self.length`.
    pmc_mean_has_fit_all: bool,
    /// Linear function that represents the values in `self.uncompressed_values`
    /// from `self.start_index` to `self.start_index` + `self.swing.length`.
    swing: Swing,
    /// Indicates if `self.swing` could represent all data points in
    /// `self.uncompressed_timestamps` and `self.uncompressed_values` from
    /// `self.start_index` to `self.start_index` + `self.length`.
    swing_has_fit_all: bool,
    /// Values in `self.uncompressed_values` from `self.start_index` to
    /// `self.start_index` + `self.gorilla.length` compressed using lossless
    /// compression.
    gorilla: Gorilla,
}

impl<'a> CompressedSegmentBuilder<'a> {
    fn new(
        uncompressed_timestamps: &'a TimestampArray,
        uncompressed_values: &'a ValueArray,
        error_bound: ErrorBound,
    ) -> Self {
        Self {
            uncompressed_timestamps,
            uncompressed_values,
            start_index: 0,
            pmc_mean: PMCMean::new(error_bound),
            pmc_mean_has_fit_all: true,
            swing: Swing::new(error_bound),
            swing_has_fit_all: true,
            gorilla: Gorilla::new(),
        }
    }

    /// Attempt to update the current models to also represent the `value` of
    /// the data point collected at `timestamp`.
    fn try_to_update_models(&mut self, timestamp: Timestamp, value: Value) {
        self.pmc_mean_has_fit_all = self.pmc_mean_has_fit_all && self.pmc_mean.fit_value(value);

        self.swing_has_fit_all =
            self.swing_has_fit_all && self.swing.fit_data_point(timestamp, value);

        // Gorilla uses lossless compression and cannot exceed the error bound.
        if self.gorilla.length < GORILLA_MAXIMUM_LENGTH {
            self.gorilla.compress_value(value);
        }
    }

    /// Return `true` if any of the current models can represent additional
    /// values, otherwise `false`.
    fn can_fit_more(&self) -> bool {
        self.pmc_mean_has_fit_all
            || self.swing_has_fit_all
            || self.gorilla.length < GORILLA_MAXIMUM_LENGTH
    }

    /// Store the model that requires the smallest number of bits per value in
    /// `compressed_record_batch_builder`. Returns the index of the value in
    /// `uncompressed_values` the selected model could not represent.
    fn finish(self, compressed_record_batch_builder: &mut CompressedRecordBatchBuilder) -> usize {
        // The model that uses the fewest number of bytes per value is selected.
        let SelectedModel {
            model_type_id,
            end_index,
            min_value,
            max_value,
            values,
        } = models::select_model(
            self.start_index,
            self.pmc_mean,
            self.swing,
            self.gorilla,
            self.uncompressed_values,
        );

        // Timestamps
        let start_time = self.uncompressed_timestamps.value(self.start_index);
        let end_time = self.uncompressed_timestamps.value(end_index);
        let timestamps = &[]; // TODO: compress irregular timestamps.

        // TODO: compute and store the actual error.
        let error = f32::NAN;

        compressed_record_batch_builder.append_compressed_segment(
            model_type_id,
            timestamps,
            start_time,
            end_time,
            &values,
            min_value,
            max_value,
            error,
        );
        end_index + 1
    }
}

/// A batch of compressed segments being build.
struct CompressedRecordBatchBuilder {
    /// Model type ids of each compressed segment in the batch.
    model_type_ids: UInt8Builder,
    /// Data required in addition to `start_times` and `end_times` to
    /// reconstruct the timestamps of each compressed segment in the batch.
    timestamps: BinaryBuilder,
    /// First timestamp of each compressed segment in the batch.
    start_times: TimestampBuilder,
    /// Last timestamp of each compressed segment in the batch.
    end_times: TimestampBuilder,
    /// Data required in addition to `min_value` and `max_value` to reconstruct
    /// the values of each compressed segment in the batch within an error
    /// bound.
    values: BinaryBuilder,
    /// Minimum value of each compressed segment in the batch.
    min_values: ValueBuilder,
    /// Maximum value of each compressed segment in the batch.
    max_values: ValueBuilder,
    /// Actual error of each compressed segment in the batch.
    error: Float32Builder,
}

impl CompressedRecordBatchBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            model_type_ids: UInt8Builder::new(capacity),
            timestamps: BinaryBuilder::new(capacity),
            start_times: TimestampBuilder::new(capacity),
            end_times: TimestampBuilder::new(capacity),
            values: BinaryBuilder::new(capacity),
            min_values: ValueBuilder::new(capacity),
            max_values: ValueBuilder::new(capacity),
            error: Float32Builder::new(capacity),
        }
    }

    /// Append a compressed segment to the builder.
    fn append_compressed_segment(
        &mut self,
        model_type_id: u8,
        timestamps: &[u8],
        start_time: Timestamp,
        end_time: Timestamp,
        values: &[u8],
        min_value: Value,
        max_value: Value,
        error: f32,
    ) {
        // unwrap() is used as append_value() never returns Error for
        // PrimitiveBuilder.
        self.model_type_ids.append_value(model_type_id).unwrap();
        self.timestamps.append_value(timestamps).unwrap();
        self.start_times.append_value(start_time).unwrap();
        self.end_times.append_value(end_time).unwrap();
        self.values.append_value(values).unwrap();
        self.min_values.append_value(min_value).unwrap();
        self.max_values.append_value(max_value).unwrap();
        self.error.append_value(error).unwrap();
    }

    /// Return `RecordBatch` of compressed segments and consume the builder.
    fn finnish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(StorageEngine::get_compressed_segment_schema()),
            vec![
                Arc::new(self.model_type_ids.finish()),
                Arc::new(self.timestamps.finish()),
                Arc::new(self.start_times.finish()),
                Arc::new(self.end_times.finish()),
                Arc::new(self.values.finish()),
                Arc::new(self.min_values.finish()),
                Arc::new(self.max_values.finish()),
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
    // TODO: add tests with random time series and where the selected model is
    // not the longest.

    // Tests compress.
    #[test]
    fn test_compress_empty_time_series() {
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let (uncompressed_timestamps, uncompressed_values) =
            create_uncompressed_time_series(&[], &[]);
        let compressed_record_batch =
            try_compress(&uncompressed_timestamps, &uncompressed_values, error_bound).unwrap();
        assert_eq!(0, compressed_record_batch.num_rows())
    }

    #[test]
    fn test_compress_regular_constant_time_series() {
        let timestamps = Vec::from_iter((100..1000).step_by(100));
        let values = vec![10.0; timestamps.len()];
        let (uncompressed_timestamps, uncompressed_values) =
            create_uncompressed_time_series(&timestamps, &values);
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let compressed_record_batch =
            try_compress(&uncompressed_timestamps, &uncompressed_values, error_bound).unwrap();
        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_compress_regular_almost_constant_time_series() {
        let timestamps = Vec::from_iter((100..1000).step_by(100));
        let values = vec![10.1, 10.0, 10.2, 10.2, 10.0, 10.1, 10.0, 10.0, 10.0];
        let (uncompressed_timestamps, uncompressed_values) =
            create_uncompressed_time_series(&timestamps, &values);
        let error_bound = ErrorBound::try_new(5.0).unwrap();
        let compressed_record_batch =
            try_compress(&uncompressed_timestamps, &uncompressed_values, error_bound).unwrap();
        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_compress_regular_linear_time_series() {
        let timestamps = Vec::from_iter((100..1000).step_by(100));
        let values = Vec::from_iter((10..100).step_by(10).map(|v| v as f32));
        let (uncompressed_timestamps, uncompressed_values) =
            create_uncompressed_time_series(&timestamps, &values);
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let compressed_record_batch =
            try_compress(&uncompressed_timestamps, &uncompressed_values, error_bound).unwrap();
        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_compress_regular_almost_linear_time_series() {
        let timestamps = Vec::from_iter((100..1000).step_by(100));
        let values = vec![10.0, 20.0, 30.1, 40.8, 51.0, 60.2, 70.1, 80.7, 90.4];
        let (uncompressed_timestamps, uncompressed_values) =
            create_uncompressed_time_series(&timestamps, &values);
        let error_bound = ErrorBound::try_new(5.0).unwrap();
        let compressed_record_batch =
            try_compress(&uncompressed_timestamps, &uncompressed_values, error_bound).unwrap();
        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_compress_regular_random_time_series() {
        let timestamps = Vec::from_iter((100..1000).step_by(100));
        let values = vec![7.47, 13.34, 14.50, 4.88, 7.84, 6.69, 8.63, 5.109, 2.16];
        let (uncompressed_timestamps, uncompressed_values) =
            create_uncompressed_time_series(&timestamps, &values);
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let compressed_record_batch =
            try_compress(&uncompressed_timestamps, &uncompressed_values, error_bound).unwrap();
        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_timestamps,
            &compressed_record_batch,
            &[models::GORILLA_ID],
        )
    }

    #[test]
    fn test_compress_regular_random_linear_constant_time_series() {
        let mut constant = vec![10.0; 100];
        let mut linear = Vec::from_iter((10..1000).step_by(10).map(|v| v as f32));
        let mut random = vec![7.47, 13.34, 14.50, 4.88, 7.84, 6.69, 8.63, 5.109, 2.16];

        let mut values = vec![];
        values.append(&mut random);
        values.append(&mut linear);
        values.append(&mut constant);

        // TODO: make all tests use an auto generated set of timestamps based on the length of values.
        let timestamps = Vec::from_iter((100..(values.len() + 1) as i64 * 100).step_by(100));
        let (uncompressed_timestamps, uncompressed_values) =
            create_uncompressed_time_series(&timestamps, &values);

        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let compressed_record_batch =
            try_compress(&uncompressed_timestamps, &uncompressed_values, error_bound).unwrap();
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
        let mut timestamps_builder = TimestampBuilder::new(timestamps.len());
        timestamps_builder.append_slice(timestamps).unwrap();
        let mut values_builder = ValueBuilder::new(values.len());
        values_builder.append_slice(values).unwrap();
        (timestamps_builder.finish(), values_builder.finish())
    }

    fn assert_compressed_record_batch_with_segments_from_regular_time_series(
        uncompressed_timestamps: &TimestampArray,
        compressed_record_batch: &RecordBatch,
        expected_model_type_ids: &[u8],
    ) {
        let sampling_interval = uncompressed_timestamps.value(1) - uncompressed_timestamps.value(0);

        assert_eq!(
            expected_model_type_ids.len(),
            compressed_record_batch.num_rows()
        );

        let mut total_compressed_length = 0;
        for segment in 0..expected_model_type_ids.len() {
            let expected_model_type_id = expected_model_type_ids[segment];
            let model_type_id = compressed_record_batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(segment);
            assert_eq!(expected_model_type_id, model_type_id);

            let start_time = compressed_record_batch
                .column(2)
                .as_any()
                .downcast_ref::<TimestampArray>()
                .unwrap()
                .value(segment);
            let end_time = compressed_record_batch
                .column(3)
                .as_any()
                .downcast_ref::<TimestampArray>()
                .unwrap()
                .value(segment);
            total_compressed_length +=
                models::length(start_time, end_time, sampling_interval as i32);
        }
        assert_eq!(uncompressed_timestamps.len(), total_compressed_length);
    }
}
