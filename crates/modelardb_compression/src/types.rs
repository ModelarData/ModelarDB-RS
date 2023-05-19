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

//! The types used throughout the crate.

use std::{cmp::Ordering, sync::Arc};

use arrow::{
    array::{BinaryBuilder, Float32Builder, UInt64Builder, UInt8Builder},
    record_batch::RecordBatch,
};
use modelardb_common::{
    errors::ModelarDbError,
    schemas::COMPRESSED_SCHEMA,
    types::{Timestamp, TimestampArray, TimestampBuilder, Value, ValueArray, ValueBuilder},
};

use crate::models::{pmc_mean::PMCMean, swing::Swing, PMC_MEAN_ID, SWING_ID};

/// Error bound in percentage that is guaranteed to be from 0.0% to 100.0%. For both `PMCMean`,
/// `Swing`, and `Gorilla` the error bound is interpreted as a relative per value error bound.
#[derive(Debug, Copy, Clone)]
pub struct ErrorBound(pub(crate) f32); // Simpler for the model types to directly work on the f32.

impl ErrorBound {
    /// Return [`ErrorBound`] if `percentage` is a value from 0.0% to 100.0%, otherwise
    /// [`CompressionError`](ModelarDbError::CompressionError) is returned.
    pub fn try_new(percentage: f32) -> Result<Self, ModelarDbError> {
        if !(0.0..=100.0).contains(&percentage) {
            Err(ModelarDbError::CompressionError(
                "Error bound must be a value from 0.0% to 100.0%.".to_owned(),
            ))
        } else {
            Ok(Self(percentage))
        }
    }

    /// Consumes `self`, returning the error bound as a [`f32`].
    pub fn into_inner(self) -> f32 {
        self.0
    }
}

/// Enable equal and not equal for [`ErrorBound`] and [`f32`].
impl PartialEq<ErrorBound> for f32 {
    fn eq(&self, other: &ErrorBound) -> bool {
        self.eq(&other.0)
    }
}

/// Enable less than and greater than for [`ErrorBound`] and [`f32`].
impl PartialOrd<ErrorBound> for f32 {
    fn partial_cmp(&self, other: &ErrorBound) -> Option<Ordering> {
        self.partial_cmp(&other.0)
    }
}

/// A batch of compressed segments being built.
pub(crate) struct CompressedSegmentBatchBuilder {
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
    /// Values between this and the next segment, compressed using [`Gorilla`],
    /// that the models could not represent efficiently within the error bound
    /// and which are too few for a new segment due to the amount of metadata.
    residuals: BinaryBuilder,
    /// Actual error of each compressed segment in the batch.
    error: Float32Builder,
}

impl CompressedSegmentBatchBuilder {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            univariate_ids: UInt64Builder::with_capacity(capacity),
            model_type_ids: UInt8Builder::with_capacity(capacity),
            start_times: TimestampBuilder::with_capacity(capacity),
            end_times: TimestampBuilder::with_capacity(capacity),
            timestamps: BinaryBuilder::with_capacity(capacity, capacity),
            min_values: ValueBuilder::with_capacity(capacity),
            max_values: ValueBuilder::with_capacity(capacity),
            values: BinaryBuilder::with_capacity(capacity, capacity),
            residuals: BinaryBuilder::with_capacity(capacity, capacity),
            error: Float32Builder::with_capacity(capacity),
        }
    }

    /// Append a compressed segment to the builder.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn append_compressed_segment(
        &mut self,
        univariate_id: u64,
        model_type_id: u8,
        start_time: Timestamp,
        end_time: Timestamp,
        timestamps: &[u8],
        min_value: Value,
        max_value: Value,
        values: &[u8],
        residuals: &[u8],
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
        self.residuals.append_value(residuals);
        self.error.append_value(error);
    }

    /// Return [`RecordBatch`] of compressed segments and consume the builder.
    pub(crate) fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            COMPRESSED_SCHEMA.0.clone(),
            vec![
                Arc::new(self.univariate_ids.finish()),
                Arc::new(self.model_type_ids.finish()),
                Arc::new(self.start_times.finish()),
                Arc::new(self.end_times.finish()),
                Arc::new(self.timestamps.finish()),
                Arc::new(self.min_values.finish()),
                Arc::new(self.max_values.finish()),
                Arc::new(self.values.finish()),
                Arc::new(self.residuals.finish()),
                Arc::new(self.error.finish()),
            ],
        )
        .unwrap()
    }
}

/// A compressed model being built from an uncompressed segment using the potentially lossy model
/// types in [`models`]. Each of the potentially lossy model types is used to fit models to the data
/// points, and then the model that uses the fewest number of bytes per value is selected.
pub(crate) struct CompressedModelBuilder {
    /// Index of the first data point in `uncompressed_timestamps` and `uncompressed_values` the
    /// compressed model represents values for.
    start_index: usize,
    /// Constant function that currently represents the values in `uncompressed_values` from
    /// `start_index` to `start_index` + `pmc_mean.length`.
    pmc_mean: PMCMean,
    /// Indicates if `pmc_mean` could represent all values in `uncompressed_values` from
    /// `start_index` to `current_index` in `new()`.
    pmc_mean_could_fit_all: bool,
    /// Linear function that represents the values in `uncompressed_values` from `start_index` to
    /// `start_index` + `swing.length`.
    swing: Swing,
    /// Indicates if `swing` could represent all values in `uncompressed_values` from `start_index`
    /// to `current_index` in `new()`.
    swing_could_fit_all: bool,
}

impl CompressedModelBuilder {
    /// Create a compressed model that represents the values in `uncompressed_values` from
    /// `start_index` to index within `error_bound` where index <= `end_index`.
    pub(crate) fn new(
        start_index: usize,
        end_index: usize,
        uncompressed_timestamps: &TimestampArray,
        uncompressed_values: &ValueArray,
        error_bound: ErrorBound,
    ) -> Self {
        let mut compressed_segment_builder = Self {
            start_index,
            pmc_mean: PMCMean::new(error_bound),
            pmc_mean_could_fit_all: true,
            swing: Swing::new(error_bound),
            swing_could_fit_all: true,
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
    }

    /// Return [`true`] if any of the current models can represent additional
    /// values, otherwise [`false`].
    fn can_fit_more(&self) -> bool {
        self.pmc_mean_could_fit_all || self.swing_could_fit_all
    }

    /// Return the model that requires the fewest number of bytes per value.
    pub(crate) fn finish(self) -> SelectedModel {
        SelectedModel::new(self.start_index, self.pmc_mean, self.swing)
    }
}

/// Model that uses the fewest number of bytes per value.
pub struct SelectedModel {
    /// Id of the model type that created this model.
    pub model_type_id: u8,
    /// Index of the first data point in the `UncompressedDataBuffer` that the
    /// selected model represents.
    pub start_index: usize,
    /// Index of the last data point in the `UncompressedDataBuffer` that the
    /// selected model represents.
    pub end_index: usize,
    /// The selected model's minimum value.
    pub min_value: Value,
    /// The selected model's maximum value.
    pub max_value: Value,
    /// Data required in addition to `min` and `max` for the model to
    /// reconstruct the values it represents when given a specific timestamp.
    pub values: Vec<u8>,
    /// The number of bytes per value used by the model.
    pub bytes_per_value: f32,
}

impl SelectedModel {
    /// Select the model that uses the fewest number of bytes per value.
    pub fn new(start_index: usize, pmc_mean: PMCMean, swing: Swing) -> Self {
        let bytes_per_value = [
            (PMC_MEAN_ID, pmc_mean.bytes_per_value()),
            (SWING_ID, swing.bytes_per_value()),
        ];

        // unwrap() cannot fail as the array is not empty and there are no NaN values.
        let selected_model_type_id = bytes_per_value
            .iter()
            .min_by(|x, y| f32::partial_cmp(&x.1, &y.1).unwrap())
            .unwrap()
            .0;

        match selected_model_type_id {
            PMC_MEAN_ID => Self::select_pmc_mean(start_index, pmc_mean),
            SWING_ID => Self::select_swing(start_index, swing),
            _ => panic!("Unknown model type."),
        }
    }

    /// Create a [`SelectedModel`] from `pmc_mean`.
    fn select_pmc_mean(start_index: usize, pmc_mean: PMCMean) -> Self {
        let value = pmc_mean.model();
        let end_index = start_index + pmc_mean.len() - 1;

        Self {
            model_type_id: PMC_MEAN_ID,
            start_index,
            end_index,
            min_value: value,
            max_value: value,
            values: vec![],
            bytes_per_value: pmc_mean.bytes_per_value(),
        }
    }

    /// Create a [`SelectedModel`] from `swing`.
    fn select_swing(start_index: usize, swing: Swing) -> Self {
        let (start_value, end_value) = swing.model();
        let end_index = start_index + swing.len() - 1;
        let min_value = Value::min(start_value, end_value);
        let max_value = Value::max(start_value, end_value);
        let values = vec![(start_value < end_value) as u8];

        Self {
            model_type_id: SWING_ID,
            start_index,
            end_index,
            min_value,
            max_value,
            values,
            bytes_per_value: swing.bytes_per_value(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::num;
    use proptest::proptest;

    use crate::test_util::{self, StructureOfValues};

    const UNCOMPRESSED_TIMESTAMPS: &[Timestamp] = &[100, 200, 300, 400, 500];

    // Tests for ErrorBound.
    proptest! {
    #[test]
    fn test_error_bound_can_be_positive_if_less_than_one_hundred(percentage in num::f32::POSITIVE) {
        if percentage <= 100.0 {
            assert!(ErrorBound::try_new(percentage).is_ok())
        } else {
            assert!(ErrorBound::try_new(percentage).is_err())
        }
    }

    #[test]
    fn test_error_bound_cannot_be_negative(percentage in num::f32::NEGATIVE) {
        assert!(ErrorBound::try_new(percentage).is_err())
    }
    }

    #[test]
    fn test_error_bound_cannot_be_positive_infinity() {
        assert!(ErrorBound::try_new(f32::INFINITY).is_err())
    }

    #[test]
    fn test_error_bound_cannot_be_negative_infinity() {
        assert!(ErrorBound::try_new(f32::NEG_INFINITY).is_err())
    }

    #[test]
    fn test_error_bound_cannot_be_nan() {
        assert!(ErrorBound::try_new(f32::NAN).is_err())
    }

    // Tests for SelectedModel.
    #[test]
    fn test_model_selected_model_attributes_for_pmc_mean() {
        let uncompressed_timestamps = TimestampArray::from(UNCOMPRESSED_TIMESTAMPS.to_vec());
        let uncompressed_values = ValueArray::from(vec![10.0, 10.0, 10.0, 10.0, 10.0]);

        let selected_model =
            create_selected_model(&uncompressed_timestamps, &uncompressed_values, 0.0);

        assert_eq!(PMC_MEAN_ID, selected_model.model_type_id);
        assert_eq!(uncompressed_timestamps.len() - 1, selected_model.end_index);
        assert_eq!(10.0, selected_model.min_value);
        assert_eq!(10.0, selected_model.max_value);
        assert_eq!(0, selected_model.values.len());
    }

    #[test]
    fn test_model_selected_model_attributes_for_increasing_swing() {
        let uncompressed_timestamps = TimestampArray::from(UNCOMPRESSED_TIMESTAMPS.to_vec());
        let uncompressed_values = ValueArray::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        let selected_model =
            create_selected_model(&uncompressed_timestamps, &uncompressed_values, 0.0);

        assert_eq!(SWING_ID, selected_model.model_type_id);
        assert_eq!(uncompressed_timestamps.len() - 1, selected_model.end_index);
        assert_eq!(10.0, selected_model.min_value);
        assert_eq!(50.0, selected_model.max_value);
        assert_eq!(1, selected_model.values.len());
    }

    #[test]
    fn test_model_selected_model_attributes_for_decreasing_swing() {
        let uncompressed_timestamps = TimestampArray::from(UNCOMPRESSED_TIMESTAMPS.to_vec());
        let uncompressed_values = ValueArray::from(vec![50.0, 40.0, 30.0, 20.0, 10.0]);
        let selected_model =
            create_selected_model(&uncompressed_timestamps, &uncompressed_values, 0.0);

        assert_eq!(SWING_ID, selected_model.model_type_id);
        assert_eq!(uncompressed_timestamps.len() - 1, selected_model.end_index);
        assert_eq!(10.0, selected_model.min_value);
        assert_eq!(50.0, selected_model.max_value);
        assert_eq!(1, selected_model.values.len());
    }

    /// This test ensures that the model with the fewest amount of bytes is selected.
    #[test]
    fn test_model_with_fewest_bytes_is_selected() {
        let timestamps = (0..25).collect::<Vec<i64>>();
        let values: Vec<f32> =
            test_util::generate_values(&timestamps, StructureOfValues::Constant, None, None)
                .into_iter()
                .chain(test_util::generate_values(
                    &timestamps,
                    StructureOfValues::Random,
                    Some(0.0),
                    Some(100.0),
                ))
                .collect();
        let timestamps =
            TimestampArray::from_iter_values(test_util::generate_timestamps(values.len(), false));
        let value_array = ValueArray::from(values);

        let selected_model = create_selected_model(&timestamps, &value_array, 10.0);

        assert_eq!(selected_model.model_type_id, PMC_MEAN_ID);
    }

    fn create_selected_model(
        uncompressed_timestamps: &TimestampArray,
        uncompressed_values: &ValueArray,
        error_bound: f32,
    ) -> SelectedModel {
        let error_bound = ErrorBound::try_new(error_bound).unwrap();
        let mut pmc_mean = PMCMean::new(error_bound);
        let mut swing = Swing::new(error_bound);

        let mut pmc_mean_could_fit_all = true;
        let mut swing_could_fit_all = true;
        for index in 0..uncompressed_timestamps.len() {
            let timestamp = uncompressed_timestamps.value(index);
            let value = uncompressed_values.value(index);

            pmc_mean_could_fit_all = pmc_mean_could_fit_all && pmc_mean.fit_value(value);
            swing_could_fit_all = swing_could_fit_all && swing.fit_data_point(timestamp, value);
        }
        SelectedModel::new(0, pmc_mean, swing)
    }
}
