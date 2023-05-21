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

use std::debug_assert;
use std::{cmp::Ordering, sync::Arc};

use arrow::array::{BinaryBuilder, Float32Builder, UInt64Builder, UInt8Builder};
use arrow::record_batch::RecordBatch;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::schemas::COMPRESSED_SCHEMA;
use modelardb_common::types::{
    Timestamp, TimestampArray, TimestampBuilder, Value, ValueArray, ValueBuilder,
};

use crate::models::gorilla::Gorilla;
use crate::models::pmc_mean::PMCMean;
use crate::models::swing::Swing;
use crate::models::{timestamps, VALUE_SIZE_IN_BYTES};
use crate::models::{PMC_MEAN_ID, SWING_ID};

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

/// A model being built from an uncompressed segment using the potentially lossy model types in
/// [`models`]. Each of the potentially lossy model types is used to fit models to the data points,
/// and then the model that uses the fewest number of bytes per value is selected.
pub(crate) struct ModelBuilder {
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

impl ModelBuilder {
    /// Create a model that represents a sub-sequence of uncompressed values that starts at
    /// `start_index`, is within `error_bound`, and uses the fewest number of bytes per value.
    pub(crate) fn new(start_index: usize, error_bound: ErrorBound) -> Self {
        Self {
            start_index,
            pmc_mean: PMCMean::new(error_bound),
            pmc_mean_could_fit_all: true,
            swing: Swing::new(error_bound),
            swing_could_fit_all: true,
        }
    }

    /// Attempt to update the current models to also represent the `value` of
    /// the data point collected at `timestamp`.
    pub(crate) fn try_to_update_models(&mut self, timestamp: Timestamp, value: Value) {
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
    pub(crate) fn can_fit_more(&self) -> bool {
        self.pmc_mean_could_fit_all || self.swing_could_fit_all
    }

    /// Return the model that requires the fewest number of bytes per value.
    pub(crate) fn finish(self) -> CompressedSegmentBuilder {
        let bytes_per_value = [
            (PMC_MEAN_ID, self.pmc_mean.bytes_per_value()),
            (SWING_ID, self.swing.bytes_per_value()),
        ];

        // unwrap() cannot fail as the array is not empty and there are no NaN values.
        let model_type_id = bytes_per_value
            .iter()
            .min_by(|x, y| f32::partial_cmp(&x.1, &y.1).unwrap())
            .unwrap()
            .0;

        match model_type_id {
            PMC_MEAN_ID => Self::select_pmc_mean(self.start_index, self.pmc_mean),
            SWING_ID => Self::select_swing(self.start_index, self.swing),
            _ => panic!("Unknown model type."),
        }
    }

    /// Return a [`CompressedSegmentBuilder`] containing the model fitted by [`PMCMean`].
    fn select_pmc_mean(start_index: usize, pmc_mean: PMCMean) -> CompressedSegmentBuilder {
        let end_index = start_index + pmc_mean.len() - 1;
        let bytes_per_value = pmc_mean.bytes_per_value();
        let value = pmc_mean.model();

        CompressedSegmentBuilder {
            model_type_id: PMC_MEAN_ID,
            start_index,
            end_index,
            min_value: value,
            max_value: value,
            values: vec![],
            model_last_value: value,
            bytes_per_value,
        }
    }

    /// Return a [`CompressedSegmentBuilder`] containing the model fitted by [`Swing`].
    fn select_swing(start_index: usize, swing: Swing) -> CompressedSegmentBuilder {
        let end_index = start_index + swing.len() - 1;
        let bytes_per_value = swing.bytes_per_value();
        let (first_value, last_value) = swing.model();
        let min_value = Value::min(first_value, last_value);
        let max_value = Value::max(first_value, last_value);
        let values = if first_value < last_value {
            vec![]
        } else {
            vec![0]
        };

        CompressedSegmentBuilder {
            model_type_id: SWING_ID,
            start_index,
            end_index,
            min_value,
            max_value,
            values,
            model_last_value: last_value,
            bytes_per_value,
        }
    }
}

/// A compressed segment being built from metadata and a model.
pub(crate) struct CompressedSegmentBuilder {
    /// Id of the model type that created the model in this segment.
    pub model_type_id: u8,
    /// Index of the first data point in the `UncompressedDataBuffer` that this segment represents.
    pub start_index: usize,
    /// Index of the last data point in the `UncompressedDataBuffer` that this segment represents.
    pub end_index: usize,
    /// The segment's minimum value.
    pub min_value: Value,
    /// The segment's maximum value.
    pub max_value: Value,
    /// Data required in addition to `min` and `max` for the model to
    /// reconstruct the values it represents when given a specific timestamp.
    pub values: Vec<u8>,
    /// The last value the model represents.
    pub model_last_value: Value,
    /// The number of bytes per value used by the model.
    pub bytes_per_value: f32,
}

impl CompressedSegmentBuilder {
    /// Create a compressed segment and add it to `compressed_segment_batch_builder`. The encoding
    /// used for the model's parameters may change if residuals are added to the segment as it
    /// changes the metadata stored in the segment. Assumes `uncompressed_timestamps` and
    /// `uncompressed_values` are of equal length and that `residuals_end_index` is the index of a
    /// value in `uncompressed_value` after the last value represented by the model in this segment.
    pub(crate) fn finish(
        mut self,
        univariate_id: u64,
        error_bound: ErrorBound,
        residuals_end_index: usize,
        uncompressed_timestamps: &TimestampArray,
        uncompressed_values: &ValueArray,
        compressed_segment_batch_builder: &mut CompressedSegmentBatchBuilder,
    ) {
        // Assert that the methods assumptions are correct to simplify development.
        debug_assert_eq!(uncompressed_timestamps.len(), uncompressed_values.len());
        debug_assert!(self.end_index <= residuals_end_index);
        debug_assert!(residuals_end_index <= uncompressed_timestamps.len());

        let model_type_id = self.model_type_id;

        // Compress the timestamps for the values stored as the model and if any exist, residuals.
        let start_time = uncompressed_timestamps.value(self.start_index);
        let end_time = uncompressed_timestamps.value(residuals_end_index);
        let timestamps = timestamps::compress_residual_timestamps(
            &uncompressed_timestamps.values()[self.start_index..=residuals_end_index],
        );

        // Compress residual values using Gorilla if any exists.
        let residuals = if self.end_index < residuals_end_index {
            let residuals_start_index = self.end_index + 1;

            let uncompressed_residuals =
                &uncompressed_values.values()[residuals_start_index..=residuals_end_index];

            let (mut residuals, residuals_min_value, residuals_max_value) =
                self.compress_residuals(error_bound, uncompressed_residuals);

            match model_type_id {
                PMC_MEAN_ID => {
                    self.update_values_for_pmc_mean(residuals_min_value, residuals_max_value)
                }
                SWING_ID => self.update_values_for_swing(residuals_min_value, residuals_max_value),
                _ => panic!("Unknown model type."),
            }

            self.min_value = Value::min(self.min_value, residuals_min_value);
            self.max_value = Value::max(self.max_value, residuals_max_value);

            // The length is known to be at most RESIDUAL_VALUES_MAX_LENGTH: u8.
            residuals.push((residuals_end_index - residuals_start_index) as u8 + 1);
            residuals
        } else {
            vec![]
        };

        compressed_segment_batch_builder.append_compressed_segment(
            univariate_id,
            self.model_type_id,
            start_time,
            end_time,
            &timestamps,
            self.min_value,
            self.max_value,
            &self.values,
            &residuals,
            f32::NAN, // TODO: compute and store the actual error.
        )
    }

    /// Compress `uncompressed_residuals` within `error_bound` using [`Gorilla`].
    fn compress_residuals(
        &self,
        error_bound: ErrorBound,
        uncompressed_residuals: &[Value],
    ) -> (Vec<u8>, Value, Value) {
        let mut gorilla = Gorilla::new(error_bound);
        gorilla.compress_values_without_first(uncompressed_residuals, self.model_last_value);
        gorilla.model()
    }

    /// Add information if required for a model of type [`PMCMean`] due to `residuals_min_value` and
    /// `residuals_max_value` overwriting the model's minimum and maximum values in the segment.
    fn update_values_for_pmc_mean(
        &mut self,
        residuals_min_value: Value,
        residuals_max_value: Value,
    ) {
        if self.min_value > residuals_min_value {
            // The models minimum is overwritten so another value must be used.
            if self.max_value >= residuals_max_value {
                // Minimum and maximum is the same for PMC-Mean, so maximum can be used with a flag.
                self.values.push(1);
            } else {
                // Minimum and maximum have been overwritten, so the model's value has to be stored.
                self.values.extend_from_slice(&self.min_value.to_le_bytes());
            }
        }
    }

    /// Decode the mean value stored for a model of type [`PMCMean`].
    pub(crate) fn decode_values_for_pmc_mean(
        min_value: Value,
        max_value: Value,
        values: &[u8],
    ) -> Value {
        // unwrap() is safe as values are encoded by update_values_for_pmc_mean().
        match values.len() {
            0 => min_value,
            1 => max_value,
            _ => Value::from_le_bytes(values.try_into().unwrap()),
        }
    }

    /// Add information if required for a model of type [`Swing`] due to `residuals_min_value` and
    /// `residuals_max_value` overwriting the model's minimum and maximum values in the segment.
    fn update_values_for_swing(&mut self, residuals_min_value: Value, residuals_max_value: Value) {
        if residuals_min_value < self.min_value && self.max_value < residuals_min_value {
            // Minimum and maximum is overwritten so first and last value are stored.
            let mut updated_values = Vec::with_capacity(2 * VALUE_SIZE_IN_BYTES as usize);
            if self.values.is_empty() {
                updated_values.extend_from_slice(&self.min_value.to_le_bytes());
                updated_values.extend_from_slice(&self.max_value.to_le_bytes());
            } else {
                updated_values.extend_from_slice(&self.max_value.to_le_bytes());
                updated_values.extend_from_slice(&self.min_value.to_le_bytes());
            }
            self.values = updated_values;
        } else if residuals_min_value < self.min_value {
            // Minimum is overwritten so a flag is stored for the order and then the models minimum.
            let mut updated_values = Vec::with_capacity(1 + VALUE_SIZE_IN_BYTES as usize);
            if self.values.is_empty() {
                updated_values.push(0);
                updated_values.extend(self.min_value.to_le_bytes());
            } else {
                updated_values.push(1);
                updated_values.extend(self.min_value.to_le_bytes());
            }
            self.values = updated_values;
        } else if self.max_value < residuals_max_value {
            // Maximum is overwritten so a flag is stored for the order and then the models maximum.
            let mut updated_values = Vec::with_capacity(1 + VALUE_SIZE_IN_BYTES as usize);
            if self.values.is_empty() {
                updated_values.push(2);
                updated_values.extend(self.max_value.to_le_bytes());
            } else {
                updated_values.push(3);
                updated_values.extend(self.max_value.to_le_bytes());
            }
            self.values = updated_values;
        }

        if self.min_value > residuals_min_value {}
    }

    /// Decode the slope and intercept stored for a model of type [`Swing`].
    pub(crate) fn decode_values_for_swing(
        min_value: Value,
        max_value: Value,
        values: &[u8],
    ) -> (Value, Value) {
        // unwrap() is safe as values are encoded by select_swing() and update_values_for_swing().
        match values.len() {
            0 => (min_value, max_value),
            1 => (max_value, min_value),
            5 => {
                let value = Value::from_le_bytes(values[1..].try_into().unwrap());
                match values[0] {
                    0 => (value, max_value),
                    1 => (max_value, value),
                    2 => (min_value, value),
                    3 => (value, min_value),
                    _ => panic!("Unknown encoding of swing."),
                }
            }
            8 => {
                let value_size = VALUE_SIZE_IN_BYTES as usize;
                (
                    Value::from_le_bytes(values[0..value_size].try_into().unwrap()),
                    Value::from_le_bytes(values[value_size..2 * value_size].try_into().unwrap()),
                )
            }
            _ => panic!("Unknown encoding of swing."),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_common::types::{TimestampArray, ValueArray};
    use proptest::num;
    use proptest::proptest;

    use crate::compression;
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

    // Tests for CompressedSegmentBuilder.
    #[test]
    fn test_model_attributes_for_pmc_mean() {
        let uncompressed_timestamps = TimestampArray::from(UNCOMPRESSED_TIMESTAMPS.to_vec());
        let uncompressed_values = ValueArray::from(vec![10.0, 10.0, 10.0, 10.0, 10.0]);

        let model = compression::fit_next_model(
            0,
            ErrorBound::try_new(0.0).unwrap(),
            &uncompressed_timestamps,
            &uncompressed_values,
        );

        assert_eq!(PMC_MEAN_ID, model.model_type_id);
        assert_eq!(uncompressed_timestamps.len() - 1, model.end_index);
        assert_eq!(10.0, model.min_value);
        assert_eq!(10.0, model.max_value);
        assert_eq!(0, model.values.len());
    }

    #[test]
    fn test_model_attributes_for_increasing_swing() {
        let uncompressed_timestamps = TimestampArray::from(UNCOMPRESSED_TIMESTAMPS.to_vec());
        let uncompressed_values = ValueArray::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        let model = compression::fit_next_model(
            0,
            ErrorBound::try_new(0.0).unwrap(),
            &uncompressed_timestamps,
            &uncompressed_values,
        );

        assert_eq!(SWING_ID, model.model_type_id);
        assert_eq!(uncompressed_timestamps.len() - 1, model.end_index);
        assert_eq!(10.0, model.min_value);
        assert_eq!(50.0, model.max_value);
        assert_eq!(0, model.values.len());
    }

    #[test]
    fn test_model_attributes_for_decreasing_swing() {
        let uncompressed_timestamps = TimestampArray::from(UNCOMPRESSED_TIMESTAMPS.to_vec());
        let uncompressed_values = ValueArray::from(vec![50.0, 40.0, 30.0, 20.0, 10.0]);
        let model = compression::fit_next_model(
            0,
            ErrorBound::try_new(0.0).unwrap(),
            &uncompressed_timestamps,
            &uncompressed_values,
        );

        assert_eq!(SWING_ID, model.model_type_id);
        assert_eq!(uncompressed_timestamps.len() - 1, model.end_index);
        assert_eq!(10.0, model.min_value);
        assert_eq!(50.0, model.max_value);
        assert_eq!(1, model.values.len());
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

        let model = compression::fit_next_model(
            0,
            ErrorBound::try_new(10.0).unwrap(),
            &timestamps,
            &value_array,
        );

        assert_eq!(model.model_type_id, PMC_MEAN_ID);
    }
}
