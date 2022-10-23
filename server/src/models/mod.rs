/* Copyright 2021 The ModelarDB Contributors
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

//! Implementation of the model types used for compressing time series segments
//! as models and functions for efficiently computing aggregates from models of
//! each type. The module itself contains general functionality used by the
//! model types.

// TODO: Test irregular and regular time series in models/* and compression.rs.

pub mod bits;
pub mod gorilla;
pub mod pmcmean;
pub mod swing;
pub mod timestamps;

use std::cmp::{Ordering, PartialOrd};
use std::mem;

use crate::errors::ModelarDBError;
use crate::models::{gorilla::Gorilla, pmcmean::PMCMean, swing::Swing};
use crate::types::{
    TimeSeriesId, TimeSeriesIdBuilder, Timestamp, TimestampBuilder, Value, ValueArray, ValueBuilder,
};

/// Unique ids for each model type. Constant values are used instead of an enum
/// so the stored model type ids can be used in match expressions without being
/// converted to an enum first. Zero and one are not used for compatibility with
/// the legacy JVM-based version of [ModelarDB].
///
/// [ModelarDB]: https://github.com/ModelarData/ModelarDB
pub const PMC_MEAN_ID: u8 = 2;
pub const SWING_ID: u8 = 3;
pub const GORILLA_ID: u8 = 4;

/// Size of [`Value`] in bytes.
const VALUE_SIZE_IN_BYTES: u8 = mem::size_of::<Value>() as u8;

/// Size of [`Value`] in bits.
const VALUE_SIZE_IN_BITS: u8 = 8 * VALUE_SIZE_IN_BYTES as u8;

/// General error bound that is guaranteed to not be negative, infinite, or NAN.
/// For [`PMCMean`] and [`Swing`] the error bound is interpreted as a relative
/// per value error bound in percentage, while [`Gorilla`] uses lossless
/// compression.
#[derive(Debug, Copy, Clone)]
pub struct ErrorBound(f32);

impl ErrorBound {
    /// Return [`ErrorBound`] if `error_bound` is a positive finite value,
    /// otherwise [`CompressionError`](ModelarDBError::CompressionError).
    pub fn try_new(error_bound: f32) -> Result<Self, ModelarDBError> {
        if error_bound < 0.0 || error_bound.is_infinite() || error_bound.is_nan() {
            Err(ModelarDBError::CompressionError(
                "Error bound cannot be negative, infinite, or NAN.".to_owned(),
            ))
        } else {
            Ok(Self(error_bound))
        }
    }

    /// Return the memory representation of the error bound as a byte array in
    /// little-endian byte order.
    pub fn to_le_bytes(&self) -> [u8; 4] {
        self.0.to_le_bytes()
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

/// Model that uses the fewest number of bytes per value.
pub struct SelectedModel {
    /// Id of the model type that created this model.
    pub model_type_id: u8,
    /// Index of the last data point in the `UncompressedSegment` that the
    /// selected model represents.
    pub end_index: usize,
    /// The selected model's minimum value.
    pub min_value: Value,
    /// The selected model's maximum value.
    pub max_value: Value,
    /// Data required in addition to `min` and `max` for the model to
    /// reconstruct the values it represents when given a specific timestamp.
    pub values: Vec<u8>,
}

impl SelectedModel {
    /// Select the model that uses the fewest number of bytes per value.
    pub fn new(
        start_index: usize,
        pmc_mean: PMCMean,
        swing: Swing,
        gorilla: Gorilla,
        uncompressed_values: &ValueArray,
    ) -> Self {
        let bytes_per_value = [
            (PMC_MEAN_ID, pmc_mean.get_bytes_per_value()),
            (SWING_ID, swing.get_bytes_per_value()),
            (GORILLA_ID, gorilla.get_bytes_per_value()),
        ];

        // unwrap() cannot fail as the array is not empty and there are no NaN.
        let selected_model_type_id = bytes_per_value
            .iter()
            .min_by(|x, y| f32::partial_cmp(&x.1, &y.1).unwrap())
            .unwrap()
            .0;

        match selected_model_type_id {
            PMC_MEAN_ID => Self::select_pmc_mean(start_index, pmc_mean),
            SWING_ID => Self::select_swing(start_index, swing),
            GORILLA_ID => Self::select_gorilla(start_index, gorilla, uncompressed_values),
            _ => panic!("Unknown model type."),
        }
    }

    /// Create a [`SelectedModel`] from `pmc_mean`.
    fn select_pmc_mean(start_index: usize, pmc_mean: PMCMean) -> Self {
        let value = pmc_mean.get_model();
        let end_index = start_index + pmc_mean.get_length() - 1;

        Self {
            model_type_id: PMC_MEAN_ID,
            end_index,
            min_value: value,
            max_value: value,
            values: vec![],
        }
    }

    /// Create a [`SelectedModel`] from `swing`.
    fn select_swing(start_index: usize, swing: Swing) -> Self {
        let (start_value, end_value) = swing.get_model();
        let end_index = start_index + swing.get_length() - 1;
        let min_value = Value::min(start_value, end_value);
        let max_value = Value::max(start_value, end_value);
        let values = vec!((start_value < end_value) as u8);

        Self {
            model_type_id: SWING_ID,
            end_index,
            min_value,
            max_value,
            values,
        }
    }

    /// Create a [`SelectedModel`] from `gorilla`.
    fn select_gorilla(
        start_index: usize,
        gorilla: Gorilla,
        uncompressed_values: &ValueArray,
    ) -> Self {
        let end_index = start_index + gorilla.get_length() - 1;
        let uncompressed_values = &uncompressed_values.values()[start_index..end_index];
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
        let values = gorilla.get_compressed_values();

        Self {
            model_type_id: GORILLA_ID,
            end_index,
            min_value,
            max_value,
            values,
        }
    }
}

/// Compute the number of data points in a time series segment.
pub fn length(start_time: Timestamp, end_time: Timestamp, timestamps: &[u8]) -> usize {
    if timestamps.is_empty() && start_time == end_time {
        // Timestamps are assumed to be unique so the segment has one timestamp.
        1
    } else if timestamps.is_empty() {
        // Timestamps are assumed to be unique so the segment has two timestamp.
        2
    } else if timestamps::are_compressed_timestamps_regular(timestamps) {
        // The flag bit is zero, so only the segment's length is stored as an
        // integer with all the prefix zeros stripped from the integer.
        let mut bytes_to_decode = [0; 8];
        let bytes_to_decode_len = bytes_to_decode.len();
        bytes_to_decode[bytes_to_decode_len - timestamps.len()..].copy_from_slice(timestamps);
        usize::from_be_bytes(bytes_to_decode)
    } else {
        // The flag bit is one, so the timestamps are compressed as
        // delta-of-deltas stored using a variable length binary encoding.
        let mut timestamp_builder = TimestampBuilder::new();
        timestamps::decompress_all_timestamps(
            start_time,
            end_time,
            timestamps,
            &mut timestamp_builder,
        );
        timestamp_builder.values_slice().len()
    }
}

/// Compute the sum of the values for a time series segment whose values are
/// represented by a model.
pub fn sum(
    model_type_id: u8,
    start_time: Timestamp,
    end_time: Timestamp,
    timestamps: &[u8],
    min_value: Value,
    max_value: Value,
    values: &[u8],
) -> Value {
    match model_type_id as u8 {
        PMC_MEAN_ID => pmcmean::sum(start_time, end_time, timestamps, min_value),
        SWING_ID => swing::sum(start_time, end_time, timestamps, min_value, max_value, values),
        GORILLA_ID => gorilla::sum(start_time, end_time, timestamps, values),
        _ => panic!("Unknown model type."),
    }
}

/// Reconstruct the data points for a time series segment whose values are
/// represented by a model. Each data point is split into its three components
/// and appended to `time_series_ids`, `timestamps`, and `values`.
pub fn grid(
    time_series_id: TimeSeriesId,
    model_type_id: u8,
    timestamps: &[u8],
    start_time: Timestamp,
    end_time: Timestamp,
    values: &[u8],
    min_value: Value,
    max_value: Value,
    time_series_id_builder: &mut TimeSeriesIdBuilder,
    timestamp_builder: &mut TimestampBuilder,
    value_builder: &mut ValueBuilder,
) {
    timestamps::decompress_all_timestamps(start_time, end_time, timestamps, timestamp_builder);
    let new_timestamps = &timestamp_builder.values_slice()[value_builder.values_slice().len()..];

    match model_type_id as u8 {
        PMC_MEAN_ID => pmcmean::grid(
            time_series_id,
            min_value, // For PMC-Mean, min and max is the same value.
            time_series_id_builder,
            new_timestamps,
            value_builder,
        ),
        SWING_ID => swing::grid(
            time_series_id,
            start_time,
            end_time,
            min_value,
            max_value,
            values,
            time_series_id_builder,
            new_timestamps,
            value_builder,
        ),
        GORILLA_ID => gorilla::grid(
            time_series_id,
            values,
            time_series_id_builder,
            new_timestamps,
            value_builder,
        ),
        _ => panic!("Unknown model type."),
    }
}

/// Returns true if `v1` and `v2` are equivalent or both values are NAN.
fn equal_or_nan(v1: f64, v2: f64) -> bool {
    v1 == v2 || (v1.is_nan() && v2.is_nan())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::from_slice::FromSlice;
    use proptest::num;
    use proptest::{prop_assert, prop_assume, proptest};

    use crate::types::TimestampArray;

    const UNCOMPRESSED_TIMESTAMPS: &[Timestamp] = &[100, 200, 300, 400, 500];

    // Tests for ErrorBound.
    proptest! {
    #[test]
    fn test_error_bound_can_be_positive(error_bound in num::f32::POSITIVE) {
        assert!(ErrorBound::try_new(error_bound).is_ok())
    }

    #[test]
    fn test_error_bound_cannot_be_negative(error_bound in num::f32::NEGATIVE) {
        assert!(ErrorBound::try_new(error_bound).is_err())
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
        let uncompressed_timestamps = TimestampArray::from_slice(UNCOMPRESSED_TIMESTAMPS);
        let uncompressed_values = ValueArray::from(vec![10.0, 10.0, 10.0, 10.0, 10.0]);

        let selected_model = create_selected_model(&uncompressed_timestamps, &uncompressed_values);

        assert_eq!(PMC_MEAN_ID, selected_model.model_type_id);
        assert_eq!(uncompressed_timestamps.len() - 1, selected_model.end_index);
        assert_eq!(10.0, selected_model.min_value);
        assert_eq!(10.0, selected_model.max_value);
        assert_eq!(0, selected_model.values.len());
    }

    #[test]
    fn test_model_selected_model_attributes_for_increasing_swing() {
        let uncompressed_timestamps = TimestampArray::from_slice(UNCOMPRESSED_TIMESTAMPS);
        let uncompressed_values = ValueArray::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        let selected_model = create_selected_model(&uncompressed_timestamps, &uncompressed_values);

        assert_eq!(SWING_ID, selected_model.model_type_id);
        assert_eq!(uncompressed_timestamps.len() - 1, selected_model.end_index);
        assert_eq!(10.0, selected_model.min_value);
        assert_eq!(50.0, selected_model.max_value);
        assert_eq!(1, selected_model.values.len());
    }

    #[test]
    fn test_model_selected_model_attributes_for_decreasing_swing() {
        let uncompressed_timestamps = TimestampArray::from_slice(UNCOMPRESSED_TIMESTAMPS);
        let uncompressed_values = ValueArray::from(vec![50.0, 40.0, 30.0, 20.0, 10.0]);
        let selected_model = create_selected_model(&uncompressed_timestamps, &uncompressed_values);

        assert_eq!(SWING_ID, selected_model.model_type_id);
        assert_eq!(uncompressed_timestamps.len() - 1, selected_model.end_index);
        assert_eq!(10.0, selected_model.min_value);
        assert_eq!(50.0, selected_model.max_value);
        assert_eq!(1, selected_model.values.len());
    }

    #[test]
    fn test_model_selected_model_attributes_for_gorilla() {
        let uncompressed_timestamps = TimestampArray::from_slice(UNCOMPRESSED_TIMESTAMPS);
        let uncompressed_values = ValueArray::from(vec![37.0, 73.0, 37.0, 73.0, 37.0]);
        let selected_model = create_selected_model(&uncompressed_timestamps, &uncompressed_values);

        assert_eq!(GORILLA_ID, selected_model.model_type_id);
        assert_eq!(uncompressed_timestamps.len() - 1, selected_model.end_index);
        assert_eq!(37.0, selected_model.min_value);
        assert_eq!(73.0, selected_model.max_value);
        assert_eq!(10, selected_model.values.len());
    }

    fn create_selected_model(
        uncompressed_timestamps: &TimestampArray,
        uncompressed_values: &ValueArray,
    ) -> SelectedModel {
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let mut pmc_mean = PMCMean::new(error_bound);
        let mut swing = Swing::new(error_bound);
        let mut gorilla = Gorilla::new();

        let mut pmc_mean_could_fit_all = true;
        let mut swing_could_fit_all = true;
        for index in 0..uncompressed_timestamps.len() {
            let timestamp = uncompressed_timestamps.value(index);
            let value = uncompressed_values.value(index);

            pmc_mean_could_fit_all = pmc_mean_could_fit_all && pmc_mean.fit_value(value);
            swing_could_fit_all = swing_could_fit_all && swing.fit_data_point(timestamp, value);
            gorilla.compress_value(value);
        }
        SelectedModel::new(0, pmc_mean, swing, gorilla, &uncompressed_values)
    }

    // Tests for length().
    #[test]
    fn test_length_of_segment_with_one_data_point() {
        assert_eq!(1, length(1658671178037, 1658671178037, &[]));
    }

    #[test]
    fn test_length_of_segment_with_ten_data_points() {
        assert_eq!(10, length(1658671178037, 1658671187047, &[10]));
    }

    // Tests for equal_or_nan().
    proptest! {
    #[test]
    fn test_equal_or_nan_equal(value in num::f64::ANY) {
        assert!(equal_or_nan(value, value));
    }

    #[test]
    fn test_equal_or_nan_not_equal(v1 in num::f64::ANY, v2 in num::f64::ANY) {
        prop_assume!(v1 != v2 && !v1.is_nan() && !v2.is_nan());
        prop_assert!(!equal_or_nan(v1, v2));
    }
    }
}
