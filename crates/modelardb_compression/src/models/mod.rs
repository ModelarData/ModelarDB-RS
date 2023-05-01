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

pub mod bits;
pub mod gorilla;
pub mod pmc_mean;
pub mod swing;
pub mod timestamps;

use std::cmp::{Ordering, PartialOrd};
use std::mem;

use modelardb_common::errors::ModelarDbError;
use modelardb_common::types::{
    Timestamp, TimestampBuilder, UnivariateId, UnivariateIdBuilder, Value, ValueArray, ValueBuilder,
};

use crate::models::{gorilla::Gorilla, pmc_mean::PMCMean, swing::Swing};

/// Unique ids for each model type. Constant values are used instead of an enum
/// so the stored model type ids can be used in match expressions without being
/// converted to an enum first.
pub const PMC_MEAN_ID: u8 = 0;
pub const SWING_ID: u8 = 1;
pub const GORILLA_ID: u8 = 2;

/// Size of [`Value`] in bytes.
pub(super) const VALUE_SIZE_IN_BYTES: u8 = mem::size_of::<Value>() as u8;

/// Size of [`Value`] in bits.
pub(super) const VALUE_SIZE_IN_BITS: u8 = 8 * VALUE_SIZE_IN_BYTES;

/// Error bound in percentage that is guaranteed to be from 0.0% to 100.0%. For both [`PMCMean`],
/// [`Swing`], and [`Gorilla`] the error bound is interpreted as a relative per value error bound.
#[derive(Debug, Copy, Clone)]
pub struct ErrorBound(f32);

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

/// Determine if `approximate_value` is within `error_bound` of `real_value`.
pub fn is_value_within_error_bound(
    error_bound: ErrorBound,
    real_value: Value,
    approximate_value: Value,
) -> bool {
    // Needed because result becomes NAN and approximate_value is rejected
    // if approximate_value and real_value are zero, and because NAN != NAN.
    if equal_or_nan(real_value as f64, approximate_value as f64) {
        true
    } else {
        let difference = real_value - approximate_value;
        let result = Value::abs(difference / real_value);
        (result * 100.0) <= error_bound
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

/// Compress the values from `start_index` to `end_index` in `uncompressed_values` using
/// [`Gorilla`].
pub fn compress_residual_value_range(
    error_bound: ErrorBound,
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
    for value in uncompressed_values {
        gorilla.compress_value(*value)
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

/// Compute the number of data points in a time series segment.
pub fn len(start_time: Timestamp, end_time: Timestamp, timestamps: &[u8]) -> usize {
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
    match model_type_id {
        PMC_MEAN_ID => pmc_mean::sum(start_time, end_time, timestamps, min_value),
        SWING_ID => swing::sum(
            start_time, end_time, timestamps, min_value, max_value, values,
        ),
        GORILLA_ID => gorilla::sum(start_time, end_time, timestamps, values),
        _ => panic!("Unknown model type."),
    }
}

/// Reconstruct the data points for a time series segment whose values are represented by a model
/// and residuals. Each data point is split into its three components and appended to
/// `univariate_ids`, `timestamps`, and `values`.
#[allow(clippy::too_many_arguments)]
pub fn grid(
    univariate_id: UnivariateId,
    model_type_id: u8,
    start_time: Timestamp,
    end_time: Timestamp,
    timestamps: &[u8],
    min_value: Value,
    max_value: Value,
    values: &[u8],
    residuals: &[u8],
    univariate_id_builder: &mut UnivariateIdBuilder,
    timestamp_builder: &mut TimestampBuilder,
    value_builder: &mut ValueBuilder,
) {
    // Extract the number of residuals stored.
    let residuals_length = if residuals.is_empty() {
        0
    } else {
        // The number of residuals are stored as the last byte.
        residuals[residuals.len() - 1]
    };

    // Decompress all of timestamps.
    let model_timestamps_start = timestamp_builder.values_slice().len();
    timestamps::decompress_all_timestamps(start_time, end_time, timestamps, timestamp_builder);
    let model_timestamps_end = timestamp_builder.values_slice().len() - residuals_length as usize;
    let model_timestamps =
        &timestamp_builder.values_slice()[model_timestamps_start..model_timestamps_end];

    // Reconstruct the values from the model.
    match model_type_id {
        PMC_MEAN_ID => pmc_mean::grid(
            univariate_id,
            min_value, // For PMC-Mean, min and max is the same value.
            univariate_id_builder,
            model_timestamps,
            value_builder,
        ),
        SWING_ID => swing::grid(
            univariate_id,
            start_time,
            end_time,
            min_value,
            max_value,
            values,
            univariate_id_builder,
            model_timestamps,
            value_builder,
        ),
        GORILLA_ID => gorilla::grid(
            univariate_id,
            values,
            univariate_id_builder,
            model_timestamps,
            value_builder,
        ),
        _ => panic!("Unknown model type."),
    }

    // Reconstruct the values from the residuals.
    if residuals_length > 0 {
        gorilla::grid(
            univariate_id,
            &residuals[..residuals.len() - 1],
            univariate_id_builder,
            &timestamp_builder.values_slice()[model_timestamps_end..],
            value_builder,
        );
    }
}

/// Returns true if `v1` and `v2` are equivalent or both values are NAN.
fn equal_or_nan(v1: f64, v2: f64) -> bool {
    v1 == v2 || (v1.is_nan() && v2.is_nan())
}

#[cfg(test)]
mod tests {
    use super::*;

    use compression_test_util::StructureOfValues;
    use modelardb_common::types::TimestampArray;
    use proptest::num;
    use proptest::num::f32 as ProptestValue;
    use proptest::{prop_assert, prop_assume, proptest};

    use crate::test_util as compression_test_util;

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

    // Tests for is_value_within_error_bound().
    proptest! {
    #[test]
    fn test_same_value_is_always_within_error_bound(value in ProptestValue::ANY) {
        prop_assert!(is_value_within_error_bound(ErrorBound::try_new(0.0).unwrap(), value, value));
    }

    #[test]
    fn test_other_value_is_never_within_error_bound_of_positive_infinity(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new(100.0).unwrap(), Value::INFINITY, value));
    }

    #[test]
    fn test_other_value_is_never_within_error_bound_of_negative_infinity(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new(100.0).unwrap(), Value::NEG_INFINITY, value));
    }

    #[test]
    fn test_other_value_is_never_within_error_bound_of_nan(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new(100.0).unwrap(), Value::NAN, value));
    }

    #[test]
    fn test_positive_infinity_is_never_within_error_bound_of_other_value(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new(100.0).unwrap(), value, Value::INFINITY));
    }

    #[test]
    fn test_negative_infinity_is_never_within_error_bound_of_other_value(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new(100.0).unwrap(), value, Value::NEG_INFINITY));
    }

    #[test]
    fn test_nan_is_never_within_error_bound_of_other_value(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new(100.0).unwrap(), value, Value::NAN));
    }
    }

    #[test]
    fn test_different_value_is_within_non_zero_error_bound() {
        assert!(is_value_within_error_bound(
            ErrorBound::try_new(10.0).unwrap(),
            10.0,
            11.0
        ));
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
        let values: Vec<f32> = compression_test_util::generate_values(
            &timestamps,
            StructureOfValues::Constant,
            None,
            None,
        )
        .into_iter()
        .chain(compression_test_util::generate_values(
            &timestamps,
            StructureOfValues::Random,
            Some(0.0),
            Some(100.0),
        ))
        .collect();
        let timestamps = TimestampArray::from_iter_values(
            compression_test_util::generate_timestamps(values.len(), false),
        );
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

    // Tests for compress_residual_value_range().
    #[test]
    fn test_compress_all_residual_value_range() {
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let uncompressed_values = ValueArray::from(vec![73.0, 37.0, 37.0, 37.0, 73.0]);
        let selected_model = compress_residual_value_range(
            error_bound,
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
        let selected_model = compress_residual_value_range(error_bound, 1, 3, &uncompressed_values);

        assert_eq!(GORILLA_ID, selected_model.model_type_id);
        assert_eq!(3, selected_model.end_index);
        assert_eq!(37.0, selected_model.min_value);
        assert_eq!(37.0, selected_model.max_value);
        assert_eq!(5, selected_model.values.len());
    }

    // Tests for len().
    #[test]
    fn test_len_of_segment_with_one_data_point() {
        assert_eq!(1, len(1658671178037, 1658671178037, &[]));
    }

    #[test]
    fn test_len_of_segment_with_ten_data_points() {
        assert_eq!(10, len(1658671178037, 1658671187047, &[10]));
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
