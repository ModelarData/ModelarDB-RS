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

//! Implementation of the model types used for compressing sequences of values as models and
//! functions for efficiently computing aggregates from models of each type. The module itself
//! contains general functionality used by the model types.

pub mod bits;
pub mod gorilla;
pub mod pmc_mean;
pub mod swing;
pub mod timestamps;

use std::mem;

use arrow::array::ArrayBuilder;
use modelardb_common::types::{
    ErrorBound, Timestamp, TimestampBuilder, UnivariateId, UnivariateIdBuilder, Value, ValueBuilder,
};

use crate::types::CompressedSegmentBuilder;

/// Unique ids for each model type. Constant values are used instead of an enum so the stored model
/// type ids can be used in match expressions without being converted to an enum first. Any changes
/// to the ids must be reflected in all statements matching on them and in [`GridStreamMetrics`].
pub const PMC_MEAN_ID: u8 = 0;
pub const SWING_ID: u8 = 1;
pub const GORILLA_ID: u8 = 2;

/// Size of [`Value`] in bytes.
pub(super) const VALUE_SIZE_IN_BYTES: u8 = mem::size_of::<Value>() as u8;

/// Size of [`Value`] in bits.
pub(super) const VALUE_SIZE_IN_BITS: u8 = 8 * VALUE_SIZE_IN_BYTES;

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

/// Returns true if `v1` and `v2` are equivalent or both values are NAN.
fn equal_or_nan(v1: f64, v2: f64) -> bool {
    v1 == v2 || (v1.is_nan() && v2.is_nan())
}

/// Compute the number of data points in a compressed segment.
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

/// Return [`true`] if the models from two segments can be merged, otherwise [`false`].
pub(crate) fn can_be_merged(
    segment_one_model_type_id: u8,
    segment_one_start_time: Timestamp,
    segment_one_end_time: Timestamp,
    segment_one_min_value: Value,
    segment_one_max_value: Value,
    segment_one_values: &[u8],
    segment_two_model_type_id: u8,
    segment_two_start_time: Timestamp,
    segment_two_end_time: Timestamp,
    segment_two_min_value: Value,
    segment_two_max_value: Value,
    segment_two_values: &[u8],
) -> bool {
    if segment_one_model_type_id != segment_two_model_type_id {
        return false;
    }

    match segment_one_model_type_id {
        PMC_MEAN_ID => {
            let segment_one_value = CompressedSegmentBuilder::decode_values_for_pmc_mean(
                segment_one_min_value,
                segment_one_max_value,
                segment_one_values,
            );

            let segment_two_value = CompressedSegmentBuilder::decode_values_for_pmc_mean(
                segment_two_min_value,
                segment_two_max_value,
                segment_two_values,
            );

            segment_one_value == segment_two_value
        }
        SWING_ID => {
            let (segment_one_first_value, segment_one_last_value) =
                CompressedSegmentBuilder::decode_values_for_swing(
                    segment_one_min_value,
                    segment_one_max_value,
                    segment_one_values,
                );

            let (segment_two_first_value, segment_two_last_value) =
                CompressedSegmentBuilder::decode_values_for_swing(
                    segment_two_min_value,
                    segment_two_max_value,
                    segment_two_values,
                );

            swing::can_be_merged(
                segment_one_start_time,
                segment_one_end_time,
                segment_one_first_value as f64,
                segment_one_last_value as f64,
                segment_two_start_time,
                segment_two_end_time,
                segment_two_first_value as f64,
                segment_two_last_value as f64,
            )
        }
        GORILLA_ID => false,
        _ => {
            panic!("Unknown model type.");
        }
    }
}

/// Encode the information required for a merged model where the `residuals_min_value` and/or
/// `residuals_max_value` overwrite the model's `min_value` and/or `max_value` in the segment.
pub(crate) fn merge(
    model_type_id: u8,
    residuals_min_value: Value,
    residuals_max_value: Value,
    values: &[u8],
) -> Vec<u8> {
    match model_type_id {
        PMC_MEAN_ID => {
            let value = CompressedSegmentBuilder::decode_values_for_pmc_mean(
                residuals_min_value,
                residuals_max_value,
                values,
            );

            CompressedSegmentBuilder::encode_values_for_pmc_mean(
                value,
                value,
                residuals_min_value,
                residuals_max_value,
            )
        }
        SWING_ID => {
            let (first_value, last_value) = CompressedSegmentBuilder::decode_values_for_swing(
                residuals_min_value,
                residuals_max_value,
                values,
            );

            let (min_value, max_value, min_value_is_first) = if first_value <= last_value {
                (first_value, last_value, true)
            } else {
                (last_value, first_value, false)
            };

            CompressedSegmentBuilder::encode_values_for_swing(
                min_value,
                max_value,
                min_value_is_first,
                residuals_min_value,
                residuals_max_value,
            )
        }
        _ => {
            panic!("Unknown model type.");
        }
    }
}

/// Compute the sum of the values for a compressed segment whose values are represented by a model
/// and residuals.
pub fn sum(
    model_type_id: u8,
    start_time: Timestamp,
    end_time: Timestamp,
    timestamps: &[u8],
    min_value: Value,
    max_value: Value,
    values: &[u8],
    residuals: &[u8],
) -> Value {
    // Extract the number of residuals stored.
    let residuals_length = residuals_length(residuals);

    let model_length = len(start_time, end_time, timestamps) - residuals_length;

    // Computes the sum from the model.
    let (model_last_value, model_sum) = match model_type_id {
        PMC_MEAN_ID => {
            let value =
                CompressedSegmentBuilder::decode_values_for_pmc_mean(min_value, max_value, values);
            (value, pmc_mean::sum(model_length, value))
        }
        SWING_ID => {
            let (first_value, last_value) =
                CompressedSegmentBuilder::decode_values_for_swing(min_value, max_value, values);
            (
                last_value,
                swing::sum(
                    start_time,
                    end_time,
                    timestamps,
                    first_value,
                    last_value,
                    residuals_length,
                ),
            )
        }
        GORILLA_ID => (
            f32::NAN, // A segment with values compressed by Gorilla never has residuals.
            gorilla::sum(model_length, values, None),
        ),
        _ => panic!("Unknown model type."),
    };

    // Compute the sum from the residuals.
    if residuals.is_empty() {
        model_sum
    } else {
        let residuals_sum = gorilla::sum(
            residuals_length,
            &residuals[..residuals.len() - 1],
            Some(model_last_value),
        );
        model_sum + residuals_sum
    }
}

/// Reconstruct the data points for a compressed segment whose values are represented by a model and
/// residuals. Each data point is split into its three components and appended to `univariate_ids`,
/// `timestamps`, and `values`.
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
    // Decompress the timestamps.
    let (model_timestamps, residuals_timestamps) =
        decompress_all_timestamps_and_split_into_models_and_residuals(
            start_time,
            end_time,
            timestamps,
            residuals,
            timestamp_builder,
        );

    // Reconstruct the values from the model.
    match model_type_id {
        PMC_MEAN_ID => pmc_mean::grid(
            univariate_id,
            CompressedSegmentBuilder::decode_values_for_pmc_mean(min_value, max_value, values),
            univariate_id_builder,
            model_timestamps,
            value_builder,
        ),
        SWING_ID => {
            let (first_value, last_value) =
                CompressedSegmentBuilder::decode_values_for_swing(min_value, max_value, values);

            // unwrap() is safe as the model is guaranteed to represent at least one value.
            let model_end_time = *model_timestamps.last().unwrap();

            swing::grid(
                univariate_id,
                start_time,
                model_end_time,
                first_value,
                last_value,
                univariate_id_builder,
                model_timestamps,
                value_builder,
            )
        }
        GORILLA_ID => gorilla::grid(
            univariate_id,
            values,
            univariate_id_builder,
            model_timestamps,
            value_builder,
            None,
        ),
        _ => panic!("Unknown model type."),
    }

    // Reconstruct the values from the residuals.
    if !residuals.is_empty() {
        let model_last_value = value_builder.values_slice()[value_builder.len() - 1];

        gorilla::grid(
            univariate_id,
            &residuals[..residuals.len() - 1],
            univariate_id_builder,
            residuals_timestamps,
            value_builder,
            Some(model_last_value),
        );
    }
}

/// Decompress the timestamps stored as `start_time`, `end_time`, and `timestamps`, add them to
/// `timestamp_builder`, and return slices to the model's timestamps and the residual's timestamps.
fn decompress_all_timestamps_and_split_into_models_and_residuals<'a>(
    start_time: Timestamp,
    end_time: Timestamp,
    timestamps: &'a [u8],
    residuals: &'a [u8],
    timestamp_builder: &'a mut TimestampBuilder,
) -> (&'a [Timestamp], &'a [Timestamp]) {
    // Extract the number of residuals stored.
    let residuals_length = residuals_length(residuals);

    let model_timestamps_start_index = timestamp_builder.values_slice().len();
    timestamps::decompress_all_timestamps(start_time, end_time, timestamps, timestamp_builder);
    let model_timestamps_end_index = timestamp_builder.values_slice().len() - residuals_length;

    let model_timestamps =
        &timestamp_builder.values_slice()[model_timestamps_start_index..model_timestamps_end_index];
    let residuals_timestamps = &timestamp_builder.values_slice()[model_timestamps_end_index..];

    (model_timestamps, residuals_timestamps)
}

/// Return the number of residual values stored in the segment.
fn residuals_length(residuals: &[u8]) -> usize {
    if residuals.is_empty() {
        0
    } else {
        // The number of residuals are stored as the last byte.
        residuals[residuals.len() - 1] as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::num;
    use proptest::num::f32 as ProptestValue;
    use proptest::{prop_assert, prop_assume, proptest};

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

    // Tests for decompress_all_timestamps_and_split_into_models_and_residuals().
    #[test]
    fn test_decompress_all_timestamps_and_split_into_models_and_residuals_no_residuals() {
        let mut timestamp_builder = TimestampBuilder::new();

        let (model_timestamps, residuals_timestamps) =
            decompress_all_timestamps_and_split_into_models_and_residuals(
                100,
                500,
                &[5],
                &[],
                &mut timestamp_builder,
            );

        assert_eq!(model_timestamps, &[100, 200, 300, 400, 500]);
        assert_eq!(residuals_timestamps, &[] as &[Timestamp]);
    }

    #[test]
    fn test_decompress_all_timestamps_and_split_into_models_and_residuals_with_residuals() {
        let mut timestamp_builder = TimestampBuilder::new();

        let (model_timestamps, residuals_timestamps) =
            decompress_all_timestamps_and_split_into_models_and_residuals(
                100,
                500,
                &[5],
                &[2],
                &mut timestamp_builder,
            );

        assert_eq!(model_timestamps, &[100, 200, 300]);
        assert_eq!(residuals_timestamps, &[400, 500]);
    }

    // Tests for residuals_length().
    #[test]
    fn test_empty_residuals_length() {
        assert_eq!(residuals_length(&[]), 0)
    }

    #[test]
    fn test_residuals_length() {
        assert_eq!(residuals_length(&[37, 73, 2]), 2)
    }
}
