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

use std::mem;

use modelardb_common::types::{
    Timestamp, TimestampBuilder, UnivariateId, UnivariateIdBuilder, Value, ValueBuilder,
};

use crate::types::{CompressedSegmentBuilder, ErrorBound};

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
        SWING_ID => swing::sum(start_time, end_time, timestamps, min_value, max_value),
        // TODO: take residuals stored as part of the segment into account when refactoring optimizer.
        GORILLA_ID => gorilla::sum(start_time, end_time, timestamps, values, None),
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
            CompressedSegmentBuilder::decode_values_for_pmc_mean(min_value, max_value, values),
            univariate_id_builder,
            model_timestamps,
            value_builder,
        ),
        SWING_ID => {
            let (first_value, last_value) =
                CompressedSegmentBuilder::decode_values_for_swing(min_value, max_value, values);

            swing::grid(
                univariate_id,
                start_time,
                end_time,
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
}
