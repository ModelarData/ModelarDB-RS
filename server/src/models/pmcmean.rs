/* Copyright 2021 The MiniModelarDB Contributors
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

//! Implementation of the PMC-Mean model type from the [Poor Man’s Compression
//! paper] and efficient computation of aggregates for models of type PMC-Mean
//! as described in the [ModelarDB paper].
//!
//! [Poor Man’s Compression paper]: https://ieeexplore.ieee.org/document/1260811
//! [ModelarDB paper]: https://dl.acm.org/doi/abs/10.14778/3236187.3236215

use crate::types::{
    TimeSeriesId, TimeSeriesIdBuilder, Timestamp, TimestampBuilder, Value, ValueBuilder,
};

/// The state the PMC-Mean model type needs while fitting a model to a time
/// series segment.
struct PMCMean {
    /// Maximum relative error for the value of each data point.
    error_bound: Value,
    /// Minimum value in the segment the current model is fitted to.
    min_value: Value,
    /// Maximum value in the segment the current model is fitted to.
    max_value: Value,
    /// The sum of the values in the segment the current model is fitted to.
    sum_of_values: f64,
    /// The number of data points the current model has been fitted to.
    length: u32,
}

impl PMCMean {
    fn try_new(error_bound: Value) -> Result<Self, String> {
        if error_bound < 0.0 || error_bound.is_infinite() || error_bound.is_nan() {
            Err("The error bound cannot be negative, infinite, or NaN".to_string())
        } else {
            Ok(Self {
                error_bound,
                min_value: Value::NAN,
                max_value: Value::NAN,
                sum_of_values: 0.0,
                length: 0,
            })
        }
    }

    /// Attempt to update the current model of type PMC-Mean to also represent
    /// `value`. Returns `true` if the model can also represent `value`,
    /// otherwise `false`.
    fn fit_value(&mut self, value: Value) -> bool {
        let next_min_value = Value::min(self.min_value, value);
        let next_max_value = Value::max(self.max_value, value);
        let next_sum_of_values = self.sum_of_values + value as f64;
        let next_length = self.length + 1;
        let average = (next_sum_of_values / next_length as f64) as Value;
        if self.is_value_within_error_bound(next_min_value, average)
            && self.is_value_within_error_bound(next_max_value, average)
        {
            self.min_value = next_min_value;
            self.max_value = next_max_value;
            self.sum_of_values = next_sum_of_values;
            self.length = next_length;
            true
        } else {
            false
        }
    }

    /// Return the current model. For a model of type PMC-Mean, its coefficient
    /// is the average value of the time series segment the model represents.
    fn get_model(&self) -> Value {
        (self.sum_of_values / self.length as f64) as Value
    }

    /// Determine if `approximate_value` is within the relative
    /// `self.error_bound` of `real_value`.
    fn is_value_within_error_bound(&self, real_value: Value, approximate_value: Value) -> bool {
        // Needed because result becomes NAN and approximate_value is rejected
        // if approximate_value and real_value are zero, and because NAN != NAN.
        if equal_or_nan(real_value, approximate_value) {
            true
        } else {
            let difference = real_value - approximate_value;
            let result = Value::abs(difference / real_value);
            (result * 100.0) <= self.error_bound
        }
    }
}

/// Compute the minimum and maximum value for a time series segment whose values
/// are represented by a model of type PMC-Mean.
pub fn min_max(
    _gid: TimeSeriesId,
    _start_time: Timestamp,
    _end_time: Timestamp,
    _sampling_interval: i32,
    model: &[u8],
    _gaps: &[u8],
) -> Value {
    decode_model(model)
}

/// Compute the sum of the values for a time series segment whose values are
/// represented by a model of type PMC-Mean.
pub fn sum(
    _gid: TimeSeriesId,
    start_time: Timestamp,
    end_time: Timestamp,
    sampling_interval: i32,
    model: &[u8],
    _gaps: &[u8],
) -> Value {
    let length = ((end_time - start_time) / sampling_interval as i64) + 1;
    length as Value * decode_model(model)
}

/// Reconstruct the data points for a time series segment whose values are
/// represented by a model of type PMC-Mean. Each data point is split into its
/// three components and appended to `tids`, `timestamps`, and `values`.
pub fn grid(
    gid: TimeSeriesId,
    start_time: Timestamp,
    end_time: Timestamp,
    sampling_interval: i32,
    model: &[u8],
    _gaps: &[u8],
    tids: &mut TimeSeriesIdBuilder,
    timestamps: &mut TimestampBuilder,
    values: &mut ValueBuilder,
) {
    let value = decode_model(model);
    let sampling_interval = sampling_interval as usize;
    for timestamp in (start_time..=end_time).step_by(sampling_interval) {
        tids.append_value(gid).unwrap();
        timestamps.append_value(timestamp).unwrap();
        values.append_value(value).unwrap();
    }
}

/// Read the coefficient for a model of type PMC-Mean from a slice of bytes. The
/// coefficient is the average value of the time series segment whose vales are
/// represented by the model.
fn decode_model(model: &[u8]) -> Value {
    Value::from_be_bytes(model.try_into().unwrap())
}

/// Returns true if `v1` and `v2` are equivalent or both values are NAN.
fn equal_or_nan(v1: Value, v2: Value) -> bool {
    v1 == v2 || (v1.is_nan() && v2.is_nan())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::tests::ProptestValue;
    use proptest::{prop_assert, prop_assume, proptest};

    // Tests for PMCMean.
    proptest! {
    #[test]
    fn test_error_bound_can_be_positive(error_bound in ProptestValue::POSITIVE) {
        assert!(PMCMean::try_new(error_bound).is_ok())
    }

    #[test]
    fn test_error_bound_cannot_be_negative(error_bound in ProptestValue::NEGATIVE) {
        assert!(PMCMean::try_new(error_bound).is_err())
    }
    }

    #[test]
    fn test_error_bound_cannot_be_positive_infinity() {
        assert!(PMCMean::try_new(Value::INFINITY).is_err())
    }

    #[test]
    fn test_error_bound_cannot_be_negative_infinity() {
        assert!(PMCMean::try_new(Value::NEG_INFINITY).is_err())
    }

    #[test]
    fn test_error_bound_cannot_be_nan() {
        assert!(PMCMean::try_new(Value::NAN).is_err())
    }

    #[test]
    fn test_can_fit_sequence_of_positive_infinity_with_error_bound_zero() {
        can_fit_sequence_of_value_with_error_bound_zero(Value::INFINITY)
    }

    #[test]
    fn test_can_fit_sequence_of_negative_infinity_with_error_bound_zero() {
        can_fit_sequence_of_value_with_error_bound_zero(Value::NEG_INFINITY)
    }

    #[test]
    fn test_can_fit_sequence_of_nans_with_error_bound_zero() {
        can_fit_sequence_of_value_with_error_bound_zero(Value::NAN)
    }

    fn can_fit_sequence_of_value_with_error_bound_zero(value: Value) {
        let mut model_type = PMCMean::try_new(0.0).unwrap();
        for _ in 0..5 {
            assert!(model_type.fit_value(value));
        }

        if value.is_nan() {
            assert!(model_type.get_model().is_nan());
        } else {
            assert_eq!(model_type.get_model(), value);
        }
    }

    proptest! {
    #[test]
    fn test_can_fit_one_value(value in ProptestValue::ANY) {
        prop_assert!(PMCMean::try_new(0.0).unwrap().fit_value(value));
    }

    #[test]
    fn test_cannot_fit_other_value_and_positive_infinity(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let mut model_type = PMCMean::try_new(Value::MAX).unwrap();
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_negative_infinity(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let mut model_type = PMCMean::try_new(Value::MAX).unwrap();
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::NEG_INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_nan(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let mut model_type = PMCMean::try_new(Value::MAX).unwrap();
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::NAN));
    }

    #[test]
    fn test_cannot_fit_positive_infinity_and_other_value(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let mut model_type = PMCMean::try_new(Value::MAX).unwrap();
        prop_assert!(model_type.fit_value(Value::INFINITY));
        prop_assert!(!model_type.fit_value(value));
    }

    #[test]
    fn test_cannot_fit_negative_infinity_and_other_value(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let mut model_type = PMCMean::try_new(Value::MAX).unwrap();
        prop_assert!(model_type.fit_value(Value::NEG_INFINITY));
        prop_assert!(!model_type.fit_value(value));
    }

    #[test]
    fn test_cannot_fit_nan_and_other_value(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let mut model_type = PMCMean::try_new(Value::MAX).unwrap();
        prop_assert!(model_type.fit_value(Value::NAN));
        prop_assert!(!model_type.fit_value(value));
    }
    }

    #[test]
    fn test_cannot_fit_sequence_of_different_values_with_error_bound_zero() {
        assert!(!fit_sequence_of_different_values_with_error_bound(0.0))
    }

    #[test]
    fn test_can_fit_sequence_of_different_values_with_error_bound_five() {
        assert!(fit_sequence_of_different_values_with_error_bound(5.0))
    }

    fn fit_sequence_of_different_values_with_error_bound(error_bound: Value) -> bool {
        let mut model_type = PMCMean::try_new(error_bound).unwrap();
        let mut fit_all_values = true;
        for value in [42.0, 42.0, 42.8, 42.0, 42.0] {
            fit_all_values &= model_type.fit_value(value);
        }
        return fit_all_values;
    }

    // Tests for min_max().
    proptest! {
    #[test]
    fn test_min_max(value in ProptestValue::ANY) {
        let model = value.to_be_bytes();
        let min_max = min_max(1, 1657734000, 1657734540, 60, &model, &[]);
        prop_assert!(equal_or_nan(min_max, value));
    }
    }

    // Tests for sum().
    proptest! {
    #[test]
    fn test_sum(value in ProptestValue::ANY) {
        let model = value.to_be_bytes();
        let sum = sum(1, 1657734000, 1657734540, 60, &model, &[]);
        prop_assert!(equal_or_nan(sum, 10.0 * value));
    }
    }

    // Tests for grid().
    proptest! {
    #[test]
    fn test_grid(value in ProptestValue::ANY) {
        let model = value.to_be_bytes();
        let sampling_interval: i64 = 60;
        let mut tids = TimeSeriesIdBuilder::new(10);
        let mut timestamps = TimestampBuilder::new(10);
        let mut values = ValueBuilder::new(10);

        grid(
            1,
            1657734000,
            1657734540,
            sampling_interval as i32,
            &model,
            &[],
            &mut tids,
            &mut timestamps,
            &mut values,
        );

        let tids = tids.finish();
        let timestamps = timestamps.finish();
        let values = values.finish();

        prop_assert!(
            tids.len() == 10 && tids.len() == timestamps.len() && tids.len() == values.len()
        );
        prop_assert!(tids.iter().all(|tid_option| tid_option.unwrap() == 1));
        prop_assert!(timestamps
            .values()
            .windows(2)
            .all(|window| window[1] - window[0] == sampling_interval));
        prop_assert!(values
            .iter()
            .all(|value_option| equal_or_nan(value_option.unwrap(), value)));
    }
    }

    // Tests for the decode_model().
    proptest! {
    #[test]
    fn test_decode_model(value in ProptestValue::ANY) {
        let decoded = decode_model(&value.to_be_bytes());
        prop_assert!(equal_or_nan(decoded, value));
    }
    }

    // Tests for equal_or_nan().
    proptest! {
    #[test]
    fn test_equal_or_nan_equal(value in ProptestValue::ANY) {
        assert!(equal_or_nan(value, value));
    }

    #[test]
    fn test_equal_or_nan_not_equal(v1 in ProptestValue::ANY, v2 in ProptestValue::ANY) {
        prop_assume!(v1 != v2 && !v1.is_nan() && !v2.is_nan());
        prop_assert!(!equal_or_nan(v1, v2));
    }
    }
}
