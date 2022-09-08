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

//! Implementation of the PMC-Mean model type from the [Poor Man’s Compression
//! paper] and efficient computation of aggregates for models of type PMC-Mean
//! as described in the [ModelarDB paper].
//!
//! [Poor Man’s Compression paper]: https://ieeexplore.ieee.org/document/1260811
//! [ModelarDB paper]: http://www.vldb.org/pvldb/vol11/p1688-jensen.pdf

use crate::models;
use crate::models::ErrorBound;
use crate::types::{
    TimeSeriesId, TimeSeriesIdBuilder, Timestamp, TimestampBuilder, Value, ValueBuilder,
};

/// The state the PMC-Mean model type needs while fitting a model to a time
/// series segment.
pub struct PMCMean {
    /// Maximum relative error for the value of each data point.
    error_bound: ErrorBound,
    /// Minimum value in the segment the current model is fitted to.
    min_value: Value,
    /// Maximum value in the segment the current model is fitted to.
    max_value: Value,
    /// The sum of the values in the segment the current model is fitted to.
    sum_of_values: f64,
    /// The number of data points the current model has been fitted to.
    length: usize,
}

impl PMCMean {
    pub fn new(error_bound: ErrorBound) -> Self {
        Self {
            error_bound,
            min_value: Value::NAN,
            max_value: Value::NAN,
            sum_of_values: 0.0,
            length: 0,
        }
    }

    /// Attempt to update the current model of type PMC-Mean to also represent
    /// `value`. Returns [`true`] if the model can also represent `value`,
    /// otherwise [`false`].
    pub fn fit_value(&mut self, value: Value) -> bool {
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

    /// Return the number of values the model currently represents.
    pub fn get_length(&self) -> usize {
        self.length
    }

    /// Return the number of bytes the current model uses per data point on average.
    pub fn get_bytes_per_value(&self) -> f32 {
        models::VALUE_SIZE_IN_BYTES as f32 / self.length as f32
    }

    /// Return the current model. For a model of type PMC-Mean, its coefficient
    /// is the average value of the time series segment the model represents.
    pub fn get_model(&self) -> Value {
        (self.sum_of_values / self.length as f64) as Value
    }

    /// Determine if `approximate_value` is within [`PMCMean's`](PMCMean)
    /// relative error bound of `real_value`.
    fn is_value_within_error_bound(&self, real_value: Value, approximate_value: Value) -> bool {
        // Needed because result becomes NAN and approximate_value is rejected
        // if approximate_value and real_value are zero, and because NAN != NAN.
        if models::equal_or_nan(real_value as f64, approximate_value as f64) {
            true
        } else {
            let difference = real_value - approximate_value;
            let result = Value::abs(difference / real_value);
            (result * 100.0) <= self.error_bound
        }
    }
}

/// Compute the minimum value for a time series segment whose values are
/// represented by a model of type PMC-Mean.
pub fn min(model: &[u8]) -> Value {
    decode_model(model)
}

/// Compute the maximum value for a time series segment whose values are
/// represented by a model of type PMC-Mean.
pub fn max(model: &[u8]) -> Value {
    decode_model(model)
}

/// Compute the sum of the values for a time series segment whose values are
/// represented by a model of type PMC-Mean.
pub fn sum(
    start_time: Timestamp,
    end_time: Timestamp,
    sampling_interval: i32,
    model: &[u8],
) -> Value {
    let length = models::length(start_time, end_time, sampling_interval);
    length as Value * decode_model(model)
}

/// Reconstruct the data points for a time series segment whose values are
/// represented by a model of type PMC-Mean. Each data point is split into its
/// three components and appended to `time_series_ids`, `timestamps`, and
/// `values`.
pub fn grid(
    time_series_id: TimeSeriesId,
    start_time: Timestamp,
    end_time: Timestamp,
    sampling_interval: i32,
    model: &[u8],
    time_series_ids: &mut TimeSeriesIdBuilder,
    timestamps: &mut TimestampBuilder,
    values: &mut ValueBuilder,
) {
    let value = decode_model(model);
    let sampling_interval = sampling_interval as usize;
    for timestamp in (start_time..=end_time).step_by(sampling_interval) {
        time_series_ids.append_value(time_series_id);
        timestamps.append_value(timestamp);
        values.append_value(value);
    }
}

/// Read the coefficient for a model of type PMC-Mean from a slice of bytes. The
/// coefficient is the average value of the time series segment whose vales are
/// represented by the model.
fn decode_model(model: &[u8]) -> Value {
    Value::from_be_bytes(model.try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::tests::ProptestValue;
    use proptest::{prop_assert, prop_assume, proptest};

    // Tests for PMCMean.
    proptest! {
    #[test]
    fn test_can_fit_sequence_of_finite_value_with_error_bound_zero(value in ProptestValue::ANY) {
        can_fit_sequence_of_value_with_error_bound_zero(value)
    }
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
        let error_bound_zero = ErrorBound::try_new(0.0).unwrap();
        let mut model_type = PMCMean::new(error_bound_zero);
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
        let error_bound_zero = ErrorBound::try_new(0.0).unwrap();
        prop_assert!(PMCMean::new(error_bound_zero).fit_value(value));
    }

    #[test]
    fn test_cannot_fit_other_value_and_positive_infinity(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new(f32::MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_negative_infinity(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new(f32::MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::NEG_INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_nan(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new(f32::MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::NAN));
    }

    #[test]
    fn test_cannot_fit_positive_infinity_and_other_value(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new(f32::MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(Value::INFINITY));
        prop_assert!(!model_type.fit_value(value));
    }

    #[test]
    fn test_cannot_fit_negative_infinity_and_other_value(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new(f32::MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(Value::NEG_INFINITY));
        prop_assert!(!model_type.fit_value(value));
    }

    #[test]
    fn test_cannot_fit_nan_and_other_value(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new(f32::MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(Value::NAN));
        prop_assert!(!model_type.fit_value(value));
    }
    }

    #[test]
    fn test_cannot_fit_sequence_of_different_values_with_error_bound_zero() {
        let error_bound_zero = ErrorBound::try_new(0.0).unwrap();
        assert!(!fit_sequence_of_different_values_with_error_bound(
            error_bound_zero
        ))
    }

    #[test]
    fn test_can_fit_sequence_of_different_values_with_error_bound_five() {
        let error_bound_five = ErrorBound::try_new(5.0).unwrap();
        assert!(fit_sequence_of_different_values_with_error_bound(
            error_bound_five
        ))
    }

    fn fit_sequence_of_different_values_with_error_bound(error_bound: ErrorBound) -> bool {
        let mut model_type = PMCMean::new(error_bound);
        let mut fit_all_values = true;
        for value in [42.0, 42.0, 42.8, 42.0, 42.0] {
            fit_all_values &= model_type.fit_value(value);
        }
        return fit_all_values;
    }

    // Tests for min().
    proptest! {
    #[test]
    fn test_min(value in ProptestValue::ANY) {
        let model = value.to_be_bytes();
        let min = min(&model);
        prop_assert!(models::equal_or_nan(min as f64, value as f64));
    }
    }

    // Tests for max().
    proptest! {
    #[test]
    fn test_max(value in ProptestValue::ANY) {
        let model = value.to_be_bytes();
        let max = max(&model);
        prop_assert!(models::equal_or_nan(max as f64, value as f64));
    }
    }

    // Tests for sum().
    proptest! {
    #[test]
    fn test_sum(value in ProptestValue::ANY) {
        let model = value.to_be_bytes();
        let sum = sum(1657734000, 1657734540, 60, &model);
        prop_assert!(models::equal_or_nan(sum as f64, (10.0 * value) as f64));
    }
    }

    // Tests for grid().
    proptest! {
    #[test]
    fn test_grid(value in ProptestValue::ANY) {
        let model = value.to_be_bytes();
        let sampling_interval: i64 = 60;
        let mut time_series_ids = TimeSeriesIdBuilder::new(10);
        let mut timestamps = TimestampBuilder::new(10);
        let mut values = ValueBuilder::new(10);

        grid(
            1,
            1657734000,
            1657734540,
            sampling_interval as i32,
            &model,
            &mut time_series_ids,
            &mut timestamps,
            &mut values,
        );

        let time_series_ids = time_series_ids.finish();
        let timestamps = timestamps.finish();
        let values = values.finish();

        prop_assert!(
            time_series_ids.len() == 10
            && time_series_ids.len() == timestamps.len()
            && time_series_ids.len() == values.len()
        );
        prop_assert!(time_series_ids
             .iter()
             .all(|time_series_id_option| time_series_id_option.unwrap() == 1));
        prop_assert!(timestamps
            .values()
            .windows(2)
            .all(|window| window[1] - window[0] == sampling_interval));
        prop_assert!(values
            .iter()
            .all(|value_option| models::equal_or_nan(value_option.unwrap() as f64, value as f64)));
    }
    }

    // Tests for the decode_model().
    proptest! {
    #[test]
    fn test_decode_model(value in ProptestValue::ANY) {
        let decoded = decode_model(&value.to_be_bytes());
        prop_assert!(models::equal_or_nan(decoded as f64, value as f64));
    }
    }
}
