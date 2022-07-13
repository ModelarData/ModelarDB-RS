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

use std::convert::TryInto;

use datafusion::arrow::array::{Float32Builder, Int32Builder, TimestampMillisecondBuilder};

///The state the PMC-Mean model type needs while fitting a model to a time
/// series segment.
struct PMCMeanModelType {
    /// Maximum relative error for the value of each data point.
    error_bound: f32,
    /// Minimum value in the segment the current model is fitted to.
    min_value: f32,
    /// Maximum value in the segment the current model is fitted to.
    max_value: f32,
    /// The sum of the values in the segment the current model is fitted to.
    sum_of_values: f64,
    /// The number of data points the current model has been fitted to.
    length: u32,
}

impl PMCMeanModelType {
    fn new(error_bound: f32) -> Self {
        Self {
            error_bound,
            min_value: f32::NAN,
            max_value: f32::NAN,
            sum_of_values: 0.0,
            length: 0,
        }
    }

    /// Attempt to update the current model of type PMC-Mean to also represent
    /// `value`. Returns `true` if the model can also represent `value`,
    /// otherwise `false`.
    fn fit_value(&mut self, value: f32) -> bool {
        let next_sum_of_values = self.sum_of_values + value as f64;
        let average = next_sum_of_values / (self.length + 1) as f64;

        let next_min_value = f32::min(self.min_value, value);
        let next_max_value = f32::max(self.max_value, value);
        if PMCMeanModelType::is_value_within_error_bound(self, average as f32, next_min_value)
            || PMCMeanModelType::is_value_within_error_bound(self, average as f32, next_max_value)
        {
            self.min_value = next_min_value;
            self.max_value = next_max_value;
            self.sum_of_values = next_sum_of_values;
            self.length += 1;
            true
        } else {
            false
        }
    }

    /// Return the current model. For a model of type PMC-Mean, its coefficient
    /// is the average value of the bounded time series the model represents.
    fn get_model(&self) -> f32 {
        (self.sum_of_values / self.length as f64) as f32
    }

    /// Determine if `approximate_value` is within the relative
    /// `self.error_bound` of `real_value`.
    fn is_value_within_error_bound(&self, real_value: f32, approximate_value: f32) -> bool {
        // Needed as the calculation in else becomes NaN if both values are zero.
        if approximate_value == real_value || (real_value.is_nan() && approximate_value.is_nan()) {
            true
        } else {
            let difference = real_value - approximate_value;
            let result = f32::abs(difference / real_value);
            (result * 100.0) <= self.error_bound
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::num::f32;
    use proptest::{prop_assert, prop_assume, proptest};

    pub static SEQUENCE_OF_NAN: [f32; 5] = [f32::NAN, f32::NAN, f32::NAN, f32::NAN, f32::NAN];

    pub static SEQUENCE_OF_POSITIVE_INFINITY: [f32; 5] = [
        f32::INFINITY,
        f32::INFINITY,
        f32::INFINITY,
        f32::INFINITY,
        f32::INFINITY,
    ];

    pub static SEQUENCE_OF_NEGATIVE_INFINITY: [f32; 5] = [
        f32::NEG_INFINITY,
        f32::NEG_INFINITY,
        f32::NEG_INFINITY,
        f32::NEG_INFINITY,
        f32::NEG_INFINITY,
    ];

    #[test]
    fn test_fit_sequence_of_nans() {
        let mut state = PMCMeanModelType::new(0.0);
        for value in SEQUENCE_OF_NAN {
            assert!(state.fit_value(value));
        }
        assert!(state.get_model().is_nan());
    }

    #[test]
    fn test_fit_sequence_of_positive_infinity() {
        let mut state = PMCMeanModelType::new(0.0);
        for value in SEQUENCE_OF_POSITIVE_INFINITY {
            assert!(state.fit_value(value));
        }
        assert!(state.get_model() == f32::INFINITY);
    }

    #[test]
    fn test_fit_sequence_of_negative_infinity() {
        let mut state = PMCMeanModelType::new(0.0);
        for value in SEQUENCE_OF_NEGATIVE_INFINITY {
            assert!(state.fit_value(value));
        }
        assert!(state.get_model() == f32::NEG_INFINITY);
    }

    proptest! {
        #[test]
        fn test_fit_one_value_always_succeeds(value in f32::ANY) {
            prop_assert!(PMCMeanModelType::new(0.0).fit_value(value));
        }

        #[test]
        fn test_fit_nan_other_always_fails(value in f32::ANY) {
            prop_assume!( ! value.is_nan());
            let mut state = PMCMeanModelType::new(0.0);
            assert!(state.fit_value(f32::NAN));
            prop_assert!( ! state.fit_value(value));
        }

        #[test]
        fn test_fit_positive_infinity_other_always_fails(value in f32::ANY) {
            prop_assume!(value != f32::INFINITY);
            let mut state = PMCMeanModelType::new(0.0);
            assert!(state.fit_value(f32::INFINITY));
            prop_assert!( ! state.fit_value(value));
        }

        #[test]
        fn test_fit_negative_infinity_other_always_fails(value in f32::ANY) {
            prop_assume!(value != f32::NEG_INFINITY);
            let mut state = PMCMeanModelType::new(0.0);
            assert!(state.fit_value(f32::NEG_INFINITY));
            prop_assert!( ! state.fit_value(value));
        }
    }
}

/// Compute the minimum and maximum value for the bounded time series whose
/// values are represented by model.
pub fn min_max(
    _gid: i32,
    _start_time: i64,
    _end_time: i64,
    _sampling_interval: i32,
    model: &[u8],
    _gaps: &[u8],
) -> f32 {
    decode_model(model)
}

/// Compute the sum of the values for the bounded time series whose values are
/// represented by the model.
pub fn sum(
    _gid: i32,
    start_time: i64,
    end_time: i64,
    sampling_interval: i32,
    model: &[u8],
    _gaps: &[u8],
) -> f32 {
    let length = ((end_time - start_time) / sampling_interval as i64) + 1;
    length as f32 * decode_model(model)
}

/// Reconstruct the data points for the bounded time series whose values are
///  represented by the model.
pub fn grid(
    gid: i32,
    start_time: i64,
    end_time: i64,
    sampling_interval: i32,
    model: &[u8],
    _gaps: &[u8],
    tids: &mut Int32Builder,
    timestamps: &mut TimestampMillisecondBuilder,
    values: &mut Float32Builder,
) {
    let value = decode_model(model);
    let sampling_interval = sampling_interval as usize;
    for timestamp in (start_time..=end_time).step_by(sampling_interval) {
        tids.append_value(gid).unwrap();
        timestamps.append_value(timestamp).unwrap();
        values.append_value(value).unwrap();
    }
}

/// Decode the model's coefficient in the byte array to a floating-point value.
fn decode_model(model: &[u8]) -> f32 {
    f32::from_be_bytes(model.try_into().unwrap())
}
