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

//! Implementation of the Swing model type from the [Swing and Slide paper] and
//! efficient computation of aggregates for models of type Swing as described in
//! the [ModelarDB paper].
//!
//! In the implementation of Swing, [`f64`] is generally used instead of
//! [`Value`] to make the calculations precise enough.
//!
//! [Swing and Slide paper]: https://dl.acm.org/doi/10.14778/1687627.1687645
//! [ModelarDB paper]: https://www.vldb.org/pvldb/vol11/p1688-jensen.pdf

use modelardb_common::schemas::COMPRESSED_METADATA_SIZE_IN_BYTES;
use modelardb_common::types::{
    ErrorBound, Timestamp, TimestampBuilder, UnivariateId, UnivariateIdBuilder, Value, ValueBuilder,
};

use super::timestamps;
use crate::models;

/// The state the Swing model type needs while fitting a model to a time series
/// segment.
pub struct Swing {
    /// Maximum relative error for the value of each data point.
    error_bound: ErrorBound,
    /// Time at which the first value represented by the current model was
    /// collected.
    start_time: Timestamp,
    /// Time at which the last value represented by the current model was
    /// collected.
    end_time: Timestamp,
    /// First value in the segment the current model is fitted to.
    first_value: f64, // f64 instead of Value to remove casts in fit_value()
    /// Slope for the linear function specifying the upper bound for the current
    /// model.
    upper_bound_slope: f64,
    /// Intercept for the linear function specifying the upper bound for the
    /// current model.
    upper_bound_intercept: f64,
    /// Slope for the linear function specifying the lower bound for the current
    /// model.
    lower_bound_slope: f64,
    /// Intercept for the linear function specifying the lower bound for the
    /// current model.
    lower_bound_intercept: f64,
    /// The number of data points the current model has been fitted to.
    length: usize,
}

impl Swing {
    pub fn new(error_bound: ErrorBound) -> Self {
        Self {
            error_bound,
            start_time: 0,
            end_time: 0,
            first_value: f64::NAN,
            upper_bound_slope: f64::NAN,
            upper_bound_intercept: f64::NAN,
            lower_bound_slope: f64::NAN,
            lower_bound_intercept: f64::NAN,
            length: 0,
        }
    }

    /// Attempt to update the current model of type Swing to also represent the
    /// data point (`timestamp`, `value`). Returns [`true`] if the model can
    /// also represent the data point, otherwise [`false`].
    ///
    /// Swing fits a linear function to a time series segment in three stages:
    /// - (1) When the first data point is received, it is stored in memory.
    /// - (2) When the second data point is received, two linear functions that
    /// intersect with the first data point are computed to designate the upper
    /// and lower bounds for the linear functions Swing can fit to the segment.
    /// - (3) Then for each subsequent data point, Swing determines if the data
    /// point can be represented by a linear function in the space delimited by
    /// the upper and lower bounds and updates these bounds if necessary.
    ///
    /// For more detail see Algorithm 1 in the [Swing and Slide paper].
    ///
    /// [Swing and Slide paper]: https://dl.acm.org/doi/10.14778/1687627.1687645
    pub fn fit_data_point(&mut self, timestamp: Timestamp, value: Value) -> bool {
        // Simplify the calculations by removing a significant number of casts.
        let value = value as f64;
        let maximum_deviation = models::maximum_allowed_deviation(self.error_bound, value);

        if self.length == 0 {
            // Line 1 - 2 of Algorithm 1 in the Swing and Slide paper.
            self.start_time = timestamp;
            self.end_time = timestamp;
            self.first_value = value;
            self.length += 1;
            true
        } else if !self.first_value.is_finite() || !value.is_finite() {
            // Extend Swing to handle both types of infinity and NAN.
            if models::equal_or_nan(self.first_value, value) {
                self.end_time = timestamp;
                self.upper_bound_slope = value;
                self.upper_bound_intercept = value;
                self.lower_bound_slope = value;
                self.lower_bound_intercept = value;
                self.length += 1;
                true
            } else {
                false
            }
        } else if self.length == 1 {
            // Line 3 of Algorithm 1 in the Swing and Slide paper.
            self.end_time = timestamp;
            (self.upper_bound_slope, self.upper_bound_intercept) = compute_slope_and_intercept(
                self.start_time,
                self.first_value,
                timestamp,
                value + maximum_deviation,
            );

            (self.lower_bound_slope, self.lower_bound_intercept) = compute_slope_and_intercept(
                self.start_time,
                self.first_value,
                timestamp,
                value - maximum_deviation,
            );
            self.length += 1;
            true
        } else {
            // Line 6 of Algorithm 1 in the Swing and Slide paper.
            let upper_bound_approximate_value =
                self.upper_bound_slope * timestamp as f64 + self.upper_bound_intercept;
            let lower_bound_approximate_value =
                self.lower_bound_slope * timestamp as f64 + self.lower_bound_intercept;

            if upper_bound_approximate_value + maximum_deviation < value
                || lower_bound_approximate_value - maximum_deviation > value
            {
                false
            } else {
                self.end_time = timestamp;

                // Line 17 of Algorithm 1 in the Swing and Slide paper.
                if upper_bound_approximate_value - maximum_deviation > value {
                    (self.upper_bound_slope, self.upper_bound_intercept) =
                        compute_slope_and_intercept(
                            self.start_time,
                            self.first_value,
                            timestamp,
                            value + maximum_deviation,
                        );
                }

                // Line 15 of Algorithm 1 in the Swing and Slide paper.
                if lower_bound_approximate_value + maximum_deviation < value {
                    (self.lower_bound_slope, self.lower_bound_intercept) =
                        compute_slope_and_intercept(
                            self.start_time,
                            self.first_value,
                            timestamp,
                            value - maximum_deviation,
                        );
                }
                self.length += 1;
                true
            }
        }
    }

    /// Return the number of values the model currently represents.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Return the number of bytes the current model uses per data point on average.
    pub fn bytes_per_value(&self) -> f32 {
        // One additional byte is needed for Swing to store if it is increasing or decreasing.
        (COMPRESSED_METADATA_SIZE_IN_BYTES.to_owned() as f32 + 1.0) / self.length as f32
    }

    /// Return the current model. For a model of type Swing, the first and last
    /// values of the time series segment the model represents are returned. The
    /// two values are returned instead of the slope and intercept as the values
    /// only require `size_of::<Value>` while the slope and intercept generally
    /// must be [`f64`] to be precise enough.
    pub fn model(self) -> (Value, Value) {
        // TODO: use the function with the minimum error as specified in the Swing and Slide paper.
        let average_slope = (self.lower_bound_slope + self.upper_bound_slope) / 2.0;
        let average_intercept = (self.lower_bound_intercept + self.upper_bound_intercept) / 2.0;
        let first_value = average_slope * self.start_time as f64 + average_intercept;
        let last_value = average_slope * self.end_time as f64 + average_intercept;
        (first_value as Value, last_value as Value)
    }
}

/// Return [`true`] if two time series segment whose values are represented by a model of type Swing
/// can be merged. Currently, only segments whose models produce a new model with the exact same
/// slope and intercept when merged can be merged as it is guaranteed to produce the same values.
pub(crate) fn can_be_merged(
    segment_one_start_time: Timestamp,
    segment_one_end_time: Timestamp,
    segment_one_first_value: f64,
    segment_one_last_value: f64,
    segment_two_start_time: Timestamp,
    segment_two_end_time: Timestamp,
    segment_two_first_value: f64,
    segment_two_last_value: f64,
) -> bool {
    let (segment_one_slope, segment_one_intercept) = compute_slope_and_intercept(
        segment_one_start_time,
        segment_one_first_value,
        segment_one_end_time,
        segment_one_last_value,
    );

    let (segment_two_slope, segment_two_intercept) = compute_slope_and_intercept(
        segment_two_start_time,
        segment_two_first_value,
        segment_two_end_time,
        segment_two_last_value,
    );

    let (segment_merged_slope, segment_merged_intercept) = compute_slope_and_intercept(
        segment_one_start_time,
        segment_one_first_value,
        segment_two_end_time,
        segment_two_last_value,
    );

    segment_one_slope == segment_merged_slope
        && segment_one_intercept == segment_merged_intercept
        && segment_two_slope == segment_merged_slope
        && segment_two_intercept == segment_merged_intercept
}

/// Compute the sum of the values for a time series segment whose values are
/// represented by a model of type Swing.
pub fn sum(
    start_time: Timestamp,
    end_time: Timestamp,
    timestamps: &[u8],
    first_value: Value,
    last_value: Value,
    residuals_length: usize,
) -> Value {
    let (slope, intercept) =
        compute_slope_and_intercept(start_time, first_value as f64, end_time, last_value as f64);

    if timestamps::are_compressed_timestamps_regular(timestamps) {
        let first = slope * start_time as f64 + intercept;
        let last = slope * end_time as f64 + intercept;
        let average = (first + last) / 2.0;
        let length = models::len(start_time, end_time, timestamps) - residuals_length;
        (average * length as f64) as Value
    } else {
        let mut timestamp_builder = TimestampBuilder::new();

        timestamps::decompress_all_timestamps(
            start_time,
            end_time,
            timestamps,
            &mut timestamp_builder,
        );

        let timestamps = timestamp_builder.finish();
        let model_timestamps_end_index = timestamps.len() - residuals_length;

        let mut sum: f64 = 0.0;
        for timestamp in &timestamps.values()[0..model_timestamps_end_index] {
            sum += slope * (*timestamp as f64) + intercept;
        }
        sum as Value
    }
}

/// Reconstruct the values for the `timestamps` without matching values in
/// `value_builder` using a model of type Swing. The `univariate_ids` and
/// `values` are appended to `univariate_id_builder` and `value_builder`.
pub fn grid(
    univariate_id: UnivariateId,
    start_time: Timestamp,
    end_time: Timestamp,
    first_value: Value,
    last_value: Value,
    univariate_id_builder: &mut UnivariateIdBuilder,
    timestamps: &[Timestamp],
    value_builder: &mut ValueBuilder,
) {
    let (slope, intercept) =
        compute_slope_and_intercept(start_time, first_value as f64, end_time, last_value as f64);

    for timestamp in timestamps {
        univariate_id_builder.append_value(univariate_id);
        let value = (slope * (*timestamp as f64) + intercept) as Value;
        value_builder.append_value(value);
    }
}

/// Compute the slope and intercept of a linear function that intersects with
/// the data points (`start_time`, `first_value`) and (`end_time`, `last_value`).
fn compute_slope_and_intercept(
    start_time: Timestamp,
    first_value: f64,
    end_time: Timestamp,
    last_value: f64,
) -> (f64, f64) {
    // An if expression is used as it seems impossible to calculate the slope
    // and intercept without creating INFINITY, NEG_INFINITY, or NaN values.
    if models::equal_or_nan(first_value, last_value) {
        (0.0, first_value)
    } else {
        debug_assert!(first_value.is_finite(), "First value is not finite.");
        debug_assert!(last_value.is_finite(), "Last value is not finite.");
        let slope = (last_value - first_value) / (end_time - start_time) as f64;
        let intercept = first_value - slope * start_time as f64;
        (slope, intercept)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{BinaryArray, Float32Array, UInt64Array, UInt8Array};
    use modelardb_common::types::{TimestampArray, TimestampBuilder, ValueArray, ValueBuilder};
    use proptest::num::f32 as ProptestValue;
    use proptest::strategy::Strategy;
    use proptest::{num, prop_assert, prop_assert_eq, prop_assume, proptest};

    use crate::models::SWING_ID;
    use crate::tests::{
        ERROR_BOUND_ABSOLUTE_MAX, ERROR_BOUND_FIVE, ERROR_BOUND_RELATIVE_MAX, ERROR_BOUND_ZERO,
    };

    // Tests constants chosen to be realistic while minimizing the testing time.
    const SAMPLING_INTERVAL: Timestamp = 1000;
    const START_TIME: Timestamp = 1658671178037;
    const END_TIME: Timestamp = START_TIME + SAMPLING_INTERVAL;
    const SEGMENT_LENGTH: Timestamp = 5; // Timestamp is used to remove casts.

    // Tests for Swing.
    proptest! {
    #[test]
    fn test_can_fit_sequence_of_finite_value_with_absolute_error_bound(value in ProptestValue::ANY) {
        can_fit_sequence_of_value_with_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            value)
    }

    #[test]
    fn test_can_fit_sequence_of_finite_value_with_relative_error_bound(value in ProptestValue::ANY) {
        can_fit_sequence_of_value_with_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            value)
    }
    }

    #[test]
    fn test_can_fit_sequence_of_positive_infinity_with_absolute_error_bound() {
        can_fit_sequence_of_value_with_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            Value::INFINITY,
        )
    }

    #[test]
    fn test_can_fit_sequence_of_positive_infinity_with_relative_error_bound() {
        can_fit_sequence_of_value_with_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            Value::INFINITY,
        )
    }

    #[test]
    fn test_can_fit_sequence_of_negative_infinity_with_absolute_error_bound() {
        can_fit_sequence_of_value_with_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            Value::NEG_INFINITY,
        )
    }

    #[test]
    fn test_can_fit_sequence_of_negative_infinity_with_relative_error_bound() {
        can_fit_sequence_of_value_with_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            Value::NEG_INFINITY,
        )
    }

    #[test]
    fn test_can_fit_sequence_of_nans_with_absolute_error_bound() {
        can_fit_sequence_of_value_with_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            Value::NAN,
        )
    }

    #[test]
    fn test_can_fit_sequence_of_nans_with_relative_error_bound() {
        can_fit_sequence_of_value_with_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            Value::NAN,
        )
    }

    fn can_fit_sequence_of_value_with_error_bound(error_bound: ErrorBound, value: Value) {
        let mut model_type = Swing::new(error_bound);
        let end_time = START_TIME + SEGMENT_LENGTH * SAMPLING_INTERVAL;
        for timestamp in (START_TIME..end_time).step_by(SAMPLING_INTERVAL as usize) {
            assert!(model_type.fit_data_point(timestamp, value));
        }

        let (first_value, last_value) = model_type.model();
        let (slope, intercept) = compute_slope_and_intercept(
            START_TIME,
            first_value as f64,
            end_time,
            last_value as f64,
        );
        if value.is_nan() {
            assert!(slope == 0.0 && intercept.is_nan());
        } else {
            for timestamp in (START_TIME..end_time).step_by(SAMPLING_INTERVAL as usize) {
                let approximate_value = slope * timestamp as f64 + intercept;
                assert_eq!(approximate_value, value as f64);
            }
        }
    }

    proptest! {
    #[test]
    fn test_can_fit_one_value_with_absolute_error_bound(value in ProptestValue::ANY) {
        let error_bound_zero = ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap();
        prop_assert!(Swing::new(error_bound_zero).fit_data_point(START_TIME, value));
    }

    #[test]
    fn test_can_fit_one_value_with_relative_error_bound(value in ProptestValue::ANY) {
        let error_bound_zero = ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap();
        prop_assert!(Swing::new(error_bound_zero).fit_data_point(START_TIME, value));
    }

    #[test]
    fn test_can_fit_two_finite_value_with_absolute_error_bound(
        first_value in ProptestValue::NORMAL,
        second_value in ProptestValue::NORMAL
    ) {
        let error_bound_zero = ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap();
        let mut model_type = Swing::new(error_bound_zero);
        prop_assert!(model_type.fit_data_point(START_TIME, first_value));
        prop_assert!(model_type.fit_data_point(END_TIME, second_value));
    }

    #[test]
    fn test_can_fit_two_finite_value_with_relative_error_bound(
        first_value in ProptestValue::NORMAL,
        second_value in ProptestValue::NORMAL
    ) {
        let error_bound_zero = ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap();
        let mut model_type = Swing::new(error_bound_zero);
        prop_assert!(model_type.fit_data_point(START_TIME, first_value));
        prop_assert!(model_type.fit_data_point(END_TIME, second_value));
    }

    #[test]
    fn test_cannot_fit_other_value_and_positive_infinity_with_absolute_error_bound(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, value));
        prop_assert!(!model_type.fit_data_point(END_TIME, Value::INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_positive_infinity_with_relative_error_bound(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, value));
        prop_assert!(!model_type.fit_data_point(END_TIME, Value::INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_negative_infinity_with_absolute_error_bound(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, value));
        prop_assert!(!model_type.fit_data_point(END_TIME, Value::NEG_INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_negative_infinity_with_relative_error_bound(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, value));
        prop_assert!(!model_type.fit_data_point(END_TIME, Value::NEG_INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_nan_with_absolute_error_bound(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, value));
        prop_assert!(!model_type.fit_data_point(END_TIME, Value::NAN));
    }

    #[test]
    fn test_cannot_fit_other_value_and_nan_with_relative_error_bound(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, value));
        prop_assert!(!model_type.fit_data_point(END_TIME, Value::NAN));
    }

    #[test]
    fn test_cannot_fit_positive_infinity_and_other_value_with_absolute_error_bound(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, Value::INFINITY));
        prop_assert!(!model_type.fit_data_point(END_TIME, value));
    }

    #[test]
    fn test_cannot_fit_positive_infinity_and_other_value_with_relative_error_bound(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, Value::INFINITY));
        prop_assert!(!model_type.fit_data_point(END_TIME, value));
    }

    #[test]
    fn test_cannot_fit_negative_infinity_and_other_value_with_absolute_error_bound(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, Value::NEG_INFINITY));
        prop_assert!(!model_type.fit_data_point(END_TIME, value));
    }

    #[test]
    fn test_cannot_fit_negative_infinity_and_other_value_with_relative_error_bound(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, Value::NEG_INFINITY));
        prop_assert!(!model_type.fit_data_point(END_TIME, value));
    }

    #[test]
    fn test_cannot_fit_nan_and_other_value_with_absolute_error_bound(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, Value::NAN));
        prop_assert!(!model_type.fit_data_point(END_TIME, value));
    }

    #[test]
    fn test_cannot_fit_nan_and_other_value_with_relative_error_bound(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(START_TIME, Value::NAN));
        prop_assert!(!model_type.fit_data_point(END_TIME, value));
    }
    }

    #[test]
    fn test_can_fit_sequence_of_linear_values_with_absolute_error_bound() {
        assert!(fit_sequence_of_values_with_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            &[42.0, 84.0, 126.0, 168.0, 210.0],
        ))
    }

    #[test]
    fn test_can_fit_sequence_of_linear_values_with_relative_error_bound() {
        assert!(fit_sequence_of_values_with_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            &[42.0, 84.0, 126.0, 168.0, 210.0],
        ))
    }

    #[test]
    fn test_cannot_fit_sequence_of_different_values_with_absolute_error_bound() {
        assert!(!fit_sequence_of_values_with_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            &[42.0, 42.0, 42.8, 42.0, 42.0],
        ))
    }

    #[test]
    fn test_cannot_fit_sequence_of_different_values_with_relative_error_bound() {
        assert!(!fit_sequence_of_values_with_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            &[42.0, 42.0, 42.8, 42.0, 42.0],
        ))
    }

    #[test]
    fn test_can_fit_sequence_of_different_values_with_absolute_error_bound() {
        assert!(fit_sequence_of_values_with_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_FIVE).unwrap(),
            &[42.0, 42.0, 42.8, 42.0, 42.0],
        ))
    }

    #[test]
    fn test_can_fit_sequence_of_different_values_with_relative_error_bound() {
        assert!(fit_sequence_of_values_with_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
            &[42.0, 42.0, 42.8, 42.0, 42.0],
        ))
    }

    fn fit_sequence_of_values_with_error_bound(error_bound: ErrorBound, values: &[Value]) -> bool {
        let mut model_type = Swing::new(error_bound);
        let mut fit_all_values = true;
        let mut timestamp = START_TIME;
        for value in values {
            fit_all_values &= model_type.fit_data_point(timestamp, *value);
            timestamp += SAMPLING_INTERVAL;
        }
        fit_all_values
    }

    // Tests for sum().
    proptest! {
    #[test]
    fn test_sum(
        first_value in num::i32::ANY.prop_map(i32_to_value),
        last_value in num::i32::ANY.prop_map(i32_to_value),
    ) {
        let sum = sum(START_TIME, END_TIME, &[], first_value, last_value, 0);
        prop_assert_eq!(sum, first_value + last_value);
    }
    }

    // Tests for grid().
    proptest! {
    #[test]
    fn test_grid(value in num::i32::ANY.prop_map(i32_to_value)) {
        let timestamps: Vec<Timestamp> = (START_TIME ..= END_TIME)
            .step_by(SAMPLING_INTERVAL as usize).collect();
        let mut univariate_id_builder = UnivariateIdBuilder::with_capacity(timestamps.len());
        let mut value_builder = ValueBuilder::with_capacity(timestamps.len());

        // The linear function represents a constant to have a known value.
        grid(
            1,
            START_TIME,
            END_TIME,
            value,
            value,
            &mut univariate_id_builder,
            &timestamps,
            &mut value_builder,
        );

        let univariate_ids = univariate_id_builder.finish();
        let values = value_builder.finish();

        prop_assert!(
            univariate_ids.len() == timestamps.len()
            && univariate_ids.len() == values.len()
        );
        prop_assert!(univariate_ids
             .iter()
             .all(|maybe_univariate_id| maybe_univariate_id.unwrap() == 1));
        prop_assert!(timestamps
            .windows(2)
            .all(|window| window[1] - window[0] == SAMPLING_INTERVAL));
        prop_assert!(values
            .iter()
            .all(|value_option| models::equal_or_nan(value_option.unwrap() as f64, value as f64)));
    }
    }

    fn i32_to_value(index: i32) -> Value {
        // TODO: support all values from ProptestValue::ANY for min, max, and
        // sum. Currently, extreme values (e.g., +-e40) produce wrong results.
        // Ensure Value is always within a range that a model of type Swing can
        // aggregate. Within one million the aggregates are always correct.
        (index % 1_000_000) as Value
    }

    #[test]
    fn test_can_reconstruct_sequence_of_linear_increasing_values_within_absolute_error_bound() {
        test_can_reconstruct_sequence_of_linear_values_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            (42..=4200).step_by(42).map(|value| value as f32).collect(),
        )
    }

    #[test]
    fn test_can_reconstruct_sequence_of_linear_increasing_values_within_relative_error_bound() {
        test_can_reconstruct_sequence_of_linear_values_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            (42..=4200).step_by(42).map(|value| value as f32).collect(),
        )
    }

    #[test]
    fn test_can_reconstruct_sequence_of_linear_decreasing_values_within_absolute_error_bound() {
        let mut values: Vec<Value> = (42..=4200).step_by(42).map(|value| value as f32).collect();
        values.reverse();

        test_can_reconstruct_sequence_of_linear_values_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            values,
        );
    }

    #[test]
    fn test_can_reconstruct_sequence_of_linear_decreasing_values_within_relative_error_bound() {
        let mut values: Vec<Value> = (42..=4200).step_by(42).map(|value| value as f32).collect();
        values.reverse();

        test_can_reconstruct_sequence_of_linear_values_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            values,
        );
    }

    fn test_can_reconstruct_sequence_of_linear_values_within_error_bound(
        error_bound: ErrorBound,
        values: Vec<Value>,
    ) {
        // Fit model of type Swing to perfectly linear sequence.
        let end_time = START_TIME + values.len() as i64 * SAMPLING_INTERVAL;
        let timestamps = TimestampArray::from_iter_values(
            (START_TIME..end_time).step_by(SAMPLING_INTERVAL as usize),
        );
        let values = ValueArray::from_iter_values(values);
        let segments = crate::try_compress(1, error_bound, &timestamps, &values).unwrap();

        // Extract the individual columns from the record batch.
        modelardb_common::arrays!(
            segments,
            _univariate_id_array,
            model_type_id_array,
            start_time_array,
            end_time_array,
            timestamps_array,
            min_value_array,
            max_value_array,
            values_array,
            residuals_array,
            _error_array
        );

        // Verify that one model of type Swing was used.
        assert_eq!(segments.num_rows(), 1);
        assert_eq!(model_type_id_array.value(0), SWING_ID);

        // Reconstruct all values from the segment.
        let mut reconstructed_ids = UnivariateIdBuilder::with_capacity(timestamps.len());
        let mut reconstructed_timestamps = TimestampBuilder::with_capacity(timestamps.len());
        let mut reconstructed_values = ValueBuilder::with_capacity(timestamps.len());

        models::grid(
            0,
            model_type_id_array.value(0),
            start_time_array.value(0),
            end_time_array.value(0),
            timestamps_array.value(0),
            min_value_array.value(0),
            max_value_array.value(0),
            values_array.value(0),
            residuals_array.value(0),
            &mut reconstructed_ids,
            &mut reconstructed_timestamps,
            &mut reconstructed_values,
        );

        assert_eq!(timestamps, reconstructed_timestamps.finish());
        assert_eq!(values, reconstructed_values.finish());
    }
}
