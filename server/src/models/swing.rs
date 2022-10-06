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

use crate::models::ErrorBound;
use crate::models::{self, timestamps};
use crate::types::{TimeSeriesId, TimeSeriesIdBuilder, Timestamp, Value, ValueBuilder};

/// The state the Swing model type needs while fitting a model to a time series
/// segment.
pub struct Swing {
    /// Maximum relative error for the value of each data point.
    error_bound: ErrorBound,
    /// Time at which the first value represented by the current model was
    /// collected.
    first_timestamp: Timestamp,
    /// Time at which the last value represented by the current model was
    /// collected.
    last_timestamp: Timestamp,
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
            first_timestamp: 0,
            last_timestamp: 0,
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
        let error_bound = self.error_bound.0 as f64;

        // Compute the maximum allowed deviation within the error bound. The
        // error bound in percentage is divided by 100.1 instead of 100.0 to
        // ensure that the approximate value is below the error bound despite
        // calculations with floating-point values not being fully accurate.
        let maximum_deviation = f64::abs(value * (error_bound / 100.1));

        if self.length == 0 {
            // Line 1 - 2 of Algorithm 1 in the Swing and Slide paper.
            self.first_timestamp = timestamp;
            self.last_timestamp = timestamp;
            self.first_value = value;
            self.length += 1;
            true
        } else if !self.first_value.is_finite() || !value.is_finite() {
            // Extend Swing to handle both types of infinity and NAN.
            if models::equal_or_nan(self.first_value, value) {
                self.last_timestamp = timestamp;
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
            self.last_timestamp = timestamp;
            (self.upper_bound_slope, self.upper_bound_intercept) = compute_slope_and_intercept(
                self.first_timestamp,
                self.first_value,
                timestamp,
                value + maximum_deviation,
            );

            (self.lower_bound_slope, self.lower_bound_intercept) = compute_slope_and_intercept(
                self.first_timestamp,
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
                self.last_timestamp = timestamp;

                // Line 17 of Algorithm 1 in the Swing and Slide paper.
                if upper_bound_approximate_value - maximum_deviation > value {
                    (self.upper_bound_slope, self.upper_bound_intercept) =
                        compute_slope_and_intercept(
                            self.first_timestamp,
                            self.first_value,
                            timestamp,
                            value + maximum_deviation,
                        );
                }

                // Line 15 of Algorithm 1 in the Swing and Slide paper.
                if lower_bound_approximate_value + maximum_deviation < value {
                    (self.lower_bound_slope, self.lower_bound_intercept) =
                        compute_slope_and_intercept(
                            self.first_timestamp,
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
    pub fn get_length(&self) -> usize {
        self.length
    }

    /// Return the number of bytes the current model uses per data point on average.
    pub fn get_bytes_per_value(&self) -> f32 {
        (2.0 * models::VALUE_SIZE_IN_BYTES as f32) / self.length as f32
    }

    /// Return the current model. For a model of type Swing, the first and last
    /// values of the time series segment the model represents are returned. The
    /// two values are returned instead of the slope and intercept as the values
    /// only require `size_of::<Value>` while the slope and intercept generally
    /// must be [`f64`] to be precise enough.
    pub fn get_model(&self) -> (Value, Value) {
        // TODO: Use the method in the Slide and Swing paper to select the
        // linear function within the lower and upper that minimizes error
        let first_value =
            self.upper_bound_slope * self.first_timestamp as f64 + self.upper_bound_intercept;
        let last_value =
            self.upper_bound_slope * self.last_timestamp as f64 + self.upper_bound_intercept;
        (first_value as Value, last_value as Value)
    }
}

/// Compute the sum of the values for a time series segment whose values are
/// represented by a model of type Swing.
pub fn sum(
    start_time: Timestamp,
    end_time: Timestamp,
    timestamps: &[u8],
    min_value: Value,
    max_value: Value,
    values: &[u8],
) -> Value {
    let (slope, intercept) =
        decode_and_compute_slope_and_intercept(start_time, end_time, min_value, max_value, values);

    if timestamps::are_compressed_timestamps_regular(timestamps) {
        let first = slope * start_time as f64 + intercept;
        let last = slope * end_time as f64 + intercept;
        let average = (first + last) / 2.0;
        let length = models::length(start_time, end_time, timestamps);
        (average * length as f64) as Value
    } else {
        let mut sum: f64 = 0.0;
        for timestamp in timestamps {
            sum += slope * (*timestamp as f64) + intercept;
        }
        sum as Value
    }
}

/// Reconstruct the values for the `timestamps` without matching values in
/// `value_builder` using a model of type Swing. The `time_series_ids` and
/// `values` are appended to `time_series_id_builder` and `value_builder`.
pub fn grid(
    time_series_id: TimeSeriesId,
    start_time: Timestamp,
    end_time: Timestamp,
    min_value: Value,
    max_value: Value,
    values: &[u8],
    time_series_ids: &mut TimeSeriesIdBuilder,
    timestamps: &[Timestamp],
    value_builder: &mut ValueBuilder,
) {
    let (slope, intercept) =
        decode_and_compute_slope_and_intercept(start_time, end_time, min_value, max_value, values);

    for timestamp in timestamps {
        time_series_ids.append_value(time_series_id);
        let value = (slope * (*timestamp as f64) + intercept) as Value;
        value_builder.append_value(value);
    }
}

/// Decode `values` to determine how `min_value` and `max_value` maps to the
/// segments first value and final value. Then compute the slope and intercept
/// of a linear function that intersects with the data points
/// (`first_timestamp`, first value) and (`final_timestamp`, final value).
fn decode_and_compute_slope_and_intercept(
    first_timestamp: Timestamp,
    final_timestamp: Timestamp,
    min_value: Value,
    max_value: Value,
    values: &[u8],
) -> (f64, f64) {
    let min_value = min_value as f64;
    let max_value = max_value as f64;

    // Check if min value is the first value and max value is the final value.
    if values[0] == 1 {
        compute_slope_and_intercept(first_timestamp, min_value, final_timestamp, max_value)
    } else {
        compute_slope_and_intercept(first_timestamp, max_value, final_timestamp, min_value)
    }
}

/// Compute the slope and intercept of a linear function that intersects with
/// the data points (`first_timestamp`, `first_value`) and (`final_timestamp`,
/// `final_value`).
fn compute_slope_and_intercept(
    first_timestamp: Timestamp,
    first_value: f64,
    final_timestamp: Timestamp,
    final_value: f64,
) -> (f64, f64) {
    // An if expression is used as it seems impossible to calculate the slope
    // and intercept without creating INFINITY, NEG_INFINITY, or NaN values.
    if models::equal_or_nan(first_value, final_value) {
        (0.0, first_value)
    } else {
        debug_assert!(first_value.is_finite(), "First value is not finite.");
        debug_assert!(final_value.is_finite(), "Second value is not finite.");
        let slope = (final_value - first_value) / (final_timestamp - first_timestamp) as f64;
        let intercept = first_value - slope * first_timestamp as f64;
        (slope, intercept)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BinaryArray, Float32Array, UInt8Array};
    use proptest::strategy::Strategy;
    use proptest::{num, prop_assert, prop_assert_eq, prop_assume, proptest};

    use crate::compression;
    use crate::models::SWING_ID;
    use crate::types::{
        tests::ProptestValue, TimestampArray, TimestampBuilder, ValueArray, ValueBuilder,
    };

    // Tests constants chosen to be realistic while minimizing the testing time.
    const SAMPLING_INTERVAL: Timestamp = 1000;
    const FIRST_TIMESTAMP: Timestamp = 1658671178037;
    const FINAL_TIMESTAMP: Timestamp = FIRST_TIMESTAMP + SAMPLING_INTERVAL;
    const SEGMENT_LENGTH: Timestamp = 5; // Timestamp is used to remove casts.

    // Tests for Swing.
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
        let mut model_type = Swing::new(error_bound_zero);
        let final_timestamp = FIRST_TIMESTAMP + SEGMENT_LENGTH * SAMPLING_INTERVAL;
        for timestamp in (FIRST_TIMESTAMP..final_timestamp).step_by(SAMPLING_INTERVAL as usize) {
            assert!(model_type.fit_data_point(timestamp, value));
        }

        let (first_value, final_value) = model_type.get_model();
        let (slope, intercept) = compute_slope_and_intercept(
            FIRST_TIMESTAMP,
            first_value as f64,
            final_timestamp,
            final_value as f64,
        );
        if value.is_nan() {
            assert!(slope == 0.0 && intercept.is_nan());
        } else {
            for timestamp in (FIRST_TIMESTAMP..final_timestamp).step_by(SAMPLING_INTERVAL as usize)
            {
                let approximate_value = slope * timestamp as f64 + intercept;
                assert_eq!(approximate_value, value as f64);
            }
        }
    }

    proptest! {
    #[test]
    fn test_can_fit_one_value(value in ProptestValue::ANY) {
        let error_bound_zero = ErrorBound::try_new(0.0).unwrap();
        prop_assert!(Swing::new(error_bound_zero).fit_data_point(FIRST_TIMESTAMP, value));
    }

    #[test]
    fn test_can_fit_two_finite_value(
        first_value in ProptestValue::NORMAL,
        second_value in ProptestValue::NORMAL
    ) {
        let error_bound_zero = ErrorBound::try_new(0.0).unwrap();
        let mut model_type = Swing::new(error_bound_zero);
        prop_assert!(model_type.fit_data_point(FIRST_TIMESTAMP, first_value));
        prop_assert!(model_type.fit_data_point(FINAL_TIMESTAMP, second_value));
    }

    #[test]
    fn test_cannot_fit_other_value_and_positive_infinity(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new(Value::MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(FIRST_TIMESTAMP, value));
        prop_assert!(!model_type.fit_data_point(FINAL_TIMESTAMP, Value::INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_negative_infinity(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new(Value::MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(FIRST_TIMESTAMP, value));
        prop_assert!(!model_type.fit_data_point(FINAL_TIMESTAMP, Value::NEG_INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_nan(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new(Value::MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(FIRST_TIMESTAMP, value));
        prop_assert!(!model_type.fit_data_point(FINAL_TIMESTAMP, Value::NAN));
    }

    #[test]
    fn test_cannot_fit_positive_infinity_and_other_value(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new(Value::MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(FIRST_TIMESTAMP, Value::INFINITY));
        prop_assert!(!model_type.fit_data_point(FINAL_TIMESTAMP, value));
    }

    #[test]
    fn test_cannot_fit_negative_infinity_and_other_value(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new(Value::MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(FIRST_TIMESTAMP, Value::NEG_INFINITY));
        prop_assert!(!model_type.fit_data_point(FINAL_TIMESTAMP, value));
    }

    #[test]
    fn test_cannot_fit_nan_and_other_value(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new(Value::MAX).unwrap();
        let mut model_type = Swing::new(error_bound_max);
        prop_assert!(model_type.fit_data_point(FIRST_TIMESTAMP, Value::NAN));
        prop_assert!(!model_type.fit_data_point(FINAL_TIMESTAMP, value));
    }
    }

    #[test]
    fn test_can_fit_sequence_of_linear_values_with_error_bound_zero() {
        assert!(fit_sequence_of_values_with_error_bound(
            &[42.0, 84.0, 126.0, 168.0, 210.0],
            0.0,
        ))
    }

    #[test]
    fn test_cannot_fit_sequence_of_different_values_with_error_bound_zero() {
        assert!(!fit_sequence_of_values_with_error_bound(
            &[42.0, 42.0, 42.8, 42.0, 42.0],
            0.0,
        ))
    }

    #[test]
    fn test_can_fit_sequence_of_different_values_with_error_bound_five() {
        assert!(fit_sequence_of_values_with_error_bound(
            &[42.0, 42.0, 42.8, 42.0, 42.0],
            5.0,
        ))
    }

    fn fit_sequence_of_values_with_error_bound(values: &[Value], error_bound: Value) -> bool {
        let error_bound = ErrorBound::try_new(error_bound).unwrap();
        let mut model_type = Swing::new(error_bound);
        let mut fit_all_values = true;
        let mut timestamp = FIRST_TIMESTAMP;
        for value in values {
            fit_all_values &= model_type.fit_data_point(timestamp, *value);
            timestamp += SAMPLING_INTERVAL;
        }
        return fit_all_values;
    }

    // Tests for sum().
    proptest! {
    #[test]
    fn test_sum(
        first_value in num::i32::ANY.prop_map(i32_to_value),
        final_value in num::i32::ANY.prop_map(i32_to_value),
    ) {
        let min_value = f32::min(first_value, final_value);
        let max_value = f32::max(first_value, final_value);
        let value = (min_value < max_value) as u8;

        let sum = sum(FIRST_TIMESTAMP, FINAL_TIMESTAMP, &[], min_value, max_value, &[value]);
        prop_assert_eq!(sum, first_value + final_value);
    }
    }

    // Tests for grid().
    proptest! {
    #[test]
    fn test_grid(value in num::i32::ANY.prop_map(i32_to_value)) {
        let timestamps: Vec<Timestamp> = (FIRST_TIMESTAMP ..= FINAL_TIMESTAMP)
            .step_by(SAMPLING_INTERVAL as usize).collect();
        let mut time_series_ids = TimeSeriesIdBuilder::with_capacity(timestamps.len());
        let mut values = ValueBuilder::with_capacity(timestamps.len());

        // The linear function represents a constant to have a known value.
        grid(
            1,
            FIRST_TIMESTAMP,
            FINAL_TIMESTAMP,
            value,
            value,
            &[0],
            &mut time_series_ids,
            &timestamps,
            &mut values,
        );

        let time_series_ids = time_series_ids.finish();
        let values = values.finish();

        prop_assert!(
            time_series_ids.len() == timestamps.len()
            && time_series_ids.len() == values.len()
        );
        prop_assert!(time_series_ids
             .iter()
             .all(|time_series_id_option| time_series_id_option.unwrap() == 1));
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
    fn test_can_reconstruct_sequence_of_linear_increasing_values_within_error_bound_zero() {
        test_can_reconstruct_sequence_of_linear_values_within_error_bound_zero(vec![
            42.0, 84.0, 126.0, 168.0, 210.0,
        ])
    }

    #[test]
    fn test_can_reconstruct_sequence_of_linear_decreasing_values_within_error_bound_zero() {
        test_can_reconstruct_sequence_of_linear_values_within_error_bound_zero(vec![
            210.0, 168.0, 126.0, 84.0, 42.0,
        ])
    }

    fn test_can_reconstruct_sequence_of_linear_values_within_error_bound_zero(values: Vec<Value>) {
        // Fit model of type Swing to perfectly linear sequence.
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let final_timestamp = FIRST_TIMESTAMP + values.len() as i64 * SAMPLING_INTERVAL;
        let timestamps = TimestampArray::from_iter_values(
            (FIRST_TIMESTAMP..final_timestamp).step_by(SAMPLING_INTERVAL as usize),
        );
        let values = ValueArray::from_iter_values(values);
        let segments = compression::try_compress(&timestamps, &values, error_bound).unwrap();

        // Extract the individual columns from the record batch.
        crate::get_arrays!(
            segments,
            model_type_id_array,
            timestamps_array,
            start_time_array,
            end_time_array,
            values_array,
            min_value_array,
            max_value_array,
            _error_array
        );

        // Verify that one model of type Swing was used.
        assert_eq!(segments.num_rows(), 1);
        assert_eq!(model_type_id_array.value(0), SWING_ID);

        // Reconstruct all values from the segment.
        let mut reconstructed_ids = TimeSeriesIdBuilder::with_capacity(timestamps.len());
        let mut reconstructed_timestamps = TimestampBuilder::with_capacity(timestamps.len());
        let mut reconstructed_values = ValueBuilder::with_capacity(timestamps.len());

        models::grid(
            0,
            model_type_id_array.value(0),
            timestamps_array.value(0),
            start_time_array.value(0),
            end_time_array.value(0),
            values_array.value(0),
            min_value_array.value(0),
            max_value_array.value(0),
            &mut reconstructed_ids,
            &mut reconstructed_timestamps,
            &mut reconstructed_values,
        );

        assert_eq!(timestamps, reconstructed_timestamps.finish());
        assert_eq!(values, reconstructed_values.finish());
    }
}
