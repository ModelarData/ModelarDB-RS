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

//! Implementation of the model types used for compressing time series segments
//! as models and functions for efficiently computing aggregates from models of
//! each type. The module itself contains general functionality used by the
//! model types.

mod gorilla;
mod pmcmean;
mod swing;

use std::cmp::{Ordering, PartialOrd};

use datafusion::arrow::array::{Int32Array, Int64Array};

use crate::errors::MiniModelarDBError;
use crate::types::{
    TimeSeriesId, TimeSeriesIdArray, TimeSeriesIdBuilder, Timestamp, TimestampBuilder, Value,
    ValueBuilder,
};

/// Unique ids for each model type. Constant values are used instead of an enum
/// so the stored model type ids can be used in match expressions without being
/// converted to an enum first. Zero and one is not used for compatibility with
/// the legacy JVM-based version of [ModelarDB].
///
/// [ModelarDB]: https://github.com/ModelarData/ModelarDB
const PMC_MEAN_ID: u8 = 2;
const SWING_ID: u8 = 3;
const GORILLA_ID: u8 = 4;

/// General error bound that is guaranteed to not be negative, infinite, or NAN.
/// For `PMCMean` and `Swing` the error bound is interpreted as a relative per
/// value error bound in percentage, while `Gorilla` uses lossless compression.
struct ErrorBound(f32);

impl ErrorBound {
    /// Return `ErrorBound` if `error_bound` is a positive finite value,
    /// otherwise `CompressionError`.
    fn try_new(error_bound: f32) -> Result<Self, MiniModelarDBError> {
        if error_bound < 0.0 || error_bound.is_infinite() || error_bound.is_nan() {
            Err(MiniModelarDBError::CompressionError(
                "Error bound cannot be negative, infinite, or NAN.".to_owned(),
            ))
        } else {
            Ok(Self(error_bound))
        }
    }
}

/// Enable equal and not equal for `ErrorBound` and `f32`.
impl PartialEq<ErrorBound> for f32 {
    fn eq(&self, other: &ErrorBound) -> bool {
        self.eq(&other.0)
    }
}

/// Enable less than and greater than for `ErrorBound` and `f32`.
impl PartialOrd<ErrorBound> for f32 {
    fn partial_cmp(&self, other: &ErrorBound) -> Option<Ordering> {
        self.partial_cmp(&other.0)
    }
}

// TODO: rename `count` and return `Result` when refactoring query engine.
// TODO: replace Int64Array with TimestampArray when refactoring query engine.
// TODO: remove unused function parameters when refactoring query engine.
/// Compute the number of data points in a batch of time series segments.
/// `time_series_ids`, `start_times`, `end_times`, and `sampling_intervals` must
/// all contain at least `num_rows` elements.
pub fn count(
    num_rows: usize,
    time_series_ids: &TimeSeriesIdArray,
    start_times: &Int64Array,
    end_times: &Int64Array,
    sampling_intervals: &Int32Array,
) -> usize {
    let mut data_points = 0;
    for row_index in 0..num_rows {
        let time_series_id = time_series_ids.value(row_index) as usize;
        let sampling_interval = sampling_intervals.value(time_series_id);
        data_points += length(
            start_times.value(row_index),
            end_times.value(row_index),
            sampling_interval,
        );
    }
    data_points as usize
}

/// Compute the number of data points in a time series segment.
pub fn length(start_time: Timestamp, end_time: Timestamp, sampling_interval: i32) -> i64 {
    ((end_time - start_time) / sampling_interval as i64) + 1
}

/// Compute the minimum value for a time series segment whose values are
/// represented by a model.
pub fn min(
    _time_series_id: TimeSeriesId,
    start_time: Timestamp,
    end_time: Timestamp,
    model_type_id: i32,
    sampling_interval: i32,
    model: &[u8],
    _gaps: &[u8],
) -> Value {
    match model_type_id as u8 {
        PMC_MEAN_ID => pmcmean::min(model),
        SWING_ID => swing::min(start_time, end_time, model),
        GORILLA_ID => gorilla::min(start_time, end_time, sampling_interval, model),
        _ => panic!("Unknown model type."),
    }
}

/// Compute the maximum value for a time series segment whose values are
/// represented by a model.
pub fn max(
    _time_series_id: TimeSeriesId,
    start_time: Timestamp,
    end_time: Timestamp,
    model_type_id: i32,
    sampling_interval: i32,
    model: &[u8],
    _gaps: &[u8],
) -> Value {
    match model_type_id as u8 {
        PMC_MEAN_ID => pmcmean::max(model),
        SWING_ID => swing::max(start_time, end_time, model),
        GORILLA_ID => gorilla::max(start_time, end_time, sampling_interval, model),
        _ => panic!("Unknown model type."),
    }
}

/// Compute the sum of the values for a time series segment whose values are
/// represented by a model.
pub fn sum(
    _time_series_id: TimeSeriesId,
    start_time: Timestamp,
    end_time: Timestamp,
    model_type_id: i32,
    sampling_interval: i32,
    model: &[u8],
    _gaps: &[u8],
) -> Value {
    match model_type_id as u8 {
        PMC_MEAN_ID => pmcmean::sum(start_time, end_time, sampling_interval, model),
        SWING_ID => swing::sum(start_time, end_time, sampling_interval, model),
        GORILLA_ID => gorilla::sum(start_time, end_time, sampling_interval, model),
        _ => panic!("Unknown model type."),
    }
}

/// Reconstruct the data points for a time series segment whose values are
/// represented by a model. Each data point is split into its three components
/// and appended to `time_series_ids`, `timestamps`, and `values`.
pub fn grid(
    time_series_id: TimeSeriesId,
    start_time: Timestamp,
    end_time: Timestamp,
    model_type_id: i32,
    sampling_interval: i32,
    model: &[u8],
    _gaps: &[u8],
    time_series_ids: &mut TimeSeriesIdBuilder,
    timestamps: &mut TimestampBuilder,
    values: &mut ValueBuilder,
) {
    match model_type_id as u8 {
        PMC_MEAN_ID => pmcmean::grid(
            time_series_id,
            start_time,
            end_time,
            sampling_interval,
            model,
            time_series_ids,
            timestamps,
            values,
        ),
        SWING_ID => swing::grid(
            time_series_id,
            start_time,
            end_time,
            sampling_interval,
            model,
            time_series_ids,
            timestamps,
            values,
        ),
        GORILLA_ID => gorilla::grid(
            time_series_id,
            start_time,
            end_time,
            sampling_interval,
            model,
            time_series_ids,
            timestamps,
            values,
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
    use proptest::num;
    use proptest::{prop_assert, prop_assume, proptest};

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

    // Tests for length().
    #[test]
    fn test_length_of_segment_with_one_data_point() {
        assert_eq!(1, length(1658671178037, 1658671178037, 1000));
    }

    #[test]
    fn test_length_of_segment_with_ten_data_points() {
        assert_eq!(10, length(1658671178037, 1658671187037, 1000));
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
