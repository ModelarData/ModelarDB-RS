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
//! as models and functions for efficient computation of aggregates for models
//! of type PMC-Mean and Swing. The module itself contains functionality used by
//! multiple of the model types.

mod gorilla;
mod pmcmean;
mod swing;

use std::cmp::{PartialOrd, Ordering};

use datafusion::arrow::array::{
    Float32Builder, Int32Array, Int32Builder, Int64Array, TimestampMillisecondBuilder,
};

use crate::errors::MiniModelarDBError;

/// General error bound that is guaranteed to not be negative, infinite, or NaN.
/// For `PMCMean` and `Swing` the error bound is interpreted as a relative per
/// value error bound in percentage. `Gorilla` is lossless.
struct ErrorBound(f32);

impl ErrorBound {

    /// Return `Self` if `error_bound` is a positive finite value, otherwise
    /// `CompressionError`.
    fn try_new(error_bound: f32) -> Result<Self, MiniModelarDBError> {
        if error_bound < 0.0 || error_bound.is_infinite() || error_bound.is_nan() {
            Err(MiniModelarDBError::CompressionError(
                "Error bound cannot be negative, infinite, or NaN".to_owned(),
            ))
        } else {
            Ok(Self(error_bound))
        }
    }
}

impl PartialEq<ErrorBound> for f32 {
    fn eq(&self, other: &ErrorBound) -> bool {
        self.eq(&other.0)
    }
}

impl PartialOrd<ErrorBound> for f32 {
    fn partial_cmp(&self, other: &ErrorBound) -> Option<Ordering> {
        self.partial_cmp(&other.0)
    }
}



// TODO: can mtid be converted to a model type enum without adding overhead?
pub fn grid(
    // TODO: support time series with different number of values and data types?
    // TODO: translate the gid to the proper tids to support groups.
    // TODO: can the tid be stored once per batch of data points from a model?
    gid: i32,
    start_time: i64,
    end_time: i64,
    mtid: i32,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
    tids: &mut Int32Builder,
    timestamps: &mut TimestampMillisecondBuilder,
    values: &mut Float32Builder,
) {
    match mtid {
        2 => pmcmean::grid(
            gid,
            start_time,
            end_time,
            sampling_interval,
            model,
            gaps,
            tids,
            timestamps,
            values,
        ),
        3 => swing::grid(
            gid,
            start_time,
            end_time,
            sampling_interval,
            model,
            gaps,
            tids,
            timestamps,
            values,
        ),
        4 => gorilla::grid(
            gid,
            start_time,
            end_time,
            sampling_interval,
            model,
            gaps,
            tids,
            timestamps,
            values,
        ),
        _ => panic!("unknown model type"),
    }
}

// TODO: refactor count to operate on values instead of arrays for consistency?
pub fn count(
    num_rows: usize,
    gids: &Int32Array,
    start_times: &Int64Array,
    end_times: &Int64Array,
    sampling_intervals: &Int32Array,
) -> usize {
    // Assumes all arrays are the same length and contain less or equal to num_rows elements.
    let mut data_points = 0;
    for row_index in 0..num_rows {
        let tid = gids.value(row_index) as usize;
        let sampling_interval = sampling_intervals.value(tid) as i64;
        data_points +=
            ((end_times.value(row_index) - start_times.value(row_index)) / sampling_interval) + 1;
    }
    data_points as usize
}

pub fn min(
    gid: i32,
    start_time: i64,
    end_time: i64,
    mtid: i32,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    match mtid {
        2 => pmcmean::min_max(gid, start_time, end_time, sampling_interval, model, gaps),
        3 => swing::min(gid, start_time, end_time, sampling_interval, model, gaps),
        4 => gorilla::min(gid, start_time, end_time, sampling_interval, model, gaps),
        _ => panic!("unknown model type"),
    }
}

pub fn max(
    gid: i32,
    start_time: i64,
    end_time: i64,
    mtid: i32,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    match mtid {
        2 => pmcmean::min_max(gid, start_time, end_time, sampling_interval, model, gaps),
        3 => swing::max(gid, start_time, end_time, sampling_interval, model, gaps),
        4 => gorilla::max(gid, start_time, end_time, sampling_interval, model, gaps),
        _ => panic!("unknown model type"),
    }
}

pub fn sum(
    gid: i32,
    start_time: i64,
    end_time: i64,
    mtid: i32,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    match mtid {
        2 => pmcmean::sum(gid, start_time, end_time, sampling_interval, model, gaps),
        3 => swing::sum(gid, start_time, end_time, sampling_interval, model, gaps),
        4 => gorilla::sum(gid, start_time, end_time, sampling_interval, model, gaps),
        _ => panic!("unknown model type"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::tests::ProptestValue;
    use proptest::{prop_assert, prop_assume, proptest};
    proptest! {
    #[test]
    fn test_error_bound_can_be_positive(error_bound in ProptestValue::POSITIVE) {
        assert!(ErrorBound::try_new(error_bound).is_ok())
    }

    #[test]
    fn test_error_bound_cannot_be_negative(error_bound in ProptestValue::NEGATIVE) {
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
}
