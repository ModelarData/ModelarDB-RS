/* Copyright 2026 The ModelarDB Contributors
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

//! Error-bounded lossy compression of floating-point values. Before values are stored, as many of
//! their least significant mantissa bits as possible within the error bound are rewritten to zero,
//! so the values are encoded more compactly in Apache Parquet files.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, RecordBatch};
use modelardb_types::types::{
    ArrowValue, ColumnMetadata, ErrorBound, TableMetadata, Value, ValueArray,
};

use crate::error::{ModelarDbCompressionError, Result};
/// Rewrite the highest number of mantissa bits possible for `value` within `error_bound` starting
/// from the least significant bits. Zero, subnormal, infinite, and NaN values are returned
/// unchanged.
fn rewrite_least_mantissa_bits(error_bound: ErrorBound, value: Value) -> Value {
    // Subnormal values have no implicit leading mantissa bit, so the computation below does
    // not bound their error.
    if !value.is_normal() {
        return value;
    }

    let value_as_u32 = value.to_bits();
    let abs_error_bound = maximum_allowed_deviation(error_bound, value as f64) as f32;
    let exponent = get_exponent(value);
    let factorized_epsilon = abs_error_bound / 2f32.powi(exponent);

    // Rewriting the last 23 - ⌈|log2 factorized_epsilon|⌉ mantissa bits never exceeds the error
    // bound. However, in that case, one more bit can be rewritten if the majority of the least
    // significant mantissa bits are 0. Thus, we rewrite bits using 23 - ⌊|log2 factorized_epsilon|⌋
    // and perform an extra check to ensure the error bound is not exceeded. If the error bound is
    // exceeded, we rewrite 23 - ⌈|log2 factorized_epsilon|⌉ least significant mantissa bits. Error
    // bounds far below or far above the value give positions outside the mantissa, so the position
    // is clamped to it.
    let mut rewrite_position = (23 - factorized_epsilon.log2().abs().floor() as i32).clamp(0, 23);
    let mut rewritten_value = f32::from_bits(rewrite_bits_by_n(value_as_u32, rewrite_position));

    // If the error bound is exceeded, value is rewritten with one less bit i.e., using
    // 23 − ⌈log2 factorized_epsilon⌉.
    if !is_value_within_error_bound(error_bound, value, rewritten_value) {
        rewrite_position -= 1;
        rewritten_value = f32::from_bits(rewrite_bits_by_n(value.to_bits(), rewrite_position));
    }

    rewritten_value
}

/// Extract the unbiased exponent from `value`.
fn get_exponent(value: f32) -> i32 {
    let n_bits: u32 = value.to_bits();
    let biased_exponent = ((n_bits >> 23) & 0xff) as i32;
    biased_exponent - 127
}

/// Left shift `bits_to_rewrite` by `positions_to_shift`.
fn rewrite_bits_by_n(bits_to_rewrite: u32, positions_to_shift: i32) -> u32 {
    let mask = u32::MAX << positions_to_shift;
    bits_to_rewrite & mask
}

/// Determine if `approximate_value` is within `error_bound` of `real_value`.
pub fn is_value_within_error_bound(
    error_bound: ErrorBound,
    real_value: Value,
    approximate_value: Value,
) -> bool {
    match error_bound {
        ErrorBound::Absolute(error_bound) => {
            // Needed to allow +INFINITY, -INFINITY, and NAN values to be stored losslessly.
            if equal_or_nan(real_value as f64, approximate_value as f64) {
                true
            } else {
                Value::abs(real_value - approximate_value) <= error_bound
            }
        }
        ErrorBound::Relative(error_bound) => {
            // Needed because result becomes NAN and approximate_value is rejected if
            // approximate_value and real_value are zero, and because NAN != NAN.
            if equal_or_nan(real_value as f64, approximate_value as f64) {
                true
            } else {
                let difference = real_value - approximate_value;
                let result = Value::abs(difference / real_value);
                (result * 100.0) <= error_bound
            }
        }
        ErrorBound::Lossless => equal_or_nan(real_value as f64, approximate_value as f64),
    }
}

/// Compute the maximum allowed deviation from `value` within `error bound`.
fn maximum_allowed_deviation(error_bound: ErrorBound, value: f64) -> f64 {
    // The allowed deviation is lower than the bound to account for inaccurate floating point math.
    match error_bound {
        ErrorBound::Absolute(error_bound) => error_bound as f64 * 0.99,
        ErrorBound::Relative(error_bound) => f64::abs(value * (error_bound as f64 / 100.1)),
        ErrorBound::Lossless => 0.0,
    }
}

/// Returns true if `v1` and `v2` are equivalent or both values are NAN.
fn equal_or_nan(v1: f64, v2: f64) -> bool {
    v1 == v2 || (v1.is_nan() && v2.is_nan())
}
