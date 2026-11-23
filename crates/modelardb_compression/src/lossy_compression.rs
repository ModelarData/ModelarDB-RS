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

/// Rewrite the values in each column of `record_batch` that has a lossy error bound in
/// `table_metadata`, so they compress better while staying within the error bound. `record_batch`
/// must contain the stored columns of the table. It must only be applied once to each value, since
/// applying it again can rewrite more bits. If no column has a lossy error bound, `record_batch` is
/// returned unchanged. If `record_batch` does not match the stored columns of the table,
/// [`ModelarDbCompressionError`] is returned.
pub fn apply_error_bounds(
    table_metadata: &TableMetadata,
    record_batch: RecordBatch,
) -> Result<RecordBatch> {
    let number_of_stored_columns = table_metadata.stored_schema.fields().len();

    if record_batch.num_columns() != number_of_stored_columns {
        return Err(ModelarDbCompressionError::InvalidArgument(format!(
            "The record batch has {} columns but the table '{}' stores {number_of_stored_columns} columns.",
            record_batch.num_columns(),
            table_metadata.name
        )));
    }

    // Error bounds are only defined for stored columns, so they are in the same order as the
    // columns in record_batch when the generated columns are skipped.
    let stored_error_bounds = table_metadata
        .column_metadata
        .iter()
        .filter_map(|column_metadata| match column_metadata {
            ColumnMetadata::Stored(error_bound) => Some(*error_bound),
            ColumnMetadata::Generated(_) => None,
        });

    // The columns are only copied if at least one of them has a lossy error bound.
    let mut maybe_columns: Option<Vec<ArrayRef>> = None;

    for (index, error_bound) in stored_error_bounds.enumerate() {
        if error_bound == ErrorBound::Lossless {
            continue;
        }

        let Some(values) = record_batch.column(index).as_primitive_opt::<ArrowValue>() else {
            return Err(ModelarDbCompressionError::InvalidArgument(format!(
                "The column '{}' has an error bound but is not of type Float32.",
                record_batch.schema().field(index).name()
            )));
        };

        let rewritten_values: ValueArray =
            values.unary(|value| rewrite_least_mantissa_bits(error_bound, value));

        // maybe_columns is only set if at least one of the columns has a lossy error bound.
        maybe_columns.get_or_insert_with(|| record_batch.columns().to_vec())[index] =
            Arc::new(rewritten_values);
    }

    match maybe_columns {
        Some(columns) => Ok(RecordBatch::try_new(record_batch.schema(), columns)?),
        None => Ok(record_batch),
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
    use datafusion::common::ToDFSchema;
    use datafusion::prelude::col;
    use modelardb_types::types::{ArrowTimestamp, GeneratedColumn, TimestampArray};
    use proptest::{prop_assert, prop_assume, property_test};

    const TABLE_NAME: &str = "test_table";
    const ERROR_BOUND_ONE: f32 = 1.0;
    const ERROR_BOUND_TEN: f32 = 10.0;
    const ERROR_BOUND_ABSOLUTE_MAX: f32 = f32::MAX;
    const ERROR_BOUND_RELATIVE_MAX: f32 = 100.0;

    // The values have non-zero least significant mantissa bits, so the error bounds rewrite them.
    const VALUES: [Value; 3] = [100.12346, -37.65432, 2500.9875];

    // Tests for apply_error_bounds().
    #[test]
    fn test_apply_error_bounds_rewrites_each_lossy_column_with_its_error_bound() {
        let absolute_error_bound = ErrorBound::try_new_absolute(ERROR_BOUND_ONE).unwrap();
        let relative_error_bound = ErrorBound::try_new_relative(ERROR_BOUND_TEN).unwrap();
        let table_metadata = table_metadata(absolute_error_bound, relative_error_bound);
        let record_batch = stored_record_batch(&table_metadata);

        let rewritten_record_batch =
            apply_error_bounds(&table_metadata, record_batch.clone()).unwrap();

        assert_eq!(rewritten_record_batch.column(0), record_batch.column(0));
        assert_rewritten_with_error_bound(rewritten_record_batch.column(1), absolute_error_bound);
        assert_rewritten_with_error_bound(rewritten_record_batch.column(2), relative_error_bound);
    }

    fn assert_rewritten_with_error_bound(rewritten_values: &ArrayRef, error_bound: ErrorBound) {
        let expected_values = ValueArray::from_iter_values(
            VALUES
                .iter()
                .map(|value| rewrite_least_mantissa_bits(error_bound, *value)),
        );

        assert_eq!(
            rewritten_values.as_primitive::<ArrowValue>(),
            &expected_values
        );
        assert_ne!(expected_values.values().as_ref(), VALUES.as_slice());
    }

    #[test]
    fn test_apply_error_bounds_without_lossy_columns_returns_record_batch_unchanged() {
        let table_metadata = table_metadata(ErrorBound::Lossless, ErrorBound::Lossless);
        let record_batch = stored_record_batch(&table_metadata);

        let result = apply_error_bounds(&table_metadata, record_batch.clone()).unwrap();

        assert_eq!(result, record_batch);
    }

    #[test]
    fn test_cannot_apply_error_bounds_to_record_batch_with_generated_columns() {
        let error_bound = ErrorBound::try_new_absolute(ERROR_BOUND_ONE).unwrap();
        let table_metadata = table_metadata(error_bound, error_bound);

        let record_batch = RecordBatch::try_new(
            table_metadata.query_schema.clone(),
            vec![
                Arc::new(TimestampArray::from(vec![100, 200, 300])),
                Arc::new(ValueArray::from(VALUES.to_vec())),
                Arc::new(ValueArray::from(VALUES.to_vec())),
                Arc::new(ValueArray::from(VALUES.to_vec())),
            ],
        )
        .unwrap();

        let result = apply_error_bounds(&table_metadata, record_batch);

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: The record batch has 4 columns but the table 'test_table' stores 3 columns."
        );
    }

    fn table_metadata(
        absolute_value_error_bound: ErrorBound,
        relative_value_error_bound: ErrorBound,
    ) -> TableMetadata {
        let query_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("absolute_value", ArrowValue::DATA_TYPE, false),
            Field::new("generated_value", ArrowValue::DATA_TYPE, false),
            Field::new("relative_value", ArrowValue::DATA_TYPE, false),
        ]));

        let df_schema = query_schema.clone().to_dfschema().unwrap();
        let generated_value = GeneratedColumn::try_from_expr(
            col("absolute_value") + col("relative_value"),
            &df_schema,
        )
        .unwrap();

        TableMetadata::try_new(
            TABLE_NAME.to_owned(),
            query_schema,
            vec![
                ColumnMetadata::Stored(ErrorBound::Lossless),
                ColumnMetadata::Stored(absolute_value_error_bound),
                ColumnMetadata::Generated(generated_value),
                ColumnMetadata::Stored(relative_value_error_bound),
            ],
        )
        .unwrap()
    }

    fn stored_record_batch(table_metadata: &TableMetadata) -> RecordBatch {
        RecordBatch::try_new(
            table_metadata.stored_schema.clone(),
            vec![
                Arc::new(TimestampArray::from(vec![100, 200, 300])),
                Arc::new(ValueArray::from(VALUES.to_vec())),
                Arc::new(ValueArray::from(VALUES.to_vec())),
            ],
        )
        .unwrap()
    }

    // Tests for rewrite_least_mantissa_bits().
    #[test]
    fn test_rewrite_least_mantissa_bits_with_absolute_error_bound_below_value_precision() {
        // The error bound is smaller than the distance to the neighboring values of 2500000.0.
        let error_bound = ErrorBound::try_new_absolute(0.1).unwrap();
        assert_eq!(
            rewrite_least_mantissa_bits(error_bound, 2500000.0),
            2500000.0
        );
    }

    #[test]
    fn test_rewrite_least_mantissa_bits_with_absolute_error_bound_above_value() {
        // The error bound is larger than the value itself.
        let error_bound = ErrorBound::try_new_absolute(1.0).unwrap();
        assert_eq!(rewrite_least_mantissa_bits(error_bound, 1e-8), 1e-8);
    }

    #[test]
    fn test_rewrite_least_mantissa_bits_with_subnormal_value() {
        let value: Value = -9.948535e-39;
        assert!(value.is_subnormal());

        let error_bound = ErrorBound::try_new_relative(40.0).unwrap();
        assert_eq!(rewrite_least_mantissa_bits(error_bound, value), value);
    }

    #[property_test]
    fn test_rewrite_least_mantissa_bits_within_absolute_error_bound(
        value: Value,
        #[strategy = f32::MIN_POSITIVE..=f32::MAX] error_bound: f32,
    ) {
        assert_rewrite_least_mantissa_bits_within_error_bound(
            ErrorBound::try_new_absolute(error_bound).unwrap(),
            value,
        );
    }

    #[property_test]
    fn test_rewrite_least_mantissa_bits_within_relative_error_bound(
        value: Value,
        #[strategy = f32::MIN_POSITIVE..=100.0] error_bound: f32,
    ) {
        assert_rewrite_least_mantissa_bits_within_error_bound(
            ErrorBound::try_new_relative(error_bound).unwrap(),
            value,
        );
    }

    fn assert_rewrite_least_mantissa_bits_within_error_bound(
        error_bound: ErrorBound,
        value: Value,
    ) {
        let rewritten_value = rewrite_least_mantissa_bits(error_bound, value);

        assert!(is_value_within_error_bound(
            error_bound,
            value,
            rewritten_value
        ));
    }

    // Tests for is_value_within_error_bound().
    #[property_test]
    fn test_same_value_is_always_within_lossless_error_bound(value: Value) {
        prop_assert!(is_value_within_error_bound(
            ErrorBound::Lossless,
            value,
            value
        ));
    }

    #[property_test]
    fn test_other_value_is_never_within_absolute_error_bound_of_positive_infinity(value: Value) {
        prop_assume!(value != Value::INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap(),
            Value::INFINITY,
            value
        ));
    }

    #[property_test]
    fn test_other_value_is_never_within_relative_error_bound_of_positive_infinity(value: Value) {
        prop_assume!(value != Value::INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap(),
            Value::INFINITY,
            value
        ));
    }

    #[property_test]
    fn test_other_value_is_never_within_absolute_error_bound_of_negative_infinity(value: Value) {
        prop_assume!(value != Value::NEG_INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap(),
            Value::NEG_INFINITY,
            value
        ));
    }

    #[property_test]
    fn test_other_value_is_never_within_relative_error_bound_of_negative_infinity(value: Value) {
        prop_assume!(value != Value::NEG_INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap(),
            Value::NEG_INFINITY,
            value
        ));
    }

    #[property_test]
    fn test_other_value_is_never_within_absolute_error_bound_of_nan(value: Value) {
        prop_assume!(!value.is_nan());
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap(),
            Value::NAN,
            value
        ));
    }

    #[property_test]
    fn test_other_value_is_never_within_relative_error_bound_of_nan(value: Value) {
        prop_assume!(!value.is_nan());
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap(),
            Value::NAN,
            value
        ));
    }

    #[property_test]
    fn test_positive_infinity_is_never_within_absolute_error_bound_of_other_value(value: Value) {
        prop_assume!(value != Value::INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap(),
            value,
            Value::INFINITY
        ));
    }

    #[property_test]
    fn test_positive_infinity_is_never_within_relative_error_bound_of_other_value(value: Value) {
        prop_assume!(value != Value::INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap(),
            value,
            Value::INFINITY
        ));
    }

    #[property_test]
    fn test_negative_infinity_is_never_within_absolute_error_bound_of_other_value(value: Value) {
        prop_assume!(value != Value::NEG_INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap(),
            value,
            Value::NEG_INFINITY
        ));
    }

    #[property_test]
    fn test_negative_infinity_is_never_within_relative_error_bound_of_other_value(value: Value) {
        prop_assume!(value != Value::NEG_INFINITY);
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap(),
            value,
            Value::NEG_INFINITY
        ));
    }

    #[property_test]
    fn test_nan_is_never_within_absolute_error_bound_of_other_value(value: Value) {
        prop_assume!(!value.is_nan());
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap(),
            value,
            Value::NAN
        ));
    }

    #[property_test]
    fn test_nan_is_never_within_relative_error_bound_of_other_value(value: Value) {
        prop_assume!(!value.is_nan());
        prop_assert!(!is_value_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap(),
            value,
            Value::NAN
        ));
    }

    #[test]
    fn test_different_value_is_within_non_zero_absolute_error_bound() {
        assert!(is_value_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ONE).unwrap(),
            10.0,
            11.0
        ));
    }

    #[test]
    fn test_different_value_is_within_non_zero_relative_error_bound() {
        assert!(is_value_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_TEN).unwrap(),
            10.0,
            11.0
        ));
    }

    // Tests for equal_or_nan().
    #[property_test]
    fn test_equal_or_nan_equal(value: f64) {
        assert!(equal_or_nan(value, value));
    }

    #[property_test]
    fn test_equal_or_nan_not_equal(value_one: f64, value_two: f64) {
        prop_assume!(value_one != value_two && !value_one.is_nan() && !value_two.is_nan());
        prop_assert!(!equal_or_nan(value_one, value_two));
    }
}
