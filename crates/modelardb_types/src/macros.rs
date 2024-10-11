/* Copyright 2022 The ModelarDB Contributors
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

/// Extract an [`array`](arrow::array::Array) from a
/// [`RecordBatch`](arrow::record_batch::RecordBatch) and cast it to the specified type:
///
/// ```
/// # use std::sync::Arc;
/// #
/// # use arrow::record_batch::RecordBatch;
/// # use modelardb_types::schemas::UNCOMPRESSED_SCHEMA;
/// # use modelardb_types::types::{Timestamp, TimestampArray, Value, ValueArray};
/// #
/// # let record_batch = RecordBatch::try_new(
/// #     UNCOMPRESSED_SCHEMA.0.clone(),
/// #     vec![
/// #         Arc::new(TimestampArray::from(Vec::<Timestamp>::new())),
/// #         Arc::new(ValueArray::from(Vec::<Value>::new())),
/// #     ],
/// # ).unwrap();
/// let array = modelardb_types::array!(record_batch, 0, TimestampArray);
/// ```
///
/// # Panics
///
/// Panics if `column` is not in `batch` or if it cannot be cast to `type`.
#[macro_export]
macro_rules! array {
    ($batch:ident, $column:expr, $type:ident) => {
        $batch
            .column($column)
            .as_any()
            .downcast_ref::<$type>()
            .unwrap()
    };
}

/// Extract the [`arrays`](arrow::array::Array) required to execute queries against a model table
/// from a [`RecordBatch`](arrow::record_batch::RecordBatch), cast them to the required type, and
/// assign the resulting arrays to the specified variables:
///
/// ```
/// # use std::sync::Arc;
/// #
/// # use arrow::array::{BinaryArray, Float32Array, UInt8Array, UInt16Array, UInt64Array};
/// # use arrow::record_batch::RecordBatch;
/// # use modelardb_types::schemas::COMPRESSED_SCHEMA;
/// # use modelardb_types::types::{Timestamp, TimestampArray, Value, ValueArray};
/// #
/// # let record_batch = RecordBatch::try_new(
/// #     COMPRESSED_SCHEMA.0.clone(),
/// #     vec![
/// #         Arc::new(UInt64Array::from(Vec::<u64>::new())),
/// #         Arc::new(UInt8Array::from(Vec::<u8>::new())),
/// #         Arc::new(TimestampArray::from(Vec::<Timestamp>::new())),
/// #         Arc::new(TimestampArray::from(Vec::<Timestamp>::new())),
/// #         Arc::new(BinaryArray::from(Vec::<&[u8]>::new())),
/// #         Arc::new(ValueArray::from(Vec::<Value>::new())),
/// #         Arc::new(ValueArray::from(Vec::<Value>::new())),
/// #         Arc::new(BinaryArray::from(Vec::<&[u8]>::new())),
/// #         Arc::new(BinaryArray::from(Vec::<&[u8]>::new())),
/// #         Arc::new(Float32Array::from(Vec::<f32>::new())),
/// #         Arc::new(UInt16Array::from(Vec::<u16>::new())),
/// #     ],
/// # ).unwrap();
/// modelardb_types::arrays!(record_batch, field_columns, univariate_ids, model_type_ids,
/// start_times, end_times, timestamps, min_values, max_values, values, residuals, errors);
/// ```
///
/// # Panics
///
/// Panics if `batch` does not contain ten columns of type UInt64Array, UInt8Array, TimestampArray,
/// TimestampArray, BinaryArray, ValueArray, ValueArray, BinaryArray, BinaryArray, and Float32Array
/// or eleven columns of type UInt64Array, UInt8Array, TimestampArray, TimestampArray,
/// BinaryArray, ValueArray, ValueArray, BinaryArray, BinaryArray, Float32Array, and UInt16Array.
#[macro_export]
macro_rules! arrays {
    ($batch:ident, $univariate_ids:ident, $model_type_ids:ident, $start_times:ident, $end_times:ident, $timestamps:ident, $min_values:ident, $max_values:ident, $values:ident, $residuals:ident, $errors:ident) => {
        let $univariate_ids = $crate::array!($batch, 0, UInt64Array);
        let $model_type_ids = $crate::array!($batch, 1, UInt8Array);
        let $start_times = $crate::array!($batch, 2, TimestampArray);
        let $end_times = $crate::array!($batch, 3, TimestampArray);
        let $timestamps = $crate::array!($batch, 4, BinaryArray);
        let $min_values = $crate::array!($batch, 5, ValueArray);
        let $max_values = $crate::array!($batch, 6, ValueArray);
        let $values = $crate::array!($batch, 7, BinaryArray);
        let $residuals = $crate::array!($batch, 8, BinaryArray);
        let $errors = $crate::array!($batch, 9, Float32Array);
    };
    ($batch:ident, $univariate_ids:ident, $model_type_ids:ident, $start_times:ident, $end_times:ident, $timestamps:ident, $min_values:ident, $max_values:ident, $values:ident, $residuals:ident, $errors:ident, $field_columns:ident) => {
        let $univariate_ids = $crate::array!($batch, 0, UInt64Array);
        let $model_type_ids = $crate::array!($batch, 1, UInt8Array);
        let $start_times = $crate::array!($batch, 2, TimestampArray);
        let $end_times = $crate::array!($batch, 3, TimestampArray);
        let $timestamps = $crate::array!($batch, 4, BinaryArray);
        let $min_values = $crate::array!($batch, 5, ValueArray);
        let $max_values = $crate::array!($batch, 6, ValueArray);
        let $values = $crate::array!($batch, 7, BinaryArray);
        let $residuals = $crate::array!($batch, 8, BinaryArray);
        let $errors = $crate::array!($batch, 9, Float32Array);
        let $field_columns = $crate::array!($batch, 10, UInt16Array);
    };
}
