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

/// Extract an [`array`](datafusion::arrow::array::Array) from a
/// [`RecordBatch`](datafusion::arrow::record_batch::RecordBatch) and cast it to the specified type:
///
/// ```
/// let array = crate::array!(record_batch, 0, UInt8Array);
/// ```
///
/// # Panics
///
/// Panics if `column` is not in `batch` or if it cannot be cast to `type`.
#[macro_export]
macro_rules! array {
    ($batch:ident, $column:literal, $type:ident) => {
        $batch
            .column($column)
            .as_any()
            .downcast_ref::<$type>()
            .unwrap()
    };
}

/// Extract the [`arrays`](datafusion::arrow::array::Array) required to execute queries against a
/// model table from a [`RecordBatch`](datafusion::arrow::record_batch::RecordBatch), cast them to
/// the required type, and assign the resulting arrays to the specified variables:
///
/// ```
/// crate::downcast_arrays!(batch, univariate_ids, model_type_ids, start_times, end_times,
/// timestamps, min_values, max_values, values, errors);
/// ```
///
/// # Panics
///
/// Panics if `batch` does not contain nine columns or if the columns are not UInt64Array,
/// UInt8Array, TimestampArray, TimestampArray, BinaryArray, ValueArray, ValueArray, BinaryArray,
/// and Float32Array.
#[macro_export]
macro_rules! arrays {
    ($batch:ident, $univariate_ids:ident, $model_type_ids:ident, $start_times:ident, $end_times:ident, $timestamps:ident, $min_values:ident, $max_values:ident, $values:ident, $errors:ident) => {
        let $univariate_ids = $crate::array!($batch, 0, UInt64Array);
        let $model_type_ids = $crate::array!($batch, 1, UInt8Array);
        let $start_times = $crate::array!($batch, 2, TimestampArray);
        let $end_times = $crate::array!($batch, 3, TimestampArray);
        let $timestamps = $crate::array!($batch, 4, BinaryArray);
        let $min_values = $crate::array!($batch, 5, ValueArray);
        let $max_values = $crate::array!($batch, 6, ValueArray);
        let $values = $crate::array!($batch, 7, BinaryArray);
        let $errors = $crate::array!($batch, 8, Float32Array);
    };
}
