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

/// Extract an [`array`](arrow::array) from a
/// [`RecordBatch`](arrow::record_batch::RecordBatch) and cast it to the
/// specified type:
///
/// ```
/// let array = crate::get_array!(record_batch, 0, UInt8Array);
/// ```
///
/// # Panics
///
/// Panics if `column` is not in `batch` or if it cannot be cast to `type`.
#[macro_export]
macro_rules! get_array {
    ($batch:ident, $column:literal, $type:ident) => {
        $batch
            .column($column)
            .as_any()
            .downcast_ref::<$type>()
            .unwrap()
    };
}

/// Extract the [`arrays`](arrow::array) required to execute queries against a
/// model table from a [`RecordBatch`](arrow::record_batch::RecordBatch), cast
/// them to the required type, and assign the resulting arrays to the specified
/// variables:
///
/// ```
/// crate::downcast_arrays!(gids, start_times, end_times, mtids, models, gaps, batch);
/// ```
///
/// # Panics
///
/// Panics if `batch` does not contain seven columns or if the columns are not
/// UInt8Array, BinaryArray, TimestampArray, TimestampArray, BinaryArray,
/// ValueArray, ValueArray, and Float32Array.
#[macro_export]
macro_rules! get_arrays {
    ($batch:ident, $model_type_id:ident, $timestamps:ident, $start_time:ident, $end_time:ident, $values:ident, $min_value:ident, $max_value:ident, $error:ident) => {
        let $model_type_id = crate::get_array!($batch, 0, UInt8Array);
        let $timestamps = crate::get_array!($batch, 1, BinaryArray);
        let $start_time = crate::get_array!($batch, 2, TimestampArray);
        let $end_time = crate::get_array!($batch, 3, TimestampArray);
        let $values = crate::get_array!($batch, 4, BinaryArray);
        let $min_value = crate::get_array!($batch, 5, ValueArray);
        let $max_value = crate::get_array!($batch, 6, ValueArray);
        let $error = crate::get_array!($batch, 7, Float32Array);
    };
}
