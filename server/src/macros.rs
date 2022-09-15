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
// TODO: rename downcast_arrays and update the types when refactoring query engine.
#[macro_export]
macro_rules! downcast_arrays {
    ($gids:ident, $start_times:ident, $end_times:ident, $mtids:ident, $models:ident, $gaps:ident, $batch:ident) => {
        let $gids = crate::get_array!($batch, 0, Int32Array);
        let $start_times = crate::get_array!($batch, 1, Int64Array);
        let $end_times = crate::get_array!($batch, 2, Int64Array);
        let $mtids = crate::get_array!($batch, 3, Int32Array);
        let $models = crate::get_array!($batch, 4, BinaryArray);
        let $gaps = crate::get_array!($batch, 5, BinaryArray);
    };
}
