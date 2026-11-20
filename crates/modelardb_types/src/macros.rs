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

/// Convert the result of an expression to [`Any`](std::any::Any) using the `as_any()` method and
/// then cast it to a concrete type using the `downcast_ref()` method. Panics if the cast fails. For
/// example, cast an [`Array`](arrow::array::Array) or [`ArrayRef`](arrow::array::ArrayRef) to its
/// actual type or panic:
///
/// ```
/// # use std::sync::Arc;
/// #
/// # use arrow::array::ArrayRef;
/// # use modelardb_types::types::{Timestamp, TimestampArray};
/// #
/// # let array_ref: ArrayRef = Arc::new(TimestampArray::from(Vec::<Timestamp>::new()));
/// let timestamp_array = modelardb_types::cast!(array_ref, TimestampArray);
/// ```
///
/// # Panics
///
/// Panics if the result of `expr` cannot be cast to `type`.
#[macro_export]
macro_rules! cast {
    ($expr:expr, $type:ident) => {
        $expr.as_any().downcast_ref::<$type>().unwrap()
    };
}

/// Extract an [`array`](arrow::array::Array) from a slice of `ArrayRef` and cast it to the
/// specified type or panic:
///
/// ```
/// # use std::sync::Arc;
/// #
/// # use arrow::array::ArrayRef;
/// # use modelardb_types::types::{Timestamp, TimestampArray, Value, ValueArray};
/// #
/// # let array_vector: Vec<ArrayRef> = vec![
/// #         Arc::new(TimestampArray::from(Vec::<Timestamp>::new())),
/// #         Arc::new(ValueArray::from(Vec::<Value>::new())),
/// # ];
/// # let array_slice = &array_vector;
/// let array = modelardb_types::value!(array_slice, 0, TimestampArray);
/// ```
///
/// # Panics
///
/// Panics if `index` is not in `values` or if the array cannot be cast to `type`.
#[macro_export]
macro_rules! value {
    ($values:expr, $index:expr, $type:ident) => {
        $crate::cast!($values[$index], $type)
    };
}

/// Extract an [`array`](arrow::array::Array) from a
/// [`RecordBatch`](arrow::record_batch::RecordBatch) and cast it to the specified type or panic:
///
/// ```
/// # use std::sync::Arc;
/// #
/// # use arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
/// # use arrow::record_batch::RecordBatch;
/// # use modelardb_types::types::{ArrowTimestamp, ArrowValue, Timestamp, TimestampArray, Value, ValueArray};
/// #
/// # let schema = Schema::new(vec![
/// #     Field::new("timestamps", ArrowTimestamp::DATA_TYPE, false),
/// #     Field::new("values", ArrowValue::DATA_TYPE, false),
/// # ]);
/// #
/// # let record_batch = RecordBatch::try_new(
/// #     Arc::new(schema),
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
/// Panics if `index` is not in `batch` or if the array cannot be cast to `type`.
#[macro_export]
macro_rules! array {
    ($batch:expr, $index:expr, $type:ident) => {
        $crate::cast!($batch.column($index), $type)
    };
}
