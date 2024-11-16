/* Copyright 2023 The ModelarDB Contributors
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

//! Implementation of storage related functions and constants used throughout ModelarDB's tests.

use std::sync::Arc;

use arrow::array::{BinaryArray, Float32Array, RecordBatch, UInt16Array, UInt64Array, UInt8Array};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use modelardb_common::test::{ERROR_BOUND_FIVE, ERROR_BOUND_ONE, ERROR_BOUND_ZERO};
use modelardb_types::functions;
use modelardb_types::schemas::COMPRESSED_SCHEMA;
use modelardb_types::types::{ArrowTimestamp, ArrowValue, ErrorBound, TimestampArray, ValueArray};

use crate::metadata::model_table_metadata::ModelTableMetadata;

/// SQL to create a normal table with a timestamp column and two floating point columns.
pub const NORMAL_TABLE_SQL: &str =
    "CREATE TABLE normal_table(timestamp TIMESTAMP, values REAL, metadata REAL)";

/// Name of the normal table used in tests.
pub const NORMAL_TABLE_NAME: &str = "normal_table";

/// SQL to create a model table with a timestamp column, two field columns, and a tag column.
pub const MODEL_TABLE_SQL: &str =
    "CREATE MODEL TABLE model_table(timestamp TIMESTAMP, field_1 FIELD, field_2 FIELD, tag TAG)";

/// Name of the model table used in tests.
pub const MODEL_TABLE_NAME: &str = "model_table";

/// Return a [`Schema`] for a normal table with a timestamp column and two floating point columns.
pub fn normal_table_schema() -> Schema {
    Schema::new(vec![
        Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        Field::new("values", ArrowValue::DATA_TYPE, false),
        Field::new("metadata", ArrowValue::DATA_TYPE, false),
    ])
}

/// Return a [`RecordBatch`] containing five rows for a normal table with a timestamp column and
/// two floating point columns.
pub fn normal_table_record_batch() -> RecordBatch {
    let timestamps = TimestampArray::from(vec![0, 1, 2, 3, 4]);
    let values = ValueArray::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    let metadata = ValueArray::from(vec![6.0, 7.0, 8.0, 9.0, 10.0]);

    RecordBatch::try_new(
        Arc::new(normal_table_schema()),
        vec![Arc::new(timestamps), Arc::new(values), Arc::new(metadata)],
    )
    .unwrap()
}

/// Return [`ModelTableMetadata`] for a model table with a schema containing a tag column, a
/// timestamp column, and two field columns.
pub fn model_table_metadata() -> ModelTableMetadata {
    let query_schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        Field::new("field_1", ArrowValue::DATA_TYPE, false),
        Field::new("field_2", ArrowValue::DATA_TYPE, false),
        Field::new("tag", DataType::Utf8, false),
    ]));

    let error_bounds = vec![
        ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
        ErrorBound::try_new_absolute(ERROR_BOUND_ONE).unwrap(),
        ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
        ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
    ];

    let generated_columns = vec![None, None, None, None];

    ModelTableMetadata::try_new(
        MODEL_TABLE_NAME.to_owned(),
        query_schema,
        error_bounds,
        generated_columns,
    )
    .unwrap()
}

/// Return [`ModelTableMetadata`] in an [`Arc`] for a model table with a schema containing a tag
/// column, a timestamp column, and two field columns.
pub fn model_table_metadata_arc() -> Arc<ModelTableMetadata> {
    Arc::new(model_table_metadata())
}

/// Return a [`RecordBatch`] containing three compressed segments.
pub fn compressed_segments_record_batch() -> RecordBatch {
    compressed_segments_record_batch_with_time(1, 0, 0.0)
}

/// Return a [`RecordBatch`] containing three compressed segments from `univariate_id`. The
/// compressed segments time range is from `time_ms` to `time_ms` + 3, while the value range is from
/// `offset` + 5.2 to `offset` + 34.2.
pub fn compressed_segments_record_batch_with_time(
    univariate_id: u64,
    time_ms: i64,
    offset: f32,
) -> RecordBatch {
    let field_column = functions::univariate_id_to_column_index(univariate_id);
    let start_times = vec![time_ms, time_ms + 2, time_ms + 4];
    let end_times = vec![time_ms + 1, time_ms + 3, time_ms + 5];
    let min_values = vec![offset + 5.2, offset + 10.3, offset + 30.2];
    let max_values = vec![offset + 20.2, offset + 12.2, offset + 34.2];

    let univariate_id = UInt64Array::from(vec![univariate_id, univariate_id, univariate_id]);
    let model_type_id = UInt8Array::from(vec![1, 1, 2]);
    let start_time = TimestampArray::from(start_times);
    let end_time = TimestampArray::from(end_times);
    let timestamps = BinaryArray::from_vec(vec![b"", b"", b""]);
    let min_value = ValueArray::from(min_values);
    let max_value = ValueArray::from(max_values);
    let values = BinaryArray::from_vec(vec![b"", b"", b""]);
    let residuals = BinaryArray::from_vec(vec![b"", b"", b""]);
    let error = Float32Array::from(vec![0.2, 0.5, 0.1]);
    let field_column = UInt16Array::from(vec![field_column, field_column, field_column]);

    let schema = COMPRESSED_SCHEMA.clone();

    RecordBatch::try_new(
        schema.0,
        vec![
            Arc::new(univariate_id),
            Arc::new(model_type_id),
            Arc::new(start_time),
            Arc::new(end_time),
            Arc::new(timestamps),
            Arc::new(min_value),
            Arc::new(max_value),
            Arc::new(values),
            Arc::new(residuals),
            Arc::new(error),
            Arc::new(field_column),
        ],
    )
    .unwrap()
}