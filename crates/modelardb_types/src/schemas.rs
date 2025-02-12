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

//! The schemas used throughout the system.

use std::sync::Arc;
use std::sync::LazyLock;

use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};

use crate::types::{
    ArrowTimestamp, ArrowUnivariateId, ArrowValue, CompressedSchema, ConfigurationSchema,
    QueryCompressedSchema, QuerySchema, TableMetadataSchema, UncompressedSchema,
};

/// Name of the column used to partition the compressed segments.
pub const FIELD_COLUMN: &str = "field_column";

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used for uncompressed data buffers.
pub static UNCOMPRESSED_SCHEMA: LazyLock<UncompressedSchema> = LazyLock::new(|| {
    UncompressedSchema(Arc::new(Schema::new(vec![
        Field::new("timestamps", ArrowTimestamp::DATA_TYPE, false),
        Field::new("values", ArrowValue::DATA_TYPE, false),
    ])))
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used for compressed segments.
pub static COMPRESSED_SCHEMA: LazyLock<CompressedSchema> = LazyLock::new(|| {
    let mut query_compressed_schema_fields = QUERY_COMPRESSED_SCHEMA.0.fields().to_vec();
    let field_column = Arc::new(Field::new(FIELD_COLUMN, DataType::UInt16, false));
    query_compressed_schema_fields.push(field_column);
    CompressedSchema(Arc::new(Schema::new(query_compressed_schema_fields)))
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used when writing compressed
/// segments to disk as the Delta Lake Protocol does not support unsigned integers.
pub static DISK_COMPRESSED_SCHEMA: LazyLock<CompressedSchema> = LazyLock::new(|| {
    let mut compressed_schema_fields = COMPRESSED_SCHEMA.0.fields().to_vec();
    compressed_schema_fields[0] = Arc::new(Field::new("univariate_id", DataType::Int64, false));
    CompressedSchema(Arc::new(Schema::new(compressed_schema_fields)))
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used for compressed segments when
/// executing queries as [`FIELD_COLUMN`] is not stored in the Apache Parquet files.
pub static QUERY_COMPRESSED_SCHEMA: LazyLock<QueryCompressedSchema> = LazyLock::new(|| {
    QueryCompressedSchema(Arc::new(Schema::new(vec![
        Field::new("univariate_id", DataType::UInt64, false),
        Field::new("model_type_id", DataType::UInt8, false),
        Field::new("start_time", ArrowTimestamp::DATA_TYPE, false),
        Field::new("end_time", ArrowTimestamp::DATA_TYPE, false),
        Field::new("timestamps", DataType::Binary, false),
        Field::new("min_value", ArrowValue::DATA_TYPE, false),
        Field::new("max_value", ArrowValue::DATA_TYPE, false),
        Field::new("values", DataType::Binary, false),
        Field::new("residuals", DataType::Binary, false),
        Field::new("error", DataType::Float32, false),
    ])))
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used when reading compressed
/// segments from disk as the Delta Lake Protocol does not support unsigned integers.
pub static DISK_QUERY_COMPRESSED_SCHEMA: LazyLock<CompressedSchema> = LazyLock::new(|| {
    let mut query_compressed_schema_fields = QUERY_COMPRESSED_SCHEMA.0.fields().to_vec();
    query_compressed_schema_fields[0] =
        Arc::new(Field::new("univariate_id", DataType::Int64, false));
    CompressedSchema(Arc::new(Schema::new(query_compressed_schema_fields)))
});

/// Minimum size of the metadata required for a compressed segment. Meaning that the sizes of
/// `timestamps` and `values` are not included as they are [`DataType::Binary`] and thus their size
/// depend on which model is selected to represent the values for that compressed segment.
pub static COMPRESSED_METADATA_SIZE_IN_BYTES: LazyLock<usize> = LazyLock::new(|| {
    QUERY_COMPRESSED_SCHEMA
        .0
        .fields()
        .iter()
        .map(|field| field.data_type().primitive_width().unwrap_or(0))
        .sum()
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used internally during query processing.
pub static GRID_SCHEMA: LazyLock<QuerySchema> = LazyLock::new(|| {
    QuerySchema(Arc::new(Schema::new(vec![
        Field::new("univariate_id", ArrowUnivariateId::DATA_TYPE, false),
        Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        Field::new("value", ArrowValue::DATA_TYPE, false),
    ])))
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used for the configuration.
pub static CONFIGURATION_SCHEMA: LazyLock<ConfigurationSchema> = LazyLock::new(|| {
    ConfigurationSchema(Arc::new(Schema::new(vec![
        Field::new("setting", DataType::Utf8, false),
        Field::new("value", DataType::UInt64, true),
    ])))
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used for creating tables using
/// table metadata.
pub static TABLE_METADATA_SCHEMA: LazyLock<TableMetadataSchema> = LazyLock::new(|| {
    TableMetadataSchema(Arc::new(Schema::new(vec![
        Field::new("is_model_table", DataType::Boolean, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("schema", DataType::Binary, false),
        Field::new(
            "error_bounds",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        ),
        Field::new(
            "generated_columns",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ])))
});
