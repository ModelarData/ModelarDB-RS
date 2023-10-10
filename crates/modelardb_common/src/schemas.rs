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

use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use once_cell::sync::Lazy;

use crate::types::{
    ArrowTimestamp, ArrowUnivariateId, ArrowValue, CompressedSchema, ConfigurationSchema,
    MetricSchema, QuerySchema, UncompressedSchema,
};

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used for uncompressed data buffers.
pub static UNCOMPRESSED_SCHEMA: Lazy<UncompressedSchema> = Lazy::new(|| {
    UncompressedSchema(Arc::new(Schema::new(vec![
        Field::new("timestamps", ArrowTimestamp::DATA_TYPE, false),
        Field::new("values", ArrowValue::DATA_TYPE, false),
    ])))
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used for compressed data buffers.
pub static COMPRESSED_SCHEMA: Lazy<CompressedSchema> = Lazy::new(|| {
    CompressedSchema(Arc::new(Schema::new(vec![
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

/// Minimum size of the metadata required for a compressed segment. Meaning that the sizes of
/// `timestamps` and `values` are not included as they are [`DataType::Binary`] and thus their size
/// depend on which model is selected to represent the values for that compressed segment.
pub static COMPRESSED_METADATA_SIZE_IN_BYTES: Lazy<usize> = Lazy::new(|| {
    COMPRESSED_SCHEMA
        .0
        .fields()
        .iter()
        .map(|field| field.data_type().primitive_width().unwrap_or(0))
        .sum()
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used for internally collected
/// metrics.
pub static METRIC_SCHEMA: Lazy<MetricSchema> = Lazy::new(|| {
    MetricSchema(Arc::new(Schema::new(vec![
        Field::new("metric", DataType::Utf8, false),
        Field::new(
            "timestamps",
            DataType::List(Arc::new(Field::new(
                "item",
                ArrowTimestamp::DATA_TYPE,
                true,
            ))),
            false,
        ),
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
            false,
        ),
    ])))
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used internally during query
/// processing.
pub static QUERY_SCHEMA: Lazy<QuerySchema> = Lazy::new(|| {
    QuerySchema(Arc::new(Schema::new(vec![
        Field::new("univariate_id", ArrowUnivariateId::DATA_TYPE, false),
        Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        Field::new("value", ArrowValue::DATA_TYPE, false),
    ])))
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used for the configuration.
pub static CONFIGURATION_SCHEMA: Lazy<ConfigurationSchema> = Lazy::new(|| {
    ConfigurationSchema(Arc::new(Schema::new(vec![
        Field::new("setting", DataType::Utf8, false),
        Field::new("value", DataType::UInt64, false),
    ])))
});
