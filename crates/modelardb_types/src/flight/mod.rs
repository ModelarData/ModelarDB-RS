/* Copyright 2025 The ModelarDB Contributors
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

use arrow::datatypes::Schema;
use prost::Message;

use crate::error::Result;
use crate::functions::try_convert_schema_to_bytes;
use crate::types::{ErrorBound, TimeSeriesTableMetadata};

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/modelardb.flight.protocol.rs"));
}

/// Serialize a request to create tables in ModelarDB.
pub fn serialize_create_tables_request(
    normal_table_metadata: Vec<protocol::create_tables_request::NormalTableMetadata>,
    time_series_table_metadata: Vec<protocol::create_tables_request::TimeSeriesTableMetadata>,
) -> Vec<u8> {
    let create_tables_request = protocol::CreateTablesRequest {
        normal_tables: normal_table_metadata,
        time_series_tables: time_series_table_metadata,
    };

    create_tables_request.encode_to_vec()
}

/// Encode and serialize the metadata for a normal table into a request to create tables in
/// ModelarDB. If the schema cannot be converted to bytes, return
/// [`ModelarDbTypesError`](crate::error::ModelarDbTypesError).
pub fn encode_and_serialize_normal_table_metadata(
    table_name: &str,
    schema: &Schema,
) -> Result<Vec<u8>> {
    let normal_table_metadata = encode_normal_table_metadata(table_name, schema)?;
    let create_tables_request = protocol::CreateTablesRequest {
        normal_tables: vec![normal_table_metadata],
        time_series_tables: vec![],
    };

    Ok(create_tables_request.encode_to_vec())
}

/// If `schema` can be converted to bytes, encode the normal table metadata into a serializable
/// protobuf message, otherwise return [`ModelarDbTypesError`](crate::error::ModelarDbTypesError).
pub fn encode_normal_table_metadata(
    table_name: &str,
    schema: &Schema,
) -> Result<protocol::create_tables_request::NormalTableMetadata> {
    Ok(protocol::create_tables_request::NormalTableMetadata {
        name: table_name.to_string(),
        schema: try_convert_schema_to_bytes(schema)?,
    })
}

/// Encode and serialize the metadata for a time series table into a request to create tables in
/// ModelarDB. If the schema cannot be converted to bytes, return
/// [ModelarDbTypesError](crate::error::ModelarDbTypesError).
pub fn encode_and_serialize_time_series_table_metadata(
    time_series_table_metadata: TimeSeriesTableMetadata,
) -> Result<Vec<u8>> {
    let time_series_table_metadata = encode_time_series_table_metadata(time_series_table_metadata)?;
    let create_tables_request = protocol::CreateTablesRequest {
        normal_tables: vec![],
        time_series_tables: vec![time_series_table_metadata],
    };

    Ok(create_tables_request.encode_to_vec())
}

/// Return a serializable protobuf message constructed from the metadata in
/// `time_series_table_metadata`. If the schema cannot be converted to bytes, return
/// [`ModelarDbTypesError`](crate::error::ModelarDbTypesError).
pub fn encode_time_series_table_metadata(
    time_series_table_metadata: TimeSeriesTableMetadata,
) -> Result<protocol::create_tables_request::TimeSeriesTableMetadata> {
    let mut generated_column_expressions =
        Vec::with_capacity(time_series_table_metadata.query_schema.fields.len());
    for generated_column in &time_series_table_metadata.generated_columns {
        if let Some(generated_column) = generated_column {
            let sql_expr = generated_column.original_expr.clone();
            generated_column_expressions.push(sql_expr);
        } else {
            // An empty string is used to represent columns that are not generated.
            generated_column_expressions.push(String::new());
        }
    }

    Ok(protocol::create_tables_request::TimeSeriesTableMetadata {
        name: time_series_table_metadata.name.clone(),
        schema: try_convert_schema_to_bytes(&time_series_table_metadata.query_schema)?,
        error_bounds: encode_error_bounds(&time_series_table_metadata),
        generated_column_expressions,
    })
}

/// Return a vector of serializable protobuf messages for the error bounds of
/// `time_series_table_metadata`.
fn encode_error_bounds(
    time_series_table_metadata: &TimeSeriesTableMetadata,
) -> Vec<protocol::create_tables_request::time_series_table_metadata::ErrorBound> {
    // Since the time series table metadata does not include error bounds for the generated columns,
    // lossless error bounds are added for each generated column.
    let mut error_bounds_all =
        Vec::with_capacity(time_series_table_metadata.query_schema.fields().len());

    let lossless = protocol::create_tables_request::time_series_table_metadata::ErrorBound {
        r#type: 0,
        value: 0.0,
    };

    for field in time_series_table_metadata.query_schema.fields() {
        if let Ok(field_index) = time_series_table_metadata.schema.index_of(field.name()) {
            let (error_bound_type, value) =
                match time_series_table_metadata.error_bounds[field_index] {
                    ErrorBound::Absolute(value) => (0, value as f32),
                    ErrorBound::Relative(value) => (1, value),
                };

            error_bounds_all.push(
                protocol::create_tables_request::time_series_table_metadata::ErrorBound {
                    r#type: error_bound_type,
                    value,
                },
            );
        } else {
            error_bounds_all.push(lossless);
        }
    }

    error_bounds_all
}
