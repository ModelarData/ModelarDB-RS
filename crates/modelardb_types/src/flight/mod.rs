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

use crate::error::Result;
use crate::functions::try_convert_schema_to_bytes;
use crate::types::{ErrorBound, TimeSeriesTableMetadata};

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/modelardb.flight.protocol.rs"));
}

/// If `schema` can be converted to bytes, encode the normal table metadata into a serializable
/// Protocol Buffer message, otherwise return [`ModelarDbTypesError`](crate::error::ModelarDbTypesError).
pub fn encode_normal_table_metadata(
    table_name: &str,
    schema: &Schema,
) -> Result<protocol::create_tables_request::NormalTableMetadata> {
    Ok(protocol::create_tables_request::NormalTableMetadata {
        name: table_name.to_string(),
        schema: try_convert_schema_to_bytes(schema)?,
    })
}

/// Return a vector of serializable Protocol Buffer messages for the error bounds of
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
