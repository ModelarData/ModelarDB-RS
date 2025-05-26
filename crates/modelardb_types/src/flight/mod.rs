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
