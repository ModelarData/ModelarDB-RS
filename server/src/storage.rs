//! Module containing support for formatting uncompressed data, storing uncompressed data both
//! in-memory and in a parquet file data buffer and storing compressed data.
//!
//! The interface for interacting with the storage engine is the public "StorageEngine" struct that
//! exposes the public "new" and "insert_data" functions. The storage engine should always be
//! initialized with "StorageEngine::new()". Using "insert_data", sensor data can be inserted into
//! the engine where it is further processed to reach the final compressed state.
//!
/* Copyright 2021 The MiniModelarDB Contributors
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
use std::collections::HashMap;
use datafusion::arrow::array::PrimitiveBuilder;
use datafusion::arrow::datatypes::Float32Type;
use datafusion::parquet::data_type::Int64Type;

/// Storage engine struct responsible for keeping track of all uncompressed data and invoking the
/// compressor to move the uncompressed data into persistent model-based storage. The fields should
/// not be directly modified and are therefore only changed when using "insert_data".
///
/// # Fields
/// * `data` - The in-memory representation of uncompressed data. A hash map from the time series ID
/// to a tuple with the following format: (timestamp_array_builder, value_array_builder, \[metadata]).
/// * `data_buffer` - Hash map from the time series ID to the path of the parquet file buffer.
pub struct StorageEngine {
    data: HashMap<String, (PrimitiveBuilder<Int64Type>, PrimitiveBuilder<Float32Type>, [&'static str])>,
    data_buffer: HashMap<String, String>
}
