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

//! Utility functions used in the Apache Arrow Flight API for both the server and manager.

use std::collections::HashMap;
use std::error::Error;

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_flight::{utils, FlightData, FlightDescriptor};
use tonic::Status;

/// Return the table stored as the first element in [`FlightDescriptor.path`], otherwise a
/// [`Status`] that specifies that the table name is missing.
pub fn table_name_from_flight_descriptor(
    flight_descriptor: &FlightDescriptor,
) -> Result<&String, Status> {
    flight_descriptor
        .path
        .first()
        .ok_or_else(|| Status::invalid_argument("No table name in FlightDescriptor.path."))
}

/// Convert `flight_data` to a [`RecordBatch`]. If `schema` does not match the data or `flight_data`
/// could not be converted, [`Status`] is returned.
pub fn flight_data_to_record_batch(
    flight_data: &FlightData,
    schema: &SchemaRef,
    dictionaries_by_id: &HashMap<i64, ArrayRef>,
) -> Result<RecordBatch, Status> {
    debug_assert_eq!(flight_data.flight_descriptor, None);

    utils::flight_data_to_arrow_batch(flight_data, schema.clone(), dictionaries_by_id)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

/// Convert an `error` to a [`Status`] with [`tonic::Code::InvalidArgument`] as the code and `error`
/// converted to a [`String`] as the message.
pub fn error_to_status_invalid_argument(error: impl Error) -> Status {
    Status::invalid_argument(error.to_string())
}

/// Convert an `error` to a [`Status`] with [`tonic::Code::Internal`] as the code and `error`
/// converted to a [`String`] as the message.
pub fn error_to_status_internal(error: impl Error) -> Status {
    Status::internal(error.to_string())
}
