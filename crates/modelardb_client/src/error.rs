/* Copyright 2024 The ModelarDB Contributors
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

//! The error type used throughout modelardb_client.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Error as IoError;

use arrow::error::ArrowError;
use object_store::Error as ObjectStoreError;
use rustyline::error::ReadlineError as RustyLineError;
use tonic::transport::Error as TonicTransportError;
use tonic::Status as TonicStatusError;

/// Result type used throughout the system. [`std::result::Result`] is used to not make the
/// definition of `Result` cyclic.
pub type Result<T> = std::result::Result<T, ModelarDbClientError>;

/// Error type used throughout the client.
#[derive(Debug)]
pub enum ModelarDbClientError {
    /// Error returned by Arrow.
    Arrow(ArrowError),
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
    /// Error returned from IO operations.
    Io(IoError),
    /// Error returned by ObjectStore.
    ObjectStore(ObjectStoreError),
    /// Error returned by RustyLine..
    RustyLine(RustyLineError),
    /// Status returned by Tonic.
    TonicStatus(TonicStatusError),
    /// Error returned by Tonic.
    TonicTransport(TonicTransportError),
}

impl Error for ModelarDbClientError {}

impl From<ArrowError> for ModelarDbClientError {
    fn from(error: ArrowError) -> ModelarDbClientError {
        ModelarDbClientError::Arrow(error)
    }
}

impl From<IoError> for ModelarDbClientError {
    fn from(error: IoError) -> ModelarDbClientError {
        ModelarDbClientError::Io(error)
    }
}

impl From<ObjectStoreError> for ModelarDbClientError {
    fn from(error: ObjectStoreError) -> ModelarDbClientError {
        ModelarDbClientError::ObjectStore(error)
    }
}

impl From<RustyLineError> for ModelarDbClientError {
    fn from(error: RustyLineError) -> ModelarDbClientError {
        ModelarDbClientError::RustyLine(error)
    }
}

impl From<TonicStatusError> for ModelarDbClientError {
    fn from(error: TonicStatusError) -> ModelarDbClientError {
        ModelarDbClientError::TonicStatus(error)
    }
}

impl From<TonicTransportError> for ModelarDbClientError {
    fn from(error: TonicTransportError) -> ModelarDbClientError {
        ModelarDbClientError::TonicTransport(error)
    }
}

impl Display for ModelarDbClientError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ModelarDbClientError::Arrow(reason) => {
                write!(f, "Arrow Error: {reason}")
            }
            ModelarDbClientError::InvalidArgument(reason) => {
                write!(f, "InvalidArgument Error: {reason}")
            }
            ModelarDbClientError::Io(reason) => {
                write!(f, "Io Error: {reason}")
            }
            ModelarDbClientError::ObjectStore(reason) => {
                write!(f, "ObjectStore Error: {reason}")
            }
            ModelarDbClientError::RustyLine(reason) => {
                write!(f, "RustyLine Error: {reason}")
            }
            ModelarDbClientError::TonicStatus(reason) => {
                write!(f, "Tonic Status Error: {reason}")
            }
            ModelarDbClientError::TonicTransport(reason) => {
                write!(f, "Tonic Transport Error: {reason}")
            }
        }
    }
}
