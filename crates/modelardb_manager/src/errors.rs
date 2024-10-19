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

//! The error type used throughout modelardb_manager.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Error as IoError;

use deltalake::errors::DeltaTableError;
use modelardb_common::errors::ModelarDbCommonError;
use tonic::transport::Error as TonicTransportError;
use tonic::Status as TonicStatusError;

/// Result type used throughout `modelardb_manager`. [`std::result::Result`] is used to not make the
/// definition of `Result` cyclic.
pub type Result<T> = std::result::Result<T, ModelarDbManagerError>;

/// Error type used throughout the client.
#[derive(Debug)]
pub enum ModelarDbManagerError {
    /// Error returned by Delta Lake.
    DeltaLake(DeltaTableError),
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
    /// Error returned when an invalid state is encountered.
    InvalidState(String),
    /// Error returned from IO operations.
    Io(IoError),
    /// Error returned by modelardb_common.
    ModelarDbCommon(ModelarDbCommonError),
    /// Status returned by Tonic.
    TonicStatus(TonicStatusError),
    /// Error returned by Tonic.
    TonicTransport(TonicTransportError),
}

impl Error for ModelarDbManagerError {}

impl From<DeltaTableError> for ModelarDbManagerError {
    fn from(error: DeltaTableError) -> ModelarDbManagerError {
        ModelarDbManagerError::DeltaLake(error)
    }
}

impl From<IoError> for ModelarDbManagerError {
    fn from(error: IoError) -> ModelarDbManagerError {
        ModelarDbManagerError::Io(error)
    }
}

impl From<ModelarDbCommonError> for ModelarDbManagerError {
    fn from(error: ModelarDbCommonError) -> ModelarDbManagerError {
        ModelarDbManagerError::ModelarDbCommon(error)
    }
}

impl From<TonicStatusError> for ModelarDbManagerError {
    fn from(error: TonicStatusError) -> ModelarDbManagerError {
        ModelarDbManagerError::TonicStatus(error)
    }
}

impl From<TonicTransportError> for ModelarDbManagerError {
    fn from(error: TonicTransportError) -> ModelarDbManagerError {
        ModelarDbManagerError::TonicTransport(error)
    }
}

impl Display for ModelarDbManagerError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ModelarDbManagerError::DeltaLake(reason) => write!(f, "Delta Lake Error: {reason}"),
            ModelarDbManagerError::InvalidArgument(reason) => {
                write!(f, "InvalidArgument Error: {reason}")
            }
            ModelarDbManagerError::InvalidState(reason) => {
                write!(f, "InvalidArgument Error: {reason}")
            }
            ModelarDbManagerError::Io(reason) => write!(f, "Io Error: {reason}"),
            ModelarDbManagerError::ModelarDbCommon(reason) => {
                write!(f, "ModelarDB Common Error: {reason}")
            }
            ModelarDbManagerError::TonicStatus(reason) => {
                write!(f, "Tonic Status Error: {reason}")
            }
            ModelarDbManagerError::TonicTransport(reason) => {
                write!(f, "Tonic Transport Error: {reason}")
            }
        }
    }
}
