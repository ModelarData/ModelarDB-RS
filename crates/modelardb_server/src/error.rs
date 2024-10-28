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

//! The [`Error`] and [`Result`] types used throughout `modelardb_server`.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Error as IoError;
use std::result::Result as StdResult;

use crossbeam_channel::RecvError as CrossbeamRecvError;
use crossbeam_channel::SendError as CrossbeamSendError;
use datafusion::error::DataFusionError;
use deltalake::errors::DeltaTableError;
use modelardb_common::error::ModelarDbCommonError;
use modelardb_query::error::ModelarDbQueryError;
use object_store::Error as ObjectStoreError;
use tonic::transport::Error as TonicTransportError;
use tonic::Status as TonicStatusError;

/// Result type used throughout `modelardb_server`.
pub type Result<T> = StdResult<T, ModelarDbServerError>;

/// Error type used throughout `modelardb_server`.
#[derive(Debug)]
pub enum ModelarDbServerError {
    /// Error returned by Crossbeam when sending data.
    CrossbeamSend(String),
    /// Error returned by Crossbeam when receiving data.
    CrossbeamRecv(String),
    /// Error returned by Apache DataFusion.
    DataFusion(DataFusionError),
    /// Error returned by Delta Lake.
    DeltaLake(DeltaTableError),
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
    /// Error returned when an invalid state is encountered.
    InvalidState(String),
    /// Error returned from IO operations.
    Io(IoError),
    /// Error returned by ObjectStore.
    ObjectStore(ObjectStoreError),
    /// Error returned by modelardb_common.
    ModelarDbCommon(ModelarDbCommonError),
    /// Error returned by modelardb_query.
    ModelarDbQuery(ModelarDbQueryError),
    /// Status returned by Tonic.
    TonicStatus(TonicStatusError),
    /// Error returned by Tonic.
    TonicTransport(TonicTransportError),
}

impl Display for ModelarDbServerError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::CrossbeamSend(reason) => write!(f, "Crossbeam Send Error: {reason}"),
            Self::CrossbeamRecv(reason) => write!(f, "Crossbeam Recv Error: {reason}"),
            Self::DataFusion(reason) => write!(f, "DataFusion Error: {reason}"),
            Self::DeltaLake(reason) => write!(f, "Delta Lake Error: {reason}"),
            Self::InvalidArgument(reason) => write!(f, "Invalid Argument Error: {reason}"),
            Self::InvalidState(reason) => write!(f, "Invalid State Error: {reason}"),
            Self::Io(reason) => write!(f, "IO Error: {reason}"),
            Self::ObjectStore(reason) => write!(f, "Object Store Error: {reason}"),
            Self::ModelarDbCommon(reason) => write!(f, "ModelarDB Common Error: {reason}"),
            Self::ModelarDbQuery(reason) => write!(f, "ModelarDB Query Error: {reason}"),
            Self::TonicStatus(reason) => write!(f, "Tonic Status Error: {reason}"),
            Self::TonicTransport(reason) => write!(f, "Tonic Transport Error: {reason}"),
        }
    }
}

impl Error for ModelarDbServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Return the error that caused self to occur if one exists.
        match self {
            Self::CrossbeamSend(_reason) => None,
            Self::CrossbeamRecv(_reason) => None,
            Self::DataFusion(reason) => Some(reason),
            Self::DeltaLake(reason) => Some(reason),
            Self::InvalidArgument(_reason) => None,
            Self::InvalidState(_reason) => None,
            Self::Io(reason) => Some(reason),
            Self::ObjectStore(reason) => Some(reason),
            Self::ModelarDbCommon(reason) => Some(reason),
            Self::ModelarDbQuery(reason) => Some(reason),
            Self::TonicStatus(reason) => Some(reason),
            Self::TonicTransport(reason) => Some(reason),
        }
    }
}

impl<T> From<CrossbeamSendError<T>> for ModelarDbServerError {
    fn from(error: CrossbeamSendError<T>) -> Self {
        Self::CrossbeamSend(error.to_string())
    }
}

impl From<CrossbeamRecvError> for ModelarDbServerError {
    fn from(error: CrossbeamRecvError) -> Self {
        Self::CrossbeamRecv(error.to_string())
    }
}

impl From<DataFusionError> for ModelarDbServerError {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}

impl From<DeltaTableError> for ModelarDbServerError {
    fn from(error: DeltaTableError) -> Self {
        Self::DeltaLake(error)
    }
}

impl From<IoError> for ModelarDbServerError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl From<ModelarDbCommonError> for ModelarDbServerError {
    fn from(error: ModelarDbCommonError) -> Self {
        Self::ModelarDbCommon(error)
    }
}

impl From<ModelarDbQueryError> for ModelarDbServerError {
    fn from(error: ModelarDbQueryError) -> Self {
        Self::ModelarDbQuery(error)
    }
}

impl From<ObjectStoreError> for ModelarDbServerError {
    fn from(error: ObjectStoreError) -> Self {
        Self::ObjectStore(error)
    }
}

impl From<TonicStatusError> for ModelarDbServerError {
    fn from(error: TonicStatusError) -> Self {
        Self::TonicStatus(error)
    }
}

impl From<TonicTransportError> for ModelarDbServerError {
    fn from(error: TonicTransportError) -> Self {
        Self::TonicTransport(error)
    }
}
