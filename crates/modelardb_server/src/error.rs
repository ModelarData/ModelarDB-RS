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

//! The error types used throughout `modelardb_server`.

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
    /// Error returned by crossbeam when sending data.
    CrossbeamSend(String),
    /// Error returned by crossbeam when receiving data.
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
            ModelarDbServerError::CrossbeamSend(reason) => {
                write!(f, "Crossbeam Send Error: {reason}")
            }
            ModelarDbServerError::CrossbeamRecv(reason) => {
                write!(f, "Crossbeam Recv Error: {reason}")
            }
            ModelarDbServerError::DataFusion(reason) => write!(f, "DataFusion Error: {reason}"),
            ModelarDbServerError::DeltaLake(reason) => write!(f, "Delta Lake Error: {reason}"),
            ModelarDbServerError::InvalidArgument(reason) => {
                write!(f, "InvalidArgumentError Error: {reason}")
            }
            ModelarDbServerError::InvalidState(reason) => write!(f, "InvalidState Error: {reason}"),
            ModelarDbServerError::Io(reason) => write!(f, "IO Error: {reason}"),
            ModelarDbServerError::ObjectStore(reason) => write!(f, "ObjectStore Error: {reason}"),
            ModelarDbServerError::ModelarDbCommon(reason) => {
                write!(f, "ModelarDB Common Error: {reason}")
            }
            ModelarDbServerError::ModelarDbQuery(reason) => {
                write!(f, "ModelarDB Query Error: {reason}")
            }
            ModelarDbServerError::TonicStatus(reason) => write!(f, "Tonic Status Error: {reason}"),
            ModelarDbServerError::TonicTransport(reason) => {
                write!(f, "Tonic Transport Error: {reason}")
            }
        }
    }
}

impl Error for ModelarDbServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ModelarDbServerError::CrossbeamSend(_reason) => None,
            ModelarDbServerError::CrossbeamRecv(_reason) => None,
            ModelarDbServerError::DataFusion(reason) => Some(reason),
            ModelarDbServerError::DeltaLake(reason) => Some(reason),
            ModelarDbServerError::InvalidArgument(_reason) => None,
            ModelarDbServerError::InvalidState(_reason) => None,
            ModelarDbServerError::Io(reason) => Some(reason),
            ModelarDbServerError::ObjectStore(reason) => Some(reason),
            ModelarDbServerError::ModelarDbCommon(reason) => Some(reason),
            ModelarDbServerError::ModelarDbQuery(reason) => Some(reason),
            ModelarDbServerError::TonicStatus(reason) => Some(reason),
            ModelarDbServerError::TonicTransport(reason) => Some(reason),
        }
    }
}

impl<T> From<CrossbeamSendError<T>> for ModelarDbServerError {
    fn from(error: CrossbeamSendError<T>) -> ModelarDbServerError {
        ModelarDbServerError::CrossbeamSend(error.to_string())
    }
}

impl From<CrossbeamRecvError> for ModelarDbServerError {
    fn from(error: CrossbeamRecvError) -> ModelarDbServerError {
        ModelarDbServerError::CrossbeamRecv(error.to_string())
    }
}

impl From<DataFusionError> for ModelarDbServerError {
    fn from(error: DataFusionError) -> ModelarDbServerError {
        ModelarDbServerError::DataFusion(error)
    }
}

impl From<DeltaTableError> for ModelarDbServerError {
    fn from(error: DeltaTableError) -> ModelarDbServerError {
        ModelarDbServerError::DeltaLake(error)
    }
}

impl From<IoError> for ModelarDbServerError {
    fn from(error: IoError) -> ModelarDbServerError {
        ModelarDbServerError::Io(error)
    }
}

impl From<ModelarDbCommonError> for ModelarDbServerError {
    fn from(error: ModelarDbCommonError) -> ModelarDbServerError {
        ModelarDbServerError::ModelarDbCommon(error)
    }
}

impl From<ModelarDbQueryError> for ModelarDbServerError {
    fn from(error: ModelarDbQueryError) -> ModelarDbServerError {
        ModelarDbServerError::ModelarDbQuery(error)
    }
}

impl From<ObjectStoreError> for ModelarDbServerError {
    fn from(error: ObjectStoreError) -> ModelarDbServerError {
        ModelarDbServerError::ObjectStore(error)
    }
}

impl From<TonicStatusError> for ModelarDbServerError {
    fn from(error: TonicStatusError) -> ModelarDbServerError {
        ModelarDbServerError::TonicStatus(error)
    }
}

impl From<TonicTransportError> for ModelarDbServerError {
    fn from(error: TonicTransportError) -> ModelarDbServerError {
        ModelarDbServerError::TonicTransport(error)
    }
}
