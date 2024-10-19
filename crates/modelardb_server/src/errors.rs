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

//! The error types used throughout the system. Their design is based on [Rust
//! by Example], [Apache Arrow], and [Apache Arrow DataFusion].
//!
//! [Rust by Example]: https://doc.rust-lang.org/rust-by-example/error/multiple_error_types/define_error_type.html
//! [Apache Arrow]: https://github.com/apache/arrow-rs/blob/master/arrow/src/error.rs
//! [Apache Arrow DataFusion]: https://github.com/apache/arrow-datafusion/blob/master/datafusion/common/src/error.rs

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Error as IoError;

use crossbeam_channel::RecvError as CrossbeamRecvError;
use crossbeam_channel::SendError as CrossbeamSendError;
use datafusion::error::DataFusionError;
use deltalake::errors::DeltaTableError;
use modelardb_common::errors::ModelarDbCommonError;
use modelardb_query::errors::ModelarDbQueryError;
use object_store::Error as ObjectStoreError;
use tonic::transport::Error as TonicTransportError;
use tonic::Status as TonicStatusError;

/// Result type used throughout the system. `std::result::Result` is used to not make the definition
/// of `Result` cyclic.
pub type Result<T> = std::result::Result<T, ModelarDbServerError>;

/// Error type used throughout the system.
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub enum ModelarDbServerError {
    /// Error returned by crossbeam when sending data.
    CrossbeamSendError(String),
    /// Error returned by crossbeam when receiving data.
    CrossbeamRecvError(String),
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

impl Error for ModelarDbServerError {}

impl<T> From<CrossbeamSendError<T>> for ModelarDbServerError {
    fn from(error: CrossbeamSendError<T>) -> ModelarDbServerError {
        ModelarDbServerError::CrossbeamSendError(error.to_string())
    }
}

impl From<CrossbeamRecvError> for ModelarDbServerError {
    fn from(error: CrossbeamRecvError) -> ModelarDbServerError {
        ModelarDbServerError::CrossbeamRecvError(error.to_string())
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

impl Display for ModelarDbServerError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ModelarDbServerError::CrossbeamSendError(reason) => {
                write!(f, "Crossbeam Send Error: {reason}")
            }
            ModelarDbServerError::CrossbeamRecvError(reason) => {
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
