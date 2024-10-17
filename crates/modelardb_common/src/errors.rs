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

//! The error type used throughout modelardb_common.

use std::env::VarError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Error as IoError;
use std::str::Utf8Error;

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use deltalake::errors::DeltaTableError;
use object_store::path::Error as ObjectStorePathError;
use object_store::Error as ObjectStoreError;
use sqlparser::parser::ParserError;
use datafusion::parquet::errors::ParquetError;

/// Result type used throughout `modelardb_common`. [`std::result::Result`] is used to not make the
/// definition of `Result` cyclic.
pub type Result<T> = std::result::Result<T, ModelarDbCommonError>;

/// Error type used throughout the client.
#[derive(Debug)]
pub enum ModelarDbCommonError {
    /// Error returned by Apache Arrow.
    Arrow(ArrowError),
    /// Error returned by Apache DataFusion.
    DataFusion(DataFusionError),
    /// Error returned by Delta Lake.
    DeltaLake(DeltaTableError),
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
    /// Error returned from IO operations.
    Io(IoError),
    /// Error returned by ObjectStore.
    ObjectStore(ObjectStoreError),
    /// Path error returned by ObjectStore.
    ObjectStorePath(ObjectStorePathError),
    /// Error returned by Apache Parquet.
    Parquet(ParquetError),
    /// Error returned by sqlparser.
    Parser(ParserError),
    /// Error returned by UTF-8.
    Utf8(Utf8Error),
    /// Error returned by environment variables.
    Var(VarError),
}

impl Error for ModelarDbCommonError {}

impl From<ArrowError> for ModelarDbCommonError {
    fn from(error: ArrowError) -> ModelarDbCommonError {
        ModelarDbCommonError::Arrow(error)
    }
}

impl From<DataFusionError> for ModelarDbCommonError {
    fn from(error: DataFusionError) -> ModelarDbCommonError {
        ModelarDbCommonError::DataFusion(error)
    }
}

impl From<DeltaTableError> for ModelarDbCommonError {
    fn from(error: DeltaTableError) -> ModelarDbCommonError {
        ModelarDbCommonError::DeltaLake(error)
    }
}

impl From<IoError> for ModelarDbCommonError {
    fn from(error: IoError) -> ModelarDbCommonError {
        ModelarDbCommonError::Io(error)
    }
}

impl From<ObjectStoreError> for ModelarDbCommonError {
    fn from(error: ObjectStoreError) -> ModelarDbCommonError {
        ModelarDbCommonError::ObjectStore(error)
    }
}

impl From<ObjectStorePathError> for ModelarDbCommonError {
    fn from(error: ObjectStorePathError) -> ModelarDbCommonError {
        ModelarDbCommonError::ObjectStorePath(error)
    }
}

impl From<ParquetError> for ModelarDbCommonError {
    fn from(error: ParquetError) -> ModelarDbCommonError {
        ModelarDbCommonError::Parquet(error)
    }
}

impl From<ParserError> for ModelarDbCommonError {
    fn from(error: ParserError) -> ModelarDbCommonError {
        ModelarDbCommonError::Parser(error)
    }
}

impl From<Utf8Error> for ModelarDbCommonError {
    fn from(error: Utf8Error) -> ModelarDbCommonError {
        ModelarDbCommonError::Utf8(error)
    }
}

impl From<VarError> for ModelarDbCommonError {
    fn from(error: VarError) -> ModelarDbCommonError {
        ModelarDbCommonError::Var(error)
    }
}

impl Display for ModelarDbCommonError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ModelarDbCommonError::Arrow(reason) => write!(f, "Arrow Error: {reason}"),
            ModelarDbCommonError::DataFusion(reason) => write!(f, "DataFusion Error: {reason}"),
            ModelarDbCommonError::DeltaLake(reason) => write!(f, "Delta Lake Error: {reason}"),
            ModelarDbCommonError::InvalidArgument(reason) => {
                write!(f, "InvalidArgument Error: {reason}")
            }
            ModelarDbCommonError::Io(reason) => write!(f, "Io Error: {reason}"),
            ModelarDbCommonError::ObjectStore(reason) => write!(f, "ObjectStore Error: {reason}"),
            ModelarDbCommonError::ObjectStorePath(reason) => {
                write!(f, "ObjectStore Path Error: {reason}")
            }
            ModelarDbCommonError::Parquet(reason) => write!(f, "Parquet Error: {reason}"),
            ModelarDbCommonError::Parser(reason) => write!(f, "Parser Error: {reason}"),
            ModelarDbCommonError::Utf8(reason) => write!(f, "UTF-8 Error: {reason}"),
            ModelarDbCommonError::Var(reason) => write!(f, "Environment Variable Error: {reason}"),
        }
    }
}
