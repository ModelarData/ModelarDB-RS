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

//! The error type used throughout `modelardb_common`.

use std::env::VarError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Error as IoError;
use std::result::Result as StdResult;
use std::str::Utf8Error;

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use deltalake::errors::DeltaTableError;
use object_store::path::Error as ObjectStorePathError;
use object_store::Error as ObjectStoreError;
use sqlparser::parser::ParserError;

/// Result type used throughout `modelardb_common`.
pub type Result<T> = StdResult<T, ModelarDbCommonError>;

/// Error type used throughout `modelardb_common`.
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

impl Display for ModelarDbCommonError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Arrow(reason) => write!(f, "Arrow Error: {reason}"),
            Self::DataFusion(reason) => write!(f, "DataFusion Error: {reason}"),
            Self::DeltaLake(reason) => write!(f, "Delta Lake Error: {reason}"),
            Self::InvalidArgument(reason) => write!(f, "InvalidArgument Error: {reason}"),
            Self::Io(reason) => write!(f, "Io Error: {reason}"),
            Self::ObjectStore(reason) => write!(f, "ObjectStore Error: {reason}"),
            Self::ObjectStorePath(reason) => write!(f, "ObjectStore Path Error: {reason}"),
            Self::Parquet(reason) => write!(f, "Parquet Error: {reason}"),
            Self::Parser(reason) => write!(f, "Parser Error: {reason}"),
            Self::Utf8(reason) => write!(f, "UTF-8 Error: {reason}"),
            Self::Var(reason) => write!(f, "Environment Variable Error: {reason}"),
        }
    }
}

impl Error for ModelarDbCommonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Arrow(reason) => Some(reason),
            Self::DataFusion(reason) => Some(reason),
            Self::DeltaLake(reason) => Some(reason),
            Self::InvalidArgument(_reason) => None,
            Self::Io(reason) => Some(reason),
            Self::ObjectStore(reason) => Some(reason),
            Self::ObjectStorePath(reason) => Some(reason),
            Self::Parquet(reason) => Some(reason),
            Self::Parser(reason) => Some(reason),
            Self::Utf8(reason) => Some(reason),
            Self::Var(reason) => Some(reason),
        }
    }
}

impl From<ArrowError> for ModelarDbCommonError {
    fn from(error: ArrowError) -> Self {
        Self::Arrow(error)
    }
}

impl From<DataFusionError> for ModelarDbCommonError {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}

impl From<DeltaTableError> for ModelarDbCommonError {
    fn from(error: DeltaTableError) -> Self {
        Self::DeltaLake(error)
    }
}

impl From<IoError> for ModelarDbCommonError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl From<ObjectStoreError> for ModelarDbCommonError {
    fn from(error: ObjectStoreError) -> Self {
        Self::ObjectStore(error)
    }
}

impl From<ObjectStorePathError> for ModelarDbCommonError {
    fn from(error: ObjectStorePathError) -> Self {
        Self::ObjectStorePath(error)
    }
}

impl From<ParquetError> for ModelarDbCommonError {
    fn from(error: ParquetError) -> Self {
        Self::Parquet(error)
    }
}

impl From<ParserError> for ModelarDbCommonError {
    fn from(error: ParserError) -> Self {
        Self::Parser(error)
    }
}

impl From<Utf8Error> for ModelarDbCommonError {
    fn from(error: Utf8Error) -> Self {
        Self::Utf8(error)
    }
}

impl From<VarError> for ModelarDbCommonError {
    fn from(error: VarError) -> Self {
        Self::Var(error)
    }
}
