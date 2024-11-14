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

//! The [`Error`] and [`Result`] types used throughout `modelardb_storage`.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::result::Result as StdResult;

use arrow::error::ArrowError;
use datafusion::parquet::errors::ParquetError;

/// Result type used throughout `modelardb_storage`.
pub type Result<T> = StdResult<T, ModelarDbStorageError>;

/// Error type used throughout `modelardb_storage`.
#[derive(Debug)]
pub enum ModelarDbStorageError {
    /// Error returned by Apache Arrow.
    Arrow(ArrowError),
    /// Error returned by Apache Parquet.
    Parquet(ParquetError),
}

impl Display for ModelarDbStorageError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Arrow(reason) => write!(f, "Arrow Error: {reason}"),
            Self::Parquet(reason) => write!(f, "Parquet Error: {reason}"),
        }
    }
}

impl Error for ModelarDbStorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Return the error that caused self to occur if one exists.
        match self {
            Self::Arrow(reason) => Some(reason),
            Self::Parquet(reason) => Some(reason),
        }
    }
}

impl From<ArrowError> for ModelarDbStorageError {
    fn from(error: ArrowError) -> Self {
        Self::Arrow(error)
    }
}

impl From<ParquetError> for ModelarDbStorageError {
    fn from(error: ParquetError) -> Self {
        Self::Parquet(error)
    }
}
