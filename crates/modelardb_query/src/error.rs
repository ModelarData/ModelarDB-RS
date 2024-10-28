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

//! The [`Error`] and [`Result`] types used throughout `modelardb_query`.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::result::Result as StdResult;

use datafusion::error::DataFusionError;

/// Result type used throughout `modelardb_query`.
pub type Result<T> = StdResult<T, ModelarDbQueryError>;

/// Error type used throughout `modelardb_query`.
#[derive(Debug)]
pub enum ModelarDbQueryError {
    /// Error returned by Apache DataFusion.
    DataFusion(DataFusionError),
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
}

impl Error for ModelarDbQueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Return the error that caused self to occur if one exists.
        match self {
            Self::DataFusion(reason) => Some(reason),
            Self::InvalidArgument(_reason) => None,
        }
    }
}

impl Display for ModelarDbQueryError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::DataFusion(reason) => write!(f, "DataFusion Error: {reason}"),
            Self::InvalidArgument(reason) => write!(f, "Invalid Argument Error: {reason}"),
        }
    }
}

impl From<DataFusionError> for ModelarDbQueryError {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}