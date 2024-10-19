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

//! The error type used throughout `modelardb_query`.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::result::Result as StdResult;

use datafusion::error::DataFusionError;

/// Result type used throughout `modelardb_query`.
pub type Result<T> = StdResult<T, ModelarDbQueryError>;

/// Error type used throughout the client.
#[derive(Debug)]
pub enum ModelarDbQueryError {
    /// Error returned by Apache DataFusion.
    DataFusion(DataFusionError),
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
}

impl Error for ModelarDbQueryError {}

impl From<DataFusionError> for ModelarDbQueryError {
    fn from(error: DataFusionError) -> ModelarDbQueryError {
        ModelarDbQueryError::DataFusion(error)
    }
}

impl Display for ModelarDbQueryError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ModelarDbQueryError::DataFusion(reason) => write!(f, "DataFusion Error: {reason}"),
            ModelarDbQueryError::InvalidArgument(reason) => {
                write!(f, "InvalidArgument Error: {reason}")
            }
        }
    }
}
