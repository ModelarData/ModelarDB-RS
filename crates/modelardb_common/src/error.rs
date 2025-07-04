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

//! The [`Error`] and [`Result`] types used throughout `modelardb_common`.

use std::env::VarError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::result::Result as StdResult;
use std::str::Utf8Error;

/// Result type used throughout `modelardb_common`.
pub type Result<T> = StdResult<T, ModelarDbCommonError>;

/// Error type used throughout `modelardb_common`.
#[derive(Debug)]
pub enum ModelarDbCommonError {
    /// Error returned by environment variables.
    EnvironmentVar(VarError),
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
    /// Error returned by UTF-8.
    Utf8(Utf8Error),
}

impl Display for ModelarDbCommonError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::EnvironmentVar(reason) => write!(f, "Environment Variable Error: {reason}"),
            Self::InvalidArgument(reason) => write!(f, "Invalid Argument Error: {reason}"),
            Self::Utf8(reason) => write!(f, "UTF-8 Error: {reason}"),
        }
    }
}

impl Error for ModelarDbCommonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Return the error that caused self to occur if one exists.
        match self {
            Self::EnvironmentVar(reason) => Some(reason),
            Self::InvalidArgument(_reason) => None,
            Self::Utf8(reason) => Some(reason),
        }
    }
}

impl From<VarError> for ModelarDbCommonError {
    fn from(error: VarError) -> Self {
        Self::EnvironmentVar(error)
    }
}

impl From<Utf8Error> for ModelarDbCommonError {
    fn from(error: Utf8Error) -> Self {
        Self::Utf8(error)
    }
}
