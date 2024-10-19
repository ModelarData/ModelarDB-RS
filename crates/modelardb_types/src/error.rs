/* Copyright 2022 The ModelarDB Contributors
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

//! The error types used throughout `modelardb_types`.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::result::Result as StdResult;

/// Result type used throughout `modelardb_types`.
pub type Result<T> = StdResult<T, ModelarDbTypesError>;

/// Error type used throughout `modelardb_types`.
#[derive(Debug)]
pub enum ModelarDbTypesError {
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
}

impl Error for ModelarDbTypesError {}

impl Display for ModelarDbTypesError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ModelarDbTypesError::InvalidArgument(reason) => {
                write!(f, "InvalidArgumentError Error: {reason}")
            }
        }
    }
}
