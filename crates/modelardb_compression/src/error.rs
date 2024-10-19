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

//! The error types used throughout `modelardb_compression`.

use std::error::Error;
use std::fmt::{Display, Formatter};

/// Result type used throughout `modelardb_compression`. `std::result::Result` is used to not make
/// the definition of `Result` cyclic.
pub type Result<T> = std::result::Result<T, ModelarDbCompressionError>;

/// Error type used throughout the system.
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub enum ModelarDbCompressionError {
    /// Error returned when an invalid argument was passed.
    InvalidArgumentError(String),
}

impl Error for ModelarDbCompressionError {}

impl Display for ModelarDbCompressionError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ModelarDbCompressionError::InvalidArgumentError(reason) => {
                write!(f, "InvalidArgumentError Error: {reason}")
            }
        }
    }
}
