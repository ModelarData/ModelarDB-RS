/* Copyright 2022 The MiniModelarDB Contributors
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
//! by Example], [Apache Arrow],  and [Apache Arrow DataFusion].
//!
//! [Rust by Example] https://doc.rust-lang.org/rust-by-example/error/multiple_error_types/define_error_type.html
//! [Apache Arrow] https://github.com/apache/arrow-rs/blob/master/arrow/src/error.rs
//! [Apache Arrow DataFusion] https://github.com/apache/arrow-datafusion/blob/master/datafusion/common/src/error.rs

use std::error::Error;
use std::fmt::{Display, Formatter};

/// Error type used throughout the system.
#[derive(Debug)]
pub enum MiniModelarDBError {
    /// Error returned by the model types.
    CompressionError(String),
}

impl Error for MiniModelarDBError {}

impl Display for MiniModelarDBError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
	match self {
	    MiniModelarDBError::CompressionError(reason) => write!(f, "Compression Error: {}", reason),
	}
    }
}
