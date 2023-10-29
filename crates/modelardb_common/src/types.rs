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

//! The types used throughout the system.
//!
//! Use declarations are purposely not used to minimize the chance of accidentally defining
//! incorrect aliases if types from different modules use the same name. It is assumed that each set
//! of aliases are all for the same underlying type.

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use crate::errors::ModelarDbError;

// Types used for a univariate id.
pub type UnivariateId = std::primitive::u64;
pub type ArrowUnivariateId = arrow::datatypes::UInt64Type;

// Types used for a collection of univariate ids.
pub type UnivariateIdBuilder = arrow::array::PrimitiveBuilder<ArrowUnivariateId>;

// Types used for a single timestamp.
pub type Timestamp = std::primitive::i64; // It is signed to match TimestampMillisecondType.
pub type ArrowTimestamp = arrow::datatypes::TimestampMillisecondType;

// Types used for a collection of timestamps.
pub type TimestampBuilder = arrow::array::PrimitiveBuilder<ArrowTimestamp>;
pub type TimestampArray = arrow::array::PrimitiveArray<ArrowTimestamp>;

// Types used for a single value.
pub type Value = std::primitive::f32;
pub type ArrowValue = arrow::datatypes::Float32Type;

// Types used for a collection of values.
pub type ValueBuilder = arrow::array::PrimitiveBuilder<ArrowValue>;
pub type ValueArray = arrow::array::PrimitiveArray<ArrowValue>;

// Types used for the schema of uncompressed data, compressed data, and metrics.
#[derive(Clone)]
pub struct UncompressedSchema(pub arrow::datatypes::SchemaRef);

#[derive(Clone)]
pub struct CompressedSchema(pub arrow::datatypes::SchemaRef);

#[derive(Clone)]
pub struct MetricSchema(pub arrow::datatypes::SchemaRef);

#[derive(Clone)]
pub struct QuerySchema(pub arrow::datatypes::SchemaRef);

#[derive(Clone)]
pub struct ConfigurationSchema(pub arrow::datatypes::SchemaRef);

#[derive(Clone)]
pub struct TagMetadataSchema(pub arrow::datatypes::SchemaRef);

#[derive(Clone)]
pub struct CompressedFileMetadataSchema(pub arrow::datatypes::SchemaRef);

/// Error bound in percentage that is guaranteed to be from 0.0% to 100.0%. For both `PMCMean`,
/// `Swing`, and `Gorilla` the error bound is interpreted as a relative per value error bound.
#[derive(Debug, Copy, Clone)]
pub struct ErrorBound(pub f32); // Simpler for the model types to directly work on the f32.

impl ErrorBound {
    /// Return [`ErrorBound`] if `percentage` is a value from 0.0% to 100.0%, otherwise
    /// [`CompressionError`](ModelarDbError::CompressionError) is returned.
    pub fn try_new(percentage: f32) -> Result<Self, ModelarDbError> {
        if !(0.0..=100.0).contains(&percentage) {
            Err(ModelarDbError::CompressionError(
                "Error bound must be a value from 0.0% to 100.0%.".to_owned(),
            ))
        } else {
            Ok(Self(percentage))
        }
    }

    /// Consumes `self`, returning the error bound as a [`f32`].
    pub fn into_inner(self) -> f32 {
        self.0
    }
}

/// Enable equal and not equal for [`ErrorBound`] and [`f32`].
impl PartialEq<ErrorBound> for f32 {
    fn eq(&self, other: &ErrorBound) -> bool {
        self.eq(&other.0)
    }
}

/// Enable less than and greater than for [`ErrorBound`] and [`f32`].
impl PartialOrd<ErrorBound> for f32 {
    fn partial_cmp(&self, other: &ErrorBound) -> Option<Ordering> {
        self.partial_cmp(&other.0)
    }
}

/// The different possible modes of a ModelarDB server, assigned when the server is started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMode {
    Cloud,
    Edge,
}

impl FromStr for ServerMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "cloud" => Ok(ServerMode::Cloud),
            "edge" => Ok(ServerMode::Edge),
            _ => Err(format!("'{value}' is not a valid value for ServerMode.")),
        }
    }
}

impl fmt::Display for ServerMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ServerMode::Cloud => write!(f, "cloud"),
            ServerMode::Edge => write!(f, "edge"),
        }
    }
}

/// The different possible modes that a ModelarDB server can be deployed in, assigned when the
/// server is started. MultiNode includes the url of the manager and the key received from the
/// manager when registering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterMode {
    SingleNode,
    MultiNode(String, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::num;
    use proptest::proptest;

    // Tests for ErrorBound.
    proptest! {
    #[test]
    fn test_error_bound_can_be_positive_if_less_than_one_hundred(percentage in num::f32::POSITIVE) {
        if percentage <= 100.0 {
            assert!(ErrorBound::try_new(percentage).is_ok())
        } else {
            assert!(ErrorBound::try_new(percentage).is_err())
        }
    }

    #[test]
    fn test_error_bound_cannot_be_negative(percentage in num::f32::NEGATIVE) {
        assert!(ErrorBound::try_new(percentage).is_err())
    }
    }

    #[test]
    fn test_error_bound_cannot_be_positive_infinity() {
        assert!(ErrorBound::try_new(f32::INFINITY).is_err())
    }

    #[test]
    fn test_error_bound_cannot_be_negative_infinity() {
        assert!(ErrorBound::try_new(f32::NEG_INFINITY).is_err())
    }

    #[test]
    fn test_error_bound_cannot_be_nan() {
        assert!(ErrorBound::try_new(f32::NAN).is_err())
    }
}
