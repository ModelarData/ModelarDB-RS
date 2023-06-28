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
