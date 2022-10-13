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
//! Use declarations are purposely not used to minimize the chance of
//! accidentally defining incorrect aliases if types from different modules use
//! the same name, e.g., `std::primitive::f32` and `proptest::num::f32`. It is
//! assumed that each set of aliases are all for the same underlying type.

// Types used for a single time series id.
pub type TimeSeriesId = std::primitive::u64;
pub type ArrowTimeSeriesId = datafusion::arrow::datatypes::UInt64Type;

// Types used for a collection of time series ids.
pub type TimeSeriesIdBuilder = datafusion::arrow::array::PrimitiveBuilder<ArrowTimeSeriesId>;

// Types used for a single timestamp.
pub type Timestamp = std::primitive::i64; // It is signed to match TimestampMillisecondType.
pub type ArrowTimestamp = datafusion::arrow::datatypes::TimestampMillisecondType;

// Types used for a collection of timestamps.
pub type TimestampBuilder = datafusion::arrow::array::PrimitiveBuilder<ArrowTimestamp>;
pub type TimestampArray = datafusion::arrow::array::PrimitiveArray<ArrowTimestamp>;

// Types used for a single value.
pub type Value = std::primitive::f32;
pub type ArrowValue = datafusion::arrow::datatypes::Float32Type;
#[cfg(test)] // Proptest is a development dependency.
pub mod tests {
    // proptest::num::i64 is not a type. It is signed to match TimestampMillisecondType.
    pub use proptest::num::i64 as ProptestTimestamp;

    // proptest::num::f32 is not a type.
    pub use proptest::num::f32 as ProptestValue;
}

// Types used for a collection of values.
pub type ValueBuilder = datafusion::arrow::array::PrimitiveBuilder<ArrowValue>;
pub type ValueArray = datafusion::arrow::array::PrimitiveArray<ArrowValue>;

// Types used for the schema of uncompressed and compressed data.
#[derive(Clone)]
pub struct UncompressedSchema(pub datafusion::arrow::datatypes::SchemaRef);

#[derive(Clone)]
pub struct CompressedSchema(pub datafusion::arrow::datatypes::SchemaRef);
