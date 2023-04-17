/* Copyright 2021 The ModelarDB Contributors
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

//! Implementation of the Gorilla model type which uses the lossless compression
//! method for floating-point values proposed for the time series management
//! system Gorilla in the [Gorilla paper]. As this compression method compresses
//! the values of a time series segment using XOR and a variable length binary
//! encoding, aggregates are computed by iterating over all values in the
//! segment.
//!
//! [Gorilla paper]: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf

use modelardb_common::schemas::COMPRESSED_METADATA_SIZE_IN_BYTES;
use modelardb_common::types::{Timestamp, UnivariateId, UnivariateIdBuilder, Value, ValueBuilder};

use crate::models;
use crate::models::bits::{BitReader, BitVecBuilder};

/// The state the Gorilla model type needs while compressing the values of a
/// time series segment.
pub struct Gorilla {
    /// Last value compressed and added to `compressed_values`.
    last_value: Value,
    /// Number of leading zero bits for the last value that was compressed by
    /// adding its meaningful bits and leading zero bits to `compressed_values`.
    last_leading_zero_bits: u8,
    /// Number of trailing zero bits for the last value that was compressed by
    /// adding its meaningful bits and leading zero bits to `compressed_values`.
    last_trailing_zero_bits: u8,
    /// Values compressed using XOR and a variable length binary encoding.
    compressed_values: BitVecBuilder,
    /// The number of values stored in `compressed_values`.
    pub length: usize, // TODO: use Gorilla as a fallback to remove pub.
}

impl Gorilla {
    pub fn new() -> Self {
        Self {
            last_value: 0.0,
            last_leading_zero_bits: u8::MAX,
            last_trailing_zero_bits: 0,
            compressed_values: BitVecBuilder::new(),
            length: 0,
        }
    }

    /// Compress `value` using XOR and a variable length binary encoding and
    /// append the compressed value to an internal buffer in [`Gorilla`].
    pub fn compress_value(&mut self, value: Value) {
        let value_as_integer = value.to_bits();
        let last_value_as_integer = self.last_value.to_bits();
        let value_xor_last_value = value_as_integer ^ last_value_as_integer;

        if self.compressed_values.is_empty() {
            // Store the first value uncompressed using size_of::<Value> bits.
            self.compressed_values
                .append_bits(value_as_integer, models::VALUE_SIZE_IN_BITS);
        } else if value_xor_last_value == 0 {
            // Store each repeated value as a single zero bit.
            self.compressed_values.append_a_zero_bit();
        } else {
            // Store each new value as its leading zero bits (if necessary) and
            // meaningful bits (all bits from the first to the last one bit).
            let leading_zero_bits = value_xor_last_value.leading_zeros() as u8;
            let trailing_zero_bits = value_xor_last_value.trailing_zeros() as u8;
            self.compressed_values.append_a_one_bit();

            if leading_zero_bits >= self.last_leading_zero_bits
                && trailing_zero_bits >= self.last_trailing_zero_bits
            {
                // Store only the meaningful bits.
                self.compressed_values.append_a_zero_bit();
                let meaningful_bits = models::VALUE_SIZE_IN_BITS
                    - self.last_leading_zero_bits
                    - self.last_trailing_zero_bits;
                self.compressed_values.append_bits(
                    value_xor_last_value >> self.last_trailing_zero_bits,
                    meaningful_bits,
                );
            } else {
                // Store the leading zero bits before the meaningful bits using
                // 5 and 6 bits respectively as described in the Gorilla paper.
                self.compressed_values.append_a_one_bit();
                self.compressed_values
                    .append_bits(leading_zero_bits as u32, 5);

                let meaningful_bits =
                    models::VALUE_SIZE_IN_BITS - leading_zero_bits - trailing_zero_bits;
                self.compressed_values
                    .append_bits(meaningful_bits as u32, 6);
                self.compressed_values
                    .append_bits(value_xor_last_value >> trailing_zero_bits, meaningful_bits);

                self.last_leading_zero_bits = leading_zero_bits;
                self.last_trailing_zero_bits = trailing_zero_bits;
            }
        }
        self.last_value = value;
        self.length += 1;
    }

    /// Return the number of values currently compressed using XOR and a
    /// variable length binary encoding.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Return the number of bytes currently used per data point on average.
    pub fn bytes_per_value(&self) -> f32 {
        // Gorilla does not use metadata for encoding values, only the data in compressed_values.
        (COMPRESSED_METADATA_SIZE_IN_BYTES.to_owned() + self.compressed_values.len()) as f32
            / self.length as f32
    }

    /// Return the values compressed using XOR and a variable length binary
    /// encoding.
    pub fn compressed_values(self) -> Vec<u8> {
        self.compressed_values.finish()
    }
}

impl Default for Gorilla {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute the sum of the values for a time series segment whose values are
/// compressed using Gorilla's compression method for floating-point values.
pub fn sum(start_time: Timestamp, end_time: Timestamp, timestamps: &[u8], values: &[u8]) -> Value {
    // This function replicates code from gorilla::grid() as it isn't necessary
    // to store the univariate ids, timestamps, and values in arrays for a sum.
    // So any changes to the decompression must be mirrored in gorilla::grid().
    let length = models::len(start_time, end_time, timestamps);
    let mut bits = BitReader::try_new(values).unwrap();
    let mut leading_zeros = u8::MAX;
    let mut trailing_zeros: u8 = 0;
    let mut last_value = bits.read_bits(models::VALUE_SIZE_IN_BITS);

    // The first value is stored uncompressed using size_of::<Value> bits.
    let mut sum = Value::from_bits(last_value);

    // Then values are stored using XOR and a variable length binary encoding.
    for _ in 0..length - 1 {
        if bits.read_bit() {
            if bits.read_bit() {
                // New leading and trailing zeros.
                leading_zeros = bits.read_bits(5) as u8;
                let meaningful_bits = bits.read_bits(6) as u8;
                trailing_zeros = models::VALUE_SIZE_IN_BITS - meaningful_bits - leading_zeros;
            }

            // Decompress the value by reading its meaningful bits, restoring
            // its trailing zeroes through shifting, and reversing the XOR.
            let meaningful_bits = models::VALUE_SIZE_IN_BITS - leading_zeros - trailing_zeros;
            let mut value = bits.read_bits(meaningful_bits);
            value <<= trailing_zeros;
            value ^= last_value;
            last_value = value;
        }
        sum += Value::from_bits(last_value);
    }
    sum
}

/// Decompress the values in `values` for the `timestamps` without matching
/// values in `value_builder`. The values in `values` are compressed using
/// Gorilla's compression method for floating-point values. `univariate_ids`
/// and `values` are appended to `univariate_id_builder` and `value_builder`.
pub fn grid(
    univariate_id: UnivariateId,
    values: &[u8],
    univariate_id_builder: &mut UnivariateIdBuilder,
    timestamps: &[Timestamp],
    value_builder: &mut ValueBuilder,
) {
    // Changes to the decompression must be mirrored in gorilla::sum().
    let mut bits = BitReader::try_new(values).unwrap();
    let mut leading_zeros = u8::MAX;
    let mut trailing_zeros: u8 = 0;
    let mut last_value = bits.read_bits(models::VALUE_SIZE_IN_BITS);

    // The first value is stored uncompressed using size_of::<Value> bits.
    univariate_id_builder.append_value(univariate_id);
    value_builder.append_value(Value::from_bits(last_value));

    // Then values are stored using XOR and a variable length binary encoding.
    for _ in 0..timestamps.len() - 1 {
        if bits.read_bit() {
            if bits.read_bit() {
                // New leading and trailing zeros.
                leading_zeros = bits.read_bits(5) as u8;
                let meaningful_bits = bits.read_bits(6) as u8;
                trailing_zeros = models::VALUE_SIZE_IN_BITS - meaningful_bits - leading_zeros;
            }

            // Decompress the value by reading its meaningful bits, restoring
            // its trailing zeroes through shifting, and reversing the XOR.
            let meaningful_bits = models::VALUE_SIZE_IN_BITS - leading_zeros - trailing_zeros;
            let mut value = bits.read_bits(meaningful_bits);
            value <<= trailing_zeros;
            value ^= last_value;
            last_value = value;
        }
        univariate_id_builder.append_value(univariate_id);
        value_builder.append_value(Value::from_bits(last_value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::compute::kernels::aggregate;
    use modelardb_common::types::ValueArray;
    use proptest::num::f32 as ProptestValue;
    use proptest::{bool, collection, prop_assert, prop_assert_eq, prop_assume, proptest};

    use crate::models;

    // Tests for Gorilla.
    #[test]
    fn test_empty_sequence() {
        assert!(Gorilla::new().compressed_values().is_empty());
    }

    proptest! {
    #[test]
    fn test_append_single_value(value in ProptestValue::ANY) {
        let mut model_type = Gorilla::new();
        model_type.compress_value(value);
        prop_assert!(models::equal_or_nan(value as f64, model_type.last_value as f64));
        prop_assert_eq!(model_type.last_leading_zero_bits, u8::MAX);
        prop_assert_eq!(model_type.last_trailing_zero_bits, 0);
    }

    #[test]
    fn test_append_repeated_values(value in ProptestValue::ANY) {
        let mut model_type = Gorilla::new();
        model_type.compress_value(value);
        model_type.compress_value(value);
        prop_assert!(models::equal_or_nan(value as f64, model_type.last_value as f64));
        prop_assert_eq!(model_type.last_leading_zero_bits, u8::MAX);
        prop_assert_eq!(model_type.last_trailing_zero_bits, 0);
    }
    }

    #[test]
    fn test_append_different_values_with_leading_zero_bits() {
        let mut model_type = Gorilla::new();
        model_type.compress_value(37.0);
        model_type.compress_value(73.0);
        assert!(models::equal_or_nan(73.0, model_type.last_value as f64));
        assert_eq!(model_type.last_leading_zero_bits, 8);
        assert_eq!(model_type.last_trailing_zero_bits, 17);
    }

    #[test]
    fn test_append_different_values_without_leading_zero_bits() {
        let mut model_type = Gorilla::new();
        model_type.compress_value(37.0);
        model_type.compress_value(71.0);
        model_type.compress_value(73.0);
        assert!(models::equal_or_nan(73.0, model_type.last_value as f64));
        assert_eq!(model_type.last_leading_zero_bits, 8);
        assert_eq!(model_type.last_trailing_zero_bits, 17);
    }

    // Tests for sum().
    proptest! {
    #[test]
    fn test_sum(values in collection::vec(ProptestValue::ANY, 0..50)) {
        prop_assume!(!values.is_empty());
        let compressed_values = compress_values_using_gorilla(&values);
        let sum = sum(1, values.len() as i64, &values.len().to_be_bytes(), &compressed_values);
        let expected_sum = aggregate::sum(&ValueArray::from_iter_values(values)).unwrap();
        prop_assert!(models::equal_or_nan(expected_sum as f64, sum as f64));
    }
    }

    // Tests for grid().
    proptest! {
    #[test]
    fn test_grid(values in collection::vec(ProptestValue::ANY, 0..50)) {
        prop_assume!(!values.is_empty());
        let compressed_values = compress_values_using_gorilla(&values);

        let mut univariate_id_builder = UnivariateIdBuilder::with_capacity(values.len());
        let timestamps: Vec<Timestamp> = (1 ..= values.len() as i64).step_by(1).collect();
        let mut value_builder = ValueBuilder::with_capacity(values.len());
        grid(
            1,
            &compressed_values,
            &mut univariate_id_builder,
            &timestamps,
            &mut value_builder
        );

        let univariate_ids_array = univariate_id_builder.finish();
        let values_array = value_builder.finish();

        prop_assert!(
            univariate_ids_array.len() == values.len()
            && univariate_ids_array.len() == timestamps.len()
            && univariate_ids_array.len() == values_array.len()
        );
        prop_assert!(univariate_ids_array
             .iter()
             .all(|maybe_univariate_id| maybe_univariate_id.unwrap() == 1));
        prop_assert!(timestamps.windows(2).all(|window| window[1] - window[0] == 1));
        prop_assert!(slice_of_value_equal(values_array.values(), &values));
    }
    }

    fn compress_values_using_gorilla(values: &[Value]) -> Vec<u8> {
        let mut model_type = Gorilla::new();
        for value in values {
            model_type.compress_value(*value);
        }
        model_type.compressed_values()
    }

    fn slice_of_value_equal(values_one: &[Value], values_two: &[Value]) -> bool {
        let mut equal = true;
        for values in values_one.iter().zip(values_two) {
            let (value_one, value_two) = values;
            equal &= models::equal_or_nan(*value_one as f64, *value_two as f64);
        }
        equal
    }
}
