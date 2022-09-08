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
//! [Gorilla paper]: https://dl.acm.org/doi/10.14778/2824032.2824078

use datafusion::arrow::compute::kernels::aggregate;

use crate::models;
use crate::models::bits::{BitReader, BitVecBuilder};
use crate::types::{
    TimeSeriesId, TimeSeriesIdBuilder, Timestamp, TimestampBuilder, Value, ValueArray, ValueBuilder,
};

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
                    meaningful_bits as u8,
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
    pub fn get_length(&self) -> usize {
        self.length
    }

    /// Return the number of bytes currently used per data point on average.
    pub fn get_bytes_per_value(&self) -> f32 {
        self.compressed_values.length() as f32 / self.length as f32
    }

    /// Return the values compressed using XOR and a variable length binary
    /// encoding.
    pub fn get_compressed_values(self) -> Vec<u8> {
        self.compressed_values.finish()
    }
}

/// Compute the minimum value for a time series segment whose values are
/// compressed using Gorilla's compression method for floating-point values.
pub fn min(
    start_time: Timestamp,
    end_time: Timestamp,
    sampling_interval: i32,
    model: &[u8],
) -> Value {
    let decompressed_values =
        decompress_values_to_array(start_time, end_time, sampling_interval, model);
    aggregate::min(&decompressed_values).unwrap()
}

/// Compute the maximum value for a time series segment whose values are
/// compressed using Gorilla's compression method for floating-point values.
pub fn max(
    start_time: Timestamp,
    end_time: Timestamp,
    sampling_interval: i32,
    model: &[u8],
) -> Value {
    let decompressed_values =
        decompress_values_to_array(start_time, end_time, sampling_interval, model);
    aggregate::max(&decompressed_values).unwrap()
}

/// Compute the sum of the values for a time series segment whose values are
/// compressed using Gorilla's compression method for floating-point values.
pub fn sum(
    start_time: Timestamp,
    end_time: Timestamp,
    sampling_interval: i32,
    model: &[u8],
) -> Value {
    let decompressed_values =
        decompress_values_to_array(start_time, end_time, sampling_interval, model);
    aggregate::sum(&decompressed_values).unwrap()
}

/// Reconstruct the data points for a time series segment whose values are
/// compressed using Gorilla's compression method for floating-point values.
/// Each data point is split into its three components and appended to
/// `time_series_ids`, `timestamps`, and `values`.
pub fn grid(
    time_series_id: TimeSeriesId,
    start_time: Timestamp,
    end_time: Timestamp,
    sampling_interval: i32,
    model: &[u8],
    time_series_ids: &mut TimeSeriesIdBuilder,
    timestamps: &mut TimestampBuilder,
    values: &mut ValueBuilder,
) {
    for timestamp in (start_time..=end_time).step_by(sampling_interval as usize) {
        time_series_ids.append_value(time_series_id);
        timestamps.append_value(timestamp);
    }
    decompress_values(start_time, end_time, sampling_interval, model, values);
}

/// Decompress values compressed using Gorilla's compression method for
/// floating-point values and store them in a new Apache Arrow array.
fn decompress_values_to_array(
    start_time: Timestamp,
    end_time: Timestamp,
    sampling_interval: i32,
    model: &[u8],
) -> ValueArray {
    let length = models::length(start_time, end_time, sampling_interval);
    let mut value_builder = ValueBuilder::new(length as usize);
    decompress_values(
        start_time,
        end_time,
        sampling_interval,
        model,
        &mut value_builder,
    );
    value_builder.finish()
}

/// Decompress values compressed using Gorilla's compression method for
/// floating-point values and append them to `values`.
fn decompress_values(
    start_time: Timestamp,
    end_time: Timestamp,
    sampling_interval: i32,
    model: &[u8],
    values: &mut ValueBuilder,
) {
    let mut bits = BitReader::try_new(model).unwrap();
    let mut leading_zeros = u8::MAX;
    let mut trailing_zeros: u8 = 0;
    let mut last_value = bits.read_bits(models::VALUE_SIZE_IN_BITS);

    // The first value is stored uncompressed using size_of::<Value> bits.
    values.append_value(Value::from_bits(last_value));

    // Then values are stored using XOR and a variable length binary encoding.
    let length_without_first_value = models::length(start_time, end_time, sampling_interval) - 1;
    for _ in 0..length_without_first_value {
        if bits.read_bit() {
            if bits.read_bit() {
                // New leading and trailing zeros.
                leading_zeros = bits.read_bits(5) as u8;
                let meaningful_bits = bits.read_bits(6) as u8;
                trailing_zeros = models::VALUE_SIZE_IN_BITS - meaningful_bits - leading_zeros;
            }

            let meaningful_bits = models::VALUE_SIZE_IN_BITS - leading_zeros - trailing_zeros;
            let mut value = bits.read_bits(meaningful_bits);
            value <<= trailing_zeros;
            value ^= last_value;
            last_value = value;
        }
        values.append_value(Value::from_bits(last_value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::{bool, collection, prop_assert, prop_assert_eq, prop_assume, proptest};

    use crate::models;
    use crate::types::tests::ProptestValue;

    // Tests for Gorilla.
    #[test]
    fn test_empty_sequence() {
        assert!(Gorilla::new().get_compressed_values().is_empty());
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


    // Tests for min().
    proptest! {
    #[test]
    fn test_min(values in collection::vec(ProptestValue::ANY, 0..50)) {
        prop_assume!(!values.is_empty());
        let compressed_values = compress_values_using_gorilla(&values);
        let min = min(1, values.len() as i64, 1, &compressed_values);
        let expected_min = aggregate::min(&ValueArray::from_iter_values(values)).unwrap();
        prop_assert!(models::equal_or_nan(expected_min as f64, min as f64));
    }
    }

    // Tests for max().
    proptest! {
    #[test]
    fn test_max(values in collection::vec(ProptestValue::ANY, 0..50)) {
        prop_assume!(!values.is_empty());
        let compressed_values = compress_values_using_gorilla(&values);
        let max = max(1, values.len() as i64, 1, &compressed_values);
        let expected_max = aggregate::max(&ValueArray::from_iter_values(values)).unwrap();
        prop_assert!(models::equal_or_nan(expected_max as f64, max as f64));
    }
    }

    // Tests for sum().
    proptest! {
    #[test]
    fn test_sum(values in collection::vec(ProptestValue::ANY, 0..50)) {
        prop_assume!(!values.is_empty());
        let compressed_values = compress_values_using_gorilla(&values);
        let sum = sum(1, values.len() as i64, 1, &compressed_values);
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
        let mut time_series_ids_builder = TimeSeriesIdBuilder::new(10);
        let mut timestamps_builder = TimestampBuilder::new(10);
        let mut values_builder = ValueBuilder::new(10);

        grid(
            1,
            1,
            values.len() as i64,
            1,
            &compressed_values,
            &mut time_series_ids_builder,
            &mut timestamps_builder,
            &mut values_builder
        );

        let time_series_ids_array = time_series_ids_builder.finish();
        let timestamps_array = timestamps_builder.finish();
        let values_array = values_builder.finish();

        prop_assert!(
            time_series_ids_array.len() == values.len()
            && time_series_ids_array.len() == timestamps_array.len()
            && time_series_ids_array.len() == values_array.len()
        );
        prop_assert!(time_series_ids_array
             .iter()
             .all(|time_series_id_option| time_series_id_option.unwrap() == 1));
        prop_assert!(timestamps_array.values().windows(2).all(|window| window[1] - window[0] == 1));
        prop_assert!(slice_of_value_equal(values_array.values(), &values));
    }
    }

    // Tests for the decompress_values().
    proptest! {
    #[test]
    fn test_decode(values in collection::vec(ProptestValue::ANY, 0..50)) {
        prop_assume!(!values.is_empty());
        let compressed_values = compress_values_using_gorilla(&values);
        let mut decompressed_values_builder = ValueBuilder::new(values.len());
        decompress_values(
            1,
            values.len() as i64,
            1,
            &compressed_values,
            &mut decompressed_values_builder
        );
        let decompressed_values = decompressed_values_builder.finish();
        prop_assert!(slice_of_value_equal(decompressed_values.values(), &values));
    }
    }

    fn compress_values_using_gorilla(values: &[Value]) -> Vec<u8> {
        let mut model_type = Gorilla::new();
        for value in values {
            model_type.compress_value(*value);
        }
        model_type.get_compressed_values()
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
