/* Copyright 2021 The MiniModelarDB Contributors
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

use datafusion::arrow::array::{Float32Builder, Int32Builder, TimestampMillisecondBuilder};

/** Public Functions **/
pub fn min(
    gid: i32,
    start_time: i64,
    end_time: i64,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    let mut bits = Bits::new(model);
    let mut stored_leading_zeroes = std::u32::MAX;
    let mut stored_trailing_zeroes: u32 = 0;
    let mut last_value = bits.read_bits(32);

    //The first value is stored as a f32
    let mut min_value = f32::from_bits(last_value);

    //The following values are stored as the delta of XOR
    let length_without_head = (end_time - start_time) / sampling_interval as i64;
    for _ in 0..length_without_head {
        if bits.read_bit() {
            if bits.read_bit() {
                //New leading and trailing zeros
                stored_leading_zeroes = bits.read_bits(5);
                let mut significant_bits = bits.read_bits(6);
                if significant_bits == 0 {
                    significant_bits = 32;
                }
                stored_trailing_zeroes = 32 - significant_bits - stored_leading_zeroes;
            }

            let count = 32 - stored_leading_zeroes - stored_trailing_zeroes;
            let mut value = bits.read_bits(count as u8);
            value <<= stored_trailing_zeroes;
            value ^= last_value;
            last_value = value;
        }
        min_value = f32::min(min_value, f32::from_bits(last_value));
    }
    min_value
}

pub fn max(
    gid: i32,
    start_time: i64,
    end_time: i64,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    let mut bits = Bits::new(model);
    let mut stored_leading_zeroes = std::u32::MAX;
    let mut stored_trailing_zeroes: u32 = 0;
    let mut last_value = bits.read_bits(32);

    //The first value is stored as a f32
    let mut max_value = f32::from_bits(last_value);

    //The following values are stored as the delta of XOR
    let length_without_head = (end_time - start_time) / sampling_interval as i64;
    for _ in 0..length_without_head {
        if bits.read_bit() {
            if bits.read_bit() {
                //New leading and trailing zeros
                stored_leading_zeroes = bits.read_bits(5);
                let mut significant_bits = bits.read_bits(6);
                if significant_bits == 0 {
                    significant_bits = 32;
                }
                stored_trailing_zeroes = 32 - significant_bits - stored_leading_zeroes;
            }

            let count = 32 - stored_leading_zeroes - stored_trailing_zeroes;
            let mut value = bits.read_bits(count as u8);
            value <<= stored_trailing_zeroes;
            value ^= last_value;
            last_value = value;
        }
        max_value = f32::max(max_value, f32::from_bits(last_value));
    }
    max_value
}

pub fn sum(
    gid: i32,
    start_time: i64,
    end_time: i64,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    let mut bits = Bits::new(model);
    let mut stored_leading_zeroes = std::u32::MAX;
    let mut stored_trailing_zeroes: u32 = 0;
    let mut last_value = bits.read_bits(32);

    //The first value is stored as a f32
    let mut sum = f32::from_bits(last_value);

    //The following values are stored as the delta of XOR
    let length_without_head = (end_time - start_time) / sampling_interval as i64;
    for _ in 0..length_without_head {
        if bits.read_bit() {
            if bits.read_bit() {
                //New leading and trailing zeros
                stored_leading_zeroes = bits.read_bits(5);
                let mut significant_bits = bits.read_bits(6);
                if significant_bits == 0 {
                    significant_bits = 32;
                }
                stored_trailing_zeroes = 32 - significant_bits - stored_leading_zeroes;
            }

            let count = 32 - stored_leading_zeroes - stored_trailing_zeroes;
            let mut value = bits.read_bits(count as u8);
            value <<= stored_trailing_zeroes;
            value ^= last_value;
            last_value = value;
        }
        sum += f32::from_bits(last_value);
    }
    sum
}

pub fn grid(
    gid: i32,
    start_time: i64,
    end_time: i64,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
    tids: &mut Int32Builder,
    timestamps: &mut TimestampMillisecondBuilder,
    values: &mut Float32Builder,
) {
    let mut bits = Bits::new(model);
    let mut stored_leading_zeroes = std::u32::MAX;
    let mut stored_trailing_zeroes: u32 = 0;
    let mut last_value = bits.read_bits(32);

    //The first value is stored as a f32
    tids.append_value(gid).unwrap();
    timestamps.append_value(start_time).unwrap();
    values.append_value(f32::from_bits(last_value)).unwrap();

    //The following values are stored as the delta of XOR
    let second_timestamp = start_time + sampling_interval as i64;
    let sampling_interval = sampling_interval as usize;
    for timestamp in (second_timestamp..=end_time).step_by(sampling_interval) {
        tids.append_value(gid).unwrap();
        timestamps.append_value(timestamp).unwrap();

        if bits.read_bit() {
            if bits.read_bit() {
                //New leading and trailing zeros
                stored_leading_zeroes = bits.read_bits(5);
                let mut significant_bits = bits.read_bits(6);
                if significant_bits == 0 {
                    significant_bits = 32;
                }
                stored_trailing_zeroes = 32 - significant_bits - stored_leading_zeroes;
            }

            let count = 32 - stored_leading_zeroes - stored_trailing_zeroes;
            let mut value = bits.read_bits(count as u8);
            value <<= stored_trailing_zeroes;
            value ^= last_value;
            last_value = value;
        }
        values.append_value(f32::from_bits(last_value)).unwrap();
    }
}

/** Private Functions **/
//TODO: can gorilla::decode be shared without allocating an array for min, max, etc?
fn decode(
    start_time: i64,
    end_time: i64,
    sampling_interval: i32,
    model: &[u8],
    values: &mut Float32Builder,
) {
    let mut bits = Bits::new(model);
    let mut stored_leading_zeroes = std::u32::MAX;
    let mut stored_trailing_zeroes: u32 = 0;
    let mut last_value = bits.read_bits(32);

    //The first value is stored as a f32
    values.append_value(f32::from_bits(last_value)).unwrap();

    //The following values are stored as the delta of XOR
    let length_without_head = (end_time - start_time) / sampling_interval as i64;
    for _ in 0..length_without_head {
        if bits.read_bit() {
            if bits.read_bit() {
                //New leading and trailing zeros
                stored_leading_zeroes = bits.read_bits(5);
                let mut significant_bits = bits.read_bits(6);
                if significant_bits == 0 {
                    significant_bits = 32;
                }
                stored_trailing_zeroes = 32 - significant_bits - stored_leading_zeroes;
            }

            let count = 32 - stored_leading_zeroes - stored_trailing_zeroes;
            let mut value = bits.read_bits(count as u8);
            value <<= stored_trailing_zeroes;
            value ^= last_value;
            last_value = value;
        }
        values.append_value(f32::from_bits(last_value)).unwrap();
    }
}

//Bits
//The implementation of this Type is based on code published by Ilkka Rauta
// under the MIT/Apache2 license. LINK: https://github.com/irauta/bitreader
struct Bits<'a> {
    bytes: &'a [u8],
    current_bit: u64,
}

impl<'a> Bits<'a> {
    pub fn new(bytes: &'a [u8]) -> Bits<'a> {
        Self {
            bytes,
            current_bit: 0,
        }
    }

    pub fn read_bit(&mut self) -> bool {
        self.read_bits(1) == 1
    }

    pub fn read_bits(&mut self, count: u8) -> u32 {
        let mut value: u64 = 0;
        let start = self.current_bit;
        let end = self.current_bit + count as u64;
        for bit in start..end {
            let current_byte = (bit / 8) as usize;
            let byte = self.bytes[current_byte];
            let shift = 7 - (bit % 8);
            let bit: u64 = (byte >> shift) as u64 & 1;
            value = (value << 1) | bit;
        }
        self.current_bit = end;
        value as u32
    }
}
