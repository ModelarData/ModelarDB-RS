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
use datafusion::arrow::array::{
    Float32Builder, Int32Array, Int32Builder, Int64Array, TimestampMillisecondBuilder,
};
use std::convert::TryInto;

pub fn length(
    num_rows: usize,
    gids: &Int32Array,
    start_times: &Int64Array,
    end_times: &Int64Array,
    sampling_intervals: &[i32],
) -> usize {
    //TODO: Use SIMD through the arrow kernels if all data is from a single time
    // series, or if all queried time series use the same sampling interval
    let mut data_points = 0;
    for row_index in 0..num_rows {
        let tid = gids.value(row_index) as usize;
        let sampling_interval = *sampling_intervals.get(tid).unwrap() as i64;
        data_points +=
            ((end_times.value(row_index) - start_times.value(row_index)) / sampling_interval) + 1;
    }
    data_points as usize
}

pub fn grid(
    //TOOD: Support time series with different number of values and data types?
    //TODO: Translate the gid to the proper tids to support groups.
    //TODO: Can the tid be stored once per batch of data points from a model?
    gid: i32,
    start_time: i64,
    end_time: i64,
    mtid: i32,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
    tids: &mut Int32Builder,
    timestamps: &mut TimestampMillisecondBuilder,
    values: &mut Float32Builder,
) {
    match mtid {
        2 => grid_pmc_mean(
            gid,
            start_time,
            end_time,
            sampling_interval,
            model,
            gaps,
            tids,
            timestamps,
            values,
        ),
        3 => grid_swing(
            gid,
            start_time,
            end_time,
            sampling_interval,
            model,
            gaps,
            tids,
            timestamps,
            values,
        ),
        4 => grid_gorilla(
            gid,
            start_time,
            end_time,
            sampling_interval,
            model,
            gaps,
            tids,
            timestamps,
            values,
        ),
        _ => panic!("unknown model type"),
    }
}

/** Private Functions **/
fn grid_pmc_mean(
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
    let value = f32::from_be_bytes(model.try_into().unwrap());
    let sampling_interval = sampling_interval as usize;
    for timestamp in (start_time..=end_time).step_by(sampling_interval) {
        tids.append_value(gid).unwrap();
        timestamps.append_value(timestamp).unwrap();
        values.append_value(value).unwrap();
    }
}

fn grid_swing(
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
    //The linear function might have required double precision floating-point
    let (a, b) = if model.len() == 16 {
        (
            f64::from_be_bytes(model[0..8].try_into().unwrap()),
            f64::from_be_bytes(model[8..16].try_into().unwrap()),
        )
    } else if model.len() == 12 {
        (
            f32::from_be_bytes(model[0..4].try_into().unwrap()) as f64,
            f64::from_be_bytes(model[4..12].try_into().unwrap()),
        )
    } else {
        (
            f32::from_be_bytes(model[0..4].try_into().unwrap()) as f64,
            f32::from_be_bytes(model[4..8].try_into().unwrap()) as f64,
        )
    };

    let sampling_interval = sampling_interval as usize;
    for timestamp in (start_time..=end_time).step_by(sampling_interval) {
        tids.append_value(gid).unwrap();
        timestamps.append_value(timestamp).unwrap();
        let value: f32 = (a * timestamp as f64 + b) as f32;
        values.append_value(value).unwrap();
    }
}

fn grid_gorilla(
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
