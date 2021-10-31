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

use std::convert::TryInto;

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
    let (a, b) = decode(model);
    if a == 0.0 {
        b as f32
    } else if a > 0.0 {
        (a * start_time as f64 + b) as f32
    } else {
        (a * end_time as f64 + b) as f32
    }
}

pub fn max(
    gid: i32,
    start_time: i64,
    end_time: i64,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    let (a, b) = decode(model);
    if a == 0.0 {
        b as f32
    } else if a < 0.0 {
        (a * start_time as f64 + b) as f32
    } else {
        (a * end_time as f64 + b) as f32
    }
}

pub fn sum(
    gid: i32,
    start_time: i64,
    end_time: i64,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    let (a, b) = decode(model);
    let first = a * start_time as f64 + b;
    let last = a * end_time as f64 + b;
    let average = (first + last) / 2.0;
    let length = ((end_time - start_time) / sampling_interval as i64) + 1;
    (average * length as f64) as f32
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
    let (a, b) = decode(model);
    let sampling_interval = sampling_interval as usize;
    for timestamp in (start_time..=end_time).step_by(sampling_interval) {
        tids.append_value(gid).unwrap();
        timestamps.append_value(timestamp).unwrap();
        let value: f32 = (a * timestamp as f64 + b) as f32;
        values.append_value(value).unwrap();
    }
}

/** Private Functions **/
fn decode(model: &[u8]) -> (f64, f64) {
    if model.len() == 16 {
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
    }
}
