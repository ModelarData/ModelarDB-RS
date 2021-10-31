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
pub fn min_max(
    gid: i32,
    start_time: i64,
    end_time: i64,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    decode(model)
}

pub fn sum(
    gid: i32,
    start_time: i64,
    end_time: i64,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    let length = ((end_time - start_time) / sampling_interval as i64) + 1;
    length as f32 * decode(model)
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
    let value = decode(model);
    let sampling_interval = sampling_interval as usize;
    for timestamp in (start_time..=end_time).step_by(sampling_interval) {
        tids.append_value(gid).unwrap();
        timestamps.append_value(timestamp).unwrap();
        values.append_value(value).unwrap();
    }
}

/** Private Functions **/
fn decode(model: &[u8]) -> f32 {
    f32::from_be_bytes(model.try_into().unwrap())
}
