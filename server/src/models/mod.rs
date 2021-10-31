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
mod gorilla;
mod pmcmean;
mod swing;

use datafusion::arrow::array::{
    Float32Builder, Int32Array, Int32Builder, Int64Array, TimestampMillisecondBuilder,
};

pub fn length(
    num_rows: usize,
    gids: &Int32Array,
    start_times: &Int64Array,
    end_times: &Int64Array,
    sampling_intervals: &[i32],
) -> usize {
    //TODO: use SIMD through the arrow kernels if all data is from a single time
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

//TODO: can mtid be converted to a model type enum without adding overhead?
pub fn grid(
    //TODO: support time series with different number of values and data types?
    //TODO: translate the gid to the proper tids to support groups.
    //TODO: can the tid be stored once per batch of data points from a model?
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
        2 => pmcmean::grid(
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
        3 => swing::grid(
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
        4 => gorilla::grid(
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

pub fn min(
    gid: i32,
    start_time: i64,
    end_time: i64,
    mtid: i32,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    match mtid {
        2 => pmcmean::min_max(gid, start_time, end_time, sampling_interval, model, gaps),
        3 => swing::min(gid, start_time, end_time, sampling_interval, model, gaps),
        4 => gorilla::min(gid, start_time, end_time, sampling_interval, model, gaps),
        _ => panic!("unknown model type"),
    }
}

pub fn max(
    gid: i32,
    start_time: i64,
    end_time: i64,
    mtid: i32,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    match mtid {
        2 => pmcmean::min_max(gid, start_time, end_time, sampling_interval, model, gaps),
        3 => swing::max(gid, start_time, end_time, sampling_interval, model, gaps),
        4 => gorilla::max(gid, start_time, end_time, sampling_interval, model, gaps),
        _ => panic!("unknown model type"),
    }
}

pub fn sum(
    gid: i32,
    start_time: i64,
    end_time: i64,
    mtid: i32,
    sampling_interval: i32,
    model: &[u8],
    gaps: &[u8],
) -> f32 {
    match mtid {
        2 => pmcmean::sum(gid, start_time, end_time, sampling_interval, model, gaps),
        3 => swing::sum(gid, start_time, end_time, sampling_interval, model, gaps),
        4 => gorilla::sum(gid, start_time, end_time, sampling_interval, model, gaps),
        _ => panic!("unknown model type"),
    }
}
