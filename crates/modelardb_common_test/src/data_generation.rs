/* Copyright 2023 The ModelarDB Contributors
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

//! Implementation of functions that can generate timestamps and values with specific structure.

use rand::distributions::Uniform;
use rand::{thread_rng, Rng};

pub enum StructureOfValues {
    Constant,
    Random,
    Linear,
    AlmostLinear,
}

/// Generate regular/irregular timestamps with the [ThreadRng](rand::rngs::ThreadRng) randomizer.
/// Select the length and type of timestamps to be generated using the parameters `length` and
/// `irregular`. Returns the generated timestamps as a [`Vec`].
pub fn generate_timestamps(length: usize, irregular: bool) -> Vec<i64> {
    let mut timestamps = vec![];
    if irregular {
        let mut randomizer = thread_rng();
        let mut previous_timestamp: i64 = 0;
        for _ in 0..length {
            let next_timestamp = (randomizer.sample(Uniform::from(10..20))) + previous_timestamp;
            timestamps.push(next_timestamp);
            previous_timestamp = next_timestamp;
        }
    } else {
        timestamps = Vec::from_iter((100..(length + 1) as i64 * 100).step_by(100));
    }

    timestamps
}

/// Generate constant/random/linear/almost-linear test values with the
/// [ThreadRng](rand::rngs::ThreadRng) randomizer. The amount of values to be generated will match
/// `timestamps` and their structure will match [`StructureOfValues`]. If `Random` is selected,
/// `min` and `max` is the range of values which can be generated. If `AlmostLinear` is selected,
/// `min` and `max` is the maximum and minimum change that should be applied from one value to the
/// next. Returns the generated values as a [`Vec<f32>`].
pub fn generate_values(
    timestamps: &[i64],
    data_type: StructureOfValues,
    min: Option<f32>,
    max: Option<f32>,
) -> Vec<f32> {
    let mut randomizer = thread_rng();
    match data_type {
        // Generates almost linear data.
        StructureOfValues::AlmostLinear => {
            // The variable slope is regenerated if it is 0, to avoid generating constant data.
            let mut slope: i64 = 0;
            while slope == 0 {
                slope = thread_rng().gen_range(-10..10);
            }
            let intercept: i64 = thread_rng().gen_range(1..50);

            timestamps
                .iter()
                .map(|timestamp| {
                    (slope * timestamp + intercept) as f32
                        + randomizer.sample(Uniform::from(min.unwrap()..max.unwrap()))
                })
                .collect()
        }
        // Generates linear data.
        StructureOfValues::Linear => {
            // The variable slope is regenerated if it is 0, to avoid generating constant data.
            let mut slope: i64 = 0;
            while slope == 0 {
                slope = thread_rng().gen_range(-10..10);
            }
            let intercept: i64 = thread_rng().gen_range(1..50);

            timestamps
                .iter()
                .map(|timestamp| (slope * timestamp + intercept) as f32)
                .collect()
        }
        // Generates randomized data.
        StructureOfValues::Random => {
            let mut random = vec![];
            let mut randomizer = thread_rng();

            for _ in 0..timestamps.len() {
                random.push(randomizer.sample(Uniform::from(min.unwrap()..max.unwrap())));
            }

            random
        }
        // Generates constant data.
        StructureOfValues::Constant => {
            vec![thread_rng().gen(); timestamps.len()]
        }
    }
}
