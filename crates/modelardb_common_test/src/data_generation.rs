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

use std::iter;
use std::ops::Range;

use arrow::array::ArrayBuilder;
use modelardb_common::types::{
    Timestamp, TimestampArray, TimestampBuilder, ValueArray, ValueBuilder,
};

use rand::distributions::Uniform;
use rand::{thread_rng, Rng};

/// The different structure of values which can be generated.
#[derive(Clone)]
pub enum ValuesStructure {
    Constant(Option<Range<f32>>),
    Linear(Option<Range<f32>>),
    Random(Range<f32>),
}

/// Generate a time series with sub-sequences of values with different [`ValuesStructure`]. The time
/// series will have at least `minimum_length` data points in sequences of `segment_length_range`
/// and the timestamp will be regular or irregular depending on the value of `irregular_timestamps`.
/// If `added_noise_range` is [`Some`], random values will be generated in the [`Range<f32>`] and
/// added to the sequences with constant and linear values. Sequences with random values are
/// generated in the range specified as `random_value_range`.
pub fn generate_time_series(
    minimum_length: usize,
    segment_length_range: Range<usize>,
    irregular_timestamps: bool,
    added_noise_range: Option<Range<f32>>,
    random_value_range: Range<f32>,
) -> (TimestampArray, ValueArray) {
    let values_structures = &[
        ValuesStructure::Constant(added_noise_range.clone()),
        ValuesStructure::Linear(added_noise_range),
        ValuesStructure::Random(random_value_range),
    ];

    let mut thread_rng = thread_rng();
    let mut timestamp_builder = TimestampBuilder::with_capacity(minimum_length);
    let mut value_builder = ValueBuilder::with_capacity(minimum_length);
    while timestamp_builder.len() < minimum_length {
        let segment_length = thread_rng.gen_range(segment_length_range.clone());
        let values_structure_index = thread_rng.gen_range(0..values_structures.len());
        let values_structure = &values_structures[values_structure_index];

        let uncompressed_timestamps = generate_timestamps(segment_length, irregular_timestamps);
        let uncompressed_values = generate_values(
            uncompressed_timestamps.values(),
            (*values_structure).clone(),
        );

        timestamp_builder.extend(&uncompressed_timestamps);
        value_builder.extend(&uncompressed_values);
    }

    debug_assert_eq!(timestamp_builder.len(), value_builder.len());
    (timestamp_builder.finish(), value_builder.finish())
}

/// Generate regular/irregular timestamps with [ThreadRng](rand::rngs::ThreadRng). Selects the
/// length and type of timestamps to be generated using the parameters `length` and `irregular`.
/// Returns the generated timestamps as a [`TimestampArray`].
pub fn generate_timestamps(length: usize, irregular: bool) -> TimestampArray {
    if irregular {
        let mut timestamps = TimestampBuilder::with_capacity(length);
        let mut thread_rng = thread_rng();
        let mut previous_timestamp: i64 = 0;
        for _ in 0..length {
            let next_timestamp = (thread_rng.sample(Uniform::from(10..20))) + previous_timestamp;
            timestamps.append_value(next_timestamp);
            previous_timestamp = next_timestamp;
        }
        timestamps.finish()
    } else {
        TimestampArray::from_iter_values((100..(length + 1) as i64 * 100).step_by(100))
    }
}

/// Generate multiple test values with different structure using [ThreadRng](rand::rngs::ThreadRng).
/// The amount of values to be generated will match `timestamps` and their structure will match
/// `structure_of_values`:
/// - If `structure_of_values` is `Constant`, a single value is generated and repeated with
/// a random value in the associated range added to each value if it is not [`None`].
/// - If `structure_of_values` is `Linear`, a sequence of increasing or decreasing values are
/// generated with a random value in the associated range added to each value if it is not [`None`].
/// - If `structure_of_values` is `Random`, a sequence of random values in the associated range is
/// generated.
pub fn generate_values(
    uncompressed_timestamps: &[Timestamp],
    values_structure: ValuesStructure,
) -> ValueArray {
    match values_structure {
        // Generates constant values.
        ValuesStructure::Constant(maybe_added_noise_range) => {
            let mut values = iter::repeat(thread_rng().gen()).take(uncompressed_timestamps.len());
            randomize_and_collect_iterator(maybe_added_noise_range, &mut values)
        }
        // Generates linear values.
        ValuesStructure::Linear(maybe_added_noise_range) => {
            // The variable slope is regenerated if it is 0, to avoid generating constant data.
            let mut slope: i64 = 0;
            while slope == 0 {
                slope = thread_rng().gen_range(-10..10);
            }
            let intercept: i64 = thread_rng().gen_range(1..50);

            let mut values = uncompressed_timestamps
                .iter()
                .map(|timestamp| (slope * timestamp + intercept) as f32);

            randomize_and_collect_iterator(maybe_added_noise_range, &mut values)
        }
        // Generates random values.
        ValuesStructure::Random(min_max) => {
            let mut thread_rng = thread_rng();
            let distr = Uniform::from(min_max);
            uncompressed_timestamps
                .iter()
                .map(|_| thread_rng.sample(distr))
                .collect()
        }
    }
}

/// Add a value in the `maybe_noise_range` to each value in `values` if `maybe_noise_range` is not
/// [`None`] and collect it to a [`Vec<f32>`] which is returned.
fn randomize_and_collect_iterator(
    maybe_noise_range: Option<Range<f32>>,
    values: &mut dyn Iterator<Item = f32>,
) -> ValueArray {
    if let Some(noise_range) = maybe_noise_range {
        let mut thread_rng = thread_rng();
        let distr = Uniform::from(noise_range);
        values
            .map(|value| value + thread_rng.sample(distr))
            .collect()
    } else {
        values.collect()
    }
}
