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

/** Public Type **/
struct PMCMeanFitState {
    error: f32,
    size: u32,
    min: f32,
    max: f32,
    sum: f64,
}

impl PMCMeanFitState {
    fn new(error: f32) -> Self {
        Self {
            error,
            size: 0,
            min: f32::NAN,
            max: f32::NAN,
            sum: 0.0,
        }
    }

    fn fit(&mut self, value: f32) -> bool {
        let next_sum = self.sum + value as f64;
        let average = next_sum / (self.size + 1) as f64;

        let next_min = f32::min(self.min, value);
        let next_max = f32::max(self.max, value);
        if PMCMeanFitState::within_percentage_error_bound(self.error, average as f32, next_min)
            || PMCMeanFitState::within_percentage_error_bound(self.error, average as f32, next_max)
        {
            self.min = next_min;
            self.max = next_max;
            self.sum = next_sum;
            self.size += 1;
            true
        } else {
            false
        }
    }

    fn get(&self) -> f32 {
        (self.sum / self.size as f64) as f32
    }

    fn within_percentage_error_bound(error: f32, real: f32, approximation: f32) -> bool {
	//Necessary as the method would return NaN if approximation and real are zero
	if approximation == real || (real.is_nan() && approximation.is_nan()) {
	    true
	} else {
	    let difference = real - approximation;
	    let result = f32::abs(difference / real);
	    (result * 100.0) <= error
	}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::num::f32;
    use proptest::{prop_assert, prop_assume, proptest};

    pub static SEQUENCE_OF_NAN: [f32; 5] = [f32::NAN, f32::NAN, f32::NAN, f32::NAN, f32::NAN];

    pub static SEQUENCE_OF_POSITIVE_INFINITY: [f32; 5] = [
        f32::INFINITY,
        f32::INFINITY,
        f32::INFINITY,
        f32::INFINITY,
        f32::INFINITY,
    ];

    pub static SEQUENCE_OF_NEGATIVE_INFINITY: [f32; 5] = [
        f32::NEG_INFINITY,
        f32::NEG_INFINITY,
        f32::NEG_INFINITY,
        f32::NEG_INFINITY,
        f32::NEG_INFINITY,
    ];

    #[test]
    fn test_fit_sequence_of_nans() {
        let mut state = PMCMeanFitState::new(0.0);
        for value in SEQUENCE_OF_NAN {
            assert!(state.fit(value));
        }
        assert!(state.get().is_nan());
    }

    #[test]
    fn test_fit_sequence_of_positive_infinity() {
        let mut state = PMCMeanFitState::new(0.0);
        for value in SEQUENCE_OF_POSITIVE_INFINITY {
            assert!(state.fit(value));
        }
        assert!(state.get() == f32::INFINITY);
    }

    #[test]
    fn test_fit_sequence_of_negative_infinity() {
        let mut state = PMCMeanFitState::new(0.0);
        for value in SEQUENCE_OF_NEGATIVE_INFINITY {
            assert!(state.fit(value));
        }
        assert!(state.get() == f32::NEG_INFINITY);
    }

    proptest! {
        #[test]
        fn test_fit_one_value_always_succeeds(value in f32::ANY) {
            prop_assert!(PMCMeanFitState::new(0.0).fit(value));
        }

        #[test]
        fn test_fit_nan_other_always_fails(value in f32::ANY) {
            prop_assume!( ! value.is_nan());
            let mut state = PMCMeanFitState::new(0.0);
            assert!(state.fit(f32::NAN));
            prop_assert!( ! state.fit(value));
        }

        #[test]
        fn test_fit_positive_infinity_other_always_fails(value in f32::ANY) {
            prop_assume!(value != f32::INFINITY);
            let mut state = PMCMeanFitState::new(0.0);
            assert!(state.fit(f32::INFINITY));
            prop_assert!( ! state.fit(value));
        }

        #[test]
        fn test_fit_negative_infinity_other_always_fails(value in f32::ANY) {
            prop_assume!(value != f32::NEG_INFINITY);
            let mut state = PMCMeanFitState::new(0.0);
            assert!(state.fit(f32::NEG_INFINITY));
            prop_assert!( ! state.fit(value));
        }
    }
}

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
