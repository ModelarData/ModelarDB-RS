/* Copyright 2022 The ModelarDB Contributors
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

//! Compress batches of sorted data points to produce compressed segments containing metadata and
//! models, merge compressed segments if possible within the error bound, compute aggregates
//! directly from the segments, and reconstruct the data points for each compressed segment.

#![allow(clippy::too_many_arguments)]

mod compression;
mod merge;
mod models;
mod types;

// Re-export the few functions and types users are meant to use.
pub use compression::try_compress;
pub use merge::try_merge_segments;
pub use models::grid;
pub use models::is_value_within_error_bound;
pub use models::len;
pub use models::sum;
pub use models::timestamps::are_compressed_timestamps_regular;
pub use models::{MODEL_TYPE_COUNT, MODEL_TYPE_NAMES};

// Named error bound values to make tests more readable.
#[cfg(test)]
mod tests {
    pub(crate) const ERROR_BOUND_ZERO: f32 = 0.0;
    pub(crate) const ERROR_BOUND_FIVE: f32 = 5.0;
    pub(crate) const ERROR_BOUND_TEN: f32 = 10.0;
    pub(crate) const ERROR_BOUND_ABSOLUTE_MAX: f32 = f32::MAX;
    pub(crate) const ERROR_BOUND_RELATIVE_MAX: f32 = 100.0;
}
