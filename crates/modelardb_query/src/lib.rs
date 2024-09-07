/* Copyright 2024 The ModelarDB Contributors
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

pub mod optimizer;
pub mod query;

// Re-export the few functions and types users are meant to use.
pub use query::model_table::ModelTable;
pub use query::table::Table;
