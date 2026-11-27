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

//! Implementation of functions and types used throughout ModelarDB for testing purposes.

pub mod data_folder;
pub mod data_generation;
pub mod object_store;
pub mod system;
pub mod table;

/// Bucket and container name used by Silo and Azurite.
pub const BUCKET_AND_CONTAINER_NAME: &str = "modelardb";

/// Number of bytes reserved for ingested data in tests.
pub const INGESTED_RESERVED_MEMORY_IN_BYTES: u64 = 5 * 1024 * 1024; // 5 MiB.
