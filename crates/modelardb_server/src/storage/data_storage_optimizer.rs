/* Copyright 2026 The ModelarDB Contributors
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

//! Support for automatically optimizing how compressed data is stored on disk. As compressed data
//! is saved, many small Apache Parquet files accumulate for each table. This component compacts
//! those small files into fewer larger files and vacuums the small files left behind to reduce
//! storage use and query time.

use dashmap::DashMap;
use modelardb_storage::data_folder::DataFolder;
use tracing::debug;

use crate::error::Result;

/// Compacts the many small Apache Parquet files that accumulate for a table into fewer larger files
/// and vacuums the files left behind by the compaction. The component accumulates an estimate of
/// how much compactable data each table has and, once the estimate reaches the target file size,
/// optimizes and vacuums that table. The component only operates on the local data folder.
pub(super) struct DataStorageOptimizer {
    /// The data folder containing all compressed data managed by the
    /// [`StorageEngine`](crate::storage::StorageEngine).
    local_data_folder: DataFolder,
    /// The target size, in bytes, of the files produced when a table is optimized. Also used as the
    /// trigger for when a table is optimized.
    optimize_target_file_size_in_bytes: u64,
    /// The retention period, in seconds, used when a table is vacuumed after it is optimized.
    /// Note that a very low value can let the vacuum physically delete files that an in-flight
    /// query is still scanning, causing that query to fail.
    vacuum_retention_period_in_seconds: u64,
    /// Map from table names to an estimate of how many bytes each table has in files smaller than
    /// the target size. The estimate over-approximates the on-disk size since it is increased by
    /// the in-memory size of the compressed data, so it ignores the compression applied by Apache
    /// Parquet. Note that it is not reduced when a table is truncated by the data transfer
    /// component for simplicity.
    estimated_compactable_size_in_bytes: DashMap<String, u64>,
}
