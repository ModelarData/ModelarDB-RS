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

impl DataStorageOptimizer {
    /// Create a new [`DataStorageOptimizer`] that optimizes the tables in `local_data_folder`,
    /// producing files of approximately `optimize_target_file_size_in_bytes` bytes and vacuuming
    /// with a retention period of `vacuum_retention_period_in_seconds` seconds. The estimate for
    /// each table is initialized with the combined size of its files smaller than the target size,  
    /// so small files written before a restart are not forgotten. If the files in `local_data_folder`
    /// could not be read, return [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    pub(super) async fn try_new(
        local_data_folder: DataFolder,
        optimize_target_file_size_in_bytes: u64,
        vacuum_retention_period_in_seconds: u64,
    ) -> Result<Self> {
        let table_names = local_data_folder.table_names().await?;

        let estimated_compactable_size_in_bytes = DashMap::with_capacity(table_names.len());
        for table_name in table_names {
            let compactable_size_in_bytes: u64 = local_data_folder
                .table_file_sizes(&table_name)
                .await?
                .into_iter()
                .filter(|size_in_bytes| *size_in_bytes < optimize_target_file_size_in_bytes)
                .sum();

            estimated_compactable_size_in_bytes.insert(table_name, compactable_size_in_bytes);
        }

        Ok(Self {
            local_data_folder,
            optimize_target_file_size_in_bytes,
            vacuum_retention_period_in_seconds,
            estimated_compactable_size_in_bytes,
        })
    }

    /// Increase the estimated compactable size of the table with `table_name` by `size_in_bytes`.
    /// If the estimate has reached `optimize_target_file_size_in_bytes`, the table's small files
    /// are compacted and the files left behind are vacuumed. The trigger assumes each newly written
    /// file is smaller than the target size. If the target is set below the size of a typical file,
    /// optimization is attempted on nearly every write, but is a harmless no-op. Returns [`Ok`] if
    /// the table did not need optimizing or was optimized successfully, otherwise
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    pub(super) async fn increase_estimated_compactable_size(
        &self,
        table_name: &str,
        size_in_bytes: u64,
    ) -> Result<()> {
        // entry() is not used as it would require the allocation of a new String for each lookup as
        // it must be given as a K, while get_mut() accepts the key as a &K so one K can be used.
        if !self
            .estimated_compactable_size_in_bytes
            .contains_key(table_name)
        {
            self.estimated_compactable_size_in_bytes
                .insert(table_name.to_owned(), 0);
        }
        *self
            .estimated_compactable_size_in_bytes
            .get_mut(table_name)
            .unwrap() += size_in_bytes;

        let estimate_reached_target = *self
            .estimated_compactable_size_in_bytes
            .get(table_name)
            .expect(&format!(
                "{table_name} should have been added to estimated_compactable_size_in_bytes."
            ))
            .value()
            >= self.optimize_target_file_size_in_bytes;

        if estimate_reached_target {
            self.optimize_and_vacuum_table(table_name).await?;
        }

        Ok(())
    }

    /// Compact the small files of the table with `table_name` into files of approximately
    /// `optimize_target_file_size_in_bytes` bytes, vacuum the small files left behind, and reset
    /// the table's estimated compactable size. Note that the vacuum can physically delete files
    /// that an in-flight query is still scanning if `vacuum_retention_period_in_seconds` is very
    /// low. Returns [`Ok`] if the table was optimized successfully, otherwise
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    async fn optimize_and_vacuum_table(&self, table_name: &str) -> Result<()> {
        debug!("Optimizing the storage of the table '{table_name}'.");

        self.local_data_folder
            .optimize_table(table_name, Some(self.optimize_target_file_size_in_bytes))
            .await?;

        self.local_data_folder
            .vacuum_table(table_name, Some(self.vacuum_retention_period_in_seconds))
            .await?;

        // Reset the estimate so the next optimization only counts data written from now on.
        *self
            .estimated_compactable_size_in_bytes
            .get_mut(table_name)
            .expect(&format!(
                "{table_name} should be in estimated_compactable_size_in_bytes."
            )) = 0;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_test::table::{self, TIME_SERIES_TABLE_NAME};
    use tempfile::{self, TempDir};

    const OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES: u64 = 1024 * 1024;
    const VACUUM_RETENTION_PERIOD_IN_SECONDS: u64 = 0;

    // Tests for increase_estimated_compactable_size().
    #[tokio::test]
    async fn test_optimize_table_when_estimate_reaches_target() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_table().await;
        let optimizer = create_data_storage_optimizer(local_data_folder.clone()).await;

        write_batches_to_table(&local_data_folder, 3).await;

        let initial_file_count = table_file_count(&local_data_folder).await;
        assert_eq!(initial_file_count, 3);

        optimizer
            .increase_estimated_compactable_size(
                TIME_SERIES_TABLE_NAME,
                OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES,
            )
            .await
            .unwrap();

        // The small files should have been compacted into a single file.
        assert_eq!(table_file_count(&local_data_folder).await, 1);

        // The estimate should have been reset after optimizing.
        assert_eq!(
            *optimizer
                .estimated_compactable_size_in_bytes
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_do_not_optimize_table_when_estimate_below_target() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_table().await;
        let optimizer = create_data_storage_optimizer(local_data_folder.clone()).await;

        write_batches_to_table(&local_data_folder, 3).await;

        let initial_file_count = table_file_count(&local_data_folder).await;
        assert_eq!(initial_file_count, 3);

        optimizer
            .increase_estimated_compactable_size(
                TIME_SERIES_TABLE_NAME,
                OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES - 1,
            )
            .await
            .unwrap();

        // No files should have been compacted since the estimate did not reach the target.
        assert_eq!(
            table_file_count(&local_data_folder).await,
            initial_file_count
        );

        // The estimate should have accumulated without being reset.
        assert_eq!(
            *optimizer
                .estimated_compactable_size_in_bytes
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap(),
            OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES - 1
        );
    }
    /// Create a [`DataFolder`] in a local [`TempDir`] containing a single time series table.
    async fn create_local_data_folder_with_table() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::open_local_url(temp_dir_url).await.unwrap();

        let time_series_table_metadata = table::time_series_table_metadata();
        local_data_folder
            .create_time_series_table(&time_series_table_metadata)
            .await
            .unwrap();

        (temp_dir, local_data_folder)
    }

    /// Write `batch_count` batches of compressed segments to the time series table in
    /// `local_data_folder`, each as a separate file.
    async fn write_batches_to_table(local_data_folder: &DataFolder, batch_count: u8) {
        for _ in 0..batch_count {
            local_data_folder
                .write_record_batches(
                    TIME_SERIES_TABLE_NAME,
                    vec![table::compressed_segments_record_batch()],
                )
                .await
                .unwrap();
        }
    }

    /// Return the number of active files in the time series table in `local_data_folder`.
    async fn table_file_count(local_data_folder: &DataFolder) -> usize {
        let mut delta_table = local_data_folder
            .delta_table(TIME_SERIES_TABLE_NAME)
            .await
            .unwrap();
        delta_table.load().await.unwrap();

        delta_table.get_file_uris().unwrap().count()
    }

    /// Create a [`DataStorageOptimizer`] that optimizes the tables in `local_data_folder`.
    async fn create_data_storage_optimizer(local_data_folder: DataFolder) -> DataStorageOptimizer {
        DataStorageOptimizer::try_new(
            local_data_folder,
            OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES,
            VACUUM_RETENTION_PERIOD_IN_SECONDS,
        )
        .await
        .unwrap()
    }
}
