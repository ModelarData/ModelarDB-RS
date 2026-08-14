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

//! Support for automatically compacting the compressed data stored on disk. As compressed data is
//! saved, many small Apache Parquet files accumulate for each table. This component compacts a
//! table by merging those small files into fewer larger ones and vacuuming the files left behind,
//! reducing storage use and query time.

use std::sync::Arc;

use dashmap::DashMap;
use modelardb_storage::data_folder::DataFolder;
use tracing::debug;

use crate::error::Result;

/// Compacts each table by merging the many small Apache Parquet files that accumulate for it into
/// fewer larger files and vacuuming the files left behind. The component accumulates an estimate of
/// how much compactable data each table has and, once the estimate reaches the target file size,
/// compacts that table. The component only operates on the local data folder.
pub(super) struct DataStorageCompactor {
    /// The data folder containing all compressed data managed by the
    /// [`StorageEngine`](crate::storage::StorageEngine).
    local_data_folder: Arc<DataFolder>,
    /// The target size, in bytes, of the files produced when a table is optimized. A table is
    /// compacted once its `estimated_compactable_size_in_bytes` reaches this size, so the same
    /// value decides both when to compact and how large the optimized files are.
    optimize_target_file_size_in_bytes: u64,
    /// The retention period, in seconds, used when a table is vacuumed as part of compaction.
    /// Note that a very low value can let the vacuum physically delete files that an in-progress
    /// query is still scanning, causing that query to fail.
    vacuum_retention_period_in_seconds: u64,
    /// Map from table names to an estimate of how many bytes each table has in files smaller than
    /// the target size. The estimate over-approximates the on-disk size since it is increased by
    /// the in-memory size of the compressed data, so it ignores the compression applied by Apache
    /// Parquet. Note that it is not reduced when a table is truncated by the data transfer
    /// component for simplicity.
    estimated_compactable_size_in_bytes: DashMap<String, u64>,
}

impl DataStorageCompactor {
    /// Create a new [`DataStorageCompactor`] that compacts the tables in `local_data_folder`,
    /// producing files of approximately `optimize_target_file_size_in_bytes` bytes and vacuuming
    /// with a retention period of `vacuum_retention_period_in_seconds` seconds. The estimate for
    /// each table is initialized with the combined size of its files smaller than the target size,
    /// so small files written before a restart are not forgotten. If the files in `local_data_folder`
    /// could not be read, return [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    pub(super) async fn try_new(
        local_data_folder: Arc<DataFolder>,
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
    /// If the estimate has reached `optimize_target_file_size_in_bytes`, the table is compacted.
    /// The trigger assumes each newly written file is smaller than the target size. If the target
    /// is set below the size of a typical file, compaction is attempted on nearly every write, but
    /// is a harmless no-op. Returns [`Ok`] if the table did not need compacting or was compacted
    /// successfully, otherwise [`ModelarDbServerError`](crate::error::ModelarDbServerError).
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
            .expect("table_name should have been added to estimated_compactable_size_in_bytes.")
            .value()
            >= self.optimize_target_file_size_in_bytes;

        if estimate_reached_target {
            self.compact_table(table_name).await?;
        }

        Ok(())
    }

    /// Compact the table with `table_name` by merging its small files into files of approximately
    /// `optimize_target_file_size_in_bytes` bytes, vacuuming the files left behind, and resetting
    /// the table's estimated compactable size. Note that the vacuum can physically delete files
    /// that an in-progress query is still scanning if `vacuum_retention_period_in_seconds` is very
    /// low. Returns [`Ok`] if the table was compacted successfully, otherwise
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    async fn compact_table(&self, table_name: &str) -> Result<()> {
        debug!("Compacting the storage of the table '{table_name}'.");

        self.local_data_folder
            .optimize_table(table_name, Some(self.optimize_target_file_size_in_bytes))
            .await?;

        self.local_data_folder
            .vacuum_table(table_name, Some(self.vacuum_retention_period_in_seconds))
            .await?;

        // Reset the estimate so the next compaction only counts data written from now on.
        *self
            .estimated_compactable_size_in_bytes
            .get_mut(table_name)
            .expect("table_name should be in estimated_compactable_size_in_bytes.") = 0;

        Ok(())
    }

    /// Set the target size, in bytes, of the files produced when a table is optimized to
    /// `new_optimize_target_file_size_in_bytes`. The new target takes effect the next time each
    /// table is written to. Tables are not re-compacted here to keep configuration updates cheap
    /// and to avoid having to re-check all files on disk to see if they are compactable.
    pub(super) fn set_optimize_target_file_size_in_bytes(
        &mut self,
        new_optimize_target_file_size_in_bytes: u64,
    ) {
        self.optimize_target_file_size_in_bytes = new_optimize_target_file_size_in_bytes;
    }

    /// Set the retention period, in seconds, used when a table is vacuumed as part of compaction to
    /// `new_vacuum_retention_period_in_seconds`.
    pub(super) fn set_vacuum_retention_period_in_seconds(
        &mut self,
        new_vacuum_retention_period_in_seconds: u64,
    ) {
        self.vacuum_retention_period_in_seconds = new_vacuum_retention_period_in_seconds;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_test::table::{self, TIME_SERIES_TABLE_NAME};
    use tempfile::{self, TempDir};

    const OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES: u64 = 1024 * 1024;
    const VACUUM_RETENTION_PERIOD_IN_SECONDS: u64 = 0;
    const BATCH_COUNT: u8 = 3;

    // Tests for try_new().
    #[tokio::test]
    async fn test_initialize_estimate_from_existing_small_files() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_table().await;
        write_batches_to_table(&local_data_folder, BATCH_COUNT).await;

        // The compactor is created after the data is written, so its estimate includes the small
        // files already on disk.
        let compactor = create_data_storage_compactor(local_data_folder.clone()).await;

        let expected_estimate: u64 = local_data_folder
            .table_file_sizes(TIME_SERIES_TABLE_NAME)
            .await
            .unwrap()
            .into_iter()
            .sum();
        assert!(expected_estimate > 0);

        assert_eq!(
            *compactor
                .estimated_compactable_size_in_bytes
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap(),
            expected_estimate
        );
    }

    #[tokio::test]
    async fn test_initialize_estimate_excludes_files_at_or_above_target() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_table().await;
        write_batches_to_table(&local_data_folder, BATCH_COUNT).await;

        // With a one-byte target, every existing file is already at or above the target, so none of
        // them count towards the compactable backlog.
        let compactor = DataStorageCompactor::try_new(local_data_folder.clone(), 1, 0)
            .await
            .unwrap();

        assert_eq!(table_file_count(&local_data_folder), BATCH_COUNT);

        assert_eq!(
            *compactor
                .estimated_compactable_size_in_bytes
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap(),
            0
        );
    }

    // Tests for increase_estimated_compactable_size().
    #[tokio::test]
    async fn test_compact_table_when_estimate_reaches_target() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_table().await;
        let compactor = create_data_storage_compactor(local_data_folder.clone()).await;

        write_batches_to_table(&local_data_folder, BATCH_COUNT).await;

        let initial_file_count = table_file_count(&local_data_folder);
        assert_eq!(initial_file_count, BATCH_COUNT);

        compactor
            .increase_estimated_compactable_size(
                TIME_SERIES_TABLE_NAME,
                OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES,
            )
            .await
            .unwrap();

        // The small files should have been compacted into a single file.
        assert_eq!(table_file_count(&local_data_folder), 1);

        // The estimate should have been reset after compacting.
        assert_eq!(
            *compactor
                .estimated_compactable_size_in_bytes
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_do_not_compact_table_when_estimate_below_target() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_table().await;
        let compactor = create_data_storage_compactor(local_data_folder.clone()).await;

        write_batches_to_table(&local_data_folder, BATCH_COUNT).await;

        let initial_file_count = table_file_count(&local_data_folder);
        assert_eq!(initial_file_count, BATCH_COUNT);

        compactor
            .increase_estimated_compactable_size(
                TIME_SERIES_TABLE_NAME,
                OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES - 1,
            )
            .await
            .unwrap();

        // No files should have been compacted since the estimate did not reach the target.
        assert_eq!(table_file_count(&local_data_folder), initial_file_count);

        // The estimate should have accumulated without being reset.
        assert_eq!(
            *compactor
                .estimated_compactable_size_in_bytes
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap(),
            OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES - 1
        );
    }

    /// Create a [`DataFolder`] in a local [`TempDir`] containing a single time series table.
    async fn create_local_data_folder_with_table() -> (TempDir, Arc<DataFolder>) {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = Arc::new(DataFolder::open_local_url(temp_dir_url).await.unwrap());

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

    /// Return the number of physical Apache Parquet files in the time series table in
    /// `local_data_folder`.
    fn table_file_count(local_data_folder: &DataFolder) -> u8 {
        let column_path = format!(
            "{}/tables/{}/field_column=0",
            local_data_folder.location(),
            TIME_SERIES_TABLE_NAME
        );

        std::fs::read_dir(column_path).unwrap().count() as u8
    }

    /// Create a [`DataStorageCompactor`] that compacts the tables in `local_data_folder`.
    async fn create_data_storage_compactor(
        local_data_folder: Arc<DataFolder>,
    ) -> DataStorageCompactor {
        DataStorageCompactor::try_new(
            local_data_folder,
            OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES,
            VACUUM_RETENTION_PERIOD_IN_SECONDS,
        )
        .await
        .unwrap()
    }
}
