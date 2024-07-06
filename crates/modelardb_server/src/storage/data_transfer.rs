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

//! Support for efficiently transferring data to a remote object store. Data saved locally on disk
//! is managed here until it is of a sufficient size to be transferred efficiently.

use std::io::Error as IOError;
use std::io::ErrorKind::Other;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use datafusion::parquet::errors::ParquetError;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::compute;
use deltalake::{DeltaOps, DeltaTableError};
use futures::TryStreamExt;
use modelardb_common::storage::DeltaLake;
use tokio::sync::RwLock;
use tokio::task::JoinHandle as TaskJoinHandle;
use tracing::debug;

use crate::storage::Metric;

// TODO: Handle the case where a connection can not be established when transferring data.
// TODO: Handle deleting the files after the transfer is complete in a safe way to avoid
//       transferring the same data multiple times.

pub struct DataTransfer {
    /// The delta lake containing all compressed data managed by the
    /// [`StorageEngine`](crate::storage::StorageEngine).
    local_data_folder: Arc<DeltaLake>,
    /// The delta lake that the data should be transferred to.
    remote_data_folder: Arc<DeltaLake>,
    /// Map from table names to the current size of the table in bytes.
    table_size_in_bytes: DashMap<String, usize>,
    /// The number of bytes that are required before transferring a batch of data to the remote
    /// delta lake. If [`None`], data is not transferred based on batch size.
    transfer_batch_size_in_bytes: Option<usize>,
    /// Handle to the task that transfers data periodically to the remote object store. If [`None`],
    /// data is not transferred based on time.
    transfer_scheduler_handle: Option<TaskJoinHandle<()>>,
    /// Metric for the total used disk space in bytes, updated when data is transferred.
    pub used_disk_space_metric: Arc<Mutex<Metric>>,
}

impl DataTransfer {
    /// Create a new data transfer instance and initialize it with the data already in
    /// `local_data_folder_path`. If `local_data_folder_path` or a path within
    /// `local_data_folder_path` could not be read, return [`DeltaLake`].
    pub async fn try_new(
        local_data_folder: Arc<DeltaLake>,
        remote_data_folder: Arc<DeltaLake>,
        table_names: Vec<String>,
        transfer_batch_size_in_bytes: Option<usize>,
        used_disk_space_metric: Arc<Mutex<Metric>>,
    ) -> Result<Self, DeltaTableError> {
        // The size of tables is computed manually as datafusion_table_statistics() is not exact.
        let table_size_in_bytes = DashMap::with_capacity(table_names.len());
        for table_name in table_names {
            let delta_table = local_data_folder.delta_table(&table_name).await?;
            let mut table_size_in_bytes = table_size_in_bytes.entry(table_name).or_insert(0);

            let object_store = delta_table.object_store();
            for file_path in delta_table.get_files_iter()? {
                let object_meta = object_store
                    .head(&file_path)
                    .await
                    .map_err(|error| DeltaTableError::MetadataError(error.to_string()))?;
                *table_size_in_bytes += object_meta.size;
            }
        }

        let data_transfer = Self {
            local_data_folder,
            remote_data_folder,
            table_size_in_bytes: table_size_in_bytes.clone(),
            transfer_batch_size_in_bytes,
            transfer_scheduler_handle: None,
            used_disk_space_metric,
        };

        // Record the initial used disk space.
        let initial_disk_space: usize = table_size_in_bytes.iter().map(|kv| *kv.value()).sum();

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        data_transfer
            .used_disk_space_metric
            .lock()
            .unwrap()
            .append(initial_disk_space as isize, true);

        // Check if data should be transferred immediately.
        for table_name_field_column_index_size_in_bytes in table_size_in_bytes.iter() {
            let table_name = &table_name_field_column_index_size_in_bytes.key();
            let size_in_bytes = table_name_field_column_index_size_in_bytes.value();

            if transfer_batch_size_in_bytes.is_some_and(|batch_size| size_in_bytes >= &batch_size) {
                data_transfer
                    .transfer_data(table_name)
                    .await
                    .map_err(|err| IOError::new(Other, err.to_string()))?;
            }
        }

        Ok(data_transfer)
    }

    /// Increase the size of the table with `table_name` by `size_in_bytes`.
    pub async fn increase_table_size(
        &self,
        table_name: &str,
        size_in_bytes: usize,
    ) -> Result<(), DeltaTableError> {
        // entry() is not used as it would require the allocation of a new String for each lookup as
        // it must be given as a K, while get_mut() accepts the key as a &K so one K can be used.
        if !self.table_size_in_bytes.contains_key(table_name) {
            self.table_size_in_bytes.insert(table_name.to_owned(), 0);
        }
        *self.table_size_in_bytes.get_mut(table_name).unwrap() += size_in_bytes;

        // If the combined size of the files is larger than the batch size, transfer the data to the
        // remote object store. unwrap() is safe as key has just been added to the map.
        if self.transfer_batch_size_in_bytes.is_some_and(|batch_size| {
            self.table_size_in_bytes.get(table_name).unwrap().value() >= &batch_size
        }) {
            self.transfer_data(table_name).await?;
        }

        Ok(())
    }

    /// Update the remote data folder, used to transfer data to.
    pub(super) async fn update_remote_data_folder(&mut self, remote_data_folder: Arc<DeltaLake>) {
        self.remote_data_folder = remote_data_folder;
    }

    /// Set the transfer batch size to `new_value`. For each table that compressed data is saved
    /// for, check if the amount of data exceeds `new_value` and transfer all the data if it does.
    /// If the value is changed successfully return [`Ok`], otherwise return [`ParquetError`].
    pub(super) async fn set_transfer_batch_size_in_bytes(
        &mut self,
        new_value: Option<usize>,
    ) -> Result<(), ParquetError> {
        if let Some(new_batch_size) = new_value {
            self.transfer_larger_than_threshold(new_batch_size).await?;
        }

        self.transfer_batch_size_in_bytes = new_value;

        Ok(())
    }

    /// If a new transfer time is given, stop the existing task transferring data periodically,
    /// if there is one, and start a new task. If `new_value` is [`None`], the task is just stopped.
    /// `data_transfer` is needed as an argument instead of using `self` so it can be moved into the
    /// periodic task.
    pub(super) fn set_transfer_time_in_seconds(
        &mut self,
        new_value: Option<usize>,
        data_transfer: Arc<RwLock<Option<Self>>>,
    ) {
        // Stop the current task periodically transferring data if there is one.
        if let Some(task) = &self.transfer_scheduler_handle {
            task.abort();
        }

        // If a transfer time was given, create the task that periodically transfers data.
        self.transfer_scheduler_handle = if let Some(transfer_time) = new_value {
            let join_handle = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(transfer_time as u64)).await;

                    data_transfer
                        .write()
                        .await
                        .as_ref()
                        .unwrap()
                        .transfer_larger_than_threshold(0)
                        .await
                        .expect("Periodic data transfer failed.");
                }
            });

            Some(join_handle)
        } else {
            None
        };
    }

    /// Transfer all compressed files from tables currently using more than `threshold` bytes in
    /// the data folder to the remote object store. Return [`Ok`] if all files were transferred
    /// successfully, otherwise [`ParquetError`]. Note that if the function fails, some of the
    /// compressed files may still have been transferred. Since the data is transferred separately
    /// for each table, the function can be called again if it failed.
    pub(crate) async fn transfer_larger_than_threshold(
        &self,
        threshold: usize,
    ) -> Result<(), ParquetError> {
        // The clone is performed to not create a deadlock with transfer_data().
        for table_name_column_index_size_in_bytes in self.table_size_in_bytes.clone().iter() {
            let table_name = table_name_column_index_size_in_bytes.key();
            let size_in_bytes = table_name_column_index_size_in_bytes.value();

            if size_in_bytes > &threshold {
                self.transfer_data(table_name)
                    .await
                    .map_err(|err| IOError::new(Other, err.to_string()))?;
            }
        }

        Ok(())
    }

    /// Transfer the data stored locally for the table with `table_name` to the remote object store.
    /// Once successfully transferred, the data is deleted from local storage. Return [`Ok`] if the
    /// files were transferred successfully, otherwise [`DeltaTableError`].
    async fn transfer_data(&self, table_name: &str) -> Result<(), DeltaTableError> {
        // All compressed segments will be transferred so the current size in bytes is also the
        // amount of data that will be transferred. unwrap() is safe as the table contains data.
        let current_size_in_bytes = *self.table_size_in_bytes.get(table_name).unwrap().value();
        let local_delta_ops = self.local_data_folder.delta_ops(table_name).await?;

        // Read the data that is currently stored for the table with table_name.
        let (table, stream) = local_delta_ops.load().await?;
        let record_batches: Vec<RecordBatch> = stream.try_collect().await?;

        // unwrap() is safe as all of the record batches are from the same delta table.
        let compressed_segments =
            compute::concat_batches(&record_batches[0].schema(), &record_batches).unwrap();

        debug!(
            "Transferring {current_size_in_bytes} as {} compressed segments for the table '{table_name}'.",
            compressed_segments.num_rows(),
        );

        // Write the data to the remote delta lake and commit it.
        self.remote_data_folder
            .write_compressed_segments_to_model_table(table_name, compressed_segments)
            .await?;

        // Delete the data that has been transferred to the remote delta lake.
        let local_delta_ops: DeltaOps = table.into();
        local_delta_ops.delete().await?;

        // Remove the transferred data from the in-memory tracking of compressed files.
        *self.table_size_in_bytes.get_mut(table_name).unwrap() -= current_size_in_bytes;

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_disk_space_metric
            .lock()
            .unwrap()
            .append(-(current_size_in_bytes as isize), true);

        debug!("Transferred {current_size_in_bytes} bytes to the remote table '{table_name}'.");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_common::metadata;
    use modelardb_common::metadata::TableMetadataManager;
    use modelardb_common::test;
    use object_store::local::LocalFileSystem;
    use ringbuf::traits::observer::Observer;
    use sqlx::Sqlite;
    use tempfile::{self, TempDir};

    const FILE_SIZE: usize = 2395;

    // Tests for data transfer component.
    #[tokio::test]
    async fn test_include_existing_files_on_start_up() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(&temp_dir).await;

        create_delta_table_and_segments(&temp_dir, &metadata_manager, 1).await;

        let (_target_dir, data_transfer) =
            create_data_transfer_component(&temp_dir, metadata_manager).await;

        assert_eq!(
            *data_transfer
                .table_size_in_bytes
                .get(test::MODEL_TABLE_NAME)
                .unwrap(),
            FILE_SIZE
        );

        assert_eq!(
            data_transfer
                .used_disk_space_metric
                .lock()
                .unwrap()
                .values()
                .occupied_len(),
            1
        );
    }

    #[tokio::test]
    async fn test_add_batch_to_new_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(&temp_dir).await;

        let (_target_dir, data_transfer) =
            create_data_transfer_component(&temp_dir, metadata_manager.clone()).await;
        let files_size = create_delta_table_and_segments(&temp_dir, &metadata_manager, 1).await;

        assert!(data_transfer
            .increase_table_size(test::MODEL_TABLE_NAME, files_size)
            .await
            .is_ok());

        assert_eq!(
            *data_transfer
                .table_size_in_bytes
                .get(test::MODEL_TABLE_NAME)
                .unwrap(),
            FILE_SIZE
        );
    }

    #[tokio::test]
    async fn test_add_batch_to_existing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(&temp_dir).await;

        let (_target_dir, data_transfer) =
            create_data_transfer_component(&temp_dir, metadata_manager.clone()).await;
        let files_size = create_delta_table_and_segments(&temp_dir, &metadata_manager, 1).await;

        data_transfer
            .increase_table_size(test::MODEL_TABLE_NAME, files_size)
            .await
            .unwrap();
        data_transfer
            .increase_table_size(test::MODEL_TABLE_NAME, files_size)
            .await
            .unwrap();

        assert_eq!(
            *data_transfer
                .table_size_in_bytes
                .get(test::MODEL_TABLE_NAME)
                .unwrap(),
            FILE_SIZE * 2
        );
    }

    #[tokio::test]
    async fn test_increase_transfer_batch_size_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(&temp_dir).await;

        create_delta_table_and_segments(&temp_dir, &metadata_manager, 2).await;

        let (_, mut data_transfer) =
            create_data_transfer_component(&temp_dir, metadata_manager).await;

        data_transfer
            .set_transfer_batch_size_in_bytes(Some(FILE_SIZE * 10))
            .await
            .unwrap();

        assert_eq!(
            data_transfer.transfer_batch_size_in_bytes,
            Some(FILE_SIZE * 10)
        );

        // Data should not have been transferred.
        assert_eq!(
            *data_transfer
                .table_size_in_bytes
                .get(test::MODEL_TABLE_NAME)
                .unwrap(),
            FILE_SIZE * 2
        );
    }

    #[tokio::test]
    async fn test_set_transfer_batch_size_in_bytes_to_none() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(&temp_dir).await;

        create_delta_table_and_segments(&temp_dir, &metadata_manager, 2).await;

        let (_, mut data_transfer) =
            create_data_transfer_component(&temp_dir, metadata_manager).await;

        assert_eq!(
            data_transfer.transfer_batch_size_in_bytes,
            Some(FILE_SIZE * 3 - 1)
        );

        data_transfer
            .set_transfer_batch_size_in_bytes(None)
            .await
            .unwrap();

        assert_eq!(data_transfer.transfer_batch_size_in_bytes, None);

        // Data should not have been transferred.
        assert_eq!(
            *data_transfer
                .table_size_in_bytes
                .get(test::MODEL_TABLE_NAME)
                .unwrap(),
            FILE_SIZE * 2
        );
    }

    /// Set up a delta table and write `batch_write_count` batches of compressed segments to it.
    /// Returns the size of the files on disk.
    async fn create_delta_table_and_segments(
        temp_dir: &TempDir,
        metadata_manager: &Arc<TableMetadataManager<Sqlite>>,
        batch_write_count: usize
    ) -> usize {
        let local_data_folder =
            Arc::new(DeltaLake::from_local_path(temp_dir.path().to_str().unwrap()).unwrap());

        local_data_folder
            .create_delta_lake_model_table(test::MODEL_TABLE_NAME)
            .await
            .unwrap();

        // Registered as a normal table as it does not require construction of a metadata object.
        metadata_manager
            .save_table_metadata(test::MODEL_TABLE_NAME, "")
            .await
            .unwrap();

        for _ in 0..batch_write_count {
            let compressed_segments = test::compressed_segments_record_batch();
            local_data_folder
                .write_compressed_segments_to_model_table(test::MODEL_TABLE_NAME, compressed_segments)
                .await
                .unwrap();
        }

        let mut delta_table = local_data_folder
            .delta_table(test::MODEL_TABLE_NAME)
            .await
            .unwrap();
        delta_table.load().await.unwrap();

        let mut files_size = 0;
        for file_path in delta_table.get_files_iter().unwrap() {
            files_size += delta_table
                .object_store()
                .head(&file_path)
                .await
                .unwrap()
                .size;
        }

        files_size
    }

    /// Create a data transfer component with a target object store that is deleted once the test is finished.
    async fn create_data_transfer_component(
        temp_dir: &TempDir,
        table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
    ) -> (TempDir, DataTransfer) {
        let local_data_folder =
            Arc::new(DeltaLake::from_local_path(temp_dir.path().to_str().unwrap()).unwrap());

        let target_dir = tempfile::tempdir().unwrap();
        let remote_data_folder =
            Arc::new(DeltaLake::from_local_path(target_dir.path().to_str().unwrap()).unwrap());

        let data_transfer = DataTransfer::try_new(
            local_data_folder,
            remote_data_folder,
            table_metadata_manager.table_names().await.unwrap(),
            Some(FILE_SIZE * 3 - 1),
            Arc::new(Mutex::new(Metric::new())),
        )
        .await
        .unwrap();

        (target_dir, data_transfer)
    }

    /// Create a table metadata manager and save a single model table to the metadata database.
    async fn create_metadata_manager(temp_dir: &TempDir) -> Arc<TableMetadataManager<Sqlite>> {
        let local_data_folder = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

        let metadata_manager = metadata::try_new_sqlite_table_metadata_manager(&local_data_folder)
            .await
            .unwrap();

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        Arc::new(metadata_manager)
    }
}
