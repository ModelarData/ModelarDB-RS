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
use futures::StreamExt;
use modelardb_common::metadata::compressed_file::CompressedFile;
use modelardb_common::metadata::TableMetadataManager;
use object_store::local::LocalFileSystem;
use object_store::path::{Path, PathPart};
use object_store::{ObjectMeta, ObjectStore};
use sqlx::Sqlite;
use tokio::sync::RwLock;
use tokio::task::JoinHandle as TaskJoinHandle;
use tracing::debug;

use crate::manager::Manager;
use crate::storage::compressed_data_manager::CompressedDataManager;
use crate::storage::Metric;
use crate::storage::COMPRESSED_DATA_FOLDER;

// TODO: Handle the case where a connection can not be established when transferring data.
// TODO: Handle deleting the files after the transfer is complete in a safe way to avoid
//       transferring the same data multiple times or deleting files that are currently used
//       elsewhere.

pub struct DataTransfer {
    /// The object store containing all compressed data managed by the
    /// [`StorageEngine`](crate::storage::StorageEngine).
    local_data_folder: Arc<LocalFileSystem>,
    /// The object store that the data should be transferred to.
    pub remote_data_folder: Arc<dyn ObjectStore>,
    /// Management of metadata for deleting file metadata after transferring.
    table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
    /// Map from table names and column indices to the combined size in bytes of the compressed
    /// files currently saved for the column in that table.
    compressed_files: DashMap<(String, u16), usize>,
    /// Interface to access the manager and transfer metadata when data is transferred.
    manager: Manager,
    /// The number of bytes that are required before transferring a batch of data to the remote
    /// object store. If [`None`], data is not transferred based on batch size.
    transfer_batch_size_in_bytes: Option<usize>,
    /// Handle to the task that transfers data periodically to the remote object store. If [`None`],
    /// data is not transferred based on time.
    transfer_scheduler_handle: Option<TaskJoinHandle<()>>,
    /// Metric for the total used disk space in bytes, updated when data is transferred.
    pub used_disk_space_metric: Arc<Mutex<Metric>>,
}

impl DataTransfer {
    /// Create a new data transfer instance and initialize it with the compressed files already
    /// existing in `local_data_folder_path`. If `local_data_folder_path` or a path within
    /// `local_data_folder_path` could not be read, return [`IOError`].
    pub async fn try_new(
        local_data_folder: Arc<LocalFileSystem>,
        remote_data_folder: Arc<dyn ObjectStore>,
        table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
        manager: Manager,
        transfer_batch_size_in_bytes: Option<usize>,
        used_disk_space_metric: Arc<Mutex<Metric>>,
    ) -> Result<Self, IOError> {
        // Parse through the data folder to retrieve already existing files that should be transferred.
        let list_stream = local_data_folder.list(None);

        let compressed_files = list_stream
            .fold(DashMap::new(), |acc, maybe_meta| async {
                if let Ok(meta) = maybe_meta {
                    // If the file is a compressed file, add the size of it to the total size of the
                    // files stored for the table with that table name.
                    if let Some(table_name) = Self::path_is_compressed_file(meta.location) {
                        *acc.entry(table_name).or_insert(0) += meta.size;
                    }
                }

                acc
            })
            .await;

        let data_transfer = Self {
            local_data_folder,
            remote_data_folder,
            table_metadata_manager,
            compressed_files: compressed_files.clone(),
            manager,
            transfer_batch_size_in_bytes,
            transfer_scheduler_handle: None,
            used_disk_space_metric,
        };

        // Record the initial used disk space.
        let initial_disk_space: usize = compressed_files.iter().map(|kv| *kv.value()).sum();

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        data_transfer
            .used_disk_space_metric
            .lock()
            .unwrap()
            .append(initial_disk_space as isize, true);

        // Check if data should be transferred immediately.
        for table_name_column_index_size_in_bytes in compressed_files.iter() {
            let table_name = &table_name_column_index_size_in_bytes.key().0;
            let column_index = table_name_column_index_size_in_bytes.key().1;
            let size_in_bytes = table_name_column_index_size_in_bytes.value();

            if transfer_batch_size_in_bytes.is_some_and(|batch_size| size_in_bytes >= &batch_size) {
                data_transfer
                    .transfer_data(table_name, column_index)
                    .await
                    .map_err(|err| IOError::new(Other, err.to_string()))?;
            }
        }

        Ok(data_transfer)
    }

    /// Insert the compressed file into the files to be transferred. Retrieve the size of the file
    /// and add it to the total size of the current local files in the table with `table_name`.
    pub async fn add_compressed_file(
        &self,
        table_name: &str,
        column_index: u16,
        compressed_file: &CompressedFile,
    ) -> Result<(), ParquetError> {
        let key = (table_name.to_owned(), column_index);

        // entry() is not used as it would require the allocation of a new String for each lookup as
        // it must be given as a K, while get_mut() accepts the key as a &K so one K can be used.
        if !self.compressed_files.contains_key(&key) {
            self.compressed_files.insert(key.clone(), 0);
        }
        *self.compressed_files.get_mut(&key).unwrap() += compressed_file.file_metadata.size;

        // If the combined size of the files is larger than the batch size, transfer the data to the
        // remote object store. unwrap() is safe as key has just been added to the map.
        if self.transfer_batch_size_in_bytes.is_some_and(|batch_size| {
            self.compressed_files.get(&key).unwrap().value() >= &batch_size
        }) {
            self.transfer_data(table_name, column_index).await?;
        }

        Ok(())
    }

    /// Set the transfer batch size to `new_value`. For each table that compressed data is saved
    /// for, check if the amount of data exceeds `new_value` and transfer all of the data if it does.
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
        for table_name_column_index_size_in_bytes in self.compressed_files.clone().iter() {
            let table_name = &table_name_column_index_size_in_bytes.key().0;
            let column_index = table_name_column_index_size_in_bytes.key().1;
            let size_in_bytes = table_name_column_index_size_in_bytes.value();

            if size_in_bytes > &threshold {
                self.transfer_data(table_name, column_index)
                    .await
                    .map_err(|err| IOError::new(Other, err.to_string()))?;
            }
        }

        Ok(())
    }

    /// Transfer the data stored locally for the column with `column_index` in table with
    /// `table_name` to the remote object store. Once successfully transferred, the data is deleted
    /// from local storage. Return [`Ok`] if the files were transferred successfully, otherwise
    /// [`ParquetError`].
    async fn transfer_data(&self, table_name: &str, column_index: u16) -> Result<(), ParquetError> {
        // Read all files that is currently stored for the table with table_name.
        let path = format!("{COMPRESSED_DATA_FOLDER}/{table_name}/{column_index}").into();
        let object_metas = self
            .local_data_folder
            .list(Some(&path))
            .filter_map(|maybe_meta| async { maybe_meta.ok() })
            .collect::<Vec<ObjectMeta>>()
            .await;

        debug!(
            "Transferring {} compressed files for the table '{}'.",
            object_metas.len(),
            table_name
        );

        // Merge the files and transfer them to the remote object store by setting the remote data
        // folder as the output data folder for the merged file.
        let compressed_file = CompressedDataManager::merge_compressed_apache_parquet_files(
            &(self.local_data_folder.clone() as Arc<dyn ObjectStore>),
            &object_metas,
            &self.remote_data_folder,
            &Path::from(format!("{COMPRESSED_DATA_FOLDER}/{table_name}/{column_index}")),
        )
        .await?;

        // Delete the metadata for the transferred files from the metadata database.
        self.table_metadata_manager
            .replace_compressed_files(table_name, column_index.into(), &object_metas, None)
            .await
            .map_err(|error| ParquetError::General(error.to_string()))?;

        // Remove the transferred files from the in-memory tracking of compressed files.
        let transferred_bytes: usize = object_metas.iter().map(|meta| meta.size).sum();
        *self
            .compressed_files
            .get_mut(&(table_name.to_owned(), column_index))
            .unwrap() -= transferred_bytes;

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_disk_space_metric
            .lock()
            .unwrap()
            .append(-(transferred_bytes as isize), true);

        debug!(
            "Transferred {} bytes of compressed data to path '{}' in remote object store.",
            transferred_bytes, path,
        );

        // Transfer the metadata of the transferred file to the manager.
        self.manager
            .transfer_compressed_file_metadata(table_name, column_index.into(), compressed_file)
            .await
            .map_err(|error| ParquetError::General(error.to_string()))?;

        Ok(())
    }

    /// Return the table name and column index if `path` is an Apache Parquet file with compressed
    /// data, otherwise [`None`].
    fn path_is_compressed_file(path: Path) -> Option<(String, u16)> {
        let path_parts: Vec<PathPart> = path.parts().collect();

        if Some(&PathPart::from(COMPRESSED_DATA_FOLDER)) == path_parts.first() {
            if let Some(table_name) = path_parts.get(1) {
                if let Some(column_index) = path_parts.get(2) {
                    if let Some(file_name) = path_parts.get(3) {
                        if file_name.as_ref().ends_with(".parquet") {
                            // unwrap() is safe as the column index folder is created by the system.
                            return Some((
                                table_name.as_ref().to_owned(),
                                column_index.as_ref().parse().unwrap(),
                            ));
                        }
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;

    use arrow_flight::flight_service_client::FlightServiceClient;
    use modelardb_common::test;
    use modelardb_common::{metadata, storage};
    use object_store::path::Path as ObjectStorePath;
    use ringbuf::Rb;
    use tempfile::{self, TempDir};
    use tokio::sync::RwLock;
    use tonic::transport::Channel;
    use uuid::Uuid;

    const COLUMN_INDEX: u16 = 5;
    const COMPRESSED_FILE_SIZE: usize = 2395;

    // Tests for path_is_compressed_file().
    #[test]
    fn test_empty_path_is_not_compressed_file() {
        let path = ObjectStorePath::from("");
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_folder_path_is_not_compressed_file() {
        let path = ObjectStorePath::from(format!(
            "{COMPRESSED_DATA_FOLDER}/{}/{COLUMN_INDEX}",
            test::MODEL_TABLE_NAME
        ));
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_table_folder_without_compressed_folder_is_not_compressed_file() {
        let path = ObjectStorePath::from(format!("test/{}/test.parquet", test::MODEL_TABLE_NAME));
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_non_apache_parquet_file_is_not_compressed_file() {
        let path = ObjectStorePath::from(format!(
            "{COMPRESSED_DATA_FOLDER}/{}/{COLUMN_INDEX}/test.txt",
            test::MODEL_TABLE_NAME
        ));
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_compressed_file_is_compressed_file() {
        let path = ObjectStorePath::from(format!(
            "{COMPRESSED_DATA_FOLDER}/{}/{COLUMN_INDEX}/test.parquet",
            test::MODEL_TABLE_NAME
        ));
        assert_eq!(
            DataTransfer::path_is_compressed_file(path),
            Some((test::MODEL_TABLE_NAME.to_owned(), COLUMN_INDEX))
        );
    }

    // Tests for data transfer component.
    #[tokio::test]
    async fn test_include_existing_files_on_start_up() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(temp_dir.path()).await;

        create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;
        create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;

        let (_target_dir, data_transfer) =
            create_data_transfer_component(metadata_manager, temp_dir.path()).await;

        assert_eq!(
            *data_transfer
                .compressed_files
                .get(&(test::MODEL_TABLE_NAME.to_owned(), COLUMN_INDEX))
                .unwrap(),
            COMPRESSED_FILE_SIZE * 2
        );

        assert_eq!(
            data_transfer
                .used_disk_space_metric
                .lock()
                .unwrap()
                .values()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_add_compressed_file_for_new_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(temp_dir.path()).await;

        let (_target_dir, data_transfer) =
            create_data_transfer_component(metadata_manager.clone(), temp_dir.path()).await;
        let compressed_file = create_compressed_file(metadata_manager, temp_dir.path()).await;

        assert!(data_transfer
            .add_compressed_file(test::MODEL_TABLE_NAME, COLUMN_INDEX, &compressed_file)
            .await
            .is_ok());

        assert_eq!(
            *data_transfer
                .compressed_files
                .get(&(test::MODEL_TABLE_NAME.to_owned(), COLUMN_INDEX))
                .unwrap(),
            COMPRESSED_FILE_SIZE
        );
    }

    #[tokio::test]
    async fn test_add_compressed_file_for_existing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(temp_dir.path()).await;

        let (_target_dir, data_transfer) =
            create_data_transfer_component(metadata_manager.clone(), temp_dir.path()).await;
        let compressed_file = create_compressed_file(metadata_manager, temp_dir.path()).await;

        data_transfer
            .add_compressed_file(test::MODEL_TABLE_NAME, COLUMN_INDEX, &compressed_file)
            .await
            .unwrap();
        data_transfer
            .add_compressed_file(test::MODEL_TABLE_NAME, COLUMN_INDEX, &compressed_file)
            .await
            .unwrap();

        assert_eq!(
            *data_transfer
                .compressed_files
                .get(&(test::MODEL_TABLE_NAME.to_owned(), COLUMN_INDEX))
                .unwrap(),
            COMPRESSED_FILE_SIZE * 2
        );
    }

    #[tokio::test]
    async fn test_increase_transfer_batch_size_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(temp_dir.path()).await;

        create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;
        create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;

        let (_, mut data_transfer) =
            create_data_transfer_component(metadata_manager, temp_dir.path()).await;

        data_transfer
            .set_transfer_batch_size_in_bytes(Some(COMPRESSED_FILE_SIZE * 10))
            .await
            .unwrap();

        assert_eq!(
            data_transfer.transfer_batch_size_in_bytes,
            Some(COMPRESSED_FILE_SIZE * 10)
        );

        // Data should not have been transferred.
        assert_eq!(
            *data_transfer
                .compressed_files
                .get(&(test::MODEL_TABLE_NAME.to_owned(), COLUMN_INDEX))
                .unwrap(),
            COMPRESSED_FILE_SIZE * 2
        );
    }

    #[tokio::test]
    async fn test_set_transfer_batch_size_in_bytes_to_none() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(temp_dir.path()).await;

        create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;
        create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;

        let (_, mut data_transfer) =
            create_data_transfer_component(metadata_manager, temp_dir.path()).await;

        assert_eq!(
            data_transfer.transfer_batch_size_in_bytes,
            Some(COMPRESSED_FILE_SIZE * 3 - 1)
        );

        data_transfer
            .set_transfer_batch_size_in_bytes(None)
            .await
            .unwrap();

        assert_eq!(data_transfer.transfer_batch_size_in_bytes, None);

        // Data should not have been transferred.
        assert_eq!(
            *data_transfer
                .compressed_files
                .get(&(test::MODEL_TABLE_NAME.to_owned(), COLUMN_INDEX))
                .unwrap(),
            COMPRESSED_FILE_SIZE * 2
        );
    }

    /// Set up a data folder with a table folder that has a single compressed file in it. Return the
    /// [`CompressedFile`] representing the created Apache Parquet file.
    async fn create_compressed_file(
        table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
        local_data_folder_path: &Path,
    ) -> CompressedFile {
        let object_store = LocalFileSystem::new_with_prefix(&local_data_folder_path).unwrap();

        let folder_path = format!(
            "{COMPRESSED_DATA_FOLDER}/{}/{COLUMN_INDEX}",
            test::MODEL_TABLE_NAME
        );

        let batch = test::compressed_segments_record_batch();

        let file_path = storage::write_compressed_segments_to_apache_parquet_file(
            &ObjectStorePath::from(folder_path),
            &batch,
            &object_store,
        )
        .await
        .unwrap();

        let object_meta = object_store.head(&file_path).await.unwrap();
        let compressed_file = CompressedFile::from_compressed_data(object_meta, &batch);

        // Save the metadata of the compressed file to the metadata database.
        table_metadata_manager
            .save_compressed_file(
                test::MODEL_TABLE_NAME,
                COLUMN_INDEX.into(),
                &compressed_file,
            )
            .await
            .unwrap();

        compressed_file
    }

    /// Create a data transfer component with a target object store that is deleted once the test is finished.
    async fn create_data_transfer_component(
        table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
        local_data_folder_path: &Path,
    ) -> (TempDir, DataTransfer) {
        let local_data_folder =
            Arc::new(LocalFileSystem::new_with_prefix(local_data_folder_path).unwrap());

        let target_dir = tempfile::tempdir().unwrap();

        // Create the target object store.
        let local_fs = LocalFileSystem::new_with_prefix(target_dir.path()).unwrap();
        let remote_data_folder_object_store = Arc::new(local_fs);

        // Create a manager interface.
        let channel = Channel::builder("grpc://server:9999".parse().unwrap()).connect_lazy();
        let lazy_flight_client = FlightServiceClient::new(channel);

        let manager = Manager::new(
            Arc::new(RwLock::new(lazy_flight_client)),
            Uuid::new_v4().to_string(),
            None,
        );

        let data_transfer = DataTransfer::try_new(
            local_data_folder,
            remote_data_folder_object_store,
            table_metadata_manager,
            manager,
            Some(COMPRESSED_FILE_SIZE * 3 - 1),
            Arc::new(Mutex::new(Metric::new())),
        )
        .await
        .unwrap();

        (target_dir, data_transfer)
    }

    /// Create a table metadata manager and save a single model table to the metadata database.
    async fn create_metadata_manager(
        local_data_folder_path: &Path,
    ) -> Arc<TableMetadataManager<Sqlite>> {
        let local_data_folder =
            Arc::new(LocalFileSystem::new_with_prefix(local_data_folder_path).unwrap());

        let metadata_manager = metadata::try_new_sqlite_table_metadata_manager(local_data_folder)
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
