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
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use datafusion::parquet::errors::ParquetError;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::{Path as ObjectStorePath, PathPart};
use object_store::{ObjectMeta, ObjectStore};
use tracing::debug;
use uuid::Uuid;

use crate::metadata::compressed_file::CompressedFile;
use crate::metadata::MetadataManager;
use crate::storage::compressed_data_manager::CompressedDataManager;
use crate::storage::Metric;
use crate::storage::COMPRESSED_DATA_FOLDER;

// TODO: Make the transfer batch size in bytes part of the user-configurable settings.
// TODO: When the storage engine is changed to use object store for everything, receive
//       the object store directly through the parameters instead.
// TODO: Handle the case where a connection can not be established when transferring data.
// TODO: Handle deleting the files after the transfer is complete in a safe way to avoid
//       transferring the same data multiple times or deleting files that are currently used
//       elsewhere.

pub struct DataTransfer {
    /// The object store containing all compressed data managed by the
    /// [`StorageEngine`](crate::storage::StorageEngine).
    local_data_folder: Arc<dyn ObjectStore>,
    /// The object store that the data should be transferred to.
    pub remote_data_folder: Arc<dyn ObjectStore>,
    /// Management of metadata for deleting file metadata after transferring.
    metadata_manager: Arc<MetadataManager>,
    /// Map from table names and column indices to the combined size in bytes of the compressed
    /// files currently saved for the column in that table.
    compressed_files: DashMap<(String, u16), usize>,
    /// The number of bytes that are required before transferring a batch of data to the remote
    /// object store.
    transfer_batch_size_in_bytes: usize,
    /// Metric for the total used disk space in bytes, updated when data is transferred.
    pub used_disk_space_metric: Arc<Mutex<Metric>>,
}

impl DataTransfer {
    /// Create a new data transfer instance and initialize it with the compressed files already
    /// existing in `local_data_folder_path`. If `local_data_folder_path` or a path within
    /// `local_data_folder_path` could not be read, return [`IOError`].
    pub async fn try_new(
        local_data_folder: PathBuf,
        remote_data_folder: Arc<dyn ObjectStore>,
        metadata_manager: Arc<MetadataManager>,
        transfer_batch_size_in_bytes: usize,
        used_disk_space_metric: Arc<Mutex<Metric>>,
    ) -> Result<Self, IOError> {
        // Parse through the data folder to retrieve already existing files that should be transferred.
        let local_data_folder = Arc::new(LocalFileSystem::new_with_prefix(local_data_folder)?);
        let list_stream = local_data_folder.list(None).await?;

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
            metadata_manager,
            compressed_files: compressed_files.clone(),
            transfer_batch_size_in_bytes,
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

            if size_in_bytes >= &transfer_batch_size_in_bytes {
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
        *self.compressed_files.get_mut(&key).unwrap() += compressed_file.size;

        // If the combined size of the files is larger than the batch size, transfer the data to the
        // remote object store. unwrap() is safe as key has just been added to the map.
        if self.compressed_files.get(&key).unwrap().value() >= &self.transfer_batch_size_in_bytes {
            self.transfer_data(table_name, column_index).await?;
        }

        Ok(())
    }

    /// Transfer all compressed files currently in the data folder to the remote object store.
    /// Return [`Ok`] if all files were transferred successfully, otherwise [`ParquetError`]. Note
    /// that if the function fails, some of the compressed files may still have been transferred.
    /// Since the data is transferred separately for each table, the function can be called again if
    /// it failed.
    pub(crate) async fn flush(&self) -> Result<(), ParquetError> {
        // The clone is performed to not create a deadlock with transfer_data().
        for table_name_column_index_size_in_bytes in self.compressed_files.clone().iter() {
            let table_name = &table_name_column_index_size_in_bytes.key().0;
            let column_index = table_name_column_index_size_in_bytes.key().1;
            let size_in_bytes = table_name_column_index_size_in_bytes.value();

            if size_in_bytes > &0_usize {
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
            .await
            .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?
            .filter_map(|maybe_meta| async { maybe_meta.ok() })
            .collect::<Vec<ObjectMeta>>()
            .await;

        debug!(
            "Transferring {} compressed files for the table '{}'.",
            object_metas.len(),
            table_name
        );

        // Merge the files and transfer them to the remote object store.
        CompressedDataManager::merge_compressed_apache_parquet_files(
            &self.local_data_folder,
            &object_metas,
            &self.remote_data_folder,
            &format!("{COMPRESSED_DATA_FOLDER}/{table_name}/{column_index}"),
        )
        .await?;

        // Delete the metadata for the transferred files from the metadata database.
        let compressed_files_to_delete: Vec<Uuid> =
            CompressedFile::object_metas_to_compressed_file_names(&object_metas)
                .map_err(|error| ParquetError::General(error.to_string()))?;

        self.metadata_manager
            .replace_compressed_files(
                table_name,
                column_index.into(),
                &compressed_files_to_delete,
                None,
            )
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

        Ok(())
    }

    /// Return the table name and column index if `path` is an Apache Parquet file with compressed
    /// data, otherwise [`None`].
    fn path_is_compressed_file(path: ObjectStorePath) -> Option<(String, u16)> {
        let path_parts: Vec<PathPart> = path.parts().collect();

        if Some(&PathPart::from(COMPRESSED_DATA_FOLDER)) == path_parts.get(0) {
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

    use std::fs;
    use std::path::Path;

    use ringbuf::Rb;
    use tempfile::{self, TempDir};

    use crate::common_test;
    use crate::storage::StorageEngine;

    const TABLE_NAME: &str = "model_table";
    const COLUMN_INDEX: u16 = 5;
    const COMPRESSED_FILE_SIZE: usize = 2429;

    // Tests for path_is_compressed_file().
    #[test]
    fn test_empty_path_is_not_compressed_file() {
        let path = ObjectStorePath::from("");
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_folder_path_is_not_compressed_file() {
        let path = ObjectStorePath::from(format!(
            "{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}/{COLUMN_INDEX}"
        ));
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_table_folder_without_compressed_folder_is_not_compressed_file() {
        let path = ObjectStorePath::from(format!("test/{TABLE_NAME}/test.parquet"));
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_non_apache_parquet_file_is_not_compressed_file() {
        let path = ObjectStorePath::from(format!(
            "{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}/{COLUMN_INDEX}/test.txt"
        ));
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_compressed_file_is_compressed_file() {
        let path = ObjectStorePath::from(format!(
            "{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}/{COLUMN_INDEX}/test.parquet"
        ));
        assert_eq!(
            DataTransfer::path_is_compressed_file(path),
            Some((TABLE_NAME.to_owned(), COLUMN_INDEX))
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
                .get(&(TABLE_NAME.to_owned(), COLUMN_INDEX))
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
        let (compressed_file, _) = create_compressed_file(metadata_manager, temp_dir.path()).await;

        assert!(data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, &compressed_file)
            .await
            .is_ok());

        assert_eq!(
            data_transfer
                .compressed_files
                .get(&(TABLE_NAME.to_owned(), COLUMN_INDEX))
                .unwrap()
                .value(),
            &COMPRESSED_FILE_SIZE
        );
    }

    #[tokio::test]
    async fn test_add_compressed_file_for_existing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(temp_dir.path()).await;

        let (_target_dir, data_transfer) =
            create_data_transfer_component(metadata_manager.clone(), temp_dir.path()).await;
        let (compressed_file, _) = create_compressed_file(metadata_manager, temp_dir.path()).await;

        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, &compressed_file)
            .await
            .unwrap();
        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, &compressed_file)
            .await
            .unwrap();

        assert_eq!(
            data_transfer
                .compressed_files
                .get(&(TABLE_NAME.to_owned(), COLUMN_INDEX))
                .unwrap()
                .value(),
            &(COMPRESSED_FILE_SIZE * 2)
        );
    }

    #[tokio::test]
    async fn test_transfer_single_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(temp_dir.path()).await;

        let (target_dir, data_transfer) =
            create_data_transfer_component(metadata_manager.clone(), temp_dir.path()).await;
        let (compressed_file, apache_parquet_path) =
            create_compressed_file(metadata_manager, temp_dir.path()).await;

        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, &compressed_file)
            .await
            .unwrap();
        data_transfer
            .transfer_data(TABLE_NAME, COLUMN_INDEX)
            .await
            .unwrap();

        assert_data_transferred(vec![apache_parquet_path], target_dir, data_transfer, 3).await;
    }

    #[tokio::test]
    async fn test_transfer_multiple_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(temp_dir.path()).await;

        let (target_dir, data_transfer) =
            create_data_transfer_component(metadata_manager.clone(), temp_dir.path()).await;
        let (compressed_file_1, path_1) =
            create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;
        let (compressed_file_2, path_2) =
            create_compressed_file(metadata_manager, temp_dir.path()).await;

        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, &compressed_file_1)
            .await
            .unwrap();
        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, &compressed_file_2)
            .await
            .unwrap();
        data_transfer
            .transfer_data(TABLE_NAME, COLUMN_INDEX)
            .await
            .unwrap();

        assert_data_transferred(vec![path_1, path_2], target_dir, data_transfer, 6).await;
    }

    #[tokio::test]
    async fn test_transfer_if_reaching_batch_size_when_adding() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(temp_dir.path()).await;

        let (target_dir, mut data_transfer) =
            create_data_transfer_component(metadata_manager.clone(), temp_dir.path()).await;
        let (compressed_file, apache_parquet_path) =
            create_compressed_file(metadata_manager, temp_dir.path()).await;

        // Set the max batch size to ensure that the file is transferred immediately.
        data_transfer.transfer_batch_size_in_bytes = COMPRESSED_FILE_SIZE - 1;
        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, &compressed_file)
            .await
            .unwrap();

        assert_data_transferred(vec![apache_parquet_path], target_dir, data_transfer, 3).await;
    }

    #[tokio::test]
    async fn test_transfer_if_reaching_batch_size_on_start_up() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(temp_dir.path()).await;

        let (_, path_1) = create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;
        let (_, path_2) = create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;
        let (_, path_3) = create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;

        // Since the max batch size is 1 byte smaller than 3 compressed files, the data should be transferred immediately.
        let (target_dir, data_transfer) =
            create_data_transfer_component(metadata_manager, temp_dir.path()).await;

        assert_data_transferred(vec![path_1, path_2, path_3], target_dir, data_transfer, 9).await;
    }

    #[tokio::test]
    async fn test_flush_compressed_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = create_metadata_manager(temp_dir.path()).await;

        let (_, path_1) = create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;
        let (_, path_2) = create_compressed_file(metadata_manager.clone(), temp_dir.path()).await;

        let (target_dir, data_transfer) =
            create_data_transfer_component(metadata_manager, temp_dir.path()).await;

        data_transfer.flush().await.unwrap();

        assert_data_transferred(vec![path_1, path_2], target_dir, data_transfer, 6).await;
    }

    /// Assert that the files in `paths` are all removed, a file has been created in `target_dir`,
    /// the file metadata has been removed from the metadata database, and that the hashmap
    /// containing the compressed files size is set to 0.
    async fn assert_data_transferred(
        paths: Vec<PathBuf>,
        target: TempDir,
        data_transfer: DataTransfer,
        expected_num_rows: usize,
    ) {
        for path in &paths {
            assert!(!path.exists());
        }

        // The transferred file should be in a sub-folder under the table name and column index.
        let target_folder = target.path().join(format!(
            "{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}/{COLUMN_INDEX}",
        ));

        assert!(target_folder.exists());
        assert_eq!(target_folder.read_dir().unwrap().count(), 1);

        let target_path = target_folder.read_dir().unwrap().last().unwrap().unwrap();

        // The file should have three rows since the rows in the compressed files are merged.
        let batch = StorageEngine::read_batch_from_apache_parquet_file(&target_path.path())
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), expected_num_rows);

        // The metadata for the files should be deleted from the metadata database.
        let compressed_files = data_transfer
            .metadata_manager
            .compressed_files(TABLE_NAME, COLUMN_INDEX.into(), None, None, None, None)
            .await
            .unwrap();

        assert_eq!(compressed_files.len(), 0);

        assert_eq!(
            *data_transfer
                .compressed_files
                .get(&(TABLE_NAME.to_owned(), COLUMN_INDEX))
                .unwrap(),
            0_usize
        );

        // The used disk space log should have an entry for when the data transfer component is
        // created and for when the transferred files are deleted from disk.
        assert_eq!(
            data_transfer
                .used_disk_space_metric
                .lock()
                .unwrap()
                .values()
                .len(),
            2
        );
    }

    /// Set up a data folder with a table folder that has a single compressed file in it. Return the
    /// [`CompressedFile`] representing the created Apache Parquet file and the path to the file.
    async fn create_compressed_file(
        metadata_manager: Arc<MetadataManager>,
        local_data_folder_path: &Path,
    ) -> (CompressedFile, PathBuf) {
        let folder_path = format!("{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}/{COLUMN_INDEX}");
        let path = local_data_folder_path.join(folder_path.clone());
        fs::create_dir_all(path.clone()).unwrap();

        let uuid = Uuid::new_v4();
        let batch = common_test::compressed_segments_record_batch();
        let apache_parquet_path = path.join(format!("{uuid}.parquet"));
        StorageEngine::write_batch_to_apache_parquet_file(
            &batch,
            apache_parquet_path.as_path(),
            None,
        )
        .unwrap();

        let compressed_file = CompressedFile::from_record_batch(
            uuid,
            folder_path.into(),
            COMPRESSED_FILE_SIZE,
            &batch,
        );

        // Save the metadata of the compressed file to the metadata database.
        metadata_manager
            .save_compressed_file(TABLE_NAME, COLUMN_INDEX.into(), &compressed_file)
            .await
            .unwrap();

        (compressed_file, apache_parquet_path)
    }

    /// Create a data transfer component with a target object store that is deleted once the test is finished.
    async fn create_data_transfer_component(
        metadata_manager: Arc<MetadataManager>,
        local_data_folder_path: &Path,
    ) -> (TempDir, DataTransfer) {
        let target_dir = tempfile::tempdir().unwrap();

        // Create the target object store.
        let local_fs = LocalFileSystem::new_with_prefix(target_dir.path()).unwrap();
        let remote_data_folder_object_store = Arc::new(local_fs);

        let data_transfer = DataTransfer::try_new(
            local_data_folder_path.to_path_buf(),
            remote_data_folder_object_store,
            metadata_manager,
            COMPRESSED_FILE_SIZE * 3 - 1,
            Arc::new(Mutex::new(Metric::new())),
        )
        .await
        .unwrap();

        (target_dir, data_transfer)
    }

    /// Create a metadata manager and save a single model table to the metadata database.
    async fn create_metadata_manager(local_data_folder_path: &Path) -> Arc<MetadataManager> {
        let metadata_manager = Arc::new(
            MetadataManager::try_new(local_data_folder_path)
                .await
                .unwrap(),
        );

        let model_table_metadata = common_test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, common_test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        metadata_manager
    }
}
