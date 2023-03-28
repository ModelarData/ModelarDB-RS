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

use std::collections::HashMap;
use std::io::Error as IOError;
use std::io::ErrorKind::Other;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::parquet::errors::ParquetError;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::{Path as ObjectStorePath, PathPart};
use object_store::{ObjectMeta, ObjectStore};
use tokio::sync::RwLock;
use tracing::debug;

use crate::storage::Metric;
use crate::storage::{StorageEngine, COMPRESSED_DATA_FOLDER};

// TODO: Make the transfer batch size in bytes part of the user-configurable settings.
// TODO: When the storage engine is changed to use object store for everything, receive
//       the object store directly through the parameters instead.
// TODO: Handle the case where a connection can not be established when transferring data.
// TODO: Handle deleting the files after the transfer is complete in a safe way to avoid
//       transferring the same data multiple times or deleting files that are currently used
//       elsewhere.

pub struct DataTransfer {
    /// The object store containing all compressed data managed by the [`StorageEngine`].
    local_data_folder: Arc<dyn ObjectStore>,
    /// The object store that the data should be transferred to.
    pub remote_data_folder: Arc<dyn ObjectStore>,
    /// Map from table names and column indices to the combined size in bytes of the compressed
    /// files currently saved for the column in that table.
    compressed_files: HashMap<(String, u16), usize>,
    /// The number of bytes that are required before transferring a batch of data to the remote
    /// object store.
    transfer_batch_size_in_bytes: usize,
    /// Metric for the total used disk space in bytes, updated when data is transferred.
    pub used_disk_space_metric: Arc<RwLock<Metric>>,
}

impl DataTransfer {
    /// Create a new data transfer instance and initialize it with the compressed files already
    /// existing in `local_data_folder_path`. If `local_data_folder_path` or a path within
    /// `local_data_folder_path` could not be read, return [`IOError`].
    pub async fn try_new(
        local_data_folder: PathBuf,
        remote_data_folder: Arc<dyn ObjectStore>,
        transfer_batch_size_in_bytes: usize,
        used_disk_space_metric: Arc<RwLock<Metric>>,
    ) -> Result<Self, IOError> {
        // Parse through the data folder to retrieve already existing files that should be transferred.
        let local_data_folder = Arc::new(LocalFileSystem::new_with_prefix(local_data_folder)?);
        let list_stream = local_data_folder.list(None).await?;

        let compressed_files = list_stream
            .fold(HashMap::new(), |mut acc, maybe_meta| async {
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

        let mut data_transfer = Self {
            local_data_folder,
            remote_data_folder,
            compressed_files: compressed_files.clone(),
            transfer_batch_size_in_bytes,
            used_disk_space_metric,
        };

        // Record the initial used disk space.
        let initial_disk_space: usize = compressed_files.values().sum();
        data_transfer
            .used_disk_space_metric
            .write()
            .await
            .append(initial_disk_space as isize, true);

        // Check if data should be transferred immediately.
        for ((table_name, column_index), size_in_bytes) in compressed_files.iter() {
            if size_in_bytes >= &transfer_batch_size_in_bytes {
                data_transfer
                    .transfer_data(table_name, *column_index)
                    .await
                    .map_err(|err| IOError::new(Other, err.to_string()))?;
            }
        }

        Ok(data_transfer)
    }

    /// Insert the compressed file into the files to be transferred. Retrieve the size of the file
    /// and add it to the total size of the current local files in the table with `table_name`.
    pub async fn add_compressed_file(
        &mut self,
        table_name: &str,
        column_index: u16,
        file_path: &Path,
    ) -> Result<(), ParquetError> {
        let key = (table_name.to_owned(), column_index);
        let file_size = file_path.metadata()?.len() as usize;

        // entry() is not used as it would require the allocation of a new String for each lookup as
        // it must be given as a K, while get_mut() accepts the key as a &K so one K can be used.
        if !self.compressed_files.contains_key(&key) {
            self.compressed_files.insert(key.clone(), 0);
        }
        *self.compressed_files.get_mut(&key).unwrap() += file_size;

        // If the combined size of the files is larger than the batch size, transfer the data to the
        // remote object store.
        if self.compressed_files.get(&key).unwrap() >= &self.transfer_batch_size_in_bytes {
            self.transfer_data(table_name, column_index).await?;
        }

        Ok(())
    }

    /// Transfer all compressed files currently in the data folder to the remote object store.
    /// Return [`Ok`] if all files were transferred successfully, otherwise [`ParquetError`]. Note
    /// that if the function fails, some of the compressed files may still have been transferred.
    /// Since the data is transferred separately for each table, the function can be called again if
    /// it failed.
    pub(crate) async fn flush(&mut self) -> Result<(), ParquetError> {
        for ((table_name, column_index), size_in_bytes) in self.compressed_files.clone().iter() {
            if size_in_bytes > &0_usize {
                self.transfer_data(table_name, *column_index)
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
    async fn transfer_data(
        &mut self,
        table_name: &str,
        column_index: u16,
    ) -> Result<(), ParquetError> {
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
        StorageEngine::merge_compressed_apache_parquet_files(
            &self.local_data_folder,
            &object_metas,
            &self.remote_data_folder,
            &format!("{COMPRESSED_DATA_FOLDER}/{table_name}/{column_index}"),
        )
        .await?;

        // Remove the transferred files from the in-memory tracking of compressed files.
        let transferred_bytes: usize = object_metas.iter().map(|meta| meta.size).sum();
        *self
            .compressed_files
            .get_mut(&(table_name.to_owned(), column_index))
            .unwrap() -= transferred_bytes;
        self.used_disk_space_metric
            .write()
            .await
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

    use ringbuf::Rb;
    use tempfile::{self, TempDir};

    use crate::PORT;
    use crate::storage::TEST_UUID;
    use crate::storage::test_util;

    const TABLE_NAME: &str = "table";
    const COLUMN_INDEX: u16 = 5;
    const COMPRESSED_FILE_SIZE: usize = 2126;

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
        create_compressed_file(temp_dir.path(), "test_1");
        create_compressed_file(temp_dir.path(), "test_2");
        let (_target_dir, data_transfer) = create_data_transfer_component(temp_dir.path()).await;

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
                .read()
                .await
                .values
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_add_compressed_file_for_new_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_target_dir, mut data_transfer) =
            create_data_transfer_component(temp_dir.path()).await;
        let apache_parquet_path = create_compressed_file(temp_dir.path(), "test");

        assert!(data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, apache_parquet_path.as_path())
            .await
            .is_ok());

        assert_eq!(
            data_transfer
                .compressed_files
                .get(&(TABLE_NAME.to_owned(), COLUMN_INDEX))
                .unwrap(),
            &COMPRESSED_FILE_SIZE
        );
    }

    #[tokio::test]
    async fn test_add_compressed_file_for_existing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_target_dir, mut data_transfer) =
            create_data_transfer_component(temp_dir.path()).await;
        let apache_parquet_path = create_compressed_file(temp_dir.path(), "test");

        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, apache_parquet_path.as_path())
            .await
            .unwrap();
        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, apache_parquet_path.as_path())
            .await
            .unwrap();

        assert_eq!(
            data_transfer
                .compressed_files
                .get(&(TABLE_NAME.to_owned(), COLUMN_INDEX))
                .unwrap(),
            &(COMPRESSED_FILE_SIZE * 2)
        );
    }

    #[tokio::test]
    async fn test_add_non_existent_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir
            .path()
            .join(format!("{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}"));
        fs::create_dir_all(path.clone()).unwrap();

        let (_target_dir, mut data_transfer) =
            create_data_transfer_component(temp_dir.path()).await;

        let apache_parquet_path = path.join("test_apache_parquet.parquet");
        assert!(data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, apache_parquet_path.as_path())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_transfer_single_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path()).await;
        let apache_parquet_path = create_compressed_file(temp_dir.path(), "test");

        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, apache_parquet_path.as_path())
            .await
            .unwrap();
        data_transfer
            .transfer_data(TABLE_NAME, COLUMN_INDEX)
            .await
            .unwrap();

        assert_data_transferred(vec![apache_parquet_path], target_dir, data_transfer).await;
    }

    #[tokio::test]
    async fn test_transfer_multiple_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path()).await;
        let path_1 = create_compressed_file(temp_dir.path(), "test_1");
        let path_2 = create_compressed_file(temp_dir.path(), "test_2");

        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, path_1.as_path())
            .await
            .unwrap();
        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, path_2.as_path())
            .await
            .unwrap();
        data_transfer
            .transfer_data(TABLE_NAME, COLUMN_INDEX)
            .await
            .unwrap();

        assert_data_transferred(vec![path_1, path_2], target_dir, data_transfer).await;
    }

    #[tokio::test]
    async fn test_transfer_if_reaching_batch_size_when_adding() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path()).await;
        let apache_parquet_path = create_compressed_file(temp_dir.path(), "test");

        // Set the max batch size to ensure that the file is transferred immediately.
        data_transfer.transfer_batch_size_in_bytes = COMPRESSED_FILE_SIZE - 1;
        data_transfer
            .add_compressed_file(TABLE_NAME, COLUMN_INDEX, apache_parquet_path.as_path())
            .await
            .unwrap();

        assert_data_transferred(vec![apache_parquet_path], target_dir, data_transfer).await;
    }

    #[tokio::test]
    async fn test_transfer_if_reaching_batch_size_on_start_up() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path_1 = create_compressed_file(temp_dir.path(), "test_1");
        let path_2 = create_compressed_file(temp_dir.path(), "test_2");
        let path_3 = create_compressed_file(temp_dir.path(), "test_3");

        // Since the max batch size is 1 byte smaller than 3 compressed files, the data should be transferred immediately.
        let (target_dir, data_transfer) = create_data_transfer_component(temp_dir.path()).await;

        assert_data_transferred(vec![path_1, path_2, path_3], target_dir, data_transfer).await;
    }

    #[tokio::test]
    async fn test_flush_compressed_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path_1 = create_compressed_file(temp_dir.path(), "test_1");
        let path_2 = create_compressed_file(temp_dir.path(), "test_2");
        let (target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path()).await;

        data_transfer.flush().await.unwrap();

        assert_data_transferred(vec![path_1, path_2], target_dir, data_transfer).await;
    }

    /// Assert that the files in `paths` are all removed, a file has been created in `target_dir`,
    /// and that the hashmap containing the compressed files size is set to 0.
    async fn assert_data_transferred(
        paths: Vec<PathBuf>,
        target: TempDir,
        data_transfer: DataTransfer,
    ) {
        for path in &paths {
            assert!(!path.exists());
        }

        // The transferred file should have a time range file name that matches the compressed data.
        let target_path = target.path().join(format!(
            "{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}/{COLUMN_INDEX}/0_5_5.2_34.2_{TEST_UUID}_{PORT}.parquet",
        ));
        assert!(target_path.exists());

        // The file should have three rows since the rows in the compressed files are merged.
        let batch = StorageEngine::read_batch_from_apache_parquet_file(target_path.as_path())
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 3);

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
                .read()
                .await
                .values
                .len(),
            2
        );
    }

    /// Set up a data folder with a table folder that has a single compressed file in it. Return the
    /// path to the created Apache Parquet file.
    fn create_compressed_file(local_data_folder_path: &Path, file_name: &str) -> PathBuf {
        let path = local_data_folder_path.join(format!(
            "{COMPRESSED_DATA_FOLDER}/{TABLE_NAME}/{COLUMN_INDEX}"
        ));
        fs::create_dir_all(path.clone()).unwrap();

        let batch = test_util::compressed_segments_record_batch();
        let apache_parquet_path = path.join(format!("{file_name}.parquet"));
        StorageEngine::write_batch_to_apache_parquet_file(
            batch,
            apache_parquet_path.as_path(),
            None,
        )
        .unwrap();

        apache_parquet_path
    }

    /// Create a data transfer component with a target object store that is deleted once the test is finished.
    async fn create_data_transfer_component(
        local_data_folder_path: &Path,
    ) -> (TempDir, DataTransfer) {
        let target_dir = tempfile::tempdir().unwrap();

        // Create the target object store.
        let local_fs = LocalFileSystem::new_with_prefix(target_dir.path()).unwrap();
        let remote_data_folder_object_store = Arc::new(local_fs);

        let data_transfer = DataTransfer::try_new(
            local_data_folder_path.to_path_buf(),
            remote_data_folder_object_store,
            COMPRESSED_FILE_SIZE * 3 - 1,
            Arc::new(RwLock::new(Metric::new())),
        )
        .await
        .unwrap();

        (target_dir, data_transfer)
    }
}
