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

use bytes::BufMut;
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::{Path as ObjectStorePath, PathPart};
use object_store::ObjectStore;
use tonic::codegen::Bytes;
use tracing::debug;

use crate::{storage, StorageEngine};

// TODO: When the storage engine is changed to use object store for everything, receive
//       the object store directly through the parameters instead.
// TODO: Handle the case where a connection can not be established when transferring data.
// TODO: When compressed data is saved, add the compressed file to the data transfer component.
// TODO: Handle deleting the files after the transfer is complete in a safe way to avoid transferring
//       the same data multiple times or deleting files that are currently used elsewhere.

// TODO: Create a new action (add to list actions) that flushes the memory and the on-disk files.
// TODO: Run Rustfmt.

pub struct DataTransfer {
    /// Path to the folder containing all compressed data managed by the [`StorageEngine`].
    local_data_folder_path: PathBuf,
    /// The object store containing all compressed data managed by the [`StorageEngine`].
    local_data_folder_object_store: Arc<dyn ObjectStore>,
    /// The object store that the data should be transferred to.
    remote_data_folder_object_store: Arc<dyn ObjectStore>,
    /// Map from keys to the combined size in bytes of the compressed files saved under the key.
    compressed_files: HashMap<u64, usize>,
    /// The number of bytes that is required before transferring a batch of data to the remote object store.
    transfer_batch_size_in_bytes: usize,
}

impl DataTransfer {
    /// Create a new data transfer instance and initialize it with the compressed files already
    /// existing in `local_data_folder_path`. If `local_data_folder_path` or a path within
    /// `local_data_folder_path` could not be read, return [`IOError`].
    pub async fn try_new(
        local_data_folder_path: PathBuf,
        remote_data_folder_object_store: Arc<dyn ObjectStore>,
        transfer_batch_size_in_bytes: usize,
    ) -> Result<Self, IOError> {
        let local_fs = LocalFileSystem::new_with_prefix(local_data_folder_path.clone())?;
        let local_data_folder_object_store = Arc::new(local_fs);

        // Parse through the data folder to retrieve already existing files that should be transferred.
        let list_stream = local_data_folder_object_store.list(None).await?;

        let compressed_files = list_stream
            .fold(HashMap::new(), |mut acc, maybe_meta| async {
                if let Ok(meta) = maybe_meta {
                    // If the file is a compressed file, add the size of it to the total size of the files under the key.
                    if let Some(key) = Self::path_is_compressed_file(meta.location) {
                        *acc.entry(key).or_insert(0) += meta.size;
                    }
                }

                acc
            })
            .await;

        let mut data_transfer = Self {
            local_data_folder_path,
            local_data_folder_object_store,
            remote_data_folder_object_store,
            compressed_files: compressed_files.clone(),
            transfer_batch_size_in_bytes,
        };

        // Check if data should be transferred immediately.
        for (key, size_in_bytes) in compressed_files.iter() {
            if size_in_bytes >= &transfer_batch_size_in_bytes {
                data_transfer
                    .transfer_data(key)
                    .await
                    .map_err(|err| IOError::new(Other, err.to_string()))?;
            }
        }

        Ok(data_transfer)
    }

    /// Insert the compressed file into the files to be transferred. Retrieve the size of the file
    /// and add it to the total size of the current local files under the key.
    pub async fn add_compressed_file(
        &mut self,
        key: &u64,
        file_path: &Path,
    ) -> Result<(), ParquetError> {
        let file_size = file_path.metadata()?.len() as usize;
        *self.compressed_files.entry(*key).or_insert(0) += file_size;

        // If the combined size of the files is larger than the batch size, transfer the data to the remote object store.
        if self.compressed_files.get(key).unwrap() >= &self.transfer_batch_size_in_bytes {
            self.transfer_data(key).await?;
        }

        Ok(())
    }

    /// Transfer all compressed files currently in the data folder to the remote object store.
    /// Return [`Ok`] if all files were transferred successfully, otherwise [`ParquetError`].
    pub(crate) async fn flush_compressed_files(&mut self) -> Result<(), ParquetError> {
        for (key, size_in_bytes) in self.compressed_files.clone().iter() {
            if size_in_bytes > &(0 as usize) {
                self.transfer_data(key)
                    .await
                    .map_err(|err| IOError::new(Other, err.to_string()))?;
            }
        }

        Ok(())
    }

    /// Transfer the data corresponding to `key` to the remote object store. Once successfully
    /// transferred, the data is deleted from local storage. Return [`Ok`] if the files were
    /// transferred successfully, otherwise [`ParquetError`].
    async fn transfer_data(&mut self, key: &u64) -> Result<(), ParquetError> {
        // Read all files that correspond to the key.
        let path = format!("{}/compressed", key).into();
        let list_stream = self
            .local_data_folder_object_store
            .list(Some(&path))
            .await
            .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

        let object_metas = list_stream
            .filter_map(|maybe_meta| async { maybe_meta.ok() })
            .collect::<Vec<_>>()
            .await
            .into_iter();

        debug!("Transferring {} compressed files from key '{}'.", object_metas.len(), key);

        // Combine the Apache Parquet files into a single RecordBatch.
        let record_batches = object_metas
            .clone()
            .filter_map(|meta| {
                let path = self.local_data_folder_path.to_string_lossy();
                let file_path = PathBuf::from(format!("{}/{}", path, meta.location));

                StorageEngine::read_entire_apache_parquet_file(file_path.as_path()).ok()
            })
            .collect::<Vec<RecordBatch>>();

        let schema = record_batches[0].schema();
        let combined = compute::concat_batches(&schema, &record_batches)?;

        debug!(
            "Combined compressed files into single record batch with {} rows.",
            combined.num_rows()
        );

        // Write the combined RecordBatch to a bytes buffer.
        let mut buf = vec![].writer();
        let mut arrow_writer = storage::create_apache_arrow_writer(&mut buf, schema)?;
        arrow_writer.write(&combined)?;
        arrow_writer.close()?;

        // Transfer the combined RecordBatch to the remote object store.
        let file_name = storage::create_time_range_file_name(&combined);
        let path = format!("{}/compressed/{}", key, file_name).into();
        self.remote_data_folder_object_store
            .put(&path, Bytes::from(buf.into_inner()))
            .await
            .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

        // Delete the transferred files from local storage.
        for meta in object_metas.clone() {
            self.local_data_folder_object_store
                .delete(&meta.location)
                .await
                .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;
        }

        // Delete the transferred files from the in-memory tracking of compressed files.
        let transferred_bytes: usize = object_metas.map(|meta| meta.size).sum();
        *self.compressed_files.get_mut(key).unwrap() -= transferred_bytes;

        debug!(
            "Transferred {} bytes of compressed data to path '{}' in remote object store.",
            transferred_bytes, path,
        );

        Ok(())
    }

    /// Return the key if `path` is an Apache Parquet file with compressed data, otherwise [`None`].
    fn path_is_compressed_file(path: ObjectStorePath) -> Option<u64> {
        let path_parts: Vec<PathPart> = path.parts().collect();

        if let Some(key_part) = path_parts.get(0) {
            if let Ok(key) = key_part.as_ref().parse::<u64>() {
                if let Some(file_name_part) = path_parts.get(2) {
                    if Some(&PathPart::from("compressed")) == path_parts.get(1)
                        && file_name_part.as_ref().ends_with(".parquet")
                    {
                        return Some(key);
                    }
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectStorePath;
    use tempfile::TempDir;

    use crate::storage::data_transfer::DataTransfer;
    use crate::storage::test_util;
    use crate::StorageEngine;

    const KEY: u64 = 1668574317311628292;
    const COMPRESSED_FILE_SIZE: usize = 2576;

    // Tests for path_is_compressed_file().
    #[test]
    fn test_empty_path_is_not_compressed_file() {
        let path = ObjectStorePath::from("");
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_folder_path_is_not_compressed_file() {
        let path = ObjectStorePath::from("4330327753845164038/compressed");
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_non_key_folder_is_not_compressed_file() {
        let path = ObjectStorePath::from("test/compressed/test.parquet");
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_key_folder_without_compressed_folder_is_not_compressed_file() {
        let path = ObjectStorePath::from("4330327753845164038/test/test.parquet");
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_non_parquet_file_is_not_compressed_file() {
        let path = ObjectStorePath::from("4330327753845164038/compressed/test.txt");
        assert!(DataTransfer::path_is_compressed_file(path).is_none());
    }

    #[test]
    fn test_compressed_file_is_compressed_file() {
        let path = ObjectStorePath::from("4330327753845164038/compressed/test.parquet");
        assert_eq!(
            DataTransfer::path_is_compressed_file(path),
            Some(4330327753845164038)
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
            *data_transfer.compressed_files.get(&KEY).unwrap(),
            COMPRESSED_FILE_SIZE * 2
        );
    }

    #[tokio::test]
    async fn test_add_compressed_file_into_new_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_target_dir, mut data_transfer) =
            create_data_transfer_component(temp_dir.path()).await;
        let parquet_path = create_compressed_file(temp_dir.path(), "test");

        assert!(data_transfer
            .add_compressed_file(&KEY, parquet_path.as_path())
            .await
            .is_ok());

        assert_eq!(
            data_transfer.compressed_files.get(&KEY).unwrap(),
            &COMPRESSED_FILE_SIZE
        );
    }

    #[tokio::test]
    async fn test_add_compressed_file_into_existing_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_target_dir, mut data_transfer) =
            create_data_transfer_component(temp_dir.path()).await;
        let parquet_path = create_compressed_file(temp_dir.path(), "test");

        data_transfer
            .add_compressed_file(&KEY, parquet_path.as_path())
            .await
            .unwrap();
        data_transfer
            .add_compressed_file(&KEY, parquet_path.as_path())
            .await
            .unwrap();

        assert_eq!(
            data_transfer.compressed_files.get(&KEY).unwrap(),
            &(COMPRESSED_FILE_SIZE * 2)
        );
    }

    #[tokio::test]
    async fn test_add_non_existent_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join(format!("{}/compressed", KEY));
        fs::create_dir_all(path.clone()).unwrap();

        let (_target_dir, mut data_transfer) =
            create_data_transfer_component(temp_dir.path()).await;

        let parquet_path = path.join("test_parquet.parquet");
        assert!(data_transfer
            .add_compressed_file(&KEY, parquet_path.as_path())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_transfer_single_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path()).await;
        let parquet_path = create_compressed_file(temp_dir.path(), "test");

        data_transfer
            .add_compressed_file(&KEY, parquet_path.as_path())
            .await
            .unwrap();
        data_transfer.transfer_data(&KEY).await.unwrap();

        assert_data_transferred(vec![parquet_path], target_dir, data_transfer);
    }

    #[tokio::test]
    async fn test_transfer_multiple_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path()).await;
        let path_1 = create_compressed_file(temp_dir.path(), "test_1");
        let path_2 = create_compressed_file(temp_dir.path(), "test_2");

        data_transfer
            .add_compressed_file(&KEY, path_1.as_path())
            .await
            .unwrap();
        data_transfer
            .add_compressed_file(&KEY, path_2.as_path())
            .await
            .unwrap();
        data_transfer.transfer_data(&KEY).await.unwrap();

        assert_data_transferred(vec![path_1, path_2], target_dir, data_transfer);
    }

    #[tokio::test]
    async fn test_transfer_if_reaching_batch_size_when_adding() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path()).await;
        let parquet_path = create_compressed_file(temp_dir.path(), "test");

        // Set the max batch size to ensure that the file is transferred immediately.
        data_transfer.transfer_batch_size_in_bytes = COMPRESSED_FILE_SIZE - 1;
        data_transfer
            .add_compressed_file(&KEY, parquet_path.as_path())
            .await
            .unwrap();

        assert_data_transferred(vec![parquet_path], target_dir, data_transfer);
    }

    #[tokio::test]
    async fn test_transfer_if_reaching_batch_size_on_start_up() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path_1 = create_compressed_file(temp_dir.path(), "test_1");
        let path_2 = create_compressed_file(temp_dir.path(), "test_2");
        let path_3 = create_compressed_file(temp_dir.path(), "test_3");

        // Since the max batch size is 1 byte smaller than 3 compressed files, the data should be transferred immediately.
        let (target_dir, data_transfer) = create_data_transfer_component(temp_dir.path()).await;

        assert_data_transferred(vec![path_1, path_2, path_3], target_dir, data_transfer);
    }

    #[tokio::test]
    async fn test_flush_compressed_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path_1 = create_compressed_file(temp_dir.path(), "test_1");
        let path_2 = create_compressed_file(temp_dir.path(), "test_2");
        let (target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path()).await;

        data_transfer.flush_compressed_files().await.unwrap();

        assert_data_transferred(vec![path_1, path_2], target_dir, data_transfer);
    }

    /// Assert that the files in `paths` are all removed, a file has been created in `target_dir`,
    /// and that the hashmap containing the compressed files size is set to 0.
    fn assert_data_transferred(paths: Vec<PathBuf>, target: TempDir, data_transfer: DataTransfer) {
        for path in &paths {
            assert!(!path.exists());
        }

        // The transferred file should have a time range file name that matches the compressed data.
        let target_path = target.path().join(format!("{}/compressed/0-3.parquet", KEY));
        assert!(target_path.exists());

        // The file should have 3 * number_of_files rows since each compressed file has 3 rows.
        let batch = StorageEngine::read_entire_apache_parquet_file(target_path.as_path()).unwrap();
        assert_eq!(batch.num_rows(), 3 * paths.len());

        assert_eq!(
            *data_transfer.compressed_files.get(&KEY).unwrap(),
            0 as usize
        );
    }

    /// Set up a data folder with a key folder that has a single compressed file in it.
    /// Return the path to the created Apache Parquet file.
    fn create_compressed_file(local_data_folder_path: &Path, file_name: &str) -> PathBuf {
        let path = local_data_folder_path.join(format!("{}/compressed", KEY));
        fs::create_dir_all(path.clone()).unwrap();

        let batch = test_util::get_compressed_segment_record_batch();
        let parquet_path = path.join(format!("{}.parquet", file_name));
        StorageEngine::write_batch_to_apache_parquet_file(batch.clone(), parquet_path.as_path())
            .unwrap();

        parquet_path
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
        )
        .await
        .unwrap();

        (target_dir, data_transfer)
    }
}
