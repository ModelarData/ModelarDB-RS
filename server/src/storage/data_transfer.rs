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

//! Support for efficiently transferring data to a blob store in the cloud. Data saved locally on
//! disk is managed here until it is of a sufficient size to be transferred efficiently. Furthermore,
//! the component ensures that local data is kept on disk until a reliable connection is established.

use std::collections::HashMap;
use std::fs;
use std::io::Error as IOError;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::{ObjectStore};
use object_store::local::LocalFileSystem;
use tokio::runtime::Runtime;

use crate::StorageEngine;

pub(super) struct DataTransfer {
    /// Tokio runtime for executing asynchronous tasks.
    runtime: Arc<Runtime>,
    /// The object store containing all compressed data managed by the [`StorageEngine`].
    data_folder_object_store: Arc<dyn ObjectStore>,
    /// The object store that the data should be transferred to.
    target_object_store: Arc<dyn ObjectStore>,
    /// Map from keys to the combined size in bytes of the compressed files saved under the key.
    compressed_files: HashMap<u64, usize>,
    /// The number of bytes that is required before transferring a batch of data to the blob store.
    transfer_batch_size_in_bytes: usize,
}

impl DataTransfer {
    /// Create a new data transfer instance and initialize it with the compressed files already
    /// existing in `data_folder_path`. If `data_folder_path` or a path within `data_folder_path`
    /// could not be read, return [`IOError`].
    pub(super) fn try_new(
        runtime: Arc<Runtime>,
        data_folder_path: PathBuf,
        target_object_store: Arc<dyn ObjectStore>,
        transfer_batch_size_in_bytes: usize,
    ) -> Result<Self, IOError> {
        // TODO: When the storage engine is changed to use object store for everything, receive
        //       the object store directly through the parameters instead.
        let local_fs = LocalFileSystem::new_with_prefix(data_folder_path.clone())?;
        let data_folder_object_store = Arc::new(local_fs);

        // Parse through the data folder to retrieve already existing files that should be transferred.
        let dir = fs::read_dir(data_folder_path.clone())?;

        // Use a filter map to remove all dir entries that does not contain compressed files. For each
        // item that does contain compressed files, return a tuple with the key and the size in bytes.
        let compressed_files = dir.filter_map(|maybe_dir_entry| {
            if let Ok(dir_entry) = maybe_dir_entry {
                let path = dir_entry.path();

                if let Some(key) = Self::path_contains_compressed_files(path.as_path()) {
                    let compressed_path = path.join("compressed");
                    let size = Self::get_total_compressed_files_size(compressed_path.as_path());

                    return Some((key, size));
                }
            }

            None
        }).into_iter().collect();

        Ok(Self {
            runtime,
            data_folder_object_store,
            target_object_store,
            compressed_files,
            transfer_batch_size_in_bytes,
        })
    }

    /// Insert the compressed file into the files to be transferred. Retrieve the size of the file
    /// and add it to the total size of the current local files under the key.
    pub(super) fn add_compressed_file(&mut self, key: &u64, file_path: &Path) -> Result<(), IOError> {
        let file_size = file_path.metadata()?.len() as usize;
        *self.compressed_files.entry(*key).or_insert(0) += file_size;

        // If the combined size of the files is larger than the batch size, transfer the data to the blob store.
        if self.compressed_files.get(key).unwrap() >= &self.transfer_batch_size_in_bytes {
            self.transfer_data(key);
        }

        Ok(())
    }

    /// Transfer all compressed files currently in the data folder to the target blob store.
    pub(super) fn flush_compressed_files(&mut self) {
        // TODO: Iterate over the keys in the compressed files hashmap and call the transfer data function for each key.
    }

    /// Transfer the data corresponding to `key` to the blob store. Once successfully transferred,
    /// delete the data from local storage.
    fn transfer_data(&mut self, key: &u64) {
        // TODO: Read all files that correspond to the key.
        // TODO: Transfer the read data to the blob store.
        // TODO: Delete the transferred files from local storage.

        // TODO: Handle the base where a connection can not be established.
    }

    /// Return the key if `path` is a key folder containing compressed data, otherwise [`None`].
    fn path_contains_compressed_files(path: &Path) -> Option<u64> {
        if path.is_dir() && path.join("compressed").is_dir() {
            // Convert the directory name to the 64-bit key and return it if possible.
            let key = path.file_name().unwrap().to_string_lossy().to_string();
            key.parse::<u64>().ok()
        } else {
            None
        }
    }

    /// Return the total combined size in bytes of the compressed files in `path`.
    fn get_total_compressed_files_size(path: &Path) -> usize {
        // Unwrap is safe since the directory is checked before calculating the files size.
        let dir = fs::read_dir(path).unwrap();

        dir.filter_map(|maybe_dir_entry| {
            if let Ok(dir_entry) = maybe_dir_entry {
                if StorageEngine::is_path_an_apache_parquet_file(dir_entry.path().as_path()) {
                    return Some(dir_entry.metadata().unwrap().len() as usize);
                }
            }

            None
        }).sum()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    use crate::storage::data_transfer::DataTransfer;
    use crate::storage::test_util;
    use crate::StorageEngine;

    const KEY: u64 = 1668574317311628292;
    const COMPRESSED_FILE_SIZE: usize = 2576;

    #[test]
    fn test_include_existing_files_on_start_up() {
        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = set_up_data_folder(temp_dir.path());
        let (_target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path());

        assert_eq!(*data_transfer.compressed_files.get(&KEY).unwrap(), COMPRESSED_FILE_SIZE)
    }

    #[test]
    fn test_transfer_if_reaching_batch_size_on_start_up() {}

    #[test]
    fn test_file_does_not_contain_compressed_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.txt");
        fs::File::create(path.clone()).unwrap();

        assert!(DataTransfer::path_contains_compressed_files(path.as_path()).is_none());
    }

    #[test]
    fn test_empty_folder_does_not_contain_compressed_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        assert!(DataTransfer::path_contains_compressed_files(temp_dir.path()).is_none());
    }

    #[test]
    fn test_non_empty_folder_without_compressed_folder_does_not_contain_compressed_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join(format!("{}/uncompressed", KEY));
        fs::create_dir_all(path.clone()).unwrap();

        let key_path = temp_dir.path().join(KEY.to_string());
        assert!(DataTransfer::path_contains_compressed_files(key_path.as_path()).is_none());
    }

    #[test]
    fn test_compressed_folder_contains_compressed_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join(format!("{}/compressed", KEY));
        fs::create_dir_all(path.clone()).unwrap();

        let key_path = temp_dir.path().join(KEY.to_string());
        assert!(DataTransfer::path_contains_compressed_files(key_path.as_path()).is_some());
    }

    #[test]
    fn test_get_total_compressed_files_size() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create a folder with a text file and an Apache Parquet file.
        let txt_path = temp_dir.path().join("test.txt");
        fs::write(txt_path.clone(), "test content").unwrap();

        assert_eq!(DataTransfer::get_total_compressed_files_size(temp_dir.path()), 0);

        let batch = test_util::get_compressed_segment_record_batch();
        let parquet_path = temp_dir.path().join("test_parquet.parquet");
        StorageEngine::write_batch_to_apache_parquet_file(batch.clone(), parquet_path.as_path()).unwrap();

        // Only the size of the Apache Parquet file should be counted.
        assert_eq!(DataTransfer::get_total_compressed_files_size(temp_dir.path()), COMPRESSED_FILE_SIZE);
    }

    #[test]
    fn test_add_compressed_file_into_new_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path());
        let parquet_path = set_up_data_folder(temp_dir.path());

        assert!(data_transfer.add_compressed_file(&KEY, parquet_path.as_path()).is_ok());
        assert_eq!(data_transfer.compressed_files.get(&KEY).unwrap(), &COMPRESSED_FILE_SIZE);
    }

    #[test]
    fn test_add_compressed_file_into_existing_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path());
        let parquet_path = set_up_data_folder(temp_dir.path());

        data_transfer.add_compressed_file(&KEY, parquet_path.as_path()).unwrap();
        data_transfer.add_compressed_file(&KEY, parquet_path.as_path()).unwrap();

        assert_eq!(data_transfer.compressed_files.get(&KEY).unwrap(), &(COMPRESSED_FILE_SIZE * 2));
    }

    /// Set up a data folder with a key folder that has a single compressed file in it.
    /// Return the path to the created Apache Parquet file.
    fn set_up_data_folder(data_folder_path: &Path) -> PathBuf {
        let path = data_folder_path.join(format!("{}/compressed", KEY));
        fs::create_dir_all(path.clone()).unwrap();

        let batch = test_util::get_compressed_segment_record_batch();
        let parquet_path = path.join("test_parquet.parquet");
        StorageEngine::write_batch_to_apache_parquet_file(batch.clone(), parquet_path.as_path()).unwrap();

        parquet_path
    }

    #[test]
    fn test_add_non_existent_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join(format!("{}/compressed", KEY));
        fs::create_dir_all(path.clone()).unwrap();

        let (_target_dir, mut data_transfer) = create_data_transfer_component(temp_dir.path());

        let parquet_path = path.join("test_parquet.parquet");
        assert!(data_transfer.add_compressed_file(&KEY, parquet_path.as_path()).is_err())
    }

    #[test]
    fn test_flush_compressed_files() {}

    #[test]
    fn test_transfer_if_reaching_batch_size_when_adding() {}

    #[test]
    fn test_transfer_single_file() {
        // TODO: Check that the file has been deleted.
    }

    #[test]
    fn test_transfer_multiple_files() {
        // TODO: Check that the files have been deleted.
    }

    /// Create a data transfer component with a target object store that is deleted once the test is finished.
    fn create_data_transfer_component(data_folder_path: &Path) -> (TempDir, DataTransfer) {
        let target_dir = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::new().unwrap());

        // Create the target object store.
        let local_fs = LocalFileSystem::new_with_prefix(target_dir.path())
            .expect("Error creating local file system.");
        let target_object_store = Arc::new(local_fs);

        (target_dir, DataTransfer::try_new(runtime, data_folder_path.to_path_buf(),
                                           target_object_store, 64).unwrap())
    }
}