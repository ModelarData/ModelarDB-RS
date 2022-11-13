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
use std::io::Error as IOError;
use std::io::ErrorKind::Other;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::BufMut;
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use object_store::path::{Path as ObjectStorePath, PathPart};
use object_store::{ObjectStore};
use object_store::local::LocalFileSystem;
use tokio::runtime::Runtime;
use futures::StreamExt;
use tonic::codegen::Bytes;

use crate::{storage, StorageEngine};

pub(super) struct DataTransfer {
    /// Tokio runtime for executing asynchronous tasks.
    runtime: Arc<Runtime>,
    /// Path to the folder containing all compressed data managed by the [`StorageEngine`].
    data_folder_path: PathBuf,
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
        let mut compressed_files = HashMap::new();
        runtime.block_on(async {
            let list_stream = data_folder_object_store.list(None).await.unwrap();

            list_stream.map(|maybe_meta| {
                if let Ok(meta) = maybe_meta {
                    // If the file is a compressed file, add it to the compressed files.
                    if let Some(key) = Self::path_is_compressed_file(meta.location) {
                        *compressed_files.entry(key).or_insert(0) += meta.size;
                    }
                }
            }).collect::<Vec<_>>().await;
        });

        let mut data_transfer = Self {
            runtime,
            data_folder_path,
            data_folder_object_store,
            target_object_store,
            compressed_files: compressed_files.clone(),
            transfer_batch_size_in_bytes,
        };

        // Check if data should be transferred immediately.
        for (key, size_in_bytes) in compressed_files.iter() {
            if size_in_bytes >= &transfer_batch_size_in_bytes {
                data_transfer.transfer_data(key).map_err(|err| IOError::new(Other, err.to_string()))?;
            }
        }

        Ok(data_transfer)
    }

    /// Insert the compressed file into the files to be transferred. Retrieve the size of the file
    /// and add it to the total size of the current local files under the key.
    pub(super) fn add_compressed_file(&mut self, key: &u64, file_path: &Path) -> Result<(), IOError> {
        let file_size = file_path.metadata()?.len() as usize;
        *self.compressed_files.entry(*key).or_insert(0) += file_size;

        // If the combined size of the files is larger than the batch size, transfer the data to the blob store.
        if self.compressed_files.get(key).unwrap() >= &self.transfer_batch_size_in_bytes {
            self.transfer_data(key).map_err(|err| IOError::new(Other, err.to_string()))?;
        }

        Ok(())
    }

    // TODO: Handle the case where a connection can not be established.
    // TODO: Handle the unwraps on the object store calls.
    /// Transfer the data corresponding to `key` to the blob store. Once successfully transferred,
    /// the data is deleted from local storage. Return [`Ok`] if the file was written successfully,
    /// otherwise [`ParquetError`].
    fn transfer_data(&mut self, key: &u64) -> Result<(), ParquetError> {
        // Read all files that correspond to the key.
        let object_metas = self.runtime.block_on(async {
            let path = format!("{}/compressed", key).into();
            let list_stream = self.data_folder_object_store
                .list(Some(&path)).await.unwrap();

            list_stream.filter_map(|maybe_meta| async {
                maybe_meta.ok()
            }).collect::<Vec<_>>().await.into_iter()
        });

        // Combine the Apache Parquet files into a single RecordBatch.
        let record_batches = object_metas.clone().filter_map(|meta| {
            let path = self.data_folder_path.to_string_lossy();
            let file_path = PathBuf::from(format!("{}/{}", path, meta.location));

            StorageEngine::read_entire_apache_parquet_file(file_path.as_path()).ok()
        }).collect::<Vec<RecordBatch>>();

        let schema = record_batches[0].schema();
        let combined = compute::concat_batches(&schema, &record_batches)?;

        // Write the combined RecordBatch to a bytes buffer.
        let mut buf = vec![].writer();
        let mut arrow_writer = storage::create_apache_arrow_writer(&mut buf, schema)?;
        arrow_writer.write(&combined)?;
        arrow_writer.close()?;

        self.runtime.block_on(async {
            // Transfer the read data to the blob store.
            let file_name = storage::create_time_range_file_name(&combined);
            let path = format!("{}/compressed/{}", key, file_name).into();
            self.target_object_store.put(&path, Bytes::from(buf.into_inner())).await.unwrap();

            // Delete the transferred files from local storage.
            for meta in object_metas.clone() {
                self.target_object_store.delete(&meta.location).await.unwrap();
            }

            // Delete the transferred files from the in-memory tracking of compressed files.
            let transferred_bytes: usize = object_metas.map(|meta| meta.size).sum();
            *self.compressed_files.get_mut(key).unwrap() -= transferred_bytes;
        });

        Ok(())
    }

    /// Return the key if `path` is an Apache Parquet file with compressed data, otherwise [`None`].
    fn path_is_compressed_file(path: ObjectStorePath) -> Option<u64> {
        let path_parts: Vec<PathPart> = path.parts().collect();

        if let Some(key_part) = path_parts.get(0) {
            if let Ok(key) = key_part.as_ref().parse::<u64>() {
                if let Some(file_name_part) = path_parts.get(2) {
                    // TODO: Use is_path_an_apache_parquet_file when merged.
                    if Some(&PathPart::from("compressed")) == path_parts.get(1)
                        && file_name_part.as_ref().ends_with(".parquet") {
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

    use object_store::path::Path as ObjectStorePath;
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
        assert_eq!(DataTransfer::path_is_compressed_file(path), Some(4330327753845164038));
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

        let transfer_batch_size_in_bytes = COMPRESSED_FILE_SIZE * 3 - 1;
        (target_dir, DataTransfer::try_new(runtime, data_folder_path.to_path_buf(),
                                           target_object_store, transfer_batch_size_in_bytes).unwrap())
    }
}