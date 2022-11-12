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

//! Converts a batch of data to uncompressed data points, stores uncompressed data points temporarily
//! in an in-memory buffer that spills to Apache Parquet files, and stores data points compressed as
//! models in memory to batch compressed data before saving it to Apache Parquet files.

mod compressed_data_manager;
mod segment;
mod time_series;
mod uncompressed_data_manager;

use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};

use crate::errors::ModelarDbError;
use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::metadata::MetadataManager;
use crate::storage::compressed_data_manager::CompressedDataManager;
use crate::storage::segment::FinishedSegment;
use crate::storage::uncompressed_data_manager::UncompressedDataManager;
use crate::types::Timestamp;

// TODO: Look into custom errors for all errors in storage engine.

/// The scheme and host at which the query data folder is stored.
pub const QUERY_DATA_FOLDER_SCHEME_AND_HOST: &str = "query";

/// The scheme with host at which the query data folder is stored.
pub const QUERY_DATA_FOLDER_SCHEME_WITH_HOST: &str = "query://query";

/// The expected [first four bytes of any Apache Parquet file].
///
/// [first four bytes of any Apache Parquet file]: https://en.wikipedia.org/wiki/List_of_file_signatures
const APACHE_PARQUET_FILE_SIGNATURE: &[u8] = &[80, 65, 82, 49]; // PAR1.

/// Note that the capacity has to be a multiple of 64 bytes to avoid the actual capacity
/// being larger due to internal alignment when allocating memory for the builders.
const BUILDER_CAPACITY: usize = 64 * 1024; // Each element is a Timestamp and a Value.

/// Manages all uncompressed and compressed data, both while being built and when finished.
pub struct StorageEngine {
    // TODO: is it better to use MetadataManager from context to share caches
    // with tables or a separate MetadataManager to not require taking a lock?
    /// Manager that controls all metadata for uncompressed and compressed data.
    metadata_manager: MetadataManager,
    /// Manager that contains and controls all uncompressed data.
    uncompressed_data_manager: UncompressedDataManager,
    /// Manager that contains and controls all compressed data.
    compressed_data_manager: CompressedDataManager,
}

impl StorageEngine {
    pub fn new(
        data_folder_path: PathBuf,
        metadata_manager: MetadataManager,
        compress_directly: bool,
    ) -> Self {
        let uncompressed_data_manager = UncompressedDataManager::new(
            data_folder_path.clone(),
            metadata_manager.uncompressed_reserved_memory_in_bytes,
            metadata_manager.get_uncompressed_schema(),
            metadata_manager.get_compressed_schema(),
            compress_directly,
        );

        let compressed_data_manager = CompressedDataManager::new(
            data_folder_path,
            metadata_manager.compressed_reserved_memory_in_bytes,
            metadata_manager.get_compressed_schema(),
        );

        Self {
            metadata_manager,
            uncompressed_data_manager,
            compressed_data_manager,
        }
    }

    /// Pass `data_points` to [`UncompressedDataManager`]. Return [`Ok`] if the data was
    /// successfully inserted, otherwise return [`Err`].
    pub fn insert_data_points(
        &mut self,
        model_table: &ModelTableMetadata,
        data_points: &RecordBatch,
    ) -> Result<(), String> {
        // TODO: When the compression component is changed, just insert the data points.
        let compressed_segments = self.uncompressed_data_manager.insert_data_points(
            &mut self.metadata_manager,
            model_table,
            data_points,
        )?;

        for (key, segment) in compressed_segments {
            self.compressed_data_manager
                .insert_compressed_segment(key, segment);
        }

        Ok(())
    }

    /// Retrieve the oldest [`FinishedSegment`] from [`UncompressedDataManager`] and return it.
    /// Return [`None`] if there are no [`FinishedSegments`](FinishedSegment).
    pub fn get_finished_segment(&mut self) -> Option<FinishedSegment> {
        self.uncompressed_data_manager.get_finished_segment()
    }

    /// Flush all of the data the [`StorageEngine`] is managing to disk.
    pub fn flush(&mut self) {
        // Flush UncompressedDataManager.
        for (key, segment) in self.uncompressed_data_manager.flush() {
            self.compressed_data_manager
                .insert_compressed_segment(key, segment);
        }

        // Flush CompressedDataManager.
        self.compressed_data_manager.flush();
    }

    /// Pass `segment` to [`CompressedDataManager`].
    pub fn insert_compressed_segment(&mut self, key: u64, segment: RecordBatch) {
        self.compressed_data_manager
            .insert_compressed_segment(key, segment)
    }

    /// Retrieve the compressed files that correspond to `keys` within the given range of time.
    /// If `keys` contain a key that does not exist or the end time is before the start time,
    /// [`DataRetrievalError`](ModelarDbError::DataRetrievalError) is returned.
    pub async fn get_compressed_files(
        &mut self,
        keys: &[u64],
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        query_data_folder: &Arc<dyn ObjectStore>,
    ) -> Result<Vec<(String, ObjectMeta)>, ModelarDbError> {
        let start_time = start_time.unwrap_or(0);
        let end_time = end_time.unwrap_or(Timestamp::MAX);
        let mut compressed_files: Vec<(String, ObjectMeta)> = vec![];

        if start_time > end_time {
            return Err(ModelarDbError::DataRetrievalError(format!(
                "Start time '{}' cannot be after end time '{}'.",
                start_time, end_time
            )));
        };

        for key in keys {
            // For each key, list the files that contain compressed data.
            let key_files = self
                .compressed_data_manager
                .save_and_get_saved_compressed_files(key, &query_data_folder)
                .await?;

            // Prune the files based on the time range the file covers.
            let pruned_key_files = key_files.into_iter().filter(|(_, object_meta)| {
                let last_file_part = object_meta.location.parts().last().unwrap();
                compressed_data_manager::is_compressed_file_within_time_range(
                    last_file_part.as_ref(),
                    start_time,
                    end_time,
                )
            });

            compressed_files.extend(pruned_key_files)
        }
        Ok(compressed_files)
    }

    /// Write `batch` to an Apache Parquet file at the location given by `file_path`. `file_path`
    /// must use the extension '.parquet'. Return [`Ok`] if the file was written successfully,
    /// otherwise [`ParquetError`].
    pub fn write_batch_to_apache_parquet_file(
        batch: RecordBatch,
        file_path: &Path,
    ) -> Result<(), ParquetError> {
        let error = ParquetError::General(format!(
            "Apache Parquet file at path '{}' could not be created.",
            file_path.display()
        ));

        // Check if the extension of the given path is correct.
        if file_path.extension().and_then(OsStr::to_str) == Some("parquet") {
            let file = File::create(file_path).map_err(|_e| error)?;
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(EnabledStatistics::Page)
                .build();

            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
            writer.write(&batch)?;
            writer.close()?;

            Ok(())
        } else {
            Err(error)
        }
    }

    /// Read all rows from the Apache Parquet file at the location given by `file_path` and return them
    /// in a [`RecordBatch`]. If the file could not be read successfully, [`ParquetError`] is returned.
    pub fn read_entire_apache_parquet_file(file_path: &Path) -> Result<RecordBatch, ParquetError> {
        let error = ParquetError::General(format!(
            "Apache Parquet file at path '{}' could not be read.",
            file_path.display()
        ));

        // Create a reader that can be used to read an Apache Parquet file.
        let file = File::open(file_path).map_err(|_e| error.clone())?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.with_batch_size(usize::MAX).build()?;

        let record_batch = reader.next().ok_or_else(|| error)??;
        Ok(record_batch)
    }

    /// Return [`true`] if `file_path` is a readable Apache Parquet file,
    /// otherwise [`false`].
    pub async fn is_path_an_apache_parquet_file(
        object_store: &Arc<dyn ObjectStore>,
        file_path: &ObjectStorePath,
    ) -> bool {
        if let Ok(bytes) = object_store.get_range(file_path, 0..4).await {
            bytes == crate::storage::APACHE_PARQUET_FILE_SIGNATURE
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Schema;
    use object_store::local::LocalFileSystem;
    use tempfile::{tempdir, TempDir};

    use crate::get_array;
    use crate::metadata::test_util as metadata_test_util;
    use crate::types::TimestampArray;

    // Tests for get_compressed_files().
    #[tokio::test]
    async fn test_can_get_compressed_files_for_keys() {
        let (temp_dir, mut storage_engine) = create_storage_engine();

        // Insert compressed segments with multiple different keys.
        let segment = test_util::get_compressed_segment_record_batch();
        storage_engine
            .compressed_data_manager
            .insert_compressed_segment(1, segment.clone());
        storage_engine
            .compressed_data_manager
            .insert_compressed_segment(2, segment);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = storage_engine
            .get_compressed_files(&[1, 2], None, None, &object_store)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_get_no_compressed_files_for_non_existent_key() {
        let (temp_dir, mut storage_engine) = create_storage_engine();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = storage_engine.get_compressed_files(&[1], None, None, &object_store);

        assert!(result.await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_can_get_compressed_files_with_start_time() {
        let (temp_dir, mut storage_engine) = create_storage_engine();
        let (segment_1, _segment_2) = insert_separated_segments(&mut storage_engine, 1, 2, 0);

        // If we have a start time after the first segments ends, only the file from the second
        // segment should be retrieved.
        let end_times = get_array!(segment_1, 3, TimestampArray);
        let start_time = Some(end_times.value(end_times.len() - 1) + 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = storage_engine.get_compressed_files(&[1, 2], start_time, None, &object_store);
        let files = result.await.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the key of the second segment.
        let file_path = files.get(0).unwrap().1.location.to_string();
        assert!(file_path.contains("2/compressed"));
    }

    #[tokio::test]
    async fn test_can_get_compressed_files_with_end_time() {
        let (temp_dir, mut storage_engine) = create_storage_engine();
        let (_segment_1, segment_2) = insert_separated_segments(&mut storage_engine, 1, 2, 0);

        // If we have an end time before the second segment starts, only the file from the first
        // segment should be retrieved.
        let start_times = get_array!(segment_2, 2, TimestampArray);
        let end_time = Some(start_times.value(1) - 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = storage_engine.get_compressed_files(&[1, 2], None, end_time, &object_store);
        let files = result.await.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the key of the first segment.
        let file_path = files.get(0).unwrap().1.location.to_string();
        assert!(file_path.contains("1/compressed"));
    }

    #[tokio::test]
    async fn test_can_get_compressed_files_with_start_time_and_end_time() {
        let (temp_dir, mut storage_engine) = create_storage_engine();

        // Insert 4 segments with a ~1 second time difference between segment 1 and 2 and segment 3 and 4.
        let (segment_1, _segment_2) = insert_separated_segments(&mut storage_engine, 1, 2, 0);
        let (_segment_3, segment_4) = insert_separated_segments(&mut storage_engine, 3, 4, 1000);

        // If we have a start time after the first segment ends and an end time before the fourth
        // segment starts, only the files from the second and third segment should be retrieved.
        let end_times = get_array!(segment_1, 3, TimestampArray);
        let start_times = get_array!(segment_4, 2, TimestampArray);

        let start_time = Some(end_times.value(end_times.len() - 1) + 100);
        let end_time = Some(start_times.value(1) - 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result =
            storage_engine.get_compressed_files(&[1, 2, 3, 4], start_time, end_time, &object_store);
        let files = result.await.unwrap();
        assert_eq!(files.len(), 2);

        // The paths to the returned compressed files should contain the key of the second and third segment.
        let file_path = files.get(0).unwrap().1.location.to_string();
        assert!(file_path.contains("2/compressed"));

        let file_path = files.get(1).unwrap().1.location.to_string();
        assert!(file_path.contains("3/compressed"));
    }

    /// Create and insert two compressed segments with a 1 second time difference offset by `start_time`.
    fn insert_separated_segments(
        storage_engine: &mut StorageEngine,
        key_1: u64,
        key_2: u64,
        start_time: i64,
    ) -> (RecordBatch, RecordBatch) {
        let segment_1 = test_util::get_compressed_segment_record_batch_with_time(1000 + start_time);
        storage_engine
            .compressed_data_manager
            .insert_compressed_segment(key_1, segment_1.clone());

        let segment_2 = test_util::get_compressed_segment_record_batch_with_time(2000 + start_time);
        storage_engine
            .compressed_data_manager
            .insert_compressed_segment(key_2, segment_2.clone());

        (segment_1, segment_2)
    }

    #[tokio::test]
    async fn test_cannot_get_compressed_files_where_end_time_is_before_start_time() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        let segment = test_util::get_compressed_segment_record_batch();
        storage_engine
            .compressed_data_manager
            .insert_compressed_segment(1, segment);

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let result = storage_engine.get_compressed_files(&[1], Some(10), Some(1), &object_store);
        assert!(result.await.is_err());
    }

    /// Create a [`StorageEngine`] with a folder that is deleted once the test is finished.
    fn create_storage_engine() -> (TempDir, StorageEngine) {
        let temp_dir = tempdir().unwrap();
        let data_folder_path_buf = temp_dir.path().to_path_buf();
        let data_folder_path = data_folder_path_buf.clone();

        (
            temp_dir,
            StorageEngine::new(
                data_folder_path_buf,
                metadata_test_util::get_test_metadata_manager(data_folder_path.as_path()),
                false,
            ),
        )
    }

    // Tests for writing and reading Apache Parquet files.
    #[test]
    fn test_write_batch_to_apache_parquet_file() {
        let temp_dir = tempdir().unwrap();
        let batch = test_util::get_compressed_segment_record_batch();

        let parquet_path = temp_dir.path().join("test.parquet");
        StorageEngine::write_batch_to_apache_parquet_file(batch, parquet_path.as_path()).unwrap();

        assert!(parquet_path.exists());
    }

    #[test]
    fn test_write_empty_batch_to_apache_parquet_file() {
        let schema = Schema::new(vec![]);
        let batch = RecordBatch::new_empty(Arc::new(schema));

        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("empty.parquet");
        StorageEngine::write_batch_to_apache_parquet_file(batch, parquet_path.as_path()).unwrap();

        assert!(parquet_path.exists());
    }

    #[test]
    fn test_write_batch_to_file_with_invalid_extension() {
        write_to_file_and_assert_failed("test.txt".to_owned());
    }

    #[test]
    fn test_write_batch_to_file_with_no_extension() {
        write_to_file_and_assert_failed("test".to_owned());
    }

    fn write_to_file_and_assert_failed(file_name: String) {
        let temp_dir = tempdir().unwrap();
        let batch = test_util::get_compressed_segment_record_batch();

        let parquet_path = temp_dir.path().join(file_name);
        let result =
            StorageEngine::write_batch_to_apache_parquet_file(batch, parquet_path.as_path());

        assert!(result.is_err());
        assert!(!parquet_path.exists());
    }

    #[test]
    fn test_read_entire_apache_parquet_file() {
        let file_name = "test.parquet".to_owned();
        let (_temp_dir, path, batch) = create_apache_parquet_file_in_temp_dir(file_name);

        let result = StorageEngine::read_entire_apache_parquet_file(path.as_path());

        assert!(result.is_ok());
        assert_eq!(batch, result.unwrap());
    }

    #[test]
    fn test_read_from_non_apache_parquet_file() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.txt");
        File::create(path.clone()).unwrap();

        let result = StorageEngine::read_entire_apache_parquet_file(path.as_path());

        assert!(result.is_err());
    }

    #[test]
    fn test_read_from_non_existent_path() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("none.parquet");
        let result = StorageEngine::read_entire_apache_parquet_file(path.as_path());

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_is_parquet_path_apache_parquet_file() {
        let file_name = "test.parquet".to_owned();
        let (_temp_dir, path, _batch) = create_apache_parquet_file_in_temp_dir(file_name);

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let object_store_path = ObjectStorePath::from_filesystem_path(path).unwrap();

        assert!(
            StorageEngine::is_path_an_apache_parquet_file(&object_store, &object_store_path).await
        );
    }

    /// Create an Apache Parquet file in the [`TempDir`] from a generated [`RecordBatch`].
    fn create_apache_parquet_file_in_temp_dir(
        file_name: String,
    ) -> (TempDir, PathBuf, RecordBatch) {
        let temp_dir = tempdir().unwrap();
        let batch = test_util::get_compressed_segment_record_batch();

        let parquet_path = temp_dir.path().join(file_name);
        StorageEngine::write_batch_to_apache_parquet_file(batch.clone(), parquet_path.as_path())
            .unwrap();

        (temp_dir, parquet_path, batch)
    }

    #[tokio::test]
    async fn test_is_non_parquet_path_apache_parquet_file() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.txt");

        let mut file = File::create(path.clone()).unwrap();
        let mut signature = APACHE_PARQUET_FILE_SIGNATURE.to_vec();
        signature.reverse();
        file.write_all(&signature).unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let object_store_path = ObjectStorePath::from_filesystem_path(&path).unwrap();

        assert!(path.exists());
        assert!(
            !StorageEngine::is_path_an_apache_parquet_file(&object_store, &object_store_path).await
        );
    }

    #[tokio::test]
    async fn test_is_empty_parquet_path_apache_parquet_file() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.txt");
        File::create(path.clone()).unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let object_store_path = ObjectStorePath::from_filesystem_path(&path).unwrap();

        assert!(path.exists());
        assert!(
            !StorageEngine::is_path_an_apache_parquet_file(&object_store, &object_store_path).await
        );
    }
}

#[cfg(test)]
/// Separate module for compressed segment utility functions since they are needed to test
/// multiple modules in the storage engine.
pub mod test_util {
    use std::sync::Arc;

    use datafusion::arrow::array::{BinaryArray, Float32Array, UInt8Array};
    use datafusion::arrow::record_batch::RecordBatch;

    use crate::metadata::test_util as metadata_test_util;
    use crate::types::{TimestampArray, ValueArray};

    pub const COMPRESSED_SEGMENT_SIZE: usize = 2176;

    /// Return a generated compressed segment with three model segments.
    pub fn get_compressed_segment_record_batch() -> RecordBatch {
        get_compressed_segment_record_batch_with_time(0)
    }

    /// Return a generated compressed segment with three model segments. The segment time span is
    /// from `time_ms` to `time_ms` + 3.
    pub fn get_compressed_segment_record_batch_with_time(time_ms: i64) -> RecordBatch {
        let start_times = vec![time_ms, time_ms + 1, time_ms + 2];
        let end_times = vec![time_ms + 1, time_ms + 2, time_ms + 3];

        let model_type_id = UInt8Array::from(vec![2, 3, 3]);
        let timestamps = BinaryArray::from_vec(vec![b"000", b"001", b"010"]);
        let start_time = TimestampArray::from(start_times);
        let end_time = TimestampArray::from(end_times);
        let values = BinaryArray::from_vec(vec![b"1111", b"1000", b"0000"]);
        let min_value = ValueArray::from(vec![5.2, 10.3, 30.2]);
        let max_value = ValueArray::from(vec![20.2, 12.2, 34.2]);
        let error = Float32Array::from(vec![0.2, 0.5, 0.1]);

        let schema = metadata_test_util::get_compressed_schema();

        RecordBatch::try_new(
            schema.0,
            vec![
                Arc::new(model_type_id),
                Arc::new(timestamps),
                Arc::new(start_time),
                Arc::new(end_time),
                Arc::new(values),
                Arc::new(min_value),
                Arc::new(max_value),
                Arc::new(error),
            ],
        )
        .unwrap()
    }
}
