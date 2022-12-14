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

//! Ingests data points into temporary in-memory buffers that can be spilled to immutable Apache
//! Parquet files if necessary, uses the compression component to compress these buffers when they
//! are full or [`StorageEngine::flush()`] is called, stores the resulting data points compressed as
//! metadata and models in in-memory buffers to batch them before saving them to immutable Apache
//! Parquet files. The path to the Apache Parquet files containing relevant compressed data points
//! for a query can be retrieved by the query engine using
//! [`StorageEngine::get_compressed_files()`].

mod compressed_data_buffer;
mod compressed_data_manager;
pub(super) mod data_transfer;
mod uncompressed_data_buffer;
mod uncompressed_data_manager;

use std::ffi::OsStr;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::compute::kernels::aggregate;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use tonic::Status;

use crate::errors::ModelarDbError;
use crate::get_array;
use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::metadata::MetadataManager;
use crate::storage::compressed_data_manager::CompressedDataManager;
use crate::storage::data_transfer::DataTransfer;
use crate::storage::uncompressed_data_buffer::UncompressedDataBuffer;
use crate::storage::uncompressed_data_manager::UncompressedDataManager;
use crate::types::{Timestamp, TimestampArray, Value, ValueArray};

// TODO: Look into custom errors for all errors in storage engine.

/// The scheme and host at which the query data folder is stored.
pub const QUERY_DATA_FOLDER_SCHEME_AND_HOST: &str = "query";

/// The scheme with host at which the query data folder is stored.
pub const QUERY_DATA_FOLDER_SCHEME_WITH_HOST: &str = "query://query";

/// The expected [first four bytes of any Apache Parquet file].
///
/// [first four bytes of any Apache Parquet file]: https://en.wikipedia.org/wiki/List_of_file_signatures
const APACHE_PARQUET_FILE_SIGNATURE: &[u8] = &[80, 65, 82, 49]; // PAR1.

/// The capacity of each uncompressed data buffer as the number of elements in the buffer where each
/// element is a [`Timestamp`] and a [`Value`](use crate::types::Value). Note that the resulting
/// size of the buffer has to be a multiple of 64 bytes to avoid the actual capacity being larger
/// than the requested due to internal alignment when allocating memory for the two array builders.
const UNCOMPRESSED_DATA_BUFFER_CAPACITY: usize = 64 * 1024;

/// Manages all uncompressed and compressed data, both while being stored in memory during ingestion
/// and when persisted to disk afterwards.
pub struct StorageEngine {
    // TODO: is it better to use MetadataManager from context to share caches
    // with tables.rs or a separate MetadataManager to not require taking a lock?
    /// Manager that contains and controls all metadata for both uncompressed and compressed data.
    metadata_manager: MetadataManager,
    /// Manager that contains and controls all uncompressed data.
    uncompressed_data_manager: UncompressedDataManager,
    /// Manager that contains and controls all compressed data.
    compressed_data_manager: CompressedDataManager,
}

impl StorageEngine {
    pub fn new(
        data_transfer: Option<DataTransfer>,
        local_data_folder: PathBuf,
        metadata_manager: MetadataManager,
        compress_directly: bool,
    ) -> Self {
        let uncompressed_data_manager = UncompressedDataManager::new(
            local_data_folder.clone(),
            metadata_manager.uncompressed_reserved_memory_in_bytes,
            metadata_manager.get_uncompressed_schema(),
            metadata_manager.get_compressed_schema(),
            compress_directly,
        );

        let compressed_data_manager = CompressedDataManager::new(
            data_transfer,
            local_data_folder,
            metadata_manager.compressed_reserved_memory_in_bytes,
            metadata_manager.get_compressed_schema(),
        );

        Self {
            metadata_manager,
            uncompressed_data_manager,
            compressed_data_manager,
        }
    }

    /// Pass `data_points` to [`UncompressedDataManager`]. Return [`Ok`] if all of the data points
    /// were successfully inserted, otherwise return [`Err`].
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

        for segments in compressed_segments {
            self.compressed_data_manager
                .insert_compressed_segments(&model_table.name, segments)
                .map_err(|error| error.to_string())?;
        }

        Ok(())
    }

    /// Retrieve the oldest finished [`UncompressedDataBuffer`] from [`UncompressedDataManager`] and
    /// return it. Return [`None`] if there are no finished
    /// [`UncompressedDataBuffers`](UncompressedDataBuffer).
    pub fn get_finished_uncompressed_data_buffer(
        &mut self,
    ) -> Option<Box<dyn UncompressedDataBuffer>> {
        self.uncompressed_data_manager.get_finished_data_buffer()
    }

    /// Flush all of the data the [`StorageEngine`] is currently storing in memory to disk. If all
    /// of the data is successfully flushed to disk, return [`Ok`], otherwise return [`String`].
    pub fn flush(&mut self) -> Result<(), String> {
        // TODO: When the compression component is changed, just flush before managers.
        // Flush UncompressedDataManager.
        let compressed_buffers = self.uncompressed_data_manager.flush();
        let hash_to_table_name = self
            .metadata_manager
            .get_mapping_from_hash_to_table_name()
            .map_err(|error| error.to_string())?;

        let tag_hash_one_bits: u64 = 18446744073709550592;
        for (univariate_id, segment) in compressed_buffers {
            let tag_hash = univariate_id & tag_hash_one_bits;

            // unwrap() is safe as new univariate ids have been added to the metadata database.
            let table_name = hash_to_table_name.get(&tag_hash).unwrap();
            self.compressed_data_manager
                .insert_compressed_segments(table_name, segment)
                .map_err(|error| error.to_string())?;
        }

        // Flush CompressedDataManager.
        self.compressed_data_manager
            .flush()
            .map_err(|error| error.to_string())?;

        Ok(())
    }

    /// Transfer all of the compressed data the [`StorageEngine`] is managing to the remote object
    /// store.
    pub async fn transfer(&mut self) -> Result<(), Status> {
        if let Some(data_transfer) = self.compressed_data_manager.data_transfer.as_mut() {
            data_transfer
                .flush()
                .await
                .map_err(|error: ParquetError| Status::internal(error.to_string()))
        } else {
            Err(Status::internal("No remote object store available."))
        }
    }

    /// Retrieve the compressed files that correspond to the table with `table_name` within the
    /// given range of time and value. If a table with `table_name` does not exist, the end time is
    /// before the start time, or the max value is larger than the min value,
    /// [`DataRetrievalError`](ModelarDbError::DataRetrievalError) is returned.
    pub async fn get_compressed_files(
        &mut self,
        table_name: &str,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        min_value: Option<Value>,
        max_value: Option<Value>,
        query_data_folder: &Arc<dyn ObjectStore>,
    ) -> Result<Vec<ObjectMeta>, ModelarDbError> {
        let start_time = start_time.unwrap_or(0);
        let end_time = end_time.unwrap_or(Timestamp::MAX);

        if start_time > end_time {
            return Err(ModelarDbError::DataRetrievalError(format!(
                "Start time '{}' cannot be after end time '{}'.",
                start_time, end_time
            )));
        };

        let min_value = min_value.unwrap_or(Value::NEG_INFINITY);
        let max_value = max_value.unwrap_or(Value::INFINITY);

        if min_value > max_value {
            return Err(ModelarDbError::DataRetrievalError(format!(
                "Min value '{}' cannot be larger than max value '{}'.",
                min_value, max_value
            )));
        };

        // List the files storing the compressed data for the table with table_name.
        let compressed_files = self
            .compressed_data_manager
            .save_and_get_saved_compressed_files(table_name, query_data_folder)
            .await?;

        // TODO: prune by time and value in save_and_get_saved_compressed_files to perform the action async?
        // Prune files based on the time and value range each file contains compressed segments for.
        let pruned_compressed_files = compressed_files
            .into_iter()
            .filter(|object_meta| {
                let last_file_part = object_meta.location.parts().last().unwrap();
                compressed_data_manager::is_compressed_file_within_time_and_value_range(
                    last_file_part.as_ref(),
                    start_time,
                    end_time,
                    min_value,
                    max_value,
                )
            })
            .collect();

        Ok(pruned_compressed_files)
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
            let mut writer = create_apache_arrow_writer(file, batch.schema())?;
            writer.write(&batch)?;
            writer.close()?;

            Ok(())
        } else {
            Err(error)
        }
    }

    /// Read all rows from the Apache Parquet file at the location given by `file_path` and return
    /// them as a [`RecordBatch`]. If the file could not be read successfully, [`ParquetError`] is
    /// returned.
    pub fn read_batch_from_apache_parquet_file(
        file_path: &Path,
    ) -> Result<RecordBatch, ParquetError> {
        let error = ParquetError::General(format!(
            "Apache Parquet file at path '{}' could not be read.",
            file_path.display()
        ));

        // Create a reader that can be used to read an Apache Parquet file.
        let file = File::open(file_path).map_err(|_e| error.clone())?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.with_batch_size(usize::MAX).build()?;

        let record_batch = reader.next().ok_or(error)??;
        Ok(record_batch)
    }

    /// Return [`true`] if `file_path` is a readable Apache Parquet file, otherwise [`false`].
    pub async fn is_path_an_apache_parquet_file(
        object_store: &Arc<dyn ObjectStore>,
        file_path: &ObjectStorePath,
    ) -> bool {
        if let Ok(bytes) = object_store.get_range(file_path, 0..4).await {
            bytes == APACHE_PARQUET_FILE_SIGNATURE
        } else {
            false
        }
    }
}

/// Create an Apache ArrowWriter that writes to `writer`. If the writer could not be created return
/// [`ParquetError`].
pub(self) fn create_apache_arrow_writer<W: Write>(
    writer: W,
    schema: SchemaRef,
) -> Result<ArrowWriter<W>, ParquetError> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();

    let writer = ArrowWriter::try_new(writer, schema, Some(props))?;
    Ok(writer)
}

/// Create a file name that includes the start timestamp of the first segment in `batch`, the end
/// timestamp of the last segment in `batch`, the minimum value stored in `batch`, and the maximum
/// value stored in `batch`.
pub(self) fn create_time_and_value_range_file_name(batch: &RecordBatch) -> String {
    let start_times = get_array!(batch, 2, TimestampArray);
    let end_times = get_array!(batch, 3, TimestampArray);

    let min_values = get_array!(batch, 5, ValueArray);
    let max_values = get_array!(batch, 6, ValueArray);

    // unwrap() is safe as None is only returned if all of the values are None.
    let min_value = aggregate::min(min_values).unwrap();
    let max_value = aggregate::max(max_values).unwrap();

    format!(
        "{}-{}-{}-{}.parquet",
        start_times.value(0),
        end_times.value(end_times.len() - 1),
        min_value,
        max_value
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::Arc;

    use datafusion::arrow::compute;
    use datafusion::arrow::datatypes::Schema;
    use object_store::local::LocalFileSystem;
    use tempfile::{tempdir, TempDir};

    use crate::get_array;
    use crate::metadata::test_util as metadata_test_util;
    use crate::types::TimestampArray;

    const TABLE_NAME: &str = "table";

    // Tests for get_compressed_files().
    #[tokio::test]
    async fn test_can_get_compressed_file_for_table() {
        let (temp_dir, mut storage_engine) = create_storage_engine();

        // Insert compressed segments into the same table.
        let segment = test_util::get_compressed_segments_record_batch();
        storage_engine
            .compressed_data_manager
            .insert_compressed_segments(TABLE_NAME, segment.clone())
            .unwrap();
        storage_engine
            .compressed_data_manager
            .insert_compressed_segments(TABLE_NAME, segment)
            .unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = storage_engine
            .get_compressed_files(TABLE_NAME, None, None, None, None, &object_store)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_get_no_compressed_files_for_non_existent_table() {
        let (temp_dir, mut storage_engine) = create_storage_engine();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result =
            storage_engine.get_compressed_files(TABLE_NAME, None, None, None, None, &object_store);

        assert!(result.await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_can_get_compressed_file_with_start_time() {
        let (temp_dir, mut storage_engine) = create_storage_engine();
        let (segment_1, _segment_2) = insert_separated_segments(&mut storage_engine, 0, 0.0);

        // If we have a start time after the first segments ends, only the file containing the
        // second segment should be retrieved.
        let end_times = get_array!(segment_1, 3, TimestampArray);
        let start_time = Some(end_times.value(end_times.len() - 1) + 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = storage_engine.get_compressed_files(
            TABLE_NAME,
            start_time,
            None,
            None,
            None,
            &object_store,
        );
        let files = result.await.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the table name.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!("compressed/{}/2000-2005-15.2-44.2.parquet", TABLE_NAME)
        );
    }

    #[tokio::test]
    async fn test_can_get_compressed_file_with_min_value() {
        let (temp_dir, mut storage_engine) = create_storage_engine();
        let (segment_1, _segment_2) = insert_separated_segments(&mut storage_engine, 0, 0.0);

        // If we have a min value higher then the max value in the first segment, only the file
        // containing the second segment should be retrieved.
        let max_values = get_array!(segment_1, 6, ValueArray);
        let min_value = Some(compute::max(max_values).unwrap() + 1.0);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = storage_engine.get_compressed_files(
            TABLE_NAME,
            None,
            None,
            min_value,
            None,
            &object_store,
        );
        let files = result.await.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the table name.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!("compressed/{}/2000-2005-15.2-44.2.parquet", TABLE_NAME)
        );
    }

    #[tokio::test]
    async fn test_can_get_compressed_file_with_end_time() {
        let (temp_dir, mut storage_engine) = create_storage_engine();
        let (_segment_1, segment_2) = insert_separated_segments(&mut storage_engine, 0, 0.0);

        // If we have an end time before the second segment starts, only the file containing the
        // first segment should be retrieved.
        let start_times = get_array!(segment_2, 2, TimestampArray);
        let end_time = Some(start_times.value(1) - 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = storage_engine.get_compressed_files(
            TABLE_NAME,
            None,
            end_time,
            None,
            None,
            &object_store,
        );
        let files = result.await.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the table name.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!("compressed/{}/1000-1005-5.2-34.2.parquet", TABLE_NAME)
        );
    }

    #[tokio::test]
    async fn test_can_get_compressed_file_with_max_value() {
        let (temp_dir, mut storage_engine) = create_storage_engine();
        let (_segment_1, segment_2) = insert_separated_segments(&mut storage_engine, 0, 0.0);

        // If we have a max value lower then the min value in the second segment, only the file
        // containing the first segment should be retrieved.
        let min_values = get_array!(segment_2, 5, ValueArray);
        let max_value = Some(compute::min(min_values).unwrap() - 1.0);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = storage_engine.get_compressed_files(
            TABLE_NAME,
            None,
            None,
            None,
            max_value,
            &object_store,
        );
        let files = result.await.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the table name.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!("compressed/{}/1000-1005-5.2-34.2.parquet", TABLE_NAME)
        );
    }

    #[tokio::test]
    async fn test_can_get_compressed_files_with_start_time_and_end_time() {
        let (temp_dir, mut storage_engine) = create_storage_engine();

        // Insert 4 segments with a ~1 second time difference between segment 1 and 2 and segment 3 and 4.
        let (segment_1, _segment_2) = insert_separated_segments(&mut storage_engine, 0, 0.0);
        let (_segment_3, segment_4) = insert_separated_segments(&mut storage_engine, 1000, 0.0);

        // If we have a start time after the first segment and an end time before the fourth
        // segment, only the files containing the second and third segment should be retrieved.
        let end_times = get_array!(segment_1, 3, TimestampArray);
        let start_times = get_array!(segment_4, 2, TimestampArray);

        let start_time = Some(end_times.value(end_times.len() - 1) + 100);
        let end_time = Some(start_times.value(1) - 100);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = storage_engine.get_compressed_files(
            TABLE_NAME,
            start_time,
            end_time,
            None,
            None,
            &object_store,
        );
        let mut files = result.await.unwrap();
        assert_eq!(files.len(), 2);

        // Sort files as get_compressed_files() does not guarantee an ordering.
        files.sort_unstable_by(|a, b| a.location.cmp(&b.location));

        // The path to the returned compressed file should contain the table name.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!("compressed/{}/2000-2005-15.2-44.2.parquet", TABLE_NAME)
        );
        let file_path = files.get(1).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!("compressed/{}/2000-2005-5.2-34.2.parquet", TABLE_NAME)
        );
    }

    #[tokio::test]
    async fn test_can_get_compressed_files_with_min_value_and_max_value() {
        let (temp_dir, mut storage_engine) = create_storage_engine();

        // Insert 4 segments with a ~1 second time difference between segment 1 and 2 and segment 3 and 4.
        let (segment_1, _segment_2) = insert_separated_segments(&mut storage_engine, 0, 0.0);
        let (_segment_3, segment_4) = insert_separated_segments(&mut storage_engine, 1000, 100.0);

        // If we have a min value higher the first segment and a max value lower than the fourth
        // segment, only the files containing the second and third segment should be retrieved.
        let max_values = get_array!(segment_1, 6, ValueArray);
        let min_values = get_array!(segment_4, 5, ValueArray);

        let min_value = Some(compute::max(max_values).unwrap() + 1.0);
        let max_value = Some(compute::min(min_values).unwrap() - 1.0);

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = storage_engine.get_compressed_files(
            TABLE_NAME,
            None,
            None,
            min_value,
            max_value,
            &object_store,
        );
        let mut files = result.await.unwrap();
        assert_eq!(files.len(), 2);

        // Sort files as get_compressed_files() does not guarantee an ordering.
        files.sort_unstable_by(|a, b| a.location.cmp(&b.location));

        // The path to the returned compressed file should contain the table name.
        let file_path = files.get(0).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!("compressed/{}/2000-2005-105.2-134.2.parquet", TABLE_NAME)
        );
        let file_path = files.get(1).unwrap().location.to_string();
        assert_eq!(
            file_path,
            format!("compressed/{}/2000-2005-15.2-44.2.parquet", TABLE_NAME)
        );
    }

    /// Create and insert two compressed segments with a 1 second time difference offset by
    /// `start_time` and values with difference of 10 offset by `value_offset`.
    fn insert_separated_segments(
        storage_engine: &mut StorageEngine,
        start_time: i64,
        value_offset: f32,
    ) -> (RecordBatch, RecordBatch) {
        let segment_1 = test_util::get_compressed_segments_record_batch_with_time(
            1000 + start_time,
            value_offset,
        );
        storage_engine
            .compressed_data_manager
            .insert_compressed_segments(TABLE_NAME, segment_1.clone())
            .unwrap();
        storage_engine.compressed_data_manager.flush().unwrap();

        let segment_2 = test_util::get_compressed_segments_record_batch_with_time(
            2000 + start_time,
            10.0 + value_offset,
        );
        storage_engine
            .compressed_data_manager
            .insert_compressed_segments(TABLE_NAME, segment_2.clone())
            .unwrap();
        storage_engine.compressed_data_manager.flush().unwrap();

        (segment_1, segment_2)
    }

    #[tokio::test]
    async fn test_cannot_get_compressed_files_where_end_time_is_before_start_time() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        let segment = test_util::get_compressed_segments_record_batch();
        storage_engine
            .compressed_data_manager
            .insert_compressed_segments(TABLE_NAME, segment)
            .unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let result = storage_engine.get_compressed_files(
            TABLE_NAME,
            Some(10),
            Some(1),
            None,
            None,
            &object_store,
        );

        assert!(result
            .await
            .unwrap_err()
            .to_string()
            .contains("cannot be after end time"));
    }

    #[tokio::test]
    async fn test_cannot_get_compressed_files_where_max_value_is_smaller_than_min_value() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        let segment = test_util::get_compressed_segments_record_batch();
        storage_engine
            .compressed_data_manager
            .insert_compressed_segments(TABLE_NAME, segment)
            .unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let result = storage_engine.get_compressed_files(
            TABLE_NAME,
            None,
            None,
            Some(10.0),
            Some(1.0),
            &object_store,
        );

        assert!(result
            .await
            .unwrap_err()
            .to_string()
            .contains("cannot be larger than max value"));
    }

    /// Create a [`StorageEngine`] with a folder that is deleted once the test is finished.
    fn create_storage_engine() -> (TempDir, StorageEngine) {
        let temp_dir = tempdir().unwrap();
        let local_data_folder = temp_dir.path().to_path_buf();
        let metadata_manager =
            metadata_test_util::get_test_metadata_manager(local_data_folder.as_path());

        (
            temp_dir,
            StorageEngine::new(None, local_data_folder, metadata_manager, false),
        )
    }

    // Tests for writing and reading Apache Parquet files.
    #[test]
    fn test_write_batch_to_apache_parquet_file() {
        let temp_dir = tempdir().unwrap();
        let batch = test_util::get_compressed_segments_record_batch();

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
        let batch = test_util::get_compressed_segments_record_batch();

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

        let result = StorageEngine::read_batch_from_apache_parquet_file(path.as_path());

        assert!(result.is_ok());
        assert_eq!(batch, result.unwrap());
    }

    #[test]
    fn test_read_from_non_apache_parquet_file() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.txt");
        File::create(path.clone()).unwrap();

        let result = StorageEngine::read_batch_from_apache_parquet_file(path.as_path());

        assert!(result.is_err());
    }

    #[test]
    fn test_read_from_non_existent_path() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("none.parquet");
        let result = StorageEngine::read_batch_from_apache_parquet_file(path.as_path());

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
        let batch = test_util::get_compressed_segments_record_batch();

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
        let path = temp_dir.path().join("test.parquet");
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
/// Separate module for compressed segments utility functions since they are needed to test multiple
/// modules in the storage engine.
pub mod test_util {
    use std::sync::Arc;

    use datafusion::arrow::array::{BinaryArray, Float32Array, UInt64Array, UInt8Array};
    use datafusion::arrow::record_batch::RecordBatch;

    use crate::metadata::test_util as metadata_test_util;
    use crate::types::{TimestampArray, ValueArray};

    pub const COMPRESSED_SEGMENTS_SIZE: usize = 2424;

    /// Return a [`RecordBatch`] containing three compressed segments.
    pub fn get_compressed_segments_record_batch() -> RecordBatch {
        get_compressed_segments_record_batch_with_time(0, 0.0)
    }

    /// Return a [`RecordBatch`] containing three compressed segments. The compressed segments time
    /// range is from `time_ms` to `time_ms` + 3, while the value range is from `offset` + 5.2 to
    /// `offset` + 34.2.
    pub fn get_compressed_segments_record_batch_with_time(
        time_ms: i64,
        offset: f32,
    ) -> RecordBatch {
        let start_times = vec![time_ms, time_ms + 2, time_ms + 4];
        let end_times = vec![time_ms + 1, time_ms + 3, time_ms + 5];
        let min_values = vec![offset + 5.2, offset + 10.3, offset + 30.2];
        let max_values = vec![offset + 20.2, offset + 12.2, offset + 34.2];

        let univariate_id = UInt64Array::from(vec![1, 2, 3]);
        let model_type_id = UInt8Array::from(vec![2, 3, 3]);
        let start_time = TimestampArray::from(start_times);
        let end_time = TimestampArray::from(end_times);
        let timestamps = BinaryArray::from_vec(vec![b"000", b"001", b"010"]);
        let min_value = ValueArray::from(min_values);
        let max_value = ValueArray::from(max_values);
        let values = BinaryArray::from_vec(vec![b"1111", b"1000", b"0000"]);
        let error = Float32Array::from(vec![0.2, 0.5, 0.1]);

        let schema = metadata_test_util::get_compressed_schema();

        RecordBatch::try_new(
            schema.0,
            vec![
                Arc::new(univariate_id),
                Arc::new(model_type_id),
                Arc::new(start_time),
                Arc::new(end_time),
                Arc::new(timestamps),
                Arc::new(min_value),
                Arc::new(max_value),
                Arc::new(values),
                Arc::new(error),
            ],
        )
        .unwrap()
    }
}
