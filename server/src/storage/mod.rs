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
//! for a query can be retrieved by the query engine using [`StorageEngine::compressed_files()`].

mod compressed_data_buffer;
mod compressed_data_manager;
mod data_transfer;
mod uncompressed_data_buffer;
mod uncompressed_data_manager;

use datafusion::arrow::array::UInt32Array;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Error as IOError, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fmt, mem};

use bytes::buf::BufMut;
use datafusion::arrow::compute;
use datafusion::arrow::compute::kernels::aggregate;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, Encoding};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::format::SortingColumn;
use futures::StreamExt;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use ringbuf::{HeapRb, Rb};
use tokio::fs::File as TokioFile;
use tokio::sync::RwLock;
use tonic::codegen::Bytes;
use tonic::Status;
use tracing::debug;

use crate::array;
use crate::errors::ModelarDbError;
use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::metadata::MetadataManager;
use crate::storage::compressed_data_manager::CompressedDataManager;
use crate::storage::data_transfer::DataTransfer;
use crate::storage::uncompressed_data_buffer::UncompressedDataBuffer;
use crate::storage::uncompressed_data_manager::UncompressedDataManager;
use crate::types::{Timestamp, TimestampArray, Value, ValueArray};

/// The folder storing uncompressed data in the data folders.
pub const UNCOMPRESSED_DATA_FOLDER: &str = "uncompressed";

/// The folder storing compressed data in the data folders.
pub const COMPRESSED_DATA_FOLDER: &str = "compressed";

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
    /// Return [`StorageEngine`] that writes ingested data to `local_data_folder` and optionally
    /// transfers compressed data to `remote_data_folder` if it is given. Returns [`String`] if
    /// `remote_data_folder` is given but [`DataTransfer`] cannot not be created.
    pub async fn try_new(
        local_data_folder: PathBuf,
        remote_data_folder: Option<Arc<dyn ObjectStore>>,
        metadata_manager: MetadataManager,
        compress_directly: bool,
    ) -> Result<Self, IOError> {
        // Create a metric for used disk space. The metric is wrapped in an Arc and a read/write lock
        // since it is appended to by multiple components, potentially at the same time.
        let used_disk_space_metric = Arc::new(RwLock::new(Metric::new()));

        // Create the uncompressed data manager.
        let uncompressed_data_manager = UncompressedDataManager::try_new(
            local_data_folder.clone(),
            metadata_manager.uncompressed_reserved_memory_in_bytes,
            metadata_manager.uncompressed_schema(),
            metadata_manager.compressed_schema(),
            compress_directly,
            used_disk_space_metric.clone(),
        )
        .await?;

        // Create the compressed data manager.
        let data_transfer = if let Some(remote_data_folder) = remote_data_folder {
            Some(
                DataTransfer::try_new(
                    local_data_folder.clone(),
                    remote_data_folder,
                    64 * 1024 * 1024, // 64 MiB.
                    used_disk_space_metric.clone(),
                )
                .await?,
            )
        } else {
            None
        };

        let compressed_data_manager = CompressedDataManager::try_new(
            data_transfer,
            local_data_folder,
            metadata_manager.compressed_reserved_memory_in_bytes,
            metadata_manager.compressed_schema(),
            used_disk_space_metric,
        )?;

        Ok(Self {
            metadata_manager,
            uncompressed_data_manager,
            compressed_data_manager,
        })
    }

    /// Pass `record_batch` to [`CompressedDataManager`]. Return [`Ok`] if `record_batch` was
    /// successfully written to an Apache Parquet file, otherwise return [`Err`].
    pub(super) async fn insert_record_batch(
        &self,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> Result<(), ParquetError> {
        self.compressed_data_manager
            .insert_record_batch(table_name, record_batch)
            .await
    }

    /// Pass `data_points` to [`UncompressedDataManager`]. Return [`Ok`] if all of the data points
    /// were successfully inserted, otherwise return [`String`].
    pub async fn insert_data_points(
        &mut self,
        model_table: &ModelTableMetadata,
        data_points: &RecordBatch,
    ) -> Result<(), String> {
        // TODO: When the compression component is changed, just insert the data points.
        let compressed_segments = self
            .uncompressed_data_manager
            .insert_data_points(&mut self.metadata_manager, model_table, data_points)
            .await?;

        for (univariate_id, compressed_segments) in compressed_segments {
            let column_index = MetadataManager::univariate_id_to_column_index(univariate_id);
            self.compressed_data_manager
                .insert_compressed_segments(&model_table.name, column_index, compressed_segments)
                .await
                .map_err(|error| error.to_string())?;
        }

        Ok(())
    }

    /// Retrieve the oldest finished [`UncompressedDataBuffer`] from [`UncompressedDataManager`] and
    /// return it. Return [`None`] if there are no finished
    /// [`UncompressedDataBuffers`](UncompressedDataBuffer).
    pub async fn finished_uncompressed_data_buffer(
        &mut self,
    ) -> Option<Box<dyn UncompressedDataBuffer>> {
        self.uncompressed_data_manager.finished_data_buffer().await
    }

    /// Flush all of the data the [`StorageEngine`] is currently storing in memory to disk. If all
    /// of the data is successfully flushed to disk, return [`Ok`], otherwise return [`String`].
    pub async fn flush(&mut self) -> Result<(), String> {
        // TODO: When the compression component is changed, just flush before managers.
        // Flush UncompressedDataManager.
        let compressed_segments = self.uncompressed_data_manager.flush().await;
        let hash_to_table_name = self
            .metadata_manager
            .mapping_from_hash_to_table_name()
            .map_err(|error| error.to_string())?;

        for (univariate_id, compressed_segments) in compressed_segments {
            let tag_hash = MetadataManager::univariate_id_to_tag_hash(univariate_id);
            let column_index = MetadataManager::univariate_id_to_column_index(univariate_id);

            // unwrap() is safe as new univariate ids have been added to the metadata database.
            let table_name = hash_to_table_name.get(&tag_hash).unwrap();
            self.compressed_data_manager
                .insert_compressed_segments(table_name, column_index, compressed_segments)
                .await
                .map_err(|error| error.to_string())?;
        }

        // Flush CompressedDataManager.
        self.compressed_data_manager
            .flush()
            .await
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

    /// Retrieve the compressed sorted files that correspond to the column at `column_index` in the
    /// table with `table_name` within the given range of time and value. If a column at
    /// `column_index` in table with `table_name` does not exist, the end time is before the start
    /// time, or the max value is larger than the min value,
    /// [`DataRetrievalError`](ModelarDbError::DataRetrievalError) is returned.
    #[allow(clippy::too_many_arguments)]
    pub async fn compressed_files(
        &mut self,
        table_name: &str,
        column_index: u16,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        min_value: Option<Value>,
        max_value: Option<Value>,
        query_data_folder: &Arc<dyn ObjectStore>,
    ) -> Result<Vec<ObjectMeta>, ModelarDbError> {
        self.compressed_data_manager
            .save_merge_and_get_saved_compressed_files(
                table_name,
                column_index,
                start_time,
                end_time,
                min_value,
                max_value,
                query_data_folder,
            )
            .await
    }

    /// Collect and return the metrics of used uncompressed/compressed memory, used disk space, and ingested
    /// data points over time. The metrics are returned in tuples with the format (metric_type, (timestamps, values)).
    pub async fn collect_metrics(&mut self) -> Vec<(MetricType, (TimestampArray, UInt32Array))> {
        vec![
            (
                MetricType::UsedUncompressedMemory,
                self.uncompressed_data_manager
                    .used_uncompressed_memory_metric
                    .finish(),
            ),
            (
                MetricType::UsedCompressedMemory,
                self.compressed_data_manager
                    .used_compressed_memory_metric
                    .finish(),
            ),
            (
                MetricType::IngestedDataPoints,
                self.uncompressed_data_manager
                    .ingested_data_points_metric
                    .finish(),
            ),
            (
                MetricType::UsedDiskSpace,
                self.compressed_data_manager
                    .used_disk_space_metric
                    .write()
                    .await
                    .finish(),
            ),
        ]
    }

    /// Write `batch` to an Apache Parquet file at the location given by `file_path`. `file_path`
    /// must use the extension '.parquet'. Return [`Ok`] if the file was written successfully,
    /// otherwise [`ParquetError`].
    pub fn write_batch_to_apache_parquet_file(
        batch: RecordBatch,
        file_path: &Path,
        sorting_columns: Option<Vec<SortingColumn>>,
    ) -> Result<(), ParquetError> {
        let error = ParquetError::General(format!(
            "Apache Parquet file at path '{}' could not be created.",
            file_path.display()
        ));

        // Check if the extension of the given path is correct.
        if file_path.extension().and_then(OsStr::to_str) == Some("parquet") {
            let file = File::create(file_path).map_err(|_e| error)?;
            let mut writer = create_apache_arrow_writer(file, batch.schema(), sorting_columns)?;
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
    pub async fn read_batch_from_apache_parquet_file(
        file_path: &Path,
    ) -> Result<RecordBatch, ParquetError> {
        let error = ParquetError::General(format!(
            "Apache Parquet file at path '{}' could not be read.",
            file_path.display()
        ));

        // Create a stream that can be used to read an Apache Parquet file.
        let file = TokioFile::open(file_path)
            .await
            .map_err(|_e| error.clone())?;
        let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
        let mut stream = builder.with_batch_size(usize::MAX).build()?;

        let record_batch = stream.next().await.ok_or(error)??;
        Ok(record_batch)
    }

    /// Merge the Apache Parquet files in `input_data_folder`/`input_files` and write them to a
    /// single Apache Parquet file in `output_data_folder`/`output_folder`. Return an [`ObjectMeta`]
    /// that represent the merged file if it is written successfully, otherwise [`ParquetError`] is
    /// returned.
    pub async fn merge_compressed_apache_parquet_files(
        input_data_folder: &Arc<dyn ObjectStore>,
        input_files: &[ObjectMeta],
        output_data_folder: &Arc<dyn ObjectStore>,
        output_folder: &str,
    ) -> Result<ObjectMeta, ParquetError> {
        // TODO: ensure termination at any place does not duplicate or loss data.
        // Read input files, for_each is not used so errors can be returned with ?.
        let mut record_batches = Vec::with_capacity(input_files.len());
        for input_file in input_files {
            let reader = ParquetObjectReader::new(input_data_folder.clone(), input_file.clone());
            let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
            let mut stream = builder.with_batch_size(usize::MAX).build()?;

            let record_batch = stream.next().await.ok_or_else(|| {
                ParquetError::General(format!(
                    "Apache Parquet file at path '{}' could not be read.",
                    input_file.location
                ))
            })??;

            record_batches.push(record_batch);
        }

        // Merge the record batches into a single combined record batches.
        let schema = record_batches[0].schema();
        let combined = compute::concat_batches(&schema, &record_batches)?;

        // Compute the name of the output file based on data in combined.
        let file_name = create_time_and_value_range_file_name(&combined);
        let output_file = format!("{output_folder}/{file_name}").into();

        // Specify that the file must be sorted by univariate_id and then by start_time.
        let sorting_columns = Some(vec![
            SortingColumn::new(0, false, false),
            SortingColumn::new(2, false, false),
        ]);

        // Delete the input files as the output file has been written.
        for input_file in input_files {
            input_data_folder
                .delete(&input_file.location)
                .await
                .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;
        }

        // Write the combined record batch to the output location.
        let mut buf = vec![].writer();
        let mut apache_arrow_writer =
            create_apache_arrow_writer(&mut buf, schema, sorting_columns)?;
        apache_arrow_writer.write(&combined)?;
        apache_arrow_writer.close()?;

        output_data_folder
            .put(&output_file, Bytes::from(buf.into_inner()))
            .await
            .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

        debug!(
            "Merged {} compressed files into single Apache Parquet file with {} rows.",
            input_files.len(),
            combined.num_rows()
        );

        // Return an ObjectMeta that represent the successfully merged and written file.
        // unwrap() is safe as the file have just been written successfully to the output_file path.
        let output_folder_path = ObjectStorePath::parse(output_folder).unwrap();
        output_data_folder
            .list(Some(&output_folder_path))
            .await
            .map_err(|error| ParquetError::General(error.to_string()))?
            .next()
            .await
            .unwrap()
            .map_err(|error| ParquetError::General(error.to_string()))
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

/// The different types of metrics that are collected in the storage engine.
pub enum MetricType {
    UsedUncompressedMemory,
    UsedCompressedMemory,
    IngestedDataPoints,
    UsedDiskSpace,
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UsedUncompressedMemory => write!(f, "used_uncompressed_memory"),
            Self::UsedCompressedMemory => write!(f, "used_compressed_memory"),
            Self::IngestedDataPoints => write!(f, "ingested_data_points"),
            Self::UsedDiskSpace => write!(f, "used_disk_space"),
        }
    }
}

/// Metric used to record changes in specific attributes in the storage engine. The timestamps
/// and values of the metric is stored in ring buffers to ensure the amount of memory used by the
/// metric is capped.
pub struct Metric {
    /// Ring buffer consisting of a capped amount of millisecond precision timestamps.
    timestamps: HeapRb<Timestamp>,
    /// Ring buffer consisting of a capped amount of values.
    values: HeapRb<u32>,
    /// Last saved metric value, used to support updating the metric based on a change to the last
    /// value instead of simply storing the new value. Since the values builder is cleared when the metric
    /// is finished, the last value is saved separately.
    last_value: isize,
}

impl Metric {
    fn new() -> Self {
        // The capacity of the timestamps and values ring buffers. This ensures that the total
        // memory used by the metric is capped to ~1 MiB.
        let capacity = (1024 * 1024) / (mem::size_of::<Timestamp>() + mem::size_of::<u32>());

        Self {
            timestamps: HeapRb::<Timestamp>::new(capacity),
            values: HeapRb::<u32>::new(capacity),
            last_value: 0,
        }
    }

    /// Add a new entry to the metric, where the timestamp is the current milliseconds since the Unix
    /// epoch and the value is either set directly or based on the last value in the metric.
    fn append(&mut self, value: isize, based_on_last: bool) {
        // unwrap() is safe since the Unix epoch is always earlier than now.
        let since_the_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let timestamp = since_the_epoch.as_millis() as Timestamp;

        let mut new_value = value;
        if based_on_last {
            new_value = self.last_value + value;
        }

        self.timestamps.push_overwrite(timestamp);
        self.values.push_overwrite(new_value as u32);
        self.last_value = new_value;
    }

    /// Finish and reset the internal ring buffers and return the timestamps and values as Apache Arrow arrays.
    fn finish(&mut self) -> (TimestampArray, UInt32Array) {
        let timestamps = TimestampArray::from_iter_values(self.timestamps.pop_iter());
        let values = UInt32Array::from_iter_values(self.values.pop_iter());

        (timestamps, values)
    }
}

/// Create an Apache ArrowWriter that writes to `writer`. If the writer could not be created return
/// [`ParquetError`].
pub(self) fn create_apache_arrow_writer<W: Write>(
    writer: W,
    schema: SchemaRef,
    sorting_columns: Option<Vec<SortingColumn>>,
) -> Result<ArrowWriter<W>, ParquetError> {
    let props = WriterProperties::builder()
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::ZSTD)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .set_sorting_columns(sorting_columns)
        .build();

    let writer = ArrowWriter::try_new(writer, schema, Some(props))?;
    Ok(writer)
}

/// Create a file name that includes the start timestamp of the first segment in `batch`, the end
/// timestamp of the last segment in `batch`, the minimum value stored in `batch`, and the maximum
/// value stored in `batch`.
pub(self) fn create_time_and_value_range_file_name(batch: &RecordBatch) -> String {
    let start_times = array!(batch, 2, TimestampArray);
    let end_times = array!(batch, 3, TimestampArray);

    let min_values = array!(batch, 5, ValueArray);
    let max_values = array!(batch, 6, ValueArray);

    // unwrap() is safe as None is only returned if all of the values are None.
    // Both aggregate::min() and aggregate::max() consider NaN to be greater than other non-null
    // values. So since min_values and max_values cannot contain null, min_value will be NaN if all
    // values in min_values are NaN while max_value will be NaN if any value in max_values is NaN.
    let min_value = aggregate::min(min_values).unwrap();
    let max_value = aggregate::max(max_values).unwrap();

    format!(
        "{}_{}_{}_{}.parquet",
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

    use datafusion::arrow::datatypes::Schema;
    use object_store::local::LocalFileSystem;
    use tempfile::{self, TempDir};

    // Tests for writing and reading Apache Parquet files.
    #[test]
    fn test_write_batch_to_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let batch = test_util::compressed_segments_record_batch();

        let apache_parquet_path = temp_dir.path().join("test.parquet");
        StorageEngine::write_batch_to_apache_parquet_file(
            batch,
            apache_parquet_path.as_path(),
            None,
        )
        .unwrap();

        assert!(apache_parquet_path.exists());
    }

    #[test]
    fn test_write_empty_batch_to_apache_parquet_file() {
        let schema = Schema::new(vec![]);
        let batch = RecordBatch::new_empty(Arc::new(schema));

        let temp_dir = tempfile::tempdir().unwrap();
        let apache_parquet_path = temp_dir.path().join("empty.parquet");
        StorageEngine::write_batch_to_apache_parquet_file(
            batch,
            apache_parquet_path.as_path(),
            None,
        )
        .unwrap();

        assert!(apache_parquet_path.exists());
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
        let temp_dir = tempfile::tempdir().unwrap();
        let batch = test_util::compressed_segments_record_batch();

        let apache_parquet_path = temp_dir.path().join(file_name);
        let result = StorageEngine::write_batch_to_apache_parquet_file(
            batch,
            apache_parquet_path.as_path(),
            None,
        );

        assert!(result.is_err());
        assert!(!apache_parquet_path.exists());
    }

    #[tokio::test]
    async fn test_read_entire_apache_parquet_file() {
        let file_name = "test.parquet".to_owned();
        let (_temp_dir, path, batch) = create_apache_parquet_file_in_temp_dir(file_name);

        let result = StorageEngine::read_batch_from_apache_parquet_file(path.as_path()).await;

        assert!(result.is_ok());
        assert_eq!(batch, result.unwrap());
    }

    #[tokio::test]
    async fn test_read_from_non_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.txt");
        File::create(path.clone()).unwrap();

        let result = StorageEngine::read_batch_from_apache_parquet_file(path.as_path());

        assert!(result.await.is_err());
    }

    #[tokio::test]
    async fn test_read_from_non_existent_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("none.parquet");
        let result = StorageEngine::read_batch_from_apache_parquet_file(path.as_path());

        assert!(result.await.is_err());
    }

    #[tokio::test]
    async fn test_is_apache_parquet_path_apache_parquet_file() {
        let file_name = "test.parquet".to_owned();
        let (_temp_dir, path, _batch) = create_apache_parquet_file_in_temp_dir(file_name);

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let object_store_path = ObjectStorePath::from_filesystem_path(path).unwrap();

        assert!(
            StorageEngine::is_path_an_apache_parquet_file(&object_store, &object_store_path).await
        );
    }

    /// Create an Apache Parquet file in the [`tempfile::TempDir`] from a generated [`RecordBatch`].
    fn create_apache_parquet_file_in_temp_dir(
        file_name: String,
    ) -> (TempDir, PathBuf, RecordBatch) {
        let temp_dir = tempfile::tempdir().unwrap();
        let batch = test_util::compressed_segments_record_batch();

        let apache_parquet_path = temp_dir.path().join(file_name);
        StorageEngine::write_batch_to_apache_parquet_file(
            batch.clone(),
            apache_parquet_path.as_path(),
            None,
        )
        .unwrap();

        (temp_dir, apache_parquet_path, batch)
    }

    #[tokio::test]
    async fn test_is_non_apache_parquet_path_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
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
    async fn test_is_empty_apache_parquet_path_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.parquet");
        File::create(path.clone()).unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let object_store_path = ObjectStorePath::from_filesystem_path(&path).unwrap();

        assert!(path.exists());
        assert!(
            !StorageEngine::is_path_an_apache_parquet_file(&object_store, &object_store_path).await
        );
    }

    // Tests for Metric.
    #[test]
    fn test_append_to_metric() {
        let mut metric = Metric::new();
        let since_the_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let timestamp = since_the_epoch.as_millis() as Timestamp;

        metric.append(30, false);
        metric.append(30, false);

        assert_eq!(metric.timestamps.pop_iter().last(), Some(timestamp));
        assert_eq!(metric.values.pop_iter().last(), Some(30));
    }

    #[test]
    fn test_append_positive_value_to_metric_based_on_last() {
        let mut metric = Metric::new();

        metric.append(30, true);
        metric.append(30, true);

        assert_eq!(metric.values.pop_iter().last(), Some(60));
    }

    #[test]
    fn test_append_negative_value_to_metric_based_on_last() {
        let mut metric = Metric::new();

        metric.append(30, true);
        metric.append(-30, true);

        assert_eq!(metric.values.pop_iter().last(), Some(0));
    }

    #[test]
    fn test_finish_metric() {
        let mut metric = Metric::new();

        metric.append(30, true);
        metric.append(-30, true);

        let (_timestamps, values) = metric.finish();
        assert_eq!(values.value(0), 30);
        assert_eq!(values.value(1), 0);

        // Ensure that the builders in the metric has been reset.
        assert_eq!(metric.timestamps.len(), 0);
        assert_eq!(metric.values.len(), 0);
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
    pub fn compressed_segments_record_batch() -> RecordBatch {
        compressed_segments_record_batch_with_time(0, 0.0)
    }

    /// Return a [`RecordBatch`] containing three compressed segments. The compressed segments time
    /// range is from `time_ms` to `time_ms` + 3, while the value range is from `offset` + 5.2 to
    /// `offset` + 34.2.
    pub fn compressed_segments_record_batch_with_time(time_ms: i64, offset: f32) -> RecordBatch {
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

        let schema = metadata_test_util::compressed_schema();

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
