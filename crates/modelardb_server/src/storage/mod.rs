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

use std::ffi::OsStr;
use std::fs::File;
use std::io::{Error as IOError, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fmt, mem};

use bytes::buf::BufMut;
use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::compute;
use datafusion::arrow::compute::kernels::aggregate;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::ZstdLevel;
use datafusion::parquet::basic::{Compression, Encoding};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::format::SortingColumn;
use futures::StreamExt;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::types::{Timestamp, TimestampArray, Value, ValueArray};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use ringbuf::{HeapRb, Rb};
use tokio::fs::File as TokioFile;
use tokio::sync::RwLock;
use tonic::codegen::Bytes;
use tonic::Status;
use tracing::debug;
use uuid::{uuid, Uuid};

use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::metadata::MetadataManager;
use crate::storage::compressed_data_manager::CompressedDataManager;
use crate::storage::data_transfer::DataTransfer;
use crate::storage::uncompressed_data_buffer::UncompressedDataBuffer;
use crate::storage::uncompressed_data_manager::UncompressedDataManager;
use crate::PORT;

/// The folder storing uncompressed data in the data folders.
pub const UNCOMPRESSED_DATA_FOLDER: &str = "uncompressed";

/// The folder storing compressed data in the data folders.
pub const COMPRESSED_DATA_FOLDER: &str = "compressed";

/// The scheme with host at which the query data folder is stored.
pub const QUERY_DATA_FOLDER_SCHEME_WITH_HOST: &str = "query://query";

/// A static UUID for use in tests. It is not in a test utilities module so it can be used both
/// inside the if cfg(test) block of [`create_time_and_value_range_file_name`] and in test modules.
pub const TEST_UUID: Uuid = uuid!("44c57d06-333c-4935-8ae3-ed7bc53a08c4");

/// The expected [first four bytes of any Apache Parquet file].
///
/// [first four bytes of any Apache Parquet file]: https://en.wikipedia.org/wiki/List_of_file_signatures
const APACHE_PARQUET_FILE_SIGNATURE: &[u8] = &[80, 65, 82, 49]; // PAR1.

/// The capacity of each uncompressed data buffer as the number of elements in the buffer where each
/// element is a [`Timestamp`] and a [`Value`](use crate::types::Value). Note that the resulting
/// size of the buffer has to be a multiple of 64 bytes to avoid the actual capacity being larger
/// than the requested due to internal alignment when allocating memory for the two array builders.
const UNCOMPRESSED_DATA_BUFFER_CAPACITY: usize = 64 * 1024;

/// The number of bytes that are required before transferring a batch of data to the remote object store.
const TRANSFER_BATCH_SIZE_IN_BYTES: usize = 64 * 1024 * 1024; // 64 MiB;

/// Manages all uncompressed and compressed data, both while being stored in memory during ingestion
/// and when persisted to disk afterwards.
pub struct StorageEngine {
    // TODO: is it better to use MetadataManager from context to share caches with tables.rs and
    // uncompressed_data_manager.rs or a separate MetadataManager to not require taking a lock?
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
            &metadata_manager,
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
                    TRANSFER_BATCH_SIZE_IN_BYTES,
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
        model_table_metadata: &ModelTableMetadata,
        data_points: &RecordBatch,
    ) -> Result<(), String> {
        // TODO: When the compression component is changed, just insert the data points.
        let compressed_segments = self
            .uncompressed_data_manager
            .insert_data_points(
                &mut self.metadata_manager,
                model_table_metadata,
                data_points,
            )
            .await?;

        for (univariate_id, compressed_segments) in compressed_segments {
            let column_index = MetadataManager::univariate_id_to_column_index(univariate_id);
            self.compressed_data_manager
                .insert_compressed_segments(
                    &model_table_metadata.name,
                    column_index,
                    compressed_segments,
                )
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
        let compressed_segments = self
            .uncompressed_data_manager
            .flush()
            .await
            .map_err(|error| error.to_string())?;
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
    /// table with `table_name` within the given range of time and value. If some compressed data
    /// that belongs to `column_index` in `table_name` is still in memory, save it to disk first. If
    /// no files belong to the column at `column_index` for the table with `table_name` an empty
    /// [`Vec`] is returned, while a [`DataRetrievalError`](ModelarDbError::DataRetrievalError) is
    /// returned if:
    /// * A table with `table_name` does not exist.
    /// * A column with `column_index` does not exist.
    /// * The compressed files could not be listed.
    /// * The end time is before the start time.
    /// * The max value is smaller than the min value.
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
        // Retrieve object_metas that represent the relevant files for table_name and column_index.
        let relevant_apache_parquet_files = self
            .compressed_data_manager
            .save_and_get_saved_compressed_files(
                table_name,
                column_index,
                start_time,
                end_time,
                min_value,
                max_value,
                query_data_folder,
            )
            .await?;

        // Merge the compressed Apache Parquet files if multiple are returned to ensure order.
        if relevant_apache_parquet_files.len() > 1 {
            let object_meta = StorageEngine::merge_compressed_apache_parquet_files(
                query_data_folder,
                &relevant_apache_parquet_files,
                query_data_folder,
                &format!("{COMPRESSED_DATA_FOLDER}/{table_name}/{column_index}"),
            )
            .await
            .map_err(|error| {
                ModelarDbError::DataRetrievalError(format!(
                    "Compressed data could not be merged for column '{column_index}' in table '{table_name}': {error}"
                ))
            })?;

            Ok(vec![object_meta])
        } else {
            Ok(relevant_apache_parquet_files)
        }
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

    /// Update the remote data folder, used to transfer data to in the data transfer component.
    /// If one does not already exists, create a new data transfer component. If the remote
    /// data folder was successfully updated, return [`Ok`], otherwise return [`IOError`].
    pub async fn update_remote_data_folder(
        &mut self,
        remote_data_folder: Arc<dyn ObjectStore>,
    ) -> Result<(), IOError> {
        if let Some(data_transfer) = &mut self.compressed_data_manager.data_transfer {
            data_transfer.remote_data_folder = remote_data_folder;
        } else {
            let data_transfer = DataTransfer::try_new(
                self.compressed_data_manager.local_data_folder.clone(),
                remote_data_folder,
                TRANSFER_BATCH_SIZE_IN_BYTES,
                self.compressed_data_manager.used_disk_space_metric.clone(),
            )
            .await?;

            self.compressed_data_manager.data_transfer = Some(data_transfer);
        }

        Ok(())
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
        // Create a stream that can be used to read an Apache Parquet file.
        let file = TokioFile::open(file_path)
            .await
            .map_err(|error| ParquetError::General(error.to_string()))?;
        let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
        let mut stream = builder.with_batch_size(usize::MAX).build()?;

        let record_batch = stream.next().await.ok_or_else(|| {
            ParquetError::General(format!(
                "Apache Parquet file at path '{}' could not be read.",
                file_path.display()
            ))
        })??;
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

        // Merge the record batches into a single concatenated and merged record batch.
        let schema = record_batches[0].schema();
        let concatenated = compute::concat_batches(&schema, &record_batches)?;
        let merged = modelardb_compression::try_merge_segments(concatenated)
            .map_err(|error| ParquetError::General(error.to_string()))?;

        // Compute the name of the output file based on data in merged.
        let file_name = create_time_and_value_range_file_name(&merged);
        let output_file_path = format!("{output_folder}/{file_name}").into();

        // Specify that the file must be sorted by univariate_id and then by start_time.
        let sorting_columns = Some(vec![
            SortingColumn::new(0, false, false),
            SortingColumn::new(2, false, false),
        ]);

        // Write the concatenated and merged record batch to the output location.
        let mut buf = vec![].writer();
        let mut apache_arrow_writer =
            create_apache_arrow_writer(&mut buf, schema, sorting_columns)?;
        apache_arrow_writer.write(&merged)?;
        apache_arrow_writer.close()?;

        output_data_folder
            .put(&output_file_path, Bytes::from(buf.into_inner()))
            .await
            .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

        // Delete the input files as the output file has been written.
        for input_file in input_files {
            input_data_folder
                .delete(&input_file.location)
                .await
                .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;
        }

        debug!(
            "Merged {} compressed files into single Apache Parquet file with {} rows.",
            input_files.len(),
            merged.num_rows()
        );

        // Return an ObjectMeta that represent the successfully merged and written file.
        // unwrap() is safe as the file have just been written successfully to output_file_path.
        output_data_folder
            .head(&output_file_path)
            .await
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
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .set_sorting_columns(sorting_columns)
        .build();

    let writer = ArrowWriter::try_new(writer, schema, Some(props))?;
    Ok(writer)
}

/// Create a file name that includes the start timestamp of the first segment in `batch`, the end
/// timestamp of the last segment in `batch`, the minimum value stored in `batch`, the maximum value
/// stored in `batch`, an UUID to make it unique across edge and cloud in practice, and an ID that
/// uniquely identifies the edge.
pub(self) fn create_time_and_value_range_file_name(batch: &RecordBatch) -> String {
    // unwrap() is safe as None is only returned if all of the values are None.
    let start_time = aggregate::min(modelardb_common::array!(batch, 2, TimestampArray)).unwrap();
    let end_time = aggregate::max(modelardb_common::array!(batch, 3, TimestampArray)).unwrap();

    // unwrap() is safe as None is only returned if all of the values are None.
    // Both aggregate::min() and aggregate::max() consider NaN to be greater than other non-null
    // values. So since min_values and max_values cannot contain null, min_value will be NaN if all
    // values in min_values are NaN while max_value will be NaN if any value in max_values is NaN.
    let min_value = aggregate::min(modelardb_common::array!(batch, 5, ValueArray)).unwrap();
    let max_value = aggregate::max(modelardb_common::array!(batch, 6, ValueArray)).unwrap();

    // An UUID is added to the file name to ensure it, in practice, is unique across edge and cloud.
    // A static UUID is set when tests are executed to allow the tests to check that files exists.
    let uuid = if cfg!(test) {
        TEST_UUID
    } else {
        Uuid::new_v4()
    };

    // TODO: Use part of the UUID or the entire Apache Arrow Flight URL to identify the edge.
    let edge_id = PORT.to_string();

    format!(
        "{}_{}_{}_{}_{}_{}.parquet",
        start_time, end_time, min_value, max_value, uuid, edge_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{Field, Schema};
    use object_store::local::LocalFileSystem;
    use tempfile::{self, TempDir};

    use crate::common_test;

    // Tests for writing and reading Apache Parquet files.
    #[test]
    fn test_write_batch_to_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let batch = common_test::compressed_segments_record_batch();

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
        let fields: Vec<Field> = vec![];
        let schema = Schema::new(fields);
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
        let batch = common_test::compressed_segments_record_batch();

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
        let batch = common_test::compressed_segments_record_batch();

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
