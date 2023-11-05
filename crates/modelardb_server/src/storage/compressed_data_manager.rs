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

//! Support for managing all compressed data that is inserted into the [`StorageEngine`].

use std::fs;
use std::io::{Error as IOError, ErrorKind};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BufMut;
use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use futures::StreamExt;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::metadata::compressed_file::CompressedFile;
use modelardb_common::metadata::TableMetadataManager;
use modelardb_common::types::{Timestamp, Value};
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::format::SortingColumn;
use sqlx::Sqlite;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tonic::codegen::Bytes;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::storage::compressed_data_buffer::{CompressedDataBuffer, CompressedSegmentBatch};
use crate::storage::data_transfer::DataTransfer;
use crate::storage::types::Message;
use crate::storage::types::{Channels, MemoryPool};
use crate::storage::{Metric, StorageEngine, COMPRESSED_DATA_FOLDER};

/// Stores data points compressed as models in memory to batch compressed data before saving it to
/// Apache Parquet files.
pub(super) struct CompressedDataManager {
    /// Component that transfers saved compressed data to the remote data folder when it is necessary.
    pub(super) data_transfer: RwLock<Option<DataTransfer>>,
    /// Path to the folder containing all compressed data managed by the [`StorageEngine`].
    pub(crate) local_data_folder: PathBuf,
    /// The compressed segments before they are saved to persistent storage. The key is the name of
    /// the table and the index of the column the compressed segments represents data points for so
    /// the Apache Parquet files can be partitioned by table and then column.
    compressed_data_buffers: DashMap<(String, u16), CompressedDataBuffer>,
    /// FIFO queue of table names and column indices referring to [`CompressedDataBuffer`] that can
    /// be saved to persistent storage.
    compressed_queue: SegQueue<(String, u16)>,
    /// Channels used by the storage engine's threads to communicate.
    channels: Arc<Channels>,
    /// Management of metadata for saving compressed file metadata.
    table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
    /// Track how much memory is left for storing uncompressed and compressed data.
    memory_pool: Arc<MemoryPool>,
    /// Metric for the used compressed memory in bytes, updated every time the used memory changes.
    pub(super) used_compressed_memory_metric: Mutex<Metric>,
    /// Metric for the total used disk space in bytes, updated every time a new compressed file is
    /// saved to disk.
    pub(super) used_disk_space_metric: Arc<Mutex<Metric>>,
}

impl CompressedDataManager {
    /// Return a [`CompressedDataManager`] if the required folder can be created in
    /// `local_data_folder`, otherwise [`IOError`] is returned.
    pub(super) fn try_new(
        data_transfer: RwLock<Option<DataTransfer>>,
        local_data_folder: PathBuf,
        channels: Arc<Channels>,
        memory_pool: Arc<MemoryPool>,
        table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
        used_disk_space_metric: Arc<Mutex<Metric>>,
    ) -> Result<Self, IOError> {
        // Ensure the folder required by the compressed data manager exists.
        fs::create_dir_all(local_data_folder.join(COMPRESSED_DATA_FOLDER))?;

        Ok(Self {
            data_transfer,
            local_data_folder,
            compressed_data_buffers: DashMap::new(),
            compressed_queue: SegQueue::new(),
            channels,
            table_metadata_manager,
            memory_pool,
            used_compressed_memory_metric: Mutex::new(Metric::new()),
            used_disk_space_metric,
        })
    }

    /// Write `record_batch` to the table with `table_name` as a compressed Apache Parquet file.
    pub(super) async fn insert_record_batch(
        &self,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> Result<(), ParquetError> {
        debug!(
            "Received record batch with {} rows for the table '{}'.",
            record_batch.num_rows(),
            table_name
        );
        let local_file_path = self
            .local_data_folder
            .join(COMPRESSED_DATA_FOLDER)
            .join(table_name);

        // Create the folder structure if it does not already exist.
        fs::create_dir_all(local_file_path.as_path())?;

        // Create a path that uses the current timestamp as the filename.
        let since_the_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| ParquetError::General(error.to_string()))?;
        let file_name = format!("{}.parquet", since_the_epoch.as_millis());
        let file_path = local_file_path.join(file_name);

        StorageEngine::write_batch_to_apache_parquet_file(&record_batch, file_path.as_path(), None)
    }

    /// Read and process messages received from the
    /// [`UncompressedDataManager`](super::UncompressedDataManager) to either insert compressed
    /// data, flush buffers, or stop.
    pub(super) fn process_compressed_messages(&self, runtime: Arc<Runtime>) -> Result<(), String> {
        loop {
            let message = self
                .channels
                .compressed_data_receiver
                .recv()
                .map_err(|error| error.to_string())?;

            match message {
                Message::Data(compressed_segment_batch) => {
                    runtime
                        .block_on(self.insert_compressed_segments(compressed_segment_batch))
                        .map_err(|error| error.to_string())?;
                }
                Message::Flush => {
                    self.flush_and_log_errors(&runtime);
                    self.channels
                        .result_sender
                        .send(Ok(()))
                        .map_err(|error| error.to_string())?;
                }
                Message::Stop => {
                    self.flush_and_log_errors(&runtime);
                    self.channels
                        .result_sender
                        .send(Ok(()))
                        .map_err(|error| error.to_string())?;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Insert the `compressed_segments` into the in-memory compressed data buffer for the table
    /// with `table_name` and the column at `column_index`. If `compressed_segments` is saved
    /// successfully, return [`Ok`], otherwise return [`IOError`].
    async fn insert_compressed_segments(
        &self,
        compressed_segment_batch: CompressedSegmentBatch,
    ) -> Result<(), IOError> {
        let column_index = compressed_segment_batch.column_index();
        let model_table_name = compressed_segment_batch.model_table_name();

        debug!(
            "Inserting batch with {} rows into compressed data buffer for column '{}' in table '{}'.",
            compressed_segment_batch.compressed_segments.num_rows(),
            column_index,
            model_table_name
        );

        // Since the compressed segments are already in memory, insert the segments into the
        // compressed data buffer first and check if the reserved memory limit is exceeded after.
        let segments_size = if let Some(mut compressed_data_buffer) = self
            .compressed_data_buffers
            .get_mut(&(model_table_name.clone(), column_index))
        {
            debug!(
                "Found existing compressed data buffer for column '{}' in table '{}'.",
                column_index, model_table_name
            );

            compressed_data_buffer
                .append_compressed_segments(compressed_segment_batch.compressed_segments)
        } else {
            debug!(
                "Could not find compressed data buffer for column '{}' in table '{}'. Creating compressed data buffer.",
                column_index,
                model_table_name
            );

            let mut compressed_data_buffer = CompressedDataBuffer::new();
            let segment_size = compressed_data_buffer
                .append_compressed_segments(compressed_segment_batch.compressed_segments);

            let key = (model_table_name.to_owned(), column_index);
            self.compressed_data_buffers
                .insert(key.clone(), compressed_data_buffer);
            self.compressed_queue.push(key);

            segment_size
        };

        // Update the remaining memory for compressed data and record the change.
        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_compressed_memory_metric
            .lock()
            .unwrap()
            .append(segments_size as isize, true);

        // If the reserved memory limit is exceeded, save compressed data to disk.
        while !self
            .memory_pool
            .try_reserve_compressed_memory(segments_size)
        {
            self.save_compressed_data_to_free_memory(segments_size)
                .await?;
        }

        Ok(())
    }

    /// Return an [`ObjectMeta`] for each compressed file that belongs to the column at `column_index`
    /// in the table with `table_name` and contains compressed segments within the given range of
    /// time and value. If no files belong to the column at `column_index` for the table with
    /// `table_name` an empty [`Vec`] is returned, while a
    /// [`DataRetrievalError`](ModelarDbError::DataRetrievalError) is returned if:
    /// * A table with `table_name` does not exist.
    /// * The compressed files could not be listed.
    /// * A column with `column_index` does not exist.
    /// * The end time is before the start time.
    /// * The max value is smaller than the min value.
    pub(super) async fn compressed_files(
        &self,
        table_name: &str,
        column_index: u16,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        min_value: Option<Value>,
        max_value: Option<Value>,
        query_data_folder: &Arc<dyn ObjectStore>,
    ) -> Result<Vec<ObjectMeta>, ModelarDbError> {
        // Retrieve the metadata of all files that fit the given arguments.
        let relevant_object_metas = self
            .table_metadata_manager
            .compressed_files(
                table_name,
                column_index.into(),
                start_time,
                end_time,
                min_value,
                max_value,
            )
            .await
            .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?;

        // Merge the compressed Apache Parquet files if multiple are retrieved to ensure order.
        if relevant_object_metas.len() > 1 {
            let compressed_file = Self::merge_compressed_apache_parquet_files(
                query_data_folder,
                &relevant_object_metas,
                query_data_folder,
                &format!("{COMPRESSED_DATA_FOLDER}/{table_name}/{column_index}"),
            )
                .await
                .map_err(|error| {
                    ModelarDbError::DataRetrievalError(format!(
                        "Compressed data could not be merged for column '{column_index}' in table '{table_name}': {error}"
                    ))
                })?;

            self.table_metadata_manager
                .replace_compressed_files(
                    table_name,
                    column_index.into(),
                    &relevant_object_metas,
                    Some(&compressed_file),
                )
                .await
                .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?;

            Ok(vec![compressed_file.file_metadata])
        } else {
            Ok(relevant_object_metas)
        }
    }

    /// Save [`CompressedDataBuffers`](CompressedDataBuffer) to disk until at least `size_in_bytes`
    /// bytes of memory is available. If all of the data is saved successfully, return [`Ok`],
    /// otherwise return [`IOError`].
    async fn save_compressed_data_to_free_memory(
        &self,
        size_in_bytes: usize,
    ) -> Result<(), IOError> {
        debug!("Out of memory for compressed data. Saving compressed data to disk.");

        while self.memory_pool.remaining_compressed_memory_in_bytes() < size_in_bytes as isize {
            let (table_name, column_index) = self
                .compressed_queue
                .pop()
                .expect("Not enough compressed data to free up the required memory.");
            self.save_compressed_data(&table_name, column_index).await?;
        }
        Ok(())
    }

    /// Flush the data that the [`CompressedDataManager`] is currently managing. Writes a log
    /// message if the data cannot be flushed.
    fn flush_and_log_errors(&self, runtime: &Runtime) {
        runtime.block_on(async {
            if let Err(error) = self.flush().await {
                error!(
                    "Failed to flush data in compressed data manager due to: {}",
                    error
                );
            }
        });
    }

    /// Flush the data that the [`CompressedDataManager`] is currently managing. Returns an
    /// [`IOError`] if the data cannot be flushed.
    async fn flush(&self) -> Result<(), IOError> {
        info!(
            "Flushing the remaining {} compressed data buffers.",
            self.compressed_queue.len()
        );

        while !self.compressed_queue.is_empty() {
            let (table_name, column_index) = self.compressed_queue.pop().unwrap();
            self.save_compressed_data(&table_name, column_index).await?;
        }

        Ok(())
    }

    /// Save the compressed data that belongs to the column at `column_index` in the table with
    /// `table_name` to disk and save the metadata to the metadata database. The size of the saved
    /// compressed data is added back to the remaining reserved memory. If the data is saved
    /// successfully, return [`Ok`], otherwise return [`IOError`].
    async fn save_compressed_data(
        &self,
        table_name: &str,
        column_index: u16,
    ) -> Result<(), IOError> {
        debug!("Saving compressed time series to disk.");

        // unwrap() is safe as key is read from compressed_queue.
        let (_key, mut compressed_data_buffer) = self
            .compressed_data_buffers
            .remove(&(table_name.to_owned(), column_index))
            .unwrap();

        let folder_path = format!("{COMPRESSED_DATA_FOLDER}/{table_name}/{column_index}");

        let compressed_file =
            compressed_data_buffer.save_to_apache_parquet(&self.local_data_folder, &folder_path)?;

        // Save the metadata of the compressed file to the metadata database.
        self.table_metadata_manager
            .save_compressed_file(table_name, column_index.into(), &compressed_file)
            .await
            .unwrap();

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        let freed_memory = compressed_data_buffer.size_in_bytes;
        self.used_compressed_memory_metric
            .lock()
            .unwrap()
            .append(-(freed_memory as isize), true);

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_disk_space_metric
            .lock()
            .unwrap()
            .append(compressed_file.file_metadata.size as isize, true);

        // Pass the saved compressed file to the data transfer component if a remote data folder
        // was provided. If the total size of the files related to table_name have reached the
        // transfer threshold, the files are transferred to the remote object store.
        if let Some(data_transfer) = &*self.data_transfer.read().await {
            data_transfer
                .add_compressed_file(table_name, column_index, &compressed_file)
                .await
                .map_err(|error| IOError::new(ErrorKind::Other, error.to_string()))?;
        }

        // Update the remaining memory for compressed data.
        self.memory_pool.free_compressed_memory(freed_memory);

        debug!(
            "Saved {} bytes of compressed data to disk. Remaining reserved bytes: {}.",
            freed_memory,
            self.memory_pool.remaining_compressed_memory_in_bytes()
        );

        Ok(())
    }

    /// Change the amount of memory for compressed data in bytes according to `value_change`. If
    /// less than zero bytes remain, save compressed data to free memory. If all the data is saved
    /// successfully return [`Ok`], otherwise return [`IOError`].
    pub(super) async fn adjust_compressed_remaining_memory_in_bytes(
        &self,
        value_change: isize,
    ) -> Result<(), IOError> {
        self.memory_pool.adjust_compressed_memory(value_change);
        self.save_compressed_data_to_free_memory(0).await?;

        Ok(())
    }

    /// Merge the Apache Parquet files in `input_data_folder`/`input_files` and write them to a
    /// single Apache Parquet file in `output_data_folder`/`output_folder`. Return a [`CompressedFile`]
    /// that represents the merged file if it is written successfully, otherwise [`ParquetError`] is
    /// returned.
    pub(super) async fn merge_compressed_apache_parquet_files(
        input_data_folder: &Arc<dyn ObjectStore>,
        input_files: &[ObjectMeta],
        output_data_folder: &Arc<dyn ObjectStore>,
        output_folder: &str,
    ) -> Result<CompressedFile, ParquetError> {
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

        // Use an UUID for the file name to ensure the name is unique.
        let uuid = Uuid::new_v4();
        let output_file_path = format!("{output_folder}/{uuid}.parquet");

        // Specify that the file must be sorted by univariate_id and then by start_time.
        let sorting_columns = Some(vec![
            SortingColumn::new(0, false, false),
            SortingColumn::new(2, false, false),
        ]);

        // Write the concatenated and merged record batch to the output location.
        let mut buf = vec![].writer();
        let mut apache_arrow_writer =
            StorageEngine::create_apache_arrow_writer(&mut buf, schema, sorting_columns)?;
        apache_arrow_writer.write(&merged)?;
        apache_arrow_writer.close()?;

        output_data_folder
            .put(
                &output_file_path.clone().into(),
                Bytes::from(buf.into_inner()),
            )
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

        // Return a CompressedFile that represents the successfully merged and written file.
        let object_meta = output_data_folder
            .head(&output_file_path.clone().into())
            .await
            .map_err(|error| ParquetError::General(error.to_string()))?;

        Ok(CompressedFile::from_record_batch(object_meta, &merged))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;

    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
    use futures::StreamExt;
    use modelardb_common::metadata;
    use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
    use modelardb_common::test;
    use modelardb_common::types::{ArrowTimestamp, ArrowValue, ErrorBound};
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectStorePath;
    use object_store::ObjectStore;
    use ringbuf::Rb;
    use tempfile::{self, TempDir};

    const COLUMN_INDEX: u16 = 5;

    // Tests for insert_record_batch().
    #[tokio::test]
    async fn test_insert_record_batch() {
        let record_batch = test::compressed_segments_record_batch();
        let (temp_dir, data_manager) = create_compressed_data_manager().await;

        let local_data_folder = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
        let table_folder = ObjectStorePath::parse(format!(
            "{COMPRESSED_DATA_FOLDER}/{}/",
            test::MODEL_TABLE_NAME
        ))
        .unwrap();

        let table_folder_files = local_data_folder
            .list(Some(&table_folder))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(table_folder_files.is_empty());

        data_manager
            .insert_record_batch(test::MODEL_TABLE_NAME, record_batch)
            .await
            .unwrap();

        let table_folder_files = local_data_folder
            .list(Some(&table_folder))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(table_folder_files.len(), 1);
    }

    // Tests for insert_compressed_segments().
    #[tokio::test]
    async fn test_can_insert_compressed_segment_into_new_compressed_data_buffer() {
        let segments = compressed_segments_record_batch();
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;
        let key = (test::MODEL_TABLE_NAME.to_owned(), COLUMN_INDEX);

        data_manager
            .insert_compressed_segments(segments)
            .await
            .unwrap();

        assert!(data_manager.compressed_data_buffers.contains_key(&key));
        assert_eq!(data_manager.compressed_queue.pop().unwrap(), key);
        assert!(
            data_manager
                .compressed_data_buffers
                .get(&key)
                .unwrap()
                .size_in_bytes
                > 0
        );
    }

    #[tokio::test]
    async fn test_can_insert_compressed_segment_into_existing_compressed_data_buffer() {
        let segments = compressed_segments_record_batch();
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;
        let key = (test::MODEL_TABLE_NAME.to_owned(), COLUMN_INDEX);

        data_manager
            .insert_compressed_segments(segments.clone())
            .await
            .unwrap();
        let previous_size = data_manager
            .compressed_data_buffers
            .get(&key)
            .unwrap()
            .size_in_bytes;

        data_manager
            .insert_compressed_segments(segments)
            .await
            .unwrap();

        assert!(
            data_manager
                .compressed_data_buffers
                .get(&key)
                .unwrap()
                .size_in_bytes
                > previous_size
        );
    }

    #[tokio::test]
    async fn test_save_first_compressed_data_buffer_if_out_of_memory() {
        let segments = compressed_segments_record_batch();
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_compressed_memory_in_bytes() as usize;

        // Insert compressed data into the storage engine until data is saved to Apache Parquet.
        let max_compressed_segments = reserved_memory / test::COMPRESSED_SEGMENTS_SIZE;
        for _ in 0..max_compressed_segments + 1 {
            data_manager
                .insert_compressed_segments(segments.clone())
                .await
                .unwrap();
        }

        // The compressed data should be saved to the table_name folder in the compressed folder.
        let local_data_folder = Path::new(&data_manager.local_data_folder);
        let compressed_path = local_data_folder.join(format!(
            "{COMPRESSED_DATA_FOLDER}/{}",
            test::MODEL_TABLE_NAME
        ));
        assert_eq!(compressed_path.read_dir().unwrap().count(), 1);

        // The metadata of the compressed data should be saved to the metadata database.
        let compressed_files = data_manager
            .table_metadata_manager
            .compressed_files(
                test::MODEL_TABLE_NAME,
                COLUMN_INDEX.into(),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(compressed_files.len(), 1);
    }

    #[tokio::test]
    async fn test_remaining_bytes_decremented_when_inserting_compressed_segments() {
        let segments = compressed_segments_record_batch();
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_compressed_memory_in_bytes();

        data_manager
            .insert_compressed_segments(segments)
            .await
            .unwrap();

        assert!(
            reserved_memory
                > data_manager
                    .memory_pool
                    .remaining_compressed_memory_in_bytes()
        );
        assert_eq!(
            data_manager
                .used_compressed_memory_metric
                .lock()
                .unwrap()
                .values()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_remaining_memory_incremented_when_saving_compressed_segments() {
        let segments = compressed_segments_record_batch();
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;

        data_manager
            .insert_compressed_segments(segments.clone())
            .await
            .unwrap();

        // Set the remaining memory to a negative value since data is only saved when out of memory.
        data_manager.memory_pool.adjust_compressed_memory(
            -(data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes()
                + 1),
        );

        data_manager
            .save_compressed_data_to_free_memory(0)
            .await
            .unwrap();

        assert!(
            -1 < data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes()
        );
        assert_eq!(
            data_manager
                .used_compressed_memory_metric
                .lock()
                .unwrap()
                .values()
                .len(),
            2
        );
        assert_eq!(
            data_manager
                .used_disk_space_metric
                .lock()
                .unwrap()
                .values()
                .len(),
            1
        );
    }

    // Tests for compressed_files().
    #[tokio::test]
    async fn test_can_get_compressed_file_for_table() {
        let (temp_dir, data_manager) = create_compressed_data_manager().await;

        // Insert compressed segments into the same table.
        let segments = compressed_segments_record_batch();
        data_manager
            .insert_compressed_segments(segments.clone())
            .await
            .unwrap();
        data_manager
            .insert_compressed_segments(segments)
            .await
            .unwrap();
        data_manager.flush().await.unwrap();

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager
            .compressed_files(
                test::MODEL_TABLE_NAME,
                COLUMN_INDEX,
                None,
                None,
                None,
                None,
                &object_store,
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_get_no_compressed_files_for_non_existent_table() {
        let (temp_dir, data_manager) = create_compressed_data_manager().await;

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result = data_manager.compressed_files(
            test::MODEL_TABLE_NAME,
            COLUMN_INDEX,
            None,
            None,
            None,
            None,
            &object_store,
        );

        assert!(result.await.unwrap().is_empty());
    }

    // Tests for adjust_compressed_remaining_memory_in_bytes().
    #[tokio::test]
    async fn test_increase_compressed_remaining_memory_in_bytes() {
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;

        data_manager
            .adjust_compressed_remaining_memory_in_bytes(10000)
            .await
            .unwrap();

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes(),
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize + 10000
        )
    }

    #[tokio::test]
    async fn test_decrease_compressed_remaining_memory_in_bytes() {
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;

        // Insert data that should be saved when the remaining memory is decreased.
        let segments = compressed_segments_record_batch();
        data_manager
            .insert_compressed_segments(segments)
            .await
            .unwrap();

        data_manager
            .adjust_compressed_remaining_memory_in_bytes(
                -(test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize),
            )
            .await
            .unwrap();

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes(),
            0
        );

        // There should no longer be any compressed data in memory.
        assert_eq!(data_manager.compressed_data_buffers.len(), 0);
    }

    #[tokio::test]
    #[should_panic(expected = "Not enough compressed data to free up the required memory.")]
    async fn test_panic_if_decreasing_compressed_remaining_memory_in_bytes_below_zero() {
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;

        data_manager
            .adjust_compressed_remaining_memory_in_bytes(
                -((test::COMPRESSED_RESERVED_MEMORY_IN_BYTES + 1) as isize),
            )
            .await
            .unwrap();
    }

    /// Create a [`CompressedDataManager`] with a folder that is deleted once the test is finished
    /// and a metadata manager with a single model table.
    async fn create_compressed_data_manager() -> (TempDir, CompressedDataManager) {
        let temp_dir = tempfile::tempdir().unwrap();

        let local_data_folder = temp_dir.path().to_path_buf();

        let channels = Arc::new(Channels::new());

        let memory_pool = Arc::new(MemoryPool::new(
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        ));

        // Create a metadata manager and save a single model table to the metadata database.
        let metadata_manager = Arc::new(
            metadata::try_new_sqlite_table_metadata_manager(temp_dir.path())
                .await
                .unwrap(),
        );

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        (
            temp_dir,
            CompressedDataManager::try_new(
                RwLock::new(None),
                local_data_folder,
                channels,
                memory_pool,
                metadata_manager,
                Arc::new(Mutex::new(Metric::new())),
            )
            .unwrap(),
        )
    }

    /// Return a [`CompressedSegmentBatch`] containing three compressed segments.
    fn compressed_segments_record_batch() -> CompressedSegmentBatch {
        compressed_segment_batch_with_time(0, 0.0)
    }

    /// Return a [`CompressedSegmentBatch`] containing three compressed segments. The compressed
    /// segments time range is from `time_ms` to `time_ms` + 3, while the value range is from
    /// `offset` + 5.2 to `offset` + 34.2.
    fn compressed_segment_batch_with_time(time_ms: i64, offset: f32) -> CompressedSegmentBatch {
        let univariate_id = COLUMN_INDEX as u64;
        let query_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("field", ArrowValue::DATA_TYPE, false),
        ]));
        let model_table_metadata = Arc::new(
            ModelTableMetadata::try_new(
                test::MODEL_TABLE_NAME.to_owned(),
                query_schema,
                vec![
                    ErrorBound::try_new(0.0).unwrap(),
                    ErrorBound::try_new(0.0).unwrap(),
                ],
                vec![None, None],
            )
            .unwrap(),
        );
        let compressed_segments =
            test::compressed_segments_record_batch_with_time(univariate_id, time_ms, offset);

        CompressedSegmentBatch::new(univariate_id, model_table_metadata, compressed_segments)
    }
}
