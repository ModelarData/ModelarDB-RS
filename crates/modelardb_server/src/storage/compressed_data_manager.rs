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

//! Support for managing all compressed data that is inserted into the
//! [`StorageEngine`](crate::storage::StorageEngine).

use std::io::{Error as IOError, ErrorKind};
use std::sync::{Arc, Mutex};

use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use deltalake::DeltaTableError;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::metadata::compressed_file::CompressedFile;
use modelardb_common::metadata::TableMetadataManager;
use modelardb_common::storage::{self, DeltaLake};
use modelardb_common::types::{Timestamp, Value};
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::async_reader::ParquetObjectReader;
use sqlx::Sqlite;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use crate::storage::compressed_data_buffer::{CompressedDataBuffer, CompressedSegmentBatch};
use crate::storage::data_transfer::DataTransfer;
use crate::storage::types::Message;
use crate::storage::types::{Channels, MemoryPool};
use crate::storage::{Metric, COMPRESSED_DATA_FOLDER};
use crate::ClusterMode;

/// Stores data points compressed as segments containing metadata and models in memory to batch the
/// compressed segments before saving them to Apache Parquet files.
pub(super) struct CompressedDataManager {
    /// Component that transfers saved compressed data to the remote data folder when it is necessary.
    pub(super) data_transfer: Arc<RwLock<Option<DataTransfer>>>,
    /// Path to the folder containing all compressed data managed by the
    /// [`StorageEngine`](crate::storage::StorageEngine).
    pub(crate) local_data_folder: Arc<DeltaLake>,
    /// The compressed segments before they are saved to persistent storage. The key is the name of
    /// the table the compressed segments represents data points for so the Apache Parquet files can
    /// be partitioned by table and then column.
    compressed_data_buffers: DashMap<String, CompressedDataBuffer>,
    /// FIFO queue of table names and column indices referring to [`CompressedDataBuffer`] that can
    /// be saved to persistent storage.
    compressed_queue: SegQueue<String>,
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
    pub(super) fn new(
        data_transfer: Arc<RwLock<Option<DataTransfer>>>,
        local_data_folder: Arc<DeltaLake>,
        channels: Arc<Channels>,
        memory_pool: Arc<MemoryPool>,
        table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
        used_disk_space_metric: Arc<Mutex<Metric>>,
    ) -> Self {
        Self {
            data_transfer,
            local_data_folder,
            compressed_data_buffers: DashMap::new(),
            compressed_queue: SegQueue::new(),
            channels,
            table_metadata_manager,
            memory_pool,
            used_compressed_memory_metric: Mutex::new(Metric::new()),
            used_disk_space_metric,
        }
    }

    /// Write `record_batch` to the table with `table_name` as a compressed Apache Parquet file.
    pub(super) async fn insert_record_batch(
        &self,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> Result<(), DeltaTableError> {
        debug!(
            "Received record batch with {} rows for the table '{}'.",
            record_batch.num_rows(),
            table_name
        );

        self.local_data_folder
            .write_record_batch_to_table(table_name, record_batch)
            .await
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
    /// with `table_name`. If `compressed_segments` is saved successfully, return [`Ok`], otherwise
    /// return [`IOError`].
    async fn insert_compressed_segments(
        &self,
        compressed_segment_batch: CompressedSegmentBatch,
    ) -> Result<(), IOError> {
        let model_table_name = compressed_segment_batch.model_table_name();
        debug!("Inserting batch into compressed data buffer for table '{model_table_name}'.");

        // Since the compressed segments are already in memory, insert the segments into the
        // compressed data buffer first and check if the reserved memory limit is exceeded after.
        let segments_size = if let Some(mut compressed_data_buffer) =
            self.compressed_data_buffers.get_mut(model_table_name)
        {
            debug!("Found existing compressed data buffer for table '{model_table_name}'.",);

            compressed_data_buffer
                .append_compressed_segments(compressed_segment_batch.compressed_segments)
        } else {
            // A String is created as two copies are required for compressed_data_buffer and
            // compressed_queue anyway and compressed_segments cannot be moved out of
            // compressed_segment_batch if model_table_name is actively being borrowed.
            let model_table_name = model_table_name.to_owned();
            debug!("Creating compressed data buffer for table '{model_table_name}' as none exist.",);

            let mut compressed_data_buffer = CompressedDataBuffer::new();
            let segment_size = compressed_data_buffer
                .append_compressed_segments(compressed_segment_batch.compressed_segments);

            self.compressed_data_buffers
                .insert(model_table_name.clone(), compressed_data_buffer);
            self.compressed_queue.push(model_table_name);

            segment_size
        }?;

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
        field_column_index: u16,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        min_value: Option<Value>,
        max_value: Option<Value>,
        cluster_mode: &ClusterMode,
        query_data_folder: &Arc<dyn ObjectStore>,
    ) -> Result<Vec<ObjectMeta>, ModelarDbError> {
        // Retrieve the metadata of all files that fit the given arguments. If the server is a cloud
        // node, use the table metadata manager for the remote metadata database.
        let relevant_object_metas =
            if let Some(table_metadata_manager) = cluster_mode.remote_table_metadata_manager() {
                table_metadata_manager
                    .compressed_files(
                        table_name,
                        field_column_index.into(),
                        start_time,
                        end_time,
                        min_value,
                        max_value,
                    )
                    .await
            } else {
                self.table_metadata_manager
                    .compressed_files(
                        table_name,
                        field_column_index.into(),
                        start_time,
                        end_time,
                        min_value,
                        max_value,
                    )
                    .await
            }
            .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?;

        // Merge the compressed Apache Parquet files if multiple are retrieved to ensure order.
        if relevant_object_metas.len() > 1 {
            let compressed_file = Self::merge_compressed_apache_parquet_files(
                query_data_folder,
                &relevant_object_metas,
                query_data_folder,
                table_name,
                field_column_index,
            )
                .await
                .map_err(|error| {
                    ModelarDbError::DataRetrievalError(format!(
                        "Compressed data could not be merged for column '{field_column_index}' in table '{table_name}': {error}"
                    ))
                })?;

            // Replace the merged files in the metadata database. If the server is a cloud node,
            // use the table metadata manager for the remote metadata database.
            if let Some(table_metadata_manager) = cluster_mode.remote_table_metadata_manager() {
                table_metadata_manager
                    .replace_compressed_files(
                        table_name,
                        field_column_index.into(),
                        &relevant_object_metas,
                        Some(&compressed_file),
                    )
                    .await
            } else {
                self.table_metadata_manager
                    .replace_compressed_files(
                        table_name,
                        field_column_index.into(),
                        &relevant_object_metas,
                        Some(&compressed_file),
                    )
                    .await
            }
            .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?;

            Ok(vec![compressed_file.file_metadata])
        } else {
            Ok(relevant_object_metas)
        }
    }

    /// Save [`CompressedDataBuffers`](CompressedDataBuffer) to disk until at least `size_in_bytes`
    /// bytes of memory is available. If all the data is saved successfully, return [`Ok`],
    /// otherwise return [`IOError`].
    async fn save_compressed_data_to_free_memory(
        &self,
        size_in_bytes: usize,
    ) -> Result<(), IOError> {
        debug!("Out of memory for compressed data. Saving compressed data to disk.");

        while self.memory_pool.remaining_compressed_memory_in_bytes() < size_in_bytes as isize {
            let table_name = self
                .compressed_queue
                .pop()
                .expect("Not enough compressed data to free up the required memory.");
            self.save_compressed_data(&table_name).await?;
        }
        Ok(())
    }

    /// Flush the data that the [`CompressedDataManager`] is currently managing. Writes a log
    /// message if some of the data cannot be flushed.
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
            let table_name = self.compressed_queue.pop().unwrap();
            self.save_compressed_data(&table_name).await?;
        }

        Ok(())
    }

    /// Save the compressed data that belongs to the table with `table_name` The size of the saved
    /// compressed data is added back to the remaining compressed memory. If the data is written
    /// successfully to disk, return [`Ok`], otherwise return [`IOError`].
    async fn save_compressed_data(&self, table_name: &str) -> Result<(), IOError> {
        debug!("Saving compressed segments to disk for {table_name}.");

        // unwrap() is safe as table_name is read from compressed_queue.
        let (_table_name, compressed_data_buffer) =
            self.compressed_data_buffers.remove(table_name).unwrap();

        // Disk space use is over approximated as Apache Parquet applies lossless compression.
        let compressed_data_buffer_size_in_bytes = compressed_data_buffer.size_in_bytes;
        let compressed_segments = compressed_data_buffer.record_batch().await;
        self.local_data_folder
            .write_compressed_segments_to_model_table(table_name, compressed_segments)
            .await
            .map_err(|error| IOError::new(ErrorKind::Other, error.to_string()))?;

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_disk_space_metric
            .lock()
            .unwrap()
            .append(compressed_data_buffer_size_in_bytes as isize, true);

        // Inform the data transfer component about the new data if a remote data folder was
        // provided. If the total size of the data related to table_name have reached the transfer
        // threshold, all of the data is transferred to the remote object store.
        if let Some(data_transfer) = &*self.data_transfer.read().await {
            data_transfer
                .increase_table_size(table_name, compressed_data_buffer_size_in_bytes)
                .await
                .map_err(|error| IOError::new(ErrorKind::Other, error.to_string()))?;
        }

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_compressed_memory_metric
            .lock()
            .unwrap()
            .append(-(compressed_data_buffer_size_in_bytes as isize), true);

        // Update the remaining memory for compressed data.
        self.memory_pool
            .adjust_compressed_memory(compressed_data_buffer_size_in_bytes as isize);

        debug!(
            "Saved {} bytes of compressed data to disk. Remaining reserved bytes: {}.",
            compressed_data_buffer_size_in_bytes,
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

    /// Merge the Apache Parquet files in `input_data_folder`/`input_files` and write them to a an
    /// Apache Parquet file in `output_data_folder`. Return a [`CompressedFile`] that represents the
    /// merged file if it is written successfully, otherwise [`ParquetError`] is returned.
    pub(super) async fn merge_compressed_apache_parquet_files(
        input_data_folder: &Arc<dyn ObjectStore>,
        input_files: &[ObjectMeta],
        output_data_folder: &Arc<dyn ObjectStore>,
        table_name: &str,
        field_column_index: u16,
    ) -> Result<CompressedFile, ParquetError> {
        // Read input files, for_each is not used so errors can be returned with ?.
        let mut record_batches = Vec::with_capacity(input_files.len());
        for input_file in input_files {
            let reader = ParquetObjectReader::new(input_data_folder.clone(), input_file.clone());
            record_batches
                .append(&mut storage::read_batches_from_apache_parquet_file(reader).await?);
        }

        // Merge the record batches into a single concatenated and merged record batch.
        let schema = record_batches[0].schema();
        let concatenated = compute::concat_batches(&schema, &record_batches)?;
        let merged = modelardb_compression::try_merge_segments(concatenated)
            .map_err(|error| ParquetError::General(error.to_string()))?;

        let output_file_path = storage::write_compressed_segments_to_apache_parquet_file(
            COMPRESSED_DATA_FOLDER,
            table_name,
            field_column_index,
            &merged,
            output_data_folder,
        )
        .await?;

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
            .head(&output_file_path)
            .await
            .map_err(|error| ParquetError::General(error.to_string()))?;

        Ok(CompressedFile::from_compressed_data(
            object_meta,
            field_column_index,
            &merged,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
    use deltalake::Path;
    use futures::StreamExt;
    use modelardb_common::metadata;
    use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
    use modelardb_common::test;
    use modelardb_common::types::{ArrowTimestamp, ArrowValue, ErrorBound};
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use ringbuf::traits::observer::Observer;
    use tempfile::{self, TempDir};

    const COLUMN_INDEX: u16 = 1;
    const ERROR_BOUND_ZERO: f32 = 0.0;

    // Tests for insert_record_batch().
    #[tokio::test]
    async fn test_insert_record_batch() {
        let record_batch = test::compressed_segments_record_batch();
        let (temp_dir, data_manager) = create_compressed_data_manager().await;

        let local_data_folder = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
        let table_folder = Path::parse(format!(
            "{COMPRESSED_DATA_FOLDER}/{}/",
            test::MODEL_TABLE_NAME
        ))
        .unwrap();

        let table_folder_files = local_data_folder
            .list(Some(&table_folder))
            .collect::<Vec<_>>()
            .await;
        assert!(table_folder_files.is_empty());

        data_manager
            .insert_record_batch(test::MODEL_TABLE_NAME, record_batch)
            .await
            .unwrap();

        let table_folder_files = local_data_folder
            .list(Some(&table_folder))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(table_folder_files.len(), 1);
    }

    // Tests for insert_compressed_segments().
    #[tokio::test]
    async fn test_can_insert_compressed_segment_into_new_compressed_data_buffer() {
        let segments = compressed_segments_record_batch();
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;
        let key = test::MODEL_TABLE_NAME;

        data_manager
            .insert_compressed_segments(segments)
            .await
            .unwrap();

        assert!(data_manager.compressed_data_buffers.contains_key(key));
        assert_eq!(data_manager.compressed_queue.pop().unwrap(), key);
        assert!(
            data_manager
                .compressed_data_buffers
                .get(key)
                .unwrap()
                .size_in_bytes
                > 0
        );
    }

    #[tokio::test]
    async fn test_can_insert_compressed_segment_into_existing_compressed_data_buffer() {
        let segments = compressed_segments_record_batch();
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;

        data_manager
            .insert_compressed_segments(segments.clone())
            .await
            .unwrap();
        let previous_size = data_manager
            .compressed_data_buffers
            .get(test::MODEL_TABLE_NAME)
            .unwrap()
            .size_in_bytes;

        data_manager
            .insert_compressed_segments(segments)
            .await
            .unwrap();

        assert!(
            data_manager
                .compressed_data_buffers
                .get(test::MODEL_TABLE_NAME)
                .unwrap()
                .size_in_bytes
                > previous_size
        );
    }

    #[tokio::test]
    async fn test_save_first_compressed_data_buffer_if_out_of_memory() {
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;

        let compressed_segment_batch = compressed_segments_record_batch();
        let reserved_memory = data_manager
            .memory_pool
            .remaining_compressed_memory_in_bytes() as usize;

        // Insert compressed data into the storage engine until data is saved to Apache Parquet.
        let compressed_buffer_size = compressed_segment_batch
            .model_table_metadata
            .field_column_indices
            .len()
            * test::COMPRESSED_SEGMENTS_SIZE;
        let max_compressed_segments = reserved_memory / compressed_buffer_size;
        for _ in 0..max_compressed_segments + 1 {
            data_manager
                .insert_compressed_segments(compressed_segment_batch.clone())
                .await
                .unwrap();
        }

        // The compressed data should be saved to the table_name folder in the compressed folder.
        let delta_table = data_manager
            .local_data_folder
            .delta_table(test::MODEL_TABLE_NAME)
            .await
            .unwrap();
        let parquet_files = delta_table.get_files_iter().unwrap().collect::<Vec<_>>();

        // One Apache Parquet file is created for each field column in the batch.
        assert_eq!(parquet_files.len(), 2);
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
                .occupied_len(),
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
                .occupied_len(),
            2
        );
        assert_eq!(
            data_manager
                .used_disk_space_metric
                .lock()
                .unwrap()
                .values()
                .occupied_len(),
            2
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
                &ClusterMode::SingleNode,
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
            &ClusterMode::SingleNode,
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
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        let channels = Arc::new(Channels::new());

        let memory_pool = Arc::new(MemoryPool::new(
            test::INGESTED_RESERVED_MEMORY_IN_BYTES,
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        ));

        // Create a metadata manager and save a single model table to the metadata database.
        let metadata_manager = Arc::new(
            metadata::try_new_sqlite_table_metadata_manager(&object_store)
                .await
                .unwrap(),
        );

        let local_data_folder =
            Arc::new(DeltaLake::from_local_path(temp_dir.path().to_str().unwrap()).unwrap());

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        (
            temp_dir,
            CompressedDataManager::new(
                Arc::new(RwLock::new(None)),
                local_data_folder,
                channels,
                memory_pool,
                metadata_manager,
                Arc::new(Mutex::new(Metric::new())),
            ),
        )
    }

    /// Return a [`CompressedSegmentBatch`] containing three compressed segments per field column.
    fn compressed_segments_record_batch() -> CompressedSegmentBatch {
        compressed_segment_batch_with_time(0, 0.0)
    }

    /// Return a [`CompressedSegmentBatch`] containing compressed segments for a model table with
    /// two field columns. For each of the field columns the batch contains three compressed
    /// segments. The compressed segments time range is from `time_ms` to `time_ms` + 3, while the
    /// value range is from `offset` + 5.2 to `offset` + 34.2.
    fn compressed_segment_batch_with_time(time_ms: i64, offset: f32) -> CompressedSegmentBatch {
        let query_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
        ]));
        let model_table_metadata = Arc::new(
            ModelTableMetadata::try_new(
                test::MODEL_TABLE_NAME.to_owned(),
                query_schema,
                vec![
                    ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
                    ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
                    ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
                ],
                vec![None, None, None],
            )
            .unwrap(),
        );

        let compressed_segments =
            test::compressed_segments_record_batch_with_time(COLUMN_INDEX as u64, time_ms, offset);

        CompressedSegmentBatch::new(
            model_table_metadata,
            vec![compressed_segments.clone(), compressed_segments],
        )
    }
}
