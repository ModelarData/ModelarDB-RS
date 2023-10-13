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

//! Support for managing all uncompressed data that is ingested into the
//! [`StorageEngine`](crate::storage::StorageEngine).

use std::fs;
use std::io::{Error as IOError, ErrorKind as IOErrorKind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_channel::SendError;
use dashmap::DashMap;
use datafusion::arrow::array::{Array, StringArray};
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::types::{Timestamp, TimestampArray, Value, ValueArray};
use tokio::runtime::Runtime;
use tracing::{debug, error};

use crate::context::Context;
use crate::metadata::MetadataManager;
use crate::storage::compressed_data_buffer::CompressedSegmentBatch;
use crate::storage::types::Channels;
use crate::storage::types::MemoryPool;
use crate::storage::types::Message;
use crate::storage::uncompressed_data_buffer::UncompressedDataMultivariate;
use crate::storage::uncompressed_data_buffer::{
    UncompressedDataBuffer, UncompressedInMemoryDataBuffer, UncompressedOnDiskDataBuffer,
};
use crate::storage::{Metric, UNCOMPRESSED_DATA_FOLDER};

/// Stores uncompressed data points temporarily in an in-memory buffer that spills to Apache Parquet
/// files. When a uncompressed data buffer is finished the data is made available for compression.
pub(super) struct UncompressedDataManager {
    /// Path to the folder containing all uncompressed data managed by the
    /// [`StorageEngine`](crate::storage::StorageEngine).
    local_data_folder: PathBuf,
    /// Counter incremented for each [`datafusion::arrow::record_batch::RecordBatch`] of data points
    /// ingested. The value is assigned to buffers that are created or updated and is used to flush
    /// buffers that are no longer used.
    current_batch_index: AtomicU64,
    /// The [`UncompressedDataBuffers`](UncompressedDataBuffer) currently being filled with ingested
    /// data points.
    active_uncompressed_data_buffers: DashMap<u64, UncompressedInMemoryDataBuffer>,
    /// Channels used by the storage engine's threads to communicate.
    channels: Arc<Channels>,
    /// Management of metadata for ingesting and compressing time series.
    metadata_manager: Arc<MetadataManager>,
    /// Track how much memory is left for storing uncompressed and compressed data.
    memory_pool: Arc<MemoryPool>,
    /// Metric for the used uncompressed memory in bytes, updated every time the used memory changes.
    pub(super) used_uncompressed_memory_metric: Mutex<Metric>,
    /// Metric for the amount of ingested data points, updated every time a new batch of data is ingested.
    pub(super) ingested_data_points_metric: Mutex<Metric>,
    /// Metric for the total used disk space in bytes, updated every time uncompressed data is spilled.
    pub used_disk_space_metric: Arc<Mutex<Metric>>,
}

impl UncompressedDataManager {
    /// Return an [`UncompressedDataManager`] if the required folder can be created in
    /// `local_data_folder` and an [`UncompressedOnDiskDataBuffer`] can be initialized for all
    /// Apache Parquet files containing uncompressed data points in `local_data_store`, otherwise
    /// [`IOError`] is returned.
    pub(super) async fn try_new(
        local_data_folder: PathBuf,
        memory_pool: Arc<MemoryPool>,
        channels: Arc<Channels>,
        metadata_manager: Arc<MetadataManager>,
        used_disk_space_metric: Arc<Mutex<Metric>>,
    ) -> Result<Self, IOError> {
        // Ensure the folder required by the uncompressed data manager exists.
        let local_uncompressed_data_folder = local_data_folder.join(UNCOMPRESSED_DATA_FOLDER);
        fs::create_dir_all(local_uncompressed_data_folder)?;

        Ok(Self {
            local_data_folder,
            current_batch_index: AtomicU64::new(0),
            active_uncompressed_data_buffers: DashMap::new(),
            channels,
            metadata_manager,
            memory_pool,
            used_uncompressed_memory_metric: Mutex::new(Metric::new()),
            ingested_data_points_metric: Mutex::new(Metric::new()),
            used_disk_space_metric,
        })
    }

    /// Add references to the [`UncompressedDataBuffers`](UncompressedDataBuffer) currently on disk
    /// to [`UncompressedDataManager`] which immediately will start compressing them.
    pub(super) async fn initialize(
        &self,
        local_data_folder: PathBuf,
        context: &Context,
    ) -> Result<(), IOError> {
        let local_uncompressed_data_folder = local_data_folder.join(UNCOMPRESSED_DATA_FOLDER);
        let mut initial_disk_space = 0;

        for maybe_folder_dir_entry in local_uncompressed_data_folder.read_dir()? {
            let folder_dir_entry = maybe_folder_dir_entry?;

            let univariate_id = folder_dir_entry
                .file_name()
                .to_str()
                .ok_or_else(|| IOError::new(IOErrorKind::InvalidData, "Path is not valid UTF-8."))?
                .parse::<u64>()
                .map_err(|error| IOError::new(IOErrorKind::InvalidData, error))?;

            // unwrap() is safe as univariate_id can only exist if it is in the metadata database.
            let table_name = context
                .metadata_manager
                .univariate_id_to_table_name(univariate_id)
                .await
                .unwrap();

            // unwrap() is safe as data cannot be ingested into a model table that does not exist.
            let model_table_metadata = context
                .model_table_metadata_from_default_database_schema(&table_name)
                .await
                .unwrap()
                .unwrap();

            for maybe_file_dir_entry in folder_dir_entry.path().read_dir()? {
                let buffer = Box::new(UncompressedOnDiskDataBuffer::try_new(
                    univariate_id,
                    model_table_metadata.clone(),
                    maybe_file_dir_entry?.path(),
                )?);

                initial_disk_space += buffer.disk_size();

                self.channels
                    .univariate_data_sender
                    .send(Message::Data(buffer))
                    .map_err(|error| IOError::new(IOErrorKind::BrokenPipe, error))?;
            }
        }

        // Record the used disk space of the uncompressed data buffers currently on disk.
        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_disk_space_metric
            .lock()
            .unwrap()
            .append(initial_disk_space as isize, true);

        Ok(())
    }

    /// Read and process messages received from the [`StorageEngine`](super::StorageEngine) to
    /// either ingest uncompressed data, flush buffers, or stop.
    pub(super) fn process_uncompressed_messages(
        &self,
        runtime: Arc<Runtime>,
    ) -> Result<(), String> {
        loop {
            let message = self
                .channels
                .multivariate_data_receiver
                .recv()
                .map_err(|error| error.to_string())?;

            match message {
                Message::Data(uncompressed_data_multivariate) => {
                    runtime
                        .block_on(self.insert_data_points(uncompressed_data_multivariate))
                        .map_err(|error| error.to_string())?;
                }
                Message::Flush => {
                    self.flush_and_log_errors();
                    self.channels
                        .univariate_data_sender
                        .send(Message::Flush)
                        .map_err(|error| error.to_string())?;
                }
                Message::Stop => {
                    self.flush_and_log_errors();
                    self.channels
                        .univariate_data_sender
                        .send(Message::Stop)
                        .map_err(|error| error.to_string())?;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Insert `uncompressed_data_multivariate` into the in-memory buffer. If the data points are
    /// from a multivariate time series, they are first split into multiple univariate time series.
    /// These univariate time series are then inserted into the storage engine. Return [`String`] if
    /// the channel or the metadata database could not be read from.
    async fn insert_data_points(
        &self,
        uncompressed_data_multivariate: UncompressedDataMultivariate,
    ) -> Result<(), String> {
        let data_points = uncompressed_data_multivariate.multivariate_data_points;
        let model_table_metadata = uncompressed_data_multivariate.model_table_metadata;

        debug!(
            "Received record batch with {} data points for the table '{}'.",
            data_points.num_rows(),
            model_table_metadata.name
        );

        // Read the current batch index as it may be updated in parallel.
        let current_batch_index = self.current_batch_index.load(Ordering::Relaxed);

        // Record the amount of ingested data points.
        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.ingested_data_points_metric
            .lock()
            .unwrap()
            .append(data_points.num_rows() as isize, false);

        // Prepare the timestamp column for iteration.
        let timestamp_index = model_table_metadata.timestamp_column_index;
        let timestamps: &TimestampArray = data_points
            .column(timestamp_index)
            .as_any()
            .downcast_ref()
            .unwrap();

        // Prepare the tag columns for iteration.
        let tag_column_arrays: Vec<&StringArray> = model_table_metadata
            .tag_column_indices
            .iter()
            .map(|index| data_points.column(*index).as_any().downcast_ref().unwrap())
            .collect();

        // Prepare the field columns for iteration. The column index is saved with the corresponding
        // array.
        let field_column_arrays: Vec<(usize, &ValueArray)> = model_table_metadata
            .schema
            .fields()
            .iter()
            .filter_map(|field| {
                let index = model_table_metadata
                    .schema
                    .index_of(field.name().as_str())
                    .unwrap();

                // Field columns are the columns that are not the timestamp column or one of the tag
                // columns.
                let not_timestamp_column = index != model_table_metadata.timestamp_column_index;
                let not_tag_column = !model_table_metadata.tag_column_indices.contains(&index);

                if not_timestamp_column && not_tag_column {
                    Some((
                        index,
                        data_points.column(index).as_any().downcast_ref().unwrap(),
                    ))
                } else {
                    None
                }
            })
            .collect();

        // For each data point, generate a hash from the tags, extract the individual fields, and
        // insert them into the storage engine.
        for (index, timestamp) in timestamps.iter().enumerate() {
            let tag_values: Vec<String> = tag_column_arrays
                .iter()
                .map(|array| array.value(index).to_string())
                .collect();

            let tag_hash = self
                .metadata_manager
                .lookup_or_compute_tag_hash(&model_table_metadata, &tag_values)
                .await
                .map_err(|error| format!("Tag hash could not be saved: {error}"))?;

            // For each field column, generate the 64-bit univariate id, and append the current
            // timestamp and the field's value into the in-memory buffer for the univariate id.
            for (field_index, field_column_array) in &field_column_arrays {
                let univariate_id = tag_hash | *field_index as u64;
                let value = field_column_array.value(index);

                // unwrap() is safe to use since the timestamps array cannot contain null values.
                self.insert_data_point(
                    univariate_id,
                    timestamp.unwrap(),
                    value,
                    model_table_metadata.clone(),
                    current_batch_index,
                )
                .await
                .map_err(|error| error.to_string())?
            }
        }

        // Unused buffers are purposely only finished at the end of insert_data_points() so that the
        // buffers required for any of the data points in the current batch are never finished.
        let current_batch_index = self.current_batch_index.fetch_add(1, Ordering::Relaxed);
        self.finish_unused_buffers(current_batch_index)
            .await
            .map_err(|error| error.to_string())?;

        // Return the memory used by the data points to the pool right before they are de-allocated.
        self.memory_pool
            .free_uncompressed_memory(data_points.get_array_memory_size());

        Ok(())
    }

    /// Insert a single data point into the in-memory buffer for `univariate_id` if one exists. If
    /// no in-memory buffer exist for `univariate_id`, allocate a new buffer that will be compressed
    /// within `error_bound`. Returns [`IOError`] if the error bound cannot be retrieved from the
    /// [`MetadataManager`].
    async fn insert_data_point(
        &self,
        univariate_id: u64,
        timestamp: Timestamp,
        value: Value,
        model_table_metadata: Arc<ModelTableMetadata>,
        current_batch_index: u64,
    ) -> Result<(), IOError> {
        debug!(
            "Inserting data point ({}, {}) into uncompressed data buffer for {}.",
            timestamp, value, univariate_id
        );

        // get_mut() maintains a reference into active_uncompressed_data_buffers which causes
        // remove() to deadlock, so removing buffers must be done after the reference is dropped.
        let mut buffer_is_full = false;

        // Insert the data point into an existing or new buffer.
        if let Some(mut univariate_id_buffer) = self
            .active_uncompressed_data_buffers
            .get_mut(&univariate_id)
        {
            debug!("Found existing buffer for {}.", univariate_id);
            let buffer = univariate_id_buffer.value_mut();
            buffer.insert_data(current_batch_index, timestamp, value);
            buffer_is_full = buffer.is_full();
        } else {
            debug!(
                "Could not find buffer for {}. Creating Buffer.",
                univariate_id
            );

            self.memory_pool
                .reserve_uncompressed_memory(UncompressedInMemoryDataBuffer::memory_size());

            // Create a new buffer and reduce the remaining amount of reserved memory by its size.
            let mut buffer = UncompressedInMemoryDataBuffer::new(
                univariate_id,
                model_table_metadata,
                current_batch_index,
            );

            let used_memory = UncompressedInMemoryDataBuffer::memory_size();

            // unwrap() is safe as lock() only returns an error if the lock is poisoned.
            self.used_uncompressed_memory_metric
                .lock()
                .unwrap()
                .append(used_memory as isize, true);

            debug!(
                "Created buffer for {}. Remaining reserved bytes: {}.",
                self.memory_pool.remaining_uncompressed_memory_in_bytes(),
                univariate_id
            );

            buffer.insert_data(current_batch_index, timestamp, value);
            self.active_uncompressed_data_buffers
                .insert(univariate_id, buffer);
        }

        // Transfer the full buffer to the compressor.
        if buffer_is_full {
            debug!(
                "Buffer for {} is full, moving it to the channel of finished buffers.",
                univariate_id
            );

            // unwrap() is safe as this is only reachable if the buffer exists in the HashMap.
            let (_univariate_id, full_uncompressed_in_memory_data_buffer) = self
                .active_uncompressed_data_buffers
                .remove(&univariate_id)
                .unwrap();

            let finished_uncompressed_data_buffer: Box<dyn UncompressedDataBuffer> =
                if self.memory_pool.remaining_uncompressed_memory_in_bytes() <= 0 {
                    debug!(
                        "Over allocated memory for uncompressed data, spilling finished buffer."
                    );

                    let uncompressed_on_disk_data_buffer = self
                        .spill_finished_buffer(full_uncompressed_in_memory_data_buffer)
                        .await?;

                    Box::new(uncompressed_on_disk_data_buffer)
                } else {
                    Box::new(full_uncompressed_in_memory_data_buffer)
                };

            return self
                .channels
                .univariate_data_sender
                .send(Message::Data(finished_uncompressed_data_buffer))
                .map_err(|error| IOError::new(IOErrorKind::BrokenPipe, error));
        }

        Ok(())
    }

    /// Spill `uncompressed_in_memory_data_buffer` if it is an [`UncompressedInMemoryDataBuffer`]
    /// and return [`IOError`] if it is an [`UncompressedOnDiskDataBuffer`].
    async fn spill_finished_buffer(
        &self,
        uncompressed_in_memory_data_buffer: UncompressedInMemoryDataBuffer,
    ) -> Result<UncompressedOnDiskDataBuffer, IOError> {
        debug_assert!(uncompressed_in_memory_data_buffer.is_full());

        let uncompressed_in_memory_data_buffer_debug_string =
            format!("{:?}", uncompressed_in_memory_data_buffer);
        let uncompressed_on_disk_data_buffer = uncompressed_in_memory_data_buffer
            .spill_to_apache_parquet(self.local_data_folder.as_path())
            .await?;

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        let freed_memory = UncompressedInMemoryDataBuffer::memory_size();
        self.used_uncompressed_memory_metric
            .lock()
            .unwrap()
            .append(-(freed_memory as isize), true);

        // Record the used disk space of the spilled finished buffer.
        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_disk_space_metric
            .lock()
            .unwrap()
            .append(uncompressed_on_disk_data_buffer.disk_size() as isize, true);

        // Add the size of the in-memory data buffer back to the remaining reserved bytes.
        self.memory_pool.free_uncompressed_memory(freed_memory);

        debug!(
            "Spilled '{}'. Remaining reserved bytes: {}.",
            uncompressed_in_memory_data_buffer_debug_string,
            self.memory_pool.remaining_uncompressed_memory_in_bytes()
        );

        Ok(uncompressed_on_disk_data_buffer)
    }

    /// Finish active in-memory data buffers that are no longer used to free memory.
    async fn finish_unused_buffers(&self, current_batch_index: u64) -> Result<(), IOError> {
        debug!("Freeing memory by finishing all in-memory buffers that are no longer used.");

        // Extract the univariate ids of the unused buffers and then delete the unused buffers as it
        // is not possible to iterate and delete together until HashMap::drain_filter() is stable.
        let mut univariate_ids_of_unused_buffers =
            Vec::with_capacity(self.active_uncompressed_data_buffers.len());

        for univariate_id_buffer in &self.active_uncompressed_data_buffers {
            if univariate_id_buffer.value().is_unused(current_batch_index) {
                univariate_ids_of_unused_buffers.push(*univariate_id_buffer.key());
            }
        }

        for univariate_id in univariate_ids_of_unused_buffers {
            // unwrap() is safe as the univariate_ids were just extracted from the map.
            let (_univariate_id, buffer) = self
                .active_uncompressed_data_buffers
                .remove(&univariate_id)
                .unwrap();

            self.channels
                .univariate_data_sender
                .send(Message::Data(Box::new(buffer)))
                .map_err(|error| IOError::new(IOErrorKind::InvalidData, error))?;

            debug!(
                "Finished in-memory buffer for {} as it is no longer used.",
                univariate_id
            );
        }

        Ok(())
    }

    /// Compress the uncompressed data buffers that the [`UncompressedDataManager`] is currently
    /// managing, and return the compressed buffers and their univariate ids. Writes a log message
    /// if a [`Message`] cannot be sent to [`CompressedDataManager`](super::CompressedDataManager).
    fn flush_and_log_errors(&self) {
        if let Err(error) = self.flush() {
            error!(
                "Failed to flush data in uncompressed data manager due to: {}",
                error
            );
        }
    }

    /// Compress the uncompressed data buffers that the [`UncompressedDataManager`] is currently
    /// managing, and return the compressed buffers and their univariate ids. Returns [`SendError`]
    /// if a [`Message`] cannot be sent to [`CompressedDataManager`](super::CompressedDataManager).
    fn flush(&self) -> Result<(), SendError<Message<Box<dyn UncompressedDataBuffer>>>> {
        // The univariate ids are copied to not have multiple borrows to self at the same time.
        let univariate_ids: Vec<u64> = self
            .active_uncompressed_data_buffers
            .iter()
            .map(|kv| *kv.key())
            .collect();

        for univariate_id in univariate_ids {
            if let Some((_univariate_id, buffer)) =
                self.active_uncompressed_data_buffers.remove(&univariate_id)
            {
                self.channels
                    .univariate_data_sender
                    .send(Message::Data(Box::new(buffer)))?;
            }
        }

        Ok(())
    }

    /// Read and process messages received from the [`UncompressedDataManager`] to either compress
    /// uncompressed data, forward a flush message, or stop.
    pub(super) fn process_compressor_messages(&self, runtime: Arc<Runtime>) -> Result<(), String> {
        loop {
            let message = self
                .channels
                .univariate_data_receiver
                .recv()
                .map_err(|error| error.to_string())?;

            match message {
                Message::Data(data_buffer) => {
                    runtime
                        .block_on(self.compress_finished_buffer(data_buffer))
                        .map_err(|error| error.to_string())?;
                }
                Message::Flush => {
                    self.channels
                        .compressed_data_sender
                        .send(Message::Flush)
                        .map_err(|error| error.to_string())?;
                }
                Message::Stop => {
                    self.channels
                        .compressed_data_sender
                        .send(Message::Stop)
                        .map_err(|error| error.to_string())?;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Compress `data_buffer` and send the compressed segments to the
    /// [`CompressedDataManager`]()super::CompressedDataManager over a channel. Returns
    /// [`SendError`] if a [`Message`] cannot be sent to
    /// [`CompressedDataManager`](super::CompressedDataManager).
    async fn compress_finished_buffer(
        &self,
        mut data_buffer: Box<dyn UncompressedDataBuffer>,
    ) -> Result<(), SendError<Message<CompressedSegmentBatch>>> {
        // unwrap() is safe to use as record_batch() only fail for spilled buffers with no files.
        let data_points = data_buffer.record_batch().await.unwrap();
        let univariate_id = data_buffer.univariate_id();
        let uncompressed_timestamps = modelardb_common::array!(data_points, 0, TimestampArray);
        let uncompressed_values = modelardb_common::array!(data_points, 1, ValueArray);
        let error_bound = data_buffer.error_bound();

        // unwrap() is safe to use since uncompressed_timestamps and uncompressed_values have the same length.
        let compressed_segments = modelardb_compression::try_compress(
            univariate_id,
            error_bound,
            uncompressed_timestamps,
            uncompressed_values,
        )
        .unwrap();

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        let freed_memory = data_buffer.memory_size();
        self.used_uncompressed_memory_metric
            .lock()
            .unwrap()
            .append(-(freed_memory as isize), true);

        // Add the size of the segment back to the remaining reserved bytes and record the change.
        self.memory_pool.free_uncompressed_memory(freed_memory);

        self.channels
            .compressed_data_sender
            .send(Message::Data(CompressedSegmentBatch::new(
                univariate_id,
                data_buffer.model_table_metadata().clone(),
                compressed_segments,
            )))
    }

    /// Change the amount of memory for uncompressed data in bytes according to `value_change`.
    pub(super) async fn adjust_uncompressed_remaining_memory_in_bytes(&self, value_change: isize) {
        self.memory_pool.adjust_uncompressed_memory(value_change);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrowPrimitiveType, StringBuilder};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use modelardb_common::schemas::UNCOMPRESSED_SCHEMA;
    use modelardb_common::types::{
        ArrowTimestamp, ArrowValue, ErrorBound, TimestampBuilder, ValueBuilder,
    };
    use ringbuf::Rb;

    use crate::common_test;
    use crate::metadata::MetadataManager;
    use crate::storage::UNCOMPRESSED_DATA_BUFFER_CAPACITY;

    const CURRENT_BATCH_INDEX: u64 = 0;
    const UNIVARIATE_ID: u64 = 17854860594986067969;

    // Tests for UncompressedDataManager.
    #[tokio::test]
    async fn test_can_find_existing_on_disk_data_buffers() {
        // Spill an uncompressed buffer to disk.
        let temp_dir = tempfile::tempdir().unwrap();
        let model_table_metadata = create_model_table_metadata();
        let mut buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            model_table_metadata,
            CURRENT_BATCH_INDEX,
        );
        buffer.insert_data(CURRENT_BATCH_INDEX, 100, 10.0);
        buffer
            .spill_to_apache_parquet(temp_dir.path())
            .await
            .unwrap();

        // The uncompressed data buffer should be referenced by the uncompressed data manager.
        let (_metadata_manager, _data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;

        // TODO: refactor tests to use a shared context with enough information for initialize().
        //data_manager.initialize(context);

        // Emulate data_manager.initialize(context) by counting files in UNCOMPRESSED_DATA_FOLDER.
        let local_uncompressed_data_folder = temp_dir.path().join(UNCOMPRESSED_DATA_FOLDER);
        assert_eq!(
            local_uncompressed_data_folder.read_dir().unwrap().count(),
            1
        )
    }

    #[tokio::test]
    async fn test_can_insert_record_batch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (metadata_manager, data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;

        let (model_table_metadata, data) = uncompressed_data(1);
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();
        let uncompressed_data_multivariate =
            UncompressedDataMultivariate::new(model_table_metadata, data);
        data_manager
            .insert_data_points(uncompressed_data_multivariate)
            .await
            .unwrap();

        // Two separate builders are created since the inserted data has two field columns.
        assert_eq!(data_manager.active_uncompressed_data_buffers.len(), 2);
        assert_eq!(
            data_manager
                .ingested_data_points_metric
                .lock()
                .unwrap()
                .values()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_can_insert_record_batch_with_multiple_data_points() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (metadata_manager, data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;

        let (model_table_metadata, data) = uncompressed_data(2);
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();
        let uncompressed_data_multivariate =
            UncompressedDataMultivariate::new(model_table_metadata, data);
        data_manager
            .insert_data_points(uncompressed_data_multivariate)
            .await
            .unwrap();

        // Since the tag is different for each data point, 4 separate buffers should be created.
        assert_eq!(data_manager.active_uncompressed_data_buffers.len(), 4);
        assert_eq!(
            data_manager
                .ingested_data_points_metric
                .lock()
                .unwrap()
                .values()
                .len(),
            1
        );
    }

    /// Create a record batch with data that resembles uncompressed data with a single tag and two
    /// field columns. The returned data has `row_count` rows, with a different tag for each row.
    /// Also create model table metadata for a model table that matches the created data.
    fn uncompressed_data(row_count: usize) -> (Arc<ModelTableMetadata>, RecordBatch) {
        let tags: Vec<String> = (0..row_count).map(|tag| tag.to_string()).collect();
        let timestamps: Vec<Timestamp> = (0..row_count).map(|ts| ts as Timestamp).collect();
        let values: Vec<Value> = (0..row_count).map(|value| value as Value).collect();

        let query_schema = Arc::new(Schema::new(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("value_1", ArrowValue::DATA_TYPE, false),
            Field::new("value_2", ArrowValue::DATA_TYPE, false),
        ]));

        let data = RecordBatch::try_new(
            query_schema.clone(),
            vec![
                Arc::new(StringArray::from(tags)),
                Arc::new(TimestampArray::from(timestamps)),
                Arc::new(ValueArray::from(values.clone())),
                Arc::new(ValueArray::from(values)),
            ],
        )
        .unwrap();

        let error_bounds = vec![
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
        ];

        let generated_columns = vec![None, None, None, None];

        let model_table_metadata = Arc::new(
            ModelTableMetadata::try_new(
                "model_table".to_owned(),
                query_schema,
                error_bounds,
                generated_columns,
            )
            .unwrap(),
        );

        (model_table_metadata, data)
    }

    #[tokio::test]
    async fn test_can_insert_data_point_into_new_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        insert_data_points(1, &mut data_manager, model_table_metadata, UNIVARIATE_ID).await;

        assert!(data_manager
            .active_uncompressed_data_buffers
            .contains_key(&UNIVARIATE_ID));
        assert_eq!(
            data_manager
                .active_uncompressed_data_buffers
                .get(&UNIVARIATE_ID)
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_can_insert_data_point_into_existing_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        insert_data_points(2, &mut data_manager, model_table_metadata, UNIVARIATE_ID).await;

        assert!(data_manager
            .active_uncompressed_data_buffers
            .contains_key(&UNIVARIATE_ID));
        assert_eq!(
            data_manager
                .active_uncompressed_data_buffers
                .get(&UNIVARIATE_ID)
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn test_will_finish_unused_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        // Insert using insert_data_points() to increment the batch counter.
        let mut timestamp = TimestampBuilder::new();
        timestamp.append_slice(&[100, 200, 300]);

        let mut field = ValueBuilder::new();
        field.append_slice(&[100.0, 200.0, 300.0]);

        let mut tag = StringBuilder::new();
        tag.append_value("A");
        tag.append_value("A");
        tag.append_value("A");

        let data = RecordBatch::try_new(
            model_table_metadata.schema.clone(),
            vec![
                Arc::new(timestamp.finish()),
                Arc::new(field.finish()),
                Arc::new(tag.finish()),
            ],
        )
        .unwrap();

        let uncompressed_data_multivariate =
            UncompressedDataMultivariate::new(model_table_metadata.clone(), data);
        data_manager
            .insert_data_points(uncompressed_data_multivariate)
            .await
            .unwrap();

        assert_eq!(data_manager.active_uncompressed_data_buffers.len(), 1);
        assert_eq!(data_manager.channels.univariate_data_receiver.len(), 0);
        assert_eq!(
            data_manager
                .active_uncompressed_data_buffers
                .get(&8346922066998771713)
                .unwrap()
                .len(),
            3
        );

        // Insert using insert_data_points() to finish unused buffers.
        let empty_record_batch = RecordBatch::new_empty(model_table_metadata.schema.clone());
        let uncompressed_data_multivariate =
            UncompressedDataMultivariate::new(model_table_metadata, empty_record_batch);

        data_manager
            .insert_data_points(uncompressed_data_multivariate)
            .await
            .unwrap();

        assert_eq!(data_manager.active_uncompressed_data_buffers.len(), 0);
        assert_eq!(data_manager.channels.univariate_data_receiver.len(), 1);
    }

    #[tokio::test]
    async fn test_can_get_finished_uncompressed_data_buffer_when_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            model_table_metadata,
            UNIVARIATE_ID,
        )
        .await;

        assert!(data_manager
            .channels
            .univariate_data_receiver
            .try_recv()
            .is_ok());
    }

    #[tokio::test]
    async fn test_can_get_multiple_finished_uncompressed_data_buffers_when_multiple_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY * 2,
            &mut data_manager,
            model_table_metadata,
            UNIVARIATE_ID,
        )
        .await;

        assert!(data_manager
            .channels
            .univariate_data_receiver
            .try_recv()
            .is_ok());
        assert!(data_manager
            .channels
            .univariate_data_receiver
            .try_recv()
            .is_ok());
    }

    #[tokio::test]
    async fn test_cannot_get_finished_uncompressed_data_buffers_when_none_are_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;

        assert!(data_manager
            .channels
            .univariate_data_receiver
            .try_recv()
            .is_err());
    }

    #[tokio::test]
    async fn test_spill_first_uncompressed_data_buffer_to_disk_if_out_of_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes() as usize;

        // Insert messages into the storage engine until a buffer is spilled to Apache Parquet.
        // If there is enough memory to hold n full buffers, we need n + 1 to spill a buffer.
        let max_full_buffers =
            (reserved_memory / UncompressedInMemoryDataBuffer::memory_size()) + 1;
        let message_count = max_full_buffers * UNCOMPRESSED_DATA_BUFFER_CAPACITY;
        insert_data_points(
            message_count,
            &mut data_manager,
            model_table_metadata,
            UNIVARIATE_ID,
        )
        .await;

        // The last UncompressedDataBuffer should have a memory size of 0 as it is spilled to disk.
        for _ in 0..6 {
            next_data_message(&data_manager);
        }
        assert_eq!(next_data_message(&data_manager).memory_size(), 0);
        assert_eq!(data_manager.channels.univariate_data_receiver.len(), 0);

        // The UncompressedDataBuffer should be spilled to univariate id in the uncompressed folder.
        let local_data_folder = Path::new(&data_manager.local_data_folder);
        let uncompressed_path =
            local_data_folder.join(format!("{UNCOMPRESSED_DATA_FOLDER}/{UNIVARIATE_ID}"));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1);
    }

    #[tokio::test]
    async fn test_remaining_memory_decremented_when_creating_new_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes();

        insert_data_points(1, &mut data_manager, model_table_metadata, UNIVARIATE_ID).await;

        assert!(
            reserved_memory
                > data_manager
                    .memory_pool
                    .remaining_uncompressed_memory_in_bytes()
        );
        assert_eq!(
            data_manager
                .used_uncompressed_memory_metric
                .lock()
                .unwrap()
                .values()
                .len(),
            1
        );
    }

    #[test]
    fn test_remaining_memory_incremented_when_popping_in_memory() {
        // This test purposely does not use tokio::test to prevent multiple Tokio runtimes.
        let temp_dir = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::new().unwrap());
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            runtime.block_on(create_managers(temp_dir.path()));

        runtime.block_on(insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            model_table_metadata,
            UNIVARIATE_ID,
        ));

        let remaining_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes();

        data_manager
            .channels
            .univariate_data_sender
            .send(Message::Stop)
            .unwrap();
        data_manager.process_compressor_messages(runtime).unwrap();

        assert!(
            remaining_memory
                < data_manager
                    .memory_pool
                    .remaining_uncompressed_memory_in_bytes()
        );
        assert_eq!(
            data_manager
                .used_uncompressed_memory_metric
                .lock()
                .unwrap()
                .values()
                .len(),
            2
        );
    }

    #[test]
    fn test_remaining_memory_not_incremented_when_popping_spilled() {
        // This test purposely does not use tokio::test to prevent multiple Tokio runtimes.
        let temp_dir = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::new().unwrap());
        let (_metadata_manager, data_manager, model_table_metadata) =
            runtime.block_on(create_managers(temp_dir.path()));

        // Add the spilled buffer.
        let uncompressed_data = RecordBatch::try_new(
            UNCOMPRESSED_SCHEMA.0.clone(),
            vec![
                Arc::new(TimestampArray::from(vec![0, 1, 2])),
                Arc::new(ValueArray::from(vec![0.2, 0.5, 0.1])),
            ],
        )
        .unwrap();

        let spilled_buffer = UncompressedOnDiskDataBuffer::try_spill(
            0,
            model_table_metadata,
            temp_dir.path(),
            uncompressed_data,
        )
        .unwrap();

        data_manager
            .channels
            .univariate_data_sender
            .send(Message::Stop)
            .unwrap();
        data_manager
            .channels
            .univariate_data_sender
            .send(Message::Data(Box::new(spilled_buffer)))
            .unwrap();

        let remaining_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes();

        // Since the UncompressedOnDiskDataBuffer is not in memory, the remaining amount of memory
        // should not increase when it is processed.
        data_manager.process_compressor_messages(runtime).unwrap();

        assert_eq!(
            remaining_memory,
            data_manager
                .memory_pool
                .remaining_uncompressed_memory_in_bytes(),
        );
    }

    #[tokio::test]
    async fn test_increase_uncompressed_remaining_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;

        data_manager
            .adjust_uncompressed_remaining_memory_in_bytes(10000)
            .await;

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_uncompressed_memory_in_bytes() as usize,
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES + 10000
        )
    }

    #[tokio::test]
    async fn test_decrease_uncompressed_remaining_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        // Insert data that should not be spilled when the remaining memory is decreased.
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            model_table_metadata.clone(),
            UNIVARIATE_ID,
        )
        .await;

        data_manager
            .adjust_uncompressed_remaining_memory_in_bytes(
                -(common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize),
            )
            .await;

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_uncompressed_memory_in_bytes(),
            -(common_test::UNCOMPRESSED_BUFFER_SIZE as isize)
        );

        // Insert data that should be spilled not that the remaining memory is decreased.
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            model_table_metadata,
            UNIVARIATE_ID,
        )
        .await;

        // The first UncompressedDataBuffer should have a memory size above 0 as it is in memory
        // while the second should have a memory size of 0 as it has been spilled to disk.
        assert_eq!(
            next_data_message(&data_manager).memory_size(),
            common_test::UNCOMPRESSED_BUFFER_SIZE
        );
        assert_eq!(next_data_message(&data_manager).memory_size(), 0);
    }

    /// Insert `count` data points into `data_manager`.
    async fn insert_data_points(
        count: usize,
        data_manager: &mut UncompressedDataManager,
        model_table_metadata: Arc<ModelTableMetadata>,
        univariate_id: u64,
    ) {
        let value: Value = 30.0;
        let current_batch_index = 0;

        for i in 0..count {
            data_manager
                .insert_data_point(
                    univariate_id,
                    i as i64,
                    value,
                    model_table_metadata.clone(),
                    current_batch_index,
                )
                .await
                .unwrap();
        }
    }

    /// Create an [`UncompressedDataManager`] with a folder that is deleted once the test is finished.
    async fn create_managers(
        path: &Path,
    ) -> (
        Arc<MetadataManager>,
        UncompressedDataManager,
        Arc<ModelTableMetadata>,
    ) {
        let metadata_manager = Arc::new(MetadataManager::try_new(path).await.unwrap());

        // Ensure the expected metadata is available through the metadata manager.
        let model_table_metadata = create_model_table_metadata();

        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();

        metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag".to_owned()])
            .await
            .unwrap();

        let memory_pool = Arc::new(MemoryPool::new(
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        ));

        let channels = Arc::new(Channels::new());

        // UncompressedDataManager::try_new() lookup the error bounds for each univariate_id.
        let uncompressed_data_manager = UncompressedDataManager::try_new(
            metadata_manager.local_data_folder().to_owned(),
            memory_pool,
            channels,
            metadata_manager.clone(),
            Arc::new(Mutex::new(Metric::new())),
        )
        .await
        .unwrap();

        (
            metadata_manager,
            uncompressed_data_manager,
            model_table_metadata,
        )
    }

    /// Create a [`ModelTableMetadata`].
    fn create_model_table_metadata() -> Arc<ModelTableMetadata> {
        let query_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("field", ArrowValue::DATA_TYPE, false),
            Field::new("tag", DataType::Utf8, false),
        ]));

        let error_bounds = vec![
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
            ErrorBound::try_new(0.0).unwrap(),
        ];

        let generated_columns = vec![None, None, None];

        Arc::new(
            ModelTableMetadata::try_new(
                "Table".to_owned(),
                query_schema,
                error_bounds,
                generated_columns,
            )
            .unwrap(),
        )
    }

    /// Read the next message from `univariate_data_receiver` and panic if it is not data.
    fn next_data_message(
        data_manager: &UncompressedDataManager,
    ) -> Box<dyn UncompressedDataBuffer> {
        let message = data_manager
            .channels
            .univariate_data_receiver
            .try_recv()
            .unwrap();

        if let Message::Data(buffer) = message {
            buffer
        } else {
            panic!("Next message was not data.");
        }
    }
}
