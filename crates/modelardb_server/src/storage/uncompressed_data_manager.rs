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

use std::io::{Error as IOError, ErrorKind as IOErrorKind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::{fs, mem};

use crossbeam_channel::SendError;
use dashmap::DashMap;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::metadata::TableMetadataManager;
use modelardb_common::schemas::COMPRESSED_SCHEMA;
use modelardb_common::types::{Timestamp, TimestampArray, Value, ValueArray};
use object_store::local::LocalFileSystem;
use sqlx::Sqlite;
use tokio::runtime::Runtime;
use tracing::{debug, error, warn};

use crate::context::Context;
use crate::storage::compressed_data_buffer::CompressedSegmentBatch;
use crate::storage::types::Channels;
use crate::storage::types::MemoryPool;
use crate::storage::types::Message;
use crate::storage::uncompressed_data_buffer::UncompressedDataMultivariate;
use crate::storage::uncompressed_data_buffer::{
    UncompressedDataBuffer, UncompressedInMemoryDataBuffer, UncompressedOnDiskDataBuffer,
};
use crate::storage::{Metric, UNCOMPRESSED_DATA_FOLDER};
use crate::ClusterMode;

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
    /// [`UncompresseInMemoryDataBuffers`](UncompressedInMemoryDataBuffer) that are ready to be
    /// filled with ingested data points. In-memory and on-disk buffers are stored separately to
    /// simplify locking when spilling in-memory buffers to disk and reading on-disk buffers back
    /// into memory.
    uncompressed_in_memory_data_buffers: DashMap<u64, UncompressedInMemoryDataBuffer>,
    /// [`UncompresseOnDiskDataBuffers`](UncompressedOnDiskDataBuffer) that must be read back into
    /// memory before they can be filled with ingested data points. In-memory and on-disk buffers
    /// are stored separately to simplify locking when spilling in-memory buffers to disk and
    /// reading on-disk buffers back into memory.
    uncompressed_on_disk_data_buffers: DashMap<u64, UncompressedOnDiskDataBuffer>,
    /// Channels used by the storage engine's threads to communicate.
    channels: Arc<Channels>,
    /// Management of metadata for ingesting and compressing time series.
    table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
    /// The mode of the cluster used to determine the behaviour when inserting data points.
    cluster_mode: ClusterMode,
    /// Track how much memory is left for storing uncompressed and compressed data.
    memory_pool: Arc<MemoryPool>,
    /// Metric for the used multivariate memory in bytes, updated every time the used memory changes.
    pub(super) used_multivariate_memory_metric: Arc<Mutex<Metric>>,
    /// Metric for the used uncompressed memory in bytes, updated every time the used memory changes.
    pub(super) used_uncompressed_memory_metric: Mutex<Metric>,
    /// Metric for the amount of ingested data points, updated every time a new batch of data is ingested.
    pub(super) ingested_data_points_metric: Mutex<Metric>,
    /// Metric for the total used disk space in bytes, updated every time uncompressed data is spilled.
    pub(super) used_disk_space_metric: Arc<Mutex<Metric>>,
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
        table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
        cluster_mode: ClusterMode,
        used_multivariate_memory_metric: Arc<Mutex<Metric>>,
        used_disk_space_metric: Arc<Mutex<Metric>>,
    ) -> Result<Self, IOError> {
        // Ensure the folder required by the uncompressed data manager exists.
        let local_uncompressed_data_folder = local_data_folder.join(UNCOMPRESSED_DATA_FOLDER);
        fs::create_dir_all(local_uncompressed_data_folder)?;

        Ok(Self {
            local_data_folder,
            current_batch_index: AtomicU64::new(0),
            uncompressed_in_memory_data_buffers: DashMap::new(),
            uncompressed_on_disk_data_buffers: DashMap::new(),
            channels,
            table_metadata_manager,
            cluster_mode,
            memory_pool,
            used_multivariate_memory_metric,
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

        let object_store = Arc::new(LocalFileSystem::new_with_prefix(&self.local_data_folder)?);

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
                .table_metadata_manager
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
                let buffer = UncompressedOnDiskDataBuffer::try_new(
                    univariate_id,
                    model_table_metadata.clone(),
                    self.current_batch_index.load(Ordering::Relaxed),
                    object_store.clone(),
                    maybe_file_dir_entry?
                        .path()
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap(),
                )?;

                initial_disk_space += buffer.disk_size().await;

                self.channels
                    .univariate_data_sender
                    .send(Message::Data(UncompressedDataBuffer::OnDisk(buffer)))
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

    /// Insert `uncompressed_data_multivariate` into in-memory buffers. If the data points are from
    /// a multivariate time series, they are first split into multiple univariate time series. These
    /// univariate time series are then inserted into the storage engine. Returns [`String`] if the
    /// channel or the metadata database could not be read from.
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

        // Track if any buffers are spilled so the resulting warning is only printed once per batch.
        let mut buffers_are_spilled = false;

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

            let (tag_hash, tag_hash_is_saved) = self
                .table_metadata_manager
                .lookup_or_compute_tag_hash(&model_table_metadata, &tag_values)
                .await
                .map_err(|error| format!("Tag hash could not be saved: {error}"))?;

            // If the server was started with a manager, transfer the tag hash metadata if it was
            // saved to the server metadata database. We purposely transfer tag metadata before the
            // associated files for convenience. This does not cause problems when querying.
            if let ClusterMode::MultiNode(manager) = &self.cluster_mode {
                if tag_hash_is_saved {
                    manager
                        .transfer_tag_metadata(&model_table_metadata, tag_hash, &tag_values)
                        .await
                        .map_err(|error| error.to_string())?;
                }
            }

            // For each field column, generate the 64-bit univariate id, and append the current
            // timestamp and the field's value into the in-memory buffer for the univariate id.
            for (field_index, field_column_array) in &field_column_arrays {
                let univariate_id = tag_hash | *field_index as u64;
                let value = field_column_array.value(index);

                // unwrap() is safe to use since the timestamps array cannot contain null values.
                buffers_are_spilled |= self
                    .insert_data_point(
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

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_multivariate_memory_metric
            .lock()
            .unwrap()
            .append(-(data_points.get_array_memory_size() as isize), true);

        // Return the memory used by the data points to the pool right before they are de-allocated.
        self.memory_pool
            .adjust_multivariate_memory(data_points.get_array_memory_size() as isize);

        // Print a single warning if any buffers are spilled so ingestion can be optimized.
        if buffers_are_spilled {
            warn!("Forced to spill uncompressed buffers. Reduce buffer size or increase memory.");
        }

        Ok(())
    }

    /// Insert a single data point into the in-memory buffer for `univariate_id` if one exists. If
    /// the buffer has been spilled, read it back into memory. If no buffer exists for
    /// `univariate_id`, allocate a new buffer that will be compressed within the error bound in
    /// `model_table_metadata`. Returns [`true`] if a buffer was spilled, [`false`] if not, and
    /// [`IOError`] if the error bound cannot be retrieved from the [`TableMetadataManager`].
    async fn insert_data_point(
        &self,
        univariate_id: u64,
        timestamp: Timestamp,
        value: Value,
        model_table_metadata: Arc<ModelTableMetadata>,
        current_batch_index: u64,
    ) -> Result<bool, IOError> {
        debug!(
            "Inserting data point ({}, {}) into uncompressed data buffer for {}.",
            timestamp, value, univariate_id
        );

        // Track if any buffers are spilled during ingestion so this information can be returned to
        // insert_data_points() and a warning printed once per batch instead of once per data point.
        let mut buffers_are_spilled = false;

        // Full finished buffers are removed at the end of the function as remove() may deadlock if
        // called when holding any sort of reference into the corresponding map at the same time.
        let mut buffer_is_full = false;

        // Insert the data point into an existing in-memory buffer if one exists.
        let buffer_is_in_memory = if let Some(mut univariate_id_buffer) = self
            .uncompressed_in_memory_data_buffers
            .get_mut(&univariate_id)
        {
            debug!("Found existing in-memory buffer for {}.", univariate_id);
            let uncompressed_in_memory_data_buffer = univariate_id_buffer.value_mut();
            uncompressed_in_memory_data_buffer.insert_data(current_batch_index, timestamp, value);
            buffer_is_full = uncompressed_in_memory_data_buffer.is_full();
            true
        } else {
            false
        };

        // Insert the data point into an on-disk buffer if one exists or create an in-memory buffer.
        if !buffer_is_in_memory {
            // No in-memory data buffer exists so it is necessary to either read a spilled buffer
            // from disk or create a new one. Memory for this is reserved without holding any
            // references into any of the maps as it may be necessary to spill an in-memory buffer
            // to do so. Thus, a combination of remove() and insert() may be called on the maps.
            buffers_are_spilled |= self
                .reserve_uncompressed_memory_for_in_memory_data_buffer()
                .await?;

            // unwrap() is safe as lock() only returns an error if the lock is poisoned.
            let reserved_memory = UncompressedInMemoryDataBuffer::memory_size();
            self.used_uncompressed_memory_metric
                .lock()
                .unwrap()
                .append(reserved_memory as isize, true);

            // Two ifs are needed until if-let chains is implemented in Rust stable, see eRFC 2497.
            if let Some(univariate_id_buffer) =
                self.uncompressed_on_disk_data_buffers.get(&univariate_id)
            {
                let uncompressed_on_disk_data_buffer = univariate_id_buffer.value();

                // Reading the buffer into memory deletes the on-disk buffer's file on disk and
                // read_apache_parquet() cannot take self as an argument due to how it is used.
                // unwrap() is safe as lock() only returns an error if the lock is poisoned.
                self.used_disk_space_metric.lock().unwrap().append(
                    -(uncompressed_on_disk_data_buffer.disk_size().await as isize),
                    true,
                );

                let mut uncompressed_in_memory_data_buffer = uncompressed_on_disk_data_buffer
                    .read_from_apache_parquet(current_batch_index)
                    .await?;

                uncompressed_in_memory_data_buffer.insert_data(
                    current_batch_index,
                    timestamp,
                    value,
                );

                buffer_is_full = uncompressed_in_memory_data_buffer.is_full();

                // The read-only reference must be dropped before the map can be modified.
                mem::drop(univariate_id_buffer);
                self.uncompressed_on_disk_data_buffers
                    .remove(&univariate_id);
                self.uncompressed_in_memory_data_buffers
                    .insert(univariate_id, uncompressed_in_memory_data_buffer);
            } else {
                debug!(
                    "Could not find buffer for {}. Creating Buffer.",
                    univariate_id
                );

                let mut uncompressed_in_memory_data_buffer = UncompressedInMemoryDataBuffer::new(
                    univariate_id,
                    model_table_metadata,
                    current_batch_index,
                );

                debug!(
                    "Created buffer for {}. Remaining reserved bytes: {}.",
                    self.memory_pool.remaining_uncompressed_memory_in_bytes(),
                    univariate_id
                );

                uncompressed_in_memory_data_buffer.insert_data(
                    current_batch_index,
                    timestamp,
                    value,
                );

                self.uncompressed_in_memory_data_buffers
                    .insert(univariate_id, uncompressed_in_memory_data_buffer);
            }
        }

        // Transfer the full buffer to the compressor.
        if buffer_is_full {
            debug!(
                "Buffer for {} is full, moving it to the channel of finished buffers.",
                univariate_id
            );

            // unwrap() is safe as this is only reachable if the buffer exists in the HashMap.
            let (_univariate_id, full_uncompressed_in_memory_data_buffer) = self
                .uncompressed_in_memory_data_buffers
                .remove(&univariate_id)
                .unwrap();

            return self
                .channels
                .univariate_data_sender
                .send(Message::Data(UncompressedDataBuffer::InMemory(
                    full_uncompressed_in_memory_data_buffer,
                )))
                .map(|_| buffers_are_spilled)
                .map_err(|error| IOError::new(IOErrorKind::BrokenPipe, error));
        }

        Ok(buffers_are_spilled)
    }

    /// Reserve enough memory to allocate a new uncompressed in-memory data buffer. If there is
    /// enough available memory for an uncompressed in-memory data buffer the memory is reserved and
    /// the method returns. Otherwise, if there are buffers waiting to be compressed, it is assumed
    /// that some of them are in-memory and the thread is blocked until memory have been returned to
    /// the pool. If there are no buffers waiting to be compressed, all of the memory for
    /// uncompressed data is used for unfinished uncompressed in-memory data buffers and it is
    /// necessary to spill one before a new buffer can ever be allocated. To keep the implementation
    /// simple, it spills a random buffer and does not check if the last uncompressed in-memory data
    /// buffer has been read from the channel but is not yet compressed. Returns [`true`] if a
    /// buffer was spilled, [`false`] if not, and [`IOError`] if spilling fails.
    async fn reserve_uncompressed_memory_for_in_memory_data_buffer(&self) -> Result<bool, IOError> {
        // It is not guaranteed that compressing the data buffers in the channel releases any memory
        // as all of the data buffers that are waiting to be compressed may all be stored on disk.
        if self.memory_pool.wait_for_uncompressed_memory_until(
            UncompressedInMemoryDataBuffer::memory_size(),
            || self.channels.univariate_data_sender.is_empty(),
        ) {
            Ok(false)
        } else {
            self.spill_in_memory_data_buffer().await?;
            Ok(true)
        }
    }

    /// Spill a random [`UncompressedInMemoryDataBuffer`]. Returns an [`IOError`] if no data buffers
    /// are currently in memory or if the writing to disk fails.
    async fn spill_in_memory_data_buffer(&self) -> Result<(), IOError> {
        // Extract univariate_id but drop the reference to the map element as remove()
        // may deadlock if called when holding any sort of reference into the map.
        let univariate_id = {
            *self
                .uncompressed_in_memory_data_buffers
                .iter()
                .next()
                .ok_or_else(|| IOError::new(IOErrorKind::NotFound, "No in-memory data buffer."))?
                .key()
        };

        // unwrap() is safe as univariate_id was just extracted from the map.
        let mut uncompressed_in_memory_data_buffer = self
            .uncompressed_in_memory_data_buffers
            .remove(&univariate_id)
            .unwrap()
            .1;

        let object_store = Arc::new(LocalFileSystem::new_with_prefix(&self.local_data_folder)?);
        let maybe_uncompressed_on_disk_data_buffer = uncompressed_in_memory_data_buffer
            .spill_to_apache_parquet(object_store)
            .await;

        // If an error occurs the in-memory buffer must be re-added to the map before returning.
        let uncompressed_on_disk_data_buffer = match maybe_uncompressed_on_disk_data_buffer {
            Ok(uncompressed_on_disk_data_buffer) => uncompressed_on_disk_data_buffer,
            Err(error) => {
                self.uncompressed_in_memory_data_buffers
                    .insert(univariate_id, uncompressed_in_memory_data_buffer);
                return Err(error);
            }
        };

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        let freed_memory = UncompressedInMemoryDataBuffer::memory_size();
        self.used_uncompressed_memory_metric
            .lock()
            .unwrap()
            .append(-(freed_memory as isize), true);

        // Record the used disk space of the spilled finished buffer.
        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        let disk_size = uncompressed_on_disk_data_buffer.disk_size().await;
        self.used_disk_space_metric
            .lock()
            .unwrap()
            .append(disk_size as isize, true);

        self.uncompressed_on_disk_data_buffers
            .insert(univariate_id, uncompressed_on_disk_data_buffer);

        // Add the size of the in-memory data buffer back to the remaining reserved bytes.
        self.memory_pool
            .adjust_uncompressed_memory(freed_memory as isize);

        debug!(
            "Spilled in-memory buffer. Remaining reserved bytes: {}.",
            self.memory_pool.remaining_uncompressed_memory_in_bytes()
        );

        Ok(())
    }

    /// Finish active in-memory and on-disk data buffers that are no longer used to free memory and
    /// bound latency.
    async fn finish_unused_buffers(&self, current_batch_index: u64) -> Result<(), IOError> {
        debug!("Freeing memory by finishing in-memory and on-disk buffers that are not used.");

        // In-memory univariate ids are copied to prevent multiple concurrent borrows to the map.
        let univariate_ids_of_unused_in_memory_buffers = self
            .uncompressed_in_memory_data_buffers
            .iter()
            .filter(|kv| kv.value().is_unused(current_batch_index))
            .map(|kv| *kv.key())
            .collect::<Vec<u64>>();

        for univariate_id in univariate_ids_of_unused_in_memory_buffers {
            // unwrap() is safe as the univariate_ids were just extracted from the map.
            let (_univariate_id, uncompressed_in_memory_data_buffer) = self
                .uncompressed_in_memory_data_buffers
                .remove(&univariate_id)
                .unwrap();

            self.channels
                .univariate_data_sender
                .send(Message::Data(UncompressedDataBuffer::InMemory(
                    uncompressed_in_memory_data_buffer,
                )))
                .map_err(|error| IOError::new(IOErrorKind::InvalidData, error))?;

            debug!(
                "Finished in-memory buffer for {} as it is no longer used.",
                univariate_id
            );
        }

        // On-disk univariate ids are copied to prevent multiple borrows to the map the same time.
        let univariate_ids_of_unused_on_disk_buffers = self
            .uncompressed_on_disk_data_buffers
            .iter()
            .filter(|kv| kv.value().is_unused(current_batch_index))
            .map(|kv| *kv.key())
            .collect::<Vec<u64>>();

        for univariate_id in univariate_ids_of_unused_on_disk_buffers {
            // unwrap() is safe as the univariate_ids were just extracted from the map.
            let (_univariate_id, uncompressed_on_disk_data_buffer) = self
                .uncompressed_on_disk_data_buffers
                .remove(&univariate_id)
                .unwrap();

            self.channels
                .univariate_data_sender
                .send(Message::Data(UncompressedDataBuffer::OnDisk(
                    uncompressed_on_disk_data_buffer,
                )))
                .map_err(|error| IOError::new(IOErrorKind::InvalidData, error))?;

            debug!(
                "Finished on-disk buffer for {} as it is no longer used.",
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

    /// Send the uncompressed data buffers that the [`UncompressedDataManager`] is managing to the
    /// compressor. Returns [`SendError`] if a [`Message`] cannot be sent to the compressor.
    fn flush(&self) -> Result<(), SendError<Message<UncompressedDataBuffer>>> {
        // In-memory univariate ids are copied to prevent multiple concurrent borrows to the map.
        let in_memory_univariate_ids: Vec<u64> = self
            .uncompressed_in_memory_data_buffers
            .iter()
            .map(|kv| *kv.key())
            .collect();

        for univariate_id in in_memory_univariate_ids {
            if let Some((_univariate_id, buffer)) = self
                .uncompressed_in_memory_data_buffers
                .remove(&univariate_id)
            {
                self.channels
                    .univariate_data_sender
                    .send(Message::Data(UncompressedDataBuffer::InMemory(buffer)))?;
            }
        }

        // On-disk univariate ids are copied to prevent multiple concurrent borrows to the map.
        let on_disk_univariate_ids: Vec<u64> = self
            .uncompressed_on_disk_data_buffers
            .iter()
            .map(|kv| *kv.key())
            .collect();

        for univariate_id in on_disk_univariate_ids {
            if let Some((_univariate_id, buffer)) = self
                .uncompressed_on_disk_data_buffers
                .remove(&univariate_id)
            {
                self.channels
                    .univariate_data_sender
                    .send(Message::Data(UncompressedDataBuffer::OnDisk(buffer)))?;
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

    /// Compress `uncompressed_data_buffer` and send the compressed segments to the
    /// [`CompressedDataManager`]()super::CompressedDataManager over a channel. Returns
    /// [`SendError`] if a [`Message`] cannot be sent to
    /// [`CompressedDataManager`](super::CompressedDataManager).
    async fn compress_finished_buffer(
        &self,
        uncompressed_data_buffer: UncompressedDataBuffer,
    ) -> Result<(), SendError<Message<CompressedSegmentBatch>>> {
        let (
            maybe_data_points,
            univariate_id,
            error_bound,
            model_table_metadata,
            memory_use,
            disk_use,
        ) = match uncompressed_data_buffer {
            UncompressedDataBuffer::InMemory(mut uncompressed_in_memory_data_buffer) => (
                uncompressed_in_memory_data_buffer.record_batch().await,
                uncompressed_in_memory_data_buffer.univariate_id(),
                uncompressed_in_memory_data_buffer.error_bound(),
                uncompressed_in_memory_data_buffer
                    .model_table_metadata()
                    .clone(),
                UncompressedInMemoryDataBuffer::memory_size(),
                0,
            ),
            UncompressedDataBuffer::OnDisk(uncompressed_on_disk_data_buffer) => (
                uncompressed_on_disk_data_buffer.record_batch().await,
                uncompressed_on_disk_data_buffer.univariate_id(),
                uncompressed_on_disk_data_buffer.error_bound(),
                uncompressed_on_disk_data_buffer
                    .model_table_metadata()
                    .clone(),
                0,
                uncompressed_on_disk_data_buffer.disk_size().await,
            ),
        };

        let data_points = maybe_data_points.map_err(|_| {
            SendError(Message::Data(CompressedSegmentBatch::new(
                univariate_id,
                model_table_metadata.clone(),
                RecordBatch::new_empty(COMPRESSED_SCHEMA.0.clone()),
            )))
        })?;
        let uncompressed_timestamps = modelardb_common::array!(data_points, 0, TimestampArray);
        let uncompressed_values = modelardb_common::array!(data_points, 1, ValueArray);

        // unwrap() is safe as uncompressed_timestamps and uncompressed_values have the same length.
        let compressed_segments = modelardb_compression::try_compress(
            univariate_id,
            error_bound,
            uncompressed_timestamps,
            uncompressed_values,
        )
        .unwrap();

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_uncompressed_memory_metric
            .lock()
            .unwrap()
            .append(-(memory_use as isize), true);

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_disk_space_metric
            .lock()
            .unwrap()
            .append(-(disk_use as isize), true);

        // Add the size of the segment back to the remaining reserved bytes.
        self.memory_pool
            .adjust_uncompressed_memory(memory_use as isize);

        self.channels
            .compressed_data_sender
            .send(Message::Data(CompressedSegmentBatch::new(
                univariate_id,
                model_table_metadata,
                compressed_segments,
            )))
    }

    /// Change the amount of memory for uncompressed data in bytes according to `value_change`.
    /// Restores the configuration and returns [`IOError`] if an in-memory buffer cannot be spilled.
    pub(super) async fn adjust_uncompressed_remaining_memory_in_bytes(
        &self,
        value_change: isize,
    ) -> Result<(), IOError> {
        self.memory_pool.adjust_uncompressed_memory(value_change);

        while self.memory_pool.remaining_uncompressed_memory_in_bytes() < 0 {
            if let Err(error) = self.spill_in_memory_data_buffer().await {
                self.memory_pool.adjust_uncompressed_memory(-value_change);
                return Err(error);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;
    use std::sync::Arc;

    use datafusion::arrow::array::StringBuilder;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::arrow::record_batch::RecordBatch;
    use modelardb_common::metadata;
    use modelardb_common::schemas::UNCOMPRESSED_SCHEMA;
    use modelardb_common::test;
    use modelardb_common::types::{TimestampBuilder, ValueBuilder};
    use ringbuf::Rb;

    use crate::storage::UNCOMPRESSED_DATA_BUFFER_CAPACITY;

    const CURRENT_BATCH_INDEX: u64 = 0;
    const UNIVARIATE_ID: u64 = 9674644176454356993;

    // Tests for UncompressedDataManager.
    #[tokio::test]
    async fn test_can_find_existing_on_disk_data_buffers() {
        // Spill an uncompressed buffer to disk.
        let temp_dir = tempfile::tempdir().unwrap();
        let model_table_metadata = Arc::new(test::model_table_metadata());
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
        let (_metadata_manager, data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        let data = uncompressed_data(1, model_table_metadata.schema.clone());
        let uncompressed_data_multivariate =
            UncompressedDataMultivariate::new(model_table_metadata, data);

        data_manager
            .insert_data_points(uncompressed_data_multivariate)
            .await
            .unwrap();

        // Two separate builders are created since the inserted data has two field columns.
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 2);
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
        let (_metadata_manager, data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        let data = uncompressed_data(2, model_table_metadata.schema.clone());
        let uncompressed_data_multivariate =
            UncompressedDataMultivariate::new(model_table_metadata, data);

        data_manager
            .insert_data_points(uncompressed_data_multivariate)
            .await
            .unwrap();

        // Since the tag is different for each data point, 4 separate buffers should be created.
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 4);
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
    async fn test_remaining_multivariate_memory_increased_after_processing_record_batch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        let data = uncompressed_data(2, model_table_metadata.schema.clone());
        let data_size = data.get_array_memory_size();

        // Simulate StorageEngine decrementing multivariate memory when receiving multivariate data.
        let multivariate_memory_before = data_manager
            .memory_pool
            .remaining_multivariate_memory_in_bytes();

        data_manager
            .used_multivariate_memory_metric
            .lock()
            .unwrap()
            .append(data.get_array_memory_size() as isize, true);

        let uncompressed_data_multivariate =
            UncompressedDataMultivariate::new(model_table_metadata, data);

        data_manager
            .insert_data_points(uncompressed_data_multivariate)
            .await
            .unwrap();

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_multivariate_memory_in_bytes(),
            multivariate_memory_before + data_size as isize
        );

        assert_eq!(
            data_manager
                .used_multivariate_memory_metric
                .lock()
                .unwrap()
                .values()
                .len(),
            2
        );
    }

    /// Create a record batch with data that resembles uncompressed data with a single tag and two
    /// field columns. The returned data has `row_count` rows, with a different tag for each row.
    /// Also create model table metadata for a model table that matches the created data.
    fn uncompressed_data(row_count: usize, schema: SchemaRef) -> RecordBatch {
        let tags: Vec<String> = (0..row_count).map(|tag| tag.to_string()).collect();
        let timestamps: Vec<Timestamp> = (0..row_count).map(|ts| ts as Timestamp).collect();
        let values: Vec<Value> = (0..row_count).map(|value| value as Value).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampArray::from(timestamps)),
                Arc::new(ValueArray::from(values.clone())),
                Arc::new(ValueArray::from(values)),
                Arc::new(StringArray::from(tags)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_can_insert_data_point_into_new_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        insert_data_points(1, &mut data_manager, &model_table_metadata, UNIVARIATE_ID).await;

        assert!(data_manager
            .uncompressed_in_memory_data_buffers
            .contains_key(&UNIVARIATE_ID));
        assert_eq!(
            data_manager
                .uncompressed_in_memory_data_buffers
                .get(&UNIVARIATE_ID)
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_can_insert_data_point_into_existing_in_memory_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        insert_data_points(1, &mut data_manager, &model_table_metadata, UNIVARIATE_ID).await;
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 0);

        insert_data_points(1, &mut data_manager, &model_table_metadata, UNIVARIATE_ID).await;
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 0);

        assert!(data_manager
            .uncompressed_in_memory_data_buffers
            .contains_key(&UNIVARIATE_ID));
        assert_eq!(
            data_manager
                .uncompressed_in_memory_data_buffers
                .get(&UNIVARIATE_ID)
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn test_can_insert_data_point_into_existing_on_disk_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        insert_data_points(1, &mut data_manager, &model_table_metadata, UNIVARIATE_ID).await;
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 0);

        data_manager.spill_in_memory_data_buffer().await.unwrap();
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 0);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 1);

        insert_data_points(1, &mut data_manager, &model_table_metadata, UNIVARIATE_ID).await;
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 0);

        assert!(data_manager
            .uncompressed_in_memory_data_buffers
            .contains_key(&UNIVARIATE_ID));
        assert_eq!(
            data_manager
                .uncompressed_in_memory_data_buffers
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

        let mut field_1 = ValueBuilder::new();
        field_1.append_slice(&[100.0, 200.0, 300.0]);

        let mut field_2 = ValueBuilder::new();
        field_2.append_slice(&[50.0, 100.0, 150.0]);

        let mut tag = StringBuilder::new();
        tag.append_value("A");
        tag.append_value("A");
        tag.append_value("A");

        let data = RecordBatch::try_new(
            model_table_metadata.schema.clone(),
            vec![
                Arc::new(timestamp.finish()),
                Arc::new(field_1.finish()),
                Arc::new(field_2.finish()),
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

        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 2);
        assert_eq!(data_manager.channels.univariate_data_receiver.len(), 0);
        assert_eq!(
            data_manager
                .uncompressed_in_memory_data_buffers
                .get(&4940964593210619905)
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

        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 0);
        assert_eq!(data_manager.channels.univariate_data_receiver.len(), 2);
    }

    #[tokio::test]
    async fn test_can_get_finished_uncompressed_data_buffer_when_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;
        insert_data_points(
            *UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            &model_table_metadata,
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
            *UNCOMPRESSED_DATA_BUFFER_CAPACITY * 2,
            &mut data_manager,
            &model_table_metadata,
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
    async fn test_spill_random_uncompressed_data_buffer_to_disk_if_out_of_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes() as usize;

        // Insert messages into the storage engine until all of the memory is used and the next
        // message inserted would block the thread until the data messages have been processed.
        let number_of_buffers = reserved_memory / UncompressedInMemoryDataBuffer::memory_size();
        for univariate_id in 0..number_of_buffers {
            // Allocate many buffers that are never finished.
            insert_data_points(
                1,
                &mut data_manager,
                &model_table_metadata.clone(),
                univariate_id as u64,
            )
            .await;
        }

        // The buffers should be in-memory and there should not be enough memory left for one more.
        assert_eq!(
            data_manager.uncompressed_in_memory_data_buffers.len(),
            number_of_buffers
        );
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 0);
        assert_eq!(data_manager.channels.univariate_data_receiver.len(), 0);
        assert!(
            data_manager
                .memory_pool
                .remaining_uncompressed_memory_in_bytes()
                < UncompressedInMemoryDataBuffer::memory_size() as isize
        );

        // If there is enough memory to hold n full buffers, n + 1 are needed to spill a buffer.
        insert_data_points(1, &mut data_manager, &model_table_metadata, UNIVARIATE_ID).await;

        // One of the buffers should be spilled due to the memory limit being exceeded.
        assert_eq!(
            data_manager.uncompressed_in_memory_data_buffers.len(),
            number_of_buffers
        );
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 1);
        assert_eq!(data_manager.channels.univariate_data_receiver.len(), 0);

        // The UncompressedDataBuffer should be spilled to univariate id in the uncompressed folder.
        let local_data_folder = Path::new(&data_manager.local_data_folder);
        let uncompressed_path = local_data_folder.join(UNCOMPRESSED_DATA_FOLDER);
        assert_eq!(fs::read_dir(uncompressed_path).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn test_remaining_memory_decremented_when_creating_new_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes();

        insert_data_points(1, &mut data_manager, &model_table_metadata, UNIVARIATE_ID).await;

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
    fn test_remaining_memory_incremented_when_processing_in_memory_buffer() {
        // This test purposely does not use tokio::test to prevent multiple Tokio runtimes.
        let temp_dir = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::new().unwrap());
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            runtime.block_on(create_managers(temp_dir.path()));

        runtime.block_on(insert_data_points(
            *UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            &model_table_metadata,
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
    fn test_remaining_memory_not_incremented_when_processing_on_disk_buffer() {
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

        let spilled_buffer = runtime
            .block_on(UncompressedOnDiskDataBuffer::try_spill(
                0,
                model_table_metadata,
                0,
                temp_dir.path(),
                uncompressed_data,
            ))
            .unwrap();

        data_manager
            .channels
            .univariate_data_sender
            .send(Message::Stop)
            .unwrap();
        data_manager
            .channels
            .univariate_data_sender
            .send(Message::Data(UncompressedDataBuffer::OnDisk(
                spilled_buffer,
            )))
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
            .await
            .unwrap();

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_uncompressed_memory_in_bytes() as usize,
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES + 10000
        )
    }

    #[tokio::test]
    async fn test_decrease_uncompressed_remaining_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(temp_dir.path()).await;

        // Insert data that should be spilled when the remaining memory is decreased.
        insert_data_points(
            1,
            &mut data_manager,
            &model_table_metadata.clone(),
            UNIVARIATE_ID,
        )
        .await;

        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 0);

        data_manager
            .adjust_uncompressed_remaining_memory_in_bytes(
                -data_manager
                    .memory_pool
                    .remaining_uncompressed_memory_in_bytes(),
            )
            .await
            .unwrap();

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_uncompressed_memory_in_bytes(),
            0
        );

        // Insert data that should force the existing data to now be spilled.
        insert_data_points(
            1,
            &mut data_manager,
            &model_table_metadata,
            UNIVARIATE_ID + 1,
        )
        .await;

        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 1);
    }

    /// Insert `count` data points into `data_manager`.
    async fn insert_data_points(
        count: usize,
        data_manager: &mut UncompressedDataManager,
        model_table_metadata: &Arc<ModelTableMetadata>,
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

    /// Create a [`MetadataManager`] with a model table saved to it and an [`UncompressedDataManager`]
    /// with a folder that is deleted once the test is finished.
    async fn create_managers(
        path: &Path,
    ) -> (
        Arc<TableMetadataManager<Sqlite>>,
        UncompressedDataManager,
        Arc<ModelTableMetadata>,
    ) {
        let metadata_manager = Arc::new(
            metadata::try_new_sqlite_table_metadata_manager(path)
                .await
                .unwrap(),
        );

        // Ensure the expected metadata is available through the metadata manager.
        let model_table_metadata = test::model_table_metadata();

        metadata_manager
            .save_model_table_metadata(&model_table_metadata, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag".to_owned()])
            .await
            .unwrap();

        let memory_pool = Arc::new(MemoryPool::new(
            test::MULTIVARIATE_RESERVED_MEMORY_IN_BYTES,
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        ));

        let channels = Arc::new(Channels::new());

        // UncompressedDataManager::try_new() lookup the error bounds for each univariate_id.
        let uncompressed_data_manager = UncompressedDataManager::try_new(
            path.to_path_buf(),
            memory_pool,
            channels,
            metadata_manager.clone(),
            ClusterMode::SingleNode,
            Arc::new(Mutex::new(Metric::new())),
            Arc::new(Mutex::new(Metric::new())),
        )
        .await
        .unwrap();

        (
            metadata_manager,
            uncompressed_data_manager,
            Arc::new(model_table_metadata),
        )
    }
}
