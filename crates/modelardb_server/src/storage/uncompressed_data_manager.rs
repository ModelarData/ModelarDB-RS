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
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_channel::SendError;
use dashmap::DashMap;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::metadata::TableMetadataManager;
use modelardb_common::schemas::COMPRESSED_SCHEMA;
use modelardb_common::storage::DeltaLake;
use modelardb_common::types::{Timestamp, TimestampArray, Value, ValueArray};
use object_store::path::{Path, PathPart};
use sqlx::Sqlite;
use tokio::runtime::Runtime;
use tracing::{debug, error, warn};

use crate::context::Context;
use crate::storage::compressed_data_buffer::CompressedSegmentBatch;
use crate::storage::types::Channels;
use crate::storage::types::MemoryPool;
use crate::storage::types::Message;
use crate::storage::uncompressed_data_buffer::{
    self, IngestedDataBuffer, UncompressedDataBuffer, UncompressedInMemoryDataBuffer,
    UncompressedOnDiskDataBuffer,
};
use crate::storage::{Metric, UNCOMPRESSED_DATA_FOLDER};
use crate::ClusterMode;

/// Stores uncompressed data points temporarily in an in-memory buffer that spills to Apache Parquet
/// files. When an uncompressed data buffer is finished the data is made available for compression.
pub(super) struct UncompressedDataManager {
    /// Path to the folder containing all uncompressed data managed by the
    /// [`StorageEngine`](crate::storage::StorageEngine).
    local_data_folder: Arc<DeltaLake>,
    /// Counter incremented for each [`RecordBatch`] of data points ingested. The value is assigned
    /// to buffers that are created or updated and is used to flush buffers that are no longer used.
    current_batch_index: AtomicU64,
    /// [`UncompressedInMemoryDataBuffers`](UncompressedInMemoryDataBuffer) that are ready to be
    /// filled with ingested data points. In-memory and on-disk buffers are stored separately to
    /// simplify locking when spilling in-memory buffers to disk and reading on-disk buffers back
    /// into memory.
    uncompressed_in_memory_data_buffers: DashMap<u64, UncompressedInMemoryDataBuffer>,
    /// [`UncompressedOnDiskDataBuffers`](UncompressedOnDiskDataBuffer) that must be read back into
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
    /// Metric for the used ingested memory in bytes, updated every time the used memory changes.
    pub(super) used_ingested_memory_metric: Arc<Mutex<Metric>>,
    /// Metric for the used uncompressed memory in bytes, updated every time the used memory changes.
    pub(super) used_uncompressed_memory_metric: Mutex<Metric>,
    /// Metric for the amount of ingested data points, updated every time a new batch of data is ingested.
    pub(super) ingested_data_points_metric: Mutex<Metric>,
    /// Metric for the total used disk space in bytes, updated every time uncompressed data is spilled.
    pub(super) used_disk_space_metric: Arc<Mutex<Metric>>,
}

impl UncompressedDataManager {
    pub(super) fn new(
        local_data_folder: Arc<DeltaLake>,
        memory_pool: Arc<MemoryPool>,
        channels: Arc<Channels>,
        table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
        cluster_mode: ClusterMode,
        used_ingested_memory_metric: Arc<Mutex<Metric>>,
        used_disk_space_metric: Arc<Mutex<Metric>>,
    ) -> Self {
        Self {
            local_data_folder,
            current_batch_index: AtomicU64::new(0),
            uncompressed_in_memory_data_buffers: DashMap::new(),
            uncompressed_on_disk_data_buffers: DashMap::new(),
            channels,
            table_metadata_manager,
            cluster_mode,
            memory_pool,
            used_ingested_memory_metric,
            used_uncompressed_memory_metric: Mutex::new(Metric::new()),
            ingested_data_points_metric: Mutex::new(Metric::new()),
            used_disk_space_metric,
        }
    }

    /// Add references to the [`UncompressedDataBuffers`](UncompressedDataBuffer) currently on disk
    /// to [`UncompressedDataManager`] which immediately will start compressing them.
    pub(super) async fn initialize(&self, context: &Context) -> Result<(), IOError> {
        let mut initial_disk_space = 0;
        let local_data_folder = self.local_data_folder.object_store();

        let mut spilled_buffers =
            local_data_folder.list(Some(&Path::from(UNCOMPRESSED_DATA_FOLDER)));
        while let Some(maybe_spilled_buffer) = spilled_buffers.next().await {
            let spilled_buffer = maybe_spilled_buffer?;
            let path_parts: Vec<PathPart> = spilled_buffer.location.parts().collect();

            // unwrap() is safe since all spilled buffers are partitioned by their tag hash.
            let tag_hash = path_parts.get(1).unwrap().as_ref().parse::<u64>().unwrap();

            // unwrap() is safe since all spilled buffers have a name generated by the system.
            let file_name = path_parts.get(2).unwrap().as_ref();

            // unwrap() is safe as tag_hash can only exist if it is in the metadata database.
            let table_name = context
                .table_metadata_manager
                .tag_hash_to_table_name(tag_hash)
                .await
                .unwrap();

            // unwrap() is safe as data cannot be ingested into a model table that does not exist.
            let model_table_metadata = context
                .model_table_metadata_from_default_database_schema(&table_name)
                .await
                .unwrap()
                .unwrap();

            let buffer = UncompressedOnDiskDataBuffer::try_new(
                tag_hash,
                model_table_metadata,
                self.current_batch_index.load(Ordering::Relaxed),
                local_data_folder.clone(),
                file_name,
            )?;

            initial_disk_space += buffer.disk_size().await;

            self.channels
                .uncompressed_data_sender
                .send(Message::Data(UncompressedDataBuffer::OnDisk(buffer)))
                .map_err(|error| IOError::new(IOErrorKind::BrokenPipe, error))?;
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
                .ingested_data_receiver
                .recv()
                .map_err(|error| error.to_string())?;

            match message {
                Message::Data(ingested_data_buffer) => {
                    runtime
                        .block_on(self.insert_data_points(ingested_data_buffer))
                        .map_err(|error| error.to_string())?;
                }
                Message::Flush => {
                    self.flush_and_log_errors();
                    self.channels
                        .uncompressed_data_sender
                        .send(Message::Flush)
                        .map_err(|error| error.to_string())?;
                }
                Message::Stop => {
                    self.flush_and_log_errors();
                    self.channels
                        .uncompressed_data_sender
                        .send(Message::Stop)
                        .map_err(|error| error.to_string())?;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Insert `ingested_data_buffer` into in-memory buffers managed by the storage engine. Returns
    /// [`String`] if the channel or the metadata database could not be read from.
    async fn insert_data_points(
        &self,
        ingested_data_buffer: IngestedDataBuffer,
    ) -> Result<(), String> {
        let data_points = ingested_data_buffer.data_points;
        let model_table_metadata = ingested_data_buffer.model_table_metadata;

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
        let timestamp_column_array: &TimestampArray = data_points
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

        // Prepare the field columns for iteration.
        let field_column_arrays: Vec<&ValueArray> = model_table_metadata
            .field_column_indices
            .iter()
            .map(|index| data_points.column(*index).as_any().downcast_ref().unwrap())
            .collect();

        // For each data point, compute a hash from the tags and pass the fields to the storage
        // engine so they can be added to the appropriate [`UncompressedDataBuffer`].
        for (index, timestamp) in timestamp_column_array.iter().enumerate() {
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

            let mut values = field_column_arrays.iter().map(|array| array.value(index));

            // unwrap() is safe to use since the timestamps array cannot contain null values.
            buffers_are_spilled |= self
                .insert_data_point(
                    tag_hash,
                    timestamp.unwrap(),
                    &mut values,
                    model_table_metadata.clone(),
                    current_batch_index,
                )
                .await
                .map_err(|error| error.to_string())?
        }

        // Unused buffers are purposely only finished at the end of insert_data_points() so that the
        // buffers required for any of the data points in the current batch are never finished.
        let current_batch_index = self.current_batch_index.fetch_add(1, Ordering::Relaxed);
        self.finish_unused_buffers(current_batch_index)
            .await
            .map_err(|error| error.to_string())?;

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_ingested_memory_metric
            .lock()
            .unwrap()
            .append(-(data_points.get_array_memory_size() as isize), true);

        // Return the memory used by the data points to the pool right before they are de-allocated.
        self.memory_pool
            .adjust_ingested_memory(data_points.get_array_memory_size() as isize);

        // Print a single warning if any buffers are spilled so ingestion can be optimized.
        if buffers_are_spilled {
            warn!("Forced to spill uncompressed buffers. Reduce buffer size or increase memory.");
        }

        Ok(())
    }

    /// Insert a single data point into the in-memory buffer for `tag_hash` if one exists. If the
    /// buffer has been spilled, read it back into memory. If no buffer exists for `tag_hash`,
    /// allocate a new buffer that will be compressed within the error bound in
    /// `model_table_metadata`. Returns [`true`] if a buffer was spilled, [`false`] if not, and
    /// [`IOError`] if the error bound cannot be retrieved from the [`TableMetadataManager`].
    async fn insert_data_point(
        &self,
        tag_hash: u64,
        timestamp: Timestamp,
        values: &mut dyn Iterator<Item = Value>,
        model_table_metadata: Arc<ModelTableMetadata>,
        current_batch_index: u64,
    ) -> Result<bool, IOError> {
        debug!("Add data point at {timestamp} to uncompressed data buffer for {tag_hash}.");

        // Track if any buffers are spilled during ingestion so this information can be returned to
        // insert_data_points() and a warning printed once per batch instead of once per data point.
        let mut buffers_are_spilled = false;

        // Full finished buffers are removed at the end of the function as remove() may deadlock if
        // called when holding any sort of reference into the corresponding map at the same time.
        let mut buffer_is_full = false;

        // Insert the data point into an existing in-memory buffer if one exists.
        let buffer_is_in_memory = if let Some(mut tag_hash_buffer) =
            self.uncompressed_in_memory_data_buffers.get_mut(&tag_hash)
        {
            debug!("Found existing in-memory buffer for {tag_hash}.");
            let uncompressed_in_memory_data_buffer = tag_hash_buffer.value_mut();
            uncompressed_in_memory_data_buffer.insert_data_point(
                current_batch_index,
                timestamp,
                values,
            );
            buffer_is_full = uncompressed_in_memory_data_buffer.is_full();
            true
        } else {
            false
        };

        // Insert the data point into an on-disk buffer if one exists or create an in-memory buffer.
        if !buffer_is_in_memory {
            // No in-memory data buffer exists, so it is necessary to either read a spilled buffer
            // from disk or create a new one. Memory for this is reserved without holding any
            // references into any of the maps as it may be necessary to spill an in-memory buffer
            // to do so. Thus, a combination of remove() and insert() may be called on the maps.
            buffers_are_spilled |= self
                .reserve_uncompressed_memory_for_in_memory_data_buffer(
                    model_table_metadata.field_column_indices.len(),
                )
                .await?;

            // unwrap() is safe as lock() only returns an error if the lock is poisoned.
            let memory_to_reserve = uncompressed_data_buffer::compute_memory_size(
                model_table_metadata.field_column_indices.len(),
            );
            self.used_uncompressed_memory_metric
                .lock()
                .unwrap()
                .append(memory_to_reserve as isize, true);

            // Two ifs are needed until if-let chains is implemented in Rust stable, see eRFC 2497.
            if let Some(tag_hash_buffer) = self.uncompressed_on_disk_data_buffers.get(&tag_hash) {
                let uncompressed_on_disk_data_buffer = tag_hash_buffer.value();

                // Reading the buffer into memory deletes the on-disk buffer's file on disk and
                // read_apache_parquet() cannot take self as an argument due to how it is used.
                // unwrap() is safe as lock() only returns an error if the lock is poisoned.
                let disk_size = uncompressed_on_disk_data_buffer.disk_size().await;
                self.used_disk_space_metric
                    .lock()
                    .unwrap()
                    .append(-(disk_size as isize), true);

                let mut uncompressed_in_memory_data_buffer = uncompressed_on_disk_data_buffer
                    .read_from_apache_parquet(current_batch_index)
                    .await?;

                uncompressed_in_memory_data_buffer.insert_data_point(
                    current_batch_index,
                    timestamp,
                    values,
                );

                buffer_is_full = uncompressed_in_memory_data_buffer.is_full();

                // The read-only reference must be dropped before the map can be modified.
                mem::drop(tag_hash_buffer);
                self.uncompressed_on_disk_data_buffers.remove(&tag_hash);
                self.uncompressed_in_memory_data_buffers
                    .insert(tag_hash, uncompressed_in_memory_data_buffer);
            } else {
                debug!("Creating Buffer for {tag_hash} as none currently exists.");

                let mut uncompressed_in_memory_data_buffer = UncompressedInMemoryDataBuffer::new(
                    tag_hash,
                    model_table_metadata,
                    current_batch_index,
                );

                debug!(
                    "Created buffer for {}. Remaining reserved bytes: {}.",
                    tag_hash,
                    self.memory_pool.remaining_uncompressed_memory_in_bytes(),
                );

                uncompressed_in_memory_data_buffer.insert_data_point(
                    current_batch_index,
                    timestamp,
                    values,
                );

                self.uncompressed_in_memory_data_buffers
                    .insert(tag_hash, uncompressed_in_memory_data_buffer);
            }
        }

        // Transfer the full buffer to the compressor.
        if buffer_is_full {
            debug!("Buffer for {tag_hash} is full, transferring it to the compressor.");

            // unwrap() is safe as this is only reachable if the buffer exists in the HashMap.
            let (_tag_hash, full_uncompressed_in_memory_data_buffer) = self
                .uncompressed_in_memory_data_buffers
                .remove(&tag_hash)
                .unwrap();

            return self
                .channels
                .uncompressed_data_sender
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
    /// the pool. If there are no buffers waiting to be compressed, all the memory for
    /// uncompressed data is used for unfinished uncompressed in-memory data buffers, and it is
    /// necessary to spill one before a new buffer can ever be allocated. To keep the implementation
    /// simple, it spills a random buffer and does not check if the last uncompressed in-memory data
    /// buffer has been read from the channel but is not yet compressed. Returns [`true`] if a
    /// buffer was spilled, [`false`] if not, and [`IOError`] if spilling fails.
    async fn reserve_uncompressed_memory_for_in_memory_data_buffer(
        &self,
        number_of_fields: usize,
    ) -> Result<bool, IOError> {
        // It is not guaranteed that compressing the data buffers in the channel releases any memory
        // as all the data buffers that are waiting to be compressed may all be stored on disk.
        if self.memory_pool.wait_for_uncompressed_memory_until(
            uncompressed_data_buffer::compute_memory_size(number_of_fields),
            || self.channels.uncompressed_data_sender.is_empty(),
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
        // Extract tag_hash but drop the reference to the map element as remove() may deadlock if
        // called when holding any sort of reference into the map.
        let tag_hash = {
            *self
                .uncompressed_in_memory_data_buffers
                .iter()
                .next()
                .ok_or_else(|| IOError::new(IOErrorKind::NotFound, "No in-memory data buffer."))?
                .key()
        };

        // unwrap() is safe as tag_hash was just extracted from the map.
        let mut uncompressed_in_memory_data_buffer = self
            .uncompressed_in_memory_data_buffers
            .remove(&tag_hash)
            .unwrap()
            .1;

        let maybe_uncompressed_on_disk_data_buffer = uncompressed_in_memory_data_buffer
            .spill_to_apache_parquet(self.local_data_folder.object_store())
            .await;

        // If an error occurs the in-memory buffer must be re-added to the map before returning.
        let uncompressed_on_disk_data_buffer = match maybe_uncompressed_on_disk_data_buffer {
            Ok(uncompressed_on_disk_data_buffer) => uncompressed_on_disk_data_buffer,
            Err(error) => {
                self.uncompressed_in_memory_data_buffers
                    .insert(tag_hash, uncompressed_in_memory_data_buffer);
                return Err(error);
            }
        };

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        let freed_memory = uncompressed_in_memory_data_buffer.memory_size();
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
            .insert(tag_hash, uncompressed_on_disk_data_buffer);

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

        // In-memory tag hashes are copied to prevent multiple concurrent borrows to the map.
        let tag_hashes_of_unused_in_memory_buffers = self
            .uncompressed_in_memory_data_buffers
            .iter()
            .filter(|kv| kv.value().is_unused(current_batch_index))
            .map(|kv| *kv.key())
            .collect::<Vec<u64>>();

        for tag_hash in tag_hashes_of_unused_in_memory_buffers {
            // unwrap() is safe as the tag hashes were just extracted from the map.
            let (_tag_hash, uncompressed_in_memory_data_buffer) = self
                .uncompressed_in_memory_data_buffers
                .remove(&tag_hash)
                .unwrap();

            self.channels
                .uncompressed_data_sender
                .send(Message::Data(UncompressedDataBuffer::InMemory(
                    uncompressed_in_memory_data_buffer,
                )))
                .map_err(|error| IOError::new(IOErrorKind::InvalidData, error))?;

            debug!("Finished in-memory buffer for {tag_hash} as it is no longer used.",);
        }

        // On-disk tag hashes are copied to prevent multiple borrows to the map the same time.
        let tag_hashes_of_unused_on_disk_buffers = self
            .uncompressed_on_disk_data_buffers
            .iter()
            .filter(|kv| kv.value().is_unused(current_batch_index))
            .map(|kv| *kv.key())
            .collect::<Vec<u64>>();

        for tag_hash in tag_hashes_of_unused_on_disk_buffers {
            // unwrap() is safe as the tag hashes were just extracted from the map.
            let (_tag_hash, uncompressed_on_disk_data_buffer) = self
                .uncompressed_on_disk_data_buffers
                .remove(&tag_hash)
                .unwrap();

            self.channels
                .uncompressed_data_sender
                .send(Message::Data(UncompressedDataBuffer::OnDisk(
                    uncompressed_on_disk_data_buffer,
                )))
                .map_err(|error| IOError::new(IOErrorKind::InvalidData, error))?;

            debug!("Finished on-disk buffer for {tag_hash} as it is no longer used.",);
        }

        Ok(())
    }

    /// Compress the uncompressed data buffers that the [`UncompressedDataManager`] is currently
    /// managing. Writes a log message if a [`Message`] cannot be sent to
    /// [`CompressedDataManager`](super::CompressedDataManager).
    fn flush_and_log_errors(&self) {
        if let Err(error) = self.flush() {
            error!("Failed to flush data in uncompressed data manager due to: {error}");
        }
    }

    /// Send the uncompressed data buffers that the [`UncompressedDataManager`] is managing to the
    /// compressor. Returns [`SendError`] if a [`Message`] cannot be sent to the compressor.
    fn flush(&self) -> Result<(), SendError<Message<UncompressedDataBuffer>>> {
        // In-memory tag hashes are copied to prevent multiple concurrent borrows to the map.
        let in_memory_tag_hashes: Vec<u64> = self
            .uncompressed_in_memory_data_buffers
            .iter()
            .map(|kv| *kv.key())
            .collect();

        for tag_hash in in_memory_tag_hashes {
            if let Some((_tag_hashes, buffer)) =
                self.uncompressed_in_memory_data_buffers.remove(&tag_hash)
            {
                self.channels
                    .uncompressed_data_sender
                    .send(Message::Data(UncompressedDataBuffer::InMemory(buffer)))?;
            }
        }

        // On-disk tag hashes are copied to prevent multiple concurrent borrows to the map.
        let on_disk_tag_hashes: Vec<u64> = self
            .uncompressed_on_disk_data_buffers
            .iter()
            .map(|kv| *kv.key())
            .collect();

        for tag_hashes in on_disk_tag_hashes {
            if let Some((_tag_hashes, buffer)) =
                self.uncompressed_on_disk_data_buffers.remove(&tag_hashes)
            {
                self.channels
                    .uncompressed_data_sender
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
                .uncompressed_data_receiver
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
    /// [`CompressedDataManager`](super::CompressedDataManager) over a channel. Returns
    /// [`SendError`] if a [`Message`] cannot be sent to
    /// [`CompressedDataManager`](super::CompressedDataManager).
    async fn compress_finished_buffer(
        &self,
        uncompressed_data_buffer: UncompressedDataBuffer,
    ) -> Result<(), SendError<Message<CompressedSegmentBatch>>> {
        let (memory_use, disk_use, maybe_data_points, tag_hash, model_table_metadata) =
            match uncompressed_data_buffer {
                UncompressedDataBuffer::InMemory(mut uncompressed_in_memory_data_buffer) => (
                    uncompressed_in_memory_data_buffer.memory_size(),
                    0,
                    uncompressed_in_memory_data_buffer.record_batch().await,
                    uncompressed_in_memory_data_buffer.tag_hash(),
                    uncompressed_in_memory_data_buffer
                        .model_table_metadata()
                        .clone(),
                ),
                UncompressedDataBuffer::OnDisk(uncompressed_on_disk_data_buffer) => (
                    0,
                    uncompressed_on_disk_data_buffer.disk_size().await,
                    uncompressed_on_disk_data_buffer.record_batch().await,
                    uncompressed_on_disk_data_buffer.tag_hash(),
                    uncompressed_on_disk_data_buffer
                        .model_table_metadata()
                        .clone(),
                ),
            };

        let data_points = maybe_data_points.map_err(|_| {
            SendError(Message::Data(CompressedSegmentBatch::new(
                model_table_metadata.clone(),
                vec![RecordBatch::new_empty(COMPRESSED_SCHEMA.0.clone())],
            )))
        })?;

        let uncompressed_timestamps = modelardb_common::array!(data_points, 0, TimestampArray);

        let compressed_segments = model_table_metadata
            .field_column_indices
            .iter()
            .enumerate()
            .map(|(value_index, field_column_index)| {
                // One is added to value_index as the first array contains the timestamps.
                let uncompressed_values =
                    modelardb_common::array!(data_points, value_index + 1, ValueArray);
                let univariate_id = tag_hash | *field_column_index as u64;
                let error_bound = model_table_metadata.error_bounds[*field_column_index];

                // unwrap() is safe as uncompressed_timestamps and uncompressed_values have the same length.
                modelardb_compression::try_compress(
                    univariate_id,
                    error_bound,
                    uncompressed_timestamps,
                    uncompressed_values,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        self.channels
            .compressed_data_sender
            .send(Message::Data(CompressedSegmentBatch::new(
                model_table_metadata.clone(),
                compressed_segments,
            )))?;

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

        // Add the size of the uncompressed buffer back to the remaining reserved bytes.
        self.memory_pool
            .adjust_uncompressed_memory(memory_use as isize);

        Ok(())
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

    use std::sync::Arc;

    use datafusion::arrow::array::StringBuilder;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::arrow::record_batch::RecordBatch;
    use modelardb_common::metadata;
    use modelardb_common::schemas::UNCOMPRESSED_SCHEMA;
    use modelardb_common::test;
    use modelardb_common::types::{TimestampBuilder, ValueBuilder};
    use object_store::local::LocalFileSystem;
    use ringbuf::traits::observer::Observer;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    use crate::storage::UNCOMPRESSED_DATA_BUFFER_CAPACITY;
    use crate::DataFolders;

    const TAG_HASH: u64 = 9674644176454356993;

    // Tests for UncompressedDataManager.
    #[tokio::test]
    async fn test_can_compress_existing_on_disk_data_buffers_when_initializing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local_data_folder =
            Arc::new(DeltaLake::try_from_local_path(temp_dir.path().to_str().unwrap()).unwrap());

        // Create a context with a storage engine.
        let context = Arc::new(
            Context::try_new(
                Arc::new(Runtime::new().unwrap()),
                DataFolders {
                    local_data_folder: local_data_folder.clone(),
                    remote_data_folder: None,
                    query_data_folder: local_data_folder,
                },
                ClusterMode::SingleNode,
            )
            .await
            .unwrap(),
        );

        // Create a table in the context.
        context
            .parse_and_create_table(test::MODEL_TABLE_SQL, &context)
            .await
            .unwrap();

        // Ingest a single data point and sleep to allow the ingestion thread to finish.
        let mut storage_engine = context.storage_engine.write().await;
        let model_table_metadata = Arc::new(test::model_table_metadata());
        let data = uncompressed_data(1, model_table_metadata.schema.clone());

        storage_engine
            .insert_data_points(model_table_metadata, data)
            .await
            .unwrap();

        sleep(Duration::from_millis(250)).await;

        storage_engine
            .uncompressed_data_manager
            .spill_in_memory_data_buffer()
            .await
            .unwrap();

        // Compress the spilled buffer and sleep to allow the compression thread to finish.
        assert!(storage_engine.initialize(&context).await.is_ok());
        sleep(Duration::from_millis(250)).await;

        // The spilled buffer should be deleted and the content should be compressed.
        let spilled_buffers = storage_engine
            .uncompressed_data_manager
            .local_data_folder
            .object_store()
            .list(Some(&Path::from(UNCOMPRESSED_DATA_FOLDER)))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(spilled_buffers.len(), 0);
        assert_eq!(
            storage_engine
                .compressed_data_manager
                .used_compressed_memory_metric
                .lock()
                .unwrap()
                .values()
                .occupied_len(),
            1
        );
    }

    #[tokio::test]
    async fn test_can_insert_record_batch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, data_manager, model_table_metadata) =
            create_managers(&temp_dir).await;

        let data = uncompressed_data(1, model_table_metadata.schema.clone());
        let ingested_data_buffer = IngestedDataBuffer::new(model_table_metadata, data);

        data_manager
            .insert_data_points(ingested_data_buffer)
            .await
            .unwrap();

        // Only a single data buffer is created despite the inserted data containing two field columns.
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(
            data_manager
                .ingested_data_points_metric
                .lock()
                .unwrap()
                .values()
                .occupied_len(),
            1
        );
    }

    #[tokio::test]
    async fn test_can_insert_record_batch_with_multiple_data_points() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, data_manager, model_table_metadata) =
            create_managers(&temp_dir).await;

        let data = uncompressed_data(2, model_table_metadata.schema.clone());
        let ingested_data_buffer = IngestedDataBuffer::new(model_table_metadata, data);

        data_manager
            .insert_data_points(ingested_data_buffer)
            .await
            .unwrap();

        // Since the tag is different for the two data points, two data buffers should be created.
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 2);
        assert_eq!(
            data_manager
                .ingested_data_points_metric
                .lock()
                .unwrap()
                .values()
                .occupied_len(),
            1
        );
    }

    #[tokio::test]
    async fn test_remaining_ingested_memory_increased_after_processing_record_batch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, data_manager, model_table_metadata) =
            create_managers(&temp_dir).await;

        let data = uncompressed_data(2, model_table_metadata.schema.clone());
        let data_size = data.get_array_memory_size();

        // Simulate StorageEngine decrementing ingested memory when receiving ingested data.
        let ingested_memory_before = data_manager
            .memory_pool
            .remaining_ingested_memory_in_bytes();

        data_manager
            .used_ingested_memory_metric
            .lock()
            .unwrap()
            .append(data.get_array_memory_size() as isize, true);

        let ingested_data_buffer = IngestedDataBuffer::new(model_table_metadata, data);

        data_manager
            .insert_data_points(ingested_data_buffer)
            .await
            .unwrap();

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_ingested_memory_in_bytes(),
            ingested_memory_before + data_size as isize
        );

        assert_eq!(
            data_manager
                .used_ingested_memory_metric
                .lock()
                .unwrap()
                .values()
                .occupied_len(),
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
            create_managers(&temp_dir).await;

        insert_data_points(1, &mut data_manager, &model_table_metadata, TAG_HASH).await;

        assert!(data_manager
            .uncompressed_in_memory_data_buffers
            .contains_key(&TAG_HASH));
        assert_eq!(
            data_manager
                .uncompressed_in_memory_data_buffers
                .get(&TAG_HASH)
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_can_insert_data_point_into_existing_in_memory_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(&temp_dir).await;

        insert_data_points(1, &mut data_manager, &model_table_metadata, TAG_HASH).await;
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 0);

        insert_data_points(1, &mut data_manager, &model_table_metadata, TAG_HASH).await;
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 0);

        assert!(data_manager
            .uncompressed_in_memory_data_buffers
            .contains_key(&TAG_HASH));
        assert_eq!(
            data_manager
                .uncompressed_in_memory_data_buffers
                .get(&TAG_HASH)
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn test_can_insert_data_point_into_existing_on_disk_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(&temp_dir).await;

        insert_data_points(1, &mut data_manager, &model_table_metadata, TAG_HASH).await;
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 0);

        data_manager.spill_in_memory_data_buffer().await.unwrap();
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 0);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 1);

        insert_data_points(1, &mut data_manager, &model_table_metadata, TAG_HASH).await;
        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 0);

        assert!(data_manager
            .uncompressed_in_memory_data_buffers
            .contains_key(&TAG_HASH));
        assert_eq!(
            data_manager
                .uncompressed_in_memory_data_buffers
                .get(&TAG_HASH)
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn test_will_finish_unused_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, data_manager, model_table_metadata) =
            create_managers(&temp_dir).await;

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

        let ingested_data_buffer = IngestedDataBuffer::new(model_table_metadata.clone(), data);
        data_manager
            .insert_data_points(ingested_data_buffer)
            .await
            .unwrap();

        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.channels.uncompressed_data_receiver.len(), 0);
        assert_eq!(
            data_manager
                .uncompressed_in_memory_data_buffers
                .get(&4940964593210619904)
                .unwrap()
                .len(),
            3
        );

        // Insert using insert_data_points() to finish unused buffers.
        let empty_record_batch = RecordBatch::new_empty(model_table_metadata.schema.clone());
        let ingested_data_buffer =
            IngestedDataBuffer::new(model_table_metadata, empty_record_batch);

        data_manager
            .insert_data_points(ingested_data_buffer)
            .await
            .unwrap();

        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 0);
        assert_eq!(data_manager.channels.uncompressed_data_receiver.len(), 1);
    }

    #[tokio::test]
    async fn test_can_get_finished_uncompressed_data_buffer_when_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(&temp_dir).await;
        insert_data_points(
            *UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            &model_table_metadata,
            TAG_HASH,
        )
        .await;

        assert!(data_manager
            .channels
            .uncompressed_data_receiver
            .try_recv()
            .is_ok());
    }

    #[tokio::test]
    async fn test_can_get_multiple_finished_uncompressed_data_buffers_when_multiple_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(&temp_dir).await;

        insert_data_points(
            *UNCOMPRESSED_DATA_BUFFER_CAPACITY * 2,
            &mut data_manager,
            &model_table_metadata,
            TAG_HASH,
        )
        .await;

        assert!(data_manager
            .channels
            .uncompressed_data_receiver
            .try_recv()
            .is_ok());

        assert!(data_manager
            .channels
            .uncompressed_data_receiver
            .try_recv()
            .is_ok());
    }

    #[tokio::test]
    async fn test_cannot_get_finished_uncompressed_data_buffers_when_none_are_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, data_manager, _model_table_metadata) =
            create_managers(&temp_dir).await;

        assert!(data_manager
            .channels
            .uncompressed_data_receiver
            .try_recv()
            .is_err());
    }

    #[tokio::test]
    async fn test_spill_random_uncompressed_data_buffer_to_disk_if_out_of_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(&temp_dir).await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes() as usize;
        let number_of_fields = model_table_metadata.field_column_indices.len();

        // Insert messages into the storage engine until all the memory is used and the next
        // message inserted would block the thread until the data messages have been processed.
        let number_of_buffers =
            reserved_memory / uncompressed_data_buffer::compute_memory_size(number_of_fields);
        for tag_hash in 0..number_of_buffers {
            // Allocate many buffers that are never finished.
            insert_data_points(
                1,
                &mut data_manager,
                &model_table_metadata.clone(),
                tag_hash as u64,
            )
            .await;
        }

        // The buffers should be in-memory and there should not be enough memory left for one more.
        assert_eq!(
            data_manager.uncompressed_in_memory_data_buffers.len(),
            number_of_buffers
        );
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 0);
        assert_eq!(data_manager.channels.uncompressed_data_receiver.len(), 0);
        assert!(
            data_manager
                .memory_pool
                .remaining_uncompressed_memory_in_bytes()
                < uncompressed_data_buffer::compute_memory_size(number_of_fields) as isize
        );

        // If there is enough memory to hold n full buffers, n + 1 are needed to spill a buffer.
        insert_data_points(1, &mut data_manager, &model_table_metadata, TAG_HASH).await;

        // One of the buffers should be spilled due to the memory limit being exceeded.
        assert_eq!(
            data_manager.uncompressed_in_memory_data_buffers.len(),
            number_of_buffers
        );
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 1);
        assert_eq!(data_manager.channels.uncompressed_data_receiver.len(), 0);

        // The UncompressedDataBuffer should be spilled to tag hash in the uncompressed folder.
        let spilled_buffers = data_manager
            .local_data_folder
            .object_store()
            .list(Some(&Path::from(UNCOMPRESSED_DATA_FOLDER)))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(spilled_buffers.len(), 1);
    }

    #[tokio::test]
    async fn test_remaining_memory_decremented_when_creating_new_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            create_managers(&temp_dir).await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes();

        insert_data_points(1, &mut data_manager, &model_table_metadata, TAG_HASH).await;

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
                .occupied_len(),
            1
        );
    }

    #[test]
    fn test_remaining_memory_incremented_when_processing_in_memory_buffer() {
        // This test purposely does not use tokio::test to prevent multiple Tokio runtimes.
        let temp_dir = tempfile::tempdir().unwrap();
        let runtime = Arc::new(Runtime::new().unwrap());
        let (_metadata_manager, mut data_manager, model_table_metadata) =
            runtime.block_on(create_managers(&temp_dir));

        runtime.block_on(insert_data_points(
            *UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            &model_table_metadata,
            TAG_HASH,
        ));

        let remaining_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes();

        data_manager
            .channels
            .uncompressed_data_sender
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
                .occupied_len(),
            2
        );
    }

    #[test]
    fn test_remaining_memory_not_incremented_when_processing_on_disk_buffer() {
        // This test purposely does not use tokio::test to prevent multiple Tokio runtimes.
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        let runtime = Arc::new(Runtime::new().unwrap());
        let (_metadata_manager, data_manager, model_table_metadata) =
            runtime.block_on(create_managers(&temp_dir));

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
                object_store,
                uncompressed_data,
            ))
            .unwrap();

        data_manager
            .channels
            .uncompressed_data_sender
            .send(Message::Stop)
            .unwrap();
        data_manager
            .channels
            .uncompressed_data_sender
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
            create_managers(&temp_dir).await;

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
            create_managers(&temp_dir).await;

        // Insert data that should be spilled when the remaining memory is decreased.
        insert_data_points(
            1,
            &mut data_manager,
            &model_table_metadata.clone(),
            TAG_HASH,
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
        insert_data_points(1, &mut data_manager, &model_table_metadata, TAG_HASH + 1).await;

        assert_eq!(data_manager.uncompressed_in_memory_data_buffers.len(), 1);
        assert_eq!(data_manager.uncompressed_on_disk_data_buffers.len(), 1);
    }

    /// Insert `count` data points into `data_manager`.
    async fn insert_data_points(
        count: usize,
        data_manager: &mut UncompressedDataManager,
        model_table_metadata: &Arc<ModelTableMetadata>,
        tag_hash: u64,
    ) {
        let values: &[Value] = &[37.0, 73.0];
        let current_batch_index = 0;

        for i in 0..count {
            data_manager
                .insert_data_point(
                    tag_hash,
                    i as i64,
                    &mut values.iter().copied(),
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
        temp_dir: &TempDir,
    ) -> (
        Arc<TableMetadataManager<Sqlite>>,
        UncompressedDataManager,
        Arc<ModelTableMetadata>,
    ) {
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let metadata_manager = Arc::new(
            metadata::try_new_sqlite_table_metadata_manager(&object_store)
                .await
                .unwrap(),
        );

        let local_data_folder =
            Arc::new(DeltaLake::try_from_local_path(temp_dir.path().to_str().unwrap()).unwrap());

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
            test::INGESTED_RESERVED_MEMORY_IN_BYTES,
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        ));

        let channels = Arc::new(Channels::new());

        // UncompressedDataManager::try_new() lookup the error bounds for each tag hash.
        let uncompressed_data_manager = UncompressedDataManager::new(
            local_data_folder,
            memory_pool,
            channels,
            metadata_manager.clone(),
            ClusterMode::SingleNode,
            Arc::new(Mutex::new(Metric::new())),
            Arc::new(Mutex::new(Metric::new())),
        );

        (
            metadata_manager,
            uncompressed_data_manager,
            Arc::new(model_table_metadata),
        )
    }
}
