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

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{Error as IOError, ErrorKind as IOErrorKind};
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::types::{ErrorBound, Timestamp, TimestampArray, Value, ValueArray};
use tokio::sync::RwLock;
use tracing::debug;

use crate::metadata::MetadataManager;
use crate::storage::types::MemoryPool;
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
    /// Counter incremented for each [`RecordBatch`] of data points ingested. The value is assigned
    /// to buffers that are created or updated and is used to flush buffers that are no longer used.
    current_batch_index: u64,
    /// The [`UncompressedDataBuffers`](UncompressedDataBuffer) currently being filled with ingested
    /// data points.
    active_uncompressed_data_buffers: HashMap<u64, UncompressedInMemoryDataBuffer>,
    /// FIFO queue of full [`UncompressedDataBuffers`](UncompressedDataBuffer) that are ready for
    /// compression.
    finished_uncompressed_data_buffers: VecDeque<Box<dyn UncompressedDataBuffer>>,
    /// Track how much memory is left for storing uncompressed and compressed data.
    memory_pool: Arc<MemoryPool>,
    // TODO: This is a temporary field used to fix existing tests. Remove when configuration component is changed.
    /// If this is true, compress finished buffers directly instead of queueing them.
    compress_directly: bool,
    /// Metric for the used uncompressed memory in bytes, updated every time the used memory changes.
    pub(super) used_uncompressed_memory_metric: Metric,
    /// Metric for the amount of ingested data points, updated every time a new batch of data is ingested.
    pub(super) ingested_data_points_metric: Metric,
    /// Metric for the total used disk space in bytes, updated every time uncompressed data is spilled.
    pub used_disk_space_metric: Arc<RwLock<Metric>>,
}

impl UncompressedDataManager {
    /// Return an [`UncompressedDataManager`] if the required folder can be created in
    /// `local_data_folder` and an [`UncompressedOnDiskDataBuffer`] can be initialized for all
    /// Apache Parquet files containing uncompressed data points in `local_data_store`, otherwise
    /// [`IOError`] is returned.
    pub(super) async fn try_new(
        local_data_folder: PathBuf,
        memory_pool: Arc<MemoryPool>,
        metadata_manager: &MetadataManager,
        compress_directly: bool,
        used_disk_space_metric: Arc<RwLock<Metric>>,
    ) -> Result<Self, IOError> {
        // Ensure the folder required by the uncompressed data manager exists.
        let local_uncompressed_data_folder = local_data_folder.join(UNCOMPRESSED_DATA_FOLDER);
        fs::create_dir_all(&local_uncompressed_data_folder)?;

        // Add references to the uncompressed data buffers currently on disk.
        let mut finished_data_buffers: VecDeque<Box<dyn UncompressedDataBuffer>> = VecDeque::new();
        for maybe_folder_dir_entry in local_uncompressed_data_folder.read_dir()? {
            let folder_dir_entry = maybe_folder_dir_entry?;

            let univariate_id = folder_dir_entry
                .file_name()
                .to_str()
                .ok_or_else(|| IOError::new(IOErrorKind::InvalidData, "Path is not valid UTF-8."))?
                .parse::<u64>()
                .map_err(|error| IOError::new(IOErrorKind::InvalidData, error))?;

            // unwrap() is safe as univariate_id can only exist if it is in the metadata database.
            let error_bound = metadata_manager.error_bound(univariate_id).await.unwrap();

            for maybe_file_dir_entry in folder_dir_entry.path().read_dir()? {
                finished_data_buffers.push_back(Box::new(UncompressedOnDiskDataBuffer::try_new(
                    univariate_id,
                    error_bound,
                    maybe_file_dir_entry?.path(),
                )?));
            }
        }

        // Record the used disk space of the uncompressed data buffers currently on disk.
        let initial_disk_space: usize = finished_data_buffers
            .iter()
            .map(|buffer| buffer.disk_size())
            .sum();
        used_disk_space_metric
            .write()
            .await
            .append(initial_disk_space as isize, true);

        Ok(Self {
            local_data_folder,
            current_batch_index: 0,
            active_uncompressed_data_buffers: HashMap::new(),
            finished_uncompressed_data_buffers: finished_data_buffers,
            memory_pool,
            compress_directly,
            used_uncompressed_memory_metric: Metric::new(),
            ingested_data_points_metric: Metric::new(),
            used_disk_space_metric,
        })
    }

    // TODO: When the compression component is changed, this function should return Ok or Err.
    /// Split `data_points` and insert them into the in-memory buffer. The multivariate time series
    /// in `data_points` are first split into multiple univariate time series based on
    /// `model_table`. These univariate time series are then inserted into the storage engine.
    /// Return a list of buffers that were compressed as a result of inserting the data points with
    /// their corresponding univariate id if the data was successfully inserted, otherwise return
    /// [`Err`].
    pub(super) async fn insert_data_points(
        &mut self,
        metadata_manager: &MetadataManager,
        model_table_metadata: &ModelTableMetadata,
        data_points: &RecordBatch,
    ) -> Result<Vec<(u64, RecordBatch)>, String> {
        debug!(
            "Received record batch with {} data points for the table '{}'.",
            data_points.num_rows(),
            model_table_metadata.name
        );

        // Record the amount of ingested data points.
        self.ingested_data_points_metric
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

        let mut compressed_buffers = vec![];

        // For each data point, generate a hash from the tags, extract the individual fields, and
        // insert them into the storage engine.
        for (index, timestamp) in timestamps.iter().enumerate() {
            let tag_values: Vec<String> = tag_column_arrays
                .iter()
                .map(|array| array.value(index).to_string())
                .collect();

            let tag_hash = metadata_manager
                .lookup_or_compute_tag_hash(model_table_metadata, &tag_values)
                .await
                .map_err(|error| format!("Tag hash could not be saved: {error}"))?;

            // For each field column, generate the 64-bit univariate id, and append the current
            // timestamp and the field's value into the in-memory buffer for the univariate id.
            for (field_index, field_column_array) in &field_column_arrays {
                let univariate_id = tag_hash | *field_index as u64;
                let value = field_column_array.value(index);
                let error_bound = model_table_metadata.error_bounds[*field_index];

                // TODO: When the compression component is changed, just insert the data points.
                // unwrap() is safe to use since the timestamps array cannot contain null values.
                if let Some(buffer) = self
                    .insert_data_point(univariate_id, timestamp.unwrap(), value, error_bound)
                    .await
                    .map_err(|error| error.to_string())?
                {
                    compressed_buffers.push((univariate_id, buffer));
                };
            }
        }

        // Unused buffers are purposely only finished at the end of insert_data_points() so that the
        // buffers required for any of the data points in the current batch are never finished.
        let mut compressed_unused_buffers = self
            .finish_unused_buffers()
            .await
            .map_err(|error| error.to_string())?;
        compressed_buffers.append(&mut compressed_unused_buffers);

        self.current_batch_index += 1;

        Ok(compressed_buffers)
    }

    // TODO: Update to handle finished buffers when compress_finished_buffer is removed.
    /// Compress the uncompressed data buffer that the [`UncompressedDataManager`] is currently
    /// managing, and return the compressed buffers and their univariate ids. Returns [`IOError`] if
    /// the error bound cannot be retrieved from the [`MetadataManager`].
    pub(super) async fn flush(&mut self) -> Result<Vec<(u64, RecordBatch)>, IOError> {
        // The univariate ids are copied to not have multiple borrows to self at the same time.
        let univariate_ids: Vec<u64> = self
            .active_uncompressed_data_buffers
            .keys()
            .cloned()
            .collect();

        let mut compressed_buffers = vec![];
        for univariate_id in univariate_ids {
            let buffer = self
                .active_uncompressed_data_buffers
                .remove(&univariate_id)
                .unwrap();
            let record_batch = self.compress_finished_buffer(univariate_id, buffer).await?;
            compressed_buffers.push((univariate_id, record_batch));
        }
        Ok(compressed_buffers)
    }

    // TODO: When the compression component is changed, this function should return Ok or Err.
    /// Insert a single data point into the in-memory buffer for `univariate_id` if one exists. If
    /// no in-memory buffer exist for `univariate_id`, allocate a new buffer that will be compressed
    /// within `error_bound`. Return a compressed data buffer if the inserted data point made the
    /// buffer full and thus finished, otherwise return [`Ok<None>`]. Returns [`IOError`] if the
    /// error bound cannot be retrieved from the [`MetadataManager`].
    async fn insert_data_point(
        &mut self,
        univariate_id: u64,
        timestamp: Timestamp,
        value: Value,
        error_bound: ErrorBound,
    ) -> Result<Option<RecordBatch>, IOError> {
        debug!(
            "Inserting data point ({}, {}) into uncompressed data buffer for {}.",
            timestamp, value, univariate_id
        );

        if let Some(buffer) = self
            .active_uncompressed_data_buffers
            .get_mut(&univariate_id)
        {
            debug!("Found existing buffer for {}.", univariate_id);
            buffer.insert_data(self.current_batch_index, timestamp, value);

            if buffer.is_full() {
                debug!(
                    "Buffer for {} is full, moving it to the queue of finished buffers.",
                    univariate_id
                );

                // unwrap() is safe as this is only reachable if the buffer exists in the HashMap.
                let full_in_memory_data_buffer = self
                    .active_uncompressed_data_buffers
                    .remove(&univariate_id)
                    .unwrap();

                return self
                    .finish_buffer(univariate_id, full_in_memory_data_buffer)
                    .await;
            }
        } else {
            debug!(
                "Could not find buffer for {}. Creating Buffer.",
                univariate_id
            );

            // If there is not enough memory for a new buffer, spill a finished in-memory buffer.
            while !self
                .memory_pool
                .try_reserve_uncompressed_memory(UncompressedInMemoryDataBuffer::memory_size())
            {
                self.spill_finished_buffer().await;
            }

            // Create a new buffer and reduce the remaining amount of reserved memory by its size.
            let mut buffer = UncompressedInMemoryDataBuffer::new(
                univariate_id,
                self.current_batch_index,
                error_bound,
            );

            let used_memory = UncompressedInMemoryDataBuffer::memory_size();
            self.used_uncompressed_memory_metric
                .append(used_memory as isize, true);

            debug!(
                "Created buffer for {}. Remaining reserved bytes: {}.",
                self.memory_pool.remaining_uncompressed_memory_in_bytes(),
                univariate_id
            );

            buffer.insert_data(self.current_batch_index, timestamp, value);
            self.active_uncompressed_data_buffers
                .insert(univariate_id, buffer);
        }

        Ok(None)
    }

    /// Remove the oldest finished [`UncompressedDataBuffer`] from the queue and return it. Returns
    /// [`None`] if there are no finished [`UncompressedDataBuffers`](UncompressedDataBuffer) in the
    /// queue.
    pub(super) async fn finished_data_buffer(&mut self) -> Option<Box<dyn UncompressedDataBuffer>> {
        if let Some(uncompressed_data_buffer) = self.finished_uncompressed_data_buffers.pop_front()
        {
            // Add the memory size of the removed buffer back to the remaining bytes and record the change.
            self.memory_pool
                .free_uncompressed_memory(uncompressed_data_buffer.memory_size());
            self.used_uncompressed_memory_metric
                .append(-(uncompressed_data_buffer.memory_size() as isize), true);

            // Since the buffer was potentially on disk, record the potential change to the used disk space.
            self.used_disk_space_metric
                .write()
                .await
                .append(-(uncompressed_data_buffer.disk_size() as isize), true);

            Some(uncompressed_data_buffer)
        } else {
            None
        }
    }

    // TODO: When the compression component is changed, this function should return Ok or Err.
    /// Finish active in-memory data buffers that are no longer used to free memory.
    async fn finish_unused_buffers(&mut self) -> Result<Vec<(u64, RecordBatch)>, IOError> {
        debug!("Freeing memory by finishing all in-memory buffers that are no longer used.");

        // Extract the univariate ids of the unused buffers and then delete the unused buffers as it
        // is not possible to iterate and delete together until HashMap::drain_filter() is stable.
        let mut univariate_ids_of_unused_buffers =
            Vec::with_capacity(self.active_uncompressed_data_buffers.len());

        for (univariate_id, buffer) in &self.active_uncompressed_data_buffers {
            if buffer.is_unused(self.current_batch_index) {
                univariate_ids_of_unused_buffers.push(*univariate_id);
            }
        }

        let mut compressed_buffers = Vec::with_capacity(univariate_ids_of_unused_buffers.len());

        for univariate_id in univariate_ids_of_unused_buffers {
            // unwrap() is safe as the univariate_ids were just extracted from the map.
            let buffer = self
                .active_uncompressed_data_buffers
                .remove(&univariate_id)
                .unwrap();

            if let Some(record_batch) = self.finish_buffer(univariate_id, buffer).await? {
                compressed_buffers.push((univariate_id, record_batch));

                debug!(
                    "Finished in-memory buffer for {} as it is no longer used.",
                    univariate_id
                );
            }
        }

        Ok(compressed_buffers)
    }

    // TODO: Update this when compression component is changed.
    /// Finish `in_memory_data_buffer` for the time series with `univariate_id`.
    async fn finish_buffer(
        &mut self,
        univariate_id: u64,
        in_memory_data_buffer: UncompressedInMemoryDataBuffer,
    ) -> Result<Option<RecordBatch>, IOError> {
        // TODO: Currently we directly compress a buffer when it is finished. This should be
        //       changed to queue the buffer and let the compression component retrieve the
        //       finished buffer and insert it back when compressed.
        if self.compress_directly {
            Ok(Some(
                self.compress_finished_buffer(univariate_id, in_memory_data_buffer)
                    .await?,
            ))
        } else {
            self.finished_uncompressed_data_buffers
                .push_back(Box::new(in_memory_data_buffer));
            Ok(None)
        }
    }

    // TODO: Remove this when compression component is changed.
    /// Compress the full [`UncompressedInMemoryDataBuffer`] given as `in_memory_data_buffer` and
    /// return the resulting compressed and merged segments as a [`RecordBatch`]. Returns
    /// [`IOError`] if the error bound cannot be retrieved from the [`MetadataManager`].
    async fn compress_finished_buffer(
        &mut self,
        univariate_id: u64,
        mut in_memory_data_buffer: UncompressedInMemoryDataBuffer,
    ) -> Result<RecordBatch, IOError> {
        // Add the size of the segment back to the remaining reserved bytes and record the change.
        let freed_memory = UncompressedInMemoryDataBuffer::memory_size();
        self.memory_pool.free_uncompressed_memory(freed_memory);
        self.used_uncompressed_memory_metric
            .append(-(freed_memory as isize), true);

        // unwrap() is safe to use since record_batch() can only fail for spilled buffers.
        let data_points = in_memory_data_buffer.record_batch().await.unwrap();
        let uncompressed_timestamps = modelardb_common::array!(data_points, 0, TimestampArray);
        let uncompressed_values = modelardb_common::array!(data_points, 1, ValueArray);
        let error_bound = in_memory_data_buffer.error_bound();

        // unwrap() is safe to use since uncompressed_timestamps and uncompressed_values have the same length.
        Ok(modelardb_compression::try_compress(
            univariate_id,
            error_bound,
            uncompressed_timestamps,
            uncompressed_values,
        )
        .unwrap())
    }

    /// Spill the first [`UncompressedInMemoryDataBuffer`] in the queue of
    /// [`UncompressedDataBuffers`](UncompressedDataBuffer). If no
    /// [`UncompressedInMemoryDataBuffers`](UncompressedInMemoryDataBuffer) could be found,
    /// [`panic`](std::panic!).
    async fn spill_finished_buffer(&mut self) {
        debug!("Not enough memory to create segment. Spilling an already finished buffer.");

        // Iterate through the finished buffers to find a buffer that is backed by memory.
        for finished in self.finished_uncompressed_data_buffers.iter_mut() {
            if let Ok(uncompressed_on_disk_data_buffer) = finished
                .spill_to_apache_parquet(self.local_data_folder.as_path())
                .await
            {
                // Add the size of the in-memory data buffer back to the remaining reserved bytes.
                let freed_memory = UncompressedInMemoryDataBuffer::memory_size();
                self.memory_pool.free_uncompressed_memory(freed_memory);
                self.used_uncompressed_memory_metric
                    .append(-(freed_memory as isize), true);

                // Record the used disk space of the spilled finished buffer.
                self.used_disk_space_metric
                    .write()
                    .await
                    .append(uncompressed_on_disk_data_buffer.disk_size() as isize, true);

                debug!(
                    "Spilled '{:?}'. Remaining reserved bytes: {}.",
                    finished,
                    self.memory_pool.remaining_uncompressed_memory_in_bytes()
                );

                *finished = Box::new(uncompressed_on_disk_data_buffer);
                return;
            }
        }

        // TODO: All uncompressed and compressed data should be saved to disk first.
        // If not able to find any in-memory finished segments, panic!() is the only option.
        panic!("Not enough reserved memory for the necessary uncompressed buffers.");
    }

    /// Change the amount of memory for uncompressed data in bytes according to `value_change`. If
    /// less than zero bytes remain, spill uncompressed data to disk. If all the data is spilled
    /// successfully return [`Ok`], otherwise return [`IOError`].
    pub(super) async fn adjust_uncompressed_remaining_memory_in_bytes(
        &mut self,
        value_change: isize,
    ) {
        self.memory_pool.adjust_uncompressed_memory(value_change);

        while self.memory_pool.remaining_uncompressed_memory_in_bytes() < 0 {
            self.spill_finished_buffer().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrowPrimitiveType, StringBuilder};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use modelardb_common::types::{ArrowTimestamp, ArrowValue, TimestampBuilder, ValueBuilder};
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
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let mut buffer =
            UncompressedInMemoryDataBuffer::new(UNIVARIATE_ID, CURRENT_BATCH_INDEX, error_bound);
        buffer.insert_data(CURRENT_BATCH_INDEX, 100, 10.0);
        buffer
            .spill_to_apache_parquet(temp_dir.path())
            .await
            .unwrap();

        // The uncompressed data buffer should be referenced by the uncompressed data manager.
        let (_metadata_manager, data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;
        assert_eq!(data_manager.finished_uncompressed_data_buffers.len(), 1)
    }

    #[tokio::test]
    async fn test_can_insert_record_batch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;

        let (model_table_metadata, data) = uncompressed_data(1);
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();
        data_manager
            .insert_data_points(&metadata_manager, &model_table_metadata, &data)
            .await
            .unwrap();

        // Two separate builders are created since the inserted data has two field columns.
        assert_eq!(
            data_manager.active_uncompressed_data_buffers.keys().len(),
            2
        );

        assert_eq!(data_manager.ingested_data_points_metric.values().len(), 1);
    }

    #[tokio::test]
    async fn test_can_insert_record_batch_with_multiple_data_points() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;

        let (model_table_metadata, data) = uncompressed_data(2);
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();
        data_manager
            .insert_data_points(&metadata_manager, &model_table_metadata, &data)
            .await
            .unwrap();

        // Since the tag is different for each data point, 4 separate buffers should be created.
        assert_eq!(
            data_manager.active_uncompressed_data_buffers.keys().len(),
            4
        );

        assert_eq!(data_manager.ingested_data_points_metric.values().len(), 1);
    }

    /// Create a record batch with data that resembles uncompressed data with a single tag and two
    /// field columns. The returned data has `row_count` rows, with a different tag for each row.
    /// Also create model table metadata for a model table that matches the created data.
    fn uncompressed_data(row_count: usize) -> (ModelTableMetadata, RecordBatch) {
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

        let model_table_metadata = ModelTableMetadata::try_new(
            "model_table".to_owned(),
            query_schema,
            error_bounds,
            generated_columns,
        )
        .unwrap();

        (model_table_metadata, data)
    }

    #[tokio::test]
    async fn test_can_insert_data_point_into_new_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;
        insert_data_points(1, &mut data_manager, UNIVARIATE_ID).await;

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
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;
        insert_data_points(2, &mut data_manager, UNIVARIATE_ID).await;

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
        let (metadata_manager, mut data_manager, model_table_metadata) =
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

        let record_batch = RecordBatch::try_new(
            model_table_metadata.schema.clone(),
            vec![
                Arc::new(timestamp.finish()),
                Arc::new(field.finish()),
                Arc::new(tag.finish()),
            ],
        )
        .unwrap();

        data_manager
            .insert_data_points(&metadata_manager, &model_table_metadata, &record_batch)
            .await
            .unwrap();

        assert_eq!(data_manager.active_uncompressed_data_buffers.len(), 1);
        assert_eq!(data_manager.finished_uncompressed_data_buffers.len(), 0);
        assert_eq!(
            data_manager
                .active_uncompressed_data_buffers
                .values()
                .next()
                .unwrap()
                .len(),
            3
        );

        // Insert using insert_data_points() to finish unused buffers.
        let empty_record_batch = RecordBatch::new_empty(model_table_metadata.schema.clone());

        data_manager
            .insert_data_points(
                &metadata_manager,
                &model_table_metadata,
                &empty_record_batch,
            )
            .await
            .unwrap();

        assert_eq!(data_manager.active_uncompressed_data_buffers.len(), 0);
        assert_eq!(data_manager.finished_uncompressed_data_buffers.len(), 1);
    }

    #[tokio::test]
    async fn test_can_get_finished_uncompressed_data_buffer_when_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            UNIVARIATE_ID,
        )
        .await;

        assert!(data_manager.finished_data_buffer().await.is_some());
    }

    #[tokio::test]
    async fn test_can_get_multiple_finished_uncompressed_data_buffers_when_multiple_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY * 2,
            &mut data_manager,
            UNIVARIATE_ID,
        )
        .await;

        assert!(data_manager.finished_data_buffer().await.is_some());
        assert!(data_manager.finished_data_buffer().await.is_some());
    }

    #[tokio::test]
    async fn test_cannot_get_finished_uncompressed_data_buffers_when_none_are_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;

        assert!(data_manager.finished_data_buffer().await.is_none());
    }

    #[tokio::test]
    async fn test_spill_first_uncompressed_data_buffer_to_disk_if_out_of_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes() as usize;

        // Insert messages into the storage engine until a buffer is spilled to Apache Parquet.
        // If there is enough memory to hold n full buffers, we need n + 1 to spill a buffer.
        let max_full_buffers = reserved_memory / UncompressedInMemoryDataBuffer::memory_size();
        let message_count = (max_full_buffers * UNCOMPRESSED_DATA_BUFFER_CAPACITY) + 1;
        insert_data_points(message_count, &mut data_manager, UNIVARIATE_ID).await;

        // The first UncompressedDataBuffer should have a memory size of 0 as it is spilled to disk.
        let first_finished = data_manager
            .finished_uncompressed_data_buffers
            .pop_front()
            .unwrap();
        assert_eq!(first_finished.memory_size(), 0);

        // The UncompressedDataBuffer should be spilled to univariate id in the uncompressed folder.
        let local_data_folder = Path::new(&data_manager.local_data_folder);
        let uncompressed_path =
            local_data_folder.join(format!("{UNCOMPRESSED_DATA_FOLDER}/{UNIVARIATE_ID}"));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1);
    }

    #[tokio::test]
    async fn test_ignore_already_spilled_uncompressed_data_buffer_when_spilling() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY * 2,
            &mut data_manager,
            UNIVARIATE_ID,
        )
        .await;

        data_manager.spill_finished_buffer().await;
        // When spilling one more, the first buffer should be ignored since it is already spilled.
        data_manager.spill_finished_buffer().await;

        data_manager.finished_uncompressed_data_buffers.pop_front();
        // The second buffer should have a memory size of 0 since it is already spilled to disk.
        let second_finished = data_manager
            .finished_uncompressed_data_buffers
            .pop_front()
            .unwrap();
        assert_eq!(second_finished.memory_size(), 0);

        // The finished buffers should be spilled to univariate id in the uncompressed folder.
        let local_data_folder = Path::new(&data_manager.local_data_folder);
        let uncompressed_path =
            local_data_folder.join(format!("{UNCOMPRESSED_DATA_FOLDER}/{UNIVARIATE_ID}"));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 2);
    }

    #[tokio::test]
    async fn test_remaining_memory_incremented_when_spilling_finished_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;

        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            UNIVARIATE_ID,
        )
        .await;
        let remaining_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes();
        data_manager.spill_finished_buffer().await;

        assert!(
            remaining_memory
                < data_manager
                    .memory_pool
                    .remaining_uncompressed_memory_in_bytes()
        );
        assert_eq!(
            data_manager.used_uncompressed_memory_metric.values().len(),
            2
        );

        // The used disk space metric should have an entry for when the uncompressed data manager is
        // created and for when the data buffer is spilled to disk.
        assert_eq!(
            data_manager
                .used_disk_space_metric
                .read()
                .await
                .values()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn test_remaining_memory_decremented_when_creating_new_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes();

        insert_data_points(1, &mut data_manager, UNIVARIATE_ID).await;

        assert!(
            reserved_memory
                > data_manager
                    .memory_pool
                    .remaining_uncompressed_memory_in_bytes()
        );
        assert_eq!(
            data_manager.used_uncompressed_memory_metric.values().len(),
            1
        );
    }

    #[tokio::test]
    async fn test_remaining_memory_incremented_when_popping_in_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            UNIVARIATE_ID,
        )
        .await;

        let remaining_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes();
        data_manager.finished_data_buffer().await;

        assert!(
            remaining_memory
                < data_manager
                    .memory_pool
                    .remaining_uncompressed_memory_in_bytes()
        );
        assert_eq!(
            data_manager.used_uncompressed_memory_metric.values().len(),
            2
        );
    }

    #[tokio::test]
    async fn test_remaining_memory_not_incremented_when_popping_spilled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            UNIVARIATE_ID,
        )
        .await;

        data_manager.spill_finished_buffer().await;
        let remaining_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes();

        // Since the UncompressedOnDiskDataBuffer is not in memory, the remaining memory should not
        // increase when popped.
        data_manager.finished_data_buffer().await;

        assert_eq!(
            remaining_memory,
            data_manager
                .memory_pool
                .remaining_uncompressed_memory_in_bytes(),
        );

        assert_eq!(
            data_manager.used_uncompressed_memory_metric.values().len(),
            3
        );

        // The used disk space metric should have an entry for when the uncompressed data manager is
        // created, an entry for when the data buffer is spilled to disk, and an entry for when the
        // finished data buffer is popped.
        assert_eq!(
            data_manager
                .used_disk_space_metric
                .read()
                .await
                .values()
                .len(),
            3
        );
    }

    #[tokio::test]
    #[should_panic(expected = "Not enough reserved memory for the necessary uncompressed buffers.")]
    async fn test_panic_if_not_enough_reserved_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_uncompressed_memory_in_bytes() as usize;

        // If there is enough reserved memory to hold n builders, we need to create n + 1 to panic.
        for i in 0..(reserved_memory / UncompressedInMemoryDataBuffer::memory_size()) + 1 {
            insert_data_points(1, &mut data_manager, i as u64).await;
        }
    }

    #[tokio::test]
    async fn test_increase_uncompressed_remaining_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
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
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;

        // Insert data that should be spilled when the remaining memory is decreased.
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
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
            0
        );

        // The first UncompressedDataBuffer should have a memory size of 0 as it is spilled to disk.
        assert_eq!(
            data_manager
                .finished_uncompressed_data_buffers
                .pop_front()
                .unwrap()
                .memory_size(),
            0
        );
    }

    #[tokio::test]
    #[should_panic(expected = "Not enough reserved memory for the necessary uncompressed buffers.")]
    async fn test_panic_if_decreasing_uncompressed_remaining_memory_in_bytes_below_zero() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager, _model_table_metadata) =
            create_managers(temp_dir.path()).await;

        data_manager
            .adjust_uncompressed_remaining_memory_in_bytes(
                -((common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES + 1) as isize),
            )
            .await;
    }

    /// Insert `count` data points into `data_manager`.
    async fn insert_data_points(
        count: usize,
        data_manager: &mut UncompressedDataManager,
        univariate_id: u64,
    ) {
        let value: Value = 30.0;
        let error_bound = ErrorBound::try_new(0.0).unwrap();

        for i in 0..count {
            data_manager
                .insert_data_point(univariate_id, i as i64, value, error_bound)
                .await
                .unwrap();
        }
    }

    /// Create an [`UncompressedDataManager`] with a folder that is deleted once the test is finished.
    async fn create_managers(
        path: &Path,
    ) -> (MetadataManager, UncompressedDataManager, ModelTableMetadata) {
        let metadata_manager = MetadataManager::try_new(path).await.unwrap();

        // Ensure the expected metadata is available through the metadata manager.
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

        let model_table = ModelTableMetadata::try_new(
            "Table".to_owned(),
            query_schema,
            error_bounds,
            generated_columns,
        )
        .unwrap();

        metadata_manager
            .save_model_table_metadata(&model_table)
            .await
            .unwrap();

        metadata_manager
            .lookup_or_compute_tag_hash(&model_table, &["tag".to_owned()])
            .await
            .unwrap();

        let memory_pool = Arc::new(MemoryPool::new(
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        ));

        // UncompressedDataManager::try_new() lookup the error bounds for each univariate_id.
        let uncompressed_data_manager = UncompressedDataManager::try_new(
            metadata_manager.local_data_folder().to_owned(),
            memory_pool,
            &metadata_manager,
            false,
            Arc::new(RwLock::new(Metric::new())),
        )
        .await
        .unwrap();

        (metadata_manager, uncompressed_data_manager, model_table)
    }
}
