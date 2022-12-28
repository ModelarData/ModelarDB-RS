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

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use tracing::debug;

use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::metadata::MetadataManager;
use crate::models::ErrorBound;
use crate::storage::uncompressed_data_buffer::{
    UncompressedDataBuffer, UncompressedInMemoryDataBuffer, UncompressedOnDiskDataBuffer,
};
use crate::storage::UNCOMPRESSED_DATA_FOLDER;
use crate::types::{
    CompressedSchema, Timestamp, TimestampArray, UncompressedSchema, Value, ValueArray,
};
use crate::{compression, get_array};

/// Stores uncompressed data points temporarily in an in-memory buffer that spills to Apache Parquet
/// files. When a uncompressed data buffer is finished the data is made available for compression.
pub(super) struct UncompressedDataManager {
    /// Path to the folder containing all uncompressed data managed by the
    /// [`StorageEngine`](crate::storage::StorageEngine).
    local_data_folder: PathBuf,
    /// The [`UncompressedDataBuffers`](UncompressedDataBuffer) currently being filled with ingested
    /// data points.
    active_uncompressed_data_buffers: HashMap<u64, UncompressedInMemoryDataBuffer>,
    /// FIFO queue of full [`UncompressedDataBuffers`](UncompressedDataBuffer) that are ready for
    /// compression.
    finished_uncompressed_data_buffers: VecDeque<Box<dyn UncompressedDataBuffer>>,
    /// How many bytes of memory that are left for storing
    /// [`UncompressedDataBuffers`](UncompressedDataBuffer).
    uncompressed_remaining_memory_in_bytes: usize,
    /// Reference to the schema for uncompressed data buffers.
    uncompressed_schema: UncompressedSchema,
    /// Reference to the schema for compressed data buffers.
    compressed_schema: CompressedSchema,
    // TODO: This is a temporary field used to fix existing tests. Remove when configuration component is changed.
    /// If this is true, compress finished buffers directly instead of queueing them.
    compress_directly: bool,
}

impl UncompressedDataManager {
    /// Return an [`UncompressedDataManager`] if the required folder can be created in
    /// `local_data_folder` and an [`UncompressedOnDiskDataBuffer`] can be initialized for all
    /// Apache Parquet files containing uncompressed data points in `local_data_store`, otherwise
    /// [`IOError`] is returned.
    pub(super) fn try_new(
        local_data_folder: PathBuf,
        uncompressed_reserved_memory_in_bytes: usize,
        uncompressed_schema: UncompressedSchema,
        compressed_schema: CompressedSchema,
        compress_directly: bool,
    ) -> Result<Self, IOError> {
        // Ensure the folder required by the uncompressed data manager exists.
        let local_uncompressed_data_folder = local_data_folder.join(UNCOMPRESSED_DATA_FOLDER);
        fs::create_dir_all(&local_uncompressed_data_folder)?;

        // Add references to the uncompressed data buffers currently on disk.
        let mut finished_data_buffers: VecDeque<Box<dyn UncompressedDataBuffer>> = VecDeque::new();
        for maybe_folder_dir_entry in local_uncompressed_data_folder.read_dir()? {
            let folder_dir_entry = maybe_folder_dir_entry?;

            // unwrap() is safe as the file_name is a univariate id.
            let univariate_id = folder_dir_entry
                .file_name()
                .to_str()
                .ok_or_else(|| IOError::new(IOErrorKind::InvalidData, "Path is not valid UTF-8."))?
                .parse::<u64>()
                .map_err(|error| IOError::new(IOErrorKind::InvalidData, error))?;

            for maybe_file_dir_entry in folder_dir_entry.path().read_dir()? {
                finished_data_buffers.push_back(Box::new(UncompressedOnDiskDataBuffer::try_new(
                    univariate_id,
                    maybe_file_dir_entry?.path(),
                )?));
            }
        }

        Ok(Self {
            local_data_folder,
            active_uncompressed_data_buffers: HashMap::new(),
            finished_uncompressed_data_buffers: finished_data_buffers,
            uncompressed_remaining_memory_in_bytes: uncompressed_reserved_memory_in_bytes,
            uncompressed_schema,
            compressed_schema,
            compress_directly,
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
        metadata_manager: &mut MetadataManager,
        model_table: &ModelTableMetadata,
        data_points: &RecordBatch,
    ) -> Result<Vec<RecordBatch>, String> {
        debug!(
            "Received record batch with {} data points for the table '{}'.",
            data_points.num_rows(),
            model_table.name
        );

        // Prepare the timestamp column for iteration.
        let timestamp_index = model_table.timestamp_column_index;
        let timestamps: &TimestampArray = data_points
            .column(timestamp_index)
            .as_any()
            .downcast_ref()
            .unwrap();

        // Prepare the tag columns for iteration.
        let tag_column_arrays: Vec<&StringArray> = model_table
            .tag_column_indices
            .iter()
            .map(|index| data_points.column(*index).as_any().downcast_ref().unwrap())
            .collect();

        // Prepare the field columns for iteration. The column index is saved with the corresponding
        // array.
        let field_column_arrays: Vec<(usize, &ValueArray)> = model_table
            .schema
            .fields()
            .iter()
            .filter_map(|field| {
                let index = model_table.schema.index_of(field.name().as_str()).unwrap();

                // Field columns are the columns that are not the timestamp column or one of the tag
                // columns.
                let not_timestamp_column = index != model_table.timestamp_column_index;
                let not_tag_column = !model_table.tag_column_indices.contains(&index);

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
                .get_or_compute_tag_hash(model_table, &tag_values)
                .map_err(|error| format!("Tag hash could not be saved: {}", error))?;

            // For each field column, generate the 64-bit univariate id, and append the current
            // timestamp and the field's value into the in-memory buffer for the univariate id.
            for (field_index, field_column_array) in &field_column_arrays {
                let univariate_id = tag_hash | *field_index as u64;
                let value = field_column_array.value(index);

                // TODO: When the compression component is changed, just insert the data points.
                // unwrap() is safe to use since the timestamps array cannot contain null values.
                if let Some(buffer) = self
                    .insert_data_point(univariate_id, timestamp.unwrap(), value)
                    .await
                {
                    compressed_buffers.push(buffer);
                };
            }
        }

        Ok(compressed_buffers)
    }

    // TODO: Update to handle finished buffers when compress_finished_buffer is removed.
    /// Compress the uncompressed data buffer that the [`UncompressedDataManager`] is currently
    /// managing, and return the compressed buffers and their univariate ids.
    pub(super) async fn flush(&mut self) -> Vec<(u64, RecordBatch)> {
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
            let record_batch = self.compress_finished_buffer(univariate_id, buffer).await;
            compressed_buffers.push((univariate_id, record_batch));
        }
        compressed_buffers
    }

    // TODO: When the compression component is changed, this function should return Ok or Err.
    /// Insert a single data point into its in-memory buffer. Return a compressed data buffer if the
    /// inserted data point made the buffer full and thus finished, otherwise return [`None`].
    async fn insert_data_point(
        &mut self,
        univariate_id: u64,
        timestamp: Timestamp,
        value: Value,
    ) -> Option<RecordBatch> {
        debug!(
            "Inserting data point ({}, {}) into uncompressed data buffer for {}.",
            timestamp, value, univariate_id
        );

        if let Some(buffer) = self
            .active_uncompressed_data_buffers
            .get_mut(&univariate_id)
        {
            debug!("Found existing buffer for {}.", univariate_id);
            buffer.insert_data(timestamp, value);

            if buffer.is_full() {
                debug!(
                    "Buffer for {} is full, moving it to the queue of finished buffers.",
                    univariate_id
                );

                // unwrap() is safe as this is only reachable if the buffer exists in the HashMap.
                let finished_buffer = self
                    .active_uncompressed_data_buffers
                    .remove(&univariate_id)
                    .unwrap();

                // TODO: Currently we directly compress a buffer when it is finished. This should be
                //       changed to queue the buffer and let the compression component retrieve the
                //       finished buffer and insert it back when compressed.
                if self.compress_directly {
                    return Some(
                        self.compress_finished_buffer(univariate_id, finished_buffer)
                            .await,
                    );
                } else {
                    self.finished_uncompressed_data_buffers
                        .push_back(Box::new(finished_buffer));
                }
            }
        } else {
            debug!(
                "Could not find buffer for {}. Creating Buffer.",
                univariate_id
            );

            // If there is not enough memory for a new buffer, spill a finished in-memory buffer.
            if UncompressedInMemoryDataBuffer::get_memory_size()
                > self.uncompressed_remaining_memory_in_bytes
            {
                self.spill_finished_buffer().await;
            }

            // Create a new buffer and reduce the remaining amount of reserved memory by its size.
            let mut buffer = UncompressedInMemoryDataBuffer::new(univariate_id);
            self.uncompressed_remaining_memory_in_bytes -=
                UncompressedInMemoryDataBuffer::get_memory_size();

            debug!(
                "Created buffer for {}. Remaining reserved bytes: {}.",
                self.uncompressed_remaining_memory_in_bytes, univariate_id
            );

            buffer.insert_data(timestamp, value);
            self.active_uncompressed_data_buffers
                .insert(univariate_id, buffer);
        }

        None
    }

    /// Remove the oldest finished [`UncompressedDataBuffer`] from the queue and return it. Returns
    /// [`None`] if there are no finished [`UncompressedDataBuffers`](UncompressedDataBuffer) in the
    /// queue.
    pub(super) fn get_finished_data_buffer(&mut self) -> Option<Box<dyn UncompressedDataBuffer>> {
        if let Some(uncompressed_data_buffer) = self.finished_uncompressed_data_buffers.pop_front()
        {
            // Add the memory size of the removed buffer back to the remaining bytes.
            self.uncompressed_remaining_memory_in_bytes +=
                uncompressed_data_buffer.get_memory_size();

            Some(uncompressed_data_buffer)
        } else {
            None
        }
    }

    // TODO: Remove this when compression component is changed.
    /// Compress the full [`UncompressedInMemoryDataBuffer`] given as `in_memory_data_buffer` and
    /// return the resulting compressed and merged segments as a [`RecordBatch`].
    async fn compress_finished_buffer(
        &mut self,
        univariate_id: u64,
        mut in_memory_data_buffer: UncompressedInMemoryDataBuffer,
    ) -> RecordBatch {
        // Add the size of the segment back to the remaining reserved bytes.
        self.uncompressed_remaining_memory_in_bytes +=
            UncompressedInMemoryDataBuffer::get_memory_size();

        // unwrap() is safe to use since the error bound is not negative, infinite, or NAN.
        let error_bound = ErrorBound::try_new(0.0).unwrap();

        // unwrap() is safe to use since get_record_batch() can only fail for spilled buffers.
        let data_points = in_memory_data_buffer
            .get_record_batch(&self.uncompressed_schema)
            .await
            .unwrap();
        let uncompressed_timestamps = get_array!(data_points, 0, TimestampArray);
        let uncompressed_values = get_array!(data_points, 1, ValueArray);

        // unwrap() is safe to use since uncompressed_timestamps and uncompressed_values have the same length.
        let compressed_segments = compression::try_compress(
            univariate_id,
            uncompressed_timestamps,
            uncompressed_values,
            error_bound,
            &self.compressed_schema,
        )
        .unwrap();

        compression::merge_segments(compressed_segments)
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
                .spill_to_apache_parquet(
                    self.local_data_folder.as_path(),
                    &self.uncompressed_schema,
                )
                .await
            {
                // Add the size of the in-memory data buffer back to the remaining reserved bytes.
                self.uncompressed_remaining_memory_in_bytes +=
                    UncompressedInMemoryDataBuffer::get_memory_size();

                debug!(
                    "Spilled '{:?}'. Remaining reserved bytes: {}.",
                    finished, self.uncompressed_remaining_memory_in_bytes
                );

                *finished = Box::new(uncompressed_on_disk_data_buffer);
                return;
            }
        }

        // TODO: All uncompressed and compressed data should be saved to disk first.
        // If not able to find any in-memory finished segments, panic!() is the only option.
        panic!("Not enough reserved memory for the necessary uncompressed buffers.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
    use tempfile;

    use crate::metadata::{test_util, MetadataManager};
    use crate::storage::UNCOMPRESSED_DATA_BUFFER_CAPACITY;
    use crate::types::{ArrowTimestamp, ArrowValue};

    const UNIVARIATE_ID: u64 = 1;

    // Tests for UncompressedDataManager.
    #[tokio::test]
    async fn test_can_find_existing_on_disk_data_buffers() {
        // Spill an uncompressed buffer to disk.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut buffer = UncompressedInMemoryDataBuffer::new(1);
        buffer.insert_data(100, 10.0);
        buffer
            .spill_to_apache_parquet(temp_dir.path(), &test_util::get_uncompressed_schema())
            .await
            .unwrap();

        // The uncompressed data buffer should be referenced by the uncompressed data manager.
        let (_metadata_manager, data_manager) = create_managers(temp_dir.path());
        assert_eq!(data_manager.finished_uncompressed_data_buffers.len(), 1)
    }

    #[tokio::test]
    async fn test_can_insert_record_batch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut metadata_manager, mut data_manager) = create_managers(temp_dir.path());

        let (model_table_metadata, data) = get_uncompressed_data(1);
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .unwrap();
        data_manager
            .insert_data_points(&mut metadata_manager, &model_table_metadata, &data)
            .await
            .unwrap();

        // Two separate builders are created since the inserted data has two field columns.
        assert_eq!(
            data_manager.active_uncompressed_data_buffers.keys().len(),
            2
        )
    }

    #[tokio::test]
    async fn test_can_insert_record_batch_with_multiple_data_points() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut metadata_manager, mut data_manager) = create_managers(temp_dir.path());

        let (model_table_metadata, data) = get_uncompressed_data(2);
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .unwrap();
        data_manager
            .insert_data_points(&mut metadata_manager, &model_table_metadata, &data)
            .await
            .unwrap();

        // Since the tag is different for each data point, 4 separate buffers should be created.
        assert_eq!(
            data_manager.active_uncompressed_data_buffers.keys().len(),
            4
        )
    }

    /// Create a record batch with data that resembles uncompressed data with a single tag and two
    /// field columns. The returned data has `row_count` rows, with a different tag for each row.
    /// Also create model table metadata for a model table that matches the created data.
    fn get_uncompressed_data(row_count: usize) -> (ModelTableMetadata, RecordBatch) {
        let tags: Vec<String> = (0..row_count).map(|tag| tag.to_string()).collect();
        let timestamps: Vec<Timestamp> = (0..row_count).map(|ts| ts as Timestamp).collect();
        let values: Vec<Value> = (0..row_count).map(|value| value as Value).collect();

        let schema = Schema::new(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("value_1", ArrowValue::DATA_TYPE, false),
            Field::new("value_2", ArrowValue::DATA_TYPE, false),
        ]);

        let data = RecordBatch::try_new(
            Arc::new(schema.clone()),
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
        ];

        let model_table_metadata =
            ModelTableMetadata::try_new("model_table".to_owned(), schema, 1, vec![0], error_bounds)
                .unwrap();

        (model_table_metadata, data)
    }

    #[tokio::test]
    async fn test_can_insert_data_point_into_new_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(1, &mut data_manager, UNIVARIATE_ID).await;

        assert!(data_manager
            .active_uncompressed_data_buffers
            .contains_key(&UNIVARIATE_ID));
        assert_eq!(
            data_manager
                .active_uncompressed_data_buffers
                .get(&UNIVARIATE_ID)
                .unwrap()
                .get_length(),
            1
        );
    }

    #[tokio::test]
    async fn test_can_insert_message_into_existing_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(2, &mut data_manager, UNIVARIATE_ID).await;

        assert!(data_manager
            .active_uncompressed_data_buffers
            .contains_key(&UNIVARIATE_ID));
        assert_eq!(
            data_manager
                .active_uncompressed_data_buffers
                .get(&UNIVARIATE_ID)
                .unwrap()
                .get_length(),
            2
        );
    }

    #[tokio::test]
    async fn test_can_get_finished_uncompressed_data_buffer_when_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            UNIVARIATE_ID,
        )
        .await;

        assert!(data_manager.get_finished_data_buffer().is_some());
    }

    #[tokio::test]
    async fn test_can_get_multiple_finished_uncompressed_data_buffers_when_multiple_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY * 2,
            &mut data_manager,
            UNIVARIATE_ID,
        )
        .await;

        assert!(data_manager.get_finished_data_buffer().is_some());
        assert!(data_manager.get_finished_data_buffer().is_some());
    }

    #[test]
    fn test_cannot_get_finished_uncompressed_data_buffers_when_none_are_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());

        assert!(data_manager.get_finished_data_buffer().is_none());
    }

    #[tokio::test]
    async fn test_spill_first_uncompressed_data_buffer_to_disk_if_out_of_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        // Insert messages into the storage engine until a buffer is spilled to Apache Parquet.
        // If there is enough memory to hold n full buffers, we need n + 1 to spill a buffer.
        let max_full_buffers = reserved_memory / UncompressedInMemoryDataBuffer::get_memory_size();
        let message_count = (max_full_buffers * UNCOMPRESSED_DATA_BUFFER_CAPACITY) + 1;
        insert_data_points(message_count, &mut data_manager, UNIVARIATE_ID).await;

        // The first UncompressedDataBuffer should have a memory size of 0 as it is spilled to disk.
        let first_finished = data_manager
            .finished_uncompressed_data_buffers
            .pop_front()
            .unwrap();
        assert_eq!(first_finished.get_memory_size(), 0);

        // The UncompressedDataBuffer should be spilled to univariate id in the uncompressed folder.
        let local_data_folder = Path::new(&data_manager.local_data_folder);
        let uncompressed_path =
            local_data_folder.join(format!("{}/{}", UNCOMPRESSED_DATA_FOLDER, UNIVARIATE_ID));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1);
    }

    #[tokio::test]
    async fn test_ignore_already_spilled_uncompressed_data_buffer_when_spilling() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
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
        assert_eq!(second_finished.get_memory_size(), 0);

        // The finished buffers should be spilled to univariate id in the uncompressed folder.
        let local_data_folder = Path::new(&data_manager.local_data_folder);
        let uncompressed_path =
            local_data_folder.join(format!("{}/{}", UNCOMPRESSED_DATA_FOLDER, UNIVARIATE_ID));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 2);
    }

    #[tokio::test]
    async fn test_remaining_memory_incremented_when_spilling_finished_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());

        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            UNIVARIATE_ID,
        )
        .await;
        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();
        data_manager.spill_finished_buffer().await;

        assert!(remaining_memory < data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[tokio::test]
    async fn test_remaining_memory_decremented_when_creating_new_uncompressed_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        insert_data_points(1, &mut data_manager, UNIVARIATE_ID).await;

        assert!(reserved_memory > data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[tokio::test]
    async fn test_remaining_memory_incremented_when_popping_in_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            UNIVARIATE_ID,
        )
        .await;

        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();
        data_manager.get_finished_data_buffer();

        assert!(remaining_memory < data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[tokio::test]
    async fn test_remaining_memory_not_incremented_when_popping_spilled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(
            UNCOMPRESSED_DATA_BUFFER_CAPACITY,
            &mut data_manager,
            UNIVARIATE_ID,
        )
        .await;

        data_manager.spill_finished_buffer().await;
        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();

        // Since the UncompressedOnDiskDataBuffer is not in memory, the remaining memory should not
        // increase when popped.
        data_manager.get_finished_data_buffer();

        assert_eq!(
            remaining_memory,
            data_manager.uncompressed_remaining_memory_in_bytes
        );
    }

    #[tokio::test]
    #[should_panic(expected = "Not enough reserved memory for the necessary uncompressed buffers.")]
    async fn test_panic_if_not_enough_reserved_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        // If there is enough reserved memory to hold n builders, we need to create n + 1 to panic.
        for i in 0..(reserved_memory / UncompressedInMemoryDataBuffer::get_memory_size()) + 1 {
            insert_data_points(1, &mut data_manager, i as u64).await;
        }
    }

    /// Insert `count` data points into `data_manager`.
    async fn insert_data_points(
        count: usize,
        data_manager: &mut UncompressedDataManager,
        univariate_id: u64,
    ) {
        let value: Value = 30.0;

        for i in 0..count {
            data_manager
                .insert_data_point(univariate_id, i as i64, value)
                .await;
        }
    }

    /// Create an [`UncompressedDataManager`] with a folder that is deleted once the test is
    /// finished.
    fn create_managers(path: &Path) -> (MetadataManager, UncompressedDataManager) {
        let metadata_manager = test_util::get_test_metadata_manager(path);

        let local_data_folder = metadata_manager.get_local_data_folder();

        let uncompressed_data_manager = UncompressedDataManager::try_new(
            local_data_folder.to_owned(),
            metadata_manager.uncompressed_reserved_memory_in_bytes,
            metadata_manager.get_uncompressed_schema(),
            metadata_manager.get_compressed_schema(),
            false,
        )
        .unwrap();

        (metadata_manager, uncompressed_data_manager)
    }
}
