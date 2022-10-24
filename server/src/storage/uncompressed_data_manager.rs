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

//! Support for managing all uncompressed data that is ingested into the [`StorageEngine`].

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use tracing::{debug, debug_span};

use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::metadata::MetadataManager;
use crate::models::ErrorBound;
use crate::storage::segment::{FinishedSegment, SegmentBuilder, UncompressedSegment};
use crate::types::{
    CompressedSchema, Timestamp, TimestampArray, UncompressedSchema, Value, ValueArray,
};
use crate::{compression, get_array};

/// Converts a batch of data to uncompressed data points and stores uncompressed data points
/// temporarily in an in-memory buffer that spills to Apache Parquet files. When finished the data
/// is made available for compression.
pub(super) struct UncompressedDataManager {
    /// Path to the folder containing all uncompressed data managed by the [`StorageEngine`].
    data_folder_path: PathBuf,
    /// The [`UncompressedSegments`](UncompressedSegment) while they are being built.
    uncompressed_data: HashMap<u64, SegmentBuilder>,
    /// FIFO queue of [`FinishedSegments`](FinishedSegment) that are ready for compression.
    finished_queue: VecDeque<FinishedSegment>,
    /// How many bytes of memory that are left for storing [`UncompressedSegments`](UncompressedSegment).
    uncompressed_remaining_memory_in_bytes: usize,
    /// Reference to the schema for uncompressed segments.
    uncompressed_schema: UncompressedSchema,
    /// Reference to the schema for compressed segments.
    compressed_schema: CompressedSchema,
    // TODO: This is a temporary field used to fix existing tests. Remove when configuration component is changed.
    /// If this is true, compress full segments directly instead of queueing them.
    compress_directly: bool,
}

impl UncompressedDataManager {
    pub(super) fn new(
        data_folder_path: PathBuf,
        uncompressed_reserved_memory_in_bytes: usize,
        uncompressed_schema: UncompressedSchema,
        compressed_schema: CompressedSchema,
        compress_directly: bool,
    ) -> Self {
        Self {
            data_folder_path,
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            uncompressed_data: HashMap::new(),
            finished_queue: VecDeque::new(),
            uncompressed_remaining_memory_in_bytes: uncompressed_reserved_memory_in_bytes,
            uncompressed_schema,
            compressed_schema,
            compress_directly,
        }
    }

    // TODO: When the compression component is changed, this function should return Ok or Err.
    /// Parse `data_points` and insert it into the in-memory buffer. The data points are first parsed
    /// into multiple univariate time series based on `model_table`. These individual time series
    /// are then inserted into the storage engine. Return a list of segments that were compressed
    /// as a result of inserting the data points with their corresponding keys if the data was
    /// successfully inserted, otherwise return [`Err`].
    pub(super) fn insert_data_points(
        &mut self,
        metadata_manager: &mut MetadataManager,
        model_table: &ModelTableMetadata,
        data_points: &RecordBatch,
    ) -> Result<Vec<(u64, RecordBatch)>, String> {
        let _span = debug_span!("insert_data_points", table = model_table.name).entered();
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

        // Prepare the field columns for iteration. The column index is saved with the corresponding array.
        let field_column_arrays: Vec<(usize, &ValueArray)> = model_table
            .schema
            .fields()
            .iter()
            .filter_map(|field| {
                let index = model_table.schema.index_of(field.name().as_str()).unwrap();

                // Field columns are the columns that are not the timestamp column or one of the tag columns.
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

        let mut compressed_segments = vec![];

        // For each row in the data, generate a tag hash, extract the individual measurements,
        // and insert them into the storage engine.
        for (index, timestamp) in timestamps.iter().enumerate() {
            let tag_values: Vec<String> = tag_column_arrays
                .iter()
                .map(|array| array.value(index).to_string())
                .collect();

            let tag_hash = metadata_manager
                .get_or_compute_tag_hash(&model_table, &tag_values)
                .map_err(|error| format!("Tag hash could not be saved: {}", error.to_string()))?;

            // For each field column, generate the 64-bit key, create a single data point, and
            // insert the data point into the in-memory buffer.
            for (field_index, field_column_array) in &field_column_arrays {
                let key = tag_hash | *field_index as u64;
                let value = field_column_array.value(index);

                // TODO: When the compression component is changed, just insert the data points.
                // unwrap() is safe to use since the timestamps array cannot contain null values.
                if let Some(segment) = self.insert_data_point(key, timestamp.unwrap(), value) {
                    compressed_segments.push((key, segment));
                };
            }
        }

        Ok(compressed_segments)
    }

    // TODO: Update to handle finished segments when compress_full_segment is removed.
    /// Compress the uncompressed data that the [`UncompressedDataManager`] is
    /// currently managing, and return the compressed segments and their keys.
    pub(super) fn flush(&mut self) -> Vec<(u64, RecordBatch)> {
        // The keys are copied to not have multiple borrows to self at the same
        // time, however, it is not clear if it necessary or can be removed.
        let keys: Vec<u64> = self.uncompressed_data.keys().cloned().collect();

        let mut compressed_segments = vec![];
        for key in keys {
            let segment = self.uncompressed_data.remove(&key).unwrap();
            let record_batch = self.compress_full_segment(segment);
            compressed_segments.push((key, record_batch));
        }
        compressed_segments
    }

    // TODO: When the compression component is changed, this function should return Ok or Err.
    /// Insert a single data point into the in-memory buffer. Return the compressed segment if
    /// the inserted data point made the segment full, otherwise return [`None`].
    fn insert_data_point(
        &mut self,
        key: u64,
        timestamp: Timestamp,
        value: Value,
    ) -> Option<RecordBatch> {
        let _span = debug_span!("insert_data_point", key = key).entered();
        debug!(
            "Inserting data point ({}, {}) into segment.",
            timestamp, value
        );

        if let Some(segment) = self.uncompressed_data.get_mut(&key) {
            debug!("Found existing segment.");
            segment.insert_data(timestamp, value);

            if segment.is_full() {
                debug!("Segment is full, moving it to the queue of finished segments.");

                // Since this is only reachable if the segment exists in the HashMap, unwrap is safe to use.
                let full_segment = self.uncompressed_data.remove(&key).unwrap();

                // TODO: Currently we directly compress a segment when it is finished. This
                //       should be changed to queue the segment and let the compression component
                //       retrieve the finished segment and insert it back when compressed.
                if self.compress_directly {
                    return Some(self.compress_full_segment(full_segment));
                } else {
                    self.enqueue_segment(key, full_segment);
                }
            }
        } else {
            debug!("Could not find segment. Creating segment.");

            // If there is not enough memory for a new segment, spill a finished segment.
            if SegmentBuilder::get_memory_size() > self.uncompressed_remaining_memory_in_bytes {
                self.spill_finished_segment();
            }

            // Create a new segment and reduce the remaining amount of reserved memory by its size.
            let mut segment = SegmentBuilder::new();
            self.uncompressed_remaining_memory_in_bytes -= SegmentBuilder::get_memory_size();

            debug!(
                "Created segment. Remaining reserved bytes: {}.",
                self.uncompressed_remaining_memory_in_bytes
            );

            segment.insert_data(timestamp, value);
            self.uncompressed_data.insert(key, segment);
        }

        None
    }

    /// Remove the oldest [`FinishedSegment`] from the queue and return it. Return [`None`] if the
    /// queue of [`FinishedSegments`](FinishedSegment) is empty.
    pub(super) fn get_finished_segment(&mut self) -> Option<FinishedSegment> {
        if let Some(finished_segment) = self.finished_queue.pop_front() {
            // Add the memory size of the removed FinishedSegment back to the remaining bytes.
            self.uncompressed_remaining_memory_in_bytes +=
                finished_segment.uncompressed_segment.get_memory_size();

            Some(finished_segment)
        } else {
            None
        }
    }

    // TODO: Remove this when compression component is changed.
    /// Compress the given full `segment_builder` and return the compressed and merged segment.
    fn compress_full_segment(&mut self, mut segment_builder: SegmentBuilder) -> RecordBatch {
        // Add the size of the segment back to the remaining reserved bytes.
        self.uncompressed_remaining_memory_in_bytes += SegmentBuilder::get_memory_size();

        // unwrap() is safe to use since the error bound is not negative, infinite, or NAN.
        let error_bound = ErrorBound::try_new(0.0).unwrap();

        // unwrap() is safe to use since get_record_batch() can only fail for spilled segments.
        let data_points = segment_builder
            .get_record_batch(&self.uncompressed_schema)
            .unwrap();
        let uncompressed_timestamps = get_array!(data_points, 0, TimestampArray);
        let uncompressed_values = get_array!(data_points, 1, ValueArray);

        // unwrap() is safe to use since uncompressed_timestamps and uncompressed_values have the same length.
        let compressed_segments = compression::try_compress(
            &uncompressed_timestamps,
            &uncompressed_values,
            error_bound,
            &self.compressed_schema,
        )
        .unwrap();

        compression::merge_segments(compressed_segments)
    }

    /// Move `segment_builder` to the queue of [`FinishedSegments`](FinishedSegment).
    fn enqueue_segment(&mut self, key: u64, segment_builder: SegmentBuilder) {
        let finished_segment = FinishedSegment {
            key,
            uncompressed_segment: Box::new(segment_builder),
        };

        self.finished_queue.push_back(finished_segment);
    }

    /// Spill the first in-memory [`FinishedSegment`] in the queue of [`FinishedSegments`](FinishedSegment).
    /// If no in-memory [`FinishedSegments`](FinishedSegment) could be found, [`panic`](std::panic).
    fn spill_finished_segment(&mut self) {
        debug!("Not enough memory to create segment. Spilling an already finished segment.");

        // Iterate through the finished segments to find a segment that is in memory.
        for finished in self.finished_queue.iter_mut() {
            if let Ok(file_path) = finished
                .spill_to_apache_parquet(self.data_folder_path.as_path(), &self.uncompressed_schema)
            {
                // Add the size of the segment back to the remaining reserved bytes.
                self.uncompressed_remaining_memory_in_bytes += SegmentBuilder::get_memory_size();

                debug!(
                    "Spilled the segment to '{}'. Remaining reserved bytes: {}.",
                    file_path.display(),
                    self.uncompressed_remaining_memory_in_bytes
                );
                return ();
            }
        }

        // TODO: All uncompressed and compressed data should be saved to disk first.
        // If not able to find any in-memory finished segments, we should panic.
        panic!("Not enough reserved memory to hold all necessary segment builders.");
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
    use crate::storage::BUILDER_CAPACITY;
    use crate::types::{ArrowTimestamp, ArrowValue};

    const KEY: u64 = 1;

    #[test]
    fn test_can_insert_record_batch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut metadata_manager, mut data_manager) = create_managers(temp_dir.path());

        let (model_table_metadata, data) = get_uncompressed_data(1);
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .unwrap();
        data_manager
            .insert_data_points(&mut metadata_manager, &model_table_metadata, &data)
            .unwrap();

        // Two separate builders are created since the inserted data has two field columns.
        assert_eq!(data_manager.uncompressed_data.keys().len(), 2)
    }

    #[test]
    fn test_can_insert_record_batch_with_multiple_data_points() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut metadata_manager, mut data_manager) = create_managers(temp_dir.path());

        let (model_table_metadata, data) = get_uncompressed_data(2);
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .unwrap();
        data_manager
            .insert_data_points(&mut metadata_manager, &model_table_metadata, &data)
            .unwrap();

        // Since the tag is different for each data point, 4 separate builders should be created.
        assert_eq!(data_manager.uncompressed_data.keys().len(), 4)
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

    #[test]
    fn test_can_insert_data_point_into_new_segment() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(1, &mut data_manager, KEY);

        assert!(data_manager.uncompressed_data.contains_key(&KEY));
        assert_eq!(
            data_manager
                .uncompressed_data
                .get(&KEY)
                .unwrap()
                .get_length(),
            1
        );
    }

    #[test]
    fn test_can_insert_message_into_existing_segment() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(2, &mut data_manager, KEY);

        assert!(data_manager.uncompressed_data.contains_key(&KEY));
        assert_eq!(
            data_manager
                .uncompressed_data
                .get(&KEY)
                .unwrap()
                .get_length(),
            2
        );
    }

    #[test]
    fn test_can_get_finished_segment_when_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(BUILDER_CAPACITY, &mut data_manager, KEY);

        assert!(data_manager.get_finished_segment().is_some());
    }

    #[test]
    fn test_can_get_multiple_finished_segments_when_multiple_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(BUILDER_CAPACITY * 2, &mut data_manager, KEY);

        assert!(data_manager.get_finished_segment().is_some());
        assert!(data_manager.get_finished_segment().is_some());
    }

    #[test]
    fn test_cannot_get_finished_segment_when_not_finished() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());

        assert!(data_manager.get_finished_segment().is_none());
    }

    #[test]
    fn test_spill_first_finished_segment_if_out_of_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        // Insert messages into the StorageEngine until a segment is spilled to Apache Parquet.
        // If there is enough memory to hold n full segments, we need n + 1 to spill a segment.
        let max_full_segments = reserved_memory / SegmentBuilder::get_memory_size();
        let message_count = (max_full_segments * BUILDER_CAPACITY) + 1;
        insert_data_points(message_count, &mut data_manager, KEY);

        // The first FinishedSegment should have a memory size of 0 since it is spilled to disk.
        let first_finished = data_manager.finished_queue.pop_front().unwrap();
        assert_eq!(first_finished.uncompressed_segment.get_memory_size(), 0);

        // The FinishedSegment should be spilled to the "uncompressed" folder under the key.
        let data_folder_path = Path::new(&data_manager.data_folder_path);
        let uncompressed_path = data_folder_path.join(format!("{}/uncompressed", KEY));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1);
    }

    #[test]
    fn test_ignore_already_spilled_segments_when_spilling() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(BUILDER_CAPACITY * 2, &mut data_manager, KEY);

        data_manager.spill_finished_segment();
        // When spilling one more, the first FinishedSegment should be ignored since it is already spilled.
        data_manager.spill_finished_segment();

        data_manager.finished_queue.pop_front();
        // The second FinishedSegment should have a memory size of 0 since it is spilled to disk.
        let second_finished = data_manager.finished_queue.pop_front().unwrap();
        assert_eq!(second_finished.uncompressed_segment.get_memory_size(), 0);

        // The finished segments should be spilled to the "uncompressed" folder under the key.
        let data_folder_path = Path::new(&data_manager.data_folder_path);
        let uncompressed_path = data_folder_path.join(format!("{}/uncompressed", KEY));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 2);
    }

    #[test]
    fn test_remaining_memory_incremented_when_spilling_finished_segment() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());

        insert_data_points(BUILDER_CAPACITY, &mut data_manager, KEY);
        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();
        data_manager.spill_finished_segment();

        assert!(remaining_memory < data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_decremented_when_creating_new_segment() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        insert_data_points(1, &mut data_manager, KEY);

        assert!(reserved_memory > data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_incremented_when_popping_in_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(BUILDER_CAPACITY, &mut data_manager, KEY);

        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();
        data_manager.get_finished_segment();

        assert!(remaining_memory < data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_not_incremented_when_popping_spilled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        insert_data_points(BUILDER_CAPACITY, &mut data_manager, KEY);

        data_manager.spill_finished_segment();
        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();

        // Since the FinishedSegment is not in memory, the remaining memory should not increase when popped.
        data_manager.get_finished_segment();

        assert_eq!(
            remaining_memory,
            data_manager.uncompressed_remaining_memory_in_bytes
        );
    }

    #[test]
    #[should_panic(expected = "Not enough reserved memory to hold all necessary segment builders.")]
    fn test_panic_if_not_enough_reserved_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_metadata_manager, mut data_manager) = create_managers(temp_dir.path());
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        // If there is enough reserved memory to hold n builders, we need to create n + 1 to panic.
        for i in 0..(reserved_memory / SegmentBuilder::get_memory_size()) + 1 {
            insert_data_points(1, &mut data_manager, i as u64);
        }
    }

    /// Insert `count` data points into `data_manager`.
    fn insert_data_points(count: usize, data_manager: &mut UncompressedDataManager, key: u64) {
        let value: Value = 30.0;

        for i in 0..count {
            data_manager.insert_data_point(key, i as i64, value);
        }
    }

    /// Create an [`UncompressedDataManager`] with a folder that is deleted once the test is finished.
    fn create_managers(path: &Path) -> (MetadataManager, UncompressedDataManager) {
        let metadata_manager = test_util::get_test_metadata_manager(path);

        let data_folder_path = metadata_manager.get_data_folder_path();

        let uncompressed_data_manager = UncompressedDataManager::new(
            data_folder_path.to_owned(),
            metadata_manager.uncompressed_reserved_memory_in_bytes,
            metadata_manager.get_uncompressed_schema(),
            metadata_manager.get_compressed_schema(),
            false,
        );

        (metadata_manager, uncompressed_data_manager)
    }
}
