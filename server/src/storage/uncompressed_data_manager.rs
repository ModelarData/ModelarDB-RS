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

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::Hasher;
use std::path::PathBuf;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use rusqlite::Connection;
use tracing::{info, info_span};

use crate::catalog::NewModelTableMetadata;
use crate::storage::segment::{FinishedSegment, SegmentBuilder};
use crate::types::{Timestamp, TimestampArray, Value, ValueArray};

const UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES: usize = 5000;

/// Converts a batch of data to uncompressed data points and stores uncompressed data points
/// temporarily in an in-memory buffer that spills to Apache Parquet files. When finished the data
/// is made available for compression.
pub struct UncompressedDataManager {
    /// Path to the folder containing all uncompressed data managed by the [`StorageEngine`].
    data_folder_path: PathBuf,
    /// The [`UncompressedSegments`](UncompressedSegment) while they are being built.
    uncompressed_data: HashMap<u64, SegmentBuilder>,
    /// FIFO queue of [`FinishedSegments`](FinishedSegment) that are ready for compression.
    finished_queue: VecDeque<FinishedSegment>,
    /// Cache of tag value hashes used to signify when to persist new unsaved tag combinations.
    tag_value_hashes: HashMap<String, u64>,
    /// How many bytes of memory that are left for storing [`UncompressedSegments`](UncompressedSegment).
    uncompressed_remaining_memory_in_bytes: usize,
}

impl UncompressedDataManager {
    pub fn new(data_folder_path: PathBuf) -> Self {
        Self {
            data_folder_path,
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            uncompressed_data: HashMap::new(),
            finished_queue: VecDeque::new(),
            tag_value_hashes: HashMap::new(),
            uncompressed_remaining_memory_in_bytes: UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
        }
    }

    // TODO: Use the new macro for downcasting the arrays.
    /// Parse `data_points` and insert it into the in-memory buffer. The data points are first parsed
    /// into multiple univariate time series based on `model_table`. These individual time series
    /// are then inserted into the storage engine. Return [`Ok`] if the data was successfully
    /// inserted, otherwise return [`Err`].
    pub fn insert_data_points(
        &mut self,
        model_table: &NewModelTableMetadata,
        data_points: &RecordBatch,
    ) -> Result<(), String> {
        let _span = info_span!("insert_data_points", table = model_table.name).entered();
        info!(
            "Received record batch with {} data points for the table '{}'.",
            data_points.num_rows(),
            model_table.name
        );

        // Prepare the timestamp column for iteration.
        let timestamp_index = model_table.timestamp_column_index as usize;
        let timestamps: &TimestampArray = data_points
            .column(timestamp_index)
            .as_any()
            .downcast_ref()
            .unwrap();

        // Prepare the tag columns for iteration.
        let tag_column_arrays: Vec<&StringArray> = model_table
            .tag_column_indices
            .iter()
            .map(|index| {
                data_points.column(*index as usize)
                    .as_any()
                    .downcast_ref()
                    .unwrap()
            })
            .collect();

        // Prepare the field columns for iteration. The column index is saved with the corresponding array.
        let field_column_arrays: Vec<(usize, &ValueArray)> = model_table
            .schema
            .fields()
            .iter()
            .filter_map(|field| {
                let index = model_table.schema.index_of(field.name().as_str()).unwrap();

                // Field columns are the columns that are not the timestamp column or one of the tag columns.
                let not_timestamp_column = index != model_table.timestamp_column_index as usize;
                let not_tag_column = !model_table.tag_column_indices.contains(&(index as u8));

                if not_timestamp_column && not_tag_column {
                    Some((
                        index,
                        data_points.column(index as usize).as_any().downcast_ref().unwrap(),
                    ))
                } else {
                    None
                }
            })
            .collect();

        // For each row in the data, generate a tag hash, extract the individual measurements,
        // and insert them into the storage engine.
        for (index, timestamp) in timestamps.iter().enumerate() {
            let tag_values: Vec<String> = tag_column_arrays
                .iter()
                .map(|array| array.value(index).to_string())
                .collect();

            let tag_hash = self
                .get_or_compute_tag_hash(&model_table, &tag_values)
                .map_err(|error| format!("Tag hash could not be saved: {}", error.to_string()))?;

            // For each field column, generate the 64-bit key, create a single data point, and
            // insert the data point into the in-memory buffer.
            for (field_index, field_column_array) in &field_column_arrays {
                let key = tag_hash | *field_index as u64;
                let value = field_column_array.value(index);

                // unwrap() is safe to use since the timestamps array cannot contain null values.
                self.insert_data_point(key, timestamp.unwrap(), value);
            }
        }

        Ok(())
    }

    /// Return the tag hash for the given list of tag values either by retrieving it from the cache
    /// or, if it is a new combination of tag values, generating a new hash. If a new hash is
    /// created, the hash is saved both in the cache and persisted to the model_table_tags table. If
    /// the table could not be accessed, return [`rusqlite::Error`].
    fn get_or_compute_tag_hash(
        &mut self,
        model_table: &NewModelTableMetadata,
        tag_values: &Vec<String>,
    ) -> Result<u64, rusqlite::Error> {
        let cache_key = {
            let mut cache_key_list = tag_values.clone();
            cache_key_list.push(model_table.name.clone());

            cache_key_list.join(";")
        };

        // Check if the tag hash is in the cache. If it is, retrieve it. If it is not, create a new
        // one and save it both in the cache and in the model_table_tags table.
        if let Some(tag_hash) = self.tag_value_hashes.get(&cache_key) {
            Ok(*tag_hash)
        } else {
            // Generate the 54-bit tag hash based on the tag values of the record batch and model table name.
            let tag_hash = {
                let mut hasher = DefaultHasher::new();
                hasher.write(cache_key.as_bytes());

                // The 64-bit hash is shifted to make the 10 least significant bits 0.
                hasher.finish() << 10
            };

            // Save the tag hash in the cache and in the metadata database model_table_tags table.
            self.tag_value_hashes.insert(cache_key, tag_hash);

            // TODO: Move this to the metadata component when it exists.
            let tag_columns: String = model_table
                .tag_column_indices
                .iter()
                .map(|index| model_table.schema.field(*index as usize).name().clone())
                .collect::<Vec<String>>()
                .join(",");

            let values = tag_values
                .iter()
                .map(|value| format!("'{}'", value))
                .collect::<Vec<String>>()
                .join(",");

            let database_path = self.data_folder_path.join("metadata.sqlite3");
            let connection = Connection::open(database_path)?;

            // OR IGNORE is used to silently fail when trying to insert an already existing hash.
            connection.execute(
                format!(
                    "INSERT OR IGNORE INTO {}_tags (hash,{}) VALUES ({},{})",
                    model_table.name, tag_columns, tag_hash, values
                )
                .as_str(),
                (),
            )?;

            Ok(tag_hash)
        }
    }

    /// Insert a single data point into the in-memory buffer. Return [`OK`] if the data point was
    /// inserted successfully, otherwise return [`Err`].
    fn insert_data_point(&mut self, key: u64, timestamp: Timestamp, value: Value)  {
        let _span = info_span!("insert_data_point", key = key).entered();
        info!(
            "Inserting data point ({}, {}) into segment.",
            timestamp, value
        );

        if let Some(segment) = self.uncompressed_data.get_mut(&key) {
            info!("Found existing segment.");
            segment.insert_data(timestamp, value);

            if segment.is_full() {
                info!("Segment is full, moving it to the queue of finished segments.");

                // Since this is only reachable if the segment exists in the HashMap, unwrap is safe to use.
                let full_segment = self.uncompressed_data.remove(&key).unwrap();
                self.enqueue_segment(key, full_segment)
            }
        } else {
            info!("Could not find segment. Creating segment.");

            // If there is not enough memory for a new segment, spill a finished segment.
            if SegmentBuilder::get_memory_size() > self.uncompressed_remaining_memory_in_bytes {
                self.spill_finished_segment();
            }

            // Create a new segment and reduce the remaining amount of reserved memory by its size.
            let mut segment = SegmentBuilder::new();
            self.uncompressed_remaining_memory_in_bytes -= SegmentBuilder::get_memory_size();

            info!(
                "Created segment. Remaining reserved bytes: {}.",
                self.uncompressed_remaining_memory_in_bytes
            );

            segment.insert_data(timestamp, value);
            self.uncompressed_data.insert(key, segment);
        }
    }

    /// Remove the oldest [`FinishedSegment`] from the queue and return it. Return [`None`] if the
    /// queue of [`FinishedSegments`](FinishedSegment) is empty.
    pub fn get_finished_segment(&mut self) -> Option<FinishedSegment> {
        if let Some(finished_segment) = self.finished_queue.pop_front() {
            // Add the memory size of the removed FinishedSegment back to the remaining bytes.
            self.uncompressed_remaining_memory_in_bytes +=
                finished_segment.uncompressed_segment.get_memory_size();

            Some(finished_segment)
        } else {
            None
        }
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
        info!("Not enough memory to create segment. Spilling an already finished segment.");

        // Iterate through the finished segments to find a segment that is in memory.
        for finished in self.finished_queue.iter_mut() {
            if let Ok(file_path) = finished.spill_to_apache_parquet(self.data_folder_path.as_path())
            {
                // Add the size of the segment back to the remaining reserved bytes.
                self.uncompressed_remaining_memory_in_bytes += SegmentBuilder::get_memory_size();

                info!(
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
    use tempfile::{tempdir, TempDir};

    use crate::storage::BUILDER_CAPACITY;
    use crate::types::{ArrowTimestamp, ArrowValue};

    const KEY: u64 = 1;

    #[test]
    fn test_can_insert_record_batch() {
        let (temp_dir, mut data_manager) = create_uncompressed_data_manager();

        let (model_table, data) = get_uncompressed_data(1);
        create_model_table_tags_table(&model_table, temp_dir.path());
        data_manager.insert_data_points(&model_table, &data);

        // Two separate builders are created since the inserted data has two field columns.
        assert_eq!(data_manager.uncompressed_data.keys().len(), 2)
    }

    #[test]
    fn test_can_insert_record_batch_with_multiple_data_points() {
        let (temp_dir, mut data_manager) = create_uncompressed_data_manager();

        let (model_table, data) = get_uncompressed_data(2);
        create_model_table_tags_table(&model_table, temp_dir.path());

        data_manager.insert_data_points(&model_table, &data);

        // Since the tag is different for each data point, 4 separate builders should be created.
        assert_eq!(data_manager.uncompressed_data.keys().len(), 4)
    }

    #[test]
    fn test_get_new_tag_hash() {
        let (temp_dir, mut data_manager) = create_uncompressed_data_manager();

        let (model_table, _data) = get_uncompressed_data(2);
        create_model_table_tags_table(&model_table, temp_dir.path());

        let result = data_manager.get_or_compute_tag_hash(&model_table, &vec!["tag1".to_owned()]);
        assert!(result.is_ok());

        // When a new tag hash is retrieved, the hash should be saved in the cache.
        assert_eq!(data_manager.tag_value_hashes.keys().len(), 1);

        // It should also be saved in the metadata database table.
        let database_path = temp_dir.path().join("metadata.sqlite3");
        let connection = Connection::open(database_path).unwrap();

        let mut statement = connection.prepare("SELECT * FROM model_table_tags").unwrap();
        let mut rows = statement.query(()).unwrap();

        assert_eq!(rows.next().unwrap().unwrap().get::<usize, String>(1).unwrap(), "tag1");
    }

    #[test]
    fn test_get_existing_tag_hash() {
        let (temp_dir, mut data_manager) = create_uncompressed_data_manager();

        let (model_table, _data) = get_uncompressed_data(1);
        create_model_table_tags_table(&model_table, temp_dir.path());

        data_manager.get_or_compute_tag_hash(&model_table, &vec!["tag1".to_owned()]);
        assert_eq!(data_manager.tag_value_hashes.keys().len(), 1);

        // When getting the same tag hash again, it should just be retrieved from the cache.
        let result = data_manager.get_or_compute_tag_hash(&model_table, &vec!["tag1".to_owned()]);

        assert!(result.is_ok());
        assert_eq!(data_manager.tag_value_hashes.keys().len(), 1);
    }

    /// Create a record batch with data that resembles uncompressed data with a single tag and two
    /// field columns. The returned data has `row_count` rows, with a different tag for each row.
    /// Also create model table metadata for a model table that matches the created data.
    fn get_uncompressed_data(row_count: usize) -> (NewModelTableMetadata, RecordBatch) {
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
        ).unwrap();

        let model_table_metadata = NewModelTableMetadata::try_new(
            "model_table".to_owned(),
            schema,
            vec![0],
            1
        ).unwrap();

        (model_table_metadata, data)
    }

    // TODO: This is duplicated from a function in remote. Change this when the metadata component exists.
    /// Create the table in the metadata database that contains the tag hashes.
    fn create_model_table_tags_table(model_table_metadata: &NewModelTableMetadata, path: &Path) {
        let database_path = path.join("metadata.sqlite3");
        let connection = Connection::open(database_path).unwrap();

        // Add a column definition for each tag field in the schema.
        let tag_columns: String = model_table_metadata
            .tag_column_indices
            .iter()
            .map(|index| {
                let field = model_table_metadata.schema.field(*index as usize);
                format!("{} TEXT NOT NULL", field.name())
            })
            .collect::<Vec<String>>()
            .join(",");

        // Create a table_name_tags SQLite table to save the 54-bit tag hashes when ingesting data.
        // The query is executed with a formatted string since CREATE TABLE cannot take parameters.
        connection.execute(
            format!(
                "CREATE TABLE {}_tags (hash BIGINT PRIMARY KEY, {})",
                model_table_metadata.name, tag_columns
            )
            .as_str(),
            (),
        ).unwrap();
    }

    #[test]
    fn test_can_insert_data_point_into_new_segment() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
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
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
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
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        insert_data_points(BUILDER_CAPACITY, &mut data_manager, KEY);

        assert!(data_manager.get_finished_segment().is_some());
    }

    #[test]
    fn test_can_get_multiple_finished_segments_when_multiple_finished() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        insert_data_points(BUILDER_CAPACITY * 2, &mut data_manager, KEY);

        assert!(data_manager.get_finished_segment().is_some());
        assert!(data_manager.get_finished_segment().is_some());
    }

    #[test]
    fn test_cannot_get_finished_segment_when_not_finished() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();

        assert!(data_manager.get_finished_segment().is_none());
    }

    #[test]
    fn test_spill_first_finished_segment_if_out_of_memory() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
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
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
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
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();

        insert_data_points(BUILDER_CAPACITY, &mut data_manager, KEY);
        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();
        data_manager.spill_finished_segment();

        assert!(remaining_memory < data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_decremented_when_creating_new_segment() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        let reserved_memory = data_manager.uncompressed_remaining_memory_in_bytes;

        insert_data_points(1, &mut data_manager, KEY);

        assert!(reserved_memory > data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_incremented_when_popping_in_memory() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
        insert_data_points(BUILDER_CAPACITY, &mut data_manager, KEY);

        let remaining_memory = data_manager.uncompressed_remaining_memory_in_bytes.clone();
        data_manager.get_finished_segment();

        assert!(remaining_memory < data_manager.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_not_incremented_when_popping_spilled() {
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
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
        let (_temp_dir, mut data_manager) = create_uncompressed_data_manager();
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
    fn create_uncompressed_data_manager() -> (TempDir, UncompressedDataManager) {
        let temp_dir = tempdir().unwrap();

        let data_folder_path = temp_dir.path().to_path_buf();
        (temp_dir, UncompressedDataManager::new(data_folder_path))
    }
}
