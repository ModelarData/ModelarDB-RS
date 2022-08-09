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

//! Converts raw MQTT messages to uncompressed data points, stores uncompressed data points temporarily
//! in an in-memory buffer that spills to Apache Parquet files, and stores data points compressed as
//! models in memory to batch compressed data before saving it to Apache Parquet files.

mod data_point;
mod segment;
mod time_series;

use std::collections::vec_deque::VecDeque;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader};
use datafusion::parquet::basic::Encoding;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use paho_mqtt::Message;
use tracing::{info, info_span};

use crate::storage::data_point::DataPoint;
use crate::storage::segment::{FinishedSegment, SegmentBuilder};
use crate::storage::time_series::CompressedTimeSeries;
use crate::types::{ArrowTimestamp, ArrowValue};

// TODO: Look into custom errors for all errors in storage engine.

// Note that the capacity has to be a multiple of 64 bytes to avoid the actual capacity
// being larger due to internal alignment when allocating memory for the builders. The reserved
// bytes for compressed data is a signed integer since compressed data is inserted first and the
// remaining bytes are checked after. This means that the remaining bytes can be negative briefly
// until compressed data is saved to disk.
const BUILDER_CAPACITY: usize = 64;
const UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES: usize = 5000;
const COMPRESSED_RESERVED_MEMORY_IN_BYTES: isize = 5000;

/// The expected [first four bytes of any Apache Parquet file].
///
/// [first four bytes of any Apache Parquet file]: https://en.wikipedia.org/wiki/List_of_file_signatures
const APACHE_PARQUET_FILE_SIGNATURE: &[u8] = &[80, 65, 82, 49]; // PAR1.

/// Manages all uncompressed and compressed data, both while being built and when finished.
pub struct StorageEngine {
    // TODO: Maybe change this to an actual Path instead of a String.
    /// Path to the folder containing all uncompressed and compressed data managed by the storage engine.
    storage_folder_path: String,
    /// The uncompressed segments while they are being built.
    uncompressed_data: HashMap<String, SegmentBuilder>,
    /// Prioritized queue of finished segments that are ready for compression.
    finished_queue: VecDeque<FinishedSegment>,
    /// How many bytes of memory that are left for storing uncompressed segments.
    uncompressed_remaining_memory_in_bytes: usize,
    /// The compressed segments before they are saved to persistent storage.
    compressed_data: HashMap<String, CompressedTimeSeries>,
    /// Prioritized queue of time series keys referring to data that can be saved to persistent storage.
    compressed_queue: VecDeque<String>,
    /// How many bytes of memory that are left for storing compressed segments.
    compressed_remaining_memory_in_bytes: isize,
}

impl StorageEngine {
    pub fn new(storage_folder_path: String) -> Self {
        Self {
            storage_folder_path,
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            uncompressed_data: HashMap::new(),
            finished_queue: VecDeque::new(),
            compressed_data: HashMap::new(),
            compressed_queue: VecDeque::new(),
            uncompressed_remaining_memory_in_bytes: UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            compressed_remaining_memory_in_bytes: COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        }
    }

    /// Parse `message` and insert it into the in-memory buffer. Return Ok if the message was
    /// successfully inserted, otherwise return Err.
    pub fn insert_message(&mut self, message: Message) -> Result<(), String> {
        let data_point = DataPoint::from_message(&message)?;
        let key = data_point.generate_unique_key();
        let _span = info_span!("insert_message", key = key.clone()).entered();

        info!("Inserting data point '{}' into segment.", data_point);

        if let Some(segment) = self.uncompressed_data.get_mut(&key) {
            info!("Found existing segment.");

            segment.insert_data(&data_point);

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

            segment.insert_data(&data_point);
            self.uncompressed_data.insert(key, segment);
        }

        Ok(())
    }

    /// Remove the oldest finished segment from the queue and return it. Return `None` if the queue
    /// of finished segments is empty.
    pub fn get_finished_segment(&mut self) -> Option<FinishedSegment> {
        if let Some(finished_segment) = self.finished_queue.pop_front() {
            // Add the memory size of the removed finished segment back to the remaining bytes.
            self.uncompressed_remaining_memory_in_bytes +=
                finished_segment.uncompressed_segment.get_memory_size();

            Some(finished_segment)
        } else {
            None
        }
    }

    /// Insert `segment` into the in-memory compressed time series buffer.
    pub fn insert_compressed_data(&mut self, key: String, segment: RecordBatch) {
        let _span = info_span!("insert_compressed_segment", key = key.clone()).entered();
        info!(
            "Inserting batch with {} rows into compressed time series.",
            segment.num_rows()
        );

        // Since the compressed segment is already in memory, insert the segment in to the structure
        // first and check if the reserved memory limit is exceeded after.
        let segment_size = if let Some(time_series) = self.compressed_data.get_mut(&key) {
            info!("Found existing compressed time series.");

            time_series.append_segment(segment)
        } else {
            info!("Could not find compressed time series. Creating compressed time series.");

            let mut time_series = CompressedTimeSeries::new();
            let segment_size = time_series.append_segment(segment);

            self.compressed_data.insert(key.clone(), time_series);
            self.compressed_queue.push_back(key.clone());

            segment_size
        };

        self.compressed_remaining_memory_in_bytes -= segment_size as isize;

        // If the reserved memory limit is exceeded, save compressed data to disk.
        if self.compressed_remaining_memory_in_bytes < 0 {
            self.save_compressed_data();
        }
    }

    /// Return the record batch schema used for uncompressed segments.
    pub fn get_uncompressed_segment_schema() -> Schema {
        Schema::new(vec![
            Field::new("timestamps", ArrowTimestamp::DATA_TYPE, false),
            Field::new("values", ArrowValue::DATA_TYPE, false),
        ])
    }

    /// Return the record batch schema used for compressed segments.
    pub fn get_compressed_segment_schema() -> Schema {
        Schema::new(vec![
            Field::new("model_type_id", DataType::UInt8, false),
            Field::new("timestamps", DataType::Binary, false),
            Field::new("start_time", ArrowTimestamp::DATA_TYPE, false),
            Field::new("end_time", ArrowTimestamp::DATA_TYPE, false),
            Field::new("values", DataType::Binary, false),
            Field::new("min_value", ArrowValue::DATA_TYPE, false),
            Field::new("max_value", ArrowValue::DATA_TYPE, false),
            Field::new("error", DataType::Float32, false),
        ])
    }

    // TODO: Test using more efficient encoding. Plain encoding makes it easier to read the files externally.
    /// Write `batch` to an Apache Parquet file at the location given by `path`. `path` must use the
    /// extension '.parquet'. Return Ok if the file was written successfully, otherwise `ParquetError`.
    pub fn write_batch_to_apache_parquet_file(
        batch: RecordBatch,
        path: &Path,
    ) -> Result<(), ParquetError> {
        let error = ParquetError::General(
            format!("Apache Parquet file at path '{}' could not be created.", path.display())
        );

        // Check if the extension of the given path is correct.
        if path.extension().and_then(OsStr::to_str) == Some("parquet") {
            let file = File::create(path).map_err(|_e| error)?;
            let props = WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_encoding(Encoding::PLAIN)
                .build();

            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
            writer.write(&batch)?;
            writer.close()?;

            Ok(())
        } else {
            Err(error)
        }
    }

    /// Read all rows from the Apache Parquet file at the location given by `path` and return them
    /// in a record batch. If the file could not be read successfully, `ParquetError` is returned.
    pub fn read_entire_apache_parquet_file(path: &Path) -> Result<RecordBatch, ParquetError> {
        let error = ParquetError::General(
            format!("Apache Parquet file at path '{}' could not be read.", path.display())
        );

        let file = File::open(path).map_err(|_e| error.clone())?;
        let reader = SerializedFileReader::new(file)?;

        // Extract the total row count from the file metadata.
        let apache_parquet_metadata = reader.metadata();
        let row_count = apache_parquet_metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .sum::<i64>() as usize;

        // Read the data and convert it to a record batch.
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
        let mut record_batch_reader = arrow_reader.get_record_reader(row_count)?;

        let batch = record_batch_reader
            .next()
            .ok_or_else(|| error)??;

        Ok(batch)
    }

    /// Return `true` if `path` is a readable Apache Parquet file, otherwise `false`.
    pub fn is_path_an_apache_parquet_file(path: &Path) -> bool {
        if let Ok(mut file) = File::open(path) {
            let mut first_four_bytes = vec![0u8; 4];
            let _ = file.read_exact(&mut first_four_bytes);
            first_four_bytes == APACHE_PARQUET_FILE_SIGNATURE
        } else {
            false
        }
    }

    /// Move `segment_builder` to the queue of finished segments.
    fn enqueue_segment(&mut self, key: String, segment_builder: SegmentBuilder) {
        let finished_segment = FinishedSegment {
            key,
            uncompressed_segment: Box::new(segment_builder),
        };

        self.finished_queue.push_back(finished_segment);
    }

    /// Spill the first in-memory finished segment in the queue of finished segments. If no
    /// in-memory finished segments could be found, panic.
    fn spill_finished_segment(&mut self) {
        info!("Not enough memory to create segment. Spilling an already finished segment.");

        // Iterate through the finished segments to find a segment that is in memory.
        for finished in self.finished_queue.iter_mut() {
            if let Ok(path) = finished.spill_to_apache_parquet(self.storage_folder_path.clone()) {
                // Add the size of the segment back to the remaining reserved bytes.
                self.uncompressed_remaining_memory_in_bytes += SegmentBuilder::get_memory_size();

                info!(
                    "Spilled the segment to '{}'. Remaining reserved bytes: {}.",
                    path, self.uncompressed_remaining_memory_in_bytes
                );
                return ();
            }
        }

        // TODO: All uncompressed and compressed data should be saved to disk first.
        // If not able to find any in-memory finished segments, we should panic.
        panic!("Not enough reserved memory to hold all necessary segment builders.");
    }

    /// Save compressed time series to disk until the reserved memory limit is no longer exceeded.
    fn save_compressed_data(&mut self) {
        info!("Out of memory to store compressed data. Saving compressed data to disk.");

        while self.compressed_remaining_memory_in_bytes < 0 {
            let key = self.compressed_queue.pop_front().unwrap();
            info!("Saving compressed time series with key '{}' to disk.", key);

            let mut time_series = self.compressed_data.remove(&key).unwrap();
            let time_series_size = time_series.size_in_bytes.clone();

            let folder_path = format!("{}/{}", self.storage_folder_path.clone(), key.clone());
            time_series.save_to_apache_parquet(folder_path);

            self.compressed_remaining_memory_in_bytes += time_series_size as isize;

            info!(
                "Saved {} bytes of compressed data to disk. Remaining reserved bytes: {}.",
                time_series_size, self.compressed_remaining_memory_in_bytes
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::read_dir;
    use std::path::PathBuf;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use tempfile::{tempdir, TempDir};

    use crate::storage::time_series::test_util;

    // Tests for uncompressed data.
    #[test]
    fn test_cannot_insert_invalid_message() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        let message = Message::new("ModelarDB/test", "invalid", 1);
        storage_engine.insert_message(message.clone());

        assert!(storage_engine.uncompressed_data.is_empty());
    }

    #[test]
    fn test_can_insert_message_into_new_segment() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let key = insert_generated_message(&mut storage_engine, "ModelarDB/test".to_owned());

        assert!(storage_engine.uncompressed_data.contains_key(&key));
        assert_eq!(storage_engine.uncompressed_data.get(&key).unwrap().get_length(), 1);
    }

    #[test]
    fn test_can_insert_message_into_existing_segment() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let key = insert_multiple_messages(2, &mut storage_engine);

        assert!(storage_engine.uncompressed_data.contains_key(&key));
        assert_eq!(storage_engine.uncompressed_data.get(&key).unwrap().get_length(), 2);
    }

    #[test]
    fn test_can_get_finished_segment_when_finished() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let key = insert_multiple_messages(BUILDER_CAPACITY, &mut storage_engine);

        assert!(storage_engine.get_finished_segment().is_some());
    }

    #[test]
    fn test_can_get_multiple_finished_segments_when_multiple_finished() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let key = insert_multiple_messages(BUILDER_CAPACITY * 2, &mut storage_engine);

        assert!(storage_engine.get_finished_segment().is_some());
        assert!(storage_engine.get_finished_segment().is_some());
    }

    #[test]
    fn test_cannot_get_finished_segment_when_not_finished() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        assert!(storage_engine.get_finished_segment().is_none());
    }

    #[test]
    fn test_spill_first_finished_segment_if_out_of_memory() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let reserved_memory = storage_engine.uncompressed_remaining_memory_in_bytes;

        // Insert messages into the storage engine until a segment is spilled to Apache Parquet.
        // If there is enough memory to hold n full segments, we need n + 1 to spill a segment.
        let max_full_segments = reserved_memory / SegmentBuilder::get_memory_size();
        let message_count = (max_full_segments * BUILDER_CAPACITY) + 1;
        insert_multiple_messages(message_count, &mut storage_engine);

        // The first finished segment should have a memory size of 0 since it is spilled to disk.
        let first_finished = storage_engine.finished_queue.pop_front().unwrap();
        assert_eq!(first_finished.uncompressed_segment.get_memory_size(), 0);

        // The finished segment should be spilled to the "uncompressed" folder under the key.
        let storage_folder_path = Path::new(&storage_engine.storage_folder_path);
        let uncompressed_path = storage_folder_path.join("modelardb-test/uncompressed");
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1);
    }

    #[test]
    fn test_ignore_already_spilled_segments_when_spilling() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        insert_multiple_messages(BUILDER_CAPACITY * 2, &mut storage_engine);

        storage_engine.spill_finished_segment();
        // When spilling one more, the first segment should be ignored since it is already spilled.
        storage_engine.spill_finished_segment();

        storage_engine.finished_queue.pop_front();
        // The second finished segment should have a memory size of 0 since it is spilled to disk.
        let second_finished = storage_engine.finished_queue.pop_front().unwrap();
        assert_eq!(second_finished.uncompressed_segment.get_memory_size(), 0);

        // The finished segments should be spilled to the "uncompressed" folder under the key.
        let storage_folder_path = Path::new(&storage_engine.storage_folder_path);
        let uncompressed_path = storage_folder_path.join("modelardb-test/uncompressed");
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 2);
    }

    #[test]
    fn test_remaining_memory_incremented_when_spilling_finished_segment() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        insert_multiple_messages(BUILDER_CAPACITY, &mut storage_engine);
        let remaining_memory = storage_engine.uncompressed_remaining_memory_in_bytes.clone();
        storage_engine.spill_finished_segment();

        assert!(remaining_memory < storage_engine.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_decremented_when_creating_new_segment() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let reserved_memory = storage_engine.uncompressed_remaining_memory_in_bytes;

        insert_generated_message(&mut storage_engine, "ModelarDB/test".to_owned());

        assert!(reserved_memory > storage_engine.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_incremented_when_popping_in_memory() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let key = insert_multiple_messages(BUILDER_CAPACITY, &mut storage_engine);

        let remaining_memory = storage_engine.uncompressed_remaining_memory_in_bytes.clone();
        storage_engine.get_finished_segment();

        assert!(remaining_memory < storage_engine.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    fn test_remaining_memory_not_incremented_when_popping_spilled() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let key = insert_multiple_messages(BUILDER_CAPACITY, &mut storage_engine);

        storage_engine.spill_finished_segment();
        let remaining_memory = storage_engine.uncompressed_remaining_memory_in_bytes.clone();

        // Since the finished segment is not in memory, the remaining memory should not increase when popped.
        storage_engine.get_finished_segment();

        assert_eq!(remaining_memory, storage_engine.uncompressed_remaining_memory_in_bytes);
    }

    #[test]
    #[should_panic(expected = "Not enough reserved memory to hold all necessary segment builders.")]
    fn test_panic_if_not_enough_reserved_memory() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let reserved_memory = storage_engine.uncompressed_remaining_memory_in_bytes;

        // If there is enough reserved memory to hold n builders, we need to create n + 1 to panic.
        for i in 0..(reserved_memory / SegmentBuilder::get_memory_size()) + 1 {
            insert_generated_message(&mut storage_engine, i.to_string());
        }
    }

    /// Generate `count` data points for the same time series and insert them into `storage_engine`.
    /// Return the key, which is the same for all generated data points.
    fn insert_multiple_messages(count: usize, storage_engine: &mut StorageEngine) -> String {
        let mut key = String::new();

        for _ in 0..count {
            key = insert_generated_message(storage_engine, "ModelarDB/test".to_owned());
        }

        key
    }

    /// Generate a data point and insert it into `storage_engine`. Return the data point key.
    fn insert_generated_message(storage_engine: &mut StorageEngine, topic: String) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let payload = format!("[{}, 30]", timestamp);
        let message = Message::new(topic, payload, 1);

        storage_engine.insert_message(message.clone());

        DataPoint::from_message(&message)
            .unwrap()
            .generate_unique_key()
    }

    // Tests for compressed data.
    #[test]
    #[should_panic(expected = "Schema of record batch does not match compressed segment schema.")]
    fn test_panic_if_inserting_invalid_compressed_segment() {
        let invalid = test_util::get_invalid_compressed_segment_record_batch();
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        storage_engine.insert_compressed_data("key".to_owned(), invalid);
    }

    #[test]
    fn test_can_insert_compressed_segment_into_new_time_series() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        storage_engine.insert_compressed_data("key".to_owned(), segment);

        assert!(storage_engine.compressed_data.contains_key("key"));
        assert_eq!(storage_engine.compressed_queue.pop_front().unwrap(), "key");
        assert!(storage_engine.compressed_data.get("key").unwrap().size_in_bytes > 0);
    }

    #[test]
    fn test_can_insert_compressed_segment_into_existing_time_series() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        storage_engine.insert_compressed_data("key".to_owned(), segment.clone());
        let previous_size = storage_engine.compressed_data.get("key").unwrap().size_in_bytes;
        storage_engine.insert_compressed_data("key".to_owned(), segment);

        assert!(storage_engine.compressed_data.get("key").unwrap().size_in_bytes > previous_size);
    }

    #[test]
    fn test_save_first_compressed_time_series_if_out_of_memory() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let reserved_memory = storage_engine.compressed_remaining_memory_in_bytes as usize;

        // Insert compressed data into the storage engine until data is saved to Apache Parquet.
        let max_compressed_segments = reserved_memory / test_util::COMPRESSED_SEGMENT_SIZE;
        for _ in 0..max_compressed_segments + 1 {
            storage_engine.insert_compressed_data("modelardb-test".to_owned(), segment.clone());
        }

        // The compressed data should be saved to the "compressed" folder under the key.
        let storage_folder_path = Path::new(&storage_engine.storage_folder_path);
        let compressed_path = storage_folder_path.join("modelardb-test/compressed");
        assert_eq!(compressed_path.read_dir().unwrap().count(), 1);
    }

    #[test]
    fn test_remaining_bytes_decremented_when_inserting_compressed_segment() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let reserved_memory = storage_engine.compressed_remaining_memory_in_bytes;

        storage_engine.insert_compressed_data("key".to_owned(), segment);

        assert!(reserved_memory > storage_engine.compressed_remaining_memory_in_bytes);
    }

	#[test]
	fn test_remaining_memory_incremented_when_saving_compressed_time_series() {
        let segment = test_util::get_compressed_segment_record_batch();
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        storage_engine.insert_compressed_data("modelardb-test".to_owned(), segment.clone());

        // Set the remaining memory to a negative value since data is only saved when out of memory.
        storage_engine.compressed_remaining_memory_in_bytes = -1;
        storage_engine.save_compressed_data();

        assert!(-1 < storage_engine.compressed_remaining_memory_in_bytes);
	}

	/// Create the storage engine with a folder that is automatically deleted once the test is finished.
	fn create_storage_engine() -> (TempDir, StorageEngine) {
		let temp_dir = tempdir().unwrap();
		let storage_folder_path = temp_dir.path().to_str().unwrap().to_string();

        (temp_dir, StorageEngine::new(storage_folder_path))
	}

    // Tests for Apache Parquet.
    #[test]
    fn test_write_batch_to_apache_parquet_file() {
        let temp_dir = tempdir().unwrap();
        let batch = test_util::get_compressed_segment_record_batch();

        let parquet_path = temp_dir.path().join("test.parquet");
        StorageEngine::write_batch_to_apache_parquet_file(batch, parquet_path.as_path());

        assert!(parquet_path.exists());
    }

    #[test]
    fn test_write_empty_batch_to_apache_parquet_file() {
        let schema = Schema::new(vec![]);
        let batch = RecordBatch::new_empty(Arc::new(schema));

        let temp_dir = tempdir().unwrap();
        let parquet_path = temp_dir.path().join("empty.parquet");
        StorageEngine::write_batch_to_apache_parquet_file(batch, parquet_path.as_path());

        assert!(parquet_path.exists());
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
        let temp_dir = tempdir().unwrap();
        let batch = test_util::get_compressed_segment_record_batch();

        let parquet_path = temp_dir.path().join(file_name);
        let result =
            StorageEngine::write_batch_to_apache_parquet_file(batch, parquet_path.as_path());

        assert!(result.is_err());
        assert!(!parquet_path.exists());
    }

    #[test]
    fn test_read_entire_apache_parquet_file() {
        let file_name = "test.parquet".to_owned();
        let (_temp_dir, path, batch) = create_apache_parquet_file_in_temp_dir(file_name);

        let result = StorageEngine::read_entire_apache_parquet_file(path.as_path());

        assert!(result.is_ok());
        assert_eq!(batch, result.unwrap());
    }

    #[test]
    fn test_read_from_non_apache_parquet_file() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.txt");
        File::create(path.clone()).unwrap();

        let result = StorageEngine::read_entire_apache_parquet_file(path.as_path());

        assert!(result.is_err());
    }

    #[test]
    fn test_read_from_non_existent_path() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("none.parquet");
        let result = StorageEngine::read_entire_apache_parquet_file(path.as_path());

        assert!(result.is_err());
    }

    #[test]
    fn test_is_parquet_path_apache_parquet_file() {
        let file_name = "test.parquet".to_owned();
        let (_temp_dir, path, _batch) = create_apache_parquet_file_in_temp_dir(file_name);

        assert!(StorageEngine::is_path_an_apache_parquet_file(path.as_path()));
    }

    /// Create an Apache Parquet file from a generated record batch in the temp dir.
    fn create_apache_parquet_file_in_temp_dir(file_name: String) -> (TempDir, PathBuf, RecordBatch) {
        let temp_dir = tempdir().unwrap();
        let batch = test_util::get_compressed_segment_record_batch();

        let parquet_path = temp_dir.path().join(file_name);
        StorageEngine::write_batch_to_apache_parquet_file(batch.clone(), parquet_path.as_path());

        (temp_dir, parquet_path, batch)
    }

    #[test]
    fn test_is_non_parquet_path_apache_parquet_file() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("test.txt");
        File::create(path.clone()).unwrap();

        assert!(path.exists());
        assert!(!StorageEngine::is_path_an_apache_parquet_file(path.as_path()));
    }
}
