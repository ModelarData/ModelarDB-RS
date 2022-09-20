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

//! Converts a batch of data to uncompressed data points, stores uncompressed data points temporarily
//! in an in-memory buffer that spills to Apache Parquet files, and stores data points compressed as
//! models in memory to batch compressed data before saving it to Apache Parquet files.

mod segment;
mod time_series;
mod uncompressed_data_manager;
mod compressed_data_manager;

use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader};
use datafusion::parquet::basic::Encoding;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use object_store::ObjectMeta;
use object_store::path::Path as ObjectStorePath;
use chrono::DateTime;

use crate::catalog::NewModelTableMetadata;
use crate::errors::ModelarDBError;
use crate::storage::compressed_data_manager::CompressedDataManager;
use crate::storage::segment::FinishedSegment;
use crate::storage::uncompressed_data_manager::UncompressedDataManager;
use crate::types::{ArrowTimestamp, ArrowValue, Timestamp};

// TODO: Look into custom errors for all errors in storage engine.

/// The expected [first four bytes of any Apache Parquet file].
///
/// [first four bytes of any Apache Parquet file]: https://en.wikipedia.org/wiki/List_of_file_signatures
const APACHE_PARQUET_FILE_SIGNATURE: &[u8] = &[80, 65, 82, 49]; // PAR1.

/// Note that the capacity has to be a multiple of 64 bytes to avoid the actual capacity
/// being larger due to internal alignment when allocating memory for the builders.
const BUILDER_CAPACITY: usize = 64;

/// Manages all uncompressed and compressed data, both while being built and when finished.
pub struct StorageEngine {
    /// Manager that contains and controls all uncompressed data.
    uncompressed_data_manager: UncompressedDataManager,
    /// Manager that contains and controls all compressed data.
    compressed_data_manager: CompressedDataManager,
}

impl StorageEngine {
    pub fn new(data_folder_path: PathBuf) -> Self {
        Self {
            uncompressed_data_manager: UncompressedDataManager::new(data_folder_path.clone()),
            compressed_data_manager: CompressedDataManager::new(data_folder_path),
        }
    }

    /// Pass `data_points` to [`UncompressedDataManager`]. Return [`Ok`] if the data was
    /// successfully inserted, otherwise return [`Err`].
    pub fn insert_data_points(
        &mut self,
        model_table: &NewModelTableMetadata,
        data_points: &RecordBatch
    ) -> Result<(), String> {
        self.uncompressed_data_manager.insert_data_points(model_table, data_points)
    }

    /// Retrieve the oldest [`FinishedSegment`] from [`UncompressedDataManager`] and return it.
    /// Return [`None`] if there are no [`FinishedSegments`](FinishedSegment).
    pub fn get_finished_segment(&mut self) -> Option<FinishedSegment> {
        self.uncompressed_data_manager.get_finished_segment()
    }

    /// Pass `segment` to [`CompressedDataManager`].
    pub fn insert_compressed_segment(&mut self, key: u64, segment: RecordBatch) {
        self.compressed_data_manager.insert_compressed_segment(key, segment)
    }

    /// Retrieve the compressed files that correspond to `keys` within the given range of time.
    /// If `keys` contain a key that does not exist or the end time is before the start time,
    /// [`DataRetrievalError`](ModelarDBError::DataRetrievalError) is returned.
    pub fn get_compressed_files(
        &mut self,
        keys: &[u64],
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>
    ) -> Result<Vec<ObjectMeta>, ModelarDBError> {
        let start_time = start_time.unwrap_or(0);
        let end_time = end_time.unwrap_or(i64::MAX);
        let mut compressed_files: Vec<ObjectMeta> = vec![];

        if start_time > end_time {
            return Err(ModelarDBError::DataRetrievalError(format!(
                "Start time '{}' cannot be after end time '{}'.",
                start_time,
                end_time
            )));
        };

        for key in keys {
            // For each key, list the files that contain compressed data.
            let key_files = self.compressed_data_manager.save_and_get_saved_compressed_files(key)?;

            // Prune the files based on the time range the file covers and convert to object meta.
            let pruned_files = key_files.iter().filter_map(|file_path| {
                if compressed_data_manager::is_compressed_file_within_time_range(file_path, start_time, end_time) {
                    // unwrap() is safe since we already know the file exists.
                    let metadata = fs::metadata(&file_path).unwrap();

                    // Create an object that contains the file path and extra metadata about the file.
                    Some(ObjectMeta {
                        location: ObjectStorePath::from(file_path.to_str().unwrap()),
                        last_modified: DateTime::from(metadata.modified().unwrap()),
                        size: metadata.len() as usize
                    })
                } else {
                    None
                }
            });

            compressed_files.extend(pruned_files);
        };

        Ok(compressed_files)
    }

    // TODO: Move to configuration struct and have a single Arc.
    /// Return the [`RecordBatch`] schema used for uncompressed segments.
    pub fn get_uncompressed_segment_schema() -> Schema {
        Schema::new(vec![
            Field::new("timestamps", ArrowTimestamp::DATA_TYPE, false),
            Field::new("values", ArrowValue::DATA_TYPE, false),
        ])
    }

    // TODO: Move to configuration struct and have a single Arc.
    /// Return the [`RecordBatch`] schema used for compressed segments.
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
    /// Write `batch` to an Apache Parquet file at the location given by `file_path`. `file_path`
    /// must use the extension '.parquet'. Return [`Ok`] if the file was written successfully,
    /// otherwise [`ParquetError`].
    pub fn write_batch_to_apache_parquet_file(
        batch: RecordBatch,
        file_path: &Path,
    ) -> Result<(), ParquetError> {
        let error = ParquetError::General(
            format!("Apache Parquet file at path '{}' could not be created.", file_path.display())
        );

        // Check if the extension of the given path is correct.
        if file_path.extension().and_then(OsStr::to_str) == Some("parquet") {
            let file = File::create(file_path).map_err(|_e| error)?;
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

    /// Read all rows from the Apache Parquet file at the location given by `file_path` and return them
    /// in a [`RecordBatch`]. If the file could not be read successfully, [`ParquetError`] is returned.
    pub fn read_entire_apache_parquet_file(file_path: &Path) -> Result<RecordBatch, ParquetError> {
        let error = ParquetError::General(
            format!("Apache Parquet file at path '{}' could not be read.", file_path.display())
        );

        let file = File::open(file_path).map_err(|_e| error.clone())?;
        let reader = SerializedFileReader::new(file)?;

        // Extract the total row count from the file metadata.
        let apache_parquet_metadata = reader.metadata();
        let row_count = apache_parquet_metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .sum::<i64>() as usize;

        // Read the data and convert it to a RecordBatch.
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
        let mut record_batch_reader = arrow_reader.get_record_reader(row_count)?;

        let batch = record_batch_reader
            .next()
            .ok_or_else(|| error)??;

        Ok(batch)
    }

    /// Return `true` if `file_path` is a readable Apache Parquet file, otherwise `false`.
    pub fn is_path_an_apache_parquet_file(file_path: &Path) -> bool {
        if let Ok(mut file) = File::open(file_path) {
            let mut first_four_bytes = vec![0u8; 4];
            let _ = file.read_exact(&mut first_four_bytes);
            first_four_bytes == APACHE_PARQUET_FILE_SIGNATURE
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    use tempfile::{tempdir, TempDir};

    use crate::types::TimestampArray;

    // Tests for get_compressed_files().
    #[test]
    fn test_can_get_compressed_files_for_keys() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        // Insert compressed segments with multiple different keys.
        let segment = test_util::get_compressed_segment_record_batch();
        storage_engine.compressed_data_manager.insert_compressed_segment(1, segment.clone());
        storage_engine.compressed_data_manager.insert_compressed_segment(2, segment);

        let result = storage_engine.get_compressed_files(&[1, 2], None, None);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_cannot_get_compressed_files_for_non_existent_key() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        let result = storage_engine.get_compressed_files(&[1], None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_can_get_compressed_files_with_start_time() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let (segment_1, _segment_2) = insert_separated_segments(&mut storage_engine, 1, 2, 0);

        // If we have a start time after the first segments ends, only the file from the second
        // segment should be retrieved.
        let end_times: &TimestampArray = segment_1.column(3).as_any().downcast_ref().unwrap();
        let start_time = Some(end_times.value(end_times.len() - 1) + 100);

        let result = storage_engine.get_compressed_files(&[1, 2], start_time, None);
        let files = result.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the key of the second segment.
        let file_path = files.get(0).unwrap().location.to_string();
        assert!(file_path.contains("2/compressed"));
    }

    #[test]
    fn test_can_get_compressed_files_with_end_time() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();
        let (_segment_1, segment_2) = insert_separated_segments(&mut storage_engine, 1, 2, 0);

        // If we have an end time before the second segment starts, only the file from the first
        // segment should be retrieved.
        let start_times: &TimestampArray = segment_2.column(2).as_any().downcast_ref().unwrap();
        let end_time = Some(start_times.value(1) - 100);

        let result = storage_engine.get_compressed_files(&[1, 2], None, end_time);
        let files = result.unwrap();
        assert_eq!(files.len(), 1);

        // The path to the returned compressed file should contain the key of the first segment.
        let file_path = files.get(0).unwrap().location.to_string();
        assert!(file_path.contains("1/compressed"));
    }

    #[test]
    fn test_can_get_compressed_files_with_start_time_and_end_time() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        // Insert 4 segments with a ~1 second time difference between segment 1 and 2 and segment 3 and 4.
        let (segment_1, _segment_2) = insert_separated_segments(&mut storage_engine, 1, 2, 0);
        let (_segment_3, segment_4) = insert_separated_segments(&mut storage_engine, 3, 4, 1000);

        // If we have a start time after the first segment ends and an end time before the fourth
        // segment starts, only the files from the second and third segment should be retrieved.
        let end_times: &TimestampArray = segment_1.column(3).as_any().downcast_ref().unwrap();
        let start_times: &TimestampArray = segment_4.column(2).as_any().downcast_ref().unwrap();

        let start_time = Some(end_times.value(end_times.len() - 1) + 100);
        let end_time = Some(start_times.value(1) - 100);

        let result = storage_engine.get_compressed_files(&[1, 2, 3, 4], start_time, end_time);
        let files = result.unwrap();
        assert_eq!(files.len(), 2);

        // The paths to the returned compressed files should contain the key of the second and third segment.
        let file_path = files.get(0).unwrap().location.to_string();
        assert!(file_path.contains("2/compressed"));

        let file_path = files.get(1).unwrap().location.to_string();
        assert!(file_path.contains("3/compressed"));
    }

    /// Create and insert two compressed segments with a 1 second time difference offset by `start_time`.
    fn insert_separated_segments(
        storage_engine: &mut StorageEngine,
        key_1: u64,
        key_2: u64,
        start_time: i64,
    ) -> (RecordBatch, RecordBatch) {
        let segment_1 = test_util::get_compressed_segment_record_batch_with_time(1000 + start_time);
        storage_engine.compressed_data_manager.insert_compressed_segment(key_1, segment_1.clone());

        let segment_2 = test_util::get_compressed_segment_record_batch_with_time(2000 + start_time);
        storage_engine.compressed_data_manager.insert_compressed_segment(key_2, segment_2.clone());

        (segment_1, segment_2)
    }

    #[test]
    fn test_cannot_get_compressed_files_where_end_time_is_before_start_time() {
        let (_temp_dir, mut storage_engine) = create_storage_engine();

        let segment = test_util::get_compressed_segment_record_batch();
        storage_engine.compressed_data_manager.insert_compressed_segment(1, segment);

        let result = storage_engine.get_compressed_files(&[1], Some(10), Some(1));
        assert!(result.is_err());
    }

    /// Create a [`StorageEngine`] with a folder that is deleted once the test is finished.
    fn create_storage_engine() -> (TempDir, StorageEngine) {
        let temp_dir = tempdir().unwrap();

        let data_folder_path = temp_dir.path().to_path_buf();
        (temp_dir, StorageEngine::new(data_folder_path))
    }

    // Tests for writing and reading Apache Parquet files.
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

    /// Create an Apache Parquet file in the [`TempDir`] from a generated [`RecordBatch`].
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

#[cfg(test)]
/// Separate module for compressed segment utility functions since they are needed to test
/// multiple modules in the storage engine.
pub mod test_util {
    use std::sync::Arc;

    use datafusion::arrow::array::{BinaryArray, Float32Array, UInt8Array};
    use datafusion::arrow::datatypes::DataType::UInt8;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    use crate::storage::StorageEngine;
    use crate::types::{TimestampArray, ValueArray};

    pub const COMPRESSED_SEGMENT_SIZE: usize = 2176;

    /// Return a [`RecordBatch`] that only has a single column, and therefore does not match the
    /// compressed segment schema.
    pub fn get_invalid_compressed_segment_record_batch() -> RecordBatch {
        let model_type_id = UInt8Array::from(vec![2, 3, 3]);
        let schema = Schema::new(vec![Field::new("model_type_id", UInt8, false)]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(model_type_id)]).unwrap()
    }

    /// Return a generated compressed segment with three model segments.
    pub fn get_compressed_segment_record_batch() -> RecordBatch {
        get_compressed_segment_record_batch_with_time(0)
    }

    /// Return a generated compressed segment with three model segments. The segment time span is
    /// from `time_ms` to `time_ms` + 3.
    pub fn get_compressed_segment_record_batch_with_time(time_ms: i64) -> RecordBatch {
        let start_times = vec![time_ms, time_ms + 1, time_ms + 2];
        let end_times = vec![time_ms + 1, time_ms + 2, time_ms + 3];

        let model_type_id = UInt8Array::from(vec![2, 3, 3]);
        let timestamps = BinaryArray::from_vec(vec![b"000", b"001", b"010"]);
        let start_time = TimestampArray::from(start_times);
        let end_time = TimestampArray::from(end_times);
        let values = BinaryArray::from_vec(vec![b"1111", b"1000", b"0000"]);
        let min_value = ValueArray::from(vec![5.2, 10.3, 30.2]);
        let max_value = ValueArray::from(vec![20.2, 12.2, 34.2]);
        let error = Float32Array::from(vec![0.2, 0.5, 0.1]);

        let schema = StorageEngine::get_compressed_segment_schema();

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(model_type_id),
                Arc::new(timestamps),
                Arc::new(start_time),
                Arc::new(end_time),
                Arc::new(values),
                Arc::new(min_value),
                Arc::new(max_value),
                Arc::new(error),
            ],
        ).unwrap()
    }
}
