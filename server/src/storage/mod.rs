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
mod uncompressed_data_manager;
mod compressed_data_manager;

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

use crate::storage::compressed_data_manager::CompressedDataManager;
use crate::storage::segment::FinishedSegment;
use crate::storage::uncompressed_data_manager::UncompressedDataManager;
use crate::types::{ArrowTimestamp, ArrowValue};

// TODO: Look into custom errors for all errors in storage engine.

/// The expected [first four bytes of any Apache Parquet file].
///
/// [first four bytes of any Apache Parquet file]: https://en.wikipedia.org/wiki/List_of_file_signatures
const APACHE_PARQUET_FILE_SIGNATURE: &[u8] = &[80, 65, 82, 49]; // PAR1.

// Note that the capacity has to be a multiple of 64 bytes to avoid the actual capacity
// being larger due to internal alignment when allocating memory for the builders.
const BUILDER_CAPACITY: usize = 64;

/// Manages all uncompressed and compressed data, both while being built and when finished.
pub struct StorageEngine {
    /// Manager that contains and controls all uncompressed data.
    uncompressed_data_manager: UncompressedDataManager,
    /// Manager that contains and controls all compressed data.
    compressed_data_manager: CompressedDataManager,
}

impl StorageEngine {
    pub fn new(storage_folder_path: String) -> Self {
        Self {
            uncompressed_data_manager: UncompressedDataManager::new(storage_folder_path.clone()),
            compressed_data_manager: CompressedDataManager::new(storage_folder_path),
        }
    }

    /// Pass `message` to the uncompressed data manager. Return Ok if the message was successfully
    /// inserted, otherwise return Err.
    pub fn insert_message(&mut self, message: Message) -> Result<(), String> {
        self.uncompressed_data_manager.insert_message(message)
    }

    /// Retrieve the oldest finished segment from the uncompressed data manager and return it.
    /// Return `None` if there are no finished segments.
    pub fn get_finished_segment(&mut self) -> Option<FinishedSegment> {
        self.uncompressed_data_manager.get_finished_segment()
    }

    /// Pass `segment` to the compressed data manager.
    pub fn insert_compressed_data(&mut self, key: String, segment: RecordBatch) {
        self.compressed_data_manager.insert_compressed_data(key, segment)
    }

    // TODO: Maybe move to configuration struct and have a single Arc.
    /// Return the record batch schema used for uncompressed segments.
    pub fn get_uncompressed_segment_schema() -> Schema {
        Schema::new(vec![
            Field::new("timestamps", ArrowTimestamp::DATA_TYPE, false),
            Field::new("values", ArrowValue::DATA_TYPE, false),
        ])
    }

    // TODO: Maybe move to configuration struct and have a single Arc.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use tempfile::{tempdir, TempDir};

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
    use crate::storage::time_series::CompressedTimeSeries;
    use crate::types::{TimestampArray, ValueArray};

    pub const COMPRESSED_SEGMENT_SIZE: usize = 2032;

    /// Return a record batch that only has a single column, and therefore does not match the
    /// compressed segment schema.
    pub fn get_invalid_compressed_segment_record_batch() -> RecordBatch {
        let model_type_id = UInt8Array::from(vec![2, 3, 3]);
        let schema = Schema::new(vec![Field::new("model_type_id", UInt8, false)]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(model_type_id)]).unwrap()
    }

    /// Return a generated compressed segment with three model segments.
    pub fn get_compressed_segment_record_batch() -> RecordBatch {
        let model_type_id = UInt8Array::from(vec![2, 3, 3]);
        let timestamps = BinaryArray::from_vec(vec![b"000", b"001", b"010"]);
        let start_time = TimestampArray::from(vec![1, 2, 3]);
        let end_time = TimestampArray::from(vec![2, 3, 4]);
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
