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

//! In-memory and on-disk buffers for storing uncompressed data points. Both types of buffers
//! implement the [`UncompressedDataBuffer`] trait. [`UncompressedInMemoryDataBuffer`] provides
//! support for inserting and storing data in-memory, while [`UncompressedOnDiskDataBuffer`]
//! provides support for storing uncompressed data in Apache Parquet files on disk.

use std::fmt::Formatter;
use std::io::Error as IOError;
use std::io::ErrorKind::Other;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, fs, mem};

use datafusion::arrow::array::{Array, ArrayBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use tracing::debug;

use crate::get_array;
use crate::storage::{StorageEngine, UNCOMPRESSED_DATA_BUFFER_CAPACITY};
use crate::types::{
    Timestamp, TimestampArray, TimestampBuilder, UncompressedSchema, Value, ValueBuilder,
};

/// Functionality shared by [`UncompressedInMemoryDataBuffer`] and [`UncompressedOnDiskDataBuffer`].
/// Since the data buffers are part of the asynchronous storage engine the buffers must be [`Sync`]
/// and [`Send`]. Both [`UncompressedInMemoryDataBuffer`] and [`UncompressedOnDiskDataBuffer`]
/// automatically implements [`Sync`] and [`Send`] as they only contain types that implements
/// [`Sync`] and [`Send`].
pub trait UncompressedDataBuffer: fmt::Debug + Sync + Send {
    fn get_record_batch(
        &mut self,
        uncompressed_schema: &UncompressedSchema,
    ) -> Result<RecordBatch, ParquetError>;

    /// Return the univariate id that uniquely identifies the univariate time series the buffer
    /// stores data points from.
    fn get_univariate_id(&self) -> u64;

    /// Return the total amount of memory used by the buffer.
    fn get_memory_size(&self) -> usize;

    /// Since both [`UncompressedInMemoryDataBuffers`](UncompressedInMemoryDataBuffer) and
    /// [`UncompressedOnDiskDataBuffers`](UncompressedOnDiskDataBuffer) are present in the queue as
    /// [`FinishedUncompressedDataBuffers`](FinishedUncompressedDataBuffer), both structs need to
    /// implement spilling to Apache Parquet, with already spilled segments returning [`IOError`].
    fn spill_to_apache_parquet(
        &mut self,
        local_data_folder: &Path,
        uncompressed_schema: &UncompressedSchema,
    ) -> Result<UncompressedOnDiskDataBuffer, IOError>;
}

/// A writeable in-memory data buffer that new data points can be efficiently appended to. It
/// consists of an ordered sequence of timestamps and values being build using [`PrimitiveBuilder`].
pub(super) struct UncompressedInMemoryDataBuffer {
    /// Key that uniquely identifies the time series the buffer stores data points from.
    univariate_id: u64,
    /// Builder consisting of timestamps.
    timestamps: TimestampBuilder,
    /// Builder consisting of float values.
    values: ValueBuilder,
}

impl UncompressedInMemoryDataBuffer {
    pub(super) fn new(univariate_id: u64) -> Self {
        Self {
            univariate_id,
            timestamps: TimestampBuilder::with_capacity(UNCOMPRESSED_DATA_BUFFER_CAPACITY),
            values: ValueBuilder::with_capacity(UNCOMPRESSED_DATA_BUFFER_CAPACITY),
        }
    }

    /// Return the total size of the [`UncompressedInMemoryDataBuffer`] in bytes. Note that this is
    /// constant.
    pub(super) fn get_memory_size() -> usize {
        (UNCOMPRESSED_DATA_BUFFER_CAPACITY * mem::size_of::<Timestamp>())
            + (UNCOMPRESSED_DATA_BUFFER_CAPACITY * mem::size_of::<Value>())
    }

    /// Return [`true`] if the [`UncompressedInMemoryDataBuffer`] is full, meaning additional data
    /// points cannot be appended.
    pub(super) fn is_full(&self) -> bool {
        self.get_length() == self.get_capacity()
    }

    /// Return how many data points the [`UncompressedInMemoryDataBuffer`] currently contains.
    pub(super) fn get_length(&self) -> usize {
        // The length is always the same for both builders.
        self.timestamps.len()
    }

    /// Add `timestamp` and `value` to the [`UncompressedInMemoryDataBuffer`].
    pub(super) fn insert_data(&mut self, timestamp: Timestamp, value: Value) {
        debug_assert!(
            !self.is_full(),
            "Cannot insert data into full UncompressedInMemoryDataBuffer."
        );

        self.timestamps.append_value(timestamp);
        self.values.append_value(value);

        debug!("Inserted data point into {:?}.", self)
    }

    /// Return how many data points the [`UncompressedInMemoryDataBuffer`] can contain.
    fn get_capacity(&self) -> usize {
        // The capacity is always the same for both builders.
        self.timestamps.capacity()
    }
}

impl fmt::Debug for UncompressedInMemoryDataBuffer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "UncompressedInMemoryDataBuffer({}, {}, {})",
            self.univariate_id,
            self.get_length(),
            self.get_capacity()
        )
    }
}

impl UncompressedDataBuffer for UncompressedInMemoryDataBuffer {
    /// Finish the array builders and return the data in a structured [`RecordBatch`].
    fn get_record_batch(
        &mut self,
        uncompressed_schema: &UncompressedSchema,
    ) -> Result<RecordBatch, ParquetError> {
        let timestamps = self.timestamps.finish();
        let values = self.values.finish();

        Ok(RecordBatch::try_new(
            uncompressed_schema.0.clone(),
            vec![Arc::new(timestamps), Arc::new(values)],
        )
        .unwrap())
    }

    /// Return the univariate id that uniquely identifies the univariate time series the buffer
    /// stores data points from.
    fn get_univariate_id(&self) -> u64 {
        self.univariate_id
    }

    /// Return the total size of the [`UncompressedInMemoryDataBuffer`] in bytes.
    fn get_memory_size(&self) -> usize {
        UncompressedInMemoryDataBuffer::get_memory_size()
    }

    /// Spill the in-memory [`UncompressedInMemoryDataBuffer`] to an Apache Parquet file and return
    /// the [`UncompressedOnDiskDataBuffer`] when finished.
    fn spill_to_apache_parquet(
        &mut self,
        local_data_folder: &Path,
        uncompressed_schema: &UncompressedSchema,
    ) -> Result<UncompressedOnDiskDataBuffer, IOError> {
        // Since the schema is constant and the columns are always the same length, creating the
        // RecordBatch should never fail and unwrap is therefore safe to use.
        let batch = self.get_record_batch(uncompressed_schema).unwrap();
        UncompressedOnDiskDataBuffer::new(self.univariate_id, local_data_folder, batch)
    }
}

/// A read only uncompressed buffer that has been spilled to disk as an Apache Parquet file due to
/// memory constraints.
pub struct UncompressedOnDiskDataBuffer {
    /// Key that uniquely identifies the time series the buffer stores data points from.
    univariate_id: u64,
    /// Path to the Apache Parquet file containing the uncompressed data in the [`SpilledSegment`].
    file_path: PathBuf,
}

impl UncompressedOnDiskDataBuffer {
    /// Spill the in-memory `data_points` to an Apache Parquet file, and return a
    /// [`UncompressedOnDiskDataBuffer`] containing the `univariate_id` and a file path. If the
    /// Apache Paruqet file is written successfully, return [`UncompressedOnDiskDataBuffer`],
    /// otherwise return [`IOError`].
    pub(super) fn new(
        univariate_id: u64,
        local_data_folder: &Path,
        data_points: RecordBatch,
    ) -> Result<Self, IOError> {
        let local_file_path = local_data_folder
            .join("uncompressed")
            .join(univariate_id.to_string());

        // Create the folder structure if it does not already exist.
        fs::create_dir_all(local_file_path.as_path())?;

        // Create a path that uses the first timestamp as the filename.
        let timestamps = get_array!(data_points, 0, TimestampArray);
        let file_name = format!("{}.parquet", timestamps.value(0));
        let file_path = local_file_path.join(file_name);

        StorageEngine::write_batch_to_apache_parquet_file(data_points, file_path.as_path())?;

        Ok(Self {
            univariate_id,
            file_path,
        })
    }
}

impl fmt::Debug for UncompressedOnDiskDataBuffer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "UncompressedOnDiskDataBuffer({}, {})",
            self.univariate_id,
            self.file_path.display()
        )
    }
}

// TODO: Delete the spilled Apache Parquet file after it has been read?
impl UncompressedDataBuffer for UncompressedOnDiskDataBuffer {
    /// Read the data from the Apache Parquet file and return it as a [`RecordBatch`].
    fn get_record_batch(
        &mut self,
        _uncompressed_schema: &UncompressedSchema,
    ) -> Result<RecordBatch, ParquetError> {
        StorageEngine::read_batch_from_apache_parquet_file(self.file_path.as_path())
    }

    /// Return the univariate id that uniquely identifies the univariate time series the buffer
    /// stores data points from.
    fn get_univariate_id(&self) -> u64 {
        self.univariate_id
    }

    /// Since the data is not kept in memory, return 0.
    fn get_memory_size(&self) -> usize {
        0
    }

    /// Since the buffer has already been spilled, return [`IOError`].
    fn spill_to_apache_parquet(
        &mut self,
        _local_data_folder: &Path,
        _uncompressed_schema: &UncompressedSchema,
    ) -> Result<UncompressedOnDiskDataBuffer, IOError> {
        Err(IOError::new(
            Other,
            format!(
                "The buffer has already been spilled to '{}'.",
                self.file_path.display()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    use crate::metadata::test_util;

    // Tests for UncompressedInMemoryDataBuffer.
    #[test]
    fn test_get_in_memory_data_buffer_memory_size() {
        let uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);

        let expected = (uncompressed_buffer.timestamps.capacity() * mem::size_of::<Timestamp>())
            + (uncompressed_buffer.values.capacity() * mem::size_of::<Value>());

        assert_eq!(UncompressedInMemoryDataBuffer::get_memory_size(), expected)
    }

    #[test]
    fn test_get_in_memory_data_buffer_length() {
        let uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);

        assert_eq!(uncompressed_buffer.get_length(), 0);
    }

    #[test]
    fn test_can_insert_data_point_into_in_memory_data_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);
        insert_data_points(1, &mut uncompressed_buffer);

        assert_eq!(uncompressed_buffer.get_length(), 1);
    }

    #[test]
    fn test_check_is_in_memory_data_buffer_full() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);
        insert_data_points(uncompressed_buffer.get_capacity(), &mut uncompressed_buffer);

        assert!(uncompressed_buffer.is_full());
    }

    #[test]
    fn test_check_is_in_memory_data_buffer_not_full() {
        let uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);

        assert!(!uncompressed_buffer.is_full());
    }

    #[test]
    #[should_panic(expected = "Cannot insert data into full UncompressedInMemoryDataBuffer.")]
    fn test_in_memory_data_buffer_panic_if_inserting_data_point_when_full() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);

        insert_data_points(
            uncompressed_buffer.get_capacity() + 1,
            &mut uncompressed_buffer,
        );
    }

    #[test]
    fn test_get_record_batch_from_in_memory_data_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);
        insert_data_points(uncompressed_buffer.get_capacity(), &mut uncompressed_buffer);

        let capacity = uncompressed_buffer.get_capacity();
        let data = uncompressed_buffer
            .get_record_batch(&test_util::get_uncompressed_schema())
            .unwrap();
        assert_eq!(data.num_columns(), 2);
        assert_eq!(data.num_rows(), capacity);
    }

    #[test]
    fn test_in_memory_data_buffer_can_spill_not_full_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);
        insert_data_points(1, &mut uncompressed_buffer);
        assert!(!uncompressed_buffer.is_full());

        let temp_dir = tempdir().unwrap();
        uncompressed_buffer
            .spill_to_apache_parquet(temp_dir.path(), &test_util::get_uncompressed_schema())
            .unwrap();

        let uncompressed_path = temp_dir.path().join("uncompressed/1");
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1)
    }

    #[test]
    fn test_in_memory_data_buffer_can_spill_full_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);
        insert_data_points(uncompressed_buffer.get_capacity(), &mut uncompressed_buffer);
        assert!(uncompressed_buffer.is_full());

        let temp_dir = tempdir().unwrap();
        uncompressed_buffer
            .spill_to_apache_parquet(temp_dir.path(), &test_util::get_uncompressed_schema())
            .unwrap();

        let uncompressed_path = temp_dir.path().join("uncompressed/1");
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1)
    }

    // Tests for UncompressedOnDiskDataBuffer.
    #[test]
    fn test_get_on_disk_data_buffer_memory_size() {
        let uncompressed_buffer = UncompressedOnDiskDataBuffer {
            univariate_id: 1,
            file_path: Path::new("file_path").to_path_buf(),
        };

        assert_eq!(uncompressed_buffer.get_memory_size(), 0)
    }

    #[test]
    fn test_get_record_batch_from_on_disk_data_buffer() {
        let mut uncompressed_in_memory_buffer = UncompressedInMemoryDataBuffer::new(1);
        let capacity = uncompressed_in_memory_buffer.get_capacity();
        insert_data_points(capacity, &mut uncompressed_in_memory_buffer);

        let temp_dir = tempdir().unwrap();
        let mut uncompressed_on_disk_buffer = uncompressed_in_memory_buffer
            .spill_to_apache_parquet(temp_dir.path(), &test_util::get_uncompressed_schema())
            .unwrap();

        let data = uncompressed_on_disk_buffer
            .get_record_batch(&test_util::get_uncompressed_schema())
            .unwrap();
        assert_eq!(data.num_columns(), 2);
        assert_eq!(data.num_rows(), capacity);
    }

    /// Insert `count` generated data points into `segment_builder`.
    fn insert_data_points(count: usize, uncompressed_buffer: &mut UncompressedInMemoryDataBuffer) {
        let timestamp: Timestamp = 1234567890123;
        let value: Value = 30.0;

        for _ in 0..count {
            uncompressed_buffer.insert_data(timestamp, value);
        }
    }

    #[test]
    fn test_cannot_spill_on_disk_data_buffer() {
        let mut uncompressed_buffer = UncompressedOnDiskDataBuffer {
            univariate_id: 1,
            file_path: Path::new("file_path").to_path_buf(),
        };

        let result = uncompressed_buffer
            .spill_to_apache_parquet(Path::new(""), &test_util::get_uncompressed_schema());
        assert!(result.is_err())
    }
}
