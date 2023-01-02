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

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use tracing::debug;

use crate::array;
use crate::storage::{StorageEngine, UNCOMPRESSED_DATA_BUFFER_CAPACITY, UNCOMPRESSED_DATA_FOLDER};
use crate::types::{
    Timestamp, TimestampArray, TimestampBuilder, UncompressedSchema, Value, ValueBuilder,
};

/// Functionality shared by [`UncompressedInMemoryDataBuffer`] and [`UncompressedOnDiskDataBuffer`].
/// Since the data buffers are part of the asynchronous storage engine the buffers must be [`Sync`]
/// and [`Send`]. Both [`UncompressedInMemoryDataBuffer`] and [`UncompressedOnDiskDataBuffer`]
/// automatically implements [`Sync`] and [`Send`] as they only contain types that implements
/// [`Sync`] and [`Send`].
#[async_trait]
pub trait UncompressedDataBuffer: fmt::Debug + Sync + Send {
    /// Return the data in the uncompressed data buffer as a [`RecordBatch`].
    async fn record_batch(
        &mut self,
        uncompressed_schema: &UncompressedSchema,
    ) -> Result<RecordBatch, ParquetError>;

    /// Return the univariate id that uniquely identifies the univariate time series the buffer
    /// stores data points from.
    fn univariate_id(&self) -> u64;

    /// Return the total amount of memory used by the buffer.
    fn memory_size(&self) -> usize;

    /// Since both [`UncompressedInMemoryDataBuffers`](UncompressedInMemoryDataBuffer) and
    /// [`UncompressedOnDiskDataBuffers`](UncompressedOnDiskDataBuffer) are present in the queue,
    /// both structs need to implement spilling to Apache Parquet, with already spilled segments
    /// returning [`IOError`].
    async fn spill_to_apache_parquet(
        &mut self,
        local_data_folder: &Path,
        uncompressed_schema: &UncompressedSchema,
    ) -> Result<UncompressedOnDiskDataBuffer, IOError>;
}

/// A writeable in-memory data buffer that new data points can be efficiently appended to. It
/// consists of an ordered sequence of timestamps and values being built using
/// [`PrimitiveBuilder`](datafusion::arrow::array::PrimitiveBuilder).
pub(super) struct UncompressedInMemoryDataBuffer {
    /// Id that uniquely identifies the time series the buffer stores data points from.
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
    pub(super) fn memory_size() -> usize {
        (UNCOMPRESSED_DATA_BUFFER_CAPACITY * mem::size_of::<Timestamp>())
            + (UNCOMPRESSED_DATA_BUFFER_CAPACITY * mem::size_of::<Value>())
    }

    /// Return [`true`] if the [`UncompressedInMemoryDataBuffer`] is full, meaning additional data
    /// points cannot be appended.
    pub(super) fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    /// Return how many data points the [`UncompressedInMemoryDataBuffer`] currently contains.
    pub(super) fn len(&self) -> usize {
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
    fn capacity(&self) -> usize {
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
            self.len(),
            self.capacity()
        )
    }
}

#[async_trait]
impl UncompressedDataBuffer for UncompressedInMemoryDataBuffer {
    /// Finish the array builders and return the data in a structured [`RecordBatch`].
    async fn record_batch(
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
    fn univariate_id(&self) -> u64 {
        self.univariate_id
    }

    /// Return the total size of the [`UncompressedInMemoryDataBuffer`] in bytes.
    fn memory_size(&self) -> usize {
        UncompressedInMemoryDataBuffer::memory_size()
    }

    /// Spill the in-memory [`UncompressedInMemoryDataBuffer`] to an Apache Parquet file and return
    /// the [`UncompressedOnDiskDataBuffer`] when finished.
    async fn spill_to_apache_parquet(
        &mut self,
        local_data_folder: &Path,
        uncompressed_schema: &UncompressedSchema,
    ) -> Result<UncompressedOnDiskDataBuffer, IOError> {
        // Since the schema is constant and the columns are always the same length, creating the
        // RecordBatch should never fail and unwrap is therefore safe to use.
        let batch = self.record_batch(uncompressed_schema).await.unwrap();
        UncompressedOnDiskDataBuffer::try_spill(self.univariate_id, local_data_folder, batch)
    }
}

/// A read only uncompressed buffer that has been spilled to disk as an Apache Parquet file due to
/// memory constraints.
pub struct UncompressedOnDiskDataBuffer {
    /// Id that uniquely identifies the time series the buffer stores data points from.
    univariate_id: u64,
    /// Path to the Apache Parquet file containing the uncompressed data in the
    /// [`UncompressedOnDiskDataBuffer`].
    file_path: PathBuf,
}

impl UncompressedOnDiskDataBuffer {
    /// Spill the in-memory `data_points` from the time series with `univariate_id` to an Apache
    /// Parquet file in `local_data_folder`. If the Apache Parquet file is written successfully,
    /// return an [`UncompressedOnDiskDataBuffer`], otherwise return [`IOError`].
    pub(super) fn try_spill(
        univariate_id: u64,
        local_data_folder: &Path,
        data_points: RecordBatch,
    ) -> Result<Self, IOError> {
        let local_file_path = local_data_folder
            .join(UNCOMPRESSED_DATA_FOLDER)
            .join(univariate_id.to_string());

        // Create the folder structure if it does not already exist.
        fs::create_dir_all(local_file_path.as_path())?;

        // Create a path that uses the first timestamp as the filename.
        let timestamps = array!(data_points, 0, TimestampArray);
        let file_name = format!("{}.parquet", timestamps.value(0));
        let file_path = local_file_path.join(file_name);

        StorageEngine::write_batch_to_apache_parquet_file(data_points, file_path.as_path())?;

        Ok(Self {
            univariate_id,
            file_path,
        })
    }

    /// Return an [`UncompressedOnDiskDataBuffer`] with the data points for `univariate_id` in
    /// `file_path` if a file at `file_path` exists, otherwise [`IOError`] is returned.
    pub(super) fn try_new(univariate_id: u64, file_path: PathBuf) -> Result<Self, IOError> {
        file_path.try_exists()?;

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

#[async_trait]
impl UncompressedDataBuffer for UncompressedOnDiskDataBuffer {
    /// Read the data from the Apache Parquet file, delete the Apache Parquet file, and return the
    /// data as a [`RecordBatch`]. Return [`ParquetError`] if the Apache Parquet file cannot be read
    /// or deleted.
    async fn record_batch(
        &mut self,
        _uncompressed_schema: &UncompressedSchema,
    ) -> Result<RecordBatch, ParquetError> {
        let record_batch =
            StorageEngine::read_batch_from_apache_parquet_file(self.file_path.as_path()).await?;

        fs::remove_file(self.file_path.as_path())
            .map_err(|error| ParquetError::General(error.to_string()))?;

        Ok(record_batch)
    }

    /// Return the univariate id that uniquely identifies the univariate time series the buffer
    /// stores data points from.
    fn univariate_id(&self) -> u64 {
        self.univariate_id
    }

    /// Since the data is not kept in memory, return 0.
    fn memory_size(&self) -> usize {
        0
    }

    /// Since the buffer has already been spilled, return [`IOError`].
    async fn spill_to_apache_parquet(
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

    use tempfile;

    use crate::metadata::test_util;

    // Tests for UncompressedInMemoryDataBuffer.
    #[test]
    fn test_get_in_memory_data_buffer_memory_size() {
        let uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);

        let expected = (uncompressed_buffer.timestamps.capacity() * mem::size_of::<Timestamp>())
            + (uncompressed_buffer.values.capacity() * mem::size_of::<Value>());

        assert_eq!(UncompressedInMemoryDataBuffer::memory_size(), expected)
    }

    #[test]
    fn test_get_in_memory_data_buffer_len() {
        let uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);

        assert_eq!(uncompressed_buffer.len(), 0);
    }

    #[test]
    fn test_can_insert_data_point_into_in_memory_data_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);
        insert_data_points(1, &mut uncompressed_buffer);

        assert_eq!(uncompressed_buffer.len(), 1);
    }

    #[test]
    fn test_check_is_in_memory_data_buffer_full() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);
        insert_data_points(uncompressed_buffer.capacity(), &mut uncompressed_buffer);

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

        insert_data_points(uncompressed_buffer.capacity() + 1, &mut uncompressed_buffer);
    }

    #[tokio::test]
    async fn test_get_record_batch_from_in_memory_data_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);
        insert_data_points(uncompressed_buffer.capacity(), &mut uncompressed_buffer);

        let capacity = uncompressed_buffer.capacity();
        let data = uncompressed_buffer
            .record_batch(&test_util::uncompressed_schema())
            .await
            .unwrap();
        assert_eq!(data.num_columns(), 2);
        assert_eq!(data.num_rows(), capacity);
    }

    #[tokio::test]
    async fn test_in_memory_data_buffer_can_spill_not_full_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);
        insert_data_points(1, &mut uncompressed_buffer);
        assert!(!uncompressed_buffer.is_full());

        let temp_dir = tempfile::tempdir().unwrap();
        uncompressed_buffer
            .spill_to_apache_parquet(temp_dir.path(), &test_util::uncompressed_schema())
            .await
            .unwrap();

        let uncompressed_path = temp_dir
            .path()
            .join(format!("{}/1", UNCOMPRESSED_DATA_FOLDER));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1)
    }

    #[tokio::test]
    async fn test_in_memory_data_buffer_can_spill_full_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(1);
        insert_data_points(uncompressed_buffer.capacity(), &mut uncompressed_buffer);
        assert!(uncompressed_buffer.is_full());

        let temp_dir = tempfile::tempdir().unwrap();
        uncompressed_buffer
            .spill_to_apache_parquet(temp_dir.path(), &test_util::uncompressed_schema())
            .await
            .unwrap();

        let uncompressed_path = temp_dir
            .path()
            .join(format!("{}/1", UNCOMPRESSED_DATA_FOLDER));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1)
    }

    // Tests for UncompressedOnDiskDataBuffer.
    #[test]
    fn test_get_on_disk_data_buffer_memory_size() {
        let uncompressed_buffer = UncompressedOnDiskDataBuffer {
            univariate_id: 1,
            file_path: Path::new("file_path").to_path_buf(),
        };

        assert_eq!(uncompressed_buffer.memory_size(), 0)
    }

    #[tokio::test]
    async fn test_get_record_batch_from_on_disk_data_buffer() {
        let mut uncompressed_in_memory_buffer = UncompressedInMemoryDataBuffer::new(1);
        let capacity = uncompressed_in_memory_buffer.capacity();
        insert_data_points(capacity, &mut uncompressed_in_memory_buffer);

        let temp_dir = tempfile::tempdir().unwrap();
        let mut uncompressed_on_disk_buffer = uncompressed_in_memory_buffer
            .spill_to_apache_parquet(temp_dir.path(), &test_util::uncompressed_schema())
            .await
            .unwrap();

        let data = uncompressed_on_disk_buffer
            .record_batch(&test_util::uncompressed_schema())
            .await
            .unwrap();

        assert_eq!(data.num_columns(), 2);
        assert_eq!(data.num_rows(), capacity);

        let spilled_buffer_path = temp_dir
            .path()
            .join(UNCOMPRESSED_DATA_FOLDER)
            .join("1")
            .join("1234567890123.parquet");
        assert!(!spilled_buffer_path.exists());
    }

    /// Insert `count` generated data points into `uncompressed_buffer`.
    fn insert_data_points(count: usize, uncompressed_buffer: &mut UncompressedInMemoryDataBuffer) {
        let timestamp: Timestamp = 1234567890123;
        let value: Value = 30.0;

        for _ in 0..count {
            uncompressed_buffer.insert_data(timestamp, value);
        }
    }

    #[tokio::test]
    async fn test_cannot_spill_on_disk_data_buffer() {
        let mut uncompressed_buffer = UncompressedOnDiskDataBuffer {
            univariate_id: 1,
            file_path: Path::new("file_path").to_path_buf(),
        };

        let schema = test_util::uncompressed_schema();
        let result = uncompressed_buffer.spill_to_apache_parquet(Path::new(""), &schema);
        assert!(result.await.is_err())
    }
}
