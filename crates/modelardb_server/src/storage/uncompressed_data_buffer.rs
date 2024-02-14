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

//! In-memory and on-disk buffers for storing uncompressed data points. Both types of buffers can be
//! stored together using the [`UncompressedDataBuffer`] enum. [`UncompressedInMemoryDataBuffer`]
//! supports inserting and storing data in-memory, while [`UncompressedOnDiskDataBuffer`] provides
//! support for storing uncompressed data points in Apache Parquet files on disk.

use std::fmt::Formatter;
use std::io::Error as IOError;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, fs, mem};

use datafusion::arrow::array::{Array, ArrayBuilder};
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::schemas::UNCOMPRESSED_SCHEMA;
use modelardb_common::types::{
    ErrorBound, Timestamp, TimestampArray, TimestampBuilder, Value, ValueArray, ValueBuilder,
};
use modelardb_common::{metadata, storage};
use tracing::debug;

use crate::storage::{UNCOMPRESSED_DATA_BUFFER_CAPACITY, UNCOMPRESSED_DATA_FOLDER};

/// Number of [`RecordBatches`](RecordBatch) that must be ingested without modifying an
/// [`UncompressedInMemoryDataBuffer`] before it is considered unused and can be finished.
const RECORD_BATCH_OFFSET_REQUIRED_FOR_UNUSED: u64 = 1;

/// Data points from a multivariate time series to be inserted into a model table.
pub(super) struct UncompressedDataMultivariate {
    /// Metadata of the model table to insert the data points into.
    pub(super) model_table_metadata: Arc<ModelTableMetadata>,
    /// Uncompressed multivariate data points to insert.
    pub(super) multivariate_data_points: RecordBatch,
}

impl UncompressedDataMultivariate {
    pub(super) fn new(
        model_table_metadata: Arc<ModelTableMetadata>,
        multivariate_data_points: RecordBatch,
    ) -> Self {
        Self {
            model_table_metadata,
            multivariate_data_points,
        }
    }
}

/// Wrapper for [`UncompressedInMemoryDataBuffer`] and [`UncompressedOnDiskDataBuffer`] so they can
/// be stored together in the data structures used by
/// [`UncompressedDataManager`](super::uncompressed_data_manager::UncompressedDataManager).
pub(super) enum UncompressedDataBuffer {
    InMemory(UncompressedInMemoryDataBuffer),
    OnDisk(UncompressedOnDiskDataBuffer),
}

/// A writeable in-memory data buffer that new data points can be efficiently appended to. It
/// consists of an ordered sequence of timestamps and values being built using
/// [`PrimitiveBuilder`](datafusion::arrow::array::PrimitiveBuilder).
pub(super) struct UncompressedInMemoryDataBuffer {
    /// Id that uniquely identifies the time series the buffer stores data points for.
    univariate_id: u64,
    /// Metadata of the model table the buffer stores data for.
    model_table_metadata: Arc<ModelTableMetadata>,
    /// Index of the last batch that caused the buffer to be updated.
    updated_by_batch_index: u64,
    /// Builder consisting of timestamps.
    timestamps: TimestampBuilder,
    /// Builder consisting of float values.
    values: ValueBuilder,
}

impl UncompressedInMemoryDataBuffer {
    pub(super) fn new(
        univariate_id: u64,
        model_table_metadata: Arc<ModelTableMetadata>,
        current_batch_index: u64,
    ) -> Self {
        Self {
            univariate_id,
            model_table_metadata,
            updated_by_batch_index: current_batch_index,
            timestamps: TimestampBuilder::with_capacity(*UNCOMPRESSED_DATA_BUFFER_CAPACITY),
            values: ValueBuilder::with_capacity(*UNCOMPRESSED_DATA_BUFFER_CAPACITY),
        }
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

    /// Return [`true`] if the [`UncompressedInMemoryDataBuffer`] has not been updated by
    /// [`RECORD_BATCH_OFFSET_REQUIRED_FOR_UNUSED`] [`RecordBatches`](`RecordBatch`) compared to the
    /// [`RecordBatch`] with index `current_batch_index` ingested by the current process.
    pub(super) fn is_unused(&self, current_batch_index: u64) -> bool {
        self.updated_by_batch_index + RECORD_BATCH_OFFSET_REQUIRED_FOR_UNUSED <= current_batch_index
    }

    /// Add `timestamp` and `value` to the [`UncompressedInMemoryDataBuffer`] from the
    /// [`RecordBatch`] ingested by the process with the index `current_batch_index`.
    pub(super) fn insert_data(
        &mut self,
        current_batch_index: u64,
        timestamp: Timestamp,
        value: Value,
    ) {
        debug_assert!(
            !self.is_full(),
            "Cannot insert data into full UncompressedInMemoryDataBuffer."
        );

        self.updated_by_batch_index = current_batch_index;
        self.timestamps.append_value(timestamp);
        self.values.append_value(value);

        debug!("Inserted data point into {:?}.", self)
    }

    /// Return how many data points the [`UncompressedInMemoryDataBuffer`] can contain.
    pub(super) fn capacity(&self) -> usize {
        // The capacity is always the same for both builders.
        self.timestamps.capacity()
    }

    /// Finish the array builders and return the data in a [`RecordBatch`] sorted by time.
    pub(super) async fn record_batch(&mut self) -> Result<RecordBatch, ParquetError> {
        let timestamps = self.timestamps.finish();
        let values = self.values.finish();

        // lexsort() is not used as it is unclear in what order it sorts multiple arrays, instead a
        // combination of sort_to_indices() and take(), like how lexsort() is implemented, is used.
        // unwrap() is safe as timestamps has a supported type and sorted_indices are within bounds.
        let sorted_indices = compute::sort_to_indices(&timestamps, None, None).unwrap();
        let sorted_timestamps = compute::take(&timestamps, &sorted_indices, None).unwrap();
        let sorted_values = compute::take(&values, &sorted_indices, None).unwrap();

        // unwrap() is safe as UNCOMPRESSED_SCHEMA contains timestamps and values.
        Ok(RecordBatch::try_new(
            UNCOMPRESSED_SCHEMA.0.clone(),
            vec![Arc::new(sorted_timestamps), Arc::new(sorted_values)],
        )
        .unwrap())
    }

    /// Return the univariate id that uniquely identifies the univariate time series the buffer
    /// stores data points from.
    pub(super) fn univariate_id(&self) -> u64 {
        self.univariate_id
    }

    /// Return the metadata for the model table the buffer stores data points for.
    pub(super) fn model_table_metadata(&self) -> &Arc<ModelTableMetadata> {
        &self.model_table_metadata
    }

    /// Return the error bound the buffer must be compressed within.
    pub(super) fn error_bound(&self) -> ErrorBound {
        let column_index: usize =
            metadata::univariate_id_to_column_index(self.univariate_id).into();
        self.model_table_metadata.error_bounds[column_index]
    }

    /// Return the total size of the [`UncompressedInMemoryDataBuffer`] in bytes.
    pub(super) fn memory_size() -> usize {
        (*UNCOMPRESSED_DATA_BUFFER_CAPACITY * mem::size_of::<Timestamp>())
            + (*UNCOMPRESSED_DATA_BUFFER_CAPACITY * mem::size_of::<Value>())
    }

    /// Spill the in-memory [`UncompressedInMemoryDataBuffer`] to an Apache Parquet file and return
    /// the [`UncompressedOnDiskDataBuffer`] when finished.
    pub(super) async fn spill_to_apache_parquet(
        &mut self,
        local_data_folder: &Path,
    ) -> Result<UncompressedOnDiskDataBuffer, IOError> {
        // Since the schema is constant and the columns are always the same length, creating the
        // RecordBatch should never fail and unwrap() is therefore safe to use.
        let batch = self.record_batch().await.unwrap();
        UncompressedOnDiskDataBuffer::try_spill(
            self.univariate_id,
            self.model_table_metadata.clone(),
            self.updated_by_batch_index,
            local_data_folder,
            batch,
        )
    }
}

impl fmt::Debug for UncompressedInMemoryDataBuffer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "UncompressedInMemoryDataBuffer({}, {}, {}, {})",
            self.univariate_id,
            self.len(),
            self.capacity(),
            self.updated_by_batch_index,
        )
    }
}

/// A read only uncompressed buffer that has been spilled to disk as an Apache Parquet file due to
/// memory constraints.
pub(super) struct UncompressedOnDiskDataBuffer {
    /// Id that uniquely identifies the time series the buffer stores data points for.
    univariate_id: u64,
    /// Metadata of the model table the buffer stores data for.
    model_table_metadata: Arc<ModelTableMetadata>,
    /// Index of the last batch that caused the buffer to be updated.
    updated_by_batch_index: u64,
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
        model_table_metadata: Arc<ModelTableMetadata>,
        updated_by_batch_index: u64,
        local_data_folder: &Path,
        data_points: RecordBatch,
    ) -> Result<Self, IOError> {
        let local_file_path = local_data_folder
            .join(UNCOMPRESSED_DATA_FOLDER)
            .join(univariate_id.to_string());

        // Create the folder structure if it does not already exist.
        fs::create_dir_all(local_file_path.as_path())?;

        // Create a path that uses the first timestamp as the filename.
        let timestamps = modelardb_common::array!(data_points, 0, TimestampArray);
        let file_name = format!("{}.parquet", timestamps.value(0));
        let file_path = local_file_path.join(file_name);

        storage::write_batch_to_apache_parquet_file(&data_points, file_path.as_path(), None)?;

        Ok(Self {
            univariate_id,
            model_table_metadata,
            updated_by_batch_index,
            file_path,
        })
    }

    /// Return an [`UncompressedOnDiskDataBuffer`] with the data points for `univariate_id` in
    /// `file_path` if a file at `file_path` exists, otherwise [`IOError`] is returned.
    pub(super) fn try_new(
        univariate_id: u64,
        model_table_metadata: Arc<ModelTableMetadata>,
        updated_by_batch_index: u64,
        file_path: PathBuf,
    ) -> Result<Self, IOError> {
        file_path.try_exists()?;

        Ok(Self {
            univariate_id,
            model_table_metadata,
            updated_by_batch_index,
            file_path,
        })
    }

    /// Read the data from the Apache Parquet file, delete the Apache Parquet file, and return the
    /// data as a [`RecordBatch`] sorted by time. Return [`ParquetError`] if the Apache Parquet file
    /// cannot be read or deleted.
    pub(super) async fn record_batch(&self) -> Result<RecordBatch, ParquetError> {
        let record_batch =
            storage::read_batch_from_apache_parquet_file(self.file_path.as_path()).await?;

        fs::remove_file(self.file_path.as_path())
            .map_err(|error| ParquetError::General(error.to_string()))?;

        Ok(record_batch)
    }

    /// Return the univariate id that uniquely identifies the univariate time series the buffer
    /// stores data points from.
    pub(super) fn univariate_id(&self) -> u64 {
        self.univariate_id
    }

    /// Return the metadata for the model table the buffer stores data points for.
    pub(super) fn model_table_metadata(&self) -> &Arc<ModelTableMetadata> {
        &self.model_table_metadata
    }

    /// Return the error bound the buffer must be compressed within.
    pub(super) fn error_bound(&self) -> ErrorBound {
        let column_index: usize =
            metadata::univariate_id_to_column_index(self.univariate_id).into();
        self.model_table_metadata.error_bounds[column_index]
    }

    /// Return the total size of the Apache Parquet file containing the uncompressed data buffer.
    pub(super) fn disk_size(&self) -> usize {
        // unwrap() is safe since the path is created internally.
        self.file_path.metadata().unwrap().len() as usize
    }

    /// Return [`true`] if the [`UncompressedOnDiskDataBuffer`] has not been updated by
    /// [`RECORD_BATCH_OFFSET_REQUIRED_FOR_UNUSED`] [`RecordBatches`](`RecordBatch`) compared to the
    /// [`RecordBatch`] with index `current_batch_index` ingested by the current process.
    pub(super) fn is_unused(&self, current_batch_index: u64) -> bool {
        self.updated_by_batch_index + RECORD_BATCH_OFFSET_REQUIRED_FOR_UNUSED <= current_batch_index
    }

    /// Since both [`UncompressedInMemoryDataBuffers`](UncompressedInMemoryDataBuffer) and
    /// [`UncompressedOnDiskDataBuffers`](UncompressedOnDiskDataBuffer) are stored together, both
    /// structs need to implement reading from Apache Parquet, with in-memory buffers returning
    /// [`IOError`].
    pub(super) async fn read_from_apache_parquet(
        &self,
        current_batch_index: u64,
    ) -> Result<UncompressedInMemoryDataBuffer, IOError> {
        let record_batch = self.record_batch().await?;

        let timestamps = modelardb_common::array!(record_batch, 0, TimestampArray);
        let values = modelardb_common::array!(record_batch, 1, ValueArray);

        let mut in_memory_buffer = UncompressedInMemoryDataBuffer::new(
            self.univariate_id,
            self.model_table_metadata.clone(),
            current_batch_index,
        );

        for index in 0..record_batch.num_rows() {
            in_memory_buffer.insert_data(
                current_batch_index,
                timestamps.value(index),
                values.value(index),
            );
        }

        Ok(in_memory_buffer)
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

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_common::test;
    use proptest::num::u64 as ProptestTimestamp;
    use proptest::{collection, proptest};
    use tokio::runtime::Runtime;

    const CURRENT_BATCH_INDEX: u64 = 1;
    const UNIVARIATE_ID: u64 = 1;

    // Tests for UncompressedInMemoryDataBuffer.
    #[test]
    fn test_get_in_memory_data_buffer_memory_size() {
        let uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        let expected = (uncompressed_buffer.timestamps.capacity() * mem::size_of::<Timestamp>())
            + (uncompressed_buffer.values.capacity() * mem::size_of::<Value>());

        assert_eq!(UncompressedInMemoryDataBuffer::memory_size(), expected);
        assert_eq!(
            UncompressedInMemoryDataBuffer::memory_size(),
            test::UNCOMPRESSED_BUFFER_SIZE
        );
    }

    #[test]
    fn test_get_in_memory_data_buffer_len() {
        let uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        assert_eq!(uncompressed_buffer.len(), 0);
    }

    #[test]
    fn test_can_insert_data_point_into_in_memory_data_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        insert_data_points(1, &mut uncompressed_buffer);

        assert_eq!(uncompressed_buffer.len(), 1);
    }

    #[test]
    fn test_check_if_in_memory_data_buffer_is_unused() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX - 1,
        );

        assert!(!uncompressed_buffer.is_unused(CURRENT_BATCH_INDEX - 1));
        assert!(uncompressed_buffer.is_unused(CURRENT_BATCH_INDEX));

        // Insert the data points as a batch with the index CURRENT_BATCH_INDEX.
        insert_data_points(uncompressed_buffer.capacity(), &mut uncompressed_buffer);

        assert!(!uncompressed_buffer.is_unused(CURRENT_BATCH_INDEX));
        assert!(uncompressed_buffer.is_unused(CURRENT_BATCH_INDEX + 1));
    }

    #[test]
    fn test_check_is_in_memory_data_buffer_full() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        insert_data_points(uncompressed_buffer.capacity(), &mut uncompressed_buffer);

        assert!(uncompressed_buffer.is_full());
    }

    #[test]
    fn test_check_is_in_memory_data_buffer_not_full() {
        let uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        assert!(!uncompressed_buffer.is_full());
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "Cannot insert data into full UncompressedInMemoryDataBuffer.")]
    fn test_in_memory_data_buffer_panic_if_inserting_data_point_when_full() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        insert_data_points(uncompressed_buffer.capacity() + 1, &mut uncompressed_buffer);
    }

    #[tokio::test]
    async fn test_get_record_batch_from_in_memory_data_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        insert_data_points(uncompressed_buffer.capacity(), &mut uncompressed_buffer);

        let capacity = uncompressed_buffer.capacity();
        let data = uncompressed_buffer.record_batch().await.unwrap();
        assert_eq!(data.num_columns(), 2);
        assert_eq!(data.num_rows(), capacity);
    }

    proptest! {
    #[test]
    fn test_record_batch_from_in_memory_data_buffer_is_sorted(timestamps in collection::vec(ProptestTimestamp::ANY, 1..50)) {
        // tokio::test is not supported in proptest! due to proptest-rs/proptest/issues/179
        let runtime = Runtime::new().unwrap();

        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        // u64 is generated and then cast to i64 to ensure only positive values are generated.
        for timestamp in timestamps {
            uncompressed_buffer.insert_data(CURRENT_BATCH_INDEX, timestamp as i64, 0.0);
        }

        let data = runtime.block_on(uncompressed_buffer.record_batch()).unwrap();
        assert_eq!(data.num_columns(), 2);
        let timestamps = modelardb_common::array!(data, 0, TimestampArray);
        assert!(timestamps.values().windows(2).all(|pair| pair[0] <= pair[1]));
    }
    }

    #[tokio::test]
    async fn test_in_memory_data_buffer_can_spill_not_full_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        insert_data_points(1, &mut uncompressed_buffer);
        assert!(!uncompressed_buffer.is_full());

        let temp_dir = tempfile::tempdir().unwrap();
        uncompressed_buffer
            .spill_to_apache_parquet(temp_dir.path())
            .await
            .unwrap();

        let uncompressed_path = temp_dir
            .path()
            .join(format!("{UNCOMPRESSED_DATA_FOLDER}/1"));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1)
    }

    #[tokio::test]
    async fn test_in_memory_data_buffer_can_spill_full_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        insert_data_points(uncompressed_buffer.capacity(), &mut uncompressed_buffer);
        assert!(uncompressed_buffer.is_full());

        let temp_dir = tempfile::tempdir().unwrap();
        uncompressed_buffer
            .spill_to_apache_parquet(temp_dir.path())
            .await
            .unwrap();

        let uncompressed_path = temp_dir
            .path()
            .join(format!("{UNCOMPRESSED_DATA_FOLDER}/1"));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1)
    }

    // Tests for UncompressedOnDiskDataBuffer.
    #[tokio::test]
    async fn test_get_on_disk_data_buffer_disk_size() {
        let mut uncompressed_in_memory_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        let capacity = uncompressed_in_memory_buffer.capacity();
        insert_data_points(capacity, &mut uncompressed_in_memory_buffer);

        let temp_dir = tempfile::tempdir().unwrap();
        let uncompressed_on_disk_buffer = uncompressed_in_memory_buffer
            .spill_to_apache_parquet(temp_dir.path())
            .await
            .unwrap();

        assert_eq!(uncompressed_on_disk_buffer.disk_size(), 3135)
    }

    #[tokio::test]
    async fn test_get_record_batch_from_on_disk_data_buffer() {
        let mut uncompressed_in_memory_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        let capacity = uncompressed_in_memory_buffer.capacity();
        insert_data_points(capacity, &mut uncompressed_in_memory_buffer);

        let temp_dir = tempfile::tempdir().unwrap();
        let uncompressed_on_disk_buffer = uncompressed_in_memory_buffer
            .spill_to_apache_parquet(temp_dir.path())
            .await
            .unwrap();

        let spilled_buffer_path = temp_dir
            .path()
            .join(UNCOMPRESSED_DATA_FOLDER)
            .join("1")
            .join("1234567890123.parquet");
        assert!(spilled_buffer_path.exists());

        let data = uncompressed_on_disk_buffer.record_batch().await.unwrap();

        assert_eq!(data.num_columns(), 2);
        assert_eq!(data.num_rows(), capacity);

        let spilled_buffer_path = temp_dir
            .path()
            .join(UNCOMPRESSED_DATA_FOLDER)
            .join("1")
            .join("1234567890123.parquet");
        assert!(!spilled_buffer_path.exists());
    }

    proptest! {
    #[test] fn test_record_batch_from_on_disk_data_buffer_is_sorted(timestamps in collection::vec(ProptestTimestamp::ANY, 1..50)) {
        // tokio::test is not supported in proptest! due to proptest-rs/proptest/issues/179
        let runtime = Runtime::new().unwrap();

        let mut uncompressed_in_memory_buffer = UncompressedInMemoryDataBuffer::new(
            UNIVARIATE_ID,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        // u64 is generated and then cast to i64 to ensure only positive values are generated.
        for timestamp in timestamps {
            uncompressed_in_memory_buffer.insert_data(CURRENT_BATCH_INDEX, timestamp as i64, 0.0);
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let uncompressed_on_disk_buffer = runtime.block_on(uncompressed_in_memory_buffer
            .spill_to_apache_parquet(temp_dir.path()))
            .unwrap();

        let spilled_buffer_folder = temp_dir
            .path()
            .join(UNCOMPRESSED_DATA_FOLDER)
            .join("1");

        assert_eq!(fs::read_dir(&spilled_buffer_folder).unwrap().count(), 1);

        let data = runtime.block_on(uncompressed_on_disk_buffer.record_batch()).unwrap();
        assert_eq!(data.num_columns(), 2);
        let timestamps = modelardb_common::array!(data, 0, TimestampArray);
        assert!(timestamps.values().windows(2).all(|pair| pair[0] <= pair[1]));

        assert_eq!(fs::read_dir(&spilled_buffer_folder).unwrap().count(), 0);
    }
    }

    /// Insert `count` generated data points into `uncompressed_buffer`.
    fn insert_data_points(count: usize, uncompressed_buffer: &mut UncompressedInMemoryDataBuffer) {
        let timestamp: Timestamp = 1234567890123;
        let value: Value = 30.0;

        for _ in 0..count {
            uncompressed_buffer.insert_data(CURRENT_BATCH_INDEX, timestamp, value);
        }
    }
}
