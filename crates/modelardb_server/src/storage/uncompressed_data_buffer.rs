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

use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::mem;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayBuilder};
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use modelardb_storage::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_types::types::{
    Timestamp, TimestampArray, TimestampBuilder, Value, ValueArray, ValueBuilder,
};
use object_store::path::Path;
use object_store::ObjectStore;
use tracing::debug;

use crate::error::Result;
use crate::storage::{UNCOMPRESSED_DATA_BUFFER_CAPACITY, UNCOMPRESSED_DATA_FOLDER};

/// Number of [`RecordBatches`](RecordBatch) that must be ingested without modifying an
/// [`UncompressedInMemoryDataBuffer`] before it is considered unused and can be finished.
const RECORD_BATCH_OFFSET_REQUIRED_FOR_UNUSED: u64 = 1;

/// Ingested data points from one or more time series to be inserted into a model table.
pub(super) struct IngestedDataBuffer {
    /// Metadata of the model table to insert the data points into.
    pub(super) model_table_metadata: Arc<ModelTableMetadata>,
    /// Uncompressed data points to insert.
    pub(super) data_points: RecordBatch,
}

impl IngestedDataBuffer {
    pub(super) fn new(
        model_table_metadata: Arc<ModelTableMetadata>,
        data_points: RecordBatch,
    ) -> Self {
        Self {
            model_table_metadata,
            data_points,
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

/// A writeable in-memory data buffer that new data points from a time series can be efficiently
/// appended to. It consists of an ordered sequence of data points with each part stored in a
/// separate [`PrimitiveBuilder`](datafusion::arrow::array::PrimitiveBuilder).
pub(super) struct UncompressedInMemoryDataBuffer {
    /// Id that uniquely identifies the time series the buffer stores data points for.
    tag_hash: u64,
    /// Metadata of the model table the buffer stores data for.
    model_table_metadata: Arc<ModelTableMetadata>,
    /// Index of the last batch that caused the buffer to be updated.
    updated_by_batch_index: u64,
    /// Builder that timestamps are appended to.
    timestamps: TimestampBuilder,
    /// Builders for each stored field that float values are appended to.
    values: Vec<ValueBuilder>,
}

impl UncompressedInMemoryDataBuffer {
    pub(super) fn new(
        tag_hash: u64,
        model_table_metadata: Arc<ModelTableMetadata>,
        current_batch_index: u64,
    ) -> Self {
        let timestamps = TimestampBuilder::with_capacity(*UNCOMPRESSED_DATA_BUFFER_CAPACITY);
        let values = (0..model_table_metadata.field_column_indices.len())
            .map(|_| ValueBuilder::with_capacity(*UNCOMPRESSED_DATA_BUFFER_CAPACITY))
            .collect();

        Self {
            tag_hash,
            model_table_metadata,
            updated_by_batch_index: current_batch_index,
            timestamps,
            values,
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

    /// Add `timestamp` and `values` to the [`UncompressedInMemoryDataBuffer`] from the
    /// [`RecordBatch`] ingested by the process with the index `current_batch_index`.
    pub(super) fn insert_data_point(
        &mut self,
        current_batch_index: u64,
        timestamp: Timestamp,
        values: &mut dyn Iterator<Item = Value>,
    ) {
        debug_assert!(
            !self.is_full(),
            "Cannot insert data into full UncompressedInMemoryDataBuffer."
        );

        self.updated_by_batch_index = current_batch_index;
        self.timestamps.append_value(timestamp);
        for (index, value) in values.enumerate() {
            self.values[index].append_value(value);
        }

        debug!("Inserted data point into {:?}.", self)
    }

    /// Return how many data points the [`UncompressedInMemoryDataBuffer`] can contain.
    pub(super) fn capacity(&self) -> usize {
        // The capacity is always the same for both builders.
        self.timestamps.capacity()
    }

    /// Finish the array builders and return the data in a [`RecordBatch`] sorted by time.
    pub(super) async fn record_batch(&mut self) -> Result<RecordBatch> {
        let timestamps = self.timestamps.finish();

        // lexsort() is not used as it is unclear in what order it sorts multiple arrays, instead a
        // combination of sort_to_indices() and take(), like how lexsort() is implemented, is used.
        // unwrap() is safe as timestamps has a supported type and sorted_indices are within bounds.
        let sorted_indices = compute::sort_to_indices(&timestamps, None, None).unwrap();

        let mut columns = Vec::with_capacity(1 + self.values.len());
        columns.push(compute::take(&timestamps, &sorted_indices, None).unwrap());
        for value in &mut self.values {
            columns.push(compute::take(&value.finish(), &sorted_indices, None).unwrap());
        }

        // unwrap() is safe as uncompressed_schema only contains timestamps and values.
        Ok(RecordBatch::try_new(
            self.model_table_metadata.uncompressed_schema.clone(),
            columns,
        )
        .unwrap())
    }

    /// Return the tag hash that identifies the time series the buffer stores data points from.
    pub(super) fn tag_hash(&self) -> u64 {
        self.tag_hash
    }

    /// Return the metadata for the model table the buffer stores data points for.
    pub(super) fn model_table_metadata(&self) -> &Arc<ModelTableMetadata> {
        &self.model_table_metadata
    }

    /// Return the total size of this [`UncompressedInMemoryDataBuffer`] in bytes.
    pub(super) fn memory_size(&self) -> usize {
        compute_memory_size(self.values.len())
    }

    /// Spill the in-memory [`UncompressedInMemoryDataBuffer`] to an Apache Parquet file and return
    /// an [`UncompressedOnDiskDataBuffer`] when finished.
    pub(super) async fn spill_to_apache_parquet(
        &mut self,
        local_data_folder: Arc<dyn ObjectStore>,
    ) -> Result<UncompressedOnDiskDataBuffer> {
        let data_points = self.record_batch().await?;

        UncompressedOnDiskDataBuffer::try_spill(
            self.tag_hash,
            self.model_table_metadata.clone(),
            self.updated_by_batch_index,
            local_data_folder,
            data_points,
        )
        .await
    }
}

impl Debug for UncompressedInMemoryDataBuffer {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "UncompressedInMemoryDataBuffer({}, {}, {}, {})",
            self.tag_hash,
            self.len(),
            self.capacity(),
            self.updated_by_batch_index,
        )
    }
}

/// Return the total amount of memory in bytes an [`UncompressedInMemoryDataBuffer`] storing data
/// points from a model table with `number_of_fields` field columns will require.
pub(super) fn compute_memory_size(number_of_fields: usize) -> usize {
    (*UNCOMPRESSED_DATA_BUFFER_CAPACITY * mem::size_of::<Timestamp>())
        + (number_of_fields * (*UNCOMPRESSED_DATA_BUFFER_CAPACITY * mem::size_of::<Value>()))
}

/// A read only uncompressed buffer containing data points from a time series that has been spilled
/// to disk as an Apache Parquet file due to memory constraints during ingestion.
pub(super) struct UncompressedOnDiskDataBuffer {
    /// Id that uniquely identifies the time series the buffer stores data points for.
    tag_hash: u64,
    /// Metadata of the model table the buffer stores data for.
    model_table_metadata: Arc<ModelTableMetadata>,
    /// Index of the last batch that added data points to this buffer.
    updated_by_batch_index: u64,
    /// Location for storing spilled buffers at `file_path`.
    local_data_folder: Arc<dyn ObjectStore>,
    /// Path to the Apache Parquet file containing the uncompressed data in the
    /// [`UncompressedOnDiskDataBuffer`].
    file_path: Path,
}

impl UncompressedOnDiskDataBuffer {
    /// Spill the in-memory `data_points` from the time series with `tag_hash` to an Apache Parquet
    /// file in `local_data_folder`. If the Apache Parquet file is written successfully, return an
    /// [`UncompressedOnDiskDataBuffer`], otherwise return
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    pub(super) async fn try_spill(
        tag_hash: u64,
        model_table_metadata: Arc<ModelTableMetadata>,
        updated_by_batch_index: u64,
        local_data_folder: Arc<dyn ObjectStore>,
        data_points: RecordBatch,
    ) -> Result<Self> {
        // Create a path that uses the first timestamp as the filename.
        let timestamps = modelardb_types::array!(data_points, 0, TimestampArray);
        let file_path = Path::from(format!(
            "{UNCOMPRESSED_DATA_FOLDER}/{tag_hash}/{}.parquet",
            timestamps.value(0)
        ));

        modelardb_storage::write_record_batch_to_apache_parquet_file(
            &file_path,
            &data_points,
            None,
            &(local_data_folder.clone() as Arc<dyn ObjectStore>),
        )
        .await?;

        Ok(Self {
            tag_hash,
            model_table_metadata,
            updated_by_batch_index,
            local_data_folder,
            file_path,
        })
    }

    /// Return an [`UncompressedOnDiskDataBuffer`] with the data points for `tag_hash` in
    /// `file_path` if a file at `file_path` exists, otherwise
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError) is returned.
    pub(super) fn try_new(
        tag_hash: u64,
        model_table_metadata: Arc<ModelTableMetadata>,
        updated_by_batch_index: u64,
        local_data_folder: Arc<dyn ObjectStore>,
        file_name: &str,
    ) -> Result<Self> {
        let file_path = Path::from(format!("{UNCOMPRESSED_DATA_FOLDER}/{tag_hash}/{file_name}"));

        Ok(Self {
            tag_hash,
            model_table_metadata,
            updated_by_batch_index,
            local_data_folder,
            file_path,
        })
    }

    /// Read the data from the Apache Parquet file, delete the Apache Parquet file, and return the
    /// data as a [`RecordBatch`] sorted by time. Return
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError) if the Apache Parquet file
    /// cannot be read or deleted.
    pub(super) async fn record_batch(&self) -> Result<RecordBatch> {
        let data_points = modelardb_storage::read_record_batch_from_apache_parquet_file(
            &self.file_path,
            self.local_data_folder.clone(),
        )
        .await?;

        self.local_data_folder.delete(&self.file_path).await?;

        Ok(data_points)
    }

    /// Return the tag hash that identifies the time series the buffer stores data points from.
    pub(super) fn tag_hash(&self) -> u64 {
        self.tag_hash
    }

    /// Return the metadata for the model table the buffer stores data points for.
    pub(super) fn model_table_metadata(&self) -> &Arc<ModelTableMetadata> {
        &self.model_table_metadata
    }

    /// Return the total size of the Apache Parquet file containing the uncompressed data buffer.
    pub(super) async fn disk_size(&self) -> usize {
        // unwrap() is safe since the path is created internally.
        self.local_data_folder
            .head(&self.file_path)
            .await
            .unwrap()
            .size
    }

    /// Return [`true`] if all the data points in the [`UncompressedOnDiskDataBuffer`] are from
    /// [`RecordBatches`](`RecordBatch`) that are [`RECORD_BATCH_OFFSET_REQUIRED_FOR_UNUSED`] older
    /// than the [`RecordBatch`] with index `current_batch_index` ingested by the current process.
    pub(super) fn is_unused(&self, current_batch_index: u64) -> bool {
        self.updated_by_batch_index + RECORD_BATCH_OFFSET_REQUIRED_FOR_UNUSED <= current_batch_index
    }

    /// Read the data from the Apache Parquet file, delete the Apache Parquet file, and return the
    /// data as a [`UncompressedInMemoryDataBuffer`] sorted by time. Return
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError) if the Apache Parquet file
    /// cannot be read or deleted.
    pub(super) async fn read_from_apache_parquet(
        &self,
        current_batch_index: u64,
    ) -> Result<UncompressedInMemoryDataBuffer> {
        let data_points = self.record_batch().await?;

        let timestamp_column_array = modelardb_types::array!(data_points, 0, TimestampArray);
        let field_column_arrays: Vec<_> = (1..data_points.num_columns())
            .map(|index| modelardb_types::array!(data_points, index, ValueArray))
            .collect();

        let mut in_memory_buffer = UncompressedInMemoryDataBuffer::new(
            self.tag_hash,
            self.model_table_metadata.clone(),
            current_batch_index,
        );

        for index in 0..data_points.num_rows() {
            let mut values = field_column_arrays.iter().map(|array| array.value(index));

            in_memory_buffer.insert_data_point(
                current_batch_index,
                timestamp_column_array.value(index),
                &mut values,
            );
        }

        Ok(in_memory_buffer)
    }
}

impl Debug for UncompressedOnDiskDataBuffer {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "UncompressedOnDiskDataBuffer({}, {})",
            self.tag_hash, self.file_path
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::StreamExt;
    use modelardb_common::test::UNCOMPRESSED_BUFFER_SIZE;
    use modelardb_storage::test;
    use object_store::local::LocalFileSystem;
    use proptest::num::u64 as ProptestTimestamp;
    use proptest::{collection, proptest};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    const CURRENT_BATCH_INDEX: u64 = 1;
    const TAG_HASH: u64 = 1;

    // Tests for UncompressedInMemoryDataBuffer.
    #[test]
    fn test_get_in_memory_data_buffer_memory_size() {
        let uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        assert_eq!(
            uncompressed_buffer.timestamps.capacity(),
            *UNCOMPRESSED_DATA_BUFFER_CAPACITY
        );
        for values in &uncompressed_buffer.values {
            assert_eq!(values.capacity(), *UNCOMPRESSED_DATA_BUFFER_CAPACITY);
        }

        let number_of_fields = test::model_table_metadata().field_column_indices.len();
        let expected = (uncompressed_buffer.timestamps.capacity() * mem::size_of::<Timestamp>())
            + (number_of_fields
                * uncompressed_buffer.values[0].capacity()
                * mem::size_of::<Value>());

        assert_eq!(uncompressed_buffer.memory_size(), expected);
        assert_eq!(uncompressed_buffer.memory_size(), UNCOMPRESSED_BUFFER_SIZE);
    }

    #[test]
    fn test_get_in_memory_data_buffer_len() {
        let uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        assert_eq!(uncompressed_buffer.len(), 0);
    }

    #[test]
    fn test_can_insert_data_point_into_in_memory_data_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        insert_data_points(1, &mut uncompressed_buffer);

        assert_eq!(uncompressed_buffer.len(), 1);
    }

    #[test]
    fn test_check_if_in_memory_data_buffer_is_unused() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            TAG_HASH,
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
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        insert_data_points(uncompressed_buffer.capacity(), &mut uncompressed_buffer);

        assert!(uncompressed_buffer.is_full());
    }

    #[test]
    fn test_check_is_in_memory_data_buffer_not_full() {
        let uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            TAG_HASH,
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
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        insert_data_points(uncompressed_buffer.capacity() + 1, &mut uncompressed_buffer);
    }

    #[tokio::test]
    async fn test_get_record_batch_from_in_memory_data_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        insert_data_points(uncompressed_buffer.capacity(), &mut uncompressed_buffer);

        let capacity = uncompressed_buffer.capacity();
        let data = uncompressed_buffer.record_batch().await.unwrap();
        assert_eq!(data.num_columns(), 3);
        assert_eq!(data.num_rows(), capacity);
    }

    proptest! {
    #[test]
    fn test_record_batch_from_in_memory_data_buffer_is_sorted(timestamps in collection::vec(ProptestTimestamp::ANY, 1..50)) {
        // tokio::test is not supported in proptest! due to proptest-rs/proptest/issues/179.
        let runtime = Runtime::new().unwrap();

        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        // u64 is generated and then cast to i64 to ensure only positive values are generated.
        let values: &[Value] = &[37.0, 73.0];
        for timestamp in timestamps {
            uncompressed_buffer.insert_data_point(CURRENT_BATCH_INDEX, timestamp as i64, &mut values.iter().copied());
        }

        let data = runtime.block_on(uncompressed_buffer.record_batch()).unwrap();
        assert_eq!(data.num_columns(), 3);
        let timestamps = modelardb_types::array!(data, 0, TimestampArray);
        assert!(timestamps.values().windows(2).all(|pair| pair[0] <= pair[1]));
    }
    }

    #[tokio::test]
    async fn test_in_memory_data_buffer_can_spill_not_full_buffer() {
        let mut uncompressed_buffer = UncompressedInMemoryDataBuffer::new(
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        insert_data_points(1, &mut uncompressed_buffer);
        assert!(!uncompressed_buffer.is_full());

        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        uncompressed_buffer
            .spill_to_apache_parquet(object_store)
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
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );
        insert_data_points(uncompressed_buffer.capacity(), &mut uncompressed_buffer);
        assert!(uncompressed_buffer.is_full());

        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        uncompressed_buffer
            .spill_to_apache_parquet(object_store)
            .await
            .unwrap();

        let uncompressed_path = temp_dir
            .path()
            .join(format!("{UNCOMPRESSED_DATA_FOLDER}/1"));
        assert_eq!(uncompressed_path.read_dir().unwrap().count(), 1)
    }

    // Tests for UncompressedOnDiskDataBuffer.
    #[tokio::test]
    async fn test_get_record_batch_from_on_disk_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let uncompressed_on_disk_buffer = create_on_disk_data_buffer(&temp_dir).await;

        let spilled_buffer_path = temp_dir
            .path()
            .join(UNCOMPRESSED_DATA_FOLDER)
            .join("1")
            .join("1234567890123.parquet");
        assert!(spilled_buffer_path.exists());

        let data = uncompressed_on_disk_buffer.record_batch().await.unwrap();

        assert_eq!(data.num_columns(), 3);
        assert_eq!(data.num_rows(), *UNCOMPRESSED_DATA_BUFFER_CAPACITY);

        let spilled_buffer_path = temp_dir
            .path()
            .join(UNCOMPRESSED_DATA_FOLDER)
            .join("1")
            .join("1234567890123.parquet");
        assert!(!spilled_buffer_path.exists());
    }

    proptest! {
    #[test] fn test_record_batch_from_on_disk_data_buffer_is_sorted(timestamps in collection::vec(ProptestTimestamp::ANY, 1..50)) {
        // tokio::test is not supported in proptest! due to proptest-rs/proptest/issues/179.
        let runtime = Runtime::new().unwrap();

        let mut uncompressed_in_memory_buffer = UncompressedInMemoryDataBuffer::new(
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        // u64 is generated and then cast to i64 to ensure only positive values are generated.
        let values: &[Value] = &[37.0, 73.0];
        for timestamp in timestamps {
            uncompressed_in_memory_buffer.insert_data_point(CURRENT_BATCH_INDEX, timestamp as i64, &mut values.iter().copied());
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        let uncompressed_on_disk_buffer = runtime.block_on(uncompressed_in_memory_buffer
            .spill_to_apache_parquet(object_store.clone()))
            .unwrap();

        let spilled_buffers = runtime.block_on(object_store.list(Some(&Path::from(UNCOMPRESSED_DATA_FOLDER))).collect::<Vec<_>>());
        assert_eq!(spilled_buffers.len(), 1);

        let data = runtime.block_on(uncompressed_on_disk_buffer.record_batch()).unwrap();
        assert_eq!(data.num_columns(), 3);
        let timestamps = modelardb_types::array!(data, 0, TimestampArray);
        assert!(timestamps.values().windows(2).all(|pair| pair[0] <= pair[1]));

        let spilled_buffers = runtime.block_on(object_store.list(Some(&Path::from(UNCOMPRESSED_DATA_FOLDER))).collect::<Vec<_>>());
        assert_eq!(spilled_buffers.len(), 0);
    }
    }

    #[tokio::test]
    async fn test_get_on_disk_data_buffer_disk_size() {
        let temp_dir = tempfile::tempdir().unwrap();
        let uncompressed_on_disk_buffer = create_on_disk_data_buffer(&temp_dir).await;

        assert_eq!(uncompressed_on_disk_buffer.disk_size().await, 4009)
    }

    #[tokio::test]
    async fn test_check_if_on_disk_data_buffer_is_unused() {
        let temp_dir = tempfile::tempdir().unwrap();
        let uncompressed_on_disk_buffer = create_on_disk_data_buffer(&temp_dir).await;

        assert!(!uncompressed_on_disk_buffer.is_unused(CURRENT_BATCH_INDEX));
        assert!(uncompressed_on_disk_buffer.is_unused(CURRENT_BATCH_INDEX + 1));
    }

    #[tokio::test]
    async fn test_read_in_memory_data_buffer_from_on_disk_data_buffer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let uncompressed_on_disk_buffer = create_on_disk_data_buffer(&temp_dir).await;

        let read_uncompressed_in_memory_buffer = uncompressed_on_disk_buffer
            .read_from_apache_parquet(CURRENT_BATCH_INDEX)
            .await
            .unwrap();

        // The creation of record_batch empties uncompressed_in_memory_buffer_to_be_spilled.
        let mut uncompressed_in_memory_buffer = UncompressedInMemoryDataBuffer::new(
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        insert_data_points(
            uncompressed_in_memory_buffer.capacity(),
            &mut uncompressed_in_memory_buffer,
        );

        assert_eq!(
            uncompressed_in_memory_buffer.timestamps.values_slice(),
            read_uncompressed_in_memory_buffer.timestamps.values_slice()
        );

        assert_eq!(
            uncompressed_in_memory_buffer.values.len(),
            read_uncompressed_in_memory_buffer.values.len()
        );

        let values_len = uncompressed_in_memory_buffer.values.len();
        for value_index in 0..values_len {
            assert_eq!(
                uncompressed_in_memory_buffer.values[value_index].values_slice(),
                read_uncompressed_in_memory_buffer.values[value_index].values_slice()
            );
        }
    }

    /// Create an on-disk data buffer in `temp_dir` from a full `UncompressedInMemoryDataBuffer`.
    async fn create_on_disk_data_buffer(temp_dir: &TempDir) -> UncompressedOnDiskDataBuffer {
        let local_data_folder =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        let mut uncompressed_in_memory_buffer_to_be_spilled = UncompressedInMemoryDataBuffer::new(
            TAG_HASH,
            test::model_table_metadata_arc(),
            CURRENT_BATCH_INDEX,
        );

        insert_data_points(
            uncompressed_in_memory_buffer_to_be_spilled.capacity(),
            &mut uncompressed_in_memory_buffer_to_be_spilled,
        );

        uncompressed_in_memory_buffer_to_be_spilled
            .spill_to_apache_parquet(local_data_folder)
            .await
            .unwrap()
    }

    /// Insert `count` generated data points into `uncompressed_buffer`.
    fn insert_data_points(count: usize, uncompressed_buffer: &mut UncompressedInMemoryDataBuffer) {
        let timestamp: Timestamp = 1234567890123;
        let values: &[Value] = &[37.0, 73.0];

        for _ in 0..count {
            uncompressed_buffer.insert_data_point(
                CURRENT_BATCH_INDEX,
                timestamp,
                &mut values.iter().copied(),
            );
        }
    }
}
