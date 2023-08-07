/* Copyright 2023 The ModelarDB Contributors
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

//! The minor types used throughout the [`StorageEngine`](crate::storage::StorageEngine).

use std::fmt;
use std::mem;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::arrow::array::UInt32Array;
use modelardb_common::types::{Timestamp, TimestampArray};
use ringbuf::{HeapRb, Rb};

/// Resizeable pool of memory for tracking and limiting the amount of memory used by the
/// [`StorageEngine`](crate::storage::StorageEngine). Signed integers are used to simplify updating
/// the amount of available memory at runtime. By using signed integers for the amount of available
/// memory it can simply be decreased without checking the current value as any attempts to reserve
/// additional memory will be rejected while the amount of available memory is negative. Thus,
/// [`StorageEngine`](crate::storage::StorageEngine) will decrease its memory usage.
pub(super) struct MemoryPool {
    /// How many bytes of memory that are left for storing
    /// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) containing multivariate time
    /// series with metadata and
    /// [`UncompressedDataBuffers`](crate::storage::uncompressed_data_buffer::UncompressedDataBuffer)
    /// containing univariate time series without metadata.
    remaining_uncompressed_memory_in_bytes: RwLock<isize>,
    /// How many bytes of memory that are left for storing
    /// [`CompressedDataBuffers`](crate::storage::compressed_data_buffer::CompressedDataBuffer).
    remaining_compressed_memory_in_bytes: RwLock<isize>,
}

/// The pool of memory the
/// [`UncompressedDataManager`](crate::storage::uncompressed_data_manager::UncompressedDataManager)
/// can use for uncompressed data and the
/// [`CompressedDataManager`](crate::storage::CompressedDataManager) can use for compressed data.
impl MemoryPool {
    /// Create a new [`MemoryPool`] with at most [`i64::MAX`] bytes of memory for uncompressed data
    /// and at most [`i64::MAX`] bytes of memory for compressed data.
    pub(super) fn new(
        uncompressed_memory_in_bytes: usize,
        compressed_memory_in_bytes: usize,
    ) -> Self {
        // unwrap() is safe as i64::MAX is 8192 PiB and the value is from ConfigurationManager.
        Self {
            remaining_uncompressed_memory_in_bytes: RwLock::new(
                uncompressed_memory_in_bytes.try_into().unwrap(),
            ),
            remaining_compressed_memory_in_bytes: RwLock::new(
                compressed_memory_in_bytes.try_into().unwrap(),
            ),
        }
    }

    /// Change the amount of memory available for uncompressed data by `size_in_bytes`.
    pub(super) fn adjust_uncompressed_memory(&self, size_in_bytes: isize) {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_uncompressed_memory_in_bytes.write().unwrap() += size_in_bytes
    }

    /// Return the amount of memory available for uncompressed data in bytes.
    pub(super) fn remaining_uncompressed_memory_in_bytes(&self) -> isize {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_uncompressed_memory_in_bytes.read().unwrap()
    }

    /// Try to reserve `size_in_bytes` bytes of memory for uncompressed data. Returns [`true`] if
    /// the reservation succeeds and [`false`] otherwise.
    #[must_use]
    pub(super) fn try_reserve_uncompressed_memory(&self, size_in_bytes: usize) -> bool {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        let mut remaining_uncompressed_memory_in_bytes =
            self.remaining_uncompressed_memory_in_bytes.write().unwrap();

        let size_in_bytes = size_in_bytes as isize;

        if size_in_bytes <= *remaining_uncompressed_memory_in_bytes {
            *remaining_uncompressed_memory_in_bytes -= size_in_bytes;
            true
        } else {
            false
        }
    }

    /// Free `size_in_bytes` bytes of memory for storing uncompressed data.
    pub(super) fn free_uncompressed_memory(&self, size_in_bytes: usize) {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_uncompressed_memory_in_bytes.write().unwrap() += size_in_bytes as isize;
    }

    /// Change the amount of memory available for storing compressed data by `size_in_bytes`.
    pub(super) fn adjust_compressed_memory(&self, size_in_bytes: isize) {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_compressed_memory_in_bytes.write().unwrap() += size_in_bytes;
    }

    /// Return the amount of memory available for storing compressed data in bytes.
    pub(super) fn remaining_compressed_memory_in_bytes(&self) -> isize {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_compressed_memory_in_bytes.read().unwrap()
    }

    /// Try to reserve `size_in_bytes` bytes of memory for storing compressed data. Returns [`true`]
    /// if the reservation succeeds and [`false`] otherwise.
    #[must_use]
    pub(super) fn try_reserve_compressed_memory(&self, size_in_bytes: usize) -> bool {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        let mut remaining_compressed_memory_in_bytes =
            self.remaining_compressed_memory_in_bytes.write().unwrap();

        let size_in_bytes = size_in_bytes as isize;

        if size_in_bytes <= *remaining_compressed_memory_in_bytes {
            *remaining_compressed_memory_in_bytes -= size_in_bytes;
            true
        } else {
            false
        }
    }

    /// Free `size_in_bytes` bytes of memory for storing compressed data.
    pub(super) fn free_compressed_memory(&self, size_in_bytes: usize) {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_compressed_memory_in_bytes.write().unwrap() += size_in_bytes as isize;
    }
}

/// The different types of metrics that are collected in the storage engine.
pub enum MetricType {
    UsedUncompressedMemory,
    UsedCompressedMemory,
    IngestedDataPoints,
    UsedDiskSpace,
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UsedUncompressedMemory => write!(f, "used_uncompressed_memory"),
            Self::UsedCompressedMemory => write!(f, "used_compressed_memory"),
            Self::IngestedDataPoints => write!(f, "ingested_data_points"),
            Self::UsedDiskSpace => write!(f, "used_disk_space"),
        }
    }
}

/// Metric used to record changes in specific attributes in the storage engine. The timestamps
/// and values of the metric is stored in ring buffers to ensure the amount of memory used by the
/// metric is capped.
pub struct Metric {
    /// Ring buffer consisting of a capped amount of millisecond precision timestamps.
    timestamps: HeapRb<Timestamp>,
    /// Ring buffer consisting of a capped amount of values.
    values: HeapRb<u32>,
    /// Last saved metric value, used to support updating the metric based on a change to the last
    /// value instead of simply storing the new value. Since the values builder is cleared when the metric
    /// is finished, the last value is saved separately.
    last_value: isize,
}

impl Metric {
    pub(super) fn new() -> Self {
        // The capacity of the timestamps and values ring buffers. This ensures that the total
        // memory used by the metric is capped to ~1 MiB.
        let capacity = (1024 * 1024) / (mem::size_of::<Timestamp>() + mem::size_of::<u32>());

        Self {
            timestamps: HeapRb::<Timestamp>::new(capacity),
            values: HeapRb::<u32>::new(capacity),
            last_value: 0,
        }
    }

    /// Add a new entry to the metric, where the timestamp is the current milliseconds since the Unix
    /// epoch and the value is either set directly or based on the last value in the metric.
    pub(super) fn append(&mut self, value: isize, based_on_last: bool) {
        // unwrap() is safe since the Unix epoch is always earlier than now.
        let since_the_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let timestamp = since_the_epoch.as_millis() as Timestamp;

        let mut new_value = value;
        if based_on_last {
            new_value = self.last_value + value;
        }

        self.timestamps.push_overwrite(timestamp);
        self.values.push_overwrite(new_value as u32);
        self.last_value = new_value;
    }

    /// Return a reference to the metric's values for testing.
    #[cfg(test)]
    pub(super) fn values(&self) -> &HeapRb<u32> {
        &self.values
    }

    /// Finish and reset the internal ring buffers and return the timestamps and values as Apache Arrow arrays.
    pub(super) fn finish(&mut self) -> (TimestampArray, UInt32Array) {
        let timestamps = TimestampArray::from_iter_values(self.timestamps.pop_iter());
        let values = UInt32Array::from_iter_values(self.values.pop_iter());

        (timestamps, values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common_test;

    // Tests for MemoryPool.
    #[test]
    fn test_adjust_uncompressed_memory_increase() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_uncompressed_memory(common_test::COMPRESSED_SEGMENTS_SIZE as isize);

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            (common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES
                + common_test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_uncompressed_memory_decrease_above_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_uncompressed_memory(-(common_test::COMPRESSED_SEGMENTS_SIZE as isize));

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            (common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES
                - common_test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_uncompressed_memory_decrease_below_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_uncompressed_memory(
            -2 * common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize,
        );

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            -(common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize)
        );
    }

    #[test]
    fn test_try_reserve_available_uncompressed_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(memory_pool
            .try_reserve_uncompressed_memory(common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES));

        assert_eq!(memory_pool.remaining_uncompressed_memory_in_bytes(), 0);
    }

    #[test]
    fn test_try_reserve_unavailable_uncompressed_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(!memory_pool.try_reserve_uncompressed_memory(
            2 * common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
        ));

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );
    }

    #[test]
    fn test_free_uncompressed_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.free_uncompressed_memory(common_test::COMPRESSED_SEGMENTS_SIZE);

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            (common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES
                + common_test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_compressed_memory_increase() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_compressed_memory(common_test::COMPRESSED_SEGMENTS_SIZE as isize);

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            (common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES
                + common_test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_compressed_memory_decrease_above_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_compressed_memory(-(common_test::COMPRESSED_SEGMENTS_SIZE as isize));

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            (common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES
                - common_test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_compressed_memory_decrease_below_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_compressed_memory(
            -2 * common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize,
        );

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            -(common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize)
        );
    }

    #[test]
    fn test_try_reserve_available_compressed_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(memory_pool
            .try_reserve_compressed_memory(common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES));

        assert_eq!(memory_pool.remaining_compressed_memory_in_bytes(), 0);
    }

    #[test]
    fn test_try_reserve_unavailable_compressed_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(!memory_pool
            .try_reserve_compressed_memory(2 * common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES));

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );
    }

    #[test]
    fn test_free_compressed_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.free_compressed_memory(common_test::COMPRESSED_SEGMENTS_SIZE);

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            (common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES
                + common_test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    fn create_memory_pool() -> MemoryPool {
        MemoryPool::new(
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        )
    }

    // Tests for Metric.
    #[test]
    fn test_append_to_metric() {
        let mut metric = Metric::new();
        let since_the_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let timestamp = since_the_epoch.as_millis() as Timestamp;

        metric.append(30, false);
        metric.append(30, false);

        assert_eq!(metric.timestamps.pop_iter().last(), Some(timestamp));
        assert_eq!(metric.values.pop_iter().last(), Some(30));
    }

    #[test]
    fn test_append_positive_value_to_metric_based_on_last() {
        let mut metric = Metric::new();

        metric.append(30, true);
        metric.append(30, true);

        assert_eq!(metric.values.pop_iter().last(), Some(60));
    }

    #[test]
    fn test_append_negative_value_to_metric_based_on_last() {
        let mut metric = Metric::new();

        metric.append(30, true);
        metric.append(-30, true);

        assert_eq!(metric.values.pop_iter().last(), Some(0));
    }

    #[test]
    fn test_finish_metric() {
        let mut metric = Metric::new();

        metric.append(30, true);
        metric.append(-30, true);

        let (_timestamps, values) = metric.finish();
        assert_eq!(values.value(0), 30);
        assert_eq!(values.value(1), 0);

        // Ensure that the builders in the metric has been reset.
        assert_eq!(metric.timestamps.len(), 0);
        assert_eq!(metric.values.len(), 0);
    }
}
