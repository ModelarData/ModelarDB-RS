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

use std::error::Error;
use std::fmt;
use std::mem;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, Sender};
use datafusion::arrow::array::UInt32Array;
use modelardb_common::types::{Timestamp, TimestampArray};
// rustc 1.77.2 warns about Observer being unused but ringbuf 0.4.0 requires it to be imported.
#[allow(unused_imports)]
use ringbuf::traits::Observer;
use ringbuf::traits::{Consumer, RingBuffer};
use ringbuf::HeapRb;

use crate::storage::compressed_data_buffer::CompressedSegmentBatch;
use crate::storage::uncompressed_data_buffer::{IngestedDataBuffer, UncompressedDataBuffer};

/// Resizeable pool of memory for tracking and limiting the amount of memory used by the
/// [`StorageEngine`](crate::storage::StorageEngine). Signed integers are used to simplify updating
/// the amount of available memory at runtime. By using signed integers for the amount of available
/// memory it can simply be decreased without checking the current value as any attempts to reserve
/// additional memory will be rejected while the amount of available memory is negative. Thus,
/// [`StorageEngine`](crate::storage::StorageEngine) will decrease its memory usage.
pub(super) struct MemoryPool {
    /// Condition variable that allows threads to wait for more ingested memory to be released.
    wait_for_ingested_memory: Condvar,
    /// How many bytes of memory that are left for storing
    /// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) containing ingested time
    /// series with metadata.
    remaining_ingested_memory_in_bytes: Mutex<isize>,
    /// Condition variable that allows threads to wait for more uncompressed memory to be released.
    wait_for_uncompressed_memory: Condvar,
    /// How many bytes of memory that are left for storing
    /// [`UncompressedDataBuffers`](UncompressedDataBuffer) containing time series without
    /// metadata.
    remaining_uncompressed_memory_in_bytes: Mutex<isize>,
    /// How many bytes of memory that are left for storing
    /// [`CompressedDataBuffers`](crate::storage::compressed_data_buffer::CompressedDataBuffer).
    remaining_compressed_memory_in_bytes: Mutex<isize>,
}

/// The pool of memory the [`StorageEngine`](super::StorageEngine) can use for ingested data, the
/// [`UncompressedDataManager`](crate::storage::uncompressed_data_manager::UncompressedDataManager)
/// can use for uncompressed data, and the
/// [`CompressedDataManager`](crate::storage::CompressedDataManager) can use for compressed data.
impl MemoryPool {
    /// Create a new [`MemoryPool`] with at most [`i64::MAX`] bytes of memory for ingested,
    /// uncompressed, and compressed data.
    pub(super) fn new(
        ingested_memory_in_bytes: usize,
        uncompressed_memory_in_bytes: usize,
        compressed_memory_in_bytes: usize,
    ) -> Self {
        // unwrap() is safe as i64::MAX is 8192 PiB and the value is from ConfigurationManager.
        Self {
            wait_for_ingested_memory: Condvar::new(),
            remaining_ingested_memory_in_bytes: Mutex::new(
                ingested_memory_in_bytes.try_into().unwrap(),
            ),
            wait_for_uncompressed_memory: Condvar::new(),
            remaining_uncompressed_memory_in_bytes: Mutex::new(
                uncompressed_memory_in_bytes.try_into().unwrap(),
            ),
            remaining_compressed_memory_in_bytes: Mutex::new(
                compressed_memory_in_bytes.try_into().unwrap(),
            ),
        }
    }

    /// Change the amount of memory available for ingested data by `size_in_bytes`.
    pub(super) fn adjust_ingested_memory(&self, size_in_bytes: isize) {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_ingested_memory_in_bytes.lock().unwrap() += size_in_bytes;
        self.wait_for_ingested_memory.notify_all();
    }

    /// Return the amount of memory available for ingested data in bytes.
    #[cfg(test)]
    #[must_use]
    pub(super) fn remaining_ingested_memory_in_bytes(&self) -> isize {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_ingested_memory_in_bytes.lock().unwrap()
    }

    /// Wait until `size_in_bytes` bytes of memory is available for ingested data and then reserve
    /// it.
    pub(super) fn wait_for_ingested_memory(&self, size_in_bytes: usize) {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        let mut memory_in_bytes = self.remaining_ingested_memory_in_bytes.lock().unwrap();

        while *memory_in_bytes < size_in_bytes as isize {
            // unwrap() is safe as wait() only returns an error if the mutex is poisoned.
            memory_in_bytes = self.wait_for_ingested_memory.wait(memory_in_bytes).unwrap();
        }

        *memory_in_bytes -= size_in_bytes as isize;
    }

    /// Change the amount of memory available for uncompressed data by `size_in_bytes`.
    pub(super) fn adjust_uncompressed_memory(&self, size_in_bytes: isize) {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_uncompressed_memory_in_bytes.lock().unwrap() += size_in_bytes;
        self.wait_for_uncompressed_memory.notify_all();
    }

    /// Return the amount of memory available for uncompressed data in bytes.
    #[must_use]
    pub(super) fn remaining_uncompressed_memory_in_bytes(&self) -> isize {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_uncompressed_memory_in_bytes.lock().unwrap()
    }

    /// Wait until `size_in_bytes` bytes of memory is available for uncompressed data or `stop_if`
    /// returns [`true`]. Note that `stop_if` is never evaluated while the thread is waiting.
    /// Returns [`true`] if the memory was reserved and [`false`] if not.
    #[must_use]
    pub(super) fn wait_for_uncompressed_memory_until<F: Fn() -> bool>(
        &self,
        size_in_bytes: usize,
        stop_if: F,
    ) -> bool {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        let mut memory_in_bytes = self.remaining_uncompressed_memory_in_bytes.lock().unwrap();

        while *memory_in_bytes < size_in_bytes as isize {
            // There is still not enough memory available, but it is no longer sensible to wait.
            if stop_if() {
                return false;
            }

            // unwrap() is safe as wait() only returns an error if the mutex is poisoned.
            memory_in_bytes = self
                .wait_for_uncompressed_memory
                .wait(memory_in_bytes)
                .unwrap();
        }

        *memory_in_bytes -= size_in_bytes as isize;
        true
    }

    /// Change the amount of memory available for storing compressed data by `size_in_bytes`.
    pub(super) fn adjust_compressed_memory(&self, size_in_bytes: isize) {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_compressed_memory_in_bytes.lock().unwrap() += size_in_bytes;
    }

    /// Return the amount of memory available for storing compressed data in bytes.
    #[must_use]
    pub(super) fn remaining_compressed_memory_in_bytes(&self) -> isize {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_compressed_memory_in_bytes.lock().unwrap()
    }

    /// Try to reserve `size_in_bytes` bytes of memory for storing compressed data. Returns [`true`]
    /// if the reservation succeeds and [`false`] otherwise.
    #[must_use]
    pub(super) fn try_reserve_compressed_memory(&self, size_in_bytes: usize) -> bool {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        let mut remaining_compressed_memory_in_bytes =
            self.remaining_compressed_memory_in_bytes.lock().unwrap();

        let size_in_bytes = size_in_bytes as isize;

        if size_in_bytes <= *remaining_compressed_memory_in_bytes {
            *remaining_compressed_memory_in_bytes -= size_in_bytes;
            true
        } else {
            false
        }
    }
}

/// Messages that can be sent between the components of [`StorageEngine`](super::StorageEngine).
pub(super) enum Message<T> {
    Data(T),
    Flush,
    Stop,
}

/// Channels used by the threads in the storage engine to communicate.
pub(super) struct Channels {
    /// Sender of [`IngestedDataBuffers`](IngestedDataBuffer) with data points from one or more
    /// time series from the [`StorageEngine`](super::StorageEngine) to the
    /// [`UncompressedDataManager`](super::UncompressedDataManager) where they are partitioned by
    /// tags into buffers of static length.
    pub(super) ingested_data_sender: Sender<Message<IngestedDataBuffer>>,
    /// Receiver of [`IngestedDataBuffers`](IngestedDataBuffer) with data points from one or more
    /// time series from the [`StorageEngine`](super::StorageEngine) to the
    /// [`UncompressedDataManager`](super::UncompressedDataManager) where they are partitioned by
    /// tags into buffers of static length.
    pub(super) ingested_data_receiver: Receiver<Message<IngestedDataBuffer>>,
    /// Sender of [`UncompressedDataBuffers`](UncompressedDataBuffer) with parts of a time series
    /// from the [`UncompressedDataManager`](super::UncompressedDataManager) to the
    /// [`UncompressedDataManager`](super::UncompressedDataManager) where they are compressed into
    /// compressed segments.
    pub(super) uncompressed_data_sender: Sender<Message<UncompressedDataBuffer>>,
    /// Receiver of [`UncompressedDataBuffers`](UncompressedDataBuffer) with parts of a time series
    /// from the [`UncompressedDataManager`](super::UncompressedDataManager) in the
    /// [`UncompressedDataManager`](super::UncompressedDataManager) where they are compressed into
    /// compressed segments.
    pub(super) uncompressed_data_receiver: Receiver<Message<UncompressedDataBuffer>>,
    /// Sender of [`CompressedSegmentBatches`](CompressedSegmentBatch) with compressed segments from
    /// the [`UncompressedDataManager`](super::UncompressedDataManager) to the
    /// [`CompressedDataManager`](super::CompressedDataManager) where they are written to a local
    /// data folder and later, possibly, a remote data folder.
    pub(super) compressed_data_sender: Sender<Message<CompressedSegmentBatch>>,
    /// Receiver of [`CompressedSegmentBatches`](CompressedSegmentBatch) with compressed segments
    /// from the [`UncompressedDataManager`](super::UncompressedDataManager) in the
    /// [`CompressedDataManager`](super::CompressedDataManager) where they are written to a local
    /// data folder and later, possibly, a remote data folder.
    pub(super) compressed_data_receiver: Receiver<Message<CompressedSegmentBatch>>,
    /// Sender of [`Results`](Result) from
    /// [`UncompressedDataManager`](super::UncompressedDataManager) or
    /// [`CompressedDataManager`](super::CompressedDataManager) to indicate that an asynchronous
    /// process has succeeded or failed to [`StorageEngine`](super::StorageEngine).
    pub(super) result_sender: Sender<Result<(), Box<dyn Error + Send + Sync>>>,
    /// Receiver of [`Results`](Result) from
    /// [`UncompressedDataManager`](super::UncompressedDataManager) or
    /// [`CompressedDataManager`](super::CompressedDataManager) to indicate that an asynchronous
    /// process has succeeded or failed to [`StorageEngine`](super::StorageEngine).
    pub(super) result_receiver: Receiver<Result<(), Box<dyn Error + Send + Sync>>>,
}

impl Channels {
    pub(super) fn new() -> Self {
        let (ingested_data_sender, ingested_data_receiver) = crossbeam_channel::unbounded();
        let (uncompressed_data_sender, uncompressed_data_receiver) = crossbeam_channel::unbounded();
        let (compressed_data_sender, compressed_data_receiver) = crossbeam_channel::unbounded();
        let (result_sender, result_receiver) = crossbeam_channel::unbounded();

        Self {
            ingested_data_sender,
            ingested_data_receiver,
            uncompressed_data_sender,
            uncompressed_data_receiver,
            compressed_data_sender,
            compressed_data_receiver,
            result_sender,
            result_receiver,
        }
    }
}

/// The different types of metrics that are collected in the storage engine.
pub enum MetricType {
    UsedIngestedMemory,
    UsedUncompressedMemory,
    UsedCompressedMemory,
    IngestedDataPoints,
    UsedDiskSpace,
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UsedIngestedMemory => write!(f, "used_ingested_memory"),
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
    /// Ring buffer consisting of a capped amount of microsecond precision timestamps.
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

    /// Add a new entry to the metric, where the timestamp is the current microseconds since the
    /// Unix epoch and the value is either set directly or based on the last value in the metric.
    pub(super) fn append(&mut self, value: isize, based_on_last: bool) {
        // unwrap() is safe since the Unix epoch is always earlier than now.
        let since_the_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let timestamp = since_the_epoch.as_micros() as Timestamp;

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
    pub(super) fn values(&mut self) -> &HeapRb<u32> {
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

    use modelardb_common::test;

    // Tests for MemoryPool.
    #[test]
    fn test_adjust_multivariate_memory_increase() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            test::INGESTED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_ingested_memory(test::COMPRESSED_SEGMENTS_SIZE as isize);

        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            (test::INGESTED_RESERVED_MEMORY_IN_BYTES + test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_multivariate_memory_decrease_above_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            test::INGESTED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_ingested_memory(-(test::COMPRESSED_SEGMENTS_SIZE as isize));

        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            (test::INGESTED_RESERVED_MEMORY_IN_BYTES - test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_multivariate_memory_decrease_below_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            test::INGESTED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_ingested_memory(-2 * test::INGESTED_RESERVED_MEMORY_IN_BYTES as isize);

        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            -(test::INGESTED_RESERVED_MEMORY_IN_BYTES as isize)
        );
    }

    #[test]
    fn test_reserve_available_multivariate_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            test::INGESTED_RESERVED_MEMORY_IN_BYTES as isize
        );

        // Blocks if memory cannot be reserved, thus forcing the test to never succeed.
        memory_pool.wait_for_ingested_memory(test::INGESTED_RESERVED_MEMORY_IN_BYTES);

        assert_eq!(memory_pool.remaining_ingested_memory_in_bytes(), 0);
    }

    #[test]
    fn test_adjust_uncompressed_memory_increase() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_uncompressed_memory(test::COMPRESSED_SEGMENTS_SIZE as isize);

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            (test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES + test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_uncompressed_memory_decrease_above_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_uncompressed_memory(-(test::COMPRESSED_SEGMENTS_SIZE as isize));

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            (test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES - test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_uncompressed_memory_decrease_below_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool
            .adjust_uncompressed_memory(-2 * test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize);

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            -(test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize)
        );
    }

    #[test]
    fn test_wait_for_available_uncompressed_memory_with_stop_if_false() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(memory_pool.wait_for_uncompressed_memory_until(
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            || false
        ));

        assert_eq!(memory_pool.remaining_uncompressed_memory_in_bytes(), 0);
    }

    #[test]
    fn test_wait_for_available_uncompressed_memory_with_stop_if_true() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(memory_pool.wait_for_uncompressed_memory_until(
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            || true
        ));

        assert_eq!(memory_pool.remaining_uncompressed_memory_in_bytes(), 0);
    }

    #[test]
    fn test_wait_for_unavailable_uncompressed_memory_with_stop_if_true() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(!memory_pool.wait_for_uncompressed_memory_until(
            2 * test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            || true
        ));

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );
    }

    #[test]
    fn test_adjust_compressed_memory_increase() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_compressed_memory(test::COMPRESSED_SEGMENTS_SIZE as isize);

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            (test::COMPRESSED_RESERVED_MEMORY_IN_BYTES + test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_compressed_memory_decrease_above_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_compressed_memory(-(test::COMPRESSED_SEGMENTS_SIZE as isize));

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            (test::COMPRESSED_RESERVED_MEMORY_IN_BYTES - test::COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_compressed_memory_decrease_below_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool
            .adjust_compressed_memory(-2 * test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize);

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            -(test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize)
        );
    }

    #[test]
    fn test_try_reserve_available_compressed_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(
            memory_pool.try_reserve_compressed_memory(test::COMPRESSED_RESERVED_MEMORY_IN_BYTES)
        );

        assert_eq!(memory_pool.remaining_compressed_memory_in_bytes(), 0);
    }

    #[test]
    fn test_try_reserve_unavailable_compressed_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(!memory_pool
            .try_reserve_compressed_memory(2 * test::COMPRESSED_RESERVED_MEMORY_IN_BYTES));

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );
    }

    fn create_memory_pool() -> MemoryPool {
        MemoryPool::new(
            test::INGESTED_RESERVED_MEMORY_IN_BYTES,
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        )
    }

    // Tests for Metric.
    #[test]
    fn test_append_to_metric() {
        let mut metric = Metric::new();

        metric.append(30, false);

        let since_the_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let timestamp = since_the_epoch.as_micros() as Timestamp;
        metric.append(30, false);

        // Check that the timestamp is no more than 10 microseconds after the expected timestamp to
        // account for the time it takes to execute the append function.
        assert!((metric.timestamps.pop_iter().last().unwrap() - timestamp) <= 10);
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
        assert_eq!(metric.timestamps.occupied_len(), 0);
        assert_eq!(metric.values.occupied_len(), 0);
    }
}
