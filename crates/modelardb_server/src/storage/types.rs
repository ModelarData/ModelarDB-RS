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

use std::sync::RwLock;

/// Resizeable pool of memory for tracking and limiting the amount of memory used by the
/// [`StorageEngine`](crate::storage::StorageEngine).
pub(super) struct MemoryPool {
    /// How many bytes of memory that are left for storing
    /// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) containing multivariate time
    /// series with metadata and
    /// [`UncompressedDataBuffers`](crate::storage::uncompressed_data_buffer::UncompressedDataBuffer)
    /// containing univariate time series without metadata.
    remaining_uncompressed_memory_in_bytes: RwLock<isize>,
    /// How many bytes of memory that are left for storing
    /// [`CompressedDataBuffers`](crate::storage::compressed_data_buffer::CompressedDataBuffer). A
    /// signed integer is used since compressed data is inserted and then the remaining bytes are
    /// checked. This means that the remaining bytes can briefly be negative until compressed data
    /// is saved to disk and if the amount of memory for uncompressed data is reduced.
    remaining_compressed_memory_in_bytes: RwLock<isize>,
}

/// The pool of memory the
/// [`UncompressedDataManager`](crate::storage::uncompressed_data_manager::UncompressedDataManager)
/// can use for uncompressed data and the
/// [`CompressedDataManager`](crate::storage::CompressedDataManager) can use for compressed data.
impl MemoryPool {
    /// Create a new [`MemoryPool`] with at most [`u64::MAX`] bytes of memory for uncompressed data
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

    /// Change the amount of memory available for storing
    /// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) containing multivariate time
    /// series with metadata and
    /// [`UncompressedDataBuffers`](crate::storage::uncompressed_data_buffer::UncompressedDataBuffer)
    /// containing univariate time series without metadata by `size_in_bytes`.
    pub(super) fn update_uncompressed_memory(&self, size_in_bytes: isize) {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_uncompressed_memory_in_bytes.write().unwrap() += size_in_bytes
    }

    /// Return the amount of memory available for storing
    /// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) containing multivariate time
    /// series with metadata and
    /// [`UncompressedDataBuffers`](crate::storage::uncompressed_data_buffer::UncompressedDataBuffer)
    /// containing univariate time series without metadata in bytes.
    pub(super) fn remaining_uncompressed_memory_in_bytes(&self) -> isize {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_uncompressed_memory_in_bytes.read().unwrap()
    }

    /// Try to reserve `size_in_bytes` bytes of memory for storing
    /// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) containing multivariate time
    /// series with metadata and
    /// [`UncompressedDataBuffers`](crate::storage::uncompressed_data_buffer::UncompressedDataBuffer)
    /// containing univariate time series without metadata. Returns [`true`] if the reservation
    /// succeeds and [`false`] otherwise.
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

    /// Free `size_in_bytes` bytes of memory for storing
    /// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) containing multivariate time
    /// series with metadata and
    /// [`UncompressedDataBuffers`](crate::storage::uncompressed_data_buffer::UncompressedDataBuffer)
    /// containing univariate time series without metadata. Returns [`true`] if the reservation
    /// succeeds and [`false`] otherwise.
    pub(super) fn free_uncompressed_memory(&self, size_in_bytes: usize) {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_uncompressed_memory_in_bytes.write().unwrap() += size_in_bytes as isize;
    }

    /// Change the amount of memory available for storing
    /// [`CompressedDataBuffers`](crate::storage::compressed_data_buffer::CompressedDataBuffer) by
    /// `size_in_bytes`.
    pub(super) fn update_compressed_memory(&self, size_in_bytes: isize) {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_compressed_memory_in_bytes.write().unwrap() += size_in_bytes;
    }

    /// Return the amount of memory available for storing
    /// [`CompressedDataBuffers`](crate::storage::compressed_data_buffer::CompressedDataBuffer) in
    /// bytes.
    pub(super) fn remaining_compressed_memory_in_bytes(&self) -> isize {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_compressed_memory_in_bytes.read().unwrap()
    }

    /// Try to reserve `size_in_bytes` bytes of memory for storing
    /// [`CompressedDataBuffers`](crate::storage::compressed_data_buffer::CompressedDataBuffer).
    /// Returns [`true`] if the reservation succeeds and [`false`] otherwise.
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

    /// Free `size_in_bytes` bytes of memory for storing
    /// [`CompressedDataBuffers`](crate::storage::compressed_data_buffer::CompressedDataBuffer).
    pub(super) fn free_compressed_memory(&self, size_in_bytes: usize) {
        // unwrap() is safe as write() only returns an error if the lock is poisoned.
        *self.remaining_compressed_memory_in_bytes.write().unwrap() += size_in_bytes as isize;
    }
}
