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

//! Management of the system's configuration. The configuration consists of the server mode and
//! the amount of reserved memory for uncompressed and compressed data.

use std::sync::Arc;

use modelardb_common::errors::ModelarDbError;
use modelardb_common::types::{ClusterMode, ServerMode};
use tokio::sync::RwLock;

use crate::storage::StorageEngine;

/// Manages the system's configuration and provides functionality for updating the configuration.
#[derive(Clone)]
pub struct ConfigurationManager {
    /// The mode of the cluster used to determine the behaviour when starting the server,
    /// creating tables, updating the remote object store, and querying.
    pub(crate) cluster_mode: ClusterMode,
    /// The mode of the server used to determine the behaviour when modifying the remote object
    /// store and querying.
    pub(crate) server_mode: ServerMode,
    /// Amount of memory to reserve for storing uncompressed data buffers.
    uncompressed_reserved_memory_in_bytes: usize,
    /// Amount of memory to reserve for storing compressed data buffers.
    compressed_reserved_memory_in_bytes: usize,
    /// Number of threads to allocate for converting multivariate time series to univariate time
    /// series.
    pub(crate) ingestion_threads: usize,
    /// Number of threads to allocate for compressing univariate time series to segments.
    pub(crate) compression_threads: usize,
    /// Number of threads to allocate for writing segments to a local and/or remote data folder.
    pub(crate) writer_threads: usize,
}

impl ConfigurationManager {
    pub fn new(cluster_mode: ClusterMode, server_mode: ServerMode) -> Self {
        Self {
            cluster_mode,
            server_mode,
            uncompressed_reserved_memory_in_bytes: 512 * 1024 * 1024, // 512 MiB
            compressed_reserved_memory_in_bytes: 512 * 1024 * 1024,   // 512 MiB
            // TODO: Add support for running multiple threads per component. The individual
            // components in the storage engine have not been validated with multiple threads, e.g.,
            // UncompressedDataManager may have race conditions finishing buffers if multiple
            // different data points are added by multiple different clients in parallel.
            ingestion_threads: 1,
            compression_threads: 1,
            writer_threads: 1,
        }
    }

    pub(crate) fn uncompressed_reserved_memory_in_bytes(&self) -> usize {
        self.uncompressed_reserved_memory_in_bytes
    }

    /// Set the new value and update the amount of memory for uncompressed data in the storage engine.
    pub(crate) async fn set_uncompressed_reserved_memory_in_bytes(
        &mut self,
        new_uncompressed_reserved_memory_in_bytes: usize,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) {
        // Since the storage engine only keeps track of the remaining reserved memory, calculate
        // how much the value should change.
        let value_change = new_uncompressed_reserved_memory_in_bytes as isize
            - self.uncompressed_reserved_memory_in_bytes as isize;

        storage_engine
            .write()
            .await
            .adjust_uncompressed_remaining_memory_in_bytes(value_change)
            .await;

        self.uncompressed_reserved_memory_in_bytes = new_uncompressed_reserved_memory_in_bytes;
    }

    pub(crate) fn compressed_reserved_memory_in_bytes(&self) -> usize {
        self.compressed_reserved_memory_in_bytes
    }

    /// Set the new value and update the amount of memory for compressed data in the storage engine.
    /// If the value was updated, return [`Ok`], otherwise return
    /// [`ConfigurationError`](ModelarDbError::ConfigurationError).
    pub(crate) async fn set_compressed_reserved_memory_in_bytes(
        &mut self,
        new_compressed_reserved_memory_in_bytes: usize,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<(), ModelarDbError> {
        // Since the storage engine only keeps track of the remaining reserved memory, calculate
        // how much the value should change.
        let value_change = new_compressed_reserved_memory_in_bytes as isize
            - self.compressed_reserved_memory_in_bytes as isize;

        storage_engine
            .write()
            .await
            .adjust_compressed_remaining_memory_in_bytes(value_change)
            .await
            .map_err(|error| ModelarDbError::ConfigurationError(error.to_string()))?;

        self.compressed_reserved_memory_in_bytes = new_compressed_reserved_memory_in_bytes;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::common_test;

    // Tests for ConfigurationManager.
    #[tokio::test]
    async fn test_set_uncompressed_reserved_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = common_test::test_context(temp_dir.path()).await;

        let configuration_manager = context.configuration_manager.clone();
        let storage_engine = context.storage_engine.clone();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .uncompressed_reserved_memory_in_bytes,
            common_test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES
        );

        configuration_manager
            .write()
            .await
            .set_uncompressed_reserved_memory_in_bytes(1024, storage_engine)
            .await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .uncompressed_reserved_memory_in_bytes,
            1024
        );
    }

    #[tokio::test]
    async fn test_set_compressed_reserved_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = common_test::test_context(temp_dir.path()).await;

        let configuration_manager = context.configuration_manager.clone();
        let storage_engine = context.storage_engine.clone();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .compressed_reserved_memory_in_bytes(),
            common_test::COMPRESSED_RESERVED_MEMORY_IN_BYTES
        );

        configuration_manager
            .write()
            .await
            .set_compressed_reserved_memory_in_bytes(1024, storage_engine)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .compressed_reserved_memory_in_bytes(),
            1024
        );
    }
}
