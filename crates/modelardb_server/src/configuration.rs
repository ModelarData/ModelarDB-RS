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

use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use modelardb_common::errors::ModelarDbError;
use modelardb_common::types::ServerMode;
use tokio::sync::RwLock;

use crate::storage::StorageEngine;
use crate::ClusterMode;

/// Manages the system's configuration and provides functionality for updating the configuration.
#[derive(Clone)]
pub struct ConfigurationManager {
    /// Folder for storing metadata and Apache Parquet files on the local file system.
    pub(crate) local_data_folder: PathBuf,
    /// The mode of the cluster used to determine the behaviour when starting the server,
    /// creating tables, updating the remote object store, and querying.
    pub(crate) cluster_mode: ClusterMode,
    /// The mode of the server used to determine the behaviour when modifying the remote object
    /// store and querying.
    pub(crate) server_mode: ServerMode,
    /// Amount of memory to reserve for storing multivariate time series.
    multivariate_reserved_memory_in_bytes: usize,
    /// Amount of memory to reserve for storing uncompressed data buffers.
    uncompressed_reserved_memory_in_bytes: usize,
    /// Amount of memory to reserve for storing compressed data buffers.
    compressed_reserved_memory_in_bytes: usize,
    /// The number of bytes that are required before transferring a batch of data to the remote
    /// object store. If [`None`], data is not transferred based on batch size.
    transfer_batch_size_in_bytes: Option<usize>,
    /// The number of seconds between each transfer of data to the remote object store. If [`None`],
    /// data is not transferred based on time.
    transfer_time_in_seconds: Option<usize>,
    /// Number of threads to allocate for converting multivariate time series to univariate
    /// time series.
    pub(crate) ingestion_threads: usize,
    /// Number of threads to allocate for compressing univariate time series to segments.
    pub(crate) compression_threads: usize,
    /// Number of threads to allocate for writing segments to a local and/or remote data folder.
    pub(crate) writer_threads: usize,
}

impl ConfigurationManager {
    pub fn new(
        local_data_folder: &Path,
        cluster_mode: ClusterMode,
        server_mode: ServerMode,
    ) -> Self {
        let multivariate_reserved_memory_in_bytes =
            env::var("MODELARDBD_MULTIVARIATE_RESERVED_MEMORY_IN_BYTES")
                .map_or(512 * 1024 * 1024, |value| value.parse().unwrap());

        let uncompressed_reserved_memory_in_bytes =
            env::var("MODELARDBD_UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES")
                .map_or(512 * 1024 * 1024, |value| value.parse().unwrap());

        let compressed_reserved_memory_in_bytes =
            env::var("MODELARDBD_COMPRESSED_RESERVED_MEMORY_IN_BYTES")
                .map_or(512 * 1024 * 1024, |value| value.parse().unwrap());

        let transfer_batch_size_in_bytes = env::var("MODELARDBD_TRANSFER_BATCH_SIZE_IN_BYTES")
            .map_or(Some(64 * 1024 * 1024), |value| Some(value.parse().unwrap()));

        let transfer_time_in_seconds = env::var("MODELARDBD_TRANSFER_TIME_IN_SECONDS")
            .map_or(None, |value| Some(value.parse().unwrap()));

        Self {
            local_data_folder: local_data_folder.to_path_buf(),
            cluster_mode,
            server_mode,
            multivariate_reserved_memory_in_bytes,
            uncompressed_reserved_memory_in_bytes,
            compressed_reserved_memory_in_bytes,
            transfer_batch_size_in_bytes,
            transfer_time_in_seconds,
            // TODO: Add support for running multiple threads per component. The individual
            // components in the storage engine have not been validated with multiple threads, e.g.,
            // UncompressedDataManager may have race conditions finishing buffers if multiple
            // different data points are added by multiple different clients in parallel.
            ingestion_threads: 1,
            compression_threads: 1,
            writer_threads: 1,
        }
    }

    pub(crate) fn multivariate_reserved_memory_in_bytes(&self) -> usize {
        self.multivariate_reserved_memory_in_bytes
    }
    // TODO: Implement set_multivariate_reserved_memory_in_bytes().

    pub(crate) fn uncompressed_reserved_memory_in_bytes(&self) -> usize {
        self.uncompressed_reserved_memory_in_bytes
    }

    /// Set the new value and update the amount of memory for uncompressed data in the storage engine.
    pub(crate) async fn set_uncompressed_reserved_memory_in_bytes(
        &mut self,
        new_uncompressed_reserved_memory_in_bytes: usize,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<(), ModelarDbError> {
        // Since the storage engine only keeps track of the remaining reserved memory, calculate
        // how much the value should change.
        let value_change = new_uncompressed_reserved_memory_in_bytes as isize
            - self.uncompressed_reserved_memory_in_bytes as isize;

        storage_engine
            .write()
            .await
            .adjust_uncompressed_remaining_memory_in_bytes(value_change)
            .await
            .map_err(|error| ModelarDbError::ConfigurationError(error.to_string()))?;

        self.uncompressed_reserved_memory_in_bytes = new_uncompressed_reserved_memory_in_bytes;
        Ok(())
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

    pub(crate) fn transfer_batch_size_in_bytes(&self) -> Option<usize> {
        self.transfer_batch_size_in_bytes
    }

    /// Set the new value and update the transfer batch size in the storage engine. If the value
    /// was updated, return [`Ok`], otherwise return
    /// [`ConfigurationError`](ModelarDbError::ConfigurationError).
    pub(crate) async fn set_transfer_batch_size_in_bytes(
        &mut self,
        new_transfer_batch_size_in_bytes: Option<usize>,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<(), ModelarDbError> {
        storage_engine
            .write()
            .await
            .set_transfer_batch_size_in_bytes(new_transfer_batch_size_in_bytes)
            .await?;

        self.transfer_batch_size_in_bytes = new_transfer_batch_size_in_bytes;
        Ok(())
    }

    pub(crate) fn transfer_time_in_seconds(&self) -> Option<usize> {
        self.transfer_time_in_seconds
    }

    /// Set the new value and update the transfer time in the storage engine. If the value was updated,
    /// return [`Ok`], otherwise return [`ConfigurationError`](ModelarDbError::ConfigurationError).
    pub(crate) async fn set_transfer_time_in_seconds(
        &mut self,
        new_transfer_time_in_seconds: Option<usize>,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<(), ModelarDbError> {
        storage_engine
            .write()
            .await
            .set_transfer_time_in_seconds(new_transfer_time_in_seconds)
            .await?;

        self.transfer_time_in_seconds = new_transfer_time_in_seconds;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;
    use std::sync::Arc;

    use arrow_flight::flight_service_client::FlightServiceClient;
    use modelardb_common::metadata;
    use modelardb_common::types::ServerMode;
    use object_store::local::LocalFileSystem;
    use tokio::runtime::Runtime;
    use tokio::sync::RwLock;
    use tonic::transport::Channel;
    use uuid::Uuid;

    use crate::manager::Manager;
    use crate::storage::StorageEngine;

    // Tests for ConfigurationManager.
    #[tokio::test]
    async fn test_set_uncompressed_reserved_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(temp_dir.path()).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .uncompressed_reserved_memory_in_bytes(),
            512 * 1024 * 1024
        );

        configuration_manager
            .write()
            .await
            .set_uncompressed_reserved_memory_in_bytes(1024, storage_engine)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .uncompressed_reserved_memory_in_bytes(),
            1024
        );
    }

    #[tokio::test]
    async fn test_set_compressed_reserved_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(temp_dir.path()).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .compressed_reserved_memory_in_bytes(),
            512 * 1024 * 1024
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

    #[tokio::test]
    async fn test_set_transfer_batch_size_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(temp_dir.path()).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .transfer_batch_size_in_bytes(),
            Some(64 * 1024 * 1024)
        );

        configuration_manager
            .write()
            .await
            .set_transfer_batch_size_in_bytes(Some(1024), storage_engine)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .transfer_batch_size_in_bytes(),
            Some(1024)
        );
    }

    #[tokio::test]
    async fn test_set_transfer_time_in_seconds() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(temp_dir.path()).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .transfer_time_in_seconds(),
            None
        );

        configuration_manager
            .write()
            .await
            .set_transfer_time_in_seconds(Some(60), storage_engine)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .transfer_time_in_seconds(),
            Some(60)
        );
    }

    /// Create a [`StorageEngine`] and a [`ConfigurationManager`].
    async fn create_components(
        path: &Path,
    ) -> (
        Arc<RwLock<StorageEngine>>,
        Arc<RwLock<ConfigurationManager>>,
    ) {
        let metadata_manager = metadata::try_new_sqlite_table_metadata_manager(path)
            .await
            .unwrap();

        let channel = Channel::builder("grpc://server:9999".parse().unwrap()).connect_lazy();
        let lazy_flight_client = FlightServiceClient::new(channel);

        let manager = Manager::new(
            Arc::new(RwLock::new(lazy_flight_client)),
            Uuid::new_v4().to_string(),
            None,
        );

        let configuration_manager = Arc::new(RwLock::new(ConfigurationManager::new(
            path,
            ClusterMode::MultiNode(manager),
            ServerMode::Edge,
        )));

        let target_dir = tempfile::tempdir().unwrap();
        let target_fs = LocalFileSystem::new_with_prefix(target_dir.path()).unwrap();

        let storage_engine = Arc::new(RwLock::new(
            StorageEngine::try_new(
                Arc::new(Runtime::new().unwrap()),
                path.to_owned(),
                Some(Arc::new(target_fs)),
                &configuration_manager,
                Arc::new(metadata_manager),
            )
            .await
            .unwrap(),
        ));

        (storage_engine, configuration_manager)
    }
}
