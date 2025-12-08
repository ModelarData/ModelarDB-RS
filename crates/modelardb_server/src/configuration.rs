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
use std::sync::Arc;

use modelardb_storage::data_folder::DataFolder;
use modelardb_types::flight::protocol;
use object_store::path::Path;
use object_store::{Error, ObjectStore, PutPayload};
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::ClusterMode;
use crate::error::{ModelarDbServerError, Result};
use crate::storage::StorageEngine;

/// Manages the system's configuration and provides functionality for updating the configuration.
#[derive(Clone, Serialize, Deserialize)]
pub struct ConfigurationManager {
    /// The mode of the cluster used to determine the behaviour when starting the server,
    /// creating tables, updating the remote object store, and querying.
    #[serde(skip)]
    pub(crate) cluster_mode: ClusterMode,
    /// Amount of memory to reserve for storing multivariate time series.
    multivariate_reserved_memory_in_bytes: u64,
    /// Amount of memory to reserve for storing uncompressed data buffers.
    uncompressed_reserved_memory_in_bytes: u64,
    /// Amount of memory to reserve for storing compressed data buffers.
    compressed_reserved_memory_in_bytes: u64,
    /// The number of bytes that are required before transferring a batch of data to the remote
    /// object store. If [`None`], data is not transferred based on batch size.
    transfer_batch_size_in_bytes: Option<u64>,
    /// The number of seconds between each transfer of data to the remote object store. If [`None`],
    /// data is not transferred based on time.
    transfer_time_in_seconds: Option<u64>,
    /// Number of threads to allocate for converting multivariate time series to univariate
    /// time series.
    pub(crate) ingestion_threads: u8,
    /// Number of threads to allocate for compressing univariate time series to segments.
    pub(crate) compression_threads: u8,
    /// Number of threads to allocate for writing segments to a local and/or remote data folder.
    pub(crate) writer_threads: u8,
}

impl ConfigurationManager {
    /// Create a new [`ConfigurationManager`] using the `modelardb.toml` configuration file in the
    /// local data folder. If the file does not exist, a configuration file is created with the
    /// values from the environment variables or default values. If the configuration file could
    /// not be read or created, [`ModelarDbServerError`] is returned.
    pub async fn try_new(local_data_folder: DataFolder, cluster_mode: ClusterMode) -> Result<Self> {
        // Check if there is a configuration file in the local data folder.
        let object_store = local_data_folder.object_store();
        let conf_file_path = &Path::from("modelardb.toml");
        let maybe_conf_file = object_store.get(conf_file_path).await;

        match maybe_conf_file {
            Ok(conf_file) => {
                // If the configuration file exists, load the configuration from the file.
                let bytes = conf_file.bytes().await?;
                let mut configuration_from_file = toml::from_slice::<Self>(&bytes)?;

                // The cluster mode is always ClusterMode::SingleNode when loading the configuration
                // from the file.
                configuration_from_file.cluster_mode = cluster_mode;

                Ok(configuration_from_file)
            }
            Err(error) => match error {
                Error::NotFound { .. } => {
                    // If the configuration file does not exist, create one with the configuration
                    // from the environment variables and default.
                    let multivariate_reserved_memory_in_bytes =
                        env::var("MODELARDBD_MULTIVARIATE_RESERVED_MEMORY_IN_BYTES")
                            .map_or(512 * 1024 * 1024, |value| value.parse().unwrap());

                    let uncompressed_reserved_memory_in_bytes =
                        env::var("MODELARDBD_UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES")
                            .map_or(512 * 1024 * 1024, |value| value.parse().unwrap());

                    let compressed_reserved_memory_in_bytes =
                        env::var("MODELARDBD_COMPRESSED_RESERVED_MEMORY_IN_BYTES")
                            .map_or(512 * 1024 * 1024, |value| value.parse().unwrap());

                    let transfer_batch_size_in_bytes =
                        env::var("MODELARDBD_TRANSFER_BATCH_SIZE_IN_BYTES")
                            .map_or(Some(64 * 1024 * 1024), |value| Some(value.parse().unwrap()));

                    let transfer_time_in_seconds = env::var("MODELARDBD_TRANSFER_TIME_IN_SECONDS")
                        .map_or(None, |value| Some(value.parse().unwrap()));

                    let configuration_manager = Self {
                        cluster_mode,
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
                    };

                    // Create the TOML file.
                    let toml = toml::to_string(&configuration_manager)?;
                    object_store
                        .put(conf_file_path, PutPayload::from(toml.into_bytes()))
                        .await?;

                    Ok(configuration_manager)
                }
                _ => Err(ModelarDbServerError::InvalidState(format!(
                    "Configuration file '{conf_file_path}' cannot be read."
                ))),
            },
        }
    }

    pub(crate) fn multivariate_reserved_memory_in_bytes(&self) -> u64 {
        self.multivariate_reserved_memory_in_bytes
    }

    /// Set the new value and update the amount of memory for multivariate data in the storage engine.
    pub(crate) async fn set_multivariate_reserved_memory_in_bytes(
        &mut self,
        new_multivariate_reserved_memory_in_bytes: u64,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) {
        // Since the storage engine only keeps track of the remaining reserved memory, calculate
        // how much the value should change.
        let value_change = new_multivariate_reserved_memory_in_bytes as i64
            - self.multivariate_reserved_memory_in_bytes as i64;

        storage_engine
            .write()
            .await
            .adjust_multivariate_remaining_memory_in_bytes(value_change)
            .await;

        self.multivariate_reserved_memory_in_bytes = new_multivariate_reserved_memory_in_bytes;
    }

    pub(crate) fn uncompressed_reserved_memory_in_bytes(&self) -> u64 {
        self.uncompressed_reserved_memory_in_bytes
    }

    /// Set the new value and update the amount of memory for uncompressed data in the storage
    /// engine. Returns [`ModelarDbServerError`](crate::error::ModelarDbServerError) if the memory
    /// cannot be updated because a buffer cannot be spilled.
    pub(crate) async fn set_uncompressed_reserved_memory_in_bytes(
        &mut self,
        new_uncompressed_reserved_memory_in_bytes: u64,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<()> {
        // Since the storage engine only keeps track of the remaining reserved memory, calculate
        // how much the value should change.
        let value_change = new_uncompressed_reserved_memory_in_bytes as i64
            - self.uncompressed_reserved_memory_in_bytes as i64;

        storage_engine
            .write()
            .await
            .adjust_uncompressed_remaining_memory_in_bytes(value_change)
            .await?;

        self.uncompressed_reserved_memory_in_bytes = new_uncompressed_reserved_memory_in_bytes;

        Ok(())
    }

    pub(crate) fn compressed_reserved_memory_in_bytes(&self) -> u64 {
        self.compressed_reserved_memory_in_bytes
    }

    /// Set the new value and update the amount of memory for compressed data in the storage engine.
    /// If the value was updated, return [`Ok`], otherwise return
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    pub(crate) async fn set_compressed_reserved_memory_in_bytes(
        &mut self,
        new_compressed_reserved_memory_in_bytes: u64,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<()> {
        // Since the storage engine only keeps track of the remaining reserved memory, calculate
        // how much the value should change.
        let value_change = new_compressed_reserved_memory_in_bytes as i64
            - self.compressed_reserved_memory_in_bytes as i64;

        storage_engine
            .write()
            .await
            .adjust_compressed_remaining_memory_in_bytes(value_change)
            .await?;

        self.compressed_reserved_memory_in_bytes = new_compressed_reserved_memory_in_bytes;

        Ok(())
    }

    pub(crate) fn transfer_batch_size_in_bytes(&self) -> Option<u64> {
        self.transfer_batch_size_in_bytes
    }

    /// Set the new value and update the transfer batch size in the storage engine. If the value was
    /// updated, return [`Ok`], otherwise return
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    pub(crate) async fn set_transfer_batch_size_in_bytes(
        &mut self,
        new_transfer_batch_size_in_bytes: Option<u64>,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<()> {
        storage_engine
            .write()
            .await
            .set_transfer_batch_size_in_bytes(new_transfer_batch_size_in_bytes)
            .await?;

        self.transfer_batch_size_in_bytes = new_transfer_batch_size_in_bytes;

        Ok(())
    }

    pub(crate) fn transfer_time_in_seconds(&self) -> Option<u64> {
        self.transfer_time_in_seconds
    }

    /// Set the new value and update the transfer time in the storage engine. If the value was
    /// updated, return [`Ok`], otherwise return
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    pub(crate) async fn set_transfer_time_in_seconds(
        &mut self,
        new_transfer_time_in_seconds: Option<u64>,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<()> {
        storage_engine
            .write()
            .await
            .set_transfer_time_in_seconds(new_transfer_time_in_seconds)
            .await?;

        self.transfer_time_in_seconds = new_transfer_time_in_seconds;

        Ok(())
    }

    /// Encode the current configuration into a [`Configuration`](protocol::Configuration)
    /// protobuf message and serialize it.
    pub(crate) fn encode_and_serialize(&self) -> Vec<u8> {
        let configuration = protocol::Configuration {
            multivariate_reserved_memory_in_bytes: self.multivariate_reserved_memory_in_bytes,
            uncompressed_reserved_memory_in_bytes: self.uncompressed_reserved_memory_in_bytes,
            compressed_reserved_memory_in_bytes: self.compressed_reserved_memory_in_bytes,
            transfer_batch_size_in_bytes: self.transfer_batch_size_in_bytes,
            transfer_time_in_seconds: self.transfer_time_in_seconds,
            ingestion_threads: self.ingestion_threads as u32,
            compression_threads: self.compression_threads as u32,
            writer_threads: self.writer_threads as u32,
        };

        configuration.encode_to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use modelardb_storage::data_folder::DataFolder;
    use tempfile::TempDir;
    use tokio::sync::RwLock;
    use uuid::Uuid;

    use crate::data_folders::DataFolders;
    use crate::manager::Manager;
    use crate::storage::StorageEngine;

    // Tests for ConfigurationManager.
    #[tokio::test]
    async fn test_set_multivariate_reserved_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .multivariate_reserved_memory_in_bytes(),
            512 * 1024 * 1024
        );

        let new_value = 1024;
        configuration_manager
            .write()
            .await
            .set_multivariate_reserved_memory_in_bytes(new_value, storage_engine)
            .await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .multivariate_reserved_memory_in_bytes(),
            new_value
        );
    }

    #[tokio::test]
    async fn test_set_uncompressed_reserved_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .uncompressed_reserved_memory_in_bytes(),
            512 * 1024 * 1024
        );

        let new_value = 1024;
        configuration_manager
            .write()
            .await
            .set_uncompressed_reserved_memory_in_bytes(new_value, storage_engine)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .uncompressed_reserved_memory_in_bytes(),
            new_value
        );
    }

    #[tokio::test]
    async fn test_set_compressed_reserved_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .compressed_reserved_memory_in_bytes(),
            512 * 1024 * 1024
        );

        let new_value = 1024;
        configuration_manager
            .write()
            .await
            .set_compressed_reserved_memory_in_bytes(new_value, storage_engine)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .compressed_reserved_memory_in_bytes(),
            new_value
        );
    }

    #[tokio::test]
    async fn test_set_transfer_batch_size_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .transfer_batch_size_in_bytes(),
            Some(64 * 1024 * 1024)
        );

        let new_value = Some(1024);
        configuration_manager
            .write()
            .await
            .set_transfer_batch_size_in_bytes(new_value, storage_engine)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .transfer_batch_size_in_bytes(),
            new_value
        );
    }

    #[tokio::test]
    async fn test_set_transfer_time_in_seconds() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .transfer_time_in_seconds(),
            None
        );

        let new_value = Some(60);
        configuration_manager
            .write()
            .await
            .set_transfer_time_in_seconds(new_value, storage_engine)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .transfer_time_in_seconds(),
            new_value
        );
    }

    /// Create a [`StorageEngine`] and a [`ConfigurationManager`].
    async fn create_components(
        temp_dir: &TempDir,
    ) -> (
        Arc<RwLock<StorageEngine>>,
        Arc<RwLock<ConfigurationManager>>,
    ) {
        let local_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::open_local_url(local_url).await.unwrap();

        let target_dir = tempfile::tempdir().unwrap();
        let target_url = target_dir.path().to_str().unwrap();
        let remote_data_folder = DataFolder::open_local_url(target_url).await.unwrap();

        let data_folders = DataFolders::new(
            local_data_folder.clone(),
            Some(remote_data_folder),
            local_data_folder.clone(),
        );

        let manager = Manager::new(Uuid::new_v4().to_string());

        let configuration_manager = Arc::new(RwLock::new(
            ConfigurationManager::try_new(local_data_folder, ClusterMode::MultiNode(manager))
                .await
                .unwrap(),
        ));

        let storage_engine = Arc::new(RwLock::new(
            StorageEngine::try_new(data_folders, &configuration_manager)
                .await
                .unwrap(),
        ));

        (storage_engine, configuration_manager)
    }
}
