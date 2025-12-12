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

const CONFIGURATION_FILE_NAME: &str = "modelardbd.toml";

/// The system's configuration. The configuration can be serialized into a [`CONFIGURATION_FILE_NAME`]
/// configuration file and deserialized from it. Accessing and modifying the configuration should
/// only be done through the [`ConfigurationManager`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Configuration {
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

impl Configuration {
    /// If the corresponding environment variable is set, update the configuration with the value
    /// from the environment variable. If the value could not be parsed, return
    /// [`ModelarDbServerError`].
    fn update_from_env(&mut self) -> Result<()> {
        if let Ok(value) = env::var("MODELARDBD_MULTIVARIATE_RESERVED_MEMORY_IN_BYTES") {
            self.multivariate_reserved_memory_in_bytes = value.parse()?;
        };

        if let Ok(value) = env::var("MODELARDBD_UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES") {
            self.uncompressed_reserved_memory_in_bytes = value.parse()?;
        };

        if let Ok(value) = env::var("MODELARDBD_COMPRESSED_RESERVED_MEMORY_IN_BYTES") {
            self.compressed_reserved_memory_in_bytes = value.parse()?;
        };

        if let Ok(value) = env::var("MODELARDBD_TRANSFER_BATCH_SIZE_IN_BYTES") {
            self.transfer_batch_size_in_bytes = Some(value.parse()?);
        }

        if let Ok(value) = env::var("MODELARDBD_TRANSFER_TIME_IN_SECONDS") {
            self.transfer_time_in_seconds = Some(value.parse()?);
        }

        Ok(())
    }

    /// Validate the fields in the configuration and return [`Ok`] if they are valid. If the
    /// configuration is invalid, return [`ModelarDbServerError`].
    fn validate(&self) -> Result<()> {
        // TODO: Add support for running multiple threads per component. The individual
        //       components in the storage engine have not been validated with multiple threads, e.g.,
        //       UncompressedDataManager may have race conditions finishing buffers if multiple
        //       different data points are added by multiple different clients in parallel.
        if self.ingestion_threads != 1 || self.compression_threads != 1 || self.writer_threads != 1
        {
            return Err(ModelarDbServerError::InvalidState(
                "Only one thread per component is currently supported.".to_string(),
            ));
        };

        Ok(())
    }

    /// Save the configuration to the configuration file at the root of `local_data_folder`. If the
    /// file does not exist, it is created. If the configuration file could not be updated or
    /// created, return [`ModelarDbServerError`].
    async fn save_to_toml(&self, local_data_folder: &DataFolder) -> Result<()> {
        let toml = toml::to_string(self)?;
        let object_store = local_data_folder.object_store();

        object_store
            .put(
                &Path::from(CONFIGURATION_FILE_NAME),
                PutPayload::from(toml.into_bytes()),
            )
            .await?;

        Ok(())
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            multivariate_reserved_memory_in_bytes: 512 * 1024 * 1024,
            uncompressed_reserved_memory_in_bytes: 512 * 1024 * 1024,
            compressed_reserved_memory_in_bytes: 512 * 1024 * 1024,
            transfer_batch_size_in_bytes: Some(64 * 1024 * 1024),
            transfer_time_in_seconds: None,
            ingestion_threads: 1,
            compression_threads: 1,
            writer_threads: 1,
        }
    }
}

/// Manages the system's configuration and provides functionality for updating the configuration.
#[derive(Clone)]
pub struct ConfigurationManager {
    /// The mode of the cluster used to determine the behaviour when starting the server,
    /// creating tables, updating the remote object store, and querying.
    cluster_mode: ClusterMode,
    /// The local data folder that stores the configuration file at the root.
    local_data_folder: DataFolder,
    /// The configuration of the system. This is stored in a separate type to allow for easier
    /// serialization and deserialization.
    configuration: Configuration,
}

impl ConfigurationManager {
    /// Create a new [`ConfigurationManager`] using the [`CONFIGURATION_FILE_NAME`] configuration
    /// file in the local data folder. If the file does not exist, a configuration file is created
    /// with the default values. Note that the configuration file and default values are overwritten
    /// if the corresponding environment variables are set. If the configuration file could not be
    /// read or created, [`ModelarDbServerError`] is returned.
    pub async fn try_new(local_data_folder: DataFolder, cluster_mode: ClusterMode) -> Result<Self> {
        // Check if there is a configuration file in the local data folder.
        let object_store = local_data_folder.object_store();
        let maybe_conf_file = object_store.get(&Path::from(CONFIGURATION_FILE_NAME)).await;

        let mut configuration = match maybe_conf_file {
            Ok(conf_file) => {
                // If the configuration file exists, load the configuration from the file.
                let bytes = conf_file.bytes().await?;
                toml::from_slice::<Configuration>(&bytes)?
            }
            Err(error) => match error {
                Error::NotFound { .. } => {
                    // If the configuration file does not exist, create one with the default values.
                    Configuration::default()
                }
                error => {
                    return Err(ModelarDbServerError::InvalidState(format!(
                        "Configuration file '{CONFIGURATION_FILE_NAME}' could not be read: {error}"
                    )));
                }
            },
        };

        configuration.update_from_env()?;
        configuration.validate()?;
        configuration.save_to_toml(&local_data_folder).await?;

        Ok(Self {
            cluster_mode,
            local_data_folder,
            configuration,
        })
    }

    pub(crate) fn cluster_mode(&self) -> &ClusterMode {
        &self.cluster_mode
    }

    pub(crate) fn multivariate_reserved_memory_in_bytes(&self) -> u64 {
        self.configuration.multivariate_reserved_memory_in_bytes
    }

    /// Set the new value and update the amount of memory for multivariate data in the storage
    /// engine. If the new configuration could not be saved to the configuration file, return
    /// [`ModelarDbServerError`].
    pub(crate) async fn set_multivariate_reserved_memory_in_bytes(
        &mut self,
        new_multivariate_reserved_memory_in_bytes: u64,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<()> {
        // Since the storage engine only keeps track of the remaining reserved memory, calculate
        // how much the value should change.
        let value_change = new_multivariate_reserved_memory_in_bytes as i64
            - self.configuration.multivariate_reserved_memory_in_bytes as i64;

        storage_engine
            .write()
            .await
            .adjust_multivariate_remaining_memory_in_bytes(value_change)
            .await;

        self.configuration.multivariate_reserved_memory_in_bytes =
            new_multivariate_reserved_memory_in_bytes;

        self.configuration
            .save_to_toml(&self.local_data_folder)
            .await
    }

    pub(crate) fn uncompressed_reserved_memory_in_bytes(&self) -> u64 {
        self.configuration.uncompressed_reserved_memory_in_bytes
    }

    /// Set the new value and update the amount of memory for uncompressed data in the storage
    /// engine. If the memory could not be updated because a buffer could not be spilled or if the
    /// new configuration could not be saved to the configuration file, return
    /// [`ModelarDbServerError`].
    pub(crate) async fn set_uncompressed_reserved_memory_in_bytes(
        &mut self,
        new_uncompressed_reserved_memory_in_bytes: u64,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<()> {
        // Since the storage engine only keeps track of the remaining reserved memory, calculate
        // how much the value should change.
        let value_change = new_uncompressed_reserved_memory_in_bytes as i64
            - self.configuration.uncompressed_reserved_memory_in_bytes as i64;

        storage_engine
            .write()
            .await
            .adjust_uncompressed_remaining_memory_in_bytes(value_change)
            .await?;

        self.configuration.uncompressed_reserved_memory_in_bytes =
            new_uncompressed_reserved_memory_in_bytes;

        self.configuration
            .save_to_toml(&self.local_data_folder)
            .await
    }

    pub(crate) fn compressed_reserved_memory_in_bytes(&self) -> u64 {
        self.configuration.compressed_reserved_memory_in_bytes
    }

    /// Set the new value and update the amount of memory for compressed data in the storage engine.
    /// If the memory could not be updated because a buffer could not be saved to disk or if the new
    /// configuration could not be saved to the configuration file, return [`ModelarDbServerError`].
    pub(crate) async fn set_compressed_reserved_memory_in_bytes(
        &mut self,
        new_compressed_reserved_memory_in_bytes: u64,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<()> {
        // Since the storage engine only keeps track of the remaining reserved memory, calculate
        // how much the value should change.
        let value_change = new_compressed_reserved_memory_in_bytes as i64
            - self.configuration.compressed_reserved_memory_in_bytes as i64;

        storage_engine
            .write()
            .await
            .adjust_compressed_remaining_memory_in_bytes(value_change)
            .await?;

        self.configuration.compressed_reserved_memory_in_bytes =
            new_compressed_reserved_memory_in_bytes;

        self.configuration
            .save_to_toml(&self.local_data_folder)
            .await
    }

    pub(crate) fn transfer_batch_size_in_bytes(&self) -> Option<u64> {
        self.configuration.transfer_batch_size_in_bytes
    }

    /// Set the new value and update the transfer batch size in the storage engine. If the batch
    /// size could not be updated or if the new configuration could not be saved to the
    /// configuration file, return [`ModelarDbServerError`].
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

        self.configuration.transfer_batch_size_in_bytes = new_transfer_batch_size_in_bytes;

        self.configuration
            .save_to_toml(&self.local_data_folder)
            .await
    }

    pub(crate) fn transfer_time_in_seconds(&self) -> Option<u64> {
        self.configuration.transfer_time_in_seconds
    }

    /// Set the new value and update the transfer time in the storage engine. If the transfer time
    /// could not be updated or if the new configuration could not be saved to the configuration
    /// file, return [`ModelarDbServerError`].
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

        self.configuration.transfer_time_in_seconds = new_transfer_time_in_seconds;

        self.configuration
            .save_to_toml(&self.local_data_folder)
            .await
    }

    pub(crate) fn ingestion_threads(&self) -> u8 {
        self.configuration.ingestion_threads
    }

    pub(crate) fn compression_threads(&self) -> u8 {
        self.configuration.compression_threads
    }

    pub(crate) fn writer_threads(&self) -> u8 {
        self.configuration.writer_threads
    }

    /// Encode the current configuration into a [`Configuration`](protocol::Configuration)
    /// protobuf message and serialize it.
    pub(crate) fn encode_and_serialize(&self) -> Vec<u8> {
        let configuration = protocol::Configuration {
            multivariate_reserved_memory_in_bytes: self
                .configuration
                .multivariate_reserved_memory_in_bytes,
            uncompressed_reserved_memory_in_bytes: self
                .configuration
                .uncompressed_reserved_memory_in_bytes,
            compressed_reserved_memory_in_bytes: self
                .configuration
                .compressed_reserved_memory_in_bytes,
            transfer_batch_size_in_bytes: self.configuration.transfer_batch_size_in_bytes,
            transfer_time_in_seconds: self.configuration.transfer_time_in_seconds,
            ingestion_threads: self.configuration.ingestion_threads as u32,
            compression_threads: self.configuration.compression_threads as u32,
            writer_threads: self.configuration.writer_threads as u32,
        };

        configuration.encode_to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use modelardb_storage::data_folder::DataFolder;
    use modelardb_types::types::{Node, ServerMode};
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    use crate::cluster::Cluster;
    use crate::data_folders::DataFolders;
    use crate::storage::StorageEngine;

    // Tests for ConfigurationManager.
    #[tokio::test]
    async fn test_configuration_file_is_created_if_it_does_not_exist() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_storage_engine, configuration_manager) = create_components(&temp_dir).await;

        let configuration_from_manager = configuration_manager.read().await.configuration.clone();
        let configuration_from_file = configuration_from_file(&temp_dir).await;

        assert_eq!(configuration_from_manager, configuration_from_file);
    }

    #[tokio::test]
    async fn test_configuration_file_is_read_if_it_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::open_local_url(local_url).await.unwrap();

        let existing_configuration = Configuration {
            multivariate_reserved_memory_in_bytes: 1,
            uncompressed_reserved_memory_in_bytes: 1,
            compressed_reserved_memory_in_bytes: 1,
            transfer_batch_size_in_bytes: Some(1),
            transfer_time_in_seconds: Some(1),
            ..Configuration::default()
        };

        existing_configuration
            .save_to_toml(&local_data_folder)
            .await
            .unwrap();

        let (_storage_engine, configuration_manager) = create_components(&temp_dir).await;

        let configuration_from_manager = configuration_manager.read().await.configuration.clone();
        let configuration_from_file = configuration_from_file(&temp_dir).await;

        assert_eq!(existing_configuration, configuration_from_manager);
        assert_eq!(existing_configuration, configuration_from_file);
    }

    #[tokio::test]
    async fn test_invalid_configuration_in_configuration_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::open_local_url(local_url).await.unwrap();

        // Multiple threads per component are not supported.
        let invalid_configuration = Configuration {
            ingestion_threads: 2,
            ..Configuration::default()
        };

        invalid_configuration
            .save_to_toml(&local_data_folder)
            .await
            .unwrap();

        let result =
            ConfigurationManager::try_new(local_data_folder, ClusterMode::SingleNode).await;

        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid State Error: Only one thread per component is currently supported.".to_owned()
        );
    }

    #[tokio::test]
    async fn test_invalid_toml_in_configuration_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::open_local_url(local_url).await.unwrap();

        // Write invalid TOML to the configuration file.
        let path = temp_dir.path().join(CONFIGURATION_FILE_NAME);
        std::fs::write(path, "invalid_toml").unwrap();

        let result =
            ConfigurationManager::try_new(local_data_folder, ClusterMode::SingleNode).await;

        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("TOML Deserialize Error: TOML parse error at line 1")
        );
    }

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
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .multivariate_reserved_memory_in_bytes(),
            new_value
        );

        let configuration_from_file = configuration_from_file(&temp_dir).await;
        assert_eq!(
            configuration_from_file.multivariate_reserved_memory_in_bytes,
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

        let configuration_from_file = configuration_from_file(&temp_dir).await;
        assert_eq!(
            configuration_from_file.uncompressed_reserved_memory_in_bytes,
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

        let configuration_from_file = configuration_from_file(&temp_dir).await;
        assert_eq!(
            configuration_from_file.compressed_reserved_memory_in_bytes,
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

        let configuration_from_file = configuration_from_file(&temp_dir).await;
        assert_eq!(
            configuration_from_file.transfer_batch_size_in_bytes,
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

        let configuration_from_file = configuration_from_file(&temp_dir).await;
        assert_eq!(configuration_from_file.transfer_time_in_seconds, new_value)
    }

    /// Return the configuration from the configuration file at the root of `temp_dir`.
    async fn configuration_from_file(temp_dir: &TempDir) -> Configuration {
        let configuration_file_path = temp_dir.path().join(CONFIGURATION_FILE_NAME);
        let file_content = std::fs::read_to_string(&configuration_file_path).unwrap();

        toml::from_str::<Configuration>(&file_content).unwrap()
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
            Some(remote_data_folder.clone()),
            local_data_folder.clone(),
        );

        let edge_node = Node::new("edge".to_owned(), ServerMode::Edge);
        let cluster = Cluster::try_new(edge_node, remote_data_folder)
            .await
            .unwrap();

        let configuration_manager = Arc::new(RwLock::new(
            ConfigurationManager::try_new(
                local_data_folder,
                ClusterMode::MultiNode(Box::new(cluster)),
            )
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
