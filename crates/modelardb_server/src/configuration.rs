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

//! Management of the system's configuration. The configuration consists of various parameters that
//! determine the behavior of the storage engine, write-ahead log, and data transfer component.
//! The value for each configuration is determined according to the following precedence order:
//!
//! 1. CLI flag.
//! 2. Environment variable.
//! 3. Configuration file.
//! 4. Default value.

use std::sync::Arc;

use modelardb_storage::data_folder::DataFolder;
use modelardb_storage::parser;
use modelardb_storage::write_ahead_log::WriteAheadLog;
use modelardb_types::flight::protocol;
use modelardb_types::types::MAX_RETENTION_PERIOD_IN_SECONDS;
use object_store::path::Path;
use object_store::{Error, ObjectStoreExt, PutPayload};
use prost::Message;
use serde::{Deserialize, Serialize};
use sysinfo::System;
use tokio::sync::RwLock;

use crate::ServerArgs;
use crate::cluster::ClusterMode;
use crate::error::{ModelarDbServerError, Result};
use crate::storage::StorageEngine;

const CONFIGURATION_FILE_NAME: &str = "modelardbd.toml";

/// The different possible modes for the write-ahead log, assigned when the server is started.
#[derive(Clone)]
pub(crate) enum WalMode {
    Enabled(Arc<RwLock<WriteAheadLog>>),
    Disabled,
}

/// The system's unparsed configuration file. The configuration file is stored in a data folder as
/// [`CONFIGURATION_FILE_NAME`]. It should only be updated through a [`ConfigurationManager`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ConfigurationFile {
    /// Amount of memory to reserve for storing ingested time series.
    ingested_reserved_memory: String,
    /// Amount of memory to reserve for storing uncompressed data buffers.
    uncompressed_reserved_memory: String,
    /// Amount of memory to reserve for storing compressed data buffers.
    compressed_reserved_memory: String,
    /// The number of data that are required before transferring a batch of data to the remote
    /// object store. If [`None`], data is only transferred on an explicit flush.
    transfer_batch_size: Option<String>,
    /// The approximate maximum size of a single WAL segment file before it is closed and a new one
    /// is started.
    segment_size_threshold: String,
    /// Target size of the files produced when automatically compacting a table's storage. This is
    /// also the default value used when an OPTIMIZE query is executed without an explicit target
    /// size.
    optimize_target_file_size: String,
    /// Retention period used when automatically vacuuming a table during compaction.
    /// This is also the default value used when a VACUUM query is executed without an explicit
    /// retention period.
    vacuum_retention_period: String,
    /// Whether the write-ahead log is enabled. If enabled, data is logged before ingestion and the
    /// log is replayed on crash recovery.
    wal_enabled: bool,
}

impl ConfigurationFile {
    /// Update the configuration file's parameters with the corresponding flags or environment
    /// variables from the command line if they are set.
    fn update_from_args(&mut self, args: ServerArgs) {
        if let Some(value) = args.ingested_reserved_memory {
            self.ingested_reserved_memory = value;
        }

        if let Some(value) = args.uncompressed_reserved_memory {
            self.uncompressed_reserved_memory = value;
        }

        if let Some(value) = args.compressed_reserved_memory {
            self.compressed_reserved_memory = value;
        }

        if let Some(value) = args.transfer_batch_size {
            self.transfer_batch_size = Some(value);
        }

        if let Some(value) = args.segment_size_threshold {
            self.segment_size_threshold = value;
        }

        if let Some(value) = args.optimize_target_file_size {
            self.optimize_target_file_size = value;
        }

        if let Some(value) = args.vacuum_retention_period {
            self.vacuum_retention_period = value;
        }

        if let Some(value) = args.wal_enabled {
            self.wal_enabled = value;
        }
    }

    /// Write the configuration file to the root of `local_data_folder`. If the file does not exist,
    /// it is created. If the configuration file could not be updated or created, return
    /// [`ModelarDbServerError`].
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

impl Default for ConfigurationFile {
    fn default() -> Self {
        Self {
            ingested_reserved_memory: "10%".to_owned(),
            uncompressed_reserved_memory: "10%".to_owned(),
            compressed_reserved_memory: "30%".to_owned(),
            transfer_batch_size: Some("64 MIB".to_owned()),
            segment_size_threshold: "64 MIB".to_owned(),
            optimize_target_file_size: "64 MIB".to_owned(),
            vacuum_retention_period: "7 DAYS".to_owned(),
            wal_enabled: true,
        }
    }
}

/// Parse a value followed by a percentage or a unit (B, KB, KiB, MB, MiB, GB, GiB, TB, TiB,
/// case-insensitive).
/// - If `input` is a percentage and a total is given, that percentage of the total in bytes is returned.
/// - If `input` has a unit as a suffix, the specified amount of `input` in bytes is returned.
/// - If a parse error occurs it is returned.
fn parse_size_argument(input: &str, maybe_total: Option<u64>) -> Result<u64> {
    let (value, suffix) = split_at_unit(input)?;
    let bytes = if let Some(total) = maybe_total
        && suffix == "%"
    {
        ((total as f64 / 100.0) * value as f64) as u64
    } else {
        value * parser::byte_unit_multiplier(suffix)?
    };

    Ok(bytes)
}

/// Parse a value followed by a unit (S, M, H, DAY, and DAYS case-insensitive). If a parse error
/// occurs it is returned.
fn parse_time_argument(input: &str) -> Result<u64> {
    let (value, unit) = split_at_unit(input)?;
    let seconds = value
        * match unit.to_uppercase().as_str() {
            "" => Ok(1),
            "S" => Ok(1),
            "M" => Ok(60),
            "H" => Ok(60 * 60),
            "DAY" => Ok(60 * 60 * 24),
            "DAYS" => Ok(60 * 60 * 24),
            _ => Err(ModelarDbServerError::InvalidArgument(format!(
                "Unit must be S, M, H, DAY, or DAYS, not '{unit}'."
            ))),
        }?;

    Ok(seconds)
}

/// Split `input` into its value and unit.
fn split_at_unit(input: &str) -> Result<(u64, &str)> {
    let input = input.trim();
    let unit_start = input
        .char_indices()
        .find_map(|(index, character)| (!character.is_numeric()).then_some(index))
        .unwrap_or(input.len());

    let value: u64 = input[0..unit_start].parse::<u64>()?;
    let unit = input[unit_start..input.len()].trim();

    Ok((value, unit))
}

/// Manages the system's configuration and provides functionality for updating the configuration.
pub struct ConfigurationManager {
    /// The local data folder that stores the configuration file at the root.
    local_data_folder: Arc<DataFolder>,
    /// The configuration file in local data folder. This is stored in simplify updating it.
    configuration_file: ConfigurationFile,
    /// The mode of the cluster used to determine the behaviour when starting the server,
    /// creating tables, updating the remote object store, and querying.
    cluster_mode: ClusterMode,
    /// Amount of memory in bytes to reserve for storing ingested time series.
    ingested_reserved_memory_in_bytes: u64,
    /// Amount of memory in bytes to reserve for storing uncompressed data buffers.
    uncompressed_reserved_memory_in_bytes: u64,
    /// Amount of memory in bytes to reserve for storing compressed data buffers.
    compressed_reserved_memory_in_bytes: u64,
    /// The number of bytes that are required before transferring a batch of data to the remote
    /// object store. If [`None`], data is only transferred on an explicit flush.
    transfer_batch_size_in_bytes: Option<u64>,
    /// The approximate maximum size, in bytes, of a single WAL segment file before it is closed and
    /// a new one is started.
    segment_size_threshold_in_bytes: u64,
    /// Target size, in bytes, of the files produced when automatically compacting a table's
    /// storage. This is also the default value used when an OPTIMIZE query is executed without an
    /// explicit target size.
    optimize_target_file_size_in_bytes: u64,
    /// Retention period, in seconds, used when automatically vacuuming a table during compaction.
    /// This is also the default value used when a VACUUM query is executed without an explicit
    /// retention period.
    vacuum_retention_period_in_seconds: u64,
    /// Number of threads to allocate for converting multivariate time series to univariate
    /// time series.
    ingestion_threads: u8,
    /// Number of threads to allocate for compressing univariate time series to segments.
    compression_threads: u8,
    /// Number of threads to allocate for writing segments to a local and/or remote data folder.
    writer_threads: u8,
    /// Whether the write-ahead log is enabled. If enabled, data is logged before ingestion and the
    /// log is replayed on crash recovery.
    wal_enabled: bool,
    /// The mode of the write-ahead log used to determine whether data is logged before ingestion.
    wal_mode: WalMode,
}

impl ConfigurationManager {
    /// Create a new [`ConfigurationManager`] using the [`CONFIGURATION_FILE_NAME`] configuration
    /// file in the local data folder. If the file does not exist, a configuration file is created
    /// with the default values. Note that the configuration file and default values are overwritten
    /// if the corresponding CLI flags or environment variables are set. If the configuration file
    /// could not be read or created, [`ModelarDbServerError`] is returned.
    pub async fn try_new(
        local_data_folder: Arc<DataFolder>,
        cluster_mode: ClusterMode,
        args: ServerArgs,
    ) -> Result<Self> {
        // Check if there is a configuration file in the local data folder.
        let object_store = local_data_folder.object_store();
        let maybe_config_file = object_store.get(&Path::from(CONFIGURATION_FILE_NAME)).await;

        let mut configuration_file = match maybe_config_file {
            Ok(config_file) => {
                // If the configuration file exists, load the configuration from the file.
                let bytes = config_file.bytes().await?;
                toml::from_slice::<ConfigurationFile>(&bytes)?
            }
            Err(error) => match error {
                Error::NotFound { .. } => {
                    // If the configuration file does not exist, create one with the default values.
                    ConfigurationFile::default()
                }
                error => {
                    return Err(ModelarDbServerError::InvalidState(format!(
                        "Configuration file '{CONFIGURATION_FILE_NAME}' could not be read: {error}"
                    )));
                }
            },
        };

        // Parameters specified on the command-line take precedence over the configuration file.
        configuration_file.update_from_args(args);

        // Parse the configuration and prepare the values for the creating manager.
        // The values is parsed outside Self as configuration_file is moved into it.
        let mut system = System::new();
        system.refresh_memory();
        let total_memory_in_bytes = Some(system.total_memory());

        let ingested_reserved_memory_in_bytes = parse_size_argument(
            &configuration_file.ingested_reserved_memory,
            total_memory_in_bytes,
        )?;
        let uncompressed_reserved_memory_in_bytes = parse_size_argument(
            &configuration_file.uncompressed_reserved_memory,
            total_memory_in_bytes,
        )?;
        let compressed_reserved_memory_in_bytes = parse_size_argument(
            &configuration_file.compressed_reserved_memory,
            total_memory_in_bytes,
        )?;
        let transfer_batch_size_in_bytes =
            if let Some(transfer_batch_size) = &configuration_file.transfer_batch_size {
                Some(parse_size_argument(transfer_batch_size, None)?)
            } else {
                None
            };
        let segment_size_threshold_in_bytes =
            parse_size_argument(&configuration_file.segment_size_threshold, None)?;
        let optimize_target_file_size_in_bytes =
            parse_size_argument(&configuration_file.optimize_target_file_size, None)?;
        let vacuum_retention_period_in_seconds =
            parse_time_argument(&configuration_file.vacuum_retention_period)?;
        let ingestion_threads = 1;
        let compression_threads = 1;
        let writer_threads = 1;
        let wal_enabled = configuration_file.wal_enabled;
        let wal_mode = if wal_enabled {
            let write_ahead_log =
                WriteAheadLog::try_new(&local_data_folder, segment_size_threshold_in_bytes).await?;

            WalMode::Enabled(Arc::new(RwLock::new(write_ahead_log)))
        } else {
            WalMode::Disabled
        };

        // Validate the configuration and prepare the values for the creating manager.
        // TODO: Add support for running multiple threads per component. The individual
        //       components in the storage engine have not been validated with multiple threads, e.g.,
        //       UncompressedDataManager may have race conditions finishing buffers if multiple
        //       different data points are added by multiple different clients in parallel.
        if ingestion_threads != 1 || compression_threads != 1 || writer_threads != 1 {
            return Err(ModelarDbServerError::InvalidState(
                "Only one thread per component is currently supported.".to_owned(),
            ));
        };

        if optimize_target_file_size_in_bytes == 0 {
            return Err(ModelarDbServerError::InvalidState(
                "Optimize target file size must be greater than zero.".to_owned(),
            ));
        }

        if vacuum_retention_period_in_seconds > MAX_RETENTION_PERIOD_IN_SECONDS {
            return Err(ModelarDbServerError::InvalidState(format!(
                "Vacuum retention period cannot be more than {MAX_RETENTION_PERIOD_IN_SECONDS} seconds."
            )));
        }

        // The configuration is correct, so it can be saved to the data folder.
        configuration_file.save_to_toml(&local_data_folder).await?;

        Ok(Self {
            local_data_folder,
            configuration_file,
            cluster_mode,
            ingested_reserved_memory_in_bytes,
            uncompressed_reserved_memory_in_bytes,
            compressed_reserved_memory_in_bytes,
            transfer_batch_size_in_bytes,
            segment_size_threshold_in_bytes,
            optimize_target_file_size_in_bytes,
            vacuum_retention_period_in_seconds,
            ingestion_threads,
            compression_threads,
            writer_threads,
            wal_enabled,
            wal_mode,
        })
    }
    pub(crate) fn cluster_mode(&self) -> &ClusterMode {
        &self.cluster_mode
    }

    pub(crate) fn wal_mode(&self) -> &WalMode {
        &self.wal_mode
    }

    pub(crate) fn ingested_reserved_memory_in_bytes(&self) -> u64 {
        self.ingested_reserved_memory_in_bytes
    }

    /// Set the new value and update the amount of memory for ingested data in the storage
    /// engine. If the new configuration could not be saved to the configuration file, return
    /// [`ModelarDbServerError`].
    pub(crate) async fn set_ingested_reserved_memory_in_bytes(
        &mut self,
        new_ingested_reserved_memory_in_bytes: u64,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<()> {
        // Since the storage engine only keeps track of the remaining reserved memory, calculate
        // how much the value should change.
        let value_change = new_ingested_reserved_memory_in_bytes as i64
            - self.ingested_reserved_memory_in_bytes as i64;

        storage_engine
            .write()
            .await
            .adjust_ingested_remaining_memory_in_bytes(value_change)
            .await;

        self.ingested_reserved_memory_in_bytes = new_ingested_reserved_memory_in_bytes;

        // Update the configuration file.
        self.configuration_file.ingested_reserved_memory =
            self.ingested_reserved_memory_in_bytes.to_string();
        self.configuration_file
            .save_to_toml(&self.local_data_folder)
            .await
    }

    pub(crate) fn uncompressed_reserved_memory_in_bytes(&self) -> u64 {
        self.uncompressed_reserved_memory_in_bytes
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
            - self.uncompressed_reserved_memory_in_bytes as i64;

        storage_engine
            .write()
            .await
            .adjust_uncompressed_remaining_memory_in_bytes(value_change)
            .await?;

        self.uncompressed_reserved_memory_in_bytes = new_uncompressed_reserved_memory_in_bytes;

        // Update the configuration file.
        self.configuration_file.uncompressed_reserved_memory =
            self.uncompressed_reserved_memory_in_bytes.to_string();
        self.configuration_file
            .save_to_toml(&self.local_data_folder)
            .await
    }

    pub(crate) fn compressed_reserved_memory_in_bytes(&self) -> u64 {
        self.compressed_reserved_memory_in_bytes
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
            - self.compressed_reserved_memory_in_bytes as i64;

        storage_engine
            .write()
            .await
            .adjust_compressed_remaining_memory_in_bytes(value_change)
            .await?;

        self.compressed_reserved_memory_in_bytes = new_compressed_reserved_memory_in_bytes;

        // Update the configuration file.
        self.configuration_file.compressed_reserved_memory =
            self.compressed_reserved_memory_in_bytes.to_string();
        self.configuration_file
            .save_to_toml(&self.local_data_folder)
            .await
    }

    pub(crate) fn transfer_batch_size_in_bytes(&self) -> Option<u64> {
        self.transfer_batch_size_in_bytes
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

        self.transfer_batch_size_in_bytes = new_transfer_batch_size_in_bytes;

        // Update the configuration file.
        self.configuration_file.transfer_batch_size = self
            .transfer_batch_size_in_bytes
            .map(|value| value.to_string());
        self.configuration_file
            .save_to_toml(&self.local_data_folder)
            .await
    }

    #[allow(dead_code)]
    pub(crate) fn segment_size_threshold_in_bytes(&self) -> u64 {
        self.segment_size_threshold_in_bytes
    }

    /// Set the new value and update the segment size threshold in the write-ahead log. If the
    /// new configuration could not be saved to the configuration file, return
    /// [`ModelarDbServerError`].
    pub(crate) async fn set_segment_size_threshold_in_bytes(
        &mut self,
        new_segment_size_threshold_in_bytes: u64,
    ) -> Result<()> {
        let write_ahead_log = match self.wal_mode() {
            WalMode::Enabled(write_ahead_log) => write_ahead_log.clone(),
            WalMode::Disabled => {
                return Err(ModelarDbServerError::InvalidState(
                    "Cannot set segment size threshold when WAL is disabled.".to_owned(),
                ));
            }
        };

        write_ahead_log
            .write()
            .await
            .set_segment_size_threshold_in_bytes(new_segment_size_threshold_in_bytes);

        self.segment_size_threshold_in_bytes = new_segment_size_threshold_in_bytes;

        // Update the configuration file.
        self.configuration_file.segment_size_threshold =
            self.segment_size_threshold_in_bytes.to_string();
        self.configuration_file
            .save_to_toml(&self.local_data_folder)
            .await
    }

    pub(crate) fn optimize_target_file_size_in_bytes(&self) -> u64 {
        self.optimize_target_file_size_in_bytes
    }

    /// Set the target file size used by the data storage compactor and as the default for OPTIMIZE
    /// queries without an explicit target size. If the new value is zero or the new configuration
    /// could not be saved to the configuration file, return [`ModelarDbServerError`].
    pub(crate) async fn set_optimize_target_file_size_in_bytes(
        &mut self,
        new_optimize_target_file_size_in_bytes: u64,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<()> {
        if new_optimize_target_file_size_in_bytes == 0 {
            return Err(ModelarDbServerError::InvalidArgument(
                "Optimize target file size must be greater than zero.".to_owned(),
            ));
        }

        storage_engine
            .write()
            .await
            .set_optimize_target_file_size_in_bytes(new_optimize_target_file_size_in_bytes)
            .await;

        self.optimize_target_file_size_in_bytes = new_optimize_target_file_size_in_bytes;

        // Update the configuration file.
        self.configuration_file.optimize_target_file_size =
            self.optimize_target_file_size_in_bytes.to_string();
        self.configuration_file
            .save_to_toml(&self.local_data_folder)
            .await
    }

    pub(crate) fn vacuum_retention_period_in_seconds(&self) -> u64 {
        self.vacuum_retention_period_in_seconds
    }

    /// Set the retention period used by the data storage compactor and as the default for VACUUM
    /// queries without an explicit retention period. If the new value is larger than
    /// [`MAX_RETENTION_PERIOD_IN_SECONDS`] or the new configuration could not be saved to the
    /// configuration file, return [`ModelarDbServerError`].
    pub(crate) async fn set_vacuum_retention_period_in_seconds(
        &mut self,
        new_vacuum_retention_period_in_seconds: u64,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Result<()> {
        if new_vacuum_retention_period_in_seconds > MAX_RETENTION_PERIOD_IN_SECONDS {
            return Err(ModelarDbServerError::InvalidArgument(format!(
                "Vacuum retention period cannot be more than {MAX_RETENTION_PERIOD_IN_SECONDS} seconds."
            )));
        }

        storage_engine
            .write()
            .await
            .set_vacuum_retention_period_in_seconds(new_vacuum_retention_period_in_seconds)
            .await;

        self.vacuum_retention_period_in_seconds = new_vacuum_retention_period_in_seconds;

        // Update the configuration file.
        self.configuration_file.vacuum_retention_period =
            self.vacuum_retention_period_in_seconds.to_string();
        self.configuration_file
            .save_to_toml(&self.local_data_folder)
            .await
    }
    pub(crate) fn ingestion_threads(&self) -> u8 {
        self.ingestion_threads
    }

    pub(crate) fn compression_threads(&self) -> u8 {
        self.compression_threads
    }

    pub(crate) fn writer_threads(&self) -> u8 {
        self.writer_threads
    }

    /// Encode the current configuration into a [`Configuration`](protocol::Configuration)
    /// protobuf message and serialize it.
    pub(crate) fn encode_and_serialize(&self) -> Vec<u8> {
        let configuration = protocol::Configuration {
            ingested_reserved_memory_in_bytes: self.ingested_reserved_memory_in_bytes,
            uncompressed_reserved_memory_in_bytes: self.uncompressed_reserved_memory_in_bytes,
            compressed_reserved_memory_in_bytes: self.compressed_reserved_memory_in_bytes,
            transfer_batch_size_in_bytes: self.transfer_batch_size_in_bytes,
            segment_size_threshold_in_bytes: self.segment_size_threshold_in_bytes,
            ingestion_threads: self.ingestion_threads as u32,
            compression_threads: self.compression_threads as u32,
            writer_threads: self.writer_threads as u32,
            wal_enabled: self.wal_enabled,
            optimize_target_file_size_in_bytes: self.optimize_target_file_size_in_bytes,
            vacuum_retention_period_in_seconds: self.vacuum_retention_period_in_seconds,
        };

        configuration.encode_to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use clap::Parser;
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

        let configuration_from_manager = &configuration_manager.read().await.configuration_file;
        let configuration_from_file = configuration_from_file(&temp_dir).await;

        assert_eq!(*configuration_from_manager, configuration_from_file);
    }

    #[tokio::test]
    async fn test_configuration_file_is_read_if_it_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::open_local_url(local_url).await.unwrap();

        let existing_configuration = ConfigurationFile {
            ingested_reserved_memory: "1".to_owned(),
            uncompressed_reserved_memory: "1".to_owned(),
            compressed_reserved_memory: "1".to_owned(),
            transfer_batch_size: Some("1".to_owned()),
            segment_size_threshold: "1".to_owned(),
            optimize_target_file_size: "1".to_owned(),
            vacuum_retention_period: "1".to_owned(),
            ..ConfigurationFile::default()
        };

        existing_configuration
            .save_to_toml(&local_data_folder)
            .await
            .unwrap();

        let (_storage_engine, configuration_manager) = create_components(&temp_dir).await;

        let configuration_from_manager = configuration_manager
            .read()
            .await
            .configuration_file
            .clone();
        let configuration_from_file = configuration_from_file(&temp_dir).await;

        assert_eq!(existing_configuration, configuration_from_manager);
        assert_eq!(existing_configuration, configuration_from_file);
    }

    #[tokio::test]
    async fn test_invalid_configuration_in_configuration_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = Arc::new(DataFolder::open_local_url(local_url).await.unwrap());

        // Percentage must be a numeric value..
        let invalid_configuration = ConfigurationFile {
            ingested_reserved_memory: "half".to_owned(),
            ..ConfigurationFile::default()
        };

        invalid_configuration
            .save_to_toml(&local_data_folder)
            .await
            .unwrap();

        let node = Node::new("edge".to_owned(), ServerMode::Edge);
        let result = ConfigurationManager::try_new(
            local_data_folder,
            ClusterMode::SingleNode(node),
            default_args(),
        )
        .await;

        assert_eq!(
            result.err().unwrap().to_string(),
            "Parse Int Error: cannot parse integer from empty string".to_owned()
        );
    }

    #[tokio::test]
    async fn test_invalid_toml_in_configuration_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = Arc::new(DataFolder::open_local_url(local_url).await.unwrap());

        // Write invalid TOML to the configuration file.
        let path = temp_dir.path().join(CONFIGURATION_FILE_NAME);
        std::fs::write(path, "invalid_toml").unwrap();

        let node = Node::new("edge".to_owned(), ServerMode::Edge);
        let result = ConfigurationManager::try_new(
            local_data_folder,
            ClusterMode::SingleNode(node),
            default_args(),
        )
        .await;

        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("TOML Deserialize Error: TOML parse error at line 1")
        );
    }

    #[tokio::test]
    async fn test_validate_rejects_zero_optimize_target_file_size() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = Arc::new(DataFolder::open_local_url(local_url).await.unwrap());

        // Write the configuration file.
        let configuration = ConfigurationFile {
            optimize_target_file_size: "0".to_owned(),
            ..ConfigurationFile::default()
        };

        configuration
            .save_to_toml(&local_data_folder)
            .await
            .unwrap();

        // Validate configuration file.
        let node = Node::new("edge".to_owned(), ServerMode::Edge);
        let result = ConfigurationManager::try_new(
            local_data_folder,
            ClusterMode::SingleNode(node),
            default_args(),
        )
        .await;

        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid State Error: Optimize target file size must be greater than zero."
        );
    }

    #[tokio::test]
    async fn test_validate_rejects_too_large_vacuum_retention_period() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = Arc::new(DataFolder::open_local_url(local_url).await.unwrap());

        // Write the configuration file.
        let configuration = ConfigurationFile {
            vacuum_retention_period: format!("{}", MAX_RETENTION_PERIOD_IN_SECONDS + 1),
            ..ConfigurationFile::default()
        };

        configuration
            .save_to_toml(&local_data_folder)
            .await
            .unwrap();

        // Validate configuration file.
        let node = Node::new("edge".to_owned(), ServerMode::Edge);
        let result = ConfigurationManager::try_new(
            local_data_folder,
            ClusterMode::SingleNode(node),
            default_args(),
        )
        .await;

        assert_eq!(
            result.err().unwrap().to_string(),
            format!(
                "Invalid State Error: Vacuum retention period cannot be more than {} seconds.",
                MAX_RETENTION_PERIOD_IN_SECONDS
            )
        );
    }

    #[tokio::test]
    async fn test_set_ingested_reserved_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        // The default is a percentage of the system's memory.
        assert!(
            configuration_manager
                .read()
                .await
                .ingested_reserved_memory_in_bytes()
                > 0
        );

        let new_value = 1024;
        configuration_manager
            .write()
            .await
            .set_ingested_reserved_memory_in_bytes(new_value, storage_engine)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .ingested_reserved_memory_in_bytes(),
            new_value
        );

        let configuration_from_file = configuration_from_file(&temp_dir).await;
        assert_eq!(
            configuration_from_file.ingested_reserved_memory,
            new_value.to_string()
        );
    }

    #[tokio::test]
    async fn test_set_uncompressed_reserved_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        // The default is a percentage of the system's memory.
        assert!(
            configuration_manager
                .read()
                .await
                .uncompressed_reserved_memory_in_bytes()
                > 0
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
            configuration_from_file.uncompressed_reserved_memory,
            new_value.to_string()
        );
    }

    #[tokio::test]
    async fn test_set_compressed_reserved_memory_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        // The default is a percentage of the system's memory.
        assert!(
            configuration_manager
                .read()
                .await
                .compressed_reserved_memory_in_bytes()
                > 0
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
            configuration_from_file.compressed_reserved_memory,
            new_value.to_string()
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
            configuration_from_file.transfer_batch_size.unwrap(),
            new_value.unwrap().to_string()
        );
    }

    #[tokio::test]
    async fn test_set_segment_size_threshold_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_storage_engine, configuration_manager) = create_components(&temp_dir).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .segment_size_threshold_in_bytes(),
            64 * 1024 * 1024
        );

        let new_value = 1024;
        configuration_manager
            .write()
            .await
            .set_segment_size_threshold_in_bytes(new_value)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .segment_size_threshold_in_bytes(),
            new_value
        );

        let configuration_from_file = configuration_from_file(&temp_dir).await;
        assert_eq!(
            configuration_from_file.segment_size_threshold,
            new_value.to_string()
        );
    }

    #[tokio::test]
    async fn test_set_segment_size_threshold_in_bytes_with_wal_disabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_storage_engine, configuration_manager) = create_components(&temp_dir).await;
        configuration_manager.write().await.wal_mode = WalMode::Disabled;

        let result = configuration_manager
            .write()
            .await
            .set_segment_size_threshold_in_bytes(1)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid State Error: Cannot set segment size threshold when WAL is disabled."
        );
    }

    #[tokio::test]
    async fn test_set_optimize_target_file_size_in_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .optimize_target_file_size_in_bytes(),
            64 * 1024 * 1024
        );

        let new_value = 1024;
        configuration_manager
            .write()
            .await
            .set_optimize_target_file_size_in_bytes(new_value, storage_engine)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .optimize_target_file_size_in_bytes(),
            new_value
        );

        let configuration_from_file = configuration_from_file(&temp_dir).await;
        assert_eq!(
            configuration_from_file.optimize_target_file_size,
            new_value.to_string()
        );
    }

    #[tokio::test]
    async fn test_set_optimize_target_file_size_in_bytes_rejects_zero() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        let result = configuration_manager
            .write()
            .await
            .set_optimize_target_file_size_in_bytes(0, storage_engine)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: Optimize target file size must be greater than zero."
        );
    }

    #[tokio::test]
    async fn test_set_vacuum_retention_period_in_seconds() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        assert_eq!(
            configuration_manager
                .read()
                .await
                .vacuum_retention_period_in_seconds(),
            60 * 60 * 24 * 7
        );

        let new_value = 60;
        configuration_manager
            .write()
            .await
            .set_vacuum_retention_period_in_seconds(new_value, storage_engine)
            .await
            .unwrap();

        assert_eq!(
            configuration_manager
                .read()
                .await
                .vacuum_retention_period_in_seconds(),
            new_value
        );

        let configuration_from_file = configuration_from_file(&temp_dir).await;
        assert_eq!(
            configuration_from_file.vacuum_retention_period,
            new_value.to_string()
        );
    }

    #[tokio::test]
    async fn test_set_vacuum_retention_period_in_seconds_rejects_too_large_value() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (storage_engine, configuration_manager) = create_components(&temp_dir).await;

        let result = configuration_manager
            .write()
            .await
            .set_vacuum_retention_period_in_seconds(
                MAX_RETENTION_PERIOD_IN_SECONDS + 1,
                storage_engine,
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: Vacuum retention period cannot be more than {} seconds.",
                MAX_RETENTION_PERIOD_IN_SECONDS
            )
        );
    }

    /// Return the configuration from the configuration file at the root of `temp_dir`.
    async fn configuration_from_file(temp_dir: &TempDir) -> ConfigurationFile {
        let configuration_file_path = temp_dir.path().join(CONFIGURATION_FILE_NAME);
        let file_content = std::fs::read_to_string(&configuration_file_path).unwrap();
        toml::from_str::<ConfigurationFile>(&file_content).unwrap()
    }

    /// Create a [`StorageEngine`] and a [`ConfigurationManager`].
    async fn create_components(
        temp_dir: &TempDir,
    ) -> (
        Arc<RwLock<StorageEngine>>,
        Arc<RwLock<ConfigurationManager>>,
    ) {
        let local_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = Arc::new(DataFolder::open_local_url(local_url).await.unwrap());

        let target_dir = tempfile::tempdir().unwrap();
        let target_url = target_dir.path().to_str().unwrap();
        let remote_data_folder = Arc::new(DataFolder::open_local_url(target_url).await.unwrap());

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
                local_data_folder.clone(),
                ClusterMode::MultiNode(cluster),
                default_args(),
            )
            .await
            .unwrap(),
        ));

        let wal_mode = configuration_manager.read().await.wal_mode().clone();

        let storage_engine = Arc::new(RwLock::new(
            StorageEngine::try_new(data_folders, wal_mode, &configuration_manager)
                .await
                .unwrap(),
        ));

        (storage_engine, configuration_manager)
    }

    fn default_args() -> ServerArgs {
        ServerArgs::parse_from(["modelardbd", "edge", "data"])
    }

    // Tests for parse_size_argument().
    #[test]
    fn test_parse_size_argument_empty_string() {
        assert_eq!(
            parse_size_argument("", Some(10240))
                .unwrap_err()
                .to_string(),
            "Parse Int Error: cannot parse integer from empty string"
        )
    }

    #[test]
    fn test_parse_size_argument_no_suffix() {
        assert_eq!(parse_size_argument("37", Some(10240)).unwrap(), 37)
    }

    #[test]
    fn test_parse_size_argument_no_suffix_with_whitespace() {
        // ast-grep-ignore as the extra spaces are purposely added for testing.
        assert_eq!(parse_size_argument("  37  ", Some(10240)).unwrap(), 37)
    }

    #[test]
    fn test_parse_size_argument_byte_suffix() {
        assert_eq!(parse_size_argument("37B", Some(10240)).unwrap(), 37)
    }

    #[test]
    fn test_parse_size_argument_byte_suffix_with_whitespace() {
        // ast-grep-ignore as the extra spaces are purposely added for testing.
        assert_eq!(parse_size_argument(" 37   B  ", Some(10240)).unwrap(), 37)
    }

    #[test]
    fn test_parse_size_argument_kilobyte_suffix() {
        assert_eq!(parse_size_argument("37KB", Some(10240)).unwrap(), 37000)
    }

    #[test]
    fn test_parse_size_argument_kibibyte_suffix() {
        assert_eq!(parse_size_argument("37KiB", Some(10240)).unwrap(), 37888)
    }

    #[test]
    fn test_parse_size_argument_megabyte_suffix() {
        assert_eq!(parse_size_argument("37MB", Some(10240)).unwrap(), 37000000)
    }

    #[test]
    fn test_parse_size_argument_mebibyte_suffix() {
        assert_eq!(parse_size_argument("37MiB", Some(10240)).unwrap(), 38797312)
    }

    #[test]
    fn test_parse_size_argument_gigabyte_suffix() {
        assert_eq!(
            parse_size_argument("37GB", Some(10240)).unwrap(),
            37000000000
        )
    }

    #[test]
    fn test_parse_size_argument_gibibyte_suffix() {
        assert_eq!(
            parse_size_argument("37GiB", Some(10240)).unwrap(),
            39728447488
        )
    }

    #[test]
    fn test_parse_size_argument_terabyte_suffix() {
        assert_eq!(
            parse_size_argument("37TB", Some(10240)).unwrap(),
            37000000000000
        )
    }

    #[test]
    fn test_parse_size_argument_tebibyte_suffix() {
        assert_eq!(
            parse_size_argument("37TiB", Some(10240)).unwrap(),
            40681930227712
        )
    }

    #[test]
    fn test_parse_size_argument_percentage() {
        assert_eq!(parse_size_argument("10%", Some(10240)).unwrap(), 1024);
    }

    #[test]
    fn test_parse_size_argument_wrong_suffix() {
        assert_eq!(
            parse_size_argument("10#", Some(10240))
                .unwrap_err()
                .to_string(),
            "Parser Error: sql parser error: Unit must be B, KB, KiB, MB, MiB, GB, GiB, TB, or TiB, not '#'."
        )
    }

    // Tests for parse_time_argument().
    #[test]
    fn test_parse_time_argument_empty_string() {
        assert_eq!(
            parse_time_argument("").unwrap_err().to_string(),
            "Parse Int Error: cannot parse integer from empty string"
        )
    }

    #[test]
    fn test_parse_time_argument_no_suffix() {
        assert_eq!(parse_time_argument("37").unwrap(), 37)
    }

    #[test]
    fn test_parse_time_argument_no_suffix_with_whitespace() {
        // ast-grep-ignore as the extra spaces are purposely added for testing.
        assert_eq!(parse_time_argument("  37  ").unwrap(), 37)
    }

    #[test]
    fn test_parse_time_argument_second_suffix() {
        assert_eq!(parse_time_argument("37S").unwrap(), 37)
    }

    #[test]
    fn test_parse_time_argument_second_suffix_with_whitespace() {
        // ast-grep-ignore as the extra spaces are purposely added for testing.
        assert_eq!(parse_time_argument(" 37   S  ").unwrap(), 37)
    }

    #[test]
    fn test_parse_time_argument_minute_suffix() {
        assert_eq!(parse_time_argument("37M").unwrap(), 2220)
    }

    #[test]
    fn test_parse_time_argument_hour_suffix() {
        assert_eq!(parse_time_argument("37H").unwrap(), 133200)
    }

    #[test]
    fn test_parse_time_argument_day_suffix() {
        assert_eq!(parse_time_argument("37DAY").unwrap(), 3196800)
    }

    #[test]
    fn test_parse_time_argument_days_suffix() {
        assert_eq!(parse_time_argument("37DAYS").unwrap(), 3196800)
    }
}
