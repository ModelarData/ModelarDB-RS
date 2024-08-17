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

//! Ingests data points into temporary in-memory buffers that can be spilled to immutable Apache
//! Parquet files if necessary, uses the compression component to compress these buffers when they
//! are full or [`StorageEngine::flush()`] is called, stores the resulting data points compressed as
//! metadata and models in in-memory buffers to batch them before saving them to immutable Apache
//! Parquet files. The path to the Apache Parquet files containing relevant compressed data points
//! for a query can be retrieved by the query engine using [`DeltaLake`].

mod compressed_data_buffer;
mod compressed_data_manager;
mod data_transfer;
mod types;
mod uncompressed_data_buffer;
mod uncompressed_data_manager;

use std::env;
use std::io::{Error as IOError, ErrorKind};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::{self, JoinHandle};

use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use deltalake_core::DeltaTableError;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::storage::DeltaLake;
use modelardb_common::types::TimestampArray;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tonic::Status;
use tracing::error;

use crate::configuration::ConfigurationManager;
use crate::context::Context;
use crate::data_folders::DataFolders;
use crate::storage::compressed_data_manager::CompressedDataManager;
use crate::storage::data_transfer::DataTransfer;
use crate::storage::types::{Channels, MemoryPool, Message, Metric, MetricType};
use crate::storage::uncompressed_data_buffer::IngestedDataBuffer;
use crate::storage::uncompressed_data_manager::UncompressedDataManager;

/// The folder storing spilled uncompressed data buffers in the local data folder.
const UNCOMPRESSED_DATA_FOLDER: &str = "buffers";

/// The capacity of each uncompressed data buffer as the number of elements in the buffer where each
/// element is a [`Timestamp`](modelardb_common::types::Timestamp) and a
/// [`Value`](modelardb_common::types::Value). Note that the resulting size of the buffer has to be
/// a multiple of 64 bytes to avoid the actual capacity being larger than the requested due to
/// internal alignment when allocating memory for the two array builders.
pub static UNCOMPRESSED_DATA_BUFFER_CAPACITY: LazyLock<usize> = LazyLock::new(|| {
    env::var("MODELARDBD_UNCOMPRESSED_DATA_BUFFER_CAPACITY").map_or(64 * 1024, |value| {
        let parsed = value.parse::<usize>().unwrap();

        if parsed >= 64 && parsed % 64 == 0 {
            parsed
        } else {
            panic!("MODELARDBD_UNCOMPRESSED_DATA_BUFFER_CAPACITY must be a multiple of 64.")
        }
    })
});

/// Manages all uncompressed and compressed data, both while being stored in memory during ingestion
/// and when persisted to disk afterward.
pub struct StorageEngine {
    /// Manager that contains and controls all uncompressed data.
    uncompressed_data_manager: Arc<UncompressedDataManager>,
    /// Manager that contains and controls all compressed data.
    compressed_data_manager: Arc<CompressedDataManager>,
    /// Track how much memory is left for storing uncompressed and compressed data.
    memory_pool: Arc<MemoryPool>,
    /// Metric for the used multivariate memory in bytes, updated every time the used memory changes.
    used_multivariate_memory_metric: Arc<Mutex<Metric>>,
    /// Threads used for ingestion, compression, and writing.
    join_handles: Vec<JoinHandle<()>>,
    /// Unbounded channels used by the threads to communicate.
    channels: Arc<Channels>,
}

impl StorageEngine {
    /// Return [`StorageEngine`] that writes ingested data to `local_data_folder` and optionally
    /// transfers compressed data to `remote_data_folder` if it is given. Returns [`String`] if
    /// `remote_data_folder` is given but [`DataTransfer`] cannot not be created.
    pub(super) async fn try_new(
        runtime: Arc<Runtime>,
        data_folders: DataFolders,
        configuration_manager: &Arc<RwLock<ConfigurationManager>>,
    ) -> Result<Self, IOError> {
        // Create shared memory pool.
        let configuration_manager = configuration_manager.read().await;
        let memory_pool = Arc::new(MemoryPool::new(
            configuration_manager.multivariate_reserved_memory_in_bytes(),
            configuration_manager.uncompressed_reserved_memory_in_bytes(),
            configuration_manager.compressed_reserved_memory_in_bytes(),
        ));

        // Create shared metrics.
        let used_disk_space_metric = Arc::new(Mutex::new(Metric::new()));
        let used_multivariate_memory_metric = Arc::new(Mutex::new(Metric::new()));

        // Create threads and shared channels.
        let mut join_handles = vec![];
        let channels = Arc::new(Channels::new());

        // Create the uncompressed data manager.
        let uncompressed_data_manager = Arc::new(UncompressedDataManager::new(
            data_folders.local_data_folder.clone(),
            data_folders.remote_data_folder.clone(),
            memory_pool.clone(),
            channels.clone(),
            used_multivariate_memory_metric.clone(),
            used_disk_space_metric.clone(),
        ));

        {
            let runtime = runtime.clone();
            let uncompressed_data_manager = uncompressed_data_manager.clone();

            Self::start_threads(
                configuration_manager.ingestion_threads,
                "Ingestion",
                move || {
                    if let Err(error) =
                        uncompressed_data_manager.process_uncompressed_messages(runtime)
                    {
                        error!("Failed to receive uncompressed message due to: {}", error);
                    };
                },
                &mut join_handles,
            )?;
        }

        {
            let runtime = runtime.clone();
            let uncompressed_data_manager = uncompressed_data_manager.clone();

            Self::start_threads(
                configuration_manager.compression_threads,
                "Compression",
                move || {
                    if let Err(error) =
                        uncompressed_data_manager.process_compressor_messages(runtime)
                    {
                        error!("Failed to receive compressor message due to: {}", error);
                    };
                },
                &mut join_handles,
            )?;
        }

        // Create the compressed data manager.
        let data_transfer = if let Some(remote_data_folder) = data_folders.remote_data_folder {
            let table_names = data_folders
                .local_data_folder
                .table_metadata_manager
                .table_names()
                .await
                .map_err(IOError::other)?;

            let data_transfer = DataTransfer::try_new(
                data_folders.local_data_folder.clone(),
                remote_data_folder,
                table_names,
                configuration_manager.transfer_batch_size_in_bytes(),
                used_disk_space_metric.clone(),
            )
            .await
            .map_err(IOError::other)?;

            Some(data_transfer)
        } else {
            None
        };

        let data_transfer_is_some = data_transfer.is_some();
        let compressed_data_manager = Arc::new(CompressedDataManager::new(
            Arc::new(RwLock::new(data_transfer)),
            data_folders.local_data_folder,
            channels.clone(),
            memory_pool.clone(),
            used_disk_space_metric,
        ));

        {
            let runtime = runtime.clone();
            let compressed_data_manager = compressed_data_manager.clone();

            Self::start_threads(
                configuration_manager.writer_threads,
                "Writer",
                move || {
                    if let Err(error) = compressed_data_manager.process_compressed_messages(runtime)
                    {
                        error!("Failed to receive compressed message due to: {}", error);
                    };
                },
                &mut join_handles,
            )?;
        }

        let mut storage_engine = Self {
            uncompressed_data_manager,
            compressed_data_manager,
            memory_pool,
            used_multivariate_memory_metric,
            join_handles,
            channels,
        };

        // Start the task that transfers data periodically if a remote data folder is given and
        // time-based data transfer is enabled.
        if data_transfer_is_some {
            storage_engine
                .set_transfer_time_in_seconds(configuration_manager.transfer_time_in_seconds())
                .await
                .map_err(|error| IOError::new(ErrorKind::Other, error))?;
        }

        Ok(storage_engine)
    }

    /// Start `num_threads` threads with `name` that executes `function` and whose [`JoinHandle`] is
    /// added to `join_handles.
    fn start_threads<F>(
        num_threads: usize,
        name: &str,
        function: F,
        join_handles: &mut Vec<JoinHandle<()>>,
    ) -> Result<(), IOError>
    where
        F: FnOnce() + Send + Clone + 'static,
    {
        for thread_number in 0..num_threads {
            let join_handle = thread::Builder::new()
                .name(format!("{} {}", name, thread_number))
                .spawn(function.clone())
                .map_err(|error| IOError::new(ErrorKind::Other, error))?;

            join_handles.push(join_handle);
        }

        Ok(())
    }

    /// Add references to the
    /// [`UncompressedDataBuffers`](uncompressed_data_buffer::UncompressedDataBuffer) currently on
    /// disk to [`UncompressedDataManager`] which immediately will start compressing them.
    pub(super) async fn initialize(&self, context: &Context) -> Result<(), IOError> {
        self.uncompressed_data_manager.initialize(context).await
    }

    /// Pass `record_batch` to [`CompressedDataManager`]. Return [`Ok`] if `record_batch` was
    /// successfully written to an Apache Parquet file, otherwise return [`Err`].
    pub(super) async fn insert_record_batch(
        &self,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> Result<(), DeltaTableError> {
        self.compressed_data_manager
            .insert_record_batch(table_name, record_batch)
            .await
    }

    /// Pass `data_points` to [`UncompressedDataManager`]. Return [`Ok`] if all of the data points
    /// were successfully inserted, otherwise return [`String`].
    pub(super) async fn insert_data_points(
        &mut self,
        model_table_metadata: Arc<ModelTableMetadata>,
        multivariate_data_points: RecordBatch,
    ) -> Result<(), String> {
        // TODO: write to a WAL and use it to ensure termination never duplicates or loses data.
        self.memory_pool
            .wait_for_ingested_memory(multivariate_data_points.get_array_memory_size());

        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        self.used_multivariate_memory_metric.lock().unwrap().append(
            multivariate_data_points.get_array_memory_size() as isize,
            true,
        );

        self.channels
            .ingested_data_sender
            .send(Message::Data(IngestedDataBuffer::new(
                model_table_metadata,
                multivariate_data_points,
            )))
            .map_err(|error| error.to_string())
    }

    /// Flush all the data the [`StorageEngine`] is currently storing in memory to disk. If all
    /// the data is successfully flushed to disk, return [`Ok`], otherwise return [`String`].
    pub(super) async fn flush(&self) -> Result<(), String> {
        self.channels
            .ingested_data_sender
            .send(Message::Flush)
            .map_err(|error| format!("Unable to flush data in storage engine due to: {}", error))?;

        // Wait until all the data in the storage engine has been flushed.
        self.channels
            .result_receiver
            .recv()
            .map_err(|error| format!("Failed to receive result message due to: {}", error))?
            .map_err(|error| format!("Failed to flush data in storage engine due to: {}", error))
    }

    /// Transfer all the compressed data the [`StorageEngine`] is managing to the remote object store.
    pub(super) async fn transfer(&mut self) -> Result<(), Status> {
        if let Some(data_transfer) = &*self.compressed_data_manager.data_transfer.read().await {
            data_transfer
                .transfer_larger_than_threshold(0)
                .await
                .map_err(|error: ParquetError| Status::internal(error.to_string()))
        } else {
            Err(Status::internal("No remote object store available."))
        }
    }

    /// Flush all the data the [`StorageEngine`] is currently storing in memory to disk and stop
    /// all the threads. If all the data is successfully flushed to disk and all the threads stopped,
    /// return [`Ok`], otherwise return [`String`]. This method is purposely `&mut self` instead of
    /// `self` so it can be called through an Arc.
    pub(super) fn close(&mut self) -> Result<(), String> {
        self.channels
            .ingested_data_sender
            .send(Message::Stop)
            .map_err(|error| format!("Unable to stop the storage engine due to: {}", error))?;

        // Wait until all the data in the storage engine has been flushed.
        self.channels
            .result_receiver
            .recv()
            .map_err(|error| format!("Failed to receive result message due to: {}", error))?
            .map_err(|error| format!("Failed to flush data in storage engine due to: {}", error))?;

        // unwrap() is safe as join() only returns an error if the thread panicked.
        self.join_handles
            .drain(..)
            .for_each(|join_handle: JoinHandle<()>| join_handle.join().unwrap());

        Ok(())
    }

    /// Collect and return the metrics of used uncompressed/compressed memory, used disk space, and ingested
    /// data points over time. The metrics are returned in tuples with the format (metric_type, (timestamps, values)).
    pub(super) async fn collect_metrics(
        &mut self,
    ) -> Vec<(MetricType, (TimestampArray, UInt32Array))> {
        // unwrap() is safe as lock() only returns an error if the lock is poisoned.
        vec![
            (
                MetricType::UsedIngestedMemory,
                self.used_multivariate_memory_metric
                    .lock()
                    .unwrap()
                    .finish(),
            ),
            (
                MetricType::UsedUncompressedMemory,
                self.uncompressed_data_manager
                    .used_uncompressed_memory_metric
                    .lock()
                    .unwrap()
                    .finish(),
            ),
            (
                MetricType::UsedCompressedMemory,
                self.compressed_data_manager
                    .used_compressed_memory_metric
                    .lock()
                    .unwrap()
                    .finish(),
            ),
            (
                MetricType::IngestedDataPoints,
                self.uncompressed_data_manager
                    .ingested_data_points_metric
                    .lock()
                    .unwrap()
                    .finish(),
            ),
            (
                MetricType::UsedDiskSpace,
                self.compressed_data_manager
                    .used_disk_space_metric
                    .lock()
                    .unwrap()
                    .finish(),
            ),
        ]
    }

    /// Update the remote data folder, used to transfer data to in the data transfer component. If
    /// a data transfer component does not exist, return [`ModelarDbError].
    pub(super) async fn update_remote_data_folder(
        &mut self,
        remote_data_folder: Arc<DeltaLake>,
    ) -> Result<(), ModelarDbError> {
        let maybe_data_transfer = &mut *self.compressed_data_manager.data_transfer.write().await;

        if let Some(data_transfer) = maybe_data_transfer {
            data_transfer
                .update_remote_data_folder(remote_data_folder)
                .await;

            Ok(())
        } else {
            Err(ModelarDbError::ConfigurationError(
                "Storage engine is not configured to transfer data.".to_owned(),
            ))
        }
    }

    /// Change the amount of memory for multivariate data in bytes according to `value_change`.
    pub(super) async fn adjust_multivariate_remaining_memory_in_bytes(&self, value_change: isize) {
        self.memory_pool.adjust_ingested_memory(value_change)
    }

    /// Change the amount of memory for uncompressed data in bytes according to `value_change`.
    /// Returns [`IOError`] if the memory cannot be updated because a buffer cannot be spilled.
    pub(super) async fn adjust_uncompressed_remaining_memory_in_bytes(
        &self,
        value_change: isize,
    ) -> Result<(), IOError> {
        self.uncompressed_data_manager
            .adjust_uncompressed_remaining_memory_in_bytes(value_change)
            .await
    }

    /// Change the amount of memory for compressed data in bytes according to `value_change`. If
    /// the value is changed successfully return [`Ok`], otherwise return [`IOError`].
    pub(super) async fn adjust_compressed_remaining_memory_in_bytes(
        &self,
        value_change: isize,
    ) -> Result<(), IOError> {
        self.compressed_data_manager
            .adjust_compressed_remaining_memory_in_bytes(value_change)
            .await
    }

    /// Set the transfer batch size in the data transfer component to `new_value` if it exists. If
    /// a data transfer component does not exist, or the value could not be changed,
    /// return [`ModelarDbError`].
    pub(super) async fn set_transfer_batch_size_in_bytes(
        &self,
        new_value: Option<usize>,
    ) -> Result<(), ModelarDbError> {
        if let Some(ref mut data_transfer) =
            *self.compressed_data_manager.data_transfer.write().await
        {
            data_transfer
                .set_transfer_batch_size_in_bytes(new_value)
                .await
                .map_err(|error| ModelarDbError::ConfigurationError(error.to_string()))
        } else {
            Err(ModelarDbError::ConfigurationError(
                "Storage engine is not configured to transfer data.".to_owned(),
            ))
        }
    }

    /// Set the transfer time in the data transfer component to `new_value` if it exists. If
    /// a data transfer component does not exist, return [`ModelarDbError`].
    pub(super) async fn set_transfer_time_in_seconds(
        &mut self,
        new_value: Option<usize>,
    ) -> Result<(), ModelarDbError> {
        if let Some(ref mut data_transfer) =
            *self.compressed_data_manager.data_transfer.write().await
        {
            data_transfer.set_transfer_time_in_seconds(
                new_value,
                self.compressed_data_manager.data_transfer.clone(),
            );

            Ok(())
        } else {
            Err(ModelarDbError::ConfigurationError(
                "Storage engine is not configured to transfer data.".to_owned(),
            ))
        }
    }
}
