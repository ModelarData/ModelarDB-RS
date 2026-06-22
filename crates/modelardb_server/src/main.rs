/* Copyright 2021 The ModelarDB Contributors
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

//! Implementation of ModelarDB's main function.

mod cluster;
mod configuration;
mod context;
mod data_folders;
mod error;
mod remote;
mod storage;

use std::sync::Arc;

use clap::{Parser, Subcommand};
use modelardb_types::types::CloudCredentials;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::cluster::Cluster;
use crate::context::Context;
use crate::data_folders::DataFolders;
use crate::error::Result;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// The different possible modes that a ModelarDB server can be deployed in, assigned when the
/// server is started.
#[derive(Clone)]
pub(crate) enum ClusterMode {
    SingleNode,
    MultiNode(Box<Cluster>),
}

/// Command line arguments for the ModelarDB server.
#[derive(Parser)]
#[command(
    about = "ModelarDB server",
    long_about = "ModelarDB server. Ingests and compresses data, stores it in a local data folder, \
    and optionally transfers it to a remote object store."
)]
pub(crate) struct ServerArgs {
    /// Host address the Apache Arrow Flight server listens on.
    #[arg(long, default_value = "127.0.0.1", env = "MODELARDBD_HOST")]
    host: String,

    /// Port the Apache Arrow Flight server listens on.
    #[arg(long, default_value_t = 9999, env = "MODELARDBD_PORT")]
    port: u16,

    /// Amount of memory in bytes to reserve for storing multivariate time series.
    #[arg(long, env = "MODELARDBD_MULTIVARIATE_RESERVED_MEMORY_IN_BYTES")]
    multivariate_reserved_memory_in_bytes: Option<u64>,

    /// Amount of memory in bytes to reserve for storing uncompressed data buffers.
    #[arg(long, env = "MODELARDBD_UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES")]
    uncompressed_reserved_memory_in_bytes: Option<u64>,

    /// Amount of memory in bytes to reserve for storing compressed data buffers.
    #[arg(long, env = "MODELARDBD_COMPRESSED_RESERVED_MEMORY_IN_BYTES")]
    compressed_reserved_memory_in_bytes: Option<u64>,

    /// Number of bytes required before transferring a batch of data to the remote object store.
    /// If not set, data is not transferred based on batch size.
    #[arg(long, env = "MODELARDBD_TRANSFER_BATCH_SIZE_IN_BYTES")]
    transfer_batch_size_in_bytes: Option<u64>,

    /// Number of seconds between each transfer of data to the remote object store. If not set,
    /// data is not transferred based on time.
    #[arg(long, env = "MODELARDBD_TRANSFER_TIME_IN_SECONDS")]
    transfer_time_in_seconds: Option<u64>,

    /// Approximate maximum size in bytes of a single WAL segment file before a new one is started.
    /// The size is approximate since the in-memory size of each batch is used instead of its
    /// on-disk size to avoid the overhead of reading the file size after each write.
    #[arg(long, env = "MODELARDBD_SEGMENT_SIZE_THRESHOLD_IN_BYTES")]
    segment_size_threshold_in_bytes: Option<u64>,

    /// Whether the write-ahead log is enabled.
    #[arg(long, env = "MODELARDBD_WAL_ENABLED")]
    wal_enabled: Option<bool>,

    /// Subcommand specifying the mode the server is started in and the required data folders.
    #[command(subcommand)]
    mode: ServerMode,
}

/// The mode and data folders a ModelarDB server is started with.
#[derive(Subcommand)]
pub(crate) enum ServerMode {
    /// Run as an edge node. Optionally connects to a remote object store to form a cluster.
    Edge {
        /// Path to the local data folder.
        local_data_folder: String,
        /// URL of the remote data folder (e.g., s3://bucket or azureblobstorage://container).
        /// If provided, this node joins a cluster and transfers data to the remote object store.
        remote_data_folder: Option<String>,
        /// Credentials for connecting to the remote data folder if it is provided.
        #[command(flatten)]
        credentials: CloudCredentials,
    },
    /// Run as a cloud node in a cluster. Queries are executed against the remote object store.
    Cloud {
        /// Path to the local data folder.
        local_data_folder: String,
        /// URL of the remote data folder (e.g., s3://bucket or azureblobstorage://container).
        remote_data_folder: String,
        /// Credentials for connecting to the remote data folder.
        #[command(flatten)]
        credentials: CloudCredentials,
    },
}

/// Setup tracing that prints to stdout, parse the command line arguments to extract
/// [`DataFolders`], construct a [`Context`] with the systems components, initialize the normal
/// tables and time series tables in the Delta Lake, initialize a CTRL+C handler that flushes the
/// data in memory to disk, and start the Apache Arrow Flight interface. Returns
/// [`ModelarDbServerError`](error::ModelarDbServerError) if the command line arguments cannot be
/// parsed, if the metadata cannot be read from the database, or if the Apache Arrow Flight
/// interface cannot be started.
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize a tracing layer that logs events to stdout.
    let stdout_log = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry().with(stdout_log).init();

    let args = ServerArgs::parse();

    let (cluster_mode, data_folders) =
        DataFolders::try_from_args(&args.mode, &args.host, args.port).await?;

    let context = Arc::new(Context::try_new(data_folders, cluster_mode.clone(), &args).await?);

    // Register normal tables and time series tables.
    context.register_normal_tables().await?;
    context.register_time_series_tables().await?;

    if let ClusterMode::MultiNode(cluster) = &cluster_mode {
        cluster.retrieve_and_create_tables(&context).await?;
    }

    // Setup CTRL+C handler.
    setup_ctrl_c_handler(&context);

    // Replay any data that was written to the storage engine but not compressed and saved to disk.
    context.replay_write_ahead_log().await?;

    // Start the Apache Arrow Flight interface.
    remote::start_apache_arrow_flight_server(context, args.port).await?;

    Ok(())
}

/// Register a handler to execute when CTRL+C is pressed. The handler takes an exclusive lock for
/// the storage engine, flushes the data the storage engine currently buffers, removes the node
/// from the cluster if necessary, and terminates the system without releasing the lock.
fn setup_ctrl_c_handler(context: &Arc<Context>) {
    let ctrl_c_context = context.clone();
    tokio::spawn(async move {
        // Errors are consciously ignored as the program should terminate if the handler cannot be
        // registered as buffers otherwise cannot be flushed.
        tokio::signal::ctrl_c().await.unwrap();

        // Stop the threads in the storage engine and close it.
        ctrl_c_context.storage_engine.write().await.close().unwrap();

        // If running in a cluster, remove the node from the remote data folder.
        let configuration_manager = ctrl_c_context.configuration_manager.read().await;
        if let ClusterMode::MultiNode(cluster) = configuration_manager.cluster_mode() {
            cluster.remove_node().await.unwrap();
        }

        std::process::exit(0)
    });
}
