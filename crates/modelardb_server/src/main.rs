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

use std::result::Result as StdResult;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use modelardb_storage::parser;
use modelardb_types::types::CloudCredentials;
use sysinfo::System;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::cluster::ClusterMode;
use crate::context::Context;
use crate::data_folders::DataFolders;
use crate::error::Result;

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

    /// Amount of memory as an absolute memory amount or percentage of system memory to reserve for storing ingested time series.
    #[arg(long = "ingested-reserved-memory", value_name = "INGESTED_RESERVED_MEMORY", env = "MODELARDBD_INGESTED_RESERVED_MEMORY", value_parser = parse_memory_size_argument)]
    ingested_reserved_memory_in_bytes: Option<u64>,

    /// Amount of memory as an absolute memory amount or percentage of system memory for storing uncompressed data buffers.
    #[arg(long = "uncompressed-reserved-memory", value_name = "UNCOMPRESSED_RESERVED_MEMORY", env = "MODELARDBD_UNCOMPRESSED_RESERVED_MEMORY", value_parser = parse_memory_size_argument)]
    uncompressed_reserved_memory_in_bytes: Option<u64>,

    /// Amount of memory as an absolute memory amount or percentage of system memory for storing compressed data buffers.
    #[arg(long = "compressed-reserved-memory", value_name = "COMPRESSED_RESERVED_MEMORY", env = "MODELARDBD_COMPRESSED_RESERVED_MEMORY", value_parser = parse_memory_size_argument)]
    compressed_reserved_memory_in_bytes: Option<u64>,

    /// Number of bytes required before transferring a batch of data to the remote object store.
    /// If not set, data is only transferred on an explicit flush.
    #[arg(long, env = "MODELARDBD_TRANSFER_BATCH_SIZE_IN_BYTES")]
    transfer_batch_size_in_bytes: Option<u64>,

    /// Approximate maximum size in bytes of a single WAL segment file before a new one is started.
    /// The size is approximate since the in-memory size of each batch is used instead of its
    /// on-disk size to avoid the overhead of reading the file size after each write.
    #[arg(long, env = "MODELARDBD_SEGMENT_SIZE_THRESHOLD_IN_BYTES")]
    segment_size_threshold_in_bytes: Option<u64>,

    /// Target size, in bytes, of the files produced when automatically compacting a table's
    /// storage. This is also the default value used when an OPTIMIZE query is executed without an
    /// explicit target size.
    #[arg(long, env = "MODELARDBD_OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES")]
    optimize_target_file_size_in_bytes: Option<u64>,

    /// Retention period, in seconds, used when automatically vacuuming a table during compaction.
    /// This is also the default value used when a VACUUM query is executed without an explicit
    /// retention period. Note that a very low value can delete files an in-progress query is still
    /// scanning.
    #[arg(long, env = "MODELARDBD_VACUUM_RETENTION_PERIOD_IN_SECONDS")]
    vacuum_retention_period_in_seconds: Option<u64>,

    /// Whether the write-ahead log is enabled.
    #[arg(long, env = "MODELARDBD_WAL_ENABLED")]
    wal_enabled: Option<bool>,

    /// Subcommand specifying the mode the server is started in and the required data folders.
    #[command(subcommand)]
    mode: ServerMode,
}

/// Parse a value followed by a percentage or a unit (B, KB, KiB, MB, MiB, GB, GiB, TB, TiB,
/// case-insensitive). The function is designed to be used with the `clap` crate, and the error is
/// returned as a string to for control and avoid depending on `sqlparser` for its `ParserError`.
/// - If a percentage is given, that percentage of the system's total memory in bytes is returned.
/// - If a unit, the specified amount of memory in bytes is returned.
/// - If a parse error occurs, that error is returned as a `String`.
fn parse_memory_size_argument(input: &str) -> StdResult<u64, String> {
    let input = input.trim();
    let suffix_start = input
        .chars()
        .position(|c| !c.is_numeric())
        .unwrap_or(input.len());

    let value: u64 = input[0..suffix_start]
        .parse::<u64>()
        .map_err(|error| error.to_string())?;
    let suffix = input[suffix_start..input.len()].trim();

    let memory_in_bytes = if suffix == "%" {
        let mut system = System::new();
        system.refresh_memory();
        let total_memory_bytes = system.total_memory();

        // Cast to u128 to avoid overflowing when multiplying.
        ((total_memory_bytes as u128 * value as u128) / 100) as u64
    } else {
        value * parser::byte_unit_multiplier(suffix).map_err(|error| error.to_string())?
    };

    Ok(memory_in_bytes)
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
    remote::start_apache_arrow_flight_server(context, None, args.port).await?;

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

#[cfg(test)]
mod test {
    use super::*;

    // Tests for parse_memory_size_argument().
    #[test]
    fn test_parse_memory_size_argument_empty_string() {
        assert!(parse_memory_size_argument("").is_err())
    }

    #[test]
    fn test_parse_memory_size_argument_no_suffix() {
        assert_eq!(parse_memory_size_argument("37").unwrap(), 37)
    }

    #[test]
    fn test_parse_memory_size_argument_no_suffix_with_whitespace() {
        // ast-grep-ignore as the extra spaces are purposely added for testing.
        assert_eq!(parse_memory_size_argument("  37  ").unwrap(), 37)
    }

    #[test]
    fn test_parse_memory_size_argument_byte_suffix() {
        assert_eq!(parse_memory_size_argument("37B").unwrap(), 37)
    }

    #[test]
    fn test_parse_memory_size_argument_byte_suffix_with_whitespac() {
        // ast-grep-ignore as the extra spaces are purposely added for testing.
        assert_eq!(parse_memory_size_argument(" 37   B  ").unwrap(), 37)
    }

    #[test]
    fn test_parse_memory_size_argument_kilobyte_suffix() {
        assert_eq!(parse_memory_size_argument("37KB").unwrap(), 37000)
    }

    #[test]
    fn test_parse_memory_size_argument_kibibyte_suffix() {
        assert_eq!(parse_memory_size_argument("37KiB").unwrap(), 37888)
    }

    #[test]
    fn test_parse_memory_size_argument_megabyte_suffix() {
        assert_eq!(parse_memory_size_argument("37MB").unwrap(), 37000000)
    }

    #[test]
    fn test_parse_memory_size_argument_mebibyte_suffix() {
        assert_eq!(parse_memory_size_argument("37MiB").unwrap(), 38797312)
    }

    #[test]
    fn test_parse_memory_size_argument_gigabyte_suffix() {
        assert_eq!(parse_memory_size_argument("37GB").unwrap(), 37000000000)
    }

    #[test]
    fn test_parse_memory_size_argument_gibibyte_suffix() {
        assert_eq!(parse_memory_size_argument("37GiB").unwrap(), 39728447488)
    }

    #[test]
    fn test_parse_memory_size_argument_terabyte_suffix() {
        assert_eq!(parse_memory_size_argument("37TB").unwrap(), 37000000000000)
    }

    #[test]
    fn test_parse_memory_size_argument_tebibyte_suffix() {
        assert_eq!(parse_memory_size_argument("37TiB").unwrap(), 40681930227712)
    }

    #[test]
    fn test_parse_memory_size_argument_percentage() {
        // As the tests will be run on different systems, it is not possible to check if the correct
        // value is returned without doing the same calculation as parse_memory_size_argument().
        assert!(parse_memory_size_argument("10%").is_ok())
    }

    #[test]
    fn test_parse_memory_size_argument_wrong_suffix() {
        assert!(parse_memory_size_argument("10#").is_err())
    }
}
