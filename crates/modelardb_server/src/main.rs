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

#![allow(clippy::too_many_arguments)]

mod configuration;
mod context;
mod manager;
mod optimizer;
mod query;
mod remote;
mod storage;

use std::path::PathBuf;
use std::sync::Arc;
use std::{env, fs};

use modelardb_common::arguments::{collect_command_line_arguments, validate_remote_data_folder};
use modelardb_common::metadata::TableMetadataManager;
use modelardb_common::types::ServerMode;
use object_store::{local::LocalFileSystem, ObjectStore};
use once_cell::sync::Lazy;
use sqlx::Postgres;
use tokio::runtime::Runtime;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::context::Context;
use crate::manager::Manager;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// The port of the Apache Arrow Flight Server. If the environment variable is not set, 9999 is used.
pub static PORT: Lazy<u16> =
    Lazy::new(|| env::var("MODELARDBD_PORT").map_or(9999, |value| value.parse().unwrap()));

/// The different possible modes that a ModelarDB server can be deployed in, assigned when the
/// server is started.
#[derive(Clone, Debug, PartialEq)]
pub enum ClusterMode {
    SingleNode,
    MultiNode(Manager),
}

impl ClusterMode {
    /// Return the optional remote table metadata manager from the manager interface if the cluster
    /// mode is `MultiNode`, otherwise return [`None`].
    fn remote_table_metadata_manager(&self) -> &Option<Arc<TableMetadataManager<Postgres>>> {
        match self {
            ClusterMode::SingleNode => &None,
            ClusterMode::MultiNode(manager) => &manager.table_metadata_manager,
        }
    }
}

/// Folders for storing metadata and Apache Parquet files.
pub struct DataFolders {
    /// Folder for storing metadata and Apache Parquet files on the local file system.
    pub local_data_folder: PathBuf, // PathBuf to support complex operations.
    /// Folder for storing Apache Parquet files in a remote object store.
    pub remote_data_folder: Option<Arc<dyn ObjectStore>>,
    /// Folder from which Apache Parquet files will be read during query execution. It is equivalent
    /// to `local_data_folder` when deployed on the edge and `remote_data_folder` when deployed
    /// in the cloud.
    pub query_data_folder: Arc<dyn ObjectStore>,
}

/// Setup tracing that prints to stdout, parse the command line arguments to extract [`DataFolders`],
/// construct a [`Context`] with the systems components, initialize the tables and model tables in
/// the metadata database, initialize a CTRL+C handler that flushes the data in memory to disk, and
/// start the Apache Arrow Flight interface. Returns [`String`] if the command line arguments cannot
/// be parsed, if the metadata cannot be read from the database, or if the Apache Arrow Flight
/// interface cannot be started.
fn main() -> Result<(), String> {
    // Initialize a tracing layer that logs events to stdout.
    let stdout_log = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry().with(stdout_log).init();

    // Create a Tokio runtime for executing asynchronous tasks. The runtime is not in the context so
    // it can be passed to the components in the context.
    let runtime = Arc::new(
        Runtime::new().map_err(|error| format!("Unable to create a Tokio Runtime: {error}"))?,
    );

    let arguments = collect_command_line_arguments(3);
    let arguments: Vec<&str> = arguments.iter().map(|arg| arg.as_str()).collect();
    let (server_mode, cluster_mode, data_folders) =
        runtime.block_on(parse_command_line_arguments(&arguments))?;

    // If a remote data folder was provided, check that it can be accessed.
    if let Some(remote_data_folder) = &data_folders.remote_data_folder {
        runtime.block_on(validate_remote_data_folder(remote_data_folder))?;
    }

    let context = Arc::new(
        runtime
            .block_on(Context::try_new(
                runtime.clone(),
                &data_folders,
                cluster_mode.clone(),
                server_mode,
            ))
            .map_err(|error| format!("Unable to create a Context: {error}"))?,
    );

    // Register tables and model tables.
    runtime
        .block_on(context.register_tables())
        .map_err(|error| format!("Unable to register tables: {error}"))?;

    runtime
        .block_on(context.register_model_tables(&context))
        .map_err(|error| format!("Unable to register model tables: {error}"))?;

    if let ClusterMode::MultiNode(manager) = &cluster_mode {
        runtime
            .block_on(manager.retrieve_and_create_tables(&context))
            .map_err(|error| format!("Unable to register manager tables: {error}"))?;
    }

    // Setup CTRL+C handler.
    setup_ctrl_c_handler(&context, &runtime);

    // Initialize storage engine with spilled buffers.
    runtime
        .block_on(async {
            context
                .storage_engine
                .read()
                .await
                .initialize(data_folders.local_data_folder, &context)
                .await
        })
        .map_err(|error| error.to_string())?;

    // Start the Apache Arrow Flight interface.
    remote::start_apache_arrow_flight_server(context, &runtime, *PORT)
        .map_err(|error| error.to_string())?;

    Ok(())
}

/// Parse the command line arguments into a [`ServerMode`], a [`ClusterMode`] and an instance of
/// [`DataFolders`]. If the necessary command line arguments are not provided, too many arguments
/// are provided, or if the arguments are malformed, [`String`] is returned.
async fn parse_command_line_arguments(
    arguments: &[&str],
) -> Result<(ServerMode, ClusterMode, DataFolders), String> {
    // Match the provided command line arguments to the supported inputs.
    match arguments {
        &["edge", local_data_folder] | &[local_data_folder] => Ok((
            ServerMode::Edge,
            ClusterMode::SingleNode,
            DataFolders {
                local_data_folder: argument_to_local_data_folder_path_buf(local_data_folder)?,
                remote_data_folder: None,
                query_data_folder: argument_to_local_object_store(local_data_folder)?,
            },
        )),
        &["cloud", local_data_folder, manager_url] => {
            let (manager, remote_object_store) =
                Manager::register_node(manager_url, ServerMode::Cloud)
                    .await
                    .map_err(|error| error.to_string())?;

            Ok((
                ServerMode::Cloud,
                ClusterMode::MultiNode(manager),
                DataFolders {
                    local_data_folder: argument_to_local_data_folder_path_buf(local_data_folder)?,
                    remote_data_folder: Some(remote_object_store.clone()),
                    query_data_folder: remote_object_store,
                },
            ))
        }
        &["edge", local_data_folder, manager_url] | &[local_data_folder, manager_url] => {
            let (manager, remote_object_store) =
                Manager::register_node(manager_url, ServerMode::Edge)
                    .await
                    .map_err(|error| error.to_string())?;

            Ok((
                ServerMode::Edge,
                ClusterMode::MultiNode(manager),
                DataFolders {
                    local_data_folder: argument_to_local_data_folder_path_buf(local_data_folder)?,
                    remote_data_folder: Some(remote_object_store),
                    query_data_folder: argument_to_local_object_store(local_data_folder)?,
                },
            ))
        }
        _ => {
            // The errors are consciously ignored as the program is terminating.
            let binary_path = std::env::current_exe().unwrap();
            let binary_name = binary_path.file_name().unwrap().to_str().unwrap();
            Err(format!(
                "Usage: {binary_name} [server_mode] local_data_folder [manager_url]."
            ))
        }
    }
}

/// Create a [`PathBuf`] that represents the path to the local data folder in `argument` and ensure
/// that the folder exists.
fn argument_to_local_data_folder_path_buf(argument: &str) -> Result<PathBuf, String> {
    let local_data_folder = PathBuf::from(argument);

    // Ensure the local data folder can be accessed as LocalFileSystem cannot
    // canonicalize the folder to the filesystem root if it does not exist.
    fs::create_dir_all(&local_data_folder).map_err(|error| {
        format!(
            "Unable to create {}: {}",
            local_data_folder.to_string_lossy(),
            error
        )
    })?;

    Ok(local_data_folder)
}

/// Create an [`ObjectStore`] that represents the local path in `argument`.
fn argument_to_local_object_store(argument: &str) -> Result<Arc<dyn ObjectStore>, String> {
    let object_store =
        LocalFileSystem::new_with_prefix(argument).map_err(|error| error.to_string())?;
    Ok(Arc::new(object_store))
}

/// Register a handler to execute when CTRL+C is pressed. The handler takes an exclusive lock for
/// the storage engine, flushes the data the storage engine currently buffers, and terminates the
/// system without releasing the lock.
fn setup_ctrl_c_handler(context: &Arc<Context>, runtime: &Arc<Runtime>) {
    let ctrl_c_context = context.clone();
    runtime.spawn(async move {
        // Errors are consciously ignored as the program should terminate if the handler cannot be
        // registered as buffers otherwise cannot be flushed.
        tokio::signal::ctrl_c().await.unwrap();

        // Stop the threads in the storage engine and close it.
        ctrl_c_context.storage_engine.write().await.close().unwrap();

        std::process::exit(0)
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for parse_command_line_arguments().
    #[tokio::test]
    async fn test_parse_empty_command_line_arguments() {
        assert!(parse_command_line_arguments(&[]).await.is_err());
    }

    #[tokio::test]
    async fn test_parse_edge_command_line_arguments_without_manager() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_str = temp_dir.path().to_str().unwrap();

        assert_single_edge_without_remote_data_folder(temp_dir_str, &["edge", temp_dir_str]).await;
    }

    #[tokio::test]
    async fn test_parse_edge_command_line_arguments_without_server_mode_and_manager() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_str = temp_dir.path().to_str().unwrap();

        assert_single_edge_without_remote_data_folder(temp_dir_str, &[temp_dir_str]).await;
    }

    async fn assert_single_edge_without_remote_data_folder(temp_dir_str: &str, input: &[&str]) {
        let (server_mode, cluster_mode, data_folders) =
            parse_command_line_arguments(input).await.unwrap();

        assert_eq!(server_mode, ServerMode::Edge);
        assert_eq!(cluster_mode, ClusterMode::SingleNode);
        assert_eq!(data_folders.local_data_folder, PathBuf::from(temp_dir_str));
        assert!(data_folders.remote_data_folder.is_none());
    }

    #[tokio::test]
    async fn test_parse_incomplete_cloud_command_line_arguments() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_str = temp_dir.path().to_str().unwrap();

        assert!(parse_command_line_arguments(&["cloud", temp_dir_str])
            .await
            .is_err())
    }
}
