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

//! Implementation of ModelarDB's main function and a [`Context`] type that
//! provides access to all of the system's components.

#![allow(clippy::too_many_arguments)]

mod common_test;
mod metadata;
mod optimizer;
mod parser;
mod query;
mod remote;
mod storage;
mod configuration;

use std::path::PathBuf;
use std::sync::Arc;
use std::{env, fs};

use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::execution::runtime_env::RuntimeEnv;
use modelardb_common::arguments::{
    argument_to_remote_object_store, collect_command_line_arguments,
    validate_remote_data_folder_from_argument,
};
use object_store::{local::LocalFileSystem, ObjectStore};
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::metadata::MetadataManager;
use crate::storage::StorageEngine;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// The port of the Apache Arrow Flight Server. If the environment variable is not set, 9999 is used.
pub static PORT: Lazy<u16> = Lazy::new(|| match env::var("MODELARDBD_PORT") {
    Ok(port) => port
        .parse()
        .map_err(|_| "MODELARDBD_PORT must be between 1 and 65535.")
        .unwrap(),
    Err(_) => 9999,
});

/// The different possible modes of a ModelarDB server, assigned when the server is started.
#[derive(Clone, PartialEq, Eq)]
pub enum ServerMode {
    Cloud,
    Edge,
}

/// Folders for storing metadata and Apache Parquet files.
pub struct DataFolders {
    /// Folder for storing metadata and Apache Parquet files on the local file
    /// system.
    pub local_data_folder: PathBuf, // PathBuf to support complex operations.
    /// Folder for storing Apache Parquet files in a remote object store.
    pub remote_data_folder: Option<Arc<dyn ObjectStore>>,
    /// Folder from which Apache Parquet files will be read during query
    /// execution. It is equivalent to `local_data_folder` when deployed on the
    /// edge and `remote_data_folder` when deployed in the cloud.
    pub query_data_folder: Arc<dyn ObjectStore>,
}

/// Provides access to the system's configuration and components.
pub struct Context {
    /// Metadata for the tables and model tables in the data folder.
    pub metadata_manager: MetadataManager,
    /// Main interface for Apache Arrow DataFusion.
    pub session: SessionContext,
    /// Manages all uncompressed and compressed data in the system.
    pub storage_engine: RwLock<StorageEngine>,
}

/// Setup tracing that prints to stdout, parse the command line arguments to
/// extract [`DataFolders`], construct a [`Context`] with the systems
/// components, initialize the tables and model tables in the metadata database,
/// initialize a CTRL+C handler that flushes the data in memory to disk, and
/// start the Apache Arrow Flight interface. Returns [`String`] if the command
/// line arguments cannot be parsed, if the metadata cannot be read from the
/// database, or if the Apache Arrow Flight interface cannot be started.
fn main() -> Result<(), String> {
    // Initialize a tracing layer that logs events to stdout.
    let stdout_log = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry().with(stdout_log).init();

    let arguments = collect_command_line_arguments(3);
    let arguments: Vec<&str> = arguments.iter().map(|arg| arg.as_str()).collect();
    let (server_mode, data_folders) = parse_command_line_arguments(&arguments)?;

    // Create a Tokio runtime for executing asynchronous tasks. The runtime is
    // not in the context so it can be passed to the components in the context.
    let runtime = Arc::new(
        Runtime::new().map_err(|error| format!("Unable to create a Tokio Runtime: {error}"))?,
    );

    // If a remote data folder was provided, check that it can be accessed. This check is performed
    // after parse_command_line_arguments() as the Tokio Runtime is required.
    if let Some(remote_data_folder) = &data_folders.remote_data_folder {
        runtime.block_on(async {
            validate_remote_data_folder_from_argument(arguments.get(2).unwrap(), remote_data_folder)
                .await
        })?;
    }

    // Create the components for the Context.
    let metadata_manager = runtime
        .block_on(MetadataManager::try_new(
            &data_folders.local_data_folder,
            server_mode,
        ))
        .map_err(|error| format!("Unable to create a MetadataManager: {error}"))?;
    let session = create_session_context(data_folders.query_data_folder);
    let storage_engine = RwLock::new(
        runtime
            .block_on(async {
                StorageEngine::try_new(
                    data_folders.local_data_folder,
                    data_folders.remote_data_folder,
                    metadata_manager.clone(),
                    true,
                )
                .await
            })
            .map_err(|error| error.to_string())?,
    );

    // Create the Context.
    let context = Arc::new(Context {
        metadata_manager,
        session,
        storage_engine,
    });

    // Register tables and model tables.
    runtime
        .block_on(context.metadata_manager.register_tables(&context))
        .map_err(|error| format!("Unable to register tables: {error}"))?;

    runtime
        .block_on(context.metadata_manager.register_model_tables(&context))
        .map_err(|error| format!("Unable to register model tables: {error}"))?;

    // Setup CTRL+C handler.
    setup_ctrl_c_handler(&context, &runtime);

    // Start the Apache Arrow Flight interface.
    remote::start_apache_arrow_flight_server(context, &runtime, *PORT)
        .map_err(|error| error.to_string())?;

    Ok(())
}

/// Parse the command lines arguments into a [`ServerMode`] and an instance of [`DataFolders`]. If
/// the necessary command line arguments are not provided, too many arguments are provided, or
/// if the arguments are malformed, [`String`] is returned.
fn parse_command_line_arguments(arguments: &[&str]) -> Result<(ServerMode, DataFolders), String> {
    // Match the provided command line arguments to the supported inputs.
    match arguments {
        &["cloud", local_data_folder, remote_data_folder] => Ok((
            ServerMode::Cloud,
            DataFolders {
                local_data_folder: argument_to_local_data_folder_path_buf(local_data_folder)?,
                remote_data_folder: Some(argument_to_remote_object_store(remote_data_folder)?),
                query_data_folder: argument_to_remote_object_store(remote_data_folder)?,
            },
        )),
        &["edge", local_data_folder, remote_data_folder] => Ok((
            ServerMode::Edge,
            DataFolders {
                local_data_folder: argument_to_local_data_folder_path_buf(local_data_folder)?,
                remote_data_folder: Some(argument_to_remote_object_store(remote_data_folder)?),
                query_data_folder: argument_to_local_object_store(local_data_folder)?,
            },
        )),
        &["edge", local_data_folder] | &[local_data_folder] => Ok((
            ServerMode::Edge,
            DataFolders {
                local_data_folder: argument_to_local_data_folder_path_buf(local_data_folder)?,
                remote_data_folder: None,
                query_data_folder: argument_to_local_object_store(local_data_folder)?,
            },
        )),
        _ => {
            // The errors are consciously ignored as the program is terminating.
            let binary_path = std::env::current_exe().unwrap();
            let binary_name = binary_path.file_name().unwrap().to_str().unwrap();
            Err(format!(
                "Usage: {binary_name} [mode] local_data_folder [remote_data_folder]."
            ))
        }
    }
}

/// Create a [`PathBuf`] that represents the path to the local data folder in
/// `argument` and ensure that the folder exists.
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

/// Create a new [`SessionContext`] for interacting with Apache Arrow
/// DataFusion. The [`SessionContext`] is constructed with the default
/// configuration, default resource managers, the local file system and if
/// provided the remote object store as [`ObjectStores`](ObjectStore), and
/// additional optimizer rules that rewrite simple aggregate queries to be
/// executed directly on the segments containing metadata and models instead of
/// on reconstructed data points created from the segments for model tables.
fn create_session_context(query_data_folder: Arc<dyn ObjectStore>) -> SessionContext {
    let session_config = SessionConfig::new();
    let session_runtime = Arc::new(RuntimeEnv::default());

    // unwrap() is safe as storage::QUERY_DATA_FOLDER_SCHEME_WITH_HOST is a const containing an URL.
    let object_store_url = storage::QUERY_DATA_FOLDER_SCHEME_WITH_HOST
        .try_into()
        .unwrap();
    session_runtime.register_object_store(&object_store_url, query_data_folder);

    // Use the add* methods instead of the with* methods as the with* methods replace the built-ins.
    // See: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionState.html
    let mut session_state = SessionState::with_config_rt(session_config, session_runtime);
    for physical_optimizer_rule in optimizer::physical_optimizer_rules() {
        session_state = session_state.add_physical_optimizer_rule(physical_optimizer_rule);
    }

    SessionContext::with_state(session_state)
}

/// Register a handler to execute when CTRL+C is pressed. The handler takes an
/// exclusive lock for the storage engine, flushes the data the storage engine
/// currently buffers, and terminates the system without releasing the lock.
fn setup_ctrl_c_handler(context: &Arc<Context>, runtime: &Arc<Runtime>) {
    let ctrl_c_context = context.clone();
    runtime.spawn(async move {
        // Errors are consciously ignored as the program should terminate if the
        // handler cannot be registered as buffers otherwise cannot be flushed.
        tokio::signal::ctrl_c().await.unwrap();
        ctrl_c_context
            .storage_engine
            .write()
            .await
            .flush()
            .await
            .unwrap();
        std::process::exit(0)
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::env;

    // Tests for parse_command_line_arguments().
    #[test]
    fn test_parse_empty_command_line_arguments() {
        assert!(parse_command_line_arguments(&[]).is_err());
    }

    #[test]
    fn test_parse_cloud_command_line_arguments() {
        setup_environment();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_str = tempdir.path().to_str().unwrap();
        let (local_data_folder, remote_data_folder) =
            new_data_folders(&["cloud", tempdir_str, "s3://bucket"]);

        // Equals cannot be applied to type dyn object_store::ObjectStore.
        assert_eq!(local_data_folder, PathBuf::from(tempdir_str));
        remote_data_folder.unwrap();
    }

    #[test]
    fn test_parse_incomplete_cloud_command_line_arguments() {
        assert!(parse_command_line_arguments(&["cloud", "s3://bucket"]).is_err())
    }

    #[test]
    fn test_parse_edge_full_command_line_arguments() {
        setup_environment();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_str = tempdir.path().to_str().unwrap();
        let (local_data_folder, remote_data_folder) =
            new_data_folders(&["edge", tempdir_str, "s3://bucket"]);

        // Equals cannot be applied to type dyn object_store::ObjectStore.
        assert_eq!(local_data_folder, PathBuf::from(tempdir_str));
        remote_data_folder.unwrap();
    }

    #[test]
    fn test_parse_edge_command_line_arguments_without_remote_object_store() {
        setup_environment();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_str = tempdir.path().to_str().unwrap();
        let (local_data_folder, remote_data_folder) = new_data_folders(&["edge", tempdir_str]);

        // Equals cannot be applied to type dyn object_store::ObjectStore.
        assert_eq!(local_data_folder, PathBuf::from(tempdir_str));
        assert!(remote_data_folder.is_none());
    }

    #[test]
    fn test_parse_edge_command_line_arguments_without_mode_and_remote_object_store() {
        setup_environment();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_str = tempdir.path().to_str().unwrap();
        let (local_data_folder, remote_data_folder) = new_data_folders(&[tempdir_str]);

        // Equals cannot be applied to type dyn object_store::ObjectStore.
        assert_eq!(local_data_folder, PathBuf::from(tempdir_str));
        assert!(remote_data_folder.is_none());
    }

    #[test]
    fn test_parse_incomplete_edge_command_line_arguments() {
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_str = tempdir.path().to_str().unwrap();
        let input = &[tempdir_str, "s3://bucket"];

        assert!(parse_command_line_arguments(input).is_err())
    }

    fn setup_environment() {
        env::set_var("AWS_DEFAULT_REGION", "");
    }

    fn new_data_folders(input: &[&str]) -> (PathBuf, Option<Arc<dyn ObjectStore>>) {
        let (_, data_folders) = parse_command_line_arguments(input).unwrap();

        (
            data_folders.local_data_folder,
            data_folders.remote_data_folder,
        )
    }
}
