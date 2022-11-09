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

mod compression;
mod errors;
mod macros;
mod metadata;
mod models;
mod optimizer;
mod parser;
mod remote;
mod storage;
mod tables;
mod types;

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::execution::runtime_env::RuntimeEnv;
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem, ObjectStore};
use parking_lot::RwLock;
use tokio::runtime::Runtime;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::metadata::MetadataManager;
use crate::optimizer::model_simple_aggregates;
use crate::storage::StorageEngine;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// Provides access to the system's configuration and components.
pub struct Context {
    /// Metadata for the tables and model tables in the data folder.
    pub metadata_manager: MetadataManager,
    /// Main interface for Apache Arrow DataFusion.
    pub session: SessionContext,
    /// Manages all uncompressed and compressed data in the system.
    pub storage_engine: RwLock<StorageEngine>,
}

/// Setup tracing, construct a `Context`, initialize tables and model tables in
/// the path given by the first command line argument, initialize a CTRL+C
/// handler that flushes the data in memory to disk, and start the Apache Arrow
/// Flight interface. Returns [`Error`] formatted as a [`String`] if metadata
/// cannot be read or the Apache Arrow Flight interface cannot be started.
fn main() -> Result<(), String> {
    // A layer that logs events to stdout.
    let stdout_log = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry().with(stdout_log).init();

    let mut args = std::env::args();
    args.next(); // Skip executable.

    // Collect at most the maximum number of command line arguments plus one.
    // The plus one argument is collected to trigger the default pattern in
    // parse_command_line_arguments() if too many arguments are passed without
    // collecting more command line arguments than required for that pattern.
    let arguments_required: Vec<String> = args.by_ref().take(4).collect();
    let arguments_required: Vec<&str> = arguments_required.iter().map(|arg| arg.as_str()).collect();
    let (local_data_folder, maybe_blob_store_data_folder, query_data_folder) =
        parse_command_line_arguments(&arguments_required)?;

    // Ensure the local data folder exists before making it a LocalFileSystem.
    let data_folder_path = PathBuf::from(local_data_folder);
    fs::create_dir_all(data_folder_path.as_path())
        .map_err(|error| format!("Unable to create {}: {}", local_data_folder, error))?;
    let local_data_folder = command_line_argument_to_local_folder(local_data_folder);

    // Create a Tokio runtime for executing asynchronous tasks. The runtime is
    // not part of the context to make it easier to pass the runtime to the
    // components in the context.
    let runtime = Arc::new(
        Runtime::new().map_err(|error| format!("Unable to create a Tokio Runtime: {}", error))?,
    );

    // Create Context components.
    let metadata_manager = MetadataManager::try_new(&data_folder_path)
        .map_err(|error| format!("Unable to create a MetadataManager: {}", error))?;
    let session = create_session_context();
    let storage_engine = RwLock::new(StorageEngine::new(
        data_folder_path.clone(),
        metadata_manager.clone(),
        true,
    ));

    // Create Context.
    let context = Arc::new(Context {
        metadata_manager,
        session,
        storage_engine,
    });

    // Register tables and model tables.
    context
        .metadata_manager
        .register_tables(&context, &runtime)
        .map_err(|error| format!("Unable to register tables: {}", error))?;

    context
        .metadata_manager
        .register_model_tables(&context)
        .map_err(|error| format!("Unable to register model tables: {}", error))?;

    // Setup CTRL+C handler.
    setup_ctrl_c_handler(&context, &runtime);

    // Start Interface.
    remote::start_arrow_flight_server(context, &runtime, 9999)
        .map_err(|error| error.to_string())?;

    Ok(())
}

/// Parse the command lines arguments into a triple containing the data folder
/// to use on the local file system, the data folder to use on blob store if it
/// was specified, and the data folder to use when executing queries. If the
/// necessary command line arguments are not provided, too many arguments are
/// provided, or if the provided arguments are malformed, [`None`] is returned.
fn parse_command_line_arguments<'a>(
    arguments: &'a [&'a str],
) -> Result<(&'a str, Option<Box<dyn ObjectStore>>, Box<dyn ObjectStore>), String> {
    // Match the provided command line arguments to the supported inputs.
    match arguments {
        &["cloud", local_data_folder, blob_store_data_folder] => Ok((
            local_data_folder,
            Some(command_line_argument_to_blob_store(blob_store_data_folder)?),
            command_line_argument_to_blob_store(blob_store_data_folder)?,
        )),
        &["edge", local_data_folder, blob_store_data_folder] => Ok((
            local_data_folder,
            Some(command_line_argument_to_blob_store(blob_store_data_folder)?),
            command_line_argument_to_local_folder(local_data_folder)?,
        )),
        &["edge", local_data_folder] => Ok((
            local_data_folder,
            None,
            command_line_argument_to_local_folder(local_data_folder)?,
        )),
        &[local_data_folder] => Ok((
            local_data_folder,
            None,
            command_line_argument_to_local_folder(local_data_folder)?,
        )),
        _ => {
            // The errors are consciously ignored as the program is terminating.
            let binary_path = std::env::current_exe().unwrap();
            let binary_name = binary_path.file_name().unwrap().to_str().unwrap();
            Err(format!(
                "Usage: {} [mode] local_data_folder [blob_store_data_folder].",
                binary_name
            ))
        }
    }
}

fn command_line_argument_to_local_folder(argument: &str) -> Result<Box<dyn ObjectStore>, String> {
    let object_store =
        LocalFileSystem::new_with_prefix(argument).map_err(|error| error.to_string())?;
    Ok(Box::new(object_store))
}

fn command_line_argument_to_blob_store(argument: &str) -> Result<Box<dyn ObjectStore>, String> {
    if let Some(bucket_name) = argument.strip_prefix("s3://") {
        let object_store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Box::new(object_store))
    } else {
        Err("Blob store must be s3://.".to_owned())
    }
}

/// Create a new `SessionContext` for interacting with Apache Arrow
/// DataFusion. The `SessionContext` is constructed with the default
/// configuration, default resource managers, the local file system as the
/// only object store, and additional optimizer rules that rewrite simple
/// aggregate queries to be executed directly on the models instead of on
/// reconstructed data points for model tables.
fn create_session_context() -> SessionContext {
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::with_config_rt(config, runtime).with_physical_optimizer_rules(vec![
        Arc::new(model_simple_aggregates::ModelSimpleAggregatesPhysicalOptimizerRule {}),
    ]);
    SessionContext::with_state(state)
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
        ctrl_c_context.storage_engine.write().flush();
        std::process::exit(0)
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::env;

    use tempfile;

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
        let input = &["cloud", tempdir_str, "s3://bucket"];

        let (local_data_folder, blob_store_data_folder, _query_data_folder) =
            parse_command_line_arguments(input).unwrap();

        // Equals cannot be applied to type dyn object_store::ObjectStore.
        assert_eq!(local_data_folder, tempdir_str);
        blob_store_data_folder.unwrap();
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
        let input = &["edge", tempdir_str, "s3://bucket"];

        let (local_data_folder, blob_store_data_folder, _query_data_folder) =
            parse_command_line_arguments(input).unwrap();

        // Equals cannot be applied to type dyn object_store::ObjectStore.
        assert_eq!(local_data_folder, tempdir_str);
        blob_store_data_folder.unwrap();
    }

    #[test]
    fn test_parse_edge_command_line_arguments_without_blob_store() {
        setup_environment();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_str = tempdir.path().to_str().unwrap();
        let input = &["edge", tempdir_str];

        let (local_data_folder, blob_store_data_folder, _query_data_folder) =
            parse_command_line_arguments(input).unwrap();

        // Equals cannot be applied to type dyn object_store::ObjectStore.
        assert_eq!(local_data_folder, tempdir_str);
        assert!(blob_store_data_folder.is_none());
    }

    #[test]
    fn test_parse_edge_command_line_arguments_without_mode_and_blob_store() {
        setup_environment();
        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_str = tempdir.path().to_str().unwrap();
        let input = &[tempdir_str];

        let (local_data_folder, blob_store_data_folder, _query_data_folder) =
            parse_command_line_arguments(input).unwrap();

        // Equals cannot be applied to type dyn object_store::ObjectStore.
        assert_eq!(local_data_folder, tempdir_str);
        assert!(blob_store_data_folder.is_none());
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
}
