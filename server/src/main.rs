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

    if let Some(data_folder) = args.next() {
        // Ensure the data folder exists.
        let data_folder_path = PathBuf::from(&data_folder);
        fs::create_dir_all(data_folder_path.as_path())
            .map_err(|error| format!("Unable to create {}: {}", &data_folder, error))?;

        // Create a Tokio runtime for executing asynchronous tasks. The runtime is not part of the
        // context to make it easier to pass the runtime to components in the context.
        let runtime = Arc::new(
            Runtime::new()
                .map_err(|error| format!("Unable to create a Tokio Runtime: {}", error))?,
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
            .map_err(|error| error.to_string())?
    } else {
        // The errors are consciously ignored as the program is terminating.
        let binary_path = std::env::current_exe().unwrap();
        let binary_name = binary_path.file_name().unwrap().to_str().unwrap();
        eprintln!("Usage: {} path_to_data_folder.", binary_name);
    }
    Ok(())
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
