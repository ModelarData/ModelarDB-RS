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

//! Implementation of ModelarDB's main function and a `Context` type that
//! provides access to the system's configuration and components throughout the
//! system.

mod catalog;
mod compression;
mod errors;
mod macros;
mod metadata;
mod models;
mod optimizer;
mod remote;
mod storage;
mod tables;
mod types;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use datafusion::common::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::execution::options::ParquetReadOptions;
use datafusion::execution::runtime_env::RuntimeEnv;
use rusqlite::Connection;
use tokio::runtime::Runtime;
use tracing::error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::catalog::Catalog;
use crate::metadata::MetadataManager;
use crate::optimizer::model_simple_aggregates;
use crate::storage::StorageEngine;
use crate::tables::ModelTable;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// Provides access to the system's configuration and components.
pub struct Context {
    /// Metadata for the tables and model tables in the data folder.
    metadata_manager: MetadataManager,
    catalog: RwLock<Catalog>,
    /// A Tokio runtime for executing asynchronous tasks.
    runtime: Runtime,
    /// Main interface for Apache Arrow DataFusion.
    session: SessionContext,
    /// Manages all uncompressed and compressed data in the system.
    storage_engine: RwLock<StorageEngine>,
}

/// Setup tracing, read table and model table metadata from the path given by
/// the first command line argument, construct a `Context`, and start Apache
/// Arrow Flight interface. Returns `Error` if the metadata cannot be read or
/// the Apache Arrow Flight interface cannot be started.
fn main() -> Result<(), String> {
    // A layer that logs events to stdout.
    let stdout_log = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry().with(stdout_log).init();

    let mut args = std::env::args();
    args.next(); // Skip executable.
    if let Some(data_folder) = args.next() {
        // Ensure the data folder exists before reading from it.
        let data_folder_path = PathBuf::from(&data_folder);
        fs::create_dir_all(data_folder_path.as_path()).map_err(|error| error.to_string())?;

        // Initialize the metadata manager with a path to the metadata database..
        let metadata_manager = MetadataManager::new(&data_folder_path);

        // Set up the metadata tables used for model tables.
        create_model_table_metadata_tables(data_folder_path.as_path())
            .map_err(|error| format!("Unable to create metadata tables: {}", error))?;

        // Build Context.
        let catalog = Catalog::try_new(&data_folder_path).map_err(|error| {
            format!("Unable to read data folder '{}': {}", &data_folder, &error)
        })?;
        let runtime = Runtime::new().unwrap();
        let session = create_session_context();
        let storage_engine = RwLock::new(StorageEngine::new(
            data_folder_path.clone(),
            &metadata_manager,
            true,
        ));

        // Create Context.
        let mut context = Arc::new(Context {
            metadata_manager,
            catalog: RwLock::new(catalog),
            runtime,
            session,
            storage_engine,
        });

        // Register Tables.
        register_tables_and_model_tables(&mut context);

        // Setup CTRL+C handler.
        setup_ctrl_c_handler(&context);

        // Start Interface.
        remote::start_arrow_flight_server(context, 9999).map_err(|error| error.to_string())?
    } else {
        // The errors are consciously ignored as the program is terminating.
        let binary_path = std::env::current_exe().unwrap();
        let binary_name = binary_path.file_name().unwrap().to_str().unwrap();
        eprintln!("Usage: {} path_to_data_folder.", binary_name);
    }
    Ok(())
}

/// Create a new `SessionContext` for interacting with Apache Arrow DataFusion.
/// The `SessionContext` is constructed with the default configuration, default
/// resource managers, the local file system as the only object store, and
/// additional optimizer rules that rewrite simple aggregate queries to be
/// executed directly on the models instead of on reconstructed data points for
/// model tables.
fn create_session_context() -> SessionContext {
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::with_config_rt(config, runtime).with_physical_optimizer_rules(vec![
        Arc::new(model_simple_aggregates::ModelSimpleAggregatesPhysicalOptimizerRule {}),
    ]);
    SessionContext::with_state(state)
}

// TODO: Move this function to the configuration/metadata component.
/// If they do not already exist, create the tables used for model table metadata. A
/// "model_table_metadata" table that can persist model tables is created. A "columns" table that
/// can save the index of field columns in specific tables is also created. If the tables already
/// exist or were successfully created, return [`Ok`], otherwise return [`rusqlite::Error`].
fn create_model_table_metadata_tables(data_folder_path: &Path) -> Result<(), rusqlite::Error> {
    let database_path = data_folder_path.join(catalog::METADATA_SQLITE_NAME);
    let connection = Connection::open(database_path)?;

    // Create the model_table_metadata SQLite table if it does not exist.
    connection.execute(
        "CREATE TABLE IF NOT EXISTS model_table_metadata (
                table_name TEXT PRIMARY KEY,
                schema BLOB NOT NULL,
                timestamp_column_index INTEGER NOT NULL,
                tag_column_indices BLOB NOT NULL
        ) STRICT",
        (),
    )?;

    // Create the model_table_field_columns SQLite table if it does not exist. Note that column_index
    // will only use a maximum of 10 bits.
    connection.execute(
        "CREATE TABLE IF NOT EXISTS model_table_field_columns (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                column_index INTEGER NOT NULL,
                PRIMARY KEY (table_name, column_name)
        ) STRICT",
        (),
    )?;

    Ok(())
}

/// Register all tables and model tables in `catalog` with Apache Arrow
/// DataFusion through `session_context`. If initialization fails the table or
/// model table is removed from `catalog`.
fn register_tables_and_model_tables(context: &mut Arc<Context>) {
    // unwrap() is safe since read() only fails if the RwLock is poisoned.
    let mut catalog = context.catalog.write().unwrap();

    // Initializes tables consisting of standard Apache Parquet files.
    catalog.table_metadata.retain(|table_metadata| {
        let result = context.runtime.block_on(context.session.register_parquet(
            &table_metadata.name,
            &table_metadata.path,
            ParquetReadOptions::default(),
        ));
        check_if_table_or_model_table_is_initialized_otherwise_log_error(
            "table",
            &table_metadata.name,
            result,
        )
    });

    // Initializes tables storing time series as models in Apache Parquet files.
    catalog.model_table_metadata.retain(|model_table_metadata| {
        let result = context.session.register_table(
            model_table_metadata.name.as_str(),
            ModelTable::new(context.clone(), model_table_metadata),
        );
        check_if_table_or_model_table_is_initialized_otherwise_log_error(
            "model table",
            &model_table_metadata.name,
            result,
        )
    });
}

/// Check if the `result` from a table or model table initialization is an
/// `Error`, if so log an error message containing `table_type` and `name` and
/// return [`false`], otherwise return [`true`].
fn check_if_table_or_model_table_is_initialized_otherwise_log_error<T>(
    table_type: &str,
    name: &str,
    result: Result<T, DataFusionError>,
) -> bool {
    if result.is_err() {
        error!(
            "Unable to initialize {} '{}': {}",
            table_type,
            name,
            result.err().unwrap(),
        );
        false
    } else {
        true
    }
}

/// Register a handler to execute when CTRL+C is pressed. The handler takes an
/// exclusive lock for the storage engine, flushes the data the storage engine
/// currently buffers, and terminates the system without releasing the lock.
fn setup_ctrl_c_handler(context: &Arc<Context>) {
    let ctrl_c_context = context.clone();
    context.runtime.spawn(async move {
        // Errors are consciously ignored as the program should terminate if the
        // handler cannot be registered as buffers otherwise cannot be flushed.
        tokio::signal::ctrl_c().await.unwrap();
        ctrl_c_context.storage_engine.write().unwrap().flush();
        std::process::exit(0)
    });
}
