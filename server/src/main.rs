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
mod ingestion;
mod macros;
mod models;
mod optimizer;
mod remote;
mod storage;
mod tables;
mod types;

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use datafusion::common::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::execution::options::ParquetReadOptions;
use datafusion::execution::runtime_env::RuntimeEnv;
use tokio::runtime::Runtime;
use tracing::error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::catalog::Catalog;
use crate::optimizer::model_simple_aggregates;
use crate::tables::ModelTable;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// Provides access to the system's configuration and components.
pub struct Context {
    /// Metadata for the tables and model tables in the data folder.
    catalog: RwLock<Catalog>,
    /// A Tokio runtime for executing asynchronous tasks.
    runtime: Runtime,
    /// Main interface for Apache Arrow DataFusion.
    session: SessionContext,
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
        let data_folder_path = PathBuf::from(&data_folder);

        // Build Context.
        let mut catalog = Catalog::try_new(&data_folder_path).map_err(|error| {
            format!("Unable to read data folder '{}': {}", &data_folder, &error)
        })?;
        let runtime = Runtime::new().unwrap();
        let mut session = create_session_context();

        // Register Tables.
        register_tables_and_model_tables(&runtime, &mut session, &mut catalog);

        // Start Interface.
        let context = Arc::new(Context {
            catalog: RwLock::new(catalog),
            runtime,
            session,
        });
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

/// Register all tables and model tables in `catalog` with Apache Arrow
/// DataFusion through `session_context`. If initialization fails the table or
/// model table is removed from `catalog`.
fn register_tables_and_model_tables(
    runtime: &Runtime,
    session_context: &mut SessionContext,
    catalog: &mut Catalog,
) {
    // Initializes tables consisting of standard Apache Parquet files.
    catalog.table_metadata.retain(|table_metadata| {
        let result = runtime.block_on(session_context.register_parquet(
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
        let result = session_context.register_table(
            model_table_metadata.name.as_str(),
            ModelTable::new(model_table_metadata),
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
/// return `false`, otherwise return `true`.
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
