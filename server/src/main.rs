/* Copyright 2021 The MiniModelarDB Contributors
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

mod catalog;
mod errors;
mod ingestion;
mod macros;
mod models;
mod optimizer;
mod remote;
mod storage;
mod tables;
mod types;

use std::sync::Arc;

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

/** Public Types **/
pub struct Context {
    catalog: Catalog,
    runtime: Runtime,
    session: SessionContext,
}

/** Public Functions **/
fn main() -> Result<(), String> {
    // A layer that logs events to stdout.
    let stdout_log = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry().with(stdout_log).init();

    let mut args = std::env::args();
    args.next(); // Skip executable.
    if let Some(data_folder) = args.next() {
        // Build Context.
        let mut catalog = Catalog::try_new(&data_folder).map_err(|error| {
            format!(
                "Unable to read data folder {} due to {}",
                &data_folder, &error
            )
        })?;
        let runtime = Runtime::new().unwrap();
        let mut session = create_session_context();

        // Register Tables.
        register_tables(&runtime, &mut session, &mut catalog);

        // Start Interface.
        let context = Arc::new(Context {
            catalog,
            runtime,
            session,
        });
        remote::start_arrow_flight_server(context, 9999);
    } else {
        // The errors are consciously ignored as the program is terminating.
        let binary_path = std::env::current_exe().unwrap();
        let binary_name = binary_path.file_name().unwrap().to_str().unwrap();
        eprintln!("Usage: {} path_to_data_folder.", binary_name);
    }
    Ok(())
}

/** Private Functions **/
fn create_session_context() -> SessionContext {
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::with_config_rt(config, runtime).with_physical_optimizer_rules(vec![
        Arc::new(model_simple_aggregates::ModelSimpleAggregatesPhysicalOptimizerRule {}),
    ]);
    SessionContext::with_state(state)
}

fn register_tables(runtime: &Runtime, session: &mut SessionContext, catalog: &mut Catalog) {
    // Initializes tables consisting of standard Apache Parquet files.
    catalog.table_metadata.retain(|table_metadata| {
        let result = runtime.block_on(session.register_parquet(
            &table_metadata.name,
            &table_metadata.path,
            ParquetReadOptions::default(),
        ));
        check_if_ok_otherwse_log_error("table", &table_metadata.name, result)
    });

    // Initializes tables storing time series as models in Apache Parquet files.
    catalog.model_table_metadata.retain(|model_table_metadata| {
        let result = session.register_table(
            model_table_metadata.name.as_str(),
            ModelTable::new(model_table_metadata),
        );
        check_if_ok_otherwse_log_error("model table", &model_table_metadata.name, result)
    });
}

fn check_if_ok_otherwse_log_error<T>(
    table_type: &str,
    name: &str,
    result: Result<T, DataFusionError>,
) -> bool {
    if result.is_err() {
        error!(
            "Unable to initialize {} {} due to {}.",
            table_type,
            name,
            result.err().unwrap(),
        );
        false
    } else {
        true
    }
}
