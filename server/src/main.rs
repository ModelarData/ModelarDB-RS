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
mod macros;
mod models;
mod optimizer;
mod remote;
mod tables;

use std::sync::Arc;

use tokio::runtime::Runtime;

use datafusion::prelude::{ExecutionConfig, ExecutionContext};

use crate::catalog::Catalog;
use crate::optimizer::model_simple_aggregates;
use crate::tables::DataPointView;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/** Public Types **/
pub struct Context {
    catalog: catalog::Catalog,
    runtime: Runtime,
    execution: ExecutionContext,
}

/** Public Functions **/
fn main() {
    let mut args = std::env::args();
    args.next(); //Skip executable
    if let Some(data_folder) = args.next() {
        //Build Context
        let catalog = catalog::new(&data_folder);
        let runtime = Runtime::new().unwrap();
        let mut execution = create_execution_context();

        //Register Tables
        runtime.block_on(register_tables(&mut execution, &catalog));

        //Start Interface
        let context = Arc::new(Context {
            catalog,
            runtime,
            execution,
        });
        remote::start_arrow_flight_server(context, 9999);
    } else {
        //The errors are consciously ignored as the program is terminating
        let binary_path = std::env::current_exe().unwrap();
        let binary_name = binary_path.file_name().unwrap();
        println!("usage: {} data_folder", binary_name.to_str().unwrap());
    }
}

/** Private Functions **/
fn create_execution_context() -> ExecutionContext {
    let config = ExecutionConfig::new().add_physical_optimizer_rule(Arc::new(
        model_simple_aggregates::ModelSimpleAggregatesPhysicalOptimizerRule {},
    ));
    ExecutionContext::with_config(config)
}

async fn register_tables(execution: &mut ExecutionContext, catalog: &Catalog) {
    //Initializes tables consisting of standard Apache Parquet files
    for table in &catalog.tables {
        if execution.register_parquet(&table.name, &table.path).await.is_err() {
            eprintln!("ERROR: unable to initialize table {}", table.name);
        }
    }

    //Initializes tables storing time series as models in Apache Parquet files
    for table in &catalog.model_tables {
        let name = table.name.clone();
        let model_table = (*table).clone();
        let table_provider = DataPointView::new(&model_table);
        if execution.register_table(name.as_str(), table_provider).is_err() {
            eprintln!("ERROR: unable to initialize model table {}", name);
        }
    }
}
