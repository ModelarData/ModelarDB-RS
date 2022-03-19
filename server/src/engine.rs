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
use std::sync::Arc;

use datafusion::prelude::{ExecutionConfig, ExecutionContext};

use crate::catalog::Catalog;
use crate::optimizer::model_simple_aggregates;
use crate::tables::DataPointView;

/** Public Functions **/
//TODO: Move registration of tables to tables.rs and construction of ExecutionContext to main.rs?
pub async fn new(catalog: &Catalog) -> ExecutionContext {
    let config = ExecutionConfig::new().add_physical_optimizer_rule(Arc::new(
        model_simple_aggregates::ModelSimpleAggregatesPhysicalOptimizerRule {},
    ));
    let mut ctx = ExecutionContext::with_config(config);

    //Initializes tables consisting of standard Apache Parquet files
    for table in &catalog.tables {
        if ctx.register_parquet(&table.name, &table.path).await.is_err() {
            eprintln!("ERROR: unable to initialize table {}", table.name);
        }
    }

    //Initializes tables storing time series as models in Apache Parquet files
    for table in &catalog.model_tables {
        let name = table.name.clone();
        let model_table = (*table).clone();
        let table_provider = DataPointView::new(&model_table);
        if ctx.register_table(name.as_str(), table_provider).is_err() {
            eprintln!("ERROR: unable to initialize model table {}", name);
        }
    }
    ctx
}
