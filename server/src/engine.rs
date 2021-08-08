use std::sync::Arc;

use datafusion::prelude::{ExecutionConfig, ExecutionContext};

use crate::catalog::Catalog;
use crate::tables::DataPointView;

/** Public Functions **/
pub fn new(catalog: &Catalog) -> ExecutionContext {
    let config = ExecutionConfig::new()
        .add_optimizer_rule(Arc::new(crate::optimizer::PrintOptimizerRule {}))
        .with_query_planner(Arc::new(crate::optimizer::PrintQueryPlanner {}))
        .add_physical_optimizer_rule(Arc::new(crate::optimizer::PrintPhysicalOptimizerRule {}));
    let mut ctx = ExecutionContext::with_config(config);

    //Initializes tables consisting of standard Apache Parquet files
    for table in &catalog.tables {
        if ctx.register_parquet(&table.name, &table.path).is_err() {
            eprintln!("ERROR: unable to initialize table {}", table.name);
        }
    }

    //Initializes tables storing time series as models in Apache Parquet files
    let cores = num_cpus::get_physical();
    for table in &catalog.model_tables {
        let name = table.name.clone();
        let model_table = (*table).clone();
        let table_provider = DataPointView::new(cores, &Arc::new(model_table));
        if ctx.register_table(name.as_str(), table_provider).is_err() {
            eprintln!("ERROR: unable to initialize model table {}", name);
        }
    }
    ctx
}
