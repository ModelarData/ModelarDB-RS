/* Copyright 2024 The ModelarDB Contributors
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

//! Execute queries against tables and model tables using Apache DataFusion. Queries may be
//! rewritten by Apache DataFusion's optimizer to make it more efficient to execute. Additional
//! rules are added to this optimizer to execute queries directly on the compressed segments.

mod errors;
mod optimizer;
mod query;

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::insert::DataSink;
use datafusion::prelude::SessionContext;
use deltalake::DeltaTable;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::metadata::table_metadata_manager::TableMetadataManager;

use crate::errors::Result;
use crate::query::model_table::ModelTable;
use crate::query::table::Table;

/// Create a new [`SessionContext`] for interacting with Apache DataFusion. The [`SessionContext`]
/// is constructed with the default configuration, default resource managers, and additional
/// optimizer rules that rewrite simple aggregate queries to be executed directly on the segments
/// containing metadata and models instead of on reconstructed data points created from the segments
/// for model tables.
pub fn create_session_context() -> SessionContext {
    let mut session_state_builder = SessionStateBuilder::new().with_default_features();

    // Uses the rule method instead of the rules method as the rules method replaces the built-ins.
    for physical_optimizer_rule in optimizer::physical_optimizer_rules() {
        session_state_builder =
            session_state_builder.with_physical_optimizer_rule(physical_optimizer_rule);
    }

    let session_state = session_state_builder.build();
    SessionContext::new_with_state(session_state)
}

/// Register the table stored in `delta_table` with `table_name` and `data_sink` in
/// `session_context`. If the table could not be registered with Apache DataFusion, return
/// [`ModelarDbQueryError`](crate::errors::ModelarDbQueryError).
pub fn register_table(
    session_context: &SessionContext,
    table_name: &str,
    delta_table: DeltaTable,
    data_sink: Arc<dyn DataSink>,
) -> Result<()> {
    let table = Arc::new(Table::new(delta_table, data_sink));

    session_context.register_table(table_name, table)?;

    Ok(())
}

/// Register the model table stored in `delta_table` with `model_table_metadata` from
/// `table_metadata_manager` and `data_sink` in `session_context`. If the model table could not be
/// registered with Apache DataFusion, return [`ModelarDbError`].
pub fn register_model_table(
    session_context: &SessionContext,
    delta_table: DeltaTable,
    model_table_metadata: Arc<ModelTableMetadata>,
    table_metadata_manager: Arc<TableMetadataManager>,
    data_sink: Arc<dyn DataSink>,
) -> Result<()> {
    let model_table = ModelTable::new(
        delta_table,
        table_metadata_manager,
        model_table_metadata.clone(),
        data_sink,
    );

    session_context.register_table(&model_table_metadata.name, model_table)?;

    Ok(())
}

/// Return the [`Arc<ModelTableMetadata>`] of the table `maybe_model_table` if it is a model table,
/// otherwise [`None`] is returned.
pub fn maybe_model_table_to_model_table_metadata(
    maybe_model_table: Arc<dyn TableProvider>,
) -> Option<Arc<ModelTableMetadata>> {
    maybe_model_table
        .as_any()
        .downcast_ref::<ModelTable>()
        .map(|model_table| model_table.model_table_metadata())
}
