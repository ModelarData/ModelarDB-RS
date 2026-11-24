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

//! Utility functions to register metadata tables, normal tables, and time series tables with Apache
//! DataFusion and to execute SQL statements and queries.

pub mod data_folder;
pub mod error;
mod optimizer;
pub mod parser;
mod query;
pub mod write_ahead_log;

use std::any::Any;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use datafusion::catalog::{MemorySchemaProvider, TableProvider};
use datafusion::datasource::sink::DataSink;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::Statement as DFStatement;
use deltalake::DeltaTable;
use modelardb_types::types::TimeSeriesTableMetadata;
use sqlparser::ast::Statement;

use crate::error::Result;
use crate::query::normal_table::NormalTable;
use crate::query::time_series_table::TimeSeriesTable;

/// The folder storing compressed table data in the data folders.
const TABLE_FOLDER: &str = "tables";

/// The folder storing metadata in the data folders.
const METADATA_FOLDER: &str = "metadata";

/// The folder storing the write-ahead log in the data folders.
pub const WRITE_AHEAD_LOG_FOLDER: &str = "wal";

/// Create a new [`SessionContext`] for interacting with Apache DataFusion. The [`SessionContext`]
/// is constructed with the default configuration, default resource managers, and additional
/// optimizer rules that rewrite simple aggregate queries to be executed directly on the segments
/// containing metadata and models instead of on reconstructed data points created from the segments
/// for time series tables.
pub fn create_session_context() -> SessionContext {
    let mut session_state_builder = SessionStateBuilder::new().with_default_features();

    // Uses the rule method instead of the rules method as the rules method replaces the built-ins.
    for physical_optimizer_rule in optimizer::physical_optimizer_rules() {
        session_state_builder =
            session_state_builder.with_physical_optimizer_rule(physical_optimizer_rule);
    }

    // Use the default configuration: https://datafusion.apache.org/user-guide/configs.html.
    let session_config = SessionConfig::new();
    session_state_builder = session_state_builder.with_config(session_config);

    let session_state = session_state_builder.build();
    let session_context = SessionContext::new_with_state(session_state);
    let default_catalog = session_context
        .catalog("datafusion")
        .expect("The datafusion catalog should always exist.");
    default_catalog
        .register_schema("metadata", Arc::new(MemorySchemaProvider::new()))
        .expect("Catalog register schema should never fail.");

    session_context
}

/// Register the normal table stored in `delta_table` with `table_name` and `data_sink` in
/// `session_context`. If the normal table could not be registered with Apache DataFusion, return
/// [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub async fn register_normal_table(
    session_context: &SessionContext,
    table_name: &str,
    delta_table: DeltaTable,
    data_sink: Arc<dyn DataSink>,
) -> Result<()> {
    let normal_table = Arc::new(NormalTable::try_new(delta_table, Some(data_sink)).await?);
    session_context.register_table(table_name, normal_table)?;

    Ok(())
}

/// Register the time series table stored in `delta_table` with `time_series_table_metadata` and
/// `data_sink` in `session_context`. If the time series table could not be registered with Apache
/// DataFusion, return [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub fn register_time_series_table(
    session_context: &SessionContext,
    delta_table: DeltaTable,
    time_series_table_metadata: Arc<TimeSeriesTableMetadata>,
    data_sink: Arc<dyn DataSink>,
) -> Result<()> {
    let time_series_table =
        TimeSeriesTable::new(delta_table, time_series_table_metadata.clone(), data_sink);

    session_context.register_table(&time_series_table_metadata.name, time_series_table)?;

    Ok(())
}

/// Return the [`Arc<TimeSeriesTableMetadata>`] of the table `maybe_time_series_table` if it is a
/// time series table, otherwise [`None`] is returned.
pub fn maybe_table_provider_to_time_series_table_metadata(
    maybe_time_series_table: Arc<dyn TableProvider>,
) -> Option<Arc<TimeSeriesTableMetadata>> {
    (maybe_time_series_table.as_ref() as &dyn Any)
        .downcast_ref::<TimeSeriesTable>()
        .map(|time_series_table| time_series_table.time_series_table_metadata())
}

/// Execute `statement` in `session_context` and return the result as a
/// [`SendableRecordBatchStream`]. If `statement` could not be executed successfully,
/// [`ModelarDbStorageError`](error::ModelarDbStorageError) is returned.
pub async fn execute_statement(
    session_context: &SessionContext,
    statement: Statement,
) -> Result<SendableRecordBatchStream> {
    let session_state = session_context.state();
    let df_statement = DFStatement::Statement(Box::new(statement));

    let logical_plan = session_state.statement_to_plan(df_statement).await?;
    let data_frame = session_context.execute_logical_plan(logical_plan).await?;
    let sendable_record_batch_stream = data_frame.execute_stream().await?;

    Ok(sendable_record_batch_stream)
}

/// Execute the SQL query `sql` in `session_context` and return the result as a single
/// [`RecordBatch`]. If the query could not be executed successfully, return
/// [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub async fn sql_and_concat(session_context: &SessionContext, sql: &str) -> Result<RecordBatch> {
    let dataframe = session_context.sql(sql).await?;
    let schema = dataframe.schema().inner().clone();

    let record_batches = dataframe.collect().await?;
    let record_batch = concat_batches(&schema, &record_batches)?;

    Ok(record_batch)
}
