/* Copyright 2026 The ModelarDB Contributors
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

//! Implementation of [`ModelarDbTable`] which allows tables and metadata tables to be queried
//! through Apache DataFusion. It wraps a [`DeltaTable`] and forwards the scan to a
//! [`TableProvider`] created from the latest version of it. Generated columns are computed by
//! [`GeneratedAsExec`] on top of the scan. If a [`DataSink`] is provided,
//! [`TableProvider::insert_into()`] is also supported.

use std::sync::Arc;

use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::ToDFSchema;
use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, ExprSchemable, TableProviderFilterPushDown};
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::physical_expr::planner;
use datafusion::physical_plan::ExecutionPlan;
use deltalake::DeltaTable;
use modelardb_types::types::{ColumnMetadata, TableMetadata};

use crate::error::Result;
use crate::query::generated_as_exec::{ColumnToGenerate, GeneratedAsExec};

/// A queryable representation of a table or a metadata table. [`ModelarDbTable`] wraps a
/// [`TableProvider`] created from a [`DeltaTable`] so it can be registered with Apache DataFusion.
/// [`DeltaTable`] is extended in three ways, `delta_table` is updated to the latest snapshot when
/// scanned, generated columns are computed from the stored columns, and, if a [`DataSink`] is
/// provided, support for inserting data with INSERT has been added. Metadata tables are registered
/// without a [`DataSink`] since they are managed internally and should not be modified by users.
#[derive(Debug)]
pub(crate) struct ModelarDbTable {
    /// Access to the Delta Lake table as a [`DeltaTable`].
    delta_table: DeltaTable,
    /// Access to the Delta Lake table as a [`TableProvider`].
    table_provider: Arc<dyn TableProvider>,
    /// Metadata of the table.
    table_metadata: Arc<TableMetadata>,
    /// Schema of the data that can be read from the table. The stored columns have the types the
    /// Delta Lake table returns, so the schema matches the record batches produced by its scan.
    schema: Arc<Schema>,
    /// Where data should be written to. [`None`] for metadata tables since they should not support
    /// INSERT as they are managed internally.
    maybe_data_sink: Option<Arc<dyn DataSink>>,
}

impl ModelarDbTable {
    /// Create a new [`ModelarDbTable`] for the table with `table_metadata` stored in `delta_table`.
    /// If `maybe_data_sink` is [`None`], INSERT is not supported. Returns
    /// [`ModelarDbStorageError`](crate::error::ModelarDbStorageError) if a [`TableProvider`] cannot
    /// be created from `delta_table`.
    pub(crate) async fn try_new(
        delta_table: DeltaTable,
        table_metadata: Arc<TableMetadata>,
        maybe_data_sink: Option<Arc<dyn DataSink>>,
    ) -> Result<Self> {
        let table_provider = delta_table.table_provider().await?;

        // Use the fields of the Delta Lake table for the stored columns since its scan returns view
        // types, e.g., Utf8View instead of Utf8, and the fields in the query schema for the
        // generated columns since GeneratedAsExec casts them to these types.
        let stored_schema = table_provider.schema();
        let mut fields = table_metadata.query_schema.fields().to_vec();
        for (stored_index, query_index) in table_metadata
            .query_schema_to_stored_schema
            .iter()
            .enumerate()
        {
            fields[*query_index] = stored_schema.fields()[stored_index].clone();
        }
        let schema = Arc::new(Schema::new(fields));

        Ok(Self {
            delta_table,
            table_provider,
            table_metadata,
            schema,
            maybe_data_sink,
        })
    }

    /// Return the [`TableMetadata`] of the table.
    pub(crate) fn table_metadata(&self) -> Arc<TableMetadata> {
        self.table_metadata.clone()
    }

    /// Return `true` if the table has at least one generated column.
    fn has_generated_columns(&self) -> bool {
        self.table_metadata.query_schema_to_stored_schema.len()
            != self.table_metadata.query_schema.fields().len()
    }

    /// Return the index in the stored schema of the stored column at `query_schema_index` in the
    /// query schema.
    fn stored_schema_index(&self, query_schema_index: usize) -> usize {
        self.table_metadata
            .query_schema_to_stored_schema
            .binary_search(&query_schema_index)
            .expect("The column at query_schema_index should be a stored column.")
    }

    /// Return `true` if `filter` only references stored columns, so it can be pushed to the Delta
    /// Lake table.
    fn is_stored_filter(&self, filter: &Expr) -> bool {
        filter.column_refs().iter().all(|column| {
            self.table_metadata
                .query_schema
                .index_of(column.name())
                .is_ok_and(|query_schema_index| {
                    matches!(
                        self.table_metadata.column_metadata[query_schema_index],
                        ColumnMetadata::Stored(_)
                    )
                })
        })
    }
}

#[async_trait]
impl TableProvider for ModelarDbTable {
    /// Return the query schema of the table registered with Apache DataFusion.
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Specify that tables are base tables and not views or temporary tables.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Create an [`ExecutionPlan`] that will scan the table and compute the generated columns in
    /// `projection`. Returns a [`DataFusionError::Plan`] if the necessary metadata cannot be
    /// retrieved or a generated column cannot be planned.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Clone the Delta Lake table and update it to the latest version. self.data_folder.load(
        // &mut self) is not an option due to TypeProvider::scan(&self, ...). Storing the DeltaTable
        // in a Mutex and RwLock is also not an option since most of the methods in TypeProvider
        // return a reference and the locks will be dropped at the end of the method.
        let mut delta_table = self.delta_table.clone();

        delta_table
            .load()
            .await
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;

        let table_provider = delta_table.table_provider().await?;

        // The query schema of a table without generated columns is the stored schema.
        if !self.has_generated_columns() {
            return table_provider.scan(state, projection, filters, limit).await;
        }

        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());

        // Scan the stored columns in the projection followed by the stored columns that are only
        // required to compute the generated columns, since GeneratedAsExec assumes they are last.
        let mut stored_projection = Vec::with_capacity(projection.len());
        let mut generated_columns = vec![];
        let mut source_columns = vec![];

        for (result_index, query_schema_index) in projection.iter().enumerate() {
            match &self.table_metadata.column_metadata[*query_schema_index] {
                ColumnMetadata::Stored(_) => {
                    stored_projection.push(self.stored_schema_index(*query_schema_index));
                }
                ColumnMetadata::Generated(generated_column) => {
                    generated_columns.push((result_index, *query_schema_index, generated_column));
                    source_columns.extend(&generated_column.source_columns);
                }
            }
        }

        // Only include GeneratedAsExec in the query plan if there are columns to generate. This
        // also covers projections without columns, e.g., for COUNT(*), since GeneratedAsExec cannot
        // create record batches without columns.
        if generated_columns.is_empty() {
            return table_provider
                .scan(state, Some(&stored_projection), filters, limit)
                .await;
        }

        for source_column in source_columns {
            let stored_schema_index = self.stored_schema_index(source_column);
            if !stored_projection.contains(&stored_schema_index) {
                stored_projection.push(stored_schema_index);
            }
        }

        let input = table_provider
            .scan(state, Some(&stored_projection), filters, limit)
            .await?;

        // Apache DataFusion coerces types when planning queries, but the expressions of generated
        // columns are planned directly, so they are coerced here, e.g., a Float32 column plus a
        // Float64 literal is computed as Float64. The result is then cast to the column's type in
        // the query schema.
        let df_input_schema = input.schema().to_dfschema()?;
        let execution_props = ExecutionProps::new();
        let planning_context = PhysicalPlanningContext::default();

        let columns_to_generate = generated_columns
            .into_iter()
            .map(|(result_index, query_schema_index, generated_column)| {
                let data_type = self.schema.field(query_schema_index).data_type();
                let expr = generated_column
                    .expr
                    .clone()
                    .rewrite(&mut TypeCoercionRewriter::new(&df_input_schema))?
                    .data
                    .cast_to(data_type, &df_input_schema)?;

                let physical_expr = planner::create_physical_expr(
                    &expr,
                    &df_input_schema,
                    &execution_props,
                    &planning_context,
                )?;

                Ok(ColumnToGenerate::new(result_index, physical_expr))
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

        Ok(GeneratedAsExec::new(
            Arc::new(self.schema.project(&projection)?),
            columns_to_generate,
            input,
        ))
    }

    /// Specify that filters on stored columns are pushed to the Delta Lake table as it reports,
    /// while filters on generated columns are unsupported since the columns are not stored.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        if !self.has_generated_columns() {
            return self.table_provider.supports_filters_pushdown(filters);
        }

        // Delta Lake decides the pushdown of each filter independently, so the filters on stored
        // columns are passed to it one at a time.
        filters
            .iter()
            .map(|filter| {
                if self.is_stored_filter(filter) {
                    Ok(self
                        .table_provider
                        .supports_filters_pushdown(&[filter])?
                        .remove(0))
                } else {
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
            })
            .collect()
    }

    /// Create an [`ExecutionPlan`] that will insert the result of `input` into the table. `input`
    /// must include the generated columns to match the query schema returned by
    /// [`TableProvider::schema()`]. Generally, `FlightService::do_put()` should be used instead of
    /// this method as it is more efficient. Returns a [`DataFusionError::Plan`] if no [`DataSink`]
    /// was provided, i.e., the table is a metadata table.
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let data_sink = self.maybe_data_sink.clone().ok_or_else(|| {
            DataFusionError::Plan("INSERT is not supported for metadata tables.".to_owned())
        })?;

        Ok(Arc::new(DataSinkExec::new(input, data_sink, None)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;

    use arrow::array::{AsArray, RecordBatch, StringViewArray, TimestampMicrosecondArray};
    use arrow::datatypes::{
        ArrowPrimitiveType, DataType, Field, Float32Type, Float64Type, Int64Type,
    };
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionContext, col, lit};
    use modelardb_test::table::{self, NoOpDataSink, TABLE_NAME};
    use modelardb_types::types::{ArrowTimestamp, ErrorBound};
    use tempfile::TempDir;

    use crate::data_folder::DataFolder;
    use crate::parser::{self, ModelarDbStatement};
    use crate::sql_and_concat;

    const ROW_COUNT: usize = 5;

    // Tests for ModelarDbTable.
    #[tokio::test]
    async fn test_scan_table_without_generated_columns() {
        let stored_schema = table::table_metadata().stored_schema;
        let table_metadata = TableMetadata::try_new(
            TABLE_NAME.to_owned(),
            stored_schema.clone(),
            vec![ColumnMetadata::Stored(ErrorBound::Lossless); stored_schema.fields().len()],
        )
        .unwrap();

        let (_temp_dir, data_folder) = create_data_folder_with_table_metadata(
            &table_metadata,
            table::table_record_batch(ROW_COUNT),
        )
        .await;

        let sql = "SELECT * FROM test_table";
        let result = sql_and_concat(data_folder.session_context(), sql)
            .await
            .unwrap();

        assert_eq!(result, table::table_record_batch(ROW_COUNT));
        assert!(!plan_has_generated_as_exec(data_folder.session_context(), sql).await);
    }
}
