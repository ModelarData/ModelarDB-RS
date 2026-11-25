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
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, ExprSchemable, TableProviderFilterPushDown};
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
