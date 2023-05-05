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

//! Implementation of [`ModelTable`] which allows model tables to be queried through Apache Arrow
//! DataFusion. It takes the projection, filters as [`Exprs`](Expr), and limit of a query as input
//! and returns a physical query plan that produces all of the data points required for the query.

// Public so the rules added to Apache Arrow DataFusion's physical optimizer can access GridExec.
pub mod generated_as_exec;
pub mod grid_exec;
pub mod sorted_join_exec;

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, SchemaRef, TimeUnit};
use datafusion::common::ToDFSchema;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::{
    datasource::TableProviderFilterPushDown, listing::PartitionedFile, TableProvider, TableType,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{ExecutionProps, SessionState};
use datafusion::logical_expr::{self, BinaryExpr, Expr, Operator};
use datafusion::optimizer::utils;
use datafusion::physical_expr::planner;
use datafusion::physical_plan::file_format::{FileScanConfig, ParquetExec};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, Statistics};
use modelardb_common::schemas::{COMPRESSED_SCHEMA, QUERY_SCHEMA};
use modelardb_common::types::ArrowValue;
use object_store::ObjectStore;
use tokio::sync::RwLockWriteGuard;

use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::query::generated_as_exec::ColumnToGenerate; //GeneratedAsExec};
use crate::query::grid_exec::GridExec;
use crate::query::sorted_join_exec::{SortedJoinColumnType, SortedJoinExec};
use crate::storage;
use crate::storage::StorageEngine;
use crate::Context;

/// A queryable representation of a model table which stores multivariate time
/// series as segments containing metadata and models. [`ModelTable`] implements
/// [`TableProvider`] so it can be registered with Apache Arrow DataFusion and
/// the multivariate time series queried as multiple univariate time series.
pub struct ModelTable {
    /// Access to the system's configuration and components.
    context: Arc<Context>,
    /// Location of the object store used by the storage engine.
    object_store_url: ObjectStoreUrl,
    /// Metadata required to read from and write to the model table.
    model_table_metadata: Arc<ModelTableMetadata>,
    /// Field column to use for queries that do not include fields.
    fallback_field_column: u16,
}

impl ModelTable {
    pub fn new(context: Arc<Context>, model_table_metadata: Arc<ModelTableMetadata>) -> Arc<Self> {
        // Compute the index of the first field column in the model table's
        // schema. This is used for queries that does not contain any fields.
        let fallback_field_column = {
            model_table_metadata
                .schema
                .fields()
                .iter()
                .position(|field| field.data_type() == &ArrowValue::DATA_TYPE)
                .unwrap() as u16 // unwrap() is safe as all model tables contain at least one field.
        };

        // unwrap() is safe as the url is predefined as a constant in storage.
        let object_store_url =
            ObjectStoreUrl::parse(storage::QUERY_DATA_FOLDER_SCHEME_WITH_HOST).unwrap();

        Arc::new(ModelTable {
            context,
            model_table_metadata,
            object_store_url,
            fallback_field_column,
        })
    }

    /// Return the [`ModelTableMetadata`] for the table.
    pub fn model_table_metadata(&self) -> Arc<ModelTableMetadata> {
        self.model_table_metadata.clone()
    }

    /// Create an [`ExecutionPlan`] that will scan the column at `column_index` in the table with
    /// `table_name`. Returns a [`DataFusionError::Plan`] if the necessary metadata cannot be
    /// retrieved from the metadata database.
    #[allow(clippy::too_many_arguments)]
    async fn scan_column(
        &self,
        storage_engine: &mut RwLockWriteGuard<'_, StorageEngine>,
        query_object_store: &Arc<dyn ObjectStore>,
        table_name: &str,
        column_index: u16,
        parquet_predicates: Option<Expr>,
        grid_predicates: Option<Expr>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // unwrap() is safe to use as compressed_files() only fails if a table with the name
        // table_name and column with column_index does not exists, if end time is before start
        // time, or if max value is larger than min value.
        // TODO: extract predicates on time and value and push them to the storage engine.
        let object_metas = storage_engine
            .compressed_files(
                table_name,
                column_index,
                None,
                None,
                None,
                None,
                query_object_store,
            )
            .await
            .unwrap();

        // Create the data source operator. Assumes the ObjectStore exists.
        let partitioned_files: Vec<PartitionedFile> = object_metas
            .into_iter()
            .map(|object_meta| PartitionedFile {
                object_meta,
                partition_values: vec![],
                range: None,
                extensions: None,
            })
            .collect::<Vec<PartitionedFile>>();

        // TODO: predict the accumulate size of the input data after filtering.
        let statistics = Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        };

        let file_scan_config = FileScanConfig {
            object_store_url: self.object_store_url.clone(),
            file_schema: COMPRESSED_SCHEMA.0.clone(),
            file_groups: vec![partitioned_files],
            statistics,
            projection: None,
            limit,
            table_partition_cols: vec![],
            output_ordering: None,
            infinite_source: false,
        };

        let physical_parquet_predicates = if let Some(parquet_predicates) = parquet_predicates {
            Some(convert_expr_to_physical_expr(
                &parquet_predicates,
                COMPRESSED_SCHEMA.0.clone(),
            )?)
        } else {
            None
        };

        let apache_parquet_exec = Arc::new(
            ParquetExec::new(file_scan_config, physical_parquet_predicates, None)
                .with_pushdown_filters(true)
                .with_reorder_filters(true),
        );

        // Create the gridding operator.
        let physical_grid_predicates = if let Some(grid_predicates) = grid_predicates {
            Some(convert_expr_to_physical_expr(
                &grid_predicates,
                QUERY_SCHEMA.0.clone(),
            )?)
        } else {
            None
        };

        Ok(GridExec::new(
            physical_grid_predicates,
            limit,
            apache_parquet_exec,
        ))
    }
}

/// Rewrite and combine the `filters` that is written in terms of the model table's schema, to a
/// filter that is written in terms of the schema used for compressed segments by the storage engine
/// and a filter that is written in terms of the schema used by [`GridExec`] for its output. If the
/// filters cannot be rewritten [`None`] is returned.
fn rewrite_and_combine_filters(
    schema: &SchemaRef,
    filters: &[Expr],
) -> (Option<Expr>, Option<Expr>) {
    let rewritten_filters = filters
        .iter()
        .filter_map(|filter| rewrite_filter(schema, filter));
    let (parquet_rewritten_filters, grid_rewritten_filters): (Vec<Expr>, Vec<Expr>) =
        rewritten_filters.unzip();
    (
        utils::conjunction(parquet_rewritten_filters),
        utils::conjunction(grid_rewritten_filters),
    )
}

/// Convert `expr` to a [`PhysicalExpr`] with the types in `schema`.
fn convert_expr_to_physical_expr(expr: &Expr, schema: SchemaRef) -> Result<Arc<dyn PhysicalExpr>> {
    let df_schema = schema.clone().to_dfschema()?;
    planner::create_physical_expr(expr, &df_schema, &schema, &ExecutionProps::new())
}

/// Rewrite the `filter` that is written in terms of the model table's schema, to a filter that is
/// written in terms of the schema used for compressed segments by the storage engine and a filter
/// that is written in terms of the schema used by [`GridExec`] for its output. If the filter cannot
/// be rewritten [`None`] is returned.
fn rewrite_filter(schema: &SchemaRef, filter: &Expr) -> Option<(Expr, Expr)> {
    match filter {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if let Expr::Column(column) = &**left {
                // unwrap() is safe as it has already been checked that the fields exists.
                let field = schema.field_with_name(&column.name).unwrap();
                // Type aliases cannot be used as a constructor and thus cannot be used here.
                if *field.data_type() == DataType::Timestamp(TimeUnit::Millisecond, None) {
                    match op {
                        Operator::Gt | Operator::GtEq => Some((
                            new_binary_expr(logical_expr::col("end_time"), *op, *right.clone()),
                            new_binary_expr(logical_expr::col("timestamp"), *op, *right.clone()),
                        )),
                        Operator::Lt | Operator::LtEq => Some((
                            new_binary_expr(logical_expr::col("start_time"), *op, *right.clone()),
                            new_binary_expr(logical_expr::col("timestamp"), *op, *right.clone()),
                        )),
                        Operator::Eq => Some((
                            new_binary_expr(
                                new_binary_expr(
                                    logical_expr::col("start_time"),
                                    Operator::LtEq,
                                    *right.clone(),
                                ),
                                Operator::And,
                                new_binary_expr(
                                    logical_expr::col("end_time"),
                                    Operator::GtEq,
                                    *right.clone(),
                                ),
                            ),
                            new_binary_expr(logical_expr::col("timestamp"), *op, *right.clone()),
                        )),
                        _ => None,
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
        _other => None,
    }
}

/// Create a [`Expr::BinaryExpr`].
fn new_binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

#[async_trait]
impl TableProvider for ModelTable {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the schema of the model table registered with Apache Arrow DataFusion.
    fn schema(&self) -> SchemaRef {
        self.model_table_metadata.schema.clone()
    }

    /// Specify that model tables are base tables and not views or temporary.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Specify that model tables performs inexact predicate push-down.
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    /// Create an [`ExecutionPlan`] that will scan the table. Returns a
    /// [`DataFusionError::Plan`] if the necessary metadata cannot be retrieved
    /// from the metadata database.
    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table_name = &self.model_table_metadata.name.as_str();
        let schema = &self.model_table_metadata.schema;

        // Ensures a projection is present for looking up the columns to return.
        let projection: Vec<usize> = if let Some(projection) = projection {
            projection.to_vec()
        } else {
            (0..schema.fields().len()).collect()
        };

        // unwrap() is safe as the projection is based on the schema.
        let schema_after_projection = Arc::new(schema.project(&projection).unwrap());

        // Since SortedJoinStream and GeneratedAsStream simply needs to append arrays to a vector,
        // the order of the field and tag columns in the projection is extracted and the streams
        // SortedJoinStream read columns from are arranged in the same order as the field columns.
        let tag_column_indices = &self.model_table_metadata.tag_column_indices;

        let mut sorted_join_order: Vec<SortedJoinColumnType> = Vec::with_capacity(projection.len());
        let mut tag_column_order: Vec<&str> = Vec::with_capacity(tag_column_indices.len());
        let mut generated_as_order: Vec<ColumnToGenerate> =
            Vec::with_capacity(schema.fields.len());
        let mut stored_field_indices_in_projection: Vec<u16> =
            Vec::with_capacity(schema.fields.len() - 1 - tag_column_indices.len());

        for index in &projection {
            if *index == self.model_table_metadata.timestamp_column_index {
                sorted_join_order.push(SortedJoinColumnType::Timestamp);
            } else if tag_column_indices.contains(index) {
                tag_column_order.push(schema.fields[*index].name());
                sorted_join_order.push(SortedJoinColumnType::Tag);
            } else if let Some(generation_expr) =
                &self.model_table_metadata.generation_exprs[*index]
            {
                let physical_expr = convert_expr_to_physical_expr(
                    generation_expr,
                    schema_after_projection.clone(),
                )?;

                generated_as_order.push(ColumnToGenerate::new(*index, physical_expr));
            } else {
                stored_field_indices_in_projection.push(*index as u16);
                sorted_join_order.push(SortedJoinColumnType::Field);
            }
        }

        // TODO: extract all of the predicates that consist of tag = tag_value from the query so the
        // segments can be pruned by univariate_id in ParquetExec and hash_to_tags can be minimized.
        let (parquet_predicates, grid_predicates) = rewrite_and_combine_filters(schema, filters);

        // Compute a mapping from hashes to tags.
        let hash_to_tags = self
            .context
            .metadata_manager
            .mapping_from_hash_to_tags(table_name, &tag_column_order)
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;

        // unwrap() is safe as the store is set by create_session_context().
        let query_object_store = ctx
            .runtime_env()
            .object_store(&self.object_store_url)
            .unwrap();

        // At least one field column must be read for the univariate_ids and timestamps.
        if stored_field_indices_in_projection.is_empty() {
            stored_field_indices_in_projection.push(self.fallback_field_column);
        }

        // Request the matching files from the storage engine. The exclusive lock on the storage
        // engine is held until object metas for all columns have been retrieved to ensure they
        // contain the same number of data points.
        let mut field_column_execution_plans: Vec<Arc<dyn ExecutionPlan>> =
            Vec::with_capacity(stored_field_indices_in_projection.len());
        let mut storage_engine = self.context.storage_engine.write().await;
        for field_column_index in stored_field_indices_in_projection {
            let execution_plan = self
                .scan_column(
                    &mut storage_engine,
                    &query_object_store,
                    table_name,
                    field_column_index,
                    parquet_predicates.clone(),
                    grid_predicates.clone(),
                    limit,
                )
                .await?;

            field_column_execution_plans.push(execution_plan);
        }

        Ok(SortedJoinExec::new(
            schema_after_projection,
            sorted_join_order,
            Arc::new(hash_to_tags),
            field_column_execution_plans,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::logical_expr::lit;
    use datafusion::prelude::Expr;
    use modelardb_common::types::Timestamp;

    use crate::metadata::test_util;

    const TIMESTAMP_PREDICATE_VALUE: Timestamp = 37;

    // Tests for rewrite_and_combine_filters().
    #[test]
    fn test_rewrite_empty_vec() {
        let schema = test_util::model_table_metadata().0.schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &[]);
        assert!(parquet_filter.is_none());
        assert!(grid_filter.is_none());
    }

    #[test]
    fn test_rewrite_greater_than_timestamp() {
        let filters = new_timestamp_filters(Operator::Gt);
        let schema = test_util::model_table_metadata().0.schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &filters);

        assert_binary_expr(
            parquet_filter.unwrap(),
            "end_time",
            Operator::Gt,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );

        assert_binary_expr(
            grid_filter.unwrap(),
            "timestamp",
            Operator::Gt,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_greater_than_or_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::GtEq);
        let schema = test_util::model_table_metadata().0.schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &filters);

        assert_binary_expr(
            parquet_filter.unwrap(),
            "end_time",
            Operator::GtEq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );

        assert_binary_expr(
            grid_filter.unwrap(),
            "timestamp",
            Operator::GtEq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_less_than_timestamp() {
        let filters = new_timestamp_filters(Operator::Lt);
        let schema = test_util::model_table_metadata().0.schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &filters);

        assert_binary_expr(
            parquet_filter.unwrap(),
            "start_time",
            Operator::Lt,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );

        assert_binary_expr(
            grid_filter.unwrap(),
            "timestamp",
            Operator::Lt,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_less_than_or_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::LtEq);
        let schema = test_util::model_table_metadata().0.schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &filters);

        assert_binary_expr(
            parquet_filter.unwrap(),
            "start_time",
            Operator::LtEq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );

        assert_binary_expr(
            grid_filter.unwrap(),
            "timestamp",
            Operator::LtEq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::Eq);
        let schema = test_util::model_table_metadata().0.schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &filters);

        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = parquet_filter.unwrap() {
            assert_binary_expr(
                *left,
                "start_time",
                Operator::LtEq,
                lit(TIMESTAMP_PREDICATE_VALUE),
            );
            assert_eq!(op, Operator::And);
            assert_binary_expr(
                *right,
                "end_time",
                Operator::GtEq,
                lit(TIMESTAMP_PREDICATE_VALUE),
            );
        } else {
            panic!("Expr is not a BinaryExpr.");
        }

        assert_binary_expr(
            grid_filter.unwrap(),
            "timestamp",
            Operator::Eq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    fn new_timestamp_filters(operator: Operator) -> Vec<Expr> {
        vec![new_binary_expr(
            logical_expr::col("timestamp"),
            operator,
            lit(TIMESTAMP_PREDICATE_VALUE),
        )]
    }

    fn assert_binary_expr(expr: Expr, column: &str, operator: Operator, value: Expr) {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
            assert_eq!(*left, logical_expr::col(column));
            assert_eq!(op, operator);
            assert_eq!(*right, value);
        } else {
            panic!("Expr is not a BinaryExpr.");
        }
    }
}
