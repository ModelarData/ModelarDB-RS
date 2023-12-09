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
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Schema, SchemaRef, TimeUnit,
};
use datafusion::common::ToDFSchema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::ParquetExec;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::provider::TableProviderFilterPushDown;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{ExecutionProps, SessionState};
use datafusion::logical_expr::{self, BinaryExpr, Expr, Operator};
use datafusion::optimizer::utils;
use datafusion::physical_expr::planner;
use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, Statistics};
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::schemas::{COMPRESSED_SCHEMA, QUERY_SCHEMA};
use modelardb_common::types::{ArrowTimestamp, ArrowValue, ServerMode};
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use tokio::sync::RwLockWriteGuard;

use crate::context::Context;
use crate::query::generated_as_exec::{ColumnToGenerate, GeneratedAsExec};
use crate::query::grid_exec::GridExec;
use crate::query::sorted_join_exec::{SortedJoinColumnType, SortedJoinExec};
use crate::storage::StorageEngine;
use crate::{storage, ClusterMode};

/// The global sort order [`ParquetExec`] guarantees for the segments it produces and that
/// [`GridExec`] requires for the segments its receives as its input. It is guaranteed by
/// [`ParquetExec`] because the storage engine uses this sort order for each Apache Parquet file and
/// these files are read sequentially by [`ParquetExec`]. Another sort order could also be used, the
/// current query pipeline simply requires that the
/// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) [`SortedJoinExec`] receive from
/// its inputs all contain data points for the same time interval and that they are sorted the same.
pub static QUERY_ORDER_SEGMENT: Lazy<Vec<PhysicalSortExpr>> = Lazy::new(|| {
    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    vec![
        PhysicalSortExpr {
            expr: Arc::new(Column::new("univariate_id", 0)),
            options: sort_options,
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("start_time", 2)),
            options: sort_options,
        },
    ]
});

/// The global sort order [`GridExec`] guarantees for the data points it produces and that
/// [`SortedJoinExec`] requires for the data points it receives as its input. It is guaranteed by
/// [`GridExec`] because it receives segments sorted by [`QUERY_ORDER_SEGMENT`] from [`ParquetExec`]
/// and because these segments cannot contain data points for overlapping time intervals. Another
/// sort order could also be used, the current query pipeline simply requires that the
/// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) [`SortedJoinExec`] receive from
/// its inputs all contain data points for the same time interval and that they are sorted the same.
pub static QUERY_ORDER_DATA_POINT: Lazy<Vec<PhysicalSortExpr>> = Lazy::new(|| {
    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    vec![
        PhysicalSortExpr {
            expr: Arc::new(Column::new("univariate_id", 0)),
            options: sort_options,
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("timestamp", 1)),
            options: sort_options,
        },
    ]
});

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
        // Compute the index of the first stored field column in the model table's query schema. It
        // is for queries without fields as the uids, timestamps, and values are stored together.
        let fallback_field_column = {
            model_table_metadata
                .query_schema
                .fields()
                .iter()
                .enumerate()
                .position(|(index, field)| {
                    model_table_metadata.generated_columns[index].is_none()
                        && field.data_type() == &ArrowValue::DATA_TYPE
                })
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

    /// Compute the schema that contains the stored columns in `projection`.
    fn apply_query_schema_projection_to_schema(&self, projection: &[usize]) -> Arc<Schema> {
        let columns = projection
            .iter()
            .filter_map(|query_schema_index| {
                if self.model_table_metadata.generated_columns[*query_schema_index].is_none() {
                    Some(
                        self.model_table_metadata
                            .query_schema
                            .field(*query_schema_index)
                            .clone(),
                    )
                } else {
                    None
                }
            })
            .collect::<Vec<Field>>();

        Arc::new(Schema::new(columns))
    }

    /// Return the index of the column in schema that has the same name as the column in query
    /// schema with the index `query_schema_index`. If a column with that name does not exist in
    /// schema a [`DataFusionError::Plan`] is returned.
    fn query_schema_index_to_schema_index(&self, query_schema_index: usize) -> Result<usize> {
        let query_schema = &self.model_table_metadata.query_schema;
        let column_name = query_schema.field(query_schema_index).name();
        Ok(self.model_table_metadata.schema.index_of(column_name)?)
    }

    /// Create an [`ExecutionPlan`] that will scan the column at `column_index` in the table with
    /// `table_name`. Returns a [`DataFusionError::Plan`] if the necessary metadata cannot be
    /// retrieved from the metadata database.
    async fn scan_column(
        &self,
        storage_engine: &mut RwLockWriteGuard<'_, StorageEngine>,
        query_object_store: &Arc<dyn ObjectStore>,
        table_name: &str,
        column_index: u16,
        maybe_parquet_predicates: Option<Expr>,
        maybe_grid_predicates: Option<Expr>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let configuration_manager = self.context.configuration_manager.read().await;

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
                &configuration_manager.server_mode,
                &configuration_manager.cluster_mode,
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

        // TODO: give the optimizer more info for timestamps and values through statistics, e.g, min
        // can be computed using only the metadata database due to the aggregate_statistics rule.
        let file_scan_config = FileScanConfig {
            object_store_url: self.object_store_url.clone(),
            file_schema: COMPRESSED_SCHEMA.0.clone(),
            file_groups: vec![partitioned_files],
            statistics: Statistics::new_unknown(&COMPRESSED_SCHEMA.0),
            projection: None,
            limit,
            table_partition_cols: vec![],
            output_ordering: vec![QUERY_ORDER_SEGMENT.clone()],
            infinite_source: false,
        };

        let maybe_physical_parquet_predicates =
            if let Some(parquet_predicates) = maybe_parquet_predicates {
                Some(convert_logical_expr_to_physical_expr(
                    &parquet_predicates,
                    COMPRESSED_SCHEMA.0.clone(),
                )?)
            } else {
                None
            };

        let apache_parquet_exec = Arc::new(
            ParquetExec::new(file_scan_config, maybe_physical_parquet_predicates, None)
                .with_pushdown_filters(true)
                .with_reorder_filters(true),
        );

        // Create the gridding operator.
        let maybe_physical_grid_predicates = if let Some(grid_predicates) = maybe_grid_predicates {
            Some(convert_logical_expr_to_physical_expr(
                &grid_predicates,
                QUERY_SCHEMA.0.clone(),
            )?)
        } else {
            None
        };

        Ok(GridExec::new(
            maybe_physical_grid_predicates,
            limit,
            apache_parquet_exec,
        ))
    }
}

/// Rewrite and combine the `filters` that is written in terms of the model table's query schema, to
/// a filter that is written in terms of the schema used for compressed segments by the storage
/// engine and a filter that is written in terms of the schema used for univariate time series by
/// [`GridExec`] for its output. If the filters cannot be rewritten [`None`] is returned for both.
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

/// Rewrite the `filter` that is written in terms of the model table's query schema, to a filter
/// that is written in terms of the schema used for compressed segments by the storage engine and a
/// filter that is written in terms of the schema used for univariate time series by [`GridExec`].
/// If the filter cannot be rewritten, [`None`] is returned.
fn rewrite_filter(query_schema: &SchemaRef, filter: &Expr) -> Option<(Expr, Expr)> {
    match filter {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if let Expr::Column(column) = &**left {
                // unwrap() is safe as it has already been checked that the fields exists.
                let field = query_schema.field_with_name(&column.name).unwrap();
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

/// Convert `expr` to a [`PhysicalExpr`] with the types in `query_schema`.
fn convert_logical_expr_to_physical_expr(
    expr: &Expr,
    query_schema: SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>> {
    let df_query_schema = query_schema.clone().to_dfschema()?;
    planner::create_physical_expr(
        expr,
        &df_query_schema,
        &query_schema,
        &ExecutionProps::new(),
    )
}

#[async_trait]
impl TableProvider for ModelTable {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the query schema of the model table registered with Apache Arrow DataFusion.
    fn schema(&self) -> SchemaRef {
        self.model_table_metadata.query_schema.clone()
    }

    /// Specify that model tables are base tables and not views or temporary tables.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Specify that model tables performs inexact predicate push-down.
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    /// Create an [`ExecutionPlan`] that will scan the table. Returns a [`DataFusionError::Plan`] if
    /// the necessary metadata cannot be retrieved from the metadata database.
    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create shorthands for the metadata used during planning to improve readability.
        let table_name = self.model_table_metadata.name.as_str();
        let schema = &self.model_table_metadata.query_schema;
        let tag_column_indices = &self.model_table_metadata.tag_column_indices;
        let query_schema = &self.model_table_metadata.query_schema;
        let generated_columns = &self.model_table_metadata.generated_columns;

        // Ensures a projection is always present for looking up the columns to return.
        let mut projection: Vec<usize> = if let Some(projection) = projection {
            projection.to_vec()
        } else {
            (0..query_schema.fields().len()).collect()
        };

        // Compute the query schema for the record batches that Apache Arrow DataFusion needs to
        // execute the query. unwrap() is safe as the projection is based on the query schema.
        let query_schema_after_projection = Arc::new(query_schema.project(&projection).unwrap());

        // Ensure that the columns which are required for any generated columns in the projection
        // are present in the record batches returned by SortedJoinStream and compute its schema.
        // GeneratedAsStream assumes that columns that are only used to generate columns are last.
        let mut generated_columns_sources: HashSet<usize> =
            HashSet::with_capacity(query_schema.fields().len());
        for query_schema_index in &projection {
            if let Some(generated_column) = &generated_columns[*query_schema_index] {
                generated_columns_sources.extend(&generated_column.source_columns);
            }
        }

        for generated_column_source in generated_columns_sources {
            if !projection.contains(&generated_column_source) {
                projection.push(generated_column_source);
            }
        }

        let schema_after_projection = self.apply_query_schema_projection_to_schema(&projection);

        // Compute the metadata needed to create the columns in the query in the correct order.
        let mut stored_columns_in_projection: Vec<SortedJoinColumnType> =
            Vec::with_capacity(projection.len());
        let mut stored_field_columns_in_projection: Vec<u16> =
            Vec::with_capacity(query_schema.fields.len() - 1 - tag_column_indices.len());
        let mut stored_tag_columns_in_projection: Vec<&str> =
            Vec::with_capacity(tag_column_indices.len());
        let mut generated_columns_in_projection: Vec<ColumnToGenerate> =
            Vec::with_capacity(query_schema.fields.len() - schema.fields().len());

        for (result_index, query_schema_index) in projection.iter().enumerate() {
            if *query_schema.field(*query_schema_index).data_type() == ArrowTimestamp::DATA_TYPE {
                // Timestamp.
                stored_columns_in_projection.push(SortedJoinColumnType::Timestamp);
            } else if tag_column_indices.contains(query_schema_index) {
                // Tag.
                stored_tag_columns_in_projection
                    .push(query_schema.fields[*query_schema_index].name());
                stored_columns_in_projection.push(SortedJoinColumnType::Tag);
            } else if let Some(generated_column) = &generated_columns[*query_schema_index] {
                // Generated field.
                let physical_expr = convert_logical_expr_to_physical_expr(
                    &generated_column.expr,
                    schema_after_projection.clone(),
                )?;

                generated_columns_in_projection
                    .push(ColumnToGenerate::new(result_index, physical_expr));
            } else {
                // Stored field. unwrap() is safe as all stored columns are in both of the schemas.
                let schema_index = self.query_schema_index_to_schema_index(*query_schema_index);
                stored_field_columns_in_projection.push(schema_index.unwrap() as u16);
                stored_columns_in_projection.push(SortedJoinColumnType::Field);
            }
        }

        // TODO: extract all of the predicates that consist of tag = tag_value from the query so the
        // segments can be pruned by univariate_id in ParquetExec and hash_to_tags can be minimized.
        let (maybe_parquet_predicates, maybe_grid_predicates) =
            rewrite_and_combine_filters(schema, filters);

        // Compute a mapping from hashes to the requested tag values in the requested order. If the
        // server is a cloud node, use the table metadata manager for the remote metadata database.
        let configuration_manager = self.context.configuration_manager.read().await;
        let hash_to_tags = if let (ServerMode::Cloud, ClusterMode::MultiNode(manager)) = (
            &configuration_manager.server_mode,
            &configuration_manager.cluster_mode,
        ) {
            // unwrap() is safe since cloud nodes always have access to the remote metadata database.
            manager
                .table_metadata_manager
                .clone()
                .unwrap()
                .mapping_from_hash_to_tags(table_name, &stored_tag_columns_in_projection)
                .await
        } else {
            self.context
                .table_metadata_manager
                .mapping_from_hash_to_tags(table_name, &stored_tag_columns_in_projection)
                .await
        }
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;

        // unwrap() is safe as the store is set by create_session_context().
        let query_object_store = ctx
            .runtime_env()
            .object_store(&self.object_store_url)
            .unwrap();

        // At least one field column must be read for the univariate_ids and timestamps.
        if stored_field_columns_in_projection.is_empty() {
            stored_field_columns_in_projection.push(self.fallback_field_column);
        }

        // Request the matching files from the storage engine and construct one or more GridExecs
        // and a SortedJoinExec. The write lock on the storage engine is held until object metas for
        // all columns have been retrieved to ensure they have the same number of data points.
        let mut field_column_execution_plans: Vec<Arc<dyn ExecutionPlan>> =
            Vec::with_capacity(stored_field_columns_in_projection.len());

        let mut storage_engine = self.context.storage_engine.write().await;

        for field_column_index in stored_field_columns_in_projection {
            let execution_plan = self
                .scan_column(
                    &mut storage_engine,
                    &query_object_store,
                    table_name,
                    field_column_index,
                    maybe_parquet_predicates.clone(),
                    maybe_grid_predicates.clone(),
                    limit,
                )
                .await?;

            field_column_execution_plans.push(execution_plan);
        }

        let sorted_join_exec = SortedJoinExec::new(
            schema_after_projection,
            stored_columns_in_projection,
            Arc::new(hash_to_tags),
            field_column_execution_plans,
        );

        // Only include GeneratedAsExec in the query plan if there are columns to generate.
        if generated_columns_in_projection.is_empty() {
            Ok(sorted_join_exec)
        } else {
            Ok(GeneratedAsExec::new(
                query_schema_after_projection,
                generated_columns_in_projection,
                sorted_join_exec,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::logical_expr::lit;
    use datafusion::prelude::Expr;
    use modelardb_common::test;
    use modelardb_common::types::Timestamp;

    const TIMESTAMP_PREDICATE_VALUE: Timestamp = 37;

    // Tests for rewrite_and_combine_filters().
    #[test]
    fn test_rewrite_empty_vec() {
        let schema = test::model_table_metadata().schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &[]);
        assert!(parquet_filter.is_none());
        assert!(grid_filter.is_none());
    }

    #[test]
    fn test_rewrite_greater_than_timestamp() {
        let filters = new_timestamp_filters(Operator::Gt);
        let schema = test::model_table_metadata().schema;
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
        let schema = test::model_table_metadata().schema;
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
        let schema = test::model_table_metadata().schema;
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
        let schema = test::model_table_metadata().schema;
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
        let schema = test::model_table_metadata().schema;
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
