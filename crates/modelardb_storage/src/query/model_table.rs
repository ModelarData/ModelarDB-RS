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

//! Implementation of [`ModelTable`] which allows model tables to be queried through Apache
//! DataFusion. It takes the projection, filters as [`Exprs`](Expr), and limit of a query as input
//! and returns a physical query plan that produces all the data points required for the query.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::result::Result as StdResult;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Schema, SchemaRef, TimeUnit,
};
use datafusion::catalog::Session;
use datafusion::common::{Statistics, ToDFSchema};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::datasource::provider::TableProviderFilterPushDown;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{self, utils, BinaryExpr, Expr, Operator};
use datafusion::physical_expr::{planner, LexOrdering};
use datafusion::physical_plan::insert::{DataSink, DataSinkExec};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use deltalake::kernel::LogicalFile;
use deltalake::{DeltaTable, DeltaTableError, ObjectMeta, PartitionFilter, PartitionValue};
use modelardb_types::schemas::{DISK_QUERY_COMPRESSED_SCHEMA, FIELD_COLUMN, GRID_SCHEMA};
use modelardb_types::types::{ArrowTimestamp, ArrowValue};

use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::metadata::table_metadata_manager::TableMetadataManager;
use crate::query::generated_as_exec::{ColumnToGenerate, GeneratedAsExec};
use crate::query::grid_exec::GridExec;
use crate::query::sorted_join_exec::{SortedJoinColumnType, SortedJoinExec};

use super::QUERY_ORDER_SEGMENT;

/// A queryable representation of a model table which stores multivariate time series as segments
/// containing metadata and models. [`ModelTable`] implements [`TableProvider`] so it can be
/// registered with Apache DataFusion and the multivariate time series queried as multiple
/// univariate time series.
pub(crate) struct ModelTable {
    /// Access to the Delta Lake table.
    delta_table: DeltaTable,
    /// Metadata for the model table.
    model_table_metadata: Arc<ModelTableMetadata>,
    /// Where data should be written to.
    data_sink: Arc<dyn DataSink>,
    /// Access to metadata related to tables.
    table_metadata_manager: Arc<TableMetadataManager>,
    /// Field column to use for queries that do not include fields.
    fallback_field_column: u16,
}

impl ModelTable {
    pub(crate) fn new(
        delta_table: DeltaTable,
        table_metadata_manager: Arc<TableMetadataManager>,
        model_table_metadata: Arc<ModelTableMetadata>,
        data_sink: Arc<dyn DataSink>,
    ) -> Arc<Self> {
        // Compute the index of the first stored field column in the model table's query schema. It
        // is used for queries without fields as uids, timestamps, and values are stored together.
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

        Arc::new(ModelTable {
            delta_table,
            model_table_metadata,
            data_sink,
            table_metadata_manager,
            fallback_field_column,
        })
    }

    /// Return the [`ModelTableMetadata`] for the model table.
    pub(crate) fn model_table_metadata(&self) -> Arc<ModelTableMetadata> {
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
    fn query_schema_index_to_schema_index(
        &self,
        query_schema_index: usize,
    ) -> DataFusionResult<usize> {
        let query_schema = &self.model_table_metadata.query_schema;
        let column_name = query_schema.field(query_schema_index).name();
        Ok(self.model_table_metadata.schema.index_of(column_name)?)
    }
}

/// The implementation is not derived many instance variables does not implement [`fmt::Debug`].
impl fmt::Debug for ModelTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ModelTable: {}\n {:?}",
            self.model_table_metadata.name, self.model_table_metadata
        )
    }
}

/// Rewrite and combine the `filters` that are written in terms of the model table's query schema,
/// to a filter that is written in terms of the schema used for compressed segments by the storage
/// engine and a filter that is written in terms of the schema used for univariate time series by
/// [`GridExec`] for its output. If the filters cannot be rewritten an empty [`None`] is returned.
fn rewrite_and_combine_filters(
    schema: &SchemaRef,
    filters: &[Expr],
) -> (Option<Expr>, Option<Expr>) {
    let rewritten_filters = filters
        .iter()
        .filter_map(|filter| rewrite_filter(schema, filter));

    let (parquet_rewritten_filters, grid_rewritten_filters): (Vec<Expr>, Vec<Expr>) =
        rewritten_filters.unzip();

    let maybe_rewritten_parquet_filters = utils::conjunction(parquet_rewritten_filters);
    let maybe_rewritten_grid_filters = utils::conjunction(grid_rewritten_filters);

    (
        maybe_rewritten_parquet_filters,
        maybe_rewritten_grid_filters,
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
                if *field.data_type() == DataType::Timestamp(TimeUnit::Microsecond, None) {
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
            } else if let Expr::Column(column) = &**right {
                // unwrap() is safe as it has already been checked that the fields exists.
                let field = query_schema.field_with_name(&column.name).unwrap();
                // Type aliases cannot be used as a constructor and thus cannot be used here.
                if *field.data_type() == DataType::Timestamp(TimeUnit::Microsecond, None) {
                    match op {
                        Operator::Gt | Operator::GtEq => Some((
                            new_binary_expr(*left.clone(), *op, logical_expr::col("start_time")),
                            new_binary_expr(*left.clone(), *op, logical_expr::col("timestamp")),
                        )),
                        Operator::Lt | Operator::LtEq => Some((
                            new_binary_expr(*left.clone(), *op, logical_expr::col("end_time")),
                            new_binary_expr(*left.clone(), *op, logical_expr::col("timestamp")),
                        )),
                        Operator::Eq => Some((
                            new_binary_expr(
                                new_binary_expr(
                                    *left.clone(),
                                    Operator::LtEq,
                                    logical_expr::col("start_time"),
                                ),
                                Operator::And,
                                new_binary_expr(
                                    *left.clone(),
                                    Operator::GtEq,
                                    logical_expr::col("end_time"),
                                ),
                            ),
                            new_binary_expr(*right.clone(), *op, logical_expr::col("timestamp")),
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

/// Convert `expr` to a [`Option<PhysicalExpr>`] with the types in `query_schema`.
fn maybe_convert_logical_expr_to_physical_expr(
    maybe_expr: Option<&Expr>,
    query_schema: SchemaRef,
) -> DataFusionResult<Option<Arc<dyn PhysicalExpr>>> {
    // Option.map() is not used so errors can be returned with ?.
    if let Some(maybe_expr) = maybe_expr {
        Ok(Some(convert_logical_expr_to_physical_expr(
            maybe_expr,
            query_schema,
        )?))
    } else {
        Ok(None)
    }
}

/// Convert `expr` to a [`PhysicalExpr`] with the types in `query_schema`.
fn convert_logical_expr_to_physical_expr(
    expr: &Expr,
    query_schema: SchemaRef,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    let df_query_schema = query_schema.clone().to_dfschema()?;
    planner::create_physical_expr(expr, &df_query_schema, &ExecutionProps::new())
}

/// Create an [`ExecutionPlan`] that will return the compressed segments that represent the data
/// points for `field_column_index` in `delta_table`. Returns a [`DataFusionError`] if the necessary
/// metadata cannot be retrieved from the metadata Delta Lake.
fn new_apache_parquet_exec(
    delta_table: &DeltaTable,
    partition_filters: &[PartitionFilter],
    maybe_limit: Option<usize>,
    maybe_parquet_filters: &Option<Arc<dyn PhysicalExpr>>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    // Collect the LogicalFiles into a Vec so they can be sorted the same for all field columns.
    let mut logical_files = delta_table
        .get_active_add_actions_by_partitions(partition_filters)
        .map_err(|error| DataFusionError::Plan(error.to_string()))?
        .collect::<StdResult<Vec<LogicalFile>, DeltaTableError>>()
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;

    // TODO: prune the Apache Parquet files using metadata and maybe_parquet_filters if possible.
    logical_files.sort_by_key(|logical_file| logical_file.modification_time());

    // Create the data source operator. Assumes the ObjectStore exists.
    let partitioned_files = logical_files
        .iter()
        .map(|logical_file| logical_file_to_partitioned_file(logical_file))
        .collect::<DataFusionResult<Vec<PartitionedFile>>>()?;

    // TODO: give the optimizer more info for timestamps and values through statistics, e.g, min
    // can be computed using only the metadata Delta Lake due to the aggregate_statistics rule.
    let log_store = delta_table.log_store();
    let file_scan_config = FileScanConfig {
        object_store_url: log_store.object_store_url(),
        file_schema: DISK_QUERY_COMPRESSED_SCHEMA.0.clone(),
        file_groups: vec![partitioned_files],
        statistics: Statistics::new_unknown(&DISK_QUERY_COMPRESSED_SCHEMA.0),
        projection: None,
        limit: maybe_limit,
        table_partition_cols: vec![],
        output_ordering: vec![LexOrdering::new(QUERY_ORDER_SEGMENT.to_vec())],
    };

    let apache_parquet_exec_builder = if let Some(parquet_filters) = maybe_parquet_filters {
        ParquetExec::builder(file_scan_config).with_predicate(parquet_filters.clone())
    } else {
        ParquetExec::builder(file_scan_config)
    };

    let apache_parquet_exec = apache_parquet_exec_builder
        .build()
        .with_pushdown_filters(true)
        .with_reorder_filters(true);

    Ok(Arc::new(apache_parquet_exec))
}

// Convert the [`LogicalFile`] `logical_file` to a [`PartitionFilter`]. A [`DataFusionError`] is
// returned if the time the file was last modified cannot be read from `logical_file`.
fn logical_file_to_partitioned_file(
    logical_file: &LogicalFile,
) -> DataFusionResult<PartitionedFile> {
    let last_modified = logical_file
        .modification_datetime()
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;

    let object_meta = ObjectMeta {
        location: logical_file.object_store_path(),
        last_modified,
        size: logical_file.size() as usize,
        e_tag: None,
        version: None,
    };

    let partitioned_file = PartitionedFile {
        object_meta,
        partition_values: vec![],
        range: None,
        statistics: None,
        extensions: None,
    };

    Ok(partitioned_file)
}

#[async_trait]
impl TableProvider for ModelTable {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the query schema of the model table registered with Apache DataFusion.
    fn schema(&self) -> SchemaRef {
        self.model_table_metadata.query_schema.clone()
    }

    /// Specify that model tables are base tables and not views or temporary tables.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Create an [`ExecutionPlan`] that will scan the model table. Returns a [`DataFusionError::Plan`]
    /// if the necessary metadata cannot be retrieved.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Create shorthands for the metadata used during planning to improve readability.
        let table_name = self.model_table_metadata.name.as_str();
        let schema = &self.model_table_metadata.schema;
        let tag_column_indices = &self.model_table_metadata.tag_column_indices;
        let query_schema = &self.model_table_metadata.query_schema;
        let generated_columns = &self.model_table_metadata.generated_columns;

        // Clone the Delta Lake table and update it to the latest version. self.delta_lake.load(
        // &mut self) is not an option due to TypeProvider::scan(&self, ...). Storing the DeltaTable
        // in a Mutex and RwLock is also not an option since most of the methods in TypeProvider
        // return a reference and the locks will be dropped at the end of the method.
        let mut delta_table = self.delta_table.clone();
        delta_table
            .load()
            .await
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;

        // Register the object store as done in DeltaTable so paths are from the table root.
        let log_store = delta_table.log_store();
        let object_store_url = log_store.object_store_url();
        state
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), log_store.object_store());

        // Ensures a projection is always present for looking up the columns to return.
        let mut projection: Vec<usize> = if let Some(projection) = projection {
            projection.to_vec()
        } else {
            (0..query_schema.fields().len()).collect()
        };

        // Compute the query schema for the record batches that Apache DataFusion needs to execute
        // the query.
        let query_schema_after_projection = Arc::new(query_schema.project(&projection)?);

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
                // Stored field.
                let schema_index = self.query_schema_index_to_schema_index(*query_schema_index);
                stored_field_columns_in_projection.push(schema_index? as u16);
                stored_columns_in_projection.push(SortedJoinColumnType::Field);
            }
        }

        // TODO: extract all of the predicates that consist of tag = tag_value from the query so the
        // segments can be pruned by univariate_id in ParquetExec and hash_to_tags can be minimized.
        // Filters are not converted to PhysicalExpr in rewrite_and_combine_filters() to simplify
        // testing rewrite_and_combine_filters() as Expr can be compared while PhysicalExpr cannot.
        let (maybe_rewritten_parquet_filters, maybe_rewritten_grid_filters) =
            rewrite_and_combine_filters(schema, filters);

        let maybe_physical_parquet_filters = maybe_convert_logical_expr_to_physical_expr(
            maybe_rewritten_parquet_filters.as_ref(),
            DISK_QUERY_COMPRESSED_SCHEMA.0.clone(),
        )?;

        let maybe_physical_grid_filters = maybe_convert_logical_expr_to_physical_expr(
            maybe_rewritten_grid_filters.as_ref(),
            GRID_SCHEMA.0.clone(),
        )?;

        // Compute a mapping from hashes to the requested tag values in the requested order. If the
        // server is a cloud node, use the table metadata manager for the remote metadata Delta Lake.
        let hash_to_tags = self
            .table_metadata_manager
            .mapping_from_hash_to_tags(table_name, &stored_tag_columns_in_projection)
            .await
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;

        if stored_field_columns_in_projection.is_empty() {
            stored_field_columns_in_projection.push(self.fallback_field_column);
        }

        // Request the matching files from the Delta Lake table and construct one or more GridExecs
        // and a SortedJoinExec. The write lock on the storage engine is held until object metas for
        // all columns have been retrieved to ensure they have the same number of data points.
        let mut field_column_execution_plans: Vec<Arc<dyn ExecutionPlan>> =
            Vec::with_capacity(stored_field_columns_in_projection.len());

        let mut partition_filters = vec![PartitionFilter {
            key: FIELD_COLUMN.to_owned(),
            value: PartitionValue::Equal("".to_owned()),
        }];

        // An expression is added so it is simple to replace with one that filters by field column.
        for field_column_index in stored_field_columns_in_projection {
            partition_filters[0].value = PartitionValue::Equal(field_column_index.to_string());

            let parquet_exec = new_apache_parquet_exec(
                &delta_table,
                &partition_filters,
                limit,
                &maybe_physical_parquet_filters,
            )?;

            let grid_exec = GridExec::new(maybe_physical_grid_filters.clone(), limit, parquet_exec);

            field_column_execution_plans.push(grid_exec);
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

    /// Specify that model tables perform inexact predicate push-down.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_filter| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    /// Create an [`ExecutionPlan`] that will insert the result of `input` into the model table.
    /// `inputs` must include generated columns to match the query schema returned by
    /// [`TableProvider::schema()`]. The generated columns are immediately dropped. Generally,
    /// [`arrow_flight::flight_service_server::FlightService::do_put()`] should be used instead of
    /// this method as it is more efficient. This method cannot fail, but it must return a
    /// [`DataFusionError`] to match the [`TableProvider`] trait.
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let data_sink_exec = Arc::new(DataSinkExec::new(
            input,
            self.data_sink.clone(),
            self.model_table_metadata.schema.clone(),
            None,
        ));

        Ok(data_sink_exec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::logical_expr::lit;
    use datafusion::prelude::Expr;
    use modelardb_types::types::Timestamp;

    use crate::test;

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
