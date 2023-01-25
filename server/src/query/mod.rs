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

//! Implementation of the types required to query model tables through Apache Arrow DataFusion. The
//! types are:
//! * [`ModelTable`] which takes the projection, filters as [`Exprs`](Expr), and limit as input and
//! produces the part of a physical query plan that produces the data points required for the query.
//! * [`GridExec`] which takes the sorted compressed segments from the Apache Parquet files for a
//! single column and reconstructs the data points they represent as three sorted arrays containing
//! the data points' univariate ids, timestamps, and values.
//! * `SortedJoinExec` which take the sorted arrays produced by each [`GridExec`] and combines them
//! with the time series tags retrieved from the
//! [`MetadataManager`](crate::metadata::MetadataManager) to create the complete results containing
//! a timestamp column, one or more field columns, and zero or more tag columns.
// TODO: implemented SortedJoinExec and make its `` a link.
// TODO: determine if ParquetExec filtering is enough and the extra FilterExec can be removed.

// Public so the rules added to Apache Arrow DataFusion's physical optimizer can access GridExec.
pub mod grid_exec;

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field, Schema, SchemaRef};
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
use datafusion::physical_plan::{
    file_format::FileScanConfig, file_format::ParquetExec, filter::FilterExec, ExecutionPlan,
    Statistics,
};

use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::query::grid_exec::GridExec;
use crate::storage;
use crate::types::{ArrowTimestamp, ArrowUnivariateId, ArrowValue, CompressedSchema};
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
    /// Schema of the model table registered with Apache Arrow DataFusion.
    schema: Arc<Schema>,
    /// Field column to use for queries that do not include fields.
    fallback_field_column: u64,
}

impl ModelTable {
    pub fn new(context: Arc<Context>, model_table_metadata: Arc<ModelTableMetadata>) -> Arc<Self> {
        // Default columns in the model table to be registered with Apache Arrow DataFusion.
        let mut columns = vec![
            Field::new("univariate_id", ArrowUnivariateId::DATA_TYPE, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("value", ArrowValue::DATA_TYPE, false),
        ];

        // Tag columns in the model table to be registered with Apache Arrow DataFusion.
        for index in &model_table_metadata.tag_column_indices {
            columns.push(model_table_metadata.schema.field(*index).clone());
        }

        // Compute the index of the first field column in the model table's
        // schema. This is used for queries that does not contain any fields.
        let fallback_field_column = {
            model_table_metadata
                .schema
                .fields()
                .iter()
                .position(|field| field.data_type() == &ArrowValue::DATA_TYPE)
                .unwrap() // unwrap() is safe as model tables contains fields.
        };

        // unwrap() is safe as the url is predefined as a constant in storage.
        let object_store_url =
            ObjectStoreUrl::parse(storage::QUERY_DATA_FOLDER_SCHEME_WITH_HOST).unwrap();

        Arc::new(ModelTable {
            context,
            model_table_metadata,
            object_store_url,
            schema: Arc::new(Schema::new(columns)),
            fallback_field_column: fallback_field_column as u64,
        })
    }

    /// Return the [`ModelTableMetadata`] for the table.
    pub fn model_table_metadata(&self) -> Arc<ModelTableMetadata> {
        self.model_table_metadata.clone()
    }
}

/// Rewrite `filters` in terms of the model table's schema to filters in
/// terms of the schema used for compressed data by the storage engine. The
/// rewritten filters are then combined into a single [`Expr`]. A [`None`]
/// is returned if `filters` is empty.
fn rewrite_and_combine_filters(filters: &[Expr]) -> Option<Expr> {
    let rewritten_filters: Vec<Expr> = filters
        .iter()
        .map(|filter| match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if **left == logical_expr::col("timestamp") {
                    match op {
                        Operator::Gt => {
                            new_binary_expr(logical_expr::col("end_time"), *op, *right.clone())
                        }
                        Operator::GtEq => {
                            new_binary_expr(logical_expr::col("end_time"), *op, *right.clone())
                        }
                        Operator::Lt => {
                            new_binary_expr(logical_expr::col("start_time"), *op, *right.clone())
                        }
                        Operator::LtEq => {
                            new_binary_expr(logical_expr::col("start_time"), *op, *right.clone())
                        }
                        Operator::Eq => new_binary_expr(
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
                        _ => filter.clone(),
                    }
                } else if **left == logical_expr::col("value") {
                    match op {
                        Operator::Gt => {
                            new_binary_expr(logical_expr::col("max_value"), *op, *right.clone())
                        }
                        Operator::GtEq => {
                            new_binary_expr(logical_expr::col("max_value"), *op, *right.clone())
                        }
                        Operator::Lt => {
                            new_binary_expr(logical_expr::col("min_value"), *op, *right.clone())
                        }
                        Operator::LtEq => {
                            new_binary_expr(logical_expr::col("min_value"), *op, *right.clone())
                        }
                        Operator::Eq => new_binary_expr(
                            new_binary_expr(
                                logical_expr::col("min_value"),
                                Operator::LtEq,
                                *right.clone(),
                            ),
                            Operator::And,
                            new_binary_expr(
                                logical_expr::col("max_value"),
                                Operator::GtEq,
                                *right.clone(),
                            ),
                        ),
                        _ => filter.clone(),
                    }
                } else {
                    filter.clone()
                }
            }
            _ => filter.clone(),
        })
        .collect();

    // Combine the rewritten filters into an expression.
    utils::conjunction(rewritten_filters)
}

/// Create a [`Expr::BinaryExpr`].
fn new_binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

/// Create a [`FilterExec`]. [`None`] is returned if `predicate` is
/// [`None`].
fn new_filter_exec(
    predicate: &Option<Expr>,
    input: &Arc<ParquetExec>,
    compressed_schema: &CompressedSchema,
) -> Result<Arc<dyn ExecutionPlan>> {
    let predicate = predicate
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("predicate is None".to_owned()))?;

    let schema = &compressed_schema.0;
    let df_schema = schema.clone().to_dfschema()?;

    let physical_predicate =
        planner::create_physical_expr(predicate, &df_schema, schema, &ExecutionProps::new())?;

    Ok(Arc::new(FilterExec::try_new(
        physical_predicate,
        input.clone(),
    )?))
}

#[async_trait]
impl TableProvider for ModelTable {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the schema of the model table registered with Apache Arrow
    /// DataFusion.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        // Request the matching files from the storage engine.
        let table_name = &self.model_table_metadata.name;
        let object_metas = {
            // TODO: make the storage engine support multiple parallel readers.
            let mut storage_engine = self.context.storage_engine.write().await;

            // unwrap() is safe as the store is set by create_session_context().
            let query_object_store = ctx
                .runtime_env()
                .object_store(&self.object_store_url)
                .unwrap();

            // TODO: extract predicates on time and value and push them to the storage engine.
            // unwrap() is safe to use as get_compressed_files() only fails if a table with the name
            // table_name does not exists, if end time is before start time, or if max value is
            // larger than min value.
            let column_index = 4; // TODO: use the column_index assigned to the pipeline.
            storage_engine
                .compressed_files(
                    table_name,
                    column_index,
                    None,
                    None,
                    None,
                    None,
                    &query_object_store,
                )
                .await
                .unwrap()
        };

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

        // TODO: partition the rows in the files to support parallel processing.
        let file_scan_config = FileScanConfig {
            object_store_url: self.object_store_url.clone(),
            file_schema: self.context.metadata_manager.compressed_schema().0,
            file_groups: vec![partitioned_files],
            statistics,
            projection: None,
            limit,
            table_partition_cols: vec![],
            output_ordering: None,
            infinite_source: false,
        };

        // TODO: extract all of the predicates that consist of tag = tag_value from the query so the
        // row groups can be pruned by univariate_id using ParquetExec and segments can be pruned by
        // univariate_id using FilterExec before reconstructing data points or computing aggregates.
        let tag_predicates = vec![];
        let _univariate_ids = self
            .context
            .metadata_manager
            .compute_univariate_ids_using_fields_and_tags(
                &self.model_table_metadata.name,
                projection,
                self.fallback_field_column,
                &tag_predicates,
            )
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;

        let predicate = rewrite_and_combine_filters(filters);
        let apache_parquet_exec = Arc::new(
            ParquetExec::new(file_scan_config, predicate.clone(), None)
                .with_pushdown_filters(true)
                .with_reorder_filters(true),
        );

        // Create a filter operator if filters are not empty.
        let compressed_schema = self.context.metadata_manager.compressed_schema();
        let input = new_filter_exec(&predicate, &apache_parquet_exec, &compressed_schema)
            .unwrap_or(apache_parquet_exec);

        // Ensures a projection is present for looking up columns to return.
        let projection: Vec<usize> = if let Some(projection) = projection {
            projection.to_vec()
        } else {
            (0..self.schema.fields().len()).collect()
        };

        // Compute a mapping from hashes to tags.
        let tag_names_in_projection: Vec<&str> = projection
            .iter()
            .filter_map(|index| {
                // Tags are appended to the schema univariate_id, timestamp, value
                if *index > 2 {
                    let i = self.model_table_metadata.tag_column_indices[index - 3];
                    Some(self.model_table_metadata.schema.fields[i].name().as_str())
                } else {
                    None
                }
            })
            .collect();

        let hash_to_tags = self
            .context
            .metadata_manager
            .mapping_from_hash_to_tags(&self.model_table_metadata.name, &tag_names_in_projection)
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;

        // Create the gridding operator.
        let grid_exec: Arc<dyn ExecutionPlan> = GridExec::new(
            self.model_table_metadata.clone(),
            projection,
            limit,
            self.schema(),
            hash_to_tags,
            input,
        );

        Ok(grid_exec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::lit;
    use datafusion::prelude::Expr;

    use crate::metadata::test_util;
    use crate::types::{Timestamp, Value};

    const TIMESTAMP_PREDICATE_VALUE: Timestamp = 37;
    const VALUE_PREDICATE_VALUE: Value = 73.00;

    // Tests for rewrite_and_combine_filters().
    #[test]
    fn test_rewrite_empty_vec() {
        assert!(rewrite_and_combine_filters(&[]).is_none());
    }

    #[test]
    fn test_rewrite_greater_than_timestamp() {
        let filters = new_timestamp_filters(Operator::Gt);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_binary_expr(
            predicate,
            "end_time",
            Operator::Gt,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_greater_than_or_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::GtEq);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_binary_expr(
            predicate,
            "end_time",
            Operator::GtEq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_less_than_timestamp() {
        let filters = new_timestamp_filters(Operator::Lt);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_binary_expr(
            predicate,
            "start_time",
            Operator::Lt,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_less_than_or_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::LtEq);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_binary_expr(
            predicate,
            "start_time",
            Operator::LtEq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::Eq);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();

        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = predicate {
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
    }

    fn new_timestamp_filters(operator: Operator) -> Vec<Expr> {
        vec![new_binary_expr(
            logical_expr::col("timestamp"),
            operator,
            lit(TIMESTAMP_PREDICATE_VALUE),
        )]
    }

    #[test]
    fn test_rewrite_greater_than_value() {
        let filters = new_value_filters(Operator::Gt);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_binary_expr(
            predicate,
            "max_value",
            Operator::Gt,
            lit(VALUE_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_greater_than_or_equal_value() {
        let filters = new_value_filters(Operator::GtEq);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_binary_expr(
            predicate,
            "max_value",
            Operator::GtEq,
            lit(VALUE_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_less_than_value() {
        let filters = new_value_filters(Operator::Lt);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_binary_expr(
            predicate,
            "min_value",
            Operator::Lt,
            lit(VALUE_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_less_than_or_equal_value() {
        let filters = new_value_filters(Operator::LtEq);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_binary_expr(
            predicate,
            "min_value",
            Operator::LtEq,
            lit(VALUE_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_equal_value() {
        let filters = new_value_filters(Operator::Eq);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();

        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = predicate {
            assert_binary_expr(
                *left,
                "min_value",
                Operator::LtEq,
                lit(VALUE_PREDICATE_VALUE),
            );
            assert_eq!(op, Operator::And);
            assert_binary_expr(
                *right,
                "max_value",
                Operator::GtEq,
                lit(VALUE_PREDICATE_VALUE),
            );
        } else {
            panic!("Expr is not a BinaryExpr.");
        }
    }

    fn new_value_filters(operator: Operator) -> Vec<Expr> {
        vec![new_binary_expr(
            logical_expr::col("value"),
            operator,
            lit(VALUE_PREDICATE_VALUE),
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

    // Tests for new_filter_exec().
    #[test]
    fn test_new_filter_exec_without_predicates() {
        let apache_parquet_exec = new_apache_parquet_exec();
        assert!(
            new_filter_exec(&None, &apache_parquet_exec, &test_util::compressed_schema()).is_err()
        );
    }

    #[test]
    fn test_new_filter_exec_with_predicates() {
        let filters = vec![new_binary_expr(
            logical_expr::col("univariate_id"),
            Operator::Eq,
            lit(1_u64),
        )];
        let predicates = rewrite_and_combine_filters(&filters);
        let apache_parquet_exec = new_apache_parquet_exec();

        assert!(new_filter_exec(
            &predicates,
            &apache_parquet_exec,
            &test_util::compressed_schema()
        )
        .is_ok());
    }

    fn new_apache_parquet_exec() -> Arc<ParquetExec> {
        let file_scan_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema: Arc::new(Schema::new(vec![Field::new(
                "model_type_id",
                DataType::UInt8,
                false,
            )])),
            file_groups: vec![],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: None,
            infinite_source: false,
        };
        Arc::new(ParquetExec::new(file_scan_config, None, None))
    }
}
