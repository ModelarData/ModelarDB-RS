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

use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as StdTaskContext, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BinaryArray, Float32Array, UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Schema, SchemaRef, TimeUnit,
};
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::{
    datasource::TableProviderFilterPushDown, listing::PartitionedFile, TableProvider, TableType,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{ExecutionProps, SessionState, TaskContext};
use datafusion::logical_plan::{col, combine_filters, Expr, Operator, ToDFSchema};
use datafusion::physical_plan::{
    expressions::PhysicalSortExpr, file_format::FileScanConfig, file_format::ParquetExec,
    filter::FilterExec, metrics::BaselineMetrics, metrics::ExecutionPlanMetricsSet,
    metrics::MetricsSet, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion_physical_expr::planner;
use futures::stream::{Stream, StreamExt};
use rusqlite::{Connection, Result as RusqliteResult};

use crate::catalog;
use crate::catalog::ModelTableMetadata;
use crate::models;
use crate::storage::StorageEngine;
use crate::types::{
    ArrowValue, TimeSeriesId, TimestampArray, TimestampBuilder, ValueArray, ValueBuilder,
};
use crate::Context;

pub struct ModelTable {
    context: Arc<Context>,
    object_store_url: ObjectStoreUrl,
    model_table_metadata: Arc<ModelTableMetadata>,
    schema: Arc<Schema>,
    fallback_field_column: u64,
}

impl ModelTable {
    pub fn new(context: Arc<Context>, model_table_metadata: &Arc<ModelTableMetadata>) -> Arc<Self> {
        // TODO: support reconstructing the ingested multivariate time series.
        let columns = vec![
            Field::new("tid", DataType::UInt64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float32, false),
        ];

        // Compute the column of the first field in schema. This column is used
        // when a query is received that only requests timestamps and tags.
        let fallback_field_column = {
            model_table_metadata
                .schema
                .fields()
                .iter()
                .position(|field| field.data_type() == &ArrowValue::DATA_TYPE)
                .unwrap() // unwrap() is safe as model tables contains fields.
        };

        Arc::new(ModelTable {
            context,
            model_table_metadata: model_table_metadata.clone(),
            object_store_url: ObjectStoreUrl::local_filesystem(),
            schema: Arc::new(Schema::new(columns)),
            fallback_field_column: fallback_field_column as u64,
        })
    }

    fn rewrite_and_combine_filters(&self, filters: &[Expr]) -> Option<Expr> {
        // TODO: rewrite the remaining filters for the remaining columns.
        let rewritten_filters: Vec<Expr> = filters
            .iter()
            .map(|filter| match filter {
                Expr::BinaryExpr { left, op, right } => {
                    if **left == col("timestamp") {
                        match op {
                            Operator::Gt => self.binary_expr(col("end_time"), *op, *right.clone()),
                            Operator::GtEq => {
                                self.binary_expr(col("end_time"), *op, *right.clone())
                            }
                            Operator::Lt => {
                                self.binary_expr(col("start_time"), *op, *right.clone())
                            }
                            Operator::LtEq => {
                                self.binary_expr(col("start_time"), *op, *right.clone())
                            }
                            Operator::Eq => self.binary_expr(
                                self.binary_expr(col("start_time"), Operator::LtEq, *right.clone()),
                                Operator::And,
                                self.binary_expr(col("end_time"), Operator::GtEq, *right.clone()),
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
        combine_filters(&rewritten_filters)
    }

    fn binary_expr(&self, left: Expr, op: Operator, right: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn add_filter_exec(
        &self,
        predicate: &Option<Expr>,
        input: &Arc<ParquetExec>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let predicate = predicate
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan("predicate is none".to_owned()))?;

        let schema = StorageEngine::get_compressed_segment_schema();
        let df_schema = schema.clone().to_dfschema()?;

        let physical_predicate =
            planner::create_physical_expr(predicate, &df_schema, &schema, &ExecutionProps::new())?;

        Ok(Arc::new(FilterExec::try_new(
            physical_predicate,
            input.clone(),
        )?))
    }

    // TODO:extract the field columns requested in the user's query.
    fn lookup_keys_from_tags(
        &self,
        table_name: &str,
        columns: &Option<Vec<usize>>,
        tag_predicates: &[(&str, &str)],
    ) -> Result<Vec<TimeSeriesId>> {
        // Open a connection to the database containing the metadata.
        let database_path = {
            // unwrap() is safe as read() only fails if the RwLock is poisoned.
            let catalog = self.context.catalog.read().unwrap();
            catalog.data_folder_path.join(catalog::METADATA_SQLITE_NAME)
        };

        // Construct the two queries that extract the field columns in the table
        // being queried and the hashes of the multivariate time series in the
        // table with tag values that match the tag values in the user's query.
        let query_field_columns = if columns.is_none() {
            format!(
                "SELECT column_index FROM model_table_field_columns WHERE table_name = '{}'",
                table_name
            )
        } else {
            let column: Vec<String> = columns
                .clone()
                .unwrap()
                .iter()
                .map(|column| format!("column_index = {}", column))
                .collect();

            format!(
                "SELECT column_index FROM model_table_field_columns WHERE table_name = '{}' AND {}",
                table_name,
                column.join(" OR ")
            )
        };

        let query_hashes = {
            if tag_predicates.is_empty() {
                format!("SELECT hash FROM {}_tags", table_name)
            } else {
                let predicates: Vec<String> = tag_predicates
                    .iter()
                    .map(|(tag, tag_value)| format!("{} = {}", tag, tag_value))
                    .collect();

                format!(
                    "SELECT hash FROM {}_tags WHERE {}",
                    table_name,
                    predicates.join(" AND ")
                )
            }
        };

        // Retrieve the hashes using the query and reconstruct the keys.
        self.lookup_keys_from_sqlite_database(&database_path, &query_field_columns, &query_hashes)
            .map_err(|error| DataFusionError::Plan(error.to_string()))
    }

    // TODO:extract the field columns requested in the user's query.
    fn lookup_keys_from_sqlite_database(
        &self,
        database_path: &PathBuf,
        query_field_columns: &str,
        query_hashes: &str,
    ) -> RusqliteResult<Vec<u64>> {
        let connection = Connection::open(database_path)?;

        // Retrieve the field columns.
        let mut select_statement = connection.prepare(query_field_columns)?;
        let mut rows = select_statement.query([])?;

        let mut field_columns = vec![];
        while let Some(row) = rows.next()? {
            field_columns.push(row.get::<usize, u64>(0)?);
        }

        // Add the fallback field column if the query did not request data for
        // any fields as the storage engine otherwise does not return any data.
        if field_columns.is_empty() {
            field_columns.push(self.fallback_field_column);
        }

        // Retrieve the hashes and compute the keys;
        let mut select_statement = connection.prepare(&query_hashes)?;
        let mut rows = select_statement.query([])?;

        let mut keys = vec![];
        while let Some(row) = rows.next()? {
            for field_column in &field_columns {
                keys.push(row.get::<usize, u64>(0)? | field_column);
            }
        }
        Ok(keys)
    }
}

#[async_trait]
impl TableProvider for ModelTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: extract predicates that consist of tag = tag_value from the query.
        let tag_predicates = vec![];
        let keys = self.lookup_keys_from_tags(
            &self.model_table_metadata.name,
            projection,
            &tag_predicates,
        )?;

        // Request the matching files from the storage engine.
        let mut object_metas = {
            // unwrap() is safe as read() only fails if the RwLock is poisoned.
            let mut storage_engines = self.context.storage_engine.write().unwrap();

            // unwrap() is safe to use as get_compressed_files() only fails if a
            // non-existing hash is passed or if end time is before start time.
            // TODO: change the storage engine to allow multiple readers.
            storage_engines
                .get_compressed_files(&keys, None, None)
                .unwrap()
        };

        //Create the data source node. Assumes the ObjectStore already exists.
        let partitioned_files: Vec<PartitionedFile> = object_metas
            .drain(0..)
            .map(|object_meta| PartitionedFile {
                object_meta,
                partition_values: vec![],
                range: None,
                extensions: None,
            })
            .collect::<Vec<PartitionedFile>>();

        //TODO: accumulate the size out the input data after filtering.
        let statistics = Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        };

        //TODO: partition and limit the number of rows read from the files.
        let file_scan_config = FileScanConfig {
            object_store_url: self.object_store_url.clone(),
            file_schema: Arc::new(StorageEngine::get_compressed_segment_schema()),
            file_groups: vec![partitioned_files],
            statistics,
            projection: None,
            limit,
            table_partition_cols: vec![],
        };

        let predicate = self.rewrite_and_combine_filters(filters);
        let parquet_exec = Arc::new(ParquetExec::new(file_scan_config, predicate.clone(), None));
        let input = self
            .add_filter_exec(&predicate, &parquet_exec)
            .unwrap_or(parquet_exec);

        //Create the grid node
        let grid_exec: Arc<dyn ExecutionPlan> = GridExec::new(
            self.model_table_metadata.clone(),
            projection,
            limit,
            self.schema(),
            input,
        );
        Ok(grid_exec)
    }
}

/* ExecutionPlan */
//GridExec is public so the physical optimizer rules can pattern match on it, and
//model_table_metadata is public so the rules use it as context is not in their scope.
#[derive(Debug, Clone)]
pub struct GridExec {
    pub model_table_metadata: Arc<ModelTableMetadata>,
    projection: Vec<usize>,
    limit: Option<usize>,
    schema_after_projection: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl GridExec {
    pub fn new(
        model_table_metadata: Arc<ModelTableMetadata>,
        projection: &Option<Vec<usize>>,
        limit: Option<usize>,
        schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<Self> {
        //Modifies the schema so it matches the passed projection
        let schema_after_projection = if let Some(ref projection) = projection {
            Arc::new(schema.project(projection).unwrap())
        } else {
            schema
        };

        //Ensures a projection is present for looking up members
        let projection: Vec<usize> = if let Some(projection) = projection {
            projection.to_vec()
        } else {
            schema_after_projection
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect()
        };

        Arc::new(GridExec {
            model_table_metadata,
            projection,
            limit,
            schema_after_projection,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

#[async_trait]
impl ExecutionPlan for GridExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema_after_projection.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        //TODO: is grid guaranteed to always output data ordered by tid and time?
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(GridExec {
                model_table_metadata: self.model_table_metadata.clone(),
                projection: self.projection.clone(),
                limit: self.limit,
                schema_after_projection: self.schema_after_projection.clone(),
                input: children[0].clone(),
                metrics: ExecutionPlanMetricsSet::new(),
            }))
        } else {
            Err(DataFusionError::Plan(format!(
                "A single child must be provided {:?}",
                self
            )))
        }
    }

    fn execute(
        &self,
        partition: usize,
        task_context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(GridStream::new(
            self.projection.clone(),
            self.limit,
            self.schema_after_projection.clone(),
            self.input.execute(partition, task_context)?,
            BaselineMetrics::new(&self.metrics, partition),
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        let columns: Vec<&String> = self
            .schema_after_projection
            .fields()
            .iter()
            .map(|f| f.name())
            .collect();

        write!(
            f,
            "GridExec: projection={:?}, limit={:?}, columns={:?}",
            self.projection, self.limit, columns
        )
    }
}

/* Stream */
struct GridStream {
    projection: Vec<usize>,
    _limit: Option<usize>,
    schema_after_projection: SchemaRef,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl GridStream {
    fn new(
        projection: Vec<usize>,
        limit: Option<usize>,
        schema_after_projection: SchemaRef,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        GridStream {
            projection,
            _limit: limit,
            schema_after_projection,
            input,
            baseline_metrics,
        }
    }

    //TODO: it is necessary to return batch_size data points to prevent skew?
    //TODO: limit the batches of data points to only contain what is needed.
    fn grid(&self, batch: &RecordBatch) -> ArrowResult<RecordBatch> {
        // Record the time elapsed from the timer is created to it is dropped.
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        // Retrieve the arrays from batch and cast them to their concrete type.
        crate::get_arrays!(
            batch,
            model_type_id_array,
            timestamps_array,
            start_time_array,
            end_time_array,
            values_array,
            min_value_array,
            max_value_array,
            _error_array
        );

        // Each segments contains at least one data point.
        //TODO: can the specific amount of memory required for the arrays be
        // allocated without explicitly storing the length of the segment?
        let num_rows = batch.num_rows();
        let mut key_builder = UInt64Array::builder(num_rows);
        let mut timestamp_builder = TimestampBuilder::with_capacity(num_rows);
        let mut value_builder = ValueBuilder::with_capacity(num_rows);

        //Reconstructs the data points from the segments
        for row_index in 0..num_rows {
            let model_type_id = model_type_id_array.value(row_index);
            let timestamps = timestamps_array.value(row_index);
            let start_time = start_time_array.value(row_index);
            let end_time = end_time_array.value(row_index);
            let values = values_array.value(row_index);
            let min_value = min_value_array.value(row_index);
            let max_value = max_value_array.value(row_index);

            models::grid(
                2550278706267027457, // TODO: how to make the keys available?
                model_type_id,
                timestamps,
                start_time,
                end_time,
                values,
                min_value,
                max_value,
                &mut key_builder,
                &mut timestamp_builder,
                &mut value_builder,
            );
        }

        // Returns the batch of reconstructed data points with metadata.
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(3);
        for column in &self.projection {
            match column {
                0 => columns.push(Arc::new(key_builder.finish())),
                1 => columns.push(Arc::new(timestamp_builder.finish())),
                2 => columns.push(Arc::new(value_builder.finish())),
                _ => unimplemented!("Tags currently cannot be added."),
            }
        }
        Ok(RecordBatch::try_new(self.schema_after_projection.clone(), columns).unwrap())
    }
}

impl Stream for GridStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut StdTaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.grid(&batch)),
            other => other,
        });
        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for GridStream {
    fn schema(&self) -> SchemaRef {
        self.schema_after_projection.clone()
    }
}
