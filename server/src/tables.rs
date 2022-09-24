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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as StdTaskContext, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BinaryArray, Float32Array, UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
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
use datafusion::scalar::ScalarValue::{Int64, TimestampNanosecond};
use datafusion_physical_expr::planner;
use futures::stream::{Stream, StreamExt};

use crate::catalog::ModelTableMetadata;
use crate::storage::StorageEngine;
use crate::types::{TimestampArray, TimestampBuilder, ValueArray, ValueBuilder};
use crate::Context;

pub struct ModelTable {
    context: Arc<Context>,
    object_store_url: ObjectStoreUrl,
    model_table_metadata: Arc<ModelTableMetadata>,
    schema: Arc<Schema>,
}

impl ModelTable {
    pub fn new(
        context: Arc<Context>,
        model_table_metadata: &Arc<ModelTableMetadata>,
    ) -> Arc<Self> {
        // TODO: support reconstructing the ingested multivariate time series.
        let mut columns = vec![
            Field::new("tid", DataType::UInt64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float32, false),
        ];

        Arc::new(ModelTable {
            context,
            model_table_metadata: model_table_metadata.clone(),
            object_store_url: ObjectStoreUrl::local_filesystem(),
            schema: Arc::new(Schema::new(columns)),
        })
    }

    fn rewrite_and_combine_filters(&self, filters: &[Expr]) -> Option<Expr> {
        // TODO: rewrite remaining filters for the remaining columns.
        let rewritten_filters: Vec<Expr> = filters
            .iter()
            .map(|filter| match filter {
                Expr::BinaryExpr { left, op, right } => {
                    if **left == col("tid") {
                        // Assumes time series are not grouped so tids and gids are equivalent.
                        self.binary_expr(col("gid"), *op, *right.clone())
                    } else if **left == col("timestamp") {
                        match op {
                            Operator::Gt => {
                                self.binary_expr(col("end_time"), *op, self.to_i64(right))
                            }
                            Operator::GtEq => {
                                self.binary_expr(col("end_time"), *op, self.to_i64(right))
                            }
                            Operator::Lt => {
                                self.binary_expr(col("start_time"), *op, self.to_i64(right))
                            }
                            Operator::LtEq => {
                                self.binary_expr(col("start_time"), *op, self.to_i64(right))
                            }
                            Operator::Eq => self.binary_expr(
                                self.binary_expr(
                                    col("start_time"),
                                    Operator::LtEq,
                                    self.to_i64(right),
                                ),
                                Operator::And,
                                self.binary_expr(
                                    col("end_time"),
                                    Operator::GtEq,
                                    self.to_i64(right),
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
        combine_filters(&rewritten_filters)
    }

    fn binary_expr(&self, left: Expr, op: Operator, right: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn to_i64(&self, expr: &Expr) -> Expr {
        // Assumes the expression is a literal with a timestamp at nanosecond resolution.
        // TODO: add proper error handling if expr can be anything but TimestampNanosecond.
        let nanoseconds_to_millisecond = 1_000_000;
        if let Expr::Literal(value) = expr {
            if let TimestampNanosecond(value, _timezone) = value {
                // TODO: ensure timezone is handled correctly as part of the conversion to ms.
                Expr::Literal(Int64(Some(value.unwrap() / nanoseconds_to_millisecond)))
            } else {
                panic!("Expr::Literal(value) is not a TimestampNanosecond");
            }
        } else {
            panic!("the expression is not an Expr::Literal");
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
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: Determine which hashes to retrieve.
        let hashes = vec![];

        // Request the matching files from the storage engine.
        let mut object_metas = {
            // unwrap() is safe as read() only fails if the RwLock is poisoned.
            let mut storage_engines = self.context.storage_engine.write().unwrap();

            // unwrap() is safe to use as get_compressed_files() only fails if a
            // non-existing hash is passed or if end time is before start time.
            // TODO: change the storage engine to allow multiple readers.
            storage_engines
                .get_compressed_files(&hashes, None, None)
                .unwrap()
        };

        //Create the data source node. Assumes the ObjectStore already exists.
        let partitioned_files: Vec<PartitionedFile> = object_metas.drain(0..)
            .map(|object_meta| PartitionedFile {
                object_meta,
                partition_values: vec![],
                range: None,
                extensions: None,
            })
            .collect::<Vec<PartitionedFile>>();

        //TODO: accumulate the size with filters
        let statistics = Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        };

        //TODO: partition and limit the number of rows read from the files properly
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
            self.model_table_metadata.clone(),
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
    model_table_metadata: Arc<ModelTableMetadata>,
    projection: Vec<usize>,
    limit: Option<usize>,
    schema_after_projection: SchemaRef,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl GridStream {
    fn new(
        model_table_metadata: Arc<ModelTableMetadata>,
        projection: Vec<usize>,
        limit: Option<usize>,
        schema_after_projection: SchemaRef,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        GridStream {
            model_table_metadata,
            projection,
            limit,
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
            error_array
        );

        // Each segments contains at least one data point.
        //TODO: can the specific amount of memory required for the arrays be
        // allocated without explicitly storing the length of the segment?
        let num_rows = batch.num_rows();
        let mut tids = UInt64Array::builder(num_rows);
        let mut timestamps = TimestampBuilder::with_capacity(num_rows);
        let mut values = ValueBuilder::with_capacity(num_rows);

        //Reconstructs the data points from the segments
        for row_index in 0..num_rows {
            let model_type_id = model_type_id_array.value(row_index);
            let timestamps = timestamps_array.value(row_index);
            let start_time = start_time_array.value(row_index);
            let end_time = end_time_array.value(row_index);
            let values = values_array.value(row_index);
            let min_value = min_value_array.value(row_index);
            let max_value = max_value_array.value(row_index);
            let error = error_array.value(row_index);

            // TODO:
        }

        // Returns the batch of reconstructed data points.
        let columns: Vec<ArrayRef> = vec![
            Arc::new(tids.finish()),
            Arc::new(timestamps.finish()),
            Arc::new(values.finish()),
        ];
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
