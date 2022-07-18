/* Copyright 2021 The MiniModelarDB Contributors
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
use std::task::{Context, Poll};

use async_trait::async_trait;

use tracing::{debug, error, info, warn, Level, event, instrument, span};

use futures::stream::{Stream, StreamExt};

use datafusion::arrow::array::{
    ArrayRef, BinaryArray, Float32Array, Int32Array, Int64Array, StringArray, StringBuilder,
    TimestampMillisecondArray,
};
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
    metrics::MetricsSet, DisplayFormatType, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue::{Int64, TimestampNanosecond};

use datafusion_physical_expr::planner;

use crate::catalog::ModelTableMetadata;
use crate::models;

/** Public Types **/
/* TableProvider */
pub struct ModelTable {
    object_store_url: ObjectStoreUrl,
    model_table_metadata: Arc<ModelTableMetadata>,
    schema: Arc<Schema>,
}

/** Public Methods **/
impl ModelTable {
    pub fn new(model_table_metadata: &Arc<ModelTableMetadata>) -> Arc<Self> {
        let mut columns = vec![
            Field::new("tid", DataType::Int32, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float32, false),
        ];

        //TODO: support dimensions with levels that are not strings?
        for level in &model_table_metadata.denormalized_dimensions {
            let level = level.as_any().downcast_ref::<StringArray>().unwrap();
            columns.push(Field::new(level.value(0), DataType::Utf8, false));
        }

        Arc::new(ModelTable {
            model_table_metadata: model_table_metadata.clone(),
            object_store_url: ObjectStoreUrl::local_filesystem(),
            schema: Arc::new(Schema::new(columns)),
        })
    }

    fn rewrite_and_combine_filters(&self, filters: &[Expr]) -> Option<Expr> {
        //TODO: implement rewriting of members to group ids
        let rewritten_filters: Vec<Expr> = filters
            .iter()
            .map(|filter| match filter {
                Expr::BinaryExpr { left, op, right } => {
                    if **left == col("tid") {
                        //Assumes time series are not grouped so tids and gids are equivalent
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
        //Assumes the expression is a literal with a timestamp at nanosecond resolution
        //TODO: add proper error handling if expr can be anything but TimestampNanosecond
        let nanoseconds_to_millisecond = 1_000_000;
        if let Expr::Literal(value) = expr {
            if let TimestampNanosecond(value, _timezone) = value {
                //TODO: ensure timezone is handled correctly as part of the conversion to ms
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

        let df_schema = self
            .model_table_metadata
            .segment_group_file_schema
            .clone()
            .to_dfschema()?;

        let physical_predicate = planner::create_physical_expr(
            predicate,
            &df_schema,
            &self.model_table_metadata.segment_group_file_schema,
            &ExecutionProps::new(),
        )?;

        Ok(Arc::new(FilterExec::try_new(
            physical_predicate,
            input.clone(),
        )?))
    }
}

/** Private Methods **/
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
        //Create the data source node
        let partitioned_files: Vec<PartitionedFile> = ctx //Assumes ObjectStore exists
            .runtime_env
            .object_store(&self.object_store_url)
            .unwrap()
            .list_file(&self.model_table_metadata.segment_folder)
            .await?
            .map(|file_meta| PartitionedFile {
                file_meta: file_meta.unwrap(),
                partition_values: vec![],
                range: None,
            })
            .collect::<Vec<PartitionedFile>>()
            .await;

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
            file_schema: self.model_table_metadata.segment_group_file_schema.clone(),
            file_groups: vec![partitioned_files],
            statistics,
            projection: None,
            limit,
            table_partition_cols: vec![],
        };

        let predicate = self.rewrite_and_combine_filters(filters);
        let parquet_exec = Arc::new(ParquetExec::new(file_scan_config, predicate.clone()));
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

    fn grid(&self, batch: &RecordBatch) -> ArrowResult<RecordBatch> {
        //The timer records the time elapsed when dropped
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        //TODO: it is necessary to only return batch_size data points to prevent skew?
        //TODO: can start_time and end_time be converted to timestamps without adding overhead?
        //TODO: can the signed ints from Java be cast to unsigned ints without adding overhead?
        crate::downcast_arrays!(gids, start_times, end_times, mtids, models, gaps, batch);

        //Compute the number of data points that will be reconstructed from the models and allocate
        //memory for the them. It is assumed that most queries will request tids, timestamps, and
        //values so they are written to the arrays together by a single iteration over the models.
        //A segment represents at least one data points, so only limit segments are needed if set.
        //TODO: reduce limit for each batch of data points returned to not output more than needed
        let num_rows = batch
            .num_rows()
            .min(self.limit.unwrap_or(usize::max_value()));
        let data_points = models::count(
            num_rows,
            gids,
            start_times,
            end_times,
            &self.model_table_metadata.sampling_intervals,
        );
        let mut tids = Int32Array::builder(data_points);
        let mut timestamps = TimestampMillisecondArray::builder(data_points);
        let mut values = Float32Array::builder(data_points);

        //Reconstructs the data points from the segments
        for row_index in 0..num_rows {
            let gid = gids.value(row_index);
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let mtid = mtids.value(row_index);
            let sampling_interval = self.model_table_metadata.sampling_intervals.value(gid as usize);
            let model = models.value(row_index);
            let gaps = gaps.value(row_index);
            models::grid(
                gid,
                start_time,
                end_time,
                mtid,
                sampling_interval,
                model,
                gaps,
                &mut tids,
                &mut timestamps,
                &mut values,
            );
        }

        //Joins the reconstructed data points with the members
        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(self.schema_after_projection.fields().len());
        let tids = Arc::new(tids.finish()); //Finished before the loop so it can be used by add_dimension_column

        //Returns the batch of reconstructed data points with metadata
        for column in &self.projection {
            match column {
                0 => columns.push(tids.clone()),
                1 => columns.push(Arc::new(timestamps.finish())),
                2 => columns.push(Arc::new(values.finish())),
                column => columns.push(self.add_dimension_column(&tids, *column)),
            }
        }
        Ok(RecordBatch::try_new(self.schema_after_projection.clone(), columns).unwrap())
    }

    fn add_dimension_column(&self, tids: &Int32Array, column: usize) -> ArrayRef {
        //TODO: support dimensions with levels that are not strings?
        let level = self
            .model_table_metadata
            .denormalized_dimensions
            .get(column - 3)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let mut members = StringBuilder::new(level.iter().map(|s| s.unwrap().len()).sum());
        for tid in tids {
            members
                .append_value(level.value(tid.unwrap() as usize))
                .unwrap();
        }
        Arc::new(members.finish())
    }
}

impl Stream for GridStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
