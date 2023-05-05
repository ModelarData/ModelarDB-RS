/* Copyright 2023 The ModelarDB Contributors
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

//! Implementation of the Apache Arrow DataFusion execution plan [`GeneratedAsExec`] and its
//! corresponding stream [`GeneratedAsStream`] which computes generated columns and adds them to the
//! result. Generated columns can be computed from other columns and constant values.

use std::any::Any;
use std::fmt::{self, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as StdTaskContext, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::stream::Stream;
use futures::StreamExt;

///  A column the [`GeneratedAsExec`] must add to each of the [`RecordBatches`](RecordBatch) using
/// [`GeneratedAsStream`] with the location it must be at and the [`PhysicalExpr`] that compute it.
#[derive(Debug, Clone)]
pub struct ColumnToGenerate {
    index: usize,
    physical_expr: Arc<dyn PhysicalExpr>,
}

impl ColumnToGenerate {
    pub fn new(index: usize, physical_expr: Arc<dyn PhysicalExpr>) -> Self {
        ColumnToGenerate {
            index,
            physical_expr,
        }
    }
}

/// An execution plan that generates one or more new columns from a [`RecordBatch`] using
/// [`PhysicalExprs`](PhysicalExpr) and creates a new [`RecordBatch`] with the new columns included.
/// It is public so the additional rules added to Apache Arrow DataFusion's physical optimizer can
/// pattern match on it.
#[derive(Debug)]
pub struct GeneratedAsExec {
    /// Schema of the execution plan.
    schema: SchemaRef,
    /// Columns to generation and the index they should be at.
    columns_to_generate: Vec<ColumnToGenerate>,
    /// Execution plan to read batches of segments from.
    input: Arc<dyn ExecutionPlan>,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    metrics: ExecutionPlanMetricsSet,
}

impl GeneratedAsExec {
    fn new(
        schema: SchemaRef,
        columns_to_generate: Vec<ColumnToGenerate>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<Self> {
        Arc::new(GeneratedAsExec {
            schema,
            columns_to_generate,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl ExecutionPlan for GeneratedAsExec {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the schema of the plan.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Return the partitioning for this execution plan. This is always the same as the input
    /// execution plan as it is never changed by [`GeneratedAsExec`].
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    /// Return the output ordering for this execution plan. This is always the same as the input
    /// execution plan as it is never changed by [`GeneratedAsExec`].
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    /// Return the single execution plan batches of rows are read from.
    fn children(&self) -> Vec<Arc<(dyn ExecutionPlan)>> {
        vec![self.input.clone()]
    }

    /// Return a new [`GeneratedAsExec`] with the execution plan to read rows from replaced.
    /// [`DataFusionError::Plan`] is returned if `children` does not contain exactly t least one element.
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<(dyn ExecutionPlan)>>,
    ) -> Result<Arc<(dyn ExecutionPlan)>> {
        if children.len() == 1 {
            Ok(GeneratedAsExec::new(
                self.schema.clone(),
                self.columns_to_generate.clone(),
                children.swap_remove(0),
            ))
        } else {
            Err(DataFusionError::Plan(format!(
                "Exactly one child must be provided {self:?}.",
            )))
        }
    }

    /// Create a stream that reads batches of rows from the child stream, adds generated columns,
    /// and returns the result.
    fn execute(
        &self,
        partition: usize,
        task_context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(GeneratedAsStream::new(
            self.schema.clone(),
            self.columns_to_generate.clone(),
            self.input.execute(partition, task_context)?,
            BaselineMetrics::new(&self.metrics, partition),
        )))
    }

    /// Specify that [`GeneratedAsExec`] knows nothing about the data it will output.
    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        }
    }

    /// Return a snapshot of the set of metrics being collected by the execution plain.
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// Write a string-based representation of the operator to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "GeneratedAsExec")
    }
}

/// A stream that read batches of rows from the input stream, generates columns from the input, adds
/// them to the batch, and then returns the result.
struct GeneratedAsStream {
    /// Schema of the stream.
    schema: SchemaRef,
    /// Columns to generation and the index they should be at.
    columns_to_generate: Vec<ColumnToGenerate>,
    /// Stream to read batches of rows from.
    input: SendableRecordBatchStream,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    baseline_metrics: BaselineMetrics,
}

impl GeneratedAsStream {
    fn new(
        schema: SchemaRef,
        columns_to_generate: Vec<ColumnToGenerate>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        GeneratedAsStream {
            schema,
            columns_to_generate,
            input,
            baseline_metrics,
        }
    }
}

impl Stream for GeneratedAsStream {
    /// Specify that [`GeneratedAsStream`] returns [`Result<RecordBatch>`] when polled.
    type Item = Result<RecordBatch>;

    /// Try to poll the next batch of data points from the [`GeneratedAsStream`] and returns:
    /// * `Poll::Pending` if the next batch is not yet ready.
    /// * `Poll::Ready(Some(Ok(batch)))` if the next batch is ready.
    /// * `Poll::Ready(None)` if the stream is empty.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut StdTaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let mut columns = Vec::with_capacity(self.schema.fields().len());

                for column_to_generate in &self.columns_to_generate {
                    // Add stored columns until the next generated columns.
                    for index in columns.len()..=column_to_generate.index {
                        columns.push(batch.column(index).clone());
                    }

                    // Compute the values of the next generated column and add it.
                    columns.push(
                        column_to_generate
                            .physical_expr
                            .evaluate(&batch)
                            .unwrap()
                            .into_array(batch.num_rows()),
                    );
                }

                // Add the remaining stored columns to the record batch.
                for index in columns.len()..self.schema.fields().len() {
                    columns.push(batch.column(index).clone());
                }

                // unwrap() is safe as GeneratedAsStream has ordered columns to match the schema.
                let batch = RecordBatch::try_new(self.schema.clone(), columns).unwrap();
                self.baseline_metrics
                    .record_poll(Poll::Ready(Some(Ok(batch))))
            }
            other => self.baseline_metrics.record_poll(other),
        }
    }
}

impl RecordBatchStream for GeneratedAsStream {
    /// Return the schema of the stream.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
