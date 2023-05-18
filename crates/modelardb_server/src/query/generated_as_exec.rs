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

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::temporal_conversions;
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
use modelardb_common::types::{TimestampArray, ValueArray};

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
/// pattern match on it. Assumes any columns that are only used to generate other columns are last.
#[derive(Debug)]
pub struct GeneratedAsExec {
    /// Schema of the execution plan.
    schema: SchemaRef,
    /// Columns to generate and the index they should be at.
    columns_to_generate: Vec<ColumnToGenerate>,
    /// Execution plan to read batches of segments from.
    input: Arc<dyn ExecutionPlan>,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    metrics: ExecutionPlanMetricsSet,
}

impl GeneratedAsExec {
    pub fn new(
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
    /// [`DataFusionError::Plan`] is returned if `children` does not contain exactly one element.
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

/// A stream that reads batches of rows from the input stream, generates columns from the input,
/// adds them to the batch, and then returns the result.
struct GeneratedAsStream {
    /// Schema of the stream.
    schema: SchemaRef,
    /// Columns to generate and the index they should be at.
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

    /// Format and return the first row in `batch` that causes `physical_expr` to fail, if
    /// `physical_expr` never fails or if `batch` is from a normal table, [`None`] is returned.
    fn failing_row(batch: &RecordBatch, physical_expr: &Arc<dyn PhysicalExpr>) -> Option<String> {
        for row_index in 0..batch.num_rows() {
            if physical_expr.evaluate(&batch.slice(row_index, 1)).is_err() {
                let schema = batch.schema();
                let mut formatted_values = Vec::with_capacity(batch.num_columns());

                for column_index in 0..batch.num_columns() {
                    let name = schema.field(column_index).name();
                    let column = batch.column(column_index);

                    if let Some(timestamps) = column.as_any().downcast_ref::<TimestampArray>() {
                        // Store a readable version of timestamp if it is in the time interval that
                        // can be represented by a NaiveDateTime, otherwise the integer is stored.
                        let timestamp = timestamps.value(row_index);
                        let formated_value = if let Some(naive_date_time) =
                            temporal_conversions::timestamp_ms_to_datetime(timestamp)
                        {
                            format!("{name}: {}", naive_date_time)
                        } else {
                            format!("{name}: {}", timestamp)
                        };
                        formatted_values.push(formated_value);
                    } else if let Some(fields) = column.as_any().downcast_ref::<ValueArray>() {
                        formatted_values.push(format!("{name}: {}", fields.value(row_index)));
                    } else if let Some(tags) = column.as_any().downcast_ref::<StringArray>() {
                        formatted_values.push(format!("{name}: {}", tags.value(row_index)));
                    } else {
                        // The method has been called for a table with unsupported column types.
                        return None;
                    }
                }

                return Some(formatted_values.join(", "));
            }
        }

        // physical_expr never failed for any of the rows in batch.
        None
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
                let schema_fields_len = self.schema.fields().len();
                let mut columns = Vec::with_capacity(schema_fields_len);
                let mut generated_columns = 0;

                for column_to_generate in &self.columns_to_generate {
                    // Add the stored columns until the next generated column.
                    for index in columns.len()..column_to_generate.index {
                        columns.push(batch.column(index - generated_columns).clone());
                    }

                    // Compute the values of the next generated column and, if successful, add it.
                    let maybe_generated_column = column_to_generate.physical_expr.evaluate(&batch);
                    if let Ok(generated_column) = maybe_generated_column {
                        columns.push(generated_column.into_array(batch.num_rows()));
                        generated_columns += 1;
                    } else {
                        let column_name = self.schema.field(column_to_generate.index).name();

                        // unwrap() is safe as it is only executed if a column was not generated.
                        let physical_expr = &column_to_generate.physical_expr;
                        let failing_row = Self::failing_row(&batch, physical_expr).unwrap();
                        let cause = maybe_generated_column.err().unwrap();

                        let error = format!(
                            "Failed to create '{column_name}' for {{{failing_row}}} due to: {cause}"
                        );
                        return Poll::Ready(Some(Err(DataFusionError::Execution(error))));
                    };
                }

                // Add the remaining stored columns to the record batch.
                for index in columns.len()..schema_fields_len {
                    columns.push(batch.column(index - generated_columns).clone());
                }

                // Drop columns that were required to generate values but were not in the query.
                columns.truncate(schema_fields_len);

                // unwrap() is safe as GeneratedAsStream ordered the columns to match the schema.
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
