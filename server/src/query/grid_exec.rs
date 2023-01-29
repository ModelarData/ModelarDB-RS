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

//! Implementation of the Apache Arrow DataFusion operator [`GridExec`] and [`GridStream`] which
//! reconstructs the data points for a specific column from the compressed segments containing
//! metadata and models.

use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as StdTaskContext, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, ArrowPrimitiveType, BinaryArray, Float32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::sorts::sort::SortOptions;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
    Statistics,
};
use futures::stream::{Stream, StreamExt};

use crate::models;
use crate::types::{
    ArrowTimestamp, ArrowUnivariateId, ArrowValue, TimestampArray, TimestampBuilder, ValueArray,
    ValueBuilder,
};

/// An execution that reconstructs the data points stored as segments containing metadata and
/// models. It is public so the additional rules added to Apache Arrow DataFusion's physical
/// optimizer can pattern match on it.
#[derive(Debug, Clone)]
pub struct GridExec {
    /// Schema of the execution plan.
    schema: SchemaRef,
    /// Ordering of the plans output.
    output_ordering: Vec<PhysicalSortExpr>,
    /// Number of data points requested by the query.
    limit: Option<usize>,
    /// Execution plan to read batches of segments from.
    input: Arc<dyn ExecutionPlan>,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    metrics: ExecutionPlanMetricsSet,
}

impl GridExec {
    pub fn new(limit: Option<usize>, input: Arc<dyn ExecutionPlan>) -> Arc<Self> {
        // Schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("univariate_id", ArrowUnivariateId::DATA_TYPE, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("value", ArrowValue::DATA_TYPE, false),
        ]));

        // Output Ordering.
        let sort_options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let output_ordering = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("univariate_id", 0)),
                options: sort_options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("timestamp", 1)),
                options: sort_options,
            },
        ];

        Arc::new(GridExec {
            schema,
            output_ordering,
            limit,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

#[async_trait]
impl ExecutionPlan for GridExec {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the schema of the plan.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Return the partitioning of the single execution plan batches of segments are read from.
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    /// Specify that the record batches produced by the execution plan will be ordered by
    /// descendingly by univariate_id and then descendingly timestamp.
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        Some(&self.output_ordering)
    }

    /// Return the single execution plan batches of rows are read from.
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Return a new [`GridExec`] with the execution plan to read batches of rows from replaced.
    /// [`DataFusionError::Plan`] is returned if `children` does not contain a single element.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(GridExec {
                schema: self.schema.clone(),
                output_ordering: self.output_ordering.clone(),
                limit: self.limit,
                input: children[0].clone(),
                metrics: self.metrics.clone(),
            }))
        } else {
            Err(DataFusionError::Plan(format!(
                "A single child must be provided {self:?}"
            )))
        }
    }

    /// Create a stream that reads batches of rows with segments from the child stream, reconstructs
    /// the data points from the metadata and models in the segments, and returns batches of rows
    /// with data points.
    fn execute(
        &self,
        partition: usize,
        task_context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(GridStream::new(
            self.schema.clone(),
            self.limit,
            self.input.execute(partition, task_context)?,
            BaselineMetrics::new(&self.metrics, partition),
        )))
    }

    /// Specify that [`GridExec`] knows nothing about the data it will output.
    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        }
    }

    /// Return a snapshot of the set of metrics being collected by the operator.
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// Write a string-based representation of the operator to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "GridExec: limit={:?}", self.limit)
    }
}

/// A stream that read batches of rows with segments from the input stream, reconstructs the data
/// points from the metadata and models in the segments, and returns batches of data points.
struct GridStream {
    /// Schema of the stream.
    schema: SchemaRef,
    /// Number of rows requested by the query.
    _limit: Option<usize>,
    /// Stream to read batches of rows from.
    input: SendableRecordBatchStream,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    baseline_metrics: BaselineMetrics,
}

impl GridStream {
    fn new(
        schema: SchemaRef,
        limit: Option<usize>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        GridStream {
            schema,
            _limit: limit,
            input,
            baseline_metrics,
        }
    }

    // TODO: it is necessary to return batch_size data points to prevent skew?
    // TODO: limit the batches of data points to only contain what is needed.
    /// Reconstruct the data points from the metadata and models in the segments in `batch`, and
    /// return batches of data points.
    fn grid(&self, batch: &RecordBatch) -> RecordBatch {
        // Record the time elapsed from the timer is created to it is dropped.
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        // Retrieve the arrays from batch and cast them to their concrete type.
        crate::arrays!(
            batch,
            univariate_ids,
            model_type_ids,
            start_times,
            end_times,
            timestamps,
            min_values,
            max_values,
            values,
            _error_array
        );

        // Each segment is guaranteed to contain at least one data point.
        let num_rows = batch.num_rows();
        let mut univariate_id_builder = UInt64Array::builder(num_rows);
        let mut timestamp_builder = TimestampBuilder::with_capacity(num_rows);
        let mut value_builder = ValueBuilder::with_capacity(num_rows);

        // Reconstructs the data points from the segments.
        for row_index in 0..num_rows {
            // unwrap() is safe as the storage engine created the strings.
            let univariate_id = univariate_ids.value(row_index);
            let model_type_id = model_type_ids.value(row_index);
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let timestamps = timestamps.value(row_index);
            let min_value = min_values.value(row_index);
            let max_value = max_values.value(row_index);
            let values = values.value(row_index);

            models::grid(
                univariate_id,
                model_type_id,
                timestamps,
                start_time,
                end_time,
                values,
                min_value,
                max_value,
                &mut univariate_id_builder,
                &mut timestamp_builder,
                &mut value_builder,
            );
        }

        let column: Vec<ArrayRef> = vec![
            Arc::new(univariate_id_builder.finish()),
            Arc::new(timestamp_builder.finish()),
            Arc::new(value_builder.finish()),
        ];

        // Return the batch, unwrap() is safe as GridStream uses a static schema.
        RecordBatch::try_new(self.schema.clone(), column).unwrap()
    }
}

impl Stream for GridStream {
    /// Specify that [`GridStream`] returns [`ArrowResult<RecordBatch>`] when polled.
    type Item = ArrowResult<RecordBatch>;

    /// Try to poll the next element from the [`GridStream`] and returns:
    /// * `Poll::Pending` if the next element is not yet ready.
    /// * `Poll::Ready(Some(Ok(batch)))` if an element is ready.
    /// * `Poll::Ready(None)` if the stream is empty.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut StdTaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(Ok(self.grid(&batch))),
            other => other,
        });
        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for GridStream {
    /// Return the schema of the stream.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
