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

//! Implementation of the Apache Arrow DataFusion operator [`SortedJoinExec`] and
//! [`SortedJoinStream`] which joins multiple sorted array produced by
//! [`GridExecs`](crate::query::grid_exec::GridExec) and combines them with the time series tags
//! retrieved from the [`MetadataManager`](crate::metadata::MetadataManager) to create the complete
//! results containing a timestamp column, one or more field columns, and zero or more tag columns.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as StdTaskContext, Poll};

use datafusion::arrow::array::{ArrayRef, StringBuilder, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    expressions::PhysicalSortExpr, DisplayFormatType, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::stream::{Stream, StreamExt};

use crate::metadata::MetadataManager;

/// An execution plan that join arrays of data points sorted by `univaraite_id` and `timestamp` from
/// multiple execution plan and tags. It is public so the additional rules added to Apache Arrow
/// DataFusion's physical optimizer can pattern match on it.
#[derive(Debug)]
pub struct SortedJoinExec {
    /// Schema of the execution plan.
    schema: SchemaRef,
    /// Mapping from tag hash to tags.
    hash_to_tags: Arc<HashMap<u64, Vec<String>>>,
    /// Execution plans to read batches of data points from.
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    metrics: ExecutionPlanMetricsSet,
}

impl SortedJoinExec {
    pub fn new(
        schema: SchemaRef,
        hash_to_tags: Arc<HashMap<u64, Vec<String>>>,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Arc<Self> {
        Arc::new(SortedJoinExec {
            schema,
            hash_to_tags,
            inputs,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl ExecutionPlan for SortedJoinExec {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the schema of the plan.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Return the partitioning of the first execution plan batches of segments are read from as all
    /// of the execution plans compressed segments are read from are equivalent.
    fn output_partitioning(&self) -> Partitioning {
        self.inputs[0].output_partitioning()
    }

    /// Specify that the record batches produced by the execution plan will have an unknown order as
    /// the output from [`SortedJoinExec`] does not include the `univariate_id` but instead tags.
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    /// Return the single execution plan batches of rows are read from.
    fn children(&self) -> Vec<Arc<(dyn ExecutionPlan)>> {
        self.inputs.clone()
    }

    /// Return a new [`SortedJoinExec`] with the execution plan to read batches of reconstructed
    /// data points from replaced. [`DataFusionError::Plan`] is returned if `children` does not
    /// contain at least one element.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<(dyn ExecutionPlan)>>,
    ) -> Result<Arc<(dyn ExecutionPlan)>> {
        if children.len() == 1 {
            Ok(SortedJoinExec::new(
                self.schema.clone(),
                self.hash_to_tags.clone(),
                children,
            ))
        } else {
            Err(DataFusionError::Plan(format!(
                "At least one child must be provided {self:?}",
            )))
        }
    }

    /// Create a stream that reads batches reconstructed data points from the child streams, joins
    /// the arrays of data points with the timestamps and tags, and returns the result.
    fn execute(
        &self,
        partition: usize,
        task_context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let streams = self
            .inputs
            .iter()
            .map(|input| input.execute(partition, task_context.clone()))
            .collect::<Result<Vec<SendableRecordBatchStream>>>()?;

        Ok(Box::pin(SortedJoinStream::new(
            self.schema.clone(),
            self.hash_to_tags.clone(),
            streams,
            BaselineMetrics::new(&self.metrics, partition),
        )))
    }

    /// Specify that [`SortedJoinExec`] knows nothing about the data it will output.
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
        write!(f, "SortedJoinExec")
    }
}

struct SortedJoinStream {
    /// Schema of the stream.
    schema: SchemaRef,
    /// Mapping from tag hash to tags.
    hash_to_tags: Arc<HashMap<u64, Vec<String>>>,
    /// Streams to read batches of data points from.
    inputs: Vec<SendableRecordBatchStream>,
    /// Current batch of data points to join from.
    batches: Vec<Option<RecordBatch>>,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    baseline_metrics: BaselineMetrics,
}

impl SortedJoinStream {
    fn new(
        schema: SchemaRef,
        hash_to_tags: Arc<HashMap<u64, Vec<String>>>,
        inputs: Vec<SendableRecordBatchStream>,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        // Must be created before SortedJoinStream as inputs are moved into it.
        let batches = vec![None; inputs.len()];

        SortedJoinStream {
            schema,
            hash_to_tags,
            inputs,
            batches,
            baseline_metrics,
        }
    }

    fn join(&self) -> Poll<Option<ArrowResult<RecordBatch>>> {
        // TODO: compute and assign arrays to columns dynamically.
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields.len());

        // The first column is retrieved separately so the timestamps and tags can be added.
        // unwrap() is safe as this method is not called until a batch is received from all inputs.
        let record_batch = self.batches[0].as_ref().unwrap();
        columns.push(record_batch.column(1).clone());

        // TODO: reconstruct all requested tag columns and assign the columns using projection.
        let univariate_ids = crate::array!(record_batch, 0, UInt64Array);
        let mut tag_column = StringBuilder::new();
        for univariate_id in univariate_ids.values() {
            let tag_hash = MetadataManager::univariate_id_to_tag_hash(*univariate_id);
            let tags = &self.hash_to_tags[&tag_hash];
            tag_column.append_value(tags[0].clone());
        }
        columns.push(Arc::new(tag_column.finish()));
        columns.push(record_batch.column(2).clone());

        // Add the remaining columns.
        // unwrap() is safe as this method is not called until a batch is received from all inputs.
        for record_batch in &self.batches[1..] {
            columns.push(record_batch.as_ref().unwrap().column(2).clone());
        }

        // Return the batch, unwrap() is safe as SortedJoinStream uses a static schema.
        let record_batch = RecordBatch::try_new(self.schema.clone(), columns).unwrap();
        Poll::Ready(Some(Ok(record_batch)))
    }

    /// Pools all inputs for [`RecordBatches`](RecordBatch) and returns [`true`] if a batch have
    /// been returned from all inputs, otherwise false.
    fn poll_all_pending_inputs(&mut self, cx: &mut StdTaskContext<'_>) -> bool {
        // TODO: return the actual value of poll as it may be None or Pending.
        // Batches are created from inputs in new() so they are always the same length.
        let mut received_a_batch_from_all_inputs = true;
        for index in 0..self.batches.len() {
            if self.batches[index].is_none() {
                // Pending is returned by Stream.poll_next() if any of the inputs return Pending.
                if let Poll::Ready(Some(Ok(batch))) = self.inputs[index].poll_next_unpin(cx) {
                    self.batches[index] = Some(batch);
                } else {
                    received_a_batch_from_all_inputs = false;
                }
            }
        }
        received_a_batch_from_all_inputs
    }
}

impl Stream for SortedJoinStream {
    /// Specify that [`SortedJoinStream`] returns [`ArrowResult<RecordBatch>`] when polled.
    type Item = ArrowResult<RecordBatch>;

    /// Try to poll the next batch of data points from the [`SortedJoinStream`] and returns:
    /// * `Poll::Pending` if the next batch is not yet ready.
    /// * `Poll::Ready(Some(Ok(batch)))` if the next batch is ready.
    /// * `Poll::Ready(None)` if the stream is empty.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut StdTaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.poll_all_pending_inputs(cx) {
            let poll = self.join();
            self.baseline_metrics.record_poll(poll)
        } else {
            Poll::Pending
        }
    }
}

impl RecordBatchStream for SortedJoinStream {
    /// Return the schema of the stream.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
