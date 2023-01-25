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
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as StdTaskContext, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, Float32Array, StringBuilder, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    expressions::PhysicalSortExpr, metrics::BaselineMetrics, metrics::ExecutionPlanMetricsSet,
    metrics::MetricsSet, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::stream::{Stream, StreamExt};

use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::metadata::MetadataManager;
use crate::models;
use crate::types::{TimestampArray, TimestampBuilder, ValueArray, ValueBuilder};

/// An operator that reconstructs the data points stored as segments containing
/// metadata and models. It is public so the additional rules added to Apache
/// Arrow DataFusion's physical optimizer can pattern match on it.
#[derive(Debug, Clone)]
pub struct GridExec {
    /// Metadata required to query the model table.
    model_table_metadata: Arc<ModelTableMetadata>,
    /// Columns requested by the query.
    projection: Vec<usize>,
    /// Number of rows requested by the query.
    limit: Option<usize>,
    /// Schema of the model table after projection.
    schema_after_projection: SchemaRef,
    /// Mapping from tag hash to tags.
    hash_to_tags: HashMap<u64, Vec<String>>,
    /// Operator to read batches of rows from.
    input: Arc<dyn ExecutionPlan>,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    metrics: ExecutionPlanMetricsSet,
}

impl GridExec {
    pub fn new(
        model_table_metadata: Arc<ModelTableMetadata>,
        projection: Vec<usize>,
        limit: Option<usize>,
        schema: SchemaRef,
        hash_to_tags: HashMap<u64, Vec<String>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<Self> {
        // Modifies the schema so it matches the passed projection. unwrap() is safe as the
        // projection is for the schema so errors due to out of bounds indexing cannot happen.
        let schema_after_projection = Arc::new(schema.project(&projection).unwrap());

        Arc::new(GridExec {
            model_table_metadata,
            projection,
            limit,
            schema_after_projection,
            hash_to_tags,
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

    /// Return the schema of the model table after projection.
    fn schema(&self) -> SchemaRef {
        self.schema_after_projection.clone()
    }

    /// Return the single operator batches of rows are read from.
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Return the partitioning of the single operator batches of rows are read
    /// from as [`GridExec`] does not repartition the batches of rows.
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    /// Return `None` to indicate that `GridExec` does not guarantee a specific
    /// ordering of the rows it produces.
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        //TODO: can it be guaranteed that GridExec outputs ordered data?
        None
    }

    /// Return a new instance of [`GridExec`] with the operator to read batches
    /// of rows from replaced. [`DataFusionError::Plan`] is returned if
    /// `children` does not contain a single element.
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
                hash_to_tags: self.hash_to_tags.clone(),
                input: children[0].clone(),
                metrics: self.metrics.clone(),
            }))
        } else {
            Err(DataFusionError::Plan(format!(
                "A single child must be provided {self:?}"
            )))
        }
    }

    /// Create a stream that read batches of rows with segments from the data
    /// source operator, reconstructs the data points from the metadata and
    /// models in the segments, and returns batches of rows with data points.
    fn execute(
        &self,
        partition: usize,
        task_context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(GridStream::new(
            self.projection.clone(),
            self.limit,
            self.schema_after_projection.clone(),
            self.hash_to_tags.clone(), // TODO: share single instance though RC or ARC.
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

/// A stream that read batches of rows with segments from the data source
/// operator, reconstructs the data points from the metadata and models in the
/// segments, and returns batches of rows with data points.
struct GridStream {
    /// Columns requested by the query.
    projection: Vec<usize>,
    /// Number of rows requested by the query.
    _limit: Option<usize>,
    /// Schema of the model table after projection.
    schema_after_projection: SchemaRef,
    /// Mapping from tag hash to tags.
    hash_to_tags: HashMap<u64, Vec<String>>,
    /// Stream to read batches of rows from.
    input: SendableRecordBatchStream,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    baseline_metrics: BaselineMetrics,
}

impl GridStream {
    fn new(
        projection: Vec<usize>,
        limit: Option<usize>,
        schema_after_projection: SchemaRef,
        hash_to_tags: HashMap<u64, Vec<String>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        GridStream {
            projection,
            _limit: limit,
            schema_after_projection,
            hash_to_tags,
            input,
            baseline_metrics,
        }
    }

    // TODO: it is necessary to return batch_size data points to prevent skew?
    // TODO: limit the batches of data points to only contain what is needed.
    /// Reconstruct the data points from the metadata and models in the segments
    /// in `batch`, and return batches of rows with data points.
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
        let mut univariate_ids_builder = UInt64Array::builder(num_rows);
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
                &mut univariate_ids_builder,
                &mut timestamp_builder,
                &mut value_builder,
            );
        }

        // Append the tags to each of the data points if any were requested.
        let mut tag_columns: Vec<StringBuilder> = Vec::with_capacity(self.projection.len());

        // Skip appending tags if none were requested.
        if !self.hash_to_tags.is_empty() {
            // unwrap() is safe as the HashMap is guaranteed to contain at least one value.
            let tag_columns_len = self.hash_to_tags.values().next().unwrap().len();
            for _ in 0..tag_columns_len {
                tag_columns.push(StringBuilder::new());
            }

            for univariate_id in univariate_ids_builder.values_slice() {
                let tag_hash = MetadataManager::univariate_id_to_tag_hash(*univariate_id);
                let tags = &self.hash_to_tags[&tag_hash];
                for index in 0..tag_columns_len {
                    tag_columns[index].append_value(tags[index].clone());
                }
            }
        }

        // Add the none tag columns to the batch.
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.projection.len());
        for column in &self.projection {
            match column {
                0 => columns.push(Arc::new(univariate_ids_builder.finish())),
                1 => columns.push(Arc::new(timestamp_builder.finish())),
                2 => columns.push(Arc::new(value_builder.finish())),
                _ => (), // The remaining columns are for tags.
            }
        }

        // Add the tag columns to the batch.
        tag_columns
            .drain(0..)
            .for_each(|mut tag_column| columns.push(Arc::new(tag_column.finish())));

        // Return the batch, unwrap() is safe as columns are constructed from self.projection.
        RecordBatch::try_new(self.schema_after_projection.clone(), columns).unwrap()
    }
}

impl Stream for GridStream {
    /// Specify that [`GridStream`] returns [`ArrowResult<RecordBatch>`] when
    /// polled.
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
    /// Return the schema of the model table after projection.
    fn schema(&self) -> SchemaRef {
        self.schema_after_projection.clone()
    }
}
