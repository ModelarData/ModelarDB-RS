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
use std::cmp::min;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;

use futures::stream::{Stream, StreamExt};

use datafusion::arrow::array::{
    ArrayRef, BinaryArray, Float32Array, Int32Array, Int64Array, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::datasource::{
    datasource::Statistics, datasource::TableProviderFilterPushDown, TableProvider,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::{binary_expr, col, combine_filters, Expr};
use datafusion::physical_plan::{
    parquet::ParquetExec, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};

use crate::catalog::ModelTable;
use crate::models;

/** Public Types **/
pub struct DataPointView {
    max_concurrency: usize,
    model_table: Arc<ModelTable>,
}

impl DataPointView {
    pub fn new(max_concurrency: usize, model_table: &Arc<ModelTable>) -> Arc<Self> {
        Arc::new(DataPointView {
            max_concurrency,
            model_table: model_table.clone(),
        })
    }
}

/** Private Methods **/
impl TableProvider for DataPointView {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tid", DataType::Int32, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float32, false),
        ]))
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        //TODO: Centralize rewriting of predicates
        //Rewrite logical_plan predicates
        let rewritten_filters: Vec<Expr> = filters
            .iter()
            .map(|filter| match filter {
                Expr::BinaryExpr { left, op, right } => {
                    if **left == col("tid") {
                        binary_expr(col("gid"), *op, *right.clone())
                    } else {
                        filter.clone()
                    }
                }
                _ => filter.clone(),
            })
            .collect();

        //Create the data source node
        let predicate = combine_filters(&rewritten_filters);
        let parquet_exec = ParquetExec::try_from_path(
            &self.model_table.segment_folder,
            Some(vec![0, 1, 2, 3, 4, 5]),
            predicate.clone(),
            batch_size,
            self.max_concurrency,
            limit,
        )
        .unwrap();

        //Create the grid node
        let limited_batch_size = limit.map(|l| min(l, batch_size)).unwrap_or(batch_size);
        Ok(GridExec::new(
            self.model_table.clone(),
            projection.clone(),
            predicate,
            limited_batch_size,
            self.schema(),
            Arc::new(parquet_exec),
        ))
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        }
    }
}

/* ExecutionPlan */
//GridExec is public so the physical optimizer rules can pattern match on it, and
//model_table is public so the rules can reuse it as context is not in their scope.
#[derive(Debug, Clone)]
pub struct GridExec {
    pub model_table: Arc<ModelTable>,
    predicate: Option<Expr>,
    limited_batch_size: usize,
    schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
}

impl GridExec {
    pub fn new(
        model_table: Arc<ModelTable>,
        projection: Option<Vec<usize>>,
        predicate: Option<Expr>,
        limited_batch_size: usize,
        schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<Self> {
        let schema = if let Some(projection) = projection {
            //Modifies the schema according to the projection
            let original_schema_fields = schema.fields().clone();
            let mut projected_schema_fields = Vec::with_capacity(projection.len());
            for column in projection {
                projected_schema_fields.push(original_schema_fields.get(column).unwrap().clone())
            }
            Arc::new(Schema::new(projected_schema_fields))
        } else {
            schema
        };

        Arc::new(GridExec {
            model_table,
            predicate,
            limited_batch_size,
            schema,
            input,
        })
    }
}

#[async_trait]
impl ExecutionPlan for GridExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(GridExec {
                input: children[0].clone(),
                ..self.clone()
            }))
        } else {
            Err(DataFusionError::Internal(format!(
                "A single child must be provided {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(GridStream::new(
            self.schema.clone(),
            self.model_table.clone(),
            self.input.execute(partition).await?,
        )))
    }
}

/* Stream */
struct GridStream {
    schema: SchemaRef,
    model_table: Arc<ModelTable>,
    input: SendableRecordBatchStream,
}

impl GridStream {
    fn new(
        schema: SchemaRef,
        model_table: Arc<ModelTable>,
        input: SendableRecordBatchStream,
    ) -> Self {
        GridStream {
            schema,
            model_table,
            input,
        }
    }

    fn grid(&self, batch: &RecordBatch) -> ArrowResult<RecordBatch> {
        //TODO: how to efficiently construct and return only the requested columns?
        //TODO: it is necessary to only return batch_limit data points to prevent skew?
        //TODO: should start_time and end_time be unsigned integers instead?
        let gids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let start_times = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let end_times = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mtids = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let models = batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let gaps = batch
            .column(5)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        //Compute the number of data points to allocate space for
        let num_rows = batch.num_rows();
        let data_points = models::length(
            num_rows,
            gids,
            start_times,
            end_times,
            &self.model_table.sampling_intervals,
        );
        let mut tids = Int32Array::builder(data_points);
        let mut timestamps = TimestampMillisecondArray::builder(data_points);
        let mut values = Float32Array::builder(data_points);

        //Reconstruct the data points using the models
        for row_index in 0..num_rows {
            let gid = gids.value(row_index);
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let mtid = mtids.value(row_index);
            let sampling_interval = *self
                .model_table
                .sampling_intervals
                .get(gid as usize)
                .unwrap();
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

        //Return the batch of reconstructed data points
        let columns: Vec<ArrayRef> = self.schema.fields().iter().map(|field| {
            let column: ArrayRef = match field.name().as_str() {
                "tid" => Arc::new(tids.finish()),
                "timestamp" =>  Arc::new(timestamps.finish()),
                "value" => Arc::new(values.finish()),
                column => panic!("unsupported column {}", column)
            };
            column
        }).collect();
        Ok(RecordBatch::try_new(self.schema.clone(), columns).unwrap())
    }
}

impl Stream for GridStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.grid(&batch)),
            other => other,
        })
    }
}

impl RecordBatchStream for GridStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
