use std::any::Any;
use std::sync::Arc;
use std::pin::Pin;
use std::cmp::min;
use std::task::{Context, Poll};

use async_trait::async_trait;

use futures::stream::{Stream, StreamExt};

use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array,
			       TimestampMillisecondArray, Float32Array, BinaryArray};
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use datafusion::datasource::{TableProvider, datasource::Statistics,
			     datasource::TableProviderFilterPushDown};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::{combine_filters, col, binary_expr, Expr};
use datafusion::physical_plan::{ExecutionPlan, Partitioning,
				parquet::ParquetExec, RecordBatchStream,
				SendableRecordBatchStream};

use crate::models;
use crate::catalog::ModelTable;

/** Public Types **/
pub struct DataPointView {
    max_concurrency: usize,
    model_table: Arc<ModelTable>
}

impl DataPointView {
    pub fn new(max_concurrency: usize,
	       model_table: &Arc<ModelTable>) -> Arc<Self> {
	Arc::new(DataPointView {
	    max_concurrency,
	    model_table: model_table.clone()
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
	    Field::new("value", DataType::Float32, false)
	]))
    }

    fn supports_filter_pushdown(&self, _filter: &Expr)
				-> Result<TableProviderFilterPushDown> {
	Ok(TableProviderFilterPushDown::Inexact)
    }

    fn scan(&self,
	    projection: &Option<Vec<usize>>,
	    batch_size: usize,
	    filters: &[Expr],
	    limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {

	//TODO: Centralize rewriting of predicates
	//Rewrite logical_plan predicates
	let rewritten_filters: Vec<Expr> = filters.iter()
	    .map(|filter| match filter {
		Expr::BinaryExpr { left, op, right } => {
		    if **left == col("tid") {
			binary_expr(col("gid"), *op, *right.clone())
		    } else {
			filter.clone()
		    }
		},
		_ => filter.clone()
	    }).collect();

	//Create the data source node
	let predicate = combine_filters(&rewritten_filters);
	let parquet_exec = ParquetExec::try_from_path(
	    &self.model_table.segment_folder,
	    Some(vec!(0, 1, 2, 3, 4, 5)),
	    predicate.clone(),
	    batch_size,
	    self.max_concurrency,
	    limit).unwrap();


	//Create the grid node
	Ok(Arc::new(GridExec {
	    model_table: self.model_table.clone(),
	    projection: projection.clone(),
	    predicate,
	    batch_size: limit.map(|l| min(l, batch_size)).unwrap_or(batch_size),
	    limit,
	    schema: self.schema(),
	    input: Arc::new(parquet_exec)

	}))
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
#[derive(Debug, Clone)]
struct GridExec {
    model_table: Arc<ModelTable>,
    projection: Option<Vec<usize>>,
    predicate: Option<Expr>,
    batch_size: usize,
    limit: Option<usize>,
    schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>
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
	vec!(self.input.clone())
    }

    fn output_partitioning(&self) -> Partitioning {
	self.input.output_partitioning()
    }

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>)
			 -> Result<Arc<dyn ExecutionPlan>> {
	if children.len() == 1 {
	    Ok(Arc::new(GridExec { input: children[0].clone(), ..self.clone() }))
	} else {
	    Err(DataFusionError::Internal(format!(
		"A single child must be provided {:?}", self)))
	}
    }

    async fn execute(&self, partition: usize)
		     -> Result<SendableRecordBatchStream> {
	Ok(Box::pin(GridStream::new(self.schema.clone(),
				    self.model_table.clone(),
				    self.input.execute(partition).await?)))
    }
}

/* Stream */
struct GridStream {
    schema: SchemaRef,
    model_table: Arc<ModelTable>,
    input: SendableRecordBatchStream,
}

impl GridStream {
    fn new(schema: SchemaRef, model_table: Arc<ModelTable>,
	   input: SendableRecordBatchStream) -> Self {
	GridStream { schema, model_table, input }
    }

    fn grid(self: &Self, batch: &RecordBatch) -> ArrowResult<RecordBatch> {
	//TODO: how to efficiently construct and return only the requested columns?
	//TODO: it is necessary to only return batch_limit data points to prevent skew?
	//TODO: should start_time and end_time be unsigned integers instead?
	let gids = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
	let start_times = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
	let end_times = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
	let mtids = batch.column(3).as_any().downcast_ref::<Int32Array>().unwrap();
	let models = batch.column(4).as_any().downcast_ref::<BinaryArray>().unwrap();
	let gaps = batch.column(5).as_any().downcast_ref::<BinaryArray>().unwrap();

	//Compute the number of data points to allocate space for
	let num_rows = batch.num_rows();
	let data_points = models::length(num_rows, gids, start_times, end_times,
					 &self.model_table.sampling_intervals);
	let mut tids = Int32Array::builder(data_points);
	let mut timestamps =
	    TimestampMillisecondArray::builder(data_points);
	let mut values = Float32Array::builder(data_points);

	//Reconstruct the data points using the models
	for row_index in 0..num_rows {
	    let gid = gids.value(row_index);
	    let start_time = start_times.value(row_index);
	    let end_time = end_times.value(row_index);
	    let mtid = mtids.value(row_index);
	    let sampling_interval = *self.model_table.sampling_intervals
		.get(gid as usize).unwrap();
	    let model = models.value(row_index);
	    let gaps = gaps.value(row_index);
	    models::grid(gid, start_time, end_time, mtid, sampling_interval,
			 model, gaps, &mut tids, &mut timestamps, &mut values);
	}

	//Return the batch of reconstructed data points
	let columns: Vec<ArrayRef> = vec!(Arc::new(tids.finish()), Arc::new(timestamps.finish()), Arc::new(values.finish()));
	let batch = RecordBatch::try_new(self.schema.clone(), columns).unwrap();
	Ok(batch)
    }
}

impl Stream for GridStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
		 -> Poll<Option<Self::Item>> {
	self.input.poll_next_unpin(cx).map(|x| match x {
	    Some(Ok(batch)) => Some(self.grid(&batch)),
	    other => other
	})
    }
}

impl RecordBatchStream for GridStream {
    fn schema(&self) -> SchemaRef {
	self.schema.clone()
    }
}
