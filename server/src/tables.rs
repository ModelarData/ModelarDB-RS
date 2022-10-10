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

//! Implementation of the types required to query model tables through Apache
//! Arrow DataFusion. The types are [`ModelTable`] which implements
//! [`TableProvider`], [`GridExec`] which implements [`ExecutionPlan`], and
//! [`GridStream`] which implements [`Stream`] and [`RecordBatchStream`].

use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as StdTaskContext, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayAccessor, ArrayRef, BinaryArray, DictionaryArray, Float32Array, StringArray, UInt64Array,
    UInt8Array,
};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field, Schema, SchemaRef, UInt16Type};
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
use datafusion::scalar::ScalarValue;
use datafusion_physical_expr::planner;
use futures::stream::{Stream, StreamExt};
use rusqlite::{Connection, Result as RusqliteResult};

use crate::catalog;
use crate::catalog::ModelTableMetadata;
use crate::models;
use crate::storage::StorageEngine;
use crate::types::{
    ArrowTimeSeriesId, ArrowTimestamp, ArrowValue, TimeSeriesId, TimestampArray, TimestampBuilder,
    ValueArray, ValueBuilder,
};
use crate::Context;

/// A queryable representation of a model table which stores multivariate time
/// series as segments containing metadata and models. [`ModelTable`] implements
/// [`TableProvider`] so it can be registered with Apache Arrow DataFusion and
/// the multivariate time series queried as multiple univariate time series.
pub struct ModelTable {
    /// Access to the system's configuration and components.
    context: Arc<Context>,
    /// Location of the object store used by the storage engine.
    object_store_url: ObjectStoreUrl,
    /// Metadata required to query the model table.
    model_table_metadata: Arc<ModelTableMetadata>,
    /// Schema of the model table registered with Apache Arrow DataFusion.
    schema: Arc<Schema>,
    /// Field column to use for queries that do not include fields.
    fallback_field_column: u64,
}

impl ModelTable {
    pub fn new(context: Arc<Context>, model_table_metadata: &Arc<ModelTableMetadata>) -> Arc<Self> {
        // Columns in the model table registered with Apache Arrow DataFusion.
        let columns = vec![
            Field::new("tid", ArrowTimeSeriesId::DATA_TYPE, false),
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("value", ArrowValue::DATA_TYPE, false),
        ];

        // Compute the index of the first field column in the model table's
        // schema. This is used for queries that does not contain any fields.
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

    // TODO: Move to the metadata component when it exists.
    /// Compute the 64-bit keys of the univariate time series to retrieve from
    /// the storage engine using the fields, tag, and tag values in the query.
    /// Returns a [`DataFusionError::Plan`] if the necessary data cannot be
    /// retrieved from the metadata database.
    fn compute_keys_using_fields_and_tags(
        &self,
        table_name: &str,
        columns: &Option<Vec<usize>>,
        tag_predicates: &[(&str, &str)],
    ) -> Result<Vec<TimeSeriesId>> {
        // Construct a query that extracts the field columns in the table being
        // queried which overlaps with the columns being requested by the query.
        let query_field_columns = if columns.is_none() {
            format!(
                "SELECT column_index FROM model_table_field_columns WHERE table_name = '{}'",
                table_name
            )
        } else {
            let column_predicates: Vec<String> = columns
                .clone()
                .unwrap()
                .iter()
                .map(|column| format!("column_index = {}", column))
                .collect();

            format!(
                "SELECT column_index FROM model_table_field_columns WHERE table_name = '{}' AND {}",
                table_name,
                column_predicates.join(" OR ")
            )
        };

        // Construct a query that extracts the hashes of the multivariate time
        // series in the table with tag values that match those in the query.
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

        // Retrieve the hashes using the queries and reconstruct the keys.
        self.compute_keys_using_metadata_database(&query_field_columns, &query_hashes)
            .map_err(|error| DataFusionError::Plan(error.to_string()))
    }

    // TODO: Move to the metadata component when it exists.
    /// Compute the 64-bit keys of the univariate time series to retrieve from
    /// the storage engine using the two queries constructed from the fields,
    /// tag, and tag values in the user's query. Returns a [`RusqliteResult`]
    /// with an [`Error`](rusqlite::Error) if the data cannot be retrieved from
    /// the metadata database, otherwise the keys are returned.
    fn compute_keys_using_metadata_database(
        &self,
        query_field_columns: &str,
        query_hashes: &str,
    ) -> RusqliteResult<Vec<u64>> {
        // Compute the location of the database containing the metadata.
        let database_path = {
            // unwrap() is safe as read() only fails if the RwLock is poisoned.
            let catalog = self.context.catalog.read().unwrap();
            catalog.data_folder_path.join(catalog::METADATA_SQLITE_NAME)
        };

        // Open a connection to the database containing the metadata.
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
            // SQLite use signed integers https://www.sqlite.org/datatype3.html.
            let signed_tag_hash = row.get::<usize, i64>(0)?;
            let tag_hash = u64::from_ne_bytes(signed_tag_hash.to_ne_bytes());

            for field_column in &field_columns {
                keys.push(tag_hash | field_column);
            }
        }
        Ok(keys)
    }
}

/// Rewrite `filters` in terms of the model table's schema to filters in
/// terms of the schema used for compressed data by the storage engine. The
/// rewritten filters are then combined into a single [`Expr`]. A [`None`]
/// is returned if `filters` is empty.
fn rewrite_and_combine_filters(filters: &[Expr]) -> Option<Expr> {
    let rewritten_filters: Vec<Expr> = filters
        .iter()
        .map(|filter| match filter {
            Expr::BinaryExpr { left, op, right } => {
                if **left == col("timestamp") {
                    match op {
                        Operator::Gt => new_binary_expr(col("end_time"), *op, *right.clone()),
                        Operator::GtEq => new_binary_expr(col("end_time"), *op, *right.clone()),
                        Operator::Lt => new_binary_expr(col("start_time"), *op, *right.clone()),
                        Operator::LtEq => new_binary_expr(col("start_time"), *op, *right.clone()),
                        Operator::Eq => new_binary_expr(
                            new_binary_expr(col("start_time"), Operator::LtEq, *right.clone()),
                            Operator::And,
                            new_binary_expr(col("end_time"), Operator::GtEq, *right.clone()),
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

    // Combine the rewritten filters into an expression.
    combine_filters(&rewritten_filters)
}

/// Create a [`Expr::BinaryExpr`].
fn new_binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

/// Create a [`FilterExec`]. [`None`] is returned if `predicate` is
/// [`None`].
fn new_filter_exec(
    predicate: &Option<Expr>,
    input: &Arc<ParquetExec>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let predicate = predicate
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("predicate is None".to_owned()))?;

    let schema = StorageEngine::get_compressed_segment_schema();
    let df_schema = schema.clone().to_dfschema()?;

    let physical_predicate =
        planner::create_physical_expr(predicate, &df_schema, &schema, &ExecutionProps::new())?;

    Ok(Arc::new(FilterExec::try_new(
        physical_predicate,
        input.clone(),
    )?))
}

#[async_trait]
impl TableProvider for ModelTable {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the schema of the model table registered with Apache Arrow
    /// DataFusion.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Specify that model tables are base tables and not views or temporary.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Specify that model tables performs inexact predicate push-down.
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    /// Create an [`ExecutionPlan`] that will scan the table. Returns a
    /// [`DataFusionError::Plan`] if the necessary metadata cannot be retrieved
    /// from the metadata database.
    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: extract predicates that consist of tag = tag_value from the query.
        let tag_predicates = vec![];
        let keys = self.compute_keys_using_fields_and_tags(
            &self.model_table_metadata.name,
            projection,
            &tag_predicates,
        )?;

        // Request the matching files from the storage engine.
        let mut key_object_metas = {
            // TODO: make the storage engine support multiple parallel readers.
            // unwrap() is safe as read() only fails if the RwLock is poisoned.
            let mut storage_engines = self.context.storage_engine.write().unwrap();

            // unwrap() is safe to use as get_compressed_files() only fails if a
            // non-existing hash is passed or if end time is before start time.
            storage_engines
                .get_compressed_files(&keys, None, None)
                .unwrap()
        };

        // Create the data source operator. Assumes the ObjectStore exists.
        let partitioned_files: Vec<PartitionedFile> = key_object_metas
            .drain(0..)
            .map(|key_object_meta| PartitionedFile {
                object_meta: key_object_meta.1,
                partition_values: vec![ScalarValue::Utf8(Some(key_object_meta.0))],
                range: None,
                extensions: None,
            })
            .collect::<Vec<PartitionedFile>>();

        // TODO: predict the accumulate size of the input data after filtering.
        let statistics = Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        };

        // TODO: partition the rows in the files to support parallel processing.
        let file_scan_config = FileScanConfig {
            object_store_url: self.object_store_url.clone(),
            file_schema: Arc::new(StorageEngine::get_compressed_segment_schema()),
            file_groups: vec![partitioned_files],
            statistics,
            projection: None,
            limit,
            table_partition_cols: vec!["storage_engine_key".to_owned()],
        };

        let predicate = rewrite_and_combine_filters(filters);
        let parquet_exec = Arc::new(ParquetExec::new(file_scan_config, predicate.clone(), None));

        // Create a filter operator if filters are not empty.
        let input = new_filter_exec(&predicate, &parquet_exec).unwrap_or(parquet_exec);

        // Create the gridding operator.
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
    /// Operator to read batches of rows from.
    input: Arc<dyn ExecutionPlan>,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
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
        // Modifies the schema so it matches the passed projection.
        let schema_after_projection = if let Some(ref projection) = projection {
            Arc::new(schema.project(projection).unwrap())
        } else {
            schema
        };

        // Ensures a projection is present for looking up columns to return.
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
                input: children[0].clone(),
                metrics: self.metrics.clone(),
            }))
        } else {
            Err(DataFusionError::Plan(format!(
                "A single child must be provided {:?}",
                self
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

    // TODO: it is necessary to return batch_size data points to prevent skew?
    // TODO: limit the batches of data points to only contain what is needed.
    /// Reconstruct the data points from the metadata and models in the segments
    /// in `batch`, and return batches of rows with data points.
    fn grid(&self, batch: &RecordBatch) -> RecordBatch {
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

        // The get_array!() macro causes compile errors due to nested generics.
        let key_string_array = batch
            .column(8)
            .as_any()
            .downcast_ref::<DictionaryArray<UInt16Type>>()
            .unwrap()
            .downcast_dict::<StringArray>()
            .unwrap();

        // Each segment is guaranteed to contain at least one data point.
        let num_rows = batch.num_rows();
        let mut key_builder = UInt64Array::builder(num_rows);
        let mut timestamp_builder = TimestampBuilder::with_capacity(num_rows);
        let mut value_builder = ValueBuilder::with_capacity(num_rows);

        // Reconstructs the data points from the segments.
        for row_index in 0..num_rows {
            // unwrap() is safe as the storage engine created the strings.
            let tid: u64 = key_string_array.value(row_index).parse().unwrap();
            let model_type_id = model_type_id_array.value(row_index);
            let timestamps = timestamps_array.value(row_index);
            let start_time = start_time_array.value(row_index);
            let end_time = end_time_array.value(row_index);
            let values = values_array.value(row_index);
            let min_value = min_value_array.value(row_index);
            let max_value = max_value_array.value(row_index);

            models::grid(
                tid,
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

        // Returns the batch of reconstructed data points.
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.projection.len());
        for column in &self.projection {
            match column {
                0 => columns.push(Arc::new(key_builder.finish())),
                1 => columns.push(Arc::new(timestamp_builder.finish())),
                2 => columns.push(Arc::new(value_builder.finish())),
                _ => unimplemented!("Tags currently cannot be added."),
            }
        }

        // unwrap() is safe as columns are constructed from self.projection.
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

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_plan::lit;
    use datafusion::prelude::Expr;

    // Tests for rewrite_and_combine_filters().
    #[test]
    fn test_rewrite_empty_vec() {
        assert!(rewrite_and_combine_filters(&vec!()).is_none());
    }

    #[test]
    fn test_rewrite_greater_than_timestamp() {
        let filters = new_timestamp_filters(Operator::Gt);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_timestamp_expr(predicate, "end_time", Operator::Gt);
    }

    #[test]
    fn test_rewrite_greater_than_or_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::GtEq);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_timestamp_expr(predicate, "end_time", Operator::GtEq);
    }

    #[test]
    fn test_rewrite_less_than_timestamp() {
        let filters = new_timestamp_filters(Operator::Lt);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_timestamp_expr(predicate, "start_time", Operator::Lt);
    }

    #[test]
    fn test_rewrite_less_than_or_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::LtEq);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();
        assert_timestamp_expr(predicate, "start_time", Operator::LtEq);
    }

    #[test]
    fn test_rewrite_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::Eq);
        let predicate = rewrite_and_combine_filters(&filters).unwrap();

        if let Expr::BinaryExpr { left, op, right } = predicate {
            assert_timestamp_expr(*left, "start_time", Operator::LtEq);
            assert_eq!(op, Operator::And);
            assert_timestamp_expr(*right, "end_time", Operator::GtEq);
        } else {
            panic!("Expr is not a BinaryExpr.");
        }
    }

    fn new_timestamp_filters(operator: Operator) -> Vec<Expr> {
        vec![new_binary_expr(col("timestamp"), operator, lit(37))]
    }

    fn assert_timestamp_expr(expr: Expr, column: &str, operator: Operator) {
        if let Expr::BinaryExpr { left, op, right } = expr {
            assert_eq!(*left, col(column));
            assert_eq!(op, operator);
            assert_eq!(*right, lit(37));
        } else {
            panic!("Expr is not a BinaryExpr.");
        }
    }

    // Tests for new_filter_exec().
    #[test]
    fn test_new_filter_exec_without_predicates() {
        let parquet_exec = new_parquet_exec();
        assert!(new_filter_exec(&None, &parquet_exec).is_err());
    }

    #[test]
    fn test_new_filter_exec_with_predicates() {
        let filters = vec![new_binary_expr(
            col("model_type_id"),
            Operator::Eq,
            lit(1_u8),
        )];
        let predicates = rewrite_and_combine_filters(&filters);
        let parquet_exec = new_parquet_exec();

        assert!(new_filter_exec(&predicates, &parquet_exec).is_ok());
    }

    fn new_parquet_exec() -> Arc<ParquetExec> {
        let file_scan_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema: Arc::new(Schema::new(vec![Field::new(
                "model_type_id",
                DataType::UInt8,
                false,
            )])),
            file_groups: vec![],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
        };
        Arc::new(ParquetExec::new(file_scan_config, None, None))
    }
}
