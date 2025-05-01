/* Copyright 2024 The ModelarDB Contributors
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

//! Utility functions to register metadata tables, normal tables, and time series tables with Apache
//! DataFusion and to read and write Apache Parquet files to and from an object store.

pub mod delta_lake;
pub mod error;
pub mod metadata;
mod optimizer;
pub mod parser;
mod query;
pub mod test;

use std::result::Result as StdResult;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float32Builder, ListArray,
    ListBuilder, RecordBatch, StringArray, StringBuilder,
};
use arrow::compute;
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow_flight::{IpcMessage, SchemaAsIpc};
use bytes::{Buf, Bytes};
use datafusion::catalog::TableProvider;
use datafusion::common::{DFSchema, ToDFSchema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::parquet::arrow::async_reader::{
    AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStream,
};
use datafusion::parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::format::SortingColumn;
use datafusion::physical_plan::insert::DataSink;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use deltalake::DeltaTable;
use futures::StreamExt;
use modelardb_types::schemas::TABLE_METADATA_SCHEMA;
use modelardb_types::types::{ErrorBound, GeneratedColumn, TimeSeriesTableMetadata};
use object_store::ObjectStore;
use object_store::path::Path;
use sqlparser::ast::Statement;

use crate::error::{ModelarDbStorageError, Result};
use crate::parser::tokenize_and_parse_sql_expression;
use crate::query::metadata_table::MetadataTable;
use crate::query::normal_table::NormalTable;
use crate::query::time_series_table::TimeSeriesTable;

/// The folder storing compressed table data in the data folders.
const TABLE_FOLDER: &str = "tables";

/// The folder storing metadata in the data folders.
const METADATA_FOLDER: &str = "metadata";

/// Create a new [`SessionContext`] for interacting with Apache DataFusion. The [`SessionContext`]
/// is constructed with the default configuration, default resource managers, and additional
/// optimizer rules that rewrite simple aggregate queries to be executed directly on the segments
/// containing metadata and models instead of on reconstructed data points created from the segments
/// for time series tables.
pub fn create_session_context() -> SessionContext {
    let mut session_state_builder = SessionStateBuilder::new().with_default_features();

    // Uses the rule method instead of the rules method as the rules method replaces the built-ins.
    for physical_optimizer_rule in optimizer::physical_optimizer_rules() {
        session_state_builder =
            session_state_builder.with_physical_optimizer_rule(physical_optimizer_rule);
    }

    let session_state = session_state_builder.build();
    SessionContext::new_with_state(session_state)
}

/// Register the metadata table stored in `delta_table` with `table_name` in `session_context`. If
/// the metadata table could not be registered with Apache DataFusion, return
/// [`ModelarDbStorageError`].
pub fn register_metadata_table(
    session_context: &SessionContext,
    table_name: &str,
    delta_table: DeltaTable,
) -> Result<()> {
    let metadata_table = Arc::new(MetadataTable::new(delta_table));
    session_context.register_table(table_name, metadata_table)?;

    Ok(())
}

/// Register the normal table stored in `delta_table` with `table_name` and `data_sink` in
/// `session_context`. If the normal table could not be registered with Apache DataFusion, return
/// [`ModelarDbStorageError`].
pub fn register_normal_table(
    session_context: &SessionContext,
    table_name: &str,
    delta_table: DeltaTable,
    data_sink: Arc<dyn DataSink>,
) -> Result<()> {
    let normal_table = Arc::new(NormalTable::new(delta_table, data_sink));
    session_context.register_table(table_name, normal_table)?;

    Ok(())
}

/// Register the time series table stored in `delta_table` with `time_series_table_metadata` and
/// `data_sink` in `session_context`. If the time series table could not be registered with Apache
/// DataFusion, return [`ModelarDbStorageError`].
pub fn register_time_series_table(
    session_context: &SessionContext,
    delta_table: DeltaTable,
    time_series_table_metadata: Arc<TimeSeriesTableMetadata>,
    data_sink: Arc<dyn DataSink>,
) -> Result<()> {
    let time_series_table =
        TimeSeriesTable::new(delta_table, time_series_table_metadata.clone(), data_sink);

    session_context.register_table(&time_series_table_metadata.name, time_series_table)?;

    Ok(())
}

/// Return the [`Arc<TimeSeriesTableMetadata>`] of the table `maybe_time_series_table` if it is a
/// time series table, otherwise [`None`] is returned.
pub fn maybe_table_provider_to_time_series_table_metadata(
    maybe_time_series_table: Arc<dyn TableProvider>,
) -> Option<Arc<TimeSeriesTableMetadata>> {
    maybe_time_series_table
        .as_any()
        .downcast_ref::<TimeSeriesTable>()
        .map(|time_series_table| time_series_table.time_series_table_metadata())
}

/// Execute `statement` in `session_context` and return the result as a
/// [`SendableRecordBatchStream`]. If `statement` could not be executed successfully,
/// [`ModelarDbStorageError`] is returned.
pub async fn execute_statement(
    session_context: &SessionContext,
    statement: Statement,
) -> Result<SendableRecordBatchStream> {
    let session_state = session_context.state();
    let df_statement = DFStatement::Statement(Box::new(statement));

    let logical_plan = session_state.statement_to_plan(df_statement).await?;
    let data_frame = session_context.execute_logical_plan(logical_plan).await?;
    let sendable_record_batch_stream = data_frame.execute_stream().await?;

    Ok(sendable_record_batch_stream)
}

/// Execute the SQL query `sql` in `session_context` and return the result as a single
/// [`RecordBatch`]. If the query could not be executed successfully, return
/// [`ModelarDbStorageError`].
pub async fn sql_and_concat(session_context: &SessionContext, sql: &str) -> Result<RecordBatch> {
    let dataframe = session_context.sql(sql).await?;
    let schema = Schema::from(dataframe.schema());

    let record_batches = dataframe.collect().await?;
    let record_batch = concat_batches(&schema.into(), &record_batches)?;

    Ok(record_batch)
}

/// Read all rows from the Apache Parquet file at the location given by `file_path` in
/// `object_store` and return them as a [`RecordBatch`]. If the file could not be read successfully,
/// [`ModelarDbStorageError`] is returned.
pub async fn read_record_batch_from_apache_parquet_file(
    file_path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<RecordBatch> {
    // Create an object reader for the Apache Parquet file.
    let file_metadata = object_store
        .head(file_path)
        .await
        .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

    let reader = ParquetObjectReader::new(object_store, file_metadata);

    // Stream the data from the Apache Parquet file into a single record batch.
    let record_batches = read_batches_from_apache_parquet_file(reader).await?;

    let schema = record_batches[0].schema();
    compute::concat_batches(&schema, &record_batches).map_err(|error| error.into())
}

/// Read each batch of data from the Apache Parquet file given by `reader` and return them as a
/// [`Vec`] of [`RecordBatch`]. If the file could not be read successfully,
/// [`ModelarDbStorageError`] is returned.
pub async fn read_batches_from_apache_parquet_file<R>(reader: R) -> Result<Vec<RecordBatch>>
where
    R: AsyncFileReader + Send + Unpin + 'static,
    ParquetRecordBatchStream<R>: StreamExt<Item = StdResult<RecordBatch, ParquetError>>,
{
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let mut stream = builder.build()?;

    let mut record_batches = Vec::new();
    while let Some(maybe_record_batch) = stream.next().await {
        let record_batch = maybe_record_batch?;
        record_batches.push(record_batch);
    }

    Ok(record_batches)
}

/// Write the rows in `record_batch` to an Apache Parquet file at the location given by `file_path`
/// in `object_store`. `file_path` must use the extension `.parquet`. `sorting_columns` can be set
/// to control the sorting order of the rows in the written file. Return [`Ok`] if the file was
/// written successfully, otherwise return [`ModelarDbStorageError`].
pub async fn write_record_batch_to_apache_parquet_file(
    file_path: &Path,
    record_batch: &RecordBatch,
    sorting_columns: Option<Vec<SortingColumn>>,
    object_store: &dyn ObjectStore,
) -> Result<()> {
    // Check if the extension of the given path is correct.
    if file_path.extension() == Some("parquet") {
        let props = apache_parquet_writer_properties(sorting_columns);

        // Write the record batch to the object store.
        let mut buffer = Vec::new();
        let mut writer =
            AsyncArrowWriter::try_new(&mut buffer, record_batch.schema(), Some(props))?;
        writer.write(record_batch).await?;
        writer.close().await?;

        object_store
            .put(file_path, Bytes::from(buffer).into())
            .await
            .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

        Ok(())
    } else {
        Err(ParquetError::General(format!(
            "'{}' is not a valid file path for an Apache Parquet file.",
            file_path.as_ref()
        )))?
    }
}

/// Return [`WriterProperties`] optimized for compressed segments for Apache Parquet and Delta Lake.
fn apache_parquet_writer_properties(
    sorting_columns: Option<Vec<SortingColumn>>,
) -> WriterProperties {
    WriterProperties::builder()
        .set_data_page_size_limit(16384)
        .set_max_row_group_size(65536)
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .set_sorting_columns(sorting_columns)
        .build()
}

/// Convert a [`Schema`] to [`Vec<u8>`].
pub fn try_convert_schema_to_bytes(schema: &Schema) -> Result<Vec<u8>> {
    let options = IpcWriteOptions::default();
    let schema_as_ipc = SchemaAsIpc::new(schema, &options);

    let ipc_message: IpcMessage = schema_as_ipc.try_into()?;

    Ok(ipc_message.0.to_vec())
}

/// Return [`Schema`] if `schema_bytes` can be converted to an Apache Arrow schema, otherwise
/// [`ModelarDbStorageError`].
pub fn try_convert_bytes_to_schema(schema_bytes: Vec<u8>) -> Result<Schema> {
    let ipc_message = IpcMessage(schema_bytes.into());
    Schema::try_from(ipc_message).map_err(|error| error.into())
}

/// Convert a [`RecordBatch`] to a [`Vec<u8>`].
pub fn try_convert_record_batch_to_bytes(record_batch: &RecordBatch) -> Result<Vec<u8>> {
    let options = IpcWriteOptions::default();
    let mut writer = StreamWriter::try_new_with_options(vec![], &record_batch.schema(), options)?;

    writer.write(record_batch)?;
    writer.into_inner().map_err(|error| error.into())
}

/// Return [`RecordBatch`] if `record_batch_bytes` can be converted to an Apache Arrow [`RecordBatch`],
/// otherwise [`ModelarDbStorageError`].
pub fn try_convert_bytes_to_record_batch(
    record_batch_bytes: Vec<u8>,
    schema: &Arc<Schema>,
) -> Result<RecordBatch> {
    let bytes: Bytes = record_batch_bytes.into();
    let reader = StreamReader::try_new(bytes.reader(), None)?;

    let mut record_batches = vec![];
    for maybe_record_batch in reader {
        let record_batch = maybe_record_batch?;
        record_batches.push(record_batch);
    }

    concat_batches(schema, &record_batches).map_err(|error| error.into())
}

/// Return a [`RecordBatch`] constructed from the metadata of a normal table with the name
/// `table_name` and the schema `schema`. If the schema could not be converted to bytes or the
/// [`RecordBatch`] could not be created, return [`ModelarDbStorageError`].
pub fn normal_table_metadata_to_record_batch(
    table_name: &str,
    schema: &Schema,
) -> Result<RecordBatch> {
    let query_schema_bytes = try_convert_schema_to_bytes(schema)?;

    let error_bounds_field = Arc::new(Field::new("item", DataType::Float32, true));
    let generated_columns_field = Arc::new(Field::new("item", DataType::Utf8, true));

    RecordBatch::try_new(
        TABLE_METADATA_SCHEMA.0.clone(),
        vec![
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(StringArray::from(vec![table_name])),
            Arc::new(BinaryArray::from_vec(vec![&query_schema_bytes])),
            Arc::new(ListArray::new_null(error_bounds_field, 1)),
            Arc::new(ListArray::new_null(generated_columns_field, 1)),
        ],
    )
    .map_err(|error| error.into())
}

/// Return a [`RecordBatch`] constructed from the metadata in `time_series_table_metadata`. If the
/// schema could not be converted to bytes or the [`RecordBatch`] could not be created, return
/// [`ModelarDbStorageError`].
pub fn time_series_table_metadata_to_record_batch(
    time_series_table_metadata: &TimeSeriesTableMetadata,
) -> Result<RecordBatch> {
    // Since the time series table metadata does not include error bounds for the generated columns,
    // lossless error bounds are added for each generated column.
    let mut error_bounds_all =
        Vec::with_capacity(time_series_table_metadata.query_schema.fields().len());

    let lossless = ErrorBound::try_new_absolute(0.0)?;

    for field in time_series_table_metadata.query_schema.fields() {
        if let Ok(field_index) = time_series_table_metadata.schema.index_of(field.name()) {
            error_bounds_all.push(time_series_table_metadata.error_bounds[field_index]);
        } else {
            error_bounds_all.push(lossless);
        }
    }

    let query_schema_bytes = try_convert_schema_to_bytes(&time_series_table_metadata.query_schema)?;
    let error_bounds_array = error_bounds_to_list_array(error_bounds_all);
    let generated_columns_array =
        generated_columns_to_list_array(time_series_table_metadata.generated_columns.clone());

    RecordBatch::try_new(
        TABLE_METADATA_SCHEMA.0.clone(),
        vec![
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(StringArray::from(vec![
                time_series_table_metadata.name.clone(),
            ])),
            Arc::new(BinaryArray::from_vec(vec![&query_schema_bytes])),
            Arc::new(error_bounds_array),
            Arc::new(generated_columns_array),
        ],
    )
    .map_err(|error| error.into())
}

/// Convert a list of [`ErrorBounds`](ErrorBound) to a [`ListArray`].
fn error_bounds_to_list_array(error_bounds: Vec<ErrorBound>) -> ListArray {
    let mut error_bounds_builder = ListBuilder::new(Float32Builder::new());

    for error_bound in error_bounds {
        match error_bound {
            ErrorBound::Absolute(value) => {
                error_bounds_builder.values().append_value(value);
            }
            ErrorBound::Relative(value) => {
                // Relative error bounds are encoded as negative values for simplicity.
                error_bounds_builder.values().append_value(-value);
            }
        }
    }

    error_bounds_builder.append(true);
    error_bounds_builder.finish()
}

/// Convert a list of optional [`GeneratedColumns`](GeneratedColumn) to a [`ListArray`].
fn generated_columns_to_list_array(generated_columns: Vec<Option<GeneratedColumn>>) -> ListArray {
    let mut generated_columns_builder = ListBuilder::new(StringBuilder::new());

    for generated_column in generated_columns {
        if let Some(generated_column) = generated_column {
            let sql_expr = generated_column.original_expr;
            generated_columns_builder.values().append_value(sql_expr);
        } else {
            generated_columns_builder.values().append_null();
        }
    }

    generated_columns_builder.append(true);
    generated_columns_builder.finish()
}

/// Extract the table metadata from `record_batch` and return the table metadata as a tuple of
/// `(normal_table_metadata, time_series_table_metadata)`. `normal_table_metadata` is a vector of tuples
/// containing the table name and schema of the normal tables. If the schema of the [`RecordBatch`]
/// is invalid or the table metadata could not be extracted, return [`ModelarDbStorageError`].
#[allow(clippy::type_complexity)]
pub fn table_metadata_from_record_batch(
    record_batch: &RecordBatch,
) -> Result<(Vec<(String, Schema)>, Vec<TimeSeriesTableMetadata>)> {
    if record_batch.schema() != TABLE_METADATA_SCHEMA.0 {
        return Err(ModelarDbStorageError::InvalidArgument(
            "Record batch does not contain the expected table metadata.".to_owned(),
        ));
    }

    let mut normal_table_metadata = Vec::new();
    let mut time_series_table_metadata = Vec::new();

    let is_time_series_table_array = modelardb_types::array!(record_batch, 0, BooleanArray);
    let name_array = modelardb_types::array!(record_batch, 1, StringArray);
    let schema_array = modelardb_types::array!(record_batch, 2, BinaryArray);
    let error_bounds_array = modelardb_types::array!(record_batch, 3, ListArray);
    let generated_columns_array = modelardb_types::array!(record_batch, 4, ListArray);

    for row_index in 0..record_batch.num_rows() {
        let is_time_series_table = is_time_series_table_array.value(row_index);
        let table_name = name_array.value(row_index).to_owned();
        let schema = try_convert_bytes_to_schema(schema_array.value(row_index).to_vec())?;

        if is_time_series_table {
            let error_bounds = array_to_error_bounds(error_bounds_array.value(row_index))?;

            let generated_columns = array_to_generated_columns(
                generated_columns_array.value(row_index),
                &schema.clone().to_dfschema()?,
            )?;

            let metadata = TimeSeriesTableMetadata::try_new(
                table_name,
                Arc::new(schema),
                error_bounds,
                generated_columns,
            )?;

            time_series_table_metadata.push(metadata);
        } else {
            normal_table_metadata.push((table_name, schema));
        }
    }

    Ok((normal_table_metadata, time_series_table_metadata))
}

/// Parse the error bound values in `error_bounds_array` into a list of [`ErrorBounds`](ErrorBound).
/// Returns [`ModelarDbServerError`] if an error bound value is invalid.
fn array_to_error_bounds(error_bounds_array: ArrayRef) -> Result<Vec<ErrorBound>> {
    let value_array = modelardb_types::cast!(error_bounds_array, Float32Array);
    let mut error_bounds = Vec::with_capacity(value_array.len());
    for value in value_array.iter().flatten() {
        if value < 0.0 {
            error_bounds.push(ErrorBound::try_new_relative(-value)?);
        } else {
            error_bounds.push(ErrorBound::try_new_absolute(value)?);
        }
    }

    Ok(error_bounds)
}

/// Parse the generated column expressions in `generated_columns_array` into a list of optional
/// [`GeneratedColumns`](GeneratedColumn). Returns [`ModelarDbServerError`] if a generated column
/// expression is invalid.
fn array_to_generated_columns(
    generated_columns_array: ArrayRef,
    df_schema: &DFSchema,
) -> Result<Vec<Option<GeneratedColumn>>> {
    let expr_array = modelardb_types::cast!(generated_columns_array, StringArray);
    let mut generated_columns = Vec::with_capacity(expr_array.len());
    for maybe_expr in expr_array.iter() {
        if let Some(expr) = maybe_expr {
            let sql_expr = tokenize_and_parse_sql_expression(expr, df_schema)?;
            generated_columns.push(Some(GeneratedColumn::try_from_sql_expr(
                sql_expr, df_schema, expr,
            )?));
        } else {
            generated_columns.push(None);
        }
    }

    Ok(generated_columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Array, Float32Array};
    use arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
    use modelardb_types::types::ArrowValue;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::test;

    // Tests for read_record_batch_from_apache_parquet_file().
    #[tokio::test]
    async fn test_read_record_batch_from_apache_parquet_file() {
        let record_batch = test::compressed_segments_record_batch();
        let apache_parquet_path = Path::from("test.parquet");

        let (temp_dir, _result) =
            write_record_batch_to_temp_dir(&apache_parquet_path, &record_batch).await;

        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result =
            read_record_batch_from_apache_parquet_file(&apache_parquet_path, object_store).await;

        assert!(result.is_ok());
        assert_eq!(record_batch, result.unwrap());
    }

    #[tokio::test]
    async fn test_read_record_batch_from_non_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        let path = Path::from("test.txt");
        object_store
            .put(&path, Bytes::from(Vec::new()).into())
            .await
            .unwrap();

        let result = read_record_batch_from_apache_parquet_file(&path, object_store);
        assert!(result.await.is_err());
    }

    #[tokio::test]
    async fn test_read_record_batch_from_non_existent_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        let path = Path::from("test.parquet");

        let result = read_record_batch_from_apache_parquet_file(&path, object_store);
        assert!(result.await.is_err());
    }

    // Tests for write_record_batch_to_apache_parquet_file().
    #[tokio::test]
    async fn test_write_record_batch_to_apache_parquet_file() {
        let record_batch = test::compressed_segments_record_batch();
        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test.parquet"), &record_batch).await;

        assert!(result.is_ok());
        assert!(temp_dir.path().join("test.parquet").exists());
    }

    #[tokio::test]
    async fn test_write_empty_record_batch_to_apache_parquet_file() {
        let fields: Vec<Field> = vec![];
        let schema = Schema::new(fields);
        let record_batch = RecordBatch::new_empty(Arc::new(schema));

        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test.parquet"), &record_batch).await;

        assert!(result.is_ok());
        assert!(temp_dir.path().join("test.parquet").exists());
    }

    #[tokio::test]
    async fn test_write_record_batch_to_file_path_with_invalid_extension() {
        let record_batch = test::compressed_segments_record_batch();
        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test.txt"), &record_batch).await;

        assert!(result.is_err());
        assert!(!temp_dir.path().join("test.txt").exists());
    }

    #[tokio::test]
    async fn test_write_record_batch_to_file_path_without_extension() {
        let record_batch = test::compressed_segments_record_batch();
        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test"), &record_batch).await;

        assert!(result.is_err());
        assert!(!temp_dir.path().join("test").exists());
    }

    async fn write_record_batch_to_temp_dir(
        file_path: &Path,
        record_batch: &RecordBatch,
    ) -> (TempDir, Result<()>) {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

        let result =
            write_record_batch_to_apache_parquet_file(file_path, record_batch, None, &object_store)
                .await;

        (temp_dir, result)
    }

    // Tests for try_convert_schema_to_bytes() and try_convert_bytes_to_schema().
    #[test]
    fn test_schema_to_bytes_and_bytes_to_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
        ]));

        // Serialize the schema to bytes.
        let bytes = try_convert_schema_to_bytes(&schema).unwrap();

        // Deserialize the bytes to the schema.
        let bytes_schema = try_convert_bytes_to_schema(bytes).unwrap();
        assert_eq!(*schema, bytes_schema);
    }

    #[test]
    fn test_invalid_bytes_to_schema() {
        assert!(try_convert_bytes_to_schema(vec!(1, 2, 4, 8)).is_err());
    }

    // Tests for try_convert_record_batch_to_bytes() and try_convert_bytes_to_record_batch().
    #[test]
    fn test_convert_record_batch_to_bytes_and_bytes_to_record_batch() {
        let record_batch = test::normal_table_record_batch();

        // Serialize the record batch to bytes.
        let bytes = try_convert_record_batch_to_bytes(&record_batch).unwrap();

        // Deserialize the bytes to the record batch.
        let bytes_record_batch =
            try_convert_bytes_to_record_batch(bytes, &record_batch.schema()).unwrap();

        assert_eq!(record_batch, bytes_record_batch);
    }

    #[test]
    fn test_convert_invalid_bytes_to_record_batch() {
        let result = try_convert_bytes_to_record_batch(
            vec![1, 2, 4, 8],
            &Arc::new(test::normal_table_schema()),
        );

        assert_eq!(
            result.unwrap_err().to_string(),
            "Arrow Error: Io error: failed to fill whole buffer"
        );
    }

    #[test]
    fn test_convert_bytes_with_invalid_schema_to_record_batch() {
        let bytes = try_convert_record_batch_to_bytes(&test::normal_table_record_batch()).unwrap();

        let field = Field::new("field", ArrowValue::DATA_TYPE, false);
        let schema = Arc::new(Schema::new(vec![field]));
        let result = try_convert_bytes_to_record_batch(bytes, &schema);

        assert_eq!(
            result.unwrap_err().to_string(),
            "Arrow Error: Invalid argument error: column types must match schema types, expected \
            Float32 but found Timestamp(Microsecond, None) at column index 0"
        );
    }

    // Tests for normal_table_metadata_to_record_batch() and time_series_table_metadata_to_record_batch().
    #[test]
    fn test_normal_table_metadata_to_record_batch() {
        let schema = test::normal_table_schema();
        let record_batch =
            normal_table_metadata_to_record_batch(test::NORMAL_TABLE_NAME, &schema).unwrap();

        assert_eq!(**record_batch.column(0), BooleanArray::from(vec![false]));
        assert_eq!(
            **record_batch.column(1),
            StringArray::from(vec![test::NORMAL_TABLE_NAME])
        );
        assert_eq!(
            **record_batch.column(2),
            BinaryArray::from_vec(vec![&try_convert_schema_to_bytes(&schema).unwrap()])
        );
    }

    #[test]
    fn test_time_series_table_metadata_to_record_batch() {
        let time_series_table_metadata = test::time_series_table_metadata();
        let record_batch =
            time_series_table_metadata_to_record_batch(&time_series_table_metadata).unwrap();

        assert_eq!(**record_batch.column(0), BooleanArray::from(vec![true]));
        assert_eq!(
            **record_batch.column(1),
            StringArray::from(vec![test::TIME_SERIES_TABLE_NAME])
        );

        let expected_schema_bytes =
            try_convert_schema_to_bytes(&time_series_table_metadata.query_schema).unwrap();
        assert_eq!(
            **record_batch.column(2),
            BinaryArray::from_vec(vec![&expected_schema_bytes])
        );

        let error_bounds_array = modelardb_types::array!(record_batch, 3, ListArray).value(0);
        let value_array = modelardb_types::cast!(error_bounds_array, Float32Array);

        assert_eq!(value_array, &Float32Array::from(vec![0.0, 1.0, -5.0, -0.0]));

        let generated_columns_array = modelardb_types::array!(record_batch, 4, ListArray).value(0);
        let expr_array = modelardb_types::cast!(generated_columns_array, StringArray);

        assert_eq!(expr_array, &StringArray::new_null(4));
    }

    // Tests for table_metadata_from_record_batch().
    #[test]
    fn test_table_metadata_from_record_batch() {
        let table_record_batch = test::table_metadata_record_batch();

        let (normal_table_metadata, time_series_table_metadata) =
            table_metadata_from_record_batch(&table_record_batch).unwrap();

        assert_eq!(normal_table_metadata.len(), 1);
        assert_eq!(normal_table_metadata[0].0, test::NORMAL_TABLE_NAME);
        assert_eq!(normal_table_metadata[0].1, test::normal_table_schema());

        let metadata = test::time_series_table_metadata();
        assert_eq!(time_series_table_metadata.len(), 1);
        assert_eq!(time_series_table_metadata[0].name, metadata.name);
        assert_eq!(
            time_series_table_metadata[0].query_schema,
            metadata.query_schema
        );
    }

    #[test]
    fn test_table_metadata_from_invalid_record_batch() {
        let record_batch = test::normal_table_record_batch();
        let result = table_metadata_from_record_batch(&record_batch);

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: Record batch does not contain the expected table metadata."
        );
    }
}
