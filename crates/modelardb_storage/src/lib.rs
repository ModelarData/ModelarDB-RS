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

pub mod data_folder;
pub mod error;
mod optimizer;
pub mod parser;
mod query;
pub mod write_ahead_log;

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute;
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Schema};
use bytes::Bytes;
use datafusion::catalog::{MemorySchemaProvider, TableProvider};
use datafusion::datasource::sink::DataSink;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::Statement as DFStatement;
use deltalake::DeltaTable;
use deltalake::parquet::arrow::arrow_reader::ArrowReaderOptions;
use deltalake::parquet::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch};
use deltalake::parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use deltalake::parquet::basic::{Compression, Encoding, ZstdLevel};
use deltalake::parquet::errors::{ParquetError, Result as ParquetResult};
use deltalake::parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, SortingColumn};
use deltalake::parquet::file::properties::{EnabledStatistics, WriterProperties};
use deltalake::parquet::schema::types::ColumnPath;
use futures::StreamExt;
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use modelardb_types::schemas::COMPRESSED_SCHEMA;
use modelardb_types::types::TimeSeriesTableMetadata;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use sqlparser::ast::Statement;

use crate::error::Result;
use crate::query::normal_table::NormalTable;
use crate::query::time_series_table::TimeSeriesTable;

/// An [`AsyncFileReader`] for an Apache Parquet file in an [`ObjectStore`]. The implementation was
/// based on the [`AsyncFileReader`] documentation, the `parquet/examples/object_store.rs` file in
/// the Arrow-RS repository, and `ParquetObjectReader` which was provided by the `parquet` crate.
struct ParquetObjectReader {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
}

impl AsyncFileReader for ParquetObjectReader {
    /// Retrieve the bytes in `range`.
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.object_store
            .get_range(&self.path, range)
            .map_err(object_store_to_parquet_error)
            .boxed()
    }

    /// Retrieve multiple byte ranges.
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        async move {
            self.object_store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(object_store_to_parquet_error)
        }
        .boxed()
    }

    /// Retrieve the metadata from this Apache Parquet file.
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_via_suffix_and_finish(self)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

impl MetadataSuffixFetch for &mut ParquetObjectReader {
    /// Fetches the last `suffix` bytes without knowing the file size.
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };

        async move {
            self.object_store
                .get_opts(&self.path, options)
                .await
                .map_err(object_store_to_parquet_error)?
                .bytes()
                .await
                .map_err(object_store_to_parquet_error)
        }
        .boxed()
    }
}

/// Convert [`ObjectStoreError`] to [`ParquetError`].
fn object_store_to_parquet_error(error_store_error: ObjectStoreError) -> ParquetError {
    ParquetError::External(Box::new(error_store_error))
}

/// The folder storing compressed table data in the data folders.
const TABLE_FOLDER: &str = "tables";

/// The folder storing metadata in the data folders.
const METADATA_FOLDER: &str = "metadata";

/// The folder storing the write-ahead log in the data folders.
pub const WRITE_AHEAD_LOG_FOLDER: &str = "wal";

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

    // Use the default configuration: https://datafusion.apache.org/user-guide/configs.html.
    let session_config = SessionConfig::new();
    session_state_builder = session_state_builder.with_config(session_config);

    let session_state = session_state_builder.build();
    let session_context = SessionContext::new_with_state(session_state);
    let default_catalog = session_context
        .catalog("datafusion")
        .expect("The datafusion catalog should always exist.");
    default_catalog
        .register_schema("metadata", Arc::new(MemorySchemaProvider::new()))
        .expect("Catalog register schema should never fail.");

    session_context
}

/// Register the normal table stored in `delta_table` with `table_name` and `data_sink` in
/// `session_context`. If the normal table could not be registered with Apache DataFusion, return
/// [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub async fn register_normal_table(
    session_context: &SessionContext,
    table_name: &str,
    delta_table: DeltaTable,
    data_sink: Arc<dyn DataSink>,
) -> Result<()> {
    let normal_table = Arc::new(NormalTable::try_new(delta_table, Some(data_sink)).await?);
    session_context.register_table(table_name, normal_table)?;

    Ok(())
}

/// Register the time series table stored in `delta_table` with `time_series_table_metadata` and
/// `data_sink` in `session_context`. If the time series table could not be registered with Apache
/// DataFusion, return [`ModelarDbStorageError`](error::ModelarDbStorageError).
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
    (maybe_time_series_table.as_ref() as &dyn Any)
        .downcast_ref::<TimeSeriesTable>()
        .map(|time_series_table| time_series_table.time_series_table_metadata())
}

/// Execute `statement` in `session_context` and return the result as a
/// [`SendableRecordBatchStream`]. If `statement` could not be executed successfully,
/// [`ModelarDbStorageError`](error::ModelarDbStorageError) is returned.
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
/// [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub async fn sql_and_concat(session_context: &SessionContext, sql: &str) -> Result<RecordBatch> {
    let dataframe = session_context.sql(sql).await?;
    let schema = dataframe.schema().inner().clone();

    let record_batches = dataframe.collect().await?;
    let record_batch = concat_batches(&schema, &record_batches)?;

    Ok(record_batch)
}

/// Read all rows from the Apache Parquet file at the location given by `file_path` in
/// `object_store` and return them as a [`RecordBatch`]. If the file could not be read successfully,
/// [`ModelarDbStorageError`](error::ModelarDbStorageError) is returned.
pub async fn read_record_batch_from_apache_parquet_file(
    file_path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<RecordBatch> {
    let reader = ParquetObjectReader {
        object_store,
        path: file_path.clone(),
    };

    // Stream the data from the Apache Parquet file into a single record batch.
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let mut stream = builder.build()?;

    let mut record_batches = Vec::new();
    while let Some(maybe_record_batch) = stream.next().await {
        let record_batch = maybe_record_batch?;
        record_batches.push(record_batch);
    }

    let schema = record_batches[0].schema();
    compute::concat_batches(&schema, &record_batches).map_err(|error| error.into())
}

/// Write the rows in `record_batch` to an Apache Parquet file at the location given by `file_path`
/// in `object_store`. `file_path` must use the extension `.parquet`. `sorting_columns` can be set
/// to control the sorting order of the rows in the written file. Return [`Ok`] if the file was
/// written successfully, otherwise return [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub async fn write_record_batch_to_apache_parquet_file(
    file_path: &Path,
    record_batch: &RecordBatch,
    object_store: &dyn ObjectStore,
) -> Result<()> {
    // Check if the extension of the given path is correct.
    if file_path.extension() == Some("parquet") {
        let schema = record_batch.schema_ref();
        let writer_properties = writer_properties_for_metadata_and_normal_tables(schema)?;

        // Write the record batch to the object store.
        let mut buffer = Vec::new();
        let mut writer =
            AsyncArrowWriter::try_new(&mut buffer, record_batch.schema(), Some(writer_properties))?;
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

/// Return [`WriterProperties`] optimized for storing relational data in Apache Parquet files
/// managed by Delta Lake.
fn writer_properties_for_metadata_and_normal_tables(schema: &Schema) -> Result<WriterProperties> {
    // Create WriterProperties with values that generally perform better than the defaults.
    let mut writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false);

    // Specify encodings for data type where specific encodings generally are known to perform well.
    for field in schema.fields() {
        let maybe_encoding = match field.data_type() {
            DataType::Timestamp(_, _) => Some(Encoding::DELTA_BINARY_PACKED),
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                Some(Encoding::BYTE_STREAM_SPLIT)
            }
            _ => None,
        };

        if let Some(encoding) = maybe_encoding {
            let path = ColumnPath::from(field.name().as_str());
            writer_properties = writer_properties.set_column_encoding(path, encoding);
        }
    }

    Ok(writer_properties.build())
}

/// Return [`WriterProperties`] optimized for storing compressed segments in Apache Parquet files
/// managed by Delta Lake.
fn writer_properties_for_time_series_table(schema: &Schema) -> Result<WriterProperties> {
    // Specify that the file must be sorted by the tag columns and then by start_time.
    let base_compressed_schema_len = COMPRESSED_SCHEMA.0.fields().len();
    let compressed_schema_len = schema.fields().len();
    let sorting_columns_len = (compressed_schema_len - base_compressed_schema_len) + 1;
    let mut sorting_columns = Vec::with_capacity(sorting_columns_len);

    // Compressed segments have the tag columns at the end of the schema.
    for tag_column_index in base_compressed_schema_len..compressed_schema_len {
        sorting_columns.push(SortingColumn {
            column_idx: tag_column_index as i32,
            descending: false,
            nulls_first: false,
        });
    }

    // Compressed segments store the first timestamp in the second column.
    sorting_columns.push(SortingColumn {
        column_idx: 1,
        descending: false,
        nulls_first: false,
    });

    Ok(WriterProperties::builder()
        .set_data_page_size_limit(16384)
        .set_max_row_group_row_count(Some(65536))
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .set_sorting_columns(Some(sorting_columns))
        .build())
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{Field, Schema};
    use modelardb_test::table;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    // Tests for read_record_batch_from_apache_parquet_file().
    #[tokio::test]
    async fn test_read_record_batch_from_apache_parquet_file() {
        let record_batch = table::compressed_segments_record_batch();
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

        let result = read_record_batch_from_apache_parquet_file(&path, object_store).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Parquet Error: EOF: footer metadata requires 8 bytes, but could only read 0"
        );
    }

    #[tokio::test]
    async fn test_read_record_batch_from_non_existent_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        let path = Path::from("test.parquet");

        let result = read_record_batch_from_apache_parquet_file(&path, object_store).await;

        // The specific error message is OS-dependent, so we only check that it contains the
        // expected prefix and that it contains the OS error code somewhere in the string.
        let actual_error_message = result.unwrap_err().to_string();
        assert!(actual_error_message.starts_with("Parquet Error: External: Object at location"));
        assert!(actual_error_message.contains("os error 2"));
    }

    // Tests for write_record_batch_to_apache_parquet_file().
    #[tokio::test]
    async fn test_write_record_batch_to_apache_parquet_file() {
        let record_batch = table::compressed_segments_record_batch();
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
        let record_batch = table::compressed_segments_record_batch();
        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test.txt"), &record_batch).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Parquet Error: Parquet error: 'test.txt' is not a valid file path for an Apache Parquet file."
        );

        assert!(!temp_dir.path().join("test.txt").exists());
    }

    #[tokio::test]
    async fn test_write_record_batch_to_file_path_without_extension() {
        let record_batch = table::compressed_segments_record_batch();
        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test"), &record_batch).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Parquet Error: Parquet error: 'test' is not a valid file path for an Apache Parquet file."
        );

        assert!(!temp_dir.path().join("test").exists());
    }

    async fn write_record_batch_to_temp_dir(
        file_path: &Path,
        record_batch: &RecordBatch,
    ) -> (TempDir, Result<()>) {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

        let result =
            write_record_batch_to_apache_parquet_file(file_path, record_batch, &object_store).await;

        (temp_dir, result)
    }
}
