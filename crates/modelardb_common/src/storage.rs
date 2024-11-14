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

//! Utility functions to read and write Apache Parquet files to and from an object store.

use std::collections::HashMap;
use std::fs;
use std::path::Path as StdPath;
use std::result::Result as StdResult;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, UInt64Array};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::async_reader::{
    AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStream,
};
use datafusion::parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::format::SortingColumn;
use deltalake::kernel::StructField;
use deltalake::operations::create::CreateBuilder;
use deltalake::{DeltaOps, DeltaTable, DeltaTableError};
use futures::{StreamExt, TryStreamExt};
use modelardb_types::schemas::{
    COMPRESSED_SCHEMA, DISK_COMPRESSED_SCHEMA, FIELD_COLUMN, QUERY_COMPRESSED_SCHEMA,
};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use tonic::codegen::Bytes;
use url::Url;
use uuid::Uuid;

use crate::arguments;
use crate::error::{ModelarDbCommonError, Result};

/// The folder storing compressed data in the data folders.
const COMPRESSED_DATA_FOLDER: &str = "tables";

/// The folder storing metadata in the data folders.
const METADATA_FOLDER: &str = "metadata";

/// Functionality for managing Delta Lake tables in a local folder or an object store.
pub struct DeltaLake {
    /// URL to access the root of the Delta Lake.
    location: String,
    /// Storage options required to access Delta Lake.
    storage_options: HashMap<String, String>,
    /// [`ObjectStore`] to access the root of the Delta Lake.
    object_store: Arc<dyn ObjectStore>,
    /// [`LocalFileSystem`] to access the root of the Delta Lake.
    maybe_local_file_system: Option<Arc<LocalFileSystem>>,
}

impl DeltaLake {
    /// Create a new [`DeltaLake`] that manages the Delta tables in `data_folder_path`. Returns a
    /// [`ModelarDbCommonError`] if `data_folder_path` does not exist and could not be created.
    pub fn try_from_local_path(data_folder_path: &StdPath) -> Result<Self> {
        // Ensure the directories in the path exists as LocalFileSystem otherwise returns an error.
        fs::create_dir_all(data_folder_path)
            .map_err(|error| DeltaTableError::generic(error.to_string()))?;

        // Use with_automatic_cleanup to ensure empty directories are deleted automatically.
        let local_file_system = Arc::new(
            LocalFileSystem::new_with_prefix(data_folder_path)
                .map_err(|error| DeltaTableError::generic(error.to_string()))?
                .with_automatic_cleanup(true),
        );

        let location = data_folder_path
            .to_str()
            .ok_or_else(|| DeltaTableError::generic("Local data folder path is not UTF-8."))?
            .to_owned();

        Ok(Self {
            location,
            storage_options: HashMap::new(),
            object_store: local_file_system.clone(),
            maybe_local_file_system: Some(local_file_system),
        })
    }

    /// Create a new [`DeltaLake`] that manages Delta tables in the remote object store given by
    /// `connection_info`. Returns [`ModelarDbCommonError`] if `connection_info` could not be parsed
    /// or a connection to the specified object store cannot be created.
    pub async fn try_remote_from_connection_info(connection_info: &[u8]) -> Result<Self> {
        let (object_store_type, offset_data) = arguments::decode_argument(connection_info)
            .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

        match object_store_type {
            "s3" => {
                // Register the S3 storage handlers to allow the use of Amazon S3 object stores.
                // This is required at runtime to initialize the S3 storage implementation in the
                // deltalake_aws storage subcrate.
                deltalake::aws::register_handlers(None);

                let (endpoint, bucket_name, access_key_id, secret_access_key, _offset_data) =
                    arguments::extract_s3_arguments(offset_data)
                        .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

                Self::try_from_s3_configuration(
                    endpoint.to_owned(),
                    bucket_name.to_owned(),
                    access_key_id.to_owned(),
                    secret_access_key.to_owned(),
                )
            }
            "azureblobstorage" => {
                let (account, access_key, container_name, _offset_data) =
                    arguments::extract_azure_blob_storage_arguments(offset_data)
                        .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

                Self::try_from_azure_configuration(
                    account.to_owned(),
                    access_key.to_owned(),
                    container_name.to_owned(),
                )
            }
            _ => Err(ModelarDbCommonError::InvalidArgument(format!(
                "{object_store_type} is not supported."
            ))),
        }
    }

    /// Create a new [`DeltaLake`] that manages the Delta tables in an object store with an
    /// S3-compatible API. Returns a [`ModelarDbCommonError`] if a connection to the object store
    /// cannot be made.
    pub fn try_from_s3_configuration(
        endpoint: String,
        bucket_name: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> Result<Self> {
        let location = format!("s3://{bucket_name}");

        // TODO: Determine if it is safe to use AWS_S3_ALLOW_UNSAFE_RENAME.
        let storage_options = HashMap::from([
            ("aws_access_key_id".to_owned(), access_key_id),
            ("aws_secret_access_key".to_owned(), secret_access_key),
            ("aws_endpoint_url".to_owned(), endpoint),
            ("aws_bucket_name".to_owned(), bucket_name),
            ("aws_s3_allow_unsafe_rename".to_owned(), "true".to_owned()),
        ]);

        let url = Url::parse(&location)
            .map_err(|error| ModelarDbCommonError::InvalidArgument(error.to_string()))?;

        // Build the Amazon S3 object store with the given storage options manually to allow http.
        let object_store = storage_options
            .iter()
            .fold(
                AmazonS3Builder::new()
                    .with_url(url.to_string())
                    .with_allow_http(true),
                |builder, (key, value)| match key.parse() {
                    Ok(k) => builder.with_config(k, value),
                    Err(_) => builder,
                },
            )
            .build()?;

        Ok(DeltaLake {
            location,
            storage_options,
            object_store: Arc::new(object_store),
            maybe_local_file_system: None,
        })
    }

    /// Create a new [`DeltaLake`] that manages the Delta tables in an object store with an
    /// Azure-compatible API. Returns a [`ModelarDbCommonError`] if a connection to the object store
    /// cannot be made.
    pub fn try_from_azure_configuration(
        account_name: String,
        access_key: String,
        container_name: String,
    ) -> Result<Self> {
        let location = format!("az://{container_name}");

        // TODO: Needs to be tested.
        let storage_options = HashMap::from([
            ("azure_storage_account_name".to_owned(), account_name),
            ("azure_storage_account_key".to_owned(), access_key),
            ("azure_container_name".to_owned(), container_name),
        ]);
        let url = Url::parse(&location)
            .map_err(|error| ModelarDbCommonError::InvalidArgument(error.to_string()))?;
        let (object_store, _path) = object_store::parse_url_opts(&url, &storage_options)?;

        Ok(DeltaLake {
            location,
            storage_options,
            object_store: Arc::new(object_store),
            maybe_local_file_system: None,
        })
    }

    /// Return an [`ObjectStore`] to access the root of the Delta Lake.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    /// Return a [`LocalFileSystem`] to access the root of the Delta Lake if it uses a local data
    /// folder.
    pub fn local_file_system(&self) -> Option<Arc<LocalFileSystem>> {
        self.maybe_local_file_system.clone()
    }

    /// Return a [`DeltaTable`] for manipulating the table with `table_name` in the Delta Lake, or a
    /// [`ModelarDbCommonError`] if a connection cannot be established or the table does not exist.
    pub async fn delta_table(&self, table_name: &str) -> Result<DeltaTable> {
        let table_path = self.location_of_compressed_table(table_name);
        deltalake::open_table_with_storage_options(&table_path, self.storage_options.clone())
            .await
            .map_err(|error| error.into())
    }

    /// Return a [`DeltaOps`] for manipulating the table with `table_name` in the Delta Lake, or a
    /// [`ModelarDbCommonError`] if a connection cannot be established or the table does not exist.
    pub async fn delta_ops(&self, table_name: &str) -> Result<DeltaOps> {
        let table_path = self.location_of_compressed_table(table_name);
        DeltaOps::try_from_uri_with_storage_options(&table_path, self.storage_options.clone())
            .await
            .map_err(|error| error.into())
    }

    /// Create a Delta Lake table for a normal table with `table_name` and `schema` if it does not
    /// already exist. If the normal table could not be created, e.g., because it already exists,
    /// [`ModelarDbCommonError`] is returned.
    pub async fn create_delta_lake_normal_table(
        &self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<DeltaTable> {
        self.create_partitioned_delta_lake_table(table_name, schema, &[])
            .await
    }

    /// Create a Delta Lake table for a model table with `table_name` and [`DISK_COMPRESSED_SCHEMA`]
    /// if it does not already exist. Returns [`DeltaTable`] if the table could be created and
    /// [`ModelarDbCommonError`] if it could not.
    pub async fn create_delta_lake_model_table(&self, table_name: &str) -> Result<DeltaTable> {
        self.create_partitioned_delta_lake_table(
            table_name,
            &DISK_COMPRESSED_SCHEMA.0,
            &[FIELD_COLUMN.to_owned()],
        )
        .await
    }

    /// Create a Delta Lake table with `table_name`, `schema`, and `partition_columns` if it does
    /// not already exist. Returns [`DeltaTable`] if the table could be created and
    /// [`ModelarDbCommonError`] if it could not.
    async fn create_partitioned_delta_lake_table(
        &self,
        table_name: &str,
        schema: &Schema,
        partition_columns: &[String],
    ) -> Result<DeltaTable> {
        let is_model_table = partition_columns == [FIELD_COLUMN.to_owned()];

        let mut columns: Vec<StructField> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let field: &Field = field;

            // Delta Lakes does not support unsigned integers, thus the Apache Arrow types UInt8,
            // UInt16, UInt32, and UInt64 are converted to Int8, Int16, Int32, and Int64 by
            // try_into(). To ensure values that are not supported by Delta Lake cannot be inserted
            // into the table, a table backed by Delta Lake cannot contain unsigned integers.
            match field.data_type() {
                _ if is_model_table => {} // Exception for model_type_id and field_column.
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    Err(DeltaTableError::SchemaMismatch {
                        msg: "Unsigned integers are not supported.".to_owned(),
                    })?
                }
                _ => {} // All possible cases must be handled.
            }

            let struct_field: StructField = field.try_into()?;
            columns.push(struct_field);
        }

        let location = self.location_of_compressed_table(table_name);

        CreateBuilder::new()
            .with_storage_options(self.storage_options.clone())
            .with_table_name(table_name)
            .with_location(location)
            .with_columns(columns)
            .with_partition_columns(partition_columns)
            .await
            .map_err(|error| error.into())
    }

    /// Drop the Delta Lake table with `table_name` from the Delta Lake by deleting every file
    /// related to the table. The table folder cannot be deleted directly since folders do not exist
    /// in object stores and therefore cannot be operated upon. If the table was dropped
    /// successfully, the paths to the deleted files are returned, otherwise a
    /// [`ModelarDbCommonError`] is returned.
    pub async fn drop_delta_lake_table(&self, table_name: &str) -> Result<Vec<Path>> {
        // List all files in the Delta Lake table folder.
        let table_path = format!("{COMPRESSED_DATA_FOLDER}/{table_name}");
        let file_locations = self
            .object_store
            .list(Some(&Path::from(table_path)))
            .map_ok(|object_meta| object_meta.location)
            .boxed();

        // Delete all files in the Delta Lake table folder using bulk operations if available.
        let deleted_paths = self
            .object_store
            .delete_stream(file_locations)
            .try_collect::<Vec<Path>>()
            .await?;

        Ok(deleted_paths)
    }

    /// Truncate the Delta Lake table with `table_name` by deleting all rows in the table. If the
    /// rows could not be deleted, a [`ModelarDbCommonError`] is returned.
    pub async fn truncate_delta_lake_table(&self, table_name: &str) -> Result<()> {
        let delta_table_ops = self.delta_ops(table_name).await?;
        delta_table_ops.delete().await?;

        Ok(())
    }

    /// Write `record_batches` to a Delta Lake table for a normal table with `table_name`. Returns
    /// an updated [`DeltaTable`] version if the file was written successfully, otherwise returns
    /// [`ModelarDbCommonError`].
    pub async fn write_record_batches_to_normal_table(
        &self,
        table_name: &str,
        record_batches: Vec<RecordBatch>,
    ) -> Result<DeltaTable> {
        let writer_properties = apache_parquet_writer_properties(None);
        self.write_record_batches_to_delta_table(
            table_name,
            record_batches,
            vec![],
            writer_properties,
        )
        .await
    }

    /// Write `compressed_segments` to a Delta Lake table for a model table with `table_name`.
    /// Returns an updated [`DeltaTable`] if the file was written successfully, otherwise returns
    /// [`ModelarDbCommonError`].
    pub async fn write_compressed_segments_to_model_table(
        &self,
        table_name: &str,
        mut compressed_segments: Vec<RecordBatch>,
    ) -> Result<DeltaTable> {
        // Reinterpret univariate_ids from uint64 to int64 if necessary to fix #187 as a stopgap until #197.
        maybe_univariate_ids_uint64_to_int64(&mut compressed_segments);

        // Specify that the file must be sorted by univariate_id and then by start_time.
        let sorting_columns = Some(vec![
            SortingColumn::new(0, false, false),
            SortingColumn::new(2, false, false),
        ]);

        let partition_columns = vec![FIELD_COLUMN.to_owned()];
        let writer_properties = apache_parquet_writer_properties(sorting_columns);

        self.write_record_batches_to_delta_table(
            table_name,
            compressed_segments,
            partition_columns,
            writer_properties,
        )
        .await
    }

    /// Write `record_batches` to a Delta Lake table with `table_name` using `writer_properties`.
    /// `partition_columns` can optionally be provided to specify that `record_batches` should be
    /// partitioned by these columns. Returns an updated [`DeltaTable`] if the file was written
    /// successfully, otherwise returns [`ModelarDbCommonError`].
    async fn write_record_batches_to_delta_table(
        &self,
        table_name: &str,
        record_batches: Vec<RecordBatch>,
        partition_columns: Vec<String>,
        writer_properties: WriterProperties,
    ) -> Result<DeltaTable> {
        let delta_table_ops = self.delta_ops(table_name).await?;
        let write_builder = delta_table_ops.write(record_batches);

        // Write the record batches to the object store.
        write_builder
            .with_partition_columns(partition_columns)
            .with_writer_properties(writer_properties)
            .await
            .map_err(|error| error.into())
    }

    /// Return the location of the compressed model or normal table with `table_name`.
    fn location_of_compressed_table(&self, table_name: &str) -> String {
        format!("{}/{COMPRESSED_DATA_FOLDER}/{table_name}", self.location)
    }

    /// Return the location of the metadata table with `table_name`.
    #[allow(dead_code)]
    fn location_of_metadata_table(&self, table_name: &str) -> String {
        format!("{}/{METADATA_FOLDER}/{table_name}", self.location)
    }
}

/// Reinterpret the bits used for univariate ids in `compressed_segments` to convert the column from
/// [`UInt64Array`] to [`Int64Array`] if the column is currently [`UInt64Array`], as the Delta Lake
/// Protocol does not support unsigned integers. `compressed_segments` is modified in-place as
/// `maybe_univariate_ids_uint64_to_int64()` is designed to be used by
/// `write_compressed_segments_to_model_table()` which owns `compressed_segments`.
fn maybe_univariate_ids_uint64_to_int64(compressed_segments: &mut Vec<RecordBatch>) {
    for record_batch in compressed_segments {
        // Only convert the univariate ids if they are stored as unsigned integers. The univariate
        // ids can be stored as signed integers already if the compressed segments have been saved
        // to disk previously.
        if record_batch.schema().field(0).data_type() == &DataType::UInt64 {
            let mut columns = record_batch.columns().to_vec();
            let univariate_ids = modelardb_types::array!(record_batch, 0, UInt64Array);
            let signed_univariate_ids: Int64Array =
                univariate_ids.unary(|value| i64::from_ne_bytes(value.to_ne_bytes()));
            columns[0] = Arc::new(signed_univariate_ids);

            // unwrap() is safe as columns is constructed to match DISK_COMPRESSED_SCHEMA.
            *record_batch =
                RecordBatch::try_new(DISK_COMPRESSED_SCHEMA.0.clone(), columns).unwrap();
        }
    }
}

/// Reinterpret the bits used for univariate ids in `compressed_segments` to convert the column from
/// [`Int64Array`] to [`UInt64Array`] as the Delta Lake Protocol does not support unsigned integers.
/// Returns a new [`RecordBatch`] with the univariate ids stored in an [`UInt64Array`] as
/// `univariate_ids_int64_to_uint64()` is designed to be used by
/// [`futures::stream::Stream::poll_next()`] and
/// [`datafusion::physical_plan::PhysicalExpr::evaluate()`] and
/// [`datafusion::physical_plan::PhysicalExpr::evaluate()`] borrows `compressed_segments` immutably.
pub fn univariate_ids_int64_to_uint64(compressed_segments: &RecordBatch) -> RecordBatch {
    let mut columns = compressed_segments.columns().to_vec();
    let signed_univariate_ids = modelardb_types::array!(compressed_segments, 0, Int64Array);
    let univariate_ids: UInt64Array =
        signed_univariate_ids.unary(|value| u64::from_ne_bytes(value.to_ne_bytes()));
    columns[0] = Arc::new(univariate_ids);

    // unwrap() is safe as columns is constructed to match QUERY_COMPRESSED_SCHEMA.
    RecordBatch::try_new(QUERY_COMPRESSED_SCHEMA.0.clone(), columns).unwrap()
}

/// Read all rows from the Apache Parquet file at the location given by `file_path` in
/// `object_store` and return them as a [`RecordBatch`]. If the file could not be read successfully,
/// [`ModelarDbCommonError`] is returned.
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
/// [`Vec`] of [`RecordBatch`]. If the file could not be read successfully, [`ParquetError`] is
/// returned.
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

/// Write `compressed_segments` for the column `field_column_index` in the table with `table_name`
/// to an Apache Parquet file with a `UUID` as the file name in `compressed_data_folder`. Return the
/// path to the file if the file was written successfully, otherwise return [`ModelarDbCommonError`].
pub async fn write_compressed_segments_to_apache_parquet_file(
    compressed_data_folder: &str,
    table_name: &str,
    field_column_index: u16,
    compressed_segments: &RecordBatch,
    object_store: &dyn ObjectStore,
) -> Result<Path> {
    if compressed_segments.schema() == COMPRESSED_SCHEMA.0 {
        // Use a UUID for the file name to make it very likely that the name is unique.
        let uuid = Uuid::new_v4();
        let output_file_path = Path::from(format!(
            "{compressed_data_folder}/{table_name}/{field_column_index}/{uuid}.parquet"
        ));

        // Specify that the file must be sorted by univariate_id and then by start_time.
        let sorting_columns = Some(vec![
            SortingColumn::new(0, false, false),
            SortingColumn::new(2, false, false),
        ]);

        write_record_batch_to_apache_parquet_file(
            &output_file_path,
            compressed_segments,
            sorting_columns,
            object_store,
        )
        .await?;

        Ok(output_file_path)
    } else {
        Err(ParquetError::General(
            "The data in the record batch is not compressed segments.".to_string(),
        )
        .into())
    }
}

/// Write the rows in `record_batch` to an Apache Parquet file at the location given by `file_path`
/// in `object_store`. `file_path` must use the extension `.parquet`. `sorting_columns` can be set
/// to control the sorting order of the rows in the written file. Return [`Ok`] if the file was
/// written successfully, otherwise return [`ModelarDbCommonError`].
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
