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
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute;
use arrow::datatypes::{Field, Schema};
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
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{open_table, DeltaOps, DeltaTable, DeltaTableError};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use tonic::codegen::Bytes;
use tonic::Status;
use url::Url;
use uuid::Uuid;

use crate::arguments::decode_argument;
use crate::schemas::{COMPRESSED_SCHEMA, FIELD_COLUMN};

/// The folder storing compressed data in the data folders.
const COMPRESSED_DATA_FOLDER: &str = "compressed";

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
    /// Create a new [`DeltaLake`] that manages the delta tables in `data_folder_path` if it exists.
    /// Returns a [`DeltaTableError`] if `data_folder_path` is not accessible.
    pub fn try_from_local_path(data_folder_path: &str) -> Result<Self, DeltaTableError> {
        let local_file_system = Arc::new(
            LocalFileSystem::new_with_prefix(data_folder_path)
                .map_err(|error| DeltaTableError::ObjectStore { source: error })?,
        );

        Ok(Self {
            location: data_folder_path.to_string(),
            storage_options: HashMap::new(),
            object_store: local_file_system.clone(),
            maybe_local_file_system: Some(local_file_system),
        })
    }

    /// Create a new [`DeltaLake`] that manages delta tables in the remote object store given by
    /// `connection_info`. Returns [`DeltaTableError`] if `connection_info` could not be parsed.
    pub async fn try_remote_from_connection_info(
        connection_info: &[u8],
    ) -> Result<Self, DeltaTableError> {
        let (object_store_type, offset_data) = decode_argument(connection_info)
            .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

        let (location, storage_options) = match object_store_type {
            "s3" => {
                let (endpoint, bucket_name, access_key_id, secret_access_key, _offset_data) =
                    extract_s3_arguments(offset_data)
                        .await
                        .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

                // TODO: Determine if safe with AWS_S3_ALLOW_UNSAFE_RENAME.
                let storage_options = HashMap::from([
                    ("REGION".to_owned(), "".to_owned()),
                    ("ALLOW_HTTP".to_owned(), "true".to_owned()),
                    ("ENDPOINT".to_owned(), endpoint.to_owned()),
                    ("BUCKET_NAME".to_owned(), bucket_name.to_owned()),
                    ("ACCESS_KEY_ID".to_owned(), access_key_id.to_owned()),
                    ("SECRET_ACCESS_KEY".to_owned(), secret_access_key.to_owned()),
                    ("AWS_S3_ALLOW_UNSAFE_RENAME".to_owned(), "true".to_owned()),
                ]);

                Ok((format!("s3://{bucket_name}"), storage_options))
            }
            // TODO: Needs to be tested.
            "azureblobstorage" => {
                let (account, access_key, container_name, _offset_data) =
                    extract_azure_blob_storage_arguments(offset_data)
                        .await
                        .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

                let storage_options = HashMap::from([
                    ("ACCOUNT_NAME".to_owned(), account.to_owned()),
                    ("ACCESS_KEY".to_owned(), access_key.to_owned()),
                    ("CONTAINER_NAME".to_owned(), container_name.to_owned()),
                ]);

                Ok((format!("az://{container_name}"), storage_options))
            }
            _ => Err(DeltaTableError::Generic(format!(
                "{object_store_type} is currently not supported."
            ))),
        }?;

        let url =
            Url::parse(&location).map_err(|error| DeltaTableError::Generic(error.to_string()))?;
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

    /// Return n [`LocalFileSystem`] to access the root of the Delta Lake if it uses a local data
    /// folder.
    pub fn local_file_system(&self) -> Option<Arc<LocalFileSystem>> {
        self.maybe_local_file_system.clone()
    }

    /// Return a [`DeltaTable`] for manipulating the table with `table_name` in the Delta Lake, or a
    /// [`DeltaTableError`] if a connection cannot be established or the table does not exist.
    pub async fn delta_table(&self, table_name: &str) -> Result<DeltaTable, DeltaTableError> {
        let table_path = self.location_of_compressed_table(table_name);
        deltalake::open_table_with_storage_options(&table_path, self.storage_options.clone()).await
    }

    /// Return a [`DeltaOps`] for manipulating the table with `table_name` in the Delta Lake, or a
    /// [`DeltaTableError`] if a connection cannot be established or the table does not exist.
    pub async fn delta_ops(&self, table_name: &str) -> Result<DeltaOps, DeltaTableError> {
        let table_path = self.location_of_compressed_table(table_name);
        DeltaOps::try_from_uri_with_storage_options(&table_path, self.storage_options.clone()).await
    }

    /// Create a Delta Lake table for a normal table with `table_name` and `schema` if it does not
    /// already exist. If the table could not be created, e.g., because it already exists,
    /// [`DeltaTableError`] is returned.
    pub async fn create_delta_lake_table(
        &self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<DeltaTable, DeltaTableError> {
        self.create_partitioned_delta_lake_table(table_name, schema, &[])
            .await
    }

    /// Create a Delta Lake table for a model table with `table_name` and `schema` if it does not
    /// already exist. Returns [`DeltaTable`] if the table could be created and [`DeltaTableError`]
    /// if it could not.
    pub async fn create_delta_lake_model_table(
        &self,
        table_name: &str,
    ) -> Result<DeltaTable, DeltaTableError> {
        self.create_partitioned_delta_lake_table(
            table_name,
            &COMPRESSED_SCHEMA.0,
            &[FIELD_COLUMN.to_owned()],
        )
        .await
    }

    /// Create a Delta Lake table with `table_name`, `schema`, and `partition_columns` if it does
    /// not already exist. Returns [`DeltaTable`] if the table could be created and
    /// [`DeltaTableError`] if it could not.
    async fn create_partitioned_delta_lake_table(
        &self,
        table_name: &str,
        schema: &Schema,
        partition_columns: &[String],
    ) -> Result<DeltaTable, DeltaTableError> {
        let mut columns: Vec<StructField> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let field: &Field = field;
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
    }

    /// Write the `record_batch` to a Delta Lake table for a normal table with `table_name`.
    /// Returns the new Delta Lake version if the file was written successfully, otherwise returns
    /// [`DeltaTableError`].
    pub async fn write_record_batch_to_table(
        &self,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> Result<i64, DeltaTableError> {
        let table_path = self.location_of_compressed_table(table_name);
        let writer_properties = apache_parquet_writer_properties(None);

        self.write_record_batch_to_delta_table(table_path, record_batch, None, writer_properties)
            .await
    }

    /// Write the `compressed_segments` to a Delta Lake table for a model table with `table_name`.
    /// Returns the new Delta Lake table version if the file was written successfully, otherwise
    /// returns [`DeltaTableError`].
    pub async fn write_compressed_segments_to_model_table(
        &self,
        table_name: &str,
        compressed_segments: RecordBatch,
    ) -> Result<i64, DeltaTableError> {
        let table_path = self.location_of_compressed_table(table_name);

        // Specify that the file must be sorted by univariate_id and then by start_time.
        let sorting_columns = Some(vec![
            SortingColumn::new(0, false, false),
            SortingColumn::new(2, false, false),
        ]);

        let partition_columns = Some(vec![FIELD_COLUMN.to_owned()]);
        let writer_properties = apache_parquet_writer_properties(sorting_columns);

        self.write_record_batch_to_delta_table(
            table_path,
            compressed_segments,
            partition_columns,
            writer_properties,
        )
        .await
    }

    /// Write the rows in `record_batch` to a Delta Lake table at `table_path` using
    /// `writer_properties`. A `partition_column` can optionally be provided to specify that
    /// `record_batch` should be partitioned by that column. Returns the new Delta Lake table
    /// version if the file was written successfully, otherwise returns [`ParquetError`].
    async fn write_record_batch_to_delta_table(
        &self,
        table_path: String,
        record_batch: RecordBatch,
        partition_columns: Option<Vec<String>>,
        writer_properties: WriterProperties,
    ) -> Result<i64, DeltaTableError> {
        let mut delta_table = open_table(&table_path).await?;

        // Write the record batch to the object store.
        let mut writer = RecordBatchWriter::try_new(
            &table_path,
            record_batch.schema(),
            partition_columns,
            None,
        )?
        .with_writer_properties(writer_properties);

        writer.write(record_batch).await?;
        writer.flush_and_commit(&mut delta_table).await
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

// TODO: replace with arguments.rs when dev/delta-metadata is merged.
/// Parse the arguments in `data` and return the arguments to connect to an
/// [`Amazon S3`](object_store::aws::AmazonS3) object store and what is remaining of `data`
/// after parsing. If `data` is missing arguments, [`Status`] is returned.
pub async fn extract_s3_arguments(data: &[u8]) -> Result<(&str, &str, &str, &str, &[u8]), Status> {
    let (endpoint, offset_data) = decode_argument(data)?;
    let (bucket_name, offset_data) = decode_argument(offset_data)?;
    let (access_key_id, offset_data) = decode_argument(offset_data)?;
    let (secret_access_key, offset_data) = decode_argument(offset_data)?;

    Ok((
        endpoint,
        bucket_name,
        access_key_id,
        secret_access_key,
        offset_data,
    ))
}

// TODO: replace with arguments.rs when dev/delta-metadata is merged.
/// Parse the arguments in `data` and return the arguments to connect to an
/// [`Azure Blob Storage`](object_store::azure::MicrosoftAzure)
/// object store and what is remaining of `data` after parsing. If `data` is missing arguments,
/// [`Status`] is returned.
pub async fn extract_azure_blob_storage_arguments(
    data: &[u8],
) -> Result<(&str, &str, &str, &[u8]), Status> {
    let (account, offset_data) = decode_argument(data)?;
    let (access_key, offset_data) = decode_argument(offset_data)?;
    let (container_name, offset_data) = decode_argument(offset_data)?;

    Ok((account, access_key, container_name, offset_data))
}

/// Read all rows from the Apache Parquet file at the location given by `file_path` in
/// `object_store` and return them as a [`RecordBatch`]. If the file could not be read successfully,
/// [`ParquetError`] is returned.
pub async fn read_record_batch_from_apache_parquet_file(
    file_path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<RecordBatch, ParquetError> {
    // Create an object reader for the Apache Parquet file.
    let file_metadata = object_store
        .head(file_path)
        .await
        .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

    let reader = ParquetObjectReader::new(object_store, file_metadata);

    // Stream the data from the Apache Parquet file into a single record batch.
    let record_batches = read_batches_from_apache_parquet_file(reader).await?;

    let schema = record_batches[0].schema();
    compute::concat_batches(&schema, &record_batches)
        .map_err(|error| ParquetError::General(error.to_string()))
}

/// Read each batch of data from the Apache Parquet file given by `reader` and return them as a
/// [`Vec`] of [`RecordBatch`]. If the file could not be read successfully, [`ParquetError`] is
/// returned.
pub async fn read_batches_from_apache_parquet_file<R>(
    reader: R,
) -> Result<Vec<RecordBatch>, ParquetError>
where
    R: AsyncFileReader + Send + Unpin + 'static,
    ParquetRecordBatchStream<R>: StreamExt<Item = Result<RecordBatch, ParquetError>>,
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
/// path to the file if the file was written successfully, otherwise return [`ParquetError`].
pub async fn write_compressed_segments_to_apache_parquet_file(
    compressed_data_folder: &str,
    table_name: &str,
    field_column_index: u16,
    compressed_segments: &RecordBatch,
    object_store: &dyn ObjectStore,
) -> Result<Path, ParquetError> {
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
        ))
    }
}

/// Write the rows in `record_batch` to an Apache Parquet file at the location given by `file_path`
/// in `object_store`. `file_path` must use the extension `.parquet`. `sorting_columns` can be
/// set to control the sorting order of the rows in the written file. Return [`Ok`] if the file
/// was written successfully, otherwise return [`ParquetError`].
pub async fn write_record_batch_to_apache_parquet_file(
    file_path: &Path,
    record_batch: &RecordBatch,
    sorting_columns: Option<Vec<SortingColumn>>,
    object_store: &dyn ObjectStore,
) -> Result<(), ParquetError> {
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
        )))
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
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
    ) -> (TempDir, Result<(), ParquetError>) {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

        let result =
            write_record_batch_to_apache_parquet_file(file_path, record_batch, None, &object_store)
                .await;

        (temp_dir, result)
    }

    // Tests for write_compressed_segments_to_apache_parquet_file().
    #[tokio::test]
    async fn test_write_compressed_segments_to_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
        let compressed_segments = test::compressed_segments_record_batch();

        let result = write_compressed_segments_to_temp_dir(&temp_dir, &compressed_segments).await;
        assert!(result.is_ok());

        // Check that the columns are sorted by univariate_id and then by start_time.
        let file_metadata = object_store.head(&result.unwrap()).await.unwrap();
        let reader = ParquetObjectReader::new(Arc::new(object_store), file_metadata);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();

        let expected_sorting_columns = Some(vec![
            SortingColumn::new(0, false, false),
            SortingColumn::new(2, false, false),
        ]);

        for row_group in builder.metadata().row_groups() {
            assert_eq!(
                row_group.sorting_columns(),
                expected_sorting_columns.as_ref()
            );
        }
    }

    #[tokio::test]
    async fn test_write_compressed_segments_to_unique_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let compressed_segments = test::compressed_segments_record_batch();

        // Write the compressed segments to the same folder twice to ensure the created file name is unique.
        let result_1 = write_compressed_segments_to_temp_dir(&temp_dir, &compressed_segments).await;
        let result_2 = write_compressed_segments_to_temp_dir(&temp_dir, &compressed_segments).await;

        assert!(result_1.is_ok());
        assert!(result_2.is_ok());
        assert_ne!(result_1.unwrap(), result_2.unwrap());
    }

    #[tokio::test]
    async fn test_write_non_compressed_segments_to_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();

        let fields: Vec<Field> = vec![];
        let schema = Schema::new(fields);
        let record_batch = RecordBatch::new_empty(Arc::new(schema));

        let result = write_compressed_segments_to_temp_dir(&temp_dir, &record_batch).await;
        assert!(result.is_err());
    }

    async fn write_compressed_segments_to_temp_dir(
        temp_dir: &TempDir,
        compressed_segments: &RecordBatch,
    ) -> Result<Path, ParquetError> {
        let object_store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

        write_compressed_segments_to_apache_parquet_file(
            "compressed",
            test::MODEL_TABLE_NAME,
            0,
            compressed_segments,
            &object_store,
        )
        .await
    }
}
