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

//! Implementation of the type used to interact with local and remote storage through a Delta Lake.

use std::collections::HashMap;
use std::fs;
use std::path::Path as StdPath;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::TableProvider;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::format::SortingColumn;
use deltalake::kernel::StructField;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable, DeltaTableError};
use futures::{StreamExt, TryStreamExt};
use modelardb_common::arguments;
use modelardb_types::schemas::{DISK_COMPRESSED_SCHEMA, FIELD_COLUMN};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use url::Url;

use crate::error::{ModelarDbStorageError, Result};
use crate::{apache_parquet_writer_properties, METADATA_FOLDER, TABLE_FOLDER};

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
    /// [`ModelarDbStorageError`] if `data_folder_path` does not exist and could not be created.
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
    /// `connection_info`. Returns [`ModelarDbStorageError`] if `connection_info` could not be
    /// parsed or a connection to the specified object store could not be created.
    pub fn try_remote_from_connection_info(connection_info: &[u8]) -> Result<Self> {
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
            _ => Err(ModelarDbStorageError::InvalidArgument(format!(
                "{object_store_type} is not supported."
            ))),
        }
    }

    /// Create a new [`DeltaLake`] that manages the Delta tables in an object store with an
    /// S3-compatible API. Returns a [`ModelarDbStorageError`] if a connection to the object store
    /// could not be made.
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
            .map_err(|error| ModelarDbStorageError::InvalidArgument(error.to_string()))?;

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
    /// Azure-compatible API. Returns a [`ModelarDbStorageError`] if a connection to the object
    /// store could not be made.
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
            .map_err(|error| ModelarDbStorageError::InvalidArgument(error.to_string()))?;
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

    /// Return a [`DeltaTable`] for manipulating the metadata table with `table_name` in the
    /// Delta Lake, or a [`ModelarDbStorageError`] if a connection to the delta lake cannot be
    /// established or the table does not exist.
    pub async fn metadata_delta_table(&self, table_name: &str) -> Result<DeltaTable> {
        let table_path = self.location_of_metadata_table(table_name);
        self.delta_table_from_path(&table_path).await
    }

    /// Return a [`DeltaTable`] for manipulating the table with `table_name` in the Delta Lake, or a
    /// [`ModelarDbStorageError`] if a connection to the delta lake cannot be established or the
    /// table does not exist.
    pub async fn delta_table(&self, table_name: &str) -> Result<DeltaTable> {
        let table_path = self.location_of_compressed_table(table_name);
        self.delta_table_from_path(&table_path).await
    }

    /// Return a [`DeltaOps`] for manipulating the metadata table with `table_name` in the Delta
    /// Lake, or a [`ModelarDbStorageError`] if a connection to the delta lake cannot be established
    /// or the table does not exist.
    pub async fn metadata_delta_ops(&self, table_name: &str) -> Result<DeltaOps> {
        let table_path = self.location_of_metadata_table(table_name);
        self.delta_table_from_path(&table_path)
            .await
            .map(Into::into)
    }

    /// Return a [`DeltaOps`] for manipulating the table with `table_name` in the Delta Lake, or a
    /// [`ModelarDbStorageError`] if a connection to the delta lake cannot be established or the
    /// table does not exist.
    pub async fn delta_ops(&self, table_name: &str) -> Result<DeltaOps> {
        let table_path = self.location_of_compressed_table(table_name);
        self.delta_table_from_path(&table_path)
            .await
            .map(Into::into)
    }

    /// Return a [`DeltaTable`] for manipulating the table at `table_path` in the Delta Lake, or a
    /// [`ModelarDbStorageError`] if a connection to the delta lake cannot be established or the
    /// table does not exist.
    async fn delta_table_from_path(&self, table_path: &str) -> Result<DeltaTable> {
        deltalake::open_table_with_storage_options(&table_path, self.storage_options.clone())
            .await
            .map_err(|error| error.into())
    }

    /// Create a Delta Lake table for a metadata table with `table_name` and `schema` if it does not
    /// already exist. If the metadata table could not be created, [`ModelarDbStorageError`] is
    /// returned. An error is not returned if the metadata table already exists.
    pub async fn create_metadata_table(
        &self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<DeltaTable> {
        self.create_table(
            table_name,
            schema,
            &[],
            self.location_of_metadata_table(table_name),
            SaveMode::Ignore,
        )
        .await
    }

    /// Create a Delta Lake table for a normal table with `table_name` and `schema` if it does not
    /// already exist. If the normal table could not be created, e.g., because it already exists,
    /// [`ModelarDbStorageError`] is returned.
    pub async fn create_normal_table(
        &self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<DeltaTable> {
        self.create_table(
            table_name,
            schema,
            &[],
            self.location_of_compressed_table(table_name),
            SaveMode::ErrorIfExists,
        )
        .await
    }

    /// Create a Delta Lake table for a model table with `table_name` and [`DISK_COMPRESSED_SCHEMA`]
    /// if it does not already exist. Returns [`DeltaTable`] if the table could be created and
    /// [`ModelarDbStorageError`] if it could not.
    pub async fn create_model_table(&self, table_name: &str) -> Result<DeltaTable> {
        self.create_table(
            table_name,
            &DISK_COMPRESSED_SCHEMA.0,
            &[FIELD_COLUMN.to_owned()],
            self.location_of_compressed_table(table_name),
            SaveMode::ErrorIfExists,
        )
        .await
    }

    /// Create a Delta Lake table with `table_name`, `schema`, and `partition_columns` if it does
    /// not already exist. Returns [`DeltaTable`] if the table could be created and
    /// [`ModelarDbStorageError`] if it could not.
    async fn create_table(
        &self,
        table_name: &str,
        schema: &Schema,
        partition_columns: &[String],
        location: String,
        save_mode: SaveMode,
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

        CreateBuilder::new()
            .with_storage_options(self.storage_options.clone())
            .with_table_name(table_name)
            .with_location(location)
            .with_columns(columns)
            .with_partition_columns(partition_columns)
            .with_save_mode(save_mode)
            .await
            .map_err(|error| error.into())
    }

    /// Drop the metadata Delta Lake table with `table_name` from the Delta Lake by deleting every
    /// file related to the table. The table folder cannot be deleted directly since folders do not
    /// exist in object stores and therefore cannot be operated upon. If the table was dropped
    /// successfully, the paths to the deleted files are returned, otherwise a
    /// [`ModelarDbStorageError`] is returned.
    pub async fn drop_metadata_table(&self, table_name: &str) -> Result<Vec<Path>> {
        let table_path = format!("{METADATA_FOLDER}/{table_name}");
        self.delete_table_files(&table_path).await
    }

    /// Drop the Delta Lake table with `table_name` from the Delta Lake by deleting every file
    /// related to the table. The table folder cannot be deleted directly since folders do not exist
    /// in object stores and therefore cannot be operated upon. If the table was dropped
    /// successfully, the paths to the deleted files are returned, otherwise a
    /// [`ModelarDbStorageError`] is returned.
    pub async fn drop_table(&self, table_name: &str) -> Result<Vec<Path>> {
        let table_path = format!("{TABLE_FOLDER}/{table_name}");
        self.delete_table_files(&table_path).await
    }

    /// Delete all files in the folder at `table_path` using bulk operations if available. If the
    /// files were deleted successfully, the paths to the deleted files are returned.
    async fn delete_table_files(&self, table_path: &str) -> Result<Vec<Path>> {
        let file_locations = self
            .object_store
            .list(Some(&Path::from(table_path)))
            .map_ok(|object_meta| object_meta.location)
            .boxed();

        let deleted_paths = self
            .object_store
            .delete_stream(file_locations)
            .try_collect::<Vec<Path>>()
            .await?;

        Ok(deleted_paths)
    }

    /// Truncate the Delta Lake table with `table_name` by deleting all rows in the table. If the
    /// rows could not be deleted, a [`ModelarDbStorageError`] is returned.
    pub async fn truncate_table(&self, table_name: &str) -> Result<()> {
        let delta_table_ops = self.delta_ops(table_name).await?;
        delta_table_ops.delete().await?;

        Ok(())
    }

    /// Write `columns` to a metadata Delta Lake table with `table_name`. Returns an updated
    /// [`DeltaTable`] version if the file was written successfully, otherwise returns
    /// [`ModelarDbStorageError`].
    pub async fn write_columns_to_metadata_table(
        &self,
        table_name: &str,
        columns: Vec<ArrayRef>,
    ) -> Result<DeltaTable> {
        let table = self.metadata_delta_table(table_name).await?;

        // TableProvider::schema(&table) is used instead of table.schema() because table.schema()
        // returns the Delta Lake schema instead of the Apache Arrow DataFusion schema.
        let record_batch = RecordBatch::try_new(TableProvider::schema(&table), columns)?;

        self.write_record_batches_to_table(
            table,
            vec![record_batch],
            vec![],
            WriterProperties::new(),
        )
        .await
    }

    /// Write `record_batches` to a Delta Lake table for a normal table with `table_name`. Returns
    /// an updated [`DeltaTable`] version if the file was written successfully, otherwise returns
    /// [`ModelarDbStorageError`].
    pub async fn write_record_batches_to_normal_table(
        &self,
        table_name: &str,
        record_batches: Vec<RecordBatch>,
    ) -> Result<DeltaTable> {
        let writer_properties = apache_parquet_writer_properties(None);
        self.write_record_batches_to_table(
            self.delta_table(table_name).await?,
            record_batches,
            vec![],
            writer_properties,
        )
        .await
    }

    /// Write `compressed_segments` to a Delta Lake table for a model table with `table_name`.
    /// Returns an updated [`DeltaTable`] if the file was written successfully, otherwise returns
    /// [`ModelarDbStorageError`].
    pub async fn write_compressed_segments_to_model_table(
        &self,
        table_name: &str,
        mut compressed_segments: Vec<RecordBatch>,
    ) -> Result<DeltaTable> {
        // Specify that the file must be sorted by univariate_id and then by start_time.
        let sorting_columns = Some(vec![
            SortingColumn::new(0, false, false),
            SortingColumn::new(2, false, false),
        ]);

        let partition_columns = vec![FIELD_COLUMN.to_owned()];
        let writer_properties = apache_parquet_writer_properties(sorting_columns);

        self.write_record_batches_to_table(
            self.delta_table(table_name).await?,
            compressed_segments,
            partition_columns,
            writer_properties,
        )
        .await
    }

    /// Write `record_batches` to the Delta Lake table `delta_table` using `writer_properties`.
    /// `partition_columns` can optionally be provided to specify that `record_batches` should be
    /// partitioned by these columns. Returns an updated [`DeltaTable`] if the file was written
    /// successfully, otherwise returns [`ModelarDbStorageError`].
    async fn write_record_batches_to_table(
        &self,
        delta_table: DeltaTable,
        record_batches: Vec<RecordBatch>,
        partition_columns: Vec<String>,
        writer_properties: WriterProperties,
    ) -> Result<DeltaTable> {
        let delta_table_ops: DeltaOps = delta_table.into();
        let write_builder = delta_table_ops.write(record_batches);

        // Write the record batches to the Delta table.
        write_builder
            .with_partition_columns(partition_columns)
            .with_writer_properties(writer_properties)
            .await
            .map_err(|error| error.into())
    }

    /// Return the location of the compressed model or normal table with `table_name`.
    fn location_of_compressed_table(&self, table_name: &str) -> String {
        format!("{}/{TABLE_FOLDER}/{table_name}", self.location)
    }

    /// Return the location of the metadata table with `table_name`.
    fn location_of_metadata_table(&self, table_name: &str) -> String {
        format!("{}/{METADATA_FOLDER}/{table_name}", self.location)
    }
}
