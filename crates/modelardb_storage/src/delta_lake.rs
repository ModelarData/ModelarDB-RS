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
use deltalake::delta_datafusion::DeltaDataChecker;
use deltalake::kernel::{Action, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::transaction::{CommitBuilder, CommitProperties};
use deltalake::operations::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{DeltaOps, DeltaTable, DeltaTableError};
use futures::{StreamExt, TryStreamExt};
use modelardb_common::arguments;
use modelardb_types::schemas::{COMPRESSED_SCHEMA, FIELD_COLUMN};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use url::Url;
use uuid::Uuid;

use crate::error::{ModelarDbStorageError, Result};
use crate::metadata::model_table_metadata::ModelTableMetadata;
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

    /// Create a Delta Lake table for a model table with `model_table_metadata` if it does not
    /// already exist. Returns [`DeltaTable`] if the table could be created and
    /// [`ModelarDbStorageError`] if it could not.
    pub async fn create_model_table(
        &self,
        model_table_metadata: &ModelTableMetadata,
    ) -> Result<DeltaTable> {
        self.create_table(
            &model_table_metadata.name,
            &model_table_metadata.compressed_schema,
            &[FIELD_COLUMN.to_owned()],
            self.location_of_compressed_table(&model_table_metadata.name),
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
                DataType::UInt16 if is_model_table => {} // Exception for field_column.
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
        let delta_table = self.metadata_delta_table(table_name).await?;

        // TableProvider::schema(&table) is used instead of table.schema() because table.schema()
        // returns the Delta Lake schema instead of the Apache DataFusion schema.
        let record_batch = RecordBatch::try_new(TableProvider::schema(&delta_table), columns)?;

        self.write_record_batches_to_table(
            delta_table,
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
        let delta_table = self.delta_table(table_name).await?;
        self.write_record_batches_to_table(delta_table, record_batches, vec![], writer_properties)
            .await
    }

    /// Write `compressed_segments` to a Delta Lake table for a model table with `table_name`.
    /// Returns an updated [`DeltaTable`] if the file was written successfully, otherwise returns
    /// [`ModelarDbStorageError`].
    pub async fn write_compressed_segments_to_model_table(
        &self,
        table_name: &str,
        compressed_segments: Vec<RecordBatch>,
    ) -> Result<DeltaTable> {
        // Specify that the file must be sorted by the tag columns and then by start_time.
        let mut sorting_columns = Vec::new();
        let base_compressed_schema_len = COMPRESSED_SCHEMA.0.fields().len();
        let compressed_schema_len = compressed_segments[0].schema().fields().len();

        // Compressed segments have the tag columns at the end of the schema.
        for tag_column_index in base_compressed_schema_len..compressed_schema_len {
            sorting_columns.push(SortingColumn::new(tag_column_index as i32, false, false));
        }

        sorting_columns.push(SortingColumn::new(1, false, false));

        let partition_columns = vec![FIELD_COLUMN.to_owned()];
        let writer_properties = apache_parquet_writer_properties(Some(sorting_columns));

        let delta_table = self.delta_table(table_name).await?;
        self.write_record_batches_to_table(
            delta_table,
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
        let mut delta_lake_writer =
            DeltaTableWriter::try_new(delta_table, partition_columns, writer_properties)?;

        let result = delta_lake_writer.write(&record_batches).await;
        if result.is_ok() {
            delta_lake_writer.commit().await
        } else {
            delta_lake_writer.rollback().await
        }
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

/// Functionality for transactionally writing [`RecordBatches`](RecordBatch) to a Delta Table stored
/// in a local folder or an object store.
pub struct DeltaTableWriter {
    /// Delta table that all of the record batches will be written to.
    delta_table: DeltaTable,
    /// Checker that ensures all of the record batches match the table.
    delta_data_checker: DeltaDataChecker,
    /// Write operation that will be committed to the delta table.
    delta_operation: DeltaOperation,
    /// Unique identifier for this write operation to the delta table.
    operation_id: Uuid,
    /// Writes record batches to the delta table as Apache Parquet files.
    delta_writer: DeltaWriter,
}

impl DeltaTableWriter {
    /// Create a new [`DeltaTableWriter`]. Returns a [`ModelarDbStorageError`] if the state of the
    /// delta table cannot be loaded from `delta_table`.
    pub fn try_new(
        delta_table: DeltaTable,
        partition_columns: Vec<String>,
        writer_properties: WriterProperties,
    ) -> Result<Self> {
        // Checker for if record batches match the table’s invariants, constraints, and nullability.
        let snapshot = delta_table.snapshot()?;
        let delta_data_checker = DeltaDataChecker::new(snapshot);

        // Operation that will be committed.
        let delta_operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: if partition_columns.is_empty() {
                None
            } else {
                Some(partition_columns.clone())
            },
            predicate: None,
        };

        // A UUID version 4 as the operation id to is used to match the Operation trait in the
        // deltalake crate as it is pub(trait) and thus cannot be used directly in DeltaTableWriter.
       let operation_id = Uuid::new_v4();

        // Writer that will write the record batches.
        let object_store = delta_table.log_store().object_store(Some(operation_id));
        let table_schema: Arc<Schema> = TableProvider::schema(&delta_table);
        let num_indexed_cols = table_schema.fields.len() as i32;
        let writer_config = WriterConfig::new(
            table_schema,
            partition_columns,
            Some(writer_properties),
            None,
            None,
            num_indexed_cols,
            None,
        );
        let delta_writer = DeltaWriter::new(object_store, writer_config);

        Ok(Self {
            delta_table,
            delta_data_checker,
            delta_operation,
            operation_id,
            delta_writer,
        })
    }

    /// Write `record_batches` to the delta table. Returns a [`ModelarDbStorageError`] if one of the
    /// [`RecordBatches`](RecordBatch) does not match the schema of the delta table or if the
    /// writing fails.
    pub async fn write(&mut self, record_batches: &[RecordBatch]) -> Result<()> {
        for batch in record_batches {
            self.delta_data_checker.check_batch(batch).await?;
            self.delta_writer.write(batch).await?;
        }
        Ok(())
    }

    /// Consume the [`DeltaLakeWriter`], finish the writing, and commit the files that has been
    /// written to the log. Returns a [`ModelarDbStorageError`] if an error occurs when finishing
    /// the writing, committing the files that has been written, or updating the [`DeltaTable`].
    pub async fn commit(mut self) -> Result<DeltaTable> {
        let actions = self
            .delta_writer
            .close()
            .await?
            .into_iter()
            .map(Action::Add)
            .collect();

        let commit_properties = CommitProperties::default();
        let table_data = self.delta_table.snapshot()?;
        let log_store = self.delta_table.log_store();
        let _finalized_commit = CommitBuilder::from(commit_properties)
            .with_actions(actions)
            .with_operation_id(self.operation_id)
            .build(Some(table_data), log_store, self.delta_operation)
            .await?;

        // DeltaTable::new_with_state() is pub(crate).
        self.delta_table.load().await?;
        Ok(self.delta_table)
    }

    /// Consume the [`DeltaLakeWriter`], abort the writing, and delete all of the files that has
    /// already been written. Returns a [`ModelarDbStorageError`] if an error occurs when aborting
    /// the writing or deleting the files that has already been written. Rollback is not called
    /// automatically in drop() is not async and async_drop() is not yet a stable API.
    pub async fn rollback(self) -> Result<DeltaTable> {
        let object_store = self.delta_table.object_store();
        for add in self.delta_writer.close().await? {
            let path: Path = Path::from(add.path);
            object_store.delete(&path).await?;
        }
        Ok(self.delta_table)
    }
}
