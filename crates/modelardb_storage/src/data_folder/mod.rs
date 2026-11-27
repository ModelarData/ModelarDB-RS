/* Copyright 2025 The ModelarDB Contributors
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

pub mod cluster;
pub mod delta_table_writer;

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU64;
use std::path::Path as StdPath;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryViewArray, BooleanArray, Float32Array, Int32Array, RecordBatch,
    StringViewArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::TimeDelta;
use dashmap::DashMap;
use datafusion::catalog::TableProvider;
use datafusion::common::{TableReference, ToDFSchema};
use datafusion::datasource::sink::DataSink;
use datafusion::logical_expr::{Expr, lit};
use datafusion::prelude::{SessionContext, col};
use datafusion_proto::bytes::Serializeable;
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::kernel::{Action, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{DeltaTable, DeltaTableError, TableProperty};
use futures::{StreamExt, TryStreamExt};
use modelardb_types::functions::{try_convert_bytes_to_schema, try_convert_schema_to_bytes};
use modelardb_types::types::{
    CloudCredentials, ColumnMetadata, ErrorBound, GeneratedColumn, MAX_RETENTION_PERIOD_IN_SECONDS,
    TableMetadata,
};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use url::Url;

use crate::data_folder::delta_table_writer::DeltaTableWriter;
use crate::error::{ModelarDbStorageError, Result};
use crate::query::modelardb_table::ModelarDbTable;
use crate::{METADATA_FOLDER, TABLE_FOLDER, sql_and_concat};

/// Functionality for managing Delta Lake tables in a local folder or an object store. A single
/// instance should be shared through an [`Arc`], so the cache and session context are only
/// created once.
pub struct DataFolder {
    /// URL to access the root of the Delta Lake.
    location: String,
    /// Storage options required to access Delta Lake.
    storage_options: HashMap<String, String>,
    /// [`ObjectStore`] to access the root of the Delta Lake.
    object_store: Arc<dyn ObjectStore>,
    /// Cache of Delta tables to avoid opening the same table multiple times.
    delta_table_cache: DashMap<String, DeltaTable>,
    /// Session context used to query the tables using Apache DataFusion.
    session_context: SessionContext,
}

impl DataFolder {
    /// Create a new [`DataFolder`] that manages the Delta tables at `local_url`. If `local_url` has
    /// the schema `file` or no schema, the Delta tables are managed in a local data folder. If
    /// `local_url` has the schema `memory`, the Delta tables are managed in memory. Return
    /// [`ModelarDbStorageError`] if `local_url` cannot be parsed or the metadata tables cannot be
    /// created.
    pub async fn open_local_url(local_url: &str) -> Result<Self> {
        match local_url.split_once("://") {
            None => Self::open_local(StdPath::new(local_url)).await,
            Some(("file", local_path)) => Self::open_local(StdPath::new(local_path)).await,
            Some(("memory", _)) => Self::open_memory().await,
            _ => Err(ModelarDbStorageError::InvalidArgument(format!(
                "{local_url} is not a valid local URL."
            ))),
        }
    }

    /// Create a new [`DataFolder`] that manages the Delta tables in memory.
    pub async fn open_memory() -> Result<Self> {
        Self::try_new(
            "memory:///modelardb".to_owned(),
            HashMap::new(),
            Arc::new(InMemory::new()),
        )
        .await
    }

    /// Create a new [`DataFolder`] that manages the Delta tables in `data_folder_path`. Returns a
    /// [`ModelarDbStorageError`] if `data_folder_path` does not exist and could not be created or
    /// the metadata tables cannot be created.
    pub async fn open_local(data_folder_path: &StdPath) -> Result<Self> {
        // Ensure the directories in the path exists as LocalFileSystem otherwise returns an error.
        std::fs::create_dir_all(data_folder_path)
            .map_err(|error| DeltaTableError::generic(error.to_string()))?;

        // Use with_automatic_cleanup to ensure empty directories are deleted automatically.
        let object_store = LocalFileSystem::new_with_prefix(data_folder_path)
            .map_err(|error| DeltaTableError::generic(error.to_string()))?
            .with_automatic_cleanup(true);

        let location = data_folder_path
            .to_str()
            .ok_or_else(|| DeltaTableError::generic("Local data folder path is not UTF-8."))?
            .to_owned();

        Self::try_new(location, HashMap::new(), Arc::new(object_store)).await
    }

    /// Create a new [`DataFolder`] that manages Delta tables in the remote object store given by
    /// `remote_url`. If `remote_url` has the schema `s3`, the object store is an Amazon S3 bucket.
    /// If `remote_url` has the schema `azureblobstorage`, the object store is an Azure Blob Storage
    /// container. Returns [`ModelarDbStorageError`] if required credentials are missing or a
    /// connection to the specified object store could not be created.
    pub async fn open_remote_url(remote_url: &str, credentials: &CloudCredentials) -> Result<Self> {
        match remote_url.split_once("://") {
            Some(("s3", bucket_name)) => {
                let endpoint = credentials.aws_endpoint.clone().ok_or_else(|| {
                    ModelarDbStorageError::InvalidArgument(
                        "Amazon S3 requires --aws-endpoint or AWS_ENDPOINT.".to_owned(),
                    )
                })?;
                let access_key_id = credentials.aws_access_key_id.clone().ok_or_else(|| {
                    ModelarDbStorageError::InvalidArgument(
                        "Amazon S3 requires --aws-access-key-id or AWS_ACCESS_KEY_ID.".to_owned(),
                    )
                })?;
                let secret_access_key = credentials.aws_secret_access_key.clone().ok_or_else(|| {
                    ModelarDbStorageError::InvalidArgument(
                        "Amazon S3 requires --aws-secret-access-key or AWS_SECRET_ACCESS_KEY.".to_owned(),
                    )
                })?;

                // Register the S3 storage handlers to allow the use of Amazon S3 object stores. This is
                // required at runtime to initialize the S3 storage implementation in the deltalake_aws
                // storage subcrate. It is safe to call this function multiple times as the handlers are
                // stored in a DashMap, thus, the handlers are simply overwritten with the same each time.
                deltalake::aws::register_handlers(None);

                Self::open_s3(endpoint, bucket_name.to_owned(), access_key_id, secret_access_key).await
            }
            Some(("azureblobstorage", container_name)) => {
                let account_name = credentials.azure_storage_account_name.clone().ok_or_else(|| {
                    ModelarDbStorageError::InvalidArgument(
                        "Azure Blob Storage requires --azure-storage-account-name or AZURE_STORAGE_ACCOUNT_NAME.".to_owned(),
                    )
                })?;
                let access_key = credentials.azure_storage_access_key.clone().ok_or_else(|| {
                    ModelarDbStorageError::InvalidArgument(
                        "Azure Blob Storage requires --azure-storage-access-key or AZURE_STORAGE_ACCESS_KEY.".to_owned(),
                    )
                })?;

                Self::open_azure(account_name, access_key, container_name.to_owned(), false).await
            }
            _ => Err(ModelarDbStorageError::InvalidArgument(
                "Remote data folder URL must be s3://bucket-name or azureblobstorage://container-name."
                    .to_owned(),
            )),
        }
    }

    /// Create a new [`DataFolder`] that manages the Delta tables in an object store with an
    /// S3-compatible API. Returns a [`ModelarDbStorageError`] if a connection to the object store
    /// could not be made or the metadata tables cannot be created.
    pub async fn open_s3(
        endpoint: String,
        bucket_name: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> Result<Self> {
        let location = format!("s3://{bucket_name}");

        let storage_options = HashMap::from([
            ("aws_access_key_id".to_owned(), access_key_id),
            ("aws_secret_access_key".to_owned(), secret_access_key),
            ("aws_endpoint_url".to_owned(), endpoint),
            ("aws_bucket_name".to_owned(), bucket_name),
            ("aws_allow_http".to_owned(), "true".to_owned()),
        ]);

        let url = Url::parse(&location)
            .map_err(|error| ModelarDbStorageError::InvalidArgument(error.to_string()))?;

        let (object_store, _path) = object_store::parse_url_opts(&url, &storage_options)?;

        Self::try_new(location, storage_options, Arc::new(object_store)).await
    }

    /// Create a new [`DataFolder`] that manages the Delta tables in an object store with an
    /// Azure-compatible API. Returns a [`ModelarDbStorageError`] if a connection to the object
    /// store could not be made or the metadata tables cannot be created.
    pub async fn open_azure(
        account_name: String,
        access_key: String,
        container_name: String,
        use_emulator: bool,
    ) -> Result<Self> {
        let location = format!("az://{container_name}");
        let use_emulator = use_emulator.to_string();

        // TODO: Needs to be tested.
        let storage_options = HashMap::from([
            ("azure_storage_account_name".to_owned(), account_name),
            ("azure_storage_account_key".to_owned(), access_key),
            ("azure_container_name".to_owned(), container_name),
            ("azure_storage_use_emulator".to_owned(), use_emulator),
        ]);

        let url = Url::parse(&location)
            .map_err(|error| ModelarDbStorageError::InvalidArgument(error.to_string()))?;

        let (object_store, _path) = object_store::parse_url_opts(&url, &storage_options)?;

        Self::try_new(location, storage_options, Arc::new(object_store)).await
    }

    /// Create a new [`DataFolder`] with the given `location`, `storage_options`, and `object_store`,
    /// create the metadata tables, and return the [`DataFolder`]. Returns [`ModelarDbStorageError`]
    /// if the metadata tables cannot be created.
    async fn try_new(
        location: String,
        storage_options: HashMap<String, String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let data_folder = Self {
            location,
            storage_options,
            object_store,
            delta_table_cache: DashMap::new(),
            session_context: crate::create_session_context(),
        };

        data_folder.create_and_register_metadata_tables().await?;

        Ok(data_folder)
    }

    /// If they do not already exist, create the tables in the Delta Lake for table metadata and
    /// register them with the Apache DataFusion session context.
    /// * The `table_metadata` table contains the name and query schema of each table.
    /// * The `column_metadata` table contains the name, index, error bound, and generated column
    ///   expression of each column that has an error bound or is generated. Columns without a row
    ///   are stored losslessly.
    ///
    /// If the tables exist or were created, return [`Ok`], otherwise return
    /// [`ModelarDbStorageError`].
    async fn create_and_register_metadata_tables(&self) -> Result<()> {
        // Create and register the table_metadata table if it does not exist.
        self.create_and_register_metadata_table(
            "table_metadata",
            &Schema::new(vec![
                Field::new("table_name", DataType::Utf8View, false),
                Field::new("query_schema", DataType::Binary, false),
            ]),
        )
        .await?;

        // Create and register the column_metadata table if it does not exist. error_bound_value and
        // error_bound_is_relative are NULL for generated columns, and generated_column_expr is NULL
        // for stored columns.
        self.create_and_register_metadata_table(
            "column_metadata",
            &Schema::new(vec![
                Field::new("table_name", DataType::Utf8View, false),
                Field::new("column_name", DataType::Utf8View, false),
                Field::new("column_index", DataType::Int32, false),
                Field::new("error_bound_value", DataType::Float32, true),
                Field::new("error_bound_is_relative", DataType::Boolean, true),
                Field::new("generated_column_expr", DataType::Binary, true),
            ]),
        )
        .await?;

        Ok(())
    }

    /// Create a Delta Lake table for a metadata table with `table_name` and `schema` and register
    /// it in the [`SessionContext`] in the `metadata` schema. If the table already exists, it is
    /// reused. Return [`ModelarDbStorageError`] if the table cannot be created or cannot be
    /// registered.
    async fn create_and_register_metadata_table(
        &self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<()> {
        let delta_table = self
            .create_delta_lake_table(
                table_name,
                schema,
                &[],
                self.location_of_metadata_table(table_name),
                SaveMode::Ignore,
            )
            .await?;

        // Metadata tables are stored losslessly and have no generated columns.
        let table_metadata = TableMetadata::try_new(
            table_name.to_owned(),
            Arc::new(schema.clone()),
            vec![ColumnMetadata::Stored(ErrorBound::Lossless); schema.fields().len()],
        )?;

        let table_reference = TableReference::partial("metadata", table_name);
        let metadata_table =
            Arc::new(ModelarDbTable::try_new(delta_table, Arc::new(table_metadata), None).await?);

        self.session_context
            .register_table(table_reference, metadata_table)?;

        Ok(())
    }

    /// Return the session context used to query the tables using Apache DataFusion.
    pub fn session_context(&self) -> &SessionContext {
        &self.session_context
    }

    /// Return the location of the Delta Lake. This is `memory:///modelardb` if the Delta Lake is
    /// in memory, the local path if the Delta Lake is stored on disk, `az://container-name`
    /// if the Delta Lake is stored in Azure Blob Storage, or `s3://bucket-name` if the Delta Lake
    /// is stored in Amazon S3.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Return an [`ObjectStore`] to access the root of the Delta Lake.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    /// Register all tables in `self` with its [`SessionContext`]. `data_sink` is set as the
    /// [`DataSink`] for all the tables. If the tables could not be registered,
    /// [`ModelarDbStorageError`] is returned.
    pub async fn register_tables(&self, data_sink: Arc<dyn DataSink>) -> Result<()> {
        for table_metadata in self.table_metadata().await? {
            let delta_table = self.delta_table(&table_metadata.name).await?;

            crate::register_table(
                &self.session_context,
                delta_table,
                table_metadata,
                data_sink.clone(),
            )
            .await?;
        }

        Ok(())
    }

    /// Create a Delta Lake table for the table with `table_metadata` and save the metadata to the
    /// `table_metadata` and `column_metadata` tables. If the table already exists or the metadata
    /// could not be saved, return [`ModelarDbStorageError`], otherwise return the created
    /// [`DeltaTable`].
    pub async fn create_table(&self, table_metadata: &TableMetadata) -> Result<DeltaTable> {
        let delta_table = self
            .create_delta_lake_table(
                &table_metadata.name,
                &table_metadata.stored_schema,
                &[],
                self.location_of_table(&table_metadata.name),
                SaveMode::ErrorIfExists,
            )
            .await?;

        self.save_table_metadata(table_metadata).await?;

        Ok(delta_table)
    }

    /// Create a Delta Lake table with `table_name`, `schema`, and `partition_columns` if it does
    /// not already exist. Returns [`DeltaTable`] if the table could be created and
    /// [`ModelarDbStorageError`] if it could not.
    async fn create_delta_lake_table(
        &self,
        table_name: &str,
        schema: &Schema,
        partition_columns: &[String],
        location: String,
        save_mode: SaveMode,
    ) -> Result<DeltaTable> {
        let mut columns: Vec<StructField> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let field: &Field = field;

            // Delta Lake does not support unsigned integers. Thus tables containing the Apache
            // Arrow types UInt8, UInt16, UInt32, and UInt64 must currently be rejected.
            match field.data_type() {
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    Err(DeltaTableError::SchemaMismatch {
                        msg: "Unsigned integers are not supported.".to_owned(),
                    })?
                }
                _ => {} // All possible cases must be handled.
            }

            let struct_field: StructField = field.try_into_kernel()?;
            columns.push(struct_field);
        }

        // WriteSerializable allows blind appends to commit alongside commits that rewrite or remove
        // files, e.g., when OPTIMIZE or a transfer runs while data is being written to the table.
        let delta_table = CreateBuilder::new()
            .with_storage_options(self.storage_options.clone())
            .with_table_name(table_name)
            .with_location(location.clone())
            .with_columns(columns)
            .with_partition_columns(partition_columns)
            .with_configuration_property(TableProperty::IsolationLevel, Some("WriteSerializable"))
            .with_save_mode(save_mode)
            .await?;

        // If the table was created successfully, add it to the cache.
        self.delta_table_cache.insert(location, delta_table.clone());

        Ok(delta_table)
    }

    /// Save the metadata of the created table to the Delta Lake. This includes adding a row to the
    /// `table_metadata` table and adding a row to the `column_metadata` table for each column that
    /// has an error bound or is generated. If the metadata was saved, return [`Ok`], otherwise
    /// return [`ModelarDbStorageError`].
    async fn save_table_metadata(&self, table_metadata: &TableMetadata) -> Result<()> {
        // Convert the query schema to bytes, so it can be saved in the Delta Lake.
        let query_schema_bytes = try_convert_schema_to_bytes(&table_metadata.query_schema)?;

        self.write_columns_to_metadata_table(
            "table_metadata",
            vec![
                Arc::new(StringViewArray::from(vec![table_metadata.name.clone()])),
                Arc::new(BinaryViewArray::from_iter_values(vec![&query_schema_bytes])),
            ],
        )
        .await?;

        // Only columns with an error bound or a generated column expression are saved, so all of
        // their rows are collected and written in a single commit.
        let mut column_names = vec![];
        let mut column_indices = vec![];
        let mut error_bound_values = vec![];
        let mut error_bound_is_relatives = vec![];
        let mut generated_column_exprs = vec![];

        for (column_index, (field, column_metadata)) in table_metadata
            .query_schema
            .fields()
            .iter()
            .zip(&table_metadata.column_metadata)
            .enumerate()
        {
            let (error_bound_value, error_bound_is_relative, generated_column_expr) =
                match column_metadata {
                    ColumnMetadata::Stored(ErrorBound::Absolute(value)) => {
                        (Some(*value), Some(false), None)
                    }
                    ColumnMetadata::Stored(ErrorBound::Relative(percentage)) => {
                        (Some(*percentage), Some(true), None)
                    }
                    ColumnMetadata::Stored(ErrorBound::Lossless) => continue,
                    ColumnMetadata::Generated(generated_column) => {
                        (None, None, Some(generated_column.expr.to_bytes()?.to_vec()))
                    }
                };

            column_names.push(field.name().as_str());
            column_indices.push(column_index as i32);
            error_bound_values.push(error_bound_value);
            error_bound_is_relatives.push(error_bound_is_relative);
            generated_column_exprs.push(generated_column_expr);
        }

        if !column_names.is_empty() {
            let table_names = vec![table_metadata.name.as_str(); column_names.len()];

            self.write_columns_to_metadata_table(
                "column_metadata",
                vec![
                    Arc::new(StringViewArray::from(table_names)),
                    Arc::new(StringViewArray::from(column_names)),
                    Arc::new(Int32Array::from(column_indices)),
                    Arc::new(Float32Array::from(error_bound_values)),
                    Arc::new(BooleanArray::from(error_bound_is_relatives)),
                    Arc::new(BinaryViewArray::from_iter(generated_column_exprs)),
                ],
            )
            .await?;
        }

        Ok(())
    }

    /// Drop the Delta Lake table with `table_name` from the Delta Lake by deleting the table
    /// metadata and deleting every file related to the table. The table folder cannot be deleted
    /// directly since folders do not exist in object stores and therefore cannot be operated upon.
    /// If the table was dropped successfully, the paths to the deleted files are returned,
    /// otherwise a [`ModelarDbStorageError`] is returned.
    pub async fn drop_table(&self, table_name: &str) -> Result<Vec<Path>> {
        self.delete_table_metadata(table_name).await?;

        let table_path = format!("{TABLE_FOLDER}/{table_name}");
        let deleted_paths = self.delete_table_files(&table_path).await?;

        self.remove_delta_table_from_cache(table_name);

        Ok(deleted_paths)
    }

    /// Delete the metadata of the table with `table_name` from the `table_metadata` and
    /// `column_metadata` tables. If the table does not exist or the metadata could not be deleted,
    /// [`ModelarDbStorageError`] is returned.
    async fn delete_table_metadata(&self, table_name: &str) -> Result<()> {
        if !self.table_exists(table_name).await? {
            return Err(ModelarDbStorageError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            )));
        }

        for metadata_table_name in ["table_metadata", "column_metadata"] {
            self.metadata_delta_table(metadata_table_name)
                .await?
                .delete()
                .with_predicate(col("table_name").eq(lit(table_name)))
                .await?;
        }

        Ok(())
    }

    /// Delete all files in the folder at `table_path` using bulk operations if available. If the
    /// files were deleted successfully, the paths to the deleted files are returned.
    async fn delete_table_files(&self, table_path: &str) -> Result<Vec<Path>> {
        let file_locations = self
            .object_store
            .list(Some(&Path::from(table_path)))
            .map_ok(|object_meta| object_meta.location)
            .boxed();

        self.object_store
            .delete_stream(file_locations)
            .try_collect::<Vec<Path>>()
            .await
            .map_err(|error| error.into())
    }

    /// Remove the [`DeltaTable`] for the table with `table_name` from the cache so the table is
    /// opened from the Delta Lake again the next time it is used.
    pub fn remove_delta_table_from_cache(&self, table_name: &str) {
        self.delta_table_cache
            .remove(&self.location_of_table(table_name));
    }

    /// Truncate the Delta Lake table with `table_name` by deleting all rows in the table. If the
    /// rows could not be deleted, a [`ModelarDbStorageError`] is returned.
    pub async fn truncate_table(&self, table_name: &str) -> Result<()> {
        let delta_table = self.delta_table(table_name).await?;
        delta_table.delete().await?;

        Ok(())
    }

    /// Vacuum the Delta Lake table with `table_name` by deleting stale files that are older than
    /// `maybe_retention_period_in_seconds` seconds. If a retention period is not given, the
    /// default retention period of 7 days is used. If the retention period is larger than
    /// [`MAX_RETENTION_PERIOD_IN_SECONDS`] seconds or the files could not be deleted, a
    /// [`ModelarDbStorageError`] is returned.
    pub async fn vacuum_table(
        &self,
        table_name: &str,
        maybe_retention_period_in_seconds: Option<u64>,
    ) -> Result<()> {
        let delta_table = self.delta_table(table_name).await?;

        let retention_period_in_seconds =
            maybe_retention_period_in_seconds.unwrap_or(60 * 60 * 24 * 7);

        let retention_period = TimeDelta::new(retention_period_in_seconds as i64, 0).ok_or(
            ModelarDbStorageError::InvalidArgument(format!(
                "Retention period cannot be more than {MAX_RETENTION_PERIOD_IN_SECONDS} seconds."
            )),
        )?;

        delta_table
            .vacuum()
            .with_retention_period(retention_period)
            .with_enforce_retention_duration(false)
            .await?;

        Ok(())
    }

    /// Optimize the Delta Lake table with `table_name` by merging its many small files into
    /// fewer larger files of approximately `maybe_target_size_in_bytes` bytes. If a target size is
    /// not given, a default target size of 64 MiB is used. Optimize only rewrites files smaller
    /// than the target, so it is safe to call repeatedly. Note that the small files are only marked
    /// as removed and are not deleted from disk until the table is vacuumed. If the target size is
    /// zero, the table does not exist, or the files could not be merged, a
    /// [`ModelarDbStorageError`] is returned.
    pub async fn optimize_table(
        &self,
        table_name: &str,
        maybe_target_size_in_bytes: Option<u64>,
    ) -> Result<()> {
        let delta_table = self.delta_table(table_name).await?;

        let target_size_in_bytes = maybe_target_size_in_bytes.unwrap_or(64 * 1024 * 1024);
        let target_size = NonZeroU64::new(target_size_in_bytes).ok_or_else(|| {
            ModelarDbStorageError::InvalidArgument(
                "Optimize target file size must be greater than zero.".to_owned(),
            )
        })?;

        let schema = delta_table.snapshot()?.snapshot().arrow_schema();
        let writer_properties = modelardb_compression::writer_properties(&schema);

        delta_table
            .optimize()
            .with_writer_properties(writer_properties)
            .with_target_size(target_size)
            .await?;

        Ok(())
    }

    /// Return the size in bytes of each Apache Parquet file that makes up the Delta Lake table with
    /// `table_name`. The sizes are read from the Delta log, so the files themselves are not
    /// accessed. If the table does not exist or its log could not be read, a
    /// [`ModelarDbStorageError`] is returned.
    pub async fn table_file_sizes(&self, table_name: &str) -> Result<Vec<u64>> {
        let delta_table = self.delta_table(table_name).await?;

        Ok(delta_table
            .snapshot()?
            .log_data()
            .iter()
            .map(|file| file.size() as u64)
            .collect())
    }

    /// Remove the files with `file_paths` from the Delta Lake table with `table_name` in a single
    /// commit, e.g., after their rows have been transferred to another data folder. Files added to
    /// the table later are not affected. The files are only marked as removed and are not deleted
    /// from disk until the table is vacuumed. If a file is no longer in the table or the commit
    /// conflicts with another commit that removed files, nothing is removed and a
    /// [`ModelarDbStorageError`] is returned, since removing the remaining files could duplicate or
    /// lose rows.
    pub async fn remove_files(&self, table_name: &str, file_paths: &[String]) -> Result<()> {
        let delta_table = self.delta_table(table_name).await?;
        let snapshot = delta_table.snapshot()?;

        let file_paths: HashSet<&str> = file_paths.iter().map(String::as_str).collect();
        let actions: Vec<Action> = snapshot
            .log_data()
            .iter()
            .filter(|file| file_paths.contains(file.path().as_ref()))
            .map(|file| Action::Remove(file.remove_action(true)))
            .collect();

        if actions.len() != file_paths.len() {
            return Err(ModelarDbStorageError::InvalidState(format!(
                "Some of the files to remove are no longer in the table '{table_name}'."
            )));
        }

        // Concurrent blind appends do not conflict with the remove, so CommitBuilder retries the
        // commit after them.
        CommitBuilder::default()
            .with_actions(actions)
            .build(
                Some(snapshot),
                delta_table.log_store(),
                DeltaOperation::Delete { predicate: None },
            )
            .await?;

        Ok(())
    }

    /// Return a [`DeltaTableWriter`] for writing to the table with `table_name` in the Delta Lake,
    /// or a [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established or
    /// the table does not exist.
    pub async fn table_writer(&self, table_name: &str) -> Result<DeltaTableWriter> {
        let delta_table = self.delta_table(table_name).await?;
        DeltaTableWriter::try_new(delta_table).await
    }

    /// Write `record_batches` to the table with `table_name` in the Delta Lake and commit them as a
    /// single blind append. `batch_ids` are the ids of the WAL batches that `record_batches`
    /// contain and are included in the commit metadata, so the WAL can delete them. If the data was
    /// not written to the WAL, `batch_ids` is empty. Returns the updated [`DeltaTable`] if the
    /// record batches were written and committed, otherwise returns [`ModelarDbStorageError`].
    pub async fn write_record_batches(
        &self,
        table_name: &str,
        record_batches: &[RecordBatch],
        batch_ids: HashSet<u64>,
    ) -> Result<DeltaTable> {
        let delta_table = self.delta_table(table_name).await?;

        DeltaTableWriter::try_new(delta_table)
            .await?
            .with_batch_ids(batch_ids)
            .write_all_and_commit(record_batches)
            .await
    }

    /// Write `columns` to a Delta Lake table with `table_name`. Returns an updated [`DeltaTable`]
    /// version if the file was written successfully, otherwise returns [`ModelarDbStorageError`].
    async fn write_columns_to_metadata_table(
        &self,
        table_name: &str,
        columns: Vec<ArrayRef>,
    ) -> Result<DeltaTable> {
        let delta_table = self.metadata_delta_table(table_name).await?;
        let schema = delta_table.table_provider().await?.schema();
        let record_batch = RecordBatch::try_new(schema, columns)?;
        let delta_table_writer = DeltaTableWriter::try_new(delta_table).await?;

        delta_table_writer
            .write_all_and_commit(&[record_batch])
            .await
    }

    /// Return a [`DeltaTable`] for manipulating the metadata table with `table_name` in the
    /// Delta Lake, or a [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be
    /// established or the table does not exist.
    async fn metadata_delta_table(&self, table_name: &str) -> Result<DeltaTable> {
        let table_path = self.location_of_metadata_table(table_name);
        self.delta_table_from_path(&table_path).await
    }

    /// Return a [`DeltaTable`] for manipulating the table with `table_name` in the Delta Lake, or a
    /// [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established or the
    /// table does not exist.
    pub async fn delta_table(&self, table_name: &str) -> Result<DeltaTable> {
        let table_path = self.location_of_table(table_name);
        self.delta_table_from_path(&table_path).await
    }

    /// Return a [`TableProvider`] for manipulating the table with `table_name` in the Delta Lake,
    /// or a [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established or
    /// the table does not exist.
    pub async fn table_provider(&self, table_name: &str) -> Result<Arc<dyn TableProvider>> {
        self.delta_table(table_name)
            .await?
            .table_provider()
            .await
            .map_err(|error| error.into())
    }

    /// Return a [`DeltaTable`] for manipulating the table at `table_path` in the Delta Lake, or a
    /// [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established or the
    /// table does not exist.
    async fn delta_table_from_path(&self, table_path: &str) -> Result<DeltaTable> {
        // Clone the cached table if possible and drop the DashMap guard before loading. load() is
        // an async IO call, and get_mut() holds a write guard on the shard, so loading while
        // holding it would serialize access to every table in the same shard.
        let maybe_delta_table = self
            .delta_table_cache
            .get(table_path)
            .map(|delta_table| delta_table.clone());

        if let Some(mut delta_table) = maybe_delta_table {
            delta_table.load().await?;
            Ok(delta_table)
        } else {
            // Return a clear error message if the table does not exist instead of the internal
            // error message from deltalake.
            let table_url =
                deltalake::table::builder::parse_table_uri(table_path).map_err(|_error| {
                    ModelarDbStorageError::InvalidArgument(format!(
                        "Delta table cannot be found at '{table_path}'."
                    ))
                })?;

            // If the table is not in the cache, open it and add it to the cache before returning.
            let delta_table =
                deltalake::open_table_with_storage_options(table_url, self.storage_options.clone())
                    .await?;

            self.delta_table_cache
                .insert(table_path.to_owned(), delta_table.clone());

            Ok(delta_table)
        }
    }

    /// Return the name of each table currently in the Delta Lake. If the table names cannot be
    /// retrieved, [`ModelarDbStorageError`] is returned.
    pub async fn table_names(&self) -> Result<Vec<String>> {
        let sql = "SELECT table_name FROM metadata.table_metadata";
        let batch = sql_and_concat(&self.session_context, sql).await?;

        let table_names = modelardb_types::array!(batch, 0, StringViewArray);
        Ok(table_names.iter().flatten().map(str::to_owned).collect())
    }

    /// Return `true` if a table with `table_name` exists in the Delta Lake, otherwise return
    /// `false`. If the metadata cannot be read, [`ModelarDbStorageError`] is returned.
    pub async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let sql = format!(
            "SELECT table_name FROM metadata.table_metadata WHERE table_name = '{table_name}'"
        );
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        Ok(batch.num_rows() > 0)
    }

    /// Return the location of the metadata table with `table_name`.
    fn location_of_metadata_table(&self, table_name: &str) -> String {
        format!("{}/{METADATA_FOLDER}/{table_name}", self.location)
    }

    /// Return the location of the table with `table_name`.
    fn location_of_table(&self, table_name: &str) -> String {
        format!("{}/{TABLE_FOLDER}/{table_name}", self.location)
    }

    /// Return the [`TableMetadata`] of each table currently in the Delta Lake. If the
    /// [`TableMetadata`] cannot be retrieved, [`ModelarDbStorageError`] is returned.
    pub async fn table_metadata(&self) -> Result<Vec<Arc<TableMetadata>>> {
        let sql = "SELECT table_name, query_schema FROM metadata.table_metadata";
        let batch = sql_and_concat(&self.session_context, sql).await?;

        let table_name_array = modelardb_types::array!(batch, 0, StringViewArray);
        let query_schema_bytes_array = modelardb_types::array!(batch, 1, BinaryViewArray);

        let mut table_metadata = Vec::with_capacity(batch.num_rows());
        for row_index in 0..batch.num_rows() {
            let metadata = self
                .table_metadata_row_to_table_metadata(
                    table_name_array.value(row_index),
                    query_schema_bytes_array.value(row_index),
                )
                .await?;

            table_metadata.push(Arc::new(metadata));
        }

        Ok(table_metadata)
    }

    /// Return the [`TableMetadata`] for the table with `table_name` in the Delta Lake. If the table
    /// does not exist or the [`TableMetadata`] cannot be retrieved, [`ModelarDbStorageError`] is
    /// returned.
    pub async fn table_metadata_for_table(&self, table_name: &str) -> Result<TableMetadata> {
        let sql = format!(
            "SELECT table_name, query_schema FROM metadata.table_metadata WHERE table_name = '{table_name}'"
        );
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        if batch.num_rows() == 0 {
            return Err(ModelarDbStorageError::InvalidArgument(format!(
                "No metadata for table named '{table_name}'."
            )));
        }

        let table_name_array = modelardb_types::array!(batch, 0, StringViewArray);
        let query_schema_bytes_array = modelardb_types::array!(batch, 1, BinaryViewArray);

        self.table_metadata_row_to_table_metadata(
            table_name_array.value(0),
            query_schema_bytes_array.value(0),
        )
        .await
    }

    /// Convert a row from the `table_metadata` table to an instance of [`TableMetadata`]. Returns
    /// [`ModelarDbStorageError`] if the bytes in `query_schema_bytes` are not a valid schema or the
    /// column metadata cannot be retrieved.
    async fn table_metadata_row_to_table_metadata(
        &self,
        table_name: &str,
        query_schema_bytes: &[u8],
    ) -> Result<TableMetadata> {
        let query_schema = Arc::new(try_convert_bytes_to_schema(query_schema_bytes.into())?);
        let column_metadata = self.column_metadata(table_name, &query_schema).await?;

        TableMetadata::try_new(table_name.to_owned(), query_schema, column_metadata)
            .map_err(|error| error.into())
    }

    /// Return the [`ColumnMetadata`] of each column in `query_schema` for the table with
    /// `table_name`. Columns without a row in the `column_metadata` table are stored losslessly. If
    /// the column metadata cannot be retrieved, [`ModelarDbStorageError`] is returned.
    async fn column_metadata(
        &self,
        table_name: &str,
        query_schema: &Arc<Schema>,
    ) -> Result<Vec<ColumnMetadata>> {
        let sql = format!(
            "SELECT column_index, error_bound_value, error_bound_is_relative, generated_column_expr
             FROM metadata.column_metadata
             WHERE table_name = '{table_name}'"
        );
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        let mut column_metadata =
            vec![ColumnMetadata::Stored(ErrorBound::Lossless); query_schema.fields().len()];

        let column_index_array = modelardb_types::array!(batch, 0, Int32Array);
        let error_bound_value_array = modelardb_types::array!(batch, 1, Float32Array);
        let error_bound_is_relative_array = modelardb_types::array!(batch, 2, BooleanArray);
        let generated_column_expr_array = modelardb_types::array!(batch, 3, BinaryViewArray);

        let df_query_schema = query_schema.clone().to_dfschema()?;

        for row_index in 0..batch.num_rows() {
            let column_index = column_index_array.value(row_index) as usize;

            column_metadata[column_index] = if generated_column_expr_array.is_valid(row_index) {
                let expr = Expr::from_bytes(generated_column_expr_array.value(row_index))?;
                let generated_column = GeneratedColumn::try_from_expr(expr, &df_query_schema)?;
                ColumnMetadata::Generated(generated_column)
            } else {
                let error_bound_value = error_bound_value_array.value(row_index);

                if error_bound_is_relative_array.value(row_index) {
                    ColumnMetadata::Stored(ErrorBound::try_new_relative(error_bound_value)?)
                } else {
                    ColumnMetadata::Stored(ErrorBound::try_new_absolute(error_bound_value)?)
                }
            };
        }

        Ok(column_metadata)
    }

    /// Return the [`TableMetadata`] of the table with `table_name` if it is registered with the
    /// [`SessionContext`] of `self`, otherwise return [`None`]. Unlike
    /// [`Self::table_metadata_for_table`], the metadata tables are not read.
    pub async fn table_metadata_for_registered_table(
        &self,
        table_name: &str,
    ) -> Option<Arc<TableMetadata>> {
        let table_provider = self.session_context.table_provider(table_name).await.ok()?;
        crate::maybe_table_provider_to_table_metadata(table_provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::compute;
    use arrow::datatypes::ArrowPrimitiveType;
    use modelardb_test::table::{self, NoOpDataSink, TABLE_NAME};
    use modelardb_types::types::ArrowTimestamp;
    use tempfile::TempDir;

    const ROW_COUNT: usize = 5;

    // Tests for DataFolder.
    #[tokio::test]
    async fn test_open_local_url_without_scheme() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        let data_folder = DataFolder::open_local_url(path).await;
        assert!(data_folder.is_ok());
    }

    #[tokio::test]
    async fn test_open_local_url_with_file_scheme() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();
        let url = format!("file://{path}");

        let data_folder = DataFolder::open_local_url(&url).await;
        assert!(data_folder.is_ok());
    }

    #[tokio::test]
    async fn test_open_local_url_with_memory_scheme() {
        let data_folder = DataFolder::open_local_url("memory://").await;
        assert!(data_folder.is_ok());
    }

    #[tokio::test]
    async fn test_open_local_url_with_invalid_scheme() {
        let url = "invalid://path";
        let result = DataFolder::open_local_url(url).await;

        assert_eq!(
            result.err().unwrap().to_string(),
            format!("Invalid Argument Error: {url} is not a valid local URL.")
        );
    }

    #[tokio::test]
    async fn test_open_memory() {
        let data_folder = DataFolder::open_memory().await;
        assert!(data_folder.is_ok());
    }

    #[tokio::test]
    async fn test_create_metadata_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        assert!(
            data_folder
                .session_context
                .sql("SELECT table_name, query_schema FROM metadata.table_metadata")
                .await
                .is_ok()
        );

        assert!(
            data_folder
                .session_context
                .sql(
                    "SELECT table_name, column_name, column_index, error_bound_value, \
                     error_bound_is_relative, generated_column_expr FROM metadata.column_metadata"
                )
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_register_tables() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        data_folder
            .register_tables(Arc::new(NoOpDataSink {}))
            .await
            .unwrap();

        // Verify the table is queryable through the session context.
        assert!(
            data_folder
                .session_context
                .table_provider(TABLE_NAME)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_create_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        assert!(data_folder.delta_table(TABLE_NAME).await.is_ok());

        // Check that a row has been added to the table_metadata table.
        let sql = "SELECT table_name, query_schema FROM metadata.table_metadata";
        let batch = sql_and_concat(&data_folder.session_context, sql)
            .await
            .unwrap();

        assert_eq!(**batch.column(0), StringViewArray::from(vec![TABLE_NAME]));
        assert_eq!(
            **batch.column(1),
            BinaryViewArray::from_iter_values([&try_convert_schema_to_bytes(
                &table::table_metadata().query_schema
            )
            .unwrap()])
        );

        // Check that a row has been added to the column_metadata table for each column with an
        // error bound or a generated column expression.
        let sql = "SELECT table_name, column_name, column_index, error_bound_value, \
                   error_bound_is_relative, generated_column_expr FROM metadata.column_metadata \
                   ORDER BY column_index";
        let batch = sql_and_concat(&data_folder.session_context, sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringViewArray::from(vec![TABLE_NAME, TABLE_NAME, TABLE_NAME])
        );
        assert_eq!(
            **batch.column(1),
            StringViewArray::from(vec!["absolute_value", "generated_value", "relative_value"])
        );
        assert_eq!(**batch.column(2), Int32Array::from(vec![1, 2, 3]));
        assert_eq!(
            **batch.column(3),
            Float32Array::from(vec![Some(1.0), None, Some(5.0)])
        );
        assert_eq!(
            **batch.column(4),
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );

        let generated_column_exprs = batch.column(5);
        assert!(generated_column_exprs.is_null(0));
        assert!(generated_column_exprs.is_valid(1));
        assert!(generated_column_exprs.is_null(2));
    }

    #[tokio::test]
    async fn test_create_table_without_error_bounds_and_generated_columns() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        data_folder
            .create_table(&lossless_table_metadata())
            .await
            .unwrap();

        // Columns that are stored losslessly do not have a row in the column_metadata table.
        let sql = "SELECT table_name FROM metadata.column_metadata";
        let batch = sql_and_concat(&data_folder.session_context, sql)
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_create_existing_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let result = data_folder.create_table(&table::table_metadata()).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Delta Lake Error: Generic error: A Delta Lake table already exists at that location."
        );
    }

    #[tokio::test]
    async fn test_create_table_with_unsigned_integers() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let query_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::Utf8View, false),
        ]));
        let table_metadata = TableMetadata::try_new(
            "uint_table".to_owned(),
            query_schema,
            vec![ColumnMetadata::Stored(ErrorBound::Lossless); 2],
        )
        .unwrap();

        let result = data_folder.create_table(&table_metadata).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Delta Lake Error: Data does not match the schema or partitions of the table: \
            Unsigned integers are not supported."
        );
    }

    #[tokio::test]
    async fn test_drop_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        data_folder.drop_table(TABLE_NAME).await.unwrap();

        assert!(data_folder.delta_table(TABLE_NAME).await.is_err());

        // Verify that the table was deleted from the table_metadata table.
        let sql = "SELECT table_name FROM metadata.table_metadata";
        let batch = sql_and_concat(&data_folder.session_context, sql)
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);

        // Verify that the columns were deleted from the column_metadata table.
        let sql = "SELECT table_name FROM metadata.column_metadata";
        let batch = sql_and_concat(&data_folder.session_context, sql)
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_drop_missing_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let result = data_folder.drop_table("missing_table").await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: Table with name 'missing_table' does not exist."
        );
    }

    #[tokio::test]
    async fn test_truncate_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let mut delta_table = write_record_batch(&data_folder).await;
        assert_eq!(delta_table.get_file_uris().unwrap().count(), 1);

        data_folder.truncate_table(TABLE_NAME).await.unwrap();

        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_file_uris().unwrap().count(), 0);
    }

    #[tokio::test]
    async fn test_truncate_missing_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let result = data_folder.truncate_table("missing_table").await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: Delta table cannot be found at '{}'.",
                data_folder.location_of_table("missing_table")
            )
        );
    }

    #[tokio::test]
    async fn test_vacuum_table() {
        let (temp_dir, data_folder) = create_data_folder_with_table().await;

        write_record_batch(&data_folder).await;
        data_folder.truncate_table(TABLE_NAME).await.unwrap();

        // The stale Apache Parquet file should still exist on disk alongside the _delta_log folder.
        let table_path = format!("{}/tables/{TABLE_NAME}", temp_dir.path().to_str().unwrap());
        assert_eq!(std::fs::read_dir(&table_path).unwrap().count(), 2);

        data_folder.vacuum_table(TABLE_NAME, Some(0)).await.unwrap();

        // Only the _delta_log folder should remain.
        assert_eq!(std::fs::read_dir(&table_path).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn test_vacuum_table_with_default_retention_period() {
        let (temp_dir, data_folder) = create_data_folder_with_table().await;

        write_record_batch(&data_folder).await;
        data_folder.truncate_table(TABLE_NAME).await.unwrap();

        // Vacuum with None uses the default 7-day retention period, so the recently truncated file
        // is not yet stale and should still exist on disk.
        data_folder.vacuum_table(TABLE_NAME, None).await.unwrap();

        let table_path = format!("{}/tables/{TABLE_NAME}", temp_dir.path().to_str().unwrap());
        assert_eq!(std::fs::read_dir(&table_path).unwrap().count(), 2);
    }

    #[tokio::test]
    async fn test_vacuum_table_with_out_of_bounds_retention_period() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let result = data_folder
            .vacuum_table(TABLE_NAME, Some(MAX_RETENTION_PERIOD_IN_SECONDS + 1))
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: \
                Retention period cannot be more than {MAX_RETENTION_PERIOD_IN_SECONDS} seconds."
            )
        );
    }

    #[tokio::test]
    async fn test_vacuum_missing_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let result = data_folder.vacuum_table("missing_table", None).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: Delta table cannot be found at '{}'.",
                data_folder.location_of_table("missing_table")
            )
        );
    }

    #[tokio::test]
    async fn test_optimize_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        // Each write is a separate commit, so four writes produce four small files.
        for _ in 0..4 {
            write_record_batch(&data_folder).await;
        }

        assert_eq!(active_file_count(&data_folder).await, 4);

        data_folder.optimize_table(TABLE_NAME, None).await.unwrap();

        // The small files should be merged into a single active file with no rows lost or
        // duplicated.
        assert_eq!(active_file_count(&data_folder).await, 1);
        assert_eq!(row_count(&data_folder).await, 4 * ROW_COUNT);
    }

    #[tokio::test]
    async fn test_optimize_table_commits_alongside_append() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        // Each write is a separate commit, so four writes produce four small files.
        for _ in 0..4 {
            write_record_batch(&data_folder).await;
        }

        // The append is committed after the snapshot that OPTIMIZE plans from is loaded, as if it
        // happened while OPTIMIZE was running.
        let delta_table = data_folder.delta_table(TABLE_NAME).await.unwrap();
        write_record_batch(&data_folder).await;
        delta_table.optimize().await.unwrap();

        // The four files are merged and the appended file is kept, so no rows are lost.
        assert_eq!(active_file_count(&data_folder).await, 2);
        assert_eq!(row_count(&data_folder).await, 5 * ROW_COUNT);
    }

    #[tokio::test]
    async fn test_optimize_table_with_small_target_file_size() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        // Each write is a separate commit, so four writes produce four small files.
        for _ in 0..4 {
            write_record_batch(&data_folder).await;
        }

        // A one-byte target is smaller than every existing file, so none of them are candidates for
        // merging, and the files should be left untouched.
        data_folder
            .optimize_table(TABLE_NAME, Some(1))
            .await
            .unwrap();

        assert_eq!(active_file_count(&data_folder).await, 4);
    }

    #[tokio::test]
    async fn test_optimize_table_with_zero_target_file_size() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let result = data_folder.optimize_table(TABLE_NAME, Some(0)).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: Optimize target file size must be greater than zero."
        );
    }

    #[tokio::test]
    async fn test_optimize_missing_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let result = data_folder.optimize_table("missing_table", None).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: Delta table cannot be found at '{}'.",
                data_folder.location_of_table("missing_table")
            )
        );
    }

    #[tokio::test]
    async fn test_table_file_sizes() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        write_record_batch(&data_folder).await;
        write_record_batch(&data_folder).await;

        let file_sizes = data_folder.table_file_sizes(TABLE_NAME).await.unwrap();

        assert_eq!(file_sizes.len(), 2);
        assert!(file_sizes.iter().all(|file_size| *file_size > 0));
    }

    #[tokio::test]
    async fn test_remove_files() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        write_record_batch(&data_folder).await;
        let file_paths_to_remove = file_paths(&data_folder).await;
        write_record_batch(&data_folder).await;

        data_folder
            .remove_files(TABLE_NAME, &file_paths_to_remove)
            .await
            .unwrap();

        // Only the file written after the file paths were read should remain.
        let remaining_file_paths = file_paths(&data_folder).await;
        assert_eq!(remaining_file_paths.len(), 1);
        assert!(!file_paths_to_remove.contains(&remaining_file_paths[0]));
        assert_eq!(row_count(&data_folder).await, ROW_COUNT);
    }

    #[tokio::test]
    async fn test_remove_files_with_missing_file() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        write_record_batch(&data_folder).await;
        let mut file_paths_to_remove = file_paths(&data_folder).await;
        file_paths_to_remove.push("missing_file.parquet".to_owned());

        let result = data_folder
            .remove_files(TABLE_NAME, &file_paths_to_remove)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid State Error: Some of the files to remove are no longer in the table 'test_table'."
        );

        // Nothing should be removed if any of the files are missing.
        assert_eq!(active_file_count(&data_folder).await, 1);
    }

    #[tokio::test]
    async fn test_write_record_batches() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let delta_table = write_record_batch(&data_folder).await;

        // Verify the write produced an Apache Parquet file.
        assert_eq!(delta_table.get_file_uris().unwrap().count(), 1);

        // Read the data back and verify the content.
        let (_table, stream) = delta_table.scan_table().await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let read_batch = compute::concat_batches(&batches[0].schema(), &batches).unwrap();

        assert_eq!(read_batch, table::table_record_batch(ROW_COUNT));
    }

    #[tokio::test]
    async fn test_write_empty_slice_to_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let delta_table = data_folder
            .write_record_batches(TABLE_NAME, &[], HashSet::new())
            .await
            .unwrap();

        assert_eq!(delta_table.get_file_uris().unwrap().count(), 0);
    }

    #[tokio::test]
    async fn test_write_empty_record_batch_to_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let empty_batch = RecordBatch::new_empty(table::table_metadata().stored_schema);
        let delta_table = data_folder
            .write_record_batches(TABLE_NAME, &[empty_batch], HashSet::new())
            .await
            .unwrap();

        assert_eq!(delta_table.get_file_uris().unwrap().count(), 0);
    }

    #[tokio::test]
    async fn test_write_record_batches_to_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder
            .write_record_batches(
                "missing_table",
                &[table::table_record_batch(ROW_COUNT)],
                HashSet::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: Delta table cannot be found at '{}'.",
                data_folder.location_of_table("missing_table")
            )
        );
    }

    #[tokio::test]
    async fn test_table_names() {
        let (_temp_dir, data_folder) = create_data_folder_and_create_normal_tables().await;

        let time_series_table_metadata = test_table::time_series_table_metadata();
        data_folder
            .create_time_series_table(&time_series_table_metadata)
            .await
            .unwrap();

        let table_names = data_folder.table_names().await.unwrap();
        assert_eq!(
            table_names,
            vec!["normal_table_2", "normal_table_1", TIME_SERIES_TABLE_NAME]
        );
    }

    #[tokio::test]
    async fn test_normal_table_names() {
        let (_temp_dir, data_folder) = create_data_folder_and_create_normal_tables().await;

        let normal_table_names = data_folder.normal_table_names().await.unwrap();
        assert_eq!(normal_table_names, vec!["normal_table_2", "normal_table_1"]);
    }

    #[tokio::test]
    async fn test_time_series_table_names() {
        let (_temp_dir, data_folder) = create_data_folder_and_create_time_series_table().await;

        let time_series_table_names = data_folder.time_series_table_names().await.unwrap();
        assert_eq!(time_series_table_names, vec![TIME_SERIES_TABLE_NAME]);
    }

    #[tokio::test]
    async fn test_normal_table_schema_for_existing_normal_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_create_normal_tables().await;

        let schema = data_folder
            .normal_table_schema("normal_table_1")
            .await
            .unwrap();

        assert_eq!(schema.as_ref(), &test_table::normal_table_schema());
    }

    #[tokio::test]
    async fn test_normal_table_schema_for_missing_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_create_normal_tables().await;

        let maybe_schema = data_folder.normal_table_schema("missing_table").await;

        assert!(maybe_schema.is_none());
    }

    #[tokio::test]
    async fn test_time_series_table_metadata() {
        let (_temp_dir, data_folder) = create_data_folder_and_create_time_series_table().await;

        let time_series_table_metadata = data_folder.time_series_table_metadata().await.unwrap();

        assert_eq!(
            time_series_table_metadata.first().unwrap().name,
            test_table::time_series_table_metadata().name,
        );
    }

    #[tokio::test]
    async fn test_time_series_table_metadata_for_existing_time_series_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_create_time_series_table().await;

        let time_series_table_metadata = data_folder
            .time_series_table_metadata_for_time_series_table(TIME_SERIES_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(
            time_series_table_metadata.name,
            test_table::time_series_table_metadata().name,
        );
    }

    #[tokio::test]
    async fn test_time_series_table_metadata_for_missing_time_series_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_create_time_series_table().await;

        let result = data_folder
            .time_series_table_metadata_for_time_series_table("missing_table")
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: No metadata for time series table named 'missing_table'."
        );
    }

    #[tokio::test]
    async fn test_time_series_table_metadata_for_registered_time_series_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_create_time_series_table().await;

        data_folder
            .register_tables(Arc::new(NoOpDataSink {}))
            .await
            .unwrap();

        let metadata = data_folder
            .time_series_table_metadata_for_registered_time_series_table(TIME_SERIES_TABLE_NAME)
            .await;

        assert_eq!(
            metadata.unwrap().as_ref(),
            &test_table::time_series_table_metadata(),
        );
    }

    #[tokio::test]
    async fn test_time_series_table_metadata_for_unregistered_time_series_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_create_time_series_table().await;

        let metadata = data_folder
            .time_series_table_metadata_for_registered_time_series_table(TIME_SERIES_TABLE_NAME)
            .await;

        assert!(metadata.is_none());
    }

    #[tokio::test]
    async fn test_error_bound() {
        let (_temp_dir, data_folder) = create_data_folder_and_create_time_series_table().await;
    async fn create_data_folder_with_table() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        data_folder
            .create_table(&table::table_metadata())
            .await
            .unwrap();

        (temp_dir, data_folder)
    }

    fn lossless_table_metadata() -> TableMetadata {
        let query_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, true),
            Field::new("value", DataType::Float32, true),
        ]));

        TableMetadata::try_new(
            "lossless_table".to_owned(),
            query_schema,
            vec![ColumnMetadata::Stored(ErrorBound::Lossless); 2],
        )
        .unwrap()
    }

    async fn write_record_batch(data_folder: &DataFolder) -> DeltaTable {
        data_folder
            .write_record_batches(
                TABLE_NAME,
                &[table::table_record_batch(ROW_COUNT)],
                HashSet::new(),
            )
            .await
            .unwrap()
    }

    async fn active_file_count(data_folder: &DataFolder) -> usize {
        let delta_table = data_folder.delta_table(TABLE_NAME).await.unwrap();
        delta_table.get_file_uris().unwrap().count()
    }

    async fn file_paths(data_folder: &DataFolder) -> Vec<String> {
        let delta_table = data_folder.delta_table(TABLE_NAME).await.unwrap();

        delta_table
            .snapshot()
            .unwrap()
            .log_data()
            .iter()
            .map(|file| file.path().to_string())
            .collect()
    }

    async fn row_count(data_folder: &DataFolder) -> usize {
        let delta_table = data_folder.delta_table(TABLE_NAME).await.unwrap();
        let (_table, stream) = delta_table.scan_table().await.unwrap();

        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        batches.iter().map(|batch| batch.num_rows()).sum()
    }
}
