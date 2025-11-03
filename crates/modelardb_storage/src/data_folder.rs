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

use std::collections::HashMap;
use std::fs;
use std::path::Path as StdPath;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray, Float32Array, Int16Array, RecordBatch,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::TimeDelta;
use dashmap::DashMap;
use datafusion::catalog::TableProvider;
use datafusion::common::{DFSchema, ToDFSchema};
use datafusion::datasource::sink::DataSink;
use datafusion::logical_expr::{Expr, lit};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::format::SortingColumn;
use datafusion::prelude::{SessionContext, col};
use datafusion_proto::bytes::Serializeable;
use deltalake::table::builder::ensure_table_uri;
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use deltalake::delta_datafusion::DeltaDataChecker;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{Action, Add, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{DeltaOps, DeltaTable, DeltaTableError};
use futures::{StreamExt, TryStreamExt};
use modelardb_types::flight::protocol;
use modelardb_types::functions::{try_convert_bytes_to_schema, try_convert_schema_to_bytes};
use modelardb_types::schemas::{COMPRESSED_SCHEMA, FIELD_COLUMN};
use modelardb_types::types::{
    ArrowValue, ErrorBound, GeneratedColumn, MAX_RETENTION_PERIOD_IN_SECONDS,
    TimeSeriesTableMetadata,
};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use url::Url;
use uuid::Uuid;

use crate::error::{ModelarDbStorageError, Result};
use crate::{
    METADATA_FOLDER, TABLE_FOLDER, apache_parquet_writer_properties, register_metadata_table,
    sql_and_concat,
};

/// Types of tables supported by ModelarDB.
enum TableType {
    NormalTable,
    TimeSeriesTable,
}

/// Functionality for managing Delta Lake tables in a local folder or an object store.
#[derive(Clone)]
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
    session_context: Arc<SessionContext>,
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
        let data_folder = Self {
            location: "memory:///modelardb".to_owned(),
            storage_options: HashMap::new(),
            object_store: Arc::new(InMemory::new()),
            delta_table_cache: DashMap::new(),
            session_context: Arc::new(crate::create_session_context()),
        };

        data_folder.create_and_register_metadata_tables().await?;

        Ok(data_folder)
    }

    /// Create a new [`DataFolder`] that manages the Delta tables in `data_folder_path`. Returns a
    /// [`ModelarDbStorageError`] if `data_folder_path` does not exist and could not be created or
    /// the metadata tables cannot be created.
    pub async fn open_local(data_folder_path: &StdPath) -> Result<Self> {
        // Ensure the directories in the path exists as LocalFileSystem otherwise returns an error.
        fs::create_dir_all(data_folder_path)
            .map_err(|error| DeltaTableError::generic(error.to_string()))?;

        // Use with_automatic_cleanup to ensure empty directories are deleted automatically.
        let object_store = LocalFileSystem::new_with_prefix(data_folder_path)
            .map_err(|error| DeltaTableError::generic(error.to_string()))?
            .with_automatic_cleanup(true);

        let location = data_folder_path
            .to_str()
            .ok_or_else(|| DeltaTableError::generic("Local data folder path is not UTF-8."))?
            .to_owned();

        let data_folder = Self {
            location,
            storage_options: HashMap::new(),
            object_store: Arc::new(object_store),
            delta_table_cache: DashMap::new(),
            session_context: Arc::new(crate::create_session_context()),
        };

        data_folder.create_and_register_metadata_tables().await?;

        Ok(data_folder)
    }

    /// Create a new [`DataFolder`] that manages Delta tables in the remote object store given by
    /// `storage_configuration`. Returns [`ModelarDbStorageError`] if a connection to the specified
    /// object store could not be created.
    pub async fn open_object_store(
        storage_configuration: protocol::manager_metadata::StorageConfiguration,
    ) -> Result<Self> {
        match storage_configuration {
            protocol::manager_metadata::StorageConfiguration::S3Configuration(s3_configuration) => {
                // Register the S3 storage handlers to allow the use of Amazon S3 object stores. This is
                // required at runtime to initialize the S3 storage implementation in the deltalake_aws
                // storage subcrate. It is safe to call this function multiple times as the handlers are
                // stored in a DashMap, thus, the handlers are simply overwritten with the same each time.
                deltalake::aws::register_handlers(None);

                Self::open_s3(
                    s3_configuration.endpoint,
                    s3_configuration.bucket_name,
                    s3_configuration.access_key_id,
                    s3_configuration.secret_access_key,
                )
                .await
            }
            protocol::manager_metadata::StorageConfiguration::AzureConfiguration(
                azure_configuration,
            ) => {
                Self::open_azure(
                    azure_configuration.account_name,
                    azure_configuration.access_key,
                    azure_configuration.container_name,
                )
                .await
            }
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

        let data_folder = DataFolder {
            location,
            storage_options,
            object_store: Arc::new(object_store),
            delta_table_cache: DashMap::new(),
            session_context: Arc::new(crate::create_session_context()),
        };

        data_folder.create_and_register_metadata_tables().await?;

        Ok(data_folder)
    }

    /// Create a new [`DataFolder`] that manages the Delta tables in an object store with an
    /// Azure-compatible API. Returns a [`ModelarDbStorageError`] if a connection to the object
    /// store could not be made or the metadata tables cannot be created.
    pub async fn open_azure(
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

        let data_folder = DataFolder {
            location,
            storage_options,
            object_store: Arc::new(object_store),
            delta_table_cache: DashMap::new(),
            session_context: Arc::new(crate::create_session_context()),
        };

        data_folder.create_and_register_metadata_tables().await?;

        Ok(data_folder)
    }

    /// If they do not already exist, create the tables in the Delta Lake for normal table and time
    /// series table metadata and register them with the Apache DataFusion session context.
    /// * The `normal_table_metadata` table contains the metadata for normal tables.
    /// * The `time_series_table_metadata` table contains the main metadata for time series tables.
    /// * The `time_series_table_field_columns` table contains the name, index, error bound value,
    ///   whether error bound is relative, and generation expression of the field columns in each
    ///   time series table.
    ///
    /// If the tables exist or were created, return [`Ok`], otherwise return
    /// [`ModelarDbStorageError`].
    async fn create_and_register_metadata_tables(&self) -> Result<()> {
        // Create and register the normal_table_metadata table if it does not exist.
        let delta_table = self
            .create_metadata_table(
                "normal_table_metadata",
                &Schema::new(vec![Field::new("table_name", DataType::Utf8, false)]),
            )
            .await?;

        register_metadata_table(&self.session_context, "normal_table_metadata", delta_table)?;

        // Create and register the time_series_table_metadata table if it does not exist.
        let delta_table = self
            .create_metadata_table(
                "time_series_table_metadata",
                &Schema::new(vec![
                    Field::new("table_name", DataType::Utf8, false),
                    Field::new("query_schema", DataType::Binary, false),
                ]),
            )
            .await?;

        register_metadata_table(
            &self.session_context,
            "time_series_table_metadata",
            delta_table,
        )?;

        // Create and register the time_series_table_field_columns table if it does not exist. Note
        // that column_index will only use a maximum of 10 bits. generated_column_expr is NULL if
        // the fields are stored as segments.
        let delta_table = self
            .create_metadata_table(
                "time_series_table_field_columns",
                &Schema::new(vec![
                    Field::new("table_name", DataType::Utf8, false),
                    Field::new("column_name", DataType::Utf8, false),
                    Field::new("column_index", DataType::Int16, false),
                    Field::new("error_bound_value", DataType::Float32, false),
                    Field::new("error_bound_is_relative", DataType::Boolean, false),
                    Field::new("generated_column_expr", DataType::Binary, true),
                ]),
            )
            .await?;

        register_metadata_table(
            &self.session_context,
            "time_series_table_field_columns",
            delta_table,
        )?;

        Ok(())
    }

    /// Register all normal tables and time series tables in `self` with its [`SessionContext`].
    /// `data_sink` is set as the [`DataSink`] for all of the tables. If the tables could not be
    /// registered, [`ModelarDbStorageError`] is returned.
    pub async fn register_tables(
        &self,
        data_sink: Arc<dyn DataSink>,
    ) -> Result<()> {
        // Register normal tables.
        for normal_table_name in self.normal_table_names().await? {
            let delta_table = self.delta_table(&normal_table_name).await?;

            crate::register_normal_table(
                &self.session_context,
                &normal_table_name,
                delta_table,
                data_sink.clone(),
            )?;
        }

        // Register time series tables.
        for metadata in self.time_series_table_metadata().await? {
            let delta_table = self.delta_table(&metadata.name).await?;

            crate::register_time_series_table(
                &self.session_context,
                delta_table,
                metadata,
                data_sink.clone(),
            )?;
        }

        Ok(())
    }

    /// Return the session context used to query the tables using Apache DataFusion.
    pub fn session_context(&self) -> &SessionContext {
        &self.session_context
    }

    /// Return an [`ObjectStore`] to access the root of the Delta Lake.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    /// Return a [`DeltaTable`] for manipulating the metadata table with `table_name` in the
    /// Delta Lake, or a [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be
    /// established or the table does not exist.
    pub async fn metadata_delta_table(&self, table_name: &str) -> Result<DeltaTable> {
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

    /// Return a [`DeltaOps`] for manipulating the metadata table with `table_name` in the Delta
    /// Lake, or a [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established
    /// or the table does not exist.
    pub async fn metadata_delta_ops(&self, table_name: &str) -> Result<DeltaOps> {
        let table_path = self.location_of_metadata_table(table_name);
        self.delta_table_from_path(&table_path)
            .await
            .map(Into::into)
    }

    /// Return a [`DeltaOps`] for manipulating the table with `table_name` in the Delta Lake, or a
    /// [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established or the
    /// table does not exist.
    pub async fn delta_ops(&self, table_name: &str) -> Result<DeltaOps> {
        let table_path = self.location_of_table(table_name);
        self.delta_table_from_path(&table_path)
            .await
            .map(Into::into)
    }

    /// Return a [`DeltaTable`] for manipulating the table at `table_path` in the Delta Lake, or a
    /// [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established or the
    /// table does not exist.
    async fn delta_table_from_path(&self, table_path: &str) -> Result<DeltaTable> {
        // Use the cache if possible and load to get the latest table data.
        if let Some(mut delta_table) = self.delta_table_cache.get_mut(table_path) {
            delta_table.load().await?;
            Ok(delta_table.clone())
        } else {
            // If the table is not in the cache, open it and add it to the cache before returning.
            let table_url = ensure_table_uri(table_path)?;
            let delta_table = deltalake::open_table_with_storage_options(
                table_url,
                self.storage_options.clone(),
            )
            .await?;

            self.delta_table_cache
                .insert(table_path.to_owned(), delta_table.clone());

            Ok(delta_table)
        }
    }

    /// Return `true` if the table with `table_name` is a normal table, otherwise return `false`.
    pub async fn is_normal_table(&self, table_name: &str) -> Result<bool> {
        Ok(self
            .normal_table_names()
            .await?
            .contains(&table_name.to_owned()))
    }

    /// Return `true` if the table with `table_name` is a time series table, otherwise return `false`.
    pub async fn is_time_series_table(&self, table_name: &str) -> Result<bool> {
        Ok(self
            .time_series_table_names()
            .await?
            .contains(&table_name.to_owned()))
    }

    /// Return the name of each table currently in the Delta Lake. If the table names cannot be
    /// retrieved, [`ModelarDbStorageError`] is returned.
    pub async fn table_names(&self) -> Result<Vec<String>> {
        let normal_table_names = self.normal_table_names().await?;
        let time_series_table_names = self.time_series_table_names().await?;

        let mut table_names = normal_table_names;
        table_names.extend(time_series_table_names);

        Ok(table_names)
    }

    /// Return the name of each normal table currently in the Delta Lake. Note that this does not
    /// include time series tables. If the normal table names cannot be retrieved,
    /// [`ModelarDbStorageError`] is returned.
    pub async fn normal_table_names(&self) -> Result<Vec<String>> {
        self.table_names_of_type(TableType::NormalTable).await
    }

    /// Return the schema of the table with the name in `table_name` if it is a normal table. If the
    /// table does not exist or the table is not a normal table, return [`None`].
    pub async fn normal_table_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        if self
            .is_normal_table(table_name)
            .await
            .is_ok_and(|is_normal_table| is_normal_table)
        {
            let schema = self.delta_table(table_name)
                .await
                .expect("Delta Lake table should exist if the metadata is in the Delta Lake.")
                .schema();

            Some(schema)
        } else {
            None
        }
    }

    /// Return the name of each time series table currently in the Delta Lake. Note that this does
    /// not include normal tables. If the time series table names cannot be retrieved,
    /// [`ModelarDbStorageError`] is returned.
    pub async fn time_series_table_names(&self) -> Result<Vec<String>> {
        self.table_names_of_type(TableType::TimeSeriesTable).await
    }

    /// Return the name of tables of `table_type`. Returns [`ModelarDbStorageError`] if the table
    /// names cannot be retrieved.
    async fn table_names_of_type(&self, table_type: TableType) -> Result<Vec<String>> {
        let table_type = match table_type {
            TableType::NormalTable => "normal_table",
            TableType::TimeSeriesTable => "time_series_table",
        };

        let sql = format!("SELECT table_name FROM metadata.{table_type}_metadata");
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        let table_names = modelardb_types::array!(batch, 0, StringArray);
        Ok(table_names.iter().flatten().map(str::to_owned).collect())
    }

    /// Return a [`DeltaTableWriter`] for writing to the table with `table_name` in the Delta Lake,
    /// or a [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established or
    /// the table does not exist.
    pub async fn table_writer(&self, table_name: &str) -> Result<DeltaTableWriter> {
        let delta_table = self.delta_table(table_name).await?;
        if self
            .time_series_table_metadata_for_registered_time_series_table(table_name)
            .await
            .is_some()
        {
            self.time_series_table_writer(delta_table).await
        } else {
            self.normal_or_metadata_table_writer(delta_table).await
        }
    }

    /// Return a [`DeltaTableWriter`] for writing to the time series table corresponding to
    /// `delta_table` in the Delta Lake, or a [`ModelarDbStorageError`] if a connection to the Delta
    /// Lake cannot be established or the table does not exist.
    pub async fn time_series_table_writer(
        &self,
        delta_table: DeltaTable,
    ) -> Result<DeltaTableWriter> {
        let partition_columns = vec![FIELD_COLUMN.to_owned()];

        // Specify that the file must be sorted by the tag columns and then by start_time.
        let base_compressed_schema_len = COMPRESSED_SCHEMA.0.fields().len();
        let compressed_schema_len = TableProvider::schema(&delta_table).fields().len();
        let sorting_columns_len = (compressed_schema_len - base_compressed_schema_len) + 1;
        let mut sorting_columns = Vec::with_capacity(sorting_columns_len);

        // Compressed segments have the tag columns at the end of the schema.
        for tag_column_index in base_compressed_schema_len..compressed_schema_len {
            sorting_columns.push(SortingColumn::new(tag_column_index as i32, false, false));
        }

        // Compressed segments store the first timestamp in the second column.
        sorting_columns.push(SortingColumn::new(1, false, false));

        let writer_properties = apache_parquet_writer_properties(Some(sorting_columns));
        DeltaTableWriter::try_new(delta_table, partition_columns, writer_properties)
    }

    /// Return a [`DeltaTableWriter`] for writing to the table corresponding to `delta_table` in the
    /// Delta Lake, or a [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be
    /// established or the table does not exist.
    pub async fn normal_or_metadata_table_writer(
        &self,
        delta_table: DeltaTable,
    ) -> Result<DeltaTableWriter> {
        let writer_properties = apache_parquet_writer_properties(None);
        DeltaTableWriter::try_new(delta_table, vec![], writer_properties)
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

    /// Return the location of the metadata table with `table_name`.
    fn location_of_metadata_table(&self, table_name: &str) -> String {
        format!("{}/{METADATA_FOLDER}/{table_name}", self.location)
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
            self.location_of_table(table_name),
            SaveMode::ErrorIfExists,
        )
        .await
    }

    /// Create a Delta Lake table for a time series table with `time_series_table_metadata` if it
    /// does not already exist. Returns [`DeltaTable`] if the table could be created and
    /// [`ModelarDbStorageError`] if it could not.
    pub async fn create_time_series_table(
        &self,
        time_series_table_metadata: &TimeSeriesTableMetadata,
    ) -> Result<DeltaTable> {
        self.create_table(
            &time_series_table_metadata.name,
            &time_series_table_metadata.compressed_schema,
            &[FIELD_COLUMN.to_owned()],
            self.location_of_table(&time_series_table_metadata.name),
            SaveMode::ErrorIfExists,
        )
        .await
    }

    /// Return the location of the table with `table_name`.
    fn location_of_table(&self, table_name: &str) -> String {
        format!("{}/{TABLE_FOLDER}/{table_name}", self.location)
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

        let delta_table = CreateBuilder::new()
            .with_storage_options(self.storage_options.clone())
            .with_table_name(table_name)
            .with_location(location.clone())
            .with_columns(columns)
            .with_partition_columns(partition_columns)
            .with_save_mode(save_mode)
            .await?;

        // If the table was created successfully, add it to the cache.
        self.delta_table_cache.insert(location, delta_table.clone());

        Ok(delta_table)
    }

    /// Drop the metadata table with `table_name` from the Delta Lake by deleting every file related
    /// to the table. The table folder cannot be deleted directly since folders do not exist in
    /// object stores and therefore cannot be operated upon. If the table was dropped successfully,
    /// the paths to the deleted files are returned, otherwise a [`ModelarDbStorageError`] is
    /// returned.
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

        // Remove the table from the cache.
        let delta_table_path = format!("{}/{}", self.location, table_path);
        self.delta_table_cache.remove(&delta_table_path);

        Ok(deleted_paths)
    }

    /// Truncate the Delta Lake table with `table_name` by deleting all rows in the table. If the
    /// rows could not be deleted, a [`ModelarDbStorageError`] is returned.
    pub async fn truncate_table(&self, table_name: &str) -> Result<()> {
        let delta_table_ops = self.delta_ops(table_name).await?;
        delta_table_ops.delete().await?;

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
        let delta_table_ops = self.delta_ops(table_name).await?;

        let retention_period_in_seconds =
            maybe_retention_period_in_seconds.unwrap_or(60 * 60 * 24 * 7);

        let retention_period = TimeDelta::new(retention_period_in_seconds as i64, 0).ok_or(
            ModelarDbStorageError::InvalidArgument(format!(
                "Retention period cannot be more than {MAX_RETENTION_PERIOD_IN_SECONDS} seconds."
            )),
        )?;

        delta_table_ops
            .vacuum()
            .with_retention_period(retention_period)
            .with_enforce_retention_duration(false)
            .await?;

        Ok(())
    }

    /// Save the created normal table to the Delta Lake. This consists of adding a row to the
    /// `normal_table_metadata` table with the `name` of the table. If the normal table metadata was
    /// saved, return [`Ok`], otherwise return [`ModelarDbStorageError`].
    pub async fn save_normal_table_metadata(&self, name: &str) -> Result<()> {
        self.write_columns_to_metadata_table(
            "normal_table_metadata",
            vec![Arc::new(StringArray::from(vec![name]))],
        )
        .await?;

        Ok(())
    }

    /// Save the created time series table to the Delta Lake. This includes adding a row to the
    /// `time_series_table_metadata` table and adding a row to the `time_series_table_field_columns`
    /// table for each field column.
    pub async fn save_time_series_table_metadata(
        &self,
        time_series_table_metadata: &TimeSeriesTableMetadata,
    ) -> Result<()> {
        // Convert the query schema to bytes, so it can be saved in the Delta Lake.
        let query_schema_bytes =
            try_convert_schema_to_bytes(&time_series_table_metadata.query_schema)?;

        // Add a new row in the time_series_table_metadata table to persist the time series table.
        self.write_columns_to_metadata_table(
            "time_series_table_metadata",
            vec![
                Arc::new(StringArray::from(vec![
                    time_series_table_metadata.name.clone(),
                ])),
                Arc::new(BinaryArray::from_vec(vec![&query_schema_bytes])),
            ],
        )
        .await?;

        // Add a row for each field column to the time_series_table_field_columns table.
        for (query_schema_index, field) in time_series_table_metadata
            .query_schema
            .fields()
            .iter()
            .enumerate()
        {
            if field.data_type() == &ArrowValue::DATA_TYPE {
                // Convert the generated column expression to bytes, if it exists.
                let maybe_generated_column_expr = match time_series_table_metadata
                    .generated_columns
                    .get(query_schema_index)
                {
                    Some(Some(generated_column)) => {
                        Some(generated_column.expr.to_bytes()?.to_vec())
                    }
                    _ => None,
                };

                // error_bounds matches schema and not query_schema to simplify looking up the error
                // bound during ingestion as it occurs far more often than creation of time series tables.
                let (error_bound_value, error_bound_is_relative) = if let Ok(schema_index) =
                    time_series_table_metadata.schema.index_of(field.name())
                {
                    match time_series_table_metadata.error_bounds[schema_index] {
                        ErrorBound::Absolute(value) => (value, false),
                        ErrorBound::Relative(value) => (value, true),
                        ErrorBound::Lossless => (0.0, false),
                    }
                } else {
                    (0.0, false)
                };

                // query_schema_index is simply cast as a time series table contains at most 32767 columns.
                self.write_columns_to_metadata_table(
                    "time_series_table_field_columns",
                    vec![
                        Arc::new(StringArray::from(vec![
                            time_series_table_metadata.name.clone(),
                        ])),
                        Arc::new(StringArray::from(vec![field.name().clone()])),
                        Arc::new(Int16Array::from(vec![query_schema_index as i16])),
                        Arc::new(Float32Array::from(vec![error_bound_value])),
                        Arc::new(BooleanArray::from(vec![error_bound_is_relative])),
                        Arc::new(BinaryArray::from_opt_vec(vec![
                            maybe_generated_column_expr.as_deref(),
                        ])),
                    ],
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Write `columns` to a Delta Lake table with `table_name`. Returns an updated [`DeltaTable`]
    /// version if the file was written successfully, otherwise returns [`ModelarDbStorageError`].
    pub async fn write_columns_to_metadata_table(
        &self,
        table_name: &str,
        columns: Vec<ArrayRef>,
    ) -> Result<DeltaTable> {
        let delta_table = self.metadata_delta_table(table_name).await?;
        let record_batch = RecordBatch::try_new(TableProvider::schema(&delta_table), columns)?;
        let delta_table_writer = self.normal_or_metadata_table_writer(delta_table).await?;
        self.write_record_batches_to_table(delta_table_writer, vec![record_batch])
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
        let delta_table = self.delta_table(table_name).await?;
        let delta_table_writer = self.normal_or_metadata_table_writer(delta_table).await?;
        self.write_record_batches_to_table(delta_table_writer, record_batches)
            .await
    }

    /// Write `compressed_segments` to a Delta Lake table for a time series table with `table_name`.
    /// Returns an updated [`DeltaTable`] if the file was written successfully, otherwise returns
    /// [`ModelarDbStorageError`].
    pub async fn write_compressed_segments_to_time_series_table(
        &self,
        table_name: &str,
        compressed_segments: Vec<RecordBatch>,
    ) -> Result<DeltaTable> {
        let delta_table = self.delta_table(table_name).await?;
        let delta_table_writer = self.time_series_table_writer(delta_table).await?;
        self.write_record_batches_to_table(delta_table_writer, compressed_segments)
            .await
    }

    /// Write `record_batches` to the `delta_table_writer` and commit. Returns an updated
    /// [`DeltaTable`] if all `record_batches` are written and committed successfully, otherwise it
    /// rolls back all writes done using `delta_table_writer` and returns [`ModelarDbStorageError`].
    async fn write_record_batches_to_table(
        &self,
        mut delta_table_writer: DeltaTableWriter,
        record_batches: Vec<RecordBatch>,
    ) -> Result<DeltaTable> {
        match delta_table_writer.write_all(&record_batches).await {
            Ok(_) => delta_table_writer.commit().await,
            Err(error) => {
                delta_table_writer.rollback().await?;
                Err(error)
            }
        }
    }

    /// Depending on the type of the table with `table_name`, drop either the normal table metadata
    /// or the time series table metadata from the Delta Lake. If the table does not exist or the
    /// metadata could not be dropped, [`ModelarDbStorageError`] is returned.
    pub async fn drop_table_metadata(&self, table_name: &str) -> Result<()> {
        if self.is_normal_table(table_name).await? {
            self.drop_normal_table_metadata(table_name).await
        } else if self.is_time_series_table(table_name).await? {
            self.drop_time_series_table_metadata(table_name).await
        } else {
            Err(ModelarDbStorageError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            )))
        }
    }

    /// Drop the metadata for the normal table with `table_name` from the `normal_table_metadata`
    /// table in the Delta Lake. If the metadata could not be dropped, [`ModelarDbStorageError`] is
    /// returned.
    async fn drop_normal_table_metadata(&self, table_name: &str) -> Result<()> {
        let delta_ops = self.metadata_delta_ops("normal_table_metadata").await?;

        delta_ops
            .delete()
            .with_predicate(col("table_name").eq(lit(table_name)))
            .await?;

        Ok(())
    }

    /// Drop the metadata for the time series table with `table_name` from the Delta Lake. This
    /// includes deleting a row from the `time_series_table_metadata` table and deleting a row from
    /// the `time_series_table_field_columns` table for each field column. If the metadata could not
    /// be dropped, [`ModelarDbStorageError`] is returned.
    async fn drop_time_series_table_metadata(&self, table_name: &str) -> Result<()> {
        // Delete the table metadata from the time_series_table_metadata table.
        self.metadata_delta_ops("time_series_table_metadata")
            .await?
            .delete()
            .with_predicate(col("table_name").eq(lit(table_name)))
            .await?;

        // Delete the column metadata from the time_series_table_field_columns table.
        self.metadata_delta_ops("time_series_table_field_columns")
            .await?
            .delete()
            .with_predicate(col("table_name").eq(lit(table_name)))
            .await?;

        Ok(())
    }

    /// Return the [`TimeSeriesTableMetadata`] of each time series table currently in the metadata
    /// Delta Lake. If the [`TimeSeriesTableMetadata`] cannot be retrieved,
    /// [`ModelarDbStorageError`] is returned.
    pub async fn time_series_table_metadata(&self) -> Result<Vec<Arc<TimeSeriesTableMetadata>>> {
        let sql = "SELECT table_name, query_schema FROM metadata.time_series_table_metadata";
        let batch = sql_and_concat(&self.session_context, sql).await?;

        let mut time_series_table_metadata: Vec<Arc<TimeSeriesTableMetadata>> = vec![];
        let table_name_array = modelardb_types::array!(batch, 0, StringArray);
        let query_schema_bytes_array = modelardb_types::array!(batch, 1, BinaryArray);

        for row_index in 0..batch.num_rows() {
            let table_name = table_name_array.value(row_index);
            let query_schema_bytes = query_schema_bytes_array.value(row_index);

            let metadata = self
                .time_series_table_metadata_row_to_time_series_table_metadata(
                    table_name,
                    query_schema_bytes,
                )
                .await?;

            time_series_table_metadata.push(Arc::new(metadata))
        }

        Ok(time_series_table_metadata)
    }

    /// Return the [`TimeSeriesTableMetadata`] for the time series table with `table_name` in the
    /// Delta Lake. If the [`TimeSeriesTableMetadata`] cannot be retrieved,
    /// [`ModelarDbStorageError`] is returned.
    pub async fn time_series_table_metadata_for_time_series_table(
        &self,
        table_name: &str,
    ) -> Result<TimeSeriesTableMetadata> {
        let sql = format!(
            "SELECT table_name, query_schema FROM metadata.time_series_table_metadata WHERE table_name = '{table_name}'"
        );
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        if batch.num_rows() == 0 {
            return Err(ModelarDbStorageError::InvalidArgument(format!(
                "No metadata for time series table named '{table_name}'."
            )));
        }

        let table_name_array = modelardb_types::array!(batch, 0, StringArray);
        let query_schema_bytes_array = modelardb_types::array!(batch, 1, BinaryArray);

        let table_name = table_name_array.value(0);
        let query_schema_bytes = query_schema_bytes_array.value(0);

        self.time_series_table_metadata_row_to_time_series_table_metadata(
            table_name,
            query_schema_bytes,
        )
        .await
    }

    /// Return [`TimeSeriesTableMetadata`] for the time series table with `table_name` if it exists,
    /// is registered with Apache DataFusion, and is a time series table.
    pub async fn time_series_table_metadata_for_registered_time_series_table(
        &self,
        table_name: &str,
    ) -> Option<Arc<TimeSeriesTableMetadata>> {
        let table_provider = self.session_context.table_provider(table_name).await.ok()?;
        crate::maybe_table_provider_to_time_series_table_metadata(table_provider)
    }

    /// Convert a row from the table "time_series_table_metadata" to an instance of
    /// [`TimeSeriesTableMetadata`]. Returns [`ModelarDbStorageError`] if a time_series table with
    /// `table_name` does not exist or the bytes in `query_schema_bytes` are not a valid schema.
    async fn time_series_table_metadata_row_to_time_series_table_metadata(
        &self,
        table_name: &str,
        query_schema_bytes: &[u8],
    ) -> Result<TimeSeriesTableMetadata> {
        let query_schema = try_convert_bytes_to_schema(query_schema_bytes.into())?;

        let error_bounds = self
            .error_bounds(table_name, query_schema.fields().len())
            .await?;

        let df_query_schema = query_schema.clone().to_dfschema()?;
        let generated_columns = self.generated_columns(table_name, &df_query_schema).await?;

        TimeSeriesTableMetadata::try_new(
            table_name.to_owned(),
            Arc::new(query_schema),
            error_bounds,
            generated_columns,
        )
        .map_err(|error| error.into())
    }

    /// Return the error bounds for the columns in the time series table with `table_name`. If a
    /// time series table with `table_name` does not exist, [`ModelarDbStorageError`] is returned.
    async fn error_bounds(
        &self,
        table_name: &str,
        query_schema_columns: usize,
    ) -> Result<Vec<ErrorBound>> {
        let sql = format!(
            "SELECT column_index, error_bound_value, error_bound_is_relative
             FROM metadata.time_series_table_field_columns
             WHERE table_name = '{table_name}'
             ORDER BY column_index"
        );
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        let mut column_to_error_bound = vec![ErrorBound::Lossless; query_schema_columns];

        let column_index_array = modelardb_types::array!(batch, 0, Int16Array);
        let error_bound_value_array = modelardb_types::array!(batch, 1, Float32Array);
        let error_bound_is_relative_array = modelardb_types::array!(batch, 2, BooleanArray);

        for row_index in 0..batch.num_rows() {
            let error_bound_index = column_index_array.value(row_index);
            let error_bound_value = error_bound_value_array.value(row_index);
            let error_bound_is_relative = error_bound_is_relative_array.value(row_index);

            if error_bound_value != 0.0 {
                let error_bound = if error_bound_is_relative {
                    ErrorBound::try_new_relative(error_bound_value)
                } else {
                    ErrorBound::try_new_absolute(error_bound_value)
                }?;

                column_to_error_bound[error_bound_index as usize] = error_bound;
            }
        }

        Ok(column_to_error_bound)
    }

    /// Return the generated columns for the time series table with `table_name` and `df_schema`. If
    /// a time series table with `table_name` does not exist, [`ModelarDbStorageError`] is returned.
    async fn generated_columns(
        &self,
        table_name: &str,
        df_schema: &DFSchema,
    ) -> Result<Vec<Option<GeneratedColumn>>> {
        let sql = format!(
            "SELECT column_index, generated_column_expr
             FROM metadata.time_series_table_field_columns
             WHERE table_name = '{table_name}'
             ORDER BY column_index"
        );
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        let mut generated_columns = vec![None; df_schema.fields().len()];

        let column_index_array = modelardb_types::array!(batch, 0, Int16Array);
        let generated_column_expr_array = modelardb_types::array!(batch, 1, BinaryArray);

        for row_index in 0..batch.num_rows() {
            let generated_column_index = column_index_array.value(row_index);
            let expr_bytes = generated_column_expr_array.value(row_index);

            // If generated_column_expr is null, it is saved as empty bytes in the column values.
            if !expr_bytes.is_empty() {
                let expr = Expr::from_bytes(expr_bytes)?;
                let generated_column = GeneratedColumn::try_from_expr(expr, df_schema)?;

                generated_columns[generated_column_index as usize] = Some(generated_column);
            }
        }

        Ok(generated_columns)
    }
}

/// Functionality for transactionally writing [`RecordBatches`](RecordBatch) to a Delta table stored
/// in an object store.
pub struct DeltaTableWriter {
    /// Delta table that all of the record batches will be written to.
    delta_table: DeltaTable,
    /// Checker that ensures all of the record batches match the table.
    delta_data_checker: DeltaDataChecker,
    /// Write operation that will be committed to the Delta table.
    delta_operation: DeltaOperation,
    /// Unique identifier for this write operation to the Delta table.
    operation_id: Uuid,
    /// Writes record batches to the Delta table as Apache Parquet files.
    delta_writer: DeltaWriter,
}

impl DeltaTableWriter {
    /// Create a new [`DeltaTableWriter`]. Returns a [`ModelarDbStorageError`] if the state of the
    /// Delta table cannot be loaded from `delta_table`.
    pub fn try_new(
        delta_table: DeltaTable,
        partition_columns: Vec<String>,
        writer_properties: WriterProperties,
    ) -> Result<Self> {
        // Checker for if record batches match the table’s invariants, constraints, and nullability.
        let delta_table_state = delta_table.snapshot()?;
        let snapshot = delta_table_state.snapshot();
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

        // A UUID version 4 is used as the operation id to match the existing Operation trait in the
        // deltalake crate as it is pub(trait) and thus cannot be used directly in DeltaTableWriter.
        let operation_id = Uuid::new_v4();

        // Writer that will write the record batches.
        let object_store = delta_table.log_store().object_store(Some(operation_id));
        let table_schema: Arc<Schema> = TableProvider::schema(&delta_table);
        let num_indexed_cols = DataSkippingNumIndexedCols::NumColumns(table_schema.fields.len() as u64);
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

    /// Write `record_batch` to the Delta table. Returns a [`ModelarDbStorageError`] if the
    /// [`RecordBatches`](RecordBatch) does not match the schema of the Delta table or if the
    /// writing fails.
    pub async fn write(&mut self, record_batch: &RecordBatch) -> Result<()> {
        self.delta_data_checker.check_batch(record_batch).await?;
        self.delta_writer.write(record_batch).await?;
        Ok(())
    }

    /// Write all `record_batches` to the Delta table. Returns a [`ModelarDbStorageError`] if one of
    /// the [`RecordBatches`](RecordBatch) does not match the schema of the Delta table or if the
    /// writing fails.
    pub async fn write_all(&mut self, record_batches: &[RecordBatch]) -> Result<()> {
        for record_batch in record_batches {
            self.write(record_batch).await?;
        }
        Ok(())
    }

    /// Consume the [`DeltaTableWriter`], finish the writing, and commit the files that have been
    /// written to the log. If an error occurs before the commit is finished, the already written
    /// files are deleted if possible. Returns a [`ModelarDbStorageError`] if an error occurs when
    /// finishing the writing, committing the files that have been written, deleting the written
    /// files, or updating the [`DeltaTable`].
    pub async fn commit(mut self) -> Result<DeltaTable> {
        // Write the remaining buffered files.
        let added_files = self.delta_writer.close().await?;

        // Clone added_files in case of rollback.
        let actions = added_files
            .clone()
            .into_iter()
            .map(Action::Add)
            .collect::<Vec<Action>>();

        // Prepare all inputs to the commit.
        let object_store = self.delta_table.object_store();
        let commit_properties = CommitProperties::default();
        let table_data = match self.delta_table.snapshot() {
            Ok(table_data) => table_data,
            Err(delta_table_error) => {
                delete_added_files(&object_store, added_files).await?;
                return Err(ModelarDbStorageError::DeltaLake(delta_table_error));
            }
        };
        let log_store = self.delta_table.log_store();

        // Construct the commit to be written.
        let commit_builder = CommitBuilder::from(commit_properties)
            .with_actions(actions)
            .with_operation_id(self.operation_id)
            .build(Some(table_data), log_store, self.delta_operation);

        // Write the commit to the Delta table.
        let _finalized_commit = match commit_builder.await {
            Ok(finalized_commit) => finalized_commit,
            Err(delta_table_error) => {
                delete_added_files(&object_store, added_files).await?;
                return Err(ModelarDbStorageError::DeltaLake(delta_table_error));
            }
        };

        // Return Delta table with the commit.
        self.delta_table.load().await?;
        Ok(self.delta_table)
    }

    /// Consume the [`DeltaTableWriter`], abort the writing, and delete all of the files that have
    /// already been written. Returns a [`ModelarDbStorageError`] if an error occurs when aborting
    /// the writing or deleting the files that have already been written. Rollback is not called
    /// automatically as drop() is not async and async_drop() is not yet a stable API.
    pub async fn rollback(self) -> Result<DeltaTable> {
        let object_store = self.delta_table.object_store();
        let added_files = self.delta_writer.close().await?;
        delete_added_files(&object_store, added_files).await?;
        Ok(self.delta_table)
    }
}

/// Delete the `added_files` from `object_store`. Returns a [`ModelarDbStorageError`] if a file
/// could not be deleted. It is a function instead of a method on [`DeltaTableWriter`] so it can be
/// called by [`DeltaTableWriter`] after the [`DeltaWriter`] is closed without lifetime issues.
async fn delete_added_files(object_store: &dyn ObjectStore, added_files: Vec<Add>) -> Result<()> {
    for add_file in added_files {
        let path: Path = Path::from(add_file.path);
        object_store.delete(&path).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{ArrowPrimitiveType, Field};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::ScalarValue::Int64;
    use datafusion::logical_expr::Expr::Literal;
    use modelardb_test::table as test;
    use modelardb_types::types::ArrowTimestamp;
    use tempfile::TempDir;

    // Tests for DataFolder.
    #[tokio::test]
    async fn test_create_metadata_data_folder_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        assert!(
            data_folder
                .session_context
                .sql("SELECT table_name FROM metadata.normal_table_metadata")
                .await
                .is_ok()
        );

        assert!(
            data_folder
                .session_context
                .sql("SELECT table_name, query_schema FROM metadata.time_series_table_metadata")
                .await
                .is_ok()
        );

        assert!(data_folder
            .session_context
            .sql("SELECT table_name, column_name, column_index, error_bound_value, error_bound_is_relative, \
                  generated_column_expr FROM metadata.time_series_table_field_columns")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_normal_table_is_normal_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_normal_tables().await;
        assert!(data_folder.is_normal_table("normal_table_1").await.unwrap());
    }

    #[tokio::test]
    async fn test_time_series_table_is_not_normal_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_time_series_table().await;
        assert!(
            !data_folder
                .is_normal_table(test::TIME_SERIES_TABLE_NAME)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_time_series_table_is_time_series_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_time_series_table().await;
        assert!(
            data_folder
                .is_time_series_table(test::TIME_SERIES_TABLE_NAME)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_normal_table_is_not_time_series_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_normal_tables().await;
        assert!(
            !data_folder
                .is_time_series_table("normal_table_1")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_table_names() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_normal_tables().await;

        let time_series_table_metadata = test::time_series_table_metadata();
        data_folder
            .save_time_series_table_metadata(&time_series_table_metadata)
            .await
            .unwrap();

        let table_names = data_folder.table_names().await.unwrap();
        assert_eq!(
            table_names,
            vec![
                "normal_table_2",
                "normal_table_1",
                test::TIME_SERIES_TABLE_NAME
            ]
        );
    }

    #[tokio::test]
    async fn test_normal_table_names() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_normal_tables().await;

        let normal_table_names = data_folder.normal_table_names().await.unwrap();
        assert_eq!(normal_table_names, vec!["normal_table_2", "normal_table_1"]);
    }

    #[tokio::test]
    async fn test_time_series_table_names() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_time_series_table().await;

        let time_series_table_names = data_folder.time_series_table_names().await.unwrap();
        assert_eq!(time_series_table_names, vec![test::TIME_SERIES_TABLE_NAME]);
    }

    #[tokio::test]
    async fn test_save_normal_table_metadata() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_normal_tables().await;

        // Retrieve the normal table from the Delta Lake.
        let sql = "SELECT table_name FROM metadata.normal_table_metadata ORDER BY table_name";
        let batch = sql_and_concat(&data_folder.session_context, sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec!["normal_table_1", "normal_table_2"])
        );
    }

    #[tokio::test]
    async fn test_save_time_series_table_metadata() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_time_series_table().await;

        // Check that a row has been added to the time_series_table_metadata table.
        let sql = "SELECT table_name, query_schema FROM metadata.time_series_table_metadata";
        let batch = sql_and_concat(&data_folder.session_context, sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![test::TIME_SERIES_TABLE_NAME])
        );
        assert_eq!(
            **batch.column(1),
            BinaryArray::from_vec(vec![
                &try_convert_schema_to_bytes(&test::time_series_table_metadata().query_schema)
                    .unwrap()
            ])
        );

        // Check that a row has been added to the time_series_table_field_columns table for each field column.
        let sql = "SELECT table_name, column_name, column_index, error_bound_value, error_bound_is_relative, \
                   generated_column_expr FROM metadata.time_series_table_field_columns ORDER BY column_name";
        let batch = sql_and_concat(&data_folder.session_context, sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![
                test::TIME_SERIES_TABLE_NAME,
                test::TIME_SERIES_TABLE_NAME
            ])
        );
        assert_eq!(
            **batch.column(1),
            StringArray::from(vec!["field_1", "field_2"])
        );
        assert_eq!(**batch.column(2), Int16Array::from(vec![1, 2]));
        assert_eq!(**batch.column(3), Float32Array::from(vec![1.0, 5.0]));
        assert_eq!(**batch.column(4), BooleanArray::from(vec![false, true]));
        assert_eq!(
            **batch.column(5),
            BinaryArray::from_opt_vec(vec![None, None])
        );
    }

    #[tokio::test]
    async fn test_drop_normal_table_metadata() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_normal_tables().await;

        data_folder
            .drop_table_metadata("normal_table_2")
            .await
            .unwrap();

        // Verify that normal_table_2 was deleted from the normal_table_metadata table.
        let sql = "SELECT table_name FROM metadata.normal_table_metadata";
        let batch = sql_and_concat(&data_folder.session_context, sql)
            .await
            .unwrap();

        assert_eq!(**batch.column(0), StringArray::from(vec!["normal_table_1"]));
    }

    #[tokio::test]
    async fn test_drop_time_series_table_metadata() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_time_series_table().await;

        data_folder
            .drop_table_metadata(test::TIME_SERIES_TABLE_NAME)
            .await
            .unwrap();

        // Verify that the time series table was deleted from the time_series_table_metadata table.
        let sql = "SELECT table_name FROM metadata.time_series_table_metadata";
        let batch = sql_and_concat(&data_folder.session_context, sql)
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);

        // Verify that the field columns were deleted from the time_series_table_field_columns table.
        let sql = "SELECT table_name FROM metadata.time_series_table_field_columns";
        let batch = sql_and_concat(&data_folder.session_context, sql)
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_drop_table_metadata_for_missing_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_normal_tables().await;

        assert!(
            data_folder
                .drop_table_metadata("missing_table")
                .await
                .is_err()
        );
    }

    async fn create_data_folder_and_save_normal_tables() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        data_folder
            .save_normal_table_metadata("normal_table_1")
            .await
            .unwrap();

        data_folder
            .save_normal_table_metadata("normal_table_2")
            .await
            .unwrap();

        (temp_dir, data_folder)
    }

    #[tokio::test]
    async fn test_time_series_table_metadata() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_time_series_table().await;

        let time_series_table_metadata = data_folder.time_series_table_metadata().await.unwrap();

        assert_eq!(
            time_series_table_metadata.first().unwrap().name,
            test::time_series_table_metadata().name,
        );
    }

    #[tokio::test]
    async fn test_time_series_table_metadata_for_existing_time_series_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_time_series_table().await;

        let time_series_table_metadata = data_folder
            .time_series_table_metadata_for_time_series_table(test::TIME_SERIES_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(
            time_series_table_metadata.name,
            test::time_series_table_metadata().name,
        );
    }

    #[tokio::test]
    async fn test_time_series_table_metadata_for_missing_time_series_table() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_time_series_table().await;

        let time_series_table_metadata = data_folder
            .time_series_table_metadata_for_time_series_table("missing_table")
            .await;

        assert!(time_series_table_metadata.is_err());
    }

    #[tokio::test]
    async fn test_error_bound() {
        let (_temp_dir, data_folder) = create_data_folder_and_save_time_series_table().await;

        let error_bounds = data_folder
            .error_bounds(test::TIME_SERIES_TABLE_NAME, 4)
            .await
            .unwrap();

        let values: Vec<f32> = error_bounds
            .iter()
            .map(|error_bound| match error_bound {
                ErrorBound::Absolute(value) => *value,
                ErrorBound::Relative(value) => *value,
                ErrorBound::Lossless => 0.0,
            })
            .collect();

        assert_eq!(values, &[0.0, 1.0, 5.0, 0.0]);
    }

    #[tokio::test]
    async fn test_generated_columns() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let query_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
            Field::new("tag", DataType::Utf8, false),
            Field::new("generated_column_1", ArrowValue::DATA_TYPE, false),
            Field::new("generated_column_2", ArrowValue::DATA_TYPE, false),
        ]));

        let error_bounds = vec![ErrorBound::Lossless; query_schema.fields.len()];

        let plus_one_column = Some(GeneratedColumn {
            expr: col("field_1") + Literal(Int64(Some(1)), None),
            source_columns: vec![1],
        });

        let addition_column = Some(GeneratedColumn {
            expr: col("field_1") + col("field_2"),
            source_columns: vec![1, 2],
        });

        let expected_generated_columns =
            vec![None, None, None, None, plus_one_column, addition_column];

        let time_series_table_metadata = TimeSeriesTableMetadata::try_new(
            "generated_columns_table".to_owned(),
            query_schema,
            error_bounds,
            expected_generated_columns.clone(),
        )
        .unwrap();

        data_folder
            .save_time_series_table_metadata(&time_series_table_metadata)
            .await
            .unwrap();

        let df_schema = time_series_table_metadata
            .query_schema
            .to_dfschema()
            .unwrap();
        let generated_columns = data_folder
            .generated_columns("generated_columns_table", &df_schema)
            .await
            .unwrap();

        assert_eq!(
            generated_columns[0..generated_columns.len() - 1],
            expected_generated_columns[0..expected_generated_columns.len() - 1]
        );

        // Sort the source columns to ensure the order is consistent.
        let mut last_generated_column = generated_columns.last().unwrap().clone().unwrap();
        last_generated_column.source_columns.sort();

        assert_eq!(
            &Some(last_generated_column),
            expected_generated_columns.last().unwrap()
        );
    }

    async fn create_data_folder_and_save_time_series_table() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        // Save a time series table to the Delta Lake.
        let time_series_table_metadata = test::time_series_table_metadata();
        data_folder
            .save_time_series_table_metadata(&time_series_table_metadata)
            .await
            .unwrap();

        (temp_dir, data_folder)
    }
}
