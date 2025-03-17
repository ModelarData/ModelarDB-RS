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

//! Operations for reading from and writing to ModelarDB data folders.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::path::Path as StdPath;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::array::{Float32Array, StringArray};
use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::sorts::sort;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, common};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use modelardb_storage::delta_lake::{DeltaLake, DeltaTableWriter};
use modelardb_storage::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_storage::metadata::table_metadata_manager::TableMetadataManager;
use modelardb_types::types::TimestampArray;

use crate::error::{ModelarDbEmbeddedError, Result};
use crate::modelardb::{ModelarDB, generate_read_model_table_sql, try_new_model_table_metadata};
use crate::{Aggregate, TableType};

/// [`DataSink`] that rejects INSERT statements passed to [`DataFolder.read()`].
struct DataFolderDataSink {
    /// The schema of the data sink is empty since it rejects everything.
    schema: Arc<Schema>,
}

impl DataFolderDataSink {
    fn new() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
        }
    }
}

#[async_trait]
impl DataSink for DataFolderDataSink {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the [`DataSink's`](DataSink) schema.
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Return [`None`] as the [`DataSink`] collects no metrics.
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Return a [`DataFusionError`] as INSERT is not supported.
    async fn write_all(
        &self,
        _data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> StdResult<u64, DataFusionError> {
        Err(DataFusionError::Execution(
            "INSERT is not supported, use DataFolder.write() to insert data.".to_owned(),
        ))
    }
}

impl Debug for DataFolderDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "DataFolderDataSink")
    }
}

impl DisplayAs for DataFolderDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "DataFolderDataSink")
    }
}

/// Provides access to modelardb_embedded's components.
pub struct DataFolder {
    /// Delta Lake for storing metadata and data in Apache Parquet files.
    delta_lake: DeltaLake,
    /// Metadata manager for providing access to metadata related to tables. It is stored in an
    /// [`Arc`] because it is shared with each of the model tables for use in query planning.
    table_metadata_manager: Arc<TableMetadataManager>,
    /// Context providing access to a specific session of Apache DataFusion.
    session_context: SessionContext,
}

impl DataFolder {
    /// Creates a [`DataFolder`] that manages data in memory and returns it. If the metadata tables
    /// could not be created, [`ModelarDbEmbeddedError`] is returned.
    pub async fn open_memory() -> Result<Self> {
        let delta_lake = DeltaLake::new_in_memory();
        let table_metadata_manager = Arc::new(TableMetadataManager::try_new_in_memory().await?);

        Self::try_new_and_register_tables(delta_lake, table_metadata_manager).await
    }

    /// Creates a [`DataFolder`] that manages data in the local folder at `data_folder_path` and
    /// returns it. If the folder does not exist and could not be created or the metadata tables
    /// could not be created, [`ModelarDbEmbeddedError`] is returned.
    pub async fn open_local(data_folder_path: &StdPath) -> Result<Self> {
        let delta_lake = DeltaLake::try_from_local_path(data_folder_path)?;

        let table_metadata_manager =
            Arc::new(TableMetadataManager::try_from_path(data_folder_path).await?);

        Self::try_new_and_register_tables(delta_lake, table_metadata_manager).await
    }

    /// Creates a [`DataFolder`] that manages data in an object store with an S3-compatible API and
    /// returns it. If a connection to the object store could not be established or the metadata
    /// tables could not be created, [`ModelarDbEmbeddedError`] is returned.
    pub async fn open_s3(
        endpoint: String,
        bucket_name: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> Result<Self> {
        // Register the S3 storage handlers to allow the use of Amazon S3 object stores. This is
        // required at runtime to initialize the S3 storage implementation in the deltalake_aws
        // storage subcrate. It is safe to call this function multiple times as the handlers are
        // stored in a DashMap, thus, the handlers are simply overwritten with the same each time.
        deltalake::aws::register_handlers(None);

        // Construct data folder.
        let delta_lake = DeltaLake::try_from_s3_configuration(
            endpoint.clone(),
            bucket_name.clone(),
            access_key_id.clone(),
            secret_access_key.clone(),
        )?;

        let table_metadata_manager = Arc::new(
            TableMetadataManager::try_from_s3_configuration(
                endpoint,
                bucket_name,
                access_key_id,
                secret_access_key,
            )
            .await?,
        );

        Self::try_new_and_register_tables(delta_lake, table_metadata_manager).await
    }

    /// Creates a [`DataFolder`] that manages data in an object store with an Azure-compatible API
    /// and returns it. If a connection to the object store could not be established or the metadata
    /// tables could not be created, [`ModelarDbEmbeddedError`] is returned.
    pub async fn open_azure(
        account_name: String,
        access_key: String,
        container_name: String,
    ) -> Result<Self> {
        let delta_lake = DeltaLake::try_from_azure_configuration(
            account_name.clone(),
            access_key.clone(),
            container_name.clone(),
        )?;

        let table_metadata_manager = Arc::new(
            TableMetadataManager::try_from_azure_configuration(
                account_name,
                access_key,
                container_name,
            )
            .await?,
        );

        Self::try_new_and_register_tables(delta_lake, table_metadata_manager).await
    }

    /// Create a [`DataFolder`], register all normal tables and model tables in it with its
    /// [`SessionContext`], and return it. If the tables could not be registered,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn try_new_and_register_tables(
        delta_lake: DeltaLake,
        table_metadata_manager: Arc<TableMetadataManager>,
    ) -> Result<Self> {
        // Construct data folder.
        let session_context = modelardb_storage::create_session_context();

        let data_folder = DataFolder {
            delta_lake,
            table_metadata_manager,
            session_context,
        };

        // Register normal tables.
        let data_sink = Arc::new(DataFolderDataSink::new());

        for normal_table_name in data_folder
            .table_metadata_manager
            .normal_table_names()
            .await?
        {
            let delta_table = data_folder
                .delta_lake
                .delta_table(&normal_table_name)
                .await?;

            modelardb_storage::register_normal_table(
                &data_folder.session_context,
                &normal_table_name,
                delta_table,
                data_sink.clone(),
            )?;
        }

        // Register model tables.
        for metadata in data_folder
            .table_metadata_manager
            .model_table_metadata()
            .await?
        {
            let delta_table = data_folder.delta_lake.delta_table(&metadata.name).await?;

            modelardb_storage::register_model_table(
                &data_folder.session_context,
                delta_table,
                metadata,
                data_sink.clone(),
            )?;
        }

        Ok(data_folder)
    }

    /// Compress the `uncompressed_data` from the table with `model_table_metadata` and return the
    /// resulting segments.
    pub async fn compress_all(
        &self,
        model_table_metadata: &ModelTableMetadata,
        uncompressed_data: &RecordBatch,
    ) -> Result<Vec<RecordBatch>> {
        // Sort by all tags and then time to simplify splitting the data into time series.
        let sorted_uncompressed_data =
            sort_record_batch_by_tags_and_time(model_table_metadata, uncompressed_data)?;

        // Split the sorted uncompressed data into time series and compress them separately.
        let mut compressed_data = vec![];

        let tag_column_arrays: Vec<&StringArray> = model_table_metadata
            .tag_column_indices
            .iter()
            .map(|index| modelardb_types::array!(sorted_uncompressed_data, *index, StringArray))
            .collect();

        let mut tag_values = Vec::with_capacity(tag_column_arrays.len());
        for tag_column_array in &tag_column_arrays {
            tag_values.push(tag_column_array.value(0).to_owned());
        }

        // The index of the first data point of each time series must be stored so slices
        // containing only data points for each time series can be extracted and compressed.
        let mut row_index_start = 0;
        for row_index in 0..sorted_uncompressed_data.num_rows() {
            // If any of the tags differ, the data point is from a new time series.
            let mut is_new_time_series = false;
            for tag_column_index in 0..tag_column_arrays.len() {
                is_new_time_series |= tag_values[tag_column_index]
                    != tag_column_arrays[tag_column_index].value(row_index);
            }

            if is_new_time_series {
                let time_series_length = row_index - row_index_start;
                let uncompressed_time_series =
                    sorted_uncompressed_data.slice(row_index_start, time_series_length);

                self.compress(
                    model_table_metadata,
                    &uncompressed_time_series,
                    &tag_values,
                    &mut compressed_data,
                )
                .await?;

                for (tag_column_index, tag_column_array) in tag_column_arrays.iter().enumerate() {
                    tag_values[tag_column_index] = tag_column_array.value(row_index).to_owned();
                }

                row_index_start = row_index;
            }
        }

        let time_series_length = sorted_uncompressed_data.num_rows() - row_index_start;
        let uncompressed_time_series =
            sorted_uncompressed_data.slice(row_index_start, time_series_length);

        self.compress(
            model_table_metadata,
            &uncompressed_time_series,
            &tag_values,
            &mut compressed_data,
        )
        .await?;

        Ok(compressed_data)
    }

    /// Compress the field columns in `uncompressed_time_series` from the table with
    /// `model_table_metadata` and append the result to `compressed_data`. It is assumed that all
    /// data points in `uncompressed_time_series` have the same tags as in `tag_values`.
    async fn compress(
        &self,
        model_table_metadata: &ModelTableMetadata,
        uncompressed_time_series: &RecordBatch,
        tag_values: &[String],
        compressed_data: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        let uncompressed_timestamps = modelardb_types::array!(
            uncompressed_time_series,
            model_table_metadata.timestamp_column_index,
            TimestampArray
        );

        for field_column_index in &model_table_metadata.field_column_indices {
            let uncompressed_values = modelardb_types::array!(
                uncompressed_time_series,
                *field_column_index,
                Float32Array
            );

            let error_bound = model_table_metadata.error_bounds[*field_column_index];

            // unwrap() is safe as uncompressed_timestamps and uncompressed_values have the same length.
            let compressed_time_series = modelardb_compression::try_compress(
                uncompressed_timestamps,
                uncompressed_values,
                error_bound,
                model_table_metadata.compressed_schema.clone(),
                tag_values.to_vec(),
                *field_column_index as i16,
            )
            .unwrap();

            compressed_data.push(compressed_time_series);
        }

        Ok(())
    }

    /// Create a writer for writing multiple batches of data to the table with the table name in
    /// `table_name`. If the table does not exist or a writer for it could not be created, a
    /// [`ModelarDbEmbeddedError`] is returned.
    pub async fn writer(&self, table_name: &str) -> Result<DeltaTableWriter> {
        let delta_table = self.delta_lake.delta_table(table_name).await?;
        if self.model_table_metadata(table_name).await.is_some() {
            self.delta_lake
                .model_table_writer(delta_table)
                .await
                .map_err(|error| error.into())
        } else {
            self.delta_lake
                .normal_or_metadata_table_writer(delta_table)
                .await
                .map_err(|error| error.into())
        }
    }

    /// Return the schema of the table with the name in `table_name` if it is a normal table. If the
    /// table does not exist or the table is not a normal table, return [`None`].
    async fn normal_table_schema(&self, table_name: &str) -> Option<Schema> {
        if self
            .table_metadata_manager
            .is_normal_table(table_name)
            .await
            .is_ok_and(|is_normal_table| is_normal_table)
        {
            self.delta_lake
                .delta_table(table_name)
                .await
                .expect("Delta Lake table should exist if the table is in the metadata Delta Lake.")
                .get_schema()
                .expect("Delta Lake table should be loaded and metadata should be in the log.")
                .try_into()
                .ok()
        } else {
            None
        }
    }

    /// Return [`ModelTableMetadata`] for the table with `table_name` if it exists, is registered
    /// with Apache DataFusion, and is a model table.
    pub async fn model_table_metadata(&self, table_name: &str) -> Option<Arc<ModelTableMetadata>> {
        let table_provider = self.session_context.table_provider(table_name).await.ok()?;
        modelardb_storage::maybe_table_provider_to_model_table_metadata(table_provider)
    }
}

#[async_trait]
impl ModelarDB for DataFolder {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a table with the name in `table_name` and the information in `table_type`. If the
    /// table could not be created or registered with Apache DataFusion, [`ModelarDbEmbeddedError`]
    /// is returned.
    async fn create(&mut self, table_name: &str, table_type: TableType) -> Result<()> {
        match table_type {
            TableType::NormalTable(schema) => {
                let delta_table = self
                    .delta_lake
                    .create_normal_table(table_name, &schema)
                    .await?;

                self.table_metadata_manager
                    .save_normal_table_metadata(table_name)
                    .await?;

                let data_sink = Arc::new(DataFolderDataSink::new());

                modelardb_storage::register_normal_table(
                    &self.session_context,
                    table_name,
                    delta_table,
                    data_sink.clone(),
                )?;
            }
            TableType::ModelTable(schema, error_bounds, generated_columns) => {
                let model_table_metadata = Arc::new(try_new_model_table_metadata(
                    table_name,
                    schema,
                    error_bounds,
                    generated_columns,
                )?);

                let delta_table = self
                    .delta_lake
                    .create_model_table(&model_table_metadata)
                    .await?;

                self.table_metadata_manager
                    .save_model_table_metadata(&model_table_metadata)
                    .await?;

                let data_sink = Arc::new(DataFolderDataSink::new());

                modelardb_storage::register_model_table(
                    &self.session_context,
                    delta_table,
                    model_table_metadata,
                    data_sink.clone(),
                )?
            }
        }

        Ok(())
    }

    /// Returns the name of all the tables. If the table names could not be retrieved from the
    /// metadata Delta Lake, [`ModelarDbEmbeddedError`] is returned.
    async fn tables(&mut self) -> Result<Vec<String>> {
        self.table_metadata_manager
            .table_names()
            .await
            .map_err(|error| error.into())
    }

    /// Returns the schema of the table with the name in `table_name`. If the table does not exist,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn schema(&mut self, table_name: &str) -> Result<Schema> {
        if let Some(model_table_metadata) = self.model_table_metadata(table_name).await {
            Ok((*model_table_metadata.query_schema).to_owned())
        } else if let Some(normal_table_schema) = self.normal_table_schema(table_name).await {
            Ok(normal_table_schema)
        } else {
            Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "{table_name} is not a table."
            )))
        }
    }

    /// Writes the data in `uncompressed_data` to the table with the table name in `table_name`. If
    /// `uncompressed_data` is empty, the schema of `uncompressed_data` does not match the schema of
    /// the table, or the data could not be written to the table, [`ModelarDbEmbeddedError`] is
    /// returned.
    async fn write(&mut self, table_name: &str, uncompressed_data: RecordBatch) -> Result<()> {
        if uncompressed_data.num_rows() == 0 {
            return Err(ModelarDbEmbeddedError::InvalidArgument(
                "The uncompressed data is empty.".to_owned(),
            ));
        }

        let schema_mismatch_error = ModelarDbEmbeddedError::InvalidArgument(format!(
            "The uncompressed data does not match the schema for the table: {table_name}."
        ));

        if let Some(model_table_metadata) = self.model_table_metadata(table_name).await {
            // Model table.
            if !schemas_are_compatible(&uncompressed_data.schema(), &model_table_metadata.schema) {
                return Err(schema_mismatch_error);
            }

            let compressed_data = self
                .compress_all(&model_table_metadata, &uncompressed_data)
                .await?;

            self.delta_lake
                .write_compressed_segments_to_model_table(table_name, compressed_data)
                .await?;
        } else if let Some(normal_table_schema) = self.normal_table_schema(table_name).await {
            // Normal table.
            if !schemas_are_compatible(&uncompressed_data.schema(), &normal_table_schema) {
                return Err(schema_mismatch_error);
            }

            self.delta_lake
                .write_record_batches_to_normal_table(table_name, vec![uncompressed_data])
                .await?;
        } else {
            return Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "{table_name} is not a table."
            )));
        }

        Ok(())
    }

    /// Reads data from the model table with the table name in `table_name` and returns it as a
    /// [`RecordBatchStream`]. The remaining parameters optionally specify which subset of the data
    /// to read. If the table is not a model table or the data could not be read,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn read_model_table(
        &mut self,
        table_name: &str,
        columns: &[(String, Aggregate)],
        group_by: &[String],
        maybe_start_time: Option<&str>,
        maybe_end_time: Option<&str>,
        tags: HashMap<String, String>,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send>>> {
        // DataFolder.read() interface is designed for model tables.
        let model_table_medata =
            if let Some(model_table_metadata) = self.model_table_metadata(table_name).await {
                model_table_metadata
            } else {
                return Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                    "{table_name} is not a model table."
                )));
            };

        let sql = generate_read_model_table_sql(
            table_name,
            &model_table_medata.query_schema,
            columns,
            group_by,
            maybe_start_time,
            maybe_end_time,
            tags,
        );

        self.read(&sql).await
    }

    /// Copy the data from the model table with the name in `from_table_name` in `self` to the model
    /// table with the name in `to_table_name` in `to_modelardb`. Note that duplicate data is not
    /// deleted. If `to_modelardb` is not a data folder, the schemas of the model tables do not
    /// match, or the data could not be copied, [`ModelarDbEmbeddedError`] is returned.
    #[allow(clippy::too_many_arguments)]
    async fn copy_model_table(
        &self,
        from_table_name: &str,
        to_modelardb: &dyn ModelarDB,
        to_table_name: &str,
        maybe_start_time: Option<&str>,
        maybe_end_time: Option<&str>,
        _tags: HashMap<String, String>, // TODO: Support tag-based filtering.
    ) -> Result<()> {
        let to_data_folder = to_modelardb
            .as_any()
            .downcast_ref::<DataFolder>()
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument(
                    "to_modelardb is not a data folder.".to_owned(),
                )
            })?;

        // DataFolder.copy_model_table() interface is designed for model tables.
        let from_model_table_metadata = self
            .model_table_metadata(from_table_name)
            .await
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument(format!(
                    "{from_table_name} is not a model table."
                ))
            })?;

        let to_model_table_metadata = to_data_folder
            .model_table_metadata(to_table_name)
            .await
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument(format!(
                    "{to_table_name} is not a model table."
                ))
            })?;

        // Check if the schemas of the model tables match.
        if !schemas_are_compatible(
            &from_model_table_metadata.schema,
            &to_model_table_metadata.schema,
        ) {
            return Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "The schema of {from_table_name} does not match the schema of {to_table_name}."
            )));
        }

        // Construct the WHERE clause.
        let mut where_clause_values: Vec<String> = vec![];

        if let Some(start_time) = maybe_start_time {
            where_clause_values.push(format!("'{start_time}' <= start_time"));
        }

        if let Some(end_time) = maybe_end_time {
            where_clause_values.push(format!("end_time <= '{end_time}'"));
        }

        // Construct the full SQL query.
        let where_clause = if where_clause_values.is_empty() {
            "".to_owned()
        } else {
            "WHERE ".to_owned() + &where_clause_values.join(" AND ")
        };

        let sql = format!("SELECT * FROM {} {}", from_table_name, where_clause,);

        // Read data to copy from from_table_name in from_data_folder.
        let from_table = Arc::new(self.delta_lake.delta_table(from_table_name).await?);

        let session_context = SessionContext::new();
        session_context.register_table(from_table_name, from_table)?;

        let df = session_context.sql(&sql).await?;
        let record_batches = df.collect().await?;

        // Write read data to to_table_name in to_data_folder.
        to_data_folder
            .delta_lake
            .write_compressed_segments_to_model_table(to_table_name, record_batches)
            .await?;

        Ok(())
    }

    /// Executes the SQL in `sql` and returns the result as a [`RecordBatchStream`]. If the SQL
    /// could not be executed, [`ModelarDbEmbeddedError`] is returned.
    async fn read(&mut self, sql: &str) -> Result<Pin<Box<dyn RecordBatchStream + Send>>> {
        let data_frame = self.session_context.sql(sql).await?;

        data_frame
            .execute_stream()
            .await
            .map_err(|error| error.into())
    }

    /// Executes the SQL in `sql` and writes the result to the normal table with the name in
    /// `to_table_name` in `to_modelardb`. Note that data can be copied from both normal tables and
    /// model tables but only to normal tables. If `to_modelardb` is not a data folder, the data
    /// could not be queried, or the result could not be written to the normal table,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn copy_normal_table(
        &mut self,
        sql: &str,
        to_modelardb: &mut dyn ModelarDB,
        to_table_name: &str,
    ) -> Result<()> {
        let to_data_folder = to_modelardb
            .as_any()
            .downcast_ref::<DataFolder>()
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument(
                    "to_modelardb is not a data folder.".to_owned(),
                )
            })?;

        let to_normal_table_schema = to_data_folder
            .normal_table_schema(to_table_name)
            .await
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument(format!(
                    "{to_table_name} is not a normal table."
                ))
            })?;

        let record_batch_stream = self.read(sql).await?;

        if record_batch_stream.schema() != to_normal_table_schema.into() {
            Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "The schema of the data to copy does not match the schema of {to_table_name}."
            )))
        } else {
            let record_batches = common::collect(record_batch_stream).await?;

            to_data_folder
                .delta_lake
                .write_record_batches_to_normal_table(to_table_name, record_batches)
                .await?;

            Ok(())
        }
    }

    /// Drop the table with the name in `table_name` by deregistering the table from the Apache
    /// Arrow DataFusion session, deleting all the table files from the data Delta Lake, and
    /// deleting the table metadata from the metadata Delta Lake. If the table could not be
    /// deregistered or the metadata or data could not be dropped, [`ModelarDbEmbeddedError`] is
    /// returned.
    async fn drop(&mut self, table_name: &str) -> Result<()> {
        // Drop the table from the Apache Arrow DataFusion session.
        self.session_context.deregister_table(table_name)?;

        // Delete the table metadata from the metadata Delta Lake.
        self.table_metadata_manager
            .drop_table_metadata(table_name)
            .await?;

        // Drop the table from the Delta Lake.
        self.delta_lake.drop_table(table_name).await?;

        Ok(())
    }

    /// Truncate the table with the name in `table_name` by deleting the table data in the data
    /// Delta Lake. If the data could not be deleted, [`ModelarDbEmbeddedError`] is returned.
    async fn truncate(&mut self, table_name: &str) -> Result<()> {
        if self.tables().await?.contains(&table_name.to_owned()) {
            self.delta_lake
                .truncate_table(table_name)
                .await
                .map_err(|error| error.into())
        } else {
            Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            )))
        }
    }

    /// Move all data from the table with the name in `from_table_name` in `self` to the table with
    /// the name in `to_table_name` in `to_modelardb`. If `to_modelardb` is not a data folder, the
    /// schemas of the tables do not match, or the data could not be moved,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn r#move(
        &mut self,
        from_table_name: &str,
        to_modelardb: &dyn ModelarDB,
        to_table_name: &str,
    ) -> Result<()> {
        let to_data_folder = to_modelardb
            .as_any()
            .downcast_ref::<DataFolder>()
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument(
                    "to_modelardb is not a data folder.".to_owned(),
                )
            })?;

        let schema_mismatch_error = ModelarDbEmbeddedError::InvalidArgument(format!(
            "The schema of {from_table_name} does not match the schema of {to_table_name}."
        ));

        if let (Some(from_model_table_metadata), Some(to_model_table_metadata)) = (
            self.model_table_metadata(from_table_name).await,
            to_data_folder.model_table_metadata(to_table_name).await,
        ) {
            // If both tables are model tables, check if their schemas match and write the data in
            // from_table_name to to_table_name if so.
            if !schemas_are_compatible(
                &from_model_table_metadata.schema,
                &to_model_table_metadata.schema,
            ) {
                return Err(schema_mismatch_error);
            }

            let delta_ops = self.delta_lake.delta_ops(from_table_name).await?;
            let (_table, stream) = delta_ops.load().await?;
            let record_batches: Vec<RecordBatch> = stream.try_collect().await?;

            to_data_folder
                .delta_lake
                .write_compressed_segments_to_model_table(to_table_name, record_batches)
                .await?;
        } else if let (Some(from_normal_table_schema), Some(to_normal_table_schema)) = (
            self.normal_table_schema(from_table_name).await,
            to_data_folder.normal_table_schema(to_table_name).await,
        ) {
            // If both tables are normal tables, check if their schemas match and write the data in
            // from_table_name to to_table_name if so.
            if !schemas_are_compatible(&from_normal_table_schema, &to_normal_table_schema) {
                return Err(schema_mismatch_error);
            }

            let delta_ops = self.delta_lake.delta_ops(from_table_name).await?;
            let (_table, stream) = delta_ops.load().await?;
            let record_batches: Vec<RecordBatch> = stream.try_collect().await?;

            to_data_folder
                .delta_lake
                .write_record_batches_to_normal_table(to_table_name, record_batches)
                .await?;
        } else {
            return Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "{from_table_name} and {to_table_name} are not both normal tables or model tables."
            )));
        }

        // Truncate the table after moving the data. This will also delete the tag hash metadata
        // if the table is a model table.
        self.truncate(from_table_name).await?;

        Ok(())
    }
}

/// Sort the `uncompressed_data` from the model table with `model_table_metadata` according to its
/// tags and then timestamps.
fn sort_record_batch_by_tags_and_time(
    model_table_metadata: &ModelTableMetadata,
    uncompressed_data: &RecordBatch,
) -> Result<RecordBatch> {
    let mut physical_sort_exprs = vec![];

    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    for tag_column_index in &model_table_metadata.tag_column_indices {
        let field = model_table_metadata.schema.field(*tag_column_index);
        physical_sort_exprs.push(PhysicalSortExpr {
            expr: Arc::new(Column::new(field.name(), *tag_column_index)),
            options: sort_options,
        });
    }

    let timestamp_column_index = model_table_metadata.timestamp_column_index;
    let field = model_table_metadata.schema.field(timestamp_column_index);
    physical_sort_exprs.push(PhysicalSortExpr {
        expr: Arc::new(Column::new(field.name(), timestamp_column_index)),
        options: sort_options,
    });

    sort::sort_batch(
        uncompressed_data,
        &LexOrdering::new(physical_sort_exprs),
        None,
    )
    .map_err(|error| error.into())
}

/// Compare `from_schema` and `to_schema` and return [`true`] if they have the same number of
/// columns, their columns have the same types, and their columns nullability is less or equally
/// restrictive in `from_schema`. Otherwise [`False`] is returned.
fn schemas_are_compatible(from_schema: &Schema, to_schema: &Schema) -> bool {
    let from_fields = from_schema.fields();
    let to_fields = to_schema.fields();

    if from_fields.len() != to_fields.len() {
        return false;
    }

    for index in 0..from_fields.len() {
        let from_field = &from_fields[index];
        let to_field = &to_fields[index];

        if from_field.data_type() != to_field.data_type() {
            return false;
        }

        if from_field.is_nullable() && !to_field.is_nullable() {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array};
    use arrow::datatypes::{ArrowPrimitiveType, DataType, Field};
    use arrow_flight::flight_service_client::FlightServiceClient;
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::col;
    use modelardb_storage::metadata::model_table_metadata::GeneratedColumn;
    use modelardb_types::types::{ArrowTimestamp, ArrowValue, ErrorBound, ValueArray};
    use tempfile::TempDir;
    use tonic::transport::Channel;

    use crate::modelardb::client::{Client, Node};
    use crate::record_batch_stream_to_record_batch;

    const NORMAL_TABLE_NAME: &str = "normal_table";
    const MODEL_TABLE_NAME: &str = "model_table";
    const MISSING_TABLE_NAME: &str = "missing_table";
    const MODEL_TABLE_WITH_GENERATED_COLUMN_NAME: &str = "model_table_with_generated";
    const INVALID_COLUMN_NAME: &str = "invalid_column";

    #[tokio::test]
    async fn test_create_normal_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_normal_table().await;
        assert_normal_table_exists(&data_folder, NORMAL_TABLE_NAME, normal_table_schema()).await;
    }

    #[tokio::test]
    async fn test_register_existing_normal_tables_on_open() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        data_folder
            .create(
                "normal_table_1",
                TableType::NormalTable(normal_table_schema()),
            )
            .await
            .unwrap();

        data_folder
            .create(
                "normal_table_2",
                TableType::NormalTable(normal_table_schema()),
            )
            .await
            .unwrap();

        // Create a new data folder and verify that the existing normal tables are registered.
        let new_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();
        assert!(
            new_data_folder
                .session_context
                .table_exist("normal_table_1")
                .unwrap()
        );
        assert!(
            new_data_folder
                .session_context
                .table_exist("normal_table_2")
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_create_normal_table_with_empty_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder
            .create(NORMAL_TABLE_NAME, TableType::NormalTable(Schema::empty()))
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "ModelarDB Storage Error: Delta Lake Error: Generic error: \
            At least one column must be defined to create a table."
        );
    }

    #[tokio::test]
    async fn test_create_existing_normal_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder
            .create(
                NORMAL_TABLE_NAME,
                TableType::NormalTable(normal_table_schema()),
            )
            .await;
        assert!(result.is_ok());

        let result = data_folder
            .create(
                NORMAL_TABLE_NAME,
                TableType::NormalTable(normal_table_schema()),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "ModelarDB Storage Error: Delta Lake Error: Generic error: \
            A Delta Lake table already exists at that location."
        );
    }

    #[tokio::test]
    async fn test_create_model_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_model_table().await;
        assert_model_table_exists(&data_folder, MODEL_TABLE_NAME, model_table_schema()).await;
    }

    #[tokio::test]
    async fn test_create_model_table_with_error_bounds() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let error_bounds = HashMap::from([
            (
                "field_1".to_owned(),
                ErrorBound::try_new_absolute(1.0).unwrap(),
            ),
            (
                "field_2".to_owned(),
                ErrorBound::try_new_relative(5.0).unwrap(),
            ),
        ]);

        let table_type = TableType::ModelTable(model_table_schema(), error_bounds, HashMap::new());
        data_folder
            .create(MODEL_TABLE_NAME, table_type)
            .await
            .unwrap();

        let model_table_metadata =
            assert_model_table_exists(&data_folder, MODEL_TABLE_NAME, model_table_schema()).await;

        let error_bound_values: Vec<f32> = model_table_metadata
            .error_bounds
            .iter()
            .map(|error_bound| match error_bound {
                ErrorBound::Absolute(value) => *value,
                ErrorBound::Relative(value) => -*value,
            })
            .collect();

        assert_eq!(error_bound_values, vec![0.0, 0.0, 0.0, 1.0, -5.0]);
    }

    #[tokio::test]
    async fn test_create_model_table_with_generated_columns() {
        let (_temp_dir, data_folder) =
            create_data_folder_with_model_table_with_generated_column().await;

        let mut model_table_metadata = assert_model_table_exists(
            &data_folder,
            MODEL_TABLE_WITH_GENERATED_COLUMN_NAME,
            model_table_with_generated_column_schema(),
        )
        .await;

        let expected_generated_column = GeneratedColumn {
            expr: col("field_1") + col("field_2"),
            source_columns: vec![3, 4],
            original_expr: "field_1 + field_2".to_owned(),
        };

        let mut actual_generated_column = model_table_metadata
            .generated_columns
            .pop()
            .unwrap()
            .unwrap();

        // Sort the source columns to ensure that the order is consistent.
        actual_generated_column.source_columns.sort();
        assert_eq!(actual_generated_column, expected_generated_column);
    }

    #[tokio::test]
    async fn test_register_existing_model_tables_on_open() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        data_folder
            .create(
                "model_table_1",
                TableType::ModelTable(model_table_schema(), HashMap::new(), HashMap::new()),
            )
            .await
            .unwrap();

        data_folder
            .create(
                "model_table_2",
                TableType::ModelTable(model_table_schema(), HashMap::new(), HashMap::new()),
            )
            .await
            .unwrap();

        // Create a new data folder and verify that the existing model tables are registered.
        let new_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();
        assert!(
            new_data_folder
                .session_context
                .table_exist("model_table_1")
                .unwrap()
        );
        assert!(
            new_data_folder
                .session_context
                .table_exist("model_table_2")
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_create_model_table_with_empty_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder
            .create(
                MODEL_TABLE_NAME,
                TableType::ModelTable(Schema::empty(), HashMap::new(), HashMap::new()),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "ModelarDB Storage Error: Invalid Argument Error: \
            There needs to be exactly one timestamp column."
        );
    }

    #[tokio::test]
    async fn test_create_existing_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder
            .create(
                MODEL_TABLE_NAME,
                TableType::ModelTable(model_table_schema(), HashMap::new(), HashMap::new()),
            )
            .await;
        assert!(result.is_ok());

        let result = data_folder
            .create(
                MODEL_TABLE_NAME,
                TableType::ModelTable(model_table_schema(), HashMap::new(), HashMap::new()),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "ModelarDB Storage Error: Delta Lake Error: Generic error: \
            A Delta Lake table already exists at that location."
        );
    }

    #[tokio::test]
    async fn test_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let table_names = data_folder.tables().await.unwrap();
        assert!(table_names.is_empty());

        data_folder
            .create(
                NORMAL_TABLE_NAME,
                TableType::NormalTable(normal_table_schema()),
            )
            .await
            .unwrap();

        data_folder
            .create(
                MODEL_TABLE_NAME,
                TableType::ModelTable(model_table_schema(), HashMap::new(), HashMap::new()),
            )
            .await
            .unwrap();

        let table_names = data_folder.tables().await.unwrap();
        assert_eq!(table_names, vec![NORMAL_TABLE_NAME, MODEL_TABLE_NAME]);
    }

    #[tokio::test]
    async fn test_normal_table_schema() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let actual_schema = data_folder.schema(NORMAL_TABLE_NAME).await.unwrap();
        assert_eq!(actual_schema, normal_table_schema());
    }

    #[tokio::test]
    async fn test_model_table_schema() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        let actual_schema = data_folder.schema(MODEL_TABLE_NAME).await.unwrap();
        assert_eq!(actual_schema, model_table_schema());
    }

    #[tokio::test]
    async fn test_missing_table_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder.schema(MISSING_TABLE_NAME).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a table.")
        );
    }

    #[tokio::test]
    async fn test_write_to_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;
        let mut delta_table = data_folder
            .delta_lake
            .delta_table(NORMAL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 0);

        data_folder
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 1);
    }

    #[tokio::test]
    async fn test_write_empty_data_to_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let empty_data = RecordBatch::new_empty(Arc::new(normal_table_schema()));
        let result = data_folder.write(NORMAL_TABLE_NAME, empty_data).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: The uncompressed data is empty."
        );
    }

    #[tokio::test]
    async fn test_write_data_with_invalid_schema_to_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let result = data_folder
            .write(NORMAL_TABLE_NAME, invalid_table_data())
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The uncompressed data does not \
                match the schema for the table: {NORMAL_TABLE_NAME}."
            )
        );
    }

    #[tokio::test]
    async fn test_write_to_model_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;
        let mut delta_table = data_folder
            .delta_lake
            .delta_table(MODEL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 0);

        data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 2);
    }

    #[tokio::test]
    async fn test_write_empty_data_to_model_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        let empty_data = RecordBatch::new_empty(Arc::new(model_table_schema()));
        let result = data_folder.write(MODEL_TABLE_NAME, empty_data).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: The uncompressed data is empty."
        );
    }

    #[tokio::test]
    async fn test_write_data_with_invalid_schema_to_model_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        let result = data_folder
            .write(MODEL_TABLE_NAME, invalid_table_data())
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The uncompressed data does not \
                match the schema for the table: {MODEL_TABLE_NAME}."
            )
        );
    }

    #[tokio::test]
    async fn test_write_to_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder
            .write(MISSING_TABLE_NAME, model_table_data())
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a table.")
        );
    }

    #[tokio::test]
    async fn test_read_model_table_from_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let result = data_folder_read_model_table(
            &mut data_folder,
            NORMAL_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {NORMAL_TABLE_NAME} is not a model table.")
        );
    }

    #[tokio::test]
    async fn test_read_model_table_from_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder_read_model_table(
            &mut data_folder,
            MISSING_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a model table.")
        );
    }

    #[tokio::test]
    async fn test_read_model_table_from_model_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let actual_result = data_folder_read_model_table(
            &mut data_folder,
            MODEL_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(actual_result, sorted_model_table_data());
    }

    #[tokio::test]
    async fn test_read_model_table_from_empty_model_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        let actual_result = data_folder_read_model_table(
            &mut data_folder,
            MODEL_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            actual_result,
            RecordBatch::new_empty(Arc::new(Schema::empty()))
        );
    }

    #[tokio::test]
    async fn test_read_model_table_from_model_table_with_columns() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let columns = vec![
            ("tag_1".to_owned(), Aggregate::None),
            ("field_1".to_owned(), Aggregate::None),
        ];

        let actual_result = data_folder_read_model_table(
            &mut data_folder,
            MODEL_TABLE_NAME,
            &columns,
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            actual_result,
            sorted_model_table_data().project(&[1, 3]).unwrap()
        );
    }

    #[tokio::test]
    async fn test_read_model_table_from_model_table_with_aggregates() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let columns = vec![
            ("tag_1".to_owned(), Aggregate::None),
            ("field_1".to_owned(), Aggregate::Count),
            ("field_1".to_owned(), Aggregate::Min),
            ("field_1".to_owned(), Aggregate::Max),
            ("field_2".to_owned(), Aggregate::Sum),
            ("field_2".to_owned(), Aggregate::Avg),
        ];

        let actual_result = data_folder_read_model_table(
            &mut data_folder,
            MODEL_TABLE_NAME,
            &columns,
            &["tag_1".to_owned()],
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            **actual_result.column(0),
            StringArray::from(vec!["tag_x", "tag_y"])
        );
        assert_eq!(**actual_result.column(1), Int64Array::from(vec![3, 3]));
        assert_eq!(
            **actual_result.column(2),
            Float32Array::from(vec![37.0, 71.0])
        );
        assert_eq!(
            **actual_result.column(3),
            Float32Array::from(vec![39.0, 73.0])
        );
        assert_eq!(
            **actual_result.column(4),
            Float64Array::from(vec![75.0, 165.0])
        );
        assert_eq!(
            **actual_result.column(5),
            Float64Array::from(vec![25.0, 55.0])
        );
    }

    #[tokio::test]
    async fn test_read_model_table_from_model_table_with_invalid_column() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        let columns = vec![(INVALID_COLUMN_NAME.to_owned(), Aggregate::None)];

        let result = data_folder_read_model_table(
            &mut data_folder,
            MODEL_TABLE_NAME,
            &columns,
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Schema error: No field named {INVALID_COLUMN_NAME}. \
                Valid fields are {}.",
                model_table_columns()
            )
        );
    }

    #[tokio::test]
    async fn test_read_model_table_from_model_table_with_invalid_group_by() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        let group_by = vec![INVALID_COLUMN_NAME.to_owned()];

        let result = data_folder_read_model_table(
            &mut data_folder,
            MODEL_TABLE_NAME,
            &[],
            &group_by,
            None,
            None,
            HashMap::new(),
        )
        .await;

        // The error message includes the valid fields twice when group_by is included in the query.
        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Schema error: No field named {INVALID_COLUMN_NAME}. \
                Valid fields are {}, {}.",
                model_table_columns(),
                model_table_columns()
            )
        );
    }

    #[tokio::test]
    async fn test_read_model_table_from_model_table_with_timestamps() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let actual_result = data_folder_read_model_table(
            &mut data_folder,
            MODEL_TABLE_NAME,
            &[],
            &[],
            Some("1970-01-01T00:00:00.000150"),
            Some("1970-01-01T00:00:00.000250"),
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(actual_result, model_table_data().slice(2, 2));
    }

    #[tokio::test]
    async fn test_read_model_table_from_model_table_with_invalid_timestamps() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let invalid_start_time = "01/01/1970T00:00:00.000150";
        let result = data_folder_read_model_table(
            &mut data_folder,
            MODEL_TABLE_NAME,
            &[],
            &[],
            Some(invalid_start_time),
            Some("01/01/1970T00:00:00.000250"),
            HashMap::new(),
        )
        .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Arrow error: Parser error: \
                Error parsing timestamp from '{invalid_start_time}': error parsing date"
            )
        );
    }

    #[tokio::test]
    async fn test_read_model_table_from_model_table_with_tags() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let mut tags = HashMap::new();
        tags.insert("tag_1".to_owned(), "tag_x".to_owned());
        tags.insert("tag_2".to_owned(), "tag_a".to_owned());

        let actual_result = data_folder_read_model_table(
            &mut data_folder,
            MODEL_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            tags,
        )
        .await
        .unwrap();

        assert_eq!(actual_result, sorted_model_table_data().slice(0, 3));
    }

    #[tokio::test]
    async fn test_read_model_table_from_model_table_with_invalid_tag() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        let mut tags = HashMap::new();
        tags.insert(INVALID_COLUMN_NAME.to_owned(), "tag_x".to_owned());

        let result = data_folder_read_model_table(
            &mut data_folder,
            MODEL_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            tags,
        )
        .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Schema error: No field named {INVALID_COLUMN_NAME}. \
                Valid fields are {}.",
                model_table_columns()
            )
        );
    }

    fn model_table_columns() -> String {
        model_table_schema()
            .fields()
            .iter()
            .map(|field| format!("{MODEL_TABLE_NAME}.{}", field.name()))
            .collect::<Vec<String>>()
            .join(", ")
    }

    #[tokio::test]
    async fn test_read_model_table_from_model_table_with_generated_column() {
        let (_temp_dir, mut data_folder) =
            create_data_folder_with_model_table_with_generated_column().await;

        data_folder
            .write(MODEL_TABLE_WITH_GENERATED_COLUMN_NAME, model_table_data())
            .await
            .unwrap();

        let actual_result = data_folder_read_model_table(
            &mut data_folder,
            MODEL_TABLE_WITH_GENERATED_COLUMN_NAME,
            &[],
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_generated_column_result_eq(actual_result, sorted_model_table_data());
    }

    async fn data_folder_read_model_table(
        data_folder: &mut DataFolder,
        table_name: &str,
        columns: &[(String, Aggregate)],
        group_by: &[String],
        maybe_start_time: Option<&str>,
        maybe_end_time: Option<&str>,
        tags: HashMap<String, String>,
    ) -> Result<RecordBatch> {
        let record_batch_stream = data_folder
            .read_model_table(
                table_name,
                columns,
                group_by,
                maybe_start_time,
                maybe_end_time,
                tags,
            )
            .await?;

        record_batch_stream_to_record_batch(record_batch_stream).await
    }

    #[tokio::test]
    async fn test_copy_model_table_from_normal_table_to_model_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;
        let (_temp_dir, to_data_folder) = create_data_folder_with_model_table().await;

        from_data_folder
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let result = from_data_folder
            .copy_model_table(
                NORMAL_TABLE_NAME,
                &to_data_folder,
                MODEL_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {NORMAL_TABLE_NAME} is not a model table.")
        );
    }

    #[tokio::test]
    async fn test_copy_model_table_from_model_table_to_normal_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;
        let (_temp_dir, to_data_folder) = create_data_folder_with_normal_table().await;

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let result = from_data_folder
            .copy_model_table(
                MODEL_TABLE_NAME,
                &to_data_folder,
                NORMAL_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {NORMAL_TABLE_NAME} is not a model table.")
        );
    }

    #[tokio::test]
    async fn test_copy_model_table_from_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let from_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let (_temp_dir, to_data_folder) = create_data_folder_with_model_table().await;

        let result = from_data_folder
            .copy_model_table(
                MISSING_TABLE_NAME,
                &to_data_folder,
                MODEL_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a model table.")
        );
    }

    #[tokio::test]
    async fn test_copy_model_table_from_model_table_to_missing_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let to_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let result = from_data_folder
            .copy_model_table(
                MODEL_TABLE_NAME,
                &to_data_folder,
                MISSING_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a model table.")
        );
    }

    #[tokio::test]
    async fn test_copy_model_table_from_model_table_to_model_table_with_invalid_schema() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let mut to_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let table_type =
            TableType::ModelTable(invalid_table_schema(), HashMap::new(), HashMap::new());
        to_data_folder
            .create(MODEL_TABLE_NAME, table_type)
            .await
            .unwrap();

        let result = from_data_folder
            .copy_model_table(
                MODEL_TABLE_NAME,
                &to_data_folder,
                MODEL_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The schema of {MODEL_TABLE_NAME} \
                does not match the schema of {MODEL_TABLE_NAME}."
            )
        );
    }

    #[tokio::test]
    async fn test_copy_model_table_from_model_table_to_model_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;
        let (_temp_dir, mut to_data_folder) = create_data_folder_with_model_table().await;

        let sql = format!("SELECT * FROM {}", MODEL_TABLE_NAME);
        let to_actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();
        assert_eq!(to_actual_result.num_rows(), 0);

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        from_data_folder
            .copy_model_table(
                MODEL_TABLE_NAME,
                &to_data_folder,
                MODEL_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await
            .unwrap();

        let from_actual_result = data_folder_read(&mut from_data_folder, &sql).await.unwrap();
        let to_actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();

        assert_eq!(from_actual_result, sorted_model_table_data());
        assert_eq!(to_actual_result, from_actual_result);
    }

    #[tokio::test]
    async fn test_copy_model_table_from_model_table_to_model_table_with_timestamps() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;
        let (_temp_dir, mut to_data_folder) = create_data_folder_with_model_table().await;

        let sql = format!("SELECT * FROM {}", MODEL_TABLE_NAME);
        let to_actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();
        assert_eq!(to_actual_result.num_rows(), 0);

        // Force the physical data to have multiple segments by writing the data in three parts.
        for i in 0..3 {
            from_data_folder
                .write(MODEL_TABLE_NAME, model_table_data().slice(i * 2, 2))
                .await
                .unwrap();
        }

        from_data_folder
            .copy_model_table(
                MODEL_TABLE_NAME,
                &to_data_folder,
                MODEL_TABLE_NAME,
                Some("1970-01-01T00:00:00.000150"),
                Some("1970-01-01T00:00:00.000250"),
                HashMap::new(),
            )
            .await
            .unwrap();

        let from_actual_result = data_folder_read(&mut from_data_folder, &sql).await.unwrap();
        let to_actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();

        assert_eq!(from_actual_result, model_table_data());
        assert_eq!(to_actual_result, model_table_data().slice(2, 2));
    }

    #[tokio::test]
    async fn test_copy_model_table_from_model_table_to_model_table_with_invalid_timestamps() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;
        let (_temp_dir, to_data_folder) = create_data_folder_with_model_table().await;

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let invalid_start_time = "01/01/1970T00:00:00.000150";
        let result = from_data_folder
            .copy_model_table(
                MODEL_TABLE_NAME,
                &to_data_folder,
                MODEL_TABLE_NAME,
                Some(invalid_start_time),
                Some("01/01/1970T00:00:00.000250"),
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Arrow error: Parser error: \
                Error parsing timestamp from '{invalid_start_time}': error parsing date"
            )
        );
    }

    #[tokio::test]
    async fn test_copy_model_table_from_model_table_to_model_table_with_generated_column() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;
        let (_temp_dir, mut to_data_folder) =
            create_data_folder_with_model_table_with_generated_column().await;

        let to_sql = format!("SELECT * FROM {}", MODEL_TABLE_WITH_GENERATED_COLUMN_NAME);
        let to_actual_result = data_folder_read(&mut to_data_folder, &to_sql)
            .await
            .unwrap();
        assert_eq!(to_actual_result.num_rows(), 0);

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        // Even though the query schemas are different, the data should still be copied.
        from_data_folder
            .copy_model_table(
                MODEL_TABLE_NAME,
                &to_data_folder,
                MODEL_TABLE_WITH_GENERATED_COLUMN_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await
            .unwrap();

        let from_sql = format!("SELECT * FROM {}", MODEL_TABLE_NAME);
        let from_actual_result = data_folder_read(&mut from_data_folder, &from_sql)
            .await
            .unwrap();

        let to_actual_result = data_folder_read(&mut to_data_folder, &to_sql)
            .await
            .unwrap();

        assert_eq!(from_actual_result, sorted_model_table_data());
        assert_generated_column_result_eq(to_actual_result, from_actual_result);
    }

    #[tokio::test]
    async fn test_copy_model_table_from_data_folder_to_client() {
        let (_temp_dir, from_data_folder) = create_data_folder_with_model_table().await;
        let to_client = lazy_modelardb_client();

        let result = from_data_folder
            .copy_model_table(
                MODEL_TABLE_NAME,
                &to_client,
                MODEL_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: to_modelardb is not a data folder.".to_owned()
        );
    }

    #[tokio::test]
    async fn test_read_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let expected_result = normal_table_data();
        data_folder
            .write(NORMAL_TABLE_NAME, expected_result.clone())
            .await
            .unwrap();

        let sql = format!("SELECT * FROM {}", NORMAL_TABLE_NAME);
        let actual_result = data_folder_read(&mut data_folder, &sql).await.unwrap();

        assert_eq!(actual_result, expected_result);
    }

    #[tokio::test]
    async fn test_read_empty_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let sql = format!("SELECT * FROM {}", NORMAL_TABLE_NAME);
        let actual_result = data_folder_read(&mut data_folder, &sql).await.unwrap();

        assert_eq!(
            actual_result,
            RecordBatch::new_empty(Arc::new(Schema::empty()))
        );
    }

    #[tokio::test]
    async fn test_read_model_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let sql = format!("SELECT * FROM {}", MODEL_TABLE_NAME);
        let actual_result = data_folder_read(&mut data_folder, &sql).await.unwrap();

        assert_eq!(actual_result, sorted_model_table_data());
    }

    #[tokio::test]
    async fn test_read_empty_model_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        let sql = format!("SELECT * FROM {}", MODEL_TABLE_NAME);
        let actual_result = data_folder_read(&mut data_folder, &sql).await.unwrap();

        assert_eq!(
            actual_result,
            RecordBatch::new_empty(Arc::new(Schema::empty()))
        );
    }

    #[tokio::test]
    async fn test_read_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let sql = format!("SELECT * FROM {}", MISSING_TABLE_NAME);
        let result = data_folder_read(&mut data_folder, &sql).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Error during planning: \
                table 'datafusion.public.{MISSING_TABLE_NAME}' not found"
            )
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_normal_table_to_normal_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;
        let (_temp_dir, mut to_data_folder) = create_data_folder_with_normal_table().await;

        let sql = format!("SELECT * FROM {}", NORMAL_TABLE_NAME);
        let to_actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();
        assert_eq!(to_actual_result.num_rows(), 0);

        from_data_folder
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT * FROM {} LIMIT 3", NORMAL_TABLE_NAME);
        from_data_folder
            .copy_normal_table(&copy_sql, &mut to_data_folder, NORMAL_TABLE_NAME)
            .await
            .unwrap();

        let from_actual_result = data_folder_read(&mut from_data_folder, &sql).await.unwrap();
        let to_actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();

        assert_eq!(from_actual_result, normal_table_data());
        assert_eq!(to_actual_result, from_actual_result.slice(0, 3));
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_normal_table_to_normal_table_with_different_schema() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut to_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let schema = normal_table_schema().project(&[0, 1]).unwrap();
        to_data_folder
            .create(NORMAL_TABLE_NAME, TableType::NormalTable(schema))
            .await
            .unwrap();

        let sql = format!("SELECT * FROM {}", NORMAL_TABLE_NAME);
        let to_actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();
        assert_eq!(to_actual_result.num_rows(), 0);

        from_data_folder
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT id, name FROM {}", NORMAL_TABLE_NAME);
        from_data_folder
            .copy_normal_table(&copy_sql, &mut to_data_folder, NORMAL_TABLE_NAME)
            .await
            .unwrap();

        let from_actual_result = data_folder_read(&mut from_data_folder, &sql).await.unwrap();
        let to_actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();

        assert_eq!(from_actual_result, normal_table_data());
        assert_eq!(
            to_actual_result,
            from_actual_result.project(&[0, 1]).unwrap()
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_normal_table_to_normal_table_with_invalid_schema() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;
        let (_temp_dir, mut to_data_folder) = create_data_folder_with_normal_table().await;

        from_data_folder
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT id, name FROM {}", NORMAL_TABLE_NAME);
        let result = from_data_folder
            .copy_normal_table(&copy_sql, &mut to_data_folder, NORMAL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The schema of the data to copy does not \
                match the schema of {NORMAL_TABLE_NAME}."
            )
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_model_table_to_normal_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;

        // Create a normal table that has the same schema as the model table in from_data_folder.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut to_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let schema = model_table_schema();
        to_data_folder
            .create(NORMAL_TABLE_NAME, TableType::NormalTable(schema))
            .await
            .unwrap();

        let to_sql = format!("SELECT * FROM {}", NORMAL_TABLE_NAME);
        let to_actual_result = data_folder_read(&mut to_data_folder, &to_sql)
            .await
            .unwrap();
        assert_eq!(to_actual_result.num_rows(), 0);

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT * FROM {}", MODEL_TABLE_NAME);
        from_data_folder
            .copy_normal_table(&copy_sql, &mut to_data_folder, NORMAL_TABLE_NAME)
            .await
            .unwrap();

        let from_actual_result = data_folder_read(&mut from_data_folder, &copy_sql)
            .await
            .unwrap();
        let to_actual_result = data_folder_read(&mut to_data_folder, &to_sql)
            .await
            .unwrap();

        assert_eq!(from_actual_result, sorted_model_table_data());
        assert_eq!(to_actual_result, from_actual_result);
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_normal_table_to_model_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;
        let (_temp_dir, mut to_data_folder) = create_data_folder_with_model_table().await;

        from_data_folder
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT * FROM {}", NORMAL_TABLE_NAME);
        let result = from_data_folder
            .copy_normal_table(&copy_sql, &mut to_data_folder, MODEL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MODEL_TABLE_NAME} is not a normal table.")
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_normal_table_to_missing_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut to_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        from_data_folder
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT * FROM {}", NORMAL_TABLE_NAME);
        let result = from_data_folder
            .copy_normal_table(&copy_sql, &mut to_data_folder, MISSING_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a normal table.")
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_missing_table_to_normal_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut from_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let (_temp_dir, mut to_data_folder) = create_data_folder_with_normal_table().await;

        let copy_sql = format!("SELECT * FROM {}", MISSING_TABLE_NAME);
        let result = from_data_folder
            .copy_normal_table(&copy_sql, &mut to_data_folder, NORMAL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Error during planning: \
                table 'datafusion.public.{MISSING_TABLE_NAME}' not found"
            )
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_data_folder_to_client() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;
        let mut to_client = lazy_modelardb_client();

        let copy_sql = format!("SELECT * FROM {}", NORMAL_TABLE_NAME);
        let result = from_data_folder
            .copy_normal_table(&copy_sql, &mut to_client, NORMAL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: to_modelardb is not a data folder.".to_owned()
        );
    }

    #[tokio::test]
    async fn test_drop_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        assert!(
            data_folder
                .session_context
                .table_exist(NORMAL_TABLE_NAME)
                .unwrap()
        );

        data_folder.drop(NORMAL_TABLE_NAME).await.unwrap();

        // Verify that the normal table was deregistered from Apache DataFusion.
        assert!(
            !data_folder
                .session_context
                .table_exist(NORMAL_TABLE_NAME)
                .unwrap()
        );

        // Verify that the normal table was dropped from the metadata Delta Lake.
        assert!(
            !data_folder
                .table_metadata_manager
                .is_normal_table(NORMAL_TABLE_NAME)
                .await
                .unwrap()
        );

        // Verify that the normal table was dropped from the Delta Lake.
        assert!(
            data_folder
                .delta_lake
                .delta_table(NORMAL_TABLE_NAME)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_drop_model_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        assert!(
            data_folder
                .session_context
                .table_exist(MODEL_TABLE_NAME)
                .unwrap()
        );

        data_folder.drop(MODEL_TABLE_NAME).await.unwrap();

        // Verify that the model table was deregistered from Apache DataFusion.
        assert!(
            !data_folder
                .session_context
                .table_exist(MODEL_TABLE_NAME)
                .unwrap()
        );

        // Verify that the model table was dropped from the metadata Delta Lake.
        assert!(
            !data_folder
                .table_metadata_manager
                .is_model_table(MODEL_TABLE_NAME)
                .await
                .unwrap()
        );

        // Verify that the model table was dropped from the Delta Lake.
        assert!(
            data_folder
                .delta_lake
                .delta_table(MODEL_TABLE_NAME)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_drop_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder.drop(MISSING_TABLE_NAME).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "ModelarDB Storage Error: Invalid Argument Error: \
                Table with name '{MISSING_TABLE_NAME}' does not exist."
            )
        );
    }

    #[tokio::test]
    async fn test_truncate_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        data_folder
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let mut delta_table = data_folder
            .delta_lake
            .delta_table(NORMAL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 1);

        data_folder.truncate(NORMAL_TABLE_NAME).await.unwrap();

        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);

        // Verify that the normal table still exists.
        assert_normal_table_exists(&data_folder, NORMAL_TABLE_NAME, normal_table_schema()).await;
    }

    #[tokio::test]
    async fn test_truncate_model_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_model_table().await;

        data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let mut delta_table = data_folder
            .delta_lake
            .delta_table(MODEL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 2);

        data_folder.truncate(MODEL_TABLE_NAME).await.unwrap();

        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);

        // Verify that the model table still exists.
        assert_model_table_exists(&data_folder, MODEL_TABLE_NAME, model_table_schema()).await;
    }

    #[tokio::test]
    async fn test_truncate_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder.truncate(MISSING_TABLE_NAME).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: Table with name '{MISSING_TABLE_NAME}' does not exist."
            )
        );
    }

    #[tokio::test]
    async fn test_move_normal_table_to_normal_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;
        let (_temp_dir, mut to_data_folder) = create_data_folder_with_normal_table().await;

        let sql = format!("SELECT * FROM {}", NORMAL_TABLE_NAME);
        let actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();
        assert_eq!(actual_result.num_rows(), 0);

        let expected_result = normal_table_data();
        from_data_folder
            .write(NORMAL_TABLE_NAME, expected_result.clone())
            .await
            .unwrap();

        let mut delta_table = from_data_folder
            .delta_lake
            .delta_table(NORMAL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 1);

        from_data_folder
            .r#move(NORMAL_TABLE_NAME, &to_data_folder, NORMAL_TABLE_NAME)
            .await
            .unwrap();

        // Verify that the data was deleted but the normal table still exists.
        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);
        assert_normal_table_exists(&from_data_folder, NORMAL_TABLE_NAME, normal_table_schema())
            .await;

        // Verify that the normal table data was moved to the new data folder.
        let actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();
        assert_eq!(actual_result, expected_result);
    }

    async fn assert_normal_table_exists(
        data_folder: &DataFolder,
        table_name: &str,
        expected_schema: Schema,
    ) {
        // Verify that the normal table exists in the Delta Lake.
        let delta_table = data_folder
            .delta_lake
            .delta_table(table_name)
            .await
            .unwrap();

        let actual_schema = TableProvider::schema(&delta_table);
        assert_eq!(actual_schema, Arc::new(expected_schema));

        // Verify that the normal table exists in the metadata Delta Lake.
        assert!(
            data_folder
                .table_metadata_manager
                .is_normal_table(table_name)
                .await
                .unwrap()
        );

        // Verify that the normal table is registered with Apache DataFusion.
        assert!(data_folder.session_context.table_exist(table_name).unwrap())
    }

    #[tokio::test]
    async fn test_move_normal_table_to_normal_table_with_invalid_schema() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut to_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        to_data_folder
            .create(
                NORMAL_TABLE_NAME,
                TableType::NormalTable(invalid_table_schema()),
            )
            .await
            .unwrap();

        let expected_result = normal_table_data();
        from_data_folder
            .write(NORMAL_TABLE_NAME, expected_result.clone())
            .await
            .unwrap();

        let result = from_data_folder
            .r#move(NORMAL_TABLE_NAME, &to_data_folder, NORMAL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The schema of {NORMAL_TABLE_NAME} \
                does not match the schema of {NORMAL_TABLE_NAME}."
            )
        );

        assert_table_not_moved(
            &mut from_data_folder,
            NORMAL_TABLE_NAME,
            expected_result,
            Some(&mut to_data_folder),
            Some(NORMAL_TABLE_NAME),
        )
        .await;
    }

    #[tokio::test]
    async fn test_move_normal_table_to_model_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;
        let (_temp_dir, mut to_data_folder) = create_data_folder_with_model_table().await;

        let expected_result = normal_table_data();
        from_data_folder
            .write(NORMAL_TABLE_NAME, expected_result.clone())
            .await
            .unwrap();

        let result = from_data_folder
            .r#move(NORMAL_TABLE_NAME, &to_data_folder, MODEL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: {NORMAL_TABLE_NAME} and {MODEL_TABLE_NAME} are not \
                both normal tables or model tables."
            )
        );

        assert_table_not_moved(
            &mut from_data_folder,
            NORMAL_TABLE_NAME,
            expected_result,
            Some(&mut to_data_folder),
            Some(MODEL_TABLE_NAME),
        )
        .await;
    }

    #[tokio::test]
    async fn test_move_normal_table_to_missing_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let to_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let expected_result = normal_table_data();
        from_data_folder
            .write(NORMAL_TABLE_NAME, expected_result.clone())
            .await
            .unwrap();

        let result = from_data_folder
            .r#move(NORMAL_TABLE_NAME, &to_data_folder, MISSING_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: {NORMAL_TABLE_NAME} and {MISSING_TABLE_NAME} are not \
                both normal tables or model tables."
            )
        );

        assert_table_not_moved(
            &mut from_data_folder,
            NORMAL_TABLE_NAME,
            expected_result,
            None,
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn test_move_model_table_to_model_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;
        let (_temp_dir, mut to_data_folder) = create_data_folder_with_model_table().await;

        let sql = format!("SELECT * FROM {}", MODEL_TABLE_NAME);
        let to_actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();
        assert_eq!(to_actual_result.num_rows(), 0);

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let mut delta_table = from_data_folder
            .delta_lake
            .delta_table(MODEL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 2);

        from_data_folder
            .r#move(MODEL_TABLE_NAME, &to_data_folder, MODEL_TABLE_NAME)
            .await
            .unwrap();

        // Verify that the data was deleted but the model table still exists.
        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);
        assert_model_table_exists(&from_data_folder, MODEL_TABLE_NAME, model_table_schema()).await;

        // Verify that the model table data was moved to the new data folder.
        let actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();
        assert_eq!(actual_result, sorted_model_table_data());
    }

    #[tokio::test]
    async fn test_move_model_table_to_model_table_with_generated_column() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;
        let (_temp_dir, mut to_data_folder) =
            create_data_folder_with_model_table_with_generated_column().await;

        let sql = format!("SELECT * FROM {}", MODEL_TABLE_WITH_GENERATED_COLUMN_NAME);
        let actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();
        assert_eq!(actual_result.num_rows(), 0);

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let mut delta_table = from_data_folder
            .delta_lake
            .delta_table(MODEL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 2);

        // Even though the query schemas are different, the data should still be moved.
        from_data_folder
            .r#move(
                MODEL_TABLE_NAME,
                &to_data_folder,
                MODEL_TABLE_WITH_GENERATED_COLUMN_NAME,
            )
            .await
            .unwrap();

        // Verify that the data was deleted but the model table still exists.
        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);
        assert_model_table_exists(&from_data_folder, MODEL_TABLE_NAME, model_table_schema()).await;

        // Verify that the model table data was moved to the new data folder.
        let actual_result = data_folder_read(&mut to_data_folder, &sql).await.unwrap();
        assert_generated_column_result_eq(actual_result, sorted_model_table_data());
    }

    async fn assert_model_table_exists(
        data_folder: &DataFolder,
        table_name: &str,
        expected_schema: Schema,
    ) -> ModelTableMetadata {
        // Verify that the model table exists in the Delta Lake.
        assert!(data_folder.delta_lake.delta_table(table_name).await.is_ok());

        // Verify that the model table exists in the metadata Delta Lake with the correct schema.
        let model_table_metadata = data_folder
            .table_metadata_manager
            .model_table_metadata_for_model_table(table_name)
            .await
            .unwrap();

        assert_eq!(model_table_metadata.name, table_name);
        assert_eq!(*model_table_metadata.query_schema, expected_schema);

        // Verify that the model table is registered with Apache DataFusion.
        assert!(data_folder.session_context.table_exist(table_name).unwrap());

        model_table_metadata
    }

    fn assert_generated_column_result_eq(
        mut actual_result: RecordBatch,
        expected_result_without_generated_column: RecordBatch,
    ) {
        let generated_column_result = actual_result.remove_column(5);
        assert_eq!(actual_result, expected_result_without_generated_column);
        assert_eq!(
            *generated_column_result
                .as_any()
                .downcast_ref::<ValueArray>()
                .unwrap(),
            ValueArray::from(vec![61.0, 63.0, 65.0, 129.0, 127.0, 125.0])
        );
    }

    #[tokio::test]
    async fn test_move_model_table_to_model_table_with_invalid_schema() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut to_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        to_data_folder
            .create(
                MODEL_TABLE_NAME,
                TableType::ModelTable(invalid_table_schema(), HashMap::new(), HashMap::new()),
            )
            .await
            .unwrap();

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let result = from_data_folder
            .r#move(MODEL_TABLE_NAME, &to_data_folder, MODEL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The schema of {MODEL_TABLE_NAME} \
                does not match the schema of {MODEL_TABLE_NAME}."
            )
        );

        assert_table_not_moved(
            &mut from_data_folder,
            MODEL_TABLE_NAME,
            sorted_model_table_data(),
            Some(&mut to_data_folder),
            Some(MODEL_TABLE_NAME),
        )
        .await;
    }

    #[tokio::test]
    async fn test_move_model_table_to_normal_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;
        let (_temp_dir, mut to_data_folder) = create_data_folder_with_normal_table().await;

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let result = from_data_folder
            .r#move(MODEL_TABLE_NAME, &to_data_folder, NORMAL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: {MODEL_TABLE_NAME} and {NORMAL_TABLE_NAME} \
                are not both normal tables or model tables."
            )
        );

        assert_table_not_moved(
            &mut from_data_folder,
            MODEL_TABLE_NAME,
            sorted_model_table_data(),
            Some(&mut to_data_folder),
            Some(NORMAL_TABLE_NAME),
        )
        .await;
    }

    #[tokio::test]
    async fn test_move_model_table_to_missing_table() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_model_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let to_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        from_data_folder
            .write(MODEL_TABLE_NAME, model_table_data())
            .await
            .unwrap();

        let result = from_data_folder
            .r#move(MODEL_TABLE_NAME, &to_data_folder, MISSING_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: {MODEL_TABLE_NAME} and {MISSING_TABLE_NAME} \
                are not both normal tables or model tables."
            )
        );

        assert_table_not_moved(
            &mut from_data_folder,
            MODEL_TABLE_NAME,
            sorted_model_table_data(),
            None,
            None,
        )
        .await;
    }

    async fn assert_table_not_moved(
        from_data_folder: &mut DataFolder,
        from_table_name: &str,
        expected_result: RecordBatch,
        maybe_to_data_folder: Option<&mut DataFolder>,
        maybe_to_table_name: Option<&str>,
    ) {
        let from_sql = format!("SELECT * FROM {from_table_name}");
        let from_actual_result = data_folder_read(from_data_folder, &from_sql).await.unwrap();
        assert_eq!(from_actual_result, expected_result);

        if let (Some(to_data_folder), Some(to_table_name)) =
            (maybe_to_data_folder, maybe_to_table_name)
        {
            let to_sql = format!("SELECT * FROM {to_table_name}");
            let to_actual_result = data_folder_read(to_data_folder, &to_sql).await.unwrap();
            assert_eq!(to_actual_result.num_rows(), 0);
        }
    }

    async fn data_folder_read(data_folder: &mut DataFolder, sql: &str) -> Result<RecordBatch> {
        let record_batch_stream = data_folder.read(sql).await?;
        record_batch_stream_to_record_batch(record_batch_stream).await
    }

    #[tokio::test]
    async fn test_move_missing_table_to_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut from_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let (_temp_dir, to_data_folder) = create_data_folder_with_model_table().await;

        let result = from_data_folder
            .r#move(MISSING_TABLE_NAME, &to_data_folder, MODEL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: {MISSING_TABLE_NAME} and {MODEL_TABLE_NAME} \
                are not both normal tables or model tables."
            )
        );
    }

    #[tokio::test]
    async fn test_move_from_data_folder_to_client() {
        let (_temp_dir, mut from_data_folder) = create_data_folder_with_normal_table().await;
        let to_client = lazy_modelardb_client();

        let result = from_data_folder
            .r#move(NORMAL_TABLE_NAME, &to_client, NORMAL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: to_modelardb is not a data folder.".to_owned()
        );
    }

    async fn create_data_folder_with_normal_table() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        data_folder
            .create(
                NORMAL_TABLE_NAME,
                TableType::NormalTable(normal_table_schema()),
            )
            .await
            .unwrap();

        (temp_dir, data_folder)
    }

    fn lazy_modelardb_client() -> Client {
        Client {
            node: Node::Server("localhost".to_owned()),
            flight_client: FlightServiceClient::new(
                Channel::from_static("localhost").connect_lazy(),
            ),
        }
    }

    fn normal_table_data() -> RecordBatch {
        let ids = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let names = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]);
        let ages = Int8Array::from(vec![20, 30, 40, 25, 35, 45]);
        let heights = Int16Array::from(vec![160, 170, 180, 165, 175, 185]);
        let weights = Int16Array::from(vec![60, 70, 80, 65, 75, 85]);

        RecordBatch::try_new(
            Arc::new(normal_table_schema()),
            vec![
                Arc::new(ids),
                Arc::new(names),
                Arc::new(ages),
                Arc::new(heights),
                Arc::new(weights),
            ],
        )
        .unwrap()
    }

    fn normal_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int8, false),
            Field::new("height", DataType::Int16, false),
            Field::new("weight", DataType::Int16, false),
        ])
    }

    async fn create_data_folder_with_model_table() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let table_type =
            TableType::ModelTable(model_table_schema(), HashMap::new(), HashMap::new());

        data_folder
            .create(MODEL_TABLE_NAME, table_type)
            .await
            .unwrap();

        (temp_dir, data_folder)
    }

    fn sorted_model_table_data() -> RecordBatch {
        let sort_options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let physical_sort_exprs = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("tag_1", 1)),
                options: sort_options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("tag_2", 2)),
                options: sort_options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("timestamp", 0)),
                options: sort_options,
            },
        ];

        sort::sort_batch(
            &model_table_data(),
            &LexOrdering::new(physical_sort_exprs),
            None,
        )
        .unwrap()
    }

    fn model_table_data() -> RecordBatch {
        let timestamps = TimestampArray::from(vec![100, 100, 200, 200, 300, 300]);
        let tag_1 = StringArray::from(vec!["tag_x", "tag_y", "tag_x", "tag_y", "tag_x", "tag_y"]);
        let tag_2 = StringArray::from(vec!["tag_a", "tag_b", "tag_a", "tag_b", "tag_a", "tag_b"]);
        let field_1 = ValueArray::from(vec![37.0, 73.0, 38.0, 72.0, 39.0, 71.0]);
        let field_2 = ValueArray::from(vec![24.0, 56.0, 25.0, 55.0, 26.0, 54.0]);

        RecordBatch::try_new(
            Arc::new(model_table_schema()),
            vec![
                Arc::new(timestamps),
                Arc::new(tag_1),
                Arc::new(tag_2),
                Arc::new(field_1),
                Arc::new(field_2),
            ],
        )
        .unwrap()
    }

    fn model_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("tag_1", DataType::Utf8, false),
            Field::new("tag_2", DataType::Utf8, false),
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
        ])
    }

    async fn create_data_folder_with_model_table_with_generated_column() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let generated_columns = vec![("generated".to_owned(), "field_1 + field_2".to_owned())];
        let table_type = TableType::ModelTable(
            model_table_with_generated_column_schema(),
            HashMap::new(),
            generated_columns.into_iter().collect(),
        );

        data_folder
            .create(MODEL_TABLE_WITH_GENERATED_COLUMN_NAME, table_type)
            .await
            .unwrap();

        (temp_dir, data_folder)
    }

    fn model_table_with_generated_column_schema() -> Schema {
        Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("tag_1", DataType::Utf8, false),
            Field::new("tag_2", DataType::Utf8, false),
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
            Field::new("generated", ArrowValue::DATA_TYPE, false),
        ])
    }

    fn invalid_table_data() -> RecordBatch {
        let timestamps = TimestampArray::from(vec![100, 100, 200, 200, 300, 300]);
        let tag = StringArray::from(vec!["tag_x", "tag_y", "tag_x", "tag_y", "tag_x", "tag_y"]);
        let field = ValueArray::from(vec![37.0, 73.0, 38.0, 72.0, 39.0, 71.0]);

        RecordBatch::try_new(
            Arc::new(invalid_table_schema()),
            vec![Arc::new(timestamps), Arc::new(tag), Arc::new(field)],
        )
        .unwrap()
    }

    fn invalid_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("tag", DataType::Utf8, false),
            Field::new("field", ArrowValue::DATA_TYPE, false),
        ])
    }
}
