/* Copyright 2023 The ModelarDB Contributors
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

//! Table metadata manager that includes functionality used to access both the server metadata delta lake
//! and the manager metadata delta lake. Note that the entire server metadata delta lake can be accessed
//! through this metadata manager, while it only supports a subset of the manager metadata delta lake.

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Int16Array, StringArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::{IpcMessage, SchemaAsIpc};
use dashmap::DashMap;
use datafusion::common::{DFSchema, ToDFSchema};
use datafusion::prelude::SessionContext;
use deltalake::kernel::{DataType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{open_table_with_storage_options, DeltaOps, DeltaTable, DeltaTableError};
use object_store::path::Path;

use crate::array;
use crate::metadata::model_table_metadata::{GeneratedColumn, ModelTableMetadata};
use crate::test::ERROR_BOUND_ZERO;
use crate::types::ErrorBound;

/// The folder storing metadata in the data folders.
const METADATA_FOLDER: &str = "metadata";

// TODO: Create a initialization function that can parse arguments from manager into table metadata manager.
// TODO: Look into using merge builder to save tag hashes if they do not already exist.

/// Stores the metadata required for reading from and writing to the tables and model tables.
/// The data that needs to be persisted is stored in the metadata delta lake.
pub struct TableMetadataManager {
    /// URL to access the base folder of the location where the metadata tables are stored.
    url_scheme: String,
    // TODO: Look into using dash map or StorageOptions.
    /// Storage options used to access delta lake tables in remote object stores.
    storage_options: HashMap<String, String>,
    /// Session used to read from the metadata delta lake using Apache Arrow DataFusion.
    session: SessionContext,
    /// Cache of tag value hashes used to signify when to persist new unsaved tag combinations.
    _tag_value_hashes: DashMap<String, u64>,
}

impl TableMetadataManager {
    /// Create a new table metadata manager that saves the metadata to [`METADATA_FOLDER`] under
    /// `folder_path` and initialize the metadata tables. If the metadata tables could not be
    /// created, return [`DeltaTableError`].
    pub async fn try_new_local_table_metadata_manager(
        folder_path: Path,
    ) -> Result<TableMetadataManager, DeltaTableError> {
        let table_metadata_manager = TableMetadataManager {
            url_scheme: format!("{folder_path}/{METADATA_FOLDER}"),
            storage_options: HashMap::new(),
            session: SessionContext::new(),
            _tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_delta_lake_tables()
            .await?;

        Ok(table_metadata_manager)
    }

    /// Create a new table metadata manager that saves the metadata to [`METADATA_FOLDER`] in a S3
    /// bucket and initialize the metadata tables. If the metadata tables could not be created,
    /// return [`DeltaTableError`].
    pub async fn try_new_s3_table_metadata_manager() -> Result<TableMetadataManager, DeltaTableError>
    {
        deltalake::aws::register_handlers(None);
        let storage_options: HashMap<String, String> = HashMap::from([
            ("REGION".to_owned(), "".to_owned()),
            ("ALLOW_HTTP".to_owned(), "true".to_owned()),
            ("ENDPOINT".to_owned(), "http://localhost:9000".to_owned()),
            ("BUCKET_NAME".to_owned(), "modelardb".to_owned()),
            ("ACCESS_KEY_ID".to_owned(), "minioadmin".to_owned()),
            ("SECRET_ACCESS_KEY".to_owned(), "minioadmin".to_owned()),
            ("AWS_S3_ALLOW_UNSAFE_RENAME".to_owned(), "true".to_owned()),
        ]);

        let table_metadata_manager = TableMetadataManager {
            url_scheme: format!("s3://modelardb/{METADATA_FOLDER}"),
            storage_options,
            session: SessionContext::new(),
            _tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_delta_lake_tables()
            .await?;

        Ok(table_metadata_manager)
    }

    /// If they do not already exist, create the tables in the metadata delta lake used for table and
    /// model table metadata.
    /// * The table_metadata table contains the metadata for tables.
    /// * The model_table_metadata table contains the main metadata for model tables.
    /// * The model_table_hash_table_name contains a mapping from each tag hash to the name of the
    /// model table that contains the time series with that tag hash.
    /// * The model_table_field_columns table contains the name, index, error bound value, whether
    /// error bound is relative, and generation expression of the field columns in each model table.
    /// If the tables exist or were created, return [`Ok`], otherwise return [`DeltaTableError`].
    async fn create_metadata_delta_lake_tables(&self) -> Result<(), DeltaTableError> {
        // Create the table_metadata table if it does not exist.
        self.create_delta_lake_table(
            "table_metadata",
            vec![
                StructField::new("table_name", DataType::STRING, false),
                StructField::new("sql", DataType::STRING, false),
            ],
        )
        .await?;

        // Create the model_table_metadata table if it does not exist.
        self.create_delta_lake_table(
            "model_table_metadata",
            vec![
                StructField::new("table_name", DataType::STRING, false),
                StructField::new("query_schema", DataType::BINARY, false),
                StructField::new("sql", DataType::STRING, false),
            ],
        )
        .await?;

        // Create the model_table_hash_table_name table if it does not exist.
        self.create_delta_lake_table(
            "model_table_hash_table_name",
            vec![
                StructField::new("hash", DataType::LONG, false),
                StructField::new("table_name", DataType::STRING, false),
            ],
        )
        .await?;

        // Create the model_table_field_columns table if it does not exist. Note that column_index
        // will only use a maximum of 10 bits. generated_column_* is NULL if the fields are stored
        // as segments.
        self.create_delta_lake_table(
            "model_table_field_columns",
            vec![
                StructField::new("table_name", DataType::STRING, false),
                StructField::new("column_name", DataType::STRING, false),
                StructField::new("column_index", DataType::SHORT, false),
                StructField::new("error_bound_value", DataType::FLOAT, false),
                StructField::new("error_bound_is_relative", DataType::BOOLEAN, false),
                StructField::new("generated_column_expr", DataType::STRING, true),
                StructField::new("generated_column_sources", DataType::BINARY, true),
            ],
        )
        .await?;

        Ok(())
    }

    /// Save the created table to the metadata delta lake. This consists of adding a row to the
    /// table_metadata table with the `name` of the table and the `sql` used to create the table.
    /// If the table metadata was saved, return the updated [`DeltaTable`], otherwise return [`DeltaTableError`].
    pub async fn save_table_metadata(
        &self,
        name: &str,
        sql: &str,
    ) -> Result<DeltaTable, DeltaTableError> {
        self.append_to_table(
            "table_metadata",
            vec![
                Arc::new(StringArray::from(vec![name])),
                Arc::new(StringArray::from(vec![sql])),
            ],
        )
        .await
    }

    /// Return the name of each table currently in the metadata delta lake. Note that this does not
    /// include model tables. If the table names cannot be retrieved, [`DeltaTableError`] is returned.
    pub async fn table_names(&self) -> Result<Vec<String>, DeltaTableError> {
        let batch = self
            .query_table("table_metadata", "SELECT * FROM table_metadata")
            .await?;

        let table_names = array!(batch, 0, StringArray);
        Ok(table_names.iter().flatten().map(String::from).collect())
    }

    /// Save the created model table to the metadata delta lake. This includes creating a tags table
    /// for the model table, creating a compressed files table for the model table, adding a row to
    /// the model_table_metadata table, and adding a row to the model_table_field_columns table for
    /// each field column.
    pub async fn save_model_table_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
        sql: &str,
    ) -> Result<(), DeltaTableError> {
        // Create a table_name_tags table to save the 54-bit tag hashes when ingesting data.
        let mut table_name_tags_columns = vec![StructField::new("hash", DataType::LONG, false)];

        // Add a column definition for each tag column in the query schema.
        table_name_tags_columns.append(
            &mut model_table_metadata
                .tag_column_indices
                .iter()
                .map(|index| {
                    let field = model_table_metadata.query_schema.field(*index);
                    StructField::new(field.name(), DataType::STRING, false)
                })
                .collect::<Vec<StructField>>(),
        );

        self.create_delta_lake_table(
            format!("{}_tags", model_table_metadata.name).as_str(),
            table_name_tags_columns,
        )
        .await?;

        // Create a table_name_compressed_files table to save the metadata of the table's files.
        self.create_delta_lake_table(
            format!("{}_compressed_files", model_table_metadata.name).as_str(),
            vec![
                StructField::new("file_path", DataType::STRING, false),
                StructField::new("field_column", DataType::SHORT, false),
                StructField::new("size", DataType::LONG, false),
                StructField::new("created_at", DataType::LONG, false),
                StructField::new("start_time", DataType::LONG, false),
                StructField::new("end_time", DataType::LONG, false),
                StructField::new("min_value", DataType::FLOAT, false),
                StructField::new("max_value", DataType::FLOAT, false),
            ],
        )
        .await?;

        // Convert the query schema to bytes, so it can be saved in the metadata delta lake.
        let query_schema_bytes = try_convert_schema_to_bytes(&model_table_metadata.query_schema)?;

        // Add a new row in the model_table_metadata table to persist the model table.
        self.append_to_table(
            "model_table_metadata",
            vec![
                Arc::new(StringArray::from(vec![model_table_metadata.name.clone()])),
                Arc::new(BinaryArray::from_vec(vec![&query_schema_bytes])),
                Arc::new(StringArray::from(vec![sql])),
            ],
        )
        .await?;

        // Add a row for each field column to the model_table_field_columns table.
        for (query_schema_index, field) in model_table_metadata
            .query_schema
            .fields()
            .iter()
            .enumerate()
        {
            // Only add a row for the field if it is not the timestamp or a tag.
            let is_timestamp = query_schema_index == model_table_metadata.timestamp_column_index;
            let in_tag_indices = model_table_metadata
                .tag_column_indices
                .contains(&query_schema_index);

            if !is_timestamp && !in_tag_indices {
                let (generated_column_expr, generated_column_sources) =
                    if let Some(generated_column) =
                        &model_table_metadata.generated_columns[query_schema_index]
                    {
                        (
                            generated_column.original_expr.clone(),
                            Some(convert_slice_usize_to_vec_u8(
                                &generated_column.source_columns,
                            )),
                        )
                    } else {
                        (None, None)
                    };

                // error_bounds matches schema and not query_schema to simplify looking up the error
                // bound during ingestion as it occurs far more often than creation of model tables.
                let (error_bound_value, error_bound_is_relative) =
                    if let Ok(schema_index) = model_table_metadata.schema.index_of(field.name()) {
                        match model_table_metadata.error_bounds[schema_index] {
                            ErrorBound::Absolute(value) => (value, false),
                            ErrorBound::Relative(value) => (value, true),
                        }
                    } else {
                        (0.0, false)
                    };

                // query_schema_index is simply cast as a model table contains at most 1024 columns.
                self.append_to_table(
                    "model_table_field_columns",
                    vec![
                        Arc::new(StringArray::from(vec![model_table_metadata.name.clone()])),
                        Arc::new(StringArray::from(vec![field.name().clone()])),
                        Arc::new(Int16Array::from(vec![query_schema_index as i16])),
                        Arc::new(Float32Array::from(vec![error_bound_value])),
                        Arc::new(BooleanArray::from(vec![error_bound_is_relative])),
                        Arc::new(StringArray::from(vec![generated_column_expr])),
                        Arc::new(BinaryArray::from_opt_vec(vec![
                            generated_column_sources.as_deref()
                        ])),
                    ],
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Return the [`ModelTableMetadata`] of each model table currently in the metadata delta lake.
    /// If the [`ModelTableMetadata`] cannot be retrieved, [`Error`] is returned.
    pub async fn model_table_metadata(
        &self,
    ) -> Result<Vec<Arc<ModelTableMetadata>>, DeltaTableError> {
        let batch = self
            .query_table("model_table_metadata", "SELECT * FROM model_table_metadata")
            .await?;

        let mut model_table_metadata: Vec<Arc<ModelTableMetadata>> = vec![];
        let table_name_array = array!(batch, 0, StringArray);
        let query_schema_bytes_array = array!(batch, 1, BinaryArray);

        for row_index in 0..batch.num_rows() {
            let table_name = table_name_array.value(row_index);

            // Convert the bytes to the concrete types.
            let query_schema_bytes = query_schema_bytes_array.value(row_index);
            let query_schema = try_convert_bytes_to_schema(query_schema_bytes.into())?;

            let error_bounds = self
                .error_bounds(table_name, query_schema.fields().len())
                .await?;

            // unwrap() is safe as the schema is checked before it is written to the metadata delta lake.
            let df_query_schema = query_schema.clone().to_dfschema().unwrap();
            let generated_columns = self.generated_columns(table_name, &df_query_schema).await?;

            // Create model table metadata.
            let metadata = Arc::new(
                ModelTableMetadata::try_new(
                    table_name.to_owned(),
                    Arc::new(query_schema),
                    error_bounds,
                    generated_columns,
                )
                .map_err(|error| DeltaTableError::Generic(error.to_string()))?,
            );

            model_table_metadata.push(metadata)
        }

        Ok(model_table_metadata)
    }

    /// Return the error bounds for the columns in the model table with `table_name`. If a model
    /// table with `table_name` does not exist, [`Error`] is returned.
    async fn error_bounds(
        &self,
        table_name: &str,
        query_schema_columns: usize,
    ) -> Result<Vec<ErrorBound>, DeltaTableError> {
        let batch = self
            .query_table(
                "model_table_field_columns",
                &format!("SELECT * FROM model_table_field_columns WHERE table_name = {table_name} ORDER BY column_index"),
            )
            .await?;

        let mut column_to_error_bound =
            vec![ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(); query_schema_columns];

        let column_index_array = array!(batch, 2, Int16Array);
        let error_bound_value_array = array!(batch, 3, Float32Array);
        let error_bound_is_relative_array = array!(batch, 4, BooleanArray);

        for row_index in 0..batch.num_rows() {
            let error_bound_index = column_index_array.value(row_index);
            let error_bound_value = error_bound_value_array.value(row_index);
            let error_bound_is_relative = error_bound_is_relative_array.value(row_index);

            // unwrap() is safe as the error bounds are checked before they are stored.
            let error_bound = if error_bound_is_relative {
                ErrorBound::try_new_relative(error_bound_value)
            } else {
                ErrorBound::try_new_absolute(error_bound_value)
            }
            .unwrap();

            column_to_error_bound[error_bound_index as usize] = error_bound;
        }

        Ok(column_to_error_bound)
    }

    /// Return the generated columns for the model table with `table_name` and `df_schema`.
    /// If a model table with `table_name` does not exist, [`Error`] is returned.
    async fn generated_columns(
        &self,
        _table_name: &str,
        _df_schema: &DFSchema,
    ) -> Result<Vec<Option<GeneratedColumn>>, DeltaTableError> {
        Err(DeltaTableError::Generic("Unimplemented".to_owned()))
    }

    /// Use `table_name` to create a delta lake table with `columns` in the location given by
    /// `url_scheme` and `storage_options` if it does not already exist. The created table is
    /// registered in the Apache Arrow Datafusion session. If the table could not be created or
    /// registered, return [`DeltaTableError`].
    async fn create_delta_lake_table(
        &self,
        table_name: &str,
        columns: Vec<StructField>,
    ) -> Result<(), DeltaTableError> {
        let table = Arc::new(
            CreateBuilder::new()
                .with_save_mode(SaveMode::Ignore)
                .with_storage_options(self.storage_options.clone())
                .with_table_name(table_name)
                .with_location(format!("{}/{table_name}", self.url_scheme))
                .with_columns(columns)
                .await?,
        );

        self.session.register_table(table_name, table.clone())?;

        Ok(())
    }

    // TODO: Look into optimizing the way we store and access tables in the struct fields (avoid open_table() every time).
    /// Append the columns to the table with the given `table_name`. If the columns are appended to
    /// the table, return the updated [`DeltaTable`], otherwise return [`DeltaTableError`].
    async fn append_to_table(
        &self,
        table_name: &str,
        columns: Vec<ArrayRef>,
    ) -> Result<DeltaTable, DeltaTableError> {
        let table = open_table_with_storage_options(
            format!("{}/{table_name}", self.url_scheme),
            self.storage_options.clone(),
        )
        .await?;

        let table_provider = self.session.table_provider(table_name).await?;
        let batch = RecordBatch::try_new(table_provider.schema(), columns).unwrap();

        let ops = DeltaOps::from(table);
        ops.write(vec![batch]).await
    }

    // TODO: Find a way to avoid having to register the table every time we want to read from it.
    // TODO: Look into issue where we have to always query all columns to match the full schema.
    /// Query the table with the given `table_name` using the given `query`. If the table is queried,
    /// return a [`RecordBatch`] with the query result, otherwise return [`DeltaTableError`].
    async fn query_table(
        &self,
        table_name: &str,
        query: &str,
    ) -> Result<RecordBatch, DeltaTableError> {
        let table = open_table_with_storage_options(
            format!("{}/{table_name}", self.url_scheme),
            self.storage_options.clone(),
        )
        .await?;

        let session = SessionContext::new();
        session.register_table(table_name, Arc::new(table))?;

        let dataframe = session.sql(query).await?;
        let batches = dataframe.collect().await?;

        let table_provider = session.table_provider(table_name).await?;
        let batch = concat_batches(&table_provider.schema(), batches.as_slice())?;

        Ok(batch)
    }
}

/// Convert a [`Schema`] to [`Vec<u8>`].
pub fn try_convert_schema_to_bytes(schema: &Schema) -> Result<Vec<u8>, DeltaTableError> {
    let options = IpcWriteOptions::default();
    let schema_as_ipc = SchemaAsIpc::new(schema, &options);

    let ipc_message: IpcMessage =
        schema_as_ipc
            .try_into()
            .map_err(|error: ArrowError| DeltaTableError::InvalidData {
                violations: vec![error.to_string()],
            })?;

    Ok(ipc_message.0.to_vec())
}

/// Return [`Schema`] if `schema_bytes` can be converted to an Apache Arrow schema, otherwise
/// [`DeltaTableError`].
pub fn try_convert_bytes_to_schema(schema_bytes: Vec<u8>) -> Result<Schema, DeltaTableError> {
    let ipc_message = IpcMessage(schema_bytes.into());
    Schema::try_from(ipc_message).map_err(|error| DeltaTableError::InvalidData {
        violations: vec![error.to_string()],
    })
}

/// Convert a [`&[usize]`] to a [`Vec<u8>`].
pub fn convert_slice_usize_to_vec_u8(usizes: &[usize]) -> Vec<u8> {
    usizes.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// Convert a [`&[u8]`] to a [`Vec<usize>`] if the length of `bytes` divides evenly by
/// [`mem::size_of::<usize>()`], otherwise [`DeltaTableError`] is returned.
pub fn try_convert_slice_u8_to_vec_usize(bytes: &[u8]) -> Result<Vec<usize>, DeltaTableError> {
    if bytes.len() % mem::size_of::<usize>() != 0 {
        Err(DeltaTableError::InvalidData {
            violations: vec!["Bytes is not a vector of usizes".to_owned()],
        })
    } else {
        // unwrap() is safe as bytes divides evenly by mem::size_of::<usize>().
        Ok(bytes
            .chunks(mem::size_of::<usize>())
            .map(|byte_slice| usize::from_le_bytes(byte_slice.try_into().unwrap()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test;

    // Tests for TableMetadataManager.
    #[tokio::test]
    async fn test_create_metadata_delta_lake_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_new_local_table_metadata_manager(
            Path::from_absolute_path(temp_dir.path()).unwrap(),
        )
        .await
        .unwrap();

        // Verify that the tables were created, registered, and has the expected columns.
        assert!(metadata_manager
            .session
            .sql("SELECT table_name, sql FROM table_metadata")
            .await
            .is_ok());

        assert!(metadata_manager
            .session
            .sql("SELECT table_name, query_schema, sql FROM model_table_metadata")
            .await
            .is_ok());

        assert!(metadata_manager
            .session
            .sql("SELECT hash, table_name FROM model_table_hash_table_name")
            .await
            .is_ok());

        assert!(metadata_manager
            .session
            .sql("SELECT table_name, column_name, column_index, error_bound_value, error_bound_is_relative,
                 generated_column_expr, generated_column_sources FROM model_table_field_columns")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_save_table_metadata() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_new_local_table_metadata_manager(
            Path::from_absolute_path(temp_dir.path()).unwrap(),
        )
        .await
        .unwrap();

        metadata_manager
            .save_table_metadata("table_1", "CREATE TABLE table_1")
            .await
            .unwrap();

        metadata_manager
            .save_table_metadata("table_2", "CREATE TABLE table_2")
            .await
            .unwrap();

        // Retrieve the table from the metadata delta lake.
        let batch = metadata_manager
            .query_table(
                "table_metadata",
                "SELECT table_name, sql FROM table_metadata ORDER BY table_name",
            )
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec!["table_1", "table_2"])
        );
        assert_eq!(
            **batch.column(1),
            StringArray::from(vec!["CREATE TABLE table_1", "CREATE TABLE table_2"])
        );
    }

    #[tokio::test]
    async fn test_table_names() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_new_local_table_metadata_manager(
            Path::from_absolute_path(temp_dir.path()).unwrap(),
        )
        .await
        .unwrap();

        metadata_manager
            .save_table_metadata("table_1", "CREATE TABLE table_1")
            .await
            .unwrap();

        metadata_manager
            .save_table_metadata("table_2", "CREATE TABLE table_2")
            .await
            .unwrap();

        let table_names = metadata_manager.table_names().await.unwrap();
        assert_eq!(table_names, vec!["table_2", "table_1"]);
    }

    #[tokio::test]
    async fn test_save_model_table_metadata() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_new_local_table_metadata_manager(
            Path::from_absolute_path(temp_dir.path()).unwrap(),
        )
        .await
        .unwrap();

        // Save a model table to the metadata database.
        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        // Verify that the tables were created, and has the expected columns.
        assert!(metadata_manager
            .session
            .sql(&format!(
                "SELECT hash, tag FROM {}_tags",
                test::MODEL_TABLE_NAME
            ))
            .await
            .is_ok());

        assert!(metadata_manager
            .session
            .sql(&format!(
                "SELECT file_path, field_column, size, created_at, start_time, end_time, min_value,
                 max_value FROM {}_compressed_files",
                test::MODEL_TABLE_NAME
            ))
            .await
            .is_ok());

        // Check that a row has been added to the model_table_metadata table.
        let batch = metadata_manager
            .query_table(
                "model_table_metadata",
                "SELECT table_name, query_schema, sql FROM model_table_metadata",
            )
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![test::MODEL_TABLE_NAME])
        );
        assert_eq!(
            **batch.column(1),
            BinaryArray::from_vec(vec![&try_convert_schema_to_bytes(
                &model_table_metadata.query_schema
            )
            .unwrap()])
        );
        assert_eq!(
            **batch.column(2),
            StringArray::from(vec![test::MODEL_TABLE_SQL])
        );

        // Check that a row has been added to the model_table_field_columns table for each field column.
        let batch = metadata_manager
            .query_table(
                "model_table_field_columns",
                "SELECT table_name, column_name, column_index, error_bound_value, error_bound_is_relative,
                 generated_column_expr, generated_column_sources FROM model_table_field_columns ORDER BY column_name",
            )
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![test::MODEL_TABLE_NAME, test::MODEL_TABLE_NAME])
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
            StringArray::from(vec![None, None] as Vec<Option<&str>>)
        );
        assert_eq!(**batch.column(6), BinaryArray::from(vec![None, None]));
    }
}
