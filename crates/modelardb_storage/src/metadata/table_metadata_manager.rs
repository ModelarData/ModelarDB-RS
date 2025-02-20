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

//! Table metadata manager that includes functionality used to access both the server metadata Delta Lake
//! and the manager metadata Delta Lake. Note that the entire server metadata Delta Lake can be accessed
//! through this metadata manager, while it only supports a subset of the manager metadata Delta Lake.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hasher};
use std::path::Path as StdPath;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Int16Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use dashmap::DashMap;
use datafusion::catalog::TableProvider;
use datafusion::common::{DFSchema, ToDFSchema};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::lit;
use datafusion::prelude::{col, SessionContext};
use modelardb_common::test::ERROR_BOUND_ZERO;
use modelardb_types::types::ErrorBound;

use crate::delta_lake::DeltaLake;
use crate::error::{ModelarDbStorageError, Result};
use crate::metadata::model_table_metadata::{GeneratedColumn, ModelTableMetadata};
use crate::{
    register_metadata_table, sql_and_concat, try_convert_bytes_to_schema,
    try_convert_schema_to_bytes,
};

/// Types of tables supported by ModelarDB.
enum TableType {
    NormalTable,
    ModelTable,
}

/// Stores the metadata required for reading from and writing to the normal tables and model tables.
/// The data that needs to be persisted is stored in the metadata Delta Lake.
pub struct TableMetadataManager {
    /// Delta Lake with functionality to read and write to and from the metadata tables.
    delta_lake: DeltaLake,
    /// Cache of tag value hashes used to signify when to persist new unsaved tag combinations.
    tag_value_hashes: DashMap<String, u64>,
    /// Session context used to query the metadata Delta Lake tables using Apache DataFusion.
    session_context: Arc<SessionContext>,
}

impl TableMetadataManager {
    /// Create a new [`TableMetadataManager`] that saves the metadata to `folder_path` and
    /// initialize the metadata tables. If the metadata tables could not be created, return
    /// [`ModelarDbStorageError`].
    pub async fn try_from_path(
        folder_path: &StdPath,
        maybe_session_context: Option<Arc<SessionContext>>,
    ) -> Result<Self> {
        let table_metadata_manager = Self {
            delta_lake: DeltaLake::try_from_local_path(folder_path)?,
            tag_value_hashes: DashMap::new(),
            session_context: maybe_session_context
                .unwrap_or_else(|| Arc::new(SessionContext::new())),
        };

        table_metadata_manager
            .create_and_register_metadata_delta_lake_tables()
            .await?;

        Ok(table_metadata_manager)
    }

    /// Create a new [`TableMetadataManager`] that saves the metadata to a remote object store given
    /// by `connection_info` and initialize the metadata tables. If `connection_info` could not be
    /// parsed, the connection cannot be made, or the metadata tables could not be created, return
    /// [`ModelarDbStorageError`].
    pub async fn try_from_connection_info(
        connection_info: &[u8],
        maybe_session_context: Option<Arc<SessionContext>>,
    ) -> Result<Self> {
        let table_metadata_manager = Self {
            delta_lake: DeltaLake::try_remote_from_connection_info(connection_info)?,
            tag_value_hashes: DashMap::new(),
            session_context: maybe_session_context
                .unwrap_or_else(|| Arc::new(SessionContext::new())),
        };

        table_metadata_manager
            .create_and_register_metadata_delta_lake_tables()
            .await?;

        Ok(table_metadata_manager)
    }

    /// Create a new [`TableMetadataManager`] that saves the metadata to a remote S3-compatible
    /// object store and initialize the metadata tables. If the connection cannot be made or the
    /// metadata tables could not be created, return [`ModelarDbStorageError`].
    pub async fn try_from_s3_configuration(
        endpoint: String,
        bucket_name: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> Result<Self> {
        let table_metadata_manager = Self {
            delta_lake: DeltaLake::try_from_s3_configuration(
                endpoint,
                bucket_name,
                access_key_id,
                secret_access_key,
            )?,
            tag_value_hashes: DashMap::new(),
            session_context: Arc::new(SessionContext::new()),
        };

        table_metadata_manager
            .create_and_register_metadata_delta_lake_tables()
            .await?;

        Ok(table_metadata_manager)
    }

    /// Create a new [`TableMetadataManager`] that saves the metadata to a remote Azure-compatible
    /// object store and initialize the metadata tables. If the connection cannot be made or the
    /// metadata tables could not be created, return [`ModelarDbStorageError`].
    pub async fn try_from_azure_configuration(
        account_name: String,
        access_key: String,
        container_name: String,
    ) -> Result<Self> {
        let table_metadata_manager = Self {
            delta_lake: DeltaLake::try_from_azure_configuration(
                account_name,
                access_key,
                container_name,
            )?,
            tag_value_hashes: DashMap::new(),
            session_context: Arc::new(SessionContext::new()),
        };

        table_metadata_manager
            .create_and_register_metadata_delta_lake_tables()
            .await?;

        Ok(table_metadata_manager)
    }

    /// If they do not already exist, create the tables in the metadata Delta Lake for normal table
    /// and model table metadata and register them with the Apache DataFusion session context.
    /// * The `normal_table_metadata` table contains the metadata for normal tables.
    /// * The `model_table_metadata` table contains the main metadata for model tables.
    /// * The `model_table_hash_table_name` contains a mapping from each tag hash to the name of the
    ///   model table that contains the time series with that tag hash.
    /// * The `model_table_field_columns` table contains the name, index, error bound value, whether
    ///   error bound is relative, and generation expression of the field columns in each model table.
    ///
    /// If the tables exist or were created, return [`Ok`], otherwise return
    /// [`ModelarDbStorageError`].
    async fn create_and_register_metadata_delta_lake_tables(&self) -> Result<()> {
        // Create and register the normal_table_metadata table if it does not exist.
        let delta_table = self
            .delta_lake
            .create_metadata_table(
                "normal_table_metadata",
                &Schema::new(vec![Field::new("table_name", DataType::Utf8, false)]),
            )
            .await?;

        register_metadata_table(&self.session_context, "normal_table_metadata", delta_table)?;

        // Create and register the model_table_metadata table if it does not exist.
        let delta_table = self
            .delta_lake
            .create_metadata_table(
                "model_table_metadata",
                &Schema::new(vec![
                    Field::new("table_name", DataType::Utf8, false),
                    Field::new("query_schema", DataType::Binary, false),
                ]),
            )
            .await?;

        register_metadata_table(&self.session_context, "model_table_metadata", delta_table)?;

        // Create and register the model_table_hash_table_name table if it does not exist.
        let delta_table = self
            .delta_lake
            .create_metadata_table(
                "model_table_hash_table_name",
                &Schema::new(vec![
                    Field::new("hash", DataType::Int64, false),
                    Field::new("table_name", DataType::Utf8, false),
                ]),
            )
            .await?;

        register_metadata_table(
            &self.session_context,
            "model_table_hash_table_name",
            delta_table,
        )?;

        // Create and register the model_table_field_columns table if it does not exist. Note that
        // column_index will only use a maximum of 10 bits. generated_column_expr is NULL if the
        // fields are stored as segments.
        let delta_table = self
            .delta_lake
            .create_metadata_table(
                "model_table_field_columns",
                &Schema::new(vec![
                    Field::new("table_name", DataType::Utf8, false),
                    Field::new("column_name", DataType::Utf8, false),
                    Field::new("column_index", DataType::Int16, false),
                    Field::new("error_bound_value", DataType::Float32, false),
                    Field::new("error_bound_is_relative", DataType::Boolean, false),
                    Field::new("generated_column_expr", DataType::Utf8, true),
                ]),
            )
            .await?;

        register_metadata_table(
            &self.session_context,
            "model_table_field_columns",
            delta_table,
        )?;

        // Register the model_table_name_tags table for each model table.
        for model_table_name in self.model_table_names().await? {
            self.register_tags_table(&model_table_name).await?;
        }

        Ok(())
    }

    /// Register the tags table for the model table with `model_table_name` if it is not already
    /// registered. The tags table is required to be registered to allow querying a model table.
    /// If the tags table could not be registered, [`ModelarDbStorageError`] is returned.
    pub async fn register_tags_table(&self, model_table_name: &str) -> Result<()> {
        let tags_table_name = format!("{}_tags", model_table_name);

        let delta_table = self
            .delta_lake
            .metadata_delta_table(&tags_table_name)
            .await?;

        if !self.session_context.table_exist(&tags_table_name)? {
            register_metadata_table(&self.session_context, &tags_table_name, delta_table)?;
        }

        Ok(())
    }

    /// Return `true` if the table with `table_name` is a normal table, otherwise return `false`.
    pub async fn is_normal_table(&self, table_name: &str) -> Result<bool> {
        Ok(self
            .normal_table_names()
            .await?
            .contains(&table_name.to_owned()))
    }

    /// Return `true` if the table with `table_name` is a model table, otherwise return `false`.
    pub async fn is_model_table(&self, table_name: &str) -> Result<bool> {
        Ok(self
            .model_table_names()
            .await?
            .contains(&table_name.to_owned()))
    }

    /// Return the name of each table currently in the metadata Delta Lake. If the table names
    /// cannot be retrieved, [`ModelarDbStorageError`] is returned.
    pub async fn table_names(&self) -> Result<Vec<String>> {
        let normal_table_names = self.normal_table_names().await?;
        let model_table_names = self.model_table_names().await?;

        let mut table_names = normal_table_names;
        table_names.extend(model_table_names);

        Ok(table_names)
    }

    /// Return the name of each normal table currently in the metadata Delta Lake. Note that this
    /// does not include model tables. If the normal table names cannot be retrieved,
    /// [`ModelarDbStorageError`] is returned.
    pub async fn normal_table_names(&self) -> Result<Vec<String>> {
        self.table_names_of_type(TableType::NormalTable).await
    }

    /// Return the name of each model table currently in the metadata Delta Lake. Note that this
    /// does not include normal tables. If the model table names cannot be retrieved,
    /// [`ModelarDbStorageError`] is returned.
    pub async fn model_table_names(&self) -> Result<Vec<String>> {
        self.table_names_of_type(TableType::ModelTable).await
    }

    /// Return the name of tables of `table_type`. Returns [`ModelarDbStorageError`] if the table
    /// names cannot be retrieved.
    async fn table_names_of_type(&self, table_type: TableType) -> Result<Vec<String>> {
        let table_type = match table_type {
            TableType::NormalTable => "normal_table",
            TableType::ModelTable => "model_table",
        };

        let sql = format!("SELECT table_name FROM {table_type}_metadata");
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        let table_names = modelardb_types::array!(batch, 0, StringArray);
        Ok(table_names.iter().flatten().map(str::to_owned).collect())
    }

    /// Save the created normal table to the metadata Delta Lake. This consists of adding a row to
    /// the `normal_table_metadata` table with the `name` of the table. If the normal table metadata
    /// was saved, return [`Ok`], otherwise return [`ModelarDbStorageError`].
    pub async fn save_normal_table_metadata(&self, name: &str) -> Result<()> {
        self.delta_lake
            .write_columns_to_metadata_table(
                "normal_table_metadata",
                vec![Arc::new(StringArray::from(vec![name]))],
            )
            .await?;

        Ok(())
    }

    /// Save the created model table to the metadata Delta Lake. This includes creating a tags table
    /// for the model table, adding a row to the `model_table_metadata` table, and adding a row to
    /// the `model_table_field_columns` table for each field column.
    pub async fn save_model_table_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
    ) -> Result<()> {
        // Create and register a table_name_tags table to save the 54-bit tag hashes when ingesting data.
        let mut table_name_tags_columns = vec![Field::new("hash", DataType::Int64, false)];

        // Add a column definition for each tag column in the query schema.
        table_name_tags_columns.append(
            &mut model_table_metadata
                .tag_column_indices
                .iter()
                .map(|index| model_table_metadata.query_schema.field(*index).clone())
                .collect::<Vec<Field>>(),
        );

        let tags_table_name = format!("{}_tags", model_table_metadata.name);
        let delta_table = self
            .delta_lake
            .create_metadata_table(&tags_table_name, &Schema::new(table_name_tags_columns))
            .await?;

        register_metadata_table(&self.session_context, &tags_table_name, delta_table)?;

        // Convert the query schema to bytes, so it can be saved in the metadata Delta Lake.
        let query_schema_bytes = try_convert_schema_to_bytes(&model_table_metadata.query_schema)?;

        // Add a new row in the model_table_metadata table to persist the model table.
        self.delta_lake
            .write_columns_to_metadata_table(
                "model_table_metadata",
                vec![
                    Arc::new(StringArray::from(vec![model_table_metadata.name.clone()])),
                    Arc::new(BinaryArray::from_vec(vec![&query_schema_bytes])),
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
            if model_table_metadata.is_field(query_schema_index) {
                let maybe_generated_column_expr = model_table_metadata.generated_columns
                    [query_schema_index]
                    .as_ref()
                    .map(|generated_column| generated_column.original_expr.clone());

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
                self.delta_lake
                    .write_columns_to_metadata_table(
                        "model_table_field_columns",
                        vec![
                            Arc::new(StringArray::from(vec![model_table_metadata.name.clone()])),
                            Arc::new(StringArray::from(vec![field.name().clone()])),
                            Arc::new(Int16Array::from(vec![query_schema_index as i16])),
                            Arc::new(Float32Array::from(vec![error_bound_value])),
                            Arc::new(BooleanArray::from(vec![error_bound_is_relative])),
                            Arc::new(StringArray::from(vec![maybe_generated_column_expr])),
                        ],
                    )
                    .await?;
            }
        }

        Ok(())
    }

    /// Depending on the type of the table with `table_name`, drop either the normal table
    /// metadata or the model table metadata from the metadata Delta Lake. If the table does not
    /// exist or the metadata could not be dropped, [`ModelarDbStorageError`] is returned.
    pub async fn drop_table_metadata(&self, table_name: &str) -> Result<()> {
        if self.is_normal_table(table_name).await? {
            self.drop_normal_table_metadata(table_name).await
        } else if self.is_model_table(table_name).await? {
            self.drop_model_table_metadata(table_name).await
        } else {
            Err(ModelarDbStorageError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            )))
        }
    }

    /// Drop the metadata for the normal table with `table_name` from the `normal_table_metadata`
    /// table in the metadata Delta Lake. If the metadata could not be dropped,
    /// [`ModelarDbStorageError`] is returned.
    async fn drop_normal_table_metadata(&self, table_name: &str) -> Result<()> {
        let delta_ops = self
            .delta_lake
            .metadata_delta_ops("normal_table_metadata")
            .await?;

        delta_ops
            .delete()
            .with_predicate(col("table_name").eq(lit(table_name)))
            .await?;

        Ok(())
    }

    /// Drop the metadata for the model table with `table_name` from the metadata Delta Lake.
    /// This includes dropping the tags table for the model table, deleting a row from the
    /// `model_table_metadata` table, deleting a row from the `model_table_field_columns` table for
    /// each field column, and deleting the tag metadata from the `model_table_hash_table_name` table
    /// and the tag cache. If the metadata could not be dropped, [`ModelarDbStorageError`] is returned.
    async fn drop_model_table_metadata(&self, table_name: &str) -> Result<()> {
        // Drop and deregister the model_table_name_tags table.
        let tags_table_name = format!("{table_name}_tags");
        self.delta_lake
            .drop_metadata_table(&tags_table_name)
            .await?;

        self.session_context.deregister_table(&tags_table_name)?;

        // Delete the table metadata from the model_table_metadata table.
        self.delta_lake
            .metadata_delta_ops("model_table_metadata")
            .await?
            .delete()
            .with_predicate(col("table_name").eq(lit(table_name)))
            .await?;

        // Delete the column metadata from the model_table_field_columns table.
        self.delta_lake
            .metadata_delta_ops("model_table_field_columns")
            .await?
            .delete()
            .with_predicate(col("table_name").eq(lit(table_name)))
            .await?;

        // Delete the tag hash metadata from the metadata Delta Lake and the tag cache.
        self.delete_tag_hash_metadata(table_name).await?;

        Ok(())
    }

    /// Depending on the type of the table with `table_name`, truncate either the normal table
    /// metadata or the model table metadata from the metadata Delta Lake. Note that if truncating
    /// the metadata of a normal table, the metadata Delta Lake is unaffected, but it is allowed to
    /// keep the interface consistent. If the table does not exist or the metadata could not be
    /// truncated, [`ModelarDbStorageError`] is returned.
    pub async fn truncate_table_metadata(&self, table_name: &str) -> Result<()> {
        if self.is_normal_table(table_name).await? {
            Ok(())
        } else if self.is_model_table(table_name).await? {
            self.truncate_model_table_metadata(table_name).await
        } else {
            Err(ModelarDbStorageError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            )))
        }
    }

    /// Truncate the metadata for the model table with `table_name` from the metadata Delta Lake.
    /// This includes truncating the tags table for the model table and deleting the tag metadata
    /// from the `model_table_hash_table_name` table and the tag cache. If the metadata could not
    /// be truncated, [`ModelarDbStorageError`] is returned.
    async fn truncate_model_table_metadata(&self, table_name: &str) -> Result<()> {
        // Truncate the model_table_name_tags table.
        self.delta_lake
            .metadata_delta_ops(&format!("{table_name}_tags"))
            .await?
            .delete()
            .await?;

        // Delete the tag hash metadata from the metadata Delta Lake and the tag cache.
        self.delete_tag_hash_metadata(table_name).await?;

        Ok(())
    }

    /// Delete the tag hash metadata for the model table with `table_name` from the
    /// `model_table_hash_table_name` table and the tag cache. If the metadata could not be deleted,
    /// [`ModelarDbStorageError`] is returned.
    async fn delete_tag_hash_metadata(&self, table_name: &str) -> Result<()> {
        // Delete the tag metadata from the model_table_hash_table_name table.
        self.delta_lake
            .metadata_delta_ops("model_table_hash_table_name")
            .await?
            .delete()
            .with_predicate(col("table_name").eq(lit(table_name)))
            .await?;

        // Delete the tag metadata from the tag cache. The table name is always the last part of
        // the cache key.
        self.tag_value_hashes
            .retain(|key, _| key.split(';').last() != Some(table_name));

        Ok(())
    }

    /// Return the [`ModelTableMetadata`] of each model table currently in the metadata Delta Lake.
    /// If the [`ModelTableMetadata`] cannot be retrieved, [`ModelarDbStorageError`] is returned.
    pub async fn model_table_metadata(&self) -> Result<Vec<Arc<ModelTableMetadata>>> {
        let sql = "SELECT table_name, query_schema FROM model_table_metadata";
        let batch = sql_and_concat(&self.session_context, sql).await?;

        let mut model_table_metadata: Vec<Arc<ModelTableMetadata>> = vec![];
        let table_name_array = modelardb_types::array!(batch, 0, StringArray);
        let query_schema_bytes_array = modelardb_types::array!(batch, 1, BinaryArray);

        for row_index in 0..batch.num_rows() {
            let table_name = table_name_array.value(row_index);
            let query_schema_bytes = query_schema_bytes_array.value(row_index);

            let metadata = self
                .model_table_metadata_row_to_model_table_metadata(table_name, query_schema_bytes)
                .await?;

            model_table_metadata.push(Arc::new(metadata))
        }

        Ok(model_table_metadata)
    }

    /// Return the [`ModelTableMetadata`] for the model table with `table_name` in the metadata
    /// Delta Lake. If the [`ModelTableMetadata`] cannot be retrieved, [`ModelarDbStorageError`] is
    /// returned.
    pub async fn model_table_metadata_for_model_table(
        &self,
        table_name: &str,
    ) -> Result<ModelTableMetadata> {
        let sql = format!(
            "SELECT table_name, query_schema FROM model_table_metadata WHERE table_name = '{table_name}'"
        );
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        if batch.num_rows() == 0 {
            return Err(ModelarDbStorageError::InvalidArgument(format!(
                "No metadata for model table named '{table_name}'."
            )));
        }

        let table_name_array = modelardb_types::array!(batch, 0, StringArray);
        let query_schema_bytes_array = modelardb_types::array!(batch, 1, BinaryArray);

        let table_name = table_name_array.value(0);
        let query_schema_bytes = query_schema_bytes_array.value(0);

        self.model_table_metadata_row_to_model_table_metadata(table_name, query_schema_bytes)
            .await
    }

    /// Convert a row from the table "model_table_metadata" to an instance of
    /// [`ModelTableMetadata`]. Returns [`ModelarDbStorageError`] if a model table with `table_name`
    /// does not exist or the bytes in `query_schema_bytes` are not a valid schema.
    async fn model_table_metadata_row_to_model_table_metadata(
        &self,
        table_name: &str,
        query_schema_bytes: &[u8],
    ) -> Result<ModelTableMetadata> {
        let query_schema = try_convert_bytes_to_schema(query_schema_bytes.into())?;

        let error_bounds = self
            .error_bounds(table_name, query_schema.fields().len())
            .await?;

        let df_query_schema = query_schema.clone().to_dfschema()?;
        let generated_columns = self.generated_columns(table_name, &df_query_schema).await?;

        ModelTableMetadata::try_new(
            table_name.to_owned(),
            Arc::new(query_schema),
            error_bounds,
            generated_columns,
        )
    }

    /// Return the error bounds for the columns in the model table with `table_name`. If a model
    /// table with `table_name` does not exist, [`ModelarDbStorageError`] is returned.
    async fn error_bounds(
        &self,
        table_name: &str,
        query_schema_columns: usize,
    ) -> Result<Vec<ErrorBound>> {
        let sql = format!(
            "SELECT column_index, error_bound_value, error_bound_is_relative
             FROM model_table_field_columns
             WHERE table_name = '{table_name}'
             ORDER BY column_index"
        );
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        let mut column_to_error_bound =
            vec![ErrorBound::try_new_absolute(ERROR_BOUND_ZERO)?; query_schema_columns];

        let column_index_array = modelardb_types::array!(batch, 0, Int16Array);
        let error_bound_value_array = modelardb_types::array!(batch, 1, Float32Array);
        let error_bound_is_relative_array = modelardb_types::array!(batch, 2, BooleanArray);

        for row_index in 0..batch.num_rows() {
            let error_bound_index = column_index_array.value(row_index);
            let error_bound_value = error_bound_value_array.value(row_index);
            let error_bound_is_relative = error_bound_is_relative_array.value(row_index);

            let error_bound = if error_bound_is_relative {
                ErrorBound::try_new_relative(error_bound_value)
            } else {
                ErrorBound::try_new_absolute(error_bound_value)
            }?;

            column_to_error_bound[error_bound_index as usize] = error_bound;
        }

        Ok(column_to_error_bound)
    }

    /// Return the generated columns for the model table with `table_name` and `df_schema`. If a
    /// model table with `table_name` does not exist, [`ModelarDbStorageError`] is returned.
    async fn generated_columns(
        &self,
        table_name: &str,
        df_schema: &DFSchema,
    ) -> Result<Vec<Option<GeneratedColumn>>> {
        let sql = format!(
            "SELECT column_index, generated_column_expr
             FROM model_table_field_columns
             WHERE table_name = '{table_name}'
             ORDER BY column_index"
        );
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        let mut generated_columns = vec![None; df_schema.fields().len()];

        let column_index_array = modelardb_types::array!(batch, 0, Int16Array);
        let generated_column_expr_array = modelardb_types::array!(batch, 1, StringArray);

        for row_index in 0..batch.num_rows() {
            let generated_column_index = column_index_array.value(row_index);
            let generated_column_expr = generated_column_expr_array.value(row_index);

            // If generated_column_expr is null, it is saved as an empty string in the column values.
            if !generated_column_expr.is_empty() {
                let generated_column =
                    GeneratedColumn::try_from_sql_expr(generated_column_expr, df_schema)?;

                generated_columns[generated_column_index as usize] = Some(generated_column);
            }
        }

        Ok(generated_columns)
    }

    /// Return the tag hash for the given list of tag values either by retrieving it from a cache
    /// or, if the combination of tag values is not in the cache, by computing a new hash. If the
    /// hash is not in the cache, it is saved to the cache, persisted to the `model_table_tags`
    /// table if it does not already contain it, and persisted to the `model_table_hash_table_name`
    /// table if it does not already contain it. If the hash was saved to the metadata Delta Lake,
    /// also return [`true`]. If the `model_table_tags` or the `model_table_hash_table_name` table
    /// cannot be accessed, [`ModelarDbStorageError`] is returned.
    pub async fn lookup_or_compute_tag_hash(
        &self,
        model_table_metadata: &ModelTableMetadata,
        tag_values: &[String],
    ) -> Result<(u64, bool)> {
        let cache_key = {
            let mut cache_key_list = tag_values.to_vec();
            cache_key_list.push(model_table_metadata.name.clone());

            cache_key_list.join(";")
        };

        // Check if the tag hash is in the cache. If it is, retrieve it. If it is not, create a new
        // one and save it both in the cache and in the metadata Delta Lake. There is a minor
        // race condition because the check if a tag hash is in the cache and the addition of the
        // hash is done without taking a lock on the tag_value_hashes. However, by allowing a hash
        // to possibly be computed more than once, the cache can be used without an explicit lock.
        if let Some(tag_hash) = self.tag_value_hashes.get(&cache_key) {
            Ok((*tag_hash, false))
        } else {
            // Generate the 54-bit tag hash based on the tag values of the record batch and model
            // table name.
            let tag_hash = {
                let mut hasher = DefaultHasher::new();
                hasher.write(cache_key.as_bytes());

                // The 64-bit hash is shifted to make the 10 least significant bits 0.
                hasher.finish() << 10
            };

            // Save the tag hash in the metadata Delta Lake and in the cache.
            let tag_hash_is_saved = self
                .save_tag_hash_metadata(model_table_metadata, tag_hash, tag_values)
                .await?;

            self.tag_value_hashes.insert(cache_key, tag_hash);

            Ok((tag_hash, tag_hash_is_saved))
        }
    }

    /// Save the given tag hash metadata to the `model_table_tags` table if it does not already
    /// contain it, and to the `model_table_hash_table_name` table if it does not already contain it.
    /// If the tables did not contain the tag hash, meaning it is a new tag combination, return
    /// [`true`]. If the metadata cannot be inserted into either `model_table_tags` or
    /// `model_table_hash_table_name`, [`ModelarDbStorageError`] is returned.
    pub async fn save_tag_hash_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
        tag_hash: u64,
        tag_values: &[String],
    ) -> Result<bool> {
        let table_name = model_table_metadata.name.as_str();
        let tag_columns = &model_table_metadata
            .tag_column_indices
            .iter()
            .map(|index| model_table_metadata.schema.field(*index).name().clone())
            .collect::<Vec<String>>();

        let signed_tag_hash = i64::from_ne_bytes(tag_hash.to_ne_bytes());

        // Save the tag hash metadata in the model_table_tags table if it does not already contain it.
        let mut table_name_tags_columns: Vec<ArrayRef> =
            vec![Arc::new(Int64Array::from(vec![signed_tag_hash]))];

        table_name_tags_columns.append(
            &mut tag_values
                .iter()
                .map(|tag_value| Arc::new(StringArray::from(vec![tag_value.clone()])) as ArrayRef)
                .collect::<Vec<ArrayRef>>(),
        );

        let source = self
            .metadata_table_data_frame(&format!("{table_name}_tags"), table_name_tags_columns)
            .await?;

        let delta_ops = self
            .delta_lake
            .metadata_delta_ops(&format!("{table_name}_tags"))
            .await?;

        // Merge the tag hash metadata in the source DataFrame into the model_table_tags table.
        // For each hash, if the hash is not already in the target table, insert the hash and the
        // tag values from the source DataFrame.
        let (_table, insert_into_tags_metrics) = delta_ops
            .merge(source, col("target.hash").eq(col("source.hash")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_not_matched_insert(|mut insert| {
                for tag_column in tag_columns {
                    insert = insert.set(tag_column, col(format!("source.{tag_column}")))
                }

                insert.set("hash", col("source.hash"))
            })?
            .await?;

        // Save the tag hash metadata in the model_table_hash_table_name table if it does not
        // already contain it.
        let source = self
            .metadata_table_data_frame(
                "model_table_hash_table_name",
                vec![
                    Arc::new(Int64Array::from(vec![signed_tag_hash])),
                    Arc::new(StringArray::from(vec![table_name])),
                ],
            )
            .await?;

        let delta_ops = self
            .delta_lake
            .metadata_delta_ops("model_table_hash_table_name")
            .await?;

        // Merge the tag hash metadata in the source DataFrame into the model_table_hash_table_name
        // table. For each hash, if the hash is not already in the target table, insert the hash and
        // the table name from the source DataFrame.
        let (_table, insert_into_hash_table_name_metrics) = delta_ops
            .merge(source, col("target.hash").eq(col("source.hash")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_not_matched_insert(|insert| {
                insert
                    .set("hash", col("source.hash"))
                    .set("table_name", col("source.table_name"))
            })?
            .await?;

        Ok(insert_into_tags_metrics.num_target_rows_inserted > 0
            || insert_into_hash_table_name_metrics.num_target_rows_inserted > 0)
    }

    /// Return a [`DataFrame`] with the given `rows` for the metadata table with the given
    /// `table_name`. If the table does not exist or the [`DataFrame`] cannot be created, return
    /// [`ModelarDbStorageError`].
    async fn metadata_table_data_frame(
        &self,
        table_name: &str,
        rows: Vec<ArrayRef>,
    ) -> Result<DataFrame> {
        let table = self.delta_lake.metadata_delta_table(table_name).await?;

        // TableProvider::schema(&table) is used instead of table.schema() because table.schema()
        // returns the Delta Lake schema instead of the Apache Arrow DataFusion schema.
        let batch = RecordBatch::try_new(TableProvider::schema(&table), rows)?;

        Ok(self.session_context.read_batch(batch)?)
    }

    /// Return the name of the model table that contains the time series with `tag_hash`. Returns a
    /// [`ModelarDbStorageError`] if the necessary data cannot be retrieved from the metadata Delta
    /// Lake.
    pub async fn tag_hash_to_model_table_name(&self, tag_hash: u64) -> Result<String> {
        let signed_tag_hash = i64::from_ne_bytes(tag_hash.to_ne_bytes());

        let sql = format!(
            "SELECT table_name
             FROM model_table_hash_table_name
             WHERE hash = '{signed_tag_hash}'
             LIMIT 1"
        );
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        let table_names = modelardb_types::array!(batch, 0, StringArray);
        if table_names.is_empty() {
            Err(ModelarDbStorageError::InvalidArgument(format!(
                "No model table contains a time series with tag hash '{tag_hash}'."
            )))
        } else {
            Ok(table_names.value(0).to_owned())
        }
    }

    /// Return a mapping from tag hashes to the tags in the columns with the names in
    /// `tag_column_names` for the time series in the model table with the name `model_table_name`.
    /// Returns a [`ModelarDbStorageError`] if the necessary data cannot be retrieved from the
    /// metadata Delta Lake.
    pub async fn mapping_from_hash_to_tags(
        &self,
        model_table_name: &str,
        tag_column_names: &[&str],
    ) -> Result<HashMap<u64, Vec<String>>> {
        // Return an empty HashMap if no tag column names are passed to keep the signature simple.
        if tag_column_names.is_empty() {
            return Ok(HashMap::new());
        }

        let sql = format!(
            "SELECT hash, {} FROM {model_table_name}_tags",
            tag_column_names.join(","),
        );
        let batch = sql_and_concat(&self.session_context, &sql).await?;

        let hash_array = modelardb_types::array!(batch, 0, Int64Array);

        // For each tag column, get the corresponding column array.
        let tag_arrays: Vec<&StringArray> = tag_column_names
            .iter()
            .enumerate()
            .map(|(index, _tag_column)| modelardb_types::array!(batch, index + 1, StringArray))
            .collect();

        let mut hash_to_tags = HashMap::new();
        for row_index in 0..batch.num_rows() {
            let signed_tag_hash = hash_array.value(row_index);
            let tag_hash = u64::from_ne_bytes(signed_tag_hash.to_ne_bytes());

            // For each tag array, add the row index value to the tags for this tag hash.
            let tags: Vec<String> = tag_arrays
                .iter()
                .map(|tag_array| tag_array.value(row_index).to_owned())
                .collect();

            hash_to_tags.insert(tag_hash, tags);
        }

        Ok(hash_to_tags)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{ArrowPrimitiveType, Field};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::ScalarValue::Int64;
    use datafusion::logical_expr::Expr::Literal;
    use modelardb_types::types::{ArrowTimestamp, ArrowValue};
    use tempfile::TempDir;

    use crate::test;

    // Tests for TableMetadataManager.
    #[tokio::test]
    async fn test_create_metadata_delta_lake_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_from_path(temp_dir.path(), None)
            .await
            .unwrap();

        // Verify that the tables were created, registered, and has the expected columns.
        assert!(metadata_manager
            .session_context
            .sql("SELECT table_name FROM normal_table_metadata")
            .await
            .is_ok());

        assert!(metadata_manager
            .session_context
            .sql("SELECT table_name, query_schema FROM model_table_metadata")
            .await
            .is_ok());

        assert!(metadata_manager
            .session_context
            .sql("SELECT hash, table_name FROM model_table_hash_table_name")
            .await
            .is_ok());

        assert!(metadata_manager
            .session_context
            .sql("SELECT table_name, column_name, column_index, error_bound_value, error_bound_is_relative, \
                  generated_column_expr FROM model_table_field_columns")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_register_tags_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;
        let session_context = &metadata_manager.session_context;

        let tags_table_name = format!("{}_tags", test::MODEL_TABLE_NAME);
        session_context.deregister_table(&tags_table_name).unwrap();
        assert!(!session_context.table_exist(&tags_table_name).unwrap());

        metadata_manager
            .register_tags_table(test::MODEL_TABLE_NAME)
            .await
            .unwrap();

        assert!(session_context.table_exist(&tags_table_name).unwrap());

        // If the table is already registered, it should not be registered again.
        let result = metadata_manager
            .register_tags_table(test::MODEL_TABLE_NAME)
            .await;

        assert!(result.is_ok());
        assert!(session_context.table_exist(&tags_table_name).unwrap());
    }

    #[tokio::test]
    async fn test_register_missing_model_table_tags_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let result = metadata_manager.register_tags_table("missing_table").await;

        assert!(result.is_err());
        assert!(!metadata_manager
            .session_context
            .table_exist("missing_table_tags")
            .unwrap());
    }

    #[tokio::test]
    async fn test_normal_table_is_normal_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_normal_tables().await;
        assert!(metadata_manager
            .is_normal_table("normal_table_1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_model_table_is_not_normal_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;
        assert!(!metadata_manager
            .is_normal_table(test::MODEL_TABLE_NAME)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_model_table_is_model_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;
        assert!(metadata_manager
            .is_model_table(test::MODEL_TABLE_NAME)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_normal_table_is_not_model_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_normal_tables().await;
        assert!(!metadata_manager
            .is_model_table("normal_table_1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_table_names() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_normal_tables().await;

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();

        let table_names = metadata_manager.table_names().await.unwrap();
        assert_eq!(
            table_names,
            vec!["normal_table_2", "normal_table_1", test::MODEL_TABLE_NAME]
        );
    }

    #[tokio::test]
    async fn test_normal_table_names() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_normal_tables().await;

        let normal_table_names = metadata_manager.normal_table_names().await.unwrap();
        assert_eq!(normal_table_names, vec!["normal_table_2", "normal_table_1"]);
    }

    #[tokio::test]
    async fn test_model_table_names() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_names = metadata_manager.model_table_names().await.unwrap();
        assert_eq!(model_table_names, vec![test::MODEL_TABLE_NAME]);
    }

    #[tokio::test]
    async fn test_save_normal_table_metadata() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_normal_tables().await;

        // Retrieve the normal table from the metadata Delta Lake.
        let sql = "SELECT table_name FROM normal_table_metadata ORDER BY table_name";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec!["normal_table_1", "normal_table_2"])
        );
    }

    #[tokio::test]
    async fn test_save_model_table_metadata() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        // Verify that the table was created and has the expected columns.
        let sql = format!("SELECT hash, tag FROM {}_tags", test::MODEL_TABLE_NAME);
        assert!(metadata_manager.session_context.sql(&sql).await.is_ok());

        // Check that a row has been added to the model_table_metadata table.
        let sql = "SELECT table_name, query_schema FROM model_table_metadata";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![test::MODEL_TABLE_NAME])
        );
        assert_eq!(
            **batch.column(1),
            BinaryArray::from_vec(vec![&try_convert_schema_to_bytes(
                &test::model_table_metadata().query_schema
            )
            .unwrap()])
        );

        // Check that a row has been added to the model_table_field_columns table for each field column.
        let sql = "SELECT table_name, column_name, column_index, error_bound_value, error_bound_is_relative, \
                   generated_column_expr FROM model_table_field_columns ORDER BY column_name";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
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
    }

    #[tokio::test]
    async fn test_drop_normal_table_metadata() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_normal_tables().await;

        metadata_manager
            .drop_table_metadata("normal_table_2")
            .await
            .unwrap();

        // Verify that normal_table_2 was deleted from the normal_table_metadata table.
        let sql = "SELECT table_name FROM normal_table_metadata";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(**batch.column(0), StringArray::from(vec!["normal_table_1"]));
    }

    #[tokio::test]
    async fn test_drop_model_table_metadata() {
        let (temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await
            .unwrap();

        metadata_manager
            .drop_table_metadata(test::MODEL_TABLE_NAME)
            .await
            .unwrap();

        // Verify that the tags table was deleted from the Delta Lake.
        let tags_table_name = format!("{}_tags", test::MODEL_TABLE_NAME);
        assert!(!temp_dir
            .path()
            .join("metadata")
            .join(tags_table_name)
            .exists());

        // Verify that the model table was deleted from the model_table_metadata table.
        let sql = "SELECT table_name FROM model_table_metadata";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);

        // Verify that the field columns were deleted from the model_table_field_columns table.
        let sql = "SELECT table_name FROM model_table_field_columns";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);

        // Verify that the tag metadata was deleted from the model_table_hash_table_name table.
        let sql = "SELECT table_name FROM model_table_hash_table_name";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);

        // Verify that the tag cache was cleared.
        assert!(metadata_manager.tag_value_hashes.is_empty());
    }

    #[tokio::test]
    async fn test_drop_table_metadata_for_missing_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_normal_tables().await;

        assert!(metadata_manager
            .drop_table_metadata("missing_table")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_truncate_normal_table_metadata() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_normal_tables().await;

        metadata_manager
            .truncate_table_metadata("normal_table_1")
            .await
            .unwrap();

        // Verify that the metadata Delta Lake was left unchanged.
        let sql = "SELECT table_name FROM normal_table_metadata";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec!["normal_table_2", "normal_table_1"])
        );
    }

    #[tokio::test]
    async fn test_truncate_model_table_metadata() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await
            .unwrap();

        metadata_manager
            .truncate_table_metadata(test::MODEL_TABLE_NAME)
            .await
            .unwrap();

        // Verify that the tags table was truncated.
        let sql = format!("SELECT hash FROM {}_tags", test::MODEL_TABLE_NAME);
        let batch = sql_and_concat(&metadata_manager.session_context, &sql)
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);

        // Verify that the tag metadata was deleted from the model_table_hash_table_name table.
        let sql = "SELECT table_name FROM model_table_hash_table_name";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);

        // Verify that the tag cache was cleared.
        assert!(metadata_manager.tag_value_hashes.is_empty());
    }

    #[tokio::test]
    async fn test_truncate_table_metadata_for_missing_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_normal_tables().await;

        assert!(metadata_manager
            .truncate_table_metadata("missing_table")
            .await
            .is_err());
    }

    async fn create_metadata_manager_and_save_normal_tables() -> (TempDir, TableMetadataManager) {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_from_path(temp_dir.path(), None)
            .await
            .unwrap();

        metadata_manager
            .save_normal_table_metadata("normal_table_1")
            .await
            .unwrap();

        metadata_manager
            .save_normal_table_metadata("normal_table_2")
            .await
            .unwrap();

        (temp_dir, metadata_manager)
    }

    #[tokio::test]
    async fn test_model_table_metadata() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_metadata = metadata_manager.model_table_metadata().await.unwrap();

        assert_eq!(
            model_table_metadata.first().unwrap().name,
            test::model_table_metadata().name,
        );
    }

    #[tokio::test]
    async fn test_model_table_metadata_for_existing_model_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_metadata = metadata_manager
            .model_table_metadata_for_model_table(test::MODEL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(model_table_metadata.name, test::model_table_metadata().name,);
    }

    #[tokio::test]
    async fn test_model_table_metadata_for_missing_model_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_metadata = metadata_manager
            .model_table_metadata_for_model_table("missing_table")
            .await;

        assert!(model_table_metadata.is_err());
    }

    #[tokio::test]
    async fn test_error_bound() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let error_bounds = metadata_manager
            .error_bounds(test::MODEL_TABLE_NAME, 4)
            .await
            .unwrap();

        let values: Vec<f32> = error_bounds
            .iter()
            .map(|error_bound| match error_bound {
                ErrorBound::Absolute(value) => *value,
                ErrorBound::Relative(value) => *value,
            })
            .collect();

        assert_eq!(values, &[0.0, 1.0, 5.0, 0.0]);
    }

    #[tokio::test]
    async fn test_generated_columns() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_from_path(temp_dir.path(), None)
            .await
            .unwrap();

        let query_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
            Field::new("tag", DataType::Utf8, false),
            Field::new("generated_column_1", ArrowValue::DATA_TYPE, false),
            Field::new("generated_column_2", ArrowValue::DATA_TYPE, false),
        ]));

        let error_bounds = vec![
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap();
            query_schema.fields.len()
        ];

        let plus_one_column = Some(GeneratedColumn {
            expr: col("field_1") + Literal(Int64(Some(1))),
            source_columns: vec![1],
            original_expr: "field_1 + 1".to_owned(),
        });

        let addition_column = Some(GeneratedColumn {
            expr: col("field_1") + col("field_2"),
            source_columns: vec![1, 2],
            original_expr: "field_1 + field_2".to_owned(),
        });

        let expected_generated_columns =
            vec![None, None, None, None, plus_one_column, addition_column];

        let model_table_metadata = ModelTableMetadata::try_new(
            "generated_columns_table".to_owned(),
            query_schema,
            error_bounds,
            expected_generated_columns.clone(),
        )
        .unwrap();

        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();

        let df_schema = model_table_metadata.query_schema.to_dfschema().unwrap();
        let generated_columns = metadata_manager
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

    #[tokio::test]
    async fn test_compute_new_tag_hash() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_metadata = test::model_table_metadata();
        let (tag_hash_1, tag_hash_1_is_saved) = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await
            .unwrap();

        let (tag_hash_2, tag_hash_2_is_saved) = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag2".to_owned()])
            .await
            .unwrap();

        assert!(tag_hash_1_is_saved && tag_hash_2_is_saved);

        // The tag hashes should be saved in the cache.
        assert_eq!(metadata_manager.tag_value_hashes.len(), 2);

        // The tag hashes should be saved in the model_table_tags table.
        let sql = format!("SELECT hash, tag FROM {}_tags", test::MODEL_TABLE_NAME);
        let batch = sql_and_concat(&metadata_manager.session_context, &sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            Int64Array::from(vec![
                i64::from_ne_bytes(tag_hash_2.to_ne_bytes()),
                i64::from_ne_bytes(tag_hash_1.to_ne_bytes()),
            ])
        );
        assert_eq!(**batch.column(1), StringArray::from(vec!["tag2", "tag1"]));

        // The tag hashes should be saved in the model_table_hash_table_name table.
        let sql = "SELECT hash, table_name FROM model_table_hash_table_name";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            Int64Array::from(vec![
                i64::from_ne_bytes(tag_hash_2.to_ne_bytes()),
                i64::from_ne_bytes(tag_hash_1.to_ne_bytes()),
            ])
        );
        assert_eq!(
            **batch.column(1),
            StringArray::from(vec![test::MODEL_TABLE_NAME, test::MODEL_TABLE_NAME])
        );
    }

    #[tokio::test]
    async fn test_lookup_existing_tag_hash() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_metadata = test::model_table_metadata();
        let (tag_hash_compute, tag_hash_compute_is_saved) = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await
            .unwrap();

        assert!(tag_hash_compute_is_saved);
        assert_eq!(metadata_manager.tag_value_hashes.len(), 1);

        // When getting the same tag hash again, it should be retrieved from the cache.
        let (tag_hash_lookup, tag_hash_lookup_is_saved) = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await
            .unwrap();

        assert!(!tag_hash_lookup_is_saved);
        assert_eq!(metadata_manager.tag_value_hashes.len(), 1);

        assert_eq!(tag_hash_compute, tag_hash_lookup);
    }

    #[tokio::test]
    async fn test_compute_tag_hash_with_invalid_tag_values() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_metadata = test::model_table_metadata();
        let zero_tags_result = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &[])
            .await;

        let two_tags_result = metadata_manager
            .lookup_or_compute_tag_hash(
                &model_table_metadata,
                &["tag1".to_owned(), "tag2".to_owned()],
            )
            .await;

        assert!(zero_tags_result.is_err());
        assert!(two_tags_result.is_err());

        // The tag hashes should not be saved in either the cache or the metadata Delta Lake.
        assert_eq!(metadata_manager.tag_value_hashes.len(), 0);

        let sql = format!("SELECT hash FROM {}_tags", test::MODEL_TABLE_NAME);
        let batch = sql_and_concat(&metadata_manager.session_context, &sql)
            .await
            .unwrap();

        assert!(batch.column(0).is_empty());

        let sql = "SELECT hash FROM model_table_hash_table_name";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert!(batch.column(0).is_empty());
    }

    #[tokio::test]
    async fn test_tag_hash_to_model_table_name() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_metadata = test::model_table_metadata();
        let (tag_hash, _tag_hash_is_saved) = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await
            .unwrap();

        let table_name = metadata_manager
            .tag_hash_to_model_table_name(tag_hash)
            .await
            .unwrap();

        assert_eq!(table_name, test::MODEL_TABLE_NAME);
    }

    #[tokio::test]
    async fn test_invalid_tag_hash_to_model_table_name() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_from_path(temp_dir.path(), None)
            .await
            .unwrap();

        assert!(metadata_manager
            .tag_hash_to_model_table_name(0)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_mapping_from_hash_to_tags() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_metadata = test::model_table_metadata();
        let (tag_hash_1, _tag_hash_is_saved) = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await
            .unwrap();

        let (tag_hash_2, _tag_hash_is_saved) = metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag2".to_owned()])
            .await
            .unwrap();

        let mapping_from_hash_to_tags = metadata_manager
            .mapping_from_hash_to_tags(test::MODEL_TABLE_NAME, &["tag"])
            .await
            .unwrap();

        assert_eq!(mapping_from_hash_to_tags.len(), 2);
        assert_eq!(
            mapping_from_hash_to_tags.get(&tag_hash_1).unwrap(),
            &vec!["tag1".to_owned()]
        );
        assert_eq!(
            mapping_from_hash_to_tags.get(&tag_hash_2).unwrap(),
            &vec!["tag2".to_owned()]
        );
    }

    #[tokio::test]
    async fn test_mapping_from_hash_to_tags_with_missing_model_table() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let result = metadata_manager
            .mapping_from_hash_to_tags("missing_table", &["tag"])
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mapping_from_hash_to_tags_with_invalid_tag_column() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .lookup_or_compute_tag_hash(&model_table_metadata, &["tag1".to_owned()])
            .await
            .unwrap();

        let result = metadata_manager
            .mapping_from_hash_to_tags(test::MODEL_TABLE_NAME, &["invalid_tag"])
            .await;

        assert!(result.is_err());
    }

    async fn create_metadata_manager_and_save_model_table() -> (TempDir, TableMetadataManager) {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_from_path(temp_dir.path(), None)
            .await
            .unwrap();

        // Save a model table to the metadata Delta Lake.
        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata)
            .await
            .unwrap();

        (temp_dir, metadata_manager)
    }
}
