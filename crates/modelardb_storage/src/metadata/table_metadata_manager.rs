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

use std::path::Path as StdPath;
use std::sync::Arc;

use arrow::array::{Array, BinaryArray, BooleanArray, Float32Array, Int16Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, ToDFSchema};
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

    /// Save the created model table to the metadata Delta Lake. This includes adding a row to the
    /// `model_table_metadata` table and adding a row to the `model_table_field_columns` table for
    /// each field column.
    pub async fn save_model_table_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
    ) -> Result<()> {
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
    /// This includes deleting a row from the `model_table_metadata` table and deleting a row from
    /// the `model_table_field_columns` table for each field column. If the metadata could not be
    /// dropped, [`ModelarDbStorageError`] is returned.
    async fn drop_model_table_metadata(&self, table_name: &str) -> Result<()> {
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
    /// This includes truncating the tags table for the model table. If the metadata could not be
    /// truncated, [`ModelarDbStorageError`] is returned.
    async fn truncate_model_table_metadata(&self, table_name: &str) -> Result<()> {
        // Truncate the model_table_name_tags table.
        self.delta_lake
            .metadata_delta_ops(&format!("{table_name}_tags"))
            .await?
            .delete()
            .await?;

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
            .sql("SELECT table_name, column_name, column_index, error_bound_value, error_bound_is_relative, \
                  generated_column_expr FROM model_table_field_columns")
            .await
            .is_ok());
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

        metadata_manager
            .drop_table_metadata(test::MODEL_TABLE_NAME)
            .await
            .unwrap();

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
