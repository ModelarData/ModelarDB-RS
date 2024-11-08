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
use std::fmt::{Debug, Formatter};
use std::hash::{DefaultHasher, Hasher};
use std::path::Path as StdPath;
use std::sync::Arc;
use std::{fmt, mem};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Int16Array, Int64Array, StringArray,
};
use arrow::datatypes::Schema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{IpcMessage, SchemaAsIpc};
use dashmap::DashMap;
use datafusion::common::{DFSchema, ToDFSchema};
use datafusion::logical_expr::lit;
use datafusion::prelude::col;
use deltalake::kernel::{DataType, StructField};
use modelardb_types::types::ErrorBound;

use crate::error::{ModelarDbCommonError, Result};
use crate::metadata::model_table_metadata::{GeneratedColumn, ModelTableMetadata};
use crate::metadata::MetadataDeltaLake;
use crate::parser;
use crate::test::ERROR_BOUND_ZERO;

/// Types of tables supported by ModelarDB.
enum TableType {
    NormalTable,
    ModelTable,
}

/// Stores the metadata required for reading from and writing to the normal tables and model tables.
/// The data that needs to be persisted is stored in the metadata Delta Lake.
#[derive(Clone)]
pub struct TableMetadataManager {
    /// Delta Lake with functionality to read and write to and from the metadata tables.
    metadata_delta_lake: MetadataDeltaLake,
    /// Cache of tag value hashes used to signify when to persist new unsaved tag combinations.
    tag_value_hashes: DashMap<String, u64>,
}

impl TableMetadataManager {
    /// Create a new [`TableMetadataManager`] that saves the metadata to `folder_path` and
    /// initialize the metadata tables. If the metadata tables could not be created, return
    /// [`ModelarDbCommonError`].
    pub async fn try_from_path(folder_path: &StdPath) -> Result<Self> {
        let table_metadata_manager = Self {
            metadata_delta_lake: MetadataDeltaLake::from_path(folder_path)?,
            tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_delta_lake_tables()
            .await?;

        Ok(table_metadata_manager)
    }

    /// Create a new [`TableMetadataManager`] that saves the metadata to a remote object store given
    /// by `connection_info` and initialize the metadata tables. If `connection_info` could not be
    /// parsed, the connection cannot be made, or the metadata tables could not be created, return
    /// [`ModelarDbCommonError`].
    pub async fn try_from_connection_info(connection_info: &[u8]) -> Result<Self> {
        let table_metadata_manager = Self {
            metadata_delta_lake: MetadataDeltaLake::try_from_connection_info(connection_info)?,
            tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_delta_lake_tables()
            .await?;

        Ok(table_metadata_manager)
    }

    /// Create a new [`TableMetadataManager`] that saves the metadata to a remote S3-compatible
    /// object store and initialize the metadata tables. If the connection cannot be made or the
    /// metadata tables could not be created, return [`ModelarDbCommonError`].
    pub async fn try_from_s3_configuration(
        endpoint: String,
        bucket_name: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> Result<Self> {
        let table_metadata_manager = Self {
            metadata_delta_lake: MetadataDeltaLake::try_from_s3_configuration(
                endpoint,
                bucket_name,
                access_key_id,
                secret_access_key,
            )?,
            tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_delta_lake_tables()
            .await?;

        Ok(table_metadata_manager)
    }

    /// Create a new [`TableMetadataManager`] that saves the metadata to a remote Azure-compatible
    /// object store and initialize the metadata tables. If the connection cannot be made or the
    /// metadata tables could not be created, return [`ModelarDbCommonError`].
    pub async fn try_from_azure_configuration(
        account_name: String,
        access_key: String,
        container_name: String,
    ) -> Result<Self> {
        let table_metadata_manager = Self {
            metadata_delta_lake: MetadataDeltaLake::try_from_azure_configuration(
                account_name,
                access_key,
                container_name,
            )?,
            tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_delta_lake_tables()
            .await?;

        Ok(table_metadata_manager)
    }

    /// If they do not already exist, create the tables in the metadata Delta Lake for normal table
    /// and model table metadata.
    /// * The `normal_table_metadata` table contains the metadata for normal tables.
    /// * The `model_table_metadata` table contains the main metadata for model tables.
    /// * The `model_table_hash_table_name` contains a mapping from each tag hash to the name of the
    ///   model table that contains the time series with that tag hash.
    /// * The `model_table_field_columns` table contains the name, index, error bound value, whether
    ///   error bound is relative, and generation expression of the field columns in each model table.
    ///
    /// If the tables exist or were created, return [`Ok`], otherwise return
    /// [`ModelarDbCommonError`].
    async fn create_metadata_delta_lake_tables(&self) -> Result<()> {
        // Create the normal_table_metadata table if it does not exist.
        self.metadata_delta_lake
            .create_delta_lake_table(
                "normal_table_metadata",
                vec![
                    StructField::new("table_name", DataType::STRING, false),
                    StructField::new("sql", DataType::STRING, false),
                ],
            )
            .await?;

        // Create the model_table_metadata table if it does not exist.
        self.metadata_delta_lake
            .create_delta_lake_table(
                "model_table_metadata",
                vec![
                    StructField::new("table_name", DataType::STRING, false),
                    StructField::new("query_schema", DataType::BINARY, false),
                    StructField::new("sql", DataType::STRING, false),
                ],
            )
            .await?;

        // Create the model_table_hash_table_name table if it does not exist.
        self.metadata_delta_lake
            .create_delta_lake_table(
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
        self.metadata_delta_lake
            .create_delta_lake_table(
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

    /// Save the created normal table to the metadata Delta Lake. This consists of adding a row to the
    /// `normal_table_metadata` table with the `name` of the table and the `sql` used to create the
    /// table. If the normal table metadata was saved, return [`Ok`], otherwise
    /// return [`ModelarDbCommonError`].
    pub async fn save_normal_table_metadata(&self, name: &str, sql: &str) -> Result<()> {
        self.metadata_delta_lake
            .append_to_table(
                "normal_table_metadata",
                vec![
                    Arc::new(StringArray::from(vec![name])),
                    Arc::new(StringArray::from(vec![sql])),
                ],
            )
            .await?;

        Ok(())
    }

    /// Return the name of each normal table currently in the metadata Delta Lake. Note that this
    /// does not include model tables. If the normal table names cannot be retrieved,
    /// [`ModelarDbCommonError`] is returned.
    pub async fn normal_table_names(&self) -> Result<Vec<String>> {
        self.table_names_of_type(TableType::NormalTable).await
    }

    /// Return the name of each model table currently in the metadata Delta Lake. Note that this
    /// does not include normal tables. If the model table names cannot be retrieved,
    /// [`ModelarDbCommonError`] is returned.
    pub async fn model_table_names(&self) -> Result<Vec<String>> {
        self.table_names_of_type(TableType::ModelTable).await
    }

    /// Return the name of tables of `table_type`. Returns [`ModelarDbCommonError`] if the table
    /// names cannot be retrieved.
    async fn table_names_of_type(&self, table_type: TableType) -> Result<Vec<String>> {
        let table_type = match table_type {
            TableType::NormalTable => "normal_table",
            TableType::ModelTable => "model_table",
        };

        let batch = self
            .metadata_delta_lake
            .query_table(
                &format!("{table_type}_metadata"),
                &format!("SELECT table_name FROM {table_type}_metadata"),
            )
            .await?;

        let table_names = modelardb_types::array!(batch, 0, StringArray);
        Ok(table_names.iter().flatten().map(str::to_owned).collect())
    }

    /// Save the created model table to the metadata Delta Lake. This includes creating a tags table
    /// for the model table, adding a row to the `model_table_metadata` table, and adding a row to
    /// the `model_table_field_columns` table for each field column.
    pub async fn save_model_table_metadata(
        &self,
        model_table_metadata: &ModelTableMetadata,
        sql: &str,
    ) -> Result<()> {
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

        self.metadata_delta_lake
            .create_delta_lake_table(
                format!("{}_tags", model_table_metadata.name).as_str(),
                table_name_tags_columns,
            )
            .await?;

        // Convert the query schema to bytes, so it can be saved in the metadata Delta Lake.
        let query_schema_bytes = try_convert_schema_to_bytes(&model_table_metadata.query_schema)?;

        // Add a new row in the model_table_metadata table to persist the model table.
        self.metadata_delta_lake
            .append_to_table(
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
            if model_table_metadata.is_field(query_schema_index) {
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
                self.metadata_delta_lake
                    .append_to_table(
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

    /// Depending on the type of the table with `table_name`, drop either the normal table
    /// metadata or the model table metadata from the metadata Delta Lake. If the table does not
    /// exist or the metadata could not be dropped, [`ModelarDbCommonError`] is returned.
    pub async fn drop_table_metadata(&self, table_name: &str) -> Result<()> {
        if self
            .normal_table_names()
            .await?
            .contains(&table_name.to_owned())
        {
            self.drop_normal_table_metadata(table_name).await
        } else if self
            .model_table_names()
            .await?
            .contains(&table_name.to_owned())
        {
            self.drop_model_table_metadata(table_name).await
        } else {
            Err(ModelarDbCommonError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            )))
        }
    }

    /// Drop the metadata for the normal table with `table_name` from the `normal_table_metadata`
    /// table in the metadata Delta Lake. If the metadata could not be dropped,
    /// [`ModelarDbCommonError`] is returned.
    async fn drop_normal_table_metadata(&self, table_name: &str) -> Result<()> {
        let ops = self
            .metadata_delta_lake
            .metadata_table_delta_ops("normal_table_metadata")
            .await?;

        ops.delete()
            .with_predicate(col("table_name").eq(lit(table_name)))
            .await?;

        Ok(())
    }

    /// Drop the metadata for the model table with `table_name` from the metadata Delta Lake.
    /// This includes dropping the tags table for the model table, deleting a row from the
    /// `model_table_metadata` table, deleting a row from the `model_table_field_columns` table for
    /// each field column, and deleting the tag metadata from the `model_table_hash_table_name` table
    /// and the tag cache. If the metadata could not be dropped, [`ModelarDbCommonError`] is returned.
    async fn drop_model_table_metadata(&self, table_name: &str) -> Result<()> {
        // Drop the model_table_name_tags table.
        self.metadata_delta_lake
            .drop_delta_lake_table(&format!("{table_name}_tags"))
            .await?;

        // Delete the table metadata from the model_table_metadata table.
        self.metadata_delta_lake
            .metadata_table_delta_ops("model_table_metadata")
            .await?
            .delete()
            .with_predicate(col("table_name").eq(lit(table_name)))
            .await?;

        // Delete the column metadata from the model_table_field_columns table.
        self.metadata_delta_lake
            .metadata_table_delta_ops("model_table_field_columns")
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
    /// truncated, [`ModelarDbCommonError`] is returned.
    pub async fn truncate_table_metadata(&self, table_name: &str) -> Result<()> {
        if self
            .normal_table_names()
            .await?
            .contains(&table_name.to_owned())
        {
            Ok(())
        } else if self
            .model_table_names()
            .await?
            .contains(&table_name.to_owned())
        {
            self.truncate_model_table_metadata(table_name).await
        } else {
            Err(ModelarDbCommonError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            )))
        }
    }

    /// Truncate the metadata for the model table with `table_name` from the metadata Delta Lake.
    /// This includes truncating the tags table for the model table and deleting the tag metadata
    /// from the `model_table_hash_table_name` table and the tag cache. If the metadata could not
    /// be truncated, [`ModelarDbCommonError`] is returned.
    async fn truncate_model_table_metadata(&self, table_name: &str) -> Result<()> {
        // Truncate the model_table_name_tags table.
        self.metadata_delta_lake
            .metadata_table_delta_ops(&format!("{table_name}_tags"))
            .await?
            .delete()
            .await?;

        // Delete the tag hash metadata from the metadata Delta Lake and the tag cache.
        self.delete_tag_hash_metadata(table_name).await?;

        Ok(())
    }

    /// Delete the tag hash metadata for the model table with `table_name` from the
    /// `model_table_hash_table_name` table and the tag cache. If the metadata could not be deleted,
    /// [`ModelarDbCommonError`] is returned.
    async fn delete_tag_hash_metadata(&self, table_name: &str) -> Result<()> {
        // Delete the tag metadata from the model_table_hash_table_name table.
        self.metadata_delta_lake
            .metadata_table_delta_ops("model_table_hash_table_name")
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
    /// If the [`ModelTableMetadata`] cannot be retrieved, [`ModelarDbCommonError`] is returned.
    pub async fn model_table_metadata(&self) -> Result<Vec<Arc<ModelTableMetadata>>> {
        let batch = self
            .metadata_delta_lake
            .query_table(
                "model_table_metadata",
                "SELECT table_name, query_schema FROM model_table_metadata",
            )
            .await?;

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
    /// Delta Lake. If the [`ModelTableMetadata`] cannot be retrieved, [`ModelarDbCommonError`] is
    /// returned.
    pub async fn model_table_metadata_for_model_table(
        &self,
        table_name: &str,
    ) -> Result<ModelTableMetadata> {
        let batch = self
            .metadata_delta_lake
            .query_table(
                "model_table_metadata",
                &format!("SELECT table_name, query_schema FROM model_table_metadata WHERE table_name = '{table_name}'"),
            )
            .await?;

        if batch.num_rows() == 0 {
            return Err(ModelarDbCommonError::InvalidArgument(format!(
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
    /// [`ModelTableMetadata`]. Returns [`ModelarDbCommonError`] if a model table with `table_name`
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
    /// table with `table_name` does not exist, [`ModelarDbCommonError`] is returned.
    async fn error_bounds(
        &self,
        table_name: &str,
        query_schema_columns: usize,
    ) -> Result<Vec<ErrorBound>> {
        let batch = self
            .metadata_delta_lake
            .query_table(
                "model_table_field_columns",
                &format!(
                    "SELECT column_index, error_bound_value, error_bound_is_relative
                     FROM model_table_field_columns
                     WHERE table_name = '{table_name}'
                     ORDER BY column_index"
                ),
            )
            .await?;

        let mut column_to_error_bound =
            vec![ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(); query_schema_columns];

        let column_index_array = modelardb_types::array!(batch, 0, Int16Array);
        let error_bound_value_array = modelardb_types::array!(batch, 1, Float32Array);
        let error_bound_is_relative_array = modelardb_types::array!(batch, 2, BooleanArray);

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

    /// Return the generated columns for the model table with `table_name` and `df_schema`. If a
    /// model table with `table_name` does not exist, [`ModelarDbCommonError`] is returned.
    async fn generated_columns(
        &self,
        table_name: &str,
        df_schema: &DFSchema,
    ) -> Result<Vec<Option<GeneratedColumn>>> {
        let batch = self
            .metadata_delta_lake
            .query_table(
                "model_table_field_columns",
                &format!(
                    "SELECT column_index, generated_column_expr, generated_column_sources
                     FROM model_table_field_columns
                     WHERE table_name = '{table_name}'
                     ORDER BY column_index"
                ),
            )
            .await?;

        let mut generated_columns = vec![None; df_schema.fields().len()];

        let column_index_array = modelardb_types::array!(batch, 0, Int16Array);
        let generated_column_expr_array = modelardb_types::array!(batch, 1, StringArray);
        let generated_column_sources_array = modelardb_types::array!(batch, 2, BinaryArray);

        for row_index in 0..batch.num_rows() {
            let generated_column_index = column_index_array.value(row_index);
            let generated_column_expr = generated_column_expr_array.value(row_index);
            let generated_column_sources = generated_column_sources_array.value(row_index);

            // If generated_column_expr is null, it is saved as an empty string in the column values.
            if !generated_column_expr.is_empty() {
                let expr = parser::parse_sql_expression(df_schema, generated_column_expr)?;

                let generated_column = GeneratedColumn {
                    expr,
                    source_columns: try_convert_slice_u8_to_vec_usize(generated_column_sources)?,
                    original_expr: Some(generated_column_expr.to_owned()),
                };

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
    /// cannot be accessed, [`ModelarDbCommonError`] is returned.
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
    /// `model_table_hash_table_name`, [`ModelarDbCommonError`] is returned.
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
            .metadata_delta_lake
            .metadata_table_data_frame(&format!("{table_name}_tags"), table_name_tags_columns)
            .await?;

        let ops = self
            .metadata_delta_lake
            .metadata_table_delta_ops(&format!("{table_name}_tags"))
            .await?;

        // Merge the tag hash metadata in the source DataFrame into the model_table_tags table.
        // For each hash, if the hash is not already in the target table, insert the hash and the
        // tag values from the source DataFrame.
        let (_table, insert_into_tags_metrics) = ops
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
            .metadata_delta_lake
            .metadata_table_data_frame(
                "model_table_hash_table_name",
                vec![
                    Arc::new(Int64Array::from(vec![signed_tag_hash])),
                    Arc::new(StringArray::from(vec![table_name])),
                ],
            )
            .await?;

        let ops = self
            .metadata_delta_lake
            .metadata_table_delta_ops("model_table_hash_table_name")
            .await?;

        // Merge the tag hash metadata in the source DataFrame into the model_table_hash_table_name
        // table. For each hash, if the hash is not already in the target table, insert the hash and
        // the table name from the source DataFrame.
        let (_table, insert_into_hash_table_name_metrics) = ops
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

    /// Return the name of the model table that contains the time series with `tag_hash`. Returns a
    /// [`ModelarDbCommonError`] if the necessary data cannot be retrieved from the metadata Delta
    /// Lake.
    pub async fn tag_hash_to_model_table_name(&self, tag_hash: u64) -> Result<String> {
        let signed_tag_hash = i64::from_ne_bytes(tag_hash.to_ne_bytes());

        let batch = self
            .metadata_delta_lake
            .query_table(
                "model_table_hash_table_name",
                &format!(
                    "SELECT table_name
                     FROM model_table_hash_table_name
                     WHERE hash = '{signed_tag_hash}'
                     LIMIT 1"
                ),
            )
            .await?;

        let table_names = modelardb_types::array!(batch, 0, StringArray);
        if table_names.is_empty() {
            Err(ModelarDbCommonError::InvalidArgument(format!(
                "No model table contains a time series with tag hash '{tag_hash}'."
            )))
        } else {
            Ok(table_names.value(0).to_owned())
        }
    }

    /// Return a mapping from tag hashes to the tags in the columns with the names in
    /// `tag_column_names` for the time series in the model table with the name `model_table_name`.
    /// Returns a [`ModelarDbCommonError`] if the necessary data cannot be retrieved from the
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

        let batch = self
            .metadata_delta_lake
            .query_table(
                &format!("{model_table_name}_tags"),
                &format!(
                    "SELECT hash, {} FROM {model_table_name}_tags",
                    tag_column_names.join(","),
                ),
            )
            .await?;

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

impl Debug for TableMetadataManager {
    /// Write a string-based representation of the [`TableMetadataManager`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.metadata_delta_lake.location)
    }
}

/// Convert a [`Schema`] to [`Vec<u8>`].
fn try_convert_schema_to_bytes(schema: &Schema) -> Result<Vec<u8>> {
    let options = IpcWriteOptions::default();
    let schema_as_ipc = SchemaAsIpc::new(schema, &options);

    let ipc_message: IpcMessage = schema_as_ipc.try_into()?;

    Ok(ipc_message.0.to_vec())
}

/// Return [`Schema`] if `schema_bytes` can be converted to an Apache Arrow schema, otherwise
/// [`ModelarDbCommonError`].
fn try_convert_bytes_to_schema(schema_bytes: Vec<u8>) -> Result<Schema> {
    let ipc_message = IpcMessage(schema_bytes.into());
    Schema::try_from(ipc_message).map_err(|error| error.into())
}

/// Convert a [`&[usize]`] to a [`Vec<u8>`].
fn convert_slice_usize_to_vec_u8(usizes: &[usize]) -> Vec<u8> {
    usizes.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// Convert a [`&[u8]`] to a [`Vec<usize>`] if the length of `bytes` divides evenly by
/// [`mem::size_of::<usize>()`], otherwise [`ModelarDbCommonError`] is returned.
fn try_convert_slice_u8_to_vec_usize(bytes: &[u8]) -> Result<Vec<usize>> {
    if bytes.len() % mem::size_of::<usize>() != 0 {
        Err(ModelarDbCommonError::InvalidArgument(
            "Bytes is not a vector of usizes.".to_owned(),
        ))
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

    use arrow::datatypes::{ArrowPrimitiveType, Field};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::ScalarValue::Int64;
    use datafusion::logical_expr::Expr::Literal;
    use proptest::{collection, num, prop_assert_eq, proptest};
    use tempfile::TempDir;

    use crate::test;
    use modelardb_types::types::{ArrowTimestamp, ArrowValue};

    // Tests for TableMetadataManager.
    #[tokio::test]
    async fn test_create_metadata_delta_lake_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_from_path(temp_dir.path())
            .await
            .unwrap();

        // Verify that the tables were created, registered, and has the expected columns.
        assert!(metadata_manager
            .metadata_delta_lake
            .session
            .sql("SELECT table_name, sql FROM normal_table_metadata")
            .await
            .is_ok());

        assert!(metadata_manager
            .metadata_delta_lake
            .session
            .sql("SELECT table_name, query_schema, sql FROM model_table_metadata")
            .await
            .is_ok());

        assert!(metadata_manager
            .metadata_delta_lake
            .session
            .sql("SELECT hash, table_name FROM model_table_hash_table_name")
            .await
            .is_ok());

        assert!(metadata_manager
            .metadata_delta_lake
            .session
            .sql("SELECT table_name, column_name, column_index, error_bound_value, error_bound_is_relative,
                 generated_column_expr, generated_column_sources FROM model_table_field_columns")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_save_normal_table_metadata() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_normal_tables().await;

        // Retrieve the normal table from the metadata Delta Lake.
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                "normal_table_metadata",
                "SELECT table_name, sql FROM normal_table_metadata ORDER BY table_name",
            )
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec!["normal_table_1", "normal_table_2"])
        );
        assert_eq!(
            **batch.column(1),
            StringArray::from(vec![
                "CREATE TABLE normal_table_1",
                "CREATE TABLE normal_table_2"
            ])
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
    async fn test_save_model_table_metadata() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_model_table().await;

        // Verify that the tables were created, and has the expected columns.
        assert!(metadata_manager
            .metadata_delta_lake
            .session
            .sql(&format!(
                "SELECT hash, tag FROM {}_tags",
                test::MODEL_TABLE_NAME
            ))
            .await
            .is_ok());

        // Check that a row has been added to the model_table_metadata table.
        let batch = metadata_manager
            .metadata_delta_lake
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
                &test::model_table_metadata().query_schema
            )
            .unwrap()])
        );
        assert_eq!(
            **batch.column(2),
            StringArray::from(vec![test::MODEL_TABLE_SQL])
        );

        // Check that a row has been added to the model_table_field_columns table for each field column.
        let batch = metadata_manager
            .metadata_delta_lake
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

    #[tokio::test]
    async fn test_drop_normal_table_metadata() {
        let (_temp_dir, metadata_manager) = create_metadata_manager_and_save_normal_tables().await;

        metadata_manager
            .drop_table_metadata("normal_table_2")
            .await
            .unwrap();

        // Verify that normal_table_2 was deleted from the normal_table_metadata table.
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                "normal_table_metadata",
                "SELECT table_name FROM normal_table_metadata",
            )
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
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                "model_table_metadata",
                "SELECT table_name FROM model_table_metadata",
            )
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);

        // Verify that the field columns were deleted from the model_table_field_columns table.
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                "model_table_field_columns",
                "SELECT table_name FROM model_table_field_columns",
            )
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);

        // Verify that the tag metadata was deleted from the model_table_hash_table_name table.
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                "model_table_hash_table_name",
                "SELECT table_name FROM model_table_hash_table_name",
            )
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
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                "normal_table_metadata",
                "SELECT table_name FROM normal_table_metadata",
            )
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
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                &format!("{}_tags", test::MODEL_TABLE_NAME),
                &format!("SELECT hash FROM {}_tags", test::MODEL_TABLE_NAME),
            )
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 0);

        // Verify that the tag metadata was deleted from the model_table_hash_table_name table.
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                "model_table_hash_table_name",
                "SELECT table_name FROM model_table_hash_table_name",
            )
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
        let metadata_manager = TableMetadataManager::try_from_path(temp_dir.path())
            .await
            .unwrap();

        metadata_manager
            .save_normal_table_metadata("normal_table_1", "CREATE TABLE normal_table_1")
            .await
            .unwrap();

        metadata_manager
            .save_normal_table_metadata("normal_table_2", "CREATE TABLE normal_table_2")
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
        let metadata_manager = TableMetadataManager::try_from_path(temp_dir.path())
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
            original_expr: Some("field_1 + 1".to_owned()),
        });

        let addition_column = Some(GeneratedColumn {
            expr: col("field_1") + col("field_2"),
            source_columns: vec![1, 2],
            original_expr: Some("field_1 + field_2".to_owned()),
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

        let sql = "CREATE MODEL TABLE generated_columns_table(timestamp TIMESTAMP,
        field_1 FIELD, field_2 FIELD, tag TAG, generated_column_1 GENERATED AS field_1 + 1,
        generated_column_2 GENERATED AS field_1 + field_2)";
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, sql)
            .await
            .unwrap();

        let df_schema = model_table_metadata.query_schema.to_dfschema().unwrap();
        let generated_columns = metadata_manager
            .generated_columns("generated_columns_table", &df_schema)
            .await
            .unwrap();

        assert_eq!(generated_columns, expected_generated_columns);
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
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                &format!("{}_tags", test::MODEL_TABLE_NAME),
                &format!("SELECT hash, tag FROM {}_tags", test::MODEL_TABLE_NAME),
            )
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
        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                "model_table_hash_table_name",
                "SELECT hash, table_name FROM model_table_hash_table_name",
            )
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

        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                &format!("{}_tags", test::MODEL_TABLE_NAME),
                &format!("SELECT hash FROM {}_tags", test::MODEL_TABLE_NAME),
            )
            .await
            .unwrap();

        assert!(batch.column(0).is_empty());

        let batch = metadata_manager
            .metadata_delta_lake
            .query_table(
                "model_table_hash_table_name",
                "SELECT hash FROM model_table_hash_table_name",
            )
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
        let metadata_manager = TableMetadataManager::try_from_path(temp_dir.path())
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
        let metadata_manager = TableMetadataManager::try_from_path(temp_dir.path())
            .await
            .unwrap();

        // Save a model table to the metadata Delta Lake.
        let model_table_metadata = test::model_table_metadata();
        metadata_manager
            .save_model_table_metadata(&model_table_metadata, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        (temp_dir, metadata_manager)
    }

    // Tests for conversion functions.
    #[test]
    fn test_invalid_bytes_to_schema() {
        assert!(try_convert_bytes_to_schema(vec!(1, 2, 4, 8)).is_err());
    }

    #[test]
    fn test_schema_to_bytes_and_bytes_to_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
        ]));

        // Serialize the schema to bytes.
        let bytes = try_convert_schema_to_bytes(&schema).unwrap();

        // Deserialize the bytes to the schema.
        let bytes_schema = try_convert_bytes_to_schema(bytes).unwrap();
        assert_eq!(*schema, bytes_schema);
    }

    proptest! {
        #[test]
        fn test_slice_usize_to_vec_u8_and_slice_u8_to_vec_usize(values in collection::vec(num::usize::ANY, 0..50)) {
            let bytes = convert_slice_usize_to_vec_u8(&values);
            let usizes = try_convert_slice_u8_to_vec_usize(&bytes).unwrap();
            prop_assert_eq!(values, usizes);
        }
    }
}
