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

//! Implementation of a [`Context`] that provides access to the system's configuration and
//! components.

use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::SchemaProvider;
use datafusion::prelude::SessionContext;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::metadata::table_metadata_manager::TableMetadataManager;
use modelardb_common::parser::{self, ValidStatement};
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing::info;

use crate::configuration::ConfigurationManager;
use crate::storage::data_sinks::{ModelTableDataSink, TableDataSink};
use crate::storage::StorageEngine;
use crate::{ClusterMode, DataFolders};

/// Provides access to the system's configuration and components.
pub struct Context {
    /// Location of local and remote data.
    pub data_folders: DataFolders,
    /// Updatable configuration of the server.
    pub configuration_manager: Arc<RwLock<ConfigurationManager>>,
    /// Main interface for Apache DataFusion.
    pub session: SessionContext,
    /// Manages all uncompressed and compressed data in the system.
    pub storage_engine: Arc<RwLock<StorageEngine>>,
}

impl Context {
    /// Create the components needed in the [`Context`] and use them to create the [`Context`]. If
    /// a metadata manager or storage engine could not be created, [`ModelarDbError`] is returned.
    pub async fn try_new(
        runtime: Arc<Runtime>,
        data_folders: DataFolders,
        cluster_mode: ClusterMode,
    ) -> Result<Self, ModelarDbError> {
        let configuration_manager = Arc::new(RwLock::new(ConfigurationManager::new(cluster_mode)));

        let session = modelardb_query::create_session_context();

        let storage_engine = Arc::new(RwLock::new(
            StorageEngine::try_new(runtime, data_folders.clone(), &configuration_manager)
                .await
                .map_err(|error| {
                    ModelarDbError::ConfigurationError(format!(
                        "Unable to create a StorageEngine: {error}"
                    ))
                })?,
        ));

        Ok(Context {
            data_folders,
            configuration_manager,
            session,
            storage_engine,
        })
    }

    /// Parse `sql` and create a normal table or a model table based on the SQL. If `sql` is not
    /// valid or the table could not be created, return [`ModelarDbError`].
    pub(crate) async fn parse_and_create_table(&self, sql: &str) -> Result<(), ModelarDbError> {
        // Parse the SQL.
        let statement = parser::tokenize_and_parse_sql(sql)
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Perform semantic checks to ensure the parsed SQL is supported.
        let valid_statement = parser::semantic_checks_for_create_table(statement)
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Create the table or model table if it does not already exist.
        match valid_statement {
            ValidStatement::CreateTable { name, schema } => {
                self.check_if_table_exists(&name).await?;
                self.register_and_save_table(&name, sql, schema).await?;
            }
            ValidStatement::CreateModelTable(model_table_metadata) => {
                self.check_if_table_exists(&model_table_metadata.name)
                    .await?;
                self.register_and_save_model_table(model_table_metadata, sql)
                    .await?;
            }
        }

        Ok(())
    }

    /// Create a normal table, register it with Apache DataFusion, and save it to the Delta Lake. If
    /// the table exists, cannot be registered with Apache DataFusion, or cannot be saved to the
    /// Delta Lake, return [`ModelarDbError`] error.
    async fn register_and_save_table(
        &self,
        table_name: &str,
        sql: &str,
        schema: Schema,
    ) -> Result<(), ModelarDbError> {
        // Create an empty Delta Lake table.
        self.data_folders
            .local_data_folder
            .delta_lake
            .create_delta_lake_table(table_name, &schema)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Register the table with Apache DataFusion.
        self.register_table(table_name).await?;

        // Persist the new table to the Delta Lake.
        self.data_folders
            .local_data_folder
            .table_metadata_manager
            .save_table_metadata(table_name, sql)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        info!("Created table '{}'.", table_name);

        Ok(())
    }

    /// Create a model table, register it in Apache DataFusion, and save it to the Delta Lake. If
    /// the table exists, cannot be registered with Apache DataFusion, or cannot be saved to the
    /// Delta Lake, return [`ModelarDbError`] error.
    async fn register_and_save_model_table(
        &self,
        model_table_metadata: Arc<ModelTableMetadata>,
        sql: &str,
    ) -> Result<(), ModelarDbError> {
        // Create an empty Delta Lake table.
        self.data_folders
            .local_data_folder
            .delta_lake
            .create_delta_lake_model_table(&model_table_metadata.name)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        let table_metadata_manager = self
            .data_folders
            .query_data_folder
            .table_metadata_manager
            .clone();

        // Register the table with Apache DataFusion.
        self.register_model_table(model_table_metadata.clone(), table_metadata_manager)
            .await?;

        // Persist the new model table to the Delta Lake.
        self.data_folders
            .local_data_folder
            .table_metadata_manager
            .save_model_table_metadata(&model_table_metadata, sql)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        info!("Created model table '{}'.", model_table_metadata.name);

        Ok(())
    }

    /// For each normal table saved in the metadata Delta Lake, register the normal table in Apache
    /// DataFusion. If the normal tables could not be retrieved from the metadata Delta Lake or a
    /// normal table could not be registered, return [`ModelarDbError`].
    pub async fn register_tables(&self) -> Result<(), ModelarDbError> {
        // We register the tables in the local data folder to avoid registering tables that
        // TableDataSink cannot write data to.
        let table_names = self
            .data_folders
            .local_data_folder
            .table_metadata_manager
            .table_names()
            .await
            .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?;

        for table_name in table_names {
            self.register_table(&table_name).await?;
        }

        Ok(())
    }

    /// Register the normal table with `table_name` in Apache DataFusion. If the normal table does
    /// not exist or could not be registered with Apache DataFusion, return [`ModelarDbError`].
    async fn register_table(&self, table_name: &str) -> Result<(), ModelarDbError> {
        let delta_table = self
            .data_folders
            .query_data_folder
            .delta_lake
            .delta_table(table_name)
            .await
            .map_err(|error| ModelarDbError::ConfigurationError(error.to_string()))?;

        let table_data_sink = Arc::new(TableDataSink::new(
            table_name.to_owned(),
            self.storage_engine.clone(),
        ));

        modelardb_query::register_table(&self.session, table_name, delta_table, table_data_sink)?;

        info!("Registered table '{table_name}'.");

        Ok(())
    }

    /// For each model table saved in the metadata Delta Lake, register the model table in Apache
    /// DataFusion. If the model tables could not be retrieved from the metadata Delta Lake or a
    /// model table could not be registered, return [`ModelarDbError`].
    pub async fn register_model_tables(&self) -> Result<(), ModelarDbError> {
        // We register the model tables in the local data folder to avoid registering tables that
        // ModelTableDataSink cannot write data to.
        let model_table_metadata = self
            .data_folders
            .local_data_folder
            .table_metadata_manager
            .model_table_metadata()
            .await
            .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?;

        let table_metadata_manager = &self.data_folders.query_data_folder.table_metadata_manager;
        for metadata in model_table_metadata {
            self.register_model_table(metadata, table_metadata_manager.clone())
                .await?;
        }

        Ok(())
    }

    /// Register the model table with `model_table_metadata` from `table_metadata_manager` in Apache
    /// DataFusion. If the model table does not exist or could not be registered with Apache
    /// DataFusion, return [`ModelarDbError`].
    async fn register_model_table(
        &self,
        model_table_metadata: Arc<ModelTableMetadata>,
        table_metadata_manager: Arc<TableMetadataManager>,
    ) -> Result<(), ModelarDbError> {
        let delta_table = self
            .data_folders
            .query_data_folder
            .delta_lake
            .delta_table(&model_table_metadata.name)
            .await
            .map_err(|error| ModelarDbError::ConfigurationError(error.to_string()))?;

        let model_table_data_sink = Arc::new(ModelTableDataSink::new(
            model_table_metadata.clone(),
            self.storage_engine.clone(),
        ));

        modelardb_query::register_model_table(
            &self.session,
            delta_table,
            model_table_metadata.clone(),
            table_metadata_manager,
            model_table_data_sink,
        )?;

        info!("Registered model table '{}'.", &model_table_metadata.name);

        Ok(())
    }

    /// Drop the table with `table_name` if it exists. The table is deregistered from the Apache
    /// Arrow Datafusion session and deleted from the storage engine, Delta Lake, and metadata
    /// Delta Lake. If the table does not exist or if it could not be dropped, [`ModelarDbError`]
    /// is returned.
    pub async fn drop_table(&self, table_name: &str) -> Result<(), ModelarDbError> {
        let table_metadata_manager = &self.data_folders.local_data_folder.table_metadata_manager;

        if table_metadata_manager
            .table_names()
            .await
            .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?
            .contains(&table_name.to_owned())
        {
            self.deregister_and_delete_table(table_name).await
        } else if table_metadata_manager
            .model_table_names()
            .await
            .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?
            .contains(&table_name.to_owned())
        {
            self.deregister_and_delete_model_table(table_name).await
        } else {
            Err(ModelarDbError::TableError(format!(
                "Table with name '{table_name}' does not exist."
            )))
        }
    }

    /// Deregister the table with `table_name` from the Apache Arrow Datafusion session and delete
    /// it from the storage engine, Delta Lake, and metadata Delta Lake. If the table could not be
    /// deregistered and deleted, [`ModelarDbError`] is returned.
    async fn deregister_and_delete_table(&self, table_name: &str) -> Result<(), ModelarDbError> {
        // TODO: Remove the table from the data transfer component.
        // TODO: Check if it needs to be removed other places in the storage engine.

        // Deregister the table from the Apache DataFusion session.
        self.session
            .deregister_table(table_name)
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Delete the table metadata from the metadata Delta Lake.
        self.data_folders
            .local_data_folder
            .table_metadata_manager
            .delete_table_metadata(table_name)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Delete the table from the Delta Lake.
        self.data_folders
            .local_data_folder
            .delta_lake
            .drop_delta_lake_table(table_name)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        Ok(())
    }

    /// Deregister the model table with `table_name` from the Apache Arrow Datafusion session and
    /// delete it from the storage engine, Delta Lake, and metadata Delta Lake. If the model table
    /// could not be deregistered and deleted, [`ModelarDbError`] is returned.
    async fn deregister_and_delete_model_table(
        &self,
        table_name: &str,
    ) -> Result<(), ModelarDbError> {
        // TODO: Drop the model table from the storage engine.

        // Deregister the model table from the Apache DataFusion session.
        self.session
            .deregister_table(table_name)
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Delete the model table metadata from the metadata Delta Lake.
        self.data_folders
            .local_data_folder
            .table_metadata_manager
            .delete_model_table_metadata(table_name)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Delete the model table from the Delta Lake.
        self.data_folders
            .local_data_folder
            .delta_lake
            .drop_delta_lake_table(table_name)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        Ok(())
    }

    /// Lookup the [`ModelTableMetadata`] of the model table with name `table_name` if it exists.
    /// Specifically, the method returns:
    /// * [`ModelTableMetadata`] if a model table with the name `table_name` exists.
    /// * [`None`] if a table with the name `table_name` exists.
    /// * [`ModelarDbError`] if the default catalog, the default schema, a table with the name
    ///   `table_name`, or a model table with the name `table_name` does not exist.
    pub async fn model_table_metadata_from_default_database_schema(
        &self,
        table_name: &str,
    ) -> Result<Option<Arc<ModelTableMetadata>>, ModelarDbError> {
        let database_schema = self.default_database_schema()?;

        let maybe_model_table = database_schema
            .table(table_name)
            .await
            .map_err(|error| {
                ModelarDbError::DataRetrievalError(format!(
                    "Failed to retrieve metadata for '{table_name}' due to: {error}"
                ))
            })?
            .ok_or_else(|| {
                ModelarDbError::DataRetrievalError(format!(
                    "Table with name '{table_name}' does not exist."
                ))
            })?;

        let maybe_model_table_metadata =
            modelardb_query::maybe_model_table_to_model_table_metadata(maybe_model_table);

        Ok(maybe_model_table_metadata)
    }

    /// Return [`ModelarDbError`] if a table named `table_name` exists in the default catalog.
    pub async fn check_if_table_exists(&self, table_name: &str) -> Result<(), ModelarDbError> {
        let maybe_schema = self.schema_of_table_in_default_database_schema(table_name);
        if maybe_schema.await.is_ok() {
            return Err(ModelarDbError::ConfigurationError(format!(
                "Table with name '{table_name}' already exists."
            )));
        }
        Ok(())
    }

    /// Return the schema of `table_name` if the table exists in the default database schema,
    /// otherwise a [`ModelarDbError`] indicating at what level the lookup failed is returned.
    pub async fn schema_of_table_in_default_database_schema(
        &self,
        table_name: &str,
    ) -> Result<SchemaRef, ModelarDbError> {
        let database_schema = self.default_database_schema()?;

        let table = database_schema
            .table(table_name)
            .await
            .map_err(|error| {
                ModelarDbError::DataRetrievalError(format!(
                    "Failed to retrieve schema for '{table_name}' due to: {error}"
                ))
            })?
            .ok_or_else(|| {
                ModelarDbError::DataRetrievalError(format!(
                    "Table with name '{table_name}' does not exist."
                ))
            })?;

        Ok(table.schema())
    }

    /// Return the default database schema if it exists, otherwise a [`ModelarDbError`] indicating
    /// at what level the lookup failed is returned.
    pub fn default_database_schema(&self) -> Result<Arc<dyn SchemaProvider>, ModelarDbError> {
        let session = self.session.clone();

        let catalog = session.catalog("datafusion").ok_or_else(|| {
            ModelarDbError::ImplementationError("Default catalog does not exist.".to_owned())
        })?;

        let schema = catalog.schema("public").ok_or_else(|| {
            ModelarDbError::ImplementationError("Default schema does not exist.".to_owned())
        })?;

        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_common::test;
    use tempfile::TempDir;

    use crate::data_folders::DataFolder;

    #[tokio::test]
    async fn test_parse_and_create_table_with_invalid_sql() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        assert!(context
            .parse_and_create_table("TABLE CREATE table_name(timestamp TIMESTAMP)")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_parse_and_create_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        context
            .parse_and_create_table(test::TABLE_SQL)
            .await
            .unwrap();

        // A Delta Lake log should be created to save the schema.
        let folder_path = temp_dir
            .path()
            .join("tables")
            .join("table_name")
            .join("_delta_log");

        assert!(folder_path.exists());

        // The table should be saved to the metadata Delta Lake.
        let table_names = context
            .data_folders
            .local_data_folder
            .table_metadata_manager
            .table_names()
            .await
            .unwrap();

        assert!(table_names.contains(&"table_name".to_owned()));

        // The table should be registered in the Apache DataFusion catalog.
        assert!(context.check_if_table_exists("table_name").await.is_err());
    }

    #[tokio::test]
    async fn test_parse_and_create_existing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        assert!(context
            .parse_and_create_table(test::TABLE_SQL)
            .await
            .is_ok());

        assert!(context
            .parse_and_create_table(test::TABLE_SQL)
            .await
            .is_err())
    }

    #[tokio::test]
    async fn test_parse_and_create_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        context
            .parse_and_create_table(test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        // The table should be saved to the metadata Delta Lake.
        let model_table_metadata = context
            .data_folders
            .local_data_folder
            .table_metadata_manager
            .model_table_metadata()
            .await
            .unwrap();

        assert_eq!(
            model_table_metadata.first().unwrap().name,
            test::model_table_metadata().name
        );

        // The model table should be registered in the Apache DataFusion catalog.
        assert!(context
            .check_if_table_exists(test::MODEL_TABLE_NAME)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_parse_and_create_existing_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        assert!(context
            .parse_and_create_table(test::MODEL_TABLE_SQL)
            .await
            .is_ok());

        assert!(context
            .parse_and_create_table(test::MODEL_TABLE_SQL)
            .await
            .is_err())
    }

    #[tokio::test]
    async fn test_register_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a table to the metadata Delta Lake.
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        context
            .parse_and_create_table(test::TABLE_SQL)
            .await
            .unwrap();

        // Create a new context to clear the Apache Datafusion catalog.
        let context_2 = create_context(&temp_dir).await;

        // Register the table with Apache DataFusion.
        context_2.register_tables().await.unwrap();
    }

    #[tokio::test]
    async fn test_register_model_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a model table to the metadata Delta Lake.
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        context
            .parse_and_create_table(test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        // Create a new context to clear the Apache Datafusion catalog.
        let context_2 = create_context(&temp_dir).await;

        // Register the table with Apache DataFusion.
        context_2.register_model_tables().await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        context
            .parse_and_create_table(test::TABLE_SQL)
            .await
            .unwrap();

        context.drop_table("table_name").await.unwrap();

        // TODO: The table should be deleted from the storage engine.

        // The table should be deregistered from the Apache DataFusion session.
        assert!(context.check_if_table_exists("table_name").await.is_ok());

        // The table should be deleted from the metadata Delta Lake.
        let table_names = context
            .data_folders
            .local_data_folder
            .table_metadata_manager
            .table_names()
            .await
            .unwrap();

        assert!(table_names.is_empty());

        // The table should be deleted from the Delta Lake.
        assert!(!temp_dir.path().join("tables").exists());
    }

    #[tokio::test]
    async fn test_drop_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        context
            .parse_and_create_table(test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        context.drop_table(test::MODEL_TABLE_NAME).await.unwrap();

        // TODO: The model table should be deleted from the storage engine.

        // The model table should be deregistered from the Apache DataFusion session.
        assert!(context
            .check_if_table_exists(test::MODEL_TABLE_NAME)
            .await
            .is_ok());

        // The model table should be deleted from the metadata Delta Lake.
        let model_table_metadata = context
            .data_folders
            .local_data_folder
            .table_metadata_manager
            .model_table_metadata()
            .await
            .unwrap();

        assert!(model_table_metadata.is_empty());

        // The model table should be deleted from the Delta Lake.
        assert!(!temp_dir.path().join("tables").exists());
    }

    #[tokio::test]
    async fn test_drop_non_existent_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        assert!(context.drop_table("invalid_table_name").await.is_err());
    }

    #[tokio::test]
    async fn test_model_table_metadata_from_default_database_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        context
            .parse_and_create_table(test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        let metadata = context
            .model_table_metadata_from_default_database_schema(test::MODEL_TABLE_NAME)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(metadata.name, test::model_table_metadata().name);
    }

    #[tokio::test]
    async fn test_table_model_table_metadata_from_default_database_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        context
            .parse_and_create_table(test::TABLE_SQL)
            .await
            .unwrap();

        assert!(context
            .model_table_metadata_from_default_database_schema("table_name")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_non_existent_model_table_metadata_from_default_database_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        assert!(context
            .model_table_metadata_from_default_database_schema(test::MODEL_TABLE_NAME)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_check_if_existing_table_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        context
            .parse_and_create_table(test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        assert!(context
            .check_if_table_exists(test::MODEL_TABLE_NAME)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_check_if_non_existent_table_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        assert!(context
            .check_if_table_exists(test::MODEL_TABLE_NAME)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_schema_of_table_in_default_database_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        context
            .parse_and_create_table(test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        let schema = context
            .schema_of_table_in_default_database_schema(test::MODEL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(schema, test::model_table_metadata().schema)
    }

    #[tokio::test]
    async fn test_schema_of_non_existent_table_in_default_database_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        assert!(context
            .schema_of_table_in_default_database_schema(test::MODEL_TABLE_NAME)
            .await
            .is_err())
    }

    /// Create a simple [`Context`] that uses `temp_dir` as the local data folder and query data folder.
    async fn create_context(temp_dir: &TempDir) -> Arc<Context> {
        let local_data_folder = DataFolder::try_from_path(temp_dir.path()).await.unwrap();

        Arc::new(
            Context::try_new(
                Arc::new(Runtime::new().unwrap()),
                DataFolders::new(local_data_folder.clone(), None, local_data_folder),
                ClusterMode::SingleNode,
            )
            .await
            .unwrap(),
        )
    }
}
