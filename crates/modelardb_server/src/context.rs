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
use modelardb_storage::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_storage::metadata::table_metadata_manager::TableMetadataManager;
use modelardb_storage::parser::{self, ModelarDbStatement};
use sqlparser::ast::CreateTable;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing::info;

use crate::configuration::ConfigurationManager;
use crate::error::{ModelarDbServerError, Result};
use crate::storage::data_sinks::{ModelTableDataSink, NormalTableDataSink};
use crate::storage::StorageEngine;
use crate::{ClusterMode, DataFolders};

/// Provides access to the system's configuration and components.
pub struct Context {
    /// Location of local and remote data.
    pub data_folders: DataFolders,
    /// Updatable configuration of the server.
    pub configuration_manager: Arc<RwLock<ConfigurationManager>>,
    /// Main interface for Apache DataFusion.
    pub session_context: SessionContext,
    /// Manages all uncompressed and compressed data in the system.
    pub storage_engine: Arc<RwLock<StorageEngine>>,
}

impl Context {
    /// Create the components needed in the [`Context`] and use them to create the [`Context`]. If a
    /// metadata manager or storage engine could not be created, [`ModelarDbServerError`] is
    /// returned.
    pub async fn try_new(
        runtime: Arc<Runtime>,
        data_folders: DataFolders,
        cluster_mode: ClusterMode,
    ) -> Result<Self> {
        let configuration_manager = Arc::new(RwLock::new(ConfigurationManager::new(cluster_mode)));

        let session_context = modelardb_storage::create_session_context();

        let storage_engine = Arc::new(RwLock::new(
            StorageEngine::try_new(runtime, data_folders.clone(), &configuration_manager).await?,
        ));

        Ok(Context {
            data_folders,
            configuration_manager,
            session_context,
            storage_engine,
        })
    }

    /// Create a normal table or a model table based on `create_table` created from `sql`. Returns
    /// [`ModelarDbServerError`] if the table could not be created.
    pub(crate) async fn validate_and_create_table(
        &self,
        sql: &str,
        create_table: CreateTable,
    ) -> Result<()> {
        // Perform semantic checks to ensure the statement is supported.
        let valid_statement = parser::semantic_checks_for_create_table(create_table)?;

        // Create the normal table or model table if it does not already exist.
        match valid_statement {
            ModelarDbStatement::CreateTable { name, schema } => {
                self.check_if_table_exists(&name).await?;
                self.register_and_save_normal_table(&name, sql, schema)
                    .await?;
            }
            ModelarDbStatement::CreateModelTable(model_table_metadata) => {
                self.check_if_table_exists(&model_table_metadata.name)
                    .await?;
                self.register_and_save_model_table(model_table_metadata, sql)
                    .await?;
            }
            ModelarDbStatement::Insert(_)
            | ModelarDbStatement::Query(_)
            | ModelarDbStatement::DropTable(_)
            | ModelarDbStatement::TruncateTable(_) => {
                return Err(ModelarDbServerError::InvalidArgument(
                    "Expected CreateTable or CreateModelTable".to_owned(),
                ));
            }
        }

        Ok(())
    }

    /// Create a normal table, register it with Apache DataFusion, and save it to the Delta Lake. If
    /// the normal table exists, cannot be registered with Apache DataFusion, or cannot be saved to
    /// the Delta Lake, return [`ModelarDbServerError`] error.
    async fn register_and_save_normal_table(
        &self,
        table_name: &str,
        sql: &str,
        schema: Schema,
    ) -> Result<()> {
        // Create an empty Delta Lake table.
        self.data_folders
            .local_data_folder
            .delta_lake
            .create_normal_table(table_name, &schema)
            .await?;

        // Register the normal table with Apache DataFusion.
        self.register_normal_table(table_name).await?;

        // Persist the new normal table to the Delta Lake.
        self.data_folders
            .local_data_folder
            .table_metadata_manager
            .save_normal_table_metadata(table_name, sql)
            .await?;

        info!("Created normal table '{}'.", table_name);

        Ok(())
    }

    /// Create a model table, register it in Apache DataFusion, and save it to the Delta Lake. If
    /// the model table exists, cannot be registered with Apache DataFusion, or cannot be saved to
    /// the Delta Lake, return [`ModelarDbServerError`] error.
    async fn register_and_save_model_table(
        &self,
        model_table_metadata: Arc<ModelTableMetadata>,
        sql: &str,
    ) -> Result<()> {
        // Create an empty Delta Lake table.
        self.data_folders
            .local_data_folder
            .delta_lake
            .create_model_table(&model_table_metadata.name)
            .await?;

        let table_metadata_manager = self
            .data_folders
            .query_data_folder
            .table_metadata_manager
            .clone();

        // Register the model table with Apache DataFusion.
        self.register_model_table(model_table_metadata.clone(), table_metadata_manager)
            .await?;

        // Persist the new model table to the Delta Lake.
        self.data_folders
            .local_data_folder
            .table_metadata_manager
            .save_model_table_metadata(&model_table_metadata, sql)
            .await?;

        info!("Created model table '{}'.", model_table_metadata.name);

        Ok(())
    }

    /// For each normal table saved in the metadata Delta Lake, register the normal table in Apache
    /// DataFusion. If the normal tables could not be retrieved from the metadata Delta Lake or a
    /// normal table could not be registered, return [`ModelarDbServerError`].
    pub async fn register_normal_tables(&self) -> Result<()> {
        // We register the normal tables in the local data folder to avoid registering tables that
        // NormalTableDataSink cannot write data to.
        let table_names = self
            .data_folders
            .local_data_folder
            .table_metadata_manager
            .normal_table_names()
            .await?;

        for table_name in table_names {
            self.register_normal_table(&table_name).await?;
        }

        Ok(())
    }

    /// Register the normal table with `table_name` in Apache DataFusion. If the normal table does
    /// not exist or could not be registered with Apache DataFusion, return
    /// [`ModelarDbServerError`].
    async fn register_normal_table(&self, table_name: &str) -> Result<()> {
        let delta_table = self
            .data_folders
            .query_data_folder
            .delta_lake
            .delta_table(table_name)
            .await?;

        let normal_table_data_sink = Arc::new(NormalTableDataSink::new(
            table_name.to_owned(),
            self.storage_engine.clone(),
        ));

        modelardb_storage::register_normal_table(
            &self.session_context,
            table_name,
            delta_table,
            normal_table_data_sink,
        )?;

        info!("Registered normal table '{table_name}'.");

        Ok(())
    }

    /// For each model table saved in the metadata Delta Lake, register the model table in Apache
    /// DataFusion. If the model tables could not be retrieved from the metadata Delta Lake or a
    /// model table could not be registered, return [`ModelarDbServerError`].
    pub async fn register_model_tables(&self) -> Result<()> {
        // We register the model tables in the local data folder to avoid registering tables that
        // ModelTableDataSink cannot write data to.
        let model_table_metadata = self
            .data_folders
            .local_data_folder
            .table_metadata_manager
            .model_table_metadata()
            .await?;

        let table_metadata_manager = &self.data_folders.query_data_folder.table_metadata_manager;
        for metadata in model_table_metadata {
            self.register_model_table(metadata, table_metadata_manager.clone())
                .await?;
        }

        Ok(())
    }

    /// Register the model table with `model_table_metadata` from `table_metadata_manager` in Apache
    /// DataFusion. If the model table does not exist or could not be registered with Apache
    /// DataFusion, return [`ModelarDbServerError`].
    async fn register_model_table(
        &self,
        model_table_metadata: Arc<ModelTableMetadata>,
        table_metadata_manager: Arc<TableMetadataManager>,
    ) -> Result<()> {
        let delta_table = self
            .data_folders
            .query_data_folder
            .delta_lake
            .delta_table(&model_table_metadata.name)
            .await?;

        let model_table_data_sink = Arc::new(ModelTableDataSink::new(
            model_table_metadata.clone(),
            self.storage_engine.clone(),
        ));

        modelardb_storage::register_model_table(
            &self.session_context,
            delta_table,
            model_table_metadata.clone(),
            table_metadata_manager,
            model_table_data_sink,
        )?;

        info!("Registered model table '{}'.", &model_table_metadata.name);

        Ok(())
    }

    /// Drop the table with `table_name` if it exists. The table is deregistered from the Apache
    /// Arrow Datafusion session context and deleted from the storage engine, metadata Delta Lake,
    /// and data Delta Lake. If the table does not exist or if it could not be dropped,
    /// [`ModelarDbServerError`] is returned.
    pub async fn drop_table(&self, table_name: &str) -> Result<()> {
        // Deregistering the table from the Apache DataFusion session context and deleting the table
        // from the storage engine does not require the table to exist, so the table is checked first.
        if self.check_if_table_exists(table_name).await.is_ok() {
            return Err(ModelarDbServerError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            )));
        }

        // Deregister the table from the Apache DataFusion session context. This is done first to
        // avoid data being ingested into the table while it is being deleted.
        self.session_context.deregister_table(table_name)?;

        self.drop_table_from_storage_engine(table_name).await?;

        // Drop the table metadata from the metadata Delta Lake.
        self.data_folders
            .local_data_folder
            .table_metadata_manager
            .drop_table_metadata(table_name)
            .await?;

        // Drop the table from the Delta Lake.
        self.data_folders
            .local_data_folder
            .delta_lake
            .drop_table(table_name)
            .await?;

        Ok(())
    }

    /// Delete all data from the table with `table_name` if it exists. The table data is deleted
    /// from the storage engine, metadata Delta Lake, and data Delta Lake. If the table does not
    /// exist or if it could not be truncated, [`ModelarDbServerError`] is returned.
    pub async fn truncate_table(&self, table_name: &str) -> Result<()> {
        // Deleting the table from the storage engine does not require the table to exist, so the
        // table is checked first.
        if self.check_if_table_exists(table_name).await.is_ok() {
            return Err(ModelarDbServerError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            )));
        }

        self.drop_table_from_storage_engine(table_name).await?;

        // Delete the table metadata from the metadata Delta Lake.
        self.data_folders
            .local_data_folder
            .table_metadata_manager
            .truncate_table_metadata(table_name)
            .await?;

        // Delete the table data from the data Delta Lake.
        self.data_folders
            .local_data_folder
            .delta_lake
            .truncate_table(table_name)
            .await?;

        Ok(())
    }

    /// Drop the table from the storage engine by flushing the data managers and clearing the
    /// table from the data transfer component. The table is marked as dropped in the data transfer
    /// component first to avoid transferring data to the remote data folder when flushing. If the
    /// table could not be dropped, [`ModelarDbServerError`] is returned.
    async fn drop_table_from_storage_engine(&self, table_name: &str) -> Result<()> {
        let storage_engine = self.storage_engine.write().await;
        storage_engine.mark_table_as_dropped(table_name).await;
        storage_engine.flush().await?;
        storage_engine.clear_table(table_name).await;

        Ok(())
    }

    /// Lookup the [`ModelTableMetadata`] of the model table with name `table_name` if it exists.
    /// Specifically, the method returns:
    /// * [`ModelTableMetadata`] if a model table with the name `table_name` exists.
    /// * [`None`] if a normal table with the name `table_name` exists.
    /// * [`ModelarDbServerError`] if the default catalog, the default schema, a normal table with
    ///   the name `table_name`, or a model table with the name `table_name` does not exist.
    pub async fn model_table_metadata_from_default_database_schema(
        &self,
        table_name: &str,
    ) -> Result<Option<Arc<ModelTableMetadata>>> {
        let database_schema = self.default_database_schema()?;

        let maybe_model_table = database_schema.table(table_name).await?.ok_or_else(|| {
            ModelarDbServerError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            ))
        })?;

        let maybe_model_table_metadata =
            modelardb_storage::maybe_table_provider_to_model_table_metadata(maybe_model_table);

        Ok(maybe_model_table_metadata)
    }

    /// Return [`ModelarDbServerError`] if a table named `table_name` exists in the default catalog.
    pub async fn check_if_table_exists(&self, table_name: &str) -> Result<()> {
        let maybe_schema = self.schema_of_table_in_default_database_schema(table_name);
        if maybe_schema.await.is_ok() {
            return Err(ModelarDbServerError::InvalidArgument(format!(
                "Table with name '{table_name}' already exists."
            )));
        }
        Ok(())
    }

    /// Return the schema of `table_name` if the table exists in the default database schema,
    /// otherwise a [`ModelarDbServerError`] indicating at what level the lookup failed is returned.
    pub async fn schema_of_table_in_default_database_schema(
        &self,
        table_name: &str,
    ) -> Result<SchemaRef> {
        let database_schema = self.default_database_schema()?;

        let table = database_schema.table(table_name).await?.ok_or_else(|| {
            ModelarDbServerError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            ))
        })?;

        Ok(table.schema())
    }

    /// Return the default database schema if it exists, otherwise a [`ModelarDbServerError`]
    /// indicating at what level the lookup failed is returned.
    pub fn default_database_schema(&self) -> Result<Arc<dyn SchemaProvider>> {
        let session_context = self.session_context.clone();

        let catalog = session_context.catalog("datafusion").ok_or_else(|| {
            ModelarDbServerError::InvalidState("Default catalog does not exist.".to_owned())
        })?;

        let schema = catalog.schema("public").ok_or_else(|| {
            ModelarDbServerError::InvalidState("Default schema does not exist.".to_owned())
        })?;

        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_storage::test;
    use sqlparser::ast::Statement;
    use tempfile::TempDir;

    use crate::data_folders::DataFolder;

    #[tokio::test]
    async fn test_parse_and_create_table_with_invalid_sql() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        assert!(
            parse_and_create_table(&context, "TABLE CREATE table_name(timestamp TIMESTAMP)")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_parse_and_create_normal_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        parse_and_create_table(&context, test::NORMAL_TABLE_SQL)
            .await
            .unwrap();

        // A Delta Lake log should be created to save the schema.
        let folder_path = temp_dir
            .path()
            .join("tables")
            .join(test::NORMAL_TABLE_NAME)
            .join("_delta_log");

        assert!(folder_path.exists());

        // The normal table should be saved to the metadata Delta Lake.
        assert!(context
            .data_folders
            .local_data_folder
            .table_metadata_manager
            .is_normal_table(test::NORMAL_TABLE_NAME)
            .await
            .unwrap());

        // The normal table should be registered in the Apache DataFusion catalog.
        assert!(context
            .check_if_table_exists(test::NORMAL_TABLE_NAME)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_parse_and_create_existing_normal_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        assert!(parse_and_create_table(&context, test::NORMAL_TABLE_SQL)
            .await
            .is_ok());

        assert!(parse_and_create_table(&context, test::NORMAL_TABLE_SQL)
            .await
            .is_err())
    }

    #[tokio::test]
    async fn test_parse_and_create_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        parse_and_create_table(&context, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        // The model table should be saved to the metadata Delta Lake.
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

        assert!(parse_and_create_table(&context, test::MODEL_TABLE_SQL)
            .await
            .is_ok());

        assert!(parse_and_create_table(&context, test::MODEL_TABLE_SQL)
            .await
            .is_err())
    }

    #[tokio::test]
    async fn test_register_normal_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a normal table to the metadata Delta Lake.
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        parse_and_create_table(&context, test::NORMAL_TABLE_SQL)
            .await
            .unwrap();

        // Create a new context to clear the Apache Datafusion catalog.
        let context_2 = create_context(&temp_dir).await;

        // Register the normal table with Apache DataFusion.
        context_2.register_normal_tables().await.unwrap();
    }

    #[tokio::test]
    async fn test_register_model_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a model table to the metadata Delta Lake.
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        parse_and_create_table(&context, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        // Create a new context to clear the Apache Datafusion catalog.
        let context_2 = create_context(&temp_dir).await;

        // Register the model table with Apache DataFusion.
        context_2.register_model_tables().await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_normal_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        parse_and_create_table(&context, test::NORMAL_TABLE_SQL)
            .await
            .unwrap();

        assert!(context
            .check_if_table_exists(test::NORMAL_TABLE_NAME)
            .await
            .is_err());

        context.drop_table(test::NORMAL_TABLE_NAME).await.unwrap();

        // The normal table should be deregistered from the Apache DataFusion session context.
        assert!(context
            .check_if_table_exists(test::NORMAL_TABLE_NAME)
            .await
            .is_ok());

        // The normal table should be deleted from the metadata Delta Lake.
        assert!(!context
            .data_folders
            .local_data_folder
            .table_metadata_manager
            .is_normal_table(test::NORMAL_TABLE_NAME)
            .await
            .unwrap());

        // The normal table should be deleted from the Delta Lake.
        assert!(!temp_dir.path().join("tables").exists());
    }

    #[tokio::test]
    async fn test_drop_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        parse_and_create_table(&context, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        assert!(context
            .check_if_table_exists(test::MODEL_TABLE_NAME)
            .await
            .is_err());

        context.drop_table(test::MODEL_TABLE_NAME).await.unwrap();

        // The model table should be deregistered from the Apache DataFusion session context.
        assert!(context
            .check_if_table_exists(test::MODEL_TABLE_NAME)
            .await
            .is_ok());

        // The model table should be deleted from the metadata Delta Lake.
        assert!(!context
            .data_folders
            .local_data_folder
            .table_metadata_manager
            .is_model_table(test::MODEL_TABLE_NAME)
            .await
            .unwrap());

        // The model table should be deleted from the Delta Lake.
        assert!(!temp_dir.path().join("tables").exists());
    }

    #[tokio::test]
    async fn test_drop_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        assert!(context.drop_table(test::MODEL_TABLE_NAME).await.is_err());
    }

    #[tokio::test]
    async fn test_truncate_normal_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        parse_and_create_table(&context, test::NORMAL_TABLE_SQL)
            .await
            .unwrap();

        let local_data_folder = &context.data_folders.local_data_folder;
        let mut delta_table = local_data_folder
            .delta_lake
            .delta_table(test::NORMAL_TABLE_NAME)
            .await
            .unwrap();

        // Write data to the normal table that should be deleted when the table is truncated.
        local_data_folder
            .delta_lake
            .write_record_batches_to_normal_table(
                test::NORMAL_TABLE_NAME,
                vec![test::normal_table_record_batch()],
            )
            .await
            .unwrap();

        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 1);

        context
            .truncate_table(test::NORMAL_TABLE_NAME)
            .await
            .unwrap();

        // The normal table should not be deleted from the metadata Delta Lake.
        assert!(local_data_folder
            .table_metadata_manager
            .is_normal_table(test::NORMAL_TABLE_NAME)
            .await
            .unwrap());

        // The normal table data should be deleted from the Delta Lake.
        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);
    }

    #[tokio::test]
    async fn test_truncate_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        parse_and_create_table(&context, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        let local_data_folder = &context.data_folders.local_data_folder;
        let mut delta_table = local_data_folder
            .delta_lake
            .delta_table(test::MODEL_TABLE_NAME)
            .await
            .unwrap();

        // Write data to the model table that should be deleted when the table is truncated.
        let record_batch = test::compressed_segments_record_batch();
        local_data_folder
            .delta_lake
            .write_compressed_segments_to_model_table(test::MODEL_TABLE_NAME, vec![record_batch])
            .await
            .unwrap();

        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 1);

        context
            .truncate_table(test::MODEL_TABLE_NAME)
            .await
            .unwrap();

        // The model table should not be deleted from the metadata Delta Lake.
        assert!(local_data_folder
            .table_metadata_manager
            .is_model_table(test::MODEL_TABLE_NAME)
            .await
            .unwrap());

        // The model table data should be deleted from the Delta Lake.
        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);
    }

    #[tokio::test]
    async fn test_truncate_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        assert!(context
            .truncate_table(test::MODEL_TABLE_NAME)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_model_table_metadata_from_default_database_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        parse_and_create_table(&context, test::MODEL_TABLE_SQL)
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
    async fn test_normal_table_model_table_metadata_from_default_database_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(&temp_dir).await;

        parse_and_create_table(&context, test::NORMAL_TABLE_SQL)
            .await
            .unwrap();

        assert!(context
            .model_table_metadata_from_default_database_schema(test::NORMAL_TABLE_NAME)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_missing_model_table_metadata_from_default_database_schema() {
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

        parse_and_create_table(&context, test::MODEL_TABLE_SQL)
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

        parse_and_create_table(&context, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        let schema = context
            .schema_of_table_in_default_database_schema(test::MODEL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(schema, test::model_table_metadata().schema)
    }

    async fn parse_and_create_table(context: &Context, sql: &str) -> Result<()> {
        let statement = parser::tokenize_and_parse_sql_statement(sql)?;
        if let Statement::CreateTable(create_table) = statement {
            context.validate_and_create_table(sql, create_table).await
        } else {
            Err(ModelarDbServerError::InvalidArgument(
                "Expected Statement::CreateTable.".to_owned(),
            ))
        }
    }

    #[tokio::test]
    async fn test_schema_of_missing_table_in_default_database_schema() {
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
