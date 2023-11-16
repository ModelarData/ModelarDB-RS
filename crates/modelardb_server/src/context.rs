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

use std::fs;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use modelardb_common::errors::ModelarDbError;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::metadata::TableMetadataManager;
use modelardb_common::parser::ValidStatement;
use modelardb_common::types::ServerMode;
use modelardb_common::{metadata, parser};
use object_store::ObjectStore;
use sqlx::Sqlite;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing::info;

use crate::configuration::ConfigurationManager;
use crate::query::ModelTable;
use crate::storage::{StorageEngine, COMPRESSED_DATA_FOLDER};
use crate::{optimizer, storage, ClusterMode, DataFolders};

/// Provides access to the system's configuration and components.
pub struct Context {
    /// Metadata for the tables and model tables in the data folder.
    pub table_metadata_manager: Arc<TableMetadataManager<Sqlite>>,
    /// Updatable configuration of the server.
    pub configuration_manager: Arc<RwLock<ConfigurationManager>>,
    /// Main interface for Apache Arrow DataFusion.
    pub session: SessionContext,
    /// Manages all uncompressed and compressed data in the system.
    pub storage_engine: Arc<RwLock<StorageEngine>>,
}

impl Context {
    /// Create the components needed in the [`Context`] and use them to create the [`Context`]. If
    /// a metadata manager or storage engine could not be created, [`ModelarDbError`] is returned.
    pub async fn try_new(
        runtime: Arc<Runtime>,
        data_folders: &DataFolders,
        cluster_mode: ClusterMode,
        server_mode: ServerMode,
    ) -> Result<Self, ModelarDbError> {
        let table_metadata_manager = Arc::new(
            metadata::try_new_sqlite_table_metadata_manager(&data_folders.local_data_folder)
                .await
                .map_err(|error| {
                    ModelarDbError::ConfigurationError(format!(
                        "Unable to create a TableMetadataManager: {error}"
                    ))
                })?,
        );

        let configuration_manager = Arc::new(RwLock::new(ConfigurationManager::new(
            &data_folders.local_data_folder,
            cluster_mode,
            server_mode,
        )));

        let session = Self::create_session_context(data_folders.query_data_folder.clone());

        let storage_engine = Arc::new(RwLock::new(
            StorageEngine::try_new(
                runtime,
                data_folders.local_data_folder.clone(),
                data_folders.remote_data_folder.clone(),
                &configuration_manager,
                table_metadata_manager.clone(),
            )
            .await
            .map_err(|error| {
                ModelarDbError::ConfigurationError(format!(
                    "Unable to create a StorageEngine: {error}"
                ))
            })?,
        ));

        Ok(Context {
            table_metadata_manager,
            configuration_manager,
            session,
            storage_engine,
        })
    }

    /// Create a new [`SessionContext`] for interacting with Apache Arrow DataFusion. The
    /// [`SessionContext`] is constructed with the default configuration, default resource managers,
    /// the local file system and if provided the remote object store as [`ObjectStores`](ObjectStore),
    /// and additional optimizer rules that rewrite simple aggregate queries to be executed directly
    /// on the segments containing metadata and models instead of on reconstructed data points
    /// created from the segments for model tables.
    fn create_session_context(query_data_folder: Arc<dyn ObjectStore>) -> SessionContext {
        let session_config = SessionConfig::new();
        let session_runtime = Arc::new(RuntimeEnv::default());

        // unwrap() is safe as storage::QUERY_DATA_FOLDER_SCHEME_WITH_HOST is a const containing an URL.
        let object_store_url = storage::QUERY_DATA_FOLDER_SCHEME_WITH_HOST
            .try_into()
            .unwrap();
        session_runtime.register_object_store(&object_store_url, query_data_folder);

        // Use the add* methods instead of the with* methods as the with* methods replace the built-ins.
        // See: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionState.html
        let mut session_state = SessionState::new_with_config_rt(session_config, session_runtime);
        for physical_optimizer_rule in optimizer::physical_optimizer_rules() {
            session_state = session_state.add_physical_optimizer_rule(physical_optimizer_rule);
        }

        SessionContext::new_with_state(session_state)
    }

    /// Parse `sql` and create a normal table or a model table based on the SQL. `context` is needed
    /// as an argument instead of using `self` to avoid having to copy the context when registering
    /// model tables. If `sql` is not valid or the table could not be created, return [`ModelarDbError`].
    pub(crate) async fn parse_and_create_table(
        &self,
        sql: &str,
        context: &Arc<Context>,
    ) -> Result<(), ModelarDbError> {
        // Parse the SQL.
        let statement = parser::tokenize_and_parse_sql(sql)
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Perform semantic checks to ensure the parsed SQL is supported.
        let valid_statement = parser::semantic_checks_for_create_table(statement)
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Create the table or model table if it does not already exists.
        match valid_statement {
            ValidStatement::CreateTable { name, schema } => {
                self.check_if_table_exists(&name).await?;
                self.register_and_save_table(&name, sql, schema).await?;
            }
            ValidStatement::CreateModelTable(model_table_metadata) => {
                self.check_if_table_exists(&model_table_metadata.name)
                    .await?;
                self.register_and_save_model_table(model_table_metadata, sql, context)
                    .await?;
            }
        };

        Ok(())
    }

    /// Create a normal table, register it with Apache Arrow DataFusion's catalog, and save it to
    /// the [`TableMetadataManager`]. If the table exists, the Apache Parquet file cannot be created,
    /// or if the table cannot be saved to the [`TableMetadataManager`], return [`ModelarDbError`] error.
    async fn register_and_save_table(
        &self,
        table_name: &str,
        sql: &str,
        schema: Schema,
    ) -> Result<(), ModelarDbError> {
        // Ensure the folder for storing the table data exists.
        let configuration_manager = self.configuration_manager.read().await;
        let folder_path = configuration_manager
            .local_data_folder
            .join(COMPRESSED_DATA_FOLDER)
            .join(table_name);

        fs::create_dir_all(&folder_path)
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Create an empty Apache Parquet file to save the schema.
        let file_path = folder_path.join("empty_for_schema.parquet");
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));
        StorageEngine::write_batch_to_apache_parquet_file(&empty_batch, &file_path, None)
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Save the table in the Apache Arrow Datafusion catalog.
        self.session
            .register_parquet(
                table_name,
                folder_path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Persist the new table to the metadata database.
        self.table_metadata_manager
            .save_table_metadata(table_name, sql)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        info!("Created table '{}'.", table_name);

        Ok(())
    }

    /// Create a model table, register it with Apache Arrow DataFusion's catalog, and save it to
    /// the [`TableMetadataManager`]. `context` is needed as an argument instead of using `self` to avoid
    /// having to copy the context when registering model tables. If the table exists or if the
    /// table cannot be saved to the [`TableMetadataManager`], return [`ModelarDbError`] error.
    async fn register_and_save_model_table(
        &self,
        model_table_metadata: ModelTableMetadata,
        sql: &str,
        context: &Arc<Context>,
    ) -> Result<(), ModelarDbError> {
        // Save the model table in the Apache Arrow DataFusion catalog.
        let model_table_metadata = Arc::new(model_table_metadata);

        self.session
            .register_table(
                model_table_metadata.name.as_str(),
                ModelTable::new(context.clone(), model_table_metadata.clone()),
            )
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        // Persist the new model table to the metadata database.
        self.table_metadata_manager
            .save_model_table_metadata(&model_table_metadata, sql)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        info!("Created model table '{}'.", model_table_metadata.name);
        Ok(())
    }

    /// For each table saved in the metadata database, register the table in Apache Arrow
    /// DataFusion. If the tables could not be retrieved from the metadata database or a table
    /// could not be registered, return [`ModelarDbError`].
    pub async fn register_tables(&self) -> Result<(), ModelarDbError> {
        let table_names = self
            .table_metadata_manager
            .table_names()
            .await
            .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?;

        let configuration_manager = self.configuration_manager.read().await;

        for table_name in table_names {
            // Compute the path to the folder containing data for the table.
            let table_folder_path = configuration_manager
                .local_data_folder
                .join(COMPRESSED_DATA_FOLDER)
                .join(&table_name);

            // unwrap() is safe since the path is created from the table name which is valid UTF-8.
            self.session
                .register_parquet(
                    &table_name,
                    table_folder_path.to_str().unwrap(),
                    ParquetReadOptions::default(),
                )
                .await
                .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

            info!("Registered table '{table_name}'.");
        }

        Ok(())
    }

    /// For each model table saved in the metadata database, register the model table in Apache
    /// Arrow DataFusion. `context` is needed as an argument instead of using `self` to avoid
    /// having to copy the context when registering model tables. If the model tables could not be
    /// retrieved from the metadata database or a model table could not be registered,
    /// return [`ModelarDbError`].
    pub async fn register_model_tables(
        &self,
        context: &Arc<Context>,
    ) -> Result<(), ModelarDbError> {
        let model_table_metadata = self
            .table_metadata_manager
            .model_table_metadata()
            .await
            .map_err(|error| ModelarDbError::DataRetrievalError(error.to_string()))?;

        for metadata in model_table_metadata {
            self.session
                .register_table(
                    &metadata.name,
                    ModelTable::new(context.clone(), metadata.clone()),
                )
                .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

            info!("Registered model table '{}'.", &metadata.name);
        }

        Ok(())
    }

    /// Lookup the [`ModelTableMetadata`] of the model table with name `table_name` if it exists.
    /// Specifically, the method returns:
    /// * [`ModelTableMetadata`] if a model table with the name `table_name` exists.
    /// * [`None`] if a table with the name `table_name` exists.
    /// * [`ModelarDbError`] if the default catalog, the default schema, a table with the name
    /// `table_name`, or a model table with the name `table_name` does not exists.
    pub async fn model_table_metadata_from_default_database_schema(
        &self,
        table_name: &str,
    ) -> Result<Option<Arc<ModelTableMetadata>>, ModelarDbError> {
        let database_schema = self.default_database_schema()?;

        let table = database_schema.table(table_name).await.ok_or_else(|| {
            ModelarDbError::DataRetrievalError(format!(
                "Table with name '{table_name}' does not exist."
            ))
        })?;

        if let Some(model_table) = table.as_any().downcast_ref::<ModelTable>() {
            Ok(Some(model_table.model_table_metadata()))
        } else {
            Ok(None)
        }
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

        let table = database_schema.table(table_name).await.ok_or_else(|| {
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

    use std::path::Path;

    use modelardb_common::test;
    use object_store::local::LocalFileSystem;

    #[tokio::test]
    async fn test_parse_and_create_table_with_invalid_sql() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(temp_dir.path()).await;

        assert!(context
            .parse_and_create_table("TABLE CREATE table_name(timestamp TIMESTAMP)", &context)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_parse_and_create_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(temp_dir.path()).await;

        context
            .parse_and_create_table(test::TABLE_SQL, &context)
            .await
            .unwrap();

        // An Apache Parquet file should be created to save the schema.
        let folder_path = temp_dir
            .path()
            .join(COMPRESSED_DATA_FOLDER)
            .join("table_name")
            .join("empty_for_schema.parquet");

        assert!(folder_path.exists());

        // The table should be saved to the metadata database.
        let table_names = context.table_metadata_manager.table_names().await.unwrap();
        assert!(table_names.contains(&"table_name".to_owned()));

        // The table should be registered in the Apache Arrow DataFusion catalog.
        assert!(context.check_if_table_exists("table_name").await.is_err());
    }

    #[tokio::test]
    async fn test_parse_and_create_existing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(temp_dir.path()).await;

        assert!(context
            .parse_and_create_table(test::TABLE_SQL, &context)
            .await
            .is_ok());

        assert!(context
            .parse_and_create_table(test::TABLE_SQL, &context)
            .await
            .is_err())
    }

    #[tokio::test]
    async fn test_parse_and_create_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(temp_dir.path()).await;

        context
            .parse_and_create_table(test::MODEL_TABLE_SQL, &context)
            .await
            .unwrap();

        // The table should be saved to the metadata database.
        let model_table_metadata = context
            .table_metadata_manager
            .model_table_metadata()
            .await
            .unwrap();

        assert_eq!(
            model_table_metadata.get(0).unwrap().name,
            test::model_table_metadata().name
        );

        // The model table should be registered in the Apache Arrow DataFusion catalog.
        assert!(context
            .check_if_table_exists(test::MODEL_TABLE_NAME)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_parse_and_create_existing_model_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(temp_dir.path()).await;

        assert!(context
            .parse_and_create_table(test::MODEL_TABLE_SQL, &context)
            .await
            .is_ok());

        assert!(context
            .parse_and_create_table(test::MODEL_TABLE_SQL, &context)
            .await
            .is_err())
    }

    #[tokio::test]
    async fn test_register_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(temp_dir.path()).await;

        context
            .parse_and_create_table(test::TABLE_SQL, &context)
            .await
            .unwrap();

        // Create a new context to clear the Apache Arrow Datafusion catalog.
        let context_2 = create_context(temp_dir.path()).await;

        // Register the table with Apache Arrow DataFusion.
        context_2.register_tables().await.unwrap();
    }

    #[tokio::test]
    async fn test_register_model_tables() {
        // The test succeeds if none of the unwrap()s fails.

        // Save a model table to the metadata database.
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(temp_dir.path()).await;

        let model_table_metadata = test::model_table_metadata();
        context
            .table_metadata_manager
            .save_model_table_metadata(&model_table_metadata, test::MODEL_TABLE_SQL)
            .await
            .unwrap();

        // Register the model table with Apache Arrow DataFusion.
        context.register_model_tables(&context).await.unwrap();
    }

    #[tokio::test]
    async fn test_model_table_metadata_from_default_database_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(temp_dir.path()).await;

        context
            .parse_and_create_table(test::MODEL_TABLE_SQL, &context)
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
        let context = create_context(temp_dir.path()).await;

        context
            .parse_and_create_table(test::TABLE_SQL, &context)
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
        let context = create_context(temp_dir.path()).await;

        assert!(context
            .model_table_metadata_from_default_database_schema(test::MODEL_TABLE_NAME)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_check_if_existing_table_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(temp_dir.path()).await;

        context
            .parse_and_create_table(test::MODEL_TABLE_SQL, &context)
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
        let context = create_context(temp_dir.path()).await;

        assert!(context
            .check_if_table_exists(test::MODEL_TABLE_NAME)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_schema_of_table_in_default_database_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let context = create_context(temp_dir.path()).await;

        context
            .parse_and_create_table(test::MODEL_TABLE_SQL, &context)
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
        let context = create_context(temp_dir.path()).await;

        assert!(context
            .schema_of_table_in_default_database_schema(test::MODEL_TABLE_NAME)
            .await
            .is_err())
    }

    /// Create a simple [`Context`] that uses `path` as the local data folder and query data folder.
    async fn create_context(path: &Path) -> Arc<Context> {
        Arc::new(
            Context::try_new(
                Arc::new(Runtime::new().unwrap()),
                &DataFolders {
                    local_data_folder: path.to_path_buf(),
                    remote_data_folder: None,
                    query_data_folder: Arc::new(LocalFileSystem::new_with_prefix(path).unwrap()),
                },
                ClusterMode::SingleNode,
                ServerMode::Edge,
            )
            .await
            .unwrap(),
        )
    }
}
