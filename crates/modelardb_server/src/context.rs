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

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use modelardb_common::errors::ModelarDbError;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::parser;
use modelardb_common::parser::ValidStatement;
use modelardb_common::types::{ClusterMode, ServerMode};
use object_store::ObjectStore;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tonic::Request;
use tracing::info;

use crate::configuration::ConfigurationManager;
use crate::metadata::MetadataManager;
use crate::query::ModelTable;
use crate::storage::{StorageEngine, COMPRESSED_DATA_FOLDER};
use crate::{optimizer, storage, DataFolders};

/// Provides access to the system's configuration and components.
pub struct Context {
    /// Metadata for the tables and model tables in the data folder.
    pub metadata_manager: Arc<MetadataManager>,
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
        // Create the components for the context.
        let metadata_manager = Arc::new(
            MetadataManager::try_new(&data_folders.local_data_folder)
                .await
                .map_err(|error| {
                    ModelarDbError::ConfigurationError(format!(
                        "Unable to create a MetadataManager: {error}"
                    ))
                })?,
        );

        let configuration_manager = Arc::new(RwLock::new(ConfigurationManager::new(
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
                metadata_manager.clone(),
            )
            .await
            .map_err(|error| {
                ModelarDbError::ConfigurationError(format!(
                    "Unable to create a StorageEngine: {error}"
                ))
            })?,
        ));

        Ok(Context {
            metadata_manager,
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

    /// Return the schema of `table_name` if the table exists in the default database schema,
    /// otherwise a [`ModelarDbError`] indicating at what level the lookup failed is returned.
    pub async fn schema_of_table_in_default_database_schema(
        &self,
        table_name: &str,
    ) -> Result<SchemaRef, ModelarDbError> {
        let database_schema = self.default_database_schema()?;

        let table = database_schema.table(table_name).await.ok_or_else(|| {
            ModelarDbError::DataRetrievalError("Table does not exist.".to_owned())
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

    /// Initialize the local database schema with the tables and model tables from the managers
    /// database schema. `context` is needed as an argument instead of using `self` to avoid having
    /// to copy the context when registering model tables. If the tables to create could not be
    /// retrieved from the manager, or the tables could not be created, return [`ModelarDbError`].
    pub(crate) async fn register_and_save_manager_tables(
        &self,
        manager_url: &str,
        context: &Arc<Context>,
    ) -> Result<(), ModelarDbError> {
        let existing_tables = self.default_database_schema()?.table_names();

        // Retrieve the tables to create from the manager.
        let mut flight_client = FlightServiceClient::connect(manager_url.to_owned())
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

        // Add the already existing tables to the action request.
        let action = Action {
            r#type: "InitializeDatabase".to_owned(),
            body: existing_tables.join(",").into_bytes().into(),
        };

        // Extract the SQL for the tables that need to be created from the response.
        let maybe_response = flight_client
            .do_action(Request::new(action))
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?
            .into_inner()
            .message()
            .await
            .map_err(|error| ModelarDbError::ClusterError(error.to_string()))?;

        if let Some(response) = maybe_response {
            let table_sql_queries = std::str::from_utf8(&response.body)
                .map_err(|error| ModelarDbError::TableError(error.to_string()))?
                .split(';')
                .filter(|sql| !sql.is_empty());

            // For each table to create, register and save the table in the metadata database.
            for sql in table_sql_queries {
                self.parse_and_create_table(sql, context).await?;
            }

            Ok(())
        } else {
            Err(ModelarDbError::ImplementationError(
                "Response for request to initialize database is empty.".to_owned(),
            ))
        }
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
    /// the [`MetadataManager`]. If the table exists, the Apache Parquet file cannot be created,
    /// or if the table cannot be saved to the [`MetadataManager`], return [`ModelarDbError`] error.
    async fn register_and_save_table(
        &self,
        table_name: &str,
        sql: &str,
        schema: Schema,
    ) -> Result<(), ModelarDbError> {
        // Ensure the folder for storing the table data exists.
        let metadata_manager = &self.metadata_manager;
        let folder_path = metadata_manager
            .local_data_folder()
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
        self.metadata_manager
            .save_table_metadata(table_name, sql)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        info!("Created table '{}'.", table_name);

        Ok(())
    }

    /// Create a model table, register it with Apache Arrow DataFusion's catalog, and save it to
    /// the [`MetadataManager`]. `context` is needed as an argument instead of using `self` to avoid
    /// having to copy the context when registering model tables. If the table exists or if the
    /// table cannot be saved to the [`MetadataManager`], return [`ModelarDbError`] error.
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
        self.metadata_manager
            .save_model_table_metadata(&model_table_metadata, sql)
            .await
            .map_err(|error| ModelarDbError::TableError(error.to_string()))?;

        info!("Created model table '{}'.", model_table_metadata.name);
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
            let message = format!("Table with name '{table_name}' does not exist.");
            ModelarDbError::DataRetrievalError(message)
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
            let message = format!("Table with name '{table_name}' already exists.");
            return Err(ModelarDbError::ConfigurationError(message));
        }
        Ok(())
    }
}
