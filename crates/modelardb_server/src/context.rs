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

use arrow_flight::Action;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use tokio::sync::RwLock;
use tonic::{Request, Status};
use tracing::info;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::parser;
use modelardb_common::parser::ValidStatement;
use modelardb_common::types::ClusterMode;

use crate::configuration::ConfigurationManager;
use crate::metadata::MetadataManager;
use crate::query::ModelTable;
use crate::storage::{COMPRESSED_DATA_FOLDER, StorageEngine};

/// Provides access to the system's configuration and components.
pub struct Context {
    /// The mode of the server used to determine the behaviour when starting the server,
    /// creating tables, updating the remote object store, and querying.
    pub cluster_mode: ClusterMode,
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
    /// Return the schema of `table_name` if the table exists in the default
    /// database schema, otherwise a [`Status`] indicating at what level the
    /// lookup failed is returned.
    pub(crate) async fn schema_of_table_in_default_database_schema(
        &self,
        table_name: &str,
    ) -> Result<SchemaRef, Status> {
        let database_schema = self.default_database_schema()?;

        let table = database_schema
            .table(table_name)
            .await
            .ok_or_else(|| Status::not_found("Table does not exist."))?;

        Ok(table.schema())
    }

    /// Return the default database schema if it exists, otherwise a [`Status`]
    /// indicating at what level the lookup failed is returned.
    pub(crate) fn default_database_schema(&self) -> Result<Arc<dyn SchemaProvider>, Status> {
        let session = self.session.clone();

        let catalog = session
            .catalog("datafusion")
            .ok_or_else(|| Status::internal("Default catalog does not exist."))?;

        let schema = catalog
            .schema("public")
            .ok_or_else(|| Status::internal("Default schema does not exist."))?;

        Ok(schema)
    }

    /// Return [`Status`] if a table named `table_name` exists in the default catalog.
    async fn check_if_table_exists(&self, table_name: &str) -> Result<(), Status> {
        let maybe_schema = self.schema_of_table_in_default_database_schema(table_name);
        if maybe_schema.await.is_ok() {
            let message = format!("Table with name '{table_name}' already exists.");
            return Err(Status::already_exists(message));
        }
        Ok(())
    }

    /// Initialize the local database schema with the tables and model tables from the managers
    /// database schema. If the tables to create could not be retrieved from the manager, or the
    /// tables could not be created, return [`Status`].
    pub(crate) async fn register_and_save_manager_tables(
        &self,
        manager_url: &str,
        context: &Arc<Context>,
    ) -> Result<(), Status> {
        let existing_tables = self.default_database_schema()?.table_names();

        // Retrieve the tables to create from the manager.
        let mut flight_client = FlightServiceClient::connect(manager_url.to_string())
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        // Add the already existing tables to the action request.
        let action = Action {
            r#type: "InitializeDatabase".to_owned(),
            body: existing_tables.join(",").into_bytes().into(),
        };

        // Extract the SQL for the tables that need to be created from the response.
        let maybe_response = flight_client
            .do_action(Request::new(action))
            .await?
            .into_inner()
            .message()
            .await?;

        if let Some(response) = maybe_response {
            let table_sql_queries = std::str::from_utf8(&response.body)
                .map_err(|error| Status::internal(error.to_string()))?
                .split(';')
                .filter(|sql| !sql.is_empty());

            // For each table to create, register and save the table in the metadata database.
            for sql in table_sql_queries {
                self.parse_and_create_table(sql, context).await?;
            }

            Ok(())
        } else {
            Err(Status::internal(
                "Response for request to initialize database is empty.".to_owned(),
            ))
        }
    }

    /// Parse `sql` and create a normal table or a model table based on the SQL. If `sql` is not
    /// valid or the table could not be created, return [`Status`].
    pub(crate) async fn parse_and_create_table(
        &self,
        sql: &str,
        context: &Arc<Context>,
    ) -> Result<(), Status> {
        // Parse the SQL.
        let statement = parser::tokenize_and_parse_sql(sql)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Perform semantic checks to ensure the parsed SQL is supported.
        let valid_statement = parser::semantic_checks_for_create_table(statement)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Create the table or model table if it does not already exists.
        match valid_statement {
            ValidStatement::CreateTable { name, schema } => {
                self.check_if_table_exists(&name).await?;
                self.register_and_save_table(name, sql.to_string(), schema)
                    .await?;
            }
            ValidStatement::CreateModelTable(model_table_metadata) => {
                self.check_if_table_exists(&model_table_metadata.name)
                    .await?;
                self.register_and_save_model_table(model_table_metadata, sql.to_string(), context)
                    .await?;
            }
        };

        Ok(())
    }

    /// Create a normal table, register it with Apache Arrow DataFusion's
    /// catalog, and save it to the [`MetadataManager`]. If the table exists,
    /// the Apache Parquet file cannot be created, or if the table cannot be
    /// saved to the [`MetadataManager`], return [`Status`] error.
    async fn register_and_save_table(
        &self,
        table_name: String,
        sql: String,
        schema: Schema,
    ) -> Result<(), Status> {
        // Ensure the folder for storing the table data exists.
        let metadata_manager = &self.metadata_manager;
        let folder_path = metadata_manager
            .local_data_folder()
            .join(COMPRESSED_DATA_FOLDER)
            .join(&table_name);
        fs::create_dir_all(&folder_path)?;

        // Create an empty Apache Parquet file to save the schema.
        let file_path = folder_path.join("empty_for_schema.parquet");
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));
        StorageEngine::write_batch_to_apache_parquet_file(empty_batch, &file_path, None)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Save the table in the Apache Arrow Datafusion catalog.
        self.session
            .register_parquet(
                &table_name,
                folder_path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Persist the new table to the metadata database.
        self.metadata_manager
            .save_table_metadata(table_name.clone(), sql)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        info!("Created table '{}'.", table_name);

        Ok(())
    }

    /// Create a model table, register it with Apache Arrow DataFusion's
    /// catalog, and save it to the [`MetadataManager`]. If the table exists or
    /// if the table cannot be saved to the [`MetadataManager`], return
    /// [`Status`] error.
    async fn register_and_save_model_table(
        &self,
        model_table_metadata: ModelTableMetadata,
        sql: String,
        context: &Arc<Context>,
    ) -> Result<(), Status> {
        // Save the model table in the Apache Arrow DataFusion catalog.
        let model_table_metadata = Arc::new(model_table_metadata);

        self.session
            .register_table(
                model_table_metadata.name.as_str(),
                ModelTable::new(context.clone(), model_table_metadata.clone()),
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Persist the new model table to the metadata database.
        self.metadata_manager
            .save_model_table_metadata(&model_table_metadata, &sql)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        info!("Created model table '{}'.", model_table_metadata.name);
        Ok(())
    }
}
