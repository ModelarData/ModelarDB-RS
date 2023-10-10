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

use datafusion::{
    arrow::datatypes::SchemaRef, catalog::schema::SchemaProvider, prelude::SessionContext,
};
use modelardb_common::{
    errors::ModelarDbError, metadata::model_table_metadata::ModelTableMetadata, types::ClusterMode,
};
use tokio::sync::RwLock;

use crate::{
    configuration::ConfigurationManager, metadata::MetadataManager, query::ModelTable,
    storage::StorageEngine,
};

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
            ModelarDbError::DataRetrievalError("Table does not exist.".to_owned())
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
