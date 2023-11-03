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

//! Management of the metadata database for the manager. Metadata which is unique to the manager,
//! such as metadata about registered edges, is handled here.

use std::str::FromStr;
use std::sync::Arc;

use futures::TryStreamExt;
use modelardb_common::errors::ModelarDbError;
use modelardb_common::metadata::TableMetadataManager;
use modelardb_common::types::ServerMode;
use sqlx::{AnyPool, Executor, Row};
use uuid::Uuid;

use crate::cluster::Node;

/// Stores the metadata required for reading from and writing to the tables and model tables and
/// persisting edges. The data that needs to be persisted is stored in the metadata database.
#[derive(Clone)]
pub struct MetadataManager {
    /// Pool of connections to the metadata database.
    metadata_database_pool: Arc<AnyPool>,
    /// Metadata manager used to interface with the subset of the manager metadata database related
    /// to tables and model tables.
    table_metadata_manager: TableMetadataManager,
}

impl MetadataManager {
    /// Return [`MetadataManager`] if the necessary tables could be created in the metadata database,
    /// otherwise return [`sqlx::Error`].
    pub async fn try_new(metadata_database_pool: Arc<AnyPool>) -> Result<Self, sqlx::Error> {
        let table_metadata_manager =
            TableMetadataManager::try_new_postgres(metadata_database_pool.clone()).await?;

        let metadata_manager = Self {
            metadata_database_pool,
            table_metadata_manager,
        };

        // Create the necessary tables in the metadata database.
        metadata_manager
            .create_manager_metadata_database_tables()
            .await?;

        Ok(metadata_manager)
    }

    /// If they do not already exist, create the tables that are specific to the manager metadata
    /// database.
    /// * The manager_metadata table contains metadata for the manager itself. It is assumed that
    /// this table will only have a single row since there can only be a single manager.
    /// * The nodes table contains metadata for each node that is controlled by the manager.
    /// If the tables exist or were created, return [`Ok`], otherwise return [`sqlx::Error`].
    async fn create_manager_metadata_database_tables(&self) -> Result<(), sqlx::Error> {
        let mut transaction = self.metadata_database_pool.begin().await?;

        // Create the manager_metadata table if it does not exist.
        sqlx::query("CREATE TABLE IF NOT EXISTS manager_metadata (key TEXT PRIMARY KEY)")
            .execute(&mut *transaction)
            .await?;

        // Create the nodes table if it does not exist.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nodes (
                 url TEXT PRIMARY KEY,
                 mode TEXT NOT NULL
             )",
        )
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;

        Ok(())
    }

    /// Retrieve the key for the manager from the manager_metadata table. If a key does not already
    /// exist, create one and save it to the database.
    pub async fn manager_key(&self) -> Result<Uuid, sqlx::Error> {
        let maybe_row = sqlx::query("SELECT key FROM manager_metadata")
            .fetch_optional(&*self.metadata_database_pool)
            .await?;

        if let Some(row) = maybe_row {
            let manager_key: String = row.try_get("key")?;

            Ok(manager_key
                .parse()
                .map_err(|error| sqlx::Error::ColumnDecode {
                    index: "key".to_owned(),
                    source: Box::new(error),
                })?)
        } else {
            let manager_key = Uuid::new_v4();

            // Add a new row to the manager_metadata table to persist the key.
            sqlx::query("INSERT INTO manager_metadata (key) VALUES ($1)")
                .bind(manager_key.to_string())
                .execute(&*self.metadata_database_pool)
                .await?;

            Ok(manager_key)
        }
    }

    /// Save the node to the metadata database and return [`Ok`]. If the node could not be saved,
    /// return [`sqlx::Error`].
    pub async fn save_node(&self, node: &Node) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO nodes (url, mode) VALUES ($1, $2)")
            .bind(&node.url)
            .bind(node.mode.to_string())
            .execute(&*self.metadata_database_pool)
            .await?;

        Ok(())
    }

    /// Remove the row in the nodes table that corresponds to the node with `url` and return [`Ok`].
    /// If the row could not be removed, return [`sqlx::Error`].
    pub async fn remove_node(&self, url: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM nodes WHERE url = $1")
            .bind(url)
            .execute(&*self.metadata_database_pool)
            .await?;

        Ok(())
    }

    /// Return the nodes currently controlled by the manager that have been persisted to the
    /// metadata database. If the nodes could not be retrieved, [`sqlx::Error`] is returned.
    pub async fn nodes(&self) -> Result<Vec<Node>, sqlx::Error> {
        let mut nodes: Vec<Node> = vec![];

        let mut rows =
            sqlx::query("SELECT url, mode FROM nodes").fetch(&*self.metadata_database_pool);

        while let Some(row) = rows.try_next().await? {
            let server_mode = ServerMode::from_str(row.try_get("mode")?).map_err(|error| {
                sqlx::Error::ColumnDecode {
                    index: "mode".to_owned(),
                    source: Box::new(ModelarDbError::DataRetrievalError(error.to_string())),
                }
            })?;

            nodes.push(Node::new(row.try_get("url")?, server_mode))
        }

        Ok(nodes)
    }

    /// Return the SQL query used to create the table with the name `table_name`. If a table with
    /// that name does not exists, return [`sqlx::Error`].
    pub async fn table_sql(&self, table_name: &str) -> Result<String, sqlx::Error> {
        let select_statement = format!(
            "SELECT sql FROM table_metadata WHERE table_name = $1
             UNION
             SELECT sql FROM model_table_metadata WHERE table_name = $2",
        );

        let row = sqlx::query(&select_statement)
            .bind(table_name)
            .bind(table_name)
            .fetch_one(&*self.metadata_database_pool)
            .await?;

        row.try_get("sql")
    }

    /// Retrieve all rows of `column` from both the table_metadata and model_table_metadata tables.
    /// If the column could not be retrieved, either because it does not exist or because it could
    /// not be converted to a string, return [`sqlx::Error`].
    pub async fn table_metadata_column(&self, column: &str) -> Result<Vec<String>, sqlx::Error> {
        let mut values: Vec<String> = vec![];

        // Retrieve the column from both tables containing table metadata.
        let select_statement = format!(
            "SELECT {column} FROM table_metadata UNION SELECT {column} FROM model_table_metadata",
        );

        let mut rows = sqlx::query(&select_statement).fetch(&*self.metadata_database_pool);
        while let Some(row) = rows.try_next().await? {
            values.push(row.try_get(column)?)
        }

        Ok(values)
    }
}
