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

//! Table metadata manager that includes functionality used to access both the server metadata delta lake
//! and the manager metadata delta lake. Note that the entire server metadata delta lake can be accessed
//! through this metadata manager, while it only supports a subset of the manager metadata delta lake.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use datafusion::prelude::SessionContext;
use deltalake::kernel::{DataType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable, DeltaTableError};
use object_store::path::Path;

/// The folder storing metadata in the data folders.
const METADATA_FOLDER: &str = "metadata";

// TODO: Create a initialization function that can parse arguments from manager into table metadata manager.
// TODO: Add function to save model table metadata.
// TODO: Add tests for these functions.
// TODO: Look into adding primary key (and maybe unique) constraints to all the tables.
// TODO: Check if this actually results in an error when trying to add duplicates.
// TODO: Look into using merge builder to save tag hashes if they do not already exist.
// TODO: Look into using save mode on the write builder instead. It seems much simpler.

/// Stores the metadata required for reading from and writing to the tables and model tables.
/// The data that needs to be persisted is stored in the metadata delta lake.
pub struct TableMetadataManager {
    /// Map from metadata delta lake table names to [`DeltaTables`](DeltaTable).
    metadata_tables: DashMap<String, Arc<DeltaTable>>,
    /// Session used to read from the metadata delta lake using Apache Arrow DataFusion.
    session: SessionContext,
    /// Cache of tag value hashes used to signify when to persist new unsaved tag combinations.
    tag_value_hashes: DashMap<String, u64>,
}

impl TableMetadataManager {
    /// Create a new table metadata manager that saves the metadata to [`METADATA_FOLDER`] under
    /// `folder_path` and initialize the metadata tables. If the metadata tables could not be
    /// created, return [`DeltaTableError`].
    pub async fn try_new_local_table_metadata_manager(
        folder_path: Path,
    ) -> Result<TableMetadataManager, DeltaTableError> {
        let table_metadata_manager = TableMetadataManager {
            metadata_tables: DashMap::new(),
            session: SessionContext::new(),
            tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_delta_lake_tables(folder_path.as_ref(), HashMap::new())
            .await?;

        Ok(table_metadata_manager)
    }

    /// Create a new table metadata manager that saves the metadata to [`METADATA_FOLDER`] in a S3
    /// bucket and initialize the metadata tables. If the metadata tables could not be created,
    /// return [`DeltaTableError`].
    pub async fn try_new_s3_table_metadata_manager() -> Result<TableMetadataManager, DeltaTableError>
    {
        deltalake::aws::register_handlers(None);
        let storage_options: HashMap<String, String> = HashMap::from([
            ("REGION".to_owned(), "".to_owned()),
            ("ALLOW_HTTP".to_owned(), "true".to_owned()),
            ("ENDPOINT".to_owned(), "http://localhost:9000".to_owned()),
            ("BUCKET_NAME".to_owned(), "modelardb".to_owned()),
            ("ACCESS_KEY_ID".to_owned(), "minioadmin".to_owned()),
            ("SECRET_ACCESS_KEY".to_owned(), "minioadmin".to_owned()),
            ("AWS_S3_ALLOW_UNSAFE_RENAME".to_owned(), "true".to_owned()),
        ]);

        let table_metadata_manager = TableMetadataManager {
            metadata_tables: DashMap::new(),
            session: SessionContext::new(),
            tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_delta_lake_tables("s3://modelardb", storage_options)
            .await?;

        Ok(table_metadata_manager)
    }

    /// If they do not already exist, create the tables in the metadata delta lake used for table and
    /// model table metadata.
    /// * The table_metadata table contains the metadata for tables.
    /// * The model_table_metadata table contains the main metadata for model tables.
    /// * The model_table_hash_table_name contains a mapping from each tag hash to the name of the
    /// model table that contains the time series with that tag hash.
    /// * The model_table_field_columns table contains the name, index, error bound value, whether
    /// error bound is relative, and generation expression of the field columns in each model table.
    /// If the tables exist or were created, return [`Ok`], otherwise return [`DeltaTableError`].
    async fn create_metadata_delta_lake_tables(
        &self,
        url_scheme: &str,
        storage_options: HashMap<String, String>,
    ) -> Result<(), DeltaTableError> {
        // Create the table_metadata table if it does not exist.
        self.create_delta_lake_table(
            "table_metadata",
            vec![
                StructField::new("table_name", DataType::STRING, false),
                StructField::new("sql", DataType::STRING, false),
            ],
            url_scheme,
            storage_options.clone(),
        )
        .await?;

        // Create the model_table_metadata table if it does not exist.
        self.create_delta_lake_table(
            "model_table_metadata",
            vec![
                StructField::new("table_name", DataType::STRING, false),
                StructField::new("query_schema", DataType::BINARY, false),
                StructField::new("sql", DataType::STRING, false),
            ],
            url_scheme,
            storage_options.clone(),
        )
        .await?;

        // Create the model_table_hash_table_name table if it does not exist.
        self.create_delta_lake_table(
            "model_table_hash_table_name",
            vec![
                StructField::new("hash", DataType::LONG, false),
                StructField::new("table_name", DataType::STRING, false),
            ],
            url_scheme,
            storage_options.clone(),
        )
        .await?;

        // Create the model_table_field_columns table if it does not exist. Note that column_index
        // will only use a maximum of 10 bits. generated_column_* is NULL if the fields are stored
        // as segments.
        self.create_delta_lake_table(
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
            url_scheme,
            storage_options,
        )
        .await?;

        Ok(())
    }

    /// Use `table_name` to create a delta lake table with `columns` in the location given by
    /// `url_scheme` and `storage_options` if it does not already exist. The created table is saved
    /// in the metadata tables and registered in the Apache Arrow Datafusion session. If the table
    /// could not be created or registered, return [`DeltaTableError`].
    async fn create_delta_lake_table(
        &self,
        table_name: &str,
        columns: Vec<StructField>,
        url_scheme: &str,
        storage_options: HashMap<String, String>,
    ) -> Result<(), DeltaTableError> {
        let table = Arc::new(
            CreateBuilder::new()
                .with_save_mode(SaveMode::Ignore)
                .with_storage_options(storage_options)
                .with_table_name(table_name)
                .with_location(format!("{url_scheme}/{METADATA_FOLDER}/{table_name}"))
                .with_columns(columns)
                .await?,
        );

        self.session.register_table(table_name, table.clone())?;
        self.metadata_tables.insert(table_name.to_owned(), table);

        Ok(())
    }

    /// Save the created table to the metadata delta lake. This consists of adding a row to the
    /// table_metadata table with the `name` of the table and the `sql` used to create the table.
    /// If the table metadata was saved, return the updated [`DeltaTable`], otherwise return [`DeltaTableError`].
    pub async fn save_table_metadata(
        &self,
        name: &str,
        sql: &str,
    ) -> Result<DeltaTable, DeltaTableError> {
        // unwrap() is safe since the "table_metadata" table is registered when the table metadata manager is created.
        let table_provider = self.session.table_provider("table_metadata").await.unwrap();
        let table: &DeltaTable = table_provider.as_any().downcast_ref().unwrap();

        let ops = DeltaOps::from(table.clone());
        let batch = RecordBatch::try_new(
            table_provider.schema(),
            vec![
                Arc::new(StringArray::from(vec![name])),
                Arc::new(StringArray::from(vec![sql])),
            ],
        )
        .unwrap();

        ops.write(vec![batch]).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for TableMetadataManager.
    #[tokio::test]
    async fn test_create_metadata_delta_lake_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_new_local_table_metadata_manager(
            Path::from_absolute_path(temp_dir.path()).unwrap(),
        )
        .await
        .unwrap();

        // Verify that the tables were created, registered, and has the expected columns.
        assert!(metadata_manager
            .metadata_tables
            .contains_key("table_metadata"));
        assert!(metadata_manager
            .session
            .sql("SELECT table_name, sql FROM table_metadata")
            .await
            .is_ok());

        assert!(metadata_manager
            .metadata_tables
            .contains_key("model_table_metadata"));
        assert!(metadata_manager
            .session
            .sql("SELECT table_name, query_schema, sql FROM model_table_metadata")
            .await
            .is_ok());

        assert!(metadata_manager
            .metadata_tables
            .contains_key("model_table_hash_table_name"));
        assert!(metadata_manager
            .session
            .sql("SELECT hash, table_name FROM model_table_hash_table_name")
            .await
            .is_ok());

        assert!(metadata_manager
            .metadata_tables
            .contains_key("model_table_field_columns"));
        assert!(metadata_manager
            .session
            .sql("SELECT table_name, column_name, column_index, error_bound_value, error_bound_is_relative,
                 generated_column_expr, generated_column_sources FROM model_table_field_columns")
            .await
            .is_ok());
    }
}
