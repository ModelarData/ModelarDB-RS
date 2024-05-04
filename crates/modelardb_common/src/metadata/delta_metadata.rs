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
use deltalake::{open_table_with_storage_options, DeltaOps, DeltaTable, DeltaTableError};
use object_store::path::Path;

/// The folder storing metadata in the data folders.
const METADATA_FOLDER: &str = "metadata";

// TODO: Create a initialization function that can parse arguments from manager into table metadata manager.
// TODO: Look into optimizing the way we store and access tables in the struct fields (avoid open_table() every time).
// TODO: Add function to save model table metadata.
// TODO: Add tests for these functions.
// TODO: Look into adding primary key (and maybe unique) constraints to all the tables.
// TODO: Check if this actually results in an error when trying to add duplicates.
// TODO: Look into using merge builder to save tag hashes if they do not already exist.
// TODO: Look into using save mode on the write builder instead. It seems much simpler.

/// Stores the metadata required for reading from and writing to the tables and model tables.
/// The data that needs to be persisted is stored in the metadata delta lake.
pub struct TableMetadataManager {
    /// URL to access the base folder of the location where the metadata tables are stored.
    url_scheme: String,
    // TODO: Look into using dash map or StorageOptions.
    /// Storage options used to access delta lake tables in remote object stores.
    storage_options: HashMap<String, String>,
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
            url_scheme: format!("{folder_path}/{METADATA_FOLDER}"),
            storage_options: HashMap::new(),
            session: SessionContext::new(),
            tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_delta_lake_tables()
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
            url_scheme: format!("s3://modelardb/{METADATA_FOLDER}"),
            storage_options,
            session: SessionContext::new(),
            tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_delta_lake_tables()
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
    async fn create_metadata_delta_lake_tables(&self) -> Result<(), DeltaTableError> {
        // Create the table_metadata table if it does not exist.
        self.create_delta_lake_table(
            "table_metadata",
            vec![
                StructField::new("table_name", DataType::STRING, false),
                StructField::new("sql", DataType::STRING, false),
            ],
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
        )
        .await?;

        // Create the model_table_hash_table_name table if it does not exist.
        self.create_delta_lake_table(
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
        )
        .await?;

        Ok(())
    }

    /// Use `table_name` to create a delta lake table with `columns` in the location given by
    /// `url_scheme` and `storage_options` if it does not already exist. The created table is
    /// registered in the Apache Arrow Datafusion session. If the table could not be created or
    /// registered, return [`DeltaTableError`].
    async fn create_delta_lake_table(
        &self,
        table_name: &str,
        columns: Vec<StructField>,
    ) -> Result<(), DeltaTableError> {
        let table = Arc::new(
            CreateBuilder::new()
                .with_save_mode(SaveMode::Ignore)
                .with_storage_options(self.storage_options.clone())
                .with_table_name(table_name)
                .with_location(format!("{}/{table_name}", self.url_scheme))
                .with_columns(columns)
                .await?,
        );

        self.session.register_table(table_name, table.clone())?;

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
        let table = open_table_with_storage_options(
            format!("{}/table_metadata", self.url_scheme),
            self.storage_options.clone(),
        )
        .await?;

        let batch = RecordBatch::try_new(
            table_provider.schema(),
            vec![
                Arc::new(StringArray::from(vec![name])),
                Arc::new(StringArray::from(vec![sql])),
            ],
        )
        .unwrap();

        let ops = DeltaOps::from(table.clone());
        ops.write(vec![batch]).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::compute::concat_batches;

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
            .session
            .sql("SELECT table_name, sql FROM table_metadata")
            .await
            .is_ok());

        assert!(metadata_manager
            .session
            .sql("SELECT table_name, query_schema, sql FROM model_table_metadata")
            .await
            .is_ok());

        assert!(metadata_manager
            .session
            .sql("SELECT hash, table_name FROM model_table_hash_table_name")
            .await
            .is_ok());

        assert!(metadata_manager
            .session
            .sql("SELECT table_name, column_name, column_index, error_bound_value, error_bound_is_relative,
                 generated_column_expr, generated_column_sources FROM model_table_field_columns")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_save_table_metadata() {
        let temp_dir = tempfile::tempdir().unwrap();
        let metadata_manager = TableMetadataManager::try_new_local_table_metadata_manager(
            Path::from_absolute_path(temp_dir.path()).unwrap(),
        )
        .await
        .unwrap();

        metadata_manager
            .save_table_metadata("table_1", "CREATE TABLE table_1")
            .await
            .unwrap();

        let table = metadata_manager
            .save_table_metadata("table_2", "CREATE TABLE table_2")
            .await
            .unwrap();

        // Retrieve the table from the metadata delta lake.
        let ctx = SessionContext::new();
        ctx.register_table("table_metadata", Arc::new(table))
            .unwrap();
        let dataframe = ctx
            .sql("SELECT table_name, sql FROM table_metadata ORDER BY table_name")
            .await
            .unwrap();

        let table_provider = metadata_manager
            .session
            .table_provider("table_metadata")
            .await
            .unwrap();
        let batches = dataframe.collect().await.unwrap();
        let batch = concat_batches(&table_provider.schema(), batches.as_slice()).unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec!["table_1", "table_2"])
        );
        assert_eq!(
            **batch.column(1),
            StringArray::from(vec!["CREATE TABLE table_1", "CREATE TABLE table_2"])
        );
    }
}
