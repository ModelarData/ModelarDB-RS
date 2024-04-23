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

//! Table metadata manager that includes functionality used to access both the server metadata deltalake
//! and the manager metadata deltalake. Note that the entire server metadata deltalake can be accessed
//! through this metadata manager, while it only supports a subset of the manager metadata deltalake.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::prelude::SessionContext;
use deltalake::kernel::{DataType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, DeltaTableError};

/// The folder storing metadata in the data folders.
const METADATA_FOLDER: &str = "metadata";

/// Stores the metadata required for reading from and writing to the tables and model tables.
/// The data that needs to be persisted is stored in the metadata deltalake.
pub struct TableMetadataManager {
    /// Map from metadata deltalake table names to [`DeltaTables`](DeltaTable).
    metadata_tables: DashMap<String, Arc<DeltaTable>>,
    /// Session used to read from the metadata deltalake using Apache Arrow DataFusion.
    session: SessionContext,
    /// Cache of tag value hashes used to signify when to persist new unsaved tag combinations.
    _tag_value_hashes: DashMap<String, u64>,
}

impl TableMetadataManager {
    /// Create a new table metadata manager that saves the metadata to [`METADATA_FOLDER`] under
    /// `folder_path` and initialize the metadata tables. If the metadata tables could not be
    /// created, return [`DeltaTableError`].
    pub async fn try_new_local_table_metadata_manager(
    ) -> Result<TableMetadataManager, DeltaTableError> {
        let table_metadata_manager = TableMetadataManager {
            metadata_tables: DashMap::new(),
            session: SessionContext::new(),
            _tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_deltalake_tables("data", HashMap::new())
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
            _tag_value_hashes: DashMap::new(),
        };

        table_metadata_manager
            .create_metadata_deltalake_tables("s3://modelardb", storage_options)
            .await?;

        Ok(table_metadata_manager)
    }

    /// If they do not already exist, create the tables in the metadata deltalake used for table and
    /// model table metadata.
    /// * The table_metadata table contains the metadata for tables.
    /// * The model_table_metadata table contains the main metadata for model tables.
    /// * The model_table_hash_table_name contains a mapping from each tag hash to the name of the
    /// model table that contains the time series with that tag hash.
    /// * The model_table_field_columns table contains the name, index, error bound value, whether
    /// error bound is relative, and generation expression of the field columns in each model table.
    /// If the tables exist or were created, return [`Ok`], otherwise return [`DeltaTableError`].
    async fn create_metadata_deltalake_tables(
        &self,
        url_scheme: &str,
        storage_options: HashMap<String, String>,
    ) -> Result<(), DeltaTableError> {
        // Create the table_metadata table if it does not exist.
        self.create_deltalake_table(
            "table_metadata",
            vec![
                StructField::new("table_name", DataType::STRING, false),
                StructField::new("sql", DataType::STRING, false),
            ],
            url_scheme,
            storage_options,
        )
        .await?;

        Ok(())
    }

    /// Use `table_name` to create a deltalake table with `columns` in the location given by
    /// `url_scheme` and `storage_options` if it does not already exist. The created table is saved
    /// in the metadata tables and registered in the Apache Arrow Datafusion session. If the table
    /// could not be created or registered, return [`DeltaTableError`].
    async fn create_deltalake_table(
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
}
