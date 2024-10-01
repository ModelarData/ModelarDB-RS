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

//! Metadata Delta Lake that includes functionality to create metadata tables, append data to the
//! created tables, and query the created tables.

use std::collections::HashMap;
use std::fs;
use std::path::Path as StdPath;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use deltalake::kernel::StructField;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{open_table_with_storage_options, DeltaOps, DeltaTable, DeltaTableError};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use url::Url;

use crate::arguments;

pub mod model_table_metadata;
pub mod table_metadata_manager;

/// The folder storing metadata in the data folders.
pub const METADATA_FOLDER: &str = "metadata";

/// Provides functionality to create and use metadata tables in a metadata Delta Lake.
#[derive(Clone)]
pub struct MetadataDeltaLake {
    /// URL to access the base folder of the location where the metadata tables are stored.
    location: String,
    /// Storage options used to access Delta Lake tables in remote object stores.
    storage_options: HashMap<String, String>,
    /// [`ObjectStore`] to access the root of the Delta Lake.
    object_store: Arc<dyn ObjectStore>,
    /// Session used to read from the metadata Delta Lake using Apache Arrow DataFusion.
    session: SessionContext,
}

impl MetadataDeltaLake {
    /// Create a new [`MetadataDeltaLake`] that saves the metadata to [`METADATA_FOLDER`] under
    /// `folder_path`.
    pub fn from_path(folder_path: &StdPath) -> Result<MetadataDeltaLake, DeltaTableError> {
        // Ensure the directories in the path exists as LocalFileSystem otherwise returns an error.
        fs::create_dir_all(folder_path)
            .map_err(|error| DeltaTableError::generic(error.to_string()))?;

        // Use with_automatic_cleanup to ensure empty directories are deleted automatically.
        let local_file_system = Arc::new(
            LocalFileSystem::new_with_prefix(folder_path)
                .map_err(|error| DeltaTableError::generic(error.to_string()))?
                .with_automatic_cleanup(true),
        );

        let location = folder_path
            .to_str()
            .ok_or_else(|| DeltaTableError::generic("Local data folder path is not UTF-8."))?
            .to_owned();

        Ok(MetadataDeltaLake {
            location,
            storage_options: HashMap::new(),
            object_store: local_file_system,
            session: SessionContext::new(),
        })
    }

    /// Create a new [`MetadataDeltaLake`] that saves the metadata to [`METADATA_FOLDER`] in a
    /// remote object store given by `connection_info`. Returns [`DeltaTableError`] if
    /// `connection_info` could not be parsed or a connection cannot be made.
    pub fn try_from_connection_info(
        connection_info: &[u8],
    ) -> Result<MetadataDeltaLake, DeltaTableError> {
        let (object_store_type, offset_data) = arguments::decode_argument(connection_info)
            .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

        match object_store_type {
            "s3" => {
                // Register the S3 storage handlers to allow the use of Amazon S3 object stores.
                // This is required at runtime to initialize the S3 storage implementation in the
                // deltalake_aws storage subcrate.
                deltalake::aws::register_handlers(None);

                let (endpoint, bucket_name, access_key_id, secret_access_key, _offset_data) =
                    arguments::extract_s3_arguments(offset_data)
                        .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

                Self::try_from_s3_configuration(
                    endpoint.to_owned(),
                    bucket_name.to_owned(),
                    access_key_id.to_owned(),
                    secret_access_key.to_owned(),
                )
            }
            "azureblobstorage" => {
                let (account, access_key, container_name, _offset_data) =
                    arguments::extract_azure_blob_storage_arguments(offset_data)
                        .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

                Self::try_from_azure_configuration(
                    account.to_owned(),
                    access_key.to_owned(),
                    container_name.to_owned(),
                )
            }
            _ => Err(DeltaTableError::Generic(format!(
                "{object_store_type} is not supported."
            ))),
        }
    }

    /// Create a new [`MetadataDeltaLake`] that saves the metadata to [`METADATA_FOLDER`] in a
    /// remote S3-compatible object store. If a connection cannot be created [`DeltaTableError`] is
    /// returned.
    pub fn try_from_s3_configuration(
        endpoint: String,
        bucket_name: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> Result<Self, DeltaTableError> {
        let location = format!("s3://{bucket_name}");

        // TODO: Determine if it is safe to use AWS_S3_ALLOW_UNSAFE_RENAME.
        let storage_options = HashMap::from([
            ("REGION".to_owned(), "".to_owned()),
            ("ALLOW_HTTP".to_owned(), "true".to_owned()),
            ("ENDPOINT".to_owned(), endpoint),
            ("BUCKET_NAME".to_owned(), bucket_name),
            ("ACCESS_KEY_ID".to_owned(), access_key_id),
            ("SECRET_ACCESS_KEY".to_owned(), secret_access_key),
            ("AWS_S3_ALLOW_UNSAFE_RENAME".to_owned(), "true".to_owned()),
        ]);
        let url =
            Url::parse(&location).map_err(|error| DeltaTableError::Generic(error.to_string()))?;
        let (object_store, _path) = object_store::parse_url_opts(&url, &storage_options)?;

        Ok(MetadataDeltaLake {
            location,
            storage_options,
            object_store: Arc::new(object_store),
            session: SessionContext::new(),
        })
    }

    /// Create a new [`MetadataDeltaLake`] that saves the metadata to [`METADATA_FOLDER`] in a
    /// remote Azure-compatible object store. If a connection cannot be created [`DeltaTableError`]
    /// is returned.
    pub fn try_from_azure_configuration(
        account_name: String,
        access_key: String,
        container_name: String,
    ) -> Result<Self, DeltaTableError> {
        let location = format!("az://{container_name}");

        // TODO: Needs to be tested.
        let storage_options = HashMap::from([
            ("ACCOUNT_NAME".to_owned(), account_name),
            ("ACCESS_KEY".to_owned(), access_key),
            ("CONTAINER_NAME".to_owned(), container_name),
        ]);
        let url =
            Url::parse(&location).map_err(|error| DeltaTableError::Generic(error.to_string()))?;
        let (object_store, _path) = object_store::parse_url_opts(&url, &storage_options)?;

        Ok(MetadataDeltaLake {
            location,
            storage_options,
            object_store: Arc::new(object_store),
            session: SessionContext::new(),
        })
    }

    /// Use `table_name` to create a Delta Lake table with `columns` in the location given by
    /// `location` and `storage_options` if it does not already exist. The created table is
    /// registered in the Apache Arrow Datafusion session. If the table could not be created or
    /// registered, return [`DeltaTableError`].
    pub async fn create_delta_lake_table(
        &self,
        table_name: &str,
        columns: Vec<StructField>,
    ) -> Result<(), DeltaTableError> {
        let location = self.location_of_metadata_table(table_name);

        // SaveMode::Ignore is used to avoid errors if the table already exists.
        let table = Arc::new(
            CreateBuilder::new()
                .with_save_mode(SaveMode::Ignore)
                .with_storage_options(self.storage_options.clone())
                .with_table_name(table_name)
                .with_location(location)
                .with_columns(columns)
                .await?,
        );

        self.session.register_table(table_name, table.clone())?;

        Ok(())
    }

    /// Append `rows` to the table with the given `table_name`. If `rows` are appended to
    /// the table, return the updated [`DeltaTable`], otherwise return [`DeltaTableError`].
    pub async fn append_to_table(
        &self,
        table_name: &str,
        rows: Vec<ArrayRef>,
    ) -> Result<DeltaTable, DeltaTableError> {
        let table = self.metadata_delta_table(table_name).await?;

        // TableProvider::schema(&table) is used instead of table.schema() because table.schema()
        // returns the Delta Lake schema instead of the Apache Arrow DataFusion schema.
        let batch = RecordBatch::try_new(TableProvider::schema(&table), rows)?;

        let ops = DeltaOps::from(table);
        ops.write(vec![batch]).await
    }

    /// Return a [`DataFrame`] with the given `rows` for the metadata table with the given
    /// `table_name`. If the table does not exist or the [`DataFrame`] cannot be created, return
    /// [`DeltaTableError`].
    pub async fn metadata_table_data_frame(
        &self,
        table_name: &str,
        rows: Vec<ArrayRef>,
    ) -> Result<DataFrame, DeltaTableError> {
        let table = self.metadata_delta_table(table_name).await?;

        // TableProvider::schema(&table) is used instead of table.schema() because table.schema()
        // returns the Delta Lake schema instead of the Apache Arrow DataFusion schema.
        let batch = RecordBatch::try_new(TableProvider::schema(&table), rows)?;

        Ok(self.session.read_batch(batch)?)
    }

    // TODO: Look into optimizing the way we store and access tables in the struct fields (avoid open_table() every time).
    // TODO: Maybe we need to use the return value every time we do a table action to update the table.
    /// Return the [`DeltaOps`] for the metadata table with the given `table_name`. If the
    /// [`DeltaOps`] cannot be retrieved, return [`DeltaTableError`].
    pub async fn metadata_table_delta_ops(
        &self,
        table_name: &str,
    ) -> Result<DeltaOps, DeltaTableError> {
        let table = self.metadata_delta_table(table_name).await?;
        Ok(DeltaOps::from(table))
    }

    // TODO: Find a way to avoid having to re-register the table every time we want to read from it.
    /// Query the table with the given `table_name` using the given `query`. If the table is queried,
    /// return a [`RecordBatch`] with the query result, otherwise return [`DeltaTableError`].
    pub async fn query_table(
        &self,
        table_name: &str,
        query: &str,
    ) -> Result<RecordBatch, DeltaTableError> {
        let table = self.metadata_delta_table(table_name).await?;

        self.session.deregister_table(table_name)?;
        self.session.register_table(table_name, Arc::new(table))?;

        let dataframe = self.session.sql(query).await?;
        let schema = Schema::from(dataframe.schema());

        let batches = dataframe.collect().await?;
        let batch = concat_batches(&schema.into(), batches.as_slice())?;

        Ok(batch)
    }

    /// Return a [`DeltaTable`] for manipulating the metadata table with `table_name` in the
    /// metadata Delta Lake, or a [`DeltaTableError`] if a connection cannot be established or
    /// the table does not exist.
    pub async fn metadata_delta_table(
        &self,
        table_name: &str,
    ) -> Result<DeltaTable, DeltaTableError> {
        let table_path = self.location_of_metadata_table(table_name);
        open_table_with_storage_options(&table_path, self.storage_options.clone()).await
    }

    /// Return the location of the metadata table with `table_name`.
    fn location_of_metadata_table(&self, table_name: &str) -> String {
        format!("{}/{METADATA_FOLDER}/{table_name}", self.location)
    }
}

/// Extract the first 54-bits from `univariate_id` which is a hash computed from tags.
pub fn univariate_id_to_tag_hash(univariate_id: u64) -> u64 {
    univariate_id & 18446744073709550592
}

/// Extract the last 10-bits from `univariate_id` which is the index of the time series column.
pub fn univariate_id_to_column_index(univariate_id: u64) -> u16 {
    (univariate_id & 1023) as u16
}

/// Normalize `name` to allow direct comparisons between names.
pub fn normalize_name(name: &str) -> String {
    name.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for normalize_name().
    #[test]
    fn test_normalize_table_name_lowercase_no_effect() {
        assert_eq!("table_name", normalize_name("table_name"));
    }

    #[test]
    fn test_normalize_table_name_uppercase() {
        assert_eq!("table_name", normalize_name("TABLE_NAME"));
    }

    #[test]
    fn test_normalize_table_name_mixed_case() {
        assert_eq!("table_name", normalize_name("Table_Name"));
    }
}
