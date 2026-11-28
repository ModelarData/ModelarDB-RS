/* Copyright 2025 The ModelarDB Contributors
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

//! Operations for interacting with ModelarDB Apache Arrow Flight servers and data folders.

pub mod client;
pub mod data_folder;

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::DFSchema;
use datafusion::execution::RecordBatchStream;
use modelardb_storage::parser::tokenize_and_parse_sql_expression;
use modelardb_types::types::{ColumnMetadata, ErrorBound, GeneratedColumn, TableMetadata};

use crate::error::{ModelarDbEmbeddedError, Result};

/// Different types of ModelarDB instances that the Operations API can interact with.
#[derive(Debug)]
pub enum ModelarDBType {
    SingleEdge,
    ClusterEdge,
    ClusterCloud,
    DataFolder,
}

impl FromStr for ModelarDBType {
    type Err = ModelarDbEmbeddedError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "SingleEdge" => Ok(ModelarDBType::SingleEdge),
            "ClusterEdge" => Ok(ModelarDBType::ClusterEdge),
            "ClusterCloud" => Ok(ModelarDBType::ClusterCloud),
            "DataFolder" => Ok(ModelarDBType::DataFolder),
            _ => Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "'{value}' is not a valid value for ModelarDBType."
            ))),
        }
    }
}

/// Trait for interacting with ModelarDB, either through an Apache Arrow Flight server or a data
/// folder.
#[async_trait]
pub trait Operations: Sync + Send {
    /// Returns the [`Operations`] instance as [`Any`] so that it can be downcast to a specific
    /// implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns the type of the ModelarDB instance that this [`Operations`] instance interacts with.
    async fn modelardb_type(&mut self) -> Result<ModelarDBType>;

    /// Creates a table with the name in `table_name`, the columns in `schema`, the error bounds in
    /// `error_bounds`, and the generated column expressions in `generated_columns`. Columns without
    /// an error bound or a generated column expression are stored losslessly.
    async fn create(
        &mut self,
        table_name: &str,
        schema: Schema,
        error_bounds: HashMap<String, ErrorBound>,
        generated_columns: HashMap<String, String>,
    ) -> Result<()>;

    /// Returns the name of all the tables.
    async fn tables(&mut self) -> Result<Vec<String>>;

    /// Returns the schema of the table with the name in `table_name`.
    async fn schema(&mut self, table_name: &str) -> Result<Schema>;

    /// Writes the data in `data` to the table with the name in `table_name`. The error bounds of
    /// the table are applied to the data.
    async fn write(&mut self, table_name: &str, data: RecordBatch) -> Result<()>;

    /// Executes the SQL in `sql` and returns the result as a [`RecordBatchStream`].
    async fn read(&mut self, sql: &str) -> Result<Pin<Box<dyn RecordBatchStream + Send>>>;

    /// Executes the SQL in `sql` and writes the result to the table with the name in
    /// `target_table_name` in `target`. The error bounds of the target table are applied to the
    /// result, so data copied from a table with error bounds can deviate more than either bound.
    async fn copy(
        &mut self,
        sql: &str,
        target: &mut dyn Operations,
        target_table_name: &str,
    ) -> Result<()>;

    /// Move all data from the table with the name in `source_table_name` in `self` to the table with
    /// the name in `target_table_name` in `target` by copying the stored columns and then
    /// truncating the source table.
    async fn r#move(
        &mut self,
        source_table_name: &str,
        target: &dyn Operations,
        target_table_name: &str,
    ) -> Result<()>;

    /// Truncate the table with the name in `table_name`.
    async fn truncate(&mut self, table_name: &str) -> Result<()>;

    /// Drop the table with the name in `table_name`.
    async fn drop(&mut self, table_name: &str) -> Result<()>;

    /// Vacuum the table with the name in `table_name` by deleting stale files that are older than
    /// `maybe_retention_period_in_seconds` seconds. If a retention period is not given, the
    /// default retention period of 7 days is used.
    async fn vacuum(
        &mut self,
        table_name: &str,
        maybe_retention_period_in_seconds: Option<u64>,
    ) -> Result<()>;

    /// Optimize the table with the name in `table_name` by merging its many small files into
    /// fewer larger files of approximately `maybe_target_size_in_bytes` bytes. If a target size is
    /// not given, the default target size of 64 MiB is used.
    async fn optimize(
        &mut self,
        table_name: &str,
        maybe_target_size_in_bytes: Option<u64>,
    ) -> Result<()>;
}

/// Use the time series table metadata in `table_name`, `schema`, `error_bounds`, and `generated_columns`
/// to create [`TimeSeriesTableMetadata`]. If the metadata is valid, return [`TimeSeriesTableMetadata`],
/// otherwise return [`ModelarDbEmbeddedError`].
fn try_new_time_series_table_metadata(
    table_name: &str,
    schema: Schema,
    mut error_bounds: HashMap<String, ErrorBound>,
    generated_columns: HashMap<String, String>,
) -> Result<TimeSeriesTableMetadata> {
    let schema = Arc::new(schema);
    let df_schema: DFSchema = schema.clone().try_into()?;

    let mut error_bounds_all = Vec::with_capacity(schema.fields().len());
    let mut generated_columns_all = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        error_bounds_all.push(
            error_bounds
                .remove(field.name())
                .unwrap_or(ErrorBound::Lossless),
        );

        if let Some(sql_expr) = generated_columns.get(field.name()) {
            let expr = tokenize_and_parse_sql_expression(sql_expr, &df_schema)?;
            generated_columns_all.push(Some(GeneratedColumn::try_from_expr(expr, &df_schema)?));
        } else {
            generated_columns_all.push(None);
        }
    }

    TimeSeriesTableMetadata::try_new(
        table_name.to_owned(),
        schema,
        error_bounds_all,
        generated_columns_all,
    )
    .map_err(|error| error.into())
}

    }

    }

}
