/* Copyright 2026 The ModelarDB Contributors
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

//! Implementation of [`DeltaTableWriter`] for transactionally writing
//! [`RecordBatches`](RecordBatch) to a Delta table stored in an object store. Writing can be
//! committed or rolled back to ensure that the Delta table is always in a consistent state.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use datafusion::catalog::TableProvider;
use datafusion::parquet::file::metadata::SortingColumn;
use datafusion::parquet::file::properties::WriterProperties;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use deltalake::DeltaTable;
use deltalake::delta_datafusion::DeltaDataChecker;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{Action, Add};
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use modelardb_types::schemas::{COMPRESSED_SCHEMA, FIELD_COLUMN};
use object_store::ObjectStore;
use object_store::path::Path;
use uuid::Uuid;

use crate::apache_parquet_writer_properties;
use crate::error::{ModelarDbStorageError, Result};

/// Functionality for transactionally writing [`RecordBatches`](RecordBatch) to a Delta table stored
/// in an object store.
pub struct DeltaTableWriter {
    /// Delta table that all of the record batches will be written to.
    delta_table: DeltaTable,
    /// Checker that ensures all of the record batches match the table.
    delta_data_checker: DeltaDataChecker,
    /// Write operation that will be committed to the Delta table.
    delta_operation: DeltaOperation,
    /// Unique identifier for this write operation to the Delta table.
    operation_id: Uuid,
    /// Writes record batches to the Delta table as Apache Parquet files.
    delta_writer: DeltaWriter,
}

impl DeltaTableWriter {
    /// Create a [`DeltaTableWriter`] configured for writing to a normal table.
    pub(crate) fn try_new_for_normal_table(delta_table: DeltaTable) -> Result<Self> {
        let writer_properties = apache_parquet_writer_properties(None);
        Self::try_new(delta_table, vec![], writer_properties)
    }

    /// Create a [`DeltaTableWriter`] configured for writing to a time series table.
    pub(crate) fn try_new_for_time_series_table(delta_table: DeltaTable) -> Result<Self> {
        let partition_columns = vec![FIELD_COLUMN.to_owned()];

        // Specify that the file must be sorted by the tag columns and then by start_time.
        let base_compressed_schema_len = COMPRESSED_SCHEMA.0.fields().len();
        let compressed_schema_len = TableProvider::schema(&delta_table).fields().len();
        let sorting_columns_len = (compressed_schema_len - base_compressed_schema_len) + 1;
        let mut sorting_columns = Vec::with_capacity(sorting_columns_len);

        // Compressed segments have the tag columns at the end of the schema.
        for tag_column_index in base_compressed_schema_len..compressed_schema_len {
            sorting_columns.push(SortingColumn {
                column_idx: tag_column_index as i32,
                descending: false,
                nulls_first: false,
            });
        }

        // Compressed segments store the first timestamp in the second column.
        sorting_columns.push(SortingColumn {
            column_idx: 1,
            descending: false,
            nulls_first: false,
        });

        let writer_properties = apache_parquet_writer_properties(Some(sorting_columns));
        Self::try_new(delta_table, partition_columns, writer_properties)
    }

    /// Create a new [`DeltaTableWriter`]. Returns a [`ModelarDbStorageError`] if the state of the
    /// Delta table cannot be loaded from `delta_table`.
    pub fn try_new(
        delta_table: DeltaTable,
        partition_columns: Vec<String>,
        writer_properties: WriterProperties,
    ) -> Result<Self> {
        // Checker for if record batches match the table’s invariants, constraints, and nullability.
        let delta_table_state = delta_table.snapshot()?;
        let snapshot = delta_table_state.snapshot();
        let delta_data_checker = DeltaDataChecker::new(snapshot);

        // Operation that will be committed.
        let delta_operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: if partition_columns.is_empty() {
                None
            } else {
                Some(partition_columns.clone())
            },
            predicate: None,
        };

        // A UUID version 4 is used as the operation id to match the existing Operation trait in the
        // deltalake crate as it is pub(trait) and thus cannot be used directly in DeltaTableWriter.
        let operation_id = Uuid::new_v4();

        // Writer that will write the record batches.
        let object_store = delta_table.log_store().object_store(Some(operation_id));
        let table_schema: Arc<Schema> = TableProvider::schema(&delta_table);
        let num_indexed_cols =
            DataSkippingNumIndexedCols::NumColumns(table_schema.fields.len() as u64);
        let writer_config = WriterConfig::new(
            table_schema,
            partition_columns,
            Some(writer_properties),
            None,
            None,
            num_indexed_cols,
            None,
        );
        let delta_writer = DeltaWriter::new(object_store, writer_config);

        Ok(Self {
            delta_table,
            delta_data_checker,
            delta_operation,
            operation_id,
            delta_writer,
        })
    }

    /// Write `record_batch` to the Delta table. Returns a [`ModelarDbStorageError`] if the
    /// [`RecordBatches`](RecordBatch) does not match the schema of the Delta table or if the
    /// writing fails.
    pub async fn write(&mut self, record_batch: &RecordBatch) -> Result<()> {
        self.delta_data_checker.check_batch(record_batch).await?;
        self.delta_writer.write(record_batch).await?;
        Ok(())
    }

    /// Write all `record_batches` to the Delta table. Returns a [`ModelarDbStorageError`] if one of
    /// the [`RecordBatches`](RecordBatch) does not match the schema of the Delta table or if the
    /// writing fails.
    pub async fn write_all(&mut self, record_batches: &[RecordBatch]) -> Result<()> {
        for record_batch in record_batches {
            self.write(record_batch).await?;
        }
        Ok(())
    }

    /// Write all `record_batches` and commit. If writing fails, roll back all writes and return
    /// [`ModelarDbStorageError`]. Returns the updated [`DeltaTable`] if all `record_batches` are
    /// written and committed successfully.
    pub async fn write_all_and_commit(
        mut self,
        record_batches: &[RecordBatch],
    ) -> Result<DeltaTable> {
        match self.write_all(record_batches).await {
            Ok(_) => self.commit().await,
            Err(error) => {
                self.rollback().await?;
                Err(error)
            }
        }
    }

    /// Consume the [`DeltaTableWriter`], finish the writing, and commit the files that have been
    /// written to the log. If an error occurs before the commit is finished, the already written
    /// files are deleted if possible. Returns a [`ModelarDbStorageError`] if an error occurs when
    /// finishing the writing, committing the files that have been written, deleting the written
    /// files, or updating the [`DeltaTable`].
    pub async fn commit(mut self) -> Result<DeltaTable> {
        // Write the remaining buffered files.
        let added_files = self.delta_writer.close().await?;

        // Clone added_files in case of rollback.
        let actions = added_files
            .clone()
            .into_iter()
            .map(Action::Add)
            .collect::<Vec<Action>>();

        // Prepare all inputs to the commit.
        let object_store = self.delta_table.object_store();
        let commit_properties = CommitProperties::default();
        let table_data = match self.delta_table.snapshot() {
            Ok(table_data) => table_data,
            Err(delta_table_error) => {
                delete_added_files(&object_store, added_files).await?;
                return Err(ModelarDbStorageError::DeltaLake(delta_table_error));
            }
        };
        let log_store = self.delta_table.log_store();

        // Construct the commit to be written.
        let commit_builder = CommitBuilder::from(commit_properties)
            .with_actions(actions)
            .with_operation_id(self.operation_id)
            .build(Some(table_data), log_store, self.delta_operation);

        // Write the commit to the Delta table.
        let _finalized_commit = match commit_builder.await {
            Ok(finalized_commit) => finalized_commit,
            Err(delta_table_error) => {
                delete_added_files(&object_store, added_files).await?;
                return Err(ModelarDbStorageError::DeltaLake(delta_table_error));
            }
        };

        // Return Delta table with the commit.
        self.delta_table.load().await?;
        Ok(self.delta_table)
    }

    /// Consume the [`DeltaTableWriter`], abort the writing, and delete all of the files that have
    /// already been written. Returns a [`ModelarDbStorageError`] if an error occurs when aborting
    /// the writing or deleting the files that have already been written. Rollback is not called
    /// automatically as drop() is not async and async_drop() is not yet a stable API.
    pub async fn rollback(self) -> Result<DeltaTable> {
        let object_store = self.delta_table.object_store();
        let added_files = self.delta_writer.close().await?;
        delete_added_files(&object_store, added_files).await?;
        Ok(self.delta_table)
    }
}

/// Delete the `added_files` from `object_store`. Returns a [`ModelarDbStorageError`] if a file
/// could not be deleted. It is a function instead of a method on [`DeltaTableWriter`] so it can be
/// called by [`DeltaTableWriter`] after the [`DeltaWriter`] is closed without lifetime issues.
async fn delete_added_files(object_store: &dyn ObjectStore, added_files: Vec<Add>) -> Result<()> {
    for add_file in added_files {
        let path: Path = Path::from(add_file.path);
        object_store.delete(&path).await?;
    }
    Ok(())
}
