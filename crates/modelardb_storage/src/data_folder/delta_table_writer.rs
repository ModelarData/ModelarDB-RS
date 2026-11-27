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

use std::collections::HashSet;

use arrow::array::RecordBatch;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use deltalake::DeltaTable;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{Action, Add};
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::json;
use uuid::Uuid;

use crate::error::{ModelarDbStorageError, Result};

/// Functionality for transactionally writing [`RecordBatches`](RecordBatch) to a Delta table stored
/// in an object store. Every commit is marked as a blind append, so it does not conflict with
/// concurrent commits that rewrite or remove files. Thus, it must only be used for writes that do
/// not depend on reading the same table.
pub struct DeltaTableWriter {
    /// Delta table that all of the record batches will be written to.
    delta_table: DeltaTable,
    /// Write operation that will be committed to the Delta table.
    delta_operation: DeltaOperation,
    /// Unique identifier for this write operation to the Delta table.
    operation_id: Uuid,
    /// Writes record batches to the Delta table as Apache Parquet files.
    delta_writer: DeltaWriter,
    /// Batch ids from the WAL to include in the commit metadata so the uncompressed batches that
    /// correspond to the written data can be deleted from the WAL.
    batch_ids: HashSet<u64>,
}

impl DeltaTableWriter {
    /// Create a new [`DeltaTableWriter`] that writes to `delta_table` with its partition columns and
    /// the encoding policy for its schema. Returns a [`ModelarDbStorageError`] if the state of the
    /// Delta table cannot be loaded from `delta_table`.
    pub async fn try_new(delta_table: DeltaTable) -> Result<Self> {
        let snapshot = delta_table.snapshot()?;
        let partition_columns = snapshot.metadata().partition_columns().to_vec();
        let writer_properties =
            modelardb_compression::writer_properties(&snapshot.snapshot().arrow_schema());

        // Operation that will be committed. deltalake checks a commit without a predicate as if it
        // read every file in its snapshot, so it would conflict with concurrent commits that remove
        // files, e.g., TRUNCATE. The predicate false states that the blind append read no files.
        let delta_operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: if partition_columns.is_empty() {
                None
            } else {
                Some(partition_columns.clone())
            },
            predicate: Some("false".to_owned()),
        };

        // A UUID version 4 is used as the operation id to match the existing Operation trait in the
        // deltalake crate as it is pub(crate) and thus cannot be used directly in DeltaTableWriter.
        let operation_id = Uuid::new_v4();

        // Writer that will write the record batches.
        let object_store = delta_table.log_store().object_store(Some(operation_id));
        let schema = delta_table.table_provider().await?.schema();
        let num_indexed_cols = DataSkippingNumIndexedCols::NumColumns(schema.fields.len() as u64);
        let writer_config = WriterConfig::new(
            schema,
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
            delta_operation,
            operation_id,
            delta_writer,
            batch_ids: HashSet::new(),
        })
    }

    /// Add batch ids from the WAL that are included in the commit metadata so the uncompressed
    /// batches that correspond to the written data can be deleted from the WAL.
    pub fn with_batch_ids(mut self, batch_ids: HashSet<u64>) -> Self {
        self.batch_ids = batch_ids;
        self
    }

    /// Write `record_batch` to the Delta table. Returns a [`ModelarDbStorageError`] if the
    /// [`RecordBatches`](RecordBatch) does not match the schema of the Delta table or if the
    /// writing fails.
    pub async fn write(&mut self, record_batch: &RecordBatch) -> Result<()> {
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
    /// written to the log as a blind append. If an error occurs before the commit is finished, the
    /// already written files are deleted if possible. Returns a [`ModelarDbStorageError`] if an
    /// error occurs when finishing the writing, committing the files that have been written,
    /// deleting the written files, or updating the [`DeltaTable`].
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

        // The commit is a blind append as it only adds files, so it does not conflict with
        // concurrent commits that rewrite or remove files when the isolation level is
        // WriteSerializable.
        let mut commit_metadata = vec![("isBlindAppend".to_owned(), json!(true))];
        if !self.batch_ids.is_empty() {
            commit_metadata.push(("batchIds".to_owned(), json!(self.batch_ids)));
        }
        let commit_properties = CommitProperties::default().with_metadata(commit_metadata);

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

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::sync::Arc;

    use arrow::array::{Float32Array, StringViewArray, TimestampMicrosecondArray};
    use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
    use futures::TryStreamExt;
    use modelardb_test::table::{self, TABLE_NAME};
    use modelardb_types::types::{
        ArrowTimestamp, ArrowValue, ColumnMetadata, ErrorBound, TableMetadata,
    };
    use parquet::basic::{Compression, Encoding, ZstdLevel};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use tempfile::TempDir;

    use crate::test::{
        ROW_COUNT, create_data_folder_with_table, create_data_folder_with_table_metadata,
        file_paths, row_count, write_record_batch,
    };

    // Tests for DeltaTableWriter.
    #[tokio::test]
    async fn test_commit_alongside_remove_files() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;
        write_record_batch(&data_folder).await;
        let file_paths_to_remove = file_paths(&data_folder).await;

        // The files are removed after the writer loaded its snapshot, as if it happened while the
        // writer was writing.
        let delta_table_writer = data_folder.table_writer(TABLE_NAME).await.unwrap();
        data_folder
            .remove_files(TABLE_NAME, &file_paths_to_remove)
            .await
            .unwrap();

        delta_table_writer
            .write_all_and_commit(&[table::table_record_batch(ROW_COUNT)])
            .await
            .unwrap();

        // Only the file written by the writer should remain.
        let remaining_file_paths = file_paths(&data_folder).await;
        assert_eq!(remaining_file_paths.len(), 1);
        assert!(!file_paths_to_remove.contains(&remaining_file_paths[0]));
        assert_eq!(row_count(&data_folder).await, ROW_COUNT);
    }

    #[tokio::test]
    async fn test_commit_alongside_truncate() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;
        write_record_batch(&data_folder).await;

        // The table is truncated after the writer loaded its snapshot, as if it happened while the
        // writer was writing.
        let delta_table_writer = data_folder.table_writer(TABLE_NAME).await.unwrap();
        data_folder.truncate_table(TABLE_NAME).await.unwrap();

        delta_table_writer
            .write_all_and_commit(&[table::table_record_batch(ROW_COUNT)])
            .await
            .unwrap();

        // Only the rows written by the writer should remain.
        assert_eq!(row_count(&data_folder).await, ROW_COUNT);
    }

    #[tokio::test]
    async fn test_commit_alongside_optimize() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;
        write_record_batch(&data_folder).await;
        write_record_batch(&data_folder).await;

        // The table is optimized after the writer loaded its snapshot, as if it happened while the
        // writer was writing.
        let delta_table_writer = data_folder.table_writer(TABLE_NAME).await.unwrap();
        data_folder.optimize_table(TABLE_NAME, None).await.unwrap();

        delta_table_writer
            .write_all_and_commit(&[table::table_record_batch(ROW_COUNT)])
            .await
            .unwrap();

        // The optimized rows and the rows written by the writer should all remain.
        assert_eq!(row_count(&data_folder).await, 3 * ROW_COUNT);
    }

    #[tokio::test]
    async fn test_commit_with_batch_ids() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let delta_table = data_folder
            .table_writer(TABLE_NAME)
            .await
            .unwrap()
            .with_batch_ids(HashSet::from([1, 2]))
            .write_all_and_commit(&[table::table_record_batch(ROW_COUNT)])
            .await
            .unwrap();

        let commit_batch_ids = last_commit_metadata(&delta_table, "batchIds")
            .await
            .unwrap();
        let mut batch_ids: Vec<u64> = serde_json::from_value(commit_batch_ids).unwrap();
        batch_ids.sort();

        assert_eq!(batch_ids, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_commit_without_batch_ids() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let delta_table = write_record_batch(&data_folder).await;

        assert!(
            last_commit_metadata(&delta_table, "batchIds")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_commit_is_blind_append() {
        let (_temp_dir, data_folder) = create_data_folder_with_table().await;

        let delta_table = write_record_batch(&data_folder).await;

        let mut history = delta_table.history(Some(1));
        let commit = history.try_next().await.unwrap().unwrap();
        assert_eq!(commit.is_blind_append, Some(true));
    }

    #[tokio::test]
    async fn test_rollback() {
        let (temp_dir, data_folder) = create_data_folder_with_table().await;

        let mut delta_table_writer = data_folder.table_writer(TABLE_NAME).await.unwrap();
        delta_table_writer
            .write(&table::table_record_batch(ROW_COUNT))
            .await
            .unwrap();
        let delta_table = delta_table_writer.rollback().await.unwrap();

        // The written file should be deleted and nothing should be committed.
        assert_eq!(files_in_table_folder(&temp_dir), vec!["_delta_log"]);
        assert_eq!(delta_table.version(), Some(0));
    }

    #[tokio::test]
    async fn test_write_all_and_commit_with_invalid_record_batch() {
        let (temp_dir, data_folder) = create_data_folder_with_table().await;

        let invalid_record_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp",
                ArrowTimestamp::DATA_TYPE,
                true,
            )])),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![0]))],
        )
        .unwrap();

        let result = data_folder
            .table_writer(TABLE_NAME)
            .await
            .unwrap()
            .write_all_and_commit(&[table::table_record_batch(ROW_COUNT), invalid_record_batch])
            .await;

        assert!(
            result
                .unwrap_err()
                .to_string()
                .starts_with("Delta Lake Error: Data does not match the schema or partitions")
        );

        // The valid record batch should be rolled back, so no files are left and nothing is
        // committed.
        assert_eq!(files_in_table_folder(&temp_dir), vec!["_delta_log"]);
        assert_eq!(
            data_folder.delta_table(TABLE_NAME).await.unwrap().version(),
            Some(0)
        );
        assert_eq!(row_count(&data_folder).await, 0);
    }

    #[tokio::test]
    async fn test_written_file_uses_encoding_policy() {
        let query_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, true),
            Field::new("value", ArrowValue::DATA_TYPE, true),
            Field::new("text", DataType::Utf8View, true),
        ]));
        let table_metadata = TableMetadata::try_new(
            TABLE_NAME.to_owned(),
            query_schema.clone(),
            vec![ColumnMetadata::Stored(ErrorBound::Lossless); 3],
        )
        .unwrap();
        let (temp_dir, data_folder) = create_data_folder_with_table_metadata(&table_metadata).await;

        let record_batch = RecordBatch::try_new(
            query_schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![0, 1, 2])),
                Arc::new(Float32Array::from(vec![0.0, 1.0, 2.0])),
                Arc::new(StringViewArray::from(vec!["text", "text", "other_text"])),
            ],
        )
        .unwrap();

        data_folder
            .write_record_batches(TABLE_NAME, &[record_batch], HashSet::new())
            .await
            .unwrap();

        let file_path = temp_dir
            .path()
            .join("tables")
            .join(TABLE_NAME)
            .join(&file_paths(&data_folder).await[0]);
        let file_reader = SerializedFileReader::new(File::open(file_path).unwrap()).unwrap();
        let row_group = file_reader.metadata().row_group(0);

        // Every column is compressed with ZSTD and only has statistics for the column chunk.
        for column in row_group.columns() {
            assert_eq!(
                column.compression(),
                Compression::ZSTD(ZstdLevel::default())
            );
            assert!(column.statistics().is_some());
            assert!(column.column_index_offset().is_none());
        }

        // Only the string column is dictionary encoded.
        let encodings: Vec<Vec<Encoding>> = row_group
            .columns()
            .iter()
            .map(|column| column.encodings().collect())
            .collect();

        assert!(encodings[0].contains(&Encoding::DELTA_BINARY_PACKED));
        assert!(row_group.column(0).dictionary_page_offset().is_none());
        assert!(encodings[1].contains(&Encoding::BYTE_STREAM_SPLIT));
        assert!(row_group.column(1).dictionary_page_offset().is_none());
        assert!(encodings[2].contains(&Encoding::RLE_DICTIONARY));
        assert!(row_group.column(2).dictionary_page_offset().is_some());
    }

    async fn last_commit_metadata(
        delta_table: &DeltaTable,
        key: &str,
    ) -> Option<serde_json::Value> {
        let mut history = delta_table.history(Some(1));
        let commit = history.try_next().await.unwrap().unwrap();

        commit.info.get(key).cloned()
    }

    fn files_in_table_folder(temp_dir: &TempDir) -> Vec<String> {
        std::fs::read_dir(temp_dir.path().join("tables").join(TABLE_NAME))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }
}
