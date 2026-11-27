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

//! Implementation of functions used by the tests in multiple modules of this crate. They are not in
//! modelardb_test since it depends on the non-test build of this crate.

use std::collections::HashSet;

use arrow::array::RecordBatch;
use deltalake::DeltaTable;
use futures::TryStreamExt;
use modelardb_test::table::{self, TABLE_NAME};
use modelardb_types::types::TableMetadata;
use tempfile::TempDir;

use crate::data_folder::DataFolder;

/// Number of rows in the record batches written by [`write_record_batch()`].
pub(crate) const ROW_COUNT: usize = 5;

/// Return a [`DataFolder`] in a [`TempDir`] with a table created by [`table::TABLE_SQL`].
pub(crate) async fn create_data_folder_with_table() -> (TempDir, DataFolder) {
    create_data_folder_with_table_metadata(&table::table_metadata()).await
}

/// Return a [`DataFolder`] in a [`TempDir`] with a table that has `table_metadata`.
pub(crate) async fn create_data_folder_with_table_metadata(
    table_metadata: &TableMetadata,
) -> (TempDir, DataFolder) {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

    data_folder.create_table(table_metadata).await.unwrap();

    (temp_dir, data_folder)
}

/// Write a record batch with [`ROW_COUNT`] rows to the table created by [`table::TABLE_SQL`] in
/// `data_folder` and return the updated [`DeltaTable`].
pub(crate) async fn write_record_batch(data_folder: &DataFolder) -> DeltaTable {
    data_folder
        .write_record_batches(
            TABLE_NAME,
            &[table::table_record_batch(ROW_COUNT)],
            HashSet::new(),
        )
        .await
        .unwrap()
}

/// Return the paths of the files in the table created by [`table::TABLE_SQL`] in `data_folder`.
pub(crate) async fn file_paths(data_folder: &DataFolder) -> Vec<String> {
    let delta_table = data_folder.delta_table(TABLE_NAME).await.unwrap();

    delta_table
        .snapshot()
        .unwrap()
        .log_data()
        .iter()
        .map(|file| file.path().to_string())
        .collect()
}

/// Return the number of rows in the table created by [`table::TABLE_SQL`] in `data_folder`.
pub(crate) async fn row_count(data_folder: &DataFolder) -> usize {
    let delta_table = data_folder.delta_table(TABLE_NAME).await.unwrap();
    let (_table, stream) = delta_table.scan_table().await.unwrap();

    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    batches.iter().map(|batch| batch.num_rows()).sum()
}

/// Return the names of the files and folders in the folder of the table created by
/// [`table::TABLE_SQL`] in the data folder in `temp_dir`.
pub(crate) fn files_in_table_folder(temp_dir: &TempDir) -> Vec<String> {
    std::fs::read_dir(temp_dir.path().join("tables").join(TABLE_NAME))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect()
}
