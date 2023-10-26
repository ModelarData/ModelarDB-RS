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

//! Support for handling file metadata for files that contain compressed segments and conversion
//! methods to convert between multiple related data structures.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{TimeZone, Utc};
use datafusion::arrow::compute::kernels::aggregate;
use datafusion::arrow::record_batch::RecordBatch;
use modelardb_common::types::{Timestamp, TimestampArray, Value, ValueArray};
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectMeta;
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use uuid::Uuid;

/// Metadata about a file tracked by [`MetadataManager`] which contains compressed segments.
#[derive(Debug, Clone)]
pub struct CompressedFile {
    /// Name of the file.
    pub(super) name: Uuid,
    /// Path to the folder containing the file.
    pub(super) folder_path: PathBuf,
    /// Size of the file in bytes.
    pub(crate) size: usize,
    /// Timestamp that the file was created at.
    pub(super) created_at: Timestamp,
    /// Timestamp of the first data point in the file.
    pub(super) start_time: Timestamp,
    /// Timestamp of the last data point in the file.
    pub(super) end_time: Timestamp,
    /// Value of the data point with the smallest value in the file.
    pub(super) min_value: Value,
    /// Value of the data point with the largest value in the file.
    pub(super) max_value: Value,
}

impl CompressedFile {
    pub fn new(
        name: Uuid,
        folder_path: PathBuf,
        size: usize,
        created_at: Timestamp,
        start_time: Timestamp,
        end_time: Timestamp,
        min_value: Value,
        max_value: Value,
    ) -> Self {
        Self {
            name,
            folder_path,
            size,
            created_at,
            start_time,
            end_time,
            min_value,
            max_value,
        }
    }

    /// Convert the given file information and [`RecordBatch`] to a [`CompressedFile`].
    pub(crate) fn from_record_batch(
        name: Uuid,
        folder_path: PathBuf,
        size: usize,
        batch: RecordBatch,
    ) -> Self {
        // unwrap() is safe since UNIX_EPOCH is always earlier than now.
        let since_the_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let created_at = since_the_epoch.as_millis() as Timestamp;

        // unwrap() is safe as None is only returned if all of the values are None.
        let start_time =
            aggregate::min(modelardb_common::array!(batch, 2, TimestampArray)).unwrap();
        let end_time = aggregate::max(modelardb_common::array!(batch, 3, TimestampArray)).unwrap();

        // unwrap() is safe as None is only returned if all of the values are None.
        // Both aggregate::min() and aggregate::max() consider NaN to be greater than other non-null
        // values. So since min_values and max_values cannot contain null, min_value will be NaN if all
        // values in min_values are NaN while max_value will be NaN if any value in max_values is NaN.
        let min_value = aggregate::min(modelardb_common::array!(batch, 5, ValueArray)).unwrap();
        let max_value = aggregate::max(modelardb_common::array!(batch, 6, ValueArray)).unwrap();

        Self {
            name,
            folder_path,
            size,
            created_at,
            start_time,
            end_time,
            min_value,
            max_value,
        }
    }

    /// Extract the file name from each [`ObjectMeta`] in `object_metas` and convert it to an UUID,
    /// if possible. If any [`ObjectMeta`] is not an Apache Parquet file with an UUID file name,
    /// return [`String`].
    pub(crate) fn object_metas_to_compressed_file_names(
        object_metas: Vec<ObjectMeta>,
    ) -> Result<Vec<Uuid>, String> {
        object_metas
            .iter()
            .map(|object_meta| {
                Uuid::try_parse(
                    object_meta
                        .location
                        .filename()
                        .ok_or_else(|| "Object meta does not have a file name.")?
                        .strip_suffix(".parquet")
                        .ok_or_else(|| "Object meta is not an Apache Parquet file.")?,
                )
                .map_err(|error| error.to_string())
            })
            .collect()
    }
}

impl TryFrom<SqliteRow> for CompressedFile {
    type Error = sqlx::Error;

    fn try_from(row: SqliteRow) -> Result<Self, Self::Error> {
        let size: i64 = row.try_get("size")?;
        let folder_path: String = row.try_get("folder_path")?;

        Ok(Self::new(
            row.try_get("file_name")?,
            folder_path.into(),
            size as usize,
            row.try_get("created_at")?,
            row.try_get("start_time")?,
            row.try_get("end_time")?,
            row.try_get("min_value")?,
            row.try_get("max_value")?,
        ))
    }
}

impl Into<ObjectMeta> for CompressedFile {
    fn into(self) -> ObjectMeta {
        // unwrap() is safe as the folder path is generated from the table name which is valid UTF-8.
        let file_path = ObjectStorePath::from(format!(
            "{}/{}.parquet",
            self.folder_path.to_str().unwrap(),
            self.name
        ));

        // unwrap() is safe as the created_at timestamp cannot be out of range.
        ObjectMeta {
            location: file_path,
            last_modified: Utc.timestamp_millis_opt(self.created_at).unwrap(),
            size: self.size,
            e_tag: None,
        }
    }
}
