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
use uuid::Uuid;

/// Metadata about a file tracked by [`MetadataManager`](crate::metadata::MetadataManager) which
/// contains compressed segments.
#[derive(Debug, Clone, PartialEq)]
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
        batch: &RecordBatch,
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
        object_metas: &[ObjectMeta],
    ) -> Result<Vec<Uuid>, String> {
        object_metas
            .iter()
            .map(|object_meta| {
                Uuid::try_parse(
                    object_meta
                        .location
                        .filename()
                        .ok_or("Object meta does not have a file name.")?
                        .strip_suffix(".parquet")
                        .ok_or("Object meta is not an Apache Parquet file.")?,
                )
                .map_err(|error| error.to_string())
            })
            .collect()
    }
}

impl From<CompressedFile> for ObjectMeta {
    fn from(compressed_file: CompressedFile) -> Self {
        // unwrap() is safe as the folder path is generated from the table name which is valid UTF-8.
        let location = ObjectStorePath::from(format!(
            "{}/{}.parquet",
            compressed_file.folder_path.to_str().unwrap(),
            compressed_file.name
        ));

        // unwrap() is safe as the created_at timestamp cannot be out of range.
        let last_modified = Utc
            .timestamp_millis_opt(compressed_file.created_at)
            .unwrap();

        ObjectMeta {
            location,
            last_modified,
            size: compressed_file.size,
            e_tag: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::SubsecRound;

    use crate::common_test;

    const TABLE_NAME: &str = "model_table";

    // Tests for object_metas_to_compressed_file_names().
    #[test]
    fn test_object_metas_to_compressed_file_names() {
        let uuid_1 = Uuid::new_v4();
        let uuid_2 = Uuid::new_v4();

        let object_metas = vec![
            create_object_meta(format!("test/{}.parquet", uuid_1)),
            create_object_meta(format!("test/{}.parquet", uuid_2)),
        ];

        let compressed_files =
            CompressedFile::object_metas_to_compressed_file_names(&object_metas).unwrap();

        assert_eq!(compressed_files.get(0), Some(&uuid_1));
        assert_eq!(compressed_files.get(1), Some(&uuid_2));
    }

    #[test]
    fn test_folder_object_metas_to_compressed_file_names() {
        let object_metas = vec![
            create_object_meta(format!("test/{}.parquet", Uuid::new_v4())),
            create_object_meta("test/folder".to_owned()),
        ];

        let compressed_files = CompressedFile::object_metas_to_compressed_file_names(&object_metas);
        assert!(compressed_files.is_err());
    }

    #[test]
    fn test_non_apache_parquet_object_metas_to_compressed_file_names() {
        let object_metas = vec![
            create_object_meta(format!("test/{}.parquet", Uuid::new_v4())),
            create_object_meta(format!("test/{}.csv", Uuid::new_v4())),
        ];

        let compressed_files = CompressedFile::object_metas_to_compressed_file_names(&object_metas);
        assert!(compressed_files.is_err());
    }

    #[test]
    fn test_non_uuid_object_metas_to_compressed_file_names() {
        let object_metas = vec![
            create_object_meta(format!("test/{}.parquet", Uuid::new_v4())),
            create_object_meta("test/test.parquet".to_owned()),
        ];

        let compressed_files = CompressedFile::object_metas_to_compressed_file_names(&object_metas);
        assert!(compressed_files.is_err());
    }

    fn create_object_meta(path: String) -> ObjectMeta {
        ObjectMeta {
            location: ObjectStorePath::from(path),
            last_modified: Utc::now(),
            size: 0,
            e_tag: None,
        }
    }

    // Tests for conversion methods.
    #[test]
    fn test_compressed_file_from_record_batch() {
        let uuid = Uuid::new_v4();
        let compressed_file = CompressedFile::from_record_batch(
            uuid,
            "".into(),
            0,
            &common_test::compressed_segments_record_batch(),
        );

        assert_eq!(compressed_file.name, uuid);
        assert_eq!(compressed_file.start_time, 0);
        assert_eq!(compressed_file.end_time, 5);
        assert_eq!(compressed_file.min_value, 5.2);
        assert_eq!(compressed_file.max_value, 34.2);
    }

    #[test]
    fn test_object_meta_from_compressed_file() {
        let uuid = Uuid::new_v4();
        let compressed_file = CompressedFile::from_record_batch(
            uuid,
            "test".into(),
            0,
            &common_test::compressed_segments_record_batch(),
        );

        let object_meta: ObjectMeta = compressed_file.into();
        assert_eq!(
            object_meta.location,
            ObjectStorePath::from(format!("test/{uuid}.parquet"))
        );
        assert_eq!(
            object_meta.last_modified.round_subsecs(0),
            Utc::now().round_subsecs(0)
        );
        assert_eq!(object_meta.size, 0);
    }
}
