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

use chrono::{TimeZone, Utc};
use datafusion::arrow::compute::kernels::aggregate;
use datafusion::arrow::record_batch::RecordBatch;
use modelardb_common::types::{Timestamp, TimestampArray, Value, ValueArray};
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectMeta;

/// Metadata about a file tracked by [`MetadataManager`](crate::metadata::MetadataManager) which
/// contains compressed segments.
#[derive(Debug, Clone, PartialEq)]
pub struct CompressedFile {
    /// Path to the file.
    pub(super) file_path: String,
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
        file_path: &str,
        size: usize,
        created_at: Timestamp,
        start_time: Timestamp,
        end_time: Timestamp,
        min_value: Value,
        max_value: Value,
    ) -> Self {
        Self {
            file_path: file_path.to_owned(),
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
        file_path: &str,
        size: usize,
        created_at: Timestamp,
        batch: &RecordBatch,
    ) -> Self {
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
            file_path: file_path.to_owned(),
            size,
            created_at,
            start_time,
            end_time,
            min_value,
            max_value,
        }
    }
}

impl From<CompressedFile> for ObjectMeta {
    fn from(compressed_file: CompressedFile) -> Self {
        // unwrap() is safe as the created_at timestamp cannot be out of range.
        let last_modified = Utc
            .timestamp_millis_opt(compressed_file.created_at)
            .unwrap();

        ObjectMeta {
            location: ObjectStorePath::from(compressed_file.file_path),
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
    use uuid::Uuid;

    use crate::common_test;

    const TABLE_NAME: &str = "model_table";

    // Tests for conversion methods.
    #[test]
    fn test_compressed_file_from_record_batch() {
        let uuid = Uuid::new_v4();
        let compressed_file = CompressedFile::from_record_batch(
            &format!("test/{uuid}.parquet"),
            0,
            0,
            &common_test::compressed_segments_record_batch(),
        );

        assert_eq!(compressed_file.start_time, 0);
        assert_eq!(compressed_file.end_time, 5);
        assert_eq!(compressed_file.min_value, 5.2);
        assert_eq!(compressed_file.max_value, 34.2);
    }

    #[test]
    fn test_object_meta_from_compressed_file() {
        let uuid = Uuid::new_v4();
        let compressed_file = CompressedFile::from_record_batch(
            &format!("test/{uuid}.parquet"),
            0,
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
