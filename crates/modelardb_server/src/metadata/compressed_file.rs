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

//! Support for handling file metadata for files that contain compressed segments.

use datafusion::arrow::compute::kernels::aggregate;
use datafusion::arrow::record_batch::RecordBatch;
use modelardb_common::types::{Timestamp, TimestampArray, Value, ValueArray};
use object_store::ObjectMeta;

/// Metadata about a file tracked by [`MetadataManager`](crate::metadata::MetadataManager) which
/// contains compressed segments.
#[derive(Debug, Clone, PartialEq)]
pub struct CompressedFile {
    /// The metadata that describes the file.
    pub(crate) file_metadata: ObjectMeta,
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
    /// Convert the given [`ObjectMeta`] and [`RecordBatch`] to a [`CompressedFile`].
    pub(crate) fn from_record_batch(file_metadata: ObjectMeta, batch: &RecordBatch) -> Self {
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
            file_metadata,
            start_time,
            end_time,
            min_value,
            max_value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Utc;
    use object_store::path::Path as ObjectStorePath;
    use uuid::Uuid;

    use crate::common_test;

    #[test]
    fn test_compressed_file_from_record_batch() {
        let uuid = Uuid::new_v4();
        let object_meta = ObjectMeta {
            location: ObjectStorePath::from(format!("test/{uuid}.parquet")),
            size: 0,
            last_modified: Utc::now(),
            e_tag: None,
        };

        let compressed_file = CompressedFile::from_record_batch(
            object_meta,
            &common_test::compressed_segments_record_batch(),
        );

        assert_eq!(compressed_file.start_time, 0);
        assert_eq!(compressed_file.end_time, 5);
        assert_eq!(compressed_file.min_value, 5.2);
        assert_eq!(compressed_file.max_value, 34.2);
    }
}
