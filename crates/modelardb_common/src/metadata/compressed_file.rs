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

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray, UInt64Array};
use datafusion::arrow::compute::kernels::aggregate;
use datafusion::arrow::record_batch::RecordBatch;
use object_store::ObjectMeta;

use crate::array;
use crate::schemas::COMPRESSED_FILE_METADATA_SCHEMA;
use crate::types::{Timestamp, TimestampArray, Value, ValueArray};

/// Metadata about a file tracked by [`TableMetadataManager`](crate::metadata::TableMetadataManager)
/// which contains compressed segments.
#[derive(Debug, Clone, PartialEq)]
pub struct CompressedFile {
    /// The metadata that describes the file.
    pub file_metadata: ObjectMeta,
    // Index of the field in the query schema.
    pub query_schema_index: u16,
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
        file_metadata: ObjectMeta,
        query_schema_index: u16,
        start_time: Timestamp,
        end_time: Timestamp,
        min_value: Value,
        max_value: Value,
    ) -> Self {
        CompressedFile {
            file_metadata,
            query_schema_index,
            start_time,
            end_time,
            min_value,
            max_value,
        }
    }

    /// Convert the given [`ObjectMeta`] and [`RecordBatch`] to a [`CompressedFile`].
    pub fn from_compressed_data(
        file_metadata: ObjectMeta,
        query_schema_index: u16,
        batch: &RecordBatch,
    ) -> Self {
        // unwrap() is safe as None is only returned if all the values are None.
        let start_time = aggregate::min(array!(batch, 2, TimestampArray)).unwrap();
        let end_time = aggregate::max(array!(batch, 3, TimestampArray)).unwrap();

        // unwrap() is safe as None is only returned if all the values are None.
        // Both aggregate::min() and aggregate::max() consider NaN to be greater than other non-null
        // values. So since min_values and max_values cannot contain null, min_value will be NaN if all
        // values in min_values are NaN while max_value will be NaN if any value in max_values is NaN.
        let min_value = aggregate::min(array!(batch, 5, ValueArray)).unwrap();
        let max_value = aggregate::max(array!(batch, 6, ValueArray)).unwrap();

        Self {
            file_metadata,
            query_schema_index,
            start_time,
            end_time,
            min_value,
            max_value,
        }
    }

    /// Create a [`RecordBatch`] with a single row containing `table_name`, `column_index`, and the
    /// compressed file.
    pub fn to_record_batch(&self, table_name: &str, column_index: usize) -> RecordBatch {
        let schema = COMPRESSED_FILE_METADATA_SCHEMA.clone();
        let file_path = self.file_metadata.location.to_string();
        let created_at = self.file_metadata.last_modified.timestamp_millis();

        // unwrap() is safe since the columns match the schema and all columns are of the same length.
        RecordBatch::try_new(
            schema.0,
            vec![
                Arc::new(StringArray::from(vec![table_name])),
                Arc::new(UInt64Array::from(vec![column_index as u64])),
                Arc::new(StringArray::from(vec![file_path])),
                Arc::new(UInt64Array::from(vec![self.file_metadata.size as u64])),
                Arc::new(Int64Array::from(vec![created_at])),
                Arc::new(TimestampArray::from(vec![self.start_time])),
                Arc::new(TimestampArray::from(vec![self.end_time])),
                Arc::new(ValueArray::from(vec![self.min_value])),
                Arc::new(ValueArray::from(vec![self.max_value])),
            ],
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Utc;
    use object_store::path::Path;
    use uuid::Uuid;

    use crate::test::compressed_segments_record_batch;

    #[test]
    fn test_compressed_file_from_compressed_data() {
        let uuid = Uuid::new_v4();
        let object_meta = ObjectMeta {
            location: Path::from(format!("test/{uuid}.parquet")),
            size: 0,
            last_modified: Utc::now(),
            e_tag: None,
            version: None,
        };

        let compressed_file = CompressedFile::from_compressed_data(
            object_meta,
            0,
            &compressed_segments_record_batch(),
        );

        assert_eq!(compressed_file.query_schema_index, 0);
        assert_eq!(compressed_file.start_time, 0);
        assert_eq!(compressed_file.end_time, 5);
        assert_eq!(compressed_file.min_value, 5.2);
        assert_eq!(compressed_file.max_value, 34.2);
    }
}
