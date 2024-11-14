/* Copyright 2024 The ModelarDB Contributors
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

//! Utility functions to read and write Apache Parquet files to and from an object store.

mod error;
mod delta_lake;

use std::result::Result as StdResult;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, UInt64Array};
use arrow::compute;
use arrow::datatypes::DataType;
use datafusion::parquet::arrow::async_reader::{
    AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStream,
};
use datafusion::parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::format::SortingColumn;
use futures::StreamExt;
use modelardb_types::schemas::{
    DISK_COMPRESSED_SCHEMA, QUERY_COMPRESSED_SCHEMA,
};
use object_store::path::Path;
use object_store::ObjectStore;
use tonic::codegen::Bytes;

use crate::error::Result;

/// The folder storing compressed data in the data folders.
const COMPRESSED_DATA_FOLDER: &str = "tables";

/// The folder storing metadata in the data folders.
const METADATA_FOLDER: &str = "metadata";

/// Reinterpret the bits used for univariate ids in `compressed_segments` to convert the column from
/// [`UInt64Array`] to [`Int64Array`] if the column is currently [`UInt64Array`], as the Delta Lake
/// Protocol does not support unsigned integers. `compressed_segments` is modified in-place as
/// `maybe_univariate_ids_uint64_to_int64()` is designed to be used by
/// `write_compressed_segments_to_model_table()` which owns `compressed_segments`.
pub(crate) fn maybe_univariate_ids_uint64_to_int64(compressed_segments: &mut Vec<RecordBatch>) {
    for record_batch in compressed_segments {
        // Only convert the univariate ids if they are stored as unsigned integers. The univariate
        // ids can be stored as signed integers already if the compressed segments have been saved
        // to disk previously.
        if record_batch.schema().field(0).data_type() == &DataType::UInt64 {
            let mut columns = record_batch.columns().to_vec();
            let univariate_ids = modelardb_types::array!(record_batch, 0, UInt64Array);
            let signed_univariate_ids: Int64Array =
                univariate_ids.unary(|value| i64::from_ne_bytes(value.to_ne_bytes()));
            columns[0] = Arc::new(signed_univariate_ids);

            // unwrap() is safe as columns is constructed to match DISK_COMPRESSED_SCHEMA.
            *record_batch =
                RecordBatch::try_new(DISK_COMPRESSED_SCHEMA.0.clone(), columns).unwrap();
        }
    }
}

/// Reinterpret the bits used for univariate ids in `compressed_segments` to convert the column from
/// [`Int64Array`] to [`UInt64Array`] as the Delta Lake Protocol does not support unsigned integers.
/// Returns a new [`RecordBatch`] with the univariate ids stored in an [`UInt64Array`] as
/// `univariate_ids_int64_to_uint64()` is designed to be used by
/// [`futures::stream::Stream::poll_next()`] and
/// [`datafusion::physical_plan::PhysicalExpr::evaluate()`] and
/// [`datafusion::physical_plan::PhysicalExpr::evaluate()`] borrows `compressed_segments` immutably.
pub fn univariate_ids_int64_to_uint64(compressed_segments: &RecordBatch) -> RecordBatch {
    let mut columns = compressed_segments.columns().to_vec();
    let signed_univariate_ids = modelardb_types::array!(compressed_segments, 0, Int64Array);
    let univariate_ids: UInt64Array =
        signed_univariate_ids.unary(|value| u64::from_ne_bytes(value.to_ne_bytes()));
    columns[0] = Arc::new(univariate_ids);

    // unwrap() is safe as columns is constructed to match QUERY_COMPRESSED_SCHEMA.
    RecordBatch::try_new(QUERY_COMPRESSED_SCHEMA.0.clone(), columns).unwrap()
}

/// Read all rows from the Apache Parquet file at the location given by `file_path` in
/// `object_store` and return them as a [`RecordBatch`]. If the file could not be read successfully,
/// [`ModelarDbStorageError`](error::ModelarDbStorageError) is returned.
pub async fn read_record_batch_from_apache_parquet_file(
    file_path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<RecordBatch> {
    // Create an object reader for the Apache Parquet file.
    let file_metadata = object_store
        .head(file_path)
        .await
        .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

    let reader = ParquetObjectReader::new(object_store, file_metadata);

    // Stream the data from the Apache Parquet file into a single record batch.
    let record_batches = read_batches_from_apache_parquet_file(reader).await?;

    let schema = record_batches[0].schema();
    compute::concat_batches(&schema, &record_batches).map_err(|error| error.into())
}

/// Read each batch of data from the Apache Parquet file given by `reader` and return them as a
/// [`Vec`] of [`RecordBatch`]. If the file could not be read successfully, [`ParquetError`] is
/// returned.
pub async fn read_batches_from_apache_parquet_file<R>(reader: R) -> Result<Vec<RecordBatch>>
where
    R: AsyncFileReader + Send + Unpin + 'static,
    ParquetRecordBatchStream<R>: StreamExt<Item = StdResult<RecordBatch, ParquetError>>,
{
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let mut stream = builder.build()?;

    let mut record_batches = Vec::new();
    while let Some(maybe_record_batch) = stream.next().await {
        let record_batch = maybe_record_batch?;
        record_batches.push(record_batch);
    }

    Ok(record_batches)
}

/// Write the rows in `record_batch` to an Apache Parquet file at the location given by `file_path`
/// in `object_store`. `file_path` must use the extension `.parquet`. `sorting_columns` can be set
/// to control the sorting order of the rows in the written file. Return [`Ok`] if the file was
/// written successfully, otherwise return [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub async fn write_record_batch_to_apache_parquet_file(
    file_path: &Path,
    record_batch: &RecordBatch,
    sorting_columns: Option<Vec<SortingColumn>>,
    object_store: &dyn ObjectStore,
) -> Result<()> {
    // Check if the extension of the given path is correct.
    if file_path.extension() == Some("parquet") {
        let props = apache_parquet_writer_properties(sorting_columns);

        // Write the record batch to the object store.
        let mut buffer = Vec::new();
        let mut writer =
            AsyncArrowWriter::try_new(&mut buffer, record_batch.schema(), Some(props))?;
        writer.write(record_batch).await?;
        writer.close().await?;

        object_store
            .put(file_path, Bytes::from(buffer).into())
            .await
            .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

        Ok(())
    } else {
        Err(ParquetError::General(format!(
            "'{}' is not a valid file path for an Apache Parquet file.",
            file_path.as_ref()
        )))?
    }
}

/// Return [`WriterProperties`] optimized for compressed segments for Apache Parquet and Delta Lake.
fn apache_parquet_writer_properties(
    sorting_columns: Option<Vec<SortingColumn>>,
) -> WriterProperties {
    WriterProperties::builder()
        .set_data_page_size_limit(16384)
        .set_max_row_group_size(65536)
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .set_sorting_columns(sorting_columns)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use modelardb_common::test;
    use object_store::local::LocalFileSystem;
    use proptest::num::u64 as ProptestUnivariateId;
    use proptest::{prop_assert_eq, proptest};
    use tempfile::TempDir;

    // Tests for maybe_univariate_ids_uint64_to_int64() and univariate_ids_int64_to_uint64().
    proptest! {
    #[test]
    fn test_univariate_ids_uint64_to_int64_to_uint64(univariate_id in ProptestUnivariateId::ANY) {
        let record_batch = test::compressed_segments_record_batch_with_time(univariate_id, 0, 0.0);
        let mut expected_record_batch = record_batch.clone();
        expected_record_batch.remove_column(10);

        let mut record_batches = vec![record_batch.clone()];
        maybe_univariate_ids_uint64_to_int64(&mut record_batches);

        // maybe_univariate_ids_uint64_to_int64 should not panic when called twice.
        maybe_univariate_ids_uint64_to_int64(&mut record_batches);

        record_batches[0].remove_column(10);
        let computed_record_batch = univariate_ids_int64_to_uint64(&record_batches[0]);

        prop_assert_eq!(expected_record_batch, computed_record_batch);
    }
    }

    // Tests for read_record_batch_from_apache_parquet_file().
    #[tokio::test]
    async fn test_read_record_batch_from_apache_parquet_file() {
        let record_batch = test::compressed_segments_record_batch();
        let apache_parquet_path = Path::from("test.parquet");

        let (temp_dir, _result) =
            write_record_batch_to_temp_dir(&apache_parquet_path, &record_batch).await;

        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result =
            read_record_batch_from_apache_parquet_file(&apache_parquet_path, object_store).await;

        assert!(result.is_ok());
        assert_eq!(record_batch, result.unwrap());
    }

    #[tokio::test]
    async fn test_read_record_batch_from_non_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        let path = Path::from("test.txt");
        object_store
            .put(&path, Bytes::from(Vec::new()).into())
            .await
            .unwrap();

        let result = read_record_batch_from_apache_parquet_file(&path, object_store);
        assert!(result.await.is_err());
    }

    #[tokio::test]
    async fn test_read_record_batch_from_non_existent_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        let path = Path::from("test.parquet");

        let result = read_record_batch_from_apache_parquet_file(&path, object_store);
        assert!(result.await.is_err());
    }

    // Tests for write_record_batch_to_apache_parquet_file().
    #[tokio::test]
    async fn test_write_record_batch_to_apache_parquet_file() {
        let record_batch = test::compressed_segments_record_batch();
        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test.parquet"), &record_batch).await;

        assert!(result.is_ok());
        assert!(temp_dir.path().join("test.parquet").exists());
    }

    #[tokio::test]
    async fn test_write_empty_record_batch_to_apache_parquet_file() {
        let fields: Vec<Field> = vec![];
        let schema = Schema::new(fields);
        let record_batch = RecordBatch::new_empty(Arc::new(schema));

        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test.parquet"), &record_batch).await;

        assert!(result.is_ok());
        assert!(temp_dir.path().join("test.parquet").exists());
    }

    #[tokio::test]
    async fn test_write_record_batch_to_file_path_with_invalid_extension() {
        let record_batch = test::compressed_segments_record_batch();
        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test.txt"), &record_batch).await;

        assert!(result.is_err());
        assert!(!temp_dir.path().join("test.txt").exists());
    }

    #[tokio::test]
    async fn test_write_record_batch_to_file_path_without_extension() {
        let record_batch = test::compressed_segments_record_batch();
        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test"), &record_batch).await;

        assert!(result.is_err());
        assert!(!temp_dir.path().join("test").exists());
    }

    async fn write_record_batch_to_temp_dir(
        file_path: &Path,
        record_batch: &RecordBatch,
    ) -> (TempDir, Result<()>) {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

        let result =
            write_record_batch_to_apache_parquet_file(file_path, record_batch, None, &object_store)
                .await;

        (temp_dir, result)
    }
}
