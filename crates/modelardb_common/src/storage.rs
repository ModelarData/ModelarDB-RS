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

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute;
use datafusion::parquet::arrow::async_reader::{
    AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStream,
};
use datafusion::parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::format::SortingColumn;
use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use tonic::codegen::Bytes;
use uuid::Uuid;

use crate::schemas::COMPRESSED_SCHEMA;

/// Read all rows from the Apache Parquet file at the location given by `file_path` in
/// `object_store` and return them as a [`RecordBatch`]. If the file could not be read successfully,
/// [`ParquetError`] is returned.
pub async fn read_record_batch_from_apache_parquet_file(
    file_path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<RecordBatch, ParquetError> {
    // Create an object reader for the Apache Parquet file.
    let file_metadata = object_store
        .head(file_path)
        .await
        .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

    let reader = ParquetObjectReader::new(object_store, file_metadata);

    // Stream the data from the Apache Parquet file into a single record batch.
    let record_batches = read_batches_from_apache_parquet_file(reader).await?;

    let schema = record_batches[0].schema();
    compute::concat_batches(&schema, &record_batches)
        .map_err(|error| ParquetError::General(error.to_string()))
}

/// Read each batch of data from the Apache Parquet file given by `reader` and return them as a
/// [`Vec`] of [`RecordBatch`]. If the file could not be read successfully, [`ParquetError`] is
/// returned.
pub async fn read_batches_from_apache_parquet_file<R>(
    reader: R,
) -> Result<Vec<RecordBatch>, ParquetError>
where
    R: AsyncFileReader + Send + Unpin + 'static,
    ParquetRecordBatchStream<R>: StreamExt<Item = Result<RecordBatch, ParquetError>>,
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
/// in `object_store`. `file_path` must use the extension `.parquet`. `sorting_columns` can be
/// set to control the sorting order of the rows in the written file. Return [`Ok`] if the file
/// was written successfully, otherwise return [`ParquetError`].
pub async fn write_record_batch_to_apache_parquet_file(
    file_path: &Path,
    record_batch: &RecordBatch,
    sorting_columns: Option<Vec<SortingColumn>>,
    object_store: &dyn ObjectStore,
) -> Result<(), ParquetError> {
    // Check if the extension of the given path is correct.
    if file_path.extension() == Some("parquet") {
        let props = WriterProperties::builder()
            .set_data_page_size_limit(16384)
            .set_max_row_group_size(65536)
            .set_encoding(Encoding::PLAIN)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_bloom_filter_enabled(false)
            .set_sorting_columns(sorting_columns)
            .build();

        // Write the record batch to the object store.
        let mut buffer = Vec::new();
        let mut writer =
            AsyncArrowWriter::try_new(&mut buffer, record_batch.schema(), 0, Some(props))?;
        writer.write(record_batch).await?;
        writer.close().await?;

        object_store
            .put(file_path, Bytes::from(buffer))
            .await
            .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

        Ok(())
    } else {
        Err(ParquetError::General(format!(
            "'{}' is not a valid file path for an Apache Parquet file.",
            file_path.as_ref()
        )))
    }
}

/// Write `compressed_segments` to an Apache Parquet file with a unique file name in `folder_path`.
/// Return the path to the file if the file was written successfully, otherwise return [`ParquetError`].
pub async fn write_compressed_segments_to_apache_parquet_file(
    folder_path: &Path,
    compressed_segments: &RecordBatch,
    object_store: &dyn ObjectStore,
) -> Result<Path, ParquetError> {
    if compressed_segments.schema() == COMPRESSED_SCHEMA.0 {
        // Use a UUID for the file name to ensure the name is unique.
        let uuid = Uuid::new_v4();
        let output_file_path = Path::from(format!("{folder_path}/{uuid}.parquet"));

        // Specify that the file must be sorted by univariate_id and then by start_time.
        let sorting_columns = Some(vec![
            SortingColumn::new(0, false, false),
            SortingColumn::new(2, false, false),
        ]);

        write_record_batch_to_apache_parquet_file(
            &output_file_path,
            compressed_segments,
            sorting_columns,
            object_store,
        )
        .await?;

        Ok(output_file_path)
    } else {
        Err(ParquetError::General(
            "The data in the record batch is not compressed segments.".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::test;

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

        let path_buf = temp_dir.path().join("test.txt");
        File::create(path_buf.clone()).unwrap();

        let path = Path::from("test.txt");
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
    ) -> (TempDir, Result<(), ParquetError>) {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

        let result =
            write_record_batch_to_apache_parquet_file(file_path, record_batch, None, &object_store)
                .await;

        (temp_dir, result)
    }

    // Tests for write_compressed_segments_to_apache_parquet_file().
    #[tokio::test]
    async fn test_write_compressed_segments_to_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
        let compressed_segments = test::compressed_segments_record_batch();

        let result_1 = write_compressed_segments_to_apache_parquet_file(
            &Path::from("compressed"),
            &compressed_segments,
            &object_store,
        )
        .await;

        // Write the compressed segments to the same folder again to ensure the created file name is unique.
        let result_2 = write_compressed_segments_to_apache_parquet_file(
            &Path::from("compressed"),
            &compressed_segments,
            &object_store,
        )
        .await;

        let result_1_path = result_1.unwrap();
        let result_2_path = result_2.unwrap();

        assert_ne!(result_1_path, result_2_path);
        assert!(temp_dir.path().join(result_1_path.to_string()).exists());
        assert!(temp_dir.path().join(result_2_path.to_string()).exists());
    }

    #[tokio::test]
    async fn test_write_non_compressed_segments_to_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

        let fields: Vec<Field> = vec![];
        let schema = Schema::new(fields);
        let record_batch = RecordBatch::new_empty(Arc::new(schema));

        let result = write_compressed_segments_to_apache_parquet_file(
            &Path::from("compressed"),
            &record_batch,
            &object_store,
        )
        .await;

        assert!(result.is_err());
    }
}
