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

//! Utility functions to support reading and writing Apache Parquet files to disk.

use std::ffi::OsStr;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use arrow::array::RecordBatch;
use arrow::compute;
use arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use datafusion::parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::format::SortingColumn;
use futures::StreamExt;
use tokio::fs::File as TokioFile;

/// Write `batch` to an Apache Parquet file at the location given by `file_path`. `file_path`
/// must use the extension '.parquet'. Return [`Ok`] if the file was written successfully,
/// otherwise [`ParquetError`].
pub fn write_batch_to_apache_parquet_file(
    batch: &RecordBatch,
    file_path: &Path,
    sorting_columns: Option<Vec<SortingColumn>>,
) -> Result<(), ParquetError> {
    let error = ParquetError::General(format!(
        "Apache Parquet file at path '{}' could not be created.",
        file_path.display()
    ));

    // Check if the extension of the given path is correct.
    if file_path.extension().and_then(OsStr::to_str) == Some("parquet") {
        let file = File::create(file_path).map_err(|_e| error)?;
        let mut writer = create_apache_arrow_writer(file, batch.schema(), sorting_columns)?;
        writer.write(batch)?;
        writer.close()?;

        Ok(())
    } else {
        Err(error)
    }
}

/// Read all rows from the Apache Parquet file at the location given by `file_path` and return
/// them as a [`RecordBatch`]. If the file could not be read successfully, [`ParquetError`] is
/// returned.
pub async fn read_batch_from_apache_parquet_file(
    file_path: &Path,
) -> Result<RecordBatch, ParquetError> {
    // Create a stream that can be used to read an Apache Parquet file.
    let file = TokioFile::open(file_path)
        .await
        .map_err(|error| ParquetError::General(error.to_string()))?;

    let record_batches = read_batches_from_apache_parquet_file(file).await?;
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

/// Create an Apache [`ArrowWriter`] that writes to `writer` with a configuration that is optimized
/// for writing compressed segments. If the writer could not be created return [`ParquetError`].
pub fn create_apache_arrow_writer<W: Write + Send>(
    writer: W,
    schema: SchemaRef,
    sorting_columns: Option<Vec<SortingColumn>>,
) -> Result<ArrowWriter<W>, ParquetError> {
    let props = WriterProperties::builder()
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .set_sorting_columns(sorting_columns)
        .build();

    let writer = ArrowWriter::try_new(writer, schema, Some(props))?;
    Ok(writer)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use datafusion::arrow::datatypes::{Field, Schema};
    use tempfile;

    use crate::test;

    // Tests for writing and reading Apache Parquet files.
    #[test]
    fn test_write_batch_to_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let batch = test::compressed_segments_record_batch();

        let apache_parquet_path = temp_dir.path().join("test.parquet");
        write_batch_to_apache_parquet_file(&batch, apache_parquet_path.as_path(), None).unwrap();

        assert!(apache_parquet_path.exists());
    }

    #[test]
    fn test_write_empty_batch_to_apache_parquet_file() {
        let fields: Vec<Field> = vec![];
        let schema = Schema::new(fields);
        let batch = RecordBatch::new_empty(Arc::new(schema));

        let temp_dir = tempfile::tempdir().unwrap();
        let apache_parquet_path = temp_dir.path().join("empty.parquet");
        write_batch_to_apache_parquet_file(&batch, apache_parquet_path.as_path(), None).unwrap();

        assert!(apache_parquet_path.exists());
    }

    #[test]
    fn test_write_batch_to_file_with_invalid_extension() {
        write_to_file_and_assert_failed("test.txt".to_owned());
    }

    #[test]
    fn test_write_batch_to_file_with_no_extension() {
        write_to_file_and_assert_failed("test".to_owned());
    }

    fn write_to_file_and_assert_failed(file_name: String) {
        let temp_dir = tempfile::tempdir().unwrap();
        let batch = test::compressed_segments_record_batch();

        let apache_parquet_path = temp_dir.path().join(file_name);
        let result =
            write_batch_to_apache_parquet_file(&batch, apache_parquet_path.as_path(), None);

        assert!(result.is_err());
        assert!(!apache_parquet_path.exists());
    }

    #[tokio::test]
    async fn test_read_apache_parquet_file_with_one_row_group() {
        let temp_dir = tempfile::tempdir().unwrap();
        let batch = test::compressed_segments_record_batch();

        let path = temp_dir.path().join("test.parquet");
        write_batch_to_apache_parquet_file(&batch, path.as_path(), None).unwrap();

        let result = read_batch_from_apache_parquet_file(path.as_path())
            .await
            .unwrap();

        assert_eq!(batch, result);
        assert_eq!(3, result.num_rows());
    }

    #[tokio::test]
    async fn test_read_apache_parquet_file_with_multiple_row_groups() {
        let temp_dir = tempfile::tempdir().unwrap();
        let batch = test::compressed_segments_record_batch();

        let path = temp_dir.path().join("test.parquet");
        let file = File::create(&path).unwrap();

        let props = WriterProperties::builder()
            .set_max_row_group_size(1)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let result = read_batch_from_apache_parquet_file(path.as_path())
            .await
            .unwrap();

        assert_eq!(batch, result);
        assert_eq!(3, result.num_rows());
    }

    #[tokio::test]
    async fn test_read_from_non_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.txt");
        File::create(path.clone()).unwrap();

        let result = read_batch_from_apache_parquet_file(path.as_path());

        assert!(result.await.is_err());
    }

    #[tokio::test]
    async fn test_read_from_non_existent_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("none.parquet");
        let result = read_batch_from_apache_parquet_file(path.as_path());

        assert!(result.await.is_err());
    }
}
