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

use std::io::Write;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use datafusion::parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::format::SortingColumn;
use futures::StreamExt;

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
