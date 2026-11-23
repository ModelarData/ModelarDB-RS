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

//! The policy that selects how data is encoded and compressed in Apache Parquet files.

use arrow::datatypes::{DataType, Schema};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

/// Return the [`WriterProperties`] used for every Apache Parquet file with data that has `schema`.
/// The data is compressed with ZSTD, and the encoding of each column is selected by its data type.
pub fn writer_properties(schema: &Schema) -> WriterProperties {
    // A column's encoding is only used when dictionary encoding is disabled for the column, so it
    // is disabled by default and only enabled for strings since they often repeat.
    let mut writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_dictionary_enabled(false);

    // Specify encodings for types where specific encodings generally are known to perform well.
    for field in schema.fields() {
        let path = ColumnPath::from(field.name().as_str());

        writer_properties = match field.data_type() {
            DataType::Timestamp(_, _) => {
                writer_properties.set_column_encoding(path, Encoding::DELTA_BINARY_PACKED)
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                writer_properties.set_column_encoding(path, Encoding::BYTE_STREAM_SPLIT)
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                writer_properties.set_column_dictionary_enabled(path, true)
            }
            _ => writer_properties,
        };
    }

    writer_properties.build()
}
