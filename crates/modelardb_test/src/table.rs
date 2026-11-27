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

//! Implementation of table-related functions and constants used throughout ModelarDB for testing
//! purposes.

use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
use async_trait::async_trait;
use datafusion::common::ToDFSchema;
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion::prelude::col;
use modelardb_types::types::{
    ArrowTimestamp, ArrowValue, ColumnMetadata, ErrorBound, GeneratedColumn, TableMetadata,
    Timestamp, TimestampArray, Value, ValueArray,
};

/// SQL to create a table with a timestamp column, a column with an absolute error bound, a
/// generated column, and a column with a relative error bound. The table has the metadata returned
/// by [`table_metadata()`].
pub const TABLE_SQL: &str = "CREATE TABLE test_table(timestamp TIMESTAMP, \
    absolute_value REAL ERROR BOUND 1, \
    generated_value REAL AS (absolute_value + relative_value), \
    relative_value REAL ERROR BOUND 5%)";

/// Name of the table used in tests.
pub const TABLE_NAME: &str = "test_table";

/// [`DataSink`] implementation that does nothing.
pub struct NoOpDataSink {}

#[async_trait]
impl DataSink for NoOpDataSink {
    fn metrics(&self) -> Option<MetricsSet> {
        unimplemented!();
    }

    fn schema(&self) -> &Arc<Schema> {
        unimplemented!();
    }

    async fn write_all(
        &self,
        _data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        unimplemented!();
    }
}

impl Debug for NoOpDataSink {
    fn fmt(&self, _f: &mut Formatter<'_>) -> FmtResult {
        unimplemented!();
    }
}

impl DisplayAs for NoOpDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter<'_>) -> FmtResult {
        unimplemented!();
    }
}

/// Return protobuf message bytes containing the metadata of the table created by [`TABLE_SQL`].
pub fn table_metadata_protobuf_bytes() -> Vec<u8> {
    modelardb_types::flight::encode_and_serialize_table_metadata(&table_metadata()).unwrap()
}

/// Return the [`TableMetadata`] of the table created by [`TABLE_SQL`].
pub fn table_metadata() -> TableMetadata {
    let query_schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", ArrowTimestamp::DATA_TYPE, true),
        Field::new("absolute_value", ArrowValue::DATA_TYPE, true),
        Field::new("generated_value", ArrowValue::DATA_TYPE, true),
        Field::new("relative_value", ArrowValue::DATA_TYPE, true),
    ]));

    let df_query_schema = query_schema.clone().to_dfschema().unwrap();
    let generated_column = GeneratedColumn::try_from_expr(
        col("absolute_value") + col("relative_value"),
        &df_query_schema,
    )
    .unwrap();

    let column_metadata = vec![
        ColumnMetadata::Stored(ErrorBound::Lossless),
        ColumnMetadata::Stored(ErrorBound::try_new_absolute(1.0).unwrap()),
        ColumnMetadata::Generated(generated_column),
        ColumnMetadata::Stored(ErrorBound::try_new_relative(5.0).unwrap()),
    ];

    TableMetadata::try_new(TABLE_NAME.to_owned(), query_schema, column_metadata).unwrap()
}

/// Return a [`RecordBatch`] with `row_count` rows in the stored schema of the table created by
/// [`TABLE_SQL`]. The timestamps and the values in each row are the index of the row.
pub fn table_record_batch(row_count: usize) -> RecordBatch {
    let timestamps: Vec<Timestamp> = (0..row_count)
        .map(|timestamp| timestamp as Timestamp)
        .collect();
    let values: Vec<Value> = (0..row_count).map(|value| value as Value).collect();

    RecordBatch::try_new(
        table_metadata().stored_schema,
        vec![
            Arc::new(TimestampArray::from(timestamps)),
            Arc::new(ValueArray::from(values.clone())),
            Arc::new(ValueArray::from(values)),
        ],
    )
    .unwrap()
}
