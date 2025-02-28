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

//! Implementation of [`DataSinks`](`DataSink`) that writes [`RecordBatches`](RecordBatch) to
//! [`StorageEngine`].

use std::any::Any;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use modelardb_storage::metadata::model_table_metadata::ModelTableMetadata;
use tokio::sync::RwLock;

use crate::storage::StorageEngine;

/// [`DataSink`] that writes [`RecordBatches`](RecordBatch) to [`StorageEngine`]. Use
/// [`ModelTableDataSink`] for writing multivariate time series to [`StorageEngine`].
pub struct NormalTableDataSink {
    /// The name of the normal table inserted data will be written to.
    table_name: String,
    /// Manages all uncompressed and compressed data in the system.
    storage_engine: Arc<RwLock<StorageEngine>>,
}

impl NormalTableDataSink {
    pub fn new(table_name: String, storage_engine: Arc<RwLock<StorageEngine>>) -> Self {
        Self {
            table_name,
            storage_engine,
        }
    }
}

#[async_trait]
impl DataSink for NormalTableDataSink {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return a snapshot of the set of metrics being collected by the [`DataSink`].
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Write all rows in `data` to [`StorageEngine`]. Returns the number of rows that have been
    /// written or a [`DataFusionError`] if the rows could not be inserted.
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let mut rows_inserted: u64 = 0;

        while let Some(record_batch) = data.next().await {
            let record_batch = record_batch?;
            rows_inserted += record_batch.num_rows() as u64;

            let storage_engine = self.storage_engine.read().await;
            storage_engine
                .insert_record_batch(&self.table_name, record_batch)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        }

        Ok(rows_inserted)
    }
}

impl Debug for NormalTableDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let name = &self.table_name;
        write!(f, "NormalTableDataSink for {name}")
    }
}

impl DisplayAs for NormalTableDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> FmtResult {
        let name = &self.table_name;
        write!(f, "NormalTableDataSink for {name}")
    }
}

/// [`DataSink`] that writes [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch)
/// containing multivariate time series to [`StorageEngine`]. Assumes the generated columns are
/// included, thus they are dropped without checking the schema.
pub struct ModelTableDataSink {
    /// Metadata for the model table inserted data will be written to.
    model_table_metadata: Arc<ModelTableMetadata>,
    /// Manages all uncompressed and compressed data in the system.
    storage_engine: Arc<RwLock<StorageEngine>>,
}

impl ModelTableDataSink {
    pub fn new(
        model_table_metadata: Arc<ModelTableMetadata>,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Self {
        Self {
            model_table_metadata,
            storage_engine,
        }
    }
}

#[async_trait]
impl DataSink for ModelTableDataSink {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return a snapshot of the set of metrics being collected by the [`DataSink`].
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Write all data points in `data` to [`StorageEngine`]. Returns the number of data points that
    /// has been written or a [`DataFusionError`] if the data points could not be inserted.
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let mut data_points_inserted: u64 = 0;

        while let Some(record_batch) = data.next().await {
            let record_batch =
                record_batch?.project(&self.model_table_metadata.query_schema_to_schema)?;

            // Ensure the fields are not nullable. It is not possible to insert null values into
            // model tables but the schema of the record batch may contain nullable fields.
            let mut fields: Vec<Field> = Vec::with_capacity(record_batch.schema().fields.len());
            for field in record_batch.schema().fields() {
                fields.push(Field::new(field.name(), field.data_type().clone(), false));
            }

            let record_batch = RecordBatch::try_new(
                Arc::new(Schema::new(fields)),
                record_batch.columns().to_vec(),
            )?;

            data_points_inserted += record_batch.num_rows() as u64;

            let mut storage_engine = self.storage_engine.write().await;
            storage_engine
                .insert_data_points(self.model_table_metadata.clone(), record_batch)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        }

        Ok(data_points_inserted)
    }
}

impl Debug for ModelTableDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let name = &self.model_table_metadata.name;
        write!(f, "ModelTableDataSink for {name}")
    }
}

impl DisplayAs for ModelTableDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> FmtResult {
        let name = &self.model_table_metadata.name;
        write!(f, "ModelTableDataSink for {name}")
    }
}
