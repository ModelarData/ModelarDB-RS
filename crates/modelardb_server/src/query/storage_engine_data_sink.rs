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

//! Implementation of a [`DataSink`] that writes
//! [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) to [`StorageEngine`].

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use tokio::sync::RwLock;

use crate::storage::StorageEngine;

/// [`DataSink`] that writes [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) to
/// [`StorageEngine`]. Assume the generated columns are included, thus they are dropped without
/// checking the schema.
pub struct StorageEngineDataSink {
    storage_engine: Arc<RwLock<StorageEngine>>,
    model_table_metadata: Arc<ModelTableMetadata>,
}

impl StorageEngineDataSink {
    pub fn new(
        storage_engine: Arc<RwLock<StorageEngine>>,
        model_table_metadata: Arc<ModelTableMetadata>,
    ) -> Self {
        Self {
            storage_engine,
            model_table_metadata,
        }
    }
}

#[async_trait]
impl DataSink for StorageEngineDataSink {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return a snapshot of the set of metrics being collected by the [`DataSink`].
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut data_points_inserted: u64 = 0;

        while let Some(record_batch) = data.next().await {
            // unwrap() is safe as ModelTable verifies the RecordBatches matches query schema.
            let record_batch = record_batch?
                .project(&self.model_table_metadata.query_schema_to_schema)
                .unwrap();

            data_points_inserted += record_batch.num_rows() as u64;

            let mut storage_engine = self.storage_engine.write().await;
            storage_engine
                .insert_data_points(self.model_table_metadata.clone(), record_batch)
                .await
                .map_err(DataFusionError::Execution)?;
        }

        Ok(data_points_inserted)
    }
}

impl Debug for StorageEngineDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "StorageEngineDataSink")
    }
}

impl DisplayAs for StorageEngineDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "StorageEngineDataSink")
    }
}
