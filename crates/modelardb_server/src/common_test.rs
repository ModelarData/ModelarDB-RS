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

//! Implementation of functions, macros, and types used throughout ModelarDB server's tests.

#![cfg(test)]

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::ArrowPrimitiveType;
use datafusion::arrow::array::{BinaryArray, Float32Array, UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::execution::runtime_env::RuntimeEnv;
use modelardb_common::metadata::model_table_metadata::ModelTableMetadata;
use modelardb_common::schemas::COMPRESSED_SCHEMA;
use modelardb_common::types::{
    ArrowTimestamp, ArrowValue, ClusterMode, ErrorBound, TimestampArray, ValueArray,
};
use object_store::local::LocalFileSystem;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

use crate::configuration::ConfigurationManager;
use crate::metadata::MetadataManager;
use crate::storage::{self, StorageEngine};
use crate::{optimizer, Context, ServerMode};

/// Expected size of the compressed data buffers produced in the tests.
pub const UNCOMPRESSED_BUFFER_SIZE: usize = 786432;

/// Expected size of the compressed segments produced in the tests.
pub const COMPRESSED_SEGMENTS_SIZE: usize = 1335;

/// Number of bytes reserved for uncompressed data in tests.
pub const UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES: usize = 5 * 1024 * 1024; // 5 MiB

/// Number of bytes reserved for compressed data in tests.
pub const COMPRESSED_RESERVED_MEMORY_IN_BYTES: usize = 5 * 1024 * 1024; // 5 MiB

/// SQL to create a model table with a timestamp column, two field columns, and a tag column.
pub const MODEL_TABLE_SQL: &str =
    "CREATE MODEL TABLE model_table(timestamp TIMESTAMP, field_1 FIELD, field_2 FIELD, tag TAG)";

/// Return a [`Context`] with a [`ConfigurationManager`] with 5 MiBs reserved for uncompressed
/// data and 5 MiBs reserved for compressed data. Reducing the amount of reserved memory makes it
/// faster to run unit tests.
pub async fn test_context(path: &Path) -> Arc<Context> {
    let runtime = Arc::new(Runtime::new().unwrap());
    let metadata_manager = Arc::new(MetadataManager::try_new(path).await.unwrap());
    let configuration_manager = Arc::new(RwLock::new(ConfigurationManager::new(
        ClusterMode::SingleNode,
        ServerMode::Edge,
    )));

    let session = test_session_context();
    let object_store_url = storage::QUERY_DATA_FOLDER_SCHEME_WITH_HOST
        .try_into()
        .unwrap();
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(path).unwrap());
    session
        .runtime_env()
        .register_object_store(&object_store_url, object_store);

    let storage_engine = Arc::new(RwLock::new(
        StorageEngine::try_new(
            runtime,
            path.to_owned(),
            None,
            &configuration_manager,
            metadata_manager.clone(),
        )
        .await
        .unwrap(),
    ));

    configuration_manager
        .write()
        .await
        .set_uncompressed_reserved_memory_in_bytes(
            UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            storage_engine.clone(),
        )
        .await;

    configuration_manager
        .write()
        .await
        .set_compressed_reserved_memory_in_bytes(
            COMPRESSED_RESERVED_MEMORY_IN_BYTES,
            storage_engine.clone(),
        )
        .await
        .unwrap();

    Arc::new(Context {
        metadata_manager,
        configuration_manager,
        session,
        storage_engine,
    })
}

/// Return a [`SessionContext`] without any additional optimizer rules.
pub fn test_session_context() -> SessionContext {
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let mut state = SessionState::new_with_config_rt(config, runtime);
    for physical_optimizer_rule in optimizer::physical_optimizer_rules() {
        state = state.add_physical_optimizer_rule(physical_optimizer_rule);
    }
    SessionContext::new_with_state(state)
}

/// Return [`ModelTableMetadata`] for a model table with a schema containing a tag column, a
/// timestamp column, and two field columns.
pub fn model_table_metadata() -> ModelTableMetadata {
    let query_schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        Field::new("field_1", ArrowValue::DATA_TYPE, false),
        Field::new("field_2", ArrowValue::DATA_TYPE, false),
        Field::new("tag", DataType::Utf8, false),
    ]));

    let error_bounds = vec![
        ErrorBound::try_new(0.0).unwrap(),
        ErrorBound::try_new(1.0).unwrap(),
        ErrorBound::try_new(5.0).unwrap(),
        ErrorBound::try_new(0.0).unwrap(),
    ];

    let generated_columns = vec![None, None, None, None];

    ModelTableMetadata::try_new(
        "model_table".to_owned(),
        query_schema,
        error_bounds,
        generated_columns,
    )
    .unwrap()
}

/// Return [`ModelTableMetadata`] in an [`Arc`] for a model table with a schema containing a tag
/// column, a timestamp column, and two field columns.
pub fn model_table_metadata_arc() -> Arc<ModelTableMetadata> {
    Arc::new(model_table_metadata())
}

/// Return a [`RecordBatch`] containing three compressed segments.
pub fn compressed_segments_record_batch() -> RecordBatch {
    compressed_segments_record_batch_with_time(1, 0, 0.0)
}

/// Return a [`RecordBatch`] containing three compressed segments from `univariate_id`. The
/// compressed segments time range is from `time_ms` to `time_ms` + 3, while the value range is from
/// `offset` + 5.2 to `offset` + 34.2.
pub fn compressed_segments_record_batch_with_time(
    univariate_id: u64,
    time_ms: i64,
    offset: f32,
) -> RecordBatch {
    let start_times = vec![time_ms, time_ms + 2, time_ms + 4];
    let end_times = vec![time_ms + 1, time_ms + 3, time_ms + 5];
    let min_values = vec![offset + 5.2, offset + 10.3, offset + 30.2];
    let max_values = vec![offset + 20.2, offset + 12.2, offset + 34.2];

    let univariate_id = UInt64Array::from(vec![univariate_id, univariate_id, univariate_id]);
    let model_type_id = UInt8Array::from(vec![1, 1, 2]);
    let start_time = TimestampArray::from(start_times);
    let end_time = TimestampArray::from(end_times);
    let timestamps = BinaryArray::from_vec(vec![b"", b"", b""]);
    let min_value = ValueArray::from(min_values);
    let max_value = ValueArray::from(max_values);
    let values = BinaryArray::from_vec(vec![b"", b"", b""]);
    let residuals = BinaryArray::from_vec(vec![b"", b"", b""]);
    let error = Float32Array::from(vec![0.2, 0.5, 0.1]);

    let schema = COMPRESSED_SCHEMA.clone();

    RecordBatch::try_new(
        schema.0,
        vec![
            Arc::new(univariate_id),
            Arc::new(model_type_id),
            Arc::new(start_time),
            Arc::new(end_time),
            Arc::new(timestamps),
            Arc::new(min_value),
            Arc::new(max_value),
            Arc::new(values),
            Arc::new(residuals),
            Arc::new(error),
        ],
    )
    .unwrap()
}
