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

use std::any::TypeId;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::ArrowPrimitiveType;
use datafusion::arrow::array::{BinaryArray, Float32Array, UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use modelardb_common::schemas::COMPRESSED_SCHEMA;
use modelardb_common::types::{ArrowTimestamp, ArrowValue};
use modelardb_common::types::{TimestampArray, ValueArray};
use modelardb_compression::ErrorBound;
use object_store::local::LocalFileSystem;
use tokio::sync::RwLock;

use crate::configuration::ConfigurationManager;
use crate::metadata::model_table_metadata::ModelTableMetadata;
use crate::metadata::MetadataManager;
use crate::query::ModelTable;
use crate::storage::{self, StorageEngine};
use crate::{optimizer, Context, ServerMode};

/// Expected size of the compressed segments produced in the tests.
pub const COMPRESSED_SEGMENTS_SIZE: usize = 1399;

/// Return a [`Context`] with the configuration manager created by `test_configuration_manager()`
/// and the data folder set to `path`.
pub async fn test_context(path: &Path) -> Arc<Context> {
    let metadata_manager = MetadataManager::try_new(path).await.unwrap();
    let configuration_manager = Arc::new(RwLock::new(ConfigurationManager::new(ServerMode::Edge)));

    let session = test_session_context();
    let object_store_url = storage::QUERY_DATA_FOLDER_SCHEME_WITH_HOST
        .try_into()
        .unwrap();
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(path).unwrap());
    session
        .runtime_env()
        .register_object_store(&object_store_url, object_store);

    let storage_engine = RwLock::new(
        StorageEngine::try_new(
            path.to_owned(),
            None,
            configuration_manager.clone(),
            metadata_manager.clone(),
            true,
        )
        .await
        .unwrap(),
    );

    Arc::new(Context {
        metadata_manager,
        configuration_manager,
        session,
        storage_engine,
    })
}

/// Return a [`ConfigurationManager`] with 5 MiBs reserved for uncompressed data and 5 MiBs reserved
/// for compressed data. Reducing the amount of reserved memory makes it faster to run unit tests.
pub fn test_configuration_manager() -> ConfigurationManager {
    let mut configuration_manager = ConfigurationManager::new(ServerMode::Edge);

    configuration_manager
        .set_uncompressed_reserved_memory_in_bytes(5 * 1024 * 1024)
        .unwrap(); // 5 MiB
    configuration_manager
        .set_compressed_reserved_memory_in_bytes(5 * 1024 * 1024)
        .unwrap(); // 5 MiB

    configuration_manager
}

/// Return a [`SessionContext`] without any additional optimizer rules.
pub fn test_session_context() -> SessionContext {
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let mut state = SessionState::with_config_rt(config, runtime);
    for physical_optimizer_rule in optimizer::physical_optimizer_rules() {
        state = state.add_physical_optimizer_rule(physical_optimizer_rule);
    }
    SessionContext::with_state(state)
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

/// Return a [`RecordBatch`] containing three compressed segments.
pub fn compressed_segments_record_batch() -> RecordBatch {
    compressed_segments_record_batch_with_time(0, 0.0)
}

/// Return a [`RecordBatch`] containing three compressed segments. The compressed segments time
/// range is from `time_ms` to `time_ms` + 3, while the value range is from `offset` + 5.2 to
/// `offset` + 34.2.
pub fn compressed_segments_record_batch_with_time(time_ms: i64, offset: f32) -> RecordBatch {
    let start_times = vec![time_ms, time_ms + 2, time_ms + 4];
    let end_times = vec![time_ms + 1, time_ms + 3, time_ms + 5];
    let min_values = vec![offset + 5.2, offset + 10.3, offset + 30.2];
    let max_values = vec![offset + 20.2, offset + 12.2, offset + 34.2];

    let univariate_id = UInt64Array::from(vec![1, 2, 3]);
    let model_type_id = UInt8Array::from(vec![2, 3, 3]);
    let start_time = TimestampArray::from(start_times);
    let end_time = TimestampArray::from(end_times);
    let timestamps = BinaryArray::from_vec(vec![b"", b"", b""]);
    let min_value = ValueArray::from(min_values);
    let max_value = ValueArray::from(max_values);
    let values = BinaryArray::from_vec(vec![b"1111", b"1000", b"0000"]);
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

/// Parse, plan, and optimize the `query` for execution on data in `path`.
pub async fn query_optimized_physical_query_plan(
    path: &Path,
    query: &str,
) -> Arc<dyn ExecutionPlan> {
    let context = test_context(path).await;
    let model_table_metadata = model_table_metadata();

    context
        .session
        .register_table(
            "model_table",
            ModelTable::new(context.clone(), Arc::new(model_table_metadata)),
        )
        .unwrap();

    context
        .session
        .sql(query)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

/// Assert that `physical_plan` and `expected_plan` contain the same operators. `expected_plan`
/// only contains the type ids so the tests do not have to construct the actual operators.
pub fn assert_eq_physical_plan_expected(
    physical_plan: Arc<dyn ExecutionPlan>,
    expected_plan: Vec<Vec<TypeId>>,
) {
    let mut level = 0;
    let mut current_execs = vec![physical_plan];
    let mut next_execs = vec![];

    while !current_execs.is_empty() {
        let expected_execs = &expected_plan[level];
        assert_eq!(current_execs.len(), expected_execs.len());

        for (current, expected) in current_execs.iter().zip(expected_execs) {
            assert_eq!(current.as_any().type_id(), *expected);
            next_execs.extend(current.children());
        }

        level += 1;
        current_execs = next_execs;
        next_execs = vec![];
    }
}
