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

use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::execution::runtime_env::RuntimeEnv;
use modelardb_common::test;
use modelardb_common::types::ClusterMode;
use object_store::local::LocalFileSystem;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

use crate::configuration::ConfigurationManager;
use crate::metadata::MetadataManager;
use crate::storage::{self, StorageEngine};
use crate::{optimizer, Context, ServerMode};

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
            test::UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            storage_engine.clone(),
        )
        .await;

    configuration_manager
        .write()
        .await
        .set_compressed_reserved_memory_in_bytes(
            test::COMPRESSED_RESERVED_MEMORY_IN_BYTES,
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
