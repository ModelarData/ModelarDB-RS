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

//! Table metadata manager that includes functionality used to access both the server metadata deltalake
//! and the manager metadata deltalake. Note that the entire server metadata deltalake can be accessed
//! through this metadata manager, while it only supports a subset of the manager metadata deltalake.

use dashmap::DashMap;
use datafusion::prelude::SessionContext;
use deltalake::DeltaTable;

/// Stores the metadata required for reading from and writing to the tables and model tables.
/// The data that needs to be persisted is stored in the metadata deltalake.
pub struct TableMetadataManager {
    /// Map from metadata deltalake table names to [`DeltaTables`](DeltaTable).
    metadata_tables: DashMap<String, DeltaTable>,
    /// Session used to read from the metadata deltalake using Apache Arrow DataFusion.
    session: SessionContext,
    /// Cache of tag value hashes used to signify when to persist new unsaved tag combinations.
    tag_value_hashes: DashMap<String, u64>,
}

impl TableMetadataManager {}
