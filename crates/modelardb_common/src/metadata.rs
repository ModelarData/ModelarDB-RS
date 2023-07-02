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

//! Common metadata functionality shared between the server metadata manager and the manager
//! metadata manager.

use sqlx::Database;
use sqlx::Pool;

/// Common metadata functionality used to save model table metadata in both the server metadata
/// manager and the manager metadata manager.
#[derive(Clone)]
pub struct MetadataManager<DB: Database> {
    /// Pool of connections to the metadata database.
    metadata_database_pool: Pool<DB>,
}
