/* Copyright 2025 The ModelarDB Contributors
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

//! Implementation of types that provide a write-ahead-log for ModelarDB that can be used to
//! efficiently persist data and operations on disk to avoid data loss and enable crash recovery.

use std::collections::HashMap;
use std::fs::File;

/// Write-ahead-log that logs data on a per-table level and operations separately.
struct WriteAheadLog {
    table_logs: HashMap<String, WriteAheadLogFile>,
    operation_log: WriteAheadLogFile,
}

/// Wrapper around a [`File`] that enforces that [`sync_all()`](File::sync_all) is called
/// immediately after writing to ensure that all data is on disk before returning.
struct WriteAheadLogFile {
    file: File,
}
