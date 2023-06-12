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

//! Management of the system's configuration, including the server mode, and the amount of
//! reserved memory for uncompressed and compressed data.

use crate::ServerMode;

/// Store's the system's configuration and provides functionality for updating the configuration.
pub struct ConfigurationManager {
    /// The mode of the server used to determine the behaviour when modifying the remote object store.
    pub server_mode: ServerMode,
    /// Amount of memory to reserve for storing uncompressed data buffers.
    pub uncompressed_reserved_memory_in_bytes: usize,
    /// Amount of memory to reserve for storing compressed data buffers.
    pub compressed_reserved_memory_in_bytes: usize,
}

impl ConfigurationManager {
    pub fn new(server_mode: ServerMode) -> Self {
        Self {
            server_mode,
            uncompressed_reserved_memory_in_bytes: 512 * 1024 * 1024, // 512 MiB
            compressed_reserved_memory_in_bytes: 512 * 1024 * 1024, // 512 MiB
        }
    }
}
