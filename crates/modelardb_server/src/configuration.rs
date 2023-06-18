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

use modelardb_common::errors::ModelarDbError;

use crate::ServerMode;

/// Store's the system's configuration and provides functionality for updating the configuration.
#[derive(Clone)]
pub struct ConfigurationManager {
    /// The mode of the server used to determine the behaviour when modifying the remote object store.
    server_mode: ServerMode,
    /// Amount of memory to reserve for storing uncompressed data buffers.
    uncompressed_reserved_memory_in_bytes: usize,
    /// Amount of memory to reserve for storing compressed data buffers.
    compressed_reserved_memory_in_bytes: usize,
}

impl ConfigurationManager {
    pub fn new(server_mode: ServerMode) -> Self {
        Self {
            server_mode,
            uncompressed_reserved_memory_in_bytes: 512 * 1024 * 1024, // 512 MiB
            compressed_reserved_memory_in_bytes: 512 * 1024 * 1024,   // 512 MiB
        }
    }

    pub(crate) fn server_mode(&self) -> &ServerMode {
        &self.server_mode
    }

    pub(crate) fn uncompressed_reserved_memory_in_bytes(&self) -> &usize {
        &self.uncompressed_reserved_memory_in_bytes
    }

    // TODO: The remaining memory in the storage engine needs to be updated when updating these.
    // TODO: Check if other places need to be updated.

    /// TODO: Set the new value and update the uncompressed remaining reserved memory in the storage
    ///       engine. If the value was updated, return [`Ok`], otherwise return
    ///       [`ConfigurationError`](ModelarDbError::ConfigurationError).
    pub(crate) fn set_uncompressed_reserved_memory_in_bytes(
        &mut self,
        new_uncompressed_reserved_memory_in_bytes: usize,
    ) -> Result<(), ModelarDbError> {
        self.uncompressed_reserved_memory_in_bytes = new_uncompressed_reserved_memory_in_bytes;
        Ok(())
    }

    pub(crate) fn compressed_reserved_memory_in_bytes(&self) -> &usize {
        &self.compressed_reserved_memory_in_bytes
    }

    /// TODO: Set the new value and update the compressed remaining reserved memory in the storage engine.
    ///       If the value was updated, return [`Ok`], otherwise return
    ///       [`ConfigurationError`](ModelarDbError::ConfigurationError).
    pub(crate) fn set_compressed_reserved_memory_in_bytes(
        &mut self,
        new_compressed_reserved_memory_in_bytes: usize,
    ) -> Result<(), ModelarDbError> {
        self.compressed_reserved_memory_in_bytes = new_compressed_reserved_memory_in_bytes;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: Test that the value is updated in the storage engine as well.
    // Tests for ConfigurationManager.
    #[test]
    fn test_set_uncompressed_reserved_memory_in_bytes() {
        let mut configuration_manager = ConfigurationManager::new(ServerMode::Edge);
        assert_eq!(*configuration_manager.uncompressed_reserved_memory_in_bytes(), 512 * 1024 * 1024);

        configuration_manager.set_uncompressed_reserved_memory_in_bytes(1024).unwrap();
        assert_eq!(*configuration_manager.uncompressed_reserved_memory_in_bytes(), 1024);
    }

    #[test]
    fn test_set_compressed_reserved_memory_in_bytes() {
        let mut configuration_manager = ConfigurationManager::new(ServerMode::Edge);
        assert_eq!(*configuration_manager.compressed_reserved_memory_in_bytes(), 512 * 1024 * 1024);

        configuration_manager.set_compressed_reserved_memory_in_bytes(1024).unwrap();
        assert_eq!(*configuration_manager.compressed_reserved_memory_in_bytes(), 1024);
    }
}
