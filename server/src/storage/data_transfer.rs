/* Copyright 2022 The ModelarDB Contributors
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

//! Support for efficiently transferring data to a blob store in the cloud. Data saved locally on
//! disk is managed here until it is of a sufficient size to be transferred efficiently. Furthermore,
//! the component ensures that local data is kept on disk until a reliable connection is established.

// TODO: Files should be combined when transferring data.
// TODO: When data is done transferring, the files should be deleted from local storage.
// TODO: A system should be in place to keep track of the location of certain data to avoid duplicate data between edge and cloud.
// TODO: Add a user configuration in the metadata component that makes it possible to change the batch size of when to send.

use std::collections::HashMap;
use std::path::PathBuf;
use crate::errors::ModelarDbError;

pub(super) struct DataTransfer {
    /// Map from keys to the combined size in bytes of the compressed files saved under the key.
    compressed_files: HashMap<u64, u64>,
    /// The number of bytes that is required before transferring a batch of data to the blob store.
    transfer_batch_size_in_bytes: usize,
}

impl DataTransfer {
    // TODO: This should look through local storage to retrieve files that should be transferred.
    pub(super) fn new(transfer_batch_size_in_bytes: usize) -> Self {
        Self {
            compressed_files: HashMap::new(),
            transfer_batch_size_in_bytes,
        }
    }

    /// Insert the location of the compressed file into the files to be transferred. Also retrieve
    /// the size of the file and add it to the total size of the current local files under the key.
    pub(super) fn insert_compressed_file() {
        // TODO: If the combined size of the files is larger than the batch size, transfer the data to the blob store.
    }

    /// Transfer the data corresponding to `key` to the blob store. Once successfully transferred,
    /// delete the data from local storage.
    fn transfer_data() {
        // TODO: Read all files that correspond to the key.
        // TODO: Transfer the read data to the blob store.
        // TODO: Delete the transferred files from local storage.
    }
}
