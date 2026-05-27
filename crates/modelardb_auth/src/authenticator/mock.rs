/* Copyright 2026 The ModelarDB Contributors
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

//! A mock [`Authenticator`] for use in tests. Always grants access and records every permission
//! that was requested so tests can assert which permission the auth layer required for a given
//! request.

use std::sync::Mutex;

use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::Permission;
use crate::authenticator::Authenticator;

/// An [`Authenticator`] for use in tests that always grants access and records every
/// [`Permission`] passed to [`authorize`](Authenticator::authorize).
pub struct MockAuthenticator {
    /// Every [`Permission`] passed to [`authorize`](Authenticator::authorize).
    calls: Mutex<Vec<Permission>>,
}

impl MockAuthenticator {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(vec![]),
        }
    }

    /// Return a copy of all permissions passed to [`authorize`](Authenticator::authorize)
    /// in the order they were received.
    pub fn calls(&self) -> Vec<Permission> {
        self.calls.lock().unwrap().clone()
    }
}

impl Authenticator for MockAuthenticator {
    fn authorize(
        &self,
        _metadata: &MetadataMap,
        required_permission: Permission,
    ) -> Result<(), Status> {
        self.calls.lock().unwrap().push(required_permission);
        Ok(())
    }
}
