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

//! Types to support authentication and authorization in ModelarDB.

use tonic::metadata::MetadataMap;
use tonic::Status;

/// The permission required to perform an operation in a ModelarDB server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

impl Permission {
    /// Return [`true`] if `granted` satisfies this required permission using the hierarchy
    /// Admin ⊇ Write ⊇ Read, otherwise [`false`] is returned.
    pub fn is_satisfied_by(&self, granted: &Permission) -> bool {
        match (self, granted) {
            (Permission::Read, _) => true,
            (Permission::Write, Permission::Write | Permission::Admin) => true,
            (Permission::Admin, Permission::Admin) => true,
            _ => false,
        }
    }
}

/// Validates the credentials in `metadata` and checks that the caller has the `required_permission`.
/// Implementations must return `Ok(())` if authorized, `Err(Status::unauthenticated(...))` if
/// credentials are missing or invalid, or `Err(Status::permission_denied(...))` if credentials are
/// valid but the caller lacks the required permission. Error messages must not reveal the specific
/// reason for rejection to prevent information leakage.
pub trait Authenticator: Send + Sync {
    fn authorize(
        &self,
        metadata: &MetadataMap,
        required_permission: Permission,
    ) -> Result<(), Status>;
}


