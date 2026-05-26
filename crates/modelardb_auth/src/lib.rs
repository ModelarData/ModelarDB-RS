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

pub mod authenticator;

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
        matches!(
            (self, granted),
            (Permission::Read, _)
                | (Permission::Write, Permission::Write | Permission::Admin)
                | (Permission::Admin, Permission::Admin)
        )
    }
}
