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

//! Defines the [`Authenticator`] trait for validating credentials and authorizing access to
//! ModelarDB.

#[cfg(feature = "testing")]
pub mod mock;

use async_trait::async_trait;
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::Permission;

/// Validates the credentials in `metadata` and checks that the caller has the `required_permission`.
/// Implementations must return:
///
/// - `Ok(())` if authorized.
/// - `Err(Status::unauthenticated(...))` if credentials are missing or invalid.
/// - `Err(Status::permission_denied(...))` if credentials are valid but the caller lacks the
///   required permission.
///
/// Error messages must not reveal the specific reason for rejection to prevent information leakage.
#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authorize(
        &self,
        metadata: &MetadataMap,
        required_permission: Permission,
    ) -> Result<(), Status>;
}
