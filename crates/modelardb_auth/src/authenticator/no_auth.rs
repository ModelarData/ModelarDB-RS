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

//! An [`Authenticator`] that grants all permissions to all callers without any validation. Used
//! when authentication is not required.

use async_trait::async_trait;
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::Permission;
use crate::authenticator::Authenticator;

/// An [`Authenticator`] that grants all permissions to all callers without any validation.
pub struct NoAuth;

#[async_trait]
impl Authenticator for NoAuth {
    async fn authorize(
        &self,
        _metadata: &MetadataMap,
        _required_permission: Permission,
    ) -> Result<(), Status> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_no_auth_allows_read() {
        assert!(
            NoAuth
                .authorize(&MetadataMap::new(), Permission::Read)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_no_auth_allows_write() {
        assert!(
            NoAuth
                .authorize(&MetadataMap::new(), Permission::Write)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_no_auth_allows_admin() {
        assert!(
            NoAuth
                .authorize(&MetadataMap::new(), Permission::Admin)
                .await
                .is_ok()
        );
    }
}
